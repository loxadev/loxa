use super::*;

fn sql_error(code: i32) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None)
}

fn cell_size_check(connection: &Connection) -> i64 {
    connection
        .query_row("PRAGMA cell_size_check", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn cell_size_checks_are_enabled_on_fresh_reopened_and_migrated_connections() {
    let (_directory, root) = private_root("loxa-cell-check-");
    let (connection, _) = open_store(&root).unwrap();
    assert_eq!(cell_size_check(&connection), 1);
    connection
        .pragma_update(None, "cell_size_check", 0)
        .unwrap();
    assert_eq!(cell_size_check(&connection), 0);
    connection.close().unwrap();
    let (connection, _) = open_store(&root).unwrap();
    assert_eq!(cell_size_check(&connection), 1);
    connection.close().unwrap();

    for create in [
        schema::create_v1_fixture as fn(&Path),
        schema::create_v2_fixture,
        schema::create_v3_fixture,
        schema::create_v4_fixture,
    ] {
        let (_directory, root) = private_root("loxa-cell-check-migration-");
        create(&root.join("app.sqlite"));
        let (connection, info) = open_store(&root).unwrap();
        assert_eq!(info.schema_version, 5);
        assert_eq!(cell_size_check(&connection), 1);
        connection.close().unwrap();
    }
}

#[test]
fn native_corruption_health_survives_commit_and_corrupt_context_remapping() {
    for code in [
        rusqlite::ffi::SQLITE_CORRUPT,
        rusqlite::ffi::SQLITE_CORRUPT_INDEX,
        rusqlite::ffi::SQLITE_NOTADB,
    ] {
        let error = schema::classify_sql_error(sql_error(code));
        assert_eq!(error.kind(), HistoryErrorKind::Corrupt);
        assert!(error.native_sqlite_corruption);
        let remapped = error.remap(HistoryErrorKind::Corrupt, "stored metadata is invalid");
        assert!(remapped.native_sqlite_corruption);
        let commit = schema::classify_commit_error(sql_error(code), "commit outcome is unknown");
        assert_eq!(commit.kind(), HistoryErrorKind::OutcomeUnknown);
        assert!(commit.native_sqlite_corruption);
    }
    let conversion = schema::classify_sql_error(rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid fixture row").into(),
    ));
    assert_eq!(conversion.kind(), HistoryErrorKind::Corrupt);
    assert!(!conversion.native_sqlite_corruption);
    assert!(
        !HistoryError::new(HistoryErrorKind::Corrupt, "schema mismatch").native_sqlite_corruption
    );
}

async fn ready_owner(label: &str) -> (tempfile::TempDir, PathBuf, HistoryOwner, HistoryHandle) {
    let (directory, root) = private_root(label);
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let owner = HistoryOwner::start(
        &root,
        models,
        RuntimeIdentity::BundledB10344,
        Arc::new(AtomicBool::new(false)),
        "boot-1".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let mut status = handle.status.subscribe();
    tokio::time::timeout(Duration::from_secs(2), async {
        while status.borrow().phase == HistoryPhase::Opening {
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("history startup timed out");
    assert_eq!(handle.status().phase, HistoryPhase::Ready);
    (directory, root, owner, handle)
}

async fn drained(owner: &HistoryOwner, handle: &HistoryHandle) {
    let mut exit = handle.exit_receiver();
    handle.begin_drain();
    tokio::time::timeout(Duration::from_secs(2), async {
        while *exit.borrow() == HistoryExit::Running {
            exit.changed().await.unwrap();
        }
    })
    .await
    .expect("history drain timed out");
    assert_eq!(*exit.borrow(), HistoryExit::Drained);
    owner.join().unwrap();
}

async fn execute(handle: &HistoryHandle, operation: WireCommand) -> HistoryCompletion {
    tokio::time::timeout(Duration::from_secs(2), handle.execute(operation))
        .await
        .expect("history operation timed out")
        .unwrap()
}

async fn create_conversation(handle: &HistoryHandle) -> loxa_ipc::ConversationSummary {
    let completion = execute(
        handle,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
    )
    .await;
    match completion.result.unwrap() {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("unexpected create reply"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_commit_corruption_retires_operations_and_keeps_explicit_close_ownership() {
    let (_directory, root, owner, handle) = ready_owner("loxa-native-retirement-").await;
    let error = handle
        .observe_sql_error(sql_error(rusqlite::ffi::SQLITE_CORRUPT_INDEX), true)
        .await;
    assert_eq!(error.kind(), HistoryErrorKind::OutcomeUnknown);
    assert!(error.native_sqlite_corruption);
    assert_eq!(handle.status().phase, HistoryPhase::Unavailable);
    assert!(!handle.draining.load(Ordering::Acquire));
    assert_eq!(*handle.exit_receiver().borrow(), HistoryExit::Running);
    let refused = execute(
        &handle,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
    )
    .await;
    assert_eq!(
        refused.result.unwrap_err().kind(),
        HistoryErrorKind::WorkerUnavailable
    );
    drop(refused.permit);
    let unresolved = handle.try_reconcile_submission([1; 16], [1; 32]).unwrap();
    let unresolved = tokio::time::timeout(Duration::from_secs(2), unresolved)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(unresolved.kind(), HistoryErrorKind::OutcomeUnknown);

    handle.fail_next_close();
    handle.begin_drain();
    let mut status = handle.status.subscribe();
    tokio::time::timeout(Duration::from_secs(2), async {
        while status.borrow().phase != HistoryPhase::FlushFailed {
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("injected close failure timed out");
    assert_eq!(*handle.exit_receiver().borrow(), HistoryExit::Running);
    assert_eq!(handle.drain_dispatches(), 1);
    drained(&owner, &handle).await;
    assert_eq!(handle.drain_dispatches(), 2);
    let connection = Connection::open(root.join("app.sqlite")).unwrap();
    let rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0, "retired worker still executed a mutation");
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_uncertain_commits_keep_the_connection_reusable_for_reconciliation() {
    let (_directory, _root, owner, handle) = ready_owner("loxa-transient-health-").await;
    let conversation = create_conversation(&handle).await;
    let prepared = Arc::new(prepared_admission(
        decode_id(&conversation.id).unwrap(),
        1,
        61,
        61,
        61,
        AdmissionKind::Send {
            user_text: "question".into(),
            draft: None,
        },
    ));
    let admitted =
        tokio::time::timeout(Duration::from_secs(2), handle.try_admit(prepared).unwrap())
            .await
            .unwrap()
            .unwrap();
    let committed = admitted.result.unwrap();
    drop(admitted.permit);
    for code in [
        rusqlite::ffi::SQLITE_BUSY,
        rusqlite::ffi::SQLITE_LOCKED,
        rusqlite::ffi::SQLITE_FULL,
        rusqlite::ffi::SQLITE_IOERR,
        rusqlite::ffi::SQLITE_INTERRUPT,
        rusqlite::ffi::SQLITE_CONSTRAINT,
    ] {
        let error = handle.observe_sql_error(sql_error(code), true).await;
        assert_eq!(error.kind(), HistoryErrorKind::OutcomeUnknown);
        assert!(!error.native_sqlite_corruption);
        assert_eq!(handle.status().phase, HistoryPhase::Ready);
        let reconcile = handle.try_reconcile_submission([61; 16], [61; 32]).unwrap();
        let reconciled = tokio::time::timeout(Duration::from_secs(2), reconcile)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reconciled, committed);
    }
    let invalid_row = handle
        .observe_sql_error(
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid fixture row").into(),
            ),
            false,
        )
        .await;
    assert_eq!(invalid_row.kind(), HistoryErrorKind::Corrupt);
    assert!(!invalid_row.native_sqlite_corruption);
    assert_eq!(handle.status().phase, HistoryPhase::Ready);
    create_conversation(&handle).await;
    drained(&owner, &handle).await;
}

async fn native_purge_failure(drop_receiver: bool) {
    let (_directory, root, owner, handle) = ready_owner("loxa-purge-native-health-").await;
    let conversation = create_conversation(&handle).await;
    handle.set_next_purge_error(schema::classify_sql_error(sql_error(
        rusqlite::ffi::SQLITE_NOTADB,
    )));
    let operation = WireCommand::DeleteConversation {
        conversation_id: conversation.id.clone(),
        expected_revision: "1".into(),
    };
    if drop_receiver {
        let permit = handle.ordinary.clone().try_acquire_owned().unwrap();
        let (reply, received) = oneshot::channel();
        drop(received);
        handle
            .commands
            .try_send(HistoryCommand::Execute {
                operation,
                generation: None,
                reply,
                permit,
            })
            .unwrap();
        let mut status = handle.status.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            while status.borrow().phase != HistoryPhase::Unavailable {
                status.changed().await.unwrap();
            }
        })
        .await
        .expect("dropped deletion reply did not retire the worker");
    } else {
        let completion = execute(&handle, operation).await;
        assert_eq!(
            completion.result.unwrap(),
            HistoryReply::ConversationDeleted {
                conversation_id: conversation.id.clone(),
                revision: "2".into(),
                purge_complete: false,
            }
        );
        drop(completion.permit);
    }
    assert_eq!(handle.status().phase, HistoryPhase::Unavailable);
    assert!(!handle.draining.load(Ordering::Acquire));
    let refused = execute(
        &handle,
        WireCommand::ListConversations {
            cursor: None,
            limit: 1,
        },
    )
    .await;
    assert_eq!(
        refused.result.unwrap_err().kind(),
        HistoryErrorKind::WorkerUnavailable
    );
    drop(refused.permit);
    assert_eq!(handle.ordinary.available_permits(), ORDINARY_CAPACITY);
    drained(&owner, &handle).await;
    let connection = Connection::open(root.join("app.sqlite")).unwrap();
    let deleted: i64 = connection
        .query_row(
            "SELECT deleted FROM conversations WHERE id = ?1",
            [decode_id(&conversation.id).unwrap().as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deleted, 1, "committed deletion tombstone was lost");
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_purge_failure_preserves_delete_acknowledgement_and_retires_worker() {
    native_purge_failure(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_purge_failure_retires_worker_even_when_delete_receiver_is_dropped() {
    native_purge_failure(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_purge_failure_preserves_delete_acknowledgement_without_retirement() {
    let (_directory, _root, owner, handle) = ready_owner("loxa-purge-transient-health-").await;
    let conversation = create_conversation(&handle).await;
    handle.set_next_purge_error(schema::classify_sql_error(sql_error(
        rusqlite::ffi::SQLITE_BUSY,
    )));
    let completion = execute(
        &handle,
        WireCommand::DeleteConversation {
            conversation_id: conversation.id.clone(),
            expected_revision: "1".into(),
        },
    )
    .await;
    assert!(matches!(
        completion.result.unwrap(),
        HistoryReply::ConversationDeleted {
            purge_complete: false,
            ..
        }
    ));
    drop(completion.permit);
    assert_eq!(handle.status().phase, HistoryPhase::Ready);
    create_conversation(&handle).await;
    drained(&owner, &handle).await;
}
