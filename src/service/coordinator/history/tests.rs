use super::*;
use crate::catalog::{Manifest, Origin};
use crate::history::{ExecutionOutcome, FinalizationInput, SuffixCommit, SuffixInput};
use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
use crate::service::coordinator::generation::parser::SseDecoder;
use crate::service::coordinator::generation::persistence::{OutputPipeline, SaveChunkFailure};
use crate::service::coordinator::{Coordinator, HistoryExit, OwnerExit};
use loxa_ipc::{ErrorCategory, GenerationTarget, HistoryCommand, HistoryPhase, HistoryReply};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

mod preflight;

struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    coordinator: Coordinator,
    conversation_id: String,
    conversation_bytes: [u8; 16],
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with_engine(super::super::state::EngineDescriptor {
            pid: 42,
            endpoint: Arc::new("/tmp/loxa-history-test-engine.sock".into()),
        })
        .await
    }

    async fn start_with_engine(engine: super::super::state::EngineDescriptor) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("lc-")
            .tempdir_in("/tmp")
            .unwrap();
        let directory_path = fs::canonicalize(directory.path()).unwrap();
        let root = directory_path.join("dev");
        let forbidden_root = directory_path.join("normal");
        fs::create_dir(&forbidden_root).unwrap();
        let executable = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let bootstrap = loxa_ipc::initialize_development_root(
            &root,
            &forbidden_root,
            &executable,
            crate::service::BUILD_ID,
        )
        .unwrap();
        let paths = crate::paths::AppPaths::from_values(Some(&root), None).unwrap();
        install_model(&paths.models);
        let fingerprint = Arc::new(
            RuntimeFingerprint::from_manifest_for_service(
                &model_manifest(),
                4096,
                EffectiveProfile::Generic,
            )
            .unwrap(),
        );
        let ownership = crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&paths.run)
            .unwrap_or_else(|_| panic!("acquire isolated runtime ownership"));
        let coordinator = Coordinator::start(
            paths,
            bootstrap.root().control_dir().to_owned(),
            bootstrap.root().root_identity().to_owned(),
            "test-machine-boot".into(),
            "test-service-boot".into(),
            ownership,
            tokio::runtime::Handle::current(),
            None,
            None,
        )
        .unwrap();
        wait_for_history_ready(&coordinator).await;
        let (created, permit) = coordinator
            .history(HistoryCommand::CreateConversation {
                model_id: "demo".into(),
            })
            .await;
        drop(permit);
        let HistoryReply::Conversation(conversation) = created.unwrap() else {
            panic!("create conversation returned the wrong reply");
        };
        let conversation_bytes = decode_id(&conversation.id);
        coordinator.force_ready_for_history_test(fingerprint, engine);
        Self {
            _directory: directory,
            root,
            coordinator,
            conversation_id: conversation.id,
            conversation_bytes,
        }
    }

    fn input(&self, submission: u8, text: &str) -> AdmissionInput {
        AdmissionInput {
            conversation_id: self.conversation_bytes,
            submission_id: [submission; 16],
            expected_conversation_revision: 1,
            expected_profile_revision: 1,
            submission_hash: None,
            effective_context: None,
            system_instruction: String::new(),
            max_output_tokens: 512,
            prompt_basis: PromptBasis {
                references: Vec::new(),
            },
            kind: AdmissionKind::Send {
                user_text: text.into(),
                draft: None,
            },
        }
    }

    fn reserve_pending(
        &self,
        input: &AdmissionInput,
    ) -> (Arc<AdmissionReservation>, GenerationTarget) {
        let pending = self.coordinator.register_generation_connection().unwrap();
        let target = GenerationTarget::Pending {
            boot_epoch: self.coordinator.boot_epoch().into(),
            pending_nonce: pending.nonce().into(),
        };
        let claim = self.coordinator.shared.state().reserve_pending_admission(
            &pending.pending,
            input.conversation_id,
            input.submission_id,
            input.semantic_hash(),
            input.expected_conversation_revision,
            input.expected_profile_revision,
        );
        let AdmissionClaim::Fresh(reservation) = claim.unwrap() else {
            panic!("pending admission was not fresh");
        };
        (reservation, target)
    }

    async fn assert_recovered_before_execution(
        &self,
        reservation: &Arc<AdmissionReservation>,
        committed: &CommittedAdmission,
    ) {
        assert_eq!(committed.submission_id, reservation.submission_id);
        assert_eq!(committed.conversation_id, reservation.conversation_id);
        assert_eq!(
            committed.operation_generation,
            reservation.operation_generation
        );
        assert_eq!(committed.owner_epoch, self.coordinator.boot_epoch());
        assert!(!self.coordinator.admission_active_for_test());
        assert!(reservation.output.lock().unwrap().is_none());
        assert!(matches!(
            self.coordinator.status().phase,
            loxa_ipc::RuntimePhase::Ready { .. }
        ));
        assert!(self.coordinator.history_is_ready());
        assert_terminalized(&self.root);

        let connection = rusqlite::Connection::open(self.root.join("app.sqlite")).unwrap();
        let (attempts, matching): (i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), COUNT(CASE WHEN id = ?1 AND submission_id = ?2
                     AND owner_epoch = ?3 AND operation_generation = ?4 THEN 1 END)
                 FROM attempts",
                rusqlite::params![
                    committed.attempt_id.as_slice(),
                    reservation.submission_id.as_slice(),
                    self.coordinator.boot_epoch(),
                    reservation.operation_generation,
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((attempts, matching), (1, 1));
        connection.close().unwrap();

        let (renamed, permit) = self
            .coordinator
            .history(HistoryCommand::RenameConversation {
                conversation_id: self.conversation_id.clone(),
                expected_revision: committed.post_conversation_revision.to_string(),
                title: "Recovered".into(),
            })
            .await;
        drop(permit);
        assert!(matches!(renamed.unwrap(), HistoryReply::Conversation(_)));
    }

    async fn finish_stopped(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let owner = *self.coordinator.owner_exit_receiver().borrow();
            let history = *self.coordinator.history_exit_receiver().borrow();
            if matches!(owner, OwnerExit::Quiesced | OwnerExit::Failed)
                && history == HistoryExit::Drained
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "owners did not drain"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.coordinator.release_runtime_if_durable();
        self.coordinator.join_owner().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_observer_and_stop_after_commit_retain_one_terminalized_admission() {
    let fixture = Fixture::start().await;
    let completion = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_admission_completion_barrier_for_test(Arc::clone(&completion));
    let input = fixture.input(1, "hello");
    let observer = fixture
        .coordinator
        .admit_history_generation(input.clone())
        .await
        .unwrap();
    completion.wait();
    drop(observer);

    let duplicate = fixture
        .coordinator
        .admit_history_generation(input.clone())
        .await
        .unwrap();
    let mut changed = input;
    changed.kind = AdmissionKind::Send {
        user_text: "changed".into(),
        draft: None,
    };
    assert_eq!(
        fixture
            .coordinator
            .admit_history_generation(changed)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    for command in [
        HistoryCommand::RenameConversation {
            conversation_id: fixture.conversation_id.clone(),
            expected_revision: "1".into(),
            title: "blocked".into(),
        },
        HistoryCommand::DeleteConversation {
            conversation_id: fixture.conversation_id.clone(),
            expected_revision: "1".into(),
        },
    ] {
        let (result, permit) = fixture.coordinator.history(command).await;
        drop(permit);
        assert_eq!(result.unwrap_err().category, ErrorCategory::Busy);
    }
    fixture.coordinator.stop_service().unwrap();
    completion.wait();
    let committed = wait_for_admission(duplicate).await.unwrap();
    assert_eq!(committed.submission_id, [1; 16]);
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end FROM attempts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal, (2, 1, 0, Some(0), Some(0)));
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_rejects_oversized_retained_capacity_before_reserving() {
    let fixture = Fixture::start().await;
    let mut oversized = String::with_capacity(300 * 1024);
    oversized.push('x');
    let mut input = fixture.input(2, "x");
    input.kind = AdmissionKind::Send {
        user_text: oversized,
        draft: None,
    };
    assert_eq!(
        fixture
            .coordinator
            .admit_history_generation(input)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::InvalidRequest
    );
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_between_reservation_and_sql_enqueue_releases_into_safe_drain() {
    let fixture = Fixture::start().await;
    let dispatch = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_admission_dispatch_barrier_for_test(Arc::clone(&dispatch));
    let coordinator = fixture.coordinator.clone();
    let input = fixture.input(3, "before enqueue");
    let admission = tokio::spawn(async move { coordinator.admit_history_generation(input).await });
    dispatch.wait();
    fixture.coordinator.stop_service().unwrap();
    dispatch.wait();
    let error = admission.await.unwrap().unwrap_err();
    assert_eq!(error.category, ErrorCategory::ServiceUnavailable);
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_inside_admission_transaction_terminalizes_before_drain() {
    let fixture = Fixture::start().await;
    fixture
        .coordinator
        .set_history_progress_interval_for_test(1);
    let commit = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_admission_commit_barrier_for_test(Arc::clone(&commit));
    let observer = fixture
        .coordinator
        .admit_history_generation(fixture.input(4, "inside commit"))
        .await
        .unwrap();
    commit.wait();
    fixture.coordinator.stop_service().unwrap();
    commit.wait();
    wait_for_admission(observer).await.unwrap();
    fixture.finish_stopped().await;
    assert_terminalized(&fixture.root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_lookup_joins_an_active_matching_reservation() {
    let fixture = Fixture::start().await;
    let before_lookup = Arc::new(Barrier::new(2));
    let completion = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_admission_pre_lookup_barrier_for_test(Arc::clone(&before_lookup));
    fixture
        .coordinator
        .set_admission_completion_barrier_for_test(Arc::clone(&completion));

    let input = fixture.input(7, "join committed reservation");
    let duplicate_coordinator = fixture.coordinator.clone();
    let duplicate_input = input.clone();
    let duplicate = tokio::spawn(async move {
        duplicate_coordinator
            .admit_history_generation(duplicate_input)
            .await
    });
    before_lookup.wait();

    let owner = fixture
        .coordinator
        .admit_history_generation(input)
        .await
        .unwrap();
    completion.wait();
    before_lookup.wait();

    let duplicate = duplicate.await.unwrap().unwrap();
    assert!(duplicate.borrow().is_none());
    fixture.coordinator.stop_service().unwrap();
    completion.wait();
    assert_eq!(
        wait_for_admission(owner).await.unwrap(),
        wait_for_admission(duplicate).await.unwrap()
    );
    fixture.finish_stopped().await;
    assert_terminalized(&fixture.root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_admission_reply_and_terminal_failure_retry_under_drain() {
    let fixture = Fixture::start().await;
    fixture.coordinator.drop_next_admission_reply_for_test();
    fixture
        .coordinator
        .fail_next_stop_before_execution_for_test();
    let user_text = "recover me";
    let submission_id = [5; 16];
    let submission_hash = super::super::generation::stable_submission_hash(
        fixture.conversation_bytes,
        1,
        1,
        user_text,
        None,
    );
    let mut input = fixture.input(5, user_text);
    input.submission_hash = Some(submission_hash);
    let observer = fixture
        .coordinator
        .admit_history_generation(input)
        .await
        .unwrap();
    let pending = fixture
        .coordinator
        .register_generation_connection()
        .unwrap();
    fixture.coordinator.stop_service().unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !fixture.coordinator.admission_stop_retry_ready_for_test()
        && tokio::time::Instant::now() < deadline
    {
        fixture.coordinator.stop_service().unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(fixture.coordinator.admission_stop_retry_ready_for_test());
    let reply = fixture
        .coordinator
        .generation_send(
            loxa_ipc::GenerationCommand::Send {
                conversation_id: fixture.conversation_id.clone(),
                submission_id: crate::history::encode_id(submission_id),
                expected_conversation_revision: "1".into(),
                expected_profile_revision: "1".into(),
                user_text: user_text.into(),
                draft: None,
            },
            pending,
        )
        .await
        .unwrap();
    let committed = wait_for_admission(observer).await.unwrap();
    let loxa_ipc::GenerationReply::Accepted(accepted) = reply else {
        panic!("reconciled generation did not return Accepted");
    };
    assert_eq!(accepted.boot_epoch, committed.owner_epoch);
    assert_eq!(
        accepted.attempt_id,
        crate::history::encode_id(committed.attempt_id)
    );
    assert_eq!(
        accepted.operation_generation,
        committed.operation_generation.to_string()
    );
    fixture.finish_stopped().await;
    assert_terminalized(&fixture.root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targeted_stop_retries_failed_pre_execution_save_once() {
    for accepted_target in [false, true] {
        let fixture = Fixture::start().await;
        fixture
            .coordinator
            .fail_next_stop_before_execution_for_test();
        let commit = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .set_admission_commit_barrier_for_test(Arc::clone(&commit));
        let input = fixture.input(19, "recover targeted stop");
        let (reservation, pending_target) = fixture.reserve_pending(&input);
        let target = if accepted_target {
            GenerationTarget::Accepted {
                boot_epoch: fixture.coordinator.boot_epoch().into(),
                submission_id: crate::history::encode_id(reservation.submission_id),
                operation_generation: reservation.operation_generation.to_string(),
            }
        } else {
            pending_target
        };
        // Drive the admission boundary directly; no generation relay is started.
        let observer = fixture
            .coordinator
            .dispatch_reserved_admission(
                input,
                reservation.submission_hash,
                Arc::clone(&reservation),
            )
            .unwrap();
        commit.wait();
        let stopped = fixture.coordinator.stop_generation(&target);
        commit.wait();
        stopped.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !reservation.stop_retry_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first stop save did not reach its retry state");
        assert!(observer.borrow().is_none());
        assert!(reservation.is_cancelled());
        assert!(fixture.coordinator.admission_active_for_test());
        assert!(fixture
            .coordinator
            .shared
            .state()
            .begin_generation_execution(&reservation)
            .is_err());
        assert_eq!(reservation.recovery_claims.load(Ordering::Relaxed), 0);

        let recovery = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_history_for_test(Arc::clone(&recovery));
        recovery.wait();
        let replies: Vec<_> = (0..8)
            .map(|_| fixture.coordinator.stop_generation(&target))
            .collect();
        let claims = reservation.recovery_claims.load(Ordering::Relaxed);
        let retry_ready = reservation.stop_retry_ready();
        let unresolved = observer.borrow().is_none();
        recovery.wait();
        for reply in replies {
            reply.unwrap();
        }
        assert_eq!(claims, 1);
        assert!(!retry_ready);
        assert!(unresolved);

        let committed = tokio::time::timeout(Duration::from_secs(2), wait_for_admission(observer))
            .await
            .expect("targeted Stop did not resolve the original admission")
            .unwrap();
        assert_eq!(reservation.recovery_claims.load(Ordering::Relaxed), 1);
        fixture
            .assert_recovered_before_execution(&reservation, &committed)
            .await;
        fixture.coordinator.stop_service().unwrap();
        fixture.finish_stopped().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targeted_stop_reconciles_a_lost_admission_reply_without_replay() {
    let fixture = Fixture::start().await;
    fixture.coordinator.drop_next_admission_reply_for_test();
    let input = fixture.input(20, "recover lost admission reply");
    let (reservation, target) = fixture.reserve_pending(&input);
    let observer = fixture
        .coordinator
        .dispatch_reserved_admission(input, reservation.submission_hash, Arc::clone(&reservation))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !reservation.admission_retry_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lost admission reply did not reach its retry state");
    let expected = fixture
        .coordinator
        .shared
        .history
        .lookup_submission(reservation.submission_id, reservation.submission_hash)
        .await
        .unwrap()
        .expect("admission committed before its reply was lost");

    let unrelated = fixture
        .coordinator
        .register_generation_connection()
        .unwrap();
    let unrelated_target = GenerationTarget::Pending {
        boot_epoch: fixture.coordinator.boot_epoch().into(),
        pending_nonce: unrelated.nonce().into(),
    };
    fixture
        .coordinator
        .stop_generation(&unrelated_target)
        .unwrap();
    for invalid in [
        GenerationTarget::Pending {
            boot_epoch: "old-boot".into(),
            pending_nonce: match &target {
                GenerationTarget::Pending { pending_nonce, .. } => pending_nonce.clone(),
                GenerationTarget::Accepted { .. } => unreachable!(),
            },
        },
        GenerationTarget::Accepted {
            boot_epoch: fixture.coordinator.boot_epoch().into(),
            submission_id: crate::history::encode_id([21; 16]),
            operation_generation: reservation.operation_generation.to_string(),
        },
    ] {
        assert_eq!(
            fixture
                .coordinator
                .stop_generation(&invalid)
                .unwrap_err()
                .category,
            ErrorCategory::Conflict
        );
    }
    drop(unrelated);
    assert_eq!(
        fixture
            .coordinator
            .stop_generation(&unrelated_target)
            .unwrap_err()
            .category,
        ErrorCategory::NotFound
    );
    assert!(!reservation.is_cancelled());
    assert!(reservation.admission_retry_ready());
    assert_eq!(reservation.recovery_claims.load(Ordering::Relaxed), 0);
    assert!(observer.borrow().is_none());

    fixture.coordinator.stop_generation(&target).unwrap();
    let committed = tokio::time::timeout(Duration::from_secs(2), wait_for_admission(observer))
        .await
        .expect("targeted Stop did not reconcile the lost admission reply")
        .unwrap();
    assert_eq!(committed, expected);
    assert_eq!(reservation.recovery_claims.load(Ordering::Relaxed), 1);
    fixture
        .assert_recovered_before_execution(&reservation, &committed)
        .await;
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_lookup_misses_share_same_payload_and_conflict_on_changed_payload() {
    for changed_payload in [false, true] {
        let fixture = Fixture::start().await;
        let lookup = Arc::new(Barrier::new(3));
        let completion = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .set_admission_lookup_barrier_for_test(Arc::clone(&lookup), 2);
        fixture
            .coordinator
            .set_admission_completion_barrier_for_test(Arc::clone(&completion));
        let first = fixture.input(6, "same");
        let mut second = first.clone();
        if changed_payload {
            second.kind = AdmissionKind::Send {
                user_text: "different".into(),
                draft: None,
            };
        }
        let first_coordinator = fixture.coordinator.clone();
        let second_coordinator = fixture.coordinator.clone();
        let first =
            tokio::spawn(async move { first_coordinator.admit_history_generation(first).await });
        let second =
            tokio::spawn(async move { second_coordinator.admit_history_generation(second).await });
        lookup.wait();
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        let mut observers = Vec::new();
        for result in [first, second] {
            match result {
                Ok(observer) => observers.push(observer),
                Err(error) => {
                    assert!(changed_payload);
                    assert_eq!(error.category, ErrorCategory::Conflict);
                }
            }
        }
        assert_eq!(observers.len(), if changed_payload { 1 } else { 2 });
        completion.wait();
        fixture.coordinator.stop_service().unwrap();
        completion.wait();
        let mut committed = Vec::new();
        for observer in observers {
            committed.push(wait_for_admission(observer).await.unwrap());
        }
        if !changed_payload {
            assert_eq!(committed[0], committed[1]);
        }
        fixture.finish_stopped().await;
        let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
        let attempts: i64 = connection
            .query_row("SELECT COUNT(*) FROM attempts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(attempts, 1);
        connection.close().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_admission_hands_off_before_publish_and_stop_keeps_the_fence() {
    let fixture = Fixture::start().await;
    let handoff = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_output_handoff_barrier_for_test(Arc::clone(&handoff));
    let observer = fixture
        .coordinator
        .admit_history_generation(fixture.input(8, "atomic handoff"))
        .await
        .unwrap();
    handoff.wait();

    assert!(fixture.coordinator.admission_active_for_test());
    let (mutation, permit) = fixture
        .coordinator
        .history(HistoryCommand::RenameConversation {
            conversation_id: fixture.conversation_id.clone(),
            expected_revision: "2".into(),
            title: "blocked".into(),
        })
        .await;
    drop(permit);
    assert_eq!(mutation.unwrap_err().category, ErrorCategory::Busy);
    assert_eq!(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(9, "competing"))
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Busy
    );
    fixture.coordinator.stop_service().unwrap();
    handoff.wait();
    let committed = wait_for_admission(observer).await.unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    assert!(output.is_cancelled());
    let terminal = Arc::new(final_input(&committed, 0, "", ExecutionOutcome::Stopped));
    let result = wait_for_output(output.finalize(terminal).unwrap())
        .await
        .unwrap();
    assert_eq!(result.end, 0);
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.finish_stopped().await;
    assert_terminalized(&fixture.root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_owner_orders_pending_final_and_survives_dropped_observer() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(10, "persist output"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .stall_history_for_test(Arc::clone(&barrier));
    barrier.wait();

    let checkpoint = Arc::new(suffix_input(&committed, 0, "saved"));
    let finalization = Arc::new(final_input(&committed, 5, "!", ExecutionOutcome::Completed));
    let checkpoint_observer = output.checkpoint(Arc::clone(&checkpoint)).unwrap();
    let final_observer = output.finalize(Arc::clone(&finalization)).unwrap();
    drop(checkpoint_observer);
    assert_eq!(
        output
            .checkpoint(Arc::new(suffix_input(&committed, 6, "extra")))
            .unwrap_err()
            .category,
        ErrorCategory::Busy
    );
    barrier.wait();
    assert_eq!(wait_for_output(final_observer).await.unwrap().end, 6);
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::Saved { saved_end: 6 }
    );
    assert_eq!(Arc::strong_count(&checkpoint), 1);
    assert_eq!(Arc::strong_count(&finalization), 1);
    assert!(!fixture.coordinator.admission_active_for_test());

    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_output_cancels_and_retry_reuses_the_exact_retained_suffix() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(11, "retry output"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let suffix = Arc::new(suffix_input(&committed, 0, "durable"));
    fixture.coordinator.drop_next_persistence_reply_for_test();
    assert!(
        wait_for_output(output.checkpoint(Arc::clone(&suffix)).unwrap())
            .await
            .is_err()
    );
    assert!(output.is_cancelled());
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::SaveFailed { saved_end: 0 }
    );
    assert_eq!(Arc::strong_count(&suffix), 2);

    assert_eq!(
        wait_for_output(output.retry_save().unwrap())
            .await
            .unwrap()
            .end,
        7
    );
    assert_eq!(Arc::strong_count(&suffix), 1);
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::Open { saved_end: 7 }
    );
    let terminal = Arc::new(final_input(&committed, 7, "", ExecutionOutcome::Stopped));
    assert_eq!(
        wait_for_output(output.finalize(terminal).unwrap())
            .await
            .unwrap()
            .end,
        7
    );
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_checkpoint_retains_a_later_stopped_terminal_for_exact_retry() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(15, "retain stopped terminal"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let checkpoint = Arc::new(suffix_input(&committed, 0, "saved"));
    fixture.coordinator.drop_next_persistence_reply_for_test();
    assert!(
        wait_for_output(output.checkpoint(Arc::clone(&checkpoint)).unwrap())
            .await
            .is_err()
    );

    let terminal = Arc::new(final_input(
        &committed,
        5,
        "tail",
        ExecutionOutcome::Stopped,
    ));
    let final_observer = output.finalize(Arc::clone(&terminal)).unwrap();
    assert_eq!(Arc::strong_count(&checkpoint), 2);
    assert_eq!(Arc::strong_count(&terminal), 2);
    assert_eq!(
        wait_for_output(output.retry_save().unwrap())
            .await
            .unwrap()
            .end,
        5
    );
    assert_eq!(wait_for_output(final_observer).await.unwrap().end, 9);
    assert_eq!(Arc::strong_count(&checkpoint), 1);
    assert_eq!(Arc::strong_count(&terminal), 1);
    assert!(!fixture.coordinator.admission_active_for_test());

    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_output_envelope_cancels_then_drains_one_large_event_exactly() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(16, "large event"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let mut pipeline = OutputPipeline::new(output.clone(), committed.clone());
    let content = "é".repeat(300_000);
    let body = format!(
        "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{content}\"}}}}]}}\n\n"
    );
    let mut decoder = SseDecoder::new();
    let first = decoder.push(body.as_bytes()).unwrap().chunk.unwrap();

    fixture.coordinator.drop_next_persistence_reply_for_test();
    let barrier = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .stall_history_for_test(Arc::clone(&barrier));
    barrier.wait();
    pipeline.save_chunk(first).unwrap();
    let second = decoder.push(&[]).unwrap().chunk.unwrap();
    assert!(matches!(
        pipeline.save_chunk(second),
        Err(SaveChunkFailure::Failed)
    ));
    assert!(output.is_cancelled());
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::SaveFailed { saved_end: 0 }
    );
    barrier.wait();

    assert_eq!(
        pipeline
            .finish(&mut decoder, ExecutionOutcome::Failed, Some("output_save"),)
            .await,
        ExecutionOutcome::Failed
    );
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<i64>, Option<i64>, Option<String>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end, failure_code FROM attempts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    let length = i64::try_from(content.len()).unwrap();
    assert_eq!(
        terminal,
        (
            3,
            1,
            length,
            Some(length),
            Some(length),
            Some("output_save".into()),
        )
    );
    let saved = {
        let mut statement = connection
            .prepare("SELECT content FROM attempt_chunks ORDER BY start_offset")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .concat()
    };
    assert_eq!(saved, content);
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_checkpoint_with_pending_completed_final_retries_before_stop_drains_history() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(14, "retry pending final"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let checkpoint = Arc::new(suffix_input(&committed, 0, "saved"));
    let finalization = Arc::new(final_input(&committed, 5, "!", ExecutionOutcome::Completed));

    fixture.coordinator.drop_next_persistence_reply_for_test();
    let barrier = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .stall_history_for_test(Arc::clone(&barrier));
    barrier.wait();
    let checkpoint_observer = output.checkpoint(Arc::clone(&checkpoint)).unwrap();
    let final_observer = output.finalize(Arc::clone(&finalization)).unwrap();
    fixture.coordinator.stop_service().unwrap();
    barrier.wait();
    assert!(wait_for_output(checkpoint_observer).await.is_err());

    assert!(fixture.coordinator.admission_active_for_test());
    assert_ne!(
        *fixture.coordinator.history_exit_receiver().borrow(),
        HistoryExit::Drained
    );
    assert_eq!(Arc::strong_count(&checkpoint), 2);
    assert_eq!(Arc::strong_count(&finalization), 2);

    assert_eq!(
        wait_for_output(output.retry_save().unwrap())
            .await
            .unwrap()
            .end,
        5
    );
    assert_eq!(wait_for_output(final_observer).await.unwrap().end, 6);
    assert_eq!(Arc::strong_count(&checkpoint), 1);
    assert_eq!(Arc::strong_count(&finalization), 1);
    assert!(!fixture.coordinator.admission_active_for_test());
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::Saved { saved_end: 6 }
    );
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end FROM attempts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal, (1, 1, 6, Some(6), Some(6)));
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_completed_final_survives_a_later_stop_and_closes_old_handles() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(12, "completed before stop"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .stall_history_for_test(Arc::clone(&barrier));
    barrier.wait();

    let terminal = Arc::new(final_input(
        &committed,
        0,
        "complete",
        ExecutionOutcome::Completed,
    ));
    let observer = output.finalize(Arc::clone(&terminal)).unwrap();
    fixture.coordinator.stop_service().unwrap();
    assert!(output.is_cancelled());
    barrier.wait();
    assert_eq!(wait_for_output(observer).await.unwrap().end, 8);
    assert!(!fixture.coordinator.admission_active_for_test());
    assert_eq!(Arc::strong_count(&terminal), 1);
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::Saved { saved_end: 8 }
    );
    assert_eq!(
        output
            .checkpoint(Arc::new(suffix_input(&committed, 8, "late")))
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        output
            .finalize(Arc::new(final_input(
                &committed,
                8,
                "",
                ExecutionOutcome::Stopped,
            )))
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        output.retry_save().unwrap_err().category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        fixture
            .coordinator
            .generation_output(&committed)
            .err()
            .unwrap()
            .category,
        ErrorCategory::Conflict
    );
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end FROM attempts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal, (1, 1, 8, Some(8), Some(8)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_winning_before_output_intent_rejects_more_completed_content() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(13, "stop first"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    fixture.coordinator.stop_service().unwrap();

    assert_eq!(
        output
            .checkpoint(Arc::new(suffix_input(&committed, 0, "late")))
            .unwrap_err()
            .category,
        ErrorCategory::ServiceUnavailable
    );
    assert_eq!(
        output
            .finalize(Arc::new(final_input(
                &committed,
                0,
                "late",
                ExecutionOutcome::Completed,
            )))
            .unwrap_err()
            .category,
        ErrorCategory::ServiceUnavailable
    );
    assert_eq!(
        wait_for_output(
            output
                .finalize(Arc::new(final_input(
                    &committed,
                    0,
                    "",
                    ExecutionOutcome::Stopped,
                )))
                .unwrap(),
        )
        .await
        .unwrap()
        .end,
        0
    );
    fixture.finish_stopped().await;
    assert_terminalized(&fixture.root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_terminal_atomically_normalizes_completion_when_stop_wins() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(17, "stop at terminal"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    fixture.coordinator.stop_service().unwrap();

    let input = Arc::new(final_input(
        &committed,
        0,
        "saved",
        ExecutionOutcome::Completed,
    ));
    let (observer, selected) = output
        .finalize_generation_owned(Arc::clone(&input))
        .unwrap();
    assert_eq!(selected, ExecutionOutcome::Stopped);
    assert_eq!(wait_for_output(observer).await.unwrap().end, 5);
    assert_eq!(Arc::strong_count(&input), 1);
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<String>) = connection
        .query_row(
            "SELECT execution_outcome, saved_end, generated_end, failure_code FROM attempts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(terminal, (2, 5, 5, Some("stopped".into())));
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_before_checkpoint_handoff_stays_stopped_and_saves_the_observed_tail() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(18, "stop before checkpoint"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let mut pipeline = OutputPipeline::new(output, committed);
    let mut decoder = SseDecoder::new();
    decoder
        .push(b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"saved\"}}]}\n\n")
        .unwrap();
    let tail = decoder.take_remaining_chunk().unwrap();
    fixture.coordinator.stop_service().unwrap();
    assert!(matches!(
        pipeline.save_chunk(tail),
        Err(SaveChunkFailure::Cancelled)
    ));
    assert_eq!(
        pipeline
            .finish(&mut decoder, ExecutionOutcome::Stopped, Some("stopped"),)
            .await,
        ExecutionOutcome::Stopped
    );
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<String>, String) = connection
        .query_row(
            "SELECT a.execution_outcome, a.saved_end, a.generated_end, a.failure_code, c.content
             FROM attempts a JOIN attempt_chunks c ON c.attempt_id = a.id",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal, (2, 5, 5, Some("stopped".into()), "saved".into()));
    connection.close().unwrap();
}

async fn wait_for_admission(mut observer: AdmissionObserver) -> AdmissionResult {
    loop {
        if let Some(result) = observer.borrow().clone() {
            return result;
        }
        observer
            .changed()
            .await
            .expect("admission owner retained the result");
    }
}

async fn wait_for_output(
    mut observer: OutputObserver,
) -> Result<SuffixCommit, loxa_ipc::ServiceError> {
    loop {
        if let Some(result) = observer.borrow().clone() {
            return result;
        }
        observer
            .changed()
            .await
            .expect("output owner retained the result");
    }
}

fn suffix_input(committed: &CommittedAdmission, start: u64, content: &str) -> SuffixInput {
    SuffixInput {
        attempt_id: committed.attempt_id,
        owner_epoch: "test-service-boot".into(),
        operation_generation: committed.operation_generation,
        expected_saved_end: start,
        content: content.into(),
    }
}

fn final_input(
    committed: &CommittedAdmission,
    start: u64,
    content: &str,
    execution_outcome: ExecutionOutcome,
) -> FinalizationInput {
    FinalizationInput {
        suffix: suffix_input(committed, start, content),
        execution_outcome,
        generated_end: start + content.len() as u64,
        failure_code: None,
    }
}

async fn wait_for_history_ready(coordinator: &Coordinator) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while coordinator.history_status().phase == HistoryPhase::Opening
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(coordinator.history_status().phase, HistoryPhase::Ready);
}

fn assert_terminalized(root: &std::path::Path) {
    let connection = rusqlite::Connection::open(root.join("app.sqlite")).unwrap();
    let terminal: (i64, i64, i64, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end FROM attempts",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal, (2, 1, 0, Some(0), Some(0)));
    connection.close().unwrap();
}

fn model_manifest() -> Manifest {
    Manifest {
        version: 2,
        id: "demo".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(Origin::Local),
        source_filename: Some("source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "b83633aa785344791618f2fddf131b010ea04912a60430760b070bad293f65bd".into(),
        size: 4,
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn install_model(models: &std::path::Path) {
    let model_dir = models.join("demo");
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.gguf"), b"GGUF").unwrap();
    crate::catalog::publish_manifest(models, &model_manifest()).unwrap();
}

fn decode_id(value: &str) -> [u8; 16] {
    let mut id = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        id[index] = (hex(pair[0]) << 4) | hex(pair[1]);
    }
    id
}

fn hex(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => panic!("invalid fixture identity"),
    }
}
