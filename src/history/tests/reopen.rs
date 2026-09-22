use super::*;
use loxa_ipc::{AttemptExecution, AttemptSave, AttemptStopReason, AttemptSummary, ContentSource};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_reopen_recovers_the_committed_utf8_prefix_and_preserves_stopped_output() {
    let (_directory, root) = private_root("loxa-history-owner-reopen-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let owner = open_owner(&root, &models, "boot-1").await;
    let handle = owner.handle();

    let (stopped_conversation, stopped) = admit(&handle, 41).await;
    let finalization = handle
        .try_finalize(Arc::new(FinalizationInput {
            suffix: suffix_input(&stopped, 41, 0, "止🙂"),
            execution_outcome: ExecutionOutcome::Stopped,
            generated_end: 7,
            failure_code: Some("stopped".into()),
            statistics: Some(AttemptStatistics {
                qualified_input_tokens: Some(17),
                qualified_output_tokens: Some(2),
                service_first_output_latency_ms: Some(3),
                qualified_engine_decode_tokens_per_second: Some(25.0),
                service_total_duration_ms: 9,
                stop_reason: AttemptStopReason::UserStop,
            }),
        }))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(finalization.result.unwrap().end, 7);
    drop(finalization.permit);
    let stopped_before = selected_attempt(&handle, &stopped_conversation).await;
    assert_eq!(stopped_before.execution, AttemptExecution::Stopped);
    assert_eq!(stopped_before.save, AttemptSave::Saved);
    assert_eq!(stopped_before.failure_code.as_deref(), Some("stopped"));
    assert_eq!(stopped_before.saved_end, "7");
    assert_eq!(stopped_before.generated_end.as_deref(), Some("7"));
    assert_eq!(stopped_before.terminal_saved_end.as_deref(), Some("7"));
    let statistics = stopped_before.statistics.as_ref().unwrap();
    assert_eq!(statistics.qualified_input_tokens, Some(17));
    assert_eq!(statistics.qualified_output_tokens, Some(2));
    assert_eq!(
        statistics.service_first_output_latency_ms.as_deref(),
        Some("3")
    );
    assert_eq!(
        statistics
            .qualified_engine_decode_tokens_per_second
            .unwrap()
            .get(),
        25.0
    );
    assert_eq!(statistics.service_total_duration_ms, "9");
    assert_eq!(statistics.stop_reason, AttemptStopReason::UserStop);

    let (pending_conversation, pending) = admit(&handle, 42).await;
    let checkpoint = handle
        .try_append_suffix(Arc::new(suffix_input(&pending, 42, 0, "é🙂")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(checkpoint.result.unwrap().end, 6);
    drop(checkpoint.permit);
    let pending_before = selected_attempt(&handle, &pending_conversation).await;
    assert_eq!(pending_before.execution, AttemptExecution::Pending);
    assert_eq!(pending_before.save, AttemptSave::Open);
    assert_eq!(pending_before.saved_end, "6");
    assert_eq!(pending_before.generated_end, None);
    assert_eq!(pending_before.terminal_saved_end, None);
    // Close the real owner without a finalization intent. Startup recovery on
    // the next owner must handle this retained attempt, not a test SQL helper.
    drain(owner);

    let mut recovered_before = None;
    for epoch in ["boot-2", "boot-3"] {
        let owner = open_owner(&root, &models, epoch).await;
        let handle = owner.handle();
        let recovered = selected_attempt(&handle, &pending_conversation).await;
        assert_eq!(recovered.id, pending_before.id);
        assert_eq!(recovered.execution, AttemptExecution::Interrupted);
        assert_eq!(recovered.save, AttemptSave::Interrupted);
        assert_eq!(recovered.saved_end, "6");
        assert_eq!(recovered.generated_end, None);
        assert_eq!(recovered.terminal_saved_end, None);
        assert_eq!(recovered.failure_code, None);
        if let Some(previous) = &recovered_before {
            assert_eq!(
                &recovered, previous,
                "second reopen changed recovered facts"
            );
        }
        assert_prefix(&handle, &recovered, "é🙂").await;
        recovered_before = Some(recovered);

        let stopped_after = selected_attempt(&handle, &stopped_conversation).await;
        assert_eq!(stopped_after, stopped_before);
        assert_prefix(&handle, &stopped_after, "止🙂").await;
        drain(owner);
    }
}

async fn open_owner(root: &Path, models: &Path, epoch: &str) -> HistoryOwner {
    let owner = HistoryOwner::start(
        root,
        models.to_owned(),
        RuntimeIdentity::BundledB10344,
        Arc::new(AtomicBool::new(false)),
        epoch.into(),
    )
    .unwrap();
    let mut status = owner.handle().status.subscribe();
    tokio::time::timeout(Duration::from_secs(2), async {
        while status.borrow().phase == HistoryPhase::Opening {
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("history owner did not finish startup");
    assert_eq!(status.borrow().phase, HistoryPhase::Ready);
    owner
}

fn drain(owner: HistoryOwner) {
    let handle = owner.handle();
    handle.begin_drain();
    owner.join().unwrap();
    assert_eq!(*handle.exit_receiver().borrow(), HistoryExit::Drained);
}

async fn history(handle: &HistoryHandle, command: WireCommand) -> HistoryReply {
    let completion = handle.execute(command).await.unwrap();
    let reply = completion.result.unwrap();
    drop(completion.permit);
    reply
}

async fn admit(handle: &HistoryHandle, submission: u8) -> (String, CommittedAdmission) {
    let HistoryReply::Conversation(conversation) = history(
        handle,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
    )
    .await
    else {
        panic!("create returned the wrong history reply");
    };
    let prepared = Arc::new(prepared_admission(
        identity::decode_id(&conversation.id).unwrap(),
        1,
        submission,
        submission,
        i64::from(submission),
        AdmissionKind::Send {
            user_text: "question".into(),
            draft: None,
        },
    ));
    let admitted = handle.try_admit(prepared).unwrap().await.unwrap();
    let committed = admitted.result.unwrap();
    drop(admitted.permit);
    (conversation.id, committed)
}

async fn selected_attempt(handle: &HistoryHandle, conversation_id: &str) -> AttemptSummary {
    let HistoryReply::TurnPage(mut page) = history(
        handle,
        WireCommand::ListTurns {
            conversation_id: conversation_id.into(),
            cursor: None,
            limit: 2,
        },
    )
    .await
    else {
        panic!("list turns returned the wrong history reply");
    };
    assert_eq!(page.turns.len(), 1);
    assert!(page.next.is_none());
    page.turns.pop().unwrap().selected_attempt.unwrap()
}

async fn assert_prefix(handle: &HistoryHandle, attempt: &AttemptSummary, expected: &str) {
    let HistoryReply::ContentRange(range) = history(
        handle,
        WireCommand::ReadContentRange {
            source: ContentSource::Assistant {
                attempt_id: attempt.id.clone(),
            },
            start: "0".into(),
            prefix_end: attempt.saved_end.clone(),
        },
    )
    .await
    else {
        panic!("read content returned the wrong history reply");
    };
    assert_eq!(range.start, "0");
    assert_eq!(range.end, expected.len().to_string());
    assert_eq!(range.prefix_end, range.end);
    assert_eq!(range.content, expected);
}
