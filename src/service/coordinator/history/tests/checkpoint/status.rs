use super::*;
use loxa_ipc::{GenerationExecutionPhase as Execution, GenerationSavePhase as Save};

#[tokio::test(flavor = "current_thread")]
async fn terminal_suffix_rolls_back_with_terminal_metadata_before_exact_retry() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(42, "atomic terminal suffix"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let mut pipeline = OutputPipeline::new(output.clone(), committed);
    let mut decoder = SseDecoder::new();
    let step = decoder
        .push(
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tail\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n",
        )
        .unwrap();
    assert!(step.done);
    assert_eq!(decoder.generated_end(), 4);

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_terminal_attempt
             BEFORE UPDATE OF execution_outcome, save_outcome, generated_end, terminal_saved_end
             ON attempts WHEN NEW.save_outcome = 1
             BEGIN
                 SELECT RAISE(ABORT, 'terminal write blocked');
             END",
        )
        .unwrap();
    let save = tokio::spawn(async move {
        pipeline
            .finish(
                &mut decoder,
                ExecutionOutcome::Completed,
                None,
                frozen_measurements(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while output.status().unwrap() != (OutputSavePhase::SaveFailed { saved_end: 0 }) {
            tokio::task::yield_now().await
        }
    })
    .await
    .expect("terminal failure did not become observable");

    let rolled_back: (i64, i64, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT saved_end, execution_outcome, save_outcome,
                    (SELECT COUNT(*) FROM attempt_chunks),
                    (SELECT COUNT(*) FROM attempt_finalizations),
                    (SELECT COUNT(*) FROM attempt_statistics)
             FROM attempts",
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
    assert_eq!(rolled_back, (0, 0, 0, 0, 0, 0));

    connection
        .execute_batch("DROP TRIGGER reject_terminal_attempt")
        .unwrap();
    connection.close().unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), save)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        ExecutionOutcome::Completed
    );
    assert_eq!(
        output.status().unwrap(),
        OutputSavePhase::Saved { saved_end: 4 }
    );
    assert!(!fixture.coordinator.admission_active_for_test());
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "current_thread")]
async fn lost_checkpoint_reconciles_automatically_and_first_cancellation_cause_wins() {
    for stop_first in [false, true] {
        let mut relay = StreamingFixture::start().await;
        relay
            .fixture
            .coordinator
            .drop_next_persistence_reply_for_test();
        let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
        relay.delta("é🙂").await;
        relay
            .advance_to_checkpoint(Duration::from_millis(250), 1)
            .await;
        let snapshot = relay.snapshot();
        assert_eq!(snapshot.execution, Execution::Working);
        assert_eq!(snapshot.save, Save::Saving);
        assert_eq!(snapshot.saved_end, "0");
        assert_eq!(snapshot.generated_end, None);

        if stop_first {
            relay
                .fixture
                .coordinator
                .stop_generation(&relay.target())
                .unwrap();
            relay.assert_transport_closed().await;
        }
        stalled.release();
        // The first write commits but loses its reply. Hold the owner again
        // after that write, so automatic retry cannot release admission yet.
        let mut retry_stalled = HistoryStall::new(&relay.fixture.coordinator);
        relay.assert_transport_closed().await;
        let expected = if stop_first {
            Execution::Stopped
        } else {
            Execution::Failed
        };
        let snapshot = relay
            .wait_status(|snapshot| snapshot.execution == expected)
            .await;
        assert_eq!(snapshot.generated_end.as_deref(), Some("6"));
        assert_eq!(snapshot.saved_end, "0");
        assert_eq!(snapshot.terminal_saved_end, None);
        assert_eq!(
            snapshot.failure_code.as_deref(),
            Some(if stop_first { "stopped" } else { "output_save" })
        );
        assert!(matches!(snapshot.save, Save::SaveFailed | Save::Saving));
        assert!(relay.fixture.coordinator.admission_active_for_test());
        assert!(!relay.task.is_finished());

        // A later explicit Stop must not turn a storage interruption into Stop.
        relay
            .fixture
            .coordinator
            .stop_generation(&relay.target())
            .unwrap();
        retry_stalled.release();
        relay
            .finish(
                if stop_first {
                    ExecutionOutcome::Stopped
                } else {
                    ExecutionOutcome::Failed
                },
                Some(if stop_first { "stopped" } else { "output_save" }),
                "é🙂",
                1,
            )
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn completed_fact_survives_lost_final_reply_and_later_cancellation() {
    let mut relay = StreamingFixture::start().await;
    relay.delta("é").await;
    relay
        .advance_to_checkpoint(Duration::from_millis(250), 1)
        .await;
    relay.assert_saved("é").await;
    relay
        .fixture
        .coordinator
        .drop_next_persistence_reply_for_test();
    let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
    relay.complete_engine().await;

    let snapshot = relay
        .wait_status(|snapshot| snapshot.execution == Execution::Completed)
        .await;
    assert_eq!(snapshot.save, Save::Saving);
    assert_eq!(snapshot.saved_end, "2");
    assert_eq!(snapshot.generated_end.as_deref(), Some("2"));
    assert_eq!(snapshot.terminal_saved_end, None);
    assert_eq!(snapshot.failure_code, None);
    assert!(relay.fixture.coordinator.admission_active_for_test());
    relay
        .fixture
        .coordinator
        .shared
        .state()
        .cancel_generation_for_engine_failure(&relay.fixture.operation);
    relay
        .fixture
        .coordinator
        .stop_generation(&relay.target())
        .unwrap();
    stalled.release();
    // Finalization's unknown-result retry must use the original Completed fact.
    relay
        .finish(ExecutionOutcome::Completed, None, "é", 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_tail_is_saving_before_sql_and_saved_output_waits_for_engine_quiescence() {
    let mut relay = StreamingFixture::start().await;
    let reservation = relay
        .fixture
        .coordinator
        .shared
        .state()
        .current_admission()
        .unwrap();
    relay
        .fixture
        .coordinator
        .shared
        .state()
        .begin_generation_execution(&reservation)
        .unwrap();
    relay.delta("終").await;
    let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
    relay.complete_engine().await;

    let snapshot = relay
        .wait_status(|snapshot| snapshot.execution == Execution::Finalizing)
        .await;
    assert_eq!(snapshot.save, Save::Saving);
    assert_eq!(snapshot.saved_end, "0");
    assert_eq!(snapshot.generated_end.as_deref(), Some("3"));
    assert_eq!(snapshot.terminal_saved_end, None);
    assert!(relay.fixture.coordinator.admission_active_for_test());
    stalled.release();

    let snapshot = relay
        .wait_status(|snapshot| snapshot.save == Save::Saved)
        .await;
    assert_eq!(snapshot.execution, Execution::Finalizing);
    assert_eq!(snapshot.saved_end, "3");
    assert_eq!(snapshot.terminal_saved_end.as_deref(), Some("3"));
    assert!(relay.fixture.coordinator.admission_active_for_test());
    let mut wrong_target = relay.target();
    let GenerationTarget::Accepted { submission_id, .. } = &mut wrong_target else {
        unreachable!()
    };
    *submission_id = "ff".repeat(16);
    assert_eq!(
        relay
            .fixture
            .coordinator
            .generation_status(&wrong_target)
            .unwrap(),
        None
    );
    assert_eq!(
        relay
            .fixture
            .coordinator
            .generation_status(&GenerationTarget::Pending {
                boot_epoch: relay.committed.owner_epoch.clone(),
                pending_nonce: "00".repeat(16),
            })
            .unwrap_err()
            .category,
        ErrorCategory::InvalidRequest
    );

    assert!(relay
        .fixture
        .coordinator
        .shared
        .state()
        .confirm_generation_quiescence(&reservation));
    relay
        .finish(ExecutionOutcome::Completed, None, "終", 1)
        .await;
}
