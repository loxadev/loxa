use super::*;
use loxa_ipc::{GenerationExecutionPhase as Execution, GenerationSavePhase as Save};

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
