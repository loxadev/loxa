use super::*;
use loxa_ipc::{GenerationExecutionPhase as Execution, GenerationSavePhase as Save};

#[tokio::test(flavor = "current_thread")]
async fn full_relay_engine_failure_preserves_prefix_and_remains_failed_after_cleanup() {
    let mut relay = StreamingFixture::start_full_run().await;
    relay.start_body().await;
    let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
    relay.delta("é🙂").await;
    relay
        .advance_to_checkpoint(Duration::from_millis(250), 1)
        .await;
    relay
        .fixture
        .coordinator
        .shared
        .state()
        .cancel_generation_for_engine_failure(&relay.fixture.operation);
    relay.assert_transport_closed().await;
    let snapshot = relay
        .wait_status(|snapshot| snapshot.execution == Execution::Finalizing)
        .await;
    assert_eq!(snapshot.failure_code.as_deref(), Some("engine_transport"));
    assert_eq!(snapshot.save, Save::Saving);
    assert_eq!(snapshot.generated_end.as_deref(), Some("6"));
    assert!(relay.fixture.coordinator.admission_active_for_test());
    relay.complete_synthetic_cleanup().await;
    let snapshot = relay.snapshot();
    assert_eq!(snapshot.execution, Execution::Failed);
    assert_eq!(snapshot.failure_code.as_deref(), Some("engine_transport"));
    assert!(relay.fixture.coordinator.admission_active_for_test());
    stalled.release();
    relay
        .finish(ExecutionOutcome::Failed, Some("engine_transport"), "é🙂", 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_relay_stop_before_headers_retains_admission_until_cleanup() {
    let mut relay = StreamingFixture::start_full_run().await;
    relay
        .fixture
        .coordinator
        .stop_generation(&relay.target())
        .unwrap();
    relay.assert_transport_closed().await;
    let snapshot = relay
        .wait_status(|snapshot| snapshot.save == Save::Saved)
        .await;
    assert_eq!(snapshot.execution, Execution::Finalizing);
    assert_eq!(snapshot.saved_end, "0");
    assert_eq!(snapshot.generated_end.as_deref(), Some("0"));
    assert_eq!(snapshot.terminal_saved_end.as_deref(), Some("0"));
    assert_eq!(snapshot.failure_code.as_deref(), Some("stopped"));
    assert!(relay.fixture.coordinator.admission_active_for_test());
    relay.complete_synthetic_cleanup().await;
    relay
        .finish(ExecutionOutcome::Stopped, Some("stopped"), "", 0)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_relay_stop_preserves_utf8_and_waits_for_both_save_and_cleanup() {
    for cleanup_first in [false, true] {
        let mut relay = StreamingFixture::start_full_run().await;
        relay.start_body().await;
        relay.delta("é🙂").await;
        relay
            .advance_to_checkpoint(Duration::from_millis(250), 1)
            .await;
        relay.assert_saved("é🙂").await;
        let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
        relay.delta("終").await;
        relay
            .fixture
            .coordinator
            .stop_generation(&relay.target())
            .unwrap();
        relay.assert_transport_closed().await;
        assert!(relay.fixture.coordinator.admission_active_for_test());
        assert!(!relay.task.is_finished());
        if cleanup_first {
            relay.complete_synthetic_cleanup().await;
            let snapshot = relay
                .wait_status(|snapshot| snapshot.execution == Execution::Stopped)
                .await;
            assert_eq!(snapshot.save, Save::Saving);
            assert_eq!(snapshot.saved_end, "6");
            assert_eq!(snapshot.generated_end.as_deref(), Some("9"));
            assert_eq!(snapshot.terminal_saved_end, None);
            assert!(relay.fixture.coordinator.admission_active_for_test());
            stalled.release();
        } else {
            stalled.release();
            let snapshot = relay
                .wait_status(|snapshot| snapshot.save == Save::Saved)
                .await;
            assert_eq!(snapshot.execution, Execution::Finalizing);
            assert_eq!(snapshot.saved_end, "9");
            assert_eq!(snapshot.generated_end.as_deref(), Some("9"));
            assert_eq!(snapshot.terminal_saved_end.as_deref(), Some("9"));
            assert!(relay.fixture.coordinator.admission_active_for_test());
            relay.complete_synthetic_cleanup().await;
        }
        relay
            .finish(ExecutionOutcome::Stopped, Some("stopped"), "é🙂終", 2)
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_relay_malformed_event_and_missing_done_save_the_valid_prefix_as_failed() {
    for malformed in [false, true] {
        let mut relay = StreamingFixture::start_full_run().await;
        relay.start_body().await;
        relay.delta("é").await;
        if malformed {
            relay
                .engine
                .as_mut()
                .unwrap()
                .write_all(b"data: {bad}\n\n")
                .await
                .unwrap();
        }
        relay.engine.as_mut().unwrap().shutdown().await.unwrap();
        let snapshot = relay
            .wait_status(|snapshot| snapshot.save == Save::Saved)
            .await;
        assert_eq!(snapshot.execution, Execution::Finalizing);
        assert_eq!(snapshot.failure_code.as_deref(), Some("engine_stream"));
        assert!(relay.fixture.coordinator.admission_active_for_test());
        relay.complete_synthetic_cleanup().await;
        relay
            .finish(ExecutionOutcome::Failed, Some("engine_stream"), "é", 1)
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_relay_observes_first_output_before_a_later_event_in_the_same_frame_fails() {
    let mut relay = StreamingFixture::start_full_run().await;
    relay.start_body().await;
    relay
        .engine
        .as_mut()
        .unwrap()
        .write_all(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"é\"}}]}\n\ndata: {bad}\n\n"
                .as_bytes(),
        )
        .await
        .unwrap();
    relay.engine.as_mut().unwrap().shutdown().await.unwrap();
    relay
        .wait_status(|snapshot| snapshot.save == Save::Saved)
        .await;
    relay.complete_synthetic_cleanup().await;
    relay
        .finish(ExecutionOutcome::Failed, Some("engine_stream"), "é", 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_relay_requires_valid_idle_slots_before_preserving_the_engine() {
    let idle_slots = "[{\"is_processing\":false}]";
    for (body, content_length, idle) in [
        ("{}", "{}".len(), false),
        // Parseable JSON does not make a truncated HTTP body complete.
        (idle_slots, idle_slots.len() + 1, false),
        (idle_slots, idle_slots.len(), true),
    ] {
        let mut relay = StreamingFixture::start_full_run().await;
        relay.start_body().await;
        relay.delta("é").await;
        relay.complete_engine().await;
        relay.respond_slots(body, content_length).await;
        if idle {
            relay
                .finish(ExecutionOutcome::Completed, None, "é", 1)
                .await;
        } else {
            let snapshot = relay
                .wait_status(|snapshot| snapshot.save == Save::Saved)
                .await;
            assert_eq!(snapshot.execution, Execution::Finalizing);
            assert_eq!(snapshot.failure_code.as_deref(), Some("engine_quiescence"));
            assert!(relay.fixture.coordinator.admission_active_for_test());
            relay.complete_synthetic_cleanup().await;
            relay
                .finish(ExecutionOutcome::Failed, Some("engine_quiescence"), "é", 1)
                .await;
        }
    }
}
