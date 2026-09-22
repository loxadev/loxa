use super::*;
use crate::service::coordinator::state::CancellationCause;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_failure_before_output_reconciles_lost_final_reply_and_fences_replacement() {
    let fixture = Fixture::start().await;
    let completion = Arc::new(Barrier::new(2));
    fixture
        .coordinator
        .set_admission_completion_barrier_for_test(Arc::clone(&completion));
    let input = fixture.input(43, "engine exits after commit");
    let observer = fixture
        .coordinator
        .admit_history_generation(input.clone())
        .await
        .unwrap();
    completion.wait();
    let release = CompletionRelease(completion);
    let reservation = fixture
        .coordinator
        .shared
        .state()
        .current_admission()
        .unwrap();
    fixture
        .coordinator
        .shared
        .state()
        .cancel_generation_for_engine_failure(&fixture.operation);
    fixture.coordinator.drop_next_persistence_reply_for_test();
    assert_eq!(
        reservation.cancellation_cause(),
        Some(CancellationCause::EngineFailure)
    );
    assert!(reservation.output.lock().unwrap().is_none());
    drop(release);

    tokio::time::timeout(Duration::from_secs(2), async {
        while !reservation.stop_retry_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lost pre-execution failure save did not retain recovery ownership");
    let first_statistics = attempt_statistics_row(&fixture.root);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(observer.borrow().is_none());
    assert!(fixture.coordinator.admission_active_for_test());
    let retry = fixture
        .coordinator
        .admit_history_generation(input)
        .await
        .unwrap();
    let committed = wait_for_admission(retry).await.unwrap();
    assert_eq!(wait_for_admission(observer).await.unwrap(), committed);
    assert!(!fixture.coordinator.admission_active_for_test());
    assert!(reservation.output.lock().unwrap().is_none());
    assert_eq!(
        reservation
            .recovery_claims
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    let mut replacement = fixture.input(44, "replacement must wait for cleanup");
    replacement.expected_conversation_revision = committed.post_conversation_revision;
    assert_eq!(
        fixture
            .coordinator
            .admit_history_generation(replacement)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::ServiceUnavailable
    );
    assert!(matches!(
        fixture.coordinator.status().phase,
        loxa_ipc::RuntimePhase::Ready { .. }
    ));
    fixture
        .coordinator
        .shared
        .state()
        .complete(&fixture.operation, None);
    assert!(matches!(
        fixture.coordinator.status().phase,
        loxa_ipc::RuntimePhase::Unloaded
    ));
    assert_eq!(
        reservation.cancellation_cause(),
        Some(CancellationCause::EngineFailure)
    );
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;

    let connection = rusqlite::Connection::open(fixture.root.join("app.sqlite")).unwrap();
    let finalization: (i64, i64, i64, i64, i64, String) = connection.query_row(
        "SELECT execution_outcome, save_outcome, saved_end, generated_end, terminal_saved_end, failure_code
         FROM attempts WHERE id = ?1", [committed.attempt_id.as_slice()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    ).unwrap();
    assert_eq!(finalization, (3, 1, 0, 0, 0, "engine_transport".into()));
    assert_eq!(attempt_statistics_row(&fixture.root), first_statistics);
    assert_eq!(first_statistics.5, 4);
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM attempts), (SELECT COUNT(*) FROM attempt_chunks)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 0));
    connection.close().unwrap();
}

struct CompletionRelease(Arc<Barrier>);

impl Drop for CompletionRelease {
    fn drop(&mut self) {
        self.0.wait();
    }
}
