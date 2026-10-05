use super::*;
use futures_util::StreamExt;
use loxa_ipc::{GenerationExecutionPhase as Execution, GenerationSavePhase as Save};
use tokio::net::UnixStream;

#[tokio::test(flavor = "current_thread")]
async fn metadata_watch_coalesces_terminal_release_without_retaining_output() {
    let fixture = Fixture::start().await;
    let committed = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(40, "observe exact attempt"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let output = fixture.coordinator.generation_output(&committed).unwrap();
    let reservation = fixture
        .coordinator
        .shared
        .state()
        .current_admission()
        .unwrap();
    let weak = Arc::downgrade(&reservation);
    let target = GenerationTarget::Accepted {
        boot_epoch: committed.owner_epoch.clone(),
        submission_id: crate::history::encode_id(committed.submission_id),
        operation_generation: committed.operation_generation.to_string(),
    };
    let attempt_id = crate::history::encode_id(committed.attempt_id);
    let mut statuses = fixture
        .coordinator
        .subscribe_generation_status(&target, &attempt_id)
        .unwrap()
        .unwrap();
    let initial = statuses.borrow_and_update().clone().unwrap();
    assert_eq!(initial.execution, Execution::Working);
    assert_eq!(initial.save, Save::Open);

    let terminal = output
        .finalize(Arc::new(final_input(
            &committed,
            0,
            "",
            ExecutionOutcome::Stopped,
        )))
        .unwrap();
    wait_for_output(terminal).await.unwrap();
    assert!(!fixture.coordinator.admission_active_for_test());
    let final_status = statuses.borrow_and_update().clone().unwrap();
    assert_eq!(final_status.execution, Execution::Stopped);
    assert_eq!(final_status.save, Save::Saved);
    assert_eq!(final_status.terminal_saved_end.as_deref(), Some("0"));

    drop(output);
    drop(reservation);
    assert!(weak.upgrade().is_none());
    assert_eq!(statuses.borrow().as_ref(), Some(&final_status));
    let mut old_boot = target.clone();
    let GenerationTarget::Accepted { boot_epoch, .. } = &mut old_boot else {
        unreachable!()
    };
    *boot_epoch = "old-service-boot".into();
    assert!(fixture
        .coordinator
        .subscribe_generation_status(&old_boot, &attempt_id)
        .unwrap()
        .is_none());
    let (durable, permit) = fixture
        .coordinator
        .history(HistoryCommand::GetAttempt {
            attempt_id: attempt_id.clone(),
        })
        .await;
    drop(permit);
    assert!(matches!(
        durable.unwrap(),
        HistoryReply::Attempt(attempt) if attempt.id == attempt_id
    ));
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

#[tokio::test(flavor = "current_thread")]
async fn socket_observation_binds_durable_attempt_to_its_exact_generation_owner() {
    let fixture = Fixture::start().await;
    let first = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(fixture.input(43, "first observed attempt"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let first_output = fixture.coordinator.generation_output(&first).unwrap();
    let first_target = accepted_target(&first);
    let first_attempt = crate::history::encode_id(first.attempt_id);
    let (mut live, live_task) = observation_socket(
        fixture.coordinator.clone(),
        first_target.clone(),
        first_attempt.clone(),
    );
    assert!(matches!(
        receive_observation(&mut live).await,
        loxa_ipc::ServerEnvelope::GenerationSnapshot {
            observation: loxa_ipc::GenerationObservation::Live { status }
        } if status.execution == Execution::Working && status.save == Save::Open
    ));

    let terminal = first_output
        .finalize(Arc::new(final_input(
            &first,
            0,
            "",
            ExecutionOutcome::Stopped,
        )))
        .unwrap();
    wait_for_output(terminal).await.unwrap();
    receive_terminal_live(&mut live, &first_target, &first_attempt).await;
    assert!(matches!(
        receive_observation(&mut live).await,
        loxa_ipc::ServerEnvelope::GenerationSnapshot {
            observation: loxa_ipc::GenerationObservation::Durable { attempt }
        } if attempt.id == first_attempt
    ));
    live_task.await.unwrap().unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), live.next())
        .await
        .unwrap()
        .is_none());

    let mut second_input = fixture.input(44, "second observed attempt");
    second_input.expected_conversation_revision = first.post_conversation_revision;
    second_input.expected_profile_revision = first.profile_revision;
    let second = wait_for_admission(
        fixture
            .coordinator
            .admit_history_generation(second_input)
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let second_output = fixture.coordinator.generation_output(&second).unwrap();
    let second_target = accepted_target(&second);
    let second_attempt = crate::history::encode_id(second.attempt_id);
    let terminal = second_output
        .finalize(Arc::new(final_input(
            &second,
            0,
            "",
            ExecutionOutcome::Stopped,
        )))
        .unwrap();
    wait_for_output(terminal).await.unwrap();

    let (mut mismatched, mismatched_task) = observation_socket(
        fixture.coordinator.clone(),
        first_target,
        second_attempt.clone(),
    );
    assert!(matches!(
        receive_observation(&mut mismatched).await,
        loxa_ipc::ServerEnvelope::Reply(loxa_ipc::Reply {
            outcome: loxa_ipc::ReplyOutcome::Rejected(loxa_ipc::ServiceError {
                category: ErrorCategory::NotFound,
                ..
            }),
            ..
        })
    ));
    mismatched_task.await.unwrap().unwrap();

    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;

    let bootstrap = loxa_ipc::ClientBootstrap::load(&fixture.root, None).unwrap();
    let paths = crate::paths::AppPaths::from_values(Some(&fixture.root), None).unwrap();
    let ownership = crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&paths.run)
        .unwrap_or_else(|_| panic!("reacquire runtime ownership for observation reopen"));
    let reopened = Coordinator::start(
        paths,
        bootstrap.root().control_dir().to_owned(),
        bootstrap.root().root_identity().to_owned(),
        "test-machine-boot".into(),
        "replacement-service-boot".into(),
        ownership,
        tokio::runtime::Handle::current(),
        None,
        None,
    )
    .unwrap();
    wait_for_history_ready(&reopened).await;
    let (mut old_boot, old_boot_task) =
        observation_socket(reopened.clone(), second_target, second_attempt.clone());
    assert!(matches!(
        receive_observation(&mut old_boot).await,
        loxa_ipc::ServerEnvelope::GenerationSnapshot {
            observation: loxa_ipc::GenerationObservation::Durable { attempt }
        } if attempt.id == second_attempt
    ));
    old_boot_task.await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), old_boot.next())
            .await
            .unwrap()
            .is_none()
    );
    reopened.stop_service().unwrap();
    finish_stopped_coordinator(&reopened).await;
}

fn accepted_target(committed: &CommittedAdmission) -> GenerationTarget {
    GenerationTarget::Accepted {
        boot_epoch: committed.owner_epoch.clone(),
        submission_id: crate::history::encode_id(committed.submission_id),
        operation_generation: committed.operation_generation.to_string(),
    }
}

fn observation_socket(
    coordinator: Coordinator,
    target: GenerationTarget,
    attempt_id: String,
) -> (
    loxa_ipc::IpcFramed,
    tokio::task::JoinHandle<Result<(), String>>,
) {
    let (client, server) = UnixStream::pair().unwrap();
    let task = tokio::spawn(async move {
        crate::service::server::stream_generation_observation_for_test(
            loxa_ipc::framed(server),
            &coordinator,
            target,
            attempt_id,
        )
        .await
    });
    (loxa_ipc::framed(client), task)
}

async fn receive_observation(transport: &mut loxa_ipc::IpcFramed) -> loxa_ipc::ServerEnvelope {
    let frame = tokio::time::timeout(Duration::from_secs(1), transport.next())
        .await
        .expect("observation response timed out")
        .expect("observation transport closed")
        .expect("observation frame failed");
    loxa_ipc::decode_with_limit(&frame, loxa_ipc::MAX_HISTORY_FRAME_BYTES)
        .expect("observation response was invalid")
}

async fn receive_terminal_live(
    transport: &mut loxa_ipc::IpcFramed,
    target: &GenerationTarget,
    attempt_id: &str,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut saved_end = 0_u64;
        loop {
            let frame = transport
                .next()
                .await
                .expect("observation transport closed before terminal status")
                .expect("terminal observation frame failed");
            let envelope: loxa_ipc::ServerEnvelope =
                loxa_ipc::decode_with_limit(&frame, loxa_ipc::MAX_HISTORY_FRAME_BYTES)
                    .expect("terminal observation response was invalid");
            let loxa_ipc::ServerEnvelope::GenerationSnapshot {
                observation: loxa_ipc::GenerationObservation::Live { status },
            } = envelope
            else {
                panic!("expected a live terminal observation")
            };
            assert_eq!(&status.target, target);
            assert_eq!(status.attempt_id, attempt_id);
            assert_eq!(status.execution, Execution::Stopped);
            assert!(matches!(status.save, Save::Saving | Save::Saved));
            let current_saved_end = status.saved_end.parse::<u64>().unwrap();
            assert!(current_saved_end >= saved_end);
            saved_end = current_saved_end;
            let generated_end = status
                .generated_end
                .as_deref()
                .map(str::parse::<u64>)
                .transpose()
                .unwrap();
            assert!(generated_end.is_none_or(|end| end >= saved_end));
            if status.save == Save::Saved {
                assert_eq!(generated_end, Some(saved_end));
                assert_eq!(
                    status.terminal_saved_end.as_deref(),
                    Some(status.saved_end.as_str())
                );
                return;
            }
            assert_eq!(status.terminal_saved_end, None);
        }
    })
    .await
    .expect("terminal observation timed out");
}

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
