use crate::control_state::state_machine::{AdmissionRequest, CommitReceipt, Transition};
use crate::control_state::worker::{
    build_reconnect_snapshot, spawn_from_repository_for_test,
    spawn_paused_from_repository_for_test, spawn_paused_with_reaper_completion_for_test,
    synthetic_queue_for_test, ControlStateError, MAX_SNAPSHOT_BYTES,
};
use crate::control_state::{ControlIdGenerator, ControlRepository};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, SlotId, StreamEpoch, V2OperationProgress, V2OperationStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::collections::HashSet;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

const NODE_ID: &str = "71111111-1111-4111-8111-111111111111";
const SLOT_ID: &str = "72222222-2222-4222-8222-222222222222";
const EPOCH: &str = "73333333-3333-4333-8333-333333333333";
const INSTANCE: &str = "74444444-4444-4444-8444-444444444444";
const INITIAL_EVENT: &str = "75555555-5555-4555-8555-555555555555";

struct InitialIds;

impl ControlIdGenerator for InitialIds {
    fn new_slot_id(&mut self) -> SlotId {
        SlotId::from_str(SLOT_ID).unwrap()
    }

    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::from_str(EPOCH).unwrap()
    }

    fn new_initial_event_id(&mut self) -> EventId {
        EventId::from_str(INITIAL_EVENT).unwrap()
    }
}

fn download(model_id: impl Into<String>) -> AdmissionRequest {
    AdmissionRequest::Download {
        model_id: model_id.into(),
        progress: V2OperationProgress {
            completed_bytes: DecimalU64::new(0),
            total_bytes: None,
        },
    }
}

struct RepositoryFixturePath {
    _root: crate::control_state::state_machine::test_support::storage::TestRoot,
    path: PathBuf,
}

impl Deref for RepositoryFixturePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

fn repository() -> (RepositoryFixturePath, ControlRepository) {
    let root = crate::control_state::state_machine::test_support::storage::TestRoot::new("worker");
    let path = root.path().join("control-state.sqlite3");
    let mut repository = ControlRepository::open_or_create(
        &path,
        NodeId::from_str(NODE_ID).unwrap(),
        &mut InitialIds,
    )
    .unwrap();
    repository
        .transaction(|tx| {
            tx.execute(
                "UPDATE node_state SET node_instance_id=?1,control_endpoint='http://127.0.0.1:19431',status='running',model_download=1,slot_load=1,slot_unload=1,operation_cancel=1,operation_stream=1 WHERE singleton=1",
                [INSTANCE],
            )?;
            Ok(())
        })
        .unwrap();
    (RepositoryFixturePath { _root: root, path }, repository)
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.owner.lock", path.display()));
    let _ = std::fs::remove_file(format!("{}.migration.bak", path.display()));
    let _ = std::fs::remove_file(format!("{}.migration.bak.owner.lock", path.display()));
}

#[test]
fn worker_fixture_repository_parent_is_private() {
    let (path, repository) = repository();
    crate::control_state::state_machine::test_support::storage::assert_private_repository_parent(
        &path,
    );
    repository.close().unwrap();
    cleanup(&path);
}

#[test]
fn command_transport_is_a_std_sync_bounded_channel() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();

    let sender_type = handle.command_sender_type_name_for_test();
    assert!(sender_type.contains("std::sync::mpsc::SyncSender<"));
    assert!(!sender_type.contains("tokio::sync::mpsc"));

    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_admissions_have_unique_monotonic_commit_order() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let mut tasks = Vec::new();
    for index in 0..32 {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            handle
                .admit(download(format!("model-{index}")))
                .await
                .unwrap()
        }));
    }
    let mut receipts = Vec::new();
    for task in tasks {
        receipts.push(task.await.unwrap());
    }
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.operation_id)
            .collect::<HashSet<_>>()
            .len(),
        32
    );
    receipts.sort_by_key(|receipt| receipt.revision);
    assert!(receipts
        .windows(2)
        .all(|pair| { pair[0].revision.checked_next() == Some(pair[1].revision) }));
    assert_eq!(handle.snapshot().operations.len(), 32);
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn full_external_queue_rejects_without_allocating_operation() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    let before = handle.snapshot();
    handle.fill_queue_for_test();
    assert_eq!(
        handle.admit(download("overflow")).await.unwrap_err(),
        ControlStateError::WriterOverloaded
    );
    assert_eq!(*handle.snapshot(), *before);
    barrier.wait();
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn saturated_progress_cell_replaces_only_the_same_operation() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    handle.fill_queue_for_test();
    let operation_id = OperationId::new_v4();
    let first = Transition::Progress {
        operation_id,
        progress: V2OperationProgress {
            completed_bytes: DecimalU64::new(1),
            total_bytes: None,
        },
    };
    let latest = Transition::Progress {
        operation_id,
        progress: V2OperationProgress {
            completed_bytes: DecimalU64::new(2),
            total_bytes: None,
        },
    };
    handle.try_observe_progress(first).unwrap();
    handle.try_observe_progress(latest.clone()).unwrap();
    assert_eq!(handle.pending_progress_for_test(operation_id), Some(latest));
    barrier.wait();
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test(start_paused = true)]
async fn required_transition_times_out_queue_at_five_seconds_without_blocking_tokio() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    handle.fill_queue_for_test();
    let waiting_handle = handle.clone();
    let observation = tokio::spawn(async move {
        waiting_handle
            .observe_required_async(Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            })
            .await
    });
    let heartbeat = tokio::spawn(async {
        tokio::task::yield_now().await;
        7
    });
    assert_eq!(heartbeat.await.unwrap(), 7);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        observation.await.unwrap().unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert!(!handle.is_healthy());
    barrier.wait();
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test(start_paused = true)]
async fn required_transition_gets_fresh_ack_window_after_late_enqueue() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();
    let waiting_handle = handle.clone();
    let observation = tokio::spawn(async move {
        waiting_handle
            .observe_required_async(Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            })
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(4_900)).await;
    synthetic.pop_one().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    let reply = synthetic.take_observe_reply().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(!observation.is_finished());
    reply
        .send(Ok(CommitReceipt {
            epoch: StreamEpoch::from_str(EPOCH).unwrap(),
            revision: DecimalU64::new(2),
            cursor: DecimalU64::new(2),
            event_id: Some(EventId::new_v4()),
        }))
        .unwrap();
    assert!(observation.await.unwrap().is_ok());
    assert!(handle.is_healthy());
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn blocking_actor_admission_waits_for_queue_and_uses_a_fresh_ack_window() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();

    let waiting_handle = handle.clone();
    let admission = std::thread::spawn(move || {
        waiting_handle.admit_blocking_with_timeouts_for_test(
            download("blocking-admission"),
            Duration::from_millis(250),
            Duration::from_millis(100),
        )
    });
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert!(!admission.is_finished());
    synthetic.pop_one().await;
    let reply = synthetic.take_admit_reply().await;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert!(!admission.is_finished());
    let expected = crate::control_state::state_machine::CommittedAdmission {
        epoch: StreamEpoch::from_str(EPOCH).unwrap(),
        operation_id: OperationId::from_str("8aaaaaaa-0000-4000-8000-000000000001").unwrap(),
        revision: DecimalU64::new(2),
        v1_operation_id: "op-1".into(),
    };
    reply.send(Ok(expected.clone())).unwrap();

    assert_eq!(admission.join().unwrap().unwrap(), expected);
    assert!(handle.is_healthy());
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn blocking_actor_required_observation_closed_ack_poisons_unknown_commit() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    let waiting_handle = handle.clone();
    let observation = std::thread::spawn(move || {
        waiting_handle.observe_required_blocking_with_timeouts_for_test(
            Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            },
            Duration::from_millis(250),
            Duration::from_millis(250),
        )
    });
    drop(synthetic.take_observe_reply().await);

    assert_eq!(
        observation.join().unwrap().unwrap_err(),
        ControlStateError::UnknownCommit
    );
    assert!(!handle.is_healthy());
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn blocking_actor_admission_queue_timeout_is_overload_without_poison_or_mutation() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let admission = std::thread::spawn(move || {
        waiting_handle.admit_blocking_with_timeouts_for_test(
            download("queue-timeout"),
            Duration::from_millis(25),
            Duration::from_millis(250),
        )
    });

    assert_eq!(
        admission.join().unwrap().unwrap_err(),
        ControlStateError::WriterOverloaded
    );
    assert!(handle.is_healthy());
    assert_eq!(*handle.snapshot(), *before);
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn blocking_required_observation_queue_timeout_poisons_unavailable() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let observation = std::thread::spawn(move || {
        waiting_handle.observe_required_blocking_with_timeouts_for_test(
            Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            },
            Duration::from_millis(25),
            Duration::from_millis(250),
        )
    });

    assert_eq!(
        observation.join().unwrap().unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert!(!handle.is_healthy());
    assert_eq!(*handle.snapshot(), *before);
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn externally_poisoned_waiting_admission_never_uses_a_released_queue_slot() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let admission = std::thread::spawn(move || {
        waiting_handle.admit_blocking_with_timeouts_for_test(
            download("must-not-submit-after-poison"),
            Duration::from_millis(250),
            Duration::from_millis(100),
        )
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    handle.poison_for_test();
    synthetic.pop_one().await;

    assert!(
        tokio::time::timeout(Duration::from_millis(50), synthetic.take_admit_reply())
            .await
            .is_err()
    );
    assert_eq!(
        admission.join().unwrap().unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert!(!handle.is_healthy());
    assert_eq!(*handle.snapshot(), *before);
    drop(handle);
    cleanup(&path);
}

#[tokio::test(start_paused = true)]
async fn externally_poisoned_async_required_observation_exits_before_released_slot() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    handle.fill_queue_for_test();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let observation = tokio::spawn(async move {
        waiting_handle
            .observe_required_async(Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            })
            .await
    });
    tokio::task::yield_now().await;

    handle.poison_for_test();
    synthetic.pop_one().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;

    assert!(observation.is_finished());
    assert!(!synthetic.drain_contains_observe_for_test());
    assert_eq!(
        observation.await.unwrap().unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert!(!handle.is_healthy());
    assert_eq!(*handle.snapshot(), *before);
    drop(handle);
    cleanup(&path);
}

#[tokio::test]
async fn expired_admission_deadline_cannot_enqueue_into_an_open_slot() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let mut synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    let waiting_handle = handle.clone();
    let admission = std::thread::spawn(move || {
        waiting_handle.admit_blocking_with_timeouts_for_test(
            download("expired-before-enqueue"),
            Duration::ZERO,
            Duration::from_millis(100),
        )
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), synthetic.take_admit_reply())
            .await
            .is_err()
    );
    assert_eq!(
        admission.join().unwrap().unwrap_err(),
        ControlStateError::WriterOverloaded
    );
    assert!(handle.is_healthy());
    drop(handle);
    cleanup(&path);
}

#[test]
fn expired_ack_deadline_rejects_an_already_available_reply_as_unknown_commit() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    repository.close().unwrap();
    let synthetic = synthetic_queue_for_test(state);
    let handle = synthetic.handle.clone();
    let (reply, receive) = tokio::sync::oneshot::channel();
    reply
        .send(Ok(CommitReceipt {
            epoch: StreamEpoch::from_str(EPOCH).unwrap(),
            revision: DecimalU64::new(2),
            cursor: DecimalU64::new(2),
            event_id: Some(EventId::new_v4()),
        }))
        .unwrap();

    assert_eq!(
        handle
            .receive_blocking_ack_until_for_test(receive, std::time::Instant::now())
            .unwrap_err(),
        ControlStateError::UnknownCommit
    );
    assert!(!handle.is_healthy());
    drop(handle);
    cleanup(&path);
}

#[tokio::test(start_paused = true)]
async fn enqueued_transition_without_ack_poisoned_as_unknown_commit() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let observation = tokio::spawn(async move {
        waiting_handle
            .observe_required_async(Transition::Cancelled {
                operation_id: OperationId::new_v4(),
            })
            .await
    });
    tokio::task::yield_now().await;
    handle.admit_and_drop_ack_for_test(download("must-not-commit"));
    handle.fill_queue_until_full_for_test();
    let pending_operation = OperationId::new_v4();
    handle
        .try_observe_progress(Transition::Progress {
            operation_id: pending_operation,
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(1_048_576),
                total_bytes: None,
            },
        })
        .unwrap();
    assert!(handle
        .pending_progress_for_test(pending_operation)
        .is_some());
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        observation.await.unwrap().unwrap_err(),
        ControlStateError::UnknownCommit
    );
    assert!(!handle.is_healthy());
    barrier.wait();
    let after = handle.snapshot();
    drop(handle);
    worker.join_for_test();
    assert_eq!(*after, *before);
    let reopened = ControlRepository::open_or_create(
        &path,
        NodeId::from_str(NODE_ID).unwrap(),
        &mut InitialIds,
    )
    .unwrap();
    assert_eq!(reopened.committed_state().unwrap(), *before);
    reopened.close().unwrap();
    cleanup(&path);
}

#[test]
fn committed_state_read_uncertainty_poison_stops_later_queued_work() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    let before = handle.snapshot();
    handle.admit_with_snapshot_failure_for_test(download("possibly-committed"));
    handle.admit_and_drop_ack_for_test(download("must-not-commit"));
    barrier.wait();
    let published = handle.snapshot();
    drop(handle);
    worker.join_for_test();

    let reopened = ControlRepository::open_or_create(
        &path,
        NodeId::from_str(NODE_ID).unwrap(),
        &mut InitialIds,
    )
    .unwrap();
    let durable = reopened.committed_state().unwrap();
    assert_eq!(durable.revision.get(), before.revision.get() + 1);
    assert_eq!(durable.operations.len(), 1);
    assert_eq!(
        durable.operations[0].model_id.as_deref(),
        Some("possibly-committed")
    );
    assert_eq!(*published, *before);
    reopened.close().unwrap();
    cleanup(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_ack_retry_of_same_terminal_observation_is_idempotent() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let admission = handle
        .admit(AdmissionRequest::Load {
            model_id: "model-a".to_owned(),
        })
        .await
        .unwrap();
    handle
        .observe_required_async(Transition::Started {
            operation_id: admission.operation_id,
            progress: None,
        })
        .await
        .unwrap();
    let terminal = Transition::Succeeded {
        operation_id: admission.operation_id,
        observed_model_id: Some("model-a".to_owned()),
    };
    handle
        .observe_and_drop_ack_for_test(terminal.clone())
        .await
        .unwrap();
    let committed_revision = handle.snapshot().revision;
    let receipt = handle.observe_required_async(terminal).await.unwrap();
    assert!(receipt.is_noop());
    assert_eq!(handle.snapshot().revision, committed_revision);
    assert_eq!(
        handle
            .snapshot()
            .events
            .iter()
            .filter(|event| event.operation_id == Some(admission.operation_id))
            .filter(|event| event.revision == committed_revision)
            .count(),
        1
    );
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn immutable_snapshot_is_replaced_only_after_commit_ack() {
    let (path, repository) = repository();
    let (handle, worker, barrier) = spawn_paused_from_repository_for_test(repository).unwrap();
    let before = handle.snapshot();
    let waiting_handle = handle.clone();
    let admission = tokio::spawn(async move { waiting_handle.admit(download("model-a")).await });
    tokio::task::yield_now().await;
    assert!(std::sync::Arc::ptr_eq(&before, &handle.snapshot()));
    barrier.wait();
    admission.await.unwrap().unwrap();
    assert!(!std::sync::Arc::ptr_eq(&before, &handle.snapshot()));
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn oversized_retained_replay_returns_all_or_nothing_gap_snapshot() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    for index in 0..3 {
        handle
            .admit(download(format!("snapshot-model-{index}")))
            .await
            .unwrap();
    }
    let state = handle.snapshot();
    let base =
        build_reconnect_snapshot(&state, None, DecimalU64::new(10), MAX_SNAPSHOT_BYTES).unwrap();
    let base_bytes = serde_json::to_vec(&base).unwrap().len();
    let reconnect = build_reconnect_snapshot(
        &state,
        Some((StreamEpoch::from_str(EPOCH).unwrap(), DecimalU64::new(1))),
        DecimalU64::new(10),
        base_bytes,
    )
    .unwrap();
    assert!(reconnect.stream.cursor_gap);
    assert!(reconnect.events.is_empty());
    assert!(serde_json::to_vec(&reconnect).unwrap().len() <= base_bytes);
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[test]
fn reconnect_epoch_replacement_and_cursor_gap_are_fail_closed() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    let reconnect = build_reconnect_snapshot(
        &state,
        Some((StreamEpoch::new_v4(), DecimalU64::new(1))),
        DecimalU64::new(10),
        MAX_SNAPSHOT_BYTES,
    )
    .unwrap();
    assert!(reconnect.stream.cursor_gap);
    assert!(reconnect.events.is_empty());
    assert_eq!(
        reconnect.generated_at_unix_ms,
        state.last_committed_at_unix_ms
    );
    repository.close().unwrap();
    cleanup(&path);
}

#[tokio::test]
async fn subscription_snapshot_and_registration_have_no_lost_event_boundary() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let mut subscription = handle.subscribe(None, DecimalU64::new(10)).await.unwrap();
    let snapshot_cursor = subscription.snapshot.stream.cursor;
    let admission = handle.admit(download("after-subscribe")).await.unwrap();
    let event = subscription.events.recv().await.unwrap();
    assert_eq!(event.sequence, snapshot_cursor.checked_next().unwrap());
    assert_eq!(event.sequence, admission.revision);
    assert_eq!(event.operation_id, Some(admission.operation_id));
    drop(subscription);
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn slow_subscriber_disconnects_without_blocking_writer() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let admission = handle.admit(download("progressing")).await.unwrap();
    let mut subscription = handle.subscribe(None, DecimalU64::new(10)).await.unwrap();
    handle
        .observe_required_async(Transition::Started {
            operation_id: admission.operation_id,
            progress: Some(V2OperationProgress {
                completed_bytes: DecimalU64::new(0),
                total_bytes: None,
            }),
        })
        .await
        .unwrap();
    for index in 1..=128 {
        handle
            .observe_required_async(Transition::Progress {
                operation_id: admission.operation_id,
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(index * 1_048_576),
                    total_bytes: None,
                },
            })
            .await
            .unwrap();
    }
    assert_eq!(
        handle
            .read_snapshot()
            .unwrap()
            .operations
            .iter()
            .find(|operation| operation.operation_id == admission.operation_id)
            .unwrap()
            .status,
        V2OperationStatus::Running
    );
    let mut delivered = 0;
    while subscription.events.recv().await.is_some() {
        delivered += 1;
    }
    assert_eq!(delivered, 128);
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn poison_closes_subscribers_and_rejects_new_reads_and_streams() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let mut subscription = handle.subscribe(None, DecimalU64::new(10)).await.unwrap();
    handle.admit_with_snapshot_failure_for_test(download("uncertain"));
    assert!(subscription.events.recv().await.is_none());
    assert_eq!(
        handle.read_snapshot().unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert_eq!(
        handle
            .subscribe(None, DecimalU64::new(11))
            .await
            .unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    assert_eq!(
        handle.reconnect(None, DecimalU64::new(11)).unwrap_err(),
        ControlStateError::DurableStateUnavailable
    );
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn external_poison_wakes_subscription_while_writer_is_blocked() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let mut subscription = handle.subscribe(None, DecimalU64::new(10)).await.unwrap();
    let release = handle.block_worker_for_test().await;
    handle.poison_for_test();
    assert!(subscription.events.recv().await.is_none());
    release.wait();
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn worker_snapshot_uncertainty_wakes_subscription_before_cleanup_unblocks() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    let mut subscription = handle.subscribe(None, DecimalU64::new(10)).await.unwrap();
    let release = handle
        .trigger_snapshot_failure_and_block_cleanup_for_test(download("uncertain-and-stuck"))
        .await;
    assert!(subscription.events.recv().await.is_none());
    release.wait();
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn abandoned_subscription_churn_is_pruned_and_next_stream_stays_atomic() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    for _ in 0..256 {
        drop(handle.subscribe(None, DecimalU64::new(10)).await.unwrap());
    }
    assert!(handle.subscriber_count_for_test().await <= 1);
    handle.cancel_subscribe_for_test().await;
    assert_eq!(handle.subscriber_count_for_test().await, 0);

    let mut subscription = handle.subscribe(None, DecimalU64::new(11)).await.unwrap();
    let snapshot_cursor = subscription.snapshot.stream.cursor;
    let admission = handle.admit(download("after-churn")).await.unwrap();
    let event = subscription.events.recv().await.unwrap();
    assert_eq!(event.sequence, snapshot_cursor.checked_next().unwrap());
    assert_eq!(event.operation_id, Some(admission.operation_id));

    drop(subscription);
    drop(handle);
    worker.join_for_test();
    cleanup(&path);
}

#[tokio::test]
async fn graceful_stop_acknowledges_and_joins_worker() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    worker.shutdown().await.unwrap();
    assert!(!handle.is_healthy());
    cleanup(&path);
}

#[tokio::test]
async fn worker_panic_is_classified_without_reporting_graceful_shutdown() {
    let (path, repository) = repository();
    let (handle, worker) = spawn_from_repository_for_test(repository).unwrap();
    handle.panic_worker_for_test().await;
    assert_eq!(
        worker.shutdown().await.unwrap_err(),
        ControlStateError::WorkerPanicked
    );
    assert!(!handle.is_healthy());
    cleanup(&path);
}

#[tokio::test(start_paused = true)]
async fn worker_stop_ack_and_join_share_one_absolute_deadline() {
    let (path, repository) = repository();
    let (handle, worker, barrier, reaper_finished) =
        spawn_paused_with_reaper_completion_for_test(repository).unwrap();
    let started = tokio::time::Instant::now();
    let shutdown = tokio::spawn(worker.shutdown());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        shutdown.await.unwrap().unwrap_err(),
        ControlStateError::ShutdownDeadlineExceeded
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(10)
    );
    assert!(!handle.is_healthy());
    barrier.wait();
    reaper_finished
        .recv_timeout(Duration::from_secs(1))
        .expect("shutdown reaper must finish before fixture cleanup");
    drop(handle);
    cleanup(&path);
}

#[test]
fn authoritative_snapshot_over_budget_is_an_error_not_a_truncation() {
    let (path, repository) = repository();
    let state = repository.committed_state().unwrap();
    let base =
        build_reconnect_snapshot(&state, None, DecimalU64::new(10), MAX_SNAPSHOT_BYTES).unwrap();
    let smaller_than_base = serde_json::to_vec(&base).unwrap().len() - 1;
    assert_eq!(
        build_reconnect_snapshot(&state, None, DecimalU64::new(10), smaller_than_base).unwrap_err(),
        ControlStateError::SnapshotTooLarge
    );
    repository.close().unwrap();
    cleanup(&path);
}

#[test]
fn test_fixture_uses_the_published_instance() {
    assert!(NodeInstanceId::from_str(INSTANCE).is_ok());
}
