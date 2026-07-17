use super::state_machine::{
    AdmissionRequest, CommitReceipt, CommittedAdmission, CommittedState, MutationIds, Transition,
    TransitionError,
};
use super::{ControlRepository, RepositoryErrorClass};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, StreamEpoch, V2ReconnectSnapshot, V2StreamPosition,
    V2_SCHEMA_VERSION,
};
use loxa_protocol::NodeInstanceId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

pub(crate) const COMMAND_CAPACITY: usize = 64;
pub(crate) const MAX_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
const ENQUEUE_TIMEOUT: Duration = Duration::from_secs(5);
const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const ENQUEUE_RETRY: Duration = Duration::from_millis(10);
const MAX_PENDING_PROGRESS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlStateError {
    WriterOverloaded,
    DurableStateUnavailable,
    UnknownCommit,
    SnapshotTooLarge,
    Transition(TransitionError),
    Repository(RepositoryErrorClass),
}

enum ControlCommand {
    Admit {
        request: AdmissionRequest,
        reply: oneshot::Sender<Result<CommittedAdmission, ControlStateError>>,
    },
    Observe {
        transition: Transition,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    #[cfg(test)]
    Noop,
    #[cfg(test)]
    AdmitWithSnapshotFailure {
        request: AdmissionRequest,
        reply: oneshot::Sender<Result<CommittedAdmission, ControlStateError>>,
    },
    #[cfg(test)]
    ObserveAndDropAck {
        transition: Transition,
        committed: oneshot::Sender<()>,
    },
}

#[derive(Default)]
struct WorkerIds;

impl MutationIds for WorkerIds {
    fn new_operation_id(&mut self) -> OperationId {
        OperationId::new_v4()
    }

    fn new_event_id(&mut self) -> EventId {
        EventId::new_v4()
    }
}

#[derive(Clone)]
pub(crate) struct ControlStateHandle {
    sender: mpsc::Sender<ControlCommand>,
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    healthy: Arc<AtomicBool>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
}

impl ControlStateHandle {
    pub(crate) fn snapshot(&self) -> Arc<CommittedState> {
        self.snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub(crate) async fn admit(
        &self,
        request: AdmissionRequest,
    ) -> Result<CommittedAdmission, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Admit { request, reply })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) async fn observe_required_async(
        &self,
        transition: Transition,
    ) -> Result<CommitReceipt, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let enqueue_deadline = tokio::time::Instant::now() + ENQUEUE_TIMEOUT;
        let receive = loop {
            let (reply, receive) = oneshot::channel();
            match self.sender.try_send(ControlCommand::Observe {
                transition: transition.clone(),
                reply,
            }) {
                Ok(()) => break receive,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let now = tokio::time::Instant::now();
                    if now >= enqueue_deadline {
                        return Err(self.poison_unavailable());
                    }
                    tokio::time::sleep(ENQUEUE_RETRY.min(enqueue_deadline - now)).await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(self.poison_unavailable());
                }
            }
        };
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    /// Best-effort progress may occupy one replaceable pre-queue cell per operation.
    pub(crate) fn try_observe_progress(
        &self,
        transition: Transition,
    ) -> Result<(), ControlStateError> {
        let operation_id = match &transition {
            Transition::Progress { operation_id, .. } => *operation_id,
            _ => return Err(ControlStateError::DurableStateUnavailable),
        };
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, _receive) = oneshot::channel();
        match self.sender.try_send(ControlCommand::Observe {
            transition: transition.clone(),
            reply,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let mut pending = self
                    .pending_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !pending.contains_key(&operation_id) && pending.len() == MAX_PENDING_PROGRESS {
                    return Err(ControlStateError::WriterOverloaded);
                }
                pending.insert(operation_id, transition);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(self.poison_unavailable()),
        }
    }

    pub(crate) fn reconnect(
        &self,
        requested: Option<(StreamEpoch, DecimalU64)>,
        generated_at_unix_ms: DecimalU64,
    ) -> Result<V2ReconnectSnapshot, ControlStateError> {
        build_reconnect_snapshot(
            &self.snapshot(),
            requested,
            generated_at_unix_ms,
            MAX_SNAPSHOT_BYTES,
        )
    }

    async fn receive_commit<T>(
        &self,
        receive: oneshot::Receiver<Result<T, ControlStateError>>,
        deadline: tokio::time::Instant,
    ) -> Result<T, ControlStateError> {
        match tokio::time::timeout_at(deadline, receive).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) | Err(_) => Err(self.poison_unknown_commit()),
        }
    }

    fn poison_unknown_commit(&self) -> ControlStateError {
        self.healthy.store(false, Ordering::Release);
        ControlStateError::UnknownCommit
    }

    fn poison_unavailable(&self) -> ControlStateError {
        self.healthy.store(false, Ordering::Release);
        ControlStateError::DurableStateUnavailable
    }
}

pub(crate) struct ControlStateWorker {
    join: Option<std::thread::JoinHandle<()>>,
}

impl ControlStateWorker {
    #[cfg(test)]
    pub(super) fn join_for_test(mut self) {
        if let Some(join) = self.join.take() {
            join.join()
                .expect("control-state test worker must not panic");
        }
    }
}

#[cfg(test)]
pub(super) fn spawn_from_repository_for_test(
    repository: ControlRepository,
) -> Result<(ControlStateHandle, ControlStateWorker), ControlStateError> {
    spawn(repository, None)
}

#[cfg(test)]
pub(super) fn spawn_paused_from_repository_for_test(
    repository: ControlRepository,
) -> Result<
    (
        ControlStateHandle,
        ControlStateWorker,
        Arc<std::sync::Barrier>,
    ),
    ControlStateError,
> {
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let result = spawn(repository, Some(Arc::clone(&barrier)))?;
    Ok((result.0, result.1, barrier))
}

fn spawn(
    repository: ControlRepository,
    start_barrier: Option<Arc<std::sync::Barrier>>,
) -> Result<(ControlStateHandle, ControlStateWorker), ControlStateError> {
    let initial = Arc::new(
        repository
            .committed_state()
            .map_err(|error| ControlStateError::Repository(error.class()))?,
    );
    let node_instance_id = initial
        .node
        .as_ref()
        .map(|node| node.node_instance_id)
        .ok_or(ControlStateError::DurableStateUnavailable)?;
    let snapshot = Arc::new(RwLock::new(initial));
    let healthy = Arc::new(AtomicBool::new(true));
    let pending_progress = Arc::new(Mutex::new(HashMap::new()));
    let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let handle = ControlStateHandle {
        sender,
        snapshot: Arc::clone(&snapshot),
        healthy: Arc::clone(&healthy),
        pending_progress: Arc::clone(&pending_progress),
    };
    let join = std::thread::Builder::new()
        .name("loxa-control-state".to_owned())
        .spawn(move || {
            if let Some(barrier) = start_barrier {
                barrier.wait();
            }
            run_worker(
                repository,
                receiver,
                node_instance_id,
                snapshot,
                healthy,
                pending_progress,
            );
        })
        .map_err(|_| ControlStateError::DurableStateUnavailable)?;
    Ok((handle, ControlStateWorker { join: Some(join) }))
}

#[cfg(test)]
impl ControlStateHandle {
    pub(super) fn fill_queue_for_test(&self) {
        for _ in 0..COMMAND_CAPACITY {
            self.sender
                .try_send(ControlCommand::Noop)
                .expect("paused worker queue has the declared capacity");
        }
    }

    pub(super) fn fill_queue_until_full_for_test(&self) {
        loop {
            match self.sender.try_send(ControlCommand::Noop) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => panic!("test queue closed"),
            }
        }
    }

    pub(super) fn admit_and_drop_ack_for_test(&self, request: AdmissionRequest) {
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Admit { request, reply })
            .expect("test admission must enqueue");
        drop(receive);
    }

    pub(super) fn admit_with_snapshot_failure_for_test(&self, request: AdmissionRequest) {
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::AdmitWithSnapshotFailure { request, reply })
            .expect("fault-injected admission must enqueue");
        drop(receive);
    }

    pub(super) fn observe_and_drop_ack_for_test(
        &self,
        transition: Transition,
    ) -> oneshot::Receiver<()> {
        let (committed, receive_committed) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::ObserveAndDropAck {
                transition,
                committed,
            })
            .expect("test command must enqueue");
        receive_committed
    }

    pub(super) fn pending_progress_for_test(
        &self,
        operation_id: OperationId,
    ) -> Option<Transition> {
        self.pending_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&operation_id)
            .cloned()
    }
}

#[cfg(test)]
pub(super) struct SyntheticQueue {
    pub(super) handle: ControlStateHandle,
    receiver: mpsc::Receiver<ControlCommand>,
}

#[cfg(test)]
impl SyntheticQueue {
    pub(super) async fn pop_one(&mut self) {
        assert!(matches!(
            self.receiver.recv().await,
            Some(ControlCommand::Noop)
        ));
    }

    pub(super) async fn take_observe_reply(
        &mut self,
    ) -> oneshot::Sender<Result<CommitReceipt, ControlStateError>> {
        loop {
            match self.receiver.recv().await.expect("synthetic queue command") {
                ControlCommand::Observe { reply, .. } => return reply,
                ControlCommand::Noop => {}
                _ => panic!("unexpected synthetic queue command"),
            }
        }
    }
}

#[cfg(test)]
pub(super) fn synthetic_queue_for_test(state: CommittedState) -> SyntheticQueue {
    let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
    SyntheticQueue {
        handle: ControlStateHandle {
            sender,
            snapshot: Arc::new(RwLock::new(Arc::new(state))),
            healthy: Arc::new(AtomicBool::new(true)),
            pending_progress: Arc::new(Mutex::new(HashMap::new())),
        },
        receiver,
    }
}

fn run_worker(
    mut repository: ControlRepository,
    mut receiver: mpsc::Receiver<ControlCommand>,
    node_instance_id: NodeInstanceId,
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    healthy: Arc<AtomicBool>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
) {
    let mut ids = WorkerIds;
    while let Some(command) = receiver.blocking_recv() {
        if !healthy.load(Ordering::Acquire) {
            break;
        }
        process_command(
            command,
            &mut repository,
            node_instance_id,
            &mut ids,
            &snapshot,
            &healthy,
        );
        if !healthy.load(Ordering::Acquire) {
            break;
        }
        let progress = {
            let mut pending = pending_progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending
                .keys()
                .next()
                .copied()
                .and_then(|id| pending.remove(&id))
        };
        if let Some(transition) = progress {
            if !healthy.load(Ordering::Acquire) {
                break;
            }
            let (reply, _receive) = oneshot::channel();
            process_command(
                ControlCommand::Observe { transition, reply },
                &mut repository,
                node_instance_id,
                &mut ids,
                &snapshot,
                &healthy,
            );
        }
    }
    if repository.close().is_err() {
        healthy.store(false, Ordering::Release);
    }
}

fn process_command(
    command: ControlCommand,
    repository: &mut ControlRepository,
    node_instance_id: NodeInstanceId,
    ids: &mut WorkerIds,
    snapshot: &RwLock<Arc<CommittedState>>,
    healthy: &AtomicBool,
) {
    match command {
        ControlCommand::Admit { request, reply } => {
            let result = map_transition_result(
                repository.admit(node_instance_id, request, now_unix_ms(), ids),
                healthy,
            );
            finish_commit(repository, result, reply, snapshot, healthy, false);
        }
        ControlCommand::Observe { transition, reply } => {
            let result = map_transition_result(
                repository.observe(node_instance_id, transition, now_unix_ms(), ids),
                healthy,
            );
            finish_commit(repository, result, reply, snapshot, healthy, false);
        }
        #[cfg(test)]
        ControlCommand::Noop => {}
        #[cfg(test)]
        ControlCommand::AdmitWithSnapshotFailure { request, reply } => {
            let result = map_transition_result(
                repository.admit(node_instance_id, request, now_unix_ms(), ids),
                healthy,
            );
            finish_commit(repository, result, reply, snapshot, healthy, true);
        }
        #[cfg(test)]
        ControlCommand::ObserveAndDropAck {
            transition,
            committed,
        } => {
            let result = map_transition_result(
                repository.observe(node_instance_id, transition, now_unix_ms(), ids),
                healthy,
            );
            let (reply, receive) = oneshot::channel();
            drop(receive);
            finish_commit(repository, result, reply, snapshot, healthy, false);
            let _ = committed.send(());
        }
    }
}

fn map_transition_result<T>(
    result: Result<T, TransitionError>,
    healthy: &AtomicBool,
) -> Result<T, ControlStateError> {
    match result {
        Ok(value) => Ok(value),
        Err(TransitionError::Repository(_)) => {
            healthy.store(false, Ordering::Release);
            Err(ControlStateError::UnknownCommit)
        }
        Err(error) => Err(ControlStateError::Transition(error)),
    }
}

fn finish_commit<T>(
    repository: &ControlRepository,
    result: Result<T, ControlStateError>,
    reply: oneshot::Sender<Result<T, ControlStateError>>,
    snapshot: &RwLock<Arc<CommittedState>>,
    healthy: &AtomicBool,
    force_snapshot_read_failure: bool,
) {
    let result = match result {
        Ok(_receipt) if force_snapshot_read_failure => {
            healthy.store(false, Ordering::Release);
            Err(ControlStateError::UnknownCommit)
        }
        Ok(receipt) => match repository.committed_state() {
            Ok(committed) => {
                *snapshot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(committed);
                Ok(receipt)
            }
            Err(error) => {
                healthy.store(false, Ordering::Release);
                let _ = error;
                Err(ControlStateError::UnknownCommit)
            }
        },
        Err(error) => Err(error),
    };
    let _ = reply.send(result);
}

pub(crate) fn build_reconnect_snapshot(
    state: &CommittedState,
    requested: Option<(StreamEpoch, DecimalU64)>,
    generated_at_unix_ms: DecimalU64,
    max_bytes: usize,
) -> Result<V2ReconnectSnapshot, ControlStateError> {
    let node = state
        .node
        .clone()
        .ok_or(ControlStateError::DurableStateUnavailable)?;
    let epoch = state
        .events
        .last()
        .map(|event| event.epoch)
        .ok_or(ControlStateError::DurableStateUnavailable)?;
    let mut result = V2ReconnectSnapshot {
        schema_version: V2_SCHEMA_VERSION,
        epoch,
        revision: state.revision,
        generated_at_unix_ms,
        stream: V2StreamPosition {
            epoch,
            cursor: state.cursor,
            cursor_gap: false,
        },
        nodes: vec![node],
        slots: vec![state.slot.clone()],
        operations: state.operations.clone(),
        events: Vec::new(),
    };
    if exact_json_len(&result)? > max_bytes {
        return Err(ControlStateError::SnapshotTooLarge);
    }
    let Some((requested_epoch, requested_cursor)) = requested else {
        return Ok(result);
    };
    if requested_epoch != epoch || requested_cursor > state.cursor {
        result.stream.cursor_gap = true;
        return Ok(result);
    }
    if requested_cursor == state.cursor {
        return Ok(result);
    }
    let required: Vec<_> = state
        .events
        .iter()
        .filter(|event| event.sequence > requested_cursor)
        .cloned()
        .collect();
    let contiguous = required
        .first()
        .is_some_and(|first| requested_cursor.checked_next() == Some(first.sequence))
        && required
            .last()
            .is_some_and(|last| last.sequence == state.cursor);
    if !contiguous {
        result.stream.cursor_gap = true;
        return Ok(result);
    }
    result.events = required;
    if exact_json_len(&result)? > max_bytes {
        result.events.clear();
        result.stream.cursor_gap = true;
    }
    Ok(result)
}

fn exact_json_len(snapshot: &V2ReconnectSnapshot) -> Result<usize, ControlStateError> {
    serde_json::to_vec(snapshot)
        .map(|bytes| bytes.len())
        .map_err(|_| ControlStateError::DurableStateUnavailable)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "test_support/worker.rs"]
mod tests;
