use super::recovery::{ExactAbsenceProof, ExactReady, RecoveryEvidence};
use super::state_machine::{
    AdmissionRequest, CommitReceipt, CommittedAdmission, CommittedState, InstancePublication,
    LifecycleObservation, MutationIds, RestartEvidence, Transition, TransitionError,
};
use super::{
    ControlIdGenerator, ControlRepository, ControlStatePath, RepositoryErrorClass, ScalarSource,
};
use crate::runtime::NodeOwnerGuard;
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, StreamEpoch, V2ControlEvent, V2ReconnectSnapshot,
    V2StreamPosition, V2_SCHEMA_VERSION,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as sync_mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) const COMMAND_CAPACITY: usize = 64;
pub(crate) const MAX_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
const SUBSCRIBER_CAPACITY: usize = 128;
const ENQUEUE_TIMEOUT: Duration = Duration::from_secs(5);
const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const ENQUEUE_RETRY: Duration = Duration::from_millis(10);
const MAX_PENDING_PROGRESS: usize = 128;
const CONTROL_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(not(test))]
static STARTUP_PERMANENTLY_POISONED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
thread_local! {
    static STARTUP_POISONED_FOR_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn startup_is_permanently_poisoned() -> bool {
    #[cfg(test)]
    {
        STARTUP_POISONED_FOR_TEST.get()
    }
    #[cfg(not(test))]
    {
        STARTUP_PERMANENTLY_POISONED.load(Ordering::Acquire)
    }
}

fn poison_future_startup() {
    #[cfg(test)]
    STARTUP_POISONED_FOR_TEST.set(true);
    #[cfg(not(test))]
    STARTUP_PERMANENTLY_POISONED.store(true, Ordering::Release);
}

#[cfg(test)]
fn clear_startup_poison_for_test() {
    STARTUP_POISONED_FOR_TEST.set(false);
}

#[derive(Clone, Copy)]
enum BlockingEnqueueTimeoutPolicy {
    AdmissionOverloaded,
    RequiredObservationUnavailable,
}

fn checked_deadline_after(now: std::time::Instant, timeout: Duration) -> std::time::Instant {
    now.checked_add(timeout).unwrap_or(now)
}

enum StartupBehavior {
    Normal,
    #[cfg(test)]
    PanicBeforeInitialization,
    #[cfg(test)]
    BlockBeforePublication {
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlStateError {
    WriterOverloaded,
    DurableStateUnavailable,
    UnknownCommit,
    SnapshotTooLarge,
    WorkerPanicked,
    ShutdownDeadlineExceeded,
    Transition(TransitionError),
    Repository(RepositoryErrorClass),
}

enum ControlCommand {
    PublishInstance {
        publication: InstancePublication,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    BeginStopping {
        now_unix_ms: u64,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    Admit {
        request: AdmissionRequest,
        reply: oneshot::Sender<Result<CommittedAdmission, ControlStateError>>,
    },
    Observe {
        transition: Transition,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    ObserveLifecycle {
        transition: Transition,
        observation: LifecycleObservation,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    Subscribe {
        requested: Option<(StreamEpoch, DecimalU64)>,
        generated_at_unix_ms: DecimalU64,
        max_snapshot_bytes: usize,
        health: watch::Receiver<bool>,
        reply: oneshot::Sender<Result<ControlSubscription, ControlStateError>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
    #[cfg(test)]
    Noop,
    #[cfg(test)]
    AdmitWithSnapshotFailure {
        request: AdmissionRequest,
        reply: oneshot::Sender<Result<CommittedAdmission, ControlStateError>>,
    },
    #[cfg(test)]
    ObserveLifecycleWithSnapshotFailure {
        transition: Transition,
        observation: LifecycleObservation,
        reply: oneshot::Sender<Result<CommitReceipt, ControlStateError>>,
    },
    #[cfg(test)]
    AdmitWithSnapshotFailureAndBlockCleanup {
        request: AdmissionRequest,
        entered: oneshot::Sender<()>,
        release: Arc<std::sync::Barrier>,
    },
    #[cfg(test)]
    ObserveAndDropAck {
        transition: Transition,
        committed: oneshot::Sender<()>,
    },
    #[cfg(test)]
    Panic {
        entered: oneshot::Sender<()>,
    },
    #[cfg(test)]
    Block {
        entered: oneshot::Sender<()>,
        release: Arc<std::sync::Barrier>,
    },
    #[cfg(test)]
    SubscriberCount {
        reply: oneshot::Sender<usize>,
    },
}

#[derive(Debug)]
pub(crate) struct ControlSubscription {
    pub(crate) snapshot: V2ReconnectSnapshot,
    pub(crate) events: ControlEventReceiver,
}

#[derive(Debug)]
pub(crate) struct ControlEventReceiver {
    events: mpsc::Receiver<V2ControlEvent>,
    health: watch::Receiver<bool>,
}

impl ControlEventReceiver {
    pub(crate) async fn recv(&mut self) -> Option<V2ControlEvent> {
        loop {
            if !*self.health.borrow() {
                return None;
            }
            tokio::select! {
                biased;
                changed = self.health.changed() => {
                    if changed.is_err() || !*self.health.borrow() {
                        return None;
                    }
                }
                event = self.events.recv() => return event,
            }
        }
    }
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

impl ControlIdGenerator for WorkerIds {
    fn new_slot_id(&mut self) -> loxa_protocol::v2::SlotId {
        loxa_protocol::v2::SlotId::new_v4()
    }

    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::new_v4()
    }
}

pub(crate) struct ControlStateOpenInput {
    pub(crate) claimed_owner: NodeOwnerGuard,
    pub(crate) first_migration_source: Option<ScalarSource>,
}

pub(crate) struct ControlStateInit {
    pub(crate) path: ControlStatePath,
    pub(crate) node_id: NodeId,
    pub(crate) open_input: ControlStateOpenInput,
    pub(crate) recovery_evidence: RecoveryEvidence,
    pub(crate) now_unix_ms: u64,
}

pub(crate) struct ControlStateBootstrap {
    pub(crate) handle: ControlStateHandle,
    pub(crate) worker: ControlStateWorker,
    pub(crate) claimed_owner: NodeOwnerGuard,
    pub(crate) ready_authority: Option<ExactReady>,
}

struct InitializedControlState {
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
    claimed_owner: NodeOwnerGuard,
    ready_authority: Option<ExactReady>,
}

#[must_use = "startup failure retains durable ownership and must enter fatal shutdown"]
pub(crate) struct ControlStateStartupFailure {
    error: ControlStateError,
    ownership: ManuallyDrop<Box<ControlStateStartupOwnership>>,
}

struct ControlStateStartupOwnership {
    sender: ManuallyDrop<Option<Arc<sync_mpsc::SyncSender<ControlCommand>>>>,
    join: ManuallyDrop<Option<std::thread::JoinHandle<Result<(), ControlStateError>>>>,
    initialization: ManuallyDrop<
        Option<sync_mpsc::Receiver<Result<InitializedControlState, ControlStateError>>>,
    >,
    completion: ManuallyDrop<Option<sync_mpsc::Receiver<Result<(), ControlStateError>>>>,
    shutdown_acknowledgement: ManuallyDrop<Option<oneshot::Receiver<()>>>,
    unstarted_init: ManuallyDrop<Option<ControlStateInit>>,
    _healthy: Arc<AtomicBool>,
    _health_signal: watch::Sender<bool>,
}

impl ControlStateStartupFailure {
    fn new(
        error: ControlStateError,
        sender: Arc<sync_mpsc::SyncSender<ControlCommand>>,
        join: std::thread::JoinHandle<Result<(), ControlStateError>>,
        initialization: sync_mpsc::Receiver<Result<InitializedControlState, ControlStateError>>,
        completion: sync_mpsc::Receiver<Result<(), ControlStateError>>,
        healthy: Arc<AtomicBool>,
        health_signal: watch::Sender<bool>,
    ) -> Self {
        Self::new_with_unstarted_init(
            error,
            sender,
            join,
            initialization,
            completion,
            healthy,
            health_signal,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_unstarted_init(
        error: ControlStateError,
        sender: Arc<sync_mpsc::SyncSender<ControlCommand>>,
        join: std::thread::JoinHandle<Result<(), ControlStateError>>,
        initialization: sync_mpsc::Receiver<Result<InitializedControlState, ControlStateError>>,
        completion: sync_mpsc::Receiver<Result<(), ControlStateError>>,
        healthy: Arc<AtomicBool>,
        health_signal: watch::Sender<bool>,
        unstarted_init: Option<ControlStateInit>,
    ) -> Self {
        poison(&healthy, &health_signal);
        poison_future_startup();
        let (reply, acknowledgement) = oneshot::channel();
        let shutdown_acknowledgement = sender
            .try_send(ControlCommand::Stop { reply })
            .ok()
            .map(|()| acknowledgement);
        Self {
            error,
            ownership: ManuallyDrop::new(Box::new(ControlStateStartupOwnership {
                sender: ManuallyDrop::new(Some(sender)),
                join: ManuallyDrop::new(Some(join)),
                initialization: ManuallyDrop::new(Some(initialization)),
                completion: ManuallyDrop::new(Some(completion)),
                shutdown_acknowledgement: ManuallyDrop::new(shutdown_acknowledgement),
                unstarted_init: ManuallyDrop::new(unstarted_init),
                _healthy: healthy,
                _health_signal: health_signal,
            })),
        }
    }

    fn unstarted(error: ControlStateError, init: ControlStateInit) -> Self {
        let healthy = Arc::new(AtomicBool::new(false));
        let (health_signal, _receiver) = watch::channel(false);
        poison_future_startup();
        Self {
            error,
            ownership: ManuallyDrop::new(Box::new(ControlStateStartupOwnership {
                sender: ManuallyDrop::new(None),
                join: ManuallyDrop::new(None),
                initialization: ManuallyDrop::new(None),
                completion: ManuallyDrop::new(None),
                shutdown_acknowledgement: ManuallyDrop::new(None),
                unstarted_init: ManuallyDrop::new(Some(init)),
                _healthy: healthy,
                _health_signal: health_signal,
            })),
        }
    }

    pub(crate) fn error(&self) -> ControlStateError {
        self.error
    }

    #[cfg(test)]
    fn retains_worker_for_test(&self) -> bool {
        self.ownership.join.is_some() || self.ownership.unstarted_init.is_some()
    }

    #[cfg(test)]
    fn worker_finished_for_test(&self) -> bool {
        self.ownership
            .join
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
    }

    #[cfg(test)]
    fn worker_joined_for_test(&self) -> bool {
        self.ownership.join.is_none()
    }

    #[cfg(test)]
    fn dispose_for_test(self, deadline: std::time::Instant) {
        let mut retained = ManuallyDrop::new(self);
        // SAFETY: `retained` cannot run Drop, and ownership is taken exactly once.
        let mut ownership = unsafe { ManuallyDrop::take(&mut retained.ownership) };
        // SAFETY: each retained field is taken exactly once before the box is released.
        let sender = unsafe { ManuallyDrop::take(&mut ownership.sender) };
        // SAFETY: same single-owner transfer as above.
        let mut join = unsafe { ManuallyDrop::take(&mut ownership.join) };
        // SAFETY: same single-owner transfer as above.
        let initialization = unsafe { ManuallyDrop::take(&mut ownership.initialization) };
        // SAFETY: same single-owner transfer as above.
        let completion = unsafe { ManuallyDrop::take(&mut ownership.completion) };
        // SAFETY: same single-owner transfer as above.
        let mut acknowledgement =
            unsafe { ManuallyDrop::take(&mut ownership.shutdown_acknowledgement) };
        // SAFETY: same single-owner transfer as above.
        let unstarted_init = unsafe { ManuallyDrop::take(&mut ownership.unstarted_init) };
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let completion_observed = completion
            .as_ref()
            .is_some_and(|completion| completion.recv_timeout(remaining).is_ok());
        if let Some(worker) = join.take() {
            let join_deadline = if completion_observed {
                checked_deadline_after(std::time::Instant::now(), Duration::from_secs(1))
            } else {
                deadline
            };
            while !worker.is_finished() && std::time::Instant::now() < join_deadline {
                std::thread::sleep(
                    ENQUEUE_RETRY
                        .min(join_deadline.saturating_duration_since(std::time::Instant::now())),
                );
            }
            assert!(
                worker.is_finished(),
                "startup worker must finish before disposal"
            );
            let _ = worker.join();
        }
        if let Some(acknowledgement) = acknowledgement.as_mut() {
            let _ = acknowledgement.try_recv();
        }
        if let Some(initialization) = initialization.as_ref() {
            drop(initialization.try_recv());
        }
        drop(sender);
        drop(unstarted_init);
        drop(ownership);
        clear_startup_poison_for_test();
    }

    #[cfg(test)]
    fn dispose_and_return_error_for_test(self, deadline: std::time::Instant) -> ControlStateError {
        let error = self.error;
        self.dispose_for_test(deadline);
        error
    }
}

impl std::fmt::Debug for ControlStateStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlStateStartupFailure")
            .field("error", &self.error)
            .field(
                "retains_worker",
                &(self.ownership.join.is_some() || self.ownership.unstarted_init.is_some()),
            )
            .finish()
    }
}

impl PartialEq<ControlStateError> for ControlStateStartupFailure {
    fn eq(&self, other: &ControlStateError) -> bool {
        self.error == *other
    }
}

impl Drop for ControlStateStartupFailure {
    fn drop(&mut self) {
        std::process::abort();
    }
}

#[derive(Clone)]
pub(crate) struct ControlStateHandle {
    sender: Arc<sync_mpsc::SyncSender<ControlCommand>>,
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
}

impl ControlStateHandle {
    #[cfg(test)]
    pub(super) fn command_sender_type_name_for_test(&self) -> &'static str {
        std::any::type_name_of_val(&self.sender)
    }

    pub(crate) fn read_snapshot(&self) -> Result<Arc<CommittedState>, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let snapshot = self.snapshot();
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        Ok(snapshot)
    }

    pub(crate) fn snapshot(&self) -> Arc<CommittedState> {
        self.snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub(crate) fn writer_is_healthy(&self) -> bool {
        self.is_healthy()
    }

    pub(crate) fn subscription_is_healthy(&self) -> bool {
        self.is_healthy()
    }

    pub(crate) fn health_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.healthy)
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
                sync_mpsc::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                sync_mpsc::TrySendError::Disconnected(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) fn admit_blocking_until(
        &self,
        request: AdmissionRequest,
        enqueue_deadline: std::time::Instant,
    ) -> Result<CommittedAdmission, ControlStateError> {
        let maximum = checked_deadline_after(std::time::Instant::now(), ENQUEUE_TIMEOUT);
        self.admit_blocking_with_deadlines(request, enqueue_deadline.min(maximum), ACK_TIMEOUT)
    }

    pub(crate) fn observe_required_blocking_until(
        &self,
        transition: Transition,
        enqueue_deadline: std::time::Instant,
    ) -> Result<CommitReceipt, ControlStateError> {
        let maximum = checked_deadline_after(std::time::Instant::now(), ENQUEUE_TIMEOUT);
        self.observe_required_blocking_with_deadlines(
            transition,
            enqueue_deadline.min(maximum),
            ACK_TIMEOUT,
        )
    }

    pub(crate) fn observe_required_blocking_before(
        &self,
        transition: Transition,
        deadline: std::time::Instant,
    ) -> Result<CommitReceipt, ControlStateError> {
        self.blocking_command_with_ack_deadline(
            deadline,
            ACK_TIMEOUT,
            Some(deadline),
            |reply| ControlCommand::Observe {
                transition: transition.clone(),
                reply,
            },
            BlockingEnqueueTimeoutPolicy::RequiredObservationUnavailable,
        )
    }

    pub(crate) async fn publish_instance(
        &self,
        publication: InstancePublication,
    ) -> Result<CommitReceipt, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::PublishInstance { publication, reply })
            .map_err(|error| match error {
                sync_mpsc::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                sync_mpsc::TrySendError::Disconnected(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) fn publish_instance_blocking_until(
        &self,
        publication: InstancePublication,
        enqueue_deadline: std::time::Instant,
    ) -> Result<CommitReceipt, ControlStateError> {
        let maximum = checked_deadline_after(std::time::Instant::now(), ENQUEUE_TIMEOUT);
        self.blocking_command(
            enqueue_deadline.min(maximum),
            ACK_TIMEOUT,
            |reply| ControlCommand::PublishInstance {
                publication: publication.clone(),
                reply,
            },
            BlockingEnqueueTimeoutPolicy::RequiredObservationUnavailable,
        )
    }

    pub(crate) async fn begin_stopping(
        &self,
        now_unix_ms: u64,
    ) -> Result<CommitReceipt, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::BeginStopping { now_unix_ms, reply })
            .map_err(|error| match error {
                sync_mpsc::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                sync_mpsc::TrySendError::Disconnected(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) fn begin_stopping_blocking_until(
        &self,
        now_unix_ms: u64,
        absolute_deadline: std::time::Instant,
    ) -> Result<CommitReceipt, ControlStateError> {
        let maximum = checked_deadline_after(std::time::Instant::now(), ENQUEUE_TIMEOUT);
        self.blocking_command_with_ack_deadline(
            absolute_deadline.min(maximum),
            ACK_TIMEOUT,
            Some(absolute_deadline),
            |reply| ControlCommand::BeginStopping { now_unix_ms, reply },
            BlockingEnqueueTimeoutPolicy::RequiredObservationUnavailable,
        )
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
            if !self.is_healthy() {
                return Err(ControlStateError::DurableStateUnavailable);
            }
            if tokio::time::Instant::now() >= enqueue_deadline {
                return Err(self.poison_unavailable());
            }
            let (reply, receive) = oneshot::channel();
            if !self.is_healthy() {
                return Err(ControlStateError::DurableStateUnavailable);
            }
            match self.sender.try_send(ControlCommand::Observe {
                transition: transition.clone(),
                reply,
            }) {
                Ok(()) => break receive,
                Err(sync_mpsc::TrySendError::Full(_)) => {
                    let now = tokio::time::Instant::now();
                    if now >= enqueue_deadline {
                        return Err(self.poison_unavailable());
                    }
                    tokio::time::sleep(ENQUEUE_RETRY.min(enqueue_deadline - now)).await;
                    if !self.is_healthy() {
                        return Err(ControlStateError::DurableStateUnavailable);
                    }
                }
                Err(sync_mpsc::TrySendError::Disconnected(_)) => {
                    return Err(self.poison_unavailable());
                }
            }
        };
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) async fn observe_lifecycle_async(
        &self,
        transition: Transition,
        observation: LifecycleObservation,
    ) -> Result<CommitReceipt, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let enqueue_deadline = tokio::time::Instant::now() + ENQUEUE_TIMEOUT;
        let receive = loop {
            if !self.is_healthy() {
                return Err(ControlStateError::DurableStateUnavailable);
            }
            if tokio::time::Instant::now() >= enqueue_deadline {
                return Err(self.poison_unavailable());
            }
            let (reply, receive) = oneshot::channel();
            match self.sender.try_send(ControlCommand::ObserveLifecycle {
                transition: transition.clone(),
                observation: observation.clone(),
                reply,
            }) {
                Ok(()) => break receive,
                Err(sync_mpsc::TrySendError::Full(_)) => {
                    let now = tokio::time::Instant::now();
                    if now >= enqueue_deadline {
                        return Err(self.poison_unavailable());
                    }
                    tokio::time::sleep(ENQUEUE_RETRY.min(enqueue_deadline - now)).await;
                }
                Err(sync_mpsc::TrySendError::Disconnected(_)) => {
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
            Err(sync_mpsc::TrySendError::Full(_)) => {
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
            Err(sync_mpsc::TrySendError::Disconnected(_)) => Err(self.poison_unavailable()),
        }
    }

    pub(crate) fn reconnect(
        &self,
        requested: Option<(StreamEpoch, DecimalU64)>,
        generated_at_unix_ms: DecimalU64,
    ) -> Result<V2ReconnectSnapshot, ControlStateError> {
        let result = build_reconnect_snapshot(
            self.read_snapshot()?.as_ref(),
            requested,
            generated_at_unix_ms,
            MAX_SNAPSHOT_BYTES,
        )?;
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        Ok(result)
    }

    pub(crate) async fn subscribe(
        &self,
        requested: Option<(StreamEpoch, DecimalU64)>,
        generated_at_unix_ms: DecimalU64,
    ) -> Result<ControlSubscription, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Subscribe {
                requested,
                generated_at_unix_ms,
                max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
                health: self.health_signal.subscribe(),
                reply,
            })
            .map_err(|error| match error {
                sync_mpsc::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                sync_mpsc::TrySendError::Disconnected(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn subscribe_with_max_snapshot_bytes_for_test(
        &self,
        requested: Option<(StreamEpoch, DecimalU64)>,
        generated_at_unix_ms: DecimalU64,
        max_snapshot_bytes: usize,
    ) -> Result<ControlSubscription, ControlStateError> {
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Subscribe {
                requested,
                generated_at_unix_ms,
                max_snapshot_bytes,
                health: self.health_signal.subscribe(),
                reply,
            })
            .map_err(|error| match error {
                sync_mpsc::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                sync_mpsc::TrySendError::Disconnected(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
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
        poison(&self.healthy, &self.health_signal);
        ControlStateError::UnknownCommit
    }

    fn poison_unavailable(&self) -> ControlStateError {
        poison(&self.healthy, &self.health_signal);
        ControlStateError::DurableStateUnavailable
    }

    fn admit_blocking_with_deadlines(
        &self,
        request: AdmissionRequest,
        enqueue_deadline: std::time::Instant,
        ack_timeout: Duration,
    ) -> Result<CommittedAdmission, ControlStateError> {
        self.blocking_command(
            enqueue_deadline,
            ack_timeout,
            |reply| ControlCommand::Admit {
                request: request.clone(),
                reply,
            },
            BlockingEnqueueTimeoutPolicy::AdmissionOverloaded,
        )
    }

    fn observe_required_blocking_with_deadlines(
        &self,
        transition: Transition,
        enqueue_deadline: std::time::Instant,
        ack_timeout: Duration,
    ) -> Result<CommitReceipt, ControlStateError> {
        self.blocking_command(
            enqueue_deadline,
            ack_timeout,
            |reply| ControlCommand::Observe {
                transition: transition.clone(),
                reply,
            },
            BlockingEnqueueTimeoutPolicy::RequiredObservationUnavailable,
        )
    }

    fn blocking_command<T>(
        &self,
        enqueue_deadline: std::time::Instant,
        ack_timeout: Duration,
        command: impl FnMut(oneshot::Sender<Result<T, ControlStateError>>) -> ControlCommand,
        timeout_policy: BlockingEnqueueTimeoutPolicy,
    ) -> Result<T, ControlStateError> {
        self.blocking_command_with_ack_deadline(
            enqueue_deadline,
            ack_timeout,
            None,
            command,
            timeout_policy,
        )
    }

    fn blocking_command_with_ack_deadline<T>(
        &self,
        enqueue_deadline: std::time::Instant,
        ack_timeout: Duration,
        absolute_ack_deadline: Option<std::time::Instant>,
        mut command: impl FnMut(oneshot::Sender<Result<T, ControlStateError>>) -> ControlCommand,
        timeout_policy: BlockingEnqueueTimeoutPolicy,
    ) -> Result<T, ControlStateError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        if !self.is_healthy() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        let receive = loop {
            if !self.is_healthy() {
                return Err(ControlStateError::DurableStateUnavailable);
            }
            if std::time::Instant::now() >= enqueue_deadline {
                return Err(self.blocking_enqueue_timeout(timeout_policy));
            }
            let (reply, receive) = oneshot::channel();
            let command = command(reply);
            if !self.is_healthy() {
                return Err(ControlStateError::DurableStateUnavailable);
            }
            if std::time::Instant::now() >= enqueue_deadline {
                return Err(self.blocking_enqueue_timeout(timeout_policy));
            }
            match self.sender.try_send(command) {
                Ok(()) => break receive,
                Err(sync_mpsc::TrySendError::Full(_)) => {
                    let now = std::time::Instant::now();
                    if now >= enqueue_deadline {
                        return Err(self.blocking_enqueue_timeout(timeout_policy));
                    }
                    std::thread::sleep(ENQUEUE_RETRY.min(enqueue_deadline - now));
                }
                Err(sync_mpsc::TrySendError::Disconnected(_)) => {
                    return Err(self.poison_unavailable());
                }
            }
        };
        let ack_deadline = absolute_ack_deadline
            .unwrap_or_else(|| checked_deadline_after(std::time::Instant::now(), ack_timeout));
        self.receive_blocking_ack_until(receive, ack_deadline)
    }

    fn blocking_enqueue_timeout(&self, policy: BlockingEnqueueTimeoutPolicy) -> ControlStateError {
        match policy {
            BlockingEnqueueTimeoutPolicy::AdmissionOverloaded => {
                ControlStateError::WriterOverloaded
            }
            BlockingEnqueueTimeoutPolicy::RequiredObservationUnavailable => {
                self.poison_unavailable()
            }
        }
    }

    fn receive_blocking_ack_until<T>(
        &self,
        mut receive: oneshot::Receiver<Result<T, ControlStateError>>,
        ack_deadline: std::time::Instant,
    ) -> Result<T, ControlStateError> {
        loop {
            if std::time::Instant::now() >= ack_deadline {
                return Err(self.poison_unknown_commit());
            }
            match receive.try_recv() {
                Ok(result) => return result,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    return Err(self.poison_unknown_commit());
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    let now = std::time::Instant::now();
                    if now >= ack_deadline {
                        return Err(self.poison_unknown_commit());
                    }
                    std::thread::sleep(ENQUEUE_RETRY.min(ack_deadline - now));
                }
            }
        }
    }
}

pub(crate) struct ControlStateWorker {
    sender: std::sync::Weak<sync_mpsc::SyncSender<ControlCommand>>,
    join: Option<std::thread::JoinHandle<Result<(), ControlStateError>>>,
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
    shutdown: Option<ControlStateShutdownProgress>,
    #[cfg(test)]
    reaper_finished_for_test: Option<sync_mpsc::SyncSender<()>>,
}

struct ControlStateShutdownProgress {
    acknowledgement: oneshot::Receiver<()>,
    acknowledged: bool,
}

#[must_use = "shutdown failure retains the durable writer owner and must be resolved"]
pub(crate) struct ControlStateShutdownFailure {
    error: ControlStateError,
    worker: ManuallyDrop<Box<ControlStateWorker>>,
}

impl ControlStateShutdownFailure {
    fn new(error: ControlStateError, worker: ControlStateWorker) -> Self {
        Self {
            error,
            worker: ManuallyDrop::new(Box::new(worker)),
        }
    }

    pub(crate) fn error(&self) -> ControlStateError {
        self.error
    }

    pub(crate) fn into_worker(mut self) -> ControlStateWorker {
        // SAFETY: `self.worker` is taken exactly once and `Drop` deliberately
        // leaves an unhandled owner retained rather than detaching it.
        let worker = unsafe { ManuallyDrop::take(&mut self.worker) };
        std::mem::forget(self);
        *worker
    }

    #[cfg(test)]
    fn retains_writer_for_test(&self) -> bool {
        self.worker.join.is_some() || self.worker.shutdown.is_some()
    }
}

impl std::fmt::Debug for ControlStateShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlStateShutdownFailure")
            .field("error", &self.error)
            .field("retains_writer", &true)
            .finish()
    }
}

impl Drop for ControlStateShutdownFailure {
    fn drop(&mut self) {
        // Fail closed: ignoring the actionable failure must not detach or
        // implicitly release the writer/repository owner.
    }
}

impl ControlStateWorker {
    pub(crate) fn request_shutdown_blocking_until(
        &mut self,
        deadline: std::time::Instant,
    ) -> Result<(), ControlStateError> {
        if self.shutdown.is_some() {
            return Ok(());
        }
        let (reply, acknowledgement) = oneshot::channel();
        let Some(sender) = self.sender.upgrade() else {
            self.shutdown = Some(ControlStateShutdownProgress {
                acknowledgement,
                acknowledged: false,
            });
            return Ok(());
        };
        let mut command = ControlCommand::Stop { reply };
        loop {
            match sender.try_send(command) {
                Ok(()) => {
                    self.shutdown = Some(ControlStateShutdownProgress {
                        acknowledgement,
                        acknowledged: false,
                    });
                    return Ok(());
                }
                Err(sync_mpsc::TrySendError::Full(returned)) => {
                    command = returned;
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        poison(&self.healthy, &self.health_signal);
                        return Err(ControlStateError::ShutdownDeadlineExceeded);
                    }
                    std::thread::sleep(ENQUEUE_RETRY.min(deadline - now));
                }
                Err(sync_mpsc::TrySendError::Disconnected(_)) => {
                    self.shutdown = Some(ControlStateShutdownProgress {
                        acknowledgement,
                        acknowledged: false,
                    });
                    return Ok(());
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn open_reconcile_and_spawn(
        init: ControlStateInit,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        match Self::open_reconcile_and_spawn_until(
            init,
            checked_deadline_after(std::time::Instant::now(), CONTROL_STARTUP_TIMEOUT),
        ) {
            Ok(bootstrap) => Ok(bootstrap),
            Err(failure) => Err(
                failure.dispose_and_return_error_for_test(checked_deadline_after(
                    std::time::Instant::now(),
                    CONTROL_STARTUP_TIMEOUT,
                )),
            ),
        }
    }

    pub(crate) fn open_reconcile_and_spawn_until(
        init: ControlStateInit,
        absolute_deadline: std::time::Instant,
    ) -> Result<ControlStateBootstrap, ControlStateStartupFailure> {
        Self::open_reconcile_and_spawn_inner(init, absolute_deadline, StartupBehavior::Normal)
    }

    fn open_reconcile_and_spawn_inner(
        init: ControlStateInit,
        absolute_deadline: std::time::Instant,
        startup_behavior: StartupBehavior,
    ) -> Result<ControlStateBootstrap, ControlStateStartupFailure> {
        if startup_is_permanently_poisoned() {
            return Err(ControlStateStartupFailure::unstarted(
                ControlStateError::DurableStateUnavailable,
                init,
            ));
        }
        let (sender, receiver) = sync_mpsc::sync_channel(COMMAND_CAPACITY);
        let sender = Arc::new(sender);
        let worker_sender = Arc::downgrade(&sender);
        let healthy = Arc::new(AtomicBool::new(true));
        let (health_signal, _health_receiver) = watch::channel(true);
        let worker_health = Arc::clone(&healthy);
        let worker_health_signal = health_signal.clone();
        let (initialized, initialization) =
            std::sync::mpsc::sync_channel::<Result<InitializedControlState, ControlStateError>>(1);
        let (completed, completion) =
            std::sync::mpsc::sync_channel::<Result<(), ControlStateError>>(1);
        let (ownership, claimed_ownership) = sync_mpsc::sync_channel::<ControlStateInit>(1);
        let join = match std::thread::Builder::new()
            .name("loxa-control-state".to_owned())
            .spawn(move || {
                let init = claimed_ownership
                    .recv()
                    .map_err(|_| ControlStateError::DurableStateUnavailable)?;
                match &startup_behavior {
                    StartupBehavior::Normal => {}
                    #[cfg(test)]
                    StartupBehavior::PanicBeforeInitialization => {
                        panic!("fault-injected control-state startup panic")
                    }
                    #[cfg(test)]
                    StartupBehavior::BlockBeforePublication { .. } => {}
                }
                let ControlStateOpenInput {
                    claimed_owner,
                    first_migration_source,
                } = init.open_input;
                let startup = (|| {
                    let evidence = if let Some(source) = first_migration_source.as_ref() {
                        RecoveryEvidence::ExactAbsent(
                            ExactAbsenceProof::from_first_migration_claim(&claimed_owner, source)
                                .map_err(|_| ControlStateError::DurableStateUnavailable)?,
                        )
                    } else {
                        init.recovery_evidence
                    };
                    let mut ids = WorkerIds;
                    let mut repository = ControlRepository::open_or_migrate(
                        init.path.as_ref(),
                        init.node_id,
                        first_migration_source,
                        &mut ids,
                    )
                    .map_err(|error| ControlStateError::Repository(error.class()))?;
                    let requires_specialized_recovery = repository
                        .requires_specialized_migration_recovery()
                        .map_err(|error| ControlStateError::Repository(error.class()))?;
                    if requires_specialized_recovery
                        && !repository
                            .specialized_migration_recovery_is_safe()
                            .map_err(|error| ControlStateError::Repository(error.class()))?
                    {
                        repository
                            .close()
                            .map_err(|_| ControlStateError::DurableStateUnavailable)?;
                        return Err(ControlStateError::DurableStateUnavailable);
                    }
                    let captured_intent = repository
                        .lifecycle_intent_snapshot()
                        .map_err(ControlStateError::Transition)?;
                    let reconciled = repository
                        .reconcile_restart(
                            RestartEvidence {
                                lifecycle: evidence,
                                captured_intent,
                            },
                            init.now_unix_ms,
                            &mut ids,
                        )
                        .map_err(ControlStateError::Transition)?;
                    let initial = Arc::new(
                        repository
                            .committed_state()
                            .map_err(|error| ControlStateError::Repository(error.class()))?,
                    );
                    let snapshot = Arc::new(RwLock::new(initial));
                    let pending_progress = Arc::new(Mutex::new(HashMap::new()));
                    #[cfg(test)]
                    if let StartupBehavior::BlockBeforePublication { entered, release } =
                        &startup_behavior
                    {
                        entered.wait();
                        release.wait();
                    }
                    initialized
                        .send(Ok(InitializedControlState {
                            snapshot: Arc::clone(&snapshot),
                            pending_progress: Arc::clone(&pending_progress),
                            claimed_owner,
                            ready_authority: reconciled.ready_authority,
                        }))
                        .map_err(|_| ControlStateError::DurableStateUnavailable)?;
                    run_worker(
                        repository,
                        receiver,
                        None,
                        snapshot,
                        worker_health,
                        worker_health_signal,
                        pending_progress,
                    )
                })();
                if let Err(error) = startup {
                    let _ = initialized.try_send(Err(error));
                }
                let _ = completed.try_send(startup);
                startup
            }) {
            Ok(join) => join,
            Err(_) => {
                drop(ownership);
                return Err(ControlStateStartupFailure::unstarted(
                    ControlStateError::DurableStateUnavailable,
                    init,
                ));
            }
        };
        if let Err(error) = ownership.send(init) {
            return Err(ControlStateStartupFailure::new_with_unstarted_init(
                ControlStateError::DurableStateUnavailable,
                sender,
                join,
                initialization,
                completion,
                healthy,
                health_signal,
                Some(error.0),
            ));
        }
        let remaining = absolute_deadline.saturating_duration_since(std::time::Instant::now());
        let initialized = match initialization.recv_timeout(remaining) {
            Ok(Ok(initialized)) => initialized,
            Ok(Err(error)) => {
                return Err(ControlStateStartupFailure::new(
                    error,
                    sender,
                    join,
                    initialization,
                    completion,
                    healthy,
                    health_signal,
                ));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ControlStateStartupFailure::new(
                    ControlStateError::WorkerPanicked,
                    sender,
                    join,
                    initialization,
                    completion,
                    healthy,
                    health_signal,
                ));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(ControlStateStartupFailure::new(
                    ControlStateError::ShutdownDeadlineExceeded,
                    sender,
                    join,
                    initialization,
                    completion,
                    healthy,
                    health_signal,
                ));
            }
        };
        let InitializedControlState {
            snapshot,
            pending_progress,
            claimed_owner,
            ready_authority,
        } = initialized;
        let handle = ControlStateHandle {
            sender,
            snapshot,
            healthy: Arc::clone(&healthy),
            health_signal: health_signal.clone(),
            pending_progress,
        };
        Ok(ControlStateBootstrap {
            handle,
            worker: Self {
                sender: worker_sender,
                join: Some(join),
                healthy,
                health_signal,
                shutdown: None,
                #[cfg(test)]
                reaper_finished_for_test: None,
            },
            claimed_owner,
            ready_authority,
        })
    }

    #[cfg(test)]
    pub(super) fn panic_during_initialization_for_test(
        init: ControlStateInit,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        match Self::open_reconcile_and_spawn_inner(
            init,
            checked_deadline_after(std::time::Instant::now(), Duration::from_secs(1)),
            StartupBehavior::PanicBeforeInitialization,
        ) {
            Ok(bootstrap) => Ok(bootstrap),
            Err(failure) => Err(
                failure.dispose_and_return_error_for_test(checked_deadline_after(
                    std::time::Instant::now(),
                    Duration::from_secs(1),
                )),
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn block_after_durable_initialization_before_publication_for_test(
        init: ControlStateInit,
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        match Self::open_reconcile_and_spawn_inner(
            init,
            checked_deadline_after(std::time::Instant::now(), Duration::from_millis(50)),
            StartupBehavior::BlockBeforePublication { entered, release },
        ) {
            Ok(bootstrap) => Ok(bootstrap),
            Err(failure) => Err(
                failure.dispose_and_return_error_for_test(checked_deadline_after(
                    std::time::Instant::now(),
                    SHUTDOWN_TIMEOUT,
                )),
            ),
        }
    }

    pub(crate) async fn shutdown_until(
        mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlStateShutdownFailure> {
        if self.shutdown.is_none() {
            let (reply, acknowledgement) = oneshot::channel();
            let Some(sender) = self.sender.upgrade() else {
                self.shutdown = Some(ControlStateShutdownProgress {
                    acknowledgement,
                    acknowledged: false,
                });
                return self.await_shutdown_completion_until(deadline).await;
            };
            let mut command = ControlCommand::Stop { reply };
            loop {
                match sender.try_send(command) {
                    Ok(()) => break,
                    Err(sync_mpsc::TrySendError::Full(returned)) => {
                        command = returned;
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            poison(&self.healthy, &self.health_signal);
                            return Err(ControlStateShutdownFailure::new(
                                ControlStateError::ShutdownDeadlineExceeded,
                                self,
                            ));
                        }
                        tokio::time::sleep(ENQUEUE_RETRY.min(deadline - now)).await;
                    }
                    Err(sync_mpsc::TrySendError::Disconnected(_)) => break,
                }
            }
            self.shutdown = Some(ControlStateShutdownProgress {
                acknowledgement,
                acknowledged: false,
            });
        }
        self.await_shutdown_completion_until(deadline).await
    }

    async fn await_shutdown_completion_until(
        mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ControlStateShutdownFailure> {
        if self.join.is_none() {
            poison(&self.healthy, &self.health_signal);
            return Err(ControlStateShutdownFailure::new(
                ControlStateError::DurableStateUnavailable,
                self,
            ));
        }
        loop {
            let progress = self.shutdown.as_mut().expect("shutdown progress present");
            if !progress.acknowledged {
                match progress.acknowledgement.try_recv() {
                    Ok(()) => progress.acknowledged = true,
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {}
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                }
            }
            if self
                .join
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
            {
                let join = self.join.take().expect("finished worker join present");
                let result = join.join();
                #[cfg(test)]
                if let Some(finished) = self.reaper_finished_for_test.take() {
                    let _ = finished.send(());
                }
                poison(&self.healthy, &self.health_signal);
                let acknowledged = self
                    .shutdown
                    .as_ref()
                    .expect("shutdown progress present")
                    .acknowledged;
                return match result {
                    Ok(Ok(())) if acknowledged => Ok(()),
                    Ok(Ok(())) => Err(ControlStateShutdownFailure::new(
                        ControlStateError::DurableStateUnavailable,
                        self,
                    )),
                    Ok(Err(error)) => Err(ControlStateShutdownFailure::new(error, self)),
                    Err(_) => Err(ControlStateShutdownFailure::new(
                        ControlStateError::WorkerPanicked,
                        self,
                    )),
                };
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                poison(&self.healthy, &self.health_signal);
                return Err(ControlStateShutdownFailure::new(
                    ControlStateError::ShutdownDeadlineExceeded,
                    self,
                ));
            }
            tokio::time::sleep(ENQUEUE_RETRY.min(deadline - now)).await;
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), ControlStateError> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        match self.shutdown_until(deadline).await {
            Ok(()) => Ok(()),
            Err(failure) => {
                let error = failure.error();
                let worker = failure.into_worker();
                let _ = worker.await_shutdown_completion_owned().await;
                Err(error)
            }
        }
    }

    async fn await_shutdown_completion_owned(mut self) -> Result<(), ControlStateShutdownFailure> {
        if self.shutdown.is_none() {
            let (reply, acknowledgement) = oneshot::channel();
            if let Some(sender) = self.sender.upgrade() {
                let mut command = ControlCommand::Stop { reply };
                loop {
                    match sender.try_send(command) {
                        Ok(()) => break,
                        Err(sync_mpsc::TrySendError::Full(returned)) => {
                            command = returned;
                            tokio::time::sleep(ENQUEUE_RETRY).await;
                        }
                        Err(sync_mpsc::TrySendError::Disconnected(_)) => break,
                    }
                }
            }
            self.shutdown = Some(ControlStateShutdownProgress {
                acknowledgement,
                acknowledged: false,
            });
        }
        while self.join.as_ref().is_some_and(|join| !join.is_finished()) {
            let progress = self.shutdown.as_mut().expect("shutdown progress present");
            if !progress.acknowledged {
                if let Ok(()) = progress.acknowledgement.try_recv() {
                    progress.acknowledged = true;
                }
            }
            tokio::time::sleep(ENQUEUE_RETRY).await;
        }
        self.await_shutdown_completion_until(tokio::time::Instant::now())
            .await
    }

    pub(crate) fn shutdown_blocking(self) -> Result<(), ControlStateError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|_| ControlStateError::DurableStateUnavailable)?
            .block_on(self.shutdown())
    }

    pub(crate) fn shutdown_blocking_until(
        self,
        deadline: std::time::Instant,
    ) -> Result<(), ControlStateShutdownFailure> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ControlStateShutdownFailure::new(
                ControlStateError::DurableStateUnavailable,
                self,
            ));
        }
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => {
                return Err(ControlStateShutdownFailure::new(
                    ControlStateError::DurableStateUnavailable,
                    self,
                ));
            }
        };
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let async_deadline = tokio::time::Instant::now() + remaining;
        runtime.block_on(self.shutdown_until(async_deadline))
    }

    #[cfg(test)]
    pub(super) fn wait_for_exit_for_test(&self, timeout: Duration) -> Result<(), &'static str> {
        let deadline = checked_deadline_after(std::time::Instant::now(), timeout);
        while self
            .join
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err("control-state test worker did not finish");
            }
            std::thread::sleep(ENQUEUE_RETRY.min(deadline - now));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn join_for_test(mut self) {
        if let Some(join) = self.join.take() {
            join.join()
                .expect("control-state test worker must not panic")
                .expect("control-state test worker must close repository");
        }
    }
}

impl Drop for ControlStateWorker {
    fn drop(&mut self) {
        poison(&self.healthy, &self.health_signal);
        // Explicit shutdown owns cleanup. A forgotten owner fails closed: it
        // neither requests ordinary Drop shutdown nor spawns detached cleanup.
        if let Some(join) = self.join.take() {
            std::mem::forget(join);
        }
    }
}

fn poison(healthy: &AtomicBool, health_signal: &watch::Sender<bool>) {
    healthy.store(false, Ordering::Release);
    health_signal.send_replace(false);
}

#[cfg(test)]
pub(super) fn spawn_from_repository_for_test(
    repository: ControlRepository,
) -> Result<(ControlStateHandle, ControlStateWorker), ControlStateError> {
    spawn(repository, None, true)
}

#[cfg(test)]
pub(super) fn spawn_unpublished_from_repository_for_test(
    repository: ControlRepository,
) -> Result<(ControlStateHandle, ControlStateWorker), ControlStateError> {
    spawn(repository, None, false)
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
    let result = spawn(repository, Some(Arc::clone(&barrier)), true)?;
    Ok((result.0, result.1, barrier))
}

#[cfg(test)]
pub(super) fn spawn_paused_with_reaper_completion_for_test(
    repository: ControlRepository,
) -> Result<
    (
        ControlStateHandle,
        ControlStateWorker,
        Arc<std::sync::Barrier>,
        sync_mpsc::Receiver<()>,
    ),
    ControlStateError,
> {
    let (handle, mut worker, barrier) = spawn_paused_from_repository_for_test(repository)?;
    let (finished, receive) = sync_mpsc::sync_channel(1);
    worker.reaper_finished_for_test = Some(finished);
    Ok((handle, worker, barrier, receive))
}

fn spawn(
    repository: ControlRepository,
    start_barrier: Option<Arc<std::sync::Barrier>>,
    require_published: bool,
) -> Result<(ControlStateHandle, ControlStateWorker), ControlStateError> {
    let initial = Arc::new(
        repository
            .committed_state()
            .map_err(|error| ControlStateError::Repository(error.class()))?,
    );
    let node_instance_id = initial.node.as_ref().map(|node| node.node_instance_id);
    if require_published && node_instance_id.is_none() {
        return Err(ControlStateError::DurableStateUnavailable);
    }
    let snapshot = Arc::new(RwLock::new(initial));
    let healthy = Arc::new(AtomicBool::new(true));
    let (health_signal, _health_receiver) = watch::channel(true);
    let pending_progress = Arc::new(Mutex::new(HashMap::new()));
    let (sender, receiver) = sync_mpsc::sync_channel(COMMAND_CAPACITY);
    let sender = Arc::new(sender);
    let worker_sender = Arc::downgrade(&sender);
    let worker_health = Arc::clone(&healthy);
    let worker_health_signal = health_signal.clone();
    let handle = ControlStateHandle {
        sender,
        snapshot: Arc::clone(&snapshot),
        healthy: Arc::clone(&healthy),
        health_signal: health_signal.clone(),
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
                worker_health,
                worker_health_signal,
                pending_progress,
            )
        })
        .map_err(|_| ControlStateError::DurableStateUnavailable)?;
    Ok((
        handle,
        ControlStateWorker {
            sender: worker_sender,
            join: Some(join),
            healthy,
            health_signal,
            shutdown: None,
            #[cfg(test)]
            reaper_finished_for_test: None,
        },
    ))
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
                Err(sync_mpsc::TrySendError::Full(_)) => break,
                Err(sync_mpsc::TrySendError::Disconnected(_)) => panic!("test queue closed"),
            }
        }
    }

    pub(crate) fn admit_and_drop_ack_for_test(&self, request: AdmissionRequest) {
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

    pub(super) async fn observe_lifecycle_with_snapshot_failure_for_test(
        &self,
        transition: Transition,
        observation: LifecycleObservation,
    ) -> Result<CommitReceipt, ControlStateError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::ObserveLifecycleWithSnapshotFailure {
                transition,
                observation,
                reply,
            })
            .expect("fault-injected lifecycle observation must enqueue");
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) fn observe_and_drop_ack_for_test(
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

    pub(super) async fn panic_worker_for_test(&self) {
        let (entered, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Panic { entered })
            .expect("panic command must enqueue");
        receive.await.expect("worker must enter panic command");
    }

    pub(super) async fn block_worker_for_test(&self) -> Arc<std::sync::Barrier> {
        let release = Arc::new(std::sync::Barrier::new(2));
        let (entered, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::Block {
                entered,
                release: Arc::clone(&release),
            })
            .expect("block command must enqueue");
        receive.await.expect("worker must enter block command");
        release
    }

    pub(crate) fn poison_for_test(&self) {
        let _ = self.poison_unavailable();
    }

    pub(super) async fn trigger_snapshot_failure_and_block_cleanup_for_test(
        &self,
        request: AdmissionRequest,
    ) -> Arc<std::sync::Barrier> {
        let release = Arc::new(std::sync::Barrier::new(2));
        let (entered, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::AdmitWithSnapshotFailureAndBlockCleanup {
                request,
                entered,
                release: Arc::clone(&release),
            })
            .expect("uncertainty command must enqueue");
        receive
            .await
            .expect("worker must reach blocked cleanup after uncertainty");
        release
    }

    pub(super) async fn cancel_subscribe_for_test(&self) {
        let (reply, receive) = oneshot::channel();
        drop(receive);
        self.sender
            .try_send(ControlCommand::Subscribe {
                requested: None,
                generated_at_unix_ms: DecimalU64::new(10),
                max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
                health: self.health_signal.subscribe(),
                reply,
            })
            .expect("cancelled subscription command must enqueue");
    }

    pub(super) async fn subscriber_count_for_test(&self) -> usize {
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(ControlCommand::SubscriberCount { reply })
            .expect("subscriber count command must enqueue");
        receive.await.expect("worker must report subscriber count")
    }

    pub(super) fn admit_blocking_with_timeouts_for_test(
        &self,
        request: AdmissionRequest,
        enqueue_timeout: Duration,
        ack_timeout: Duration,
    ) -> Result<CommittedAdmission, ControlStateError> {
        self.admit_blocking_with_deadlines(
            request,
            checked_deadline_after(std::time::Instant::now(), enqueue_timeout),
            ack_timeout,
        )
    }

    pub(super) fn observe_required_blocking_with_timeouts_for_test(
        &self,
        transition: Transition,
        enqueue_timeout: Duration,
        ack_timeout: Duration,
    ) -> Result<CommitReceipt, ControlStateError> {
        self.observe_required_blocking_with_deadlines(
            transition,
            checked_deadline_after(std::time::Instant::now(), enqueue_timeout),
            ack_timeout,
        )
    }

    pub(super) fn receive_blocking_ack_until_for_test<T>(
        &self,
        receive: oneshot::Receiver<Result<T, ControlStateError>>,
        ack_deadline: std::time::Instant,
    ) -> Result<T, ControlStateError> {
        self.receive_blocking_ack_until(receive, ack_deadline)
    }
}

#[cfg(test)]
pub(super) struct SyntheticQueue {
    pub(super) handle: ControlStateHandle,
    receiver: sync_mpsc::Receiver<ControlCommand>,
}

#[cfg(test)]
impl SyntheticQueue {
    pub(super) fn drain_contains_observe_for_test(&mut self) -> bool {
        let mut observed = false;
        loop {
            match self.receiver.try_recv() {
                Ok(ControlCommand::Observe { .. }) => observed = true,
                Ok(_) => {}
                Err(sync_mpsc::TryRecvError::Empty) => return observed,
                Err(sync_mpsc::TryRecvError::Disconnected) => return observed,
            }
        }
    }

    async fn receive(&mut self) -> ControlCommand {
        loop {
            match self.receiver.try_recv() {
                Ok(command) => return command,
                Err(sync_mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(sync_mpsc::TryRecvError::Disconnected) => {
                    panic!("synthetic queue disconnected")
                }
            }
        }
    }

    pub(super) async fn pop_one(&mut self) {
        assert!(matches!(self.receive().await, ControlCommand::Noop));
    }

    pub(super) async fn take_observe_reply(
        &mut self,
    ) -> oneshot::Sender<Result<CommitReceipt, ControlStateError>> {
        loop {
            match self.receive().await {
                ControlCommand::Observe { reply, .. } => return reply,
                ControlCommand::Noop => {}
                _ => panic!("unexpected synthetic queue command"),
            }
        }
    }

    pub(super) async fn take_admit_reply(
        &mut self,
    ) -> oneshot::Sender<Result<CommittedAdmission, ControlStateError>> {
        loop {
            match self.receive().await {
                ControlCommand::Admit { reply, .. } => return reply,
                ControlCommand::Noop => {}
                _ => panic!("unexpected synthetic queue command"),
            }
        }
    }
}

#[cfg(test)]
pub(super) fn synthetic_queue_for_test(state: CommittedState) -> SyntheticQueue {
    let (sender, receiver) = sync_mpsc::sync_channel(COMMAND_CAPACITY);
    let (health_signal, _health_receiver) = watch::channel(true);
    SyntheticQueue {
        handle: ControlStateHandle {
            sender: Arc::new(sender),
            snapshot: Arc::new(RwLock::new(Arc::new(state))),
            healthy: Arc::new(AtomicBool::new(true)),
            health_signal,
            pending_progress: Arc::new(Mutex::new(HashMap::new())),
        },
        receiver,
    }
}

fn run_worker(
    mut repository: ControlRepository,
    receiver: sync_mpsc::Receiver<ControlCommand>,
    mut node_instance_id: Option<NodeInstanceId>,
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
) -> Result<(), ControlStateError> {
    let _health_guard = WorkerHealthGuard {
        healthy: Arc::clone(&healthy),
        health_signal: health_signal.clone(),
    };
    let mut ids = WorkerIds;
    let mut subscribers = Vec::new();
    while let Ok(command) = receiver.recv() {
        if !healthy.load(Ordering::Acquire) {
            break;
        }
        if let ControlCommand::Stop { reply } = command {
            poison(&healthy, &health_signal);
            let _ = reply.send(());
            break;
        }
        #[cfg(test)]
        if let ControlCommand::Panic { entered } = command {
            let _ = entered.send(());
            panic!("fault-injected control-state worker panic");
        }
        #[cfg(test)]
        if let ControlCommand::Block { entered, release } = command {
            let _ = entered.send(());
            release.wait();
            continue;
        }
        process_command(
            command,
            &mut repository,
            &mut node_instance_id,
            &mut ids,
            WorkerPublication {
                snapshot: &snapshot,
                healthy: &healthy,
                health_signal: &health_signal,
                subscribers: &mut subscribers,
            },
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
                &mut node_instance_id,
                &mut ids,
                WorkerPublication {
                    snapshot: &snapshot,
                    healthy: &healthy,
                    health_signal: &health_signal,
                    subscribers: &mut subscribers,
                },
            );
        }
    }
    subscribers.clear();
    repository.close().map_err(|error| {
        poison(&healthy, &health_signal);
        ControlStateError::Repository(error.class())
    })
}

struct WorkerPublication<'a> {
    snapshot: &'a RwLock<Arc<CommittedState>>,
    healthy: &'a AtomicBool,
    health_signal: &'a watch::Sender<bool>,
    subscribers: &'a mut Vec<mpsc::Sender<V2ControlEvent>>,
}

fn process_command(
    command: ControlCommand,
    repository: &mut ControlRepository,
    node_instance_id: &mut Option<NodeInstanceId>,
    ids: &mut WorkerIds,
    mut publication: WorkerPublication<'_>,
) {
    match command {
        ControlCommand::PublishInstance {
            publication: requested,
            reply,
        } => {
            if node_instance_id.is_some() {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            }
            let published_instance = requested.node_instance_id;
            let result = map_transition_result(
                repository.publish_instance(requested, ids),
                publication.healthy,
                publication.health_signal,
            );
            if result.is_ok() {
                *node_instance_id = Some(published_instance);
            }
            finish_commit(repository, result, reply, &mut publication, false);
        }
        ControlCommand::BeginStopping { now_unix_ms, reply } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.begin_stopping(instance, now_unix_ms, ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, false);
        }
        ControlCommand::Admit { request, reply } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.admit(instance, request, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, false);
        }
        ControlCommand::Observe { transition, reply } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.observe(instance, transition, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, false);
        }
        ControlCommand::ObserveLifecycle {
            transition,
            observation,
            reply,
        } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.observe_lifecycle(instance, transition, observation, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, false);
        }
        ControlCommand::Subscribe {
            requested,
            generated_at_unix_ms,
            max_snapshot_bytes,
            health,
            reply,
        } => {
            publication
                .subscribers
                .retain(|subscriber| !subscriber.is_closed());
            let state = publication
                .snapshot
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            match build_reconnect_snapshot(
                &state,
                requested,
                generated_at_unix_ms,
                max_snapshot_bytes,
            ) {
                Ok(snapshot) => {
                    let (events, receiver) = mpsc::channel(SUBSCRIBER_CAPACITY);
                    let subscription = ControlSubscription {
                        snapshot,
                        events: ControlEventReceiver {
                            events: receiver,
                            health,
                        },
                    };
                    if reply.send(Ok(subscription)).is_ok() {
                        publication.subscribers.push(events);
                    }
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
        }
        ControlCommand::Stop { .. } => unreachable!("stop is handled by the worker loop"),
        #[cfg(test)]
        ControlCommand::Noop => {}
        #[cfg(test)]
        ControlCommand::AdmitWithSnapshotFailure { request, reply } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.admit(instance, request, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, true);
        }
        #[cfg(test)]
        ControlCommand::ObserveLifecycleWithSnapshotFailure {
            transition,
            observation,
            reply,
        } => {
            let Some(instance) = *node_instance_id else {
                let _ = reply.send(Err(ControlStateError::DurableStateUnavailable));
                return;
            };
            let result = map_transition_result(
                repository.observe_lifecycle(instance, transition, observation, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            finish_commit(repository, result, reply, &mut publication, true);
        }
        #[cfg(test)]
        ControlCommand::AdmitWithSnapshotFailureAndBlockCleanup {
            request,
            entered,
            release,
        } => {
            let Some(instance) = *node_instance_id else {
                let _ = entered.send(());
                return;
            };
            let result = map_transition_result(
                repository.admit(instance, request, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            let (reply, receive) = oneshot::channel();
            drop(receive);
            finish_commit(repository, result, reply, &mut publication, true);
            let _ = entered.send(());
            release.wait();
        }
        #[cfg(test)]
        ControlCommand::ObserveAndDropAck {
            transition,
            committed,
        } => {
            let Some(instance) = *node_instance_id else {
                let _ = committed.send(());
                return;
            };
            let result = map_transition_result(
                repository.observe(instance, transition, now_unix_ms(), ids),
                publication.healthy,
                publication.health_signal,
            );
            let (reply, receive) = oneshot::channel();
            drop(receive);
            finish_commit(repository, result, reply, &mut publication, false);
            let _ = committed.send(());
        }
        #[cfg(test)]
        ControlCommand::Panic { .. } => unreachable!("panic is handled by the worker loop"),
        #[cfg(test)]
        ControlCommand::Block { .. } => unreachable!("block is handled by the worker loop"),
        #[cfg(test)]
        ControlCommand::SubscriberCount { reply } => {
            let _ = reply.send(publication.subscribers.len());
        }
    }
}

struct WorkerHealthGuard {
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
}

impl Drop for WorkerHealthGuard {
    fn drop(&mut self) {
        poison(&self.healthy, &self.health_signal);
    }
}

fn map_transition_result<T>(
    result: Result<T, TransitionError>,
    healthy: &AtomicBool,
    health_signal: &watch::Sender<bool>,
) -> Result<T, ControlStateError> {
    match result {
        Ok(value) => Ok(value),
        Err(TransitionError::Repository(_)) => {
            poison(healthy, health_signal);
            Err(ControlStateError::UnknownCommit)
        }
        Err(error) => Err(ControlStateError::Transition(error)),
    }
}

fn finish_commit<T>(
    repository: &ControlRepository,
    result: Result<T, ControlStateError>,
    reply: oneshot::Sender<Result<T, ControlStateError>>,
    publication: &mut WorkerPublication<'_>,
    force_snapshot_read_failure: bool,
) {
    let prior_cursor = publication
        .snapshot
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .cursor;
    let result = match result {
        Ok(_receipt) if force_snapshot_read_failure => {
            poison(publication.healthy, publication.health_signal);
            Err(ControlStateError::UnknownCommit)
        }
        Ok(receipt) => match repository.committed_state() {
            Ok(committed) => {
                let event = (committed.cursor > prior_cursor)
                    .then(|| committed.events.last().cloned())
                    .flatten()
                    .filter(|event| event.sequence == committed.cursor);
                *publication
                    .snapshot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(committed);
                if let Some(event) = event {
                    publication
                        .subscribers
                        .retain(|subscriber| subscriber.try_send(event.clone()).is_ok());
                }
                Ok(receipt)
            }
            Err(error) => {
                poison(publication.healthy, publication.health_signal);
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
    let generated_at_unix_ms = DecimalU64::new(
        generated_at_unix_ms
            .get()
            .max(state.last_committed_at_unix_ms.get()),
    );
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

#[cfg(test)]
mod shutdown_contract_tests {
    use super::*;

    enum ShutdownTestBehavior {
        Clean,
        BlockAfterAck(Arc<std::sync::Barrier>),
        Panic,
        DropAck,
    }

    fn shutdown_test_worker(
        behavior: ShutdownTestBehavior,
    ) -> (
        ControlStateWorker,
        Arc<sync_mpsc::SyncSender<ControlCommand>>,
    ) {
        let (sender, receiver) = sync_mpsc::sync_channel(1);
        let sender = Arc::new(sender);
        let healthy = Arc::new(AtomicBool::new(true));
        let (health_signal, _health_receiver) = watch::channel(true);
        let join = std::thread::spawn(move || match behavior {
            ShutdownTestBehavior::Panic => panic!("fault-injected shutdown panic"),
            ShutdownTestBehavior::Clean => match receiver.recv().unwrap() {
                ControlCommand::Stop { reply } => {
                    let _ = reply.send(());
                    Ok(())
                }
                _ => unreachable!("shutdown test accepts only Stop"),
            },
            ShutdownTestBehavior::BlockAfterAck(release) => match receiver.recv().unwrap() {
                ControlCommand::Stop { reply } => {
                    let _ = reply.send(());
                    release.wait();
                    Ok(())
                }
                _ => unreachable!("shutdown test accepts only Stop"),
            },
            ShutdownTestBehavior::DropAck => match receiver.recv().unwrap() {
                ControlCommand::Stop { reply } => {
                    drop(reply);
                    Ok(())
                }
                _ => unreachable!("shutdown test accepts only Stop"),
            },
        });
        (
            ControlStateWorker {
                sender: Arc::downgrade(&sender),
                join: Some(join),
                healthy,
                health_signal,
                shutdown: None,
                reaper_finished_for_test: None,
            },
            sender,
        )
    }

    #[tokio::test]
    async fn deadline_shutdown_success_acknowledges_and_joins() {
        let (worker, _sender) = shutdown_test_worker(ShutdownTestBehavior::Clean);
        worker
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn deadline_shutdown_timeout_returns_move_only_owner() {
        let release = Arc::new(std::sync::Barrier::new(2));
        let (worker, sender) =
            shutdown_test_worker(ShutdownTestBehavior::BlockAfterAck(Arc::clone(&release)));
        let failure = worker
            .shutdown_until(tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert_eq!(failure.error(), ControlStateError::ShutdownDeadlineExceeded);
        assert!(failure.retains_writer_for_test());

        release.wait();
        drop(sender);
        failure
            .into_worker()
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn deadline_shutdown_panic_returns_retained_owner() {
        let (worker, sender) = shutdown_test_worker(ShutdownTestBehavior::Panic);
        drop(sender);
        let failure = worker
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(failure.error(), ControlStateError::WorkerPanicked);
        assert!(failure.retains_writer_for_test());
    }

    #[tokio::test]
    async fn lost_ack_returns_retained_owner_after_join() {
        let (worker, _sender) = shutdown_test_worker(ShutdownTestBehavior::DropAck);
        let failure = worker
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(failure.error(), ControlStateError::DurableStateUnavailable);
        assert!(failure.retains_writer_for_test());
    }

    #[tokio::test]
    async fn lost_completion_handle_returns_retained_owner() {
        let (sender, receiver) = sync_mpsc::sync_channel(1);
        drop(receiver);
        let sender = Arc::new(sender);
        let healthy = Arc::new(AtomicBool::new(true));
        let (health_signal, _health_receiver) = watch::channel(true);
        let worker = ControlStateWorker {
            sender: Arc::downgrade(&sender),
            join: None,
            healthy,
            health_signal,
            shutdown: None,
            reaper_finished_for_test: None,
        };

        let failure = worker
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(failure.error(), ControlStateError::DurableStateUnavailable);
        assert!(failure.retains_writer_for_test());
    }
}

#[cfg(test)]
mod startup_ownership_contract_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn startup_timeout_retains_join_and_completion_until_explicit_disposal() {
        let release = Arc::new(std::sync::Barrier::new(2));
        let completed = Arc::new(AtomicUsize::new(0));
        let failure = retained_startup_failure_for_test(
            ControlStateError::ShutdownDeadlineExceeded,
            Some(Arc::clone(&release)),
            Arc::clone(&completed),
        );

        assert_eq!(failure.error(), ControlStateError::ShutdownDeadlineExceeded);
        assert!(failure.retains_worker_for_test());
        assert_eq!(completed.load(AtomicOrdering::Acquire), 0);

        release.wait();
        while completed.load(AtomicOrdering::Acquire) == 0 {
            std::thread::yield_now();
        }
        let finish_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !failure.worker_finished_for_test() && std::time::Instant::now() < finish_deadline {
            std::thread::yield_now();
        }
        assert!(failure.worker_finished_for_test());
        assert!(!failure.worker_joined_for_test());

        failure.dispose_for_test(std::time::Instant::now() + Duration::from_secs(1));
    }

    #[test]
    fn startup_panic_and_completion_disconnect_remain_retained() {
        for error in [
            ControlStateError::WorkerPanicked,
            ControlStateError::DurableStateUnavailable,
        ] {
            let completed = Arc::new(AtomicUsize::new(0));
            let failure = retained_startup_failure_for_test(error, None, Arc::clone(&completed));
            assert_eq!(failure.error(), error);
            assert!(failure.retains_worker_for_test());
            failure.dispose_for_test(std::time::Instant::now() + Duration::from_secs(1));
        }
    }

    #[test]
    fn startup_failure_debug_is_bounded_and_does_not_consume_owner() {
        let completed = Arc::new(AtomicUsize::new(0));
        let failure = retained_startup_failure_for_test(
            ControlStateError::ShutdownDeadlineExceeded,
            None,
            completed,
        );
        let debug = format!("{failure:?}");
        assert!(debug.contains("ShutdownDeadlineExceeded"));
        assert!(debug.contains("retains_worker: true"));
        assert!(failure.retains_worker_for_test());
        failure.dispose_for_test(std::time::Instant::now() + Duration::from_secs(1));
    }

    #[test]
    fn repeated_retained_startups_have_no_static_join_registry() {
        for _ in 0..3 {
            let completed = Arc::new(AtomicUsize::new(0));
            let failure = retained_startup_failure_for_test(
                ControlStateError::ShutdownDeadlineExceeded,
                None,
                completed,
            );
            assert_eq!(startup_retained_join_registry_len_for_test(), 0);
            failure.dispose_for_test(std::time::Instant::now() + Duration::from_secs(1));
        }
    }

    #[test]
    fn dropping_startup_failure_aborts_promptly() {
        const CHILD: &str = "LOXA_CONTROL_STARTUP_FAILURE_DROP_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let completed = Arc::new(AtomicUsize::new(0));
            let failure = retained_startup_failure_for_test(
                ControlStateError::ShutdownDeadlineExceeded,
                None,
                completed,
            );
            drop(failure);
            panic!("dropping retained startup ownership returned");
        }

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("control_state::worker::startup_ownership_contract_tests::dropping_startup_failure_aborts_promptly")
            .arg("--exact")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD, "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("startup failure Drop blocked instead of aborting");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            !status.success(),
            "startup failure Drop unexpectedly returned"
        );
    }
}

#[cfg(test)]
fn retained_startup_failure_for_test(
    error: ControlStateError,
    release: Option<Arc<std::sync::Barrier>>,
    completed_count: Arc<std::sync::atomic::AtomicUsize>,
) -> ControlStateStartupFailure {
    let (sender, receiver) = sync_mpsc::sync_channel(1);
    let sender = Arc::new(sender);
    let (initialized, initialization) = sync_mpsc::sync_channel(1);
    let (completed, completion) = sync_mpsc::sync_channel(1);
    let healthy = Arc::new(AtomicBool::new(true));
    let (health_signal, _health_receiver) = watch::channel(true);
    let join = std::thread::spawn(move || {
        if let Some(release) = release {
            release.wait();
        }
        completed_count.fetch_add(1, Ordering::Release);
        match receiver.recv() {
            Ok(ControlCommand::Stop { reply }) => {
                let _ = reply.send(());
            }
            _ => return Err(ControlStateError::DurableStateUnavailable),
        }
        drop(initialized);
        let result = Ok(());
        let _ = completed.send(result);
        result
    });
    ControlStateStartupFailure::new(
        error,
        sender,
        join,
        initialization,
        completion,
        healthy,
        health_signal,
    )
}

#[cfg(test)]
fn startup_retained_join_registry_len_for_test() -> usize {
    0
}
