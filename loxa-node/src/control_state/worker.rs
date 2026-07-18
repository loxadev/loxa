use super::recovery::{ExactAbsenceProof, ExactReady, RecoveryEvidence};
use super::state_machine::{
    AdmissionRequest, CommitReceipt, CommittedAdmission, CommittedState, InstancePublication,
    MutationIds, Transition, TransitionError,
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
use std::sync::atomic::{AtomicBool, Ordering};
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
static STARTUP_PERMANENTLY_POISONED: AtomicBool = AtomicBool::new(false);
static STARTUP_REAPERS: std::sync::OnceLock<Mutex<Vec<std::thread::JoinHandle<()>>>> =
    std::sync::OnceLock::new();

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
    BlockBeforeInitialization {
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

#[derive(Clone)]
pub(crate) struct ControlStateHandle {
    sender: mpsc::Sender<ControlCommand>,
    snapshot: Arc<RwLock<Arc<CommittedState>>>,
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
    pending_progress: Arc<Mutex<HashMap<OperationId, Transition>>>,
}

impl ControlStateHandle {
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
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
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
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
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
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
            })?;
        self.receive_commit(receive, tokio::time::Instant::now() + ACK_TIMEOUT)
            .await
    }

    pub(crate) fn begin_stopping_blocking_until(
        &self,
        now_unix_ms: u64,
        enqueue_deadline: std::time::Instant,
    ) -> Result<CommitReceipt, ControlStateError> {
        let maximum = checked_deadline_after(std::time::Instant::now(), ENQUEUE_TIMEOUT);
        self.blocking_command(
            enqueue_deadline.min(maximum),
            ACK_TIMEOUT,
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
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
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
                mpsc::error::TrySendError::Full(_) => ControlStateError::WriterOverloaded,
                mpsc::error::TrySendError::Closed(_) => self.poison_unavailable(),
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
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let now = std::time::Instant::now();
                    if now >= enqueue_deadline {
                        return Err(self.blocking_enqueue_timeout(timeout_policy));
                    }
                    std::thread::sleep(ENQUEUE_RETRY.min(enqueue_deadline - now));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
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
    sender: mpsc::WeakSender<ControlCommand>,
    join: Option<std::thread::JoinHandle<Result<(), ControlStateError>>>,
    healthy: Arc<AtomicBool>,
    health_signal: watch::Sender<bool>,
}

impl ControlStateWorker {
    pub(crate) fn open_reconcile_and_spawn(
        init: ControlStateInit,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        Self::open_reconcile_and_spawn_inner(
            init,
            CONTROL_STARTUP_TIMEOUT,
            CONTROL_STARTUP_TIMEOUT,
            StartupBehavior::Normal,
        )
    }

    fn open_reconcile_and_spawn_inner(
        init: ControlStateInit,
        startup_timeout: Duration,
        cleanup_timeout: Duration,
        startup_behavior: StartupBehavior,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        if STARTUP_PERMANENTLY_POISONED.load(Ordering::Acquire) {
            return Err(ControlStateError::DurableStateUnavailable);
        }
        type Initialized = (
            Arc<RwLock<Arc<CommittedState>>>,
            Arc<Mutex<HashMap<OperationId, Transition>>>,
            NodeOwnerGuard,
            Option<ExactReady>,
        );

        let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let worker_sender = sender.downgrade();
        let healthy = Arc::new(AtomicBool::new(true));
        let (health_signal, _health_receiver) = watch::channel(true);
        let worker_health = Arc::clone(&healthy);
        let worker_health_signal = health_signal.clone();
        let (initialized, initialization) =
            std::sync::mpsc::sync_channel::<Result<Initialized, ControlStateError>>(1);
        let join = std::thread::Builder::new()
            .name("loxa-control-state".to_owned())
            .spawn(move || {
                match startup_behavior {
                    StartupBehavior::Normal => {}
                    #[cfg(test)]
                    StartupBehavior::PanicBeforeInitialization => {
                        panic!("fault-injected control-state startup panic")
                    }
                    #[cfg(test)]
                    StartupBehavior::BlockBeforeInitialization { entered, release } => {
                        entered.wait();
                        release.wait();
                    }
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
                    let reconciled = repository
                        .reconcile_offline(evidence, init.now_unix_ms, &mut ids)
                        .map_err(ControlStateError::Transition)?;
                    let initial = Arc::new(
                        repository
                            .committed_state()
                            .map_err(|error| ControlStateError::Repository(error.class()))?,
                    );
                    let snapshot = Arc::new(RwLock::new(initial));
                    let pending_progress = Arc::new(Mutex::new(HashMap::new()));
                    initialized
                        .send(Ok((
                            Arc::clone(&snapshot),
                            Arc::clone(&pending_progress),
                            claimed_owner,
                            reconciled.ready_authority,
                        )))
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
                    return Err(error);
                }
                Ok(())
            })
            .map_err(|_| ControlStateError::DurableStateUnavailable)?;
        let (snapshot, pending_progress, claimed_owner, ready_authority) =
            match initialization.recv_timeout(startup_timeout) {
                Ok(Ok(initialized)) => initialized,
                Ok(Err(error)) => {
                    cleanup_failed_initialization(
                        &sender,
                        join,
                        std::time::Instant::now() + cleanup_timeout,
                    )?;
                    return Err(error);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return match cleanup_failed_initialization(
                        &sender,
                        join,
                        std::time::Instant::now() + cleanup_timeout,
                    ) {
                        Err(ControlStateError::WorkerPanicked) => {
                            Err(ControlStateError::WorkerPanicked)
                        }
                        Err(error) => Err(error),
                        Ok(()) => Err(ControlStateError::DurableStateUnavailable),
                    };
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let cleanup = cleanup_failed_initialization(
                        &sender,
                        join,
                        std::time::Instant::now() + cleanup_timeout,
                    );
                    return match cleanup {
                        Ok(()) => Err(ControlStateError::ShutdownDeadlineExceeded),
                        Err(error) => Err(error),
                    };
                }
            };
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
            },
            claimed_owner,
            ready_authority,
        })
    }

    #[cfg(test)]
    pub(super) fn panic_during_initialization_for_test(
        init: ControlStateInit,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        Self::open_reconcile_and_spawn_inner(
            init,
            Duration::from_secs(1),
            Duration::from_secs(1),
            StartupBehavior::PanicBeforeInitialization,
        )
    }

    #[cfg(test)]
    pub(super) fn block_during_initialization_for_test(
        init: ControlStateInit,
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) -> Result<ControlStateBootstrap, ControlStateError> {
        Self::open_reconcile_and_spawn_inner(
            init,
            Duration::from_millis(50),
            Duration::from_secs(1),
            StartupBehavior::BlockBeforeInitialization { entered, release },
        )
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ControlStateError> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let reaper = self.start_reaper()?;
        let Some(sender) = self.sender.upgrade() else {
            return await_reaper(reaper, deadline, &self.healthy, &self.health_signal).await;
        };
        let (reply, mut acknowledgement) = oneshot::channel();
        let mut command = ControlCommand::Stop { reply };
        loop {
            match sender.try_send(command) {
                Ok(()) => break,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    command = returned;
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(shutdown_deadline(&self.healthy, &self.health_signal));
                    }
                    tokio::time::sleep(ENQUEUE_RETRY.min(deadline - now)).await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return await_reaper(reaper, deadline, &self.healthy, &self.health_signal)
                        .await;
                }
            }
        }
        tokio::pin!(reaper);
        let acknowledged = tokio::select! {
            result = &mut acknowledgement => result.is_ok(),
            result = &mut reaper => return classify_join(result, false, &self.healthy, &self.health_signal),
            () = tokio::time::sleep_until(deadline) => {
                return Err(shutdown_deadline(&self.healthy, &self.health_signal));
            }
        };
        if !acknowledged {
            return match tokio::time::timeout_at(deadline, &mut reaper).await {
                Ok(result) => classify_join(result, false, &self.healthy, &self.health_signal),
                Err(_) => Err(shutdown_deadline(&self.healthy, &self.health_signal)),
            };
        }
        match tokio::time::timeout_at(deadline, &mut reaper).await {
            Ok(result) => classify_join(result, true, &self.healthy, &self.health_signal),
            Err(_) => Err(shutdown_deadline(&self.healthy, &self.health_signal)),
        }
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

    fn start_reaper(
        &mut self,
    ) -> Result<
        oneshot::Receiver<std::thread::Result<Result<(), ControlStateError>>>,
        ControlStateError,
    > {
        let (finished, receive) = oneshot::channel();
        if let Some(join) = self.join.take() {
            if std::thread::Builder::new()
                .name("loxa-control-state-reaper".to_owned())
                .spawn(move || {
                    let _ = finished.send(join.join());
                })
                .is_err()
            {
                poison(&self.healthy, &self.health_signal);
                return Err(ControlStateError::DurableStateUnavailable);
            }
        }
        Ok(receive)
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
        if let Some(sender) = self.sender.upgrade() {
            let (reply, _receive) = oneshot::channel();
            let _ = sender.try_send(ControlCommand::Stop { reply });
        }
        if let Some(join) = self.join.take() {
            let _ = std::thread::Builder::new()
                .name("loxa-control-state-drop-reaper".to_owned())
                .spawn(move || {
                    let _ = join.join();
                });
        }
    }
}

async fn await_reaper(
    reaper: oneshot::Receiver<std::thread::Result<Result<(), ControlStateError>>>,
    deadline: tokio::time::Instant,
    healthy: &AtomicBool,
    health_signal: &watch::Sender<bool>,
) -> Result<(), ControlStateError> {
    match tokio::time::timeout_at(deadline, reaper).await {
        Ok(result) => classify_join(result, false, healthy, health_signal),
        Err(_) => Err(shutdown_deadline(healthy, health_signal)),
    }
}

fn classify_join(
    result: Result<std::thread::Result<Result<(), ControlStateError>>, oneshot::error::RecvError>,
    acknowledged: bool,
    healthy: &AtomicBool,
    health_signal: &watch::Sender<bool>,
) -> Result<(), ControlStateError> {
    poison(healthy, health_signal);
    match result {
        Ok(Ok(Ok(()))) if acknowledged => Ok(()),
        Ok(Ok(Ok(()))) | Err(_) => Err(ControlStateError::DurableStateUnavailable),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(_)) => Err(ControlStateError::WorkerPanicked),
    }
}

fn shutdown_deadline(
    healthy: &AtomicBool,
    health_signal: &watch::Sender<bool>,
) -> ControlStateError {
    poison(healthy, health_signal);
    ControlStateError::ShutdownDeadlineExceeded
}

fn poison(healthy: &AtomicBool, health_signal: &watch::Sender<bool>) {
    healthy.store(false, Ordering::Release);
    health_signal.send_replace(false);
}

fn cleanup_failed_initialization(
    sender: &mpsc::Sender<ControlCommand>,
    join: std::thread::JoinHandle<Result<(), ControlStateError>>,
    deadline: std::time::Instant,
) -> Result<(), ControlStateError> {
    let (reply, _acknowledgement) = oneshot::channel();
    let _ = sender.try_send(ControlCommand::Stop { reply });
    let (finished, completion) = std::sync::mpsc::sync_channel(1);
    let reaper = std::thread::Builder::new()
        .name("loxa-control-state-startup-reaper".to_owned())
        .spawn(move || {
            let _ = finished.send(join.join());
        })
        .map_err(|_| ControlStateError::DurableStateUnavailable)?;
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    match completion.recv_timeout(remaining) {
        Ok(Ok(Ok(()))) => {
            reaper
                .join()
                .map_err(|_| ControlStateError::WorkerPanicked)?;
            Ok(())
        }
        Ok(Ok(Err(error))) => {
            let _ = reaper.join();
            Err(error)
        }
        Ok(Err(_)) => {
            let _ = reaper.join();
            Err(ControlStateError::WorkerPanicked)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let _ = reaper.join();
            Err(ControlStateError::DurableStateUnavailable)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            STARTUP_PERMANENTLY_POISONED.store(true, Ordering::Release);
            STARTUP_REAPERS
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(reaper);
            Err(ControlStateError::ShutdownDeadlineExceeded)
        }
    }
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
    let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let worker_sender = sender.downgrade();
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

    pub(super) async fn panic_worker_for_test(&self) {
        let (entered, receive) = oneshot::channel();
        self.sender
            .send(ControlCommand::Panic { entered })
            .await
            .expect("panic command must enqueue");
        receive.await.expect("worker must enter panic command");
    }

    pub(super) async fn block_worker_for_test(&self) -> Arc<std::sync::Barrier> {
        let release = Arc::new(std::sync::Barrier::new(2));
        let (entered, receive) = oneshot::channel();
        self.sender
            .send(ControlCommand::Block {
                entered,
                release: Arc::clone(&release),
            })
            .await
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
            .send(ControlCommand::AdmitWithSnapshotFailureAndBlockCleanup {
                request,
                entered,
                release: Arc::clone(&release),
            })
            .await
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
            .send(ControlCommand::Subscribe {
                requested: None,
                generated_at_unix_ms: DecimalU64::new(10),
                max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
                health: self.health_signal.subscribe(),
                reply,
            })
            .await
            .expect("cancelled subscription command must enqueue");
    }

    pub(super) async fn subscriber_count_for_test(&self) -> usize {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(ControlCommand::SubscriberCount { reply })
            .await
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

    pub(super) async fn take_admit_reply(
        &mut self,
    ) -> oneshot::Sender<Result<CommittedAdmission, ControlStateError>> {
        loop {
            match self.receiver.recv().await.expect("synthetic queue command") {
                ControlCommand::Admit { reply, .. } => return reply,
                ControlCommand::Noop => {}
                _ => panic!("unexpected synthetic queue command"),
            }
        }
    }
}

#[cfg(test)]
pub(super) fn synthetic_queue_for_test(state: CommittedState) -> SyntheticQueue {
    let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (health_signal, _health_receiver) = watch::channel(true);
    SyntheticQueue {
        handle: ControlStateHandle {
            sender,
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
    mut receiver: mpsc::Receiver<ControlCommand>,
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
    while let Some(command) = receiver.blocking_recv() {
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
