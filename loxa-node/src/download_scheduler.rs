use crate::artifact_coordinator::{
    valid_portable_artifact_component, ArtifactKey, ArtifactMutationLease,
};
use crate::operation_cancellation::OperationCancellation;
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{mpsc, Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub(crate) const DOWNLOAD_WORKERS: usize = 2;
pub(crate) const DOWNLOAD_WAITING: usize = 8;

const MAX_MODEL_ID_BYTES: usize = 256;
const MAX_SOURCE_FIELD_BYTES: usize = 512;
const MAX_ARTIFACT_SUBPATH_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DownloadKey {
    pub(crate) model_id: String,
    pub(crate) source_namespace: String,
    pub(crate) source_identity: String,
    pub(crate) immutable_revision: Option<String>,
    pub(crate) artifact_subpath: String,
    pub(crate) expected_sha256: Option<[u8; 32]>,
    pub(crate) expected_size: Option<u64>,
    pub(crate) artifact: ArtifactKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadKeyError {
    Empty,
    TooLong,
    AmbiguousSource,
    UnsafeSubpath,
}

impl DownloadKey {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        model_id: &str,
        source_namespace: &str,
        source_identity: &str,
        immutable_revision: Option<&str>,
        artifact_subpath: &str,
        expected_sha256: Option<[u8; 32]>,
        expected_size: Option<u64>,
        artifact: ArtifactKey,
    ) -> Result<Self, DownloadKeyError> {
        validate_model_id(model_id)?;
        validate_source_namespace(source_namespace)?;
        validate_repository(source_identity)?;
        validate_revision(immutable_revision)?;
        validate_artifact_subpath(artifact_subpath)?;
        Ok(Self {
            model_id: model_id.into(),
            source_namespace: source_namespace.into(),
            source_identity: source_identity.into(),
            immutable_revision: immutable_revision.map(str::to_owned),
            artifact_subpath: artifact_subpath.into(),
            expected_sha256,
            expected_size,
            artifact,
        })
    }

    pub(crate) fn artifact(&self) -> &ArtifactKey {
        &self.artifact
    }
}

fn validate_field(value: &str, max: usize) -> Result<(), DownloadKeyError> {
    if value.is_empty() {
        Err(DownloadKeyError::Empty)
    } else if value.len() > max {
        Err(DownloadKeyError::TooLong)
    } else if value.chars().any(char::is_control) {
        Err(DownloadKeyError::AmbiguousSource)
    } else {
        Ok(())
    }
}

fn validate_model_id(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_MODEL_ID_BYTES)?;
    if value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Ok(())
    } else {
        Err(DownloadKeyError::AmbiguousSource)
    }
}

fn validate_source_namespace(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_SOURCE_FIELD_BYTES)?;
    if value == "hugging-face" {
        Ok(())
    } else {
        Err(DownloadKeyError::AmbiguousSource)
    }
}

fn validate_repository(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_SOURCE_FIELD_BYTES)?;
    let mut components = value.split('/');
    let namespace = components.next().unwrap_or_default();
    let repository = components.next().unwrap_or_default();
    if components.next().is_some()
        || !valid_repository_component(namespace)
        || !valid_repository_component(repository)
    {
        Err(DownloadKeyError::AmbiguousSource)
    } else {
        Ok(())
    }
}

fn valid_repository_component(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with(['.', '-'])
        && !value.ends_with(['.', '-'])
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && !value.contains("..")
}

fn validate_revision(value: Option<&str>) -> Result<(), DownloadKeyError> {
    let value = value.ok_or(DownloadKeyError::AmbiguousSource)?;
    validate_field(value, MAX_SOURCE_FIELD_BYTES)?;
    if value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(DownloadKeyError::AmbiguousSource)
    }
}

fn validate_artifact_subpath(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_ARTIFACT_SUBPATH_BYTES)?;
    let path = std::path::Path::new(value);
    if path.is_absolute()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
        || value.contains('\\')
        || value.contains(['?', '#', ':', '%', '&', '='])
        || value.split('/').any(|component| {
            !valid_portable_artifact_component(component)
                || component.bytes().any(|byte| {
                    !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
                })
        })
    {
        Err(DownloadKeyError::UnsafeSubpath)
    } else {
        Ok(())
    }
}

pub(crate) struct DownloadContinuation {
    pub(crate) operation_id: OperationId,
    pub(crate) admission_revision: DecimalU64,
    pub(crate) cancellation: OperationCancellation,
    pub(crate) artifact: ArtifactMutationLease,
    permit: DownloadWorkerPermit,
}

impl DownloadContinuation {
    pub(super) fn into_verification_parts(
        self,
    ) -> (
        crate::verification_scheduler::DownloadVerificationOwnership,
        DownloadWorkerPermit,
    ) {
        let Self {
            operation_id,
            admission_revision,
            cancellation,
            artifact,
            permit,
        } = self;
        (
            crate::verification_scheduler::DownloadVerificationOwnership {
                operation_id,
                admission_revision,
                cancellation,
                artifact,
            },
            permit,
        )
    }

    #[cfg(test)]
    pub(super) fn with_release_probe_for_test(
        operation_id: OperationId,
        admission_revision: DecimalU64,
        cancellation: OperationCancellation,
        artifact: ArtifactMutationLease,
        release_probe: Box<dyn FnOnce() + Send>,
    ) -> Self {
        Self {
            operation_id,
            admission_revision,
            cancellation,
            artifact,
            permit: DownloadWorkerPermit::with_release_probe_for_test(release_probe),
        }
    }
}

pub(crate) trait DownloadExecutor: Send + Sync + 'static {
    fn execute(&self, bound: BoundDownload, permit: DownloadWorkerPermit);
}

pub(crate) struct DownloadWorkerPermit {
    owner: Weak<DownloadShared>,
    worker_index: usize,
    released: bool,
    #[cfg(test)]
    release_probe: Option<Box<dyn FnOnce() + Send>>,
}

impl DownloadWorkerPermit {
    fn new(owner: &Arc<DownloadShared>, worker_index: usize) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            worker_index,
            released: false,
            #[cfg(test)]
            release_probe: None,
        }
    }

    pub(super) fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_worker(self.worker_index);
            }
            #[cfg(test)]
            if let Some(probe) = self.release_probe.take() {
                probe();
            }
            self.released = true;
        }
    }

    #[cfg(test)]
    fn with_release_probe_for_test(probe: Box<dyn FnOnce() + Send>) -> Self {
        Self {
            owner: Weak::new(),
            worker_index: 0,
            released: false,
            release_probe: Some(probe),
        }
    }
}

impl Drop for DownloadWorkerPermit {
    fn drop(&mut self) {
        if !self.released && thread::panicking() {
            if let Some(owner) = self.owner.upgrade() {
                owner.seal();
                self.released = true;
                return;
            }
        }
        self.release_inner();
    }
}

#[allow(clippy::large_enum_variant)] // Frozen inline RAII reservation ownership is intentional.
pub(crate) enum DownloadReserveOutcome {
    Reserved(DownloadAdmissionReservation),
    Active {
        operation_id: OperationId,
        admission_revision: DecimalU64,
    },
    PendingConflict,
    CapacityConflict,
    Stopping,
}

pub(crate) struct DownloadAdmissionReservation {
    key: DownloadKey,
    ticket: u64,
    owner: Weak<DownloadShared>,
    state: ReservationState,
}

pub(crate) struct BoundDownload {
    key: DownloadKey,
    operation_id: OperationId,
    admission_revision: DecimalU64,
    cancellation: OperationCancellation,
}

impl BoundDownload {
    pub(crate) fn key(&self) -> &DownloadKey {
        &self.key
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn admission_revision(&self) -> DecimalU64 {
        self.admission_revision
    }

    pub(crate) fn cancellation(&self) -> OperationCancellation {
        self.cancellation.clone()
    }

    pub(crate) fn into_continuation(
        self,
        artifact: ArtifactMutationLease,
        permit: DownloadWorkerPermit,
    ) -> DownloadContinuation {
        DownloadContinuation {
            operation_id: self.operation_id,
            admission_revision: self.admission_revision,
            cancellation: self.cancellation,
            artifact,
            permit,
        }
    }
}

pub(crate) struct PoisonedDownloadReservation(pub(crate) DownloadAdmissionReservation);

impl DownloadAdmissionReservation {
    pub(crate) fn bind(
        mut self,
        operation_id: OperationId,
        admission_revision: DecimalU64,
        cancellation: OperationCancellation,
    ) -> Result<BoundDownload, DownloadBindError> {
        let Some(owner) = self.owner.upgrade() else {
            self.state = ReservationState::Poisoned;
            return Err(DownloadBindError::Poisoned);
        };
        let mut state = match owner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                if matches!(
                    state.reservations.get(&self.key),
                    Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
                ) {
                    state
                        .reservations
                        .insert(self.key.clone(), ReservationEntry::Poisoned);
                }
                self.state = ReservationState::Poisoned;
                drop(state);
                owner.seal();
                return Err(DownloadBindError::Poisoned);
            }
        };
        let matching_pending = matches!(
            state.reservations.get(&self.key),
            Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
        );
        if !matching_pending || state.sealed {
            state.sealed = true;
            state
                .reservations
                .insert(self.key.clone(), ReservationEntry::Poisoned);
            self.state = ReservationState::Poisoned;
            drop(state);
            owner.seal();
            return Err(DownloadBindError::Poisoned);
        }

        state.reservations.insert(
            self.key.clone(),
            ReservationEntry::Active {
                operation_id,
                admission_revision,
            },
        );
        if !state.transferring.insert(operation_id) {
            state
                .reservations
                .insert(self.key.clone(), ReservationEntry::Poisoned);
            state.sealed = true;
            self.state = ReservationState::Poisoned;
            drop(state);
            owner.seal();
            return Err(DownloadBindError::Poisoned);
        }
        self.state = ReservationState::Bound;
        let stopping = state.stopping;
        drop(state);
        owner.notify_changed();
        if stopping {
            return Err(DownloadBindError::Stopping);
        }
        Ok(BoundDownload {
            key: self.key.clone(),
            operation_id,
            admission_revision,
            cancellation,
        })
    }

    pub(crate) fn poison(mut self) -> PoisonedDownloadReservation {
        let Some(owner) = self.owner.upgrade() else {
            self.state = ReservationState::Poisoned;
            return PoisonedDownloadReservation(self);
        };
        let mut state = match owner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.sealed = true;
        if matches!(
            state.reservations.get(&self.key),
            Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
        ) {
            state
                .reservations
                .insert(self.key.clone(), ReservationEntry::Poisoned);
        }
        self.state = ReservationState::Poisoned;
        drop(state);
        owner.seal();
        PoisonedDownloadReservation(self)
    }
}

impl Drop for DownloadAdmissionReservation {
    fn drop(&mut self) {
        if self.state != ReservationState::PendingCommit {
            return;
        }
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut state = match owner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                self.state = ReservationState::Poisoned;
                drop(state);
                owner.seal();
                return;
            }
        };
        let removed = if matches!(
            state.reservations.get(&self.key),
            Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
        ) {
            state.reservations.remove_entry(&self.key)
        } else {
            None
        };
        drop(state);
        let changed = removed.is_some();
        drop(removed);
        if changed {
            owner.notify_changed();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationState {
    PendingCommit,
    Bound,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerExit {
    Stopped,
    Panicked,
}

struct DownloadShared {
    state: Mutex<DownloadState>,
    changed: Condvar,
    #[cfg(test)]
    notification_probe: Option<DownloadNotificationProbe>,
}

#[cfg(test)]
type DownloadNotificationProbe = Arc<dyn Fn(&DownloadShared) + Send + Sync>;

impl DownloadShared {
    fn new() -> Self {
        Self {
            state: Mutex::new(DownloadState {
                reservations: HashMap::new(),
                ready: BTreeMap::new(),
                executing: HashMap::new(),
                transferring: HashSet::new(),
                workers: [None; DOWNLOAD_WORKERS],
                next_ticket: 1,
                stopping: false,
                sealed: false,
            }),
            changed: Condvar::new(),
            #[cfg(test)]
            notification_probe: None,
        }
    }

    fn notify_changed(&self) {
        #[cfg(test)]
        if let Some(probe) = &self.notification_probe {
            probe(self);
        }
        self.changed.notify_all();
    }

    fn release_worker(&self, worker_index: usize) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.seal();
                return;
            }
        };
        if state.sealed {
            drop(state);
            self.seal();
            return;
        }
        let Some(operation_id) = state.workers.get(worker_index).copied().flatten() else {
            state.sealed = true;
            drop(state);
            self.seal();
            return;
        };
        if !state.executing.contains_key(&operation_id)
            || !state.transferring.contains(&operation_id)
        {
            state.sealed = true;
            drop(state);
            self.seal();
            return;
        }

        state.workers[worker_index] = None;
        let cancellation = state.executing.remove(&operation_id);
        let transfer_removed = state.transferring.remove(&operation_id);
        let valid = cancellation.is_some() && transfer_removed;
        if !valid {
            state.sealed = true;
        }
        drop(state);
        drop(cancellation);
        if valid {
            self.notify_changed();
        } else {
            self.seal();
        }
    }

    fn stop(&self) {
        self.close_admission(false);
    }

    fn seal(&self) {
        self.close_admission(true);
    }

    fn close_admission(&self, seal: bool) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if seal {
            state.sealed = true;
        } else {
            state.stopping = true;
        }
        let cancellations = state
            .ready
            .values()
            .map(|bound| bound.cancellation.clone())
            .chain(state.executing.values().cloned())
            .collect::<Vec<_>>();
        drop(state);
        for cancellation in cancellations {
            let _ = cancellation.request_cancel();
        }
        self.notify_changed();
    }
}

struct DownloadState {
    reservations: HashMap<DownloadKey, ReservationEntry>,
    ready: BTreeMap<(DecimalU64, OperationId), BoundDownload>,
    executing: HashMap<OperationId, OperationCancellation>,
    transferring: HashSet<OperationId>,
    workers: [Option<OperationId>; DOWNLOAD_WORKERS],
    next_ticket: u64,
    stopping: bool,
    sealed: bool,
}

impl DownloadState {
    fn transfer_population(&self) -> usize {
        self.reservations
            .values()
            .filter(|entry| matches!(entry, ReservationEntry::Pending { .. }))
            .count()
            + self.transferring.len()
    }
}

enum ReservationEntry {
    Pending {
        ticket: u64,
    },
    Active {
        operation_id: OperationId,
        admission_revision: DecimalU64,
    },
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadBindError {
    Stopping,
    Poisoned,
}

pub(crate) struct DownloadSchedulerHandle {
    shared: Arc<DownloadShared>,
}

impl Clone for DownloadSchedulerHandle {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

pub(crate) struct DownloadSchedulerOwner {
    handles: Vec<JoinHandle<()>>,
    completions: Vec<std::sync::mpsc::Receiver<WorkerExit>>,
    shared: Arc<DownloadShared>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadSubmitOutcome {
    Submitted,
    Cancelled,
    Stopping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadKeyReleaseOutcome {
    Released,
    TimedOut,
    Stopping,
    Poisoned,
}

pub(crate) struct DownloadFatalShutdown {
    shared: Arc<DownloadShared>,
}

impl DownloadFatalShutdown {
    pub(crate) fn is_sealed(&self) -> bool {
        match self.shared.state.lock() {
            Ok(state) => state.sealed,
            Err(_) => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadShutdownReason {
    DeadlineExceeded,
    CompletionDisconnected,
    WorkerPanicked,
    WorkerJoinPanicked,
}

pub(crate) struct DownloadShutdownFailure {
    reason: DownloadShutdownReason,
    owner: Option<DownloadSchedulerOwner>,
}

impl fmt::Debug for DownloadShutdownFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DownloadShutdownFailure")
            .field("reason", &self.reason)
            .field("retains_unjoined_workers", &self.retains_unjoined_workers())
            .finish()
    }
}

impl DownloadShutdownFailure {
    pub(crate) fn reason(&self) -> DownloadShutdownReason {
        self.reason
    }

    pub(crate) fn retains_unjoined_workers(&self) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| !owner.handles.is_empty())
    }

    pub(crate) fn into_owner(mut self) -> Option<DownloadSchedulerOwner> {
        self.owner.take()
    }
}

impl DownloadSchedulerHandle {
    #[cfg(test)]
    pub(crate) fn replace_active_revision_for_test(
        &self,
        key: &DownloadKey,
        revision: DecimalU64,
    ) -> bool {
        let Ok(mut state) = self.shared.state.lock() else {
            self.shared.seal();
            return false;
        };
        let Some(ReservationEntry::Active {
            admission_revision, ..
        }) = state.reservations.get_mut(key)
        else {
            return false;
        };
        *admission_revision = revision;
        true
    }

    pub(crate) fn reserve(&self, key: DownloadKey) -> DownloadReserveOutcome {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return DownloadReserveOutcome::Stopping;
            }
        };
        if state.stopping || state.sealed {
            return DownloadReserveOutcome::Stopping;
        }
        match state.reservations.get(&key) {
            Some(ReservationEntry::Pending { .. }) => {
                return DownloadReserveOutcome::PendingConflict;
            }
            Some(ReservationEntry::Active {
                operation_id,
                admission_revision,
            }) => {
                return DownloadReserveOutcome::Active {
                    operation_id: *operation_id,
                    admission_revision: *admission_revision,
                };
            }
            Some(ReservationEntry::Poisoned) => {
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return DownloadReserveOutcome::Stopping;
            }
            None => {}
        }
        if state.transfer_population() >= DOWNLOAD_WORKERS + DOWNLOAD_WAITING {
            return DownloadReserveOutcome::CapacityConflict;
        }
        let ticket = state.next_ticket;
        let Some(next_ticket) = ticket.checked_add(1) else {
            state.sealed = true;
            drop(state);
            self.shared.seal();
            return DownloadReserveOutcome::Stopping;
        };
        state.next_ticket = next_ticket;
        state
            .reservations
            .insert(key.clone(), ReservationEntry::Pending { ticket });
        drop(state);
        DownloadReserveOutcome::Reserved(DownloadAdmissionReservation {
            key,
            ticket,
            owner: Arc::downgrade(&self.shared),
            state: ReservationState::PendingCommit,
        })
    }

    pub(crate) fn wait_key_released_until(
        &self,
        key: &DownloadKey,
        deadline: Instant,
    ) -> DownloadKeyReleaseOutcome {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return DownloadKeyReleaseOutcome::Poisoned;
            }
        };
        loop {
            if !matches!(
                state.reservations.get(key),
                Some(ReservationEntry::Active { .. })
            ) {
                return DownloadKeyReleaseOutcome::Released;
            }
            if state.stopping || state.sealed {
                return DownloadKeyReleaseOutcome::Stopping;
            }
            let now = Instant::now();
            if now >= deadline {
                return DownloadKeyReleaseOutcome::TimedOut;
            }
            let (next, timeout) = match self
                .shared
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
            {
                Ok(next) => next,
                Err(poisoned) => {
                    let (mut state, _) = poisoned.into_inner();
                    state.sealed = true;
                    drop(state);
                    self.shared.seal();
                    return DownloadKeyReleaseOutcome::Poisoned;
                }
            };
            state = next;
            if timeout.timed_out()
                && matches!(
                    state.reservations.get(key),
                    Some(ReservationEntry::Active { .. })
                )
            {
                return DownloadKeyReleaseOutcome::TimedOut;
            }
        }
    }

    pub(crate) fn submit(&self, bound: BoundDownload) -> DownloadSubmitOutcome {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return DownloadSubmitOutcome::Stopping;
            }
        };
        if state.stopping || state.sealed {
            drop(state);
            return DownloadSubmitOutcome::Stopping;
        }
        let matches_active = matches!(
            state.reservations.get(&bound.key),
            Some(ReservationEntry::Active {
                operation_id,
                admission_revision,
            }) if *operation_id == bound.operation_id
                && *admission_revision == bound.admission_revision
        );
        if !matches_active
            || !state.transferring.contains(&bound.operation_id)
            || state
                .ready
                .contains_key(&(bound.admission_revision, bound.operation_id))
            || state.executing.contains_key(&bound.operation_id)
        {
            state.sealed = true;
            drop(state);
            self.shared.seal();
            return DownloadSubmitOutcome::Stopping;
        }
        if bound.cancellation.is_cancel_requested() {
            drop(state);
            return DownloadSubmitOutcome::Cancelled;
        }
        state
            .ready
            .insert((bound.admission_revision, bound.operation_id), bound);
        drop(state);
        self.shared.notify_changed();
        DownloadSubmitOutcome::Submitted
    }

    pub(crate) fn request_cancel(&self, operation_id: OperationId) -> bool {
        let cancellation = match self.shared.state.lock() {
            Ok(state) => state.executing.get(&operation_id).cloned().or_else(|| {
                state
                    .ready
                    .values()
                    .find(|bound| bound.operation_id == operation_id)
                    .map(|bound| bound.cancellation.clone())
            }),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return false;
            }
        };
        cancellation.is_some_and(|cancellation| cancellation.request_cancel())
    }

    pub(crate) fn cancel_queued_committed(&self, operation_id: OperationId) -> bool {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return false;
            }
        };
        let ready_key = state
            .ready
            .keys()
            .find(|(_, candidate)| *candidate == operation_id)
            .copied();
        let Some(ready_key) = ready_key else {
            return false;
        };
        let reservation_key = state.reservations.iter().find_map(|(key, entry)| {
            matches!(
                entry,
                ReservationEntry::Active {
                    operation_id: candidate,
                    ..
                } if *candidate == operation_id
            )
            .then(|| key.clone())
        });
        if reservation_key.is_none() || !state.transferring.contains(&operation_id) {
            state.sealed = true;
            drop(state);
            self.shared.seal();
            return false;
        }
        let bound = state.ready.remove(&ready_key);
        drop(state);
        let Some(bound) = bound else {
            self.shared.seal();
            return false;
        };
        let _ = bound.cancellation.request_cancel();
        drop(bound);
        self.shared.notify_changed();
        true
    }

    pub(crate) fn finish_committed(&self, operation_id: OperationId) -> bool {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                self.shared.seal();
                return false;
            }
        };
        if state.executing.contains_key(&operation_id)
            || state
                .ready
                .values()
                .any(|bound| bound.operation_id == operation_id)
            || state.workers.contains(&Some(operation_id))
        {
            return false;
        }
        let reservation_key = state.reservations.iter().find_map(|(key, entry)| {
            matches!(
                entry,
                ReservationEntry::Active {
                    operation_id: candidate,
                    ..
                } if *candidate == operation_id
            )
            .then(|| key.clone())
        });
        let Some(reservation_key) = reservation_key else {
            let inconsistent = state.transferring.contains(&operation_id);
            if inconsistent {
                state.sealed = true;
            }
            drop(state);
            if inconsistent {
                self.shared.seal();
            }
            return false;
        };
        let removed = state.reservations.remove_entry(&reservation_key);
        let transfer_removed = state.transferring.remove(&operation_id);
        drop(state);
        let changed = removed.is_some() || transfer_removed;
        drop(removed);
        if changed {
            self.shared.notify_changed();
        }
        changed
    }

    pub(crate) fn stop(&self) {
        self.shared.stop();
    }

    pub(crate) fn seal_and_retain(&self) -> DownloadFatalShutdown {
        self.shared.seal();
        DownloadFatalShutdown {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl DownloadSchedulerOwner {
    pub(crate) fn request_shutdown(&self) {
        self.shared.stop();
    }

    pub(crate) fn spawn(
        executor: Arc<dyn DownloadExecutor>,
    ) -> io::Result<(DownloadSchedulerHandle, Self)> {
        let shared = Arc::new(DownloadShared::new());
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(DOWNLOAD_WORKERS);
        let mut completions = Vec::with_capacity(DOWNLOAD_WORKERS);
        for worker_index in 0..DOWNLOAD_WORKERS {
            let worker_shared = Arc::clone(&shared);
            let worker_executor = Arc::clone(&executor);
            let (completion, receiver) = mpsc::channel();
            let handle = match thread::Builder::new()
                .name(format!("loxa-download-{worker_index}"))
                .spawn(move || {
                    let exit = match panic::catch_unwind(AssertUnwindSafe(|| {
                        download_worker_loop(
                            Arc::clone(&worker_shared),
                            worker_index,
                            worker_executor,
                        );
                    })) {
                        Ok(()) => WorkerExit::Stopped,
                        Err(_) => {
                            worker_shared.seal();
                            WorkerExit::Panicked
                        }
                    };
                    let _ = completion.send(exit);
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    shared.stop();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            };
            handles.push(handle);
            completions.push(receiver);
        }
        Ok((
            DownloadSchedulerHandle {
                shared: Arc::clone(&shared),
            },
            Self {
                handles,
                completions,
                shared,
            },
        ))
    }

    pub(crate) fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<Vec<WorkerExit>, DownloadShutdownFailure> {
        self.shared.stop();
        let mut exits = Vec::with_capacity(self.completions.len());
        let mut failure = None;
        for receiver in &self.completions {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match receiver.recv_timeout(remaining) {
                Ok(exit) => {
                    if exit == WorkerExit::Panicked && failure.is_none() {
                        failure = Some(DownloadShutdownReason::WorkerPanicked);
                    }
                    exits.push(exit);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if failure.is_none() {
                        failure = Some(DownloadShutdownReason::CompletionDisconnected);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(DownloadShutdownFailure {
                        reason: DownloadShutdownReason::DeadlineExceeded,
                        owner: Some(self),
                    });
                }
            }
        }
        for handle in self.handles.drain(..) {
            if handle.join().is_err() && failure.is_none() {
                failure = Some(DownloadShutdownReason::WorkerJoinPanicked);
            }
        }
        if let Some(reason) = failure {
            Err(DownloadShutdownFailure {
                reason,
                owner: None,
            })
        } else {
            Ok(exits)
        }
    }
}

impl Drop for DownloadSchedulerOwner {
    fn drop(&mut self) {
        if self.handles.is_empty() {
            return;
        }
        self.shared.stop();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn download_worker_loop(
    shared: Arc<DownloadShared>,
    worker_index: usize,
    executor: Arc<dyn DownloadExecutor>,
) {
    loop {
        let bound = {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    state.sealed = true;
                    drop(state);
                    shared.seal();
                    panic!("download scheduler state poisoned");
                }
            };
            while state.ready.is_empty() && !state.stopping && !state.sealed {
                state = match shared.changed.wait(state) {
                    Ok(state) => state,
                    Err(poisoned) => {
                        let mut state = poisoned.into_inner();
                        state.sealed = true;
                        drop(state);
                        shared.seal();
                        panic!("download scheduler state poisoned");
                    }
                };
            }
            if state.stopping || state.sealed {
                return;
            }
            let (ready_key, bound) = state.ready.pop_first().expect("ready work exists");
            let invalid = state.executing.contains_key(&bound.operation_id)
                || !state.transferring.contains(&bound.operation_id)
                || state.workers.get(worker_index).is_none_or(Option::is_some);
            if invalid {
                state.ready.insert(ready_key, bound);
                state.sealed = true;
                drop(state);
                shared.seal();
                panic!("invalid download worker ownership");
            }
            state.workers[worker_index] = Some(bound.operation_id);
            state
                .executing
                .insert(bound.operation_id, bound.cancellation.clone());
            bound
        };
        let operation_id = bound.operation_id;
        executor.execute(bound, DownloadWorkerPermit::new(&shared, worker_index));

        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                drop(state);
                shared.seal();
                panic!("download scheduler state poisoned");
            }
        };
        if state.sealed {
            return;
        }
        let cleaned = state.workers.get(worker_index).is_some_and(Option::is_none)
            && state
                .workers
                .iter()
                .all(|current| *current != Some(operation_id))
            && !state.executing.contains_key(&operation_id)
            && !state.transferring.contains(&operation_id);
        if !cleaned {
            state.sealed = true;
        }
        drop(state);
        if !cleaned {
            shared.seal();
            panic!("download permit did not release worker ownership");
        }
    }
}

#[cfg(test)]
fn reservation_with_notification_probe_for_test(
    probe: impl Fn(&DownloadShared) + Send + Sync + 'static,
) -> (Arc<DownloadShared>, DownloadAdmissionReservation) {
    let parent = std::env::temp_dir().join(format!(
        "loxa-download-reservation-probe-{}-{}",
        std::process::id(),
        OperationId::new_v4()
    ));
    std::fs::create_dir(&parent).unwrap();
    let artifact = ArtifactKey::from_destination(&parent.join("model.gguf")).unwrap();
    let _ = std::fs::remove_dir(&parent);
    let key = DownloadKey::new(
        "coding",
        "hugging-face",
        "publisher/repository",
        Some("0123456789abcdef0123456789abcdef01234567"),
        "weights/model.gguf",
        Some([7; 32]),
        Some(42),
        artifact,
    )
    .unwrap();
    let owner = Arc::new(DownloadShared {
        state: Mutex::new(DownloadState {
            reservations: HashMap::from([(key.clone(), ReservationEntry::Pending { ticket: 1 })]),
            ready: BTreeMap::new(),
            executing: HashMap::new(),
            transferring: HashSet::new(),
            workers: [None; DOWNLOAD_WORKERS],
            next_ticket: 2,
            stopping: false,
            sealed: false,
        }),
        changed: Condvar::new(),
        notification_probe: Some(Arc::new(probe)),
    });
    let reservation = DownloadAdmissionReservation {
        key,
        ticket: 1,
        owner: Arc::downgrade(&owner),
        state: ReservationState::PendingCommit,
    };
    (owner, reservation)
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn portable_subpath_rejects_windows_device_basenames() {
        for path in [
            "CON",
            "con.gguf",
            "weights/CON .gguf",
            "weights/PRN.bin",
            "weights/aux",
            "weights/NUL.gguf",
            "weights/com1.gguf",
            "weights/COM1 .bin",
            "weights/COM9",
            "weights/lpt1.bin",
            "weights/LPT9",
            "weights/model.gguf.",
            "weights/model.gguf ",
        ] {
            assert!(
                validate_artifact_subpath(path).is_err(),
                "accepted {path:?}"
            );
        }
    }

    #[test]
    fn poisoned_reservation_drop_notifies_only_after_releasing_state_lock() {
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&observed_unlocked);
        let (owner, reservation) = reservation_with_notification_probe_for_test(move |shared| {
            let available = !matches!(
                shared.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            observed.store(available, Ordering::SeqCst);
        });

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = owner.state.lock().unwrap();
            panic!("poison download state");
        }));
        drop(reservation);

        assert!(observed_unlocked.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    type ExecutionGate = Arc<(Mutex<bool>, Condvar)>;

    struct GateExecutor {
        started: mpsc::Sender<(OperationId, OperationCancellation)>,
        gates: Mutex<HashMap<OperationId, ExecutionGate>>,
        release_all: AtomicBool,
    }

    impl GateExecutor {
        fn new(started: mpsc::Sender<(OperationId, OperationCancellation)>) -> Self {
            Self {
                started,
                gates: Mutex::new(HashMap::new()),
                release_all: AtomicBool::new(false),
            }
        }

        fn release(&self, operation_id: OperationId) {
            let gate = self
                .gates
                .lock()
                .unwrap()
                .get(&operation_id)
                .cloned()
                .expect("operation gate exists after start");
            let (released, changed) = &*gate;
            *released.lock().unwrap() = true;
            changed.notify_all();
        }

        fn release_all(&self) {
            self.release_all.store(true, Ordering::SeqCst);
            let gates = self
                .gates
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for gate in gates {
                let (released, changed) = &*gate;
                *released.lock().unwrap() = true;
                changed.notify_all();
            }
        }

        fn reset_release_all(&self) {
            self.release_all.store(false, Ordering::SeqCst);
        }
    }

    impl DownloadExecutor for GateExecutor {
        fn execute(&self, bound: BoundDownload, permit: DownloadWorkerPermit) {
            let operation_id = bound.operation_id();
            let cancellation = bound.cancellation();
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            self.gates
                .lock()
                .unwrap()
                .insert(operation_id, Arc::clone(&gate));
            self.started.send((operation_id, cancellation)).unwrap();

            let (released, changed) = &*gate;
            let mut released = released.lock().unwrap();
            while !*released && !self.release_all.load(Ordering::SeqCst) {
                released = changed.wait(released).unwrap();
            }
            drop(released);
            drop(permit);
        }
    }

    struct PanicExecutor(mpsc::Sender<()>);

    impl DownloadExecutor for PanicExecutor {
        fn execute(&self, _: BoundDownload, _: DownloadWorkerPermit) {
            self.0.send(()).unwrap();
            panic!("injected download executor panic");
        }
    }

    struct HandoffExecutor {
        started: mpsc::Sender<OperationId>,
        accept: Mutex<mpsc::Receiver<()>>,
        permit_released: mpsc::Sender<()>,
        return_worker: Mutex<mpsc::Receiver<()>>,
        returned: mpsc::Sender<()>,
    }

    impl DownloadExecutor for HandoffExecutor {
        fn execute(&self, bound: BoundDownload, permit: DownloadWorkerPermit) {
            self.started.send(bound.operation_id()).unwrap();
            self.accept.lock().unwrap().recv().unwrap();
            drop(permit);
            self.permit_released.send(()).unwrap();
            self.return_worker.lock().unwrap().recv().unwrap();
            self.returned.send(()).unwrap();
        }
    }

    struct RetainPermitExecutor {
        started: mpsc::Sender<()>,
        permit: Mutex<Option<DownloadWorkerPermit>>,
    }

    impl DownloadExecutor for RetainPermitExecutor {
        fn execute(&self, _: BoundDownload, permit: DownloadWorkerPermit) {
            *self.permit.lock().unwrap() = Some(permit);
            self.started.send(()).unwrap();
        }
    }

    fn operation(sequence: u64) -> OperationId {
        OperationId::from_str(&format!("5aaaaaaa-0000-4000-8000-{sequence:012x}")).unwrap()
    }

    fn key(sequence: u64) -> DownloadKey {
        let parent = std::env::temp_dir().join(format!(
            "loxa-download-scheduler-{}-{}-{sequence}",
            std::process::id(),
            OperationId::new_v4()
        ));
        std::fs::create_dir(&parent).unwrap();
        let artifact = ArtifactKey::from_destination(&parent.join("model.gguf")).unwrap();
        std::fs::remove_dir(&parent).unwrap();
        DownloadKey::new(
            &format!("model-{sequence}"),
            "hugging-face",
            &format!("publisher/repository-{sequence}"),
            Some("0123456789abcdef0123456789abcdef01234567"),
            &format!("weights/model-{sequence}.gguf"),
            Some([sequence as u8; 32]),
            Some(sequence + 1),
            artifact,
        )
        .unwrap()
    }

    fn reserve(handle: &DownloadSchedulerHandle, key: DownloadKey) -> DownloadAdmissionReservation {
        match handle.reserve(key) {
            DownloadReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("expected a fresh reservation"),
        }
    }

    fn bind_submit(
        handle: &DownloadSchedulerHandle,
        reservation: DownloadAdmissionReservation,
        operation_id: OperationId,
        revision: u64,
    ) -> OperationCancellation {
        let cancellation = OperationCancellation::new();
        let bound = reservation
            .bind(
                operation_id,
                DecimalU64::new(revision),
                cancellation.clone(),
            )
            .unwrap();
        assert_eq!(handle.submit(bound), DownloadSubmitOutcome::Submitted);
        cancellation
    }

    fn shutdown(owner: DownloadSchedulerOwner) {
        match owner.shutdown(Instant::now() + Duration::from_secs(2)) {
            Ok(exits) => assert!(exits.iter().all(|exit| *exit == WorkerExit::Stopped)),
            Err(failure) => panic!("scheduler shutdown failed: {:?}", failure.reason()),
        }
    }

    fn wait_until_not_executing(handle: &DownloadSchedulerHandle, operation_id: OperationId) {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = handle.shared.state.lock().unwrap();
        while state.executing.contains_key(&operation_id) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "operation remained executing");
            state = handle
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
    }

    fn wait_until_transfers_idle(handle: &DownloadSchedulerHandle) {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = handle.shared.state.lock().unwrap();
        while !state.ready.is_empty() || !state.executing.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "download transfers did not become idle"
            );
            state = handle
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
    }

    fn bind_without_submit(
        handle: &DownloadSchedulerHandle,
        key: DownloadKey,
        operation_id: OperationId,
    ) {
        reserve(handle, key)
            .bind(
                operation_id,
                DecimalU64::new(1),
                OperationCancellation::new(),
            )
            .unwrap();
    }

    #[test]
    fn exact_key_release_wait_wakes_without_mutating_capacity() {
        let (started_tx, _) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let key = key(901);
        let operation_id = operation(901);
        bind_without_submit(&handle, key.clone(), operation_id);
        let waiter = handle.clone();
        let waited_key = key.clone();
        let waiting = thread::spawn(move || {
            waiter.wait_key_released_until(&waited_key, Instant::now() + Duration::from_secs(1))
        });

        assert!(handle.finish_committed(operation_id));
        assert_eq!(waiting.join().unwrap(), DownloadKeyReleaseOutcome::Released);
        assert_eq!(handle.shared.state.lock().unwrap().transfer_population(), 0);
        shutdown(owner);
    }

    #[test]
    fn exact_key_release_wait_times_out_without_capacity_side_effects() {
        let (started_tx, _) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let key = key(902);
        let operation_id = operation(902);
        bind_without_submit(&handle, key.clone(), operation_id);
        let before = handle.shared.state.lock().unwrap().transfer_population();

        assert_eq!(
            handle.wait_key_released_until(&key, Instant::now() + Duration::from_millis(1)),
            DownloadKeyReleaseOutcome::TimedOut
        );
        assert_eq!(
            handle.shared.state.lock().unwrap().transfer_population(),
            before
        );
        assert!(handle.finish_committed(operation_id));
        shutdown(owner);
    }

    #[test]
    fn exact_key_release_wait_fails_closed_when_scheduler_stops() {
        let (started_tx, _) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let key = key(903);
        bind_without_submit(&handle, key.clone(), operation(903));
        handle.stop();

        assert_eq!(
            handle.wait_key_released_until(&key, Instant::now() + Duration::from_secs(1)),
            DownloadKeyReleaseOutcome::Stopping
        );
        drop(owner);
    }

    #[test]
    fn exact_key_release_wait_reports_poison_and_seals() {
        let (started_tx, _) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let shared = Arc::clone(&handle.shared);
        let _ = thread::spawn(move || {
            let _guard = shared.state.lock().unwrap();
            panic!("inject scheduler state poison");
        })
        .join();

        assert_eq!(
            handle.wait_key_released_until(&key(904), Instant::now() + Duration::from_secs(1)),
            DownloadKeyReleaseOutcome::Poisoned
        );
        assert!(handle.seal_and_retain().is_sealed());
        drop(owner);
    }

    #[test]
    fn capacity_is_exactly_two_executing_plus_eight_waiting_unique_keys() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();

        let mut cancellations = Vec::new();
        for sequence in 1..=(DOWNLOAD_WORKERS + DOWNLOAD_WAITING) as u64 {
            let reservation = reserve(&handle, key(sequence));
            cancellations.push(bind_submit(
                &handle,
                reservation,
                operation(sequence),
                sequence,
            ));
        }

        let first = started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0;
        let second = started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0;
        assert_ne!(first, second);
        assert!(started_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(matches!(
            handle.reserve(key(11)),
            DownloadReserveOutcome::CapacityConflict
        ));

        handle.stop();
        assert!(cancellations
            .iter()
            .all(OperationCancellation::is_cancel_requested));
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn verification_owned_active_entries_do_not_consume_transfer_capacity() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();
        let first_key = key(200);
        let first_operation = operation(200);

        for sequence in 200..210 {
            let download_key = if sequence == 200 {
                first_key.clone()
            } else {
                key(sequence)
            };
            bind_submit(
                &handle,
                reserve(&handle, download_key),
                operation(sequence),
                sequence,
            );
        }
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        executor.release_all();
        wait_until_transfers_idle(&handle);
        executor.reset_release_all();

        assert!(matches!(
            handle.reserve(first_key),
            DownloadReserveOutcome::Active {
                operation_id,
                admission_revision,
            } if operation_id == first_operation && admission_revision == DecimalU64::new(200)
        ));
        for sequence in 300..310 {
            bind_submit(
                &handle,
                reserve(&handle, key(sequence)),
                operation(sequence),
                sequence,
            );
        }
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            handle.reserve(key(311)),
            DownloadReserveOutcome::CapacityConflict
        ));

        handle.stop();
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn definite_commit_rejection_drops_pending_reservation_and_releases_capacity() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let mut reservations = (1..=(DOWNLOAD_WORKERS + DOWNLOAD_WAITING) as u64)
            .map(|sequence| reserve(&handle, key(100 + sequence)))
            .collect::<Vec<_>>();
        assert!(matches!(
            handle.reserve(key(111)),
            DownloadReserveOutcome::CapacityConflict
        ));

        drop(reservations.pop());
        assert!(matches!(
            handle.reserve(key(111)),
            DownloadReserveOutcome::Reserved(_)
        ));
        drop(reservations);

        handle.stop();
        shutdown(owner);
    }

    #[test]
    fn reservation_precedes_commit_and_pending_or_active_duplicates_do_not_displace() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();
        let duplicate_key = key(20);
        let reservation = reserve(&handle, duplicate_key.clone());

        assert!(matches!(
            handle.reserve(duplicate_key.clone()),
            DownloadReserveOutcome::PendingConflict
        ));

        let operation_id = operation(20);
        let cancellation = OperationCancellation::new();
        let bound = reservation
            .bind(operation_id, DecimalU64::new(200), cancellation)
            .unwrap();
        match handle.reserve(duplicate_key) {
            DownloadReserveOutcome::Active {
                operation_id: active,
                admission_revision,
            } => {
                assert_eq!(active, operation_id);
                assert_eq!(admission_revision, DecimalU64::new(200));
            }
            _ => panic!("bound duplicate must return the active operation"),
        }
        assert_eq!(handle.submit(bound), DownloadSubmitOutcome::Submitted);

        handle.stop();
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn fifo() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();

        let first = operation(31);
        let second = operation(32);
        bind_submit(&handle, reserve(&handle, key(31)), first, 1);
        bind_submit(&handle, reserve(&handle, key(32)), second, 2);
        let running = [
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
        ];
        assert!(running.contains(&first));
        assert!(running.contains(&second));

        let unresolved = reserve(&handle, key(33));
        let revision_30 = operation(34);
        let revision_10 = operation(35);
        let revision_20 = operation(36);
        let revision_40_low = operation(37);
        let revision_40_high = operation(38);
        bind_submit(&handle, reserve(&handle, key(34)), revision_30, 30);
        bind_submit(&handle, reserve(&handle, key(35)), revision_10, 10);
        bind_submit(&handle, reserve(&handle, key(36)), revision_20, 20);
        bind_submit(&handle, reserve(&handle, key(38)), revision_40_high, 40);
        bind_submit(&handle, reserve(&handle, key(37)), revision_40_low, 40);

        executor.release(first);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            revision_10
        );
        executor.release(revision_10);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            revision_20
        );
        executor.release(revision_20);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            revision_30
        );
        executor.release(revision_30);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            revision_40_low
        );
        executor.release(revision_40_low);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap().0,
            revision_40_high
        );

        drop(unresolved);
        handle.stop();
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn queued_and_running_cancellation_are_bounded_and_key_scoped() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();

        let first = operation(41);
        let second = operation(42);
        let queued = operation(43);
        bind_submit(&handle, reserve(&handle, key(41)), first, 1);
        bind_submit(&handle, reserve(&handle, key(42)), second, 2);
        let first_started = started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let second_started = started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        bind_submit(&handle, reserve(&handle, key(43)), queued, 3);

        assert!(handle.cancel_queued_committed(queued));
        assert!(handle.request_cancel(first));
        let running_cancellation = if first_started.0 == first {
            first_started.1
        } else {
            second_started.1
        };
        assert!(running_cancellation.is_cancel_requested());
        assert!(matches!(
            handle.reserve(key(43)),
            DownloadReserveOutcome::Reserved(_)
        ));
        assert!(started_rx.recv_timeout(Duration::from_millis(50)).is_err());

        handle.stop();
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn terminal_commit_releases_active_deduplication_only_after_worker_handoff() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();
        let download_key = key(45);
        let operation_id = operation(45);
        bind_submit(
            &handle,
            reserve(&handle, download_key.clone()),
            operation_id,
            45,
        );
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert!(matches!(
            handle.reserve(download_key.clone()),
            DownloadReserveOutcome::Active {
                operation_id: active,
                admission_revision,
            } if active == operation_id && admission_revision == DecimalU64::new(45)
        ));
        assert!(!handle.finish_committed(operation_id));

        executor.release(operation_id);
        wait_until_not_executing(&handle, operation_id);
        assert!(handle.finish_committed(operation_id));
        assert!(matches!(
            handle.reserve(download_key),
            DownloadReserveOutcome::Reserved(_)
        ));

        handle.stop();
        executor.release_all();
        shutdown(owner);
    }

    #[test]
    fn permit_release_linearizes_immediate_terminal_finish_before_worker_return() {
        let (started_tx, started_rx) = mpsc::channel();
        let (accept_tx, accept_rx) = mpsc::channel();
        let (permit_released_tx, permit_released_rx) = mpsc::channel();
        let (return_worker_tx, return_worker_rx) = mpsc::channel();
        let (returned_tx, returned_rx) = mpsc::channel();
        let executor = Arc::new(HandoffExecutor {
            started: started_tx,
            accept: Mutex::new(accept_rx),
            permit_released: permit_released_tx,
            return_worker: Mutex::new(return_worker_rx),
            returned: returned_tx,
        });
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let operation_id = operation(46);
        bind_submit(&handle, reserve(&handle, key(46)), operation_id, 46);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            operation_id
        );

        accept_tx.send(()).unwrap();
        permit_released_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let first_finish = handle.finish_committed(operation_id);
        let second_finish = handle.finish_committed(operation_id);
        return_worker_tx.send(()).unwrap();
        returned_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        handle.stop();
        shutdown(owner);

        assert!(
            first_finish,
            "permit release must make terminal finish immediate"
        );
        assert!(!second_finish, "terminal ownership releases exactly once");
    }

    #[test]
    fn mismatched_permit_release_seals_and_retains_transfer_ownership() {
        let shared = Arc::new(DownloadShared::new());
        let operation_id = operation(47);
        let download_key = key(47);
        let cancellation = OperationCancellation::new();
        {
            let mut state = shared.state.lock().unwrap();
            state.reservations.insert(
                download_key.clone(),
                ReservationEntry::Active {
                    operation_id,
                    admission_revision: DecimalU64::new(47),
                },
            );
            state.transferring.insert(operation_id);
            state.executing.insert(operation_id, cancellation);
            state.workers[0] = Some(operation_id);
        }

        DownloadWorkerPermit::new(&shared, 1).release();

        let state = shared.state.lock().unwrap();
        assert!(state.sealed);
        assert_eq!(state.workers[0], Some(operation_id));
        assert!(state.executing.contains_key(&operation_id));
        assert!(state.transferring.contains(&operation_id));
        assert!(matches!(
            state.reservations.get(&download_key),
            Some(ReservationEntry::Active {
                operation_id: active,
                admission_revision,
            }) if *active == operation_id && *admission_revision == DecimalU64::new(47)
        ));
    }

    #[test]
    fn worker_return_without_permit_release_panics_seals_and_retains() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(RetainPermitExecutor {
            started: started_tx,
            permit: Mutex::new(None),
        });
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();
        let operation_id = operation(48);
        let download_key = key(48);
        bind_submit(
            &handle,
            reserve(&handle, download_key.clone()),
            operation_id,
            48,
        );
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = handle.shared.state.lock().unwrap();
        while !state.sealed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "worker mismatch did not seal scheduler"
            );
            state = handle
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
        assert_eq!(
            state.workers.iter().flatten().copied().next(),
            Some(operation_id)
        );
        assert!(state.executing.contains_key(&operation_id));
        assert!(state.transferring.contains(&operation_id));
        assert!(state.reservations.contains_key(&download_key));
        drop(state);
        drop(executor.permit.lock().unwrap().take());

        let failure = owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect_err("worker ownership mismatch must be reported");
        assert_eq!(failure.reason(), DownloadShutdownReason::WorkerPanicked);
    }

    #[test]
    fn stop_rejects_new_reservations_and_bound_submission() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let reservation = reserve(&handle, key(50));
        let bound = reservation
            .bind(
                operation(50),
                DecimalU64::new(50),
                OperationCancellation::new(),
            )
            .unwrap();

        handle.stop();
        assert_eq!(handle.submit(bound), DownloadSubmitOutcome::Stopping);
        assert!(matches!(
            handle.reserve(key(51)),
            DownloadReserveOutcome::Stopping
        ));
        shutdown(owner);
    }

    #[test]
    fn stop_racing_known_commit_retains_active_key_until_terminal_commit() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let download_key = key(55);
        let reservation = reserve(&handle, download_key.clone());
        let operation_id = operation(55);
        handle.stop();

        assert!(matches!(
            reservation.bind(
                operation_id,
                DecimalU64::new(55),
                OperationCancellation::new(),
            ),
            Err(DownloadBindError::Stopping)
        ));
        assert!(matches!(
            handle.shared.state.lock().unwrap().reservations.get(&download_key),
            Some(ReservationEntry::Active {
                operation_id: active,
                admission_revision,
            }) if *active == operation_id && *admission_revision == DecimalU64::new(55)
        ));
        assert!(handle.finish_committed(operation_id));

        shutdown(owner);
    }

    #[test]
    fn unknown_commit_and_terminalization_seal_and_retain_ownership() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();

        let poisoned_key = key(60);
        let poisoned = reserve(&handle, poisoned_key.clone()).poison();
        {
            let state = handle.shared.state.lock().unwrap();
            assert!(state.sealed);
            assert!(matches!(
                state.reservations.get(&poisoned_key),
                Some(ReservationEntry::Poisoned)
            ));
        }
        assert!(matches!(
            handle.reserve(key(61)),
            DownloadReserveOutcome::Stopping
        ));
        drop(poisoned);

        let fatal = handle.seal_and_retain();
        assert!(fatal.is_sealed());
        shutdown(owner);
        drop(fatal);
    }

    #[test]
    fn uncertain_terminalization_retains_active_key_for_fatal_shutdown() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let download_key = key(65);
        let operation_id = operation(65);
        let bound = reserve(&handle, download_key.clone())
            .bind(
                operation_id,
                DecimalU64::new(65),
                OperationCancellation::new(),
            )
            .unwrap();

        let fatal = handle.seal_and_retain();
        drop(bound);
        assert!(matches!(
            handle.shared.state.lock().unwrap().reservations.get(&download_key),
            Some(ReservationEntry::Active {
                operation_id: active,
                admission_revision,
            }) if *active == operation_id && *admission_revision == DecimalU64::new(65)
        ));
        assert!(matches!(
            handle.reserve(key(66)),
            DownloadReserveOutcome::Stopping
        ));

        shutdown(owner);
        drop(fatal);
    }

    #[test]
    fn worker_panic_seals_admission_and_is_reported_after_all_workers_join() {
        let (started_tx, started_rx) = mpsc::channel();
        let (handle, owner) =
            DownloadSchedulerOwner::spawn(Arc::new(PanicExecutor(started_tx))).unwrap();
        let download_key = key(70);
        let operation_id = operation(70);
        bind_submit(
            &handle,
            reserve(&handle, download_key.clone()),
            operation_id,
            70,
        );
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = handle.shared.state.lock().unwrap();
        while !state.sealed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "panic did not seal scheduler");
            state = handle
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
        assert!(matches!(
            state.reservations.get(&download_key),
            Some(ReservationEntry::Active {
                operation_id: active,
                admission_revision,
            }) if *active == operation_id && *admission_revision == DecimalU64::new(70)
        ));
        assert!(state.workers.contains(&Some(operation_id)));
        assert!(state.executing.contains_key(&operation_id));
        assert!(state.transferring.contains(&operation_id));
        drop(state);
        assert!(matches!(
            handle.reserve(key(71)),
            DownloadReserveOutcome::Stopping
        ));

        let failure = owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect_err("panicked worker must make shutdown fallible");
        assert_eq!(failure.reason(), DownloadShutdownReason::WorkerPanicked);
    }

    #[test]
    fn disconnected_worker_completion_is_fatal_uncertainty_but_handles_are_joined() {
        let (started_tx, _started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, mut owner) = DownloadSchedulerOwner::spawn(executor).unwrap();
        let (disconnected_tx, disconnected_rx) = mpsc::channel();
        drop(disconnected_tx);
        owner.completions[0] = disconnected_rx;
        handle.stop();

        let failure = owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect_err("disconnected completion must be fatal uncertainty");
        assert_eq!(
            failure.reason(),
            DownloadShutdownReason::CompletionDisconnected
        );
        assert!(!failure.retains_unjoined_workers());
    }

    #[test]
    fn shutdown_timeout_retains_unjoined_owner_and_never_detaches_workers() {
        let (started_tx, started_rx) = mpsc::channel();
        let executor = Arc::new(GateExecutor::new(started_tx));
        let (handle, owner) = DownloadSchedulerOwner::spawn(executor.clone()).unwrap();
        bind_submit(&handle, reserve(&handle, key(80)), operation(80), 80);
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let failure = owner
            .shutdown(Instant::now() + Duration::from_millis(20))
            .expect_err("blocked executor must exceed the shutdown deadline");
        assert_eq!(failure.reason(), DownloadShutdownReason::DeadlineExceeded);
        assert!(failure.retains_unjoined_workers());

        let owner = failure.into_owner().expect("fatal owner is retained");
        executor.release_all();
        drop(owner);
    }
}
