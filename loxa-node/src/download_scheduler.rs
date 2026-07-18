use crate::artifact_coordinator::{ArtifactKey, ArtifactMutationLease};
use crate::operation_cancellation::OperationCancellation;
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;

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
        validate_field(model_id, MAX_MODEL_ID_BYTES)?;
        validate_source_field(source_namespace)?;
        validate_source_field(source_identity)?;
        if let Some(revision) = immutable_revision {
            validate_source_field(revision)?;
        }
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

fn validate_source_field(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_SOURCE_FIELD_BYTES)?;
    if value.contains("://")
        || value.contains(':')
        || value.contains('?')
        || value.contains('#')
        || value.contains('@')
        || value.contains("..")
    {
        Err(DownloadKeyError::AmbiguousSource)
    } else {
        Ok(())
    }
}

fn validate_artifact_subpath(value: &str) -> Result<(), DownloadKeyError> {
    validate_field(value, MAX_ARTIFACT_SUBPATH_BYTES)?;
    let path = std::path::Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
        || value.contains('\\')
        || value.contains(['?', '#'])
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
}

pub(crate) struct DownloadWorkerPermit {
    owner: Weak<DownloadShared>,
    worker_index: usize,
    released: bool,
}

impl DownloadWorkerPermit {
    pub(super) fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_worker(self.worker_index);
            }
            self.released = true;
        }
    }
}

impl Drop for DownloadWorkerPermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}

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

pub(crate) struct PoisonedDownloadReservation(pub(crate) DownloadAdmissionReservation);

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
                owner.changed.notify_all();
                return;
            }
        };
        if matches!(
            state.reservations.get(&self.key),
            Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
        ) {
            state.reservations.remove(&self.key);
            owner.changed.notify_all();
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

pub(crate) struct DownloadShared {
    state: Mutex<DownloadState>,
    changed: Condvar,
}

impl DownloadShared {
    fn release_worker(&self, _worker_index: usize) {
        self.changed.notify_all();
    }
}

struct DownloadState {
    reservations: HashMap<DownloadKey, ReservationEntry>,
    ready: BTreeMap<(DecimalU64, OperationId), BoundDownload>,
    executing: HashMap<OperationId, OperationCancellation>,
    next_ticket: u64,
    stopping: bool,
    sealed: bool,
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

pub(crate) struct DownloadSchedulerOwner {
    handles: Vec<JoinHandle<()>>,
    completions: Vec<std::sync::mpsc::Receiver<WorkerExit>>,
    shared: Arc<DownloadShared>,
}

pub(crate) enum DownloadSubmitOutcome {
    Submitted,
    Cancelled,
    Stopping,
}
