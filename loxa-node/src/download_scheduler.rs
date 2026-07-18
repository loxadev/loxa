use crate::artifact_coordinator::{
    valid_portable_artifact_component, ArtifactKey, ArtifactMutationLease,
};
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

pub(crate) struct DownloadWorkerPermit {
    owner: Weak<DownloadShared>,
    worker_index: usize,
    released: bool,
    #[cfg(test)]
    release_probe: Option<Box<dyn FnOnce() + Send>>,
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
                drop(state);
                owner.notify_changed();
                return;
            }
        };
        let removed = if matches!(
            state.reservations.get(&self.key),
            Some(ReservationEntry::Pending { ticket }) if *ticket == self.ticket
        ) {
            state.reservations.remove(&self.key)
        } else {
            None
        };
        drop(state);
        if removed.is_some() {
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
    fn notify_changed(&self) {
        #[cfg(test)]
        if let Some(probe) = &self.notification_probe {
            probe(self);
        }
        self.changed.notify_all();
    }

    fn release_worker(&self, _worker_index: usize) {
        self.notify_changed();
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
