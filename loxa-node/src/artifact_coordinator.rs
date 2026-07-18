use crate::operation_cancellation::OperationCancellation;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct ArtifactKey {
    canonical_destination: PathBuf,
    // Construction fails unless this physical parent identity remains stable
    // throughout capture. Equality deliberately stays destination-based so a
    // replaced parent cannot evade an already-held mutation exclusion.
    _parent_evidence: ParentEvidence,
}

impl PartialEq for ArtifactKey {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_destination == other.canonical_destination
    }
}

impl Eq for ArtifactKey {}

impl Hash for ArtifactKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical_destination.hash(state);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParentEvidence {
    device: u64,
    file: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactKeyError {
    AmbiguousDestination,
    UnsafeDestination,
}

impl ArtifactKey {
    pub(crate) fn from_destination(destination: &Path) -> Result<Self, ArtifactKeyError> {
        Self::from_destination_inner(destination, || {})
    }

    #[cfg(test)]
    pub(crate) fn from_destination_with_test_hook(
        destination: &Path,
        hook: impl FnOnce(),
    ) -> Result<Self, ArtifactKeyError> {
        Self::from_destination_inner(destination, hook)
    }

    fn from_destination_inner(
        destination: &Path,
        hook: impl FnOnce(),
    ) -> Result<Self, ArtifactKeyError> {
        validate_portable_destination(destination)?;
        let file_name = destination
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or(ArtifactKeyError::AmbiguousDestination)?;
        if destination
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(ArtifactKeyError::UnsafeDestination);
        }
        let parent = destination
            .parent()
            .ok_or(ArtifactKeyError::AmbiguousDestination)?;
        let canonical_parent =
            std::fs::canonicalize(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
        let opened_parent = open_parent(&canonical_parent)?;
        let parent_evidence = parent_evidence(&opened_parent)?;

        validate_existing_destination(destination)?;
        hook();

        let recanonicalized_parent =
            std::fs::canonicalize(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
        if recanonicalized_parent != canonical_parent
            || path_evidence(&recanonicalized_parent)? != parent_evidence
        {
            return Err(ArtifactKeyError::AmbiguousDestination);
        }
        Ok(Self {
            canonical_destination: canonical_parent.join(file_name),
            _parent_evidence: parent_evidence,
        })
    }
}

fn validate_portable_destination(destination: &Path) -> Result<(), ArtifactKeyError> {
    let raw = destination
        .to_str()
        .ok_or(ArtifactKeyError::UnsafeDestination)?;
    if !destination.is_absolute()
        || raw.contains('\\')
        || raw.contains(':')
        || raw.contains("//")
        || destination.components().any(|component| {
            matches!(
                component,
                Component::CurDir | Component::ParentDir | Component::Prefix(_)
            )
        })
    {
        return Err(ArtifactKeyError::UnsafeDestination);
    }
    for component in destination.components() {
        let Component::Normal(component) = component else {
            continue;
        };
        let component = component
            .to_str()
            .ok_or(ArtifactKeyError::UnsafeDestination)?;
        if component.ends_with(['.', ' ']) || is_windows_reserved_name(component) {
            return Err(ArtifactKeyError::UnsafeDestination);
        }
    }
    Ok(())
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or(component);
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn validate_existing_destination(destination: &Path) -> Result<(), ArtifactKeyError> {
    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_file() && has_single_link(&metadata) => Ok(()),
        Ok(_) => Err(ArtifactKeyError::UnsafeDestination),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ArtifactKeyError::AmbiguousDestination),
    }
}

#[cfg(unix)]
fn open_parent(parent: &Path) -> Result<std::fs::File, ArtifactKeyError> {
    std::fs::File::open(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)
}

#[cfg(windows)]
fn open_parent(parent: &Path) -> Result<std::fs::File, ArtifactKeyError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(parent)
        .map_err(|_| ArtifactKeyError::AmbiguousDestination)
}

#[cfg(unix)]
fn parent_evidence(parent: &std::fs::File) -> Result<ParentEvidence, ArtifactKeyError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = parent
        .metadata()
        .map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
    Ok(ParentEvidence {
        device: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(unix)]
fn path_evidence(parent: &Path) -> Result<ParentEvidence, ArtifactKeyError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
    Ok(ParentEvidence {
        device: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(unix)]
fn has_single_link(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}

#[cfg(windows)]
fn parent_evidence(parent: &std::fs::File) -> Result<ParentEvidence, ArtifactKeyError> {
    use std::os::windows::fs::MetadataExt;
    let metadata = parent
        .metadata()
        .map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
    Ok(ParentEvidence {
        device: u64::from(
            metadata
                .volume_serial_number()
                .ok_or(ArtifactKeyError::AmbiguousDestination)?,
        ),
        file: metadata
            .file_index()
            .ok_or(ArtifactKeyError::AmbiguousDestination)?,
    })
}

#[cfg(windows)]
fn path_evidence(parent: &Path) -> Result<ParentEvidence, ArtifactKeyError> {
    parent_evidence(&open_parent(parent)?)
}

#[cfg(windows)]
fn has_single_link(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.number_of_links() == Some(1)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactAcquireError {
    Busy,
    Cancelled,
    Poisoned,
    Sealed,
}

#[derive(Clone)]
pub(crate) struct ArtifactMutationCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl ArtifactMutationCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState::default()),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn try_acquire_mutation(
        &self,
        key: ArtifactKey,
    ) -> Result<ArtifactMutationLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        state.ensure_available(&key)?;
        if state.access.contains_key(&key) {
            return Err(ArtifactAcquireError::Busy);
        }
        state
            .access
            .insert(key.clone(), ArtifactAccessState::Mutating);
        Ok(ArtifactMutationLease {
            key,
            owner: Arc::downgrade(&self.inner),
            released: false,
        })
    }

    pub(crate) fn acquire_mutation(
        &self,
        key: ArtifactKey,
        cancellation: &OperationCancellation,
    ) -> Result<ArtifactMutationLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        loop {
            if cancellation.is_cancel_requested() {
                return Err(ArtifactAcquireError::Cancelled);
            }
            state.ensure_available(&key)?;
            if !state.access.contains_key(&key) {
                state
                    .access
                    .insert(key.clone(), ArtifactAccessState::Mutating);
                return Ok(ArtifactMutationLease {
                    key,
                    owner: Arc::downgrade(&self.inner),
                    released: false,
                });
            }
            match self
                .inner
                .changed
                .wait_timeout(state, Duration::from_millis(10))
            {
                Ok((next, _)) => state = next,
                Err(poisoned) => {
                    let (mut next, _) = poisoned.into_inner();
                    next.sealed = true;
                    return Err(ArtifactAcquireError::Sealed);
                }
            }
        }
    }

    pub(crate) fn try_acquire_read(
        &self,
        key: ArtifactKey,
    ) -> Result<ArtifactReadLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        state.ensure_available(&key)?;
        match state.access.get_mut(&key) {
            Some(ArtifactAccessState::Mutating) => return Err(ArtifactAcquireError::Busy),
            Some(ArtifactAccessState::Readers(readers)) => *readers += 1,
            None => {
                state
                    .access
                    .insert(key.clone(), ArtifactAccessState::Readers(1));
            }
        }
        Ok(ArtifactReadLease {
            key,
            owner: Arc::downgrade(&self.inner),
            released: false,
        })
    }

    pub(crate) fn seal(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sealed = true;
        self.inner.changed.notify_all();
    }
}

pub(crate) struct ArtifactMutationLease {
    key: ArtifactKey,
    owner: Weak<CoordinatorInner>,
    released: bool,
}

impl fmt::Debug for ArtifactMutationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactMutationLease")
            .field("key", &self.key)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl ArtifactMutationLease {
    pub(crate) fn poison(mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.poison(&self.key);
        }
        self.released = true;
    }
}

impl Drop for ArtifactMutationLease {
    fn drop(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_mutation(&self.key);
            }
            self.released = true;
        }
    }
}

pub(crate) struct ArtifactReadLease {
    key: ArtifactKey,
    owner: Weak<CoordinatorInner>,
    released: bool,
}

impl fmt::Debug for ArtifactReadLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactReadLease")
            .field("key", &self.key)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl Drop for ArtifactReadLease {
    fn drop(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_read(&self.key);
            }
            self.released = true;
        }
    }
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
    changed: Condvar,
}

impl CoordinatorInner {
    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CoordinatorState>, ArtifactAcquireError> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                Err(ArtifactAcquireError::Sealed)
            }
        }
    }

    fn release_mutation(&self, key: &ArtifactKey) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                return;
            }
        };
        let removed = if !state.poisoned.contains(key)
            && matches!(state.access.get(key), Some(ArtifactAccessState::Mutating))
        {
            state.access.remove(key)
        } else {
            None
        };
        drop(state);
        if removed.is_some() {
            self.changed.notify_all();
        }
    }

    fn release_read(&self, key: &ArtifactKey) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                return;
            }
        };
        if state.poisoned.contains(key) {
            return;
        }
        let remove = if let Some(ArtifactAccessState::Readers(readers)) = state.access.get_mut(key)
        {
            *readers -= 1;
            *readers == 0
        } else {
            false
        };
        let removed = remove.then(|| state.access.remove(key)).flatten();
        drop(state);
        if removed.is_some() {
            self.changed.notify_all();
        }
    }

    fn poison(&self, key: &ArtifactKey) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.poisoned.insert(key.clone());
        state.sealed = true;
        drop(state);
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct CoordinatorState {
    access: HashMap<ArtifactKey, ArtifactAccessState>,
    poisoned: HashSet<ArtifactKey>,
    sealed: bool,
}

impl CoordinatorState {
    fn ensure_available(&self, key: &ArtifactKey) -> Result<(), ArtifactAcquireError> {
        if self.poisoned.contains(key) {
            Err(ArtifactAcquireError::Poisoned)
        } else if self.sealed {
            Err(ArtifactAcquireError::Sealed)
        } else {
            Ok(())
        }
    }
}

enum ArtifactAccessState {
    Mutating,
    Readers(usize),
}
