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
    _destination_evidence: Option<ParentEvidence>,
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
        validate_native_destination(destination)?;
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

        let captured_destination = destination_evidence(destination)?;
        hook();

        let recanonicalized_parent =
            std::fs::canonicalize(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
        if recanonicalized_parent != canonical_parent
            || path_evidence(&recanonicalized_parent)? != parent_evidence
            || destination_evidence(destination)? != captured_destination
        {
            return Err(ArtifactKeyError::AmbiguousDestination);
        }
        let logical_destination = if let Some(captured) = captured_destination {
            let canonical_destination = std::fs::canonicalize(destination)
                .map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
            if canonical_destination.parent() != Some(canonical_parent.as_path())
                || destination_evidence(&canonical_destination)? != Some(captured)
                || destination_evidence(destination)? != Some(captured)
            {
                return Err(ArtifactKeyError::AmbiguousDestination);
            }
            normalize_existing_destination(canonical_destination)?
        } else {
            let leaf = file_name
                .to_str()
                .ok_or(ArtifactKeyError::UnsafeDestination)?;
            canonical_parent.join(normalize_absent_leaf(leaf, conservative_ascii_case_fold())?)
        };
        Ok(Self {
            canonical_destination: logical_destination,
            _parent_evidence: parent_evidence,
            _destination_evidence: captured_destination,
        })
    }
}

fn normalize_absent_leaf(leaf: &str, fold_ascii_case: bool) -> Result<String, ArtifactKeyError> {
    if !valid_portable_artifact_component(leaf)
        || !leaf
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ArtifactKeyError::UnsafeDestination);
    }
    Ok(if fold_ascii_case {
        leaf.to_ascii_lowercase()
    } else {
        leaf.to_owned()
    })
}

fn normalize_existing_destination(destination: PathBuf) -> Result<PathBuf, ArtifactKeyError> {
    if !conservative_ascii_case_fold() {
        return Ok(destination);
    }
    let leaf = destination
        .file_name()
        .and_then(|leaf| leaf.to_str())
        .ok_or(ArtifactKeyError::AmbiguousDestination)?;
    let parent = destination
        .parent()
        .ok_or(ArtifactKeyError::AmbiguousDestination)?;
    Ok(parent.join(leaf.to_ascii_lowercase()))
}

const fn conservative_ascii_case_fold() -> bool {
    cfg!(any(windows, target_os = "macos"))
}

#[cfg(unix)]
fn validate_native_destination(destination: &Path) -> Result<(), ArtifactKeyError> {
    if !destination.is_absolute()
        || destination
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
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
        if !valid_portable_artifact_component(component) {
            return Err(ArtifactKeyError::UnsafeDestination);
        }
    }
    Ok(())
}

#[cfg(windows)]
fn validate_native_destination(destination: &Path) -> Result<(), ArtifactKeyError> {
    use std::path::Prefix;

    let raw = destination
        .to_str()
        .ok_or(ArtifactKeyError::UnsafeDestination)?;
    if !destination.is_absolute()
        || !validate_windows_absolute_text(raw)
        || destination.components().any(|component| {
            matches!(component, Component::CurDir | Component::ParentDir)
                || matches!(
                    component,
                    Component::Prefix(prefix)
                        if !matches!(
                            prefix.kind(),
                            Prefix::Disk(_)
                                | Prefix::UNC(_, _)
                                | Prefix::VerbatimDisk(_)
                                | Prefix::VerbatimUNC(_, _)
                        )
                )
        })
    {
        return Err(ArtifactKeyError::UnsafeDestination);
    }
    Ok(())
}

pub(crate) fn valid_portable_artifact_component(component: &str) -> bool {
    !component.is_empty()
        && !component.ends_with(['.', ' '])
        && !component.chars().any(char::is_control)
        && !is_windows_reserved_name(component)
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end_matches(['.', ' ']);
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

fn validate_windows_absolute_text(raw: &str) -> bool {
    if raw.is_empty() || raw.chars().any(char::is_control) {
        return false;
    }
    if let Some(remainder) = raw
        .strip_prefix(r"\\?\UNC\")
        .or_else(|| raw.strip_prefix("//?/UNC/"))
    {
        return validate_windows_unc_remainder(remainder);
    }
    if let Some(remainder) = raw
        .strip_prefix(r"\\?\")
        .or_else(|| raw.strip_prefix("//?/"))
    {
        return validate_windows_drive_remainder(remainder);
    }
    if raw.starts_with(r"\\.\") || raw.starts_with("//./") {
        return false;
    }
    if let Some(remainder) = raw.strip_prefix(r"\\").or_else(|| raw.strip_prefix("//")) {
        return validate_windows_unc_remainder(remainder);
    }
    validate_windows_drive_remainder(raw)
}

fn validate_windows_drive_remainder(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if bytes.len() < 4
        || !bytes[0].is_ascii_alphabetic()
        || bytes[1] != b':'
        || !matches!(bytes[2], b'/' | b'\\')
    {
        return false;
    }
    validate_windows_components(&raw[3..], 1)
}

fn validate_windows_unc_remainder(raw: &str) -> bool {
    validate_windows_components(raw, 3)
}

fn validate_windows_components(raw: &str, minimum: usize) -> bool {
    let components = raw.split(['/', '\\']).collect::<Vec<_>>();
    components.len() >= minimum
        && components.iter().all(|component| {
            !component.is_empty()
                && *component != "."
                && *component != ".."
                && !component.contains(':')
                && valid_portable_artifact_component(component)
        })
}

#[cfg(unix)]
fn destination_evidence(destination: &Path) -> Result<Option<ParentEvidence>, ArtifactKeyError> {
    use std::os::unix::fs::OpenOptionsExt;
    let opened = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(destination)
    {
        Ok(opened) => opened,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Err(ArtifactKeyError::UnsafeDestination);
        }
        Err(_) => return Err(ArtifactKeyError::AmbiguousDestination),
    };
    let metadata = opened
        .metadata()
        .map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
    if !metadata.file_type().is_file() || !has_single_link(&metadata) {
        return Err(ArtifactKeyError::UnsafeDestination);
    }
    Ok(Some(parent_evidence(&opened)?))
}

#[cfg(windows)]
fn destination_evidence(destination: &Path) -> Result<Option<ParentEvidence>, ArtifactKeyError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let opened = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(destination)
    {
        Ok(opened) => opened,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ArtifactKeyError::AmbiguousDestination),
    };
    let metadata = opened
        .metadata()
        .map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || !has_single_link(&metadata)
    {
        return Err(ArtifactKeyError::UnsafeDestination);
    }
    Ok(Some(parent_evidence(&opened)?))
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
    let device = u64::from(
        metadata
            .volume_serial_number()
            .ok_or(ArtifactKeyError::AmbiguousDestination)?,
    );
    let file = metadata
        .file_index()
        .ok_or(ArtifactKeyError::AmbiguousDestination)?;
    if device == 0 || file == 0 {
        return Err(ArtifactKeyError::AmbiguousDestination);
    }
    Ok(ParentEvidence { device, file })
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
                #[cfg(test)]
                notification_probe: None,
            }),
        }
    }

    #[cfg(test)]
    fn new_with_notification_probe_for_test(
        probe: impl Fn(&CoordinatorInner) + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState::default()),
                changed: Condvar::new(),
                notification_probe: Some(Arc::new(probe)),
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
        drop(state);
        self.inner.notify_changed();
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
    #[cfg(test)]
    notification_probe: Option<CoordinatorNotificationProbe>,
}

#[cfg(test)]
type CoordinatorNotificationProbe = Arc<dyn Fn(&CoordinatorInner) + Send + Sync>;

impl CoordinatorInner {
    fn notify_changed(&self) {
        #[cfg(test)]
        if let Some(probe) = &self.notification_probe {
            probe(self);
        }
        self.changed.notify_all();
    }

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
            self.notify_changed();
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
            self.notify_changed();
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
        self.notify_changed();
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

#[cfg(test)]
mod contract_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn windows_absolute_parser_accepts_native_drive_unc_and_extended_forms() {
        for path in [
            r"C:\models\model.gguf",
            r"C:/models/model.gguf",
            r"\\server\share\models\model.gguf",
            r"\\?\C:\models\model.gguf",
            r"\\?\UNC\server\share\models\model.gguf",
        ] {
            assert!(validate_windows_absolute_text(path), "rejected {path:?}");
        }
    }

    #[test]
    fn windows_absolute_parser_rejects_device_relative_ads_and_ambiguous_forms() {
        for path in [
            r"model.gguf",
            r"C:model.gguf",
            r"\model.gguf",
            r"\\.\PhysicalDrive0",
            r"\\?\GLOBALROOT\Device\HarddiskVolume1\model.gguf",
            r"C:\models\..\model.gguf",
            r"C:\models\\model.gguf",
            r"C:\models\model.gguf:stream",
            r"C:\models\CON .gguf",
            r"C:\models\COM1 .bin",
            r"\\server\share",
            r"\\server\\model.gguf",
        ] {
            assert!(!validate_windows_absolute_text(path), "accepted {path:?}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_native_path_components_accept_only_supported_absolute_prefixes() {
        for path in [
            r"C:\models\model.gguf",
            r"\\server\share\models\model.gguf",
            r"\\?\C:\models\model.gguf",
            r"\\?\UNC\server\share\models\model.gguf",
        ] {
            assert!(
                validate_native_destination(Path::new(path)).is_ok(),
                "rejected {path:?}"
            );
        }
        for path in [
            r"C:model.gguf",
            r"\\.\PhysicalDrive0",
            r"\\?\GLOBALROOT\Device\HarddiskVolume1\model.gguf",
        ] {
            assert!(
                validate_native_destination(Path::new(path)).is_err(),
                "accepted {path:?}"
            );
        }
    }

    #[test]
    fn coordinator_seal_notifies_only_after_releasing_state_lock() {
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&observed_unlocked);
        let coordinator =
            ArtifactMutationCoordinator::new_with_notification_probe_for_test(move |inner| {
                observed.store(inner.state.try_lock().is_ok(), Ordering::SeqCst);
            });

        coordinator.seal();

        assert!(observed_unlocked.load(Ordering::SeqCst));
    }

    #[test]
    fn absent_leaf_normalization_is_portable_and_explicitly_case_policy_driven() {
        assert_eq!(
            normalize_absent_leaf("Model-Q4.GGUF", true).unwrap(),
            "model-q4.gguf"
        );
        assert_eq!(
            normalize_absent_leaf("Model-Q4.GGUF", false).unwrap(),
            "Model-Q4.GGUF"
        );
        for leaf in [
            "CON.gguf",
            "model gguf",
            "model:stream",
            "nested/model.gguf",
            "nested\\model.gguf",
            "mødel.gguf",
            "model.gguf.",
        ] {
            assert!(
                normalize_absent_leaf(leaf, true).is_err(),
                "accepted {leaf:?}"
            );
        }
    }
}
