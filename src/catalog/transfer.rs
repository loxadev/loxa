use super::{Manifest, ModelLock};
use crate::safe_file::{
    ensure_regular_descriptors_match, regular_file_identity, RegularFileIdentity,
};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

pub(crate) const MAX_CATALOG_MANIFEST_BYTES: usize = 4_194_304;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogTransferState {
    Fresh,
    MatchingPending,
    Installed,
    InstalledCompletionDebris,
    ArtifactConflict,
    Unsafe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogTransferPlan {
    state: CatalogTransferState,
    pending_temp: Option<RegularFileIdentity>,
    manifest_temp: Option<RegularFileIdentity>,
}

impl CatalogTransferPlan {
    pub(crate) fn state(&self) -> CatalogTransferState {
        self.state
    }
}

#[derive(Eq, PartialEq)]
pub(crate) struct CatalogDiscardFacts {
    pending: Manifest,
    pending_identity: RegularFileIdentity,
    pending_temp: Option<RegularFileIdentity>,
    manifest_temp: Option<RegularFileIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogDiscardError {
    NoIncompleteTransfer,
    InstalledAuthority,
    ArtifactConflict,
    UnsafeLocalState,
}

pub(crate) fn plan_discard(
    lock: &ModelLock,
    expected_model_id: &str,
) -> Result<CatalogDiscardFacts, CatalogDiscardError> {
    plan_discard_with_manifest_read_observer(lock, expected_model_id, |_, _| {})
}

fn plan_discard_with_manifest_read_observer(
    lock: &ModelLock,
    expected_model_id: &str,
    mut after_read: impl FnMut(CatalogEntry, usize),
) -> Result<CatalogDiscardFacts, CatalogDiscardError> {
    let pending_temp = read_catalog_temp_identity(lock, CatalogEntry::PendingTemp, &mut |_| {})
        .map_err(|_| CatalogDiscardError::UnsafeLocalState)?;
    let manifest_temp = read_catalog_temp_identity(lock, CatalogEntry::ManifestTemp, &mut |_| {})
        .map_err(|_| CatalogDiscardError::UnsafeLocalState)?;
    let manifest = read_remote_manifest(lock, CatalogEntry::Manifest, &mut after_read);
    let pending = read_remote_manifest(lock, CatalogEntry::Pending, &mut after_read);
    let (pending, pending_identity) = match (manifest, pending) {
        (ManifestEntry::Unsafe, _) | (_, ManifestEntry::Unsafe) => {
            return Err(CatalogDiscardError::UnsafeLocalState);
        }
        (ManifestEntry::Valid(installed), ManifestEntry::Valid(pending)) => {
            return Err(
                if installed.manifest.id != expected_model_id
                    || pending.manifest.id != expected_model_id
                {
                    CatalogDiscardError::ArtifactConflict
                } else {
                    CatalogDiscardError::InstalledAuthority
                },
            );
        }
        (ManifestEntry::Valid(installed), ManifestEntry::Missing) => {
            return Err(if installed.manifest.id == expected_model_id {
                CatalogDiscardError::InstalledAuthority
            } else {
                CatalogDiscardError::ArtifactConflict
            });
        }
        (ManifestEntry::Missing, ManifestEntry::Valid(pending)) => {
            if pending.manifest.id != expected_model_id {
                return Err(CatalogDiscardError::ArtifactConflict);
            }
            (*pending.manifest, pending.identity)
        }
        (ManifestEntry::Missing, ManifestEntry::Missing) => {
            return Err(CatalogDiscardError::NoIncompleteTransfer);
        }
    };
    Ok(CatalogDiscardFacts {
        pending,
        pending_identity,
        pending_temp,
        manifest_temp,
    })
}

pub(crate) fn plan_transfer(lock: &ModelLock, expected: &Manifest) -> CatalogTransferPlan {
    plan_transfer_with_temp_open_observer(lock, expected, |_| {})
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogRecoveryPoint {
    PendingTempRemoved,
    ManifestTempRemoved,
    DirectorySynced,
    PendingRemoved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogMutationError {
    Changed,
    Durability,
}

pub(crate) fn remove_pending_last(
    lock: &ModelLock,
    facts: CatalogDiscardFacts,
) -> Result<(), CatalogMutationError> {
    let mut sync_directory = File::sync_all;
    let mut after_checkpoint = |_: CatalogRecoveryPoint| Ok(());
    remove_pending_last_inner(lock, facts, &mut sync_directory, &mut after_checkpoint)
}

fn remove_pending_last_inner(
    lock: &ModelLock,
    facts: CatalogDiscardFacts,
    sync_directory: &mut impl FnMut(&File) -> io::Result<()>,
    after_checkpoint: &mut impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
) -> Result<(), CatalogMutationError> {
    let rechecked =
        plan_discard(lock, &facts.pending.id).map_err(|_| CatalogMutationError::Changed)?;
    if rechecked != facts {
        return Err(CatalogMutationError::Changed);
    }
    revalidate_catalog_entry_identity(lock, CatalogEntry::Pending, &facts.pending_identity)
        .map_err(|_| CatalogMutationError::Changed)?;
    unlink_catalog_entry(lock.model_directory(), CatalogDeletion::Pending)
        .map_err(|_| CatalogMutationError::Durability)?;
    after_checkpoint(CatalogRecoveryPoint::PendingRemoved)
        .map_err(|_| CatalogMutationError::Durability)?;
    sync_directory(lock.model_directory()).map_err(|_| CatalogMutationError::Durability)?;
    after_checkpoint(CatalogRecoveryPoint::DirectorySynced)
        .map_err(|_| CatalogMutationError::Durability)?;
    Ok(())
}

pub(crate) fn recover_admitted_catalog_temps(
    lock: &ModelLock,
    plan: &CatalogTransferPlan,
) -> Result<(), CatalogMutationError> {
    let mut sync_directory = File::sync_all;
    let mut after_checkpoint = |_: CatalogRecoveryPoint| Ok(());
    recover_admitted_catalog_temps_inner(lock, plan, &mut sync_directory, &mut after_checkpoint)
}

fn recover_admitted_catalog_temps_inner(
    lock: &ModelLock,
    plan: &CatalogTransferPlan,
    sync_directory: &mut impl FnMut(&File) -> io::Result<()>,
    after_checkpoint: &mut impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
) -> Result<(), CatalogMutationError> {
    if matches!(
        plan.state,
        CatalogTransferState::ArtifactConflict | CatalogTransferState::Unsafe
    ) {
        return Err(CatalogMutationError::Changed);
    }
    revalidate_catalog_temp(lock, CatalogEntry::PendingTemp, plan.pending_temp.as_ref())
        .map_err(|_| CatalogMutationError::Changed)?;
    revalidate_catalog_temp(
        lock,
        CatalogEntry::ManifestTemp,
        plan.manifest_temp.as_ref(),
    )
    .map_err(|_| CatalogMutationError::Changed)?;

    let mut removed = false;
    for (entry, deletion, captured, point) in [
        (
            CatalogEntry::PendingTemp,
            CatalogDeletion::PendingTemp,
            plan.pending_temp.as_ref(),
            CatalogRecoveryPoint::PendingTempRemoved,
        ),
        (
            CatalogEntry::ManifestTemp,
            CatalogDeletion::ManifestTemp,
            plan.manifest_temp.as_ref(),
            CatalogRecoveryPoint::ManifestTempRemoved,
        ),
    ] {
        if captured.is_some() {
            revalidate_catalog_temp(lock, entry, captured)
                .map_err(|_| CatalogMutationError::Changed)?;
            unlink_catalog_entry(lock.model_directory(), deletion)
                .map_err(|_| CatalogMutationError::Durability)?;
            removed = true;
            after_checkpoint(point).map_err(|_| CatalogMutationError::Durability)?;
        }
    }
    if removed {
        sync_directory(lock.model_directory()).map_err(|_| CatalogMutationError::Durability)?;
        after_checkpoint(CatalogRecoveryPoint::DirectorySynced)
            .map_err(|_| CatalogMutationError::Durability)?;
    }
    Ok(())
}

pub(crate) fn recover_installed_completion(
    lock: &ModelLock,
    expected: &Manifest,
    plan: &CatalogTransferPlan,
) -> Result<(), CatalogMutationError> {
    let mut sync_directory = File::sync_all;
    let mut after_checkpoint = |_: CatalogRecoveryPoint| Ok(());
    recover_installed_completion_inner(
        lock,
        expected,
        plan,
        &mut sync_directory,
        &mut after_checkpoint,
    )
}

fn recover_installed_completion_inner(
    lock: &ModelLock,
    expected: &Manifest,
    plan: &CatalogTransferPlan,
    sync_directory: &mut impl FnMut(&File) -> io::Result<()>,
    after_checkpoint: &mut impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
) -> Result<(), CatalogMutationError> {
    if plan.state != CatalogTransferState::InstalledCompletionDebris
        || plan_transfer(lock, expected) != *plan
    {
        return Err(CatalogMutationError::Changed);
    }

    recover_admitted_catalog_temps_inner(lock, plan, sync_directory, after_checkpoint)?;

    let mut after_read = |_: CatalogEntry, _: usize| {};
    let manifest_identity = read_matching_remote_manifest_identity(
        lock,
        CatalogEntry::Manifest,
        expected,
        &mut after_read,
    )
    .map_err(|_| CatalogMutationError::Changed)?
    .ok_or(CatalogMutationError::Changed)?;
    revalidate_catalog_entry_identity(lock, CatalogEntry::Manifest, &manifest_identity)
        .map_err(|_| CatalogMutationError::Changed)?;

    let pending_identity = read_matching_remote_manifest_identity(
        lock,
        CatalogEntry::Pending,
        expected,
        &mut after_read,
    )
    .map_err(|_| CatalogMutationError::Changed)?;
    if let Some(pending_identity) = pending_identity {
        revalidate_catalog_entry_identity(lock, CatalogEntry::Manifest, &manifest_identity)
            .map_err(|_| CatalogMutationError::Changed)?;
        revalidate_catalog_entry_identity(lock, CatalogEntry::Pending, &pending_identity)
            .map_err(|_| CatalogMutationError::Changed)?;
        unlink_catalog_entry(lock.model_directory(), CatalogDeletion::Pending)
            .map_err(|_| CatalogMutationError::Durability)?;
        after_checkpoint(CatalogRecoveryPoint::PendingRemoved)
            .map_err(|_| CatalogMutationError::Durability)?;
        sync_directory(lock.model_directory()).map_err(|_| CatalogMutationError::Durability)?;
        after_checkpoint(CatalogRecoveryPoint::DirectorySynced)
            .map_err(|_| CatalogMutationError::Durability)?;
    }
    Ok(())
}

fn plan_transfer_with_temp_open_observer(
    lock: &ModelLock,
    expected: &Manifest,
    after_open: impl FnMut(CatalogEntry),
) -> CatalogTransferPlan {
    plan_transfer_with_observers(lock, expected, after_open, |_, _| {})
}

fn plan_transfer_with_observers(
    lock: &ModelLock,
    expected: &Manifest,
    mut after_open: impl FnMut(CatalogEntry),
    mut after_read: impl FnMut(CatalogEntry, usize),
) -> CatalogTransferPlan {
    let unsafe_plan = || CatalogTransferPlan {
        state: CatalogTransferState::Unsafe,
        pending_temp: None,
        manifest_temp: None,
    };
    if expected.version != 1 || expected.validate().is_err() {
        return unsafe_plan();
    }
    let manifest = read_remote_manifest(lock, CatalogEntry::Manifest, &mut after_read);
    let pending = read_remote_manifest(lock, CatalogEntry::Pending, &mut after_read);
    let pending_temp =
        match read_catalog_temp_identity(lock, CatalogEntry::PendingTemp, &mut after_open) {
            Ok(identity) => identity,
            Err(()) => return unsafe_plan(),
        };
    let manifest_temp =
        match read_catalog_temp_identity(lock, CatalogEntry::ManifestTemp, &mut after_open) {
            Ok(identity) => identity,
            Err(()) => return unsafe_plan(),
        };
    let mut state = match (manifest, pending) {
        (ManifestEntry::Missing, ManifestEntry::Missing) => CatalogTransferState::Fresh,
        (ManifestEntry::Missing, ManifestEntry::Valid(found))
            if found.manifest.as_ref() == expected =>
        {
            CatalogTransferState::MatchingPending
        }
        (ManifestEntry::Valid(found), ManifestEntry::Missing)
            if found.manifest.as_ref() == expected =>
        {
            CatalogTransferState::Installed
        }
        (ManifestEntry::Valid(installed), ManifestEntry::Valid(pending))
            if installed.manifest.as_ref() == expected && pending.manifest.as_ref() == expected =>
        {
            CatalogTransferState::InstalledCompletionDebris
        }
        (ManifestEntry::Valid(_), ManifestEntry::Missing)
        | (ManifestEntry::Missing, ManifestEntry::Valid(_))
        | (ManifestEntry::Valid(_), ManifestEntry::Valid(_)) => {
            CatalogTransferState::ArtifactConflict
        }
        _ => return unsafe_plan(),
    };
    if state == CatalogTransferState::Installed
        && (pending_temp.is_some() || manifest_temp.is_some())
    {
        state = CatalogTransferState::InstalledCompletionDebris;
    }
    CatalogTransferPlan {
        state,
        pending_temp,
        manifest_temp,
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CatalogEntry {
    Manifest,
    Pending,
    PendingTemp,
    ManifestTemp,
}

#[derive(Clone, Copy)]
enum CatalogDeletion {
    Pending,
    PendingTemp,
    ManifestTemp,
}

#[cfg(unix)]
impl CatalogDeletion {
    fn c_name(self) -> &'static [u8] {
        match self {
            Self::Pending => b"pending.json\0",
            Self::PendingTemp => b"pending.json.tmp\0",
            Self::ManifestTemp => b"manifest.json.tmp\0",
        }
    }
}

impl CatalogEntry {
    fn name(self) -> &'static str {
        match self {
            Self::Manifest => "manifest.json",
            Self::Pending => "pending.json",
            Self::PendingTemp => "pending.json.tmp",
            Self::ManifestTemp => "manifest.json.tmp",
        }
    }

    #[cfg(unix)]
    fn c_name(self) -> &'static [u8] {
        match self {
            Self::Manifest => b"manifest.json\0",
            Self::Pending => b"pending.json\0",
            Self::PendingTemp => b"pending.json.tmp\0",
            Self::ManifestTemp => b"manifest.json.tmp\0",
        }
    }
}

enum ManifestEntry {
    Missing,
    Valid(OpenedManifest),
    Unsafe,
}

struct OpenedManifest {
    manifest: Box<Manifest>,
    identity: RegularFileIdentity,
}

fn read_remote_manifest(
    lock: &ModelLock,
    entry: CatalogEntry,
    after_read: &mut impl FnMut(CatalogEntry, usize),
) -> ManifestEntry {
    let (bytes, identity) =
        match read_catalog_manifest_with_identity(lock.model_directory(), entry, after_read) {
            Ok(Some(opened)) => opened,
            Ok(None) => return ManifestEntry::Missing,
            Err(()) => return ManifestEntry::Unsafe,
        };
    let manifest: Manifest = match serde_json::from_slice(&bytes) {
        Ok(manifest) => manifest,
        Err(_) => return ManifestEntry::Unsafe,
    };
    if manifest.version == 1 && manifest.validate().is_ok() {
        ManifestEntry::Valid(OpenedManifest {
            manifest: Box::new(manifest),
            identity,
        })
    } else {
        ManifestEntry::Unsafe
    }
}

fn read_matching_remote_manifest_identity(
    lock: &ModelLock,
    entry: CatalogEntry,
    expected: &Manifest,
    after_read: &mut impl FnMut(CatalogEntry, usize),
) -> Result<Option<RegularFileIdentity>, ()> {
    match read_remote_manifest(lock, entry, after_read) {
        ManifestEntry::Missing => Ok(None),
        ManifestEntry::Valid(opened) if opened.manifest.as_ref() == expected => {
            Ok(Some(opened.identity))
        }
        ManifestEntry::Valid(_) | ManifestEntry::Unsafe => Err(()),
    }
}

fn read_catalog_temp_identity(
    lock: &ModelLock,
    entry: CatalogEntry,
    after_open: &mut impl FnMut(CatalogEntry),
) -> Result<Option<RegularFileIdentity>, ()> {
    let Some(file) = open_catalog_entry(lock.model_directory(), entry).map_err(|_| ())? else {
        return Ok(None);
    };
    after_open(entry);
    let display = Path::new(entry.name());
    let opened = regular_file_identity(&file, display).map_err(|_| ())?;
    let resolved = open_catalog_entry(lock.model_directory(), entry)
        .map_err(|_| ())?
        .ok_or(())?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, display).map_err(|_| ())?;
    Ok(Some(opened))
}

fn revalidate_catalog_temp(
    lock: &ModelLock,
    entry: CatalogEntry,
    captured: Option<&RegularFileIdentity>,
) -> Result<(), ()> {
    let current = open_catalog_entry(lock.model_directory(), entry).map_err(|_| ())?;
    match (captured, current) {
        (None, None) => Ok(()),
        (Some(captured), Some(_)) => revalidate_catalog_entry_identity(lock, entry, captured),
        _ => Err(()),
    }
}

fn revalidate_catalog_entry_identity(
    lock: &ModelLock,
    entry: CatalogEntry,
    captured: &RegularFileIdentity,
) -> Result<(), ()> {
    let file = open_catalog_entry(lock.model_directory(), entry)
        .map_err(|_| ())?
        .ok_or(())?;
    let display = Path::new(entry.name());
    let opened = regular_file_identity(&file, display).map_err(|_| ())?;
    if &opened != captured {
        return Err(());
    }
    let resolved = open_catalog_entry(lock.model_directory(), entry)
        .map_err(|_| ())?
        .ok_or(())?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, display).map_err(|_| ())
}

fn read_catalog_manifest_with_identity(
    directory: &File,
    entry: CatalogEntry,
    after_read: &mut impl FnMut(CatalogEntry, usize),
) -> Result<Option<(Vec<u8>, RegularFileIdentity)>, ()> {
    let Some(mut file) = open_catalog_entry(directory, entry).map_err(|_| ())? else {
        return Ok(None);
    };
    let display = Path::new(entry.name());
    let opened = regular_file_identity(&file, display).map_err(|_| ())?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8_192];
    while bytes.len() < MAX_CATALOG_MANIFEST_BYTES + 1 {
        let remaining = MAX_CATALOG_MANIFEST_BYTES + 1 - bytes.len();
        let requested = remaining.min(buffer.len());
        let read = file.read(&mut buffer[..requested]).map_err(|_| ())?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    after_read(entry, bytes.len());
    let resolved = open_catalog_entry(directory, entry)
        .map_err(|_| ())?
        .ok_or(())?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, display).map_err(|_| ())?;
    if bytes.len() > MAX_CATALOG_MANIFEST_BYTES {
        return Err(());
    }
    Ok(Some((bytes, opened)))
}

#[cfg(unix)]
fn open_catalog_entry(directory: &File, entry: CatalogEntry) -> io::Result<Option<File>> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is a live directory descriptor, `entry` selects only a
    // fixed NUL-terminated catalog name, and a successful descriptor is owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            entry.c_name().as_ptr().cast(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: `openat` returned a new owned descriptor which has not been wrapped.
    Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
}

#[cfg(unix)]
fn unlink_catalog_entry(directory: &File, deletion: CatalogDeletion) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is a live directory descriptor and `deletion` selects
    // only one of the three fixed recoverable NUL-terminated catalog names.
    let result =
        unsafe { libc::unlinkat(directory.as_raw_fd(), deletion.c_name().as_ptr().cast(), 0) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn open_catalog_entry(_directory: &File, _entry: CatalogEntry) -> io::Result<Option<File>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative catalog audit is unsupported",
    ))
}

#[cfg(not(unix))]
fn unlink_catalog_entry(_directory: &File, _deletion: CatalogDeletion) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative catalog recovery is unsupported",
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        plan_discard, plan_discard_with_manifest_read_observer, plan_transfer,
        plan_transfer_with_observers, plan_transfer_with_temp_open_observer,
        recover_admitted_catalog_temps_inner, recover_installed_completion_inner,
        remove_pending_last_inner, CatalogDiscardError, CatalogDiscardFacts, CatalogEntry,
        CatalogMutationError, CatalogRecoveryPoint, CatalogTransferPlan, CatalogTransferState,
    };
    use crate::catalog::{
        Artifact, ArtifactProvenance, ArtifactRole, Manifest, ModelLock, Origin,
        RuntimeQualification, TEST_LLAMA_BUILD, TEST_MTP_PROFILE,
    };
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[cfg(unix)]
    #[derive(Debug, Eq, PartialEq)]
    struct SnapshotNode {
        device: u64,
        inode: u64,
        mode: u32,
        links: u64,
        len: u64,
        content: SnapshotContent,
    }

    #[cfg(unix)]
    #[derive(Debug, Eq, PartialEq)]
    enum SnapshotContent {
        File(Vec<u8>),
        Directory(Vec<(OsString, SnapshotNode)>),
        Symlink(PathBuf),
        Other,
    }

    #[cfg(unix)]
    fn snapshot_node(path: &Path) -> SnapshotNode {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::symlink_metadata(path).unwrap();
        let file_type = metadata.file_type();
        let content = if file_type.is_file() {
            SnapshotContent::File(fs::read(path).unwrap())
        } else if file_type.is_dir() {
            let mut entries = fs::read_dir(path)
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (entry.file_name(), snapshot_node(&entry.path()))
                })
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            SnapshotContent::Directory(entries)
        } else if file_type.is_symlink() {
            SnapshotContent::Symlink(fs::read_link(path).unwrap())
        } else {
            SnapshotContent::Other
        };
        SnapshotNode {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            links: metadata.nlink(),
            len: metadata.len(),
            content,
        }
    }

    #[cfg(unix)]
    fn create_fifo(path: &Path) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path` is a live NUL-terminated path and the test owns its root.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }

    #[cfg(unix)]
    fn release_waiting_fifo_reader(path: &Path) {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;
        use std::thread;
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path)
            {
                Ok(writer) => {
                    drop(writer);
                    return;
                }
                Err(error)
                    if error.raw_os_error() == Some(libc::ENXIO) && Instant::now() < deadline =>
                {
                    thread::yield_now();
                }
                Err(error) => panic!("failed to release FIFO reader: {error}"),
            }
        }
    }

    #[cfg(unix)]
    fn run_fifo_audit_with_deadline<T>(
        fifo_path: &Path,
        audit: impl FnOnce() -> T + Send + 'static,
    ) -> (bool, T)
    where
        T: Send + 'static,
    {
        use std::sync::mpsc::{self, RecvTimeoutError};
        use std::thread;
        use std::time::Duration;

        let (result_tx, result_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let _ = result_tx.send(audit());
        });
        let first_result = result_rx.recv_timeout(Duration::from_millis(250));
        let returned_without_waiting = first_result.is_ok();
        if matches!(first_result, Err(RecvTimeoutError::Timeout)) {
            release_waiting_fifo_reader(fifo_path);
        }
        let (result, worker_finished): (Result<T, String>, bool) = match first_result {
            Ok(result) => (Ok(result), true),
            Err(RecvTimeoutError::Timeout) => {
                match result_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(result) => (Ok(result), true),
                    Err(RecvTimeoutError::Disconnected) => {
                        (Err("FIFO audit worker disconnected".into()), true)
                    }
                    Err(RecvTimeoutError::Timeout) => (
                        Err(
                            "released FIFO audit did not finish: timed out waiting on channel"
                                .into(),
                        ),
                        false,
                    ),
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                (Err("FIFO audit worker disconnected".into()), true)
            }
        };
        if worker_finished {
            let joined = worker.join();
            assert!(joined.is_ok(), "FIFO audit worker panicked");
        } else {
            drop(worker);
        }
        (returned_without_waiting, result.unwrap())
    }

    fn remote_manifest(id: &str) -> Manifest {
        Manifest {
            version: 1,
            id: id.into(),
            repo: Some("owner/repo".into()),
            revision: Some("0123456789abcdef0123456789abcdef01234567".into()),
            remote_filename: Some("demo-Q4_K_M.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size: 3,
            artifacts: None,
            profile: None,
            runtime: None,
        }
    }

    fn padded_manifest_bytes(manifest: &Manifest, exact_len: usize) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(manifest).unwrap();
        assert!(bytes.len() <= exact_len);
        bytes.resize(exact_len, b' ');
        bytes
    }

    fn local_manifest(id: &str) -> Manifest {
        let mut manifest = remote_manifest(id);
        manifest.version = 2;
        manifest.repo = None;
        manifest.revision = None;
        manifest.remote_filename = None;
        manifest.origin = Some(Origin::Local);
        manifest.source_filename = Some("hostile-local-source.gguf".into());
        manifest
    }

    fn bundle_manifest(id: &str) -> Manifest {
        let primary = local_manifest(id);
        Manifest {
            version: 3,
            id: id.into(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: None,
            source_filename: None,
            local_filename: primary.local_filename.clone(),
            sha256: primary.sha256.clone(),
            size: primary.size,
            artifacts: Some(vec![Artifact {
                role: ArtifactRole::Model,
                local_filename: primary.local_filename,
                sha256: primary.sha256,
                size: primary.size,
                provenance: ArtifactProvenance::Local {
                    source_filename: "hostile-bundle-source.gguf".into(),
                },
            }]),
            profile: Some(TEST_MTP_PROFILE.into()),
            runtime: Some(RuntimeQualification {
                engine: "llama.cpp".into(),
                build: TEST_LLAMA_BUILD.into(),
            }),
        }
    }

    fn write_installed_completion_fixture(model_dir: &Path, expected: &Manifest) {
        fs::create_dir(model_dir).unwrap();
        let bytes = serde_json::to_vec_pretty(expected).unwrap();
        fs::write(model_dir.join("manifest.json"), &bytes).unwrap();
        fs::write(model_dir.join("pending.json"), &bytes).unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"pending temp debris").unwrap();
        fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp debris").unwrap();
    }

    fn recover_admitted_catalog_temps(
        lock: &ModelLock,
        plan: &CatalogTransferPlan,
        mut after_checkpoint: impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
    ) -> Result<(), CatalogMutationError> {
        let mut sync_directory = fs::File::sync_all;
        recover_admitted_catalog_temps_inner(lock, plan, &mut sync_directory, &mut after_checkpoint)
    }

    fn recover_installed_completion(
        lock: &ModelLock,
        expected: &Manifest,
        plan: &CatalogTransferPlan,
        mut after_checkpoint: impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
    ) -> Result<(), CatalogMutationError> {
        let mut sync_directory = fs::File::sync_all;
        recover_installed_completion_inner(
            lock,
            expected,
            plan,
            &mut sync_directory,
            &mut after_checkpoint,
        )
    }

    fn remove_pending_last(
        lock: &ModelLock,
        facts: CatalogDiscardFacts,
        mut after_checkpoint: impl FnMut(CatalogRecoveryPoint) -> Result<(), ()>,
    ) -> Result<(), CatalogMutationError> {
        let mut sync_directory = fs::File::sync_all;
        remove_pending_last_inner(lock, facts, &mut sync_directory, &mut after_checkpoint)
    }

    #[cfg(unix)]
    fn plan_without_mutation(model_dir: &Path, expected: &Manifest) -> CatalogTransferPlan {
        let lock = ModelLock::acquire(model_dir).unwrap();
        let before = snapshot_node(model_dir);

        let plan = plan_transfer(&lock, expected);

        assert_eq!(snapshot_node(model_dir), before);
        plan
    }

    #[cfg(unix)]
    fn plan_with_read_lengths_without_mutation(
        model_dir: &Path,
        expected: &Manifest,
    ) -> (CatalogTransferPlan, Vec<(CatalogEntry, usize)>) {
        let lock = ModelLock::acquire(model_dir).unwrap();
        let before = snapshot_node(model_dir);
        let mut reads = Vec::new();

        let plan = plan_transfer_with_observers(
            &lock,
            expected,
            |_| {},
            |entry, len| reads.push((entry, len)),
        );

        assert_eq!(snapshot_node(model_dir), before);
        (plan, reads)
    }

    #[cfg(unix)]
    fn assert_plan_without_mutation(
        model_dir: &Path,
        expected: &Manifest,
        expected_state: CatalogTransferState,
    ) {
        let plan = plan_without_mutation(model_dir, expected);
        assert_eq!(
            plan,
            CatalogTransferPlan {
                state: expected_state,
                pending_temp: None,
                manifest_temp: None,
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_classifies_fresh_matching_pending_and_clean_installed() {
        let root = tempdir().unwrap();

        let fresh = remote_manifest("fresh");
        let fresh_dir = root.path().join(&fresh.id);
        fs::create_dir(&fresh_dir).unwrap();
        assert_plan_without_mutation(&fresh_dir, &fresh, CatalogTransferState::Fresh);

        let pending = remote_manifest("pending");
        let pending_dir = root.path().join(&pending.id);
        fs::create_dir(&pending_dir).unwrap();
        fs::write(
            pending_dir.join("pending.json"),
            serde_json::to_vec_pretty(&pending).unwrap(),
        )
        .unwrap();
        assert_plan_without_mutation(
            &pending_dir,
            &pending,
            CatalogTransferState::MatchingPending,
        );

        let installed = remote_manifest("installed");
        let installed_dir = root.path().join(&installed.id);
        fs::create_dir(&installed_dir).unwrap();
        fs::write(
            installed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        assert_plan_without_mutation(&installed_dir, &installed, CatalogTransferState::Installed);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_distinguishes_installed_completion_debris() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("installed-with-pending");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        let bytes = serde_json::to_vec_pretty(&expected).unwrap();
        fs::write(model_dir.join("manifest.json"), &bytes).unwrap();
        fs::write(model_dir.join("pending.json"), &bytes).unwrap();

        assert_plan_without_mutation(
            &model_dir,
            &expected,
            CatalogTransferState::InstalledCompletionDebris,
        );
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_reports_different_valid_identity_as_conflict() {
        let root = tempdir().unwrap();

        let expected_installed = remote_manifest("installed-conflict");
        let installed_dir = root.path().join(&expected_installed.id);
        fs::create_dir(&installed_dir).unwrap();
        let mut different_installed = expected_installed.clone();
        different_installed.sha256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
        fs::write(
            installed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&different_installed).unwrap(),
        )
        .unwrap();
        assert_plan_without_mutation(
            &installed_dir,
            &expected_installed,
            CatalogTransferState::ArtifactConflict,
        );

        let expected_pending = remote_manifest("pending-conflict");
        let pending_dir = root.path().join(&expected_pending.id);
        fs::create_dir(&pending_dir).unwrap();
        let mut different_pending = expected_pending.clone();
        different_pending.revision = Some("fedcba9876543210fedcba9876543210fedcba98".into());
        fs::write(
            pending_dir.join("pending.json"),
            serde_json::to_vec_pretty(&different_pending).unwrap(),
        )
        .unwrap();
        assert_plan_without_mutation(
            &pending_dir,
            &expected_pending,
            CatalogTransferState::ArtifactConflict,
        );

        let expected_completion = remote_manifest("completion-conflict");
        let completion_dir = root.path().join(&expected_completion.id);
        fs::create_dir(&completion_dir).unwrap();
        fs::write(
            completion_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&expected_completion).unwrap(),
        )
        .unwrap();
        let mut different_completion = expected_completion.clone();
        different_completion.remote_filename = Some("other-Q4_K_M.gguf".into());
        fs::write(
            completion_dir.join("pending.json"),
            serde_json::to_vec_pretty(&different_completion).unwrap(),
        )
        .unwrap();
        assert_plan_without_mutation(
            &completion_dir,
            &expected_completion,
            CatalogTransferState::ArtifactConflict,
        );
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_refuses_malformed_local_v2_bundle_v3_and_unsafe_state_unchanged() {
        use std::os::unix::fs::symlink;

        const HOSTILE: &str = "hostile-token-secret-escape-\\u001b[31m";
        let root = tempdir().unwrap();

        let malformed = remote_manifest("malformed");
        let malformed_dir = root.path().join(&malformed.id);
        fs::create_dir(&malformed_dir).unwrap();
        let malformed_bytes =
            format!(r#"{{"raw":"{HOSTILE}","path":"../../outside"}}"#).into_bytes();
        assert!(serde_json::from_slice::<Manifest>(&malformed_bytes).is_err());
        fs::write(malformed_dir.join("pending.json"), malformed_bytes).unwrap();
        let malformed_plan = plan_without_mutation(&malformed_dir, &malformed);
        assert_eq!(malformed_plan.state, CatalogTransferState::Unsafe);
        let malformed_debug = format!("{malformed_plan:?}");
        assert!(!malformed_debug.contains(HOSTILE), "{malformed_debug}");
        assert!(
            !malformed_debug.contains("../../outside"),
            "{malformed_debug}"
        );

        let local = remote_manifest("local-v2");
        let local_dir = root.path().join(&local.id);
        fs::create_dir(&local_dir).unwrap();
        let local_owner = local_manifest(&local.id);
        local_owner.validate().unwrap();
        fs::write(
            local_dir.join("pending.json"),
            serde_json::to_vec_pretty(&local_owner).unwrap(),
        )
        .unwrap();
        let local_plan = plan_without_mutation(&local_dir, &local);
        assert_eq!(local_plan.state, CatalogTransferState::Unsafe);
        assert!(!format!("{local_plan:?}").contains("hostile-local-source"));

        let bundle = remote_manifest("bundle-v3");
        let bundle_dir = root.path().join(&bundle.id);
        fs::create_dir(&bundle_dir).unwrap();
        let bundle_owner = bundle_manifest(&bundle.id);
        bundle_owner.validate().unwrap();
        fs::write(
            bundle_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&bundle_owner).unwrap(),
        )
        .unwrap();
        let bundle_plan = plan_without_mutation(&bundle_dir, &bundle);
        assert_eq!(bundle_plan.state, CatalogTransferState::Unsafe);
        assert!(!format!("{bundle_plan:?}").contains("hostile-bundle-source"));

        let unsafe_manifest = remote_manifest("unsafe-entry");
        let unsafe_dir = root.path().join(&unsafe_manifest.id);
        fs::create_dir(&unsafe_dir).unwrap();
        let outside = root.path().join("outside-hostile-token-secret");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);
        symlink(&outside, unsafe_dir.join("pending.json")).unwrap();
        let unsafe_plan = plan_without_mutation(&unsafe_dir, &unsafe_manifest);
        assert_eq!(unsafe_plan.state, CatalogTransferState::Unsafe);
        assert_eq!(snapshot_node(&outside), outside_before);
        let unsafe_debug = format!("{unsafe_plan:?}");
        assert!(!unsafe_debug.contains(&outside.display().to_string()));
        assert!(!unsafe_debug.contains("hostile-token-secret"));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_treats_only_two_safe_fixed_temps_as_content_agnostic_debris() {
        const HOSTILE: &str = "hostile-temp-token-escape-\\u001b[31m";
        let root = tempdir().unwrap();
        let contents = [
            Vec::new(),
            b"{".to_vec(),
            format!(r#"{{"raw":"{HOSTILE}","path":"../../outside"}}"#).into_bytes(),
            serde_json::to_vec_pretty(&remote_manifest("complete-temp-owner")).unwrap(),
        ];

        for (index, content) in contents.iter().enumerate() {
            for (temp_name, pending_present, manifest_present) in [
                ("pending.json.tmp", true, false),
                ("manifest.json.tmp", false, true),
            ] {
                let expected = remote_manifest(&format!("temp-{index}-{pending_present}"));
                let model_dir = root.path().join(&expected.id);
                fs::create_dir(&model_dir).unwrap();
                fs::write(model_dir.join(temp_name), content).unwrap();

                let plan = plan_without_mutation(&model_dir, &expected);

                assert_eq!(plan.state, CatalogTransferState::Fresh);
                assert_eq!(plan.pending_temp.is_some(), pending_present);
                assert_eq!(plan.manifest_temp.is_some(), manifest_present);
                let debug = format!("{plan:?}");
                assert!(!debug.contains(HOSTILE), "{debug}");
                assert!(!debug.contains("../../outside"), "{debug}");
            }
        }

        let installed = remote_manifest("installed-temp-debris");
        let installed_dir = root.path().join(&installed.id);
        fs::create_dir(&installed_dir).unwrap();
        fs::write(
            installed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        fs::write(installed_dir.join("manifest.json.tmp"), b"unparsed debris").unwrap();
        let installed_plan = plan_without_mutation(&installed_dir, &installed);
        assert_eq!(
            installed_plan.state,
            CatalogTransferState::InstalledCompletionDebris
        );
        assert!(installed_plan.manifest_temp.is_some());

        let unrelated = remote_manifest("unrelated-temp-names");
        let unrelated_dir = root.path().join(&unrelated.id);
        fs::create_dir(&unrelated_dir).unwrap();
        fs::write(
            unrelated_dir.join("pending.json.tmp.backup"),
            b"foreign pending temp",
        )
        .unwrap();
        fs::write(
            unrelated_dir.join("manifest.json.tmp.extra"),
            b"foreign manifest temp",
        )
        .unwrap();
        assert_plan_without_mutation(&unrelated_dir, &unrelated, CatalogTransferState::Fresh);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_rejects_symlink_hard_link_nonregular_and_substituted_temps() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let outside = root.path().join("outside-hostile-temp-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        let symlinked = remote_manifest("symlinked-temp");
        let symlinked_dir = root.path().join(&symlinked.id);
        fs::create_dir(&symlinked_dir).unwrap();
        symlink(&outside, symlinked_dir.join("pending.json.tmp")).unwrap();
        let symlinked_plan = plan_without_mutation(&symlinked_dir, &symlinked);
        assert_eq!(symlinked_plan.state, CatalogTransferState::Unsafe);
        assert_eq!(snapshot_node(&outside), outside_before);

        let hard_linked = remote_manifest("hard-linked-temp");
        let hard_linked_dir = root.path().join(&hard_linked.id);
        fs::create_dir(&hard_linked_dir).unwrap();
        fs::hard_link(&outside, hard_linked_dir.join("manifest.json.tmp")).unwrap();
        let outside_with_hard_link = snapshot_node(&outside);
        let hard_linked_plan = plan_without_mutation(&hard_linked_dir, &hard_linked);
        assert_eq!(hard_linked_plan.state, CatalogTransferState::Unsafe);
        assert_eq!(snapshot_node(&outside), outside_with_hard_link);

        let nonregular = remote_manifest("nonregular-temp");
        let nonregular_dir = root.path().join(&nonregular.id);
        fs::create_dir(&nonregular_dir).unwrap();
        fs::create_dir(nonregular_dir.join("pending.json.tmp")).unwrap();
        let nonregular_plan = plan_without_mutation(&nonregular_dir, &nonregular);
        assert_eq!(nonregular_plan.state, CatalogTransferState::Unsafe);
        assert_eq!(snapshot_node(&outside), outside_with_hard_link);

        for plan in [symlinked_plan, hard_linked_plan, nonregular_plan] {
            let debug = format!("{plan:?}");
            assert!(!debug.contains(&outside.display().to_string()), "{debug}");
            assert!(!debug.contains("hostile-temp-witness"), "{debug}");
        }

        let substituted = remote_manifest("substituted-temp");
        let substituted_dir = root.path().join(&substituted.id);
        fs::create_dir(&substituted_dir).unwrap();
        let temp_path = substituted_dir.join("pending.json.tmp");
        let replacement_path = substituted_dir.join("replacement-sentinel");
        let swap_path = substituted_dir.join("swap-sentinel");
        fs::write(&temp_path, b"original temp").unwrap();
        fs::write(&replacement_path, b"replacement temp").unwrap();
        let lock = ModelLock::acquire(&substituted_dir).unwrap();
        let before = snapshot_node(&substituted_dir);
        let mut observer_called = false;

        let substituted_plan =
            plan_transfer_with_temp_open_observer(&lock, &substituted, |entry| {
                if entry == CatalogEntry::PendingTemp {
                    assert!(!observer_called);
                    fs::rename(&temp_path, &swap_path).unwrap();
                    fs::rename(&replacement_path, &temp_path).unwrap();
                    fs::rename(&swap_path, &replacement_path).unwrap();
                    observer_called = true;
                }
            });

        assert!(observer_called);
        fs::rename(&temp_path, &swap_path).unwrap();
        fs::rename(&replacement_path, &temp_path).unwrap();
        fs::rename(&swap_path, &replacement_path).unwrap();
        assert_eq!(snapshot_node(&substituted_dir), before);
        assert_eq!(substituted_plan.state, CatalogTransferState::Unsafe);
        let substituted_debug = format!("{substituted_plan:?}");
        assert!(!substituted_debug.contains("replacement-sentinel"));
        assert!(!substituted_debug.contains("replacement temp"));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_rejects_fifo_managed_entries_without_waiting() {
        let root = tempdir().unwrap();
        let outside = root.path().join("fifo-transfer-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        for (entry, suffix) in [
            (CatalogEntry::Manifest, "manifest"),
            (CatalogEntry::Pending, "pending"),
            (CatalogEntry::PendingTemp, "pending-temp"),
            (CatalogEntry::ManifestTemp, "manifest-temp"),
        ] {
            let expected = remote_manifest(&format!("fifo-transfer-{suffix}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            let fifo_path = model_dir.join(entry.name());
            create_fifo(&fifo_path);
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let before = snapshot_node(&model_dir);

            let (returned_without_waiting, plan) =
                run_fifo_audit_with_deadline(&fifo_path, move || plan_transfer(&lock, &expected));

            assert!(returned_without_waiting, "catalog audit waited on {suffix}");
            assert_eq!(plan.state, CatalogTransferState::Unsafe);
            assert_eq!(snapshot_node(&model_dir), before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn discard_plan_rejects_fifo_managed_entries_without_waiting() {
        let root = tempdir().unwrap();
        let outside = root.path().join("fifo-discard-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        for (entry, suffix) in [
            (CatalogEntry::Manifest, "manifest"),
            (CatalogEntry::Pending, "pending"),
            (CatalogEntry::PendingTemp, "pending-temp"),
            (CatalogEntry::ManifestTemp, "manifest-temp"),
        ] {
            let expected = remote_manifest(&format!("fifo-discard-{suffix}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            if entry != CatalogEntry::Pending {
                fs::write(
                    model_dir.join("pending.json"),
                    serde_json::to_vec_pretty(&expected).unwrap(),
                )
                .unwrap();
            }
            let fifo_path = model_dir.join(entry.name());
            create_fifo(&fifo_path);
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let before = snapshot_node(&model_dir);
            let model_id = expected.id.clone();

            let (returned_without_waiting, result) =
                run_fifo_audit_with_deadline(&fifo_path, move || plan_discard(&lock, &model_id));

            assert!(returned_without_waiting, "discard audit waited on {suffix}");
            assert_eq!(result.err(), Some(CatalogDiscardError::UnsafeLocalState));
            assert_eq!(snapshot_node(&model_dir), before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_audit_second_timeout_does_not_join_a_still_blocked_worker() {
        use std::fs::OpenOptions;
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use std::sync::mpsc::{self, RecvTimeoutError};
        use std::thread;
        use std::time::Duration;

        let root = tempdir().unwrap();
        let fifo_path = root.path().join("second-timeout-fifo");
        create_fifo(&fifo_path);
        let (audit_release_tx, audit_release_rx) = mpsc::channel();
        let (audit_finished_tx, audit_finished_rx) = mpsc::channel();
        let (helper_finished_tx, helper_finished_rx) = mpsc::channel();
        let helper_fifo = fifo_path.clone();
        let audit_fifo = fifo_path.clone();
        let controller = thread::spawn(move || {
            let helper_panicked = catch_unwind(AssertUnwindSafe(|| {
                run_fifo_audit_with_deadline(&helper_fifo, move || {
                    let reader = OpenOptions::new().read(true).open(&audit_fifo).unwrap();
                    drop(reader);
                    audit_release_rx.recv().unwrap();
                    audit_finished_tx.send(()).unwrap();
                });
            }))
            .is_err();
            let _ = helper_finished_tx.send(helper_panicked);
        });

        let first_completion = helper_finished_rx.recv_timeout(Duration::from_secs(2));
        let returned_after_second_timeout = first_completion.is_ok();
        let _ = audit_release_tx.send(());
        let audit_finished = audit_finished_rx
            .recv_timeout(Duration::from_secs(1))
            .is_ok();
        let final_completion = match first_completion {
            Ok(helper_panicked) => Ok(helper_panicked),
            Err(RecvTimeoutError::Timeout) => {
                helper_finished_rx.recv_timeout(Duration::from_secs(1))
            }
            Err(RecvTimeoutError::Disconnected) => Err(RecvTimeoutError::Disconnected),
        };
        if final_completion.is_ok() {
            controller.join().unwrap();
        } else {
            drop(controller);
        }

        assert!(audit_finished, "controlled FIFO audit did not clean up");
        assert!(
            returned_after_second_timeout,
            "FIFO helper waited in join after its second timeout"
        );
        assert!(final_completion.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn catalog_manifest_reads_accept_the_exact_limit_and_reject_limit_plus_one() {
        let root = tempdir().unwrap();
        let outside = root.path().join("manifest-limit-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        for (entry, exact_state, suffix) in [
            (
                CatalogEntry::Pending,
                CatalogTransferState::MatchingPending,
                "pending",
            ),
            (
                CatalogEntry::Manifest,
                CatalogTransferState::Installed,
                "manifest",
            ),
        ] {
            let exact = remote_manifest(&format!("exact-limit-{suffix}"));
            let exact_dir = root.path().join(&exact.id);
            fs::create_dir(&exact_dir).unwrap();
            fs::write(
                exact_dir.join(entry.name()),
                padded_manifest_bytes(&exact, 4_194_304),
            )
            .unwrap();
            let (exact_plan, exact_reads) =
                plan_with_read_lengths_without_mutation(&exact_dir, &exact);
            assert_eq!(exact_plan.state, exact_state);
            assert_eq!(exact_reads.len(), 1);
            assert!(exact_reads[0].0 == entry);
            assert_eq!(exact_reads[0].1, 4_194_304);

            let over = remote_manifest(&format!("over-limit-{suffix}"));
            let over_dir = root.path().join(&over.id);
            fs::create_dir(&over_dir).unwrap();
            fs::write(
                over_dir.join(entry.name()),
                padded_manifest_bytes(&over, 4_194_305),
            )
            .unwrap();
            let (over_plan, over_reads) = plan_with_read_lengths_without_mutation(&over_dir, &over);
            assert_eq!(over_plan.state, CatalogTransferState::Unsafe);
            assert_eq!(over_reads.len(), 1);
            assert!(over_reads[0].0 == entry);
            assert_eq!(over_reads[0].1, 4_194_305);
            assert_eq!(snapshot_node(&outside), outside_before);
        }

        let far_over = remote_manifest("far-over-limit-pending");
        let far_over_dir = root.path().join(&far_over.id);
        fs::create_dir(&far_over_dir).unwrap();
        fs::write(
            far_over_dir.join("pending.json"),
            padded_manifest_bytes(&far_over, 4_198_401),
        )
        .unwrap();
        let (far_over_plan, far_over_reads) =
            plan_with_read_lengths_without_mutation(&far_over_dir, &far_over);
        assert_eq!(far_over_plan.state, CatalogTransferState::Unsafe);
        assert_eq!(far_over_reads.len(), 1);
        assert!(far_over_reads[0].0 == CatalogEntry::Pending);
        assert_eq!(far_over_reads[0].1, 4_194_305);
        assert_eq!(snapshot_node(&outside), outside_before);

        let temp = remote_manifest("over-limit-temp-remains-unread");
        let temp_dir = root.path().join(&temp.id);
        fs::create_dir(&temp_dir).unwrap();
        fs::write(
            temp_dir.join("pending.json.tmp"),
            padded_manifest_bytes(&temp, 4_194_306),
        )
        .unwrap();
        let (temp_plan, temp_reads) = plan_with_read_lengths_without_mutation(&temp_dir, &temp);
        assert_eq!(temp_plan.state, CatalogTransferState::Fresh);
        assert!(temp_plan.pending_temp.is_some());
        assert!(temp_reads.is_empty());
        assert_eq!(snapshot_node(&outside), outside_before);
    }

    #[cfg(unix)]
    #[test]
    fn installed_completion_recovery_uses_the_same_catalog_manifest_limit() {
        let root = tempdir().unwrap();
        let outside = root.path().join("installed-limit-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        let exact = remote_manifest("installed-exact-limit");
        let exact_dir = root.path().join(&exact.id);
        fs::create_dir(&exact_dir).unwrap();
        let exact_bytes = padded_manifest_bytes(&exact, 4_194_304);
        fs::write(exact_dir.join("manifest.json"), &exact_bytes).unwrap();
        fs::write(exact_dir.join("pending.json"), &exact_bytes).unwrap();
        let exact_lock = ModelLock::acquire(&exact_dir).unwrap();
        let manifest_before = snapshot_node(&exact_dir.join("manifest.json"));
        let lock_before = snapshot_node(&exact_dir.join(".lock"));
        let exact_plan = plan_transfer(&exact_lock, &exact);
        assert_eq!(
            exact_plan.state,
            CatalogTransferState::InstalledCompletionDebris
        );
        let mut exact_points = Vec::new();

        recover_installed_completion(&exact_lock, &exact, &exact_plan, |point| {
            exact_points.push(point);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            exact_points,
            [
                CatalogRecoveryPoint::PendingRemoved,
                CatalogRecoveryPoint::DirectorySynced,
            ]
        );
        assert_eq!(
            snapshot_node(&exact_dir.join("manifest.json")),
            manifest_before
        );
        assert!(!exact_dir.join("pending.json").exists());
        assert_eq!(snapshot_node(&exact_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&outside), outside_before);

        for (over_entry, suffix) in [
            (CatalogEntry::Manifest, "manifest"),
            (CatalogEntry::Pending, "pending"),
        ] {
            let expected = remote_manifest(&format!("installed-over-limit-{suffix}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            let bytes = serde_json::to_vec_pretty(&expected).unwrap();
            fs::write(model_dir.join("manifest.json"), &bytes).unwrap();
            fs::write(model_dir.join("pending.json"), &bytes).unwrap();
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let plan = plan_transfer(&lock, &expected);
            assert_eq!(plan.state, CatalogTransferState::InstalledCompletionDebris);
            fs::write(
                model_dir.join(over_entry.name()),
                padded_manifest_bytes(&expected, 4_194_305),
            )
            .unwrap();
            let before = snapshot_node(&model_dir);
            let mut points = Vec::new();

            assert_eq!(
                recover_installed_completion(&lock, &expected, &plan, |point| {
                    points.push(point);
                    Ok(())
                }),
                Err(CatalogMutationError::Changed)
            );

            assert!(points.is_empty());
            assert_eq!(snapshot_node(&model_dir), before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn discard_plan_uses_one_bounded_pending_read() {
        use crate::safe_file::regular_file_identity;

        let root = tempdir().unwrap();
        let outside = root.path().join("discard-read-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        let exact = remote_manifest("discard-one-read-exact-limit");
        let exact_dir = root.path().join(&exact.id);
        fs::create_dir(&exact_dir).unwrap();
        let exact_pending = exact_dir.join("pending.json");
        fs::write(&exact_pending, padded_manifest_bytes(&exact, 4_194_304)).unwrap();
        let exact_lock = ModelLock::acquire(&exact_dir).unwrap();
        let exact_before = snapshot_node(&exact_dir);
        let mut exact_reads = Vec::new();

        let exact_facts =
            plan_discard_with_manifest_read_observer(&exact_lock, &exact.id, |entry, len| {
                exact_reads.push((entry, len))
            })
            .unwrap();

        assert!(exact_reads.as_slice() == [(CatalogEntry::Pending, 4_194_304)]);
        assert_eq!(exact_facts.pending, exact);
        let exact_file = fs::File::open(&exact_pending).unwrap();
        assert_eq!(
            exact_facts.pending_identity,
            regular_file_identity(&exact_file, Path::new("pending.json")).unwrap()
        );
        assert_eq!(snapshot_node(&exact_dir), exact_before);
        assert_eq!(snapshot_node(&outside), outside_before);

        let over = remote_manifest("discard-one-read-over-limit");
        let over_dir = root.path().join(&over.id);
        fs::create_dir(&over_dir).unwrap();
        fs::write(
            over_dir.join("pending.json"),
            padded_manifest_bytes(&over, 4_194_305),
        )
        .unwrap();
        let over_lock = ModelLock::acquire(&over_dir).unwrap();
        let over_before = snapshot_node(&over_dir);
        let mut over_reads = Vec::new();

        assert_eq!(
            plan_discard_with_manifest_read_observer(&over_lock, &over.id, |entry, len| {
                over_reads.push((entry, len));
            })
            .err(),
            Some(CatalogDiscardError::UnsafeLocalState)
        );

        assert!(over_reads.as_slice() == [(CatalogEntry::Pending, 4_194_305)]);
        assert_eq!(snapshot_node(&over_dir), over_before);
        assert_eq!(snapshot_node(&outside), outside_before);

        let substituted = remote_manifest("discard-one-read-substitution");
        let substituted_dir = root.path().join(&substituted.id);
        fs::create_dir(&substituted_dir).unwrap();
        let pending_path = substituted_dir.join("pending.json");
        let replacement_path = substituted_dir.join("replacement-pending");
        let swap_path = substituted_dir.join("swap-pending");
        fs::write(
            &pending_path,
            serde_json::to_vec_pretty(&substituted).unwrap(),
        )
        .unwrap();
        fs::write(
            &replacement_path,
            serde_json::to_vec_pretty(&substituted).unwrap(),
        )
        .unwrap();
        let substituted_lock = ModelLock::acquire(&substituted_dir).unwrap();
        let substituted_before = snapshot_node(&substituted_dir);
        let mut substituted_reads = 0;

        let substituted_result = plan_discard_with_manifest_read_observer(
            &substituted_lock,
            &substituted.id,
            |entry, _| {
                if entry == CatalogEntry::Pending {
                    substituted_reads += 1;
                    fs::rename(&pending_path, &swap_path).unwrap();
                    fs::rename(&replacement_path, &pending_path).unwrap();
                    fs::rename(&swap_path, &replacement_path).unwrap();
                }
            },
        );
        fs::rename(&pending_path, &swap_path).unwrap();
        fs::rename(&replacement_path, &pending_path).unwrap();
        fs::rename(&swap_path, &replacement_path).unwrap();

        assert_eq!(
            substituted_result.err(),
            Some(CatalogDiscardError::UnsafeLocalState)
        );
        assert_eq!(substituted_reads, 1);
        assert_eq!(snapshot_node(&substituted_dir), substituted_before);
        assert_eq!(snapshot_node(&outside), outside_before);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_transfer_plan_exposes_its_closed_state() {
        let root = tempdir().unwrap();
        let assert_state =
            |model_dir: &Path, expected: &Manifest, expected_state: CatalogTransferState| {
                let lock = ModelLock::acquire(model_dir).unwrap();
                let before = snapshot_node(model_dir);

                let plan = plan_transfer(&lock, expected);

                assert_eq!(plan.state(), expected_state);
                assert_eq!(snapshot_node(model_dir), before);
            };

        let fresh = remote_manifest("closed-state-fresh");
        let fresh_dir = root.path().join(&fresh.id);
        fs::create_dir(&fresh_dir).unwrap();
        assert_state(&fresh_dir, &fresh, CatalogTransferState::Fresh);

        let pending = remote_manifest("closed-state-matching-pending");
        let pending_dir = root.path().join(&pending.id);
        fs::create_dir(&pending_dir).unwrap();
        fs::write(
            pending_dir.join("pending.json"),
            serde_json::to_vec_pretty(&pending).unwrap(),
        )
        .unwrap();
        assert_state(
            &pending_dir,
            &pending,
            CatalogTransferState::MatchingPending,
        );

        let installed = remote_manifest("closed-state-installed");
        let installed_dir = root.path().join(&installed.id);
        fs::create_dir(&installed_dir).unwrap();
        fs::write(
            installed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        assert_state(&installed_dir, &installed, CatalogTransferState::Installed);

        let completion = remote_manifest("closed-state-installed-completion");
        let completion_dir = root.path().join(&completion.id);
        fs::create_dir(&completion_dir).unwrap();
        let completion_bytes = serde_json::to_vec_pretty(&completion).unwrap();
        fs::write(completion_dir.join("manifest.json"), &completion_bytes).unwrap();
        fs::write(completion_dir.join("pending.json"), &completion_bytes).unwrap();
        assert_state(
            &completion_dir,
            &completion,
            CatalogTransferState::InstalledCompletionDebris,
        );

        let conflict = remote_manifest("closed-state-conflict");
        let conflict_dir = root.path().join(&conflict.id);
        fs::create_dir(&conflict_dir).unwrap();
        let mut different = conflict.clone();
        different.sha256 =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into();
        different.validate().unwrap();
        fs::write(
            conflict_dir.join("pending.json"),
            serde_json::to_vec_pretty(&different).unwrap(),
        )
        .unwrap();
        assert_state(
            &conflict_dir,
            &conflict,
            CatalogTransferState::ArtifactConflict,
        );

        let unsafe_manifest = remote_manifest("closed-state-unsafe");
        let unsafe_dir = root.path().join(&unsafe_manifest.id);
        fs::create_dir(&unsafe_dir).unwrap();
        fs::write(unsafe_dir.join("pending.json"), b"{ malformed").unwrap();
        assert_state(&unsafe_dir, &unsafe_manifest, CatalogTransferState::Unsafe);
    }

    #[cfg(unix)]
    #[test]
    fn discard_plan_distinguishes_missing_installed_conflict_and_unsafe_state() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let outside = root.path().join("discard-errors-outside-witness");
        fs::write(&outside, b"outside witness").unwrap();
        let outside_before = snapshot_node(&outside);

        let assert_error_without_mutation =
            |model_dir: &Path, model_id: &str, expected_error: CatalogDiscardError| {
                let lock = ModelLock::acquire(model_dir).unwrap();
                let before = snapshot_node(model_dir);
                let error = plan_discard(&lock, model_id).err().unwrap();
                assert_eq!(error, expected_error);
                assert_eq!(snapshot_node(model_dir), before);
                assert_eq!(snapshot_node(&outside), outside_before);
                let debug = format!("{error:?}");
                assert!(!debug.contains(&outside.display().to_string()));
                assert!(!debug.contains("hostile"));
            };

        let missing = remote_manifest("discard-error-missing");
        let missing_dir = root.path().join(&missing.id);
        fs::create_dir(&missing_dir).unwrap();
        assert_error_without_mutation(
            &missing_dir,
            &missing.id,
            CatalogDiscardError::NoIncompleteTransfer,
        );

        let candidate = remote_manifest("discard-error-candidate");
        let candidate_dir = root.path().join(&candidate.id);
        fs::create_dir(&candidate_dir).unwrap();
        fs::write(
            candidate_dir.join("pending.json"),
            serde_json::to_vec_pretty(&candidate).unwrap(),
        )
        .unwrap();
        let candidate_lock = ModelLock::acquire(&candidate_dir).unwrap();
        let candidate_before = snapshot_node(&candidate_dir);
        let candidate_facts = plan_discard(&candidate_lock, &candidate.id).unwrap();
        assert_eq!(candidate_facts.pending, candidate);
        assert_eq!(snapshot_node(&candidate_dir), candidate_before);

        let installed = remote_manifest("discard-error-installed");
        let installed_dir = root.path().join(&installed.id);
        fs::create_dir(&installed_dir).unwrap();
        fs::write(
            installed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        assert_error_without_mutation(
            &installed_dir,
            &installed.id,
            CatalogDiscardError::InstalledAuthority,
        );

        let conflict = remote_manifest("discard-error-conflict");
        let conflict_dir = root.path().join(&conflict.id);
        fs::create_dir(&conflict_dir).unwrap();
        fs::write(
            conflict_dir.join("pending.json"),
            serde_json::to_vec_pretty(&remote_manifest("different-owner")).unwrap(),
        )
        .unwrap();
        assert_error_without_mutation(
            &conflict_dir,
            &conflict.id,
            CatalogDiscardError::ArtifactConflict,
        );

        let malformed = remote_manifest("discard-error-malformed");
        let malformed_dir = root.path().join(&malformed.id);
        fs::create_dir(&malformed_dir).unwrap();
        fs::write(
            malformed_dir.join("pending.json"),
            b"{ hostile malformed pending",
        )
        .unwrap();
        assert_error_without_mutation(
            &malformed_dir,
            &malformed.id,
            CatalogDiscardError::UnsafeLocalState,
        );

        let local = remote_manifest("discard-error-local");
        let local_dir = root.path().join(&local.id);
        fs::create_dir(&local_dir).unwrap();
        fs::write(
            local_dir.join("pending.json"),
            serde_json::to_vec_pretty(&local_manifest(&local.id)).unwrap(),
        )
        .unwrap();
        assert_error_without_mutation(&local_dir, &local.id, CatalogDiscardError::UnsafeLocalState);

        let bundle = remote_manifest("discard-error-bundle");
        let bundle_dir = root.path().join(&bundle.id);
        fs::create_dir(&bundle_dir).unwrap();
        fs::write(
            bundle_dir.join("pending.json"),
            serde_json::to_vec_pretty(&bundle_manifest(&bundle.id)).unwrap(),
        )
        .unwrap();
        assert_error_without_mutation(
            &bundle_dir,
            &bundle.id,
            CatalogDiscardError::UnsafeLocalState,
        );

        let over = remote_manifest("discard-error-over-limit");
        let over_dir = root.path().join(&over.id);
        fs::create_dir(&over_dir).unwrap();
        fs::write(
            over_dir.join("pending.json"),
            padded_manifest_bytes(&over, 4_194_305),
        )
        .unwrap();
        assert_error_without_mutation(&over_dir, &over.id, CatalogDiscardError::UnsafeLocalState);

        let symlinked = remote_manifest("discard-error-symlink");
        let symlinked_dir = root.path().join(&symlinked.id);
        fs::create_dir(&symlinked_dir).unwrap();
        symlink(&outside, symlinked_dir.join("pending.json")).unwrap();
        assert_error_without_mutation(
            &symlinked_dir,
            &symlinked.id,
            CatalogDiscardError::UnsafeLocalState,
        );
    }

    #[cfg(unix)]
    #[test]
    fn admitted_catalog_temp_recovery_removes_only_safe_fixed_temps_then_syncs() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("admitted-temp-recovery");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"truncated pending").unwrap();
        fs::write(model_dir.join("manifest.json.tmp"), b"malformed manifest").unwrap();
        let witness = model_dir.join("foreign-witness");
        fs::write(&witness, b"keep exactly").unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let witness_before = snapshot_node(&witness);
        let lock_before = snapshot_node(&model_dir.join(".lock"));
        let plan = plan_transfer(&lock, &expected);
        assert_eq!(plan.state, CatalogTransferState::Fresh);
        assert!(plan.pending_temp.is_some());
        assert!(plan.manifest_temp.is_some());
        let mut points = Vec::new();

        recover_admitted_catalog_temps(&lock, &plan, |point| {
            points.push(point);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            points,
            [
                CatalogRecoveryPoint::PendingTempRemoved,
                CatalogRecoveryPoint::ManifestTempRemoved,
                CatalogRecoveryPoint::DirectorySynced,
            ]
        );
        assert!(!model_dir.join("pending.json.tmp").exists());
        assert!(!model_dir.join("manifest.json.tmp").exists());
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        drop(lock);
        assert_plan_without_mutation(&model_dir, &expected, CatalogTransferState::Fresh);
    }

    #[cfg(unix)]
    #[test]
    fn admitted_catalog_temp_sync_failure_is_durability_and_reenters_safely() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("admitted-temp-sync-failure");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"pending temp debris").unwrap();
        fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp debris").unwrap();
        let witness = model_dir.join("foreign-witness");
        fs::write(&witness, b"keep admitted witness").unwrap();
        let outside = root.path().join("outside-admitted-sync-witness");
        fs::write(&outside, b"keep outside witness").unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let lock_before = snapshot_node(&model_dir.join(".lock"));
        let witness_before = snapshot_node(&witness);
        let outside_before = snapshot_node(&outside);
        let plan = plan_transfer(&lock, &expected);
        assert_eq!(plan.state, CatalogTransferState::Fresh);
        let mut sync_calls = 0;
        let mut points = Vec::new();

        let result = recover_admitted_catalog_temps_inner(
            &lock,
            &plan,
            &mut |_| {
                sync_calls += 1;
                Err(std::io::Error::other("injected admitted-temp sync failure"))
            },
            &mut |point| {
                points.push(point);
                Ok(())
            },
        );

        assert_eq!(result, Err(CatalogMutationError::Durability));
        assert_eq!(sync_calls, 1);
        assert_eq!(
            points,
            [
                CatalogRecoveryPoint::PendingTempRemoved,
                CatalogRecoveryPoint::ManifestTempRemoved,
            ]
        );
        assert!(!model_dir.join("pending.json.tmp").exists());
        assert!(!model_dir.join("manifest.json.tmp").exists());
        assert!(!model_dir.join("pending.json").exists());
        assert!(!model_dir.join("manifest.json").exists());
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&outside), outside_before);
        drop(lock);

        let reentry_lock = ModelLock::acquire_existing(&model_dir).unwrap();
        let reentry_plan = plan_transfer(&reentry_lock, &expected);
        assert_eq!(reentry_plan.state, CatalogTransferState::Fresh);
        assert!(reentry_plan.pending_temp.is_none());
        assert!(reentry_plan.manifest_temp.is_none());
        super::recover_admitted_catalog_temps(&reentry_lock, &reentry_plan).unwrap();
        assert_eq!(plan_transfer(&reentry_lock, &expected), reentry_plan);
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&outside), outside_before);
    }

    #[cfg(unix)]
    #[test]
    fn matching_pending_with_safe_catalog_temps_recovers_after_admission() {
        for (suffix, has_pending_temp, has_manifest_temp) in [
            ("pending-temp", true, false),
            ("manifest-temp", false, true),
            ("both-temps", true, true),
        ] {
            let root = tempdir().unwrap();
            let expected = remote_manifest(&format!("matching-pending-{suffix}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            fs::write(
                model_dir.join("pending.json"),
                serde_json::to_vec_pretty(&expected).unwrap(),
            )
            .unwrap();
            if has_pending_temp {
                fs::write(model_dir.join("pending.json.tmp"), b"safe pending temp").unwrap();
            }
            if has_manifest_temp {
                fs::write(model_dir.join("manifest.json.tmp"), b"safe manifest temp").unwrap();
            }
            let foreign = model_dir.join("foreign-directory");
            fs::create_dir(&foreign).unwrap();
            fs::write(foreign.join("nested-witness"), b"keep foreign tree").unwrap();
            let outside = root.path().join("outside-matching-pending-witness");
            fs::write(&outside, b"keep outside matching witness").unwrap();
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let pending_before = snapshot_node(&model_dir.join("pending.json"));
            let lock_before = snapshot_node(&model_dir.join(".lock"));
            let foreign_before = snapshot_node(&foreign);
            let outside_before = snapshot_node(&outside);
            assert!(!model_dir.join("manifest.json").exists());
            let plan = plan_transfer(&lock, &expected);
            assert_eq!(plan.state, CatalogTransferState::MatchingPending);
            assert_eq!(plan.pending_temp.is_some(), has_pending_temp);
            assert_eq!(plan.manifest_temp.is_some(), has_manifest_temp);
            let mut sync_calls = 0;
            let mut points = Vec::new();

            recover_admitted_catalog_temps_inner(
                &lock,
                &plan,
                &mut |directory| {
                    sync_calls += 1;
                    directory.sync_all()
                },
                &mut |point| {
                    points.push(point);
                    Ok(())
                },
            )
            .unwrap();

            let mut expected_points = Vec::new();
            if has_pending_temp {
                expected_points.push(CatalogRecoveryPoint::PendingTempRemoved);
            }
            if has_manifest_temp {
                expected_points.push(CatalogRecoveryPoint::ManifestTempRemoved);
            }
            expected_points.push(CatalogRecoveryPoint::DirectorySynced);
            assert_eq!(sync_calls, 1);
            assert_eq!(points, expected_points);
            assert!(!model_dir.join("pending.json.tmp").exists());
            assert!(!model_dir.join("manifest.json.tmp").exists());
            assert!(!model_dir.join("manifest.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("pending.json")),
                pending_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&foreign), foreign_before);
            assert_eq!(snapshot_node(&outside), outside_before);
            drop(lock);

            let reentry_lock = ModelLock::acquire_existing(&model_dir).unwrap();
            let reentry_plan = plan_transfer(&reentry_lock, &expected);
            assert_eq!(reentry_plan.state, CatalogTransferState::MatchingPending);
            assert!(reentry_plan.pending_temp.is_none());
            assert!(reentry_plan.manifest_temp.is_none());
            let mut reentry_sync_calls = 0;
            let mut reentry_points = Vec::new();
            recover_admitted_catalog_temps_inner(
                &reentry_lock,
                &reentry_plan,
                &mut |directory| {
                    reentry_sync_calls += 1;
                    directory.sync_all()
                },
                &mut |point| {
                    reentry_points.push(point);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(reentry_sync_calls, 0);
            assert!(reentry_points.is_empty());
            assert!(!model_dir.join("manifest.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("pending.json")),
                pending_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&foreign), foreign_before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejected_admission_preserves_safe_fixed_temp_bytes_and_identities() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("rejected-temp-recovery");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(
            model_dir.join("pending.json.tmp"),
            b"pending bytes preserved on rejection",
        )
        .unwrap();
        fs::write(
            model_dir.join("manifest.json.tmp"),
            b"manifest bytes preserved on rejection",
        )
        .unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let before = snapshot_node(&model_dir);

        let rejected_plan = plan_transfer(&lock, &expected);

        assert_eq!(snapshot_node(&model_dir), before);
        assert_eq!(rejected_plan.state, CatalogTransferState::Fresh);
        assert!(rejected_plan.pending_temp.is_some());
        assert!(rejected_plan.manifest_temp.is_some());
        drop(lock);

        let fresh_lock = ModelLock::acquire(&model_dir).unwrap();
        let fresh_plan = plan_transfer(&fresh_lock, &expected);
        assert_eq!(fresh_plan, rejected_plan);
        assert_eq!(snapshot_node(&model_dir), before);
    }

    #[cfg(unix)]
    #[test]
    fn installed_completion_recovery_revalidates_exact_authority_and_removes_pending_last() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("installed-completion-recovery");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        let manifest_bytes = serde_json::to_vec_pretty(&expected).unwrap();
        fs::write(model_dir.join("manifest.json"), &manifest_bytes).unwrap();
        fs::write(model_dir.join("pending.json"), &manifest_bytes).unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"pending temp debris").unwrap();
        fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp debris").unwrap();
        let witness = model_dir.join("foreign-witness");
        fs::write(&witness, b"keep installed witness").unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let manifest_before = snapshot_node(&model_dir.join("manifest.json"));
        let witness_before = snapshot_node(&witness);
        let lock_before = snapshot_node(&model_dir.join(".lock"));
        let plan = plan_transfer(&lock, &expected);
        assert_eq!(plan.state, CatalogTransferState::InstalledCompletionDebris);
        let mut points = Vec::new();

        recover_installed_completion(&lock, &expected, &plan, |point| {
            match point {
                CatalogRecoveryPoint::PendingTempRemoved => {
                    assert!(!model_dir.join("pending.json.tmp").exists());
                    assert!(model_dir.join("manifest.json.tmp").exists());
                    assert!(model_dir.join("pending.json").exists());
                }
                CatalogRecoveryPoint::ManifestTempRemoved => {
                    assert!(!model_dir.join("pending.json.tmp").exists());
                    assert!(!model_dir.join("manifest.json.tmp").exists());
                    assert!(model_dir.join("pending.json").exists());
                }
                CatalogRecoveryPoint::PendingRemoved => {
                    assert!(!model_dir.join("pending.json.tmp").exists());
                    assert!(!model_dir.join("manifest.json.tmp").exists());
                    assert!(!model_dir.join("pending.json").exists());
                }
                CatalogRecoveryPoint::DirectorySynced => {}
            }
            assert!(model_dir.join("manifest.json").exists());
            points.push(point);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            points,
            [
                CatalogRecoveryPoint::PendingTempRemoved,
                CatalogRecoveryPoint::ManifestTempRemoved,
                CatalogRecoveryPoint::DirectorySynced,
                CatalogRecoveryPoint::PendingRemoved,
                CatalogRecoveryPoint::DirectorySynced,
            ]
        );
        assert_eq!(
            snapshot_node(&model_dir.join("manifest.json")),
            manifest_before
        );
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        drop(lock);
        assert_plan_without_mutation(&model_dir, &expected, CatalogTransferState::Installed);
    }

    #[cfg(unix)]
    #[test]
    fn installed_completion_sync_failures_are_durability_and_reenter_safely() {
        for failing_sync_call in [1, 2] {
            let root = tempdir().unwrap();
            let expected = remote_manifest(&format!(
                "installed-completion-sync-failure-{failing_sync_call}"
            ));
            let model_dir = root.path().join(&expected.id);
            write_installed_completion_fixture(&model_dir, &expected);
            let witness = model_dir.join("foreign-witness");
            fs::write(&witness, b"keep installed sync witness").unwrap();
            let outside = root.path().join("outside-installed-sync-witness");
            fs::write(&outside, b"keep outside installed sync witness").unwrap();
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let manifest_before = snapshot_node(&model_dir.join("manifest.json"));
            let lock_before = snapshot_node(&model_dir.join(".lock"));
            let witness_before = snapshot_node(&witness);
            let outside_before = snapshot_node(&outside);
            let plan = plan_transfer(&lock, &expected);
            assert_eq!(plan.state, CatalogTransferState::InstalledCompletionDebris);
            let mut sync_calls = 0;
            let mut points = Vec::new();

            let result = recover_installed_completion_inner(
                &lock,
                &expected,
                &plan,
                &mut |directory| {
                    sync_calls += 1;
                    if sync_calls == failing_sync_call {
                        Err(std::io::Error::other("injected installed sync failure"))
                    } else {
                        directory.sync_all()
                    }
                },
                &mut |point| {
                    points.push(point);
                    Ok(())
                },
            );

            assert_eq!(result, Err(CatalogMutationError::Durability));
            assert_eq!(sync_calls, failing_sync_call);
            let expected_points: &[CatalogRecoveryPoint] = if failing_sync_call == 1 {
                &[
                    CatalogRecoveryPoint::PendingTempRemoved,
                    CatalogRecoveryPoint::ManifestTempRemoved,
                ]
            } else {
                &[
                    CatalogRecoveryPoint::PendingTempRemoved,
                    CatalogRecoveryPoint::ManifestTempRemoved,
                    CatalogRecoveryPoint::DirectorySynced,
                    CatalogRecoveryPoint::PendingRemoved,
                ]
            };
            assert_eq!(points, expected_points);
            assert!(!model_dir.join("pending.json.tmp").exists());
            assert!(!model_dir.join("manifest.json.tmp").exists());
            assert_eq!(
                model_dir.join("pending.json").exists(),
                failing_sync_call == 1
            );
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&witness), witness_before);
            assert_eq!(snapshot_node(&outside), outside_before);
            drop(lock);

            let reentry_lock = ModelLock::acquire_existing(&model_dir).unwrap();
            let reentry_plan = plan_transfer(&reentry_lock, &expected);
            let reentry_state = if failing_sync_call == 1 {
                CatalogTransferState::InstalledCompletionDebris
            } else {
                CatalogTransferState::Installed
            };
            assert_eq!(reentry_plan.state, reentry_state);
            if reentry_state == CatalogTransferState::InstalledCompletionDebris {
                super::recover_installed_completion(&reentry_lock, &expected, &reentry_plan)
                    .unwrap();
            }
            assert_eq!(
                plan_transfer(&reentry_lock, &expected).state,
                CatalogTransferState::Installed
            );
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&witness), witness_before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn installed_with_safe_temps_and_no_pending_recovers_completion() {
        for (suffix, has_pending_temp, has_manifest_temp) in [
            ("pending-temp", true, false),
            ("manifest-temp", false, true),
            ("both-temps", true, true),
        ] {
            let root = tempdir().unwrap();
            let expected = remote_manifest(&format!("installed-no-pending-{suffix}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            fs::write(
                model_dir.join("manifest.json"),
                serde_json::to_vec_pretty(&expected).unwrap(),
            )
            .unwrap();
            if has_pending_temp {
                fs::write(model_dir.join("pending.json.tmp"), b"safe pending temp").unwrap();
            }
            if has_manifest_temp {
                fs::write(model_dir.join("manifest.json.tmp"), b"safe manifest temp").unwrap();
            }
            let foreign = model_dir.join("foreign-directory");
            fs::create_dir(&foreign).unwrap();
            fs::write(foreign.join("nested-witness"), b"keep installed tree").unwrap();
            let outside = root.path().join("outside-installed-no-pending-witness");
            fs::write(&outside, b"keep outside installed witness").unwrap();
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let manifest_before = snapshot_node(&model_dir.join("manifest.json"));
            let lock_before = snapshot_node(&model_dir.join(".lock"));
            let foreign_before = snapshot_node(&foreign);
            let outside_before = snapshot_node(&outside);
            assert!(!model_dir.join("pending.json").exists());
            let plan = plan_transfer(&lock, &expected);
            assert_eq!(plan.state, CatalogTransferState::InstalledCompletionDebris);
            assert_eq!(plan.pending_temp.is_some(), has_pending_temp);
            assert_eq!(plan.manifest_temp.is_some(), has_manifest_temp);
            let mut sync_calls = 0;
            let mut points = Vec::new();

            recover_installed_completion_inner(
                &lock,
                &expected,
                &plan,
                &mut |directory| {
                    sync_calls += 1;
                    directory.sync_all()
                },
                &mut |point| {
                    points.push(point);
                    Ok(())
                },
            )
            .unwrap();

            let mut expected_points = Vec::new();
            if has_pending_temp {
                expected_points.push(CatalogRecoveryPoint::PendingTempRemoved);
            }
            if has_manifest_temp {
                expected_points.push(CatalogRecoveryPoint::ManifestTempRemoved);
            }
            expected_points.push(CatalogRecoveryPoint::DirectorySynced);
            assert_eq!(sync_calls, 1);
            assert_eq!(points, expected_points);
            assert!(!model_dir.join("pending.json.tmp").exists());
            assert!(!model_dir.join("manifest.json.tmp").exists());
            assert!(!model_dir.join("pending.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&foreign), foreign_before);
            assert_eq!(snapshot_node(&outside), outside_before);
            drop(lock);

            let reentry_lock = ModelLock::acquire_existing(&model_dir).unwrap();
            let reentry_plan = plan_transfer(&reentry_lock, &expected);
            assert_eq!(reentry_plan.state, CatalogTransferState::Installed);
            assert!(reentry_plan.pending_temp.is_none());
            assert!(reentry_plan.manifest_temp.is_none());
            assert!(!model_dir.join("pending.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&foreign), foreign_before);
            assert_eq!(snapshot_node(&outside), outside_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn installed_completion_recovery_refuses_changed_malformed_or_unsafe_debris_without_mutation() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();

        let changed = remote_manifest("changed-installed-authority");
        let changed_dir = root.path().join(&changed.id);
        write_installed_completion_fixture(&changed_dir, &changed);
        let changed_lock = ModelLock::acquire(&changed_dir).unwrap();
        let changed_plan = plan_transfer(&changed_lock, &changed);
        let mut different = changed.clone();
        different.sha256 =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into();
        different.validate().unwrap();
        fs::write(
            changed_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&different).unwrap(),
        )
        .unwrap();
        let changed_before = snapshot_node(&changed_dir);
        let mut changed_points = Vec::new();
        assert_eq!(
            recover_installed_completion(&changed_lock, &changed, &changed_plan, |point| {
                changed_points.push(point);
                Ok(())
            },),
            Err(CatalogMutationError::Changed)
        );
        assert!(changed_points.is_empty());
        assert_eq!(snapshot_node(&changed_dir), changed_before);

        let malformed = remote_manifest("malformed-pending-debris");
        let malformed_dir = root.path().join(&malformed.id);
        write_installed_completion_fixture(&malformed_dir, &malformed);
        let malformed_lock = ModelLock::acquire(&malformed_dir).unwrap();
        let malformed_plan = plan_transfer(&malformed_lock, &malformed);
        const HOSTILE: &str = "hostile-pending-token-\\u001b[31m";
        fs::write(
            malformed_dir.join("pending.json"),
            format!(r#"{{"raw":"{HOSTILE}","path":"../../outside"}}"#),
        )
        .unwrap();
        let malformed_before = snapshot_node(&malformed_dir);
        let mut malformed_points = Vec::new();
        let malformed_error =
            recover_installed_completion(&malformed_lock, &malformed, &malformed_plan, |point| {
                malformed_points.push(point);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(malformed_error, CatalogMutationError::Changed);
        let error_debug = format!("{malformed_error:?}");
        assert!(malformed_points.is_empty());
        assert_eq!(snapshot_node(&malformed_dir), malformed_before);
        assert!(!error_debug.contains(HOSTILE));
        assert!(!error_debug.contains("../../outside"));

        let unsafe_temp = remote_manifest("unsafe-temp-debris");
        let unsafe_dir = root.path().join(&unsafe_temp.id);
        write_installed_completion_fixture(&unsafe_dir, &unsafe_temp);
        let unsafe_lock = ModelLock::acquire(&unsafe_dir).unwrap();
        let unsafe_plan = plan_transfer(&unsafe_lock, &unsafe_temp);
        let outside = root.path().join("outside-unsafe-temp-witness");
        fs::write(&outside, b"outside bytes").unwrap();
        fs::remove_file(unsafe_dir.join("pending.json.tmp")).unwrap();
        symlink(&outside, unsafe_dir.join("pending.json.tmp")).unwrap();
        let unsafe_before = snapshot_node(&unsafe_dir);
        let outside_before = snapshot_node(&outside);
        let mut unsafe_points = Vec::new();
        assert_eq!(
            recover_installed_completion(&unsafe_lock, &unsafe_temp, &unsafe_plan, |point| {
                unsafe_points.push(point);
                Ok(())
            },),
            Err(CatalogMutationError::Changed)
        );
        assert!(unsafe_points.is_empty());
        assert_eq!(snapshot_node(&unsafe_dir), unsafe_before);
        assert_eq!(snapshot_node(&outside), outside_before);

        let substituted = remote_manifest("substituted-temp-debris");
        let substituted_dir = root.path().join(&substituted.id);
        write_installed_completion_fixture(&substituted_dir, &substituted);
        let substituted_lock = ModelLock::acquire(&substituted_dir).unwrap();
        let substituted_plan = plan_transfer(&substituted_lock, &substituted);
        fs::rename(
            substituted_dir.join("pending.json.tmp"),
            substituted_dir.join("original-pending-temp"),
        )
        .unwrap();
        fs::write(
            substituted_dir.join("pending.json.tmp"),
            b"safe replacement inode",
        )
        .unwrap();
        let substituted_before = snapshot_node(&substituted_dir);
        let mut substituted_points = Vec::new();
        assert_eq!(
            recover_installed_completion(
                &substituted_lock,
                &substituted,
                &substituted_plan,
                |point| {
                    substituted_points.push(point);
                    Ok(())
                },
            ),
            Err(CatalogMutationError::Changed)
        );
        assert!(substituted_points.is_empty());
        assert_eq!(snapshot_node(&substituted_dir), substituted_before);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_reentry_after_each_temp_and_pending_checkpoint_is_recoverable_or_fail_closed() {
        const COMPLETE_SEQUENCE: [CatalogRecoveryPoint; 5] = [
            CatalogRecoveryPoint::PendingTempRemoved,
            CatalogRecoveryPoint::ManifestTempRemoved,
            CatalogRecoveryPoint::DirectorySynced,
            CatalogRecoveryPoint::PendingRemoved,
            CatalogRecoveryPoint::DirectorySynced,
        ];

        for failure_index in 0..COMPLETE_SEQUENCE.len() {
            let root = tempdir().unwrap();
            let expected = remote_manifest(&format!("reentry-checkpoint-{failure_index}"));
            let model_dir = root.path().join(&expected.id);
            write_installed_completion_fixture(&model_dir, &expected);
            let witness = model_dir.join("foreign-witness");
            fs::write(&witness, b"keep through every checkpoint").unwrap();
            let lock = ModelLock::acquire(&model_dir).unwrap();
            let manifest_before = snapshot_node(&model_dir.join("manifest.json"));
            let witness_before = snapshot_node(&witness);
            let lock_before = snapshot_node(&model_dir.join(".lock"));
            let plan = plan_transfer(&lock, &expected);
            let mut observed = Vec::new();

            let result = recover_installed_completion(&lock, &expected, &plan, |point| {
                observed.push(point);
                if observed.len() - 1 == failure_index {
                    Err(())
                } else {
                    Ok(())
                }
            });

            assert_eq!(result, Err(CatalogMutationError::Durability));
            assert_eq!(observed, COMPLETE_SEQUENCE[..=failure_index]);
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&witness), witness_before);
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert!(!model_dir.join("pending.json.tmp").exists());
            assert_eq!(
                model_dir.join("manifest.json.tmp").exists(),
                failure_index == 0
            );
            assert_eq!(model_dir.join("pending.json").exists(), failure_index <= 2);
            drop(lock);

            let reentry_lock = ModelLock::acquire(&model_dir).unwrap();
            let reentry_plan = plan_transfer(&reentry_lock, &expected);
            let expected_state = if failure_index <= 2 {
                CatalogTransferState::InstalledCompletionDebris
            } else {
                CatalogTransferState::Installed
            };
            assert_eq!(reentry_plan.state, expected_state);
            if reentry_plan.state == CatalogTransferState::InstalledCompletionDebris {
                recover_installed_completion(&reentry_lock, &expected, &reentry_plan, |_| Ok(()))
                    .unwrap();
            }
            drop(reentry_lock);

            assert_plan_without_mutation(&model_dir, &expected, CatalogTransferState::Installed);
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json")),
                manifest_before
            );
            assert_eq!(snapshot_node(&witness), witness_before);
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        }
    }

    #[cfg(unix)]
    #[test]
    fn discard_catalog_audit_accepts_only_exact_remote_v1_pending_without_installed_authority() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("discard-exact-pending");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec_pretty(&expected).unwrap(),
        )
        .unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let before = snapshot_node(&model_dir);

        let facts: CatalogDiscardFacts = plan_discard(&lock, &expected.id).unwrap();

        assert_eq!(facts.pending, expected);
        assert_eq!(snapshot_node(&model_dir), before);

        let different_expected = remote_manifest("discard-different-owner");
        let different_dir = root.path().join(&different_expected.id);
        fs::create_dir(&different_dir).unwrap();
        let different_pending = remote_manifest("other-owner");
        fs::write(
            different_dir.join("pending.json"),
            serde_json::to_vec_pretty(&different_pending).unwrap(),
        )
        .unwrap();
        let different_lock = ModelLock::acquire(&different_dir).unwrap();
        let different_before = snapshot_node(&different_dir);

        assert_eq!(
            plan_discard(&different_lock, &different_expected.id).err(),
            Some(CatalogDiscardError::ArtifactConflict)
        );
        assert_eq!(snapshot_node(&different_dir), different_before);

        let installed = remote_manifest("discard-installed-authority");
        let installed_dir = root.path().join(&installed.id);
        fs::create_dir(&installed_dir).unwrap();
        let installed_bytes = serde_json::to_vec_pretty(&installed).unwrap();
        fs::write(installed_dir.join("manifest.json"), &installed_bytes).unwrap();
        fs::write(installed_dir.join("pending.json"), &installed_bytes).unwrap();
        let installed_lock = ModelLock::acquire(&installed_dir).unwrap();
        let installed_before = snapshot_node(&installed_dir);

        assert_eq!(
            plan_discard(&installed_lock, &installed.id).err(),
            Some(CatalogDiscardError::InstalledAuthority)
        );
        assert_eq!(snapshot_node(&installed_dir), installed_before);
    }

    #[cfg(unix)]
    #[test]
    fn pending_last_reports_changed_separately_from_unlink_or_sync_durability() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().unwrap();
        let outside = root.path().join("outside-durability-witness");
        fs::write(&outside, b"outside durability witness").unwrap();
        let outside_before = snapshot_node(&outside);

        let stale = remote_manifest("pending-last-changed");
        let stale_dir = root.path().join(&stale.id);
        fs::create_dir(&stale_dir).unwrap();
        let stale_pending = stale_dir.join("pending.json");
        fs::write(&stale_pending, serde_json::to_vec_pretty(&stale).unwrap()).unwrap();
        let stale_lock = ModelLock::acquire(&stale_dir).unwrap();
        let stale_facts = plan_discard(&stale_lock, &stale.id).unwrap();
        let mut changed = stale.clone();
        changed.sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into();
        fs::write(&stale_pending, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
        let stale_before = snapshot_node(&stale_dir);
        let mut stale_sync_calls = 0;
        let mut stale_points = Vec::new();

        let stale_result = remove_pending_last_inner(
            &stale_lock,
            stale_facts,
            &mut |_| {
                stale_sync_calls += 1;
                Ok(())
            },
            &mut |point| {
                stale_points.push(point);
                Ok(())
            },
        );

        assert_eq!(stale_result, Err(CatalogMutationError::Changed));
        assert_eq!(stale_sync_calls, 0);
        assert!(stale_points.is_empty());
        assert_eq!(snapshot_node(&stale_dir), stale_before);
        assert_eq!(snapshot_node(&outside), outside_before);

        let unlink_failure = remote_manifest("pending-last-unlink-durability");
        let unlink_dir = root.path().join(&unlink_failure.id);
        fs::create_dir(&unlink_dir).unwrap();
        fs::write(
            unlink_dir.join("pending.json"),
            serde_json::to_vec_pretty(&unlink_failure).unwrap(),
        )
        .unwrap();
        let unlink_lock = ModelLock::acquire(&unlink_dir).unwrap();
        let unlink_facts = plan_discard(&unlink_lock, &unlink_failure.id).unwrap();
        fs::set_permissions(&unlink_dir, fs::Permissions::from_mode(0o500)).unwrap();
        let unlink_before = snapshot_node(&unlink_dir);
        let mut unlink_sync_calls = 0;
        let mut unlink_points = Vec::new();

        let unlink_result = remove_pending_last_inner(
            &unlink_lock,
            unlink_facts,
            &mut |_| {
                unlink_sync_calls += 1;
                Ok(())
            },
            &mut |point| {
                unlink_points.push(point);
                Ok(())
            },
        );

        assert_eq!(unlink_result, Err(CatalogMutationError::Durability));
        assert_eq!(unlink_sync_calls, 0);
        assert!(unlink_points.is_empty());
        assert_eq!(snapshot_node(&unlink_dir), unlink_before);
        assert_eq!(snapshot_node(&outside), outside_before);
        fs::set_permissions(&unlink_dir, fs::Permissions::from_mode(0o700)).unwrap();

        let sync_failure = remote_manifest("pending-last-sync-durability");
        let sync_dir = root.path().join(&sync_failure.id);
        fs::create_dir(&sync_dir).unwrap();
        fs::write(
            sync_dir.join("pending.json"),
            serde_json::to_vec_pretty(&sync_failure).unwrap(),
        )
        .unwrap();
        let sync_lock = ModelLock::acquire(&sync_dir).unwrap();
        let sync_facts = plan_discard(&sync_lock, &sync_failure.id).unwrap();
        let mut sync_calls = 0;
        let mut sync_points = Vec::new();

        let sync_result = remove_pending_last_inner(
            &sync_lock,
            sync_facts,
            &mut |_| {
                sync_calls += 1;
                Err(std::io::Error::other("injected directory sync failure"))
            },
            &mut |point| {
                sync_points.push(point);
                Ok(())
            },
        );

        assert_eq!(sync_result, Err(CatalogMutationError::Durability));
        assert_eq!(sync_calls, 1);
        assert_eq!(sync_points, [CatalogRecoveryPoint::PendingRemoved]);
        assert!(!sync_dir.join("pending.json").exists());
        assert_eq!(snapshot_node(&outside), outside_before);
        assert_eq!(format!("{:?}", CatalogMutationError::Changed), "Changed");
        assert_eq!(
            format!("{:?}", CatalogMutationError::Durability),
            "Durability"
        );
    }

    #[cfg(unix)]
    #[test]
    fn discard_pending_last_sync_failure_is_durability_and_reenters_safely() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("discard-pending-last-sync-failure");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec_pretty(&expected).unwrap(),
        )
        .unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"retained pending temp").unwrap();
        fs::write(
            model_dir.join("manifest.json.tmp"),
            b"retained manifest temp",
        )
        .unwrap();
        let witness = model_dir.join("foreign-witness");
        fs::write(&witness, b"keep discard sync witness").unwrap();
        let outside = root.path().join("outside-discard-sync-witness");
        fs::write(&outside, b"keep outside discard witness").unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let pending_temp_before = snapshot_node(&model_dir.join("pending.json.tmp"));
        let manifest_temp_before = snapshot_node(&model_dir.join("manifest.json.tmp"));
        let lock_before = snapshot_node(&model_dir.join(".lock"));
        let witness_before = snapshot_node(&witness);
        let outside_before = snapshot_node(&outside);
        let facts = plan_discard(&lock, &expected.id).unwrap();
        let mut sync_calls = 0;
        let mut points = Vec::new();

        let result = remove_pending_last_inner(
            &lock,
            facts,
            &mut |_| {
                sync_calls += 1;
                Err(std::io::Error::other("injected discard sync failure"))
            },
            &mut |point| {
                points.push(point);
                Ok(())
            },
        );

        assert_eq!(result, Err(CatalogMutationError::Durability));
        assert_eq!(sync_calls, 1);
        assert_eq!(points, [CatalogRecoveryPoint::PendingRemoved]);
        assert!(!model_dir.join("pending.json").exists());
        assert!(!model_dir.join("manifest.json").exists());
        assert_eq!(
            snapshot_node(&model_dir.join("pending.json.tmp")),
            pending_temp_before
        );
        assert_eq!(
            snapshot_node(&model_dir.join("manifest.json.tmp")),
            manifest_temp_before
        );
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&outside), outside_before);
        drop(lock);

        let reentry_lock = ModelLock::acquire_existing(&model_dir).unwrap();
        assert_eq!(
            plan_discard(&reentry_lock, &expected.id).err(),
            Some(CatalogDiscardError::NoIncompleteTransfer)
        );
        assert!(!model_dir.join("pending.json").exists());
        assert_eq!(
            snapshot_node(&model_dir.join("pending.json.tmp")),
            pending_temp_before
        );
        assert_eq!(
            snapshot_node(&model_dir.join("manifest.json.tmp")),
            manifest_temp_before
        );
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(snapshot_node(&outside), outside_before);
    }

    #[cfg(unix)]
    #[test]
    fn discard_catalog_audit_captures_pending_temp_identities_and_absence_facts_read_only() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("discard-captured-temp-facts");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec_pretty(&expected).unwrap(),
        )
        .unwrap();
        fs::write(
            model_dir.join("pending.json.tmp"),
            b"content-agnostic pending temp",
        )
        .unwrap();
        fs::write(
            model_dir.join("manifest.json.tmp"),
            b"content-agnostic manifest temp",
        )
        .unwrap();
        let lock = ModelLock::acquire(&model_dir).unwrap();
        let before = snapshot_node(&model_dir);

        let captured = plan_discard(&lock, &expected.id).unwrap();

        assert!(!model_dir.join("manifest.json").exists());
        assert!(captured.pending_temp.is_some());
        assert!(captured.manifest_temp.is_some());
        assert_eq!(snapshot_node(&model_dir), before);
        drop(lock);

        let fresh_lock = ModelLock::acquire_existing(&model_dir).unwrap();
        let fresh = plan_discard(&fresh_lock, &expected.id).unwrap();
        assert!(fresh == captured);
        assert!(!model_dir.join("manifest.json").exists());
        assert_eq!(snapshot_node(&model_dir), before);

        let absent = remote_manifest("discard-captured-absence-facts");
        let absent_dir = root.path().join(&absent.id);
        fs::create_dir(&absent_dir).unwrap();
        fs::write(
            absent_dir.join("pending.json"),
            serde_json::to_vec_pretty(&absent).unwrap(),
        )
        .unwrap();
        let absent_lock = ModelLock::acquire(&absent_dir).unwrap();
        let absent_before = snapshot_node(&absent_dir);

        let absent_facts = plan_discard(&absent_lock, &absent.id).unwrap();

        assert!(!absent_dir.join("manifest.json").exists());
        assert!(absent_facts.pending_temp.is_none());
        assert!(absent_facts.manifest_temp.is_none());
        assert_eq!(snapshot_node(&absent_dir), absent_before);
    }

    #[cfg(unix)]
    #[test]
    fn discard_pending_last_revalidates_identity_and_leaves_safe_catalog_temps() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("discard-pending-last");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        let pending_bytes = serde_json::to_vec_pretty(&expected).unwrap();
        fs::write(model_dir.join("pending.json"), &pending_bytes).unwrap();
        fs::write(model_dir.join("pending.json.tmp"), b"keep pending temp").unwrap();
        fs::write(model_dir.join("manifest.json.tmp"), b"keep manifest temp").unwrap();
        let witness = model_dir.join("foreign-witness");
        fs::write(&witness, b"keep foreign witness").unwrap();
        drop(ModelLock::acquire(&model_dir).unwrap());
        let lock = ModelLock::acquire_existing(&model_dir).unwrap();
        let pending_temp_before = snapshot_node(&model_dir.join("pending.json.tmp"));
        let manifest_temp_before = snapshot_node(&model_dir.join("manifest.json.tmp"));
        let lock_before = snapshot_node(&model_dir.join(".lock"));
        let witness_before = snapshot_node(&witness);
        let facts = plan_discard(&lock, &expected.id).unwrap();
        let mut points = Vec::new();

        remove_pending_last(&lock, facts, |point| {
            assert!(!model_dir.join("pending.json").exists());
            assert!(model_dir.join("pending.json.tmp").exists());
            assert!(model_dir.join("manifest.json.tmp").exists());
            points.push(point);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            points,
            [
                CatalogRecoveryPoint::PendingRemoved,
                CatalogRecoveryPoint::DirectorySynced,
            ]
        );
        assert_eq!(
            snapshot_node(&model_dir.join("pending.json.tmp")),
            pending_temp_before
        );
        assert_eq!(
            snapshot_node(&model_dir.join("manifest.json.tmp")),
            manifest_temp_before
        );
        assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
        assert_eq!(snapshot_node(&witness), witness_before);
        assert_eq!(
            plan_discard(&lock, &expected.id).err(),
            Some(CatalogDiscardError::NoIncompleteTransfer)
        );

        let substituted = remote_manifest("discard-substituted-pending");
        let substituted_dir = root.path().join(&substituted.id);
        fs::create_dir(&substituted_dir).unwrap();
        let substituted_bytes = serde_json::to_vec_pretty(&substituted).unwrap();
        let substituted_pending = substituted_dir.join("pending.json");
        fs::write(&substituted_pending, &substituted_bytes).unwrap();
        drop(ModelLock::acquire(&substituted_dir).unwrap());
        let substituted_lock = ModelLock::acquire_existing(&substituted_dir).unwrap();
        let stale_facts = plan_discard(&substituted_lock, &substituted.id).unwrap();
        fs::rename(
            &substituted_pending,
            substituted_dir.join("original-pending"),
        )
        .unwrap();
        fs::write(&substituted_pending, &substituted_bytes).unwrap();
        let substituted_before = snapshot_node(&substituted_dir);
        let mut substituted_points = Vec::new();

        assert_eq!(
            remove_pending_last(&substituted_lock, stale_facts, |point| {
                substituted_points.push(point);
                Ok(())
            }),
            Err(CatalogMutationError::Changed)
        );
        assert!(substituted_points.is_empty());
        assert_eq!(snapshot_node(&substituted_dir), substituted_before);
    }

    #[cfg(unix)]
    #[test]
    fn discard_pending_last_accepts_zero_artifact_bytes_but_never_missing_pending() {
        let root = tempdir().unwrap();
        let expected = remote_manifest("discard-zero-artifact-bytes");
        let model_dir = root.path().join(&expected.id);
        fs::create_dir(&model_dir).unwrap();
        fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec_pretty(&expected).unwrap(),
        )
        .unwrap();
        drop(ModelLock::acquire(&model_dir).unwrap());
        let lock = ModelLock::acquire_existing(&model_dir).unwrap();
        let facts = plan_discard(&lock, &expected.id).unwrap();

        remove_pending_last(&lock, facts, |_| Ok(())).unwrap();

        assert!(!model_dir.join("pending.json").exists());
        assert!(model_dir.join(".lock").is_file());

        let missing = remote_manifest("discard-missing-pending");
        let missing_dir = root.path().join(&missing.id);
        fs::create_dir(&missing_dir).unwrap();
        drop(ModelLock::acquire(&missing_dir).unwrap());
        let missing_lock = ModelLock::acquire_existing(&missing_dir).unwrap();
        let missing_before = snapshot_node(&missing_dir);

        assert_eq!(
            plan_discard(&missing_lock, &missing.id).err(),
            Some(CatalogDiscardError::NoIncompleteTransfer)
        );
        assert_eq!(snapshot_node(&missing_dir), missing_before);

        let disappeared = remote_manifest("discard-pending-disappeared");
        let disappeared_dir = root.path().join(&disappeared.id);
        fs::create_dir(&disappeared_dir).unwrap();
        let disappeared_pending = disappeared_dir.join("pending.json");
        fs::write(
            &disappeared_pending,
            serde_json::to_vec_pretty(&disappeared).unwrap(),
        )
        .unwrap();
        drop(ModelLock::acquire(&disappeared_dir).unwrap());
        let disappeared_lock = ModelLock::acquire_existing(&disappeared_dir).unwrap();
        let stale_facts = plan_discard(&disappeared_lock, &disappeared.id).unwrap();
        fs::remove_file(&disappeared_pending).unwrap();
        let disappeared_before = snapshot_node(&disappeared_dir);
        let mut points = Vec::new();

        assert_eq!(
            remove_pending_last(&disappeared_lock, stale_facts, |point| {
                points.push(point);
                Ok(())
            }),
            Err(CatalogMutationError::Changed)
        );
        assert!(points.is_empty());
        assert_eq!(snapshot_node(&disappeared_dir), disappeared_before);
    }

    #[cfg(unix)]
    #[test]
    fn discard_pending_last_refuses_stale_symlink_hard_link_substitution_local_bundle_or_completion_state(
    ) {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let write_pending = |model_dir: &Path, manifest: &Manifest| {
            fs::create_dir(model_dir).unwrap();
            fs::write(
                model_dir.join("pending.json"),
                serde_json::to_vec_pretty(manifest).unwrap(),
            )
            .unwrap();
        };
        let assert_unsafe_plan_refuses_without_mutation = |model_dir: &Path, model_id: &str| {
            let lock = ModelLock::acquire(model_dir).unwrap();
            let before = snapshot_node(model_dir);
            assert_eq!(
                plan_discard(&lock, model_id).err(),
                Some(CatalogDiscardError::UnsafeLocalState)
            );
            assert_eq!(snapshot_node(model_dir), before);
        };

        let stale = remote_manifest("discard-stale-value");
        let stale_dir = root.path().join(&stale.id);
        write_pending(&stale_dir, &stale);
        let stale_lock = ModelLock::acquire(&stale_dir).unwrap();
        let stale_facts = plan_discard(&stale_lock, &stale.id).unwrap();
        let mut changed = stale.clone();
        changed.sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into();
        changed.validate().unwrap();
        fs::write(
            stale_dir.join("pending.json"),
            serde_json::to_vec_pretty(&changed).unwrap(),
        )
        .unwrap();
        let stale_before = snapshot_node(&stale_dir);
        let mut stale_points = Vec::new();
        assert_eq!(
            remove_pending_last(&stale_lock, stale_facts, |point| {
                stale_points.push(point);
                Ok(())
            }),
            Err(CatalogMutationError::Changed)
        );
        assert!(stale_points.is_empty());
        assert_eq!(snapshot_node(&stale_dir), stale_before);

        let symlinked = remote_manifest("discard-symlinked-pending");
        let symlinked_dir = root.path().join(&symlinked.id);
        fs::create_dir(&symlinked_dir).unwrap();
        let symlink_outside = root.path().join("discard-symlink-outside");
        fs::write(
            &symlink_outside,
            serde_json::to_vec_pretty(&symlinked).unwrap(),
        )
        .unwrap();
        symlink(&symlink_outside, symlinked_dir.join("pending.json")).unwrap();
        let symlink_outside_before = snapshot_node(&symlink_outside);
        assert_unsafe_plan_refuses_without_mutation(&symlinked_dir, &symlinked.id);
        assert_eq!(snapshot_node(&symlink_outside), symlink_outside_before);

        let hard_linked = remote_manifest("discard-hard-linked-temp");
        let hard_linked_dir = root.path().join(&hard_linked.id);
        write_pending(&hard_linked_dir, &hard_linked);
        let hard_link_outside = root.path().join("discard-hard-link-outside");
        fs::write(&hard_link_outside, b"outside hard-link witness").unwrap();
        fs::hard_link(
            &hard_link_outside,
            hard_linked_dir.join("manifest.json.tmp"),
        )
        .unwrap();
        let hard_link_outside_before = snapshot_node(&hard_link_outside);
        assert_unsafe_plan_refuses_without_mutation(&hard_linked_dir, &hard_linked.id);
        assert_eq!(snapshot_node(&hard_link_outside), hard_link_outside_before);

        let nonregular = remote_manifest("discard-nonregular-temp");
        let nonregular_dir = root.path().join(&nonregular.id);
        write_pending(&nonregular_dir, &nonregular);
        fs::create_dir(nonregular_dir.join("pending.json.tmp")).unwrap();
        assert_unsafe_plan_refuses_without_mutation(&nonregular_dir, &nonregular.id);

        let substituted = remote_manifest("discard-substituted-temp-fact");
        let substituted_dir = root.path().join(&substituted.id);
        write_pending(&substituted_dir, &substituted);
        let substituted_temp = substituted_dir.join("pending.json.tmp");
        fs::write(&substituted_temp, b"same-length-temp").unwrap();
        let substituted_lock = ModelLock::acquire(&substituted_dir).unwrap();
        let substituted_facts = plan_discard(&substituted_lock, &substituted.id).unwrap();
        fs::rename(
            &substituted_temp,
            substituted_dir.join("original-pending-temp"),
        )
        .unwrap();
        fs::write(&substituted_temp, b"same-length-temp").unwrap();
        let substituted_before = snapshot_node(&substituted_dir);
        let mut substituted_points = Vec::new();
        assert_eq!(
            remove_pending_last(&substituted_lock, substituted_facts, |point| {
                substituted_points.push(point);
                Ok(())
            },),
            Err(CatalogMutationError::Changed)
        );
        assert!(substituted_points.is_empty());
        assert_eq!(snapshot_node(&substituted_dir), substituted_before);

        let local = local_manifest("discard-local-v2");
        let local_dir = root.path().join(&local.id);
        write_pending(&local_dir, &local);
        assert_unsafe_plan_refuses_without_mutation(&local_dir, &local.id);

        let bundle = bundle_manifest("discard-bundle-v3");
        let bundle_dir = root.path().join(&bundle.id);
        write_pending(&bundle_dir, &bundle);
        assert_unsafe_plan_refuses_without_mutation(&bundle_dir, &bundle.id);

        let completion = remote_manifest("discard-completion-authority");
        let completion_dir = root.path().join(&completion.id);
        write_pending(&completion_dir, &completion);
        fs::write(
            completion_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&completion).unwrap(),
        )
        .unwrap();
        let completion_lock = ModelLock::acquire(&completion_dir).unwrap();
        let completion_before = snapshot_node(&completion_dir);
        assert_eq!(
            plan_discard(&completion_lock, &completion.id).err(),
            Some(CatalogDiscardError::InstalledAuthority)
        );
        assert_eq!(snapshot_node(&completion_dir), completion_before);

        const HOSTILE: &str = "hostile-discard-token-\\u001b[31m";
        let malformed_id = "discard-malformed-pending";
        let malformed_dir = root.path().join(malformed_id);
        fs::create_dir(&malformed_dir).unwrap();
        fs::write(
            malformed_dir.join("pending.json"),
            format!(r#"{{"raw":"{HOSTILE}","path":"../../outside"}}"#),
        )
        .unwrap();
        let malformed_lock = ModelLock::acquire(&malformed_dir).unwrap();
        let malformed_before = snapshot_node(&malformed_dir);
        let malformed_error = plan_discard(&malformed_lock, malformed_id).err().unwrap();
        assert_eq!(malformed_error, CatalogDiscardError::UnsafeLocalState);
        let malformed_debug = format!("{malformed_error:?}");
        assert_eq!(snapshot_node(&malformed_dir), malformed_before);
        assert_eq!(malformed_debug, "UnsafeLocalState");
        assert!(!malformed_debug.contains(HOSTILE));
        assert!(!malformed_debug.contains("../../outside"));
    }

    #[cfg(unix)]
    #[test]
    fn discard_pending_last_crash_reentry_retains_the_ownership_marker_until_final_sync() {
        let root = tempdir().unwrap();
        let prepared = remote_manifest("discard-crash-before-pending-last");
        let prepared_dir = root.path().join(&prepared.id);
        fs::create_dir(&prepared_dir).unwrap();
        fs::write(
            prepared_dir.join("pending.json"),
            serde_json::to_vec_pretty(&prepared).unwrap(),
        )
        .unwrap();
        drop(ModelLock::acquire(&prepared_dir).unwrap());
        let prepared_lock = ModelLock::acquire_existing(&prepared_dir).unwrap();
        let captured = plan_discard(&prepared_lock, &prepared.id).unwrap();
        drop(prepared_lock);

        let reentry_lock = ModelLock::acquire_existing(&prepared_dir).unwrap();
        let reentered = plan_discard(&reentry_lock, &prepared.id).unwrap();
        assert!(reentered == captured);
        assert!(prepared_dir.join("pending.json").is_file());
        drop(reentry_lock);

        const POINTS: [CatalogRecoveryPoint; 2] = [
            CatalogRecoveryPoint::PendingRemoved,
            CatalogRecoveryPoint::DirectorySynced,
        ];
        for failure_index in 0..POINTS.len() {
            let expected = remote_manifest(&format!("discard-crash-reentry-{failure_index}"));
            let model_dir = root.path().join(&expected.id);
            fs::create_dir(&model_dir).unwrap();
            fs::write(
                model_dir.join("pending.json"),
                serde_json::to_vec_pretty(&expected).unwrap(),
            )
            .unwrap();
            fs::write(model_dir.join("pending.json.tmp"), b"retained pending temp").unwrap();
            fs::write(
                model_dir.join("manifest.json.tmp"),
                b"retained manifest temp",
            )
            .unwrap();
            let witness = model_dir.join("foreign-witness");
            fs::write(&witness, b"retained foreign witness").unwrap();
            drop(ModelLock::acquire(&model_dir).unwrap());
            let lock = ModelLock::acquire_existing(&model_dir).unwrap();
            let pending_temp_before = snapshot_node(&model_dir.join("pending.json.tmp"));
            let manifest_temp_before = snapshot_node(&model_dir.join("manifest.json.tmp"));
            let lock_before = snapshot_node(&model_dir.join(".lock"));
            let witness_before = snapshot_node(&witness);
            let facts = plan_discard(&lock, &expected.id).unwrap();
            let mut observed = Vec::new();

            let result = remove_pending_last(&lock, facts, |point| {
                observed.push(point);
                if observed.len() - 1 == failure_index {
                    Err(())
                } else {
                    Ok(())
                }
            });

            assert_eq!(result, Err(CatalogMutationError::Durability));
            assert_eq!(observed, POINTS[..=failure_index]);
            assert!(!model_dir.join("pending.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("pending.json.tmp")),
                pending_temp_before
            );
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json.tmp")),
                manifest_temp_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&witness), witness_before);
            drop(lock);

            let fresh_lock = ModelLock::acquire_existing(&model_dir).unwrap();
            assert!(plan_discard(&fresh_lock, &expected.id).is_err());
            assert!(!model_dir.join("pending.json").exists());
            assert_eq!(
                snapshot_node(&model_dir.join("pending.json.tmp")),
                pending_temp_before
            );
            assert_eq!(
                snapshot_node(&model_dir.join("manifest.json.tmp")),
                manifest_temp_before
            );
            assert_eq!(snapshot_node(&model_dir.join(".lock")), lock_before);
            assert_eq!(snapshot_node(&witness), witness_before);
        }
    }
}
