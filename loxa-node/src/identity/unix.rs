use super::{IdentityError, IdentityErrorClass};
use loxa_protocol::NodeId;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::{Cell, RefCell};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Condvar, LazyLock, Mutex as StandardMutex};

const IDENTITY_DIRECTORY: &[u8] = b"identity\0";
const PRIMARY: &[u8] = b"node.json\0";
const BACKUP: &[u8] = b"node.json.bak\0";
const MAX_RECORD_BYTES: usize = 4096;
const MAX_PASSES: usize = 3;
const TEMP_PREFIX: &[u8] = b".node.json.tmp-";
const TEMP_TOKEN_BYTES: usize = 16;
const TEMP_TOKEN_HEX_LENGTH: usize = TEMP_TOKEN_BYTES * 2;
static ACTIVE_TEMPORARIES: LazyLock<TemporaryRegistry> = LazyLock::new(TemporaryRegistry::default);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ActiveTemporary {
    directory_device: libc::dev_t,
    directory_inode: libc::ino_t,
    name: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TemporaryState {
    Creating,
    Active,
}

#[derive(Default)]
struct TemporaryRegistry {
    entries: StandardMutex<std::collections::HashMap<ActiveTemporary, TemporaryState>>,
    changed: Condvar,
}

#[derive(Clone, Copy)]
pub(super) enum MetadataPolicy {
    Root,
    IdentityDirectory,
    Record { links: libc::nlink_t },
}

pub(super) fn validate_metadata_policy(
    metadata: &libc::stat,
    policy: MetadataPolicy,
    expected_uid: libc::uid_t,
) -> bool {
    if metadata.st_uid != expected_uid {
        return false;
    }
    match policy {
        MetadataPolicy::Root => {
            file_type(metadata) == libc::S_IFDIR && metadata.st_mode & 0o022 == 0
        }
        MetadataPolicy::IdentityDirectory => {
            file_type(metadata) == libc::S_IFDIR && metadata.st_mode & 0o777 == 0o700
        }
        MetadataPolicy::Record { links } => {
            file_type(metadata) == libc::S_IFREG
                && metadata.st_mode & 0o777 == 0o600
                && metadata.st_nlink == links
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FaultPoint {
    Mkdir,
    RootSync,
    DirectoryReopen,
    FileWrite,
    PartialWrite,
    FileSync,
    Publish,
    PostLink,
    Unlink,
    DirectorySync,
    Reopen,
    Cleanup,
    PrimaryRead,
    BackupRead,
    DestinationContention,
    RecoverySync,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BoundaryPoint {
    RootRevalidate,
    DirectoryRevalidate,
    RecoveryUnlink,
    PublicationUnlink,
    RecordReadComplete,
    TemporaryReserved,
}

#[cfg(test)]
type BoundaryHook = Option<(BoundaryPoint, Box<dyn FnOnce()>)>;

#[cfg(test)]
thread_local! {
    static FAULT: RefCell<Option<(FaultPoint, usize)>> = const { RefCell::new(None) };
    static CLEANUP_DIAGNOSTIC: Cell<bool> = const { Cell::new(false) };
    static BOUNDARY_HOOK: RefCell<BoundaryHook> = RefCell::new(None);
}

#[cfg(test)]
pub(super) fn inject_fault(point: FaultPoint) {
    inject_repeated_fault(point, 1);
    CLEANUP_DIAGNOSTIC.with(|observed| observed.set(false));
}

#[cfg(test)]
pub(super) fn inject_repeated_fault(point: FaultPoint, count: usize) {
    assert!(count > 0);
    FAULT.with(|fault| *fault.borrow_mut() = Some((point, count)));
}

#[cfg(test)]
pub(super) fn inject_boundary_hook(point: BoundaryPoint, hook: impl FnOnce() + 'static) {
    BOUNDARY_HOOK.with(|state| *state.borrow_mut() = Some((point, Box::new(hook))));
}

#[cfg(test)]
fn run_boundary_hook(point: BoundaryPoint) {
    BOUNDARY_HOOK.with(|state| {
        let hook = {
            let mut state = state.borrow_mut();
            match state.as_ref().map(|(configured, _)| *configured) {
                Some(configured) if configured == point => state.take().map(|(_, hook)| hook),
                _ => None,
            }
        };
        if let Some(hook) = hook {
            hook();
        }
    });
}

#[cfg(test)]
pub(super) fn cleanup_diagnostic_observed() -> bool {
    CLEANUP_DIAGNOSTIC.with(Cell::get)
}

#[cfg(test)]
pub(super) fn active_temporary_lock_available() -> bool {
    ACTIVE_TEMPORARIES.entries.try_lock().is_ok()
}

#[cfg(test)]
fn fault(point: FaultPoint) -> bool {
    FAULT.with(|fault| {
        let mut fault = fault.borrow_mut();
        match fault.as_mut() {
            Some((configured, remaining)) if *configured == point => {
                *remaining -= 1;
                if *remaining == 0 {
                    *fault = None;
                }
                true
            }
            _ => false,
        }
    })
}

struct RootOwner {
    parent: OwnedFd,
    root: OwnedFd,
    root_name: CString,
    device: libc::dev_t,
    inode: libc::ino_t,
    uid: libc::uid_t,
}

struct IdentityDirectoryOwner<'a> {
    root: &'a RootOwner,
    directory: OwnedFd,
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IdentityEnvelopeV1 {
    schema_version: u32,
    node_id: NodeId,
}

struct ValidRecord {
    node_id: NodeId,
    bytes: Vec<u8>,
}

enum Observation {
    Missing,
    Valid(ValidRecord),
    Unsafe,
    Io(io::Error),
    UnsupportedSchema,
    Corrupt,
    Changing,
}

#[derive(PartialEq, Eq)]
enum Publication {
    Published,
    Existing,
    Contended,
}

enum Recovery {
    Recovered,
    Changing,
}

enum ActiveLink {
    Present(ActiveTemporary),
    Absent,
    Changing,
}

pub(super) fn open_or_create(loxa_root: &Path) -> Result<NodeId, IdentityError> {
    let root = RootOwner::open(loxa_root)?;
    let identity = root.open_identity_directory()?;

    for pass in 0..MAX_PASSES {
        #[cfg(test)]
        if pass > 0 && fault(FaultPoint::Reopen) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        let primary = identity.observe_stable(PRIMARY)?;
        let backup = identity.observe_stable(BACKUP)?;

        match classify_pair(primary, backup)? {
            Matrix::Retry => {
                if pass + 1 == MAX_PASSES {
                    return Err(IdentityError::classified(
                        IdentityErrorClass::ConcurrentChange,
                    ));
                }
                std::thread::yield_now();
                continue;
            }
            Matrix::Open(node_id) => {
                identity.cleanup_stale_temporaries()?;
                return Ok(node_id);
            }
            Matrix::Create => {
                let node_id = NodeId::new_v4();
                let bytes = canonical_bytes(node_id)?;
                match identity.publish(PRIMARY, &bytes)? {
                    Publication::Contended => {
                        if pass + 1 == MAX_PASSES {
                            return Err(IdentityError::classified(
                                IdentityErrorClass::ConcurrentChange,
                            ));
                        }
                        continue;
                    }
                    Publication::Existing => {}
                    Publication::Published => {
                        if identity.publish(BACKUP, &bytes)? == Publication::Contended {
                            if pass + 1 == MAX_PASSES {
                                return Err(IdentityError::classified(
                                    IdentityErrorClass::ConcurrentChange,
                                ));
                            }
                            continue;
                        }
                    }
                }
            }
            Matrix::Repair { destination, bytes } => {
                if identity.publish(destination, &bytes)? == Publication::Contended {
                    if pass + 1 == MAX_PASSES {
                        return Err(IdentityError::classified(
                            IdentityErrorClass::ConcurrentChange,
                        ));
                    }
                    continue;
                }
            }
        }

        #[cfg(test)]
        if fault(FaultPoint::Reopen) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        let primary = identity.observe_stable(PRIMARY)?;
        let backup = identity.observe_stable(BACKUP)?;
        if let Matrix::Open(node_id) = classify_pair(primary, backup)? {
            identity.cleanup_stale_temporaries()?;
            return Ok(node_id);
        }
        if pass + 1 == MAX_PASSES {
            return Err(IdentityError::classified(
                IdentityErrorClass::ConcurrentChange,
            ));
        }
    }

    Err(IdentityError::classified(
        IdentityErrorClass::ConcurrentChange,
    ))
}

enum Matrix {
    Retry,
    Open(NodeId),
    Create,
    Repair {
        destination: &'static [u8],
        bytes: Vec<u8>,
    },
}

fn classify_pair(primary: Observation, backup: Observation) -> Result<Matrix, IdentityError> {
    if matches!(&primary, Observation::Changing) || matches!(&backup, Observation::Changing) {
        return Ok(Matrix::Retry);
    }
    match (primary, backup) {
        (Observation::Valid(primary), Observation::Valid(backup)) => {
            if primary.bytes != backup.bytes {
                Err(IdentityError::classified(IdentityErrorClass::Conflict))
            } else {
                Ok(Matrix::Open(primary.node_id))
            }
        }
        (Observation::Missing, Observation::Missing) => Ok(Matrix::Create),
        (Observation::Valid(primary), Observation::Missing) => Ok(Matrix::Repair {
            destination: BACKUP,
            bytes: primary.bytes,
        }),
        (Observation::Missing, Observation::Valid(backup)) => Ok(Matrix::Repair {
            destination: PRIMARY,
            bytes: backup.bytes,
        }),
        (primary, backup) => Err(precedence_error(primary, backup)),
    }
}

fn precedence_error(primary: Observation, backup: Observation) -> IdentityError {
    let observations = [primary, backup];
    if observations
        .iter()
        .any(|observation| matches!(observation, Observation::Unsafe))
    {
        return IdentityError::classified(IdentityErrorClass::UnsafeRecord);
    }
    if observations
        .iter()
        .any(|observation| matches!(observation, Observation::Io(_)))
    {
        for observation in observations {
            if let Observation::Io(source) = observation {
                return IdentityError::with_source(IdentityErrorClass::Io, source);
            }
        }
        unreachable!("an I/O observation was present");
    }
    if observations
        .iter()
        .any(|observation| matches!(observation, Observation::UnsupportedSchema))
    {
        return IdentityError::classified(IdentityErrorClass::SchemaUnsupported);
    }
    IdentityError::classified(IdentityErrorClass::Corrupt)
}

impl RootOwner {
    fn open(loxa_root: &Path) -> Result<Self, IdentityError> {
        let parent_path = loxa_root
            .parent()
            .ok_or_else(|| IdentityError::classified(IdentityErrorClass::UnsafeRoot))?;
        let root_name = path_component(loxa_root.file_name())
            .ok_or_else(|| IdentityError::classified(IdentityErrorClass::UnsafeRoot))?;
        let parent_path = CString::new(parent_path.as_os_str().as_bytes())
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRoot))?;
        let parent = open_owned(
            libc::AT_FDCWD,
            &parent_path,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            IdentityErrorClass::UnsafeRoot,
        )?;
        let root = open_owned(
            parent.as_raw_fd(),
            &root_name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            IdentityErrorClass::UnsafeRoot,
        )?;
        let metadata = fstat(root.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let uid = unsafe { libc::geteuid() };
        if !validate_metadata_policy(&metadata, MetadataPolicy::Root, uid) {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRoot));
        }
        let owner = Self {
            parent,
            root,
            root_name,
            device: metadata.st_dev,
            inode: metadata.st_ino,
            uid,
        };
        owner.revalidate()?;
        Ok(owner)
    }

    fn revalidate(&self) -> Result<(), IdentityError> {
        #[cfg(test)]
        run_boundary_hook(BoundaryPoint::RootRevalidate);
        let metadata = fstatat_nofollow(self.parent.as_raw_fd(), &self.root_name)
            .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeRoot))?;
        if !validate_metadata_policy(&metadata, MetadataPolicy::Root, self.uid)
            || metadata.st_dev != self.device
            || metadata.st_ino != self.inode
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRoot));
        }
        Ok(())
    }

    fn open_identity_directory(&self) -> Result<IdentityDirectoryOwner<'_>, IdentityError> {
        self.revalidate()?;
        let name = cstr(IDENTITY_DIRECTORY);
        #[cfg(test)]
        if fault(FaultPoint::Mkdir) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        let created = unsafe { libc::mkdirat(self.root.as_raw_fd(), name.as_ptr(), 0o700) } == 0;
        if !created {
            let source = io::Error::last_os_error();
            if source.raw_os_error() != Some(libc::EEXIST) {
                return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
            }
        }
        self.revalidate()?;
        #[cfg(test)]
        if fault(FaultPoint::RootSync) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        fsync_fd(self.root.as_raw_fd(), IdentityErrorClass::Durability)?;
        #[cfg(test)]
        if fault(FaultPoint::DirectoryReopen) {
            return Err(IdentityError::classified(
                IdentityErrorClass::UnsafeDirectory,
            ));
        }
        let directory = open_owned(
            self.root.as_raw_fd(),
            name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            IdentityErrorClass::UnsafeDirectory,
        )?;
        let metadata = fstat(directory.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        if !validate_metadata_policy(&metadata, MetadataPolicy::IdentityDirectory, self.uid) {
            return Err(IdentityError::classified(
                IdentityErrorClass::UnsafeDirectory,
            ));
        }
        let owner = IdentityDirectoryOwner {
            root: self,
            directory,
            device: metadata.st_dev,
            inode: metadata.st_ino,
        };
        owner.revalidate()?;
        Ok(owner)
    }
}

impl IdentityDirectoryOwner<'_> {
    fn observe_stable(&self, name: &[u8]) -> Result<Observation, IdentityError> {
        self.observe(name)
    }

    fn revalidate(&self) -> Result<(), IdentityError> {
        self.root.revalidate()?;
        #[cfg(test)]
        run_boundary_hook(BoundaryPoint::DirectoryRevalidate);
        let metadata = fstatat_nofollow(self.root.root.as_raw_fd(), cstr(IDENTITY_DIRECTORY))
            .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeDirectory))?;
        if !validate_metadata_policy(&metadata, MetadataPolicy::IdentityDirectory, self.root.uid)
            || metadata.st_dev != self.device
            || metadata.st_ino != self.inode
        {
            return Err(IdentityError::classified(
                IdentityErrorClass::UnsafeDirectory,
            ));
        }
        Ok(())
    }

    fn observe(&self, name: &[u8]) -> Result<Observation, IdentityError> {
        let name = cstr(name);
        let descriptor = match open_owned_raw(
            self.directory.as_raw_fd(),
            name,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        ) {
            Ok(descriptor) => descriptor,
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                return Ok(Observation::Missing);
            }
            Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                return Ok(Observation::Unsafe);
            }
            Err(source) => return Ok(self.classify_open_failure(name, source)),
        };
        let before = match fstat(descriptor.as_raw_fd()) {
            Ok(metadata) => metadata,
            Err(source) => return Ok(Observation::Io(source)),
        };
        if !record_metadata_has_allowed_links(&before, self.root.uid) {
            return Ok(Observation::Unsafe);
        }
        if before.st_nlink == 2 {
            match self.has_active_link(&before)? {
                ActiveLink::Present(active) => {
                    wait_for_temporary_transition(&active);
                    return Ok(Observation::Changing);
                }
                ActiveLink::Changing => return Ok(Observation::Changing),
                ActiveLink::Absent => {}
            }
            let bytes = match read_bounded(descriptor.as_raw_fd()) {
                Ok(bytes) => bytes,
                Err(source) => return Ok(Observation::Io(source)),
            };
            return match self.recover_interrupted_publication(name, &descriptor, &before, &bytes)? {
                Recovery::Recovered => self.observe(name.to_bytes_with_nul()),
                Recovery::Changing => Ok(Observation::Changing),
            };
        }
        if before.st_nlink != 1 {
            return Ok(Observation::Unsafe);
        }
        #[cfg(test)]
        if (name.to_bytes_with_nul() == PRIMARY && fault(FaultPoint::PrimaryRead))
            || (name.to_bytes_with_nul() == BACKUP && fault(FaultPoint::BackupRead))
        {
            return Ok(Observation::Io(io::Error::other(
                "injected identity read fault",
            )));
        }
        let bytes = match read_bounded(descriptor.as_raw_fd()) {
            Ok(bytes) => bytes,
            Err(source) => return Ok(Observation::Io(source)),
        };
        #[cfg(test)]
        run_boundary_hook(BoundaryPoint::RecordReadComplete);
        let after = match fstat(descriptor.as_raw_fd()) {
            Ok(metadata) => metadata,
            Err(source) => return Ok(Observation::Io(source)),
        };
        let path_metadata = match fstatat_nofollow(self.directory.as_raw_fd(), name) {
            Ok(metadata) => metadata,
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                return Ok(Observation::Unsafe);
            }
            Err(source) => return Ok(Observation::Io(source)),
        };
        if !validate_metadata_policy(&after, MetadataPolicy::Record { links: 1 }, self.root.uid)
            || !validate_metadata_policy(
                &path_metadata,
                MetadataPolicy::Record { links: 1 },
                self.root.uid,
            )
            || !same_file(&before, &after)
            || !same_file(&before, &path_metadata)
        {
            return Ok(Observation::Unsafe);
        }
        Ok(parse_record(bytes))
    }

    fn classify_open_failure(&self, name: &std::ffi::CStr, source: io::Error) -> Observation {
        match fstatat_nofollow(self.directory.as_raw_fd(), name) {
            Ok(metadata) if !record_metadata_has_allowed_links(&metadata, self.root.uid) => {
                Observation::Unsafe
            }
            Ok(_) => Observation::Io(source),
            Err(metadata_source) if matches!(metadata_source.raw_os_error(), Some(libc::ELOOP)) => {
                Observation::Unsafe
            }
            Err(_) => Observation::Io(source),
        }
    }

    fn publish(
        &self,
        destination: &'static [u8],
        bytes: &[u8],
    ) -> Result<Publication, IdentityError> {
        self.revalidate()?;
        let temporary = self.create_temporary()?;
        #[cfg(test)]
        if fault(FaultPoint::FileWrite) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        #[cfg(test)]
        if fault(FaultPoint::PartialWrite) {
            let partial = bytes.len() / 2;
            write_all(temporary.descriptor.as_raw_fd(), &bytes[..partial])?;
            return Err(injected_error(IdentityErrorClass::Io));
        }
        write_all(temporary.descriptor.as_raw_fd(), bytes)?;
        #[cfg(test)]
        if fault(FaultPoint::FileSync) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        fsync_fd(
            temporary.descriptor.as_raw_fd(),
            IdentityErrorClass::Durability,
        )?;
        let metadata = fstat(temporary.descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        if file_type(&metadata) != libc::S_IFREG
            || metadata.st_uid != self.root.uid
            || metadata.st_mode & 0o777 != 0o600
            || metadata.st_nlink != 1
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        self.revalidate()?;
        #[cfg(test)]
        if fault(FaultPoint::Publish) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        #[cfg(test)]
        if fault(FaultPoint::DestinationContention) {
            self.remove_current_temporary(&temporary)?;
            return Ok(Publication::Contended);
        }
        let result = unsafe {
            libc::linkat(
                self.directory.as_raw_fd(),
                temporary.name.as_ptr(),
                self.directory.as_raw_fd(),
                cstr(destination).as_ptr(),
                0,
            )
        };
        if result != 0 {
            let source = io::Error::last_os_error();
            if source.raw_os_error() == Some(libc::EEXIST) {
                self.remove_current_temporary(&temporary)?;
                return Ok(Publication::Existing);
            }
            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
        }
        #[cfg(test)]
        if fault(FaultPoint::PostLink) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        self.root.revalidate()?;
        self.revalidate()?;
        #[cfg(test)]
        if fault(FaultPoint::Unlink) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        self.remove_linked_temporary(destination, &temporary)?;
        #[cfg(test)]
        if fault(FaultPoint::DirectorySync) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)?;
        self.validate_committed_after_sync(cstr(destination), &temporary.descriptor, bytes)?;
        self.root.revalidate()?;
        self.revalidate()?;
        Ok(Publication::Published)
    }

    fn recover_interrupted_publication(
        &self,
        committed_name: &std::ffi::CStr,
        committed: &OwnedFd,
        committed_metadata: &libc::stat,
        committed_bytes: &[u8],
    ) -> Result<Recovery, IdentityError> {
        if !matches!(
            parse_record(committed_bytes.to_vec()),
            Observation::Valid(_)
        ) {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        let mut matching = Vec::new();
        for name in self.recognized_temporary_names()? {
            let descriptor = match open_owned_raw(
                self.directory.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            ) {
                Ok(descriptor) => descriptor,
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                    return Ok(Recovery::Changing);
                }
                Err(source) => match self.classify_open_failure(&name, source) {
                    Observation::Unsafe => {
                        return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                    }
                    Observation::Io(source) => {
                        return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                    }
                    _ => unreachable!("open failure classification is unsafe or I/O"),
                },
            };
            let metadata = fstat(descriptor.as_raw_fd())
                .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
            if !record_metadata_has_allowed_links(&metadata, self.root.uid) {
                if metadata.st_nlink == 0 {
                    match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
                        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => continue,
                        Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {}
                        Err(source) => {
                            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                        }
                        Ok(_) => {}
                    }
                }
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            if same_file(&metadata, committed_metadata) {
                if metadata.st_nlink == 1 {
                    match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
                        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                            return self.finish_concurrent_recovery(
                                committed_name,
                                committed,
                                committed_bytes,
                            );
                        }
                        Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {}
                        Err(source) => {
                            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                        }
                        Ok(_) => {}
                    }
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                let bytes = read_bounded(descriptor.as_raw_fd())
                    .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
                if metadata.st_nlink != 2 || bytes != committed_bytes {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                matching.push((name, descriptor, metadata));
            }
        }
        if matching.is_empty() {
            return self.finish_concurrent_recovery(committed_name, committed, committed_bytes);
        }
        if matching.len() != 1 {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        let (name, descriptor, metadata) = matching.pop().expect("one matching temporary");
        self.revalidate()?;
        #[cfg(test)]
        run_boundary_hook(BoundaryPoint::RecoveryUnlink);
        let path_metadata = match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
            Ok(metadata) => metadata,
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                return self.finish_concurrent_recovery(committed_name, committed, committed_bytes);
            }
            Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            Err(source) => {
                return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
            }
        };
        let descriptor_metadata = fstat(descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        if descriptor_metadata.st_nlink == 1 {
            match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                    return self.finish_concurrent_recovery(
                        committed_name,
                        committed,
                        committed_bytes,
                    );
                }
                Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                Err(source) => {
                    return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                }
                Ok(_) => {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
            }
        }
        let committed_path_metadata = fstatat_nofollow(self.directory.as_raw_fd(), committed_name)
            .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeRecord))?;
        if !validate_metadata_policy(
            &path_metadata,
            MetadataPolicy::Record { links: 2 },
            self.root.uid,
        ) || !validate_metadata_policy(
            &descriptor_metadata,
            MetadataPolicy::Record { links: 2 },
            self.root.uid,
        ) || !validate_metadata_policy(
            &committed_path_metadata,
            MetadataPolicy::Record { links: 2 },
            self.root.uid,
        ) || !same_file(&metadata, &path_metadata)
            || !same_file(&metadata, &descriptor_metadata)
            || !same_file(&metadata, &committed_path_metadata)
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        if unsafe { libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
            let source = io::Error::last_os_error();
            if source.raw_os_error() == Some(libc::ENOENT) {
                return self.finish_concurrent_recovery(committed_name, committed, committed_bytes);
            }
            return Err(IdentityError::with_source(
                IdentityErrorClass::Durability,
                source,
            ));
        }
        #[cfg(test)]
        if fault(FaultPoint::RecoverySync) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)?;
        self.validate_committed_after_sync(committed_name, committed, committed_bytes)?;
        Ok(Recovery::Recovered)
    }

    fn finish_concurrent_recovery(
        &self,
        committed_name: &std::ffi::CStr,
        committed: &OwnedFd,
        committed_bytes: &[u8],
    ) -> Result<Recovery, IdentityError> {
        let descriptor_metadata = fstat(committed.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let path_metadata = match fstatat_nofollow(self.directory.as_raw_fd(), committed_name) {
            Ok(metadata) => metadata,
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                return Ok(Recovery::Changing);
            }
            Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            Err(source) => {
                return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
            }
        };
        if !validate_metadata_policy(
            &descriptor_metadata,
            MetadataPolicy::Record { links: 1 },
            self.root.uid,
        ) || !validate_metadata_policy(
            &path_metadata,
            MetadataPolicy::Record { links: 1 },
            self.root.uid,
        ) || !same_file(&descriptor_metadata, &path_metadata)
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)?;
        self.validate_committed_after_sync(committed_name, committed, committed_bytes)?;
        Ok(Recovery::Recovered)
    }

    fn has_active_link(&self, committed: &libc::stat) -> Result<ActiveLink, IdentityError> {
        for name in self.recognized_temporary_names()? {
            let active = ActiveTemporary {
                directory_device: self.device,
                directory_inode: self.inode,
                name: name.to_bytes().to_vec(),
            };
            if !temporary_is_active(self.device, self.inode, &name) {
                continue;
            }
            let metadata = match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
                Ok(metadata) => metadata,
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                    return Ok(ActiveLink::Changing);
                }
                Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                Err(source) => {
                    return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                }
            };
            if same_file(committed, &metadata) {
                return Ok(ActiveLink::Present(active));
            }
        }
        Ok(ActiveLink::Absent)
    }

    fn cleanup_stale_temporaries(&self) -> Result<(), IdentityError> {
        let mut removed = false;
        let names = match self.recognized_temporary_names() {
            Ok(names) => names,
            Err(_) => {
                emit_cleanup_diagnostic();
                return Ok(());
            }
        };
        for name in names {
            if temporary_is_active(self.device, self.inode, &name) {
                continue;
            }
            if temporary_belongs_to_live_other_process(&name) {
                continue;
            }
            let descriptor = match open_owned_raw(
                self.directory.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            ) {
                Ok(descriptor) => descriptor,
                Err(source) => match self.classify_open_failure(&name, source) {
                    Observation::Unsafe => {
                        return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                    }
                    Observation::Io(_) => {
                        emit_cleanup_diagnostic();
                        return Ok(());
                    }
                    _ => unreachable!("open failure classification is unsafe or I/O"),
                },
            };
            let metadata = match fstat(descriptor.as_raw_fd()) {
                Ok(metadata) => metadata,
                Err(_) => {
                    emit_cleanup_diagnostic();
                    return Ok(());
                }
            };
            if !validate_metadata_policy(
                &metadata,
                MetadataPolicy::Record { links: 1 },
                self.root.uid,
            ) {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            if let Err(error) = self.revalidate() {
                if error.class == IdentityErrorClass::Io {
                    emit_cleanup_diagnostic();
                    return Ok(());
                }
                return Err(error);
            }
            let path_metadata = match fstatat_nofollow(self.directory.as_raw_fd(), &name) {
                Ok(metadata) => metadata,
                Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                Err(_) => {
                    emit_cleanup_diagnostic();
                    return Ok(());
                }
            };
            if !validate_metadata_policy(
                &path_metadata,
                MetadataPolicy::Record { links: 1 },
                self.root.uid,
            ) || !same_file(&metadata, &path_metadata)
            {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            #[cfg(test)]
            if fault(FaultPoint::Cleanup) {
                emit_cleanup_diagnostic();
                return Ok(());
            }
            if unsafe { libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
                emit_cleanup_diagnostic();
                return Ok(());
            }
            removed = true;
        }
        if removed && unsafe { libc::fsync(self.directory.as_raw_fd()) } != 0 {
            emit_cleanup_diagnostic();
        }
        Ok(())
    }

    fn recognized_temporary_names(&self) -> Result<Vec<CString>, IdentityError> {
        let duplicate = unsafe { libc::dup(self.directory.as_raw_fd()) };
        if duplicate < 0 {
            return Err(IdentityError::with_source(
                IdentityErrorClass::Io,
                io::Error::last_os_error(),
            ));
        }
        if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
            let source = io::Error::last_os_error();
            unsafe { libc::close(duplicate) };
            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            let source = io::Error::last_os_error();
            unsafe { libc::close(duplicate) };
            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
        }
        let stream = DirectoryStream(stream);
        let mut names = Vec::new();
        loop {
            set_errno_zero();
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                if current_errno() != 0 {
                    return Err(IdentityError::with_source(
                        IdentityErrorClass::Io,
                        io::Error::last_os_error(),
                    ));
                }
                break;
            }
            let raw_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            if recognized_temporary_pid(raw_name).is_some() {
                names.push(raw_name.to_owned());
            }
        }
        Ok(names)
    }

    fn create_temporary(&self) -> Result<TemporaryRecord, IdentityError> {
        for _ in 0..MAX_PASSES {
            let mut token = [0_u8; TEMP_TOKEN_BYTES];
            getrandom::fill(&mut token)
                .map_err(|_| IdentityError::classified(IdentityErrorClass::Io))?;
            let token: String = token.iter().map(|byte| format!("{byte:02x}")).collect();
            let name = CString::new(format!(".node.json.tmp-{}-{token}", std::process::id()))
                .expect("temporary identity name contains no NUL");
            let active = ActiveTemporary {
                directory_device: self.device,
                directory_inode: self.inode,
                name: name.as_bytes().to_vec(),
            };
            ACTIVE_TEMPORARIES
                .entries
                .lock()
                .expect("active identity temporary lock")
                .insert(active.clone(), TemporaryState::Creating);
            #[cfg(test)]
            run_boundary_hook(BoundaryPoint::TemporaryReserved);
            match open_owned_raw(
                self.directory.as_raw_fd(),
                &name,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            ) {
                Ok(descriptor) => {
                    let result = unsafe {
                        libc::fchmod(descriptor.as_raw_fd(), libc::mode_t::from(0o600_u16))
                    };
                    if result != 0 {
                        unregister_temporary(&active);
                        return Err(IdentityError::with_source(
                            IdentityErrorClass::Io,
                            io::Error::last_os_error(),
                        ));
                    }
                    ACTIVE_TEMPORARIES
                        .entries
                        .lock()
                        .expect("active identity temporary lock")
                        .insert(active.clone(), TemporaryState::Active);
                    return Ok(TemporaryRecord {
                        name,
                        descriptor,
                        active,
                    });
                }
                Err(source) if source.raw_os_error() == Some(libc::EEXIST) => {
                    unregister_temporary(&active);
                    continue;
                }
                Err(source) => {
                    unregister_temporary(&active);
                    return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
                }
            }
        }
        Err(IdentityError::classified(
            IdentityErrorClass::ConcurrentChange,
        ))
    }

    fn remove_current_temporary(&self, temporary: &TemporaryRecord) -> Result<(), IdentityError> {
        self.revalidate_temporary(temporary, 1)?;
        unlinkat(self.directory.as_raw_fd(), &temporary.name)?;
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)
    }

    fn remove_linked_temporary(
        &self,
        destination: &'static [u8],
        temporary: &TemporaryRecord,
    ) -> Result<(), IdentityError> {
        #[cfg(test)]
        run_boundary_hook(BoundaryPoint::PublicationUnlink);
        let descriptor = fstat(temporary.descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        match fstatat_nofollow(self.directory.as_raw_fd(), &temporary.name) {
            Ok(path) => {
                let committed = fstatat_nofollow(self.directory.as_raw_fd(), cstr(destination))
                    .map_err(|source| {
                        revalidation_error(source, IdentityErrorClass::UnsafeRecord)
                    })?;
                if !validate_metadata_policy(
                    &descriptor,
                    MetadataPolicy::Record { links: 2 },
                    self.root.uid,
                ) || !validate_metadata_policy(
                    &path,
                    MetadataPolicy::Record { links: 2 },
                    self.root.uid,
                ) || !validate_metadata_policy(
                    &committed,
                    MetadataPolicy::Record { links: 2 },
                    self.root.uid,
                ) || !same_file(&descriptor, &path)
                    || !same_file(&descriptor, &committed)
                {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                unlinkat(self.directory.as_raw_fd(), &temporary.name)
            }
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                let committed = fstatat_nofollow(self.directory.as_raw_fd(), cstr(destination))
                    .map_err(|source| {
                        revalidation_error(source, IdentityErrorClass::UnsafeRecord)
                    })?;
                if !same_file(&descriptor, &committed)
                    || descriptor.st_nlink != 1
                    || file_type(&descriptor) != libc::S_IFREG
                    || descriptor.st_uid != self.root.uid
                    || descriptor.st_mode & 0o777 != 0o600
                {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                Ok(())
            }
            Err(source) => Err(revalidation_error(source, IdentityErrorClass::UnsafeRecord)),
        }
    }

    fn revalidate_temporary(
        &self,
        temporary: &TemporaryRecord,
        links: libc::nlink_t,
    ) -> Result<(), IdentityError> {
        let descriptor = fstat(temporary.descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let path = fstatat_nofollow(self.directory.as_raw_fd(), &temporary.name)
            .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeRecord))?;
        if !same_file(&descriptor, &path)
            || file_type(&descriptor) != libc::S_IFREG
            || descriptor.st_uid != self.root.uid
            || descriptor.st_mode & 0o777 != 0o600
            || descriptor.st_nlink != links
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        Ok(())
    }

    fn validate_committed_after_sync(
        &self,
        name: &std::ffi::CStr,
        retained: &OwnedFd,
        expected_bytes: &[u8],
    ) -> Result<(), IdentityError> {
        let reopened = open_owned_raw(
            self.directory.as_raw_fd(),
            name,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
        .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeRecord))?;
        let retained_metadata = fstat(retained.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let before = fstat(reopened.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let bytes = read_bounded(reopened.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let after = fstat(reopened.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let path = fstatat_nofollow(self.directory.as_raw_fd(), name)
            .map_err(|source| revalidation_error(source, IdentityErrorClass::UnsafeRecord))?;
        if !validate_metadata_policy(
            &retained_metadata,
            MetadataPolicy::Record { links: 1 },
            self.root.uid,
        ) || !validate_metadata_policy(
            &before,
            MetadataPolicy::Record { links: 1 },
            self.root.uid,
        ) || !validate_metadata_policy(
            &after,
            MetadataPolicy::Record { links: 1 },
            self.root.uid,
        ) || !validate_metadata_policy(&path, MetadataPolicy::Record { links: 1 }, self.root.uid)
            || !same_file(&retained_metadata, &before)
            || !same_file(&retained_metadata, &after)
            || !same_file(&retained_metadata, &path)
            || bytes != expected_bytes
            || !matches!(parse_record(bytes), Observation::Valid(_))
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        Ok(())
    }
}

struct TemporaryRecord {
    name: CString,
    descriptor: OwnedFd,
    active: ActiveTemporary,
}

impl Drop for TemporaryRecord {
    fn drop(&mut self) {
        unregister_temporary(&self.active);
    }
}

fn temporary_is_active(
    directory_device: libc::dev_t,
    directory_inode: libc::ino_t,
    name: &std::ffi::CStr,
) -> bool {
    ACTIVE_TEMPORARIES
        .entries
        .lock()
        .expect("active identity temporary lock")
        .contains_key(&ActiveTemporary {
            directory_device,
            directory_inode,
            name: name.to_bytes().to_vec(),
        })
}

fn temporary_belongs_to_live_other_process(name: &std::ffi::CStr) -> bool {
    let Some(pid) = recognized_temporary_pid(name) else {
        return false;
    };
    if pid == unsafe { libc::getpid() } {
        return false;
    }
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn unregister_temporary(active: &ActiveTemporary) {
    ACTIVE_TEMPORARIES
        .entries
        .lock()
        .expect("active identity temporary lock")
        .remove(active);
    ACTIVE_TEMPORARIES.changed.notify_all();
}

fn wait_for_temporary_transition(active: &ActiveTemporary) {
    let entries = ACTIVE_TEMPORARIES
        .entries
        .lock()
        .expect("active identity temporary lock");
    if entries.contains_key(active) {
        let _guard = ACTIVE_TEMPORARIES
            .changed
            .wait_timeout_while(entries, std::time::Duration::from_secs(1), |entries| {
                entries.contains_key(active)
            })
            .expect("active identity temporary lock");
    }
}

fn recognized_temporary_pid(name: &std::ffi::CStr) -> Option<libc::pid_t> {
    let suffix = name.to_bytes().strip_prefix(TEMP_PREFIX)?;
    let separator = suffix.iter().position(|byte| *byte == b'-')?;
    let pid_bytes = &suffix[..separator];
    let token = &suffix[separator + 1..];
    if pid_bytes.is_empty()
        || !pid_bytes.iter().all(u8::is_ascii_digit)
        || token.len() != TEMP_TOKEN_HEX_LENGTH
        || !token
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return None;
    }
    let pid_text = std::str::from_utf8(pid_bytes).ok()?;
    let pid = pid_text.parse::<libc::pid_t>().ok()?;
    (pid > 0 && pid.to_string() == pid_text).then_some(pid)
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe { libc::closedir(self.0) };
    }
}

fn emit_cleanup_diagnostic() {
    #[cfg(test)]
    CLEANUP_DIAGNOSTIC.with(|observed| observed.set(true));
    tracing::warn!(
        event_code = "identity_temp_cleanup_failed",
        result = "identity_temp_cleanup_failed",
        "identity temporary cleanup failed"
    );
}

#[cfg(test)]
fn injected_error(class: IdentityErrorClass) -> IdentityError {
    IdentityError::with_source(class, io::Error::other("injected identity fault"))
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
fn set_errno_zero() {
    unsafe { *libc::__error() = 0 };
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__error() }
}

#[cfg(target_os = "linux")]
fn set_errno_zero() {
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(target_os = "linux")]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

fn parse_record(bytes: Vec<u8>) -> Observation {
    if bytes.len() > MAX_RECORD_BYTES {
        return Observation::Corrupt;
    }
    let envelope: IdentityEnvelopeV1 = match serde_json::from_slice(&bytes) {
        Ok(envelope) => envelope,
        Err(_) => return Observation::Corrupt,
    };
    if envelope.schema_version != 1 {
        return Observation::UnsupportedSchema;
    }
    let expected = match canonical_bytes(envelope.node_id) {
        Ok(expected) => expected,
        Err(_) => return Observation::Corrupt,
    };
    if expected != bytes {
        return Observation::Corrupt;
    }
    Observation::Valid(ValidRecord {
        node_id: envelope.node_id,
        bytes,
    })
}

fn canonical_bytes(node_id: NodeId) -> Result<Vec<u8>, IdentityError> {
    let mut bytes = serde_json::to_vec(&IdentityEnvelopeV1 {
        schema_version: 1,
        node_id,
    })
    .map_err(|_| IdentityError::classified(IdentityErrorClass::Corrupt))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn path_component(component: Option<&OsStr>) -> Option<CString> {
    CString::new(component?.as_bytes()).ok()
}

fn cstr(bytes: &[u8]) -> &std::ffi::CStr {
    std::ffi::CStr::from_bytes_with_nul(bytes).expect("static C string is valid")
}

fn open_owned(
    directory: RawFd,
    name: &std::ffi::CStr,
    flags: libc::c_int,
    class: IdentityErrorClass,
) -> Result<OwnedFd, IdentityError> {
    open_owned_raw(directory, name, flags)
        .map_err(|source| IdentityError::with_source(class, source))
}

fn open_owned_raw(
    directory: RawFd,
    name: &std::ffi::CStr,
    flags: libc::c_int,
) -> io::Result<OwnedFd> {
    let descriptor = unsafe { libc::openat(directory, name.as_ptr(), flags, 0o600) };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
    }
}

fn fstat(descriptor: RawFd) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { metadata.assume_init() })
    }
}

fn fstatat_nofollow(directory: RawFd, name: &std::ffi::CStr) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { metadata.assume_init() })
    }
}

fn file_type(metadata: &libc::stat) -> libc::mode_t {
    metadata.st_mode & libc::S_IFMT
}

fn record_metadata_has_allowed_links(metadata: &libc::stat, expected_uid: libc::uid_t) -> bool {
    matches!(metadata.st_nlink, 1 | 2)
        && file_type(metadata) == libc::S_IFREG
        && metadata.st_uid == expected_uid
        && metadata.st_mode & 0o777 == 0o600
}

fn same_file(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn revalidation_error(source: io::Error, unsafe_class: IdentityErrorClass) -> IdentityError {
    if matches!(
        source.raw_os_error(),
        Some(libc::ENOENT) | Some(libc::ELOOP) | Some(libc::ENOTDIR)
    ) {
        IdentityError::classified(unsafe_class)
    } else {
        IdentityError::with_source(IdentityErrorClass::Io, source)
    }
}

fn read_bounded(descriptor: RawFd) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0_u8; MAX_RECORD_BYTES + 1];
    let mut used = 0;
    loop {
        let result = unsafe {
            libc::read(
                descriptor,
                bytes[used..].as_mut_ptr().cast(),
                bytes.len() - used,
            )
        };
        if result < 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(source);
        }
        if result == 0 || used + result as usize == bytes.len() {
            used += result as usize;
            break;
        }
        used += result as usize;
    }
    bytes.truncate(used);
    Ok(bytes)
}

fn write_all(descriptor: RawFd, bytes: &[u8]) -> Result<(), IdentityError> {
    let mut written = 0;
    while written < bytes.len() {
        let result = unsafe {
            libc::write(
                descriptor,
                bytes[written..].as_ptr().cast(),
                bytes.len() - written,
            )
        };
        if result < 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(IdentityError::with_source(IdentityErrorClass::Io, source));
        }
        if result == 0 {
            return Err(IdentityError::classified(IdentityErrorClass::Io));
        }
        written += result as usize;
    }
    Ok(())
}

fn fsync_fd(descriptor: RawFd, class: IdentityErrorClass) -> Result<(), IdentityError> {
    if unsafe { libc::fsync(descriptor) } != 0 {
        Err(IdentityError::with_source(
            class,
            io::Error::last_os_error(),
        ))
    } else {
        Ok(())
    }
}

fn unlinkat(directory: RawFd, name: &std::ffi::CStr) -> Result<(), IdentityError> {
    if unsafe { libc::unlinkat(directory, name.as_ptr(), 0) } != 0 {
        Err(IdentityError::with_source(
            IdentityErrorClass::Durability,
            io::Error::last_os_error(),
        ))
    } else {
        Ok(())
    }
}
