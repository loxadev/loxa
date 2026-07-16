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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex as StandardMutex};

const IDENTITY_DIRECTORY: &[u8] = b"identity\0";
const PRIMARY: &[u8] = b"node.json\0";
const BACKUP: &[u8] = b"node.json.bak\0";
const MAX_RECORD_BYTES: usize = 4096;
const MAX_PASSES: usize = 3;
const TEMP_PREFIX: &[u8] = b".node.json.tmp-";
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
static ACTIVE_TEMPORARIES: LazyLock<StandardMutex<std::collections::HashSet<ActiveTemporary>>> =
    LazyLock::new(|| StandardMutex::new(std::collections::HashSet::new()));

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ActiveTemporary {
    directory_device: libc::dev_t,
    directory_inode: libc::ino_t,
    name: Vec<u8>,
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
}

#[cfg(test)]
thread_local! {
    static FAULT: RefCell<Option<FaultPoint>> = const { RefCell::new(None) };
    static CLEANUP_DIAGNOSTIC: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(super) fn inject_fault(point: FaultPoint) {
    FAULT.with(|fault| *fault.borrow_mut() = Some(point));
    CLEANUP_DIAGNOSTIC.with(|observed| observed.set(false));
}

#[cfg(test)]
pub(super) fn cleanup_diagnostic_observed() -> bool {
    CLEANUP_DIAGNOSTIC.with(Cell::get)
}

#[cfg(test)]
fn fault(point: FaultPoint) -> bool {
    FAULT.with(|fault| {
        let mut fault = fault.borrow_mut();
        if fault.as_ref() == Some(&point) {
            *fault = None;
            true
        } else {
            false
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
    Contended,
}

pub(super) fn open_or_create(loxa_root: &Path) -> Result<NodeId, IdentityError> {
    let root = RootOwner::open(loxa_root)?;
    let identity = root.open_identity_directory()?;

    for pass in 0..MAX_PASSES {
        #[cfg(test)]
        if pass > 0 && fault(FaultPoint::Reopen) {
            return Err(injected_error(IdentityErrorClass::Io));
        }
        let primary = identity.observe(PRIMARY)?;
        let backup = identity.observe(BACKUP)?;

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
                if identity.publish(PRIMARY, &bytes)? == Publication::Contended {
                    if pass + 1 == MAX_PASSES {
                        return Err(IdentityError::classified(
                            IdentityErrorClass::ConcurrentChange,
                        ));
                    }
                    continue;
                }
                if identity.publish(BACKUP, &bytes)? == Publication::Contended {
                    if pass + 1 == MAX_PASSES {
                        return Err(IdentityError::classified(
                            IdentityErrorClass::ConcurrentChange,
                        ));
                    }
                    continue;
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
        let primary = identity.observe(PRIMARY)?;
        let backup = identity.observe(BACKUP)?;
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
        if file_type(&metadata) != libc::S_IFDIR
            || metadata.st_uid != uid
            || metadata.st_mode & 0o022 != 0
        {
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
        let metadata = fstatat_nofollow(self.parent.as_raw_fd(), &self.root_name)
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRoot))?;
        if file_type(&metadata) != libc::S_IFDIR
            || metadata.st_dev != self.device
            || metadata.st_ino != self.inode
            || metadata.st_uid != self.uid
            || metadata.st_mode & 0o022 != 0
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
        if file_type(&metadata) != libc::S_IFDIR
            || metadata.st_uid != self.uid
            || metadata.st_mode & 0o777 != 0o700
        {
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
    fn revalidate(&self) -> Result<(), IdentityError> {
        self.root.revalidate()?;
        let metadata = fstatat_nofollow(self.root.root.as_raw_fd(), cstr(IDENTITY_DIRECTORY))
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeDirectory))?;
        if file_type(&metadata) != libc::S_IFDIR
            || metadata.st_dev != self.device
            || metadata.st_ino != self.inode
            || metadata.st_uid != self.root.uid
            || metadata.st_mode & 0o777 != 0o700
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
            Err(source) => return Ok(Observation::Io(source)),
        };
        let before = match fstat(descriptor.as_raw_fd()) {
            Ok(metadata) => metadata,
            Err(source) => return Ok(Observation::Io(source)),
        };
        if file_type(&before) != libc::S_IFREG
            || before.st_uid != self.root.uid
            || before.st_mode & 0o777 != 0o600
        {
            return Ok(Observation::Unsafe);
        }
        if before.st_nlink == 2 {
            if self.has_active_link(&before)? {
                for _ in 0..64 {
                    std::thread::yield_now();
                    let metadata = fstat(descriptor.as_raw_fd()).map_err(|source| {
                        IdentityError::with_source(IdentityErrorClass::Io, source)
                    })?;
                    if metadata.st_nlink == 1 {
                        return self.observe(name.to_bytes_with_nul());
                    }
                }
                return Ok(Observation::Changing);
            }
            let bytes = match read_bounded(descriptor.as_raw_fd()) {
                Ok(bytes) => bytes,
                Err(source) => return Ok(Observation::Io(source)),
            };
            self.recover_interrupted_publication(name, &descriptor, &before, &bytes)?;
            return self.observe(name.to_bytes_with_nul());
        }
        if before.st_nlink != 1 {
            return Ok(Observation::Unsafe);
        }
        let bytes = match read_bounded(descriptor.as_raw_fd()) {
            Ok(bytes) => bytes,
            Err(source) => return Ok(Observation::Io(source)),
        };
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
        if !same_file(&before, &after) || !same_file(&before, &path_metadata) {
            return Ok(Observation::Unsafe);
        }
        Ok(parse_record(bytes))
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
                return Ok(Publication::Contended);
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
        self.remove_linked_temporary(&temporary)?;
        #[cfg(test)]
        if fault(FaultPoint::DirectorySync) {
            return Err(injected_error(IdentityErrorClass::Durability));
        }
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)?;
        self.root.revalidate()?;
        self.revalidate()?;
        Ok(Publication::Published)
    }

    fn recover_interrupted_publication(
        &self,
        _committed_name: &std::ffi::CStr,
        _committed: &OwnedFd,
        committed_metadata: &libc::stat,
        committed_bytes: &[u8],
    ) -> Result<(), IdentityError> {
        let mut matching = Vec::new();
        for name in self.recognized_temporary_names()? {
            let descriptor = open_owned_raw(
                self.directory.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
            let metadata = fstat(descriptor.as_raw_fd())
                .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
            if file_type(&metadata) != libc::S_IFREG
                || metadata.st_uid != self.root.uid
                || metadata.st_mode & 0o777 != 0o600
            {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            if same_file(&metadata, committed_metadata) {
                let bytes = read_bounded(descriptor.as_raw_fd())
                    .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
                if metadata.st_nlink != 2 || bytes != committed_bytes {
                    return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
                }
                matching.push((name, descriptor, metadata));
            }
        }
        if matching.len() != 1 {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        let (name, descriptor, metadata) = matching.pop().expect("one matching temporary");
        self.revalidate()?;
        let path_metadata = fstatat_nofollow(self.directory.as_raw_fd(), &name)
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
        let descriptor_metadata = fstat(descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        if !same_file(&metadata, &path_metadata)
            || !same_file(&metadata, &descriptor_metadata)
            || descriptor_metadata.st_nlink != 2
        {
            return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
        }
        unlinkat(self.directory.as_raw_fd(), &name)?;
        fsync_fd(self.directory.as_raw_fd(), IdentityErrorClass::Durability)?;
        Ok(())
    }

    fn has_active_link(&self, committed: &libc::stat) -> Result<bool, IdentityError> {
        for name in self.recognized_temporary_names()? {
            if !temporary_is_active(self.device, self.inode, &name) {
                continue;
            }
            let metadata = fstatat_nofollow(self.directory.as_raw_fd(), &name)
                .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
            if same_file(committed, &metadata) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cleanup_stale_temporaries(&self) -> Result<(), IdentityError> {
        let mut removed = false;
        for name in self.recognized_temporary_names()? {
            if temporary_is_active(self.device, self.inode, &name) {
                continue;
            }
            let descriptor = open_owned_raw(
                self.directory.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
            let metadata = fstat(descriptor.as_raw_fd())
                .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
            if file_type(&metadata) != libc::S_IFREG
                || metadata.st_uid != self.root.uid
                || metadata.st_mode & 0o777 != 0o600
                || metadata.st_nlink != 1
            {
                return Err(IdentityError::classified(IdentityErrorClass::UnsafeRecord));
            }
            self.revalidate()?;
            let path_metadata = fstatat_nofollow(self.directory.as_raw_fd(), &name)
                .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
            if !same_file(&metadata, &path_metadata) {
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
            if raw_name.to_bytes().starts_with(TEMP_PREFIX) {
                names.push(raw_name.to_owned());
            }
        }
        Ok(names)
    }

    fn create_temporary(&self) -> Result<TemporaryRecord, IdentityError> {
        for _ in 0..64 {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let name = CString::new(format!(
                ".node.json.tmp-{}-{sequence:016x}",
                std::process::id()
            ))
            .expect("temporary identity name contains no NUL");
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
                        return Err(IdentityError::with_source(
                            IdentityErrorClass::Io,
                            io::Error::last_os_error(),
                        ));
                    }
                    let active = ActiveTemporary {
                        directory_device: self.device,
                        directory_inode: self.inode,
                        name: name.as_bytes().to_vec(),
                    };
                    ACTIVE_TEMPORARIES
                        .lock()
                        .expect("active identity temporary lock")
                        .insert(active.clone());
                    return Ok(TemporaryRecord {
                        name,
                        descriptor,
                        active,
                    });
                }
                Err(source) if source.raw_os_error() == Some(libc::EEXIST) => continue,
                Err(source) => {
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

    fn remove_linked_temporary(&self, temporary: &TemporaryRecord) -> Result<(), IdentityError> {
        self.revalidate_temporary(temporary, 2)?;
        unlinkat(self.directory.as_raw_fd(), &temporary.name)
    }

    fn revalidate_temporary(
        &self,
        temporary: &TemporaryRecord,
        links: libc::nlink_t,
    ) -> Result<(), IdentityError> {
        let descriptor = fstat(temporary.descriptor.as_raw_fd())
            .map_err(|source| IdentityError::with_source(IdentityErrorClass::Io, source))?;
        let path = fstatat_nofollow(self.directory.as_raw_fd(), &temporary.name)
            .map_err(|_| IdentityError::classified(IdentityErrorClass::UnsafeRecord))?;
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
}

struct TemporaryRecord {
    name: CString,
    descriptor: OwnedFd,
    active: ActiveTemporary,
}

impl Drop for TemporaryRecord {
    fn drop(&mut self) {
        ACTIVE_TEMPORARIES
            .lock()
            .expect("active identity temporary lock")
            .remove(&self.active);
    }
}

fn temporary_is_active(
    directory_device: libc::dev_t,
    directory_inode: libc::ino_t,
    name: &std::ffi::CStr,
) -> bool {
    ACTIVE_TEMPORARIES
        .lock()
        .expect("active identity temporary lock")
        .contains(&ActiveTemporary {
            directory_device,
            directory_inode,
            name: name.to_bytes().to_vec(),
        })
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

fn same_file(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
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
