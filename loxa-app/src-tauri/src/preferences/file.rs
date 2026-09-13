use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::MAX_PREFERENCES_BYTES;

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(1);
const PRIVATE_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteFault {
    None,
    BeforeRename,
    AfterRename,
}

#[derive(Clone, Debug)]
pub(super) struct WriteFailure {
    pub(super) outcome_unknown: bool,
    pub(super) context: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileIdentity {
    size: u64,
    device: u64,
    inode: u64,
    links: u64,
    owner: u32,
    mode: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl DirectoryIdentity {
    fn from_metadata(metadata: &Metadata, path: &Path) -> io::Result<Self> {
        if !metadata.file_type().is_dir() {
            return Err(unsafe_path(path, "expected a directory"));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata, path: &Path) -> io::Result<Self> {
        if !metadata.file_type().is_file() {
            return Err(unsafe_path(path, "expected a regular file"));
        }
        let identity = Self {
            size: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
            owner: metadata.uid(),
            mode: metadata.mode() & 0o777,
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        };
        identity.require_private(path)?;
        Ok(identity)
    }

    fn require_private(&self, path: &Path) -> io::Result<()> {
        if self.owner != unsafe { libc::geteuid() } || self.mode != PRIVATE_MODE || self.links != 1
        {
            return Err(unsafe_path(
                path,
                "expected a private user-owned 0600 single-link file",
            ));
        }
        Ok(())
    }

    fn same_installed_file(&self, installed: &Self) -> bool {
        self.size == installed.size
            && self.device == installed.device
            && self.inode == installed.inode
            && self.links == installed.links
            && self.owner == installed.owner
            && self.mode == installed.mode
            && self.modified_seconds == installed.modified_seconds
            && self.modified_nanoseconds == installed.modified_nanoseconds
    }
}

pub(super) fn read(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut opened = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let identity = file_identity(&opened, path)?;
    let mut bytes = Vec::with_capacity(MAX_PREFERENCES_BYTES.min(8 * 1024));
    Read::by_ref(&mut opened)
        .take(MAX_PREFERENCES_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_PREFERENCES_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop preferences exceed the byte limit",
        ));
    }
    ensure_file_matches_path(&opened, &identity, path)?;
    Ok(Some(bytes))
}

pub(super) fn replace(path: &Path, bytes: &[u8], fault: WriteFault) -> Result<(), WriteFailure> {
    if bytes.len() > MAX_PREFERENCES_BYTES {
        return Err(failure(
            false,
            "encoded desktop preferences exceed the byte limit",
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| failure(false, "desktop preferences have no parent directory"))?;
    let (directory, directory_identity) = open_directory(parent)
        .map_err(|error| io_failure(false, "open preferences directory", error))?;
    let destination = capture_destination(path)?;
    let (temporary_path, mut temporary) = create_temporary(parent, path)?;
    let written = (|| {
        temporary
            .write_all(bytes)
            .map_err(|error| io_failure(false, "write desktop preferences", error))?;
        durable_file_sync(&temporary)
            .map_err(|error| io_failure(false, "sync desktop preferences", error))?;
        let identity = file_identity(&temporary, &temporary_path)
            .map_err(|error| io_failure(false, "validate temporary preferences", error))?;
        ensure_file_matches_path(&temporary, &identity, &temporary_path)
            .map_err(|error| io_failure(false, "revalidate temporary preferences", error))?;
        validate_before_rename(
            &directory,
            &directory_identity,
            parent,
            path,
            destination.as_ref(),
        )?;
        if fault == WriteFault::BeforeRename {
            return Err(failure(
                false,
                "injected desktop preferences failure before rename",
            ));
        }
        fs::rename(&temporary_path, path)
            .map_err(|error| io_failure(false, "replace desktop preferences", error))?;
        let installed = path_identity(path)
            .map_err(|error| io_failure(true, "validate replaced preferences", error))?;
        if !identity.same_installed_file(&installed) {
            return Err(failure(
                true,
                "replaced desktop preferences identity changed after rename",
            ));
        }
        if fault == WriteFault::AfterRename {
            return Err(failure(
                true,
                "injected desktop preferences failure after rename",
            ));
        }
        sync_directory(&directory, &directory_identity, parent)
            .map_err(|error| io_failure(true, "sync preferences directory", error))?;
        ensure_file_matches_path(&temporary, &installed, path)
            .map_err(|error| io_failure(true, "revalidate replaced preferences", error))
    })();
    if written.as_ref().is_err_and(|error| !error.outcome_unknown) {
        if file_identity(&temporary, &temporary_path)
            .and_then(|identity| ensure_file_matches_path(&temporary, &identity, &temporary_path))
            .is_ok()
        {
            let _ = fs::remove_file(&temporary_path);
        }
    }
    written
}

pub(super) fn reconcile(path: &Path, expected: &[u8]) -> Result<(), WriteFailure> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut opened = options
        .open(path)
        .map_err(|error| io_failure(true, "open uncertain preferences", error))?;
    let identity = file_identity(&opened, path)
        .map_err(|error| io_failure(true, "validate uncertain preferences", error))?;
    let mut bytes = Vec::with_capacity(expected.len().min(MAX_PREFERENCES_BYTES));
    Read::by_ref(&mut opened)
        .take(MAX_PREFERENCES_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_failure(true, "read uncertain preferences", error))?;
    if bytes.len() > MAX_PREFERENCES_BYTES || bytes != expected {
        return Err(failure(
            true,
            "uncertain preferences do not match the retained candidate",
        ));
    }
    ensure_file_matches_path(&opened, &identity, path)
        .map_err(|error| io_failure(true, "revalidate uncertain preferences", error))?;
    durable_file_sync(&opened)
        .map_err(|error| io_failure(true, "sync uncertain preferences", error))?;
    ensure_file_matches_path(&opened, &identity, path)
        .map_err(|error| io_failure(true, "revalidate uncertain preferences", error))?;
    let parent = path
        .parent()
        .ok_or_else(|| failure(true, "desktop preferences have no parent directory"))?;
    let (directory, directory_identity) = open_directory(parent)
        .map_err(|error| io_failure(true, "open preferences directory", error))?;
    sync_directory(&directory, &directory_identity, parent)
        .map_err(|error| io_failure(true, "sync preferences directory", error))?;
    ensure_file_matches_path(&opened, &identity, path)
        .map_err(|error| io_failure(true, "revalidate uncertain preferences", error))
}

fn capture_destination(path: &Path) -> Result<Option<FileIdentity>, WriteFailure> {
    match path_identity(path) {
        Ok(identity) => Ok(Some(identity)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_failure(
            false,
            "validate current desktop preferences",
            error,
        )),
    }
}

fn validate_before_rename(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    parent: &Path,
    path: &Path,
    expected: Option<&FileIdentity>,
) -> Result<(), WriteFailure> {
    ensure_directory_matches_path(directory, directory_identity, parent)
        .map_err(|error| io_failure(false, "revalidate preferences directory", error))?;
    match (expected, path_identity(path)) {
        (Some(expected), Ok(current)) if expected == &current => Ok(()),
        (None, Err(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        _ => Err(failure(
            false,
            "desktop preferences changed before durable replace",
        )),
    }
}

fn create_temporary(parent: &Path, path: &Path) -> Result<(PathBuf, File), WriteFailure> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| failure(false, "desktop preferences filename is invalid"))?;
    for _ in 0..16 {
        let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), sequence));
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .mode(PRIVATE_MODE)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        match options.open(&temporary) {
            Ok(file) => {
                file.set_permissions(fs::Permissions::from_mode(PRIVATE_MODE))
                    .map_err(|error| io_failure(false, "secure temporary preferences", error))?;
                return Ok((temporary, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(io_failure(
                    false,
                    "create temporary desktop preferences",
                    error,
                ))
            }
        }
    }
    Err(failure(
        false,
        "temporary preferences name capacity is exhausted",
    ))
}

fn open_directory(path: &Path) -> io::Result<(File, DirectoryIdentity)> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let directory = options.open(path)?;
    let identity = DirectoryIdentity::from_metadata(&directory.metadata()?, path)?;
    ensure_directory_matches_path(&directory, &identity, path)?;
    Ok((directory, identity))
}

fn ensure_directory_matches_path(
    directory: &File,
    expected: &DirectoryIdentity,
    path: &Path,
) -> io::Result<()> {
    let descriptor = DirectoryIdentity::from_metadata(&directory.metadata()?, path)?;
    let resolved = DirectoryIdentity::from_metadata(&fs::symlink_metadata(path)?, path)?;
    if &descriptor != expected || &resolved != expected {
        return Err(changed(path, "directory"));
    }
    Ok(())
}

fn file_identity(file: &File, path: &Path) -> io::Result<FileIdentity> {
    FileIdentity::from_metadata(&file.metadata()?, path)
}

fn path_identity(path: &Path) -> io::Result<FileIdentity> {
    FileIdentity::from_metadata(&fs::symlink_metadata(path)?, path)
}

fn ensure_file_matches_path(file: &File, expected: &FileIdentity, path: &Path) -> io::Result<()> {
    let descriptor = file_identity(file, path)?;
    let resolved = path_identity(path)?;
    if &descriptor != expected || &resolved != expected {
        return Err(changed(path, "file"));
    }
    Ok(())
}

fn sync_directory(directory: &File, identity: &DirectoryIdentity, path: &Path) -> io::Result<()> {
    ensure_directory_matches_path(directory, identity, path)?;
    directory.sync_all()?;
    ensure_directory_matches_path(directory, identity, path)
}

fn durable_file_sync(file: &File) -> io::Result<()> {
    file.sync_all()
}

fn changed(path: &Path, kind: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{} {kind} changed while open", path.display()),
    )
}

fn unsafe_path(path: &Path, context: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: {context}", path.display()),
    )
}

fn io_failure(outcome_unknown: bool, action: &str, error: io::Error) -> WriteFailure {
    failure(outcome_unknown, format!("{action} failed: {error}"))
}

fn failure(outcome_unknown: bool, context: impl Into<String>) -> WriteFailure {
    WriteFailure {
        outcome_unknown,
        context: context.into(),
    }
}
