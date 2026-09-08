//! Retained model-directory and lock-entry authority through guard destruction.
use super::ensure_catalog_directory;
use std::fs::{self, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ModelLockError {
    Missing,
    Busy,
    UnsafeLocalState,
}

struct ModelLockFailure {
    kind: ModelLockError,
    legacy: String,
}

impl ModelLockFailure {
    fn io(path: &Path, error: io::Error, missing_is_missing: bool) -> Self {
        Self {
            kind: if missing_is_missing && error.kind() == io::ErrorKind::NotFound {
                ModelLockError::Missing
            } else {
                ModelLockError::UnsafeLocalState
            },
            legacy: format!("{}: {error}", path.display()),
        }
    }

    fn unsafe_legacy(legacy: String) -> Self {
        Self {
            kind: ModelLockError::UnsafeLocalState,
            legacy,
        }
    }

    fn busy() -> Self {
        Self {
            kind: ModelLockError::Busy,
            legacy: "model is busy in another Loxa command".into(),
        }
    }
}

pub struct ModelLock {
    lock_file: fs::File,
    lock_identity: crate::safe_file::RegularFileIdentity,
    lock_path: PathBuf,
    model_directory: fs::File,
    model_directory_identity: crate::safe_file::DirectoryIdentity,
    model_directory_path: PathBuf,
    acquiring_process_id: u32,
}

impl ModelLock {
    pub fn acquire(model_dir: &Path) -> Result<Self, String> {
        Self::acquire_with_after_open(model_dir, || {})
    }

    pub(crate) fn acquire_existing(model_dir: &Path) -> Result<Self, ModelLockError> {
        Self::acquire_inner(model_dir, false, || {}).map_err(|failure| failure.kind)
    }

    pub(crate) fn acquire_for_transfer(model_dir: &Path) -> Result<Self, ModelLockError> {
        Self::acquire_inner(model_dir, true, || {}).map_err(|failure| failure.kind)
    }

    pub(crate) fn model_directory(&self) -> &std::fs::File {
        &self.model_directory
    }

    fn acquire_with_after_open(
        model_dir: &Path,
        after_open: impl FnOnce(),
    ) -> Result<Self, String> {
        Self::acquire_inner(model_dir, true, after_open).map_err(|failure| failure.legacy)
    }

    fn acquire_inner(
        model_dir: &Path,
        create: bool,
        after_open: impl FnOnce(),
    ) -> Result<Self, ModelLockFailure> {
        if create {
            ensure_catalog_directory(model_dir).map_err(ModelLockFailure::unsafe_legacy)?;
        }
        let (model_directory, model_directory_identity) =
            crate::safe_file::open_directory(model_dir)
                .map_err(|error| ModelLockFailure::io(model_dir, error, !create))?;
        let path = model_dir.join(".lock");
        let file = open_model_lock_entry(&model_directory, &path, create)
            .map_err(|error| ModelLockFailure::io(&path, error, !create))?;
        let lock_identity = crate::safe_file::regular_file_identity(&file, &path)
            .map_err(|error| ModelLockFailure::io(&path, error, false))?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => ModelLockFailure::busy(),
            TryLockError::Error(error) => ModelLockFailure::io(&path, error, false),
        })?;
        after_open();
        let lock = Self {
            lock_file: file,
            lock_identity,
            lock_path: path,
            model_directory,
            model_directory_identity,
            model_directory_path: model_dir.to_owned(),
            acquiring_process_id: std::process::id(),
        };
        lock.revalidate().map_err(ModelLockFailure::unsafe_legacy)?;
        Ok(lock)
    }

    pub(crate) fn revalidate(&self) -> Result<(), String> {
        self.revalidate_model_directory()?;
        self.revalidate_lock_entry()
    }

    pub(crate) fn revalidate_for(&self, model_dir: &Path) -> Result<(), String> {
        if self.model_directory_path != model_dir {
            return Err("model lock directory mismatch".into());
        }
        self.revalidate()
    }

    fn revalidate_lock_entry(&self) -> Result<(), String> {
        let resolved = open_model_lock_entry(&self.model_directory, &self.lock_path, false)
            .map_err(|error| format!("{}: {error}", self.lock_path.display()))?;
        crate::safe_file::ensure_regular_descriptors_match(
            &self.lock_file,
            &self.lock_identity,
            &resolved,
            &self.lock_path,
        )
        .map_err(|error| format!("{}: {error}", self.lock_path.display()))
    }

    fn revalidate_model_directory(&self) -> Result<(), String> {
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &self.model_directory,
            &self.model_directory_identity,
            &self.model_directory_path,
        )
        .map_err(|error| format!("{}: {error}", self.model_directory_path.display()))
    }
}

impl Drop for ModelLock {
    fn drop(&mut self) {
        // A concurrent fork can retain the locked file description until exec.
        // Only the acquiring process may unlock; an inherited guard must not
        // release the parent's lock when dropped in a child.
        if self.acquiring_process_id == std::process::id() {
            let _ = self.lock_file.unlock();
        }
    }
}

#[cfg(unix)]
fn open_model_lock_entry(directory: &fs::File, _path: &Path, create: bool) -> io::Result<fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_RDWR;
    if create {
        flags |= libc::O_CREAT;
    }
    // SAFETY: `directory` supplies a live directory descriptor, the entry name is a
    // fixed NUL-terminated byte string, and a successful descriptor is owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c".lock".as_ptr(),
            flags,
            0o600 as libc::c_uint,
        )
    };
    if descriptor == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `openat` returned a new owned descriptor which has not been wrapped.
    Ok(unsafe { fs::File::from_raw_fd(descriptor) })
}

#[cfg(not(unix))]
fn open_model_lock_entry(_directory: &fs::File, path: &Path, create: bool) -> io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options
        .create(create)
        .truncate(false)
        .read(true)
        .write(true);
    options.open(path)
}

pub(crate) fn model_is_busy(model_dir: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(model_dir) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(format!("unsafe model directory {}", model_dir.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("{}: {error}", model_dir.display())),
    }

    let path = model_dir.join(".lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe model lock {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe model lock {}", path.display()));
        }
    }
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => Err(format!("{}: {error}", path.display())),
    }
}

#[cfg(test)]
mod tests;
