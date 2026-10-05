use std::fs::{File, OpenOptions, TryLockError};
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;

#[cfg(unix)]
mod local;
#[cfg(all(test, unix))]
pub(super) use local::LOCAL_FOREGROUND_LOCK_OPERATIONS;
#[cfg(unix)]
pub(super) use local::{lock_local_foreground_operation, LocalForegroundLock};

#[cfg(unix)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ForegroundLockProtocol {
    OpenFileDescription,
    Traditional,
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum ForegroundLockMode {
    Automatic,
    OpenFileDescriptionOnly,
    #[cfg(test)]
    TraditionalOnly,
}

pub(super) fn foreground_lock_is_held(path: &Path) -> Result<bool, String> {
    foreground_lock_is_held_with_after_open(path, || {})
}

pub(super) fn foreground_lock_is_held_with_after_open(
    path: &Path,
    after_open: impl FnOnce(),
) -> Result<bool, String> {
    #[cfg(unix)]
    // Keep a traditional-lock query descriptor's whole lifetime ordered with a
    // same-process fallback acquisition. Otherwise a query opened just before
    // acquisition could close just after it and release that record lock.
    let _operation = lock_local_foreground_operation()?;
    #[cfg(unix)]
    if LocalForegroundLock::is_held(path)? {
        return Ok(true);
    }

    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe runtime lock {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe runtime lock {}", path.display()));
        }
    }
    after_open();
    foreground_lock_is_held_from_file(&file)
}

#[cfg(unix)]
fn foreground_lock_is_held_from_file(file: &File) -> Result<bool, String> {
    #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
    match foreground_lock_is_held_with_command(file, libc::F_OFD_GETLK) {
        Ok(held) => return Ok(held),
        Err(error) if ofd_lock_command_is_unsupported(&error) => {}
        Err(error) => return Err(error.to_string()),
    }

    foreground_lock_is_held_with_command(file, libc::F_GETLK).map_err(|error| error.to_string())
}

#[cfg(not(unix))]
fn foreground_lock_is_held_from_file(_file: &File) -> Result<bool, String> {
    Err("foreground lock observation is unsupported on this platform".into())
}

#[cfg(all(test, unix))]
pub(super) fn try_acquire_foreground_lock(
    file: &File,
) -> Result<ForegroundLockProtocol, TryLockError> {
    try_acquire_foreground_lock_with_mode(file, ForegroundLockMode::Automatic)
}

#[cfg(unix)]
fn try_acquire_foreground_lock_with_mode(
    file: &File,
    mode: ForegroundLockMode,
) -> Result<ForegroundLockProtocol, TryLockError> {
    let use_ofd = match mode {
        ForegroundLockMode::Automatic | ForegroundLockMode::OpenFileDescriptionOnly => true,
        #[cfg(test)]
        ForegroundLockMode::TraditionalOnly => false,
    };
    if use_ofd {
        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
        match set_foreground_lock_with_command(file, libc::F_OFD_SETLK) {
            Ok(()) => return Ok(ForegroundLockProtocol::OpenFileDescription),
            Err(error)
                if ofd_lock_command_is_unsupported(&error)
                    && matches!(mode, ForegroundLockMode::Automatic) => {}
            Err(error) if ofd_lock_command_is_unsupported(&error) => {
                return Err(TryLockError::Error(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "service runtime requires an open-file-description foreground lock",
                )));
            }
            Err(error) => return Err(as_try_lock_error(error)),
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
        if matches!(mode, ForegroundLockMode::OpenFileDescriptionOnly) {
            return Err(TryLockError::Error(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "service runtime requires an open-file-description foreground lock",
            )));
        }
    }

    set_foreground_lock_with_command(file, libc::F_SETLK)
        .map(|()| ForegroundLockProtocol::Traditional)
        .map_err(as_try_lock_error)
}

#[cfg(not(unix))]
pub(super) fn try_acquire_foreground_lock(file: &File) -> Result<(), TryLockError> {
    file.try_lock()
}

#[cfg(unix)]
fn foreground_record_lock() -> libc::flock {
    // SAFETY: all fields are initialized below before use by `fcntl`.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    lock.l_start = 0;
    lock.l_len = 0;
    lock
}

#[cfg(unix)]
fn foreground_lock_is_held_with_command(
    file: &File,
    command: libc::c_int,
) -> std::io::Result<bool> {
    let mut lock = foreground_record_lock();
    // SAFETY: `lock` is a valid writable `libc::flock` with a whole-file write
    // range, and `file` remains open for the duration of this query.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), command, &mut lock) };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(lock.l_type != libc::F_UNLCK as libc::c_short)
    }
}

#[cfg(unix)]
pub(super) fn set_foreground_lock_with_command(
    file: &File,
    command: libc::c_int,
) -> std::io::Result<()> {
    let mut lock = foreground_record_lock();
    // SAFETY: `lock` is a valid writable `libc::flock` with a whole-file write
    // range, and `file` remains open while the process owns the record lock.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), command, &mut lock) };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn as_try_lock_error(error: std::io::Error) -> TryLockError {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        TryLockError::WouldBlock
    } else {
        TryLockError::Error(error)
    }
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
fn ofd_lock_command_is_unsupported(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EINVAL || code == libc::ENOTSUP
    ) || error.kind() == std::io::ErrorKind::Unsupported
}

pub(super) enum ForegroundLockAcquireError {
    WouldBlock,
    Error(String),
}

#[cfg(test)]
struct ForegroundLockTestHook {
    after_file_close: Option<Box<dyn FnOnce() + Send>>,
}

#[cfg(not(test))]
struct ForegroundLockTestHook;

impl ForegroundLockTestHook {
    fn none() -> Self {
        #[cfg(test)]
        {
            Self {
                after_file_close: None,
            }
        }
        #[cfg(not(test))]
        {
            Self
        }
    }

    #[cfg(test)]
    fn after_file_close(after_file_close: impl FnOnce() + Send + 'static) -> Self {
        Self {
            after_file_close: Some(Box::new(after_file_close)),
        }
    }

    fn run_after_file_close(&mut self) {
        #[cfg(test)]
        if let Some(after_file_close) = self.after_file_close.take() {
            after_file_close();
        }
    }
}

pub(super) struct ForegroundLock {
    file: Option<File>,
    #[cfg(unix)]
    protocol: ForegroundLockProtocol,
    #[cfg(unix)]
    local_traditional_lock: Option<LocalForegroundLock>,
    test_hook: ForegroundLockTestHook,
}

impl ForegroundLock {
    #[cfg(unix)]
    pub(super) fn acquire_service_at(
        directory: &File,
        path: &Path,
    ) -> Result<Self, ForegroundLockAcquireError> {
        use std::os::fd::FromRawFd as _;
        use std::os::unix::fs::MetadataExt as _;

        let local_lock =
            LocalForegroundLock::reserve(path).map_err(map_local_foreground_lock_error)?;
        // SAFETY: the retained directory is live and the static filename is NUL terminated.
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                c"foreground.lock".as_ptr(),
                libc::O_CREAT
                    | libc::O_RDWR
                    | libc::O_NOFOLLOW
                    | libc::O_CLOEXEC
                    | libc::O_NONBLOCK,
                0o600,
            )
        };
        if descriptor == -1 {
            return Err(ForegroundLockAcquireError::Error(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        // SAFETY: openat returned a newly owned descriptor.
        let file = unsafe { File::from_raw_fd(descriptor) };
        validate_lock_file(&file, path).map_err(ForegroundLockAcquireError::Error)?;
        let opened = file
            .metadata()
            .map_err(|error| ForegroundLockAcquireError::Error(error.to_string()))?;
        // SAFETY: geteuid has no preconditions.
        if opened.uid() != unsafe { libc::geteuid() } || opened.mode() & 0o022 != 0 {
            return Err(ForegroundLockAcquireError::Error(
                "unsafe service runtime lock".into(),
            ));
        }
        let lock = Self::finish_acquisition(
            path,
            file,
            local_lock,
            ForegroundLockMode::OpenFileDescriptionOnly,
            ForegroundLockTestHook::none(),
        )?;
        let mut current = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the retained directory and output storage remain live.
        if unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                c"foreground.lock".as_ptr(),
                current.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(ForegroundLockAcquireError::Error(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        // SAFETY: fstatat initialized the complete stat on success.
        let current = unsafe { current.assume_init() };
        // Darwin and Linux use different stat integer widths for these identities.
        #[allow(clippy::unnecessary_cast)]
        let matches =
            current.st_dev as u64 == opened.dev() && current.st_ino as u64 == opened.ino();
        if !matches {
            return Err(ForegroundLockAcquireError::Error(
                "service runtime lock identity changed".into(),
            ));
        }
        Ok(lock)
    }

    pub(super) fn acquire_service(path: &Path) -> Result<Self, ForegroundLockAcquireError> {
        #[cfg(unix)]
        {
            Self::acquire_with_mode(
                path,
                ForegroundLockMode::OpenFileDescriptionOnly,
                ForegroundLockTestHook::none(),
            )
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(ForegroundLockAcquireError::Error(
                "service runtime requires an open-file-description foreground lock".into(),
            ))
        }
    }

    #[cfg(unix)]
    pub(super) fn duplicate_for_service_child(&self) -> Result<File, String> {
        if self.protocol != ForegroundLockProtocol::OpenFileDescription {
            return Err("service runtime requires an open-file-description foreground lock".into());
        }
        self.file
            .as_ref()
            .ok_or_else(|| "runtime foreground lock was released".to_string())?
            .as_fd()
            .try_clone_to_owned()
            .map(File::from)
            .map_err(|error| error.to_string())
    }

    pub(super) fn acquire(path: &Path) -> Result<Self, ForegroundLockAcquireError> {
        #[cfg(unix)]
        {
            Self::acquire_with_mode(
                path,
                ForegroundLockMode::Automatic,
                ForegroundLockTestHook::none(),
            )
        }
        #[cfg(not(unix))]
        {
            let file = open_lock(path).map_err(ForegroundLockAcquireError::Error)?;
            file.try_lock().map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    ForegroundLockAcquireError::WouldBlock
                } else {
                    ForegroundLockAcquireError::Error(format!("{}: {error}", path.display()))
                }
            })?;
            Ok(Self {
                file: Some(file),
                test_hook: ForegroundLockTestHook::none(),
            })
        }
    }

    #[cfg(all(test, unix))]
    pub(super) fn acquire_forced_traditional(
        path: &Path,
    ) -> Result<Self, ForegroundLockAcquireError> {
        Self::acquire_with_mode(
            path,
            ForegroundLockMode::TraditionalOnly,
            ForegroundLockTestHook::none(),
        )
    }

    #[cfg(all(test, unix))]
    pub(super) fn acquire_forced_traditional_with_after_file_close(
        path: &Path,
        after_file_close: impl FnOnce() + Send + 'static,
    ) -> Result<Self, ForegroundLockAcquireError> {
        Self::acquire_with_mode(
            path,
            ForegroundLockMode::TraditionalOnly,
            ForegroundLockTestHook::after_file_close(after_file_close),
        )
    }

    #[cfg(unix)]
    fn acquire_with_mode(
        path: &Path,
        mode: ForegroundLockMode,
        test_hook: ForegroundLockTestHook,
    ) -> Result<Self, ForegroundLockAcquireError> {
        let local_lock =
            LocalForegroundLock::reserve(path).map_err(map_local_foreground_lock_error)?;
        let file = open_lock(path).map_err(ForegroundLockAcquireError::Error)?;
        Self::finish_acquisition(path, file, local_lock, mode, test_hook)
    }

    #[cfg(unix)]
    fn finish_acquisition(
        path: &Path,
        file: File,
        mut local_lock: LocalForegroundLock,
        mode: ForegroundLockMode,
        test_hook: ForegroundLockTestHook,
    ) -> Result<Self, ForegroundLockAcquireError> {
        local_lock
            .bind_to_file(&file)
            .map_err(map_local_foreground_lock_error)?;
        let protocol =
            try_acquire_foreground_lock_with_mode(&file, mode).map_err(|error| match error {
                TryLockError::WouldBlock => ForegroundLockAcquireError::WouldBlock,
                TryLockError::Error(error) => {
                    ForegroundLockAcquireError::Error(format!("{}: {error}", path.display()))
                }
            })?;
        let local_traditional_lock = if protocol == ForegroundLockProtocol::Traditional {
            Some(local_lock)
        } else {
            local_lock.release();
            None
        };
        Ok(Self {
            file: Some(file),
            protocol,
            local_traditional_lock,
            test_hook,
        })
    }
}

impl Drop for ForegroundLock {
    fn drop(&mut self) {
        // A traditional POSIX record lock is process-scoped and any close of a
        // descriptor for this file can release it. Keep the in-process
        // reservation until this descriptor has definitely closed.
        drop(self.file.take());
        self.test_hook.run_after_file_close();
        #[cfg(unix)]
        drop(self.local_traditional_lock.take());
    }
}

#[cfg(unix)]
fn map_local_foreground_lock_error(error: String) -> ForegroundLockAcquireError {
    if error == "another Loxa runtime is active" {
        ForegroundLockAcquireError::WouldBlock
    } else {
        ForegroundLockAcquireError::Error(error)
    }
}

pub(super) fn open_lock(path: &Path) -> Result<File, String> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    validate_lock_file(&file, path)?;
    Ok(file)
}

fn validate_lock_file(file: &File, path: &Path) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe runtime lock {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe runtime lock {}", path.display()));
        }
    }
    Ok(())
}
