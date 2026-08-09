use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System, UpdateKind};

const LEASE_VERSION: u32 = 1;
const STOP_TIMEOUT: Duration = Duration::from_secs(2);
const OBSERVER_TEARDOWN_GRACE: Duration = Duration::from_millis(500);
static LEASE_STATE_IO: Mutex<()> = Mutex::new(());
#[cfg(unix)]
static LOCAL_FOREGROUND_LOCKS: Mutex<Vec<LocalForegroundLockKey>> = Mutex::new(Vec::new());
#[cfg(unix)]
static LOCAL_FOREGROUND_LOCK_OPERATIONS: Mutex<()> = Mutex::new(());

fn lock_lease_state() -> Result<MutexGuard<'static, ()>, String> {
    LEASE_STATE_IO
        .lock()
        .map_err(|_| "runtime lease state lock is poisoned".to_string())
}

#[cfg(unix)]
fn lock_local_foreground_operation() -> Result<MutexGuard<'static, ()>, String> {
    LOCAL_FOREGROUND_LOCK_OPERATIONS
        .lock()
        .map_err(|_| "local foreground lock operation is poisoned".to_string())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLease {
    version: u32,
    owner_pid: u32,
    owner_start_time: u64,
    child_pid: u32,
    child_start_time: u64,
    child_pgid: i32,
    server: PathBuf,
    model_id: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeProvenance {
    Managed,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForegroundObservation {
    Idle,
    Starting,
    Running(RuntimeProvenance, u16),
    Stopping,
    Error,
}

pub(crate) struct ForegroundObserver {
    run_dir: PathBuf,
    previously_running: bool,
    mismatch_started: Option<Instant>,
}

impl ForegroundObserver {
    pub(crate) fn new(run_dir: PathBuf) -> Self {
        Self {
            run_dir,
            previously_running: false,
            mismatch_started: None,
        }
    }

    pub(crate) fn observe(&mut self, managed_server: &Path) -> ForegroundObservation {
        let lock_path = self.run_dir.join("foreground.lock");
        let lease_path = self.run_dir.join("foreground.json");
        match foreground_lock_is_held(&lock_path) {
            Ok(false) => {
                self.previously_running = false;
                self.mismatch_started = None;
                if lease_is_absent(&lease_path) {
                    ForegroundObservation::Idle
                } else {
                    ForegroundObservation::Error
                }
            }
            Err(_) => {
                self.previously_running = false;
                self.mismatch_started = None;
                ForegroundObservation::Error
            }
            Ok(true) => match read_observed_lease(&lease_path) {
                ObservedLease::Absent | ObservedLease::Invalid => {
                    self.mismatch_started = None;
                    if self.previously_running {
                        ForegroundObservation::Stopping
                    } else {
                        ForegroundObservation::Starting
                    }
                }
                ObservedLease::Valid(lease) => {
                    match exact_live_provenance(&lease, managed_server) {
                        Ok(Some(provenance)) => {
                            self.previously_running = true;
                            self.mismatch_started = None;
                            ForegroundObservation::Running(provenance, lease.port)
                        }
                        Ok(None) | Err(_) if self.previously_running => {
                            let started = self.mismatch_started.get_or_insert_with(Instant::now);
                            if started.elapsed() < OBSERVER_TEARDOWN_GRACE {
                                ForegroundObservation::Stopping
                            } else {
                                self.previously_running = false;
                                ForegroundObservation::Error
                            }
                        }
                        Ok(None) | Err(_) => ForegroundObservation::Error,
                    }
                }
            },
        }
    }
}

enum ObservedLease {
    Absent,
    Invalid,
    Valid(RuntimeLease),
}

#[cfg(unix)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum ForegroundLockProtocol {
    OpenFileDescription,
    Traditional,
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum ForegroundLockMode {
    Automatic,
    #[cfg(test)]
    TraditionalOnly,
}

fn foreground_lock_is_held(path: &Path) -> Result<bool, String> {
    foreground_lock_is_held_with_after_open(path, || {})
}

fn foreground_lock_is_held_with_after_open(
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
fn try_acquire_foreground_lock(file: &File) -> Result<ForegroundLockProtocol, TryLockError> {
    try_acquire_foreground_lock_with_mode(file, ForegroundLockMode::Automatic)
}

#[cfg(unix)]
fn try_acquire_foreground_lock_with_mode(
    file: &File,
    mode: ForegroundLockMode,
) -> Result<ForegroundLockProtocol, TryLockError> {
    if matches!(mode, ForegroundLockMode::Automatic) {
        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
        match set_foreground_lock_with_command(file, libc::F_OFD_SETLK) {
            Ok(()) => return Ok(ForegroundLockProtocol::OpenFileDescription),
            Err(error) if ofd_lock_command_is_unsupported(&error) => {}
            Err(error) => return Err(as_try_lock_error(error)),
        }
    }

    set_foreground_lock_with_command(file, libc::F_SETLK)
        .map(|()| ForegroundLockProtocol::Traditional)
        .map_err(as_try_lock_error)
}

#[cfg(not(unix))]
fn try_acquire_foreground_lock(file: &File) -> Result<(), TryLockError> {
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
fn set_foreground_lock_with_command(file: &File, command: libc::c_int) -> std::io::Result<()> {
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

fn lease_is_absent(path: &Path) -> bool {
    matches!(
        fs::symlink_metadata(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    )
}

fn read_observed_lease(path: &Path) -> ObservedLease {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ObservedLease::Absent,
        Err(_) => ObservedLease::Invalid,
        Ok(_) => match read_lease(path) {
            Ok(lease) => ObservedLease::Valid(lease),
            Err(_) => ObservedLease::Invalid,
        },
    }
}

fn exact_live_provenance(
    lease: &RuntimeLease,
    managed_server: &Path,
) -> Result<Option<RuntimeProvenance>, String> {
    let Some(owner) = process_snapshot(lease.owner_pid)? else {
        return Ok(None);
    };
    if owner.start_identity != lease.owner_start_time {
        return Ok(None);
    }
    let Some(child) = process_snapshot(lease.child_pid)? else {
        return Ok(None);
    };
    if child.start_identity != lease.child_start_time
        || child.executable != lease.server
        || process_group(lease.child_pid)? != lease.child_pgid
        || !command_has_unique_option(&child.command, "--alias", OsStr::new(&lease.model_id))
        || !command_has_unique_option(
            &child.command,
            "--port",
            OsStr::new(&lease.port.to_string()),
        )
    {
        return Ok(None);
    }
    Ok(Some(if child.executable == managed_server {
        RuntimeProvenance::Managed
    } else {
        RuntimeProvenance::External
    }))
}

#[derive(Deserialize)]
struct LegacyRuntimeState {
    schema_version: u32,
    runs: Vec<LegacyRun>,
}

#[derive(Deserialize)]
struct LegacyRun {
    owner_pid: u32,
    owner_process_start_time_unix_s: u64,
    child_pid: Option<u32>,
    child_process_start_time_unix_s: Option<u64>,
    child_pgid: Option<i32>,
    generation_alias: Option<String>,
    port: Option<u16>,
}

#[derive(Debug)]
struct ProcessSnapshot {
    start_identity: u64,
    start_time_seconds: u64,
    executable: PathBuf,
    command: Vec<OsString>,
}

enum ForegroundLockAcquireError {
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

struct ForegroundLock {
    file: Option<File>,
    #[cfg(unix)]
    local_traditional_lock: Option<LocalForegroundLock>,
    test_hook: ForegroundLockTestHook,
}

impl ForegroundLock {
    fn acquire(path: &Path) -> Result<Self, ForegroundLockAcquireError> {
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
    fn acquire_forced_traditional(path: &Path) -> Result<Self, ForegroundLockAcquireError> {
        Self::acquire_with_mode(
            path,
            ForegroundLockMode::TraditionalOnly,
            ForegroundLockTestHook::none(),
        )
    }

    #[cfg(all(test, unix))]
    fn acquire_forced_traditional_with_after_file_close(
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
        let mut local_lock =
            LocalForegroundLock::reserve(path).map_err(map_local_foreground_lock_error)?;
        let file = open_lock(path).map_err(ForegroundLockAcquireError::Error)?;
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

pub(crate) struct RuntimeOwnership {
    _foreground_lock: ForegroundLock,
    state_path: PathBuf,
    lease: Option<RuntimeLease>,
}

impl RuntimeOwnership {
    pub(crate) fn acquire(run_dir: &Path) -> Result<Self, String> {
        Self::acquire_with_lock(run_dir, ForegroundLock::acquire)
    }

    #[cfg(all(test, unix))]
    fn acquire_forced_traditional(run_dir: &Path) -> Result<Self, String> {
        Self::acquire_with_lock(run_dir, ForegroundLock::acquire_forced_traditional)
    }

    #[cfg(all(test, unix))]
    fn acquire_forced_traditional_with_after_file_close(
        run_dir: &Path,
        after_file_close: impl FnOnce() + Send + 'static,
    ) -> Result<Self, String> {
        Self::acquire_with_lock(run_dir, |lock_path| {
            ForegroundLock::acquire_forced_traditional_with_after_file_close(
                lock_path,
                after_file_close,
            )
        })
    }

    fn acquire_with_lock(
        run_dir: &Path,
        acquire_lock: impl FnOnce(&Path) -> Result<ForegroundLock, ForegroundLockAcquireError>,
    ) -> Result<Self, String> {
        ensure_directory(run_dir)?;
        let lock_path = run_dir.join("foreground.lock");
        #[cfg(unix)]
        let operation = lock_local_foreground_operation()?;
        let foreground_lock = acquire_lock(&lock_path).map_err(|error| match error {
            ForegroundLockAcquireError::WouldBlock => "another Loxa runtime is active".into(),
            ForegroundLockAcquireError::Error(error) => error,
        })?;

        let state_path = run_dir.join("foreground.json");
        // Keep `operation` until the fallible reconciliation has completed.
        // On an error or panic, `foreground_lock` drops first, which closes the
        // traditional descriptor before releasing its local reservation.
        reconcile_state(&state_path)?;
        #[cfg(unix)]
        drop(operation);

        Ok(Self {
            _foreground_lock: foreground_lock,
            state_path,
            lease: None,
        })
    }

    pub(crate) fn record(
        &mut self,
        child_pid: u32,
        child_pgid: i32,
        model_id: &str,
        port: u16,
    ) -> Result<(), String> {
        let owner_pid = std::process::id();
        let owner = process_snapshot(owner_pid)?
            .ok_or_else(|| "failed to identify the Loxa process".to_string())?;
        let child = process_snapshot(child_pid)?
            .ok_or_else(|| "failed to identify the llama-server process".to_string())?;
        if process_group(child_pid)? != child_pgid {
            return Err("llama-server process group identity changed".into());
        }
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid,
            owner_start_time: owner.start_identity,
            child_pid,
            child_start_time: child.start_identity,
            child_pgid,
            server: child.executable,
            model_id: model_id.to_owned(),
            port,
        };
        let _state_guard = lock_lease_state()?;
        write_lease(&self.state_path, &lease)?;
        self.lease = Some(lease);
        Ok(())
    }

    pub(crate) fn clear(&mut self) -> Result<(), String> {
        let Some(expected) = self.lease.take() else {
            return Ok(());
        };
        let _state_guard = lock_lease_state()?;
        match read_lease(&self.state_path) {
            Ok(current) if current == expected => fs::remove_file(&self.state_path)
                .map_err(|error| format!("{}: {error}", self.state_path.display())),
            Ok(_) => Err(format!(
                "runtime lease changed unexpectedly: {}",
                self.state_path.display()
            )),
            Err(_error) if !self.state_path.exists() => Ok(()),
            Err(error) => Err(error),
        }
    }
}

pub(crate) fn clear_terminated_owned_lease(
    run_dir: &Path,
    child_pid: u32,
    child_pgid: i32,
) -> Result<(), String> {
    // foreground.lock excludes other Loxa processes; this guard serializes the
    // signal watcher with publication and cleanup inside the owning process.
    let _state_guard = lock_lease_state()?;
    let state_path = run_dir.join("foreground.json");
    let lease = match read_lease(&state_path) {
        Ok(lease) => lease,
        Err(_error) if !state_path.exists() => return Ok(()),
        Err(error) => return Err(error),
    };
    let owner_pid = std::process::id();
    let owner = process_snapshot(owner_pid)?
        .ok_or_else(|| "failed to identify the Loxa process".to_string())?;
    if lease.owner_pid != owner_pid
        || lease.owner_start_time != owner.start_identity
        || lease.child_pid != child_pid
        || lease.child_pgid != child_pgid
    {
        return Err(format!(
            "runtime lease changed unexpectedly: {}",
            state_path.display()
        ));
    }
    match fs::remove_file(&state_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("{}: {error}", state_path.display())),
    }
}

pub(crate) fn recover_stale(run_dir: &Path) -> Result<(), String> {
    if !run_dir.exists() {
        return Ok(());
    }
    ensure_directory(run_dir)?;
    let lock_path = run_dir.join("foreground.lock");
    #[cfg(unix)]
    let operation = lock_local_foreground_operation()?;
    #[cfg(unix)]
    if LocalForegroundLock::is_held(&lock_path)? {
        return Ok(());
    }
    let foreground_lock = match ForegroundLock::acquire(&lock_path) {
        Ok(lock) => lock,
        Err(ForegroundLockAcquireError::WouldBlock) => return Ok(()),
        Err(ForegroundLockAcquireError::Error(error)) => return Err(error),
    };
    reconcile_state(&run_dir.join("foreground.json"))?;
    reconcile_legacy_state(&run_dir.join("managed.json"))?;
    drop(foreground_lock);
    #[cfg(unix)]
    drop(operation);
    Ok(())
}

#[cfg(unix)]
struct LocalForegroundLock {
    key: Option<LocalForegroundLockKey>,
}

#[cfg(unix)]
#[derive(Clone, Eq, PartialEq)]
struct LocalForegroundLockKey {
    path: PathBuf,
    identity: Option<LocalForegroundLockIdentity>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Eq, PartialEq)]
struct LocalForegroundLockIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl LocalForegroundLockKey {
    fn from_path(path: &Path) -> Self {
        let identity = fs::symlink_metadata(path)
            .ok()
            .and_then(|metadata| foreground_lock_identity(&metadata));
        Self {
            path: path.to_path_buf(),
            identity,
        }
    }

    fn conflicts_with(&self, other: &Self) -> bool {
        self.path == other.path
            || matches!(
                (self.identity, other.identity),
                (Some(left), Some(right)) if left == right
            )
    }
}

#[cfg(unix)]
fn foreground_lock_identity(metadata: &fs::Metadata) -> Option<LocalForegroundLockIdentity> {
    use std::os::unix::fs::MetadataExt;

    if !metadata.file_type().is_file() {
        return None;
    }
    Some(LocalForegroundLockIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
impl LocalForegroundLock {
    fn reserve(path: &Path) -> Result<Self, String> {
        let key = LocalForegroundLockKey::from_path(path);
        let mut held = LOCAL_FOREGROUND_LOCKS
            .lock()
            .map_err(|_| "local foreground lock registry is poisoned".to_string())?;
        if held.iter().any(|existing| existing.conflicts_with(&key)) {
            return Err("another Loxa runtime is active".into());
        }
        held.push(key.clone());
        Ok(Self { key: Some(key) })
    }

    fn bind_to_file(&mut self, file: &File) -> Result<(), String> {
        let metadata = file
            .metadata()
            .map_err(|error| format!("runtime lock metadata: {error}"))?;
        let identity =
            foreground_lock_identity(&metadata).ok_or_else(|| "unsafe runtime lock".to_string())?;
        let path = self
            .key
            .as_ref()
            .ok_or_else(|| "local foreground lock reservation is missing".to_string())?
            .path
            .clone();
        let bound = LocalForegroundLockKey {
            path,
            identity: Some(identity),
        };
        let mut held = LOCAL_FOREGROUND_LOCKS
            .lock()
            .map_err(|_| "local foreground lock registry is poisoned".to_string())?;
        let index = held
            .iter()
            .position(|existing| self.key.as_ref() == Some(existing))
            .ok_or_else(|| "local foreground lock reservation is missing".to_string())?;
        if held
            .iter()
            .enumerate()
            .any(|(other, existing)| other != index && existing.conflicts_with(&bound))
        {
            return Err("another Loxa runtime is active".into());
        }
        held[index] = bound.clone();
        self.key = Some(bound);
        Ok(())
    }

    fn is_held(path: &Path) -> Result<bool, String> {
        let candidate = LocalForegroundLockKey::from_path(path);
        LOCAL_FOREGROUND_LOCKS
            .lock()
            .map(|held| {
                held.iter()
                    .any(|existing| existing.conflicts_with(&candidate))
            })
            .map_err(|_| "local foreground lock registry is poisoned".to_string())
    }

    fn release(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        if let Ok(mut held) = LOCAL_FOREGROUND_LOCKS.lock() {
            if let Some(index) = held.iter().position(|existing| existing == &key) {
                held.swap_remove(index);
            }
        }
    }
}

#[cfg(unix)]
impl Drop for LocalForegroundLock {
    fn drop(&mut self) {
        self.release();
    }
}

fn reconcile_legacy_state(state_path: &Path) -> Result<(), String> {
    if !state_path.exists() {
        return Ok(());
    }
    let state: LegacyRuntimeState = serde_json::from_slice(&read_regular_file(state_path)?)
        .map_err(|error| format!("{}: {error}", state_path.display()))?;
    if state.schema_version != 4 {
        return Err(format!(
            "unsupported legacy runtime state: {}",
            state_path.display()
        ));
    }

    let mut active_owner = false;
    for run in state.runs {
        if run.owner_pid == 0 || run.owner_process_start_time_unix_s == 0 {
            return Err(format!(
                "invalid legacy runtime state: {}",
                state_path.display()
            ));
        }
        if process_snapshot(run.owner_pid)?
            .is_some_and(|owner| owner.start_time_seconds == run.owner_process_start_time_unix_s)
        {
            active_owner = true;
            continue;
        }
        let (child_pid, child_start_time, child_pgid) = match (
            run.child_pid,
            run.child_process_start_time_unix_s,
            run.child_pgid,
        ) {
            (None, None, None) => continue,
            (Some(pid), Some(start_time), Some(group))
                if pid != 0
                    && start_time != 0
                    && group > 1
                    && group == i32::try_from(pid).unwrap_or(-1) =>
            {
                (pid, start_time, group)
            }
            _ => {
                return Err(format!(
                    "invalid legacy runtime state: {}",
                    state_path.display()
                ))
            }
        };
        let Some(child) = process_snapshot(child_pid)? else {
            continue;
        };
        let legacy_identity_matches = run
            .generation_alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .zip(run.port.filter(|port| *port != 0))
            .is_some_and(|(alias, port)| {
                command_has_unique_option(&child.command, "--alias", OsStr::new(alias))
                    && command_has_unique_option(
                        &child.command,
                        "--port",
                        OsStr::new(&port.to_string()),
                    )
            });
        if child.start_time_seconds == child_start_time
            && child.executable.file_name() == Some(OsStr::new("llama-server"))
            && legacy_identity_matches
            && process_group(child_pid)? == child_pgid
        {
            terminate_stale_process_group(child_pgid)?;
        }
    }

    if active_owner {
        return Err("another Loxa runtime owns the legacy llama-server".into());
    }
    fs::remove_file(state_path).map_err(|error| format!("{}: {error}", state_path.display()))?;
    Ok(())
}

fn reconcile_state(state_path: &Path) -> Result<(), String> {
    if !state_path.exists() {
        return Ok(());
    }
    let stale = read_lease(state_path)?;
    reconcile(&stale)?;
    fs::remove_file(state_path).map_err(|error| format!("{}: {error}", state_path.display()))
}

fn reconcile(lease: &RuntimeLease) -> Result<(), String> {
    validate_lease(lease)?;
    if process_snapshot(lease.owner_pid)?
        .is_some_and(|owner| owner.start_identity == lease.owner_start_time)
    {
        return Err("another Loxa runtime owns the recorded llama-server".into());
    }
    if let Some(child) = process_snapshot(lease.child_pid)? {
        if child.start_identity != lease.child_start_time
            || child.executable != lease.server
            || process_group(lease.child_pid)? != lease.child_pgid
        {
            return Ok(());
        }
    } else if !process_group_exists(lease.child_pgid)? {
        return Ok(());
    }
    terminate_stale_process_group(lease.child_pgid)
}

fn validate_lease(lease: &RuntimeLease) -> Result<(), String> {
    if lease.version != LEASE_VERSION
        || lease.owner_pid == 0
        || lease.child_pid == 0
        || lease.child_pgid <= 1
        || lease.child_pgid != i32::try_from(lease.child_pid).unwrap_or(-1)
        || lease.owner_start_time == 0
        || lease.child_start_time == 0
        || lease.server.as_os_str().is_empty()
        || lease.model_id.is_empty()
        || lease.port == 0
    {
        Err("invalid runtime lease".into())
    } else {
        Ok(())
    }
}

fn ensure_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(format!(
            "runtime path is not a directory: {}",
            path.display()
        ))
    }
}

fn open_lock(path: &Path) -> Result<File, String> {
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
    Ok(file)
}

fn read_lease(path: &Path) -> Result<RuntimeLease, String> {
    let lease: RuntimeLease = serde_json::from_slice(&read_regular_file(path)?)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    validate_lease(&lease)?;
    Ok(lease)
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    crate::safe_file::read_regular_file(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::InvalidData {
            format!("unsafe runtime lease {}", path.display())
        } else {
            format!("{}: {error}", path.display())
        }
    })
}

fn write_lease(path: &Path, lease: &RuntimeLease) -> Result<(), String> {
    validate_lease(lease)?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(lease).map_err(|error| error.to_string())?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| format!("{}: {error}", temporary.display()))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("{}: {error}", temporary.display()))?;
    fs::rename(&temporary, path).map_err(|error| format!("{}: {error}", path.display()))
}

fn process_snapshot(pid: u32) -> Result<Option<ProcessSnapshot>, String> {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_cmd(UpdateKind::OnlyIfNotSet)
            .with_exe(UpdateKind::OnlyIfNotSet),
    );
    let Some(process) = system.process(pid) else {
        return Ok(None);
    };
    let executable = process
        .exe()
        .ok_or_else(|| format!("failed to inspect executable for process {pid}"))?;
    let start_time_seconds = process.start_time();
    Ok(Some(ProcessSnapshot {
        start_identity: process_start_identity(pid.as_u32(), start_time_seconds)?,
        start_time_seconds,
        executable: executable.to_path_buf(),
        command: process.cmd().to_vec(),
    }))
}

fn command_has_unique_option(command: &[OsString], option: &str, expected: &OsStr) -> bool {
    let option = OsStr::new(option);
    let mut matches = command
        .iter()
        .enumerate()
        .filter_map(|(index, argument)| (argument == option).then_some(index));
    let Some(index) = matches.next() else {
        return false;
    };
    matches.next().is_none()
        && command
            .get(index + 1)
            .is_some_and(|value| value.as_os_str() == expected)
}

#[cfg(target_os = "macos")]
fn process_start_identity(pid: u32, expected_seconds: u64) -> Result<u64, String> {
    let pid = i32::try_from(pid).map_err(|_| "process id is not representable".to_string())?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>();
    let size =
        i32::try_from(size).map_err(|_| "process identity buffer is too large".to_string())?;
    // SAFETY: proc_pidinfo initializes exactly one proc_bsdinfo when it returns its full size.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if read != size {
        return Err(format!("failed to inspect start time for process {pid}"));
    }
    // SAFETY: the full proc_bsdinfo was initialized above.
    let info = unsafe { info.assume_init() };
    if info.pbi_start_tvsec != expected_seconds {
        return Err(format!("process {pid} changed while inspecting it"));
    }
    info.pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec))
        .ok_or_else(|| format!("invalid start time for process {pid}"))
}

#[cfg(not(target_os = "macos"))]
fn process_start_identity(_pid: u32, expected_seconds: u64) -> Result<u64, String> {
    Ok(expected_seconds)
}

fn process_group(pid: u32) -> Result<i32, String> {
    let pid = i32::try_from(pid).map_err(|_| "process id is not representable".to_string())?;
    // SAFETY: getpgid only observes the process-group identity for this validated PID.
    let group = unsafe { libc::getpgid(pid) };
    if group >= 0 {
        Ok(group)
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

pub(crate) fn terminate_process_group(child: &mut Child, group: i32) -> Result<(), String> {
    signal_process_group(group, libc::SIGTERM)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_exists(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_process_group(group, libc::SIGKILL)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_exists(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("owned process group survived SIGKILL".into())
}

pub(crate) fn terminate_stale_process_group(group: i32) -> Result<(), String> {
    signal_process_group(group, libc::SIGTERM)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        if !process_group_has_live_members(group)? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_process_group(group, libc::SIGKILL)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        if !process_group_has_live_members(group)? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("stale process group survived SIGKILL".into())
}

fn signal_process_group(group: i32, signal: i32) -> Result<(), String> {
    if group <= 1 {
        return Err("refusing to signal an unsafe process group".into());
    }
    // SAFETY: the negative PID targets only the validated process group.
    let result = unsafe { libc::kill(-group, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error.to_string())
    }
}

fn process_group_exists(group: i32) -> Result<bool, String> {
    // SAFETY: signal 0 probes existence without delivering a signal.
    let result = unsafe { libc::kill(-group, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error.to_string()),
    }
}

fn process_group_has_live_members(group: i32) -> Result<bool, String> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    for (pid, process) in system.processes() {
        if matches!(
            process.status(),
            ProcessStatus::Dead | ProcessStatus::Zombie
        ) {
            continue;
        }
        let Ok(pid) = i32::try_from(pid.as_u32()) else {
            continue;
        };
        // SAFETY: getpgid only observes the process-group identity.
        let observed = unsafe { libc::getpgid(pid) };
        if observed == group {
            return Ok(true);
        }
        if observed < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.to_string());
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{RuntimeInventorySnapshot, RuntimeSnapshot, SnapshotReader};
    use crate::paths::AppPaths;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    fn spawn_sleep() -> Child {
        let mut command = Command::new("/bin/sleep");
        command.arg("60");
        command.process_group(0);
        command.spawn().unwrap()
    }

    fn spawn_observable_server(model_id: &str, port: u16) -> Child {
        let mut command = Command::new("/bin/bash");
        command
            .arg("-c")
            .arg("while :; do sleep 60; done")
            .arg("--alias")
            .arg(model_id)
            .arg("--port")
            .arg(port.to_string())
            .process_group(0);
        command.spawn().unwrap()
    }

    fn observed_lease(child: &Child, model_id: &str, port: u16) -> RuntimeLease {
        let child_pid = child.id();
        let child = process_snapshot(child_pid).unwrap().unwrap();
        let owner = process_snapshot(std::process::id()).unwrap().unwrap();
        RuntimeLease {
            version: LEASE_VERSION,
            owner_pid: std::process::id(),
            owner_start_time: owner.start_identity,
            child_pid,
            child_start_time: child.start_identity,
            child_pgid: i32::try_from(child_pid).unwrap(),
            server: child.executable,
            model_id: model_id.into(),
            port,
        }
    }

    struct ForegroundLockHolder {
        child: Child,
        release: PathBuf,
    }

    impl Drop for ForegroundLockHolder {
        fn drop(&mut self) {
            let _ = fs::write(&self.release, b"release");
            let _ = self.child.wait();
        }
    }

    fn wait_for_path(path: &Path, description: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}: {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn hold_foreground_lock(run_dir: &Path) -> ForegroundLockHolder {
        fs::create_dir_all(run_dir).unwrap();
        let ready = run_dir.join("foreground-lock.ready");
        let release = run_dir.join("foreground-lock.release");
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--ignored")
            .arg("--exact")
            .arg("runtime::tests::foreground_record_lock_holder_process")
            .env("LOXA_FOREGROUND_LOCK_PATH", run_dir.join("foreground.lock"))
            .env("LOXA_FOREGROUND_LOCK_READY", &ready)
            .env("LOXA_FOREGROUND_LOCK_RELEASE", &release)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_for_path(&ready, "foreground lock holder");
        ForegroundLockHolder { child, release }
    }

    fn foreground_contender_acquires(run_dir: &Path) -> bool {
        fs::create_dir_all(run_dir).unwrap();
        let result = run_dir.join("foreground-contender.result");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--ignored")
            .arg("--exact")
            .arg("runtime::tests::foreground_record_lock_contender_process")
            .env("LOXA_FOREGROUND_LOCK_PATH", run_dir.join("foreground.lock"))
            .env("LOXA_FOREGROUND_LOCK_RESULT", &result)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());
        fs::read(&result).unwrap() == b"acquired"
    }

    #[cfg(unix)]
    #[test]
    #[ignore]
    fn foreground_record_lock_holder_process() {
        let path = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_PATH").unwrap());
        let ready = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_READY").unwrap());
        let release = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_RELEASE").unwrap());
        let lock = open_lock(&path).unwrap();
        try_acquire_foreground_lock(&lock).unwrap();
        fs::write(ready, b"ready").unwrap();
        wait_for_path(&release, "foreground lock release");
    }

    #[cfg(unix)]
    #[test]
    #[ignore]
    fn foreground_record_lock_contender_process() {
        let path = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_PATH").unwrap());
        let result = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_RESULT").unwrap());
        let lock = open_lock(&path).unwrap();
        let outcome = match try_acquire_foreground_lock(&lock) {
            Ok(_) => b"acquired".as_slice(),
            Err(TryLockError::WouldBlock) => b"blocked".as_slice(),
            Err(TryLockError::Error(error)) => panic!("unexpected contender error: {error}"),
        };
        fs::write(result, outcome).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn foreground_lock_query_never_owns_an_available_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("foreground.lock");
        drop(open_lock(&path).unwrap());

        assert!(!foreground_lock_is_held(&path).unwrap());

        let _holder = hold_foreground_lock(dir.path());
        let contender = open_lock(&path).unwrap();
        assert!(matches!(
            try_acquire_foreground_lock(&contender),
            Err(TryLockError::WouldBlock)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn foreground_query_holds_its_local_operation_guard_while_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("foreground.lock");
        drop(open_lock(&path).unwrap());

        assert!(!foreground_lock_is_held_with_after_open(&path, || {
            assert!(
                LOCAL_FOREGROUND_LOCK_OPERATIONS.try_lock().is_err(),
                "the query's descriptor lifetime must be serialized with local acquisition"
            );
        })
        .unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn same_process_observation_keeps_foreground_owner_exclusive() {
        let dir = tempdir().unwrap();
        let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();
        recover_stale(dir.path()).unwrap();
        let mut observer = ForegroundObserver::new(dir.path().to_path_buf());

        let observation = observer.observe(Path::new("/managed/llama-server"));
        let contender_acquired = foreground_contender_acquires(dir.path());
        assert_eq!(
            (observation, contender_acquired),
            (ForegroundObservation::Starting, false),
            "an in-process observation must neither hide nor release lifecycle ownership"
        );

        drop(ownership);
        assert!(
            foreground_contender_acquires(dir.path()),
            "dropping ownership must release the lifecycle lock"
        );
    }

    #[cfg(unix)]
    #[test]
    fn traditional_lock_fallback_isolates_parent_symlink_aliases() {
        let dir = tempdir().unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
        let path = dir.path().join("foreground.lock");
        let mut local_lock = LocalForegroundLock::reserve(&path).unwrap();
        let lock = open_lock(&path).unwrap();
        local_lock.bind_to_file(&lock).unwrap();
        set_foreground_lock_with_command(&lock, libc::F_SETLK).unwrap();

        assert!(
            foreground_lock_is_held(&alias.join("foreground.lock")).unwrap(),
            "the fallback must not open and close an alias of its own lock"
        );
        assert!(
            !foreground_contender_acquires(&alias),
            "an alias observation must not release traditional ownership"
        );
    }

    #[cfg(unix)]
    #[test]
    fn traditional_fallback_reconciliation_failure_preserves_the_next_owner_lock() {
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("foreground.json");
        let lock_path = dir.path().join("foreground.lock");
        fs::write(&state_path, b"not valid runtime state").unwrap();

        let (closed_tx, closed_rx) = std::sync::mpsc::sync_channel(0);
        let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(0);
        let first_dir = dir.path().to_path_buf();
        let first_lock_path = lock_path.clone();
        let first = thread::spawn(move || {
            RuntimeOwnership::acquire_forced_traditional_with_after_file_close(
                &first_dir,
                move || {
                    assert!(
                        LocalForegroundLock::is_held(&first_lock_path).unwrap(),
                        "the traditional reservation must survive until after its descriptor closes"
                    );
                    assert!(
                        LOCAL_FOREGROUND_LOCK_OPERATIONS.try_lock().is_err(),
                        "the operation guard must cover reconciliation unwind"
                    );
                    closed_tx.send(()).unwrap();
                    release_first_rx.recv().unwrap();
                },
            )
        });

        closed_rx.recv().unwrap();

        let (second_attempt_tx, second_attempt_rx) = std::sync::mpsc::sync_channel(0);
        let (second_ready_tx, second_ready_rx) = std::sync::mpsc::sync_channel(0);
        let (release_second_tx, release_second_rx) = std::sync::mpsc::sync_channel(0);
        let second_dir = dir.path().to_path_buf();
        let second = thread::spawn(move || {
            second_attempt_tx.send(()).unwrap();
            let ownership = RuntimeOwnership::acquire_forced_traditional(&second_dir).unwrap();
            second_ready_tx.send(()).unwrap();
            release_second_rx.recv().unwrap();
            drop(ownership);
        });
        second_attempt_rx.recv().unwrap();

        fs::remove_file(&state_path).unwrap();
        release_first_tx.send(()).unwrap();
        let first_result = first.join().unwrap();
        assert!(
            first_result.is_err(),
            "first acquisition unexpectedly succeeded"
        );
        let first_error = first_result.err().unwrap();
        assert!(first_error.contains("foreground.json"), "{first_error}");

        second_ready_rx.recv().unwrap();
        assert!(
            !foreground_contender_acquires(dir.path()),
            "the next traditional owner must stay exclusive to child contenders"
        );

        release_second_tx.send(()).unwrap();
        second.join().unwrap();
        assert!(
            foreground_contender_acquires(dir.path()),
            "dropping the next traditional owner must release the lifecycle lock"
        );
    }

    #[cfg(unix)]
    #[test]
    fn foreground_observer_reports_a_real_contending_owner_without_acquiring_the_lock() {
        let dir = tempdir().unwrap();
        let _holder = hold_foreground_lock(dir.path());
        let mut observer = ForegroundObserver::new(dir.path().to_path_buf());

        assert_eq!(
            observer.observe(Path::new("/managed/llama-server")),
            ForegroundObservation::Starting
        );

        let contender = open_lock(&dir.path().join("foreground.lock")).unwrap();
        assert!(matches!(
            try_acquire_foreground_lock(&contender),
            Err(TryLockError::WouldBlock)
        ));
    }

    #[test]
    fn foreground_observer_distinguishes_cold_start_running_stopping_and_idle_without_mutation() {
        let dir = tempdir().unwrap();
        let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
        let managed = Path::new("/managed/llama-server");
        let lock = hold_foreground_lock(dir.path());
        assert_eq!(observer.observe(managed), ForegroundObservation::Starting);

        let mut child = spawn_observable_server("demo", 43123);
        let lease = observed_lease(&child, "demo", 43123);
        let state_path = dir.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();
        let before = fs::read(&state_path).unwrap();
        let owner = process_snapshot(lease.owner_pid).unwrap().unwrap();
        let observed_child = process_snapshot(lease.child_pid).unwrap().unwrap();
        assert_eq!(owner.start_identity, lease.owner_start_time);
        assert_eq!(observed_child.start_identity, lease.child_start_time);
        assert_eq!(observed_child.executable, lease.server);
        assert_eq!(process_group(lease.child_pid).unwrap(), lease.child_pgid);
        assert!(command_has_unique_option(
            &observed_child.command,
            "--alias",
            OsStr::new("demo")
        ));
        assert!(command_has_unique_option(
            &observed_child.command,
            "--port",
            OsStr::new("43123")
        ));
        assert_eq!(
            observer.observe(managed),
            ForegroundObservation::Running(RuntimeProvenance::External, 43123)
        );
        assert_eq!(fs::read(&state_path).unwrap(), before);
        assert!(dir.path().join("foreground.lock").is_file());

        fs::remove_file(&state_path).unwrap();
        assert_eq!(observer.observe(managed), ForegroundObservation::Stopping);
        drop(lock);
        assert_eq!(observer.observe(managed), ForegroundObservation::Idle);
        let group = i32::try_from(child.id()).unwrap();
        terminate_process_group(&mut child, group).unwrap();
    }

    #[test]
    fn foreground_observer_treats_lease_lock_contradictions_and_malformed_leases_conservatively() {
        let managed = Path::new("/managed/llama-server");
        let mut child = spawn_observable_server("demo", 43123);
        let lease = observed_lease(&child, "demo", 43123);

        let contradictory = tempdir().unwrap();
        write_lease(&contradictory.path().join("foreground.json"), &lease).unwrap();
        let mut contradictory_observer =
            ForegroundObserver::new(contradictory.path().to_path_buf());
        assert_eq!(
            contradictory_observer.observe(managed),
            ForegroundObservation::Error
        );
        assert!(contradictory.path().join("foreground.json").is_file());

        let malformed = tempdir().unwrap();
        let _lock = hold_foreground_lock(malformed.path());
        let malformed_path = malformed.path().join("foreground.json");
        let bytes = b"Authorization: Bearer secret";
        fs::write(&malformed_path, bytes).unwrap();
        let mut malformed_observer = ForegroundObserver::new(malformed.path().to_path_buf());
        assert_eq!(
            malformed_observer.observe(managed),
            ForegroundObservation::Starting
        );
        assert_eq!(fs::read(&malformed_path).unwrap(), bytes);
        let group = i32::try_from(child.id()).unwrap();
        terminate_process_group(&mut child, group).unwrap();
    }

    #[test]
    fn foreground_observer_requires_every_live_identity_and_allows_only_teardown_grace() {
        let managed = Path::new("/managed/llama-server");
        let mut child = spawn_observable_server("demo", 43123);
        let lease = observed_lease(&child, "demo", 43123);

        for mismatch in [
            {
                let mut mismatch = lease.clone();
                mismatch.owner_start_time = mismatch.owner_start_time.saturating_add(1);
                mismatch
            },
            {
                let mut mismatch = lease.clone();
                mismatch.child_start_time = mismatch.child_start_time.saturating_add(1);
                mismatch
            },
            {
                let mut mismatch = lease.clone();
                mismatch.child_pgid = -1;
                mismatch
            },
            {
                let mut mismatch = lease.clone();
                mismatch.server = PathBuf::from("/other/llama-server");
                mismatch
            },
            {
                let mut mismatch = lease.clone();
                mismatch.model_id = "other".into();
                mismatch
            },
            {
                let mut mismatch = lease.clone();
                mismatch.port = 43124;
                mismatch
            },
        ] {
            let dir = tempdir().unwrap();
            let _lock = hold_foreground_lock(dir.path());
            fs::write(
                dir.path().join("foreground.json"),
                serde_json::to_vec_pretty(&mismatch).unwrap(),
            )
            .unwrap();
            let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
            assert_ne!(
                observer.observe(managed),
                ForegroundObservation::Running(RuntimeProvenance::External, 43123),
                "mismatch unexpectedly became Running: {mismatch:?}"
            );
        }

        let grace = tempdir().unwrap();
        let _lock = hold_foreground_lock(grace.path());
        let state_path = grace.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();
        let mut observer = ForegroundObserver::new(grace.path().to_path_buf());
        assert_eq!(
            observer.observe(managed),
            ForegroundObservation::Running(RuntimeProvenance::External, 43123)
        );
        let mut wrong_port = lease;
        wrong_port.port = 43124;
        write_lease(&state_path, &wrong_port).unwrap();
        assert_eq!(observer.observe(managed), ForegroundObservation::Stopping);
        thread::sleep(Duration::from_millis(510));
        assert_eq!(observer.observe(managed), ForegroundObservation::Error);
        let group = i32::try_from(child.id()).unwrap();
        terminate_process_group(&mut child, group).unwrap();
    }

    #[test]
    fn snapshot_reader_surfaces_only_an_exact_external_foreground_runtime() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let lock = hold_foreground_lock(&paths.run);
        let mut child = spawn_observable_server("demo", 43123);
        let lease = observed_lease(&child, "demo", 43123);
        let lease_path = paths.run.join("foreground.json");
        write_lease(&lease_path, &lease).unwrap();
        let before = fs::read(&lease_path).unwrap();

        let mut reader = SnapshotReader::new(paths);
        let snapshot = reader.observe();

        assert_eq!(snapshot.runtime(), RuntimeSnapshot::Running);
        assert_eq!(
            snapshot.runtime_inventory(),
            RuntimeInventorySnapshot::External
        );
        assert_eq!(fs::read(&lease_path).unwrap(), before);
        drop(lock);
        let group = i32::try_from(child.id()).unwrap();
        terminate_process_group(&mut child, group).unwrap();
    }

    fn wait_until_gone(pid: u32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if process_snapshot(pid).is_ok_and(|snapshot| snapshot.is_none()) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn general_recovery_stops_an_exact_orphaned_child() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: 1,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity,
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        write_lease(&dir.path().join("foreground.json"), &lease).unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(wait_until_gone(pid), "exact orphan survived recovery");
        assert!(!dir.path().join("foreground.json").exists());
        let _ = child.wait();
    }

    #[test]
    fn signal_cleanup_removes_the_exact_owned_lease_after_group_termination() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let group = i32::try_from(pid).unwrap();
        let child_snapshot = process_snapshot(pid).unwrap().unwrap();
        let owner_snapshot = process_snapshot(std::process::id()).unwrap().unwrap();
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid: std::process::id(),
            owner_start_time: owner_snapshot.start_identity,
            child_pid: pid,
            child_start_time: child_snapshot.start_identity,
            child_pgid: group,
            server: child_snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        let state_path = dir.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();

        terminate_stale_process_group(group).unwrap();
        clear_terminated_owned_lease(dir.path(), pid, group).unwrap();

        assert!(!state_path.exists());
        let _ = child.wait();
    }

    #[test]
    fn signal_cleanup_preserves_a_foreign_lease() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let group = i32::try_from(pid).unwrap();
        let child_snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: child_snapshot.start_identity,
            child_pgid: group,
            server: child_snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        let state_path = dir.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();

        terminate_stale_process_group(group).unwrap();
        let error = clear_terminated_owned_lease(dir.path(), pid, group).unwrap_err();

        assert!(
            error.contains("runtime lease changed unexpectedly"),
            "{error}"
        );
        assert!(state_path.exists());
        let _ = child.wait();
    }

    #[test]
    fn acquiring_runtime_never_signals_a_reused_process_identity() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: 1,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity.saturating_add(1),
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        write_lease(&dir.path().join("foreground.json"), &lease).unwrap();

        let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();

        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "reused PID was signaled"
        );
        assert!(!dir.path().join("foreground.json").exists());
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
        drop(ownership);
    }

    #[test]
    fn legacy_recovery_never_signals_without_unique_command_identity() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let server = dir.path().join("llama-server");
        fs::copy("/bin/sleep", &server).unwrap();
        fs::set_permissions(&server, fs::Permissions::from_mode(0o755)).unwrap();
        let mut command = Command::new(&server);
        command.arg("60").process_group(0);
        let mut child = command.spawn().unwrap();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "schema_version": 4,
                "run_id": "legacy",
                "model_id": "demo",
                "owner_pid": u32::MAX,
                "owner_process_start_time_unix_s": 1,
                "stop_requested": false,
                "lifecycle": "running",
                "generation": 1,
                "generation_alias": "legacy-g1",
                "control_port": 11436,
                "port": 1234,
                "log_path": "/tmp/legacy.log",
                "child_pid": pid,
                "child_process_start_time_unix_s": snapshot.start_time_seconds,
                "child_pgid": pid
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "ambiguous legacy process was signaled"
        );
        assert!(!dir.path().join("managed.json").exists());
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
    }

    #[test]
    fn legacy_command_identity_requires_exact_alias_and_port_pairs() {
        let command = [
            OsString::from("llama-server"),
            OsString::from("--alias"),
            OsString::from("legacy-g1"),
            OsString::from("--port"),
            OsString::from("1234"),
        ];

        assert!(command_has_unique_option(
            &command,
            "--alias",
            OsStr::new("legacy-g1")
        ));
        assert!(command_has_unique_option(
            &command,
            "--port",
            OsStr::new("1234")
        ));
        assert!(!command_has_unique_option(
            &command,
            "--alias",
            OsStr::new("other")
        ));
        let duplicate = [
            command.as_slice(),
            &[OsString::from("--alias"), OsString::from("other")],
        ]
        .concat();
        assert!(!command_has_unique_option(
            &duplicate,
            "--alias",
            OsStr::new("legacy-g1")
        ));
    }

    #[test]
    fn recovery_accepts_a_stale_unloaded_legacy_record() {
        let dir = tempdir().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "owner_pid": u32::MAX,
                "owner_process_start_time_unix_s": 1,
                "child_pid": null,
                "child_process_start_time_unix_s": null,
                "child_pgid": null
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(!dir.path().join("managed.json").exists());
    }

    #[test]
    fn recovery_blocks_a_second_runtime_while_a_legacy_owner_is_alive() {
        let dir = tempdir().unwrap();
        let owner_pid = std::process::id();
        let owner = process_snapshot(owner_pid).unwrap().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "owner_pid": owner_pid,
                "owner_process_start_time_unix_s": owner.start_time_seconds,
                "child_pid": null,
                "child_process_start_time_unix_s": null,
                "child_pgid": null
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        let error = recover_stale(dir.path()).unwrap_err();

        assert!(error.contains("legacy llama-server"), "{error}");
        assert!(dir.path().join("managed.json").exists());
    }

    #[test]
    fn stale_cleanup_waits_for_term_resistant_descendants() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("descendant-ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(format!(
                "(trap '' TERM; printf ready > '{}'; while :; do sleep 1; done) & wait",
                marker.display()
            ))
            .process_group(0);
        let mut child = command.spawn().unwrap();
        let group = i32::try_from(child.id()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !marker.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(marker.exists(), "descendant did not start");

        terminate_stale_process_group(group).unwrap();

        assert!(!process_group_has_live_members(group).unwrap());
        let _ = child.wait();
    }
}
