use crate::runtime_fingerprint::RuntimeFingerprint;
use serde::Deserialize;
use std::ffi::OsStr;
#[cfg(test)]
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{Read as _, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

mod lease;
use lease::{
    decode_lease, encode_lease, validate_lease, LeaseOwnerMode, RuntimeLease, ServiceLeaseFields,
    LEASE_VERSION, PERSISTENT_LEASE_VERSION, SERVICE_LEASE_VERSION,
};
#[cfg(test)]
use lease::{encode_v3_lease, RuntimeLeaseV1, RuntimeLeaseV2, LEGACY_LEASE_VERSION};

mod process;
#[cfg(test)]
pub(crate) use process::fail_next_owned_group_terminations_for_test;
use process::{
    command_has_unique_option, process_group, process_group_exists, process_snapshot,
    process_snapshot_from_refreshed_system, ProcessSnapshot,
};
pub(crate) use process::{
    current_process_start_identity, process_group_has_live_members, terminate_process_group,
    terminate_process_group_immediately, terminate_stale_process_group,
};

const OBSERVER_TEARDOWN_GRACE: Duration = Duration::from_millis(500);
const MAX_RUNTIME_RECORD_BYTES: usize = 64 * 1024;
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

pub(crate) enum RuntimeLeasePublication<'a> {
    Foreground,
    PersistentApp(&'a RuntimeFingerprint),
    Service {
        fingerprint: &'a RuntimeFingerprint,
        endpoint: &'a Path,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeProvenance {
    Managed,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeOwner {
    Legacy,
    Foreground,
    PersistentApp,
    Service,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ForegroundObservation {
    Idle,
    Starting,
    Running {
        provenance: RuntimeProvenance,
        owner: RuntimeOwner,
        model_id: String,
        port: u16,
    },
    Stopping,
    Error,
}

mod attachment;
pub(crate) use attachment::{
    lookup_persistent_runtime, lookup_runtime_presence, AttachedRuntime, PersistentRuntimeLookup,
    RuntimePresence,
};

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
                            let owner = match lease.owner_mode {
                                None => RuntimeOwner::Legacy,
                                Some(LeaseOwnerMode::Foreground) => RuntimeOwner::Foreground,
                                Some(LeaseOwnerMode::PersistentApp) => RuntimeOwner::PersistentApp,
                                Some(LeaseOwnerMode::Service) => RuntimeOwner::Service,
                            };
                            ForegroundObservation::Running {
                                provenance,
                                owner,
                                model_id: lease.model_id.clone(),
                                port: lease.port,
                            }
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
    Valid(Box<RuntimeLease>),
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
            Ok(lease) => ObservedLease::Valid(Box::new(lease)),
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
    Ok(Some(
        if lease.attributed_managed_source() == managed_server {
            RuntimeProvenance::Managed
        } else {
            RuntimeProvenance::External
        },
    ))
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

/// Common runtime ownership. The foreground lock remains held while either this
/// handle or a reserved child token exists, and only one child may be reserved.
pub(crate) struct RuntimeOwnership {
    inner: Arc<Mutex<RuntimeOwnershipInner>>,
}

struct RuntimeOwnershipInner {
    _foreground_lock: ForegroundLock,
    state_path: PathBuf,
    child_reserved: bool,
}

/// Exclusive child slot borrowed from [`RuntimeOwnership`]. After
/// [`Self::child_spawned`], keep this token until the exact child is confirmed
/// terminated and [`Self::clear`] has removed its recorded lease and stage.
/// Dropping earlier leaves the child slot closed while the common owner is
/// retained.
pub(crate) struct RuntimeChildOwnership {
    inner: Arc<Mutex<RuntimeOwnershipInner>>,
    lease: Option<RuntimeLease>,
    release_on_drop: bool,
}

pub(crate) enum RuntimeOwnershipAcquireError {
    Conflict,
    Failed(String),
}

impl RuntimeOwnershipAcquireError {
    fn into_message(self) -> String {
        match self {
            Self::Conflict => "another Loxa runtime is active".into(),
            Self::Failed(message) => message,
        }
    }
}

impl RuntimeOwnership {
    pub(crate) fn acquire(run_dir: &Path) -> Result<Self, String> {
        Self::acquire_with_lock(run_dir, ForegroundLock::acquire)
    }

    pub(crate) fn acquire_persistent(run_dir: &Path) -> Result<Self, RuntimeOwnershipAcquireError> {
        Self::acquire_with_lock_classified(run_dir, ForegroundLock::acquire)
    }

    /// Acquires the common runtime lock without touching prior leases or
    /// prepared stages. The service uses this seam so a recovery failure cannot
    /// release the only cross-version ownership fence.
    pub(crate) fn acquire_service_unreconciled(
        run_dir: &Path,
    ) -> Result<Self, RuntimeOwnershipAcquireError> {
        Self::acquire_unreconciled_with_lock(run_dir, ForegroundLock::acquire)
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
        Self::acquire_with_lock_classified(run_dir, acquire_lock)
            .map_err(RuntimeOwnershipAcquireError::into_message)
    }

    fn acquire_with_lock_classified(
        run_dir: &Path,
        acquire_lock: impl FnOnce(&Path) -> Result<ForegroundLock, ForegroundLockAcquireError>,
    ) -> Result<Self, RuntimeOwnershipAcquireError> {
        ensure_directory(run_dir).map_err(RuntimeOwnershipAcquireError::Failed)?;
        #[cfg(unix)]
        let operation =
            lock_local_foreground_operation().map_err(RuntimeOwnershipAcquireError::Failed)?;
        let ownership = Self::acquire_lock_after_directory(run_dir, acquire_lock)?;
        ownership.reconcile_existing_while_serialized()?;
        #[cfg(unix)]
        drop(operation);
        Ok(ownership)
    }

    fn acquire_unreconciled_with_lock(
        run_dir: &Path,
        acquire_lock: impl FnOnce(&Path) -> Result<ForegroundLock, ForegroundLockAcquireError>,
    ) -> Result<Self, RuntimeOwnershipAcquireError> {
        ensure_directory(run_dir).map_err(RuntimeOwnershipAcquireError::Failed)?;
        #[cfg(unix)]
        let operation =
            lock_local_foreground_operation().map_err(RuntimeOwnershipAcquireError::Failed)?;
        let ownership = Self::acquire_lock_after_directory(run_dir, acquire_lock)?;
        #[cfg(unix)]
        drop(operation);
        Ok(ownership)
    }

    fn acquire_lock_after_directory(
        run_dir: &Path,
        acquire_lock: impl FnOnce(&Path) -> Result<ForegroundLock, ForegroundLockAcquireError>,
    ) -> Result<Self, RuntimeOwnershipAcquireError> {
        let lock_path = run_dir.join("foreground.lock");
        let foreground_lock = acquire_lock(&lock_path).map_err(|error| match error {
            ForegroundLockAcquireError::WouldBlock => RuntimeOwnershipAcquireError::Conflict,
            ForegroundLockAcquireError::Error(error) => RuntimeOwnershipAcquireError::Failed(error),
        })?;

        let ownership = Self {
            inner: Arc::new(Mutex::new(RuntimeOwnershipInner {
                _foreground_lock: foreground_lock,
                state_path: run_dir.join("foreground.json"),
                child_reserved: false,
            })),
        };
        Ok(ownership)
    }

    fn reconcile_existing_while_serialized(&self) -> Result<(), RuntimeOwnershipAcquireError> {
        let state_path = self
            .inner
            .lock()
            .map_err(|_| {
                RuntimeOwnershipAcquireError::Failed(
                    "runtime ownership state lock is poisoned".into(),
                )
            })?
            .state_path
            .clone();
        let run_dir = state_path.parent().ok_or_else(|| {
            RuntimeOwnershipAcquireError::Failed("runtime lease has no run-directory parent".into())
        })?;
        #[cfg(unix)]
        reconcile_interrupted_execution_builds(run_dir)?;
        reconcile_state(&state_path).map_err(RuntimeOwnershipAcquireError::Failed)?;
        #[cfg(unix)]
        reconcile_unleased_execution_stages(run_dir)?;
        Ok(())
    }

    /// Service startup uses a fail-closed recovery audit. Existing leases and
    /// prepared runtime artifacts are retained for explicit diagnosis because
    /// older Linux leases do not carry a sufficiently strong per-boot process
    /// identity for safe survivor signaling.
    pub(crate) fn audit_clean_for_service(&self) -> Result<(), RuntimeOwnershipAcquireError> {
        let state_path = self
            .inner
            .lock()
            .map_err(|_| {
                RuntimeOwnershipAcquireError::Failed(
                    "runtime ownership state lock is poisoned".into(),
                )
            })?
            .state_path
            .clone();
        if !lease_is_absent(&state_path) {
            read_lease(&state_path).map_err(|error| {
                RuntimeOwnershipAcquireError::Failed(format!(
                    "service recovery required; retained runtime lease: {error}"
                ))
            })?;
            return Err(RuntimeOwnershipAcquireError::Failed(
                "service recovery required; a prior runtime lease was retained".into(),
            ));
        }
        let legacy_state_path = state_path.with_file_name("managed.json");
        if !lease_is_absent(&legacy_state_path) {
            return Err(RuntimeOwnershipAcquireError::Failed(
                "service recovery required; legacy managed runtime state was retained".into(),
            ));
        }
        #[cfg(unix)]
        {
            let run_dir = state_path.parent().ok_or_else(|| {
                RuntimeOwnershipAcquireError::Failed(
                    "runtime lease has no run-directory parent".into(),
                )
            })?;
            if !crate::runtime_bundle::recoverable_execution_builds(run_dir)
                .map_err(RuntimeOwnershipAcquireError::Failed)?
                .is_empty()
                || !crate::runtime_bundle::recoverable_execution_stages(run_dir)
                    .map_err(RuntimeOwnershipAcquireError::Failed)?
                    .is_empty()
            {
                return Err(RuntimeOwnershipAcquireError::Failed(
                    "service recovery required; retained prepared runtime evidence".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn reserve_child(&self) -> Result<RuntimeChildOwnership, String> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| "runtime ownership state lock is poisoned".to_string())?;
        if inner.child_reserved {
            return Err("runtime ownership already has an active child".into());
        }
        inner.child_reserved = true;
        drop(inner);
        Ok(RuntimeChildOwnership {
            inner: Arc::clone(&self.inner),
            lease: None,
            release_on_drop: true,
        })
    }
}

impl RuntimeChildOwnership {
    pub(crate) fn child_spawned(&mut self) {
        self.release_on_drop = false;
    }

    pub(crate) fn record(
        &mut self,
        child_pid: u32,
        child_pgid: i32,
        model_id: &str,
        port: u16,
        managed_source: Option<&Path>,
        publication: RuntimeLeasePublication<'_>,
    ) -> Result<(), String> {
        let owner_pid = std::process::id();
        let owner = process_snapshot(owner_pid)?
            .ok_or_else(|| "failed to identify the Loxa process".to_string())?;
        let child = process_snapshot(child_pid)?
            .ok_or_else(|| "failed to identify the llama-server process".to_string())?;
        if process_group(child_pid)? != child_pgid {
            return Err("llama-server process group identity changed".into());
        }
        let (version, owner_mode, fingerprint, service) = match publication {
            RuntimeLeasePublication::Foreground => {
                (LEASE_VERSION, LeaseOwnerMode::Foreground, None, None)
            }
            RuntimeLeasePublication::PersistentApp(fingerprint) => {
                fingerprint.validate_persistent_lease(model_id)?;
                (
                    LEASE_VERSION,
                    LeaseOwnerMode::PersistentApp,
                    Some(fingerprint.clone()),
                    None,
                )
            }
            RuntimeLeasePublication::Service {
                fingerprint,
                endpoint,
            } => {
                fingerprint.validate_service_lease(model_id)?;
                (
                    SERVICE_LEASE_VERSION,
                    LeaseOwnerMode::Service,
                    Some(fingerprint.clone()),
                    Some(ServiceLeaseFields {
                        endpoint: endpoint.to_path_buf(),
                        parallel: 1,
                        offline: true,
                    }),
                )
            }
        };
        let lease = RuntimeLease {
            version,
            owner_mode: Some(owner_mode),
            fingerprint,
            managed_source: managed_source.map(Path::to_path_buf),
            owner_pid,
            owner_start_time: owner.start_identity,
            child_pid,
            child_start_time: child.start_identity,
            child_pgid,
            server: child.executable,
            model_id: model_id.to_owned(),
            port: if service.is_some() { 0 } else { port },
            service,
        };
        let state_path = self.state_path()?;
        let _state_guard = lock_lease_state()?;
        write_lease(&state_path, &lease)?;
        self.lease = Some(lease);
        Ok(())
    }

    pub(crate) fn clear(&mut self) -> Result<(), String> {
        let Some(expected) = self.lease.as_ref() else {
            self.release_on_drop = true;
            return Ok(());
        };
        let state_path = self.state_path()?;
        let _state_guard = lock_lease_state()?;
        match read_lease(&state_path) {
            Ok(current) if current == *expected => {
                cleanup_recorded_execution_stage(
                    state_path
                        .parent()
                        .expect("runtime lease has a run-directory parent"),
                    expected,
                )?;
                fs::remove_file(&state_path)
                    .map_err(|error| format!("{}: {error}", state_path.display()))?;
                self.lease = None;
                self.release_on_drop = true;
                Ok(())
            }
            Ok(_) => Err(format!(
                "runtime lease changed unexpectedly: {}",
                state_path.display()
            )),
            Err(_error) if lease_is_absent(&state_path) => {
                self.lease = None;
                self.release_on_drop = true;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn state_path(&self) -> Result<PathBuf, String> {
        self.inner
            .lock()
            .map(|inner| inner.state_path.clone())
            .map_err(|_| "runtime ownership state lock is poisoned".to_string())
    }
}

impl Drop for RuntimeChildOwnership {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.child_reserved = false;
        }
    }
}

#[cfg(unix)]
fn reconcile_interrupted_execution_builds(
    run_dir: &Path,
) -> Result<(), RuntimeOwnershipAcquireError> {
    let builds = crate::runtime_bundle::recoverable_execution_builds(run_dir)
        .map_err(RuntimeOwnershipAcquireError::Failed)?;
    for build in builds {
        let (owner_pid, owner_start) = build.owner();
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
        {
            continue;
        }
        build
            .ensure_current()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
        {
            continue;
        }
        build
            .cleanup()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
    }
    Ok(())
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
    cleanup_recorded_execution_stage(run_dir, &lease)?;
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
    if lease_is_absent(state_path) {
        return Ok(());
    }
    let bytes = read_regular_file(state_path)?;
    let stale = match decode_lease(&bytes) {
        Ok(lease) => lease,
        Err(_) => {
            return fs::remove_file(state_path)
                .map_err(|error| format!("{}: {error}", state_path.display()))
        }
    };
    if reconcile(
        &stale,
        state_path
            .parent()
            .expect("runtime lease has a run-directory parent"),
    )? {
        cleanup_recorded_execution_stage(
            state_path
                .parent()
                .expect("runtime lease has a run-directory parent"),
            &stale,
        )?;
    }
    fs::remove_file(state_path).map_err(|error| format!("{}: {error}", state_path.display()))
}

#[cfg(unix)]
fn reconcile_unleased_execution_stages(run_dir: &Path) -> Result<(), RuntimeOwnershipAcquireError> {
    let stages = crate::runtime_bundle::recoverable_execution_stages(run_dir)
        .map_err(RuntimeOwnershipAcquireError::Failed)?;
    for mut stage in stages {
        let (owner_pid, owner_start) = stage.owner();
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
            && !stage.is_abandoned()
        {
            continue;
        }
        if !stage
            .lock_and_refresh()
            .map_err(RuntimeOwnershipAcquireError::Failed)?
        {
            return Err(RuntimeOwnershipAcquireError::Conflict);
        }
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
            && !stage.is_abandoned()
        {
            continue;
        }
        let Some((child_pid, child_group)) = stage.child() else {
            stage
                .cleanup()
                .map_err(RuntimeOwnershipAcquireError::Failed)?;
            continue;
        };
        stage
            .ensure_current()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
        match process_snapshot(child_pid).map_err(RuntimeOwnershipAcquireError::Failed)? {
            Some(child) => {
                let observed_group =
                    process_group(child_pid).map_err(RuntimeOwnershipAcquireError::Failed)?;
                if child.executable == stage.server() && observed_group == child_group {
                    #[cfg(all(test, target_os = "macos"))]
                    if FAIL_NEXT_EXECUTION_STAGE_TERMINATION
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        return Err(RuntimeOwnershipAcquireError::Failed(
                            "injected prepared runtime termination failure".into(),
                        ));
                    }
                    terminate_stale_process_group(child_group)
                        .map_err(RuntimeOwnershipAcquireError::Failed)?;
                } else {
                    #[cfg(test)]
                    eprintln!(
                        "prepared recovery identity: executable={:?}, expected={:?}, group={observed_group}, expected_group={child_group}",
                        child.executable,
                        stage.server()
                    );
                    return Err(RuntimeOwnershipAcquireError::Failed(
                        "prepared runtime recovery child identity does not match".into(),
                    ));
                }
            }
            None if process_group_has_live_members(child_group)
                .map_err(RuntimeOwnershipAcquireError::Failed)? =>
            {
                return Err(RuntimeOwnershipAcquireError::Failed(
                    "prepared runtime recovery group has no matching leader".into(),
                ));
            }
            None => {}
        }
        if process_group_has_live_members(child_group)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
        {
            return Err(RuntimeOwnershipAcquireError::Failed(
                "prepared runtime recovery group is still active".into(),
            ));
        }
        stage
            .cleanup()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
    }
    Ok(())
}

#[cfg(all(test, target_os = "macos"))]
static FAIL_NEXT_EXECUTION_STAGE_TERMINATION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(all(test, target_os = "macos"))]
pub(crate) struct ExecutionStageTerminationFaultReset;

#[cfg(all(test, target_os = "macos"))]
impl Drop for ExecutionStageTerminationFaultReset {
    fn drop(&mut self) {
        FAIL_NEXT_EXECUTION_STAGE_TERMINATION.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(all(test, target_os = "macos"))]
pub(crate) fn fail_next_execution_stage_termination_for_test() -> ExecutionStageTerminationFaultReset
{
    FAIL_NEXT_EXECUTION_STAGE_TERMINATION.store(true, std::sync::atomic::Ordering::SeqCst);
    ExecutionStageTerminationFaultReset
}

fn reconcile(lease: &RuntimeLease, run_dir: &Path) -> Result<bool, String> {
    validate_lease(lease)?;
    #[cfg(unix)]
    let abandoned_stage = lease.managed_source.is_some()
        && crate::runtime_bundle::execution_stage_is_abandoned(run_dir, &lease.server)?;
    #[cfg(not(unix))]
    let abandoned_stage = {
        let _ = run_dir;
        false
    };
    if process_snapshot(lease.owner_pid)?
        .is_some_and(|owner| owner.start_identity == lease.owner_start_time)
        && !abandoned_stage
    {
        return Err("another Loxa runtime owns the recorded llama-server".into());
    }
    if let Some(child) = process_snapshot(lease.child_pid)? {
        if child.start_identity != lease.child_start_time
            || child.executable != lease.server
            || process_group(lease.child_pid)? != lease.child_pgid
        {
            return Ok(false);
        }
    } else if !process_group_exists(lease.child_pgid)? {
        return Ok(true);
    }
    terminate_stale_process_group(lease.child_pgid)?;
    Ok(true)
}

fn cleanup_recorded_execution_stage(run_dir: &Path, lease: &RuntimeLease) -> Result<(), String> {
    if lease.managed_source.is_none() {
        return Ok(());
    }
    if process_group_has_live_members(lease.child_pgid)? {
        return Err("llama-server process group is still active during stage cleanup".into());
    }
    #[cfg(unix)]
    {
        crate::runtime_bundle::cleanup_execution_stage(run_dir, &lease.server)
    }
    #[cfg(not(unix))]
    {
        let _ = (run_dir, lease);
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
    decode_lease(&read_regular_file(path)?).map_err(|error| format!("{}: {error}", path.display()))
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options
        .open(path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    let opened = crate::safe_file::regular_file_identity(&file, path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    let mut bytes = Vec::with_capacity(4 * 1024);
    (&mut file)
        .take((MAX_RUNTIME_RECORD_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    crate::safe_file::ensure_descriptor_matches_path(&file, &opened, path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    if bytes.len() > MAX_RUNTIME_RECORD_BYTES {
        return Err(format!(
            "runtime record exceeds the {MAX_RUNTIME_RECORD_BYTES}-byte limit: {}",
            path.display()
        ));
    }
    Ok(bytes)
}

fn map_runtime_lease_read_error(path: &Path, error: std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::InvalidData {
        format!("unsafe runtime lease {}", path.display())
    } else {
        format!("{}: {error}", path.display())
    }
}

fn write_lease(path: &Path, lease: &RuntimeLease) -> Result<(), String> {
    let temporary = path.with_extension("json.tmp");
    let bytes = encode_lease(lease)?;
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

#[cfg(test)]
mod tests;
