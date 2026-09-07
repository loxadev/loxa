use crate::runtime_fingerprint::RuntimeFingerprint;
use serde::Deserialize;
use std::ffi::OsStr;
#[cfg(test)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

mod lease;
use lease::{
    decode_lease, validate_lease, LeaseOwnerMode, RuntimeLease, ServiceLeaseFields, LEASE_VERSION,
    SERVICE_LEASE_VERSION,
};
#[cfg(test)]
use lease::{
    encode_lease, encode_v3_lease, RuntimeLeaseV1, RuntimeLeaseV2, LEGACY_LEASE_VERSION,
    PERSISTENT_LEASE_VERSION,
};

mod process;
#[cfg(test)]
pub(crate) use process::fail_next_owned_group_terminations_for_test;
use process::{command_has_unique_option, process_group, process_group_exists, process_snapshot};
pub(crate) use process::{
    current_process_start_identity, process_group_has_live_members, terminate_process_group,
    terminate_process_group_immediately, terminate_stale_process_group,
};

mod lock;
#[cfg(test)]
use lock::{foreground_lock_is_held, foreground_lock_is_held_with_after_open, open_lock};
#[cfg(unix)]
use lock::{lock_local_foreground_operation, LocalForegroundLock};
#[cfg(all(test, unix))]
use lock::{
    set_foreground_lock_with_command, try_acquire_foreground_lock, LOCAL_FOREGROUND_LOCK_OPERATIONS,
};
use lock::{ForegroundLock, ForegroundLockAcquireError};

mod record;
pub(crate) use record::clear_terminated_owned_lease;
#[cfg(test)]
use record::MAX_RUNTIME_RECORD_BYTES;
use record::{
    cleanup_recorded_execution_stage, ensure_directory, lease_is_absent, lock_lease_state,
    read_lease, read_regular_file, write_lease,
};

pub(crate) enum RuntimeLeasePublication<'a> {
    Foreground,
    PersistentApp(&'a RuntimeFingerprint),
    Service {
        fingerprint: &'a RuntimeFingerprint,
        endpoint: &'a Path,
    },
}

mod observation;
pub(crate) use observation::{
    ForegroundObservation, ForegroundObserver, RuntimeOwner, RuntimeProvenance,
};

mod attachment;
pub(crate) use attachment::{
    lookup_persistent_runtime, lookup_runtime_presence, AttachedRuntime, PersistentRuntimeLookup,
    RuntimePresence,
};

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
                    Some(ServiceLeaseFields::qualified(endpoint)),
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

#[cfg(test)]
mod tests;
