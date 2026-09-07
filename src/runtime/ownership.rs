use super::lease::{
    LeaseOwnerMode, RuntimeLease, ServiceLeaseFields, LEASE_VERSION, SERVICE_LEASE_VERSION,
};
#[cfg(unix)]
use super::lock::lock_local_foreground_operation;
use super::lock::{ForegroundLock, ForegroundLockAcquireError};
use super::process::{process_group, process_snapshot};
use super::record::{
    cleanup_recorded_execution_stage, ensure_directory, lease_is_absent, lock_lease_state,
    read_lease, write_lease,
};
use super::recovery::reconcile_state;
#[cfg(unix)]
use super::recovery::stages::{
    reconcile_interrupted_execution_builds, reconcile_unleased_execution_stages,
};
use crate::runtime_fingerprint::RuntimeFingerprint;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(crate) enum RuntimeLeasePublication<'a> {
    Foreground,
    PersistentApp(&'a RuntimeFingerprint),
    Service {
        fingerprint: &'a RuntimeFingerprint,
        endpoint: &'a Path,
    },
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
    pub(super) fn acquire_forced_traditional(run_dir: &Path) -> Result<Self, String> {
        Self::acquire_with_lock(run_dir, ForegroundLock::acquire_forced_traditional)
    }

    #[cfg(all(test, unix))]
    pub(super) fn acquire_forced_traditional_with_after_file_close(
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
