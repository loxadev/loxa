#![allow(
    dead_code,
    reason = "persistent attachment is consumed by the follow-on session task"
)]

use super::{
    foreground_lock_is_held, lease_is_absent, lock_lease_state, process_group, process_snapshot,
    read_lease, LeaseOwnerMode, ProcessSnapshot, RuntimeLease, LEASE_VERSION,
};
use crate::runtime_fingerprint::RuntimeFingerprint;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub(crate) enum PersistentRuntimeLookup {
    NoRuntime,
    ActiveButNotAttachable,
    Attached(AttachedRuntime),
}

pub(crate) struct AttachedRuntime {
    lock_path: PathBuf,
    lease_path: PathBuf,
    expected_lease: Box<RuntimeLease>,
    expected_managed_server: PathBuf,
    expected_argv: Vec<OsString>,
}

impl AttachedRuntime {
    pub(crate) fn port(&self) -> u16 {
        self.expected_lease.port
    }

    pub(crate) fn model_id(&self) -> &str {
        &self.expected_lease.model_id
    }

    pub(crate) fn revalidate(&self) -> Result<(), String> {
        validate_attachment_identity(self)
    }
}

pub(crate) fn lookup_persistent_runtime(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprint: &RuntimeFingerprint,
) -> PersistentRuntimeLookup {
    lookup_persistent_runtime_with(
        run_dir,
        models_root,
        managed_server,
        expected_fingerprint,
        validate_attachment_identity,
        crate::runner::probe_model_alias,
    )
}

#[cfg(test)]
fn lookup_persistent_runtime_with_probe(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprint: &RuntimeFingerprint,
    identity_probe: impl Fn(&AttachedRuntime) -> Result<(), String>,
    probe: impl FnOnce(u16, &str) -> Result<bool, String>,
) -> PersistentRuntimeLookup {
    lookup_persistent_runtime_with(
        run_dir,
        models_root,
        managed_server,
        expected_fingerprint,
        identity_probe,
        probe,
    )
}

fn lookup_persistent_runtime_with(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprint: &RuntimeFingerprint,
    identity_probe: impl Fn(&AttachedRuntime) -> Result<(), String>,
    probe: impl FnOnce(u16, &str) -> Result<bool, String>,
) -> PersistentRuntimeLookup {
    let lock_path = run_dir.join("foreground.lock");
    let lease_path = run_dir.join("foreground.json");
    if matches!(foreground_lock_is_held(&lock_path), Ok(false)) && lease_is_absent(&lease_path) {
        return PersistentRuntimeLookup::NoRuntime;
    }
    if !matches!(foreground_lock_is_held(&lock_path), Ok(true)) {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    let lease = match read_lease(&lease_path) {
        Ok(lease) => lease,
        Err(_) => return PersistentRuntimeLookup::ActiveButNotAttachable,
    };
    if !lease_matches_attachment_expectation(&lease, managed_server, expected_fingerprint) {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    let expected_argv = match crate::runner::build_persistent_args_for_fingerprint(
        models_root,
        expected_fingerprint,
        lease.port,
    ) {
        Ok(argv) => argv,
        Err(_) => return PersistentRuntimeLookup::ActiveButNotAttachable,
    };
    let attached = AttachedRuntime {
        lock_path,
        lease_path,
        expected_lease: Box::new(lease),
        expected_managed_server: managed_server.to_path_buf(),
        expected_argv,
    };
    if identity_probe(&attached).is_err() {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    if !matches!(probe(attached.port(), attached.model_id()), Ok(true)) {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    if identity_probe(&attached).is_err() {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    PersistentRuntimeLookup::Attached(attached)
}

fn lease_matches_attachment_expectation(
    lease: &RuntimeLease,
    managed_server: &Path,
    expected_fingerprint: &RuntimeFingerprint,
) -> bool {
    lease.version == LEASE_VERSION
        && lease.owner_mode == Some(LeaseOwnerMode::PersistentApp)
        && lease.fingerprint.as_ref() == Some(expected_fingerprint)
        && lease.server == managed_server
        && lease.model_id == expected_fingerprint.model_id()
}

fn validate_attachment_identity(attached: &AttachedRuntime) -> Result<(), String> {
    validate_attachment_identity_with(attached, process_snapshot, process_group)
}

fn validate_attachment_identity_with(
    attached: &AttachedRuntime,
    process_probe: impl Fn(u32) -> Result<Option<ProcessSnapshot>, String>,
    process_group_probe: impl Fn(u32) -> Result<i32, String>,
) -> Result<(), String> {
    require_held_runtime_lock(&attached.lock_path)?;
    require_exact_runtime_lease(&attached.lease_path, &attached.expected_lease)?;

    let owner = process_probe(attached.expected_lease.owner_pid)?
        .ok_or_else(|| "persistent runtime owner is no longer running".to_string())?;
    if owner.start_identity != attached.expected_lease.owner_start_time {
        return Err("persistent runtime owner identity changed".into());
    }
    let child = process_probe(attached.expected_lease.child_pid)?
        .ok_or_else(|| "persistent runtime child is no longer running".to_string())?;
    if child.start_identity != attached.expected_lease.child_start_time {
        return Err("persistent runtime child identity changed".into());
    }
    if attached.expected_lease.server != attached.expected_managed_server
        || child.executable != attached.expected_managed_server
    {
        return Err("persistent runtime executable changed".into());
    }
    if attached.expected_lease.child_pgid
        != i32::try_from(attached.expected_lease.child_pid).unwrap_or(-1)
        || process_group_probe(attached.expected_lease.child_pid)?
            != attached.expected_lease.child_pgid
    {
        return Err("persistent runtime process group changed".into());
    }
    if child.command.len() != attached.expected_argv.len() + 1
        || child.command.get(1..) != Some(attached.expected_argv.as_slice())
    {
        return Err("persistent runtime arguments changed".into());
    }

    require_exact_runtime_lease(&attached.lease_path, &attached.expected_lease)?;
    require_held_runtime_lock(&attached.lock_path)
}

fn require_held_runtime_lock(path: &Path) -> Result<(), String> {
    match foreground_lock_is_held(path) {
        Ok(true) => Ok(()),
        Ok(false) => Err("persistent runtime lock is not held".into()),
        Err(error) => Err(error),
    }
}

fn require_exact_runtime_lease(path: &Path, expected: &RuntimeLease) -> Result<(), String> {
    let _state_guard = lock_lease_state()?;
    if read_lease(path)? == *expected {
        Ok(())
    } else {
        Err("persistent runtime lease changed".into())
    }
}
#[cfg(test)]
mod tests;
