use super::{
    foreground_lock_is_held, lease_is_absent, lock_lease_state, process_group,
    process_snapshot_from_refreshed_system, read_lease, LeaseOwnerMode, ProcessSnapshot,
    RuntimeLease, LEASE_VERSION, PERSISTENT_LEASE_VERSION,
};
use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
use std::cell::RefCell;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

pub(crate) enum PersistentRuntimeLookup {
    NoRuntime,
    ActiveButNotAttachable,
    Attached(AttachedRuntime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimePresence {
    NoRuntime,
    Active,
}

enum RuntimeState {
    NoRuntime,
    Held,
    OtherActive,
}

pub(crate) struct AttachedRuntime {
    lock_path: PathBuf,
    lease_path: PathBuf,
    expected_lease: Box<RuntimeLease>,
    expected_managed_server: PathBuf,
    expected_argv: Vec<OsString>,
    processes: RefCell<Box<System>>,
    #[cfg(test)]
    test_revalidate: Option<Box<dyn Fn() -> Result<(), String> + Send + Sync>>,
    #[cfg(test)]
    test_owner_control: Option<Box<dyn Fn() -> Result<(), String> + Send + Sync>>,
    #[cfg(test)]
    normalize_bsd_yes_argv: bool,
}

impl AttachedRuntime {
    pub(crate) fn port(&self) -> u16 {
        self.expected_lease.port
    }

    pub(crate) fn model_id(&self) -> &str {
        &self.expected_lease.model_id
    }

    pub(crate) fn revalidate(&self) -> Result<(), String> {
        #[cfg(test)]
        if let Some(revalidate) = &self.test_revalidate {
            return revalidate();
        }
        validate_attachment_identity(self)
    }

    #[cfg(test)]
    pub(crate) fn for_session_test(
        model_id: &str,
        port: u16,
        revalidate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self::for_session_test_with_owner_witness(model_id, port, revalidate, || Ok(()))
    }

    #[cfg(test)]
    pub(crate) fn for_session_test_with_owner_witness(
        model_id: &str,
        port: u16,
        revalidate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
        owner_control: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            lock_path: PathBuf::new(),
            lease_path: PathBuf::new(),
            expected_lease: Box::new(RuntimeLease {
                version: LEASE_VERSION,
                owner_mode: Some(LeaseOwnerMode::PersistentApp),
                fingerprint: None,
                managed_source: None,
                owner_pid: 1,
                owner_start_time: 1,
                child_pid: 2,
                child_start_time: 2,
                child_pgid: 2,
                server: PathBuf::new(),
                model_id: model_id.to_owned(),
                port,
            }),
            expected_managed_server: PathBuf::new(),
            expected_argv: Vec::new(),
            processes: RefCell::new(Box::new(System::new())),
            test_revalidate: Some(Box::new(revalidate)),
            test_owner_control: Some(Box::new(owner_control)),
            normalize_bsd_yes_argv: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn normalize_bsd_yes_argv_for_test(&mut self) {
        assert_eq!(self.expected_managed_server, Path::new("/usr/bin/yes"));
        self.normalize_bsd_yes_argv = true;
    }

    #[cfg(test)]
    pub(crate) fn control_owner_for_session_test(&self) -> Result<(), String> {
        self.test_owner_control
            .as_ref()
            .expect("session-test attachment has an owner-control witness")()
    }
}

pub(crate) fn lookup_persistent_runtime(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprints: &[RuntimeFingerprint],
) -> PersistentRuntimeLookup {
    lookup_persistent_runtime_with(
        run_dir,
        models_root,
        managed_server,
        expected_fingerprints,
        validate_attachment_identity,
        crate::runner::probe_model_alias,
    )
}

pub(crate) fn lookup_runtime_presence(run_dir: &Path) -> RuntimePresence {
    match runtime_state(run_dir) {
        RuntimeState::NoRuntime => RuntimePresence::NoRuntime,
        RuntimeState::Held | RuntimeState::OtherActive => RuntimePresence::Active,
    }
}

#[cfg(test)]
fn lookup_persistent_runtime_with_probe(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprints: &[RuntimeFingerprint],
    identity_probe: impl Fn(&AttachedRuntime) -> Result<(), String>,
    probe: impl FnOnce(u16, &str) -> Result<bool, String>,
) -> PersistentRuntimeLookup {
    lookup_persistent_runtime_with(
        run_dir,
        models_root,
        managed_server,
        expected_fingerprints,
        identity_probe,
        probe,
    )
}

fn lookup_persistent_runtime_with(
    run_dir: &Path,
    models_root: &Path,
    managed_server: &Path,
    expected_fingerprints: &[RuntimeFingerprint],
    identity_probe: impl Fn(&AttachedRuntime) -> Result<(), String>,
    probe: impl FnOnce(u16, &str) -> Result<bool, String>,
) -> PersistentRuntimeLookup {
    if !expected_fingerprints_are_closed(expected_fingerprints) {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    }
    match runtime_state(run_dir) {
        RuntimeState::NoRuntime => return PersistentRuntimeLookup::NoRuntime,
        RuntimeState::OtherActive => return PersistentRuntimeLookup::ActiveButNotAttachable,
        RuntimeState::Held => {}
    }
    let lock_path = run_dir.join("foreground.lock");
    let lease_path = run_dir.join("foreground.json");
    let lease = match read_lease(&lease_path) {
        Ok(lease) => lease,
        Err(_) => return PersistentRuntimeLookup::ActiveButNotAttachable,
    };
    let Some(expected_fingerprint) = expected_fingerprints
        .iter()
        .find(|candidate| lease_matches_attachment_expectation(&lease, managed_server, candidate))
    else {
        return PersistentRuntimeLookup::ActiveButNotAttachable;
    };
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
        processes: RefCell::new(Box::new(System::new())),
        #[cfg(test)]
        test_revalidate: None,
        #[cfg(test)]
        test_owner_control: None,
        #[cfg(test)]
        normalize_bsd_yes_argv: false,
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

fn runtime_state(run_dir: &Path) -> RuntimeState {
    let lock_path = run_dir.join("foreground.lock");
    match foreground_lock_is_held(&lock_path) {
        Ok(false) if lease_is_absent(&run_dir.join("foreground.json")) => RuntimeState::NoRuntime,
        Ok(true) => RuntimeState::Held,
        Ok(false) | Err(_) => RuntimeState::OtherActive,
    }
}

fn expected_fingerprints_are_closed(candidates: &[RuntimeFingerprint]) -> bool {
    if !(1..=2).contains(&candidates.len())
        || candidates.iter().any(|candidate| {
            candidate
                .validate_persistent_lease(candidate.model_id())
                .is_err()
        })
    {
        return false;
    }
    match candidates {
        [exact] => exact.effective_profile() == EffectiveProfile::Generic,
        [exact, fallback] => exact.primary_only().as_ref() == Some(fallback),
        _ => false,
    }
}

fn lease_matches_attachment_expectation(
    lease: &RuntimeLease,
    managed_server: &Path,
    expected_fingerprint: &RuntimeFingerprint,
) -> bool {
    matches!(lease.version, PERSISTENT_LEASE_VERSION | LEASE_VERSION)
        && lease.owner_mode == Some(LeaseOwnerMode::PersistentApp)
        && lease.fingerprint.as_ref() == Some(expected_fingerprint)
        && lease.attributed_managed_source() == managed_server
        && lease.model_id == expected_fingerprint.model_id()
}

fn validate_attachment_identity(attached: &AttachedRuntime) -> Result<(), String> {
    validate_attachment_identity_with(attached, refresh_attachment_processes, process_group)
}

fn validate_attachment_identity_with(
    attached: &AttachedRuntime,
    mut process_probe: impl FnMut(
        &mut System,
        u32,
        u32,
    )
        -> Result<(Option<ProcessSnapshot>, Option<ProcessSnapshot>), String>,
    process_group_probe: impl Fn(u32) -> Result<i32, String>,
) -> Result<(), String> {
    require_held_runtime_lock(&attached.lock_path)?;
    require_exact_runtime_lease(&attached.lease_path, &attached.expected_lease)?;

    let (owner, child) = {
        let mut processes = attached.processes.borrow_mut();
        process_probe(
            processes.as_mut(),
            attached.expected_lease.owner_pid,
            attached.expected_lease.child_pid,
        )?
    };
    let owner = owner.ok_or_else(|| "persistent runtime owner is no longer running".to_string())?;
    if owner.start_identity != attached.expected_lease.owner_start_time {
        return Err("persistent runtime owner identity changed".into());
    }
    let child = child.ok_or_else(|| "persistent runtime child is no longer running".to_string())?;
    #[cfg(test)]
    let child = {
        let mut child = child;
        if attached.normalize_bsd_yes_argv {
            child.command = std::iter::once(attached.expected_managed_server.clone().into())
                .chain(attached.expected_argv.iter().cloned())
                .collect();
        }
        child
    };
    if child.start_identity != attached.expected_lease.child_start_time {
        return Err("persistent runtime child identity changed".into());
    }
    if attached.expected_lease.attributed_managed_source() != attached.expected_managed_server
        || child.executable != attached.expected_lease.server
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

fn attachment_process_refresh_kind() -> ProcessRefreshKind {
    ProcessRefreshKind::nothing()
        .with_cmd(UpdateKind::Always)
        .with_exe(UpdateKind::Always)
}

fn refresh_attachment_processes(
    system: &mut System,
    owner_pid: u32,
    child_pid: u32,
) -> Result<(Option<ProcessSnapshot>, Option<ProcessSnapshot>), String> {
    let owner_pid = Pid::from_u32(owner_pid);
    let child_pid = Pid::from_u32(child_pid);
    let pids = [owner_pid, child_pid];
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&pids),
        true,
        attachment_process_refresh_kind(),
    );
    Ok((
        process_snapshot_from_refreshed_system(system, owner_pid)?,
        process_snapshot_from_refreshed_system(system, child_pid)?,
    ))
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
