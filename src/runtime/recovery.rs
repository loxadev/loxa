use super::lease::{decode_lease, validate_lease, RuntimeLease};
#[cfg(unix)]
use super::lock::{lock_local_foreground_operation, LocalForegroundLock};
use super::lock::{ForegroundLock, ForegroundLockAcquireError};
use super::process::{
    command_has_unique_option, process_group, process_group_exists, process_snapshot,
    terminate_stale_process_group,
};
use super::record::{
    cleanup_recorded_execution_stage, ensure_directory, lease_is_absent, read_regular_file,
};
use serde::Deserialize;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;

#[cfg(unix)]
pub(super) mod stages;

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
        reconcile_legacy_orphan(&run, state_path)?;
    }

    if active_owner {
        return Err("another Loxa runtime owns the legacy llama-server".into());
    }
    fs::remove_file(state_path).map_err(|error| format!("{}: {error}", state_path.display()))?;
    Ok(())
}

fn reconcile_legacy_orphan(run: &LegacyRun, state_path: &Path) -> Result<(), String> {
    let (child_pid, child_start_time, child_pgid) = match (
        run.child_pid,
        run.child_process_start_time_unix_s,
        run.child_pgid,
    ) {
        (None, None, None) => return Ok(()),
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
        return Ok(());
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
    Ok(())
}

pub(super) fn reconcile_state(state_path: &Path) -> Result<(), String> {
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
