//! Process snapshots and operating-system start identities.

use std::ffi::OsString;
use std::path::PathBuf;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

#[derive(Debug)]
pub(crate) struct ProcessSnapshot {
    pub(crate) start_identity: u64,
    pub(crate) start_time_seconds: u64,
    pub(crate) executable: PathBuf,
    pub(crate) command: Vec<OsString>,
}

pub(crate) fn process_snapshot(pid: u32) -> Result<Option<ProcessSnapshot>, String> {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_cmd(UpdateKind::OnlyIfNotSet)
            .with_exe(UpdateKind::OnlyIfNotSet),
    );
    process_snapshot_from_refreshed_system(&system, pid)
}

#[cfg(unix)]
pub(crate) fn current_process_start_identity() -> Result<u64, String> {
    let pid = std::process::id();
    process_snapshot(pid)?
        .map(|process| process.start_identity)
        .ok_or_else(|| "failed to identify the Loxa process".to_string())
}

pub(crate) fn process_snapshot_from_refreshed_system(
    system: &System,
    pid: Pid,
) -> Result<Option<ProcessSnapshot>, String> {
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
