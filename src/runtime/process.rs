use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Child;
use std::thread;
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System, UpdateKind};

const STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(all(test, unix))]
std::thread_local! {
    static FAIL_NEXT_OWNED_GROUP_TERMINATIONS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(all(test, unix))]
pub(crate) struct OwnedGroupTerminationFaultReset {
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(all(test, unix))]
impl Drop for OwnedGroupTerminationFaultReset {
    fn drop(&mut self) {
        FAIL_NEXT_OWNED_GROUP_TERMINATIONS.with(|remaining| remaining.set(0));
    }
}

#[cfg(all(test, unix))]
pub(crate) fn fail_next_owned_group_terminations_for_test(
    count: usize,
) -> OwnedGroupTerminationFaultReset {
    FAIL_NEXT_OWNED_GROUP_TERMINATIONS.with(|remaining| remaining.set(count));
    OwnedGroupTerminationFaultReset {
        _not_send: std::marker::PhantomData,
    }
}

#[cfg(all(test, unix))]
fn inject_owned_group_termination_failure_for_test() -> Result<(), String> {
    let failed = FAIL_NEXT_OWNED_GROUP_TERMINATIONS.with(|remaining| {
        let Some(next) = remaining.get().checked_sub(1) else {
            return false;
        };
        remaining.set(next);
        true
    });
    if failed {
        Err("injected owned process-group termination failure".into())
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ProcessSnapshot {
    pub(super) start_identity: u64,
    pub(super) start_time_seconds: u64,
    pub(super) executable: PathBuf,
    pub(super) command: Vec<OsString>,
}

pub(super) fn process_snapshot(pid: u32) -> Result<Option<ProcessSnapshot>, String> {
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

pub(super) fn process_snapshot_from_refreshed_system(
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

pub(super) fn command_has_unique_option(
    command: &[OsString],
    option: &str,
    expected: &OsStr,
) -> bool {
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

pub(super) fn process_group(pid: u32) -> Result<i32, String> {
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
    #[cfg(test)]
    inject_owned_group_termination_failure_for_test()?;
    signal_process_group(group, libc::SIGTERM)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_has_live_members(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_process_group(group, libc::SIGKILL)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_has_live_members(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("owned process group survived SIGKILL".into())
}

#[cfg(unix)]
pub(crate) fn terminate_process_group_immediately(
    child: &mut Child,
    group: i32,
) -> Result<(), String> {
    #[cfg(test)]
    inject_owned_group_termination_failure_for_test()?;
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

pub(super) fn process_group_exists(group: i32) -> Result<bool, String> {
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

pub(crate) fn process_group_has_live_members(group: i32) -> Result<bool, String> {
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        // Linux exposes threads as tasks; retain them so a live worker remains
        // visible even when its process-group leader is already a zombie.
        ProcessRefreshKind::nothing().with_tasks(),
    );
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

#[cfg(all(test, unix))]
mod tests {
    use super::{
        fail_next_owned_group_terminations_for_test,
        inject_owned_group_termination_failure_for_test,
    };

    #[test]
    fn owned_group_termination_fault_is_thread_local() {
        let _fault = fail_next_owned_group_terminations_for_test(1);

        let spawned = std::thread::spawn(inject_owned_group_termination_failure_for_test)
            .join()
            .unwrap();

        assert_eq!(spawned, Ok(()));
        assert_eq!(
            inject_owned_group_termination_failure_for_test(),
            Err("injected owned process-group termination failure".into())
        );
        assert_eq!(inject_owned_group_termination_failure_for_test(), Ok(()));
    }
}
