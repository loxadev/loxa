use std::ffi::{OsStr, OsString};
use std::process::Child;
use std::thread;
use std::time::{Duration, Instant};

use sysinfo::{ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

#[cfg(unix)]
pub(crate) use crate::process_inspection::current_process_start_identity;
pub(super) use crate::process_inspection::{
    process_snapshot, process_snapshot_from_refreshed_system, ProcessSnapshot,
};

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
