//! Process snapshots and operating-system start identities.

use std::ffi::OsString;
use std::path::PathBuf;
#[cfg(all(test, unix))]
use std::process::Child;
#[cfg(all(test, unix))]
use std::time::{Duration, Instant};

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

#[cfg(target_os = "linux")]
// A CLOEXEC spawn acknowledgement can arrive before Linux installs the child executable.
pub(crate) fn process_has_execed(pid: u32) -> Result<bool, String> {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = match std::fs::read(&path) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    linux_process_stat_has_execed(&stat, &path)
}

#[cfg(target_os = "linux")]
fn linux_process_stat_has_execed(stat: &[u8], path: &std::path::Path) -> Result<bool, String> {
    let process_name_end = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(|| format!("malformed process status: {}", path.display()))?;
    let flags = stat[process_name_end + 1..]
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .nth(6)
        .ok_or_else(|| format!("missing process flags: {}", path.display()))?;
    let flags = std::str::from_utf8(flags)
        .map_err(|error| format!("invalid process flags in {}: {error}", path.display()))?
        .parse::<u64>()
        .map_err(|error| format!("invalid process flags in {}: {error}", path.display()))?;
    Ok(flags & libc::PF_FORKNOEXEC as u64 == 0)
}

#[cfg(all(test, unix))]
pub(crate) fn wait_for_test_process_executable(child: &mut Child, expected: &std::path::Path) {
    let expected = std::fs::canonicalize(expected).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = match child.try_wait() {
            Ok(status) => status,
            Err(error) => {
                stop_test_process(child);
                panic!("failed to inspect test process: {error}");
            }
        };
        if let Some(status) = status {
            stop_test_process(child);
            panic!("test process exited before exec synchronization: {status}");
        }
        #[cfg(target_os = "linux")]
        let child_has_execed = process_has_execed(child.id());
        #[cfg(not(target_os = "linux"))]
        let child_has_execed = Ok::<_, String>(true);
        let observation = match child_has_execed {
            Ok(true) => match process_snapshot(child.id()) {
                Ok(Some(snapshot)) if snapshot.executable == expected => return,
                Ok(observed) => format!("{observed:?}"),
                Err(error) => error,
            },
            Ok(false) => "process has not execed".into(),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            stop_test_process(child);
            panic!(
                "test process did not exec {} before the deadline: {observation}",
                expected.display()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(all(test, unix))]
fn stop_test_process(child: &mut Child) {
    if let Ok(group) = i32::try_from(child.id()) {
        // SAFETY: test fixtures start the child as the leader of its own process group.
        unsafe { libc::kill(-group, libc::SIGKILL) };
    }
    let _ = child.kill();
    let _ = child.wait();
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

#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    use super::linux_process_stat_has_execed;
    use std::path::Path;

    #[test]
    fn process_exec_state_accepts_non_utf8_names_and_tracks_fork_no_exec() {
        let path = Path::new("/proc/123/stat");
        let mut pending = b"123 (test ) name ".to_vec();
        pending.push(0xff);
        pending.extend_from_slice(b") R 1 2 3 4 5 64");
        assert!(!linux_process_stat_has_execed(&pending, path).unwrap());

        let mut execed = pending;
        execed.truncate(execed.len() - 2);
        execed.extend_from_slice(b"0");
        assert!(linux_process_stat_has_execed(&execed, path).unwrap());
    }

    #[test]
    fn process_exec_state_rejects_malformed_status() {
        let path = Path::new("/proc/123/stat");
        for malformed in [
            b"123 malformed".as_slice(),
            b"123 (name) R 1".as_slice(),
            b"123 (name) R 1 2 3 4 5 invalid".as_slice(),
            b"123 (name) R 1 2 3 4 5 18446744073709551616".as_slice(),
        ] {
            assert!(linux_process_stat_has_execed(malformed, path).is_err());
        }
    }
}
