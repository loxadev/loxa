use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessStatus, ProcessesToUpdate, System};

const LEASE_VERSION: u32 = 1;
const STOP_TIMEOUT: Duration = Duration::from_secs(2);
static LEASE_STATE_IO: Mutex<()> = Mutex::new(());

fn lock_lease_state() -> Result<MutexGuard<'static, ()>, String> {
    LEASE_STATE_IO
        .lock()
        .map_err(|_| "runtime lease state lock is poisoned".to_string())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLease {
    version: u32,
    owner_pid: u32,
    owner_start_time: u64,
    child_pid: u32,
    child_start_time: u64,
    child_pgid: i32,
    server: PathBuf,
    model_id: String,
    port: u16,
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

#[derive(Debug)]
struct ProcessSnapshot {
    start_identity: u64,
    start_time_seconds: u64,
    executable: PathBuf,
    command: Vec<OsString>,
}

pub(crate) struct RuntimeOwnership {
    _lock: File,
    state_path: PathBuf,
    lease: Option<RuntimeLease>,
}

impl RuntimeOwnership {
    pub(crate) fn acquire(run_dir: &Path) -> Result<Self, String> {
        ensure_directory(run_dir)?;
        let lock_path = run_dir.join("foreground.lock");
        let lock = open_lock(&lock_path)?;
        lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => "another Loxa runtime is active".into(),
            TryLockError::Error(error) => format!("{}: {error}", lock_path.display()),
        })?;

        let state_path = run_dir.join("foreground.json");
        reconcile_state(&state_path)?;

        Ok(Self {
            _lock: lock,
            state_path,
            lease: None,
        })
    }

    pub(crate) fn record(
        &mut self,
        child_pid: u32,
        child_pgid: i32,
        model_id: &str,
        port: u16,
    ) -> Result<(), String> {
        let owner_pid = std::process::id();
        let owner = process_snapshot(owner_pid)?
            .ok_or_else(|| "failed to identify the Loxa process".to_string())?;
        let child = process_snapshot(child_pid)?
            .ok_or_else(|| "failed to identify the llama-server process".to_string())?;
        if process_group(child_pid)? != child_pgid {
            return Err("llama-server process group identity changed".into());
        }
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid,
            owner_start_time: owner.start_identity,
            child_pid,
            child_start_time: child.start_identity,
            child_pgid,
            server: child.executable,
            model_id: model_id.to_owned(),
            port,
        };
        let _state_guard = lock_lease_state()?;
        write_lease(&self.state_path, &lease)?;
        self.lease = Some(lease);
        Ok(())
    }

    pub(crate) fn clear(&mut self) -> Result<(), String> {
        let Some(expected) = self.lease.take() else {
            return Ok(());
        };
        let _state_guard = lock_lease_state()?;
        match read_lease(&self.state_path) {
            Ok(current) if current == expected => fs::remove_file(&self.state_path)
                .map_err(|error| format!("{}: {error}", self.state_path.display())),
            Ok(_) => Err(format!(
                "runtime lease changed unexpectedly: {}",
                self.state_path.display()
            )),
            Err(_error) if !self.state_path.exists() => Ok(()),
            Err(error) => Err(error),
        }
    }
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
    let lock = open_lock(&lock_path)?;
    match lock.try_lock() {
        Ok(()) => {
            reconcile_state(&run_dir.join("foreground.json"))?;
            reconcile_legacy_state(&run_dir.join("managed.json"))
        }
        Err(TryLockError::WouldBlock) => Ok(()),
        Err(TryLockError::Error(error)) => Err(format!("{}: {error}", lock_path.display())),
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
    if !state_path.exists() {
        return Ok(());
    }
    let stale = read_lease(state_path)?;
    reconcile(&stale)?;
    fs::remove_file(state_path).map_err(|error| format!("{}: {error}", state_path.display()))
}

fn reconcile(lease: &RuntimeLease) -> Result<(), String> {
    validate_lease(lease)?;
    if process_snapshot(lease.owner_pid)?
        .is_some_and(|owner| owner.start_identity == lease.owner_start_time)
    {
        return Err("another Loxa runtime owns the recorded llama-server".into());
    }
    if let Some(child) = process_snapshot(lease.child_pid)? {
        if child.start_identity != lease.child_start_time
            || child.executable != lease.server
            || process_group(lease.child_pid)? != lease.child_pgid
        {
            return Ok(());
        }
    } else if !process_group_exists(lease.child_pgid)? {
        return Ok(());
    }
    terminate_stale_process_group(lease.child_pgid)
}

fn validate_lease(lease: &RuntimeLease) -> Result<(), String> {
    if lease.version != LEASE_VERSION
        || lease.owner_pid == 0
        || lease.child_pid == 0
        || lease.child_pgid <= 1
        || lease.child_pgid != i32::try_from(lease.child_pid).unwrap_or(-1)
        || lease.owner_start_time == 0
        || lease.child_start_time == 0
        || lease.server.as_os_str().is_empty()
        || lease.model_id.is_empty()
        || lease.port == 0
    {
        Err("invalid runtime lease".into())
    } else {
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
    let lease: RuntimeLease = serde_json::from_slice(&read_regular_file(path)?)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    validate_lease(&lease)?;
    Ok(lease)
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe runtime lease {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe runtime lease {}", path.display()));
        }
    }
    fs::read(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn write_lease(path: &Path, lease: &RuntimeLease) -> Result<(), String> {
    validate_lease(lease)?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(lease).map_err(|error| error.to_string())?;
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

fn process_snapshot(pid: u32) -> Result<Option<ProcessSnapshot>, String> {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
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

fn command_has_unique_option(command: &[OsString], option: &str, expected: &OsStr) -> bool {
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

fn process_group(pid: u32) -> Result<i32, String> {
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
    signal_process_group(group, libc::SIGTERM)?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_exists(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
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

fn process_group_exists(group: i32) -> Result<bool, String> {
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

fn process_group_has_live_members(group: i32) -> Result<bool, String> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    fn spawn_sleep() -> Child {
        let mut command = Command::new("/bin/sleep");
        command.arg("60");
        command.process_group(0);
        command.spawn().unwrap()
    }

    fn wait_until_gone(pid: u32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if process_snapshot(pid).is_ok_and(|snapshot| snapshot.is_none()) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[test]
    fn general_recovery_stops_an_exact_orphaned_child() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: 1,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity,
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        write_lease(&dir.path().join("foreground.json"), &lease).unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(wait_until_gone(pid), "exact orphan survived recovery");
        assert!(!dir.path().join("foreground.json").exists());
        let _ = child.wait();
    }

    #[test]
    fn signal_cleanup_removes_the_exact_owned_lease_after_group_termination() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let group = i32::try_from(pid).unwrap();
        let child_snapshot = process_snapshot(pid).unwrap().unwrap();
        let owner_snapshot = process_snapshot(std::process::id()).unwrap().unwrap();
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid: std::process::id(),
            owner_start_time: owner_snapshot.start_identity,
            child_pid: pid,
            child_start_time: child_snapshot.start_identity,
            child_pgid: group,
            server: child_snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        let state_path = dir.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();

        terminate_stale_process_group(group).unwrap();
        clear_terminated_owned_lease(dir.path(), pid, group).unwrap();

        assert!(!state_path.exists());
        let _ = child.wait();
    }

    #[test]
    fn signal_cleanup_preserves_a_foreign_lease() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let group = i32::try_from(pid).unwrap();
        let child_snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: LEASE_VERSION,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: child_snapshot.start_identity,
            child_pgid: group,
            server: child_snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        let state_path = dir.path().join("foreground.json");
        write_lease(&state_path, &lease).unwrap();

        terminate_stale_process_group(group).unwrap();
        let error = clear_terminated_owned_lease(dir.path(), pid, group).unwrap_err();

        assert!(
            error.contains("runtime lease changed unexpectedly"),
            "{error}"
        );
        assert!(state_path.exists());
        let _ = child.wait();
    }

    #[test]
    fn acquiring_runtime_never_signals_a_reused_process_identity() {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version: 1,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity.saturating_add(1),
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
        };
        write_lease(&dir.path().join("foreground.json"), &lease).unwrap();

        let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();

        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "reused PID was signaled"
        );
        assert!(!dir.path().join("foreground.json").exists());
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
        drop(ownership);
    }

    #[test]
    fn legacy_recovery_never_signals_without_unique_command_identity() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let server = dir.path().join("llama-server");
        fs::copy("/bin/sleep", &server).unwrap();
        fs::set_permissions(&server, fs::Permissions::from_mode(0o755)).unwrap();
        let mut command = Command::new(&server);
        command.arg("60").process_group(0);
        let mut child = command.spawn().unwrap();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "schema_version": 4,
                "run_id": "legacy",
                "model_id": "demo",
                "owner_pid": u32::MAX,
                "owner_process_start_time_unix_s": 1,
                "stop_requested": false,
                "lifecycle": "running",
                "generation": 1,
                "generation_alias": "legacy-g1",
                "control_port": 11436,
                "port": 1234,
                "log_path": "/tmp/legacy.log",
                "child_pid": pid,
                "child_process_start_time_unix_s": snapshot.start_time_seconds,
                "child_pgid": pid
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "ambiguous legacy process was signaled"
        );
        assert!(!dir.path().join("managed.json").exists());
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
    }

    #[test]
    fn legacy_command_identity_requires_exact_alias_and_port_pairs() {
        let command = [
            OsString::from("llama-server"),
            OsString::from("--alias"),
            OsString::from("legacy-g1"),
            OsString::from("--port"),
            OsString::from("1234"),
        ];

        assert!(command_has_unique_option(
            &command,
            "--alias",
            OsStr::new("legacy-g1")
        ));
        assert!(command_has_unique_option(
            &command,
            "--port",
            OsStr::new("1234")
        ));
        assert!(!command_has_unique_option(
            &command,
            "--alias",
            OsStr::new("other")
        ));
        let duplicate = [
            command.as_slice(),
            &[OsString::from("--alias"), OsString::from("other")],
        ]
        .concat();
        assert!(!command_has_unique_option(
            &duplicate,
            "--alias",
            OsStr::new("legacy-g1")
        ));
    }

    #[test]
    fn recovery_accepts_a_stale_unloaded_legacy_record() {
        let dir = tempdir().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "owner_pid": u32::MAX,
                "owner_process_start_time_unix_s": 1,
                "child_pid": null,
                "child_process_start_time_unix_s": null,
                "child_pgid": null
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        recover_stale(dir.path()).unwrap();

        assert!(!dir.path().join("managed.json").exists());
    }

    #[test]
    fn recovery_blocks_a_second_runtime_while_a_legacy_owner_is_alive() {
        let dir = tempdir().unwrap();
        let owner_pid = std::process::id();
        let owner = process_snapshot(owner_pid).unwrap().unwrap();
        let legacy = serde_json::json!({
            "schema_version": 4,
            "runs": [{
                "owner_pid": owner_pid,
                "owner_process_start_time_unix_s": owner.start_time_seconds,
                "child_pid": null,
                "child_process_start_time_unix_s": null,
                "child_pgid": null
            }]
        });
        fs::write(
            dir.path().join("managed.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        let error = recover_stale(dir.path()).unwrap_err();

        assert!(error.contains("legacy llama-server"), "{error}");
        assert!(dir.path().join("managed.json").exists());
    }

    #[test]
    fn stale_cleanup_waits_for_term_resistant_descendants() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("descendant-ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(format!(
                "(trap '' TERM; printf ready > '{}'; while :; do sleep 1; done) & wait",
                marker.display()
            ))
            .process_group(0);
        let mut child = command.spawn().unwrap();
        let group = i32::try_from(child.id()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !marker.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(marker.exists(), "descendant did not start");

        terminate_stale_process_group(group).unwrap();

        assert!(!process_group_has_live_members(group).unwrap());
        let _ = child.wait();
    }
}
