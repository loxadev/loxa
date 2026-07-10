use super::SupervisorError;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const RUNTIME_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub const RUNTIME_STATE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);
pub const RUNTIME_STATE_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLifecycle {
    Starting,
    Running,
    Restarting,
    Stopping,
    RecoveryRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedRun {
    pub schema_version: u32,
    pub run_id: String,
    pub model_id: String,
    pub owner_pid: u32,
    pub owner_process_start_time_unix_s: u64,
    pub stop_requested: bool,
    pub lifecycle: RunLifecycle,
    pub generation: u32,
    pub generation_alias: String,
    pub port: u16,
    pub log_path: PathBuf,
    pub child_pid: Option<u32>,
    pub child_process_start_time_unix_s: Option<u64>,
    pub child_pgid: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedRunIdentity {
    pub run_id: String,
    pub generation: u32,
    pub child_pid: Option<u32>,
    pub child_process_start_time_unix_s: Option<u64>,
}

impl ManagedRun {
    pub fn identity(&self) -> ManagedRunIdentity {
        ManagedRunIdentity {
            run_id: self.run_id.clone(),
            generation: self.generation,
            child_pid: self.child_pid,
            child_process_start_time_unix_s: self.child_process_start_time_unix_s,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeStateEnvelope {
    schema_version: u32,
    runs: Vec<ManagedRun>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeStateRead {
    Missing,
    Loaded(Vec<ManagedRun>),
    Legacy,
    Corrupt(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StopRequestMatch {
    NoMatch,
    Requested(ManagedRun),
}

pub fn runtime_dir() -> PathBuf {
    home_dir().join(".loxa").join("run")
}

pub fn runtime_state_path() -> PathBuf {
    runtime_dir().join("managed.json")
}

pub fn runtime_logs_dir() -> PathBuf {
    runtime_dir().join("logs")
}

pub fn read_runtime_state(path: &Path) -> Result<RuntimeStateRead, SupervisorError> {
    match fs::read(path) {
        Ok(bytes) => {
            if bytes.iter().all(u8::is_ascii_whitespace) {
                return Ok(RuntimeStateRead::Missing);
            }

            if serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).is_ok() {
                return Ok(RuntimeStateRead::Legacy);
            }

            match serde_json::from_slice::<RuntimeStateEnvelope>(&bytes) {
                Ok(envelope) if envelope.schema_version != RUNTIME_STATE_SCHEMA_VERSION => {
                    Ok(RuntimeStateRead::Corrupt(format!(
                        "unsupported managed state schema version {}",
                        envelope.schema_version
                    )))
                }
                Ok(envelope) if envelope.runs.len() > 1 => Ok(RuntimeStateRead::Corrupt(
                    "managed state contains more than one active run".to_string(),
                )),
                Ok(envelope) => {
                    if let Some(message) = envelope
                        .runs
                        .iter()
                        .find_map(|run| validate_runtime_run(run).err())
                    {
                        Ok(RuntimeStateRead::Corrupt(message))
                    } else {
                        Ok(RuntimeStateRead::Loaded(envelope.runs))
                    }
                }
                Err(error) => Ok(RuntimeStateRead::Corrupt(error.to_string())),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(RuntimeStateRead::Missing),
        Err(error) => Err(SupervisorError::Io(error)),
    }
}

pub(super) fn write_runtime_state(path: &Path, runs: &[ManagedRun]) -> Result<(), SupervisorError> {
    write_runtime_state_with_hook(path, runs, |_| Ok(()))
}

fn write_runtime_state_with_hook<F>(
    path: &Path,
    runs: &[ManagedRun],
    before_rename: F,
) -> Result<(), SupervisorError>
where
    F: FnOnce(&Path) -> io::Result<()>,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    if runs.len() > 1 {
        return Err(SupervisorError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only one managed run is supported",
        )));
    }
    if let Some(message) = runs.iter().find_map(|run| validate_runtime_run(run).err()) {
        return Err(SupervisorError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            message,
        )));
    }

    let envelope = RuntimeStateEnvelope {
        schema_version: RUNTIME_STATE_SCHEMA_VERSION,
        runs: runs.to_vec(),
    };
    let bytes = serde_json::to_vec_pretty(&envelope)
        .map_err(|error| SupervisorError::Io(io::Error::new(io::ErrorKind::InvalidData, error)))?;
    let temp_path = temp_path_for(path);
    let result = (|| -> Result<(), SupervisorError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_all()?;
        before_rename(&temp_path)?;
        drop(file);
        fs::rename(&temp_path, path)?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

pub fn create_starting_run(path: &Path, run: ManagedRun) -> Result<ManagedRun, SupervisorError> {
    create_starting_run_with_lock_options(
        path,
        run,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )
}

fn create_starting_run_with_lock_options(
    path: &Path,
    run: ManagedRun,
    timeout: Duration,
    interval: Duration,
) -> Result<ManagedRun, SupervisorError> {
    validate_runtime_run(&run).map_err(SupervisorError::RunStateConflict)?;
    if run.lifecycle != RunLifecycle::Starting
        || run.child_pid.is_some()
        || run.child_process_start_time_unix_s.is_some()
        || run.child_pgid.is_some()
    {
        return Err(SupervisorError::RunStateConflict(
            "new run must be childless and in the starting lifecycle".to_string(),
        ));
    }
    let _lock = acquire_runtime_state_lock_for_mutation(path, timeout, interval)?;
    let runs = match read_runtime_state(path)? {
        RuntimeStateRead::Missing => Vec::new(),
        RuntimeStateRead::Loaded(runs) => runs,
        RuntimeStateRead::Legacy => {
            return Err(SupervisorError::LegacyRuntimeState(path.to_path_buf()))
        }
        RuntimeStateRead::Corrupt(message) => {
            return Err(SupervisorError::Io(io::Error::other(format!(
                "managed sidecar state is corrupt: {message}"
            ))))
        }
    };

    if let Some(existing) = runs.first() {
        return Err(SupervisorError::ActiveRun(existing.run_id.clone()));
    }

    write_runtime_state(path, std::slice::from_ref(&run))?;
    Ok(run)
}

pub fn update_runtime_state_run(
    path: &Path,
    expected: &ManagedRunIdentity,
    updated: ManagedRun,
) -> Result<bool, SupervisorError> {
    update_runtime_state_run_with_lock_options(
        path,
        expected,
        updated,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )
}

pub fn update_runtime_state_run_committed(
    path: &Path,
    expected: &ManagedRunIdentity,
    updated: ManagedRun,
) -> Result<Option<ManagedRun>, SupervisorError> {
    update_runtime_state_run_committed_with_lock_options_and_hook(
        path,
        expected,
        updated,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
        |_| Ok(()),
    )
}

fn update_runtime_state_run_with_lock_options(
    path: &Path,
    expected: &ManagedRunIdentity,
    updated: ManagedRun,
    timeout: Duration,
    interval: Duration,
) -> Result<bool, SupervisorError> {
    update_runtime_state_run_with_lock_options_and_hook(
        path,
        expected,
        updated,
        timeout,
        interval,
        |_| Ok(()),
    )
}

fn update_runtime_state_run_with_lock_options_and_hook<F>(
    path: &Path,
    expected: &ManagedRunIdentity,
    updated: ManagedRun,
    timeout: Duration,
    interval: Duration,
    before_rename: F,
) -> Result<bool, SupervisorError>
where
    F: FnOnce(&Path) -> io::Result<()>,
{
    Ok(
        update_runtime_state_run_committed_with_lock_options_and_hook(
            path,
            expected,
            updated,
            timeout,
            interval,
            before_rename,
        )?
        .is_some(),
    )
}

fn update_runtime_state_run_committed_with_lock_options_and_hook<F>(
    path: &Path,
    expected: &ManagedRunIdentity,
    mut updated: ManagedRun,
    timeout: Duration,
    interval: Duration,
    before_rename: F,
) -> Result<Option<ManagedRun>, SupervisorError>
where
    F: FnOnce(&Path) -> io::Result<()>,
{
    validate_runtime_run(&updated).map_err(SupervisorError::RunStateConflict)?;
    let _lock = acquire_runtime_state_lock_for_mutation(path, timeout, interval)?;
    let mut runs = runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Ok(None);
    };
    if current.identity() != *expected {
        return Ok(None);
    }
    if updated.run_id != current.run_id {
        return Err(SupervisorError::RunStateConflict(format!(
            "run ID changed from {} to {}",
            current.run_id, updated.run_id
        )));
    }

    updated.stop_requested |= current.stop_requested;
    runs[0] = updated.clone();
    write_runtime_state_with_hook(path, &runs, before_rename)?;
    Ok(Some(updated))
}

pub fn finish_runtime_state_run(
    path: &Path,
    expected: &ManagedRunIdentity,
) -> Result<bool, SupervisorError> {
    finish_runtime_state_run_with_lock_options(
        path,
        expected,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )
}

fn finish_runtime_state_run_with_lock_options(
    path: &Path,
    expected: &ManagedRunIdentity,
    timeout: Duration,
    interval: Duration,
) -> Result<bool, SupervisorError> {
    let _lock = acquire_runtime_state_lock_for_mutation(path, timeout, interval)?;
    let runs = runtime_state_runs_for_mutation(path)?;
    if runs.first().map(ManagedRun::identity).as_ref() != Some(expected) {
        return Ok(false);
    }

    write_runtime_state(path, &[])?;
    Ok(true)
}

pub(super) fn record_stop_request(
    path: &Path,
    target: &str,
) -> Result<StopRequestMatch, SupervisorError> {
    record_stop_request_with_lock_options_and_hook(
        path,
        target,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
        |_| Ok(()),
    )
}

pub(super) fn record_stop_request_with_lock_options_and_hook<F>(
    path: &Path,
    target: &str,
    timeout: Duration,
    interval: Duration,
    before_rename: F,
) -> Result<StopRequestMatch, SupervisorError>
where
    F: FnOnce(&Path) -> io::Result<()>,
{
    let _lock = acquire_runtime_state_lock_for_mutation(path, timeout, interval)?;
    let mut runs = runtime_state_runs_for_mutation(path)?;
    let Some(run) = runs.first_mut() else {
        return Ok(StopRequestMatch::NoMatch);
    };
    if target != "all" && run.model_id != target {
        return Ok(StopRequestMatch::NoMatch);
    }

    if !run.stop_requested {
        run.stop_requested = true;
        write_runtime_state_with_hook(path, &runs, before_rename)?;
    }
    Ok(StopRequestMatch::Requested(runs[0].clone()))
}

pub(super) fn stable_run_is_present(path: &Path, run_id: &str) -> Result<bool, SupervisorError> {
    match read_runtime_state(path)? {
        RuntimeStateRead::Missing => Ok(false),
        RuntimeStateRead::Loaded(runs) => Ok(runs.iter().any(|run| run.run_id == run_id)),
        RuntimeStateRead::Legacy => Err(SupervisorError::LegacyRuntimeState(path.to_path_buf())),
        RuntimeStateRead::Corrupt(message) => Err(SupervisorError::Io(io::Error::other(format!(
            "managed sidecar state is corrupt: {message}"
        )))),
    }
}

pub fn current_runtime_state_run(
    path: &Path,
    expected: &ManagedRunIdentity,
) -> Result<ManagedRun, SupervisorError> {
    let _lock = acquire_runtime_state_lock_for_mutation(
        path,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let runs = runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} is no longer present",
            expected.run_id, expected.generation
        )));
    };
    if current.identity() != *expected {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} no longer matches",
            expected.run_id, expected.generation
        )));
    }
    Ok(current.clone())
}

pub fn remove_runtime_state_entry(
    path: &Path,
    identity: &ManagedRunIdentity,
) -> Result<bool, SupervisorError> {
    finish_runtime_state_run(path, identity)
}

pub(super) fn runtime_state_runs_for_mutation(
    path: &Path,
) -> Result<Vec<ManagedRun>, SupervisorError> {
    match read_runtime_state(path)? {
        RuntimeStateRead::Missing => Ok(Vec::new()),
        RuntimeStateRead::Loaded(runs) => Ok(runs),
        RuntimeStateRead::Legacy => Err(SupervisorError::LegacyRuntimeState(path.to_path_buf())),
        RuntimeStateRead::Corrupt(message) => Err(SupervisorError::Io(io::Error::other(format!(
            "managed sidecar state is corrupt: {message}"
        )))),
    }
}

fn validate_runtime_run(run: &ManagedRun) -> Result<(), String> {
    if run.schema_version != RUNTIME_STATE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported managed run schema version {}",
            run.schema_version
        ));
    }
    if run.run_id.is_empty() {
        return Err("managed run ID must not be empty".to_string());
    }
    if run.model_id.is_empty() {
        return Err("managed run model ID must not be empty".to_string());
    }
    if run.generation_alias.is_empty() {
        return Err("managed run generation alias must not be empty".to_string());
    }
    if run.port == 0 {
        return Err("managed run port must not be zero".to_string());
    }
    if run.log_path.as_os_str().is_empty() {
        return Err("managed run log path must not be empty".to_string());
    }
    if run.child_pid.is_none()
        && (run.child_process_start_time_unix_s.is_some() || run.child_pgid.is_some())
    {
        return Err("managed run child metadata requires a child PID".to_string());
    }
    Ok(())
}

pub(super) struct RuntimeStateLock {
    _file: File,
}

pub(super) fn acquire_runtime_state_lock_for_mutation(
    state_path: &Path,
    timeout: Duration,
    interval: Duration,
) -> Result<RuntimeStateLock, SupervisorError> {
    reject_legacy_runtime_artifacts(state_path)?;
    let lock = RuntimeStateLock::acquire(state_path, timeout, interval)?;
    reject_legacy_runtime_artifacts(state_path)?;
    Ok(lock)
}

fn reject_legacy_runtime_artifacts(state_path: &Path) -> Result<(), SupervisorError> {
    let sentinel_path = legacy_runtime_state_lock_path(state_path);
    if sentinel_path.try_exists()? {
        return Err(SupervisorError::LegacyRuntimeState(sentinel_path));
    }
    if matches!(read_runtime_state(state_path)?, RuntimeStateRead::Legacy) {
        return Err(SupervisorError::LegacyRuntimeState(
            state_path.to_path_buf(),
        ));
    }
    Ok(())
}

impl RuntimeStateLock {
    fn acquire(
        state_path: &Path,
        timeout: Duration,
        interval: Duration,
    ) -> Result<Self, SupervisorError> {
        let lock_path = runtime_state_lock_path(state_path);
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => {
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    writeln!(file, "{}", std::process::id())?;
                    file.flush()?;
                    file.sync_all()?;
                    return Ok(Self { _file: file });
                }
                Err(fs::TryLockError::WouldBlock) => {
                    if started.elapsed() >= timeout {
                        return Err(SupervisorError::Io(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            format!(
                                "timed out waiting for runtime state lock {}",
                                lock_path.display()
                            ),
                        )));
                    }
                    thread::sleep(interval);
                }
                Err(fs::TryLockError::Error(error)) => {
                    return Err(SupervisorError::Io(error));
                }
            }
        }
    }
}

fn runtime_state_lock_path(state_path: &Path) -> PathBuf {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("managed.json");
    state_path.with_file_name(format!("{file_name}.v2.lock"))
}

fn legacy_runtime_state_lock_path(state_path: &Path) -> PathBuf {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("managed.json");
    state_path.with_file_name(format!("{file_name}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("managed.json");
    path.with_file_name(format!("{file_name}.{nanos}.tmp"))
}

fn home_dir() -> PathBuf {
    if let Some(home) = non_empty_env_path("HOME") {
        return home;
    }
    if let Some(home) = non_empty_env_path("USERPROFILE") {
        return home;
    }
    if let (Some(drive), Some(path)) = (env::var_os("HOMEDRIVE"), env::var_os("HOMEPATH")) {
        if !drive.is_empty() && !path.is_empty() {
            let mut combined = drive;
            combined.push(path);
            return PathBuf::from(combined);
        }
    }

    PathBuf::from(".")
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::{
        request_managed_stop_with, request_managed_stop_with_hooks, ManagedServer,
        OwnerIdentityProbe, OwnerIdentityStatus, StopRequestOutcome, StopWaitTiming,
    };
    use std::cell::Cell;
    use std::fs;
    use std::io::Read;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use tempfile::tempdir;

    fn managed_run_for(server: &ManagedServer) -> ManagedRun {
        ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: format!("test-run-{}", server.pid),
            model_id: server.id.clone(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Running,
            generation: 0,
            generation_alias: format!("loxa-test-run-{}-g0", server.pid),
            port: server.port,
            log_path: PathBuf::from(format!("/tmp/test-run-{}.log", server.pid)),
            child_pid: Some(server.pid),
            child_process_start_time_unix_s: server.process_start_time_unix_s,
            child_pgid: None,
        }
    }

    fn childless_starting_run(root: &Path, run_id: &str) -> ManagedRun {
        ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: "gemma-3-4b-it-q4".to_string(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            port: 8080,
            log_path: root.join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn run_runtime_state_lock_helper_if_requested() -> bool {
        let Some(state_path) = std::env::var_os("LOXA_TEST_V2_LOCK_HELPER_STATE") else {
            return false;
        };
        let ready_path =
            std::env::var_os("LOXA_TEST_V2_LOCK_HELPER_READY").expect("lock helper readiness path");
        let _lock = RuntimeStateLock::acquire(
            Path::new(&state_path),
            Duration::from_secs(2),
            Duration::from_millis(1),
        )
        .expect("helper acquires advisory lock");
        fs::write(&ready_path, b"locked\n").expect("publish lock readiness barrier");
        let mut release = [0_u8; 1];
        std::io::stdin()
            .read_exact(&mut release)
            .expect("helper blocks behind parent-owned release pipe");
        true
    }

    fn spawn_runtime_state_lock_helper(
        state_path: &Path,
        ready_path: &Path,
    ) -> (Child, std::process::ChildStdin) {
        let mut helper = Command::new(std::env::current_exe().expect("current test binary"))
            .arg("--exact")
            .arg("supervisor::state::tests::runtime_state_advisory_lock_recovers_after_helper_is_killed")
            .arg("--nocapture")
            .env("LOXA_TEST_V2_LOCK_HELPER_STATE", state_path)
            .env("LOXA_TEST_V2_LOCK_HELPER_READY", ready_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn lock helper test process");
        let helper_stdin = helper.stdin.take().expect("helper release pipe");
        (helper, helper_stdin)
    }

    fn wait_for_lock_helper_ready(helper: &mut Child, ready_path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if ready_path.is_file() {
                return;
            }
            if let Some(status) = helper.try_wait().expect("poll lock helper") {
                panic!("lock helper exited before readiness barrier: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "lock helper did not reach readiness barrier"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn read_and_write_runtime_state_handle_missing_and_corrupt_files() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");

        assert_eq!(
            read_runtime_state(&state_path).expect("missing state read"),
            RuntimeStateRead::Missing
        );

        let expected = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 42,
            port: 8080,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 123,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(456),
        };
        let expected_run = managed_run_for(&expected);
        write_runtime_state(&state_path, std::slice::from_ref(&expected_run))
            .expect("write runtime state");

        assert_eq!(
            read_runtime_state(&state_path).expect("read runtime state"),
            RuntimeStateRead::Loaded(vec![expected_run])
        );

        fs::write(&state_path, "{not-json").expect("write corrupt state");
        match read_runtime_state(&state_path).expect("corrupt state read") {
            RuntimeStateRead::Corrupt(message) => assert!(!message.is_empty()),
            other => panic!("unexpected runtime state: {other:?}"),
        }
    }

    #[test]
    fn childless_starting_runtime_state_round_trips_all_generation_metadata() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");

        write_runtime_state(&state_path, std::slice::from_ref(&run))
            .expect("write v2 runtime state");

        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).expect("read raw v2 runtime state"))
                .expect("parse v2 runtime state");
        assert_eq!(value["schema_version"], RUNTIME_STATE_SCHEMA_VERSION);
        assert_eq!(value["runs"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            value["runs"][0]["schema_version"],
            RUNTIME_STATE_SCHEMA_VERSION
        );
        assert_eq!(value["runs"][0]["generation_alias"], "loxa-run-1-g0");
        assert_eq!(value["runs"][0]["port"], 8080);
        assert_eq!(
            value["runs"][0]["log_path"],
            run.log_path.display().to_string()
        );
        assert!(value["runs"][0]["child_pid"].is_null());
        assert_eq!(
            read_runtime_state(&state_path).expect("read v2 runtime state"),
            RuntimeStateRead::Loaded(vec![run])
        );

        fs::write(&state_path, "[]").expect("write legacy array");
        assert_eq!(
            read_runtime_state(&state_path).expect("read legacy state"),
            RuntimeStateRead::Legacy
        );
    }

    #[test]
    fn create_starting_run_persists_the_childless_record() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");

        let stored = create_starting_run(&state_path, run.clone()).expect("create starting run");

        assert_eq!(stored, run);
        assert_eq!(
            read_runtime_state(&state_path).expect("read created state"),
            RuntimeStateRead::Loaded(vec![run])
        );
        assert!(runtime_state_lock_path(&state_path).is_file());
    }

    #[test]
    fn create_starting_run_rejects_a_second_run_without_changing_the_first() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = childless_starting_run(temp.path(), "run-1");
        let second = childless_starting_run(temp.path(), "run-2");
        create_starting_run(&state_path, first.clone()).expect("create first run");
        let committed = fs::read(&state_path).expect("read first committed bytes");

        let error = create_starting_run(&state_path, second).expect_err("reject second run");

        assert!(matches!(error, SupervisorError::ActiveRun(run_id) if run_id == "run-1"));
        assert_eq!(
            fs::read(&state_path).expect("read unchanged state"),
            committed
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read first run"),
            RuntimeStateRead::Loaded(vec![first])
        );
    }

    #[test]
    fn stale_generation_cannot_update_or_finish_the_current_run_with_identical_child() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, first.clone()).expect("create first generation");
        let mut stale_generation = first.clone();
        stale_generation.lifecycle = RunLifecycle::Running;
        stale_generation.child_pid = Some(777);
        stale_generation.child_process_start_time_unix_s = Some(111);
        assert!(
            update_runtime_state_run(&state_path, &first.identity(), stale_generation.clone())
                .expect("attach first-generation child")
        );
        let stale_identity = stale_generation.identity();
        let mut current = stale_generation.clone();
        current.generation = 1;
        current.generation_alias = "loxa-run-1-g1".to_string();
        assert!(
            update_runtime_state_run(&state_path, &stale_identity, current.clone())
                .expect("advance current generation")
        );
        assert_eq!(stale_identity.child_pid, current.identity().child_pid);
        assert_eq!(
            stale_identity.child_process_start_time_unix_s,
            current.identity().child_process_start_time_unix_s
        );
        let committed = fs::read(&state_path).expect("read current bytes");

        let mut stale_update = stale_generation;
        stale_update.lifecycle = RunLifecycle::RecoveryRequired;
        assert!(
            !update_runtime_state_run(&state_path, &stale_identity, stale_update)
                .expect("reject stale update")
        );
        assert!(
            !finish_runtime_state_run(&state_path, &stale_identity).expect("reject stale finish")
        );

        assert_eq!(
            fs::read(&state_path).expect("read unchanged bytes"),
            committed
        );

        assert!(finish_runtime_state_run(&state_path, &current.identity())
            .expect("finish current generation"));
        assert_eq!(
            read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
        let terminal: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).expect("read terminal envelope"))
                .expect("parse terminal envelope");
        assert_eq!(terminal["schema_version"], RUNTIME_STATE_SCHEMA_VERSION);
        assert_eq!(terminal["runs"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn wrong_child_cannot_update_or_finish_the_current_run() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, first.clone()).expect("create run");
        let mut current = first.clone();
        current.lifecycle = RunLifecycle::Running;
        current.child_pid = Some(777);
        current.child_process_start_time_unix_s = Some(111);
        assert!(
            update_runtime_state_run(&state_path, &first.identity(), current.clone())
                .expect("attach child")
        );
        let committed = fs::read(&state_path).expect("read current bytes");
        let mut wrong_child = current.identity();
        wrong_child.child_pid = Some(778);

        assert!(
            !update_runtime_state_run(&state_path, &wrong_child, current.clone())
                .expect("reject wrong child update")
        );
        assert!(!finish_runtime_state_run(&state_path, &wrong_child)
            .expect("reject wrong child finish"));
        assert_eq!(
            fs::read(&state_path).expect("read unchanged bytes"),
            committed
        );
    }

    #[test]
    fn ordinary_runtime_state_update_cannot_clear_a_true_stop_request() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, first.clone()).expect("create run");
        let mut stopped = first.clone();
        stopped.stop_requested = true;
        assert!(
            update_runtime_state_run(&state_path, &first.identity(), stopped.clone())
                .expect("set stop request")
        );

        let mut stale_ordinary_update = stopped.clone();
        stale_ordinary_update.stop_requested = false;
        stale_ordinary_update.lifecycle = RunLifecycle::Running;
        assert!(
            update_runtime_state_run(&state_path, &stopped.identity(), stale_ordinary_update)
                .expect("apply ordinary update")
        );

        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read updated run")
        else {
            panic!("expected loaded run");
        };
        assert!(runs[0].stop_requested);
        assert_eq!(runs[0].lifecycle, RunLifecycle::Running);
    }

    #[test]
    fn stop_request_transaction_matches_model_and_all_idempotently_without_changing_metadata() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let mut run = childless_starting_run(temp.path(), "run-1");
        run.lifecycle = RunLifecycle::Running;
        run.child_pid = Some(777);
        run.child_process_start_time_unix_s = Some(111);
        run.child_pgid = Some(777);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");

        assert_eq!(
            record_stop_request(&state_path, "missing-model").expect("no-match transaction"),
            StopRequestMatch::NoMatch
        );
        let requested =
            record_stop_request(&state_path, &run.model_id).expect("model stop transaction");
        let StopRequestMatch::Requested(first) = requested else {
            panic!("expected requested run");
        };
        let mut expected = run.clone();
        expected.stop_requested = true;
        assert_eq!(first, expected);
        let committed = fs::read(&state_path).expect("read committed request");

        assert_eq!(
            record_stop_request(&state_path, "all").expect("idempotent all transaction"),
            StopRequestMatch::Requested(expected.clone())
        );
        assert_eq!(
            fs::read(&state_path).expect("read unchanged idempotent bytes"),
            committed
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read stopped run"),
            RuntimeStateRead::Loaded(vec![expected])
        );
    }

    #[test]
    fn external_stop_request_records_intent_without_child_or_pgid_signals() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let mut run = childless_starting_run(temp.path(), "run-1");
        run.lifecycle = RunLifecycle::Running;
        run.child_pid = Some(777);
        run.child_process_start_time_unix_s = Some(111);
        run.child_pgid = Some(700);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");
        let probe = FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Dead]);
        let now = Cell::new(Duration::ZERO);

        let outcome = request_managed_stop_with(
            &state_path,
            "all",
            &probe,
            StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
            || now.get(),
            |duration| now.set(now.get() + duration),
        )
        .expect("external stop outcome");

        assert_eq!(
            outcome,
            StopRequestOutcome::RecoveryRequired {
                run_id: run.run_id.clone(),
                model_id: run.model_id.clone(),
                owner_status: OwnerIdentityStatus::Dead,
            }
        );
        assert_eq!(
            probe.events(),
            vec![(run.owner_pid, run.owner_process_start_time_unix_s)]
        );
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read preserved run")
        else {
            panic!("expected loaded run");
        };
        assert!(runs[0].stop_requested);
        assert_eq!(runs[0].child_pid, Some(777));
        assert_eq!(runs[0].child_pgid, Some(700));
    }

    #[test]
    fn external_stop_wait_releases_lock_before_owner_exact_finish() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let entered_wait = Arc::new(Barrier::new(2));
        let release_wait = Arc::new(Barrier::new(2));
        let clock_ms = Arc::new(AtomicU64::new(0));
        let waiter_path = state_path.clone();
        let entered_for_waiter = Arc::clone(&entered_wait);
        let release_for_waiter = Arc::clone(&release_wait);
        let clock_for_now = Arc::clone(&clock_ms);
        let clock_for_sleep = Arc::clone(&clock_ms);
        let waiter = thread::spawn(move || {
            request_managed_stop_with(
                &waiter_path,
                "all",
                &FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Live]),
                StopWaitTiming::test(Duration::from_secs(15), Duration::from_millis(1)),
                || Duration::from_millis(clock_for_now.load(Ordering::SeqCst)),
                |duration| {
                    entered_for_waiter.wait();
                    release_for_waiter.wait();
                    clock_for_sleep.fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
                },
            )
        });

        entered_wait.wait();
        assert!(finish_runtime_state_run(&state_path, &run.identity())
            .expect("owner exact-finishes while waiter is blocked"));
        release_wait.wait();

        assert_eq!(
            waiter.join().expect("waiter joins").expect("wait outcome"),
            StopRequestOutcome::Completed {
                run_id: run.run_id,
                model_id: run.model_id,
            }
        );
    }

    #[test]
    fn external_stop_timeout_preserves_stopped_record() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let probe = FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Live]);
        let now = Cell::new(Duration::ZERO);

        let outcome = request_managed_stop_with(
            &state_path,
            &run.model_id,
            &probe,
            StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(5)),
            || now.get(),
            |duration| now.set(now.get() + duration),
        )
        .expect("timeout outcome");

        assert_eq!(
            outcome,
            StopRequestOutcome::TimedOut {
                run_id: run.run_id.clone(),
                model_id: run.model_id.clone(),
            }
        );
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read timed-out run")
        else {
            panic!("expected loaded run");
        };
        assert!(runs[0].stop_requested);
        assert_eq!(runs[0].run_id, run.run_id);
    }

    #[test]
    fn external_stop_dead_unavailable_and_mismatched_owner_preserve_state() {
        for status in [
            OwnerIdentityStatus::Dead,
            OwnerIdentityStatus::Unavailable,
            OwnerIdentityStatus::Mismatched,
        ] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let run = childless_starting_run(temp.path(), "run-1");
            create_starting_run(&state_path, run.clone()).expect("seed run");
            let probe = FakeOwnerIdentityProbe::new(vec![status]);
            let now = Cell::new(Duration::ZERO);

            let outcome = request_managed_stop_with(
                &state_path,
                "all",
                &probe,
                StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
                || now.get(),
                |duration| now.set(now.get() + duration),
            )
            .expect("recovery-required outcome");

            assert_eq!(
                outcome,
                StopRequestOutcome::RecoveryRequired {
                    run_id: run.run_id.clone(),
                    model_id: run.model_id.clone(),
                    owner_status: status,
                }
            );
            let RuntimeStateRead::Loaded(runs) =
                read_runtime_state(&state_path).expect("read preserved stopped run")
            else {
                panic!("expected loaded run");
            };
            assert_eq!(runs.len(), 1);
            assert!(runs[0].stop_requested);
        }
    }

    #[test]
    fn external_stop_completion_wins_when_run_finishes_before_dead_owner_probe() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let request_recorded = Arc::new(Barrier::new(2));
        let release_probe = Arc::new(Barrier::new(2));
        let recorded_for_waiter = Arc::clone(&request_recorded);
        let release_for_waiter = Arc::clone(&release_probe);
        let waiter_path = state_path.clone();
        let waiter = thread::spawn(move || {
            request_managed_stop_with_hooks(
                &waiter_path,
                "all",
                &FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Dead]),
                StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
                || {
                    recorded_for_waiter.wait();
                    release_for_waiter.wait();
                },
                || Duration::ZERO,
                |_| {},
            )
        });

        request_recorded.wait();
        assert!(finish_runtime_state_run(&state_path, &run.identity())
            .expect("finish before owner probe"));
        release_probe.wait();

        assert_eq!(
            waiter.join().expect("waiter joins").expect("stop outcome"),
            StopRequestOutcome::Completed {
                run_id: run.run_id,
                model_id: run.model_id,
            }
        );
    }

    #[test]
    fn external_stop_completion_wins_when_run_finishes_during_mismatched_owner_probe() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let probe_entered = Arc::new(Barrier::new(2));
        let release_probe = Arc::new(Barrier::new(2));
        let waiter_path = state_path.clone();
        let probe = BlockingOwnerIdentityProbe {
            entered: Arc::clone(&probe_entered),
            release: Arc::clone(&release_probe),
            status: OwnerIdentityStatus::Mismatched,
        };
        let waiter = thread::spawn(move || {
            request_managed_stop_with_hooks(
                &waiter_path,
                "all",
                &probe,
                StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
                || {},
                || Duration::ZERO,
                |_| {},
            )
        });

        probe_entered.wait();
        assert!(finish_runtime_state_run(&state_path, &run.identity())
            .expect("finish during owner probe"));
        release_probe.wait();

        assert_eq!(
            waiter.join().expect("waiter joins").expect("stop outcome"),
            StopRequestOutcome::Completed {
                run_id: run.run_id,
                model_id: run.model_id,
            }
        );
    }

    #[test]
    fn external_stop_wait_ignores_generation_change_with_same_run_id() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let probe = FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Live]);
        let now = Cell::new(Duration::ZERO);
        let sleeps = Cell::new(0_u8);

        let outcome = request_managed_stop_with(
            &state_path,
            "all",
            &probe,
            StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
            || now.get(),
            |duration| {
                let sleep = sleeps.get();
                if sleep == 0 {
                    let RuntimeStateRead::Loaded(runs) =
                        read_runtime_state(&state_path).expect("read stopped generation zero")
                    else {
                        panic!("expected generation zero");
                    };
                    let mut generation_one = runs[0].clone();
                    generation_one.generation = 1;
                    generation_one.generation_alias = "loxa-run-1-g1".to_string();
                    assert!(update_runtime_state_run(
                        &state_path,
                        &runs[0].identity(),
                        generation_one,
                    )
                    .expect("advance generation"));
                } else {
                    let RuntimeStateRead::Loaded(runs) =
                        read_runtime_state(&state_path).expect("read generation one")
                    else {
                        panic!("expected generation one");
                    };
                    assert!(finish_runtime_state_run(&state_path, &runs[0].identity())
                        .expect("finish stable run"));
                }
                sleeps.set(sleep + 1);
                now.set(now.get() + duration);
            },
        )
        .expect("wait outcome");

        assert_eq!(
            outcome,
            StopRequestOutcome::Completed {
                run_id: run.run_id,
                model_id: run.model_id,
            }
        );
        assert_eq!(sleeps.get(), 2, "generation change is not completion");
    }

    #[test]
    fn ordinary_update_and_stop_request_keep_stop_monotonic_in_both_orderings() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, run.clone()).expect("seed run");
        let update_entered = Arc::new(Barrier::new(2));
        let release_update = Arc::new(Barrier::new(2));
        let update_path = state_path.clone();
        let update_run = run.clone();
        let entered = Arc::clone(&update_entered);
        let release = Arc::clone(&release_update);
        let updater = thread::spawn(move || {
            let mut ordinary = update_run.clone();
            ordinary.lifecycle = RunLifecycle::Running;
            update_runtime_state_run_with_lock_options_and_hook(
                &update_path,
                &update_run.identity(),
                ordinary,
                Duration::from_secs(2),
                Duration::from_millis(1),
                |_| {
                    entered.wait();
                    release.wait();
                    Ok(())
                },
            )
        });
        update_entered.wait();
        let stop_path = state_path.clone();
        let stopper = thread::spawn(move || record_stop_request(&stop_path, "all"));
        release_update.wait();
        assert!(updater
            .join()
            .expect("updater joins")
            .expect("ordinary update"));
        assert!(matches!(
            stopper
                .join()
                .expect("stopper joins")
                .expect("stop request"),
            StopRequestMatch::Requested(_)
        ));
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read update-first result")
        else {
            panic!("expected loaded run");
        };
        assert!(runs[0].stop_requested);

        let second_path = temp.path().join("second-managed.json");
        let second = childless_starting_run(temp.path(), "run-2");
        create_starting_run(&second_path, second.clone()).expect("seed second run");
        assert!(matches!(
            record_stop_request(&second_path, "all").expect("stop first"),
            StopRequestMatch::Requested(_)
        ));
        let mut stale = second.clone();
        stale.lifecycle = RunLifecycle::Running;
        assert!(
            update_runtime_state_run(&second_path, &second.identity(), stale)
                .expect("stale ordinary update")
        );
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&second_path).expect("read stop-first result")
        else {
            panic!("expected loaded run");
        };
        assert!(runs[0].stop_requested);
    }

    #[test]
    fn stop_request_legacy_state_and_sentinel_fail_before_mutation() {
        for legacy_sentinel in [false, true] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let protected_path = if legacy_sentinel {
                let sentinel = legacy_runtime_state_lock_path(&state_path);
                fs::write(&sentinel, b"legacy owner\n").expect("write sentinel");
                sentinel
            } else {
                fs::write(&state_path, b"[]").expect("write legacy array");
                state_path.clone()
            };
            let committed = fs::read(&protected_path).expect("read protected bytes");
            let probe = FakeOwnerIdentityProbe::new(vec![OwnerIdentityStatus::Live]);
            let now = Cell::new(Duration::ZERO);

            let error = request_managed_stop_with(
                &state_path,
                "all",
                &probe,
                StopWaitTiming::test(Duration::from_secs(15), Duration::from_secs(1)),
                || now.get(),
                |duration| now.set(now.get() + duration),
            )
            .expect_err("legacy state must fail closed");

            assert!(matches!(error, SupervisorError::LegacyRuntimeState(_)));
            assert_eq!(
                fs::read(&protected_path).expect("read unchanged protected bytes"),
                committed
            );
            assert!(probe.events().is_empty());
        }
    }

    #[test]
    fn legacy_runtime_state_arrays_fail_closed_with_path_and_guidance() {
        for (case, legacy) in [
            ("empty", b"[]".as_slice()),
            ("populated", b"[{}]".as_slice()),
        ] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join(format!("{case}-managed.json"));
            fs::write(&state_path, legacy).expect("write legacy state");
            let original = fs::read(&state_path).expect("read legacy bytes");
            let run = childless_starting_run(temp.path(), "run-1");

            let error = create_starting_run(&state_path, run).expect_err("reject legacy array");
            let message = error.to_string();

            assert!(
                matches!(error, SupervisorError::LegacyRuntimeState(ref path) if path == &state_path)
            );
            assert!(message.contains(&state_path.display().to_string()));
            assert!(message.contains("archive it manually"));
            assert_eq!(
                fs::read(&state_path).expect("read unchanged legacy"),
                original
            );
            assert!(!runtime_state_lock_path(&state_path).exists());
            assert_eq!(
                read_runtime_state(&state_path).expect("read-only legacy read"),
                RuntimeStateRead::Legacy
            );
            assert!(!runtime_state_lock_path(&state_path).exists());
        }
    }

    #[test]
    fn legacy_runtime_state_lock_sentinel_fails_closed_with_path_and_guidance() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let sentinel_path = state_path.with_file_name("managed.json.lock");
        fs::write(&sentinel_path, b"legacy owner metadata\n").expect("write legacy sentinel");
        let sentinel = fs::read(&sentinel_path).expect("read sentinel bytes");
        let run = childless_starting_run(temp.path(), "run-1");

        let error = create_starting_run(&state_path, run).expect_err("reject legacy sentinel");
        let message = error.to_string();

        assert!(
            matches!(error, SupervisorError::LegacyRuntimeState(ref path) if path == &sentinel_path)
        );
        assert!(message.contains(&sentinel_path.display().to_string()));
        assert!(message.contains("archive it manually"));
        assert_eq!(
            fs::read(&sentinel_path).expect("read unchanged sentinel"),
            sentinel
        );
        assert!(!state_path.exists());
        assert!(!runtime_state_lock_path(&state_path).exists());
    }

    #[test]
    fn injected_pre_rename_failure_preserves_runtime_state_bytes_and_cleans_temp_file() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, first.clone()).expect("create run");
        let committed = fs::read(&state_path).expect("read committed bytes");
        let mut update = first.clone();
        update.lifecycle = RunLifecycle::Running;

        let error = update_runtime_state_run_with_lock_options_and_hook(
            &state_path,
            &first.identity(),
            update,
            Duration::from_millis(100),
            Duration::from_millis(1),
            |_| {
                Err(io::Error::other(
                    "injected failure immediately before rename",
                ))
            },
        )
        .expect_err("inject pre-rename failure");

        assert!(
            matches!(error, SupervisorError::Io(ref error) if error.kind() == io::ErrorKind::Other)
        );
        assert_eq!(
            fs::read(&state_path).expect("read preserved state"),
            committed
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read preserved envelope"),
            RuntimeStateRead::Loaded(vec![first])
        );
        let temp_prefix = format!(
            "{}.",
            state_path
                .file_name()
                .expect("state file name")
                .to_string_lossy()
        );
        let temp_files = fs::read_dir(temp.path())
            .expect("read runtime directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with(&temp_prefix) && name.ends_with(".tmp")
            })
            .collect::<Vec<_>>();
        assert!(
            temp_files.is_empty(),
            "temporary state file must be cleaned"
        );
    }

    #[test]
    fn runtime_state_reader_rejects_unsupported_envelope_and_run_versions_and_multiple_runs() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "run-1");
        let mut value = serde_json::json!({
            "schema_version": RUNTIME_STATE_SCHEMA_VERSION,
            "runs": [run.clone()]
        });
        value["runs"][0]["schema_version"] = serde_json::json!(99);
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&value).expect("serialize unsupported run state"),
        )
        .expect("write unsupported run state");
        assert!(matches!(
            read_runtime_state(&state_path).expect("read unsupported run state"),
            RuntimeStateRead::Corrupt(message)
                if message.contains("unsupported managed run schema version 99")
        ));

        value["runs"][0]["schema_version"] = serde_json::json!(RUNTIME_STATE_SCHEMA_VERSION);
        value["schema_version"] = serde_json::json!(99);
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&value).expect("serialize unsupported envelope"),
        )
        .expect("write unsupported envelope");
        assert!(matches!(
            read_runtime_state(&state_path).expect("read unsupported envelope"),
            RuntimeStateRead::Corrupt(message)
                if message.contains("unsupported managed state schema version 99")
        ));

        value["schema_version"] = serde_json::json!(RUNTIME_STATE_SCHEMA_VERSION);
        value["runs"] = serde_json::json!([run.clone(), run]);
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&value).expect("serialize multiple runs"),
        )
        .expect("write multiple runs");
        assert!(matches!(
            read_runtime_state(&state_path).expect("read multiple runs"),
            RuntimeStateRead::Corrupt(message)
                if message.contains("more than one active run")
        ));
    }

    #[test]
    fn create_starting_run_rejects_nonstarting_or_child_attached_records() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let mut invalid = childless_starting_run(temp.path(), "run-1");
        invalid.lifecycle = RunLifecycle::Running;
        invalid.child_pid = Some(777);
        invalid.child_process_start_time_unix_s = Some(111);

        let error = create_starting_run(&state_path, invalid)
            .expect_err("create operation accepts only childless starting records");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert!(!state_path.exists());
    }

    #[test]
    fn remove_runtime_state_entry_does_not_remove_a_different_instance() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let first = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("first.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        let second = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 778,
            port: 8082,
            model_path: temp.path().join("second.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        write_runtime_state(&state_path, &[managed_run_for(&second)]).expect("seed runtime state");

        let removed = remove_runtime_state_entry(&state_path, &managed_run_for(&first).identity())
            .expect("remove matching runtime state");

        assert!(!removed);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after removal"),
            RuntimeStateRead::Loaded(vec![managed_run_for(&second)])
        );
    }

    #[test]
    fn runtime_state_lock_release_leaves_the_persistent_lock_file() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let lock_path = state_path.with_file_name("managed.json.v2.lock");
        fs::write(&lock_path, "stale owner metadata\n").expect("write stale metadata");

        let lock = RuntimeStateLock::acquire(
            &state_path,
            Duration::from_millis(100),
            Duration::from_millis(1),
        )
        .expect("stale metadata must not block the kernel lock");
        assert_eq!(runtime_state_lock_path(&state_path), lock_path);
        drop(lock);

        assert!(lock_path.is_file(), "v2 lock inode must remain persistent");
        RuntimeStateLock::acquire(
            &state_path,
            Duration::from_millis(100),
            Duration::from_millis(1),
        )
        .expect("released kernel lock must be immediately reusable");
    }

    #[test]
    fn runtime_state_advisory_lock_recovers_after_helper_is_killed() {
        if run_runtime_state_lock_helper_if_requested() {
            return;
        }

        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let ready_path = temp.path().join("lock-helper.ready");
        let first = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, first.clone()).expect("seed run");
        let committed = fs::read(&state_path).expect("read committed state");
        let (mut helper, _helper_stdin) = spawn_runtime_state_lock_helper(&state_path, &ready_path);
        wait_for_lock_helper_ready(&mut helper, &ready_path);

        let mut update = first.clone();
        update.lifecycle = RunLifecycle::Running;
        let error = update_runtime_state_run_with_lock_options(
            &state_path,
            &first.identity(),
            update.clone(),
            Duration::from_millis(25),
            Duration::from_millis(1),
        )
        .expect_err("contender must time out while helper owns advisory lock");
        assert!(
            matches!(error, SupervisorError::Io(ref error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        assert_eq!(
            fs::read(&state_path).expect("read state after contention"),
            committed
        );

        let helper_pid = helper.id();
        helper.kill().expect("kill lock helper");
        helper.wait().expect("reap lock helper");
        let lock_path = runtime_state_lock_path(&state_path);
        assert!(lock_path.is_file());
        assert_eq!(
            fs::read_to_string(&lock_path)
                .expect("read stale lock metadata")
                .trim(),
            helper_pid.to_string()
        );

        assert!(update_runtime_state_run_with_lock_options(
            &state_path,
            &first.identity(),
            update.clone(),
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect("acquire immediately after helper crash"));
        assert_eq!(
            read_runtime_state(&state_path).expect("read updated state"),
            RuntimeStateRead::Loaded(vec![update])
        );
        assert!(lock_path.is_file());
    }

    struct FakeOwnerIdentityProbe {
        statuses: Mutex<Vec<OwnerIdentityStatus>>,
        events: Mutex<Vec<(u32, u64)>>,
    }

    impl FakeOwnerIdentityProbe {
        fn new(statuses: Vec<OwnerIdentityStatus>) -> Self {
            Self {
                statuses: Mutex::new(statuses),
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<(u32, u64)> {
            self.events.lock().expect("owner events lock").clone()
        }
    }

    impl OwnerIdentityProbe for FakeOwnerIdentityProbe {
        fn probe(&self, pid: u32, expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
            self.events
                .lock()
                .expect("owner events lock")
                .push((pid, expected_start_time_unix_s));
            let mut statuses = self.statuses.lock().expect("owner statuses lock");
            if statuses.len() > 1 {
                statuses.remove(0)
            } else {
                statuses
                    .first()
                    .copied()
                    .unwrap_or(OwnerIdentityStatus::Unavailable)
            }
        }
    }

    struct BlockingOwnerIdentityProbe {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        status: OwnerIdentityStatus,
    }

    impl OwnerIdentityProbe for BlockingOwnerIdentityProbe {
        fn probe(&self, _pid: u32, _expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
            self.entered.wait();
            self.release.wait();
            self.status
        }
    }
}
