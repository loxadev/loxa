use crate::engine::{EngineLaunchSpec, ReadinessStrategy};
use crate::registry::{self, ModelEntry};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, ProcessesToUpdate, Signal, System};

mod lifecycle;
mod readiness;
mod state;
mod teardown;

pub use lifecycle::{
    decide_observed_child_exit, finish_childless_runtime_state_run, finish_owner_teardown_with,
    ChildlessFinishOutcome, InterruptStatus, ObservedChildExit, OwnerTeardownDecision,
    OwnerTerminalOutcome, PostSpawnCleanupOutcome, SpawnStartingRunOutcome,
};
pub use readiness::{
    process_start_time_with_retry, reserve_localhost_port, wait_for_generation_ready_or_exit,
    LocalhostPortReservation, PROCESS_IDENTITY_POLL_INTERVAL, PROCESS_IDENTITY_TIMEOUT,
};
pub use state::{
    create_starting_run, current_runtime_state_run, finish_runtime_state_run, read_runtime_state,
    remove_runtime_state_entry, runtime_dir, runtime_logs_dir, runtime_state_path,
    update_runtime_state_run, update_runtime_state_run_committed, ManagedRun, ManagedRunIdentity,
    RunLifecycle, RuntimeStateRead, RUNTIME_STATE_LOCK_POLL_INTERVAL, RUNTIME_STATE_LOCK_TIMEOUT,
    RUNTIME_STATE_SCHEMA_VERSION,
};
use state::{record_stop_request, stable_run_is_present, StopRequestMatch};
use teardown::prepare_managed_command;
#[cfg(test)]
use teardown::spawn_managed_command;
pub use teardown::{
    teardown_managed_child, LogDrainingChild, ManagedChild, SpawnedServer, TeardownConfirmation,
};

struct PreparedEngineSpawn {
    prepared: teardown::PreparedManagedCommand,
    reservation: LocalhostPortReservation,
    expected_port: u16,
}

struct RawEngineSpawn {
    raw: teardown::RawSpawnedServer,
}

pub const DEFAULT_CTX_TOKENS: u32 = 8_192;
pub const CTRL_C_GRACE_PERIOD: Duration = Duration::from_secs(5);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(60);
pub const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub const LLAMA_SERVER_VERSION_TIMEOUT: Duration = Duration::from_secs(5);
pub const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const FORCE_KILL_CONFIRMATION_PERIOD: Duration = Duration::from_secs(5);
pub const STOP_OWNER_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
pub const LOG_TAIL_BYTES: usize = 8 * 1024;
pub const MAX_LOG_BYTES: usize = 1024 * 1024;

pub struct ServerSpec<'a> {
    pub entry: &'a ModelEntry,
    pub model_path: PathBuf,
    pub llama_server_path: PathBuf,
    pub port: u16,
    pub ctx_tokens: u32,
    pub generation_alias: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedServer {
    pub id: String,
    pub pid: u32,
    pub port: u16,
    pub model_path: PathBuf,
    pub started_at_unix_s: u64,
    pub llama_server_version: String,
    #[serde(default)]
    pub process_start_time_unix_s: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManagedServerIdentity {
    pub pid: u32,
    pub port: u16,
    pub process_start_time_unix_s: Option<u64>,
}

/// An opaque, single-generation managed server owned by a calibration run.
///
/// Process control deliberately remains inside the supervisor: callers can
/// observe identity and liveness, then consume the session to finish it.
pub struct ManagedCalibrationSession {
    child: SpawnedServer,
    state_path: PathBuf,
    run: ManagedRun,
    server: ManagedServer,
}

fn resolve_calibration_initialization_failure(
    run_id: &str,
    initialization_error: SupervisorError,
    cleanup: Result<PostSpawnCleanupOutcome, SupervisorError>,
) -> SupervisorError {
    match cleanup {
        Ok(PostSpawnCleanupOutcome::Cleaned) => initialization_error,
        Ok(PostSpawnCleanupOutcome::RequestedStop) => SupervisorError::RunStateConflict(format!(
            "managed calibration run {run_id} was stopped during initialization"
        )),
        Ok(PostSpawnCleanupOutcome::RecoveryRequired) => {
            SupervisorError::RecoveryRequired(run_id.to_owned())
        }
        Err(cleanup_error) => cleanup_error,
    }
}

fn resolve_calibration_childless_spawn_failure(
    run_id: &str,
    spawn_error: SupervisorError,
    finish: Result<ChildlessFinishOutcome, SupervisorError>,
) -> SupervisorError {
    match finish {
        Ok(ChildlessFinishOutcome::Finished) => spawn_error,
        Ok(ChildlessFinishOutcome::RequestedStop) => SupervisorError::RunStateConflict(format!(
            "managed calibration run {run_id} was stopped while spawn failed"
        )),
        Err(finish_error) => finish_error,
    }
}

fn validate_calibration_attached_state(
    expected: &ManagedRun,
    current: &ManagedRun,
    server: &ManagedServer,
    child_pid: u32,
    pid_live: bool,
    process_start_time: Option<u64>,
) -> Result<(), SupervisorError> {
    let exact = current == expected
        && current.lifecycle == RunLifecycle::Running
        && !current.stop_requested
        && current.model_id == server.id
        && current.port == server.port
        && current.child_pid == Some(server.pid)
        && current.child_process_start_time_unix_s == server.process_start_time_unix_s
        && server.pid == child_pid
        && pid_live
        && server.process_start_time_unix_s.is_some()
        && process_start_time == server.process_start_time_unix_s;
    if exact {
        Ok(())
    } else {
        Err(SupervisorError::RunStateConflict(format!(
            "managed calibration run {} no longer matches its attached identity",
            expected.run_id
        )))
    }
}

impl ManagedCalibrationSession {
    pub fn start(
        state_path: &Path,
        models_dir: &Path,
        model_id: &str,
        ctx_tokens: u32,
    ) -> Result<Self, SupervisorError> {
        let (entry, model_path) = resolve_model_path(model_id, models_dir)?;
        let llama_server_path = detect_llama_server()?;
        let llama_server_version = llama_server_version(&llama_server_path)?;
        let reservation = reserve_localhost_port(None)?;
        let port = reservation.port();
        let owner_pid = std::process::id();
        let owner_start = process_start_time_with_retry(owner_pid)
            .ok_or(SupervisorError::ProcessIdentityUnavailable(owner_pid))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| SupervisorError::Io(io::Error::other(error)))?
            .as_secs();
        let run_id = format!("calibration-{owner_pid}-{owner_start}-{now}-{port}");
        let log_path = runtime_logs_dir().join(format!("{run_id}.log"));
        let starting = create_starting_run(
            state_path,
            ManagedRun {
                schema_version: RUNTIME_STATE_SCHEMA_VERSION,
                run_id: run_id.clone(),
                model_id: model_id.to_owned(),
                owner_pid,
                owner_process_start_time_unix_s: owner_start,
                stop_requested: false,
                lifecycle: RunLifecycle::Starting,
                generation: 0,
                generation_alias: format!("loxa-{run_id}-g0"),
                port,
                log_path: log_path.clone(),
                child_pid: None,
                child_process_start_time_unix_s: None,
                child_pgid: None,
            },
        )?;
        let spec = ServerSpec {
            entry,
            model_path: model_path.clone(),
            llama_server_path,
            port,
            ctx_tokens,
            generation_alias: starting.generation_alias.clone(),
        };
        let spawned = spawn_starting_llama_server(
            state_path,
            &starting.identity(),
            &spec,
            &log_path,
            reservation,
        );
        let (starting, mut child) = match spawned {
            Ok(SpawnStartingRunOutcome::Spawned { run, value }) => (run, value),
            Ok(SpawnStartingRunOutcome::RequestedStop) => {
                return Err(SupervisorError::RunStateConflict(format!(
                    "managed calibration run {run_id} was stopped before spawn"
                )))
            }
            Err(error) => {
                return Err(resolve_calibration_childless_spawn_failure(
                    &run_id,
                    error,
                    finish_childless_runtime_state_run(state_path, &starting.identity()),
                ));
            }
        };
        if let Some(error) = child.take_initialization_error() {
            let cleanup = cleanup_post_spawn_failure(&mut child, state_path, &starting.identity());
            return Err(resolve_calibration_initialization_failure(
                &run_id, error, cleanup,
            ));
        }
        let server = ManagedServer {
            id: model_id.to_owned(),
            pid: child.pid(),
            port,
            model_path,
            started_at_unix_s: now,
            llama_server_version,
            process_start_time_unix_s: process_start_time_with_retry(child.pid()),
        };
        let run = match persist_managed_server_or_cleanup(
            &mut child,
            state_path,
            starting,
            server.clone(),
            CTRL_C_GRACE_PERIOD,
        )? {
            PersistManagedServerOutcome::Attached(run) => run,
            PersistManagedServerOutcome::RequestedStop => {
                return Err(SupervisorError::RunStateConflict(format!(
                    "managed calibration run {run_id} was stopped during attachment"
                )))
            }
            PersistManagedServerOutcome::RecoveryRequired => {
                return Err(SupervisorError::RecoveryRequired(run_id))
            }
        };
        if let Err(error) = wait_for_generation_ready_or_exit(
            &mut child,
            port,
            &run.generation_alias,
            HEALTH_TIMEOUT,
            HEALTH_POLL_INTERVAL,
        ) {
            let outcome = teardown_owned_run(
                &mut child,
                state_path,
                &run.identity(),
                OwnerTeardownDecision::UnexpectedExit,
            )?;
            if outcome == OwnerTerminalOutcome::RecoveryRequired {
                return Err(SupervisorError::RecoveryRequired(run.run_id));
            }
            return Err(error);
        }
        Ok(Self {
            child,
            state_path: state_path.to_owned(),
            run,
            server,
        })
    }

    pub fn run(&self) -> &ManagedRun {
        &self.run
    }
    pub fn server(&self) -> &ManagedServer {
        &self.server
    }
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.server.port)
    }
    pub fn generation_alias(&self) -> &str {
        &self.run.generation_alias
    }

    pub fn ensure_running(&mut self) -> Result<(), SupervisorError> {
        let current = current_runtime_state_run(&self.state_path, &self.run.identity())?;
        match self.child.try_wait()? {
            None => validate_calibration_attached_state(
                &self.run,
                &current,
                &self.server,
                self.child.pid(),
                pid_is_alive(self.server.pid),
                process_start_time_with_retry(self.server.pid),
            ),
            Some(_) => Err(child_exited_early(&self.run.log_path)?),
        }
    }

    pub fn finish(mut self) -> Result<OwnerTerminalOutcome, SupervisorError> {
        teardown_owned_run(
            &mut self.child,
            &self.state_path,
            &self.run.identity(),
            OwnerTeardownDecision::RequestedStop,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistManagedServerOutcome {
    Attached(ManagedRun),
    RequestedStop,
    RecoveryRequired,
}

impl ManagedServer {
    pub fn identity(&self) -> ManagedServerIdentity {
        ManagedServerIdentity {
            pid: self.pid,
            port: self.port,
            process_start_time_unix_s: self.process_start_time_unix_s,
        }
    }
}

#[derive(Debug)]
pub enum SupervisorError {
    UnknownModel(String),
    ModelNotDownloaded(PathBuf),
    LlamaServerNotFound,
    LlamaServerVersionTimeout,
    NoFreePort,
    ProcessIdentityUnavailable(u32),
    LegacyRuntimeState(PathBuf),
    ActiveRun(String),
    RecoveryRequired(String),
    RunStateConflict(String),
    CleanupNotConfirmed(String),
    HealthTimeout,
    ChildExitedEarly(String),
    ChildReapedDiagnosticsFailed(String),
    Io(io::Error),
    Http(reqwest::Error),
}

impl fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SupervisorError::UnknownModel(id) => write!(f, "unknown model id: {id}"),
            SupervisorError::ModelNotDownloaded(path) => {
                write!(f, "model not downloaded: {}", path.display())
            }
            SupervisorError::LlamaServerNotFound => write!(f, "llama-server not found"),
            SupervisorError::LlamaServerVersionTimeout => {
                write!(f, "llama-server --version timed out")
            }
            SupervisorError::NoFreePort => write!(f, "no free localhost port available"),
            SupervisorError::ProcessIdentityUnavailable(pid) => {
                write!(f, "could not capture process identity for pid {pid}")
            }
            SupervisorError::LegacyRuntimeState(path) => write!(
                f,
                "legacy runtime state blocks safe mutation at {}; confirm no old Loxa process remains, then archive it manually",
                path.display()
            ),
            SupervisorError::ActiveRun(run_id) => {
                write!(f, "managed run {run_id} is already active")
            }
            SupervisorError::RecoveryRequired(run_id) => {
                write!(f, "recovery required for managed run {run_id}")
            }
            SupervisorError::RunStateConflict(message) => {
                write!(f, "managed run state conflict: {message}")
            }
            SupervisorError::CleanupNotConfirmed(run_id) => write!(
                f,
                "cleanup could not be confirmed for managed run {run_id}; recovery required"
            ),
            SupervisorError::HealthTimeout => write!(f, "llama-server did not become healthy"),
            SupervisorError::ChildExitedEarly(message) => {
                write!(f, "llama-server exited before becoming healthy: {message}")
            }
            SupervisorError::ChildReapedDiagnosticsFailed(message) => write!(
                f,
                "llama-server exited, but crash diagnostics failed after it was reaped: {message}"
            ),
            SupervisorError::Io(error) => write!(f, "io error: {error}"),
            SupervisorError::Http(error) => write!(f, "http error: {error}"),
        }
    }
}

impl Error for SupervisorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SupervisorError::Io(error) => Some(error),
            SupervisorError::Http(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SupervisorError {
    fn from(error: io::Error) -> Self {
        SupervisorError::Io(error)
    }
}

impl From<reqwest::Error> for SupervisorError {
    fn from(error: reqwest::Error) -> Self {
        SupervisorError::Http(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedServerInspection {
    pub server: ManagedServer,
    pub pid_alive: bool,
    pub port_alive: bool,
    pub process_identity_matches: bool,
    pub stale: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedRunStatus {
    Starting,
    Running,
    Stopping,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManagedRunInspection {
    pub status: ManagedRunStatus,
    pub owner_status: OwnerIdentityStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CleanupResult {
    pub forced: bool,
    pub removed_state: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerIdentityStatus {
    Live,
    Dead,
    Unavailable,
    Mismatched,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopRequestOutcome {
    NoMatch,
    Completed {
        run_id: String,
        model_id: String,
    },
    RecoveryRequired {
        run_id: String,
        model_id: String,
        owner_status: OwnerIdentityStatus,
    },
    TimedOut {
        run_id: String,
        model_id: String,
    },
}

#[derive(Clone, Copy)]
struct StopWaitTiming {
    timeout: Duration,
    interval: Duration,
}

impl StopWaitTiming {
    fn production() -> Self {
        Self {
            timeout: STOP_OWNER_WAIT_TIMEOUT,
            interval: STOP_POLL_INTERVAL,
        }
    }

    #[cfg(test)]
    fn test(timeout: Duration, interval: Duration) -> Self {
        Self { timeout, interval }
    }
}

trait ProcessController {
    fn pid_is_alive(&self, pid: u32) -> bool;
    fn port_is_alive(&self, port: u16) -> bool;
    fn process_start_time(&self, pid: u32) -> Option<u64>;
}

trait OwnerIdentityProbe {
    fn probe(&self, pid: u32, expected_start_time_unix_s: u64) -> OwnerIdentityStatus;
}

struct SystemProcessController;

struct SystemOwnerIdentityProbe;

fn probe_owner_identity_with<L, S>(
    pid: u32,
    expected_start_time_unix_s: u64,
    mut pid_is_alive: L,
    mut process_start_time: S,
) -> OwnerIdentityStatus
where
    L: FnMut(u32) -> bool,
    S: FnMut(u32) -> Option<u64>,
{
    if !pid_is_alive(pid) {
        return OwnerIdentityStatus::Dead;
    }

    match process_start_time(pid) {
        Some(actual) if actual != expected_start_time_unix_s => OwnerIdentityStatus::Mismatched,
        Some(_) if pid_is_alive(pid) => OwnerIdentityStatus::Live,
        Some(_) => OwnerIdentityStatus::Dead,
        None if !pid_is_alive(pid) => OwnerIdentityStatus::Dead,
        None => OwnerIdentityStatus::Unavailable,
    }
}

impl OwnerIdentityProbe for SystemOwnerIdentityProbe {
    fn probe(&self, pid: u32, expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
        probe_owner_identity_with(
            pid,
            expected_start_time_unix_s,
            pid_is_alive,
            process_start_time_with_retry,
        )
    }
}

impl ProcessController for SystemProcessController {
    fn pid_is_alive(&self, pid: u32) -> bool {
        pid_is_alive(pid)
    }

    fn port_is_alive(&self, port: u16) -> bool {
        port_is_alive(port)
    }

    fn process_start_time(&self, pid: u32) -> Option<u64> {
        process_start_time_with_retry(pid)
    }
}

pub fn resolve_model_path(
    id: &str,
    models_dir: &Path,
) -> Result<(&'static ModelEntry, PathBuf), SupervisorError> {
    let entry = registry::find(id).ok_or_else(|| SupervisorError::UnknownModel(id.to_string()))?;
    let path = models_dir.join(entry.filename);
    if path.is_file() {
        Ok((entry, path))
    } else {
        Err(SupervisorError::ModelNotDownloaded(path))
    }
}

pub fn detect_llama_server() -> Result<PathBuf, SupervisorError> {
    if let Some(path) = env::var_os("LOXA_LLAMA_SERVER").filter(|value| !value.is_empty()) {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
    }

    for dir in env::split_paths(&env::var_os("PATH").unwrap_or_default()) {
        let candidate = dir.join(binary_name("llama-server"));
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(SupervisorError::LlamaServerNotFound)
}

pub fn llama_server_version(path: &Path) -> Result<String, SupervisorError> {
    let mut child = Command::new(path)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let started = Instant::now();
    while started.elapsed() < LLAMA_SERVER_VERSION_TIMEOUT {
        if child.try_wait()?.is_some() {
            let output = read_child_output(&mut child)?;
            let version = output.trim();
            if version.is_empty() {
                return Ok("unknown".to_string());
            }
            return Ok(version.to_string());
        }

        thread::sleep(Duration::from_millis(50));
    }

    let _ = child.kill();
    let _ = child.wait();
    Err(SupervisorError::LlamaServerVersionTimeout)
}

pub fn log_file_path(id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
    runtime_logs_dir().join(format!("{id}-{port}-{started_at_unix_s}.log"))
}

pub fn request_managed_stop(
    path: &Path,
    target: &str,
) -> Result<StopRequestOutcome, SupervisorError> {
    let started = Instant::now();
    request_managed_stop_with(
        path,
        target,
        &SystemOwnerIdentityProbe,
        StopWaitTiming::production(),
        || started.elapsed(),
        thread::sleep,
    )
}

fn request_managed_stop_with<P, N, S>(
    path: &Path,
    target: &str,
    owner_probe: &P,
    timing: StopWaitTiming,
    now: N,
    sleep: S,
) -> Result<StopRequestOutcome, SupervisorError>
where
    P: OwnerIdentityProbe,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    request_managed_stop_with_hooks(path, target, owner_probe, timing, || {}, now, sleep)
}

fn request_managed_stop_with_hooks<P, H, N, S>(
    path: &Path,
    target: &str,
    owner_probe: &P,
    timing: StopWaitTiming,
    after_record: H,
    mut now: N,
    mut sleep: S,
) -> Result<StopRequestOutcome, SupervisorError>
where
    P: OwnerIdentityProbe,
    H: FnOnce(),
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let run = match record_stop_request(path, target)? {
        StopRequestMatch::NoMatch => return Ok(StopRequestOutcome::NoMatch),
        StopRequestMatch::Requested(run) => run,
    };
    after_record();
    let completed = || StopRequestOutcome::Completed {
        run_id: run.run_id.clone(),
        model_id: run.model_id.clone(),
    };
    let recovery = |owner_status| StopRequestOutcome::RecoveryRequired {
        run_id: run.run_id.clone(),
        model_id: run.model_id.clone(),
        owner_status,
    };
    let mut owner_status = owner_probe.probe(run.owner_pid, run.owner_process_start_time_unix_s);
    if !stable_run_is_present(path, &run.run_id)? {
        return Ok(completed());
    }
    if owner_status != OwnerIdentityStatus::Live {
        return Ok(recovery(owner_status));
    }

    let started = now();
    loop {
        if !stable_run_is_present(path, &run.run_id)? {
            return Ok(StopRequestOutcome::Completed {
                run_id: run.run_id,
                model_id: run.model_id,
            });
        }

        owner_status = owner_probe.probe(run.owner_pid, run.owner_process_start_time_unix_s);
        if !stable_run_is_present(path, &run.run_id)? {
            return Ok(completed());
        }
        if owner_status != OwnerIdentityStatus::Live {
            return Ok(recovery(owner_status));
        }

        let elapsed = now().saturating_sub(started);
        if elapsed >= timing.timeout {
            return Ok(StopRequestOutcome::TimedOut {
                run_id: run.run_id,
                model_id: run.model_id,
            });
        }
        sleep(timing.interval.min(timing.timeout - elapsed));
    }
}

pub fn inspect_managed_servers(servers: &[ManagedServer]) -> Vec<ManagedServerInspection> {
    inspect_managed_servers_with(servers, &SystemProcessController)
}

pub fn inspect_managed_run(run: &ManagedRun) -> ManagedRunInspection {
    inspect_managed_run_with(run, &SystemOwnerIdentityProbe, &SystemProcessController)
}

fn inspect_managed_run_with<O, P>(
    run: &ManagedRun,
    owner_probe: &O,
    controller: &P,
) -> ManagedRunInspection
where
    O: OwnerIdentityProbe,
    P: ProcessController,
{
    let owner_status = owner_probe.probe(run.owner_pid, run.owner_process_start_time_unix_s);
    let status = if owner_status != OwnerIdentityStatus::Live {
        ManagedRunStatus::RecoveryRequired
    } else if run.stop_requested || run.lifecycle == RunLifecycle::Stopping {
        ManagedRunStatus::Stopping
    } else if run.lifecycle == RunLifecycle::RecoveryRequired {
        ManagedRunStatus::RecoveryRequired
    } else if matches!(
        run.lifecycle,
        RunLifecycle::Starting | RunLifecycle::Restarting
    ) && run.child_pid.is_none()
    {
        ManagedRunStatus::Starting
    } else if run.lifecycle == RunLifecycle::Running {
        match (run.child_pid, run.child_process_start_time_unix_s) {
            (Some(pid), Some(expected_start_time))
                if controller.pid_is_alive(pid)
                    && controller.process_start_time(pid) == Some(expected_start_time)
                    && controller.port_is_alive(run.port) =>
            {
                ManagedRunStatus::Running
            }
            _ => ManagedRunStatus::RecoveryRequired,
        }
    } else {
        ManagedRunStatus::RecoveryRequired
    };

    ManagedRunInspection {
        status,
        owner_status,
    }
}

fn inspect_managed_servers_with<P: ProcessController>(
    servers: &[ManagedServer],
    controller: &P,
) -> Vec<ManagedServerInspection> {
    servers
        .iter()
        .cloned()
        .map(|server| {
            let pid_alive = controller.pid_is_alive(server.pid);
            let port_alive = controller.port_is_alive(server.port);
            let process_identity_matches = process_identity_matches(&server, controller);
            ManagedServerInspection {
                stale: !(pid_alive && port_alive && process_identity_matches),
                server,
                pid_alive,
                port_alive,
                process_identity_matches,
            }
        })
        .collect()
}

pub fn persist_managed_server_or_cleanup<C>(
    child: &mut C,
    state_path: &Path,
    run: ManagedRun,
    server: ManagedServer,
    grace_period: Duration,
) -> Result<PersistManagedServerOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let _ = grace_period;
    persist_managed_server_or_cleanup_with(
        child,
        state_path,
        run,
        server,
        |child, path, expected| cleanup_post_spawn_failure(child, path, expected),
    )
}

fn persist_managed_server_or_cleanup_with<C, T>(
    child: &mut C,
    state_path: &Path,
    mut run: ManagedRun,
    server: ManagedServer,
    cleanup: T,
) -> Result<PersistManagedServerOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
    T: FnOnce(
        &mut C,
        &Path,
        &ManagedRunIdentity,
    ) -> Result<PostSpawnCleanupOutcome, SupervisorError>,
{
    let expected = run.identity();
    let error = if server.process_start_time_unix_s.is_none() {
        SupervisorError::ProcessIdentityUnavailable(server.pid)
    } else {
        run.model_id = server.id;
        run.lifecycle = RunLifecycle::Running;
        run.port = server.port;
        run.child_pid = Some(server.pid);
        run.child_process_start_time_unix_s = server.process_start_time_unix_s;
        run.child_pgid = child.owned_pgid();
        match update_runtime_state_run_committed(state_path, &expected, run) {
            Ok(Some(committed)) => {
                return Ok(PersistManagedServerOutcome::Attached(committed));
            }
            Ok(None) => SupervisorError::RunStateConflict(format!(
                "starting run {} generation {} no longer matches",
                expected.run_id, expected.generation
            )),
            Err(error) => error,
        }
    };

    match cleanup(child, state_path, &expected)? {
        PostSpawnCleanupOutcome::Cleaned => Err(error),
        PostSpawnCleanupOutcome::RequestedStop => Ok(PersistManagedServerOutcome::RequestedStop),
        PostSpawnCleanupOutcome::RecoveryRequired => {
            Ok(PersistManagedServerOutcome::RecoveryRequired)
        }
    }
}

pub fn cleanup_post_spawn_failure<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
) -> Result<PostSpawnCleanupOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    lifecycle::finish_post_spawn_failure_with(state_path, state_identity, || {
        teardown::teardown_managed_child_result(child).confirmation
    })
}

pub fn teardown_owned_run<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    decision: OwnerTeardownDecision,
) -> Result<OwnerTerminalOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    lifecycle::finish_owner_teardown_with(state_path, state_identity, decision, |_| {
        teardown::teardown_managed_child_result(child).confirmation
    })
}

pub fn handle_observed_child_exit<C, I>(
    child: &mut C,
    log_path: &Path,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
) -> Result<ObservedChildExit, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptStatus,
{
    lifecycle::decide_observed_child_exit_with_diagnostics(
        state_path,
        state_identity,
        interrupt,
        || teardown::teardown_managed_child_result(child).confirmation,
        || read_log_tail(log_path, LOG_TAIL_BYTES),
    )
}

/// Spawns one validated childless managed engine generation through the complete boundary.
///
/// The lower-level arbitrary-closure transaction is intentionally crate-private:
///
/// ```compile_fail
/// use loxa_core::supervisor::spawn_starting_run_with;
/// ```
pub fn spawn_starting_engine(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    spec: &EngineLaunchSpec,
    log_path: &Path,
    reservation: LocalhostPortReservation,
) -> Result<SpawnStartingRunOutcome<SpawnedServer>, SupervisorError> {
    spawn_starting_engine_with_hooks(
        state_path,
        expected,
        spec,
        log_path,
        reservation,
        || {},
        || {},
        || {},
        || {},
    )
}

pub fn spawn_starting_llama_server(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    spec: &ServerSpec<'_>,
    log_path: &Path,
    reservation: LocalhostPortReservation,
) -> Result<SpawnStartingRunOutcome<SpawnedServer>, SupervisorError> {
    let launch = llama_engine_launch_spec(spec);
    spawn_starting_engine(state_path, expected, &launch, log_path, reservation)
}

#[allow(clippy::too_many_arguments)]
fn spawn_starting_engine_with_hooks<L, B, A, D>(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    spec: &EngineLaunchSpec,
    log_path: &Path,
    reservation: LocalhostPortReservation,
    before_log_open: L,
    before_reservation_release: B,
    after_os_spawn: A,
    after_log_drain_setup: D,
) -> Result<SpawnStartingRunOutcome<SpawnedServer>, SupervisorError>
where
    L: FnOnce(),
    B: FnOnce(),
    A: FnOnce(),
    D: FnOnce(),
{
    let prepared = prepare_engine_spawn_with_hook(spec, log_path, reservation, before_log_open)?;
    match lifecycle::spawn_starting_run_with(state_path, expected, || {
        prepared.spawn_raw_with_hooks(before_reservation_release, after_os_spawn)
    })? {
        SpawnStartingRunOutcome::Spawned { run, value: raw } => {
            Ok(SpawnStartingRunOutcome::Spawned {
                run,
                value: raw.finish_with_hook(after_log_drain_setup),
            })
        }
        SpawnStartingRunOutcome::RequestedStop => Ok(SpawnStartingRunOutcome::RequestedStop),
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn spawn_starting_llama_server_with_hooks<L, B, A, D>(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    spec: &ServerSpec<'_>,
    log_path: &Path,
    reservation: LocalhostPortReservation,
    before_log_open: L,
    before_reservation_release: B,
    after_os_spawn: A,
    after_log_drain_setup: D,
) -> Result<SpawnStartingRunOutcome<SpawnedServer>, SupervisorError>
where
    L: FnOnce(),
    B: FnOnce(),
    A: FnOnce(),
    D: FnOnce(),
{
    let launch = llama_engine_launch_spec(spec);
    spawn_starting_engine_with_hooks(
        state_path,
        expected,
        &launch,
        log_path,
        reservation,
        before_log_open,
        before_reservation_release,
        after_os_spawn,
        after_log_drain_setup,
    )
}

fn llama_engine_launch_spec(spec: &ServerSpec<'_>) -> EngineLaunchSpec {
    EngineLaunchSpec {
        program: spec.llama_server_path.clone(),
        args: llama_server_args(spec)
            .into_iter()
            .map(Into::into)
            .collect(),
        port: spec.port,
        engine_name: "llama-server".to_string(),
        engine_version: "unknown".to_string(),
        runtime_model: spec.model_path.display().to_string(),
        upstream_model: spec.generation_alias.clone(),
        readiness: ReadinessStrategy::LlamaModelAlias {
            expected_alias: spec.generation_alias.clone(),
        },
    }
}

fn prepare_engine_spawn_with_hook<F>(
    spec: &EngineLaunchSpec,
    log_path: &Path,
    reservation: LocalhostPortReservation,
    before_log_open: F,
) -> Result<PreparedEngineSpawn, SupervisorError>
where
    F: FnOnce(),
{
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    before_log_open();
    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)?;

    let mut command = Command::new(&spec.program);
    command.args(&spec.args);

    let writer = Arc::new(Mutex::new(BoundedLogWriter {
        file: log_file,
        remaining: MAX_LOG_BYTES,
        truncated: false,
    }));

    Ok(PreparedEngineSpawn {
        prepared: prepare_managed_command(command, writer),
        reservation,
        expected_port: spec.port,
    })
}

impl PreparedEngineSpawn {
    fn spawn_raw_with_hooks<B, A>(
        self,
        before_reservation_release: B,
        after_os_spawn: A,
    ) -> Result<RawEngineSpawn, SupervisorError>
    where
        B: FnOnce(),
        A: FnOnce(),
    {
        let Self {
            prepared,
            reservation,
            expected_port,
        } = self;
        let raw = prepared.spawn_raw(move || {
            before_reservation_release();
            reservation.release_for(expected_port)
        })?;
        after_os_spawn();
        Ok(RawEngineSpawn { raw })
    }
}

impl RawEngineSpawn {
    fn finish(self) -> SpawnedServer {
        self.raw.finish()
    }

    fn finish_with_hook(self, after_log_drain_setup: impl FnOnce()) -> SpawnedServer {
        let mut spawned = self.finish();
        let hook = std::panic::catch_unwind(std::panic::AssertUnwindSafe(after_log_drain_setup));
        if let Err(payload) = hook {
            let _ = teardown::teardown_managed_child_result(&mut spawned);
            std::panic::resume_unwind(payload);
        }
        spawned
    }
}

pub fn read_log_tail(path: &Path, max_bytes: usize) -> Result<String, SupervisorError> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(max_bytes as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail)?;
    Ok(String::from_utf8_lossy(&tail).into_owned())
}

pub fn child_exited_early(log_path: &Path) -> Result<SupervisorError, SupervisorError> {
    let log_tail = read_log_tail(log_path, LOG_TAIL_BYTES)?;
    Ok(SupervisorError::ChildExitedEarly(log_tail))
}

pub fn cleanup_after_ctrl_c<C: ManagedChild + LogDrainingChild>(
    child: &mut C,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    _grace_period: Duration,
) -> Result<CleanupResult, SupervisorError> {
    let result = teardown::teardown_managed_child_result(child);
    let removed_state = if result.confirmation == TeardownConfirmation::Confirmed {
        remove_runtime_state_entry(state_path, state_identity)?
    } else {
        false
    };
    Ok(CleanupResult {
        forced: result.forced,
        removed_state,
    })
}

fn read_child_output(child: &mut Child) -> io::Result<String> {
    let mut stdout = String::new();
    let mut stderr = String::new();

    if let Some(mut handle) = child.stdout.take() {
        handle.read_to_string(&mut stdout)?;
    }
    if let Some(mut handle) = child.stderr.take() {
        handle.read_to_string(&mut stderr)?;
    }

    let output = format!("{stdout}{stderr}");
    Ok(output)
}

#[cfg(test)]
fn cleanup_after_ctrl_c_with<C, S>(
    child: &mut C,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    grace_period: Duration,
    force_kill_confirmation: Duration,
    interval: Duration,
    mut sleep: S,
) -> Result<CleanupResult, SupervisorError>
where
    C: ManagedChild,
    S: FnMut(Duration),
{
    let result = teardown_managed_child_with(
        child,
        grace_period,
        force_kill_confirmation,
        interval,
        &mut sleep,
    )?;
    let removed_state = if result.confirmation == TeardownConfirmation::Confirmed {
        remove_runtime_state_entry(state_path, state_identity)?
    } else {
        false
    };

    Ok(CleanupResult {
        forced: result.forced,
        removed_state,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(test)]
struct ChildTeardownResult {
    confirmation: TeardownConfirmation,
    forced: bool,
}

#[cfg(test)]
fn teardown_managed_child_with<C, S>(
    child: &mut C,
    grace_period: Duration,
    force_kill_confirmation: Duration,
    interval: Duration,
    mut sleep: S,
) -> Result<ChildTeardownResult, SupervisorError>
where
    C: ManagedChild,
    S: FnMut(Duration),
{
    child.terminate()?;

    if wait_for_child_exit(child, grace_period, interval, &mut sleep)? {
        return Ok(ChildTeardownResult {
            confirmation: TeardownConfirmation::Confirmed,
            forced: false,
        });
    }

    child.kill()?;
    let confirmed_stopped =
        wait_for_child_exit(child, force_kill_confirmation, interval, &mut sleep)?;
    Ok(ChildTeardownResult {
        confirmation: if confirmed_stopped {
            TeardownConfirmation::Confirmed
        } else {
            TeardownConfirmation::Unconfirmed
        },
        forced: true,
    })
}

#[cfg(test)]
fn wait_for_child_exit<C, S>(
    child: &mut C,
    timeout: Duration,
    interval: Duration,
    sleep: &mut S,
) -> Result<bool, SupervisorError>
where
    C: ManagedChild,
    S: FnMut(Duration),
{
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }

        sleep(interval);
    }
}

fn process_identity_matches<P: ProcessController>(server: &ManagedServer, controller: &P) -> bool {
    let Some(expected) = server.process_start_time_unix_s else {
        return false;
    };

    controller.process_start_time(server.pid) == Some(expected)
}

fn llama_server_args(spec: &ServerSpec<'_>) -> Vec<String> {
    vec![
        "--model".to_string(),
        spec.model_path.display().to_string(),
        "--alias".to_string(),
        spec.generation_alias.clone(),
        "--host".to_string(),
        "127.0.0.1".to_string(),
        "--port".to_string(),
        spec.port.to_string(),
        "--ctx-size".to_string(),
        spec.ctx_tokens.to_string(),
        "--gpu-layers".to_string(),
        "auto".to_string(),
        "--flash-attn".to_string(),
        "auto".to_string(),
        "--metrics".to_string(),
        "--log-disable".to_string(),
    ]
}

fn pid_is_alive(pid: u32) -> bool {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).is_some()
}

fn port_is_alive(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok()
}

fn signal_pid(pid: u32, signal: Signal) -> io::Result<()> {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let Some(process) = system.process(pid) else {
        return Ok(());
    };

    match process.kill_with(signal) {
        Some(true) => Ok(()),
        Some(false) => Err(io::Error::other(format!(
            "failed to send {signal:?} to pid {pid}"
        ))),
        None => Err(io::Error::other(format!(
            "{signal:?} is unsupported on this platform"
        ))),
    }
}

fn binary_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.exe")
    } else {
        stem.to_string()
    }
}

struct BoundedLogWriter {
    file: File,
    remaining: usize,
    truncated: bool,
}

impl BoundedLogWriter {
    fn write_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        if self.remaining == 0 {
            return Ok(());
        }

        let marker = b"\n[loxa log truncated]\n";
        let will_truncate = chunk.len() >= self.remaining;
        let reserve_for_marker = will_truncate && !self.truncated && self.remaining > marker.len();
        let available_for_chunk = if reserve_for_marker {
            self.remaining - marker.len()
        } else {
            self.remaining
        };
        let written = chunk.len().min(available_for_chunk);
        self.file.write_all(&chunk[..written])?;
        self.remaining -= written;

        if will_truncate && !self.truncated && self.remaining > 0 {
            let marker_len = marker.len().min(self.remaining);
            self.file.write_all(&marker[..marker_len])?;
            self.remaining -= marker_len;
            self.truncated = true;
        }

        self.file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::state::write_runtime_state;
    use super::*;
    use std::cell::Cell;
    #[cfg(unix)]
    use std::ffi::OsString;
    use std::fs;
    use std::io::Write;
    use std::net::TcpListener;
    #[cfg(unix)]
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
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

    #[cfg(unix)]
    fn executable_script(path: &Path, source: &str) {
        fs::write(path, source).expect("write executable script");
        let mut permissions = fs::metadata(path).expect("script metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("make script executable");
    }

    #[cfg(unix)]
    fn test_engine_launch_spec(
        program: PathBuf,
        args: Vec<OsString>,
        port: u16,
    ) -> crate::engine::EngineLaunchSpec {
        crate::engine::EngineLaunchSpec {
            program,
            args,
            port,
            engine_name: "test-engine".to_string(),
            engine_version: "1.0.0".to_string(),
            runtime_model: "test-runtime-model".to_string(),
            upstream_model: "test-upstream-model".to_string(),
            readiness: crate::engine::ReadinessStrategy::ChatCompletionProbe {
                request_model: "test-upstream-model".to_string(),
            },
        }
    }

    #[cfg(unix)]
    #[test]
    fn generic_spawn_preserves_exact_program_and_argument_bytes() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let program = temp.path().join("engine with spaces");
        executable_script(&program, "#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\"\n");
        let args = vec![
            OsString::from("plain"),
            OsString::from("with spaces"),
            OsString::from_vec(b"native-\x80-byte".to_vec()),
        ];
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let port = reservation.port();
        let mut run = childless_starting_run(temp.path(), "generic-exact-command");
        run.port = port;
        create_starting_run(&state_path, run.clone()).expect("publish starting run");
        let spec = test_engine_launch_spec(program.clone(), args.clone(), port);

        let outcome = spawn_starting_engine(
            &state_path,
            &run.identity(),
            &spec,
            &run.log_path,
            reservation,
        )
        .expect("spawn exact command");
        let SpawnStartingRunOutcome::Spawned {
            value: mut child, ..
        } = outcome
        else {
            panic!("unexpected requested stop");
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().expect("observe exact command").is_none()
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            child.try_wait().expect("reap exact command").is_some(),
            "exact command must exit"
        );
        assert_eq!(
            teardown_managed_child(&mut child, Duration::ZERO).expect("reap exact command"),
            TeardownConfirmation::Confirmed
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(program.as_os_str().as_bytes());
        expected.push(b'\n');
        for arg in &args {
            expected.extend_from_slice(arg.as_os_str().as_bytes());
            expected.push(b'\n');
        }
        assert_eq!(
            fs::read(&run.log_path).expect("read captured argv"),
            expected
        );
        assert_eq!(
            finish_childless_runtime_state_run(&state_path, &run.identity())
                .expect("finish exact-command state"),
            ChildlessFinishOutcome::Finished
        );
    }

    #[cfg(unix)]
    #[test]
    fn generic_spawn_holds_reservation_to_spawn_and_attaches_pid_and_pgid_transactionally() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let program = temp.path().join("long-running engine");
        executable_script(
            &program,
            "#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n",
        );
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let port = reservation.port();
        let address = ("127.0.0.1", port);
        let mut run = childless_starting_run(temp.path(), "generic-attached-command");
        run.port = port;
        create_starting_run(&state_path, run.clone()).expect("publish starting run");
        let spec = test_engine_launch_spec(program, Vec::new(), port);
        let reservation_was_held = Cell::new(false);
        let reservation_was_released = Cell::new(false);

        let outcome = spawn_starting_engine_with_hooks(
            &state_path,
            &run.identity(),
            &spec,
            &run.log_path,
            reservation,
            || {},
            || {
                let error = TcpListener::bind(address)
                    .expect_err("reservation must still be held immediately before spawn");
                assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
                reservation_was_held.set(true);
            },
            || {
                drop(TcpListener::bind(address).expect("reservation released for OS spawn"));
                reservation_was_released.set(true);
            },
            || {},
        )
        .expect("spawn generic engine");
        let SpawnStartingRunOutcome::Spawned {
            run: starting,
            value: mut child,
        } = outcome
        else {
            panic!("unexpected requested stop");
        };
        assert!(reservation_was_held.get());
        assert!(reservation_was_released.get());
        let child_pid = child.pid();
        let child_pgid = child.owned_pgid();
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: child_pid,
            port,
            model_path: temp.path().join("test-model"),
            started_at_unix_s: 1,
            llama_server_version: spec.engine_version.clone(),
            process_start_time_unix_s: process_start_time_with_retry(child_pid),
        };

        let attached = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting,
            server,
            Duration::ZERO,
        )
        .expect("attach generic engine");
        let PersistManagedServerOutcome::Attached(attached) = attached else {
            panic!("generic engine must attach");
        };
        assert_eq!(attached.child_pid, Some(child_pid));
        assert_eq!(attached.child_pgid, child_pgid);
        assert_eq!(child_pgid, i32::try_from(child_pid).ok());
        assert_eq!(
            read_runtime_state(&state_path).expect("read attached generic engine"),
            RuntimeStateRead::Loaded(vec![attached.clone()])
        );
        assert_eq!(
            teardown_owned_run(
                &mut child,
                &state_path,
                &attached.identity(),
                OwnerTeardownDecision::RequestedStop,
            )
            .expect("teardown attached generic engine"),
            OwnerTerminalOutcome::RequestedStop
        );
    }

    #[cfg(unix)]
    #[test]
    fn generic_spawn_cleans_up_when_post_spawn_initialization_panics() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let pid_path = temp.path().join("engine.pid");
        let program = temp.path().join("post-spawn failure engine");
        executable_script(
            &program,
            "#!/bin/sh\nprintf '%s' \"$$\" > \"$1\"\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n",
        );
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let port = reservation.port();
        let mut run = childless_starting_run(temp.path(), "generic-post-spawn-failure");
        run.port = port;
        create_starting_run(&state_path, run.clone()).expect("publish starting run");
        let spec = test_engine_launch_spec(program, vec![pid_path.as_os_str().to_owned()], port);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = spawn_starting_engine_with_hooks(
                &state_path,
                &run.identity(),
                &spec,
                &run.log_path,
                reservation,
                || {},
                || {},
                || {},
                || {
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while !pid_path.is_file() && Instant::now() < deadline {
                        thread::sleep(Duration::from_millis(10));
                    }
                    assert!(pid_path.is_file(), "spawned engine must publish its pid");
                    panic!("injected post-spawn initialization failure");
                },
            );
        }));

        assert!(panic.is_err());
        let pid = fs::read_to_string(&pid_path)
            .expect("read spawned pid")
            .parse::<u32>()
            .expect("parse spawned pid");
        assert!(!pid_is_alive(pid), "post-spawn failure must reap child");
        assert_eq!(
            read_runtime_state(&state_path).expect("read childless state after failed spawn"),
            RuntimeStateRead::Loaded(vec![run.clone()])
        );
        assert_eq!(
            finish_childless_runtime_state_run(&state_path, &run.identity())
                .expect("finish failed-spawn state"),
            ChildlessFinishOutcome::Finished
        );
    }

    #[test]
    fn production_spawn_pipeline_holds_stop_lock_only_across_reservation_release_and_os_spawn() {
        fn assert_stop_lock_available(state_path: &Path) {
            let outcome = state::record_stop_request_with_lock_options_and_hook(
                state_path,
                "not-the-managed-model",
                Duration::ZERO,
                Duration::ZERO,
                |_| Ok(()),
            )
            .expect("stop mutation lock is available");
            assert_eq!(outcome, state::StopRequestMatch::NoMatch);
        }

        fn assert_stop_lock_unavailable(state_path: &Path) {
            let outcome = state::record_stop_request_with_lock_options_and_hook(
                state_path,
                "not-the-managed-model",
                Duration::ZERO,
                Duration::ZERO,
                |_| Ok(()),
            );
            assert!(
                matches!(
                    outcome,
                    Err(SupervisorError::Io(ref error))
                        if error.kind() == io::ErrorKind::WouldBlock
                ),
                "stop mutation lock must cover the true OS spawn boundary: {outcome:?}"
            );
        }

        for generation in [0, 1] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let mut run =
                childless_starting_run(temp.path(), &format!("run-production-spawn-g{generation}"));
            run.generation = generation;
            run.generation_alias = format!("loxa-{}-g{generation}", run.run_id);
            let reservation = reserve_localhost_port(None).expect("reserve localhost port");
            run.port = reservation.port();
            create_starting_run(&state_path, run.clone()).expect("publish childless generation");
            let entry = registry::find(&run.model_id).expect("registry entry");
            let spec = ServerSpec {
                entry,
                model_path: temp.path().join(entry.filename),
                llama_server_path: std::env::current_exe().expect("current test executable"),
                port: run.port,
                ctx_tokens: DEFAULT_CTX_TOKENS,
                generation_alias: run.generation_alias.clone(),
            };
            let log_path = run.log_path.clone();
            let outcome = spawn_starting_llama_server_with_hooks(
                &state_path,
                &run.identity(),
                &spec,
                &log_path,
                reservation,
                || assert_stop_lock_available(&state_path),
                || assert_stop_lock_unavailable(&state_path),
                || assert_stop_lock_unavailable(&state_path),
                || assert_stop_lock_available(&state_path),
            )
            .expect("production spawn pipeline");
            let SpawnStartingRunOutcome::Spawned {
                value: mut child, ..
            } = outcome
            else {
                panic!("unexpected requested stop");
            };

            assert_eq!(
                teardown_managed_child(&mut child, Duration::ZERO).expect("teardown test child"),
                TeardownConfirmation::Confirmed
            );
            assert_eq!(
                finish_childless_runtime_state_run(&state_path, &run.identity())
                    .expect("finish childless test state"),
                ChildlessFinishOutcome::Finished
            );
        }
    }

    #[test]
    fn production_facade_committed_childless_stop_suppresses_initial_and_replacement_spawn() {
        for generation in [0, 1] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let mut run = childless_starting_run(
                temp.path(),
                &format!("run-production-stopped-g{generation}"),
            );
            run.generation = generation;
            run.generation_alias = format!("loxa-{}-g{generation}", run.run_id);
            let reservation = reserve_localhost_port(None).expect("reserve localhost port");
            let reserved_port = reservation.port();
            run.port = reserved_port;
            create_starting_run(&state_path, run.clone()).expect("publish childless generation");
            let stopped = state::record_stop_request(&state_path, "all")
                .expect("commit stop during version/reservation interval");
            assert!(matches!(
                stopped,
                state::StopRequestMatch::Requested(ref current) if current.stop_requested
            ));
            let entry = registry::find(&run.model_id).expect("registry entry");
            let spec = ServerSpec {
                entry,
                model_path: temp.path().join(entry.filename),
                llama_server_path: std::env::current_exe().expect("current test executable"),
                port: run.port,
                ctx_tokens: DEFAULT_CTX_TOKENS,
                generation_alias: run.generation_alias.clone(),
            };
            let log_path = run.log_path.clone();
            let reservation_release_reached = Cell::new(false);
            let os_spawn_reached = Cell::new(false);
            let drain_setup_reached = Cell::new(false);

            let outcome = spawn_starting_llama_server_with_hooks(
                &state_path,
                &run.identity(),
                &spec,
                &log_path,
                reservation,
                || {},
                || reservation_release_reached.set(true),
                || os_spawn_reached.set(true),
                || drain_setup_reached.set(true),
            )
            .expect("committed childless stop outcome");

            assert!(matches!(outcome, SpawnStartingRunOutcome::RequestedStop));
            assert!(!reservation_release_reached.get());
            assert!(!os_spawn_reached.get());
            assert!(!drain_setup_reached.get());
            assert_eq!(
                read_runtime_state(&state_path).expect("read exact-finished stopped state"),
                RuntimeStateRead::Loaded(Vec::new())
            );
            let rebound = TcpListener::bind(("127.0.0.1", reserved_port))
                .expect("reservation is released without OS spawn");
            assert_eq!(
                rebound.local_addr().expect("rebound address").port(),
                reserved_port
            );
        }
    }

    #[test]
    fn resolve_model_path_returns_model_not_downloaded_for_missing_file() {
        let temp = tempdir().expect("tempdir");
        let error = match resolve_model_path("gemma-3-4b-it-q4", temp.path()) {
            Ok(_) => panic!("expected missing model error"),
            Err(error) => error,
        };

        match error {
            SupervisorError::ModelNotDownloaded(path) => {
                assert_eq!(path, temp.path().join("gemma-3-4b-it-Q4_K_M.gguf"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn inspect_managed_servers_marks_dead_pid_and_port_as_stale() {
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 999_999,
            port: 65_530,
            model_path: PathBuf::from("/tmp/model.gguf"),
            started_at_unix_s: 456,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(789),
        };

        let inspections = inspect_managed_servers(std::slice::from_ref(&server));

        assert_eq!(inspections.len(), 1);
        assert_eq!(
            inspections[0],
            ManagedServerInspection {
                server,
                pid_alive: false,
                port_alive: false,
                stale: true,
                process_identity_matches: false,
            }
        );
    }

    #[test]
    fn inspect_managed_servers_marks_identity_mismatch_as_stale() {
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: PathBuf::from("/tmp/model.gguf"),
            started_at_unix_s: 456,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let controller =
            FakeProcessController::new_with_start_times(vec![true], vec![true], vec![Some(222)]);

        let inspections = inspect_managed_servers_with(std::slice::from_ref(&server), &controller);

        assert_eq!(inspections.len(), 1);
        assert!(inspections[0].pid_alive);
        assert!(inspections[0].port_alive);
        assert!(!inspections[0].process_identity_matches);
        assert!(inspections[0].stale);
    }

    #[test]
    fn inspect_managed_run_requires_live_owner_identity() {
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: PathBuf::from("/tmp/model.gguf"),
            started_at_unix_s: 456,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = managed_run_for(&server);

        for owner_status in [
            OwnerIdentityStatus::Dead,
            OwnerIdentityStatus::Unavailable,
            OwnerIdentityStatus::Mismatched,
        ] {
            let inspection = inspect_managed_run_with(
                &run,
                &FixedOwnerIdentityProbe(owner_status),
                &FakeProcessController::new_with_start_times(
                    vec![true],
                    vec![true],
                    vec![Some(111)],
                ),
            );

            assert_eq!(
                inspection,
                ManagedRunInspection {
                    status: ManagedRunStatus::RecoveryRequired,
                    owner_status,
                }
            );
        }
    }

    #[test]
    fn inspect_managed_run_applies_live_owner_lifecycle_precedence() {
        let mut run = childless_starting_run(Path::new("/tmp"), "run-precedence");
        run.stop_requested = true;
        run.lifecycle = RunLifecycle::RecoveryRequired;
        let owner = FixedOwnerIdentityProbe(OwnerIdentityStatus::Live);
        let controller = FakeProcessController::new_with_start_times(vec![], vec![], vec![]);

        assert_eq!(
            inspect_managed_run_with(&run, &owner, &controller).status,
            ManagedRunStatus::Stopping
        );

        run.stop_requested = false;
        run.lifecycle = RunLifecycle::Stopping;
        assert_eq!(
            inspect_managed_run_with(&run, &owner, &controller).status,
            ManagedRunStatus::Stopping
        );

        run.lifecycle = RunLifecycle::RecoveryRequired;
        assert_eq!(
            inspect_managed_run_with(&run, &owner, &controller).status,
            ManagedRunStatus::RecoveryRequired
        );

        run.lifecycle = RunLifecycle::Restarting;
        assert_eq!(
            inspect_managed_run_with(&run, &owner, &controller).status,
            ManagedRunStatus::Starting
        );

        run.child_pid = Some(777);
        run.child_process_start_time_unix_s = Some(111);
        assert_eq!(
            inspect_managed_run_with(&run, &owner, &controller).status,
            ManagedRunStatus::RecoveryRequired
        );
    }

    #[test]
    fn inspect_managed_run_rejects_ambiguous_running_child_identity() {
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: PathBuf::from("/tmp/model.gguf"),
            started_at_unix_s: 456,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let owner = FixedOwnerIdentityProbe(OwnerIdentityStatus::Live);

        let mut missing_identity = managed_run_for(&server);
        missing_identity.child_process_start_time_unix_s = None;
        assert_eq!(
            inspect_managed_run_with(
                &missing_identity,
                &owner,
                &FakeProcessController::new_with_start_times(vec![true], vec![], vec![]),
            )
            .status,
            ManagedRunStatus::RecoveryRequired
        );

        assert_eq!(
            inspect_managed_run_with(
                &managed_run_for(&server),
                &owner,
                &FakeProcessController::new_with_start_times(vec![false], vec![], vec![]),
            )
            .status,
            ManagedRunStatus::RecoveryRequired
        );

        assert_eq!(
            inspect_managed_run_with(
                &managed_run_for(&server),
                &owner,
                &FakeProcessController::new_with_start_times(vec![true], vec![], vec![Some(222)],),
            )
            .status,
            ManagedRunStatus::RecoveryRequired
        );

        assert_eq!(
            inspect_managed_run_with(
                &managed_run_for(&server),
                &owner,
                &FakeProcessController::new_with_start_times(
                    vec![true],
                    vec![false],
                    vec![Some(111)],
                ),
            )
            .status,
            ManagedRunStatus::RecoveryRequired
        );
    }

    #[test]
    fn process_identity_transient_owner_and_child_misses_keep_run_inspection_running() {
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: PathBuf::from("/tmp/model.gguf"),
            started_at_unix_s: 456,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let owner = RetryingOwnerIdentityProbe::new(vec![true, true], vec![None, Some(456)]);
        let child = RetryingProcessController::new(true, true, vec![None, Some(111)]);

        let inspection = inspect_managed_run_with(&managed_run_for(&server), &owner, &child);

        assert_eq!(
            inspection,
            ManagedRunInspection {
                status: ManagedRunStatus::Running,
                owner_status: OwnerIdentityStatus::Live,
            }
        );
        assert_eq!(owner.lookups.get(), 2);
        assert_eq!(child.lookups.get(), 2);
    }

    #[test]
    fn process_identity_transient_owner_miss_preserves_external_stop_inspection() {
        let mut run = childless_starting_run(Path::new("/tmp"), "run-stopping");
        run.stop_requested = true;
        let owner = RetryingOwnerIdentityProbe::new(
            vec![true, true],
            vec![None, Some(run.owner_process_start_time_unix_s)],
        );
        let child = FakeProcessController::new_with_start_times(vec![], vec![], vec![]);

        let inspection = inspect_managed_run_with(&run, &owner, &child);

        assert_eq!(inspection.status, ManagedRunStatus::Stopping);
        assert_eq!(inspection.owner_status, OwnerIdentityStatus::Live);
        assert_eq!(owner.lookups.get(), 2);
    }

    #[test]
    fn process_identity_owner_that_dies_during_retry_is_dead() {
        let owner = RetryingOwnerIdentityProbe::new(vec![true, false], vec![None, Some(456)]);

        let status = owner.probe(42, 456);

        assert_eq!(status, OwnerIdentityStatus::Dead);
    }

    #[test]
    fn process_identity_mismatch_is_definitive_during_run_inspection() {
        let run = childless_starting_run(Path::new("/tmp"), "run-mismatch");
        let owner = RetryingOwnerIdentityProbe::new(
            vec![true, true],
            vec![Some(999), Some(run.owner_process_start_time_unix_s)],
        );
        let child = FakeProcessController::new_with_start_times(vec![], vec![], vec![]);

        let inspection = inspect_managed_run_with(&run, &owner, &child);

        assert_eq!(inspection.status, ManagedRunStatus::RecoveryRequired);
        assert_eq!(inspection.owner_status, OwnerIdentityStatus::Mismatched);
        assert_eq!(owner.lookups.get(), 1);
    }

    #[test]
    fn cleanup_after_ctrl_c_requires_owned_unix_group_before_removing_state() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        write_runtime_state(&state_path, &[managed_run_for(&server)]).expect("seed runtime state");

        let mut child = FakeChild::default();
        let result = cleanup_after_ctrl_c(
            &mut child,
            &state_path,
            &managed_run_for(&server).identity(),
            Duration::from_millis(10),
        )
        .expect("cleanup result");

        #[cfg(unix)]
        {
            assert_eq!(
                result,
                CleanupResult {
                    forced: false,
                    removed_state: false,
                }
            );
            assert_eq!(child.events, vec!["terminate", "try_wait", "try_wait"]);
            assert_eq!(
                read_runtime_state(&state_path).expect("runtime state after cleanup"),
                RuntimeStateRead::Loaded(vec![managed_run_for(&server)])
            );
        }
        #[cfg(not(unix))]
        {
            assert!(result.removed_state);
            assert_eq!(
                read_runtime_state(&state_path).expect("runtime state after cleanup"),
                RuntimeStateRead::Loaded(Vec::new())
            );
        }
    }

    #[test]
    fn cleanup_after_ctrl_c_cannot_remove_a_newer_generation_with_the_same_child() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        let stale_generation = managed_run_for(&server);
        let mut current = stale_generation.clone();
        current.generation = 1;
        current.generation_alias = format!("loxa-{}-g1", current.run_id);
        write_runtime_state(&state_path, &[current.clone()]).expect("seed newer generation");
        let mut child = FakeChild::default();

        let result = cleanup_after_ctrl_c(
            &mut child,
            &state_path,
            &stale_generation.identity(),
            Duration::from_millis(10),
        )
        .expect("cleanup result");

        assert!(!result.removed_state);
        assert_eq!(
            read_runtime_state(&state_path).expect("read newer generation"),
            RuntimeStateRead::Loaded(vec![current])
        );
    }

    #[test]
    fn cleanup_after_ctrl_c_preserves_state_when_force_kill_is_not_confirmed() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        write_runtime_state(&state_path, &[managed_run_for(&server)]).expect("seed runtime state");

        let mut child = FakeChild::with_wait_results(vec![None]);
        let result = cleanup_after_ctrl_c_with(
            &mut child,
            &state_path,
            &managed_run_for(&server).identity(),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            |_| {},
        )
        .expect("cleanup result");

        assert_eq!(
            result,
            CleanupResult {
                forced: true,
                removed_state: false,
            }
        );
        assert_eq!(
            child.events,
            vec!["terminate", "try_wait", "kill", "try_wait"]
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after cleanup"),
            RuntimeStateRead::Loaded(vec![managed_run_for(&server)])
        );
    }

    #[test]
    fn cleanup_after_ctrl_c_waits_briefly_after_force_kill_before_removing_state() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        write_runtime_state(&state_path, &[managed_run_for(&server)]).expect("seed runtime state");

        let mut child = FakeChild::with_wait_results(vec![None, Some(0)]);
        let result = cleanup_after_ctrl_c_with(
            &mut child,
            &state_path,
            &managed_run_for(&server).identity(),
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::ZERO,
            |_| {},
        )
        .expect("cleanup result");

        assert_eq!(
            result,
            CleanupResult {
                forced: true,
                removed_state: true,
            }
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after cleanup"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn child_teardown_confirmation_boundary_does_not_mutate_runtime_state() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = managed_run_for(&server);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");
        let mut child = FakeChild::with_wait_results(vec![Some(0)]);

        let confirmation = teardown_managed_child_with(
            &mut child,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            |_| {},
        )
        .expect("child teardown result")
        .confirmation;

        assert_eq!(confirmation, TeardownConfirmation::Confirmed);
        assert_eq!(child.events, vec!["terminate", "try_wait"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("state remains owner-controlled"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[cfg(unix)]
    #[test]
    fn persist_failure_with_missing_unix_owned_group_preserves_state_for_recovery() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: None,
        };
        let mut child = FakeChild::with_wait_results(vec![Some(0)]);

        let outcome = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting.clone(),
            server,
            Duration::from_millis(10),
        )
        .expect("missing owned group reports recovery");

        assert_eq!(outcome, PersistManagedServerOutcome::RecoveryRequired);
        assert_eq!(child.events, vec!["terminate", "try_wait"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after cleanup"),
            RuntimeStateRead::Loaded(vec![starting])
        );
    }

    #[test]
    fn process_identity_persistent_child_miss_after_committed_stop_uses_one_unified_teardown() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let mut stopped = starting.clone();
        stopped.stop_requested = true;
        assert!(
            update_runtime_state_run(&state_path, &starting.identity(), stopped)
                .expect("commit stop before identity failure")
        );
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: 777,
            port: starting.port,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: None,
        };
        let mut child = FakeChild::with_wait_results(vec![Some(0)]);
        let cleanup_calls = Cell::new(0_u8);

        let outcome = persist_managed_server_or_cleanup_with(
            &mut child,
            &state_path,
            starting,
            server,
            |_, path, expected| {
                cleanup_calls.set(cleanup_calls.get() + 1);
                lifecycle::finish_post_spawn_failure_with(path, expected, || {
                    TeardownConfirmation::Confirmed
                })
            },
        )
        .expect("committed stop wins attachment identity failure");

        assert_eq!(outcome, PersistManagedServerOutcome::RequestedStop);
        assert_eq!(cleanup_calls.get(), 1);
        assert_eq!(
            read_runtime_state(&state_path).expect("read exact-finished stopped run"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[cfg(unix)]
    #[test]
    fn production_attachment_identity_failure_honors_committed_stop_after_group_teardown() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let mut stopped = starting.clone();
        stopped.stop_requested = true;
        assert!(
            update_runtime_state_run(&state_path, &starting.identity(), stopped)
                .expect("commit stop before identity failure")
        );
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("owned-group.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let mut command = Command::new(std::env::current_exe().expect("current test binary"));
        command.arg("--exact").arg("__loxa_gate3_no_test_matches__");
        let mut child =
            spawn_managed_command(command, writer, || Ok(())).expect("spawn owned group child");
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: child.pid(),
            port: starting.port,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: None,
        };

        let outcome = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting,
            server,
            Duration::from_millis(10),
        )
        .expect("committed stop wins after production group teardown");

        assert_eq!(outcome, PersistManagedServerOutcome::RequestedStop);
        assert!(child.try_wait().expect("idempotent leader reap").is_some());
        assert_eq!(
            read_runtime_state(&state_path).expect("read exact-finished state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[cfg(unix)]
    #[test]
    fn attachment_cas_failure_invokes_unified_teardown_once_and_preserves_newer_state() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let mut newer = starting.clone();
        newer.generation = 1;
        newer.generation_alias = "loxa-run-1-g1".to_string();
        assert!(
            update_runtime_state_run(&state_path, &starting.identity(), newer.clone(),)
                .expect("publish newer state")
        );
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: 777,
            port: starting.port,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let mut child = FakeChild::with_wait_results(vec![Some(0)]);

        let outcome = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting,
            server,
            Duration::from_millis(10),
        )
        .expect("stale attachment reports recovery");

        assert_eq!(outcome, PersistManagedServerOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read preserved newer state"),
            RuntimeStateRead::Loaded(vec![newer])
        );
    }

    #[test]
    fn persist_managed_server_attaches_child_through_exact_starting_identity() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: 777,
            port: starting.port,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let mut child = FakeChild::default();

        let attached = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting,
            server,
            Duration::from_millis(10),
        )
        .expect("attach managed child");
        let PersistManagedServerOutcome::Attached(attached) = attached else {
            panic!("managed child must attach");
        };

        assert_eq!(attached.lifecycle, RunLifecycle::Running);
        assert_eq!(attached.child_pid, Some(777));
        assert_eq!(attached.child_process_start_time_unix_s, Some(111));
        assert_eq!(
            read_runtime_state(&state_path).expect("read attached run"),
            RuntimeStateRead::Loaded(vec![attached])
        );
        assert!(child.events.is_empty());
    }

    #[test]
    fn persist_managed_server_attaches_the_live_owned_pgid_for_diagnostics() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let server = ManagedServer {
            id: starting.model_id.clone(),
            pid: 777,
            port: starting.port,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let mut child = OwnedPgidFakeChild;

        let attached = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            starting,
            server,
            Duration::from_millis(10),
        )
        .expect("attach managed child");
        let PersistManagedServerOutcome::Attached(attached) = attached else {
            panic!("managed child must attach");
        };

        assert_eq!(attached.child_pgid, Some(777));
        assert_eq!(
            read_runtime_state(&state_path).expect("read attached run"),
            RuntimeStateRead::Loaded(vec![attached])
        );
    }

    #[test]
    fn bounded_log_writer_keeps_truncation_marker_when_write_exactly_exhausts_budget() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("bounded.log");
        let marker = b"\n[loxa log truncated]\n";
        let mut writer = BoundedLogWriter {
            file: File::create(&log_path).expect("create log"),
            remaining: marker.len() + 3,
            truncated: false,
        };
        let chunk = vec![b'x'; marker.len() + 3];

        writer.write_chunk(&chunk).expect("write chunk");
        drop(writer);

        let bytes = fs::read(&log_path).expect("read log");
        assert_eq!(&bytes[..3], b"xxx");
        assert_eq!(&bytes[3..], marker);
    }

    #[test]
    fn read_log_tail_replaces_invalid_utf8_when_offset_splits_codepoint() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("crash.log");
        fs::write(&log_path, b"ab\xf0\x9f\x92\xa5cd").expect("write log");

        let tail = read_log_tail(&log_path, 3).expect("read lossy tail");

        assert_eq!(tail, "\u{fffd}cd");
    }

    #[test]
    fn child_exit_crash_evidence_comes_from_captured_output_even_with_log_disable() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("captured.log");
        let mut file = File::create(&log_path).expect("create log");
        file.write_all(b"stdout prefix\n").expect("write stdout");
        file.write_all(b"\xf0\x28\x8c\x28panic on stderr\n")
            .expect("write stderr");
        file.flush().expect("flush log");

        let error = child_exited_early(&log_path).expect("child exited early");
        let args = llama_server_args(&ServerSpec {
            entry: registry::find("gemma-3-4b-it-q4").expect("registry entry"),
            model_path: temp.path().join("model.gguf"),
            llama_server_path: PathBuf::from("/tmp/llama-server"),
            port: 8080,
            ctx_tokens: DEFAULT_CTX_TOKENS,
            generation_alias: "loxa-test-run-g0".to_string(),
        });

        assert!(args.iter().any(|arg| arg == "--log-disable"));
        match error {
            SupervisorError::ChildExitedEarly(message) => {
                assert!(message.contains("stdout prefix"));
                assert!(message.contains("panic on stderr"));
                assert!(message.contains('\u{fffd}'));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    struct FakeChild {
        events: Vec<&'static str>,
        wait_results: Vec<Option<i32>>,
    }

    impl Default for FakeChild {
        fn default() -> Self {
            Self::with_wait_results(vec![None, Some(0)])
        }
    }

    impl FakeChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self {
                events: Vec::new(),
                wait_results,
            }
        }
    }

    impl ManagedChild for FakeChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.push("try_wait");
            if self.wait_results.len() > 1 {
                Ok(self.wait_results.remove(0))
            } else {
                Ok(self.wait_results.first().copied().unwrap_or(Some(0)))
            }
        }
    }

    impl LogDrainingChild for FakeChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.push("join_log_drains");
            Ok(())
        }
    }

    struct OwnedPgidFakeChild;

    impl ManagedChild for OwnedPgidFakeChild {
        fn pid(&self) -> u32 {
            777
        }

        fn owned_pgid(&self) -> Option<i32> {
            Some(777)
        }

        fn terminate(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            Ok(Some(0))
        }
    }

    impl LogDrainingChild for OwnedPgidFakeChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            Ok(())
        }
    }

    struct FakeProcessController {
        pid_alive: Mutex<Vec<bool>>,
        port_alive: Mutex<Vec<bool>>,
        process_start_times: Mutex<Vec<Option<u64>>>,
    }

    struct FixedOwnerIdentityProbe(OwnerIdentityStatus);

    struct RetryingOwnerIdentityProbe {
        alive: Mutex<Vec<bool>>,
        start_times: Mutex<Vec<Option<u64>>>,
        lookups: Cell<u8>,
    }

    struct RetryingProcessController {
        pid_alive: bool,
        port_alive: bool,
        start_times: Mutex<Vec<Option<u64>>>,
        lookups: Cell<u8>,
    }

    impl OwnerIdentityProbe for FixedOwnerIdentityProbe {
        fn probe(&self, _pid: u32, _expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
            self.0
        }
    }

    impl RetryingOwnerIdentityProbe {
        fn new(alive: Vec<bool>, start_times: Vec<Option<u64>>) -> Self {
            Self {
                alive: Mutex::new(alive),
                start_times: Mutex::new(start_times),
                lookups: Cell::new(0),
            }
        }
    }

    impl OwnerIdentityProbe for RetryingOwnerIdentityProbe {
        fn probe(&self, pid: u32, expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
            let elapsed = Cell::new(Duration::ZERO);
            probe_owner_identity_with(
                pid,
                expected_start_time_unix_s,
                |_| FakeProcessController::next_state(&self.alive),
                |pid| {
                    readiness::process_start_time_with_retry_with(
                        pid,
                        Duration::from_millis(100),
                        Duration::from_millis(25),
                        |_| {
                            self.lookups.set(self.lookups.get() + 1);
                            FakeProcessController::next_start_time(&self.start_times)
                        },
                        || elapsed.get(),
                        |duration| elapsed.set(elapsed.get() + duration),
                    )
                },
            )
        }
    }

    impl RetryingProcessController {
        fn new(pid_alive: bool, port_alive: bool, start_times: Vec<Option<u64>>) -> Self {
            Self {
                pid_alive,
                port_alive,
                start_times: Mutex::new(start_times),
                lookups: Cell::new(0),
            }
        }
    }

    impl ProcessController for RetryingProcessController {
        fn pid_is_alive(&self, _pid: u32) -> bool {
            self.pid_alive
        }

        fn port_is_alive(&self, _port: u16) -> bool {
            self.port_alive
        }

        fn process_start_time(&self, pid: u32) -> Option<u64> {
            let elapsed = Cell::new(Duration::ZERO);
            readiness::process_start_time_with_retry_with(
                pid,
                Duration::from_millis(100),
                Duration::from_millis(25),
                |_| {
                    self.lookups.set(self.lookups.get() + 1);
                    FakeProcessController::next_start_time(&self.start_times)
                },
                || elapsed.get(),
                |duration| elapsed.set(elapsed.get() + duration),
            )
        }
    }

    impl FakeProcessController {
        fn new_with_start_times(
            pid_alive: Vec<bool>,
            port_alive: Vec<bool>,
            process_start_times: Vec<Option<u64>>,
        ) -> Self {
            Self {
                pid_alive: Mutex::new(pid_alive),
                port_alive: Mutex::new(port_alive),
                process_start_times: Mutex::new(process_start_times),
            }
        }

        fn next_state(states: &Mutex<Vec<bool>>) -> bool {
            let mut states = states.lock().expect("states lock");
            if states.len() > 1 {
                states.remove(0)
            } else {
                *states.first().unwrap_or(&false)
            }
        }

        fn next_start_time(states: &Mutex<Vec<Option<u64>>>) -> Option<u64> {
            let mut states = states.lock().expect("start times lock");
            if states.len() > 1 {
                states.remove(0)
            } else {
                states.first().copied().unwrap_or(None)
            }
        }
    }

    impl ProcessController for FakeProcessController {
        fn pid_is_alive(&self, _pid: u32) -> bool {
            Self::next_state(&self.pid_alive)
        }

        fn port_is_alive(&self, _port: u16) -> bool {
            Self::next_state(&self.port_alive)
        }

        fn process_start_time(&self, _pid: u32) -> Option<u64> {
            Self::next_start_time(&self.process_start_times)
        }
    }
}
#[test]
fn managed_calibration_session_exposes_only_recorded_identity() {
    fn assert_send<T: Send>() {}
    assert_send::<crate::supervisor::ManagedCalibrationSession>();
}

#[test]
fn calibration_session_rejects_attached_state_drift() {
    let server = ManagedServer {
        id: "model".into(),
        pid: 10,
        port: 8080,
        model_path: PathBuf::from("model.gguf"),
        started_at_unix_s: 1,
        llama_server_version: "v".into(),
        process_start_time_unix_s: Some(2),
    };
    let mut expected = ManagedRun {
        schema_version: RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "run".into(),
        model_id: "model".into(),
        owner_pid: 1,
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: RunLifecycle::Running,
        generation: 0,
        generation_alias: "alias".into(),
        port: 8080,
        log_path: PathBuf::from("run.log"),
        child_pid: Some(10),
        child_process_start_time_unix_s: Some(2),
        child_pgid: Some(10),
    };
    let mut current = expected.clone();
    current.generation_alias.push_str("-drift");
    assert!(
        validate_calibration_attached_state(&expected, &current, &server, 10, true, Some(2))
            .is_err()
    );
    expected.stop_requested = true;
    assert!(
        validate_calibration_attached_state(&expected, &expected, &server, 10, true, Some(2))
            .is_err()
    );
}

#[test]
fn calibration_initialization_cleanup_preserves_terminal_truth() {
    assert!(matches!(
        resolve_calibration_initialization_failure(
            "run",
            SupervisorError::HealthTimeout,
            Ok(PostSpawnCleanupOutcome::RecoveryRequired)
        ),
        SupervisorError::RecoveryRequired(_)
    ));
    assert!(matches!(
        resolve_calibration_initialization_failure(
            "run",
            SupervisorError::HealthTimeout,
            Err(SupervisorError::RunStateConflict("cleanup".into()))
        ),
        SupervisorError::RunStateConflict(_)
    ));
}

#[test]
fn calibration_childless_finish_preserves_cleanup_truth() {
    assert!(matches!(
        resolve_calibration_childless_spawn_failure(
            "run",
            SupervisorError::HealthTimeout,
            Err(SupervisorError::RunStateConflict("finish".into()))
        ),
        SupervisorError::RunStateConflict(_)
    ));
    assert!(matches!(
        resolve_calibration_childless_spawn_failure(
            "run",
            SupervisorError::HealthTimeout,
            Ok(ChildlessFinishOutcome::RequestedStop)
        ),
        SupervisorError::RunStateConflict(_)
    ));
}
