use crate::registry::{self, ModelEntry};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, Signal, System};

mod lifecycle;
mod readiness;
mod state;
mod teardown;

pub use readiness::{reserve_localhost_port, LocalhostPortReservation};

pub use lifecycle::{
    decide_observed_child_exit, finish_childless_runtime_state_run, finish_owner_teardown_with,
    prepare_starting_run_for_spawn, ChildlessFinishOutcome, InterruptStatus, ObservedChildExit,
    OwnerTeardownDecision, OwnerTerminalOutcome, PostSpawnCleanupOutcome, PreSpawnDecision,
};
pub use state::{
    create_starting_run, current_runtime_state_run, finish_runtime_state_run, read_runtime_state,
    remove_runtime_state_entry, runtime_dir, runtime_logs_dir, runtime_state_path,
    update_runtime_state_run, update_runtime_state_run_committed, ManagedRun, ManagedRunIdentity,
    RunLifecycle, RuntimeStateRead, RUNTIME_STATE_LOCK_POLL_INTERVAL, RUNTIME_STATE_LOCK_TIMEOUT,
    RUNTIME_STATE_SCHEMA_VERSION,
};
use state::{record_stop_request, stable_run_is_present, StopRequestMatch};
#[cfg(test)]
use teardown::spawn_log_drain;
use teardown::spawn_managed_command;
pub use teardown::{
    teardown_managed_child, LogDrainingChild, ManagedChild, SpawnedServer, TeardownConfirmation,
};

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

impl OwnerIdentityProbe for SystemOwnerIdentityProbe {
    fn probe(&self, pid: u32, expected_start_time_unix_s: u64) -> OwnerIdentityStatus {
        if !pid_is_alive(pid) {
            return OwnerIdentityStatus::Dead;
        }

        match process_start_time(pid) {
            Some(actual) if actual == expected_start_time_unix_s => OwnerIdentityStatus::Live,
            Some(_) => OwnerIdentityStatus::Mismatched,
            None => OwnerIdentityStatus::Unavailable,
        }
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
        process_start_time(pid)
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

pub fn build_server_spec(
    id: &str,
    models_dir: &Path,
    requested_port: Option<u16>,
    requested_ctx: Option<u32>,
) -> Result<ServerSpec<'static>, SupervisorError> {
    let (entry, model_path) = resolve_model_path(id, models_dir)?;
    let llama_server_path = detect_llama_server()?;
    let port = choose_localhost_port(requested_port)?;

    Ok(ServerSpec {
        entry,
        model_path,
        llama_server_path,
        port,
        ctx_tokens: requested_ctx.unwrap_or(DEFAULT_CTX_TOKENS),
    })
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

pub fn choose_localhost_port(requested: Option<u16>) -> Result<u16, SupervisorError> {
    let address = SocketAddr::from(([127, 0, 0, 1], requested.unwrap_or(0)));
    let listener = TcpListener::bind(address).map_err(|_| SupervisorError::NoFreePort)?;
    let port = listener
        .local_addr()
        .map_err(|_| SupervisorError::NoFreePort)?
        .port();
    drop(listener);
    Ok(port)
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

pub fn spawn_llama_server(
    spec: &ServerSpec<'_>,
    log_path: &Path,
) -> Result<SpawnedServer, SupervisorError> {
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)?;

    let mut command = Command::new(&spec.llama_server_path);
    for arg in llama_server_args(spec) {
        command.arg(arg);
    }

    let writer = Arc::new(Mutex::new(BoundedLogWriter {
        file: log_file,
        remaining: MAX_LOG_BYTES,
        truncated: false,
    }));

    spawn_managed_command(command, writer)
}

pub fn wait_for_health(
    port: u16,
    timeout: Duration,
    interval: Duration,
) -> Result<(), SupervisorError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(health_request_timeout(timeout, interval))
        .build()?;
    let started = Instant::now();

    while started.elapsed() < timeout {
        if server_ready(&client, port) {
            return Ok(());
        }

        thread::sleep(interval);
    }

    Err(SupervisorError::HealthTimeout)
}

pub fn wait_for_health_or_exit<C: ManagedChild>(
    child: &mut C,
    port: u16,
    _log_path: &Path,
    timeout: Duration,
    interval: Duration,
) -> Result<(), SupervisorError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(health_request_timeout(timeout, interval))
        .build()?;
    let started = Instant::now();

    while started.elapsed() < timeout {
        if server_ready(&client, port) {
            return Ok(());
        }

        if child.try_wait()?.is_some() {
            return Err(SupervisorError::ChildExitedEarly(String::new()));
        }

        thread::sleep(interval);
    }

    if child.try_wait()?.is_some() {
        return Err(SupervisorError::ChildExitedEarly(String::new()));
    }

    Err(SupervisorError::HealthTimeout)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthEndpointProbe {
    Healthy,
    Unsupported,
    NotReady,
}

fn server_ready(client: &reqwest::blocking::Client, port: u16) -> bool {
    match probe_health_endpoint(client, port) {
        HealthEndpointProbe::Healthy => true,
        HealthEndpointProbe::Unsupported => endpoint_healthy(client, port, "/v1/models"),
        HealthEndpointProbe::NotReady => false,
    }
}

fn probe_health_endpoint(client: &reqwest::blocking::Client, port: u16) -> HealthEndpointProbe {
    let url = format!("http://127.0.0.1:{port}/health");
    match client.get(url).send() {
        Ok(response) if response.status().is_success() => HealthEndpointProbe::Healthy,
        Ok(response) if health_endpoint_unsupported(response.status()) => {
            HealthEndpointProbe::Unsupported
        }
        Ok(_) | Err(_) => HealthEndpointProbe::NotReady,
    }
}

fn health_endpoint_unsupported(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 404 | 405 | 501)
}

fn endpoint_healthy(client: &reqwest::blocking::Client, port: u16, path: &str) -> bool {
    let url = format!("http://127.0.0.1:{port}{path}");
    client
        .get(url)
        .send()
        .map(|response| response.status().is_success())
        .unwrap_or(false)
}

fn health_request_timeout(timeout: Duration, interval: Duration) -> Duration {
    let timeout = timeout.min(interval).min(Duration::from_secs(1));
    timeout.max(Duration::from_millis(1))
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

pub fn process_start_time(pid: u32) -> Option<u64> {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(|process| process.start_time())
}

fn llama_server_args(spec: &ServerSpec<'_>) -> Vec<String> {
    vec![
        "--model".to_string(),
        spec.model_path.display().to_string(),
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
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
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

    #[test]
    fn choose_localhost_port_returns_bindable_localhost_port() {
        let port = choose_localhost_port(None).expect("choose localhost port");
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind chosen port");

        assert_eq!(
            listener.local_addr().expect("local addr").ip().to_string(),
            "127.0.0.1"
        );
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
    fn attachment_identity_failure_after_committed_stop_returns_requested_stop_after_one_cleanup() {
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
        let mut child = spawn_managed_command(command, writer).expect("spawn owned group child");
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

    #[test]
    fn wait_for_health_or_exit_reports_reaped_child_without_preteardown_drain_or_tail_read() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("captured.log");
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&log_path)
            .expect("create log");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: log_file,
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let handle = spawn_log_drain(
            DelayedReader::new(b"startup panic\n".to_vec(), Duration::from_millis(5)),
            writer,
        )
        .expect("spawn startup log drain");
        let mut child = FakeSpawnedServer::new(vec![handle]);

        let error = wait_for_health_or_exit(
            &mut child,
            65_530,
            &log_path,
            Duration::from_millis(10),
            Duration::from_millis(1),
        )
        .expect_err("expected child exit");

        match error {
            SupervisorError::ChildExitedEarly(message) => {
                assert!(message.is_empty());
                assert_eq!(child.log_drains.len(), 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn wait_for_health_keeps_polling_when_health_is_unhealthy() {
        let server = TestHttpServer::spawn(vec![
            ("/health", TestHttpAction::Status(503)),
            ("/v1/models", TestHttpAction::Status(200)),
        ]);

        let result = wait_for_health(
            server.port(),
            Duration::from_millis(80),
            Duration::from_millis(10),
        );

        assert!(
            matches!(result, Err(SupervisorError::HealthTimeout)),
            "unexpected result: {result:?}; requests: {:?}",
            server.requests()
        );
        assert!(!server.requests().iter().any(|path| path == "/v1/models"));
    }

    #[test]
    fn wait_for_health_keeps_polling_when_health_connection_fails() {
        let server = TestHttpServer::spawn(vec![
            ("/health", TestHttpAction::Close),
            ("/v1/models", TestHttpAction::Status(200)),
        ]);

        let error = wait_for_health(
            server.port(),
            Duration::from_millis(80),
            Duration::from_millis(10),
        )
        .expect_err("expected failed /health connection to keep polling");

        assert!(matches!(error, SupervisorError::HealthTimeout));
        assert!(!server.requests().iter().any(|path| path == "/v1/models"));
    }

    #[test]
    fn wait_for_health_falls_back_to_models_when_health_is_unsupported() {
        let server = TestHttpServer::spawn(vec![
            ("/health", TestHttpAction::Status(404)),
            ("/v1/models", TestHttpAction::Status(200)),
        ]);

        wait_for_health(
            server.port(),
            Duration::from_millis(80),
            Duration::from_millis(10),
        )
        .expect("expected unsupported /health to fall back to models");

        let requests = server.requests();
        assert!(requests.iter().any(|path| path == "/health"));
        assert!(requests.iter().any(|path| path == "/v1/models"));
    }

    #[test]
    fn wait_for_health_or_exit_keeps_polling_when_health_is_unhealthy() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("captured.log");
        let mut child = FakeChild::with_wait_results(vec![None]);
        let server = TestHttpServer::spawn(vec![
            ("/health", TestHttpAction::Status(503)),
            ("/v1/models", TestHttpAction::Status(200)),
        ]);

        let error = wait_for_health_or_exit(
            &mut child,
            server.port(),
            &log_path,
            Duration::from_millis(80),
            Duration::from_millis(10),
        )
        .expect_err("expected unhealthy /health to keep polling");

        assert!(matches!(error, SupervisorError::HealthTimeout));
        assert!(!server.requests().iter().any(|path| path == "/v1/models"));
        assert!(child.events.contains(&"try_wait"));
    }

    #[derive(Clone, Copy)]
    enum TestHttpAction {
        Status(u16),
        Close,
    }

    struct TestHttpServer {
        port: u16,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl TestHttpServer {
        fn spawn(routes: Vec<(&'static str, TestHttpAction)>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
            let port = listener.local_addr().expect("local addr").port();
            listener
                .set_nonblocking(true)
                .expect("set test server nonblocking");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let server_requests = Arc::clone(&requests);
            let server_stop = Arc::clone(&stop);

            let handle = thread::spawn(move || {
                while !server_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _addr)) => {
                            let path = read_request_path(&mut stream);
                            server_requests
                                .lock()
                                .expect("lock requests")
                                .push(path.clone());
                            let action = routes
                                .iter()
                                .find(|(route, _action)| *route == path)
                                .map(|(_route, action)| *action)
                                .unwrap_or(TestHttpAction::Close);
                            respond_to_test_request(&mut stream, action);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                port,
                requests,
                stop,
                handle: Some(handle),
            }
        }

        fn port(&self) -> u16 {
            self.port
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().expect("lock requests").clone()
        }
    }

    impl Drop for TestHttpServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(("127.0.0.1", self.port));
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn read_request_path(stream: &mut TcpStream) -> String {
        let mut buffer = [0_u8; 1024];
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        let read = stream.read(&mut buffer).unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]);

        request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string()
    }

    fn respond_to_test_request(stream: &mut TcpStream, action: TestHttpAction) {
        let TestHttpAction::Status(status) = action else {
            return;
        };
        let reason = match status {
            200 => "OK",
            404 => "Not Found",
            405 => "Method Not Allowed",
            501 => "Not Implemented",
            503 => "Service Unavailable",
            _ => "Test Status",
        };
        let body = if status == 200 { "{}" } else { "" };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );

        stream
            .write_all(response.as_bytes())
            .expect("write test response");
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

    struct FakeSpawnedServer {
        wait_results: Vec<Option<i32>>,
        log_drains: Vec<thread::JoinHandle<io::Result<()>>>,
    }

    impl FakeSpawnedServer {
        fn new(log_drains: Vec<thread::JoinHandle<io::Result<()>>>) -> Self {
            Self {
                wait_results: vec![Some(1)],
                log_drains,
            }
        }
    }

    impl ManagedChild for FakeSpawnedServer {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            if self.wait_results.len() > 1 {
                Ok(self.wait_results.remove(0))
            } else {
                Ok(self.wait_results.first().copied().unwrap_or(Some(1)))
            }
        }
    }

    impl LogDrainingChild for FakeSpawnedServer {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            for drain in self.log_drains.drain(..) {
                drain
                    .join()
                    .map_err(|_| SupervisorError::Io(io::Error::other("drain panic")))?
                    .map_err(SupervisorError::Io)?;
            }
            Ok(())
        }
    }

    struct FakeProcessController {
        pid_alive: Mutex<Vec<bool>>,
        port_alive: Mutex<Vec<bool>>,
        process_start_times: Mutex<Vec<Option<u64>>>,
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

    struct DelayedReader {
        bytes: Vec<u8>,
        delay: Duration,
        offset: usize,
        slept: bool,
    }

    impl DelayedReader {
        fn new(bytes: Vec<u8>, delay: Duration) -> Self {
            Self {
                bytes,
                delay,
                offset: 0,
                slept: false,
            }
        }
    }

    impl Read for DelayedReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if !self.slept {
                thread::sleep(self.delay);
                self.slept = true;
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }

            let remaining = &self.bytes[self.offset..];
            let read = remaining.len().min(buffer.len());
            buffer[..read].copy_from_slice(&remaining[..read]);
            self.offset += read;
            Ok(read)
        }
    }
}
