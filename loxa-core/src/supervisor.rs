use crate::registry::{self, ModelEntry};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, ProcessesToUpdate, Signal, System};

pub const DEFAULT_CTX_TOKENS: u32 = 8_192;
pub const CTRL_C_GRACE_PERIOD: Duration = Duration::from_secs(5);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(60);
pub const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub const LLAMA_SERVER_VERSION_TIMEOUT: Duration = Duration::from_secs(5);
pub const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub const FORCE_KILL_CONFIRMATION_PERIOD: Duration = Duration::from_millis(250);
pub const RUNTIME_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub const RUNTIME_STATE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);
pub const STOP_OWNER_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
pub const LOG_TAIL_BYTES: usize = 8 * 1024;
pub const MAX_LOG_BYTES: usize = 1024 * 1024;
pub const RUNTIME_STATE_SCHEMA_VERSION: u32 = 2;

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

pub struct SpawnedServer {
    child: Child,
    log_drains: Vec<JoinHandle<()>>,
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
pub enum RuntimeStateRead {
    Missing,
    Loaded(Vec<ManagedRun>),
    Legacy,
    Corrupt(String),
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
pub struct StopResult {
    pub was_running: bool,
    pub forced: bool,
    pub removed_state: bool,
    pub pid_alive: bool,
    pub port_alive: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartPolicy {
    remaining_restarts: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservedChildExit {
    Interrupted,
    Restart,
    Crash { log_tail: String },
}

pub trait ManagedChild {
    fn pid(&self) -> u32;
    fn terminate(&mut self) -> io::Result<()>;
    fn kill(&mut self) -> io::Result<()>;
    fn try_wait(&mut self) -> io::Result<Option<i32>>;
}

pub trait LogDrainingChild {
    fn join_log_drains(&mut self) -> Result<(), SupervisorError>;
}

trait ProcessController {
    fn pid_is_alive(&self, pid: u32) -> bool;
    fn port_is_alive(&self, port: u16) -> bool;
    fn process_start_time(&self, pid: u32) -> Option<u64>;
    fn terminate(&self, pid: u32) -> io::Result<()>;
    fn kill(&self, pid: u32) -> io::Result<()>;
}

pub trait InterruptStatus {
    fn interrupted(&self) -> bool;
}

struct SystemProcessController;

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

impl ManagedChild for Child {
    fn pid(&self) -> u32 {
        self.id()
    }

    fn terminate(&mut self) -> io::Result<()> {
        signal_pid(self.id(), Signal::Term)
    }

    fn kill(&mut self) -> io::Result<()> {
        if signal_pid(self.id(), Signal::Kill).is_err() {
            return Child::kill(self);
        }

        Ok(())
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        Ok(self
            .try_wait()?
            .map(|status| status.code().unwrap_or_default()))
    }
}

impl SpawnedServer {
    pub fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
        for drain in mem::take(&mut self.log_drains) {
            drain
                .join()
                .map_err(|_| SupervisorError::Io(io::Error::other("log drain thread panicked")))?;
        }
        Ok(())
    }
}

impl ManagedChild for SpawnedServer {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn terminate(&mut self) -> io::Result<()> {
        signal_pid(self.child.id(), Signal::Term)
    }

    fn kill(&mut self) -> io::Result<()> {
        if signal_pid(self.child.id(), Signal::Kill).is_err() {
            return Child::kill(&mut self.child);
        }

        Ok(())
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        Ok(self
            .child
            .try_wait()?
            .map(|status| status.code().unwrap_or_default()))
    }
}

impl LogDrainingChild for SpawnedServer {
    fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
        SpawnedServer::join_log_drains(self)
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

pub fn runtime_dir() -> PathBuf {
    home_dir().join(".loxa").join("run")
}

pub fn runtime_state_path() -> PathBuf {
    runtime_dir().join("managed.json")
}

pub fn runtime_logs_dir() -> PathBuf {
    runtime_dir().join("logs")
}

pub fn log_file_path(id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
    runtime_logs_dir().join(format!("{id}-{port}-{started_at_unix_s}.log"))
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

fn write_runtime_state(path: &Path, runs: &[ManagedRun]) -> Result<(), SupervisorError> {
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

    servers.retain(|existing| existing.identity() != server.identity());
    servers.push(server);
    write_runtime_state(path, &servers)
}

pub fn persist_managed_server_or_cleanup<C>(
    child: &mut C,
    state_path: &Path,
    server: ManagedServer,
    grace_period: Duration,
) -> Result<(), SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let identity = server.identity();
    match upsert_runtime_state_entry(state_path, server) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = cleanup_after_ctrl_c(child, state_path, identity, grace_period);
            let _ = child.join_log_drains();
            Err(error)
        }
    }
}

pub fn remove_runtime_state_entry(
    path: &Path,
    identity: ManagedServerIdentity,
) -> Result<bool, SupervisorError> {
    remove_runtime_state_entry_with_lock_options(
        path,
        identity,
        RUNTIME_STATE_LOCK_TIMEOUT,
        RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )
}

fn remove_runtime_state_entry_with_lock_options(
    path: &Path,
    identity: ManagedServerIdentity,
    timeout: Duration,
    interval: Duration,
) -> Result<bool, SupervisorError> {
    let _lock = RuntimeStateLock::acquire(path, timeout, interval)?;
    let RuntimeStateRead::Loaded(mut servers) = read_runtime_state(path)? else {
        return Ok(false);
    };

    let original_len = servers.len();
    servers.retain(|server| server.identity() != identity);
    if servers.len() == original_len {
        return Ok(false);
    }

    write_runtime_state(path, &servers)?;
    Ok(true)
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
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let writer = Arc::new(Mutex::new(BoundedLogWriter {
        file: log_file,
        remaining: MAX_LOG_BYTES,
        truncated: false,
    }));

    let mut log_drains = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        log_drains.push(spawn_log_drain(stdout, Arc::clone(&writer)));
    }
    if let Some(stderr) = child.stderr.take() {
        log_drains.push(spawn_log_drain(stderr, writer));
    }

    Ok(SpawnedServer { child, log_drains })
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

pub fn wait_for_health_or_exit<C: ManagedChild + LogDrainingChild>(
    child: &mut C,
    port: u16,
    log_path: &Path,
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
            return Err(child_exited_early_with_drains(child, log_path)?);
        }

        thread::sleep(interval);
    }

    if child.try_wait()?.is_some() {
        return Err(child_exited_early_with_drains(child, log_path)?);
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

pub fn child_exited_early_with_drains<C: LogDrainingChild>(
    child: &mut C,
    log_path: &Path,
) -> Result<SupervisorError, SupervisorError> {
    child.join_log_drains()?;
    child_exited_early(log_path)
}

pub fn stop_managed_server(
    server: &ManagedServer,
    state_path: &Path,
    grace_period: Duration,
) -> Result<StopResult, SupervisorError> {
    stop_managed_server_with(
        server,
        state_path,
        grace_period,
        FORCE_KILL_CONFIRMATION_PERIOD,
        STOP_POLL_INTERVAL,
        &SystemProcessController,
        thread::sleep,
    )
}

pub fn cleanup_after_ctrl_c<C: ManagedChild>(
    child: &mut C,
    state_path: &Path,
    identity: ManagedServerIdentity,
    grace_period: Duration,
) -> Result<CleanupResult, SupervisorError> {
    cleanup_after_ctrl_c_with(
        child,
        state_path,
        identity,
        grace_period,
        FORCE_KILL_CONFIRMATION_PERIOD,
        STOP_POLL_INTERVAL,
        thread::sleep,
    )
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

fn spawn_log_drain<R>(mut reader: R, writer: Arc<Mutex<BoundedLogWriter>>) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            let read = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => read,
                Err(_) => break,
            };

            let mut writer = match writer.lock() {
                Ok(writer) => writer,
                Err(_) => break,
            };
            if writer.write_chunk(&buffer[..read]).is_err() {
                break;
            }
        }
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

struct RuntimeStateLock {
    _file: File,
}

fn acquire_runtime_state_lock_for_mutation(
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

fn stop_managed_server_with<P, S>(
    server: &ManagedServer,
    state_path: &Path,
    grace_period: Duration,
    force_kill_confirmation: Duration,
    interval: Duration,
    controller: &P,
    mut sleep: S,
) -> Result<StopResult, SupervisorError>
where
    P: ProcessController,
    S: FnMut(Duration),
{
    let initial_status = inspect_stop_status(server, controller);
    let was_running = initial_status.pid_alive && initial_status.identity_matches;

    if !was_running {
        let removed_state = remove_runtime_state_entry(state_path, server.identity())?;

        return Ok(StopResult {
            was_running: false,
            forced: false,
            removed_state,
            pid_alive: initial_status.pid_alive,
            port_alive: initial_status.port_alive,
        });
    }

    controller.terminate(server.pid)?;

    let stopped = wait_for_stop_confirmation(grace_period, interval, &mut sleep, || {
        Ok(inspect_stop_status(server, controller))
    })?;
    if let Some(status) = stopped {
        let removed_state = remove_runtime_state_entry(state_path, server.identity())?;
        return Ok(StopResult {
            was_running,
            forced: false,
            removed_state,
            pid_alive: status.pid_alive,
            port_alive: status.port_alive,
        });
    }

    let pre_kill_status = inspect_stop_status(server, controller);
    if !pre_kill_status.pid_alive || !pre_kill_status.identity_matches {
        let removed_state = remove_runtime_state_entry(state_path, server.identity())?;
        return Ok(StopResult {
            was_running,
            forced: false,
            removed_state,
            pid_alive: pre_kill_status.pid_alive,
            port_alive: pre_kill_status.port_alive,
        });
    }

    controller.kill(server.pid)?;
    let status =
        match wait_for_stop_confirmation(force_kill_confirmation, interval, &mut sleep, || {
            Ok(inspect_stop_status(server, controller))
        })? {
            Some(status) => status,
            None => inspect_stop_status(server, controller),
        };
    let removed_state = if !status.pid_alive || !status.identity_matches {
        remove_runtime_state_entry(state_path, server.identity())?
    } else {
        false
    };

    Ok(StopResult {
        was_running,
        forced: true,
        removed_state,
        pid_alive: status.pid_alive,
        port_alive: status.port_alive,
    })
}

fn cleanup_after_ctrl_c_with<C, S>(
    child: &mut C,
    state_path: &Path,
    identity: ManagedServerIdentity,
    grace_period: Duration,
    force_kill_confirmation: Duration,
    interval: Duration,
    mut sleep: S,
) -> Result<CleanupResult, SupervisorError>
where
    C: ManagedChild,
    S: FnMut(Duration),
{
    child.terminate()?;

    if wait_for_child_exit(child, grace_period, interval, &mut sleep)? {
        let removed_state = remove_runtime_state_entry(state_path, identity)?;
        return Ok(CleanupResult {
            forced: false,
            removed_state,
        });
    }

    child.kill()?;
    let confirmed_stopped =
        wait_for_child_exit(child, force_kill_confirmation, interval, &mut sleep)?;
    let removed_state = if confirmed_stopped {
        remove_runtime_state_entry(state_path, identity)?
    } else {
        false
    };

    Ok(CleanupResult {
        forced: true,
        removed_state,
    })
}

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

pub fn decide_observed_child_exit<I: InterruptStatus>(
    log_tail: String,
    state_path: &Path,
    identity: ManagedServerIdentity,
    interrupt: &I,
    restart_policy: &mut RestartPolicy,
) -> Result<ObservedChildExit, SupervisorError> {
    if interrupt.interrupted() {
        remove_runtime_state_entry(state_path, identity)?;
        return Ok(ObservedChildExit::Interrupted);
    }

    remove_runtime_state_entry(state_path, identity)?;
    if restart_policy.should_restart() {
        Ok(ObservedChildExit::Restart)
    } else {
        Ok(ObservedChildExit::Crash { log_tail })
    }
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
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Barrier;
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
            .arg("supervisor::tests::runtime_state_advisory_lock_recovers_after_helper_is_killed")
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
        write_runtime_state(&state_path, std::slice::from_ref(&expected))
            .expect("write runtime state");

        assert_eq!(
            read_runtime_state(&state_path).expect("read runtime state"),
            RuntimeStateRead::Loaded(vec![expected])
        );

        fs::write(&state_path, "{not-json").expect("write corrupt state");
        match read_runtime_state(&state_path).expect("corrupt state read") {
            RuntimeStateRead::Corrupt(message) => assert!(message.contains("expected")),
            other => panic!("unexpected runtime state: {other:?}"),
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
        write_runtime_state(&state_path, &[first.clone(), second.clone()])
            .expect("seed runtime state");

        let removed = remove_runtime_state_entry(&state_path, first.identity())
            .expect("remove matching runtime state");

        assert!(removed);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after removal"),
            RuntimeStateRead::Loaded(vec![second])
        );
    }

    #[test]
    fn cleanup_after_ctrl_c_removes_only_matching_instance() {
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
        let duplicate_id = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 888,
            port: 8082,
            model_path: temp.path().join("other-model.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        write_runtime_state(&state_path, &[server.clone(), duplicate_id.clone()])
            .expect("seed runtime state");

        let mut child = FakeChild::default();
        let result = cleanup_after_ctrl_c(
            &mut child,
            &state_path,
            server.identity(),
            Duration::from_millis(10),
        )
        .expect("cleanup result");

        assert_eq!(
            result,
            CleanupResult {
                forced: false,
                removed_state: true,
            }
        );
        assert_eq!(child.events, vec!["terminate", "try_wait", "try_wait"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after cleanup"),
            RuntimeStateRead::Loaded(vec![duplicate_id])
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
        let other = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 888,
            port: 8082,
            model_path: temp.path().join("other-model.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        write_runtime_state(&state_path, &[server.clone(), other.clone()])
            .expect("seed runtime state");

        let mut child = FakeChild::with_wait_results(vec![None]);
        let result = cleanup_after_ctrl_c_with(
            &mut child,
            &state_path,
            server.identity(),
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
            RuntimeStateRead::Loaded(vec![server, other])
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let mut child = FakeChild::with_wait_results(vec![None, Some(0)]);
        let result = cleanup_after_ctrl_c_with(
            &mut child,
            &state_path,
            server.identity(),
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
    fn stop_managed_server_uses_grace_then_force_and_removes_matching_instance() {
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
        let other = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 778,
            port: 8082,
            model_path: temp.path().join("other.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        write_runtime_state(&state_path, &[server.clone(), other.clone()])
            .expect("seed runtime state");

        let controller = FakeProcessController::new(
            vec![true, true, true, false],
            vec![true, true, true, false],
        );

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: true,
                forced: true,
                removed_state: true,
                pid_alive: false,
                port_alive: false,
            }
        );
        assert_eq!(controller.events(), vec!["terminate:777", "kill:777"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(vec![other])
        );
    }

    #[test]
    fn stop_managed_server_waits_briefly_after_force_kill_before_removing_state() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller = FakeProcessController::new(
            vec![true, true, true, false],
            vec![true, true, true, false],
        );

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: true,
                forced: true,
                removed_state: true,
                pid_alive: false,
                port_alive: false,
            }
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn stop_managed_server_removes_state_when_force_kill_leaves_only_port_alive() {
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
        let other = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 778,
            port: 8082,
            model_path: temp.path().join("other.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        write_runtime_state(&state_path, &[server.clone(), other.clone()])
            .expect("seed runtime state");

        let controller =
            FakeProcessController::new(vec![true, true, true, false], vec![true, true, true, true]);

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: true,
                forced: true,
                removed_state: true,
                pid_alive: false,
                port_alive: true,
            }
        );
        assert_eq!(controller.events(), vec!["terminate:777", "kill:777"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(vec![other])
        );
    }

    #[test]
    fn stop_managed_server_stops_matching_identity_even_when_port_is_dead() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller = FakeProcessController::new_with_start_times(
            vec![true, false],
            vec![false, false],
            vec![Some(1)],
        );

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: true,
                forced: false,
                removed_state: true,
                pid_alive: false,
                port_alive: false,
            }
        );
        assert_eq!(controller.events(), vec!["terminate:777"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn stop_managed_server_removes_stale_pid_dead_port_alive_without_signaling() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller =
            FakeProcessController::new_with_start_times(vec![false], vec![true], vec![Some(1)]);

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: false,
                forced: false,
                removed_state: true,
                pid_alive: false,
                port_alive: true,
            }
        );
        assert!(controller.events().is_empty());
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn stop_managed_server_removes_identity_mismatch_with_pid_and_port_alive_without_signaling() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller =
            FakeProcessController::new_with_start_times(vec![true], vec![true], vec![Some(222)]);

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: false,
                forced: false,
                removed_state: true,
                pid_alive: true,
                port_alive: true,
            }
        );
        assert!(controller.events().is_empty());
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn stop_managed_server_removes_missing_identity_without_signaling() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: None,
        };
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller =
            FakeProcessController::new_with_start_times(vec![true], vec![true], vec![Some(111)]);

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert!(!result.was_running);
        assert!(result.removed_state);
        assert!(result.pid_alive);
        assert!(result.port_alive);
        assert!(controller.events().is_empty());
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn stop_managed_server_rechecks_identity_before_force_kill() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");

        let controller = FakeProcessController::new_with_start_times(
            vec![true, true],
            vec![true, true],
            vec![Some(111), Some(222)],
        );

        let result = stop_managed_server_with(
            &server,
            &state_path,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            &controller,
            |_| {},
        )
        .expect("stop result");

        assert_eq!(
            result,
            StopResult {
                was_running: true,
                forced: false,
                removed_state: true,
                pid_alive: true,
                port_alive: true,
            }
        );
        assert_eq!(controller.events(), vec!["terminate:777"]);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after stop"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn upsert_runtime_state_entry_rejects_missing_process_identity() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: None,
        };

        let error = upsert_runtime_state_entry(&state_path, server)
            .expect_err("missing process identity should not persist");

        match error {
            SupervisorError::ProcessIdentityUnavailable(777) => {}
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after rejected upsert"),
            RuntimeStateRead::Missing
        );
    }

    #[test]
    fn persist_managed_server_or_cleanup_cleans_child_when_identity_is_missing() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
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

        let error = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            server,
            Duration::from_millis(10),
        )
        .expect_err("missing process identity should fail");

        match error {
            SupervisorError::ProcessIdentityUnavailable(777) => {}
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(
            child.events,
            vec!["terminate", "try_wait", "join_log_drains"]
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after cleanup"),
            RuntimeStateRead::Missing
        );
    }

    #[test]
    fn child_exit_decision_removes_state_when_restart_budget_is_exhausted() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");
        let mut restarts = RestartPolicy::default();
        assert!(restarts.should_restart());

        let decision = decide_observed_child_exit(
            "crash tail".to_string(),
            &state_path,
            server.identity(),
            &NeverInterrupted,
            &mut restarts,
        )
        .expect("child exit decision");

        assert_eq!(
            decision,
            ObservedChildExit::Crash {
                log_tail: "crash tail".to_string(),
            }
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after crash"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn child_exit_decision_treats_interrupt_race_as_ctrl_c_cleanup() {
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
        write_runtime_state(&state_path, std::slice::from_ref(&server))
            .expect("seed runtime state");
        let mut restarts = RestartPolicy::default();

        let decision = decide_observed_child_exit(
            "crash tail".to_string(),
            &state_path,
            server.identity(),
            &AlwaysInterrupted,
            &mut restarts,
        )
        .expect("child exit decision");

        assert_eq!(decision, ObservedChildExit::Interrupted);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after interrupt"),
            RuntimeStateRead::Loaded(Vec::new())
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
    fn child_exited_early_waits_for_log_drains_before_reading_tail() {
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
            DelayedReader::new(
                b"delayed stderr panic\n".to_vec(),
                Duration::from_millis(20),
            ),
            writer,
        );
        let mut child = FakeSpawnedServer::new(vec![handle]);

        let error =
            child_exited_early_with_drains(&mut child, &log_path).expect("child exited early");

        match error {
            SupervisorError::ChildExitedEarly(message) => {
                assert!(message.contains("delayed stderr panic"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn wait_for_health_or_exit_reports_child_exit_with_drained_log_tail() {
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
        );
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
                assert!(message.contains("startup panic"));
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

    struct FakeSpawnedServer {
        wait_results: Vec<Option<i32>>,
        log_drains: Vec<thread::JoinHandle<()>>,
    }

    impl FakeSpawnedServer {
        fn new(log_drains: Vec<thread::JoinHandle<()>>) -> Self {
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
                    .map_err(|_| SupervisorError::Io(io::Error::other("drain panic")))?;
            }
            Ok(())
        }
    }

    struct NeverInterrupted;

    impl InterruptStatus for NeverInterrupted {
        fn interrupted(&self) -> bool {
            false
        }
    }

    struct AlwaysInterrupted;

    impl InterruptStatus for AlwaysInterrupted {
        fn interrupted(&self) -> bool {
            true
        }
    }

    struct FakeProcessController {
        events: Mutex<Vec<String>>,
        pid_alive: Mutex<Vec<bool>>,
        port_alive: Mutex<Vec<bool>>,
        process_start_times: Mutex<Vec<Option<u64>>>,
    }

    impl FakeProcessController {
        fn new(pid_alive: Vec<bool>, port_alive: Vec<bool>) -> Self {
            Self::new_with_start_times(pid_alive, port_alive, vec![Some(1)])
        }

        fn new_with_start_times(
            pid_alive: Vec<bool>,
            port_alive: Vec<bool>,
            process_start_times: Vec<Option<u64>>,
        ) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                pid_alive: Mutex::new(pid_alive),
                port_alive: Mutex::new(port_alive),
                process_start_times: Mutex::new(process_start_times),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().expect("events lock").clone()
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
