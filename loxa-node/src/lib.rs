use loxa_core::download;
use loxa_core::engine::{py_mlx_lm, EngineLaunchSpec, ReadinessStrategy, RuntimeBackendKind};
use loxa_core::registry::{self, ModelEntry, REGISTRY};
use loxa_core::supervisor::{
    self, InterruptStatus, LogDrainingChild, ManagedChild, ManagedServer, ObservedChildExit,
    RuntimeStateRead, SupervisorError,
};
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct NodePaths {
    pub models_dir: PathBuf,
    pub state_path: PathBuf,
    pub logs_dir: PathBuf,
}

impl NodePaths {
    pub fn detect() -> Self {
        Self {
            models_dir: download::model_dir(),
            state_path: supervisor::runtime_state_path(),
            logs_dir: supervisor::runtime_logs_dir(),
        }
    }

    pub fn log_path(&self, id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
        self.logs_dir
            .join(format!("{id}-{port}-{started_at_unix_s}.log"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    RequestedStop,
    Interrupted,
    Restart { run: supervisor::ManagedRun },
    Exhausted { log_tail: String },
    RecoveryRequired,
}

#[derive(Debug)]
pub enum ManagedAttachmentBoundary {
    Attached(supervisor::ManagedRun),
    Terminal(ExitCode),
}

#[derive(Debug)]
pub enum TypedManagedAttachmentBoundary {
    Attached(supervisor::ManagedRun),
    Terminal(RunTermination),
}

#[derive(Debug)]
pub enum OwnedReplacementPreparation<R, D> {
    Prepared {
        run: supervisor::ManagedRun,
        resolved: R,
        detected: D,
    },
    RequestedStop,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupPoll {
    Pending,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupWaitOutcome {
    Ready,
    RequestedStop,
    Interrupted,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyOutputOutcome {
    Ready,
    RequestedStop,
    RecoveryRequired,
}

#[derive(Debug)]
pub enum StartupWaitFailure {
    BeforeTeardown(SupervisorError),
    AfterTeardown(SupervisorError),
    AfterChildReaped {
        log_tail: String,
        diagnostics_error: Option<String>,
    },
}

pub struct RunSession<'a> {
    pub id: &'a str,
    pub state_identity: &'a supervisor::ManagedRunIdentity,
    pub log_path: &'a Path,
    pub state_path: &'a Path,
}

pub trait InterruptSource {
    fn interrupted(&self) -> bool;
}

static CTRL_C_RECEIVED: AtomicBool = AtomicBool::new(false);

pub fn clear_ctrl_c_received() {
    CTRL_C_RECEIVED.store(false, Ordering::SeqCst);
}

pub fn set_ctrl_c_received() {
    CTRL_C_RECEIVED.store(true, Ordering::SeqCst);
}

pub fn ctrl_c_received() -> bool {
    CTRL_C_RECEIVED.load(Ordering::SeqCst)
}

#[cfg(unix)]
extern "C" fn handle_sigint(_signal: std::ffi::c_int) {
    set_ctrl_c_received();
}

#[derive(Clone, Debug)]
pub struct ResolvedRuntimeBackend {
    pub kind: RuntimeBackendKind,
    pub model_id: String,
    pub model_path: PathBuf,
    pub program: PathBuf,
    pub engine_version: String,
}

impl ResolvedRuntimeBackend {
    pub fn launch_spec(
        &self,
        port: u16,
        ctx_tokens: u32,
        generation_alias: &str,
    ) -> EngineLaunchSpec {
        match self.kind {
            RuntimeBackendKind::LlamaCpp => EngineLaunchSpec {
                program: self.program.clone(),
                args: vec![
                    OsString::from("--model"),
                    self.model_path.as_os_str().to_owned(),
                    OsString::from("--alias"),
                    OsString::from(generation_alias),
                    OsString::from("--host"),
                    OsString::from("127.0.0.1"),
                    OsString::from("--port"),
                    OsString::from(port.to_string()),
                    OsString::from("--ctx-size"),
                    OsString::from(ctx_tokens.to_string()),
                    OsString::from("--gpu-layers"),
                    OsString::from("auto"),
                    OsString::from("--flash-attn"),
                    OsString::from("auto"),
                    OsString::from("--metrics"),
                    OsString::from("--log-disable"),
                ],
                port,
                engine_name: "llama.cpp".to_string(),
                engine_version: self.engine_version.clone(),
                runtime_model: self.model_path.display().to_string(),
                upstream_model: generation_alias.to_string(),
                readiness: ReadinessStrategy::LlamaModelAlias {
                    expected_alias: generation_alias.to_string(),
                },
            },
            RuntimeBackendKind::PyMlxLm => {
                py_mlx_lm::launch_spec(&self.program, &self.model_path, port, &self.engine_version)
            }
        }
    }

    pub fn process_label(&self) -> &'static str {
        match self.kind {
            RuntimeBackendKind::LlamaCpp => "llama-server",
            RuntimeBackendKind::PyMlxLm => "mlx_lm.server",
        }
    }

    pub fn log_key(&self) -> &str {
        match self.kind {
            RuntimeBackendKind::LlamaCpp => &self.model_id,
            RuntimeBackendKind::PyMlxLm => "py-mlx-lm",
        }
    }
}

pub fn resolve_runtime_backend(
    kind: RuntimeBackendKind,
    id: &str,
    models_dir: &Path,
) -> Result<ResolvedRuntimeBackend, SupervisorError> {
    match kind {
        RuntimeBackendKind::LlamaCpp => {
            let (_, model_path) = supervisor::resolve_model_path(id, models_dir)?;
            let program = supervisor::detect_llama_server()?;
            let engine_version = supervisor::llama_server_version(&program)?;
            Ok(ResolvedRuntimeBackend {
                kind,
                model_id: id.to_string(),
                model_path,
                program,
                engine_version,
            })
        }
        RuntimeBackendKind::PyMlxLm => {
            py_mlx_lm::validate_current_platform().map_err(py_mlx_error_to_supervisor)?;
            let model_path = py_mlx_lm::canonicalize_model_dir(Path::new(id))
                .map_err(py_mlx_error_to_supervisor)?;
            let program = py_mlx_lm::discover_server_from_environment()
                .map_err(py_mlx_error_to_supervisor)?;
            let version_command =
                py_mlx_lm::discover_version_command(&program, std::env::var_os("PATH").as_deref())
                    .map_err(py_mlx_error_to_supervisor)?;
            let engine_version =
                py_mlx_lm::detect_version(&version_command).map_err(py_mlx_error_to_supervisor)?;
            Ok(ResolvedRuntimeBackend {
                kind,
                model_id: model_path.display().to_string(),
                model_path,
                program,
                engine_version,
            })
        }
    }
}

pub fn py_mlx_error_to_supervisor(error: py_mlx_lm::PyMlxLmError) -> SupervisorError {
    SupervisorError::Io(io::Error::other(error))
}

pub fn gateway_target(
    backend: &ResolvedRuntimeBackend,
    spec: &EngineLaunchSpec,
) -> loxa_core::gateway::EngineTarget {
    loxa_core::gateway::EngineTarget {
        base_url: format!("http://127.0.0.1:{}", spec.port),
        backend_alias: spec.upstream_model.clone(),
        engine: spec.engine_name.clone(),
        engine_version: spec.engine_version.clone(),
        model_id: backend.model_id.clone(),
        profile: format!("{}:{}", spec.engine_name, backend.model_id),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RunRequest<'a> {
    pub id: &'a str,
    pub ctx: Option<u32>,
    pub port: Option<u16>,
    pub engine: RuntimeBackendKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    NodeListening {
        port: u16,
        model_alias: String,
    },
    ModelReady {
        server: ManagedServer,
    },
    Restarting {
        process_label: String,
        before_healthy: bool,
    },
    EngineExited {
        process_label: String,
        model_id: String,
        before_healthy: bool,
        log_tail: String,
    },
    HealthTimeout {
        process_label: String,
        log_path: PathBuf,
    },
    RecoveryRequired {
        run_id: String,
    },
}

pub trait LifecycleEventSink {
    fn emit(&mut self, event: LifecycleEvent) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunTermination {
    RequestedStop,
    Interrupted,
    Failed,
    RecoveryRequired,
}

pub fn run_model(
    request: RunRequest<'_>,
    paths: &NodePaths,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    events: &mut dyn LifecycleEventSink,
) -> io::Result<RunTermination> {
    let RunRequest {
        id,
        ctx,
        port,
        engine,
    } = request;
    let mut initial_backend = match engine {
        RuntimeBackendKind::LlamaCpp => {
            let Some(_) = registry::find(id) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown model: {id}"),
                ));
            };
            None
        }
        RuntimeBackendKind::PyMlxLm => Some(
            resolve_runtime_backend(engine, id, &paths.models_dir)
                .map_err(supervisor_error_to_io)?,
        ),
    };

    ensure_runtime_state_is_mutable(&paths.state_path)?;

    let signal_guard = SignalGuard::install()?;
    let owner_pid = std::process::id();
    let owner_process_start_time_unix_s = supervisor::process_start_time_with_retry(owner_pid)
        .ok_or_else(|| {
            supervisor_error_to_io(SupervisorError::ProcessIdentityUnavailable(owner_pid))
        })?;
    let run_id = format!("run-{owner_pid}-{owner_process_start_time_unix_s}");
    let mut replacement_run: Option<supervisor::ManagedRun> = None;

    loop {
        let owned_replacement = replacement_run.take();
        let started_at_unix_s = unix_timestamp_now();
        let (backend, starting_run, initial_generation, initial_reservation) =
            if let Some(run) = owned_replacement {
                let preparation = prepare_owned_replacement_run(
                    &paths.state_path,
                    run,
                    &signal_guard,
                    || resolve_runtime_backend(engine, id, &paths.models_dir),
                    || Ok(()),
                );
                let preparation = match preparation {
                    Ok(preparation) => preparation,
                    Err(SupervisorError::ModelNotDownloaded(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("model not downloaded: {id}"),
                        ));
                    }
                    Err(error) => return Err(supervisor_error_to_io(error)),
                };
                match preparation {
                    OwnedReplacementPreparation::Prepared {
                        run,
                        resolved: backend,
                        detected: (),
                    } => (backend, run, false, None),
                    OwnedReplacementPreparation::RequestedStop => {
                        return Ok(RunTermination::RequestedStop)
                    }
                    OwnedReplacementPreparation::Interrupted => {
                        return Ok(RunTermination::Interrupted);
                    }
                }
            } else {
                if signal_guard.interrupted() {
                    return Ok(RunTermination::Interrupted);
                }
                let backend = match initial_backend
                    .take()
                    .map(Ok)
                    .unwrap_or_else(|| resolve_runtime_backend(engine, id, &paths.models_dir))
                {
                    Ok(resolved) => resolved,
                    Err(SupervisorError::ModelNotDownloaded(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("model not downloaded: {id}"),
                        ));
                    }
                    Err(error) => return Err(supervisor_error_to_io(error)),
                };
                if signal_guard.interrupted() {
                    return Ok(RunTermination::Interrupted);
                }
                let reservation =
                    supervisor::reserve_localhost_port(port).map_err(supervisor_error_to_io)?;
                let selected_port = reservation.port();
                let log_path = paths.log_path(backend.log_key(), selected_port, started_at_unix_s);
                (
                    backend,
                    supervisor::ManagedRun {
                        schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
                        run_id: run_id.clone(),
                        model_id: id.to_string(),
                        owner_pid,
                        owner_process_start_time_unix_s,
                        stop_requested: false,
                        lifecycle: supervisor::RunLifecycle::Starting,
                        generation: 0,
                        generation_alias: format!("loxa-{run_id}-g0"),
                        port: selected_port,
                        log_path,
                        child_pid: None,
                        child_process_start_time_unix_s: None,
                        child_pgid: None,
                    },
                    true,
                    Some(reservation),
                )
            };
        let log_path = starting_run.log_path.clone();
        let spec = backend.launch_spec(
            starting_run.port,
            ctx.unwrap_or(supervisor::DEFAULT_CTX_TOKENS),
            &starting_run.generation_alias,
        );
        let starting_run = if initial_generation {
            supervisor::create_starting_run(&paths.state_path, starting_run)
                .map_err(supervisor_error_to_io)?
        } else {
            starting_run
        };
        let replacement_port = spec.port;
        let reservation = match initial_reservation {
            Some(reservation) => Ok(reservation),
            None => supervisor::reserve_localhost_port(Some(replacement_port)),
        };
        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(error) => {
                return match supervisor::finish_childless_runtime_state_run(
                    &paths.state_path,
                    &starting_run.identity(),
                )
                .map_err(supervisor_error_to_io)?
                {
                    supervisor::ChildlessFinishOutcome::RequestedStop => {
                        Ok(RunTermination::RequestedStop)
                    }
                    supervisor::ChildlessFinishOutcome::Finished => {
                        Err(supervisor_error_to_io(error))
                    }
                }
            }
        };
        let spawn = supervisor::spawn_starting_engine(
            &paths.state_path,
            &starting_run.identity(),
            &spec,
            &log_path,
            reservation,
        );
        let (starting_run, mut child) = match spawn {
            Ok(supervisor::SpawnStartingRunOutcome::Spawned { run, value }) => (run, value),
            Ok(supervisor::SpawnStartingRunOutcome::RequestedStop) => {
                return Ok(RunTermination::RequestedStop)
            }
            Err(error) => {
                return match supervisor::finish_childless_runtime_state_run(
                    &paths.state_path,
                    &starting_run.identity(),
                )
                .map_err(supervisor_error_to_io)?
                {
                    supervisor::ChildlessFinishOutcome::RequestedStop => {
                        Ok(RunTermination::RequestedStop)
                    }
                    supervisor::ChildlessFinishOutcome::Finished => {
                        Err(supervisor_error_to_io(error))
                    }
                }
            }
        };
        let initialization_error = child.take_initialization_error();
        if let Some(outcome) = finish_spawn_initialization(
            &mut child,
            &paths.state_path,
            &starting_run.identity(),
            initialization_error,
        )
        .map_err(supervisor_error_to_io)?
        {
            return match outcome {
                supervisor::PostSpawnCleanupOutcome::RequestedStop => {
                    Ok(RunTermination::RequestedStop)
                }
                supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
                    emit_recovery_required(events, &starting_run.run_id)
                }
                supervisor::PostSpawnCleanupOutcome::Cleaned => Ok(RunTermination::Failed),
            };
        }
        let server = ManagedServer {
            id: backend.model_id.clone(),
            pid: child.pid(),
            port: spec.port,
            model_path: backend.model_path.clone(),
            started_at_unix_s,
            llama_server_version: backend.engine_version.clone(),
            process_start_time_unix_s: supervisor::process_start_time_with_retry(child.pid()),
        };

        if signal_guard.interrupted() {
            let outcome =
                finish_spawned_interrupt(&mut child, &paths.state_path, &starting_run.identity())
                    .map_err(supervisor_error_to_io)?;
            if outcome == supervisor::OwnerTerminalOutcome::RecoveryRequired {
                return emit_recovery_required(events, &starting_run.run_id);
            }
            return Ok(owner_terminal_termination(outcome));
        }

        let starting_run_id = starting_run.run_id.clone();
        let persist_outcome = supervisor::persist_managed_server_or_cleanup(
            &mut child,
            &paths.state_path,
            starting_run,
            server.clone(),
            supervisor::CTRL_C_GRACE_PERIOD,
        )
        .map_err(supervisor_error_to_io)?;
        let run = match resolve_managed_attachment_typed(persist_outcome) {
            TypedManagedAttachmentBoundary::Attached(run) => run,
            TypedManagedAttachmentBoundary::Terminal(termination) => {
                if termination == RunTermination::RecoveryRequired {
                    return emit_recovery_required(events, &starting_run_id);
                }
                return Ok(termination);
            }
        };
        let state_identity = run.identity();

        if let Some(outcome) = observe_attached_stop(&mut child, &paths.state_path, &state_identity)
            .map_err(supervisor_error_to_io)?
        {
            if outcome == supervisor::OwnerTerminalOutcome::RecoveryRequired {
                return emit_recovery_required(events, &run.run_id);
            }
            return Ok(owner_terminal_termination(outcome));
        }

        let readiness_worker = match &spec.readiness {
            ReadinessStrategy::LlamaModelAlias { .. } => Ok(None),
            ReadinessStrategy::ChatCompletionProbe { request_model } => {
                supervisor::spawn_chat_completion_readiness_worker(
                    server.port,
                    request_model.clone(),
                    supervisor::HEALTH_TIMEOUT,
                    supervisor::HEALTH_POLL_INTERVAL,
                )
                .map(Some)
            }
        };
        let startup = match readiness_worker {
            Ok(mut readiness_worker) => wait_for_startup_owned(
                &mut child,
                &state_identity,
                &paths.state_path,
                &signal_guard,
                readiness_worker.as_mut(),
                |child, timeout, interval| match supervisor::wait_for_engine_ready_or_exit(
                    child,
                    server.port,
                    &spec.readiness,
                    timeout,
                    interval,
                ) {
                    Ok(()) => Ok(StartupPoll::Ready),
                    Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                    Err(error) => Err(error),
                },
            ),
            Err(error) => {
                finish_owned_startup_failure(&mut child, &paths.state_path, &state_identity, error)
            }
        };
        match startup {
            Ok(StartupWaitOutcome::Ready) => {
                match emit_run_ready_owned(
                    events,
                    &server,
                    &mut child,
                    &paths.state_path,
                    &state_identity,
                )? {
                    ReadyOutputOutcome::Ready => {}
                    ReadyOutputOutcome::RequestedStop => return Ok(RunTermination::RequestedStop),
                    ReadyOutputOutcome::RecoveryRequired => {
                        return emit_recovery_required(events, &run.run_id)
                    }
                }
                if let Some(gateway) = gateway {
                    gateway.publish(gateway_target(&backend, &spec));
                }
                match supervise_running_server(
                    RunSession {
                        id: &backend.model_id,
                        state_identity: &state_identity,
                        log_path: &log_path,
                        state_path: &paths.state_path,
                    },
                    &mut child,
                    &signal_guard,
                    gateway,
                    backend.process_label(),
                    events,
                )? {
                    RunOutcome::RequestedStop => {
                        if let Some(gateway) = gateway {
                            gateway.withdraw();
                        }
                        return Ok(RunTermination::RequestedStop);
                    }
                    RunOutcome::Interrupted => {
                        if let Some(gateway) = gateway {
                            gateway.withdraw();
                        }
                        return Ok(RunTermination::Interrupted);
                    }
                    RunOutcome::Restart { run } => {
                        if let Some(gateway) = gateway {
                            gateway.withdraw();
                        }
                        replacement_run = Some(run);
                        continue;
                    }
                    RunOutcome::Exhausted { .. } => {
                        if let Some(gateway) = gateway {
                            gateway.withdraw();
                        }
                        return Ok(RunTermination::Failed);
                    }
                    RunOutcome::RecoveryRequired => {
                        if let Some(gateway) = gateway {
                            gateway.withdraw();
                        }
                        return emit_recovery_required(events, &run.run_id);
                    }
                }
            }
            Ok(StartupWaitOutcome::RequestedStop) => return Ok(RunTermination::RequestedStop),
            Ok(StartupWaitOutcome::Interrupted) => return Ok(RunTermination::Interrupted),
            Ok(StartupWaitOutcome::RecoveryRequired) => {
                return emit_recovery_required(events, &run.run_id)
            }
            Err(StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            }) => {
                let _ = (log_tail, diagnostics_error);
                match supervisor::handle_observed_child_exit(
                    &mut child,
                    &log_path,
                    &paths.state_path,
                    &state_identity,
                    &signal_guard,
                )
                .map_err(supervisor_error_to_io)?
                {
                    ObservedChildExit::RequestedStop => return Ok(RunTermination::RequestedStop),
                    ObservedChildExit::Interrupted => return Ok(RunTermination::Interrupted),
                    ObservedChildExit::Restart { run } => {
                        let _ = events.emit(LifecycleEvent::Restarting {
                            process_label: backend.process_label().to_string(),
                            before_healthy: true,
                        });
                        replacement_run = Some(run);
                        continue;
                    }
                    ObservedChildExit::Exhausted { log_tail } => {
                        events.emit(LifecycleEvent::EngineExited {
                            process_label: backend.process_label().to_string(),
                            model_id: backend.model_id.clone(),
                            before_healthy: true,
                            log_tail,
                        })?;
                        return Ok(RunTermination::Failed);
                    }
                    ObservedChildExit::RecoveryRequired => {
                        return emit_recovery_required(events, &run.run_id)
                    }
                }
            }
            Err(StartupWaitFailure::AfterTeardown(error)) => {
                return finish_startup_failure(events, &log_path, backend.process_label(), error);
            }
            Err(StartupWaitFailure::BeforeTeardown(error)) => {
                let cleanup = supervisor::cleanup_post_spawn_failure(
                    &mut child,
                    &paths.state_path,
                    &state_identity,
                )
                .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RequestedStop {
                    return Ok(RunTermination::RequestedStop);
                }
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &run.run_id);
                }
                return finish_startup_failure(events, &log_path, backend.process_label(), error);
            }
        }
    }
}

fn emit_recovery_required(
    events: &mut dyn LifecycleEventSink,
    run_id: &str,
) -> io::Result<RunTermination> {
    events.emit(LifecycleEvent::RecoveryRequired {
        run_id: run_id.to_string(),
    })?;
    Ok(RunTermination::RecoveryRequired)
}

fn owner_terminal_termination(outcome: supervisor::OwnerTerminalOutcome) -> RunTermination {
    match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => RunTermination::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => RunTermination::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => RunTermination::RecoveryRequired,
    }
}

fn finish_startup_failure(
    events: &mut dyn LifecycleEventSink,
    log_path: &Path,
    process_label: &str,
    error: SupervisorError,
) -> io::Result<RunTermination> {
    match error {
        SupervisorError::HealthTimeout => {
            events.emit(LifecycleEvent::HealthTimeout {
                process_label: process_label.to_string(),
                log_path: log_path.to_path_buf(),
            })?;
            Ok(RunTermination::Failed)
        }
        other => Err(supervisor_error_to_io(other)),
    }
}

const DEFAULT_GATEWAY_PORT: u16 = 11_435;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelSelectionError {
    UnknownModel { id: String },
    NotDownloaded { id: String },
    NoDownloadedModels { suggested_id: String },
    MissingModelRequest { backend: RuntimeBackendKind },
}

impl fmt::Display for ModelSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownModel { id } => write!(formatter, "unknown model: {id}"),
            Self::NotDownloaded { id } => write!(formatter, "model not downloaded: {id}"),
            Self::NoDownloadedModels { .. } => write!(formatter, "no registry model is downloaded"),
            Self::MissingModelRequest { backend } => {
                write!(formatter, "model request is required for backend {backend}")
            }
        }
    }
}

impl std::error::Error for ModelSelectionError {}

pub fn select_serve_model(
    models_dir: &Path,
    requested: Option<&str>,
) -> Result<&'static ModelEntry, ModelSelectionError> {
    if let Some(id) = requested {
        let entry = registry::find(id)
            .ok_or_else(|| ModelSelectionError::UnknownModel { id: id.to_string() })?;
        if !models_dir.join(entry.filename).is_file() {
            return Err(ModelSelectionError::NotDownloaded { id: id.to_string() });
        }
        return Ok(entry);
    }
    REGISTRY
        .iter()
        .find(|entry| models_dir.join(entry.filename).is_file())
        .ok_or_else(|| ModelSelectionError::NoDownloadedModels {
            suggested_id: REGISTRY[0].id.to_string(),
        })
}

pub fn serve_node(
    requested_model: Option<&str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    events: &mut dyn LifecycleEventSink,
) -> io::Result<RunTermination> {
    let model_id = match engine {
        RuntimeBackendKind::LlamaCpp => {
            select_serve_model(&paths.models_dir, requested_model)
                .map_err(io::Error::other)?
                .id
        }
        RuntimeBackendKind::PyMlxLm => requested_model.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                ModelSelectionError::MissingModelRequest { backend: engine },
            )
        })?,
    };
    let node_id = format!("loxa-node-{}", std::process::id());
    let gateway_state = loxa_core::gateway::GatewayState::new(node_id);
    let gateway = loxa_core::gateway::GatewayServer::start(
        port.unwrap_or(DEFAULT_GATEWAY_PORT),
        gateway_state.clone(),
    )?;
    events.emit(LifecycleEvent::NodeListening {
        port: gateway.port(),
        model_alias: "loxa".to_string(),
    })?;
    let outcome = run_model(
        RunRequest {
            id: model_id,
            ctx: None,
            port: None,
            engine,
        },
        paths,
        Some(&gateway_state),
        events,
    );
    gateway_state.withdraw();
    let shutdown = gateway.shutdown();
    match (outcome, shutdown) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(exit), Ok(())) => Ok(exit),
    }
}

pub fn render_post_cleanup_startup_failure<E: Write>(
    stderr: &mut E,
    log_path: &Path,
    process_label: &str,
    error: SupervisorError,
) -> io::Result<ExitCode> {
    match error {
        SupervisorError::HealthTimeout => {
            writeln!(
                stderr,
                "{process_label} did not become healthy within {} seconds",
                supervisor::HEALTH_TIMEOUT.as_secs()
            )?;
            writeln!(stderr, "log file: {}", log_path.display())?;
            Ok(ExitCode::from(1))
        }
        other => Err(supervisor_error_to_io(other)),
    }
}

pub fn resolve_managed_attachment<E: Write>(
    outcome: supervisor::PersistManagedServerOutcome,
    stderr: &mut E,
    run_id: &str,
) -> io::Result<ManagedAttachmentBoundary> {
    match outcome {
        supervisor::PersistManagedServerOutcome::Attached(run) => {
            Ok(ManagedAttachmentBoundary::Attached(run))
        }
        supervisor::PersistManagedServerOutcome::RequestedStop => {
            Ok(ManagedAttachmentBoundary::Terminal(ExitCode::SUCCESS))
        }
        supervisor::PersistManagedServerOutcome::RecoveryRequired => Ok(
            ManagedAttachmentBoundary::Terminal(recovery_required_exit(stderr, run_id)?),
        ),
    }
}

pub fn resolve_managed_attachment_typed(
    outcome: supervisor::PersistManagedServerOutcome,
) -> TypedManagedAttachmentBoundary {
    match outcome {
        supervisor::PersistManagedServerOutcome::Attached(run) => {
            TypedManagedAttachmentBoundary::Attached(run)
        }
        supervisor::PersistManagedServerOutcome::RequestedStop => {
            TypedManagedAttachmentBoundary::Terminal(RunTermination::RequestedStop)
        }
        supervisor::PersistManagedServerOutcome::RecoveryRequired => {
            TypedManagedAttachmentBoundary::Terminal(RunTermination::RecoveryRequired)
        }
    }
}

pub fn recovery_required_exit<E: Write>(stderr: &mut E, run_id: &str) -> io::Result<ExitCode> {
    writeln!(
        stderr,
        "cleanup could not be confirmed for managed run {run_id}; recovery required"
    )?;
    Ok(ExitCode::from(1))
}

pub fn retain_restart_after_best_effort_announcement<W: Write>(
    stdout: &mut W,
    message: &str,
    run: supervisor::ManagedRun,
) -> supervisor::ManagedRun {
    let _ = writeln!(stdout, "{message}");
    run
}

pub fn prepare_owned_replacement_run<R, D, I, RF, DF>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    interrupt: &I,
    resolve: RF,
    detect: DF,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError>
where
    I: InterruptSource,
    RF: FnOnce() -> Result<R, SupervisorError>,
    DF: FnOnce() -> Result<D, SupervisorError>,
{
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    let resolved = match resolve() {
        Ok(resolved) => resolved,
        Err(error) => return finish_owned_replacement_error(state_path, &run, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    let detected = match detect() {
        Ok(detected) => detected,
        Err(error) => return finish_owned_replacement_error(state_path, &run, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    Ok(OwnedReplacementPreparation::Prepared {
        run,
        resolved,
        detected,
    })
}

pub fn finish_owned_replacement_interrupt<R, D>(
    state_path: &Path,
    run: &supervisor::ManagedRun,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => {
            Ok(OwnedReplacementPreparation::RequestedStop)
        }
        supervisor::ChildlessFinishOutcome::Finished => {
            Ok(OwnedReplacementPreparation::Interrupted)
        }
    }
}

pub fn finish_owned_replacement_error<R, D>(
    state_path: &Path,
    run: &supervisor::ManagedRun,
    error: SupervisorError,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => {
            Ok(OwnedReplacementPreparation::RequestedStop)
        }
        supervisor::ChildlessFinishOutcome::Finished => Err(error),
    }
}

pub fn finish_childless_owner_error(
    state_path: &Path,
    run: &supervisor::ManagedRun,
    error: SupervisorError,
) -> Result<ExitCode, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => Ok(ExitCode::SUCCESS),
        supervisor::ChildlessFinishOutcome::Finished => Err(error),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedRunsSnapshot {
    Missing,
    Corrupt { message: String },
    Legacy { path: PathBuf },
    Runs(Vec<ManagedRunSummary>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedRunSummary {
    pub model_id: String,
    pub child_pid: Option<u32>,
    pub port: u16,
    pub status: &'static str,
    pub model_path: Option<PathBuf>,
}

pub fn managed_servers(paths: &NodePaths) -> Result<ManagedRunsSnapshot, SupervisorError> {
    let state = supervisor::read_runtime_state(&paths.state_path)?;
    let runs = match state {
        RuntimeStateRead::Missing => return Ok(ManagedRunsSnapshot::Missing),
        RuntimeStateRead::Corrupt(message) => return Ok(ManagedRunsSnapshot::Corrupt { message }),
        RuntimeStateRead::Legacy(path) => return Ok(ManagedRunsSnapshot::Legacy { path }),
        RuntimeStateRead::Loaded(runs) => runs,
    };
    let rows = runs
        .into_iter()
        .map(|run| {
            let inspection = supervisor::inspect_managed_run(&run);
            let model_path =
                registry::find(&run.model_id).map(|entry| paths.models_dir.join(entry.filename));
            ManagedRunSummary {
                model_id: run.model_id,
                child_pid: run.child_pid,
                port: run.port,
                status: managed_run_status(&inspection),
                model_path,
            }
        })
        .collect();
    Ok(ManagedRunsSnapshot::Runs(rows))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopRequest<'a> {
    pub target: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopOutcome {
    NoMatch,
    Completed {
        model_id: String,
    },
    RecoveryRequired {
        run_id: String,
        model_id: String,
        owner_status: supervisor::OwnerIdentityStatus,
    },
    TimedOut {
        run_id: String,
        model_id: String,
    },
}

pub fn stop_managed_servers(
    request: StopRequest<'_>,
    paths: &NodePaths,
) -> Result<StopOutcome, SupervisorError> {
    match supervisor::request_managed_stop(&paths.state_path, request.target)? {
        supervisor::StopRequestOutcome::NoMatch => Ok(StopOutcome::NoMatch),
        supervisor::StopRequestOutcome::Completed { model_id, .. } => {
            Ok(StopOutcome::Completed { model_id })
        }
        supervisor::StopRequestOutcome::RecoveryRequired {
            run_id,
            model_id,
            owner_status,
        } => Ok(StopOutcome::RecoveryRequired {
            run_id,
            model_id,
            owner_status,
        }),
        supervisor::StopRequestOutcome::TimedOut { run_id, model_id } => {
            Ok(StopOutcome::TimedOut { run_id, model_id })
        }
    }
}

pub fn observe_attached_stop_with<C, H, T>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    after_attachment: H,
    teardown: T,
) -> Result<Option<supervisor::OwnerTerminalOutcome>, SupervisorError>
where
    H: FnOnce(),
    T: FnOnce(&mut C, supervisor::OwnerTeardownDecision) -> supervisor::TeardownConfirmation,
{
    after_attachment();
    let current = supervisor::current_runtime_state_run(state_path, state_identity)?;
    if !current.stop_requested {
        return Ok(None);
    }

    Ok(Some(supervisor::finish_owner_teardown_with(
        state_path,
        &current.identity(),
        supervisor::OwnerTeardownDecision::RequestedStop,
        |decision| teardown(child, decision),
    )?))
}

pub fn observe_attached_stop<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
) -> Result<Option<supervisor::OwnerTerminalOutcome>, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let current = match supervisor::current_runtime_state_run(state_path, state_identity) {
        Ok(current) => current,
        Err(error) => {
            return match supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)?
            {
                supervisor::PostSpawnCleanupOutcome::Cleaned => Err(error),
                supervisor::PostSpawnCleanupOutcome::RequestedStop => {
                    Ok(Some(supervisor::OwnerTerminalOutcome::RequestedStop))
                }
                supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
                    Ok(Some(supervisor::OwnerTerminalOutcome::RecoveryRequired))
                }
            };
        }
    };
    if !current.stop_requested {
        return Ok(None);
    }
    supervisor::teardown_owned_run(
        child,
        state_path,
        &current.identity(),
        supervisor::OwnerTeardownDecision::RequestedStop,
    )
    .map(Some)
}

pub fn finish_spawn_initialization<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    error: Option<SupervisorError>,
) -> Result<Option<supervisor::PostSpawnCleanupOutcome>, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let Some(error) = error else {
        return Ok(None);
    };
    match supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)? {
        supervisor::PostSpawnCleanupOutcome::Cleaned => Err(error),
        outcome @ (supervisor::PostSpawnCleanupOutcome::RequestedStop
        | supervisor::PostSpawnCleanupOutcome::RecoveryRequired) => Ok(Some(outcome)),
    }
}

pub fn finish_spawned_interrupt<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    supervisor::teardown_owned_run(
        child,
        state_path,
        state_identity,
        supervisor::OwnerTeardownDecision::Interrupted,
    )
}

pub fn owner_teardown_child<C>(
    child: &mut C,
    decision: supervisor::OwnerTeardownDecision,
) -> supervisor::TeardownConfirmation
where
    C: ManagedChild + LogDrainingChild,
{
    if decision == supervisor::OwnerTeardownDecision::UnexpectedExit {
        return if child.join_log_drains().is_ok() {
            supervisor::TeardownConfirmation::Confirmed
        } else {
            supervisor::TeardownConfirmation::Unconfirmed
        };
    }
    if child.terminate().is_err() {
        return supervisor::TeardownConfirmation::Unconfirmed;
    }
    match child.try_wait() {
        Ok(Some(_)) if child.join_log_drains().is_ok() => {
            supervisor::TeardownConfirmation::Confirmed
        }
        Ok(None) => {
            let _ = child.kill();
            match child.try_wait() {
                Ok(Some(_)) if child.join_log_drains().is_ok() => {
                    supervisor::TeardownConfirmation::Confirmed
                }
                _ => supervisor::TeardownConfirmation::Unconfirmed,
            }
        }
        _ => supervisor::TeardownConfirmation::Unconfirmed,
    }
}

pub fn wait_for_startup_owned<C, I, W>(
    child: &mut C,
    state_identity: &supervisor::ManagedRunIdentity,
    state_path: &Path,
    interrupt: &I,
    mut readiness_worker: Option<&mut supervisor::ChatCompletionReadinessWorker>,
    mut wait_step: W,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptSource,
    W: FnMut(&mut C, Duration, Duration) -> Result<StartupPoll, SupervisorError>,
{
    let started = std::time::Instant::now();
    loop {
        let current = match supervisor::current_runtime_state_run(state_path, state_identity) {
            Ok(current) => current,
            Err(error) => {
                return finish_owned_startup_failure_after_readiness(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    error,
                )
            }
        };
        if current.stop_requested {
            return finish_owned_startup_transition_after_readiness(
                child,
                state_path,
                &current.identity(),
                supervisor::OwnerTeardownDecision::RequestedStop,
                &mut readiness_worker,
            );
        }
        if InterruptSource::interrupted(interrupt) {
            return finish_owned_startup_transition_after_readiness(
                child,
                state_path,
                state_identity,
                supervisor::OwnerTeardownDecision::Interrupted,
                &mut readiness_worker,
            );
        }

        match child.try_wait() {
            Ok(Some(_)) => {
                let diagnostics_error = cancel_startup_readiness(&mut readiness_worker)
                    .err()
                    .map(|error| error.to_string());
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail: String::new(),
                    diagnostics_error,
                });
            }
            Ok(None) => {}
            Err(error) => {
                return finish_owned_startup_failure_after_readiness(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    SupervisorError::Io(error),
                )
            }
        }

        let remaining = supervisor::HEALTH_TIMEOUT.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return finish_owned_startup_failure_after_readiness(
                child,
                state_path,
                state_identity,
                &mut readiness_worker,
                SupervisorError::HealthTimeout,
            );
        }
        let step_timeout = remaining.min(supervisor::HEALTH_POLL_INTERVAL);
        let poll = if let Some(worker) = readiness_worker.as_deref_mut() {
            match worker.poll() {
                supervisor::ReadinessWorkerPoll::Pending => Ok(StartupPoll::Pending),
                supervisor::ReadinessWorkerPoll::Ready => Ok(StartupPoll::Ready),
                supervisor::ReadinessWorkerPoll::Failed(error) => Err(error),
            }
        } else {
            wait_step(child, step_timeout, step_timeout)
        };
        match poll {
            Ok(StartupPoll::Pending) => {}
            Ok(StartupPoll::Ready) => {
                if let Err(error) = cancel_startup_readiness(&mut readiness_worker) {
                    return finish_owned_startup_failure(child, state_path, state_identity, error);
                }
                let current =
                    match supervisor::current_runtime_state_run(state_path, state_identity) {
                        Ok(current) => current,
                        Err(error) => {
                            return finish_owned_startup_failure(
                                child,
                                state_path,
                                state_identity,
                                error,
                            )
                        }
                    };
                if current.stop_requested {
                    return finish_owned_startup_transition_after_readiness(
                        child,
                        state_path,
                        &current.identity(),
                        supervisor::OwnerTeardownDecision::RequestedStop,
                        &mut readiness_worker,
                    );
                }
                return Ok(StartupWaitOutcome::Ready);
            }
            Err(SupervisorError::ChildExitedEarly(log_tail)) => {
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail,
                    diagnostics_error: None,
                })
            }
            Err(SupervisorError::ChildReapedDiagnosticsFailed(error)) => {
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail: String::new(),
                    diagnostics_error: Some(error),
                })
            }
            Err(error) => {
                return finish_owned_startup_failure_after_readiness(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    error,
                )
            }
        }
        if readiness_worker.is_some() {
            std::thread::sleep(step_timeout);
        }
    }
}

pub fn cancel_startup_readiness(
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
) -> Result<(), SupervisorError> {
    let result = match worker.as_deref_mut() {
        Some(worker) => worker.cancel_and_join(),
        None => Ok(()),
    };
    *worker = None;
    result
}

pub fn finish_owned_startup_failure_after_readiness<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
    error: SupervisorError,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    let error = cancel_startup_readiness(worker).err().unwrap_or(error);
    finish_owned_startup_failure(child, state_path, state_identity, error)
}

pub fn finish_owned_startup_transition_after_readiness<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    decision: supervisor::OwnerTeardownDecision,
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    let cancellation = cancel_startup_readiness(worker);
    let outcome = supervisor::teardown_owned_run(child, state_path, state_identity, decision)
        .map_err(StartupWaitFailure::AfterTeardown)?;
    if let Err(error) = cancellation {
        return Err(StartupWaitFailure::AfterTeardown(error));
    }
    map_owned_startup_transition(outcome)
}

pub fn finish_owned_startup_failure<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    error: SupervisorError,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    match supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)
        .map_err(StartupWaitFailure::AfterTeardown)?
    {
        supervisor::PostSpawnCleanupOutcome::Cleaned => {
            Err(StartupWaitFailure::AfterTeardown(error))
        }
        supervisor::PostSpawnCleanupOutcome::RequestedStop => Ok(StartupWaitOutcome::RequestedStop),
        supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
            Ok(StartupWaitOutcome::RecoveryRequired)
        }
    }
}

pub fn map_owned_startup_transition(
    outcome: supervisor::OwnerTerminalOutcome,
) -> Result<StartupWaitOutcome, StartupWaitFailure> {
    Ok(match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => StartupWaitOutcome::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => StartupWaitOutcome::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => StartupWaitOutcome::RecoveryRequired,
    })
}

pub fn wait_for_startup<C, I, W, T>(
    child: &mut C,
    state_identity: &supervisor::ManagedRunIdentity,
    state_path: &Path,
    interrupt: &I,
    wait_step: W,
    teardown: T,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptSource,
    W: FnMut(&mut C, Duration, Duration) -> Result<StartupPoll, SupervisorError>,
    T: FnMut(&mut C, supervisor::OwnerTeardownDecision) -> supervisor::TeardownConfirmation,
{
    wait_for_startup_with_finalizer(
        child,
        state_identity,
        state_path,
        interrupt,
        wait_step,
        teardown,
        |path, identity, decision, confirmation| {
            supervisor::finish_owner_teardown_with(path, identity, decision, |_| confirmation)
        },
    )
}

pub fn wait_for_startup_with_finalizer<C, I, W, T, F>(
    child: &mut C,
    state_identity: &supervisor::ManagedRunIdentity,
    state_path: &Path,
    interrupt: &I,
    mut wait_step: W,
    mut teardown: T,
    mut finalize: F,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptSource,
    W: FnMut(&mut C, Duration, Duration) -> Result<StartupPoll, SupervisorError>,
    T: FnMut(&mut C, supervisor::OwnerTeardownDecision) -> supervisor::TeardownConfirmation,
    F: FnMut(
        &Path,
        &supervisor::ManagedRunIdentity,
        supervisor::OwnerTeardownDecision,
        supervisor::TeardownConfirmation,
    ) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>,
{
    let started = std::time::Instant::now();

    loop {
        let current = supervisor::current_runtime_state_run(state_path, state_identity)
            .map_err(StartupWaitFailure::BeforeTeardown)?;
        if current.stop_requested {
            return finish_startup_owner_transition(
                child,
                state_path,
                &current.identity(),
                supervisor::OwnerTeardownDecision::RequestedStop,
                &mut teardown,
                &mut finalize,
            );
        }

        if InterruptSource::interrupted(interrupt) {
            return finish_startup_owner_transition(
                child,
                state_path,
                state_identity,
                supervisor::OwnerTeardownDecision::Interrupted,
                &mut teardown,
                &mut finalize,
            );
        }

        let remaining = supervisor::HEALTH_TIMEOUT.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(StartupWaitFailure::BeforeTeardown(
                SupervisorError::HealthTimeout,
            ));
        }

        let step_timeout = remaining.min(supervisor::HEALTH_POLL_INTERVAL);
        match wait_step(child, step_timeout, step_timeout) {
            Ok(StartupPoll::Ready) => {
                let current = supervisor::current_runtime_state_run(state_path, state_identity)
                    .map_err(StartupWaitFailure::BeforeTeardown)?;
                if current.stop_requested {
                    return finish_startup_owner_transition(
                        child,
                        state_path,
                        &current.identity(),
                        supervisor::OwnerTeardownDecision::RequestedStop,
                        &mut teardown,
                        &mut finalize,
                    );
                }
                return Ok(StartupWaitOutcome::Ready);
            }
            Ok(StartupPoll::Pending) => {}
            Err(SupervisorError::ChildExitedEarly(log_tail)) => {
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail,
                    diagnostics_error: None,
                });
            }
            Err(SupervisorError::ChildReapedDiagnosticsFailed(error)) => {
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail: String::new(),
                    diagnostics_error: Some(error),
                });
            }
            Err(error) => return Err(StartupWaitFailure::BeforeTeardown(error)),
        }
    }
}

pub fn finish_startup_owner_transition<C, T, F>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    decision: supervisor::OwnerTeardownDecision,
    teardown: &mut T,
    finalize: &mut F,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    T: FnMut(&mut C, supervisor::OwnerTeardownDecision) -> supervisor::TeardownConfirmation,
    F: FnMut(
        &Path,
        &supervisor::ManagedRunIdentity,
        supervisor::OwnerTeardownDecision,
        supervisor::TeardownConfirmation,
    ) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>,
{
    let confirmation = teardown(child, decision);
    let outcome = finalize(state_path, state_identity, decision, confirmation)
        .map_err(StartupWaitFailure::AfterTeardown)?;
    Ok(match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => StartupWaitOutcome::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => StartupWaitOutcome::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => StartupWaitOutcome::RecoveryRequired,
    })
}

pub fn supervise_running_server<C, I: InterruptSource + InterruptStatus>(
    session: RunSession<'_>,
    child: &mut C,
    interrupt: &I,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    process_label: &str,
    events: &mut dyn LifecycleEventSink,
) -> io::Result<RunOutcome>
where
    C: ManagedChild + LogDrainingChild,
{
    loop {
        if let Some(outcome) =
            observe_attached_stop(child, session.state_path, session.state_identity)
                .map_err(supervisor_error_to_io)?
        {
            if let Some(gateway) = gateway {
                gateway.withdraw();
            }
            return Ok(owner_terminal_to_run_outcome(outcome));
        }
        if InterruptSource::interrupted(interrupt) {
            if let Some(gateway) = gateway {
                gateway.withdraw();
            }
            let outcome = supervisor::teardown_owned_run(
                child,
                session.state_path,
                session.state_identity,
                supervisor::OwnerTeardownDecision::Interrupted,
            )
            .map_err(supervisor_error_to_io)?;
            return Ok(owner_terminal_to_run_outcome(outcome));
        }
        match child.try_wait() {
            Ok(None) => std::thread::sleep(Duration::from_millis(250)),
            Err(error) => {
                if let Some(gateway) = gateway {
                    gateway.withdraw();
                }
                return match supervisor::cleanup_post_spawn_failure(
                    child,
                    session.state_path,
                    session.state_identity,
                )
                .map_err(supervisor_error_to_io)?
                {
                    supervisor::PostSpawnCleanupOutcome::Cleaned => Err(error),
                    supervisor::PostSpawnCleanupOutcome::RequestedStop => {
                        Ok(RunOutcome::RequestedStop)
                    }
                    supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
                        Ok(RunOutcome::RecoveryRequired)
                    }
                };
            }
            Ok(Some(_)) => {
                if let Some(gateway) = gateway {
                    gateway.withdraw();
                }
                match supervisor::handle_observed_child_exit(
                    child,
                    session.log_path,
                    session.state_path,
                    session.state_identity,
                    interrupt,
                )
                .map_err(supervisor_error_to_io)?
                {
                    ObservedChildExit::RequestedStop => return Ok(RunOutcome::RequestedStop),
                    ObservedChildExit::Interrupted => return Ok(RunOutcome::Interrupted),
                    ObservedChildExit::Restart { run } => {
                        let _ = events.emit(LifecycleEvent::Restarting {
                            process_label: process_label.to_string(),
                            before_healthy: false,
                        });
                        return Ok(RunOutcome::Restart { run });
                    }
                    ObservedChildExit::Exhausted { log_tail } => {
                        events.emit(LifecycleEvent::EngineExited {
                            process_label: process_label.to_string(),
                            model_id: session.id.to_string(),
                            before_healthy: false,
                            log_tail: log_tail.clone(),
                        })?;
                        return Ok(RunOutcome::Exhausted { log_tail });
                    }
                    ObservedChildExit::RecoveryRequired => return Ok(RunOutcome::RecoveryRequired),
                }
            }
        }
    }
}

pub fn owner_terminal_to_run_outcome(outcome: supervisor::OwnerTerminalOutcome) -> RunOutcome {
    match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => RunOutcome::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => RunOutcome::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => RunOutcome::RecoveryRequired,
    }
}

pub fn owner_terminal_exit_code(outcome: supervisor::OwnerTerminalOutcome) -> ExitCode {
    match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => ExitCode::SUCCESS,
        supervisor::OwnerTerminalOutcome::Interrupted => ExitCode::from(130),
        supervisor::OwnerTerminalOutcome::RecoveryRequired => ExitCode::from(1),
    }
}

pub fn observed_terminal_exit_code(outcome: &ObservedChildExit) -> Option<ExitCode> {
    match outcome {
        ObservedChildExit::RequestedStop => Some(ExitCode::SUCCESS),
        ObservedChildExit::Interrupted => Some(ExitCode::from(130)),
        ObservedChildExit::Restart { .. } => None,
        ObservedChildExit::Exhausted { .. } | ObservedChildExit::RecoveryRequired => {
            Some(ExitCode::from(1))
        }
    }
}

pub fn supervise_running_server_with<C, W, E, I, T, S>(
    session: RunSession<'_>,
    child: &mut C,
    interrupt: &I,
    stdout: &mut W,
    stderr: &mut E,
    mut teardown: T,
    mut sleep: S,
) -> io::Result<RunOutcome>
where
    C: ManagedChild + LogDrainingChild,
    W: Write,
    E: Write,
    I: InterruptSource + InterruptStatus,
    T: FnMut(&mut C, supervisor::OwnerTeardownDecision) -> supervisor::TeardownConfirmation,
    S: FnMut(Duration),
{
    loop {
        if let Some(outcome) = observe_attached_stop_with(
            child,
            session.state_path,
            session.state_identity,
            || {},
            &mut teardown,
        )
        .map_err(supervisor_error_to_io)?
        {
            return Ok(match outcome {
                supervisor::OwnerTerminalOutcome::RequestedStop => RunOutcome::RequestedStop,
                supervisor::OwnerTerminalOutcome::Interrupted => RunOutcome::Interrupted,
                supervisor::OwnerTerminalOutcome::RecoveryRequired => RunOutcome::RecoveryRequired,
            });
        }

        if InterruptSource::interrupted(interrupt) {
            let outcome = supervisor::finish_owner_teardown_with(
                session.state_path,
                session.state_identity,
                supervisor::OwnerTeardownDecision::Interrupted,
                |decision| teardown(child, decision),
            )
            .map_err(supervisor_error_to_io)?;
            return Ok(match outcome {
                supervisor::OwnerTerminalOutcome::RequestedStop => RunOutcome::RequestedStop,
                supervisor::OwnerTerminalOutcome::Interrupted => RunOutcome::Interrupted,
                supervisor::OwnerTerminalOutcome::RecoveryRequired => RunOutcome::RecoveryRequired,
            });
        }

        if child.try_wait()?.is_some() {
            let log_tail = supervisor::read_log_tail(session.log_path, supervisor::LOG_TAIL_BYTES)
                .unwrap_or_else(|error| format!("crash diagnostics unavailable: {error}"));
            match supervisor::decide_observed_child_exit(
                log_tail,
                session.state_path,
                session.state_identity,
                interrupt,
                |decision| teardown(child, decision),
            )
            .map_err(supervisor_error_to_io)?
            {
                ObservedChildExit::RequestedStop => return Ok(RunOutcome::RequestedStop),
                ObservedChildExit::Interrupted => return Ok(RunOutcome::Interrupted),
                ObservedChildExit::Restart { run } => {
                    let run = retain_restart_after_best_effort_announcement(
                        stdout,
                        "llama-server exited unexpectedly; restarting once...",
                        run,
                    );
                    return Ok(RunOutcome::Restart { run });
                }
                ObservedChildExit::Exhausted { log_tail } => {
                    writeln!(
                        stderr,
                        "llama-server exited unexpectedly for {}",
                        session.id
                    )?;
                    write_log_tail(stderr, &log_tail)?;
                    return Ok(RunOutcome::Exhausted { log_tail });
                }
                ObservedChildExit::RecoveryRequired => return Ok(RunOutcome::RecoveryRequired),
            }
        }

        sleep(Duration::from_millis(250));
    }
}

pub fn print_run_ready<W: Write>(stdout: &mut W, server: &ManagedServer) -> io::Result<()> {
    writeln!(stdout, "model id: {}", server.id)?;
    writeln!(stdout, "pid: {}", server.pid)?;
    writeln!(stdout, "port: {}", server.port)?;
    writeln!(stdout, "model path: {}", server.model_path.display())?;
    writeln!(
        stdout,
        "health url: http://127.0.0.1:{}/health",
        server.port
    )?;
    stdout.flush()
}

pub fn print_run_ready_owned<C, W>(
    stdout: &mut W,
    server: &ManagedServer,
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
) -> io::Result<ReadyOutputOutcome>
where
    C: ManagedChild + LogDrainingChild,
    W: Write,
{
    let Err(output_error) = print_run_ready(stdout, server) else {
        return Ok(ReadyOutputOutcome::Ready);
    };
    match supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)
        .map_err(supervisor_error_to_io)?
    {
        supervisor::PostSpawnCleanupOutcome::Cleaned => Err(output_error),
        supervisor::PostSpawnCleanupOutcome::RequestedStop => Ok(ReadyOutputOutcome::RequestedStop),
        supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
            Ok(ReadyOutputOutcome::RecoveryRequired)
        }
    }
}

pub fn emit_run_ready_owned<C>(
    events: &mut dyn LifecycleEventSink,
    server: &ManagedServer,
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
) -> io::Result<ReadyOutputOutcome>
where
    C: ManagedChild + LogDrainingChild,
{
    let Err(output_error) = events.emit(LifecycleEvent::ModelReady {
        server: server.clone(),
    }) else {
        return Ok(ReadyOutputOutcome::Ready);
    };
    match supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)
        .map_err(supervisor_error_to_io)?
    {
        supervisor::PostSpawnCleanupOutcome::Cleaned => Err(output_error),
        supervisor::PostSpawnCleanupOutcome::RequestedStop => Ok(ReadyOutputOutcome::RequestedStop),
        supervisor::PostSpawnCleanupOutcome::RecoveryRequired => {
            Ok(ReadyOutputOutcome::RecoveryRequired)
        }
    }
}

pub fn ensure_runtime_state_is_mutable(state_path: &Path) -> io::Result<()> {
    match supervisor::read_runtime_state(state_path).map_err(supervisor_error_to_io)? {
        RuntimeStateRead::Corrupt(message) => Err(io::Error::other(format!(
            "managed sidecar state is corrupt: {message}"
        ))),
        RuntimeStateRead::Legacy(legacy_path) => Err(io::Error::other(format!(
            "legacy managed sidecar state requires manual recovery: {}",
            legacy_path.display()
        ))),
        RuntimeStateRead::Missing | RuntimeStateRead::Loaded(_) => Ok(()),
    }
}

pub fn managed_run_status(inspection: &supervisor::ManagedRunInspection) -> &'static str {
    match inspection.status {
        supervisor::ManagedRunStatus::Starting => "starting",
        supervisor::ManagedRunStatus::Running => "running",
        supervisor::ManagedRunStatus::Stopping => "stopping",
        supervisor::ManagedRunStatus::RecoveryRequired => "recovery-required",
    }
}

pub fn supervisor_error_to_io(error: SupervisorError) -> io::Error {
    io::Error::other(error.to_string())
}

pub fn write_log_tail<W: Write>(writer: &mut W, log_tail: &str) -> io::Result<()> {
    if log_tail.trim().is_empty() {
        return Ok(());
    }

    writeln!(writer, "{log_tail}")
}

pub fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(unix)]
pub struct SignalGuard {
    previous: usize,
}

#[cfg(unix)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        const SIG_ERR: usize = usize::MAX;

        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }

        clear_ctrl_c_received();
        let previous = unsafe { signal(SIGINT, handle_sigint as *const () as usize) };
        if previous == SIG_ERR {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { previous })
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(unix)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(unix)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        use std::ffi::c_int;

        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }

        const SIGINT: c_int = 2;
        let _ = unsafe { signal(SIGINT, self.previous) };
    }
}

#[cfg(windows)]
pub struct SignalGuard;

#[cfg(windows)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const TRUE: Bool = 1;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        clear_ctrl_c_received();
        let registered = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), TRUE) };
        if registered == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self)
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(windows)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const FALSE: Bool = 0;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        let _ = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), FALSE) };
    }
}

#[cfg(windows)]
unsafe extern "system" fn handle_console_ctrl(control_type: u32) -> i32 {
    const CTRL_C_EVENT: u32 = 0;
    const CTRL_BREAK_EVENT: u32 = 1;

    match control_type {
        CTRL_C_EVENT | CTRL_BREAK_EVENT => {
            set_ctrl_c_received();
            1
        }
        _ => 0,
    }
}

#[cfg(not(any(unix, windows)))]
pub struct SignalGuard;

#[cfg(not(any(unix, windows)))]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Ctrl-C cleanup is unsupported on this platform",
        ))
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(test)]
mod lifecycle_api_tests {
    use super::*;

    #[derive(Default)]
    struct RecordingLifecycleSink {
        events: Vec<LifecycleEvent>,
    }

    impl LifecycleEventSink for RecordingLifecycleSink {
        fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
            self.events.push(event);
            Ok(())
        }
    }

    #[test]
    fn lifecycle_contract_is_product_neutral() {
        let event = LifecycleEvent::NodeListening {
            port: 11_435,
            model_alias: "loxa".to_string(),
        };
        assert_eq!(
            event,
            LifecycleEvent::NodeListening {
                port: 11_435,
                model_alias: "loxa".to_string(),
            }
        );
        assert_eq!(RunTermination::Interrupted, RunTermination::Interrupted);
    }

    #[test]
    fn serve_selection_reports_product_neutral_unknown_model_evidence() {
        let temp = std::env::temp_dir().join(format!(
            "loxa-node-selection-{}-{}",
            std::process::id(),
            unix_timestamp_now()
        ));
        std::fs::create_dir_all(&temp).unwrap();

        let error = match select_serve_model(&temp, Some("missing-model")) {
            Ok(_) => panic!("missing model unexpectedly selected"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            ModelSelectionError::UnknownModel {
                id: "missing-model".to_string()
            }
        );
        assert!(!error.to_string().contains("loxa pull"));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[test]
    fn python_serve_missing_model_error_is_typed_and_product_neutral() {
        let temp = std::env::temp_dir().join(format!(
            "loxa-node-missing-model-{}-{}",
            std::process::id(),
            unix_timestamp_now()
        ));
        let paths = NodePaths {
            models_dir: temp.join("models"),
            state_path: temp.join("managed.json"),
            logs_dir: temp.join("logs"),
        };
        let mut events = RecordingLifecycleSink::default();

        let error = serve_node(
            None,
            Some(0),
            RuntimeBackendKind::PyMlxLm,
            &paths,
            &mut events,
        )
        .expect_err("Python serve needs a model request");
        let selection = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<ModelSelectionError>())
            .expect("missing model remains a typed selection error");

        assert_eq!(
            selection,
            &ModelSelectionError::MissingModelRequest {
                backend: RuntimeBackendKind::PyMlxLm,
            }
        );
        assert!(!error.to_string().contains("--model"));
        assert!(!error.to_string().contains("--engine"));
        assert!(events.events.is_empty());
    }

    #[test]
    fn stop_result_is_typed_instead_of_rendered() {
        let paths = NodePaths {
            models_dir: PathBuf::from("/unused"),
            state_path: std::env::temp_dir().join(format!(
                "loxa-node-stop-missing-{}-{}",
                std::process::id(),
                unix_timestamp_now()
            )),
            logs_dir: PathBuf::from("/unused"),
        };

        assert_eq!(
            stop_managed_servers(StopRequest { target: "all" }, &paths).unwrap(),
            StopOutcome::NoMatch
        );
    }
}
