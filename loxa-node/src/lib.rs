use loxa_core::download;
use loxa_core::engine::{py_mlx_lm, EngineLaunchSpec, ReadinessStrategy, RuntimeBackendKind};
use loxa_core::registry::{self, ModelEntry, REGISTRY};
use loxa_core::supervisor::{
    self, InterruptStatus, LogDrainingChild, ManagedChild, ManagedServer, ObservedChildExit,
    RuntimeStateRead, SupervisorError,
};
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod actor;
pub mod chat_history;
pub mod chat_routes;
pub mod control_router;
pub mod download_control;
mod engine_session;
pub mod model_lifecycle;
mod production_lifecycle;

use engine_session::EngineSession;

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

    fn log_path(&self, id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
        self.logs_dir
            .join(format!("{id}-{port}-{started_at_unix_s}.log"))
    }

    fn loxa_dir(&self) -> io::Result<&Path> {
        let state_dir = self
            .state_path
            .parent()
            .ok_or_else(|| io::Error::other("runtime state path has no parent directory"))?;
        if state_dir.file_name().is_some_and(|name| name == "run") {
            state_dir
                .parent()
                .ok_or_else(|| io::Error::other("runtime run path has no Loxa directory"))
        } else {
            Ok(state_dir)
        }
    }

    fn history_path(&self) -> io::Result<PathBuf> {
        Ok(self
            .loxa_dir()?
            .join("history")
            .join("chat-history.sqlite3"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RunOutcome {
    RequestedStop,
    Interrupted,
    Restart { run: supervisor::ManagedRun },
    Exhausted { log_tail: String },
    RecoveryRequired,
}

#[derive(Debug)]
enum TypedManagedAttachmentBoundary {
    Attached(supervisor::ManagedRun),
    Terminal(RunTermination),
}

#[derive(Debug)]
enum OwnedReplacementPreparation<R, D> {
    Prepared {
        run: supervisor::ManagedRun,
        resolved: R,
        detected: D,
    },
    RequestedStop,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupPoll {
    Pending,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupWaitOutcome {
    Ready,
    RequestedStop,
    Interrupted,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadyOutputOutcome {
    Ready,
    RequestedStop,
    RecoveryRequired,
}

#[derive(Debug)]
#[allow(dead_code)]
enum StartupWaitFailure {
    BeforeTeardown(SupervisorError),
    AfterTeardown(SupervisorError),
    AfterChildReaped {
        log_tail: String,
        diagnostics_error: Option<String>,
    },
}

struct RunSession<'a> {
    id: &'a str,
    state_identity: &'a supervisor::ManagedRunIdentity,
    log_path: &'a Path,
    state_path: &'a Path,
}

trait InterruptSource {
    fn interrupted(&self) -> bool;
}

static CTRL_C_RECEIVED: AtomicBool = AtomicBool::new(false);

fn clear_ctrl_c_received() {
    CTRL_C_RECEIVED.store(false, Ordering::SeqCst);
}

fn set_ctrl_c_received() {
    CTRL_C_RECEIVED.store(true, Ordering::SeqCst);
}

fn ctrl_c_received() -> bool {
    CTRL_C_RECEIVED.load(Ordering::SeqCst)
}

#[cfg(unix)]
extern "C" fn handle_sigint(_signal: std::ffi::c_int) {
    set_ctrl_c_received();
}

#[derive(Clone, Debug)]
struct ResolvedRuntimeBackend {
    kind: RuntimeBackendKind,
    model_id: String,
    model_path: PathBuf,
    program: PathBuf,
    engine_version: String,
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

fn resolve_runtime_backend(
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

fn py_mlx_error_to_supervisor(error: py_mlx_lm::PyMlxLmError) -> SupervisorError {
    SupervisorError::Io(io::Error::other(error))
}

fn gateway_target(
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
    StableModelReady {
        model_id: String,
        port: u16,
        model_alias: String,
    },
    StableModelFailed {
        model_id: String,
        reason: String,
        recovery_required: bool,
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
                        return Ok(RunTermination::RequestedStop);
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
                        model_id: Some(id.to_string()),
                        owner_pid,
                        owner_process_start_time_unix_s,
                        stop_requested: false,
                        lifecycle: supervisor::RunLifecycle::Starting,
                        generation: 0,
                        generation_alias: format!("loxa-{run_id}-g0"),
                        control_port: None,
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
                };
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
                return Ok(RunTermination::RequestedStop);
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
                };
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
        let observed_child_pid = server.pid;
        let observed_child_start = match server.process_start_time_unix_s {
            Some(start) => start,
            None => {
                let cleanup = supervisor::cleanup_post_spawn_failure(
                    &mut child,
                    &paths.state_path,
                    &run.identity(),
                )
                .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &run.run_id);
                }
                return Err(io::Error::other(
                    "spawned engine process identity is unavailable",
                ));
            }
        };
        let correlation_identity = run.identity();
        let correlation_run_id = run.run_id.clone();
        let mut session = match EngineSession::new(
            child,
            run,
            server,
            backend.process_label(),
            observed_child_pid,
            observed_child_start,
        ) {
            Ok(session) => session,
            Err((mut child, error)) => {
                let cleanup = supervisor::cleanup_post_spawn_failure(
                    &mut child,
                    &paths.state_path,
                    &correlation_identity,
                )
                .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &correlation_run_id);
                }
                return Err(io::Error::other(error));
            }
        };
        let state_identity = session.identity();

        if let Some(outcome) =
            observe_attached_stop(session.child_mut(), &paths.state_path, &state_identity)
                .map_err(supervisor_error_to_io)?
        {
            if outcome == supervisor::OwnerTerminalOutcome::RecoveryRequired {
                return emit_recovery_required(events, &session.run().run_id);
            }
            return Ok(owner_terminal_termination(outcome));
        }

        let readiness_worker = match &spec.readiness {
            ReadinessStrategy::LlamaModelAlias { .. } => Ok(None),
            ReadinessStrategy::ChatCompletionProbe { request_model } => {
                supervisor::spawn_chat_completion_readiness_worker(
                    session.server().port,
                    request_model.clone(),
                    supervisor::HEALTH_TIMEOUT,
                    supervisor::HEALTH_POLL_INTERVAL,
                )
                .map(Some)
            }
        };
        let engine_port = session.server().port;
        let startup = match readiness_worker {
            Ok(mut readiness_worker) => wait_for_startup_owned(
                session.child_mut(),
                &state_identity,
                &paths.state_path,
                &signal_guard,
                readiness_worker.as_mut(),
                |child, timeout, interval| match supervisor::wait_for_engine_ready_or_exit(
                    child,
                    engine_port,
                    &spec.readiness,
                    timeout,
                    interval,
                ) {
                    Ok(()) => Ok(StartupPoll::Ready),
                    Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                    Err(error) => Err(error),
                },
            ),
            Err(error) => finish_owned_startup_failure(
                session.child_mut(),
                &paths.state_path,
                &state_identity,
                error,
            ),
        };
        match startup {
            Ok(StartupWaitOutcome::Ready) => {
                let ready_server = session.server().clone();
                match emit_run_ready_owned(
                    events,
                    &ready_server,
                    session.child_mut(),
                    &paths.state_path,
                    &state_identity,
                )? {
                    ReadyOutputOutcome::Ready => {}
                    ReadyOutputOutcome::RequestedStop => return Ok(RunTermination::RequestedStop),
                    ReadyOutputOutcome::RecoveryRequired => {
                        return emit_recovery_required(events, &session.run().run_id);
                    }
                }
                if let Some(gateway) = gateway {
                    gateway.publish(gateway_target(&backend, &spec));
                }
                let session_model_id = session.server().id.clone();
                let session_log_path = session.run().log_path.clone();
                let session_process_label = session.process_label().to_owned();
                match supervise_running_server(
                    RunSession {
                        id: &session_model_id,
                        state_identity: &state_identity,
                        log_path: &session_log_path,
                        state_path: &paths.state_path,
                    },
                    session.child_mut(),
                    &signal_guard,
                    gateway,
                    &session_process_label,
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
                        return emit_recovery_required(events, &session.run().run_id);
                    }
                }
            }
            Ok(StartupWaitOutcome::RequestedStop) => return Ok(RunTermination::RequestedStop),
            Ok(StartupWaitOutcome::Interrupted) => return Ok(RunTermination::Interrupted),
            Ok(StartupWaitOutcome::RecoveryRequired) => {
                return emit_recovery_required(events, &session.run().run_id);
            }
            Err(StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            }) => {
                let _ = (log_tail, diagnostics_error);
                let session_log_path = session.run().log_path.clone();
                match supervisor::handle_observed_child_exit(
                    session.child_mut(),
                    &session_log_path,
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
                        return emit_recovery_required(events, &session.run().run_id);
                    }
                }
            }
            Err(StartupWaitFailure::AfterTeardown(error)) => {
                return finish_startup_failure(events, &log_path, backend.process_label(), error);
            }
            Err(StartupWaitFailure::BeforeTeardown(error)) => {
                let cleanup = supervisor::cleanup_post_spawn_failure(
                    session.child_mut(),
                    &paths.state_path,
                    &state_identity,
                )
                .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RequestedStop {
                    return Ok(RunTermination::RequestedStop);
                }
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &session.run().run_id);
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

fn select_serve_model(
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

fn requested_startup_model<'a>(
    models_dir: &Path,
    requested_model: Option<&'a str>,
    engine: RuntimeBackendKind,
) -> Result<Option<&'a str>, ModelSelectionError> {
    let Some(requested_model) = requested_model else {
        return Ok(None);
    };
    match engine {
        RuntimeBackendKind::LlamaCpp => {
            select_serve_model(models_dir, Some(requested_model)).map(|entry| Some(entry.id))
        }
        RuntimeBackendKind::PyMlxLm => Ok(Some(requested_model)),
    }
}

fn uses_stable_node_host(startup_model: Option<&str>, engine: RuntimeBackendKind) -> bool {
    startup_model.is_none() || engine == RuntimeBackendKind::LlamaCpp
}

pub fn serve_node(
    requested_model: Option<&str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    events: &mut dyn LifecycleEventSink,
) -> io::Result<RunTermination> {
    let startup_model = requested_startup_model(&paths.models_dir, requested_model, engine)
        .map_err(io::Error::other)?;
    let stable_llama_node = engine == RuntimeBackendKind::LlamaCpp;
    let (gateway_port, unloaded_run) = if uses_stable_node_host(startup_model, engine) {
        let reservation =
            supervisor::reserve_localhost_port(Some(port.unwrap_or(DEFAULT_GATEWAY_PORT)))
                .map_err(supervisor_error_to_io)?;
        let gateway_port = reservation.port();
        let run = claim_unloaded_owner(paths, gateway_port)?;
        drop(reservation);
        (gateway_port, Some(run))
    } else {
        (port.unwrap_or(DEFAULT_GATEWAY_PORT), None)
    };
    let node_id = format!("loxa-node-{}", std::process::id());
    let gateway_state = loxa_core::gateway::GatewayState::new(node_id);
    let mut download_runtime = unloaded_run.as_ref().map(|run| {
        if engine == RuntimeBackendKind::LlamaCpp {
            let owner = model_lifecycle::StableNodeOwner {
                run_id: run.run_id.clone(),
                pid: run.owner_pid,
                process_start_time_unix_s: run.owner_process_start_time_unix_s,
                gateway_port,
            };
            let lifecycle = model_lifecycle::ModelLifecycle::new(
                owner,
                production_lifecycle::ProductionEngineDriver::new(
                    paths.state_path.clone(),
                    paths.logs_dir.clone(),
                    gateway_port,
                ),
                production_lifecycle::ProductionGatewayPublisher(gateway_state.clone()),
            );
            download_control::DownloadControl::spawn_with_lifecycle(
                paths.models_dir.clone(),
                lifecycle,
            )
        } else {
            download_control::DownloadControl::spawn(paths.models_dir.clone())
        }
    });
    let loxa_dir = paths.loxa_dir()?;
    let token_path = loxa_dir.join("control.token");
    let token = match loxa_core::control::auth::ControlToken::load_or_create(&token_path) {
        Ok(token) => token,
        Err(error) => {
            let _ =
                cleanup_stable_node_runtime(paths, unloaded_run.as_ref(), &mut download_runtime);
            return Err(error);
        }
    };
    let history_path = paths.history_path()?;
    let (history, history_worker) = match chat_history::ChatHistory::spawn(history_path) {
        Ok(history) => history,
        Err(error) => {
            let _ =
                cleanup_stable_node_runtime(paths, unloaded_run.as_ref(), &mut download_runtime);
            return Err(io::Error::other(error));
        }
    };
    let chat_routes_state =
        chat_routes::ChatRoutesState::new(token.clone(), history, gateway_state.clone());
    let router = loxa_core::gateway::router(gateway_state.clone())
        .merge(chat_routes::router(chat_routes_state.clone()));
    let gateway_router = if let Some(run) = &unloaded_run {
        router.merge(control_router::router(control_router::ControlState::new(
            token,
            format!("loxa-node-{}", std::process::id()),
            run.run_id.clone(),
            download_runtime
                .as_ref()
                .expect("unloaded node has download control")
                .0
                .clone(),
        )))
    } else {
        router
    };
    let gateway = match loxa_core::gateway::GatewayServer::start_with_router(
        gateway_port,
        gateway_state.clone(),
        gateway_router,
    ) {
        Ok(gateway) => gateway,
        Err(error) => {
            let _ = history_worker.stop_and_join();
            let _ =
                cleanup_stable_node_runtime(paths, unloaded_run.as_ref(), &mut download_runtime);
            return Err(error);
        }
    };
    let outcome = (|| {
        if let Err(error) = events.emit(LifecycleEvent::NodeListening {
            port: gateway.port(),
            model_alias: "loxa".to_string(),
        }) {
            return match cleanup_stable_node_runtime(
                paths,
                unloaded_run.as_ref(),
                &mut download_runtime,
            ) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(cleanup),
            };
        }
        match startup_model {
            Some(model_id) if stable_llama_node => run_stable_node_actor(
                paths,
                unloaded_run.expect("stable llama node owner claimed"),
                Some(
                    download_runtime
                        .as_ref()
                        .expect("stable llama node has model control")
                        .0
                        .clone(),
                ),
                download_runtime
                    .take()
                    .expect("stable llama node has model control")
                    .1,
                Some(model_id),
                Some(events),
            ),
            Some(model_id) => run_model(
                RunRequest {
                    id: model_id,
                    ctx: None,
                    port: None,
                    engine,
                },
                paths,
                Some(&gateway_state),
                events,
            ),
            None => run_stable_node_actor(
                paths,
                unloaded_run.expect("unloaded owner claimed"),
                Some(
                    download_runtime
                        .as_ref()
                        .expect("unloaded node has download control")
                        .0
                        .clone(),
                ),
                download_runtime
                    .take()
                    .expect("unloaded node has download control")
                    .1,
                None,
                Some(events),
            ),
        }
    })();
    gateway_state.withdraw();
    chat_routes_state.shutdown_and_wait();
    let shutdown = gateway.shutdown();
    let history_shutdown = history_worker.stop_and_join().map_err(io::Error::other);
    match (outcome, shutdown, history_shutdown) {
        (Err(error), _, _) => Err(error),
        (Ok(_), Err(error), _) => Err(error),
        (Ok(_), Ok(()), Err(error)) => Err(error),
        (Ok(exit), Ok(()), Ok(())) => Ok(exit),
    }
}

fn cleanup_stable_node_runtime(
    paths: &NodePaths,
    run: Option<&supervisor::ManagedRun>,
    runtime: &mut Option<(
        download_control::DownloadControl,
        download_control::DownloadControlWorker,
    )>,
) -> io::Result<()> {
    let worker = runtime.take().map(|(_, worker)| worker.stop_and_join());
    let owner = run.map(|run| finish_unloaded_owner(paths, run));
    match (worker, owner) {
        (Some(Err(error)), _) => Err(error),
        (_, Some(Err(error))) => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
fn run_unloaded_actor(
    paths: &NodePaths,
    run: supervisor::ManagedRun,
    download_worker: download_control::DownloadControlWorker,
) -> io::Result<RunTermination> {
    run_stable_node_actor(paths, run, None, download_worker, None, None)
}

fn run_stable_node_actor(
    paths: &NodePaths,
    run: supervisor::ManagedRun,
    download_control: Option<download_control::DownloadControl>,
    download_worker: download_control::DownloadControlWorker,
    startup_model: Option<&str>,
    mut events: Option<&mut dyn LifecycleEventSink>,
) -> io::Result<RunTermination> {
    let signal_guard = match SignalGuard::install() {
        Ok(guard) => guard,
        Err(error) => {
            let _ = download_worker.stop_and_join();
            let _ = finish_unloaded_owner(paths, &run);
            return Err(error);
        }
    };
    let startup = if let Some(model_id) = startup_model {
        let download_control = download_control
            .as_ref()
            .expect("startup model requires stable model control");
        match download_control.start_startup_load(model_id) {
            Err(error) => Some(Err(io::Error::other(format!(
                "startup model admission failed: {error:?}"
            )))),
            Ok(operation_id) => loop {
                if download_worker.is_finished() {
                    break Some(Err(io::Error::other(
                        "model lifecycle actor worker terminated unexpectedly",
                    )));
                }
                let current = match current_same_owner_run(paths, &run) {
                    Ok(current) => current,
                    Err(error) => break Some(Err(error)),
                };
                if current.stop_requested || signal_guard.interrupted() {
                    break Some(Ok(if current.stop_requested {
                        RunTermination::RequestedStop
                    } else {
                        RunTermination::Interrupted
                    }));
                }
                let Some(operation) = download_control.operation(&operation_id) else {
                    break Some(Err(io::Error::other("startup model operation disappeared")));
                };
                match operation.status {
                    loxa_core::control::contracts::OperationStatus::Succeeded => {
                        if let Some(events) = events.as_deref_mut() {
                            if let Err(error) = events.emit(LifecycleEvent::StableModelReady {
                                model_id: model_id.to_owned(),
                                port: run.control_port.unwrap_or(run.port),
                                model_alias: model_lifecycle::PUBLIC_MODEL_ALIAS.to_owned(),
                            }) {
                                break Some(Err(error));
                            }
                        }
                        break None;
                    }
                    loxa_core::control::contracts::OperationStatus::Failed
                    | loxa_core::control::contracts::OperationStatus::Cancelled => {
                        let recovery_required =
                            download_control
                                .lifecycle_snapshot()
                                .is_some_and(|snapshot| {
                                    snapshot.status
                                        == model_lifecycle::NodeLifecycleStatus::RecoveryRequired
                                });
                        if let Some(events) = events.as_deref_mut() {
                            if let Err(error) = events.emit(LifecycleEvent::StableModelFailed {
                                model_id: model_id.to_owned(),
                                reason: operation
                                    .error
                                    .clone()
                                    .unwrap_or_else(|| "model startup was cancelled".into()),
                                recovery_required,
                            }) {
                                break Some(Err(error));
                            }
                        }
                        break Some(Ok(if recovery_required {
                            RunTermination::RecoveryRequired
                        } else {
                            RunTermination::Failed
                        }));
                    }
                    loxa_core::control::contracts::OperationStatus::Queued
                    | loxa_core::control::contracts::OperationStatus::Running => {}
                }
                std::thread::sleep(Duration::from_millis(25));
            },
        }
    } else {
        None
    };
    let outcome = if let Some(startup) = startup {
        startup
    } else {
        loop {
            if download_worker.is_finished() {
                break Err(io::Error::other(
                    "download actor worker terminated unexpectedly",
                ));
            }
            let current = match current_same_owner_run(paths, &run) {
                Ok(current) => current,
                Err(error) => break Err(error),
            };
            if current.stop_requested || signal_guard.interrupted() {
                let stopped = current.stop_requested;
                break Ok(if stopped {
                    RunTermination::RequestedStop
                } else {
                    RunTermination::Interrupted
                });
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let worker_cleanup = download_worker.stop_and_join();
    let cleanup = finish_unloaded_owner(paths, &run);
    match (outcome, worker_cleanup, cleanup) {
        (Err(error), _, _) => Err(error),
        (Ok(_), Err(error), _) => Err(error),
        (Ok(_), Ok(()), Err(error)) => Err(error),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

fn current_same_owner_run(
    paths: &NodePaths,
    owner: &supervisor::ManagedRun,
) -> io::Result<supervisor::ManagedRun> {
    let supervisor::RuntimeStateRead::Loaded(runs) =
        supervisor::read_runtime_state(&paths.state_path).map_err(supervisor_error_to_io)?
    else {
        return Err(io::Error::other("stable node owner state is unavailable"));
    };
    if runs.len() != 1 {
        return Err(io::Error::other("stable node owner state is not singular"));
    }
    let current = runs.into_iter().next().expect("one run exists");
    if current.run_id != owner.run_id
        || current.owner_pid != owner.owner_pid
        || current.owner_process_start_time_unix_s != owner.owner_process_start_time_unix_s
    {
        return Err(io::Error::other("stable node owner identity changed"));
    }
    Ok(current)
}

fn finish_unloaded_owner(paths: &NodePaths, run: &supervisor::ManagedRun) -> io::Result<()> {
    let current = match supervisor::read_runtime_state(&paths.state_path)
        .map_err(supervisor_error_to_io)?
    {
        supervisor::RuntimeStateRead::Loaded(runs) => runs.into_iter().next().ok_or_else(|| {
            io::Error::other("stable node owner disappeared before final cleanup")
        })?,
        _ => return Err(io::Error::other("stable node owner state is unavailable")),
    };
    if current.run_id != run.run_id
        || current.owner_pid != run.owner_pid
        || current.owner_process_start_time_unix_s != run.owner_process_start_time_unix_s
        || current.lifecycle != supervisor::RunLifecycle::Unloaded
        || current.child_pid.is_some()
    {
        return Err(io::Error::other(
            "stable node owner is not safely unloaded at final cleanup",
        ));
    }
    supervisor::finish_childless_runtime_state_run(&paths.state_path, &current.identity())
        .map(|_| ())
        .map_err(supervisor_error_to_io)
}

fn claim_unloaded_owner(
    paths: &NodePaths,
    gateway_port: u16,
) -> io::Result<supervisor::ManagedRun> {
    let owner_pid = std::process::id();
    let owner_start = supervisor::process_start_time_with_retry(owner_pid)
        .ok_or_else(|| io::Error::other("node owner process identity is unavailable"))?;
    let now = unix_timestamp_now();
    let run_id = format!("node-{owner_pid}-{owner_start}-{now}-{gateway_port}");
    supervisor::create_unloaded_node_owner(
        &paths.state_path,
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.clone(),
            model_id: None,
            owner_pid,
            owner_process_start_time_unix_s: owner_start,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            control_port: Some(gateway_port),
            port: gateway_port,
            log_path: paths.logs_dir.join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        },
    )
    .map_err(supervisor_error_to_io)
}

fn resolve_managed_attachment_typed(
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

fn prepare_owned_replacement_run<R, D, I, RF, DF>(
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

fn finish_owned_replacement_interrupt<R, D>(
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

fn finish_owned_replacement_error<R, D>(
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedRunsSnapshot {
    Missing,
    Corrupt { message: String },
    Legacy { path: PathBuf },
    Runs(Vec<ManagedRunSummary>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedRunSummary {
    pub model_id: Option<String>,
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
            let model_path = run
                .model_id
                .as_deref()
                .and_then(registry::find)
                .map(|entry| paths.models_dir.join(entry.filename));
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
        model_id: Option<String>,
    },
    RecoveryRequired {
        run_id: String,
        model_id: Option<String>,
        owner_status: supervisor::OwnerIdentityStatus,
    },
    TimedOut {
        run_id: String,
        model_id: Option<String>,
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

fn observe_attached_stop<C>(
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

fn finish_spawn_initialization<C>(
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

fn finish_spawned_interrupt<C>(
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

fn wait_for_startup_owned<C, I, W>(
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
                );
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
                );
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
                            );
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
                });
            }
            Err(SupervisorError::ChildReapedDiagnosticsFailed(error)) => {
                return Err(StartupWaitFailure::AfterChildReaped {
                    log_tail: String::new(),
                    diagnostics_error: Some(error),
                });
            }
            Err(error) => {
                return finish_owned_startup_failure_after_readiness(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    error,
                );
            }
        }
        if readiness_worker.is_some() {
            std::thread::sleep(step_timeout);
        }
    }
}

fn cancel_startup_readiness(
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
) -> Result<(), SupervisorError> {
    let result = match worker.as_deref_mut() {
        Some(worker) => worker.cancel_and_join(),
        None => Ok(()),
    };
    *worker = None;
    result
}

fn finish_owned_startup_failure_after_readiness<C>(
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

fn finish_owned_startup_transition_after_readiness<C>(
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

fn finish_owned_startup_failure<C>(
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

fn map_owned_startup_transition(
    outcome: supervisor::OwnerTerminalOutcome,
) -> Result<StartupWaitOutcome, StartupWaitFailure> {
    Ok(match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => StartupWaitOutcome::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => StartupWaitOutcome::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => StartupWaitOutcome::RecoveryRequired,
    })
}

fn supervise_running_server<C, I: InterruptSource + InterruptStatus>(
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

fn owner_terminal_to_run_outcome(outcome: supervisor::OwnerTerminalOutcome) -> RunOutcome {
    match outcome {
        supervisor::OwnerTerminalOutcome::RequestedStop => RunOutcome::RequestedStop,
        supervisor::OwnerTerminalOutcome::Interrupted => RunOutcome::Interrupted,
        supervisor::OwnerTerminalOutcome::RecoveryRequired => RunOutcome::RecoveryRequired,
    }
}

fn emit_run_ready_owned<C>(
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

fn ensure_runtime_state_is_mutable(state_path: &Path) -> io::Result<()> {
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

fn managed_run_status(inspection: &supervisor::ManagedRunInspection) -> &'static str {
    match inspection.status {
        supervisor::ManagedRunStatus::Unloaded => "unloaded",
        supervisor::ManagedRunStatus::Starting => "starting",
        supervisor::ManagedRunStatus::Running => "running",
        supervisor::ManagedRunStatus::Stopping => "stopping",
        supervisor::ManagedRunStatus::RecoveryRequired => "recovery-required",
    }
}

fn supervisor_error_to_io(error: SupervisorError) -> io::Error {
    io::Error::other(error.to_string())
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

trait SignalRegistration {
    type Installed;

    fn install(&self) -> io::Result<Self::Installed>;
    fn restore(&self, installed: &mut Self::Installed);
}

struct RegistrationGuard<R: SignalRegistration> {
    registration: R,
    installed: R::Installed,
}

impl<R: SignalRegistration> RegistrationGuard<R> {
    fn install(registration: R) -> io::Result<Self> {
        clear_ctrl_c_received();
        let installed = registration.install()?;
        Ok(Self {
            registration,
            installed,
        })
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

impl<R: SignalRegistration> InterruptSource for RegistrationGuard<R> {
    fn interrupted(&self) -> bool {
        RegistrationGuard::interrupted(self)
    }
}

impl<R: SignalRegistration> InterruptStatus for RegistrationGuard<R> {
    fn interrupted(&self) -> bool {
        RegistrationGuard::interrupted(self)
    }
}

impl<R: SignalRegistration> Drop for RegistrationGuard<R> {
    fn drop(&mut self) {
        self.registration.restore(&mut self.installed);
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct UnixSignalRegistration;

#[cfg(unix)]
impl SignalRegistration for UnixSignalRegistration {
    type Installed = usize;

    fn install(&self) -> io::Result<Self::Installed> {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        const SIG_ERR: usize = usize::MAX;
        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }
        let previous = unsafe { signal(SIGINT, handle_sigint as *const () as usize) };
        if previous == SIG_ERR {
            return Err(io::Error::last_os_error());
        }
        Ok(previous)
    }

    fn restore(&self, previous: &mut Self::Installed) {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }
        let _ = unsafe { signal(SIGINT, *previous) };
    }
}

#[cfg(unix)]
struct SignalGuard(RegistrationGuard<UnixSignalRegistration>);

#[cfg(unix)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        RegistrationGuard::install(UnixSignalRegistration).map(Self)
    }

    fn interrupted(&self) -> bool {
        self.0.interrupted()
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

#[cfg(windows)]
#[derive(Clone, Copy)]
struct WindowsSignalRegistration;

#[cfg(windows)]
impl SignalRegistration for WindowsSignalRegistration {
    type Installed = ();

    fn install(&self) -> io::Result<Self::Installed> {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const TRUE: Bool = 1;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        let registered = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), TRUE) };
        if registered == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    fn restore(&self, _installed: &mut Self::Installed) {
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
struct SignalGuard(RegistrationGuard<WindowsSignalRegistration>);

#[cfg(windows)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        RegistrationGuard::install(WindowsSignalRegistration).map(Self)
    }

    fn interrupted(&self) -> bool {
        self.0.interrupted()
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
struct SignalGuard;

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
    use loxa_core::supervisor::{LogDrainingChild, ManagedChild};
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::Mutex;

    static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());
    use std::sync::atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering};

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    static MLX_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    struct TestEnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl TestEnvRestore {
        fn set(values: &[(&'static str, &std::ffi::OsStr)]) -> Self {
            let previous = values
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(name)))
                .collect();
            for (name, value) in values {
                unsafe { std::env::set_var(name, value) };
            }
            Self(previous)
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl Drop for TestEnvRestore {
        fn drop(&mut self) {
            for (name, value) in self.0.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(name, value) },
                    None => unsafe { std::env::remove_var(name) },
                }
            }
        }
    }

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

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    struct RestartRecordingLifecycleSink {
        events: Vec<LifecycleEvent>,
        ready_ack_path: PathBuf,
        ready_generation: u64,
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl RestartRecordingLifecycleSink {
        fn new(ready_ack_path: PathBuf) -> Self {
            Self {
                events: Vec::new(),
                ready_ack_path,
                ready_generation: 0,
            }
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl LifecycleEventSink for RestartRecordingLifecycleSink {
        fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
            let model_ready = matches!(event, LifecycleEvent::ModelReady { .. });
            self.events.push(event);
            if model_ready {
                self.ready_generation += 1;
                fs::write(&self.ready_ack_path, format!("{}\n", self.ready_generation))?;
            }
            Ok(())
        }
    }

    struct ChannelLifecycleSink(std::sync::mpsc::Sender<LifecycleEvent>);

    impl LifecycleEventSink for ChannelLifecycleSink {
        fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
            self.0
                .send(event)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "test receiver dropped"))
        }
    }

    fn http_request(port: u16, request: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect gateway");
        stream.write_all(request.as_bytes()).expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    fn http_stream_prefix(port: u16, request: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect gateway");
        stream.write_all(request.as_bytes()).expect("write request");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = [0_u8; 8192];
        let read = stream.read(&mut bytes).expect("read stream prefix");
        String::from_utf8_lossy(&bytes[..read]).into_owned()
    }

    struct FailingLifecycleSink {
        events: Vec<LifecycleEvent>,
    }

    impl LifecycleEventSink for FailingLifecycleSink {
        fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
            self.events.push(event);
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected lifecycle sink failure",
            ))
        }
    }

    #[derive(Default)]
    struct FailingFirstLifecycleSink {
        listening_port: Option<u16>,
    }

    impl LifecycleEventSink for FailingFirstLifecycleSink {
        fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
            if let LifecycleEvent::NodeListening { port, .. } = event {
                self.listening_port = Some(port);
            }
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected first lifecycle event failure",
            ))
        }
    }

    struct CleanupChild {
        events: RefCell<Vec<&'static str>>,
    }

    impl ManagedChild for CleanupChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.borrow_mut().push("try_wait");
            Ok(Some(0))
        }
    }

    impl LogDrainingChild for CleanupChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("join_log_drains");
            Ok(())
        }
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "loxa-node-{label}-{}-{}",
                std::process::id(),
                unix_timestamp_now()
            ));
            std::fs::create_dir_all(&path).expect("create test directory");
            let path = std::fs::canonicalize(path).expect("canonical test directory");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                    .expect("secure test directory");
            }
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
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
    fn node_listening_sink_failure_joins_gateway_before_returning() {
        let temp = TestDir::new("listening-sink-failure");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let mut sink = FailingFirstLifecycleSink::default();

        let error = serve_node(
            None,
            Some(0),
            RuntimeBackendKind::LlamaCpp,
            &paths,
            &mut sink,
        )
        .expect_err("first event failure must escape");

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let port = sink.listening_port.expect("listening payload captured");
        TcpListener::bind(("127.0.0.1", port)).expect("gateway joined and released listener");
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new()),
            "failed listening publication releases unloaded ownership"
        );
    }

    #[test]
    fn python_gateway_metadata_uses_default_model_and_pinned_mlx_identity() {
        let backend = ResolvedRuntimeBackend {
            kind: RuntimeBackendKind::PyMlxLm,
            model_id: "/tmp/mlx model".to_string(),
            model_path: PathBuf::from("/tmp/mlx model"),
            program: PathBuf::from("/tmp/bin/mlx_lm.server"),
            engine_version: "0.31.3".to_string(),
        };
        let spec = backend.launch_spec(8123, supervisor::DEFAULT_CTX_TOKENS, "ignored-g0");

        let target = gateway_target(&backend, &spec);

        assert_eq!(target.backend_alias, "default_model");
        assert_eq!(target.engine, "mlx-lm");
        assert_eq!(target.engine_version, "0.31.3");
        assert_eq!(target.model_id, "/tmp/mlx model");
        assert_eq!(target.profile, "mlx-lm:/tmp/mlx model");
    }

    #[test]
    fn lifecycle_sink_preserves_event_order_and_payloads() {
        let mut sink = RecordingLifecycleSink::default();
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 77,
            port: 11_435,
            model_path: PathBuf::from("/models/gemma.gguf"),
            started_at_unix_s: 123,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(122),
        };
        let events = vec![
            LifecycleEvent::NodeListening {
                port: 11_435,
                model_alias: "loxa".to_string(),
            },
            LifecycleEvent::ModelReady { server },
            LifecycleEvent::Restarting {
                process_label: "llama-server".to_string(),
                before_healthy: true,
            },
            LifecycleEvent::EngineExited {
                process_label: "llama-server".to_string(),
                model_id: "gemma-3-4b-it-q4".to_string(),
                before_healthy: false,
                log_tail: "engine exited".to_string(),
            },
            LifecycleEvent::HealthTimeout {
                process_label: "llama-server".to_string(),
                log_path: PathBuf::from("/logs/run.log"),
            },
            LifecycleEvent::RecoveryRequired {
                run_id: "run-7".to_string(),
            },
        ];

        for event in events.clone() {
            sink.emit(event).expect("record lifecycle event");
        }

        assert_eq!(sink.events, events);
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn restart_sink_acknowledges_each_published_model_ready_generation() {
        let temp = TestDir::new("restart-ready-ack");
        let ready_ack_path = temp.0.join("ready-generation");
        let mut sink = RestartRecordingLifecycleSink::new(ready_ack_path.clone());
        let server = ManagedServer {
            id: "model".to_string(),
            pid: 77,
            port: 11_435,
            model_path: PathBuf::from("/models/model"),
            started_at_unix_s: 123,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(122),
        };

        sink.emit(LifecycleEvent::ModelReady {
            server: server.clone(),
        })
        .expect("acknowledge first ready generation");
        assert_eq!(fs::read_to_string(&ready_ack_path).unwrap(), "1\n");

        sink.emit(LifecycleEvent::ModelReady { server })
            .expect("acknowledge replacement ready generation");
        assert_eq!(fs::read_to_string(&ready_ack_path).unwrap(), "2\n");
    }

    #[test]
    fn model_ready_sink_failure_cleans_up_the_owned_generation_once() {
        let temp = TestDir::new("sink-cleanup");
        let state_path = temp.0.join("managed.json");
        let run = supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            model_id: Some("gemma-3-4b-it-q4".to_string()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Starting,
            generation: 0,
            generation_alias: "loxa-run-1-g0".to_string(),
            control_port: None,
            port: 8081,
            log_path: temp.0.join("run-1.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        supervisor::create_starting_run(&state_path, run.clone()).expect("persist starting run");
        let server = ManagedServer {
            id: run.model_id.clone().expect("model id"),
            pid: 777,
            port: run.port,
            model_path: temp.0.join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let mut attached = run.clone();
        attached.lifecycle = supervisor::RunLifecycle::Running;
        attached.child_pid = Some(server.pid);
        attached.child_process_start_time_unix_s = server.process_start_time_unix_s;
        assert!(supervisor::update_runtime_state_run(
            &state_path,
            &run.identity(),
            attached.clone()
        )
        .expect("attach child"));
        let mut sink = FailingLifecycleSink { events: Vec::new() };
        let mut child = CleanupChild {
            events: RefCell::new(Vec::new()),
        };

        let outcome = emit_run_ready_owned(
            &mut sink,
            &server,
            &mut child,
            &state_path,
            &attached.identity(),
        );

        assert_eq!(
            outcome.expect("cleanup outcome"),
            ReadyOutputOutcome::RecoveryRequired
        );
        assert_eq!(
            sink.events,
            vec![LifecycleEvent::ModelReady {
                server: server.clone()
            }]
        );
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert!(matches!(
            supervisor::read_runtime_state(&state_path).expect("read retained state"),
            RuntimeStateRead::Loaded(runs) if runs == vec![attached]
        ));
    }

    #[test]
    fn ctrl_c_flag_helpers_round_trip() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
        set_ctrl_c_received();
        assert!(ctrl_c_received());
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
    }

    #[cfg(unix)]
    #[test]
    fn unix_signal_handler_records_interrupt_without_platform_state_leaking() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();
        handle_sigint(2);
        assert!(ctrl_c_received());
        clear_ctrl_c_received();
    }

    #[derive(Clone, Default)]
    struct FakeSignalRegistration {
        calls: Arc<Mutex<Vec<bool>>>,
    }

    impl SignalRegistration for FakeSignalRegistration {
        type Installed = ();

        fn install(&self) -> io::Result<Self::Installed> {
            self.calls.lock().unwrap().push(true);
            Ok(())
        }

        fn restore(&self, _installed: &mut Self::Installed) {
            self.calls.lock().unwrap().push(false);
        }
    }

    #[test]
    fn portable_signal_registration_guard_installs_and_restores_exactly_once() {
        let registration = FakeSignalRegistration::default();
        let calls = registration.calls.clone();
        let guard = RegistrationGuard::install(registration).expect("install fake registration");
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        drop(guard);
        assert_eq!(*calls.lock().unwrap(), vec![true, false]);
    }

    #[cfg(unix)]
    #[test]
    fn signal_guard_can_install_drop_and_restore_repeatedly() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();

        let first = SignalGuard::install().expect("install first signal guard");
        assert!(!first.interrupted());
        drop(first);

        let second = SignalGuard::install().expect("restore then install second signal guard");
        assert!(!second.interrupted());
        drop(second);
        clear_ctrl_c_received();
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
    fn clean_install_startup_is_unloaded_for_every_engine() {
        let temp = std::env::temp_dir().join(format!(
            "loxa-node-missing-model-{}-{}",
            std::process::id(),
            unix_timestamp_now()
        ));
        assert_eq!(
            requested_startup_model(&temp, None, RuntimeBackendKind::LlamaCpp),
            Ok(None)
        );
        assert_eq!(
            requested_startup_model(&temp, None, RuntimeBackendKind::PyMlxLm),
            Ok(None)
        );
    }

    #[test]
    fn node_paths_place_history_in_a_dedicated_private_subdirectory() {
        let temp = TestDir::new("history-path");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("run").join("managed.json"),
            logs_dir: temp.0.join("run").join("logs"),
        };

        assert_eq!(
            paths.history_path().unwrap(),
            temp.0.join("history").join("chat-history.sqlite3")
        );
    }

    #[cfg(unix)]
    #[test]
    fn history_startup_uses_private_child_without_repairing_or_migrating_loxa_root() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TestDir::new("history-private-child");
        std::fs::set_permissions(&temp.0, std::fs::Permissions::from_mode(0o755)).unwrap();
        let unsafe_old = temp.0.join("chat-history.sqlite3");
        std::fs::write(&unsafe_old, b"unshipped sentinel").unwrap();
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("run").join("managed.json"),
            logs_dir: temp.0.join("run").join("logs"),
        };
        let history_path = paths.history_path().unwrap();

        let (_history, worker) = chat_history::ChatHistory::spawn(history_path.clone()).unwrap();
        worker.stop_and_join().unwrap();

        assert!(history_path.is_file());
        assert_eq!(std::fs::read(&unsafe_old).unwrap(), b"unshipped sentinel");
        assert_eq!(
            std::fs::metadata(&temp.0).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert_eq!(
            std::fs::metadata(history_path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[cfg(unix)]
    #[test]
    fn history_startup_fails_closed_when_dedicated_destination_is_unsafe() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TestDir::new("history-unsafe-destination");
        let history_dir = temp.0.join("history");
        std::fs::create_dir(&history_dir).unwrap();
        std::fs::set_permissions(&history_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("run").join("managed.json"),
            logs_dir: temp.0.join("run").join("logs"),
        };

        assert!(chat_history::ChatHistory::spawn(paths.history_path().unwrap()).is_err());
        assert_eq!(
            std::fs::metadata(&history_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[test]
    fn known_llama_startup_and_all_unloaded_modes_use_the_stable_node_host() {
        assert!(uses_stable_node_host(
            Some("gemma-3-4b-it-q4"),
            RuntimeBackendKind::LlamaCpp
        ));
        assert!(uses_stable_node_host(None, RuntimeBackendKind::LlamaCpp));
        assert!(uses_stable_node_host(None, RuntimeBackendKind::PyMlxLm));
        assert!(!uses_stable_node_host(
            Some("external-mlx-model"),
            RuntimeBackendKind::PyMlxLm
        ));
    }

    #[test]
    fn unloaded_node_claim_is_visible_and_prevents_a_second_owner() {
        let temp = TestDir::new("unloaded-owner");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let run = claim_unloaded_owner(&paths, 11_435).expect("claim unloaded node");

        let ManagedRunsSnapshot::Runs(rows) = managed_servers(&paths).expect("inspect owner")
        else {
            panic!("unloaded owner must be visible");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].model_id, None);
        assert_eq!(rows[0].status, "unloaded");
        assert_eq!(rows[0].port, 11_435);
        assert!(claim_unloaded_owner(&paths, 12_435).is_err());

        assert_eq!(
            supervisor::finish_childless_runtime_state_run(&paths.state_path, &run.identity())
                .unwrap(),
            supervisor::ChildlessFinishOutcome::Finished
        );
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
    }

    #[test]
    fn unloaded_node_claim_recovers_a_dead_childless_model_free_owner() {
        let temp = TestDir::new("dead-unloaded-owner");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let stale = supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "dead-node-owner".to_string(),
            model_id: None,
            owner_pid: u32::MAX,
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Unloaded,
            generation: 4,
            generation_alias: "loxa-dead-node-owner-g4".to_string(),
            control_port: Some(11_440),
            port: 11_440,
            log_path: paths.logs_dir.join("dead-node-owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        supervisor::create_starting_run(&paths.state_path, stale)
            .expect("seed dead unloaded owner");

        let replacement = claim_unloaded_owner(&paths, 11_441)
            .expect("recover dead unloaded owner through production probe");

        assert_eq!(replacement.model_id, None);
        assert_eq!(replacement.lifecycle, supervisor::RunLifecycle::Unloaded);
        assert_eq!(replacement.control_port, Some(11_441));
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&paths.state_path).expect("read replacement")
        else {
            panic!("replacement state must be loaded");
        };
        assert_eq!(runs, vec![replacement.clone()]);
        assert_eq!(
            supervisor::finish_childless_runtime_state_run(
                &paths.state_path,
                &replacement.identity(),
            )
            .expect("clean replacement"),
            supervisor::ChildlessFinishOutcome::Finished
        );
    }

    #[test]
    fn final_owner_cleanup_resolves_the_current_unloaded_generation() {
        let temp = TestDir::new("advanced-unloaded-owner");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let original = claim_unloaded_owner(&paths, 11_438).expect("claim unloaded owner");
        let mut advanced = original.clone();
        advanced.generation = 3;
        advanced.generation_alias = format!("loxa-{}-g3", original.run_id);
        supervisor::update_runtime_state_run_committed(
            &paths.state_path,
            &original.identity(),
            advanced,
        )
        .unwrap()
        .expect("advance owner generation");

        finish_unloaded_owner(&paths, &original).expect("finish current owner generation");
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
    }

    #[test]
    fn unloaded_actor_observes_durable_stop_and_cleans_up_without_deadlock() {
        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("unloaded-stop");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let actor_paths = paths.clone();
        let run = claim_unloaded_owner(&paths, 11_436).expect("claim unloaded owner");
        let (_, download_worker) =
            download_control::DownloadControl::spawn(paths.models_dir.clone());
        let actor =
            std::thread::spawn(move || run_unloaded_actor(&actor_paths, run, download_worker));

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while matches!(managed_servers(&paths), Ok(ManagedRunsSnapshot::Missing)) {
            assert!(
                std::time::Instant::now() < deadline,
                "owner was not published"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        assert!(matches!(
            stop_managed_servers(StopRequest { target: "all" }, &paths).unwrap(),
            StopOutcome::Completed { model_id } if model_id.is_none()
        ));
        assert_eq!(
            actor.join().expect("join unloaded actor").unwrap(),
            RunTermination::RequestedStop
        );
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
    }

    #[test]
    fn unloaded_actor_monitor_accepts_same_owner_generation_advances() {
        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("unloaded-generation-advance");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let actor_paths = paths.clone();
        let run = claim_unloaded_owner(&paths, 11_439).expect("claim unloaded owner");
        let (_, worker) = download_control::DownloadControl::spawn(paths.models_dir.clone());
        let original = run.clone();
        let actor = std::thread::spawn(move || run_unloaded_actor(&actor_paths, run, worker));

        let mut advanced = original.clone();
        advanced.generation = 2;
        advanced.generation_alias = format!("loxa-{}-g2", original.run_id);
        supervisor::update_runtime_state_run_committed(
            &paths.state_path,
            &original.identity(),
            advanced,
        )
        .unwrap()
        .expect("advance stable owner generation");
        std::thread::sleep(Duration::from_millis(75));
        assert!(
            !actor.is_finished(),
            "same-owner generation must not stop node"
        );

        assert!(matches!(
            stop_managed_servers(StopRequest { target: "all" }, &paths).unwrap(),
            StopOutcome::Completed { .. }
        ));
        assert_eq!(
            actor.join().unwrap().unwrap(),
            RunTermination::RequestedStop
        );
    }

    #[test]
    fn unloaded_actor_worker_panic_is_typed_and_cleans_exact_owner() {
        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("unloaded-worker-panic");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let run = claim_unloaded_owner(&paths, 11_437).unwrap();
        let error = run_unloaded_actor(&paths, run, download_control::panicking_worker())
            .expect_err("worker panic terminates node");
        assert!(error.to_string().contains("worker"));
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
    }

    #[test]
    fn production_serve_starts_unloaded_reports_unavailable_and_stops_cleanly() {
        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("serve-unloaded-integration");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let serve_paths = paths.clone();
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            serve_node(
                None,
                Some(0),
                RuntimeBackendKind::LlamaCpp,
                &serve_paths,
                &mut ChannelLifecycleSink(event_tx),
            )
        });
        let LifecycleEvent::NodeListening { port, .. } = event_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("node listening event")
        else {
            panic!("first event must publish listening node");
        };
        let status = http_request(
            port,
            "GET /loxa/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        );
        assert!(status.starts_with("HTTP/1.1 200"), "{status}");
        assert!(status.contains("\"health\":\"unavailable\""), "{status}");
        let nonce = "01".repeat(32);
        let proof = http_request(
            port,
            &format!(
                "GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: {nonce}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(proof.starts_with("HTTP/1.1 200"), "{proof}");
        assert!(proof.contains("\"protocol_version\":1"), "{proof}");
        assert!(proof.contains("\"status\":\"unloaded\""), "{proof}");
        assert!(!proof.contains("active_model_id"), "{proof}");
        for bad_proof in [
            "GET /loxa/v1/node?nonce=00 HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: 0000000000000000000000000000000000000000000000000000000000000000\r\nConnection: close\r\n\r\n".to_string(),
            "GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: bad\r\nConnection: close\r\n\r\n".to_string(),
            format!("GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: https://evil.invalid\r\nX-Loxa-Challenge: {nonce}\r\nConnection: close\r\n\r\n"),
        ] {
            let response = http_request(port, &bad_proof);
            assert!(response.starts_with("HTTP/1.1 400") || response.starts_with("HTTP/1.1 403"), "{response}");
        }
        let preflight = http_request(
            port,
            "OPTIONS /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: tauri://localhost\r\nAccess-Control-Request-Method: GET\r\nAccess-Control-Request-Headers: authorization\r\nConnection: close\r\n\r\n",
        );
        assert!(preflight.starts_with("HTTP/1.1 204"), "{preflight}");
        assert!(
            preflight.contains("access-control-allow-origin: tauri://localhost"),
            "{preflight}"
        );
        assert!(preflight.contains("vary: Origin"), "{preflight}");
        let dev_preflight = http_request(port, "OPTIONS /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: http://127.0.0.1:1420\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n");
        assert!(dev_preflight.starts_with("HTTP/1.1 204"), "{dev_preflight}");
        assert!(dev_preflight.contains("access-control-allow-origin: http://127.0.0.1:1420"));
        let evil_preflight = http_request(
            port,
            "OPTIONS /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: https://evil.invalid\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n",
        );
        assert!(
            evil_preflight.starts_with("HTTP/1.1 403"),
            "{evil_preflight}"
        );
        assert!(!evil_preflight.contains("access-control-allow-origin"));
        let unauthenticated_models = http_request(
            port,
            "GET /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        );
        assert!(
            unauthenticated_models.starts_with("HTTP/1.1 401"),
            "{unauthenticated_models}"
        );
        let wrong_bearer = http_request(
            port,
            "GET /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: tauri://localhost\r\nAuthorization: Bearer 0000000000000000000000000000000000000000000000000000000000000000\r\nConnection: close\r\n\r\n",
        );
        assert!(wrong_bearer.starts_with("HTTP/1.1 401"), "{wrong_bearer}");
        assert!(wrong_bearer.contains("access-control-allow-origin: tauri://localhost"));
        let token = loxa_core::control::auth::ControlToken::load(&temp.0.join("control.token"))
            .expect("load control token");
        let bearer = token.expose_for_authorization();
        let authenticated_node = http_request(port, &format!("GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bearer}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n"));
        assert!(
            authenticated_node.starts_with("HTTP/1.1 200"),
            "{authenticated_node}"
        );
        assert!(authenticated_node.contains("\"active_model_id\":null"));
        assert!(!authenticated_node.contains("challenge_proof"));
        let capabilities = http_request(port, &format!("GET /loxa/v1/capabilities HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bearer}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n"));
        assert!(capabilities.starts_with("HTTP/1.1 200"), "{capabilities}");
        assert!(capabilities.contains("\"document_input\":false"));
        let authenticated_models = http_request(
            port,
            &format!(
                "GET /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n",
                bearer
            ),
        );
        assert!(
            authenticated_models.starts_with("HTTP/1.1 200"),
            "{authenticated_models}"
        );
        assert!(authenticated_models.contains("gemma-3-4b-it-q4"));
        let download_preflight = http_request(port, "OPTIONS /loxa/v1/models/download HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: tauri://localhost\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: authorization, content-type\r\nConnection: close\r\n\r\n");
        assert!(
            download_preflight.starts_with("HTTP/1.1 204"),
            "{download_preflight}"
        );
        assert!(download_preflight.contains("access-control-allow-methods: POST, OPTIONS"));
        let wrong_download_preflight = http_request(port, "OPTIONS /loxa/v1/models/download HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: tauri://localhost\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n");
        assert!(
            wrong_download_preflight.starts_with("HTTP/1.1 403"),
            "{wrong_download_preflight}"
        );
        let unauthenticated_download = http_request(port, "POST /loxa/v1/models/download HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"model_id\":\"gemma-3-4b-it-q4\"}");
        assert!(
            unauthenticated_download.starts_with("HTTP/1.1 401"),
            "{unauthenticated_download}"
        );
        let unknown_download = http_request(port, &format!("POST /loxa/v1/models/download HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bearer}\r\nOrigin: tauri://localhost\r\nContent-Type: application/json\r\nContent-Length: 22\r\nConnection: close\r\n\r\n{{\"model_id\":\"unknown\"}}"));
        assert!(
            unknown_download.starts_with("HTTP/1.1 400"),
            "{unknown_download}"
        );
        let events = http_stream_prefix(port, &format!("GET /loxa/v1/events?cursor=0 HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bearer}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n"));
        assert!(events.starts_with("HTTP/1.1 200"), "{events}");
        assert!(
            events.contains("content-type: text/event-stream"),
            "{events}"
        );
        assert!(events.contains("event: snapshot"), "{events}");
        let chat = http_request(
            port,
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 30\r\n\r\n{\"model\":\"loxa\",\"messages\":[]}",
        );
        assert!(chat.starts_with("HTTP/1.1 503"), "{chat}");
        assert!(claim_unloaded_owner(&paths, port.saturating_add(1)).is_err());
        assert!(matches!(
            stop_managed_servers(StopRequest { target: "all" }, &paths).unwrap(),
            StopOutcome::Completed { model_id: None }
        ));
        assert_eq!(
            server.join().expect("join node").unwrap(),
            RunTermination::RequestedStop
        );
        TcpListener::bind(("127.0.0.1", port)).expect("gateway released port");
    }

    #[test]
    fn loaded_llama_startup_mounts_stable_authenticated_control_before_model_work() {
        struct BlockingListeningSink {
            listening: std::sync::mpsc::Sender<u16>,
            release: std::sync::mpsc::Receiver<()>,
        }

        impl LifecycleEventSink for BlockingListeningSink {
            fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
                if let LifecycleEvent::NodeListening { port, .. } = event {
                    self.listening.send(port).unwrap();
                    self.release.recv().unwrap();
                }
                Ok(())
            }
        }

        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("serve-loaded-control-integration");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        std::fs::create_dir_all(&paths.models_dir).unwrap();
        let recipe = &REGISTRY[0];
        std::fs::write(paths.models_dir.join(recipe.filename), b"invalid fixture").unwrap();
        let serve_paths = paths.clone();
        let (listening_tx, listening_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            serve_node(
                Some(recipe.id),
                Some(0),
                RuntimeBackendKind::LlamaCpp,
                &serve_paths,
                &mut BlockingListeningSink {
                    listening: listening_tx,
                    release: release_rx,
                },
            )
        });
        let port = listening_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("loaded node listening event");

        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&paths.state_path).unwrap()
        else {
            panic!("stable owner state must be visible before startup load")
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].model_id, None);
        assert_eq!(runs[0].control_port, Some(port));
        assert!(claim_unloaded_owner(&paths, port.saturating_add(1)).is_err());
        let token = loxa_core::control::auth::ControlToken::load(&temp.0.join("control.token"))
            .expect("loaded startup control token");
        let models = http_request(
            port,
            &format!(
                "GET /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n",
                token.expose_for_authorization()
            ),
        );
        assert!(models.starts_with("HTTP/1.1 200"), "{models}");
        assert!(models.contains(recipe.id), "{models}");

        release_tx.send(()).unwrap();
        assert_eq!(
            server.join().unwrap().unwrap(),
            RunTermination::Failed,
            "invalid startup artifact must fail truthfully"
        );
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        TcpListener::bind(("127.0.0.1", port)).expect("gateway released after startup failure");
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
    fn read_test_http_request(stream: &mut std::net::TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set request timeout");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                break;
            }
        }
        String::from_utf8(request).expect("request is utf8")
    }

    fn respond_test_http(stream: &mut std::net::TcpStream, body: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("write response");
        stream.flush().expect("flush response");
    }

    fn persist_run_for_server(state_path: &Path, server: &ManagedServer) -> supervisor::ManagedRun {
        let run_id = format!("test-run-{}", server.pid);
        let mut run = starting_run_for_test(state_path, &run_id);
        run.model_id = Some(server.id.clone());
        run.port = server.port;
        run.generation_alias = format!("loxa-{run_id}-g0");
        run.log_path = state_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{run_id}.log"));
        supervisor::create_starting_run(state_path, run.clone()).expect("create starting run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(server.pid);
        run.child_process_start_time_unix_s = server.process_start_time_unix_s;
        assert!(
            supervisor::update_runtime_state_run(state_path, &starting_identity, run.clone())
                .expect("attach test child")
        );
        run
    }

    fn starting_run_for_test(state_path: &Path, run_id: &str) -> supervisor::ManagedRun {
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: Some("gemma-3-4b-it-q4".to_string()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            control_port: None,
            port: 8080,
            log_path: state_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn request_stop_for_test(
        state_path: &Path,
        identity: &supervisor::ManagedRunIdentity,
    ) -> supervisor::ManagedRun {
        let mut run = supervisor::current_runtime_state_run(state_path, identity)
            .expect("read current run before test stop");
        run.stop_requested = true;
        supervisor::update_runtime_state_run_committed(state_path, identity, run)
            .expect("commit test stop")
            .expect("exact test stop")
    }

    #[test]
    fn childless_spawn_error_cleanup_conflict_preserves_a_newer_generation() {
        let temp = TempDir::new("loxa-pre-spawn-cleanup-conflict");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let starting_identity = run.identity();
        let mut newer_generation = run.clone();
        newer_generation.generation = 1;
        newer_generation.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish starting run");
        assert!(supervisor::update_runtime_state_run(
            &state_path,
            &starting_identity,
            newer_generation.clone(),
        )
        .expect("advance generation before stale cleanup"));

        let error = finish_owned_replacement_error::<(), ()>(
            &state_path,
            &run,
            SupervisorError::NoFreePort,
        )
        .expect_err("cleanup conflict must replace the spawn error");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved newer state"),
            RuntimeStateRead::Loaded(vec![newer_generation])
        );
    }

    #[test]
    fn published_replacement_resolution_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-resolution-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Err::<(), _>(SupervisorError::NoFreePort),
            || -> Result<(), SupervisorError> {
                panic!("detection must not run after resolution failure")
            },
        )
        .expect_err("resolution failure");

        assert!(matches!(error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_detection_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-detection-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Err::<(), _>(SupervisorError::LlamaServerNotFound),
        )
        .expect_err("detection failure");

        assert!(matches!(error, SupervisorError::LlamaServerNotFound));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_resolution_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-resolution");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || -> Result<(), SupervisorError> { panic!("detection must not run after interrupt") },
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_detection_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-detection");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Ok("detected"),
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_concurrent_stop_and_interrupt_prefers_requested_stop() {
        let temp = TempDir::new("loxa-replacement-stop-interrupt");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let identity = run.identity();
        let spawn_count = Cell::new(0_u8);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || {
                request_stop_for_test(&state_path, &identity);
                Ok("resolved")
            },
            || Ok("detected"),
        )
        .expect("requested stop outcome");
        if matches!(outcome, OwnedReplacementPreparation::Prepared { .. }) {
            spawn_count.set(spawn_count.get() + 1);
        }

        assert!(matches!(
            outcome,
            OwnedReplacementPreparation::RequestedStop
        ));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn replacement_reservation_failure_exact_finishes_childless_state_and_stop_still_wins() {
        let ordinary_temp = TempDir::new("loxa-replacement-reservation-failure");
        let ordinary_state_path = ordinary_temp.path().join("managed.json");
        let ordinary_blocker =
            TcpListener::bind(("127.0.0.1", 0)).expect("block ordinary replacement port");
        let ordinary_port = ordinary_blocker
            .local_addr()
            .expect("ordinary blocker address")
            .port();
        let mut ordinary_run = starting_run_for_test(&ordinary_state_path, "run-ordinary");
        ordinary_run.generation = 1;
        ordinary_run.generation_alias = "loxa-run-ordinary-g1".to_string();
        ordinary_run.port = ordinary_port;
        supervisor::create_starting_run(&ordinary_state_path, ordinary_run.clone())
            .expect("publish ordinary replacement");
        let reservation_error = match supervisor::reserve_localhost_port(Some(ordinary_port)) {
            Err(error) => error,
            Ok(_) => panic!("blocked port must reject a replacement reservation"),
        };
        let ordinary_error = finish_owned_replacement_error::<(), ()>(
            &ordinary_state_path,
            &ordinary_run,
            reservation_error,
        )
        .expect_err("blocked replacement reservation must fail");

        assert!(matches!(ordinary_error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&ordinary_state_path)
                .expect("read ordinary terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        let stopped_temp = TempDir::new("loxa-stopped-replacement-reservation-failure");
        let stopped_state_path = stopped_temp.path().join("managed.json");
        let stopped_blocker =
            TcpListener::bind(("127.0.0.1", 0)).expect("block stopped replacement port");
        let stopped_port = stopped_blocker
            .local_addr()
            .expect("stopped blocker address")
            .port();
        let mut stopped_run = starting_run_for_test(&stopped_state_path, "run-stopped");
        stopped_run.generation = 1;
        stopped_run.generation_alias = "loxa-run-stopped-g1".to_string();
        stopped_run.port = stopped_port;
        supervisor::create_starting_run(&stopped_state_path, stopped_run.clone())
            .expect("publish stopped replacement");
        let stopped_identity = stopped_run.identity();
        request_stop_for_test(&stopped_state_path, &stopped_identity);
        let reservation_error = match supervisor::reserve_localhost_port(Some(stopped_port)) {
            Err(error) => error,
            Ok(_) => panic!("blocked port must reject a stopped replacement reservation"),
        };
        let stopped_outcome = finish_owned_replacement_error::<(), ()>(
            &stopped_state_path,
            &stopped_run,
            reservation_error,
        )
        .expect("durable stop must win over replacement reservation failure");

        assert!(matches!(
            stopped_outcome,
            OwnedReplacementPreparation::RequestedStop
        ));
        assert_eq!(
            supervisor::read_runtime_state(&stopped_state_path)
                .expect("read stopped terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn attachment_boundary_stop_during_and_immediately_after_tears_down_once_without_generation_two(
    ) {
        for stop_during_attachment in [true, false] {
            let temp = TempDir::new("loxa-attachment-stop-boundary");
            let state_path = temp.path().join("managed.json");
            let mut starting = starting_run_for_test(&state_path, "run-1");
            starting.generation = 1;
            starting.generation_alias = "loxa-run-1-g1".to_string();
            supervisor::create_starting_run(&state_path, starting.clone())
                .expect("publish generation one");
            if stop_during_attachment {
                starting = request_stop_for_test(&state_path, &starting.identity());
            }
            let server = ManagedServer {
                id: starting.model_id.clone().expect("model id"),
                pid: 778,
                port: starting.port,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(222),
            };
            let mut child =
                FakeStartupChild::with_wait_results(vec![Some(0)]).with_owned_process_group();
            let attached = supervisor::persist_managed_server_or_cleanup(
                &mut child,
                &state_path,
                starting,
                server,
                Duration::from_millis(10),
            )
            .expect("attach replacement");
            let supervisor::PersistManagedServerOutcome::Attached(attached) = attached else {
                panic!("replacement attachment must remain owned");
            };
            let identity = attached.identity();
            if !stop_during_attachment {
                request_stop_for_test(&state_path, &identity);
            }

            let outcome = observe_attached_stop(&mut child, &state_path, &identity)
                .expect("attachment stop outcome");

            assert_eq!(
                outcome,
                Some(supervisor::OwnerTerminalOutcome::RequestedStop)
            );
            assert_eq!(
                child.events.into_inner(),
                vec!["try_wait", "join_log_drains"]
            );
            assert_eq!(
                supervisor::read_runtime_state(&state_path).expect("read terminal state"),
                RuntimeStateRead::Loaded(Vec::new())
            );
        }
    }

    #[test]
    fn post_spawn_interrupt_before_attachment_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-pre-attach-interrupt");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        supervisor::create_starting_run(&state_path, run.clone()).expect("create starting run");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = finish_spawned_interrupt(&mut child, &state_path, &run.identity())
            .expect("interrupt cleanup outcome");

        assert_eq!(outcome, supervisor::OwnerTerminalOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved starting run"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn post_spawn_initialization_failure_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-initialization-failure");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        supervisor::create_starting_run(&state_path, run.clone()).expect("create starting run");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = finish_spawn_initialization(
            &mut child,
            &state_path,
            &run.identity(),
            Some(SupervisorError::Io(io::Error::other(
                "injected drain initialization failure",
            ))),
        )
        .expect("initialization cleanup outcome");

        assert_eq!(
            outcome,
            Some(supervisor::PostSpawnCleanupOutcome::RecoveryRequired)
        );
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved starting run"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn post_spawn_immediate_attachment_reread_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-attachment-reread");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt state after attachment");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = observe_attached_stop(&mut child, &state_path, &run.identity())
            .expect("reread recovery outcome");

        assert_eq!(
            outcome,
            Some(supervisor::OwnerTerminalOutcome::RecoveryRequired)
        );
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_startup_state_read_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-startup-state-read");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt startup state");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            None,
            |_, _, _| panic!("state read error must precede readiness polling"),
        )
        .expect("startup recovery outcome");

        assert_eq!(outcome, StartupWaitOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_startup_readiness_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-startup-readiness");
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
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![None, Some(0)]);

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            None,
            |_, _, _| Err(SupervisorError::NoFreePort),
        )
        .expect("startup recovery outcome");

        assert_eq!(outcome, StartupWaitOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn python_startup_accepts_completion_slower_than_llama_poll_interval() {
        let temp = TempDir::new("loxa-python-slow-readiness");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fake MLX server");
        let port = listener.local_addr().expect("fake MLX address").port();
        let exited = Arc::new(TestAtomicBool::new(false));
        let server_exit = Arc::clone(&exited);
        let server_thread = std::thread::spawn(move || {
            let (mut health, _) = listener.accept().expect("accept health request");
            read_test_http_request(&mut health);
            respond_test_http(&mut health, r#"{"status":"ok"}"#);

            let (mut completion, _) = listener.accept().expect("accept completion request");
            read_test_http_request(&mut completion);
            std::thread::sleep(Duration::from_millis(400));
            respond_test_http(
                &mut completion,
                r#"{"choices":[{"message":{"content":"ok"}}]}"#,
            );
            std::thread::sleep(Duration::from_millis(500));
            server_exit.store(true, TestOrdering::SeqCst);
        });
        let server = ManagedServer {
            id: "/tmp/mlx-model".to_string(),
            pid: 777,
            port,
            model_path: PathBuf::from("/tmp/mlx-model"),
            started_at_unix_s: 789,
            llama_server_version: "0.31.3".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_shared_exit(exited);
        let mut worker = supervisor::spawn_chat_completion_readiness_worker(
            port,
            "default_model".to_string(),
            supervisor::HEALTH_TIMEOUT,
            supervisor::HEALTH_POLL_INTERVAL,
        )
        .expect("spawn slow readiness worker");

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            Some(&mut worker),
            |_, _, _| panic!("worker owns Python readiness"),
        );
        server_thread.join().expect("join fake MLX server");

        assert_eq!(
            outcome.expect("slow completion readiness"),
            StartupWaitOutcome::Ready
        );
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn py_mlx_restart_helper() {
        if std::env::var_os("LOXA_MLX_RESTART_CHILD").as_deref() != Some(std::ffi::OsStr::new("1"))
        {
            return;
        }
        let Some(port) = std::env::var_os("LOXA_MLX_RESTART_HELPER_PORT") else {
            return;
        };
        let port = port.to_string_lossy().parse::<u16>().expect("helper port");
        let generation =
            std::env::var("LOXA_MLX_RESTART_HELPER_GENERATION").expect("helper generation");
        let requests_path = PathBuf::from(
            std::env::var_os("LOXA_MLX_RESTART_REQUESTS").expect("helper requests path"),
        );
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind restart helper");

        let (mut health, _) = listener.accept().expect("accept restart health");
        let health_request = read_test_http_request(&mut health);
        assert!(health_request.starts_with("GET /health "));
        respond_test_http(&mut health, r#"{"status":"ok"}"#);

        let (mut completion, _) = listener.accept().expect("accept restart completion");
        let completion_request = read_test_http_request(&mut completion);
        assert!(completion_request.starts_with("POST /v1/chat/completions "));
        assert!(completion_request.contains(r#""model":"default_model""#));
        let mut requests = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(requests_path)
            .expect("open readiness evidence");
        writeln!(requests, "generation={generation} health completion")
            .expect("write readiness evidence");
        if let Some(marker) = std::env::var_os("LOXA_MLX_RESTART_MARKER") {
            fs::write(marker, generation.as_bytes()).expect("write completion marker");
        }
        let delay = std::env::var("LOXA_MLX_RESTART_DELAY_MS")
            .expect("helper delay")
            .parse::<u64>()
            .expect("numeric helper delay");
        if delay > 0 {
            completion
                .set_read_timeout(Some(Duration::from_millis(50)))
                .expect("set cancellation poll timeout");
            let deadline = std::time::Instant::now() + Duration::from_millis(delay);
            let mut byte = [0_u8; 1];
            while std::time::Instant::now() < deadline {
                match completion.read(&mut byte) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(_) => return,
                }
            }
        }
        respond_test_http(
            &mut completion,
            r#"{"choices":[{"message":{"content":"ok"}}]}"#,
        );
        let expected_generation = generation.parse::<u64>().expect("numeric generation");
        let ready_ack_path = PathBuf::from(
            std::env::var_os("LOXA_MLX_RESTART_READY_ACK")
                .expect("helper ready acknowledgement path"),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let acknowledged = fs::read_to_string(&ready_ack_path)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .is_some_and(|ready_generation| ready_generation >= expected_generation);
            if acknowledged {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "model-ready acknowledgement timeout for generation {generation}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    fn run_model_for_test(
        id: &str,
        paths: &NodePaths,
    ) -> io::Result<(RunTermination, Vec<LifecycleEvent>)> {
        let ready_ack_path = PathBuf::from(
            std::env::var_os("LOXA_MLX_RESTART_READY_ACK").expect("ready acknowledgement path"),
        );
        let mut events = RestartRecordingLifecycleSink::new(ready_ack_path);
        let outcome = run_model(
            RunRequest {
                id,
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
            paths,
            None,
            &mut events,
        )?;
        Ok((outcome, events.events))
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn actual_run_model_restarts_python_once_with_same_backend_plan() {
        use std::os::unix::fs::PermissionsExt;

        let _lock = MLX_ENV_LOCK.lock().expect("MLX environment lock");
        let temp = TempDir::new("loxa-python-actual-restart");
        let bin_dir = temp.path().join("fake bin");
        let model_dir = temp.path().join("mlx model with spaces");
        let wrapper = bin_dir.join("mlx_lm.server");
        let version = bin_dir.join("mlx_lm");
        let count_path = temp.path().join("generation-count");
        let args_path = temp.path().join("launch-args");
        let requests_path = temp.path().join("readiness-requests");
        let marker_path = temp.path().join("completion-marker");
        let ready_ack_path = temp.path().join("ready-generation");
        fs::create_dir_all(&bin_dir).expect("create fake bin");
        fs::create_dir_all(&model_dir).expect("create fake model");
        fs::write(
            &wrapper,
            r#"#!/bin/sh
count=0
if [ -f "$LOXA_MLX_RESTART_COUNT" ]; then
  count=$(<"$LOXA_MLX_RESTART_COUNT")
fi
count=$((count + 1))
printf '%s\n' "$count" > "$LOXA_MLX_RESTART_COUNT"
printf 'generation=%s\n' "$count" >> "$LOXA_MLX_RESTART_ARGS"
for arg in "$@"; do
  printf 'arg=%s\n' "$arg" >> "$LOXA_MLX_RESTART_ARGS"
done
port=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = '--port' ]; then
    shift
    port="$1"
  fi
  shift
done
LOXA_MLX_RESTART_HELPER_PORT="$port" \
LOXA_MLX_RESTART_HELPER_GENERATION="$count" \
LOXA_MLX_RESTART_CHILD="1" \
"$LOXA_MLX_RESTART_TEST_EXE" --exact lifecycle_api_tests::py_mlx_restart_helper --nocapture
"#,
        )
        .expect("write fake server");
        fs::write(&version, "#!/bin/sh\nprintf '0.31.3\\n'\n").expect("write fake version command");
        for path in [&wrapper, &version] {
            let mut permissions = fs::metadata(path).expect("fake metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("make fake executable");
        }
        let test_exe = std::env::current_exe().expect("test executable");
        let _environment = TestEnvRestore::set(&[
            ("LOXA_MLX_LM_SERVER", wrapper.as_os_str()),
            ("LOXA_MLX_RESTART_COUNT", count_path.as_os_str()),
            ("LOXA_MLX_RESTART_ARGS", args_path.as_os_str()),
            ("LOXA_MLX_RESTART_REQUESTS", requests_path.as_os_str()),
            ("LOXA_MLX_RESTART_TEST_EXE", test_exe.as_os_str()),
            ("LOXA_MLX_RESTART_MARKER", marker_path.as_os_str()),
            ("LOXA_MLX_RESTART_READY_ACK", ready_ack_path.as_os_str()),
            ("LOXA_MLX_RESTART_DELAY_MS", std::ffi::OsStr::new("0")),
        ]);
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let (outcome, events) =
            run_model_for_test(model_dir.to_str().expect("utf8 model path"), &paths)
                .expect("run fake Python engine");

        assert_eq!(
            outcome,
            RunTermination::Failed,
            "generation one must exhaust restart"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    LifecycleEvent::Restarting {
                        before_healthy: false,
                        ..
                    }
                ))
                .count(),
            1,
            "{events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::ModelReady { .. }))
                .count(),
            2,
            "{events:?}"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                LifecycleEvent::EngineExited {
                    before_healthy: false,
                    ..
                }
            )),
            "{events:?}"
        );

        let canonical_model = fs::canonicalize(&model_dir).expect("canonical model");
        let args = fs::read_to_string(&args_path).expect("read launch arguments");
        let generations = args
            .split("generation=")
            .filter(|block| !block.is_empty())
            .map(|block| {
                let mut lines = block.lines();
                let generation = lines.next().expect("generation number").to_string();
                let argv = lines
                    .map(|line| {
                        line.strip_prefix("arg=")
                            .expect("only argv evidence follows generation")
                            .to_string()
                    })
                    .collect::<Vec<_>>();
                (generation, argv)
            })
            .collect::<Vec<_>>();
        assert_eq!(generations.len(), 2, "{args}");
        assert_eq!(generations[0].0, "1");
        assert_eq!(generations[1].0, "2");
        for (_, argv) in &generations {
            assert_eq!(argv.len(), 6, "unexpected extra argv: {argv:?}");
            assert_eq!(argv[0], "--model");
            assert_eq!(argv[1], canonical_model.display().to_string());
            assert_eq!(argv[2], "--host");
            assert_eq!(argv[3], "127.0.0.1");
            assert_eq!(argv[4], "--port");
            assert!(argv[5].parse::<u16>().is_ok(), "invalid port: {argv:?}");
        }
        assert_eq!(generations[0].1, generations[1].1);
        let requests = fs::read_to_string(&requests_path).expect("read readiness evidence");
        assert!(
            requests.contains("generation=1 health completion"),
            "{requests}"
        );
        assert!(
            requests.contains("generation=2 health completion"),
            "{requests}"
        );
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).expect("read final state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        for path in [
            &count_path,
            &args_path,
            &requests_path,
            &marker_path,
            &ready_ack_path,
        ] {
            let _ = fs::remove_file(path);
        }
        unsafe { std::env::set_var("LOXA_MLX_RESTART_DELAY_MS", "5000") };
        let stop_paths = NodePaths {
            models_dir: temp.path().join("models-stop"),
            state_path: temp.path().join("managed-stop.json"),
            logs_dir: temp.path().join("logs-stop"),
        };
        let stop_state = stop_paths.state_path.clone();
        let stop_marker = marker_path.clone();
        let stop_thread = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !stop_marker.is_file() {
                assert!(std::time::Instant::now() < deadline, "stop marker timeout");
                std::thread::sleep(Duration::from_millis(10));
            }
            supervisor::request_managed_stop(&stop_state, "all").expect("request external stop")
        });
        let stop_started = std::time::Instant::now();
        let (stop_outcome_kind, _) =
            run_model_for_test(model_dir.to_str().expect("utf8 model path"), &stop_paths)
                .expect("stop stalled Python readiness");
        let stop_outcome = stop_thread.join().expect("join external stop request");
        assert_eq!(stop_outcome_kind, RunTermination::RequestedStop);
        assert!(
            matches!(
                stop_outcome,
                supervisor::StopRequestOutcome::Completed { .. }
            ),
            "{stop_outcome:?}"
        );
        assert!(
            stop_started.elapsed() < Duration::from_secs(2),
            "external stop was blocked for {:?}",
            stop_started.elapsed()
        );
        assert_eq!(
            fs::read_to_string(&count_path)
                .expect("stop generation count")
                .trim(),
            "1"
        );
        assert_eq!(
            supervisor::read_runtime_state(&stop_paths.state_path).expect("read stopped state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        for path in [
            &count_path,
            &args_path,
            &requests_path,
            &marker_path,
            &ready_ack_path,
        ] {
            let _ = fs::remove_file(path);
        }
        let interrupt_paths = NodePaths {
            models_dir: temp.path().join("models-interrupt"),
            state_path: temp.path().join("managed-interrupt.json"),
            logs_dir: temp.path().join("logs-interrupt"),
        };
        let interrupt_marker = marker_path.clone();
        let interrupt_thread = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !interrupt_marker.is_file() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "interrupt marker timeout"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            set_ctrl_c_received();
        });
        let interrupt_started = std::time::Instant::now();
        let (interrupt_outcome, _) = run_model_for_test(
            model_dir.to_str().expect("utf8 model path"),
            &interrupt_paths,
        )
        .expect("interrupt stalled Python readiness");
        interrupt_thread.join().expect("join interrupt request");
        clear_ctrl_c_received();
        assert_eq!(interrupt_outcome, RunTermination::Interrupted);
        assert!(
            interrupt_started.elapsed() < Duration::from_secs(2),
            "interrupt was blocked for {:?}",
            interrupt_started.elapsed()
        );
        assert_eq!(
            fs::read_to_string(&count_path)
                .expect("interrupt generation count")
                .trim(),
            "1"
        );
        assert_eq!(
            supervisor::read_runtime_state(&interrupt_paths.state_path)
                .expect("read interrupted state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn post_spawn_running_state_read_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-running-state-read");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt running state");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
        let mut events = RecordingLifecycleSink::default();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut events,
        )
        .expect("running recovery outcome");

        assert_eq!(outcome, RunOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_running_try_wait_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-running-try-wait");
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
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_error_then(vec![Some(0)]);
        let mut events = RecordingLifecycleSink::default();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut events,
        )
        .expect("running recovery outcome");

        assert_eq!(outcome, RunOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn startup_polling_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-startup-stop-poll");
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
            let run = persist_run_for_server(&state_path, &server);
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results_and_drain_error(
                vec![None, Some(0)],
                confirmation == supervisor::TeardownConfirmation::Unconfirmed,
            )
            .with_owned_process_group();
            let identity = run.identity();

            let outcome = wait_for_startup_owned(
                &mut child,
                &identity,
                &state_path,
                &signal,
                None,
                |_, _, _| {
                    request_stop_for_test(&state_path, &identity);
                    Ok(StartupPoll::Pending)
                },
            )
            .expect("startup stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                StartupWaitOutcome::RequestedStop
            } else {
                StartupWaitOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert!(child.events.borrow().contains(&"join_log_drains"));
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read startup state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_loop_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-running-stop-poll");
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
            let run = persist_run_for_server(&state_path, &server);
            let stopped = request_stop_for_test(&state_path, &run.identity());
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results_and_drain_error(
                vec![Some(0)],
                confirmation == supervisor::TeardownConfirmation::Unconfirmed,
            )
            .with_owned_process_group();
            let mut events = RecordingLifecycleSink::default();

            let outcome = supervise_running_server(
                RunSession {
                    id: &server.id,
                    state_identity: &stopped.identity(),
                    log_path: stopped.log_path.as_path(),
                    state_path: &state_path,
                },
                &mut child,
                &signal,
                None,
                "llama-server",
                &mut events,
            )
            .expect("running stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                RunOutcome::RequestedStop
            } else {
                RunOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert!(events.events.is_empty());
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read running state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_reaped_exit_with_concurrent_stop_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-stop");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false]);
        let identity = run.identity();
        let callback_state_path = state_path.clone();
        let callback_identity = identity.clone();
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)])
            .on_join_log_drains(move || {
                request_stop_for_test(&callback_state_path, &callback_identity);
            })
            .with_owned_process_group();
        let mut events = RecordingLifecycleSink::default();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &identity,
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut events,
        )
        .expect("reaped stop outcome");

        assert_eq!(outcome, RunOutcome::RequestedStop);
        let child_events = child.events.into_inner();
        assert_eq!(
            child_events
                .iter()
                .filter(|event| matches!(event, &&"terminate" | &&"kill"))
                .count(),
            0
        );
        assert_eq!(
            child_events
                .iter()
                .filter(|event| **event == "join_log_drains")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_reaped_exit_with_concurrent_interrupt_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-interrupt");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child =
            FakeStartupChild::with_wait_results(vec![Some(1)]).with_owned_process_group();
        let mut events = RecordingLifecycleSink::default();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut events,
        )
        .expect("reaped interrupt outcome");

        assert_eq!(outcome, RunOutcome::Interrupted);
        let child_events = child.events.into_inner();
        assert_eq!(
            child_events
                .iter()
                .filter(|event| matches!(event, &&"terminate" | &&"kill"))
                .count(),
            0
        );
        assert_eq!(
            child_events
                .iter()
                .filter(|event| **event == "join_log_drains")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_restart_event_failure_retains_handoff_and_stop_wins() {
        let temp = TempDir::new("loxa-running-restart-broken-pipe");
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
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false, false]);
        let mut child =
            FakeStartupChild::with_wait_results(vec![Some(1)]).with_owned_process_group();
        let mut events = FailingLifecycleSink { events: Vec::new() };

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut events,
        )
        .expect("restart event failure must not drop the owned handoff");
        let RunOutcome::Restart { run: replacement } = outcome else {
            panic!("expected owned replacement handoff, got {outcome:?}");
        };
        request_stop_for_test(&state_path, &replacement.identity());
        let terminal = finish_owned_replacement_error::<(), ()>(
            &state_path,
            &replacement,
            SupervisorError::NoFreePort,
        )
        .expect("committed stop must win after the non-fatal event");

        assert!(matches!(
            terminal,
            OwnedReplacementPreparation::RequestedStop
        ));
        assert!(matches!(
            events.events.as_slice(),
            [LifecycleEvent::Restarting { .. }]
        ));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    fn assert_startup_reaped_diagnostic_failure_never_resignals(drain_fails: bool) {
        let temp = TempDir::new("loxa-startup-reaped-diagnostics");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 65_535,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        if drain_fails {
            fs::write(
                &run.log_path,
                b"diagnostics exist but drain joining failed\n",
            )
            .expect("write crash log for drain failure");
        }
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child =
            FakeStartupChild::with_wait_results_and_drain_error(vec![Some(1)], drain_fails);

        let failure = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            None,
            |child, _, _| match supervisor::wait_for_generation_ready_or_exit(
                child,
                server.port,
                &run.generation_alias,
                Duration::ZERO,
                Duration::ZERO,
            ) {
                Ok(()) => Ok(StartupPoll::Ready),
                Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                Err(error) => Err(error),
            },
        )
        .expect_err("reaped child diagnostic failure");

        let observed_exit = match failure {
            StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            } => Some(
                supervisor::decide_observed_child_exit(
                    diagnostics_error
                        .map(|error| format!("crash diagnostics unavailable: {error}"))
                        .unwrap_or(log_tail),
                    &state_path,
                    &run.identity(),
                    &signal,
                    |decision| {
                        assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                        if child.join_log_drains().is_ok() {
                            supervisor::TeardownConfirmation::Confirmed
                        } else {
                            supervisor::TeardownConfirmation::Unconfirmed
                        }
                    },
                )
                .expect("transition already-reaped startup child"),
            ),
            StartupWaitFailure::BeforeTeardown(_) => {
                let _ = supervisor::cleanup_after_ctrl_c(
                    &mut child,
                    &state_path,
                    &run.identity(),
                    supervisor::CTRL_C_GRACE_PERIOD,
                );
                let _ = child.join_log_drains();
                None
            }
            StartupWaitFailure::AfterTeardown(_) => None,
        };

        let events = child.events.into_inner();
        assert_eq!(
            events.iter().filter(|event| **event == "terminate").count(),
            0,
            "an already-reaped PID must never be terminated again: {events:?}"
        );
        assert_eq!(
            events.iter().filter(|event| **event == "kill").count(),
            0,
            "an already-reaped PID must never be killed again: {events:?}"
        );
        if drain_fails {
            assert_eq!(observed_exit, Some(ObservedChildExit::RecoveryRequired));
            assert!(matches!(
                supervisor::read_runtime_state(&state_path).expect("read preserved state"),
                RuntimeStateRead::Loaded(runs) if runs.len() == 1
            ));
        } else {
            assert!(matches!(
                observed_exit,
                Some(ObservedChildExit::Restart { .. })
            ));
        }
    }

    #[test]
    fn startup_reaped_drain_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(true);
    }

    #[test]
    fn startup_reaped_log_tail_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(false);
    }

    #[test]
    fn startup_restart_handoff_cas_conflict_preserves_newer_state() {
        let temp = TempDir::new("loxa-startup-restart-broken-pipe");
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
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let observed = supervisor::decide_observed_child_exit(
            "startup crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |decision| {
                assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                supervisor::TeardownConfirmation::Confirmed
            },
        )
        .expect("publish owned generation one");
        let ObservedChildExit::Restart { run: replacement } = observed else {
            panic!("expected replacement handoff, got {observed:?}");
        };
        let replacement_identity = replacement.identity();
        let newer_run = starting_run_for_test(&state_path, "newer-run");
        assert!(
            supervisor::finish_runtime_state_run(&state_path, &replacement_identity)
                .expect("exact-finish generation one")
        );
        supervisor::create_starting_run(&state_path, newer_run.clone())
            .expect("publish newer state");
        let error = finish_owned_replacement_error::<(), ()>(
            &state_path,
            &replacement,
            SupervisorError::NoFreePort,
        )
        .expect_err("newer exact state must beat stale handoff");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read newer state"),
            RuntimeStateRead::Loaded(vec![newer_run])
        );
    }

    #[test]
    fn attachment_requested_stop_maps_to_typed_termination() {
        let boundary = resolve_managed_attachment_typed(
            supervisor::PersistManagedServerOutcome::RequestedStop,
        );

        assert!(matches!(
            boundary,
            TypedManagedAttachmentBoundary::Terminal(RunTermination::RequestedStop)
        ));
    }

    #[test]
    fn unconfirmed_requested_stop_exits_1_and_preserves_state() {
        let temp = TempDir::new("loxa-unconfirmed-requested-stop-exit");
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
        let run = persist_run_for_server(&state_path, &server);
        let stopped = request_stop_for_test(&state_path, &run.identity());
        let teardown_calls = Cell::new(0_u8);

        let outcome = supervisor::finish_owner_teardown_with(
            &state_path,
            &stopped.identity(),
            supervisor::OwnerTeardownDecision::RequestedStop,
            |_| {
                teardown_calls.set(teardown_calls.get() + 1);
                supervisor::TeardownConfirmation::Unconfirmed
            },
        )
        .expect("unconfirmed stop outcome");

        assert_eq!(outcome, supervisor::OwnerTerminalOutcome::RecoveryRequired);
        assert_eq!(
            owner_terminal_termination(outcome),
            RunTermination::RecoveryRequired
        );
        assert_eq!(teardown_calls.get(), 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved stopped state"),
            RuntimeStateRead::Loaded(vec![stopped])
        );
    }

    #[test]
    fn unconfirmed_generation_zero_unexpected_exit_exits_1_without_restart_and_preserves_exact_state(
    ) {
        let temp = TempDir::new("loxa-unconfirmed-generation-zero-exit");
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
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let teardown_calls = Cell::new(0_u8);

        let outcome = supervisor::decide_observed_child_exit(
            "first crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |_| {
                teardown_calls.set(teardown_calls.get() + 1);
                supervisor::TeardownConfirmation::Unconfirmed
            },
        )
        .expect("unconfirmed generation-zero outcome");

        assert_eq!(outcome, ObservedChildExit::RecoveryRequired);
        assert_eq!(teardown_calls.get(), 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved generation zero"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn confirmed_second_crash_removes_exact_state_then_exits_1() {
        let temp = TempDir::new("loxa-confirmed-second-crash-exit");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 778,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(222),
        };
        let mut run = persist_run_for_server(&state_path, &server);
        let old_identity = run.identity();
        run.generation = 1;
        run.generation_alias = "loxa-test-run-778-g1".to_string();
        assert!(
            supervisor::update_runtime_state_run(&state_path, &old_identity, run.clone(),)
                .expect("publish generation one")
        );
        let signal = FakeInterruptSource::new(vec![false]);

        let outcome = supervisor::decide_observed_child_exit(
            "second crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |_| supervisor::TeardownConfirmation::Confirmed,
        )
        .expect("confirmed second-crash outcome");

        assert!(matches!(outcome, ObservedChildExit::Exhausted { .. }));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read removed generation one"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn post_spawn_startup_timeout_emits_typed_lifecycle_event() {
        let log_path = PathBuf::from("/tmp/loxa-startup-timeout.log");
        let mut events = RecordingLifecycleSink::default();

        let termination = finish_startup_failure(
            &mut events,
            &log_path,
            "llama-server",
            SupervisorError::HealthTimeout,
        )
        .expect("emit confirmed startup timeout");

        assert_eq!(termination, RunTermination::Failed);
        assert_eq!(
            events.events,
            vec![LifecycleEvent::HealthTimeout {
                process_label: "llama-server".to_string(),
                log_path,
            }]
        );
    }

    #[test]
    fn recovery_required_emits_typed_lifecycle_event() {
        let mut events = RecordingLifecycleSink::default();

        let termination =
            emit_recovery_required(&mut events, "run-1").expect("emit recovery state");

        assert_eq!(termination, RunTermination::RecoveryRequired);
        assert_eq!(
            events.events,
            vec![LifecycleEvent::RecoveryRequired {
                run_id: "run-1".to_string(),
            }]
        );
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            let path = fs::canonicalize(path).expect("canonical temp dir");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .expect("secure temp dir");
            }
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct FakeInterruptSource {
        states: Vec<bool>,
        index: Cell<usize>,
    }

    impl FakeInterruptSource {
        fn new(states: Vec<bool>) -> Self {
            Self {
                states,
                index: Cell::new(0),
            }
        }
    }

    impl InterruptSource for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            let index = self.index.get();
            let value = self
                .states
                .get(index)
                .copied()
                .or_else(|| self.states.last().copied())
                .unwrap_or(false);
            if index + 1 < self.states.len() {
                self.index.set(index + 1);
            }
            value
        }
    }

    impl InterruptStatus for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            InterruptSource::interrupted(self)
        }
    }

    struct FakeStartupChild {
        events: RefCell<Vec<&'static str>>,
        wait_results: RefCell<Vec<Option<i32>>>,
        drain_error: bool,
        wait_error_once: Cell<bool>,
        shared_exit: Option<Arc<TestAtomicBool>>,
        on_join_log_drains: RefCell<Option<Box<dyn FnOnce()>>>,
        owned_pgid: Option<i32>,
    }

    impl FakeStartupChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self::with_wait_results_and_drain_error(wait_results, false)
        }

        fn with_wait_results_and_drain_error(
            wait_results: Vec<Option<i32>>,
            drain_error: bool,
        ) -> Self {
            Self {
                events: RefCell::new(Vec::new()),
                wait_results: RefCell::new(wait_results),
                drain_error,
                wait_error_once: Cell::new(false),
                shared_exit: None,
                on_join_log_drains: RefCell::new(None),
                owned_pgid: None,
            }
        }

        fn with_owned_process_group(mut self) -> Self {
            self.owned_pgid = Some(2_000_000);
            self
        }

        fn on_join_log_drains(mut self, callback: impl FnOnce() + 'static) -> Self {
            *self.on_join_log_drains.get_mut() = Some(Box::new(callback));
            self
        }

        fn with_shared_exit(shared_exit: Arc<TestAtomicBool>) -> Self {
            let mut child = Self::with_wait_results(vec![None]);
            child.shared_exit = Some(shared_exit);
            child
        }

        fn with_wait_error_then(wait_results: Vec<Option<i32>>) -> Self {
            let child = Self::with_wait_results(wait_results);
            child.wait_error_once.set(true);
            child
        }
    }

    impl ManagedChild for FakeStartupChild {
        fn pid(&self) -> u32 {
            2_000_000
        }

        fn owned_pgid(&self) -> Option<i32> {
            self.owned_pgid
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.borrow_mut().push("try_wait");
            if self
                .shared_exit
                .as_ref()
                .is_some_and(|exited| exited.load(TestOrdering::SeqCst))
            {
                return Ok(Some(0));
            }
            if self.wait_error_once.replace(false) {
                return Err(io::Error::other("injected try_wait failure"));
            }
            let mut wait_results = self.wait_results.borrow_mut();
            if wait_results.len() > 1 {
                Ok(wait_results.remove(0))
            } else {
                Ok(wait_results.first().copied().unwrap_or(Some(0)))
            }
        }
    }

    impl LogDrainingChild for FakeStartupChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("join_log_drains");
            if let Some(callback) = self.on_join_log_drains.borrow_mut().take() {
                callback();
            }
            if self.drain_error {
                Err(SupervisorError::Io(io::Error::other(
                    "injected log-drain join failure",
                )))
            } else {
                Ok(())
            }
        }
    }
}
