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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod actor;
mod bootstrap;
pub mod chat_history;
pub mod chat_routes;
pub mod control_router;
#[allow(dead_code, unused_imports)]
mod control_state;
mod daemon;
pub mod download_control;
mod engine_session;
mod http_observability;
#[allow(dead_code)]
mod identity;
pub mod model_lifecycle;
mod production_lifecycle;
mod runtime;
#[cfg(test)]
mod slice3_test_support;
mod v2_control_router;

#[cfg(test)]
struct Slice3ControlStateFixture {
    handle: control_state::ControlStateHandle,
    bootstrap: Option<control_state::ControlStateBootstrap>,
}

#[cfg(test)]
impl Slice3ControlStateFixture {
    async fn shutdown(mut self) {
        let bootstrap = self.bootstrap.take().unwrap();
        bootstrap.worker.shutdown().await.unwrap();
        drop(bootstrap.handle);
        drop(self.handle);
        drop(bootstrap.claimed_owner);
    }
}

#[cfg(test)]
fn open_slice3_control_state_fixture(
    path: PathBuf,
    node_id: loxa_protocol::NodeId,
    paths: NodePaths,
    baseline: supervisor::ManagedRun,
) -> Result<Slice3ControlStateFixture, control_state::ControlStateError> {
    let bootstrap = control_state::open_control_state_for_test(control_state::ControlStateInit {
        path: path.into(),
        node_id,
        open_input: control_state::ControlStateOpenInput {
            claimed_owner: runtime::NodeOwnerGuard::new(paths, baseline),
            first_migration_source: Some(control_state::ScalarSource::Fresh),
        },
        recovery_evidence: control_state::ownership_unavailable_recovery_for_test(),
        now_unix_ms: 10,
    })?;
    Ok(Slice3ControlStateFixture {
        handle: bootstrap.handle.clone(),
        bootstrap: Some(bootstrap),
    })
}

pub use bootstrap::{
    emit_final_shutdown_diagnostic, install_daemon_diagnostics, DiagnosticsBootstrap, NodePaths,
};

use bootstrap::NodeBuilder;
use daemon::signals::{InterruptSource, SignalGuard};
use engine_session::EngineSession;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedPythonOwnerDisposition {
    Restored(supervisor::ManagedRun),
    ConsumedByRequestedStop,
    RecoveryRequired,
}

#[derive(Debug)]
pub(crate) struct PreparedPythonRunResult {
    pub(crate) outcome: io::Result<RunTermination>,
    pub(crate) owner: PreparedPythonOwnerDisposition,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedPythonOwnerPolicy<'a> {
    baseline: &'a supervisor::ManagedRun,
}

impl<'a> PreparedPythonOwnerPolicy<'a> {
    pub(crate) fn new(baseline: &'a supervisor::ManagedRun) -> Self {
        Self { baseline }
    }

    fn finish_childless(
        self,
        state_path: &Path,
        current: &supervisor::ManagedRun,
    ) -> Result<PreparedPythonOwnerDisposition, SupervisorError> {
        self.finish(
            state_path,
            current,
            supervisor::PreparedOwnerCleanup::Childless,
        )
    }

    fn finish(
        self,
        state_path: &Path,
        current: &supervisor::ManagedRun,
        cleanup: supervisor::PreparedOwnerCleanup,
    ) -> Result<PreparedPythonOwnerDisposition, SupervisorError> {
        match supervisor::restore_unloaded_owner_after_prepared_run(
            state_path,
            current,
            self.baseline,
            cleanup,
        )? {
            supervisor::RestoreUnloadedOwnerOutcome::Restored(run) => {
                Ok(PreparedPythonOwnerDisposition::Restored(run))
            }
            supervisor::RestoreUnloadedOwnerOutcome::RequestedStop => {
                Ok(PreparedPythonOwnerDisposition::ConsumedByRequestedStop)
            }
            supervisor::RestoreUnloadedOwnerOutcome::RecoveryRequired => {
                Ok(PreparedPythonOwnerDisposition::RecoveryRequired)
            }
        }
    }

    fn cleanup_child<C>(
        self,
        child: &mut C,
        state_path: &Path,
        current: &supervisor::ManagedRun,
    ) -> Result<PreparedPythonOwnerDisposition, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        let confirmation =
            supervisor::teardown_managed_child(child, supervisor::CTRL_C_GRACE_PERIOD)?;
        self.finish(
            state_path,
            current,
            match confirmation {
                supervisor::TeardownConfirmation::Confirmed => {
                    supervisor::PreparedOwnerCleanup::ConfirmedReaped
                }
                supervisor::TeardownConfirmation::Unconfirmed => {
                    supervisor::PreparedOwnerCleanup::Uncertain
                }
            },
        )
    }
}

#[derive(Clone, Copy)]
enum RunOwnerPolicy<'a> {
    Standalone,
    Prepared(PreparedPythonOwnerPolicy<'a>),
}

impl RunOwnerPolicy<'_> {
    fn handle_observed_exit<C, I>(
        self,
        child: &mut C,
        log_path: &Path,
        state_path: &Path,
        state_identity: &supervisor::ManagedRunIdentity,
        interrupt: &I,
    ) -> Result<ObservedChildExit, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
        I: InterruptStatus,
    {
        match self {
            Self::Standalone => supervisor::handle_observed_child_exit(
                child,
                log_path,
                state_path,
                state_identity,
                interrupt,
            ),
            Self::Prepared(policy) => supervisor::handle_observed_child_exit_prepared(
                child,
                log_path,
                state_path,
                state_identity,
                policy.baseline,
                interrupt,
            ),
        }
    }

    fn current(
        self,
        state_path: &Path,
        state_identity: &supervisor::ManagedRunIdentity,
    ) -> Result<supervisor::ManagedRun, SupervisorError> {
        supervisor::current_runtime_state_run(state_path, state_identity)
    }

    fn cleanup_by_identity<C>(
        self,
        child: &mut C,
        state_path: &Path,
        state_identity: &supervisor::ManagedRunIdentity,
    ) -> Result<supervisor::PostSpawnCleanupOutcome, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        match self {
            Self::Standalone => {
                supervisor::cleanup_post_spawn_failure(child, state_path, state_identity)
            }
            Self::Prepared(_) => match self.current(state_path, state_identity) {
                Ok(current) => self.cleanup_child(child, state_path, &current),
                Err(_) => Ok(supervisor::PostSpawnCleanupOutcome::RecoveryRequired),
            },
        }
    }

    fn teardown_by_identity<C>(
        self,
        child: &mut C,
        state_path: &Path,
        state_identity: &supervisor::ManagedRunIdentity,
        decision: supervisor::OwnerTeardownDecision,
    ) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        match self {
            Self::Standalone => {
                supervisor::teardown_owned_run(child, state_path, state_identity, decision)
            }
            Self::Prepared(_) => match self.current(state_path, state_identity) {
                Ok(current) => self.teardown_child(child, state_path, &current, decision),
                Err(_) => Ok(supervisor::OwnerTerminalOutcome::RecoveryRequired),
            },
        }
    }

    fn finish_childless(
        self,
        state_path: &Path,
        current: &supervisor::ManagedRun,
    ) -> Result<supervisor::ChildlessFinishOutcome, SupervisorError> {
        match self {
            Self::Standalone => {
                supervisor::finish_childless_runtime_state_run(state_path, &current.identity())
            }
            Self::Prepared(policy) => match policy.finish_childless(state_path, current)? {
                PreparedPythonOwnerDisposition::Restored(_) => {
                    Ok(supervisor::ChildlessFinishOutcome::Finished)
                }
                PreparedPythonOwnerDisposition::ConsumedByRequestedStop => {
                    Ok(supervisor::ChildlessFinishOutcome::RequestedStop)
                }
                PreparedPythonOwnerDisposition::RecoveryRequired => {
                    Err(SupervisorError::RecoveryRequired(current.run_id.clone()))
                }
            },
        }
    }

    fn cleanup_child<C>(
        self,
        child: &mut C,
        state_path: &Path,
        current: &supervisor::ManagedRun,
    ) -> Result<supervisor::PostSpawnCleanupOutcome, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        match self {
            Self::Standalone => {
                supervisor::cleanup_post_spawn_failure(child, state_path, &current.identity())
            }
            Self::Prepared(policy) => match policy.cleanup_child(child, state_path, current)? {
                PreparedPythonOwnerDisposition::Restored(_) => {
                    Ok(supervisor::PostSpawnCleanupOutcome::Cleaned)
                }
                PreparedPythonOwnerDisposition::ConsumedByRequestedStop => {
                    Ok(supervisor::PostSpawnCleanupOutcome::RequestedStop)
                }
                PreparedPythonOwnerDisposition::RecoveryRequired => {
                    Ok(supervisor::PostSpawnCleanupOutcome::RecoveryRequired)
                }
            },
        }
    }

    fn attach_or_cleanup<C>(
        self,
        child: &mut C,
        state_path: &Path,
        run: supervisor::ManagedRun,
        server: ManagedServer,
    ) -> Result<supervisor::PersistManagedServerOutcome, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        match self {
            Self::Standalone => supervisor::persist_managed_server_or_cleanup(
                child,
                state_path,
                run,
                server,
                supervisor::CTRL_C_GRACE_PERIOD,
            ),
            Self::Prepared(policy) => supervisor::persist_managed_server_or_cleanup_prepared(
                child,
                state_path,
                run,
                server,
                policy.baseline,
            ),
        }
    }

    fn teardown_child<C>(
        self,
        child: &mut C,
        state_path: &Path,
        current: &supervisor::ManagedRun,
        decision: supervisor::OwnerTeardownDecision,
    ) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>
    where
        C: ManagedChild + LogDrainingChild,
    {
        match self {
            Self::Standalone => {
                supervisor::teardown_owned_run(child, state_path, &current.identity(), decision)
            }
            Self::Prepared(policy) => match policy.cleanup_child(child, state_path, current)? {
                PreparedPythonOwnerDisposition::Restored(_) => {
                    Ok(supervisor::OwnerTerminalOutcome::Interrupted)
                }
                PreparedPythonOwnerDisposition::ConsumedByRequestedStop => {
                    Ok(supervisor::OwnerTerminalOutcome::RequestedStop)
                }
                PreparedPythonOwnerDisposition::RecoveryRequired => {
                    Ok(supervisor::OwnerTerminalOutcome::RecoveryRequired)
                }
            },
        }
    }
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
    run_model_with_diagnostics_health(
        request,
        paths,
        gateway,
        events,
        &loxa_core::diagnostics::DiagnosticsHealth::new(),
    )
}

fn emit_engine_readiness_failed(
    generation: u64,
    backend_kind: &'static str,
    result_class: &'static str,
) {
    tracing::warn!(
        target: "loxa_core::engine",
        event_code = "engine.readiness.failed",
        component = "engine",
        generation,
        backend_kind,
        result_class,
    );
}

fn run_model_with_diagnostics_health(
    request: RunRequest<'_>,
    paths: &NodePaths,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    events: &mut dyn LifecycleEventSink,
    diagnostics_health: &loxa_core::diagnostics::DiagnosticsHealth,
) -> io::Result<RunTermination> {
    run_model_with_owner_policy(
        request,
        paths,
        gateway,
        events,
        diagnostics_health,
        RunOwnerPolicy::Standalone,
        None,
    )
}

pub(crate) fn run_prepared_python_model_with_diagnostics_health(
    request: RunRequest<'_>,
    paths: &NodePaths,
    baseline: &supervisor::ManagedRun,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    events: &mut dyn LifecycleEventSink,
    diagnostics_health: &loxa_core::diagnostics::DiagnosticsHealth,
    durable_interrupt: Option<&std::sync::atomic::AtomicBool>,
) -> PreparedPythonRunResult {
    if request.engine != RuntimeBackendKind::PyMlxLm {
        return PreparedPythonRunResult {
            outcome: Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prepared node owner supports only the Python MLX backend",
            )),
            owner: PreparedPythonOwnerDisposition::Restored(baseline.clone()),
        };
    }
    let outcome = run_model_with_owner_policy(
        request,
        paths,
        gateway,
        events,
        diagnostics_health,
        RunOwnerPolicy::Prepared(PreparedPythonOwnerPolicy::new(baseline)),
        durable_interrupt,
    );
    let owner = classify_prepared_python_owner(&paths.state_path, baseline, &outcome);
    PreparedPythonRunResult { outcome, owner }
}

fn classify_prepared_python_owner(
    state_path: &Path,
    baseline: &supervisor::ManagedRun,
    outcome: &io::Result<RunTermination>,
) -> PreparedPythonOwnerDisposition {
    let Ok(RuntimeStateRead::Loaded(runs)) = supervisor::read_runtime_state(state_path) else {
        return PreparedPythonOwnerDisposition::RecoveryRequired;
    };
    let [] = runs.as_slice() else {
        if let [current] = runs.as_slice() {
            let mut current = current.clone();
            current.stop_requested = baseline.stop_requested;
            if current == *baseline {
                return PreparedPythonOwnerDisposition::Restored(baseline.clone());
            }
        }
        return PreparedPythonOwnerDisposition::RecoveryRequired;
    };
    if matches!(outcome, Ok(RunTermination::RequestedStop)) {
        PreparedPythonOwnerDisposition::ConsumedByRequestedStop
    } else {
        PreparedPythonOwnerDisposition::RecoveryRequired
    }
}

struct RuntimeInterrupt<'a> {
    signal: &'a SignalGuard,
    durable: Option<&'a std::sync::atomic::AtomicBool>,
}

impl RuntimeInterrupt<'_> {
    fn interrupted(&self) -> bool {
        self.signal.interrupted()
            || self
                .durable
                .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
    }
}

impl InterruptSource for RuntimeInterrupt<'_> {
    fn interrupted(&self) -> bool {
        self.interrupted()
    }
}

impl InterruptStatus for RuntimeInterrupt<'_> {
    fn interrupted(&self) -> bool {
        self.interrupted()
    }
}

fn run_model_with_owner_policy(
    request: RunRequest<'_>,
    paths: &NodePaths,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    events: &mut dyn LifecycleEventSink,
    diagnostics_health: &loxa_core::diagnostics::DiagnosticsHealth,
    owner_policy: RunOwnerPolicy<'_>,
    durable_interrupt: Option<&std::sync::atomic::AtomicBool>,
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

    let installed_signal = SignalGuard::install()?;
    let signal_guard = RuntimeInterrupt {
        signal: &installed_signal,
        durable: durable_interrupt,
    };
    let (owner_pid, owner_process_start_time_unix_s, run_id) = match owner_policy {
        RunOwnerPolicy::Standalone => {
            let owner_pid = std::process::id();
            let owner_start =
                supervisor::process_start_time_with_retry(owner_pid).ok_or_else(|| {
                    supervisor_error_to_io(SupervisorError::ProcessIdentityUnavailable(owner_pid))
                })?;
            (
                owner_pid,
                owner_start,
                format!("run-{owner_pid}-{owner_start}"),
            )
        }
        RunOwnerPolicy::Prepared(policy) => (
            policy.baseline.owner_pid,
            policy.baseline.owner_process_start_time_unix_s,
            policy.baseline.run_id.clone(),
        ),
    };
    let mut replacement_run: Option<supervisor::ManagedRun> = None;

    loop {
        let owned_replacement = replacement_run.take();
        let started_at_unix_s = unix_timestamp_now();
        let (backend, starting_run, initial_generation, initial_reservation) =
            if let Some(run) = owned_replacement {
                let preparation = prepare_owned_replacement_run(
                    &paths.state_path,
                    run,
                    owner_policy,
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
                let standalone_starting = supervisor::ManagedRun {
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
                };
                let (starting, create) = match owner_policy {
                    RunOwnerPolicy::Standalone => (standalone_starting, true),
                    RunOwnerPolicy::Prepared(policy) => {
                        match supervisor::prepare_unloaded_owner_for_model(
                            &paths.state_path,
                            policy.baseline,
                            id.to_string(),
                            selected_port,
                            standalone_starting.log_path,
                        )
                        .map_err(supervisor_error_to_io)?
                        {
                            supervisor::PrepareUnloadedOwnerOutcome::Prepared(run) => (run, false),
                            supervisor::PrepareUnloadedOwnerOutcome::RequestedStop => {
                                return Ok(RunTermination::RequestedStop);
                            }
                        }
                    }
                };
                (backend, starting, create, Some(reservation))
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
                return match owner_policy
                    .finish_childless(&paths.state_path, &starting_run)
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
        let spawn = supervisor::spawn_starting_engine_with_health(
            &paths.state_path,
            &starting_run.identity(),
            &spec,
            &log_path,
            reservation,
            diagnostics_health,
        );
        let (starting_run, mut child) = match spawn {
            Ok(supervisor::SpawnStartingRunOutcome::Spawned { run, value }) => (run, value),
            Ok(supervisor::SpawnStartingRunOutcome::RequestedStop) => {
                return Ok(RunTermination::RequestedStop);
            }
            Err(error) => {
                return match owner_policy
                    .finish_childless(&paths.state_path, &starting_run)
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
            owner_policy,
            &starting_run,
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
            let outcome = finish_spawned_interrupt(
                &mut child,
                &paths.state_path,
                &starting_run.identity(),
                owner_policy,
                &starting_run,
            )
            .map_err(supervisor_error_to_io)?;
            if outcome == supervisor::OwnerTerminalOutcome::RecoveryRequired {
                return emit_recovery_required(events, &starting_run.run_id);
            }
            return Ok(owner_terminal_termination(outcome));
        }

        let starting_run_id = starting_run.run_id.clone();
        let persist_outcome = owner_policy
            .attach_or_cleanup(&mut child, &paths.state_path, starting_run, server.clone())
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
                let cleanup = owner_policy
                    .cleanup_child(&mut child, &paths.state_path, &run)
                    .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &run.run_id);
                }
                return Err(io::Error::other(
                    "spawned engine process identity is unavailable",
                ));
            }
        };
        let correlation_run_id = run.run_id.clone();
        let correlation_run = run.clone();
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
                let cleanup = owner_policy
                    .cleanup_child(&mut child, &paths.state_path, &correlation_run)
                    .map_err(supervisor_error_to_io)?;
                if cleanup == supervisor::PostSpawnCleanupOutcome::RecoveryRequired {
                    return emit_recovery_required(events, &correlation_run_id);
                }
                return Err(io::Error::other(error));
            }
        };
        let state_identity = session.identity();

        if let Some(outcome) = observe_attached_stop_with_policy(
            session.child_mut(),
            &paths.state_path,
            &state_identity,
            owner_policy,
        )
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
            Ok(mut readiness_worker) => wait_for_startup_owned_with_policy(
                session.child_mut(),
                &state_identity,
                &paths.state_path,
                &signal_guard,
                readiness_worker.as_mut(),
                owner_policy,
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
            Err(error) => finish_owned_startup_failure_with_policy(
                session.child_mut(),
                &paths.state_path,
                &state_identity,
                owner_policy,
                error,
            ),
        };
        if startup.is_err() {
            let backend_kind = match backend.kind {
                RuntimeBackendKind::LlamaCpp => "llama_cpp",
                RuntimeBackendKind::PyMlxLm => "py_mlx_lm",
            };
            emit_engine_readiness_failed(
                u64::from(state_identity.generation),
                backend_kind,
                "readiness_failed",
            );
        }
        match startup {
            Ok(StartupWaitOutcome::Ready) => {
                let ready_server = session.server().clone();
                match emit_run_ready_owned_with_policy(
                    events,
                    &ready_server,
                    session.child_mut(),
                    &paths.state_path,
                    &state_identity,
                    owner_policy,
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
                match supervise_running_server_with_policy(
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
                    owner_policy,
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
                match owner_policy
                    .handle_observed_exit(
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

#[cfg(test)]
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
    serve_node_with_diagnostics_health(
        requested_model,
        port,
        engine,
        paths,
        events,
        loxa_core::diagnostics::DiagnosticsHealth::new(),
    )
}

pub fn serve_node_with_diagnostics_health(
    requested_model: Option<&str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    events: &mut dyn LifecycleEventSink,
    diagnostics_health: loxa_core::diagnostics::DiagnosticsHealth,
) -> io::Result<RunTermination> {
    tracing::info!(
        target: "loxa_node::lifecycle",
        event_code = "node.starting",
        component = "node",
        result_class = "starting",
    );
    let runtime = NodeBuilder::with_diagnostics_health(
        requested_model,
        port,
        engine,
        paths,
        diagnostics_health,
    )
    .build();
    match runtime {
        Ok(runtime) => runtime.run(events),
        Err(error) => {
            tracing::warn!(
                target: "loxa_node::lifecycle",
                event_code = "node.start_failed",
                component = "node",
                result_class = "build_failed",
            );
            Err(error)
        }
    }
}

#[cfg(test)]
fn resolve_pre_listening_cleanup(
    worker: Option<io::Result<()>>,
    owner: Option<io::Result<()>>,
) -> io::Result<()> {
    match (worker, owner) {
        (_, Some(Err(error))) => Err(error),
        (Some(Err(error)), _) => Err(error),
        _ => Ok(()),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
struct StableRuntimeCleanup<'a> {
    paths: &'a NodePaths,
    run: Option<supervisor::ManagedRun>,
    download_worker: Option<download_control::DownloadControlWorker>,
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_stable_runtime_cleanup(
    outcome: io::Result<RunTermination>,
    worker_cleanup: io::Result<()>,
    owner_cleanup: io::Result<()>,
) -> io::Result<RunTermination> {
    match (outcome, worker_cleanup, owner_cleanup) {
        (_, _, Err(error)) => Err(error),
        (_, Err(error), Ok(())) => Err(error),
        (Err(error), Ok(()), Ok(())) => Err(error),
        (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<'a> StableRuntimeCleanup<'a> {
    fn new(
        paths: &'a NodePaths,
        run: supervisor::ManagedRun,
        download_worker: download_control::DownloadControlWorker,
    ) -> Self {
        Self {
            paths,
            run: Some(run),
            download_worker: Some(download_worker),
        }
    }

    fn download_worker(&self) -> &download_control::DownloadControlWorker {
        self.download_worker
            .as_ref()
            .expect("stable runtime download worker present")
    }

    fn finish(mut self, outcome: io::Result<RunTermination>) -> io::Result<RunTermination> {
        let worker_cleanup = self
            .download_worker
            .take()
            .expect("stable runtime download worker present")
            .stop_and_join();
        let cleanup = finish_unloaded_owner(
            self.paths,
            self.run
                .as_ref()
                .expect("stable runtime exact owner present"),
        );
        self.run.take();
        resolve_stable_runtime_cleanup(outcome, worker_cleanup, cleanup)
    }
}

impl Drop for StableRuntimeCleanup<'_> {
    fn drop(&mut self) {
        let worker_cleanup = self
            .download_worker
            .take()
            .map(download_control::DownloadControlWorker::stop_and_join);
        let owner_cleanup = self
            .run
            .take()
            .map(|run| finish_unloaded_owner(self.paths, &run));
        let cleanup_succeeded =
            matches!(worker_cleanup, Some(Ok(()))) && matches!(owner_cleanup, Some(Ok(())));
        #[cfg(test)]
        if cleanup_succeeded {
            STABLE_RUNTIME_PANIC_CLEANUPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        #[cfg(not(test))]
        let _ = cleanup_succeeded;
    }
}

#[cfg(test)]
static STABLE_RUNTIME_PANIC_CLEANUPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn stable_runtime_panic_cleanup_count() -> usize {
    STABLE_RUNTIME_PANIC_CLEANUPS.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(test)]
pub(crate) fn record_stable_runtime_panic_cleanup() {
    STABLE_RUNTIME_PANIC_CLEANUPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
fn run_unloaded_actor(
    paths: &NodePaths,
    run: supervisor::ManagedRun,
    download_worker: download_control::DownloadControlWorker,
) -> io::Result<RunTermination> {
    run_stable_node_actor(paths, run, None, download_worker, None, None)
}

#[cfg_attr(not(test), allow(dead_code))]
fn run_stable_node_actor(
    paths: &NodePaths,
    run: supervisor::ManagedRun,
    download_control: Option<download_control::DownloadControl>,
    download_worker: download_control::DownloadControlWorker,
    startup_model: Option<&str>,
    events: Option<&mut dyn LifecycleEventSink>,
) -> io::Result<RunTermination> {
    let cleanup = StableRuntimeCleanup::new(paths, run.clone(), download_worker);
    let outcome = monitor_stable_node_actor(
        paths,
        &run,
        download_control.as_ref(),
        cleanup.download_worker(),
        startup_model,
        None,
        events,
    );
    cleanup.finish(outcome)
}

fn monitor_stable_node_actor(
    paths: &NodePaths,
    run: &supervisor::ManagedRun,
    download_control: Option<&download_control::DownloadControl>,
    download_worker: &download_control::DownloadControlWorker,
    startup_model: Option<&str>,
    durable_control: Option<&control_state::ControlStateHandle>,
    mut events: Option<&mut dyn LifecycleEventSink>,
) -> io::Result<RunTermination> {
    let signal_guard = SignalGuard::install()?;
    let startup = if let Some(model_id) = startup_model {
        let download_control =
            download_control.expect("startup model requires stable model control");
        match download_control.start_startup_load(model_id) {
            Err(error) => Some(Err(io::Error::other(format!(
                "startup model admission failed: {error:?}"
            )))),
            Ok(operation_id) => loop {
                if durable_control.is_some_and(|control| !control.is_healthy()) {
                    break Some(Err(io::Error::other(
                        "durable control state became unavailable",
                    )));
                }
                if download_worker.is_finished() {
                    break Some(Err(io::Error::other(
                        "model lifecycle actor worker terminated unexpectedly",
                    )));
                }
                let current = match current_same_owner_run(paths, run) {
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
    if let Some(startup) = startup {
        startup
    } else {
        loop {
            if durable_control.is_some_and(|control| !control.is_healthy()) {
                break Err(io::Error::other("durable control state became unavailable"));
            }
            if download_worker.is_finished() {
                break Err(io::Error::other(
                    "download actor worker terminated unexpectedly",
                ));
            }
            let current = match current_same_owner_run(paths, run) {
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
    supervisor::finish_exact_unloaded_owner(&paths.state_path, run)
        .map(|_| ())
        .map_err(supervisor_error_to_io)
}

#[cfg_attr(not(test), allow(dead_code))]
fn claim_unloaded_owner(
    paths: &NodePaths,
    gateway_port: u16,
) -> io::Result<supervisor::ManagedRun> {
    let candidate = unloaded_owner_candidate(paths, gateway_port)?;
    supervisor::create_unloaded_node_owner(&paths.state_path, candidate)
        .map_err(supervisor_error_to_io)
}

fn unloaded_owner_candidate(
    paths: &NodePaths,
    gateway_port: u16,
) -> io::Result<supervisor::ManagedRun> {
    let owner_pid = std::process::id();
    let owner_start = supervisor::process_start_time_with_retry(owner_pid)
        .ok_or_else(|| io::Error::other("node owner process identity is unavailable"))?;
    let now = unix_timestamp_now();
    let run_id = format!("node-{owner_pid}-{owner_start}-{now}-{gateway_port}");
    Ok(supervisor::ManagedRun {
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
    })
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
    owner_policy: RunOwnerPolicy<'_>,
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
        return finish_owned_replacement_interrupt(state_path, &run, owner_policy);
    }

    let resolved = match resolve() {
        Ok(resolved) => resolved,
        Err(error) => return finish_owned_replacement_error(state_path, &run, owner_policy, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run, owner_policy);
    }

    let detected = match detect() {
        Ok(detected) => detected,
        Err(error) => return finish_owned_replacement_error(state_path, &run, owner_policy, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run, owner_policy);
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
    owner_policy: RunOwnerPolicy<'_>,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match owner_policy.finish_childless(state_path, run)? {
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
    owner_policy: RunOwnerPolicy<'_>,
    error: SupervisorError,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match owner_policy.finish_childless(state_path, run)? {
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

#[cfg(test)]
fn observe_attached_stop<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
) -> Result<Option<supervisor::OwnerTerminalOutcome>, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    observe_attached_stop_with_policy(
        child,
        state_path,
        state_identity,
        RunOwnerPolicy::Standalone,
    )
}

fn observe_attached_stop_with_policy<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    owner_policy: RunOwnerPolicy<'_>,
) -> Result<Option<supervisor::OwnerTerminalOutcome>, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let current = match supervisor::current_runtime_state_run(state_path, state_identity) {
        Ok(current) => current,
        Err(error) => {
            return match owner_policy.cleanup_by_identity(child, state_path, state_identity)? {
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
    owner_policy
        .teardown_child(
            child,
            state_path,
            &current,
            supervisor::OwnerTeardownDecision::RequestedStop,
        )
        .map(Some)
}

fn finish_spawn_initialization<C>(
    child: &mut C,
    state_path: &Path,
    _state_identity: &supervisor::ManagedRunIdentity,
    error: Option<SupervisorError>,
    owner_policy: RunOwnerPolicy<'_>,
    current: &supervisor::ManagedRun,
) -> Result<Option<supervisor::PostSpawnCleanupOutcome>, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let Some(error) = error else {
        return Ok(None);
    };
    match owner_policy.cleanup_child(child, state_path, current)? {
        supervisor::PostSpawnCleanupOutcome::Cleaned => Err(error),
        outcome @ (supervisor::PostSpawnCleanupOutcome::RequestedStop
        | supervisor::PostSpawnCleanupOutcome::RecoveryRequired) => Ok(Some(outcome)),
    }
}

fn finish_spawned_interrupt<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    owner_policy: RunOwnerPolicy<'_>,
    current: &supervisor::ManagedRun,
) -> Result<supervisor::OwnerTerminalOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    let _ = state_identity;
    owner_policy.teardown_child(
        child,
        state_path,
        current,
        supervisor::OwnerTeardownDecision::Interrupted,
    )
}

#[cfg(test)]
fn wait_for_startup_owned<C, I, W>(
    child: &mut C,
    state_identity: &supervisor::ManagedRunIdentity,
    state_path: &Path,
    interrupt: &I,
    readiness_worker: Option<&mut supervisor::ChatCompletionReadinessWorker>,
    wait_step: W,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptSource,
    W: FnMut(&mut C, Duration, Duration) -> Result<StartupPoll, SupervisorError>,
{
    wait_for_startup_owned_with_policy(
        child,
        state_identity,
        state_path,
        interrupt,
        readiness_worker,
        RunOwnerPolicy::Standalone,
        wait_step,
    )
}

fn wait_for_startup_owned_with_policy<C, I, W>(
    child: &mut C,
    state_identity: &supervisor::ManagedRunIdentity,
    state_path: &Path,
    interrupt: &I,
    mut readiness_worker: Option<&mut supervisor::ChatCompletionReadinessWorker>,
    owner_policy: RunOwnerPolicy<'_>,
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
                return finish_owned_startup_failure_after_readiness_with_policy(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    owner_policy,
                    error,
                );
            }
        };
        if current.stop_requested {
            return finish_owned_startup_transition_after_readiness_with_policy(
                child,
                state_path,
                &current.identity(),
                supervisor::OwnerTeardownDecision::RequestedStop,
                &mut readiness_worker,
                owner_policy,
            );
        }
        if InterruptSource::interrupted(interrupt) {
            return finish_owned_startup_transition_after_readiness_with_policy(
                child,
                state_path,
                state_identity,
                supervisor::OwnerTeardownDecision::Interrupted,
                &mut readiness_worker,
                owner_policy,
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
                return finish_owned_startup_failure_after_readiness_with_policy(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    owner_policy,
                    SupervisorError::Io(error),
                );
            }
        }

        let remaining = supervisor::HEALTH_TIMEOUT.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return finish_owned_startup_failure_after_readiness_with_policy(
                child,
                state_path,
                state_identity,
                &mut readiness_worker,
                owner_policy,
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
                    return finish_owned_startup_failure_with_policy(
                        child,
                        state_path,
                        state_identity,
                        owner_policy,
                        error,
                    );
                }
                let current =
                    match supervisor::current_runtime_state_run(state_path, state_identity) {
                        Ok(current) => current,
                        Err(error) => {
                            return finish_owned_startup_failure_with_policy(
                                child,
                                state_path,
                                state_identity,
                                owner_policy,
                                error,
                            );
                        }
                    };
                if current.stop_requested {
                    return finish_owned_startup_transition_after_readiness_with_policy(
                        child,
                        state_path,
                        &current.identity(),
                        supervisor::OwnerTeardownDecision::RequestedStop,
                        &mut readiness_worker,
                        owner_policy,
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
                return finish_owned_startup_failure_after_readiness_with_policy(
                    child,
                    state_path,
                    state_identity,
                    &mut readiness_worker,
                    owner_policy,
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

fn finish_owned_startup_failure_after_readiness_with_policy<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
    owner_policy: RunOwnerPolicy<'_>,
    error: SupervisorError,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    let error = cancel_startup_readiness(worker).err().unwrap_or(error);
    finish_owned_startup_failure_with_policy(child, state_path, state_identity, owner_policy, error)
}

fn finish_owned_startup_transition_after_readiness_with_policy<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    decision: supervisor::OwnerTeardownDecision,
    worker: &mut Option<&mut supervisor::ChatCompletionReadinessWorker>,
    owner_policy: RunOwnerPolicy<'_>,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    let cancellation = cancel_startup_readiness(worker);
    let outcome = owner_policy
        .teardown_by_identity(child, state_path, state_identity, decision)
        .map_err(StartupWaitFailure::AfterTeardown)?;
    if let Err(error) = cancellation {
        return Err(StartupWaitFailure::AfterTeardown(error));
    }
    map_owned_startup_transition(outcome)
}

fn finish_owned_startup_failure_with_policy<C>(
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    owner_policy: RunOwnerPolicy<'_>,
    error: SupervisorError,
) -> Result<StartupWaitOutcome, StartupWaitFailure>
where
    C: ManagedChild + LogDrainingChild,
{
    match owner_policy
        .cleanup_by_identity(child, state_path, state_identity)
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

#[cfg(test)]
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
    supervise_running_server_with_policy(
        session,
        child,
        interrupt,
        gateway,
        process_label,
        events,
        RunOwnerPolicy::Standalone,
    )
}

fn supervise_running_server_with_policy<C, I: InterruptSource + InterruptStatus>(
    session: RunSession<'_>,
    child: &mut C,
    interrupt: &I,
    gateway: Option<&loxa_core::gateway::GatewayState>,
    process_label: &str,
    events: &mut dyn LifecycleEventSink,
    owner_policy: RunOwnerPolicy<'_>,
) -> io::Result<RunOutcome>
where
    C: ManagedChild + LogDrainingChild,
{
    loop {
        if let Some(outcome) = observe_attached_stop_with_policy(
            child,
            session.state_path,
            session.state_identity,
            owner_policy,
        )
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
            let outcome = owner_policy
                .teardown_by_identity(
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
                return match owner_policy
                    .cleanup_by_identity(child, session.state_path, session.state_identity)
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
                match owner_policy
                    .handle_observed_exit(
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

#[cfg(test)]
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
    emit_run_ready_owned_with_policy(
        events,
        server,
        child,
        state_path,
        state_identity,
        RunOwnerPolicy::Standalone,
    )
}

fn emit_run_ready_owned_with_policy<C>(
    events: &mut dyn LifecycleEventSink,
    server: &ManagedServer,
    child: &mut C,
    state_path: &Path,
    state_identity: &supervisor::ManagedRunIdentity,
    owner_policy: RunOwnerPolicy<'_>,
) -> io::Result<ReadyOutputOutcome>
where
    C: ManagedChild + LogDrainingChild,
{
    let Err(output_error) = events.emit(LifecycleEvent::ModelReady {
        server: server.clone(),
    }) else {
        return Ok(ReadyOutputOutcome::Ready);
    };
    match owner_policy
        .cleanup_by_identity(child, state_path, state_identity)
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

#[cfg(test)]
mod lifecycle_api_tests {
    use super::*;
    use crate::daemon::signals::test_support::SIGNAL_TEST_LOCK;
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    use crate::daemon::signals::test_support::{clear_ctrl_c_received, set_ctrl_c_received};
    use loxa_core::supervisor::{LogDrainingChild, ManagedChild};
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    use std::sync::atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering};

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    static MLX_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone, Default)]
    struct DiagnosticCapture(Arc<Mutex<Vec<u8>>>);

    struct DiagnosticWriter(DiagnosticCapture);

    impl Write for DiagnosticWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 .0.lock().expect("capture poisoned").extend(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for DiagnosticCapture {
        type Writer = DiagnosticWriter;

        fn make_writer(&'a self) -> Self::Writer {
            DiagnosticWriter(self.clone())
        }
    }

    impl DiagnosticCapture {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().expect("capture poisoned").clone())
                .expect("diagnostics are UTF-8")
        }
    }

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

    fn http_json(response: &str) -> serde_json::Value {
        let (_, body) = response
            .split_once("\r\n\r\n")
            .expect("HTTP response has a body separator");
        serde_json::from_str(body).expect("HTTP response body is JSON")
    }

    fn wait_for_runtime_cleanup(paths: &NodePaths, port: u16) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let listener = TcpListener::bind(("127.0.0.1", port));
            let owner_released = matches!(
                managed_servers(paths),
                Ok(ManagedRunsSnapshot::Runs(ref runs)) if runs.is_empty()
            );
            if listener.is_ok() && owner_released {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "runtime cleanup did not release listener and exact owner"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
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
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .env("LOXA_LISTENING_SINK_FAILURE_CHILD", "1")
            .args([
                "--exact",
                "lifecycle_api_tests::node_listening_sink_failure_child",
                "--nocapture",
            ])
            .output()
            .expect("run isolated lifecycle diagnostic regression");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "isolated lifecycle regression failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(stdout.contains("running 1 test"), "{stdout}");
        assert!(stdout.contains("1 passed; 0 failed"), "{stdout}");
    }

    #[test]
    fn node_listening_sink_failure_child() {
        let arguments: Vec<_> = std::env::args().collect();
        let exact_child = std::env::var_os("LOXA_LISTENING_SINK_FAILURE_CHILD").as_deref()
            == Some(std::ffi::OsStr::new("1"))
            && arguments.iter().any(|argument| argument == "--exact")
            && arguments.iter().any(|argument| {
                argument == "lifecycle_api_tests::node_listening_sink_failure_child"
            });
        if !exact_child {
            return;
        }

        let temp = TestDir::new("SECRET_MODEL_PATH-listening-sink-failure");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let mut sink = FailingFirstLifecycleSink::default();

        let capture = DiagnosticCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        let error = tracing::subscriber::with_default(subscriber, || {
            serve_node(
                None,
                Some(0),
                RuntimeBackendKind::LlamaCpp,
                &paths,
                &mut sink,
            )
            .expect_err("first event failure must escape")
        });

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let port = sink.listening_port.expect("listening payload captured");
        TcpListener::bind(("127.0.0.1", port)).expect("gateway joined and released listener");
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new()),
            "failed listening publication releases unloaded ownership"
        );
        let diagnostics = capture.text();
        let ordered = [
            "shutdown.requested",
            "withdraw_routes",
            "chat_cancel_wait",
            "gateway_join",
            "history_join",
            "node.stopped",
        ];
        let mut previous = 0;
        for expected in ordered {
            let position = diagnostics[previous..]
                .find(expected)
                .map(|position| previous + position)
                .unwrap_or_else(|| panic!("missing {expected}: {diagnostics}"));
            previous = position + expected.len();
        }
        for forbidden in [
            "SECRET_MODEL_PATH",
            "--secret-command-argument",
            "SECRET_CHILD_OUTPUT",
            "ARBITRARY_ERROR_SENTINEL",
        ] {
            assert!(!diagnostics.contains(forbidden), "{diagnostics}");
        }
    }

    #[test]
    fn dropping_unrun_runtime_eventually_releases_listener_and_exact_owner() {
        let temp = TestDir::new("drop-unrun-runtime");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let runtime = NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
            .build()
            .expect("construct production runtime graph");
        let port = runtime.port();

        drop(runtime);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let listener = TcpListener::bind(("127.0.0.1", port));
            let owner_released = matches!(
                managed_servers(&paths),
                Ok(ManagedRunsSnapshot::Runs(ref runs)) if runs.is_empty()
            );
            if listener.is_ok() && owner_released {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "abandoned runtime did not release listener and exact owner"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn panicking_stable_lifecycle_sink_stops_worker_before_releasing_runtime() {
        struct PanickingStableLifecycleSink {
            emitted: usize,
        }

        impl LifecycleEventSink for PanickingStableLifecycleSink {
            fn emit(&mut self, _: LifecycleEvent) -> io::Result<()> {
                self.emitted += 1;
                if self.emitted > 1 {
                    panic!("injected stable lifecycle sink panic");
                }
                Ok(())
            }
        }

        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        let temp = TestDir::new("panic-stable-lifecycle-sink");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        std::fs::create_dir_all(&paths.models_dir).expect("create models directory");
        let recipe = &REGISTRY[0];
        std::fs::write(paths.models_dir.join(recipe.filename), b"invalid fixture")
            .expect("write invalid startup fixture");
        let runtime = NodeBuilder::new(
            Some(recipe.id),
            Some(0),
            RuntimeBackendKind::LlamaCpp,
            &paths,
        )
        .build()
        .expect("construct production stable runtime graph");
        let port = runtime.port();
        let cleanup_before = stable_runtime_panic_cleanup_count();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = runtime.run(&mut PanickingStableLifecycleSink { emitted: 0 });
        }));

        assert!(panic.is_err(), "sink panic must cross the runtime boundary");
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let listener = TcpListener::bind(("127.0.0.1", port));
            let owner_released = matches!(
                managed_servers(&paths),
                Ok(ManagedRunsSnapshot::Runs(ref runs)) if runs.is_empty()
            );
            let cleanup_completed = stable_runtime_panic_cleanup_count() == cleanup_before + 1;
            if listener.is_ok() && owner_released && cleanup_completed {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "panic cleanup did not stop workers, release listener, and clear exact owner"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            stable_runtime_panic_cleanup_count(),
            cleanup_before + 1,
            "panic guard must clean the stable runtime exactly once"
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
    fn serve_node_facade_matches_builder_selection_errors() {
        let temp = TestDir::new("builder-selection-errors");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };

        let builder_error = NodeBuilder::new(
            Some("ARBITRARY_ERROR_SENTINEL"),
            Some(0),
            RuntimeBackendKind::LlamaCpp,
            &paths,
        )
        .build()
        .err()
        .expect("builder must reject an unknown model");
        let capture = DiagnosticCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        let facade_error = tracing::subscriber::with_default(subscriber, || {
            serve_node(
                Some("ARBITRARY_ERROR_SENTINEL"),
                Some(0),
                RuntimeBackendKind::LlamaCpp,
                &paths,
                &mut RecordingLifecycleSink::default(),
            )
            .expect_err("facade must reject an unknown model")
        });

        assert_eq!(facade_error.to_string(), builder_error.to_string());
        assert!(facade_error
            .to_string()
            .contains("ARBITRARY_ERROR_SENTINEL"));
        let diagnostics = capture.text();
        assert_eq!(diagnostics.matches("node.starting").count(), 1);
        assert_eq!(diagnostics.matches("node.start_failed").count(), 1);
        assert!(
            diagnostics.contains(" INFO loxa_node::lifecycle:"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(" WARN loxa_node::lifecycle:"),
            "{diagnostics}"
        );
        assert_eq!(diagnostics.matches("component=\"node\"").count(), 2);
        assert!(!diagnostics.contains("runtime_identity="));
        assert!(!diagnostics.contains("node_id="));
        assert!(!diagnostics.contains("node_instance_id="));
        assert!(!diagnostics.contains("ARBITRARY_ERROR_SENTINEL"));
    }

    #[cfg(unix)]
    #[test]
    fn builder_and_startup_preserve_corrupt_identity_class_with_sanitized_diagnostics() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TestDir::new("builder-corrupt-identity");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let identity_dir = temp.0.join("identity");
        fs::create_dir(&identity_dir).expect("create identity directory");
        fs::set_permissions(&identity_dir, fs::Permissions::from_mode(0o700))
            .expect("secure identity directory");
        let primary = identity_dir.join("node.json");
        fs::write(&primary, b"{not-json}\n").expect("write corrupt identity record");
        fs::set_permissions(&primary, fs::Permissions::from_mode(0o600))
            .expect("secure identity record");

        let builder_listener = TcpListener::bind(("127.0.0.1", 0)).expect("select builder port");
        let builder_port = builder_listener.local_addr().unwrap().port();
        drop(builder_listener);
        let builder_error = NodeBuilder::new(
            None,
            Some(builder_port),
            RuntimeBackendKind::LlamaCpp,
            &paths,
        )
        .build()
        .err()
        .expect("builder must reject corrupt identity");
        assert_eq!(
            builder_error.to_string(),
            "node identity failed: identity_corrupt"
        );

        let capture = DiagnosticCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        let startup_listener = TcpListener::bind(("127.0.0.1", 0)).expect("select startup port");
        let startup_port = startup_listener.local_addr().unwrap().port();
        drop(startup_listener);
        let startup_error = tracing::subscriber::with_default(subscriber, || {
            serve_node(
                None,
                Some(startup_port),
                RuntimeBackendKind::LlamaCpp,
                &paths,
                &mut RecordingLifecycleSink::default(),
            )
            .expect_err("startup must reject corrupt identity")
        });
        assert_eq!(startup_error.to_string(), builder_error.to_string());

        let diagnostic = capture.text();
        assert!(
            diagnostic.contains("node.identity_open_failed"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("trigger_class=\"identity_corrupt\""),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("cleanup_class=\"owner_cleanup_completed\""),
            "{diagnostic}"
        );
        assert!(
            !diagnostic.contains(temp.0.to_str().unwrap()),
            "{diagnostic}"
        );
        assert!(!diagnostic.contains("os error"), "{diagnostic}");
        assert_eq!(
            managed_servers(&paths).expect("inspect owner cleanup"),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
    }

    #[test]
    fn readiness_failure_event_is_warn_with_the_approved_envelope() {
        let capture = DiagnosticCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            emit_engine_readiness_failed(7, "llama_cpp", "readiness_failed");
        });

        let diagnostics = capture.text();
        assert_eq!(diagnostics.matches("engine.readiness.failed").count(), 1);
        assert!(
            diagnostics.contains(" WARN loxa_core::engine:"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("component=\"engine\""),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("generation=7"), "{diagnostics}");
        assert!(
            diagnostics.contains("backend_kind=\"llama_cpp\""),
            "{diagnostics}"
        );
    }

    #[test]
    fn stable_actor_cleanup_uncertainty_outranks_triggering_runtime_error() {
        let worker = resolve_stable_runtime_cleanup(
            Err(io::Error::other("trigger")),
            Err(io::Error::other("worker join")),
            Ok(()),
        )
        .expect_err("worker uncertainty wins");
        assert_eq!(worker.to_string(), "worker join");

        let owner = resolve_stable_runtime_cleanup(
            Err(io::Error::other("trigger")),
            Ok(()),
            Err(io::Error::other("exact owner recovery")),
        )
        .expect_err("owner uncertainty wins");
        assert_eq!(owner.to_string(), "exact owner recovery");
    }

    #[test]
    fn pre_listening_rollback_prefers_owner_uncertainty_over_worker_failure() {
        let error = resolve_pre_listening_cleanup(
            Some(Err(io::Error::other("download worker join failure"))),
            Some(Err(io::Error::other("exact owner recovery uncertainty"))),
        )
        .expect_err("owner uncertainty must win");

        assert_eq!(error.to_string(), "exact owner recovery uncertainty");
    }

    #[test]
    fn externally_missing_prepared_row_is_recovery_not_consumed_stop() {
        let temp = TestDir::new("prepared-external-missing-row");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, 19_765).expect("claim prepared owner");
        supervisor::finish_exact_unloaded_owner(&paths.state_path, &baseline)
            .expect("externally remove exact row");
        let mut events = RecordingLifecycleSink::default();

        let result = run_prepared_python_model_with_diagnostics_health(
            RunRequest {
                id: "missing-python-model",
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
            &paths,
            &baseline,
            None,
            &mut events,
            &loxa_core::diagnostics::DiagnosticsHealth::new(),
            None,
        );

        assert!(result.outcome.is_err());
        assert_eq!(
            result.owner,
            PreparedPythonOwnerDisposition::RecoveryRequired
        );
    }

    #[test]
    fn builder_failure_releases_exact_unloaded_owner() {
        let temp = TestDir::new("builder-failure-cleanup");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        std::fs::create_dir(temp.0.join("control.token"))
            .expect("block token creation with a directory");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve test port");
        let port = listener.local_addr().expect("test address").port();
        drop(listener);

        NodeBuilder::new(None, Some(port), RuntimeBackendKind::LlamaCpp, &paths)
            .build()
            .err()
            .expect("token creation must fail");

        assert_eq!(
            managed_servers(&paths).expect("inspect managed runs"),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        TcpListener::bind(("127.0.0.1", port)).expect("failed build released reserved port");
    }

    #[cfg(unix)]
    #[test]
    fn token_and_history_failures_spawn_no_download_or_model_actor() {
        use std::os::unix::fs::PermissionsExt;

        crate::bootstrap::reset_download_worker_spawn_count();
        let token_temp = TestDir::new("builder-token-before-workers");
        let token_paths = NodePaths {
            models_dir: token_temp.0.join("models"),
            state_path: token_temp.0.join("managed.json"),
            logs_dir: token_temp.0.join("logs"),
        };
        std::fs::create_dir(token_temp.0.join("control.token")).unwrap();
        assert!(
            NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &token_paths)
                .build()
                .is_err()
        );
        assert_eq!(crate::bootstrap::download_worker_spawn_count(), 0);

        let history_temp = TestDir::new("builder-history-before-workers");
        let history_dir = history_temp.0.join("history");
        std::fs::create_dir(&history_dir).unwrap();
        std::fs::set_permissions(&history_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let history_paths = NodePaths {
            models_dir: history_temp.0.join("models"),
            state_path: history_temp.0.join("run").join("managed.json"),
            logs_dir: history_temp.0.join("run").join("logs"),
        };
        assert!(
            NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &history_paths)
                .build()
                .is_err()
        );
        assert_eq!(crate::bootstrap::download_worker_spawn_count(), 0);
    }

    #[test]
    fn builder_starts_control_before_startup_model_work() {
        let temp = TestDir::new("builder-control-before-model");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        std::fs::create_dir_all(&paths.models_dir).expect("create models directory");
        let recipe = &REGISTRY[0];
        std::fs::write(paths.models_dir.join(recipe.filename), b"invalid fixture")
            .expect("write invalid startup fixture");

        let runtime = NodeBuilder::new(
            Some(recipe.id),
            Some(0),
            RuntimeBackendKind::LlamaCpp,
            &paths,
        )
        .build()
        .expect("build runtime before startup model work");
        let port = runtime.port();
        let token = loxa_core::control::auth::ControlToken::load(&temp.0.join("control.token"))
            .expect("load control token");
        let models = http_request(
            port,
            &format!(
                "GET /loxa/v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nOrigin: tauri://localhost\r\nConnection: close\r\n\r\n",
                token.expose_for_authorization()
            ),
        );

        assert!(models.starts_with("HTTP/1.1 200"), "{models}");
        assert!(models.contains(recipe.id), "{models}");
        drop(runtime);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let listener = TcpListener::bind(("127.0.0.1", port));
            let owner_released = matches!(
                managed_servers(&paths),
                Ok(ManagedRunsSnapshot::Runs(ref runs)) if runs.is_empty()
            );
            if listener.is_ok() && owner_released {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "builder runtime cleanup did not release listener and exact owner"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn node_id_is_stable_and_instance_identity_is_fresh_across_runtime_restart() {
        let temp = TestDir::new("builder-identity-restart");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        let nonce = "01".repeat(32);

        let observe = || {
            let runtime = NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
                .build()
                .expect("build unloaded runtime");
            let port = runtime.port();
            let status = http_json(&http_request(
                port,
                "GET /loxa/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
            ));
            let proof = http_json(&http_request(
                port,
                &format!(
                    "GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: {nonce}\r\nConnection: close\r\n\r\n"
                ),
            ));
            assert_eq!(status["node_id"], proof["node_id"]);
            assert_eq!(proof["protocol_version"], 1);
            assert_eq!(proof["status"], "unloaded");
            let token = loxa_core::control::auth::ControlToken::load(&temp.0.join("control.token"))
                .expect("load control token");
            assert!(token.verify_node_identity_proof(
                &nonce,
                proof["node_id"].as_str().expect("node id string"),
                proof["runtime_identity"]
                    .as_str()
                    .expect("runtime identity string"),
                loxa_core::control::contracts::NodeStatus::Unloaded,
                proof["challenge_proof"].as_str().expect("proof string"),
            ));
            let observed = (
                proof["node_id"].as_str().unwrap().to_owned(),
                proof["runtime_identity"].as_str().unwrap().to_owned(),
            );
            assert_eq!(
                runtime.shutdown_for_test().expect("shutdown runtime"),
                RunTermination::Interrupted
            );
            wait_for_runtime_cleanup(&paths, port);
            observed
        };

        let first = observe();
        let second = observe();
        assert_eq!(first.0, second.0, "durable NodeId must survive restart");
        assert_ne!(
            first.1, second.1,
            "NodeInstanceId must be fresh for each successful start"
        );
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
    fn every_api_path_rejects_a_second_owner_before_identity_mutation() {
        let temp = TestDir::new("all-path-second-owner");
        let paths = NodePaths {
            models_dir: temp.0.join("models"),
            state_path: temp.0.join("managed.json"),
            logs_dir: temp.0.join("logs"),
        };
        std::fs::create_dir_all(&paths.models_dir).unwrap();
        let recipe = &REGISTRY[0];
        std::fs::write(
            paths.models_dir.join(recipe.filename),
            b"validation fixture",
        )
        .unwrap();
        let owner = claim_unloaded_owner(&paths, 19_742).expect("claim active owner");

        for (requested, engine) in [
            (None, RuntimeBackendKind::LlamaCpp),
            (Some(recipe.id), RuntimeBackendKind::LlamaCpp),
            (
                Some("mlx-community/second-owner-fixture"),
                RuntimeBackendKind::PyMlxLm,
            ),
        ] {
            let error = NodeBuilder::new(requested, Some(0), engine, &paths)
                .build()
                .err()
                .expect("active owner must reject second runtime");
            assert!(!error.to_string().is_empty());
            assert!(!temp.0.join("identity").exists());
            assert!(!temp.0.join("control.token").exists());
            assert!(!temp.0.join("history").exists());
            let supervisor::RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&paths.state_path).unwrap()
            else {
                panic!("active owner remains loaded");
            };
            assert_eq!(runs, vec![owner.clone()]);
        }

        assert_eq!(
            supervisor::finish_childless_runtime_state_run(&paths.state_path, &owner.identity(),)
                .unwrap(),
            supervisor::ChildlessFinishOutcome::Finished
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
    fn final_owner_cleanup_preserves_a_changed_unloaded_generation() {
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
            advanced.clone(),
        )
        .unwrap()
        .expect("advance owner generation");

        finish_unloaded_owner(&paths, &original).expect_err("changed generation must fail closed");
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).unwrap(),
            supervisor::RuntimeStateRead::Loaded(vec![advanced])
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
    fn unloaded_actor_cleanup_preserves_changed_generation_and_outranks_stop() {
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
        advanced.stop_requested = true;
        supervisor::update_runtime_state_run_committed(
            &paths.state_path,
            &original.identity(),
            advanced.clone(),
        )
        .unwrap()
        .expect("advance and stop stable owner generation");
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !actor.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "actor did not fail closed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let error = actor
            .join()
            .unwrap()
            .expect_err("changed baseline cleanup must outrank requested stop");
        assert!(
            error.to_string().contains("full baseline"),
            "cleanup uncertainty must outrank monitor trigger: {error}"
        );
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).unwrap(),
            supervisor::RuntimeStateRead::Loaded(vec![advanced])
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
        let capture = DiagnosticCapture::default();
        let server_capture = capture.clone();
        let server = std::thread::spawn(move || {
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(server_capture)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                serve_node(
                    None,
                    Some(0),
                    RuntimeBackendKind::LlamaCpp,
                    &serve_paths,
                    &mut ChannelLifecycleSink(event_tx),
                )
            })
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
        assert!(status.contains("\"model\":\"loxa\""), "{status}");
        assert!(status.contains("\"engine\":null"), "{status}");
        assert!(status.contains("\"runtime_model\":null"), "{status}");
        assert!(status.contains("\"profile\":null"), "{status}");
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
        let nonce = "01".repeat(32);
        let identity_proof = http_json(&http_request(
            port,
            &format!(
                "GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: {nonce}\r\nConnection: close\r\n\r\n"
            ),
        ));
        let diagnostic_node_id = identity_proof["node_id"].as_str().unwrap().to_owned();
        let diagnostic_instance_id = identity_proof["runtime_identity"]
            .as_str()
            .unwrap()
            .to_owned();
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
        let diagnostics = capture.text();
        for code in [
            "node.starting",
            "node.listening",
            "node.stopping",
            "node.stopped",
        ] {
            assert_eq!(diagnostics.matches(code).count(), 1, "{diagnostics}");
        }
        assert_eq!(diagnostics.matches("shutdown.stage.completed").count(), 4);
        assert_eq!(diagnostics.matches("shutdown.stage.failed").count(), 0);
        for line in diagnostics
            .lines()
            .filter(|line| line.contains("shutdown.requested") || line.contains("shutdown.stage."))
        {
            assert!(
                line.contains(&format!("node_id=\"{diagnostic_node_id}\"")),
                "shutdown diagnostic omitted node id: {line}"
            );
            assert!(
                line.contains(&format!("node_instance_id=\"{diagnostic_instance_id}\"")),
                "shutdown diagnostic omitted node instance id: {line}"
            );
            assert!(!line.contains("runtime_identity="), "{line}");
        }
        assert!(
            diagnostics.contains(" INFO loxa_node::lifecycle:"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(" INFO loxa_node::shutdown:"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("component=\"node\""), "{diagnostics}");
        assert!(diagnostics.contains(&format!("node_id=\"{diagnostic_node_id}\"")));
        assert!(diagnostics.contains(&format!("node_instance_id=\"{diagnostic_instance_id}\"")));
        assert!(!diagnostics.contains("runtime_identity="));
        assert!(
            diagnostics.contains("component=\"shutdown\""),
            "{diagnostics}"
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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

        let outcome = finish_spawned_interrupt(
            &mut child,
            &state_path,
            &run.identity(),
            RunOwnerPolicy::Standalone,
            &run,
        )
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
            RunOwnerPolicy::Standalone,
            &run,
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
        let _signal_lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
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
            RunOwnerPolicy::Standalone,
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
            RunOwnerPolicy::Standalone,
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
    fn prepared_python_policy_restores_childless_owner_without_a_lease_gap() {
        let temp = TempDir::new("loxa-prepared-python-childless");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, 19_751).expect("claim unloaded owner");
        let supervisor::PrepareUnloadedOwnerOutcome::Prepared(prepared) =
            supervisor::prepare_unloaded_owner_for_model(
                &paths.state_path,
                &baseline,
                "mlx/model".to_string(),
                19_752,
                paths.logs_dir.join("engine.log"),
            )
            .expect("prepare Python owner")
        else {
            panic!("owner must prepare");
        };
        let policy = PreparedPythonOwnerPolicy::new(&baseline);

        let disposition = policy
            .finish_childless(&paths.state_path, &prepared)
            .expect("restore childless owner");

        assert_eq!(
            disposition,
            PreparedPythonOwnerDisposition::Restored(baseline.clone())
        );
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).expect("read restored owner"),
            RuntimeStateRead::Loaded(vec![baseline])
        );
    }

    #[test]
    fn prepared_python_policy_consumes_a_concurrent_stop_instead_of_restoring() {
        let temp = TempDir::new("loxa-prepared-python-stop");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, 19_753).expect("claim unloaded owner");
        let supervisor::PrepareUnloadedOwnerOutcome::Prepared(prepared) =
            supervisor::prepare_unloaded_owner_for_model(
                &paths.state_path,
                &baseline,
                "mlx/model".to_string(),
                19_754,
                paths.logs_dir.join("engine.log"),
            )
            .expect("prepare Python owner")
        else {
            panic!("owner must prepare");
        };
        let stopped = request_stop_for_test(&paths.state_path, &prepared.identity());

        let disposition = PreparedPythonOwnerPolicy::new(&baseline)
            .finish_childless(&paths.state_path, &stopped)
            .expect("consume prepared stop");

        assert_eq!(
            disposition,
            PreparedPythonOwnerDisposition::ConsumedByRequestedStop
        );
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).expect("read consumed state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn prepared_python_missing_exact_row_requires_recovery_without_signalling_child() {
        let temp = TempDir::new("loxa-prepared-python-missing-row");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, 19_755).expect("claim unloaded owner");
        let supervisor::PrepareUnloadedOwnerOutcome::Prepared(prepared) =
            supervisor::prepare_unloaded_owner_for_model(
                &paths.state_path,
                &baseline,
                "mlx/model".to_string(),
                19_756,
                paths.logs_dir.join("engine.log"),
            )
            .expect("prepare Python owner")
        else {
            panic!("owner must prepare");
        };
        assert!(
            supervisor::finish_runtime_state_run(&paths.state_path, &prepared.identity())
                .expect("remove exact row")
        );
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = RunOwnerPolicy::Prepared(PreparedPythonOwnerPolicy::new(&baseline))
            .cleanup_by_identity(&mut child, &paths.state_path, &prepared.identity())
            .expect("missing row is typed recovery");

        assert_eq!(
            outcome,
            supervisor::PostSpawnCleanupOutcome::RecoveryRequired
        );
        assert!(child.events.into_inner().is_empty());
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
