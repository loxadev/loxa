use super::NodePaths;
use crate::runtime::{
    effective_capabilities, DurableHealthMonitor, EffectiveCapabilityInputs, FatalShutdown,
    FatalShutdownParts, NodeOwnerGuard, NodeRuntime, NodeRuntimeParts, PublicationGate,
    ShutdownDeadlines, ShutdownResult,
};
use crate::{
    chat_history, chat_routes, control_router, control_state, download_control, model_lifecycle,
    production_lifecycle, requested_startup_model, supervisor_error_to_io, DEFAULT_GATEWAY_PORT,
};
use loxa_core::diagnostics::DiagnosticsHealth;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::supervisor;
use loxa_protocol::NodeInstanceId;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTROL_STARTUP_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_BUILD_OWNERSHIP_TIMEOUT: Duration = Duration::from_secs(20);

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuilderFailureBoundary {
    Control,
    History,
    Execution,
    Routes,
    Gateway,
    Health,
    Publication,
    OpenGate,
}

#[cfg(test)]
impl BuilderFailureBoundary {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::History => "history",
            Self::Execution => "execution",
            Self::Routes => "routes",
            Self::Gateway => "gateway",
            Self::Health => "health",
            Self::Publication => "publication",
            Self::OpenGate => "open_gate",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "control" => Some(Self::Control),
            "history" => Some(Self::History),
            "execution" => Some(Self::Execution),
            "routes" => Some(Self::Routes),
            "gateway" => Some(Self::Gateway),
            "health" => Some(Self::Health),
            "publication" => Some(Self::Publication),
            "open_gate" => Some(Self::OpenGate),
            _ => None,
        }
    }
}

pub(crate) enum NodeBuildError {
    Ordinary(io::Error),
    RequiresProcessExit(Box<FatalShutdown>),
}

impl NodeBuildError {
    pub(crate) fn into_shutdown_result(self) -> ShutdownResult {
        match self {
            Self::Ordinary(error) => ShutdownResult::Failed(error),
            Self::RequiresProcessExit(fatal) => ShutdownResult::RequiresProcessExit(fatal),
        }
    }
}

impl From<io::Error> for NodeBuildError {
    fn from(error: io::Error) -> Self {
        Self::Ordinary(error)
    }
}

impl std::fmt::Display for NodeBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary(error) => error.fmt(formatter),
            Self::RequiresProcessExit(_) => {
                formatter.write_str("node startup cleanup requires process exit")
            }
        }
    }
}

impl std::fmt::Debug for NodeBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

fn retained_control_startup_failure(
    failure: control_state::ControlStateStartupFailure,
) -> NodeBuildError {
    NodeBuildError::RequiresProcessExit(Box::new(FatalShutdown::new(FatalShutdownParts {
        diagnostic: "durable control startup did not release authoritative ownership".into(),
        gateway: None,
        gateway_failure: None,
        history: None,
        history_failure: None,
        health: None,
        health_failure: None,
        execution: None,
        control_failure: None,
        control_startup_failure: Some(failure),
        control: None,
        unloaded_run: None,
        publication: Some(PublicationGate::default()),
        owner: None,
        owner_failure: None,
        routes: None,
        routes_failure: None,
        gateway_state: None,
    })))
}

fn finish_early_owner_or_retain(
    trigger: io::Error,
    owner: NodeOwnerGuard,
    deadline: std::time::Instant,
) -> NodeBuildError {
    match owner.finish_retained(deadline) {
        Ok(_) => NodeBuildError::Ordinary(trigger),
        Err(owner_failure) => {
            NodeBuildError::RequiresProcessExit(Box::new(FatalShutdown::new(FatalShutdownParts {
                diagnostic: format!("{trigger}; exact startup owner cleanup was not proven"),
                gateway: None,
                gateway_failure: None,
                history: None,
                history_failure: None,
                health: None,
                health_failure: None,
                execution: None,
                control_failure: None,
                control_startup_failure: None,
                control: None,
                unloaded_run: None,
                publication: Some(PublicationGate::default()),
                owner: None,
                owner_failure: Some(owner_failure),
                routes: None,
                routes_failure: None,
                gateway_state: None,
            })))
        }
    }
}

fn finish_identity_owner_or_retain(
    trigger: crate::identity::IdentityError,
    owner: NodeOwnerGuard,
    deadline: std::time::Instant,
) -> NodeBuildError {
    let trigger_class = trigger.class().as_str();
    let trigger = identity_error_to_io(trigger);
    match owner.finish_retained(deadline) {
        Ok(_) => {
            tracing::warn!(
                target: "loxa_node::lifecycle",
                event_code = "node.identity_open_failed",
                component = "node",
                result_class = "failed",
                trigger_class,
                cleanup_class = "owner_cleanup_completed",
            );
            NodeBuildError::Ordinary(trigger)
        }
        Err(owner_failure) => {
            tracing::warn!(
                target: "loxa_node::lifecycle",
                event_code = "node.identity_open_failed",
                component = "node",
                result_class = "failed",
                trigger_class,
                cleanup_class = "owner_cleanup_failed",
            );
            NodeBuildError::RequiresProcessExit(Box::new(FatalShutdown::new(FatalShutdownParts {
                diagnostic: format!("{trigger}; exact startup owner cleanup was not proven"),
                gateway: None,
                gateway_failure: None,
                history: None,
                history_failure: None,
                health: None,
                health_failure: None,
                execution: None,
                control_failure: None,
                control_startup_failure: None,
                control: None,
                unloaded_run: None,
                publication: Some(PublicationGate::default()),
                owner: None,
                owner_failure: Some(owner_failure),
                routes: None,
                routes_failure: None,
                gateway_state: None,
            })))
        }
    }
}

fn current_unix_ms() -> io::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::other("system clock precedes the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| io::Error::other("system time exceeds supported range"))
}

fn control_state_error_to_io(_: control_state::ControlStateError) -> io::Error {
    io::Error::other("durable control state is unavailable")
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityCleanupClass {
    Completed,
    OwnerCleanupFailed,
}

#[cfg(test)]
impl IdentityCleanupClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "owner_cleanup_completed",
            Self::OwnerCleanupFailed => "owner_cleanup_failed",
        }
    }
}

fn identity_error_to_io(error: crate::identity::IdentityError) -> io::Error {
    io::Error::other(format!("node identity failed: {}", error.class().as_str()))
}

#[cfg(test)]
fn resolve_identity_failure(
    trigger: crate::identity::IdentityError,
    owner_cleanup: io::Result<loxa_core::supervisor::ChildlessFinishOutcome>,
) -> io::Error {
    let trigger_class = trigger.class().as_str();
    let (cleanup_class, result) = match owner_cleanup {
        Ok(_) => (
            IdentityCleanupClass::Completed,
            identity_error_to_io(trigger),
        ),
        Err(cleanup) => (IdentityCleanupClass::OwnerCleanupFailed, cleanup),
    };
    tracing::warn!(
        target: "loxa_node::lifecycle",
        event_code = "node.identity_open_failed",
        component = "node",
        result_class = "failed",
        trigger_class,
        cleanup_class = cleanup_class.as_str(),
    );
    result
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_build_failure(
    trigger: io::Error,
    owner_cleanup: io::Result<()>,
    worker_cleanup: io::Result<()>,
    history_cleanup: io::Result<()>,
) -> io::Error {
    owner_cleanup
        .err()
        .or_else(|| history_cleanup.err())
        .or_else(|| worker_cleanup.err())
        .unwrap_or(trigger)
}

fn resolve_durable_build_failure(
    trigger: io::Error,
    owner: io::Result<()>,
    history: io::Result<()>,
    execution: io::Result<()>,
    control: io::Result<()>,
    gateway: io::Result<()>,
) -> io::Error {
    owner
        .err()
        .or_else(|| history.err())
        .or_else(|| execution.err())
        .or_else(|| control.err())
        .or_else(|| gateway.err())
        .unwrap_or(trigger)
}

#[allow(clippy::too_many_arguments)]
fn finish_failed_durable_build(
    trigger: io::Error,
    gate: &PublicationGate,
    owner_guard: NodeOwnerGuard,
    runtime: &mut Option<(
        download_control::DownloadControl,
        download_control::DownloadControlWorker,
    )>,
    mut chat_routes_state: Option<chat_routes::ChatRoutesState>,
    mut gateway: Option<loxa_core::gateway::GatewayServer>,
    mut history_worker: Option<chat_history::ChatHistoryWorker>,
    control_worker: control_state::ControlStateWorker,
    health_monitor: Option<DurableHealthMonitor>,
    control: control_state::ControlStateHandle,
    gateway_state: loxa_core::gateway::GatewayState,
    #[cfg(test)] force_expired_cleanup: bool,
) -> NodeBuildError {
    let now = std::time::Instant::now();
    #[cfg(test)]
    let deadlines = if force_expired_cleanup {
        ShutdownDeadlines {
            admission: now,
            signal: now,
            verification: now,
            download: now,
            lifecycle: now,
            repository: now,
        }
    } else {
        ShutdownDeadlines::from_started(now)
    };
    #[cfg(not(test))]
    let deadlines = ShutdownDeadlines::from_started(now);
    gate.close();
    if let Some((download, worker)) = runtime.as_ref() {
        download.seal_for_shutdown();
        worker.request_shutdown(deadlines.lifecycle);
    }
    let mut signal_error = None;
    if let Some(routes) = chat_routes_state.as_ref() {
        if let Err(error) = routes.request_shutdown() {
            signal_error = Some(io::Error::other(format!(
                "chat routes shutdown signal failed: {error:?}"
            )));
        }
    }
    if let Some(gateway) = gateway.as_mut() {
        gateway.request_shutdown();
    }
    if let Some(history) = history_worker.as_mut() {
        history.request_shutdown();
    }
    if let Some(health) = health_monitor.as_ref() {
        health.request_shutdown();
    }
    gateway_state.withdraw();

    let mut execution_failure = None;
    let mut execution_error = Ok(());
    if let Some((download, worker)) = runtime.take() {
        match worker.shutdown_staged(
            download,
            download_control::ExecutionShutdownDeadlines {
                verification: deadlines.verification,
                download: deadlines.download,
                lifecycle: deadlines.lifecycle,
                finalize: deadlines.repository,
            },
        ) {
            download_control::ExecutionShutdownResult::Stopped => {}
            download_control::ExecutionShutdownResult::Failed(diagnostics) => {
                execution_error = Err(io::Error::other(diagnostics.messages().join("; ")));
            }
            download_control::ExecutionShutdownResult::Retained(retained) => {
                execution_failure = Some(retained);
            }
        }
    }

    let routes_failure = chat_routes_state
        .take()
        .and_then(|routes| routes.shutdown_until(deadlines.repository).err());
    let gateway_failure = gateway
        .take()
        .and_then(|gateway| gateway.shutdown_until(deadlines.repository).err());
    let health_failure =
        health_monitor.and_then(|monitor| monitor.stop_and_join_until(deadlines.repository).err());

    let mut retained_history = None;
    let history_cleanup = match history_worker.take() {
        None => Ok(()),
        Some(history) => match history.shutdown_until(deadlines.repository) {
            chat_history::ChatHistoryShutdownResult::Stopped => Ok(()),
            chat_history::ChatHistoryShutdownResult::Failed(error) => Err(io::Error::other(error)),
            chat_history::ChatHistoryShutdownResult::Retained(failure) => {
                retained_history = Some(failure);
                Ok(())
            }
        },
    };

    let control_failure = control_worker
        .shutdown_blocking_until(deadlines.repository)
        .err();
    let mut owner = None;
    let mut owner_failure = None;
    let owner_cleanup = if std::time::Instant::now() >= deadlines.repository {
        owner = Some(owner_guard);
        Ok(())
    } else {
        match owner_guard.finish_retained(deadlines.repository) {
            Ok(_) => Ok(()),
            Err(failure) => {
                owner_failure = Some(failure);
                Ok(())
            }
        }
    };

    let requires_exit = routes_failure.is_some()
        || gateway_failure.is_some()
        || health_failure.is_some()
        || execution_failure.is_some()
        || retained_history.is_some()
        || control_failure.is_some()
        || owner.is_some()
        || owner_failure.is_some()
        || signal_error.is_some();
    if requires_exit {
        return NodeBuildError::RequiresProcessExit(Box::new(FatalShutdown::new(
            FatalShutdownParts {
                diagnostic: "node startup cleanup retained authoritative ownership".into(),
                gateway: None,
                gateway_failure,
                history: None,
                history_failure: retained_history,
                health: None,
                health_failure,
                execution: execution_failure,
                control_failure,
                control_startup_failure: None,
                control: Some(control),
                unloaded_run: None,
                publication: Some(gate.clone()),
                owner,
                owner_failure,
                routes: None,
                routes_failure,
                gateway_state: Some(gateway_state),
            },
        )));
    }

    NodeBuildError::Ordinary(resolve_durable_build_failure(
        trigger,
        owner_cleanup,
        history_cleanup,
        execution_error,
        signal_error.map_or(Ok(()), Err),
        Ok(()),
    ))
}

pub(crate) struct NodeBuilder<'a> {
    requested_model: Option<&'a str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &'a NodePaths,
    diagnostics_health: DiagnosticsHealth,
    #[cfg(test)]
    download_worker_spawn_count: Option<&'a std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    failure_after: Option<BuilderFailureBoundary>,
    #[cfg(test)]
    force_expired_failure_cleanup: bool,
}

impl<'a> NodeBuilder<'a> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(
        requested_model: Option<&'a str>,
        port: Option<u16>,
        engine: RuntimeBackendKind,
        paths: &'a NodePaths,
    ) -> Self {
        Self {
            requested_model,
            port,
            engine,
            paths,
            diagnostics_health: DiagnosticsHealth::new(),
            #[cfg(test)]
            download_worker_spawn_count: None,
            #[cfg(test)]
            failure_after: None,
            #[cfg(test)]
            force_expired_failure_cleanup: false,
        }
    }

    pub(crate) fn with_diagnostics_health(
        requested_model: Option<&'a str>,
        port: Option<u16>,
        engine: RuntimeBackendKind,
        paths: &'a NodePaths,
        diagnostics_health: DiagnosticsHealth,
    ) -> Self {
        Self {
            requested_model,
            port,
            engine,
            paths,
            diagnostics_health,
            #[cfg(test)]
            download_worker_spawn_count: None,
            #[cfg(test)]
            failure_after: None,
            #[cfg(test)]
            force_expired_failure_cleanup: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_download_worker_spawn_count(
        mut self,
        count: &'a std::sync::atomic::AtomicUsize,
    ) -> Self {
        self.download_worker_spawn_count = Some(count);
        self
    }

    #[cfg(test)]
    fn with_failure_after_for_test(
        mut self,
        boundary: BuilderFailureBoundary,
        force_expired_cleanup: bool,
    ) -> Self {
        self.failure_after = Some(boundary);
        self.force_expired_failure_cleanup = force_expired_cleanup;
        self
    }

    pub(crate) fn build(self) -> Result<NodeRuntime, NodeBuildError> {
        let build_started = std::time::Instant::now();
        let ownership_deadline = build_started + NODE_BUILD_OWNERSHIP_TIMEOUT;
        let startup_model =
            requested_startup_model(&self.paths.models_dir, self.requested_model, self.engine)
                .map_err(io::Error::other)?;
        let stable_llama_node = self.engine == RuntimeBackendKind::LlamaCpp;
        let reservation =
            supervisor::reserve_localhost_port(Some(self.port.unwrap_or(DEFAULT_GATEWAY_PORT)))
                .map_err(supervisor_error_to_io)?;
        let gateway_port = reservation.port();
        let control_path = self.paths.control_state_path()?;
        let capture_mode = match std::fs::symlink_metadata(control_path.as_ref()) {
            Ok(_) => supervisor::ScalarCaptureMode::ExistingDatabase,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                supervisor::ScalarCaptureMode::FirstMigration
            }
            Err(error) => return Err(error.into()),
        };
        let candidate = crate::unloaded_owner_candidate(self.paths, gateway_port)?;
        let acquisition =
            supervisor::acquire_managed_owner(&self.paths.state_path, candidate, capture_mode)
                .map_err(supervisor_error_to_io)?;
        let (owner_guard, managed_scalar_source) =
            NodeOwnerGuard::from_acquisition(self.paths.clone(), acquisition);
        let first_migration_source =
            control_state::ScalarSource::from_managed(managed_scalar_source);
        let recovery_evidence = match control_state::acquisition_recovery_evidence(
            &owner_guard,
            first_migration_source.as_ref(),
        ) {
            Ok(evidence) => evidence,
            Err(_) => {
                return Err(finish_early_owner_or_retain(
                    io::Error::other("exact lifecycle recovery authority is unavailable"),
                    owner_guard,
                    ownership_deadline,
                ))
            }
        };
        let loxa_dir = match self.paths.loxa_dir() {
            Ok(loxa_dir) => loxa_dir,
            Err(error) => {
                return Err(finish_early_owner_or_retain(
                    error,
                    owner_guard,
                    ownership_deadline,
                ))
            }
        };
        let node_id = match crate::identity::open_or_create(loxa_dir) {
            Ok(node_id) => node_id,
            Err(error) => {
                return Err(finish_identity_owner_or_retain(
                    error,
                    owner_guard,
                    ownership_deadline,
                ));
            }
        };
        let node_instance_id = NodeInstanceId::new_v4();
        let recovery_now_unix_ms = match current_unix_ms() {
            Ok(now_unix_ms) => now_unix_ms,
            Err(error) => {
                return Err(finish_early_owner_or_retain(
                    error,
                    owner_guard,
                    ownership_deadline,
                ))
            }
        };
        let bootstrap = match control_state::ControlStateWorker::open_reconcile_and_spawn_until(
            control_state::ControlStateInit {
                path: control_path,
                node_id,
                open_input: control_state::ControlStateOpenInput {
                    claimed_owner: owner_guard,
                    first_migration_source,
                },
                recovery_evidence,
                now_unix_ms: recovery_now_unix_ms,
            },
            ownership_deadline,
        ) {
            Ok(bootstrap) => bootstrap,
            Err(failure) => return Err(retained_control_startup_failure(failure)),
        };
        let control_state::ControlStateBootstrap {
            handle: control,
            worker: control_worker,
            claimed_owner: mut owner_guard,
            ready_authority,
        } = bootstrap;
        let mut download_runtime = None;
        let gateway_state = loxa_core::gateway::GatewayState::new(node_id, node_instance_id);
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Control) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after control construction"),
                &PublicationGate::default(),
                owner_guard,
                &mut download_runtime,
                None,
                None,
                None,
                control_worker,
                None,
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        let token_path = loxa_dir.join("control.token");
        let token = match loxa_core::control::auth::ControlToken::load_or_create(&token_path) {
            Ok(token) => token,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    error,
                    &PublicationGate::default(),
                    owner_guard,
                    &mut download_runtime,
                    None,
                    None,
                    None,
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ))
            }
        };
        let history_path = match self.paths.history_path() {
            Ok(history_path) => history_path,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    error,
                    &PublicationGate::default(),
                    owner_guard,
                    &mut download_runtime,
                    None,
                    None,
                    None,
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ))
            }
        };
        let (history, history_worker) = match chat_history::ChatHistory::spawn(history_path) {
            Ok(history) => history,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    io::Error::other(error),
                    &PublicationGate::default(),
                    owner_guard,
                    &mut download_runtime,
                    None,
                    None,
                    None,
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ))
            }
        };
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::History) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after history construction"),
                &PublicationGate::default(),
                owner_guard,
                &mut download_runtime,
                None,
                None,
                Some(history_worker),
                control_worker,
                None,
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        download_runtime = Some({
            let run = owner_guard.baseline();
            if self.engine == RuntimeBackendKind::LlamaCpp {
                let owner = model_lifecycle::StableNodeOwner {
                    run_id: run.run_id.clone(),
                    pid: run.owner_pid,
                    process_start_time_unix_s: run.owner_process_start_time_unix_s,
                    gateway_port,
                };
                let lifecycle = match ready_authority {
                    Some(authority) => authority.into_owned_session().into_lifecycle(),
                    None => model_lifecycle::ModelLifecycle::new(
                        owner,
                        production_lifecycle::ProductionEngineDriver::new(
                            self.paths.state_path.clone(),
                            self.paths.logs_dir.clone(),
                            gateway_port,
                        )
                        .with_diagnostics_health(self.diagnostics_health.clone()),
                        production_lifecycle::ProductionGatewayPublisher(gateway_state.clone()),
                    ),
                };
                #[cfg(test)]
                if let Some(count) = self.download_worker_spawn_count {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                download_control::DownloadControl::spawn_with_lifecycle_and_control_state(
                    self.paths.models_dir.clone(),
                    lifecycle,
                    control.clone(),
                )
            } else {
                if ready_authority.is_some() {
                    return Err(finish_failed_durable_build(
                        io::Error::other("recovered slot authority requires llama-cpp lifecycle"),
                        &PublicationGate::default(),
                        owner_guard,
                        &mut download_runtime,
                        None,
                        None,
                        Some(history_worker),
                        control_worker,
                        None,
                        control.clone(),
                        gateway_state.clone(),
                        #[cfg(test)]
                        self.force_expired_failure_cleanup,
                    ));
                }
                #[cfg(test)]
                if let Some(count) = self.download_worker_spawn_count {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                download_control::DownloadControl::spawn_with_control_state(
                    self.paths.models_dir.clone(),
                    control.clone(),
                )
            }
        });
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Execution) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after execution construction"),
                &PublicationGate::default(),
                owner_guard,
                &mut download_runtime,
                None,
                None,
                Some(history_worker),
                control_worker,
                None,
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        let publication_gate = PublicationGate::with_durable_health(control.health_flag());
        let chat_routes_state =
            chat_routes::ChatRoutesState::new(token.clone(), history, gateway_state.clone())
                .with_publication_gate(publication_gate.clone());
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Routes) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after routes construction"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                None,
                Some(history_worker),
                control_worker,
                None,
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        let router = publication_gate
            .protect(loxa_core::gateway::router(gateway_state.clone()))
            .merge(chat_routes::router(chat_routes_state.clone()));
        let control_routes = match control_router::router_with_optional_v2(
            control_router::ControlState::new(
                token,
                node_id,
                node_instance_id,
                download_runtime
                    .as_ref()
                    .expect("unloaded node has download control")
                    .0
                    .clone(),
            )
            .with_publication_gate(publication_gate.clone()),
            Some(control.clone()),
        ) {
            Ok(routes) => routes,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    io::Error::other(error),
                    &publication_gate,
                    owner_guard,
                    &mut download_runtime,
                    Some(chat_routes_state),
                    None,
                    Some(history_worker),
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ));
            }
        };
        let gateway_router = router.merge(control_routes);
        let gateway_router = crate::http_observability::apply(gateway_router);
        let gateway = match loxa_core::gateway::GatewayServer::start_with_router_on(
            reservation.into_listener(),
            gateway_state.clone(),
            gateway_router,
        ) {
            Ok(gateway) => gateway,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    error,
                    &publication_gate,
                    owner_guard,
                    &mut download_runtime,
                    Some(chat_routes_state),
                    None,
                    Some(history_worker),
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ));
            }
        };
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Gateway) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after gateway construction"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                None,
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        let writer_healthy = control.writer_is_healthy();
        let lifecycle_supported = stable_llama_node;
        let capabilities = effective_capabilities(EffectiveCapabilityInputs {
            downloader_owner: writer_healthy,
            slot_load_support: lifecycle_supported && writer_healthy,
            slot_unload_support: lifecycle_supported && writer_healthy,
            cancellation_authority: writer_healthy,
            durable_writer_healthy: writer_healthy,
            subscription_healthy: control.subscription_is_healthy(),
        });
        let publication_now_unix_ms = match current_unix_ms() {
            Ok(now_unix_ms) => now_unix_ms,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    error,
                    &publication_gate,
                    owner_guard,
                    &mut download_runtime,
                    Some(chat_routes_state),
                    Some(gateway),
                    Some(history_worker),
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ));
            }
        };
        let health_monitor = match DurableHealthMonitor::spawn(
            control.clone(),
            publication_gate.clone(),
            gateway_state.clone(),
            self.paths.state_path.clone(),
            owner_guard.baseline().identity(),
        ) {
            Ok(monitor) => monitor,
            Err(error) => {
                return Err(finish_failed_durable_build(
                    error,
                    &publication_gate,
                    owner_guard,
                    &mut download_runtime,
                    Some(chat_routes_state),
                    Some(gateway),
                    Some(history_worker),
                    control_worker,
                    None,
                    control.clone(),
                    gateway_state.clone(),
                    #[cfg(test)]
                    self.force_expired_failure_cleanup,
                ));
            }
        };
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Health) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after health construction"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                Some(health_monitor),
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        if let Err(error) = control.publish_instance_blocking_until(
            control_state::InstancePublication {
                node_instance_id,
                control_endpoint: format!("http://127.0.0.1:{}", gateway.port()),
                capabilities,
                now_unix_ms: publication_now_unix_ms,
            },
            std::time::Instant::now() + CONTROL_STARTUP_COMMIT_TIMEOUT,
        ) {
            return Err(finish_failed_durable_build(
                control_state_error_to_io(error),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                Some(health_monitor),
                control.clone(),
                gateway_state.clone(),
                #[cfg(test)]
                self.force_expired_failure_cleanup,
            ));
        }
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::Publication) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after durable publication"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                Some(health_monitor),
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        owner_guard.commit_acquisition();
        if !publication_gate.open() {
            return Err(finish_failed_durable_build(
                io::Error::other("publication gate could not be opened"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                Some(health_monitor),
                control.clone(),
                gateway_state.clone(),
                #[cfg(test)]
                self.force_expired_failure_cleanup,
            ));
        }
        #[cfg(test)]
        if self.failure_after == Some(BuilderFailureBoundary::OpenGate) {
            return Err(finish_failed_durable_build(
                io::Error::other("injected failure after publication gate opened"),
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
                Some(health_monitor),
                control.clone(),
                gateway_state.clone(),
                self.force_expired_failure_cleanup,
            ));
        }
        Ok(NodeRuntime::new(NodeRuntimeParts {
            paths: self.paths.clone(),
            startup_model: startup_model.map(str::to_owned),
            engine: self.engine,
            stable_llama_node,
            unloaded_run: Some(owner_guard.baseline().clone()),
            owner_guard,
            download_runtime,
            gateway_state,
            chat_routes_state,
            gateway,
            history_worker,
            diagnostics_health: self.diagnostics_health,
            node_id,
            node_instance_id,
            publication_gate,
            control,
            control_worker,
            health_monitor,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        identity_error_to_io, resolve_build_failure, resolve_durable_build_failure,
        resolve_identity_failure, BuilderFailureBoundary, NodeBuildError, NodeBuilder,
    };
    use crate::identity::{IdentityError, IdentityErrorClass};
    use crate::NodePaths;
    use loxa_core::engine::RuntimeBackendKind;
    use loxa_core::supervisor::ChildlessFinishOutcome;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    struct BuilderTestDir(std::path::PathBuf);

    impl BuilderTestDir {
        fn new(label: &str) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "loxa-builder-fault-{label}-{}-{}-{}",
                std::process::id(),
                super::current_unix_ms().expect("clock"),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
            std::fs::create_dir_all(&path).expect("create builder test directory");
            Self(path)
        }

        fn paths(&self) -> NodePaths {
            NodePaths {
                models_dir: self.0.join("models"),
                state_path: self.0.join("managed.json"),
                logs_dir: self.0.join("logs"),
            }
        }
    }

    impl Drop for BuilderTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct CaptureWriter(Capture);

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 .0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(self.clone())
        }
    }

    impl Capture {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    #[test]
    fn every_constructed_owner_and_publication_boundary_uses_retaining_cleanup() {
        const CHILD_ENV: &str = "LOXA_BUILDER_RETAINED_BOUNDARY";
        const CHILD_ROOT_ENV: &str = "LOXA_BUILDER_RETAINED_ROOT";
        const CHILD_PORT_ENV: &str = "LOXA_BUILDER_RETAINED_PORT";
        const RETAINED_EXIT_CODE: i32 = 73;
        if let Ok(label) = std::env::var(CHILD_ENV) {
            let boundary = BuilderFailureBoundary::from_str(&label)
                .unwrap_or_else(|| panic!("unknown injected builder boundary: {label}"));
            let root = std::path::PathBuf::from(
                std::env::var_os(CHILD_ROOT_ENV).expect("retained child root"),
            );
            let port = std::env::var(CHILD_PORT_ENV)
                .expect("retained child port")
                .parse::<u16>()
                .expect("valid retained child port");
            let paths = NodePaths {
                models_dir: root.join("models"),
                state_path: root.join("managed.json"),
                logs_dir: root.join("logs"),
            };
            let error =
                match NodeBuilder::new(None, Some(port), RuntimeBackendKind::LlamaCpp, &paths)
                    .with_failure_after_for_test(boundary, true)
                    .build()
                {
                    Ok(_) => panic!("injected builder boundary must fail"),
                    Err(error) => error,
                };
            match error {
                NodeBuildError::RequiresProcessExit(fatal) => {
                    assert!(
                        fatal
                            .diagnostic_for_test()
                            .contains("retained authoritative ownership"),
                        "{}: {}",
                        boundary.as_str(),
                        fatal.diagnostic_for_test()
                    );
                    fatal.exit(RETAINED_EXIT_CODE);
                }
                NodeBuildError::Ordinary(error) => panic!(
                    "{} released all real owners despite an expired cleanup deadline: {error}",
                    boundary.as_str()
                ),
            }
        }

        let cases = [
            BuilderFailureBoundary::Control,
            BuilderFailureBoundary::History,
            BuilderFailureBoundary::Execution,
            BuilderFailureBoundary::Routes,
            BuilderFailureBoundary::Gateway,
            BuilderFailureBoundary::Health,
            BuilderFailureBoundary::Publication,
            BuilderFailureBoundary::OpenGate,
        ];

        for boundary in cases {
            let temp = BuilderTestDir::new(boundary.as_str());
            let listener =
                std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve retained child port");
            let port = listener.local_addr().expect("reserved address").port();
            drop(listener);
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("test binary"),
            )
                .arg("--exact")
                .arg("bootstrap::builder::tests::every_constructed_owner_and_publication_boundary_uses_retaining_cleanup")
                .arg("--nocapture")
                .env(CHILD_ENV, boundary.as_str())
                .env(CHILD_ROOT_ENV, &temp.0)
                .env(CHILD_PORT_ENV, port.to_string())
                .spawn()
                .expect("run retained builder boundary child");
            let child_pid = child.id();
            let status = child.wait().expect("wait for retained builder child");
            assert_eq!(
                status.code(),
                Some(RETAINED_EXIT_CODE),
                "{} child {child_pid} did not take the required nonzero exit",
                boundary.as_str(),
            );
            let rebound =
                std::net::TcpListener::bind(("127.0.0.1", port)).unwrap_or_else(|error| {
                    panic!(
                        "{} child {child_pid} left gateway port {port} orphaned: {error}",
                        boundary.as_str()
                    )
                });
            drop(rebound);
        }
    }

    #[test]
    fn every_constructed_owner_and_publication_boundary_releases_on_completed_cleanup() {
        let cases = [
            BuilderFailureBoundary::Control,
            BuilderFailureBoundary::History,
            BuilderFailureBoundary::Execution,
            BuilderFailureBoundary::Routes,
            BuilderFailureBoundary::Gateway,
            BuilderFailureBoundary::Health,
            BuilderFailureBoundary::Publication,
            BuilderFailureBoundary::OpenGate,
        ];

        for boundary in cases {
            let temp = BuilderTestDir::new(boundary.as_str());
            let paths = temp.paths();
            let error = match NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
                .with_failure_after_for_test(boundary, false)
                .build()
            {
                Ok(_) => panic!("injected builder boundary must fail"),
                Err(error) => error,
            };
            match error {
                NodeBuildError::Ordinary(_) => {}
                NodeBuildError::RequiresProcessExit(fatal) => {
                    let diagnostic = fatal.diagnostic_for_test().to_owned();
                    std::mem::forget(fatal);
                    panic!(
                        "{} retained ownership despite completed cleanup: {diagnostic}",
                        boundary.as_str()
                    );
                }
            }
        }
    }

    #[test]
    fn history_join_failure_outranks_gateway_bind_failure() {
        let error = resolve_build_failure(
            io::Error::other("gateway bind failure"),
            Ok(()),
            Ok(()),
            Err(io::Error::other("history join failure")),
        );

        assert_eq!(error.to_string(), "history join failure");
    }

    #[test]
    fn durable_build_failure_uses_authority_precedence_not_cleanup_recency() {
        let error = |message| Err(io::Error::other(message));
        assert_eq!(
            resolve_durable_build_failure(
                io::Error::other("trigger"),
                error("owner"),
                error("history"),
                error("execution"),
                error("control"),
                error("gateway"),
            )
            .to_string(),
            "owner"
        );
        assert_eq!(
            resolve_durable_build_failure(
                io::Error::other("trigger"),
                Ok(()),
                error("history"),
                error("execution"),
                error("control"),
                error("gateway"),
            )
            .to_string(),
            "history"
        );
        assert_eq!(
            resolve_durable_build_failure(
                io::Error::other("trigger"),
                Ok(()),
                Ok(()),
                error("execution"),
                error("control"),
                error("gateway"),
            )
            .to_string(),
            "execution"
        );
    }

    #[test]
    fn identity_error_projection_preserves_stable_classes_without_source_details() {
        let cases = [
            (
                IdentityErrorClass::UnsupportedPlatform,
                "unsupported_platform",
            ),
            (IdentityErrorClass::Corrupt, "identity_corrupt"),
            (IdentityErrorClass::UnsafeRoot, "unsafe_root"),
            (IdentityErrorClass::UnsafeDirectory, "unsafe_directory"),
            (IdentityErrorClass::UnsafeRecord, "unsafe_record"),
            (
                IdentityErrorClass::SchemaUnsupported,
                "identity_schema_unsupported",
            ),
            (IdentityErrorClass::Conflict, "identity_conflict"),
            (
                IdentityErrorClass::ConcurrentChange,
                "identity_concurrent_change",
            ),
            (IdentityErrorClass::Io, "identity_io"),
            (IdentityErrorClass::Durability, "identity_durability"),
        ];

        for (class, expected) in cases {
            let projected = identity_error_to_io(IdentityError::classified(class));
            assert_eq!(
                projected.to_string(),
                format!("node identity failed: {expected}")
            );
            assert!(!projected.to_string().contains('/'));
            assert!(!projected.to_string().contains("os error"));
            assert!(!projected.to_string().contains("injected"));
        }
    }

    #[test]
    fn identity_cleanup_failure_outranks_trigger_without_collapsing_either_class() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        let error = tracing::subscriber::with_default(subscriber, || {
            resolve_identity_failure(
                IdentityError::classified(IdentityErrorClass::Corrupt),
                Err(io::Error::other(
                    "owner recovery required: ARBITRARY /private/path os error 13",
                )),
            )
        });
        assert_eq!(
            error.to_string(),
            "owner recovery required: ARBITRARY /private/path os error 13"
        );
        let diagnostic = capture.text();
        assert!(
            diagnostic.contains("event_code=\"node.identity_open_failed\""),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("trigger_class=\"identity_corrupt\""),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("cleanup_class=\"owner_cleanup_failed\""),
            "{diagnostic}"
        );
        assert!(!diagnostic.contains("ARBITRARY"), "{diagnostic}");
        assert!(!diagnostic.contains("/private/path"), "{diagnostic}");
        assert!(!diagnostic.contains("os error"), "{diagnostic}");

        let trigger = resolve_identity_failure(
            IdentityError::classified(IdentityErrorClass::Durability),
            Ok(ChildlessFinishOutcome::Finished),
        );
        assert_eq!(
            trigger.to_string(),
            "node identity failed: identity_durability"
        );
    }
}
