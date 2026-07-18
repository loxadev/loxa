use super::NodePaths;
use crate::runtime::{
    effective_capabilities, DurableHealthMonitor, EffectiveCapabilityInputs, NodeOwnerGuard,
    NodeRuntime, NodeRuntimeParts, PublicationGate,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityCleanupClass {
    Completed,
    OwnerCleanupFailed,
}

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
    chat_routes_state: Option<chat_routes::ChatRoutesState>,
    gateway: Option<loxa_core::gateway::GatewayServer>,
    history_worker: Option<chat_history::ChatHistoryWorker>,
    control_worker: control_state::ControlStateWorker,
) -> io::Error {
    gate.close();
    let gateway_cleanup = gateway
        .map(loxa_core::gateway::GatewayServer::shutdown)
        .unwrap_or(Ok(()));
    if let Some(chat) = chat_routes_state {
        chat.shutdown_and_wait();
    }
    let worker_cleanup = runtime
        .take()
        .map(|(_, worker)| worker.stop_and_join())
        .transpose()
        .map(|_| ());
    let history_cleanup = history_worker
        .map(chat_history::ChatHistoryWorker::stop_and_join)
        .transpose()
        .map(|_| ())
        .map_err(io::Error::other);
    let control_cleanup = control_worker
        .shutdown_blocking()
        .map_err(control_state_error_to_io);
    let owner_cleanup = owner_guard.finish().map(|_| ());
    resolve_durable_build_failure(
        trigger,
        owner_cleanup,
        history_cleanup,
        worker_cleanup,
        control_cleanup,
        gateway_cleanup,
    )
}

#[cfg(test)]
static DOWNLOAD_WORKER_SPAWN_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_download_worker_spawn_count() {
    DOWNLOAD_WORKER_SPAWN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn download_worker_spawn_count() -> usize {
    DOWNLOAD_WORKER_SPAWN_COUNT.load(std::sync::atomic::Ordering::SeqCst)
}

pub(crate) struct NodeBuilder<'a> {
    requested_model: Option<&'a str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &'a NodePaths,
    diagnostics_health: DiagnosticsHealth,
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
        }
    }

    pub(crate) fn build(self) -> io::Result<NodeRuntime> {
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
            Err(error) => return Err(error),
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
                return Err(owner_guard.finish().err().unwrap_or_else(|| {
                    io::Error::other("exact lifecycle recovery authority is unavailable")
                }))
            }
        };
        let loxa_dir = match self.paths.loxa_dir() {
            Ok(loxa_dir) => loxa_dir,
            Err(error) => return Err(owner_guard.finish().err().unwrap_or(error)),
        };
        let node_id = match crate::identity::open_or_create(loxa_dir) {
            Ok(node_id) => node_id,
            Err(error) => {
                return Err(resolve_identity_failure(error, owner_guard.finish()));
            }
        };
        let node_instance_id = NodeInstanceId::new_v4();
        let recovery_now_unix_ms = match current_unix_ms() {
            Ok(now_unix_ms) => now_unix_ms,
            Err(error) => return Err(owner_guard.finish().err().unwrap_or(error)),
        };
        let bootstrap = match control_state::ControlStateWorker::open_reconcile_and_spawn(
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
        ) {
            Ok(bootstrap) => bootstrap,
            Err(error) => return Err(control_state_error_to_io(error)),
        };
        let control_state::ControlStateBootstrap {
            handle: control,
            worker: control_worker,
            claimed_owner: mut owner_guard,
            ready_authority,
        } = bootstrap;
        let mut download_runtime = None;
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
                ))
            }
        };
        let gateway_state = loxa_core::gateway::GatewayState::new(node_id, node_instance_id);
        #[cfg(test)]
        DOWNLOAD_WORKER_SPAWN_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                    ));
                }
                download_control::DownloadControl::spawn_with_control_state(
                    self.paths.models_dir.clone(),
                    control.clone(),
                )
            }
        });
        let publication_gate = PublicationGate::with_durable_health(control.health_flag());
        let chat_routes_state =
            chat_routes::ChatRoutesState::new(token.clone(), history, gateway_state.clone())
                .with_publication_gate(publication_gate.clone());
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
                    &PublicationGate::default(),
                    owner_guard,
                    &mut download_runtime,
                    Some(chat_routes_state),
                    None,
                    Some(history_worker),
                    control_worker,
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
                ));
            }
        };
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
                ));
            }
        };
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
            ));
        }
        owner_guard.commit_acquisition();
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
                ));
            }
        };
        if !publication_gate.open() {
            let monitor_error = health_monitor
                .stop_and_join_until(std::time::Instant::now() + Duration::from_secs(10))
                .err()
                .unwrap_or_else(|| io::Error::other("publication gate could not be opened"));
            return Err(finish_failed_durable_build(
                monitor_error,
                &publication_gate,
                owner_guard,
                &mut download_runtime,
                Some(chat_routes_state),
                Some(gateway),
                Some(history_worker),
                control_worker,
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
        resolve_identity_failure,
    };
    use crate::identity::{IdentityError, IdentityErrorClass};
    use loxa_core::supervisor::ChildlessFinishOutcome;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

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
