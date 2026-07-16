use super::NodePaths;
use crate::runtime::{NodeOwnerGuard, NodeRuntime, NodeRuntimeParts};
use crate::{
    chat_history, chat_routes, control_router, download_control, model_lifecycle,
    production_lifecycle, requested_startup_model, supervisor_error_to_io, DEFAULT_GATEWAY_PORT,
};
use loxa_core::diagnostics::DiagnosticsHealth;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::supervisor;
use loxa_protocol::NodeInstanceId;
use std::io;

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

fn finish_failed_build(
    trigger: io::Error,
    owner_guard: NodeOwnerGuard,
    runtime: &mut Option<(
        download_control::DownloadControl,
        download_control::DownloadControlWorker,
    )>,
    history_worker: Option<chat_history::ChatHistoryWorker>,
) -> io::Error {
    let worker_cleanup = runtime
        .take()
        .map(|(_, worker)| worker.stop_and_join())
        .transpose()
        .map(|_| ())
        .map_err(io::Error::other);
    let history_cleanup = history_worker
        .map(chat_history::ChatHistoryWorker::stop_and_join)
        .transpose()
        .map(|_| ())
        .map_err(io::Error::other);
    let owner_cleanup = owner_guard.finish().map(|_| ());
    resolve_build_failure(trigger, owner_cleanup, worker_cleanup, history_cleanup)
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
        let unloaded_run = crate::claim_unloaded_owner(self.paths, gateway_port)?;
        let owner_guard = NodeOwnerGuard::new(self.paths.clone(), unloaded_run.clone());
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
        let mut download_runtime = None;
        let token_path = loxa_dir.join("control.token");
        let token = match loxa_core::control::auth::ControlToken::load_or_create(&token_path) {
            Ok(token) => token,
            Err(error) => {
                return Err(finish_failed_build(
                    error,
                    owner_guard,
                    &mut download_runtime,
                    None,
                ))
            }
        };
        let history_path = match self.paths.history_path() {
            Ok(history_path) => history_path,
            Err(error) => {
                return Err(finish_failed_build(
                    error,
                    owner_guard,
                    &mut download_runtime,
                    None,
                ))
            }
        };
        let (history, history_worker) = match chat_history::ChatHistory::spawn(history_path) {
            Ok(history) => history,
            Err(error) => {
                return Err(finish_failed_build(
                    io::Error::other(error),
                    owner_guard,
                    &mut download_runtime,
                    None,
                ))
            }
        };
        let gateway_state = loxa_core::gateway::GatewayState::new(node_id, node_instance_id);
        #[cfg(test)]
        DOWNLOAD_WORKER_SPAWN_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        download_runtime = Some({
            let run = &unloaded_run;
            if self.engine == RuntimeBackendKind::LlamaCpp {
                let owner = model_lifecycle::StableNodeOwner {
                    run_id: run.run_id.clone(),
                    pid: run.owner_pid,
                    process_start_time_unix_s: run.owner_process_start_time_unix_s,
                    gateway_port,
                };
                let lifecycle = model_lifecycle::ModelLifecycle::new(
                    owner,
                    production_lifecycle::ProductionEngineDriver::new(
                        self.paths.state_path.clone(),
                        self.paths.logs_dir.clone(),
                        gateway_port,
                    )
                    .with_diagnostics_health(self.diagnostics_health.clone()),
                    production_lifecycle::ProductionGatewayPublisher(gateway_state.clone()),
                );
                download_control::DownloadControl::spawn_with_lifecycle(
                    self.paths.models_dir.clone(),
                    lifecycle,
                )
            } else {
                download_control::DownloadControl::spawn(self.paths.models_dir.clone())
            }
        });
        let chat_routes_state =
            chat_routes::ChatRoutesState::new(token.clone(), history, gateway_state.clone());
        let router = loxa_core::gateway::router(gateway_state.clone())
            .merge(chat_routes::router(chat_routes_state.clone()));
        let gateway_router =
            router.merge(control_router::router(control_router::ControlState::new(
                token,
                node_id,
                node_instance_id,
                download_runtime
                    .as_ref()
                    .expect("unloaded node has download control")
                    .0
                    .clone(),
            )));
        let gateway_router = crate::http_observability::apply(gateway_router);
        let gateway = match loxa_core::gateway::GatewayServer::start_with_router_on(
            reservation.into_listener(),
            gateway_state.clone(),
            gateway_router,
        ) {
            Ok(gateway) => gateway,
            Err(error) => {
                return Err(finish_failed_build(
                    error,
                    owner_guard,
                    &mut download_runtime,
                    Some(history_worker),
                ));
            }
        };
        Ok(NodeRuntime::new(NodeRuntimeParts {
            paths: self.paths.clone(),
            startup_model: startup_model.map(str::to_owned),
            engine: self.engine,
            stable_llama_node,
            unloaded_run: Some(unloaded_run),
            owner_guard,
            download_runtime,
            gateway_state,
            chat_routes_state,
            gateway,
            history_worker,
            diagnostics_health: self.diagnostics_health,
            node_id,
            node_instance_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{identity_error_to_io, resolve_build_failure, resolve_identity_failure};
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
