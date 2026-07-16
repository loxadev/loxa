use crate::bootstrap::NodePaths;
use crate::chat_history::ChatHistoryWorker;
use crate::chat_routes::ChatRoutesState;
use crate::download_control::{DownloadControl, DownloadControlWorker};
use crate::{
    LifecycleEvent, LifecycleEventSink, PreparedPythonOwnerDisposition, PreparedPythonRunResult,
    RunRequest, RunTermination,
};
use loxa_core::diagnostics::DiagnosticsHealth;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::gateway::{GatewayServer, GatewayState};
use loxa_core::supervisor::ManagedRun;
use loxa_protocol::{NodeId, NodeInstanceId};
use std::io;
use std::thread;

#[must_use]
pub(crate) struct NodeOwnerGuard {
    paths: NodePaths,
    baseline: Option<ManagedRun>,
}

impl NodeOwnerGuard {
    pub(crate) fn new(paths: NodePaths, baseline: ManagedRun) -> Self {
        Self {
            paths,
            baseline: Some(baseline),
        }
    }

    pub(crate) fn into_baseline(mut self) -> ManagedRun {
        self.baseline.take().expect("node owner guard armed")
    }

    pub(crate) fn baseline(&self) -> &ManagedRun {
        self.baseline.as_ref().expect("node owner guard armed")
    }

    pub(crate) fn disarm(mut self) {
        self.baseline.take();
    }

    pub(crate) fn finish(mut self) -> io::Result<loxa_core::supervisor::ChildlessFinishOutcome> {
        let baseline = self.baseline.as_ref().expect("node owner guard armed");
        let outcome =
            loxa_core::supervisor::finish_exact_unloaded_owner(&self.paths.state_path, baseline)
                .map_err(crate::supervisor_error_to_io)?;
        self.baseline.take();
        Ok(outcome)
    }
}

impl Drop for NodeOwnerGuard {
    fn drop(&mut self) {
        let Some(baseline) = self.baseline.take() else {
            return;
        };
        let _ =
            loxa_core::supervisor::finish_exact_unloaded_owner(&self.paths.state_path, &baseline);
    }
}

fn emit_shutdown_stage(
    stage: &'static str,
    duration_ms: u64,
    node_id: NodeId,
    node_instance_id: NodeInstanceId,
    error: Option<&io::Error>,
) {
    if error.is_some() {
        tracing::warn!(
            target: "loxa_node::shutdown",
            event_code = "shutdown.stage.failed",
            component = "shutdown",
            node_id = node_id.to_string().as_str(),
            node_instance_id = node_instance_id.to_string().as_str(),
            stage,
            result_class = "join_failed",
            duration_ms,
        );
    } else {
        tracing::info!(
            target: "loxa_node::shutdown",
            event_code = "shutdown.stage.completed",
            component = "shutdown",
            node_id = node_id.to_string().as_str(),
            node_instance_id = node_instance_id.to_string().as_str(),
            stage,
            result_class = "completed",
            duration_ms,
        );
    }
}

fn resolve_shutdown_outcome(
    outcome: io::Result<RunTermination>,
    gateway_shutdown: io::Result<()>,
    history_shutdown: io::Result<()>,
) -> io::Result<RunTermination> {
    match (outcome, gateway_shutdown, history_shutdown) {
        (_, Err(error), _) => Err(error),
        (_, Ok(()), Err(error)) => Err(error),
        (Err(error), Ok(()), Ok(())) => Err(error),
        (Ok(exit), Ok(()), Ok(())) => Ok(exit),
    }
}

fn resolve_prepared_python_owner(
    result: PreparedPythonRunResult,
    guard: NodeOwnerGuard,
) -> io::Result<RunTermination> {
    match result.owner {
        PreparedPythonOwnerDisposition::Restored(_) => match guard.finish()? {
            loxa_core::supervisor::ChildlessFinishOutcome::Finished => result.outcome,
            loxa_core::supervisor::ChildlessFinishOutcome::RequestedStop => {
                Ok(RunTermination::RequestedStop)
            }
        },
        PreparedPythonOwnerDisposition::ConsumedByRequestedStop => {
            guard.disarm();
            result.outcome
        }
        PreparedPythonOwnerDisposition::RecoveryRequired => {
            guard.disarm();
            Err(io::Error::other("prepared Python owner requires recovery"))
        }
    }
}

#[must_use]
pub(crate) struct NodeRuntimeParts {
    pub(crate) paths: NodePaths,
    pub(crate) startup_model: Option<String>,
    pub(crate) engine: RuntimeBackendKind,
    pub(crate) stable_llama_node: bool,
    pub(crate) unloaded_run: Option<ManagedRun>,
    pub(crate) owner_guard: NodeOwnerGuard,
    pub(crate) download_runtime: Option<(DownloadControl, DownloadControlWorker)>,
    pub(crate) gateway_state: GatewayState,
    pub(crate) chat_routes_state: ChatRoutesState,
    pub(crate) gateway: GatewayServer,
    pub(crate) history_worker: ChatHistoryWorker,
    pub(crate) diagnostics_health: DiagnosticsHealth,
    pub(crate) node_id: NodeId,
    pub(crate) node_instance_id: NodeInstanceId,
}

#[must_use]
pub(crate) struct NodeRuntime {
    paths: Option<NodePaths>,
    startup_model: Option<String>,
    engine: RuntimeBackendKind,
    stable_llama_node: bool,
    unloaded_run: Option<ManagedRun>,
    owner_guard: Option<NodeOwnerGuard>,
    download_runtime: Option<(DownloadControl, DownloadControlWorker)>,
    gateway_state: Option<GatewayState>,
    chat_routes_state: Option<ChatRoutesState>,
    gateway: Option<GatewayServer>,
    history_worker: Option<ChatHistoryWorker>,
    diagnostics_health: DiagnosticsHealth,
    node_id: NodeId,
    node_instance_id: NodeInstanceId,
}

impl NodeRuntime {
    pub(crate) fn new(parts: NodeRuntimeParts) -> Self {
        Self {
            paths: Some(parts.paths),
            startup_model: parts.startup_model,
            engine: parts.engine,
            stable_llama_node: parts.stable_llama_node,
            unloaded_run: parts.unloaded_run,
            owner_guard: Some(parts.owner_guard),
            download_runtime: parts.download_runtime,
            gateway_state: Some(parts.gateway_state),
            chat_routes_state: Some(parts.chat_routes_state),
            gateway: Some(parts.gateway),
            history_worker: Some(parts.history_worker),
            diagnostics_health: parts.diagnostics_health,
            node_id: parts.node_id,
            node_instance_id: parts.node_instance_id,
        }
    }

    pub(crate) fn port(&self) -> u16 {
        self.gateway
            .as_ref()
            .expect("runtime gateway present")
            .port()
    }

    pub(crate) fn run(mut self, events: &mut dyn LifecycleEventSink) -> io::Result<RunTermination> {
        let outcome = if let Err(error) = events.emit(LifecycleEvent::NodeListening {
            port: self.port(),
            model_alias: "loxa".to_string(),
        }) {
            let cleanup = crate::cleanup_stable_node_runtime(
                self.paths.as_ref().expect("runtime paths present"),
                self.unloaded_run.as_ref(),
                &mut self.download_runtime,
            );
            self.unloaded_run.take();
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(cleanup),
            }
        } else {
            tracing::info!(
                target: "loxa_node::lifecycle",
                event_code = "node.listening",
                component = "node",
                node_id = self.node_id.to_string().as_str(),
                node_instance_id = self.node_instance_id.to_string().as_str(),
                result_class = "listening",
            );
            self.run_lifecycle(events)
        };

        self.shutdown_services(outcome)
    }

    fn run_lifecycle(&mut self, events: &mut dyn LifecycleEventSink) -> io::Result<RunTermination> {
        let paths = self.paths.as_ref().expect("runtime paths present");
        match self.startup_model.as_deref() {
            Some(model_id) if self.stable_llama_node => {
                self.unloaded_run.take();
                let run = self
                    .owner_guard
                    .take()
                    .expect("stable llama node owner guarded")
                    .into_baseline();
                let (download_control, download_worker) = self
                    .download_runtime
                    .take()
                    .expect("stable llama node has model control");
                crate::run_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    Some(model_id),
                    Some(events),
                )
            }
            Some(model_id) => {
                let result = crate::run_prepared_python_model_with_diagnostics_health(
                    RunRequest {
                        id: model_id,
                        ctx: None,
                        port: None,
                        engine: self.engine,
                    },
                    paths,
                    self.owner_guard
                        .as_ref()
                        .expect("prepared Python node owner guarded")
                        .baseline(),
                    Some(
                        self.gateway_state
                            .as_ref()
                            .expect("runtime gateway state present"),
                    ),
                    events,
                    &self.diagnostics_health,
                );
                resolve_prepared_python_owner(
                    result,
                    self.owner_guard
                        .take()
                        .expect("prepared Python node owner guarded"),
                )
            }
            None => {
                self.unloaded_run.take();
                let run = self
                    .owner_guard
                    .take()
                    .expect("unloaded node owner guarded")
                    .into_baseline();
                let (download_control, download_worker) = self
                    .download_runtime
                    .take()
                    .expect("unloaded node has download control");
                crate::run_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    None,
                    Some(events),
                )
            }
        }
    }

    fn shutdown_services(
        &mut self,
        outcome: io::Result<RunTermination>,
    ) -> io::Result<RunTermination> {
        let shutdown_started = std::time::Instant::now();
        tracing::info!(
            target: "loxa_node::shutdown",
            event_code = "shutdown.requested",
            component = "shutdown",
            node_id = self.node_id.to_string().as_str(),
            node_instance_id = self.node_instance_id.to_string().as_str(),
            result_class = "requested",
        );
        tracing::info!(
            target: "loxa_node::lifecycle",
            event_code = "node.stopping",
            component = "node",
            node_id = self.node_id.to_string().as_str(),
            node_instance_id = self.node_instance_id.to_string().as_str(),
            result_class = "stopping",
        );
        self.gateway_state
            .take()
            .expect("runtime gateway state present")
            .withdraw();
        emit_shutdown_stage(
            "withdraw_routes",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            None,
        );
        self.chat_routes_state
            .take()
            .expect("runtime chat routes state present")
            .shutdown_and_wait();
        emit_shutdown_stage(
            "chat_cancel_wait",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            None,
        );
        let shutdown = self
            .gateway
            .take()
            .expect("runtime gateway present")
            .shutdown();
        emit_shutdown_stage(
            "gateway_join",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            shutdown.as_ref().err(),
        );
        let history_shutdown = self
            .history_worker
            .take()
            .expect("runtime history worker present")
            .stop_and_join()
            .map_err(io::Error::other);
        emit_shutdown_stage(
            "history_join",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            history_shutdown.as_ref().err(),
        );
        let result = resolve_shutdown_outcome(outcome, shutdown, history_shutdown);
        let result_class = if result.is_ok() { "stopped" } else { "failed" };
        tracing::info!(
            target: "loxa_node::lifecycle",
            event_code = "node.stopped",
            component = "node",
            node_id = self.node_id.to_string().as_str(),
            node_instance_id = self.node_instance_id.to_string().as_str(),
            result_class,
            duration_ms = u64::try_from(shutdown_started.elapsed().as_millis())
                .unwrap_or(u64::MAX),
        );
        result
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        if let Some(gateway_state) = self.gateway_state.take() {
            gateway_state.withdraw();
        }

        let paths = self.paths.take();
        let owner_guard = self.owner_guard.take();
        let unloaded_run = self.unloaded_run.take();
        let download_runtime = self.download_runtime.take();
        let chat_routes_state = self.chat_routes_state.take();
        let gateway = self.gateway.take();
        let history_worker = self.history_worker.take();

        if unloaded_run.is_none()
            && download_runtime.is_none()
            && chat_routes_state.is_none()
            && gateway.is_none()
            && history_worker.is_none()
            && owner_guard.is_none()
        {
            return;
        }

        let _ = thread::Builder::new()
            .name("loxa-node-runtime-cleanup".to_string())
            .spawn(move || {
                let _ = (paths, unloaded_run);
                if let Some((_, download_worker)) = download_runtime {
                    let _ = download_worker.stop_and_join();
                }
                if let Some(chat_routes_state) = chat_routes_state {
                    chat_routes_state.shutdown_and_wait();
                }
                if let Some(gateway) = gateway {
                    let _ = gateway.shutdown();
                }
                if let Some(history_worker) = history_worker {
                    let _ = history_worker.stop_and_join();
                }
                if let Some(owner_guard) = owner_guard {
                    let _ = owner_guard.finish();
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        emit_shutdown_stage, resolve_prepared_python_owner, resolve_shutdown_outcome,
        NodeOwnerGuard,
    };
    use crate::NodePaths;
    use crate::{
        claim_unloaded_owner, managed_servers, ManagedRunsSnapshot, PreparedPythonOwnerDisposition,
        PreparedPythonRunResult, RunTermination,
    };
    use loxa_core::supervisor::ManagedRun;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    #[test]
    fn owner_guard_explicit_finish_removes_only_its_exact_unloaded_owner() {
        let root = std::env::temp_dir().join(format!(
            "loxa-node-owner-guard-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, 19_741).unwrap();
        NodeOwnerGuard::new(paths.clone(), baseline)
            .finish()
            .unwrap();

        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_uncertainty_outranks_the_triggering_runtime_error() {
        let result = resolve_shutdown_outcome(
            Err(io::Error::other("triggering startup error")),
            Err(io::Error::other("gateway join uncertainty")),
            Ok(()),
        )
        .expect_err("cleanup uncertainty must win");

        assert_eq!(result.to_string(), "gateway join uncertainty");
    }

    #[test]
    fn prepared_owner_result_explicitly_finishes_restored_owner() {
        let (paths, baseline) = guarded_owner("prepared-restored", 19_761);
        let result = resolve_prepared_python_owner(
            PreparedPythonRunResult {
                outcome: Ok(RunTermination::Interrupted),
                owner: PreparedPythonOwnerDisposition::Restored(baseline.clone()),
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
        )
        .expect("restored owner finishes explicitly");

        assert_eq!(result, RunTermination::Interrupted);
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        std::fs::remove_dir_all(paths.state_path.parent().unwrap()).unwrap();
    }

    #[test]
    fn late_stop_after_restored_classification_overrides_runtime_outcome() {
        let (paths, baseline) = guarded_owner("prepared-late-stop", 19_764);
        let classified = PreparedPythonRunResult {
            outcome: Ok(RunTermination::Interrupted),
            owner: PreparedPythonOwnerDisposition::Restored(baseline.clone()),
        };
        let mut stopped = baseline.clone();
        stopped.stop_requested = true;
        loxa_core::supervisor::update_runtime_state_run(
            &paths.state_path,
            &baseline.identity(),
            stopped,
        )
        .unwrap();

        let result =
            resolve_prepared_python_owner(classified, NodeOwnerGuard::new(paths.clone(), baseline))
                .expect("late stop remains observable");

        assert_eq!(result, RunTermination::RequestedStop);
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        std::fs::remove_dir_all(paths.state_path.parent().unwrap()).unwrap();
    }

    #[test]
    fn prepared_owner_result_disarms_consumed_stop_without_redeleting() {
        let (paths, baseline) = guarded_owner("prepared-stopped", 19_762);
        let mut stopped = baseline.clone();
        stopped.stop_requested = true;
        loxa_core::supervisor::update_runtime_state_run(
            &paths.state_path,
            &baseline.identity(),
            stopped,
        )
        .unwrap();
        loxa_core::supervisor::finish_exact_unloaded_owner(&paths.state_path, &baseline).unwrap();

        let result = resolve_prepared_python_owner(
            PreparedPythonRunResult {
                outcome: Ok(RunTermination::RequestedStop),
                owner: PreparedPythonOwnerDisposition::ConsumedByRequestedStop,
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
        )
        .expect("consumed stop remains terminal");

        assert_eq!(result, RunTermination::RequestedStop);
        assert_eq!(
            managed_servers(&paths).unwrap(),
            ManagedRunsSnapshot::Runs(Vec::new())
        );
        std::fs::remove_dir_all(paths.state_path.parent().unwrap()).unwrap();
    }

    #[test]
    fn prepared_owner_result_preserves_recovery_and_outranks_trigger() {
        let (paths, baseline) = guarded_owner("prepared-recovery", 19_763);
        let mut recovery = baseline.clone();
        recovery.model_id = Some("mlx/model".to_string());
        recovery.lifecycle = loxa_core::supervisor::RunLifecycle::RecoveryRequired;
        loxa_core::supervisor::update_runtime_state_run(
            &paths.state_path,
            &baseline.identity(),
            recovery.clone(),
        )
        .unwrap();

        let error = resolve_prepared_python_owner(
            PreparedPythonRunResult {
                outcome: Err(io::Error::other("triggering runtime error")),
                owner: PreparedPythonOwnerDisposition::RecoveryRequired,
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
        )
        .expect_err("recovery uncertainty must win");

        assert_eq!(error.to_string(), "prepared Python owner requires recovery");
        assert_eq!(
            loxa_core::supervisor::read_runtime_state(&paths.state_path).unwrap(),
            loxa_core::supervisor::RuntimeStateRead::Loaded(vec![recovery])
        );
        std::fs::remove_dir_all(paths.state_path.parent().unwrap()).unwrap();
    }

    fn guarded_owner(label: &str, port: u16) -> (NodePaths, ManagedRun) {
        let root = std::env::temp_dir().join(format!(
            "loxa-node-{label}-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        let baseline = claim_unloaded_owner(&paths, port).unwrap();
        (paths, baseline)
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct CaptureWriter(Capture);

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 .0.lock().expect("capture poisoned").extend(bytes);
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

    #[test]
    fn failed_shutdown_stage_is_warn_and_does_not_serialize_error_display() {
        let capture = Capture::default();
        let output = capture.0.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(capture)
            .finish();
        let error = io::Error::other("ARBITRARY_SHUTDOWN_JOIN_ERROR");
        tracing::subscriber::with_default(subscriber, || {
            emit_shutdown_stage(
                "gateway_join",
                9,
                loxa_protocol::NodeId::new_v4(),
                loxa_protocol::NodeInstanceId::new_v4(),
                Some(&error),
            );
        });

        let output = String::from_utf8(output.lock().expect("capture poisoned").clone())
            .expect("UTF-8 diagnostics");
        assert_eq!(output.matches("shutdown.stage.failed").count(), 1);
        assert!(output.contains(" WARN loxa_node::shutdown:"), "{output}");
        assert!(output.contains("component=\"shutdown\""), "{output}");
        assert!(output.contains("stage=\"gateway_join\""), "{output}");
        assert!(!output.contains("ARBITRARY_SHUTDOWN_JOIN_ERROR"));
    }
}
