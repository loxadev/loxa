use crate::bootstrap::NodePaths;
use crate::chat_history::ChatHistoryWorker;
use crate::chat_routes::ChatRoutesState;
use crate::download_control::{DownloadControl, DownloadControlWorker};
use crate::{LifecycleEvent, LifecycleEventSink, RunRequest, RunTermination};
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::gateway::{GatewayServer, GatewayState};
use loxa_core::supervisor::ManagedRun;
use std::io;
use std::thread;

#[must_use]
pub(crate) struct NodeRuntimeParts {
    pub(crate) paths: NodePaths,
    pub(crate) startup_model: Option<String>,
    pub(crate) engine: RuntimeBackendKind,
    pub(crate) stable_llama_node: bool,
    pub(crate) unloaded_run: Option<ManagedRun>,
    pub(crate) download_runtime: Option<(DownloadControl, DownloadControlWorker)>,
    pub(crate) gateway_state: GatewayState,
    pub(crate) chat_routes_state: ChatRoutesState,
    pub(crate) gateway: GatewayServer,
    pub(crate) history_worker: ChatHistoryWorker,
}

#[must_use]
pub(crate) struct NodeRuntime {
    paths: Option<NodePaths>,
    startup_model: Option<String>,
    engine: RuntimeBackendKind,
    stable_llama_node: bool,
    unloaded_run: Option<ManagedRun>,
    download_runtime: Option<(DownloadControl, DownloadControlWorker)>,
    gateway_state: Option<GatewayState>,
    chat_routes_state: Option<ChatRoutesState>,
    gateway: Option<GatewayServer>,
    history_worker: Option<ChatHistoryWorker>,
}

impl NodeRuntime {
    pub(crate) fn new(parts: NodeRuntimeParts) -> Self {
        Self {
            paths: Some(parts.paths),
            startup_model: parts.startup_model,
            engine: parts.engine,
            stable_llama_node: parts.stable_llama_node,
            unloaded_run: parts.unloaded_run,
            download_runtime: parts.download_runtime,
            gateway_state: Some(parts.gateway_state),
            chat_routes_state: Some(parts.chat_routes_state),
            gateway: Some(parts.gateway),
            history_worker: Some(parts.history_worker),
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
            self.run_lifecycle(events)
        };

        self.shutdown_services(outcome)
    }

    fn run_lifecycle(&mut self, events: &mut dyn LifecycleEventSink) -> io::Result<RunTermination> {
        let paths = self.paths.as_ref().expect("runtime paths present");
        match self.startup_model.as_deref() {
            Some(model_id) if self.stable_llama_node => {
                let run = self
                    .unloaded_run
                    .as_ref()
                    .expect("stable llama node owner claimed")
                    .clone();
                let (download_control, download_worker) = self
                    .download_runtime
                    .take()
                    .expect("stable llama node has model control");
                let outcome = crate::run_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    Some(model_id),
                    Some(events),
                );
                self.unloaded_run.take();
                outcome
            }
            Some(model_id) => crate::run_model(
                RunRequest {
                    id: model_id,
                    ctx: None,
                    port: None,
                    engine: self.engine,
                },
                paths,
                Some(
                    self.gateway_state
                        .as_ref()
                        .expect("runtime gateway state present"),
                ),
                events,
            ),
            None => {
                let run = self
                    .unloaded_run
                    .as_ref()
                    .expect("unloaded owner claimed")
                    .clone();
                let (download_control, download_worker) = self
                    .download_runtime
                    .take()
                    .expect("unloaded node has download control");
                let outcome = crate::run_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    None,
                    Some(events),
                );
                self.unloaded_run.take();
                outcome
            }
        }
    }

    fn shutdown_services(
        &mut self,
        outcome: io::Result<RunTermination>,
    ) -> io::Result<RunTermination> {
        self.gateway_state
            .take()
            .expect("runtime gateway state present")
            .withdraw();
        self.chat_routes_state
            .take()
            .expect("runtime chat routes state present")
            .shutdown_and_wait();
        let shutdown = self
            .gateway
            .take()
            .expect("runtime gateway present")
            .shutdown();
        let history_shutdown = self
            .history_worker
            .take()
            .expect("runtime history worker present")
            .stop_and_join()
            .map_err(io::Error::other);
        match (outcome, shutdown, history_shutdown) {
            (Err(error), _, _) => Err(error),
            (Ok(_), Err(error), _) => Err(error),
            (Ok(_), Ok(()), Err(error)) => Err(error),
            (Ok(exit), Ok(()), Ok(())) => Ok(exit),
        }
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        if let Some(gateway_state) = self.gateway_state.take() {
            gateway_state.withdraw();
        }

        let paths = self.paths.take();
        let unloaded_run = self.unloaded_run.take();
        let mut download_runtime = self.download_runtime.take();
        let chat_routes_state = self.chat_routes_state.take();
        let gateway = self.gateway.take();
        let history_worker = self.history_worker.take();

        if unloaded_run.is_none()
            && download_runtime.is_none()
            && chat_routes_state.is_none()
            && gateway.is_none()
            && history_worker.is_none()
        {
            return;
        }

        let _ = thread::Builder::new()
            .name("loxa-node-runtime-cleanup".to_string())
            .spawn(move || {
                if let Some(paths) = paths.as_ref() {
                    let _ = crate::cleanup_stable_node_runtime(
                        paths,
                        unloaded_run.as_ref(),
                        &mut download_runtime,
                    );
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
            });
    }
}
