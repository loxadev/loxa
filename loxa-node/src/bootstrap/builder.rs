use super::NodePaths;
use crate::runtime::{NodeRuntime, NodeRuntimeParts};
use crate::{
    chat_history, chat_routes, cleanup_stable_node_runtime, control_router, download_control,
    model_lifecycle, production_lifecycle, requested_startup_model, supervisor_error_to_io,
    uses_stable_node_host, DEFAULT_GATEWAY_PORT,
};
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::supervisor;
use std::io;

pub(crate) struct NodeBuilder<'a> {
    requested_model: Option<&'a str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &'a NodePaths,
}

impl<'a> NodeBuilder<'a> {
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
        }
    }

    pub(crate) fn build(self) -> io::Result<NodeRuntime> {
        let startup_model =
            requested_startup_model(&self.paths.models_dir, self.requested_model, self.engine)
                .map_err(io::Error::other)?;
        let stable_llama_node = self.engine == RuntimeBackendKind::LlamaCpp;
        let (gateway_port, unloaded_run) = if uses_stable_node_host(startup_model, self.engine) {
            let reservation =
                supervisor::reserve_localhost_port(Some(self.port.unwrap_or(DEFAULT_GATEWAY_PORT)))
                    .map_err(supervisor_error_to_io)?;
            let gateway_port = reservation.port();
            let run = crate::claim_unloaded_owner(self.paths, gateway_port)?;
            drop(reservation);
            (gateway_port, Some(run))
        } else {
            (self.port.unwrap_or(DEFAULT_GATEWAY_PORT), None)
        };
        let node_id = format!("loxa-node-{}", std::process::id());
        let gateway_state = loxa_core::gateway::GatewayState::new(node_id);
        let mut download_runtime = unloaded_run.as_ref().map(|run| {
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
                    ),
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
        let loxa_dir = match self.paths.loxa_dir() {
            Ok(loxa_dir) => loxa_dir,
            Err(error) => {
                let _ = cleanup_stable_node_runtime(
                    self.paths,
                    unloaded_run.as_ref(),
                    &mut download_runtime,
                );
                return Err(error);
            }
        };
        let token_path = loxa_dir.join("control.token");
        let token = match loxa_core::control::auth::ControlToken::load_or_create(&token_path) {
            Ok(token) => token,
            Err(error) => {
                let _ = cleanup_stable_node_runtime(
                    self.paths,
                    unloaded_run.as_ref(),
                    &mut download_runtime,
                );
                return Err(error);
            }
        };
        let history_path = match self.paths.history_path() {
            Ok(history_path) => history_path,
            Err(error) => {
                let _ = cleanup_stable_node_runtime(
                    self.paths,
                    unloaded_run.as_ref(),
                    &mut download_runtime,
                );
                return Err(error);
            }
        };
        let (history, history_worker) = match chat_history::ChatHistory::spawn(history_path) {
            Ok(history) => history,
            Err(error) => {
                let _ = cleanup_stable_node_runtime(
                    self.paths,
                    unloaded_run.as_ref(),
                    &mut download_runtime,
                );
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
        let gateway_router = crate::http_observability::apply(gateway_router);
        let gateway = match loxa_core::gateway::GatewayServer::start_with_router(
            gateway_port,
            gateway_state.clone(),
            gateway_router,
        ) {
            Ok(gateway) => gateway,
            Err(error) => {
                let _ = history_worker.stop_and_join();
                let _ = cleanup_stable_node_runtime(
                    self.paths,
                    unloaded_run.as_ref(),
                    &mut download_runtime,
                );
                return Err(error);
            }
        };
        Ok(NodeRuntime::new(NodeRuntimeParts {
            paths: self.paths.clone(),
            startup_model: startup_model.map(str::to_owned),
            engine: self.engine,
            stable_llama_node,
            unloaded_run,
            download_runtime,
            gateway_state,
            chat_routes_state,
            gateway,
            history_worker,
        }))
    }
}
