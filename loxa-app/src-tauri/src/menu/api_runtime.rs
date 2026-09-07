use std::fmt;

use loxa::api_runtime::ApiRuntimeActivity;
use loxa::paths::AppPaths;
use loxa_ipc::{OperationTarget, ServiceClient};

mod legacy;
#[cfg_attr(test, allow(dead_code))]
mod service;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiEndpoint {
    pub(super) model_id: String,
    port: u16,
    service_target: Option<OperationTarget>,
}

impl ApiEndpoint {
    pub(super) fn legacy(model_id: String, port: u16) -> Self {
        Self {
            model_id,
            port,
            service_target: None,
        }
    }

    #[cfg(test)]
    pub(super) fn new(model_id: String, port: u16) -> Self {
        Self::legacy(model_id, port)
    }

    pub(super) fn service(model_id: String, target: OperationTarget) -> Self {
        Self {
            model_id,
            port: 0,
            service_target: Some(target),
        }
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub(super) fn service_target(&self) -> Option<&OperationTarget> {
        self.service_target.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ApiRuntimePhase {
    Idle,
    Starting {
        generation: u64,
        model_id: String,
    },
    Ready {
        generation: u64,
        endpoint: ApiEndpoint,
        activity: ApiRuntimeActivity,
    },
    Stopping,
    CleanupFailed,
    ControllerFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiRuntimeNotice {
    ServiceAbsent,
    Conflict,
    ModelUnavailable,
    StartFailed,
    UnexpectedStop,
    CleanupFailed,
    ControllerFailed,
}

impl ApiRuntimeNotice {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::ServiceAbsent => "Background service stopped",
            Self::Conflict => "Another Loxa model operation is active",
            Self::ModelUnavailable => "The selected installed model is unavailable",
            Self::StartFailed => "Could not start the API",
            Self::UnexpectedStop => "The API stopped unexpectedly",
            Self::CleanupFailed => "Could not stop the API. Try Stop API again.",
            Self::ControllerFailed => "The API controller is unavailable. Quit and reopen Loxa.",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiRuntimeKind {
    Legacy,
    Service,
}

pub(crate) struct ApiRuntimeView<'a> {
    kind: ApiRuntimeKind,
    initialized: bool,
    phase: &'a ApiRuntimePhase,
    notice: Option<ApiRuntimeNotice>,
    active_model_id: Option<&'a str>,
}

impl<'a> ApiRuntimeView<'a> {
    pub(crate) fn kind(&self) -> ApiRuntimeKind {
        self.kind
    }
    pub(crate) fn initialized(&self) -> bool {
        self.initialized
    }
    pub(crate) fn phase(&self) -> &'a ApiRuntimePhase {
        self.phase
    }
    pub(crate) fn notice(&self) -> Option<ApiRuntimeNotice> {
        self.notice
    }
    pub(crate) fn active_model_id(&self) -> Option<&'a str> {
        self.active_model_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ApiRuntimeShutdownError;
impl fmt::Display for ApiRuntimeShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the API runtime could not be shut down")
    }
}
impl std::error::Error for ApiRuntimeShutdownError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum StartOutcome {
    Ready(ApiEndpoint),
    Conflict,
    Cancelled,
    ModelUnavailable,
    StartupFailed,
    CleanupFailed(ApiEndpoint),
}

pub(super) struct RuntimeState {
    phase: ApiRuntimePhase,
    notice: Option<ApiRuntimeNotice>,
    active_model_id: Option<String>,
}

impl RuntimeState {
    fn available() -> Self {
        Self {
            phase: ApiRuntimePhase::Idle,
            notice: None,
            active_model_id: None,
        }
    }
    fn failed() -> Self {
        Self {
            phase: ApiRuntimePhase::ControllerFailed,
            notice: Some(ApiRuntimeNotice::ControllerFailed),
            active_model_id: None,
        }
    }
    fn view(&self, kind: ApiRuntimeKind, initialized: bool) -> ApiRuntimeView<'_> {
        ApiRuntimeView {
            kind,
            initialized,
            phase: &self.phase,
            notice: self.notice,
            active_model_id: self.active_model_id.as_deref(),
        }
    }
    fn clear(&mut self, notice: Option<ApiRuntimeNotice>) {
        self.phase = ApiRuntimePhase::Idle;
        self.notice = notice;
        self.active_model_id = None;
    }
    fn fail(&mut self) {
        self.phase = ApiRuntimePhase::ControllerFailed;
        self.notice = Some(ApiRuntimeNotice::ControllerFailed);
    }
}

enum RuntimeBackend {
    Legacy(legacy::LegacyBackend),
    Service(service::ServiceBackend),
}

pub(crate) struct ApiRuntimeController {
    backend: RuntimeBackend,
}

impl ApiRuntimeController {
    pub(crate) fn start(paths: AppPaths) -> Self {
        Self {
            backend: RuntimeBackend::Legacy(legacy::LegacyBackend::start(paths)),
        }
    }
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn start_service(client: ServiceClient) -> Self {
        Self {
            backend: RuntimeBackend::Service(service::ServiceBackend::start(client)),
        }
    }
    #[cfg(test)]
    pub(super) fn assemble(
        spawn: impl FnOnce(
            std::sync::mpsc::Receiver<legacy::worker::LegacyRequest>,
            std::sync::mpsc::Sender<legacy::worker::LegacyEvent>,
        ) -> Result<std::thread::JoinHandle<()>, ()>,
    ) -> Self {
        Self {
            backend: RuntimeBackend::Legacy(legacy::LegacyBackend::assemble(spawn)),
        }
    }
    #[cfg(test)]
    fn assemble_service(
        initialized: bool,
        disconnect: std::sync::Arc<std::sync::atomic::AtomicBool>,
        observation: std::sync::Arc<service::ServiceObservationSlot>,
        spawn: impl FnOnce(
            std::sync::mpsc::Receiver<service::ServiceRequest>,
            std::sync::mpsc::Sender<service::ServiceEvent>,
        ) -> Result<std::thread::JoinHandle<()>, ()>,
    ) -> Self {
        Self {
            backend: RuntimeBackend::Service(service::ServiceBackend::assemble(
                initialized,
                disconnect,
                observation,
                spawn,
            )),
        }
    }
    #[cfg(test)]
    fn legacy_backend(&self) -> &legacy::LegacyBackend {
        let RuntimeBackend::Legacy(backend) = &self.backend else {
            panic!("expected legacy backend")
        };
        backend
    }
    #[cfg(test)]
    fn legacy_backend_mut(&mut self) -> &mut legacy::LegacyBackend {
        let RuntimeBackend::Legacy(backend) = &mut self.backend else {
            panic!("expected legacy backend")
        };
        backend
    }
    #[cfg(test)]
    fn apply_message(&mut self, event: legacy::worker::LegacyEvent) -> bool {
        self.legacy_backend_mut().apply(event)
    }
    #[cfg(test)]
    fn service_backend_mut(&mut self) -> &mut service::ServiceBackend {
        let RuntimeBackend::Service(backend) = &mut self.backend else {
            panic!("expected service backend")
        };
        backend
    }
    #[cfg(test)]
    fn service_backend(&self) -> &service::ServiceBackend {
        let RuntimeBackend::Service(backend) = &self.backend else {
            panic!("expected service backend")
        };
        backend
    }
    pub(crate) fn view(&self) -> ApiRuntimeView<'_> {
        match &self.backend {
            RuntimeBackend::Legacy(b) => b.view(),
            RuntimeBackend::Service(b) => b.view(),
        }
    }
    #[cfg(test)]
    pub(crate) fn phase(&self) -> &ApiRuntimePhase {
        self.view().phase()
    }
    #[cfg(test)]
    pub(crate) fn notice(&self) -> Option<ApiRuntimeNotice> {
        self.view().notice()
    }
    #[cfg(test)]
    pub(crate) fn active_model_id(&self) -> Option<&str> {
        self.view().active_model_id()
    }
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn is_shared_service(&self) -> bool {
        self.view().kind() == ApiRuntimeKind::Service
    }
    #[cfg(test)]
    pub(crate) fn service_initialized(&self) -> bool {
        self.view().initialized()
    }
    pub(crate) fn request_start(&mut self, model_id: String) -> bool {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.request_start(model_id),
            RuntimeBackend::Service(b) => b.request_start(model_id),
        }
    }
    pub(crate) fn request_stop(&mut self) -> bool {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.request_stop(),
            RuntimeBackend::Service(b) => b.request_stop(),
        }
    }
    pub(crate) fn request_probe(&mut self) -> bool {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.request_probe(),
            RuntimeBackend::Service(b) => b.request_probe(),
        }
    }
    pub(crate) fn prepare_shutdown(&mut self) -> bool {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.prepare_shutdown(),
            RuntimeBackend::Service(b) => b.prepare_shutdown(),
        }
    }
    pub(crate) fn drain(&mut self) -> bool {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.drain(),
            RuntimeBackend::Service(b) => b.drain(),
        }
    }
    pub(crate) fn shutdown_and_join(&mut self) -> Result<(), ApiRuntimeShutdownError> {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.shutdown_and_join(),
            RuntimeBackend::Service(b) => b.shutdown_and_join(),
        }
    }
    pub(crate) fn mark_unavailable_after_exit_failure(&mut self) {
        match &mut self.backend {
            RuntimeBackend::Legacy(b) => b.mark_unavailable(),
            RuntimeBackend::Service(b) => b.mark_unavailable(),
        }
    }
    #[cfg(test)]
    pub(crate) fn endpoint(&self) -> Option<&ApiEndpoint> {
        match self.phase() {
            ApiRuntimePhase::Ready { endpoint, .. } => Some(endpoint),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn owned_endpoint(&self) -> Option<&ApiEndpoint> {
        match &self.backend {
            RuntimeBackend::Legacy(b) => b.owned_endpoint(),
            RuntimeBackend::Service(b) => b.owned_endpoint(),
        }
    }
}

#[cfg(test)]
pub(crate) fn idle_controller_for_exit_test() -> ApiRuntimeController {
    ApiRuntimeController::assemble(|requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-exit-test".into())
            .spawn(move || {
                legacy::worker::run_legacy_worker::<loxa::api_runtime::ApiRuntimeHost>(
                    Err(()),
                    requests,
                    messages,
                )
            })
            .map_err(|_| ())
    })
}

#[cfg(test)]
#[path = "api_runtime_tests.rs"]
mod tests;

#[cfg(test)]
pub(super) use legacy::worker::{
    run_legacy_worker as run_runtime_worker, LegacyEvent as RuntimeMessage,
    LegacyRequest as RuntimeRequest, RuntimeHost,
};
#[cfg(test)]
pub(super) use StartOutcome as RuntimeHostStart;
