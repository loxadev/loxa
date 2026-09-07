#![cfg_attr(test, allow(dead_code))]

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use loxa::api_runtime::{ApiRuntimeActivity, ApiRuntimeHost, ApiStartCancellation};
use loxa::paths::AppPaths;
use loxa_ipc::{OperationTarget, RuntimePhase, RuntimeStatus, ServiceClient};

mod service;
mod worker;

use service::run_service_runtime_worker;
#[cfg(test)]
pub(super) use worker::RuntimeHost;
pub(super) use worker::{run_runtime_worker, RuntimeHostStart, RuntimeMessage, RuntimeRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ServiceObservation {
    Absent,
    Present(RuntimeStatus),
    Unavailable,
}

impl ServiceObservation {
    fn is_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable
                | Self::Present(RuntimeStatus {
                    phase: RuntimePhase::RecoveryRequired { .. } | RuntimePhase::Draining,
                    ..
                })
        )
    }

    fn status(&self) -> Option<&RuntimeStatus> {
        match self {
            Self::Present(status) => Some(status),
            Self::Absent | Self::Unavailable => None,
        }
    }

    fn operation(&self) -> Option<(u64, OperationTarget, &str)> {
        let status = self.status()?;
        let (task_id, generation, model_id) = match &status.phase {
            RuntimePhase::Starting {
                task_id,
                generation,
                model_id,
            }
            | RuntimePhase::Ready {
                task_id,
                generation,
                model_id,
                ..
            }
            | RuntimePhase::Stopping {
                task_id,
                generation,
                model_id,
            }
            | RuntimePhase::CleanupFailed {
                task_id,
                generation,
                model_id,
            }
            | RuntimePhase::LoadFailed {
                task_id,
                generation,
                model_id,
                ..
            } => (task_id, generation, model_id.as_str()),
            RuntimePhase::Unloaded
            | RuntimePhase::RecoveryRequired { .. }
            | RuntimePhase::Draining => return None,
        };
        let numeric_generation = generation
            .parse()
            .expect("validated service status has a numeric generation");
        Some((
            numeric_generation,
            OperationTarget {
                boot_epoch: status.boot_epoch.clone(),
                task_id: task_id.clone(),
                generation: generation.clone(),
            },
            model_id,
        ))
    }

    fn target(&self) -> Option<OperationTarget> {
        self.operation().map(|(_, target, _)| target)
    }

    fn endpoint(&self) -> Option<ApiEndpoint> {
        let (_, target, model_id) = self.operation()?;
        Some(ApiEndpoint::for_service(model_id.to_owned(), target))
    }
}

#[derive(Default)]
struct ServiceObservationSlot {
    latest: Mutex<Option<ServiceObservation>>,
}

impl ServiceObservationSlot {
    fn publish(&self, observed: ServiceObservation) -> Result<(), ()> {
        let mut latest = self.latest.lock().map_err(|_| ())?;
        *latest = Some(observed);
        Ok(())
    }

    fn take(&self) -> Result<Option<ServiceObservation>, ()> {
        self.latest
            .lock()
            .map_err(|_| ())
            .map(|mut latest| latest.take())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiEndpoint {
    model_id: String,
    port: u16,
    service_target: Option<OperationTarget>,
}

impl ApiEndpoint {
    pub(super) fn new(model_id: String, port: u16) -> Self {
        Self {
            model_id,
            port,
            service_target: None,
        }
    }

    pub(super) fn for_service(model_id: String, target: OperationTarget) -> Self {
        Self {
            model_id,
            port: 0,
            service_target: Some(target),
        }
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    fn service_target(&self) -> Option<&OperationTarget> {
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
pub(crate) struct ApiRuntimeShutdownError;

impl fmt::Display for ApiRuntimeShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the API runtime could not be shut down")
    }
}

impl std::error::Error for ApiRuntimeShutdownError {}

pub(crate) struct ApiRuntimeController {
    request_sender: Option<Sender<RuntimeRequest>>,
    receiver: Option<Receiver<RuntimeMessage>>,
    worker: Option<JoinHandle<()>>,
    phase: ApiRuntimePhase,
    notice: Option<ApiRuntimeNotice>,
    generation: Option<u64>,
    next_generation: u64,
    startup_cancellation: Option<ApiStartCancellation>,
    probe_in_flight: bool,
    owned_endpoint: Option<ApiEndpoint>,
    active_model_id: Option<String>,
    shared_service: bool,
    service_initialized: bool,
    service_target: Option<OperationTarget>,
    service_command_in_flight: bool,
    service_observation: Option<Arc<ServiceObservationSlot>>,
    service_disconnect: Option<Arc<AtomicBool>>,
}

impl ApiRuntimeController {
    pub(crate) fn start(paths: AppPaths) -> Self {
        Self::assemble_mode(false, true, None, None, |requests, messages| {
            std::thread::Builder::new()
                .name("loxa-menu-api-runtime".into())
                .spawn(move || {
                    run_runtime_worker(Ok(ApiRuntimeHost::new(paths)), requests, messages)
                })
                .map_err(|_| ())
        })
    }

    pub(crate) fn start_service(client: ServiceClient) -> Self {
        let disconnect = Arc::new(AtomicBool::new(false));
        let worker_disconnect = Arc::clone(&disconnect);
        let observation = Arc::new(ServiceObservationSlot::default());
        let worker_observation = Arc::clone(&observation);
        Self::assemble_mode(
            true,
            false,
            Some(disconnect),
            Some(observation),
            move |requests, messages| {
                std::thread::Builder::new()
                    .name("loxa-menu-service-runtime".into())
                    .spawn(move || {
                        run_service_runtime_worker(
                            client,
                            worker_disconnect,
                            worker_observation,
                            requests,
                            messages,
                        )
                    })
                    .map_err(|_| ())
            },
        )
    }

    #[cfg(test)]
    pub(super) fn assemble(
        spawn: impl FnOnce(
            Receiver<RuntimeRequest>,
            Sender<RuntimeMessage>,
        ) -> Result<JoinHandle<()>, ()>,
    ) -> Self {
        Self::assemble_mode(false, true, None, None, spawn)
    }

    fn assemble_mode(
        shared_service: bool,
        service_initialized: bool,
        service_disconnect: Option<Arc<AtomicBool>>,
        service_observation: Option<Arc<ServiceObservationSlot>>,
        spawn: impl FnOnce(
            Receiver<RuntimeRequest>,
            Sender<RuntimeMessage>,
        ) -> Result<JoinHandle<()>, ()>,
    ) -> Self {
        let (request_sender, requests) = mpsc::channel();
        let (messages, receiver) = mpsc::channel();
        let worker = spawn(requests, messages).ok();
        let controller_available = worker.is_some();
        Self {
            request_sender: controller_available.then_some(request_sender),
            receiver: controller_available.then_some(receiver),
            worker,
            phase: if controller_available {
                ApiRuntimePhase::Idle
            } else {
                ApiRuntimePhase::ControllerFailed
            },
            notice: (!controller_available).then_some(ApiRuntimeNotice::ControllerFailed),
            generation: None,
            next_generation: 1,
            startup_cancellation: None,
            probe_in_flight: false,
            owned_endpoint: None,
            active_model_id: None,
            shared_service,
            service_initialized: service_initialized || !controller_available,
            service_target: None,
            service_command_in_flight: false,
            service_observation,
            service_disconnect,
        }
    }

    pub(crate) fn phase(&self) -> &ApiRuntimePhase {
        &self.phase
    }

    pub(crate) fn notice(&self) -> Option<ApiRuntimeNotice> {
        self.notice
    }

    #[cfg(test)]
    pub(crate) fn endpoint(&self) -> Option<&ApiEndpoint> {
        match &self.phase {
            ApiRuntimePhase::Ready { endpoint, .. } => Some(endpoint),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn owned_endpoint(&self) -> Option<&ApiEndpoint> {
        self.owned_endpoint.as_ref()
    }

    pub(crate) fn active_model_id(&self) -> Option<&str> {
        self.active_model_id.as_deref()
    }

    pub(crate) fn is_shared_service(&self) -> bool {
        self.shared_service
    }

    pub(crate) fn service_initialized(&self) -> bool {
        self.service_initialized
    }

    pub(crate) fn request_start(&mut self, model_id: String) -> bool {
        if !matches!(self.phase, ApiRuntimePhase::Idle)
            || (self.shared_service && !self.service_initialized)
        {
            return false;
        }
        let generation = self.next_generation;
        let Some(next_generation) = generation.checked_add(1) else {
            self.fail_controller();
            return false;
        };
        let cancellation = ApiStartCancellation::new();
        if !self.send(RuntimeRequest::Start {
            generation,
            model_id: model_id.clone(),
            cancellation: cancellation.clone(),
        }) {
            return false;
        }
        self.next_generation = next_generation;
        self.generation = Some(generation);
        self.startup_cancellation = Some(cancellation);
        self.active_model_id = Some(model_id.clone());
        self.owned_endpoint = None;
        self.service_target = None;
        self.service_command_in_flight = self.shared_service;
        self.probe_in_flight = false;
        self.notice = None;
        self.phase = ApiRuntimePhase::Starting {
            generation,
            model_id,
        };
        true
    }

    pub(crate) fn request_stop(&mut self) -> bool {
        if !matches!(
            self.phase,
            ApiRuntimePhase::Starting { .. }
                | ApiRuntimePhase::Ready { .. }
                | ApiRuntimePhase::CleanupFailed
        ) {
            return false;
        }
        if let Some(cancellation) = self.startup_cancellation.as_ref() {
            cancellation.cancel();
        }
        if !self.send(RuntimeRequest::Stop {
            generation: self.generation,
            target: self.service_target.clone(),
        }) {
            return false;
        }
        self.service_command_in_flight = self.shared_service;
        self.notice = None;
        self.probe_in_flight = false;
        self.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(crate) fn request_probe(&mut self) -> bool {
        let eligible = if self.shared_service {
            self.service_initialized && !self.service_command_in_flight
        } else {
            matches!(self.phase, ApiRuntimePhase::Ready { .. })
        };
        if !eligible || self.probe_in_flight {
            return false;
        }
        if !self.send(RuntimeRequest::Probe) {
            return false;
        }
        self.probe_in_flight = true;
        true
    }

    pub(crate) fn prepare_shutdown(&mut self) -> bool {
        if let Some(disconnect) = self.service_disconnect.as_ref() {
            disconnect.store(true, Ordering::Release);
        }
        if self.shared_service {
            return false;
        }
        if let Some(cancellation) = self.startup_cancellation.as_ref() {
            cancellation.cancel();
        }
        if !matches!(
            self.phase,
            ApiRuntimePhase::Starting { .. }
                | ApiRuntimePhase::Ready { .. }
                | ApiRuntimePhase::CleanupFailed
        ) {
            return false;
        }
        self.notice = None;
        self.probe_in_flight = false;
        self.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(crate) fn drain(&mut self) -> bool {
        let mut changed = false;
        let mut disconnected = false;
        while let Some(receiver) = self.receiver.as_ref() {
            let result = receiver.try_recv();
            match result {
                Ok(message) => {
                    changed |= self.apply_message(message);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if disconnected && self.worker.is_some() {
            self.fail_controller();
            if let Some(observation) = self.service_observation.as_ref() {
                let _ = observation.take();
            }
            return true;
        }
        if !self.service_command_in_flight {
            if let Some(observation) = self.service_observation.as_ref() {
                match observation.take() {
                    Ok(Some(observed)) => changed |= self.apply_observed(observed),
                    Ok(None) => {}
                    Err(()) => {
                        self.fail_controller();
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    pub(crate) fn shutdown_and_join(&mut self) -> Result<(), ApiRuntimeShutdownError> {
        if let Some(disconnect) = self.service_disconnect.as_ref() {
            disconnect.store(true, Ordering::Release);
        }
        if !self.shared_service {
            if let Some(cancellation) = self.startup_cancellation.as_ref() {
                cancellation.cancel();
            }
        }
        if self.worker.is_none() {
            return Ok(());
        }
        let (reply, response) = mpsc::channel();
        let sent = self
            .request_sender
            .as_ref()
            .is_some_and(|sender| sender.send(RuntimeRequest::Shutdown { reply }).is_ok());
        if !sent {
            self.fail_controller();
            return Err(ApiRuntimeShutdownError);
        }
        let Ok(reply) = response.recv() else {
            self.fail_controller();
            return Err(ApiRuntimeShutdownError);
        };
        if reply.result.is_err() {
            if let Some(endpoint) = reply.endpoint {
                self.active_model_id = Some(endpoint.model_id.clone());
                self.owned_endpoint = Some(endpoint);
            }
            self.generation = reply.generation.or(self.generation);
            self.notice = Some(ApiRuntimeNotice::CleanupFailed);
            self.phase = ApiRuntimePhase::CleanupFailed;
            return Err(ApiRuntimeShutdownError);
        }

        let worker = self.worker.take().expect("the clean worker is retained");
        if worker.join().is_err() {
            self.fail_controller();
            return Err(ApiRuntimeShutdownError);
        }
        self.request_sender.take();
        self.receiver.take();
        self.clear_owned_state();
        Ok(())
    }

    pub(crate) fn mark_unavailable_after_exit_failure(&mut self) {
        debug_assert!(self.worker.is_none());
        self.fail_controller();
    }

    fn send(&mut self, request: RuntimeRequest) -> bool {
        let sent = self
            .request_sender
            .as_ref()
            .is_some_and(|sender| sender.send(request).is_ok());
        if !sent {
            self.fail_controller();
        }
        sent
    }

    fn apply_message(&mut self, message: RuntimeMessage) -> bool {
        match message {
            RuntimeMessage::Started {
                generation,
                outcome,
            } if matches!(
                &self.phase,
                ApiRuntimePhase::Starting {
                    generation: current,
                    ..
                } if *current == generation
            ) =>
            {
                self.startup_cancellation = None;
                self.service_command_in_flight = false;
                match outcome {
                    RuntimeHostStart::Ready(endpoint) => {
                        self.service_target = endpoint.service_target().cloned();
                        self.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint.clone());
                        self.notice = None;
                        self.probe_in_flight = true;
                        self.phase = ApiRuntimePhase::Ready {
                            generation,
                            endpoint,
                            activity: ApiRuntimeActivity::Unknown,
                        };
                    }
                    RuntimeHostStart::CleanupFailed(endpoint) => {
                        self.service_target = endpoint.service_target().cloned();
                        self.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint);
                        self.notice = Some(ApiRuntimeNotice::CleanupFailed);
                        self.phase = ApiRuntimePhase::CleanupFailed;
                    }
                    RuntimeHostStart::Conflict => {
                        self.clear_to_idle(Some(ApiRuntimeNotice::Conflict));
                    }
                    RuntimeHostStart::Cancelled => self.clear_to_idle(None),
                    RuntimeHostStart::ModelUnavailable => {
                        self.clear_to_idle(Some(ApiRuntimeNotice::ModelUnavailable));
                    }
                    RuntimeHostStart::StartupFailed => {
                        self.clear_to_idle(Some(ApiRuntimeNotice::StartFailed));
                    }
                }
                true
            }
            RuntimeMessage::Activity {
                generation,
                activity,
            } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    endpoint,
                    ..
                } = &self.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                self.probe_in_flight = false;
                self.phase = ApiRuntimePhase::Ready {
                    generation,
                    endpoint: endpoint.clone(),
                    activity,
                };
                true
            }
            RuntimeMessage::UnexpectedStop { generation } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    ..
                } = &self.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                self.clear_to_idle(Some(ApiRuntimeNotice::UnexpectedStop));
                true
            }
            RuntimeMessage::ProbeCleanupFailed {
                generation,
                endpoint,
            } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    ..
                } = &self.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                self.startup_cancellation = None;
                self.probe_in_flight = false;
                self.active_model_id = Some(endpoint.model_id.clone());
                self.owned_endpoint = Some(endpoint);
                self.notice = Some(ApiRuntimeNotice::CleanupFailed);
                self.phase = ApiRuntimePhase::CleanupFailed;
                true
            }
            RuntimeMessage::Stopped {
                generation,
                result,
                endpoint,
            } if matches!(self.phase, ApiRuntimePhase::Stopping)
                && generation.is_some()
                && generation == self.generation =>
            {
                self.startup_cancellation = None;
                self.service_command_in_flight = false;
                self.probe_in_flight = false;
                if result.is_ok() {
                    self.clear_to_idle(None);
                } else {
                    if let Some(endpoint) = endpoint {
                        self.service_target = endpoint.service_target().cloned();
                        self.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint);
                    }
                    self.notice = Some(ApiRuntimeNotice::CleanupFailed);
                    self.phase = ApiRuntimePhase::CleanupFailed;
                }
                true
            }
            RuntimeMessage::ControllerFailed => {
                self.fail_controller();
                true
            }
            RuntimeMessage::Started { .. } | RuntimeMessage::Stopped { .. } => false,
        }
    }

    fn apply_observed(&mut self, observed: ServiceObservation) -> bool {
        if !self.shared_service {
            return false;
        }
        let initialized_changed = !self.service_initialized;
        self.service_initialized = true;
        self.probe_in_flight = false;
        if self.service_command_in_flight {
            return initialized_changed;
        }
        if self.retained_authority_conflicts_with(&observed) {
            if observed.is_unavailable() {
                self.fail_controller();
            } else {
                self.startup_cancellation = None;
                self.probe_in_flight = false;
                self.notice = Some(ApiRuntimeNotice::CleanupFailed);
                self.phase = ApiRuntimePhase::CleanupFailed;
            }
            return true;
        }
        let operation = observed
            .operation()
            .map(|(generation, target, model_id)| (generation, target, model_id.to_owned()));
        match observed {
            ServiceObservation::Absent => {
                self.clear_to_idle(Some(ApiRuntimeNotice::ServiceAbsent));
            }
            ServiceObservation::Present(status) => match status.phase {
                RuntimePhase::Unloaded => self.clear_to_idle(None),
                phase @ (RuntimePhase::Starting { .. }
                | RuntimePhase::Ready { .. }
                | RuntimePhase::Stopping { .. }
                | RuntimePhase::CleanupFailed { .. }) => {
                    let Some((generation, target, model_id)) = operation else {
                        self.fail_controller();
                        return true;
                    };
                    self.generation = Some(generation);
                    self.startup_cancellation = None;
                    self.service_target = Some(target.clone());
                    self.active_model_id = Some(model_id.clone());
                    self.notice = None;
                    match phase {
                        RuntimePhase::Starting { .. } => {
                            self.owned_endpoint = None;
                            self.phase = ApiRuntimePhase::Starting {
                                generation,
                                model_id,
                            };
                        }
                        RuntimePhase::Ready { .. } => {
                            let endpoint = ApiEndpoint::for_service(model_id, target);
                            self.owned_endpoint = Some(endpoint.clone());
                            self.phase = ApiRuntimePhase::Ready {
                                generation,
                                endpoint,
                                activity: ApiRuntimeActivity::Unknown,
                            };
                        }
                        RuntimePhase::Stopping { .. } => {
                            self.owned_endpoint = None;
                            self.phase = ApiRuntimePhase::Stopping;
                        }
                        RuntimePhase::CleanupFailed { .. } => {
                            let endpoint = ApiEndpoint::for_service(model_id, target);
                            self.owned_endpoint = Some(endpoint);
                            self.notice = Some(ApiRuntimeNotice::CleanupFailed);
                            self.phase = ApiRuntimePhase::CleanupFailed;
                        }
                        RuntimePhase::Unloaded
                        | RuntimePhase::LoadFailed { .. }
                        | RuntimePhase::RecoveryRequired { .. }
                        | RuntimePhase::Draining => unreachable!("operation phase was matched"),
                    }
                }
                RuntimePhase::LoadFailed { category, .. } => {
                    let notice = match category {
                        loxa_ipc::ErrorCategory::Busy | loxa_ipc::ErrorCategory::Conflict => {
                            ApiRuntimeNotice::Conflict
                        }
                        loxa_ipc::ErrorCategory::NotFound
                        | loxa_ipc::ErrorCategory::ModelUnavailable => {
                            ApiRuntimeNotice::ModelUnavailable
                        }
                        _ => ApiRuntimeNotice::StartFailed,
                    };
                    self.clear_to_idle(Some(notice));
                }
                RuntimePhase::RecoveryRequired { .. } | RuntimePhase::Draining => {
                    self.fail_controller()
                }
            },
            ServiceObservation::Unavailable => self.fail_controller(),
        }
        true
    }

    fn retained_authority_conflicts_with(&self, observed: &ServiceObservation) -> bool {
        if matches!(self.phase, ApiRuntimePhase::CleanupFailed) && self.service_target.is_none() {
            return true;
        }
        let Some(retained) = self.service_target.as_ref() else {
            return false;
        };
        match observed {
            ServiceObservation::Absent | ServiceObservation::Unavailable => true,
            ServiceObservation::Present(status) => {
                matches!(
                    status.phase,
                    RuntimePhase::RecoveryRequired { .. } | RuntimePhase::Draining
                ) || status.boot_epoch != retained.boot_epoch
            }
        }
    }

    fn clear_to_idle(&mut self, notice: Option<ApiRuntimeNotice>) {
        self.generation = None;
        self.startup_cancellation = None;
        self.probe_in_flight = false;
        self.owned_endpoint = None;
        self.active_model_id = None;
        self.service_target = None;
        self.service_command_in_flight = false;
        self.notice = notice;
        self.phase = ApiRuntimePhase::Idle;
    }

    fn clear_owned_state(&mut self) {
        self.clear_to_idle(None);
    }

    fn fail_controller(&mut self) {
        self.startup_cancellation = None;
        self.probe_in_flight = false;
        self.service_command_in_flight = false;
        self.notice = Some(ApiRuntimeNotice::ControllerFailed);
        self.phase = ApiRuntimePhase::ControllerFailed;
    }
}

#[cfg(test)]
pub(crate) fn idle_controller_for_exit_test() -> ApiRuntimeController {
    ApiRuntimeController::assemble(|requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-exit-test".into())
            .spawn(move || {
                run_runtime_worker::<ApiRuntimeHost>(Err(()), requests, messages);
            })
            .map_err(|_| ())
    })
}

#[cfg(test)]
#[path = "api_runtime_tests.rs"]
mod tests;
