mod worker;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use loxa::api_runtime::ApiStartCancellation;
use loxa_ipc::{OperationTarget, RuntimePhase, RuntimeStatus, ServiceClient};

use super::{
    ApiEndpoint, ApiRuntimeKind, ApiRuntimeNotice, ApiRuntimePhase, ApiRuntimeShutdownError,
    ApiRuntimeView, RuntimeState, StartOutcome,
};
use worker::run_service_runtime_worker;

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
            _ => None,
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
        Some((
            generation
                .parse()
                .expect("validated service status has a numeric generation"),
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
        Some(ApiEndpoint::service(model_id.to_owned(), target))
    }
}

#[derive(Default)]
pub(super) struct ServiceObservationSlot {
    latest: Mutex<Option<ServiceObservation>>,
}
impl ServiceObservationSlot {
    fn publish(&self, observed: ServiceObservation) -> Result<(), ()> {
        *self.latest.lock().map_err(|_| ())? = Some(observed);
        Ok(())
    }
    fn take(&self) -> Result<Option<ServiceObservation>, ()> {
        self.latest
            .lock()
            .map_err(|_| ())
            .map(|mut latest| latest.take())
    }
}

pub(super) enum ServiceRequest {
    Start {
        generation: u64,
        model_id: String,
        cancellation: ApiStartCancellation,
    },
    Stop {
        generation: Option<u64>,
        target: Option<OperationTarget>,
    },
    Probe,
    Shutdown {
        reply: Sender<ServiceShutdownReply>,
    },
}

pub(super) enum ServiceEvent {
    Started {
        generation: u64,
        outcome: StartOutcome,
    },
    Stopped {
        generation: Option<u64>,
        result: Result<(), ()>,
        endpoint: Option<ApiEndpoint>,
    },
    ControllerFailed,
}

pub(super) struct ServiceShutdownReply {
    pub(super) generation: Option<u64>,
    pub(super) result: Result<(), ()>,
    pub(super) endpoint: Option<ApiEndpoint>,
}

pub(super) struct ServiceBackend {
    pub(super) requests: Option<Sender<ServiceRequest>>,
    pub(super) events: Option<Receiver<ServiceEvent>>,
    pub(super) worker: Option<JoinHandle<()>>,
    pub(super) state: RuntimeState,
    pub(super) generation: Option<u64>,
    pub(super) next_generation: u64,
    pub(super) startup_cancellation: Option<ApiStartCancellation>,
    pub(super) target: Option<OperationTarget>,
    pub(super) command_in_flight: bool,
    probe_in_flight: bool,
    pub(super) initialized: bool,
    pub(super) observation: Arc<ServiceObservationSlot>,
    pub(super) disconnect: Arc<AtomicBool>,
    pub(super) owned_endpoint: Option<ApiEndpoint>,
    command_executable: Option<std::path::PathBuf>,
    command_root: Option<std::path::PathBuf>,
}

impl ServiceBackend {
    pub(super) fn start(client: ServiceClient) -> Self {
        let command_executable = std::env::current_exe().ok();
        let command_root = client.bootstrap().root().root().to_owned();
        let worker_client = client.clone();
        let disconnect = Arc::new(AtomicBool::new(false));
        let worker_disconnect = Arc::clone(&disconnect);
        let observation = Arc::new(ServiceObservationSlot::default());
        let worker_observation = Arc::clone(&observation);
        let mut backend =
            Self::assemble(false, disconnect, observation, move |requests, events| {
                std::thread::Builder::new()
                    .name("loxa-menu-service-runtime".into())
                    .spawn(move || {
                        run_service_runtime_worker(
                            worker_client,
                            worker_disconnect,
                            worker_observation,
                            requests,
                            events,
                        )
                    })
                    .map_err(|_| ())
            });
        backend.command_executable = command_executable;
        backend.command_root = Some(command_root);
        backend
    }

    pub(super) fn assemble(
        initialized: bool,
        disconnect: Arc<AtomicBool>,
        observation: Arc<ServiceObservationSlot>,
        spawn: impl FnOnce(Receiver<ServiceRequest>, Sender<ServiceEvent>) -> Result<JoinHandle<()>, ()>,
    ) -> Self {
        let (request_sender, requests) = mpsc::channel();
        let (events_sender, events) = mpsc::channel();
        let worker = spawn(requests, events_sender).ok();
        let available = worker.is_some();
        Self {
            requests: available.then_some(request_sender),
            events: available.then_some(events),
            worker,
            state: if available {
                RuntimeState::available()
            } else {
                RuntimeState::failed()
            },
            generation: None,
            next_generation: 1,
            startup_cancellation: None,
            target: None,
            command_in_flight: false,
            probe_in_flight: false,
            initialized: initialized || !available,
            observation,
            disconnect,
            owned_endpoint: None,
            command_executable: None,
            command_root: None,
        }
    }

    pub(super) fn view(&self) -> ApiRuntimeView<'_> {
        self.state.view(ApiRuntimeKind::Service, self.initialized)
    }
    pub(super) fn command_context(&self) -> Option<(&std::path::Path, &std::path::Path)> {
        Some((
            self.command_executable.as_deref()?,
            self.command_root.as_deref()?,
        ))
    }
    #[cfg(test)]
    pub(super) fn owned_endpoint(&self) -> Option<&ApiEndpoint> {
        self.owned_endpoint.as_ref()
    }

    pub(super) fn request_start(&mut self, model_id: String) -> bool {
        if !self.initialized || !matches!(self.state.phase, ApiRuntimePhase::Idle) {
            return false;
        }
        let generation = self.next_generation;
        let Some(next) = generation.checked_add(1) else {
            self.fail();
            return false;
        };
        let cancellation = ApiStartCancellation::new();
        if !self.send(ServiceRequest::Start {
            generation,
            model_id: model_id.clone(),
            cancellation: cancellation.clone(),
        }) {
            return false;
        }
        self.next_generation = next;
        self.generation = Some(generation);
        self.startup_cancellation = Some(cancellation);
        self.state.active_model_id = Some(model_id.clone());
        self.owned_endpoint = None;
        self.target = None;
        self.command_in_flight = true;
        self.probe_in_flight = false;
        self.state.notice = None;
        self.state.phase = ApiRuntimePhase::Starting {
            generation,
            model_id,
        };
        true
    }

    pub(super) fn request_stop(&mut self) -> bool {
        if !matches!(
            self.state.phase,
            ApiRuntimePhase::Starting { .. }
                | ApiRuntimePhase::Ready { .. }
                | ApiRuntimePhase::CleanupFailed
        ) {
            return false;
        }
        if let Some(cancellation) = &self.startup_cancellation {
            cancellation.cancel();
        }
        if !self.send(ServiceRequest::Stop {
            generation: self.generation,
            target: self.target.clone(),
        }) {
            return false;
        }
        self.command_in_flight = true;
        self.probe_in_flight = false;
        self.state.notice = None;
        self.state.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(super) fn request_probe(&mut self) -> bool {
        if !self.initialized || self.command_in_flight || self.probe_in_flight {
            return false;
        }
        if !self.send(ServiceRequest::Probe) {
            return false;
        }
        self.probe_in_flight = true;
        true
    }

    pub(super) fn prepare_shutdown(&mut self) -> bool {
        self.disconnect.store(true, Ordering::Release);
        false
    }

    pub(super) fn drain(&mut self) -> bool {
        let mut changed = false;
        let mut disconnected = false;
        while let Some(events) = &self.events {
            match events.try_recv() {
                Ok(event) => changed |= self.apply_event(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if disconnected && self.worker.is_some() {
            self.fail();
            let _ = self.observation.take();
            return true;
        }
        if !self.command_in_flight {
            match self.observation.take() {
                Ok(Some(observed)) => changed |= self.apply_observed(observed),
                Ok(None) => {}
                Err(()) => {
                    self.fail();
                    changed = true;
                }
            }
        }
        changed
    }

    pub(super) fn shutdown_and_join(&mut self) -> Result<(), ApiRuntimeShutdownError> {
        self.disconnect.store(true, Ordering::Release);
        if self.worker.is_none() {
            return Ok(());
        }
        let (reply, response) = mpsc::channel();
        if !self
            .requests
            .as_ref()
            .is_some_and(|sender| sender.send(ServiceRequest::Shutdown { reply }).is_ok())
        {
            self.fail();
            return Err(ApiRuntimeShutdownError);
        }
        let Ok(reply) = response.recv() else {
            self.fail();
            return Err(ApiRuntimeShutdownError);
        };
        if reply.result.is_err() {
            if let Some(endpoint) = reply.endpoint {
                self.state.active_model_id = Some(endpoint.model_id.clone());
                self.owned_endpoint = Some(endpoint);
            }
            self.generation = reply.generation.or(self.generation);
            self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
            self.state.phase = ApiRuntimePhase::CleanupFailed;
            return Err(ApiRuntimeShutdownError);
        }
        let worker = self.worker.take().expect("the clean worker is retained");
        if worker.join().is_err() {
            self.fail();
            return Err(ApiRuntimeShutdownError);
        }
        self.requests.take();
        self.events.take();
        self.clear(None);
        Ok(())
    }

    pub(super) fn mark_unavailable(&mut self) {
        debug_assert!(self.worker.is_none());
        self.fail();
    }

    fn send(&mut self, request: ServiceRequest) -> bool {
        if self
            .requests
            .as_ref()
            .is_some_and(|sender| sender.send(request).is_ok())
        {
            true
        } else {
            self.fail();
            false
        }
    }

    pub(super) fn apply_event(&mut self, event: ServiceEvent) -> bool {
        match event {
            ServiceEvent::Started {
                generation,
                outcome,
            } if matches!(&self.state.phase, ApiRuntimePhase::Starting { generation: current, .. } if *current == generation) =>
            {
                self.startup_cancellation = None;
                self.command_in_flight = false;
                match outcome {
                    StartOutcome::Ready(endpoint) => {
                        self.target = endpoint.service_target().cloned();
                        self.state.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint.clone());
                        self.state.notice = None;
                        self.probe_in_flight = true;
                        self.state.phase = ApiRuntimePhase::Ready {
                            generation,
                            endpoint,
                            activity: loxa::api_runtime::ApiRuntimeActivity::Unknown,
                        };
                    }
                    StartOutcome::CleanupFailed(endpoint) => {
                        self.target = endpoint.service_target().cloned();
                        self.state.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint);
                        self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                        self.state.phase = ApiRuntimePhase::CleanupFailed;
                    }
                    StartOutcome::Conflict => self.clear(Some(ApiRuntimeNotice::Conflict)),
                    StartOutcome::Cancelled => self.clear(None),
                    StartOutcome::ModelUnavailable => {
                        self.clear(Some(ApiRuntimeNotice::ModelUnavailable))
                    }
                    StartOutcome::StartupFailed => self.clear(Some(ApiRuntimeNotice::StartFailed)),
                }
                true
            }
            ServiceEvent::Stopped {
                generation,
                result,
                endpoint,
            } if matches!(self.state.phase, ApiRuntimePhase::Stopping)
                && generation.is_some()
                && generation == self.generation =>
            {
                self.startup_cancellation = None;
                self.command_in_flight = false;
                self.probe_in_flight = false;
                if result.is_ok() {
                    self.clear(None);
                } else {
                    if let Some(endpoint) = endpoint {
                        self.target = endpoint.service_target().cloned();
                        self.state.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint);
                    }
                    self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                    self.state.phase = ApiRuntimePhase::CleanupFailed;
                }
                true
            }
            ServiceEvent::ControllerFailed => {
                self.fail();
                true
            }
            ServiceEvent::Started { .. } | ServiceEvent::Stopped { .. } => false,
        }
    }

    pub(super) fn apply_observed(&mut self, observed: ServiceObservation) -> bool {
        let initialized_changed = !self.initialized;
        self.initialized = true;
        self.probe_in_flight = false;
        if self.command_in_flight {
            return initialized_changed;
        }
        if self.retained_authority_conflicts_with(&observed) {
            if observed.is_unavailable() {
                self.fail();
            } else {
                self.startup_cancellation = None;
                self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                self.state.phase = ApiRuntimePhase::CleanupFailed;
            }
            return true;
        }
        let operation = observed
            .operation()
            .map(|(generation, target, model)| (generation, target, model.to_owned()));
        match observed {
            ServiceObservation::Absent => self.clear(Some(ApiRuntimeNotice::ServiceAbsent)),
            ServiceObservation::Unavailable => self.fail(),
            ServiceObservation::Present(status) => match status.phase {
                RuntimePhase::Unloaded => self.clear(None),
                phase @ (RuntimePhase::Starting { .. }
                | RuntimePhase::Ready { .. }
                | RuntimePhase::Stopping { .. }
                | RuntimePhase::CleanupFailed { .. }) => {
                    let Some((generation, target, model_id)) = operation else {
                        self.fail();
                        return true;
                    };
                    self.generation = Some(generation);
                    self.startup_cancellation = None;
                    self.target = Some(target.clone());
                    self.state.active_model_id = Some(model_id.clone());
                    self.state.notice = None;
                    match phase {
                        RuntimePhase::Starting { .. } => {
                            self.owned_endpoint = None;
                            self.state.phase = ApiRuntimePhase::Starting {
                                generation,
                                model_id,
                            };
                        }
                        RuntimePhase::Ready { .. } => {
                            let endpoint = ApiEndpoint::service(model_id, target);
                            self.owned_endpoint = Some(endpoint.clone());
                            self.state.phase = ApiRuntimePhase::Ready {
                                generation,
                                endpoint,
                                activity: loxa::api_runtime::ApiRuntimeActivity::Unknown,
                            };
                        }
                        RuntimePhase::Stopping { .. } => {
                            self.owned_endpoint = None;
                            self.state.phase = ApiRuntimePhase::Stopping;
                        }
                        RuntimePhase::CleanupFailed { .. } => {
                            let endpoint = ApiEndpoint::service(model_id, target);
                            self.owned_endpoint = Some(endpoint);
                            self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                            self.state.phase = ApiRuntimePhase::CleanupFailed;
                        }
                        _ => unreachable!(),
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
                    self.clear(Some(notice));
                }
                RuntimePhase::RecoveryRequired { .. } | RuntimePhase::Draining => self.fail(),
            },
        }
        true
    }

    fn retained_authority_conflicts_with(&self, observed: &ServiceObservation) -> bool {
        if matches!(self.state.phase, ApiRuntimePhase::CleanupFailed) && self.target.is_none() {
            return true;
        }
        let Some(retained) = &self.target else {
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

    fn clear(&mut self, notice: Option<ApiRuntimeNotice>) {
        self.generation = None;
        self.startup_cancellation = None;
        self.owned_endpoint = None;
        self.target = None;
        self.command_in_flight = false;
        self.probe_in_flight = false;
        self.state.clear(notice);
    }
    fn fail(&mut self) {
        self.startup_cancellation = None;
        self.command_in_flight = false;
        self.probe_in_flight = false;
        self.state.fail();
    }
}

#[cfg(test)]
mod tests;
