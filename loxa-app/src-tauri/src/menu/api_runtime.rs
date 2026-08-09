use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::JoinHandle;

use loxa::api_runtime::{ApiRuntimeActivity, ApiRuntimeHost, ApiStartCancellation};
use loxa::paths::AppPaths;

mod worker;

#[cfg(test)]
pub(super) use worker::RuntimeHost;
pub(super) use worker::{run_runtime_worker, RuntimeHostStart, RuntimeMessage, RuntimeRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiEndpoint {
    model_id: String,
    port: u16,
}

impl ApiEndpoint {
    pub(super) fn new(model_id: String, port: u16) -> Self {
        Self { model_id, port }
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
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
}

impl ApiRuntimeController {
    pub(crate) fn start() -> Self {
        Self::assemble(|requests, messages| {
            std::thread::Builder::new()
                .name("loxa-menu-api-runtime".into())
                .spawn(move || match AppPaths::from_env() {
                    Ok(paths) => {
                        run_runtime_worker(Ok(ApiRuntimeHost::new(paths)), requests, messages)
                    }
                    Err(_) => run_runtime_worker::<ApiRuntimeHost>(Err(()), requests, messages),
                })
                .map_err(|_| ())
        })
    }

    pub(super) fn assemble(
        spawn: impl FnOnce(
            Receiver<RuntimeRequest>,
            Sender<RuntimeMessage>,
        ) -> Result<JoinHandle<()>, ()>,
    ) -> Self {
        let (request_sender, requests) = mpsc::channel();
        let (messages, receiver) = mpsc::channel();
        match spawn(requests, messages) {
            Ok(worker) => Self {
                request_sender: Some(request_sender),
                receiver: Some(receiver),
                worker: Some(worker),
                phase: ApiRuntimePhase::Idle,
                notice: None,
                generation: None,
                next_generation: 1,
                startup_cancellation: None,
                probe_in_flight: false,
                owned_endpoint: None,
                active_model_id: None,
            },
            Err(()) => Self {
                request_sender: None,
                receiver: None,
                worker: None,
                phase: ApiRuntimePhase::ControllerFailed,
                notice: Some(ApiRuntimeNotice::ControllerFailed),
                generation: None,
                next_generation: 1,
                startup_cancellation: None,
                probe_in_flight: false,
                owned_endpoint: None,
                active_model_id: None,
            },
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

    pub(crate) fn request_start(&mut self, model_id: String) -> bool {
        if !matches!(self.phase, ApiRuntimePhase::Idle) {
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
        if !self.send(RuntimeRequest::Stop) {
            return false;
        }
        self.notice = None;
        self.probe_in_flight = false;
        self.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(crate) fn request_probe(&mut self) -> bool {
        if !matches!(self.phase, ApiRuntimePhase::Ready { .. }) || self.probe_in_flight {
            return false;
        }
        if !self.send(RuntimeRequest::Probe) {
            return false;
        }
        self.probe_in_flight = true;
        true
    }

    pub(crate) fn prepare_shutdown(&mut self) -> bool {
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
            changed = true;
        }
        changed
    }

    pub(crate) fn shutdown_and_join(&mut self) -> Result<(), ApiRuntimeShutdownError> {
        if let Some(cancellation) = self.startup_cancellation.as_ref() {
            cancellation.cancel();
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
                match outcome {
                    RuntimeHostStart::Ready(endpoint) => {
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
                self.probe_in_flight = false;
                if result.is_ok() {
                    self.clear_to_idle(None);
                } else {
                    if let Some(endpoint) = endpoint {
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

    fn clear_to_idle(&mut self, notice: Option<ApiRuntimeNotice>) {
        self.generation = None;
        self.startup_cancellation = None;
        self.probe_in_flight = false;
        self.owned_endpoint = None;
        self.active_model_id = None;
        self.notice = notice;
        self.phase = ApiRuntimePhase::Idle;
    }

    fn clear_owned_state(&mut self) {
        self.clear_to_idle(None);
    }

    fn fail_controller(&mut self) {
        self.startup_cancellation = None;
        self.probe_in_flight = false;
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
