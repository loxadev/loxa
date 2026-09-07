use std::sync::mpsc::{Receiver, Sender};

use loxa::api_runtime::{
    ApiRuntimeActivity, ApiRuntimeHost, ApiRuntimeProbe, ApiStartCancellation, ApiStartError,
    ApiStartOutcome,
};

use crate::menu::api_runtime::{ApiEndpoint, StartOutcome};

pub(in crate::menu) trait RuntimeHost: Send + 'static {
    fn endpoint(&self) -> Option<ApiEndpoint>;
    fn start(&mut self, model_id: &str, cancellation: &ApiStartCancellation) -> StartOutcome;
    fn stop(&mut self) -> Result<(), ()>;
    fn activity(&mut self) -> ApiRuntimeActivity;
    fn probe(&mut self) -> ApiRuntimeProbe {
        ApiRuntimeProbe::Activity(self.activity())
    }
}

impl RuntimeHost for ApiRuntimeHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        ApiRuntimeHost::endpoint(self)
            .map(|endpoint| ApiEndpoint::legacy(endpoint.model_id().to_owned(), endpoint.port()))
    }

    fn start(&mut self, model_id: &str, cancellation: &ApiStartCancellation) -> StartOutcome {
        match ApiRuntimeHost::start(self, model_id, cancellation) {
            Ok(ApiStartOutcome::Started(endpoint) | ApiStartOutcome::AlreadyRunning(endpoint)) => {
                StartOutcome::Ready(ApiEndpoint::legacy(
                    endpoint.model_id().to_owned(),
                    endpoint.port(),
                ))
            }
            Err(ApiStartError::Conflict) => StartOutcome::Conflict,
            Err(ApiStartError::Cancelled) => StartOutcome::Cancelled,
            Err(ApiStartError::ModelUnavailable) => StartOutcome::ModelUnavailable,
            Err(ApiStartError::StartupFailed) => StartOutcome::StartupFailed,
        }
    }

    fn stop(&mut self) -> Result<(), ()> {
        ApiRuntimeHost::stop(self).map(|_| ()).map_err(|_| ())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        ApiRuntimeHost::activity(self)
    }

    fn probe(&mut self) -> ApiRuntimeProbe {
        ApiRuntimeHost::probe(self)
    }
}

pub(in crate::menu) enum LegacyRequest {
    Start {
        generation: u64,
        model_id: String,
        cancellation: ApiStartCancellation,
    },
    Stop,
    Probe,
    Shutdown {
        reply: Sender<LegacyShutdownReply>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::menu) enum LegacyEvent {
    Started {
        generation: u64,
        outcome: StartOutcome,
    },
    Activity {
        generation: u64,
        activity: ApiRuntimeActivity,
    },
    UnexpectedStop {
        generation: u64,
    },
    ProbeCleanupFailed {
        generation: u64,
        endpoint: ApiEndpoint,
    },
    Stopped {
        generation: Option<u64>,
        result: Result<(), ()>,
        endpoint: Option<ApiEndpoint>,
    },
    ControllerFailed,
}

pub(in crate::menu) struct LegacyShutdownReply {
    pub(in crate::menu) generation: Option<u64>,
    pub(in crate::menu) result: Result<(), ()>,
    pub(in crate::menu) endpoint: Option<ApiEndpoint>,
}

pub(in crate::menu) fn run_legacy_worker<H: RuntimeHost>(
    host: Result<H, ()>,
    requests: Receiver<LegacyRequest>,
    messages: Sender<LegacyEvent>,
) {
    let Ok(mut host) = host else {
        if messages.send(LegacyEvent::ControllerFailed).is_err() {
            return;
        }
        while let Ok(request) = requests.recv() {
            if let LegacyRequest::Shutdown { reply } = request {
                let _ = reply.send(LegacyShutdownReply {
                    generation: None,
                    result: Ok(()),
                    endpoint: None,
                });
                return;
            }
        }
        return;
    };
    let mut active_generation = None;

    while let Ok(request) = requests.recv() {
        match request {
            LegacyRequest::Start {
                generation,
                model_id,
                cancellation,
            } => {
                active_generation = Some(generation);
                let mut outcome = host.start(&model_id, &cancellation);
                if !matches!(outcome, StartOutcome::Ready(_)) {
                    if let Some(endpoint) = host.endpoint() {
                        outcome = StartOutcome::CleanupFailed(endpoint);
                    }
                }
                let ready = matches!(&outcome, StartOutcome::Ready(_));
                if messages
                    .send(LegacyEvent::Started {
                        generation,
                        outcome,
                    })
                    .is_err()
                {
                    return;
                }
                if ready {
                    let message = probe_message(&mut host, generation);
                    if messages.send(message).is_err() {
                        return;
                    }
                }
            }
            LegacyRequest::Stop => {
                let result = host.stop();
                let endpoint = host.endpoint();
                let generation = active_generation;
                if result.is_ok() {
                    active_generation = None;
                }
                if messages
                    .send(LegacyEvent::Stopped {
                        generation,
                        result,
                        endpoint,
                    })
                    .is_err()
                {
                    return;
                }
            }
            LegacyRequest::Probe => {
                let Some(generation) = active_generation else {
                    continue;
                };
                if host.endpoint().is_none() {
                    continue;
                }
                let message = probe_message(&mut host, generation);
                if messages.send(message).is_err() {
                    return;
                }
            }
            LegacyRequest::Shutdown { reply } => {
                let result = host.stop();
                let endpoint = host.endpoint();
                let stopped = result.is_ok();
                let _ = reply.send(LegacyShutdownReply {
                    generation: active_generation,
                    result,
                    endpoint,
                });
                if stopped {
                    return;
                }
            }
        }
    }
}

fn probe_message(host: &mut impl RuntimeHost, generation: u64) -> LegacyEvent {
    match host.probe() {
        ApiRuntimeProbe::Activity(activity) => LegacyEvent::Activity {
            generation,
            activity,
        },
        ApiRuntimeProbe::Stopped => LegacyEvent::UnexpectedStop { generation },
        ApiRuntimeProbe::CleanupFailed => match host.endpoint() {
            Some(endpoint) => LegacyEvent::ProbeCleanupFailed {
                generation,
                endpoint,
            },
            None => LegacyEvent::ControllerFailed,
        },
    }
}
