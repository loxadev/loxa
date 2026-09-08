pub(super) mod worker;

use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::JoinHandle;

use loxa::api_runtime::{ApiRuntimeActivity, ApiRuntimeHost, ApiStartCancellation};
use loxa::paths::AppPaths;

use super::{
    ApiEndpoint, ApiRuntimeKind, ApiRuntimeNotice, ApiRuntimePhase, ApiRuntimeShutdownError,
    ApiRuntimeView, RuntimeState, StartOutcome,
};
use worker::{run_legacy_worker, LegacyEvent, LegacyRequest, LegacyShutdownReply};

pub(super) struct LegacyBackend {
    pub(super) requests: Option<Sender<LegacyRequest>>,
    pub(super) events: Option<Receiver<LegacyEvent>>,
    pub(super) worker: Option<JoinHandle<()>>,
    pub(super) state: RuntimeState,
    pub(super) generation: Option<u64>,
    pub(super) next_generation: u64,
    pub(super) startup_cancellation: Option<ApiStartCancellation>,
    preparation_cancellation: Option<ApiStartCancellation>,
    pub(super) probe_in_flight: bool,
    pub(super) owned_endpoint: Option<ApiEndpoint>,
}

impl LegacyBackend {
    pub(super) fn start(paths: AppPaths) -> Self {
        let preparation = ApiStartCancellation::new();
        let worker_preparation = preparation.clone();
        let mut backend = Self::assemble(|requests, events| {
            std::thread::Builder::new()
                .name("loxa-menu-api-runtime".into())
                .spawn(move || {
                    let mut host = ApiRuntimeHost::new(paths);
                    // Ordinary preparation failures can be retried by Load.
                    // The host retains cleanup failures as a recovery barrier.
                    let _ = host.prepare_runtime(&worker_preparation);
                    run_legacy_worker(Ok(host), requests, events);
                })
                .map_err(|_| ())
        });
        backend.preparation_cancellation = Some(preparation);
        backend
    }

    pub(super) fn assemble(
        spawn: impl FnOnce(Receiver<LegacyRequest>, Sender<LegacyEvent>) -> Result<JoinHandle<()>, ()>,
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
            preparation_cancellation: None,
            probe_in_flight: false,
            owned_endpoint: None,
        }
    }

    pub(super) fn view(&self) -> ApiRuntimeView<'_> {
        self.state.view(ApiRuntimeKind::Legacy, true)
    }
    #[cfg(test)]
    pub(super) fn owned_endpoint(&self) -> Option<&ApiEndpoint> {
        self.owned_endpoint.as_ref()
    }

    pub(super) fn request_start(&mut self, model_id: String) -> bool {
        if !matches!(self.state.phase, ApiRuntimePhase::Idle) {
            return false;
        }
        let generation = self.next_generation;
        let Some(next_generation) = generation.checked_add(1) else {
            self.fail();
            return false;
        };
        let cancellation = ApiStartCancellation::new();
        if !self.send(LegacyRequest::Start {
            generation,
            model_id: model_id.clone(),
            cancellation: cancellation.clone(),
        }) {
            return false;
        }
        self.next_generation = next_generation;
        self.generation = Some(generation);
        self.startup_cancellation = Some(cancellation);
        self.state.active_model_id = Some(model_id.clone());
        self.owned_endpoint = None;
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
        if let Some(cancellation) = &self.preparation_cancellation {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.startup_cancellation {
            cancellation.cancel();
        }
        if !self.send(LegacyRequest::Stop) {
            return false;
        }
        self.state.notice = None;
        self.probe_in_flight = false;
        self.state.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(super) fn request_probe(&mut self) -> bool {
        if !matches!(self.state.phase, ApiRuntimePhase::Ready { .. }) || self.probe_in_flight {
            return false;
        }
        if !self.send(LegacyRequest::Probe) {
            return false;
        }
        self.probe_in_flight = true;
        true
    }

    pub(super) fn prepare_shutdown(&mut self) -> bool {
        if let Some(cancellation) = &self.preparation_cancellation {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.startup_cancellation {
            cancellation.cancel();
        }
        if !matches!(
            self.state.phase,
            ApiRuntimePhase::Starting { .. }
                | ApiRuntimePhase::Ready { .. }
                | ApiRuntimePhase::CleanupFailed
        ) {
            return false;
        }
        self.state.notice = None;
        self.probe_in_flight = false;
        self.state.phase = ApiRuntimePhase::Stopping;
        true
    }

    pub(super) fn drain(&mut self) -> bool {
        let mut changed = false;
        let mut disconnected = false;
        while let Some(events) = &self.events {
            match events.try_recv() {
                Ok(event) => changed |= self.apply(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if disconnected && self.worker.is_some() {
            self.fail();
            return true;
        }
        changed
    }

    pub(super) fn shutdown_and_join(&mut self) -> Result<(), ApiRuntimeShutdownError> {
        if let Some(cancellation) = &self.preparation_cancellation {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.startup_cancellation {
            cancellation.cancel();
        }
        if self.worker.is_none() {
            return Ok(());
        }
        let (reply, response) = mpsc::channel();
        if !self
            .requests
            .as_ref()
            .is_some_and(|sender| sender.send(LegacyRequest::Shutdown { reply }).is_ok())
        {
            self.fail();
            return Err(ApiRuntimeShutdownError);
        }
        let Ok(LegacyShutdownReply {
            generation,
            result,
            endpoint,
        }) = response.recv()
        else {
            self.fail();
            return Err(ApiRuntimeShutdownError);
        };
        if result.is_err() {
            if let Some(endpoint) = endpoint {
                self.state.active_model_id = Some(endpoint.model_id.clone());
                self.owned_endpoint = Some(endpoint);
            }
            self.generation = generation.or(self.generation);
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

    fn send(&mut self, request: LegacyRequest) -> bool {
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

    pub(super) fn apply(&mut self, event: LegacyEvent) -> bool {
        match event {
            LegacyEvent::PreparationCleanupFailed { generation } => {
                if generation != self.generation
                    || !matches!(
                        self.state.phase,
                        ApiRuntimePhase::Idle | ApiRuntimePhase::Starting { .. }
                    )
                {
                    return false;
                }
                self.startup_cancellation = None;
                self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                self.state.phase = ApiRuntimePhase::CleanupFailed;
                true
            }
            LegacyEvent::Started {
                generation,
                outcome,
            } if matches!(&self.state.phase, ApiRuntimePhase::Starting { generation: current, .. } if *current == generation) =>
            {
                self.startup_cancellation = None;
                match outcome {
                    StartOutcome::Ready(endpoint) => {
                        self.state.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint.clone());
                        self.state.notice = None;
                        self.probe_in_flight = true;
                        self.state.phase = ApiRuntimePhase::Ready {
                            generation,
                            endpoint,
                            activity: ApiRuntimeActivity::Unknown,
                        };
                    }
                    StartOutcome::CleanupFailed(endpoint) => {
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
            LegacyEvent::Activity {
                generation,
                activity,
            } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    endpoint,
                    ..
                } = &self.state.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                let endpoint = endpoint.clone();
                self.probe_in_flight = false;
                self.state.phase = ApiRuntimePhase::Ready {
                    generation,
                    endpoint,
                    activity,
                };
                true
            }
            LegacyEvent::UnexpectedStop { generation } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    ..
                } = &self.state.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                self.clear(Some(ApiRuntimeNotice::UnexpectedStop));
                true
            }
            LegacyEvent::ProbeCleanupFailed {
                generation,
                endpoint,
            } => {
                let ApiRuntimePhase::Ready {
                    generation: current,
                    ..
                } = &self.state.phase
                else {
                    return false;
                };
                if *current != generation {
                    return false;
                }
                self.startup_cancellation = None;
                self.probe_in_flight = false;
                self.state.active_model_id = Some(endpoint.model_id.clone());
                self.owned_endpoint = Some(endpoint);
                self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                self.state.phase = ApiRuntimePhase::CleanupFailed;
                true
            }
            LegacyEvent::Stopped {
                generation,
                result,
                endpoint,
            } if matches!(self.state.phase, ApiRuntimePhase::Stopping)
                && generation == self.generation =>
            {
                self.startup_cancellation = None;
                self.probe_in_flight = false;
                if result.is_ok() {
                    self.clear(None);
                } else {
                    if let Some(endpoint) = endpoint {
                        self.state.active_model_id = Some(endpoint.model_id.clone());
                        self.owned_endpoint = Some(endpoint);
                    }
                    self.state.notice = Some(ApiRuntimeNotice::CleanupFailed);
                    self.state.phase = ApiRuntimePhase::CleanupFailed;
                }
                true
            }
            LegacyEvent::ControllerFailed => {
                self.fail();
                true
            }
            LegacyEvent::Started { .. } | LegacyEvent::Stopped { .. } => false,
        }
    }

    fn clear(&mut self, notice: Option<ApiRuntimeNotice>) {
        self.generation = None;
        self.startup_cancellation = None;
        self.probe_in_flight = false;
        self.owned_endpoint = None;
        self.state.clear(notice);
    }
    fn fail(&mut self) {
        self.startup_cancellation = None;
        self.probe_in_flight = false;
        self.state.fail();
    }
}

#[cfg(test)]
mod preparation_tests {
    use super::*;
    use std::time::{Duration, Instant};

    struct FailedPreparation(bool);

    impl worker::RuntimeHost for FailedPreparation {
        fn preparation_requires_recovery(&self) -> bool {
            self.0
        }
        fn endpoint(&self) -> Option<ApiEndpoint> {
            None
        }
        fn start(&mut self, _: &str, _: &ApiStartCancellation) -> StartOutcome {
            StartOutcome::StartupFailed
        }
        fn stop(&mut self) -> Result<(), ()> {
            self.0 = false;
            Ok(())
        }
        fn activity(&mut self) -> ApiRuntimeActivity {
            ApiRuntimeActivity::Unknown
        }
    }

    fn drain_until(backend: &mut LegacyBackend, expected: ApiRuntimePhase) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while backend.state.phase != expected {
            assert!(
                Instant::now() < deadline,
                "worker did not reach {expected:?}"
            );
            backend.drain();
            std::thread::yield_now();
        }
    }

    #[test]
    fn preparation_cleanup_failure_can_be_retried_without_a_model_endpoint() {
        for load_queued in [false, true] {
            let mut backend = LegacyBackend::assemble(|requests, events| {
                std::thread::Builder::new()
                    .spawn(move || {
                        run_legacy_worker(Ok(FailedPreparation(true)), requests, events);
                    })
                    .map_err(|_| ())
            });
            if load_queued {
                assert!(backend.request_start("demo".into()));
            }
            drain_until(&mut backend, ApiRuntimePhase::CleanupFailed);
            assert!(backend.owned_endpoint.is_none());
            assert!(backend.request_stop());
            drain_until(&mut backend, ApiRuntimePhase::Idle);
            backend.shutdown_and_join().unwrap();
        }
    }

    #[test]
    fn idle_shutdown_cancels_preparation_before_waiting_for_the_worker() {
        let preparation = ApiStartCancellation::new();
        let observed = preparation.clone();
        let mut backend = LegacyBackend::assemble(|requests, _events| {
            std::thread::Builder::new()
                .spawn(move || {
                    let LegacyRequest::Shutdown { reply } = requests.recv().unwrap() else {
                        panic!("expected shutdown");
                    };
                    assert!(observed.is_cancelled());
                    reply
                        .send(LegacyShutdownReply {
                            generation: None,
                            result: Ok(()),
                            endpoint: None,
                        })
                        .unwrap();
                })
                .map_err(|_| ())
        });
        backend.preparation_cancellation = Some(preparation);
        assert!(!backend.prepare_shutdown());
        assert!(backend
            .preparation_cancellation
            .as_ref()
            .unwrap()
            .is_cancelled());
        backend.shutdown_and_join().unwrap();
    }
}
