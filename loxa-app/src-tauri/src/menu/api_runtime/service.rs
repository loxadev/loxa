use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use loxa::api_runtime::ApiStartCancellation;
use loxa_ipc::{
    Accepted, ClientError, ConnectMode, ErrorCategory, OperationTarget, ReplyOutcome, RuntimePhase,
    ServiceClient, ServiceCommand, ServiceSubscription,
};

use super::worker::{RuntimeHostStart, RuntimeMessage, RuntimeRequest, RuntimeShutdownReply};
use super::{ApiEndpoint, ServiceObservation, ServiceObservationSlot};

const SNAPSHOT_WAIT: Duration = Duration::from_millis(40);
const ABSENT_RETRY: Duration = Duration::from_millis(250);

pub(super) fn run_service_runtime_worker(
    client: ServiceClient,
    disconnect: Arc<AtomicBool>,
    observation: Arc<ServiceObservationSlot>,
    requests: Receiver<RuntimeRequest>,
    messages: Sender<RuntimeMessage>,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        run_failed_worker(requests, messages);
        return;
    };

    ServiceRuntimeWorker {
        runtime,
        client,
        disconnect,
        observation,
        requests,
        messages,
        subscription: None,
        next_subscribe: Instant::now(),
        last_observed: None,
        pending: None,
        admitted_start: None,
    }
    .run();
}

fn run_failed_worker(requests: Receiver<RuntimeRequest>, messages: Sender<RuntimeMessage>) {
    if messages.send(RuntimeMessage::ControllerFailed).is_err() {
        return;
    }
    while let Ok(request) = requests.recv() {
        if let RuntimeRequest::Shutdown { reply } = request {
            let _ = reply.send(RuntimeShutdownReply {
                generation: None,
                result: Ok(()),
                endpoint: None,
            });
            return;
        }
    }
}

struct ServiceRuntimeWorker {
    runtime: tokio::runtime::Runtime,
    client: ServiceClient,
    disconnect: Arc<AtomicBool>,
    observation: Arc<ServiceObservationSlot>,
    requests: Receiver<RuntimeRequest>,
    messages: Sender<RuntimeMessage>,
    subscription: Option<ServiceSubscription>,
    next_subscribe: Instant,
    last_observed: Option<ServiceObservation>,
    pending: Option<PendingCommand>,
    admitted_start: Option<AdmittedStart>,
}

enum PendingCommand {
    Start(PendingStart),
    Stop(PendingStop),
}

struct PendingStart {
    ui_generation: u64,
    model_id: String,
    cancellation: ApiStartCancellation,
    target: OperationTarget,
    unload_sent: bool,
}

struct PendingStop {
    ui_generation: Option<u64>,
    target: OperationTarget,
}

struct AdmittedStart {
    ui_generation: u64,
    model_id: String,
    target: OperationTarget,
}

impl ServiceRuntimeWorker {
    fn run(mut self) {
        loop {
            if self.disconnected() {
                self.wait_for_shutdown();
                return;
            }

            match self.requests.try_recv() {
                Ok(RuntimeRequest::Shutdown { reply }) => {
                    self.disconnect.store(true, Ordering::Release);
                    let _ = reply.send(clean_shutdown_reply());
                    return;
                }
                Ok(request) => {
                    self.handle_request(request);
                    continue;
                }
                Err(TryRecvError::Disconnected) => return,
                Err(TryRecvError::Empty) => {}
            }

            self.advance_cancelled_start();
            if self.disconnected() {
                continue;
            }
            self.poll_subscription();
        }
    }

    fn wait_for_shutdown(&self) {
        while let Ok(request) = self.requests.recv() {
            if let RuntimeRequest::Shutdown { reply } = request {
                let _ = reply.send(clean_shutdown_reply());
                return;
            }
        }
    }

    fn handle_request(&mut self, request: RuntimeRequest) {
        match request {
            RuntimeRequest::Start {
                generation,
                model_id,
                cancellation,
            } => self.start(generation, model_id, cancellation),
            RuntimeRequest::Stop { generation, target } => self.stop(generation, target),
            RuntimeRequest::Probe => self.probe(),
            RuntimeRequest::Shutdown { reply } => {
                let _ = reply.send(clean_shutdown_reply());
            }
        }
    }

    fn start(&mut self, ui_generation: u64, model_id: String, cancellation: ApiStartCancellation) {
        if self.pending.is_some() {
            self.send_start_result(ui_generation, RuntimeHostStart::Conflict);
            return;
        }
        let request = self.client.request(
            ConnectMode::EnsureStarted,
            ServiceCommand::Load {
                model_id: model_id.clone(),
            },
        );
        let outcome = wait_interruptible(&self.runtime, &self.disconnect, request);
        let accepted = match outcome {
            Ok(Ok(ReplyOutcome::Accepted(accepted))) => accepted,
            Ok(Ok(ReplyOutcome::Status(_) | ReplyOutcome::Rejected(_))) => {
                let _ = self.messages.send(RuntimeMessage::ControllerFailed);
                return;
            }
            Ok(Err(ClientError::Rejected(error))) => {
                let failure = classify_start_rejection(error);
                self.finish_rejected_start(ui_generation, &cancellation, failure);
                return;
            }
            Ok(Err(ClientError::Absent | ClientError::Transport(_))) => {
                let _ = self.messages.send(RuntimeMessage::ControllerFailed);
                self.reconnect_now();
                return;
            }
            Err(Interrupted) => return,
        };

        let target = accepted_target(accepted);
        self.admitted_start = Some(AdmittedStart {
            ui_generation,
            model_id: model_id.clone(),
            target: target.clone(),
        });
        self.pending = Some(PendingCommand::Start(PendingStart {
            ui_generation,
            model_id,
            cancellation,
            target,
            unload_sent: false,
        }));
        self.reconnect_now();
        self.advance_cancelled_start();
    }

    fn finish_rejected_start(
        &self,
        ui_generation: u64,
        cancellation: &ApiStartCancellation,
        failure: RuntimeHostStart,
    ) {
        if cancellation.is_cancelled() {
            self.send_stopped(Some(ui_generation), Ok(()), None);
        } else {
            self.send_start_result(ui_generation, failure);
        }
    }

    fn stop(&mut self, ui_generation: Option<u64>, target: Option<OperationTarget>) {
        if let Some(PendingCommand::Start(start)) = self.pending.as_ref() {
            if Some(start.ui_generation) == ui_generation {
                start.cancellation.cancel();
                self.advance_cancelled_start();
                return;
            }
        }
        if self.pending.is_some() {
            self.send_stopped(
                ui_generation,
                Err(()),
                target
                    .as_ref()
                    .and_then(|target| self.endpoint_for_target(target)),
            );
            return;
        }
        let target = effective_stop_target(target, ui_generation, self.admitted_start.as_ref());
        let Some(target) = target else {
            self.send_stopped(ui_generation, Err(()), None);
            return;
        };

        if self.last_observed.as_ref().is_some_and(|observed| {
            cleanup_observation(&target, observed) == CleanupObservation::Complete
        }) {
            self.clear_admitted_target(&target);
            self.send_stopped(ui_generation, Ok(()), None);
            return;
        }

        match self.request_unload(target.clone()) {
            Ok(()) => {
                self.pending = Some(PendingCommand::Stop(PendingStop {
                    ui_generation,
                    target,
                }));
                self.reconnect_now();
            }
            Err(()) => {
                let endpoint = self
                    .endpoint_for_target(&target)
                    .or_else(|| self.admitted_endpoint(&target));
                self.send_stopped(ui_generation, Err(()), endpoint);
                self.reconnect_now();
            }
        }
    }

    fn probe(&mut self) {
        if let Some(observed) = self.last_observed.clone() {
            if self.observation.publish(observed).is_err() {
                let _ = self.messages.send(RuntimeMessage::ControllerFailed);
            }
        } else {
            self.reconnect_now();
        }
    }

    fn advance_cancelled_start(&mut self) {
        let Some(mut start) = take_pending_start(&mut self.pending) else {
            return;
        };
        if !start.cancellation.is_cancelled() || start.unload_sent {
            self.pending = Some(PendingCommand::Start(start));
            return;
        }

        match self.request_unload(start.target.clone()) {
            Ok(()) => {
                start.unload_sent = true;
                self.pending = Some(PendingCommand::Start(start));
                self.reconnect_now();
            }
            Err(()) => {
                let endpoint = self.endpoint_for_target(&start.target).or_else(|| {
                    Some(ApiEndpoint::for_service(
                        start.model_id.clone(),
                        start.target.clone(),
                    ))
                });
                self.send_stopped(Some(start.ui_generation), Err(()), endpoint);
                self.reconnect_now();
            }
        }
    }

    fn request_unload(&self, target: OperationTarget) -> Result<(), ()> {
        let request = self.client.request(
            ConnectMode::ObserveExisting,
            ServiceCommand::Unload { target },
        );
        match wait_interruptible(&self.runtime, &self.disconnect, request) {
            Ok(Ok(ReplyOutcome::Accepted(_))) => Ok(()),
            _ => Err(()),
        }
    }

    fn poll_subscription(&mut self) {
        if self.subscription.is_none() {
            if Instant::now() < self.next_subscribe {
                std::thread::sleep(SNAPSHOT_WAIT);
                return;
            }
            let subscribe = self.client.subscribe(ConnectMode::ObserveExisting);
            match wait_interruptible(&self.runtime, &self.disconnect, subscribe) {
                Ok(Ok(subscription)) => self.subscription = Some(subscription),
                Ok(Err(ClientError::Absent)) => {
                    self.resolve_and_emit(ServiceObservation::Absent);
                    self.next_subscribe = Instant::now() + ABSENT_RETRY;
                    return;
                }
                Ok(Err(_)) => {
                    self.resolve_and_emit(ServiceObservation::Unavailable);
                    self.next_subscribe = Instant::now() + ABSENT_RETRY;
                    return;
                }
                Err(Interrupted) => return,
            }
        }

        let result = {
            let Some(subscription) = self.subscription.as_mut() else {
                return;
            };
            self.runtime.block_on(async {
                tokio::time::timeout(SNAPSHOT_WAIT, subscription.next_snapshot()).await
            })
        };
        match result {
            Err(_) => {}
            Ok(Ok(status)) => self.resolve_and_emit(ServiceObservation::Present(status)),
            Ok(Err(_)) => {
                self.subscription = None;
                self.next_subscribe = Instant::now();
            }
        }
    }

    fn resolve_and_emit(&mut self, observed: ServiceObservation) {
        self.resolve_pending(&observed);
        self.send_observed(observed);
    }

    fn resolve_pending(&mut self, observed: &ServiceObservation) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        match pending {
            PendingCommand::Start(start) => self.resolve_start(start, observed),
            PendingCommand::Stop(stop) => self.resolve_stop(stop, observed),
        }
    }

    fn resolve_start(&mut self, start: PendingStart, observed: &ServiceObservation) {
        let expected = observed.target().as_ref() == Some(&start.target);
        if start.cancellation.is_cancelled() {
            match cleanup_observation(&start.target, observed) {
                CleanupObservation::Pending => {
                    self.pending = Some(PendingCommand::Start(start));
                }
                CleanupObservation::Complete => {
                    self.clear_admitted_target(&start.target);
                    self.send_stopped(Some(start.ui_generation), Ok(()), None);
                }
                CleanupObservation::Failed(endpoint) => {
                    let endpoint = endpoint.or_else(|| {
                        Some(ApiEndpoint::for_service(
                            start.model_id,
                            start.target.clone(),
                        ))
                    });
                    self.send_stopped(Some(start.ui_generation), Err(()), endpoint);
                }
            }
            return;
        }

        if let (true, Some(status)) = (expected, observed.status()) {
            match &status.phase {
                RuntimePhase::Starting { .. } | RuntimePhase::Stopping { .. } => {
                    self.pending = Some(PendingCommand::Start(start));
                    return;
                }
                RuntimePhase::Ready { .. } => {
                    self.send_start_result(
                        start.ui_generation,
                        RuntimeHostStart::Ready(
                            observed.endpoint().expect("ready status has an operation"),
                        ),
                    );
                    return;
                }
                RuntimePhase::CleanupFailed { .. } => {
                    self.send_start_result(
                        start.ui_generation,
                        RuntimeHostStart::CleanupFailed(
                            observed
                                .endpoint()
                                .expect("cleanup-failed status has an operation"),
                        ),
                    );
                    return;
                }
                RuntimePhase::LoadFailed { category, .. } => {
                    self.clear_admitted_target(&start.target);
                    self.send_start_result(start.ui_generation, start_failure(*category));
                    return;
                }
                RuntimePhase::Unloaded
                | RuntimePhase::RecoveryRequired { .. }
                | RuntimePhase::Draining => {}
            }
        }

        match cleanup_observation(&start.target, observed) {
            CleanupObservation::Complete if observed.target().is_some() => {
                self.clear_admitted_target(&start.target);
                self.send_start_result(start.ui_generation, RuntimeHostStart::Conflict);
            }
            CleanupObservation::Complete => {
                self.clear_admitted_target(&start.target);
                self.send_start_result(start.ui_generation, RuntimeHostStart::StartupFailed);
            }
            CleanupObservation::Failed(endpoint) => {
                let endpoint = endpoint
                    .unwrap_or_else(|| ApiEndpoint::for_service(start.model_id, start.target));
                self.send_start_result(
                    start.ui_generation,
                    RuntimeHostStart::CleanupFailed(endpoint),
                );
            }
            CleanupObservation::Pending => {
                unreachable!("matching nonterminal start states were handled above")
            }
        }
    }

    fn resolve_stop(&mut self, stop: PendingStop, observed: &ServiceObservation) {
        match cleanup_observation(&stop.target, observed) {
            CleanupObservation::Pending => {
                self.pending = Some(PendingCommand::Stop(stop));
            }
            CleanupObservation::Complete => {
                self.clear_admitted_target(&stop.target);
                self.send_stopped(stop.ui_generation, Ok(()), None);
            }
            CleanupObservation::Failed(endpoint) => {
                let endpoint = endpoint.or_else(|| self.admitted_endpoint(&stop.target));
                self.send_stopped(stop.ui_generation, Err(()), endpoint);
            }
        }
    }

    fn reconnect_now(&mut self) {
        self.subscription = None;
        self.next_subscribe = Instant::now();
    }

    fn clear_admitted_target(&mut self, target: &OperationTarget) {
        if self
            .admitted_start
            .as_ref()
            .is_some_and(|accepted| &accepted.target == target)
        {
            self.admitted_start = None;
        }
    }

    fn admitted_endpoint(&self, target: &OperationTarget) -> Option<ApiEndpoint> {
        self.admitted_start.as_ref().and_then(|accepted| {
            (&accepted.target == target).then(|| {
                ApiEndpoint::for_service(accepted.model_id.clone(), accepted.target.clone())
            })
        })
    }

    fn endpoint_for_target(&self, expected: &OperationTarget) -> Option<ApiEndpoint> {
        let observed = self.last_observed.as_ref()?;
        if observed.target().as_ref() != Some(expected) {
            return None;
        }
        observed.endpoint()
    }

    fn send_observed(&mut self, observed: ServiceObservation) {
        if self.last_observed.as_ref() == Some(&observed) {
            return;
        }
        self.last_observed = Some(observed.clone());
        if self.observation.publish(observed).is_err() {
            let _ = self.messages.send(RuntimeMessage::ControllerFailed);
        }
    }

    fn send_start_result(&self, generation: u64, outcome: RuntimeHostStart) {
        let _ = self.messages.send(RuntimeMessage::Started {
            generation,
            outcome,
        });
    }

    fn send_stopped(
        &self,
        generation: Option<u64>,
        result: Result<(), ()>,
        endpoint: Option<ApiEndpoint>,
    ) {
        let _ = self.messages.send(RuntimeMessage::Stopped {
            generation,
            result,
            endpoint,
        });
    }

    fn disconnected(&self) -> bool {
        self.disconnect.load(Ordering::Acquire)
    }
}

fn take_pending_start(pending: &mut Option<PendingCommand>) -> Option<PendingStart> {
    match pending.take() {
        Some(PendingCommand::Start(start)) => Some(start),
        retained => {
            *pending = retained;
            None
        }
    }
}

fn effective_stop_target(
    requested: Option<OperationTarget>,
    ui_generation: Option<u64>,
    admitted: Option<&AdmittedStart>,
) -> Option<OperationTarget> {
    requested.or_else(|| {
        admitted
            .filter(|accepted| Some(accepted.ui_generation) == ui_generation)
            .map(|accepted| accepted.target.clone())
    })
}

#[derive(Clone, Copy)]
struct Interrupted;

fn wait_interruptible<F>(
    runtime: &tokio::runtime::Runtime,
    disconnect: &AtomicBool,
    future: F,
) -> Result<F::Output, Interrupted>
where
    F: Future,
{
    let mut future = Box::pin(future);
    loop {
        if disconnect.load(Ordering::Acquire) {
            return Err(Interrupted);
        }
        if let Ok(output) =
            runtime.block_on(async { tokio::time::timeout(SNAPSHOT_WAIT, future.as_mut()).await })
        {
            return Ok(output);
        }
    }
}

fn clean_shutdown_reply() -> RuntimeShutdownReply {
    RuntimeShutdownReply {
        generation: None,
        result: Ok(()),
        endpoint: None,
    }
}

fn accepted_target(accepted: Accepted) -> OperationTarget {
    OperationTarget {
        boot_epoch: accepted.boot_epoch,
        task_id: accepted.task_id,
        generation: accepted.generation,
    }
}

#[derive(Debug, Eq, PartialEq)]
enum CleanupObservation {
    Pending,
    Complete,
    Failed(Option<ApiEndpoint>),
}

fn cleanup_observation(
    expected: &OperationTarget,
    observed: &ServiceObservation,
) -> CleanupObservation {
    let Some(status) = observed.status() else {
        return CleanupObservation::Failed(None);
    };
    if status.boot_epoch != expected.boot_epoch {
        return CleanupObservation::Failed(None);
    }
    let target = observed.target();
    if target.as_ref() == Some(expected) {
        return match status.phase {
            RuntimePhase::Starting { .. }
            | RuntimePhase::Ready { .. }
            | RuntimePhase::Stopping { .. } => CleanupObservation::Pending,
            RuntimePhase::CleanupFailed { .. } => CleanupObservation::Failed(observed.endpoint()),
            RuntimePhase::LoadFailed { .. } => CleanupObservation::Complete,
            RuntimePhase::Unloaded
            | RuntimePhase::RecoveryRequired { .. }
            | RuntimePhase::Draining => unreachable!("matching target requires an operation"),
        };
    }
    if matches!(status.phase, RuntimePhase::Unloaded) || target.is_some() {
        // The coordinator admits one operation at a time. A different
        // operation in the same boot epoch proves the accepted target ended.
        CleanupObservation::Complete
    } else {
        CleanupObservation::Failed(None)
    }
}

fn classify_start_rejection(error: loxa_ipc::ServiceError) -> RuntimeHostStart {
    start_failure(error.category)
}

fn start_failure(category: ErrorCategory) -> RuntimeHostStart {
    match category {
        ErrorCategory::Busy | ErrorCategory::Conflict => RuntimeHostStart::Conflict,
        ErrorCategory::NotFound | ErrorCategory::ModelUnavailable => {
            RuntimeHostStart::ModelUnavailable
        }
        _ => RuntimeHostStart::StartupFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_observation, effective_stop_target, take_pending_start, AdmittedStart,
        CleanupObservation, PendingCommand, PendingStart, PendingStop, ServiceObservation,
        ServiceRuntimeWorker,
    };
    use crate::menu::api_runtime::{RuntimeMessage, ServiceObservationSlot};
    use loxa::api_runtime::ApiStartCancellation;
    use loxa_ipc::{
        initialize_development_root, OperationTarget, RuntimePhase, RuntimeStatus, ServiceClient,
    };
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::{mpsc, Arc};
    use std::time::Instant;

    fn target(epoch: &str) -> OperationTarget {
        OperationTarget {
            boot_epoch: epoch.into(),
            task_id: "7".into(),
            generation: "11".into(),
        }
    }

    fn observed(epoch: &str, phase: RuntimePhase) -> ServiceObservation {
        ServiceObservation::Present(RuntimeStatus {
            boot_epoch: epoch.into(),
            state_revision: "1".into(),
            phase,
        })
    }

    fn ready(target: &OperationTarget) -> ServiceObservation {
        observed(
            &target.boot_epoch,
            RuntimePhase::Ready {
                task_id: target.task_id.clone(),
                generation: target.generation.clone(),
                model_id: "demo".into(),
                engine_pid: 42,
            },
        )
    }

    #[test]
    fn restarted_service_with_reused_counters_never_matches_the_accepted_target() {
        let accepted = target("epoch-a");
        let restarted = target("epoch-b");
        let observed = ready(&restarted);

        assert_ne!(observed.target().as_ref(), Some(&accepted));
    }

    #[test]
    fn cleanup_requires_same_epoch_authoritative_terminal_evidence() {
        let accepted = target("epoch-a");

        assert_eq!(
            cleanup_observation(&accepted, &ServiceObservation::Absent),
            CleanupObservation::Failed(None)
        );
        assert_eq!(
            cleanup_observation(&accepted, &observed("epoch-b", RuntimePhase::Unloaded),),
            CleanupObservation::Failed(None)
        );
        assert_eq!(
            cleanup_observation(&accepted, &observed("epoch-a", RuntimePhase::Unloaded),),
            CleanupObservation::Complete
        );
        for phase in [
            RuntimePhase::Draining,
            RuntimePhase::RecoveryRequired {
                reason: "repair required".into(),
            },
        ] {
            assert_eq!(
                cleanup_observation(&accepted, &observed("epoch-a", phase)),
                CleanupObservation::Failed(None)
            );
        }
    }

    #[test]
    fn cancelled_start_scan_preserves_an_accepted_stop_pending_state() {
        let stop_target = target("epoch-a");
        let mut pending = Some(PendingCommand::Stop(PendingStop {
            ui_generation: Some(4),
            target: stop_target.clone(),
        }));

        let extracted = take_pending_start(&mut pending);

        assert!(extracted.is_none());
        assert!(matches!(
            pending,
            Some(PendingCommand::Stop(PendingStop {
                ui_generation: Some(4),
                target,
            })) if target == stop_target
        ));
    }

    #[test]
    fn queued_stop_after_ready_uses_the_retained_accepted_target() {
        let accepted_target = target("epoch-a");
        let admitted = AdmittedStart {
            ui_generation: 4,
            model_id: "demo".into(),
            target: accepted_target.clone(),
        };

        assert_eq!(
            effective_stop_target(None, Some(4), Some(&admitted)),
            Some(accepted_target)
        );
        assert_eq!(effective_stop_target(None, Some(5), Some(&admitted)), None);
    }

    #[test]
    fn cancelled_start_subscription_loss_retains_the_accepted_target() {
        let parent = std::path::Path::new("/tmp").join(format!(
            "lms-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let parent = fs::canonicalize(parent).unwrap();
        let forbidden = parent.join("normal");
        let root = parent.join("development");
        fs::create_dir(&forbidden).unwrap();
        fs::set_permissions(&forbidden, fs::Permissions::from_mode(0o700)).unwrap();
        initialize_development_root(
            &root,
            &forbidden,
            &std::env::current_exe().unwrap(),
            "test-build",
        )
        .unwrap();
        let client = ServiceClient::load(&root, Some(&forbidden), "test-build").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_request_sender, requests) = mpsc::channel();
        let (messages, received) = mpsc::channel();
        let target = target("epoch-a");
        let mut worker = ServiceRuntimeWorker {
            runtime,
            client,
            disconnect: Arc::new(AtomicBool::new(false)),
            observation: Arc::new(ServiceObservationSlot::default()),
            requests,
            messages,
            subscription: None,
            next_subscribe: Instant::now(),
            last_observed: None,
            pending: None,
            admitted_start: Some(AdmittedStart {
                ui_generation: 4,
                model_id: "demo".into(),
                target: target.clone(),
            }),
        };
        let cancellation = ApiStartCancellation::new();
        cancellation.cancel();

        worker.resolve_start(
            PendingStart {
                ui_generation: 4,
                model_id: "demo".into(),
                cancellation,
                target: target.clone(),
                unload_sent: true,
            },
            &ServiceObservation::Unavailable,
        );

        assert!(matches!(
            received.recv().unwrap(),
            RuntimeMessage::Stopped {
                generation: Some(4),
                result: Err(()),
                endpoint: Some(endpoint),
            } if endpoint.service_target() == Some(&target)
        ));
        drop(worker);
        fs::remove_dir_all(parent).unwrap();
    }
}
