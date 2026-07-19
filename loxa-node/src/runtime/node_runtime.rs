use crate::bootstrap::NodePaths;
use crate::chat_history::{ChatHistoryShutdownResult, ChatHistoryWorker};
use crate::chat_routes::ChatRoutesState;
use crate::control_state::{ControlStateHandle, ControlStateWorker};
use crate::download_control::{
    DownloadControl, DownloadControlWorker, ExecutionShutdownDeadlines,
    ExecutionShutdownFailureClass, ExecutionShutdownResult,
};
use crate::runtime::{
    shutdown_failure_rank, FatalShutdown, FatalShutdownParts, PublicationGate, ShutdownDeadlines,
    ShutdownFailureClass, ShutdownResult,
};
#[cfg(test)]
use crate::PreparedPythonRunResult;
use crate::{
    LifecycleEvent, LifecycleEventSink, PreparedPythonOwnerDisposition, RunRequest, RunTermination,
};
use loxa_core::diagnostics::DiagnosticsHealth;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::gateway::{GatewayServer, GatewayState};
use loxa_core::supervisor::ManagedRun;
use loxa_protocol::{NodeId, NodeInstanceId};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn current_unix_ms() -> io::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::other("system clock precedes the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| io::Error::other("system time exceeds supported range"))
}

#[must_use]
pub(crate) struct NodeOwnerGuard {
    paths: NodePaths,
    baseline: Option<ManagedRun>,
    acquisition_recovery: Option<loxa_core::supervisor::ManagedRecoverySource>,
}

impl NodeOwnerGuard {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(paths: NodePaths, baseline: ManagedRun) -> Self {
        Self {
            paths,
            baseline: Some(baseline),
            acquisition_recovery: None,
        }
    }

    pub(crate) fn from_acquisition(
        paths: NodePaths,
        acquisition: loxa_core::supervisor::ManagedOwnerAcquisition,
    ) -> (Self, loxa_core::supervisor::ManagedScalarSource) {
        (
            Self {
                paths,
                baseline: Some(acquisition.claimed_run),
                acquisition_recovery: Some(acquisition.recovery_source),
            },
            acquisition.scalar_source,
        )
    }

    pub(crate) fn commit_acquisition(&mut self) {
        self.acquisition_recovery.take();
    }

    pub(crate) fn acquisition_recovery(
        &self,
    ) -> Option<&loxa_core::supervisor::ManagedRecoverySource> {
        self.acquisition_recovery.as_ref()
    }

    pub(crate) fn baseline(&self) -> &ManagedRun {
        self.baseline.as_ref().expect("node owner guard armed")
    }

    pub(crate) fn disarm(mut self) {
        self.baseline.take();
    }

    #[cfg(test)]
    pub(crate) fn finish(mut self) -> io::Result<loxa_core::supervisor::ChildlessFinishOutcome> {
        let outcome = if let Some(recovery) = self.acquisition_recovery.take() {
            let baseline = self.baseline.take().expect("node owner guard armed");
            loxa_core::supervisor::abort_managed_owner_acquisition(
                &self.paths.state_path,
                &baseline,
                recovery,
            )
            .map_err(crate::supervisor_error_to_io)?;
            loxa_core::supervisor::ChildlessFinishOutcome::Finished
        } else {
            let baseline = self.baseline.as_ref().expect("node owner guard armed");
            loxa_core::supervisor::finish_exact_unloaded_owner(&self.paths.state_path, baseline)
                .map_err(crate::supervisor_error_to_io)?
        };
        self.baseline.take();
        Ok(outcome)
    }

    pub(crate) fn finish_retained(
        self,
        deadline: std::time::Instant,
    ) -> Result<loxa_core::supervisor::ChildlessFinishOutcome, Box<NodeOwnerShutdownFailure>> {
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let owner = std::mem::ManuallyDrop::new(self);
        let worker = thread::Builder::new()
            .name("loxa-exact-owner-shutdown".into())
            .spawn(move || {
                struct Completion(mpsc::SyncSender<()>);
                impl Drop for Completion {
                    fn drop(&mut self) {
                        let _ = self.0.send(());
                    }
                }
                let _completion = Completion(completed_tx);
                let mut owner = owner;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let baseline = owner.baseline.as_ref().expect("node owner guard armed");
                    if let Some(recovery) = owner.acquisition_recovery.as_ref() {
                        loxa_core::supervisor::abort_managed_owner_acquisition_preserving_source(
                            &owner.paths.state_path,
                            baseline,
                            recovery,
                        )
                        .map(|()| loxa_core::supervisor::ChildlessFinishOutcome::Finished)
                    } else {
                        loxa_core::supervisor::finish_exact_unloaded_owner_until(
                            &owner.paths.state_path,
                            baseline,
                            deadline,
                        )
                    }
                }));
                match result {
                    Ok(Ok(outcome)) => {
                        owner.baseline.take();
                        owner.acquisition_recovery.take();
                        // SAFETY: the owner was disarmed and is dropped exactly once here.
                        unsafe { std::mem::ManuallyDrop::drop(&mut owner) };
                        NodeOwnerWorkerExit::Stopped(outcome)
                    }
                    Ok(Err(_)) | Err(_) => NodeOwnerWorkerExit::Retained(Box::new(owner)),
                }
            });
        let worker = worker.unwrap_or_else(|_| std::process::abort());
        if completed_rx
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .is_err()
        {
            return Err(Box::new(NodeOwnerShutdownFailure {
                diagnostic: "exact node owner shutdown deadline exceeded".into(),
                _owner: None,
                _worker: Some(std::mem::ManuallyDrop::new(worker)),
            }));
        }
        match worker.join() {
            Ok(NodeOwnerWorkerExit::Stopped(outcome)) => Ok(outcome),
            Ok(NodeOwnerWorkerExit::Retained(owner)) => Err(Box::new(NodeOwnerShutdownFailure {
                diagnostic: "exact node owner shutdown failed".into(),
                _owner: Some(*owner),
                _worker: None,
            })),
            Err(_) => Err(Box::new(NodeOwnerShutdownFailure {
                diagnostic: "exact node owner shutdown worker panicked".into(),
                _owner: None,
                _worker: None,
            })),
        }
    }
}

enum NodeOwnerWorkerExit {
    Stopped(loxa_core::supervisor::ChildlessFinishOutcome),
    Retained(Box<std::mem::ManuallyDrop<NodeOwnerGuard>>),
}

#[must_use = "node owner shutdown failure retains exact ownership"]
pub(crate) struct NodeOwnerShutdownFailure {
    diagnostic: String,
    _owner: Option<std::mem::ManuallyDrop<NodeOwnerGuard>>,
    _worker: Option<std::mem::ManuallyDrop<thread::JoinHandle<NodeOwnerWorkerExit>>>,
}

impl std::fmt::Display for NodeOwnerShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl Drop for NodeOwnerGuard {
    fn drop(&mut self) {
        let Some(baseline) = self.baseline.take() else {
            return;
        };
        if let Some(recovery) = self.acquisition_recovery.take() {
            let _ = loxa_core::supervisor::abort_managed_owner_acquisition(
                &self.paths.state_path,
                &baseline,
                recovery,
            );
        } else {
            let _ = loxa_core::supervisor::finish_exact_unloaded_owner(
                &self.paths.state_path,
                &baseline,
            );
        }
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

#[cfg(test)]
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

fn resolve_node_owner_for_shutdown(
    guard: NodeOwnerGuard,
    prepared: Option<PreparedPythonOwnerDisposition>,
    deadline: std::time::Instant,
    outcome: &mut io::Result<RunTermination>,
) -> NodeOwnerShutdownResolution {
    match prepared {
        None | Some(PreparedPythonOwnerDisposition::Restored(_)) => {
            match guard.finish_retained(deadline) {
                Ok(loxa_core::supervisor::ChildlessFinishOutcome::Finished) => {
                    NodeOwnerShutdownResolution::Released
                }
                Ok(loxa_core::supervisor::ChildlessFinishOutcome::RequestedStop) => {
                    *outcome = Ok(RunTermination::RequestedStop);
                    NodeOwnerShutdownResolution::Released
                }
                Err(failure) => NodeOwnerShutdownResolution::Retained(failure),
            }
        }
        Some(PreparedPythonOwnerDisposition::ConsumedByRequestedStop) => {
            guard.disarm();
            NodeOwnerShutdownResolution::Released
        }
        Some(PreparedPythonOwnerDisposition::RecoveryRequired) => {
            guard.disarm();
            NodeOwnerShutdownResolution::RecoveryRequired
        }
    }
}

enum NodeOwnerShutdownResolution {
    Released,
    Retained(Box<NodeOwnerShutdownFailure>),
    RecoveryRequired,
}

#[cfg(test)]
fn resolve_prepared_python_owner_for_test(
    result: PreparedPythonRunResult,
    guard: NodeOwnerGuard,
    deadline: std::time::Instant,
) -> (io::Result<RunTermination>, NodeOwnerShutdownResolution) {
    let mut outcome = result.outcome;
    let resolution =
        resolve_node_owner_for_shutdown(guard, Some(result.owner), deadline, &mut outcome);
    (outcome, resolution)
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
    pub(crate) publication_gate: PublicationGate,
    pub(crate) control: ControlStateHandle,
    pub(crate) control_worker: ControlStateWorker,
    pub(crate) health_monitor: DurableHealthMonitor,
}

pub(crate) struct DurableHealthMonitor {
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
    completion: mpsc::Receiver<()>,
    failed: Arc<AtomicBool>,
    signal_failed: Arc<AtomicBool>,
}

struct HealthCompletion(mpsc::SyncSender<()>);

impl Drop for HealthCompletion {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[must_use = "health shutdown failure retains monitor ownership"]
pub(crate) struct DurableHealthShutdownFailure {
    diagnostic: &'static str,
    _monitor: std::mem::ManuallyDrop<DurableHealthMonitor>,
}

impl std::fmt::Display for DurableHealthShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.diagnostic)
    }
}

impl std::fmt::Debug for DurableHealthShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableHealthShutdownFailure")
            .field("diagnostic", &self.diagnostic)
            .field("retains_monitor", &true)
            .finish()
    }
}

impl std::error::Error for DurableHealthShutdownFailure {}

impl DurableHealthMonitor {
    #[cfg(test)]
    fn poison_completion_for_test(&mut self) {
        let (never_complete, completion) = mpsc::sync_channel(1);
        self.completion = completion;
        std::mem::forget(never_complete);
    }

    pub(crate) fn spawn(
        control: ControlStateHandle,
        gate: PublicationGate,
        gateway: GatewayState,
        state_path: std::path::PathBuf,
        owner_identity: loxa_core::supervisor::ManagedRunIdentity,
    ) -> io::Result<Self> {
        let (stop, stopped) = mpsc::channel();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let signal_failed = Arc::new(AtomicBool::new(false));
        let worker_signal_failed = Arc::clone(&signal_failed);
        let (completion_tx, completion) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("loxa-durable-health".into())
            .spawn(move || {
                let _completion = HealthCompletion(completion_tx);
                loop {
                    if !control.is_healthy() {
                        worker_failed.store(true, Ordering::Release);
                        gate.close();
                        gateway.withdraw();
                        if !matches!(
                            loxa_core::supervisor::signal_exact_managed_stop(
                                &state_path,
                                &owner_identity,
                            ),
                            Ok(true)
                        ) {
                            worker_signal_failed.store(true, Ordering::Release);
                        }
                        break;
                    }
                    match stopped.recv_timeout(Duration::from_millis(10)) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                }
            })?;
        Ok(Self {
            stop,
            worker: Some(worker),
            completion,
            failed,
            signal_failed,
        })
    }

    pub(crate) fn stop_and_join_until(
        mut self,
        deadline: std::time::Instant,
    ) -> Result<bool, DurableHealthShutdownFailure> {
        let _ = self.stop.send(());
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if self.completion.recv_timeout(remaining).is_err() {
            return Err(DurableHealthShutdownFailure {
                diagnostic: "durable health monitor shutdown deadline exceeded",
                _monitor: std::mem::ManuallyDrop::new(self),
            });
        }
        let worker = self
            .worker
            .take()
            .expect("durable health monitor worker present");
        if worker.join().is_err() {
            return Err(DurableHealthShutdownFailure {
                diagnostic: "durable health monitor panicked",
                _monitor: std::mem::ManuallyDrop::new(self),
            });
        }
        if self.signal_failed.load(Ordering::Acquire) {
            return Ok(true);
        }
        Ok(self.failed.load(Ordering::Acquire))
    }

    pub(crate) fn request_shutdown(&self) {
        let _ = self.stop.send(());
    }

    fn failure_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.failed)
    }
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
    publication_gate: PublicationGate,
    control: Option<ControlStateHandle>,
    control_worker: Option<ControlStateWorker>,
    health_monitor: Option<DurableHealthMonitor>,
    prepared_owner_disposition: Option<PreparedPythonOwnerDisposition>,
    #[cfg(test)]
    injected_missed_completion: Option<InjectedRetainedOwner>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InjectedRetainedOwner {
    Gateway,
    Routes,
    History,
    Health,
    Execution,
    Control,
    ExactOwner,
}

impl NodeRuntime {
    #[cfg(test)]
    pub(crate) fn shutdown_with_injected_retained_for_test(
        mut self,
        injected: InjectedRetainedOwner,
    ) -> ShutdownResult {
        match injected {
            InjectedRetainedOwner::Gateway => self
                .gateway
                .as_ref()
                .expect("runtime gateway present")
                .poison_completion_for_test(),
            InjectedRetainedOwner::Routes => self
                .chat_routes_state
                .as_ref()
                .expect("runtime routes present")
                .poison_shutdown_registry_for_test(),
            InjectedRetainedOwner::History => self
                .history_worker
                .as_mut()
                .expect("runtime history present")
                .poison_completion_for_test(),
            InjectedRetainedOwner::Health => self
                .health_monitor
                .as_mut()
                .expect("runtime health present")
                .poison_completion_for_test(),
            InjectedRetainedOwner::Execution => self
                .download_runtime
                .as_ref()
                .expect("runtime execution present")
                .0
                .poison_scheduler_completion_for_test(),
            InjectedRetainedOwner::Control => self.poison_control_for_test(),
            InjectedRetainedOwner::ExactOwner => {
                let state_path = &self
                    .paths
                    .as_ref()
                    .expect("runtime paths present")
                    .state_path;
                let lock_path = state_path.with_file_name("managed.json.v2.lock");
                let lock = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(lock_path)
                    .expect("runtime state lock exists");
                lock.lock().expect("lock exact owner state");
                std::mem::forget(lock);
            }
        }
        self.injected_missed_completion = Some(injected);
        self.shutdown_services(Ok(RunTermination::Interrupted))
    }

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
            publication_gate: parts.publication_gate,
            control: Some(parts.control),
            control_worker: Some(parts.control_worker),
            health_monitor: Some(parts.health_monitor),
            prepared_owner_disposition: None,
            #[cfg(test)]
            injected_missed_completion: None,
        }
    }

    pub(crate) fn port(&self) -> u16 {
        self.gateway
            .as_ref()
            .expect("runtime gateway present")
            .port()
    }

    #[cfg(test)]
    pub(crate) fn control_snapshot_for_test(
        &self,
    ) -> std::sync::Arc<crate::control_state::state_machine::CommittedState> {
        self.control
            .as_ref()
            .expect("runtime control handle present")
            .read_snapshot()
            .expect("runtime durable snapshot available")
    }

    #[cfg(test)]
    pub(crate) fn shutdown_for_test(mut self) -> ShutdownResult {
        self.shutdown_services(Ok(RunTermination::Interrupted))
    }

    #[cfg(test)]
    pub(crate) fn poison_control_for_test(&self) {
        self.control
            .as_ref()
            .expect("runtime control handle present")
            .poison_for_test();
    }

    #[cfg(test)]
    pub(crate) fn publication_gate_is_open_for_test(&self) -> bool {
        self.publication_gate.is_open()
    }

    #[cfg(test)]
    pub(crate) fn gateway_state_for_test(&self) -> GatewayState {
        self.gateway_state
            .as_ref()
            .expect("runtime gateway state present")
            .clone()
    }

    pub(crate) fn run(mut self, events: &mut dyn LifecycleEventSink) -> ShutdownResult {
        let outcome = if let Err(error) = events.emit(LifecycleEvent::NodeListening {
            port: self.port(),
            model_alias: "loxa".to_string(),
        }) {
            Err(error)
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
                let run = self
                    .owner_guard
                    .as_ref()
                    .expect("stable llama node owner guarded")
                    .baseline();
                let (download_control, download_worker) = self
                    .download_runtime
                    .as_ref()
                    .expect("stable llama node has model control");
                crate::monitor_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    Some(model_id),
                    self.control.as_ref(),
                    Some(events),
                )
            }
            Some(model_id) => {
                let durable_interrupt = self
                    .health_monitor
                    .as_ref()
                    .expect("runtime durable health monitor present")
                    .failure_signal();
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
                    Some(durable_interrupt.as_ref()),
                );
                self.prepared_owner_disposition = Some(result.owner);
                result.outcome
            }
            None => {
                let run = self
                    .owner_guard
                    .as_ref()
                    .expect("unloaded node owner guarded")
                    .baseline();
                let (download_control, download_worker) = self
                    .download_runtime
                    .as_ref()
                    .expect("unloaded node has download control");
                crate::monitor_stable_node_actor(
                    paths,
                    run,
                    Some(download_control),
                    download_worker,
                    None,
                    self.control.as_ref(),
                    Some(events),
                )
            }
        }
    }

    fn shutdown_services(&mut self, mut outcome: io::Result<RunTermination>) -> ShutdownResult {
        let shutdown_started = std::time::Instant::now();
        let deadlines = ShutdownDeadlines::from_started(shutdown_started);
        #[cfg(test)]
        let injected_missed_completion = self.injected_missed_completion.take();
        #[cfg(test)]
        let missed_deadline = |owner| {
            if injected_missed_completion == Some(owner) {
                shutdown_started
            } else {
                deadlines.repository
            }
        };
        let mut diagnostics: Vec<(ShutdownFailureClass, String)> = Vec::new();
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
        let stopping_now = current_unix_ms();
        let (stopping_commit, stopping_commit_uncertain) = match stopping_now {
            Ok(now) => match self
                .control
                .as_ref()
                .expect("runtime control handle present")
                .begin_stopping_blocking_until(now, deadlines.admission)
            {
                Ok(_) => (Ok(()), false),
                Err(_) => (
                    Err(io::Error::other("durable stopping commit failed")),
                    true,
                ),
            },
            Err(error) => (Err(error), false),
        };
        if let Some((control, _)) = &self.download_runtime {
            control.seal_for_shutdown();
        }
        if let Some((_, worker)) = &self.download_runtime {
            worker.request_shutdown(deadlines.lifecycle);
        }
        if let Some(routes) = &self.chat_routes_state {
            if let Err(error) = routes.request_shutdown() {
                diagnostics.push((
                    ShutdownFailureClass::Routes,
                    format!("chat routes stop signal failed: {error:?}"),
                ));
            }
        }
        if let Some(gateway) = &mut self.gateway {
            gateway.request_shutdown();
        }
        if let Some(history) = &mut self.history_worker {
            if !history.request_shutdown() {
                diagnostics.push((
                    ShutdownFailureClass::DurableRepository,
                    "chat history stop signal queue was full".into(),
                ));
            }
        }
        if let Some(health) = &self.health_monitor {
            health.request_shutdown();
        }
        self.publication_gate.close();
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
        let (execution_retained, execution_failed) = match self.download_runtime.take() {
            Some((control, worker)) => match worker.shutdown_staged(
                control,
                ExecutionShutdownDeadlines {
                    verification: deadlines.verification,
                    download: deadlines.download,
                    lifecycle: deadlines.lifecycle,
                    finalize: deadlines.repository,
                },
            ) {
                ExecutionShutdownResult::Stopped => (None, None),
                ExecutionShutdownResult::Failed(summary) => {
                    for message in summary.messages() {
                        diagnostics.push((
                            match summary.primary() {
                                ExecutionShutdownFailureClass::ExactChild => {
                                    ShutdownFailureClass::ExactChild
                                }
                                ExecutionShutdownFailureClass::Artifact => {
                                    ShutdownFailureClass::Artifact
                                }
                                ExecutionShutdownFailureClass::DurableRepository => {
                                    ShutdownFailureClass::DurableRepository
                                }
                                ExecutionShutdownFailureClass::Lifecycle => {
                                    ShutdownFailureClass::Lifecycle
                                }
                                ExecutionShutdownFailureClass::Download => {
                                    ShutdownFailureClass::Download
                                }
                                ExecutionShutdownFailureClass::Verification => {
                                    ShutdownFailureClass::Verification
                                }
                            },
                            message.clone(),
                        ));
                    }
                    (None, Some(summary))
                }
                ExecutionShutdownResult::Retained(retained) => {
                    let summary = retained.diagnostics();
                    for message in summary.messages() {
                        diagnostics.push((
                            match summary.primary() {
                                ExecutionShutdownFailureClass::ExactChild => {
                                    ShutdownFailureClass::ExactChild
                                }
                                ExecutionShutdownFailureClass::Artifact => {
                                    ShutdownFailureClass::Artifact
                                }
                                ExecutionShutdownFailureClass::DurableRepository => {
                                    ShutdownFailureClass::DurableRepository
                                }
                                ExecutionShutdownFailureClass::Lifecycle => {
                                    ShutdownFailureClass::Lifecycle
                                }
                                ExecutionShutdownFailureClass::Download => {
                                    ShutdownFailureClass::Download
                                }
                                ExecutionShutdownFailureClass::Verification => {
                                    ShutdownFailureClass::Verification
                                }
                            },
                            message.clone(),
                        ));
                    }
                    (Some(retained), None)
                }
            },
            None => (None, None),
        };
        let routes_failure = self
            .chat_routes_state
            .take()
            .expect("runtime chat routes state present")
            .shutdown_until({
                #[cfg(test)]
                {
                    if injected_missed_completion == Some(InjectedRetainedOwner::Routes) {
                        shutdown_started
                    } else {
                        deadlines.signal
                    }
                }
                #[cfg(not(test))]
                {
                    deadlines.signal
                }
            })
            .err();
        if let Some(error) = &routes_failure {
            diagnostics.push((ShutdownFailureClass::Routes, error.to_string()));
        }
        let mut owner_recovery_required = false;
        let owner_failure = match self.owner_guard.take() {
            Some(owner) => match resolve_node_owner_for_shutdown(
                owner,
                self.prepared_owner_disposition.take(),
                {
                    #[cfg(test)]
                    {
                        missed_deadline(InjectedRetainedOwner::ExactOwner)
                    }
                    #[cfg(not(test))]
                    {
                        deadlines.repository
                    }
                },
                &mut outcome,
            ) {
                NodeOwnerShutdownResolution::Released => None,
                NodeOwnerShutdownResolution::Retained(failure) => {
                    diagnostics.push((ShutdownFailureClass::ExactChild, failure.to_string()));
                    Some(failure)
                }
                NodeOwnerShutdownResolution::RecoveryRequired => {
                    owner_recovery_required = true;
                    diagnostics.push((
                        ShutdownFailureClass::ExactChild,
                        "prepared Python owner requires recovery".into(),
                    ));
                    None
                }
            },
            None => None,
        };
        let health_shutdown = self
            .health_monitor
            .take()
            .expect("runtime durable health monitor present")
            .stop_and_join_until({
                #[cfg(test)]
                {
                    missed_deadline(InjectedRetainedOwner::Health)
                }
                #[cfg(not(test))]
                {
                    deadlines.repository
                }
            });
        let (health_failure, health_was_unavailable) = match health_shutdown {
            Ok(failed) => (None, failed),
            Err(failure) => {
                diagnostics.push((ShutdownFailureClass::Routes, failure.to_string()));
                (Some(failure), false)
            }
        };
        if health_was_unavailable {
            diagnostics.push((
                ShutdownFailureClass::DurableRepository,
                "durable control state became unavailable".into(),
            ));
        }
        let unloaded_run = self.unloaded_run.take();
        emit_shutdown_stage(
            "chat_cancel_wait",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            None,
        );
        let gateway_failure = self
            .gateway
            .take()
            .expect("runtime gateway present")
            .shutdown_until({
                #[cfg(test)]
                {
                    missed_deadline(InjectedRetainedOwner::Gateway)
                }
                #[cfg(not(test))]
                {
                    deadlines.repository
                }
            })
            .err();
        if let Some(error) = &gateway_failure {
            diagnostics.push((
                ShutdownFailureClass::Routes,
                format!("gateway shutdown failed: {:?}", error.kind()),
            ));
        }
        emit_shutdown_stage(
            "gateway_join",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            None,
        );
        let (history_failure, history_error) = match self
            .history_worker
            .take()
            .expect("runtime history worker present")
            .shutdown_until({
                #[cfg(test)]
                {
                    missed_deadline(InjectedRetainedOwner::History)
                }
                #[cfg(not(test))]
                {
                    deadlines.repository
                }
            }) {
            ChatHistoryShutdownResult::Stopped => (None, None),
            ChatHistoryShutdownResult::Failed(error) => {
                let error = io::Error::other(error);
                diagnostics.push((ShutdownFailureClass::DurableRepository, error.to_string()));
                (None, Some(error))
            }
            ChatHistoryShutdownResult::Retained(failure) => {
                diagnostics.push((ShutdownFailureClass::DurableRepository, failure.to_string()));
                (Some(failure), None)
            }
        };
        emit_shutdown_stage(
            "history_join",
            u64::try_from(shutdown_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.node_id,
            self.node_instance_id,
            history_error.as_ref(),
        );
        let control = self.control.take().expect("runtime control handle present");
        let control_failure = self
            .control_worker
            .take()
            .expect("runtime control worker present")
            .shutdown_blocking_until({
                #[cfg(test)]
                {
                    missed_deadline(InjectedRetainedOwner::Control)
                }
                #[cfg(not(test))]
                {
                    deadlines.repository
                }
            })
            .err();
        if let Some(error) = &control_failure {
            diagnostics.push((
                ShutdownFailureClass::DurableRepository,
                format!(
                    "durable control worker shutdown failed: {:?}",
                    error.error()
                ),
            ));
        }
        if let Err(error) = &stopping_commit {
            diagnostics.push((ShutdownFailureClass::DurableRepository, error.to_string()));
        }
        let requires_exit = execution_retained.is_some()
            || routes_failure.is_some()
            || gateway_failure.is_some()
            || history_failure.is_some()
            || health_failure.is_some()
            || control_failure.is_some()
            || owner_failure.is_some()
            || owner_recovery_required
            || stopping_commit_uncertain;
        let result_class = if outcome.is_err() || !diagnostics.is_empty() {
            "failed"
        } else {
            "stopped"
        };
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
        if requires_exit {
            if let Err(error) = &outcome {
                diagnostics.push((
                    ShutdownFailureClass::OrdinaryCancellation,
                    error.to_string(),
                ));
            }
            diagnostics.sort_by_key(|(class, _)| shutdown_failure_rank(*class));
            let diagnostic = diagnostics
                .iter()
                .map(|(_, message)| message.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return ShutdownResult::RequiresProcessExit(Box::new(FatalShutdown::new(
                FatalShutdownParts {
                    diagnostic,
                    gateway: None,
                    gateway_failure,
                    history: None,
                    history_failure,
                    health: None,
                    health_failure,
                    execution: execution_retained,
                    control_failure,
                    control_startup_failure: None,
                    control: Some(control),
                    unloaded_run,
                    publication: Some(self.publication_gate.clone()),
                    owner: None,
                    owner_failure,
                    routes: None,
                    routes_failure,
                    gateway_state: self.gateway_state.take(),
                },
            )));
        }
        drop(unloaded_run);
        drop(control);
        if let Some(summary) = execution_failed {
            let message = summary.messages().join("; ");
            return ShutdownResult::Failed(io::Error::other(message));
        }
        if let Some((_, message)) = diagnostics
            .into_iter()
            .min_by_key(|(class, _)| shutdown_failure_rank(*class))
        {
            return ShutdownResult::Failed(io::Error::other(message));
        }
        match outcome {
            Ok(termination) => ShutdownResult::Stopped(termination),
            Err(error) => ShutdownResult::Failed(error),
        }
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        let owns_runtime = self.owner_guard.is_some()
            || self.download_runtime.is_some()
            || self.gateway.is_some()
            || self.history_worker.is_some()
            || self.control_worker.is_some()
            || self.health_monitor.is_some();
        if !owns_runtime {
            return;
        }
        match self.shutdown_services(Err(io::Error::other(
            "node runtime dropped without explicit shutdown",
        ))) {
            ShutdownResult::RequiresProcessExit(fatal) => (*fatal).exit(1),
            #[cfg(test)]
            _ if std::thread::panicking() && self.stable_llama_node => {
                crate::record_stable_runtime_panic_cleanup_for_test();
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        emit_shutdown_stage, resolve_prepared_python_owner_for_test, resolve_shutdown_outcome,
        NodeOwnerGuard, NodeOwnerShutdownResolution,
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
    fn acquisition_guard_abort_consumes_the_sealed_exact_absence_source() {
        let root = std::env::temp_dir().join(format!(
            "loxa-acquisition-owner-guard-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        std::fs::create_dir_all(root.join("logs")).unwrap();
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        let owner_pid = std::process::id();
        let owner_start = loxa_core::supervisor::process_start_time_with_retry(owner_pid).unwrap();
        let candidate = ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "acquired-owner".into(),
            model_id: None,
            owner_pid,
            owner_process_start_time_unix_s: owner_start,
            stop_requested: false,
            lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: "loxa-acquired-owner-g0".into(),
            control_port: Some(19_742),
            port: 19_742,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        let acquisition = loxa_core::supervisor::acquire_managed_owner(
            &paths.state_path,
            candidate,
            loxa_core::supervisor::ScalarCaptureMode::FirstMigration,
        )
        .unwrap();
        let (guard, scalar) = NodeOwnerGuard::from_acquisition(paths.clone(), acquisition);
        assert_eq!(scalar, loxa_core::supervisor::ManagedScalarSource::Fresh);
        match guard.finish_retained(std::time::Instant::now() + std::time::Duration::from_secs(2)) {
            Ok(_) => {}
            Err(failure) => panic!("acquisition rollback failed: {failure}"),
        }

        assert_eq!(
            crate::managed_servers(&paths).unwrap(),
            crate::ManagedRunsSnapshot::Runs(Vec::new())
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acquisition_guard_abort_failure_cannot_fall_back_to_looser_drop_cleanup() {
        let root = std::env::temp_dir().join(format!(
            "loxa-acquisition-mismatch-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        std::fs::create_dir_all(root.join("logs")).unwrap();
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        let owner_pid = std::process::id();
        let owner_start = loxa_core::supervisor::process_start_time_with_retry(owner_pid).unwrap();
        let candidate = ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "mismatched-owner".into(),
            model_id: None,
            owner_pid,
            owner_process_start_time_unix_s: owner_start,
            stop_requested: false,
            lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: "loxa-mismatched-owner-g0".into(),
            control_port: Some(19_743),
            port: 19_743,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        let acquisition = loxa_core::supervisor::acquire_managed_owner(
            &paths.state_path,
            candidate,
            loxa_core::supervisor::ScalarCaptureMode::FirstMigration,
        )
        .unwrap();
        let (guard, _) = NodeOwnerGuard::from_acquisition(paths.clone(), acquisition);
        let mut changed = guard.baseline().clone();
        changed.stop_requested = true;
        loxa_core::supervisor::update_runtime_state_run(
            &paths.state_path,
            &guard.baseline().identity(),
            changed.clone(),
        )
        .unwrap();

        assert!(guard.finish().is_err());
        assert_eq!(
            loxa_core::supervisor::read_runtime_state(&paths.state_path).unwrap(),
            loxa_core::supervisor::RuntimeStateRead::Loaded(vec![changed])
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acquisition_guard_deadline_retains_the_recovery_worker_until_completion() {
        let root = std::env::temp_dir().join(format!(
            "loxa-acquisition-retained-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        std::fs::create_dir_all(root.join("logs")).unwrap();
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        let owner_pid = std::process::id();
        let owner_start = loxa_core::supervisor::process_start_time_with_retry(owner_pid).unwrap();
        let candidate = ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "retained-acquired-owner".into(),
            model_id: None,
            owner_pid,
            owner_process_start_time_unix_s: owner_start,
            stop_requested: false,
            lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: "loxa-retained-acquired-owner-g0".into(),
            control_port: Some(19_744),
            port: 19_744,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        let acquisition = loxa_core::supervisor::acquire_managed_owner(
            &paths.state_path,
            candidate,
            loxa_core::supervisor::ScalarCaptureMode::FirstMigration,
        )
        .unwrap();
        let (guard, _) = NodeOwnerGuard::from_acquisition(paths.clone(), acquisition);

        let lock_path = paths.state_path.with_file_name("managed.json.v2.lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        lock.lock().unwrap();
        let failure = guard
            .finish_retained(std::time::Instant::now() + std::time::Duration::from_millis(25))
            .expect_err("blocked acquisition rollback must retain its owned worker");
        assert!(failure.to_string().contains("deadline exceeded"));

        lock.unlock().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if crate::managed_servers(&paths).unwrap()
                == crate::ManagedRunsSnapshot::Runs(Vec::new())
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "retained acquisition rollback worker did not finish after lock release"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        drop(failure);
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
        let (result, resolution) = resolve_prepared_python_owner_for_test(
            PreparedPythonRunResult {
                outcome: Ok(RunTermination::Interrupted),
                owner: PreparedPythonOwnerDisposition::Restored(baseline.clone()),
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        );
        let result = result.expect("restored owner finishes explicitly");

        assert!(matches!(resolution, NodeOwnerShutdownResolution::Released));
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

        let (result, resolution) = resolve_prepared_python_owner_for_test(
            classified,
            NodeOwnerGuard::new(paths.clone(), baseline),
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        );
        let result = result.expect("late stop remains observable");

        assert!(matches!(resolution, NodeOwnerShutdownResolution::Released));
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

        let (result, resolution) = resolve_prepared_python_owner_for_test(
            PreparedPythonRunResult {
                outcome: Ok(RunTermination::RequestedStop),
                owner: PreparedPythonOwnerDisposition::ConsumedByRequestedStop,
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        );
        let result = result.expect("consumed stop remains terminal");

        assert!(matches!(resolution, NodeOwnerShutdownResolution::Released));
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

        let (outcome, resolution) = resolve_prepared_python_owner_for_test(
            PreparedPythonRunResult {
                outcome: Err(io::Error::other("triggering runtime error")),
                owner: PreparedPythonOwnerDisposition::RecoveryRequired,
            },
            NodeOwnerGuard::new(paths.clone(), baseline),
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        );

        assert!(outcome.is_err());
        assert!(matches!(
            resolution,
            NodeOwnerShutdownResolution::RecoveryRequired
        ));
        assert_eq!(
            loxa_core::supervisor::read_runtime_state(&paths.state_path).unwrap(),
            loxa_core::supervisor::RuntimeStateRead::Loaded(vec![recovery])
        );
        std::fs::remove_dir_all(paths.state_path.parent().unwrap()).unwrap();
    }

    #[test]
    fn prepared_restored_owner_deadline_moves_real_cleanup_into_fatal_subprocess() {
        const CHILD_ENV: &str = "LOXA_TEST_PREPARED_OWNER_DEADLINE";
        if std::env::var_os(CHILD_ENV).is_some() {
            let (paths, baseline) = guarded_owner("prepared-retained", 19_765);
            let lock_path = paths.state_path.with_file_name("managed.json.v2.lock");
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(lock_path)
                .unwrap();
            lock.lock().unwrap();
            std::mem::forget(lock);
            let (_outcome, resolution) = resolve_prepared_python_owner_for_test(
                PreparedPythonRunResult {
                    outcome: Ok(RunTermination::Interrupted),
                    owner: PreparedPythonOwnerDisposition::Restored(baseline.clone()),
                },
                NodeOwnerGuard::new(paths, baseline),
                std::time::Instant::now(),
            );
            let NodeOwnerShutdownResolution::Retained(owner_failure) = resolution else {
                panic!("blocked prepared owner cleanup must retain its owned worker");
            };
            crate::runtime::FatalShutdown::new(crate::runtime::FatalShutdownParts {
                diagnostic: "prepared Python owner cleanup deadline exceeded".into(),
                gateway: None,
                gateway_failure: None,
                history: None,
                history_failure: None,
                health: None,
                health_failure: None,
                execution: None,
                control_failure: None,
                control_startup_failure: None,
                control: None,
                unloaded_run: None,
                publication: Some(crate::runtime::PublicationGate::default()),
                owner: None,
                owner_failure: Some(owner_failure),
                routes: None,
                routes_failure: None,
                gateway_state: None,
            })
            .exit(17);
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("runtime::node_runtime::tests::prepared_restored_owner_deadline_moves_real_cleanup_into_fatal_subprocess")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(17));
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
