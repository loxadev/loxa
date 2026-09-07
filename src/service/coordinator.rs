use super::intent::{self, LaunchIntent};
use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::runner::{PersistentServer, PersistentStart, PersistentStartError};
use loxa_ipc::{
    Accepted, DiagnosticsStatus, ErrorCategory, OperationTarget, RuntimePhase, RuntimeStatus,
    ServiceError, ServiceStatus,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{oneshot, watch};

// A synchronous child has no event primitive to select with this owner's
// command channel; 20 ms sets a short cancellation-check cadence while ready.
const OWNER_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Clone)]
pub(super) struct Coordinator {
    shared: Arc<Shared>,
    owner_tx: SyncSender<OwnerCommand>,
    owner: Arc<Mutex<Option<JoinHandle<()>>>>,
}

struct Shared {
    boot_epoch: String,
    root_identity: String,
    machine_boot_id: String,
    projection: Projection,
    diagnostics: Option<loxa_diagnostics::DiagnosticsHealthHandle>,
    admission: Mutex<Admission>,
    draining: AtomicBool,
    owner_exit_tx: watch::Sender<OwnerExit>,
    server_stop_tx: watch::Sender<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnerExit {
    Running,
    Drained,
    Failed,
}

struct Admission {
    current: Option<Arc<OperationControl>>,
    next_task_id: u64,
    next_generation: u64,
}

struct OperationControl {
    task_id: u64,
    generation: u64,
    model_id: String,
    cancel: AtomicBool,
    retry_cleanup: AtomicU64,
}

impl OperationControl {
    fn phase_starting(&self) -> RuntimePhase {
        RuntimePhase::Starting {
            task_id: self.task_id.to_string(),
            generation: self.generation.to_string(),
            model_id: self.model_id.clone(),
        }
    }

    fn phase_ready(&self, engine_pid: u32) -> RuntimePhase {
        RuntimePhase::Ready {
            task_id: self.task_id.to_string(),
            generation: self.generation.to_string(),
            model_id: self.model_id.clone(),
            engine_pid,
        }
    }

    fn phase_stopping(&self) -> RuntimePhase {
        RuntimePhase::Stopping {
            task_id: self.task_id.to_string(),
            generation: self.generation.to_string(),
            model_id: self.model_id.clone(),
        }
    }

    fn phase_cleanup_failed(&self) -> RuntimePhase {
        RuntimePhase::CleanupFailed {
            task_id: self.task_id.to_string(),
            generation: self.generation.to_string(),
            model_id: self.model_id.clone(),
        }
    }

    fn phase_load_failed(&self, category: ErrorCategory) -> RuntimePhase {
        RuntimePhase::LoadFailed {
            task_id: self.task_id.to_string(),
            generation: self.generation.to_string(),
            model_id: self.model_id.clone(),
            category,
        }
    }
}

struct Projection {
    tx: watch::Sender<RuntimeStatus>,
}

impl Projection {
    fn new(boot_epoch: &str, initial: RuntimePhase) -> Self {
        let status = RuntimeStatus {
            boot_epoch: boot_epoch.to_owned(),
            state_revision: "0".into(),
            phase: initial,
        };
        let (tx, _) = watch::channel(status);
        Self { tx }
    }

    fn current(&self) -> RuntimeStatus {
        self.tx.borrow().clone()
    }

    fn publish(&self, phase: RuntimePhase) -> RuntimeStatus {
        let mut published = None;
        self.tx.send_modify(|status| {
            let revision = status
                .state_revision
                .parse::<u64>()
                .expect("the service owns numeric state revisions")
                .saturating_add(1);
            status.state_revision = revision.to_string();
            status.phase = phase;
            published = Some(status.clone());
        });
        published.expect("watch publication captured its new value")
    }

    fn subscribe(&self) -> watch::Receiver<RuntimeStatus> {
        self.tx.subscribe()
    }
}

fn diagnostics_status(
    diagnostics: Option<&loxa_diagnostics::DiagnosticsHealthHandle>,
) -> DiagnosticsStatus {
    let Some(health) = diagnostics.map(loxa_diagnostics::DiagnosticsHealthHandle::health) else {
        return DiagnosticsStatus {
            available: false,
            enqueue_drops: 0,
            sink_failures: 0,
            sink_discards: 0,
            at_capacity: false,
            sink_failed: false,
        };
    };
    DiagnosticsStatus {
        available: health.is_available(),
        enqueue_drops: u64::try_from(health.enqueue_drops).unwrap_or(u64::MAX),
        sink_failures: u64::try_from(health.sink_failures).unwrap_or(u64::MAX),
        sink_discards: u64::try_from(health.sink_discards).unwrap_or(u64::MAX),
        at_capacity: health.at_capacity,
        sink_failed: health.sink_failed,
    }
}

enum OwnerCommand {
    Load {
        operation: Arc<OperationControl>,
        accepted: oneshot::Sender<Result<Accepted, ServiceError>>,
    },
    Wake,
    #[cfg(test)]
    PanicForTest,
}

impl Coordinator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn start(
        paths: AppPaths,
        control_dir: PathBuf,
        root_identity: String,
        machine_boot_id: String,
        boot_epoch: String,
        ownership: crate::runtime::RuntimeOwnership,
        runtime_handle: tokio::runtime::Handle,
        initial_recovery: Option<String>,
        diagnostics: Option<loxa_diagnostics::DiagnosticsHealthHandle>,
    ) -> Result<Coordinator, String> {
        let initial = initial_recovery
            .as_deref()
            .map(recovery_phase)
            .unwrap_or(RuntimePhase::Unloaded);
        let (owner_exit_tx, _) = watch::channel(OwnerExit::Running);
        let (server_stop_tx, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            boot_epoch: boot_epoch.clone(),
            root_identity,
            machine_boot_id,
            projection: Projection::new(&boot_epoch, initial),
            diagnostics,
            admission: Mutex::new(Admission {
                current: None,
                next_task_id: 1,
                next_generation: 1,
            }),
            draining: AtomicBool::new(false),
            owner_exit_tx,
            server_stop_tx,
        });
        let (owner_tx, owner_rx) = mpsc::sync_channel(1);
        let owner_shared = Arc::clone(&shared);
        let owner = std::thread::Builder::new()
            .name("loxa-service-runtime-owner".into())
            .spawn(move || {
                let completion = OwnerCompletion::new(owner_shared.owner_exit_tx.clone());
                owner_loop(
                    owner_shared,
                    owner_rx,
                    paths,
                    control_dir,
                    ownership,
                    runtime_handle,
                );
                completion.drained();
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            shared,
            owner_tx,
            owner: Arc::new(Mutex::new(Some(owner))),
        })
    }

    pub(super) fn status(&self) -> RuntimeStatus {
        self.shared.projection.current()
    }

    pub(super) fn status_report(&self) -> ServiceStatus {
        ServiceStatus {
            runtime: self.status(),
            diagnostics: diagnostics_status(self.shared.diagnostics.as_ref()),
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<RuntimeStatus> {
        self.shared.projection.subscribe()
    }

    pub(super) fn server_stop_receiver(&self) -> watch::Receiver<bool> {
        self.shared.server_stop_tx.subscribe()
    }

    pub(super) fn owner_exit_receiver(&self) -> watch::Receiver<OwnerExit> {
        self.shared.owner_exit_tx.subscribe()
    }

    #[cfg(test)]
    pub(super) fn panic_owner_for_test(&self) {
        self.owner_tx
            .try_send(OwnerCommand::PanicForTest)
            .expect("test owner command queue must accept panic command");
    }

    pub(super) async fn load(&self, model_id: String) -> Result<Accepted, ServiceError> {
        let operation = {
            let mut admission = self.shared.admission.lock().map_err(|_| internal_error())?;
            if self.shared.draining.load(Ordering::Acquire) {
                return Err(ServiceError::new(
                    ErrorCategory::ServiceUnavailable,
                    "service is draining",
                ));
            }
            match &self.shared.projection.current().phase {
                RuntimePhase::RecoveryRequired { reason } => {
                    return Err(ServiceError::new(
                        ErrorCategory::RecoveryRequired,
                        reason.clone(),
                    ));
                }
                RuntimePhase::Unloaded | RuntimePhase::LoadFailed { .. } => {}
                _ => {
                    return Err(ServiceError::new(
                        ErrorCategory::Busy,
                        "another model operation is active",
                    ));
                }
            }
            if admission.current.is_some() {
                return Err(ServiceError::new(
                    ErrorCategory::Busy,
                    "another model operation is active",
                ));
            }
            let operation = Arc::new(OperationControl {
                task_id: admission.next_task_id,
                generation: admission.next_generation,
                model_id,
                cancel: AtomicBool::new(false),
                retry_cleanup: AtomicU64::new(0),
            });
            admission.next_task_id = admission.next_task_id.saturating_add(1);
            admission.next_generation = admission.next_generation.saturating_add(1);
            admission.current = Some(Arc::clone(&operation));
            operation
        };
        let (accepted_tx, accepted_rx) = oneshot::channel();
        if self
            .owner_tx
            .try_send(OwnerCommand::Load {
                operation: Arc::clone(&operation),
                accepted: accepted_tx,
            })
            .is_err()
        {
            self.clear_current(&operation);
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "runtime owner queue is unavailable",
            ));
        }
        accepted_rx.await.unwrap_or_else(|_| {
            self.clear_current(&operation);
            Err(ServiceError::new(
                ErrorCategory::Internal,
                "runtime owner stopped before acknowledging the load",
            ))
        })
    }

    pub(super) fn unload(&self, target: &OperationTarget) -> Result<Accepted, ServiceError> {
        let (operation, status) = {
            let admission = self.shared.admission.lock().map_err(|_| internal_error())?;
            let Some(operation) = admission.current.as_ref() else {
                return Err(ServiceError::new(
                    ErrorCategory::NotFound,
                    "no model operation is active",
                ));
            };
            if target.boot_epoch != self.shared.boot_epoch
                || target.task_id != operation.task_id.to_string()
                || target.generation != operation.generation.to_string()
            {
                return Err(ServiceError::new(
                    ErrorCategory::Conflict,
                    "unload target does not identify the active operation",
                ));
            }
            operation.cancel.store(true, Ordering::Release);
            operation.retry_cleanup.fetch_add(1, Ordering::AcqRel);
            let status = if self.shared.draining.load(Ordering::Acquire) {
                self.shared.projection.current()
            } else {
                self.shared.projection.publish(operation.phase_stopping())
            };
            (Arc::clone(operation), status)
        };
        Ok(Accepted {
            boot_epoch: self.shared.boot_epoch.clone(),
            task_id: operation.task_id.to_string(),
            generation: operation.generation.to_string(),
            state_revision: status.state_revision,
        })
    }

    pub(super) fn stop_service(&self) -> Result<Accepted, ServiceError> {
        let status = {
            let admission = self.shared.admission.lock().map_err(|_| internal_error())?;
            if let RuntimePhase::RecoveryRequired { reason } =
                self.shared.projection.current().phase
            {
                return Err(ServiceError::new(ErrorCategory::RecoveryRequired, reason));
            }
            self.begin_draining(&admission)
        };
        let _ = self.owner_tx.try_send(OwnerCommand::Wake);
        Ok(Accepted {
            boot_epoch: self.shared.boot_epoch.clone(),
            task_id: "0".into(),
            generation: "0".into(),
            state_revision: status.state_revision,
        })
    }

    pub(super) fn announce_server_stop(&self) {
        self.shared.server_stop_tx.send_replace(true);
    }

    pub(super) fn drain_after_server_failure(&self) {
        {
            let admission = self
                .shared
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.begin_draining(&admission);
        }
        let _ = self.owner_tx.try_send(OwnerCommand::Wake);
    }

    pub(super) fn join_owner(&self) -> Result<(), String> {
        let owner = self
            .owner
            .lock()
            .map_err(|_| "runtime owner join lock is poisoned".to_string())?
            .take();
        if let Some(owner) = owner {
            owner
                .join()
                .map_err(|_| "runtime owner thread panicked".to_string())?;
        }
        Ok(())
    }

    fn clear_current(&self, expected: &Arc<OperationControl>) {
        clear_current(&self.shared, expected);
    }

    fn begin_draining(&self, admission: &Admission) -> RuntimeStatus {
        self.shared.draining.store(true, Ordering::Release);
        if let Some(operation) = &admission.current {
            operation.cancel.store(true, Ordering::Release);
            operation.retry_cleanup.fetch_add(1, Ordering::AcqRel);
        }
        self.shared.projection.publish(RuntimePhase::Draining)
    }
}

struct OwnerCompletion {
    tx: watch::Sender<OwnerExit>,
    completed: bool,
}

impl OwnerCompletion {
    fn new(tx: watch::Sender<OwnerExit>) -> Self {
        Self {
            tx,
            completed: false,
        }
    }

    fn drained(mut self) {
        self.tx.send_replace(OwnerExit::Drained);
        self.completed = true;
    }
}

impl Drop for OwnerCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.tx.send_replace(OwnerExit::Failed);
        }
    }
}

fn owner_loop(
    shared: Arc<Shared>,
    owner_rx: Receiver<OwnerCommand>,
    paths: AppPaths,
    control_dir: PathBuf,
    ownership: crate::runtime::RuntimeOwnership,
    runtime_handle: tokio::runtime::Handle,
) {
    loop {
        if shared.draining.load(Ordering::Acquire) {
            drop(ownership);
            return;
        }
        match owner_rx.recv() {
            Ok(OwnerCommand::Load {
                operation,
                accepted,
            }) => run_load(
                &shared,
                &paths,
                &control_dir,
                &ownership,
                &runtime_handle,
                operation,
                accepted,
            ),
            Ok(OwnerCommand::Wake) => {}
            #[cfg(test)]
            Ok(OwnerCommand::PanicForTest) => panic!("injected runtime owner failure"),
            Err(mpsc::RecvError) => {
                shared.draining.store(true, Ordering::Release);
            }
        }
    }
}

fn run_load(
    shared: &Arc<Shared>,
    paths: &AppPaths,
    control_dir: &Path,
    ownership: &crate::runtime::RuntimeOwnership,
    runtime_handle: &tokio::runtime::Handle,
    operation: Arc<OperationControl>,
    accepted: oneshot::Sender<Result<Accepted, ServiceError>>,
) {
    if shared.draining.load(Ordering::Acquire) || operation.cancel.load(Ordering::Acquire) {
        complete_current(shared, &operation);
        let _ = accepted.send(Err(ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "service began draining before the load was admitted",
        )));
        return;
    }
    let launch_intent = match LaunchIntent::new(
        &shared.root_identity,
        &shared.machine_boot_id,
        &shared.boot_epoch,
        operation.task_id,
        operation.generation,
        &operation.model_id,
    ) {
        Ok(intent) => intent,
        Err(error) => {
            complete_current(shared, &operation);
            let _ = accepted.send(Err(ServiceError::new(ErrorCategory::Internal, error)));
            return;
        }
    };
    if let Err(error) = intent::publish(control_dir, &launch_intent) {
        clear_current_and_publish(shared, &operation, recovery_phase(&error));
        let _ = accepted.send(Err(ServiceError::new(
            ErrorCategory::RecoveryRequired,
            error,
        )));
        return;
    }
    let Some(starting) = publish_for_current(shared, &operation, operation.phase_starting(), true)
    else {
        let _ = accepted.send(Err(ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "service began draining before the load was admitted",
        )));
        finish_without_server(shared, control_dir, operation, launch_intent, None);
        return;
    };
    let _ = accepted.send(Ok(Accepted {
        boot_epoch: shared.boot_epoch.clone(),
        task_id: operation.task_id.to_string(),
        generation: operation.generation.to_string(),
        state_revision: starting.state_revision,
    }));

    let result = resolve_manifest(paths, &operation.model_id)
        .map_err(|_| Some(ErrorCategory::ModelUnavailable))
        .and_then(|manifest| {
            crate::runnable::resolve_managed_runnable_for_service(manifest, paths, &|| {
                operation.cancel.load(Ordering::Acquire) || shared.draining.load(Ordering::Acquire)
            })
            .map_err(managed_error)
        });
    let runnable = match result {
        Ok(runnable) => runnable,
        Err(error) => {
            tracing::warn!(
                event = "service_model_admission_failed",
                model_id = %operation.model_id,
                failure = "model_admission"
            );
            finish_without_server(shared, control_dir, operation, launch_intent, error);
            return;
        }
    };
    let endpoint = control_dir.join(format!("engine-{:016x}.sock", operation.generation));
    let started = crate::runner::start_service_with_ownership(
        runnable,
        ownership,
        &endpoint,
        runtime_handle,
        || operation.cancel.load(Ordering::Acquire) || shared.draining.load(Ordering::Acquire),
    );
    match started {
        Ok(PersistentStart::Ready(mut server)) => {
            let Some(pid) = server.pid() else {
                manage_cleanup_failed(shared, control_dir, operation, launch_intent, *server, 0);
                return;
            };
            if publish_for_current(shared, &operation, operation.phase_ready(pid), true).is_none() {
                let attempted_through = operation.retry_cleanup.load(Ordering::Acquire);
                if server.terminate().is_err() {
                    manage_cleanup_failed(
                        shared,
                        control_dir,
                        operation,
                        launch_intent,
                        *server,
                        attempted_through,
                    );
                } else {
                    finish_without_server(shared, control_dir, operation, launch_intent, None);
                }
                return;
            }
            manage_ready(shared, control_dir, operation, launch_intent, *server);
        }
        Ok(PersistentStart::CleanupFailed(server)) => {
            manage_cleanup_failed(shared, control_dir, operation, launch_intent, *server, 0)
        }
        Ok(PersistentStart::Stopped(_)) | Ok(PersistentStart::Interrupted(_)) => {
            finish_without_server(shared, control_dir, operation, launch_intent, None)
        }
        Err(error) => finish_without_server(
            shared,
            control_dir,
            operation,
            launch_intent,
            start_error(error),
        ),
    }
}

fn manage_ready(
    shared: &Arc<Shared>,
    control_dir: &Path,
    operation: Arc<OperationControl>,
    launch_intent: LaunchIntent,
    mut server: PersistentServer,
) {
    loop {
        if shared.draining.load(Ordering::Acquire) || operation.cancel.load(Ordering::Acquire) {
            if !shared.draining.load(Ordering::Acquire) {
                publish_for_current(shared, &operation, operation.phase_stopping(), false);
            }
            let attempted_through = operation.retry_cleanup.load(Ordering::Acquire);
            match server.terminate() {
                Ok(()) => {
                    finish_without_server(shared, control_dir, operation, launch_intent, None)
                }
                Err(_) => manage_cleanup_failed(
                    shared,
                    control_dir,
                    operation,
                    launch_intent,
                    server,
                    attempted_through,
                ),
            }
            return;
        }
        match server.poll() {
            Ok(Some(_)) => {
                finish_without_server(shared, control_dir, operation, launch_intent, None);
                return;
            }
            Ok(None) => std::thread::sleep(OWNER_POLL_INTERVAL),
            Err(_) => {
                let attempted_through = operation.retry_cleanup.load(Ordering::Acquire);
                if server.terminate().is_ok() {
                    finish_without_server(shared, control_dir, operation, launch_intent, None);
                } else {
                    manage_cleanup_failed(
                        shared,
                        control_dir,
                        operation,
                        launch_intent,
                        server,
                        attempted_through,
                    );
                }
                return;
            }
        }
    }
}

fn manage_cleanup_failed(
    shared: &Arc<Shared>,
    control_dir: &Path,
    operation: Arc<OperationControl>,
    launch_intent: LaunchIntent,
    mut server: PersistentServer,
    mut attempted_through: u64,
) {
    publish_cleanup_failed_for_current(shared, &operation);
    loop {
        let requested_through = operation.retry_cleanup.load(Ordering::Acquire);
        if requested_through != attempted_through {
            attempted_through = requested_through;
            if server.terminate().is_ok() {
                finish_without_server(shared, control_dir, operation, launch_intent, None);
                return;
            }
            publish_cleanup_failed_for_current(shared, &operation);
        }
        std::thread::sleep(OWNER_POLL_INTERVAL);
    }
}

fn finish_without_server(
    shared: &Arc<Shared>,
    control_dir: &Path,
    operation: Arc<OperationControl>,
    launch_intent: LaunchIntent,
    failure: Option<ErrorCategory>,
) {
    if failure.is_some() {
        tracing::warn!(
            event = "service_load_failed",
            model_id = %operation.model_id,
            failure = "startup"
        );
    }
    let mut attempted_through = operation.retry_cleanup.load(Ordering::Acquire);
    let mut clear_progress = intent::ClearProgress::default();
    loop {
        if intent::clear(control_dir, &launch_intent, &mut clear_progress).is_ok() {
            complete_current_as(
                shared,
                &operation,
                failure.map_or(RuntimePhase::Unloaded, |category| {
                    operation.phase_load_failed(category)
                }),
            );
            return;
        }
        publish_cleanup_failed_for_current(shared, &operation);
        loop {
            let requested_through = operation.retry_cleanup.load(Ordering::Acquire);
            if requested_through != attempted_through {
                attempted_through = requested_through;
                break;
            }
            std::thread::sleep(OWNER_POLL_INTERVAL);
        }
    }
}

fn publish_for_current(
    shared: &Shared,
    expected: &Arc<OperationControl>,
    phase: RuntimePhase,
    require_active: bool,
) -> Option<RuntimeStatus> {
    let admission = shared
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if shared.draining.load(Ordering::Acquire)
        || require_active && expected.cancel.load(Ordering::Acquire)
        || !admission
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        return None;
    }
    Some(shared.projection.publish(phase))
}

fn publish_cleanup_failed_for_current(
    shared: &Shared,
    expected: &Arc<OperationControl>,
) -> Option<RuntimeStatus> {
    let admission = shared
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    admission
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
        .then(|| shared.projection.publish(expected.phase_cleanup_failed()))
}

fn complete_current(shared: &Shared, expected: &Arc<OperationControl>) {
    complete_current_as(shared, expected, RuntimePhase::Unloaded);
}

fn complete_current_as(shared: &Shared, expected: &Arc<OperationControl>, phase: RuntimePhase) {
    let _ = clear_current_and_publish(shared, expected, phase);
}

fn clear_current_and_publish(
    shared: &Shared,
    expected: &Arc<OperationControl>,
    phase: RuntimePhase,
) -> Option<RuntimeStatus> {
    let mut admission = shared
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !admission
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        return None;
    }
    admission.current = None;
    (!shared.draining.load(Ordering::Acquire)).then(|| shared.projection.publish(phase))
}

fn clear_current(shared: &Shared, expected: &Arc<OperationControl>) {
    let mut admission = shared
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if admission
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        admission.current = None;
    }
}

fn resolve_manifest(paths: &AppPaths, model_id: &str) -> Result<Manifest, String> {
    crate::catalog::load_catalog(&paths.models)?
        .into_iter()
        .find(|manifest| manifest.id == model_id)
        .ok_or_else(|| format!("model {model_id} is not installed"))
}

fn managed_error(error: crate::runnable::ManagedRunnableError) -> Option<ErrorCategory> {
    match error {
        crate::runnable::ManagedRunnableError::Conflict => Some(ErrorCategory::Conflict),
        crate::runnable::ManagedRunnableError::Cancelled => None,
        crate::runnable::ManagedRunnableError::ModelUnavailable(_) => {
            Some(ErrorCategory::ModelUnavailable)
        }
        crate::runnable::ManagedRunnableError::StartupFailed(_) => {
            Some(ErrorCategory::StartupFailed)
        }
    }
}

fn start_error(error: PersistentStartError) -> Option<ErrorCategory> {
    match error {
        PersistentStartError::Conflict => Some(ErrorCategory::Conflict),
        PersistentStartError::Failed(_) => Some(ErrorCategory::StartupFailed),
    }
}

fn recovery_phase(reason: &str) -> RuntimePhase {
    RuntimePhase::RecoveryRequired {
        reason: ServiceError::new(ErrorCategory::RecoveryRequired, reason).context,
    }
}

fn internal_error() -> ServiceError {
    ServiceError::new(
        ErrorCategory::Internal,
        "service admission lock is poisoned",
    )
}
