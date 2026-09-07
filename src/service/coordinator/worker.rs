use super::state::OperationPhase;
use super::{OperationControl, OwnerCommand, OwnerExit, Shared};
use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::runner::{PersistentServer, PersistentStart, PersistentStartError};
use crate::runtime::RuntimeOwnership;
use crate::service::intent::{self, LaunchIntent};
use loxa_ipc::{Accepted, ErrorCategory, ServiceError};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};

// A synchronous child has no event primitive to select with this owner's
// command channel; 20 ms sets a short cancellation-check cadence while ready.
const OWNER_POLL_INTERVAL: Duration = Duration::from_millis(20);

pub(super) fn run(
    shared: Arc<Shared>,
    owner_rx: Receiver<OwnerCommand>,
    paths: AppPaths,
    control_dir: PathBuf,
    ownership: RuntimeOwnership,
    runtime_handle: tokio::runtime::Handle,
) {
    let completion = OwnerCompletion::new(shared.owner_exit_tx.clone());
    // The worker drops common runtime ownership before announcing its exit,
    // including when an engine operation unwinds through a panic.
    RuntimeWorker {
        shared,
        paths,
        control_dir,
        ownership,
        runtime_handle,
    }
    .run(owner_rx);
    completion.drained();
}

struct RuntimeWorker {
    shared: Arc<Shared>,
    paths: AppPaths,
    control_dir: PathBuf,
    // This same owner excludes other runtimes while idle and across all loads.
    ownership: RuntimeOwnership,
    runtime_handle: tokio::runtime::Handle,
}

impl RuntimeWorker {
    fn run(self, owner_rx: Receiver<OwnerCommand>) {
        loop {
            if self.shared.draining.load(Ordering::Acquire) {
                return;
            }
            match owner_rx.recv() {
                Ok(OwnerCommand::Load {
                    operation,
                    accepted,
                }) => self.load(operation, accepted),
                Ok(OwnerCommand::Wake) => {}
                #[cfg(test)]
                Ok(OwnerCommand::PanicForTest) => panic!("injected runtime owner failure"),
                // No control handles remain, and recv only runs between operations.
                Err(mpsc::RecvError) => return,
            }
        }
    }

    fn cancellation_requested(&self, operation: &OperationControl) -> bool {
        self.shared.draining.load(Ordering::Acquire) || operation.cancel.load(Ordering::Acquire)
    }

    fn load(
        &self,
        operation: Arc<OperationControl>,
        accepted: oneshot::Sender<Result<Accepted, ServiceError>>,
    ) {
        if self.cancellation_requested(&operation) {
            self.shared.state().complete(&operation, None);
            let _ = accepted.send(Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service began draining before the load was admitted",
            )));
            return;
        }
        let launch_intent = match LaunchIntent::new(
            &self.shared.root_identity,
            &self.shared.machine_boot_id,
            &self.shared.boot_epoch,
            operation.task_id,
            operation.generation,
            &operation.model_id,
        ) {
            Ok(intent) => intent,
            Err(error) => {
                self.shared.state().complete(&operation, None);
                let _ = accepted.send(Err(ServiceError::new(ErrorCategory::Internal, error)));
                return;
            }
        };
        if let Err(error) = intent::publish(&self.control_dir, &launch_intent) {
            self.shared.state().require_recovery(&operation, &error);
            let _ = accepted.send(Err(ServiceError::new(
                ErrorCategory::RecoveryRequired,
                error,
            )));
            return;
        }
        let accepted_start = self.shared.state().accept_start(&operation);
        let Some(starting) = accepted_start else {
            let _ = accepted.send(Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service began draining before the load was admitted",
            )));
            self.finish_without_server(operation, launch_intent, None);
            return;
        };
        let _ = accepted.send(Ok(starting));

        let result = resolve_manifest(&self.paths, &operation.model_id)
            .map_err(|_| Some(ErrorCategory::ModelUnavailable))
            .and_then(|manifest| {
                crate::runnable::resolve_managed_runnable_for_service(
                    manifest,
                    &self.paths,
                    &|| self.cancellation_requested(&operation),
                )
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
                self.finish_without_server(operation, launch_intent, error);
                return;
            }
        };
        let endpoint = self
            .control_dir
            .join(format!("engine-{:016x}.sock", operation.generation));
        let started = crate::runner::start_service_with_ownership(
            runnable,
            &self.ownership,
            &endpoint,
            &self.runtime_handle,
            || self.cancellation_requested(&operation),
        );
        match started {
            Ok(PersistentStart::Ready(server)) => {
                let Some(pid) = server.pid() else {
                    self.manage_cleanup_failed(operation, launch_intent, *server, 0);
                    return;
                };
                let ready = self
                    .shared
                    .state()
                    .advance(&operation, OperationPhase::Ready { engine_pid: pid });
                if ready {
                    self.manage_ready(operation, launch_intent, *server);
                } else {
                    self.stop_and_finish(operation, launch_intent, *server);
                }
            }
            Ok(PersistentStart::CleanupFailed(server)) => {
                self.manage_cleanup_failed(operation, launch_intent, *server, 0)
            }
            Ok(PersistentStart::Stopped(_)) | Ok(PersistentStart::Interrupted(_)) => {
                self.finish_without_server(operation, launch_intent, None)
            }
            Err(error) => self.finish_without_server(operation, launch_intent, start_error(error)),
        }
    }

    fn manage_ready(
        &self,
        operation: Arc<OperationControl>,
        launch_intent: LaunchIntent,
        mut server: PersistentServer,
    ) {
        loop {
            if self.cancellation_requested(&operation) {
                self.shared
                    .state()
                    .advance(&operation, OperationPhase::Stopping);
                self.stop_and_finish(operation, launch_intent, server);
                return;
            }
            match server.poll() {
                Ok(Some(_)) => {
                    self.finish_without_server(operation, launch_intent, None);
                    return;
                }
                Ok(None) => std::thread::sleep(OWNER_POLL_INTERVAL),
                Err(_) => {
                    self.stop_and_finish(operation, launch_intent, server);
                    return;
                }
            }
        }
    }

    fn stop_and_finish(
        &self,
        operation: Arc<OperationControl>,
        launch_intent: LaunchIntent,
        mut server: PersistentServer,
    ) {
        let attempted_through = operation.retry_cleanup.load(Ordering::Acquire);
        if server.terminate().is_ok() {
            self.finish_without_server(operation, launch_intent, None);
        } else {
            self.manage_cleanup_failed(operation, launch_intent, server, attempted_through);
        }
    }

    fn manage_cleanup_failed(
        &self,
        operation: Arc<OperationControl>,
        launch_intent: LaunchIntent,
        mut server: PersistentServer,
        mut attempted_through: u64,
    ) {
        self.shared
            .state()
            .advance(&operation, OperationPhase::CleanupFailed);
        loop {
            let requested_through = operation.retry_cleanup.load(Ordering::Acquire);
            if requested_through != attempted_through {
                attempted_through = requested_through;
                if server.terminate().is_ok() {
                    self.finish_without_server(operation, launch_intent, None);
                    return;
                }
                self.shared
                    .state()
                    .advance(&operation, OperationPhase::CleanupFailed);
            }
            std::thread::sleep(OWNER_POLL_INTERVAL);
        }
    }

    fn finish_without_server(
        &self,
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
            if intent::clear(&self.control_dir, &launch_intent, &mut clear_progress).is_ok() {
                self.shared.state().complete(&operation, failure);
                return;
            }
            self.shared
                .state()
                .advance(&operation, OperationPhase::CleanupFailed);
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
