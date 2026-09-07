use crate::paths::AppPaths;
use loxa_ipc::{
    Accepted, DiagnosticsStatus, ErrorCategory, OperationTarget, RuntimeStatus, ServiceError,
    ServiceStatus,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use tokio::sync::{oneshot, watch};

mod state;
mod worker;
use state::CoordinatorState;

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
    state: Mutex<CoordinatorState>,
    diagnostics: Option<loxa_diagnostics::DiagnosticsHealthHandle>,
    draining: Arc<AtomicBool>,
    owner_exit_tx: watch::Sender<OwnerExit>,
    server_stop_tx: watch::Sender<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnerExit {
    Running,
    Drained,
    Failed,
}

struct OperationControl {
    task_id: u64,
    generation: u64,
    model_id: String,
    cancel: AtomicBool,
    retry_cleanup: AtomicU64,
}

impl OperationControl {
    // These signals never release admission or prove completion. They let the
    // owner cancel blocking startup and retry cleanup after an explicit request.
    fn request_cleanup(&self) {
        self.cancel.store(true, Ordering::Release);
        self.retry_cleanup.fetch_add(1, Ordering::AcqRel);
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, CoordinatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let (owner_exit_tx, _) = watch::channel(OwnerExit::Running);
        let (server_stop_tx, _) = watch::channel(false);
        let draining = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(Shared {
            boot_epoch: boot_epoch.clone(),
            root_identity,
            machine_boot_id,
            state: Mutex::new(CoordinatorState::new(
                boot_epoch,
                initial_recovery,
                Arc::clone(&draining),
            )),
            diagnostics,
            draining,
            owner_exit_tx,
            server_stop_tx,
        });
        let (owner_tx, owner_rx) = mpsc::sync_channel(1);
        let owner_shared = Arc::clone(&shared);
        let owner = std::thread::Builder::new()
            .name("loxa-service-runtime-owner".into())
            .spawn(move || {
                worker::run(
                    owner_shared,
                    owner_rx,
                    paths,
                    control_dir,
                    ownership,
                    runtime_handle,
                );
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            shared,
            owner_tx,
            owner: Arc::new(Mutex::new(Some(owner))),
        })
    }

    pub(super) fn boot_epoch(&self) -> &str {
        &self.shared.boot_epoch
    }

    pub(super) fn status(&self) -> RuntimeStatus {
        self.shared.state().snapshot()
    }

    pub(super) fn status_report(&self) -> ServiceStatus {
        ServiceStatus {
            runtime: self.status(),
            diagnostics: diagnostics_status(self.shared.diagnostics.as_ref()),
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<RuntimeStatus> {
        self.shared.state().subscribe()
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
        let operation = self
            .shared
            .state
            .lock()
            .map_err(|_| internal_error())?
            .reserve_load(model_id)?;
        let (accepted_tx, accepted_rx) = oneshot::channel();
        if self
            .owner_tx
            .try_send(OwnerCommand::Load {
                operation: Arc::clone(&operation),
                accepted: accepted_tx,
            })
            .is_err()
        {
            self.shared.state().release_reservation(&operation);
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "runtime owner queue is unavailable",
            ));
        }
        accepted_rx.await.unwrap_or_else(|_| {
            self.shared.state().release_reservation(&operation);
            Err(ServiceError::new(
                ErrorCategory::Internal,
                "runtime owner stopped before acknowledging the load",
            ))
        })
    }

    pub(super) fn unload(&self, target: &OperationTarget) -> Result<Accepted, ServiceError> {
        self.shared
            .state
            .lock()
            .map_err(|_| internal_error())?
            .unload(target)
    }

    pub(super) fn stop_service(&self) -> Result<Accepted, ServiceError> {
        let accepted = self
            .shared
            .state
            .lock()
            .map_err(|_| internal_error())?
            .stop_service()?;
        let _ = self.owner_tx.try_send(OwnerCommand::Wake);
        Ok(accepted)
    }

    pub(super) fn announce_server_stop(&self) {
        self.shared.server_stop_tx.send_replace(true);
    }

    pub(super) fn drain_after_server_failure(&self) {
        self.shared.state().begin_draining();
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
}

fn internal_error() -> ServiceError {
    ServiceError::new(
        ErrorCategory::Internal,
        "service admission lock is poisoned",
    )
}
