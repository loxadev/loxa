use super::state::OperationPhase;
use super::{OperationControl, OwnerCommand, OwnerExit, Shared};
use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::runner::{
    PersistentServer, PersistentStart, PersistentStartError, ValidatedManagedRuntime,
};
use crate::runtime::RuntimeOwnership;
use crate::service::intent::{self, LaunchIntent};
use loxa_ipc::{Accepted, ErrorCategory, ServiceError};
use std::io::Read;
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
    runtime_release_rx: Receiver<()>,
) {
    let completion = OwnerCompletion::new(shared.owner_exit_tx.clone());
    // Keep common ownership in this outer thread frame so unwinding the
    // operation loop cannot release the cross-process runtime exclusion before
    // the SQL/config durability barrier resolves.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        RuntimeWorker {
            shared,
            paths,
            control_dir,
            ownership: &ownership,
            runtime_handle,
            retained_runtime: None,
        }
        .run(owner_rx);
    }));
    match outcome {
        Ok(()) => {
            let _ = runtime_release_rx.recv();
            drop(ownership);
            completion.drained();
        }
        Err(payload) => {
            completion.failed();
            let _ = runtime_release_rx.recv();
            drop(ownership);
            std::panic::resume_unwind(payload);
        }
    }
}

struct RuntimeWorker<'a> {
    shared: Arc<Shared>,
    paths: AppPaths,
    control_dir: PathBuf,
    // This same owner excludes other runtimes while idle and across all loads.
    ownership: &'a RuntimeOwnership,
    runtime_handle: tokio::runtime::Handle,
    retained_runtime: Option<ValidatedManagedRuntime>,
}

impl RuntimeWorker<'_> {
    fn run(mut self, owner_rx: Receiver<OwnerCommand>) {
        loop {
            if self.shared.draining.load(Ordering::Acquire) {
                self.shared.owner_exit_tx.send_replace(OwnerExit::Quiesced);
                return;
            }
            match owner_rx.recv() {
                Ok(OwnerCommand::Load {
                    operation,
                    config,
                    accepted,
                }) => self.load(operation, config, accepted),
                Ok(OwnerCommand::Wake) => {}
                #[cfg(test)]
                Ok(OwnerCommand::PanicForTest) => panic!("injected runtime owner failure"),
                #[cfg(all(test, target_os = "macos"))]
                Ok(OwnerCommand::QueueProbeForTest(completion)) => {
                    let _ = completion.try_send(());
                }
                // No control handles remain, and recv only runs between operations.
                Err(mpsc::RecvError) => return,
            }
        }
    }

    fn cancellation_requested(&self, operation: &OperationControl) -> bool {
        self.shared.draining.load(Ordering::Acquire) || operation.cancel.load(Ordering::Acquire)
    }

    fn load(
        &mut self,
        operation: Arc<OperationControl>,
        config: crate::config::Config,
        accepted: Option<oneshot::Sender<Result<Accepted, ServiceError>>>,
    ) {
        #[cfg(all(test, target_os = "macos"))]
        self.runtime_handle
            .block_on(super::native_test_gate::pause_once(
                &self.shared.native_reload_launch_gate,
            ));
        if self.cancellation_requested(&operation)
            || !self.shared.state().launch_is_current(&operation)
        {
            self.complete(&operation, None);
            if let Some(accepted) = accepted {
                let _ = accepted.send(Err(ServiceError::new(
                    ErrorCategory::ServiceUnavailable,
                    "service began draining before the load was admitted",
                )));
            }
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
                self.complete(&operation, None);
                if let Some(accepted) = accepted {
                    let _ = accepted.send(Err(ServiceError::new(ErrorCategory::Internal, error)));
                }
                return;
            }
        };
        if let Err(error) = intent::publish(&self.control_dir, &launch_intent) {
            self.shared.state().require_recovery(&operation, &error);
            if let Some(accepted) = accepted {
                let _ = accepted.send(Err(ServiceError::new(
                    ErrorCategory::RecoveryRequired,
                    error,
                )));
            }
            return;
        }
        let accepted_start = self.shared.state().accept_start(&operation);
        let Some(starting) = accepted_start else {
            if let Some(accepted) = accepted {
                let _ = accepted.send(Err(ServiceError::new(
                    ErrorCategory::ServiceUnavailable,
                    "service began draining before the load was admitted",
                )));
            }
            self.finish_without_server(operation, launch_intent, None);
            return;
        };
        if let Some(accepted) = accepted {
            let _ = accepted.send(Ok(starting));
        }

        let result = resolve_manifest(&self.paths, &operation.model_id)
            .map_err(crate::runnable::ManagedRunnableError::ModelUnavailable)
            .and_then(|manifest| {
                crate::runnable::resolve_managed_runnable_for_service(
                    manifest,
                    &self.paths,
                    config,
                    self.retained_runtime.clone(),
                    &|| self.cancellation_requested(&operation),
                )
            });
        let runnable = match result {
            Ok(runnable) => runnable,
            Err(crate::runnable::ManagedRunnableError::CleanupFailed(error)) => {
                self.retained_runtime = None;
                tracing::error!(
                    event = "service_runtime_probe_cleanup_failed",
                    model_id = %operation.model_id,
                    failure = "cleanup_failed"
                );
                // The intent still describes a process group whose cleanup did
                // not complete. Keep it durable and block later loads until
                // service recovery has reconciled that authority.
                self.shared.state().require_recovery(&operation, &error);
                return;
            }
            Err(error) => {
                self.retained_runtime = None;
                tracing::warn!(
                    event = "service_model_admission_failed",
                    model_id = %operation.model_id,
                    failure = "model_admission"
                );
                self.finish_without_server(operation, launch_intent, managed_error(error));
                return;
            }
        };
        if self.retained_runtime.is_none() && self.paths.runtime_identity.is_bundled() {
            self.retained_runtime = runnable.managed_runtime().cloned();
        }
        let endpoint = match private_engine_endpoint(&self.control_dir, operation.generation) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                self.finish_without_server(
                    operation,
                    launch_intent,
                    Some(ErrorCategory::StartupFailed),
                );
                tracing::warn!(event = "service_engine_endpoint_failed", failure = %error);
                return;
            }
        };
        let started = crate::runner::start_service_with_ownership(
            runnable,
            self.ownership,
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
                let fingerprint = Arc::new(server.fingerprint().clone());
                let observed_context = if self.cancellation_requested(&operation) {
                    None
                } else {
                    crate::runner::service_transport::observe_context(
                        &self.runtime_handle,
                        &endpoint,
                        pid,
                        &operation.model_id,
                    )
                    .filter(|context| crate::runnable::service_context_is_supported(*context))
                };
                let ready = self.shared.state().advance(
                    &operation,
                    OperationPhase::Ready {
                        engine: super::state::EngineDescriptor {
                            pid,
                            endpoint: Arc::new(endpoint),
                        },
                        fingerprint,
                        observed_context,
                    },
                );
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
                    self.shared
                        .state()
                        .cancel_generation_for_engine_failure(&operation);
                    self.shared.state().engine_gone(&operation);
                    self.finish_without_server(operation, launch_intent, None);
                    return;
                }
                Ok(None) => std::thread::sleep(OWNER_POLL_INTERVAL),
                Err(_) => {
                    self.shared
                        .state()
                        .cancel_generation_for_engine_failure(&operation);
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
        if self.terminate(&mut server).is_ok() {
            self.shared.state().engine_gone(&operation);
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
                if self.terminate(&mut server).is_ok() {
                    self.shared.state().engine_gone(&operation);
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
            if self
                .clear_intent(&launch_intent, &mut clear_progress)
                .is_ok()
            {
                self.complete(&operation, failure);
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

    fn complete(&self, operation: &Arc<OperationControl>, failure: Option<ErrorCategory>) {
        if self.shared.state().complete(operation, failure) {
            super::history::maybe_begin_history_drain(&self.shared);
        }
    }

    fn terminate(&self, server: &mut PersistentServer) -> Result<(), String> {
        #[cfg(all(test, target_os = "macos"))]
        if self
            .shared
            .fail_next_runtime_termination
            .swap(false, Ordering::AcqRel)
        {
            return Err("injected runtime termination failure".into());
        }
        server.terminate()
    }

    fn clear_intent(
        &self,
        launch_intent: &LaunchIntent,
        progress: &mut intent::ClearProgress,
    ) -> Result<(), String> {
        #[cfg(all(test, target_os = "macos"))]
        if self
            .shared
            .fail_next_intent_clear
            .swap(false, Ordering::AcqRel)
        {
            return Err("injected launch-intent clear failure".into());
        }
        intent::clear(&self.control_dir, launch_intent, progress)
    }
}

fn private_engine_endpoint(
    control_dir: &std::path::Path,
    generation: u64,
) -> Result<PathBuf, String> {
    let mut nonce = [0_u8; loxa_ipc::ENGINE_SOCKET_NONCE_BYTES];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut nonce))
        .map_err(|error| format!("could not create private engine endpoint identity: {error}"))?;
    let mut suffix = String::with_capacity(loxa_ipc::ENGINE_SOCKET_NONCE_BYTES * 2);
    for byte in nonce {
        use std::fmt::Write as _;
        write!(&mut suffix, "{byte:02x}").expect("writing to a String is infallible");
    }
    let filename = format!("engine-{generation:016x}-{suffix}.sock");
    debug_assert_eq!(filename.len(), loxa_ipc::ENGINE_SOCKET_FILENAME_BYTES);
    Ok(control_dir.join(filename))
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
        crate::runnable::ManagedRunnableError::CleanupFailed(_) => {
            Some(ErrorCategory::RecoveryRequired)
        }
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

    fn failed(mut self) {
        self.tx.send_replace(OwnerExit::Failed);
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
