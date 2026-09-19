use crate::config::{SettingsExit, SettingsOwner};
use crate::history::{HistoryError, HistoryErrorKind, HistoryExit, HistoryHandle, HistoryOwner};
use crate::paths::AppPaths;
use loxa_ipc::{
    Accepted, DiagnosticsStatus, DraftCommand, DraftReply, ErrorCategory, HistoryCommand,
    HistoryReply, HistoryStatus, OperationTarget, RuntimeStatus, ServiceError,
    ServiceSettingsCommand, ServiceSettingsReply, ServiceStatus,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit};

mod state;
mod worker;
use state::CoordinatorState;
mod generation;
mod history;
#[cfg(all(test, target_os = "macos"))]
mod native_test_gate;
#[cfg(all(test, target_os = "macos"))]
pub(in crate::service) use generation::run_bundled_generation_acceptance;

pub(super) struct PendingGenerationConnection {
    coordinator: Coordinator,
    pending: Arc<state::PendingGeneration>,
}

impl PendingGenerationConnection {
    pub(super) fn nonce(&self) -> &str {
        self.pending.nonce()
    }
}

impl Drop for PendingGenerationConnection {
    fn drop(&mut self) {
        self.coordinator
            .shared
            .state()
            .finish_pending_generation(&self.pending);
        history::maybe_begin_history_drain(&self.coordinator.shared);
    }
}

#[derive(Clone)]
pub(super) struct Coordinator {
    shared: Arc<Shared>,
    owner_tx: SyncSender<OwnerCommand>,
    owner: Arc<Mutex<Option<JoinHandle<()>>>>,
    history_owner: Arc<HistoryOwner>,
    settings_owner: Arc<SettingsOwner>,
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
    history: HistoryHandle,
    settings: Arc<SettingsOwner>,
    runtime_identity: crate::runtime_identity::RuntimeIdentity,
    runtime_release: Mutex<Option<SyncSender<()>>>,
    #[cfg(test)]
    admission_dispatch_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    admission_completion_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    admission_lookup_barrier: Mutex<Option<(Arc<std::sync::Barrier>, usize)>>,
    #[cfg(test)]
    admission_pre_lookup_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    output_handoff_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(all(test, target_os = "macos"))]
    native_admission_completion_gate: Mutex<Option<Arc<native_test_gate::NativeTestGate>>>,
    #[cfg(all(test, target_os = "macos"))]
    native_generation_execution_gate: Mutex<Option<Arc<native_test_gate::NativeTestGate>>>,
    #[cfg(all(test, target_os = "macos"))]
    native_generation_output_gate: Mutex<Option<Arc<native_test_gate::NativeTestGate>>>,
    #[cfg(test)]
    settings_drain_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnerExit {
    Running,
    Quiesced,
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
        config: crate::config::Config,
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
        // History admission and runtime admission close on the same monotonic
        // flag. A history request that observes false is already admitted;
        // every request that observes the StopService store is rejected.
        let draining = Arc::new(AtomicBool::new(false));
        let settings_owner = Arc::new(SettingsOwner::open(&paths.config)?);
        let history_owner = Arc::new(
            HistoryOwner::start(
                &paths.root,
                paths.models.clone(),
                paths.runtime_identity,
                Arc::clone(&draining),
                boot_epoch.clone(),
            )
            .map_err(|error| error.context().to_owned())?,
        );
        let history = history_owner.handle();
        let (owner_exit_tx, _) = watch::channel(OwnerExit::Running);
        let (server_stop_tx, _) = watch::channel(false);
        let (runtime_release_tx, runtime_release_rx) = mpsc::sync_channel(1);
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
            history,
            settings: Arc::clone(&settings_owner),
            runtime_identity: paths.runtime_identity,
            runtime_release: Mutex::new(Some(runtime_release_tx)),
            #[cfg(test)]
            admission_dispatch_barrier: Mutex::new(None),
            #[cfg(test)]
            admission_completion_barrier: Mutex::new(None),
            #[cfg(test)]
            admission_lookup_barrier: Mutex::new(None),
            #[cfg(test)]
            admission_pre_lookup_barrier: Mutex::new(None),
            #[cfg(test)]
            output_handoff_barrier: Mutex::new(None),
            #[cfg(all(test, target_os = "macos"))]
            native_admission_completion_gate: Mutex::new(None),
            #[cfg(all(test, target_os = "macos"))]
            native_generation_execution_gate: Mutex::new(None),
            #[cfg(all(test, target_os = "macos"))]
            native_generation_output_gate: Mutex::new(None),
            #[cfg(test)]
            settings_drain_barrier: Mutex::new(None),
        });
        let (owner_tx, owner_rx) = mpsc::sync_channel(1);
        let owner_shared = Arc::clone(&shared);
        let owner = match std::thread::Builder::new()
            .name("loxa-service-runtime-owner".into())
            .spawn(move || {
                worker::run(
                    owner_shared,
                    owner_rx,
                    paths,
                    control_dir,
                    ownership,
                    runtime_handle,
                    runtime_release_rx,
                );
            }) {
            Ok(owner) => owner,
            Err(error) => {
                history_owner.handle().begin_drain();
                settings_owner.begin_drain();
                let history_cleanup = history_owner
                    .join()
                    .map_err(|cleanup| cleanup.context().to_owned());
                let settings_cleanup = settings_owner.join();
                return match (history_cleanup, settings_cleanup) {
                    (Ok(()), Ok(())) => Err(error.to_string()),
                    (history, settings) => Err(format!(
                        "{error}; owner cleanup also failed: history={history:?}, settings={settings:?}"
                    )),
                };
            }
        };
        Ok(Self {
            shared,
            owner_tx,
            owner: Arc::new(Mutex::new(Some(owner))),
            history_owner,
            settings_owner,
        })
    }

    pub(super) fn boot_epoch(&self) -> &str {
        &self.shared.boot_epoch
    }

    pub(super) fn register_generation_connection(
        &self,
    ) -> Result<PendingGenerationConnection, ServiceError> {
        let pending = self.shared.state().register_pending_generation()?;
        Ok(PendingGenerationConnection {
            coordinator: self.clone(),
            pending,
        })
    }

    pub(super) fn finish_generation_connection(&self, pending: &PendingGenerationConnection) {
        self.shared
            .state()
            .finish_pending_generation(&pending.pending);
        history::maybe_begin_history_drain(&self.shared);
    }

    pub(super) fn stop_generation(
        &self,
        target: &loxa_ipc::GenerationTarget,
    ) -> Result<(), ServiceError> {
        let admission = {
            let mut state = self.shared.state();
            state.cancel_generation(target)?
        };
        if let Some(admission) = admission {
            history::maybe_resume_admission(Arc::clone(&self.shared), admission);
        }
        Ok(())
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

    pub(super) fn history_exit_receiver(&self) -> watch::Receiver<HistoryExit> {
        self.shared.history.exit_receiver()
    }

    pub(super) fn settings_exit_receiver(&self) -> watch::Receiver<SettingsExit> {
        self.shared.settings.exit_receiver()
    }

    pub(super) fn finish_settings_write(&self) -> Result<(), ServiceError> {
        self.shared.settings.finish_write()
    }

    pub(super) async fn settings(
        &self,
        command: ServiceSettingsCommand,
    ) -> (
        Result<ServiceSettingsReply, ServiceError>,
        Option<OwnedSemaphorePermit>,
    ) {
        match command {
            ServiceSettingsCommand::GetServiceSettings => (
                Ok(ServiceSettingsReply::Service(
                    self.shared.settings.snapshot(),
                )),
                None,
            ),
            ServiceSettingsCommand::PatchServiceSettings {
                expected_revision,
                patch,
            } => {
                let observer = {
                    // Stop and settings admission share this lock. Once Stop
                    // publishes the monotonic draining flag, no patch can enter
                    // the settings owner before its private drain begins.
                    let _admission = self.shared.state();
                    if self.shared.draining.load(Ordering::Acquire) {
                        return (
                            Err(ServiceError::new(
                                ErrorCategory::ServiceUnavailable,
                                "service is draining",
                            )),
                            None,
                        );
                    }
                    match self.shared.settings.patch(&expected_revision, patch) {
                        Ok(observer) => observer,
                        Err(error) => return (Err(error), None),
                    }
                };
                (
                    await_settings(observer)
                        .await
                        .map(ServiceSettingsReply::Service),
                    None,
                )
            }
            ServiceSettingsCommand::RetryServiceSettingsSave => {
                let observer = match self.shared.settings.retry() {
                    Ok(observer) => observer,
                    Err(error) => return (Err(error), None),
                };
                (
                    await_settings(observer)
                        .await
                        .map(ServiceSettingsReply::Service),
                    None,
                )
            }
            ServiceSettingsCommand::GetConversationProfile { conversation_id } => {
                let operation = ServiceSettingsCommand::GetConversationProfile { conversation_id };
                let completion = match self.shared.history.execute_profile(operation, None).await {
                    Ok(completion) => completion,
                    Err(error) => return (Err(history_error(error)), None),
                };
                (
                    completion
                        .result
                        .map(ServiceSettingsReply::Conversation)
                        .map_err(history_error),
                    Some(completion.permit),
                )
            }
            ServiceSettingsCommand::PatchConversationProfile {
                conversation_id,
                expected_conversation_revision,
                expected_profile_revision,
                patch,
            } => {
                let id = match decode_conversation_id(&conversation_id) {
                    Ok(id) => id,
                    Err(error) => return (Err(error), None),
                };
                let reset_default = if matches!(patch, loxa_ipc::GenerationSettingsPatch::Reset) {
                    match self.shared.settings.capture_generation() {
                        Ok(generation) => Some(generation),
                        Err(error) => return (Err(error), None),
                    }
                } else {
                    None
                };
                let reservation = match self.shared.state().reserve_conversation_mutation(id) {
                    Ok(reservation) => reservation,
                    Err(error) => return (Err(error), None),
                };
                let operation = ServiceSettingsCommand::PatchConversationProfile {
                    conversation_id,
                    expected_conversation_revision,
                    expected_profile_revision,
                    patch,
                };
                let completion = self
                    .shared
                    .history
                    .execute_profile(operation, reset_default)
                    .await;
                self.shared
                    .state()
                    .finish_conversation_mutation(&reservation);
                match completion {
                    Ok(completion) => (
                        completion
                            .result
                            .map(ServiceSettingsReply::Conversation)
                            .map_err(history_error),
                        Some(completion.permit),
                    ),
                    Err(error) => (Err(history_error(error)), None),
                }
            }
        }
    }

    pub(super) fn history_status(&self) -> HistoryStatus {
        self.shared.history.status()
    }

    pub(super) fn history_is_ready(&self) -> bool {
        self.history_status().is_ready()
    }

    pub(super) async fn history(
        &self,
        command: HistoryCommand,
    ) -> (
        Result<HistoryReply, ServiceError>,
        Option<OwnedSemaphorePermit>,
    ) {
        if matches!(command, HistoryCommand::GetHistoryStatus) {
            return (Ok(HistoryReply::Status(self.history_status())), None);
        }
        let mutation = match conversation_mutation_target(&command) {
            Ok(Some(conversation_id)) => match self
                .shared
                .state()
                .reserve_conversation_mutation(conversation_id)
            {
                Ok(reservation) => Some(reservation),
                Err(error) => return (Err(error), None),
            },
            Ok(None) => None,
            Err(error) => return (Err(error), None),
        };
        let generation = if matches!(command, HistoryCommand::CreateConversation { .. }) {
            match self.shared.settings.capture_generation() {
                Ok(generation) => Some(generation),
                Err(error) => return (Err(error), None),
            }
        } else {
            None
        };
        let completion = match self
            .shared
            .history
            .execute_with_generation(command, generation)
            .await
        {
            Ok(completion) => completion,
            Err(error) => {
                if let Some(reservation) = &mutation {
                    self.shared
                        .state()
                        .finish_conversation_mutation(reservation);
                }
                return (Err(history_error(error)), None);
            }
        };
        if let Some(reservation) = &mutation {
            self.shared
                .state()
                .finish_conversation_mutation(reservation);
        }
        (
            completion.result.map_err(history_error),
            Some(completion.permit),
        )
    }

    pub(super) async fn draft(
        &self,
        command: DraftCommand,
    ) -> (
        Result<DraftReply, ServiceError>,
        Option<OwnedSemaphorePermit>,
    ) {
        let completion = match self.shared.history.execute_draft(command).await {
            Ok(completion) => completion,
            Err(error) => return (Err(history_error(error)), None),
        };
        (
            completion.result.map_err(history_error),
            Some(completion.permit),
        )
    }

    #[cfg(test)]
    pub(super) fn panic_owner_for_test(&self) {
        self.owner_tx
            .try_send(OwnerCommand::PanicForTest)
            .expect("test owner command queue must accept panic command");
    }

    #[cfg(test)]
    pub(super) fn stall_history_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        self.shared.history.stall(barrier);
    }

    #[cfg(test)]
    pub(super) fn fail_next_history_close_for_test(&self) {
        self.shared.history.fail_next_close();
    }

    #[cfg(test)]
    pub(super) fn set_admission_commit_barrier_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        self.shared.history.set_admission_commit_barrier(barrier);
    }

    #[cfg(test)]
    pub(super) fn drop_next_admission_reply_for_test(&self) {
        self.shared.history.drop_next_admission_reply();
    }

    #[cfg(test)]
    pub(super) fn fail_next_stop_before_execution_for_test(&self) {
        self.shared.history.fail_next_stop_before_execution();
    }

    #[cfg(test)]
    pub(super) fn drop_next_persistence_reply_for_test(&self) {
        self.shared.history.drop_next_persistence_reply();
    }

    #[cfg(test)]
    pub(super) fn history_ordinary_available_for_test(&self) -> usize {
        self.shared.history.ordinary_available()
    }

    pub(super) async fn load(&self, model_id: String) -> Result<Accepted, ServiceError> {
        let config = self.shared.settings.capture_config()?;
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
                config,
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
        let mut state = self.shared.state.lock().map_err(|_| internal_error())?;
        let accepted = state.stop_service()?;
        let close_history = state.history_close_is_safe();
        let admission = state.current_admission();
        drop(state);
        let _ = self.owner_tx.try_send(OwnerCommand::Wake);
        if let Some(admission) = admission {
            history::maybe_resume_admission(Arc::clone(&self.shared), admission);
        }
        if close_history {
            self.shared.history.begin_drain();
        }
        #[cfg(test)]
        if let Some(barrier) = self
            .shared
            .settings_drain_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            barrier.wait();
            barrier.wait();
        }
        self.shared.settings.begin_drain();
        Ok(accepted)
    }

    #[cfg(test)]
    pub(super) fn set_admission_dispatch_barrier_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .shared
            .admission_dispatch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn stall_next_settings_write_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        self.shared.settings.stall_next(barrier);
    }

    #[cfg(test)]
    pub(super) fn stall_settings_drain_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .shared
            .settings_drain_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn set_admission_completion_barrier_for_test(
        &self,
        barrier: Arc<std::sync::Barrier>,
    ) {
        *self
            .shared
            .admission_completion_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn set_admission_lookup_barrier_for_test(
        &self,
        barrier: Arc<std::sync::Barrier>,
        participants: usize,
    ) {
        *self
            .shared
            .admission_lookup_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((barrier, participants));
    }

    #[cfg(test)]
    pub(super) fn set_admission_pre_lookup_barrier_for_test(
        &self,
        barrier: Arc<std::sync::Barrier>,
    ) {
        *self
            .shared
            .admission_pre_lookup_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn set_output_handoff_barrier_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .shared
            .output_handoff_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn set_history_progress_interval_for_test(&self, instructions: i32) {
        self.shared.history.set_progress_interval(instructions);
    }

    #[cfg(test)]
    fn force_ready_for_history_test(
        &self,
        fingerprint: Arc<crate::runtime_fingerprint::RuntimeFingerprint>,
        engine: state::EngineDescriptor,
    ) -> Arc<OperationControl> {
        let mut state = self.shared.state();
        let operation = state
            .reserve_load(fingerprint.model_id().to_owned())
            .expect("history test runtime reservation");
        state
            .accept_start(&operation)
            .expect("history test runtime start");
        assert!(state.advance(
            &operation,
            state::OperationPhase::Ready {
                engine,
                fingerprint,
            },
        ));
        operation
    }

    #[cfg(test)]
    pub(super) fn admission_active_for_test(&self) -> bool {
        self.shared.state().current_admission().is_some()
    }

    #[cfg(test)]
    pub(super) fn admission_stop_retry_ready_for_test(&self) -> bool {
        self.shared
            .state()
            .current_admission()
            .is_some_and(|admission| admission.stop_retry_ready())
    }

    pub(super) fn announce_server_stop(&self) {
        self.shared.server_stop_tx.send_replace(true);
    }

    pub(super) fn drain_after_server_failure(&self) {
        let close_history = {
            let mut state = self.shared.state();
            state.begin_draining();
            state.history_close_is_safe()
        };
        let _ = self.owner_tx.try_send(OwnerCommand::Wake);
        if close_history {
            self.shared.history.begin_drain();
        }
        self.shared.settings.begin_drain();
    }

    pub(super) fn release_runtime_if_durable(&self) {
        if !matches!(
            *self.shared.owner_exit_tx.borrow(),
            OwnerExit::Quiesced | OwnerExit::Failed
        ) || *self.shared.history.exit_receiver().borrow() != HistoryExit::Drained
            || *self.shared.settings.exit_receiver().borrow() != SettingsExit::Drained
        {
            return;
        }
        let sender = self
            .shared
            .runtime_release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(sender) = sender {
            let _ = sender.try_send(());
        }
    }

    pub(super) fn join_owner(&self) -> Result<(), String> {
        self.shared.history.begin_drain();
        self.shared.settings.begin_drain();
        let history_result = self
            .history_owner
            .join()
            .map_err(|error| error.context().to_owned());
        let settings_result = self.settings_owner.join();
        self.release_runtime_if_durable();
        let owner = self
            .owner
            .lock()
            .map_err(|_| "runtime owner join lock is poisoned".to_string())?
            .take();
        let runtime_result = owner.map_or(Ok(()), |owner| {
            owner
                .join()
                .map_err(|_| "runtime owner thread panicked".to_string())
        });
        runtime_result.and(history_result).and(settings_result)
    }
}

async fn await_settings(
    mut observer: crate::config::SettingsObserver,
) -> Result<loxa_ipc::ServiceSettings, ServiceError> {
    loop {
        if let Some(result) = observer.borrow_and_update().clone() {
            return result;
        }
        observer.changed().await.map_err(|_| {
            ServiceError::new(
                ErrorCategory::Internal,
                "settings owner stopped before acknowledging persistence",
            )
        })?;
    }
}

fn history_error(error: HistoryError) -> ServiceError {
    let category = match error.kind() {
        HistoryErrorKind::NotFound => ErrorCategory::NotFound,
        HistoryErrorKind::Conflict => ErrorCategory::Conflict,
        HistoryErrorKind::Busy => ErrorCategory::Busy,
        HistoryErrorKind::LimitExceeded | HistoryErrorKind::InvalidInput => {
            ErrorCategory::InvalidRequest
        }
        HistoryErrorKind::UnsupportedSchema
        | HistoryErrorKind::UnsafePath
        | HistoryErrorKind::Corrupt => ErrorCategory::RecoveryRequired,
        HistoryErrorKind::ReadOnly
        | HistoryErrorKind::DiskFull
        | HistoryErrorKind::Io
        | HistoryErrorKind::Interrupted
        | HistoryErrorKind::WorkerUnavailable
        | HistoryErrorKind::OutcomeUnknown => ErrorCategory::ServiceUnavailable,
    };
    ServiceError::new(category, error.context())
}

fn internal_error() -> ServiceError {
    ServiceError::new(
        ErrorCategory::Internal,
        "service admission lock is poisoned",
    )
}

fn conversation_mutation_target(
    command: &HistoryCommand,
) -> Result<Option<[u8; 16]>, ServiceError> {
    let value = match command {
        HistoryCommand::RenameConversation {
            conversation_id, ..
        }
        | HistoryCommand::DeleteConversation {
            conversation_id, ..
        } => conversation_id,
        _ => return Ok(None),
    };
    decode_conversation_id(value).map(Some)
}

fn decode_conversation_id(value: &str) -> Result<[u8; 16], ServiceError> {
    if value.len() != 32 {
        return Err(ServiceError::new(
            ErrorCategory::InvalidRequest,
            "invalid conversation identity",
        ));
    }
    let mut id = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or_else(|| {
            ServiceError::new(
                ErrorCategory::InvalidRequest,
                "invalid conversation identity",
            )
        })?;
        let low = hex_nibble(pair[1]).ok_or_else(|| {
            ServiceError::new(
                ErrorCategory::InvalidRequest,
                "invalid conversation identity",
            )
        })?;
        id[index] = (high << 4) | low;
    }
    Ok(id)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
