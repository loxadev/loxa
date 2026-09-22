use super::{LaunchSettings, OperationControl};
use loxa_ipc::{
    Accepted, ErrorCategory, OperationTarget, RuntimePhase, RuntimeStatus, ServiceError,
    ServiceSettingsApplication,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

mod admission;
mod generation;
use admission::AdmissionRecovery;
pub(super) use admission::{AdmissionClaim, AdmissionRecoveryAction, AdmissionReservation};
pub(super) use generation::{CancellationCause, EngineDescriptor, PendingGeneration};

// All admission and publication decisions run under Shared::state. The watch
// value is an observer copy: never read it back to decide a transition.
pub(super) struct CoordinatorState {
    current: Option<Arc<OperationControl>>,
    reload: Option<ReloadReservation>,
    applied: Option<AppliedRuntime>,
    admission: Option<Arc<AdmissionReservation>>,
    pending_generations: Vec<Arc<PendingGeneration>>,
    conversation_mutations: Vec<Arc<ConversationMutationReservation>>,
    next_task_id: u64,
    next_generation: u64,
    next_pending_nonce: u128,
    revision: u64,
    phase: Phase,
    boot_epoch: String,
    // The owner reads this same signal without the state lock while blocked in
    // preparation/startup. Only transitions under the state lock set it.
    draining: Arc<AtomicBool>,
    snapshots: watch::Sender<RuntimeStatus>,
}

// Reservation precedes durable intent publication. During that interval the
// previous observer phase remains visible, but current already excludes Load.
// Terminal LoadFailed also retains its identity after releasing admission.
enum Phase {
    Unloaded,
    Operation {
        control: Arc<OperationControl>,
        phase: OperationPhase,
    },
    RecoveryRequired(String),
    Draining,
}

struct ReloadReservation {
    retiring: Arc<OperationControl>,
    successor: Arc<OperationControl>,
    active: bool,
}

struct AppliedRuntime {
    operation: Arc<OperationControl>,
    observed_context: Option<u32>,
}

#[derive(Clone)]
pub(super) enum OperationPhase {
    Starting,
    Ready {
        engine: EngineDescriptor,
        fingerprint: Arc<crate::runtime_fingerprint::RuntimeFingerprint>,
        observed_context: Option<u32>,
    },
    Stopping,
    CleanupFailed,
    LoadFailed(ErrorCategory),
}

pub(super) struct ConversationMutationReservation {
    conversation_id: [u8; 16],
}

impl CoordinatorState {
    pub(super) fn new(
        boot_epoch: String,
        recovery: Option<String>,
        draining: Arc<AtomicBool>,
    ) -> Self {
        let phase = recovery.map_or(Phase::Unloaded, |reason| {
            Phase::RecoveryRequired(
                ServiceError::new(ErrorCategory::RecoveryRequired, reason).context,
            )
        });
        let initial = RuntimeStatus {
            boot_epoch: boot_epoch.clone(),
            state_revision: "0".into(),
            phase: phase.to_wire(),
        };
        let (snapshots, _) = watch::channel(initial);
        Self {
            current: None,
            reload: None,
            applied: None,
            admission: None,
            pending_generations: Vec::with_capacity(16),
            conversation_mutations: Vec::with_capacity(8),
            next_task_id: 1,
            next_generation: 1,
            next_pending_nonce: 1,
            revision: 0,
            phase,
            boot_epoch,
            draining,
            snapshots,
        }
    }

    pub(super) fn snapshot(&self) -> RuntimeStatus {
        RuntimeStatus {
            boot_epoch: self.boot_epoch.clone(),
            state_revision: self.revision.to_string(),
            phase: self.phase.to_wire(),
        }
    }

    pub(super) fn settings_application(
        &self,
        desired_context: Option<u32>,
    ) -> ServiceSettingsApplication {
        let Some(applied) = &self.applied else {
            return ServiceSettingsApplication::NotApplied;
        };
        let launch = applied.operation.launch_settings;
        ServiceSettingsApplication::Applied {
            target: applied.operation.target(&self.boot_epoch),
            settings_revision: launch.settings_revision.to_string(),
            context_preference: launch.context_preference,
            requested_context: launch.requested_context,
            observed_context: applied.observed_context,
            runtime_build: launch.runtime_identity.build().to_owned(),
            runtime_version: launch.runtime_identity.version_line().to_owned(),
            reload_required: crate::runnable::resolve_service_context(desired_context)
                != launch.requested_context,
        }
    }

    pub(super) fn matching_admission(
        &self,
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
    ) -> Result<Option<Arc<AdmissionReservation>>, ServiceError> {
        let Some(admission) = &self.admission else {
            return Ok(None);
        };
        if admission.submission_id != submission_id {
            return Ok(None);
        }
        if admission.submission_hash != submission_hash {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "submission identity was reused with a different payload",
            ));
        }
        Ok(Some(Arc::clone(admission)))
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<RuntimeStatus> {
        self.snapshots.subscribe()
    }

    pub(super) fn reserve_load_with_settings(
        &mut self,
        model_id: String,
        launch_settings: LaunchSettings,
    ) -> Result<Arc<OperationControl>, ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if let Phase::RecoveryRequired(reason) = &self.phase {
            return Err(ServiceError::new(
                ErrorCategory::RecoveryRequired,
                reason.clone(),
            ));
        }
        if self.admission.is_some() {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "conversation output is still active or unresolved",
            ));
        }
        if self.current.is_some()
            || !matches!(
                self.phase,
                Phase::Unloaded
                    | Phase::Operation {
                        phase: OperationPhase::LoadFailed(_),
                        ..
                    }
            )
        {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "another model operation is active",
            ));
        }
        let operation = Arc::new(OperationControl {
            task_id: self.next_task_id,
            generation: self.next_generation,
            model_id,
            launch_settings,
            cancel: AtomicBool::new(false),
            retry_cleanup: AtomicU64::new(0),
        });
        self.next_task_id = self.next_task_id.saturating_add(1);
        self.next_generation = self.next_generation.saturating_add(1);
        self.current = Some(Arc::clone(&operation));
        Ok(operation)
    }

    #[cfg(test)]
    pub(super) fn reserve_load(
        &mut self,
        model_id: String,
    ) -> Result<Arc<OperationControl>, ServiceError> {
        self.reserve_load_with_settings(
            model_id,
            LaunchSettings {
                settings_revision: 0,
                context_preference: None,
                requested_context: 4096,
                runtime_identity: crate::runtime_identity::RuntimeIdentity::BundledB10344,
            },
        )
    }

    pub(super) fn reserve_reload(
        &mut self,
        target: &OperationTarget,
        launch_settings: LaunchSettings,
    ) -> Result<(Arc<OperationControl>, Arc<OperationControl>), ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if self.reload.is_some() {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "a runtime Reload is already pending",
            ));
        }
        if self.admission.is_some() {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "conversation output is still active or unresolved",
            ));
        }
        if !self.pending_generations.is_empty() {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "generation connections are still pending",
            ));
        }
        let retiring = self.current.as_ref().ok_or_else(|| {
            ServiceError::new(ErrorCategory::NotFound, "no model operation is active")
        })?;
        if !self.matches_target(retiring, target) {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "Reload target does not identify the active operation",
            ));
        }
        if retiring.cancel.load(Ordering::Acquire)
            || !matches!(
                &self.phase,
                Phase::Operation {
                    control,
                    phase: OperationPhase::Ready { .. },
                } if Arc::ptr_eq(control, retiring)
            )
        {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "the active model operation is not idle and Ready",
            ));
        }
        let retiring = Arc::clone(retiring);
        let successor = Arc::new(OperationControl {
            task_id: self.next_task_id,
            generation: self.next_generation,
            model_id: retiring.model_id.clone(),
            launch_settings,
            cancel: AtomicBool::new(false),
            retry_cleanup: AtomicU64::new(0),
        });
        self.next_task_id = self.next_task_id.saturating_add(1);
        self.next_generation = self.next_generation.saturating_add(1);
        self.reload = Some(ReloadReservation {
            retiring: Arc::clone(&retiring),
            successor: Arc::clone(&successor),
            active: false,
        });
        Ok((retiring, successor))
    }

    pub(super) fn activate_reload(
        &mut self,
        retiring: &Arc<OperationControl>,
        successor: &Arc<OperationControl>,
    ) -> Option<Accepted> {
        let matches = self.reload.as_ref().is_some_and(|reservation| {
            !reservation.active
                && Arc::ptr_eq(&reservation.retiring, retiring)
                && Arc::ptr_eq(&reservation.successor, successor)
        });
        if !matches || !self.is_current(retiring) {
            return None;
        }
        self.reload.as_mut()?.active = true;
        retiring.request_cleanup();
        self.publish(Phase::Operation {
            control: Arc::clone(retiring),
            phase: OperationPhase::Stopping,
        });
        Some(self.accepted(Some(successor)))
    }

    pub(super) fn release_reload_reservation(&mut self, successor: &Arc<OperationControl>) {
        if self.reload.as_ref().is_some_and(|reservation| {
            !reservation.active && Arc::ptr_eq(&reservation.successor, successor)
        }) {
            self.reload = None;
        }
    }

    pub(super) fn accept_start(&mut self, operation: &Arc<OperationControl>) -> Option<Accepted> {
        self.advance(operation, OperationPhase::Starting)
            .then(|| self.accepted(Some(operation)))
    }

    pub(super) fn unload(&mut self, target: &OperationTarget) -> Result<Accepted, ServiceError> {
        if self.admission.is_some() {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "a conversation admission is active",
            ));
        }
        let operation = self.current.as_ref().ok_or_else(|| {
            ServiceError::new(ErrorCategory::NotFound, "no model operation is active")
        })?;
        let matched_successor = self.reload.as_ref().and_then(|reload| {
            self.matches_target(&reload.successor, target)
                .then(|| Arc::clone(&reload.successor))
        });
        if !self.matches_target(operation, target) && matched_successor.is_none() {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "unload target does not identify the active operation",
            ));
        }
        let operation = Arc::clone(operation);
        operation.request_cleanup();
        if let Some(reload) = &self.reload {
            reload.successor.request_cleanup();
        }
        self.advance(&operation, OperationPhase::Stopping);
        Ok(self.accepted(Some(matched_successor.as_deref().unwrap_or(&operation))))
    }

    pub(super) fn stop_service(&mut self) -> Result<Accepted, ServiceError> {
        if let Phase::RecoveryRequired(reason) = &self.phase {
            return Err(ServiceError::new(
                ErrorCategory::RecoveryRequired,
                reason.clone(),
            ));
        }
        self.begin_draining();
        Ok(self.accepted(None))
    }

    pub(super) fn begin_draining(&mut self) {
        self.draining.store(true, Ordering::Release);
        if let Some(operation) = &self.current {
            operation.request_cleanup();
        }
        if let Some(reload) = &self.reload {
            reload.successor.request_cleanup();
        }
        if let Some(admission) = &self.admission {
            admission.request_cancel();
            admission.publish_current_generation_status();
        }
        for pending in &self.pending_generations {
            pending.request_cancel();
        }
        self.publish(Phase::Draining);
    }

    pub(super) fn advance(
        &mut self,
        expected: &Arc<OperationControl>,
        phase: OperationPhase,
    ) -> bool {
        if !self.is_current(expected) {
            return false;
        }
        // Stop and Ready serialize on this lock. Cleanup failure must remain
        // visible even during drain, because the owner still retains resources.
        if !matches!(phase, OperationPhase::CleanupFailed)
            && (self.draining.load(Ordering::Acquire)
                || matches!(
                    phase,
                    OperationPhase::Starting | OperationPhase::Ready { .. }
                ) && expected.cancel.load(Ordering::Acquire))
        {
            return false;
        }
        if let OperationPhase::Ready {
            observed_context, ..
        } = &phase
        {
            self.applied = Some(AppliedRuntime {
                operation: Arc::clone(expected),
                observed_context: *observed_context,
            });
        }
        self.publish(Phase::Operation {
            control: Arc::clone(expected),
            phase,
        });
        true
    }

    pub(super) fn launch_is_current(&self, expected: &Arc<OperationControl>) -> bool {
        self.is_current(expected)
            && !self.draining.load(Ordering::Acquire)
            && !expected.cancel.load(Ordering::Acquire)
    }

    pub(super) fn engine_gone(&mut self, expected: &Arc<OperationControl>) {
        if self
            .applied
            .as_ref()
            .is_some_and(|applied| Arc::ptr_eq(&applied.operation, expected))
        {
            self.applied = None;
        }
    }

    pub(super) fn complete(
        &mut self,
        expected: &Arc<OperationControl>,
        failure: Option<ErrorCategory>,
    ) -> bool {
        if !self.is_current(expected) {
            return false;
        }
        let mut admission_released = false;
        if let Some(admission) = &self.admission {
            admission.request_cancel();
            admission.publish_current_generation_status();
            if Arc::ptr_eq(&admission.operation, expected) {
                admission.mark_engine_quiescent();
                admission.publish_current_generation_status();
                let admission = Arc::clone(admission);
                admission_released = self.finish_admission_if_resolved(&admission);
            }
        }
        for pending in &self.pending_generations {
            pending.request_cancel();
        }
        let promote = self.reload.as_ref().is_some_and(|reload| {
            reload.active
                && Arc::ptr_eq(&reload.retiring, expected)
                && failure.is_none()
                && !self.draining.load(Ordering::Acquire)
                && !reload.successor.cancel.load(Ordering::Acquire)
        });
        if promote {
            let reload = self.reload.take().expect("Reload reservation is present");
            self.current = Some(reload.successor);
            self.applied = None;
            return admission_released;
        }
        if self
            .reload
            .as_ref()
            .is_some_and(|reload| Arc::ptr_eq(&reload.retiring, expected))
        {
            if let Some(reload) = self.reload.take() {
                reload.successor.request_cleanup();
            }
        }
        self.applied = None;
        let phase = failure.map_or(Phase::Unloaded, |category| Phase::Operation {
            control: Arc::clone(expected),
            phase: OperationPhase::LoadFailed(category),
        });
        self.release_and_publish(expected, phase);
        admission_released
    }

    pub(super) fn require_recovery(&mut self, expected: &Arc<OperationControl>, reason: &str) {
        if !self.is_current(expected) {
            return;
        }
        if let Some(admission) = &self.admission {
            admission.request_cancel();
            admission.publish_current_generation_status();
        }
        for pending in &self.pending_generations {
            pending.request_cancel();
        }
        if let Some(reload) = self.reload.take() {
            reload.successor.request_cleanup();
        }
        self.release_and_publish(
            expected,
            Phase::RecoveryRequired(
                ServiceError::new(ErrorCategory::RecoveryRequired, reason).context,
            ),
        );
    }

    pub(super) fn release_reservation(&mut self, expected: &Arc<OperationControl>) {
        if self.is_current(expected) {
            self.current = None;
        }
    }

    pub(super) fn reserve_conversation_mutation(
        &mut self,
        conversation_id: [u8; 16],
    ) -> Result<Arc<ConversationMutationReservation>, ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if self
            .admission
            .as_ref()
            .is_some_and(|admission| admission.conversation_id == conversation_id)
            || self
                .conversation_mutations
                .iter()
                .any(|mutation| mutation.conversation_id == conversation_id)
        {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "the conversation has an active mutation",
            ));
        }
        if self.conversation_mutations.len() == 8 {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "conversation mutation capacity is full",
            ));
        }
        let reservation = Arc::new(ConversationMutationReservation { conversation_id });
        self.conversation_mutations.push(Arc::clone(&reservation));
        Ok(reservation)
    }

    pub(super) fn finish_conversation_mutation(
        &mut self,
        reservation: &Arc<ConversationMutationReservation>,
    ) {
        self.conversation_mutations
            .retain(|current| !Arc::ptr_eq(current, reservation));
    }

    pub(super) fn history_close_is_safe(&self) -> bool {
        self.admission.is_none() && self.pending_generations.is_empty()
    }

    fn release_and_publish(&mut self, expected: &Arc<OperationControl>, phase: Phase) {
        if !self.is_current(expected) {
            return;
        }
        self.current = None;
        if !self.draining.load(Ordering::Acquire) {
            self.publish(phase);
        }
    }

    fn is_current(&self, expected: &Arc<OperationControl>) -> bool {
        self.current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    }

    fn matches_target(&self, operation: &OperationControl, target: &OperationTarget) -> bool {
        target.boot_epoch == self.boot_epoch
            && target.task_id == operation.task_id.to_string()
            && target.generation == operation.generation.to_string()
    }

    fn publish(&mut self, phase: Phase) {
        self.phase = phase;
        self.revision = self.revision.saturating_add(1);
        self.snapshots.send_replace(self.snapshot());
    }

    fn accepted(&self, operation: Option<&OperationControl>) -> Accepted {
        Accepted {
            boot_epoch: self.boot_epoch.clone(),
            task_id: operation
                .map_or(0, |operation| operation.task_id)
                .to_string(),
            generation: operation
                .map_or(0, |operation| operation.generation)
                .to_string(),
            state_revision: self.revision.to_string(),
        }
    }
}

impl Phase {
    fn to_wire(&self) -> RuntimePhase {
        match self {
            Self::Unloaded => RuntimePhase::Unloaded,
            Self::Draining => RuntimePhase::Draining,
            Self::RecoveryRequired(reason) => RuntimePhase::RecoveryRequired {
                reason: reason.clone(),
            },
            Self::Operation { control, phase } => {
                let task_id = control.task_id.to_string();
                let generation = control.generation.to_string();
                let model_id = control.model_id.clone();
                match phase {
                    OperationPhase::Starting => RuntimePhase::Starting {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::Ready { engine, .. } => RuntimePhase::Ready {
                        task_id,
                        generation,
                        model_id,
                        engine_pid: engine.pid,
                    },
                    OperationPhase::Stopping => RuntimePhase::Stopping {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::CleanupFailed => RuntimePhase::CleanupFailed {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::LoadFailed(category) => RuntimePhase::LoadFailed {
                        task_id,
                        generation,
                        model_id,
                        category: *category,
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
