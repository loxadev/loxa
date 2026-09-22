use super::OperationControl;
use loxa_ipc::{
    Accepted, ErrorCategory, OperationTarget, RuntimePhase, RuntimeStatus, ServiceError,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::history::OutputState;

mod generation;
pub(super) use generation::{CancellationCause, EngineDescriptor, PendingGeneration};

// All admission and publication decisions run under Shared::state. The watch
// value is an observer copy: never read it back to decide a transition.
pub(super) struct CoordinatorState {
    current: Option<Arc<OperationControl>>,
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

#[derive(Clone)]
pub(super) enum OperationPhase {
    Starting,
    Ready {
        engine: EngineDescriptor,
        fingerprint: Arc<crate::runtime_fingerprint::RuntimeFingerprint>,
    },
    Stopping,
    CleanupFailed,
    LoadFailed(ErrorCategory),
}

pub(super) struct AdmissionReservation {
    pub(super) conversation_id: [u8; 16],
    pub(super) submission_id: [u8; 16],
    pub(super) submission_hash: [u8; 32],
    pub(super) expected_conversation_revision: i64,
    pub(super) expected_profile_revision: i64,
    pub(super) operation_generation: i64,
    operation: Arc<OperationControl>,
    pub(super) fingerprint: Arc<crate::runtime_fingerprint::RuntimeFingerprint>,
    pub(super) engine: EngineDescriptor,
    pending_nonce: Option<String>,
    cancellation: CancellationToken,
    cancellation_cause: Mutex<Option<CancellationCause>>,
    engine_quiescent: AtomicBool,
    durable_terminal: AtomicBool,
    accepted_at: OnceLock<Instant>,
    outcome: watch::Sender<Option<Result<crate::history::CommittedAdmission, ServiceError>>>,
    recovery: Mutex<AdmissionRecovery>,
    #[cfg(test)]
    pub(super) recovery_claims: AtomicU64,
    pub(super) output: Mutex<Option<OutputState>>,
}

pub(super) struct ConversationMutationReservation {
    conversation_id: [u8; 16],
}

pub(super) enum AdmissionClaim {
    Existing(Arc<AdmissionReservation>),
    Fresh(Arc<AdmissionReservation>),
}

pub(super) enum AdmissionRecoveryAction {
    Lookup(Arc<crate::history::PreparedAdmission>),
    Stop(
        Arc<crate::history::PreparedAdmission>,
        crate::history::CommittedAdmission,
        Arc<crate::history::FinalizationInput>,
    ),
}

enum AdmissionRecovery {
    Preparing,
    AdmissionInFlight(Arc<crate::history::PreparedAdmission>),
    AdmissionUnknown(Arc<crate::history::PreparedAdmission>),
    StopInFlight(
        Arc<crate::history::PreparedAdmission>,
        crate::history::CommittedAdmission,
        Arc<crate::history::FinalizationInput>,
    ),
    StopUnknown(
        Arc<crate::history::PreparedAdmission>,
        crate::history::CommittedAdmission,
        Arc<crate::history::FinalizationInput>,
    ),
    OutputOwned,
}

impl AdmissionReservation {
    pub(super) fn subscribe(
        &self,
    ) -> watch::Receiver<Option<Result<crate::history::CommittedAdmission, ServiceError>>> {
        self.outcome.subscribe()
    }

    pub(super) fn publish(
        &self,
        outcome: Result<crate::history::CommittedAdmission, ServiceError>,
    ) {
        self.outcome.send_replace(Some(outcome));
    }

    pub(super) fn retain_prepared(&self, prepared: Arc<crate::history::PreparedAdmission>) {
        *self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            AdmissionRecovery::AdmissionInFlight(prepared);
    }

    pub(super) fn admission_unknown(&self) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let AdmissionRecovery::AdmissionInFlight(prepared) = &*recovery {
            *recovery = AdmissionRecovery::AdmissionUnknown(Arc::clone(prepared));
        }
    }

    pub(super) fn retain_stop(
        &self,
        committed: crate::history::CommittedAdmission,
        terminal: Arc<crate::history::FinalizationInput>,
    ) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prepared = match &*recovery {
            AdmissionRecovery::AdmissionInFlight(prepared)
            | AdmissionRecovery::AdmissionUnknown(prepared) => Arc::clone(prepared),
            AdmissionRecovery::StopInFlight(prepared, _, _)
            | AdmissionRecovery::StopUnknown(prepared, _, _) => Arc::clone(prepared),
            AdmissionRecovery::Preparing | AdmissionRecovery::OutputOwned => return,
        };
        *recovery = AdmissionRecovery::StopInFlight(prepared, committed, terminal);
    }

    pub(super) fn stop_unknown(&self) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let AdmissionRecovery::StopInFlight(prepared, committed, terminal) = &*recovery {
            *recovery = AdmissionRecovery::StopUnknown(
                Arc::clone(prepared),
                committed.clone(),
                Arc::clone(terminal),
            );
        }
    }

    pub(super) fn take_recovery_action(&self) -> Option<AdmissionRecoveryAction> {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*recovery {
            AdmissionRecovery::AdmissionUnknown(prepared) => {
                let prepared = Arc::clone(prepared);
                *recovery = AdmissionRecovery::AdmissionInFlight(Arc::clone(&prepared));
                #[cfg(test)]
                self.recovery_claims.fetch_add(1, Ordering::Relaxed);
                Some(AdmissionRecoveryAction::Lookup(prepared))
            }
            AdmissionRecovery::StopUnknown(prepared, committed, terminal) => {
                let prepared = Arc::clone(prepared);
                let committed = committed.clone();
                let terminal = Arc::clone(terminal);
                *recovery = AdmissionRecovery::StopInFlight(
                    Arc::clone(&prepared),
                    committed.clone(),
                    Arc::clone(&terminal),
                );
                #[cfg(test)]
                self.recovery_claims.fetch_add(1, Ordering::Relaxed);
                Some(AdmissionRecoveryAction::Stop(prepared, committed, terminal))
            }
            _ => None,
        }
    }

    pub(super) fn install_output(&self, committed: crate::history::CommittedAdmission) -> bool {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if output.is_some()
            || !matches!(
                &*recovery,
                AdmissionRecovery::AdmissionInFlight(_) | AdmissionRecovery::AdmissionUnknown(_)
            )
        {
            return false;
        }
        *output = Some(OutputState::new(committed));
        *recovery = AdmissionRecovery::OutputOwned;
        true
    }

    #[cfg(test)]
    pub(super) fn admission_retry_ready(&self) -> bool {
        let recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(&*recovery, AdmissionRecovery::AdmissionUnknown(_))
    }

    #[cfg(test)]
    pub(super) fn stop_retry_ready(&self) -> bool {
        let recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(&*recovery, AdmissionRecovery::StopUnknown(_, _, _))
    }
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

    pub(super) fn reserve_load(
        &mut self,
        model_id: String,
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
            cancel: AtomicBool::new(false),
            retry_cleanup: AtomicU64::new(0),
        });
        self.next_task_id = self.next_task_id.saturating_add(1);
        self.next_generation = self.next_generation.saturating_add(1);
        self.current = Some(Arc::clone(&operation));
        Ok(operation)
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
        if target.boot_epoch != self.boot_epoch
            || target.task_id != operation.task_id.to_string()
            || target.generation != operation.generation.to_string()
        {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "unload target does not identify the active operation",
            ));
        }
        let operation = Arc::clone(operation);
        operation.request_cleanup();
        self.advance(&operation, OperationPhase::Stopping);
        Ok(self.accepted(Some(&operation)))
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
        if let Some(admission) = &self.admission {
            admission.request_cancel();
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
        self.publish(Phase::Operation {
            control: Arc::clone(expected),
            phase,
        });
        true
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
            if Arc::ptr_eq(&admission.operation, expected) {
                admission.mark_engine_quiescent();
                let admission = Arc::clone(admission);
                admission_released = self.finish_admission_if_resolved(&admission);
            }
        }
        for pending in &self.pending_generations {
            pending.request_cancel();
        }
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
        }
        for pending in &self.pending_generations {
            pending.request_cancel();
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
