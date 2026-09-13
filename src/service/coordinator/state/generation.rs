use super::{
    AdmissionClaim, AdmissionRecovery, AdmissionReservation, CoordinatorState, OperationPhase,
    Phase,
};
use loxa_ipc::{ErrorCategory, GenerationTarget, ServiceError};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Notify};

const MAX_PENDING_GENERATIONS: usize = 16;

#[derive(Clone)]
pub(super) struct EngineDescriptor {
    pub(super) pid: u32,
    pub(super) endpoint: Arc<PathBuf>,
}

pub(super) struct PendingGeneration {
    nonce: String,
    identity: Mutex<Option<PendingIdentity>>,
    cancellation: Arc<Cancellation>,
}

#[derive(Clone, Copy)]
struct PendingIdentity {
    submission_id: [u8; 16],
    submission_hash: [u8; 32],
}

pub(super) struct Cancellation {
    cancelled: AtomicBool,
    wake: Notify,
}

impl Cancellation {
    pub(super) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }

    pub(super) fn request(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(super) async fn wait(&self) {
        loop {
            let notified = self.wake.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl PendingGeneration {
    pub(super) fn nonce(&self) -> &str {
        &self.nonce
    }

    pub(super) fn request_cancel(&self) {
        self.cancellation.request();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

impl AdmissionReservation {
    pub(super) fn request_cancel(&self) {
        self.cancellation.request();
    }

    pub(super) async fn wait_cancelled(&self) {
        self.cancellation.wait().await;
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn begin_execution(&self) {
        self.engine_quiescent.store(false, Ordering::Release);
    }

    pub(super) fn mark_engine_quiescent(&self) {
        self.engine_quiescent.store(true, Ordering::Release);
    }

    fn engine_is_quiescent(&self) -> bool {
        self.engine_quiescent.load(Ordering::Acquire)
    }

    pub(super) fn mark_durable_terminal(&self) {
        self.durable_terminal.store(true, Ordering::Release);
    }

    fn terminal_resolved(&self) -> bool {
        self.engine_quiescent.load(Ordering::Acquire)
            && self.durable_terminal.load(Ordering::Acquire)
    }
}

impl CoordinatorState {
    pub(super) fn register_pending_generation(
        &mut self,
    ) -> Result<Arc<PendingGeneration>, ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if self.pending_generations.len() >= MAX_PENDING_GENERATIONS {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "generation connection capacity is full",
            ));
        }
        let nonce = format!("{:032x}", self.next_pending_nonce);
        self.next_pending_nonce = self.next_pending_nonce.checked_add(1).ok_or_else(|| {
            ServiceError::new(
                ErrorCategory::RecoveryRequired,
                "generation connection identity space is exhausted",
            )
        })?;
        let pending = Arc::new(PendingGeneration {
            nonce,
            identity: Mutex::new(None),
            cancellation: Arc::new(Cancellation::new()),
        });
        self.pending_generations.push(Arc::clone(&pending));
        Ok(pending)
    }

    pub(super) fn bind_pending_generation(
        &self,
        pending: &Arc<PendingGeneration>,
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
    ) -> Result<(), ServiceError> {
        if !self.pending_is_current(pending) {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "generation connection is no longer current",
            ));
        }
        let mut identity = pending.identity.lock().map_err(|_| {
            ServiceError::new(ErrorCategory::Internal, "pending identity lock is poisoned")
        })?;
        match *identity {
            Some(current)
                if current.submission_id != submission_id
                    || current.submission_hash != submission_hash =>
            {
                Err(ServiceError::new(
                    ErrorCategory::Conflict,
                    "generation connection was rebound to another submission",
                ))
            }
            Some(_) => Ok(()),
            None => {
                *identity = Some(PendingIdentity {
                    submission_id,
                    submission_hash,
                });
                Ok(())
            }
        }
    }

    pub(super) fn finish_pending_generation(&mut self, pending: &Arc<PendingGeneration>) {
        self.pending_generations
            .retain(|current| !Arc::ptr_eq(current, pending));
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reserve_admission(
        &mut self,
        conversation_id: [u8; 16],
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
    ) -> Result<AdmissionClaim, ServiceError> {
        self.reserve_admission_with_cancellation(
            conversation_id,
            submission_id,
            submission_hash,
            expected_conversation_revision,
            expected_profile_revision,
            None,
            Arc::new(Cancellation::new()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reserve_pending_admission(
        &mut self,
        pending: &Arc<PendingGeneration>,
        conversation_id: [u8; 16],
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
    ) -> Result<AdmissionClaim, ServiceError> {
        if !self.pending_is_current(pending) {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "generation connection is no longer current",
            ));
        }
        let identity = *pending.identity.lock().map_err(|_| {
            ServiceError::new(ErrorCategory::Internal, "pending identity lock is poisoned")
        })?;
        if !identity.is_some_and(|identity| {
            identity.submission_id == submission_id && identity.submission_hash == submission_hash
        }) {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "generation connection identity changed",
            ));
        }

        if let Some(admission) = &self.admission {
            let result = if admission.submission_id == submission_id {
                if admission.submission_hash != submission_hash {
                    Err(ServiceError::new(
                        ErrorCategory::Conflict,
                        "submission identity was reused with a different payload",
                    ))
                } else {
                    Ok(AdmissionClaim::Existing(Arc::clone(admission)))
                }
            } else {
                Err(ServiceError::new(
                    ErrorCategory::Busy,
                    "another conversation admission is active",
                ))
            };
            self.finish_pending_generation(pending);
            return result;
        }
        if pending.is_cancelled() {
            self.finish_pending_generation(pending);
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation was stopped before admission",
            ));
        }
        self.finish_pending_generation(pending);
        self.reserve_admission_with_cancellation(
            conversation_id,
            submission_id,
            submission_hash,
            expected_conversation_revision,
            expected_profile_revision,
            Some(pending.nonce.clone()),
            Arc::clone(&pending.cancellation),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_admission_with_cancellation(
        &mut self,
        conversation_id: [u8; 16],
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
        pending_nonce: Option<String>,
        cancellation: Arc<Cancellation>,
    ) -> Result<AdmissionClaim, ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if let Some(admission) = &self.admission {
            if admission.submission_id == submission_id {
                if admission.submission_hash != submission_hash {
                    return Err(ServiceError::new(
                        ErrorCategory::Conflict,
                        "submission identity was reused with a different payload",
                    ));
                }
                return Ok(AdmissionClaim::Existing(Arc::clone(admission)));
            }
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "another conversation admission is active",
            ));
        }
        if self
            .conversation_mutations
            .iter()
            .any(|mutation| mutation.conversation_id == conversation_id)
        {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "a conversation mutation is active",
            ));
        }
        let (operation, engine, fingerprint) = match &self.phase {
            Phase::Operation {
                control,
                phase:
                    OperationPhase::Ready {
                        engine,
                        fingerprint,
                    },
            } if self.is_current(control) && !control.cancel.load(Ordering::Acquire) => {
                (control, engine, fingerprint)
            }
            _ => {
                return Err(ServiceError::new(
                    ErrorCategory::ServiceUnavailable,
                    "no verified runtime is ready",
                ));
            }
        };
        let operation_generation = i64::try_from(operation.generation).map_err(|_| {
            ServiceError::new(
                ErrorCategory::Internal,
                "runtime generation exceeds the history range",
            )
        })?;
        let (outcome, _) = watch::channel(None);
        let reservation = Arc::new(AdmissionReservation {
            conversation_id,
            submission_id,
            submission_hash,
            expected_conversation_revision,
            expected_profile_revision,
            operation_generation,
            operation: Arc::clone(operation),
            fingerprint: Arc::clone(fingerprint),
            engine: engine.clone(),
            pending_nonce,
            cancellation,
            engine_quiescent: AtomicBool::new(true),
            durable_terminal: AtomicBool::new(false),
            outcome,
            recovery: Mutex::new(AdmissionRecovery::Preparing),
            output: Mutex::new(None),
        });
        self.admission = Some(Arc::clone(&reservation));
        Ok(AdmissionClaim::Fresh(reservation))
    }

    pub(super) fn admission_is_current(&self, expected: &Arc<AdmissionReservation>) -> bool {
        self.admission
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    }

    pub(super) fn current_admission(&self) -> Option<Arc<AdmissionReservation>> {
        self.admission.as_ref().map(Arc::clone)
    }

    pub(super) fn cancel_generation(
        &mut self,
        target: &GenerationTarget,
    ) -> Result<(), ServiceError> {
        match target {
            GenerationTarget::Pending {
                boot_epoch,
                pending_nonce,
            } => {
                if boot_epoch != &self.boot_epoch {
                    return Err(ServiceError::new(
                        ErrorCategory::Conflict,
                        "pending target belongs to another service boot",
                    ));
                }
                if let Some(pending) = self
                    .pending_generations
                    .iter()
                    .find(|pending| pending.nonce == *pending_nonce)
                {
                    pending.request_cancel();
                    return Ok(());
                }
                if let Some(admission) = self.admission.as_ref().filter(|admission| {
                    admission.pending_nonce.as_deref() == Some(pending_nonce.as_str())
                }) {
                    admission.request_cancel();
                    self.request_engine_cleanup_if_needed(admission);
                    return Ok(());
                }
                Err(ServiceError::new(
                    ErrorCategory::NotFound,
                    "pending generation target is no longer active",
                ))
            }
            GenerationTarget::Accepted {
                boot_epoch,
                submission_id,
                operation_generation,
            } => {
                let admission = self.admission.as_ref().ok_or_else(|| {
                    ServiceError::new(ErrorCategory::NotFound, "no generation is active")
                })?;
                if boot_epoch != &self.boot_epoch
                    || submission_id != &crate::history::encode_id(admission.submission_id)
                    || operation_generation != &admission.operation_generation.to_string()
                {
                    return Err(ServiceError::new(
                        ErrorCategory::Conflict,
                        "generation target does not identify the active reservation",
                    ));
                }
                admission.request_cancel();
                self.request_engine_cleanup_if_needed(admission);
                Ok(())
            }
        }
    }

    pub(super) fn require_generation_cleanup(&self, expected: &Arc<AdmissionReservation>) {
        if self.admission_is_current(expected) {
            expected.request_cancel();
            self.request_engine_cleanup_if_needed(expected);
        }
    }

    pub(super) fn begin_generation_execution(
        &self,
        expected: &Arc<AdmissionReservation>,
    ) -> Result<EngineDescriptor, ServiceError> {
        if !self.admission_is_current(expected)
            || expected.is_cancelled()
            || self.draining.load(Ordering::Acquire)
        {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation was cancelled before engine execution",
            ));
        }
        let exact_runtime = matches!(
            &self.phase,
            Phase::Operation {
                control,
                phase: OperationPhase::Ready { engine, .. },
            } if self.is_current(control)
                && !control.cancel.load(Ordering::Acquire)
                && i64::try_from(control.generation).ok()
                    == Some(expected.operation_generation)
                && engine.pid == expected.engine.pid
                && engine.endpoint == expected.engine.endpoint
        );
        if !exact_runtime {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "the admitted runtime changed before engine execution",
            ));
        }
        expected.begin_execution();
        Ok(expected.engine.clone())
    }

    pub(super) fn confirm_generation_quiescence(
        &mut self,
        expected: &Arc<AdmissionReservation>,
    ) -> bool {
        if !self.admission_is_current(expected)
            || expected.is_cancelled()
            || expected.operation.cancel.load(Ordering::Acquire)
            || self.draining.load(Ordering::Acquire)
        {
            return false;
        }
        expected.mark_engine_quiescent();
        self.finish_admission_if_resolved(expected);
        true
    }

    pub(super) fn finish_admission(&mut self, expected: &Arc<AdmissionReservation>) {
        if self.admission_is_current(expected) {
            self.admission = None;
        }
    }

    pub(super) fn finish_admission_if_resolved(
        &mut self,
        expected: &Arc<AdmissionReservation>,
    ) -> bool {
        if self.admission_is_current(expected) && expected.terminal_resolved() {
            self.admission = None;
            true
        } else {
            false
        }
    }

    fn pending_is_current(&self, pending: &Arc<PendingGeneration>) -> bool {
        self.pending_generations
            .iter()
            .any(|current| Arc::ptr_eq(current, pending))
    }

    fn request_engine_cleanup_if_needed(&self, admission: &AdmissionReservation) {
        if !admission.engine_is_quiescent() {
            admission.operation.request_cleanup();
        }
    }
}
