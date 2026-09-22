use super::{CancellationCause, EngineDescriptor};
use crate::service::coordinator::history::OutputState;
use crate::service::coordinator::OperationControl;
use loxa_ipc::ServiceError;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub(in crate::service::coordinator) struct AdmissionReservation {
    pub(in crate::service::coordinator) conversation_id: [u8; 16],
    pub(in crate::service::coordinator) submission_id: [u8; 16],
    pub(in crate::service::coordinator) submission_hash: [u8; 32],
    pub(in crate::service::coordinator) expected_conversation_revision: i64,
    pub(in crate::service::coordinator) expected_profile_revision: i64,
    pub(in crate::service::coordinator) operation_generation: i64,
    pub(super) operation: Arc<OperationControl>,
    pub(in crate::service::coordinator) fingerprint:
        Arc<crate::runtime_fingerprint::RuntimeFingerprint>,
    pub(in crate::service::coordinator) engine: EngineDescriptor,
    pub(super) pending_nonce: Option<String>,
    pub(super) cancellation: CancellationToken,
    pub(super) cancellation_cause: Mutex<Option<CancellationCause>>,
    pub(super) engine_quiescent: AtomicBool,
    pub(super) durable_terminal: AtomicBool,
    pub(super) accepted_at: OnceLock<Instant>,
    pub(super) outcome:
        watch::Sender<Option<Result<crate::history::CommittedAdmission, ServiceError>>>,
    pub(super) recovery: Mutex<AdmissionRecovery>,
    #[cfg(test)]
    pub(in crate::service::coordinator) recovery_claims: AtomicU64,
    pub(in crate::service::coordinator) output: Mutex<Option<OutputState>>,
}

pub(in crate::service::coordinator) enum AdmissionClaim {
    Existing(Arc<AdmissionReservation>),
    Fresh(Arc<AdmissionReservation>),
}

pub(in crate::service::coordinator) enum AdmissionRecoveryAction {
    Lookup(Arc<crate::history::PreparedAdmission>),
    Stop(
        Arc<crate::history::PreparedAdmission>,
        crate::history::CommittedAdmission,
        Arc<crate::history::FinalizationInput>,
    ),
}

pub(super) enum AdmissionRecovery {
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
    pub(in crate::service::coordinator) fn subscribe(
        &self,
    ) -> watch::Receiver<Option<Result<crate::history::CommittedAdmission, ServiceError>>> {
        self.outcome.subscribe()
    }

    pub(in crate::service::coordinator) fn publish(
        &self,
        outcome: Result<crate::history::CommittedAdmission, ServiceError>,
    ) {
        self.outcome.send_replace(Some(outcome));
    }

    pub(in crate::service::coordinator) fn retain_prepared(
        &self,
        prepared: Arc<crate::history::PreparedAdmission>,
    ) {
        *self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            AdmissionRecovery::AdmissionInFlight(prepared);
    }

    pub(in crate::service::coordinator) fn admission_unknown(&self) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let AdmissionRecovery::AdmissionInFlight(prepared) = &*recovery {
            *recovery = AdmissionRecovery::AdmissionUnknown(Arc::clone(prepared));
        }
    }

    pub(in crate::service::coordinator) fn retain_stop(
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

    pub(in crate::service::coordinator) fn stop_unknown(&self) {
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

    pub(in crate::service::coordinator) fn take_recovery_action(
        &self,
    ) -> Option<AdmissionRecoveryAction> {
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

    pub(in crate::service::coordinator) fn install_output(
        &self,
        committed: crate::history::CommittedAdmission,
    ) -> bool {
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
    pub(in crate::service::coordinator) fn admission_retry_ready(&self) -> bool {
        let recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(&*recovery, AdmissionRecovery::AdmissionUnknown(_))
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn stop_retry_ready(&self) -> bool {
        let recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(&*recovery, AdmissionRecovery::StopUnknown(_, _, _))
    }
}
