use super::state_machine::CommitReceipt;
use crate::production_lifecycle::{OwnedLifecycleSession, ProvenReady};

pub(crate) struct ExactAbsenceProof {
    _private: (),
}

impl ExactAbsenceProof {
    /// A first migration has no prior durable slot or lifecycle child. The
    /// managed claim and captured scalar source must agree on that boundary.
    pub(in crate::control_state) fn from_first_migration_claim(
        owner: &crate::runtime::NodeOwnerGuard,
        source: &super::ScalarSource,
    ) -> Result<Self, SlotRecoveryError> {
        let run = owner.baseline();
        let exact_claim = run.model_id.is_none()
            && run.lifecycle == loxa_core::supervisor::RunLifecycle::Unloaded
            && !run.stop_requested
            && run.child_pid.is_none()
            && run.child_process_start_time_unix_s.is_none()
            && run.child_pgid.is_none();
        if exact_claim
            && matches!(
                source,
                super::ScalarSource::Fresh
                    | super::ScalarSource::PriorDeadChildlessModelFreeUnloadedV4(_)
            )
        {
            Ok(Self { _private: () })
        } else {
            Err(SlotRecoveryError::LifecycleRecoveryRequired)
        }
    }

    pub(in crate::control_state) fn from_existing_database_claim(
        owner: &crate::runtime::NodeOwnerGuard,
        source: &loxa_core::supervisor::ManagedRecoverySource,
    ) -> Result<Self, SlotRecoveryError> {
        let run = owner.baseline();
        let exact_claim = run.model_id.is_none()
            && run.lifecycle == loxa_core::supervisor::RunLifecycle::Unloaded
            && !run.stop_requested
            && run.child_pid.is_none()
            && run.child_process_start_time_unix_s.is_none()
            && run.child_pgid.is_none();
        if exact_claim && source.is_exact_absent_for(run) {
            Ok(Self { _private: () })
        } else {
            Err(SlotRecoveryError::LifecycleRecoveryRequired)
        }
    }

    #[cfg(test)]
    pub(super) fn fresh_for_test() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UncertaintyReason {
    OwnershipUnavailable,
    LifecycleRecoveryRequired,
}

pub(crate) struct ExactReady {
    authority: Box<OwnedLifecycleSession>,
    _readiness: ProvenReady,
}

impl ExactReady {
    pub(crate) fn model_id(&self) -> &str {
        self.authority.model_id()
    }

    #[allow(dead_code)] // Task 7 transfers this into the runtime lifecycle owner.
    pub(crate) fn into_owned_session(self) -> OwnedLifecycleSession {
        *self.authority
    }

    pub(crate) fn seal(
        authority: OwnedLifecycleSession,
        readiness: ProvenReady,
    ) -> Result<Self, SlotRecoveryError> {
        if authority.model_id() != readiness.model_id() {
            return Err(SlotRecoveryError::LifecycleRecoveryRequired);
        }
        Ok(Self {
            authority: Box::new(authority),
            _readiness: readiness,
        })
    }
}

pub(crate) enum RecoveryEvidence {
    ExactAbsent(ExactAbsenceProof),
    ExactReady(ExactReady),
    Uncertain(UncertaintyReason),
}

pub(crate) enum LifecycleRecoverySource {
    ExactAbsent(ExactAbsenceProof),
    OwnedSession {
        session: Box<OwnedLifecycleSession>,
        readiness: ProvenReady,
    },
    Uncertain(UncertaintyReason),
}

pub(crate) fn gather_recovery_evidence(
    source: LifecycleRecoverySource,
) -> Result<RecoveryEvidence, SlotRecoveryError> {
    match source {
        LifecycleRecoverySource::ExactAbsent(proof) => Ok(RecoveryEvidence::ExactAbsent(proof)),
        LifecycleRecoverySource::OwnedSession { session, readiness } => Ok(
            RecoveryEvidence::ExactReady(ExactReady::seal(*session, readiness)?),
        ),
        LifecycleRecoverySource::Uncertain(reason) => Ok(RecoveryEvidence::Uncertain(reason)),
    }
}

pub(crate) fn existing_database_absence_evidence(
    owner: &crate::runtime::NodeOwnerGuard,
    source: &loxa_core::supervisor::ManagedRecoverySource,
) -> Result<RecoveryEvidence, SlotRecoveryError> {
    Ok(RecoveryEvidence::ExactAbsent(
        ExactAbsenceProof::from_existing_database_claim(owner, source)?,
    ))
}

impl RecoveryEvidence {
    pub(crate) fn uncertain(reason: UncertaintyReason) -> Self {
        Self::Uncertain(reason)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlotRecoveryError {
    LifecycleRecoveryRequired,
}

pub(crate) enum RecoveryDecision {
    Unloaded,
    Ready { authority: ExactReady },
    Recovery { error: SlotRecoveryError },
}

pub(crate) fn decide(evidence: RecoveryEvidence) -> RecoveryDecision {
    match evidence {
        RecoveryEvidence::ExactAbsent(_proof) => RecoveryDecision::Unloaded,
        RecoveryEvidence::ExactReady(authority) => RecoveryDecision::Ready { authority },
        RecoveryEvidence::Uncertain(_reason) => RecoveryDecision::Recovery {
            error: SlotRecoveryError::LifecycleRecoveryRequired,
        },
    }
}

pub(crate) struct ReconciledControlState {
    pub(crate) receipts: Vec<CommitReceipt>,
    pub(crate) ready_authority: Option<ExactReady>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_port_cache_and_probe_values_have_no_exact_ready_constructor() {
        assert!(matches!(
            decide(RecoveryEvidence::uncertain(
                UncertaintyReason::OwnershipUnavailable
            )),
            RecoveryDecision::Recovery { .. }
        ));
    }
}
