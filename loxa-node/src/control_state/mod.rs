mod recovery;
mod repository;
mod schema;
pub(crate) mod state_machine;
mod worker;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ControlStatePath(std::path::PathBuf);

impl From<std::path::PathBuf> for ControlStatePath {
    fn from(path: std::path::PathBuf) -> Self {
        Self(path)
    }
}

impl AsRef<std::path::Path> for ControlStatePath {
    fn as_ref(&self) -> &std::path::Path {
        &self.0
    }
}

impl PartialEq<std::path::PathBuf> for ControlStatePath {
    fn eq(&self, other: &std::path::PathBuf) -> bool {
        self.0 == *other
    }
}

pub(crate) use recovery::{
    existing_database_absence_evidence, gather_recovery_evidence, LifecycleRecoverySource,
    RecoveryEvidence, SlotRecoveryError,
};
pub(crate) use repository::{
    ControlIdGenerator, ControlRepository, RepositoryError, RepositoryErrorClass, RestoreSummary,
    ScalarProvenance, ScalarSource, ValidationSummary,
};
pub(crate) use state_machine::InstancePublication;
pub(crate) use worker::{
    ControlStateBootstrap, ControlStateError, ControlStateHandle, ControlStateInit,
    ControlStateOpenInput, ControlStateWorker,
};

pub(crate) fn acquisition_recovery_evidence(
    owner: &crate::runtime::NodeOwnerGuard,
    first_migration_source: Option<&ScalarSource>,
) -> Result<RecoveryEvidence, SlotRecoveryError> {
    if first_migration_source.is_some() {
        return Ok(RecoveryEvidence::uncertain(
            recovery::UncertaintyReason::OwnershipUnavailable,
        ));
    }
    let source = owner
        .acquisition_recovery()
        .ok_or(SlotRecoveryError::LifecycleRecoveryRequired)?;
    match existing_database_absence_evidence(owner, source) {
        Ok(evidence) => Ok(evidence),
        Err(SlotRecoveryError::LifecycleRecoveryRequired) => Ok(RecoveryEvidence::uncertain(
            recovery::UncertaintyReason::LifecycleRecoveryRequired,
        )),
    }
}

#[cfg(test)]
pub(crate) fn open_control_state_for_test(
    init: ControlStateInit,
) -> Result<ControlStateBootstrap, ControlStateError> {
    worker::ControlStateWorker::open_reconcile_and_spawn(init)
}

#[cfg(test)]
pub(crate) fn ownership_unavailable_recovery_for_test() -> RecoveryEvidence {
    recovery::RecoveryEvidence::uncertain(recovery::UncertaintyReason::OwnershipUnavailable)
}
