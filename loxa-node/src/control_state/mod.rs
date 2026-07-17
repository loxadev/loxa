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
    ControlStateBootstrap, ControlStateHandle, ControlStateInit, ControlStateOpenInput,
};
