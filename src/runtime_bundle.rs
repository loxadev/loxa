mod capture;
mod inventory;
mod native;
mod record;
mod recovery;
mod stage;

pub use inventory::validate_embedded_runtime;
// Preserve the existing crate-visible facade paths.
#[allow(unused_imports)]
pub(crate) use record::STAGE_STATE_OFFSET;
#[allow(unused_imports)]
pub(crate) use recovery::RecoverableExecutionBuild;
pub(crate) use recovery::{
    cleanup_execution_stage, execution_stage_is_abandoned, recoverable_execution_builds,
    recoverable_execution_stages, RecoverableExecutionStage,
};
pub(crate) use stage::{prepare_embedded_runtime, PreparedRuntime};

#[cfg(all(test, target_os = "macos"))]
mod tests;
