use crate::runtime::ownership::RuntimeOwnershipAcquireError;
use crate::runtime::process::{
    process_group, process_group_has_live_members, process_snapshot, terminate_stale_process_group,
};
use crate::runtime_bundle::RecoverableExecutionStage;
use std::path::Path;

pub(in crate::runtime) fn reconcile_interrupted_execution_builds(
    run_dir: &Path,
) -> Result<(), RuntimeOwnershipAcquireError> {
    let builds = crate::runtime_bundle::recoverable_execution_builds(run_dir)
        .map_err(RuntimeOwnershipAcquireError::Failed)?;
    for build in builds {
        let (owner_pid, owner_start) = build.owner();
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
        {
            continue;
        }
        build
            .ensure_current()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
        if process_snapshot(owner_pid)
            .map_err(RuntimeOwnershipAcquireError::Failed)?
            .is_some_and(|owner| owner.start_identity == owner_start)
        {
            continue;
        }
        build
            .cleanup()
            .map_err(RuntimeOwnershipAcquireError::Failed)?;
    }
    Ok(())
}

pub(in crate::runtime) fn reconcile_unleased_execution_stages(
    run_dir: &Path,
) -> Result<(), RuntimeOwnershipAcquireError> {
    let stages = crate::runtime_bundle::recoverable_execution_stages(run_dir)
        .map_err(RuntimeOwnershipAcquireError::Failed)?;
    for stage in stages {
        reconcile_unleased_execution_stage(stage)?;
    }
    Ok(())
}

fn reconcile_unleased_execution_stage(
    mut stage: RecoverableExecutionStage,
) -> Result<(), RuntimeOwnershipAcquireError> {
    let (owner_pid, owner_start) = stage.owner();
    if process_snapshot(owner_pid)
        .map_err(RuntimeOwnershipAcquireError::Failed)?
        .is_some_and(|owner| owner.start_identity == owner_start)
        && !stage.is_abandoned()
    {
        return Ok(());
    }
    if !stage
        .lock_and_refresh()
        .map_err(RuntimeOwnershipAcquireError::Failed)?
    {
        return Err(RuntimeOwnershipAcquireError::Conflict);
    }
    if process_snapshot(owner_pid)
        .map_err(RuntimeOwnershipAcquireError::Failed)?
        .is_some_and(|owner| owner.start_identity == owner_start)
        && !stage.is_abandoned()
    {
        return Ok(());
    }
    let Some((child_pid, child_group)) = stage.child() else {
        return stage
            .cleanup()
            .map_err(RuntimeOwnershipAcquireError::Failed);
    };
    reconcile_execution_stage_child(&stage, child_pid, child_group)?;
    stage
        .cleanup()
        .map_err(RuntimeOwnershipAcquireError::Failed)
}

fn reconcile_execution_stage_child(
    stage: &RecoverableExecutionStage,
    child_pid: u32,
    child_group: i32,
) -> Result<(), RuntimeOwnershipAcquireError> {
    stage
        .ensure_current()
        .map_err(RuntimeOwnershipAcquireError::Failed)?;
    match process_snapshot(child_pid).map_err(RuntimeOwnershipAcquireError::Failed)? {
        Some(child) => {
            let observed_group =
                process_group(child_pid).map_err(RuntimeOwnershipAcquireError::Failed)?;
            if child.executable == stage.server() && observed_group == child_group {
                #[cfg(all(test, target_os = "macos"))]
                if FAIL_NEXT_EXECUTION_STAGE_TERMINATION
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(RuntimeOwnershipAcquireError::Failed(
                        "injected prepared runtime termination failure".into(),
                    ));
                }
                terminate_stale_process_group(child_group)
                    .map_err(RuntimeOwnershipAcquireError::Failed)?;
            } else {
                #[cfg(test)]
                eprintln!(
                    "prepared recovery identity: executable={:?}, expected={:?}, group={observed_group}, expected_group={child_group}",
                    child.executable,
                    stage.server()
                );
                return Err(RuntimeOwnershipAcquireError::Failed(
                    "prepared runtime recovery child identity does not match".into(),
                ));
            }
        }
        None if process_group_has_live_members(child_group)
            .map_err(RuntimeOwnershipAcquireError::Failed)? =>
        {
            return Err(RuntimeOwnershipAcquireError::Failed(
                "prepared runtime recovery group has no matching leader".into(),
            ));
        }
        None => {}
    }
    if process_group_has_live_members(child_group).map_err(RuntimeOwnershipAcquireError::Failed)? {
        return Err(RuntimeOwnershipAcquireError::Failed(
            "prepared runtime recovery group is still active".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "macos"))]
static FAIL_NEXT_EXECUTION_STAGE_TERMINATION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(all(test, target_os = "macos"))]
pub(crate) struct ExecutionStageTerminationFaultReset;

#[cfg(all(test, target_os = "macos"))]
impl Drop for ExecutionStageTerminationFaultReset {
    fn drop(&mut self) {
        FAIL_NEXT_EXECUTION_STAGE_TERMINATION.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(all(test, target_os = "macos"))]
pub(crate) fn fail_next_execution_stage_termination_for_test() -> ExecutionStageTerminationFaultReset
{
    FAIL_NEXT_EXECUTION_STAGE_TERMINATION.store(true, std::sync::atomic::Ordering::SeqCst);
    ExecutionStageTerminationFaultReset
}
