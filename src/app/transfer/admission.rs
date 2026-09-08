use super::{TransferError, TransferErrorKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CapacityPlan {
    CleanInstalled,
    InstalledCleanup,
    InstalledRepairRequest,
    PendingPublish,
    PendingRequest,
    FreshRequest,
}

pub(super) fn combine_plans(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
) -> Result<CapacityPlan, TransferErrorKind> {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    match (catalog, artifact) {
        (Catalog::ArtifactConflict, _) => Err(TransferErrorKind::ArtifactConflict),
        (Catalog::Unsafe, _) | (_, Artifact::Unsafe) => Err(TransferErrorKind::UnsafeLocalState),
        (Catalog::Installed, Artifact::ValidFinal) => Ok(CapacityPlan::CleanInstalled),
        (Catalog::InstalledCompletionDebris, Artifact::ValidFinal) => {
            Ok(CapacityPlan::InstalledCleanup)
        }
        (Catalog::Installed, Artifact::Fresh | Artifact::InstalledRepair) => {
            Ok(CapacityPlan::InstalledRepairRequest)
        }
        (Catalog::Installed, Artifact::CompletePart | Artifact::CompleteRestart) => {
            Ok(CapacityPlan::PendingPublish)
        }
        (
            Catalog::MatchingPending,
            Artifact::ValidFinal | Artifact::CompletePart | Artifact::CompleteRestart,
        ) => Ok(CapacityPlan::PendingPublish),
        (
            Catalog::MatchingPending,
            Artifact::Fresh | Artifact::RequestCapable | Artifact::InstalledRepair,
        ) => Ok(CapacityPlan::PendingRequest),
        (Catalog::Fresh, Artifact::Fresh) => Ok(CapacityPlan::FreshRequest),
        _ => Err(TransferErrorKind::UnsafeLocalState),
    }
}

pub(super) fn round_capacity(value: u64, fragment: u64) -> Result<u64, TransferErrorKind> {
    if fragment == 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    if value == 0 {
        return Ok(0);
    }
    value
        .checked_add(
            fragment
                .checked_sub(1)
                .ok_or(TransferErrorKind::CapacityOverflow)?,
        )
        .and_then(|rounded| rounded.checked_div(fragment))
        .and_then(|fragments| fragments.checked_mul(fragment))
        .ok_or(TransferErrorKind::CapacityOverflow)
}

pub(super) fn required_capacity(
    plan: CapacityPlan,
    artifact_size: u64,
    manifest_len: u64,
    fragment: u64,
) -> Result<u64, TransferErrorKind> {
    if matches!(
        plan,
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
    ) {
        return Ok(0);
    }
    let exact = round_capacity(artifact_size, fragment)?;
    let write_peak = round_capacity(
        artifact_size
            .checked_add(1)
            .ok_or(TransferErrorKind::CapacityOverflow)?,
        fragment,
    )?;
    let manifest = round_capacity(manifest_len, fragment)?;
    match plan {
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup => Ok(0),
        CapacityPlan::InstalledRepairRequest => Ok(write_peak),
        CapacityPlan::PendingPublish => Ok(manifest),
        CapacityPlan::PendingRequest => Ok(write_peak.max(
            exact
                .checked_add(manifest)
                .ok_or(TransferErrorKind::CapacityOverflow)?,
        )),
        CapacityPlan::FreshRequest => Ok(write_peak
            .checked_add(manifest)
            .ok_or(TransferErrorKind::CapacityOverflow)?
            .max(
                exact
                    .checked_add(
                        manifest
                            .checked_mul(2)
                            .ok_or(TransferErrorKind::CapacityOverflow)?,
                    )
                    .ok_or(TransferErrorKind::CapacityOverflow)?,
            )),
    }
}

pub(super) fn capacity_is_sufficient(required: u64, available: u64) -> bool {
    available >= required
}

pub(super) fn checked_available_bytes(
    available_fragments: u64,
    fragment: u64,
) -> Result<u64, TransferErrorKind> {
    if fragment == 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    available_fragments
        .checked_mul(fragment)
        .ok_or(TransferErrorKind::CapacityOverflow)
}

pub(super) fn checked_u64(value: u128) -> Result<u64, TransferErrorKind> {
    u64::try_from(value).map_err(|_| TransferErrorKind::CapacityOverflow)
}

pub(super) fn admit_capacity_with(
    lock: &crate::catalog::ModelLock,
    plan: CapacityPlan,
    artifact_size: u64,
    manifest_len: u64,
    probe: impl FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
) -> Result<u64, TransferError> {
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    if matches!(
        plan,
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
    ) {
        return Ok(0);
    }
    let (available, fragment) = probe(lock.model_directory()).map_err(TransferError::terminal)?;
    let required = required_capacity(plan, artifact_size, manifest_len, fragment)
        .map_err(TransferError::terminal)?;
    if capacity_is_sufficient(required, available) {
        Ok(required)
    } else {
        Err(TransferError::insufficient_disk(required, available))
    }
}

#[cfg(unix)]
pub(super) fn available_capacity(
    directory: &std::fs::File,
) -> Result<(u64, u64), TransferErrorKind> {
    use std::os::fd::AsRawFd;

    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `directory` supplies a live descriptor and `status` points to writable storage.
    if unsafe { libc::fstatvfs(directory.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    // SAFETY: successful `fstatvfs` initialized the entire output structure.
    let status = unsafe { status.assume_init() };
    let fragment = checked_u64(status.f_frsize as u128)?;
    let available_fragments = checked_u64(status.f_bavail as u128)?;
    Ok((
        checked_available_bytes(available_fragments, fragment)?,
        fragment,
    ))
}

#[cfg(not(unix))]
pub(super) fn available_capacity(
    _directory: &std::fs::File,
) -> Result<(u64, u64), TransferErrorKind> {
    Err(TransferErrorKind::CapacityUnavailable)
}
