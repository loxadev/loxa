use super::{TransferDisposition, TransferError, TransferErrorKind, TransferResult};
use crate::catalog::{self, Manifest};
use crate::download::{self, DownloadFailure, VerifiedRegularFile};
use crate::huggingface::ResolvedFile;
use std::path::Path;

pub(super) fn recovery_is_discardable(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
    has_invalid_authority: bool,
    pending_created: bool,
) -> bool {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    if has_invalid_authority {
        return false;
    }
    match (catalog, artifact) {
        (Catalog::Fresh, Artifact::Fresh) => pending_created,
        (
            Catalog::MatchingPending,
            Artifact::Fresh
            | Artifact::RequestCapable
            | Artifact::CompletePart
            | Artifact::CompleteRestart,
        ) => true,
        _ => false,
    }
}

pub(super) fn audited_retained_bytes(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
    plan: &crate::download::plan::ArtifactTransferPlan,
    artifact_size: u64,
) -> Option<u64> {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    if plan.invalid_length().is_some()
        && !matches!(
            artifact,
            Artifact::CompletePart
                | Artifact::CompleteRestart
                | Artifact::InstalledRepair
                | Artifact::ValidFinalRepairDebris
        )
    {
        return None;
    }
    let staging = || {
        plan.part_length()
            .into_iter()
            .chain(plan.restart_length())
            .max()
            .unwrap_or(0)
    };
    match (catalog, artifact) {
        (
            Catalog::MatchingPending,
            Artifact::ValidFinal | Artifact::CompletePart | Artifact::CompleteRestart,
        )
        | (Catalog::Installed, Artifact::CompletePart | Artifact::CompleteRestart) => {
            Some(artifact_size)
        }
        (Catalog::MatchingPending, Artifact::Fresh) => Some(0),
        (
            Catalog::MatchingPending | Catalog::Installed,
            Artifact::RequestCapable | Artifact::InstalledRepair,
        ) => Some(staging()),
        _ => None,
    }
}

pub(super) fn interrupted(model_id: String, artifact: ResolvedFile) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition: TransferDisposition::Interrupted,
        retained_bytes: None,
        discardable: false,
    }
}

pub(super) fn paused(
    model_id: String,
    artifact: ResolvedFile,
    retained_bytes: u64,
    discardable: bool,
) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition: TransferDisposition::Paused,
        retained_bytes: Some(retained_bytes),
        discardable,
    }
}

pub(super) fn completed(
    model_id: String,
    artifact: ResolvedFile,
    disposition: TransferDisposition,
) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition,
        retained_bytes: None,
        discardable: false,
    }
}

pub(super) fn catalog_mutation_error(
    error: catalog::transfer::CatalogMutationError,
) -> TransferError {
    TransferError::terminal(match error {
        catalog::transfer::CatalogMutationError::Changed => TransferErrorKind::UnsafeLocalState,
        catalog::transfer::CatalogMutationError::Durability => TransferErrorKind::Durability,
    })
}

pub(super) fn download_failure(
    failure: DownloadFailure,
    model_id: String,
    artifact: ResolvedFile,
    discardable: bool,
) -> TransferError {
    match failure {
        DownloadFailure::Legacy(_) => TransferError::terminal(TransferErrorKind::Remote),
        DownloadFailure::Remote { retained_bytes } => TransferError::recovery(
            TransferErrorKind::Remote,
            model_id,
            artifact,
            retained_bytes,
            discardable,
        ),
        DownloadFailure::Integrity {
            retained_bytes,
            authority,
        } => TransferError::recovery(
            TransferErrorKind::Integrity,
            model_id,
            artifact,
            retained_bytes,
            authority == download::IntegrityAuthority::PendingOnly && discardable,
        ),
        DownloadFailure::Durability => TransferError::terminal(TransferErrorKind::Durability),
        DownloadFailure::DiskExhausted { retained_bytes } => TransferError::recovery(
            TransferErrorKind::DiskExhausted,
            model_id,
            artifact,
            retained_bytes,
            discardable,
        ),
    }
}

pub(super) fn admission_error_with_audited_recovery(
    error: TransferError,
    catalog_state: crate::catalog::transfer::CatalogTransferState,
    artifact_state: crate::download::plan::ArtifactTransferState,
    artifact_plan: &crate::download::plan::ArtifactTransferPlan,
    model_id: &str,
    artifact: &ResolvedFile,
) -> TransferError {
    if error.kind != TransferErrorKind::InsufficientDisk {
        return error;
    }
    let Some(retained_bytes) = audited_retained_bytes(
        catalog_state,
        artifact_state,
        artifact_plan,
        artifact.size(),
    ) else {
        return error;
    };
    error.with_recovery(
        model_id.to_owned(),
        artifact.clone(),
        retained_bytes,
        recovery_is_discardable(
            catalog_state,
            artifact_state,
            artifact_plan.has_invalid_authority(),
            false,
        ),
    )
}

pub(super) fn publication_error_after_final_audit(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    model_id: &str,
    artifact: &ResolvedFile,
    verified: &VerifiedRegularFile,
) -> TransferError {
    if lock.revalidate().is_err()
        || verified
            .proves(
                &model_dir.join("model.gguf"),
                artifact.size(),
                artifact.sha256(),
            )
            .is_err()
    {
        return TransferError::terminal(TransferErrorKind::Publication);
    }
    TransferError::recovery(
        TransferErrorKind::Publication,
        model_id.to_owned(),
        artifact.clone(),
        artifact.size(),
        false,
    )
}

pub(super) fn recover_installed_completion_after_artifact_revalidation(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    artifact: &ResolvedFile,
    artifact_plan: &mut crate::download::plan::ArtifactTransferPlan,
    catalog_plan: &crate::catalog::transfer::CatalogTransferPlan,
) -> Result<(), TransferError> {
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    artifact_plan
        .take_revalidated_clean_final(
            lock.model_directory(),
            model_dir,
            artifact.size(),
            artifact.sha256(),
        )
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    catalog::transfer::recover_installed_completion(lock, manifest, catalog_plan)
        .map_err(catalog_mutation_error)
}
