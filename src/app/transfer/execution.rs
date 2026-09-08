use super::admission::{admit_capacity_with, checked_u64, combine_plans, CapacityPlan};
use super::recovery::{
    admission_error_with_audited_recovery, catalog_mutation_error, completed, download_failure,
    interrupted, paused, publication_error_after_final_audit,
    recover_installed_completion_after_artifact_revalidation, recovery_is_discardable,
};
use super::{
    TransferControl, TransferDisposition, TransferError, TransferErrorKind, TransferIntent,
    TransferPhase, TransferProgress, TransferResult, TransferSelected,
};
use crate::app::{installed, AppService};
use crate::catalog::{self, Manifest, ModelLockError};
use crate::download::{self, DownloadFailure, DownloadTerminalOutcome, VerifiedRegularFile};
use crate::huggingface::ResolvedFile;

pub(super) fn deterministic_model_id(artifact: &ResolvedFile) -> String {
    let stem = artifact
        .path()
        .strip_suffix(".gguf")
        .or_else(|| artifact.path().strip_suffix(".GGUF"))
        .unwrap_or(artifact.path());
    let identity = format!("{}-{stem}", artifact.repo());
    let mut prefix = identity
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() {
                byte.to_ascii_lowercase() as char
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let suffix = &artifact.sha256()[..artifact.sha256().len().min(16)];
    prefix.truncate(120usize.saturating_sub(suffix.len() + 1));
    format!("{}-{suffix}", prefix.trim_end_matches('-'))
}

pub(super) fn exact_manifest(model_id: String, artifact: &ResolvedFile) -> Manifest {
    Manifest {
        version: 1,
        id: model_id,
        repo: Some(artifact.repo().to_owned()),
        revision: Some(artifact.commit().to_owned()),
        remote_filename: Some(artifact.path().to_owned()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: artifact.sha256().to_owned(),
        size: artifact.size(),
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

pub(super) fn transfer_selected_with_observers<F, C, T, D, L, P>(
    service: &AppService,
    request_and_observers: (TransferSelected, L, P),
    control: TransferControl,
    mut progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        Option<VerifiedRegularFile>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
    L: FnOnce(&str),
    P: FnOnce(),
{
    let (request, after_alternate_lookup, before_manifest_publication) = request_and_observers;
    let TransferSelected { artifact, intent } = request;
    let (requested_model_id, requires_existing_catalog) = match intent {
        TransferIntent::Requested(model_id) => (model_id, false),
        TransferIntent::InspectedInstalled(model_id) => (Some(model_id), true),
    };
    let (model_id, chosen_by_alternate_reuse) = match requested_model_id {
        Some(model_id) => (model_id, false),
        None => match installed::exact_remote_model_id(&service.reader.paths.models, &artifact)
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?
        {
            Some(model_id) => (model_id, true),
            None => (deterministic_model_id(&artifact), false),
        },
    };
    if chosen_by_alternate_reuse {
        after_alternate_lookup(&model_id);
    }
    crate::paths::validate_id(&model_id)
        .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
    let manifest = exact_manifest(model_id.clone(), &artifact);
    manifest
        .validate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    if manifest_bytes.len() > crate::catalog::transfer::MAX_CATALOG_MANIFEST_BYTES {
        return Err(TransferError::terminal(
            TransferErrorKind::CatalogManifestTooLarge,
        ));
    }
    let manifest_len =
        checked_u64(manifest_bytes.len() as u128).map_err(TransferError::terminal)?;
    let model_dir = service
        .reader
        .paths
        .model_dir(&model_id)
        .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
    let lock_result = if chosen_by_alternate_reuse || requires_existing_catalog {
        catalog::ModelLock::acquire_existing(&model_dir)
    } else {
        catalog::ModelLock::acquire_for_transfer(&model_dir)
    };
    let lock = lock_result.map_err(|error| {
        TransferError::terminal(match error {
            ModelLockError::Busy => TransferErrorKind::Busy,
            ModelLockError::Missing | ModelLockError::UnsafeLocalState => {
                TransferErrorKind::UnsafeLocalState
            }
        })
    })?;
    let catalog_plan = catalog::transfer::plan_transfer(&lock, &manifest);
    let catalog_state = catalog_plan.state();
    if (chosen_by_alternate_reuse || requires_existing_catalog)
        && !matches!(
            catalog_state,
            crate::catalog::transfer::CatalogTransferState::Installed
                | crate::catalog::transfer::CatalogTransferState::InstalledCompletionDebris
        )
    {
        return Err(TransferError::terminal(TransferErrorKind::UnsafeLocalState));
    }
    let mut artifact_plan = match crate::download::plan::plan_artifact_transfer(
        lock.model_directory(),
        &model_dir,
        artifact.size(),
        artifact.sha256(),
        &|| control.pause_requested(),
    ) {
        crate::download::plan::ArtifactPlanOutcome::Interrupted => {
            return Ok(interrupted(model_id, artifact));
        }
        crate::download::plan::ArtifactPlanOutcome::Ready(plan) => plan,
    };
    let artifact_state = artifact_plan.state();
    let plan = combine_plans(catalog_state, artifact_state).map_err(TransferError::terminal)?;
    if chosen_by_alternate_reuse
        && !matches!(
            plan,
            CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
        )
    {
        return Err(TransferError::terminal(TransferErrorKind::UnsafeLocalState));
    }
    if let Err(error) = admit_capacity_with(&lock, plan, artifact.size(), manifest_len, capacity) {
        return Err(admission_error_with_audited_recovery(
            error,
            catalog_state,
            artifact_state,
            &artifact_plan,
            &model_id,
            &artifact,
        ));
    }
    if control.pause_requested() {
        return Ok(interrupted(model_id, artifact));
    }

    if plan == CapacityPlan::InstalledCleanup {
        recover_installed_completion_after_artifact_revalidation(
            &lock,
            &model_dir,
            &manifest,
            &artifact,
            &mut artifact_plan,
            &catalog_plan,
        )?;
        return Ok(completed(
            model_id,
            artifact,
            TransferDisposition::AlreadyInstalled,
        ));
    }
    catalog::transfer::recover_admitted_catalog_temps(&lock, &catalog_plan)
        .map_err(catalog_mutation_error)?;

    if plan == CapacityPlan::CleanInstalled {
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        return Ok(completed(
            model_id,
            artifact,
            TransferDisposition::AlreadyInstalled,
        ));
    }

    let mut discardable = recovery_is_discardable(
        catalog_state,
        artifact_state,
        artifact_plan.has_invalid_authority(),
        false,
    );

    let verified = if plan == CapacityPlan::PendingPublish
        && artifact_state == crate::download::plan::ArtifactTransferState::ValidFinal
    {
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        if control.pause_requested() {
            return Ok(interrupted(model_id, artifact));
        }
        artifact_plan
            .take_verified_final()
            .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?
    } else {
        let token = if matches!(
            plan,
            CapacityPlan::FreshRequest
                | CapacityPlan::PendingRequest
                | CapacityPlan::InstalledRepairRequest
        ) {
            Some(token())
        } else {
            None
        };
        if plan == CapacityPlan::FreshRequest {
            lock.revalidate()
                .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
            catalog::prepare_pull(&model_dir, &manifest)
                .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
            discardable = recovery_is_discardable(
                catalog_state,
                artifact_state,
                artifact_plan.has_invalid_authority(),
                true,
            );
        }
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        let verified_part = match artifact_state {
            crate::download::plan::ArtifactTransferState::CompletePart => Some(
                artifact_plan
                    .take_verified_part()
                    .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?,
            ),
            crate::download::plan::ArtifactTransferState::CompleteRestart => {
                let verified_restart = artifact_plan
                    .take_verified_restart()
                    .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
                match crate::download::normalize_complete_restart(
                    lock.model_directory(),
                    &model_dir,
                    &artifact,
                    verified_restart,
                ) {
                    Ok(verified) => Some(verified),
                    Err(failure) => {
                        return Err(download_failure(failure, model_id, artifact, discardable));
                    }
                }
            }
            _ => None,
        };
        let outcome = download(
            &artifact,
            download::DownloadDirectoryAuthority::retained(&model_dir, lock.model_directory()),
            token.flatten(),
            verified_part,
            &control,
            &mut progress,
        );
        match outcome {
            Ok(DownloadTerminalOutcome::Paused { retained_bytes }) => {
                return Ok(paused(model_id, artifact, retained_bytes, discardable));
            }
            Err(failure) => {
                return Err(download_failure(failure, model_id, artifact, discardable));
            }
            Ok(DownloadTerminalOutcome::Complete(completion)) => completion.into_parts().1,
        }
    };

    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Publication))?;
    progress(TransferProgress {
        phase: TransferPhase::Publishing,
        transferred_bytes: artifact.size(),
        total_bytes: artifact.size(),
    });
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Publication))?;
    before_manifest_publication();
    if catalog::publish_manifest_verified(&service.reader.paths.models, &manifest, &lock, &verified)
        .is_err()
    {
        return Err(publication_error_after_final_audit(
            &lock, &model_dir, &model_id, &artifact, &verified,
        ));
    }
    Ok(completed(
        model_id,
        artifact,
        if catalog_state == crate::catalog::transfer::CatalogTransferState::Installed {
            TransferDisposition::AlreadyInstalled
        } else {
            TransferDisposition::Installed
        },
    ))
}

impl AppService {
    pub fn transfer_selected<F>(
        &self,
        request: TransferSelected,
        control: TransferControl,
        progress: F,
    ) -> Result<TransferResult, TransferError>
    where
        F: FnMut(TransferProgress),
    {
        transfer_selected_with_observers(
            self,
            (request, |_| {}, || {}),
            control,
            progress,
            super::admission::available_capacity,
            crate::huggingface::discover_token,
            |artifact, model_dir, token, verified_part, control, progress| {
                download::download_controlled(
                    artifact,
                    model_dir,
                    token,
                    verified_part,
                    || control.pause_requested(),
                    |update| progress(TransferProgress::from_download(update)),
                )
            },
        )
    }
}
