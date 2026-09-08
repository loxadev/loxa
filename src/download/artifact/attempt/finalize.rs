use super::super::entry::{open_final_entry, open_part_entry, open_restart_entry};
use super::super::prefix::{
    normalize_durable_prefix, remove_invalid_authoritative_part,
    remove_invalid_restart_and_recover_part,
};
use super::super::publication::{promote_authoritative_part, promote_authoritative_restart};
use super::super::repair::{
    ensure_repair_debris_absent, integrity_authority_at_fence, remove_invalid_before_promotion,
    CapturedInvalidAuthority,
};
use super::super::ArtifactTransferError;
use super::AttemptInputs;
use crate::download::{
    ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure, IntegrityAuthority,
    ProgressUpdate,
};
use crate::safe_file::{regular_file_identity, RegularFileIdentity};
use crate::verification::file::{
    verify_regular_entry_controlled, VerificationOutcome, VerifiedRegularFile,
};
use std::fs::File;
use std::path::Path;

pub(super) fn verify_and_promote_staging(
    inputs: &AttemptInputs<'_>,
    staging: (&File, &Path, bool),
    recovered_part: Option<&(File, RegularFileIdentity, u64)>,
    repair: (Option<&CapturedInvalidAuthority>, IntegrityAuthority),
    should_pause: &impl Fn() -> bool,
    progress: &mut impl FnMut(ProgressUpdate),
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<VerifiedRegularFile, ArtifactTransferError> {
    let directory = inputs.directory;
    let directory_identity = inputs.directory_identity;
    let spec = inputs.spec;
    let paths = inputs;
    let (output, target, ignored_range) = staging;
    let (captured_invalid_authority, integrity_authority) = repair;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::StagingSynced,
        file: output,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    progress(ProgressUpdate::Verifying {
        transferred: spec.size(),
        total: spec.size(),
    });
    let staging_identity = regular_file_identity(output, target)
        .map_err(|_| crate::download::http::TransferError::fatal("unsafe staging artifact"))?;
    let open_staging = if target == paths.restart_path {
        open_restart_entry
    } else {
        open_part_entry
    };
    let verified = match verify_regular_entry_controlled(
        directory,
        target,
        open_staging,
        spec.size(),
        spec.sha256(),
        Some((output, &staging_identity)),
        should_pause,
    ) {
        Ok(VerificationOutcome::Verified(verified)) => verified,
        Ok(VerificationOutcome::Interrupted) => {
            let retained_bytes = normalize_durable_prefix(
                ignored_range,
                (directory, directory_identity, paths.model_dir),
                (output, target),
                recovered_part.map(|(part, identity, length)| {
                    (part, identity, *length, paths.part_path.as_path())
                }),
                artifact_operation,
            )?;
            return Err(crate::download::http::TransferError::paused(retained_bytes).into());
        }
        Ok(VerificationOutcome::ChecksumMismatch) if !ignored_range => {
            remove_invalid_authoritative_part(
                directory,
                directory_identity,
                paths.model_dir,
                output,
                &staging_identity,
                paths.part_path,
                artifact_operation,
            )?;
            let integrity_authority = integrity_authority_at_fence(
                directory,
                directory_identity,
                paths.model_dir,
                paths.invalid_path,
                integrity_authority,
                artifact_operation,
            )?;
            return Err(ArtifactTransferError::Integrity {
                retained_bytes: 0,
                authority: integrity_authority,
            });
        }
        Ok(VerificationOutcome::ChecksumMismatch) => {
            let (part, part_identity, _) =
                recovered_part.ok_or(ArtifactTransferError::Durability)?;
            let retained_bytes = remove_invalid_restart_and_recover_part(
                directory,
                directory_identity,
                paths.model_dir,
                (output, &staging_identity, paths.restart_path),
                (part, part_identity, paths.part_path),
                artifact_operation,
            )?;
            let integrity_authority = integrity_authority_at_fence(
                directory,
                directory_identity,
                paths.model_dir,
                paths.invalid_path,
                integrity_authority,
                artifact_operation,
            )?;
            return Err(ArtifactTransferError::Integrity {
                retained_bytes,
                authority: integrity_authority,
            });
        }
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforePromotion,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    if should_pause() {
        let retained_bytes = normalize_durable_prefix(
            ignored_range,
            (directory, directory_identity, paths.model_dir),
            (output, target),
            recovered_part.map(|(part, identity, length)| {
                (part, identity, *length, paths.part_path.as_path())
            }),
            artifact_operation,
        )?;
        return Err(crate::download::http::TransferError::paused(retained_bytes).into());
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::CompletionFencePassed,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    if !ignored_range {
        if let Some(invalid_authority) = captured_invalid_authority {
            remove_invalid_before_promotion(
                (directory, directory_identity, paths.model_dir),
                (invalid_authority, paths.invalid_path),
                (output, &staging_identity, paths.part_path, open_part_entry),
                [
                    (paths.restart_path, open_restart_entry),
                    (paths.final_path, open_final_entry),
                ],
                artifact_operation,
            )?;
        } else {
            ensure_repair_debris_absent(
                directory,
                directory_identity,
                paths.model_dir,
                paths.invalid_path,
                paths.restart_path,
            )?;
        }
        promote_authoritative_part(
            directory,
            directory_identity,
            (
                paths.model_dir,
                paths.part_path,
                paths.final_path,
                paths.invalid_path,
                paths.restart_path,
            ),
            output,
            &staging_identity,
            spec.size(),
            artifact_operation,
        )?;
        let resolved = open_final_entry(directory, paths.final_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        return verified
            .rebind_after_rename_resolved(paths.final_path, resolved)
            .map_err(|_| ArtifactTransferError::Durability);
    }
    let (part, part_identity, _) = recovered_part.ok_or(ArtifactTransferError::Durability)?;
    promote_authoritative_restart(
        (directory, directory_identity, paths.model_dir),
        (output, &staging_identity, paths.restart_path),
        (part, part_identity, paths.part_path),
        (
            captured_invalid_authority,
            paths.invalid_path,
            paths.final_path,
            spec.size(),
        ),
        artifact_operation,
    )?;
    let resolved = open_final_entry(directory, paths.final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    verified
        .rebind_after_rename_resolved(paths.final_path, resolved)
        .map_err(|_| ArtifactTransferError::Durability)
}
