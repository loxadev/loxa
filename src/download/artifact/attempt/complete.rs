use super::super::entry::{open_final_entry, open_part_entry, open_restart_entry};
use super::super::prefix::remove_invalid_authoritative_part;
use super::super::publication::promote_authoritative_part;
use super::super::repair::{
    ensure_complete_part_authority, ensure_repair_debris_absent, integrity_authority_at_fence,
    remove_invalid_before_promotion, CapturedInvalidAuthority,
};
use super::super::staging::durable_part_barrier;
use super::super::ArtifactTransferError;
use super::AttemptInputs;
use crate::download::{
    ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure, IntegrityAuthority,
    ProgressUpdate,
};
use crate::safe_file::RegularFileIdentity;
use crate::verification::file::{
    verify_regular_entry_controlled, VerificationOutcome, VerifiedRegularFile,
};
use std::fs::File;

pub(super) fn finish_complete_recovered_part(
    inputs: &AttemptInputs<'_>,
    recovered_part: Option<&(File, RegularFileIdentity, u64)>,
    verified_part: Option<VerifiedRegularFile>,
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
    let (captured_invalid_authority, integrity_authority) = repair;
    let repairing = integrity_authority == IntegrityAuthority::Repair;
    if repairing && (verified_part.is_none() || captured_invalid_authority.is_none()) {
        return Err(ArtifactTransferError::Durability);
    }
    let invalid_authority = repairing.then_some(captured_invalid_authority).flatten();
    ensure_complete_part_authority(
        directory,
        directory_identity,
        paths.model_dir,
        paths.invalid_path,
        paths.restart_path,
        invalid_authority,
    )?;
    let (part, part_identity, _) = recovered_part.ok_or(ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::StagingSynced,
        file: part,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    progress(ProgressUpdate::Verifying {
        transferred: spec.size(),
        total: spec.size(),
    });
    let verified = match verified_part {
        Some(verified) => {
            let resolved = open_part_entry(directory, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            verified
                .proves_resolved(paths.part_path, spec.size(), spec.sha256(), resolved)
                .map_err(|_| ArtifactTransferError::Durability)?;
            verified
        }
        None => match verify_regular_entry_controlled(
            directory,
            paths.part_path,
            open_part_entry,
            spec.size(),
            spec.sha256(),
            Some((part, part_identity)),
            should_pause,
        ) {
            Ok(VerificationOutcome::Verified(verified)) => verified,
            Ok(VerificationOutcome::Interrupted) => {
                let retained_bytes = durable_part_barrier(
                    directory,
                    directory_identity,
                    paths.model_dir,
                    part,
                    paths.part_path,
                    artifact_operation,
                )?;
                ensure_complete_part_authority(
                    directory,
                    directory_identity,
                    paths.model_dir,
                    paths.invalid_path,
                    paths.restart_path,
                    invalid_authority,
                )?;
                return Err(crate::download::http::TransferError::paused(retained_bytes).into());
            }
            Ok(VerificationOutcome::ChecksumMismatch) => {
                remove_invalid_authoritative_part(
                    directory,
                    directory_identity,
                    paths.model_dir,
                    part,
                    part_identity,
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
            Err(_) => return Err(ArtifactTransferError::Durability),
        },
    };
    ensure_complete_part_authority(
        directory,
        directory_identity,
        paths.model_dir,
        paths.invalid_path,
        paths.restart_path,
        invalid_authority,
    )?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforePromotion,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    if should_pause() {
        let retained_bytes = durable_part_barrier(
            directory,
            directory_identity,
            paths.model_dir,
            part,
            paths.part_path,
            artifact_operation,
        )?;
        ensure_complete_part_authority(
            directory,
            directory_identity,
            paths.model_dir,
            paths.invalid_path,
            paths.restart_path,
            invalid_authority,
        )?;
        return Err(crate::download::http::TransferError::paused(retained_bytes).into());
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::CompletionFencePassed,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_complete_part_authority(
        directory,
        directory_identity,
        paths.model_dir,
        paths.invalid_path,
        paths.restart_path,
        invalid_authority,
    )?;
    if let Some(invalid_authority) = invalid_authority {
        remove_invalid_before_promotion(
            (directory, directory_identity, paths.model_dir),
            (invalid_authority, paths.invalid_path),
            (part, part_identity, paths.part_path, open_part_entry),
            [
                (paths.restart_path, open_restart_entry),
                (paths.final_path, open_final_entry),
            ],
            artifact_operation,
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
        part,
        part_identity,
        spec.size(),
        artifact_operation,
    )?;
    ensure_repair_debris_absent(
        directory,
        directory_identity,
        paths.model_dir,
        paths.invalid_path,
        paths.restart_path,
    )?;
    let resolved = open_final_entry(directory, paths.final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    verified
        .rebind_after_rename_resolved(paths.final_path, resolved)
        .map_err(|_| ArtifactTransferError::Durability)
}
