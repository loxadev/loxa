use super::super::entry::{
    capture_artifact_entry, open_final_entry, open_part_entry, open_restart_entry,
    reject_unsafe_artifact_entry_if_present,
};
use super::super::prefix::{
    captured_prefixes_match, normalize_restart_prefix, remove_invalid_authoritative_part,
};
use super::super::repair::{
    invalid_authority_is_present, quarantine_captured_final, CapturedInvalidAuthority,
};
use super::super::ArtifactTransferError;
use super::AttemptInputs;
use crate::download::{
    ArtifactOperation, ArtifactOperationFailure, IntegrityAuthority, ProgressUpdate,
};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, RegularFileIdentity,
};
use crate::verification::file::{
    verify_regular_entry_controlled, VerificationOutcome, VerifiedRegularFile,
};
use std::fs::File;

pub(super) fn inspect_existing_final(
    inputs: &AttemptInputs<'_>,
    captured_invalid_authority: &mut Option<CapturedInvalidAuthority>,
    integrity_authority: &mut IntegrityAuthority,
    progress: &mut impl FnMut(ProgressUpdate),
) -> Result<Option<VerifiedRegularFile>, ArtifactTransferError> {
    let directory = inputs.directory;
    let directory_identity = inputs.directory_identity;
    let spec = inputs.spec;
    let paths = inputs;
    if let Some((final_file, final_identity)) =
        capture_artifact_entry(directory, paths.final_path, open_final_entry)?
    {
        if final_identity.size() == spec.size() {
            match verify_regular_entry_controlled(
                directory,
                paths.final_path,
                open_final_entry,
                spec.size(),
                spec.sha256(),
                Some((&final_file, &final_identity)),
                &|| false,
            ) {
                Ok(VerificationOutcome::Verified(verified)) => {
                    progress(ProgressUpdate::Verifying {
                        transferred: spec.size(),
                        total: spec.size(),
                    });
                    super::super::repair::finish_repair(
                        directory,
                        directory_identity,
                        paths.model_dir,
                    )?;
                    return Ok(Some(verified));
                }
                Ok(VerificationOutcome::ChecksumMismatch) => {}
                Ok(VerificationOutcome::Interrupted) | Err(_) => {
                    return Err(ArtifactTransferError::Durability);
                }
            }
        }
        if *integrity_authority == IntegrityAuthority::Repair
            || invalid_authority_is_present(directory, paths.invalid_path)?
        {
            return Err("a prior corrupt artifact repair is still pending".into());
        }
        *captured_invalid_authority = Some(quarantine_captured_final(
            directory,
            (&final_file, &final_identity, paths.final_path),
            paths.invalid_path,
        )?);
        *integrity_authority = IntegrityAuthority::Repair;
    }
    Ok(None)
}

pub(super) fn recover_staging_prefix(
    inputs: &AttemptInputs<'_>,
    prefix_must_be_reproved: bool,
    initial_invalid_authority: bool,
    should_pause: &impl Fn() -> bool,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<Option<(File, RegularFileIdentity, u64)>, ArtifactTransferError> {
    let directory = inputs.directory;
    let directory_identity = inputs.directory_identity;
    let spec = inputs.spec;
    let paths = inputs;
    reject_unsafe_artifact_entry_if_present(directory, paths.part_path, open_part_entry).map_err(
        |error| {
            if prefix_must_be_reproved {
                ArtifactTransferError::Durability
            } else {
                error.into()
            }
        },
    )?;
    reject_unsafe_artifact_entry_if_present(directory, paths.restart_path, open_restart_entry)
        .map_err(|error| {
            if prefix_must_be_reproved {
                ArtifactTransferError::Durability
            } else {
                error.into()
            }
        })?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, paths.model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let mut recovered_part = match open_part_entry(directory, paths.part_path) {
        Ok(part) => {
            let identity = regular_file_identity(&part, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let resolved = open_part_entry(directory, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_regular_descriptors_match(&part, &identity, &resolved, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let length = part
                .metadata()
                .map(|metadata| metadata.len())
                .map_err(|_| ArtifactTransferError::Durability)?;
            Some((part, identity, length))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    let recovered_restart = match open_restart_entry(directory, paths.restart_path) {
        Ok(restart) => Some(restart),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    match (recovered_restart, recovered_part.as_ref()) {
        (Some(restart), Some((part, part_identity, part_length))) if *part_length < spec.size() => {
            let restart_identity = regular_file_identity(&restart, paths.restart_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let restart_length = restart
                .metadata()
                .map(|metadata| metadata.len())
                .map_err(|_| ArtifactTransferError::Durability)?;
            if *part_length > spec.size()
                || restart_length > spec.size()
                || (*part_length == 0 && restart_length > 0)
            {
                return Err(ArtifactTransferError::Durability);
            }
            if should_pause() {
                return Err(crate::download::http::TransferError::paused(0).into());
            }
            let prefixes_match = if restart_length > *part_length {
                let Some(prefixes_match) = captured_prefixes_match(
                    (directory, directory_identity, paths.model_dir),
                    (part, part_identity, *part_length, paths.part_path),
                    (
                        &restart,
                        &restart_identity,
                        restart_length,
                        paths.restart_path,
                    ),
                    should_pause,
                )?
                else {
                    return Err(crate::download::http::TransferError::paused(0).into());
                };
                prefixes_match
            } else {
                false
            };
            normalize_restart_prefix(
                directory,
                directory_identity,
                paths.model_dir,
                (&restart, paths.restart_path),
                (part, part_identity, *part_length, paths.part_path),
                Some((
                    &restart_identity,
                    restart_length,
                    restart_length > *part_length && prefixes_match,
                )),
                artifact_operation,
            )?;
            let part = open_part_entry(directory, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let identity = regular_file_identity(&part, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let resolved = open_part_entry(directory, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_regular_descriptors_match(&part, &identity, &resolved, paths.part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let length = part
                .metadata()
                .map(|metadata| metadata.len())
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_directory_descriptor_matches_path(
                directory,
                directory_identity,
                paths.model_dir,
            )
            .map_err(|_| ArtifactTransferError::Durability)?;
            recovered_part = Some((part, identity, length));
        }
        (Some(_), Some((_, _, part_length))) if *part_length > spec.size() => {
            return Err(ArtifactTransferError::Durability);
        }
        (Some(_), None) if !initial_invalid_authority => {
            return Err(ArtifactTransferError::Durability);
        }
        _ => {}
    }
    let offset = recovered_part
        .as_ref()
        .map(|(_, _, length)| *length)
        .unwrap_or(0);
    if offset > spec.size() {
        let (part, part_identity, _) = recovered_part
            .as_ref()
            .ok_or(ArtifactTransferError::Durability)?;
        remove_invalid_authoritative_part(
            directory,
            directory_identity,
            paths.model_dir,
            part,
            part_identity,
            paths.part_path,
            artifact_operation,
        )?;
        recovered_part = None;
    }
    if prefix_must_be_reproved && recovered_part.is_none() {
        return Err(ArtifactTransferError::Durability);
    }
    Ok(recovered_part)
}
