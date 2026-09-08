use super::super::entry::{
    open_part_entry, open_restart_entry, unlink_part_entry, unlink_restart_entry,
};
use super::super::ArtifactTransferError;
use crate::download::{ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match, DirectoryIdentity,
    RegularFileIdentity,
};
use std::fs::File;
use std::path::Path;

pub(in crate::download::artifact) fn remove_invalid_authoritative_part(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    staging: &File,
    staging_identity: &RegularFileIdentity,
    part_path: &Path,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactTransferError> {
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, staging_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativePartIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, staging_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    unlink_part_entry(directory).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativePartUnlinked,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_part_entry(directory, part_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativePartAbsent,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_part_entry(directory, part_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(())
}

pub(in crate::download::artifact) fn remove_invalid_restart_and_recover_part(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    restart: (&File, &RegularFileIdentity, &Path),
    part: (&File, &RegularFileIdentity, &Path),
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let (restart, restart_identity, restart_path) = restart;
    let (part, part_identity, part_path) = part;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeRestartUnlink,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    unlink_restart_entry(directory).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartUnlinked,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeRestartAbsenceProof,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_restart_entry(directory, restart_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartAbsent,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_restart_entry(directory, restart_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::RecoveredPartSynced,
        file: part,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeRecoveredPartRestat,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let retained_bytes = authoritative
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(retained_bytes)
}

pub(in crate::download::artifact) fn remove_restart_after_authoritative_part_is_durable(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
    restart: (&File, &RegularFileIdentity, u64, &Path),
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (part, part_identity, part_length, part_path) = part;
    let (restart, restart_identity, restart_length, restart_path) = restart;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeRestartUnlink,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let candidate = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &candidate, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if authoritative
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
        || candidate
            .metadata()
            .map_err(|_| ArtifactTransferError::Durability)?
            .len()
            != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let candidate = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &candidate, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if authoritative
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
        || candidate
            .metadata()
            .map_err(|_| ArtifactTransferError::Durability)?
            .len()
            != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    unlink_restart_entry(directory).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartUnlinked,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::NormalizationDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeRestartAbsenceProof,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_restart_entry(directory, restart_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::RestartAbsent,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_restart_entry(directory, restart_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let retained_bytes = authoritative
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    if retained_bytes != part_length {
        return Err(ArtifactTransferError::Durability);
    }
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(retained_bytes)
}
