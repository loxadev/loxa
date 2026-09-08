use super::entry::{
    open_final_entry, open_invalid_entry, open_restart_entry, rename_final_to_invalid_no_replace,
    unlink_invalid_entry, unlink_repair_entry_if_present, OpenArtifactEntry,
};
use super::ArtifactTransferError;
use crate::download::{
    ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure, IntegrityAuthority,
};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
use std::fs::File;
use std::path::Path;

pub(super) struct CapturedInvalidAuthority {
    file: File,
    identity: RegularFileIdentity,
}

pub(super) fn ensure_repair_debris_absent(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    invalid_path: &Path,
    restart_path: &Path,
) -> Result<(), ArtifactTransferError> {
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    for entry in [
        open_invalid_entry(directory, invalid_path),
        open_restart_entry(directory, restart_path),
    ] {
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn ensure_complete_part_authority(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    invalid_path: &Path,
    restart_path: &Path,
    invalid_authority: Option<&CapturedInvalidAuthority>,
) -> Result<(), ArtifactTransferError> {
    let Some(invalid_authority) = invalid_authority else {
        return ensure_repair_debris_absent(
            directory,
            directory_identity,
            model_dir,
            invalid_path,
            restart_path,
        );
    };
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_invalid_entry(directory, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(
        &invalid_authority.file,
        &invalid_authority.identity,
        &resolved,
        invalid_path,
    )
    .map_err(|_| ArtifactTransferError::Durability)?;
    match open_restart_entry(directory, restart_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn remove_invalid_before_promotion(
    directory: (&File, &DirectoryIdentity, &Path),
    invalid: (&CapturedInvalidAuthority, &Path),
    source: (&File, &RegularFileIdentity, &Path, OpenArtifactEntry),
    forbidden: [(&Path, OpenArtifactEntry); 2],
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (invalid, invalid_path) = invalid;
    let (source, source_identity, source_path, open_source) = source;
    let prove_current = || {
        ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
            .map_err(|_| ArtifactTransferError::Durability)?;
        let resolved = open_invalid_entry(directory, invalid_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        ensure_regular_descriptors_match(&invalid.file, &invalid.identity, &resolved, invalid_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        let resolved =
            open_source(directory, source_path).map_err(|_| ArtifactTransferError::Durability)?;
        ensure_regular_descriptors_match(source, source_identity, &resolved, source_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        for &(path, open) in &forbidden {
            let entry = open(directory, path);
            match entry {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
            }
        }
        ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
            .map_err(|_| ArtifactTransferError::Durability)
    };

    prove_current()?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeInvalidUnlink,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    prove_current()?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::InvalidIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    prove_current()?;
    unlink_invalid_entry(directory).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::InvalidUnlinked,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_source(directory, source_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(source, source_identity, &resolved, source_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_invalid_entry(directory, invalid_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    for &(path, open) in &forbidden {
        let entry = open(directory, path);
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn finish_repair(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
) -> Result<(), ArtifactTransferError> {
    for name in [
        b"model.gguf.invalid\0".as_slice(),
        b"model.gguf.part.restart\0".as_slice(),
    ] {
        unlink_repair_entry_if_present(directory, name)
            .map_err(|_| ArtifactTransferError::Durability)?;
    }
    directory
        .sync_all()
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn integrity_authority_at_fence(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    invalid_path: &Path,
    authority: IntegrityAuthority,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<IntegrityAuthority, ArtifactTransferError> {
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeIntegrityAuthorityObservation,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let invalid_is_present = invalid_authority_is_present(directory, invalid_path)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if invalid_is_present {
        Ok(IntegrityAuthority::Repair)
    } else {
        Ok(authority)
    }
}

pub(super) fn invalid_authority_is_present(
    directory: &File,
    invalid_path: &Path,
) -> Result<bool, ArtifactTransferError> {
    capture_invalid_authority(directory, invalid_path).map(|authority| authority.is_some())
}

pub(super) fn capture_invalid_authority(
    directory: &File,
    invalid_path: &Path,
) -> Result<Option<CapturedInvalidAuthority>, ArtifactTransferError> {
    let invalid = match open_invalid_entry(directory, invalid_path) {
        Ok(invalid) => invalid,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    let identity = regular_file_identity(&invalid, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_invalid_entry(directory, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(&invalid, &identity, &resolved, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(Some(CapturedInvalidAuthority {
        file: invalid,
        identity,
    }))
}

pub(super) fn quarantine_captured_final(
    directory: &File,
    final_artifact: (&File, &RegularFileIdentity, &Path),
    invalid_path: &Path,
) -> Result<CapturedInvalidAuthority, ArtifactTransferError> {
    let (final_file, final_identity, final_path) = final_artifact;
    let resolved =
        open_final_entry(directory, final_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(final_file, final_identity, &resolved, final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_invalid_entry(directory, invalid_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    rename_final_to_invalid_no_replace(directory).map_err(|_| ArtifactTransferError::Durability)?;
    let identity = regular_file_identity(final_file, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if !final_identity.same_file_after_rename(&identity) {
        return Err(ArtifactTransferError::Durability);
    }
    let invalid = open_invalid_entry(directory, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(final_file, &identity, &invalid, invalid_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    match open_final_entry(directory, final_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
    }
    Ok(CapturedInvalidAuthority {
        file: final_file
            .try_clone()
            .map_err(|_| ArtifactTransferError::Durability)?,
        identity,
    })
}
