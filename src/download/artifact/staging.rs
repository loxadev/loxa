use super::entry::open_part_entry;
use super::ArtifactTransferError;
use crate::download::http::TransferError;
use crate::download::{ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure};
use crate::huggingface::ResolvedFile;
use crate::safe_file::{
    directory_identity, ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    open_directory, regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
use std::fs::File;
use std::path::Path;

pub(in crate::download) fn prove_existing_part_for_pause(
    spec: &ResolvedFile,
    directory_authority: super::DownloadDirectoryAuthority<'_>,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let model_dir = directory_authority.as_ref();
    let directory = if let Some(retained) = directory_authority.retained_directory() {
        retained
            .try_clone()
            .map_err(|_| ArtifactTransferError::Durability)?
    } else {
        open_directory(model_dir)
            .map_err(|_| ArtifactTransferError::Durability)?
            .0
    };
    let directory_identity =
        directory_identity(&directory, model_dir).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let part_path = model_dir.join("model.gguf.part");
    let part =
        open_part_entry(&directory, &part_path).map_err(|_| ArtifactTransferError::Durability)?;
    let part_identity =
        regular_file_identity(&part, &part_path).map_err(|_| ArtifactTransferError::Durability)?;
    let part_length = part
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    if part_length > spec.size() {
        return Err(ArtifactTransferError::Durability);
    }
    ensure_captured_part_is_current(
        (&directory, &directory_identity, model_dir),
        (&part, &part_identity, part_length, &part_path),
    )?;
    let retained_bytes = durable_part_barrier(
        &directory,
        &directory_identity,
        model_dir,
        &part,
        &part_path,
        artifact_operation,
    )?;
    ensure_captured_part_is_current(
        (&directory, &directory_identity, model_dir),
        (&part, &part_identity, part_length, &part_path),
    )?;
    if retained_bytes != part_length {
        return Err(ArtifactTransferError::Durability);
    }
    Ok(retained_bytes)
}

pub(super) fn classify_pre_body_terminal(
    error: TransferError,
    directory: (&File, &DirectoryIdentity, &Path),
    recovered_part: Option<(&File, &RegularFileIdentity, u64, &Path)>,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<ArtifactTransferError, ArtifactTransferError> {
    let Some((part, part_identity, part_length, part_path)) = recovered_part else {
        return Ok(ArtifactTransferError::RemoteBeforeBody(error));
    };
    let (directory, directory_identity, model_dir) = directory;
    ensure_captured_part_is_current(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
    )?;
    let retained_bytes = durable_part_barrier(
        directory,
        directory_identity,
        model_dir,
        part,
        part_path,
        artifact_operation,
    )?;
    ensure_captured_part_is_current(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
    )?;
    if retained_bytes != part_length {
        return Err(ArtifactTransferError::Durability);
    }
    if error.is_paused() {
        Ok(TransferError::paused(retained_bytes).into())
    } else {
        Ok(ArtifactTransferError::Remote {
            error,
            retained_bytes,
        })
    }
}

fn ensure_captured_part_is_current(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
) -> Result<(), ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (part, part_identity, part_length, part_path) = part;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if part
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn durable_part_barrier(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    staging: &File,
    part_path: &Path,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::StagingSynced,
        file: staging,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    let identity =
        regular_file_identity(staging, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, &identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::StagingIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::DirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::NormalizationDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, &identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    authoritative
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativeRestatted,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let authoritative =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, &identity, &authoritative, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let retained_bytes = authoritative
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(staging, &identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(retained_bytes)
}
