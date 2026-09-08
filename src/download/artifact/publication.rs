use super::entry::{
    open_final_entry, open_invalid_entry, open_part_entry, open_restart_entry,
    rename_part_to_final_no_replace, rename_restart_to_final_no_replace,
    rename_restart_to_part_no_replace, unlink_part_entry,
};
use super::repair::{
    invalid_authority_is_present, remove_invalid_before_promotion, CapturedInvalidAuthority,
};
use super::ArtifactTransferError;
use crate::download::{ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure};
use crate::huggingface::ResolvedFile;
use crate::safe_file::{
    directory_identity, ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
use crate::verification::file::VerifiedRegularFile;
use std::fs::File;
use std::path::Path;

fn prove_complete_restart(
    directory: &File,
    verified: &VerifiedRegularFile,
    spec: &ResolvedFile,
    paths: (&Path, &Path, &Path, &Path),
    invalid_authority: bool,
) -> Result<(), ArtifactTransferError> {
    let (restart_path, part_path, final_path, invalid_path) = paths;
    let _restart = verified
        .resolve_proven_entry(restart_path, spec.size(), spec.sha256(), || {
            open_restart_entry(directory, restart_path)
        })
        .map_err(|_| ArtifactTransferError::Durability)?;
    for entry in [
        open_part_entry(directory, part_path),
        open_final_entry(directory, final_path),
    ] {
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    if invalid_authority_is_present(directory, invalid_path)? != invalid_authority {
        return Err(ArtifactTransferError::Durability);
    }
    Ok(())
}

pub(in crate::download) fn normalize_complete_restart(
    directory: &File,
    model_dir: &Path,
    spec: &ResolvedFile,
    verified: VerifiedRegularFile,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<VerifiedRegularFile, ArtifactTransferError> {
    let directory_identity =
        directory_identity(directory, model_dir).map_err(|_| ArtifactTransferError::Durability)?;
    let final_path = model_dir.join("model.gguf");
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let invalid_path = model_dir.join("model.gguf.invalid");
    ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let invalid_authority = invalid_authority_is_present(directory, &invalid_path)?;
    prove_complete_restart(
        directory,
        &verified,
        spec,
        (&restart_path, &part_path, &final_path, &invalid_path),
        invalid_authority,
    )?;
    ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforePromotion,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    prove_complete_restart(
        directory,
        &verified,
        spec,
        (&restart_path, &part_path, &final_path, &invalid_path),
        invalid_authority,
    )?;
    rename_restart_to_part_no_replace(directory).map_err(|_| ArtifactTransferError::Durability)?;
    verified
        .rebind_after_rename_with_fences(
            part_path,
            |part_path| {
                artifact_operation(ArtifactOperation::Sync {
                    checkpoint: ArtifactCheckpoint::NormalizationDirectorySynced,
                    file: directory,
                })
                .map_err(|_| ())?;
                ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
                    .map_err(|_| ())?;
                open_part_entry(directory, part_path).map_err(|_| ())
            },
            || {
                for entry in [
                    open_restart_entry(directory, &restart_path),
                    open_final_entry(directory, &final_path),
                ] {
                    match entry {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Ok(_) | Err(_) => return Err(()),
                    }
                }
                if invalid_authority_is_present(directory, &invalid_path).map_err(|_| ())?
                    != invalid_authority
                {
                    return Err(());
                }
                ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
                    .map_err(|_| ())
            },
        )
        .map_err(|_| ArtifactTransferError::Durability)
}

pub(super) fn promote_authoritative_part(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    paths: (&Path, &Path, &Path, &Path, &Path),
    part: &File,
    verified_identity: &RegularFileIdentity,
    expected_size: u64,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactTransferError> {
    let (model_dir, part_path, final_path, invalid_path, restart_path) = paths;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, verified_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    for entry in [
        open_invalid_entry(directory, invalid_path),
        open_restart_entry(directory, restart_path),
        open_final_entry(directory, final_path),
    ] {
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    rename_part_to_final_no_replace(directory).map_err(|_| ArtifactTransferError::Durability)?;
    let promoted_identity =
        regular_file_identity(part, final_path).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let final_file =
        open_final_entry(directory, final_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, &promoted_identity, &final_file, final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if final_file
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != expected_size
    {
        return Err(ArtifactTransferError::Durability);
    }
    for entry in [
        open_part_entry(directory, part_path),
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

pub(super) fn promote_authoritative_restart(
    directory: (&File, &DirectoryIdentity, &Path),
    restart: (&File, &RegularFileIdentity, &Path),
    part: (&File, &RegularFileIdentity, &Path),
    final_artifact: (Option<&CapturedInvalidAuthority>, &Path, &Path, u64),
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (restart, restart_identity, restart_path) = restart;
    let (part, part_identity, part_path) = part;
    let (invalid_authority, invalid_path, final_path, expected_size) = final_artifact;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    unlink_part_entry(directory).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if resolved
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != expected_size
    {
        return Err(ArtifactTransferError::Durability);
    }
    for entry in [
        open_part_entry(directory, part_path),
        open_final_entry(directory, final_path),
    ] {
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    if let Some(invalid_authority) = invalid_authority {
        remove_invalid_before_promotion(
            (directory, directory_identity, model_dir),
            (invalid_authority, invalid_path),
            (restart, restart_identity, restart_path, open_restart_entry),
            [(part_path, open_part_entry), (final_path, open_final_entry)],
            artifact_operation,
        )?;
    } else {
        match open_invalid_entry(directory, invalid_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    for entry in [
        open_part_entry(directory, part_path),
        open_invalid_entry(directory, invalid_path),
        open_final_entry(directory, final_path),
    ] {
        match entry {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(ArtifactTransferError::Durability),
        }
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    rename_restart_to_final_no_replace(directory).map_err(|_| ArtifactTransferError::Durability)?;
    let promoted_identity = regular_file_identity(restart, final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let final_file =
        open_final_entry(directory, final_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &promoted_identity, &final_file, final_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if final_file
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != expected_size
    {
        return Err(ArtifactTransferError::Durability);
    }
    for entry in [
        open_part_entry(directory, part_path),
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
