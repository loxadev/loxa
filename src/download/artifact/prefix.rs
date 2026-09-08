mod cleanup;

use super::entry::{exchange_part_and_restart, open_part_entry, open_restart_entry};
use super::staging::durable_part_barrier;
use super::ArtifactTransferError;
use crate::download::{ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
pub(super) use cleanup::{
    remove_invalid_authoritative_part, remove_invalid_restart_and_recover_part,
    remove_restart_after_authoritative_part_is_durable,
};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(super) fn normalize_durable_prefix(
    ignored_range: bool,
    directory: (&File, &DirectoryIdentity, &Path),
    staging: (&File, &Path),
    recovered_part: Option<(&File, &RegularFileIdentity, u64, &Path)>,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (staging, staging_path) = staging;
    if ignored_range {
        let part = recovered_part.ok_or(ArtifactTransferError::Durability)?;
        normalize_restart_prefix(
            directory,
            directory_identity,
            model_dir,
            (staging, staging_path),
            part,
            None,
            artifact_operation,
        )
    } else {
        durable_part_barrier(
            directory,
            directory_identity,
            model_dir,
            staging,
            staging_path,
            artifact_operation,
        )
    }
}

fn open_captured_prefixes(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
    restart: (&File, &RegularFileIdentity, u64, &Path),
) -> Result<(File, File), ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (part, part_identity, part_length, part_path) = part;
    let (restart, restart_identity, restart_length, restart_path) = restart;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let part_reader =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &part_reader, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let restart_reader = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &restart_reader, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if part_reader
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
        || restart_reader
            .metadata()
            .map_err(|_| ArtifactTransferError::Durability)?
            .len()
            != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok((part_reader, restart_reader))
}

pub(super) fn captured_prefixes_match(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
    restart: (&File, &RegularFileIdentity, u64, &Path),
    should_pause: &impl Fn() -> bool,
) -> Result<Option<bool>, ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (part, part_identity, part_length, part_path) = part;
    let (restart, restart_identity, restart_length, restart_path) = restart;
    if should_pause() {
        return Ok(None);
    }
    let (mut part_reader, mut restart_reader) = open_captured_prefixes(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
        (restart, restart_identity, restart_length, restart_path),
    )?;
    let mut remaining = part_length.min(restart_length);
    let mut part_buffer = [0_u8; 64 * 1024];
    let mut restart_buffer = [0_u8; 64 * 1024];
    let mut matches = true;
    while remaining > 0 {
        if should_pause() {
            return Ok(None);
        }
        let read_length = remaining.min(part_buffer.len() as u64) as usize;
        part_reader
            .read_exact(&mut part_buffer[..read_length])
            .map_err(|_| ArtifactTransferError::Durability)?;
        restart_reader
            .read_exact(&mut restart_buffer[..read_length])
            .map_err(|_| ArtifactTransferError::Durability)?;
        if should_pause() {
            return Ok(None);
        }
        if part_buffer[..read_length] != restart_buffer[..read_length] {
            matches = false;
            break;
        }
        remaining -= read_length as u64;
    }
    open_captured_prefixes(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
        (restart, restart_identity, restart_length, restart_path),
    )?;
    if should_pause() {
        return Ok(None);
    }
    open_captured_prefixes(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
        (restart, restart_identity, restart_length, restart_path),
    )?;
    Ok(Some(matches))
}

pub(super) fn normalize_restart_prefix(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    model_dir: &Path,
    restart: (&File, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
    startup_expectation: Option<(&RegularFileIdentity, u64, bool)>,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let (restart, restart_path) = restart;
    let (part, part_identity, part_length, part_path) = part;
    let (expected_restart, allow_longer_exchange) = match startup_expectation {
        Some((identity, length, allow_longer_exchange)) => {
            (Some((identity, length)), allow_longer_exchange)
        }
        None => (None, true),
    };
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if let Some((restart_identity, restart_length)) = expected_restart {
        let resolved = open_restart_entry(directory, restart_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        if restart
            .metadata()
            .map_err(|_| ArtifactTransferError::Durability)?
            .len()
            != restart_length
        {
            return Err(ArtifactTransferError::Durability);
        }
    }
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::StagingSynced,
        file: restart,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    let restart_identity = regular_file_identity(restart, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if let Some((expected_identity, expected_length)) = expected_restart {
        if &restart_identity != expected_identity
            || restart
                .metadata()
                .map_err(|_| ArtifactTransferError::Durability)?
                .len()
                != expected_length
        {
            return Err(ArtifactTransferError::Durability);
        }
    }
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::StagingIdentityMatched,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let restart_length = restart
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    if restart_length <= part_length || !allow_longer_exchange {
        return retain_old_part_after_nonlonger_restart(
            (directory, directory_identity, model_dir),
            (restart, &restart_identity, restart_length, restart_path),
            (part, part_identity, part_length, part_path),
            artifact_operation,
        );
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforePrefixExchange,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let resolved =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, part_identity, &resolved, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    exchange_part_and_restart(directory).map_err(|_| ArtifactTransferError::Durability)?;
    let normalized_part_identity =
        regular_file_identity(restart, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    let old_part_identity =
        regular_file_identity(part, restart_path).map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::PrefixExchanged,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let normalized =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &normalized_part_identity, &normalized, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let old = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(part, &old_part_identity, &old, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::DirectorySynced,
        file: directory,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let normalized =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &normalized_part_identity, &normalized, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if normalized
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativeRestatted,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let normalized =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &normalized_part_identity, &normalized, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let retained_bytes = normalized
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|_| ArtifactTransferError::Durability)?;
    let normalized =
        open_part_entry(directory, part_path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, &normalized_part_identity, &normalized, part_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    remove_restart_after_authoritative_part_is_durable(
        (directory, directory_identity, model_dir),
        (
            restart,
            &normalized_part_identity,
            retained_bytes,
            part_path,
        ),
        (part, &old_part_identity, part_length, restart_path),
        artifact_operation,
    )
}

fn retain_old_part_after_nonlonger_restart(
    directory: (&File, &DirectoryIdentity, &Path),
    restart: (&File, &RegularFileIdentity, u64, &Path),
    part: (&File, &RegularFileIdentity, u64, &Path),
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<u64, ArtifactTransferError> {
    let (directory, directory_identity, model_dir) = directory;
    let (restart, restart_identity, restart_length, restart_path) = restart;
    let (part, part_identity, part_length, part_path) = part;
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
    if resolved
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if resolved
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::DirectorySynced,
        file: directory,
    })
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
    if authoritative
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != part_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    let resolved = open_restart_entry(directory, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(restart, restart_identity, &resolved, restart_path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if resolved
        .metadata()
        .map_err(|_| ArtifactTransferError::Durability)?
        .len()
        != restart_length
    {
        return Err(ArtifactTransferError::Durability);
    }
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    remove_restart_after_authoritative_part_is_durable(
        (directory, directory_identity, model_dir),
        (part, part_identity, part_length, part_path),
        (restart, restart_identity, restart_length, restart_path),
        artifact_operation,
    )
}
