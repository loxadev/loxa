use super::http::{artifact_url, wait_with_pause, Transfer, TransferError, Transport, WaitOutcome};
use super::plan::plan_artifact_discard_inner;
use super::{
    ArtifactCheckpoint, ArtifactDiscardError, ArtifactDiscardFacts, ArtifactOperation,
    ArtifactOperationFailure, DownloadCompletion, DownloadOutcome, IntegrityAuthority,
    ProgressUpdate,
};
use crate::huggingface::ResolvedFile;
use crate::safe_file::{
    directory_identity, ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    open_directory, regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
#[cfg(test)]
pub(super) use crate::verification::file::hex;
use crate::verification::file::{
    verify_regular_entry_controlled, VerificationOutcome, VerifiedRegularFile,
};
use reqwest::StatusCode;
#[cfg(not(unix))]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

pub(super) enum ArtifactTransferError {
    RemoteBeforeBody(TransferError),
    Transfer(TransferError),
    Remote {
        error: TransferError,
        retained_bytes: u64,
    },
    Integrity {
        retained_bytes: u64,
        authority: IntegrityAuthority,
    },
    Durability,
    DiskExhausted {
        retained_bytes: u64,
    },
}

enum CopyFailure {
    Transfer(TransferError),
    DiskExhausted,
    Durability,
}

struct CapturedInvalidAuthority {
    file: File,
    identity: RegularFileIdentity,
}

type OpenArtifactEntry = fn(&File, &Path) -> std::io::Result<File>;

impl From<TransferError> for CopyFailure {
    fn from(error: TransferError) -> Self {
        Self::Transfer(error)
    }
}

impl From<TransferError> for ArtifactTransferError {
    fn from(error: TransferError) -> Self {
        Self::Transfer(error)
    }
}

impl From<String> for ArtifactTransferError {
    fn from(message: String) -> Self {
        Self::Transfer(TransferError::fatal(message))
    }
}

impl From<&str> for ArtifactTransferError {
    fn from(message: &str) -> Self {
        Self::Transfer(TransferError::fatal(message))
    }
}

pub(super) struct DownloadRequest<'a> {
    pub(super) spec: &'a ResolvedFile,
    pub(super) directory: super::DownloadDirectoryAuthority<'a>,
    pub(super) verified_part: Option<VerifiedRegularFile>,
}

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

pub(super) fn normalize_complete_restart(
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

pub(super) async fn download_once(
    request: DownloadRequest<'_>,
    transport: &impl Transport,
    progress: &mut impl FnMut(ProgressUpdate),
    should_pause: &impl Fn() -> bool,
    prefix_must_be_reproved: bool,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<DownloadCompletion, ArtifactTransferError> {
    let DownloadRequest {
        spec,
        directory: directory_authority,
        verified_part,
    } = request;
    let model_dir = directory_authority.as_ref();
    let retained_directory_authority = directory_authority.retained_directory().is_some();
    let directory_failure = || {
        if directory_authority.retained_directory().is_some()
            || prefix_must_be_reproved
            || verified_part.is_some()
        {
            ArtifactTransferError::Durability
        } else {
            TransferError::fatal("unsafe model directory").into()
        }
    };
    let (directory, directory_identity) =
        if let Some(retained) = directory_authority.retained_directory() {
            let directory = retained.try_clone().map_err(|_| directory_failure())?;
            let identity =
                directory_identity(&directory, model_dir).map_err(|_| directory_failure())?;
            (directory, identity)
        } else {
            if !prefix_must_be_reproved {
                fs::create_dir_all(model_dir).map_err(|error| error.to_string())?;
            }
            open_directory(model_dir).map_err(|_| directory_failure())?
        };
    ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
        .map_err(|_| directory_failure())?;
    let final_path = model_dir.join("model.gguf");
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let invalid_path = model_dir.join("model.gguf.invalid");
    let mut captured_invalid_authority = capture_invalid_authority(&directory, &invalid_path)?;
    let initial_invalid_authority = captured_invalid_authority.is_some();
    ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if retained_directory_authority {
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeFinalInspection,
        ))
        .map_err(|_| ArtifactTransferError::Durability)?;
    }
    let mut integrity_authority = if initial_invalid_authority {
        IntegrityAuthority::Repair
    } else {
        IntegrityAuthority::PendingOnly
    };
    if let Some((final_file, final_identity)) =
        capture_artifact_entry(&directory, &final_path, open_final_entry)?
    {
        if final_identity.size() == spec.size() {
            match verify_regular_entry_controlled(
                &directory,
                &final_path,
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
                    finish_repair(&directory, &directory_identity, model_dir)?;
                    return Ok(DownloadCompletion::new(
                        DownloadOutcome::AlreadyInstalled(final_path),
                        verified,
                    ));
                }
                Ok(VerificationOutcome::ChecksumMismatch) => {}
                Ok(VerificationOutcome::Interrupted) | Err(_) => {
                    return Err(ArtifactTransferError::Durability);
                }
            }
        }
        if integrity_authority == IntegrityAuthority::Repair
            || invalid_authority_is_present(&directory, &invalid_path)?
        {
            return Err("a prior corrupt artifact repair is still pending".into());
        }
        captured_invalid_authority = Some(quarantine_captured_final(
            &directory,
            (&final_file, &final_identity, &final_path),
            &invalid_path,
        )?);
        integrity_authority = IntegrityAuthority::Repair;
    }
    reject_unsafe_artifact_entry_if_present(&directory, &part_path, open_part_entry).map_err(
        |error| {
            if prefix_must_be_reproved {
                ArtifactTransferError::Durability
            } else {
                error.into()
            }
        },
    )?;
    reject_unsafe_artifact_entry_if_present(&directory, &restart_path, open_restart_entry)
        .map_err(|error| {
            if prefix_must_be_reproved {
                ArtifactTransferError::Durability
            } else {
                error.into()
            }
        })?;
    ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    let mut recovered_part = match open_part_entry(&directory, &part_path) {
        Ok(part) => {
            let identity = regular_file_identity(&part, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let resolved = open_part_entry(&directory, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_regular_descriptors_match(&part, &identity, &resolved, &part_path)
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
    let recovered_restart = match open_restart_entry(&directory, &restart_path) {
        Ok(restart) => Some(restart),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    match (recovered_restart, recovered_part.as_ref()) {
        (Some(restart), Some((part, part_identity, part_length))) if *part_length < spec.size() => {
            let restart_identity = regular_file_identity(&restart, &restart_path)
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
                return Err(TransferError::paused(0).into());
            }
            let prefixes_match = if restart_length > *part_length {
                let Some(prefixes_match) = captured_prefixes_match(
                    (&directory, &directory_identity, model_dir),
                    (part, part_identity, *part_length, &part_path),
                    (&restart, &restart_identity, restart_length, &restart_path),
                    should_pause,
                )?
                else {
                    return Err(TransferError::paused(0).into());
                };
                prefixes_match
            } else {
                false
            };
            normalize_restart_prefix(
                &directory,
                &directory_identity,
                model_dir,
                (&restart, &restart_path),
                (part, part_identity, *part_length, &part_path),
                Some((
                    &restart_identity,
                    restart_length,
                    restart_length > *part_length && prefixes_match,
                )),
                artifact_operation,
            )?;
            let part = open_part_entry(&directory, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let identity = regular_file_identity(&part, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let resolved = open_part_entry(&directory, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_regular_descriptors_match(&part, &identity, &resolved, &part_path)
                .map_err(|_| ArtifactTransferError::Durability)?;
            let length = part
                .metadata()
                .map(|metadata| metadata.len())
                .map_err(|_| ArtifactTransferError::Durability)?;
            ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
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
    let mut offset = recovered_part
        .as_ref()
        .map(|(_, _, length)| *length)
        .unwrap_or(0);
    if offset > spec.size() {
        let (part, part_identity, _) = recovered_part
            .as_ref()
            .ok_or(ArtifactTransferError::Durability)?;
        remove_invalid_authoritative_part(
            &directory,
            &directory_identity,
            model_dir,
            part,
            part_identity,
            &part_path,
            artifact_operation,
        )?;
        offset = 0;
        recovered_part = None;
    }
    if prefix_must_be_reproved && recovered_part.is_none() {
        return Err(ArtifactTransferError::Durability);
    }
    progress(ProgressUpdate::Transferring {
        transferred: offset,
        total: spec.size(),
    });
    if offset == spec.size() && offset > 0 {
        let repairing = integrity_authority == IntegrityAuthority::Repair;
        if repairing && (verified_part.is_none() || captured_invalid_authority.is_none()) {
            return Err(ArtifactTransferError::Durability);
        }
        let invalid_authority = repairing
            .then_some(captured_invalid_authority.as_ref())
            .flatten();
        ensure_complete_part_authority(
            &directory,
            &directory_identity,
            model_dir,
            &invalid_path,
            &restart_path,
            invalid_authority,
        )?;
        let (part, part_identity, _) = recovered_part
            .as_ref()
            .ok_or(ArtifactTransferError::Durability)?;
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
                let resolved = open_part_entry(&directory, &part_path)
                    .map_err(|_| ArtifactTransferError::Durability)?;
                verified
                    .proves_resolved(&part_path, spec.size(), spec.sha256(), resolved)
                    .map_err(|_| ArtifactTransferError::Durability)?;
                verified
            }
            None => match verify_regular_entry_controlled(
                &directory,
                &part_path,
                open_part_entry,
                spec.size(),
                spec.sha256(),
                Some((part, part_identity)),
                should_pause,
            ) {
                Ok(VerificationOutcome::Verified(verified)) => verified,
                Ok(VerificationOutcome::Interrupted) => {
                    let retained_bytes = durable_part_barrier(
                        &directory,
                        &directory_identity,
                        model_dir,
                        part,
                        &part_path,
                        artifact_operation,
                    )?;
                    ensure_complete_part_authority(
                        &directory,
                        &directory_identity,
                        model_dir,
                        &invalid_path,
                        &restart_path,
                        invalid_authority,
                    )?;
                    return Err(TransferError::paused(retained_bytes).into());
                }
                Ok(VerificationOutcome::ChecksumMismatch) => {
                    remove_invalid_authoritative_part(
                        &directory,
                        &directory_identity,
                        model_dir,
                        part,
                        part_identity,
                        &part_path,
                        artifact_operation,
                    )?;
                    integrity_authority = integrity_authority_at_fence(
                        &directory,
                        &directory_identity,
                        model_dir,
                        &invalid_path,
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
            &directory,
            &directory_identity,
            model_dir,
            &invalid_path,
            &restart_path,
            invalid_authority,
        )?;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforePromotion,
        ))
        .map_err(|_| ArtifactTransferError::Durability)?;
        if should_pause() {
            let retained_bytes = durable_part_barrier(
                &directory,
                &directory_identity,
                model_dir,
                part,
                &part_path,
                artifact_operation,
            )?;
            ensure_complete_part_authority(
                &directory,
                &directory_identity,
                model_dir,
                &invalid_path,
                &restart_path,
                invalid_authority,
            )?;
            return Err(TransferError::paused(retained_bytes).into());
        }
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::CompletionFencePassed,
        ))
        .map_err(|_| ArtifactTransferError::Durability)?;
        ensure_complete_part_authority(
            &directory,
            &directory_identity,
            model_dir,
            &invalid_path,
            &restart_path,
            invalid_authority,
        )?;
        if let Some(invalid_authority) = invalid_authority {
            remove_invalid_before_promotion(
                (&directory, &directory_identity, model_dir),
                (invalid_authority, &invalid_path),
                (part, part_identity, &part_path, open_part_entry),
                [
                    (&restart_path, open_restart_entry),
                    (&final_path, open_final_entry),
                ],
                artifact_operation,
            )?;
        }
        promote_authoritative_part(
            &directory,
            &directory_identity,
            (
                model_dir,
                &part_path,
                &final_path,
                &invalid_path,
                &restart_path,
            ),
            part,
            part_identity,
            spec.size(),
            artifact_operation,
        )?;
        ensure_repair_debris_absent(
            &directory,
            &directory_identity,
            model_dir,
            &invalid_path,
            &restart_path,
        )?;
        let resolved = open_final_entry(&directory, &final_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        let verified = verified
            .rebind_after_rename_resolved(&final_path, resolved)
            .map_err(|_| ArtifactTransferError::Durability)?;
        return Ok(DownloadCompletion::new(
            DownloadOutcome::Pulled(final_path),
            verified,
        ));
    }
    let mut transfer = match wait_with_pause(
        transport.get(
            &artifact_url(spec)?,
            (offset > 0).then_some(offset),
            should_pause,
        ),
        should_pause,
    )
    .await
    {
        WaitOutcome::Ready(Ok(transfer)) => transfer,
        WaitOutcome::Ready(Err(error)) => {
            return Err(classify_pre_body_terminal(
                error,
                (&directory, &directory_identity, model_dir),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?)
        }
        WaitOutcome::Paused => {
            return Err(classify_pre_body_terminal(
                TransferError::paused(0),
                (&directory, &directory_identity, model_dir),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?)
        }
    };
    let ignored_range = offset > 0 && transfer.status == StatusCode::OK;
    let (target, append) = if ignored_range {
        progress(ProgressUpdate::Transferring {
            transferred: 0,
            total: spec.size(),
        });
        (&restart_path, false)
    } else {
        if transfer.status == StatusCode::PARTIAL_CONTENT {
            if let Err(message) =
                validate_content_range(transfer.content_range.as_deref(), offset, spec.size())
            {
                return Err(classify_pre_body_terminal(
                    TransferError::fatal(message),
                    (&directory, &directory_identity, model_dir),
                    recovered_part.as_ref().map(|(part, identity, length)| {
                        (part, identity, *length, part_path.as_path())
                    }),
                    artifact_operation,
                )?);
            }
        } else if transfer.status != StatusCode::OK || offset > 0 {
            return Err(classify_pre_body_terminal(
                TransferError::fatal(format!("unexpected artifact HTTP {}", transfer.status)),
                (&directory, &directory_identity, model_dir),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?);
        }
        (&part_path, offset > 0)
    };
    ensure_directory_descriptor_matches_path(&directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactTransferError::Durability)?;
    if retained_directory_authority {
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeStagingOpen,
        ))
        .map_err(|_| ArtifactTransferError::Durability)?;
    }
    let expected_written = if append {
        spec.size() - offset
    } else {
        spec.size()
    };
    let read_limit = expected_written
        .checked_add(1)
        .ok_or_else(|| "artifact is too large".to_string())?;
    let captured_part_output = target == &part_path && recovered_part.is_some();
    let mut output = if target == &restart_path {
        create_restart_entry(&directory, target)
    } else if captured_part_output {
        open_part_entry_for_write(&directory, target, append)
    } else {
        create_part_entry(&directory, target)
    }
    .map_err(|_| {
        if captured_part_output {
            ArtifactTransferError::Durability
        } else {
            directory_failure()
        }
    })?;
    reject_unsafe_open_transfer(&output, target).map_err(|_| ArtifactTransferError::Durability)?;
    if target == &part_path {
        if let Some((part, identity, _)) = recovered_part.as_ref() {
            ensure_regular_descriptors_match(part, identity, &output, target)
                .map_err(|_| ArtifactTransferError::Durability)?;
        }
    }
    if !append {
        output
            .set_len(0)
            .map_err(|error| format!("{}: {error}", target.display()))?;
    }
    let progress_offset = if append { offset } else { 0 };
    let copied = match copy_bounded(
        &mut transfer,
        &mut output,
        read_limit,
        should_pause,
        artifact_operation,
        |copied| {
            progress(ProgressUpdate::Transferring {
                transferred: progress_offset.saturating_add(copied),
                total: spec.size(),
            })
        },
    )
    .await
    {
        Ok(copied) => copied,
        Err(CopyFailure::Transfer(error)) => {
            let retained_bytes = normalize_durable_prefix(
                ignored_range,
                (&directory, &directory_identity, model_dir),
                (&output, target),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?;
            if error.is_paused() {
                return Err(TransferError::paused(retained_bytes).into());
            }
            return Err(ArtifactTransferError::Remote {
                error,
                retained_bytes,
            });
        }
        Err(CopyFailure::Durability) => return Err(ArtifactTransferError::Durability),
        Err(CopyFailure::DiskExhausted) => {
            let retained_bytes = normalize_durable_prefix(
                ignored_range,
                (&directory, &directory_identity, model_dir),
                (&output, target),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?;
            return Err(ArtifactTransferError::DiskExhausted { retained_bytes });
        }
    };
    if copied > expected_written {
        let staging_identity = regular_file_identity(&output, target)
            .map_err(|_| ArtifactTransferError::Durability)?;
        let retained_bytes = if ignored_range {
            let (part, part_identity, _) = recovered_part
                .as_ref()
                .ok_or(ArtifactTransferError::Durability)?;
            remove_invalid_restart_and_recover_part(
                &directory,
                &directory_identity,
                model_dir,
                (&output, &staging_identity, &restart_path),
                (part, part_identity, &part_path),
                artifact_operation,
            )?
        } else {
            remove_invalid_authoritative_part(
                &directory,
                &directory_identity,
                model_dir,
                &output,
                &staging_identity,
                &part_path,
                artifact_operation,
            )?;
            0
        };
        return Err(ArtifactTransferError::Remote {
            error: TransferError::fatal("artifact response exceeded expected size"),
            retained_bytes,
        });
    }
    if copied != expected_written {
        let retained_bytes = normalize_durable_prefix(
            ignored_range,
            (&directory, &directory_identity, model_dir),
            (&output, target),
            recovered_part
                .as_ref()
                .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
            artifact_operation,
        )?;
        return Err(ArtifactTransferError::Remote {
            error: TransferError::retryable("artifact response ended before expected size"),
            retained_bytes,
        });
    }
    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::StagingSynced,
        file: &output,
    })
    .map_err(|_| ArtifactTransferError::Durability)?;
    progress(ProgressUpdate::Verifying {
        transferred: spec.size(),
        total: spec.size(),
    });
    let staging_identity = regular_file_identity(&output, target)
        .map_err(|_| TransferError::fatal("unsafe staging artifact"))?;
    let open_staging = if target == &restart_path {
        open_restart_entry
    } else {
        open_part_entry
    };
    let verified = match verify_regular_entry_controlled(
        &directory,
        target,
        open_staging,
        spec.size(),
        spec.sha256(),
        Some((&output, &staging_identity)),
        should_pause,
    ) {
        Ok(VerificationOutcome::Verified(verified)) => verified,
        Ok(VerificationOutcome::Interrupted) => {
            let retained_bytes = normalize_durable_prefix(
                ignored_range,
                (&directory, &directory_identity, model_dir),
                (&output, target),
                recovered_part
                    .as_ref()
                    .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
                artifact_operation,
            )?;
            return Err(TransferError::paused(retained_bytes).into());
        }
        Ok(VerificationOutcome::ChecksumMismatch) if !ignored_range => {
            remove_invalid_authoritative_part(
                &directory,
                &directory_identity,
                model_dir,
                &output,
                &staging_identity,
                &part_path,
                artifact_operation,
            )?;
            integrity_authority = integrity_authority_at_fence(
                &directory,
                &directory_identity,
                model_dir,
                &invalid_path,
                integrity_authority,
                artifact_operation,
            )?;
            return Err(ArtifactTransferError::Integrity {
                retained_bytes: 0,
                authority: integrity_authority,
            });
        }
        Ok(VerificationOutcome::ChecksumMismatch) => {
            let (part, part_identity, _) = recovered_part
                .as_ref()
                .ok_or(ArtifactTransferError::Durability)?;
            let retained_bytes = remove_invalid_restart_and_recover_part(
                &directory,
                &directory_identity,
                model_dir,
                (&output, &staging_identity, &restart_path),
                (part, part_identity, &part_path),
                artifact_operation,
            )?;
            integrity_authority = integrity_authority_at_fence(
                &directory,
                &directory_identity,
                model_dir,
                &invalid_path,
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
            (&directory, &directory_identity, model_dir),
            (&output, target),
            recovered_part
                .as_ref()
                .map(|(part, identity, length)| (part, identity, *length, part_path.as_path())),
            artifact_operation,
        )?;
        return Err(TransferError::paused(retained_bytes).into());
    }
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::CompletionFencePassed,
    ))
    .map_err(|_| ArtifactTransferError::Durability)?;
    if !ignored_range {
        if let Some(invalid_authority) = captured_invalid_authority.as_ref() {
            remove_invalid_before_promotion(
                (&directory, &directory_identity, model_dir),
                (invalid_authority, &invalid_path),
                (&output, &staging_identity, &part_path, open_part_entry),
                [
                    (&restart_path, open_restart_entry),
                    (&final_path, open_final_entry),
                ],
                artifact_operation,
            )?;
        } else {
            ensure_repair_debris_absent(
                &directory,
                &directory_identity,
                model_dir,
                &invalid_path,
                &restart_path,
            )?;
        }
        promote_authoritative_part(
            &directory,
            &directory_identity,
            (
                model_dir,
                &part_path,
                &final_path,
                &invalid_path,
                &restart_path,
            ),
            &output,
            &staging_identity,
            spec.size(),
            artifact_operation,
        )?;
        let resolved = open_final_entry(&directory, &final_path)
            .map_err(|_| ArtifactTransferError::Durability)?;
        let verified = verified
            .rebind_after_rename_resolved(&final_path, resolved)
            .map_err(|_| ArtifactTransferError::Durability)?;
        return Ok(DownloadCompletion::new(
            DownloadOutcome::Pulled(final_path),
            verified,
        ));
    }
    let (part, part_identity, _) = recovered_part
        .as_ref()
        .ok_or(ArtifactTransferError::Durability)?;
    promote_authoritative_restart(
        (&directory, &directory_identity, model_dir),
        (&output, &staging_identity, &restart_path),
        (part, part_identity, &part_path),
        (
            captured_invalid_authority.as_ref(),
            &invalid_path,
            &final_path,
            spec.size(),
        ),
        artifact_operation,
    )?;
    let resolved =
        open_final_entry(&directory, &final_path).map_err(|_| ArtifactTransferError::Durability)?;
    let verified = verified
        .rebind_after_rename_resolved(&final_path, resolved)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(DownloadCompletion::new(
        DownloadOutcome::Pulled(final_path),
        verified,
    ))
}

pub(super) fn prove_existing_part_for_pause(
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

async fn copy_bounded(
    transfer: &mut Transfer,
    output: &mut File,
    limit: u64,
    should_pause: &impl Fn() -> bool,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
    mut progress: impl FnMut(u64),
) -> Result<u64, CopyFailure> {
    let mut copied = 0;
    while copied < limit {
        let remaining = (limit - copied).min(64 * 1024) as usize;
        let chunk = match wait_with_pause(transfer.chunk(remaining), should_pause).await {
            WaitOutcome::Ready(Ok(Some(chunk))) => chunk,
            WaitOutcome::Ready(Ok(None)) => break,
            WaitOutcome::Ready(Err(error)) => {
                return Err(error.into());
            }
            WaitOutcome::Paused => return Err(TransferError::paused(0).into()),
        };
        if chunk.is_empty() {
            continue;
        }
        let read = chunk.len();
        match artifact_operation(ArtifactOperation::Write {
            file: output,
            bytes: &chunk[..read],
        }) {
            Ok(()) => {}
            Err(ArtifactOperationFailure::DiskExhausted) => {
                return Err(CopyFailure::DiskExhausted);
            }
            Err(ArtifactOperationFailure::Other) => {
                return Err(CopyFailure::Durability);
            }
        }
        copied += read as u64;
        progress(copied);
    }
    Ok(copied)
}

fn ensure_repair_debris_absent(
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

fn ensure_complete_part_authority(
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

fn remove_invalid_before_promotion(
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

fn promote_authoritative_part(
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

fn promote_authoritative_restart(
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rename_part_to_final_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part",
        directory,
        "model.gguf",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rename_final_to_invalid_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf",
        directory,
        "model.gguf.invalid",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rename_restart_to_final_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part.restart",
        directory,
        "model.gguf",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rename_restart_to_part_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part.restart",
        directory,
        "model.gguf.part",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_restart_to_part_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace restart normalization is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_restart_to_final_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace restart promotion is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_part_to_final_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace promotion is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_final_to_invalid_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative corrupt-artifact quarantine is unsupported",
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn exchange_part_and_restart(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part",
        directory,
        "model.gguf.part.restart",
        RenameFlags::EXCHANGE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn exchange_part_and_restart(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative prefix exchange is unsupported",
    ))
}

fn finish_repair(
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

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn unlink_invalid_entry(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{unlinkat, AtFlags};

    unlinkat(directory, "model.gguf.invalid", AtFlags::empty())
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn unlink_invalid_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative repair cleanup is unsupported",
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn unlink_repair_entry_if_present(directory: &File, name: &[u8]) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the retained live model-directory descriptor and
    // callers provide only fixed NUL-terminated repair entry names.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr().cast(), 0) };
    if result == -1 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn unlink_repair_entry_if_present(_directory: &File, _name: &[u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative repair cleanup is unsupported",
    ))
}

fn remove_invalid_authoritative_part(
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

fn remove_invalid_restart_and_recover_part(
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

fn normalize_durable_prefix(
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

fn classify_pre_body_terminal(
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

fn captured_prefixes_match(
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

fn normalize_restart_prefix(
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

fn remove_restart_after_authoritative_part_is_durable(
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

fn durable_part_barrier(
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

fn integrity_authority_at_fence(
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

fn invalid_authority_is_present(
    directory: &File,
    invalid_path: &Path,
) -> Result<bool, ArtifactTransferError> {
    capture_invalid_authority(directory, invalid_path).map(|authority| authority.is_some())
}

fn capture_invalid_authority(
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

fn capture_artifact_entry(
    directory: &File,
    path: &Path,
    open_entry: OpenArtifactEntry,
) -> Result<Option<(File, RegularFileIdentity)>, ArtifactTransferError> {
    let file = match open_entry(directory, path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    let identity =
        regular_file_identity(&file, path).map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_entry(directory, path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(Some((file, identity)))
}

fn quarantine_captured_final(
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

fn reject_unsafe_artifact_entry_if_present(
    directory: &File,
    path: &Path,
    open_entry: OpenArtifactEntry,
) -> Result<(), String> {
    let file = match open_entry(directory, path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let identity = regular_file_identity(&file, path).map_err(|error| error.to_string())?;
    let resolved =
        open_entry(directory, path).map_err(|error| format!("{}: {error}", path.display()))?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, path)
        .map_err(|_| format!("unsafe artifact path {}", path.display()))
}

#[cfg(unix)]
fn create_part_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    open_artifact_entry_for_write(directory, c"model.gguf.part", libc::O_CREAT | libc::O_EXCL)
}

#[cfg(not(unix))]
fn create_part_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

#[cfg(unix)]
fn create_restart_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    open_artifact_entry_for_write(
        directory,
        c"model.gguf.part.restart",
        libc::O_CREAT | libc::O_EXCL,
    )
}

#[cfg(not(unix))]
fn create_restart_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

#[cfg(unix)]
fn open_part_entry_for_write(
    directory: &File,
    _path: &Path,
    append: bool,
) -> std::io::Result<File> {
    open_artifact_entry_for_write(
        directory,
        c"model.gguf.part",
        if append { libc::O_APPEND } else { 0 },
    )
}

#[cfg(not(unix))]
fn open_part_entry_for_write(
    _directory: &File,
    path: &Path,
    append: bool,
) -> std::io::Result<File> {
    OpenOptions::new().write(true).append(append).open(path)
}

#[cfg(unix)]
fn open_artifact_entry_for_write(
    directory: &File,
    name: &std::ffi::CStr,
    extra_flags: libc::c_int,
) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model-directory descriptor, callers
    // provide only closed artifact literals, and a successful descriptor is
    // transferred to `File` exactly once.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_WRONLY | extra_flags,
            0o600,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
fn open_final_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
fn open_final_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.open(path)
}

#[cfg(unix)]
fn open_part_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.part".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
fn open_part_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
fn open_restart_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model directory descriptor, the name
    // is the closed restart literal, and a successful descriptor is owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.part.restart".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
fn open_restart_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
fn open_invalid_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model directory descriptor, the name
    // is the closed invalid-artifact literal, and a successful descriptor is
    // owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.invalid".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
fn open_invalid_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
fn unlink_part_entry(directory: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the pinned model directory descriptor and the
    // NUL-terminated name is the one closed authoritative part literal.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), c"model.gguf.part".as_ptr(), 0) };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn unlink_part_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative artifact cleanup is unsupported",
    ))
}

#[cfg(unix)]
fn unlink_restart_entry(directory: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the pinned model directory descriptor and the
    // NUL-terminated name is the one closed restart literal.
    let result = unsafe {
        libc::unlinkat(
            directory.as_raw_fd(),
            c"model.gguf.part.restart".as_ptr(),
            0,
        )
    };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn unlink_restart_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative artifact cleanup is unsupported",
    ))
}

pub(super) fn discard_artifact_bytes_inner(
    directory: &File,
    model_dir: &Path,
    facts: ArtifactDiscardFacts,
) -> Result<(), ArtifactDiscardError> {
    discard_artifact_bytes_controlled(
        (directory, model_dir),
        facts,
        &mut super::perform_artifact_operation,
    )
}

pub(super) fn discard_artifact_bytes_controlled(
    directory: (&File, &Path),
    facts: ArtifactDiscardFacts,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactDiscardError> {
    let (directory_file, model_dir) = directory;
    ensure_directory_descriptor_matches_path(directory_file, facts.directory_identity(), model_dir)
        .map_err(|_| ArtifactDiscardError::Changed)?;
    let current = plan_artifact_discard_inner(directory_file, model_dir)?;
    if current != facts {
        return Err(ArtifactDiscardError::Changed);
    }
    let (directory_identity, part, restart) = facts.into_entries();
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let final_path = model_dir.join("model.gguf");
    let invalid_path = model_dir.join("model.gguf.invalid");
    let ensure_current = |part, restart, error| {
        ensure_discard_state(
            (directory_file, &directory_identity, model_dir),
            (part, &part_path),
            (restart, &restart_path),
            (&final_path, &invalid_path),
        )
        .map_err(|_| error)
    };
    let mut deleted = false;
    if restart.is_some() {
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeRestartUnlink,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            restart.as_ref(),
            ArtifactDiscardError::Changed,
        )?;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::RestartIdentityMatched,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            restart.as_ref(),
            ArtifactDiscardError::Changed,
        )?;
        unlink_restart_entry(directory_file).map_err(|_| ArtifactDiscardError::Durability)?;
        deleted = true;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::RestartUnlinked,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(part.as_ref(), None, ArtifactDiscardError::Durability)?;
    }
    if part.is_some() {
        let remaining_restart = if deleted { None } else { restart.as_ref() };
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            remaining_restart,
            discard_state_error(deleted),
        )?;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::AuthoritativePartIdentityMatched,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            remaining_restart,
            discard_state_error(deleted),
        )?;
        unlink_part_entry(directory_file).map_err(|_| ArtifactDiscardError::Durability)?;
        deleted = true;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::AuthoritativePartUnlinked,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(None, remaining_restart, ArtifactDiscardError::Durability)?;
    }
    if !deleted {
        return Ok(());
    }

    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        file: directory_file,
    })
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
    ))
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativePartAbsent,
    ))
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)
}

fn discard_state_error(deleted: bool) -> ArtifactDiscardError {
    if deleted {
        ArtifactDiscardError::Durability
    } else {
        ArtifactDiscardError::Changed
    }
}

fn ensure_discard_state(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (Option<&RegularFileIdentity>, &Path),
    restart: (Option<&RegularFileIdentity>, &Path),
    forbidden: (&Path, &Path),
) -> Result<(), ()> {
    let (directory, directory_identity, model_dir) = directory;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ())?;
    ensure_discard_entry(part.0, part.1, || open_part_entry(directory, part.1))?;
    ensure_discard_entry(restart.0, restart.1, || {
        open_restart_entry(directory, restart.1)
    })?;
    ensure_discard_entry(None, forbidden.0, || {
        open_final_entry(directory, forbidden.0)
    })?;
    ensure_discard_entry(None, forbidden.1, || {
        open_invalid_entry(directory, forbidden.1)
    })?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ())
}

fn ensure_discard_entry(
    expected: Option<&RegularFileIdentity>,
    path: &Path,
    open: impl Fn() -> std::io::Result<File>,
) -> Result<(), ()> {
    match (expected, open()) {
        (None, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        (Some(identity), Ok(file)) => {
            let resolved = open().map_err(|_| ())?;
            ensure_regular_descriptors_match(&file, identity, &resolved, path).map_err(|_| ())
        }
        (None, Ok(_)) | (None, Err(_)) | (Some(_), Err(_)) => Err(()),
    }
}

fn reject_unsafe_open_transfer(file: &File, path: &Path) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe artifact path {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe artifact path {}", path.display()));
        }
    }
    Ok(())
}

fn validate_content_range(value: Option<&str>, offset: u64, total: u64) -> Result<(), String> {
    let value = value.ok_or_else(|| "206 response omitted Content-Range".to_string())?;
    let rest = value
        .strip_prefix("bytes ")
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (range, observed_total) = rest
        .split_once('/')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let start = start.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let end = end.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let observed_total = observed_total
        .parse::<u64>()
        .map_err(|_| "invalid Content-Range")?;
    if start == offset && observed_total == total && end.checked_add(1) == Some(total) {
        Ok(())
    } else {
        Err("Content-Range does not match requested artifact".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn restart_cleanup_targets_the_pinned_directory_after_a_path_swap() {
        let root = tempfile::tempdir().unwrap();
        let model_dir = root.path().join("model");
        let moved_model_dir = root.path().join("moved-model");
        let part_path = model_dir.join("model.gguf.part");
        let restart_path = model_dir.join("model.gguf.part.restart");
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&part_path, b"abcdef").unwrap();
        std::fs::write(&restart_path, b"XYZ").unwrap();
        let (directory, directory_identity) = open_directory(&model_dir).unwrap();
        let part = open_part_entry(&directory, &part_path).unwrap();
        let part_identity = regular_file_identity(&part, &part_path).unwrap();
        let restart = open_restart_entry(&directory, &restart_path).unwrap();
        let restart_identity = regular_file_identity(&restart, &restart_path).unwrap();

        std::fs::rename(&model_dir, &moved_model_dir).unwrap();
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&restart_path, b"replacement").unwrap();
        std::fs::write(model_dir.join("replacement-witness"), b"current").unwrap();

        let mut artifact_operation = super::super::perform_artifact_operation;
        let retained = remove_restart_after_authoritative_part_is_durable(
            (&directory, &directory_identity, &moved_model_dir),
            (&part, &part_identity, 6, &part_path),
            (&restart, &restart_identity, 3, &restart_path),
            &mut artifact_operation,
        );

        assert_eq!(
            std::fs::read(moved_model_dir.join("model.gguf.part")).unwrap(),
            b"abcdef"
        );
        assert_eq!(
            (
                moved_model_dir.join("model.gguf.part.restart").exists(),
                std::fs::read(&restart_path).ok(),
            ),
            (false, Some(b"replacement".to_vec()))
        );
        assert_eq!(
            std::fs::read(model_dir.join("replacement-witness")).unwrap(),
            b"current"
        );
        assert!(!model_dir.join("model.gguf.part").exists());
        assert!(matches!(retained, Ok(6)));
    }
}
