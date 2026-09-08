use super::entry::{
    capture_artifact_entry, create_part_entry, create_restart_entry, open_final_entry,
    open_part_entry, open_part_entry_for_write, open_restart_entry,
    reject_unsafe_artifact_entry_if_present, reject_unsafe_open_transfer,
};
use super::prefix::{
    captured_prefixes_match, normalize_durable_prefix, normalize_restart_prefix,
    remove_invalid_authoritative_part, remove_invalid_restart_and_recover_part,
};
use super::publication::{promote_authoritative_part, promote_authoritative_restart};
use super::repair::{
    capture_invalid_authority, ensure_complete_part_authority, ensure_repair_debris_absent,
    finish_repair, integrity_authority_at_fence, invalid_authority_is_present,
    quarantine_captured_final, remove_invalid_before_promotion,
};
use super::staging::{classify_pre_body_terminal, durable_part_barrier};
use super::ArtifactTransferError;
use crate::download::http::{
    artifact_url, wait_with_pause, Transfer, TransferError, Transport, WaitOutcome,
};
use crate::download::{
    ArtifactCheckpoint, ArtifactOperation, ArtifactOperationFailure, DownloadCompletion,
    DownloadOutcome, IntegrityAuthority, ProgressUpdate,
};
use crate::huggingface::ResolvedFile;
use crate::safe_file::{
    directory_identity, ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    open_directory, regular_file_identity,
};
use crate::verification::file::{
    verify_regular_entry_controlled, VerificationOutcome, VerifiedRegularFile,
};
use reqwest::StatusCode;
use std::fs::{self, File};

enum CopyFailure {
    Transfer(TransferError),
    DiskExhausted,
    Durability,
}

impl From<TransferError> for CopyFailure {
    fn from(error: TransferError) -> Self {
        Self::Transfer(error)
    }
}

pub(in crate::download) struct DownloadRequest<'a> {
    pub(in crate::download) spec: &'a ResolvedFile,
    pub(in crate::download) directory: super::DownloadDirectoryAuthority<'a>,
    pub(in crate::download) verified_part: Option<VerifiedRegularFile>,
}

pub(in crate::download) async fn download_once(
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
