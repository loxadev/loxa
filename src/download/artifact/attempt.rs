use super::entry::{
    create_part_entry, create_restart_entry, open_part_entry_for_write, reject_unsafe_open_transfer,
};
use super::prefix::{
    normalize_durable_prefix, remove_invalid_authoritative_part,
    remove_invalid_restart_and_recover_part,
};
use super::repair::capture_invalid_authority;
use super::staging::classify_pre_body_terminal;
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
use crate::verification::file::VerifiedRegularFile;
use reqwest::StatusCode;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

mod complete;
mod finalize;
mod initial;

struct AttemptInputs<'a> {
    directory: &'a File,
    directory_identity: &'a crate::safe_file::DirectoryIdentity,
    spec: &'a ResolvedFile,
    model_dir: &'a Path,
    final_path: &'a PathBuf,
    part_path: &'a PathBuf,
    restart_path: &'a PathBuf,
    invalid_path: &'a PathBuf,
}

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
    let inputs = AttemptInputs {
        directory: &directory,
        directory_identity: &directory_identity,
        spec,
        model_dir,
        final_path: &final_path,
        part_path: &part_path,
        restart_path: &restart_path,
        invalid_path: &invalid_path,
    };
    if let Some(verified) = initial::inspect_existing_final(
        &inputs,
        &mut captured_invalid_authority,
        &mut integrity_authority,
        progress,
    )? {
        return Ok(DownloadCompletion::new(
            DownloadOutcome::AlreadyInstalled(final_path),
            verified,
        ));
    }
    let recovered_part = initial::recover_staging_prefix(
        &inputs,
        prefix_must_be_reproved,
        initial_invalid_authority,
        should_pause,
        artifact_operation,
    )?;
    let offset = recovered_part
        .as_ref()
        .map(|(_, _, length)| *length)
        .unwrap_or(0);
    progress(ProgressUpdate::Transferring {
        transferred: offset,
        total: spec.size(),
    });
    if offset == spec.size() && offset > 0 {
        let verified = complete::finish_complete_recovered_part(
            &inputs,
            recovered_part.as_ref(),
            verified_part,
            (captured_invalid_authority.as_ref(), integrity_authority),
            should_pause,
            progress,
            artifact_operation,
        )?;
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
    let verified = finalize::verify_and_promote_staging(
        &inputs,
        (&output, target, ignored_range),
        recovered_part.as_ref(),
        (captured_invalid_authority.as_ref(), integrity_authority),
        should_pause,
        progress,
        artifact_operation,
    )?;
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
