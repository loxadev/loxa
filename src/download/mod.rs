mod artifact;
mod http;
pub(crate) mod plan;

use crate::huggingface::ResolvedFile;
use crate::safe_file::{DirectoryIdentity, RegularFileIdentity};
#[cfg(test)]
use artifact::hex;
use artifact::{download_once, prove_existing_part_for_pause, ArtifactTransferError};
use backon::{BackoffBuilder, ExponentialBuilder};
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(test)]
pub(crate) use artifact::{content_hash_count, reset_content_hash_count};
pub(crate) use artifact::{
    hash_local_gguf_captured, verify_regular, verify_regular_captured,
    verify_regular_captured_cancellable, CapturedVerification, VerifiedRegularFile,
};
#[cfg(test)]
pub(crate) use http::artifact_url;
#[cfg(test)]
use http::TransferError;
#[cfg(test)]
use http::{follow_redirects, redirect_target, transient_status, Transfer};
use http::{wait_with_pause, ReqwestTransport, Transport, WaitOutcome};

const MAX_RETRIES: usize = 3;

struct RetryWait<Observer, Sleeper> {
    observer: Observer,
    sleep: Sleeper,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProgressUpdate {
    Transferring { transferred: u64, total: u64 },
    Verifying { transferred: u64, total: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactCheckpoint {
    StagingSynced,
    StagingIdentityMatched,
    DirectorySynced,
    NormalizationDirectorySynced,
    AuthoritativeRestatted,
    BeforeAuthoritativePartUnlink,
    AuthoritativePartIdentityMatched,
    AuthoritativePartUnlinked,
    ChecksumCleanupDirectorySynced,
    BeforeAuthoritativePartAbsenceProof,
    AuthoritativePartAbsent,
    BeforeRestartUnlink,
    RestartIdentityMatched,
    RestartUnlinked,
    BeforeRestartAbsenceProof,
    RestartAbsent,
    RecoveredPartSynced,
    BeforeRecoveredPartRestat,
    BeforeIntegrityAuthorityObservation,
    BeforePromotion,
    CompletionFencePassed,
    PromotionDirectorySynced,
    BeforePrefixExchange,
    PrefixExchanged,
}

enum ArtifactOperation<'a> {
    Sync {
        checkpoint: ArtifactCheckpoint,
        file: &'a File,
    },
    Write {
        file: &'a mut File,
        bytes: &'a [u8],
    },
    Observe(ArtifactCheckpoint),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactOperationFailure {
    DiskExhausted,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactDiscardError {
    Changed,
    Durability,
}

#[derive(Eq, PartialEq)]
pub(crate) struct ArtifactDiscardFacts {
    directory_identity: DirectoryIdentity,
    part: Option<RegularFileIdentity>,
    restart: Option<RegularFileIdentity>,
}

impl ArtifactDiscardFacts {
    fn directory_identity(&self) -> &DirectoryIdentity {
        &self.directory_identity
    }

    fn into_entries(
        self,
    ) -> (
        DirectoryIdentity,
        Option<RegularFileIdentity>,
        Option<RegularFileIdentity>,
    ) {
        (self.directory_identity, self.part, self.restart)
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.part
            .as_ref()
            .into_iter()
            .chain(self.restart.as_ref())
            .map(RegularFileIdentity::size)
            .max()
            .unwrap_or(0)
    }
}

pub(crate) fn plan_artifact_discard(
    directory: &File,
    model_dir: &Path,
) -> Result<ArtifactDiscardFacts, ArtifactDiscardError> {
    plan::plan_artifact_discard_inner(directory, model_dir)
}

pub(crate) fn discard_artifact_bytes(
    directory: &File,
    model_dir: &Path,
    facts: ArtifactDiscardFacts,
) -> Result<(), ArtifactDiscardError> {
    artifact::discard_artifact_bytes_inner(directory, model_dir, facts)
}

fn perform_artifact_operation(
    operation: ArtifactOperation<'_>,
) -> Result<(), ArtifactOperationFailure> {
    match operation {
        ArtifactOperation::Sync { checkpoint, file } => {
            debug_assert!(matches!(
                checkpoint,
                ArtifactCheckpoint::StagingSynced
                    | ArtifactCheckpoint::DirectorySynced
                    | ArtifactCheckpoint::NormalizationDirectorySynced
                    | ArtifactCheckpoint::ChecksumCleanupDirectorySynced
                    | ArtifactCheckpoint::RecoveredPartSynced
                    | ArtifactCheckpoint::PromotionDirectorySynced
            ));
            file.sync_all().map_err(|_| ArtifactOperationFailure::Other)
        }
        ArtifactOperation::Write { file, bytes } => {
            file.write_all(bytes)
                .map_err(|error| match error.raw_os_error() {
                    #[cfg(unix)]
                    Some(libc::ENOSPC | libc::EDQUOT) => ArtifactOperationFailure::DiskExhausted,
                    _ if error.kind() == std::io::ErrorKind::StorageFull => {
                        ArtifactOperationFailure::DiskExhausted
                    }
                    _ => ArtifactOperationFailure::Other,
                })
        }
        ArtifactOperation::Observe(checkpoint) => {
            debug_assert!(matches!(
                checkpoint,
                ArtifactCheckpoint::StagingIdentityMatched
                    | ArtifactCheckpoint::AuthoritativeRestatted
                    | ArtifactCheckpoint::BeforeAuthoritativePartUnlink
                    | ArtifactCheckpoint::AuthoritativePartIdentityMatched
                    | ArtifactCheckpoint::AuthoritativePartUnlinked
                    | ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof
                    | ArtifactCheckpoint::AuthoritativePartAbsent
                    | ArtifactCheckpoint::BeforeRestartUnlink
                    | ArtifactCheckpoint::RestartIdentityMatched
                    | ArtifactCheckpoint::RestartUnlinked
                    | ArtifactCheckpoint::BeforeRestartAbsenceProof
                    | ArtifactCheckpoint::RestartAbsent
                    | ArtifactCheckpoint::BeforeRecoveredPartRestat
                    | ArtifactCheckpoint::BeforeIntegrityAuthorityObservation
                    | ArtifactCheckpoint::BeforePromotion
                    | ArtifactCheckpoint::CompletionFencePassed
                    | ArtifactCheckpoint::BeforePrefixExchange
                    | ArtifactCheckpoint::PrefixExchanged
            ));
            Ok(())
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum DownloadOutcome {
    Pulled(PathBuf),
    AlreadyInstalled(PathBuf),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DownloadTerminalOutcome {
    Complete(DownloadOutcome),
    Paused { retained_bytes: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntegrityAuthority {
    PendingOnly,
    Repair,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DownloadFailure {
    Legacy(String),
    Remote {
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

#[cfg(test)]
impl DownloadFailure {
    fn into_message(self) -> String {
        match self {
            Self::Legacy(message) => message,
            Self::Remote { .. } => "artifact response body failed".into(),
            Self::Integrity { .. } => "model artifact checksum mismatch".into(),
            Self::Durability => "artifact durability failed".into(),
            Self::DiskExhausted { .. } => "artifact disk exhausted".into(),
        }
    }
}

impl AsRef<Path> for DownloadOutcome {
    fn as_ref(&self) -> &Path {
        match self {
            Self::Pulled(path) | Self::AlreadyInstalled(path) => path,
        }
    }
}

pub(crate) fn download_controlled(
    file: &ResolvedFile,
    model_dir: &Path,
    token: Option<String>,
    should_pause: impl Fn() -> bool,
    progress: impl FnMut(ProgressUpdate),
) -> Result<DownloadTerminalOutcome, DownloadFailure> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| DownloadFailure::Legacy(error.to_string()))?;
    runtime.block_on(async {
        if should_pause() {
            return Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 });
        }
        let transport = ReqwestTransport::new(token).map_err(DownloadFailure::Legacy)?;
        download_with_transport_controlled_async(
            file,
            model_dir,
            &transport,
            &should_pause,
            progress,
            RetryWait {
                observer: |_| {},
                sleep: tokio::time::sleep,
            },
            perform_artifact_operation,
        )
        .await
    })
}

#[cfg(test)]
fn download_with_transport(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
) -> Result<DownloadOutcome, String> {
    download_with_transport_progress(spec, model_dir, transport, |_| {})
}

#[cfg(test)]
fn download_with_transport_progress(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    progress: impl FnMut(ProgressUpdate),
) -> Result<DownloadOutcome, String> {
    test_runtime().block_on(download_with_transport_progress_async(
        spec, model_dir, transport, progress,
    ))
}

#[cfg(test)]
fn download_with_transport_controlled<Observer, S, Sleep>(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    should_pause: impl Fn() -> bool,
    progress: impl FnMut(ProgressUpdate),
    retry_wait: RetryWait<Observer, S>,
    artifact_operation: impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<DownloadTerminalOutcome, DownloadFailure>
where
    Observer: FnMut(Duration),
    S: FnMut(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    test_runtime().block_on(download_with_transport_controlled_async(
        spec,
        model_dir,
        transport,
        &should_pause,
        progress,
        retry_wait,
        artifact_operation,
    ))
}

async fn download_with_transport_controlled_async<Observer, S, Sleep>(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    should_pause: &impl Fn() -> bool,
    mut progress: impl FnMut(ProgressUpdate),
    retry_wait: RetryWait<Observer, S>,
    mut artifact_operation: impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<DownloadTerminalOutcome, DownloadFailure>
where
    Observer: FnMut(Duration),
    S: FnMut(Duration) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let RetryWait {
        observer: mut retry_observer,
        mut sleep,
    } = retry_wait;
    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(250))
        .with_jitter()
        .with_max_times(MAX_RETRIES)
        .build();
    let mut prefix_must_be_reproved = false;
    let total = spec.size();
    let mut last_transferred: Option<u64> = None;
    loop {
        if should_pause() && !prefix_must_be_reproved {
            return Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 });
        }
        let attempt = {
            let mut normalized_progress = |update| match update {
                ProgressUpdate::Transferring { transferred, .. } => {
                    let transferred = transferred.min(total);
                    let transferred =
                        last_transferred.map_or(transferred, |last| last.max(transferred));
                    if last_transferred != Some(transferred) {
                        last_transferred = Some(transferred);
                        progress(ProgressUpdate::Transferring { transferred, total });
                    }
                }
                ProgressUpdate::Verifying { .. } => {
                    last_transferred = Some(total);
                    progress(ProgressUpdate::Verifying {
                        transferred: total,
                        total,
                    });
                }
            };
            download_once(
                spec,
                model_dir,
                transport,
                &mut normalized_progress,
                should_pause,
                prefix_must_be_reproved,
                &mut artifact_operation,
            )
            .await
        };
        match attempt {
            Ok(outcome) => return Ok(DownloadTerminalOutcome::Complete(outcome)),
            Err(ArtifactTransferError::Durability) => return Err(DownloadFailure::Durability),
            Err(ArtifactTransferError::DiskExhausted { retained_bytes }) => {
                return Err(DownloadFailure::DiskExhausted { retained_bytes });
            }
            Err(ArtifactTransferError::Integrity {
                retained_bytes,
                authority,
            }) => {
                return Err(DownloadFailure::Integrity {
                    retained_bytes,
                    authority,
                });
            }
            Err(ArtifactTransferError::Remote {
                error,
                retained_bytes,
            }) => {
                prefix_must_be_reproved = true;
                if !error.is_retryable() {
                    return Err(DownloadFailure::Remote { retained_bytes });
                }
                let Some(delay) = backoff.next() else {
                    return Err(DownloadFailure::Remote { retained_bytes });
                };
                retry_observer(delay);
                if matches!(
                    wait_with_pause(sleep(delay), should_pause).await,
                    WaitOutcome::Paused
                ) {
                    return prove_existing_part_for_pause(spec, model_dir, &mut artifact_operation)
                        .map(|retained_bytes| DownloadTerminalOutcome::Paused { retained_bytes })
                        .map_err(|_| DownloadFailure::Durability);
                }
            }
            Err(ArtifactTransferError::RemoteBeforeBody(error)) => {
                if prefix_must_be_reproved {
                    return Err(DownloadFailure::Durability);
                }
                if error.is_paused() {
                    return Ok(DownloadTerminalOutcome::Paused {
                        retained_bytes: error.retained_bytes().unwrap_or(0),
                    });
                }
                if !error.is_retryable() {
                    return Err(DownloadFailure::Remote { retained_bytes: 0 });
                }
                let Some(delay) = backoff.next() else {
                    return Err(DownloadFailure::Remote { retained_bytes: 0 });
                };
                retry_observer(delay);
                if matches!(
                    wait_with_pause(sleep(delay), should_pause).await,
                    WaitOutcome::Paused
                ) {
                    return Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 });
                }
            }
            Err(ArtifactTransferError::Transfer(error)) if error.is_paused() => {
                return Ok(DownloadTerminalOutcome::Paused {
                    retained_bytes: error.retained_bytes().unwrap_or(0),
                });
            }
            Err(ArtifactTransferError::Transfer(error)) if !error.is_retryable() => {
                return Err(DownloadFailure::Legacy(error.into_message()));
            }
            Err(ArtifactTransferError::Transfer(error)) => {
                let Some(delay) = backoff.next() else {
                    return Err(DownloadFailure::Legacy(error.into_message()));
                };
                retry_observer(delay);
                if matches!(
                    wait_with_pause(sleep(delay), should_pause).await,
                    WaitOutcome::Paused
                ) {
                    if prefix_must_be_reproved {
                        return prove_existing_part_for_pause(
                            spec,
                            model_dir,
                            &mut artifact_operation,
                        )
                        .map(|retained_bytes| DownloadTerminalOutcome::Paused { retained_bytes })
                        .map_err(|_| DownloadFailure::Durability);
                    }
                    return Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 });
                }
            }
        }
    }
}

#[cfg(test)]
async fn download_with_transport_progress_async(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    mut progress: impl FnMut(ProgressUpdate),
) -> Result<DownloadOutcome, String> {
    let should_pause = || false;
    match download_with_transport_controlled_async(
        spec,
        model_dir,
        transport,
        &should_pause,
        &mut progress,
        RetryWait {
            observer: |_| {},
            sleep: tokio::time::sleep,
        },
        perform_artifact_operation,
    )
    .await
    .map_err(DownloadFailure::into_message)?
    {
        DownloadTerminalOutcome::Complete(outcome) => Ok(outcome),
        DownloadTerminalOutcome::Paused { .. } => Err("artifact transfer paused".into()),
    }
}

#[cfg(test)]
fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[cfg(test)]
include!("tests.rs");
