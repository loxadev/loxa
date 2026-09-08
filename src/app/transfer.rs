use super::AppService;
use crate::catalog;
#[cfg(test)]
use crate::catalog::Manifest;
use crate::download::{self, ProgressUpdate};
#[cfg(test)]
use crate::download::{DownloadFailure, DownloadTerminalOutcome};
use crate::huggingface::ResolvedFile;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod admission;
mod discard;
mod execution;
mod recovery;
#[cfg(test)]
mod test_support;

#[cfg(test)]
use admission::{
    admit_capacity_with, available_capacity, capacity_is_sufficient, checked_available_bytes,
    checked_u64, combine_plans, required_capacity, round_capacity, CapacityPlan,
};
#[cfg(test)]
use discard::finish_discard_after_artifact_bytes;
#[cfg(test)]
use execution::{deterministic_model_id, exact_manifest};
#[cfg(test)]
use recovery::{download_failure, recover_installed_completion_after_artifact_revalidation};
#[cfg(test)]
use test_support::{
    transfer_selected_with, transfer_selected_with_lookup_observer, transfer_selected_with_proof,
    transfer_selected_with_publication_observer,
};

pub struct ResolveArtifactRequest {
    repo: String,
    revision: Option<String>,
    selector: ArtifactSelector,
}

enum ArtifactSelector {
    ExactFile(String),
    UniqueQuant(String),
}

impl ResolveArtifactRequest {
    pub fn exact_file(repo: String, revision: Option<String>, filename: String) -> Self {
        Self {
            repo,
            revision,
            selector: ArtifactSelector::ExactFile(filename),
        }
    }

    pub fn unique_quant(repo: String, revision: Option<String>, quant: String) -> Self {
        Self {
            repo,
            revision,
            selector: ArtifactSelector::UniqueQuant(quant),
        }
    }
}

pub struct ResolveArtifactError {
    cause: crate::huggingface::ResolveError,
}

impl fmt::Debug for ResolveArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.cause {
            crate::huggingface::ResolveError::Discovery(_) => {
                "ResolveArtifactError { category: Discovery }"
            }
            crate::huggingface::ResolveError::Selection(_) => {
                "ResolveArtifactError { category: Selection }"
            }
        })
    }
}

impl fmt::Display for ResolveArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            crate::huggingface::ResolveError::Discovery(error) => error.fmt(formatter),
            crate::huggingface::ResolveError::Selection(selection) => {
                use crate::huggingface::SelectionError;

                formatter.write_str(match selection {
                    SelectionError::MissingSelection => {
                        "GGUF selection requires --file or --quant"
                    }
                    SelectionError::NoEligibleFiles => {
                        "repository has no verified single-file GGUF"
                    }
                    SelectionError::FileNotFound(_) => {
                        "selected verified file was not found; retry with --file <verified filename>"
                    }
                    SelectionError::QuantUnavailable { .. } => {
                        "requested quantization is unavailable; retry with --quant <an available value>"
                    }
                    SelectionError::AmbiguousQuant { .. } => {
                        "requested quantization matched multiple files; use --file <filename> to choose one"
                    }
                })
            }
        }
    }
}

impl std::error::Error for ResolveArtifactError {}

fn resolve_artifact_with(
    request: ResolveArtifactRequest,
    resolve: impl FnOnce(
        &str,
        Option<&str>,
        Option<&str>,
        Option<&str>,
    ) -> Result<ResolvedFile, crate::huggingface::ResolveError>,
) -> Result<ResolvedFile, ResolveArtifactError> {
    let (filename, quant) = match &request.selector {
        ArtifactSelector::ExactFile(filename) => (Some(filename.as_str()), None),
        ArtifactSelector::UniqueQuant(quant) => (None, Some(quant.as_str())),
    };
    resolve(&request.repo, request.revision.as_deref(), filename, quant)
        .map_err(|cause| ResolveArtifactError { cause })
}

pub struct TransferSelected {
    artifact: ResolvedFile,
    intent: TransferIntent,
}

enum TransferIntent {
    Requested(Option<String>),
    InspectedInstalled(String),
}

impl TransferSelected {
    pub fn new(artifact: ResolvedFile, requested_model_id: Option<String>) -> Self {
        Self {
            artifact,
            intent: TransferIntent::Requested(requested_model_id),
        }
    }

    pub fn for_installed(artifact: ResolvedFile, model_id: String) -> Self {
        Self {
            artifact,
            intent: TransferIntent::InspectedInstalled(model_id),
        }
    }
}

#[derive(Clone)]
pub struct TransferControl {
    pause: Arc<AtomicBool>,
}

impl TransferControl {
    pub fn new() -> Self {
        Self {
            pause: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn request_pause(&self) {
        self.pause.store(true, Ordering::SeqCst);
    }

    fn pause_requested(&self) -> bool {
        self.pause.load(Ordering::SeqCst)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn pause_requested_for_test(&self) -> bool {
        self.pause_requested()
    }
}

impl Default for TransferControl {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPhase {
    Transferring,
    Verifying,
    Publishing,
}

pub struct TransferProgress {
    phase: TransferPhase,
    transferred_bytes: u64,
    total_bytes: u64,
}

impl TransferProgress {
    fn from_download(update: ProgressUpdate) -> Self {
        match update {
            ProgressUpdate::Transferring { transferred, total } => Self {
                phase: TransferPhase::Transferring,
                transferred_bytes: transferred,
                total_bytes: total,
            },
            ProgressUpdate::Verifying { transferred, total } => Self {
                phase: TransferPhase::Verifying,
                transferred_bytes: transferred,
                total_bytes: total,
            },
        }
    }

    pub fn phase(&self) -> TransferPhase {
        self.phase
    }

    pub fn transferred_bytes(&self) -> u64 {
        self.transferred_bytes
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferDisposition {
    Installed,
    AlreadyInstalled,
    Paused,
    Interrupted,
}

pub struct TransferResult {
    model_id: String,
    artifact: ResolvedFile,
    disposition: TransferDisposition,
    retained_bytes: Option<u64>,
    discardable: bool,
}

impl TransferResult {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn artifact(&self) -> &ResolvedFile {
        &self.artifact
    }

    pub fn disposition(&self) -> TransferDisposition {
        self.disposition
    }

    pub fn retained_bytes(&self) -> Option<u64> {
        self.retained_bytes
    }

    pub fn discardable(&self) -> bool {
        self.discardable
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferErrorKind {
    InvalidModelId,
    Busy,
    UnsafeLocalState,
    ArtifactConflict,
    CapacityUnavailable,
    CapacityOverflow,
    CatalogManifestTooLarge,
    InsufficientDisk,
    Remote,
    Integrity,
    DiskExhausted,
    Durability,
    Publication,
    NoIncompleteTransfer,
    CompletionWon,
    IncompleteTransferChanged,
}

struct TransferRecovery {
    model_id: String,
    artifact: ResolvedFile,
    retained_bytes: u64,
    discardable: bool,
}

pub struct TransferError {
    kind: TransferErrorKind,
    required_available_bytes: Option<u64>,
    available_bytes: Option<u64>,
    recovery: Option<Box<TransferRecovery>>,
}

impl TransferError {
    fn terminal(kind: TransferErrorKind) -> Self {
        Self {
            kind,
            required_available_bytes: None,
            available_bytes: None,
            recovery: None,
        }
    }

    fn insufficient_disk(required: u64, available: u64) -> Self {
        Self {
            kind: TransferErrorKind::InsufficientDisk,
            required_available_bytes: Some(required),
            available_bytes: Some(available),
            recovery: None,
        }
    }

    fn recovery(
        kind: TransferErrorKind,
        model_id: String,
        artifact: ResolvedFile,
        retained_bytes: u64,
        discardable: bool,
    ) -> Self {
        Self {
            kind,
            required_available_bytes: None,
            available_bytes: None,
            recovery: Some(Box::new(TransferRecovery {
                model_id,
                artifact,
                retained_bytes,
                discardable,
            })),
        }
    }

    fn with_recovery(
        mut self,
        model_id: String,
        artifact: ResolvedFile,
        retained_bytes: u64,
        discardable: bool,
    ) -> Self {
        self.recovery = Some(Box::new(TransferRecovery {
            model_id,
            artifact,
            retained_bytes,
            discardable,
        }));
        self
    }

    pub(crate) fn kind(&self) -> TransferErrorKind {
        self.kind
    }

    pub(crate) fn required_available_bytes(&self) -> Option<u64> {
        self.required_available_bytes
    }

    pub(crate) fn available_bytes(&self) -> Option<u64> {
        self.available_bytes
    }

    pub(crate) fn recovery_model_id(&self) -> Option<&str> {
        self.recovery
            .as_deref()
            .map(|recovery| recovery.model_id.as_str())
    }

    pub(crate) fn recovery_artifact(&self) -> Option<&ResolvedFile> {
        self.recovery.as_deref().map(|recovery| &recovery.artifact)
    }

    pub(crate) fn retained_bytes(&self) -> Option<u64> {
        self.recovery
            .as_deref()
            .map(|recovery| recovery.retained_bytes)
    }

    pub(crate) fn discardable(&self) -> bool {
        self.recovery
            .as_deref()
            .is_some_and(|recovery| recovery.discardable)
    }
}

impl fmt::Debug for TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            TransferErrorKind::InvalidModelId => "invalid model id",
            TransferErrorKind::Busy => "model is busy",
            TransferErrorKind::UnsafeLocalState => "unsafe local model state",
            TransferErrorKind::ArtifactConflict => "model id refers to another artifact",
            TransferErrorKind::CapacityUnavailable => "disk capacity is unavailable",
            TransferErrorKind::CapacityOverflow => "disk capacity calculation overflowed",
            TransferErrorKind::CatalogManifestTooLarge => "catalog manifest is too large",
            TransferErrorKind::InsufficientDisk => "insufficient disk space",
            TransferErrorKind::Remote => "artifact transfer failed",
            TransferErrorKind::Integrity => "artifact integrity failed",
            TransferErrorKind::DiskExhausted => "artifact disk exhausted",
            TransferErrorKind::Durability => "artifact durability failed",
            TransferErrorKind::Publication => "artifact publication failed",
            TransferErrorKind::NoIncompleteTransfer => "no incomplete transfer exists",
            TransferErrorKind::CompletionWon => "artifact completion won",
            TransferErrorKind::IncompleteTransferChanged => "incomplete transfer changed",
        })
    }
}

impl std::error::Error for TransferError {}

pub struct DiscardCandidate {
    model_id: String,
    catalog: catalog::transfer::CatalogDiscardFacts,
    artifact: download::ArtifactDiscardFacts,
}

impl DiscardCandidate {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        self.artifact.retained_bytes()
    }

    pub(super) fn total_bytes(&self) -> u64 {
        self.catalog.total_bytes()
    }
}

impl AppService {
    pub fn resolve_artifact(
        &self,
        request: ResolveArtifactRequest,
    ) -> Result<ResolvedFile, ResolveArtifactError> {
        resolve_artifact_with(request, crate::huggingface::resolve_selected)
    }
}

#[cfg(test)]
mod tests;
