use super::{installed, AppService};
use crate::catalog::{self, Manifest, ModelLockError};
use crate::download::{
    self, DownloadFailure, DownloadTerminalOutcome, ProgressUpdate, VerifiedRegularFile,
};
use crate::huggingface::ResolvedFile;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapacityPlan {
    CleanInstalled,
    InstalledCleanup,
    InstalledRepairRequest,
    PendingPublish,
    PendingRequest,
    FreshRequest,
}

fn combine_plans(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
) -> Result<CapacityPlan, TransferErrorKind> {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    match (catalog, artifact) {
        (Catalog::ArtifactConflict, _) => Err(TransferErrorKind::ArtifactConflict),
        (Catalog::Unsafe, _) | (_, Artifact::Unsafe) => Err(TransferErrorKind::UnsafeLocalState),
        (Catalog::Installed, Artifact::ValidFinal) => Ok(CapacityPlan::CleanInstalled),
        (Catalog::InstalledCompletionDebris, Artifact::ValidFinal) => {
            Ok(CapacityPlan::InstalledCleanup)
        }
        (Catalog::Installed, Artifact::Fresh | Artifact::InstalledRepair) => {
            Ok(CapacityPlan::InstalledRepairRequest)
        }
        (Catalog::Installed, Artifact::CompletePart | Artifact::CompleteRestart) => {
            Ok(CapacityPlan::PendingPublish)
        }
        (
            Catalog::MatchingPending,
            Artifact::ValidFinal | Artifact::CompletePart | Artifact::CompleteRestart,
        ) => Ok(CapacityPlan::PendingPublish),
        (
            Catalog::MatchingPending,
            Artifact::Fresh | Artifact::RequestCapable | Artifact::InstalledRepair,
        ) => Ok(CapacityPlan::PendingRequest),
        (Catalog::Fresh, Artifact::Fresh) => Ok(CapacityPlan::FreshRequest),
        _ => Err(TransferErrorKind::UnsafeLocalState),
    }
}

fn recovery_is_discardable(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
    has_invalid_authority: bool,
    pending_created: bool,
) -> bool {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    if has_invalid_authority {
        return false;
    }
    match (catalog, artifact) {
        (Catalog::Fresh, Artifact::Fresh) => pending_created,
        (
            Catalog::MatchingPending,
            Artifact::Fresh
            | Artifact::RequestCapable
            | Artifact::CompletePart
            | Artifact::CompleteRestart,
        ) => true,
        _ => false,
    }
}

fn audited_retained_bytes(
    catalog: crate::catalog::transfer::CatalogTransferState,
    artifact: crate::download::plan::ArtifactTransferState,
    plan: &crate::download::plan::ArtifactTransferPlan,
    artifact_size: u64,
) -> Option<u64> {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    if plan.invalid_length().is_some()
        && !matches!(
            artifact,
            Artifact::CompletePart
                | Artifact::CompleteRestart
                | Artifact::InstalledRepair
                | Artifact::ValidFinalRepairDebris
        )
    {
        return None;
    }
    let staging = || {
        plan.part_length()
            .into_iter()
            .chain(plan.restart_length())
            .max()
            .unwrap_or(0)
    };
    match (catalog, artifact) {
        (
            Catalog::MatchingPending,
            Artifact::ValidFinal | Artifact::CompletePart | Artifact::CompleteRestart,
        )
        | (Catalog::Installed, Artifact::CompletePart | Artifact::CompleteRestart) => {
            Some(artifact_size)
        }
        (Catalog::MatchingPending, Artifact::Fresh) => Some(0),
        (
            Catalog::MatchingPending | Catalog::Installed,
            Artifact::RequestCapable | Artifact::InstalledRepair,
        ) => Some(staging()),
        _ => None,
    }
}

fn round_capacity(value: u64, fragment: u64) -> Result<u64, TransferErrorKind> {
    if fragment == 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    if value == 0 {
        return Ok(0);
    }
    value
        .checked_add(
            fragment
                .checked_sub(1)
                .ok_or(TransferErrorKind::CapacityOverflow)?,
        )
        .and_then(|rounded| rounded.checked_div(fragment))
        .and_then(|fragments| fragments.checked_mul(fragment))
        .ok_or(TransferErrorKind::CapacityOverflow)
}

fn required_capacity(
    plan: CapacityPlan,
    artifact_size: u64,
    manifest_len: u64,
    fragment: u64,
) -> Result<u64, TransferErrorKind> {
    if matches!(
        plan,
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
    ) {
        return Ok(0);
    }
    let exact = round_capacity(artifact_size, fragment)?;
    let write_peak = round_capacity(
        artifact_size
            .checked_add(1)
            .ok_or(TransferErrorKind::CapacityOverflow)?,
        fragment,
    )?;
    let manifest = round_capacity(manifest_len, fragment)?;
    match plan {
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup => Ok(0),
        CapacityPlan::InstalledRepairRequest => Ok(write_peak),
        CapacityPlan::PendingPublish => Ok(manifest),
        CapacityPlan::PendingRequest => Ok(write_peak.max(
            exact
                .checked_add(manifest)
                .ok_or(TransferErrorKind::CapacityOverflow)?,
        )),
        CapacityPlan::FreshRequest => Ok(write_peak
            .checked_add(manifest)
            .ok_or(TransferErrorKind::CapacityOverflow)?
            .max(
                exact
                    .checked_add(
                        manifest
                            .checked_mul(2)
                            .ok_or(TransferErrorKind::CapacityOverflow)?,
                    )
                    .ok_or(TransferErrorKind::CapacityOverflow)?,
            )),
    }
}

fn capacity_is_sufficient(required: u64, available: u64) -> bool {
    available >= required
}

fn checked_available_bytes(
    available_fragments: u64,
    fragment: u64,
) -> Result<u64, TransferErrorKind> {
    if fragment == 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    available_fragments
        .checked_mul(fragment)
        .ok_or(TransferErrorKind::CapacityOverflow)
}

fn checked_u64(value: u128) -> Result<u64, TransferErrorKind> {
    u64::try_from(value).map_err(|_| TransferErrorKind::CapacityOverflow)
}

fn admit_capacity_with(
    lock: &crate::catalog::ModelLock,
    plan: CapacityPlan,
    artifact_size: u64,
    manifest_len: u64,
    probe: impl FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
) -> Result<u64, TransferError> {
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    if matches!(
        plan,
        CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
    ) {
        return Ok(0);
    }
    let (available, fragment) = probe(lock.model_directory()).map_err(TransferError::terminal)?;
    let required = required_capacity(plan, artifact_size, manifest_len, fragment)
        .map_err(TransferError::terminal)?;
    if capacity_is_sufficient(required, available) {
        Ok(required)
    } else {
        Err(TransferError::insufficient_disk(required, available))
    }
}

#[cfg(unix)]
fn available_capacity(directory: &std::fs::File) -> Result<(u64, u64), TransferErrorKind> {
    use std::os::fd::AsRawFd;

    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `directory` supplies a live descriptor and `status` points to writable storage.
    if unsafe { libc::fstatvfs(directory.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(TransferErrorKind::CapacityUnavailable);
    }
    // SAFETY: successful `fstatvfs` initialized the entire output structure.
    let status = unsafe { status.assume_init() };
    let fragment = checked_u64(status.f_frsize as u128)?;
    let available_fragments = checked_u64(status.f_bavail as u128)?;
    Ok((
        checked_available_bytes(available_fragments, fragment)?,
        fragment,
    ))
}

#[cfg(not(unix))]
fn available_capacity(_directory: &std::fs::File) -> Result<(u64, u64), TransferErrorKind> {
    Err(TransferErrorKind::CapacityUnavailable)
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

fn deterministic_model_id(artifact: &ResolvedFile) -> String {
    let stem = artifact
        .path()
        .strip_suffix(".gguf")
        .or_else(|| artifact.path().strip_suffix(".GGUF"))
        .unwrap_or(artifact.path());
    let identity = format!("{}-{stem}", artifact.repo());
    let mut prefix = identity
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() {
                byte.to_ascii_lowercase() as char
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let suffix = &artifact.sha256()[..artifact.sha256().len().min(16)];
    prefix.truncate(120usize.saturating_sub(suffix.len() + 1));
    format!("{}-{suffix}", prefix.trim_end_matches('-'))
}

fn exact_manifest(model_id: String, artifact: &ResolvedFile) -> Manifest {
    Manifest {
        version: 1,
        id: model_id,
        repo: Some(artifact.repo().to_owned()),
        revision: Some(artifact.commit().to_owned()),
        remote_filename: Some(artifact.path().to_owned()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: artifact.sha256().to_owned(),
        size: artifact.size(),
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn interrupted(model_id: String, artifact: ResolvedFile) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition: TransferDisposition::Interrupted,
        retained_bytes: None,
        discardable: false,
    }
}

fn paused(
    model_id: String,
    artifact: ResolvedFile,
    retained_bytes: u64,
    discardable: bool,
) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition: TransferDisposition::Paused,
        retained_bytes: Some(retained_bytes),
        discardable,
    }
}

fn completed(
    model_id: String,
    artifact: ResolvedFile,
    disposition: TransferDisposition,
) -> TransferResult {
    TransferResult {
        model_id,
        artifact,
        disposition,
        retained_bytes: None,
        discardable: false,
    }
}

fn catalog_mutation_error(error: catalog::transfer::CatalogMutationError) -> TransferError {
    TransferError::terminal(match error {
        catalog::transfer::CatalogMutationError::Changed => TransferErrorKind::UnsafeLocalState,
        catalog::transfer::CatalogMutationError::Durability => TransferErrorKind::Durability,
    })
}

fn download_failure(
    failure: DownloadFailure,
    model_id: String,
    artifact: ResolvedFile,
    discardable: bool,
) -> TransferError {
    match failure {
        DownloadFailure::Legacy(_) => TransferError::terminal(TransferErrorKind::Remote),
        DownloadFailure::Remote { retained_bytes } => TransferError::recovery(
            TransferErrorKind::Remote,
            model_id,
            artifact,
            retained_bytes,
            discardable,
        ),
        DownloadFailure::Integrity {
            retained_bytes,
            authority,
        } => TransferError::recovery(
            TransferErrorKind::Integrity,
            model_id,
            artifact,
            retained_bytes,
            authority == download::IntegrityAuthority::PendingOnly && discardable,
        ),
        DownloadFailure::Durability => TransferError::terminal(TransferErrorKind::Durability),
        DownloadFailure::DiskExhausted { retained_bytes } => TransferError::recovery(
            TransferErrorKind::DiskExhausted,
            model_id,
            artifact,
            retained_bytes,
            discardable,
        ),
    }
}

fn finish_discard_after_artifact_bytes(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    catalog: catalog::transfer::CatalogDiscardFacts,
) -> Result<(), TransferError> {
    download::plan_artifact_discard(lock.model_directory(), model_dir)
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
    catalog::transfer::remove_pending_last(lock, catalog)
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))
}

fn admission_error_with_audited_recovery(
    error: TransferError,
    catalog_state: crate::catalog::transfer::CatalogTransferState,
    artifact_state: crate::download::plan::ArtifactTransferState,
    artifact_plan: &crate::download::plan::ArtifactTransferPlan,
    model_id: &str,
    artifact: &ResolvedFile,
) -> TransferError {
    if error.kind != TransferErrorKind::InsufficientDisk {
        return error;
    }
    let Some(retained_bytes) = audited_retained_bytes(
        catalog_state,
        artifact_state,
        artifact_plan,
        artifact.size(),
    ) else {
        return error;
    };
    error.with_recovery(
        model_id.to_owned(),
        artifact.clone(),
        retained_bytes,
        recovery_is_discardable(
            catalog_state,
            artifact_state,
            artifact_plan.has_invalid_authority(),
            false,
        ),
    )
}

fn publication_error_after_final_audit(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    model_id: &str,
    artifact: &ResolvedFile,
    verified: &VerifiedRegularFile,
) -> TransferError {
    if lock.revalidate().is_err()
        || verified
            .proves(
                &model_dir.join("model.gguf"),
                artifact.size(),
                artifact.sha256(),
            )
            .is_err()
    {
        return TransferError::terminal(TransferErrorKind::Publication);
    }
    TransferError::recovery(
        TransferErrorKind::Publication,
        model_id.to_owned(),
        artifact.clone(),
        artifact.size(),
        false,
    )
}

fn recover_installed_completion_after_artifact_revalidation(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    artifact: &ResolvedFile,
    artifact_plan: &mut crate::download::plan::ArtifactTransferPlan,
    catalog_plan: &crate::catalog::transfer::CatalogTransferPlan,
) -> Result<(), TransferError> {
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    artifact_plan
        .take_revalidated_clean_final(
            lock.model_directory(),
            model_dir,
            artifact.size(),
            artifact.sha256(),
        )
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    catalog::transfer::recover_installed_completion(lock, manifest, catalog_plan)
        .map_err(catalog_mutation_error)
}

#[cfg(test)]
fn transfer_selected_with<F, C, T, D>(
    service: &AppService,
    request: TransferSelected,
    control: TransferControl,
    progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
{
    transfer_selected_with_lookup_observer(
        service,
        (request, |_| {}),
        control,
        progress,
        capacity,
        token,
        download,
    )
}

#[cfg(test)]
fn transfer_selected_with_proof<F, C, T, D>(
    service: &AppService,
    request: TransferSelected,
    control: TransferControl,
    progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        Option<VerifiedRegularFile>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
{
    transfer_selected_with_observers(
        service,
        (request, |_| {}, || {}),
        control,
        progress,
        capacity,
        token,
        download,
    )
}

#[cfg(test)]
fn transfer_selected_with_lookup_observer<F, C, T, D, L>(
    service: &AppService,
    request_and_observer: (TransferSelected, L),
    control: TransferControl,
    progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
    L: FnOnce(&str),
{
    transfer_selected_with_observers(
        service,
        (request_and_observer.0, request_and_observer.1, || {}),
        control,
        progress,
        capacity,
        token,
        |artifact, model_dir, token, _, control, progress| {
            download(artifact, model_dir, token, control, progress)
        },
    )
}

#[cfg(test)]
fn transfer_selected_with_publication_observer<F, C, T, D, P>(
    service: &AppService,
    request_and_observer: (TransferSelected, P),
    control: TransferControl,
    progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
    P: FnOnce(),
{
    transfer_selected_with_observers(
        service,
        (request_and_observer.0, |_| {}, request_and_observer.1),
        control,
        progress,
        capacity,
        token,
        |artifact, model_dir, token, _, control, progress| {
            download(artifact, model_dir, token, control, progress)
        },
    )
}

fn transfer_selected_with_observers<F, C, T, D, L, P>(
    service: &AppService,
    request_and_observers: (TransferSelected, L, P),
    control: TransferControl,
    mut progress: F,
    capacity: C,
    token: T,
    download: D,
) -> Result<TransferResult, TransferError>
where
    F: FnMut(TransferProgress),
    C: FnOnce(&std::fs::File) -> Result<(u64, u64), TransferErrorKind>,
    T: FnOnce() -> Option<String>,
    D: FnOnce(
        &ResolvedFile,
        download::DownloadDirectoryAuthority<'_>,
        Option<String>,
        Option<VerifiedRegularFile>,
        &TransferControl,
        &mut F,
    ) -> Result<DownloadTerminalOutcome, DownloadFailure>,
    L: FnOnce(&str),
    P: FnOnce(),
{
    let (request, after_alternate_lookup, before_manifest_publication) = request_and_observers;
    let TransferSelected { artifact, intent } = request;
    let (requested_model_id, requires_existing_catalog) = match intent {
        TransferIntent::Requested(model_id) => (model_id, false),
        TransferIntent::InspectedInstalled(model_id) => (Some(model_id), true),
    };
    let (model_id, chosen_by_alternate_reuse) = match requested_model_id {
        Some(model_id) => (model_id, false),
        None => match installed::exact_remote_model_id(&service.reader.paths.models, &artifact)
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?
        {
            Some(model_id) => (model_id, true),
            None => (deterministic_model_id(&artifact), false),
        },
    };
    if chosen_by_alternate_reuse {
        after_alternate_lookup(&model_id);
    }
    crate::paths::validate_id(&model_id)
        .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
    let manifest = exact_manifest(model_id.clone(), &artifact);
    manifest
        .validate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
    if manifest_bytes.len() > crate::catalog::transfer::MAX_CATALOG_MANIFEST_BYTES {
        return Err(TransferError::terminal(
            TransferErrorKind::CatalogManifestTooLarge,
        ));
    }
    let manifest_len =
        checked_u64(manifest_bytes.len() as u128).map_err(TransferError::terminal)?;
    let model_dir = service
        .reader
        .paths
        .model_dir(&model_id)
        .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
    let lock_result = if chosen_by_alternate_reuse || requires_existing_catalog {
        catalog::ModelLock::acquire_existing(&model_dir)
    } else {
        catalog::ModelLock::acquire_for_transfer(&model_dir)
    };
    let lock = lock_result.map_err(|error| {
        TransferError::terminal(match error {
            ModelLockError::Busy => TransferErrorKind::Busy,
            ModelLockError::Missing | ModelLockError::UnsafeLocalState => {
                TransferErrorKind::UnsafeLocalState
            }
        })
    })?;
    let catalog_plan = catalog::transfer::plan_transfer(&lock, &manifest);
    let catalog_state = catalog_plan.state();
    if (chosen_by_alternate_reuse || requires_existing_catalog)
        && !matches!(
            catalog_state,
            crate::catalog::transfer::CatalogTransferState::Installed
                | crate::catalog::transfer::CatalogTransferState::InstalledCompletionDebris
        )
    {
        return Err(TransferError::terminal(TransferErrorKind::UnsafeLocalState));
    }
    let mut artifact_plan = match crate::download::plan::plan_artifact_transfer(
        lock.model_directory(),
        &model_dir,
        artifact.size(),
        artifact.sha256(),
        &|| control.pause_requested(),
    ) {
        crate::download::plan::ArtifactPlanOutcome::Interrupted => {
            return Ok(interrupted(model_id, artifact));
        }
        crate::download::plan::ArtifactPlanOutcome::Ready(plan) => plan,
    };
    let artifact_state = artifact_plan.state();
    let plan = combine_plans(catalog_state, artifact_state).map_err(TransferError::terminal)?;
    if chosen_by_alternate_reuse
        && !matches!(
            plan,
            CapacityPlan::CleanInstalled | CapacityPlan::InstalledCleanup
        )
    {
        return Err(TransferError::terminal(TransferErrorKind::UnsafeLocalState));
    }
    if let Err(error) = admit_capacity_with(&lock, plan, artifact.size(), manifest_len, capacity) {
        return Err(admission_error_with_audited_recovery(
            error,
            catalog_state,
            artifact_state,
            &artifact_plan,
            &model_id,
            &artifact,
        ));
    }
    if control.pause_requested() {
        return Ok(interrupted(model_id, artifact));
    }

    if plan == CapacityPlan::InstalledCleanup {
        recover_installed_completion_after_artifact_revalidation(
            &lock,
            &model_dir,
            &manifest,
            &artifact,
            &mut artifact_plan,
            &catalog_plan,
        )?;
        return Ok(completed(
            model_id,
            artifact,
            TransferDisposition::AlreadyInstalled,
        ));
    }
    catalog::transfer::recover_admitted_catalog_temps(&lock, &catalog_plan)
        .map_err(catalog_mutation_error)?;

    if plan == CapacityPlan::CleanInstalled {
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        return Ok(completed(
            model_id,
            artifact,
            TransferDisposition::AlreadyInstalled,
        ));
    }

    let mut discardable = recovery_is_discardable(
        catalog_state,
        artifact_state,
        artifact_plan.has_invalid_authority(),
        false,
    );

    let verified = if plan == CapacityPlan::PendingPublish
        && artifact_state == crate::download::plan::ArtifactTransferState::ValidFinal
    {
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        if control.pause_requested() {
            return Ok(interrupted(model_id, artifact));
        }
        artifact_plan
            .take_verified_final()
            .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?
    } else {
        let token = if matches!(
            plan,
            CapacityPlan::FreshRequest
                | CapacityPlan::PendingRequest
                | CapacityPlan::InstalledRepairRequest
        ) {
            Some(token())
        } else {
            None
        };
        if plan == CapacityPlan::FreshRequest {
            lock.revalidate()
                .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
            catalog::prepare_pull(&model_dir, &manifest)
                .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
            discardable = recovery_is_discardable(
                catalog_state,
                artifact_state,
                artifact_plan.has_invalid_authority(),
                true,
            );
        }
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        let verified_part = match artifact_state {
            crate::download::plan::ArtifactTransferState::CompletePart => Some(
                artifact_plan
                    .take_verified_part()
                    .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?,
            ),
            crate::download::plan::ArtifactTransferState::CompleteRestart => {
                let verified_restart = artifact_plan
                    .take_verified_restart()
                    .ok_or_else(|| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
                match crate::download::normalize_complete_restart(
                    lock.model_directory(),
                    &model_dir,
                    &artifact,
                    verified_restart,
                ) {
                    Ok(verified) => Some(verified),
                    Err(failure) => {
                        return Err(download_failure(failure, model_id, artifact, discardable));
                    }
                }
            }
            _ => None,
        };
        let outcome = download(
            &artifact,
            download::DownloadDirectoryAuthority::retained(&model_dir, lock.model_directory()),
            token.flatten(),
            verified_part,
            &control,
            &mut progress,
        );
        match outcome {
            Ok(DownloadTerminalOutcome::Paused { retained_bytes }) => {
                return Ok(paused(model_id, artifact, retained_bytes, discardable));
            }
            Err(failure) => {
                return Err(download_failure(failure, model_id, artifact, discardable));
            }
            Ok(DownloadTerminalOutcome::Complete(completion)) => completion.into_parts().1,
        }
    };

    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Publication))?;
    progress(TransferProgress {
        phase: TransferPhase::Publishing,
        transferred_bytes: artifact.size(),
        total_bytes: artifact.size(),
    });
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Publication))?;
    before_manifest_publication();
    if catalog::publish_manifest_verified(&service.reader.paths.models, &manifest, &lock, &verified)
        .is_err()
    {
        return Err(publication_error_after_final_audit(
            &lock, &model_dir, &model_id, &artifact, &verified,
        ));
    }
    Ok(completed(
        model_id,
        artifact,
        if catalog_state == crate::catalog::transfer::CatalogTransferState::Installed {
            TransferDisposition::AlreadyInstalled
        } else {
            TransferDisposition::Installed
        },
    ))
}

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

    pub fn transfer_selected<F>(
        &self,
        request: TransferSelected,
        control: TransferControl,
        progress: F,
    ) -> Result<TransferResult, TransferError>
    where
        F: FnMut(TransferProgress),
    {
        transfer_selected_with_observers(
            self,
            (request, |_| {}, || {}),
            control,
            progress,
            available_capacity,
            crate::huggingface::discover_token,
            |artifact, model_dir, token, verified_part, control, progress| {
                download::download_controlled(
                    artifact,
                    model_dir,
                    token,
                    verified_part,
                    || control.pause_requested(),
                    |update| progress(TransferProgress::from_download(update)),
                )
            },
        )
    }

    pub fn prepare_discard(&self, model_id: String) -> Result<DiscardCandidate, TransferError> {
        crate::paths::validate_id(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let model_dir = self
            .reader
            .paths
            .model_dir(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let lock = catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| {
            TransferError::terminal(match error {
                ModelLockError::Missing => TransferErrorKind::NoIncompleteTransfer,
                ModelLockError::Busy => TransferErrorKind::Busy,
                ModelLockError::UnsafeLocalState => TransferErrorKind::UnsafeLocalState,
            })
        })?;
        let catalog = catalog::transfer::plan_discard(&lock, &model_id).map_err(|error| {
            TransferError::terminal(match error {
                catalog::transfer::CatalogDiscardError::NoIncompleteTransfer => {
                    TransferErrorKind::NoIncompleteTransfer
                }
                catalog::transfer::CatalogDiscardError::InstalledAuthority => {
                    TransferErrorKind::CompletionWon
                }
                catalog::transfer::CatalogDiscardError::ArtifactConflict => {
                    TransferErrorKind::ArtifactConflict
                }
                catalog::transfer::CatalogDiscardError::UnsafeLocalState => {
                    TransferErrorKind::UnsafeLocalState
                }
            })
        })?;
        let artifact = download::plan_artifact_discard(lock.model_directory(), &model_dir)
            .map_err(|error| {
                TransferError::terminal(match error {
                    download::ArtifactDiscardError::Changed => TransferErrorKind::UnsafeLocalState,
                    download::ArtifactDiscardError::Durability => TransferErrorKind::Durability,
                })
            })?;
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        Ok(DiscardCandidate {
            model_id,
            catalog,
            artifact,
        })
    }

    pub fn discard_transfer(&self, candidate: DiscardCandidate) -> Result<(), TransferError> {
        let DiscardCandidate {
            model_id,
            catalog: captured_catalog,
            artifact: captured_artifact,
        } = candidate;
        let model_dir = self
            .reader
            .paths
            .model_dir(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let lock = catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| {
            TransferError::terminal(match error {
                ModelLockError::Busy => TransferErrorKind::Busy,
                ModelLockError::Missing | ModelLockError::UnsafeLocalState => {
                    TransferErrorKind::IncompleteTransferChanged
                }
            })
        })?;
        let current_catalog = catalog::transfer::plan_discard(&lock, &model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        if current_catalog != captured_catalog {
            return Err(TransferError::terminal(
                TransferErrorKind::IncompleteTransferChanged,
            ));
        }
        let current_artifact = download::plan_artifact_discard(lock.model_directory(), &model_dir)
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        if current_artifact != captured_artifact {
            return Err(TransferError::terminal(
                TransferErrorKind::IncompleteTransferChanged,
            ));
        }
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        download::discard_artifact_bytes(lock.model_directory(), &model_dir, captured_artifact)
            .map_err(|error| {
                TransferError::terminal(match error {
                    download::ArtifactDiscardError::Changed => {
                        TransferErrorKind::IncompleteTransferChanged
                    }
                    download::ArtifactDiscardError::Durability => TransferErrorKind::Durability,
                })
            })?;
        finish_discard_after_artifact_bytes(&lock, &model_dir, captured_catalog)
    }
}

#[cfg(test)]
mod tests;
