use crate::registry::VerifiedModel;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::{CONTENT_RANGE, RANGE};
use reqwest::{StatusCode, Url};
use sha2::{Digest, Sha256};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use sysinfo::{DiskRefreshKind, Disks};

const TODO_VERIFY: &str = "TODO_VERIFY";
const HF_BASE_URL: &str = "https://huggingface.co";
const LOXA_USER_AGENT: &str = concat!("loxa/", env!("CARGO_PKG_VERSION"));
const CONNECT_TIMEOUT_SECS: u64 = 30;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
// TODO(v2): Parallel/ranged chunk downloads may be faster, but only add them
// after the dependable single-stream path is proven. Doing this correctly needs
// chunk planning, per-chunk Content-Range validation, seeked writes or per-chunk
// temp files, merged progress, cooperative cancellation, retry policy, bounded
// concurrency, final SHA-256 verification, and crash-safe cleanup.
const HF_TOKEN: &str = "HF_TOKEN";
const HF_TOKEN_PATH: &str = "HF_TOKEN_PATH";
const HF_HOME: &str = "HF_HOME";
const HF_HUB_DISABLE_IMPLICIT_TOKEN: &str = "HF_HUB_DISABLE_IMPLICIT_TOKEN";
const XDG_CACHE_HOME: &str = "XDG_CACHE_HOME";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactFinalizationStage {
    Rename,
    FinalFileSync,
    ParentDirectorySync,
}

#[derive(Debug)]
pub enum DownloadError {
    Cancelled,
    AuthRequired,
    Forbidden,
    InvalidFilename,
    UnsafeArtifactPath,
    InvalidContentRange,
    ChecksumMismatch {
        expected: String,
        actual: String,
    },
    SizeMismatch {
        expected: u64,
        actual: u64,
    },
    InsufficientDiskSpace {
        needed: u64,
        available: u64,
    },
    Http(String),
    Io(std::io::Error),
    ArtifactFinalizationUncertain {
        stage: ArtifactFinalizationStage,
        source: std::io::Error,
    },
}

impl DownloadError {
    pub fn artifact_state_uncertain(&self) -> bool {
        matches!(self, Self::ArtifactFinalizationUncertain { .. })
    }
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadError::Cancelled => write!(f, "download cancelled"),
            DownloadError::AuthRequired => write!(
                f,
                "authentication required by Hugging Face; set HF_TOKEN if this is a private or gated repo"
            ),
            DownloadError::Forbidden => write!(
                f,
                "Hugging Face returned 403 forbidden; check HF_TOKEN and gated repos access"
            ),
            DownloadError::InvalidFilename => write!(f, "invalid flat model filename"),
            DownloadError::UnsafeArtifactPath => write!(f, "model artifact path is not a regular file"),
            DownloadError::InvalidContentRange => write!(f, "invalid Content-Range header"),
            DownloadError::ChecksumMismatch { expected, actual } => {
                write!(f, "checksum mismatch: expected {expected}, got {actual}")
            }
            DownloadError::SizeMismatch { expected, actual } => {
                write!(f, "size mismatch: expected {expected} bytes, got {actual} bytes")
            }
            DownloadError::InsufficientDiskSpace { needed, available } => write!(
                f,
                "insufficient disk space: need {needed} bytes, available {available} bytes"
            ),
            DownloadError::Http(message) => write!(f, "http error: {message}"),
            DownloadError::Io(error) => write!(f, "io error: {error}"),
            DownloadError::ArtifactFinalizationUncertain { stage, source } => write!(
                f,
                "artifact finalization state is uncertain at {stage:?}: {source}"
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
}

/// Product-neutral hook used by control surfaces to observe and cooperatively
/// cancel a download. Implementations must make `is_cancelled` cheap.
pub trait DownloadObserver {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn progress(&mut self, _progress: DownloadProgress) {}
}

pub struct NoopDownloadObserver;
impl DownloadObserver for NoopDownloadObserver {}

impl Error for DownloadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DownloadError::Io(error)
            | DownloadError::ArtifactFinalizationUncertain { source: error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(error: std::io::Error) -> Self {
        DownloadError::Io(error)
    }
}

impl From<reqwest::Error> for DownloadError {
    fn from(error: reqwest::Error) -> Self {
        DownloadError::Http(error.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ExistingDownload {
    Ready(PathBuf),
    Download { resume_from: u64 },
}

trait WarningSink {
    fn warn(&mut self, message: &str);
}

struct StderrWarnings;

impl WarningSink for StderrWarnings {
    fn warn(&mut self, message: &str) {
        eprintln!("{message}");
    }
}

struct BodyResponse {
    status: StatusCode,
    content_range: Option<String>,
    reader: Box<dyn Read>,
}

trait DownloadTransport {
    fn probe_size(&self, url: &Url) -> Result<u64, DownloadError>;
    fn body(&self, url: &Url, offset: u64) -> Result<BodyResponse, DownloadError>;
}

/// Closed filesystem boundary for artifact finalization. This remains private so callers cannot
/// weaken no-follow/link validation or substitute publication semantics.
trait ArtifactFileOps {
    fn sync_partial(&self, file: &File) -> std::io::Result<()>;
    fn rename(
        &self,
        from: &Path,
        to: &Path,
        evidence: &ArtifactPromotionEvidence,
    ) -> std::io::Result<()>;
    fn sync_final(&self, file: &File, evidence: &ArtifactPromotionEvidence) -> std::io::Result<()>;
    fn sync_parent(
        &self,
        parent: &Path,
        evidence: &ArtifactPromotionEvidence,
    ) -> std::io::Result<()>;
}

#[derive(Clone)]
struct ArtifactPromotionEvidence {
    parent: fs::Metadata,
    source: fs::Metadata,
    destination: Option<fs::Metadata>,
    destination_path: PathBuf,
}

impl ArtifactPromotionEvidence {
    fn capture(
        source_file: &File,
        source: &Path,
        destination: &Path,
    ) -> Result<Self, DownloadError> {
        let source_opened = source_file.metadata()?;
        let source_path =
            regular_artifact_metadata(source)?.ok_or(DownloadError::UnsafeArtifactPath)?;
        if !same_file_identity(&source_opened, &source_path) {
            return Err(DownloadError::UnsafeArtifactPath);
        }
        let parent_path = destination
            .parent()
            .ok_or(DownloadError::UnsafeArtifactPath)?;
        let parent = fs::symlink_metadata(parent_path)?;
        let parent_opened = File::open(parent_path)?.metadata()?;
        if !parent.file_type().is_dir() || !same_file_identity(&parent, &parent_opened) {
            return Err(DownloadError::UnsafeArtifactPath);
        }
        Ok(Self {
            parent,
            source: source_opened,
            destination: regular_artifact_metadata(destination)?,
            destination_path: destination.to_path_buf(),
        })
    }

    fn revalidate_parent(&self, parent: &Path) -> std::io::Result<()> {
        self.open_revalidated_parent(parent).map(drop)
    }

    fn open_revalidated_parent(&self, parent: &Path) -> std::io::Result<File> {
        let path_metadata = fs::symlink_metadata(parent)?;
        let opened = File::open(parent)?;
        let opened_metadata = opened.metadata()?;
        if !path_metadata.file_type().is_dir()
            || !same_file_identity(&self.parent, &path_metadata)
            || !same_file_identity(&self.parent, &opened_metadata)
        {
            return Err(invalid_finalization_evidence());
        }
        Ok(opened)
    }

    fn revalidate_before_rename(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        self.revalidate_parent(
            destination
                .parent()
                .ok_or_else(invalid_finalization_evidence)?,
        )?;
        let source_metadata = fs::symlink_metadata(source)?;
        if !source_metadata.file_type().is_file()
            || !artifact_has_single_link(&source_metadata)
            || !same_file_identity(&self.source, &source_metadata)
        {
            return Err(invalid_finalization_evidence());
        }
        match (self.destination.as_ref(), fs::symlink_metadata(destination)) {
            (None, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            (Some(expected), Ok(current))
                if current.file_type().is_file()
                    && artifact_has_single_link(&current)
                    && same_file_identity(expected, &current) =>
            {
                Ok(())
            }
            _ => Err(invalid_finalization_evidence()),
        }
    }

    fn revalidate_after_rename(&self, destination: &Path) -> std::io::Result<()> {
        self.revalidate_parent(
            destination
                .parent()
                .ok_or_else(invalid_finalization_evidence)?,
        )?;
        let current = fs::symlink_metadata(destination)?;
        if current.file_type().is_file()
            && artifact_has_single_link(&current)
            && same_file_identity(&self.source, &current)
        {
            Ok(())
        } else {
            Err(invalid_finalization_evidence())
        }
    }

    fn revalidate_opened_final(&self, file: &File) -> std::io::Result<()> {
        self.revalidate_parent(
            self.destination_path
                .parent()
                .ok_or_else(invalid_finalization_evidence)?,
        )?;
        let current = file.metadata()?;
        let path_metadata = fs::symlink_metadata(&self.destination_path)?;
        if current.file_type().is_file()
            && artifact_has_single_link(&current)
            && same_file_identity(&self.source, &current)
            && path_metadata.file_type().is_file()
            && artifact_has_single_link(&path_metadata)
            && same_file_identity(&self.source, &path_metadata)
        {
            Ok(())
        } else {
            Err(invalid_finalization_evidence())
        }
    }
}

fn invalid_finalization_evidence() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "artifact finalization evidence changed",
    )
}

struct StdArtifactFileOps;

impl ArtifactFileOps for StdArtifactFileOps {
    fn sync_partial(&self, file: &File) -> std::io::Result<()> {
        file.sync_all()
    }

    fn rename(
        &self,
        from: &Path,
        to: &Path,
        evidence: &ArtifactPromotionEvidence,
    ) -> std::io::Result<()> {
        evidence.revalidate_before_rename(from, to)?;
        fs::rename(from, to)?;
        evidence.revalidate_after_rename(to)
    }

    fn sync_final(&self, file: &File, evidence: &ArtifactPromotionEvidence) -> std::io::Result<()> {
        evidence.revalidate_opened_final(file)?;
        file.sync_all()
    }

    fn sync_parent(
        &self,
        parent: &Path,
        evidence: &ArtifactPromotionEvidence,
    ) -> std::io::Result<()> {
        evidence.open_revalidated_parent(parent)?.sync_all()
    }
}

struct ReqwestTransport {
    client: Client,
}

struct DownloadBodyContext<'a> {
    entry: &'a dyn VerifiedModel,
    part_path: &'a Path,
    final_path: &'a Path,
    progress: &'a ProgressBar,
    available_space_override: Option<u64>,
    observer: &'a mut dyn DownloadObserver,
    file_ops: &'a dyn ArtifactFileOps,
}

impl DownloadTransport for ReqwestTransport {
    fn probe_size(&self, url: &Url) -> Result<u64, DownloadError> {
        let response =
            send_with_hf_token(self.client.get(url.clone()).header(RANGE, "bytes=0-0"), url)
                .send()
                .map_err(DownloadError::from)?;
        map_status(response.status())?;

        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .ok_or(DownloadError::InvalidContentRange)?
            .to_str()
            .map_err(|_| DownloadError::InvalidContentRange)?;

        parse_total_from_content_range(content_range)
    }

    fn body(&self, url: &Url, offset: u64) -> Result<BodyResponse, DownloadError> {
        let mut request = self.client.get(url.clone());
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
        }

        let response = send_with_hf_token(request, url)
            .send()
            .map_err(DownloadError::from)?;
        let status = response.status();
        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .map(|value| value.to_str().map(str::to_string))
            .transpose()
            .map_err(|_| DownloadError::InvalidContentRange)?;

        Ok(BodyResponse {
            status,
            content_range,
            reader: Box::new(response),
        })
    }
}

pub fn model_dir() -> PathBuf {
    let home = home_dir();
    let path = home.join(".loxa").join("models");
    if let Err(error) = fs::create_dir_all(&path) {
        eprintln!(
            "warning: could not create model directory {}: {error}",
            path.display()
        );
    }
    path
}

fn home_dir() -> PathBuf {
    if let Some(home) = non_empty_env_path("HOME") {
        return home;
    }
    if let Some(home) = non_empty_env_path("USERPROFILE") {
        return home;
    }
    if let (Some(drive), Some(path)) = (non_empty_env_os("HOMEDRIVE"), non_empty_env_os("HOMEPATH"))
    {
        let mut combined = drive;
        combined.push(path);
        return PathBuf::from(combined);
    }
    PathBuf::from(".")
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    non_empty_env_os(name).map(PathBuf::from)
}

fn non_empty_env_os(name: &str) -> Option<std::ffi::OsString> {
    env::var_os(name).filter(|value| !value.is_empty())
}

pub fn download(entry: &dyn VerifiedModel, dest_dir: &Path) -> Result<PathBuf, DownloadError> {
    download_from_base_url(entry, dest_dir, HF_BASE_URL)
}

pub fn download_with_observer(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    observer: &mut dyn DownloadObserver,
) -> Result<PathBuf, DownloadError> {
    download_from_base_url_with_observer(entry, dest_dir, HF_BASE_URL, observer)
}

fn download_from_base_url(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
) -> Result<PathBuf, DownloadError> {
    let client = Client::builder()
        .user_agent(LOXA_USER_AGENT)
        .connect_timeout(std::time::Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .build()?;
    let transport = ReqwestTransport { client };
    download_with_transport(entry, dest_dir, base_url, &transport)
}

fn download_from_base_url_with_observer(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
    observer: &mut dyn DownloadObserver,
) -> Result<PathBuf, DownloadError> {
    let client = Client::builder()
        .user_agent(LOXA_USER_AGENT)
        .connect_timeout(std::time::Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .build()?;
    let transport = ReqwestTransport { client };
    download_with_transport_and_observer(entry, dest_dir, base_url, &transport, observer, None)
}

fn download_with_transport(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
) -> Result<PathBuf, DownloadError> {
    download_with_transport_and_available_space(entry, dest_dir, base_url, transport, None)
}

fn download_with_transport_and_available_space(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
    available_space_override: Option<u64>,
) -> Result<PathBuf, DownloadError> {
    let mut observer = NoopDownloadObserver;
    download_with_transport_and_observer(
        entry,
        dest_dir,
        base_url,
        transport,
        &mut observer,
        available_space_override,
    )
}

fn download_with_transport_and_observer(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
    observer: &mut dyn DownloadObserver,
    available_space_override: Option<u64>,
) -> Result<PathBuf, DownloadError> {
    download_with_transport_observer_and_file_ops(
        entry,
        dest_dir,
        base_url,
        transport,
        observer,
        available_space_override,
        &StdArtifactFileOps,
    )
}

fn download_with_transport_observer_and_file_ops(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
    observer: &mut dyn DownloadObserver,
    available_space_override: Option<u64>,
    file_ops: &dyn ArtifactFileOps,
) -> Result<PathBuf, DownloadError> {
    if observer.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    let filename = sanitize_filename(entry.filename())?;
    fs::create_dir_all(dest_dir)?;
    cleanup_stale_restart(&part_path(dest_dir, &filename))?;

    let mut warnings = StderrWarnings;
    if let ExistingDownload::Ready(path) = inspect_existing_download_with_observer_and_file_ops(
        entry,
        dest_dir,
        &mut warnings,
        false,
        observer,
        file_ops,
    )? {
        return Ok(path);
    }

    let url = build_download_url(base_url, entry.repo(), entry.revision(), &filename)?;
    let remote_size = transport.probe_size(&url)?;
    if observer.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    if remote_size != entry.size_bytes() {
        return Err(DownloadError::SizeMismatch {
            expected: entry.size_bytes(),
            actual: remote_size,
        });
    }

    let final_path = dest_dir.join(&filename);
    let part_path = part_path(dest_dir, &filename);
    let resume_from = match inspect_existing_download_with_observer_and_file_ops(
        entry,
        dest_dir,
        &mut warnings,
        true,
        observer,
        file_ops,
    )? {
        ExistingDownload::Ready(path) => return Ok(path),
        ExistingDownload::Download { resume_from } => resume_from,
    };
    let bytes_needed = entry.size_bytes().saturating_sub(resume_from);
    ensure_disk_space_for_download(dest_dir, bytes_needed, available_space_override)?;

    let progress = progress_bar(entry.size_bytes(), &filename, resume_from);
    let result = download_body(
        transport,
        url,
        resume_from,
        DownloadBodyContext {
            entry,
            part_path: &part_path,
            final_path: &final_path,
            progress: &progress,
            available_space_override,
            observer,
            file_ops,
        },
    );

    match result {
        Ok(path) => {
            progress.finish_with_message(format!("downloaded {}", filename));
            Ok(path)
        }
        Err(error) => {
            progress.abandon();
            Err(error)
        }
    }
}

fn sanitize_filename(filename: &str) -> Result<String, DownloadError> {
    if filename.is_empty()
        || filename == "."
        || filename == ".."
        || filename.ends_with('.')
        || filename.contains('/')
        || filename.contains('\\')
        || filename.contains('\0')
        || filename.chars().any(char::is_whitespace)
        || filename
            .chars()
            .any(|ch| matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        || is_windows_reserved_filename(filename)
    {
        return Err(DownloadError::InvalidFilename);
    }

    Ok(filename.to_string())
}

fn is_windows_reserved_filename(filename: &str) -> bool {
    let stem = filename.split('.').next().unwrap_or(filename);
    let upper = stem.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || is_windows_numbered_device(&upper, "COM")
        || is_windows_numbered_device(&upper, "LPT")
}

fn is_windows_numbered_device(value: &str, prefix: &str) -> bool {
    let Some(number) = value.strip_prefix(prefix) else {
        return false;
    };
    matches!(number, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
}

fn ensure_enough_disk_space(available: u64, needed: u64) -> Result<(), DownloadError> {
    if available < needed {
        return Err(DownloadError::InsufficientDiskSpace { needed, available });
    }

    Ok(())
}

fn ensure_disk_space_for_download(
    dest_dir: &Path,
    bytes_needed: u64,
    available_space_override: Option<u64>,
) -> Result<(), DownloadError> {
    if bytes_needed == 0 {
        return Ok(());
    }

    let available = match available_space_override {
        Some(available) => available,
        None => available_space_for_path(dest_dir)?,
    };
    ensure_enough_disk_space(available, bytes_needed)
}

fn available_space_for_path(path: &Path) -> Result<u64, DownloadError> {
    let canonical_path = path.canonicalize()?;
    let disks = Disks::new_with_refreshed_list_specifics(DiskRefreshKind::nothing().with_storage());

    let Some(disk) = disks
        .list()
        .iter()
        .filter(|disk| canonical_path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
    else {
        return Ok(u64::MAX);
    };

    Ok(disk.available_space())
}

fn parse_total_from_content_range(header: &str) -> Result<u64, DownloadError> {
    let (start, end, total) = parse_content_range(header)?;
    if start != 0 || end != 0 {
        return Err(DownloadError::InvalidContentRange);
    }
    Ok(total)
}

fn parse_content_range(header: &str) -> Result<(u64, u64, u64), DownloadError> {
    let (range, total) = header
        .trim()
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .ok_or(DownloadError::InvalidContentRange)?;
    if total.is_empty() || total == "*" || !total.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DownloadError::InvalidContentRange);
    }

    let (start, end) = range
        .split_once('-')
        .ok_or(DownloadError::InvalidContentRange)?;
    if start.is_empty()
        || end.is_empty()
        || !start.bytes().all(|byte| byte.is_ascii_digit())
        || !end.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DownloadError::InvalidContentRange);
    }

    let start = start
        .parse::<u64>()
        .map_err(|_| DownloadError::InvalidContentRange)?;
    let end = end
        .parse::<u64>()
        .map_err(|_| DownloadError::InvalidContentRange)?;
    let total = total
        .parse::<u64>()
        .map_err(|_| DownloadError::InvalidContentRange)?;
    if start > end || end >= total {
        return Err(DownloadError::InvalidContentRange);
    }

    Ok((start, end, total))
}

fn validate_resume_content_range(
    header: Option<&str>,
    resume_from: u64,
    expected_total: u64,
) -> Result<(), DownloadError> {
    let header = header.ok_or(DownloadError::InvalidContentRange)?;
    let (start, _end, total) = parse_content_range(header)?;
    if start != resume_from || total != expected_total {
        return Err(DownloadError::InvalidContentRange);
    }
    Ok(())
}

#[cfg(test)]
fn hash_file(path: &Path) -> Result<String, DownloadError> {
    let mut hasher = Sha256::new();
    hash_existing_prefix_into(&mut hasher, path)?;
    Ok(hex_bytes(hasher.finalize().as_ref()))
}

#[cfg(test)]
fn hash_existing_prefix_into(hasher: &mut Sha256, path: &Path) -> Result<u64, DownloadError> {
    let mut observer = NoopDownloadObserver;
    hash_existing_prefix_into_observed(hasher, path, &mut observer)
}

fn hash_existing_prefix_into_observed(
    hasher: &mut Sha256,
    path: &Path,
    observer: &mut dyn DownloadObserver,
) -> Result<u64, DownloadError> {
    let mut file = open_regular_read_no_follow(path)?;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut total = 0_u64;

    loop {
        if observer.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }

    Ok(total)
}

#[cfg(test)]
fn inspect_existing_download(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
) -> Result<ExistingDownload, DownloadError> {
    let mut warnings = StderrWarnings;
    inspect_existing_download_with_warnings(entry, dest_dir, &mut warnings, true)
}

#[cfg(test)]
fn inspect_existing_download_with_warnings(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    warnings: &mut impl WarningSink,
    announce_resume: bool,
) -> Result<ExistingDownload, DownloadError> {
    let mut observer = NoopDownloadObserver;
    inspect_existing_download_with_observer(
        entry,
        dest_dir,
        warnings,
        announce_resume,
        &mut observer,
    )
}

#[cfg(test)]
fn inspect_existing_download_with_observer(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    warnings: &mut impl WarningSink,
    announce_resume: bool,
    observer: &mut dyn DownloadObserver,
) -> Result<ExistingDownload, DownloadError> {
    inspect_existing_download_with_observer_and_file_ops(
        entry,
        dest_dir,
        warnings,
        announce_resume,
        observer,
        &StdArtifactFileOps,
    )
}

fn inspect_existing_download_with_observer_and_file_ops(
    entry: &dyn VerifiedModel,
    dest_dir: &Path,
    warnings: &mut impl WarningSink,
    announce_resume: bool,
    observer: &mut dyn DownloadObserver,
    file_ops: &dyn ArtifactFileOps,
) -> Result<ExistingDownload, DownloadError> {
    let filename = sanitize_filename(entry.filename())?;
    fs::create_dir_all(dest_dir)?;

    let final_path = dest_dir.join(&filename);
    let part_path = part_path(dest_dir, &filename);

    if let Some(final_metadata) = regular_artifact_metadata(&final_path)? {
        let final_size = final_metadata.len();
        if final_size == entry.size_bytes() {
            if entry.sha256() == TODO_VERIFY {
                warnings.warn(&hash_unverified_warning(&final_path));
                return Ok(ExistingDownload::Ready(final_path));
            }

            let mut hasher = Sha256::new();
            hash_existing_prefix_into_observed(&mut hasher, &final_path, observer)?;
            let actual = hex_bytes(hasher.finalize().as_ref());
            if actual == entry.sha256() {
                eprintln!("already present, verified: {}", final_path.display());
                return Ok(ExistingDownload::Ready(final_path));
            }

            fs::remove_file(&final_path)?;
        } else {
            fs::remove_file(&final_path)?;
        }
    }

    if let Some(part_metadata) = regular_artifact_metadata(&part_path)? {
        let part_size = part_metadata.len();
        if part_size > entry.size_bytes() {
            fs::remove_file(&part_path)?;
            return Ok(ExistingDownload::Download { resume_from: 0 });
        }

        if part_size == entry.size_bytes() {
            verify_part_and_rename_observed(
                entry,
                &part_path,
                &final_path,
                warnings,
                observer,
                file_ops,
            )?;
            return Ok(ExistingDownload::Ready(final_path));
        }

        if part_size > 0 {
            if announce_resume {
                eprintln!("resuming {} from byte {}", filename, part_size);
            }
            return Ok(ExistingDownload::Download {
                resume_from: part_size,
            });
        }
    }

    Ok(ExistingDownload::Download { resume_from: 0 })
}

fn build_download_url(
    base_url: &str,
    repo: &str,
    revision: &str,
    filename: &str,
) -> Result<Url, DownloadError> {
    let mut url = Url::parse(base_url).map_err(|error| DownloadError::Http(error.to_string()))?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| DownloadError::Http("could not build Hugging Face URL".to_string()))?;
        for segment in repo.split('/') {
            segments.push(segment);
        }
        segments.push("resolve");
        segments.push(revision);
        segments.push(filename);
    }
    url.query_pairs_mut().append_pair("download", "true");
    Ok(url)
}

fn download_body(
    transport: &impl DownloadTransport,
    url: Url,
    resume_from: u64,
    context: DownloadBodyContext<'_>,
) -> Result<PathBuf, DownloadError> {
    let mut hasher = Sha256::new();
    let mut offset = resume_from;
    if offset > 0 {
        let hashed =
            hash_existing_prefix_into_observed(&mut hasher, context.part_path, context.observer)?;
        if hashed != offset {
            return Err(DownloadError::SizeMismatch {
                expected: offset,
                actual: hashed,
            });
        }
    }

    let mut response = transport.body(&url, offset)?;
    if context.observer.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    let mut restart_from_zero = false;
    match (offset, response.status) {
        (_, StatusCode::UNAUTHORIZED) => return Err(DownloadError::AuthRequired),
        (_, StatusCode::FORBIDDEN) => return Err(DownloadError::Forbidden),
        (0, StatusCode::OK | StatusCode::PARTIAL_CONTENT) => {}
        (1.., StatusCode::PARTIAL_CONTENT) => {}
        (1.., StatusCode::OK) => {
            restart_from_zero = true;
        }
        (_, status) => {
            return Err(DownloadError::Http(format!(
                "unexpected response status {status}"
            )));
        }
    }

    if restart_from_zero {
        // The verified partial remains rollback evidence until replacement is
        // promoted, so none of its bytes are reclaimable during preflight.
        ensure_disk_space_for_download(
            context.final_path.parent().unwrap_or(Path::new(".")),
            context.entry.size_bytes(),
            context.available_space_override,
        )?;
        hasher = Sha256::new();
        offset = 0;
        context.progress.set_length(context.entry.size_bytes());
        context.progress.set_position(0);
        context.progress.reset_eta();
        context.observer.progress(DownloadProgress {
            downloaded_bytes: 0,
            total_bytes: context.entry.size_bytes(),
        });
    } else {
        context.observer.progress(DownloadProgress {
            downloaded_bytes: offset,
            total_bytes: context.entry.size_bytes(),
        });
    }

    let restart_path = restart_path(context.part_path);
    let transfer_path = if restart_from_zero {
        restart_path.as_path()
    } else {
        context.part_path
    };
    let mut file = if offset > 0 {
        validate_resume_content_range(
            response.content_range.as_deref(),
            offset,
            context.entry.size_bytes(),
        )?;
        open_regular_append_no_follow(context.part_path)?
    } else {
        if restart_from_zero && restart_path.exists() {
            fs::remove_file(&restart_path)?;
        }
        create_regular_truncate_no_follow(transfer_path)?
    };

    let total = match copy_response_to_part(
        response.reader.as_mut(),
        &mut file,
        &mut hasher,
        context.progress,
        offset,
        context.observer,
        context.entry.size_bytes(),
    ) {
        Ok(total) => total,
        Err(error) => {
            if restart_from_zero && restart_path.exists() {
                let _ = fs::remove_file(&restart_path);
            }
            return Err(error);
        }
    };
    if total != context.entry.size_bytes() {
        if restart_from_zero && restart_path.exists() {
            let _ = fs::remove_file(&restart_path);
        }
        return Err(DownloadError::SizeMismatch {
            expected: context.entry.size_bytes(),
            actual: total,
        });
    }

    context.file_ops.sync_partial(&file)?;
    if context.observer.is_cancelled() {
        if restart_from_zero && restart_path.exists() {
            let _ = fs::remove_file(&restart_path);
        }
        return Err(DownloadError::Cancelled);
    }
    let promotion_evidence =
        match ArtifactPromotionEvidence::capture(&file, transfer_path, context.final_path) {
            Ok(evidence) => evidence,
            Err(error) => {
                if restart_from_zero && restart_path.exists() {
                    let _ = fs::remove_file(&restart_path);
                }
                return Err(error);
            }
        };
    drop(file);

    if let Err(error) = verify_hash_policy(context.entry, transfer_path, hasher) {
        if restart_from_zero && restart_path.exists() {
            let _ = fs::remove_file(&restart_path);
        }
        return Err(error);
    }
    if let Err(error) =
        context
            .file_ops
            .rename(transfer_path, context.final_path, &promotion_evidence)
    {
        if restart_from_zero && restart_path.exists() {
            let _ = fs::remove_file(&restart_path);
        }
        return Err(finalization_uncertain(
            ArtifactFinalizationStage::Rename,
            error,
        ));
    }
    let final_file = open_regular_read_no_follow(context.final_path).map_err(|error| {
        finalization_uncertain_from_download(ArtifactFinalizationStage::Rename, error)
    })?;
    context
        .file_ops
        .sync_final(&final_file, &promotion_evidence)
        .map_err(|error| finalization_uncertain(ArtifactFinalizationStage::FinalFileSync, error))?;
    let parent = context.final_path.parent().ok_or_else(|| {
        finalization_uncertain(
            ArtifactFinalizationStage::ParentDirectorySync,
            invalid_finalization_evidence(),
        )
    })?;
    context
        .file_ops
        .sync_parent(parent, &promotion_evidence)
        .map_err(|error| {
            finalization_uncertain(ArtifactFinalizationStage::ParentDirectorySync, error)
        })?;
    if restart_from_zero && context.part_path.exists() {
        fs::remove_file(context.part_path).map_err(|error| {
            finalization_uncertain(ArtifactFinalizationStage::ParentDirectorySync, error)
        })?;
    }
    Ok(context.final_path.to_path_buf())
}

fn finalization_uncertain(
    stage: ArtifactFinalizationStage,
    source: std::io::Error,
) -> DownloadError {
    DownloadError::ArtifactFinalizationUncertain { stage, source }
}

fn finalization_uncertain_from_download(
    stage: ArtifactFinalizationStage,
    error: DownloadError,
) -> DownloadError {
    match error {
        DownloadError::Io(source) => finalization_uncertain(stage, source),
        DownloadError::ArtifactFinalizationUncertain { source, .. } => {
            finalization_uncertain(stage, source)
        }
        _ => finalization_uncertain(stage, invalid_finalization_evidence()),
    }
}

fn send_with_hf_token(request: RequestBuilder, url: &Url) -> RequestBuilder {
    if url.domain() == Some("huggingface.co") {
        if let Some(token) = resolve_hf_token() {
            return request.bearer_auth(token);
        }
    }
    request
}

fn resolve_hf_token() -> Option<String> {
    if env::var_os(HF_HUB_DISABLE_IMPLICIT_TOKEN).is_some_and(|value| !value.is_empty()) {
        return None;
    }

    if let Ok(token) = env::var(HF_TOKEN) {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Some(token);
        }
    }

    if let Ok(path) = env::var(HF_TOKEN_PATH) {
        if let Some(token) = read_token_file(Path::new(&path)) {
            return Some(token);
        }
    }

    read_token_file(&hf_home().join("token"))
}

fn hf_home() -> PathBuf {
    if let Some(path) = env::var_os(HF_HOME) {
        return PathBuf::from(path);
    }
    if let Some(path) = env::var_os(XDG_CACHE_HOME) {
        return PathBuf::from(path).join("huggingface");
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".cache")
        .join("huggingface")
}

fn read_token_file(path: &Path) -> Option<String> {
    let token = fs::read_to_string(path).ok()?;
    let token = token.trim().to_string();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

fn map_status(status: StatusCode) -> Result<(), DownloadError> {
    match status {
        StatusCode::UNAUTHORIZED => Err(DownloadError::AuthRequired),
        StatusCode::FORBIDDEN => Err(DownloadError::Forbidden),
        status if status.is_success() => Ok(()),
        status => Err(DownloadError::Http(format!(
            "unexpected response status {status}"
        ))),
    }
}

fn copy_response_to_part(
    response: &mut dyn Read,
    file: &mut File,
    hasher: &mut Sha256,
    progress: &ProgressBar,
    start: u64,
    observer: &mut dyn DownloadObserver,
    expected_total: u64,
) -> Result<u64, DownloadError> {
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut total = start;

    loop {
        if observer.is_cancelled() {
            file.flush()?;
            return Err(DownloadError::Cancelled);
        }
        let read = response.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        total += read as u64;
        progress.set_position(total.saturating_sub(start));
        observer.progress(DownloadProgress {
            downloaded_bytes: total,
            total_bytes: expected_total,
        });
    }

    file.flush()?;
    Ok(total)
}

fn verify_part_and_rename_observed(
    entry: &dyn VerifiedModel,
    part_path: &Path,
    final_path: &Path,
    warnings: &mut impl WarningSink,
    observer: &mut dyn DownloadObserver,
    file_ops: &dyn ArtifactFileOps,
) -> Result<(), DownloadError> {
    let actual_size = fs::metadata(part_path)?.len();
    if actual_size != entry.size_bytes() {
        return Err(DownloadError::SizeMismatch {
            expected: entry.size_bytes(),
            actual: actual_size,
        });
    }

    if entry.sha256() == TODO_VERIFY {
        warnings.warn(&hash_unverified_warning(part_path));
        let part = open_regular_read_no_follow(part_path)?;
        file_ops.sync_partial(&part)?;
        if observer.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        let promotion_evidence = ArtifactPromotionEvidence::capture(&part, part_path, final_path)?;
        drop(part);
        finalize_part(file_ops, part_path, final_path, &promotion_evidence)?;
        return Ok(());
    }

    let mut hasher = Sha256::new();
    hash_existing_prefix_into_observed(&mut hasher, part_path, observer)?;
    let actual = hex_bytes(hasher.finalize().as_ref());
    if actual != entry.sha256() {
        fs::remove_file(part_path)?;
        return Err(DownloadError::ChecksumMismatch {
            expected: entry.sha256().to_string(),
            actual,
        });
    }

    let part = open_regular_read_no_follow(part_path)?;
    file_ops.sync_partial(&part)?;
    if observer.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    let promotion_evidence = ArtifactPromotionEvidence::capture(&part, part_path, final_path)?;
    drop(part);
    finalize_part(file_ops, part_path, final_path, &promotion_evidence)
}

fn finalize_part(
    file_ops: &dyn ArtifactFileOps,
    part_path: &Path,
    final_path: &Path,
    evidence: &ArtifactPromotionEvidence,
) -> Result<(), DownloadError> {
    file_ops
        .rename(part_path, final_path, evidence)
        .map_err(|error| finalization_uncertain(ArtifactFinalizationStage::Rename, error))?;
    let final_file = open_regular_read_no_follow(final_path).map_err(|error| {
        finalization_uncertain_from_download(ArtifactFinalizationStage::Rename, error)
    })?;
    file_ops
        .sync_final(&final_file, evidence)
        .map_err(|error| finalization_uncertain(ArtifactFinalizationStage::FinalFileSync, error))?;
    let parent = final_path.parent().ok_or_else(|| {
        finalization_uncertain(
            ArtifactFinalizationStage::ParentDirectorySync,
            invalid_finalization_evidence(),
        )
    })?;
    file_ops.sync_parent(parent, evidence).map_err(|error| {
        finalization_uncertain(ArtifactFinalizationStage::ParentDirectorySync, error)
    })
}

fn verify_hash_policy(
    entry: &dyn VerifiedModel,
    part_path: &Path,
    hasher: Sha256,
) -> Result<(), DownloadError> {
    if entry.sha256() == TODO_VERIFY {
        eprintln!(
            "warning: hash unverified for downloaded file {}",
            part_path.display()
        );
        return Ok(());
    }

    let actual = hex_bytes(hasher.finalize().as_ref());
    if actual != entry.sha256() {
        fs::remove_file(part_path)?;
        return Err(DownloadError::ChecksumMismatch {
            expected: entry.sha256().to_string(),
            actual,
        });
    }

    Ok(())
}

fn part_path(dest_dir: &Path, filename: &str) -> PathBuf {
    dest_dir.join(format!("{filename}.part"))
}

fn restart_path(part_path: &Path) -> PathBuf {
    let mut name = part_path.as_os_str().to_os_string();
    name.push(".restart");
    PathBuf::from(name)
}

fn cleanup_stale_restart(part_path: &Path) -> Result<(), DownloadError> {
    let path = restart_path(part_path);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
            fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => Err(DownloadError::UnsafeArtifactPath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn regular_artifact_metadata(path: &Path) -> Result<Option<fs::Metadata>, DownloadError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && artifact_has_single_link(&metadata) => {
            Ok(Some(metadata))
        }
        Ok(_) => Err(DownloadError::UnsafeArtifactPath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn artifact_has_single_link(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}

#[cfg(windows)]
fn artifact_has_single_link(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.number_of_links() == Some(1)
}

#[cfg(not(any(unix, windows)))]
fn artifact_has_single_link(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const DOWNLOAD_NO_FOLLOW: i32 = 0x20_000;
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
const DOWNLOAD_NO_FOLLOW: i32 = 0x100;

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd"
))]
fn apply_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(DOWNLOAD_NO_FOLLOW);
}

#[cfg(windows)]
fn apply_no_follow(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;
    options.custom_flags(0x0020_0000);
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd"
)))]
fn apply_no_follow(_options: &mut OpenOptions) {}

fn open_regular_read_no_follow(path: &Path) -> Result<File, DownloadError> {
    open_regular_no_follow(path, |options| {
        options.read(true);
    })
}

fn open_regular_append_no_follow(path: &Path) -> Result<File, DownloadError> {
    open_regular_no_follow(path, |options| {
        options.append(true);
    })
}

fn create_regular_truncate_no_follow(path: &Path) -> Result<File, DownloadError> {
    if let Some(metadata) = regular_artifact_metadata(path)? {
        if !metadata.file_type().is_file() {
            return Err(DownloadError::UnsafeArtifactPath);
        }
    }
    open_regular_no_follow(path, |options| {
        options.write(true).create(true).truncate(true);
    })
}

fn open_regular_no_follow(
    path: &Path,
    configure: impl FnOnce(&mut OpenOptions),
) -> Result<File, DownloadError> {
    let before = fs::symlink_metadata(path).ok();
    if before
        .as_ref()
        .is_some_and(|metadata| !metadata.file_type().is_file())
    {
        return Err(DownloadError::UnsafeArtifactPath);
    }
    let mut options = OpenOptions::new();
    configure(&mut options);
    apply_no_follow(&mut options);
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.file_type().is_file() || !artifact_has_single_link(&opened) {
        return Err(DownloadError::UnsafeArtifactPath);
    }
    if let Some(before) = before {
        if !same_file_identity(&before, &opened) {
            return Err(DownloadError::UnsafeArtifactPath);
        }
    }
    Ok(file)
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(windows)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    match (
        left.volume_serial_number(),
        left.file_index(),
        right.volume_serial_number(),
        right.file_index(),
    ) {
        (Some(left_volume), Some(left_index), Some(right_volume), Some(right_index))
            if left_volume != 0 && left_index != 0 =>
        {
            left_volume == right_volume && left_index == right_index
        }
        _ => false,
    }
}

#[cfg(not(any(unix, windows)))]
fn same_file_identity(_: &fs::Metadata, _: &fs::Metadata) -> bool {
    false
}

fn hash_unverified_warning(path: &Path) -> String {
    format!("warning: hash unverified for {}", path.display())
}

fn progress_bar(total: u64, filename: &str, resume_from: u64) -> ProgressBar {
    let progress = ProgressBar::new(total.saturating_sub(resume_from));
    progress.set_message(filename.to_string());
    let style = ProgressStyle::with_template(
        "{msg} {wide_bar} {bytes}/{total_bytes} ({percent}%) {bytes_per_sec} ETA {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar());
    progress.set_style(style);
    progress
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ModelEntry;
    use reqwest::header::AUTHORIZATION;
    use sha2::{Digest, Sha256};
    use std::cell::Cell;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::io::{self, Cursor};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::tempdir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvRestore {
        values: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvRestore {
        fn capture(names: &[&'static str]) -> Self {
            Self {
                values: names
                    .iter()
                    .map(|name| (*name, env::var_os(name)))
                    .collect(),
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (name, value) in &self.values {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }

    fn entry(filename: &'static str, sha256: &'static str, size_bytes: u64) -> ModelEntry {
        ModelEntry {
            id: "test-model",
            repo: "owner/repo",
            revision: "main",
            filename,
            sha256,
            size_bytes,
            license: "apache-2.0",
            params: "1B",
            quant: "Q4",
            min_free_mem_gb: 1.0,
        }
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex_bytes(Sha256::digest(bytes).as_ref())
    }

    #[derive(Default)]
    struct CapturedWarnings(Vec<String>);

    impl WarningSink for CapturedWarnings {
        fn warn(&mut self, message: &str) {
            self.0.push(message.to_string());
        }
    }

    fn live_small_entry() -> ModelEntry {
        ModelEntry {
            id: "live-hf-small-config",
            repo: "julien-c/dummy-unknown",
            revision: "main",
            filename: "config.json",
            sha256: "b908f2b7227d4d31a2105dfa31095e28d304f9bc938bfaaa57ee2cacf1f62d32",
            size_bytes: 496,
            license: "unknown",
            params: "tiny",
            quant: "json",
            min_free_mem_gb: 0.1,
        }
    }

    fn live_small_config_bytes() -> &'static [u8] {
        br#"{
  "architectures": [
    "RobertaForMaskedLM"
  ],
  "attention_probs_dropout_prob": 0.1,
  "bos_token_id": 0,
  "eos_token_id": 1,
  "hidden_act": "gelu",
  "hidden_dropout_prob": 0.1,
  "hidden_size": 20,
  "initializer_range": 0.02,
  "intermediate_size": 40,
  "layer_norm_eps": 1e-12,
  "max_position_embeddings": 512,
  "model_type": "roberta",
  "num_attention_heads": 1,
  "num_hidden_layers": 1,
  "output_past": true,
  "pad_token_id": 2,
  "type_vocab_size": 2,
  "vocab_size": 10
}
"#
    }

    struct FakeBody {
        status: StatusCode,
        content_range: Option<String>,
        body: Vec<u8>,
    }

    struct FakeTransport {
        probe_total: u64,
        bodies: Mutex<Vec<FakeBody>>,
        body_offsets: Arc<Mutex<Vec<u64>>>,
    }

    impl FakeTransport {
        fn new(probe_total: u64, bodies: Vec<FakeBody>) -> Self {
            Self {
                probe_total,
                bodies: Mutex::new(bodies),
                body_offsets: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn offsets(&self) -> Vec<u64> {
            self.body_offsets.lock().unwrap().clone()
        }
    }

    impl DownloadTransport for FakeTransport {
        fn probe_size(&self, _url: &Url) -> Result<u64, DownloadError> {
            Ok(self.probe_total)
        }

        fn body(&self, _url: &Url, offset: u64) -> Result<BodyResponse, DownloadError> {
            self.body_offsets.lock().unwrap().push(offset);
            let body = self.bodies.lock().unwrap().remove(0);
            Ok(BodyResponse {
                status: body.status,
                content_range: body.content_range,
                reader: Box::new(Cursor::new(body.body)),
            })
        }
    }

    struct RenameBlockingReader {
        body: Cursor<Vec<u8>>,
        final_path: PathBuf,
        blocked: bool,
    }

    impl Read for RenameBlockingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let read = self.body.read(buffer)?;
            if read == 0 && !self.blocked {
                fs::create_dir(&self.final_path)?;
                self.blocked = true;
            }
            Ok(read)
        }
    }

    struct RenameFailureTransport {
        bytes: Vec<u8>,
        final_path: PathBuf,
    }

    impl DownloadTransport for RenameFailureTransport {
        fn probe_size(&self, _url: &Url) -> Result<u64, DownloadError> {
            Ok(self.bytes.len() as u64)
        }
        fn body(&self, _url: &Url, _offset: u64) -> Result<BodyResponse, DownloadError> {
            Ok(BodyResponse {
                status: StatusCode::OK,
                content_range: None,
                reader: Box::new(RenameBlockingReader {
                    body: Cursor::new(self.bytes.clone()),
                    final_path: self.final_path.clone(),
                    blocked: false,
                }),
            })
        }
    }

    #[derive(Default)]
    struct RecordingObserver {
        progress: Vec<DownloadProgress>,
        cancel_after: Option<u64>,
        cancelled: bool,
    }

    impl DownloadObserver for RecordingObserver {
        fn is_cancelled(&self) -> bool {
            self.cancelled
        }

        fn progress(&mut self, progress: DownloadProgress) {
            self.progress.push(progress);
            if self
                .cancel_after
                .is_some_and(|limit| progress.downloaded_bytes >= limit)
            {
                self.cancelled = true;
            }
        }
    }

    struct CheckCountObserver {
        checks: Cell<usize>,
        cancel_at: usize,
    }

    impl DownloadObserver for CheckCountObserver {
        fn is_cancelled(&self) -> bool {
            let next = self.checks.get() + 1;
            self.checks.set(next);
            next >= self.cancel_at
        }
    }

    #[test]
    fn cooperative_cancellation_interrupts_existing_prefix_hash_without_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("model.gguf.part");
        let bytes = vec![3_u8; COPY_BUFFER_BYTES * 3];
        fs::write(&path, &bytes).unwrap();
        let mut observer = CheckCountObserver {
            checks: Cell::new(0),
            cancel_at: 2,
        };
        let mut hasher = Sha256::new();

        let error =
            hash_existing_prefix_into_observed(&mut hasher, &path, &mut observer).unwrap_err();

        assert!(matches!(error, DownloadError::Cancelled));
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn observer_cancels_before_transfer_without_creating_a_partial() {
        let dir = tempdir().unwrap();
        let bytes = b"never requested".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(bytes.len() as u64, vec![]);
        let mut observer = RecordingObserver {
            cancelled: true,
            ..Default::default()
        };

        let error = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap_err();

        assert!(matches!(error, DownloadError::Cancelled));
        assert!(transport.offsets().is_empty());
        assert!(!dir.path().join("model.gguf.part").exists());
    }

    #[cfg(unix)]
    #[test]
    fn mutating_downloader_rejects_final_and_partial_symlinks_without_touching_targets() {
        use std::os::unix::fs::symlink;
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let target = outside.path().join("target");
        fs::write(&target, b"outside").unwrap();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(b"model").into_boxed_str()),
            5,
        );
        symlink(&target, dir.path().join("model.gguf")).unwrap();
        assert!(matches!(
            download_with_transport(
                &model,
                dir.path(),
                HF_BASE_URL,
                &FakeTransport::new(5, vec![])
            ),
            Err(DownloadError::UnsafeArtifactPath)
        ));
        assert_eq!(fs::read(&target).unwrap(), b"outside");

        fs::remove_file(dir.path().join("model.gguf")).unwrap();
        symlink(&target, dir.path().join("model.gguf.part")).unwrap();
        assert!(matches!(
            download_with_transport(
                &model,
                dir.path(),
                HF_BASE_URL,
                &FakeTransport::new(5, vec![])
            ),
            Err(DownloadError::UnsafeArtifactPath)
        ));
        assert_eq!(fs::read(&target).unwrap(), b"outside");

        fs::remove_file(dir.path().join("model.gguf.part")).unwrap();
        let part = dir.path().join("model.gguf.part");
        fs::write(&part, b"mod").unwrap();
        symlink(&target, restart_path(&part)).unwrap();
        cleanup_stale_restart(&part).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"outside");
        assert!(!restart_path(&part).exists());
    }

    #[test]
    fn mutating_downloader_rejects_artifact_directories() {
        let dir = tempdir().unwrap();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(b"model").into_boxed_str()),
            5,
        );
        fs::create_dir(dir.path().join("model.gguf.part")).unwrap();
        assert!(matches!(
            download_with_transport(
                &model,
                dir.path(),
                HF_BASE_URL,
                &FakeTransport::new(5, vec![])
            ),
            Err(DownloadError::UnsafeArtifactPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn mutating_downloader_rejects_final_and_partial_hardlink_ambiguity() {
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let target = outside.path().join("target");
        fs::write(&target, b"model").unwrap();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(b"model").into_boxed_str()),
            5,
        );

        fs::hard_link(&target, dir.path().join("model.gguf")).unwrap();
        assert!(matches!(
            download_with_transport(
                &model,
                dir.path(),
                HF_BASE_URL,
                &FakeTransport::new(5, vec![])
            ),
            Err(DownloadError::UnsafeArtifactPath)
        ));
        fs::remove_file(dir.path().join("model.gguf")).unwrap();

        fs::hard_link(&target, dir.path().join("model.gguf.part")).unwrap();
        assert!(matches!(
            download_with_transport(
                &model,
                dir.path(),
                HF_BASE_URL,
                &FakeTransport::new(5, vec![])
            ),
            Err(DownloadError::UnsafeArtifactPath)
        ));
        assert_eq!(fs::read(target).unwrap(), b"model");
    }

    #[test]
    fn honored_range_removes_stale_restart_before_resuming() {
        let dir = tempdir().unwrap();
        let bytes = b"prefix-and-suffix".to_vec();
        let split = 7;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let part = dir.path().join("model.gguf.part");
        fs::write(&part, &bytes[..split]).unwrap();
        fs::write(restart_path(&part), b"stale replacement").unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::PARTIAL_CONTENT,
                content_range: Some(format!("bytes {split}-{}/{}", bytes.len() - 1, bytes.len())),
                body: bytes[split..].to_vec(),
            }],
        );

        let path = download_with_transport(&model, dir.path(), HF_BASE_URL, &transport).unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert!(!restart_path(&part).exists());
    }

    #[test]
    fn observer_progress_is_monotonic_and_mid_transfer_cancel_keeps_resumable_part() {
        let dir = tempdir().unwrap();
        let bytes = vec![7_u8; COPY_BUFFER_BYTES * 2 + 11];
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes,
            }],
        );
        let mut observer = RecordingObserver {
            cancel_after: Some(COPY_BUFFER_BYTES as u64),
            ..Default::default()
        };

        let error = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap_err();

        assert!(matches!(error, DownloadError::Cancelled));
        assert_eq!(
            fs::metadata(dir.path().join("model.gguf.part"))
                .unwrap()
                .len(),
            COPY_BUFFER_BYTES as u64
        );
        assert!(observer
            .progress
            .windows(2)
            .all(|pair| pair[0].downloaded_bytes <= pair[1].downloaded_bytes));
        assert!(observer
            .progress
            .iter()
            .all(|item| item.total_bytes == model.size_bytes));
    }

    #[test]
    fn ignored_resume_range_cancelled_from_zero_progress_preserves_original_partial() {
        let dir = tempdir().unwrap();
        let bytes = vec![9_u8; COPY_BUFFER_BYTES + 3];
        let split = 31;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let original = bytes[..split].to_vec();
        fs::write(dir.path().join("model.gguf.part"), &original).unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes,
            }],
        );
        let mut observer = RecordingObserver {
            cancel_after: Some(0),
            ..Default::default()
        };

        let error = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap_err();

        assert!(matches!(error, DownloadError::Cancelled));
        assert_eq!(
            fs::read(dir.path().join("model.gguf.part")).unwrap(),
            original
        );
        assert!(!restart_path(&dir.path().join("model.gguf.part")).exists());
    }

    #[test]
    fn ignored_resume_range_mid_replacement_cancel_preserves_original_partial() {
        let dir = tempdir().unwrap();
        let bytes = vec![5_u8; COPY_BUFFER_BYTES * 2 + 3];
        let split = 19;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let original = bytes[..split].to_vec();
        fs::write(dir.path().join("model.gguf.part"), &original).unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes,
            }],
        );
        let mut observer = RecordingObserver {
            cancel_after: Some(COPY_BUFFER_BYTES as u64),
            ..Default::default()
        };

        let error = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap_err();

        assert!(matches!(error, DownloadError::Cancelled));
        assert_eq!(
            fs::read(dir.path().join("model.gguf.part")).unwrap(),
            original
        );
        assert!(!restart_path(&dir.path().join("model.gguf.part")).exists());
    }

    #[test]
    fn observer_completion_promotes_only_verified_sha_and_reports_total() {
        let dir = tempdir().unwrap();
        let bytes = b"verified completion".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes.clone(),
            }],
        );
        let mut observer = RecordingObserver::default();

        let path = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(
            observer.progress.last().unwrap().downloaded_bytes,
            model.size_bytes
        );
        assert!(!dir.path().join("model.gguf.part").exists());
    }

    #[test]
    fn ignored_range_rename_failure_cleans_restart_and_preserves_original_partial() {
        let dir = tempdir().unwrap();
        let bytes = b"replacement body".to_vec();
        let split = 5;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let original = bytes[..split].to_vec();
        let part = dir.path().join("model.gguf.part");
        fs::write(&part, &original).unwrap();
        let transport = RenameFailureTransport {
            bytes,
            final_path: dir.path().join("model.gguf"),
        };
        let mut observer = RecordingObserver::default();

        let error = download_with_transport_and_observer(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
        )
        .unwrap_err();

        assert!(matches!(error, DownloadError::UnsafeArtifactPath));
        assert!(!error.artifact_state_uncertain());
        assert_eq!(fs::read(&part).unwrap(), original);
        assert!(!restart_path(&part).exists());
    }

    #[test]
    fn download_url_uses_the_verified_revision() {
        let url = build_download_url(
            "https://huggingface.co",
            "owner/repo",
            "0123456789abcdef0123456789abcdef01234567",
            "model.gguf",
        )
        .unwrap();
        assert_eq!(
            url.path(),
            "/owner/repo/resolve/0123456789abcdef0123456789abcdef01234567/model.gguf"
        );
    }

    #[test]
    fn filename_sanitization_rejects_unsafe_flat_names() {
        for filename in [
            "../x",
            "a/b",
            "a\\b",
            "",
            ".",
            "..",
            "name.",
            " white",
            "has space",
            "model:Q4.gguf",
            "a?.gguf",
            "bad<name>.gguf",
            "bad>name.gguf",
            "bad\"name.gguf",
            "pipe|name.gguf",
            "star*.gguf",
            "CON",
            "con.gguf",
            "NUL.gguf",
            "LPT1.gguf",
            "COM9.gguf",
            "CONOUT$.gguf",
        ] {
            assert!(
                matches!(
                    sanitize_filename(filename),
                    Err(DownloadError::InvalidFilename)
                ),
                "filename should be rejected: {filename:?}"
            );
        }

        assert_eq!(
            sanitize_filename("model-Q4_K_M.gguf").unwrap(),
            "model-Q4_K_M.gguf"
        );
    }

    #[test]
    fn model_dir_uses_userprofile_when_home_is_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _restore = EnvRestore::capture(&["HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH"]);
        let dir = tempdir().unwrap();
        env::remove_var("HOME");
        env::remove_var("HOMEDRIVE");
        env::remove_var("HOMEPATH");
        env::set_var("USERPROFILE", dir.path());

        let path = model_dir();

        assert_eq!(path, dir.path().join(".loxa").join("models"));
        assert!(path.exists());
    }

    #[test]
    fn home_dir_uses_windows_drive_and_path_fallback() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _restore = EnvRestore::capture(&["HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH"]);
        let dir = tempdir().unwrap();
        let mut drive = dir.path().as_os_str().to_os_string();
        drive.push(std::path::MAIN_SEPARATOR.to_string());
        env::remove_var("HOME");
        env::remove_var("USERPROFILE");
        env::set_var("HOMEDRIVE", drive);
        env::set_var("HOMEPATH", "profile");

        assert_eq!(home_dir(), dir.path().join("profile"));
    }

    #[test]
    fn content_range_parser_accepts_total_and_rejects_bad_values() {
        assert_eq!(
            parse_total_from_content_range("bytes 0-0/123").unwrap(),
            123
        );

        for header in ["bytes 1-1/123", "bytes 0-0/*", "bytes 0-0", "bad"] {
            assert!(
                matches!(
                    parse_total_from_content_range(header),
                    Err(DownloadError::InvalidContentRange)
                ),
                "header should be rejected: {header:?}"
            );
        }
    }

    #[test]
    fn resumed_progress_bar_tracks_only_remaining_transfer() {
        let progress = progress_bar(100, "model.gguf", 40);

        assert_eq!(progress.length(), Some(60));
        assert_eq!(progress.position(), 0);
    }

    #[test]
    fn resumed_copy_reports_current_session_bytes_to_progress() {
        let dir = tempdir().unwrap();
        let part_path = dir.path().join("model.gguf.part");
        fs::write(&part_path, b"seed").unwrap();
        let mut file = OpenOptions::new().append(true).open(&part_path).unwrap();
        let mut response = Cursor::new(b"more".to_vec());
        let mut hasher = Sha256::new();
        let progress = progress_bar(10, "model.gguf", 4);

        let mut observer = NoopDownloadObserver;
        let total = copy_response_to_part(
            &mut response,
            &mut file,
            &mut hasher,
            &progress,
            4,
            &mut observer,
            10,
        )
        .unwrap();

        assert_eq!(total, 8);
        assert_eq!(progress.length(), Some(6));
        assert_eq!(progress.position(), 4);
    }

    #[test]
    fn resume_content_range_must_match_requested_offset_and_total() {
        assert!(validate_resume_content_range(Some("bytes 50-99/100"), 50, 100).is_ok());

        for (header, offset, total) in [
            (None, 50, 100),
            (Some("bytes 0-49/100"), 50, 100),
            (Some("bytes 50-99/101"), 50, 100),
            (Some("bytes 50-49/100"), 50, 100),
            (Some("bytes 50-100/100"), 50, 100),
            (Some("bytes 50-99/*"), 50, 100),
        ] {
            assert!(
                matches!(
                    validate_resume_content_range(header, offset, total),
                    Err(DownloadError::InvalidContentRange)
                ),
                "resume Content-Range should be rejected: {header:?}"
            );
        }
    }

    #[test]
    fn prefix_rehash_plus_suffix_matches_one_shot_hash() {
        let dir = tempdir().unwrap();
        let prefix = b"prefix bytes";
        let suffix = b" plus suffix";
        let path = dir.path().join("model.part");
        fs::write(&path, prefix).unwrap();

        let mut hasher = Sha256::new();
        let hashed_len = hash_existing_prefix_into(&mut hasher, &path).unwrap();
        hasher.update(suffix);

        assert_eq!(hashed_len, prefix.len() as u64);
        assert_eq!(
            hex_bytes(hasher.finalize().as_ref()),
            sha256_hex(&[prefix.as_slice(), suffix.as_slice()].concat())
        );
    }

    #[test]
    fn disk_space_check_rejects_shortfall() {
        assert!(ensure_enough_disk_space(100, 100).is_ok());
        assert!(ensure_enough_disk_space(101, 100).is_ok());

        let err = ensure_enough_disk_space(99, 100).unwrap_err();
        assert!(matches!(
            err,
            DownloadError::InsufficientDiskSpace {
                needed: 100,
                available: 99
            }
        ));
    }

    #[test]
    fn download_fetches_full_file_with_probe_body_checksum_and_rename() {
        let dir = tempdir().unwrap();
        let bytes = b"complete http body".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes.clone(),
            }],
        );

        let path = download_with_transport(&model, dir.path(), HF_BASE_URL, &transport).unwrap();

        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(!dir.path().join("model.gguf.part").exists());
        assert_eq!(transport.offsets(), vec![0]);
    }

    #[test]
    fn download_rejects_low_disk_space_before_body_request() {
        let dir = tempdir().unwrap();
        let bytes = b"needs-more-space".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes,
            }],
        );

        let err = download_with_transport_and_available_space(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            Some(model.size_bytes - 1),
        )
        .unwrap_err();

        assert!(matches!(err, DownloadError::InsufficientDiskSpace { .. }));
        assert_eq!(transport.offsets(), Vec::<u64>::new());
        assert!(!dir.path().join("model.gguf.part").exists());
    }

    #[test]
    fn download_resumes_partial_with_range_and_verified_suffix() {
        let dir = tempdir().unwrap();
        let bytes = b"prefix-and-suffix".to_vec();
        let split = 7;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        fs::write(dir.path().join("model.gguf.part"), &bytes[..split]).unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::PARTIAL_CONTENT,
                content_range: Some(format!("bytes {split}-{}/{}", bytes.len() - 1, bytes.len())),
                body: bytes[split..].to_vec(),
            }],
        );

        let path = download_with_transport(&model, dir.path(), HF_BASE_URL, &transport).unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(transport.offsets(), vec![split as u64]);
    }

    #[test]
    fn download_restarts_from_zero_when_server_ignores_resume_range() {
        let dir = tempdir().unwrap();
        let bytes = b"full-body-after-ignored-range".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        fs::write(dir.path().join("model.gguf.part"), b"stale-prefix").unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes.clone(),
            }],
        );

        let path = download_with_transport(&model, dir.path(), HF_BASE_URL, &transport).unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(transport.offsets(), vec![12]);
    }

    #[test]
    fn ignored_resume_range_requires_full_replacement_space_while_preserving_partial() {
        let dir = tempdir().unwrap();
        let bytes = b"01234567890123456789".to_vec();
        let split = 10;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        fs::write(dir.path().join("model.gguf.part"), &bytes[..split]).unwrap();
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes.clone(),
            }],
        );

        let error = download_with_transport_and_available_space(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            Some(12),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DownloadError::InsufficientDiskSpace {
                needed: 20,
                available: 12
            }
        ));
        assert_eq!(transport.offsets(), vec![split as u64]);
        assert_eq!(
            fs::read(dir.path().join("model.gguf.part")).unwrap(),
            bytes[..split]
        );
        assert!(!restart_path(&dir.path().join("model.gguf.part")).exists());
    }

    #[test]
    fn interrupted_body_keeps_part_and_next_call_resumes() {
        let dir = tempdir().unwrap();
        let bytes = b"interrupted-then-resumed".to_vec();
        let split = 11;
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let interrupted = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes[..split].to_vec(),
            }],
        );

        let err =
            download_with_transport(&model, dir.path(), HF_BASE_URL, &interrupted).unwrap_err();

        assert!(matches!(err, DownloadError::SizeMismatch { .. }));
        assert_eq!(
            fs::read(dir.path().join("model.gguf.part")).unwrap(),
            bytes[..split]
        );

        let resumed = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::PARTIAL_CONTENT,
                content_range: Some(format!("bytes {split}-{}/{}", bytes.len() - 1, bytes.len())),
                body: bytes[split..].to_vec(),
            }],
        );

        let path = download_with_transport(&model, dir.path(), HF_BASE_URL, &resumed).unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(resumed.offsets(), vec![split as u64]);
    }

    #[test]
    #[ignore = "hits Hugging Face; run manually when validating live downloader behavior"]
    fn live_huggingface_tiny_download_verifies_size_hash_and_final_path() {
        let dir = tempdir().unwrap();
        let entry = live_small_entry();

        let path = download(&entry, dir.path()).unwrap();

        assert_eq!(path, dir.path().join("config.json"));
        assert_eq!(fs::read(&path).unwrap(), live_small_config_bytes());
        assert!(!dir.path().join("config.json.part").exists());
        assert_eq!(hash_file(&path).unwrap(), entry.sha256);
    }

    #[test]
    #[ignore = "hits Hugging Face; run manually when validating live resume behavior"]
    fn live_huggingface_tiny_download_resumes_existing_part() {
        let dir = tempdir().unwrap();
        let entry = live_small_entry();
        let prefix_len = 128;
        fs::write(
            dir.path().join("config.json.part"),
            &live_small_config_bytes()[..prefix_len],
        )
        .unwrap();

        let path = download(&entry, dir.path()).unwrap();

        assert_eq!(fs::read(&path).unwrap(), live_small_config_bytes());
        assert!(!dir.path().join("config.json.part").exists());
        assert_eq!(hash_file(&path).unwrap(), entry.sha256);
    }

    #[test]
    fn hf_token_is_added_only_for_huggingface_host() {
        let _guard = ENV_LOCK.lock().unwrap();
        env::remove_var("HF_TOKEN_PATH");
        env::remove_var("HF_HOME");
        env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN");
        env::set_var("HF_TOKEN", "test-token");
        let client = Client::builder().build().unwrap();
        let hf_url =
            Url::parse("https://huggingface.co/owner/repo/resolve/main/model.gguf").unwrap();
        let local_url = Url::parse("http://127.0.0.1/model.gguf").unwrap();

        let hf_request = send_with_hf_token(client.get(hf_url.clone()), &hf_url)
            .build()
            .unwrap();
        let local_request = send_with_hf_token(client.get(local_url.clone()), &local_url)
            .build()
            .unwrap();
        env::remove_var("HF_TOKEN");

        assert_eq!(
            hf_request.headers().get(AUTHORIZATION).unwrap(),
            "Bearer test-token"
        );
        assert!(local_request.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn hf_token_path_file_is_used_when_env_token_is_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let token_path = dir.path().join("token-file");
        fs::write(&token_path, "path-token\n").unwrap();
        env::remove_var("HF_TOKEN");
        env::remove_var("HF_HOME");
        env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN");
        env::set_var("HF_TOKEN_PATH", &token_path);
        let client = Client::builder().build().unwrap();
        let hf_url =
            Url::parse("https://huggingface.co/owner/repo/resolve/main/model.gguf").unwrap();

        let request = send_with_hf_token(client.get(hf_url.clone()), &hf_url)
            .build()
            .unwrap();
        env::remove_var("HF_TOKEN_PATH");

        assert_eq!(
            request.headers().get(AUTHORIZATION).unwrap(),
            "Bearer path-token"
        );
    }

    #[test]
    fn hf_home_token_file_is_used_when_no_token_env_or_path_exists() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("token"), "home-token\n").unwrap();
        env::remove_var("HF_TOKEN");
        env::remove_var("HF_TOKEN_PATH");
        env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN");
        env::set_var("HF_HOME", dir.path());
        let client = Client::builder().build().unwrap();
        let hf_url =
            Url::parse("https://huggingface.co/owner/repo/resolve/main/model.gguf").unwrap();

        let request = send_with_hf_token(client.get(hf_url.clone()), &hf_url)
            .build()
            .unwrap();
        env::remove_var("HF_HOME");

        assert_eq!(
            request.headers().get(AUTHORIZATION).unwrap(),
            "Bearer home-token"
        );
    }

    #[test]
    fn implicit_hf_token_can_be_disabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("token"), "home-token\n").unwrap();
        env::remove_var("HF_TOKEN");
        env::remove_var("HF_TOKEN_PATH");
        env::set_var("HF_HOME", dir.path());
        env::set_var("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1");
        let client = Client::builder().build().unwrap();
        let hf_url =
            Url::parse("https://huggingface.co/owner/repo/resolve/main/model.gguf").unwrap();

        let request = send_with_hf_token(client.get(hf_url.clone()), &hf_url)
            .build()
            .unwrap();
        env::remove_var("HF_HOME");
        env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN");

        assert!(request.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn oversized_partial_is_deleted_and_restarts_from_zero() {
        let dir = tempdir().unwrap();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(b"abc").into_boxed_str()),
            3,
        );
        fs::write(dir.path().join("model.gguf.part"), b"abcd").unwrap();

        let decision = inspect_existing_download(&model, dir.path()).unwrap();

        assert_eq!(decision, ExistingDownload::Download { resume_from: 0 });
        assert!(!dir.path().join("model.gguf.part").exists());
    }

    #[test]
    fn completed_partial_verifies_then_renames() {
        let dir = tempdir().unwrap();
        let bytes = b"complete";
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let part = dir.path().join("model.gguf.part");
        let final_path = dir.path().join("model.gguf");
        fs::write(&part, bytes).unwrap();

        let decision = inspect_existing_download(&model, dir.path()).unwrap();

        assert_eq!(decision, ExistingDownload::Ready(final_path.clone()));
        assert!(!part.exists());
        assert_eq!(fs::read(final_path).unwrap(), bytes);
    }

    #[test]
    fn checksum_mismatch_deletes_completed_partial_and_errors() {
        let dir = tempdir().unwrap();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(b"expected").into_boxed_str()),
            3,
        );
        let part = dir.path().join("model.gguf.part");
        fs::write(&part, b"bad").unwrap();

        let err = inspect_existing_download(&model, dir.path()).unwrap_err();

        assert!(matches!(err, DownloadError::ChecksumMismatch { .. }));
        assert!(!part.exists());
    }

    #[test]
    fn todo_verify_completed_partial_warns_and_keeps_file() {
        let dir = tempdir().unwrap();
        let bytes = b"unchecked";
        let model = entry("model.gguf", "TODO_VERIFY", bytes.len() as u64);
        let part = dir.path().join("model.gguf.part");
        let final_path = dir.path().join("model.gguf");
        fs::write(&part, bytes).unwrap();
        let mut warnings = CapturedWarnings::default();

        let decision =
            inspect_existing_download_with_warnings(&model, dir.path(), &mut warnings, true)
                .unwrap();

        assert_eq!(decision, ExistingDownload::Ready(final_path.clone()));
        assert!(!part.exists());
        assert_eq!(fs::read(final_path).unwrap(), bytes);
        assert!(
            warnings
                .0
                .iter()
                .any(|message| message.contains("hash unverified")),
            "expected TODO_VERIFY path to warn"
        );
    }

    #[test]
    fn early_return_verified_path_does_not_rewrite_final_file() {
        let dir = tempdir().unwrap();
        let bytes = b"already here";
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let final_path = dir.path().join("model.gguf");
        fs::write(&final_path, bytes).unwrap();
        let original_modified = fs::metadata(&final_path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let decision = inspect_existing_download(&model, dir.path()).unwrap();

        assert_eq!(decision, ExistingDownload::Ready(final_path.clone()));
        assert_eq!(fs::read(&final_path).unwrap(), bytes);
        assert_eq!(
            fs::metadata(final_path).unwrap().modified().unwrap(),
            original_modified
        );
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum InjectedArtifactFault {
        PartialSync,
        Rename,
        FinalSync,
        ParentSync,
    }

    struct FaultArtifactFileOps(InjectedArtifactFault);

    impl ArtifactFileOps for FaultArtifactFileOps {
        fn sync_partial(&self, file: &File) -> io::Result<()> {
            if self.0 == InjectedArtifactFault::PartialSync {
                Err(io::Error::other("injected partial sync failure"))
            } else {
                file.sync_all()
            }
        }

        fn rename(&self, from: &Path, to: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            if self.0 == InjectedArtifactFault::Rename {
                fs::rename(from, to)?;
                Err(io::Error::other(
                    "injected rename failure after destination mutation",
                ))
            } else {
                fs::rename(from, to)
            }
        }

        fn sync_final(&self, file: &File, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            if self.0 == InjectedArtifactFault::FinalSync {
                Err(io::Error::other("injected final sync failure"))
            } else {
                file.sync_all()
            }
        }

        fn sync_parent(&self, parent: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            if self.0 == InjectedArtifactFault::ParentSync {
                Err(io::Error::other("injected parent sync failure"))
            } else {
                File::open(parent)?.sync_all()
            }
        }
    }

    #[test]
    fn every_finalize_fault_returns_failure_before_verified_publication() {
        for fault in [
            InjectedArtifactFault::PartialSync,
            InjectedArtifactFault::Rename,
            InjectedArtifactFault::FinalSync,
            InjectedArtifactFault::ParentSync,
        ] {
            let dir = tempdir().unwrap();
            let bytes = b"durable artifact".to_vec();
            let model = entry(
                "model.gguf",
                Box::leak(sha256_hex(&bytes).into_boxed_str()),
                bytes.len() as u64,
            );
            let transport = FakeTransport::new(
                bytes.len() as u64,
                vec![FakeBody {
                    status: StatusCode::OK,
                    content_range: None,
                    body: bytes,
                }],
            );
            let mut observer = RecordingObserver::default();

            let result = download_with_transport_observer_and_file_ops(
                &model,
                dir.path(),
                HF_BASE_URL,
                &transport,
                &mut observer,
                Some(u64::MAX),
                &FaultArtifactFileOps(fault),
            );

            match (fault, result.unwrap_err()) {
                (InjectedArtifactFault::PartialSync, DownloadError::Io(_)) => {}
                (
                    InjectedArtifactFault::Rename,
                    DownloadError::ArtifactFinalizationUncertain {
                        stage: ArtifactFinalizationStage::Rename,
                        ..
                    },
                ) => {}
                (
                    InjectedArtifactFault::FinalSync,
                    DownloadError::ArtifactFinalizationUncertain {
                        stage: ArtifactFinalizationStage::FinalFileSync,
                        ..
                    },
                ) => {}
                (
                    InjectedArtifactFault::ParentSync,
                    DownloadError::ArtifactFinalizationUncertain {
                        stage: ArtifactFinalizationStage::ParentDirectorySync,
                        ..
                    },
                ) => {}
                (fault, error) => panic!("unexpected classification for {fault:?}: {error:?}"),
            }
        }
    }

    struct PostRenameHardlinkOps {
        alias: PathBuf,
    }

    impl ArtifactFileOps for PostRenameHardlinkOps {
        fn sync_partial(&self, file: &File) -> io::Result<()> {
            file.sync_all()
        }

        fn rename(
            &self,
            from: &Path,
            to: &Path,
            evidence: &ArtifactPromotionEvidence,
        ) -> io::Result<()> {
            StdArtifactFileOps.rename(from, to, evidence)?;
            fs::hard_link(to, &self.alias)
        }

        fn sync_final(&self, file: &File, evidence: &ArtifactPromotionEvidence) -> io::Result<()> {
            StdArtifactFileOps.sync_final(file, evidence)
        }

        fn sync_parent(
            &self,
            parent: &Path,
            evidence: &ArtifactPromotionEvidence,
        ) -> io::Result<()> {
            StdArtifactFileOps.sync_parent(parent, evidence)
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn post_rename_hardlink_is_reported_as_uncertain_publication() {
        let dir = tempdir().unwrap();
        let bytes = b"hardlink after rename".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes,
            }],
        );
        let mut observer = RecordingObserver::default();

        let error = download_with_transport_observer_and_file_ops(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
            &PostRenameHardlinkOps {
                alias: dir.path().join("model-alias.gguf"),
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DownloadError::ArtifactFinalizationUncertain {
                stage: ArtifactFinalizationStage::Rename,
                ..
            }
        ));
        assert!(error.artifact_state_uncertain());
    }

    #[test]
    fn std_promotion_rejects_parent_swap_before_rename() {
        let dir = tempdir().unwrap();
        let parent = dir.path().join("models");
        let displaced = dir.path().join("models-displaced");
        fs::create_dir(&parent).unwrap();
        let source = parent.join("model.gguf.part");
        let destination = parent.join("model.gguf");
        fs::write(&source, b"artifact").unwrap();
        let source_file = open_regular_read_no_follow(&source).unwrap();
        let evidence =
            ArtifactPromotionEvidence::capture(&source_file, &source, &destination).unwrap();

        fs::rename(&parent, &displaced).unwrap();
        fs::create_dir(&parent).unwrap();
        fs::write(&source, b"attacker replacement").unwrap();

        let error = StdArtifactFileOps
            .rename(&source, &destination, &evidence)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!destination.exists());
        assert_eq!(
            fs::read(displaced.join("model.gguf.part")).unwrap(),
            b"artifact"
        );
    }

    struct PermissionDeniedArtifactFileOps;

    impl ArtifactFileOps for PermissionDeniedArtifactFileOps {
        fn sync_partial(&self, _: &File) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected permission denial",
            ))
        }

        fn rename(&self, from: &Path, to: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            fs::rename(from, to)
        }

        fn sync_final(&self, file: &File, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            file.sync_all()
        }

        fn sync_parent(&self, parent: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            File::open(parent)?.sync_all()
        }
    }

    #[test]
    fn finalize_permission_denial_fails_closed_and_retains_partial() {
        let dir = tempdir().unwrap();
        let bytes = b"permission artifact".to_vec();
        let model = entry(
            "model.gguf",
            Box::leak(sha256_hex(&bytes).into_boxed_str()),
            bytes.len() as u64,
        );
        let transport = FakeTransport::new(
            bytes.len() as u64,
            vec![FakeBody {
                status: StatusCode::OK,
                content_range: None,
                body: bytes.clone(),
            }],
        );
        let mut observer = RecordingObserver::default();

        let error = download_with_transport_observer_and_file_ops(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            &mut observer,
            Some(u64::MAX),
            &PermissionDeniedArtifactFileOps,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DownloadError::Io(ref error) if error.kind() == io::ErrorKind::PermissionDenied
        ));
        assert_eq!(fs::read(dir.path().join("model.gguf.part")).unwrap(), bytes);
        assert!(!dir.path().join("model.gguf").exists());
    }

    #[derive(Clone, Copy)]
    enum CancelFinalizeBoundary {
        PartialSync,
        Rename,
        FinalSync,
        ParentSync,
    }

    struct BoundaryCancellationOps {
        boundary: CancelFinalizeBoundary,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    }

    impl BoundaryCancellationOps {
        fn cancel(&self, boundary: CancelFinalizeBoundary) {
            if std::mem::discriminant(&self.boundary) == std::mem::discriminant(&boundary) {
                self.cancelled
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }

    impl ArtifactFileOps for BoundaryCancellationOps {
        fn sync_partial(&self, file: &File) -> io::Result<()> {
            file.sync_all()?;
            self.cancel(CancelFinalizeBoundary::PartialSync);
            Ok(())
        }

        fn rename(&self, from: &Path, to: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            fs::rename(from, to)?;
            self.cancel(CancelFinalizeBoundary::Rename);
            Ok(())
        }

        fn sync_final(&self, file: &File, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            file.sync_all()?;
            self.cancel(CancelFinalizeBoundary::FinalSync);
            Ok(())
        }

        fn sync_parent(&self, parent: &Path, _: &ArtifactPromotionEvidence) -> io::Result<()> {
            File::open(parent)?.sync_all()?;
            self.cancel(CancelFinalizeBoundary::ParentSync);
            Ok(())
        }
    }

    struct SharedCancellationObserver(Arc<std::sync::atomic::AtomicBool>);

    impl DownloadObserver for SharedCancellationObserver {
        fn is_cancelled(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::Acquire)
        }

        fn progress(&mut self, _: DownloadProgress) {}
    }

    #[test]
    fn cancellation_wins_before_rename_and_loses_after_promotion_at_every_finalize_boundary() {
        for boundary in [
            CancelFinalizeBoundary::PartialSync,
            CancelFinalizeBoundary::Rename,
            CancelFinalizeBoundary::FinalSync,
            CancelFinalizeBoundary::ParentSync,
        ] {
            let dir = tempdir().unwrap();
            let bytes = b"cancellation boundary".to_vec();
            let model = entry(
                "model.gguf",
                Box::leak(sha256_hex(&bytes).into_boxed_str()),
                bytes.len() as u64,
            );
            let transport = FakeTransport::new(
                bytes.len() as u64,
                vec![FakeBody {
                    status: StatusCode::OK,
                    content_range: None,
                    body: bytes.clone(),
                }],
            );
            let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut observer = SharedCancellationObserver(Arc::clone(&cancelled));
            let result = download_with_transport_observer_and_file_ops(
                &model,
                dir.path(),
                HF_BASE_URL,
                &transport,
                &mut observer,
                Some(u64::MAX),
                &BoundaryCancellationOps {
                    boundary,
                    cancelled,
                },
            );

            if matches!(boundary, CancelFinalizeBoundary::PartialSync) {
                assert!(matches!(result, Err(DownloadError::Cancelled)));
                assert!(dir.path().join("model.gguf.part").is_file());
                assert!(!dir.path().join("model.gguf").exists());
            } else {
                assert_eq!(fs::read(result.unwrap()).unwrap(), bytes);
                assert!(!dir.path().join("model.gguf.part").exists());
            }
        }
    }
}
