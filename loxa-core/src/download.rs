use crate::registry::ModelEntry;
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

#[derive(Debug)]
pub enum DownloadError {
    AuthRequired,
    Forbidden,
    InvalidFilename,
    InvalidContentRange,
    ChecksumMismatch { expected: String, actual: String },
    SizeMismatch { expected: u64, actual: u64 },
    InsufficientDiskSpace { needed: u64, available: u64 },
    Http(String),
    Io(std::io::Error),
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadError::AuthRequired => write!(
                f,
                "authentication required by Hugging Face; set HF_TOKEN if this is a private or gated repo"
            ),
            DownloadError::Forbidden => write!(
                f,
                "Hugging Face returned 403 forbidden; check HF_TOKEN and gated repos access"
            ),
            DownloadError::InvalidFilename => write!(f, "invalid flat model filename"),
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
        }
    }
}

impl Error for DownloadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DownloadError::Io(error) => Some(error),
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

struct ReqwestTransport {
    client: Client,
}

struct DownloadBodyContext<'a> {
    entry: &'a ModelEntry,
    part_path: &'a Path,
    final_path: &'a Path,
    progress: &'a ProgressBar,
    available_space_override: Option<u64>,
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

pub fn download(entry: &ModelEntry, dest_dir: &Path) -> Result<PathBuf, DownloadError> {
    download_from_base_url(entry, dest_dir, HF_BASE_URL)
}

fn download_from_base_url(
    entry: &ModelEntry,
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

fn download_with_transport(
    entry: &ModelEntry,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
) -> Result<PathBuf, DownloadError> {
    download_with_transport_and_available_space(entry, dest_dir, base_url, transport, None)
}

fn download_with_transport_and_available_space(
    entry: &ModelEntry,
    dest_dir: &Path,
    base_url: &str,
    transport: &impl DownloadTransport,
    available_space_override: Option<u64>,
) -> Result<PathBuf, DownloadError> {
    let filename = sanitize_filename(entry.filename)?;
    fs::create_dir_all(dest_dir)?;

    let mut warnings = StderrWarnings;
    if let ExistingDownload::Ready(path) =
        inspect_existing_download_with_warnings(entry, dest_dir, &mut warnings, false)?
    {
        return Ok(path);
    }

    let url = build_download_url(base_url, entry.repo, &filename)?;
    let remote_size = transport.probe_size(&url)?;
    if remote_size != entry.size_bytes {
        return Err(DownloadError::SizeMismatch {
            expected: entry.size_bytes,
            actual: remote_size,
        });
    }

    let final_path = dest_dir.join(&filename);
    let part_path = part_path(dest_dir, &filename);
    let resume_from = match inspect_existing_download(entry, dest_dir)? {
        ExistingDownload::Ready(path) => return Ok(path),
        ExistingDownload::Download { resume_from } => resume_from,
    };
    let bytes_needed = entry.size_bytes.saturating_sub(resume_from);
    ensure_disk_space_for_download(dest_dir, bytes_needed, available_space_override)?;

    let progress = progress_bar(entry.size_bytes, &filename, resume_from);

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

fn ensure_disk_space_after_reclaiming_part(
    dest_dir: &Path,
    part_path: &Path,
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
    let reclaimable = match fs::metadata(part_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(DownloadError::Io(error)),
    };

    ensure_enough_disk_space(available.saturating_add(reclaimable), bytes_needed)
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

fn hash_file(path: &Path) -> Result<String, DownloadError> {
    let mut hasher = Sha256::new();
    hash_existing_prefix_into(&mut hasher, path)?;
    Ok(hex_bytes(hasher.finalize().as_ref()))
}

fn hash_existing_prefix_into(hasher: &mut Sha256, path: &Path) -> Result<u64, DownloadError> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut total = 0_u64;

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }

    Ok(total)
}

fn inspect_existing_download(
    entry: &ModelEntry,
    dest_dir: &Path,
) -> Result<ExistingDownload, DownloadError> {
    let mut warnings = StderrWarnings;
    inspect_existing_download_with_warnings(entry, dest_dir, &mut warnings, true)
}

fn inspect_existing_download_with_warnings(
    entry: &ModelEntry,
    dest_dir: &Path,
    warnings: &mut impl WarningSink,
    announce_resume: bool,
) -> Result<ExistingDownload, DownloadError> {
    let filename = sanitize_filename(entry.filename)?;
    fs::create_dir_all(dest_dir)?;

    let final_path = dest_dir.join(&filename);
    let part_path = part_path(dest_dir, &filename);

    if final_path.exists() {
        let final_size = fs::metadata(&final_path)?.len();
        if final_size == entry.size_bytes {
            if entry.sha256 == TODO_VERIFY {
                warnings.warn(&hash_unverified_warning(&final_path));
                return Ok(ExistingDownload::Ready(final_path));
            }

            let actual = hash_file(&final_path)?;
            if actual == entry.sha256 {
                eprintln!("already present, verified: {}", final_path.display());
                return Ok(ExistingDownload::Ready(final_path));
            }

            fs::remove_file(&final_path)?;
        } else {
            fs::remove_file(&final_path)?;
        }
    }

    if part_path.exists() {
        let part_size = fs::metadata(&part_path)?.len();
        if part_size > entry.size_bytes {
            fs::remove_file(&part_path)?;
            return Ok(ExistingDownload::Download { resume_from: 0 });
        }

        if part_size == entry.size_bytes {
            verify_part_and_rename(entry, &part_path, &final_path, warnings)?;
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

fn build_download_url(base_url: &str, repo: &str, filename: &str) -> Result<Url, DownloadError> {
    let mut url = Url::parse(base_url).map_err(|error| DownloadError::Http(error.to_string()))?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| DownloadError::Http("could not build Hugging Face URL".to_string()))?;
        for segment in repo.split('/') {
            segments.push(segment);
        }
        segments.push("resolve");
        segments.push("main");
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
        let hashed = hash_existing_prefix_into(&mut hasher, context.part_path)?;
        if hashed != offset {
            return Err(DownloadError::SizeMismatch {
                expected: offset,
                actual: hashed,
            });
        }
    }

    let mut response = transport.body(&url, offset)?;
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
        ensure_disk_space_after_reclaiming_part(
            context.final_path.parent().unwrap_or(Path::new(".")),
            context.part_path,
            context.entry.size_bytes,
            context.available_space_override,
        )?;
        if context.part_path.exists() {
            fs::remove_file(context.part_path)?;
        }
        hasher = Sha256::new();
        offset = 0;
        context.progress.set_length(context.entry.size_bytes);
        context.progress.set_position(0);
        context.progress.reset_eta();
    }

    let mut file = if offset > 0 {
        validate_resume_content_range(
            response.content_range.as_deref(),
            offset,
            context.entry.size_bytes,
        )?;
        OpenOptions::new().append(true).open(context.part_path)?
    } else {
        File::create(context.part_path)?
    };

    let total = copy_response_to_part(
        response.reader.as_mut(),
        &mut file,
        &mut hasher,
        context.progress,
        offset,
    )?;
    if total != context.entry.size_bytes {
        return Err(DownloadError::SizeMismatch {
            expected: context.entry.size_bytes,
            actual: total,
        });
    }

    verify_hash_policy(context.entry, context.part_path, hasher)?;
    fs::rename(context.part_path, context.final_path)?;
    Ok(context.final_path.to_path_buf())
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
) -> Result<u64, DownloadError> {
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut total = start;

    loop {
        let read = response.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        total += read as u64;
        progress.set_position(total.saturating_sub(start));
    }

    file.flush()?;
    Ok(total)
}

fn verify_part_and_rename(
    entry: &ModelEntry,
    part_path: &Path,
    final_path: &Path,
    warnings: &mut impl WarningSink,
) -> Result<(), DownloadError> {
    let actual_size = fs::metadata(part_path)?.len();
    if actual_size != entry.size_bytes {
        return Err(DownloadError::SizeMismatch {
            expected: entry.size_bytes,
            actual: actual_size,
        });
    }

    if entry.sha256 == TODO_VERIFY {
        warnings.warn(&hash_unverified_warning(part_path));
        fs::rename(part_path, final_path)?;
        return Ok(());
    }

    let actual = hash_file(part_path)?;
    if actual != entry.sha256 {
        fs::remove_file(part_path)?;
        return Err(DownloadError::ChecksumMismatch {
            expected: entry.sha256.to_string(),
            actual,
        });
    }

    fs::rename(part_path, final_path)?;
    Ok(())
}

fn verify_hash_policy(
    entry: &ModelEntry,
    part_path: &Path,
    hasher: Sha256,
) -> Result<(), DownloadError> {
    if entry.sha256 == TODO_VERIFY {
        eprintln!(
            "warning: hash unverified for downloaded file {}",
            part_path.display()
        );
        return Ok(());
    }

    let actual = hex_bytes(hasher.finalize().as_ref());
    if actual != entry.sha256 {
        fs::remove_file(part_path)?;
        return Err(DownloadError::ChecksumMismatch {
            expected: entry.sha256.to_string(),
            actual,
        });
    }

    Ok(())
}

fn part_path(dest_dir: &Path, filename: &str) -> PathBuf {
    dest_dir.join(format!("{filename}.part"))
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
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::io::Cursor;
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

        let total =
            copy_response_to_part(&mut response, &mut file, &mut hasher, &progress, 4).unwrap();

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
    fn ignored_resume_range_counts_reclaimable_part_space_before_restart() {
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

        let path = download_with_transport_and_available_space(
            &model,
            dir.path(),
            HF_BASE_URL,
            &transport,
            Some(12),
        )
        .unwrap();

        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(transport.offsets(), vec![split as u64]);
        assert!(!dir.path().join("model.gguf.part").exists());
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
}
