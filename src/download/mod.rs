mod artifact;
mod http;

use crate::huggingface::ResolvedFile;
use artifact::download_once;
#[cfg(test)]
use artifact::hex;
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(test)]
use retry::delay::NoDelay;
use retry::delay::{jitter, Exponential};
use retry::{retry, OperationResult};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) use artifact::verify_regular;
#[cfg(test)]
pub(crate) use http::artifact_url;
#[cfg(test)]
use http::{redirect_target, transient_status, Transfer, TransferError};
use http::{ReqwestTransport, Transport};

const MAX_RETRIES: usize = 3;

#[derive(Debug, Eq, PartialEq)]
enum ProgressUpdate {
    Seed(u64),
    Position(u64),
    Verifying,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DownloadOutcome {
    Pulled(PathBuf),
    AlreadyInstalled(PathBuf),
}

impl AsRef<Path> for DownloadOutcome {
    fn as_ref(&self) -> &Path {
        match self {
            Self::Pulled(path) | Self::AlreadyInstalled(path) => path,
        }
    }
}

pub fn download(
    file: &ResolvedFile,
    model_dir: &Path,
    token: Option<String>,
) -> Result<DownloadOutcome, String> {
    let transport = ReqwestTransport::new(token)?;
    let progress = ProgressBar::new(file.size);
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} {eta}",
    )
    .map_err(|error| error.to_string())?
    .progress_chars("=>-");
    progress.set_style(style);
    progress.set_message(file.filename.clone());
    let delays = Exponential::from_millis(250).map(jitter).take(MAX_RETRIES);
    let result = download_with_transport_progress_and_delays(
        file,
        model_dir,
        &transport,
        delays,
        |update| match update {
            ProgressUpdate::Seed(position) => {
                progress.set_message(file.filename.clone());
                progress.set_position(position);
                progress.reset_eta();
            }
            ProgressUpdate::Position(position) => progress.set_position(position),
            ProgressUpdate::Verifying => {
                progress.set_message(format!("Verifying {}", file.filename));
                progress.tick();
            }
        },
    );
    progress.finish_and_clear();
    result
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
    download_with_transport_progress_and_delays(
        spec,
        model_dir,
        transport,
        NoDelay.take(MAX_RETRIES),
        progress,
    )
}

fn download_with_transport_progress_and_delays(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    delays: impl IntoIterator<Item = Duration>,
    mut progress: impl FnMut(ProgressUpdate),
) -> Result<DownloadOutcome, String> {
    retry(delays, || {
        match download_once(spec, model_dir, transport, &mut progress) {
            Ok(path) => OperationResult::Ok(path),
            Err(error) if error.is_retryable() => OperationResult::Retry(error),
            Err(error) => OperationResult::Err(error),
        }
    })
    .map_err(|error| error.error.into_message())
}

#[cfg(test)]
include!("tests.rs");
