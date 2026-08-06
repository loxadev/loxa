mod artifact;
mod http;

use crate::huggingface::ResolvedFile;
use artifact::download_once;
#[cfg(test)]
use artifact::hex;
use backon::{ExponentialBuilder, RetryableWithContext};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) use artifact::verify_regular;
#[cfg(test)]
pub(crate) use http::artifact_url;
#[cfg(test)]
use http::{redirect_target, transient_status, Transfer};
use http::{ReqwestTransport, TransferError, Transport};

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
    let progress = ProgressBar::new(file.size());
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} {eta}",
    )
    .map_err(|error| error.to_string())?
    .progress_chars("=>-");
    progress.set_style(style);
    progress.set_message(file.path().to_owned());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let result = runtime.block_on(async {
        let transport = ReqwestTransport::new(token)?;
        download_with_transport_progress_async(file, model_dir, &transport, |update| match update {
            ProgressUpdate::Seed(position) => {
                progress.set_message(file.path().to_owned());
                progress.set_position(position);
                progress.reset_eta();
            }
            ProgressUpdate::Position(position) => progress.set_position(position),
            ProgressUpdate::Verifying => {
                progress.set_message(format!("Verifying {}", file.path()));
                progress.tick();
            }
        })
        .await
    });
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
    test_runtime().block_on(download_with_transport_progress_async(
        spec, model_dir, transport, progress,
    ))
}

async fn download_with_transport_progress_async(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    mut progress: impl FnMut(ProgressUpdate),
) -> Result<DownloadOutcome, String> {
    let context = (transport, &mut progress);
    let (_, result) = (|context| retry_once(spec, model_dir, context))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(250))
                .with_jitter()
                .with_max_times(MAX_RETRIES),
        )
        .sleep(tokio::time::sleep)
        .when(|error: &TransferError| error.is_retryable())
        .context(context)
        .await;
    result.map_err(TransferError::into_message)
}

async fn retry_once<'a, T, P>(
    spec: &ResolvedFile,
    model_dir: &Path,
    context: (&'a T, &'a mut P),
) -> ((&'a T, &'a mut P), Result<DownloadOutcome, TransferError>)
where
    T: Transport,
    P: FnMut(ProgressUpdate),
{
    let (transport, progress) = context;
    let result = download_once(spec, model_dir, transport, &mut *progress).await;
    ((transport, progress), result)
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
