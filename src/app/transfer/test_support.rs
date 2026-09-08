use super::execution::transfer_selected_with_observers;
use super::{
    TransferControl, TransferError, TransferErrorKind, TransferProgress, TransferResult,
    TransferSelected,
};
use crate::app::AppService;
use crate::download::{self, DownloadFailure, DownloadTerminalOutcome, VerifiedRegularFile};
use crate::huggingface::ResolvedFile;

#[cfg(test)]
pub(super) fn transfer_selected_with<F, C, T, D>(
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
pub(super) fn transfer_selected_with_proof<F, C, T, D>(
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
pub(super) fn transfer_selected_with_lookup_observer<F, C, T, D, L>(
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
pub(super) fn transfer_selected_with_publication_observer<F, C, T, D, P>(
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
