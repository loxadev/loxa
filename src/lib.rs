pub mod app;
pub mod catalog;
pub mod chat;
pub mod cli;
pub mod config;
mod diagnostics;
pub mod discovery;
#[cfg(test)]
mod discovery_public_contract_tests {
    use crate::app::AppService;
    use crate::discovery::{
        ArtifactCandidate, AuxiliaryRole, CandidateDisposition, DiscoveryError, DiscoveryErrorKind,
        GatedStatus, InspectRepository, ModelSearchHit, ModelSearchPage, RepositoryPlan,
        SearchModels, UnsupportedPackagingReason,
    };
    use crate::huggingface::ResolvedFile;

    #[test]
    fn discovery_public_contract_has_exact_owned_accessors() {
        let search = SearchModels::new("two words".into());
        assert_eq!(search.query(), "two words");
        let inspect = InspectRepository::new("owner/repo".into(), Some("main".into()));
        assert_eq!(inspect.repo(), "owner/repo");
        assert_eq!(inspect.revision(), Some("main"));

        let _: fn(&ModelSearchPage) -> &[ModelSearchHit] = ModelSearchPage::hits;
        let _: fn(&ModelSearchHit) -> &str = ModelSearchHit::repo;
        let _: fn(&ModelSearchHit) -> GatedStatus = ModelSearchHit::gated;
        let _: fn(&ModelSearchHit) -> Option<u64> = ModelSearchHit::downloads;
        let _: fn(&RepositoryPlan) -> &str = RepositoryPlan::repo;
        let _: fn(&RepositoryPlan) -> &str = RepositoryPlan::commit;
        let _: fn(&RepositoryPlan) -> &[ArtifactCandidate] = RepositoryPlan::candidates;
        let _: fn(&ArtifactCandidate) -> &str = ArtifactCandidate::display_path;
        let _: fn(&ArtifactCandidate) -> Option<u64> = ArtifactCandidate::size;
        let _: fn(&ArtifactCandidate) -> Option<&ResolvedFile> = ArtifactCandidate::identity;
        let _: fn(&ArtifactCandidate) -> CandidateDisposition = ArtifactCandidate::disposition;
        let _: fn(&DiscoveryError) -> DiscoveryErrorKind = DiscoveryError::kind;
        let _: fn(&AppService, SearchModels) -> Result<ModelSearchPage, DiscoveryError> =
            AppService::search_models;
        let _: fn(&AppService, InspectRepository) -> Result<RepositoryPlan, DiscoveryError> =
            AppService::inspect_repository;

        assert_eq!(GatedStatus::Unknown, GatedStatus::Unknown);
        assert_eq!(AuxiliaryRole::Mtp, AuxiliaryRole::Mtp);
        assert_eq!(
            CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                AuxiliaryRole::Draft
            )),
            CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                AuxiliaryRole::Draft
            ))
        );
        assert_eq!(
            DiscoveryErrorKind::RevisionNotFound,
            DiscoveryErrorKind::RevisionNotFound
        );
    }
}
#[cfg(test)]
mod selected_transfer_public_contract_tests {
    use crate::app::transfer::{DiscardCandidate, TransferErrorKind};
    use crate::app::{
        AppService, ResolveArtifactError, ResolveArtifactRequest, TransferControl,
        TransferDisposition, TransferError, TransferPhase, TransferProgress, TransferResult,
        TransferSelected,
    };
    use crate::huggingface::ResolvedFile;

    fn assert_send_static<T: Send + 'static>() {}
    fn assert_clone_send_sync_static<T: Clone + Send + Sync + 'static>() {}
    fn assert_error<T: std::error::Error + Send + 'static>() {}

    #[test]
    fn selected_transfer_public_contract_compiles_the_approved_owned_values() {
        assert_send_static::<ResolveArtifactRequest>();
        assert_send_static::<TransferSelected>();
        assert_send_static::<TransferProgress>();
        assert_send_static::<TransferResult>();
        assert_clone_send_sync_static::<TransferControl>();
        assert_error::<ResolveArtifactError>();
        assert_error::<TransferError>();

        let _: fn(String, Option<String>, String) -> ResolveArtifactRequest =
            ResolveArtifactRequest::exact_file;
        let _: fn(String, Option<String>, String) -> ResolveArtifactRequest =
            ResolveArtifactRequest::unique_quant;
        let _: fn(ResolvedFile, Option<String>) -> TransferSelected = TransferSelected::new;
        let _: fn() -> TransferControl = TransferControl::new;
        let _: fn(&TransferControl) = TransferControl::request_pause;
        let _: fn(&TransferProgress) -> TransferPhase = TransferProgress::phase;
        let _: fn(&TransferProgress) -> u64 = TransferProgress::transferred_bytes;
        let _: fn(&TransferProgress) -> u64 = TransferProgress::total_bytes;
        let _: fn(&TransferResult) -> &str = TransferResult::model_id;
        let _: fn(&TransferResult) -> &ResolvedFile = TransferResult::artifact;
        let _: fn(&TransferResult) -> TransferDisposition = TransferResult::disposition;
        let _: fn(&TransferResult) -> Option<u64> = TransferResult::retained_bytes;
        let _: fn(&TransferResult) -> bool = TransferResult::discardable;
        let _: fn(&DiscardCandidate) -> &str = DiscardCandidate::model_id;
        let _: fn(&TransferError) -> TransferErrorKind = TransferError::kind;
        let _: fn(&TransferError) -> Option<u64> = TransferError::required_available_bytes;
        let _: fn(&TransferError) -> Option<u64> = TransferError::available_bytes;
        let _: fn(&TransferError) -> Option<&str> = TransferError::recovery_model_id;
        let _: fn(&TransferError) -> Option<&ResolvedFile> = TransferError::recovery_artifact;
        let _: fn(&TransferError) -> Option<u64> = TransferError::retained_bytes;
        let _: fn(&TransferError) -> bool = TransferError::discardable;

        fn service_signatures(
            service: &AppService,
            resolve: ResolveArtifactRequest,
            selected: TransferSelected,
            control: TransferControl,
            candidate: DiscardCandidate,
        ) {
            let _: Result<ResolvedFile, ResolveArtifactError> = service.resolve_artifact(resolve);
            let _: Result<TransferResult, TransferError> =
                service.transfer_selected(selected, control, |_: TransferProgress| {});
            let _: Result<DiscardCandidate, TransferError> =
                service.prepare_discard("model".into());
            let _: Result<(), TransferError> = service.discard_transfer(candidate);
        }
        let _ = service_signatures;

        assert_eq!(TransferPhase::Transferring, TransferPhase::Transferring);
        assert_eq!(TransferPhase::Verifying, TransferPhase::Verifying);
        assert_eq!(TransferPhase::Publishing, TransferPhase::Publishing);
        assert_eq!(
            TransferDisposition::Installed,
            TransferDisposition::Installed
        );
        assert_eq!(
            TransferDisposition::AlreadyInstalled,
            TransferDisposition::AlreadyInstalled
        );
        assert_eq!(TransferDisposition::Paused, TransferDisposition::Paused);
        assert_eq!(
            TransferDisposition::Interrupted,
            TransferDisposition::Interrupted
        );
    }
}
mod download;
pub mod huggingface;
pub mod paths;
mod runnable;
pub mod runner;
mod runtime;
mod safe_file;
mod session;
mod ui;
mod verification;

use catalog::Manifest;
use cli::{Cli, Command};
use indicatif::BinaryBytes;
use paths::{validate_id, AppPaths};
use runnable::resolve_runnable;
use std::io::IsTerminal;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;

#[cfg(unix)]
struct PromptInterrupt {
    id: signal_hook::SigId,
    interrupted: Arc<AtomicBool>,
}

#[cfg(unix)]
impl PromptInterrupt {
    fn install() -> Result<Self, String> {
        let interrupted = Arc::new(AtomicBool::new(false));
        let id = signal_hook::flag::register(signal_hook::consts::SIGINT, interrupted.clone())
            .map_err(|error| format!("failed to install prompt interrupt handler: {error}"))?;
        Ok(Self { id, interrupted })
    }

    fn received(&self) -> bool {
        self.interrupted.load(Ordering::SeqCst)
    }
}

#[cfg(unix)]
impl Drop for PromptInterrupt {
    fn drop(&mut self) {
        signal_hook::low_level::unregister(self.id);
        if self.received() {
            let _ = dialoguer::console::Term::stderr().show_cursor();
        }
    }
}

#[cfg(unix)]
struct DiscardPromptInterrupt {
    id: signal_hook::SigId,
    interrupted: Arc<AtomicBool>,
    wake_read: std::fs::File,
    _wake_write: std::fs::File,
}

#[cfg(unix)]
impl DiscardPromptInterrupt {
    fn install() -> Result<Self, String> {
        let mut descriptors = [-1; 2];
        if unsafe { libc::pipe(descriptors.as_mut_ptr()) } == -1 {
            return Err("failed to install discard interrupt handler".into());
        }
        let wake_read = unsafe { std::fs::File::from_raw_fd(descriptors[0]) };
        let wake_write = unsafe { std::fs::File::from_raw_fd(descriptors[1]) };
        for descriptor in [&wake_read, &wake_write] {
            let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFL) };
            if flags == -1
                || unsafe {
                    libc::fcntl(
                        descriptor.as_raw_fd(),
                        libc::F_SETFL,
                        flags | libc::O_NONBLOCK,
                    )
                } == -1
            {
                return Err("failed to install discard interrupt handler".into());
            }
        }
        let interrupted = Arc::new(AtomicBool::new(false));
        let signal_flag = interrupted.clone();
        let wake_descriptor = wake_write.as_raw_fd();
        let id = unsafe {
            signal_hook::low_level::register(signal_hook::consts::SIGINT, move || {
                signal_flag.store(true, Ordering::SeqCst);
                let byte = 1_u8;
                let _ = libc::write(wake_descriptor, (&byte as *const u8).cast(), 1);
            })
        }
        .map_err(|_| "failed to install discard interrupt handler".to_string())?;
        Ok(Self {
            id,
            interrupted,
            wake_read,
            _wake_write: wake_write,
        })
    }

    fn confirm(&self, model_id: &str) -> Result<Option<bool>, String> {
        use std::io::Write;

        eprint!("Discard incomplete download {model_id}? [y/N] ");
        std::io::stderr()
            .flush()
            .map_err(|_| "discard confirmation failed".to_string())?;
        loop {
            let mut descriptors = [
                libc::pollfd {
                    fd: libc::STDIN_FILENO,
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.wake_read.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            let result = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if result == -1 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err("discard confirmation failed".into());
            }
            if self.interrupted.load(Ordering::SeqCst) || descriptors[1].revents != 0 {
                eprintln!();
                return Ok(None);
            }
            if descriptors[0].revents != 0 {
                let mut answer = String::new();
                std::io::stdin()
                    .read_line(&mut answer)
                    .map_err(|_| "discard confirmation failed".to_string())?;
                return Ok(Some(matches!(
                    answer.trim().to_ascii_lowercase().as_str(),
                    "y" | "yes"
                )));
            }
        }
    }
}

#[cfg(unix)]
impl Drop for DiscardPromptInterrupt {
    fn drop(&mut self) {
        signal_hook::low_level::unregister(self.id);
    }
}

#[cfg(unix)]
struct ScopedTransferInterrupt {
    handle: signal_hook::iterator::Handle,
    listener: Option<std::thread::JoinHandle<()>>,
}

#[cfg(unix)]
impl ScopedTransferInterrupt {
    fn install(control: app::TransferControl) -> Result<Self, ()> {
        let mut signals =
            signal_hook::iterator::Signals::new([signal_hook::consts::SIGINT]).map_err(|_| ())?;
        let handle = signals.handle();
        let listener = std::thread::spawn(move || {
            for _ in signals.forever() {
                control.request_pause();
            }
        });
        Ok(Self {
            handle,
            listener: Some(listener),
        })
    }

    fn close_and_join(&mut self) {
        self.handle.close();
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
    }
}

#[cfg(unix)]
impl Drop for ScopedTransferInterrupt {
    fn drop(&mut self) {
        self.close_and_join();
    }
}

#[cfg(not(unix))]
struct ScopedTransferInterrupt;

#[cfg(not(unix))]
impl ScopedTransferInterrupt {
    fn install(_: app::TransferControl) -> Result<Self, ()> {
        Ok(Self)
    }
}

pub fn run_from_env() -> Result<i32, String> {
    let cli = cli::parse_checked();
    let paths = AppPaths::from_env()?;
    let _diagnostics = diagnostics::init(&paths.logs).map_err(|error| {
        format!(
            "failed to initialize diagnostics at {}: {error}",
            paths.logs.display()
        )
    })?;
    let command = command_name(&cli.command);
    tracing::info!(event = "cli_startup", command);
    let result = run(cli, paths);
    match &result {
        Ok(code) => tracing::info!(event = "cli_finished", command, exit_code = *code),
        Err(_) => tracing::error!(event = "cli_failed", command),
    }
    result
}

pub fn report_error(error: &str) {
    let danger = ui::danger();
    let error = ui::sanitize_terminal(error);
    anstream::eprintln!("{danger}Error:{danger:#} {error}");
    if let Some(path) = diagnostics::active_log_dir() {
        let muted = ui::muted();
        let path = ui::sanitize_terminal(&path.display().to_string());
        anstream::eprintln!("{muted}Diagnostics: {path}{muted:#}");
    }
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Search(_) => "search",
        Command::Inspect(_) => "inspect",
        Command::Pull(_) => "pull",
        Command::List => "list",
        Command::Rm(_) => "rm",
        Command::Discard(_) => "discard",
        Command::Run(_) => "run",
        Command::Chat(_) => "chat",
    }
}

fn discovery_error_message(kind: discovery::DiscoveryErrorKind) -> &'static str {
    use discovery::DiscoveryErrorKind;

    match kind {
        DiscoveryErrorKind::InvalidQuery => "invalid Hugging Face search query",
        DiscoveryErrorKind::InvalidRepository => {
            "invalid Hugging Face repository; expected owner/repo"
        }
        DiscoveryErrorKind::InvalidRevision => "invalid Hugging Face revision",
        DiscoveryErrorKind::AuthenticationRequired => "Hugging Face authentication is required",
        DiscoveryErrorKind::AccessDenied => "Hugging Face repository access was denied",
        DiscoveryErrorKind::RepositoryNotFound => "Hugging Face repository was not found",
        DiscoveryErrorKind::RevisionNotFound => "Hugging Face revision was not found",
        DiscoveryErrorKind::RateLimited => "Hugging Face rate limit exceeded; try again later",
        DiscoveryErrorKind::RemoteUnavailable => "Hugging Face is unavailable; try again later",
        DiscoveryErrorKind::DeadlineExceeded => "Hugging Face request timed out",
        DiscoveryErrorKind::RedirectRejected => "Hugging Face response was rejected: redirect",
        DiscoveryErrorKind::PaginationRejected => {
            "Hugging Face response was rejected: invalid pagination"
        }
        DiscoveryErrorKind::ResponseTooLarge => {
            "Hugging Face response was rejected: response too large"
        }
        DiscoveryErrorKind::MalformedResponse => {
            "Hugging Face response was rejected: malformed response"
        }
    }
}

fn execute_search<F>(args: cli::SearchArgs, operation: F) -> Result<String, String>
where
    F: FnOnce(
        discovery::SearchModels,
    ) -> Result<discovery::ModelSearchPage, discovery::DiscoveryError>,
{
    let page = operation(discovery::SearchModels::new(args.query))
        .map_err(|error| discovery_error_message(error.kind()).to_owned())?;
    Ok(format_search_results(&page))
}

fn format_search_results(page: &discovery::ModelSearchPage) -> String {
    use std::fmt::Write as _;

    let hits = page.hits();
    let mut output = format!("Repositories ({})\n", hits.len());
    if hits.is_empty() {
        output.push_str("No matching repositories.\n");
        return output;
    }

    for hit in hits {
        let access = match hit.gated() {
            discovery::GatedStatus::Public => "Public",
            discovery::GatedStatus::AutomaticApproval => "Automatic approval",
            discovery::GatedStatus::ManualApproval => "Manual approval",
            discovery::GatedStatus::Unknown => "Unknown",
        };
        let downloads = hit
            .downloads()
            .map_or_else(|| "Unknown".to_owned(), |downloads| downloads.to_string());
        writeln!(
            output,
            "\n{}\n  Access: {access}\n  Downloads: {downloads}\n  Inspect: loxa inspect {}",
            hit.repo(),
            hit.repo(),
        )
        .expect("writing to a String cannot fail");
    }
    output
}

fn execute_inspect<F>(args: cli::InspectArgs, operation: F) -> Result<String, String>
where
    F: FnOnce(
        discovery::InspectRepository,
    ) -> Result<discovery::RepositoryPlan, discovery::DiscoveryError>,
{
    let requested_revision = args.revision.clone();
    let plan = operation(discovery::InspectRepository::new(args.repo, args.revision))
        .map_err(|error| discovery_error_message(error.kind()).to_owned())?;
    Ok(format_repository_plan(&plan, requested_revision.as_deref()))
}

fn format_repository_plan(
    plan: &discovery::RepositoryPlan,
    requested_revision: Option<&str>,
) -> String {
    use std::fmt::Write as _;

    let candidates = plan.candidates();
    let eligible = candidates
        .iter()
        .filter(|candidate| {
            candidate.disposition()
                == discovery::CandidateDisposition::EligibleForDownloadAndLocalValidation
        })
        .count();
    let mut output = format!(
        "Repository: {}\nCommit: {}\nRuntime compatibility: Unknown (local validation not run)\nGGUF candidates ({}; {eligible} eligible)\n",
        plan.repo(),
        plan.commit(),
        candidates.len(),
    );

    for candidate in candidates {
        let size = candidate
            .size()
            .map_or_else(|| "Unknown".to_owned(), |size| format!("{size} bytes"));
        writeln!(
            output,
            "\n{}\n  Size: {size}\n  Packaging: {}",
            candidate.display_path(),
            packaging_label(candidate.disposition()),
        )
        .expect("writing to a String cannot fail");
        if let Some(identity) = candidate.identity() {
            writeln!(output, "  SHA-256: {}", identity.sha256())
                .expect("writing to a String cannot fail");
            if candidate.disposition()
                == discovery::CandidateDisposition::EligibleForDownloadAndLocalValidation
            {
                writeln!(
                    output,
                    "  Pull: {}",
                    inspection_pull_command(plan.repo(), identity.path(), requested_revision)
                )
                .expect("writing to a String cannot fail");
            }
        }
    }
    output
}

fn inspection_pull_command(repo: &str, filename: &str, requested_revision: Option<&str>) -> String {
    let revision = requested_revision
        .map(|revision| format!(" --revision={}", cli::shell_quote(revision)))
        .unwrap_or_default();
    if let Some(reference) = cli::compact_file_reference(repo, filename) {
        format!("loxa pull {}{revision}", cli::shell_quote(&reference))
    } else {
        format!(
            "loxa pull {repo} --file={}{revision}",
            cli::shell_quote(filename)
        )
    }
}

#[derive(Debug)]
enum PullAdapterError {
    Resolution(app::ResolveArtifactError),
    Interrupt,
    Transfer {
        error: app::TransferError,
        model_id: String,
        total_bytes: u64,
    },
}

fn pull_adapter_with<R, T, P>(
    args: cli::PullInput,
    resolve: R,
    transfer: T,
    mut progress: P,
) -> Result<app::TransferResult, PullAdapterError>
where
    R: FnOnce(
        app::ResolveArtifactRequest,
    ) -> Result<huggingface::ResolvedFile, app::ResolveArtifactError>,
    T: FnOnce(
        app::TransferSelected,
        app::TransferControl,
        &mut dyn FnMut(app::TransferProgress),
    ) -> Result<app::TransferResult, app::TransferError>,
    P: FnMut(&str, &str, &app::TransferProgress),
{
    let request = match (args.filename, args.quant) {
        (Some(filename), None) => {
            app::ResolveArtifactRequest::exact_file(args.repo, args.revision, filename)
        }
        (None, Some(quant)) => {
            app::ResolveArtifactRequest::unique_quant(args.repo, args.revision, quant)
        }
        _ => unreachable!("PullInput has exactly one selector"),
    };
    let artifact = resolve(request).map_err(PullAdapterError::Resolution)?;
    let control = app::TransferControl::new();
    let interrupt = ScopedTransferInterrupt::install(control.clone())
        .map_err(|()| PullAdapterError::Interrupt)?;
    let model_id = args
        .name
        .clone()
        .unwrap_or_else(|| default_id(artifact.repo(), artifact.path(), artifact.sha256()));
    let total_bytes = artifact.size();
    let artifact_path = artifact.path().to_owned();
    let selected = app::TransferSelected::new(artifact, args.name);
    let mut forward_progress = |update: app::TransferProgress| {
        progress(&artifact_path, &model_id, &update);
    };
    let result = transfer(selected, control, &mut forward_progress);
    drop(interrupt);
    result.map_err(|error| PullAdapterError::Transfer {
        error,
        model_id,
        total_bytes,
    })
}

fn ordinary_pull_command(args: &cli::PullInput) -> String {
    let mut command = format!("loxa pull {}", cli::shell_quote(&args.repo));
    if let Some(filename) = args.filename.as_deref() {
        command.push_str(&format!(" --file={}", cli::shell_quote(filename)));
    } else if let Some(quant) = args.quant.as_deref() {
        command.push_str(&format!(" --quant={}", cli::shell_quote(quant)));
    }
    if let Some(revision) = args.revision.as_deref() {
        command.push_str(&format!(" --revision={}", cli::shell_quote(revision)));
    }
    if let Some(name) = args.name.as_deref() {
        command.push_str(&format!(" --name={}", cli::shell_quote(name)));
    }
    command
}

fn recovery_command(artifact: &huggingface::ResolvedFile, model_id: &str) -> String {
    format!(
        "loxa pull {} --file={} --revision={} --name={}",
        cli::shell_quote(artifact.repo()),
        cli::shell_quote(artifact.path()),
        cli::shell_quote(artifact.commit()),
        cli::shell_quote(model_id),
    )
}

#[derive(Clone, Copy)]
struct RecoveryNotice<'a> {
    model_id: &'a str,
    artifact: &'a huggingface::ResolvedFile,
    retained_bytes: u64,
    discardable: bool,
}

fn recovery_actions(recovery: &RecoveryNotice<'_>) -> String {
    let mut output = format!(
        "Resume: {}",
        recovery_command(recovery.artifact, recovery.model_id)
    );
    if recovery.discardable {
        output.push_str(&format!(
            "\nDiscard: loxa discard {}",
            cli::shell_quote(recovery.model_id)
        ));
    } else {
        output.push_str("\nInstalled or repair evidence was retained; discard is unavailable.");
    }
    output
}

fn format_paused(recovery: &RecoveryNotice<'_>) -> String {
    format!(
        "Paused {} · {} / {} bytes retained\n{}",
        recovery.model_id,
        recovery.retained_bytes,
        recovery.artifact.size(),
        recovery_actions(recovery),
    )
}

fn format_resumable_failure(heading: &str, recovery: &RecoveryNotice<'_>) -> String {
    format!(
        "{heading}\n{} / {} bytes retained\n{}",
        recovery.retained_bytes,
        recovery.artifact.size(),
        recovery_actions(recovery),
    )
}

fn format_insufficient_disk(
    model_id: &str,
    _total_bytes: u64,
    required: u64,
    available: u64,
    recovery: Option<&RecoveryNotice<'_>>,
    ordinary_command: &str,
) -> String {
    let mut output = format!(
        concat!(
            "Not enough disk space to transfer {model_id}.\n",
            "Required available: {required} bytes\n",
            "Available now:      {available} bytes\n",
            "Shortfall:          {shortfall} bytes\n",
        ),
        model_id = model_id,
        required = required,
        available = available,
        shortfall = required.saturating_sub(available),
    );
    if let Some(recovery) = recovery {
        output.push_str(&format!(
            "{} / {} bytes retained\n{}",
            recovery.retained_bytes,
            recovery.artifact.size(),
            recovery_actions(recovery),
        ));
    } else {
        output.push_str(&format!(
            "No artifact bytes were downloaded. Free space and rerun: {ordinary_command}"
        ));
    }
    output
}

#[derive(Default)]
struct PlainProgressRenderer {
    phases: [bool; 3],
}

impl PlainProgressRenderer {
    fn line(
        &mut self,
        artifact_path: &str,
        model_id: &str,
        phase: app::TransferPhase,
        transferred: u64,
        total: u64,
    ) -> Option<String> {
        let index = match phase {
            app::TransferPhase::Transferring => 0,
            app::TransferPhase::Verifying => 1,
            app::TransferPhase::Publishing => 2,
        };
        if std::mem::replace(&mut self.phases[index], true) {
            return None;
        }
        Some(match phase {
            app::TransferPhase::Transferring => {
                format!("Downloading {artifact_path}  {transferred} / {total}")
            }
            app::TransferPhase::Verifying => format!("Verifying {artifact_path}"),
            app::TransferPhase::Publishing => format!("Publishing {model_id}"),
        })
    }
}

fn completion_output(id: &str, disposition: app::TransferDisposition) -> Option<String> {
    let status = match disposition {
        app::TransferDisposition::Installed => format!("Pulled {id}"),
        app::TransferDisposition::AlreadyInstalled => {
            format!("Verified {id} · already installed")
        }
        app::TransferDisposition::Paused | app::TransferDisposition::Interrupted => return None,
    };
    Some(format!("{status}\nRun: loxa run {id}\n"))
}

fn static_transfer_error_message(kind: app::transfer::TransferErrorKind) -> &'static str {
    use app::transfer::TransferErrorKind;

    match kind {
        TransferErrorKind::InvalidModelId => "Invalid model ID.",
        TransferErrorKind::Busy => "Another transfer is already using this model.",
        TransferErrorKind::UnsafeLocalState => {
            "Local model state is unsafe; no files were changed."
        }
        TransferErrorKind::ArtifactConflict => {
            "This model ID already refers to a different artifact."
        }
        TransferErrorKind::CapacityUnavailable => {
            "Destination disk capacity could not be determined."
        }
        TransferErrorKind::CapacityOverflow => {
            "Destination disk capacity could not be calculated safely."
        }
        TransferErrorKind::CatalogManifestTooLarge => {
            "Selected artifact metadata exceeds Loxa's 4,194,304-byte catalog limit."
        }
        TransferErrorKind::InsufficientDisk => "Insufficient disk space.",
        TransferErrorKind::Remote => "Artifact transfer failed.",
        TransferErrorKind::Integrity => "Artifact integrity verification failed.",
        TransferErrorKind::DiskExhausted => {
            "The destination ran out of disk space during transfer."
        }
        TransferErrorKind::Durability => "Artifact durability could not be confirmed.",
        TransferErrorKind::Publication => "Artifact publication failed.",
        TransferErrorKind::NoIncompleteTransfer => "No incomplete transfer exists.",
        TransferErrorKind::CompletionWon => "The artifact completed before this action.",
        TransferErrorKind::IncompleteTransferChanged => {
            "The incomplete transfer changed; rerun the command."
        }
    }
}

fn discard_error_message(error: &app::TransferError, model_id: &str) -> String {
    use app::transfer::TransferErrorKind;

    match error.kind() {
        TransferErrorKind::NoIncompleteTransfer => {
            format!(
                "No incomplete transfer exists for {}.",
                cli::shell_quote(model_id)
            )
        }
        TransferErrorKind::IncompleteTransferChanged => format!(
            "The incomplete transfer changed. Rerun: loxa discard {}",
            cli::shell_quote(model_id)
        ),
        TransferErrorKind::CompletionWon => format!(
            "The artifact is installed. Remove it separately with: loxa rm {}",
            cli::shell_quote(model_id)
        ),
        kind => static_transfer_error_message(kind).into(),
    }
}

fn recovery_from_error(error: &app::TransferError) -> Option<RecoveryNotice<'_>> {
    Some(RecoveryNotice {
        model_id: error.recovery_model_id()?,
        artifact: error.recovery_artifact()?,
        retained_bytes: error.retained_bytes()?,
        discardable: error.discardable(),
    })
}

fn format_transfer_error(
    error: &app::TransferError,
    model_id: &str,
    total_bytes: u64,
    ordinary_command: &str,
) -> String {
    let recovery = recovery_from_error(error);
    match error.kind() {
        app::transfer::TransferErrorKind::InsufficientDisk => {
            let (Some(required), Some(available)) =
                (error.required_available_bytes(), error.available_bytes())
            else {
                return static_transfer_error_message(error.kind()).into();
            };
            format_insufficient_disk(
                recovery.map_or(model_id, |facts| facts.model_id),
                total_bytes,
                required,
                available,
                recovery.as_ref(),
                ordinary_command,
            )
        }
        kind if recovery.is_some() => {
            let recovery = recovery.expect("checked above");
            format_resumable_failure(static_transfer_error_message(kind), &recovery)
        }
        kind => static_transfer_error_message(kind).into(),
    }
}

#[cfg(test)]
fn execute_pull_resolution<F>(
    args: &cli::PullInput,
    operation: F,
) -> Result<huggingface::ResolvedFile, String>
where
    F: FnOnce(
        &str,
        Option<&str>,
        Option<&str>,
        Option<&str>,
    ) -> Result<huggingface::ResolvedFile, huggingface::ResolveError>,
{
    operation(
        &args.repo,
        args.revision.as_deref(),
        args.filename.as_deref(),
        args.quant.as_deref(),
    )
    .map_err(|error| match error {
        huggingface::ResolveError::Discovery(error) => error.to_string(),
        huggingface::ResolveError::Selection(error) => {
            let revision = args
                .revision
                .as_deref()
                .map(|revision| format!(" --revision={}", cli::shell_quote(revision)))
                .unwrap_or_default();
            format!(
                "{error}\n\nInspect every eligible GGUF:\n  loxa inspect {}{revision}",
                args.repo
            )
        }
    })
}

fn print_pull_completion(id: &str, disposition: app::TransferDisposition) {
    if let Some(output) = completion_output(id, disposition) {
        anstream::print!("{output}");
    }
}

fn packaging_label(disposition: discovery::CandidateDisposition) -> &'static str {
    use discovery::{AuxiliaryRole, CandidateDisposition, UnsupportedPackagingReason};

    match disposition {
        CandidateDisposition::EligibleForDownloadAndLocalValidation => {
            "Eligible for download and local validation"
        }
        CandidateDisposition::UnsupportedPackaging(reason) => match reason {
            UnsupportedPackagingReason::UnsupportedEntryType => {
                "Unsupported (unsupported entry type)"
            }
            UnsupportedPackagingReason::UnsafePath => "Unsupported (unsafe path)",
            UnsupportedPackagingReason::NestedPath => "Unsupported (nested path)",
            UnsupportedPackagingReason::Sharded => "Unsupported (sharded)",
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mtp) => {
                "Unsupported (MTP auxiliary)"
            }
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft) => {
                "Unsupported (draft auxiliary)"
            }
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mmproj) => {
                "Unsupported (mmproj auxiliary)"
            }
            UnsupportedPackagingReason::MissingSize => "Unsupported (missing size)",
            UnsupportedPackagingReason::ZeroSize => "Unsupported (zero size)",
            UnsupportedPackagingReason::MissingLfsIdentity => "Unsupported (missing LFS identity)",
            UnsupportedPackagingReason::SizeMismatch => "Unsupported (size mismatch)",
            UnsupportedPackagingReason::InvalidLfsSha256 => "Unsupported (invalid LFS SHA-256)",
        },
    }
}

pub fn run(cli: Cli, paths: AppPaths) -> Result<i32, String> {
    run_with_recovery(cli, paths, |paths| {
        runtime::recover_stale(&paths.run)?;
        catalog::local::recover_pending(&paths.models)?;
        Ok(())
    })
}

fn run_with_recovery<F>(cli: Cli, paths: AppPaths, recovery: F) -> Result<i32, String>
where
    F: FnOnce(&AppPaths) -> Result<(), String>,
{
    cli::preflight(&cli).map_err(|error| error.to_string())?;
    if !matches!(
        &cli.command,
        Command::Search(_) | Command::Inspect(_) | Command::Discard(_)
    ) {
        recovery(&paths)?;
    }
    match cli.command {
        Command::Search(args) => {
            let service = app::AppService::from_paths(paths);
            let output = execute_search(args, |request| service.search_models(request))?;
            anstream::print!("{output}");
            Ok(0)
        }
        Command::Inspect(args) => {
            let service = app::AppService::from_paths(paths);
            let output = execute_inspect(args, |request| service.inspect_repository(request))?;
            anstream::print!("{output}");
            Ok(0)
        }
        Command::Pull(args) => {
            let args = cli::parse_pull_input(&args).map_err(|error| error.to_string())?;
            if let Some(name) = args.name.as_deref() {
                validate_id(name)?;
            }
            let ordinary_command = ordinary_pull_command(&args);
            let service = app::AppService::from_paths(paths);
            let mut renderer = PlainProgressRenderer::default();
            let result = pull_adapter_with(
                args,
                |request| service.resolve_artifact(request),
                |selected, control, progress| {
                    service.transfer_selected(selected, control, progress)
                },
                |artifact, model_id, update| {
                    if let Some(line) = renderer.line(
                        artifact,
                        model_id,
                        update.phase(),
                        update.transferred_bytes(),
                        update.total_bytes(),
                    ) {
                        anstream::eprintln!("{line}");
                    }
                },
            );
            let result = match result {
                Ok(result) => result,
                Err(PullAdapterError::Resolution(error)) => return Err(error.to_string()),
                Err(PullAdapterError::Interrupt) => {
                    return Err("failed to install transfer interrupt handler".into());
                }
                Err(PullAdapterError::Transfer {
                    error,
                    model_id,
                    total_bytes,
                }) => {
                    return Err(format_transfer_error(
                        &error,
                        &model_id,
                        total_bytes,
                        &ordinary_command,
                    ));
                }
            };
            match result.disposition() {
                app::TransferDisposition::Installed
                | app::TransferDisposition::AlreadyInstalled => {
                    print_pull_completion(result.model_id(), result.disposition());
                    Ok(0)
                }
                app::TransferDisposition::Paused => {
                    let recovery = RecoveryNotice {
                        model_id: result.model_id(),
                        artifact: result.artifact(),
                        retained_bytes: result.retained_bytes().unwrap_or(0),
                        discardable: result.discardable(),
                    };
                    anstream::eprintln!("{}", format_paused(&recovery));
                    Ok(130)
                }
                app::TransferDisposition::Interrupted => {
                    anstream::eprintln!("Interrupted before transfer began");
                    Ok(130)
                }
            }
        }
        Command::List => {
            let installed = load_installed_models(&paths)?;
            let candidates = local_candidates(&paths, &installed)?;
            let runnable = runnable_candidates(&candidates);
            let auxiliaries = auxiliary_candidates(&candidates);
            if installed.is_empty() && runnable.is_empty() {
                let muted = ui::muted();
                anstream::println!("No runnable models installed.");
                anstream::println!(
                    "{muted}Choose a GGUF with `loxa inspect <owner/repo>`, then download it with `loxa pull <owner/repo> --file <filename>`.{muted:#}"
                );
            }
            let accent = ui::accent();
            let muted = ui::muted();
            if !installed.is_empty() {
                let heading = ui::success();
                anstream::println!(
                    "{heading}Installed models{heading:#} {muted}({}){muted:#}",
                    installed.len()
                );
                for entry in installed {
                    anstream::println!(
                        "\n  {accent}{}{accent:#}  {}",
                        entry.id,
                        installed_model_size(&entry)
                    );
                    let (source, filename, revision) = entry.description();
                    if let Some(revision) = revision {
                        anstream::println!(
                            "    {muted}{source} · {filename} · {}{muted:#}",
                            &revision[..12]
                        );
                    } else {
                        anstream::println!("    {muted}Local import · {filename}{muted:#}");
                    }
                }
            }
            if !runnable.is_empty() {
                let heading = ui::accent();
                anstream::println!(
                    "\n{heading}Local GGUF candidates{heading:#} {muted}({} · unverified){muted:#}",
                    runnable.len()
                );
                for candidate in runnable {
                    anstream::println!(
                        "\n  {accent}{}{accent:#}  {}",
                        candidate.id,
                        BinaryBytes(candidate.size)
                    );
                    anstream::println!(
                        "    {muted}{} · adopt on run or chat{muted:#}",
                        candidate.filename
                    );
                }
            }
            if !auxiliaries.is_empty() {
                let heading = ui::muted();
                anstream::println!(
                    "\n{heading}Local GGUF auxiliaries{heading:#} {muted}({} · not runnable){muted:#}",
                    auxiliaries.len()
                );
                for candidate in auxiliaries {
                    anstream::println!(
                        "\n  {accent}{}{accent:#}  {}",
                        candidate.id,
                        BinaryBytes(candidate.size)
                    );
                    anstream::println!(
                        "    {muted}{} · auxiliary GGUF{muted:#}",
                        candidate.filename
                    );
                }
            }
            Ok(0)
        }
        Command::Rm(args) => {
            let installed = load_installed_models(&paths)?;
            if args.id.is_none() && installed.is_empty() {
                return Err("no models installed".into());
            }
            let stdin = std::io::stdin().is_terminal();
            let stderr = std::io::stderr().is_terminal();
            if args.id.is_none() && (!stdin || !stderr) {
                return Err(
                    "non-interactive removal requires an explicit model ID; pass `loxa rm <id> --yes`"
                        .into(),
                );
            }
            let id = match select_model("rm", args.id, &installed, stdin, stderr)? {
                ModelSelection::Selected(id) => id,
                ModelSelection::Exit(code) => return Ok(code),
            };
            let manifest = installed
                .into_iter()
                .find(|entry| entry.id == id)
                .ok_or_else(|| format!("unknown model id {id}"))?;
            if !args.yes {
                if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
                    return Err(
                        "removal confirmation requires an interactive terminal; pass --yes".into(),
                    );
                }
                let prompt = removal_prompt(&manifest);
                #[cfg(unix)]
                let interrupt = PromptInterrupt::install()?;
                let result = dialoguer::Confirm::new()
                    .with_prompt(prompt)
                    .default(false)
                    .interact_opt();
                #[cfg(not(unix))]
                let _ = dialoguer::console::Term::stderr().show_cursor();
                #[cfg(unix)]
                if interrupt.received() {
                    return Ok(130);
                }
                match result {
                    Ok(Some(true)) => {}
                    Ok(Some(false)) | Ok(None) => return Ok(0),
                    Err(dialoguer::Error::IO(error))
                        if error.kind() == std::io::ErrorKind::Interrupted =>
                    {
                        return Ok(130)
                    }
                    Err(error) => return Err(format!("removal confirmation failed: {error}")),
                }
            }
            catalog::remove_model(&paths.models, &manifest)?;
            let success = ui::success();
            anstream::println!("{success}Removed{success:#} {}", manifest.id);
            Ok(0)
        }
        Command::Discard(args) => {
            if !args.yes && (!std::io::stdin().is_terminal() || !std::io::stderr().is_terminal()) {
                return Err("non-interactive discard requires --yes".into());
            }
            let service = app::AppService::from_paths(paths);
            let requested_id = args.id;
            let candidate = service
                .prepare_discard(requested_id.clone())
                .map_err(|error| discard_error_message(&error, &requested_id))?;
            let model_id = candidate.model_id().to_owned();
            if !args.yes {
                #[cfg(unix)]
                match DiscardPromptInterrupt::install()?.confirm(&model_id)? {
                    Some(true) => {}
                    Some(false) => return Ok(0),
                    None => return Ok(130),
                }
                #[cfg(not(unix))]
                match dialoguer::Confirm::new()
                    .with_prompt(format!("Discard incomplete download {model_id}?"))
                    .default(false)
                    .interact_opt()
                {
                    Ok(Some(true)) => {}
                    Ok(Some(false)) | Ok(None) => return Ok(0),
                    Err(dialoguer::Error::IO(error))
                        if error.kind() == std::io::ErrorKind::Interrupted =>
                    {
                        return Ok(130)
                    }
                    Err(_) => return Err("discard confirmation failed".into()),
                }
            }
            service
                .discard_transfer(candidate)
                .map_err(|error| discard_error_message(&error, &model_id))?;
            anstream::println!("Discarded incomplete download {model_id}");
            Ok(0)
        }
        Command::Run(args) => {
            let installed = load_installed_models(&paths)?;
            let candidates = local_candidates(&paths, &installed)?;
            let candidates = runnable_candidates(&candidates);
            let id = match select_model_options(
                "run",
                model_options_with_candidates(args.id, &installed, &candidates)?,
                std::io::stdin().is_terminal(),
                std::io::stderr().is_terminal(),
            )? {
                ModelSelection::Selected(id) => id,
                ModelSelection::Exit(code) => return Ok(code),
            };
            let importing = candidates.iter().any(|candidate| candidate.id == id);
            let verifying = ui::spinner(if importing {
                format!("Importing and verifying {id}")
            } else {
                format!("Verifying {id}")
            });
            let runnable = resolve_runnable(id, args.runtime, &paths);
            verifying.finish_and_clear();
            let runnable = runnable?;
            if importing {
                let success = ui::success();
                anstream::println!("{success}Imported{success:#} {}", runnable.launch.id);
            }
            runner::run_launch(&runnable.launch, &paths.run)
        }
        Command::Chat(args) => {
            let max_tokens = args.max_tokens;
            let installed = load_installed_models(&paths)?;
            let candidates = local_candidates(&paths, &installed)?;
            let candidates = runnable_candidates(&candidates);
            let options = model_options_with_candidates(args.id, &installed, &candidates)?;
            ensure_interactive_chat(
                std::io::stdin().is_terminal(),
                std::io::stdout().is_terminal(),
            )?;
            let id = match select_model_options(
                "chat",
                options,
                std::io::stdin().is_terminal(),
                std::io::stderr().is_terminal(),
            )? {
                ModelSelection::Selected(id) => id,
                ModelSelection::Exit(code) => return Ok(code),
            };
            let importing = candidates.iter().any(|candidate| candidate.id == id);
            let starting = ui::spinner(if importing {
                format!("Importing and verifying {id}")
            } else {
                format!("Starting {id}")
            });
            let runnable = resolve_runnable(id, args.runtime, &paths);
            let runnable = match runnable {
                Ok(runnable) => runnable,
                Err(error) => {
                    starting.finish_and_clear();
                    return Err(error);
                }
            };
            starting.finish_and_clear();
            if importing {
                let success = ui::success();
                anstream::println!("{success}Imported{success:#} {}", runnable.launch.id);
            }
            let starting = ui::spinner(format!("Starting {}", runnable.launch.id));
            let started = runner::start_foreground(&runnable.launch, &paths.run);
            starting.finish_and_clear();
            match started? {
                runner::ForegroundStart::Ready(server) => {
                    session::run(server, &runnable.launch.id, max_tokens)
                }
                runner::ForegroundStart::Stopped(exit) => Ok(runner::report_exit(exit)),
            }
        }
    }
}

fn installed_model_size(manifest: &Manifest) -> BinaryBytes {
    BinaryBytes(manifest.total_size())
}

fn load_installed_models(paths: &AppPaths) -> Result<Vec<Manifest>, String> {
    load_installed_models_with_reconciler(paths, catalog::local::reconcile_qualified_bundle)
}

fn load_installed_models_with_reconciler<F>(
    paths: &AppPaths,
    reconcile: F,
) -> Result<Vec<Manifest>, String>
where
    F: FnOnce(&Path) -> Result<Option<Manifest>, String>,
{
    match reconcile(&paths.models) {
        Ok(Some(manifest)) => {
            tracing::info!(event = "bundle_reconciled", model_id = %manifest.id)
        }
        Ok(None) => tracing::debug!(event = "bundle_reconciliation_not_needed"),
        Err(_) => tracing::warn!(event = "bundle_reconciliation_failed"),
    }
    catalog::load_catalog(&paths.models)
}

fn removal_prompt(manifest: &Manifest) -> String {
    format!(
        "Remove {} ({})?",
        manifest.id,
        installed_model_size(manifest)
    )
}

fn model_options(requested: Option<String>, installed: &[Manifest]) -> Result<Vec<String>, String> {
    model_options_with_candidates(requested, installed, &[])
}

fn model_options_with_candidates(
    requested: Option<String>,
    installed: &[Manifest],
    candidates: &[catalog::local::Candidate],
) -> Result<Vec<String>, String> {
    if let Some(id) = requested {
        if !installed.iter().any(|model| model.id == id)
            && !candidates.iter().any(|candidate| candidate.id == id)
        {
            return Err(format!("unknown model id {id}"));
        }
        return Ok(vec![id]);
    }
    if installed.is_empty() && candidates.is_empty() {
        return Err("no models installed; choose a GGUF with `loxa inspect <owner/repo>`, then download it with `loxa pull <owner/repo> --file <filename>`".into());
    }
    let mut options = installed
        .iter()
        .map(|model| model.id.clone())
        .chain(candidates.iter().map(|candidate| candidate.id.clone()))
        .collect::<Vec<_>>();
    options.sort();
    Ok(options)
}

fn local_candidates(
    paths: &AppPaths,
    installed: &[Manifest],
) -> Result<Vec<catalog::local::Candidate>, String> {
    Ok(catalog::local::discover(&paths.models)?
        .into_iter()
        .filter(|candidate| !installed.iter().any(|model| model.id == candidate.id))
        .collect())
}

fn runnable_candidates(candidates: &[catalog::local::Candidate]) -> Vec<catalog::local::Candidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.kind == catalog::local::CandidateKind::Runnable)
        .cloned()
        .collect()
}

fn auxiliary_candidates(
    candidates: &[catalog::local::Candidate],
) -> Vec<catalog::local::Candidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.kind == catalog::local::CandidateKind::Auxiliary)
        .cloned()
        .collect()
}

#[derive(Debug, Eq, PartialEq)]
enum ModelSelection {
    Selected(String),
    Exit(i32),
}

fn select_model(
    command: &str,
    requested: Option<String>,
    installed: &[Manifest],
    stdin: bool,
    stderr: bool,
) -> Result<ModelSelection, String> {
    select_model_options(command, model_options(requested, installed)?, stdin, stderr)
}

fn select_model_options(
    command: &str,
    mut options: Vec<String>,
    stdin: bool,
    stderr: bool,
) -> Result<ModelSelection, String> {
    if options.len() == 1 {
        return Ok(ModelSelection::Selected(options.remove(0)));
    }
    if !stdin || !stderr {
        return Err(format!(
            "model selection requires an interactive terminal; pass `loxa {command} <id>`"
        ));
    }
    #[cfg(unix)]
    let interrupt = PromptInterrupt::install()?;
    let result = dialoguer::FuzzySelect::new()
        .with_prompt("Choose a model (type to filter)")
        .items(&options)
        .default(0)
        .interact_opt();
    #[cfg(not(unix))]
    let _ = dialoguer::console::Term::stderr().show_cursor();
    #[cfg(unix)]
    if interrupt.received() {
        return Ok(ModelSelection::Exit(130));
    }
    match result {
        Ok(Some(index)) => Ok(ModelSelection::Selected(options.remove(index))),
        Ok(None) => Ok(ModelSelection::Exit(0)),
        Err(dialoguer::Error::IO(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
            Ok(ModelSelection::Exit(130))
        }
        Err(error) => Err(format!("model selection failed: {error}")),
    }
}

fn ensure_interactive_chat(stdin: bool, stdout: bool) -> Result<(), String> {
    if stdin && stdout {
        Ok(())
    } else {
        Err(
            "chat requires an interactive terminal; run `loxa chat <id>` directly in a terminal"
                .into(),
        )
    }
}

fn default_id(repo: &str, filename: &str, sha256: &str) -> String {
    let stem = filename
        .strip_suffix(".gguf")
        .or_else(|| filename.strip_suffix(".GGUF"))
        .unwrap_or(filename);
    let identity = format!("{repo}-{stem}");
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
    let suffix = &sha256[..sha256.len().min(16)];
    prefix.truncate(120usize.saturating_sub(suffix.len() + 1));
    format!("{}-{suffix}", prefix.trim_end_matches('-'))
}

#[cfg(test)]
mod tests {
    use super::{
        completion_output, default_id, discovery_error_message, ensure_interactive_chat,
        execute_inspect, execute_pull_resolution, execute_search, format_insufficient_disk,
        format_paused, format_resumable_failure, installed_model_size,
        load_installed_models_with_reconciler, local_candidates, model_options,
        model_options_with_candidates, ordinary_pull_command, print_pull_completion,
        pull_adapter_with, recovery_command, removal_prompt, resolve_runnable, run,
        run_with_recovery, runnable_candidates, select_model, static_transfer_error_message,
        ModelSelection, PlainProgressRenderer, RecoveryNotice,
    };
    use crate::catalog::{
        Artifact, ArtifactProvenance, ArtifactRole, Manifest, RuntimeQualification,
        TEST_LLAMA_BUILD, TEST_MTP_PROFILE,
    };
    use crate::cli::{Cli, Command, InspectArgs, PullArgs, RuntimeArgs, SearchArgs};
    use crate::discovery::{
        ArtifactCandidate, AuxiliaryRole, CandidateDisposition, DiscoveryError, DiscoveryErrorKind,
        GatedStatus, ModelSearchHit, ModelSearchPage, RepositoryPlan, UnsupportedPackagingReason,
    };
    use crate::paths::AppPaths;
    use clap::Parser;

    fn manifest(id: &str) -> Manifest {
        Manifest {
            version: 1,
            id: id.into(),
            repo: Some("owner/repo".into()),
            revision: Some("0".repeat(40)),
            remote_filename: Some("model-Q4_K_M.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "a".repeat(64),
            size: 1,
            artifacts: None,
            profile: None,
            runtime: None,
        }
    }

    fn test_bundle(id: &str) -> Manifest {
        Manifest {
            version: 3,
            id: id.into(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size: 3,
            artifacts: Some(vec![
                Artifact {
                    role: ArtifactRole::Model,
                    local_filename: "model.gguf".into(),
                    sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                        .into(),
                    size: 3,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "model-source.gguf".into(),
                    },
                },
                Artifact {
                    role: ArtifactRole::Draft,
                    local_filename: "draft.gguf".into(),
                    sha256: "7743ce348d9284d677a185f33295b92266cc435a5b5f775029b300066d26693a"
                        .into(),
                    size: 5,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "draft-source.gguf".into(),
                    },
                },
            ]),
            profile: Some(TEST_MTP_PROFILE.into()),
            runtime: Some(RuntimeQualification {
                engine: "llama.cpp".into(),
                build: TEST_LLAMA_BUILD.into(),
            }),
        }
    }

    fn install_bundle(paths: &AppPaths, id: &str) -> Manifest {
        let manifest = test_bundle(id);
        let model_dir = paths.model_dir(id).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
        std::fs::write(model_dir.join("draft.gguf"), b"draft").unwrap();
        crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
        manifest
    }

    fn install(paths: &AppPaths, id: &str) -> Manifest {
        let manifest = Manifest {
            version: 1,
            id: id.into(),
            repo: Some("owner/repo".into()),
            revision: Some("0".repeat(40)),
            remote_filename: Some("model-Q4_K_M.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size: 3,
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let model_dir = paths.model_dir(id).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
        crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
        manifest
    }

    #[test]
    fn default_id_owns_repo_artifact_and_digest_identity() {
        let first = default_id(
            "alice/demo",
            "demo-Q4_K_M.gguf",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let other_owner = default_id(
            "bob/demo",
            "demo-Q4_K_M.gguf",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let other_file = default_id(
            "alice/demo",
            "demo-Q8_0.gguf",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );

        assert_ne!(first, other_owner);
        assert_ne!(first, other_file);
        assert!(first.starts_with("alice-demo-demo-q4-k-m-"));
        let long = default_id(
            &format!("owner/{}", "a".repeat(100)),
            &format!("{}.gguf", "b".repeat(100)),
            "0123456789abcdefaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        assert!(long.ends_with("0123456789abcdef"));
    }

    #[test]
    fn discovery_errors_map_to_exact_static_cli_messages() {
        let cases = [
            (
                DiscoveryErrorKind::InvalidQuery,
                "invalid Hugging Face search query",
            ),
            (
                DiscoveryErrorKind::InvalidRepository,
                "invalid Hugging Face repository; expected owner/repo",
            ),
            (
                DiscoveryErrorKind::InvalidRevision,
                "invalid Hugging Face revision",
            ),
            (
                DiscoveryErrorKind::AuthenticationRequired,
                "Hugging Face authentication is required",
            ),
            (
                DiscoveryErrorKind::AccessDenied,
                "Hugging Face repository access was denied",
            ),
            (
                DiscoveryErrorKind::RepositoryNotFound,
                "Hugging Face repository was not found",
            ),
            (
                DiscoveryErrorKind::RevisionNotFound,
                "Hugging Face revision was not found",
            ),
            (
                DiscoveryErrorKind::RateLimited,
                "Hugging Face rate limit exceeded; try again later",
            ),
            (
                DiscoveryErrorKind::RemoteUnavailable,
                "Hugging Face is unavailable; try again later",
            ),
            (
                DiscoveryErrorKind::DeadlineExceeded,
                "Hugging Face request timed out",
            ),
            (
                DiscoveryErrorKind::RedirectRejected,
                "Hugging Face response was rejected: redirect",
            ),
            (
                DiscoveryErrorKind::PaginationRejected,
                "Hugging Face response was rejected: invalid pagination",
            ),
            (
                DiscoveryErrorKind::ResponseTooLarge,
                "Hugging Face response was rejected: response too large",
            ),
            (
                DiscoveryErrorKind::MalformedResponse,
                "Hugging Face response was rejected: malformed response",
            ),
        ];

        for (kind, expected) in cases {
            assert_eq!(discovery_error_message(kind), expected);
        }
    }

    #[test]
    fn search_execution_forwards_exact_input_once_and_formats_empty_success() {
        let calls = std::cell::Cell::new(0);
        let raw_input = "  hf://Owner/Repo  ";

        let output = execute_search(
            SearchArgs {
                query: raw_input.into(),
            },
            |request| {
                calls.set(calls.get() + 1);
                assert_eq!(request.query(), raw_input);
                Ok(ModelSearchPage::new(Vec::new()))
            },
        )
        .unwrap();

        assert_eq!(calls.get(), 1);
        assert_eq!(output, "Repositories (0)\nNo matching repositories.\n");
    }

    #[test]
    fn search_results_include_one_neutral_inspect_command_per_hit() {
        let output = execute_search(
            SearchArgs {
                query: "gemma".into(),
            },
            |_| {
                Ok(ModelSearchPage::new(vec![
                    ModelSearchHit::new("owner/public".into(), GatedStatus::Public, Some(1200)),
                    ModelSearchHit::new(
                        "owner/automatic".into(),
                        GatedStatus::AutomaticApproval,
                        Some(7),
                    ),
                    ModelSearchHit::new(
                        "owner/manual".into(),
                        GatedStatus::ManualApproval,
                        Some(0),
                    ),
                    ModelSearchHit::new("owner/unknown".into(), GatedStatus::Unknown, None),
                ]))
            },
        )
        .unwrap();

        assert_eq!(
            output,
            concat!(
                "Repositories (4)\n",
                "\n",
                "owner/public\n",
                "  Access: Public\n",
                "  Downloads: 1200\n",
                "  Inspect: loxa inspect owner/public\n",
                "\n",
                "owner/automatic\n",
                "  Access: Automatic approval\n",
                "  Downloads: 7\n",
                "  Inspect: loxa inspect owner/automatic\n",
                "\n",
                "owner/manual\n",
                "  Access: Manual approval\n",
                "  Downloads: 0\n",
                "  Inspect: loxa inspect owner/manual\n",
                "\n",
                "owner/unknown\n",
                "  Access: Unknown\n",
                "  Downloads: Unknown\n",
                "  Inspect: loxa inspect owner/unknown\n",
            )
        );
        assert_eq!(output.matches("  Inspect: loxa inspect ").count(), 4);
        let lower = output.to_ascii_lowercase();
        for excluded in ["recommended", "best", "fits", "compatible"] {
            assert!(
                !lower.contains(excluded),
                "unexpected {excluded} in {output}"
            );
        }
    }

    #[test]
    fn search_execution_displays_a_sole_hit_with_only_neutral_inspect_guidance() {
        let output = execute_search(
            SearchArgs {
                query: "owner/sole".into(),
            },
            |_| {
                Ok(ModelSearchPage::new(vec![ModelSearchHit::new(
                    "owner/sole".into(),
                    GatedStatus::Public,
                    None,
                )]))
            },
        )
        .unwrap();

        assert_eq!(
            output,
            concat!(
                "Repositories (1)\n",
                "\n",
                "owner/sole\n",
                "  Access: Public\n",
                "  Downloads: Unknown\n",
                "  Inspect: loxa inspect owner/sole\n",
            )
        );
        for excluded in ["loxa pull", "compatible", "Compatible", "recommend", "best"] {
            assert!(
                !output.contains(excluded),
                "unexpected {excluded} in {output}"
            );
        }
    }

    #[test]
    fn search_execution_errors_never_include_raw_query_or_remote_detail() {
        let raw_query = "raw-query\nhttps://evil.example/body?token=UNIQUE_SEARCH_SECRET";
        let kinds = [
            DiscoveryErrorKind::InvalidQuery,
            DiscoveryErrorKind::InvalidRepository,
            DiscoveryErrorKind::InvalidRevision,
            DiscoveryErrorKind::AuthenticationRequired,
            DiscoveryErrorKind::AccessDenied,
            DiscoveryErrorKind::RepositoryNotFound,
            DiscoveryErrorKind::RevisionNotFound,
            DiscoveryErrorKind::RateLimited,
            DiscoveryErrorKind::RemoteUnavailable,
            DiscoveryErrorKind::DeadlineExceeded,
            DiscoveryErrorKind::RedirectRejected,
            DiscoveryErrorKind::PaginationRejected,
            DiscoveryErrorKind::ResponseTooLarge,
            DiscoveryErrorKind::MalformedResponse,
        ];

        for kind in kinds {
            let error = execute_search(
                SearchArgs {
                    query: raw_query.into(),
                },
                |_| Err(DiscoveryError::new(kind)),
            )
            .unwrap_err();

            for secret in ["raw-query", "evil.example", "body", "UNIQUE_SEARCH_SECRET"] {
                assert!(!error.contains(secret), "{kind:?}: {error}");
            }
        }
    }

    #[test]
    fn inspection_execution_forwards_repository_and_optional_revision_once() {
        for revision in [None, Some("refs/pr/7".to_owned())] {
            let calls = std::cell::Cell::new(0);
            let raw_repo = " owner/repo ";
            let output = execute_inspect(
                InspectArgs {
                    repo: raw_repo.into(),
                    revision: revision.clone(),
                },
                |request| {
                    calls.set(calls.get() + 1);
                    assert_eq!(request.repo(), raw_repo);
                    assert_eq!(request.revision(), revision.as_deref());
                    Ok(RepositoryPlan::new(
                        "owner/repo".into(),
                        "0123456789abcdef0123456789abcdef01234567".into(),
                        Vec::new(),
                    ))
                },
            )
            .unwrap();

            assert_eq!(calls.get(), 1);
            assert_eq!(
                output,
                concat!(
                    "Repository: owner/repo\n",
                    "Commit: 0123456789abcdef0123456789abcdef01234567\n",
                    "Runtime compatibility: Unknown (local validation not run)\n",
                    "GGUF candidates (0; 0 eligible)\n",
                )
            );
        }
    }

    #[test]
    fn inspection_execution_formats_one_eligible_candidate_with_full_identity() {
        let sha256 = "a".repeat(64);
        let identity = crate::huggingface::test_resolved_file(sha256.clone(), 4_512_345_678);
        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: None,
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    vec![ArtifactCandidate::new(
                        "model-Q4_K_M.gguf".into(),
                        Some(4_512_345_678),
                        Some(identity),
                        CandidateDisposition::EligibleForDownloadAndLocalValidation,
                    )],
                ))
            },
        )
        .unwrap();

        assert_eq!(
            output,
            concat!(
                "Repository: owner/repo\n",
                "Commit: 0123456789abcdef0123456789abcdef01234567\n",
                "Runtime compatibility: Unknown (local validation not run)\n",
                "GGUF candidates (1; 1 eligible)\n",
                "\n",
                "model-Q4_K_M.gguf\n",
                "  Size: 4512345678 bytes\n",
                "  Packaging: Eligible for download and local validation\n",
                "  SHA-256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
                "  Pull: loxa pull 'hf.co/owner/repo:model.gguf'\n",
            )
        );
        assert!(output.contains(&sha256));
    }

    fn eligible_candidate(repo: &str, path: &str, size: u64) -> ArtifactCandidate {
        ArtifactCandidate::new(
            path.into(),
            Some(size),
            Some(crate::huggingface::test_resolved_file_for(
                repo,
                path,
                "a".repeat(64),
                size,
            )),
            CandidateDisposition::EligibleForDownloadAndLocalValidation,
        )
    }

    #[test]
    fn inspection_execution_prints_exact_commands_in_candidate_order_without_claims() {
        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: None,
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    vec![
                        eligible_candidate("owner/repo", "first-Q4_K_M.gguf", 10),
                        ArtifactCandidate::new(
                            "unsupported.gguf".into(),
                            Some(15),
                            Some(crate::huggingface::test_resolved_file_for(
                                "owner/repo",
                                "unsupported.gguf",
                                "b".repeat(64),
                                15,
                            )),
                            CandidateDisposition::UnsupportedPackaging(
                                UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
                            ),
                        ),
                        eligible_candidate("owner/repo", "second-Q4_K_M.gguf", 20),
                    ],
                ))
            },
        )
        .unwrap();

        let first = "  Pull: loxa pull 'hf.co/owner/repo:first-Q4_K_M.gguf'";
        let second = "  Pull: loxa pull 'hf.co/owner/repo:second-Q4_K_M.gguf'";
        let first_position = output.find(first).expect("first exact pull command");
        let second_position = output.find(second).expect("second exact pull command");
        assert!(first_position < second_position, "{output}");
        assert_eq!(output.matches("  Pull:").count(), 2, "{output}");
        assert!(!output.contains("hf.co/owner/repo:unsupported.gguf"));
        assert!(
            output.contains("Runtime compatibility: Unknown (local validation not run)"),
            "{output}"
        );
        let lower = output.to_ascii_lowercase();
        for excluded in ["recommended", "best", "fits", "compatible"] {
            assert!(
                !lower.contains(excluded),
                "unexpected {excluded} in {output}"
            );
        }
    }

    #[cfg(unix)]
    fn shell_argv(command: &str, directory: &std::path::Path) -> Vec<String> {
        let script = format!("set -- {command}; printf '%s\\n' \"$@\"");
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .current_dir(directory)
            .output()
            .expect("evaluate fixture-generated command words");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stderr, b"");
        String::from_utf8(output.stdout)
            .expect("UTF-8 shell argv")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[cfg(unix)]
    #[test]
    fn inspection_execution_shell_commands_round_trip_without_evaluation() {
        let temp = tempfile::tempdir().unwrap();
        let filename = "-model ' $(touch compact-dollar) `touch compact-backtick`:tag.gguf";
        let revision = "-release ' $(touch revision-dollar) `touch revision-backtick`";
        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: Some(revision.into()),
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    vec![eligible_candidate("owner/repo", filename, 42)],
                ))
            },
        )
        .unwrap();

        let command = output
            .lines()
            .find_map(|line| line.strip_prefix("  Pull: "))
            .expect("copyable pull command");
        let argv = shell_argv(command, temp.path());
        assert_eq!(argv.len(), 4, "{argv:?}");
        assert_eq!(argv[0], "loxa");
        assert_eq!(argv[1], "pull");
        assert_eq!(argv[2], format!("hf.co/owner/repo:{filename}"));
        assert_eq!(argv[3], format!("--revision={revision}"));
        for marker in [
            "compact-dollar",
            "compact-backtick",
            "revision-dollar",
            "revision-backtick",
        ] {
            assert!(!temp.path().join(marker).exists(), "created {marker}");
        }

        let parsed = Cli::try_parse_from(argv).unwrap();
        let Command::Pull(args) = parsed.command else {
            panic!("expected pull command");
        };
        let normalized = crate::cli::parse_pull_input(&args).unwrap();
        assert_eq!(normalized.filename.as_deref(), Some(filename));
        assert_eq!(normalized.revision.as_deref(), Some(revision));
    }

    #[cfg(unix)]
    #[test]
    fn inspection_execution_compact_and_legacy_fallback_round_trip_exact_filenames() {
        let fallback = format!("{}.gguf", "x".repeat(251));
        assert_eq!(fallback.len(), 256);
        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: None,
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    vec![
                        eligible_candidate("owner/repo", "model?#.gguf", 1),
                        eligible_candidate("owner/repo", &fallback, 2),
                    ],
                ))
            },
        )
        .unwrap();

        let commands = output
            .lines()
            .filter_map(|line| line.strip_prefix("  Pull: "))
            .collect::<Vec<_>>();
        assert_eq!(commands.len(), 2, "{output}");
        assert!(commands[0].contains("'hf.co/owner/repo:model?#.gguf'"));
        assert!(commands[1].contains("owner/repo --file="));

        for (command, expected_filename) in commands.into_iter().zip(["model?#.gguf", &fallback]) {
            let directory = tempfile::tempdir().unwrap();
            let argv = shell_argv(command, directory.path());
            let parsed = Cli::try_parse_from(argv).unwrap();
            let Command::Pull(args) = parsed.command else {
                panic!("expected pull command");
            };
            let normalized = crate::cli::parse_pull_input(&args).unwrap();
            assert_eq!(normalized.filename.as_deref(), Some(expected_filename));
            assert_eq!(normalized.revision, None);
        }
    }

    #[test]
    fn inspection_execution_omits_pull_commands_without_candidate_identity() {
        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: Some("main".into()),
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "fedcba9876543210fedcba9876543210fedcba98".into(),
                    vec![
                        ArtifactCandidate::new(
                            "first.gguf".into(),
                            Some(10),
                            None,
                            CandidateDisposition::EligibleForDownloadAndLocalValidation,
                        ),
                        ArtifactCandidate::new(
                            "unsupported.gguf".into(),
                            None,
                            None,
                            CandidateDisposition::UnsupportedPackaging(
                                UnsupportedPackagingReason::MissingSize,
                            ),
                        ),
                        ArtifactCandidate::new(
                            "second.gguf".into(),
                            Some(20),
                            None,
                            CandidateDisposition::EligibleForDownloadAndLocalValidation,
                        ),
                    ],
                ))
            },
        )
        .unwrap();

        assert_eq!(
            output,
            concat!(
                "Repository: owner/repo\n",
                "Commit: fedcba9876543210fedcba9876543210fedcba98\n",
                "Runtime compatibility: Unknown (local validation not run)\n",
                "GGUF candidates (3; 2 eligible)\n",
                "\n",
                "first.gguf\n",
                "  Size: 10 bytes\n",
                "  Packaging: Eligible for download and local validation\n",
                "\n",
                "unsupported.gguf\n",
                "  Size: Unknown\n",
                "  Packaging: Unsupported (missing size)\n",
                "\n",
                "second.gguf\n",
                "  Size: 20 bytes\n",
                "  Packaging: Eligible for download and local validation\n",
            )
        );
        for excluded in [
            "loxa pull",
            "--file",
            "fits",
            "recommended",
            "best",
            "runnable",
            "engine compatible",
        ] {
            assert!(
                !output.contains(excluded),
                "unexpected {excluded} in {output}"
            );
        }
    }

    #[test]
    fn inspection_execution_maps_every_unsupported_packaging_reason_exactly() {
        let cases = [
            (
                "entry.txt",
                Some(1),
                UnsupportedPackagingReason::UnsupportedEntryType,
            ),
            (
                "../unsafe.gguf",
                Some(2),
                UnsupportedPackagingReason::UnsafePath,
            ),
            (
                "nested/model.gguf",
                Some(3),
                UnsupportedPackagingReason::NestedPath,
            ),
            (
                "model-00001-of-00002.gguf",
                Some(4),
                UnsupportedPackagingReason::Sharded,
            ),
            (
                "mtp-model.gguf",
                Some(5),
                UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mtp),
            ),
            (
                "draft-model.gguf",
                Some(6),
                UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
            ),
            (
                "model-mmproj.gguf",
                Some(7),
                UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mmproj),
            ),
            (
                "missing-size.gguf",
                None,
                UnsupportedPackagingReason::MissingSize,
            ),
            (
                "zero-size.gguf",
                Some(0),
                UnsupportedPackagingReason::ZeroSize,
            ),
            (
                "missing-lfs.gguf",
                Some(9),
                UnsupportedPackagingReason::MissingLfsIdentity,
            ),
            (
                "size-mismatch.gguf",
                Some(10),
                UnsupportedPackagingReason::SizeMismatch,
            ),
            (
                "invalid-sha.gguf",
                Some(11),
                UnsupportedPackagingReason::InvalidLfsSha256,
            ),
        ];
        let candidates = cases
            .iter()
            .map(|(path, size, reason)| {
                ArtifactCandidate::new(
                    (*path).into(),
                    *size,
                    None,
                    CandidateDisposition::UnsupportedPackaging(*reason),
                )
            })
            .collect();

        let output = execute_inspect(
            InspectArgs {
                repo: "owner/repo".into(),
                revision: None,
            },
            |_| {
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    candidates,
                ))
            },
        )
        .unwrap();

        assert_eq!(
            output,
            concat!(
                "Repository: owner/repo\n",
                "Commit: 0123456789abcdef0123456789abcdef01234567\n",
                "Runtime compatibility: Unknown (local validation not run)\n",
                "GGUF candidates (12; 0 eligible)\n",
                "\n",
                "entry.txt\n",
                "  Size: 1 bytes\n",
                "  Packaging: Unsupported (unsupported entry type)\n",
                "\n",
                "../unsafe.gguf\n",
                "  Size: 2 bytes\n",
                "  Packaging: Unsupported (unsafe path)\n",
                "\n",
                "nested/model.gguf\n",
                "  Size: 3 bytes\n",
                "  Packaging: Unsupported (nested path)\n",
                "\n",
                "model-00001-of-00002.gguf\n",
                "  Size: 4 bytes\n",
                "  Packaging: Unsupported (sharded)\n",
                "\n",
                "mtp-model.gguf\n",
                "  Size: 5 bytes\n",
                "  Packaging: Unsupported (MTP auxiliary)\n",
                "\n",
                "draft-model.gguf\n",
                "  Size: 6 bytes\n",
                "  Packaging: Unsupported (draft auxiliary)\n",
                "\n",
                "model-mmproj.gguf\n",
                "  Size: 7 bytes\n",
                "  Packaging: Unsupported (mmproj auxiliary)\n",
                "\n",
                "missing-size.gguf\n",
                "  Size: Unknown\n",
                "  Packaging: Unsupported (missing size)\n",
                "\n",
                "zero-size.gguf\n",
                "  Size: 0 bytes\n",
                "  Packaging: Unsupported (zero size)\n",
                "\n",
                "missing-lfs.gguf\n",
                "  Size: 9 bytes\n",
                "  Packaging: Unsupported (missing LFS identity)\n",
                "\n",
                "size-mismatch.gguf\n",
                "  Size: 10 bytes\n",
                "  Packaging: Unsupported (size mismatch)\n",
                "\n",
                "invalid-sha.gguf\n",
                "  Size: 11 bytes\n",
                "  Packaging: Unsupported (invalid LFS SHA-256)\n",
            )
        );
    }

    #[test]
    fn inspection_execution_errors_never_include_raw_repository_revision_or_remote_detail() {
        let raw_repo = "raw-repo\nhttps://evil.example/body";
        let raw_revision = "raw-revision?token=UNIQUE_INSPECT_SECRET";
        let kinds = [
            DiscoveryErrorKind::InvalidQuery,
            DiscoveryErrorKind::InvalidRepository,
            DiscoveryErrorKind::InvalidRevision,
            DiscoveryErrorKind::AuthenticationRequired,
            DiscoveryErrorKind::AccessDenied,
            DiscoveryErrorKind::RepositoryNotFound,
            DiscoveryErrorKind::RevisionNotFound,
            DiscoveryErrorKind::RateLimited,
            DiscoveryErrorKind::RemoteUnavailable,
            DiscoveryErrorKind::DeadlineExceeded,
            DiscoveryErrorKind::RedirectRejected,
            DiscoveryErrorKind::PaginationRejected,
            DiscoveryErrorKind::ResponseTooLarge,
            DiscoveryErrorKind::MalformedResponse,
        ];

        for kind in kinds {
            let error = execute_inspect(
                InspectArgs {
                    repo: raw_repo.into(),
                    revision: Some(raw_revision.into()),
                },
                |_| Err(DiscoveryError::new(kind)),
            )
            .unwrap_err();

            for secret in [
                "raw-repo",
                "raw-revision",
                "evil.example",
                "body",
                "UNIQUE_INSPECT_SECRET",
            ] {
                assert!(!error.contains(secret), "{kind:?}: {error}");
            }
        }
    }

    #[test]
    fn discovery_commands_bypass_recovery_and_legacy_commands_recover() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();

        for cli in [
            Cli::parse_from(["loxa", "search", "\n"]),
            Cli::parse_from(["loxa", "inspect", "invalid/repo/shape"]),
        ] {
            let calls = std::cell::Cell::new(0);
            let result = run_with_recovery(cli, paths.clone(), |_| {
                calls.set(calls.get() + 1);
                Err("unexpected recovery".into())
            });

            assert!(result.is_err());
            assert_eq!(calls.get(), 0);
        }

        let calls = std::cell::Cell::new(0);
        let error = run_with_recovery(Cli::parse_from(["loxa", "list"]), paths, |_| {
            calls.set(calls.get() + 1);
            Err("injected combined recovery stop".into())
        })
        .unwrap_err();

        assert_eq!(error, "injected combined recovery stop");
        assert_eq!(calls.get(), 1);
    }

    fn pull_cli(
        repo: &str,
        revision: Option<&str>,
        filename: Option<&str>,
        quant: Option<&str>,
    ) -> Cli {
        Cli {
            command: Command::Pull(PullArgs {
                repo: repo.into(),
                revision: revision.map(str::to_owned),
                filename: filename.map(str::to_owned),
                quant: quant.map(str::to_owned),
                name: None,
            }),
        }
    }

    #[test]
    fn pull_adapter_resolves_once_then_transfers_the_exact_returned_value_once() {
        let root = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let service = crate::app::AppService::from_paths(paths.clone());
        let artifact = crate::huggingface::test_resolved_file_for(
            "owner/repo",
            "exact.gguf",
            "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
            6,
        );
        let installed = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some(artifact.repo().into()),
            revision: Some(artifact.commit().into()),
            remote_filename: Some(artifact.path().into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: artifact.sha256().into(),
            size: artifact.size(),
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let model_dir = paths.model_dir("demo").unwrap();
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        drop(lock);
        std::fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();

        let calls = std::cell::RefCell::new(Vec::new());
        let result = pull_adapter_with(
            crate::cli::parse_pull_input(&PullArgs {
                repo: "owner/repo".into(),
                revision: Some("moving-branch".into()),
                filename: Some("exact.gguf".into()),
                quant: None,
                name: Some("demo".into()),
            })
            .unwrap(),
            |_| {
                calls.borrow_mut().push("resolve");
                Ok(artifact.clone())
            },
            |selected, control, progress| {
                calls.borrow_mut().push("transfer");
                service.transfer_selected(selected, control, progress)
            },
            |_, _, _| {},
        )
        .unwrap();

        assert_eq!(&*calls.borrow(), &["resolve", "transfer"]);
        assert_eq!(result.artifact(), &artifact);
        assert_eq!(result.model_id(), "demo");
    }

    #[cfg(unix)]
    fn kill_and_reap_sigint_child(child: &mut std::process::Child) -> String {
        let kill = child.kill();
        let wait = child.wait();
        format!("kill={kill:?}; wait={wait:?}")
    }

    #[cfg(unix)]
    fn wait_for_sigint_path(path: &std::path::Path, child: &mut std::process::Child) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !path.exists() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    use std::io::Read as _;

                    let mut stdout = String::new();
                    let mut stderr = String::new();
                    child
                        .stdout
                        .take()
                        .unwrap()
                        .read_to_string(&mut stdout)
                        .unwrap();
                    child
                        .stderr
                        .take()
                        .unwrap()
                        .read_to_string(&mut stderr)
                        .unwrap();
                    panic!(
                        "SIGINT child exited before handshake: {status}; stdout={stdout:?}; stderr={stderr:?}"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    let cleanup = kill_and_reap_sigint_child(child);
                    panic!("SIGINT child handshake poll failed: {error}; {cleanup}");
                }
            }
            if std::time::Instant::now() >= deadline {
                let cleanup = kill_and_reap_sigint_child(child);
                panic!("SIGINT child handshake timed out; {cleanup}");
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[cfg(unix)]
    fn wait_for_sigint_child_with<P>(
        mut child: std::process::Child,
        mut poll: P,
    ) -> std::process::Output
    where
        P: FnMut(&mut std::process::Child) -> std::io::Result<Option<std::process::ExitStatus>>,
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match poll(&mut child) {
                Ok(Some(_)) => return child.wait_with_output().unwrap(),
                Ok(None) => {}
                Err(error) => {
                    let cleanup = kill_and_reap_sigint_child(&mut child);
                    panic!("SIGINT child poll failed: {error}; {cleanup}");
                }
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!("SIGINT child timed out and was killed: {output:?}");
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[cfg(unix)]
    fn wait_for_sigint_child(child: std::process::Child) -> std::process::Output {
        wait_for_sigint_child_with(child, std::process::Child::try_wait)
    }

    #[cfg(unix)]
    fn run_sigint_child(scenario: &str) -> (tempfile::TempDir, std::process::Output) {
        let root = tempfile::tempdir().unwrap();
        let scenario_path = root.path().join("scenario");
        let handshake = root.path().join("handshake");
        let release = root.path().join("release");
        std::fs::write(&scenario_path, scenario).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::selected_transfer_sigint_child_helper",
                "--nocapture",
            ])
            .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
            .env("LOXA_SIGINT_HANDSHAKE", &handshake)
            .env("LOXA_SIGINT_RELEASE", &release)
            .env("LOXA_HOME", root.path().join("loxa-home"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();

        wait_for_sigint_path(&handshake, &mut child);
        let signal_count = if scenario == "repeated" { 3 } else { 1 };
        for _ in 0..signal_count {
            assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
        }
        std::fs::write(release, b"continue").unwrap();
        let output = wait_for_sigint_child(child);
        (root, output)
    }

    #[cfg(unix)]
    #[test]
    fn sigint_handshake_timeout_kills_and_reaps_child() {
        let root = tempfile::tempdir().unwrap();
        let scenario_path = root.path().join("scenario");
        let handshake = root.path().join("never-created-handshake");
        std::fs::write(&scenario_path, "handshake-timeout").unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::selected_transfer_sigint_child_helper",
                "--nocapture",
            ])
            .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wait_for_sigint_path(&handshake, &mut child);
        }));
        assert!(result.is_err());
        let reaped = child.try_wait().is_ok_and(|status| status.is_some());
        if !reaped {
            let _ = child.kill();
            let _ = child.wait();
        }
        assert!(reaped, "handshake timeout left its child running");
    }

    #[cfg(unix)]
    #[test]
    fn sigint_child_poll_error_kills_and_reaps_child() {
        let root = tempfile::tempdir().unwrap();
        let scenario_path = root.path().join("scenario");
        std::fs::write(&scenario_path, "handshake-timeout").unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::selected_transfer_sigint_child_helper",
                "--nocapture",
            ])
            .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id() as libc::pid_t;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wait_for_sigint_child_with(child, |_| {
                Err(std::io::Error::other("injected child poll failure"))
            });
        }));
        assert!(result.is_err());
        let still_running = unsafe { libc::kill(pid, 0) } == 0;
        if still_running {
            assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
            let mut status = 0;
            assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        }
        assert!(!still_running, "poll error left its child running");
    }

    #[cfg(unix)]
    #[test]
    fn selected_transfer_sigint_child_helper() {
        let Some(scenario_path) = std::env::var_os("LOXA_SIGINT_SCENARIO_PATH") else {
            return;
        };
        let scenario = std::fs::read_to_string(scenario_path).unwrap();
        if scenario == "handshake-timeout" {
            loop {
                std::thread::park();
            }
        }
        let handshake = std::path::PathBuf::from(
            std::env::var_os("LOXA_SIGINT_HANDSHAKE").expect("handshake path"),
        );
        let release = std::path::PathBuf::from(
            std::env::var_os("LOXA_SIGINT_RELEASE").expect("release path"),
        );
        let paths = AppPaths::from_values(
            std::env::var_os("LOXA_HOME")
                .as_deref()
                .map(std::path::Path::new),
            None,
        )
        .unwrap();
        let service = crate::app::AppService::from_paths(paths.clone());
        let artifact = crate::huggingface::test_resolved_file_for(
            "owner/repo",
            "exact.gguf",
            "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
            6,
        );
        if scenario == "old-control" {
            let manifest = Manifest {
                version: 1,
                id: "demo".into(),
                repo: Some(artifact.repo().into()),
                revision: Some(artifact.commit().into()),
                remote_filename: Some(artifact.path().into()),
                origin: None,
                source_filename: None,
                local_filename: "model.gguf".into(),
                sha256: artifact.sha256().into(),
                size: artifact.size(),
                artifacts: None,
                profile: None,
                runtime: None,
            };
            let model_dir = paths.model_dir("demo").unwrap();
            let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
            drop(lock);
            std::fs::write(
                model_dir.join("manifest.json"),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();
            std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();

            let old_control = crate::app::TransferControl::new();
            drop(super::ScopedTransferInterrupt::install(old_control.clone()).unwrap());
            let prompt = super::PromptInterrupt::install().unwrap();
            std::fs::write(&handshake, b"ready").unwrap();
            while !release.exists() {
                std::thread::yield_now();
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !prompt.received() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "SIGINT was not observed"
                );
                std::thread::yield_now();
            }
            drop(prompt);
            let result = service
                .transfer_selected(
                    crate::app::TransferSelected::new(artifact, Some("demo".into())),
                    old_control,
                    |_| {},
                )
                .unwrap();
            assert_eq!(
                result.disposition(),
                crate::app::TransferDisposition::AlreadyInstalled
            );
            return;
        }
        if matches!(scenario.as_str(), "verification" | "repeated" | "late") {
            let model_dir = paths.model_dir("demo").unwrap();
            let manifest = Manifest {
                version: 1,
                id: "demo".into(),
                repo: Some(artifact.repo().into()),
                revision: Some(artifact.commit().into()),
                remote_filename: Some(artifact.path().into()),
                origin: None,
                source_filename: None,
                local_filename: "model.gguf".into(),
                sha256: artifact.sha256().into(),
                size: artifact.size(),
                artifacts: None,
                profile: None,
                runtime: None,
            };
            crate::catalog::prepare_pull(&model_dir, &manifest).unwrap();
            std::fs::write(model_dir.join("model.gguf.part"), b"abcdef").unwrap();
        }
        let result = pull_adapter_with(
            crate::cli::parse_pull_input(&PullArgs {
                repo: "owner/repo".into(),
                revision: Some("main".into()),
                filename: Some("exact.gguf".into()),
                quant: None,
                name: Some("demo".into()),
            })
            .unwrap(),
            |_| Ok(artifact),
            |selected, control, progress| {
                if scenario == "before" {
                    std::fs::write(&handshake, b"ready").unwrap();
                    while !release.exists() {
                        std::thread::yield_now();
                    }
                    return service.transfer_selected(selected, control, progress);
                }
                let handshake_phase = if scenario == "late" {
                    crate::app::TransferPhase::Publishing
                } else {
                    crate::app::TransferPhase::Verifying
                };
                service.transfer_selected(selected, control, |update| {
                    if update.phase() == handshake_phase && !handshake.exists() {
                        std::fs::write(&handshake, b"ready").unwrap();
                        while !release.exists() {
                            std::thread::yield_now();
                        }
                    }
                    progress(update);
                })
            },
            |_, _, _| {},
        )
        .unwrap();
        match scenario.as_str() {
            "before" => {
                assert_eq!(
                    result.disposition(),
                    crate::app::TransferDisposition::Interrupted
                );
                assert_eq!(result.retained_bytes(), None);
                eprintln!("Interrupted before transfer began");
                std::process::exit(130);
            }
            "verification" | "repeated" => {
                assert_eq!(
                    result.disposition(),
                    crate::app::TransferDisposition::Paused
                );
                assert_eq!(result.retained_bytes(), Some(6));
                let recovery = RecoveryNotice {
                    model_id: result.model_id(),
                    artifact: result.artifact(),
                    retained_bytes: 6,
                    discardable: result.discardable(),
                };
                eprintln!("{}", format_paused(&recovery));
                std::process::exit(130);
            }
            "late" => {
                assert_eq!(
                    result.disposition(),
                    crate::app::TransferDisposition::Installed
                );
                print_pull_completion(result.model_id(), result.disposition());
            }
            _ => panic!("unknown SIGINT scenario {scenario:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn sigint_before_mutable_transfer_exits_130_without_retained_claim() {
        use std::os::unix::process::ExitStatusExt as _;

        let (_root, output) = run_sigint_child("before");
        assert_eq!(output.status.code(), Some(130), "{output:?}");
        assert_eq!(output.status.signal(), None, "{output:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("Interrupted before transfer began"),
            "{stderr}"
        );
        assert!(!stderr.contains("bytes retained"), "{stderr}");
    }

    #[cfg(unix)]
    #[test]
    fn sigint_during_verification_exits_130_only_after_durable_pause_barrier() {
        let (root, output) = run_sigint_child("verification");
        assert_eq!(output.status.code(), Some(130), "{output:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("Paused demo · 6 / 6 bytes retained"),
            "{stderr}"
        );
        let model_dir = root.path().join("loxa-home/models/demo");
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
            b"abcdef"
        );
        assert!(!model_dir.join("model.gguf").exists());
        assert!(model_dir.join("pending.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn repeated_sigint_is_idempotent_and_cannot_bypass_sync_or_join() {
        let (root, output) = run_sigint_child("repeated");
        assert_eq!(output.status.code(), Some(130), "{output:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(
            stderr.matches("Paused demo · 6 / 6 bytes retained").count(),
            1
        );
        let model_dir = root.path().join("loxa-home/models/demo");
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
            b"abcdef"
        );
        assert!(!model_dir.join("model.gguf").exists());
    }

    #[cfg(unix)]
    #[test]
    fn sigint_after_completion_fence_allows_normal_installed_completion() {
        let (root, output) = run_sigint_child("late");
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.contains("Pulled demo\nRun: loxa run demo"),
            "{stdout}"
        );
        let model_dir = root.path().join("loxa-home/models/demo");
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf")).unwrap(),
            b"abcdef"
        );
        assert!(!model_dir.join("model.gguf.part").exists());
        assert!(model_dir.join("manifest.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn scoped_pause_listener_closes_and_joins_on_success_error_pause_and_panic_unwind() {
        fn artifact() -> crate::huggingface::ResolvedFile {
            crate::huggingface::test_resolved_file_for(
                "owner/repo",
                "exact.gguf",
                "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
                6,
            )
        }
        fn input() -> crate::cli::PullInput {
            crate::cli::parse_pull_input(&PullArgs {
                repo: "owner/repo".into(),
                revision: Some("main".into()),
                filename: Some("exact.gguf".into()),
                quant: None,
                name: Some("demo".into()),
            })
            .unwrap()
        }

        let success_root = tempfile::tempdir().unwrap();
        let success_paths = AppPaths::from_values(Some(success_root.path()), None).unwrap();
        let success_artifact = artifact();
        let success_manifest = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some(success_artifact.repo().into()),
            revision: Some(success_artifact.commit().into()),
            remote_filename: Some(success_artifact.path().into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: success_artifact.sha256().into(),
            size: success_artifact.size(),
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let success_dir = success_paths.model_dir("demo").unwrap();
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&success_dir).unwrap();
        drop(lock);
        std::fs::write(
            success_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&success_manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(success_dir.join("model.gguf"), b"abcdef").unwrap();
        let success_service = crate::app::AppService::from_paths(success_paths);
        assert_eq!(
            pull_adapter_with(
                input(),
                |_| Ok(success_artifact),
                |selected, control, progress| {
                    success_service.transfer_selected(selected, control, progress)
                },
                |_, _, _| {},
            )
            .unwrap()
            .disposition(),
            crate::app::TransferDisposition::AlreadyInstalled
        );

        let error_root = tempfile::tempdir().unwrap();
        let error_paths = AppPaths::from_values(Some(error_root.path()), None).unwrap();
        let error_dir = error_paths.model_dir("demo").unwrap();
        let _busy_lock = crate::catalog::ModelLock::acquire_for_transfer(&error_dir).unwrap();
        let error_service = crate::app::AppService::from_paths(error_paths);
        let error = match pull_adapter_with(
            input(),
            |_| Ok(artifact()),
            |selected, control, progress| {
                error_service.transfer_selected(selected, control, progress)
            },
            |_, _, _| {},
        ) {
            Ok(_) => panic!("busy transfer unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            super::PullAdapterError::Transfer { error, .. }
                if error.kind() == crate::app::transfer::TransferErrorKind::Busy
        ));

        let pause_root = tempfile::tempdir().unwrap();
        let pause_paths = AppPaths::from_values(Some(pause_root.path()), None).unwrap();
        let pause_artifact = artifact();
        let pause_manifest = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some(pause_artifact.repo().into()),
            revision: Some(pause_artifact.commit().into()),
            remote_filename: Some(pause_artifact.path().into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: pause_artifact.sha256().into(),
            size: pause_artifact.size(),
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let pause_dir = pause_paths.model_dir("demo").unwrap();
        crate::catalog::prepare_pull(&pause_dir, &pause_manifest).unwrap();
        std::fs::write(pause_dir.join("model.gguf.part"), b"abcdef").unwrap();
        let pause_service = crate::app::AppService::from_paths(pause_paths);
        let paused = pull_adapter_with(
            input(),
            |_| Ok(pause_artifact),
            |selected, control, progress| {
                let pause = control.clone();
                pause_service.transfer_selected(selected, control, |update| {
                    if update.phase() == crate::app::TransferPhase::Verifying {
                        pause.request_pause();
                    }
                    progress(update);
                })
            },
            |_, _, _| {},
        )
        .unwrap();
        assert_eq!(
            paused.disposition(),
            crate::app::TransferDisposition::Paused
        );

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pull_adapter_with(
                input(),
                |_| Ok(artifact()),
                |_, _, _| -> Result<crate::app::TransferResult, crate::app::TransferError> {
                    panic!("injected transfer panic")
                },
                |_, _, _| {},
            );
        }));
        assert!(panic.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn old_signal_control_cannot_pause_a_later_activation() {
        let (_root, output) = run_sigint_child("old-control");
        assert_eq!(output.status.code(), Some(0), "{output:?}");
    }

    #[test]
    fn ordinary_pull_and_clean_disk_retry_preserve_the_users_optional_revision() {
        let input = crate::cli::parse_pull_input(&PullArgs {
            repo: "owner/repo".into(),
            revision: Some("moving branch".into()),
            filename: Some("exact.gguf".into()),
            quant: None,
            name: Some("demo".into()),
        })
        .unwrap();
        let command = ordinary_pull_command(&input);
        assert_eq!(
            command,
            "loxa pull 'owner/repo' --file='exact.gguf' --revision='moving branch' --name='demo'"
        );
        assert_eq!(
            format_insufficient_disk("demo", 6, 100, 40, None, &command),
            concat!(
                "Not enough disk space to transfer demo.\n",
                "Required available: 100 bytes\n",
                "Available now:      40 bytes\n",
                "Shortfall:          60 bytes\n",
                "No artifact bytes were downloaded. Free space and rerun: ",
                "loxa pull 'owner/repo' --file='exact.gguf' --revision='moving branch' --name='demo'",
            )
        );
    }

    #[test]
    fn pause_and_resumable_error_commands_pin_full_commit_exact_file_and_same_id() {
        let artifact = crate::huggingface::test_resolved_file_for(
            "owner/repo",
            "exact.gguf",
            "a".repeat(64),
            6,
        );
        let recovery = RecoveryNotice {
            model_id: "demo",
            artifact: &artifact,
            retained_bytes: 3,
            discardable: true,
        };
        assert_eq!(
            format_paused(&recovery),
            concat!(
                "Paused demo · 3 / 6 bytes retained\n",
                "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
                "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
                "Discard: loxa discard 'demo'",
            )
        );

        let zero_prefix = RecoveryNotice {
            retained_bytes: 0,
            ..recovery
        };
        assert_eq!(
            format_resumable_failure(
                static_transfer_error_message(crate::app::transfer::TransferErrorKind::Remote),
                &zero_prefix,
            ),
            concat!(
                "Artifact transfer failed.\n",
                "0 / 6 bytes retained\n",
                "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
                "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
                "Discard: loxa discard 'demo'",
            )
        );
    }

    #[cfg(unix)]
    #[test]
    fn recovery_commands_shell_round_trip_hostile_complete_words_without_evaluation() {
        let temp = tempfile::tempdir().unwrap();
        let hostile = "-model ' $(touch recovery-dollar) `touch recovery-backtick`.gguf";
        let artifact =
            crate::huggingface::test_resolved_file_for("owner/repo", hostile, "a".repeat(64), 6);
        let command = recovery_command(&artifact, "demo");
        let argv = shell_argv(&command, temp.path());
        assert_eq!(
            argv,
            [
                "loxa",
                "pull",
                "owner/repo",
                "--file=-model ' $(touch recovery-dollar) `touch recovery-backtick`.gguf",
                "--revision=0123456789abcdef0123456789abcdef01234567",
                "--name=demo",
            ]
        );
        assert!(!temp.path().join("recovery-dollar").exists());
        assert!(!temp.path().join("recovery-backtick").exists());
    }

    #[test]
    fn insufficient_disk_prints_exact_required_available_shortfall_and_no_reservation_claim() {
        let output = format_insufficient_disk(
            "demo",
            6,
            8192,
            4096,
            None,
            "loxa pull 'owner/repo' --quant='Q4_K_M'",
        );
        assert_eq!(
            output,
            concat!(
                "Not enough disk space to transfer demo.\n",
                "Required available: 8192 bytes\n",
                "Available now:      4096 bytes\n",
                "Shortfall:          4096 bytes\n",
                "No artifact bytes were downloaded. Free space and rerun: ",
                "loxa pull 'owner/repo' --quant='Q4_K_M'",
            )
        );
        for excluded in ["reserved", "compatible", "fits", "will run"] {
            assert!(!output.to_ascii_lowercase().contains(excluded), "{output}");
        }
    }

    #[test]
    fn resume_disk_error_prints_exact_retained_bytes_and_only_safe_discard_guidance() {
        let artifact = crate::huggingface::test_resolved_file_for(
            "owner/repo",
            "exact.gguf",
            "a".repeat(64),
            6,
        );
        let safe = RecoveryNotice {
            model_id: "demo",
            artifact: &artifact,
            retained_bytes: 3,
            discardable: true,
        };
        assert_eq!(
            format_resumable_failure("Disk space was exhausted while transferring demo.", &safe),
            concat!(
                "Disk space was exhausted while transferring demo.\n",
                "3 / 6 bytes retained\n",
                "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
                "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
                "Discard: loxa discard 'demo'",
            )
        );

        let repair = RecoveryNotice {
            discardable: false,
            ..safe
        };
        let repair_output =
            format_resumable_failure("Disk space was exhausted while transferring demo.", &repair);
        assert!(!repair_output.contains("loxa discard"), "{repair_output}");
        assert!(
            repair_output
                .ends_with("Installed or repair evidence was retained; discard is unavailable."),
            "{repair_output}"
        );
    }

    #[test]
    fn non_tty_progress_emits_at_most_one_plain_line_per_phase_without_ansi_or_cursor_controls() {
        let mut renderer = PlainProgressRenderer::default();
        let updates = [
            (crate::app::TransferPhase::Transferring, 1, 6),
            (crate::app::TransferPhase::Transferring, 4, 6),
            (crate::app::TransferPhase::Verifying, 6, 6),
            (crate::app::TransferPhase::Verifying, 6, 6),
            (crate::app::TransferPhase::Publishing, 6, 6),
            (crate::app::TransferPhase::Publishing, 6, 6),
        ];
        let output = updates
            .into_iter()
            .filter_map(|(phase, transferred, total)| {
                renderer.line("exact.gguf", "demo", phase, transferred, total)
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            output,
            "Downloading exact.gguf  1 / 6\nVerifying exact.gguf\nPublishing demo"
        );
        for control in ['\u{1b}', '\r', '\u{8}'] {
            assert!(!output.contains(control), "{output:?}");
        }
    }

    #[test]
    fn completion_output_preserves_pulled_verified_and_optional_run_guidance_without_compatibility_claims(
    ) {
        for (disposition, expected) in [
            (
                crate::app::TransferDisposition::Installed,
                "Pulled demo\nRun: loxa run demo\n",
            ),
            (
                crate::app::TransferDisposition::AlreadyInstalled,
                "Verified demo · already installed\nRun: loxa run demo\n",
            ),
        ] {
            let output = completion_output("demo", disposition).unwrap();
            assert_eq!(output, expected);
            let words = output
                .split(|character: char| !character.is_ascii_alphanumeric())
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>();
            for excluded in ["compatible", "supported", "ready", "recommended", "fits"] {
                assert!(!words.iter().any(|word| word == excluded), "{output}");
            }
        }
    }

    #[test]
    fn transfer_error_mapping_is_static_exhaustive_and_redacted() {
        use crate::app::transfer::TransferErrorKind;

        let cases = [
            (TransferErrorKind::InvalidModelId, "Invalid model ID."),
            (
                TransferErrorKind::Busy,
                "Another transfer is already using this model.",
            ),
            (
                TransferErrorKind::UnsafeLocalState,
                "Local model state is unsafe; no files were changed.",
            ),
            (
                TransferErrorKind::ArtifactConflict,
                "This model ID already refers to a different artifact.",
            ),
            (
                TransferErrorKind::CapacityUnavailable,
                "Destination disk capacity could not be determined.",
            ),
            (
                TransferErrorKind::CapacityOverflow,
                "Destination disk capacity could not be calculated safely.",
            ),
            (
                TransferErrorKind::CatalogManifestTooLarge,
                "Selected artifact metadata exceeds Loxa's 4,194,304-byte catalog limit.",
            ),
            (
                TransferErrorKind::InsufficientDisk,
                "Insufficient disk space.",
            ),
            (TransferErrorKind::Remote, "Artifact transfer failed."),
            (
                TransferErrorKind::Integrity,
                "Artifact integrity verification failed.",
            ),
            (
                TransferErrorKind::DiskExhausted,
                "The destination ran out of disk space during transfer.",
            ),
            (
                TransferErrorKind::Durability,
                "Artifact durability could not be confirmed.",
            ),
            (
                TransferErrorKind::Publication,
                "Artifact publication failed.",
            ),
            (
                TransferErrorKind::NoIncompleteTransfer,
                "No incomplete transfer exists.",
            ),
            (
                TransferErrorKind::CompletionWon,
                "The artifact completed before this action.",
            ),
            (
                TransferErrorKind::IncompleteTransferChanged,
                "The incomplete transfer changed; rerun the command.",
            ),
        ];
        for (kind, expected) in cases {
            let output = static_transfer_error_message(kind);
            assert_eq!(output, expected);
            for secret in [
                "https://evil.invalid",
                "HF_TOKEN",
                "/private/model",
                "\u{1b}",
            ] {
                assert!(!output.contains(secret));
            }
        }
    }

    #[test]
    fn catalog_manifest_too_large_maps_to_the_exact_static_cli_text() {
        assert_eq!(
            static_transfer_error_message(
                crate::app::transfer::TransferErrorKind::CatalogManifestTooLarge
            ),
            "Selected artifact metadata exceeds Loxa's 4,194,304-byte catalog limit."
        );
    }

    #[test]
    fn pull_preflight_rejects_missing_selection_before_recovery_with_actionable_revision() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let calls = std::cell::Cell::new(0);

        let error = run_with_recovery(
            pull_cli("owner/repo", Some("release candidate"), None, None),
            paths,
            |_| {
                calls.set(calls.get() + 1);
                Err("injected recovery must not run".into())
            },
        )
        .unwrap_err();

        assert_eq!(calls.get(), 0);
        for expected in [
            "no GGUF was selected for owner/repo",
            "loxa inspect owner/repo --revision='release candidate'",
            "loxa pull owner/repo --file <FILENAME> --revision='release candidate'",
            "loxa pull owner/repo --quant <QUANT> --revision='release candidate'",
            "loxa pull hf.co/owner/repo:<FILENAME-or-QUANT> --revision='release candidate'",
        ] {
            assert!(error.contains(expected), "missing {expected:?} in {error}");
        }
    }

    #[test]
    fn pull_preflight_rejects_conflicting_and_malformed_inputs_before_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let cases = [
            pull_cli("owner/repo", None, Some("model.gguf"), Some("Q4_K_M")),
            pull_cli("hf.co/owner/repo:Q4_K_M", None, Some("model.gguf"), None),
            pull_cli("hf.co/owner/repo:", None, None, None),
            pull_cli("owner/extra/repo", None, Some("model.gguf"), None),
            pull_cli(
                "owner/repo",
                Some("release\u{202e}UNSAFE"),
                Some("model.gguf"),
                None,
            ),
            pull_cli("owner/\u{202e}repo\nUNSAFE", None, Some("model.gguf"), None),
        ];

        for cli in cases {
            let calls = std::cell::Cell::new(0);
            let error = run_with_recovery(cli, paths.clone(), |_| {
                calls.set(calls.get() + 1);
                Err("injected recovery must not run".into())
            })
            .unwrap_err();

            assert_eq!(calls.get(), 0, "{error}");
            assert!(!error.contains("UNSAFE"), "{error:?}");
            assert!(!error.contains('\u{202e}'), "{error:?}");
            assert!(!error.contains("injected recovery"), "{error:?}");
        }
    }

    fn normalized_pull_for_selection(
        filename: Option<&str>,
        quant: Option<&str>,
        revision: Option<&str>,
    ) -> crate::cli::PullInput {
        crate::cli::parse_pull_input(&PullArgs {
            repo: "owner/repo".into(),
            revision: revision.map(str::to_owned),
            filename: filename.map(str::to_owned),
            quant: quant.map(str::to_owned),
            name: None,
        })
        .unwrap()
    }

    #[test]
    fn missing_quant_selection_error_preserves_labels_and_appends_inspect() {
        let args = normalized_pull_for_selection(None, Some("NOT_A_QUANT"), None);
        let error = execute_pull_resolution(&args, |repo, revision, filename, quant| {
            assert_eq!(repo, "owner/repo");
            assert_eq!(revision, None);
            assert_eq!(filename, None);
            assert_eq!(quant, Some("NOT_A_QUANT"));
            Err(crate::huggingface::ResolveError::Selection(
                crate::huggingface::SelectionError::QuantUnavailable {
                    requested: "NOT_A_QUANT".into(),
                    available: vec!["Q4_K_M".into(), "Q8_0".into()],
                },
            ))
        })
        .unwrap_err();

        assert_eq!(
            error,
            concat!(
                "quantization \"NOT_A_QUANT\" is not available; available quantizations: Q4_K_M, Q8_0. Retry with --quant <one of these values>.\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect owner/repo",
            )
        );
    }

    #[test]
    fn ambiguous_quant_selection_error_preserves_filenames_and_revision_inspect() {
        let args = normalized_pull_for_selection(None, Some("Q4_K_M"), Some("release candidate"));
        let error = execute_pull_resolution(&args, |_, _, _, _| {
            Err(crate::huggingface::ResolveError::Selection(
                crate::huggingface::SelectionError::AmbiguousQuant {
                    requested: "Q4_K_M".into(),
                    filenames: vec!["first-Q4_K_M.gguf".into(), "second-Q4_K_M.gguf".into()],
                },
            ))
        })
        .unwrap_err();

        assert_eq!(
            error,
            concat!(
                "quantization \"Q4_K_M\" matched multiple files: first-Q4_K_M.gguf, second-Q4_K_M.gguf. Use --file <filename> to choose one.\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect owner/repo --revision='release candidate'",
            )
        );
    }

    #[test]
    fn missing_exact_file_selection_error_appends_only_safe_inspect_guidance() {
        let args = normalized_pull_for_selection(Some("missing.gguf"), None, None);
        let error = execute_pull_resolution(&args, |_, _, _, _| {
            Err(crate::huggingface::ResolveError::Selection(
                crate::huggingface::SelectionError::FileNotFound("missing.gguf".into()),
            ))
        })
        .unwrap_err();

        assert_eq!(
            error,
            concat!(
                "verified file \"missing.gguf\" not found\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect owner/repo",
            )
        );
        for character in error.chars() {
            assert!(
                !character.is_control() || character == '\n',
                "unsafe character in {error:?}"
            );
            assert!(
                !crate::huggingface::unsafe_presentation_character(character) || character == '\n',
                "unsafe presentation character in {error:?}"
            );
        }
        for secret in ["REMOTE_BODY", "HF_TOKEN", "token path"] {
            assert!(!error.contains(secret), "{error}");
        }
    }

    #[test]
    fn discovery_failures_never_gain_selection_recovery_guidance() {
        let args = normalized_pull_for_selection(Some("model.gguf"), None, None);
        for kind in [
            DiscoveryErrorKind::AuthenticationRequired,
            DiscoveryErrorKind::RateLimited,
            DiscoveryErrorKind::DeadlineExceeded,
            DiscoveryErrorKind::MalformedResponse,
        ] {
            let error = execute_pull_resolution(&args, |_, _, _, _| {
                Err(crate::huggingface::ResolveError::Discovery(
                    DiscoveryError::new(kind),
                ))
            })
            .unwrap_err();

            assert_eq!(error, "Hugging Face discovery request failed", "{kind:?}");
            assert!(!error.contains("loxa inspect"), "{kind:?}: {error}");
        }
    }

    #[test]
    fn pull_completion_prints_observable_status_and_run_guidance_once() {
        const CHILD_OUTCOME: &str = "LOXA_PULL_COMPLETION_TEST_OUTCOME";
        if let Ok(outcome) = std::env::var(CHILD_OUTCOME) {
            let outcome = match outcome.as_str() {
                "pulled" => crate::app::TransferDisposition::Installed,
                "already-installed" => crate::app::TransferDisposition::AlreadyInstalled,
                unexpected => panic!("unexpected child outcome {unexpected:?}"),
            };
            print_pull_completion("demo-model", outcome);
            return;
        }

        for (outcome, status) in [
            ("pulled", "Pulled demo-model"),
            (
                "already-installed",
                "Verified demo-model · already installed",
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::pull_completion_prints_observable_status_and_run_guidance_once",
                    "--nocapture",
                ])
                .env(CHILD_OUTCOME, outcome)
                .env("NO_COLOR", "1")
                .current_dir(root.path())
                .output()
                .expect("capture pull completion output");

            assert!(output.status.success(), "{output:?}");
            assert_eq!(output.stderr, b"");
            let stdout = String::from_utf8(output.stdout).expect("UTF-8 completion output");
            assert!(stdout.contains(status), "{stdout:?}");
            assert_eq!(
                stdout.matches("Run: loxa run demo-model").count(),
                1,
                "{stdout:?}"
            );
            assert!(
                root.path().read_dir().unwrap().next().is_none(),
                "completion guidance created local state"
            );
        }
    }

    #[test]
    fn pull_normalizes_only_hf_wrapper_before_existing_local_validation() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let invalid_name = "invalid/name";
        let canonical_error = run(
            Cli::parse_from([
                "loxa",
                "pull",
                "owner/repo",
                "--file",
                "model.gguf",
                "--name",
                invalid_name,
            ]),
            paths.clone(),
        )
        .unwrap_err();
        let wrapped_error = run(
            Cli::parse_from([
                "loxa",
                "pull",
                "hf://owner/repo",
                "--file",
                "model.gguf",
                "--name",
                invalid_name,
            ]),
            paths.clone(),
        )
        .unwrap_err();

        assert_eq!(canonical_error, "invalid model id \"invalid/name\"");
        assert_eq!(wrapped_error, canonical_error);

        for compact in [
            "hf.co/owner/repo:Q4_K_M",
            "huggingface.co/owner/repo:model.GgUf",
        ] {
            let error = run(
                Cli::parse_from(["loxa", "pull", compact, "--name", invalid_name]),
                paths.clone(),
            )
            .unwrap_err();
            assert_eq!(error, canonical_error, "{compact}");
        }

        for repo in ["hf://owner", "https://huggingface.co/owner/repo"] {
            let error = run(
                Cli::parse_from([
                    "loxa",
                    "pull",
                    repo,
                    "--file",
                    "model.gguf",
                    "--name",
                    invalid_name,
                ]),
                paths.clone(),
            )
            .unwrap_err();
            assert!(
                error.contains("repository must be exactly owner/repo"),
                "{repo}: {error}"
            );
        }
    }

    #[test]
    fn completed_bundle_size_is_shown_in_list_and_removal_confirmation() {
        let bundle = test_bundle("gemma4");
        bundle.validate().unwrap();

        assert_eq!(installed_model_size(&bundle).to_string(), "8 B");
        assert_eq!(removal_prompt(&bundle), "Remove gemma4 (8 B)?");
    }

    #[cfg(unix)]
    #[test]
    fn qualified_bundle_rejects_an_unqualified_explicit_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install_bundle(&paths, "gemma4");
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'version: wrong-build\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = resolve_runnable(
            "gemma4".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("qualified bundle accepted an unqualified runtime"),
        };

        assert!(error.contains("test-build"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn qualified_bundle_requires_a_verified_draft_before_launch() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install_bundle(&paths, "gemma4");
        std::fs::write(
            paths.model_dir("gemma4").unwrap().join("draft.gguf"),
            b"broken",
        )
        .unwrap();
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'test-build\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = resolve_runnable(
            "gemma4".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("qualified bundle accepted a damaged draft"),
        };

        assert!(error.contains("draft.gguf"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn verified_model_launch_writes_a_receipt_for_future_admission() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install(&paths, "demo");
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runnable = resolve_runnable(
            "demo".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        )
        .unwrap();

        assert_eq!(runnable.launch.id, "demo");
        assert!(
            paths
                .model_dir("demo")
                .unwrap()
                .join("verification-receipt.json")
                .is_file(),
            "a successful full verification must refresh the receipt"
        );
    }

    #[cfg(unix)]
    #[test]
    fn receipt_records_the_manifest_and_filesystem_identities_it_will_recheck() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let manifest = install(&paths, "demo");
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        resolve_runnable(
            "demo".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        )
        .unwrap();

        let receipt: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                paths
                    .model_dir("demo")
                    .unwrap()
                    .join("verification-receipt.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["version"], 1);
        assert_eq!(receipt["manifest"]["primary_sha256"], manifest.sha256);
        assert_eq!(receipt["manifest"]["primary_size"], manifest.size);
        assert_eq!(receipt["primary"]["size"], manifest.size);
        assert!(receipt["directory"]["device"].is_u64());
        assert!(receipt["directory"]["inode"].is_u64());
    }

    #[cfg(unix)]
    #[test]
    fn model_removal_deletes_its_generated_verification_receipt() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let manifest = install(&paths, "demo");
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();
        let model_dir = paths.model_dir("demo").unwrap();

        drop(
            resolve_runnable(
                "demo".into(),
                RuntimeArgs {
                    ctx: None,
                    port: None,
                    server: Some(server),
                },
                &paths,
            )
            .unwrap(),
        );
        assert!(model_dir.join("verification-receipt.json").is_file());

        crate::catalog::remove_model(&paths.models, &manifest).unwrap();

        assert!(!model_dir.join("verification-receipt.json").exists());
        assert!(!model_dir.join("manifest.json").exists());
    }

    #[test]
    fn installed_model_load_runs_reconciliation_without_blocking_a_valid_catalog() {
        use std::cell::Cell;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        let installed = install(&paths, "demo");
        let reconciled = Cell::new(false);

        let loaded = load_installed_models_with_reconciler(&paths, |models_root| {
            assert_eq!(models_root, paths.models);
            reconciled.set(true);
            Err("injected reconciliation failure".into())
        })
        .unwrap();

        assert!(reconciled.get());
        assert_eq!(loaded, vec![installed]);
    }

    #[test]
    fn chat_with_missing_model_creates_no_config_or_history_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("loxa-home");
        let paths = AppPaths::from_values(Some(&root), None).unwrap();
        let error = run(Cli::parse_from(["loxa", "chat", "missing"]), paths.clone()).unwrap_err();

        assert!(error.contains("unknown model id missing"), "{error}");
        assert!(!paths.config.exists());
        assert!(!root.exists());
    }

    #[test]
    fn chat_without_models_explains_how_to_pull_one() {
        let error = model_options(None, &[]).unwrap_err();

        assert_eq!(
            error,
            "no models installed; choose a GGUF with `loxa inspect <owner/repo>`, then download it with `loxa pull <owner/repo> --file <filename>`"
        );
    }

    #[test]
    fn chat_without_id_auto_selects_one_model() {
        assert_eq!(
            model_options(None, &[manifest("alpha")]).unwrap(),
            ["alpha"]
        );
    }

    #[test]
    fn chat_without_id_offers_all_installed_models() {
        assert_eq!(
            model_options(None, &[manifest("alpha"), manifest("beta")]).unwrap(),
            ["alpha", "beta"]
        );
    }

    #[test]
    fn target_is_the_only_selectable_candidate_when_mtp_and_partial_files_are_present() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        std::fs::create_dir_all(&paths.models).unwrap();
        for name in ["Gemma 4.gguf", "mtp-gemma-4.gguf", "Qwen.gguf.part"] {
            std::fs::write(paths.models.join(name), b"GGUF\x03\0\0\0payload").unwrap();
        }

        let candidates = local_candidates(&paths, &[]).unwrap();
        let runnable = runnable_candidates(&candidates);

        assert_eq!(
            model_options_with_candidates(None, &[], &runnable).unwrap(),
            ["gemma-4"]
        );
        assert!(model_options_with_candidates(Some("mtp-gemma-4".into()), &[], &runnable).is_err());
    }

    #[test]
    fn chat_requires_an_interactive_input_and_output() {
        assert!(ensure_interactive_chat(true, true).is_ok());
        for (stdin, stdout) in [(false, true), (true, false), (false, false)] {
            let error = ensure_interactive_chat(stdin, stdout).unwrap_err();
            assert!(error.contains("interactive terminal"), "{error}");
            assert!(error.contains("loxa chat <id>"), "{error}");
        }
    }

    #[test]
    fn model_selection_only_requires_a_terminal_when_there_are_choices() {
        assert!(matches!(
            select_model("run", None, &[manifest("alpha")], false, false).unwrap(),
            ModelSelection::Selected(id) if id == "alpha"
        ));

        for (stdin, stderr) in [(false, true), (true, false), (false, false)] {
            let error = select_model(
                "run",
                None,
                &[manifest("alpha"), manifest("beta")],
                stdin,
                stderr,
            )
            .unwrap_err();
            assert!(error.contains("loxa run <id>"), "{error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolving_a_selected_local_candidate_adopts_it_before_launch() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        std::fs::create_dir_all(&paths.models).unwrap();
        let source = paths.models.join("Gemma 4.gguf");
        std::fs::write(&source, b"GGUF\x03\0\0\0payload").unwrap();
        let server = temp.path().join("llama-server");
        std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runnable = resolve_runnable(
            "gemma-4".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        )
        .unwrap();

        assert_eq!(runnable.launch.id, "gemma-4");
        assert!(!source.exists());
        assert!(paths.models.join("gemma-4/manifest.json").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn invalid_explicit_server_does_not_adopt_an_auto_selected_local_candidate() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        std::fs::create_dir_all(&paths.models).unwrap();
        let source = paths.models.join("Gemma 4.gguf");
        std::fs::write(&source, b"GGUF\x03\0\0\0payload").unwrap();
        let server = temp.path().join("missing-llama-server");

        let error = run(
            Cli::parse_from(["loxa", "run", "--server", server.to_str().unwrap()]),
            paths.clone(),
        )
        .unwrap_err();

        assert!(error.contains("--server is not executable"), "{error}");
        assert!(source.is_file());
        assert!(!paths.models.join("gemma-4/manifest.json").exists());
    }

    #[test]
    fn rm_without_models_reports_an_empty_catalog_without_a_pull_suggestion() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();

        let error = run(Cli::parse_from(["loxa", "rm", "--yes"]), paths).unwrap_err();

        assert_eq!(error, "no models installed");
    }

    #[test]
    fn rm_yes_removes_the_selected_managed_model() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install(&paths, "demo");

        assert_eq!(
            run(
                Cli::parse_from(["loxa", "rm", "demo", "--yes"]),
                paths.clone()
            )
            .unwrap(),
            0
        );
        let model_dir = paths.model_dir("demo").unwrap();
        assert!(model_dir.join(".lock").is_file());
        assert!(!model_dir.join("manifest.json").exists());
        assert!(!model_dir.join("model.gguf").exists());
        assert!(crate::catalog::load_catalog(&paths.models)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rm_without_yes_is_non_destructive_outside_a_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install(&paths, "demo");

        let error = run(Cli::parse_from(["loxa", "rm", "demo"]), paths.clone()).unwrap_err();

        assert!(error.contains("pass --yes"), "{error}");
        assert!(paths.model_dir("demo").unwrap().exists());
    }

    #[test]
    fn noninteractive_rm_yes_still_requires_an_explicit_model_id() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
        install(&paths, "demo");

        let error = run(Cli::parse_from(["loxa", "rm", "--yes"]), paths.clone()).unwrap_err();

        assert!(error.contains("loxa rm <id> --yes"), "{error}");
        assert!(paths.model_dir("demo").unwrap().exists());
    }
}
