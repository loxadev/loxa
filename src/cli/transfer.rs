use super::{DiscardArgs, PullArgs};
use crate::paths::{validate_id, AppPaths};
use crate::{app, cli, huggingface};
use std::io::IsTerminal;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;

#[cfg(unix)]
pub(super) struct PromptInterrupt {
    id: signal_hook::SigId,
    interrupted: Arc<AtomicBool>,
}

#[cfg(unix)]
impl PromptInterrupt {
    pub(super) fn install() -> Result<Self, String> {
        let interrupted = Arc::new(AtomicBool::new(false));
        let id = signal_hook::flag::register(signal_hook::consts::SIGINT, interrupted.clone())
            .map_err(|error| format!("failed to install prompt interrupt handler: {error}"))?;
        Ok(Self { id, interrupted })
    }

    pub(super) fn received(&self) -> bool {
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
    pub(super) model_id: &'a str,
    pub(super) artifact: &'a huggingface::ResolvedFile,
    pub(super) retained_bytes: u64,
    pub(super) discardable: bool,
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

pub(super) fn run_pull(args: PullArgs, paths: AppPaths) -> Result<i32, String> {
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
        |selected, control, progress| service.transfer_selected(selected, control, progress),
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
        app::TransferDisposition::Installed | app::TransferDisposition::AlreadyInstalled => {
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

pub(super) fn run_discard(args: DiscardArgs, paths: AppPaths) -> Result<i32, String> {
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
            Err(dialoguer::Error::IO(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
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

#[cfg(test)]
#[path = "transfer/tests.rs"]
mod tests;
