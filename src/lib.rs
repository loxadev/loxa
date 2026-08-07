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
pub mod download;
pub mod huggingface;
pub mod paths;
pub mod runner;
mod runtime;
mod safe_file;
mod session;
mod ui;

use catalog::Manifest;
use cli::{Cli, Command};
use indicatif::BinaryBytes;
use paths::{validate_id, AppPaths};
use std::io::IsTerminal;
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;

struct Runnable {
    _model_lock: catalog::ModelLock,
    launch: runner::Launch,
}

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

fn print_pull_completion(id: &str, outcome: &download::DownloadOutcome) {
    let success = ui::success();
    match outcome {
        download::DownloadOutcome::Pulled(_) => {
            anstream::println!("{success}Pulled{success:#} {id}")
        }
        download::DownloadOutcome::AlreadyInstalled(_) => {
            let muted = ui::muted();
            anstream::println!(
                "{success}Verified{success:#} {id} {muted}· already installed{muted:#}"
            );
        }
    }
    let muted = ui::muted();
    anstream::println!("{muted}Run: loxa run {id}{muted:#}");
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
    if !matches!(&cli.command, Command::Search(_) | Command::Inspect(_)) {
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
            let token = huggingface::discover_token();
            let client = reqwest::blocking::Client::builder()
                .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(std::time::Duration::from_secs(30))
                .timeout(std::time::Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| error.to_string())?;
            let resolving = ui::spinner(format!("Resolving {}", args.repo));
            let resolved = execute_pull_resolution(&args, |repo, revision, filename, quant| {
                huggingface::resolve(&client, repo, revision, filename, quant, token.as_deref())
            });
            resolving.finish_and_clear();
            let resolved = resolved?;
            let repo = args.repo;
            let id = args
                .name
                .unwrap_or_else(|| default_id(&repo, resolved.path(), resolved.sha256()));
            let model_dir = paths.model_dir(&id)?;
            let _model_lock = catalog::ModelLock::acquire(&model_dir)?;
            let manifest = Manifest {
                version: 1,
                id: id.clone(),
                repo: Some(resolved.repo().to_owned()),
                revision: Some(resolved.commit().to_owned()),
                remote_filename: Some(resolved.path().to_owned()),
                origin: None,
                source_filename: None,
                local_filename: "model.gguf".into(),
                sha256: resolved.sha256().to_owned(),
                size: resolved.size(),
                artifacts: None,
                profile: None,
                runtime: None,
            };
            if model_dir.join("manifest.json").exists() {
                let existing = load_installed_models(&paths)?
                    .into_iter()
                    .find(|entry| entry.id == id)
                    .ok_or_else(|| format!("missing manifest for model {id}"))?;
                if existing != manifest {
                    return Err(format!(
                        "model id {id} already refers to a different artifact"
                    ));
                }
            } else {
                catalog::prepare_pull(&model_dir, &manifest)?;
            }
            let accent = ui::accent();
            let muted = ui::muted();
            anstream::println!("{accent}Checking{accent:#} {id}");
            anstream::println!(
                "  {muted}{} · {} · {}@{}{muted:#}",
                resolved.path(),
                BinaryBytes(resolved.size()),
                resolved.repo(),
                &resolved.commit()[..12]
            );
            tracing::info!(
                event = "pull_started",
                model_id = %id,
                repo = %resolved.repo(),
                revision = %resolved.commit(),
                size = resolved.size()
            );
            let outcome = download::download(&resolved, &model_dir, token)?;
            let verifying = ui::spinner(format!("Verifying {id}"));
            let published = catalog::publish_manifest(&paths.models, &manifest);
            verifying.finish_and_clear();
            published?;
            let download_outcome = match &outcome {
                download::DownloadOutcome::Pulled(_) => "pulled",
                download::DownloadOutcome::AlreadyInstalled(_) => "already_installed",
            };
            tracing::info!(
                event = "pull_finished",
                model_id = %id,
                repo = %resolved.repo(),
                revision = %resolved.commit(),
                size = resolved.size(),
                outcome = download_outcome
            );
            print_pull_completion(&id, &outcome);
            Ok(0)
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
                runner::ForegroundStart::Ready(server) => session::run(server, &runnable.launch.id),
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

fn resolve_runnable(
    id: String,
    runtime: cli::RuntimeArgs,
    paths: &AppPaths,
) -> Result<Runnable, String> {
    let config = config::load(&paths.config)?;
    let ctx = config::resolve_value(runtime.ctx, config.ctx, 4096);
    let port = config::resolve_value(runtime.port, config.port, 0);
    let installed = load_installed_models(paths)?;
    let installed = installed.into_iter().find(|entry| entry.id == id);
    let (manifest, profile, server) = match installed {
        Some(manifest) => {
            let profile = launch_profile(&manifest, &paths.models)?;
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
            )?;
            (manifest, profile, server)
        }
        None => {
            let candidate = catalog::local::discover(&paths.models)?
                .into_iter()
                .find(|candidate| candidate.id == id)
                .ok_or_else(|| format!("unknown model id {id}"))?;
            let profile = runner::LaunchProfile::generic();
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
            )?;
            let manifest = catalog::local::adopt(&paths.models, &candidate)?;
            tracing::info!(event = "local_model_adopted", model_id = %manifest.id);
            (manifest, profile, server)
        }
    };
    let model_lock = catalog::ModelLock::acquire(&paths.model_dir(&manifest.id)?)?;
    let artifact = manifest.artifact_path(&paths.models);
    let primary = manifest.primary_artifact();
    download::verify_regular(&artifact, primary.size, primary.sha256)?;
    if let Some(draft) = manifest.draft_artifact() {
        let path = paths.models.join(&manifest.id).join(draft.local_filename);
        download::verify_regular(&path, draft.size, draft.sha256)?;
    }
    Ok(Runnable {
        _model_lock: model_lock,
        launch: runner::Launch {
            server,
            model: artifact,
            id: manifest.id,
            requested_port: port,
            ctx,
            profile,
        },
    })
}

fn launch_profile(
    manifest: &Manifest,
    models_root: &Path,
) -> Result<runner::LaunchProfile, String> {
    match (
        manifest.version,
        manifest.profile.as_deref(),
        manifest.runtime.as_ref(),
    ) {
        (3, Some(catalog::GEMMA4_MTP_PROFILE), Some(runtime))
            if runtime.engine == "llama.cpp" && runtime.build == catalog::GEMMA4_LLAMA_BUILD =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp(
                manifest.draft_path(models_root),
            ))
        }
        #[cfg(test)]
        (3, Some(catalog::TEST_MTP_PROFILE), Some(runtime))
            if runtime.engine == "llama.cpp" && runtime.build == catalog::TEST_LLAMA_BUILD =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp_for_test(
                manifest.draft_path(models_root),
                runtime.build.clone(),
            ))
        }
        (1 | 2, None, None) => Ok(runner::LaunchProfile::generic()),
        _ => Err("unsupported validated runtime profile".into()),
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
        default_id, discovery_error_message, ensure_interactive_chat, execute_inspect,
        execute_pull_resolution, execute_search, installed_model_size,
        load_installed_models_with_reconciler, local_candidates, model_options,
        model_options_with_candidates, print_pull_completion, removal_prompt, resolve_runnable,
        run, run_with_recovery, runnable_candidates, select_model, ModelSelection,
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
                "pulled" => crate::download::DownloadOutcome::Pulled("model.gguf".into()),
                "already-installed" => {
                    crate::download::DownloadOutcome::AlreadyInstalled("model.gguf".into())
                }
                unexpected => panic!("unexpected child outcome {unexpected:?}"),
            };
            print_pull_completion("demo-model", &outcome);
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
