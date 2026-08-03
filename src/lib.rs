pub mod catalog;
pub mod chat;
pub mod cli;
pub mod config;
pub mod download;
pub mod huggingface;
pub mod paths;
pub mod runner;
mod runtime;
mod session;
mod ui;

use catalog::Manifest;
use clap::Parser;
use cli::{Cli, Command};
use indicatif::BinaryBytes;
use paths::{validate_id, AppPaths};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;

struct Runnable {
    _model_lock: catalog::ModelLock,
    server: PathBuf,
    artifact: PathBuf,
    id: String,
    port: u16,
    ctx: u32,
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
    run(Cli::parse(), AppPaths::from_env()?)
}

pub fn report_error(error: &str) {
    let danger = ui::danger();
    let error = ui::sanitize_terminal(error);
    anstream::eprintln!("{danger}Error:{danger:#} {error}");
}

pub fn run(cli: Cli, paths: AppPaths) -> Result<i32, String> {
    runtime::recover_stale(&paths.run)?;
    catalog::local::recover_pending(&paths.models)?;
    match cli.command {
        Command::Pull(args) => {
            let repo = huggingface::parse_repo(&args.repo)?;
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
            let resolving = ui::spinner(format!("Resolving {repo}"));
            let resolved = huggingface::resolve(
                &client,
                &repo,
                args.revision.as_deref(),
                args.filename.as_deref(),
                args.quant.as_deref(),
                token.as_deref(),
            );
            resolving.finish_and_clear();
            let resolved = resolved?;
            let id = args
                .name
                .unwrap_or_else(|| default_id(&repo, &resolved.filename, &resolved.sha256));
            let model_dir = paths.model_dir(&id)?;
            let _model_lock = catalog::ModelLock::acquire(&model_dir)?;
            let manifest = Manifest {
                version: 1,
                id: id.clone(),
                repo: Some(resolved.repo.clone()),
                revision: Some(resolved.revision.clone()),
                remote_filename: Some(resolved.filename.clone()),
                origin: None,
                source_filename: None,
                local_filename: "model.gguf".into(),
                sha256: resolved.sha256.clone(),
                size: resolved.size,
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
                resolved.filename,
                BinaryBytes(resolved.size),
                resolved.repo,
                &resolved.revision[..12]
            );
            let outcome = download::download(&resolved, &model_dir, token)?;
            let verifying = ui::spinner(format!("Verifying {id}"));
            let published = catalog::publish_manifest(&paths.models, &manifest);
            verifying.finish_and_clear();
            published?;
            let success = ui::success();
            match outcome {
                download::DownloadOutcome::Pulled(_) => {
                    anstream::println!("{success}Pulled{success:#} {id}")
                }
                download::DownloadOutcome::AlreadyInstalled(_) => {
                    let muted = ui::muted();
                    anstream::println!(
                        "{success}Verified{success:#} {id} {muted}· already installed{muted:#}"
                    )
                }
            }
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
                anstream::println!("{muted}Download one with `loxa pull <owner/repo>`{muted:#}");
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
                anstream::println!("{success}Imported{success:#} {}", runnable.id);
            }
            runner::run(
                &runnable.server,
                &runnable.artifact,
                &runnable.id,
                runnable.port,
                runnable.ctx,
                &paths.run,
            )
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
                anstream::println!("{success}Imported{success:#} {}", runnable.id);
            }
            let starting = ui::spinner(format!("Starting {}", runnable.id));
            let started = runner::start_foreground(
                &runnable.server,
                &runnable.artifact,
                &runnable.id,
                runnable.port,
                runnable.ctx,
                &paths.run,
            );
            starting.finish_and_clear();
            match started? {
                runner::ForegroundStart::Ready(server) => session::run(server, &runnable.id),
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
    let _ = reconcile(&paths.models);
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
        return Err("no models installed; download one with `loxa pull <owner/repo>`".into());
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
    let server = runner::discover_from_process(runtime.server.as_deref(), &paths.managed_server)?;
    let manifest = match load_installed_models(paths)?
        .into_iter()
        .find(|entry| entry.id == id)
    {
        Some(manifest) => manifest,
        None => {
            let candidate = catalog::local::discover(&paths.models)?
                .into_iter()
                .find(|candidate| candidate.id == id)
                .ok_or_else(|| format!("unknown model id {id}"))?;
            catalog::local::adopt(&paths.models, &candidate)?
        }
    };
    let model_lock = catalog::ModelLock::acquire(&paths.model_dir(&manifest.id)?)?;
    let artifact = manifest.artifact_path(&paths.models);
    download::verify_regular(&artifact, manifest.size, &manifest.sha256)?;
    Ok(Runnable {
        _model_lock: model_lock,
        server,
        artifact,
        id: manifest.id,
        port,
        ctx,
    })
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
        default_id, ensure_interactive_chat, installed_model_size,
        load_installed_models_with_reconciler, local_candidates, model_options,
        model_options_with_candidates, removal_prompt, resolve_runnable, run, runnable_candidates,
        select_model, ModelSelection,
    };
    use crate::catalog::{
        Artifact, ArtifactProvenance, ArtifactRole, Manifest, RuntimeQualification,
        TEST_LLAMA_BUILD, TEST_MTP_PROFILE,
    };
    use crate::cli::{Cli, RuntimeArgs};
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
            sha256: "a".repeat(64),
            size: 3,
            artifacts: Some(vec![
                Artifact {
                    role: ArtifactRole::Model,
                    local_filename: "model.gguf".into(),
                    sha256: "a".repeat(64),
                    size: 3,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "model-source.gguf".into(),
                    },
                },
                Artifact {
                    role: ArtifactRole::Draft,
                    local_filename: "draft.gguf".into(),
                    sha256: "b".repeat(64),
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
    fn completed_bundle_size_is_shown_in_list_and_removal_confirmation() {
        let bundle = test_bundle("gemma4");
        bundle.validate().unwrap();

        assert_eq!(installed_model_size(&bundle).to_string(), "8 B");
        assert_eq!(removal_prompt(&bundle), "Remove gemma4 (8 B)?");
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

        assert!(error.contains("loxa pull"), "{error}");
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

        assert_eq!(runnable.id, "gemma-4");
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
