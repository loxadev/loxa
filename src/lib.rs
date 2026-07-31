pub mod catalog;
pub mod chat;
pub mod cli;
pub mod config;
pub mod download;
pub mod huggingface;
pub mod paths;
pub mod runner;
mod session;
mod ui;

use catalog::Manifest;
use clap::Parser;
use cli::{Cli, Command};
use indicatif::BinaryBytes;
use paths::{validate_id, AppPaths};
use std::io::IsTerminal;
use std::path::PathBuf;

struct Runnable {
    server: PathBuf,
    artifact: PathBuf,
    id: String,
    port: u16,
    ctx: u32,
}

pub fn run_from_env() -> Result<i32, String> {
    run(Cli::parse(), AppPaths::from_env()?)
}

pub fn report_error(error: &str) {
    let danger = ui::danger();
    anstream::eprintln!("{danger}Error:{danger:#} {error}");
}

pub fn run(cli: Cli, paths: AppPaths) -> Result<i32, String> {
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
            let resolved = huggingface::resolve(
                &client,
                &repo,
                args.revision.as_deref(),
                args.filename.as_deref(),
                args.quant.as_deref(),
                token.as_deref(),
            )?;
            let id = args
                .name
                .unwrap_or_else(|| default_id(&repo, &resolved.filename, &resolved.sha256));
            let model_dir = paths.model_dir(&id)?;
            let _pull_lock = catalog::PullLock::acquire(&model_dir)?;
            let manifest = Manifest {
                version: 1,
                id: id.clone(),
                repo: resolved.repo.clone(),
                revision: resolved.revision.clone(),
                remote_filename: resolved.filename.clone(),
                local_filename: "model.gguf".into(),
                sha256: resolved.sha256.clone(),
                size: resolved.size,
            };
            if model_dir.join("manifest.json").exists() {
                let existing = catalog::load_catalog(&paths.models)?
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
            anstream::println!("{accent}Pulling{accent:#} {id}");
            anstream::println!(
                "  {muted}{} · {} · {}@{}{muted:#}",
                resolved.filename,
                BinaryBytes(resolved.size),
                resolved.repo,
                &resolved.revision[..12]
            );
            download::download(&resolved, &model_dir, token)?;
            catalog::publish_manifest(&paths.models, &manifest)?;
            let success = ui::success();
            anstream::println!("{success}Pulled{success:#} {id}");
            Ok(0)
        }
        Command::List => {
            let installed = catalog::load_catalog(&paths.models)?;
            if installed.is_empty() {
                let muted = ui::muted();
                anstream::println!("No models installed.");
                anstream::println!("{muted}Download one with `loxa pull <owner/repo>`{muted:#}");
                return Ok(0);
            }
            let heading = ui::success();
            let accent = ui::accent();
            let muted = ui::muted();
            anstream::println!(
                "{heading}Installed models{heading:#} {muted}({}){muted:#}",
                installed.len()
            );
            for entry in installed {
                anstream::println!(
                    "\n  {accent}{}{accent:#}  {}",
                    entry.id,
                    BinaryBytes(entry.size)
                );
                anstream::println!(
                    "    {muted}{} · {} · {}{muted:#}",
                    entry.repo,
                    entry.remote_filename,
                    &entry.revision[..12]
                );
            }
            Ok(0)
        }
        Command::Run(args) => {
            let installed = catalog::load_catalog(&paths.models)?;
            let id = match select_model(
                "run",
                args.id,
                &installed,
                std::io::stdin().is_terminal(),
                std::io::stderr().is_terminal(),
            )? {
                ModelSelection::Selected(id) => id,
                ModelSelection::Exit(code) => return Ok(code),
            };
            let runnable = resolve_runnable(id, args.runtime, &paths)?;
            runner::run(
                &runnable.server,
                &runnable.artifact,
                &runnable.id,
                runnable.port,
                runnable.ctx,
            )
        }
        Command::Chat(args) => {
            let installed = catalog::load_catalog(&paths.models)?;
            let options = model_options(args.id, &installed)?;
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
            let runnable = resolve_runnable(id, args.runtime, &paths)?;
            match runner::start_foreground(
                &runnable.server,
                &runnable.artifact,
                &runnable.id,
                runnable.port,
                runnable.ctx,
            )? {
                runner::ForegroundStart::Ready(server) => session::run(server, &runnable.id),
                runner::ForegroundStart::Stopped(code) => Ok(code),
            }
        }
    }
}

fn model_options(requested: Option<String>, installed: &[Manifest]) -> Result<Vec<String>, String> {
    if let Some(id) = requested {
        if !installed.iter().any(|model| model.id == id) {
            return Err(format!("unknown model id {id}"));
        }
        return Ok(vec![id]);
    }
    if installed.is_empty() {
        return Err("no models installed; download one with `loxa pull <owner/repo>`".into());
    }
    Ok(installed.iter().map(|model| model.id.clone()).collect())
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
    match inquire::Select::new("Choose a model", options)
        .with_help_message("↑↓ navigate · enter select · type to filter")
        .prompt()
    {
        Ok(id) => Ok(ModelSelection::Selected(id)),
        Err(inquire::InquireError::OperationCanceled) => Ok(ModelSelection::Exit(0)),
        Err(inquire::InquireError::OperationInterrupted) => Ok(ModelSelection::Exit(130)),
        Err(inquire::InquireError::NotTTY) => Err(format!(
            "model selection requires an interactive terminal; pass `loxa {command} <id>`"
        )),
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
    let manifest = catalog::load_catalog(&paths.models)?
        .into_iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| format!("unknown model id {id}"))?;
    let artifact = manifest.artifact_path(&paths.models);
    download::verify_regular(&artifact, manifest.size, &manifest.sha256)?;
    let server = runner::discover_from_process(runtime.server.as_deref(), &paths.managed_server)?;
    Ok(Runnable {
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
        default_id, ensure_interactive_chat, model_options, run, select_model, ModelSelection,
    };
    use crate::catalog::Manifest;
    use crate::cli::Cli;
    use crate::paths::AppPaths;
    use clap::Parser;

    fn manifest(id: &str) -> Manifest {
        Manifest {
            version: 1,
            id: id.into(),
            repo: "owner/repo".into(),
            revision: "0".repeat(40),
            remote_filename: "model-Q4_K_M.gguf".into(),
            local_filename: "model.gguf".into(),
            sha256: "a".repeat(64),
            size: 1,
        }
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
}
