pub mod catalog;
pub mod chat;
pub mod cli;
pub mod config;
pub mod download;
pub mod huggingface;
pub mod paths;
pub mod runner;
mod session;

use catalog::Manifest;
use clap::Parser;
use cli::{Cli, Command};
use paths::{validate_id, AppPaths};
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
            println!(
                "pulling {} ({} bytes) as {}",
                resolved.filename, resolved.size, id
            );
            download::download(&resolved, &model_dir, token)?;
            catalog::publish_manifest(&paths.models, &manifest)?;
            println!("pulled {id}");
            Ok(0)
        }
        Command::List => {
            for entry in catalog::load_catalog(&paths.models)? {
                println!(
                    "{}\t{}@{}\t{}\t{} bytes",
                    entry.id, entry.repo, entry.revision, entry.remote_filename, entry.size
                );
            }
            Ok(0)
        }
        Command::Run(args) => {
            let runnable = resolve_runnable(args, &paths)?;
            runner::run(
                &runnable.server,
                &runnable.artifact,
                &runnable.id,
                runnable.port,
                runnable.ctx,
            )
        }
        Command::Chat(args) => {
            let runnable = resolve_runnable(args, &paths)?;
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

fn resolve_runnable(args: cli::RunArgs, paths: &AppPaths) -> Result<Runnable, String> {
    let config = config::load(&paths.config)?;
    let ctx = config::resolve_value(args.runtime.ctx, config.ctx, 4096);
    let port = config::resolve_value(args.runtime.port, config.port, 0);
    let manifest = catalog::load_catalog(&paths.models)?
        .into_iter()
        .find(|entry| entry.id == args.id)
        .ok_or_else(|| format!("unknown model id {}", args.id))?;
    let artifact = manifest.artifact_path(&paths.models);
    download::verify_regular(&artifact, manifest.size, &manifest.sha256)?;
    let server =
        runner::discover_from_process(args.runtime.server.as_deref(), &paths.managed_server)?;
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
    use super::{default_id, run};
    use crate::cli::Cli;
    use crate::paths::AppPaths;
    use clap::Parser;

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
}
