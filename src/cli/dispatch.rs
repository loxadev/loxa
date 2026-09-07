use super::{discovery, models, transfer, Cli, Command};
use crate::paths::AppPaths;
use crate::{catalog, cli, runtime, service, ui};

pub(crate) fn run_from_env() -> Result<i32, String> {
    match service::run_hidden_from_env() {
        service::HiddenServiceResult::NotServiceCommand => {}
        service::HiddenServiceResult::Exit(result) => return result,
    }
    let cli = cli::parse_checked();
    let paths = AppPaths::from_env()?;
    if matches!(&cli.command, Command::ServiceDev(_) | Command::List) {
        return run(cli, paths);
    }
    let diagnostics = match loxa_diagnostics::init(&paths.logs, loxa_diagnostics::ProcessRole::Cli)
    {
        Ok(diagnostics) => Some(diagnostics),
        Err(_) => {
            anstream::eprintln!("Warning: local diagnostics are unavailable");
            None
        }
    };
    let command = command_name(&cli.command);
    tracing::info!(event = "cli_startup", command);
    let result = run(cli, paths);
    match &result {
        Ok(code) => tracing::info!(event = "cli_finished", command, exit_code = *code),
        Err(_) => tracing::error!(event = "cli_failed", command),
    }
    if diagnostics
        .map(loxa_diagnostics::Diagnostics::finish)
        .is_some_and(|health| !health.is_healthy())
    {
        anstream::eprintln!("Warning: some local diagnostics could not be retained");
    }
    result
}

pub(crate) fn report_error(error: &str) {
    let danger = ui::danger();
    let error = ui::sanitize_terminal(error);
    anstream::eprintln!("{danger}Error:{danger:#} {error}");
    if let Some(path) = loxa_diagnostics::active_log_dir() {
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
        Command::ServiceDev(_) => "service-dev",
    }
}

pub(crate) fn run(cli: Cli, paths: AppPaths) -> Result<i32, String> {
    run_with_recovery(cli, paths, |paths| {
        runtime::recover_stale(&paths.run)?;
        catalog::local::recover_pending(&paths.models)?;
        Ok(())
    })
}

pub(super) fn run_with_recovery<F>(cli: Cli, paths: AppPaths, recovery: F) -> Result<i32, String>
where
    F: FnOnce(&AppPaths) -> Result<(), String>,
{
    cli::preflight(&cli).map_err(|error| error.to_string())?;
    let service_development = matches!(&cli.command, Command::ServiceDev(_));
    if !service_development {
        reject_development_root_for_legacy_command(&paths)?;
        if !matches!(
            &cli.command,
            Command::Search(_) | Command::Inspect(_) | Command::Discard(_)
        ) {
            recovery(&paths)?;
        }
    }
    match cli.command {
        Command::Search(args) => discovery::run_search(args, paths),
        Command::Inspect(args) => discovery::run_inspect(args, paths),
        Command::Pull(args) => transfer::run_pull(args, paths),
        Command::List => models::run_list(paths),
        Command::Rm(args) => models::run_rm(args, paths),
        Command::Discard(args) => transfer::run_discard(args, paths),
        Command::Run(args) => models::run_model(args, paths),
        Command::Chat(args) => models::run_chat(args, paths),
        Command::ServiceDev(args) => service::run_development_cli(args),
    }
}

fn reject_development_root_for_legacy_command(paths: &AppPaths) -> Result<(), String> {
    let marker = paths.root.join(loxa_ipc::DEVELOPMENT_MARKER_FILENAME);
    match std::fs::symlink_metadata(&marker) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(
            "this data root is reserved for background-service development; use `loxa service-dev`"
                .into(),
        ),
        Err(error) => Err(format!("could not inspect {}: {error}", marker.display())),
    }
}

#[cfg(test)]
#[path = "dispatch/tests.rs"]
mod tests;
