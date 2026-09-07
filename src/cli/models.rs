use super::transfer::PromptInterrupt;
use super::{ChatArgs, RmArgs, RunArgs};
use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::runnable::resolve_runnable;
use crate::{app, catalog, cli, runner, session, ui};
use indicatif::BinaryBytes;
use std::io::IsTerminal;

fn installed_model_size(manifest: &Manifest) -> BinaryBytes {
    BinaryBytes(manifest.total_size())
}

fn format_incomplete_transfers(
    entries: &[app::IncompleteTransferSummary],
    unrecognized_root_partials: usize,
) -> String {
    let mut output = String::new();
    if !entries.is_empty() {
        output.push_str(&format!("Incomplete downloads ({})\n", entries.len()));
        for entry in entries {
            let completed = entry.completed_bytes();
            let total = entry.total_bytes();
            let percent = if total == 0 {
                0
            } else {
                ((u128::from(completed.min(total)) * 100 + u128::from(total) / 2)
                    / u128::from(total)) as u64
            };
            output.push_str(&format!(
                "\n  {}  {percent}% · {} of {}\n    Discard: loxa discard {}\n",
                entry.model_id(),
                format_decimal_bytes(completed),
                format_decimal_bytes(total),
                cli::shell_quote(entry.model_id()),
            ));
        }
    }
    if unrecognized_root_partials > 0 {
        if !output.is_empty() {
            output.push('\n');
        }
        let noun = if unrecognized_root_partials == 1 {
            "file was"
        } else {
            "files were"
        };
        output.push_str(&format!(
            "{unrecognized_root_partials} unrecognized partial {noun} left untouched.\n"
        ));
    }
    output
}

fn format_decimal_bytes(bytes: u64) -> String {
    const KB: f64 = 1_000.0;
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;

    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / GB)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / MB)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / KB)
    } else {
        format!("{bytes} bytes")
    }
}

fn load_installed_models(paths: &AppPaths) -> Result<Vec<Manifest>, String> {
    catalog::load_reconciled_catalog(&paths.models)
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

pub(super) fn run_list(paths: AppPaths) -> Result<i32, String> {
    let installed = load_installed_models(&paths)?;
    let candidates = local_candidates(&paths, &installed)?;
    let runnable = runnable_candidates(&candidates);
    let auxiliaries = auxiliary_candidates(&candidates);
    let incomplete = app::AppService::from_paths(paths.clone()).incomplete_transfers()?;
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
    let incomplete_output = format_incomplete_transfers(
        incomplete.entries(),
        incomplete.unrecognized_root_partials(),
    );
    if !incomplete_output.is_empty() {
        anstream::println!();
        anstream::print!("{incomplete_output}");
    }
    Ok(0)
}

pub(super) fn run_rm(args: RmArgs, paths: AppPaths) -> Result<i32, String> {
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
            return Err("removal confirmation requires an interactive terminal; pass --yes".into());
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
            Err(dialoguer::Error::IO(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
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

pub(super) fn run_model(args: RunArgs, paths: AppPaths) -> Result<i32, String> {
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
        anstream::println!("{success}Imported{success:#} {}", runnable.launch().id);
    }
    runner::run_launch(runnable.launch(), &paths.run)
}

pub(super) fn run_chat(args: ChatArgs, paths: AppPaths) -> Result<i32, String> {
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
    match session::route_chat(
        installed.iter().find(|manifest| manifest.id == id),
        &args.runtime,
        &paths,
    )? {
        session::ChatRoute::Attached(attached) => {
            return session::run_attached(attached, max_tokens);
        }
        session::ChatRoute::Foreground => {}
    }
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
        anstream::println!("{success}Imported{success:#} {}", runnable.launch().id);
    }
    let starting = ui::spinner(format!("Starting {}", runnable.launch().id));
    let started = runner::start_foreground(runnable.launch(), &paths.run);
    starting.finish_and_clear();
    match started? {
        runner::ForegroundStart::Ready(server) => {
            session::run(server, &runnable.launch().id, max_tokens)
        }
        runner::ForegroundStart::Stopped(exit) => Ok(runner::report_exit(exit)),
    }
}

#[cfg(test)]
#[path = "models/tests.rs"]
mod tests;
