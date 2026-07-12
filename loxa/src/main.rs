use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::download;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
use loxa_core::supervisor::{self, SupervisorError};
#[cfg(test)]
use loxa_core::supervisor::{ManagedServer, RuntimeStateRead};
use loxa_node::*;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "loxa", version, about = "Measured local AI infrastructure")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    Calibrate,
    Doctor,
    Pull {
        id: String,
        #[arg(long)]
        quant: Option<String>,
    },
    List,
    Rm {
        id: String,
    },
    Run {
        id: String,
        #[arg(long)]
        ctx: Option<u32>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, default_value_t = RuntimeBackendKind::LlamaCpp)]
        engine: RuntimeBackendKind,
    },
    Serve {
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, default_value_t = RuntimeBackendKind::LlamaCpp)]
        engine: RuntimeBackendKind,
    },
    Ps,
    Stop {
        target: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    run(cli, io::stdout(), io::stderr())
}

fn run<W: Write, E: Write>(cli: Cli, mut stdout: W, mut stderr: E) -> ExitCode {
    let paths = NodePaths::detect();
    run_with_paths(cli, &paths, &mut stdout, &mut stderr)
}

fn run_with_paths<W: Write, E: Write>(
    cli: Cli,
    paths: &NodePaths,
    mut stdout: W,
    mut stderr: E,
) -> ExitCode {
    if let Err(error) = validate_cli_contract(&cli) {
        return finish_cli_result(Err(error), &mut stderr);
    }
    let result = match cli.command {
        Command::Calibrate => run_calibration(&mut stdout),
        Command::Doctor => print_doctor(&mut stdout),
        Command::Pull { id, quant } => pull_model(&id, quant.as_deref(), &mut stdout, &mut stderr),
        Command::List => print_list(&mut stdout),
        Command::Rm { id } => remove_model(&id, &mut stdout, &mut stderr),
        Command::Run {
            id,
            ctx,
            port,
            engine,
        } => run_model_cli(&id, ctx, port, engine, paths, &mut stdout, &mut stderr),
        Command::Serve {
            model,
            port,
            engine,
        } => serve_node_cli(
            model.as_deref(),
            port,
            engine,
            paths,
            &mut stdout,
            &mut stderr,
        ),
        Command::Ps => render_managed_servers(managed_servers(paths), &mut stdout),
        Command::Stop { target } => render_stop_outcome(
            &target,
            stop_managed_servers(StopRequest { target: &target }, paths),
            &mut stdout,
            &mut stderr,
        ),
    };

    finish_cli_result(result, &mut stderr)
}

fn run_model_cli<W: Write, E: Write>(
    id: &str,
    ctx: Option<u32>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if engine == RuntimeBackendKind::LlamaCpp {
        let Some(_) = registry::find(id) else {
            write_unknown_id(id, stderr)?;
            return Ok(ExitCode::from(1));
        };
    }
    let outcome = {
        let mut events = CliLifecycleSink { stdout, stderr };
        run_model(
            RunRequest {
                id,
                ctx,
                port,
                engine,
            },
            paths,
            None,
            &mut events,
        )
    };
    match outcome {
        Err(error)
            if engine == RuntimeBackendKind::LlamaCpp
                && error.kind() == io::ErrorKind::NotFound =>
        {
            writeln!(
                stderr,
                "model not downloaded for {id}; run `loxa pull {id}`"
            )?;
            Ok(ExitCode::from(1))
        }
        Ok(outcome) => Ok(exit_code_for_termination(outcome)),
        Err(error) => Err(error),
    }
}

fn serve_node_cli<W: Write, E: Write>(
    requested_model: Option<&str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if engine == RuntimeBackendKind::PyMlxLm && requested_model.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--model <local-directory> is required with --engine py-mlx-lm",
        ));
    }
    if engine == RuntimeBackendKind::LlamaCpp {
        if let Err(error) = select_serve_model(&paths.models_dir, requested_model) {
            let kind = match &error {
                ModelSelectionError::UnknownModel { .. }
                | ModelSelectionError::MissingModelRequest { .. } => io::ErrorKind::InvalidInput,
                _ => io::ErrorKind::NotFound,
            };
            return Err(io::Error::new(
                kind,
                match error {
                    ModelSelectionError::UnknownModel { id } => {
                        format!("unknown model: {id}; check `loxa list`, then run `loxa pull {id}`")
                    }
                    ModelSelectionError::NotDownloaded { id } => {
                        format!("model not downloaded for {id}; run `loxa pull {id}`")
                    }
                    ModelSelectionError::NoDownloadedModels { suggested_id } => {
                        format!("no registry model is downloaded; run `loxa pull {suggested_id}`")
                    }
                    ModelSelectionError::MissingModelRequest { backend } => {
                        format!("--model <local-directory> is required with --engine {backend}")
                    }
                },
            ));
        }
    }
    let mut events = CliLifecycleSink { stdout, stderr };
    serve_node(requested_model, port, engine, paths, &mut events).map(exit_code_for_termination)
}

fn exit_code_for_termination(outcome: RunTermination) -> ExitCode {
    match outcome {
        RunTermination::RequestedStop => ExitCode::SUCCESS,
        RunTermination::Interrupted => ExitCode::from(130),
        RunTermination::Failed | RunTermination::RecoveryRequired => ExitCode::from(1),
    }
}

struct CliLifecycleSink<'a, W, E> {
    stdout: &'a mut W,
    stderr: &'a mut E,
}

impl<W: Write, E: Write> LifecycleEventSink for CliLifecycleSink<'_, W, E> {
    fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
        match event {
            LifecycleEvent::NodeListening { port, model_alias } => writeln!(
                self.stdout,
                "loxa node listening on http://127.0.0.1:{port} with model alias {model_alias}"
            ),
            LifecycleEvent::ModelReady { server } => print_run_ready(self.stdout, &server),
            LifecycleEvent::Restarting {
                process_label,
                before_healthy,
            } => {
                if before_healthy {
                    writeln!(
                        self.stdout,
                        "{process_label} exited before becoming healthy; restarting once..."
                    )
                } else {
                    writeln!(
                        self.stdout,
                        "{process_label} exited unexpectedly; restarting once..."
                    )
                }
            }
            LifecycleEvent::EngineExited {
                process_label,
                model_id,
                before_healthy,
                log_tail,
            } => {
                if before_healthy {
                    writeln!(
                        self.stderr,
                        "{process_label} exited before becoming healthy for {model_id}"
                    )?;
                } else {
                    writeln!(
                        self.stderr,
                        "{process_label} exited unexpectedly for {model_id}"
                    )?;
                }
                write_log_tail(self.stderr, &log_tail)
            }
            LifecycleEvent::HealthTimeout {
                process_label,
                log_path,
            } => {
                writeln!(
                    self.stderr,
                    "{process_label} did not become healthy within {} seconds",
                    supervisor::HEALTH_TIMEOUT.as_secs()
                )?;
                writeln!(self.stderr, "log file: {}", log_path.display())
            }
            LifecycleEvent::RecoveryRequired { run_id } => writeln!(
                self.stderr,
                "cleanup could not be confirmed for managed run {run_id}; recovery required"
            ),
        }
    }
}

fn print_run_ready<W: Write>(stdout: &mut W, server: &supervisor::ManagedServer) -> io::Result<()> {
    writeln!(stdout, "model id: {}", server.id)?;
    writeln!(stdout, "pid: {}", server.pid)?;
    writeln!(stdout, "port: {}", server.port)?;
    writeln!(stdout, "model path: {}", server.model_path.display())?;
    writeln!(
        stdout,
        "health url: http://127.0.0.1:{}/health",
        server.port
    )?;
    stdout.flush()
}

fn write_log_tail<W: Write>(writer: &mut W, log_tail: &str) -> io::Result<()> {
    if !log_tail.is_empty() {
        writeln!(writer, "log tail:\n{log_tail}")?;
    }
    Ok(())
}

fn supervisor_error_to_io(error: SupervisorError) -> io::Error {
    io::Error::other(error)
}

fn render_managed_servers<W: Write>(
    snapshot: Result<ManagedRunsSnapshot, SupervisorError>,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    match snapshot.map_err(supervisor_error_to_io)? {
        ManagedRunsSnapshot::Missing => {
            writeln!(stdout, "no managed sidecars")?;
        }
        ManagedRunsSnapshot::Runs(rows) if rows.is_empty() => {
            writeln!(stdout, "no managed sidecars")?;
        }
        ManagedRunsSnapshot::Corrupt { message } => {
            writeln!(stdout, "managed sidecar state is corrupt: {message}")?;
        }
        ManagedRunsSnapshot::Legacy { path } => {
            writeln!(
                stdout,
                "legacy managed sidecar state requires manual recovery at {}; confirm no old Loxa process remains, then archive it manually",
                path.display()
            )?;
        }
        ManagedRunsSnapshot::Runs(rows) => {
            writeln!(
                stdout,
                "id                  pid    port   status               model"
            )?;
            for row in rows {
                let pid = row
                    .child_pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".into());
                let model_path = row
                    .model_path
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "-".into());
                writeln!(
                    stdout,
                    "{:<19} {:>6}  {:>5}  {:<19} {}",
                    row.model_id, pid, row.port, row.status, model_path
                )?;
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn render_stop_outcome<W: Write, E: Write>(
    target: &str,
    outcome: Result<StopOutcome, SupervisorError>,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    match outcome.map_err(supervisor_error_to_io)? {
        StopOutcome::NoMatch if target == "all" => {
            writeln!(stdout, "no managed sidecars")?;
            Ok(ExitCode::SUCCESS)
        }
        StopOutcome::NoMatch => {
            writeln!(stderr, "no managed sidecar found for {target}")?;
            Ok(ExitCode::from(1))
        }
        StopOutcome::Completed { model_id } => {
            writeln!(stdout, "stop completed for {model_id}")?;
            Ok(ExitCode::SUCCESS)
        }
        StopOutcome::RecoveryRequired {
            run_id,
            model_id,
            owner_status,
        } => {
            writeln!(
                stderr,
                "stop requested for {model_id}, but owner identity is {owner_status:?}; recovery required for {run_id}"
            )?;
            Ok(ExitCode::from(1))
        }
        StopOutcome::TimedOut { run_id, model_id } => {
            writeln!(
                stderr,
                "stop requested for {model_id}, but the owner did not finish within {} seconds; recovery required for {run_id}",
                supervisor::STOP_OWNER_WAIT_TIMEOUT.as_secs()
            )?;
            Ok(ExitCode::from(1))
        }
    }
}

fn validate_cli_contract(cli: &Cli) -> io::Result<()> {
    if matches!(
        &cli.command,
        Command::Run {
            ctx: Some(_),
            engine: RuntimeBackendKind::PyMlxLm,
            ..
        }
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--ctx is not supported with --engine py-mlx-lm",
        ));
    }
    Ok(())
}

fn finish_cli_result<E: Write>(result: io::Result<ExitCode>, stderr: &mut E) -> ExitCode {
    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_calibration<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    run_calibration_with(loxa_core::calibration::run_pinned_calibration, stdout)
}

fn run_calibration_with<W: Write>(
    execute: impl FnOnce() -> Result<
        loxa_core::calibration::CalibrationOutcome,
        loxa_core::calibration::CalibrationError,
    >,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    match execute() {
        Ok(outcome) => {
            render_calibration_outcome(&outcome, stdout)?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Err(io::Error::other(calibration_error_message(&error))),
    }
}

fn render_calibration_outcome<W: Write>(
    outcome: &loxa_core::calibration::CalibrationOutcome,
    output: &mut W,
) -> io::Result<()> {
    use loxa_core::evidence::EvidenceVerdict;
    let evidence = &outcome.evidence;
    let evidence_path = outcome
        .evidence_path
        .as_deref()
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            io::Error::other("calibration succeeded without an absolute evidence path")
        })?;
    writeln!(output, "workload: {}", evidence.workload_version)?;
    for (index, candidate) in evidence.candidates.iter().enumerate() {
        let label = if index == 0 { 'A' } else { 'B' };
        writeln!(
            output,
            "candidate {label}: {} fingerprint={} digest={} provider={:?}",
            candidate.identity.candidate_id,
            candidate.fingerprint,
            candidate.identity.artifact.digest_sha256,
            candidate.identity.provider_kind
        )?;
    }
    writeln!(output, "\nqualification:")?;
    for (index, candidate) in evidence.candidates.iter().enumerate() {
        let label = if index == 0 { 'A' } else { 'B' };
        let qualification = evidence
            .qualifications
            .iter()
            .find(|q| q.candidate_fingerprint == candidate.fingerprint);
        let passed = qualification.is_some_and(|q| q.passed_current_contract());
        let failure = qualification.and_then(|q| {
            q.case_results
                .iter()
                .find(|case| !case.passed)
                .and_then(|case| case.reason.as_deref())
                .or_else(|| q.failure_codes.first().map(String::as_str))
        });
        match failure {
            Some(reason) => writeln!(output, "  candidate {label}: failed — {reason}")?,
            None if passed => writeln!(output, "  candidate {label}: passed")?,
            None => writeln!(output, "  candidate {label}: failed — qualification_failed")?,
        }
    }
    match &evidence.verdict {
        EvidenceVerdict::Selected {
            candidate_id,
            reason_code,
            ..
        } => {
            let label = candidate_label(evidence, candidate_id)?;
            writeln!(output, "\nverdict: selected candidate {label}")?;
            writeln!(output, "reason: {reason_code}")?;
        }
        EvidenceVerdict::NoVerifiedPlan { reason_codes, .. } => {
            writeln!(output, "\nverdict: no verified plan")?;
            writeln!(output, "reason: {}", reason_codes.join(", "))?;
        }
        EvidenceVerdict::NoMaterialWinner {
            baseline_candidate_id,
            reason_code,
            ..
        } => {
            let label = candidate_label(evidence, baseline_candidate_id)?;
            writeln!(output, "\nverdict: no material winner")?;
            writeln!(output, "reason: {reason_code}")?;
            writeln!(output, "baseline retained: candidate {label}")?;
        }
    }
    writeln!(output, "evidence: {}", evidence_path.display())?;
    Ok(())
}

fn candidate_label(
    evidence: &loxa_core::evidence::CalibrationEvidence,
    id: &str,
) -> io::Result<char> {
    match evidence
        .candidates
        .iter()
        .position(|candidate| candidate.identity.candidate_id == id)
    {
        Some(0) => Ok('A'),
        Some(1) => Ok('B'),
        _ => Err(io::Error::other(format!(
            "verdict references unknown candidate id: {id:?}"
        ))),
    }
}

fn calibration_error_message(error: &loxa_core::calibration::CalibrationError) -> String {
    use loxa_core::calibration::CalibrationError;
    match error {
        CalibrationError::Isolation(reasons) => {
            format!("isolation prerequisite failed: {}", reasons.join(", "))
        }
        CalibrationError::Provider(error) => format!("provider prerequisite failed: {error}"),
        CalibrationError::IdentityChanged => {
            "evidence error: candidate identity changed during calibration".into()
        }
        CalibrationError::Evidence(error) => format!("evidence persistence failed: {error}"),
        CalibrationError::OperationAndTeardown {
            operation,
            teardown,
        } => format!(
            "{}; managed teardown also failed: {teardown}",
            calibration_error_message(operation)
        ),
        CalibrationError::Aborted {
            kind,
            evidence_path,
        } => format!(
            "calibration aborted: {kind}; evidence: {}",
            evidence_path.display()
        ),
    }
}

fn pull_model<W: Write, E: Write>(
    id: &str,
    quant: Option<&str>,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if id.starts_with("hf://") || id.matches('/').count() == 1 {
        let reference = loxa_core::resolve::ModelReference::parse(id).map_err(io::Error::other)?;
        let available = HardwareReport::detect().ram_available_bytes;
        let resolved = match loxa_core::resolve::resolve(&reference, quant, available) {
            Ok(resolved) => resolved,
            Err(error) => {
                writeln!(stderr, "pull resolution failed: {error}")?;
                return Ok(ExitCode::from(1));
            }
        };
        let generated_id = format!(
            "{}-{}",
            reference
                .repo
                .split('/')
                .next_back()
                .unwrap_or("model")
                .to_ascii_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "-"),
            resolved.quant.to_ascii_lowercase().replace('_', "-")
        );
        let entry = registry::UserModelEntry {
            id: generated_id,
            repo: resolved.repo,
            revision: resolved.revision,
            filename: resolved.filename,
            sha256: resolved.sha256,
            size_bytes: resolved.size_bytes,
            license: resolved.license,
            params: "unknown".into(),
            quant: resolved.quant,
            min_free_mem_gb: resolved.min_free_mem_gb,
        };
        if registry::find(&entry.id).is_some()
            || registry::load_user_entries(&user_registry_dir())
                .map_err(io::Error::other)?
                .iter()
                .any(|old| old.id == entry.id)
        {
            writeln!(
                stderr,
                "model id {} already exists; run `loxa rm {}` first",
                entry.id, entry.id
            )?;
            return Ok(ExitCode::from(1));
        }
        writeln!(
            stdout,
            "selected {} ({}, {:.1} GB minimum free RAM)",
            entry.filename, entry.quant, entry.min_free_mem_gb
        )?;
        return match download::download(&entry, &download::model_dir()) {
            Ok(path) => {
                registry::save_user_entry(&user_registry_dir(), &entry)
                    .map_err(io::Error::other)?;
                writeln!(stdout, "{}", path.display())?;
                Ok(ExitCode::SUCCESS)
            }
            Err(error) => {
                writeln!(stderr, "pull failed for {}: {error}", entry.id)?;
                Ok(ExitCode::from(1))
            }
        };
    }
    let Some(entry) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    let dir = download::model_dir();
    match download::download(entry, &dir) {
        Ok(path) => {
            writeln!(stdout, "{}", path.display())?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            writeln!(stderr, "pull failed for {id}: {error}")?;
            Ok(ExitCode::from(1))
        }
    }
}

fn user_registry_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".loxa/registry.d")
}

fn print_list<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    let dir = download::model_dir();
    let rows = REGISTRY
        .iter()
        .map(|entry| {
            (
                entry,
                bytes_to_gb_string(entry.size_bytes),
                model_status(entry, &dir).to_string(),
            )
        })
        .collect::<Vec<_>>();

    let id_width = rows
        .iter()
        .map(|(entry, _, _)| entry.id.len())
        .chain([2])
        .max()
        .unwrap_or(2);
    let params_width = rows
        .iter()
        .map(|(entry, _, _)| entry.params.len())
        .chain([6])
        .max()
        .unwrap_or(6);
    let quant_width = rows
        .iter()
        .map(|(entry, _, _)| entry.quant.len())
        .chain([5])
        .max()
        .unwrap_or(5);
    let size_width = rows
        .iter()
        .map(|(_, size, _)| size.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let license_width = rows
        .iter()
        .map(|(entry, _, _)| entry.license.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let status_width = rows
        .iter()
        .map(|(_, _, status)| status.len())
        .chain([6])
        .max()
        .unwrap_or(6);

    writeln!(
        stdout,
        "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
        "id", "params", "quant", "size GB", "license", "status",
    )?;

    for (entry, size, status) in rows {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id, entry.params, entry.quant, size, entry.license, status,
        )?;
    }

    for entry in registry::load_user_entries(&user_registry_dir()).map_err(io::Error::other)? {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id,
            entry.params,
            entry.quant,
            bytes_to_gb_string(entry.size_bytes),
            entry.license,
            if download::model_dir().join(&entry.filename).exists() {
                "downloaded"
            } else {
                "not downloaded"
            },
        )?;
    }

    Ok(ExitCode::SUCCESS)
}

fn remove_model<W: Write, E: Write>(
    id: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let dir = download::model_dir();
    let removed = if let Some(entry) = REGISTRY.iter().find(|entry| entry.id == id) {
        remove_model_files(entry, &dir)?
    } else {
        let Some(removed) = remove_user_entry(id, &user_registry_dir(), &dir)? else {
            write_unknown_id(id, stderr)?;
            return Ok(ExitCode::from(1));
        };
        removed
    };
    if removed.is_empty() {
        writeln!(stdout, "nothing present for {id}")?;
    } else {
        for path in removed {
            writeln!(stdout, "removed {}", path.display())?;
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn remove_user_entry(
    id: &str,
    registry_dir: &Path,
    models_dir: &Path,
) -> io::Result<Option<Vec<PathBuf>>> {
    let entries = registry::load_user_entries(registry_dir).map_err(io::Error::other)?;
    let Some(entry) = entries.into_iter().find(|entry| entry.id == id) else {
        return Ok(None);
    };
    let mut removed = Vec::new();
    for path in [
        models_dir.join(&entry.filename),
        models_dir.join(format!("{}.part", entry.filename)),
        registry_dir.join(format!("{}.json", entry.id)),
    ] {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }
    Ok(Some(removed))
}

fn write_unknown_id<W: Write>(id: &str, stderr: &mut W) -> io::Result<()> {
    writeln!(stderr, "unknown model id: {id}")?;
    writeln!(stderr, "valid ids: {}", valid_ids())
}

fn print_doctor<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    write_doctor(stdout)?;
    Ok(ExitCode::SUCCESS)
}

fn write_doctor<W: Write>(stdout: &mut W) -> io::Result<()> {
    let hardware = HardwareReport::detect();
    let tools = LocalToolsReport::detect();
    write_doctor_report(stdout, &hardware, &tools)
}

fn write_doctor_report<W: Write>(
    stdout: &mut W,
    hardware: &HardwareReport,
    tools: &LocalToolsReport,
) -> io::Result<()> {
    writeln!(stdout, "Machine")?;
    writeln!(stdout, "  {:<16} {}", "Chip:", hardware.chip)?;
    writeln!(
        stdout,
        "  {:<16} {} physical / {} logical",
        "Cores:", hardware.physical_cores, hardware.logical_cores
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB available / {:.1} GB used",
        "RAM:",
        bytes_to_gb(hardware.ram_total_bytes),
        bytes_to_gb(hardware.ram_available_bytes),
        bytes_to_gb(hardware.ram_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB used",
        "Swap:",
        bytes_to_gb(hardware.swap_total_bytes),
        bytes_to_gb(hardware.swap_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} total / {} available",
        "Disk (/):",
        optional_bytes_to_gb(hardware.root_disk_total_bytes),
        optional_bytes_to_gb(hardware.root_disk_available_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} {}",
        "OS:", hardware.os_name, hardware.os_version
    )?;
    writeln!(stdout)?;
    writeln!(stdout, "Detected tools")?;
    for tool in &tools.tools {
        write_tool(stdout, tool)?;
    }

    Ok(())
}

fn write_tool<W: Write>(stdout: &mut W, tool: &DetectedTool) -> io::Result<()> {
    let detection = &tool.detection;
    let evidence = if detection.evidence.is_empty() {
        "unknown".to_string()
    } else {
        detection.evidence.join("; ")
    };

    writeln!(
        stdout,
        "  {:<10} {:<13} {:<11} {}",
        tool.name, detection.install_state, detection.run_state, evidence
    )
}

fn bytes_to_gb_string(bytes: u64) -> String {
    format!("{:.1}", bytes_to_gb(bytes))
}

fn valid_ids() -> String {
    REGISTRY
        .iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>()
        .join(", ")
}

fn model_paths(entry: &ModelEntry, dir: &Path) -> (PathBuf, PathBuf) {
    (
        dir.join(entry.filename),
        dir.join(format!("{}.part", entry.filename)),
    )
}

#[derive(Debug, PartialEq, Eq)]
enum ModelStatus {
    Downloaded,
    Partial,
    NotDownloaded,
}

impl fmt::Display for ModelStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModelStatus::Downloaded => write!(f, "downloaded"),
            ModelStatus::Partial => write!(f, "partial"),
            ModelStatus::NotDownloaded => write!(f, "not downloaded"),
        }
    }
}

fn model_status(entry: &ModelEntry, dir: &Path) -> ModelStatus {
    let (final_path, part_path) = model_paths(entry, dir);

    if final_path.exists() {
        ModelStatus::Downloaded
    } else if part_path.exists() {
        ModelStatus::Partial
    } else {
        ModelStatus::NotDownloaded
    }
}

fn remove_model_files(entry: &ModelEntry, dir: &Path) -> io::Result<Vec<PathBuf>> {
    let (final_path, part_path) = model_paths(entry, dir);
    let mut removed = Vec::new();

    for path in [final_path, part_path] {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }

    Ok(removed)
}

fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn optional_bytes_to_gb(bytes: Option<u64>) -> String {
    bytes
        .map(|bytes| format!("{:.1} GB", bytes_to_gb(bytes)))
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::detect::{InstallState, RunState, ToolDetection};
    use loxa_core::registry::REGISTRY;
    use std::fs;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn doctor_report_renders_injected_python_mlx_evidence() {
        let hardware = HardwareReport {
            chip: "Apple M4".to_string(),
            physical_cores: 4,
            logical_cores: 8,
            ram_total_bytes: 16 * 1024 * 1024 * 1024,
            ram_available_bytes: 8 * 1024 * 1024 * 1024,
            ram_used_bytes: 8 * 1024 * 1024 * 1024,
            swap_total_bytes: 0,
            swap_used_bytes: 0,
            root_disk_total_bytes: Some(512 * 1024 * 1024 * 1024),
            root_disk_available_bytes: Some(256 * 1024 * 1024 * 1024),
            os_name: "macOS".to_string(),
            os_version: "15.0".to_string(),
        };
        let tools = LocalToolsReport {
            tools: vec![DetectedTool {
                name: "Python MLX (external)".to_string(),
                detection: ToolDetection {
                    install_state: InstallState::Installed,
                    run_state: RunState::ReachableUnverified,
                    evidence: vec![
                        "platform compatible: macos/aarch64".to_string(),
                        "server path: /opt/tools/mlx_lm.server".to_string(),
                        "required version: 0.31.3".to_string(),
                        "detected version: 0.31.3".to_string(),
                        "external default endpoint reachable: 127.0.0.1:8080".to_string(),
                    ],
                },
            }],
        };
        let mut output = Vec::new();

        write_doctor_report(&mut output, &hardware, &tools).expect("render doctor report");

        let output = String::from_utf8(output).expect("doctor output is utf8");
        assert!(output.contains("Python MLX"));
        assert!(output.contains("platform compatible: macos/aarch64"));
        assert!(output.contains("server path: /opt/tools/mlx_lm.server"));
        assert!(output.contains("required version: 0.31.3"));
        assert!(output.contains("detected version: 0.31.3"));
        assert!(output.contains("reachable (unverified)"));
        assert!(output.contains("external default endpoint reachable: 127.0.0.1:8080"));
    }

    fn persist_run_for_server(state_path: &Path, server: &ManagedServer) -> supervisor::ManagedRun {
        let run_id = format!("test-run-{}", server.pid);
        let mut run = starting_run_for_test(state_path, &run_id);
        run.model_id = server.id.clone();
        run.port = server.port;
        run.generation_alias = format!("loxa-{run_id}-g0");
        run.log_path = state_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{run_id}.log"));
        supervisor::create_starting_run(state_path, run.clone()).expect("create starting run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(server.pid);
        run.child_process_start_time_unix_s = server.process_start_time_unix_s;
        assert!(
            supervisor::update_runtime_state_run(state_path, &starting_identity, run.clone())
                .expect("attach test child")
        );
        run
    }

    fn starting_run_for_test(state_path: &Path, run_id: &str) -> supervisor::ManagedRun {
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: "gemma-3-4b-it-q4".to_string(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            port: 8080,
            log_path: state_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn set_test_owner_to_current_process(run: &mut supervisor::ManagedRun) {
        run.owner_pid = std::process::id();
        run.owner_process_start_time_unix_s =
            supervisor::process_start_time_with_retry(run.owner_pid)
                .expect("current test process start time");
    }

    fn set_test_child_to_current_process(run: &mut supervisor::ManagedRun, listener: &TcpListener) {
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.port = listener.local_addr().expect("listener address").port();
        run.child_pid = Some(std::process::id());
        run.child_process_start_time_unix_s = Some(
            supervisor::process_start_time_with_retry(std::process::id())
                .expect("current test child process start time"),
        );
    }

    fn persist_test_run(state_path: &Path, run: supervisor::ManagedRun) {
        let mut starting = run.clone();
        starting.stop_requested = false;
        starting.lifecycle = supervisor::RunLifecycle::Starting;
        starting.child_pid = None;
        starting.child_process_start_time_unix_s = None;
        starting.child_pgid = None;
        supervisor::create_starting_run(state_path, starting.clone())
            .expect("create test starting run");
        if run != starting {
            assert!(
                supervisor::update_runtime_state_run(state_path, &starting.identity(), run)
                    .expect("persist final test run")
            );
        }
    }

    fn render_ps_for_test(temp: &TempDir) -> String {
        let state_path = temp.path().join("managed.json");
        let before = fs::read(&state_path).expect("read state before ps");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(
            Cli {
                command: Command::Ps,
            },
            &paths,
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            fs::read(&state_path).expect("read state after ps"),
            before,
            "ps must not mutate managed state"
        );
        String::from_utf8(stdout).expect("stdout is utf8")
    }

    fn only_ps_row_fields(stdout: &str) -> Vec<&str> {
        let lines = stdout.lines().collect::<Vec<_>>();
        assert_eq!(
            lines.len(),
            2,
            "expected one header and one row: {stdout:?}"
        );
        lines[1].split_whitespace().collect()
    }

    #[test]
    fn serve_selects_first_downloaded_registry_model_in_order() {
        let temp = TempDir::new("serve-selection");
        let later = &REGISTRY[2];
        let first = &REGISTRY[1];
        fs::write(temp.path().join(later.filename), b"later").unwrap();
        fs::write(temp.path().join(first.filename), b"first").unwrap();

        let selected = select_serve_model(temp.path(), None).unwrap();

        assert_eq!(selected.id, first.id);
    }

    #[test]
    fn serve_selection_error_remains_product_neutral() {
        let temp = TempDir::new("serve-selection");

        let error = match select_serve_model(temp.path(), Some("not-in-registry")) {
            Ok(_) => panic!("unknown model unexpectedly selected"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            ModelSelectionError::UnknownModel {
                id: "not-in-registry".into()
            }
        );
        assert!(!error.to_string().contains("loxa pull"));
    }

    #[test]
    fn clap_parses_serve_options() {
        let cli = Cli::try_parse_from([
            "loxa",
            "serve",
            "--model",
            "gemma-3-4b-it-q4",
            "--port",
            "11435",
        ])
        .unwrap();
        match cli.command {
            Command::Serve {
                model,
                port,
                engine,
            } => {
                assert_eq!(model.as_deref(), Some("gemma-3-4b-it-q4"));
                assert_eq!(port, Some(11435));
                assert_eq!(engine, RuntimeBackendKind::LlamaCpp);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn clap_preserves_llama_default_and_accepts_explicit_runtime_engines() {
        assert!(matches!(
            Cli::try_parse_from(["loxa", "run", "gemma-3-4b-it-q4"]),
            Ok(Cli {
                command: Command::Run {
                    engine: RuntimeBackendKind::LlamaCpp,
                    ..
                }
            })
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "loxa",
                "run",
                "/tmp/mlx model",
                "--engine",
                "py-mlx-lm",
            ]),
            Ok(Cli {
                command: Command::Run {
                    id,
                    engine: RuntimeBackendKind::PyMlxLm,
                    ..
                }
            }) if id == "/tmp/mlx model"
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "loxa",
                "serve",
                "--model",
                "/tmp/mlx model",
                "--engine",
                "py-mlx-lm",
            ]),
            Ok(Cli {
                command: Command::Serve {
                    model: Some(model),
                    engine: RuntimeBackendKind::PyMlxLm,
                    ..
                }
            }) if model == "/tmp/mlx model"
        ));
        assert!(Cli::try_parse_from(["loxa", "serve", "--engine", "llama-cpp",]).is_ok());
    }

    #[test]
    fn clap_rejects_invalid_engine() {
        assert!(
            Cli::try_parse_from([
                "loxa",
                "run",
                "gemma-3-4b-it-q4",
                "--engine",
                "not-an-engine",
            ])
            .is_err()
        );
    }

    #[test]
    fn python_ctx_is_rejected_before_execution() {
        let cli = Cli::try_parse_from([
            "loxa",
            "run",
            "/tmp/mlx-model",
            "--engine",
            "py-mlx-lm",
            "--ctx",
            "4096",
        ])
        .expect("parse Python engine request");
        let temp = TempDir::new("loxa-python-ctx");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        let error = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(error.contains("--ctx"));
        assert!(error.contains("py-mlx-lm"));
        assert!(!paths.state_path.exists());
    }

    #[test]
    fn clap_parses_all_subcommands() {
        assert!(matches!(
            Cli::try_parse_from(["loxa", "calibrate"]),
            Ok(Cli {
                command: Command::Calibrate
            })
        ));
        assert!(Cli::try_parse_from(["loxa", "doctor"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "list"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "pull", "gemma-3-4b-it-q4"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "rm", "gemma-3-4b-it-q4"]).is_ok());
        assert!(matches!(
            Cli::try_parse_from(["loxa", "run", "gemma-3-4b-it-q4", "--ctx", "4096", "--port", "9000"]),
            Ok(Cli {
                command: Command::Run {
                    id,
                    ctx: Some(4096),
                    port: Some(9000),
                    engine: RuntimeBackendKind::LlamaCpp,
                },
            }) if id == "gemma-3-4b-it-q4"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "ps"]),
            Ok(Cli {
                command: Command::Ps,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "stop", "all"]),
            Ok(Cli {
                command: Command::Stop { target },
            }) if target == "all"
        ));
    }

    #[test]
    fn calibration_renderer_reports_no_material_winner_and_retained_baseline() {
        let outcome = calibration_outcome_for_test(
            loxa_core::evidence::EvidenceVerdict::NoMaterialWinner {
                schema_version: 1,
                baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                reason_code: "no_material_winner".into(),
            },
            loxa_core::selector::SelectorVerdict::NoMaterialWinner {
                schema_version: 1,
                baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                reason: "both qualified; attached candidate did not clear both thresholds".into(),
            },
        );
        let mut output = Vec::new();
        render_calibration_outcome(&outcome, &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("workload: tool-use-v1"));
        assert!(output.contains("verdict: no material winner"));
        assert!(output.contains("baseline retained: candidate A"));
        assert!(output.contains("evidence: /tmp/calibration.json"));
        assert!(!output.contains("best"));
    }

    #[test]
    fn calibration_renderer_reports_selected_and_no_verified_outcomes() {
        use loxa_core::evidence::EvidenceVerdict;
        use loxa_core::selector::SelectorVerdict;
        let cases = [
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "only_managed_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "only managed qualified".into(),
                },
                "verdict: selected candidate A",
            ),
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "candidate-b".into(),
                    reason_code: "only_attached_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "candidate-b".into(),
                    reason: "only attached qualified".into(),
                },
                "verdict: selected candidate B",
            ),
            (
                EvidenceVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reason_codes: vec!["qualification_failed".into()],
                },
                SelectorVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reasons: vec!["qualification failed".into()],
                },
                "verdict: no verified plan",
            ),
        ];
        for (evidence_verdict, verdict, expected) in cases {
            let outcome = calibration_outcome_for_test(evidence_verdict, verdict);
            let mut output = Vec::new();
            render_calibration_outcome(&outcome, &mut output).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(expected));
            assert!(output.contains("candidate A: gemma-3-4b-it-q4 fingerprint="));
            assert!(output.contains("digest="));
            assert!(output.contains("provider=Ollama"));
            assert!(output.contains("candidate A: passed"));
            assert!(output.contains("candidate B: passed"));
            assert!(output.contains("reason:"));
            assert!(output.contains("evidence: /tmp/calibration.json"));
        }
    }

    #[test]
    fn calibration_errors_are_nonzero_and_name_the_prerequisite_class() {
        use loxa_core::calibration::CalibrationError;
        use loxa_core::evidence::read_evidence_json;
        use loxa_core::provider::ProviderError;
        let evidence_error = read_evidence_json(b"not-json").unwrap_err();
        let cases = [
            (
                CalibrationError::Isolation(vec!["other model loaded".into()]),
                "isolation prerequisite failed",
            ),
            (
                CalibrationError::Provider(ProviderError::Unreachable),
                "provider prerequisite failed",
            ),
            (CalibrationError::IdentityChanged, "evidence error"),
            (
                CalibrationError::Evidence(evidence_error),
                "evidence persistence failed",
            ),
            (
                CalibrationError::OperationAndTeardown {
                    operation: Box::new(CalibrationError::Provider(ProviderError::Unreachable)),
                    teardown: ProviderError::Lifecycle("cleanup".into()),
                },
                "managed teardown also failed",
            ),
            (
                CalibrationError::Aborted {
                    kind: "isolation_lost".into(),
                    evidence_path: PathBuf::from("/tmp/aborted-evidence.json"),
                },
                "calibration aborted: isolation_lost; evidence: /tmp/aborted-evidence.json",
            ),
        ];
        for (error, expected) in cases {
            let mut output = Vec::new();
            let result = run_calibration_with(|| Err(error), &mut output);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected));
            assert!(output.is_empty());
        }
    }

    #[test]
    fn calibration_error_reaches_top_level_stderr_and_nonzero_exit() {
        let mut stdout = Vec::new();
        let result = run_calibration_with(
            || {
                Err(loxa_core::calibration::CalibrationError::Provider(
                    loxa_core::provider::ProviderError::Unreachable,
                ))
            },
            &mut stdout,
        );
        let mut stderr = Vec::new();
        let exit = finish_cli_result(result, &mut stderr);
        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("error: provider prerequisite failed: provider is unreachable")
        );
    }

    #[test]
    fn calibration_success_requires_absolute_evidence_for_every_verdict() {
        use loxa_core::evidence::EvidenceVerdict;
        use loxa_core::selector::SelectorVerdict;
        let verdicts = vec![
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "only_managed_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "x".into(),
                },
            ),
            (
                EvidenceVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reason_codes: vec!["qualification_failed".into()],
                },
                SelectorVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reasons: vec!["x".into()],
                },
            ),
            (
                EvidenceVerdict::NoMaterialWinner {
                    schema_version: 1,
                    baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "no_material_winner".into(),
                },
                SelectorVerdict::NoMaterialWinner {
                    schema_version: 1,
                    baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "x".into(),
                },
            ),
        ];
        for (evidence_verdict, verdict) in verdicts {
            for path in [None, Some(PathBuf::from("relative.json"))] {
                let mut outcome =
                    calibration_outcome_for_test(evidence_verdict.clone(), verdict.clone());
                outcome.evidence_path = path;
                assert!(render_calibration_outcome(&outcome, &mut Vec::new()).is_err());
            }
        }
    }

    #[test]
    fn calibration_qualification_requires_all_five_clean_passes() {
        use loxa_core::workload::QualificationCaseResult;
        let mut outcome = calibration_outcome_for_test(
            loxa_core::evidence::EvidenceVerdict::NoVerifiedPlan {
                schema_version: 1,
                reason_codes: vec!["qualification_failed".into()],
            },
            loxa_core::selector::SelectorVerdict::NoVerifiedPlan {
                schema_version: 1,
                reasons: vec!["x".into()],
            },
        );
        outcome.evidence.qualifications[0].case_results = (0..4)
            .map(|i| QualificationCaseResult {
                schema_version: 1,
                case_id: format!("case-{i}"),
                passed: true,
                reason: None,
            })
            .collect();
        let mut output = Vec::new();
        render_calibration_outcome(&outcome, &mut output).unwrap();
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("candidate A: failed — qualification_failed")
        );
    }

    #[test]
    fn calibration_unknown_or_empty_selected_candidate_is_an_error() {
        for id in ["", "unknown"] {
            let mut outcome = calibration_outcome_for_test(
                loxa_core::evidence::EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: id.into(),
                    reason_code: "only_managed_qualified".into(),
                },
                loxa_core::selector::SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: id.into(),
                    reason: "x".into(),
                },
            );
            outcome.evidence.verdict = loxa_core::evidence::EvidenceVerdict::Selected {
                schema_version: 1,
                candidate_id: id.into(),
                reason_code: "only_managed_qualified".into(),
            };
            assert!(render_calibration_outcome(&outcome, &mut Vec::new()).is_err());
        }
    }

    fn calibration_outcome_for_test(
        evidence_verdict: loxa_core::evidence::EvidenceVerdict,
        verdict: loxa_core::selector::SelectorVerdict,
    ) -> loxa_core::calibration::CalibrationOutcome {
        use loxa_core::evidence::*;
        let mut a = loxa_core::provider::managed_llama::managed_candidate_spec(
            "fixture-provider",
            "fixture-revision",
        )
        .expect("valid fixture candidate");
        a.provider_kind = loxa_core::provider::ProviderKind::Ollama;
        a.ownership = loxa_core::provider::ProviderOwnership::Attached;
        a.endpoint = "http://127.0.0.1:11434".into();
        a.engine.engine_kind = "ollama-managed-gguf-engine".into();
        a.candidate_id = "gemma-3-4b-it-q4".into();
        let mut b = a.clone();
        b.candidate_id = "candidate-b".into();
        b.artifact.artifact_id = "candidate-b-artifact".into();
        let candidates = [
            CandidateEvidence {
                schema_version: 1,
                fingerprint: a.fingerprint(),
                identity: a,
            },
            CandidateEvidence {
                schema_version: 1,
                fingerprint: b.fingerprint(),
                identity: b,
            },
        ];
        loxa_core::calibration::CalibrationOutcome {
            evidence: CalibrationEvidence {
                schema_version: 1,
                protocol_version: CALIBRATION_PROTOCOL_VERSION.into(),
                workload_version: "tool-use-v1".into(),
                policy_version: "selector-v1".into(),
                started_at_unix_ms: 1,
                ended_at_unix_ms: 2,
                host: HostFingerprint {
                    schema_version: 1,
                    os_name: "test".into(),
                    os_version: "1".into(),
                    hardware_model: "test".into(),
                    physical_cores: 1,
                    logical_cores: 1,
                    memory_total_bytes: 1,
                    memory_available_bytes: 1,
                    root_disk_total_bytes: Some(1),
                    root_disk_available_bytes: Some(1),
                },
                qualifications: candidates
                    .iter()
                    .map(|candidate| QualificationEvidence {
                        schema_version: 1,
                        candidate_fingerprint: candidate.fingerprint.clone(),
                        case_results: (0..5)
                            .map(|i| loxa_core::workload::QualificationCaseResult {
                                schema_version: 1,
                                case_id: format!("case-{i}"),
                                passed: true,
                                reason: None,
                            })
                            .collect(),
                        failure_codes: vec![],
                    })
                    .collect(),
                candidates,
                disclosed_differences: vec![],
                measurements: vec![],
                isolation_observations: vec![],
                verdict: evidence_verdict,
                explanation_codes: vec![],
            },
            evidence_path: Some(PathBuf::from("/tmp/calibration.json")),
            verdict,
        }
    }

    #[test]
    fn unknown_pull_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Pull {
                id: "missing-model".to_string(),
                quant: None,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_rm_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Rm {
                id: "missing-model".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_run_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Run {
                id: "missing-model".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: PathBuf::from("/tmp/unused-models"),
            state_path: PathBuf::from("/tmp/unused-managed.json"),
            logs_dir: PathBuf::from("/tmp/unused-logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn model_not_downloaded_run_error_tells_user_to_pull() {
        let temp = TempDir::new("loxa-run-not-downloaded");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("model not downloaded"));
        assert!(stderr.contains("loxa pull gemma-3-4b-it-q4"));
    }

    #[test]
    fn invalid_python_model_fails_before_runtime_state_creation() {
        let temp = TempDir::new("loxa-python-invalid-model");
        let missing_model = temp.path().join("missing mlx model");
        let state_path = temp.path().join("managed.json");
        let cli = Cli {
            command: Command::Run {
                id: missing_model.display().to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
        };
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            assert!(stderr.contains("py-mlx-lm model path"), "{stderr}");
            assert!(stderr.contains("existing directory"), "{stderr}");
        } else {
            assert!(stderr.contains("requires Apple Silicon macOS"), "{stderr}");
        }
        assert!(!stderr.contains("unknown model id"), "{stderr}");
        assert!(
            !state_path.exists(),
            "validation must precede state creation"
        );
    }

    #[test]
    fn python_serve_requires_an_explicit_local_model_before_gateway_start() {
        let temp = TempDir::new("loxa-python-serve-no-model");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let error = serve_node_cli(
            None,
            Some(0),
            RuntimeBackendKind::PyMlxLm,
            &paths,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("Python serve needs --model");

        assert_eq!(
            error.to_string(),
            "--model <local-directory> is required with --engine py-mlx-lm"
        );
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        assert!(!paths.state_path.exists());
    }

    #[test]
    fn ps_renders_clear_message_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-ps-empty");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn ps_legacy_sentinel_without_managed_json_fails_closed_with_exact_recovery_guidance() {
        let temp = TempDir::new("loxa-ps-legacy-sentinel");
        let state_path = temp.path().join("managed.json");
        let sentinel_path = state_path.with_file_name("managed.json.lock");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(&sentinel_path, b"legacy owner metadata\n").expect("write legacy sentinel");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(
            Cli {
                command: Command::Ps,
            },
            &paths,
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            String::from_utf8(stdout).expect("stdout is utf8"),
            format!(
                "legacy managed sidecar state requires manual recovery at {}; confirm no old Loxa process remains, then archive it manually\n",
                sentinel_path.display()
            )
        );
        assert!(sentinel_path.exists());
        assert!(!state_path.exists());
        assert!(!state_path.with_file_name("managed.json.v2.lock").exists());
    }

    #[test]
    fn ps_renders_childless_live_owner_as_starting() {
        let temp = TempDir::new("loxa-ps-starting");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-starting");
        set_test_owner_to_current_process(&mut run);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], "-");
        assert_eq!(fields[3], "starting");
    }

    #[test]
    fn ps_renders_live_owner_with_stop_intent_as_stopping() {
        let temp = TempDir::new("loxa-ps-stopping");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-stopping");
        set_test_owner_to_current_process(&mut run);
        run.stop_requested = true;
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(u32::MAX);
        run.child_process_start_time_unix_s = Some(1);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[3], "stopping");
    }

    #[test]
    fn ps_renders_dead_owner_with_live_child_as_recovery_required() {
        let temp = TempDir::new("loxa-ps-dead-owner");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-dead-owner");
        run.owner_pid = u32::MAX;
        run.owner_process_start_time_unix_s = 1;
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "recovery-required");
        assert!(!stdout.contains("  running"));
    }

    #[test]
    fn ps_renders_running_only_for_live_owner_and_exact_live_child() {
        let temp = TempDir::new("loxa-ps-running");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-running");
        set_test_owner_to_current_process(&mut run);
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "running");
    }

    #[test]
    fn ps_model_column_renders_the_canonical_model_path_without_shifting_child_pid() {
        let temp = TempDir::new("loxa-ps-model-column");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-model-column");
        set_test_owner_to_current_process(&mut run);
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);
        let entry = registry::find("gemma-3-4b-it-q4").expect("registry entry");
        let expected_model_path = temp.path().join("models").join(entry.filename);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "running");
        assert_eq!(fields[4], expected_model_path.display().to_string());
    }

    #[test]
    fn ps_marks_inconsistent_entries_as_recovery_required() {
        let temp = TempDir::new("loxa-ps-stale");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let stale = loxa_core::supervisor::ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 999_999,
            port: 65_530,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 1_700_000_000,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        persist_run_for_server(&temp.path().join("managed.json"), &stale);
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("recovery-required"));
        assert!(stdout.contains("gemma-3-4b-it-q4"));
    }

    #[test]
    fn ps_reports_corrupt_state_without_failing() {
        let temp = TempDir::new("loxa-ps-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn run_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-run-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn stop_all_is_idempotent_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-stop-all-empty");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn stop_all_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-stop-all-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn cli_stop_dead_owner_records_durable_intent_and_preserves_full_run() {
        let temp = TempDir::new("loxa-stop-dead-owner");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.owner_pid = 999_999;
        run.owner_process_start_time_unix_s = 1;
        supervisor::create_starting_run(&state_path, run.clone()).expect("create run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(999_998);
        run.child_process_start_time_unix_s = Some(2);
        run.child_pgid = Some(999_998);
        let run =
            supervisor::update_runtime_state_run_committed(&state_path, &starting_identity, run)
                .expect("attach child metadata")
                .expect("exact attachment");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = render_stop_outcome(
            &run.model_id,
            stop_managed_servers(
                StopRequest {
                    target: &run.model_id,
                },
                &paths,
            ),
            &mut stdout,
            &mut stderr,
        )
        .expect("stop command result");

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8(stderr)
                .expect("stderr utf8")
                .contains("recovery required")
        );
        let RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).expect("read preserved run")
        else {
            panic!("expected loaded run");
        };
        assert_eq!(runs.len(), 1);
        let mut expected = run;
        expected.stop_requested = true;
        assert_eq!(runs[0], expected);
    }

    #[test]
    fn model_status_prioritizes_downloaded_then_partial_then_not_downloaded() {
        let temp = TempDir::new("loxa-status");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());

        assert_eq!(model_status(entry, temp.path()), ModelStatus::NotDownloaded);

        fs::write(&part_path, b"partial").expect("write part file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::write(&final_path, b"final").expect("write final file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Downloaded);
    }

    #[test]
    fn remove_model_files_deletes_final_and_part_then_returns_empty_when_absent() {
        let temp = TempDir::new("loxa-rm");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());
        fs::write(&final_path, b"final").expect("write final file");
        fs::write(&part_path, b"partial").expect("write part file");

        let removed = remove_model_files(entry, temp.path()).expect("remove model files");

        assert_eq!(removed, vec![final_path.clone(), part_path.clone()]);
        assert!(!final_path.exists());
        assert!(!part_path.exists());

        let removed = remove_model_files(entry, temp.path()).expect("remove absent model files");
        assert!(removed.is_empty());
    }

    #[test]
    fn remove_user_entry_deletes_registry_final_and_partial_files() {
        let temp = TempDir::new("loxa-user-rm");
        let registry_dir = temp.path().join("registry.d");
        let models_dir = temp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        let entry = registry::UserModelEntry {
            id: "demo-q4-k-m".into(),
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "demo-Q4_K_M.gguf".into(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size_bytes: 100 * 1024 * 1024,
            license: "apache-2.0".into(),
            params: "unknown".into(),
            quant: "Q4_K_M".into(),
            min_free_mem_gb: 0.1,
        };
        let registry_path = registry::save_user_entry(&registry_dir, &entry).unwrap();
        let final_path = models_dir.join(&entry.filename);
        let part_path = models_dir.join(format!("{}.part", entry.filename));
        fs::write(&final_path, b"final").unwrap();
        fs::write(&part_path, b"partial").unwrap();

        let removed = remove_user_entry(&entry.id, &registry_dir, &models_dir)
            .unwrap()
            .unwrap();

        assert_eq!(
            removed,
            vec![final_path.clone(), part_path.clone(), registry_path.clone()]
        );
        assert!(!final_path.exists() && !part_path.exists() && !registry_path.exists());
    }

    #[test]
    fn bytes_to_gb_string_uses_one_decimal() {
        assert_eq!(bytes_to_gb_string(0), "0.0");
        assert_eq!(bytes_to_gb_string(1_073_741_824), "1.0");
        assert_eq!(bytes_to_gb_string(1_610_612_736), "1.5");
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
