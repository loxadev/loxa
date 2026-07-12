use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::download;
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
#[cfg(test)]
use loxa_core::supervisor::{
    self, InterruptStatus, ManagedChild, ManagedServer, ObservedChildExit, RuntimeStateRead,
    SupervisorError,
};
use loxa_node::*;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
#[cfg(test)]
use std::time::Duration;

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
        } => run_model(
            RunRequest {
                id: &id,
                ctx,
                port,
                engine,
            },
            paths,
            &mut stdout,
            &mut stderr,
            None,
        ),
        Command::Serve {
            model,
            port,
            engine,
        } => serve_node(
            model.as_deref(),
            port,
            engine,
            paths,
            &mut stdout,
            &mut stderr,
        ),
        Command::Ps => print_managed_servers(paths, &mut stdout),
        Command::Stop { target } => stop_managed_servers(&target, paths, &mut stdout, &mut stderr),
    };

    finish_cli_result(result, &mut stderr)
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
        "id",
        "params",
        "quant",
        "size GB",
        "license",
        "status",
    )?;

    for (entry, size, status) in rows {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id,
            entry.params,
            entry.quant,
            size,
            entry.license,
            status,
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
            if download::model_dir().join(&entry.filename).exists() { "downloaded" } else { "not downloaded" },
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
    use loxa_core::supervisor::LogDrainingChild;
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::io::Read;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering};
    use std::sync::Arc;
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    static MLX_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    struct TestEnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl TestEnvRestore {
        fn set(values: &[(&'static str, &std::ffi::OsStr)]) -> Self {
            let previous = values
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(name)))
                .collect();
            for (name, value) in values {
                unsafe { std::env::set_var(name, value) };
            }
            Self(previous)
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    impl Drop for TestEnvRestore {
        fn drop(&mut self) {
            for (name, value) in self.0.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(name, value) },
                    None => unsafe { std::env::remove_var(name) },
                }
            }
        }
    }

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

    fn read_test_http_request(stream: &mut std::net::TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set request timeout");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                break;
            }
        }
        String::from_utf8(request).expect("request is utf8")
    }

    fn respond_test_http(stream: &mut std::net::TcpStream, body: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("write response");
        stream.flush().expect("flush response");
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

    fn request_stop_for_test(
        state_path: &Path,
        identity: &supervisor::ManagedRunIdentity,
    ) -> supervisor::ManagedRun {
        let mut run = supervisor::current_runtime_state_run(state_path, identity)
            .expect("read current run before test stop");
        run.stop_requested = true;
        supervisor::update_runtime_state_run_committed(state_path, identity, run)
            .expect("commit test stop")
            .expect("exact test stop")
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
    fn process_identity_wiring_uses_retry_for_owner_and_post_spawn_child_capture() {
        let source = include_str!("../../loxa-node/src/lib.rs");
        let run_model = source
            .split_once("fn run_model")
            .expect("run_model source")
            .1
            .split_once("fn render_post_cleanup_startup_failure")
            .expect("run_model boundary")
            .0;
        let retry_call = ["process_start_time_with", "_retry("].concat();
        let one_shot_call = ["process_start", "_time("].concat();
        let retry_positions = run_model
            .match_indices(&retry_call)
            .map(|(position, _)| position)
            .collect::<Vec<_>>();

        assert_eq!(retry_positions.len(), 2, "owner and child must both retry");
        assert_eq!(run_model.matches(&one_shot_call).count(), 0);

        let loop_position = run_model.find("loop {").expect("run loop");
        assert!(
            retry_positions[0] < loop_position,
            "persistent owner miss must happen before state creation or spawn"
        );

        let spawned_position = run_model
            .find("let (starting_run, mut child) = match spawn")
            .expect("managed child spawn boundary");
        let attachment_position = run_model
            .find("persist_managed_server_or_cleanup")
            .expect("managed child attachment boundary");
        assert!(spawned_position < retry_positions[1]);
        assert!(retry_positions[1] < attachment_position);
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
    fn serve_unknown_explicit_model_includes_pull_guidance() {
        let temp = TempDir::new("serve-selection");

        let error = match select_serve_model(temp.path(), Some("not-in-registry")) {
            Ok(_) => panic!("unknown model unexpectedly selected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("loxa pull not-in-registry"));
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
        assert!(Cli::try_parse_from([
            "loxa",
            "run",
            "gemma-3-4b-it-q4",
            "--engine",
            "not-an-engine",
        ])
        .is_err());
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
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("error: provider prerequisite failed: provider is unreachable"));
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
        assert!(String::from_utf8(output)
            .unwrap()
            .contains("candidate A: failed — qualification_failed"));
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
    fn python_gateway_metadata_uses_default_model_and_pinned_mlx_identity() {
        let backend = ResolvedRuntimeBackend {
            kind: RuntimeBackendKind::PyMlxLm,
            model_id: "/tmp/mlx model".to_string(),
            model_path: PathBuf::from("/tmp/mlx model"),
            program: PathBuf::from("/tmp/bin/mlx_lm.server"),
            engine_version: "0.31.3".to_string(),
        };
        let spec = backend.launch_spec(8123, supervisor::DEFAULT_CTX_TOKENS, "ignored-g0");

        let target = gateway_target(&backend, &spec);

        assert_eq!(target.backend_alias, "default_model");
        assert_eq!(target.engine, "mlx-lm");
        assert_eq!(target.engine_version, "0.31.3");
        assert_eq!(target.model_id, "/tmp/mlx model");
        assert_eq!(target.profile, "mlx-lm:/tmp/mlx model");
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

        let error = serve_node(
            None,
            Some(0),
            RuntimeBackendKind::PyMlxLm,
            &paths,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("Python serve needs --model");

        assert!(error.to_string().contains("--model <local-directory>"));
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
    fn childless_spawn_error_cleanup_conflict_preserves_a_newer_generation() {
        let temp = TempDir::new("loxa-pre-spawn-cleanup-conflict");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let starting_identity = run.identity();
        let mut newer_generation = run.clone();
        newer_generation.generation = 1;
        newer_generation.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish starting run");
        assert!(supervisor::update_runtime_state_run(
            &state_path,
            &starting_identity,
            newer_generation.clone(),
        )
        .expect("advance generation before stale cleanup"));

        let error = finish_childless_owner_error(&state_path, &run, SupervisorError::NoFreePort)
            .expect_err("cleanup conflict must replace the spawn error");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved newer state"),
            RuntimeStateRead::Loaded(vec![newer_generation])
        );
    }

    #[test]
    fn published_replacement_resolution_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-resolution-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Err::<(), _>(SupervisorError::NoFreePort),
            || -> Result<(), SupervisorError> {
                panic!("detection must not run after resolution failure")
            },
        )
        .expect_err("resolution failure");

        assert!(matches!(error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_detection_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-detection-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Err::<(), _>(SupervisorError::LlamaServerNotFound),
        )
        .expect_err("detection failure");

        assert!(matches!(error, SupervisorError::LlamaServerNotFound));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_resolution_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-resolution");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || -> Result<(), SupervisorError> { panic!("detection must not run after interrupt") },
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_detection_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-detection");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Ok("detected"),
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_concurrent_stop_and_interrupt_prefers_requested_stop() {
        let temp = TempDir::new("loxa-replacement-stop-interrupt");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let identity = run.identity();
        let spawn_count = Cell::new(0_u8);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || {
                request_stop_for_test(&state_path, &identity);
                Ok("resolved")
            },
            || Ok("detected"),
        )
        .expect("requested stop outcome");
        if matches!(outcome, OwnedReplacementPreparation::Prepared { .. }) {
            spawn_count.set(spawn_count.get() + 1);
        }

        assert!(matches!(
            outcome,
            OwnedReplacementPreparation::RequestedStop
        ));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn replacement_reservation_failure_exact_finishes_childless_state_and_stop_still_wins() {
        let ordinary_temp = TempDir::new("loxa-replacement-reservation-failure");
        let ordinary_state_path = ordinary_temp.path().join("managed.json");
        let ordinary_blocker =
            TcpListener::bind(("127.0.0.1", 0)).expect("block ordinary replacement port");
        let ordinary_port = ordinary_blocker
            .local_addr()
            .expect("ordinary blocker address")
            .port();
        let mut ordinary_run = starting_run_for_test(&ordinary_state_path, "run-ordinary");
        ordinary_run.generation = 1;
        ordinary_run.generation_alias = "loxa-run-ordinary-g1".to_string();
        ordinary_run.port = ordinary_port;
        supervisor::create_starting_run(&ordinary_state_path, ordinary_run.clone())
            .expect("publish ordinary replacement");
        let reservation_error = match supervisor::reserve_localhost_port(Some(ordinary_port)) {
            Err(error) => error,
            Ok(_) => panic!("blocked port must reject a replacement reservation"),
        };
        let ordinary_error =
            finish_childless_owner_error(&ordinary_state_path, &ordinary_run, reservation_error)
                .expect_err("blocked replacement reservation must fail");

        assert!(matches!(ordinary_error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&ordinary_state_path)
                .expect("read ordinary terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        let stopped_temp = TempDir::new("loxa-stopped-replacement-reservation-failure");
        let stopped_state_path = stopped_temp.path().join("managed.json");
        let stopped_blocker =
            TcpListener::bind(("127.0.0.1", 0)).expect("block stopped replacement port");
        let stopped_port = stopped_blocker
            .local_addr()
            .expect("stopped blocker address")
            .port();
        let mut stopped_run = starting_run_for_test(&stopped_state_path, "run-stopped");
        stopped_run.generation = 1;
        stopped_run.generation_alias = "loxa-run-stopped-g1".to_string();
        stopped_run.port = stopped_port;
        supervisor::create_starting_run(&stopped_state_path, stopped_run.clone())
            .expect("publish stopped replacement");
        let stopped_identity = stopped_run.identity();
        request_stop_for_test(&stopped_state_path, &stopped_identity);
        let reservation_error = match supervisor::reserve_localhost_port(Some(stopped_port)) {
            Err(error) => error,
            Ok(_) => panic!("blocked port must reject a stopped replacement reservation"),
        };
        let stopped_exit =
            finish_childless_owner_error(&stopped_state_path, &stopped_run, reservation_error)
                .expect("durable stop must win over replacement reservation failure");

        assert_eq!(stopped_exit, ExitCode::SUCCESS);
        assert_eq!(
            supervisor::read_runtime_state(&stopped_state_path)
                .expect("read stopped terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn attachment_boundary_stop_during_and_immediately_after_tears_down_once_without_generation_two(
    ) {
        for stop_during_attachment in [true, false] {
            let temp = TempDir::new("loxa-attachment-stop-boundary");
            let state_path = temp.path().join("managed.json");
            let mut starting = starting_run_for_test(&state_path, "run-1");
            starting.generation = 1;
            starting.generation_alias = "loxa-run-1-g1".to_string();
            supervisor::create_starting_run(&state_path, starting.clone())
                .expect("publish generation one");
            if stop_during_attachment {
                starting = request_stop_for_test(&state_path, &starting.identity());
            }
            let server = ManagedServer {
                id: starting.model_id.clone(),
                pid: 778,
                port: starting.port,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(222),
            };
            let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
            let attached = supervisor::persist_managed_server_or_cleanup(
                &mut child,
                &state_path,
                starting,
                server,
                Duration::from_millis(10),
            )
            .expect("attach replacement");
            let supervisor::PersistManagedServerOutcome::Attached(attached) = attached else {
                panic!("replacement attachment must remain owned");
            };
            let events = RefCell::new(Vec::new());
            let identity = attached.identity();

            let outcome = observe_attached_stop_with(
                &mut child,
                &state_path,
                &attached.identity(),
                || {
                    events.borrow_mut().push("after_attachment");
                    if !stop_during_attachment {
                        request_stop_for_test(&state_path, &identity);
                    }
                },
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    supervisor::TeardownConfirmation::Confirmed
                },
            )
            .expect("attachment stop outcome");

            assert_eq!(
                outcome,
                Some(supervisor::OwnerTerminalOutcome::RequestedStop)
            );
            assert_eq!(
                events.into_inner(),
                vec!["after_attachment", "requested_stop_teardown"]
            );
            assert_eq!(
                supervisor::read_runtime_state(&state_path).expect("read terminal state"),
                RuntimeStateRead::Loaded(Vec::new())
            );
        }
    }

    #[test]
    fn post_spawn_interrupt_before_attachment_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-pre-attach-interrupt");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        supervisor::create_starting_run(&state_path, run.clone()).expect("create starting run");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = finish_spawned_interrupt(&mut child, &state_path, &run.identity())
            .expect("interrupt cleanup outcome");

        assert_eq!(outcome, supervisor::OwnerTerminalOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved starting run"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn post_spawn_initialization_failure_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-initialization-failure");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        supervisor::create_starting_run(&state_path, run.clone()).expect("create starting run");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = finish_spawn_initialization(
            &mut child,
            &state_path,
            &run.identity(),
            Some(SupervisorError::Io(io::Error::other(
                "injected drain initialization failure",
            ))),
        )
        .expect("initialization cleanup outcome");

        assert_eq!(
            outcome,
            Some(supervisor::PostSpawnCleanupOutcome::RecoveryRequired)
        );
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved starting run"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn post_spawn_immediate_attachment_reread_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-attachment-reread");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt state after attachment");
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = observe_attached_stop(&mut child, &state_path, &run.identity())
            .expect("reread recovery outcome");

        assert_eq!(
            outcome,
            Some(supervisor::OwnerTerminalOutcome::RecoveryRequired)
        );
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_startup_state_read_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-startup-state-read");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt startup state");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            None,
            |_, _, _| panic!("state read error must precede readiness polling"),
        )
        .expect("startup recovery outcome");

        assert_eq!(outcome, StartupWaitOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_startup_readiness_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-startup-readiness");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![None, Some(0)]);

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            None,
            |_, _, _| Err(SupervisorError::NoFreePort),
        )
        .expect("startup recovery outcome");

        assert_eq!(outcome, StartupWaitOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn python_startup_accepts_completion_slower_than_llama_poll_interval() {
        let temp = TempDir::new("loxa-python-slow-readiness");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fake MLX server");
        let port = listener.local_addr().expect("fake MLX address").port();
        let exited = Arc::new(TestAtomicBool::new(false));
        let server_exit = Arc::clone(&exited);
        let server_thread = std::thread::spawn(move || {
            let (mut health, _) = listener.accept().expect("accept health request");
            read_test_http_request(&mut health);
            respond_test_http(&mut health, r#"{"status":"ok"}"#);

            let (mut completion, _) = listener.accept().expect("accept completion request");
            read_test_http_request(&mut completion);
            std::thread::sleep(Duration::from_millis(400));
            respond_test_http(
                &mut completion,
                r#"{"choices":[{"message":{"content":"ok"}}]}"#,
            );
            std::thread::sleep(Duration::from_millis(500));
            server_exit.store(true, TestOrdering::SeqCst);
        });
        let server = ManagedServer {
            id: "/tmp/mlx-model".to_string(),
            pid: 777,
            port,
            model_path: PathBuf::from("/tmp/mlx-model"),
            started_at_unix_s: 789,
            llama_server_version: "0.31.3".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_shared_exit(exited);
        let mut worker = supervisor::spawn_chat_completion_readiness_worker(
            port,
            "default_model".to_string(),
            supervisor::HEALTH_TIMEOUT,
            supervisor::HEALTH_POLL_INTERVAL,
        )
        .expect("spawn slow readiness worker");

        let outcome = wait_for_startup_owned(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            Some(&mut worker),
            |_, _, _| panic!("worker owns Python readiness"),
        );
        server_thread.join().expect("join fake MLX server");

        assert_eq!(
            outcome.expect("slow completion readiness"),
            StartupWaitOutcome::Ready
        );
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn py_mlx_restart_helper() {
        if std::env::var_os("LOXA_MLX_RESTART_CHILD").as_deref() != Some(std::ffi::OsStr::new("1"))
        {
            return;
        }
        let Some(port) = std::env::var_os("LOXA_MLX_RESTART_HELPER_PORT") else {
            return;
        };
        let port = port.to_string_lossy().parse::<u16>().expect("helper port");
        let generation =
            std::env::var("LOXA_MLX_RESTART_HELPER_GENERATION").expect("helper generation");
        let requests_path = PathBuf::from(
            std::env::var_os("LOXA_MLX_RESTART_REQUESTS").expect("helper requests path"),
        );
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind restart helper");

        let (mut health, _) = listener.accept().expect("accept restart health");
        let health_request = read_test_http_request(&mut health);
        assert!(health_request.starts_with("GET /health "));
        respond_test_http(&mut health, r#"{"status":"ok"}"#);

        let (mut completion, _) = listener.accept().expect("accept restart completion");
        let completion_request = read_test_http_request(&mut completion);
        assert!(completion_request.starts_with("POST /v1/chat/completions "));
        assert!(completion_request.contains(r#""model":"default_model""#));
        let mut requests = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(requests_path)
            .expect("open readiness evidence");
        writeln!(requests, "generation={generation} health completion")
            .expect("write readiness evidence");
        if let Some(marker) = std::env::var_os("LOXA_MLX_RESTART_MARKER") {
            fs::write(marker, generation.as_bytes()).expect("write completion marker");
        }
        let delay = std::env::var("LOXA_MLX_RESTART_DELAY_MS")
            .expect("helper delay")
            .parse::<u64>()
            .expect("numeric helper delay");
        if delay > 0 {
            completion
                .set_read_timeout(Some(Duration::from_millis(50)))
                .expect("set cancellation poll timeout");
            let deadline = std::time::Instant::now() + Duration::from_millis(delay);
            let mut byte = [0_u8; 1];
            while std::time::Instant::now() < deadline {
                match completion.read(&mut byte) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(_) => return,
                }
            }
        }
        respond_test_http(
            &mut completion,
            r#"{"choices":[{"message":{"content":"ok"}}]}"#,
        );
        std::thread::sleep(Duration::from_millis(300));
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn actual_run_model_restarts_python_once_with_same_backend_plan() {
        use std::os::unix::fs::PermissionsExt;

        let _lock = MLX_ENV_LOCK.lock().expect("MLX environment lock");
        let temp = TempDir::new("loxa-python-actual-restart");
        let bin_dir = temp.path().join("fake bin");
        let model_dir = temp.path().join("mlx model with spaces");
        let wrapper = bin_dir.join("mlx_lm.server");
        let version = bin_dir.join("mlx_lm");
        let count_path = temp.path().join("generation-count");
        let args_path = temp.path().join("launch-args");
        let requests_path = temp.path().join("readiness-requests");
        let marker_path = temp.path().join("completion-marker");
        fs::create_dir_all(&bin_dir).expect("create fake bin");
        fs::create_dir_all(&model_dir).expect("create fake model");
        fs::write(
            &wrapper,
            r#"#!/bin/sh
count=0
if [ -f "$LOXA_MLX_RESTART_COUNT" ]; then
  count=$(<"$LOXA_MLX_RESTART_COUNT")
fi
count=$((count + 1))
printf '%s\n' "$count" > "$LOXA_MLX_RESTART_COUNT"
printf 'generation=%s\n' "$count" >> "$LOXA_MLX_RESTART_ARGS"
for arg in "$@"; do
  printf 'arg=%s\n' "$arg" >> "$LOXA_MLX_RESTART_ARGS"
done
port=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = '--port' ]; then
    shift
    port="$1"
  fi
  shift
done
LOXA_MLX_RESTART_HELPER_PORT="$port" \
LOXA_MLX_RESTART_HELPER_GENERATION="$count" \
LOXA_MLX_RESTART_CHILD="1" \
"$LOXA_MLX_RESTART_TEST_EXE" --exact tests::py_mlx_restart_helper --nocapture
"#,
        )
        .expect("write fake server");
        fs::write(&version, "#!/bin/sh\nprintf '0.31.3\\n'\n").expect("write fake version command");
        for path in [&wrapper, &version] {
            let mut permissions = fs::metadata(path).expect("fake metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("make fake executable");
        }
        let test_exe = std::env::current_exe().expect("test executable");
        let _environment = TestEnvRestore::set(&[
            ("LOXA_MLX_LM_SERVER", wrapper.as_os_str()),
            ("LOXA_MLX_RESTART_COUNT", count_path.as_os_str()),
            ("LOXA_MLX_RESTART_ARGS", args_path.as_os_str()),
            ("LOXA_MLX_RESTART_REQUESTS", requests_path.as_os_str()),
            ("LOXA_MLX_RESTART_TEST_EXE", test_exe.as_os_str()),
            ("LOXA_MLX_RESTART_MARKER", marker_path.as_os_str()),
            ("LOXA_MLX_RESTART_DELAY_MS", std::ffi::OsStr::new("0")),
        ]);
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_model(
            RunRequest {
                id: model_dir.to_str().expect("utf8 model path"),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
            &paths,
            &mut stdout,
            &mut stderr,
            None,
        )
        .expect("run fake Python engine");

        assert_eq!(
            exit,
            ExitCode::from(1),
            "generation one must exhaust restart"
        );
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert_eq!(
            stdout
                .matches("mlx_lm.server exited unexpectedly; restarting once...")
                .count(),
            1,
            "{stdout}"
        );
        assert_eq!(stdout.matches("model id:").count(), 2, "{stdout}");
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(
            stderr.contains("mlx_lm.server exited unexpectedly"),
            "{stderr}"
        );

        let canonical_model = fs::canonicalize(&model_dir).expect("canonical model");
        let args = fs::read_to_string(&args_path).expect("read launch arguments");
        let generations = args
            .split("generation=")
            .filter(|block| !block.is_empty())
            .map(|block| {
                let mut lines = block.lines();
                let generation = lines.next().expect("generation number").to_string();
                let argv = lines
                    .map(|line| {
                        line.strip_prefix("arg=")
                            .expect("only argv evidence follows generation")
                            .to_string()
                    })
                    .collect::<Vec<_>>();
                (generation, argv)
            })
            .collect::<Vec<_>>();
        assert_eq!(generations.len(), 2, "{args}");
        assert_eq!(generations[0].0, "1");
        assert_eq!(generations[1].0, "2");
        for (_, argv) in &generations {
            assert_eq!(argv.len(), 6, "unexpected extra argv: {argv:?}");
            assert_eq!(argv[0], "--model");
            assert_eq!(argv[1], canonical_model.display().to_string());
            assert_eq!(argv[2], "--host");
            assert_eq!(argv[3], "127.0.0.1");
            assert_eq!(argv[4], "--port");
            assert!(argv[5].parse::<u16>().is_ok(), "invalid port: {argv:?}");
        }
        assert_eq!(generations[0].1, generations[1].1);
        let requests = fs::read_to_string(&requests_path).expect("read readiness evidence");
        assert!(
            requests.contains("generation=1 health completion"),
            "{requests}"
        );
        assert!(
            requests.contains("generation=2 health completion"),
            "{requests}"
        );
        assert_eq!(
            supervisor::read_runtime_state(&paths.state_path).expect("read final state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        for path in [&count_path, &args_path, &requests_path, &marker_path] {
            let _ = fs::remove_file(path);
        }
        unsafe { std::env::set_var("LOXA_MLX_RESTART_DELAY_MS", "5000") };
        let stop_paths = NodePaths {
            models_dir: temp.path().join("models-stop"),
            state_path: temp.path().join("managed-stop.json"),
            logs_dir: temp.path().join("logs-stop"),
        };
        let stop_state = stop_paths.state_path.clone();
        let stop_marker = marker_path.clone();
        let stop_thread = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !stop_marker.is_file() {
                assert!(std::time::Instant::now() < deadline, "stop marker timeout");
                std::thread::sleep(Duration::from_millis(10));
            }
            supervisor::request_managed_stop(&stop_state, "all").expect("request external stop")
        });
        let mut stop_stdout = Vec::new();
        let mut stop_stderr = Vec::new();
        let stop_started = std::time::Instant::now();
        let stop_exit = run_model(
            RunRequest {
                id: model_dir.to_str().expect("utf8 model path"),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
            &stop_paths,
            &mut stop_stdout,
            &mut stop_stderr,
            None,
        )
        .expect("stop stalled Python readiness");
        let stop_outcome = stop_thread.join().expect("join external stop request");
        assert_eq!(stop_exit, ExitCode::SUCCESS);
        assert!(
            matches!(
                stop_outcome,
                supervisor::StopRequestOutcome::Completed { .. }
            ),
            "{stop_outcome:?}"
        );
        assert!(
            stop_started.elapsed() < Duration::from_secs(2),
            "external stop was blocked for {:?}",
            stop_started.elapsed()
        );
        assert_eq!(
            fs::read_to_string(&count_path)
                .expect("stop generation count")
                .trim(),
            "1"
        );
        assert_eq!(
            supervisor::read_runtime_state(&stop_paths.state_path).expect("read stopped state"),
            RuntimeStateRead::Loaded(Vec::new())
        );

        for path in [&count_path, &args_path, &requests_path, &marker_path] {
            let _ = fs::remove_file(path);
        }
        let interrupt_paths = NodePaths {
            models_dir: temp.path().join("models-interrupt"),
            state_path: temp.path().join("managed-interrupt.json"),
            logs_dir: temp.path().join("logs-interrupt"),
        };
        let interrupt_marker = marker_path.clone();
        let interrupt_thread = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !interrupt_marker.is_file() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "interrupt marker timeout"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            set_ctrl_c_received();
        });
        let mut interrupt_stdout = Vec::new();
        let mut interrupt_stderr = Vec::new();
        let interrupt_started = std::time::Instant::now();
        let interrupt_exit = run_model(
            RunRequest {
                id: model_dir.to_str().expect("utf8 model path"),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
            &interrupt_paths,
            &mut interrupt_stdout,
            &mut interrupt_stderr,
            None,
        )
        .expect("interrupt stalled Python readiness");
        interrupt_thread.join().expect("join interrupt request");
        clear_ctrl_c_received();
        assert_eq!(interrupt_exit, ExitCode::from(130));
        assert!(
            interrupt_started.elapsed() < Duration::from_secs(2),
            "interrupt was blocked for {:?}",
            interrupt_started.elapsed()
        );
        assert_eq!(
            fs::read_to_string(&count_path)
                .expect("interrupt generation count")
                .trim(),
            "1"
        );
        assert_eq!(
            supervisor::read_runtime_state(&interrupt_paths.state_path)
                .expect("read interrupted state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn post_spawn_ready_writer_failure_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-ready-writer");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
        let mut stdout = BrokenPipeWriter::new(|| {});

        let outcome = print_run_ready_owned(
            &mut stdout,
            &server,
            &mut child,
            &state_path,
            &run.identity(),
        )
        .expect("ready output recovery outcome");

        assert_eq!(outcome, ReadyOutputOutcome::RecoveryRequired);
        assert_eq!(stdout.write_attempts, 1);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_running_state_read_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-running-state-read");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&state_path, b"{corrupt").expect("corrupt running state");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut stdout,
            &mut stderr,
        )
        .expect("running recovery outcome");

        assert_eq!(outcome, RunOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn post_spawn_running_try_wait_error_invokes_unified_teardown_once() {
        let temp = TempDir::new("loxa-post-spawn-running-try-wait");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_error_then(vec![Some(0)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let outcome = supervise_running_server(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            None,
            "llama-server",
            &mut stdout,
            &mut stderr,
        )
        .expect("running recovery outcome");

        assert_eq!(outcome, RunOutcome::RecoveryRequired);
        assert_eq!(
            child
                .events
                .borrow()
                .iter()
                .filter(|event| **event == "terminate")
                .count(),
            1
        );
    }

    #[test]
    fn startup_interrupt_after_state_write_cleans_up_and_returns_130() {
        let temp = TempDir::new("loxa-run-interrupt");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = wait_for_startup(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            |_, _, _| Ok(StartupPoll::Pending),
            owner_teardown_child,
        )
        .expect("startup wait outcome");

        assert_eq!(outcome, StartupWaitOutcome::Interrupted);
        assert!(child.events.borrow().contains(&"terminate"));
        assert!(child.events.borrow().contains(&"join_log_drains"));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("runtime state after interrupt"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn startup_polling_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-startup-stop-poll");
            let state_path = temp.path().join("managed.json");
            let server = ManagedServer {
                id: "gemma-3-4b-it-q4".to_string(),
                pid: 777,
                port: 8081,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(111),
            };
            let run = persist_run_for_server(&state_path, &server);
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
            let events = RefCell::new(Vec::new());
            let identity = run.identity();

            let outcome = wait_for_startup(
                &mut child,
                &identity,
                &state_path,
                &signal,
                |_, _, _| {
                    events.borrow_mut().push("startup_poll");
                    request_stop_for_test(&state_path, &identity);
                    Ok(StartupPoll::Pending)
                },
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    confirmation
                },
            )
            .expect("startup stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                StartupWaitOutcome::RequestedStop
            } else {
                StartupWaitOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert_eq!(
                events.into_inner(),
                vec!["startup_poll", "requested_stop_teardown"]
            );
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read startup state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_loop_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-running-stop-poll");
            let state_path = temp.path().join("managed.json");
            let server = ManagedServer {
                id: "gemma-3-4b-it-q4".to_string(),
                pid: 777,
                port: 8081,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(111),
            };
            let run = persist_run_for_server(&state_path, &server);
            let stopped = request_stop_for_test(&state_path, &run.identity());
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results(vec![None]);
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let events = RefCell::new(Vec::new());

            let outcome = supervise_running_server_with(
                RunSession {
                    id: &server.id,
                    state_identity: &stopped.identity(),
                    log_path: stopped.log_path.as_path(),
                    state_path: &state_path,
                },
                &mut child,
                &signal,
                &mut stdout,
                &mut stderr,
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    confirmation
                },
                |_| panic!("running loop must return before sleeping"),
            )
            .expect("running stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                RunOutcome::RequestedStop
            } else {
                RunOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert_eq!(events.into_inner(), vec!["requested_stop_teardown"]);
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read running state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_reaped_exit_with_concurrent_stop_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-stop");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let identity = run.identity();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &identity,
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            |child, decision| {
                assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                request_stop_for_test(&state_path, &identity);
                owner_teardown_child(child, decision)
            },
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("reaped stop outcome");

        assert_eq!(outcome, RunOutcome::RequestedStop);
        assert_eq!(
            child.events.into_inner(),
            vec!["try_wait", "join_log_drains"]
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_reaped_exit_with_concurrent_interrupt_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-interrupt");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            owner_teardown_child,
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("reaped interrupt outcome");

        assert_eq!(outcome, RunOutcome::Interrupted);
        assert_eq!(
            child.events.into_inner(),
            vec!["try_wait", "join_log_drains"]
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_restart_announcement_broken_pipe_retains_handoff_and_stop_wins() {
        let temp = TempDir::new("loxa-running-restart-broken-pipe");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let generation_one_identity = supervisor::ManagedRunIdentity {
            run_id: run.run_id.clone(),
            generation: 1,
            child_pid: None,
            child_process_start_time_unix_s: None,
        };
        let signal = FakeInterruptSource::new(vec![false, false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = BrokenPipeWriter::new(|| {
            request_stop_for_test(&state_path, &generation_one_identity);
        });
        let mut stderr = Vec::new();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            owner_teardown_child,
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("restart announcement BrokenPipe must not drop the owned handoff");
        let RunOutcome::Restart { run: replacement } = outcome else {
            panic!("expected owned replacement handoff, got {outcome:?}");
        };
        let exit =
            finish_childless_owner_error(&state_path, &replacement, SupervisorError::NoFreePort)
                .expect("committed stop must win after the non-fatal announcement");

        assert_eq!(exit, ExitCode::SUCCESS);
        assert_eq!(stdout.write_attempts, 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn startup_finalization_error_after_teardown_does_not_retry_cleanup() {
        let temp = TempDir::new("loxa-startup-finalization-error");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let stopped = request_stop_for_test(&state_path, &run.identity());
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
        let finalizations = Cell::new(0_u8);

        let error = wait_for_startup_with_finalizer(
            &mut child,
            &stopped.identity(),
            &state_path,
            &signal,
            |_, _, _| panic!("durable stop must win before startup polling"),
            owner_teardown_child,
            |_, _, _, confirmation| {
                assert_eq!(confirmation, supervisor::TeardownConfirmation::Confirmed);
                finalizations.set(finalizations.get() + 1);
                Err(SupervisorError::Io(io::Error::other(
                    "injected exact-finalization failure",
                )))
            },
        )
        .expect_err("finalization failure");

        assert!(matches!(error, StartupWaitFailure::AfterTeardown(_)));
        assert_eq!(finalizations.get(), 1);
        assert_eq!(
            child.events.into_inner(),
            vec!["terminate", "try_wait", "join_log_drains"]
        );
        let RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).expect("read preserved stopped state")
        else {
            panic!("expected loaded state");
        };
        assert_eq!(runs, vec![stopped]);
    }

    fn assert_startup_reaped_diagnostic_failure_never_resignals(drain_fails: bool) {
        let temp = TempDir::new("loxa-startup-reaped-diagnostics");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 65_535,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        if drain_fails {
            fs::write(
                &run.log_path,
                b"diagnostics exist but drain joining failed\n",
            )
            .expect("write crash log for drain failure");
        }
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child =
            FakeStartupChild::with_wait_results_and_drain_error(vec![Some(1)], drain_fails);

        let failure = wait_for_startup(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            |child, _, _| match supervisor::wait_for_generation_ready_or_exit(
                child,
                server.port,
                &run.generation_alias,
                Duration::ZERO,
                Duration::ZERO,
            ) {
                Ok(()) => Ok(StartupPoll::Ready),
                Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                Err(error) => Err(error),
            },
            owner_teardown_child,
        )
        .expect_err("reaped child diagnostic failure");

        let observed_exit = match failure {
            StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            } => Some(
                supervisor::decide_observed_child_exit(
                    diagnostics_error
                        .map(|error| format!("crash diagnostics unavailable: {error}"))
                        .unwrap_or(log_tail),
                    &state_path,
                    &run.identity(),
                    &signal,
                    |decision| owner_teardown_child(&mut child, decision),
                )
                .expect("transition already-reaped startup child"),
            ),
            StartupWaitFailure::BeforeTeardown(_) => {
                let _ = supervisor::cleanup_after_ctrl_c(
                    &mut child,
                    &state_path,
                    &run.identity(),
                    supervisor::CTRL_C_GRACE_PERIOD,
                );
                let _ = child.join_log_drains();
                None
            }
            StartupWaitFailure::AfterTeardown(_) => None,
        };

        let events = child.events.into_inner();
        assert_eq!(
            events.iter().filter(|event| **event == "terminate").count(),
            0,
            "an already-reaped PID must never be terminated again: {events:?}"
        );
        assert_eq!(
            events.iter().filter(|event| **event == "kill").count(),
            0,
            "an already-reaped PID must never be killed again: {events:?}"
        );
        if drain_fails {
            assert_eq!(observed_exit, Some(ObservedChildExit::RecoveryRequired));
            assert!(matches!(
                supervisor::read_runtime_state(&state_path).expect("read preserved state"),
                RuntimeStateRead::Loaded(runs) if runs.len() == 1
            ));
        } else {
            assert!(matches!(
                observed_exit,
                Some(ObservedChildExit::Restart { .. })
            ));
        }
    }

    #[test]
    fn startup_reaped_drain_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(true);
    }

    #[test]
    fn startup_reaped_log_tail_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(false);
    }

    #[test]
    fn startup_restart_announcement_broken_pipe_retains_handoff_and_cas_conflict_wins() {
        let temp = TempDir::new("loxa-startup-restart-broken-pipe");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let observed = supervisor::decide_observed_child_exit(
            "startup crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |decision| {
                assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                supervisor::TeardownConfirmation::Confirmed
            },
        )
        .expect("publish owned generation one");
        let ObservedChildExit::Restart { run: replacement } = observed else {
            panic!("expected replacement handoff, got {observed:?}");
        };
        let replacement_identity = replacement.identity();
        let newer_run = starting_run_for_test(&state_path, "newer-run");
        let newer_for_write = newer_run.clone();
        let mut stdout = BrokenPipeWriter::new(|| {
            assert!(
                supervisor::finish_runtime_state_run(&state_path, &replacement_identity)
                    .expect("exact-finish generation one during failed announcement")
            );
            supervisor::create_starting_run(&state_path, newer_for_write)
                .expect("publish newer state during failed announcement");
        });

        let replacement = retain_restart_after_best_effort_announcement(
            &mut stdout,
            "llama-server exited before becoming healthy; restarting once...",
            replacement,
        );
        let error =
            finish_childless_owner_error(&state_path, &replacement, SupervisorError::NoFreePort)
                .expect_err("newer exact state must beat the stale generation-one handoff");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(stdout.write_attempts, 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read newer state"),
            RuntimeStateRead::Loaded(vec![newer_run])
        );
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

        let exit = stop_managed_servers(&run.model_id, &paths, &mut stdout, &mut stderr)
            .expect("stop command result");

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr)
            .expect("stderr utf8")
            .contains("recovery required"));
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

    #[test]
    fn ctrl_c_flag_helpers_round_trip() {
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let _lock = MLX_ENV_LOCK.lock().expect("process-global test lock");
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());

        set_ctrl_c_received();
        assert!(ctrl_c_received());

        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
    }

    #[test]
    fn truthful_owner_terminal_exit_codes_never_map_unconfirmed_cleanup_to_130() {
        assert_eq!(
            owner_terminal_exit_code(supervisor::OwnerTerminalOutcome::RequestedStop),
            ExitCode::SUCCESS
        );
        assert_eq!(
            owner_terminal_exit_code(supervisor::OwnerTerminalOutcome::Interrupted),
            ExitCode::from(130)
        );
        assert_eq!(
            owner_terminal_exit_code(supervisor::OwnerTerminalOutcome::RecoveryRequired),
            ExitCode::from(1)
        );
    }

    #[test]
    fn attachment_requested_stop_outcome_maps_to_exit_0() {
        let mut stderr = Vec::new();

        let boundary = resolve_managed_attachment(
            supervisor::PersistManagedServerOutcome::RequestedStop,
            &mut stderr,
            "run-1",
        )
        .expect("map requested-stop attachment outcome");

        assert!(matches!(
            boundary,
            ManagedAttachmentBoundary::Terminal(exit) if exit == ExitCode::SUCCESS
        ));
        assert!(stderr.is_empty());
    }

    #[test]
    fn unconfirmed_requested_stop_exits_1_and_preserves_state() {
        let temp = TempDir::new("loxa-unconfirmed-requested-stop-exit");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let stopped = request_stop_for_test(&state_path, &run.identity());
        let teardown_calls = Cell::new(0_u8);

        let outcome = supervisor::finish_owner_teardown_with(
            &state_path,
            &stopped.identity(),
            supervisor::OwnerTeardownDecision::RequestedStop,
            |_| {
                teardown_calls.set(teardown_calls.get() + 1);
                supervisor::TeardownConfirmation::Unconfirmed
            },
        )
        .expect("unconfirmed stop outcome");

        assert_eq!(outcome, supervisor::OwnerTerminalOutcome::RecoveryRequired);
        assert_eq!(owner_terminal_exit_code(outcome), ExitCode::from(1));
        assert_eq!(teardown_calls.get(), 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved stopped state"),
            RuntimeStateRead::Loaded(vec![stopped])
        );
    }

    #[test]
    fn unconfirmed_generation_zero_unexpected_exit_exits_1_without_restart_and_preserves_exact_state(
    ) {
        let temp = TempDir::new("loxa-unconfirmed-generation-zero-exit");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let teardown_calls = Cell::new(0_u8);

        let outcome = supervisor::decide_observed_child_exit(
            "first crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |_| {
                teardown_calls.set(teardown_calls.get() + 1);
                supervisor::TeardownConfirmation::Unconfirmed
            },
        )
        .expect("unconfirmed generation-zero outcome");

        assert_eq!(outcome, ObservedChildExit::RecoveryRequired);
        assert_eq!(
            observed_terminal_exit_code(&outcome),
            Some(ExitCode::from(1))
        );
        assert_eq!(teardown_calls.get(), 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved generation zero"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn confirmed_second_crash_removes_exact_state_then_exits_1() {
        let temp = TempDir::new("loxa-confirmed-second-crash-exit");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 778,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(222),
        };
        let mut run = persist_run_for_server(&state_path, &server);
        let old_identity = run.identity();
        run.generation = 1;
        run.generation_alias = "loxa-test-run-778-g1".to_string();
        assert!(
            supervisor::update_runtime_state_run(&state_path, &old_identity, run.clone(),)
                .expect("publish generation one")
        );
        let signal = FakeInterruptSource::new(vec![false]);

        let outcome = supervisor::decide_observed_child_exit(
            "second crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |_| supervisor::TeardownConfirmation::Confirmed,
        )
        .expect("confirmed second-crash outcome");

        assert!(matches!(outcome, ObservedChildExit::Exhausted { .. }));
        assert_eq!(
            observed_terminal_exit_code(&outcome),
            Some(ExitCode::from(1))
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read removed generation one"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn post_spawn_startup_timeout_preserves_existing_output_after_unified_teardown() {
        let log_path = PathBuf::from("/tmp/loxa-startup-timeout.log");
        let mut stderr = Vec::new();

        let exit = render_post_cleanup_startup_failure(
            &mut stderr,
            &log_path,
            "llama-server",
            SupervisorError::HealthTimeout,
        )
        .expect("render confirmed startup timeout");

        assert_eq!(exit, ExitCode::from(1));
        assert_eq!(
            String::from_utf8(stderr).expect("utf8 timeout output"),
            format!(
                "llama-server did not become healthy within {} seconds\nlog file: {}\n",
                supervisor::HEALTH_TIMEOUT.as_secs(),
                log_path.display()
            )
        );
    }

    #[test]
    fn recovery_required_exit_writes_guidance_and_status_1() {
        let mut stderr = Vec::new();

        let exit = recovery_required_exit(&mut stderr, "run-1").expect("recovery exit");

        assert_eq!(exit, ExitCode::from(1));
        let stderr = String::from_utf8(stderr).expect("utf8 guidance");
        assert!(stderr.contains("recovery required"));
        assert!(stderr.contains("run-1"));
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

    struct FakeInterruptSource {
        states: Vec<bool>,
        index: Cell<usize>,
    }

    impl FakeInterruptSource {
        fn new(states: Vec<bool>) -> Self {
            Self {
                states,
                index: Cell::new(0),
            }
        }
    }

    impl InterruptSource for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            let index = self.index.get();
            let value = self
                .states
                .get(index)
                .copied()
                .or_else(|| self.states.last().copied())
                .unwrap_or(false);
            if index + 1 < self.states.len() {
                self.index.set(index + 1);
            }
            value
        }
    }

    impl InterruptStatus for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            InterruptSource::interrupted(self)
        }
    }

    struct FakeStartupChild {
        events: RefCell<Vec<&'static str>>,
        wait_results: RefCell<Vec<Option<i32>>>,
        drain_error: bool,
        wait_error_once: Cell<bool>,
        shared_exit: Option<Arc<TestAtomicBool>>,
    }

    impl FakeStartupChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self::with_wait_results_and_drain_error(wait_results, false)
        }

        fn with_wait_results_and_drain_error(
            wait_results: Vec<Option<i32>>,
            drain_error: bool,
        ) -> Self {
            Self {
                events: RefCell::new(Vec::new()),
                wait_results: RefCell::new(wait_results),
                drain_error,
                wait_error_once: Cell::new(false),
                shared_exit: None,
            }
        }

        fn with_shared_exit(shared_exit: Arc<TestAtomicBool>) -> Self {
            let mut child = Self::with_wait_results(vec![None]);
            child.shared_exit = Some(shared_exit);
            child
        }

        fn with_wait_error_then(wait_results: Vec<Option<i32>>) -> Self {
            let child = Self::with_wait_results(wait_results);
            child.wait_error_once.set(true);
            child
        }
    }

    impl ManagedChild for FakeStartupChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.borrow_mut().push("try_wait");
            if self
                .shared_exit
                .as_ref()
                .is_some_and(|exited| exited.load(TestOrdering::SeqCst))
            {
                return Ok(Some(0));
            }
            if self.wait_error_once.replace(false) {
                return Err(io::Error::other("injected try_wait failure"));
            }
            let mut wait_results = self.wait_results.borrow_mut();
            if wait_results.len() > 1 {
                Ok(wait_results.remove(0))
            } else {
                Ok(wait_results.first().copied().unwrap_or(Some(0)))
            }
        }
    }

    impl LogDrainingChild for FakeStartupChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("join_log_drains");
            if self.drain_error {
                Err(SupervisorError::Io(io::Error::other(
                    "injected log-drain join failure",
                )))
            } else {
                Ok(())
            }
        }
    }

    struct BrokenPipeWriter<F> {
        on_write: Option<F>,
        write_attempts: usize,
    }

    impl<F> BrokenPipeWriter<F> {
        fn new(on_write: F) -> Self {
            Self {
                on_write: Some(on_write),
                write_attempts: 0,
            }
        }
    }

    impl<F: FnOnce()> Write for BrokenPipeWriter<F> {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            self.write_attempts += 1;
            if let Some(on_write) = self.on_write.take() {
                on_write();
            }
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected restart announcement failure",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
