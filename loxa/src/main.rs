use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
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
    Doctor,
    Pull { id: String },
    List,
    Rm { id: String },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    run(cli, io::stdout(), io::stderr())
}

fn run<W: Write, E: Write>(cli: Cli, mut stdout: W, mut stderr: E) -> ExitCode {
    let result = match cli.command {
        Command::Doctor => print_doctor(&mut stdout),
        Command::Pull { id } => pull_model(&id, &mut stdout, &mut stderr),
        Command::List => print_list(&mut stdout),
        Command::Rm { id } => remove_model(&id, &mut stdout, &mut stderr),
    };

    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}");
            ExitCode::from(1)
        }
    }
}

fn pull_model<W: Write, E: Write>(
    id: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
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

    Ok(ExitCode::SUCCESS)
}

fn remove_model<W: Write, E: Write>(
    id: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let Some(entry) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    let dir = download::model_dir();
    let removed = remove_model_files(entry, &dir)?;
    if removed.is_empty() {
        writeln!(stdout, "nothing present for {id}")?;
    } else {
        for path in removed {
            writeln!(stdout, "removed {}", path.display())?;
        }
    }

    Ok(ExitCode::SUCCESS)
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
    use loxa_core::registry::REGISTRY;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn clap_parses_all_subcommands() {
        assert!(Cli::try_parse_from(["loxa", "doctor"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "list"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "pull", "gemma-3-4b-it-q4"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "rm", "gemma-3-4b-it-q4"]).is_ok());
    }

    #[test]
    fn unknown_pull_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Pull {
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
