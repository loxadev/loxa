use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::hardware::HardwareReport;

#[derive(Parser)]
#[command(name = "loxa", version, about = "Measured local AI infrastructure")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    Doctor,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Doctor => print_doctor(),
    }
}

fn print_doctor() {
    let hardware = HardwareReport::detect();
    let tools = LocalToolsReport::detect();

    println!("Machine");
    println!("  {:<16} {}", "Chip:", hardware.chip);
    println!(
        "  {:<16} {} physical / {} logical",
        "Cores:", hardware.physical_cores, hardware.logical_cores
    );
    println!(
        "  {:<16} {:.1} GB total / {:.1} GB available / {:.1} GB used",
        "RAM:",
        bytes_to_gb(hardware.ram_total_bytes),
        bytes_to_gb(hardware.ram_available_bytes),
        bytes_to_gb(hardware.ram_used_bytes)
    );
    println!(
        "  {:<16} {:.1} GB total / {:.1} GB used",
        "Swap:",
        bytes_to_gb(hardware.swap_total_bytes),
        bytes_to_gb(hardware.swap_used_bytes)
    );
    println!(
        "  {:<16} {} total / {} available",
        "Disk (/):",
        optional_bytes_to_gb(hardware.root_disk_total_bytes),
        optional_bytes_to_gb(hardware.root_disk_available_bytes)
    );
    println!(
        "  {:<16} {} {}",
        "OS:", hardware.os_name, hardware.os_version
    );
    println!();
    println!("Detected tools");
    for tool in &tools.tools {
        print_tool(tool);
    }
}

fn print_tool(tool: &DetectedTool) {
    let detection = &tool.detection;
    let evidence = if detection.evidence.is_empty() {
        "unknown".to_string()
    } else {
        detection.evidence.join("; ")
    };

    println!(
        "  {:<10} {:<13} {:<11} {}",
        tool.name, detection.install_state, detection.run_state, evidence
    );
}

fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn optional_bytes_to_gb(bytes: Option<u64>) -> String {
    bytes
        .map(|bytes| format!("{:.1} GB", bytes_to_gb(bytes)))
        .unwrap_or_else(|| "unknown".to_string())
}
