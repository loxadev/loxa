use std::path::Path;

#[cfg(target_os = "macos")]
use std::process::Command;

use sysinfo::{CpuRefreshKind, DiskRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};

#[derive(Debug, Clone)]
pub struct HardwareReport {
    pub chip: String,
    pub physical_cores: usize,
    pub logical_cores: usize,
    pub ram_total_bytes: u64,
    pub ram_available_bytes: u64,
    pub ram_used_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_used_bytes: u64,
    pub root_disk_total_bytes: Option<u64>,
    pub root_disk_available_bytes: Option<u64>,
    pub os_name: String,
    pub os_version: String,
}

impl HardwareReport {
    pub fn detect() -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram().with_swap()),
        );
        system.refresh_cpu_list(CpuRefreshKind::nothing());
        let (root_disk_total_bytes, root_disk_available_bytes) = root_disk_metrics();

        let chip = chip_name(&system);
        let physical_cores = System::physical_core_count().unwrap_or(0);
        let logical_cores = system.cpus().len();
        let os_name = System::name().unwrap_or_else(|| "unknown".to_string());
        let os_version = System::os_version().unwrap_or_else(|| "unknown".to_string());

        Self {
            chip,
            physical_cores,
            logical_cores,
            ram_total_bytes: system.total_memory(),
            ram_available_bytes: system.available_memory(),
            ram_used_bytes: system.used_memory(),
            swap_total_bytes: system.total_swap(),
            swap_used_bytes: system.used_swap(),
            root_disk_total_bytes,
            root_disk_available_bytes,
            os_name,
            os_version,
        }
    }
}

#[cfg(target_os = "macos")]
fn chip_name(system: &System) -> String {
    if let Some(brand) = sysinfo_cpu_brand(system) {
        return brand;
    }

    let output = Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output();

    if let Ok(output) = output {
        if output.status.success() {
            let chip = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !chip.is_empty() {
                return chip;
            }
        }
    }

    "unknown".to_string()
}

#[cfg(not(target_os = "macos"))]
fn chip_name(system: &System) -> String {
    sysinfo_cpu_brand(system).unwrap_or_else(|| "unknown".to_string())
}

fn sysinfo_cpu_brand(system: &System) -> Option<String> {
    system
        .cpus()
        .first()
        .map(|cpu| cpu.brand().trim().to_string())
        .filter(|brand| !brand.is_empty())
}

fn root_disk_metrics() -> (Option<u64>, Option<u64>) {
    let disks = Disks::new_with_refreshed_list_specifics(DiskRefreshKind::nothing().with_storage());
    let root = Path::new("/");

    let Some(disk) = disks
        .list()
        .iter()
        .filter(|disk| root.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
    else {
        return (None, None);
    };

    let total = disk.total_space();
    if total == 0 {
        return (None, None);
    }

    (Some(total), Some(disk.available_space()))
}
