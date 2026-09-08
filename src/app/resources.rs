//! Resource eligibility and destination-volume observation without lifecycle changes.
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use sysinfo::{Disks, System};

const GIB: u64 = 1024 * 1024 * 1024;
const MACOS_MEMORY_RESERVE_BYTES: u64 = 4 * GIB;
const MEMORY_OVERHEAD_PERCENT: u64 = 5;
#[cfg(unix)]
pub(super) fn resource_budget_for_destination(destination: &Path) -> Option<ResourceBudget> {
    let mut system = System::new();
    system.refresh_memory();
    let physical_memory_bytes = system.total_memory();
    let destination = existing_destination_ancestor(destination)?;
    let disks = Disks::new_with_refreshed_list();
    let destination_free_bytes = destination_free_bytes_for_mounts(
        &destination,
        disks
            .list()
            .iter()
            .map(|disk| (disk.mount_point(), disk.available_space())),
    )?;
    Some(ResourceBudget::new(
        physical_memory_bytes,
        destination_free_bytes,
    ))
}

#[cfg(unix)]
pub(super) fn existing_destination_ancestor(destination: &Path) -> Option<PathBuf> {
    destination
        .ancestors()
        .find(|path| path.is_dir())?
        .canonicalize()
        .ok()
}

#[cfg(unix)]
pub(super) fn destination_free_bytes_for_mounts<'a>(
    destination: &Path,
    mounts: impl IntoIterator<Item = (&'a Path, u64)>,
) -> Option<u64> {
    mounts
        .into_iter()
        .filter(|(mount_point, _)| destination.starts_with(mount_point))
        .max_by_key(|(mount_point, _)| mount_point.components().count())
        .map(|(_, available_space)| available_space)
}

#[cfg(not(unix))]
pub(super) fn resource_budget_for_destination(_destination: &Path) -> Option<ResourceBudget> {
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationUnavailableReason {
    InsufficientMemory,
    InsufficientDisk,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationAvailability {
    Available,
    Unavailable(RecommendationUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceBudget {
    physical_memory_bytes: u64,
    destination_free_bytes: u64,
}

impl ResourceBudget {
    pub fn new(physical_memory_bytes: u64, destination_free_bytes: u64) -> Self {
        Self {
            physical_memory_bytes,
            destination_free_bytes,
        }
    }

    pub fn recommendation(self) -> RecommendationAvailability {
        let bundle_bytes = crate::catalog::GEMMA4_MODEL_SIZE + crate::catalog::GEMMA4_DRAFT_SIZE;
        let required_memory_bytes = bundle_bytes
            .checked_mul(100 + MEMORY_OVERHEAD_PERCENT)
            .and_then(|bytes| bytes.checked_div(100))
            .unwrap_or(u64::MAX);
        let required_disk_bytes = bundle_bytes.saturating_add(GIB);
        let available_memory_bytes = self
            .physical_memory_bytes
            .saturating_sub(MACOS_MEMORY_RESERVE_BYTES);

        if available_memory_bytes < required_memory_bytes {
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientMemory,
            )
        } else if self.destination_free_bytes < required_disk_bytes {
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientDisk,
            )
        } else {
            RecommendationAvailability::Available
        }
    }
}
