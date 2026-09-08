//! Application contracts and the readers that project core state for clients.
use crate::discovery::{
    DiscoveryError, InspectRepository, ModelSearchPage, RepositoryPlan, SearchModels,
};
use crate::paths::AppPaths;
use crate::runtime::ForegroundObserver;

mod incomplete;
mod installed;
mod resources;
mod snapshot;
pub(crate) mod transfer;

pub use incomplete::{IncompleteTransferInventory, IncompleteTransferSummary};
pub use installed::InstalledModelSummary;
use resources::resource_budget_for_destination;
#[cfg(all(test, unix))]
use resources::{destination_free_bytes_for_mounts, existing_destination_ancestor};
pub use resources::{RecommendationAvailability, RecommendationUnavailableReason, ResourceBudget};
use snapshot::observe_bundle;
#[cfg(all(test, unix))]
use snapshot::published_bundle_snapshot;
pub use transfer::{
    DiscardCandidate, ResolveArtifactError, ResolveArtifactRequest, TransferControl,
    TransferDisposition, TransferError, TransferPhase, TransferProgress, TransferResult,
    TransferSelected,
};

const TARGET_MODEL_ID: &str = "gemma-4-12b-it-qat-ud-q4-k-xl";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BundleUnavailableReason {
    Invalid,
    Busy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedBundle {
    target_bytes: u64,
    draft_bytes: u64,
}

impl VerifiedBundle {
    pub fn target_bytes(&self) -> u64 {
        self.target_bytes
    }

    pub fn draft_bytes(&self) -> u64 {
        self.draft_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartialBundle {
    completed_bytes: u64,
    total_bytes: u64,
}

impl PartialBundle {
    pub fn completed_bytes(&self) -> u64 {
        self.completed_bytes
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BundleSnapshot {
    Verified(VerifiedBundle),
    Partial(PartialBundle),
    Absent,
    Unavailable(BundleUnavailableReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendedBundle {
    target_bytes: u64,
    draft_bytes: u64,
}

impl RecommendedBundle {
    pub fn target_bytes(&self) -> u64 {
        self.target_bytes
    }

    pub fn draft_bytes(&self) -> u64 {
        self.draft_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecommendationSnapshot {
    Hidden,
    Available(RecommendedBundle),
    Unavailable(RecommendationUnavailableReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PausedDownload {
    completed_bytes: u64,
    total_bytes: u64,
}

impl PausedDownload {
    pub fn completed_bytes(&self) -> u64 {
        self.completed_bytes
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DownloadSnapshot {
    Idle,
    Paused(PausedDownload),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeSnapshot {
    Idle,
    Starting,
    Running,
    Stopping,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOwnerSnapshot {
    Legacy,
    Foreground,
    PersistentApp,
    Service,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeInventorySnapshot {
    External,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppSnapshot {
    bundle: BundleSnapshot,
    recommendation: RecommendationSnapshot,
    download: DownloadSnapshot,
    runtime: RuntimeSnapshot,
    runtime_port: Option<u16>,
    runtime_owner: Option<RuntimeOwnerSnapshot>,
    runtime_model_id: Option<String>,
    runtime_inventory: RuntimeInventorySnapshot,
}

pub struct SnapshotReader {
    paths: crate::paths::AppPaths,
    foreground: ForegroundObserver,
}

impl SnapshotReader {
    pub fn new(paths: crate::paths::AppPaths) -> Self {
        Self {
            foreground: ForegroundObserver::new(paths.run.clone()),
            paths,
        }
    }

    pub fn observe(&mut self) -> AppSnapshot {
        self.observe_with_budget(resource_budget_for_destination(&self.paths.models))
    }

    fn observe_with_budget(&mut self, budget: Option<ResourceBudget>) -> AppSnapshot {
        let foreground = self.foreground.observe(&self.paths.managed_server);
        AppSnapshot::from_observation(observe_bundle(&self.paths.models), budget, foreground)
    }
}

pub struct AppService {
    reader: SnapshotReader,
}

impl AppService {
    pub fn from_paths(paths: AppPaths) -> Self {
        Self {
            reader: SnapshotReader::new(paths),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        Ok(Self::from_paths(AppPaths::from_env()?))
    }

    pub fn snapshot(&mut self) -> AppSnapshot {
        self.reader.observe()
    }

    pub fn search_models(&self, request: SearchModels) -> Result<ModelSearchPage, DiscoveryError> {
        crate::huggingface::search_models(request)
    }

    pub fn inspect_repository(
        &self,
        request: InspectRepository,
    ) -> Result<RepositoryPlan, DiscoveryError> {
        crate::huggingface::inspect_repository(request)
    }

    pub fn installed_models(&self) -> Result<Vec<InstalledModelSummary>, String> {
        installed::load(&self.reader.paths.models)
    }
}

#[cfg(test)]
mod tests;
