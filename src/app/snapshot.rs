//! Build application snapshots from guarded catalog and runtime observations.
use super::{
    AppSnapshot, BundleSnapshot, BundleUnavailableReason, DownloadSnapshot, PartialBundle,
    PausedDownload, RecommendationAvailability, RecommendationSnapshot,
    RecommendationUnavailableReason, RecommendedBundle, ResourceBudget, RuntimeInventorySnapshot,
    RuntimeOwnerSnapshot, RuntimeSnapshot, VerifiedBundle, TARGET_MODEL_ID,
};
use crate::catalog::{self, BundlePending, Manifest};
use crate::runtime::{ForegroundObservation, RuntimeOwner, RuntimeProvenance};
use std::fs;
use std::path::Path;

impl AppSnapshot {
    pub fn bundle(&self) -> &BundleSnapshot {
        &self.bundle
    }

    pub fn recommendation(&self) -> &RecommendationSnapshot {
        &self.recommendation
    }

    pub fn download(&self) -> &DownloadSnapshot {
        &self.download
    }

    pub fn runtime(&self) -> RuntimeSnapshot {
        self.runtime
    }

    pub fn runtime_port(&self) -> Option<u16> {
        self.runtime_port
    }

    pub fn runtime_owner(&self) -> Option<RuntimeOwnerSnapshot> {
        self.runtime_owner
    }

    pub fn runtime_model_id(&self) -> Option<&str> {
        self.runtime_model_id.as_deref()
    }

    pub fn bundle_model_id(&self) -> &'static str {
        TARGET_MODEL_ID
    }

    pub fn runtime_inventory(&self) -> RuntimeInventorySnapshot {
        self.runtime_inventory
    }

    pub(super) fn from_observation(
        bundle: BundleSnapshot,
        budget: Option<ResourceBudget>,
        foreground: ForegroundObservation,
    ) -> Self {
        let (recommendation, download) = match &bundle {
            BundleSnapshot::Verified(_) | BundleSnapshot::Unavailable(_) => {
                (RecommendationSnapshot::Hidden, DownloadSnapshot::Idle)
            }
            BundleSnapshot::Partial(partial) => (
                RecommendationSnapshot::Hidden,
                DownloadSnapshot::Paused(PausedDownload {
                    completed_bytes: partial.completed_bytes,
                    total_bytes: partial.total_bytes,
                }),
            ),
            BundleSnapshot::Absent => match budget {
                Some(budget) => match budget.recommendation() {
                    RecommendationAvailability::Available => (
                        RecommendationSnapshot::Available(RecommendedBundle {
                            target_bytes: crate::catalog::GEMMA4_MODEL_SIZE,
                            draft_bytes: crate::catalog::GEMMA4_DRAFT_SIZE,
                        }),
                        DownloadSnapshot::Idle,
                    ),
                    RecommendationAvailability::Unavailable(reason) => (
                        RecommendationSnapshot::Unavailable(reason),
                        DownloadSnapshot::Idle,
                    ),
                },
                None => (
                    RecommendationSnapshot::Unavailable(
                        RecommendationUnavailableReason::Unavailable,
                    ),
                    DownloadSnapshot::Idle,
                ),
            },
        };
        let (runtime, runtime_port, runtime_owner, runtime_model_id, runtime_inventory) =
            match foreground {
                ForegroundObservation::Idle => (
                    RuntimeSnapshot::Idle,
                    None,
                    None,
                    None,
                    RuntimeInventorySnapshot::Missing,
                ),
                ForegroundObservation::Starting => (
                    RuntimeSnapshot::Starting,
                    None,
                    None,
                    None,
                    RuntimeInventorySnapshot::Missing,
                ),
                ForegroundObservation::Running {
                    provenance,
                    owner,
                    model_id,
                    port,
                } => (
                    RuntimeSnapshot::Running,
                    Some(port),
                    Some(match owner {
                        RuntimeOwner::Legacy => RuntimeOwnerSnapshot::Legacy,
                        RuntimeOwner::Foreground => RuntimeOwnerSnapshot::Foreground,
                        RuntimeOwner::PersistentApp => RuntimeOwnerSnapshot::PersistentApp,
                        RuntimeOwner::Service => RuntimeOwnerSnapshot::Service,
                    }),
                    Some(model_id),
                    if provenance == RuntimeProvenance::External {
                        RuntimeInventorySnapshot::External
                    } else {
                        RuntimeInventorySnapshot::Missing
                    },
                ),
                ForegroundObservation::Stopping => (
                    RuntimeSnapshot::Stopping,
                    None,
                    None,
                    None,
                    RuntimeInventorySnapshot::Missing,
                ),
                ForegroundObservation::Error => (
                    RuntimeSnapshot::Error,
                    None,
                    None,
                    None,
                    RuntimeInventorySnapshot::Missing,
                ),
            };
        Self {
            bundle,
            recommendation,
            download,
            runtime,
            runtime_port,
            runtime_owner,
            runtime_model_id,
            runtime_inventory,
        }
    }
}

struct QualifiedBundle {
    target_bytes: u64,
    draft_bytes: u64,
}

impl QualifiedBundle {
    fn total_bytes(&self) -> u64 {
        self.target_bytes.saturating_add(self.draft_bytes)
    }
}

pub(super) fn observe_bundle(models_root: &Path) -> BundleSnapshot {
    let model_dir = models_root.join(TARGET_MODEL_ID);
    match catalog::model_is_busy(&model_dir) {
        Ok(true) => return BundleSnapshot::Unavailable(BundleUnavailableReason::Busy),
        Ok(false) => {}
        Err(_) => return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid),
    }
    let catalog = match catalog::load_catalog(models_root) {
        Ok(catalog) => catalog,
        Err(_) => return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid),
    };
    if let Some(manifest) = catalog
        .iter()
        .find(|manifest| manifest.id == TARGET_MODEL_ID)
    {
        return published_bundle_snapshot(models_root, manifest);
    }

    match catalog::bundle_pending(&model_dir) {
        BundlePending::Valid(manifest) => partial_bundle_snapshot(&manifest),
        BundlePending::UnsafeOrInvalid => {
            BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
        }
        BundlePending::Absent if target_directory_is_clean(&model_dir) => BundleSnapshot::Absent,
        BundlePending::Absent => BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid),
    }
}

pub(super) fn published_bundle_snapshot(models_root: &Path, manifest: &Manifest) -> BundleSnapshot {
    let Some(bundle) = qualified_bundle(manifest) else {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    };
    let primary = manifest.primary_artifact();
    let model_dir = models_root.join(&manifest.id);
    let model_lock = match catalog::ModelLock::acquire_existing(&model_dir) {
        Ok(model_lock) => model_lock,
        Err(catalog::ModelLockError::Busy) => {
            return BundleSnapshot::Unavailable(BundleUnavailableReason::Busy);
        }
        Err(catalog::ModelLockError::Missing | catalog::ModelLockError::UnsafeLocalState) => {
            return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
        }
    };
    let primary_path = model_dir.join(primary.local_filename);
    let draft_path = manifest
        .draft_artifact()
        .map(|draft| model_dir.join(draft.local_filename));
    if crate::verification::verify_or_refresh(
        &model_lock,
        &model_dir,
        manifest,
        &primary_path,
        draft_path.as_deref(),
        || crate::verification::verify_artifacts(manifest, &primary_path, draft_path.as_deref()),
    )
    .is_err()
    {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    }
    let Some(_draft) = manifest.draft_artifact() else {
        return partial_from_bytes(primary.size, bundle.total_bytes());
    };
    BundleSnapshot::Verified(VerifiedBundle {
        target_bytes: bundle.target_bytes,
        draft_bytes: bundle.draft_bytes,
    })
}

fn partial_bundle_snapshot(manifest: &Manifest) -> BundleSnapshot {
    let Some(bundle) = qualified_bundle(manifest) else {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    };
    partial_from_bytes(0, bundle.total_bytes())
}

fn partial_from_bytes(completed_bytes: u64, total_bytes: u64) -> BundleSnapshot {
    if completed_bytes > total_bytes || total_bytes == 0 {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    }
    BundleSnapshot::Partial(PartialBundle {
        completed_bytes,
        total_bytes,
    })
}

fn qualified_bundle(manifest: &Manifest) -> Option<QualifiedBundle> {
    if manifest.id != TARGET_MODEL_ID {
        return None;
    }
    let runtime = manifest.runtime.as_ref()?;
    let production_profile = catalog::is_qualified_gemma4_bundle(manifest);
    #[cfg(test)]
    let test_profile = manifest.profile.as_deref() == Some(catalog::TEST_MTP_PROFILE)
        && runtime.engine == "llama.cpp"
        && runtime.build == catalog::TEST_LLAMA_BUILD;
    #[cfg(not(test))]
    let test_profile = {
        let _ = runtime;
        false
    };
    if !production_profile && !test_profile {
        return None;
    }

    let primary = manifest.primary_artifact();
    let draft = manifest.draft_artifact();
    if primary.local_filename != "model.gguf"
        || draft.is_some_and(|draft| draft.local_filename != "draft.gguf")
    {
        return None;
    }
    if production_profile
        && (primary.size != catalog::GEMMA4_MODEL_SIZE
            || primary.sha256 != catalog::GEMMA4_MODEL_SHA256
            || draft.is_some_and(|draft| {
                draft.size != catalog::GEMMA4_DRAFT_SIZE
                    || draft.sha256 != catalog::GEMMA4_DRAFT_SHA256
            }))
    {
        return None;
    }
    Some(QualifiedBundle {
        target_bytes: primary.size,
        draft_bytes: draft.map_or(catalog::GEMMA4_DRAFT_SIZE, |draft| draft.size),
    })
}

fn target_directory_is_clean(model_dir: &Path) -> bool {
    let mut entries = match fs::read_dir(model_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    entries.all(|entry| match entry {
        Ok(entry) => entry.file_name() == ".lock",
        Err(_) => false,
    })
}
