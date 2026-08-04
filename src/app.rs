use crate::catalog::{self, BundlePending, Manifest};
use crate::runtime::{ForegroundObservation, ForegroundObserver, RuntimeProvenance};
use std::fs;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use sysinfo::{Disks, System};

const GIB: u64 = 1024 * 1024 * 1024;
const MACOS_MEMORY_RESERVE_BYTES: u64 = 4 * GIB;
const MEMORY_OVERHEAD_PERCENT: u64 = 5;
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
pub enum RuntimeInventorySnapshot {
    ManagedB10121,
    External,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppSnapshot {
    bundle: BundleSnapshot,
    recommendation: RecommendationSnapshot,
    download: DownloadSnapshot,
    runtime: RuntimeSnapshot,
    runtime_inventory: RuntimeInventorySnapshot,
}

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

    pub fn runtime_inventory(&self) -> RuntimeInventorySnapshot {
        self.runtime_inventory
    }

    fn from_observation(
        bundle: BundleSnapshot,
        budget: Option<ResourceBudget>,
        foreground: ForegroundObservation,
        managed_runtime_valid: bool,
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
        let runtime = match foreground {
            ForegroundObservation::Idle => RuntimeSnapshot::Idle,
            ForegroundObservation::Starting => RuntimeSnapshot::Starting,
            ForegroundObservation::Running(_) => RuntimeSnapshot::Running,
            ForegroundObservation::Stopping => RuntimeSnapshot::Stopping,
            ForegroundObservation::Error => RuntimeSnapshot::Error,
        };
        let runtime_inventory = match foreground {
            ForegroundObservation::Running(RuntimeProvenance::External) => {
                RuntimeInventorySnapshot::External
            }
            _ if managed_runtime_valid => RuntimeInventorySnapshot::ManagedB10121,
            _ => RuntimeInventorySnapshot::Missing,
        };
        Self {
            bundle,
            recommendation,
            download,
            runtime,
            runtime_inventory,
        }
    }
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
        let managed_runtime_valid =
            crate::runner::managed_runtime_inventory_is_valid(&self.paths.managed_server);
        AppSnapshot::from_observation(
            observe_bundle(&self.paths.models),
            budget,
            foreground,
            managed_runtime_valid,
        )
    }
}

#[cfg(unix)]
fn resource_budget_for_destination(destination: &Path) -> Option<ResourceBudget> {
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
fn existing_destination_ancestor(destination: &Path) -> Option<PathBuf> {
    destination
        .ancestors()
        .find(|path| path.is_dir())?
        .canonicalize()
        .ok()
}

#[cfg(unix)]
fn destination_free_bytes_for_mounts<'a>(
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
fn resource_budget_for_destination(_destination: &Path) -> Option<ResourceBudget> {
    None
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

fn observe_bundle(models_root: &Path) -> BundleSnapshot {
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

fn published_bundle_snapshot(models_root: &Path, manifest: &Manifest) -> BundleSnapshot {
    let Some(bundle) = qualified_bundle(manifest) else {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    };
    let primary = manifest.primary_artifact();
    if crate::download::verify_regular(
        &models_root.join(&manifest.id).join(primary.local_filename),
        primary.size,
        primary.sha256,
    )
    .is_err()
    {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    }
    let Some(draft) = manifest.draft_artifact() else {
        return partial_from_bytes(primary.size, bundle.total_bytes());
    };
    if crate::download::verify_regular(
        &models_root.join(&manifest.id).join(draft.local_filename),
        draft.size,
        draft.sha256,
    )
    .is_err()
    {
        return BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid);
    }
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
    let production_profile = manifest.profile.as_deref() == Some(catalog::GEMMA4_MTP_PROFILE)
        && runtime.engine == "llama.cpp"
        && runtime.build == catalog::GEMMA4_LLAMA_BUILD;
    #[cfg(test)]
    let test_profile = manifest.profile.as_deref() == Some(catalog::TEST_MTP_PROFILE)
        && runtime.engine == "llama.cpp"
        && runtime.build == catalog::TEST_LLAMA_BUILD;
    #[cfg(not(test))]
    let test_profile = false;
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

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::{destination_free_bytes_for_mounts, existing_destination_ancestor};
    use super::{
        AppSnapshot, BundleSnapshot, BundleUnavailableReason, DownloadSnapshot, PartialBundle,
        PausedDownload, RecommendationAvailability, RecommendationSnapshot,
        RecommendationUnavailableReason, RecommendedBundle, ResourceBudget,
        RuntimeInventorySnapshot, RuntimeSnapshot, SnapshotReader, VerifiedBundle,
    };
    use crate::catalog::{
        Artifact, ArtifactProvenance, ArtifactRole, Manifest, RuntimeQualification,
        TEST_LLAMA_BUILD, TEST_MTP_PROFILE,
    };
    use crate::paths::AppPaths;
    use crate::runtime::{ForegroundObservation, RuntimeProvenance};
    use sha2::{Digest, Sha256};
    use std::fs::{self, OpenOptions};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::tempdir;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    const TARGET_AND_DRAFT_BYTES: u64 = 6_970_065_600;
    const REQUIRED_MEMORY_BYTES: u64 = 7_318_568_880;
    const REQUIRED_DISK_BYTES: u64 = 8_043_807_424;

    fn sha256(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn test_bundle_manifest() -> Manifest {
        let target = b"test target";
        let draft = b"test draft";
        Manifest {
            version: 3,
            id: "gemma-4-12b-it-qat-ud-q4-k-xl".into(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: sha256(target),
            size: target.len() as u64,
            artifacts: Some(vec![
                Artifact {
                    role: ArtifactRole::Model,
                    local_filename: "model.gguf".into(),
                    sha256: sha256(target),
                    size: target.len() as u64,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "target.gguf".into(),
                    },
                },
                Artifact {
                    role: ArtifactRole::Draft,
                    local_filename: "draft.gguf".into(),
                    sha256: sha256(draft),
                    size: draft.len() as u64,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "draft.gguf".into(),
                    },
                },
            ]),
            profile: Some(TEST_MTP_PROFILE.into()),
            runtime: Some(RuntimeQualification {
                engine: "llama.cpp".into(),
                build: TEST_LLAMA_BUILD.into(),
            }),
        }
    }

    fn write_bundle_artifacts(models_root: &Path, manifest: &Manifest) {
        let model_dir = models_root.join(&manifest.id);
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(model_dir.join("model.gguf"), b"test target").unwrap();
        fs::write(model_dir.join("draft.gguf"), b"test draft").unwrap();
    }

    fn test_paths(root: &Path) -> AppPaths {
        AppPaths::from_values(Some(root), None).unwrap()
    }

    fn enough_budget() -> ResourceBudget {
        ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES, REQUIRED_DISK_BYTES)
    }

    #[cfg(unix)]
    fn write_managed_runtime_qualification(server: &Path) {
        let bytes = fs::read(server).unwrap();
        let evidence = serde_json::json!({
            "version": 1,
            "build": "b10121",
            "managed_version": "version: 10121 (555881ebc)",
            "sha256": sha256(&bytes),
            "size": bytes.len(),
        });
        fs::write(
            server.with_extension("qualification.json"),
            serde_json::to_vec(&evidence).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn resource_budget_reserves_macos_memory_and_exact_bundle_capacity() {
        let available = ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES, REQUIRED_DISK_BYTES);
        assert_eq!(
            available.recommendation(),
            RecommendationAvailability::Available
        );

        let memory_short =
            ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES - 1, REQUIRED_DISK_BYTES);
        assert_eq!(
            memory_short.recommendation(),
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientMemory,
            )
        );

        let disk_short =
            ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES, REQUIRED_DISK_BYTES - 1);
        assert_eq!(
            disk_short.recommendation(),
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientDisk,
            )
        );

        assert_eq!(TARGET_AND_DRAFT_BYTES * 105 / 100, REQUIRED_MEMORY_BYTES);
        assert_eq!(TARGET_AND_DRAFT_BYTES + GIB, REQUIRED_DISK_BYTES);
    }

    #[cfg(unix)]
    #[test]
    fn destination_volume_selector_uses_the_resolved_symlink_backing_mount() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let backing = root.path().join("backing-volume");
        let link = root.path().join("models-link");
        fs::create_dir_all(&backing).unwrap();
        symlink(&backing, &link).unwrap();

        let resolved = existing_destination_ancestor(&link.join("future/models")).unwrap();
        let root_mount = root.path().canonicalize().unwrap();
        let backing_mount = backing.canonicalize().unwrap();
        assert_eq!(resolved, backing_mount);

        assert_eq!(
            destination_free_bytes_for_mounts(
                &resolved,
                [
                    (root_mount.as_path(), 1_u64),
                    (backing_mount.as_path(), 2_u64),
                ],
            ),
            Some(2)
        );
    }

    #[cfg(unix)]
    #[test]
    fn destination_volume_selector_uses_the_deepest_component_mount() {
        assert_eq!(
            destination_free_bytes_for_mounts(
                Path::new("/volumes/models/cache"),
                [
                    (Path::new("/"), 1_u64),
                    (Path::new("/volumes"), 2_u64),
                    (Path::new("/volumes/models"), 3_u64),
                    (Path::new("/volumes/models-other"), 4_u64),
                ],
            ),
            Some(3)
        );
    }

    #[test]
    fn reader_derives_only_canonical_bundle_recommendation_and_download_states() {
        let absent_root = tempdir().unwrap();
        let mut absent_reader = SnapshotReader::new(test_paths(absent_root.path()));
        let absent = absent_reader.observe_with_budget(Some(enough_budget()));
        assert!(matches!(absent.bundle(), BundleSnapshot::Absent));
        assert!(matches!(
            absent.recommendation(),
            RecommendationSnapshot::Available(_)
        ));
        assert!(matches!(absent.download(), DownloadSnapshot::Idle));

        let ineligible_root = tempdir().unwrap();
        let mut ineligible_reader = SnapshotReader::new(test_paths(ineligible_root.path()));
        let ineligible = ineligible_reader.observe_with_budget(Some(ResourceBudget::new(
            4 * GIB + REQUIRED_MEMORY_BYTES - 1,
            REQUIRED_DISK_BYTES,
        )));
        assert!(matches!(ineligible.bundle(), BundleSnapshot::Absent));
        assert_eq!(
            ineligible.recommendation(),
            &RecommendationSnapshot::Unavailable(
                RecommendationUnavailableReason::InsufficientMemory,
            )
        );
        assert!(matches!(ineligible.download(), DownloadSnapshot::Idle));

        let partial_root = tempdir().unwrap();
        let partial_paths = test_paths(partial_root.path());
        let partial_manifest = test_bundle_manifest();
        let partial_dir = partial_paths.models.join(&partial_manifest.id);
        fs::create_dir_all(&partial_dir).unwrap();
        fs::write(
            partial_dir.join("bundle.pending.json"),
            serde_json::to_vec(&partial_manifest).unwrap(),
        )
        .unwrap();
        let mut partial_reader = SnapshotReader::new(partial_paths);
        let partial = partial_reader.observe_with_budget(Some(enough_budget()));
        assert!(matches!(partial.bundle(), BundleSnapshot::Partial(_)));
        assert!(matches!(
            partial.recommendation(),
            RecommendationSnapshot::Hidden
        ));
        assert!(matches!(partial.download(), DownloadSnapshot::Paused(_)));

        let verified_root = tempdir().unwrap();
        let verified_paths = test_paths(verified_root.path());
        let verified_manifest = test_bundle_manifest();
        write_bundle_artifacts(&verified_paths.models, &verified_manifest);
        crate::catalog::publish_manifest(&verified_paths.models, &verified_manifest).unwrap();
        let mut verified_reader = SnapshotReader::new(verified_paths);
        let verified = verified_reader.observe_with_budget(Some(enough_budget()));
        assert!(matches!(verified.bundle(), BundleSnapshot::Verified(_)));
        assert!(matches!(
            verified.recommendation(),
            RecommendationSnapshot::Hidden
        ));
        assert!(matches!(verified.download(), DownloadSnapshot::Idle));

        let invalid_root = tempdir().unwrap();
        let invalid_paths = test_paths(invalid_root.path());
        let invalid_dir = invalid_paths.models.join("gemma-4-12b-it-qat-ud-q4-k-xl");
        fs::create_dir_all(&invalid_dir).unwrap();
        fs::write(
            invalid_dir.join("manifest.json"),
            b"Authorization: Bearer secret",
        )
        .unwrap();
        let mut invalid_reader = SnapshotReader::new(invalid_paths);
        let invalid = invalid_reader.observe_with_budget(Some(enough_budget()));
        assert_eq!(
            invalid.bundle(),
            &BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
        );
        assert!(matches!(
            invalid.recommendation(),
            RecommendationSnapshot::Hidden
        ));
        assert!(matches!(invalid.download(), DownloadSnapshot::Idle));

        let busy_root = tempdir().unwrap();
        let busy_paths = test_paths(busy_root.path());
        let busy_dir = busy_paths.models.join("gemma-4-12b-it-qat-ud-q4-k-xl");
        fs::create_dir_all(&busy_dir).unwrap();
        let lock = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(busy_dir.join(".lock"))
            .unwrap();
        lock.try_lock().unwrap();
        let mut busy_reader = SnapshotReader::new(busy_paths);
        let busy = busy_reader.observe_with_budget(Some(enough_budget()));
        assert_eq!(
            busy.bundle(),
            &BundleSnapshot::Unavailable(BundleUnavailableReason::Busy)
        );
        assert!(matches!(
            busy.recommendation(),
            RecommendationSnapshot::Hidden
        ));
        assert!(matches!(busy.download(), DownloadSnapshot::Idle));

        assert_send_sync::<AppSnapshot>();
        assert_send_sync::<BundleSnapshot>();
        assert_send_sync::<VerifiedBundle>();
        assert_send_sync::<PartialBundle>();
        assert_send_sync::<RecommendationSnapshot>();
        assert_send_sync::<RecommendedBundle>();
        assert_send_sync::<DownloadSnapshot>();
        assert_send_sync::<PausedDownload>();
        assert_send_sync::<RuntimeSnapshot>();
        assert_send_sync::<RuntimeInventorySnapshot>();
    }

    #[test]
    fn reader_treats_a_verified_primary_waiting_for_its_draft_as_a_paused_partial() {
        let root = tempdir().unwrap();
        let paths = test_paths(root.path());
        let mut manifest = test_bundle_manifest();
        manifest
            .artifacts
            .as_mut()
            .unwrap()
            .retain(|artifact| artifact.role == ArtifactRole::Model);
        let model_dir = paths.models.join(&manifest.id);
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(model_dir.join("model.gguf"), b"test target").unwrap();
        crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();

        let mut reader = SnapshotReader::new(paths);
        let snapshot = reader.observe_with_budget(Some(enough_budget()));

        assert!(matches!(snapshot.bundle(), BundleSnapshot::Partial(_)));
        assert!(matches!(
            snapshot.recommendation(),
            RecommendationSnapshot::Hidden
        ));
        assert!(matches!(snapshot.download(), DownloadSnapshot::Paused(_)));
    }

    #[test]
    fn app_snapshot_maps_safe_runtime_provenance_without_exposing_server_paths() {
        let external = AppSnapshot::from_observation(
            BundleSnapshot::Absent,
            Some(enough_budget()),
            ForegroundObservation::Running(RuntimeProvenance::External),
            true,
        );
        assert_eq!(external.runtime(), RuntimeSnapshot::Running);
        assert_eq!(
            external.runtime_inventory(),
            RuntimeInventorySnapshot::External
        );
        assert!(!format!("{external:?}").contains('/'));

        let managed = AppSnapshot::from_observation(
            BundleSnapshot::Absent,
            Some(enough_budget()),
            ForegroundObservation::Running(RuntimeProvenance::Managed),
            true,
        );
        assert_eq!(managed.runtime(), RuntimeSnapshot::Running);
        assert_eq!(
            managed.runtime_inventory(),
            RuntimeInventorySnapshot::ManagedB10121
        );

        let idle = AppSnapshot::from_observation(
            BundleSnapshot::Absent,
            Some(enough_budget()),
            ForegroundObservation::Idle,
            false,
        );
        assert_eq!(idle.runtime(), RuntimeSnapshot::Idle);
        assert_eq!(idle.runtime_inventory(), RuntimeInventorySnapshot::Missing);
    }

    #[test]
    fn reader_inspects_the_destination_volume_without_creating_lifecycle_state() {
        let root = tempdir().unwrap();
        let paths = test_paths(root.path());
        let mut reader = SnapshotReader::new(paths.clone());

        let snapshot = reader.observe();

        assert!(matches!(snapshot.bundle(), BundleSnapshot::Absent));
        assert!(!matches!(
            snapshot.recommendation(),
            RecommendationSnapshot::Unavailable(RecommendationUnavailableReason::Unavailable)
        ));
        assert!(!paths.models.exists());
        assert!(!paths.run.exists());
    }

    #[cfg(unix)]
    #[test]
    fn reader_observation_never_executes_managed_runtime_candidate() {
        let root = tempdir().unwrap();
        let paths = test_paths(root.path());
        fs::create_dir_all(paths.managed_server.parent().unwrap()).unwrap();
        let sentinel = paths.managed_server.with_extension("ran");
        fs::write(
            &paths.managed_server,
            b"#!/bin/sh\n: > \"$0.ran\"\nprintf '%s\\n' 'version: 10121 (555881ebc)'\n",
        )
        .unwrap();
        fs::set_permissions(&paths.managed_server, fs::Permissions::from_mode(0o700)).unwrap();
        write_managed_runtime_qualification(&paths.managed_server);

        let mut reader = SnapshotReader::new(paths);
        let snapshot = reader.observe();

        assert_eq!(
            snapshot.runtime_inventory(),
            RuntimeInventorySnapshot::ManagedB10121
        );
        assert!(
            !sentinel.exists(),
            "snapshot observation must not execute the managed candidate"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reader_reports_only_an_exact_managed_b10121_runtime_inventory() {
        let root = tempdir().unwrap();
        let paths = test_paths(root.path());
        fs::create_dir_all(paths.managed_server.parent().unwrap()).unwrap();
        fs::write(
            &paths.managed_server,
            b"#!/bin/sh\nprintf '%s\\n' 'version: 10121 (555881ebc)'\n",
        )
        .unwrap();
        fs::set_permissions(&paths.managed_server, fs::Permissions::from_mode(0o700)).unwrap();

        let mut reader = SnapshotReader::new(paths.clone());
        let missing = reader.observe();
        assert_eq!(
            missing.runtime_inventory(),
            RuntimeInventorySnapshot::Missing
        );

        write_managed_runtime_qualification(&paths.managed_server);
        let managed = reader.observe();
        assert_eq!(
            managed.runtime_inventory(),
            RuntimeInventorySnapshot::ManagedB10121
        );

        fs::write(
            &paths.managed_server,
            b"#!/bin/sh\nprintf '%s\\n' 'version: 10090 (stale)'\n",
        )
        .unwrap();
        fs::set_permissions(&paths.managed_server, fs::Permissions::from_mode(0o700)).unwrap();
        let missing = reader.observe();
        assert_eq!(
            missing.runtime_inventory(),
            RuntimeInventorySnapshot::Missing
        );
    }

    #[test]
    fn reader_rejects_unsafe_or_stale_bundle_inputs_without_leaking_content() {
        let unsafe_root = tempdir().unwrap();
        let unsafe_paths = test_paths(unsafe_root.path());
        let manifest = test_bundle_manifest();
        write_bundle_artifacts(&unsafe_paths.models, &manifest);
        let model_dir = unsafe_paths.models.join(&manifest.id);
        let source = unsafe_root.path().join("external-manifest.json");
        fs::write(&source, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
        fs::hard_link(&source, model_dir.join("manifest.json")).unwrap();
        let mut unsafe_reader = SnapshotReader::new(unsafe_paths);
        let unsafe_snapshot = unsafe_reader.observe_with_budget(Some(enough_budget()));
        assert_eq!(
            unsafe_snapshot.bundle(),
            &BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
        );

        let stale_root = tempdir().unwrap();
        let stale_paths = test_paths(stale_root.path());
        write_bundle_artifacts(&stale_paths.models, &manifest);
        crate::catalog::publish_manifest(&stale_paths.models, &manifest).unwrap();
        fs::write(
            stale_paths.models.join(&manifest.id).join("model.gguf"),
            b"stale artifact",
        )
        .unwrap();
        let mut stale_reader = SnapshotReader::new(stale_paths);
        let stale_snapshot = stale_reader.observe_with_budget(Some(enough_budget()));
        assert_eq!(
            stale_snapshot.bundle(),
            &BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
        );

        let malformed_root = tempdir().unwrap();
        let malformed_paths = test_paths(malformed_root.path());
        let malformed_dir = malformed_paths.models.join("gemma-4-12b-it-qat-ud-q4-k-xl");
        fs::create_dir_all(&malformed_dir).unwrap();
        fs::write(
            malformed_dir.join("manifest.json"),
            b"Authorization: Bearer token; prompt=secret; raw child output; HOME=/private/user",
        )
        .unwrap();
        let mut malformed_reader = SnapshotReader::new(malformed_paths);
        let malformed_snapshot = malformed_reader.observe_with_budget(Some(enough_budget()));
        assert_eq!(
            malformed_snapshot.bundle(),
            &BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
        );
        let visible = format!("{malformed_snapshot:?}");
        for secret in [
            "Authorization",
            "token",
            "prompt",
            "raw child output",
            "HOME",
            "/private/user",
        ] {
            assert!(
                !visible.contains(secret),
                "presentation leaked {secret}: {visible}"
            );
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}
}
