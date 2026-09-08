#[cfg(unix)]
use super::{destination_free_bytes_for_mounts, existing_destination_ancestor};
use super::{
    observe_bundle, AppService, AppSnapshot, BundleSnapshot, BundleUnavailableReason,
    DownloadSnapshot, InstalledModelSummary, PartialBundle, PausedDownload,
    RecommendationAvailability, RecommendationSnapshot, RecommendationUnavailableReason,
    RecommendedBundle, ResourceBudget, RuntimeInventorySnapshot, RuntimeOwnerSnapshot,
    RuntimeSnapshot, SnapshotReader, VerifiedBundle, TARGET_MODEL_ID,
};
use crate::catalog::{
    Artifact, ArtifactProvenance, ArtifactRole, Manifest, Origin, RuntimeQualification,
    TEST_LLAMA_BUILD, TEST_MTP_PROFILE,
};
use crate::discovery::{DiscoveryErrorKind, GatedStatus, InspectRepository, SearchModels};
use crate::paths::AppPaths;
use crate::runtime::{ForegroundObservation, RuntimeOwner, RuntimeProvenance};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
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
    fs::write(model_dir.join(".lock"), b"").unwrap();
    fs::write(model_dir.join("model.gguf"), b"test target").unwrap();
    fs::write(model_dir.join("draft.gguf"), b"test draft").unwrap();
}

fn test_paths(root: &Path) -> AppPaths {
    AppPaths::from_values(Some(root), None).unwrap()
}

fn snapshot_tree(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    fn visit(root: &Path, path: &Path, snapshot: &mut Vec<(PathBuf, Option<Vec<u8>>)>) {
        if !path.exists() {
            return;
        }
        let relative = path.strip_prefix(root).unwrap().to_path_buf();
        if path.is_dir() {
            snapshot.push((relative, None));
            let mut entries = fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            entries.sort();
            for entry in entries {
                visit(root, &entry, snapshot);
            }
        } else {
            snapshot.push((relative, Some(fs::read(path).unwrap())));
        }
    }

    let mut snapshot = Vec::new();
    visit(root, root, &mut snapshot);
    snapshot
}

fn enough_budget() -> ResourceBudget {
    ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES, REQUIRED_DISK_BYTES)
}

#[cfg(unix)]
fn write_legacy_managed_runtime_qualification(server: &Path) {
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

    let disk_short = ResourceBudget::new(4 * GIB + REQUIRED_MEMORY_BYTES, REQUIRED_DISK_BYTES - 1);
    assert_eq!(
        disk_short.recommendation(),
        RecommendationAvailability::Unavailable(RecommendationUnavailableReason::InsufficientDisk,)
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
        &RecommendationSnapshot::Unavailable(RecommendationUnavailableReason::InsufficientMemory,)
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

#[cfg(unix)]
#[test]
fn published_snapshot_verifies_missing_receipt_once_then_uses_metadata_only() {
    let root = tempdir().unwrap();
    let paths = test_paths(root.path());
    let manifest = test_bundle_manifest();
    write_bundle_artifacts(&paths.models, &manifest);
    crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
    crate::download::reset_content_hash_count();

    assert!(matches!(
        observe_bundle(&paths.models),
        BundleSnapshot::Verified(_)
    ));
    assert_eq!(crate::download::content_hash_count(), 2);

    assert!(matches!(
        observe_bundle(&paths.models),
        BundleSnapshot::Verified(_)
    ));
    assert_eq!(crate::download::content_hash_count(), 2);

    let primary = paths.models.join(&manifest.id).join("model.gguf");
    let replacement = paths.models.join(&manifest.id).join("replacement.gguf");
    fs::write(&replacement, b"bad target!").unwrap();
    fs::rename(&replacement, &primary).unwrap();
    assert_eq!(
        observe_bundle(&paths.models),
        BundleSnapshot::Unavailable(BundleUnavailableReason::Invalid)
    );
    assert_eq!(crate::download::content_hash_count(), 3);
}

#[cfg(unix)]
#[test]
fn published_snapshot_reports_a_held_runtime_lock_as_busy_without_hashing() {
    let root = tempdir().unwrap();
    let paths = test_paths(root.path());
    let manifest = test_bundle_manifest();
    write_bundle_artifacts(&paths.models, &manifest);
    crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
    let model_dir = paths.models.join(&manifest.id);
    let runtime_lock = crate::catalog::ModelLock::acquire(&model_dir).unwrap();
    crate::download::reset_content_hash_count();

    assert_eq!(
        super::published_bundle_snapshot(&paths.models, &manifest),
        BundleSnapshot::Unavailable(BundleUnavailableReason::Busy)
    );
    assert_eq!(crate::download::content_hash_count(), 0);
    assert!(!model_dir.join("verification-receipt.json").exists());
    drop(runtime_lock);
}

#[test]
fn app_service_discovery_preserves_from_paths_from_env_and_snapshot() {
    let root = tempdir().unwrap();
    let paths = test_paths(root.path());
    let mut reader = SnapshotReader::new(paths.clone());
    let mut service = AppService::from_paths(paths);

    assert_eq!(service.snapshot(), reader.observe());
    let page = service
        .search_models(SearchModels::new("owner/repo".into()))
        .unwrap();
    assert_eq!(page.hits().len(), 1);
    assert_eq!(page.hits()[0].repo(), "owner/repo");
    assert_eq!(service.snapshot(), reader.observe());

    let _: fn() -> Result<AppService, String> = AppService::from_env;
}

#[test]
fn installed_inventory_projects_sorted_published_metadata_without_artifact_hashing() {
    fn assert_owned_contract<T: Clone + std::fmt::Debug + Eq + PartialEq + Send + 'static>() {}

    let root = tempdir().unwrap();
    let paths = test_paths(root.path());
    let remote = Manifest {
        version: 1,
        id: "alpha".into(),
        repo: Some("owner/repo".into()),
        revision: Some("0123456789abcdef0123456789abcdef01234567".into()),
        remote_filename: Some("alpha-q4.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 42,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let local = Manifest {
        version: 2,
        id: "local".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(Origin::Local),
        source_filename: Some("local-source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "b".repeat(64),
        size: 7,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let bundle = Manifest {
        version: 3,
        id: "bundle".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "c".repeat(64),
        size: 11,
        artifacts: Some(vec![Artifact {
            role: ArtifactRole::Model,
            local_filename: "model.gguf".into(),
            sha256: "c".repeat(64),
            size: 11,
            provenance: ArtifactProvenance::Local {
                source_filename: "bundle-source.gguf".into(),
            },
        }]),
        profile: Some(TEST_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: TEST_LLAMA_BUILD.into(),
        }),
    };
    for manifest in [&local, &bundle, &remote] {
        manifest.validate().unwrap();
        let model_dir = paths.models.join(&manifest.id);
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(manifest).unwrap(),
        )
        .unwrap();
    }
    let pending_dir = paths.models.join("pending-only");
    fs::create_dir_all(&pending_dir).unwrap();
    fs::write(
        pending_dir.join("pending.json"),
        serde_json::to_vec_pretty(&Manifest {
            id: "pending-only".into(),
            ..remote.clone()
        })
        .unwrap(),
    )
    .unwrap();

    let exact_alpha = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "alpha-q4.gguf",
        "a".repeat(64),
        42,
    );
    let different_sha = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "alpha-q4.gguf",
        "d".repeat(64),
        42,
    );
    let installed = AppService::from_paths(paths.clone())
        .installed_models()
        .unwrap();

    assert_owned_contract::<InstalledModelSummary>();
    assert_eq!(
        installed
            .iter()
            .map(InstalledModelSummary::id)
            .collect::<Vec<_>>(),
        ["alpha", "bundle", "local"]
    );
    assert_eq!(installed[0].display_name(), "alpha-q4.gguf");
    assert_eq!(installed[0].total_bytes(), 42);
    assert!(installed[0].matches_remote(&exact_alpha));
    assert!(!installed[0].matches_remote(&different_sha));
    assert!(!installed[1].matches_remote(&exact_alpha));
    assert!(!installed[2].matches_remote(&exact_alpha));
    assert!(!paths.models.join("alpha/model.gguf").exists());
    assert!(!paths.models.join("bundle/model.gguf").exists());
    assert!(!paths.models.join("local/model.gguf").exists());
}

#[test]
fn app_service_search_and_inspection_delegate_to_the_exact_core() {
    let root = tempdir().unwrap();
    let service = AppService::from_paths(test_paths(root.path()));
    let search = SearchModels::new("owner/repo".into());
    assert_eq!(
        service.search_models(search.clone()).unwrap(),
        crate::huggingface::search_models(search).unwrap()
    );

    let inspect = InspectRepository::new("invalid".into(), None);
    assert_eq!(
        service
            .inspect_repository(inspect.clone())
            .unwrap_err()
            .kind(),
        crate::huggingface::inspect_repository(inspect)
            .unwrap_err()
            .kind()
    );
}

#[test]
fn exact_app_service_input_returns_without_a_transport_request() {
    let root = tempdir().unwrap();
    let service = AppService::from_paths(test_paths(root.path()));

    let page = service
        .search_models(SearchModels::new("hf://owner/repo".into()))
        .unwrap();

    assert_eq!(page.hits().len(), 1);
    assert_eq!(page.hits()[0].repo(), "owner/repo");
    assert_eq!(page.hits()[0].downloads(), None);
}

#[test]
fn app_service_errors_and_results_are_terminal_safe() {
    let root = tempdir().unwrap();
    let service = AppService::from_paths(test_paths(root.path()));

    let error = service
        .inspect_repository(InspectRepository::new("owner/\u{1b}[31mrepo".into(), None))
        .unwrap_err();

    assert_eq!(error.kind(), DiscoveryErrorKind::InvalidRepository);
    assert_eq!(error.to_string(), "Hugging Face discovery request failed");
    assert!(!format!("{error:?}").contains("31m"));
}

#[test]
fn app_service_discovery_child() {
    let Some(root) = std::env::var_os("LOXA_APP_DISCOVERY_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let loxa_root = root.join("loxa-root");
    let hf_home = root.join("hf-home");
    let cwd = root.join("cwd");
    let before = (
        snapshot_tree(&loxa_root),
        snapshot_tree(&hf_home),
        snapshot_tree(&cwd),
    );

    let service = AppService::from_env().unwrap();
    assert_eq!(service.reader.paths.root, loxa_root);
    assert_eq!(
        service
            .search_models(SearchModels::new("owner/repo".into()))
            .unwrap()
            .hits()[0]
            .repo(),
        "owner/repo"
    );
    let error = service
        .inspect_repository(InspectRepository::new("invalid".into(), None))
        .unwrap_err();
    assert_eq!(error.kind(), DiscoveryErrorKind::InvalidRepository);

    assert!(!loxa_root.exists());
    assert_eq!(
        before,
        (
            snapshot_tree(&loxa_root),
            snapshot_tree(&hf_home),
            snapshot_tree(&cwd),
        )
    );
}

#[test]
fn app_service_discovery_creates_no_local_state_or_process() {
    let root = tempdir().unwrap();
    let loxa_root = root.path().join("loxa-root");
    let hf_home = root.path().join("hf-home");
    let home = root.path().join("home");
    let cwd = root.path().join("cwd");
    fs::create_dir_all(&hf_home).unwrap();
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&cwd).unwrap();
    let before = (
        snapshot_tree(&loxa_root),
        snapshot_tree(&hf_home),
        snapshot_tree(&cwd),
    );

    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .arg("--exact")
        .arg("app::tests::app_service_discovery_child")
        .arg("--nocapture")
        .current_dir(&cwd)
        .env("LOXA_APP_DISCOVERY_CHILD_ROOT", root.path())
        .env("LOXA_HOME", &loxa_root)
        .env("HF_HOME", &hf_home)
        .env("HOME", &home);
    for name in [
        "HF_HUB_DISABLE_IMPLICIT_TOKEN",
        "HF_TOKEN",
        "HF_TOKEN_PATH",
        "USERPROFILE",
    ] {
        child.env_remove(name);
    }
    assert!(child.status().unwrap().success());

    assert!(!loxa_root.exists());
    assert_eq!(
        before,
        (
            snapshot_tree(&loxa_root),
            snapshot_tree(&hf_home),
            snapshot_tree(&cwd),
        )
    );
}

#[test]
#[ignore]
fn live_hugging_face_discovery() {
    let query = std::env::var("LOXA_LIVE_HF_QUERY")
        .expect("set LOXA_LIVE_HF_QUERY for the ignored Hugging Face smoke test");
    let repo = std::env::var("LOXA_LIVE_HF_REPO")
        .expect("set LOXA_LIVE_HF_REPO for the ignored Hugging Face smoke test");
    let root = tempdir().unwrap();
    let loxa_root = root.path().join("absent-loxa-root");
    let service = AppService::from_paths(test_paths(&loxa_root));

    assert!(!loxa_root.exists());
    let search = service
        .search_models(SearchModels::new(query))
        .expect("keyword discovery should succeed");
    assert!(!search.hits().is_empty());
    assert!(search.hits().iter().all(|hit| !hit.repo().is_empty()));

    let exact = service
        .search_models(SearchModels::new(repo.clone()))
        .expect("exact repository routing should succeed locally");
    assert!(exact.hits().len() == 1);
    let exact_hit = &exact.hits()[0];
    assert!(exact_hit.repo() == repo);
    assert!(exact_hit.gated() == GatedStatus::Unknown);
    assert!(exact_hit.downloads().is_none());

    let plan = service
        .inspect_repository(InspectRepository::new(repo, None))
        .expect("repository inspection should succeed");
    assert!(plan.commit().len() == 40);
    assert!(plan.commit().bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(plan.commit().bytes().all(|byte| !byte.is_ascii_uppercase()));
    let candidate = plan
        .candidates()
        .iter()
        .find(|candidate| candidate.identity().is_some())
        .expect("inspection should return one eligible candidate");
    let identity = candidate.identity().expect("eligible identity");
    assert!(candidate.display_path() == identity.path());
    assert!(candidate.size() == Some(identity.size()));
    assert!(identity.repo() == plan.repo());
    assert!(identity.commit() == plan.commit());
    assert!(identity.sha256().len() == 64);
    assert!(identity
        .sha256()
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    assert!(identity
        .sha256()
        .bytes()
        .all(|byte| !byte.is_ascii_uppercase()));
    assert!(identity.size() > 0);
    assert!(!loxa_root.exists());
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
    let lock = crate::catalog::ModelLock::acquire(&model_dir).unwrap();
    drop(lock);
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
        ForegroundObservation::Running {
            provenance: RuntimeProvenance::External,
            owner: RuntimeOwner::Foreground,
            model_id: "external-model".into(),
            port: 43123,
        },
    );
    assert_eq!(external.runtime(), RuntimeSnapshot::Running);
    assert_eq!(external.runtime_port(), Some(43123));
    assert_eq!(
        external.runtime_owner(),
        Some(RuntimeOwnerSnapshot::Foreground)
    );
    assert_eq!(external.runtime_model_id(), Some("external-model"));
    assert_eq!(external.bundle_model_id(), TARGET_MODEL_ID);
    assert_eq!(
        external.runtime_inventory(),
        RuntimeInventorySnapshot::External
    );
    assert!(!format!("{external:?}").contains('/'));

    let managed = AppSnapshot::from_observation(
        BundleSnapshot::Absent,
        Some(enough_budget()),
        ForegroundObservation::Running {
            provenance: RuntimeProvenance::Managed,
            owner: RuntimeOwner::PersistentApp,
            model_id: TARGET_MODEL_ID.into(),
            port: 43124,
        },
    );
    assert_eq!(managed.runtime(), RuntimeSnapshot::Running);
    assert_eq!(managed.runtime_port(), Some(43124));
    assert_eq!(
        managed.runtime_owner(),
        Some(RuntimeOwnerSnapshot::PersistentApp)
    );
    assert_eq!(managed.runtime_model_id(), Some(TARGET_MODEL_ID));
    assert_eq!(
        managed.runtime_inventory(),
        RuntimeInventorySnapshot::Missing
    );

    let idle = AppSnapshot::from_observation(
        BundleSnapshot::Absent,
        Some(enough_budget()),
        ForegroundObservation::Idle,
    );
    assert_eq!(idle.runtime(), RuntimeSnapshot::Idle);
    assert_eq!(idle.runtime_port(), None);
    assert_eq!(idle.runtime_owner(), None);
    assert_eq!(idle.runtime_model_id(), None);
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
fn reader_observation_never_executes_or_publishes_managed_runtime_evidence() {
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
    let qualification = paths.managed_server.with_extension("qualification.json");

    let mut reader = SnapshotReader::new(paths);
    let snapshot = reader.observe();

    assert_eq!(
        snapshot.runtime_inventory(),
        RuntimeInventorySnapshot::Missing
    );
    assert!(
        !sentinel.exists(),
        "snapshot observation must not execute the managed candidate"
    );
    assert!(
        !qualification.exists(),
        "snapshot observation must not publish managed runtime evidence"
    );
}

#[cfg(unix)]
#[test]
fn reader_ignores_legacy_managed_runtime_evidence() {
    let root = tempdir().unwrap();
    let paths = test_paths(root.path());
    fs::create_dir_all(paths.managed_server.parent().unwrap()).unwrap();
    fs::write(
        &paths.managed_server,
        b"#!/bin/sh\nprintf '%s\\n' 'version: 10121 (555881ebc)'\n",
    )
    .unwrap();
    fs::set_permissions(&paths.managed_server, fs::Permissions::from_mode(0o700)).unwrap();

    write_legacy_managed_runtime_qualification(&paths.managed_server);
    let qualification = paths.managed_server.with_extension("qualification.json");
    let before = fs::read(&qualification).unwrap();

    let mut reader = SnapshotReader::new(paths);
    let snapshot = reader.observe();
    assert_eq!(
        snapshot.runtime_inventory(),
        RuntimeInventorySnapshot::Missing
    );
    assert_eq!(
        fs::read(qualification).unwrap(),
        before,
        "snapshot observation must leave legacy evidence untouched"
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
