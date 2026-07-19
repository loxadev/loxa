use crate::artifact_coordinator::{ArtifactAcquireError, ArtifactKey, ArtifactMutationCoordinator};
use crate::control_state::state_machine::{InstancePublication, Transition};
use crate::download_control::{DownloadControl, DownloadControlError, DownloadControlWorker};
use crate::download_scheduler::{
    BoundDownload, DownloadExecutor, DownloadKey, DownloadReserveOutcome, DownloadSchedulerOwner,
    DownloadWorkerPermit,
};
use crate::operation_cancellation::OperationCancellation;
use crate::verification_scheduler::{
    DownloadCompletionQueue, DownloadVerificationOutcome, DownloadVerificationOwnership,
    VerificationKey, VerificationResult,
};
use crate::{open_slice3_control_state_fixture, NodePaths, Slice3ControlStateFixture};
use loxa_core::model_inventory::{
    ArtifactState, StableVerificationInput, VerificationCache, VerifiedArtifact,
};
use loxa_core::registry::ModelEntry;
use loxa_core::supervisor::{ManagedRun, RunLifecycle, RUNTIME_STATE_SCHEMA_VERSION};
use loxa_protocol::v2::{
    DecimalU64, OperationId, StreamEpoch, V2NodeCapabilities, V2OperationStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const GOOD_SHA256: &str = "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c";

struct LaneFixture {
    root: PathBuf,
    recipes: &'static [ModelEntry],
    cache: Arc<VerificationCache>,
    downloads: Option<DownloadControl>,
    worker: Option<DownloadControlWorker>,
    entered: std::sync::mpsc::Receiver<String>,
    release: std::sync::mpsc::Sender<()>,
    control: Option<Slice3ControlStateFixture>,
}

enum FixtureDownload {
    Blocking,
    Uncertain(loxa_core::download::ArtifactFinalizationStage),
    #[cfg(unix)]
    Hardlink,
}

impl LaneFixture {
    async fn new(label: &str) -> Self {
        Self::new_with_download(label, FixtureDownload::Blocking).await
    }

    async fn new_uncertain(
        label: &str,
        stage: loxa_core::download::ArtifactFinalizationStage,
    ) -> Self {
        Self::new_with_download(label, FixtureDownload::Uncertain(stage)).await
    }

    #[cfg(unix)]
    async fn new_hardlink(label: &str) -> Self {
        Self::new_with_download(label, FixtureDownload::Hardlink).await
    }

    async fn new_with_download(label: &str, download: FixtureDownload) -> Self {
        let root = std::env::temp_dir().join(format!(
            "loxa-execution-lanes-{label}-{}-{}",
            std::process::id(),
            StreamEpoch::new_v4()
        ));
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("run/managed.json"),
            logs_dir: root.join("run/logs"),
        };
        std::fs::create_dir_all(&paths.logs_dir).unwrap();
        let baseline = ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: format!("slice4-{label}"),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-slice4-{label}-g0"),
            control_port: Some(19_434),
            port: 19_434,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
            .unwrap();
        let control = open_slice3_control_state_fixture(
            root.join("state/control-state.sqlite3"),
            NodeId::new_v4(),
            paths.clone(),
            baseline,
        )
        .unwrap();
        control
            .handle
            .publish_instance(InstancePublication {
                node_instance_id: NodeInstanceId::new_v4(),
                control_endpoint: "http://127.0.0.1:19434".into(),
                capabilities: V2NodeCapabilities {
                    model_download: true,
                    slot_load: false,
                    slot_unload: false,
                    operation_cancel: true,
                    operation_stream: true,
                },
                now_unix_ms: 11,
            })
            .await
            .unwrap();
        let recipes = Box::leak(
            vec![
                recipe("lane-a", "lane-a.gguf"),
                recipe("lane-b", "lane-b.gguf"),
            ]
            .into_boxed_slice(),
        );
        let cache = Arc::new(VerificationCache::default());
        let (downloads, worker, entered, release) = match download {
            FixtureDownload::Blocking => DownloadControl::spawn_blocking_durable_fixture_for_test(
                paths.models_dir,
                Arc::clone(&cache),
                recipes,
                b"good",
                control.handle.clone(),
            ),
            FixtureDownload::Uncertain(stage) => {
                let (downloads, worker) = DownloadControl::spawn_uncertain_durable_fixture_for_test(
                    paths.models_dir,
                    Arc::clone(&cache),
                    recipes,
                    control.handle.clone(),
                    stage,
                );
                let (_entered_tx, entered) = std::sync::mpsc::channel();
                let (release, _release_rx) = std::sync::mpsc::channel();
                (downloads, worker, entered, release)
            }
            #[cfg(unix)]
            FixtureDownload::Hardlink => {
                let (downloads, worker) = DownloadControl::spawn_hardlink_durable_fixture_for_test(
                    paths.models_dir,
                    Arc::clone(&cache),
                    recipes,
                    control.handle.clone(),
                );
                let (_entered_tx, entered) = std::sync::mpsc::channel();
                let (release, _release_rx) = std::sync::mpsc::channel();
                (downloads, worker, entered, release)
            }
        };
        Self {
            root,
            recipes,
            cache,
            downloads: Some(downloads),
            worker: Some(worker),
            entered,
            release,
            control: Some(control),
        }
    }

    fn downloads(&self) -> &DownloadControl {
        self.downloads.as_ref().unwrap()
    }

    fn control(&self) -> &Slice3ControlStateFixture {
        self.control.as_ref().unwrap()
    }

    async fn wait_status(&self, operation_id: OperationId, expected: V2OperationStatus) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let state = self.control().handle.read_snapshot().unwrap();
            if state.operations.iter().any(|operation| {
                operation.operation_id == operation_id && operation.status == expected
            }) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "status did not converge"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn shutdown(mut self) {
        self.worker.take().unwrap().stop_and_join().unwrap();
        drop(self.downloads.take());
        self.control.take().unwrap().shutdown().await;
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for LaneFixture {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.stop_and_join();
        }
        drop(self.downloads.take());
        if let Some(control) = self.control.take() {
            let _ = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(control.shutdown());
            })
            .join();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn recipe(id: &'static str, filename: &'static str) -> ModelEntry {
    ModelEntry {
        id,
        repo: "owner/repo",
        revision: REVISION,
        filename,
        sha256: GOOD_SHA256,
        size_bytes: 4,
        license: "apache-2.0",
        params: "tiny",
        quant: "Q4",
        min_free_mem_gb: 0.0,
    }
}

#[tokio::test]
async fn reserve_commit_bind_progress_verify_publish_and_v2_v1_dedupe_are_atomic() {
    let fixture = LaneFixture::new("dedupe").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        "lane-a"
    );

    let attached = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert_eq!(attached, first);
    assert_eq!(
        fixture
            .downloads()
            .start_download_async(fixture.recipes[0].id)
            .await,
        Err(DownloadControlError::Conflict)
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let running = loop {
        let operation = fixture
            .control()
            .handle
            .read_snapshot()
            .unwrap()
            .operations
            .iter()
            .find(|operation| operation.operation_id == first.operation_id)
            .unwrap()
            .clone();
        if operation
            .progress
            .as_ref()
            .is_some_and(|progress| progress.completed_bytes == DecimalU64::new(4))
        {
            break operation;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    };
    assert_eq!(running.status, V2OperationStatus::Running);
    assert_eq!(
        running.progress.unwrap().completed_bytes,
        DecimalU64::new(4)
    );
    let attached_after_progress = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert_eq!(attached_after_progress, first);
    assert_ne!(
        fixture
            .cache
            .artifact_state(&fixture.root.join("models"), &fixture.recipes[0]),
        ArtifactState::Downloaded
    );

    drop(durable.clone());
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(first.operation_id, V2OperationStatus::Succeeded)
        .await;
    assert_eq!(fixture.cache.verification_runs(), 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while fixture
        .cache
        .artifact_state(&fixture.root.join("models"), &fixture.recipes[0])
        != ArtifactState::Downloaded
    {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn duplicate_active_revision_divergence_seals_instead_of_guessing_identity() {
    let fixture = LaneFixture::new("duplicate-revision-divergence").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    durable.replace_active_download_revision_for_test(
        fixture.recipes[0].id,
        DecimalU64::new(first.revision.get() + 1),
    );

    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture.release.send(()).unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn terminal_active_revision_divergence_seals_before_release_race_handling() {
    let fixture = LaneFixture::new("terminal-duplicate-revision-divergence").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    fixture
        .control()
        .handle
        .observe_required_async(Transition::Succeeded {
            operation_id: first.operation_id,
            observed_model_id: None,
        })
        .await
        .unwrap();
    durable.replace_active_download_revision_for_test(
        fixture.recipes[0].id,
        DecimalU64::new(first.revision.get() + 1),
    );

    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture.release.send(()).unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn two_different_downloads_execute_concurrently_without_lifecycle_fifo_coupling() {
    let fixture = LaneFixture::new("parallel").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    let second = durable
        .start_download(fixture.recipes[1].id, 4)
        .await
        .unwrap();
    let mut entered = vec![
        fixture
            .entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        fixture
            .entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
    ];
    entered.sort();
    assert_eq!(entered, ["lane-a", "lane-b"]);
    assert_eq!(
        durable.start_load(fixture.recipes[0].id).await,
        Err(DownloadControlError::ModelUnavailable)
    );
    fixture.release.send(()).unwrap();
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(first.operation_id, V2OperationStatus::Succeeded)
        .await;
    fixture
        .wait_status(second.operation_id, V2OperationStatus::Succeeded)
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn detach_is_non_mutating_global_cancel_commits_first_and_partial_supports_new_id() {
    let fixture = LaneFixture::new("cancel-reuse").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    drop(durable.clone());
    assert_eq!(
        fixture.control().handle.read_snapshot().unwrap().operations[0].status,
        V2OperationStatus::Running
    );
    assert_eq!(
        fixture.downloads().cancel_async("op-1").await.unwrap(),
        loxa_core::control::contracts::OperationStatus::Running
    );
    fixture
        .wait_status(first.operation_id, V2OperationStatus::Cancelled)
        .await;
    assert!(fixture.root.join("models/lane-a.gguf.part").is_file());

    let second = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert_ne!(second.operation_id, first.operation_id);
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(second.operation_id, V2OperationStatus::Succeeded)
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn production_admission_lost_ack_poison_seals_reserved_key() {
    let fixture = LaneFixture::new("admission-lost-ack").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.fail_next_admission_with_lost_ack_for_test();
    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    assert!(fixture
        .control()
        .handle
        .read_snapshot()
        .unwrap()
        .operations
        .iter()
        .any(|operation| operation.model_id.as_deref() == Some(fixture.recipes[0].id)));
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn production_bind_failure_terminalizes_known_commit_and_lost_terminal_ack_seals() {
    let fixture = LaneFixture::new("bind-known").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.fail_next_bind_for_test(false);
    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture
        .wait_status(
            fixture.control().handle.read_snapshot().unwrap().operations[0].operation_id,
            V2OperationStatus::Failed,
        )
        .await;
    fixture.shutdown().await;

    let fixture = LaneFixture::new("bind-lost-ack").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.fail_next_bind_for_test(true);
    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    assert_eq!(
        fixture.control().handle.read_snapshot().unwrap().operations[0].status,
        V2OperationStatus::Failed
    );
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancellation_lost_ack_commits_cancelling_without_signalling_shared_work() {
    let fixture = LaneFixture::new("cancel-lost-ack").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let admission = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    durable.fail_next_cancel_with_lost_ack_for_test();
    assert_eq!(
        fixture.downloads().cancel_async("op-1").await,
        Err(DownloadControlError::Stopping)
    );
    assert_eq!(
        fixture.control().handle.read_snapshot().unwrap().operations[0].status,
        V2OperationStatus::Cancelling
    );
    assert!(fixture.root.join("models/lane-a.gguf.part").is_file());
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(admission.operation_id, V2OperationStatus::Cancelled)
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn durable_terminal_releases_key_before_cache_ack_but_artifact_lease_still_excludes_mutation()
{
    let fixture = LaneFixture::new("terminal-release-race").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.arm_completion_pause_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    fixture.release.send(()).unwrap();
    assert!(durable.wait_completion_paused_for_test(Instant::now() + Duration::from_secs(2)));
    assert_eq!(
        fixture.control().handle.read_snapshot().unwrap().operations[0].status,
        V2OperationStatus::Succeeded
    );
    let second = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert_ne!(second.operation_id, first.operation_id);
    assert!(fixture.entered.try_recv().is_err());
    durable.release_completion_for_test();
    fixture
        .wait_status(first.operation_id, V2OperationStatus::Succeeded)
        .await;
    assert_eq!(
        fixture
            .entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        fixture.recipes[0].id
    );
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(second.operation_id, V2OperationStatus::Succeeded)
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn completion_terminal_commit_lost_ack_poison_seals_and_skips_cache_publication() {
    let fixture = LaneFixture::new("completion-lost-ack").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.fail_next_completion_with_lost_ack_for_test();
    let admission = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(admission.operation_id, V2OperationStatus::Succeeded)
        .await;
    assert_ne!(
        fixture
            .cache
            .artifact_state(&fixture.root.join("models"), &fixture.recipes[0]),
        ArtifactState::Downloaded
    );
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn production_verification_saturation_waits_in_existing_worker_without_completion_growth_and_cancel_is_bounded(
) {
    let fixture = LaneFixture::new("verification-saturation").await;
    let durable = fixture.downloads().durable_execution_for_test();
    let admission = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    let mut keys = Vec::new();
    for index in 0..9_u8 {
        let path = fixture.root.join(format!("verification-held-{index}.bin"));
        std::fs::write(&path, [index]).unwrap();
        let input = StableVerificationInput::open(&path, [index; 32]).unwrap();
        keys.push(VerificationKey::new(input.stable, [index; 32]));
    }
    let held = durable.reserve_verification_capacity_for_test(keys);
    let before = durable.completion_population_for_test();
    fixture.release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(durable.completion_population_for_test(), before);
    let started = Instant::now();
    assert_eq!(
        fixture.downloads().cancel_async("op-1").await.unwrap(),
        loxa_core::control::contracts::OperationStatus::Running
    );
    fixture
        .wait_status(admission.operation_id, V2OperationStatus::Cancelled)
        .await;
    assert!(started.elapsed() < Duration::from_millis(750));
    assert_eq!(durable.completion_population_for_test(), before);
    drop(held);
    fixture.shutdown().await;
}

#[tokio::test]
async fn completing_verification_cancel_delivers_terminal_before_releasing_artifact_ownership() {
    let fixture = LaneFixture::new("completing-verification-cancel").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.arm_verification_before_publish_pause_for_test();
    durable.arm_completion_pause_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    fixture
        .entered
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    fixture.release.send(()).unwrap();
    assert!(durable
        .wait_verification_before_publish_paused_for_test(Instant::now() + Duration::from_secs(1)));

    assert_eq!(
        fixture.downloads().cancel_async("op-1").await.unwrap(),
        loxa_core::control::contracts::OperationStatus::Running
    );
    durable.release_verification_before_publish_for_test();
    fixture
        .wait_status(first.operation_id, V2OperationStatus::Cancelled)
        .await;
    assert!(durable.wait_completion_paused_for_test(Instant::now() + Duration::from_secs(1)));
    let snapshot = fixture.control().handle.read_snapshot().unwrap();
    let operation = snapshot
        .operations
        .iter()
        .find(|operation| operation.operation_id == first.operation_id)
        .unwrap();
    assert_eq!(operation.updated_revision.get(), first.revision.get() + 3);
    assert_eq!(
        snapshot
            .events
            .iter()
            .filter(|event| {
                event.operation.as_ref().is_some_and(|operation| {
                    operation.operation_id == first.operation_id
                        && matches!(
                            operation.status,
                            V2OperationStatus::Succeeded
                                | V2OperationStatus::Failed
                                | V2OperationStatus::Cancelled
                        )
                })
            })
            .count(),
        1
    );
    drop(snapshot);
    assert_ne!(
        fixture
            .cache
            .artifact_state(&fixture.root.join("models"), &fixture.recipes[0]),
        ArtifactState::Downloaded
    );
    let second = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .expect("durable terminal releases Active before completion acknowledgement");
    assert_ne!(second.operation_id, first.operation_id);
    assert!(fixture.entered.try_recv().is_err());

    durable.release_completion_for_test();
    assert_eq!(
        fixture
            .entered
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        fixture.recipes[0].id
    );
    fixture.release.send(()).unwrap();
    fixture
        .wait_status(second.operation_id, V2OperationStatus::Succeeded)
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn poisoned_completion_wait_stops_worker_and_seals_all_durable_admissions() {
    let fixture = LaneFixture::new("completion-wait-poison").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.poison_completion_wait_for_test();
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if durable.start_download(fixture.recipes[0].id, 4).await
            == Err(DownloadControlError::Stopping)
        {
            break;
        }
        assert!(Instant::now() < deadline, "completion poison did not seal");
        tokio::task::yield_now().await;
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn every_finalization_uncertainty_stage_poison_retains_active_ownership_and_seals() {
    use loxa_core::download::ArtifactFinalizationStage;
    for (label, stage) in [
        ("rename", ArtifactFinalizationStage::Rename),
        ("final-sync", ArtifactFinalizationStage::FinalFileSync),
        (
            "parent-sync",
            ArtifactFinalizationStage::ParentDirectorySync,
        ),
    ] {
        let fixture = LaneFixture::new_uncertain(label, stage).await;
        let durable = fixture.downloads().durable_execution_for_test();
        let admission = durable
            .start_download(fixture.recipes[0].id, 4)
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while durable.start_download(fixture.recipes[1].id, 4).await
            != Err(DownloadControlError::Stopping)
        {
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        let snapshot = fixture.control().handle.read_snapshot().unwrap();
        let operation = snapshot
            .operations
            .iter()
            .find(|operation| operation.operation_id == admission.operation_id)
            .unwrap();
        assert!(!matches!(
            operation.status,
            V2OperationStatus::Succeeded | V2OperationStatus::Failed | V2OperationStatus::Cancelled
        ));
        drop(snapshot);
        drop(fixture);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn post_finalization_hardlink_ambiguity_retains_active_ownership_and_seals() {
    let fixture = LaneFixture::new_hardlink("post-finalization-hardlink").await;
    let durable = fixture.downloads().durable_execution_for_test();
    durable.arm_fatal_admission_pause_for_test();
    let first = durable
        .start_download(fixture.recipes[0].id, 4)
        .await
        .unwrap();
    assert!(durable.wait_fatal_admission_closed_for_test(Instant::now() + Duration::from_secs(1)));
    let state = fixture.control().handle.read_snapshot().unwrap();
    let operation = state
        .operations
        .iter()
        .find(|operation| operation.operation_id == first.operation_id)
        .unwrap();
    assert!(!matches!(
        operation.status,
        V2OperationStatus::Succeeded | V2OperationStatus::Failed | V2OperationStatus::Cancelled
    ));
    drop(state);
    assert_ne!(
        fixture
            .cache
            .artifact_state(&fixture.root.join("models"), &fixture.recipes[0]),
        ArtifactState::Downloaded
    );

    assert_eq!(
        durable.start_download(fixture.recipes[0].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    assert_eq!(
        durable.start_download(fixture.recipes[1].id, 4).await,
        Err(DownloadControlError::Stopping)
    );
    durable.release_fatal_admission_for_test();
    drop(fixture);
}

struct NoopExecutor;

impl DownloadExecutor for NoopExecutor {
    fn execute(&self, _: BoundDownload, permit: DownloadWorkerPermit) {
        drop(permit);
    }
}

fn scheduler_key(root: &Path) -> DownloadKey {
    let artifact = ArtifactKey::from_destination(&root.join("fault.gguf")).unwrap();
    DownloadKey::new(
        "fault-model",
        "hugging-face",
        "owner/repo",
        Some(REVISION),
        "fault.gguf",
        Some([7_u8; 32]),
        Some(4),
        artifact,
    )
    .unwrap()
}

#[test]
fn definite_commit_failure_releases_reservation_and_unknown_commit_poison_seals() {
    let root = test_dir("reservation");
    let key = scheduler_key(&root);
    let (handle, owner) = DownloadSchedulerOwner::spawn(Arc::new(NoopExecutor)).unwrap();
    let reservation = match handle.reserve(key.clone()) {
        DownloadReserveOutcome::Reserved(reservation) => reservation,
        _ => panic!("fresh reservation"),
    };
    drop(reservation);
    assert!(matches!(
        handle.reserve(key.clone()),
        DownloadReserveOutcome::Reserved(_)
    ));
    drop(owner);

    let (handle, owner) = DownloadSchedulerOwner::spawn(Arc::new(NoopExecutor)).unwrap();
    let reservation = match handle.reserve(key) {
        DownloadReserveOutcome::Reserved(reservation) => reservation,
        _ => panic!("fresh reservation"),
    };
    drop(reservation.poison());
    assert!(matches!(
        handle.reserve(scheduler_key(&root)),
        DownloadReserveOutcome::Stopping
    ));
    drop(owner);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bind_after_scheduler_stop_fails_closed_without_execution() {
    let root = test_dir("bind-stop");
    let key = scheduler_key(&root);
    let (handle, owner) = DownloadSchedulerOwner::spawn(Arc::new(NoopExecutor)).unwrap();
    let reservation = match handle.reserve(key) {
        DownloadReserveOutcome::Reserved(reservation) => reservation,
        _ => panic!("fresh reservation"),
    };
    handle.stop();
    assert!(reservation
        .bind(
            OperationId::new_v4(),
            DecimalU64::new(1),
            OperationCancellation::new()
        )
        .is_err());
    drop(owner);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn active_mutation_excludes_load_read_and_completion_poison_retains_lease() {
    let root = test_dir("lease");
    let path = root.join("lease.gguf");
    std::fs::write(&path, b"good").unwrap();
    let key = ArtifactKey::from_destination(&path).unwrap();
    let coordinator = ArtifactMutationCoordinator::new();
    let lease = coordinator.try_acquire_mutation(key.clone()).unwrap();
    assert_eq!(
        coordinator.try_acquire_read(key.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );
    let input = StableVerificationInput::open(&path, [7_u8; 32]).unwrap();
    let queue = DownloadCompletionQueue::new(1);
    let completion = queue.reserve().unwrap();
    completion
        .publish(DownloadVerificationOutcome {
            ownership: DownloadVerificationOwnership {
                operation_id: OperationId::new_v4(),
                admission_revision: DecimalU64::new(1),
                cancellation: OperationCancellation::new(),
                artifact: lease,
            },
            stable_identity: input.stable,
            result: VerificationResult::Verified(VerifiedArtifact {
                size_bytes: 4,
                expected_sha256: "07".repeat(32),
                matches: true,
            }),
        })
        .unwrap();
    queue.ready().unwrap().take_ready().unwrap().poison();
    assert_eq!(
        coordinator.try_acquire_read(key.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );
    queue.dispose_poisoned_for_test();
    assert!(coordinator.try_acquire_read(key).is_ok());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn download_completion_destination_supports_a_bounded_blocking_receive() {
    let completions = DownloadCompletionQueue::new(1);
    let deadline = Instant::now() + Duration::from_millis(1);
    assert!(matches!(
        completions.wait_ready_until(deadline),
        crate::verification_scheduler::CompletionWaitOutcome::TimedOut
    ));
}

#[test]
fn scheduler_cache_publication_rejects_identity_replacement_without_rehash() {
    let root = test_dir("cache-race");
    let recipe = recipe("cache-race", "cache.gguf");
    let path = root.join(recipe.filename);
    std::fs::write(&path, b"good").unwrap();
    let input = StableVerificationInput::open(
        &path,
        [
            0x77, 0x0e, 0x60, 0x76, 0x24, 0xd6, 0x89, 0x26, 0x5c, 0xa6, 0xc4, 0x48, 0x84, 0xd0,
            0x80, 0x7d, 0x9b, 0x05, 0x4d, 0x23, 0xc4, 0x73, 0xc1, 0x06, 0xc7, 0x2b, 0xe9, 0xde,
            0x08, 0xb7, 0x37, 0x6c,
        ],
    )
    .unwrap();
    let stable = input.stable;
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, b"good").unwrap();
    let cache = VerificationCache::default();
    assert!(cache
        .publish_verified_recipe(
            &root,
            &recipe,
            &stable,
            &VerifiedArtifact {
                size_bytes: 4,
                expected_sha256: GOOD_SHA256.into(),
                matches: true,
            },
        )
        .is_err());
    assert_eq!(cache.verification_runs(), 0);
    assert_ne!(
        cache.artifact_state(&root, &recipe),
        ArtifactState::Downloaded
    );
    let _ = std::fs::remove_dir_all(root);
}

fn test_dir(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "loxa-execution-lanes-unit-{label}-{}-{}",
        std::process::id(),
        OperationId::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}
