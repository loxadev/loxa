use crate::actor::{Mutation, MutationCancellation, MutationExecutor, NodeActor};
use crate::control_router::{router, ControlState};
use crate::control_state::state_machine::{InstancePublication, Transition};
use crate::download_control::{
    DownloadControl, DownloadControlError, DownloadControlWorker, DurableExecutionControl,
};
use crate::{open_slice3_control_state_fixture, NodePaths, Slice3ControlStateFixture};
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use loxa_core::control::auth::ControlToken;
use loxa_core::model_inventory::VerificationCache;
use loxa_core::registry::{ModelEntry, REGISTRY};
use loxa_core::supervisor::{ManagedRun, RunLifecycle, RUNTIME_STATE_SCHEMA_VERSION};
use loxa_protocol::v2::{OperationId, StreamEpoch, V2NodeCapabilities, V2OperationStatus};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc;
use std::sync::Arc;
use tower::ServiceExt;

struct DurableFixture {
    root: PathBuf,
    token: ControlToken,
    node_id: NodeId,
    instance_id: NodeInstanceId,
    downloads: DownloadControl,
    download_worker: DownloadControlWorker,
    control: Slice3ControlStateFixture,
}

impl DurableFixture {
    async fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "loxa-slice3-execution-{label}-{}-{}",
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
            run_id: format!("slice3-{label}"),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-slice3-{label}-g0"),
            control_port: Some(19_431),
            port: 19_431,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
            .unwrap();
        let node_id = NodeId::new_v4();
        let instance_id = NodeInstanceId::new_v4();
        let bootstrap = open_slice3_control_state_fixture(
            root.join("state/control-state.sqlite3"),
            node_id,
            paths.clone(),
            baseline,
        )
        .unwrap();
        bootstrap
            .handle
            .publish_instance(InstancePublication {
                node_instance_id: instance_id,
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: V2NodeCapabilities {
                    model_download: true,
                    slot_load: true,
                    slot_unload: true,
                    operation_cancel: true,
                    operation_stream: true,
                },
                now_unix_ms: 11,
            })
            .await
            .unwrap();
        let registry = &REGISTRY[0];
        let recipes: &'static [ModelEntry] = Box::leak(
            vec![ModelEntry {
                id: registry.id,
                repo: registry.repo,
                revision: registry.revision,
                filename: "fixture.gguf",
                sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
                size_bytes: 4,
                license: registry.license,
                params: registry.params,
                quant: registry.quant,
                min_free_mem_gb: 0.0,
            }]
            .into_boxed_slice(),
        );
        let (downloads, download_worker) = DownloadControl::spawn_durable_fixture_for_test(
            paths.models_dir,
            Arc::new(VerificationCache::default()),
            recipes,
            b"good",
            bootstrap.handle.clone(),
        );
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        Self {
            root,
            token,
            node_id,
            instance_id,
            downloads,
            download_worker,
            control: bootstrap,
        }
    }

    fn authorized_json_request(&self, path: &str, body: String) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.token.expose_for_authorization()),
            )
            .body(Body::from(body))
            .unwrap()
    }

    fn authorized_get(&self, path: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(path)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.token.expose_for_authorization()),
            )
            .body(Body::empty())
            .unwrap()
    }

    async fn shutdown(self) {
        self.download_worker.stop_and_join().unwrap();
        drop(self.downloads);
        self.control.shutdown().await;
        let _ = std::fs::remove_dir_all(self.root);
    }
}

struct BlockingActor {
    submitted: mpsc::Sender<String>,
    release: mpsc::Receiver<()>,
}

impl MutationExecutor for BlockingActor {
    fn execute(&mut self, id: &str, _: &Mutation, cancellation: &MutationCancellation) {
        self.submitted.send(id.to_owned()).unwrap();
        while self
            .release
            .recv_timeout(std::time::Duration::from_millis(5))
            .is_err()
            && !cancellation.is_cancelled()
        {}
    }
}

struct ClaimedTerminalActor {
    control_state: crate::control_state::ControlStateHandle,
    claimed: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl MutationExecutor for ClaimedTerminalActor {
    fn execute(&mut self, id: &str, _: &Mutation, cancellation: &MutationCancellation) {
        let operation_id = OperationId::from_str(id).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        self.control_state
            .observe_required_blocking_until(
                Transition::Started {
                    operation_id,
                    progress: None,
                },
                deadline,
            )
            .unwrap();
        assert!(cancellation.claim_terminal());
        self.claimed.send(()).unwrap();
        self.release.recv().unwrap();
        self.control_state
            .observe_required_blocking_until(
                Transition::Succeeded {
                    operation_id,
                    observed_model_id: None,
                },
                deadline,
            )
            .unwrap();
    }
}

#[tokio::test]
async fn durable_default_slot_preserves_exact_v1_node_bytes() {
    let fixture = DurableFixture::new("node-projection").await;
    let app = router(ControlState::new(
        fixture.token.clone(),
        fixture.node_id,
        fixture.instance_id,
        fixture.downloads.clone(),
    ));
    let response = app
        .oneshot(fixture.authorized_get("/loxa/v1/node"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        body.as_ref(),
        br#"{"status":"unloaded","active_model_id":null,"operation_id":null,"error":null}"#
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn durable_control_yields_only_the_narrow_execution_authority() {
    let fixture = DurableFixture::new("execution-authority").await;
    assert!(fixture.downloads.durable_execution().is_some());
    fixture.shutdown().await;
}

#[tokio::test]
async fn v1_route_accepts_exact_op_alias_after_durable_uuid_admission() {
    let fixture = DurableFixture::new("route-alias").await;
    let model_id = REGISTRY[0].id;
    let app = router(ControlState::new(
        fixture.token.clone(),
        fixture.node_id,
        fixture.instance_id,
        fixture.downloads.clone(),
    ));
    let response = app
        .oneshot(fixture.authorized_json_request(
            "/loxa/v1/models/download",
            format!(r#"{{"model_id":"{model_id}"}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), br#"{"operation_id":"op-1"}"#);

    let state = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(state.current_instance_v1.operations.len(), 1);
    assert_eq!(
        state.current_instance_v1.operations[0].v1_operation_id,
        "op-1"
    );
    assert_eq!(
        state.current_instance_v1.operations[0]
            .operation
            .operation_id
            .to_string()
            .len(),
        36
    );
    assert!(
        fixture.control.handle.is_healthy(),
        "durable writer poisoned after execution: {state:?}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn submission_failure_terminalizes_same_durable_operation_without_replacement_id() {
    let fixture = DurableFixture::new("submit-failure").await;
    fixture.downloads.stop_actor();
    let durable = fixture.downloads.durable_execution_for_test();
    let error = durable.start_download(REGISTRY[0].id, 4).await.unwrap_err();
    assert_eq!(error, DownloadControlError::Stopping);
    let state = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(state.operations.len(), 1);
    assert_eq!(state.operations[0].status, V2OperationStatus::Failed);
    assert_eq!(state.current_instance_v1.operations.len(), 1);
    assert_eq!(
        state.operations[0].operation_id,
        state.current_instance_v1.operations[0]
            .operation
            .operation_id
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn durable_v1_stream_is_snapshot_first_and_uses_compatibility_sequence() {
    let fixture = DurableFixture::new("stream").await;
    let (snapshot, mut events) = fixture
        .downloads
        .subscribe_v1_with_snapshot(0)
        .await
        .unwrap();
    assert_eq!(snapshot.cursor, 0);
    assert!(snapshot.operations.is_empty());
    let operation_id = fixture
        .downloads
        .start_download_async(REGISTRY[0].id)
        .await
        .unwrap();
    assert_eq!(operation_id, "op-1");
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.sequence, 1);
    assert_eq!(event.operation.id, "op-1");
    fixture.shutdown().await;
}

#[tokio::test]
async fn concurrent_lifecycle_admission_has_one_commit_and_submits_the_same_uuid() {
    let fixture = DurableFixture::new("lifecycle-conflict").await;
    let (submitted_tx, submitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (actor, actor_worker) = NodeActor::spawn(BlockingActor {
        submitted: submitted_tx,
        release: release_rx,
    });
    let durable =
        DurableExecutionControl::with_actor_for_test(fixture.control.handle.clone(), actor.clone());
    let first = durable.clone();
    let second = durable.clone();
    let (left, right) = tokio::join!(first.start_load("model-a"), second.start_load("model-b"));
    let accepted = match (left, right) {
        (Ok(accepted), Err(DownloadControlError::Conflict))
        | (Err(DownloadControlError::Conflict), Ok(accepted)) => accepted,
        result => panic!("expected one accepted lifecycle admission: {result:?}"),
    };
    assert_eq!(
        submitted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        accepted.operation_id.to_string()
    );
    let state = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(state.operations.len(), 1);
    assert_eq!(state.operations[0].operation_id, accepted.operation_id);
    release_tx.send(()).unwrap();
    actor.stop();
    actor_worker.join().unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn durable_cancel_commits_once_and_preserves_v1_terminal_error_on_retry() {
    let fixture = DurableFixture::new("cancel-terminal").await;
    let (submitted_tx, submitted_rx) = mpsc::channel();
    let (_release_tx, release_rx) = mpsc::channel();
    let (actor, actor_worker) = NodeActor::spawn(BlockingActor {
        submitted: submitted_tx,
        release: release_rx,
    });
    let durable =
        DurableExecutionControl::with_actor_for_test(fixture.control.handle.clone(), actor.clone());
    let admitted = durable.start_download("fixture", 4).await.unwrap();
    assert_eq!(
        submitted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        admitted.operation_id.to_string()
    );
    assert_eq!(
        durable.cancel("op-1").await.unwrap(),
        loxa_core::control::contracts::OperationStatus::Cancelled
    );
    let after = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(after.operations[0].status, V2OperationStatus::Cancelled);
    assert_eq!(
        durable.cancel("op-1").await,
        Err(DownloadControlError::Terminal)
    );
    assert_eq!(
        fixture.control.handle.read_snapshot().unwrap().revision,
        after.revision
    );
    actor.stop();
    actor_worker.join().unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn terminal_claim_prevents_a_late_cancel_from_reporting_false_success_or_transient_503() {
    let fixture = DurableFixture::new("terminal-cancel-race").await;
    let (claimed_tx, claimed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (actor, actor_worker) = NodeActor::spawn(ClaimedTerminalActor {
        control_state: fixture.control.handle.clone(),
        claimed: claimed_tx,
        release: release_rx,
    });
    let durable =
        DurableExecutionControl::with_actor_for_test(fixture.control.handle.clone(), actor.clone());
    durable.start_download("fixture", 4).await.unwrap();
    claimed_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let cancelling = durable.clone();
    let cancel = tokio::spawn(async move { cancelling.cancel("op-1").await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    release_tx.send(()).unwrap();
    assert_eq!(cancel.await.unwrap(), Err(DownloadControlError::Terminal));
    assert_eq!(
        fixture.control.handle.read_snapshot().unwrap().operations[0].status,
        V2OperationStatus::Succeeded
    );
    actor.stop();
    actor_worker.join().unwrap();
    fixture.shutdown().await;
}
