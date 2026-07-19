use crate::control_router::{router_with_optional_v2, ControlState};
use crate::control_state::state_machine::InstancePublication;
use crate::download_control::{DownloadControl, DownloadControlError, DownloadControlWorker};
use crate::{open_slice3_control_state_fixture, NodePaths, Slice3ControlStateFixture};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use futures_util::StreamExt;
use loxa_core::control::auth::ControlToken;
use loxa_core::control::contracts::{ControlErrorBody, ControlErrorCode};
use loxa_core::model_inventory::VerificationCache;
use loxa_core::registry::REGISTRY;
use loxa_core::supervisor::{ManagedRun, RunLifecycle, RUNTIME_STATE_SCHEMA_VERSION};
use loxa_protocol::v2::{
    DecimalU64, OperationId, V2ControlErrorBody, V2ControlErrorCode, V2NodeCapabilities,
    V2Operation, V2OperationAccepted, V2OperationKind, V2OperationStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::PathBuf;
use std::sync::Arc;
use tower::ServiceExt;

struct HttpCompatibilityFixture {
    root: PathBuf,
    token: ControlToken,
    app: axum::Router,
    downloads: Option<DownloadControl>,
    worker: Option<DownloadControlWorker>,
    entered: std::sync::mpsc::Receiver<String>,
    release: std::sync::mpsc::Sender<()>,
    control: Option<Slice3ControlStateFixture>,
}

impl HttpCompatibilityFixture {
    async fn new(label: &str) -> Self {
        Self::new_inner(label, None).await
    }

    async fn with_download_error(label: &str, error: DownloadControlError) -> Self {
        Self::new_inner(label, Some(error)).await
    }

    async fn new_inner(label: &str, download_error: Option<DownloadControlError>) -> Self {
        let root = std::env::temp_dir().join(format!(
            "loxa-router-compatibility-{label}-{}-{}",
            std::process::id(),
            loxa_protocol::v2::StreamEpoch::new_v4()
        ));
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("run/managed.json"),
            logs_dir: root.join("run/logs"),
        };
        std::fs::create_dir_all(&paths.logs_dir).unwrap();
        let baseline = ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: format!("router-compatibility-{label}"),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-router-compatibility-{label}-g0"),
            control_port: Some(19_438),
            port: 19_438,
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
                control_endpoint: "http://127.0.0.1:19438".into(),
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
        let (downloads, worker, entered, release) =
            DownloadControl::spawn_blocking_durable_fixture_for_test(
                paths.models_dir,
                Arc::new(VerificationCache::default()),
                REGISTRY,
                b"fixture",
                control.handle.clone(),
            );
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        let app = if let Some(error) = download_error {
            let state = crate::v2_control_router::V2ControlState::new_with_inventory_for_test(
                token.clone(),
                control.handle.clone(),
                downloads.durable_execution().unwrap(),
                downloads.clone(),
            )
            .with_injected_download_error_for_test(error);
            crate::v2_control_router::router(state)
        } else {
            let snapshot = control.handle.read_snapshot().unwrap();
            let node = snapshot.node.as_ref().unwrap();
            let state = ControlState::new(
                token.clone(),
                node.node_id,
                node.node_instance_id,
                downloads.clone(),
            );
            router_with_optional_v2(state, Some(control.handle.clone())).unwrap()
        };
        Self {
            root,
            token,
            app,
            downloads: Some(downloads),
            worker: Some(worker),
            entered,
            release,
            control: Some(control),
        }
    }

    fn request(&self, method: Method, uri: &str, body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.token.expose_for_authorization()),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap()
    }

    async fn body(response: axum::response::Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec()
    }

    async fn shutdown(mut self) {
        for _ in 0..16 {
            let _ = self.release.send(());
        }
        let downloads = self.downloads.take().unwrap();
        let worker = self.worker.take().unwrap();
        let now = std::time::Instant::now();
        match worker.shutdown_staged(
            downloads,
            crate::download_control::ExecutionShutdownDeadlines {
                verification: now + std::time::Duration::from_secs(2),
                download: now + std::time::Duration::from_secs(2),
                lifecycle: now + std::time::Duration::from_secs(2),
                finalize: now + std::time::Duration::from_secs(2),
            },
        ) {
            crate::download_control::ExecutionShutdownResult::Stopped
            | crate::download_control::ExecutionShutdownResult::Failed(_) => {}
            crate::download_control::ExecutionShutdownResult::Retained(retained) => {
                retained.dispose_for_test()
            }
        }
        self.control.take().unwrap().shutdown().await;
        let _ = std::fs::remove_dir_all(self.root);
    }
}

fn operation(
    operation_id: &str,
    kind: V2OperationKind,
    status: V2OperationStatus,
    created_revision: u64,
) -> V2Operation {
    let lifecycle = matches!(kind, V2OperationKind::Load | V2OperationKind::Unload);
    V2Operation {
        operation_id: operation_id.parse().unwrap(),
        node_id: "123e4567-e89b-42d3-a456-426614174000"
            .parse::<NodeId>()
            .unwrap(),
        kind,
        status,
        slot_id: lifecycle.then(|| "123e4567-e89b-42d3-8456-426614174002".parse().unwrap()),
        model_id: (kind != V2OperationKind::Unload).then(|| "gemma-3-4b-it-q4".into()),
        progress: None,
        error: None,
        created_revision: DecimalU64::new(created_revision),
        updated_revision: DecimalU64::new(created_revision),
        created_at_unix_ms: DecimalU64::new(created_revision),
        updated_at_unix_ms: DecimalU64::new(created_revision),
    }
}

fn active(operation: &V2Operation) -> bool {
    matches!(
        operation.status,
        V2OperationStatus::Queued | V2OperationStatus::Running | V2OperationStatus::Cancelling
    )
}

fn presentation_key(operation: &V2Operation) -> (u8, DecimalU64, OperationId) {
    let lane = match operation.kind {
        V2OperationKind::Load | V2OperationKind::Unload => 0,
        V2OperationKind::Download => 1,
    };
    let status = match operation.status {
        V2OperationStatus::Cancelling => 0,
        V2OperationStatus::Running => 1,
        V2OperationStatus::Queued => 2,
        _ => 3,
    };
    (
        lane * 4 + status,
        operation.created_revision,
        operation.operation_id,
    )
}

#[test]
fn presentation_prioritizes_active_lifecycle_and_keeps_canonical_collection_unchanged() {
    let canonical = [
        operation(
            "11111111-1111-4111-8111-111111111111",
            V2OperationKind::Load,
            V2OperationStatus::Running,
            1,
        ),
        operation(
            "22222222-2222-4222-8222-222222222222",
            V2OperationKind::Download,
            V2OperationStatus::Running,
            2,
        ),
        operation(
            "33333333-3333-4333-8333-333333333333",
            V2OperationKind::Download,
            V2OperationStatus::Cancelling,
            3,
        ),
        operation(
            "44444444-4444-4444-8444-444444444444",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            4,
        ),
        operation(
            "55555555-5555-4555-8555-555555555555",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            4,
        ),
        operation(
            "66666666-6666-4666-8666-666666666666",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            5,
        ),
    ];
    assert!(canonical
        .iter()
        .all(|operation| operation.validate().is_ok()));
    let canonical_ids = canonical
        .iter()
        .map(|operation| operation.operation_id)
        .collect::<Vec<_>>();
    let mut active_operations = canonical
        .iter()
        .filter(|operation| active(operation))
        .collect::<Vec<_>>();
    active_operations.sort_by_key(|operation| presentation_key(operation));

    assert_eq!(active_operations.len(), 6);
    assert_eq!(
        active_operations
            .iter()
            .take(5)
            .map(|operation| operation.operation_id)
            .collect::<Vec<_>>(),
        vec![
            "11111111-1111-4111-8111-111111111111"
                .parse::<OperationId>()
                .unwrap(),
            "33333333-3333-4333-8333-333333333333".parse().unwrap(),
            "22222222-2222-4222-8222-222222222222".parse().unwrap(),
            "44444444-4444-4444-8444-444444444444".parse().unwrap(),
            "55555555-5555-4555-8555-555555555555".parse().unwrap(),
        ]
    );
    assert_eq!(active_operations.len() - 5, 1);
    assert_eq!(
        canonical
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<Vec<_>>(),
        canonical_ids
    );
}

#[tokio::test]
async fn authenticated_v2_exact_duplicate_is_accepted_but_v1_duplicate_remains_conflict() {
    let fixture = HttpCompatibilityFixture::new("duplicate-wire").await;
    let model_id = REGISTRY[0].id;
    let v2_path = format!("/loxa/v2/models/{model_id}/download");

    let first = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::POST, &v2_path, "{}"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first_body = HttpCompatibilityFixture::body(first).await;
    let first: V2OperationAccepted = serde_json::from_slice(&first_body).unwrap();
    let first_json = serde_json::from_slice::<serde_json::Value>(&first_body).unwrap();
    assert_eq!(
        first_json.as_object().unwrap().keys().collect::<Vec<_>>(),
        vec!["epoch", "operation_id", "revision"]
    );
    fixture
        .entered
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let before_duplicates = fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap();

    let duplicate = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::POST, &v2_path, "{}"))
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::ACCEPTED);
    let duplicate_body = HttpCompatibilityFixture::body(duplicate).await;
    let duplicate: V2OperationAccepted = serde_json::from_slice(&duplicate_body).unwrap();
    assert_eq!(duplicate, first);

    let v1 = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            "/loxa/v1/models/download",
            serde_json::to_vec(&serde_json::json!({ "model_id": model_id })).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(v1.status(), StatusCode::CONFLICT);
    let v1_error: ControlErrorBody =
        serde_json::from_slice(&HttpCompatibilityFixture::body(v1).await).unwrap();
    assert_eq!(v1_error.code, ControlErrorCode::OperationConflict);
    assert_eq!(
        v1_error.message,
        "a conflicting model operation is already active"
    );

    let snapshot = fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap();
    assert_eq!(snapshot.revision, before_duplicates.revision);
    assert_eq!(snapshot.events.len(), before_duplicates.events.len());
    assert_eq!(snapshot.operations.len(), 1);
    assert_eq!(snapshot.operations[0].operation_id, first.operation_id);
    assert_eq!(snapshot.operations[0].created_revision, first.revision);
    fixture.shutdown().await;
}

#[tokio::test]
async fn authenticated_v2_scheduler_saturation_is_exact_operation_conflict() {
    let fixture = HttpCompatibilityFixture::with_download_error(
        "scheduler-overload-wire",
        DownloadControlError::Conflict,
    )
    .await;
    let model_id = REGISTRY[0].id;
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/models/{model_id}/download"),
            "{}",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = HttpCompatibilityFixture::body(response).await;
    let strict: V2ControlErrorBody = serde_json::from_slice(&body).unwrap();
    assert_eq!(strict.code, V2ControlErrorCode::OperationConflict);
    assert_eq!(strict.message, "A conflicting operation is active.");
    let object = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
    assert_eq!(
        object.as_object().unwrap().keys().collect::<Vec<_>>(),
        vec!["code", "message"]
    );
    assert!(fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap()
        .operations
        .is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn authenticated_v2_writer_saturation_is_exact_state_writer_overloaded() {
    let fixture = HttpCompatibilityFixture::with_download_error(
        "writer-overload-wire",
        DownloadControlError::WriterOverloaded,
    )
    .await;
    let model_id = REGISTRY[0].id;
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/models/{model_id}/download"),
            "{}",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = HttpCompatibilityFixture::body(response).await;
    let strict: V2ControlErrorBody = serde_json::from_slice(&body).unwrap();
    assert_eq!(strict.code, V2ControlErrorCode::StateWriterOverloaded);
    assert_eq!(strict.message, "The durable state writer is overloaded.");
    let object = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
    assert_eq!(
        object.as_object().unwrap().keys().collect::<Vec<_>>(),
        vec!["code", "message"]
    );
    assert!(fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap()
        .operations
        .is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_observer_detaches_while_explicit_cancel_mutates_the_shared_operation() {
    let fixture = HttpCompatibilityFixture::new("cancel-detach-wire").await;
    let model_id = REGISTRY[0].id;
    let download_path = format!("/loxa/v2/models/{model_id}/download");
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::POST, &download_path, "{}"))
        .await
        .unwrap();
    let admitted: V2OperationAccepted =
        serde_json::from_slice(&HttpCompatibilityFixture::body(response).await).unwrap();
    assert_eq!(
        fixture
            .entered
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap(),
        model_id
    );

    let before_detach = fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap();
    let event_count = before_detach.events.len();
    let revision = before_detach.revision;
    let observed = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/events", Body::empty()))
        .await
        .unwrap();
    assert_eq!(observed.status(), StatusCode::OK);
    let mut live_observer = observed.into_body().into_data_stream();
    let first_frame = tokio::time::timeout(std::time::Duration::from_secs(2), live_observer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first_frame
        .windows(b"event: snapshot".len())
        .any(|window| { window == b"event: snapshot" }));
    drop(live_observer);
    let after_detach = fixture
        .control
        .as_ref()
        .unwrap()
        .handle
        .read_snapshot()
        .unwrap();
    assert_eq!(after_detach.revision, revision);
    assert_eq!(after_detach.events.len(), event_count);

    let cancelled = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/operations/{}/cancel", admitted.operation_id),
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    let cancelled: V2OperationAccepted =
        serde_json::from_slice(&HttpCompatibilityFixture::body(cancelled).await).unwrap();
    assert_eq!(cancelled.operation_id, admitted.operation_id);
    assert!(cancelled.revision > revision);

    let final_snapshot = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = fixture
                .control
                .as_ref()
                .unwrap()
                .handle
                .read_snapshot()
                .unwrap();
            let operation = snapshot
                .operations
                .iter()
                .find(|operation| operation.operation_id == admitted.operation_id)
                .unwrap();
            if operation.status == V2OperationStatus::Cancelled {
                break snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let operation = final_snapshot
        .operations
        .iter()
        .find(|operation| operation.operation_id == admitted.operation_id)
        .unwrap();
    assert_eq!(operation.updated_revision, final_snapshot.revision);
    let event = final_snapshot.events.last().unwrap();
    assert_eq!(event.revision, final_snapshot.revision);
    assert_eq!(event.operation_id, Some(admitted.operation_id));
    assert_eq!(
        event.operation.as_ref().unwrap().status,
        V2OperationStatus::Cancelled
    );
    fixture.shutdown().await;
}
