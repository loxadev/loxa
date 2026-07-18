use crate::actor::{Mutation, MutationCancellation, MutationExecutor, NodeActor};
use crate::control_state::state_machine::{AdmissionRequest, InstancePublication, Transition};
use crate::download_control::DurableExecutionControl;
use crate::v2_control_router::{router, V2ControlState};
use crate::{open_slice3_control_state_fixture, NodePaths, Slice3ControlStateFixture};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use futures_util::StreamExt;
use loxa_core::control::auth::ControlToken;
use loxa_core::supervisor::{ManagedRun, RunLifecycle, RUNTIME_STATE_SCHEMA_VERSION};
use loxa_protocol::v2::{
    DecimalU64, OperationId, StreamEpoch, V2ControlErrorBody, V2ControlEvent, V2NodeCapabilities,
    V2NodeCollection, V2OperationAccepted, V2OperationCollection, V2OperationEnvelope,
    V2OperationProgress, V2OperationStatus, V2ReconnectSnapshot, V2SlotCollection, V2SlotStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::PathBuf;
use std::sync::mpsc;
use tower::ServiceExt;

struct WaitingExecutor {
    submitted: mpsc::Sender<String>,
    control: crate::control_state::ControlStateHandle,
}

impl MutationExecutor for WaitingExecutor {
    fn execute(&mut self, id: &str, mutation: &Mutation, cancellation: &MutationCancellation) {
        self.submitted.send(id.to_owned()).unwrap();
        let operation_id: OperationId = id.parse().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        self.control
            .observe_required_blocking_until(
                Transition::Started {
                    operation_id,
                    progress: None,
                },
                deadline,
            )
            .unwrap();
        match mutation {
            Mutation::Download { .. } => {
                while !cancellation.is_cancelled() {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
            Mutation::Load { model_id } => {
                if cancellation.claim_terminal() {
                    self.control
                        .observe_required_blocking_until(
                            Transition::Succeeded {
                                operation_id,
                                observed_model_id: Some(model_id.clone()),
                            },
                            deadline,
                        )
                        .unwrap();
                }
            }
            Mutation::Unload => {
                if cancellation.claim_terminal() {
                    self.control
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
        }
    }
}

struct RouterFixture {
    root: PathBuf,
    token: ControlToken,
    node_id: NodeId,
    slot_id: loxa_protocol::v2::SlotId,
    epoch: StreamEpoch,
    app: axum::Router,
    execution: DurableExecutionControl,
    actor: crate::actor::NodeActorHandle,
    actor_worker: std::thread::JoinHandle<()>,
    submissions: mpsc::Receiver<String>,
    control: Slice3ControlStateFixture,
}

impl RouterFixture {
    async fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "loxa-slice3-router-{label}-{}-{}",
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
            run_id: format!("slice3-router-{label}"),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-slice3-router-{label}-g0"),
            control_port: Some(19_432),
            port: 19_432,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
            .unwrap();
        let node_id = NodeId::new_v4();
        let instance_id = NodeInstanceId::new_v4();
        let control = open_slice3_control_state_fixture(
            root.join("state/control-state.sqlite3"),
            node_id,
            paths,
            baseline,
        )
        .unwrap();
        control
            .handle
            .publish_instance(InstancePublication {
                node_instance_id: instance_id,
                control_endpoint: "http://127.0.0.1:19432".into(),
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
        let snapshot = control.handle.read_snapshot().unwrap();
        let slot_id = snapshot.slot.slot_id;
        let epoch = snapshot.events.last().unwrap().epoch;
        let (submitted, submissions) = mpsc::channel();
        let (actor, actor_worker) = NodeActor::spawn(WaitingExecutor {
            submitted,
            control: control.handle.clone(),
        });
        let execution =
            DurableExecutionControl::with_actor_for_test(control.handle.clone(), actor.clone());
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        let app = router(V2ControlState::new_for_test(
            token.clone(),
            control.handle.clone(),
            execution.clone(),
        ));
        Self {
            root,
            token,
            node_id,
            slot_id,
            epoch,
            app,
            execution,
            actor,
            actor_worker,
            submissions,
            control,
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

    async fn shutdown(self) {
        self.actor.stop();
        self.actor_worker.join().unwrap();
        self.control.shutdown().await;
        let _ = std::fs::remove_dir_all(self.root);
    }
}

async fn body_bytes(response: axum::response::Response) -> axum::body::Bytes {
    axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap()
}

async fn sanitized_response(
    response: axum::response::Response,
) -> (StatusCode, Vec<(String, String)>, axum::body::Bytes) {
    let status = response.status();
    let mut headers: Vec<_> = response
        .headers()
        .iter()
        .filter(|(name, _)| *name != header::CONTENT_LENGTH)
        .map(|(name, value)| (name.as_str().to_owned(), value.to_str().unwrap().to_owned()))
        .collect();
    headers.sort_unstable();
    (status, headers, body_bytes(response).await)
}

#[tokio::test]
async fn v2_control_router_node_and_default_slot_collections_have_exact_envelopes() {
    let fixture = RouterFixture::new("collections").await;
    let nodes_response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/nodes", ""))
        .await
        .unwrap();
    assert_eq!(nodes_response.status(), StatusCode::OK);
    let nodes: V2NodeCollection =
        serde_json::from_slice(&body_bytes(nodes_response).await).unwrap();
    assert_eq!(nodes.nodes.len(), 1);
    assert_eq!(nodes.nodes[0].node_id, fixture.node_id);
    assert_eq!(nodes.epoch, fixture.epoch);

    let path = format!("/loxa/v2/nodes/{}/slots", fixture.node_id);
    let slots_response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, &path, ""))
        .await
        .unwrap();
    assert_eq!(slots_response.status(), StatusCode::OK);
    let slots: V2SlotCollection =
        serde_json::from_slice(&body_bytes(slots_response).await).unwrap();
    assert_eq!(slots.node_id, fixture.node_id);
    assert_eq!(slots.slots[0].slot_id, fixture.slot_id);
    assert_eq!(slots.slots[0].name, "default");
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_auth_precedes_query_parsing_and_empty_body_is_strict() {
    let fixture = RouterFixture::new("strict").await;
    let unauthorized = Request::builder()
        .uri("/loxa/v2/events?cursor=bad")
        .body(Body::empty())
        .unwrap();
    let response = fixture.app.clone().oneshot(unauthorized).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let unauthorized_oversized = Request::builder()
        .method(Method::POST)
        .uri("/loxa/v2/models/%20bad/download")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(vec![b'x'; 4097]))
        .unwrap();
    let response = fixture
        .app
        .clone()
        .oneshot(unauthorized_oversized)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let unauthorized_bad_path = Request::builder()
        .method(Method::POST)
        .uri("/loxa/v2/models/%FF/download")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let response = fixture
        .app
        .clone()
        .oneshot(unauthorized_bad_path)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let unauthorized_malformed = Request::builder()
        .method(Method::POST)
        .uri("/loxa/v2/models/gemma-3-4b-it-q4/download")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .unwrap();
    let response = fixture
        .app
        .clone()
        .oneshot(unauthorized_malformed)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let path = format!(
        "/loxa/v2/nodes/{}/slots/{}/unload",
        fixture.node_id, fixture.slot_id
    );
    for body in [b"".as_slice(), br#"{"extra":true}"#, br#"{"x":1,"x":1}"#] {
        let response = fixture
            .app
            .clone()
            .oneshot(fixture.request(Method::POST, &path, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: V2ControlErrorBody =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        assert_eq!(
            error.code,
            loxa_protocol::v2::V2ControlErrorCode::InvalidRequest
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_auth_cors_media_type_paths_and_body_limit_are_closed() {
    let fixture = RouterFixture::new("closed-boundaries").await;
    let authorized_origin = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "tauri://localhost")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", fixture.token.expose_for_authorization()),
        )
        .body(Body::empty())
        .unwrap();
    let response = fixture
        .app
        .clone()
        .oneshot(authorized_origin)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&header::HeaderValue::from_static("tauri://localhost"))
    );

    let preflight = Request::builder()
        .method(Method::OPTIONS)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "tauri://localhost")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
        .body(Body::empty())
        .unwrap();
    let response = fixture.app.clone().oneshot(preflight).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_METHODS),
        Some(&header::HeaderValue::from_static("GET, OPTIONS"))
    );

    let wrong_media = Request::builder()
        .method(Method::POST)
        .uri("/loxa/v2/models/gemma-3-4b-it-q4/download")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", fixture.token.expose_for_authorization()),
        )
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("{}"))
        .unwrap();
    let response = fixture.app.clone().oneshot(wrong_media).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let error: V2ControlErrorBody = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        error.code,
        loxa_protocol::v2::V2ControlErrorCode::UnsupportedMediaType
    );

    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::POST, "/loxa/v2/models/not-a-model/download", "{}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: V2ControlErrorBody = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        error.code,
        loxa_protocol::v2::V2ControlErrorCode::UnknownModel
    );

    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/nodes/not-a-node/slots", ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/loxa/v2/models/gemma-3-4b-it-q4/download")
                .header(header::ORIGIN, "tauri://localhost")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", fixture.token.expose_for_authorization()),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(vec![b' '; 4097]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response.headers().get(header::VARY),
        Some(&header::HeaderValue::from_static("Origin"))
    );
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&header::HeaderValue::from_static("tauri://localhost"))
    );
    let error: V2ControlErrorBody = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        error.code,
        loxa_protocol::v2::V2ControlErrorCode::InvalidRequest
    );

    for invalid_model in ["%20bad", "bad%20", "%01bad", "%FF"] {
        let response = fixture
            .app
            .clone()
            .oneshot(fixture.request(
                Method::POST,
                &format!("/loxa/v2/models/{invalid_model}/download"),
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{invalid_model}"
        );
        let error: V2ControlErrorBody =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        assert_eq!(
            error.code,
            loxa_protocol::v2::V2ControlErrorCode::InvalidRequest
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn closed_publication_gate_preserves_auth_cors_and_preflight_priority() {
    let fixture = RouterFixture::new("closed-publication-auth").await;
    let gate = crate::runtime::PublicationGate::default();
    let app = router(
        V2ControlState::new_for_test(
            fixture.token.clone(),
            fixture.control.handle.clone(),
            fixture.execution.clone(),
        )
        .with_publication_gate_for_test(gate),
    );

    let unauthorized = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "tauri://localhost")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(unauthorized).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&header::HeaderValue::from_static("tauri://localhost"))
    );

    let forbidden = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "https://forbidden.example")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", fixture.token.expose_for_authorization()),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(forbidden).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    let preflight = Request::builder()
        .method(Method::OPTIONS)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "tauri://localhost")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(preflight).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    let authorized = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v2/nodes")
        .header(header::ORIGIN, "tauri://localhost")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", fixture.token.expose_for_authorization()),
        )
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(authorized).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&header::HeaderValue::from_static("tauri://localhost"))
    );

    let (legacy, legacy_worker) = crate::download_control::DownloadControl::spawn(
        fixture.root.join("closed-publication-v1-models"),
    );
    let v1 = crate::control_router::router(
        crate::control_router::ControlState::new(
            fixture.token.clone(),
            fixture.node_id,
            NodeInstanceId::new_v4(),
            legacy,
        )
        .with_publication_gate(crate::runtime::PublicationGate::default()),
    );
    let unauthorized = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v1/models")
        .header(header::ORIGIN, "tauri://localhost")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        v1.clone().oneshot(unauthorized).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let authorized = Request::builder()
        .method(Method::GET)
        .uri("/loxa/v1/models")
        .header(header::ORIGIN, "tauri://localhost")
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", fixture.token.expose_for_authorization()),
        )
        .body(Body::empty())
        .unwrap();
    let response = v1.oneshot(authorized).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&header::HeaderValue::from_static("tauri://localhost"))
    );
    legacy_worker.stop_and_join().unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_auth_cors_preflight_matches_v1_transport_fence() {
    let fixture = RouterFixture::new("transport-parity").await;
    let legacy_root = fixture.root.join("legacy-models");
    let (legacy, legacy_worker) = crate::download_control::DownloadControl::spawn(legacy_root);
    let v1 = crate::control_router::router(crate::control_router::ControlState::new(
        fixture.token.clone(),
        fixture.node_id,
        NodeInstanceId::new_v4(),
        legacy,
    ));

    for authorization in [None, Some("Bearer invalid")] {
        let mut v1_request = Request::builder()
            .method(Method::GET)
            .uri("/loxa/v1/models");
        let mut v2_request = Request::builder().method(Method::GET).uri("/loxa/v2/nodes");
        if let Some(value) = authorization {
            v1_request = v1_request.header(header::AUTHORIZATION, value);
            v2_request = v2_request.header(header::AUTHORIZATION, value);
        }
        let v1_response = v1
            .clone()
            .oneshot(v1_request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let v2_response = fixture
            .app
            .clone()
            .oneshot(v2_request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            sanitized_response(v2_response).await,
            sanitized_response(v1_response).await
        );
    }

    let forbidden = |path: &'static str| {
        Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(header::ORIGIN, "https://forbidden.invalid")
            .body(Body::empty())
            .unwrap()
    };
    let v1_forbidden = v1
        .clone()
        .oneshot(forbidden("/loxa/v1/models"))
        .await
        .unwrap();
    let v2_forbidden = fixture
        .app
        .clone()
        .oneshot(forbidden("/loxa/v2/nodes"))
        .await
        .unwrap();
    assert_eq!(
        sanitized_response(v2_forbidden).await,
        sanitized_response(v1_forbidden).await
    );

    let preflight = |path: &'static str| {
        Request::builder()
            .method(Method::OPTIONS)
            .uri(path)
            .header(header::ORIGIN, "tauri://localhost")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
            .body(Body::empty())
            .unwrap()
    };
    let v1_preflight = v1
        .clone()
        .oneshot(preflight("/loxa/v1/models"))
        .await
        .unwrap();
    let v2_preflight = fixture
        .app
        .clone()
        .oneshot(preflight("/loxa/v2/nodes"))
        .await
        .unwrap();
    assert_eq!(
        sanitized_response(v2_preflight).await,
        sanitized_response(v1_preflight).await
    );

    let allowed = |path: &'static str| {
        Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(header::ORIGIN, "tauri://localhost")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", fixture.token.expose_for_authorization()),
            )
            .body(Body::empty())
            .unwrap()
    };
    let v1_allowed = sanitized_response(
        v1.clone()
            .oneshot(allowed("/loxa/v1/models"))
            .await
            .unwrap(),
    )
    .await;
    let v2_allowed = sanitized_response(
        fixture
            .app
            .clone()
            .oneshot(allowed("/loxa/v2/nodes"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(v2_allowed.0, v1_allowed.0);
    assert_eq!(v2_allowed.1, v1_allowed.1);
    assert!(!v1_allowed.2.is_empty());
    assert!(!v2_allowed.2.is_empty());
    legacy_worker.stop_and_join().unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_download_returns_the_committed_uuid_epoch_and_revision() {
    let fixture = RouterFixture::new("mutation").await;
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            "/loxa/v2/models/gemma-3-4b-it-q4/download",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let accepted: V2OperationAccepted =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    let submitted = fixture
        .submissions
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let snapshot = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(accepted.epoch, fixture.epoch);
    assert_eq!(submitted, accepted.operation_id.to_string());
    assert_eq!(accepted.revision, snapshot.operations[0].created_revision);
    assert_eq!(accepted.operation_id, snapshot.operations[0].operation_id);
    fixture.shutdown().await;
}

async fn wait_for_slot_status(fixture: &RouterFixture, expected: V2SlotStatus) -> V2SlotStatus {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let status = fixture.control.handle.read_snapshot().unwrap().slot.status;
        if status == expected || tokio::time::Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn v2_control_router_every_mutation_uses_and_settles_the_exact_committed_uuid() {
    let fixture = RouterFixture::new("all-mutations").await;
    let model_id = loxa_core::registry::REGISTRY[0].id;
    let load_path = format!(
        "/loxa/v2/nodes/{}/slots/{}/load",
        fixture.node_id, fixture.slot_id
    );
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &load_path,
            format!(r#"{{"model_id":"{model_id}"}}"#),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let load: V2OperationAccepted = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        fixture
            .submissions
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap(),
        load.operation_id.to_string()
    );
    assert_eq!(
        wait_for_slot_status(&fixture, V2SlotStatus::Ready).await,
        V2SlotStatus::Ready
    );

    let unload_path = format!(
        "/loxa/v2/nodes/{}/slots/{}/unload",
        fixture.node_id, fixture.slot_id
    );
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::POST, &unload_path, b"{}".as_slice()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let unload: V2OperationAccepted = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        fixture
            .submissions
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap(),
        unload.operation_id.to_string()
    );
    assert_eq!(
        wait_for_slot_status(&fixture, V2SlotStatus::Unloaded).await,
        V2SlotStatus::Unloaded
    );

    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/models/{model_id}/download"),
            b"{}".as_slice(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let download: V2OperationAccepted =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        fixture
            .submissions
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap(),
        download.operation_id.to_string()
    );
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/operations/{}/cancel", download.operation_id),
            b"{}".as_slice(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let cancel: V2OperationAccepted = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(cancel.operation_id, download.operation_id);
    let snapshot = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(
        snapshot
            .operations
            .iter()
            .find(|operation| operation.operation_id == download.operation_id)
            .unwrap()
            .status,
        V2OperationStatus::Cancelled
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_operation_collection_and_item_are_correlated() {
    let fixture = RouterFixture::new("operation-reads").await;
    let model_id = loxa_core::registry::REGISTRY[0].id;
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            &format!("/loxa/v2/models/{model_id}/download"),
            b"{}".as_slice(),
        ))
        .await
        .unwrap();
    let accepted: V2OperationAccepted =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/operations", b"".as_slice()))
        .await
        .unwrap();
    let collection: V2OperationCollection =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(collection.operations.len(), 1);
    assert_eq!(collection.operations[0].operation_id, accepted.operation_id);
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::GET,
            &format!("/loxa/v2/operations/{}", accepted.operation_id),
            b"".as_slice(),
        ))
        .await
        .unwrap();
    let envelope: V2OperationEnvelope =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(envelope.operation.operation_id, accepted.operation_id);
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_submission_failure_fails_the_only_admitted_uuid() {
    let fixture = RouterFixture::new("submission-failure").await;
    fixture.actor.stop();
    fixture.actor_worker.thread().unpark();
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            "/loxa/v2/models/gemma-3-4b-it-q4/download",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: V2ControlErrorBody = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        error.code,
        loxa_protocol::v2::V2ControlErrorCode::NodeStopping
    );
    let snapshot = fixture.control.handle.read_snapshot().unwrap();
    assert_eq!(snapshot.operations.len(), 1);
    assert_eq!(snapshot.operations[0].status, V2OperationStatus::Failed);
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_production_inventory_rejects_unavailable_load_before_admission() {
    let fixture = RouterFixture::new("inventory-rejection").await;
    let (inventory, inventory_worker) =
        crate::download_control::DownloadControl::spawn(fixture.root.join("unavailable-inventory"));
    let app = router(V2ControlState::new_with_inventory_for_test(
        fixture.token.clone(),
        fixture.control.handle.clone(),
        fixture.execution.clone(),
        inventory,
    ));
    let response = app
        .oneshot(fixture.request(
            Method::POST,
            &format!(
                "/loxa/v2/nodes/{}/slots/{}/load",
                fixture.node_id, fixture.slot_id
            ),
            format!(
                r#"{{"model_id":"{}"}}"#,
                loxa_core::registry::REGISTRY[0].id
            ),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: V2ControlErrorBody = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        error.code,
        loxa_protocol::v2::V2ControlErrorCode::ModelUnavailable
    );
    assert!(fixture
        .control
        .handle
        .read_snapshot()
        .unwrap()
        .operations
        .is_empty());
    inventory_worker.stop_and_join().unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_sse_is_snapshot_first_atomic_and_query_is_closed() {
    let fixture = RouterFixture::new("sse").await;
    for query in [
        "?epoch=00000000-0000-4000-8000-000000000001",
        "?cursor=0",
        "?epoch=bad&cursor=0",
        "?epoch=00000000-0000-4000-8000-000000000001&cursor=0&cursor=1",
        "?epoch=00000000-0000-4000-8000-000000000001&cursor=01",
        "?epoch=00000000-0000-4000-8000-000000000001&cursor=18446744073709551616",
        "?epoch=00000000-0000-4000-8000-000000000001&cursor=0&extra=1",
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(fixture.request(Method::GET, &format!("/loxa/v2/events{query}"), ""))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
    }
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/events", ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.starts_with("event: snapshot\ndata: "), "{text:?}");
    let json = text.strip_prefix("event: snapshot\ndata: ").unwrap().trim();
    let snapshot: V2ReconnectSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(snapshot.epoch, fixture.epoch);

    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::POST,
            "/loxa/v2/models/gemma-3-4b-it-q4/download",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let live_bytes = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let live_text = std::str::from_utf8(&live_bytes).unwrap();
    assert!(live_text.contains("event: state\n"), "{live_text:?}");
    let live_json = live_text
        .split("data: ")
        .nth(1)
        .expect("live state data")
        .trim();
    let live: V2ControlEvent = serde_json::from_str(live_json).unwrap();
    assert_eq!(
        live.sequence.get(),
        snapshot.stream.cursor.get().checked_add(1).unwrap()
    );

    let gap_response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::GET,
            "/loxa/v2/events?epoch=00000000-0000-4000-8000-000000000001&cursor=0",
            "",
        ))
        .await
        .unwrap();
    let mut gap_body = gap_response.into_body().into_data_stream();
    let gap_bytes = tokio::time::timeout(std::time::Duration::from_secs(2), gap_body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let gap_text = std::str::from_utf8(&gap_bytes).unwrap();
    let gap_json = gap_text
        .strip_prefix("event: snapshot\ndata: ")
        .unwrap()
        .trim();
    let gap: V2ReconnectSnapshot = serde_json::from_str(gap_json).unwrap();
    assert!(gap.stream.cursor_gap);
    assert!(gap.events.is_empty());
    for (query, expected_gap) in [
        (format!("?cursor=0&epoch={}", fixture.epoch), false),
        (
            format!("?epoch={}&cursor={}", fixture.epoch, u64::MAX),
            true,
        ),
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(fixture.request(Method::GET, &format!("/loxa/v2/events{query}"), ""))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut resume_body = response.into_body().into_data_stream();
        let resume_bytes = resume_body.next().await.unwrap().unwrap();
        let resume_text = std::str::from_utf8(&resume_bytes).unwrap();
        let resume_json = resume_text
            .strip_prefix("event: snapshot\ndata: ")
            .unwrap()
            .trim();
        let resume: V2ReconnectSnapshot = serde_json::from_str(resume_json).unwrap();
        assert_eq!(resume.stream.cursor_gap, expected_gap);
        if expected_gap {
            assert!(resume.events.is_empty());
        }
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_evicted_cursor_is_an_actual_gap_snapshot_over_http() {
    let fixture = RouterFixture::new("evicted-http-gap").await;
    for index in 0..512_u64 {
        let admission = fixture
            .control
            .handle
            .admit(AdmissionRequest::Download {
                model_id: format!("evicted-{index}"),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            })
            .await
            .unwrap();
        fixture
            .control
            .handle
            .observe_required_async(Transition::Cancelled {
                operation_id: admission.operation_id,
            })
            .await
            .unwrap();
    }
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(
            Method::GET,
            &format!("/loxa/v2/events?epoch={}&cursor=0", fixture.epoch),
            "",
        ))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let bytes = body.next().await.unwrap().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    let json = text.strip_prefix("event: snapshot\ndata: ").unwrap().trim();
    let snapshot: V2ReconnectSnapshot = serde_json::from_str(json).unwrap();
    assert!(snapshot.stream.cursor_gap);
    assert!(snapshot.events.is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_byte_budget_is_an_actual_gap_snapshot_over_http() {
    let fixture = RouterFixture::new("byte-budget-http-gap").await;
    let admission = fixture
        .control
        .handle
        .admit(AdmissionRequest::Download {
            model_id: "byte-budget".into(),
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(0),
                total_bytes: None,
            },
        })
        .await
        .unwrap();
    fixture
        .control
        .handle
        .observe_required_async(Transition::Cancelled {
            operation_id: admission.operation_id,
        })
        .await
        .unwrap();
    let committed = fixture.control.handle.read_snapshot().unwrap();
    let cursor = committed.cursor.get().checked_sub(1).unwrap();
    let base = fixture
        .control
        .handle
        .reconnect(None, DecimalU64::new(u64::MAX))
        .unwrap();
    let max_snapshot_bytes = serde_json::to_vec(&base).unwrap().len();
    let state = V2ControlState::new_for_test(
        fixture.token.clone(),
        fixture.control.handle.clone(),
        fixture.execution.clone(),
    )
    .with_subscription_max_snapshot_bytes_for_test(max_snapshot_bytes);
    let response = router(state)
        .oneshot(fixture.request(
            Method::GET,
            &format!("/loxa/v2/events?epoch={}&cursor={cursor}", fixture.epoch),
            "",
        ))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let bytes = body.next().await.unwrap().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    let json = text.strip_prefix("event: snapshot\ndata: ").unwrap().trim();
    let snapshot: V2ReconnectSnapshot = serde_json::from_str(json).unwrap();
    assert!(snapshot.stream.cursor_gap);
    assert!(snapshot.events.is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn v2_control_router_http_slow_subscriber_disconnects_without_blocking_writer() {
    let fixture = RouterFixture::new("slow-http-subscriber").await;
    let response = fixture
        .app
        .clone()
        .oneshot(fixture.request(Method::GET, "/loxa/v2/events", ""))
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    for index in 0..65_u64 {
        let admission = fixture
            .control
            .handle
            .admit(AdmissionRequest::Download {
                model_id: format!("slow-{index}"),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            })
            .await
            .unwrap();
        fixture
            .control
            .handle
            .observe_required_async(Transition::Cancelled {
                operation_id: admission.operation_id,
            })
            .await
            .unwrap();
    }
    let admission = fixture
        .control
        .handle
        .admit(AdmissionRequest::Download {
            model_id: "after-disconnect".into(),
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(0),
                total_bytes: None,
            },
        })
        .await
        .unwrap();
    fixture
        .control
        .handle
        .observe_required_async(Transition::Cancelled {
            operation_id: admission.operation_id,
        })
        .await
        .unwrap();
    for _ in 0..=130 {
        let next = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
            .await
            .unwrap();
        if next.is_none() {
            fixture.shutdown().await;
            return;
        }
    }
    panic!("slow HTTP subscriber did not close after its bounded queue filled");
}
