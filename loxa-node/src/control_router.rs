use crate::download_control::{DownloadControl, DownloadControlError};
use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, options, post};
use axum::{Json, Router};
use loxa_core::control::auth::{desktop_origins, is_desktop_origin, AuthPolicy, ControlToken};
use loxa_core::control::contracts::{
    CapabilitiesSnapshot, ControlErrorBody, ModelRequest, NodeIdentityChallenge,
    NodeIdentityProofResponse, NodeSnapshot, NodeStatus, OperationAccepted,
    CONTROL_PROTOCOL_VERSION,
};
use loxa_core::model_inventory::current_available_memory_bytes;
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ControlState {
    token: ControlToken,
    policy: Arc<AuthPolicy>,
    node_id: Arc<str>,
    runtime_identity: Arc<str>,
    downloads: DownloadControl,
}

impl ControlState {
    pub fn new(
        token: ControlToken,
        node_id: String,
        runtime_identity: String,
        downloads: DownloadControl,
    ) -> Self {
        let policy = AuthPolicy::new(token.clone(), desktop_origins());
        Self {
            token,
            policy: Arc::new(policy),
            node_id: node_id.into(),
            runtime_identity: runtime_identity.into(),
            downloads,
        }
    }
}

fn authorize(state: &ControlState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let origin = request_origin(headers)?;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state
        .policy
        .authorize(origin.as_deref(), bearer)
        .map_err(|_| StatusCode::UNAUTHORIZED)
}

fn request_origin(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(header::ORIGIN) else {
        return Ok(None);
    };
    let origin = value.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
    if is_desktop_origin(origin) {
        Ok(Some(origin.to_owned()))
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

fn cors(mut response: Response, origin: Option<&str>) -> Response {
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Origin"));
    if let Some(origin) = origin.and_then(|value| HeaderValue::from_str(value).ok()) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    response
}

async fn preflight(headers: HeaderMap, methods: &'static str) -> Response {
    let origin = match request_origin(&headers) {
        Ok(Some(origin)) => origin,
        _ => return StatusCode::FORBIDDEN.into_response(),
    };
    let mut response = StatusCode::NO_CONTENT.into_response();
    let requested_method = headers
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok());
    let expected_method = methods.split(',').next().expect("method exists");
    if requested_method != Some(expected_method) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Some(requested_headers) = headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
    {
        let allowed = requested_headers.split(',').all(|name| {
            matches!(
                name.trim().to_ascii_lowercase().as_str(),
                "authorization" | "content-type" | "x-loxa-challenge"
            )
        });
        if !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, x-loxa-challenge"),
    );
    cors(response, Some(&origin))
}

async fn get_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "GET, OPTIONS").await
}

async fn post_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "POST, OPTIONS").await
}

fn control_error(status: StatusCode, code: &str, message: &str, origin: Option<&str>) -> Response {
    cors(
        (
            status,
            Json(ControlErrorBody {
                code: code.into(),
                message: message.into(),
            }),
        )
            .into_response(),
        origin,
    )
}

fn map_download_error(error: DownloadControlError, origin: Option<&str>) -> Response {
    match error {
        DownloadControlError::Conflict => control_error(
            StatusCode::CONFLICT,
            "operation_conflict",
            "a conflicting model operation is already active",
            origin,
        ),
        DownloadControlError::Missing => control_error(
            StatusCode::NOT_FOUND,
            "operation_not_found",
            "operation or model was not found",
            origin,
        ),
        DownloadControlError::Terminal => control_error(
            StatusCode::CONFLICT,
            "operation_terminal",
            "operation is already terminal",
            origin,
        ),
        DownloadControlError::Stopping => control_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node_stopping",
            "node is stopping",
            origin,
        ),
        DownloadControlError::CancellationNotSafe => control_error(
            StatusCode::CONFLICT,
            "cancellation_not_safe",
            "the model operation passed its safe cancellation point",
            origin,
        ),
        DownloadControlError::ModelUnavailable => control_error(
            StatusCode::CONFLICT,
            "model_unavailable",
            "the model must be downloaded, verified, compatible, and engine eligible",
            origin,
        ),
    }
}

async fn start_download(
    State(state): State<ControlState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.split(';').next() != Some("application/json"))
    {
        return control_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "content type must be application/json",
            origin.as_deref(),
        );
    }
    let request = match serde_json::from_slice::<ModelRequest>(&body) {
        Ok(request) => request,
        Err(_) => {
            return control_error(
                StatusCode::BAD_REQUEST,
                "unknown_model",
                "request must name a known registry model",
                origin.as_deref(),
            )
        }
    };
    match state.downloads.start(&request.model_id) {
        Ok(operation_id) => cors(
            (
                StatusCode::ACCEPTED,
                Json(OperationAccepted { operation_id }),
            )
                .into_response(),
            origin.as_deref(),
        ),
        Err(error) => map_download_error(error, origin.as_deref()),
    }
}

async fn start_load(
    State(state): State<ControlState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.split(';').next() != Some("application/json"))
    {
        return control_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "content type must be application/json",
            origin.as_deref(),
        );
    }
    let request = match serde_json::from_slice::<ModelRequest>(&body) {
        Ok(request) => request,
        Err(_) => {
            return control_error(
                StatusCode::BAD_REQUEST,
                "unknown_model",
                "request must name a known registry model",
                origin.as_deref(),
            );
        }
    };
    match state.downloads.start_load(&request.model_id) {
        Ok(operation_id) => cors(
            (
                StatusCode::ACCEPTED,
                Json(OperationAccepted { operation_id }),
            )
                .into_response(),
            origin.as_deref(),
        ),
        Err(error) => map_download_error(error, origin.as_deref()),
    }
}

async fn start_unload(State(state): State<ControlState>, headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    match state.downloads.start_unload() {
        Ok(operation_id) => cors(
            (
                StatusCode::ACCEPTED,
                Json(OperationAccepted { operation_id }),
            )
                .into_response(),
            origin.as_deref(),
        ),
        Err(error) => map_download_error(error, origin.as_deref()),
    }
}

async fn operation(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    match state.downloads.operation(&id) {
        Some(operation) => cors(Json(operation).into_response(), origin.as_deref()),
        None => map_download_error(DownloadControlError::Missing, origin.as_deref()),
    }
}

async fn cancel_operation(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    match state.downloads.cancel(&id) {
        Ok(_) => cors(
            Json(
                state
                    .downloads
                    .operation(&id)
                    .expect("cancelled operation exists"),
            )
            .into_response(),
            origin.as_deref(),
        ),
        Err(error) => map_download_error(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    #[serde(default)]
    cursor: u64,
}

async fn events(
    State(state): State<ControlState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    let (snapshot, subscription) = state.downloads.subscribe_with_snapshot(query.cursor);
    let initial = serde_json::to_string(&snapshot).expect("snapshot serializes");
    let (sender, receiver) = tokio::sync::mpsc::channel(128);
    std::thread::spawn(move || loop {
        match subscription
            .receiver
            .recv_timeout(Duration::from_millis(250))
        {
            Ok(event) => {
                if sender.blocking_send(event).is_err() {
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) if sender.is_closed() => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    });
    let stream = futures_util::stream::unfold(
        (Some(initial), receiver),
        |(initial, mut receiver)| async move {
            if let Some(initial) = initial {
                return Some((
                    Ok::<_, Infallible>(Event::default().event("snapshot").data(initial)),
                    (None, receiver),
                ));
            }
            match receiver.recv().await {
                Some(control_event) => Some((
                    Ok(Event::default()
                        .event("operation")
                        .id(control_event.sequence.to_string())
                        .data(
                            serde_json::to_string(&control_event)
                                .expect("control event serializes"),
                        )),
                    (None, receiver),
                )),
                None => None,
            }
        },
    );
    cors(
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response(),
        origin.as_deref(),
    )
}

async fn node_proof(
    State(state): State<ControlState>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if headers.contains_key(header::AUTHORIZATION) {
        return cors(
            node_snapshot(State(state), headers).await,
            origin.as_deref(),
        );
    }
    if query.is_some() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(nonce) = headers
        .get("x-loxa-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let challenge = match NodeIdentityChallenge::new(nonce) {
        Ok(challenge) => challenge,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let status = node_status(&state);
    let proof = match state.token.node_identity_proof(
        &challenge.nonce,
        &state.node_id,
        &state.runtime_identity,
        status,
    ) {
        Ok(proof) => proof,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    cors(
        Json(
            NodeIdentityProofResponse::new(
                CONTROL_PROTOCOL_VERSION,
                state.node_id.to_string(),
                state.runtime_identity.to_string(),
                status,
                proof,
            )
            .expect("validated node identity"),
        )
        .into_response(),
        origin.as_deref(),
    )
}

async fn capabilities(State(state): State<ControlState>, headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    cors(
        Json(CapabilitiesSnapshot {
            document_input: false,
            document_input_reason: "Document input is not supported by this model and backend."
                .into(),
            text_chat: true,
        })
        .into_response(),
        origin.as_deref(),
    )
}

async fn models(State(state): State<ControlState>, headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    cors(
        Json(state.downloads.inventory(current_available_memory_bytes())).into_response(),
        origin.as_deref(),
    )
}

async fn node_snapshot(State(state): State<ControlState>, headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    if let Err(status) = authorize(&state, &headers) {
        return cors(status.into_response(), origin.as_deref());
    }
    let lifecycle = state.downloads.lifecycle_snapshot();
    let status = lifecycle_status(lifecycle.as_ref());
    cors(
        Json(NodeSnapshot {
            status,
            active_model_id: lifecycle
                .as_ref()
                .and_then(|snapshot| snapshot.active_model_id.clone()),
            operation_id: lifecycle
                .as_ref()
                .and_then(|snapshot| snapshot.operation_id.clone()),
            error: lifecycle.and_then(|snapshot| snapshot.error),
        })
        .into_response(),
        origin.as_deref(),
    )
}

fn node_status(state: &ControlState) -> NodeStatus {
    let lifecycle = state.downloads.lifecycle_snapshot();
    lifecycle_status(lifecycle.as_ref())
}

fn lifecycle_status(snapshot: Option<&crate::model_lifecycle::LifecycleSnapshot>) -> NodeStatus {
    use crate::model_lifecycle::NodeLifecycleStatus;
    match snapshot.map(|snapshot| &snapshot.status) {
        None | Some(NodeLifecycleStatus::Unloaded) => NodeStatus::Unloaded,
        Some(NodeLifecycleStatus::Loading) => NodeStatus::Loading,
        Some(NodeLifecycleStatus::Ready) => NodeStatus::Ready,
        Some(NodeLifecycleStatus::Unloading) => NodeStatus::Unloading,
        Some(NodeLifecycleStatus::RecoveryRequired) => NodeStatus::RecoveryRequired,
    }
}

pub fn router(state: ControlState) -> Router {
    Router::new()
        .route("/loxa/v1/node", get(node_proof))
        .route("/loxa/v1/capabilities", get(capabilities))
        .route("/loxa/v1/models", get(models))
        .route(
            "/loxa/v1/models/download",
            post(start_download).options(post_preflight),
        )
        .route(
            "/loxa/v1/models/load",
            post(start_load).options(post_preflight),
        )
        .route(
            "/loxa/v1/models/unload",
            post(start_unload).options(post_preflight),
        )
        .route(
            "/loxa/v1/operations/{id}",
            get(operation).options(get_preflight),
        )
        .route(
            "/loxa/v1/operations/{id}/cancel",
            post(cancel_operation).options(post_preflight),
        )
        .route("/loxa/v1/events", get(events).options(get_preflight))
        .route("/loxa/v1/node", options(get_preflight))
        .route("/loxa/v1/capabilities", options(get_preflight))
        .route("/loxa/v1/models", options(get_preflight))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::control::contracts::OperationStatus;
    use loxa_core::model_inventory::VerificationCache;
    use loxa_core::registry::ModelEntry;
    use std::path::PathBuf;

    struct RouterDriver;
    impl crate::model_lifecycle::EngineLifecycleDriver for RouterDriver {
        type Session = ();
        fn start(
            &mut self,
            _: &crate::model_lifecycle::StableNodeOwner,
            _: &crate::model_lifecycle::LaunchPlan,
            _: u64,
        ) -> Result<
            crate::model_lifecycle::StartedSession<()>,
            crate::model_lifecycle::LifecycleError,
        > {
            panic!("unload does not spawn")
        }
        fn wait_ready(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<()>,
            _: crate::model_lifecycle::LifecycleSignals<'_>,
        ) -> Result<(), crate::model_lifecycle::LifecycleError> {
            panic!("unload does not wait")
        }
        fn stop_exact(
            &mut self,
            _: crate::model_lifecycle::StartedSession<()>,
        ) -> Result<(), crate::model_lifecycle::LifecycleError> {
            Ok(())
        }
    }
    struct RouterGateway;
    impl crate::model_lifecycle::GatewayPublisher for RouterGateway {
        fn withdraw(&mut self) {}
        fn publish(
            &mut self,
            _: &crate::model_lifecycle::LaunchPlan,
            _: &crate::model_lifecycle::SessionCorrelation,
        ) {
        }
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "loxa-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn malformed_present_origin_is_rejected_not_treated_as_native() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(request_origin(&headers), Err(StatusCode::FORBIDDEN));
        assert_eq!(request_origin(&HeaderMap::new()), Ok(None));
    }

    #[tokio::test]
    async fn successful_download_publishes_shared_evidence_to_authorized_models_route() {
        let temp = TestDir::new("download-models-e2e");
        let models_dir = temp.0.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let recipes: &'static [ModelEntry] = Box::leak(
            vec![ModelEntry {
                id: "fixture",
                repo: "owner/repo",
                revision: "main",
                filename: "fixture.gguf",
                sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
                size_bytes: 4,
                license: "apache-2.0",
                params: "tiny",
                quant: "Q4",
                min_free_mem_gb: 0.1,
            }]
            .into_boxed_slice(),
        );
        let cache = Arc::new(VerificationCache::default());

        let (downloads, worker) = DownloadControl::spawn_fixture_for_test(
            models_dir,
            Arc::clone(&cache),
            recipes,
            b"good",
        );
        let operation_id = downloads.start("fixture").unwrap();
        let operation = wait_for_terminal_operation(&downloads, &operation_id);
        assert_eq!(operation.status, OperationStatus::Succeeded);
        let token = ControlToken::load_or_create(&temp.0.join("control.token")).unwrap();
        let state = ControlState::new(
            token.clone(),
            "node-id".into(),
            "runtime-id".into(),
            downloads,
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("tauri://localhost"),
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_for_authorization())).unwrap(),
        );

        let response = models(State(state), headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let inventory: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(inventory[0]["id"], "fixture");
        assert_eq!(inventory[0]["artifact"], "downloaded");

        worker.stop_and_join().unwrap();
    }

    #[tokio::test]
    async fn authenticated_unload_route_returns_operation_and_rejects_missing_bearer() {
        let temp = TestDir::new("unload-route");
        let lifecycle = crate::model_lifecycle::ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            RouterDriver,
            RouterGateway,
        );
        let (downloads, worker) =
            DownloadControl::spawn_with_lifecycle(temp.0.join("models"), lifecycle);
        let token = ControlToken::load_or_create(&temp.0.join("control.token")).unwrap();
        let state = ControlState::new(
            token.clone(),
            "node".into(),
            "runtime".into(),
            downloads.clone(),
        );
        let mut unauthorized = HeaderMap::new();
        unauthorized.insert(
            header::ORIGIN,
            HeaderValue::from_static("tauri://localhost"),
        );
        assert_eq!(
            start_unload(State(state.clone()), unauthorized)
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );

        let mut authorized = HeaderMap::new();
        authorized.insert(
            header::ORIGIN,
            HeaderValue::from_static("tauri://localhost"),
        );
        authorized.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_for_authorization())).unwrap(),
        );
        let response = start_unload(State(state), authorized).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let accepted: OperationAccepted = serde_json::from_slice(&body).unwrap();
        assert!(accepted.operation_id.starts_with("op-"));
        worker.stop_and_join().unwrap();
    }

    fn wait_for_terminal_operation(
        downloads: &DownloadControl,
        operation_id: &str,
    ) -> loxa_core::control::contracts::OperationView {
        for _ in 0..1_000 {
            let operation = downloads.operation(operation_id).unwrap();
            if matches!(
                operation.status,
                OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Cancelled
            ) {
                return operation;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("fixture download did not reach a terminal state");
    }
}
