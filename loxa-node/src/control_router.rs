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
    CapabilitiesSnapshot, ControlErrorBody, ControlErrorCode, ModelRequest, NodeIdentityChallenge,
    NodeIdentityProofResponse, NodeStatus, OperationAccepted, CONTROL_PROTOCOL_VERSION,
};
use loxa_core::model_inventory::current_available_memory_bytes;
use loxa_protocol::{NodeId, NodeInstanceId};
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ControlState {
    token: ControlToken,
    policy: Arc<AuthPolicy>,
    node_id: NodeId,
    node_instance_id: NodeInstanceId,
    downloads: DownloadControl,
    publication_gate: Option<crate::runtime::PublicationGate>,
}

impl ControlState {
    pub fn new(
        token: ControlToken,
        node_id: NodeId,
        node_instance_id: NodeInstanceId,
        downloads: DownloadControl,
    ) -> Self {
        let policy = AuthPolicy::new(token.clone(), desktop_origins());
        Self {
            token,
            policy: Arc::new(policy),
            node_id,
            node_instance_id,
            downloads,
            publication_gate: None,
        }
    }

    pub(crate) fn with_publication_gate(mut self, gate: crate::runtime::PublicationGate) -> Self {
        self.publication_gate = Some(gate);
        self
    }
}

fn publication_closed(state: &ControlState, origin: Option<&str>) -> Option<Response> {
    state
        .publication_gate
        .as_ref()
        .is_some_and(|gate| !gate.is_open())
        .then(|| {
            control_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ControlErrorCode::NodeStopping,
                "node is stopping",
                origin,
            )
        })
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

pub(crate) fn request_origin(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
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

pub(crate) fn cors(mut response: Response, origin: Option<&str>) -> Response {
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

pub(crate) async fn get_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "GET, OPTIONS").await
}

pub(crate) async fn post_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "POST, OPTIONS").await
}

fn control_error(
    status: StatusCode,
    code: ControlErrorCode,
    message: &str,
    origin: Option<&str>,
) -> Response {
    cors(
        (
            status,
            Json(ControlErrorBody {
                code,
                message: message.into(),
            }),
        )
            .into_response(),
        origin,
    )
}

fn map_download_error(error: DownloadControlError, origin: Option<&str>) -> Response {
    match error {
        DownloadControlError::Conflict | DownloadControlError::WriterOverloaded => control_error(
            StatusCode::CONFLICT,
            ControlErrorCode::OperationConflict,
            "a conflicting model operation is already active",
            origin,
        ),
        DownloadControlError::Missing => control_error(
            StatusCode::NOT_FOUND,
            ControlErrorCode::OperationNotFound,
            "operation or model was not found",
            origin,
        ),
        DownloadControlError::Terminal => control_error(
            StatusCode::CONFLICT,
            ControlErrorCode::OperationTerminal,
            "operation is already terminal",
            origin,
        ),
        DownloadControlError::Stopping => control_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ControlErrorCode::NodeStopping,
            "node is stopping",
            origin,
        ),
        DownloadControlError::CancellationNotSafe => control_error(
            StatusCode::CONFLICT,
            ControlErrorCode::CancellationNotSafe,
            "the model operation passed its safe cancellation point",
            origin,
        ),
        DownloadControlError::ModelUnavailable => control_error(
            StatusCode::CONFLICT,
            ControlErrorCode::ModelUnavailable,
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.split(';').next() != Some("application/json"))
    {
        return control_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ControlErrorCode::UnsupportedMediaType,
            "content type must be application/json",
            origin.as_deref(),
        );
    }
    let request = match serde_json::from_slice::<ModelRequest>(&body) {
        Ok(request) => request,
        Err(_) => {
            return control_error(
                StatusCode::BAD_REQUEST,
                ControlErrorCode::UnknownModel,
                "request must name a known registry model",
                origin.as_deref(),
            )
        }
    };
    match state
        .downloads
        .start_download_async(&request.model_id)
        .await
    {
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.split(';').next() != Some("application/json"))
    {
        return control_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ControlErrorCode::UnsupportedMediaType,
            "content type must be application/json",
            origin.as_deref(),
        );
    }
    let request = match serde_json::from_slice::<ModelRequest>(&body) {
        Ok(request) => request,
        Err(_) => {
            return control_error(
                StatusCode::BAD_REQUEST,
                ControlErrorCode::UnknownModel,
                "request must name a known registry model",
                origin.as_deref(),
            );
        }
    };
    match state.downloads.start_load_async(&request.model_id).await {
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    match state.downloads.start_unload_async().await {
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    match state.downloads.operation_checked(&id) {
        Ok(Some(operation)) => cors(Json(operation).into_response(), origin.as_deref()),
        Ok(None) => map_download_error(DownloadControlError::Missing, origin.as_deref()),
        Err(error) => map_download_error(error, origin.as_deref()),
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    match state.downloads.cancel_async(&id).await {
        Ok(_) => cors(
            Json(match state.downloads.operation_checked(&id) {
                Ok(Some(operation)) => operation,
                _ => return map_download_error(DownloadControlError::Stopping, origin.as_deref()),
            })
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    let (snapshot, subscription) = match state
        .downloads
        .subscribe_v1_with_snapshot(query.cursor)
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => return map_download_error(error, origin.as_deref()),
    };
    let initial = serde_json::to_string(&snapshot).expect("snapshot serializes");
    let stream = futures_util::stream::unfold(
        (Some(initial), subscription),
        |(initial, mut subscription)| async move {
            if let Some(initial) = initial {
                return Some((
                    Ok::<_, Infallible>(Event::default().event("snapshot").data(initial)),
                    (None, subscription),
                ));
            }
            match subscription.recv().await {
                Some(control_event) => Some((
                    Ok(Event::default()
                        .event("operation")
                        .id(control_event.sequence.to_string())
                        .data(
                            serde_json::to_string(&control_event)
                                .expect("control event serializes"),
                        )),
                    (None, subscription),
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
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
    let status = match node_status(&state) {
        Ok(status) => status,
        Err(error) => return map_download_error(error, origin.as_deref()),
    };
    let node_id = state.node_id.to_string();
    let node_instance_id = state.node_instance_id.to_string();
    let proof =
        match state
            .token
            .node_identity_proof(&challenge.nonce, &node_id, &node_instance_id, status)
        {
            Ok(proof) => proof,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
    cors(
        Json(
            NodeIdentityProofResponse::new(
                CONTROL_PROTOCOL_VERSION,
                node_id,
                node_instance_id,
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
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
    if let Some(response) = publication_closed(&state, origin.as_deref()) {
        return response;
    }
    match state.downloads.node_snapshot_checked() {
        Ok(snapshot) => cors(Json(snapshot).into_response(), origin.as_deref()),
        Err(error) => map_download_error(error, origin.as_deref()),
    }
}

fn node_status(state: &ControlState) -> Result<NodeStatus, DownloadControlError> {
    state
        .downloads
        .node_snapshot_checked()
        .map(|snapshot| snapshot.status)
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

pub(crate) fn router_with_optional_v2(
    state: ControlState,
    control: Option<crate::control_state::ControlStateHandle>,
) -> Result<Router, &'static str> {
    let Some(control) = control else {
        return Ok(router(state));
    };
    let v2_state = crate::v2_control_router::V2ControlState::new(
        state.token.clone(),
        control,
        state.downloads.clone(),
        state.publication_gate.clone(),
    )?;
    Ok(router(state).merge(crate::v2_control_router::router(v2_state)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::control::contracts::OperationStatus;
    use loxa_core::model_inventory::VerificationCache;
    use loxa_core::registry::ModelEntry;
    use std::path::PathBuf;
    use std::str::FromStr;

    struct RouterDriver;
    impl crate::model_lifecycle::EngineLifecycleDriver for RouterDriver {
        type Session = ();
        fn start(
            &mut self,
            _: &crate::model_lifecycle::StableNodeOwner,
            _: &crate::model_lifecycle::LaunchPlan,
            _: u64,
            _: &mut crate::model_lifecycle::CandidateSlot<()>,
        ) -> Result<(), crate::model_lifecycle::LifecycleError> {
            panic!("unload does not spawn")
        }
        fn wait_ready(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<()>,
            _: crate::model_lifecycle::LifecycleSignals<'_>,
        ) -> Result<(), crate::model_lifecycle::LifecycleError> {
            panic!("unload does not wait")
        }
        fn stop_exact<'a>(
            &mut self,
            _: &'a mut crate::model_lifecycle::StartedSession<()>,
        ) -> Result<(), crate::model_lifecycle::ExactStopFailure<'a, ()>> {
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
    async fn typed_identity_projects_the_exact_v1_proof_fixture() {
        let temp = TestDir::new("typed-v1-proof");
        let token = ControlToken::load_or_create(&temp.0.join("control.token")).unwrap();
        let node_id = NodeId::from_str("123e4567-e89b-42d3-a456-426614174000").unwrap();
        let instance_id = NodeInstanceId::from_str("123e4567-e89b-42d3-b456-426614174001").unwrap();
        let (downloads, worker) = DownloadControl::spawn(temp.0.join("models"));
        let state = ControlState::new(token.clone(), node_id, instance_id, downloads);
        let nonce = "01".repeat(32);
        let mut headers = HeaderMap::new();
        headers.insert("x-loxa-challenge", HeaderValue::from_str(&nonce).unwrap());

        let response = node_proof(State(state), headers, RawQuery(None)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let expected_proof = token
            .node_identity_proof(
                &nonce,
                &node_id.to_string(),
                &instance_id.to_string(),
                NodeStatus::Unloaded,
            )
            .unwrap();
        assert_eq!(
            body.as_ref(),
            format!(
                "{{\"protocol_version\":1,\"node_id\":\"{node_id}\",\"runtime_identity\":\"{instance_id}\",\"status\":\"unloaded\",\"challenge_proof\":\"{expected_proof}\"}}"
            )
            .as_bytes()
        );
        worker.stop_and_join().unwrap();
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
            NodeId::new_v4(),
            NodeInstanceId::new_v4(),
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
            NodeId::new_v4(),
            NodeInstanceId::new_v4(),
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

    #[tokio::test]
    async fn authenticated_load_route_admits_full_restart_artifact_but_rejects_partial_state() {
        let temp = TestDir::new("restart-load-route");
        let models_dir = temp.0.join("models");
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
            DownloadControl::spawn_with_lifecycle(models_dir.clone(), lifecycle);
        let recipe = loxa_core::registry::REGISTRY
            .iter()
            .min_by_key(|entry| entry.size_bytes)
            .unwrap();
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::File::create(models_dir.join(recipe.filename))
            .unwrap()
            .set_len(recipe.size_bytes)
            .unwrap();
        let token = ControlToken::load_or_create(&temp.0.join("control.token")).unwrap();
        let state = ControlState::new(
            token.clone(),
            NodeId::new_v4(),
            NodeInstanceId::new_v4(),
            downloads,
        );
        let mut authorized = HeaderMap::new();
        authorized.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        authorized.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token.expose_for_authorization())).unwrap(),
        );

        let accepted = start_load(
            State(state.clone()),
            authorized.clone(),
            Bytes::from(format!(r#"{{"model_id":"{}"}}"#, recipe.id)),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);

        worker.stop_and_join().unwrap();

        let partial_temp = TestDir::new("partial-load-route");
        let partial_models = partial_temp.0.join("models");
        let partial_lifecycle = crate::model_lifecycle::ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "partial-owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            RouterDriver,
            RouterGateway,
        );
        let (partial_downloads, partial_worker) =
            DownloadControl::spawn_with_lifecycle(partial_models.clone(), partial_lifecycle);
        std::fs::create_dir_all(&partial_models).unwrap();
        std::fs::write(
            partial_models.join(format!("{}.part", recipe.filename)),
            b"x",
        )
        .unwrap();
        let partial_token =
            ControlToken::load_or_create(&partial_temp.0.join("control.token")).unwrap();
        let partial_state = ControlState::new(
            partial_token.clone(),
            NodeId::new_v4(),
            NodeInstanceId::new_v4(),
            partial_downloads,
        );
        authorized.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!(
                "Bearer {}",
                partial_token.expose_for_authorization()
            ))
            .unwrap(),
        );
        let rejected = start_load(
            State(partial_state),
            authorized,
            Bytes::from(format!(r#"{{"model_id":"{}"}}"#, recipe.id)),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(rejected.into_body(), 4096)
            .await
            .unwrap();
        let error: loxa_core::control::contracts::ControlErrorBody =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, ControlErrorCode::ModelUnavailable);
        partial_worker.stop_and_join().unwrap();
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
