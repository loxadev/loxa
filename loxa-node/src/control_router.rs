use axum::extract::{RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, options};
use axum::{Json, Router};
use loxa_core::control::auth::{AuthPolicy, ControlToken};
use loxa_core::control::contracts::{
    CapabilitiesSnapshot, NodeIdentityChallenge, NodeIdentityProofResponse, NodeSnapshot,
    NodeStatus, CONTROL_PROTOCOL_VERSION,
};
use loxa_core::model_inventory::{current_available_memory_bytes, known_registry_inventory};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct ControlState {
    token: ControlToken,
    policy: Arc<AuthPolicy>,
    node_id: Arc<str>,
    runtime_identity: Arc<str>,
    models_dir: PathBuf,
}

impl ControlState {
    pub fn new(
        token: ControlToken,
        node_id: String,
        runtime_identity: String,
        models_dir: PathBuf,
    ) -> Self {
        let policy = AuthPolicy::new(
            token.clone(),
            ["tauri://localhost", "http://127.0.0.1:1420"],
        );
        Self {
            token,
            policy: Arc::new(policy),
            node_id: node_id.into(),
            runtime_identity: runtime_identity.into(),
            models_dir,
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
    if matches!(origin, "tauri://localhost" | "http://127.0.0.1:1420") {
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

async fn preflight(headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(Some(origin)) => origin,
        _ => return StatusCode::FORBIDDEN.into_response(),
    };
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, x-loxa-challenge"),
    );
    cors(response, Some(&origin))
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
    let status = NodeStatus::Unloaded;
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
        Json(known_registry_inventory(
            &state.models_dir,
            current_available_memory_bytes(),
        ))
        .into_response(),
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
    cors(
        Json(NodeSnapshot {
            status: NodeStatus::Unloaded,
            active_model_id: None,
            operation_id: None,
            error: None,
        })
        .into_response(),
        origin.as_deref(),
    )
}

pub fn router(state: ControlState) -> Router {
    Router::new()
        .route("/loxa/v1/node", get(node_proof))
        .route("/loxa/v1/capabilities", get(capabilities))
        .route("/loxa/v1/models", get(models))
        .route("/loxa/v1/node", options(preflight))
        .route("/loxa/v1/capabilities", options(preflight))
        .route("/loxa/v1/models", options(preflight))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_present_origin_is_rejected_not_treated_as_native() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(request_origin(&headers), Err(StatusCode::FORBIDDEN));
        assert_eq!(request_origin(&HeaderMap::new()), Ok(None));
    }
}
