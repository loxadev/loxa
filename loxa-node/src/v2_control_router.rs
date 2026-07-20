use crate::control_router::{cors, get_preflight, post_preflight, request_origin};
use crate::control_state::{ControlStateError, ControlStateHandle};
use crate::download_control::{DownloadControl, DownloadControlError, DurableExecutionControl};
use axum::body::Bytes;
use axum::extract::Request;
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use loxa_core::control::auth::{desktop_origins, AuthPolicy, ControlToken};
use loxa_core::registry::REGISTRY;
use loxa_protocol::v2::{
    DecimalU64, OperationId, SlotId, StreamEpoch, V2ControlErrorBody, V2ControlErrorCode,
    V2EmptyRequest, V2LoadRequest, V2NodeCollection, V2OperationAccepted, V2OperationCollection,
    V2OperationEnvelope, V2OperationStatus, V2SlotCollection, V2_SCHEMA_VERSION,
};
use loxa_protocol::NodeId;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_JSON_BODY_BYTES: usize = 4 * 1024;

#[derive(Clone)]
pub(crate) struct V2ControlState {
    policy: Arc<AuthPolicy>,
    control: ControlStateHandle,
    execution: DurableExecutionControl,
    inventory: Option<DownloadControl>,
    publication_gate: Option<crate::runtime::PublicationGate>,
    generated_at: GeneratedAtClock,
    #[cfg(test)]
    subscription_max_snapshot_bytes: Option<usize>,
    #[cfg(test)]
    injected_download_error: Option<DownloadControlError>,
}

#[derive(Clone)]
struct GeneratedAtClock {
    floor: Arc<AtomicU64>,
    #[cfg(test)]
    observed_unix_ms: Option<Arc<AtomicU64>>,
}

impl GeneratedAtClock {
    fn from_control(control: &ControlStateHandle) -> Result<Self, ControlStateError> {
        let floor = control.read_snapshot()?.last_committed_at_unix_ms.get();
        Ok(Self {
            floor: Arc::new(AtomicU64::new(floor)),
            #[cfg(test)]
            observed_unix_ms: None,
        })
    }

    fn next(&self, snapshot: &crate::control_state::state_machine::CommittedState) -> DecimalU64 {
        let observed = self.observed_wall_time();
        let candidate = observed.max(snapshot.last_committed_at_unix_ms.get());
        let previous = self.floor.fetch_max(candidate, Ordering::AcqRel);
        DecimalU64::new(previous.max(candidate))
    }

    fn observed_wall_time(&self) -> u64 {
        #[cfg(test)]
        if let Some(observed) = &self.observed_unix_ms {
            return observed.load(Ordering::Acquire);
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    fn with_observed_unix_ms(mut self, observed_unix_ms: Arc<AtomicU64>) -> Self {
        self.observed_unix_ms = Some(observed_unix_ms);
        self
    }
}

impl V2ControlState {
    pub(crate) fn new(
        token: ControlToken,
        control: ControlStateHandle,
        downloads: DownloadControl,
        publication_gate: Option<crate::runtime::PublicationGate>,
    ) -> Result<Self, &'static str> {
        let execution = downloads
            .durable_execution()
            .ok_or("v2 routes require durable execution authority")?;
        let generated_at = GeneratedAtClock::from_control(&control)
            .map_err(|_| "v2 routes require durable control state")?;
        Ok(Self {
            policy: Arc::new(AuthPolicy::new(token, desktop_origins())),
            control,
            execution,
            inventory: Some(downloads),
            publication_gate,
            generated_at,
            #[cfg(test)]
            subscription_max_snapshot_bytes: None,
            #[cfg(test)]
            injected_download_error: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        token: ControlToken,
        control: ControlStateHandle,
        execution: DurableExecutionControl,
    ) -> Self {
        let generated_at = GeneratedAtClock::from_control(&control)
            .expect("v2 test routes require durable control state");
        Self {
            policy: Arc::new(AuthPolicy::new(token, desktop_origins())),
            control,
            execution,
            inventory: None,
            publication_gate: None,
            generated_at,
            subscription_max_snapshot_bytes: None,
            injected_download_error: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_inventory_for_test(
        token: ControlToken,
        control: ControlStateHandle,
        execution: DurableExecutionControl,
        inventory: DownloadControl,
    ) -> Self {
        let generated_at = GeneratedAtClock::from_control(&control)
            .expect("v2 test routes require durable control state");
        Self {
            policy: Arc::new(AuthPolicy::new(token, desktop_origins())),
            control,
            execution,
            inventory: Some(inventory),
            publication_gate: None,
            generated_at,
            subscription_max_snapshot_bytes: None,
            injected_download_error: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_max_snapshot_bytes_for_test(
        mut self,
        max_snapshot_bytes: usize,
    ) -> Self {
        self.subscription_max_snapshot_bytes = Some(max_snapshot_bytes);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_injected_download_error_for_test(
        mut self,
        error: DownloadControlError,
    ) -> Self {
        self.injected_download_error = Some(error);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_publication_gate_for_test(
        mut self,
        gate: crate::runtime::PublicationGate,
    ) -> Self {
        self.publication_gate = Some(gate);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_observed_unix_ms_for_test(
        mut self,
        observed_unix_ms: Arc<AtomicU64>,
    ) -> Self {
        self.generated_at = self.generated_at.with_observed_unix_ms(observed_unix_ms);
        self
    }

    async fn validate_load_model(&self, model_id: &str) -> Result<(), V2RouteError> {
        validate_model_id(model_id)?;
        if !REGISTRY.iter().any(|entry| entry.id == model_id) {
            return Err(V2RouteError::new(
                StatusCode::NOT_FOUND,
                V2ControlErrorCode::UnknownModel,
                "The model was not found.",
            ));
        }
        let Some(downloads) = &self.inventory else {
            return Ok(());
        };
        let downloads = downloads.clone();
        let model_id = model_id.to_owned();
        let entry = tokio::task::spawn_blocking(move || {
            downloads.inventory(loxa_core::model_inventory::current_available_memory_bytes())
        })
        .await
        .map_err(|_| map_control_error(ControlStateError::DurableStateUnavailable))?
        .into_iter()
        .find(|entry| entry.id == model_id)
        .ok_or(V2RouteError::new(
            StatusCode::NOT_FOUND,
            V2ControlErrorCode::UnknownModel,
            "The model was not found.",
        ))?;
        if !crate::download_control::artifact_can_enter_load_verification(&entry.artifact)
            || !entry.compatibility.compatible
            || !entry.engine.eligible
            || entry.engine.engine != "llama-cpp"
        {
            return Err(V2RouteError::new(
                StatusCode::CONFLICT,
                V2ControlErrorCode::ModelUnavailable,
                "The model is unavailable for loading.",
            ));
        }
        Ok(())
    }
}

fn validate_model_id(model_id: &str) -> Result<(), V2RouteError> {
    if model_id.is_empty()
        || model_id.len() > 256
        || model_id.trim() != model_id
        || model_id.chars().any(char::is_control)
    {
        Err(V2RouteError::invalid_request())
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct V2RouteError {
    status: StatusCode,
    code: V2ControlErrorCode,
    message: &'static str,
}

impl V2RouteError {
    const fn invalid_request() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            V2ControlErrorCode::InvalidRequest,
            "The request is invalid.",
        )
    }

    const fn new(status: StatusCode, code: V2ControlErrorCode, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
}

impl IntoResponse for V2RouteError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(V2ControlErrorBody {
                code: self.code,
                message: self.message.into(),
            }),
        )
            .into_response()
    }
}

fn authorize(state: &V2ControlState, headers: &HeaderMap) -> Result<Option<String>, V2AuthError> {
    let origin = request_origin(headers).map_err(|status| V2AuthError {
        status,
        origin: None,
        apply_cors: false,
    })?;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state
        .policy
        .authorize(origin.as_deref(), bearer)
        .map_err(|_| V2AuthError {
            status: StatusCode::UNAUTHORIZED,
            origin: origin.clone(),
            apply_cors: true,
        })?;
    Ok(origin)
}

struct V2AuthError {
    status: StatusCode,
    origin: Option<String>,
    apply_cors: bool,
}

impl IntoResponse for V2AuthError {
    fn into_response(self) -> Response {
        let response = self.status.into_response();
        if self.apply_cors {
            cors(response, self.origin.as_deref())
        } else {
            response
        }
    }
}

async fn authenticated_boundary(
    State(state): State<V2ControlState>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let origin = match authorize(&state, request.headers()) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    if state
        .publication_gate
        .as_ref()
        .is_some_and(|gate| !gate.is_open())
    {
        return cors(
            V2RouteError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                V2ControlErrorCode::DurableStateUnavailable,
                "Durable control state is unavailable.",
            )
            .into_response(),
            origin.as_deref(),
        );
    }
    let mut response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        response = V2RouteError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            V2ControlErrorCode::InvalidRequest,
            "The request body exceeds 4096 bytes.",
        )
        .into_response();
    } else if response.status() == StatusCode::BAD_REQUEST
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_none_or(|value| value != HeaderValue::from_static("application/json"))
    {
        response = V2RouteError::invalid_request().into_response();
    }
    if !response.headers().contains_key(header::VARY) {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("Origin"));
    }
    if let Some(origin) = origin.and_then(|value| HeaderValue::from_str(&value).ok()) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    response
}

fn json_content_type(headers: &HeaderMap) -> Result<(), V2RouteError> {
    let valid = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if valid {
        Ok(())
    } else {
        Err(V2RouteError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            V2ControlErrorCode::UnsupportedMediaType,
            "Content type must be application/json.",
        ))
    }
}

fn strict_empty_body(body: &[u8]) -> Result<(), V2RouteError> {
    if body.is_empty() || body.len() > MAX_JSON_BODY_BYTES {
        return Err(V2RouteError::invalid_request());
    }
    serde_json::from_slice::<V2EmptyRequest>(body)
        .map(|_| ())
        .map_err(|_| V2RouteError::invalid_request())
}

fn strict_load_body(body: &[u8]) -> Result<V2LoadRequest, V2RouteError> {
    if body.is_empty() || body.len() > MAX_JSON_BODY_BYTES {
        return Err(V2RouteError::invalid_request());
    }
    serde_json::from_slice(body).map_err(|_| V2RouteError::invalid_request())
}

fn snapshot_parts(
    state: &V2ControlState,
) -> Result<
    (
        Arc<crate::control_state::state_machine::CommittedState>,
        StreamEpoch,
    ),
    V2RouteError,
> {
    let snapshot = state.control.read_snapshot().map_err(map_control_error)?;
    let epoch = snapshot
        .events
        .last()
        .map(|event| event.epoch)
        .ok_or_else(|| map_control_error(ControlStateError::DurableStateUnavailable))?;
    Ok((snapshot, epoch))
}

fn map_control_error(error: ControlStateError) -> V2RouteError {
    match error {
        ControlStateError::WriterOverloaded => V2RouteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            V2ControlErrorCode::StateWriterOverloaded,
            "The durable state writer is overloaded.",
        ),
        _ => V2RouteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            V2ControlErrorCode::DurableStateUnavailable,
            "Durable control state is unavailable.",
        ),
    }
}

fn map_execution_error(state: &V2ControlState, error: DownloadControlError) -> V2RouteError {
    match error {
        DownloadControlError::Conflict => V2RouteError::new(
            StatusCode::CONFLICT,
            V2ControlErrorCode::OperationConflict,
            "A conflicting operation is active.",
        ),
        DownloadControlError::WriterOverloaded => V2RouteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            V2ControlErrorCode::StateWriterOverloaded,
            "The durable state writer is overloaded.",
        ),
        DownloadControlError::Missing => V2RouteError::new(
            StatusCode::NOT_FOUND,
            V2ControlErrorCode::OperationNotFound,
            "The operation was not found.",
        ),
        DownloadControlError::Terminal => V2RouteError::new(
            StatusCode::CONFLICT,
            V2ControlErrorCode::OperationTerminal,
            "The operation is already terminal.",
        ),
        DownloadControlError::Stopping if state.control.is_healthy() => V2RouteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            V2ControlErrorCode::NodeStopping,
            "The node is stopping.",
        ),
        DownloadControlError::Stopping => V2RouteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            V2ControlErrorCode::DurableStateUnavailable,
            "Durable control state is unavailable.",
        ),
        DownloadControlError::CancellationNotSafe => V2RouteError::new(
            StatusCode::CONFLICT,
            V2ControlErrorCode::CancellationNotSafe,
            "Cancellation is not safe at the current operation boundary.",
        ),
        DownloadControlError::ModelUnavailable => V2RouteError::new(
            StatusCode::CONFLICT,
            V2ControlErrorCode::ModelUnavailable,
            "The model is unavailable for loading.",
        ),
    }
}

async fn nodes(State(state): State<V2ControlState>, headers: HeaderMap) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = snapshot_parts(&state).map(|(snapshot, epoch)| {
        let generated_at_unix_ms = state.generated_at.next(&snapshot);
        Json(V2NodeCollection {
            schema_version: V2_SCHEMA_VERSION,
            epoch,
            revision: snapshot.revision,
            generated_at_unix_ms,
            nodes: snapshot.node.iter().cloned().collect(),
        })
        .into_response()
    });
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn slots(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result: Result<Response, V2RouteError> = (|| {
        let requested = NodeId::from_str(&node_id).map_err(|_| V2RouteError::invalid_request())?;
        let (snapshot, epoch) = snapshot_parts(&state)?;
        let node = snapshot
            .node
            .as_ref()
            .ok_or_else(|| map_control_error(ControlStateError::DurableStateUnavailable))?;
        if requested != node.node_id {
            return Err(V2RouteError::new(
                StatusCode::NOT_FOUND,
                V2ControlErrorCode::NodeNotFound,
                "The node was not found.",
            ));
        }
        Ok(Json(V2SlotCollection {
            schema_version: V2_SCHEMA_VERSION,
            epoch,
            revision: snapshot.revision,
            generated_at_unix_ms: state.generated_at.next(&snapshot),
            node_id: requested,
            slots: vec![snapshot.slot.clone()],
        })
        .into_response())
    })();
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn operations(State(state): State<V2ControlState>, headers: HeaderMap) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = snapshot_parts(&state).map(|(snapshot, epoch)| {
        let generated_at_unix_ms = state.generated_at.next(&snapshot);
        Json(V2OperationCollection {
            schema_version: V2_SCHEMA_VERSION,
            epoch,
            revision: snapshot.revision,
            generated_at_unix_ms,
            operations: snapshot.operations.clone(),
        })
        .into_response()
    });
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn operation(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path(operation_id): Path<String>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result: Result<Response, V2RouteError> = (|| {
        let requested =
            OperationId::from_str(&operation_id).map_err(|_| V2RouteError::invalid_request())?;
        let (snapshot, epoch) = snapshot_parts(&state)?;
        let operation = snapshot
            .operations
            .iter()
            .find(|operation| operation.operation_id == requested)
            .cloned()
            .ok_or(V2RouteError::new(
                StatusCode::NOT_FOUND,
                V2ControlErrorCode::OperationNotFound,
                "The operation was not found.",
            ))?;
        Ok(Json(V2OperationEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            epoch,
            revision: snapshot.revision,
            generated_at_unix_ms: state.generated_at.next(&snapshot),
            operation,
        })
        .into_response())
    })();
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

fn validate_node_slot(
    state: &V2ControlState,
    node_id: &str,
    slot_id: &str,
) -> Result<(), V2RouteError> {
    let requested_node = NodeId::from_str(node_id).map_err(|_| V2RouteError::invalid_request())?;
    let requested_slot = SlotId::from_str(slot_id).map_err(|_| V2RouteError::invalid_request())?;
    let (snapshot, _) = snapshot_parts(state)?;
    let Some(node) = snapshot.node.as_ref() else {
        return Err(map_control_error(
            ControlStateError::DurableStateUnavailable,
        ));
    };
    if requested_node != node.node_id {
        return Err(V2RouteError::new(
            StatusCode::NOT_FOUND,
            V2ControlErrorCode::NodeNotFound,
            "The node was not found.",
        ));
    }
    if requested_slot != snapshot.slot.slot_id {
        return Err(V2RouteError::new(
            StatusCode::NOT_FOUND,
            V2ControlErrorCode::SlotNotFound,
            "The slot was not found.",
        ));
    }
    Ok(())
}

fn accepted(admission: crate::control_state::state_machine::CommittedAdmission) -> Response {
    (
        StatusCode::ACCEPTED,
        Json(V2OperationAccepted {
            epoch: admission.epoch,
            operation_id: admission.operation_id,
            revision: admission.revision,
        }),
    )
        .into_response()
}

async fn download(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path(model_id): Path<String>,
    body: Bytes,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = async {
        json_content_type(&headers)?;
        strict_empty_body(&body)?;
        validate_model_id(&model_id)?;
        let recipe =
            REGISTRY
                .iter()
                .find(|entry| entry.id == model_id)
                .ok_or(V2RouteError::new(
                    StatusCode::NOT_FOUND,
                    V2ControlErrorCode::UnknownModel,
                    "The model was not found.",
                ))?;
        #[cfg(test)]
        if let Some(error) = state.injected_download_error {
            return Err(map_execution_error(&state, error));
        }
        state
            .execution
            .start_download(&model_id, recipe.size_bytes)
            .await
            .map(accepted)
            .map_err(|error| map_execution_error(&state, error))
    }
    .await;
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn load(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path((node_id, slot_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = async {
        json_content_type(&headers)?;
        let request = strict_load_body(&body)?;
        validate_node_slot(&state, &node_id, &slot_id)?;
        state.validate_load_model(&request.model_id).await?;
        state
            .execution
            .start_load(&request.model_id)
            .await
            .map(accepted)
            .map_err(|error| map_execution_error(&state, error))
    }
    .await;
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn unload(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path((node_id, slot_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = async {
        json_content_type(&headers)?;
        strict_empty_body(&body)?;
        validate_node_slot(&state, &node_id, &slot_id)?;
        state
            .execution
            .start_unload()
            .await
            .map(accepted)
            .map_err(|error| map_execution_error(&state, error))
    }
    .await;
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

async fn cancel(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    Path(operation_id): Path<String>,
    body: Bytes,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let result = async {
        json_content_type(&headers)?;
        strict_empty_body(&body)?;
        let requested =
            OperationId::from_str(&operation_id).map_err(|_| V2RouteError::invalid_request())?;
        let (snapshot, epoch) = snapshot_parts(&state)?;
        let operation = snapshot
            .operations
            .iter()
            .find(|operation| operation.operation_id == requested)
            .ok_or(V2RouteError::new(
                StatusCode::NOT_FOUND,
                V2ControlErrorCode::OperationNotFound,
                "The operation was not found.",
            ))?;
        if matches!(
            operation.status,
            V2OperationStatus::Succeeded | V2OperationStatus::Failed | V2OperationStatus::Cancelled
        ) {
            return Err(V2RouteError::new(
                StatusCode::CONFLICT,
                V2ControlErrorCode::OperationTerminal,
                "The operation is already terminal.",
            ));
        }
        let alias = snapshot
            .current_instance_v1
            .operations
            .iter()
            .find(|entry| entry.operation.operation_id == requested)
            .map(|entry| entry.v1_operation_id.clone())
            .ok_or(V2RouteError::new(
                StatusCode::NOT_FOUND,
                V2ControlErrorCode::OperationNotFound,
                "The operation was not found.",
            ))?;
        state
            .execution
            .cancel(&alias)
            .await
            .map_err(|error| map_execution_error(&state, error))?;
        let committed = state.control.read_snapshot().map_err(map_control_error)?;
        Ok((
            StatusCode::ACCEPTED,
            Json(V2OperationAccepted {
                epoch,
                operation_id: requested,
                revision: committed.revision,
            }),
        )
            .into_response())
    }
    .await;
    cors(
        result.unwrap_or_else(IntoResponse::into_response),
        origin.as_deref(),
    )
}

fn parse_resume(raw: Option<String>) -> Result<Option<(StreamEpoch, DecimalU64)>, V2RouteError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut epoch = None;
    let mut cursor = None;
    for pair in raw.split('&') {
        let Some((name, value)) = pair.split_once('=') else {
            return Err(V2RouteError::invalid_request());
        };
        match name {
            "epoch" if epoch.is_none() => {
                epoch = Some(
                    StreamEpoch::from_str(value).map_err(|_| V2RouteError::invalid_request())?,
                )
            }
            "cursor" if cursor.is_none() => {
                cursor = Some(
                    serde_json::from_str::<DecimalU64>(&format!("\"{value}\""))
                        .map_err(|_| V2RouteError::invalid_request())?,
                )
            }
            _ => return Err(V2RouteError::invalid_request()),
        }
    }
    match (epoch, cursor) {
        (Some(epoch), Some(cursor)) => Ok(Some((epoch, cursor))),
        _ => Err(V2RouteError::invalid_request()),
    }
}

async fn events(
    State(state): State<V2ControlState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(origin) => origin,
        Err(error) => return error.into_response(),
    };
    let resume = match parse_resume(raw) {
        Ok(resume) => resume,
        Err(error) => return cors(error.into_response(), origin.as_deref()),
    };
    let generated_at_unix_ms = match state.control.read_snapshot() {
        Ok(snapshot) => state.generated_at.next(&snapshot),
        Err(error) => return cors(map_control_error(error).into_response(), origin.as_deref()),
    };
    #[cfg(test)]
    let subscription_result = match state.subscription_max_snapshot_bytes {
        Some(max_snapshot_bytes) => {
            state
                .control
                .subscribe_with_max_snapshot_bytes_for_test(
                    resume,
                    generated_at_unix_ms,
                    max_snapshot_bytes,
                )
                .await
        }
        None => state.control.subscribe(resume, generated_at_unix_ms).await,
    };
    #[cfg(not(test))]
    let subscription_result = state.control.subscribe(resume, generated_at_unix_ms).await;
    let subscription = match subscription_result {
        Ok(subscription) => subscription,
        Err(error) => return cors(map_control_error(error).into_response(), origin.as_deref()),
    };
    let initial = match serde_json::to_string(&subscription.snapshot) {
        Ok(initial) => initial,
        Err(_) => {
            return cors(
                map_control_error(ControlStateError::DurableStateUnavailable).into_response(),
                origin.as_deref(),
            )
        }
    };
    let stream = futures_util::stream::unfold(
        (Some(initial), subscription.events),
        |(initial, mut events)| async move {
            if let Some(initial) = initial {
                return Some((
                    Ok::<Event, Infallible>(Event::default().event("snapshot").data(initial)),
                    (None, events),
                ));
            }
            let event = events.recv().await?;
            let data = serde_json::to_string(&event).ok()?;
            Some((
                Ok(Event::default()
                    .id(event.sequence.to_string())
                    .event("state")
                    .data(data)),
                (None, events),
            ))
        },
    );
    cors(
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response(),
        origin.as_deref(),
    )
}

pub(crate) fn router(state: V2ControlState) -> Router {
    let boundary_state = state.clone();
    Router::new()
        .route("/loxa/v2/nodes", get(nodes).options(get_preflight))
        .route(
            "/loxa/v2/nodes/{node_id}/slots",
            get(slots).options(get_preflight),
        )
        .route(
            "/loxa/v2/operations",
            get(operations).options(get_preflight),
        )
        .route(
            "/loxa/v2/operations/{operation_id}",
            get(operation).options(get_preflight),
        )
        .route("/loxa/v2/events", get(events).options(get_preflight))
        .route(
            "/loxa/v2/models/{model_id}/download",
            post(download).options(post_preflight),
        )
        .route(
            "/loxa/v2/nodes/{node_id}/slots/{slot_id}/load",
            post(load).options(post_preflight),
        )
        .route(
            "/loxa/v2/nodes/{node_id}/slots/{slot_id}/unload",
            post(unload).options(post_preflight),
        )
        .route(
            "/loxa/v2/operations/{operation_id}/cancel",
            post(cancel).options(post_preflight),
        )
        .layer(DefaultBodyLimit::max(MAX_JSON_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            boundary_state,
            authenticated_boundary,
        ))
        .with_state(state)
}
