use axum::{
    body::{Body, Bytes},
    extract::{rejection::JsonRejection, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::thread;

use crate::control::auth::is_desktop_origin;

pub const MODEL_ALIAS: &str = "loxa";
const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineTarget {
    pub base_url: String,
    pub backend_alias: String,
    pub engine: String,
    pub engine_version: String,
    pub model_id: String,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationProvenance {
    pub engine: String,
    pub engine_version: String,
    pub model_id: String,
    pub profile: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationError {
    InvalidModel,
    EngineUnavailable,
    GatewayShuttingDown,
    UpstreamUnavailable,
    UpstreamRejected,
    InvalidResponse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationStreamError {
    Upstream,
    EventTooLarge,
}

impl std::fmt::Display for GenerationStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Upstream => "the managed engine stream ended unexpectedly",
            Self::EventTooLarge => "the managed engine stream exceeded the event size limit",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for GenerationStreamError {}

pub type GenerationStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, GenerationStreamError>> + Send + 'static>>;

pub enum GenerationOutput {
    Json { status: StatusCode, body: Value },
    Stream(GenerationStream),
}

pub struct PreparedGeneration {
    request: Value,
    target: Arc<EngineTarget>,
    client: reqwest::Client,
    cancellation: tokio::sync::watch::Receiver<bool>,
    streaming: bool,
    provenance: GenerationProvenance,
}

impl PreparedGeneration {
    pub fn provenance(&self) -> &GenerationProvenance {
        &self.provenance
    }

    pub async fn execute(self) -> Result<GenerationOutput, GenerationError> {
        let Self {
            request,
            target,
            client,
            mut cancellation,
            streaming,
            provenance: _,
        } = self;
        let send = client
            .post(format!("{}/v1/chat/completions", target.base_url))
            .json(&request)
            .send();
        let upstream = match tokio::select! {
            response = send => response,
            _ = cancellation.changed() => return Err(GenerationError::GatewayShuttingDown),
        } {
            Ok(response) => response,
            Err(_) => return Err(GenerationError::UpstreamUnavailable),
        };
        if streaming {
            if !upstream.status().is_success() {
                return Err(GenerationError::UpstreamRejected);
            }
            return Ok(GenerationOutput::Stream(Box::pin(normalize_sse(
                upstream.bytes_stream(),
                target.backend_alias.clone(),
                cancellation,
            ))));
        }
        let status = upstream.status();
        let body = upstream.json::<Value>();
        let mut body = tokio::select! {
            body = body => body.map_err(|_| GenerationError::InvalidResponse)?,
            _ = cancellation.changed() => return Err(GenerationError::GatewayShuttingDown),
        };
        normalize_aliases(&mut body, &target.backend_alias);
        if !status.is_success() {
            normalize_embedded_aliases(&mut body, &target.backend_alias);
        }
        let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        Ok(GenerationOutput::Json { status, body })
    }
}

#[derive(Clone)]
pub struct GatewayState {
    node_id: Arc<str>,
    target: Arc<RwLock<Option<Arc<EngineTarget>>>>,
    client: reqwest::Client,
    cancellation: tokio::sync::watch::Sender<bool>,
}

impl GatewayState {
    pub fn new(node_id: impl Into<String>) -> Self {
        let (cancellation, _) = tokio::sync::watch::channel(false);
        Self {
            node_id: Arc::from(node_id.into()),
            target: Arc::new(RwLock::new(None)),
            client: reqwest::Client::new(),
            cancellation,
        }
    }

    pub fn publish(&self, target: EngineTarget) {
        *self.target.write().expect("gateway target lock poisoned") = Some(Arc::new(target));
    }

    pub fn withdraw(&self) {
        *self.target.write().expect("gateway target lock poisoned") = None;
    }

    pub fn snapshot(&self) -> Option<Arc<EngineTarget>> {
        self.target
            .read()
            .expect("gateway target lock poisoned")
            .clone()
    }

    pub fn prepare_generation(
        &self,
        mut request: Value,
    ) -> Result<PreparedGeneration, GenerationError> {
        if request.get("model").and_then(Value::as_str) != Some(MODEL_ALIAS) {
            return Err(GenerationError::InvalidModel);
        }
        let target = self.snapshot().ok_or(GenerationError::EngineUnavailable)?;
        request["model"] = Value::String(target.backend_alias.clone());
        let streaming = request.get("stream").and_then(Value::as_bool) == Some(true);
        let cancellation = self.cancellation.subscribe();
        if *cancellation.borrow() {
            return Err(GenerationError::GatewayShuttingDown);
        }
        let provenance = GenerationProvenance {
            engine: target.engine.clone(),
            engine_version: target.engine_version.clone(),
            model_id: target.model_id.clone(),
            profile: target.profile.clone(),
        };
        Ok(PreparedGeneration {
            request,
            target,
            client: self.client.clone(),
            cancellation,
            streaming,
            provenance,
        })
    }

    fn cancel_requests(&self) {
        self.cancellation.send_replace(true);
    }
}

#[derive(Serialize)]
struct ModelList {
    object: &'static str,
    data: [Model; 1],
}

#[derive(Serialize)]
struct Model {
    id: &'static str,
    object: &'static str,
    owned_by: &'static str,
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

async fn preflight(headers: HeaderMap, method: &'static str) -> Response {
    let origin = match request_origin(&headers) {
        Ok(Some(origin)) => origin,
        _ => return StatusCode::FORBIDDEN.into_response(),
    };
    if headers
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok())
        != Some(method)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Some(requested_headers) = headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
    {
        let allowed = requested_headers.split(',').all(|name| {
            name.trim().is_empty()
                || (method == "POST" && name.trim().eq_ignore_ascii_case("content-type"))
        });
        if !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(if method == "GET" {
            "GET, OPTIONS"
        } else {
            "POST, OPTIONS"
        }),
    );
    if method == "POST" {
        response.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("content-type"),
        );
    }
    cors(response, Some(&origin))
}

async fn get_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "GET").await
}

async fn post_preflight(headers: HeaderMap) -> Response {
    preflight(headers, "POST").await
}

async fn models(headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    cors(
        Json(ModelList {
            object: "list",
            data: [Model {
                id: MODEL_ALIAS,
                object: "model",
                owned_by: "loxa",
            }],
        })
        .into_response(),
        origin.as_deref(),
    )
}

async fn status(State(state): State<GatewayState>, headers: HeaderMap) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    let target = state.snapshot();
    cors(
        Json(match target {
            Some(target) => json!({
                "node_id": &*state.node_id,
                "health": "ready",
                "model": MODEL_ALIAS,
                "engine": { "name": target.engine, "version": target.engine_version },
                "runtime_model": target.model_id,
                "profile": target.profile,
            }),
            None => json!({
                "node_id": &*state.node_id,
                "health": "unavailable",
                "model": MODEL_ALIAS,
                "engine": null,
                "runtime_model": null,
                "profile": null,
            }),
        })
        .into_response(),
        origin.as_deref(),
    )
}

fn openai_error(status: StatusCode, message: &str, code: &'static str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message,
                "type": if status.is_client_error() { "invalid_request_error" } else { "server_error" },
                "param": if code == "invalid_model" { Value::String("model".into()) } else { Value::Null },
                "code": code,
            }
        })),
    )
        .into_response()
}

async fn chat(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    payload: Result<Json<Value>, JsonRejection>,
) -> Response {
    let origin = match request_origin(&headers) {
        Ok(origin) => origin,
        Err(status) => return status.into_response(),
    };
    cors(chat_inner(state, payload).await, origin.as_deref())
}

async fn chat_inner(state: GatewayState, payload: Result<Json<Value>, JsonRejection>) -> Response {
    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return openai_error(
                StatusCode::BAD_REQUEST,
                "request body must be valid JSON",
                "invalid_request",
            )
        }
    };
    let generation = match state.prepare_generation(request) {
        Ok(generation) => generation,
        Err(error) => return generation_error_response(error),
    };
    match generation.execute().await {
        Ok(GenerationOutput::Json { status, body }) => (status, Json(body)).into_response(),
        Ok(GenerationOutput::Stream(stream)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .expect("valid streaming response"),
        Err(error) => generation_error_response(error),
    }
}

fn generation_error_response(error: GenerationError) -> Response {
    match error {
        GenerationError::InvalidModel => openai_error(
            StatusCode::BAD_REQUEST,
            "model must be the stable alias 'loxa'",
            "invalid_model",
        ),
        GenerationError::EngineUnavailable => openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the managed engine is temporarily unavailable",
            "engine_unavailable",
        ),
        GenerationError::GatewayShuttingDown => openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the gateway is shutting down",
            "engine_unavailable",
        ),
        GenerationError::UpstreamUnavailable => openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the managed engine could not accept the request",
            "upstream_error",
        ),
        GenerationError::UpstreamRejected => openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the managed engine rejected the streaming request",
            "upstream_error",
        ),
        GenerationError::InvalidResponse => openai_error(
            StatusCode::BAD_GATEWAY,
            "the managed engine returned an invalid response",
            "upstream_error",
        ),
    }
}

struct SseState {
    upstream: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buffer: Vec<u8>,
    ready: VecDeque<Bytes>,
    finished: bool,
    backend_alias: String,
    cancellation: tokio::sync::watch::Receiver<bool>,
}

fn normalize_sse(
    upstream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static,
    backend_alias: String,
    cancellation: tokio::sync::watch::Receiver<bool>,
) -> impl Stream<Item = Result<Bytes, GenerationStreamError>> + Send {
    futures_util::stream::unfold(
        SseState {
            upstream: Box::pin(upstream),
            buffer: Vec::new(),
            ready: VecDeque::new(),
            finished: false,
            backend_alias,
            cancellation,
        },
        |mut state| async move {
            loop {
                if *state.cancellation.borrow() {
                    return None;
                }
                if let Some(chunk) = state.ready.pop_front() {
                    return Some((Ok(chunk), state));
                }
                while let Some((end, delimiter_len)) = next_sse_boundary(&state.buffer) {
                    if end > MAX_SSE_EVENT_BYTES {
                        state.buffer.clear();
                        state.finished = true;
                        return Some((Err(GenerationStreamError::EventTooLarge), state));
                    }
                    let event = state
                        .buffer
                        .drain(..end + delimiter_len)
                        .collect::<Vec<_>>();
                    state.ready.push_back(Bytes::from(normalize_sse_event(
                        &event,
                        &state.backend_alias,
                    )));
                }
                if let Some(chunk) = state.ready.pop_front() {
                    return Some((Ok(chunk), state));
                }
                if state.finished {
                    if state.buffer.is_empty() {
                        return None;
                    }
                    let tail = std::mem::take(&mut state.buffer);
                    return Some((
                        Ok(Bytes::from(normalize_sse_event(
                            &tail,
                            &state.backend_alias,
                        ))),
                        state,
                    ));
                }
                if state.buffer.len() > MAX_SSE_EVENT_BYTES {
                    state.buffer.clear();
                    state.finished = true;
                    return Some((Err(GenerationStreamError::EventTooLarge), state));
                }
                let next = tokio::select! {
                    next = state.upstream.next() => next,
                    _ = state.cancellation.changed() => return None,
                };
                match next {
                    Some(Ok(chunk)) => state.buffer.extend_from_slice(&chunk),
                    Some(Err(_error)) => {
                        state.finished = true;
                        return Some((Err(GenerationStreamError::Upstream), state));
                    }
                    None => state.finished = true,
                }
            }
        },
    )
}

fn next_sse_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    let crlf = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (index, 4));
    let lf = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2));
    match (crlf, lf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(boundary), None) | (None, Some(boundary)) => Some(boundary),
        (None, None) => None,
    }
}

fn normalize_sse_event(event: &[u8], backend_alias: &str) -> Vec<u8> {
    let Ok(text) = std::str::from_utf8(event) else {
        return event.to_vec();
    };
    let mut normalized = Vec::with_capacity(event.len());
    for line_with_ending in text.split_inclusive('\n') {
        let (line, ending) = match line_with_ending.strip_suffix("\r\n") {
            Some(line) => (line, "\r\n"),
            None => match line_with_ending.strip_suffix('\n') {
                Some(line) => (line, "\n"),
                None => (line_with_ending, ""),
            },
        };
        let replacement = line.strip_prefix("data:").and_then(|data| {
            let whitespace_len = data.len() - data.trim_start().len();
            let payload = &data[whitespace_len..];
            if payload == "[DONE]" {
                return None;
            }
            let mut json = serde_json::from_str::<Value>(payload).ok()?;
            normalize_aliases(&mut json, backend_alias);
            Some(format!("data:{}{}", &data[..whitespace_len], json))
        });
        normalized.extend_from_slice(replacement.as_deref().unwrap_or(line).as_bytes());
        normalized.extend_from_slice(ending.as_bytes());
    }
    normalized
}

fn normalize_aliases(value: &mut Value, backend_alias: &str) {
    match value {
        Value::String(text) if text == backend_alias => *text = MODEL_ALIAS.to_string(),
        Value::Array(values) => {
            for value in values {
                normalize_aliases(value, backend_alias);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                normalize_aliases(value, backend_alias);
            }
            if object.contains_key("model") {
                object.insert("model".into(), Value::String(MODEL_ALIAS.into()));
            }
        }
        _ => {}
    }
}

fn normalize_embedded_aliases(value: &mut Value, backend_alias: &str) {
    match value {
        Value::String(text) => *text = text.replace(backend_alias, MODEL_ALIAS),
        Value::Array(values) => {
            for value in values {
                normalize_embedded_aliases(value, backend_alias);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                normalize_embedded_aliases(value, backend_alias);
            }
        }
        _ => {}
    }
}

pub fn router(state: GatewayState) -> Router {
    Router::new()
        .route("/v1/models", get(models).options(get_preflight))
        .route("/v1/chat/completions", post(chat).options(post_preflight))
        .route("/loxa/status", get(status).options(get_preflight))
        .with_state(state)
}

pub struct GatewayServer {
    port: u16,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<io::Result<()>>>,
    state: GatewayState,
}

impl GatewayServer {
    pub fn start(port: u16, state: GatewayState) -> io::Result<Self> {
        let app = router(state.clone());
        Self::start_with_router(port, state, app)
    }

    pub fn start_with_router(port: u16, state: GatewayState, app: Router) -> io::Result<Self> {
        Self::start_with_router_and_spawner(
            port,
            state,
            app,
            |listener, app, receiver, server_state| {
                thread::Builder::new()
                    .name("loxa-gateway".into())
                    .spawn(move || {
                        let runtime = tokio::runtime::Runtime::new().map_err(io::Error::other)?;
                        runtime.block_on(async move {
                            let listener = tokio::net::TcpListener::from_std(listener)?;
                            let _ = server_state;
                            axum::serve(listener, app)
                                .with_graceful_shutdown(async move {
                                    let _ = receiver.await;
                                })
                                .await
                                .map_err(io::Error::other)
                        })
                    })
            },
        )
    }

    fn start_with_router_and_spawner<F>(
        port: u16,
        state: GatewayState,
        app: Router,
        spawn: F,
    ) -> io::Result<Self>
    where
        F: FnOnce(
            std::net::TcpListener,
            Router,
            tokio::sync::oneshot::Receiver<()>,
            GatewayState,
        ) -> io::Result<thread::JoinHandle<io::Result<()>>>,
    {
        let started = std::time::Instant::now();
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        let server_state = state.clone();
        let thread = spawn(listener, app, receiver, server_state)?;
        tracing::info!(
            target: "loxa_core::gateway",
            event_code = "gateway.starting",
            component = "gateway",
            runtime_identity = state.node_id.as_ref(),
            result_class = "started",
        );
        tracing::info!(
            target: "loxa_core::gateway",
            event_code = "gateway.listening",
            component = "gateway",
            runtime_identity = state.node_id.as_ref(),
            result_class = "listening",
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        Ok(Self {
            port,
            shutdown: Some(shutdown),
            thread: Some(thread),
            state,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn shutdown(mut self) -> io::Result<()> {
        let started = std::time::Instant::now();
        tracing::info!(
            target: "loxa_core::gateway",
            event_code = "gateway.stop_requested",
            component = "gateway",
            runtime_identity = self.state.node_id.as_ref(),
            result_class = "requested",
        );
        self.state.cancel_requests();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let result = match self.thread.take().expect("gateway thread present").join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("gateway thread panicked")),
        };
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        match result {
            Ok(()) => {
                tracing::info!(
                    target: "loxa_core::gateway",
                    event_code = "gateway.stopped",
                    component = "gateway",
                    runtime_identity = self.state.node_id.as_ref(),
                    result_class = "stopped",
                    duration_ms,
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    target: "loxa_core::gateway",
                    event_code = "gateway.join_failed",
                    component = "gateway",
                    runtime_identity = self.state.node_id.as_ref(),
                    result_class = "join_failed",
                    duration_ms,
                );
                Err(error)
            }
        }
    }
}

impl Drop for GatewayServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_sse_boundary, normalize_sse, router, EngineTarget, GatewayServer, GatewayState,
        GenerationError, GenerationOutput, GenerationProvenance, GenerationStreamError,
        MAX_SSE_EVENT_BYTES,
    };
    use axum::http::{header, Method, StatusCode};
    use axum::{
        body::{Body, Bytes},
        response::{IntoResponse, Response},
    };
    use axum::{extract::State, routing::post, Json, Router};
    use futures_util::stream;
    use futures_util::StreamExt;
    use reqwest::Client;
    use serde_json::json;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Metadata, Subscriber};

    #[derive(Clone, Debug)]
    struct CapturedEvent {
        target: String,
        level: tracing::Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct EventCapture(Arc<Mutex<Vec<CapturedEvent>>>);

    struct FieldCapture<'a>(&'a mut BTreeMap<String, String>);

    impl Visit for FieldCapture<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl Subscriber for EventCapture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut FieldCapture(&mut fields));
            self.0
                .lock()
                .expect("capture poisoned")
                .push(CapturedEvent {
                    target: event.metadata().target().to_owned(),
                    level: *event.metadata().level(),
                    fields,
                });
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn event_codes(events: &[CapturedEvent]) -> Vec<&str> {
        events
            .iter()
            .filter_map(|event| event.fields.get("event_code").map(String::as_str))
            .collect()
    }

    #[test]
    fn gateway_start_and_shutdown_emit_static_lifecycle_events() {
        let capture = EventCapture::default();
        let output = capture.0.clone();
        tracing::subscriber::with_default(capture, || {
            let server =
                GatewayServer::start(0, GatewayState::new("runtime-test")).expect("start gateway");
            server.shutdown().expect("shutdown gateway");
        });

        let output = output.lock().expect("capture poisoned");
        assert_eq!(
            event_codes(&output),
            [
                "gateway.starting",
                "gateway.listening",
                "gateway.stop_requested",
                "gateway.stopped",
            ]
        );
        for event in output.iter() {
            assert_eq!(event.target, "loxa_core::gateway");
            assert_eq!(event.level, tracing::Level::INFO);
            assert_eq!(
                event.fields.get("component").map(String::as_str),
                Some("gateway")
            );
        }
    }

    #[test]
    fn gateway_bind_failure_emits_no_start_or_listening_event() {
        let occupied =
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("occupy port");
        let port = occupied.local_addr().expect("occupied address").port();
        let capture = EventCapture::default();
        let output = capture.0.clone();
        tracing::subscriber::with_default(capture, || {
            assert!(GatewayServer::start(port, GatewayState::new("runtime-test")).is_err());
        });

        let output = output.lock().expect("capture poisoned");
        assert!(event_codes(&output).is_empty(), "{output:?}");
    }

    #[test]
    fn gateway_thread_spawn_failure_emits_no_start_or_listening_event() {
        let capture = EventCapture::default();
        let output = capture.0.clone();
        tracing::subscriber::with_default(capture, || {
            let error = match GatewayServer::start_with_router_and_spawner(
                0,
                GatewayState::new("runtime-test"),
                Router::new(),
                |_, _, _, _| Err(io::Error::other("ARBITRARY_THREAD_SPAWN_ERROR")),
            ) {
                Ok(_) => panic!("thread spawn must fail"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("ARBITRARY_THREAD_SPAWN_ERROR"));
        });

        let output = output.lock().expect("capture poisoned");
        assert!(event_codes(&output).is_empty(), "{output:?}");
        assert!(!format!("{output:?}").contains("ARBITRARY_THREAD_SPAWN_ERROR"));
    }

    #[test]
    fn gateway_join_failure_is_warn_after_owned_start_and_stop_request() {
        let capture = EventCapture::default();
        let output = capture.0.clone();
        tracing::subscriber::with_default(capture, || {
            let server = GatewayServer::start_with_router_and_spawner(
                0,
                GatewayState::new("runtime-test"),
                Router::new(),
                |_, _, _, _| {
                    thread::Builder::new().spawn(|| panic!("ARBITRARY_GATEWAY_THREAD_PANIC"))
                },
            )
            .expect("thread ownership acquired");
            assert!(server.shutdown().is_err());
        });

        let output = output.lock().expect("capture poisoned");
        assert_eq!(
            event_codes(&output),
            [
                "gateway.starting",
                "gateway.listening",
                "gateway.stop_requested",
                "gateway.join_failed",
            ]
        );
        assert!(output[..3]
            .iter()
            .all(|event| event.level == tracing::Level::INFO));
        assert_eq!(output[3].level, tracing::Level::WARN);
        assert!(output
            .iter()
            .all(|event| event.target == "loxa_core::gateway"));
        assert!(!format!("{output:?}").contains("ARBITRARY_GATEWAY_THREAD_PANIC"));
    }
    use std::task::{Context, Poll};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    async fn spawn_gateway(state: GatewayState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn public_routes_allow_only_the_two_desktop_origins() {
        let base = spawn_gateway(GatewayState::new("node-test")).await;
        let client = Client::new();

        for origin in ["tauri://localhost", "http://127.0.0.1:1420"] {
            for path in ["/loxa/status", "/v1/models"] {
                let response = client
                    .get(format!("{base}{path}"))
                    .header(header::ORIGIN, origin)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    response
                        .headers()
                        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                        .unwrap(),
                    origin
                );
                assert_eq!(response.headers().get(header::VARY).unwrap(), "Origin");
            }

            let response = client
                .post(format!("{base}/v1/chat/completions"))
                .header(header::ORIGIN, origin)
                .json(&json!({"model": "loxa", "messages": []}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .unwrap(),
                origin
            );
            assert_eq!(response.headers().get(header::VARY).unwrap(), "Origin");
        }

        for path in ["/loxa/status", "/v1/models", "/v1/chat/completions"] {
            let request = if path == "/v1/chat/completions" {
                client.post(format!("{base}{path}")).json(&json!({
                    "model": "loxa",
                    "messages": []
                }))
            } else {
                client.get(format!("{base}{path}"))
            };
            let response = request
                .header(header::ORIGIN, "https://attacker.invalid")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            assert!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .is_none(),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn public_preflights_are_route_specific_and_fail_closed() {
        let base = spawn_gateway(GatewayState::new("node-test")).await;
        let client = Client::new();

        for (path, method, allowed_methods, allowed_headers) in [
            ("/loxa/status", Method::GET, "GET, OPTIONS", None),
            ("/v1/models", Method::GET, "GET, OPTIONS", None),
            (
                "/v1/chat/completions",
                Method::POST,
                "POST, OPTIONS",
                Some("content-type"),
            ),
        ] {
            let response = client
                .request(Method::OPTIONS, format!("{base}{path}"))
                .header(header::ORIGIN, "tauri://localhost")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, method.as_str())
                .header(
                    header::ACCESS_CONTROL_REQUEST_HEADERS,
                    if method == Method::POST {
                        "content-type"
                    } else {
                        ""
                    },
                )
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT, "{path}");
            assert_eq!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .unwrap(),
                "tauri://localhost"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                    .unwrap(),
                allowed_methods
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
                    .and_then(|value| value.to_str().ok()),
                allowed_headers
            );
        }

        for (path, origin, method, headers) in [
            (
                "/v1/chat/completions",
                "https://attacker.invalid",
                "GET",
                "",
            ),
            ("/v1/chat/completions", "tauri://localhost", "DELETE", ""),
            (
                "/v1/chat/completions",
                "tauri://localhost",
                "POST",
                "authorization",
            ),
            ("/loxa/status", "tauri://localhost", "GET", "content-type"),
        ] {
            let response = client
                .request(Method::OPTIONS, format!("{base}{path}"))
                .header(header::ORIGIN, origin)
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, method)
                .header(header::ACCESS_CONTROL_REQUEST_HEADERS, headers)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert!(response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none());
        }
    }

    #[tokio::test]
    async fn public_routes_preserve_originless_api_clients() {
        let base = spawn_gateway(GatewayState::new("node-test")).await;
        let response = Client::new()
            .get(format!("{base}/loxa/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());
        assert_eq!(response.headers().get(header::VARY).unwrap(), "Origin");
    }

    #[tokio::test]
    async fn models_and_status_are_stable() {
        let state = GatewayState::new("node-test");
        let base = spawn_gateway(state.clone()).await;
        let client = Client::new();

        let models: Value = client
            .get(format!("{base}/v1/models"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(models["object"], "list");
        assert_eq!(models["data"].as_array().unwrap().len(), 1);
        assert_eq!(models["data"][0]["id"], "loxa");
        assert_eq!(models["data"][0]["object"], "model");

        let status: Value = client
            .get(format!("{base}/loxa/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["node_id"], "node-test");
        assert_eq!(status["health"], "unavailable");
        assert_eq!(status["model"], "loxa");

        state.publish(EngineTarget {
            base_url: "http://127.0.0.1:31001".into(),
            backend_alias: "loxa-run-g0".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let status: Value = client
            .get(format!("{base}/loxa/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["health"], "ready");
        assert_eq!(status["model"], "loxa");
        assert_eq!(status["engine"]["name"], "llama.cpp");
        assert_eq!(status["engine"]["version"], "b9999");
        assert_eq!(status["runtime_model"], "gemma-3-4b-it-q4");
        assert_eq!(status["profile"], "default");
    }

    async fn fake_chat(
        State(seen): State<Arc<Mutex<Option<Value>>>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        *seen.lock().unwrap() = Some(request);
        Json(json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "loxa-node-test-g0",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}}]
        }))
    }

    async fn spawn_fake_engine(seen: Arc<Mutex<Option<Value>>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/chat/completions", post(fake_chat))
            .with_state(seen);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn non_stream_proxy_rewrites_and_normalizes_alias() {
        let seen = Arc::new(Mutex::new(None));
        let engine = spawn_fake_engine(seen.clone()).await;
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: engine,
            backend_alias: "loxa-node-test-g0".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;

        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "messages": [{"role": "user", "content": "hi"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let text = response.text().await.unwrap();
        assert!(!text.contains("loxa-node-test-g0"));
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["model"], "loxa");
        assert_eq!(
            seen.lock().unwrap().as_ref().unwrap()["model"],
            "loxa-node-test-g0"
        );
    }

    #[tokio::test]
    async fn internal_generation_service_holds_one_target_snapshot_and_normalizes_output() {
        let seen = Arc::new(Mutex::new(None));
        let engine = spawn_fake_engine(seen.clone()).await;
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: engine,
            backend_alias: "loxa-node-test-g0".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });

        let prepared = state
            .prepare_generation(json!({
                "model": "loxa",
                "messages": [{"role": "user", "content": "hi"}]
            }))
            .unwrap();
        assert_eq!(
            prepared.provenance(),
            &GenerationProvenance {
                engine: "llama.cpp".into(),
                engine_version: "b9999".into(),
                model_id: "gemma-3-4b-it-q4".into(),
                profile: "default".into(),
            }
        );

        state.publish(EngineTarget {
            base_url: "http://127.0.0.1:1".into(),
            backend_alias: "replacement".into(),
            engine: "replacement-engine".into(),
            engine_version: "replacement-version".into(),
            model_id: "replacement-model".into(),
            profile: "replacement-profile".into(),
        });

        let GenerationOutput::Json { status, body } = prepared.execute().await.unwrap() else {
            panic!("non-stream request returned a stream");
        };
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["model"], "loxa");
        assert_eq!(
            seen.lock().unwrap().as_ref().unwrap()["model"],
            "loxa-node-test-g0"
        );
    }

    async fn fake_default_model_chat(
        State(seen): State<Arc<Mutex<Option<Value>>>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        *seen.lock().unwrap() = Some(request);
        Json(json!({
            "id": "chatcmpl-mlx-test",
            "object": "chat.completion",
            "model": "default_model",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello from MLX"}}]
        }))
    }

    #[tokio::test]
    async fn non_stream_default_model_alias_round_trips_as_loxa() {
        let seen = Arc::new(Mutex::new(None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/chat/completions", post(fake_default_model_chat))
            .with_state(seen.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "default_model".into(),
            engine: "mlx-lm".into(),
            engine_version: "0.31.3".into(),
            model_id: "/models/mlx-test".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;

        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "messages": [{"role": "user", "content": "hi"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let text = response.text().await.unwrap();
        assert!(!text.contains("default_model"));
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["model"], "loxa");
        assert_eq!(body["choices"][0]["message"]["content"], "hello from MLX");
        assert_eq!(
            seen.lock().unwrap().as_ref().unwrap()["model"],
            "default_model"
        );
    }

    #[tokio::test]
    async fn non_stream_errors_are_openai_shaped() {
        let state = GatewayState::new("node-test");
        let base = spawn_gateway(state).await;
        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["error"]["code"], "engine_unavailable");
        assert!(json["error"]["message"].is_string());
        assert!(json["error"]["type"].is_string());
        assert!(json["error"].get("param").is_some());
    }

    #[tokio::test]
    async fn invalid_model_and_transport_errors_are_openai_shaped() {
        let state = GatewayState::new("node-test");
        let base = spawn_gateway(state.clone()).await;
        let invalid = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "backend", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let invalid: Value = invalid.json().await.unwrap();
        assert_eq!(invalid["error"]["code"], "invalid_model");
        assert_eq!(invalid["error"]["param"], "model");

        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_address = dead.local_addr().unwrap();
        drop(dead);
        state.publish(EngineTarget {
            base_url: format!("http://{dead_address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let transport = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(transport.status(), StatusCode::SERVICE_UNAVAILABLE);
        let transport: Value = transport.json().await.unwrap();
        assert_eq!(transport["error"]["code"], "upstream_error");
        assert!(transport["error"]["message"].is_string());
        assert!(transport["error"]["type"].is_string());
        assert!(transport["error"].get("param").is_some());
    }

    async fn fake_stream_chat() -> Response {
        let chunks = vec![
            Ok::<_, std::convert::Infallible>("data: {\"model\":\"loxa-node-"),
            Ok("test-g0\",\"choices\":[]}\n\n"),
            Ok("data: [DONE]\n\n"),
        ];
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap()
    }

    #[tokio::test]
    async fn internal_generation_service_reuses_the_bounded_normalized_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(fake_stream_chat)),
            )
            .await
            .unwrap()
        });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "loxa-node-test-g0".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });

        let generation = state
            .prepare_generation(json!({
                "model": "loxa",
                "stream": true,
                "messages": []
            }))
            .unwrap();
        let GenerationOutput::Stream(mut stream) = generation.execute().await.unwrap() else {
            panic!("stream request returned JSON");
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(
            bytes,
            b"data: {\"choices\":[],\"model\":\"loxa\"}\n\ndata: [DONE]\n\n"
        );
    }

    async fn fake_default_model_stream_chat(
        State(seen): State<Arc<Mutex<Option<Value>>>>,
        Json(request): Json<Value>,
    ) -> Response {
        *seen.lock().unwrap() = Some(request);
        let chunks = vec![
            Ok::<_, std::convert::Infallible>(": mlx keepalive\n\n"),
            Ok("data: {\"model\":\"default_"),
            Ok("model\",\"choices\":[]}\n\n"),
            Ok("data: [DONE]\n\n"),
        ];
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap()
    }

    async fn fake_gated_stream_chat(State(gate): State<Arc<Notify>>) -> Response {
        let chunks = stream::unfold((0, gate), |(step, gate)| async move {
            match step {
                0 => Some((
                    Ok::<_, std::convert::Infallible>(
                        "data: {\"model\":\"backend\",\"choices\":[]}\n\n",
                    ),
                    (1, gate),
                )),
                1 => {
                    gate.notified().await;
                    Some((Ok("data: [DONE]\n\n"), (2, gate)))
                }
                _ => None,
            }
        });
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(chunks))
            .unwrap()
    }

    #[tokio::test]
    async fn allowed_origin_receives_headers_and_first_event_before_upstream_completion() {
        let gate = Arc::new(Notify::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/chat/completions", post(fake_gated_stream_chat))
            .with_state(gate.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;

        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .header(header::ORIGIN, "tauri://localhost")
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "tauri://localhost"
        );

        let mut body = response.bytes_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
            .await
            .expect("first event was buffered behind upstream completion")
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            Bytes::from_static(b"data: {\"choices\":[],\"model\":\"loxa\"}\n\n")
        );

        let next = body.next();
        tokio::pin!(next);
        tokio::select! {
            biased;
            event = &mut next => panic!("stream completed before gate release: {event:?}"),
            _ = tokio::task::yield_now() => {}
        }
        gate.notify_one();
        let final_event = tokio::time::timeout(std::time::Duration::from_secs(1), &mut next)
            .await
            .expect("final event did not arrive after gate release")
            .unwrap()
            .unwrap();
        assert_eq!(final_event, Bytes::from_static(b"data: [DONE]\n\n"));
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn streaming_default_model_alias_round_trips_as_loxa_with_one_done() {
        let seen = Arc::new(Mutex::new(None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/chat/completions", post(fake_default_model_stream_chat))
            .with_state(seen.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "default_model".into(),
            engine: "mlx-lm".into(),
            engine_version: "0.31.3".into(),
            model_id: "/models/mlx-test".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;

        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .header(header::ORIGIN, "tauri://localhost")
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "tauri://localhost"
        );
        let text = response.text().await.unwrap();
        assert_eq!(
            text,
            ": mlx keepalive\n\ndata: {\"choices\":[],\"model\":\"loxa\"}\n\ndata: [DONE]\n\n"
        );
        assert_eq!(text.matches("data: [DONE]").count(), 1);
        assert!(!text.contains("default_model"));
        let upstream = seen.lock().unwrap();
        assert_eq!(upstream.as_ref().unwrap()["model"], "default_model");
        assert_eq!(upstream.as_ref().unwrap()["stream"], true);
    }

    #[tokio::test]
    async fn stream_is_incremental_and_normalizes_split_alias() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(fake_stream_chat)),
            )
            .await
            .unwrap()
        });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "loxa-node-test-g0".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;
        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let text = response.text().await.unwrap();
        assert_eq!(
            text,
            "data: {\"choices\":[],\"model\":\"loxa\"}\n\ndata: [DONE]\n\n"
        );
        assert!(!text.contains("loxa-node-test-g0"));
    }

    #[tokio::test]
    async fn first_sse_event_is_emitted_before_delayed_final_event() {
        let upstream = stream::unfold(0, |step| async move {
            match step {
                0 => Some((
                    Ok::<_, reqwest::Error>(Bytes::from_static(
                        b"data: {\"model\":\"backend\"}\n\n",
                    )),
                    1,
                )),
                1 => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    Some((
                        Ok::<_, reqwest::Error>(Bytes::from_static(b"data: [DONE]\n\n")),
                        2,
                    ))
                }
                _ => None,
            }
        });
        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let mut output = Box::pin(normalize_sse(upstream, "backend".into(), cancellation));

        let first = tokio::time::timeout(std::time::Duration::from_millis(50), output.next())
            .await
            .expect("first event was buffered behind final event")
            .unwrap()
            .unwrap();
        assert_eq!(first, "data: {\"model\":\"loxa\"}\n\n");
        assert_eq!(output.next().await.unwrap().unwrap(), "data: [DONE]\n\n");
    }

    #[test]
    fn target_snapshots_and_mixed_sse_boundaries_are_stable() {
        let state = GatewayState::new("node-test");
        let target = |port| EngineTarget {
            base_url: format!("http://127.0.0.1:{port}"),
            backend_alias: format!("loxa-node-g{port}"),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        };
        state.publish(target(1));
        let first_request = state.snapshot().unwrap();
        state.publish(target(2));
        assert_eq!(first_request.base_url, "http://127.0.0.1:1");
        assert_eq!(state.snapshot().unwrap().base_url, "http://127.0.0.1:2");
        assert_eq!(
            next_sse_boundary(b"data: first\n\ndata: second\r\n\r\n"),
            Some((11, 2))
        );
    }

    #[test]
    fn generation_preparation_distinguishes_unloaded_from_shutdown() {
        let state = GatewayState::new("node-test");
        assert_eq!(
            state.prepare_generation(json!({"model": "loxa"})).err(),
            Some(GenerationError::EngineUnavailable)
        );

        state.publish(EngineTarget {
            base_url: "http://127.0.0.1:1".into(),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        state.cancel_requests();
        assert_eq!(
            state.prepare_generation(json!({"model": "loxa"})).err(),
            Some(GenerationError::GatewayShuttingDown)
        );
    }

    #[tokio::test]
    async fn stream_preserves_non_json_data_comments_done_and_line_endings() {
        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let input = b": keep  two spaces\r\ndata:plain text  \r\n\r\ndata:[DONE]\n\n";
        let mut output = Box::pin(normalize_sse(
            stream::iter([Ok::<_, reqwest::Error>(Bytes::from_static(input))]),
            "backend".into(),
            cancellation,
        ));
        let mut actual = Vec::new();
        while let Some(chunk) = output.next().await {
            actual.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(actual, input);
    }

    #[tokio::test]
    async fn reusable_stream_closes_transport_errors_without_retaining_private_material() {
        let raw = Client::new()
            .get("http://[::1/private-token-and-prompt")
            .build()
            .unwrap_err();
        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let mut output = Box::pin(normalize_sse(
            stream::iter([Err(raw)]),
            "private-backend-alias".into(),
            cancellation,
        ));

        let error = output.next().await.unwrap().unwrap_err();
        assert_eq!(error, GenerationStreamError::Upstream);
        let rendered = format!("{error:?} {error}");
        for secret in [
            "private-token-and-prompt",
            "private-backend-alias",
            "http://",
            "[::1",
        ] {
            assert!(!rendered.contains(secret), "leaked {secret}: {rendered}");
        }
    }

    #[tokio::test]
    async fn completed_oversized_sse_event_is_rejected() {
        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let mut event = vec![b'x'; MAX_SSE_EVENT_BYTES + 1];
        event.extend_from_slice(b"\n\n");
        let mut output = Box::pin(normalize_sse(
            stream::iter([Ok::<_, reqwest::Error>(Bytes::from(event))]),
            "backend".into(),
            cancellation,
        ));

        assert_eq!(
            output.next().await.unwrap().unwrap_err(),
            GenerationStreamError::EventTooLarge
        );
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn malformed_json_is_openai_shaped() {
        let base = spawn_gateway(GatewayState::new("node-test")).await;
        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], "invalid_request");
    }

    async fn never_ending_stream() -> Response {
        let first = stream::once(async {
            Ok::<_, std::convert::Infallible>("data: {\"model\":\"backend\"}\n\n")
        });
        let pending = stream::pending::<Result<&'static str, std::convert::Infallible>>();
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(first.chain(pending)))
            .unwrap()
    }

    async fn stalled_json() -> Response {
        let first = stream::once(async { Ok::<_, std::convert::Infallible>("{") });
        let pending = stream::pending::<Result<&'static str, std::convert::Infallible>>();
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from_stream(first.chain(pending)))
            .unwrap()
    }

    #[tokio::test]
    async fn shutdown_cancels_non_stream_body_parsing_after_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(stalled_json)),
            )
            .await
            .unwrap()
        });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let server = GatewayServer::start(0, state).unwrap();
        let port = server.port();
        let request = tokio::spawn(async move {
            Client::new()
                .post(format!("http://127.0.0.1:{port}/v1/chat/completions"))
                .json(&json!({"model": "loxa", "messages": []}))
                .send()
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::task::spawn_blocking(move || server.shutdown()),
        )
        .await
        .expect("gateway shutdown waited on stalled JSON body")
        .unwrap()
        .unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), request)
            .await
            .expect("cancelled client request remained pending")
            .expect("client request task panicked");
    }

    #[tokio::test]
    async fn stream_subscribed_after_shutdown_observes_cancellation() {
        let state = GatewayState::new("node-test");
        state.cancel_requests();
        let mut output = Box::pin(normalize_sse(
            stream::pending::<Result<Bytes, reqwest::Error>>(),
            "backend".into(),
            state.cancellation.subscribe(),
        ));

        let next = tokio::time::timeout(std::time::Duration::from_millis(100), output.next())
            .await
            .expect("late subscriber missed cancellation");
        assert!(next.is_none());
    }

    async fn aliased_upstream_error() -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"message": "model backend is unavailable", "type": "invalid_request_error", "param": "model", "code": "bad_model"}})),
        )
            .into_response()
    }

    #[tokio::test]
    async fn upstream_error_prose_does_not_leak_backend_alias() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(aliased_upstream_error)),
            )
            .await
            .unwrap()
        });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;
        let response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let text = response.text().await.unwrap();
        assert!(!text.contains("backend"));
        assert!(text.contains("model loxa is unavailable"));
    }

    #[tokio::test]
    async fn shutdown_cancels_an_active_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(never_ending_stream)),
            )
            .await
            .unwrap()
        });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let server = GatewayServer::start(0, state).unwrap();
        let mut response = Client::new()
            .post(format!(
                "http://127.0.0.1:{}/v1/chat/completions",
                server.port()
            ))
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert!(response.chunk().await.unwrap().is_some());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::task::spawn_blocking(move || server.shutdown()),
        )
        .await
        .expect("gateway shutdown waited on active SSE")
        .unwrap()
        .unwrap();
    }

    struct DropAwareStream {
        yielded: bool,
        dropped: Arc<AtomicBool>,
    }

    impl futures_util::Stream for DropAwareStream {
        type Item = Result<axum::body::Bytes, reqwest::Error>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.yielded {
                Poll::Pending
            } else {
                self.yielded = true;
                Poll::Ready(Some(Ok(axum::body::Bytes::from_static(
                    b"data: {\"model\":\"backend\"}\n\n",
                ))))
            }
        }
    }

    impl Drop for DropAwareStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct HttpDropAwareStream {
        yielded: bool,
        dropped: Arc<AtomicBool>,
    }

    impl futures_util::Stream for HttpDropAwareStream {
        type Item = Result<&'static str, std::convert::Infallible>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.yielded {
                Poll::Pending
            } else {
                self.yielded = true;
                Poll::Ready(Some(Ok("data: {\"model\":\"backend\"}\n\n")))
            }
        }
    }

    impl Drop for HttpDropAwareStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    async fn http_drop_stream(State(dropped): State<Arc<AtomicBool>>) -> Response {
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(HttpDropAwareStream {
                yielded: false,
                dropped,
            }))
            .unwrap()
    }

    #[tokio::test]
    async fn real_downstream_http_drop_cancels_fake_upstream() {
        let dropped = Arc::new(AtomicBool::new(false));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/v1/chat/completions", post(http_drop_stream))
            .with_state(dropped.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let state = GatewayState::new("node-test");
        state.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "llama.cpp".into(),
            engine_version: "b9999".into(),
            model_id: "gemma-3-4b-it-q4".into(),
            profile: "default".into(),
        });
        let base = spawn_gateway(state).await;
        let mut response = Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert!(response.chunk().await.unwrap().is_some());
        drop(response);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping downstream HTTP body did not cancel upstream");
    }

    #[tokio::test]
    async fn downstream_drop_cancels_upstream_and_oversized_events_fail() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let mut output = Box::pin(normalize_sse(
            DropAwareStream {
                yielded: false,
                dropped: dropped.clone(),
            },
            "backend".into(),
            cancellation,
        ));
        assert!(output.next().await.unwrap().is_ok());
        drop(output);
        assert!(dropped.load(Ordering::SeqCst));

        let (_cancellation_tx, cancellation) = tokio::sync::watch::channel(false);
        let oversized = axum::body::Bytes::from(vec![b'x'; MAX_SSE_EVENT_BYTES + 1]);
        let mut output = Box::pin(normalize_sse(
            stream::iter([Ok::<_, reqwest::Error>(oversized)]),
            "backend".into(),
            cancellation,
        ));
        assert_eq!(
            output.next().await.unwrap().unwrap_err(),
            GenerationStreamError::EventTooLarge
        );
    }
}
