use axum::{
    body::{Body, Bytes},
    extract::{rejection::JsonRejection, State},
    http::{header, StatusCode},
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

async fn models() -> Json<ModelList> {
    Json(ModelList {
        object: "list",
        data: [Model {
            id: MODEL_ALIAS,
            object: "model",
            owned_by: "loxa",
        }],
    })
}

async fn status(State(state): State<GatewayState>) -> Json<Value> {
    let target = state.snapshot();
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
    payload: Result<Json<Value>, JsonRejection>,
) -> Response {
    let Json(mut request) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return openai_error(
                StatusCode::BAD_REQUEST,
                "request body must be valid JSON",
                "invalid_request",
            )
        }
    };
    if request.get("model").and_then(Value::as_str) != Some(MODEL_ALIAS) {
        return openai_error(
            StatusCode::BAD_REQUEST,
            "model must be the stable alias 'loxa'",
            "invalid_model",
        );
    }
    let Some(target) = state.snapshot() else {
        return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the managed engine is temporarily unavailable",
            "engine_unavailable",
        );
    };
    request["model"] = Value::String(target.backend_alias.clone());
    let streaming = request.get("stream").and_then(Value::as_bool) == Some(true);
    let mut cancellation = state.cancellation.subscribe();
    if *cancellation.borrow() {
        return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the gateway is shutting down",
            "engine_unavailable",
        );
    }
    let send = state
        .client
        .post(format!("{}/v1/chat/completions", target.base_url))
        .json(&request)
        .send();
    let upstream = match tokio::select! {
        response = send => response,
        _ = cancellation.changed() => return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the gateway is shutting down",
            "engine_unavailable",
        ),
    } {
        Ok(response) => response,
        Err(_) => {
            return openai_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "the managed engine could not accept the request",
                "upstream_error",
            )
        }
    };
    if streaming {
        if !upstream.status().is_success() {
            return openai_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "the managed engine rejected the streaming request",
                "upstream_error",
            );
        }
        let stream = normalize_sse(
            upstream.bytes_stream(),
            target.backend_alias.clone(),
            state.cancellation.subscribe(),
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .expect("valid streaming response");
    }
    let status = upstream.status();
    let body = upstream.json::<Value>();
    let mut body = match tokio::select! {
        body = body => body,
        _ = cancellation.changed() => return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the gateway is shutting down",
            "engine_unavailable",
        ),
    } {
        Ok(body) => body,
        Err(_) => {
            return openai_error(
                StatusCode::BAD_GATEWAY,
                "the managed engine returned an invalid response",
                "upstream_error",
            )
        }
    };
    normalize_aliases(&mut body, &target.backend_alias);
    if !status.is_success() {
        normalize_embedded_aliases(&mut body, &target.backend_alias);
    }
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    (status, Json(body)).into_response()
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
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send {
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
                        return Some((
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "upstream SSE event exceeds gateway limit",
                            )),
                            state,
                        ));
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
                    return Some((
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "upstream SSE event exceeds gateway limit",
                        )),
                        state,
                    ));
                }
                let next = tokio::select! {
                    next = state.upstream.next() => next,
                    _ = state.cancellation.changed() => return None,
                };
                match next {
                    Some(Ok(chunk)) => state.buffer.extend_from_slice(&chunk),
                    Some(Err(error)) => {
                        state.finished = true;
                        return Some((Err(io::Error::other(error)), state));
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
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat))
        .route("/loxa/status", get(status))
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
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let (shutdown, receiver) = tokio::sync::oneshot::channel();
        let server_state = state.clone();
        let thread = thread::Builder::new()
            .name("loxa-gateway".into())
            .spawn(move || {
                let runtime = tokio::runtime::Runtime::new().map_err(io::Error::other)?;
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)?;
                    axum::serve(listener, router(server_state))
                        .with_graceful_shutdown(async move {
                            let _ = receiver.await;
                        })
                        .await
                        .map_err(io::Error::other)
                })
            })?;
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
        self.state.cancel_requests();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match self.thread.take().expect("gateway thread present").join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("gateway thread panicked")),
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
        MAX_SSE_EVENT_BYTES,
    };
    use axum::http::StatusCode;
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
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::net::TcpListener;

    async fn spawn_gateway(state: GatewayState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
        format!("http://{address}")
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
            .json(&json!({"model": "loxa", "stream": true, "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
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
            output.next().await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
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
            output.next().await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
