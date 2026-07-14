use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::error::Error;
use std::fmt;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_TOKENS: u32 = 256;
const DEFAULT_TEMPERATURE: f32 = 0.0;
pub const MAX_NDJSON_LINE_BYTES: usize = 1024 * 1024;
pub const MAX_NDJSON_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, PartialEq, Eq)]
pub enum EngineProtocol {
    OpenAi { backend_alias: String },
    SwiftMlx { engine_token: String },
}

impl fmt::Debug for EngineProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenAi { backend_alias } => formatter
                .debug_struct("OpenAi")
                .field("backend_alias", backend_alias)
                .finish(),
            Self::SwiftMlx { .. } => formatter
                .debug_struct("SwiftMlx")
                .field("engine_token", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct SwiftGenerateRequest {
    pub request_id: String,
    pub prompt: String,
    pub temperature: f32,
    pub max_tokens: u32,
}

#[derive(Clone)]
pub struct SwiftMlxClient {
    base_url: String,
    engine_token: String,
    client: reqwest::Client,
}

impl fmt::Debug for SwiftMlxClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SwiftMlxClient")
            .field("base_url", &"[REDACTED]")
            .field("engine_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl SwiftMlxClient {
    pub fn new(
        base_url: impl Into<String>,
        engine_token: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        let base_url = base_url.into();
        let parsed = reqwest::Url::parse(&base_url).map_err(|_| ProtocolError::InvalidEndpoint)?;
        let loopback = parsed
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some_and(|address| address.is_loopback());
        if parsed.scheme() != "http"
            || !loopback
            || parsed.port().is_none()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(ProtocolError::InvalidEndpoint);
        }

        let engine_token = engine_token.into();
        let authorization = format!("Bearer {engine_token}");
        if engine_token.is_empty()
            || reqwest::header::HeaderValue::from_str(&authorization).is_err()
        {
            return Err(ProtocolError::InvalidToken);
        }

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ProtocolError::Transport)?;

        Ok(Self {
            base_url: parsed.as_str().trim_end_matches('/').to_string(),
            engine_token,
            client,
        })
    }

    pub async fn health(&self) -> Result<(), ProtocolError> {
        let response = self
            .client
            .get(format!("{}/health", self.base_url))
            .bearer_auth(&self.engine_token)
            .send()
            .await
            .map_err(|_| ProtocolError::Transport)?;
        ensure_success(response.status())
    }

    pub async fn ready(&self) -> Result<bool, ProtocolError> {
        let response = self
            .client
            .get(format!("{}/ready", self.base_url))
            .bearer_auth(&self.engine_token)
            .send()
            .await
            .map_err(|_| ProtocolError::Transport)?;
        ensure_success(response.status())?;
        response
            .json::<Value>()
            .await
            .map_err(|_| ProtocolError::InvalidResponse)?
            .get("ready")
            .and_then(Value::as_bool)
            .ok_or(ProtocolError::InvalidResponse)
    }

    pub async fn generate(
        &self,
        request: &SwiftGenerateRequest,
    ) -> Result<reqwest::Response, ProtocolError> {
        let response = self
            .client
            .post(format!("{}/generate", self.base_url))
            .bearer_auth(&self.engine_token)
            .json(request)
            .send()
            .await
            .map_err(|_| ProtocolError::Transport)?;
        ensure_success(response.status())?;
        let is_ndjson = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-ndjson"));
        if !is_ndjson {
            return Err(ProtocolError::InvalidResponse);
        }
        Ok(response)
    }

    pub async fn cancel(&self, request_id: &str) -> Result<(), ProtocolError> {
        let response = self
            .client
            .post(format!("{}/cancel", self.base_url))
            .bearer_auth(&self.engine_token)
            .json(&json!({"request_id": request_id}))
            .send()
            .await
            .map_err(|_| ProtocolError::Transport)?;
        ensure_success(response.status())
    }
}

fn ensure_success(status: reqwest::StatusCode) -> Result<(), ProtocolError> {
    if status.is_success() {
        Ok(())
    } else if status == reqwest::StatusCode::UNAUTHORIZED
        || status == reqwest::StatusCode::FORBIDDEN
    {
        Err(ProtocolError::Authentication)
    } else {
        Err(ProtocolError::Rejected)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SwiftMlxUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwiftMlxFinishReason {
    Stop,
    Length,
    Cancelled,
    Error,
}

impl SwiftMlxFinishReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SwiftMlxEvent {
    Started,
    Token(String),
    Usage(SwiftMlxUsage),
    Finished(SwiftMlxFinishReason),
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwiftMlxOutput {
    pub content: String,
    pub usage: Option<SwiftMlxUsage>,
    pub finish_reason: SwiftMlxFinishReason,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WireEvent {
    Started {
        request_id: String,
    },
    Token {
        text: String,
    },
    Usage {
        prompt_tokens: u64,
        completion_tokens: u64,
    },
    Finished {
        reason: SwiftMlxFinishReason,
    },
    Error {
        message: String,
    },
}

enum TerminalEvent {
    Finished(SwiftMlxFinishReason),
    Error,
}

pub struct NdjsonDecoder {
    expected_request_id: String,
    buffer: Vec<u8>,
    total_bytes: usize,
    event_count: usize,
    started: bool,
    content: String,
    usage: Option<SwiftMlxUsage>,
    terminal: Option<TerminalEvent>,
}

impl NdjsonDecoder {
    pub fn new(expected_request_id: impl Into<String>) -> Self {
        Self {
            expected_request_id: expected_request_id.into(),
            buffer: Vec::new(),
            total_bytes: 0,
            event_count: 0,
            started: false,
            content: String::new(),
            usage: None,
            terminal: None,
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SwiftMlxEvent>, ProtocolError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(chunk.len())
            .filter(|total| *total <= MAX_NDJSON_RESPONSE_BYTES)
            .ok_or(ProtocolError::ResponseTooLarge)?;
        if self.terminal.is_some() && !chunk.is_empty() {
            return Err(ProtocolError::ProtocolViolation);
        }
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();

        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            if newline > MAX_NDJSON_LINE_BYTES {
                return Err(ProtocolError::LineTooLarge);
            }
            let mut line = self.buffer.drain(..=newline).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            events.push(self.parse_line(&line)?);
        }
        if self.buffer.len() > MAX_NDJSON_LINE_BYTES {
            return Err(ProtocolError::LineTooLarge);
        }
        if self.terminal.is_some() && !self.buffer.is_empty() {
            return Err(ProtocolError::ProtocolViolation);
        }

        Ok(events)
    }

    pub fn finish(mut self) -> Result<SwiftMlxOutput, ProtocolError> {
        if !self.buffer.is_empty() {
            if self.buffer.len() > MAX_NDJSON_LINE_BYTES {
                return Err(ProtocolError::LineTooLarge);
            }
            let line = std::mem::take(&mut self.buffer);
            self.parse_line(&line)?;
        }
        if self.event_count == 0 {
            return Err(ProtocolError::EmptyResponse);
        }
        if !self.started {
            return Err(ProtocolError::ProtocolViolation);
        }
        let terminal = self.terminal.ok_or(ProtocolError::ProtocolViolation)?;
        let finish_reason = match terminal {
            TerminalEvent::Finished(reason) => reason,
            TerminalEvent::Error => return Err(ProtocolError::EngineFailure),
        };
        if self.content.is_empty() {
            return Err(ProtocolError::EmptyResponse);
        }

        Ok(SwiftMlxOutput {
            content: self.content,
            usage: self.usage,
            finish_reason,
        })
    }

    fn parse_line(&mut self, line: &[u8]) -> Result<SwiftMlxEvent, ProtocolError> {
        if line.is_empty() || self.terminal.is_some() {
            return Err(ProtocolError::ProtocolViolation);
        }
        let event = serde_json::from_slice::<WireEvent>(line)
            .map_err(|_| ProtocolError::ProtocolViolation)?;
        let event = match event {
            WireEvent::Started { request_id } => {
                if self.started || self.event_count != 0 || request_id != self.expected_request_id {
                    return Err(ProtocolError::ProtocolViolation);
                }
                self.started = true;
                SwiftMlxEvent::Started
            }
            WireEvent::Token { text } => {
                if !self.started || self.usage.is_some() || text.is_empty() {
                    return Err(ProtocolError::ProtocolViolation);
                }
                self.content.push_str(&text);
                SwiftMlxEvent::Token(text)
            }
            WireEvent::Usage {
                prompt_tokens,
                completion_tokens,
            } => {
                if !self.started || self.usage.is_some() {
                    return Err(ProtocolError::ProtocolViolation);
                }
                let usage = SwiftMlxUsage {
                    prompt_tokens,
                    completion_tokens,
                };
                self.usage = Some(usage);
                SwiftMlxEvent::Usage(usage)
            }
            WireEvent::Finished { reason } => {
                if !self.started {
                    return Err(ProtocolError::ProtocolViolation);
                }
                self.terminal = Some(TerminalEvent::Finished(reason));
                SwiftMlxEvent::Finished(reason)
            }
            WireEvent::Error { message } => {
                if !self.started || message.is_empty() {
                    return Err(ProtocolError::ProtocolViolation);
                }
                self.terminal = Some(TerminalEvent::Error);
                SwiftMlxEvent::Error
            }
        };
        self.event_count += 1;
        Ok(event)
    }
}

pub async fn collect_generation(
    response: reqwest::Response,
    request_id: &str,
) -> Result<SwiftMlxOutput, ProtocolError> {
    let mut decoder = NdjsonDecoder::new(request_id);
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| ProtocolError::Transport)?;
        decoder.push(&chunk)?;
    }
    decoder.finish()
}

impl SwiftGenerateRequest {
    pub fn from_openai(request: &Value) -> Result<Self, ProtocolError> {
        let object = request.as_object().ok_or(ProtocolError::InvalidRequest)?;
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "model" | "messages" | "temperature" | "max_tokens" | "stream"
            )
        }) {
            return Err(ProtocolError::InvalidRequest);
        }
        let messages = object
            .get("messages")
            .and_then(Value::as_array)
            .ok_or(ProtocolError::InvalidRequest)?;
        let [message] = messages.as_slice() else {
            return Err(ProtocolError::InvalidRequest);
        };
        if message.get("role").and_then(Value::as_str) != Some("user") {
            return Err(ProtocolError::InvalidRequest);
        }
        let prompt = message
            .get("content")
            .and_then(Value::as_str)
            .ok_or(ProtocolError::InvalidRequest)?;
        let temperature = request
            .get("temperature")
            .map(|value| value.as_f64().ok_or(ProtocolError::InvalidRequest))
            .transpose()?
            .unwrap_or(DEFAULT_TEMPERATURE as f64);
        if !temperature.is_finite() || !(0.0..=f32::MAX as f64).contains(&temperature) {
            return Err(ProtocolError::InvalidRequest);
        }
        let max_tokens = request
            .get("max_tokens")
            .map(|value| value.as_u64().ok_or(ProtocolError::InvalidRequest))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_TOKENS as u64);
        let max_tokens = u32::try_from(max_tokens)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ProtocolError::InvalidRequest)?;

        Ok(Self {
            request_id: next_request_id(),
            prompt: prompt.to_string(),
            temperature: temperature as f32,
            max_tokens,
        })
    }
}

fn next_request_id() -> String {
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("req_{nanos:032x}_{sequence:016x}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidRequest,
    InvalidEndpoint,
    InvalidToken,
    Transport,
    Authentication,
    Rejected,
    InvalidResponse,
    ProtocolViolation,
    LineTooLarge,
    ResponseTooLarge,
    EmptyResponse,
    EngineFailure,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest => {
                formatter.write_str("the request is not supported by this engine")
            }
            Self::InvalidEndpoint => formatter.write_str("the private engine endpoint is invalid"),
            Self::InvalidToken => formatter.write_str("the private engine credential is invalid"),
            Self::Transport => {
                formatter.write_str("the managed engine could not accept the request")
            }
            Self::Authentication => {
                formatter.write_str("the managed engine rejected its private credential")
            }
            Self::Rejected => formatter.write_str("the managed engine rejected the request"),
            Self::InvalidResponse => {
                formatter.write_str("the managed engine returned an invalid response")
            }
            Self::ProtocolViolation => {
                formatter.write_str("the managed engine violated its private protocol")
            }
            Self::LineTooLarge => {
                formatter.write_str("the managed engine returned an oversized event")
            }
            Self::ResponseTooLarge => {
                formatter.write_str("the managed engine returned an oversized response")
            }
            Self::EmptyResponse => {
                formatter.write_str("the managed engine returned no generated text")
            }
            Self::EngineFailure => {
                formatter.write_str("the managed engine failed during generation")
            }
        }
    }
}

impl Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::{OriginalUri, State};
    use axum::http::{header, HeaderMap};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    #[derive(Clone, Default)]
    struct RecordedRequests(Arc<Mutex<Vec<(String, String, Value)>>>);

    async fn private_get(
        State(recorded): State<RecordedRequests>,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
    ) -> Json<Value> {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        recorded
            .0
            .lock()
            .unwrap()
            .push((uri.path().to_string(), authorization, Value::Null));
        Json(json!({"ready": uri.path() == "/ready"}))
    }

    async fn private_post(
        State(recorded): State<RecordedRequests>,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let request_id = body
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or("missing")
            .to_string();
        recorded
            .0
            .lock()
            .unwrap()
            .push((uri.path().to_string(), authorization, body));
        if uri.path() == "/generate" {
            Response::builder()
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .body(Body::from(format!(
                    "{{\"type\":\"started\",\"request_id\":{}}}\n\
                     {{\"type\":\"token\",\"text\":\"Hello\"}}\n\
                     {{\"type\":\"token\",\"text\":\" Swift\"}}\n\
                     {{\"type\":\"usage\",\"prompt_tokens\":1,\"completion_tokens\":2}}\n\
                     {{\"type\":\"finished\",\"reason\":\"stop\"}}\n",
                    serde_json::to_string(&request_id).unwrap()
                )))
                .unwrap()
        } else {
            Json(json!({"cancelled": true})).into_response()
        }
    }

    async fn spawn_private_sidecar(recorded: RecordedRequests) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/health", get(private_get))
            .route("/ready", get(private_get))
            .route("/generate", post(private_post))
            .route("/cancel", post(private_post))
            .with_state(recorded);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[test]
    fn openai_request_normalization_accepts_one_plain_user_message() {
        let request = SwiftGenerateRequest::from_openai(&json!({
            "model": "loxa",
            "messages": [{"role": "user", "content": "Hello Swift"}],
            "temperature": 0.25,
            "max_tokens": 17,
            "stream": true,
        }))
        .expect("supported request");

        assert!(request.request_id.starts_with("req_"));
        assert_ne!(request.request_id, "Hello Swift");
        assert_eq!(request.prompt, "Hello Swift");
        assert_eq!(request.temperature, 0.25);
        assert_eq!(request.max_tokens, 17);
    }

    #[test]
    fn openai_request_normalization_rejects_unsupported_controls() {
        let result = SwiftGenerateRequest::from_openai(&json!({
            "model": "loxa",
            "messages": [{"role": "user", "content": "Hello"}],
            "tools": [{"type": "function", "function": {"name": "lookup"}}],
        }));

        assert_eq!(result.err(), Some(ProtocolError::InvalidRequest));
    }

    #[tokio::test]
    async fn private_client_authenticates_every_route_and_sends_narrow_generate_body() {
        let recorded = RecordedRequests::default();
        let base_url = spawn_private_sidecar(recorded.clone()).await;
        let token = "private-engine-token";
        let client = SwiftMlxClient::new(base_url, token).expect("private client");
        let request = SwiftGenerateRequest::from_openai(&json!({
            "messages": [{"role": "user", "content": "Hello"}],
            "temperature": 0.5,
            "max_tokens": 8,
        }))
        .unwrap();

        client.health().await.unwrap();
        assert!(client.ready().await.unwrap());
        drop(client.generate(&request).await.unwrap());
        client.cancel(&request.request_id).await.unwrap();

        let seen = recorded.0.lock().unwrap();
        assert_eq!(
            seen.iter()
                .map(|(path, _, _)| path.as_str())
                .collect::<Vec<_>>(),
            ["/health", "/ready", "/generate", "/cancel"]
        );
        assert!(seen
            .iter()
            .all(|(_, authorization, _)| authorization == "Bearer private-engine-token"));
        assert_eq!(
            seen[2].2,
            json!({
                "request_id": request.request_id,
                "prompt": "Hello",
                "temperature": 0.5,
                "max_tokens": 8,
            })
        );
        assert_eq!(seen[3].2, json!({"request_id": request.request_id}));
    }

    #[tokio::test]
    async fn collect_generation_decodes_the_bounded_private_response() {
        let recorded = RecordedRequests::default();
        let base_url = spawn_private_sidecar(recorded).await;
        let client = SwiftMlxClient::new(base_url, "token").unwrap();
        let request = SwiftGenerateRequest::from_openai(&json!({
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 8,
        }))
        .unwrap();

        let response = client.generate(&request).await.unwrap();
        let output = collect_generation(response, &request.request_id)
            .await
            .unwrap();

        assert_eq!(output.content, "Hello Swift");
        assert_eq!(
            output.usage,
            Some(SwiftMlxUsage {
                prompt_tokens: 1,
                completion_tokens: 2,
            })
        );
        assert_eq!(output.finish_reason, SwiftMlxFinishReason::Stop);
    }

    #[test]
    fn private_protocol_debug_output_redacts_credentials_and_endpoint() {
        let token = "secret-engine-token";
        let endpoint = "http://127.0.0.1:32123";
        let protocol = EngineProtocol::SwiftMlx {
            engine_token: token.to_string(),
        };
        let client = SwiftMlxClient::new(endpoint, token).unwrap();

        let rendered = format!("{protocol:?} {client:?}");
        assert!(!rendered.contains(token));
        assert!(!rendered.contains(endpoint));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn ndjson_decoder_preserves_split_utf8_tokens_usage_and_terminal_order() {
        let mut decoder = NdjsonDecoder::new("req_expected");
        let mut events = decoder
            .push(b"{\"type\":\"started\",\"request_id\":\"req_expected\"}\n{\"type\":\"token\",\"text\":\"Hello ")
            .unwrap();
        let globe = "world 🌍\"}\n{\"type\":\"usage\",\"prompt_tokens\":12,\"completion_tokens\":2}\n{\"type\":\"finished\",\"reason\":\"stop\"}\n";
        let bytes = globe.as_bytes();
        let split = bytes.iter().position(|byte| *byte >= 0x80).unwrap() + 1;
        events.extend(decoder.push(&bytes[..split]).unwrap());
        events.extend(decoder.push(&bytes[split..]).unwrap());
        let output = decoder.finish().unwrap();

        assert_eq!(
            events,
            vec![
                SwiftMlxEvent::Started,
                SwiftMlxEvent::Token("Hello world 🌍".into()),
                SwiftMlxEvent::Usage(SwiftMlxUsage {
                    prompt_tokens: 12,
                    completion_tokens: 2,
                }),
                SwiftMlxEvent::Finished(SwiftMlxFinishReason::Stop),
            ]
        );
        assert_eq!(output.content, "Hello world 🌍");
        assert_eq!(
            output.usage,
            Some(SwiftMlxUsage {
                prompt_tokens: 12,
                completion_tokens: 2,
            })
        );
        assert_eq!(output.finish_reason, SwiftMlxFinishReason::Stop);
    }
}
