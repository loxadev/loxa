use crate::control::auth::ControlToken;
use crate::control::contracts::{
    ControlErrorBody, ModelRequest, NodeIdentityProofResponse, OperationAccepted, OperationStatus,
    OperationView, CONTROL_PROTOCOL_VERSION,
};
use crate::model_inventory::VerifiedRecipeInventoryEntry;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

const MAX_PROOF_BYTES: usize = 16 * 1024;
const MAX_CONTROL_BYTES: usize = 1024 * 1024;
const MAX_SSE_LINE_BYTES: usize = 2 * 1024 * 1024 + 1024;
const STREAM_READ_POLL: Duration = Duration::from_millis(50);

const CHATS_PATH: &str = "/loxa/v1/chats";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChatView {
    pub id: String,
    pub title: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChatPageView {
    pub chats: Vec<ChatView>,
    pub next_before: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct TurnProvenanceView {
    pub model_alias: String,
    pub recipe_id: String,
    pub engine_name: Option<String>,
    pub engine_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct TurnView {
    pub id: String,
    pub chat_id: String,
    pub ordinal: i64,
    pub state: String,
    pub provenance: TurnProvenanceView,
    pub error_code: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct TurnPageView {
    pub turns: Vec<TurnView>,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageSummaryView {
    pub id: String,
    pub turn_id: String,
    pub role: String,
    pub content_bytes: usize,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageSummariesView {
    pub messages: Vec<MessageSummaryView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageSegmentView {
    pub message_id: String,
    pub turn_id: String,
    pub role: String,
    pub segment_index: u32,
    pub segment_count: u32,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessagePageView {
    pub message_id: String,
    pub turn_id: String,
    pub role: String,
    pub segment_count: u32,
    pub segments: Vec<MessageSegmentView>,
    pub next_segment: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnStreamEvent {
    Started {
        chat_id: String,
        turn_id: String,
        omitted_turns: usize,
    },
    Delta {
        turn_id: String,
        content: String,
    },
    Terminal {
        turn_id: String,
        state: String,
        error_code: Option<String>,
    },
}

struct TurnStreamState<'a> {
    requested_chat_id: &'a str,
    turn_id: Option<String>,
    terminal: bool,
}

impl<'a> TurnStreamState<'a> {
    fn new(requested_chat_id: &'a str) -> Self {
        Self {
            requested_chat_id,
            turn_id: None,
            terminal: false,
        }
    }

    fn accept(&mut self, event: &TurnStreamEvent) -> Result<(), ClientError> {
        if self.terminal {
            return Err(ClientError::Transport(
                "turn event followed terminal state".into(),
            ));
        }
        match event {
            TurnStreamEvent::Started {
                chat_id, turn_id, ..
            } => {
                if self.turn_id.is_some()
                    || chat_id != self.requested_chat_id
                    || safe_id(chat_id).is_err()
                    || safe_id(turn_id).is_err()
                {
                    return Err(ClientError::Transport(
                        "invalid turn.started sequence".into(),
                    ));
                }
                self.turn_id = Some(turn_id.clone());
            }
            TurnStreamEvent::Delta { turn_id, .. } => {
                if self.turn_id.as_deref() != Some(turn_id) {
                    return Err(ClientError::Transport("invalid turn.delta sequence".into()));
                }
            }
            TurnStreamEvent::Terminal { turn_id, .. } => {
                if self.turn_id.as_deref() != Some(turn_id) {
                    return Err(ClientError::Transport(
                        "invalid terminal turn sequence".into(),
                    ));
                }
                self.terminal = true;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct StartedEvent {
    chat_id: String,
    turn_id: String,
    state: String,
    omitted_turns: usize,
}

#[derive(Deserialize)]
struct DeltaEvent {
    turn_id: String,
    content: String,
}

#[derive(Deserialize)]
struct TerminalEvent {
    turn_id: String,
    state: String,
    error_code: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    Transport(String),
    PeerProof,
    Rejected(String),
    OperationFailed(String),
    OperationCancelled,
    OperationTimeout,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => write!(f, "control transport failed: {message}"),
            Self::PeerProof => {
                f.write_str("managed node peer proof failed; refusing to send credentials")
            }
            Self::Rejected(message) => write!(f, "control request rejected: {message}"),
            Self::OperationFailed(message) => write!(f, "operation failed: {message}"),
            Self::OperationCancelled => f.write_str("operation cancelled"),
            Self::OperationTimeout => f.write_str("operation did not finish before the deadline"),
        }
    }
}

impl std::error::Error for ClientError {}

pub struct LiveControlClient {
    address: SocketAddr,
    runtime_identity: String,
    token: ControlToken,
    timeout: Duration,
}

impl fmt::Debug for LiveControlClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveControlClient")
            .field("address", &self.address)
            .field("runtime_identity", &self.runtime_identity)
            .field("token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl LiveControlClient {
    pub fn connect(
        address: SocketAddr,
        token: ControlToken,
        expected_runtime_identity: &str,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(ClientError::PeerProof);
        }
        drop(open_and_prove(
            address,
            &token,
            expected_runtime_identity,
            timeout,
        )?);
        Ok(Self {
            address,
            runtime_identity: expected_runtime_identity.to_owned(),
            token,
            timeout,
        })
    }

    pub fn download(&self, model_id: &str) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/download", Some(model_id))
    }

    pub fn load(&self, model_id: &str) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/load", Some(model_id))
    }

    pub fn unload(&self) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/unload", None)
    }

    fn start(&self, path: &str, model_id: Option<&str>) -> Result<String, ClientError> {
        let body = match model_id {
            Some(model_id) => serde_json::to_vec(
                &ModelRequest::known(model_id)
                    .map_err(|_| ClientError::Rejected("unknown model id".into()))?,
            )
            .map_err(|error| ClientError::Transport(error.to_string()))?,
            None => b"{}".to_vec(),
        };
        let response = self.authenticated("POST", path, &body)?;
        serde_json::from_slice::<OperationAccepted>(&response)
            .map(|accepted| accepted.operation_id)
            .map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn operation(&self, operation_id: &str) -> Result<OperationView, ClientError> {
        if operation_id.is_empty() || operation_id.contains(['/', '?', '#']) {
            return Err(ClientError::Rejected("invalid operation id".into()));
        }
        let response =
            self.authenticated("GET", &format!("/loxa/v1/operations/{operation_id}"), &[])?;
        serde_json::from_slice(&response).map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn models(&self) -> Result<Vec<VerifiedRecipeInventoryEntry>, ClientError> {
        let response = self.authenticated("GET", "/loxa/v1/models", &[])?;
        serde_json::from_slice(&response).map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn create_chat(&self) -> Result<ChatView, ClientError> {
        self.json("POST", CHATS_PATH, &serde_json::json!({}))
    }

    pub fn chat(&self, chat_id: &str) -> Result<ChatView, ClientError> {
        self.json_get(&chat_path(chat_id)?)
    }

    pub fn chats(&self, limit: usize, before: Option<&str>) -> Result<ChatPageView, ClientError> {
        validate_limit(limit)?;
        let mut path = format!("{CHATS_PATH}?limit={limit}");
        if let Some(before) = before {
            validate_cursor(before)?;
            path.push_str("&before=");
            path.push_str(before);
        }
        self.json_get(&path)
    }

    pub fn rename_chat(&self, chat_id: &str, title: &str) -> Result<ChatView, ClientError> {
        self.json(
            "PATCH",
            &chat_path(chat_id)?,
            &serde_json::json!({ "title": title }),
        )
    }

    pub fn delete_chat(&self, chat_id: &str) -> Result<(), ClientError> {
        let response = self.authenticated("DELETE", &chat_path(chat_id)?, &[])?;
        if response.is_empty() {
            Ok(())
        } else {
            Err(ClientError::Transport("unexpected delete response".into()))
        }
    }

    pub fn clear_chats(&self) -> Result<usize, ClientError> {
        #[derive(Deserialize)]
        struct Cleared {
            deleted: usize,
        }
        self.json::<_, Cleared>(
            "POST",
            "/loxa/v1/chats/clear",
            &serde_json::json!({ "confirm": "delete_all_chat_history" }),
        )
        .map(|value| value.deleted)
    }

    pub fn turns(
        &self,
        chat_id: &str,
        limit: usize,
        after: Option<&str>,
    ) -> Result<TurnPageView, ClientError> {
        validate_limit(limit)?;
        let mut path = format!("{}/turns?limit={limit}", chat_path(chat_id)?);
        if let Some(after) = after {
            validate_cursor(after)?;
            path.push_str("&after=");
            path.push_str(after);
        }
        self.json_get(&path)
    }

    pub fn message_summaries(
        &self,
        chat_id: &str,
        turn_id: &str,
    ) -> Result<MessageSummariesView, ClientError> {
        self.json_get(&format!(
            "{}/turns/{}/messages",
            chat_path(chat_id)?,
            safe_id(turn_id)?
        ))
    }

    pub fn message_page(
        &self,
        chat_id: &str,
        turn_id: &str,
        message_id: &str,
        segment: u32,
    ) -> Result<MessagePageView, ClientError> {
        self.json_get(&format!(
            "{}/turns/{}/messages/{}?segment={segment}",
            chat_path(chat_id)?,
            safe_id(turn_id)?,
            safe_id(message_id)?
        ))
    }

    pub fn cancel_turn(&self, chat_id: &str, turn_id: &str) -> Result<(), ClientError> {
        let path = format!("{}/turns/{}/cancel", chat_path(chat_id)?, safe_id(turn_id)?);
        let mut stream = self.open_authenticated("POST", &path, &[])?;
        let response = read_response(&mut stream, MAX_CONTROL_BYTES)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if response.status != 202 {
            return Err(rejected_response(response.status, &response.body));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CancelAccepted {
            turn_id: String,
            cancel_requested: bool,
        }
        let accepted: CancelAccepted = serde_json::from_slice(&response.body)
            .map_err(|_| ClientError::Transport("invalid cancel response".into()))?;
        if accepted.turn_id == turn_id && accepted.cancel_requested {
            Ok(())
        } else {
            Err(ClientError::Transport("unexpected cancel response".into()))
        }
    }

    pub fn stream_turn(
        &self,
        chat_id: &str,
        content: &str,
        emit: impl FnMut(TurnStreamEvent) -> Result<(), ClientError>,
    ) -> Result<TurnStreamEvent, ClientError> {
        self.stream_turn_with_cancel(chat_id, content, || false, emit)
    }

    pub fn stream_turn_with_cancel(
        &self,
        chat_id: &str,
        content: &str,
        mut cancelled: impl FnMut() -> bool,
        mut emit: impl FnMut(TurnStreamEvent) -> Result<(), ClientError>,
    ) -> Result<TurnStreamEvent, ClientError> {
        if cancelled() {
            return Err(ClientError::OperationCancelled);
        }
        let path = format!("{}/turns", chat_path(chat_id)?);
        let body = serde_json::to_vec(&serde_json::json!({
            "content": content,
            "model": "loxa"
        }))
        .map_err(|error| ClientError::Transport(error.to_string()))?;
        let mut stream = self.open_authenticated("POST", &path, &body)?;
        stream
            .set_read_timeout(Some(STREAM_READ_POLL))
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        let headers = read_stream_headers_interruptible(&mut stream, &mut cancelled)?;
        if !(200..300).contains(&headers.status) {
            let body = read_bounded_http_body(&mut stream, &headers, MAX_CONTROL_BYTES)?;
            return Err(rejected_response(headers.status, &body));
        }
        let mut parser = SseParser::default();
        let mut state = TurnStreamState::new(chat_id);
        let mut terminal = None;
        let result =
            read_http_body_chunks_interruptible(&mut stream, &headers, &mut cancelled, |chunk| {
                for event in parser.push(chunk)? {
                    let parsed = parse_turn_event(&event.name, &event.data)?;
                    state.accept(&parsed)?;
                    if matches!(parsed, TurnStreamEvent::Terminal { .. }) {
                        terminal = Some(parsed.clone());
                    }
                    emit(parsed)?;
                }
                Ok(())
            })
            .and_then(|_| parser.finish())
            .and_then(|_| {
                terminal.ok_or_else(|| {
                    ClientError::Transport("turn stream ended without terminal state".into())
                })
            });
        let started_turn = state.turn_id.clone();
        drop(stream);
        if result.is_err() {
            if let Some(turn_id) = started_turn {
                let _ = self.cancel_turn(chat_id, &turn_id);
            }
        }
        result
    }

    fn json_get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ClientError> {
        let response = self.authenticated("GET", path, &[])?;
        serde_json::from_slice(&response)
            .map_err(|_| ClientError::Transport("invalid control response".into()))
    }

    fn json<B: Serialize, T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let body =
            serde_json::to_vec(body).map_err(|error| ClientError::Transport(error.to_string()))?;
        let response = self.authenticated(method, path, &body)?;
        serde_json::from_slice(&response)
            .map_err(|_| ClientError::Transport("invalid control response".into()))
    }

    pub fn wait_terminal(&self, operation_id: &str, timeout: Duration) -> Result<(), ClientError> {
        let deadline = Instant::now() + timeout;
        loop {
            let operation = self.operation(operation_id)?;
            match operation.status {
                OperationStatus::Succeeded => return Ok(()),
                OperationStatus::Failed => {
                    return Err(ClientError::OperationFailed(
                        operation.error.unwrap_or_else(|| "unknown failure".into()),
                    ));
                }
                OperationStatus::Cancelled => return Err(ClientError::OperationCancelled),
                OperationStatus::Queued | OperationStatus::Running => {}
            }
            if Instant::now() >= deadline {
                return Err(ClientError::OperationTimeout);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn authenticated(&self, method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, ClientError> {
        let mut stream = self.open_authenticated(method, path, body)?;
        let response = read_response(&mut stream, MAX_CONTROL_BYTES)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if !(200..300).contains(&response.status) {
            return Err(rejected_response(response.status, &response.body));
        }
        Ok(response.body)
    }

    fn open_authenticated(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<TcpStream, ClientError> {
        let mut stream = open_and_prove(
            self.address,
            &self.token,
            &self.runtime_identity,
            self.timeout,
        )?;
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.address.ip(),
            self.address.port(),
            self.token.expose_for_authorization(),
            body.len()
        )
        .and_then(|_| stream.write_all(body))
        .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(stream)
    }
}

fn rejected_response(status: u16, body: &[u8]) -> ClientError {
    let message = serde_json::from_slice::<ControlErrorBody>(body)
        .map(|error| sanitize_error(&error.message))
        .unwrap_or_else(|_| format!("HTTP {status}"));
    ClientError::Rejected(message)
}

fn safe_id(value: &str) -> Result<&str, ClientError> {
    if value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(value)
    } else {
        Err(ClientError::Rejected("invalid chat history id".into()))
    }
}

fn chat_path(chat_id: &str) -> Result<String, ClientError> {
    Ok(format!("{CHATS_PATH}/{}", safe_id(chat_id)?))
}

fn validate_limit(limit: usize) -> Result<(), ClientError> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(ClientError::Rejected(
            "history page limit must be between 1 and 100".into(),
        ))
    }
}

fn validate_cursor(value: &str) -> Result<(), ClientError> {
    if !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(ClientError::Rejected("invalid history cursor".into()))
    }
}

fn open_and_prove(
    address: SocketAddr,
    token: &ControlToken,
    expected_runtime_identity: &str,
    timeout: Duration,
) -> Result<TcpStream, ClientError> {
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| ClientError::PeerProof)?;
    let nonce = encode_hex(&nonce);
    let timeout = timeout.max(Duration::from_millis(1));
    let deadline = Instant::now() + timeout;
    let mut stream =
        TcpStream::connect_timeout(&address, timeout).map_err(|_| ClientError::PeerProof)?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|_| ClientError::PeerProof)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|_| ClientError::PeerProof)?;
    let host = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    write!(stream, "GET /loxa/v1/node HTTP/1.1\r\nHost: {host}:{}\r\nX-Loxa-Challenge: {nonce}\r\nConnection: keep-alive\r\n\r\n", address.port()).map_err(|_| ClientError::PeerProof)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ClientError::PeerProof)?;
    stream
        .set_read_timeout(Some(remaining.max(Duration::from_millis(1))))
        .map_err(|_| ClientError::PeerProof)?;
    let response =
        read_response(&mut stream, MAX_PROOF_BYTES).map_err(|_| ClientError::PeerProof)?;
    if response.status != 200 {
        return Err(ClientError::PeerProof);
    }
    let value: NodeIdentityProofResponse =
        serde_json::from_slice(&response.body).map_err(|_| ClientError::PeerProof)?;
    if value.protocol_version != CONTROL_PROTOCOL_VERSION
        || value.runtime_identity != expected_runtime_identity
        || matches!(
            value.status,
            crate::control::contracts::NodeStatus::RecoveryRequired
                | crate::control::contracts::NodeStatus::Error
        )
        || !token.verify_node_identity_proof(
            &nonce,
            &value.node_id,
            &value.runtime_identity,
            value.status,
            &value.challenge_proof,
        )
    {
        return Err(ClientError::PeerProof);
    }
    Ok(stream)
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

struct StreamHeaders {
    status: u16,
    content_length: Option<usize>,
    chunked: bool,
}

fn read_stream_headers_interruptible(
    stream: &mut TcpStream,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<StreamHeaders, ClientError> {
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        if headers.len() >= 16 * 1024 {
            return Err(ClientError::Transport("response headers too large".into()));
        }
        let mut byte = [0_u8; 1];
        read_exact_interruptible(stream, &mut byte, cancelled)?;
        headers.push(byte[0]);
    }
    parse_stream_headers(&headers)
}

fn parse_stream_headers(headers: &[u8]) -> Result<StreamHeaders, ClientError> {
    let text = std::str::from_utf8(headers)
        .map_err(|_| ClientError::Transport("invalid response headers".into()))?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| ClientError::Transport("invalid HTTP status".into()))?;
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(
                    value
                        .trim()
                        .parse()
                        .map_err(|_| ClientError::Transport("invalid content length".into()))?,
                );
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
            }
        }
    }
    if chunked == content_length.is_some() {
        return Err(ClientError::Transport(
            "ambiguous HTTP response framing".into(),
        ));
    }
    Ok(StreamHeaders {
        status,
        content_length,
        chunked,
    })
}

fn read_bounded_http_body(
    stream: &mut TcpStream,
    headers: &StreamHeaders,
    max: usize,
) -> Result<Vec<u8>, ClientError> {
    let mut output = Vec::new();
    read_http_body_chunks(stream, headers, |chunk| {
        if output.len().saturating_add(chunk.len()) > max {
            return Err(ClientError::Transport("response body too large".into()));
        }
        output.extend_from_slice(chunk);
        Ok(())
    })?;
    Ok(output)
}

fn read_http_body_chunks(
    stream: &mut TcpStream,
    headers: &StreamHeaders,
    mut consume: impl FnMut(&[u8]) -> Result<(), ClientError>,
) -> Result<(), ClientError> {
    if let Some(mut remaining) = headers.content_length {
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            let take = remaining.min(buffer.len());
            stream
                .read_exact(&mut buffer[..take])
                .map_err(|error| ClientError::Transport(error.to_string()))?;
            consume(&buffer[..take])?;
            remaining -= take;
        }
        return Ok(());
    }
    if !headers.chunked {
        return Err(ClientError::Transport("missing HTTP body framing".into()));
    }
    loop {
        let size_line = read_crlf_line(stream, 128)?;
        let size_text = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| ClientError::Transport("invalid chunk size".into()))?;
        if size == 0 {
            loop {
                if read_crlf_line(stream, 16 * 1024)?.is_empty() {
                    return Ok(());
                }
            }
        }
        let mut remaining = size;
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            let take = remaining.min(buffer.len());
            stream
                .read_exact(&mut buffer[..take])
                .map_err(|error| ClientError::Transport(error.to_string()))?;
            consume(&buffer[..take])?;
            remaining -= take;
        }
        let mut ending = [0_u8; 2];
        stream
            .read_exact(&mut ending)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if ending != *b"\r\n" {
            return Err(ClientError::Transport("invalid chunk ending".into()));
        }
    }
}

fn read_http_body_chunks_interruptible(
    stream: &mut TcpStream,
    headers: &StreamHeaders,
    cancelled: &mut impl FnMut() -> bool,
    mut consume: impl FnMut(&[u8]) -> Result<(), ClientError>,
) -> Result<(), ClientError> {
    if let Some(mut remaining) = headers.content_length {
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            let take = remaining.min(buffer.len());
            read_exact_interruptible(stream, &mut buffer[..take], cancelled)?;
            if cancelled() {
                return Err(ClientError::OperationCancelled);
            }
            consume(&buffer[..take])?;
            remaining -= take;
        }
        return Ok(());
    }
    if !headers.chunked {
        return Err(ClientError::Transport("missing HTTP body framing".into()));
    }
    loop {
        let size_line = read_crlf_line_interruptible(stream, 128, cancelled)?;
        let size_text = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| ClientError::Transport("invalid chunk size".into()))?;
        if size == 0 {
            loop {
                if read_crlf_line_interruptible(stream, 16 * 1024, cancelled)?.is_empty() {
                    return Ok(());
                }
            }
        }
        let mut remaining = size;
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            let take = remaining.min(buffer.len());
            read_exact_interruptible(stream, &mut buffer[..take], cancelled)?;
            if cancelled() {
                return Err(ClientError::OperationCancelled);
            }
            consume(&buffer[..take])?;
            remaining -= take;
        }
        let mut ending = [0_u8; 2];
        read_exact_interruptible(stream, &mut ending, cancelled)?;
        if ending != *b"\r\n" {
            return Err(ClientError::Transport("invalid chunk ending".into()));
        }
    }
}

fn read_exact_interruptible(
    stream: &mut TcpStream,
    mut output: &mut [u8],
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), ClientError> {
    while !output.is_empty() {
        if cancelled() {
            return Err(ClientError::OperationCancelled);
        }
        match stream.read(output) {
            Ok(0) => return Err(ClientError::Transport("turn stream ended early".into())),
            Ok(read) => {
                let (_, remaining) = output.split_at_mut(read);
                output = remaining;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err(ClientError::Transport(error.to_string())),
        }
    }
    Ok(())
}

fn read_crlf_line_interruptible(
    stream: &mut TcpStream,
    max: usize,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<String, ClientError> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n") {
        if bytes.len() >= max {
            return Err(ClientError::Transport("HTTP line too large".into()));
        }
        let mut byte = [0_u8; 1];
        read_exact_interruptible(stream, &mut byte, cancelled)?;
        bytes.push(byte[0]);
    }
    bytes.truncate(bytes.len() - 2);
    String::from_utf8(bytes).map_err(|_| ClientError::Transport("invalid HTTP line".into()))
}

fn read_crlf_line(stream: &mut TcpStream, max: usize) -> Result<String, ClientError> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n") {
        if bytes.len() >= max {
            return Err(ClientError::Transport("HTTP line too large".into()));
        }
        let mut byte = [0_u8; 1];
        stream
            .read_exact(&mut byte)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        bytes.push(byte[0]);
    }
    bytes.truncate(bytes.len() - 2);
    String::from_utf8(bytes).map_err(|_| ClientError::Transport("invalid HTTP line".into()))
}

#[derive(Default)]
struct SseParser {
    pending: Vec<u8>,
    event_name: Option<String>,
    data_lines: Vec<String>,
}

struct RawSseEvent {
    name: String,
    data: String,
}

impl SseParser {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<RawSseEvent>, ClientError> {
        self.pending.extend_from_slice(bytes);
        if self.pending.len() > MAX_SSE_LINE_BYTES {
            return Err(ClientError::Transport("turn stream line too large".into()));
        }
        let mut events = Vec::new();
        while let Some(index) = self.pending.iter().position(|byte| *byte == b'\n') {
            let mut line = self.pending.drain(..=index).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8(line)
                .map_err(|_| ClientError::Transport("turn stream is not UTF-8".into()))?;
            if line.is_empty() {
                if let (Some(name), false) = (self.event_name.take(), self.data_lines.is_empty()) {
                    events.push(RawSseEvent {
                        name,
                        data: self.data_lines.join("\n"),
                    });
                }
                self.data_lines.clear();
            } else if let Some(name) = line.strip_prefix("event:") {
                if self.event_name.is_some() {
                    return Err(ClientError::Transport("duplicate SSE event field".into()));
                }
                self.event_name = Some(name.trim_start().to_owned());
            } else if let Some(data) = line.strip_prefix("data:") {
                self.data_lines.push(data.trim_start().to_owned());
            } else if !line.starts_with(':') {
                return Err(ClientError::Transport("unsupported SSE field".into()));
            }
        }
        Ok(events)
    }

    fn finish(&self) -> Result<(), ClientError> {
        if self.pending.is_empty() && self.event_name.is_none() && self.data_lines.is_empty() {
            Ok(())
        } else {
            Err(ClientError::Transport("truncated turn stream".into()))
        }
    }
}

fn parse_turn_event(name: &str, data: &str) -> Result<TurnStreamEvent, ClientError> {
    match name {
        "turn.started" => {
            let value: StartedEvent = serde_json::from_str(data)
                .map_err(|_| ClientError::Transport("malformed turn.started event".into()))?;
            if value.state != "streaming" {
                return Err(ClientError::Transport("invalid started turn state".into()));
            }
            Ok(TurnStreamEvent::Started {
                chat_id: value.chat_id,
                turn_id: value.turn_id,
                omitted_turns: value.omitted_turns,
            })
        }
        "turn.delta" => {
            let value: DeltaEvent = serde_json::from_str(data)
                .map_err(|_| ClientError::Transport("malformed turn.delta event".into()))?;
            Ok(TurnStreamEvent::Delta {
                turn_id: value.turn_id,
                content: value.content,
            })
        }
        "turn.completed" | "turn.cancelled" | "turn.failed" => {
            let value: TerminalEvent = serde_json::from_str(data)
                .map_err(|_| ClientError::Transport("malformed terminal turn event".into()))?;
            let expected = name.strip_prefix("turn.").unwrap_or_default();
            if value.state != expected {
                return Err(ClientError::Transport(
                    "terminal event state mismatch".into(),
                ));
            }
            Ok(TurnStreamEvent::Terminal {
                turn_id: value.turn_id,
                state: value.state,
                error_code: value.error_code,
            })
        }
        _ => Err(ClientError::Transport("unknown turn stream event".into())),
    }
}

fn read_response(stream: &mut TcpStream, max_body: usize) -> std::io::Result<HttpResponse> {
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        if headers.len() >= 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response headers too large",
            ));
        }
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        headers.push(byte[0]);
    }
    let text = std::str::from_utf8(&headers).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid response headers")
    })?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP status")
        })?;
    let mut length = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid content length")
                })?);
            }
            if name.eq_ignore_ascii_case("transfer-encoding") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "transfer encoding is unsupported",
                ));
            }
        }
    }
    let length = length.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing content length")
    })?;
    if length > max_body {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response body too large",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(HttpResponse { status, body })
}

fn sanitize_error(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::auth::ControlToken;
    use crate::control::contracts::{NodeStatus, OperationStatus};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    fn token() -> (tempfile::TempDir, ControlToken) {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let token = ControlToken::load_or_create(&dir.path().join("control.token")).unwrap();
        (dir, token)
    }

    fn serve(
        token: ControlToken,
        responses: Vec<String>,
    ) -> (
        std::net::SocketAddr,
        Arc<Mutex<Vec<String>>>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let worker = std::thread::spawn(move || {
            for body in responses {
                let (mut socket, _) = listener.accept().unwrap();
                let request = read_request(&mut socket);
                captured.lock().unwrap().push(request.clone());
                if body.contains("\"challenge_proof\"") {
                    send_response(&mut socket, 200, &body, "close");
                    continue;
                }
                let proof_body = proof_body(&token, &request);
                if body == "__PROOF__" {
                    send_response(&mut socket, 200, &proof_body, "close");
                    continue;
                }
                send_response(&mut socket, 200, &proof_body, "keep-alive");
                let authenticated = read_request(&mut socket);
                captured.lock().unwrap().push(authenticated);
                let (status, body) = body
                    .split_once(':')
                    .and_then(|(status, body)| {
                        status.parse::<u16>().ok().map(|status| (status, body))
                    })
                    .unwrap_or((200, body.as_str()));
                send_response(&mut socket, status, body, "close");
            }
        });
        (address, seen, worker)
    }

    fn read_request(socket: &mut std::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut chunk = [0; 1];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut chunk).unwrap();
            request.push(chunk[0]);
        }
        let headers = String::from_utf8(request.clone()).unwrap();
        let content_length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        socket.read_exact(&mut body).unwrap();
        request.extend_from_slice(&body);
        String::from_utf8(request).unwrap()
    }

    fn proof_body(token: &ControlToken, request: &str) -> String {
        let nonce = request
            .lines()
            .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
            .unwrap();
        let proof = token
            .node_identity_proof(nonce, "node", "runtime", NodeStatus::Unloaded)
            .unwrap();
        format!(
            r#"{{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"{proof}"}}"#
        )
    }

    fn send_response(socket: &mut std::net::TcpStream, status: u16, body: &str, connection: &str) {
        write!(
            socket,
            "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    }

    #[test]
    fn bearer_is_sent_only_after_nonce_bound_peer_proof() {
        let (_dir, token) = token();
        let accepted = r#"{"operation_id":"op-1"}"#.to_string();
        let (address, seen, worker) = serve(token.clone(), vec!["__PROOF__".into(), accepted]);
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(client.load("gemma-3-4b-it-q4").unwrap(), "op-1");
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
        assert!(!requests[1].to_ascii_lowercase().contains("authorization:"));
        assert!(requests[2]
            .to_ascii_lowercase()
            .contains("authorization: bearer "));
        assert!(!requests[2].contains("X-Loxa-Challenge"));
    }

    #[test]
    fn wrong_peer_proof_fails_before_any_authenticated_request() {
        let (_dir, token) = token();
        let bad = r#"{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"0000000000000000000000000000000000000000000000000000000000000000"}"#.to_string();
        let (address, seen, worker) = serve(token.clone(), vec![bad]);
        assert!(matches!(
            LiveControlClient::connect(
                address,
                token,
                "runtime",
                std::time::Duration::from_secs(1)
            ),
            Err(ClientError::PeerProof)
        ));
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
    }

    #[test]
    fn valid_proof_from_a_replacement_runtime_is_rejected() {
        let (_dir, token) = token();
        let (address, seen, worker) = serve(token.clone(), vec!["__PROOF__".into()]);
        assert!(matches!(
            LiveControlClient::connect(
                address,
                token,
                "different-runtime",
                std::time::Duration::from_secs(1)
            ),
            Err(ClientError::PeerProof)
        ));
        worker.join().unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn polling_reports_each_terminal_operation_state() {
        for (status, expected) in [
            ("succeeded", Ok(())),
            ("failed", Err(ClientError::OperationFailed("boom".into()))),
            ("cancelled", Err(ClientError::OperationCancelled)),
        ] {
            let (_dir, token) = token();
            let operation = format!(
                r#"{{"id":"op","kind":"load","status":"{status}","model_id":"gemma-3-4b-it-q4","progress":null,"error":{},"created_at_unix_ms":1,"updated_at_unix_ms":2}}"#,
                if status == "failed" {
                    "\"boom\""
                } else {
                    "null"
                }
            );
            let (address, _, worker) = serve(token.clone(), vec!["__PROOF__".into(), operation]);
            let client = LiveControlClient::connect(
                address,
                token,
                "runtime",
                std::time::Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(
                client.wait_terminal("op", std::time::Duration::ZERO),
                expected
            );
            worker.join().unwrap();
        }
        let _ = OperationStatus::Succeeded;
    }

    #[test]
    fn typed_control_errors_are_bounded_sanitized_and_actionable() {
        let (_dir, token) = token();
        let error = r#"409:{"code":"operation_conflict","message":"a model operation is already active\nretry later"}"#.to_string();
        let (address, _, worker) = serve(token.clone(), vec!["__PROOF__".into(), error]);
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(
            client.load("gemma-3-4b-it-q4"),
            Err(ClientError::Rejected(
                "a model operation is already activeretry later".into()
            ))
        );
        worker.join().unwrap();
    }

    #[test]
    fn every_polling_reconnect_reproves_on_the_socket_that_receives_bearer() {
        let (_dir, token) = token();
        let operation = |status: &str| {
            format!(
                r#"{{"id":"op","kind":"load","status":"{status}","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}}"#
            )
        };
        let (address, seen, worker) = serve(
            token.clone(),
            vec![
                "__PROOF__".into(),
                operation("running"),
                operation("succeeded"),
            ],
        );
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            client.operation("op").unwrap().status,
            OperationStatus::Running
        );
        assert_eq!(
            client.operation("op").unwrap().status,
            OperationStatus::Succeeded
        );
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 5);
        for (proof, authenticated) in [(1, 2), (3, 4)] {
            assert!(requests[proof].contains("X-Loxa-Challenge:"));
            assert!(!requests[proof]
                .to_ascii_lowercase()
                .contains("authorization:"));
            assert!(requests[authenticated]
                .to_ascii_lowercase()
                .contains("authorization: bearer "));
        }
    }

    #[test]
    fn history_methods_use_authenticated_bounded_routes() {
        let (_dir, token) = token();
        let id = "0123456789abcdef0123456789abcdef";
        let chat =
            format!(r#"{{"id":"{id}","title":"New chat","created_at_ms":1,"updated_at_ms":2}}"#);
        let list = format!(r#"{{"chats":[{chat}],"next_before":null}}"#);
        let renamed =
            format!(r#"{{"id":"{id}","title":"Renamed","created_at_ms":1,"updated_at_ms":3}}"#);
        let turn_id = "11111111111111111111111111111111";
        let message_id = "22222222222222222222222222222222";
        let turns = format!(
            r#"{{"turns":[{{"id":"{turn_id}","chat_id":"{id}","ordinal":1,"state":"completed","provenance":{{"model_alias":"loxa","recipe_id":"recipe","engine_name":null,"engine_version":null}},"error_code":null,"created_at_ms":1,"updated_at_ms":2}}],"next_after":null}}"#
        );
        let summaries = format!(
            r#"{{"messages":[{{"id":"{message_id}","turn_id":"{turn_id}","role":"assistant","content_bytes":5,"created_at_ms":1,"updated_at_ms":2}}]}}"#
        );
        let message = format!(
            r#"{{"message_id":"{message_id}","turn_id":"{turn_id}","role":"assistant","segment_count":1,"segments":[{{"message_id":"{message_id}","turn_id":"{turn_id}","role":"assistant","segment_index":0,"segment_count":1,"content":"hello"}}],"next_segment":null}}"#
        );
        let (address, seen, worker) = serve(
            token.clone(),
            vec![
                "__PROOF__".into(),
                chat.clone(),
                list,
                chat,
                turns,
                summaries,
                message,
                renamed,
                format!(r#"202:{{"turn_id":"{turn_id}","cancel_requested":true}}"#),
                String::new(),
                r#"{"deleted":4}"#.into(),
            ],
        );
        let client =
            LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();

        assert_eq!(client.create_chat().unwrap().title, "New chat");
        assert_eq!(client.chats(30, None).unwrap().chats.len(), 1);
        assert_eq!(client.chat(id).unwrap().title, "New chat");
        assert_eq!(client.turns(id, 30, None).unwrap().turns.len(), 1);
        assert_eq!(
            client
                .message_summaries(id, turn_id)
                .unwrap()
                .messages
                .len(),
            1
        );
        assert_eq!(
            client
                .message_page(id, turn_id, message_id, 0)
                .unwrap()
                .segments[0]
                .content,
            "hello"
        );
        assert_eq!(client.rename_chat(id, "Renamed").unwrap().title, "Renamed");
        client.cancel_turn(id, turn_id).unwrap();
        client.delete_chat(id).unwrap();
        assert_eq!(client.clear_chats().unwrap(), 4);
        worker.join().unwrap();

        let requests = seen.lock().unwrap();
        let authenticated = requests
            .iter()
            .filter(|request| {
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer ")
            })
            .collect::<Vec<_>>();
        assert!(authenticated[0].starts_with("POST /loxa/v1/chats "));
        assert!(authenticated[1].starts_with("GET /loxa/v1/chats?limit=30 "));
        assert!(authenticated[2].starts_with(&format!("GET /loxa/v1/chats/{id} ")));
        assert!(authenticated[3].starts_with(&format!("GET /loxa/v1/chats/{id}/turns?limit=30 ")));
        assert!(authenticated[4].starts_with(&format!(
            "GET /loxa/v1/chats/{id}/turns/{turn_id}/messages "
        )));
        assert!(authenticated[5].contains(&format!("/messages/{message_id}?segment=0")));
        assert!(authenticated[6].starts_with(&format!("PATCH /loxa/v1/chats/{id} ")));
        assert!(authenticated[6].ends_with(r#"{"title":"Renamed"}"#));
        assert!(authenticated[7]
            .starts_with(&format!("POST /loxa/v1/chats/{id}/turns/{turn_id}/cancel ")));
        assert!(authenticated[7].contains("Content-Length: 0\r\n"));
        assert!(authenticated[7].ends_with("\r\n\r\n"));
        assert!(authenticated[8].starts_with(&format!("DELETE /loxa/v1/chats/{id} ")));
        assert!(authenticated[9].starts_with("POST /loxa/v1/chats/clear "));
        assert!(authenticated[9].contains("delete_all_chat_history"));
    }

    #[test]
    fn turn_stream_parses_chunked_sse_incrementally_and_returns_terminal() {
        let (_dir, token) = token();
        let id = "0123456789abcdef0123456789abcdef";
        let turn_id = "11111111111111111111111111111111";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_token = token.clone();
        let worker = std::thread::spawn(move || {
            for connection_index in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                let proof_request = read_request(&mut socket);
                let proof = proof_body(&server_token, &proof_request);
                if connection_index == 0 {
                    send_response(&mut socket, 200, &proof, "close");
                    continue;
                }
                send_response(&mut socket, 200, &proof, "keep-alive");
                let authenticated = read_request(&mut socket);
                assert!(authenticated.starts_with(&format!("POST /loxa/v1/chats/{id}/turns ")));
                let events = format!(
                    "event: turn.started\ndata: {{\"chat_id\":\"{id}\",\"turn_id\":\"{turn_id}\",\"state\":\"streaming\",\"omitted_turns\":0}}\n\nevent: turn.delta\ndata: {{\"turn_id\":\"{turn_id}\",\"content\":\"hello\"}}\n\nevent: turn.completed\ndata: {{\"turn_id\":\"{turn_id}\",\"state\":\"completed\",\"error_code\":null}}\n\n"
                );
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                for chunk in events.as_bytes().chunks(7) {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                    socket.write_all(chunk).unwrap();
                    socket.write_all(b"\r\n").unwrap();
                }
                socket.write_all(b"0\r\n\r\n").unwrap();
            }
        });
        let client =
            LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();
        let mut events = Vec::new();

        let terminal = client
            .stream_turn(id, "prompt", |event| {
                events.push(event);
                Ok(())
            })
            .unwrap();

        worker.join().unwrap();
        assert_eq!(events.len(), 3);
        assert!(
            matches!(events[1], TurnStreamEvent::Delta { ref content, .. } if content == "hello")
        );
        assert!(
            matches!(terminal, TurnStreamEvent::Terminal { ref state, .. } if state == "completed")
        );
    }

    #[test]
    fn turn_stream_state_rejects_missing_duplicate_and_mismatched_events() {
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = "11111111111111111111111111111111";
        let other = "22222222222222222222222222222222";
        let delta = |id: &str| TurnStreamEvent::Delta {
            turn_id: id.into(),
            content: "x".into(),
        };
        assert!(TurnStreamState::new(chat).accept(&delta(turn)).is_err());

        let mut state = TurnStreamState::new(chat);
        state
            .accept(&TurnStreamEvent::Started {
                chat_id: chat.into(),
                turn_id: turn.into(),
                omitted_turns: 0,
            })
            .unwrap();
        assert!(state
            .accept(&TurnStreamEvent::Started {
                chat_id: chat.into(),
                turn_id: turn.into(),
                omitted_turns: 0,
            })
            .is_err());
        assert!(state.accept(&delta(other)).is_err());
        assert!(state
            .accept(&TurnStreamEvent::Terminal {
                turn_id: other.into(),
                state: "completed".into(),
                error_code: None,
            })
            .is_err());

        let mut wrong_chat = TurnStreamState::new(chat);
        assert!(wrong_chat
            .accept(&TurnStreamEvent::Started {
                chat_id: other.into(),
                turn_id: turn.into(),
                omitted_turns: 0,
            })
            .is_err());
    }

    #[test]
    fn malformed_stream_after_started_best_effort_cancels_exact_turn() {
        let (_dir, token) = token();
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = "11111111111111111111111111111111";
        let wrong = "22222222222222222222222222222222";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_token = token.clone();
        let worker = std::thread::spawn(move || {
            for connection_index in 0..3 {
                let (mut socket, _) = listener.accept().unwrap();
                let proof_request = read_request(&mut socket);
                let proof = proof_body(&server_token, &proof_request);
                if connection_index == 0 {
                    send_response(&mut socket, 200, &proof, "close");
                    continue;
                }
                send_response(&mut socket, 200, &proof, "keep-alive");
                let authenticated = read_request(&mut socket);
                if connection_index == 1 {
                    assert!(
                        authenticated.starts_with(&format!("POST /loxa/v1/chats/{chat}/turns "))
                    );
                    let events = format!(
                        "event: turn.started\ndata: {{\"chat_id\":\"{chat}\",\"turn_id\":\"{turn}\",\"state\":\"streaming\",\"omitted_turns\":0}}\n\nevent: turn.delta\ndata: {{\"turn_id\":\"{wrong}\",\"content\":\"bad\"}}\n\n"
                    );
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        events.len(),
                        events
                    )
                    .unwrap();
                } else {
                    assert!(authenticated
                        .starts_with(&format!("POST /loxa/v1/chats/{chat}/turns/{turn}/cancel ")));
                    assert!(authenticated.contains("Content-Length: 0\r\n"));
                    assert!(authenticated.ends_with("\r\n\r\n"));
                    send_response(
                        &mut socket,
                        202,
                        &format!(r#"{{"turn_id":"{turn}","cancel_requested":true}}"#),
                        "close",
                    );
                }
            }
        });
        let client =
            LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();

        assert!(client.stream_turn(chat, "prompt", |_| Ok(())).is_err());
        worker.join().unwrap();
    }

    #[test]
    fn consumer_failure_after_started_best_effort_cancels_exact_turn() {
        let (_dir, token) = token();
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = "11111111111111111111111111111111";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_token = token.clone();
        let worker = std::thread::spawn(move || {
            for connection_index in 0..3 {
                let (mut socket, _) = listener.accept().unwrap();
                let proof_request = read_request(&mut socket);
                let proof = proof_body(&server_token, &proof_request);
                if connection_index == 0 {
                    send_response(&mut socket, 200, &proof, "close");
                    continue;
                }
                send_response(&mut socket, 200, &proof, "keep-alive");
                let authenticated = read_request(&mut socket);
                if connection_index == 1 {
                    let event = format!(
                        "event: turn.started\ndata: {{\"chat_id\":\"{chat}\",\"turn_id\":\"{turn}\",\"state\":\"streaming\",\"omitted_turns\":0}}\n\n"
                    );
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        event.len(),
                        event
                    )
                    .unwrap();
                } else {
                    assert!(authenticated
                        .starts_with(&format!("POST /loxa/v1/chats/{chat}/turns/{turn}/cancel ")));
                    assert!(authenticated.contains("Content-Length: 0\r\n"));
                    assert!(authenticated.ends_with("\r\n\r\n"));
                    send_response(
                        &mut socket,
                        202,
                        &format!(r#"{{"turn_id":"{turn}","cancel_requested":true}}"#),
                        "close",
                    );
                }
            }
        });
        let client =
            LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();

        assert!(client
            .stream_turn(chat, "prompt", |_| Err(ClientError::OperationCancelled))
            .is_err());
        worker.join().unwrap();
    }

    #[test]
    fn quiet_stream_polls_cancellation_and_cancels_exact_started_turn() {
        let (_dir, token) = token();
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = "11111111111111111111111111111111";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_token = token.clone();
        let worker = std::thread::spawn(move || {
            let (mut initial, _) = listener.accept().unwrap();
            let request = read_request(&mut initial);
            send_response(
                &mut initial,
                200,
                &proof_body(&server_token, &request),
                "close",
            );

            let (mut quiet_stream, _) = listener.accept().unwrap();
            let request = read_request(&mut quiet_stream);
            send_response(
                &mut quiet_stream,
                200,
                &proof_body(&server_token, &request),
                "keep-alive",
            );
            let authenticated = read_request(&mut quiet_stream);
            assert!(authenticated.starts_with(&format!("POST /loxa/v1/chats/{chat}/turns ")));
            let event = format!(
                "event: turn.started\ndata: {{\"chat_id\":\"{chat}\",\"turn_id\":\"{turn}\",\"state\":\"streaming\",\"omitted_turns\":0}}\n\n"
            );
            write!(
                quiet_stream,
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n{:x}\r\n{}\r\n",
                event.len(),
                event
            )
            .unwrap();

            let (mut cancel, _) = listener.accept().unwrap();
            let request = read_request(&mut cancel);
            send_response(
                &mut cancel,
                200,
                &proof_body(&server_token, &request),
                "keep-alive",
            );
            let authenticated = read_request(&mut cancel);
            assert!(authenticated
                .starts_with(&format!("POST /loxa/v1/chats/{chat}/turns/{turn}/cancel ")));
            assert!(authenticated.contains("Content-Length: 0\r\n"));
            send_response(
                &mut cancel,
                202,
                &format!(r#"{{"turn_id":"{turn}","cancel_requested":true}}"#),
                "close",
            );
            drop(quiet_stream);
        });
        let client =
            LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();
        let started = Instant::now();
        let mut saw_started = false;

        let result = client.stream_turn_with_cancel(
            chat,
            "prompt",
            || started.elapsed() >= Duration::from_millis(120),
            |event| {
                saw_started |= matches!(event, TurnStreamEvent::Started { .. });
                Ok(())
            },
        );

        worker.join().unwrap();
        assert_eq!(result, Err(ClientError::OperationCancelled));
        assert!(saw_started);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cancel_requires_the_real_accepted_response_shape() {
        let (_dir, token) = token();
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = "11111111111111111111111111111111";
        for response in [String::new(), "202:{}".into()] {
            let (address, _, worker) = serve(token.clone(), vec!["__PROOF__".into(), response]);
            let client = LiveControlClient::connect(
                address,
                token.clone(),
                "runtime",
                Duration::from_secs(1),
            )
            .unwrap();
            assert!(client.cancel_turn(chat, turn).is_err());
            worker.join().unwrap();
        }
    }
}
