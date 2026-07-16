use crate::chat_history::{unix_time_ms, ChatHistory, ChatHistoryError};
use axum::{
    body::{Body, Bytes},
    extract::{rejection::BytesRejection, DefaultBodyLimit, Extension, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use loxa_core::{
    chat_history::{
        ChatCursor, ChatId, HistoryError, MessageContent, MessageId, MessageRole, Title,
        TurnCursor, TurnId, TurnMetrics, TurnProvenance, TurnState, LIST_DEFAULT_LIMIT,
        LIST_MAX_LIMIT,
    },
    control::auth::{desktop_origins, is_desktop_origin, AuthPolicy, ControlToken},
    gateway::{GatewayState, GenerationOutput},
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

const TURN_BODY_MAX_BYTES: usize = loxa_core::chat_history::USER_CONTENT_MAX_BYTES + 1024;
const SMALL_MUTATION_BODY_MAX_BYTES: usize = 4 * 1024;
const MAX_CONTEXT_TURNS: usize = 32;
const CONSERVATIVE_CONTEXT_BYTES: usize = 24 * 1024;

#[derive(Clone)]
struct ActiveTurn {
    turn_id: TurnId,
    cancel: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct ActiveTurns {
    starting: HashSet<String>,
    running: HashMap<String, ActiveTurn>,
    shutting_down: bool,
}

#[derive(Default)]
struct ActiveTurnRegistry {
    state: Mutex<ActiveTurns>,
    empty: Condvar,
}

#[derive(Clone)]
pub struct ChatRoutesState {
    policy: Arc<AuthPolicy>,
    history: ChatHistory,
    gateway: GatewayState,
    active: Arc<ActiveTurnRegistry>,
}

impl ChatRoutesState {
    pub fn new(token: ControlToken, history: ChatHistory, gateway: GatewayState) -> Self {
        Self {
            policy: Arc::new(AuthPolicy::new(token, desktop_origins())),
            history,
            gateway,
            active: Arc::new(ActiveTurnRegistry::default()),
        }
    }

    pub fn shutdown_and_wait(&self) {
        let mut active = self.active.state.lock().expect("active turns poisoned");
        active.shutting_down = true;
        for turn in active.running.values() {
            turn.cancel.send_replace(true);
        }
        while !active.running.is_empty() || !active.starting.is_empty() {
            active = self
                .active
                .empty
                .wait(active)
                .expect("active turns poisoned");
        }
    }
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

fn authorize(
    state: &ChatRoutesState,
    headers: &HeaderMap,
) -> Result<Option<String>, Box<Response>> {
    let origin = request_origin(headers).map_err(|status| Box::new(status.into_response()))?;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state
        .policy
        .authorize(origin.as_deref(), bearer)
        .map_err(|_| {
            Box::new(cors(
                StatusCode::UNAUTHORIZED.into_response(),
                origin.as_deref(),
            ))
        })?;
    Ok(origin)
}

fn cors(mut response: Response, origin: Option<&str>) -> Response {
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Origin"));
    if let Some(value) = origin.and_then(|value| HeaderValue::from_str(value).ok()) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    response
}

fn error_response(error: ChatHistoryError, origin: Option<&str>) -> Response {
    let (status, code, message) = match error {
        ChatHistoryError::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            "history_busy",
            "chat history is temporarily busy",
        ),
        ChatHistoryError::Stopped => (
            StatusCode::SERVICE_UNAVAILABLE,
            "history_unavailable",
            "chat history is unavailable",
        ),
        ChatHistoryError::Repository(HistoryError::NotFound) => (
            StatusCode::NOT_FOUND,
            "history_not_found",
            "chat history record was not found",
        ),
        ChatHistoryError::Repository(HistoryError::Conflict) => (
            StatusCode::CONFLICT,
            "history_conflict",
            "chat history operation conflicts with current state",
        ),
        ChatHistoryError::Repository(
            HistoryError::InvalidId
            | HistoryError::InvalidTitle
            | HistoryError::InvalidContent
            | HistoryError::InvalidTimestamp
            | HistoryError::InvalidCursor
            | HistoryError::InvalidPageLimit
            | HistoryError::InvalidMetadata
            | HistoryError::InvalidTurnState
            | HistoryError::InvalidMessageRole,
        ) => (
            StatusCode::BAD_REQUEST,
            "invalid_history_request",
            "chat history request is invalid",
        ),
        ChatHistoryError::Repository(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "history_failed",
            "chat history operation failed",
        ),
    };
    cors(
        (
            status,
            Json(json!({"error":{"code":code,"message":message}})),
        )
            .into_response(),
        origin,
    )
}

fn coded_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    origin: Option<&str>,
) -> Response {
    cors(
        (
            status,
            Json(json!({"error":{"code":code,"message":message}})),
        )
            .into_response(),
        origin,
    )
}

fn chat_busy(origin: Option<&str>) -> Response {
    coded_error(
        StatusCode::CONFLICT,
        "chat_busy",
        "this chat already has an active turn",
        origin,
    )
}

fn bounded_body(
    body: Result<Bytes, BytesRejection>,
    origin: Option<&str>,
) -> Result<Bytes, Box<Response>> {
    body.map_err(|_| {
        Box::new(coded_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "the request body exceeds this route's limit",
            origin,
        ))
    })
}

fn chat_is_busy(state: &ChatRoutesState, chat: &ChatId) -> bool {
    let active = state.active.state.lock().expect("active turns poisoned");
    active.starting.contains(chat.as_str()) || active.running.contains_key(chat.as_str())
}

fn reserve_chat(state: &ChatRoutesState, chat: &ChatId) -> Result<(), ()> {
    let mut active = state.active.state.lock().expect("active turns poisoned");
    if active.shutting_down
        || active.starting.contains(chat.as_str())
        || active.running.contains_key(chat.as_str())
    {
        return Err(());
    }
    active.starting.insert(chat.to_string());
    Ok(())
}

fn release_starting(state: &ChatRoutesState, chat: &ChatId) {
    let mut active = state.active.state.lock().expect("active turns poisoned");
    active.starting.remove(chat.as_str());
    if active.starting.is_empty() && active.running.is_empty() {
        state.active.empty.notify_all();
    }
}

fn parse_id<T>(
    value: &str,
    parser: impl FnOnce(&str) -> Result<T, HistoryError>,
    origin: Option<&str>,
) -> Result<T, Box<Response>> {
    parser(value)
        .map_err(|error| Box::new(error_response(ChatHistoryError::Repository(error), origin)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    limit: Option<usize>,
    before: Option<String>,
}

async fn list_chats(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let before = match query.before.as_deref().map(ChatCursor::parse).transpose() {
        Ok(value) => value,
        Err(error) => {
            return error_response(ChatHistoryError::Repository(error), origin.as_deref())
        }
    };
    match state
        .history
        .list_chats(query.limit.unwrap_or(LIST_DEFAULT_LIMIT), before)
        .await
    {
        Ok(page) => cors(Json(page).into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

async fn create_chat(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let body = match bounded_body(body, origin.as_deref()) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    if !body.is_empty() && body.as_ref() != b"{}" {
        return error_response(
            ChatHistoryError::Repository(HistoryError::InvalidMetadata),
            origin.as_deref(),
        );
    }
    match state.history.create_chat(unix_time_ms()).await {
        Ok(chat) => cors(
            (StatusCode::CREATED, Json(chat)).into_response(),
            origin.as_deref(),
        ),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

async fn get_chat(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let id = match parse_id(&id, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.history.get_chat(id).await {
        Ok(chat) => cors(Json(chat).into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameRequest {
    title: String,
}
async fn rename_chat(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let body = match bounded_body(body, origin.as_deref()) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let id = match parse_id(&id, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    if chat_is_busy(&state, &id) {
        return chat_busy(origin.as_deref());
    }
    let request: RenameRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::InvalidTitle),
                origin.as_deref(),
            )
        }
    };
    let title = match Title::new(&request.title) {
        Ok(value) => value,
        Err(error) => {
            return error_response(ChatHistoryError::Repository(error), origin.as_deref())
        }
    };
    match state.history.rename_chat(id, title, unix_time_ms()).await {
        Ok(chat) => cors(Json(chat).into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

async fn delete_chat(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let id = match parse_id(&id, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    if chat_is_busy(&state, &id) {
        return chat_busy(origin.as_deref());
    }
    match state.history.delete_chat(id).await {
        Ok(()) => cors(StatusCode::NO_CONTENT.into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClearRequest {
    confirm: String,
}
async fn clear_all(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let body = match bounded_body(body, origin.as_deref()) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let request: ClearRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::InvalidMetadata),
                origin.as_deref(),
            )
        }
    };
    if request.confirm != "delete_all_chat_history" {
        return error_response(
            ChatHistoryError::Repository(HistoryError::InvalidMetadata),
            origin.as_deref(),
        );
    }
    let any_chat_busy = {
        let active = state.active.state.lock().expect("active turns poisoned");
        !active.starting.is_empty() || !active.running.is_empty()
    };
    if any_chat_busy {
        return coded_error(
            StatusCode::CONFLICT,
            "chat_busy",
            "chat history cannot be cleared while a turn is active",
            origin.as_deref(),
        );
    }
    match state.history.clear_all().await {
        Ok(deleted) => cors(
            Json(json!({"deleted":deleted})).into_response(),
            origin.as_deref(),
        ),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TurnQuery {
    limit: Option<usize>,
    after: Option<String>,
}
async fn list_turns(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<TurnQuery>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let id = match parse_id(&id, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let after = match query.after.as_deref().map(TurnCursor::parse).transpose() {
        Ok(value) => value,
        Err(error) => {
            return error_response(ChatHistoryError::Repository(error), origin.as_deref())
        }
    };
    match state
        .history
        .list_turns(id, query.limit.unwrap_or(LIST_DEFAULT_LIMIT), after)
        .await
    {
        Ok(page) => cors(Json(page).into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

async fn message_summaries(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path((chat, turn)): Path<(String, String)>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let chat = match parse_id(&chat, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let turn = match parse_id(&turn, TurnId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.history.get_turn(turn.clone()).await {
        Ok(record) if record.chat_id == chat => {}
        Ok(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::NotFound),
                origin.as_deref(),
            )
        }
        Err(error) => return error_response(error, origin.as_deref()),
    }
    match state.history.message_summaries(turn).await {
        Ok(messages) => cors(
            Json(json!({"messages":messages})).into_response(),
            origin.as_deref(),
        ),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentQuery {
    segment: Option<u32>,
}
async fn message_page(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path((chat, turn, message)): Path<(String, String, String)>,
    Query(query): Query<SegmentQuery>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let chat = match parse_id(&chat, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let turn = match parse_id(&turn, TurnId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let message = match parse_id(&message, MessageId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.history.get_turn(turn.clone()).await {
        Ok(record) if record.chat_id == chat => {}
        Ok(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::NotFound),
                origin.as_deref(),
            )
        }
        Err(error) => return error_response(error, origin.as_deref()),
    }
    match state.history.message_summaries(turn).await {
        Ok(summaries) if summaries.iter().any(|summary| summary.id == message) => {}
        Ok(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::NotFound),
                origin.as_deref(),
            )
        }
        Err(error) => return error_response(error, origin.as_deref()),
    }
    match state
        .history
        .message_page(message, query.segment.unwrap_or(0))
        .await
    {
        Ok(page) => cors(Json(page).into_response(), origin.as_deref()),
        Err(error) => error_response(error, origin.as_deref()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TurnRequest {
    content: String,
    model: String,
}

fn sse_event(name: &str, value: serde_json::Value) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {value}\n\n"))
}

#[derive(Debug, PartialEq, Eq)]
enum UpstreamEvent {
    Chunk {
        delta: Option<String>,
        finish_reason: Option<String>,
        output_tokens: Option<u64>,
    },
    Done,
    Ignored,
}

#[derive(Debug, PartialEq, Eq)]
enum UpstreamSseError {
    Malformed,
}

fn parse_upstream_event(bytes: &[u8]) -> Result<UpstreamEvent, UpstreamSseError> {
    let text = std::str::from_utf8(bytes).map_err(|_| UpstreamSseError::Malformed)?;
    let mut data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim));
    let Some(payload) = data.next() else {
        if text
            .lines()
            .all(|line| line.is_empty() || line.starts_with(':'))
        {
            return Ok(UpstreamEvent::Ignored);
        }
        return Err(UpstreamSseError::Malformed);
    };
    if data.next().is_some() {
        return Err(UpstreamSseError::Malformed);
    }
    if payload == "[DONE]" {
        return Ok(UpstreamEvent::Done);
    }
    let value: serde_json::Value =
        serde_json::from_str(payload).map_err(|_| UpstreamSseError::Malformed)?;
    let root = value.as_object().ok_or(UpstreamSseError::Malformed)?;
    let choices = root
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .ok_or(UpstreamSseError::Malformed)?;
    if choices.len() > 1 {
        return Err(UpstreamSseError::Malformed);
    }
    let choice = choices
        .first()
        .map(|choice| choice.as_object().ok_or(UpstreamSseError::Malformed))
        .transpose()?;
    let delta = match choice.and_then(|choice| choice.get("delta")) {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Object(delta)) => match delta.get("content") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(content)) => Some(content.clone()),
            Some(_) => return Err(UpstreamSseError::Malformed),
        },
        Some(_) => return Err(UpstreamSseError::Malformed),
    };
    let finish_reason = match choice.and_then(|choice| choice.get("finish_reason")) {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(reason))
            if !reason.trim().is_empty() && reason.len() <= 128 && !reason.contains('\0') =>
        {
            Some(reason.clone())
        }
        Some(_) => return Err(UpstreamSseError::Malformed),
    };
    let output_tokens = match root.get("usage") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Object(usage)) => {
            match usage
                .get("completion_tokens")
                .and_then(serde_json::Value::as_u64)
            {
                Some(tokens) if tokens <= i64::MAX as u64 => Some(tokens),
                _ => return Err(UpstreamSseError::Malformed),
            }
        }
        Some(_) => return Err(UpstreamSseError::Malformed),
    };
    Ok(UpstreamEvent::Chunk {
        delta,
        finish_reason,
        output_tokens,
    })
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .min(i64::MAX as u64)
}

#[derive(Clone, Debug)]
struct ContextTurn {
    user: String,
    assistant: String,
}

fn chat_message(role: &'static str, content: &str) -> serde_json::Value {
    json!({"role":role,"content":content})
}

fn budget_context(
    turns: Vec<ContextTurn>,
    current: &str,
    budget: usize,
) -> Result<(Vec<serde_json::Value>, usize), HistoryError> {
    let mut messages = vec![chat_message("user", current)];
    if serde_json::to_vec(&messages)
        .map_err(|_| HistoryError::Database)?
        .len()
        > budget
    {
        return Err(HistoryError::InvalidContent);
    }
    let candidate_count = turns.len().min(MAX_CONTEXT_TURNS);
    let mut omitted = turns.len().saturating_sub(candidate_count);
    let selected = turns
        .into_iter()
        .skip(omitted)
        .collect::<Vec<ContextTurn>>();
    for (index, turn) in selected.into_iter().rev().enumerate() {
        let mut candidate = vec![
            chat_message("user", &turn.user),
            chat_message("assistant", &turn.assistant),
        ];
        candidate.extend(messages.clone());
        if serde_json::to_vec(&candidate)
            .map_err(|_| HistoryError::Database)?
            .len()
            <= budget
        {
            messages = candidate;
        } else {
            omitted += candidate_count - index;
            break;
        }
    }
    Ok((messages, omitted))
}

async fn read_message(
    history: &ChatHistory,
    message: MessageId,
) -> Result<String, ChatHistoryError> {
    let mut next = 0;
    let mut content = String::new();
    loop {
        let page = history.message_page(message.clone(), next).await?;
        for segment in page.segments {
            content.push_str(&segment.content);
        }
        match page.next_segment {
            Some(segment) => next = segment,
            None => return Ok(content),
        }
    }
}

async fn conversation_context(
    history: &ChatHistory,
    chat: ChatId,
    current: &str,
) -> Result<(Vec<serde_json::Value>, usize), ChatHistoryError> {
    let mut after = None;
    let mut completed = VecDeque::with_capacity(MAX_CONTEXT_TURNS);
    let mut completed_count = 0usize;
    loop {
        let page = history
            .list_turns(chat.clone(), LIST_MAX_LIMIT, after)
            .await?;
        for turn in page
            .turns
            .into_iter()
            .filter(|turn| turn.state == TurnState::Completed)
        {
            completed_count = completed_count.saturating_add(1);
            if completed.len() == MAX_CONTEXT_TURNS {
                completed.pop_front();
            }
            completed.push_back(turn);
        }
        match page.next_after {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    let mut context_turns = Vec::new();
    let skipped = completed_count.saturating_sub(completed.len());
    for turn in completed {
        let summaries = history.message_summaries(turn.id).await?;
        let user = summaries
            .iter()
            .find(|message| message.role == MessageRole::User)
            .ok_or(ChatHistoryError::Repository(HistoryError::CorruptDatabase))?;
        let assistant = summaries
            .iter()
            .find(|message| message.role == MessageRole::Assistant)
            .ok_or(ChatHistoryError::Repository(HistoryError::CorruptDatabase))?;
        context_turns.push(ContextTurn {
            user: read_message(history, user.id.clone()).await?,
            assistant: read_message(history, assistant.id.clone()).await?,
        });
    }
    let (messages, budget_omitted) =
        budget_context(context_turns, current, CONSERVATIVE_CONTEXT_BYTES)
            .map_err(ChatHistoryError::Repository)?;
    Ok((messages, skipped + budget_omitted))
}

struct GuardedReceiverStream {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, Infallible>>,
    cancel: tokio::sync::watch::Sender<bool>,
}

impl futures_util::Stream for GuardedReceiverStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for GuardedReceiverStream {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

async fn send_downstream(
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, Infallible>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    event: Bytes,
) -> bool {
    tokio::select! {
        biased;
        _ = cancelled.changed() => false,
        sent = sender.send(Ok(event)) => sent.is_ok(),
    }
}

fn emit_turn_started(chat_id: &str, turn_id: &str, request_id: Option<&str>) {
    if let Some(request_id) = request_id {
        tracing::info!(
            target: "loxa_node::chat",
            event_code = "chat.turn.started",
            component = "chat",
            chat_id,
            turn_id,
            request_id,
            state = "streaming",
        );
    } else {
        tracing::info!(
            target: "loxa_node::chat",
            event_code = "chat.turn.started",
            component = "chat",
            chat_id,
            turn_id,
            state = "streaming",
        );
    }
}

fn emit_turn_terminal(
    chat_id: &str,
    turn_id: &str,
    request_id: Option<&str>,
    result: &'static str,
) {
    if let Some(request_id) = request_id {
        tracing::info!(
            target: "loxa_node::chat",
            event_code = "chat.turn.terminal",
            component = "chat",
            chat_id,
            turn_id,
            request_id,
            state = result,
            status = result,
            result_class = result,
        );
    } else {
        tracing::info!(
            target: "loxa_node::chat",
            event_code = "chat.turn.terminal",
            component = "chat",
            chat_id,
            turn_id,
            state = result,
            status = result,
            result_class = result,
        );
    }
}

async fn persistent_turn(
    State(state): State<ChatRoutesState>,
    request_id: Option<Extension<crate::http_observability::DiagnosticRequestId>>,
    headers: HeaderMap,
    Path(chat): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let body = match bounded_body(body, origin.as_deref()) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let chat = match parse_id(&chat, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let request: TurnRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return error_response(
                ChatHistoryError::Repository(HistoryError::InvalidContent),
                origin.as_deref(),
            )
        }
    };
    if request.model != "loxa" {
        return error_response(
            ChatHistoryError::Repository(HistoryError::InvalidMetadata),
            origin.as_deref(),
        );
    }
    let content = match MessageContent::user(&request.content) {
        Ok(value) => value,
        Err(error) => {
            return error_response(ChatHistoryError::Repository(error), origin.as_deref())
        }
    };
    if reserve_chat(&state, &chat).is_err() {
        return chat_busy(origin.as_deref());
    }
    let (messages, omitted_turns) =
        match conversation_context(&state.history, chat.clone(), content.as_str()).await {
            Ok(value) => value,
            Err(error) => {
                release_starting(&state, &chat);
                return error_response(error, origin.as_deref());
            }
        };
    let prepared = match state.gateway.prepare_generation(json!({
        "model":"loxa",
        "stream":true,
        "stream_options":{"include_usage":true},
        "messages":messages
    })) {
        Ok(value) => value,
        Err(_) => {
            release_starting(&state, &chat);
            return coded_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "model_unavailable",
                "the Loxa model is not ready",
                origin.as_deref(),
            );
        }
    };
    let provenance = match TurnProvenance::new(
        &prepared.provenance().model_id,
        Some(&prepared.provenance().engine),
        Some(&prepared.provenance().engine_version),
    ) {
        Ok(value) => value,
        Err(error) => {
            release_starting(&state, &chat);
            return error_response(ChatHistoryError::Repository(error), origin.as_deref());
        }
    };
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    let turn = match state
        .history
        .begin_turn(chat.clone(), content, provenance, unix_time_ms())
        .await
    {
        Ok(value) => value,
        Err(error) => {
            release_starting(&state, &chat);
            return error_response(error, origin.as_deref());
        }
    };
    let turn_id = turn.id.clone();
    {
        let mut active = state.active.state.lock().expect("active turns poisoned");
        active.starting.remove(chat.as_str());
        active.running.insert(
            chat.to_string(),
            ActiveTurn {
                turn_id: turn_id.clone(),
                cancel: cancel.clone(),
            },
        );
        if active.shutting_down {
            cancel.send_replace(true);
        }
    }
    let diagnostic_request_id = request_id.map(|Extension(value)| value.0);
    emit_turn_started(
        chat.as_str(),
        turn_id.as_str(),
        diagnostic_request_id.as_deref(),
    );
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(32);
    let task_state = state.clone();
    tokio::spawn(async move {
        let started = sse_event(
            "turn.started",
            json!({"chat_id":chat,"turn_id":turn_id,"state":"streaming","omitted_turns":omitted_turns}),
        );
        let mut receiver_alive = send_downstream(&sender, &mut cancelled, started).await;
        let mut assistant = String::new();
        let mut terminal = TurnState::Failed;
        let mut error_code: Option<String>;
        let mut last_checkpoint = Instant::now();
        let mut checkpoint_bytes = 0usize;
        let mut dispatch_started = None;
        let mut ttft_ms = None;
        let mut output_tokens = None;
        let mut stop_reason = None;
        if receiver_alive {
            dispatch_started = Some(Instant::now());
            match tokio::select! { value = prepared.execute() => Some(value), _ = cancelled.changed() => None }
            {
                Some(Ok(GenerationOutput::Stream(mut stream))) => {
                    terminal = TurnState::Completed;
                    error_code = None;
                    let mut saw_done = false;
                    loop {
                        let next = tokio::select! { value = stream.next() => value, _ = cancelled.changed() => { terminal = TurnState::Cancelled; error_code = None; break; } };
                        match next {
                            Some(Ok(bytes)) => match parse_upstream_event(&bytes) {
                                Ok(UpstreamEvent::Done) => {
                                    saw_done = true;
                                    break;
                                }
                                Ok(UpstreamEvent::Ignored) => continue,
                                Err(UpstreamSseError::Malformed) => {
                                    terminal = TurnState::Failed;
                                    error_code = Some("malformed_upstream_event".into());
                                    break;
                                }
                                Ok(UpstreamEvent::Chunk {
                                    delta,
                                    finish_reason,
                                    output_tokens: chunk_output_tokens,
                                }) => {
                                    if let Some(value) = finish_reason {
                                        if stop_reason.as_ref().is_some_and(|known| known != &value)
                                        {
                                            terminal = TurnState::Failed;
                                            error_code =
                                                Some("conflicting_upstream_metrics".into());
                                            break;
                                        }
                                        stop_reason = Some(value);
                                    }
                                    if let Some(value) = chunk_output_tokens {
                                        if output_tokens.is_some_and(|known| known != value) {
                                            terminal = TurnState::Failed;
                                            error_code =
                                                Some("conflicting_upstream_metrics".into());
                                            break;
                                        }
                                        output_tokens = Some(value);
                                    }
                                    let Some(delta) = delta else { continue };
                                    if !delta.is_empty() && ttft_ms.is_none() {
                                        ttft_ms = dispatch_started.map(elapsed_ms);
                                    }
                                    if assistant.len().saturating_add(delta.len())
                                        > loxa_core::chat_history::ASSISTANT_CONTENT_MAX_BYTES
                                    {
                                        terminal = TurnState::Failed;
                                        error_code = Some("response_too_large".into());
                                        break;
                                    }
                                    assistant.push_str(&delta);
                                    checkpoint_bytes += delta.len();
                                    receiver_alive = send_downstream(
                                        &sender,
                                        &mut cancelled,
                                        sse_event(
                                            "turn.delta",
                                            json!({"turn_id":turn_id,"content":delta}),
                                        ),
                                    )
                                    .await;
                                    if !receiver_alive {
                                        terminal = TurnState::Cancelled;
                                        error_code = None;
                                        break;
                                    }
                                    if checkpoint_bytes >= 2 * 1024
                                        && last_checkpoint.elapsed() >= Duration::from_millis(250)
                                    {
                                        let checkpoint = MessageContent::assistant(&assistant)
                                            .expect("bounded assistant content");
                                        if task_state
                                            .history
                                            .checkpoint(turn_id.clone(), checkpoint, unix_time_ms())
                                            .await
                                            .is_err()
                                        {
                                            terminal = TurnState::Failed;
                                            error_code = Some("history_write_failed".into());
                                            break;
                                        }
                                        checkpoint_bytes = 0;
                                        last_checkpoint = Instant::now();
                                    }
                                }
                            },
                            Some(Err(_)) => {
                                terminal = TurnState::Failed;
                                error_code = Some("generation_failed".into());
                                break;
                            }
                            None => {
                                terminal = TurnState::Failed;
                                error_code = Some("incomplete_upstream_stream".into());
                                break;
                            }
                        }
                    }
                    if terminal == TurnState::Completed && !saw_done {
                        terminal = TurnState::Failed;
                        error_code = Some("incomplete_upstream_stream".into());
                    }
                }
                Some(Ok(GenerationOutput::Json { .. })) => {
                    error_code = Some("invalid_generation_response".into());
                }
                Some(Err(_)) => {
                    error_code = Some("generation_failed".into());
                }
                None => {
                    terminal = TurnState::Cancelled;
                    error_code = None;
                }
            }
        } else {
            terminal = TurnState::Cancelled;
            error_code = None;
        }
        let content = MessageContent::assistant(&assistant)
            .unwrap_or_else(|_| MessageContent::assistant("").expect("empty assistant content"));
        let metrics = TurnMetrics::new(
            output_tokens,
            dispatch_started.map(elapsed_ms),
            ttft_ms,
            stop_reason.as_deref(),
        )
        .expect("bounded upstream metrics and monotonic local timings");
        let persisted = task_state
            .history
            .finalize(
                turn_id.clone(),
                terminal,
                content,
                error_code.clone(),
                metrics.clone(),
                unix_time_ms(),
            )
            .await;
        if persisted.is_ok() {
            let diagnostic_result = match terminal {
                TurnState::Completed => "completed",
                TurnState::Cancelled => "cancelled",
                _ => "failed",
            };
            emit_turn_terminal(
                chat.as_str(),
                turn_id.as_str(),
                diagnostic_request_id.as_deref(),
                diagnostic_result,
            );
        }
        if receiver_alive {
            let (event, state_name, code) = if persisted.is_err() {
                ("turn.failed", "failed", Some("history_write_failed"))
            } else {
                match terminal {
                    TurnState::Completed => ("turn.completed", "completed", None),
                    TurnState::Cancelled => ("turn.cancelled", "cancelled", None),
                    _ => ("turn.failed", "failed", error_code.as_deref()),
                }
            };
            let _ = send_downstream(
                &sender,
                &mut cancelled,
                sse_event(
                    event,
                    json!({"turn_id":turn_id,"state":state_name,"error_code":code,"metrics":metrics}),
                ),
            )
            .await;
        }
        {
            let mut active = task_state
                .active
                .state
                .lock()
                .expect("active turns poisoned");
            active.running.remove(chat.as_str());
            if active.starting.is_empty() && active.running.is_empty() {
                task_state.active.empty.notify_all();
            }
        }
    });
    let stream = GuardedReceiverStream { receiver, cancel };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    cors(response, origin.as_deref())
}

async fn cancel_turn(
    State(state): State<ChatRoutesState>,
    headers: HeaderMap,
    Path((chat, turn)): Path<(String, String)>,
) -> Response {
    let origin = match authorize(&state, &headers) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let chat = match parse_id(&chat, ChatId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let turn = match parse_id(&turn, TurnId::parse, origin.as_deref()) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let record = match state.history.get_turn(turn.clone()).await {
        Ok(value) => value,
        Err(error) => return error_response(error, origin.as_deref()),
    };
    if record.chat_id != chat {
        return error_response(
            ChatHistoryError::Repository(HistoryError::NotFound),
            origin.as_deref(),
        );
    }
    let requested = {
        let active = state.active.state.lock().expect("active turns poisoned");
        active
            .running
            .get(chat.as_str())
            .filter(|active| active.turn_id == turn)
            .map(|active| active.cancel.clone())
    };
    let Some(cancel) = requested else {
        return coded_error(
            StatusCode::CONFLICT,
            "turn_not_active",
            "the requested turn is not active",
            origin.as_deref(),
        );
    };
    cancel.send_replace(true);
    tracing::info!(
        target: "loxa_node::chat",
        event_code = "chat.turn.cancel_requested",
        component = "chat",
        chat_id = chat.as_str(),
        turn_id = turn.as_str(),
        state = "cancel_requested",
        result_class = "accepted",
    );
    cors(
        (
            StatusCode::ACCEPTED,
            Json(json!({"turn_id":turn,"cancel_requested":true})),
        )
            .into_response(),
        origin.as_deref(),
    )
}

fn preflight_method_allowed(method: &str, allowed: &[&str]) -> bool {
    allowed.contains(&method)
}

async fn preflight_for(headers: HeaderMap, allowed: &'static [&'static str]) -> Response {
    let origin = match request_origin(&headers) {
        Ok(Some(value)) => value,
        _ => return StatusCode::FORBIDDEN.into_response(),
    };
    let method = headers
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !preflight_method_allowed(method, allowed) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Some(requested) = headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
    {
        let permits_content_type = matches!(method, "POST" | "PATCH");
        if !requested.split(',').all(|name| {
            let name = name.trim();
            name.is_empty()
                || name.eq_ignore_ascii_case("authorization")
                || (permits_content_type && name.eq_ignore_ascii_case("content-type"))
        }) {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_str(&format!("{}, OPTIONS", allowed.join(", ")))
            .expect("static preflight methods are valid"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type"),
    );
    cors(response, Some(&origin))
}

async fn preflight_chats(headers: HeaderMap) -> Response {
    preflight_for(headers, &["GET", "POST"]).await
}
async fn preflight_post(headers: HeaderMap) -> Response {
    preflight_for(headers, &["POST"]).await
}
async fn preflight_chat(headers: HeaderMap) -> Response {
    preflight_for(headers, &["GET", "PATCH", "DELETE"]).await
}
async fn preflight_get(headers: HeaderMap) -> Response {
    preflight_for(headers, &["GET"]).await
}
async fn preflight_turns(headers: HeaderMap) -> Response {
    preflight_for(headers, &["GET", "POST"]).await
}

pub fn router(state: ChatRoutesState) -> Router {
    Router::new()
        .route(
            "/loxa/v1/chats",
            get(list_chats)
                .post(create_chat)
                .options(preflight_chats)
                .layer(DefaultBodyLimit::max(SMALL_MUTATION_BODY_MAX_BYTES)),
        )
        .route(
            "/loxa/v1/chats/clear",
            post(clear_all)
                .options(preflight_post)
                .layer(DefaultBodyLimit::max(SMALL_MUTATION_BODY_MAX_BYTES)),
        )
        .route(
            "/loxa/v1/chats/{chat}",
            get(get_chat)
                .patch(rename_chat)
                .delete(delete_chat)
                .options(preflight_chat)
                .layer(DefaultBodyLimit::max(SMALL_MUTATION_BODY_MAX_BYTES)),
        )
        .route(
            "/loxa/v1/chats/{chat}/turns",
            get(list_turns)
                .post(persistent_turn)
                .options(preflight_turns)
                .layer(DefaultBodyLimit::max(TURN_BODY_MAX_BYTES)),
        )
        .route(
            "/loxa/v1/chats/{chat}/turns/{turn}/cancel",
            post(cancel_turn)
                .options(preflight_post)
                .layer(DefaultBodyLimit::max(0)),
        )
        .route(
            "/loxa/v1/chats/{chat}/turns/{turn}/messages",
            get(message_summaries)
                .options(preflight_get)
                .layer(DefaultBodyLimit::max(0)),
        )
        .route(
            "/loxa/v1/chats/{chat}/turns/{turn}/messages/{message}",
            get(message_page)
                .options(preflight_get)
                .layer(DefaultBodyLimit::max(0)),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Method;
    use loxa_protocol::{NodeId, NodeInstanceId};
    use std::collections::BTreeMap;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;
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
        fn register_callsite(
            &self,
            _: &'static Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::always()
        }
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            Some(tracing::metadata::LevelFilter::TRACE)
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut FieldCapture(&mut fields));
            self.0.lock().unwrap().push(CapturedEvent {
                target: event.metadata().target().to_owned(),
                level: *event.metadata().level(),
                fields,
            });
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn run_isolated_capture_test(test_name: &str, marker: &str) -> bool {
        let arguments: Vec<_> = std::env::args().collect();
        let exact_child = std::env::var_os(marker).as_deref()
            == Some(std::ffi::OsStr::new("child"))
            && arguments.iter().any(|argument| argument == "--exact")
            && arguments.iter().any(|argument| argument == test_name);
        if exact_child {
            return false;
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(marker, "child")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success()
                && stdout.contains("running 1 test")
                && stdout.contains("1 passed; 0 failed"),
            "isolated test did not run exactly once\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        true
    }

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn gateway_state() -> GatewayState {
        GatewayState::new(NodeId::new_v4(), NodeInstanceId::new_v4())
    }

    fn temp_history_path() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "loxa-node-routes-{}-{nonce}-{}",
                std::process::id(),
                TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ))
            .join("chat-history.sqlite3")
    }

    struct RouteFixture {
        base: String,
        bearer: String,
        history: ChatHistory,
        gateway: GatewayState,
        state: ChatRoutesState,
        server: Option<loxa_core::gateway::GatewayServer>,
        worker: Option<crate::chat_history::ChatHistoryWorker>,
        root: std::path::PathBuf,
    }

    impl RouteFixture {
        fn shutdown(mut self) {
            self.state.shutdown_and_wait();
            self.server.take().unwrap().shutdown().unwrap();
            self.worker.take().unwrap().stop_and_join().unwrap();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    fn route_fixture() -> RouteFixture {
        let history_path = temp_history_path();
        let root = history_path.parent().unwrap().to_owned();
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        let bearer = format!("Bearer {}", token.expose_for_authorization());
        let (history, worker) = ChatHistory::spawn(history_path).unwrap();
        let gateway = gateway_state();
        let state = ChatRoutesState::new(token, history.clone(), gateway.clone());
        let proof_token = ControlToken::load(&root.join("control.token")).unwrap();
        let proof_router = Router::new().route(
            "/loxa/v1/node",
            get(move |headers: HeaderMap| {
                let token = proof_token.clone();
                async move {
                    let nonce = headers
                        .get("X-Loxa-Challenge")
                        .and_then(|value| value.to_str().ok())
                        .unwrap();
                    let status = loxa_core::control::contracts::NodeStatus::Ready;
                    let challenge_proof = token
                        .node_identity_proof(nonce, "route-test-node", "route-test-runtime", status)
                        .unwrap();
                    Json(json!({
                        "protocol_version": 1,
                        "node_id": "route-test-node",
                        "runtime_identity": "route-test-runtime",
                        "status": status,
                        "challenge_proof": challenge_proof,
                    }))
                }
            }),
        );
        let server = loxa_core::gateway::GatewayServer::start_with_router(
            0,
            gateway.clone(),
            proof_router.merge(router(state.clone())),
        )
        .unwrap();
        RouteFixture {
            base: format!("http://127.0.0.1:{}", server.port()),
            bearer,
            history,
            gateway,
            state,
            server: Some(server),
            worker: Some(worker),
            root,
        }
    }

    async fn publish_fake_engine(fixture: &RouteFixture, body: &'static str) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || async move {
                let mut response = Response::new(Body::from(body));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/event-stream"),
                );
                response
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        fixture.gateway.publish(loxa_core::gateway::EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "test".into(),
            engine_version: "1".into(),
            model_id: "recipe".into(),
            profile: "test".into(),
        });
    }

    async fn publish_stalled_engine(fixture: &RouteFixture) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                let pending = futures_util::stream::pending::<Result<Bytes, Infallible>>();
                let mut response = Response::new(Body::from_stream(pending));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/event-stream"),
                );
                response
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        fixture.gateway.publish(loxa_core::gateway::EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "test".into(),
            engine_version: "1".into(),
            model_id: "recipe".into(),
            profile: "test".into(),
        });
    }

    #[test]
    fn upstream_sse_requires_typed_json_or_done() {
        assert_eq!(
            parse_upstream_event(b"data: [DONE]\n\n"),
            Ok(UpstreamEvent::Done)
        );
        assert_eq!(
            parse_upstream_event(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"),
            Ok(UpstreamEvent::Chunk {
                delta: Some("hi".into()),
                finish_reason: None,
                output_tokens: None,
            })
        );
        assert_eq!(
            parse_upstream_event(
                b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":17}}\n\n"
            ),
            Ok(UpstreamEvent::Chunk {
                delta: None,
                finish_reason: Some("stop".into()),
                output_tokens: Some(17),
            })
        );
        assert_eq!(
            parse_upstream_event(b"data: not-json\n\n"),
            Err(UpstreamSseError::Malformed)
        );
        assert_eq!(
            parse_upstream_event(b"event: message\n\n"),
            Err(UpstreamSseError::Malformed)
        );
        assert_eq!(
            parse_upstream_event(b": keepalive\n\n"),
            Ok(UpstreamEvent::Ignored)
        );

        for malformed in [
            b"data: {\"choices\":[42]}\n\n".as_slice(),
            b"data: {\"choices\":[{\"delta\":42}]}\n\n".as_slice(),
            b"data: {\"choices\":[],\"usage\":42}\n\n".as_slice(),
            b"data: {\"choices\":[],\"usage\":{}}\n\n".as_slice(),
            b"data: {\"choices\":[],\"usage\":{\"completion_tokens\":\"17\"}}\n\n".as_slice(),
            b"data: {\"choices\":[],\"usage\":{\"completion_tokens\":null}}\n\n".as_slice(),
        ] {
            assert_eq!(
                parse_upstream_event(malformed),
                Err(UpstreamSseError::Malformed)
            );
        }
        assert_eq!(
            parse_upstream_event(b"data: {\"choices\":[],\"usage\":null}\n\n"),
            Ok(UpstreamEvent::Chunk {
                delta: None,
                finish_reason: None,
                output_tokens: None,
            })
        );
    }

    #[tokio::test]
    async fn completed_turn_emits_and_persists_exact_optional_metrics() {
        let fixture = route_fixture();
        publish_fake_engine(
            &fixture,
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"completion_tokens\":17}}\n\ndata: [DONE]\n\n",
        )
        .await;
        let chat = fixture.history.create_chat(1).await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id))
            .header(header::AUTHORIZATION, &fixture.bearer)
            .json(&json!({"model":"loxa","content":"hello"}))
            .send()
            .await
            .unwrap();
        let body = response.text().await.unwrap();
        let terminal = body
            .split("\n\n")
            .find(|event| event.starts_with("event: turn.completed"))
            .unwrap();
        let data: serde_json::Value = serde_json::from_str(
            terminal
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(data["metrics"]["output_tokens"], 17);
        assert_eq!(data["metrics"]["stop_reason"], "stop");
        assert!(data["metrics"]["ttft_ms"].is_u64());
        assert!(data["metrics"]["total_duration_ms"].is_u64());

        let stored = fixture
            .history
            .list_turns(chat.id, LIST_DEFAULT_LIMIT, None)
            .await
            .unwrap()
            .turns
            .pop()
            .unwrap();
        assert_eq!(stored.metrics.output_tokens, Some(17));
        assert_eq!(stored.metrics.stop_reason.as_deref(), Some("stop"));
        assert!(stored.metrics.ttft_ms.is_some());
        assert!(stored.metrics.total_duration_ms.is_some());
        fixture.shutdown();
    }

    #[test]
    fn persistent_chat_diagnostics_emit_only_admission_and_post_persistence_terminal() {
        const ISOLATED: &str = "LOXA_CHAT_TERMINAL_DIAGNOSTICS_TEST_CHILD";
        if run_isolated_capture_test(
            "chat_routes::tests::persistent_chat_diagnostics_emit_only_admission_and_post_persistence_terminal",
            ISOLATED,
        ) {
            return;
        }
        let capture = EventCapture::default();
        let output = Arc::clone(&capture.0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tracing::subscriber::with_default(capture, || {
            runtime.block_on(async {
            let path = temp_history_path();
            let root = path.parent().unwrap().to_owned();
            let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
            let bearer = format!("Bearer {}", token.expose_for_authorization());
            let (history, worker) = ChatHistory::spawn(path).unwrap();
            let gateway = gateway_state();
            let state = ChatRoutesState::new(token, history.clone(), gateway.clone());
            let fixture = RouteFixture {
                base: String::new(),
                bearer: bearer.clone(),
                history: history.clone(),
                gateway,
                state: state.clone(),
                server: None,
                worker: None,
                root: root.clone(),
            };
            for (index, (engine_body, prompt, expected_state)) in [
                (
                    "data: {\"choices\":[{\"delta\":{\"content\":\"SECRET_RESPONSE_TOKEN\"}}]}\n\ndata: [DONE]\n\n",
                    "SECRET_PROMPT_TOKEN /private/owner/chat",
                    TurnState::Completed,
                ),
                (
                    "data: SECRET_RAW_ERROR /private/owner/upstream\n\n",
                    "failure prompt",
                    TurnState::Failed,
                ),
            ]
            .into_iter()
            .enumerate()
            {
                publish_fake_engine(&fixture, engine_body).await;
                let chat = history.create_chat(1).await.unwrap();
                let mut request = axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(format!("/loxa/v1/chats/{}/turns", chat.id))
                    .header(header::AUTHORIZATION, &bearer)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"model":"loxa","content":prompt}).to_string(),
                    ))
                    .unwrap();
                if index == 0 {
                    request.extensions_mut().insert(
                        crate::http_observability::DiagnosticRequestId(
                            "present-correlation".to_owned(),
                        ),
                    );
                }
                let response = router(state.clone()).oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let stored = history
                    .list_turns(chat.id, LIST_DEFAULT_LIMIT, None)
                    .await
                    .unwrap()
                    .turns
                    .pop()
                    .unwrap();
                assert_eq!(stored.state, expected_state);
            }
            publish_stalled_engine(&fixture).await;
            let cancelled_chat = history.create_chat(2).await.unwrap();
            let request = axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/loxa/v1/chats/{}/turns",
                    cancelled_chat.id
                ))
                .header(header::AUTHORIZATION, &bearer)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"model":"loxa","content":"cancelled prompt"}).to_string(),
                ))
                .unwrap();
            let response = router(state.clone()).oneshot(request).await.unwrap();
            let cancel = state
                .active
                .state
                .lock()
                .unwrap()
                .running
                .get(cancelled_chat.id.as_str())
                .unwrap()
                .cancel
                .clone();
            cancel.send_replace(true);
            let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let stored = history
                .list_turns(cancelled_chat.id, LIST_DEFAULT_LIMIT, None)
                .await
                .unwrap()
                .turns
                .pop()
                .unwrap();
            assert_eq!(stored.state, TurnState::Cancelled);
            state.shutdown_and_wait();
            worker.stop_and_join().unwrap();
            std::fs::remove_dir_all(root).unwrap();
        })
        });
        let events = output.lock().unwrap();
        let diagnostic: Vec<_> = events
            .iter()
            .filter(|event| {
                event
                    .fields
                    .get("event_code")
                    .is_some_and(|code| code.starts_with("chat.turn."))
            })
            .collect();
        assert_eq!(diagnostic.len(), 6, "{diagnostic:?}");
        assert_eq!(diagnostic[0].fields["event_code"], "chat.turn.started");
        assert_eq!(diagnostic[0].fields["request_id"], "present-correlation");
        assert_eq!(diagnostic[1].fields["event_code"], "chat.turn.terminal");
        assert_eq!(diagnostic[1].fields["request_id"], "present-correlation");
        assert_eq!(diagnostic[1].fields["result_class"], "completed");
        assert_eq!(diagnostic[2].fields["event_code"], "chat.turn.started");
        assert!(!diagnostic[2].fields.contains_key("request_id"));
        assert_eq!(diagnostic[3].fields["event_code"], "chat.turn.terminal");
        assert!(!diagnostic[3].fields.contains_key("request_id"));
        assert_eq!(diagnostic[3].fields["result_class"], "failed");
        assert_eq!(diagnostic[4].fields["event_code"], "chat.turn.started");
        assert!(!diagnostic[4].fields.contains_key("request_id"));
        assert_eq!(diagnostic[5].fields["event_code"], "chat.turn.terminal");
        assert!(!diagnostic[5].fields.contains_key("request_id"));
        assert_eq!(diagnostic[5].fields["result_class"], "cancelled");
        assert!(diagnostic.iter().all(|event| {
            event.target == "loxa_node::chat" && event.level == tracing::Level::INFO
        }));
        let rendered = format!("{diagnostic:?}");
        for forbidden in [
            "SECRET_PROMPT_TOKEN",
            "SECRET_RESPONSE_TOKEN",
            "SECRET_RAW_ERROR",
            "/private/owner/chat",
            "/private/owner/upstream",
            "turn.delta",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "leaked {forbidden}: {rendered}"
            );
        }
    }

    #[test]
    fn accepted_chat_cancellation_emits_one_safe_request_diagnostic() {
        const ISOLATED: &str = "LOXA_CHAT_CANCEL_DIAGNOSTICS_TEST_CHILD";
        if run_isolated_capture_test(
            "chat_routes::tests::accepted_chat_cancellation_emits_one_safe_request_diagnostic",
            ISOLATED,
        ) {
            return;
        }
        let capture = EventCapture::default();
        let output = Arc::clone(&capture.0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tracing::subscriber::with_default(capture, || {
            runtime.block_on(async {
                let path = temp_history_path();
                let root = path.parent().unwrap().to_owned();
                let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
                let bearer = format!("Bearer {}", token.expose_for_authorization());
                let (history, worker) = ChatHistory::spawn(path).unwrap();
                let state = ChatRoutesState::new(token, history.clone(), gateway_state());
                let chat = history.create_chat(1).await.unwrap();
                let turn = history
                    .begin_turn(
                        chat.id.clone(),
                        MessageContent::user("SECRET_PROMPT_TOKEN").unwrap(),
                        TurnProvenance::new("recipe", Some("engine"), Some("1")).unwrap(),
                        2,
                    )
                    .await
                    .unwrap();
                let (cancel, _cancelled) = tokio::sync::watch::channel(false);
                state.active.state.lock().unwrap().running.insert(
                    chat.id.to_string(),
                    ActiveTurn {
                        turn_id: turn.id.clone(),
                        cancel,
                    },
                );
                let request = axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(format!(
                        "/loxa/v1/chats/{}/turns/{}/cancel",
                        chat.id, turn.id
                    ))
                    .header(header::AUTHORIZATION, bearer)
                    .body(Body::empty())
                    .unwrap();
                let response = router(state.clone()).oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::ACCEPTED);
                state.active.state.lock().unwrap().running.clear();
                worker.stop_and_join().unwrap();
                std::fs::remove_dir_all(root).unwrap();
            })
        });
        let events = output.lock().unwrap();
        let diagnostic: Vec<_> = events
            .iter()
            .filter(|event| {
                event.fields.get("event_code").map(String::as_str)
                    == Some("chat.turn.cancel_requested")
            })
            .collect();
        assert_eq!(diagnostic.len(), 1, "{diagnostic:?}");
        assert_eq!(diagnostic[0].target, "loxa_node::chat");
        assert_eq!(diagnostic[0].level, tracing::Level::INFO);
        assert_eq!(diagnostic[0].fields["result_class"], "accepted");
        assert!(!format!("{diagnostic:?}").contains("SECRET_PROMPT_TOKEN"));
    }

    #[test]
    fn failed_chat_persistence_emits_no_terminal_diagnostic() {
        const ISOLATED: &str = "LOXA_CHAT_PERSISTENCE_DIAGNOSTICS_TEST_CHILD";
        if run_isolated_capture_test(
            "chat_routes::tests::failed_chat_persistence_emits_no_terminal_diagnostic",
            ISOLATED,
        ) {
            return;
        }
        let capture = EventCapture::default();
        let output = Arc::clone(&capture.0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tracing::subscriber::with_default(capture, || {
            runtime.block_on(async {
                let path = temp_history_path();
                let root = path.parent().unwrap().to_owned();
                let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
                let bearer = format!("Bearer {}", token.expose_for_authorization());
                let (history, worker) = ChatHistory::spawn(path).unwrap();
                let gateway = gateway_state();
                let state = ChatRoutesState::new(token, history.clone(), gateway.clone());
                let fixture = RouteFixture {
                    base: String::new(),
                    bearer: bearer.clone(),
                    history: history.clone(),
                    gateway,
                    state: state.clone(),
                    server: None,
                    worker: None,
                    root: root.clone(),
                };
                publish_stalled_engine(&fixture).await;
                let chat = history.create_chat(1).await.unwrap();
                let request = axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(format!("/loxa/v1/chats/{}/turns", chat.id))
                    .header(header::AUTHORIZATION, bearer)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"model":"loxa","content":"SECRET_PROMPT_TOKEN"}).to_string(),
                    ))
                    .unwrap();
                let response = router(state.clone()).oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                history.fail_next_finalize_for_test().await.unwrap();
                let (turn_id, cancel) = {
                    let active = state.active.state.lock().unwrap();
                    let active = active.running.get(chat.id.as_str()).unwrap();
                    (active.turn_id.clone(), active.cancel.clone())
                };
                cancel.send_replace(true);
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                assert!(String::from_utf8_lossy(&body).contains("history_write_failed"));
                assert_eq!(
                    history.get_turn(turn_id).await.unwrap().state,
                    TurnState::Queued
                );
                state.shutdown_and_wait();
                worker.stop_and_join().unwrap();
                std::fs::remove_dir_all(root).unwrap();
            })
        });
        let events = output.lock().unwrap();
        let diagnostic: Vec<_> = events
            .iter()
            .filter(|event| {
                event
                    .fields
                    .get("event_code")
                    .is_some_and(|code| code.starts_with("chat.turn."))
            })
            .collect();
        assert_eq!(diagnostic.len(), 1, "{diagnostic:?}");
        assert_eq!(diagnostic[0].fields["event_code"], "chat.turn.started");
        assert!(!format!("{diagnostic:?}").contains("SECRET_PROMPT_TOKEN"));
    }

    #[tokio::test]
    async fn persistent_turn_requests_streamed_usage_from_the_engine() {
        let fixture = route_fixture();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let request_tx = Arc::new(Mutex::new(Some(request_tx)));
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |Json(request): Json<serde_json::Value>| {
                let request_tx = request_tx.clone();
                async move {
                    if let Some(sender) = request_tx.lock().unwrap().take() {
                        let _ = sender.send(request);
                    }
                    let mut response = Response::new(Body::from("data: [DONE]\n\n"));
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    response
                }
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        fixture.gateway.publish(loxa_core::gateway::EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "backend".into(),
            engine: "test".into(),
            engine_version: "1".into(),
            model_id: "recipe".into(),
            profile: "test".into(),
        });
        let chat = fixture.history.create_chat(1).await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id))
            .header(header::AUTHORIZATION, &fixture.bearer)
            .json(&json!({"model":"loxa","content":"hello"}))
            .send()
            .await
            .unwrap();
        let _ = response.text().await.unwrap();
        let request = request_rx.await.unwrap();
        assert_eq!(request["stream_options"]["include_usage"], true);
        fixture.shutdown();
    }

    #[test]
    fn context_budget_keeps_complete_recent_turns_and_counts_omissions() {
        let old = vec![
            ContextTurn {
                user: "old user".into(),
                assistant: "old answer".into(),
            },
            ContextTurn {
                user: "recent user".into(),
                assistant: "recent answer".into(),
            },
        ];
        let (messages, omitted) = budget_context(old, "current", 150).unwrap();
        assert_eq!(omitted, 1);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["content"], "recent user");
        assert_eq!(messages[2]["content"], "current");
    }

    #[test]
    fn context_is_capped_at_32_complete_turn_pairs() {
        let turns = (0..40)
            .map(|index| ContextTurn {
                user: format!("user {index}"),
                assistant: format!("answer {index}"),
            })
            .collect();
        let (messages, omitted) = budget_context(turns, "current", usize::MAX).unwrap();
        assert_eq!(omitted, 8);
        assert_eq!(messages.len(), 65);
        assert_eq!(messages[0]["content"], "user 8");
        assert_eq!(messages[64]["content"], "current");
    }

    #[test]
    fn dropping_the_response_stream_immediately_requests_cancellation() {
        let (cancel, cancelled) = tokio::sync::watch::channel(false);
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(GuardedReceiverStream { receiver, cancel });
        assert!(*cancelled.borrow());
    }

    #[tokio::test]
    async fn live_client_cancellation_drives_real_worker_to_durable_cancelled_state() {
        let fixture = route_fixture();
        publish_stalled_engine(&fixture).await;
        let address = fixture
            .base
            .strip_prefix("http://")
            .unwrap()
            .parse()
            .unwrap();
        let token_path = fixture.root.join("control.token");
        let control_client = loxa_core::control::client::LiveControlClient::connect(
            address,
            ControlToken::load(&token_path).unwrap(),
            "route-test-runtime",
            Duration::from_secs(1),
        )
        .unwrap();
        let chat = control_client.create_chat().unwrap();
        let stream_client = loxa_core::control::client::LiveControlClient::connect(
            address,
            ControlToken::load(&token_path).unwrap(),
            "route-test-runtime",
            Duration::from_secs(1),
        )
        .unwrap();
        let stream_chat = chat.id.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let stream_worker = std::thread::spawn(move || {
            stream_client.stream_turn(&stream_chat, "cancel me", |event| {
                if let loxa_core::control::client::TurnStreamEvent::Started {
                    chat_id,
                    turn_id,
                    ..
                } = &event
                {
                    started_tx.send((chat_id.clone(), turn_id.clone())).unwrap();
                }
                Ok(())
            })
        });
        let (started_chat, started_turn) = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(started_chat, chat.id);

        control_client
            .cancel_turn(&started_chat, &started_turn)
            .unwrap();
        let terminal = stream_worker.join().unwrap().unwrap();
        assert!(matches!(
            terminal,
            loxa_core::control::client::TurnStreamEvent::Terminal {
                ref turn_id,
                ref state,
                error_code: None,
                ..
            } if turn_id == &started_turn && state == "cancelled"
        ));
        let stored = fixture
            .history
            .get_turn(TurnId::parse(&started_turn).unwrap())
            .await
            .unwrap();
        assert_eq!(stored.id.as_str(), started_turn);
        assert_eq!(stored.state, TurnState::Cancelled);
        fixture.shutdown();
    }

    #[tokio::test]
    async fn conversation_context_excludes_non_completed_turns() {
        let path = temp_history_path();
        let (history, worker) = ChatHistory::spawn(path.clone()).unwrap();
        let chat = history.create_chat(1).await.unwrap();
        let provenance = TurnProvenance::new("recipe", Some("engine"), Some("1")).unwrap();
        let completed = history
            .begin_turn(
                chat.id.clone(),
                MessageContent::user("kept user").unwrap(),
                provenance.clone(),
                2,
            )
            .await
            .unwrap();
        history
            .finalize(
                completed.id,
                TurnState::Completed,
                MessageContent::assistant("kept answer").unwrap(),
                None,
                TurnMetrics::default(),
                3,
            )
            .await
            .unwrap();
        let cancelled = history
            .begin_turn(
                chat.id.clone(),
                MessageContent::user("excluded user").unwrap(),
                provenance,
                4,
            )
            .await
            .unwrap();
        history
            .finalize(
                cancelled.id,
                TurnState::Cancelled,
                MessageContent::assistant("excluded answer").unwrap(),
                None,
                TurnMetrics::default(),
                5,
            )
            .await
            .unwrap();

        let (messages, omitted) = conversation_context(&history, chat.id, "current")
            .await
            .unwrap();
        assert_eq!(omitted, 0);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["content"], "kept user");
        assert_eq!(messages[1]["content"], "kept answer");
        assert_eq!(messages[2]["content"], "current");

        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn route_specific_preflight_rejects_methods_not_owned_by_the_route() {
        assert!(preflight_method_allowed("GET", &["GET"]));
        assert!(!preflight_method_allowed("DELETE", &["GET"]));
        assert!(preflight_method_allowed(
            "PATCH",
            &["GET", "PATCH", "DELETE"]
        ));
    }

    #[tokio::test]
    async fn routes_enforce_specific_cors_and_turn_body_limit() {
        let fixture = route_fixture();
        let client = reqwest::Client::new();
        let chat = fixture.history.create_chat(1).await.unwrap();

        let denied = client
            .request(
                Method::OPTIONS,
                format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id),
            )
            .header(header::ORIGIN, "tauri://localhost")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "DELETE")
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let allowed = client
            .request(
                Method::OPTIONS,
                format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id),
            )
            .header(header::ORIGIN, "tauri://localhost")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "authorization, content-type",
            )
            .send()
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            allowed
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .unwrap(),
            "GET, POST, OPTIONS"
        );

        let oversized = client
            .post(format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id))
            .header(header::AUTHORIZATION, &fixture.bearer)
            .header(header::ORIGIN, "tauri://localhost")
            .body("x".repeat(TURN_BODY_MAX_BYTES + 1))
            .send()
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            oversized
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "tauri://localhost"
        );
        fixture.shutdown();
    }

    #[tokio::test]
    async fn nested_message_routes_enforce_chat_and_turn_parent_ids() {
        let fixture = route_fixture();
        let first = fixture.history.create_chat(1).await.unwrap();
        let second = fixture.history.create_chat(2).await.unwrap();
        let turn = fixture
            .history
            .begin_turn(
                first.id.clone(),
                MessageContent::user("hello").unwrap(),
                TurnProvenance::new("recipe", Some("engine"), Some("1")).unwrap(),
                3,
            )
            .await
            .unwrap();
        fixture
            .history
            .finalize(
                turn.id.clone(),
                TurnState::Completed,
                MessageContent::assistant("world").unwrap(),
                None,
                TurnMetrics::default(),
                4,
            )
            .await
            .unwrap();
        let message = fixture
            .history
            .message_summaries(turn.id.clone())
            .await
            .unwrap()[0]
            .id
            .clone();
        let client = reqwest::Client::new();
        for path in [
            format!("/loxa/v1/chats/{}/turns/{}/messages", second.id, turn.id),
            format!(
                "/loxa/v1/chats/{}/turns/{}/messages/{}",
                second.id, turn.id, message
            ),
        ] {
            let response = client
                .get(format!("{}{path}", fixture.base))
                .header(header::AUTHORIZATION, &fixture.bearer)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
        fixture.shutdown();
    }

    #[tokio::test]
    async fn upstream_eof_without_done_persists_a_failed_terminal_turn() {
        let fixture = route_fixture();
        publish_fake_engine(
            &fixture,
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        )
        .await;
        let chat = fixture.history.create_chat(1).await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/loxa/v1/chats/{}/turns", fixture.base, chat.id))
            .header(header::AUTHORIZATION, &fixture.bearer)
            .json(&json!({"model":"loxa","content":"hello"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.unwrap();
        assert!(body.contains("event: turn.failed"));
        assert!(body.contains("incomplete_upstream_stream"));

        let turn = fixture
            .history
            .list_turns(chat.id, LIST_DEFAULT_LIMIT, None)
            .await
            .unwrap()
            .turns
            .pop()
            .unwrap();
        assert_eq!(turn.state, TurnState::Failed);
        assert_eq!(
            turn.error_code.as_deref(),
            Some("incomplete_upstream_stream")
        );
        fixture.shutdown();
    }

    #[tokio::test]
    async fn shutdown_cancels_a_turn_blocked_by_full_downstream_backpressure() {
        let path = temp_history_path();
        let root = path.parent().unwrap().to_owned();
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        let (history, worker) = ChatHistory::spawn(path).unwrap();
        let state = ChatRoutesState::new(token, history.clone(), gateway_state());
        let chat = history.create_chat(1).await.unwrap();
        let turn = history
            .begin_turn(
                chat.id.clone(),
                MessageContent::user("hello").unwrap(),
                TurnProvenance::new("recipe", Some("engine"), Some("1")).unwrap(),
                2,
            )
            .await
            .unwrap();
        let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
        state.active.state.lock().unwrap().running.insert(
            chat.id.to_string(),
            ActiveTurn {
                turn_id: turn.id.clone(),
                cancel,
            },
        );
        let (sender, _stalled_receiver) =
            tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(32);
        for _ in 0..32 {
            sender.try_send(Ok(Bytes::from_static(b"full"))).unwrap();
        }
        let task_state = state.clone();
        let chat_id = chat.id.clone();
        let turn_id = turn.id.clone();
        tokio::spawn(async move {
            assert!(
                !send_downstream(&sender, &mut cancelled, Bytes::from_static(b"blocked")).await
            );
            task_state
                .history
                .finalize(
                    turn_id,
                    TurnState::Cancelled,
                    MessageContent::assistant("").unwrap(),
                    None,
                    TurnMetrics::default(),
                    3,
                )
                .await
                .unwrap();
            let mut active = task_state.active.state.lock().unwrap();
            active.running.remove(chat_id.as_str());
            task_state.active.empty.notify_all();
        });

        let shutdown_state = state.clone();
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || shutdown_state.shutdown_and_wait()),
        )
        .await
        .expect("shutdown must not deadlock on a full downstream channel")
        .unwrap();
        assert_eq!(
            history.get_turn(turn.id).await.unwrap().state,
            TurnState::Cancelled
        );
        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
