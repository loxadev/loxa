//! Typed, fail-closed domain primitives for Loxa-owned chat history.
//!
//! This module deliberately contains no HTTP, task, or Tokio code. The node
//! owns the worker and authenticated API; this crate owns only data that is
//! safe to persist and query through fixed SQLite statements.

mod migrations;
mod repository;

pub use repository::{ChatHistoryRepository, HistoryPage, MessagePage, TurnPage};

use serde::Serialize;
use std::fmt;

pub const CHAT_ID_HEX_LEN: usize = 32;
pub const USER_CONTENT_MAX_BYTES: usize = 64 * 1024;
pub const ASSISTANT_CONTENT_MAX_BYTES: usize = 2 * 1024 * 1024;
pub const TITLE_MAX_SCALARS: usize = 160;
pub const FIRST_TURN_TITLE_MAX_SCALARS: usize = 80;
pub const LIST_DEFAULT_LIMIT: usize = 30;
pub const LIST_MAX_LIMIT: usize = 100;
pub const RESPONSE_SEGMENT_MAX_BYTES: usize = 128 * 1024;
pub const MAX_SERIALIZED_MESSAGE_PAGE_BYTES: usize = 900 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryError {
    InvalidId,
    InvalidTitle,
    InvalidContent,
    InvalidTimestamp,
    InvalidCursor,
    InvalidPageLimit,
    InvalidTurnState,
    InvalidMessageRole,
    InvalidMetadata,
    NotFound,
    Conflict,
    CorruptDatabase,
    UnsupportedSchema,
    Security,
    Database,
    Randomness,
}

impl fmt::Display for HistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::InvalidId => "invalid chat history identifier",
            Self::InvalidTitle => "invalid chat history title",
            Self::InvalidContent => "invalid chat history content",
            Self::InvalidTimestamp => "invalid chat history timestamp",
            Self::InvalidCursor => "invalid chat history cursor",
            Self::InvalidPageLimit => "invalid chat history page limit",
            Self::InvalidTurnState => "invalid chat history turn state",
            Self::InvalidMessageRole => "invalid chat history message role",
            Self::InvalidMetadata => "invalid chat history metadata",
            Self::NotFound => "chat history record was not found",
            Self::Conflict => "chat history operation conflicts with current state",
            Self::CorruptDatabase => "chat history database failed integrity validation",
            Self::UnsupportedSchema => "chat history database schema is unsupported",
            Self::Security => "chat history storage permissions are unsafe",
            Self::Database => "chat history database operation failed",
            Self::Randomness => "chat history identifier generation failed",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for HistoryError {}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: &str) -> Result<Self, HistoryError> {
                let valid = value.len() == CHAT_ID_HEX_LEN
                    && value.bytes().all(|byte| {
                        byte.is_ascii_digit()
                            || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
                    });
                if valid {
                    Ok(Self(value.to_owned()))
                } else {
                    Err(HistoryError::InvalidId)
                }
            }

            pub fn generate() -> Result<Self, HistoryError> {
                let mut bytes = [0_u8; CHAT_ID_HEX_LEN / 2];
                getrandom::fill(&mut bytes).map_err(|_| HistoryError::Randomness)?;
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let mut value = String::with_capacity(CHAT_ID_HEX_LEN);
                for byte in bytes {
                    value.push(HEX[(byte >> 4) as usize] as char);
                    value.push(HEX[(byte & 0x0f) as usize] as char);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

opaque_id!(ChatId);
opaque_id!(TurnId);
opaque_id!(MessageId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Title(String);

impl Title {
    pub fn new(value: &str) -> Result<Self, HistoryError> {
        if value.is_empty()
            || value.trim().is_empty()
            || value.contains('\0')
            || value.chars().count() > TITLE_MAX_SCALARS
        {
            return Err(HistoryError::InvalidTitle);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn provisional() -> Self {
        Self("New chat".to_owned())
    }

    pub fn from_first_user_message(value: &str) -> Option<Self> {
        let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.is_empty() || collapsed.contains('\0') {
            return None;
        }
        let title = collapsed
            .chars()
            .take(FIRST_TURN_TITLE_MAX_SCALARS)
            .collect::<String>();
        Self::new(&title).ok()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct MessageContent(String);

impl MessageContent {
    pub fn user(value: &str) -> Result<Self, HistoryError> {
        Self::validate(value, USER_CONTENT_MAX_BYTES, true)
    }

    pub fn assistant(value: &str) -> Result<Self, HistoryError> {
        Self::validate(value, ASSISTANT_CONTENT_MAX_BYTES, false)
    }

    fn validate(value: &str, max_bytes: usize, nonblank: bool) -> Result<Self, HistoryError> {
        if value.contains('\0') || value.len() > max_bytes || (nonblank && value.trim().is_empty())
        {
            return Err(HistoryError::InvalidContent);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ChatSummary {
    pub id: ChatId,
    pub title: Title,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// URL-safe opaque keyset cursor for descending `(updated_at_ms, id)` pages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ChatCursor(String);

impl ChatCursor {
    pub(crate) fn from_key(updated_at_ms: i64, id: &ChatId) -> Self {
        let mut bytes = [0_u8; 24];
        bytes[..8].copy_from_slice(&updated_at_ms.to_be_bytes());
        for (index, pair) in id.as_str().as_bytes().chunks_exact(2).enumerate() {
            bytes[8 + index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
        }
        Self(base64url_encode(&bytes))
    }

    pub fn parse(value: &str) -> Result<Self, HistoryError> {
        if value.len() > 256 || value.len() != 32 {
            return Err(HistoryError::InvalidCursor);
        }
        let bytes = base64url_decode(value)?;
        if bytes.len() != 24 {
            return Err(HistoryError::InvalidCursor);
        }
        let mut timestamp = [0_u8; 8];
        timestamp.copy_from_slice(&bytes[..8]);
        let updated_at_ms = i64::from_be_bytes(timestamp);
        if updated_at_ms < 0 {
            return Err(HistoryError::InvalidCursor);
        }
        let mut id = String::with_capacity(CHAT_ID_HEX_LEN);
        for byte in &bytes[8..] {
            use std::fmt::Write;
            let _ = write!(&mut id, "{byte:02x}");
        }
        ChatId::parse(&id).map_err(|_| HistoryError::InvalidCursor)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn key(&self) -> Result<(i64, ChatId), HistoryError> {
        let parsed = Self::parse(&self.0)?;
        let bytes = base64url_decode(parsed.as_str())?;
        let mut timestamp = [0_u8; 8];
        timestamp.copy_from_slice(&bytes[..8]);
        let mut id = String::with_capacity(CHAT_ID_HEX_LEN);
        for byte in &bytes[8..] {
            use std::fmt::Write;
            let _ = write!(&mut id, "{byte:02x}");
        }
        Ok((i64::from_be_bytes(timestamp), ChatId::parse(&id)?))
    }
}

/// URL-safe opaque keyset cursor for ascending turn ordinals within one chat.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct TurnCursor(String);

impl TurnCursor {
    pub(crate) fn from_ordinal(ordinal: i64) -> Self {
        Self(base64url_encode(&ordinal.to_be_bytes()))
    }

    pub fn parse(value: &str) -> Result<Self, HistoryError> {
        if value.len() > 256 || value.len() != 11 {
            return Err(HistoryError::InvalidCursor);
        }
        let bytes = base64url_decode(value)?;
        if bytes.len() != 8 {
            return Err(HistoryError::InvalidCursor);
        }
        let mut ordinal = [0_u8; 8];
        ordinal.copy_from_slice(&bytes);
        if i64::from_be_bytes(ordinal) < 0 {
            return Err(HistoryError::InvalidCursor);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn ordinal(&self) -> Result<i64, HistoryError> {
        let parsed = Self::parse(&self.0)?;
        let bytes = base64url_decode(parsed.as_str())?;
        let mut ordinal = [0_u8; 8];
        ordinal.copy_from_slice(&bytes);
        Ok(i64::from_be_bytes(ordinal))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TurnProvenance {
    pub model_alias: &'static str,
    pub recipe_id: String,
    pub engine_name: Option<String>,
    pub engine_version: Option<String>,
}

impl TurnProvenance {
    pub fn new(
        recipe_id: &str,
        engine_name: Option<&str>,
        engine_version: Option<&str>,
    ) -> Result<Self, HistoryError> {
        validate_metadata(recipe_id, 256, true)?;
        if let Some(value) = engine_name {
            validate_metadata(value, 128, true)?;
        }
        if let Some(value) = engine_version {
            validate_metadata(value, 128, true)?;
        }
        Ok(Self {
            model_alias: "loxa",
            recipe_id: recipe_id.to_owned(),
            engine_name: engine_name.map(str::to_owned),
            engine_version: engine_version.map(str::to_owned),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TurnRecord {
    pub id: TurnId,
    pub chat_id: ChatId,
    pub ordinal: i64,
    pub state: TurnState,
    pub provenance: TurnProvenance,
    pub error_code: Option<String>,
    pub metrics: TurnMetrics,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TurnMetrics {
    pub output_tokens: Option<u64>,
    pub total_duration_ms: Option<u64>,
    pub ttft_ms: Option<u64>,
    pub stop_reason: Option<String>,
}

impl TurnMetrics {
    pub fn new(
        output_tokens: Option<u64>,
        total_duration_ms: Option<u64>,
        ttft_ms: Option<u64>,
        stop_reason: Option<&str>,
    ) -> Result<Self, HistoryError> {
        if output_tokens.is_some_and(|value| value > i64::MAX as u64)
            || total_duration_ms.is_some_and(|value| value > i64::MAX as u64)
            || ttft_ms.is_some_and(|value| value > i64::MAX as u64)
        {
            return Err(HistoryError::InvalidMetadata);
        }
        if matches!((total_duration_ms, ttft_ms), (Some(total), Some(ttft)) if ttft > total) {
            return Err(HistoryError::InvalidMetadata);
        }
        if let Some(value) = stop_reason {
            validate_metadata(value, 128, true)?;
        }
        Ok(Self {
            output_tokens,
            total_duration_ms,
            ttft_ms,
            stop_reason: stop_reason.map(str::to_owned),
        })
    }

    pub(crate) fn validate(&self) -> Result<(), HistoryError> {
        Self::new(
            self.output_tokens,
            self.total_duration_ms,
            self.ttft_ms,
            self.stop_reason.as_deref(),
        )
        .map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MessageRecord {
    pub id: MessageId,
    pub turn_id: TurnId,
    pub role: MessageRole,
    pub content: MessageContent,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MessageSummary {
    pub id: MessageId,
    pub turn_id: TurnId,
    pub role: MessageRole,
    pub content_bytes: usize,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MessageSegment {
    pub message_id: MessageId,
    pub turn_id: TurnId,
    pub role: MessageRole,
    pub segment_index: u32,
    pub segment_count: u32,
    pub content: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnState {
    Queued,
    Streaming,
    Completed,
    Cancelled,
    Failed,
}

impl TurnState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Streaming => "streaming",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, HistoryError> {
        match value {
            "queued" => Ok(Self::Queued),
            "streaming" => Ok(Self::Streaming),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(HistoryError::InvalidTurnState),
        }
    }

    pub fn is_interrupted(self) -> bool {
        matches!(self, Self::Queued | Self::Streaming)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, HistoryError> {
        match value {
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            _ => Err(HistoryError::InvalidMessageRole),
        }
    }
}

pub(crate) fn valid_timestamp_pair(created_at_ms: i64, updated_at_ms: i64) -> bool {
    created_at_ms >= 0 && updated_at_ms >= created_at_ms
}

pub(crate) fn validate_metadata(
    value: &str,
    max_bytes: usize,
    required: bool,
) -> Result<(), HistoryError> {
    if value.contains('\0') || value.len() > max_bytes || (required && value.trim().is_empty()) {
        return Err(HistoryError::InvalidMetadata);
    }
    Ok(())
}

pub(crate) fn validate_error_code(value: Option<&str>) -> Result<(), HistoryError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(HistoryError::InvalidMetadata);
    }
    Ok(())
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

fn base64url_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut result = String::with_capacity((bytes.len() * 4).div_ceil(3));
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        result.push(ALPHABET[((value >> 18) & 0x3f) as usize] as char);
        result.push(ALPHABET[((value >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((value >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[(value & 0x3f) as usize] as char);
        }
    }
    result
}

fn base64url_decode(value: &str) -> Result<Vec<u8>, HistoryError> {
    if value.is_empty() || value.len() % 4 == 1 {
        return Err(HistoryError::InvalidCursor);
    }
    let mut decoded = Vec::with_capacity(value.len() * 3 / 4);
    let source = value.as_bytes();
    for chunk in source.chunks(4) {
        let first = base64url_value(chunk[0])?;
        let second = chunk
            .get(1)
            .ok_or(HistoryError::InvalidCursor)
            .and_then(|byte| base64url_value(*byte))?;
        let third = chunk.get(2).copied().map(base64url_value).transpose()?;
        let fourth = chunk.get(3).copied().map(base64url_value).transpose()?;
        if chunk.len() == 2 && (second & 0x0f) != 0 {
            return Err(HistoryError::InvalidCursor);
        }
        if chunk.len() == 3 && (third.ok_or(HistoryError::InvalidCursor)? & 0x03) != 0 {
            return Err(HistoryError::InvalidCursor);
        }
        decoded.push(((u16::from(first) << 2) | u16::from(second >> 4)) as u8);
        if let Some(third) = third {
            decoded.push(((u16::from(second) << 4) | u16::from(third >> 2)) as u8);
        }
        if let Some(fourth) = fourth {
            decoded.push(
                ((u16::from(third.ok_or(HistoryError::InvalidCursor)?) << 6) | u16::from(fourth))
                    as u8,
            );
        }
    }
    Ok(decoded)
}

fn base64url_value(byte: u8) -> Result<u8, HistoryError> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'-' => Ok(62),
        b'_' => Ok(63),
        _ => Err(HistoryError::InvalidCursor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_text_reject_invalid_bytes_and_preserve_unicode_bounds() {
        assert!(ChatId::parse("0123456789abcdef0123456789abcdef").is_ok());
        assert!(ChatId::parse("0123456789ABCDEF0123456789abcdef").is_err());
        assert!(ChatId::parse("short").is_err());

        assert!(Title::new("A title").is_ok());
        assert!(Title::new("   \t").is_err());
        assert!(Title::new("contains\0nul").is_err());
        assert!(Title::new(&"a".repeat(161)).is_err());
        assert!(MessageContent::user("🙂").is_ok());
        assert!(MessageContent::user("contains\0nul").is_err());
        assert!(MessageContent::assistant("contains\0nul").is_err());
        assert!(MessageContent::user(&"a".repeat(USER_CONTENT_MAX_BYTES + 1)).is_err());
        assert!(MessageContent::assistant(&"a".repeat(ASSISTANT_CONTENT_MAX_BYTES + 1)).is_err());
    }

    #[test]
    fn turn_metrics_preserve_nullable_values_and_bound_stop_reason() {
        let metrics = TurnMetrics::new(Some(42), Some(1_250), Some(85), Some("stop")).unwrap();
        assert_eq!(metrics.output_tokens, Some(42));
        assert_eq!(metrics.total_duration_ms, Some(1_250));
        assert_eq!(metrics.ttft_ms, Some(85));
        assert_eq!(metrics.stop_reason.as_deref(), Some("stop"));

        assert_eq!(
            TurnMetrics::default(),
            TurnMetrics::new(None, None, None, None).unwrap()
        );
        assert!(TurnMetrics::new(None, None, None, Some("")).is_err());
        assert!(TurnMetrics::new(None, None, None, Some("contains\0nul")).is_err());
        assert!(TurnMetrics::new(None, None, None, Some(&"x".repeat(129))).is_err());
        assert!(TurnMetrics::new(None, Some(10), Some(11), None).is_err());
    }
}
