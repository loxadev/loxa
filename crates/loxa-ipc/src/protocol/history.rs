use super::{validate_decimal, validate_identifier, validate_model_id, MAX_ERROR_CONTEXT_BYTES};
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

mod turns;

pub use turns::{
    AttemptExecution, AttemptSave, AttemptStatistics, AttemptStopReason, AttemptSummary,
    ContentRange, ContentSource, EngineDecodeRate, TurnCursor, TurnPage, TurnSummary,
    MAX_CONTENT_RANGE_BYTES, MAX_TURN_PAGE_BYTES, MAX_TURN_PAGE_ITEMS,
};

pub const HISTORY_SCHEMA_VERSION: u32 = 5;
pub const MAX_CONVERSATION_TITLE_BYTES: usize = 256;
pub const MAX_CONVERSATION_PAGE_ITEMS: usize = 50;
pub const MAX_CONVERSATION_PAGE_BYTES: usize = 24 * 1024;
pub(super) const MAX_ATTEMPT_CONTENT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HistoryCommand {
    GetHistoryStatus,
    CreateConversation {
        model_id: String,
    },
    RenameConversation {
        conversation_id: String,
        expected_revision: String,
        title: String,
    },
    ListConversations {
        cursor: Option<ConversationCursor>,
        limit: u16,
    },
    ListTurns {
        conversation_id: String,
        cursor: Option<TurnCursor>,
        limit: u16,
    },
    ReadContentRange {
        source: ContentSource,
        start: String,
        prefix_end: String,
    },
    DeleteConversation {
        conversation_id: String,
        expected_revision: String,
    },
}

impl HistoryCommand {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::GetHistoryStatus => Ok(()),
            Self::CreateConversation { model_id } => validate_model_id(model_id),
            Self::RenameConversation {
                conversation_id,
                expected_revision,
                title,
            } => {
                validate_hex_id(conversation_id)?;
                validate_positive_decimal(expected_revision, "invalid conversation revision")?;
                validate_title(title)
            }
            Self::ListConversations { cursor, limit } => {
                if *limit == 0 || usize::from(*limit) > MAX_CONVERSATION_PAGE_ITEMS {
                    return Err("invalid conversation page limit");
                }
                if let Some(cursor) = cursor {
                    cursor.validate_shape()?;
                }
                Ok(())
            }
            Self::ListTurns {
                conversation_id,
                cursor,
                limit,
            } => {
                validate_hex_id(conversation_id)?;
                if *limit == 0 || usize::from(*limit) > MAX_TURN_PAGE_ITEMS {
                    return Err("invalid turn page limit");
                }
                if let Some(cursor) = cursor {
                    cursor.validate_shape()?;
                }
                Ok(())
            }
            Self::ReadContentRange {
                source,
                start,
                prefix_end,
            } => {
                source.validate_shape()?;
                let start = bounded_decimal(
                    start,
                    MAX_ATTEMPT_CONTENT_BYTES,
                    "invalid content range start",
                )?;
                let prefix_end = bounded_decimal(
                    prefix_end,
                    MAX_ATTEMPT_CONTENT_BYTES,
                    "invalid content range prefix end",
                )?;
                if start > prefix_end {
                    return Err("invalid captured content range");
                }
                Ok(())
            }
            Self::DeleteConversation {
                conversation_id,
                expected_revision,
            } => {
                validate_hex_id(conversation_id)?;
                validate_positive_decimal(expected_revision, "invalid conversation revision")
            }
        }
    }

    pub fn requires_ready_history(&self) -> bool {
        !matches!(self, Self::GetHistoryStatus)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HistoryReply {
    Status(HistoryStatus),
    Conversation(ConversationSummary),
    ConversationPage(ConversationPage),
    TurnPage(TurnPage),
    ContentRange(ContentRange),
    ConversationDeleted {
        conversation_id: String,
        revision: String,
        purge_complete: bool,
    },
}

impl HistoryReply {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Status(status) => status.validate_shape(),
            Self::Conversation(conversation) => conversation.validate_shape(),
            Self::ConversationPage(page) => page.validate_shape(),
            Self::TurnPage(page) => page.validate_shape(),
            Self::ContentRange(range) => range.validate_shape(),
            Self::ConversationDeleted {
                conversation_id,
                revision,
                ..
            } => {
                validate_hex_id(conversation_id)?;
                validate_positive_decimal(revision, "invalid conversation revision")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPhase {
    Opening,
    Ready,
    Unavailable,
    FlushPending,
    FlushFailed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryStatus {
    pub phase: HistoryPhase,
    pub schema_version: u32,
    pub sqlite_version: Option<String>,
    pub sqlite_source_id: Option<String>,
    pub context: Option<String>,
}

impl HistoryStatus {
    pub fn opening() -> Self {
        Self {
            phase: HistoryPhase::Opening,
            schema_version: 0,
            sqlite_version: None,
            sqlite_source_id: None,
            context: None,
        }
    }

    pub fn ready(
        schema_version: u32,
        sqlite_version: impl Into<String>,
        sqlite_source_id: impl Into<String>,
    ) -> Self {
        Self {
            phase: HistoryPhase::Ready,
            schema_version,
            sqlite_version: Some(sqlite_version.into()),
            sqlite_source_id: Some(sqlite_source_id.into()),
            context: None,
        }
    }

    pub fn unavailable(context: impl Into<String>) -> Self {
        Self {
            phase: HistoryPhase::Unavailable,
            schema_version: 0,
            sqlite_version: None,
            sqlite_source_id: None,
            context: Some(bounded_context(context)),
        }
    }

    pub fn flush_pending(
        schema_version: u32,
        sqlite_version: Option<String>,
        sqlite_source_id: Option<String>,
    ) -> Self {
        Self {
            phase: HistoryPhase::FlushPending,
            schema_version,
            sqlite_version,
            sqlite_source_id,
            context: None,
        }
    }

    pub fn flush_failed(schema_version: u32, context: impl Into<String>) -> Self {
        Self {
            phase: HistoryPhase::FlushFailed,
            schema_version,
            sqlite_version: None,
            sqlite_source_id: None,
            context: Some(bounded_context(context)),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.phase == HistoryPhase::Ready && self.schema_version == HISTORY_SCHEMA_VERSION
    }

    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        if let Some(value) = &self.sqlite_version {
            validate_identifier(value, "invalid SQLite version")?;
        }
        if let Some(value) = &self.sqlite_source_id {
            if value.is_empty() || value.len() > MAX_ERROR_CONTEXT_BYTES {
                return Err("invalid SQLite source identity");
            }
        }
        if let Some(value) = &self.context {
            if value.is_empty() || value.len() > MAX_ERROR_CONTEXT_BYTES {
                return Err("invalid history status context");
            }
        }
        match self.phase {
            HistoryPhase::Opening => {
                if self.schema_version == 0
                    && self.sqlite_version.is_none()
                    && self.sqlite_source_id.is_none()
                    && self.context.is_none()
                {
                    Ok(())
                } else {
                    Err("invalid opening history status")
                }
            }
            HistoryPhase::Ready => {
                if self.schema_version == HISTORY_SCHEMA_VERSION
                    && self.sqlite_version.is_some()
                    && self.sqlite_source_id.is_some()
                    && self.context.is_none()
                {
                    Ok(())
                } else {
                    Err("invalid ready history status")
                }
            }
            HistoryPhase::Unavailable => {
                if self.schema_version == 0 && self.context.is_some() {
                    Ok(())
                } else {
                    Err("invalid unavailable history status")
                }
            }
            HistoryPhase::FlushPending => {
                if self.schema_version <= HISTORY_SCHEMA_VERSION && self.context.is_none() {
                    Ok(())
                } else {
                    Err("invalid pending history flush status")
                }
            }
            HistoryPhase::FlushFailed => {
                if self.schema_version <= HISTORY_SCHEMA_VERSION && self.context.is_some() {
                    Ok(())
                } else {
                    Err("invalid failed history flush status")
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationCursor {
    pub updated_ms: String,
    pub conversation_id: String,
}

impl ConversationCursor {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_decimal(&self.updated_ms, "invalid conversation cursor")?;
        validate_hex_id(&self.conversation_id)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationSummary {
    pub id: String,
    pub model_id: String,
    pub title: String,
    pub created_ms: String,
    pub updated_ms: String,
    pub revision: String,
    pub profile_revision: String,
}

impl ConversationSummary {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.id)?;
        validate_model_id(&self.model_id)?;
        validate_title(&self.title)?;
        validate_decimal(&self.created_ms, "invalid conversation creation time")?;
        validate_decimal(&self.updated_ms, "invalid conversation update time")?;
        validate_positive_decimal(&self.revision, "invalid conversation revision")?;
        validate_positive_decimal(&self.profile_revision, "invalid profile revision")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationPage {
    #[serde(deserialize_with = "deserialize_conversations")]
    pub conversations: Vec<ConversationSummary>,
    pub next: Option<ConversationCursor>,
}

impl ConversationPage {
    fn validate_shape(&self) -> Result<(), &'static str> {
        if self.conversations.len() > MAX_CONVERSATION_PAGE_ITEMS {
            return Err("too many conversations in a page");
        }
        for conversation in &self.conversations {
            conversation.validate_shape()?;
        }
        if conversation_backing_bytes(
            &self.conversations,
            self.conversations.capacity(),
            self.next.as_ref(),
        ) > MAX_CONVERSATION_PAGE_BYTES
        {
            return Err("conversation page exceeds the decoded backing limit");
        }
        if let Some(next) = &self.next {
            next.validate_shape()?;
            if self.conversations.is_empty() {
                return Err("empty conversation page has a cursor");
            }
        }
        let encoded = serde_json::to_vec(self).map_err(|_| "conversation page is invalid")?;
        if encoded.len() > MAX_CONVERSATION_PAGE_BYTES {
            return Err("conversation page exceeds the byte limit");
        }
        Ok(())
    }
}

fn deserialize_conversations<'de, D>(deserializer: D) -> Result<Vec<ConversationSummary>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ConversationsVisitor;

    impl<'de> Visitor<'de> for ConversationsVisitor {
        type Value = Vec<ConversationSummary>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 50 conversation summaries")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|size| size > MAX_CONVERSATION_PAGE_ITEMS)
            {
                return Err(serde::de::Error::custom("too many conversations in a page"));
            }
            let mut values = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(MAX_CONVERSATION_PAGE_ITEMS),
            );
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX_CONVERSATION_PAGE_ITEMS {
                    return Err(serde::de::Error::custom("too many conversations in a page"));
                }
                values.push(value);
            }
            values.shrink_to_fit();
            if conversation_backing_bytes(&values, values.capacity(), None)
                > MAX_CONVERSATION_PAGE_BYTES
            {
                return Err(serde::de::Error::custom(
                    "conversation page exceeds the decoded backing limit",
                ));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(ConversationsVisitor)
}

fn conversation_backing_bytes(
    items: &[ConversationSummary],
    capacity: usize,
    next: Option<&ConversationCursor>,
) -> usize {
    let structs = capacity.saturating_mul(std::mem::size_of::<ConversationSummary>());
    let items = items.iter().fold(structs, |total, item| {
        total
            .saturating_add(item.id.capacity())
            .saturating_add(item.model_id.capacity())
            .saturating_add(item.title.capacity())
            .saturating_add(item.created_ms.capacity())
            .saturating_add(item.updated_ms.capacity())
            .saturating_add(item.revision.capacity())
            .saturating_add(item.profile_revision.capacity())
    });
    next.map_or(items, |cursor| {
        items
            .saturating_add(cursor.updated_ms.capacity())
            .saturating_add(cursor.conversation_id.capacity())
    })
}

fn validate_title(title: &str) -> Result<(), &'static str> {
    if title.is_empty() || title.len() > MAX_CONVERSATION_TITLE_BYTES {
        Err("invalid conversation title")
    } else {
        Ok(())
    }
}

pub(super) fn validate_hex_id(value: &str) -> Result<(), &'static str> {
    if value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err("invalid conversation identity")
    }
}

fn validate_positive_decimal(value: &str, error: &'static str) -> Result<(), &'static str> {
    validate_decimal(value, error)?;
    if value == "0" {
        Err(error)
    } else {
        Ok(())
    }
}

fn bounded_decimal(value: &str, maximum: u64, error: &'static str) -> Result<u64, &'static str> {
    validate_decimal(value, error)?;
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value <= maximum)
        .ok_or(error)
}

pub(super) fn positive_bounded_decimal(
    value: &str,
    maximum: u64,
    error: &'static str,
) -> Result<u64, &'static str> {
    let value = bounded_decimal(value, maximum, error)?;
    if value == 0 {
        Err(error)
    } else {
        Ok(value)
    }
}

fn bounded_context(context: impl Into<String>) -> String {
    let mut context = context.into();
    if context.len() > MAX_ERROR_CONTEXT_BYTES {
        let mut boundary = MAX_ERROR_CONTEXT_BYTES;
        while !context.is_char_boundary(boundary) {
            boundary -= 1;
        }
        context.truncate(boundary);
    }
    if context.is_empty() {
        "history is unavailable".into()
    } else {
        context
    }
}
