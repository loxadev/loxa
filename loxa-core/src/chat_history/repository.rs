//! Synchronous fixed-query SQLite repository. A node-owned worker invokes it.

use super::migrations::MIGRATIONS;
#[cfg(test)]
use super::ASSISTANT_CONTENT_MAX_BYTES;
use super::{
    valid_timestamp_pair, validate_error_code, ChatCursor, ChatId, ChatSummary, HistoryError,
    MessageContent, MessageId, MessageRecord, MessageRole, MessageSegment, MessageSummary, Title,
    TurnCursor, TurnId, TurnMetrics, TurnProvenance, TurnRecord, TurnState,
    MAX_SERIALIZED_MESSAGE_PAGE_BYTES, RESPONSE_SEGMENT_MAX_BYTES,
};
use rusqlite::{
    config::DbConfig, limits::Limit, Connection, Error as SqlError, ErrorCode, OpenFlags,
    OptionalExtension, TransactionBehavior,
};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

const MIGRATIONS_TABLE: &str = "loxa_schema_migrations";
const TRACKING_SCHEMA: &str = r#"
CREATE TABLE loxa_schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at_ms INTEGER NOT NULL
) STRICT;
"#;

const SQLITE_LENGTH_LIMIT: i32 = 4 * 1024 * 1024;
const SQLITE_SQL_LENGTH_LIMIT: i32 = 1024 * 1024;
const SQLITE_COLUMN_LIMIT: i32 = 64;
const SQLITE_EXPR_DEPTH_LIMIT: i32 = 100;
const SQLITE_COMPOUND_SELECT_LIMIT: i32 = 16;
const SQLITE_VDBE_OP_LIMIT: i32 = 100_000;
const SQLITE_FUNCTION_ARG_LIMIT: i32 = 32;
const SQLITE_ATTACHED_LIMIT: i32 = 0;
const SQLITE_LIKE_PATTERN_LIMIT: i32 = 8 * 1024;
const SQLITE_VARIABLE_LIMIT: i32 = 64;
const SQLITE_TRIGGER_DEPTH_LIMIT: i32 = 16;
const SQLITE_WORKER_THREADS_LIMIT: i32 = 0;

pub struct ChatHistoryRepository {
    connection: Connection,
    #[cfg(unix)]
    _directory_guard: fs::File,
    #[cfg(unix)]
    _main_guard: fs::File,
}

struct PreparedStorage {
    created: bool,
    sqlite_path: PathBuf,
    #[cfg(unix)]
    parent_path: PathBuf,
    #[cfg(unix)]
    directory_guard: fs::File,
    #[cfg(unix)]
    main_guard: fs::File,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct HistoryPage {
    pub chats: Vec<ChatSummary>,
    pub next_before: Option<ChatCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct TurnPage {
    pub turns: Vec<TurnRecord>,
    pub next_after: Option<TurnCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MessagePage {
    pub message_id: MessageId,
    pub turn_id: TurnId,
    pub role: MessageRole,
    pub segment_count: u32,
    pub segments: Vec<MessageSegment>,
    pub next_segment: Option<u32>,
}

impl ChatHistoryRepository {
    /// Opens a user-only database and checks all historical migration records.
    ///
    /// The caller must keep this repository on a single dedicated blocking
    /// worker thread. `rusqlite::Connection` is deliberately not synchronized
    /// here so an async runtime cannot accidentally share it across handlers.
    pub fn open(path: &Path) -> Result<Self, HistoryError> {
        Self::open_internal(path, || {})
    }

    #[cfg(all(test, unix))]
    fn open_with_boundary_hook(
        path: &Path,
        boundary_hook: impl FnOnce(),
    ) -> Result<Self, HistoryError> {
        Self::open_internal(path, boundary_hook)
    }

    fn open_internal(path: &Path, boundary_hook: impl FnOnce()) -> Result<Self, HistoryError> {
        let prepared = prepare_storage_path(path)?;
        // SQLite may open existing WAL/SHM files while enabling WAL. Reject
        // them before handing the path to SQLite; the bundled NOFOLLOW flag
        // then also covers a path swap during the open itself.
        validate_auxiliary_files(&prepared.sqlite_path)?;
        boundary_hook();
        let mut connection = Connection::open_with_flags(
            &prepared.sqlite_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NOFOLLOW
                | OpenFlags::SQLITE_OPEN_EXRESCODE,
        )
        .map_err(map_sql_error)?;
        // Compare the inode SQLite opened with the already validated main-file
        // descriptor before issuing any mutating PRAGMA or migration SQL.
        validate_open_storage(&prepared)?;
        configure_connection(&connection)?;
        let changed = apply_migrations(&mut connection)?;
        // WAL/SHM may have been created by configuration or migrations; check
        // their open descriptors and the parent/main identities again.
        validate_open_storage(&prepared)?;
        if prepared.created || changed {
            quick_check_connection(&connection)?;
        }
        Ok(Self {
            connection,
            #[cfg(unix)]
            _directory_guard: prepared.directory_guard,
            #[cfg(unix)]
            _main_guard: prepared.main_guard,
        })
    }

    pub fn schema_version(&self) -> Result<i64, HistoryError> {
        self.connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM loxa_schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(map_sql_error)
    }

    pub fn sqlite_version(&self) -> &'static str {
        rusqlite::version()
    }

    pub fn quick_check(&self) -> Result<(), HistoryError> {
        quick_check_connection(&self.connection)
    }

    #[cfg(test)]
    fn journal_mode(&self) -> Result<String, HistoryError> {
        self.connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(map_sql_error)
    }

    #[cfg(test)]
    fn pragma_i64(&self, pragma: &'static str) -> Result<i64, HistoryError> {
        let sql = match pragma {
            "foreign_keys" => "PRAGMA foreign_keys",
            "synchronous" => "PRAGMA synchronous",
            "busy_timeout" => "PRAGMA busy_timeout",
            "trusted_schema" => "PRAGMA trusted_schema",
            "secure_delete" => "PRAGMA secure_delete",
            "temp_store" => "PRAGMA temp_store",
            "mmap_size" => "PRAGMA mmap_size",
            _ => return Err(HistoryError::InvalidMetadata),
        };
        self.connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(map_sql_error)
    }

    #[cfg(test)]
    fn defensive_mode(&self) -> Result<bool, HistoryError> {
        self.connection
            .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
            .map_err(map_sql_error)
    }

    pub fn create_chat(&mut self, created_at_ms: i64) -> Result<ChatSummary, HistoryError> {
        if !valid_timestamp_pair(created_at_ms, created_at_ms) {
            return Err(HistoryError::InvalidTimestamp);
        }
        let chat = ChatSummary {
            id: ChatId::generate()?,
            title: Title::provisional(),
            created_at_ms,
            updated_at_ms: created_at_ms,
        };
        self.connection
            .execute(
                "INSERT INTO chats(id, title, created_at_ms, updated_at_ms) VALUES(?1, ?2, ?3, ?4)",
                (
                    chat.id.as_str(),
                    chat.title.as_str(),
                    chat.created_at_ms,
                    chat.updated_at_ms,
                ),
            )
            .map_err(map_sql_error)?;
        Ok(chat)
    }

    pub fn get_chat(&self, id: &ChatId) -> Result<ChatSummary, HistoryError> {
        let raw = self
            .connection
            .query_row(
                "SELECT id, title, created_at_ms, updated_at_ms FROM chats WHERE id = ?1",
                [id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?
            .ok_or(HistoryError::NotFound)?;
        chat_from_raw(raw)
    }

    pub fn list_chats(
        &self,
        limit: usize,
        before: Option<&ChatCursor>,
    ) -> Result<HistoryPage, HistoryError> {
        if limit == 0 || limit > super::LIST_MAX_LIMIT {
            return Err(HistoryError::InvalidPageLimit);
        }
        let requested = i64::try_from(limit + 1).map_err(|_| HistoryError::InvalidPageLimit)?;
        let raw = if let Some(before) = before {
            let (updated_at_ms, id) = before.key()?;
            let mut statement = self
                .connection
                .prepare(
                    "SELECT id, title, created_at_ms, updated_at_ms FROM chats WHERE updated_at_ms < ?1 OR (updated_at_ms = ?1 AND id < ?2) ORDER BY updated_at_ms DESC, id DESC LIMIT ?3",
                )
                .map_err(map_sql_error)?;
            let rows = statement
                .query_map((updated_at_ms, id.as_str(), requested), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .map_err(map_sql_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(map_sql_error)?
        } else {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT id, title, created_at_ms, updated_at_ms FROM chats ORDER BY updated_at_ms DESC, id DESC LIMIT ?1",
                )
                .map_err(map_sql_error)?;
            let rows = statement
                .query_map([requested], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .map_err(map_sql_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(map_sql_error)?
        };
        let mut chats = raw
            .into_iter()
            .map(chat_from_raw)
            .collect::<Result<Vec<_>, _>>()?;
        let next_before = if chats.len() > limit {
            chats.pop();
            chats
                .last()
                .map(|chat| ChatCursor::from_key(chat.updated_at_ms, &chat.id))
        } else {
            None
        };
        Ok(HistoryPage { chats, next_before })
    }

    pub fn list_turns(
        &self,
        chat_id: &ChatId,
        limit: usize,
        after: Option<&TurnCursor>,
    ) -> Result<TurnPage, HistoryError> {
        if limit == 0 || limit > super::LIST_MAX_LIMIT {
            return Err(HistoryError::InvalidPageLimit);
        }
        self.get_chat(chat_id)?;
        let requested = i64::try_from(limit + 1).map_err(|_| HistoryError::InvalidPageLimit)?;
        let after = after.map(TurnCursor::ordinal).transpose()?.unwrap_or(-1);
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, chat_id, ordinal, state, model_alias, recipe_id, engine_name, engine_version, error_code, output_tokens, total_duration_ms, ttft_ms, stop_reason, created_at_ms, updated_at_ms FROM turns WHERE chat_id = ?1 AND ordinal > ?2 ORDER BY ordinal ASC LIMIT ?3",
            )
            .map_err(map_sql_error)?;
        let rows = statement
            .query_map((chat_id.as_str(), after, requested), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<i64>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, i64>(14)?,
                ))
            })
            .map_err(map_sql_error)?;
        let mut turns = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sql_error)?
            .into_iter()
            .map(turn_from_raw)
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = if turns.len() > limit {
            turns.pop();
            turns
                .last()
                .map(|turn| TurnCursor::from_ordinal(turn.ordinal))
        } else {
            None
        };
        Ok(TurnPage { turns, next_after })
    }

    pub fn rename_chat(
        &mut self,
        id: &ChatId,
        title: Title,
        updated_at_ms: i64,
    ) -> Result<ChatSummary, HistoryError> {
        let chat = self.get_chat(id)?;
        if updated_at_ms < chat.created_at_ms {
            return Err(HistoryError::InvalidTimestamp);
        }
        let updated = self
            .connection
            .execute(
                "UPDATE chats SET title = ?1, updated_at_ms = MAX(updated_at_ms, ?2) WHERE id = ?3",
                (title.as_str(), updated_at_ms, id.as_str()),
            )
            .map_err(map_sql_error)?;
        if updated != 1 {
            return Err(HistoryError::NotFound);
        }
        self.get_chat(id)
    }

    pub fn delete_chat(&mut self, id: &ChatId) -> Result<(), HistoryError> {
        let deleted = self
            .connection
            .execute("DELETE FROM chats WHERE id = ?1", [id.as_str()])
            .map_err(map_sql_error)?;
        if deleted == 1 {
            Ok(())
        } else {
            Err(HistoryError::NotFound)
        }
    }

    pub fn clear_all(&mut self) -> Result<usize, HistoryError> {
        self.connection
            .execute("DELETE FROM chats", [])
            .map_err(map_sql_error)
    }

    pub fn begin_turn(
        &mut self,
        chat_id: &ChatId,
        user_content: MessageContent,
        provenance: TurnProvenance,
        created_at_ms: i64,
    ) -> Result<TurnRecord, HistoryError> {
        if !valid_timestamp_pair(created_at_ms, created_at_ms) {
            return Err(HistoryError::InvalidTimestamp);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sql_error)?;
        let chat = transaction
            .query_row(
                "SELECT title, created_at_ms, updated_at_ms FROM chats WHERE id = ?1",
                [chat_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?
            .ok_or(HistoryError::NotFound)?;
        if created_at_ms < chat.1 || created_at_ms < chat.2 {
            return Err(HistoryError::InvalidTimestamp);
        }
        let ordinal: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM turns WHERE chat_id = ?1",
                [chat_id.as_str()],
                |row| row.get(0),
            )
            .map_err(map_sql_error)?;
        let turn_id = TurnId::generate()?;
        let user_message_id = MessageId::generate()?;
        let assistant_message_id = MessageId::generate()?;
        let assistant_content = MessageContent::assistant("")?;
        transaction
            .execute(
                "INSERT INTO turns(id, chat_id, ordinal, state, model_alias, recipe_id, engine_name, engine_version, error_code, created_at_ms, updated_at_ms) VALUES(?1, ?2, ?3, 'queued', 'loxa', ?4, ?5, ?6, NULL, ?7, ?8)",
                (
                    turn_id.as_str(),
                    chat_id.as_str(),
                    ordinal,
                    provenance.recipe_id.as_str(),
                    provenance.engine_name.as_deref(),
                    provenance.engine_version.as_deref(),
                    created_at_ms,
                    created_at_ms,
                ),
            )
            .map_err(map_sql_error)?;
        transaction
            .execute(
                "INSERT INTO messages(id, turn_id, role, content, created_at_ms, updated_at_ms) VALUES(?1, ?2, 'user', ?3, ?4, ?5), (?6, ?2, 'assistant', ?7, ?4, ?5)",
                (
                    user_message_id.as_str(),
                    turn_id.as_str(),
                    user_content.as_str(),
                    created_at_ms,
                    created_at_ms,
                    assistant_message_id.as_str(),
                    assistant_content.as_str(),
                ),
            )
            .map_err(map_sql_error)?;
        let first_title = if ordinal == 0 && chat.0 == Title::provisional().as_str() {
            Title::from_first_user_message(user_content.as_str())
        } else {
            None
        };
        transaction
            .execute(
                "UPDATE chats SET title = COALESCE(?1, title), updated_at_ms = ?2 WHERE id = ?3",
                (
                    first_title.as_ref().map(Title::as_str),
                    created_at_ms,
                    chat_id.as_str(),
                ),
            )
            .map_err(map_sql_error)?;
        transaction.commit().map_err(map_sql_error)?;
        Ok(TurnRecord {
            id: turn_id,
            chat_id: chat_id.clone(),
            ordinal,
            state: TurnState::Queued,
            provenance,
            error_code: None,
            metrics: TurnMetrics::default(),
            created_at_ms,
            updated_at_ms: created_at_ms,
        })
    }

    pub fn finalize_turn(
        &mut self,
        turn_id: &TurnId,
        state: TurnState,
        assistant_content: MessageContent,
        error_code: Option<&str>,
        updated_at_ms: i64,
    ) -> Result<(), HistoryError> {
        self.finalize_turn_with_metrics(
            turn_id,
            state,
            assistant_content,
            error_code,
            TurnMetrics::default(),
            updated_at_ms,
        )
    }

    pub fn finalize_turn_with_metrics(
        &mut self,
        turn_id: &TurnId,
        state: TurnState,
        assistant_content: MessageContent,
        error_code: Option<&str>,
        metrics: TurnMetrics,
        updated_at_ms: i64,
    ) -> Result<(), HistoryError> {
        if !matches!(
            state,
            TurnState::Completed | TurnState::Cancelled | TurnState::Failed
        ) {
            return Err(HistoryError::InvalidTurnState);
        }
        validate_error_code(error_code)?;
        metrics.validate()?;
        if (state == TurnState::Failed) != error_code.is_some() {
            return Err(HistoryError::InvalidMetadata);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sql_error)?;
        let turn = transaction
            .query_row(
                "SELECT chat_id, created_at_ms, updated_at_ms FROM turns WHERE id = ?1",
                [turn_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?
            .ok_or(HistoryError::NotFound)?;
        if updated_at_ms < turn.1 || updated_at_ms < turn.2 {
            return Err(HistoryError::InvalidTimestamp);
        }
        let updated = transaction
            .execute(
                "UPDATE turns SET state = ?1, error_code = ?2, output_tokens = ?3, total_duration_ms = ?4, ttft_ms = ?5, stop_reason = ?6, updated_at_ms = ?7 WHERE id = ?8 AND state IN ('queued', 'streaming')",
                (
                    state.as_str(),
                    error_code,
                    metrics.output_tokens.map(|value| value as i64),
                    metrics.total_duration_ms.map(|value| value as i64),
                    metrics.ttft_ms.map(|value| value as i64),
                    metrics.stop_reason.as_deref(),
                    updated_at_ms,
                    turn_id.as_str(),
                ),
            )
            .map_err(map_sql_error)?;
        if updated != 1 {
            return Err(HistoryError::Conflict);
        }
        let message_updated = transaction
            .execute(
                "UPDATE messages SET content = ?1, updated_at_ms = ?2 WHERE turn_id = ?3 AND role = 'assistant'",
                (assistant_content.as_str(), updated_at_ms, turn_id.as_str()),
            )
            .map_err(map_sql_error)?;
        if message_updated != 1 {
            return Err(HistoryError::CorruptDatabase);
        }
        transaction
            .execute(
                "UPDATE chats SET updated_at_ms = MAX(updated_at_ms, ?1) WHERE id = ?2",
                (updated_at_ms, turn.0.as_str()),
            )
            .map_err(map_sql_error)?;
        transaction.commit().map_err(map_sql_error)?;
        Ok(())
    }

    pub fn checkpoint_assistant(
        &mut self,
        turn_id: &TurnId,
        assistant_content: MessageContent,
        updated_at_ms: i64,
    ) -> Result<(), HistoryError> {
        let turn = self.get_turn(turn_id)?;
        if !turn.state.is_interrupted() {
            return Err(HistoryError::Conflict);
        }
        if updated_at_ms < turn.updated_at_ms {
            return Err(HistoryError::InvalidTimestamp);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sql_error)?;
        let updated = transaction
            .execute(
                "UPDATE turns SET state = 'streaming', updated_at_ms = ?1 WHERE id = ?2 AND state IN ('queued', 'streaming')",
                (updated_at_ms, turn_id.as_str()),
            )
            .map_err(map_sql_error)?;
        if updated != 1 {
            return Err(HistoryError::Conflict);
        }
        let message_updated = transaction
            .execute(
                "UPDATE messages SET content = ?1, updated_at_ms = ?2 WHERE turn_id = ?3 AND role = 'assistant'",
                (assistant_content.as_str(), updated_at_ms, turn_id.as_str()),
            )
            .map_err(map_sql_error)?;
        if message_updated != 1 {
            return Err(HistoryError::CorruptDatabase);
        }
        transaction
            .execute(
                "UPDATE chats SET updated_at_ms = MAX(updated_at_ms, ?1) WHERE id = ?2",
                (updated_at_ms, turn.chat_id.as_str()),
            )
            .map_err(map_sql_error)?;
        transaction.commit().map_err(map_sql_error)?;
        Ok(())
    }

    pub fn recover_interrupted(&mut self, recovered_at_ms: i64) -> Result<usize, HistoryError> {
        if recovered_at_ms < 0 {
            return Err(HistoryError::InvalidTimestamp);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sql_error)?;
        transaction
            .execute(
                "UPDATE chats SET updated_at_ms = MAX(updated_at_ms, ?1) WHERE id IN (SELECT DISTINCT chat_id FROM turns WHERE state IN ('queued', 'streaming'))",
                [recovered_at_ms],
            )
            .map_err(map_sql_error)?;
        let recovered = transaction
            .execute(
                "UPDATE turns SET state = 'failed', error_code = 'node_restarted', updated_at_ms = MAX(updated_at_ms, ?1) WHERE state IN ('queued', 'streaming')",
                [recovered_at_ms],
            )
            .map_err(map_sql_error)?;
        transaction.commit().map_err(map_sql_error)?;
        Ok(recovered)
    }

    pub fn get_turn(&self, id: &TurnId) -> Result<TurnRecord, HistoryError> {
        let raw = self
            .connection
            .query_row(
                "SELECT id, chat_id, ordinal, state, model_alias, recipe_id, engine_name, engine_version, error_code, output_tokens, total_duration_ms, ttft_ms, stop_reason, created_at_ms, updated_at_ms FROM turns WHERE id = ?1",
                [id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, Option<i64>>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, i64>(13)?,
                        row.get::<_, i64>(14)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?
            .ok_or(HistoryError::NotFound)?;
        turn_from_raw(raw)
    }

    #[cfg(test)]
    pub(crate) fn messages_for_turn(
        &self,
        turn_id: &TurnId,
    ) -> Result<Vec<MessageRecord>, HistoryError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, turn_id, role, content, created_at_ms, updated_at_ms FROM messages WHERE turn_id = ?1 ORDER BY CASE role WHEN 'user' THEN 0 ELSE 1 END",
            )
            .map_err(map_sql_error)?;
        let rows = statement
            .query_map([turn_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(map_sql_error)?;
        let messages = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sql_error)?
            .into_iter()
            .map(message_from_raw)
            .collect::<Result<Vec<_>, _>>()?;
        if messages.len() == 2 {
            Ok(messages)
        } else if messages.is_empty() {
            Err(HistoryError::NotFound)
        } else {
            Err(HistoryError::CorruptDatabase)
        }
    }

    pub fn message_summaries_for_turn(
        &self,
        turn_id: &TurnId,
    ) -> Result<Vec<MessageSummary>, HistoryError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, turn_id, role, length(CAST(content AS BLOB)), created_at_ms, updated_at_ms FROM messages WHERE turn_id = ?1 ORDER BY CASE role WHEN 'user' THEN 0 ELSE 1 END",
            )
            .map_err(map_sql_error)?;
        let rows = statement
            .query_map([turn_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(map_sql_error)?;
        let messages = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sql_error)?
            .into_iter()
            .map(message_summary_from_raw)
            .collect::<Result<Vec<_>, _>>()?;
        if messages.len() == 2 {
            Ok(messages)
        } else if messages.is_empty() {
            Err(HistoryError::NotFound)
        } else {
            Err(HistoryError::CorruptDatabase)
        }
    }

    pub fn message_page(
        &self,
        message_id: &MessageId,
        start_segment: u32,
    ) -> Result<MessagePage, HistoryError> {
        let raw = self
            .connection
            .query_row(
                "SELECT id, turn_id, role, content, created_at_ms, updated_at_ms FROM messages WHERE id = ?1",
                [message_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?
            .ok_or(HistoryError::NotFound)?;
        let message = message_from_raw(raw)?;
        let ranges = utf8_segment_ranges(message.content.as_str());
        let segment_count = u32::try_from(ranges.len()).map_err(|_| HistoryError::Database)?;
        let start = usize::try_from(start_segment).map_err(|_| HistoryError::InvalidCursor)?;
        if start >= ranges.len() {
            return Err(HistoryError::InvalidCursor);
        }
        let mut segments = Vec::new();
        let mut next = start;
        while next < ranges.len() {
            let (begin, end) = ranges[next];
            let candidate = MessageSegment {
                message_id: message.id.clone(),
                turn_id: message.turn_id.clone(),
                role: message.role,
                segment_index: u32::try_from(next).map_err(|_| HistoryError::Database)?,
                segment_count,
                content: message.content.as_str()[begin..end].to_owned(),
            };
            let mut candidate_segments = segments.clone();
            candidate_segments.push(candidate.clone());
            let candidate_page = MessagePage {
                message_id: message.id.clone(),
                turn_id: message.turn_id.clone(),
                role: message.role,
                segment_count,
                segments: candidate_segments,
                next_segment: if next + 1 < ranges.len() {
                    Some(u32::try_from(next + 1).map_err(|_| HistoryError::Database)?)
                } else {
                    None
                },
            };
            let encoded =
                serde_json::to_vec(&candidate_page).map_err(|_| HistoryError::Database)?;
            if encoded.len() >= MAX_SERIALIZED_MESSAGE_PAGE_BYTES && !segments.is_empty() {
                break;
            }
            if encoded.len() >= MAX_SERIALIZED_MESSAGE_PAGE_BYTES {
                return Err(HistoryError::Database);
            }
            segments.push(candidate);
            next += 1;
        }
        Ok(MessagePage {
            message_id: message.id,
            turn_id: message.turn_id,
            role: message.role,
            segment_count,
            segments,
            next_segment: if next < ranges.len() {
                Some(u32::try_from(next).map_err(|_| HistoryError::Database)?)
            } else {
                None
            },
        })
    }
}

fn chat_from_raw(raw: (String, String, i64, i64)) -> Result<ChatSummary, HistoryError> {
    if !valid_timestamp_pair(raw.2, raw.3) {
        return Err(HistoryError::CorruptDatabase);
    }
    Ok(ChatSummary {
        id: ChatId::parse(&raw.0).map_err(|_| HistoryError::CorruptDatabase)?,
        title: Title::new(&raw.1).map_err(|_| HistoryError::CorruptDatabase)?,
        created_at_ms: raw.2,
        updated_at_ms: raw.3,
    })
}

type TurnRaw = (
    String,
    String,
    i64,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    i64,
    i64,
);

fn turn_from_raw(raw: TurnRaw) -> Result<TurnRecord, HistoryError> {
    if raw.2 < 0 || !valid_timestamp_pair(raw.13, raw.14) || raw.4 != "loxa" {
        return Err(HistoryError::CorruptDatabase);
    }
    let provenance = TurnProvenance::new(&raw.5, raw.6.as_deref(), raw.7.as_deref())
        .map_err(|_| HistoryError::CorruptDatabase)?;
    validate_error_code(raw.8.as_deref()).map_err(|_| HistoryError::CorruptDatabase)?;
    let metrics = TurnMetrics::new(
        raw.9
            .map(|value| u64::try_from(value).map_err(|_| HistoryError::CorruptDatabase))
            .transpose()?,
        raw.10
            .map(|value| u64::try_from(value).map_err(|_| HistoryError::CorruptDatabase))
            .transpose()?,
        raw.11
            .map(|value| u64::try_from(value).map_err(|_| HistoryError::CorruptDatabase))
            .transpose()?,
        raw.12.as_deref(),
    )
    .map_err(|_| HistoryError::CorruptDatabase)?;
    Ok(TurnRecord {
        id: TurnId::parse(&raw.0).map_err(|_| HistoryError::CorruptDatabase)?,
        chat_id: ChatId::parse(&raw.1).map_err(|_| HistoryError::CorruptDatabase)?,
        ordinal: raw.2,
        state: TurnState::from_db(&raw.3).map_err(|_| HistoryError::CorruptDatabase)?,
        provenance,
        error_code: raw.8,
        metrics,
        created_at_ms: raw.13,
        updated_at_ms: raw.14,
    })
}

type MessageRaw = (String, String, String, String, i64, i64);

fn message_from_raw(raw: MessageRaw) -> Result<MessageRecord, HistoryError> {
    if !valid_timestamp_pair(raw.4, raw.5) {
        return Err(HistoryError::CorruptDatabase);
    }
    let role = MessageRole::from_db(&raw.2).map_err(|_| HistoryError::CorruptDatabase)?;
    let content = match role {
        MessageRole::User => MessageContent::user(&raw.3),
        MessageRole::Assistant => MessageContent::assistant(&raw.3),
    }
    .map_err(|_| HistoryError::CorruptDatabase)?;
    Ok(MessageRecord {
        id: MessageId::parse(&raw.0).map_err(|_| HistoryError::CorruptDatabase)?,
        turn_id: TurnId::parse(&raw.1).map_err(|_| HistoryError::CorruptDatabase)?,
        role,
        content,
        created_at_ms: raw.4,
        updated_at_ms: raw.5,
    })
}

type MessageSummaryRaw = (String, String, String, i64, i64, i64);

fn message_summary_from_raw(raw: MessageSummaryRaw) -> Result<MessageSummary, HistoryError> {
    if raw.3 < 0 || !valid_timestamp_pair(raw.4, raw.5) {
        return Err(HistoryError::CorruptDatabase);
    }
    let role = MessageRole::from_db(&raw.2).map_err(|_| HistoryError::CorruptDatabase)?;
    let max = match role {
        MessageRole::User => super::USER_CONTENT_MAX_BYTES,
        MessageRole::Assistant => super::ASSISTANT_CONTENT_MAX_BYTES,
    };
    let content_bytes = usize::try_from(raw.3).map_err(|_| HistoryError::CorruptDatabase)?;
    if content_bytes > max {
        return Err(HistoryError::CorruptDatabase);
    }
    Ok(MessageSummary {
        id: MessageId::parse(&raw.0).map_err(|_| HistoryError::CorruptDatabase)?,
        turn_id: TurnId::parse(&raw.1).map_err(|_| HistoryError::CorruptDatabase)?,
        role,
        content_bytes,
        created_at_ms: raw.4,
        updated_at_ms: raw.5,
    })
}

fn utf8_segment_ranges(value: &str) -> Vec<(usize, usize)> {
    if value.is_empty() {
        return vec![(0, 0)];
    }
    let mut ranges = Vec::with_capacity(value.len().div_ceil(RESPONSE_SEGMENT_MAX_BYTES));
    let mut start = 0;
    while start < value.len() {
        let mut end = (start + RESPONSE_SEGMENT_MAX_BYTES).min(value.len());
        while end > start && !value.is_char_boundary(end) {
            end -= 1;
        }
        debug_assert!(end > start);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn configure_connection(connection: &Connection) -> Result<(), HistoryError> {
    connection
        .set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)
        .map_err(map_sql_error)?;
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys=ON;
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA busy_timeout=2000;
            PRAGMA trusted_schema=OFF;
            PRAGMA secure_delete=ON;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=0;
            ",
        )
        .map_err(map_sql_error)?;
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_LENGTH, SQLITE_LENGTH_LIMIT),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, SQLITE_SQL_LENGTH_LIMIT),
        (Limit::SQLITE_LIMIT_COLUMN, SQLITE_COLUMN_LIMIT),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, SQLITE_EXPR_DEPTH_LIMIT),
        (
            Limit::SQLITE_LIMIT_COMPOUND_SELECT,
            SQLITE_COMPOUND_SELECT_LIMIT,
        ),
        (Limit::SQLITE_LIMIT_VDBE_OP, SQLITE_VDBE_OP_LIMIT),
        (Limit::SQLITE_LIMIT_FUNCTION_ARG, SQLITE_FUNCTION_ARG_LIMIT),
        (Limit::SQLITE_LIMIT_ATTACHED, SQLITE_ATTACHED_LIMIT),
        (
            Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
            SQLITE_LIKE_PATTERN_LIMIT,
        ),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, SQLITE_VARIABLE_LIMIT),
        (
            Limit::SQLITE_LIMIT_TRIGGER_DEPTH,
            SQLITE_TRIGGER_DEPTH_LIMIT,
        ),
        (
            Limit::SQLITE_LIMIT_WORKER_THREADS,
            SQLITE_WORKER_THREADS_LIMIT,
        ),
    ] {
        connection.set_limit(limit, value).map_err(map_sql_error)?;
    }
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .map_err(map_sql_error)?;
    let defensive = connection
        .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
        .map_err(map_sql_error)?;
    if foreign_keys != 1 || !defensive {
        return Err(HistoryError::Database);
    }
    Ok(())
}

fn apply_migrations(connection: &mut Connection) -> Result<bool, HistoryError> {
    let ledger_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            [MIGRATIONS_TABLE],
            |row| row.get(0),
        )
        .map_err(map_sql_error)?;
    if !ledger_exists {
        let user_table_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .map_err(map_sql_error)?;
        if user_table_count != 0 {
            return Err(HistoryError::CorruptDatabase);
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sql_error)?;
        transaction
            .execute_batch(TRACKING_SCHEMA)
            .map_err(map_sql_error)?;
        apply_migrations_in_transaction(&transaction, 0)?;
        validate_required_schema(&transaction, MIGRATIONS.len(), true)?;
        transaction.commit().map_err(map_sql_error)?;
        return Ok(true);
    }

    let existing = {
        let mut statement = connection
            .prepare("SELECT version, name, checksum FROM loxa_schema_migrations ORDER BY version")
            .map_err(map_sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(map_sql_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(map_sql_error)?
    };

    if existing.len() > MIGRATIONS.len() {
        return Err(HistoryError::UnsupportedSchema);
    }
    for (index, (version, name, checksum)) in existing.iter().enumerate() {
        let expected = MIGRATIONS
            .get(index)
            .ok_or(HistoryError::UnsupportedSchema)?;
        if *version != expected.version {
            return if *version > expected.version {
                Err(HistoryError::UnsupportedSchema)
            } else {
                Err(HistoryError::CorruptDatabase)
            };
        }
        if name != expected.name || checksum != expected.checksum || !checksum_matches(expected) {
            return Err(HistoryError::CorruptDatabase);
        }
    }

    validate_required_schema(
        connection,
        existing.len(),
        existing.len() == MIGRATIONS.len(),
    )?;
    if existing.len() == MIGRATIONS.len() {
        return Ok(false);
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(map_sql_error)?;
    apply_migrations_in_transaction(&transaction, existing.len())?;
    validate_required_schema(&transaction, MIGRATIONS.len(), true)?;
    transaction.commit().map_err(map_sql_error)?;
    Ok(true)
}

type SchemaObject = (String, String, String, Option<String>);

fn validate_required_schema(
    connection: &Connection,
    applied_migrations: usize,
    exact: bool,
) -> Result<(), HistoryError> {
    let expected_connection = Connection::open_in_memory().map_err(|_| HistoryError::Database)?;
    expected_connection
        .execute_batch(TRACKING_SCHEMA)
        .map_err(|_| HistoryError::Database)?;
    for migration in MIGRATIONS.iter().take(applied_migrations) {
        expected_connection
            .execute_batch(migration.sql)
            .map_err(|_| HistoryError::Database)?;
    }

    let expected = schema_objects(&expected_connection)?;
    let actual = schema_objects(connection)?;
    let matches = if exact {
        actual == expected
    } else {
        expected.iter().all(|object| actual.contains(object))
    };
    if matches {
        Ok(())
    } else {
        Err(HistoryError::CorruptDatabase)
    }
}

fn schema_objects(connection: &Connection) -> Result<Vec<SchemaObject>, HistoryError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        )
        .map_err(|_| HistoryError::CorruptDatabase)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|_| HistoryError::CorruptDatabase)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| HistoryError::CorruptDatabase)
}

fn apply_migrations_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    start: usize,
) -> Result<(), HistoryError> {
    for migration in &MIGRATIONS[start..] {
        if !checksum_matches(migration) {
            return Err(HistoryError::CorruptDatabase);
        }
        transaction
            .execute_batch(migration.sql)
            .map_err(map_sql_error)?;
        transaction
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, ?4)",
                (migration.version, migration.name, migration.checksum, now_ms()?),
            )
            .map_err(map_sql_error)?;
    }
    Ok(())
}

fn checksum_matches(migration: &super::migrations::Migration) -> bool {
    let digest = Sha256::digest(migration.sql.as_bytes());
    let mut actual = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut actual, "{byte:02x}");
    }
    actual == migration.checksum
}

fn now_ms() -> Result<i64, HistoryError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| HistoryError::InvalidTimestamp)?;
    i64::try_from(duration.as_millis()).map_err(|_| HistoryError::InvalidTimestamp)
}

fn quick_check_connection(connection: &Connection) -> Result<(), HistoryError> {
    let result: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(map_sql_error)?;
    if result == "ok" {
        Ok(())
    } else {
        Err(HistoryError::CorruptDatabase)
    }
}

fn map_sql_error(error: SqlError) -> HistoryError {
    match error {
        SqlError::SqliteFailure(code, _)
            if matches!(
                code.code,
                ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase
            ) =>
        {
            HistoryError::CorruptDatabase
        }
        _ => HistoryError::Database,
    }
}

#[cfg(unix)]
fn prepare_storage_path(path: &Path) -> Result<PreparedStorage, HistoryError> {
    let parent = path.parent().ok_or(HistoryError::Security)?;
    path.file_name().ok_or(HistoryError::Security)?;
    let directory_guard = open_secure_directory(parent)?;
    let (main_guard, created) = open_or_create_private_file(path)?;
    Ok(PreparedStorage {
        created,
        sqlite_path: path.to_owned(),
        parent_path: parent.to_owned(),
        directory_guard,
        main_guard,
    })
}

#[cfg(not(unix))]
fn prepare_storage_path(path: &Path) -> Result<PreparedStorage, HistoryError> {
    let parent = path.parent().ok_or(HistoryError::Security)?;
    fs::create_dir_all(parent).map_err(|_| HistoryError::Security)?;
    let (main_guard, created) = open_or_create_private_file(path)?;
    drop(main_guard);
    Ok(PreparedStorage {
        created,
        sqlite_path: path.to_owned(),
    })
}

#[cfg(unix)]
fn open_secure_directory(path: &Path) -> Result<fs::File, HistoryError> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

    let open = || {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options.open(path)
    };
    let directory = match open() {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(HistoryError::Security),
            }
            open().map_err(|_| HistoryError::Security)?
        }
        Err(_) => return Err(HistoryError::Security),
    };
    validate_secure_directory_metadata(&directory.metadata().map_err(|_| HistoryError::Security)?)?;
    Ok(directory)
}

fn open_or_create_private_file(path: &Path) -> Result<(fs::File, bool), HistoryError> {
    if let Some(file) = validate_optional_open_file(path, None)? {
        return Ok((file, false));
    }
    match open_private_file(path, true) {
        Ok(file) => {
            file.sync_all().map_err(|_| HistoryError::Security)?;
            let expected = file.metadata().map_err(|_| HistoryError::Security)?;
            let guard = validate_optional_open_file(path, Some(&expected))?
                .ok_or(HistoryError::Security)?;
            Ok((guard, true))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let file = validate_optional_open_file(path, None)?.ok_or(HistoryError::Security)?;
            Ok((file, false))
        }
        Err(_) => Err(HistoryError::Security),
    }
}

fn open_private_file(path: &Path, create_new: bool) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    validate_secure_file_metadata(&file.metadata()?)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::PermissionDenied))?;
    Ok(file)
}

fn auxiliary_path(path: &Path, suffix: &str) -> Result<PathBuf, HistoryError> {
    let mut file_name = path
        .file_name()
        .ok_or(HistoryError::Security)?
        .to_os_string();
    file_name.push(suffix);
    Ok(path.with_file_name(file_name))
}

fn validate_auxiliary_files(path: &Path) -> Result<(), HistoryError> {
    for suffix in ["-wal", "-shm"] {
        validate_optional_open_file(&auxiliary_path(path, suffix)?, None)?;
    }
    Ok(())
}

fn validate_open_storage(prepared: &PreparedStorage) -> Result<(), HistoryError> {
    #[cfg(unix)]
    {
        let expected_directory = prepared
            .directory_guard
            .metadata()
            .map_err(|_| HistoryError::Security)?;
        validate_secure_directory_metadata(&expected_directory)?;
        let current_directory =
            fs::symlink_metadata(&prepared.parent_path).map_err(|_| HistoryError::Security)?;
        validate_secure_directory_metadata(&current_directory)?;
        if !same_file_identity(&expected_directory, &current_directory) {
            return Err(HistoryError::Security);
        }
        let expected = prepared
            .main_guard
            .metadata()
            .map_err(|_| HistoryError::Security)?;
        validate_optional_open_file(&prepared.sqlite_path, Some(&expected))?
            .ok_or(HistoryError::Security)?;
    }
    #[cfg(not(unix))]
    {
        validate_optional_open_file(&prepared.sqlite_path, None)?.ok_or(HistoryError::Security)?;
    }
    validate_auxiliary_files(&prepared.sqlite_path)
}

fn validate_optional_open_file(
    path: &Path,
    expected: Option<&fs::Metadata>,
) -> Result<Option<fs::File>, HistoryError> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(HistoryError::Security),
    };
    validate_secure_file_metadata(&before)?;
    let file = match open_private_file(path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(HistoryError::Security);
        }
        Err(_) => return Err(HistoryError::Security),
    };
    let opened = file.metadata().map_err(|_| HistoryError::Security)?;
    let after = fs::symlink_metadata(path).map_err(|_| HistoryError::Security)?;
    validate_secure_file_metadata(&after)?;
    if !same_file_identity(&before, &opened)
        || !same_file_identity(&opened, &after)
        || expected.is_some_and(|value| !same_file_identity(value, &opened))
    {
        return Err(HistoryError::Security);
    }
    Ok(Some(file))
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

#[cfg(unix)]
fn validate_secure_directory_metadata(metadata: &fs::Metadata) -> Result<(), HistoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_dir()
        || metadata.uid() != current_user_id()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(HistoryError::Security);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_secure_file_metadata(metadata: &fs::Metadata) -> Result<(), HistoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_file()
        || metadata.uid() != current_user_id()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(HistoryError::Security);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_secure_file_metadata(metadata: &fs::Metadata) -> Result<(), HistoryError> {
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(HistoryError::Security)
    }
}

#[cfg(unix)]
fn current_user_id() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn history_path(directory: &tempfile::TempDir) -> PathBuf {
        let parent = std::fs::canonicalize(directory.path())
            .unwrap()
            .join(".loxa");
        std::fs::create_dir(&parent).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        parent.join("chat-history.sqlite3")
    }

    #[test]
    fn fresh_database_has_the_checked_schema_and_hardened_connection_settings() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory);

        let repository = ChatHistoryRepository::open(&path).unwrap();

        assert_eq!(repository.schema_version().unwrap(), 3);
        assert!(repository.sqlite_version().starts_with("3.53.2"));
        assert_eq!(repository.journal_mode().unwrap(), "wal");
        assert_eq!(repository.pragma_i64("foreign_keys").unwrap(), 1);
        assert_eq!(repository.pragma_i64("synchronous").unwrap(), 2);
        assert_eq!(repository.pragma_i64("busy_timeout").unwrap(), 2_000);
        assert_eq!(repository.pragma_i64("trusted_schema").unwrap(), 0);
        assert_eq!(repository.pragma_i64("secure_delete").unwrap(), 1);
        assert_eq!(repository.pragma_i64("temp_store").unwrap(), 2);
        assert_eq!(repository.pragma_i64("mmap_size").unwrap(), 0);
        assert!(repository.defensive_mode().unwrap());
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_LENGTH)
                .unwrap(),
            SQLITE_LENGTH_LIMIT
        );
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_ATTACHED)
                .unwrap(),
            SQLITE_ATTACHED_LIMIT
        );
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_WORKER_THREADS)
                .unwrap(),
            SQLITE_WORKER_THREADS_LIMIT
        );
        assert!(repository.quick_check().is_ok());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn first_turn_replaces_only_the_provisional_new_chat_title() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        assert_eq!(chat.title.as_str(), "New chat");

        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("  Explain\n\n node   health states  ").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", Some("llama.cpp"), Some("9910")).unwrap(),
                1_010,
            )
            .unwrap();

        assert_eq!(turn.state, TurnState::Queued);
        assert_eq!(
            repository.get_chat(&chat.id).unwrap().title.as_str(),
            "Explain node health states"
        );
        repository
            .finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("A response").unwrap(),
                None,
                1_020,
            )
            .unwrap();
        assert_eq!(
            repository.get_chat(&chat.id).unwrap().title.as_str(),
            "Explain node health states"
        );
    }

    #[test]
    fn finalized_turn_metrics_round_trip_through_get_and_list() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("metrics please").unwrap(),
                TurnProvenance::new("recipe", Some("engine"), Some("1")).unwrap(),
                1_010,
            )
            .unwrap();
        let metrics = TurnMetrics::new(Some(17), Some(950), Some(75), Some("stop")).unwrap();

        repository
            .finalize_turn_with_metrics(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("done").unwrap(),
                None,
                metrics.clone(),
                1_020,
            )
            .unwrap();

        assert_eq!(repository.get_turn(&turn.id).unwrap().metrics, metrics);
        assert_eq!(
            repository.list_turns(&chat.id, 30, None).unwrap().turns[0].metrics,
            metrics
        );
    }

    #[test]
    fn chat_listing_uses_stable_descending_keyset_pages() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let created = (0..3)
            .map(|_| repository.create_chat(1_000).unwrap())
            .collect::<Vec<_>>();

        let first = repository.list_chats(2, None).unwrap();
        assert_eq!(first.chats.len(), 2);
        assert!(first.next_before.is_some());
        assert!(first
            .chats
            .windows(2)
            .all(|pair| pair[0].id.as_str() > pair[1].id.as_str()));

        let second = repository
            .list_chats(2, first.next_before.as_ref())
            .unwrap();
        assert_eq!(second.chats.len(), 1);
        assert!(second.next_before.is_none());
        let seen = first
            .chats
            .iter()
            .chain(second.chats.iter())
            .map(|chat| chat.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(seen.len(), created.len());
    }

    #[test]
    fn delete_cascades_turns_and_clear_leaves_unrelated_private_state_intact() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory);
        let sentinel = path.parent().unwrap().join("control.token");
        std::fs::write(&sentinel, "unrelated state").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut repository = ChatHistoryRepository::open(&path).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("first").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        repository
            .finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("done").unwrap(),
                None,
                1_020,
            )
            .unwrap();

        repository.delete_chat(&chat.id).unwrap();
        assert_eq!(repository.get_chat(&chat.id), Err(HistoryError::NotFound));
        let turns: i64 = repository
            .connection
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .unwrap();
        let messages: i64 = repository
            .connection
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!((turns, messages), (0, 0));

        repository.create_chat(1_030).unwrap();
        assert_eq!(repository.clear_all().unwrap(), 1);
        assert!(sentinel.exists());
    }

    #[test]
    fn recovery_marks_interrupted_turns_failed_without_losing_partial_assistant_text() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("resume me").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        repository
            .checkpoint_assistant(
                &turn.id,
                MessageContent::assistant("partial response").unwrap(),
                1_020,
            )
            .unwrap();

        assert_eq!(repository.recover_interrupted(1_030).unwrap(), 1);
        let restored = repository.get_turn(&turn.id).unwrap();
        assert_eq!(restored.state, TurnState::Failed);
        assert_eq!(restored.error_code.as_deref(), Some("node_restarted"));
        let messages = repository.messages_for_turn(&turn.id).unwrap();
        assert_eq!(messages[1].content.as_str(), "partial response");
    }

    #[test]
    fn checksum_mismatch_and_newer_schema_fail_closed_without_resetting_history() {
        let directory = tempfile::tempdir().unwrap();
        let mismatch_path = history_path(&directory);
        prepare_storage_path(&mismatch_path).unwrap();
        let connection = Connection::open(&mismatch_path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(1, ?1, '0', 1)",
                [MIGRATIONS[0].name],
            )
            .unwrap();
        drop(connection);
        assert!(matches!(
            ChatHistoryRepository::open(&mismatch_path),
            Err(HistoryError::CorruptDatabase)
        ));
        assert!(mismatch_path.exists());

        let newer_path = std::fs::canonicalize(directory.path())
            .unwrap()
            .join(".loxa")
            .join("newer.sqlite3");
        prepare_storage_path(&newer_path).unwrap();
        let connection = Connection::open(&newer_path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(999, 'future', 'future', 1)",
                [],
            )
            .unwrap();
        drop(connection);
        assert!(matches!(
            ChatHistoryRepository::open(&newer_path),
            Err(HistoryError::UnsupportedSchema)
        ));
        assert!(newer_path.exists());

        let corrupt_path = std::fs::canonicalize(directory.path())
            .unwrap()
            .join(".loxa")
            .join("corrupt.sqlite3");
        prepare_storage_path(&corrupt_path).unwrap();
        std::fs::write(&corrupt_path, b"not a sqlite database").unwrap();
        assert!(matches!(
            ChatHistoryRepository::open(&corrupt_path),
            Err(HistoryError::CorruptDatabase)
        ));
        assert!(corrupt_path.exists());
    }

    #[test]
    fn checksum_valid_ledger_with_missing_recorded_schema_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory).with_file_name("missing-schema.sqlite3");
        prepare_storage_path(&path).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, 0)",
                (
                    MIGRATIONS[0].version,
                    MIGRATIONS[0].name,
                    MIGRATIONS[0].checksum,
                ),
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            ChatHistoryRepository::open(&path),
            Err(HistoryError::CorruptDatabase)
        ));
    }

    #[test]
    fn checksum_valid_ledger_with_altered_recorded_schema_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory).with_file_name("altered-schema.sqlite3");
        prepare_storage_path(&path).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection.execute_batch(MIGRATIONS[0].sql).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, 0)",
                (
                    MIGRATIONS[0].version,
                    MIGRATIONS[0].name,
                    MIGRATIONS[0].checksum,
                ),
            )
            .unwrap();
        connection
            .execute("ALTER TABLE chats ADD COLUMN injected TEXT", [])
            .unwrap();
        drop(connection);

        assert!(matches!(
            ChatHistoryRepository::open(&path),
            Err(HistoryError::CorruptDatabase)
        ));
    }

    #[test]
    fn failed_pending_migration_rolls_back_its_ledger_write_and_preserves_existing_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory);
        prepare_storage_path(&path).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection.execute_batch(MIGRATIONS[0].sql).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, 1)",
                (
                    MIGRATIONS[0].version,
                    MIGRATIONS[0].name,
                    MIGRATIONS[0].checksum,
                ),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO chats(id, title, created_at_ms, updated_at_ms) VALUES('0123456789abcdef0123456789abcdef', 'preserve me', 1, 1)",
                [],
            )
            .unwrap();
        connection
            .execute("CREATE TABLE messages_by_turn(conflict INTEGER)", [])
            .unwrap();
        drop(connection);

        assert!(matches!(
            ChatHistoryRepository::open(&path),
            Err(HistoryError::Database)
        ));

        let connection = Connection::open(&path).unwrap();
        let recorded: i64 = connection
            .query_row("SELECT COUNT(*) FROM loxa_schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        let title: String = connection
            .query_row("SELECT title FROM chats", [], |row| row.get(0))
            .unwrap();
        assert_eq!(recorded, 1);
        assert_eq!(title, "preserve me");
    }

    #[test]
    fn prior_schema_migrates_transactionally_and_preserves_existing_chat_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory);
        prepare_storage_path(&path).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(TRACKING_SCHEMA).unwrap();
        connection.execute_batch(MIGRATIONS[0].sql).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, 1)",
                (
                    MIGRATIONS[0].version,
                    MIGRATIONS[0].name,
                    MIGRATIONS[0].checksum,
                ),
            )
            .unwrap();
        let id = "0123456789abcdef0123456789abcdef";
        connection
            .execute(
                "INSERT INTO chats(id, title, created_at_ms, updated_at_ms) VALUES(?1, 'legacy chat', 1, 1)",
                [id],
            )
            .unwrap();
        drop(connection);

        let repository = ChatHistoryRepository::open(&path).unwrap();
        assert_eq!(repository.schema_version().unwrap(), 3);
        assert_eq!(
            repository
                .get_chat(&ChatId::parse(id).unwrap())
                .unwrap()
                .title
                .as_str(),
            "legacy chat"
        );
        let index_count: i64 = repository
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = 'messages_by_turn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 1);
        let metric_columns: i64 = repository
            .connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('turns') WHERE name IN ('output_tokens', 'total_duration_ms', 'ttft_ms', 'stop_reason')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(metric_columns, 4);
    }

    #[test]
    fn two_megabyte_assistant_content_is_returned_as_bounded_utf8_safe_pages() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("large reply").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        let content = format!("{}{}", "a".repeat(ASSISTANT_CONTENT_MAX_BYTES - 4), "🙂");
        repository
            .finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant(&content).unwrap(),
                None,
                1_020,
            )
            .unwrap();
        let assistant = repository
            .message_summaries_for_turn(&turn.id)
            .unwrap()
            .into_iter()
            .find(|message| message.role == MessageRole::Assistant)
            .unwrap();

        let mut index = 0;
        let mut restored = String::new();
        loop {
            let page = repository.message_page(&assistant.id, index).unwrap();
            assert!(serde_json::to_vec(&page).unwrap().len() < MAX_SERIALIZED_MESSAGE_PAGE_BYTES);
            for segment in &page.segments {
                assert!(segment.content.len() <= RESPONSE_SEGMENT_MAX_BYTES);
                restored.push_str(&segment.content);
            }
            let Some(next) = page.next_segment else { break };
            index = next;
        }
        assert_eq!(restored, content);
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_symlinked_database_or_sqlite_sidecar_before_following_it() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let parent = std::fs::canonicalize(directory.path())
            .unwrap()
            .join(".loxa");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        let target = parent.join("target");
        std::fs::write(&target, "must stay untouched").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let linked_main = parent.join("linked-main.sqlite3");
        symlink(&target, &linked_main).unwrap();
        assert!(matches!(
            ChatHistoryRepository::open(&linked_main),
            Err(HistoryError::Security)
        ));

        let sidecar_path = parent.join("sidecar.sqlite3");
        prepare_storage_path(&sidecar_path).unwrap();
        symlink(&target, sidecar_path.with_file_name("sidecar.sqlite3-wal")).unwrap();
        assert!(matches!(
            ChatHistoryRepository::open(&sidecar_path),
            Err(HistoryError::Security)
        ));
        assert_eq!(
            std::fs::read_to_string(target).unwrap(),
            "must stay untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_a_main_file_inode_swap_at_the_sqlite_open_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let path = history_path(&directory);
        let replacement = path.with_file_name("replacement.sqlite3");
        let original = path.with_file_name("original.sqlite3");
        prepare_storage_path(&path).unwrap();
        prepare_storage_path(&replacement).unwrap();

        let result = ChatHistoryRepository::open_with_boundary_hook(&path, || {
            std::fs::rename(&path, &original).unwrap();
            std::fs::rename(&replacement, &path).unwrap();
        });

        assert!(matches!(result, Err(HistoryError::Security)));
        assert!(original.exists());
        assert!(path.exists());
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    }

    #[test]
    fn timestamps_must_be_nonnegative_and_monotonic_per_chat_and_turn() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        assert_eq!(
            repository.create_chat(-1),
            Err(HistoryError::InvalidTimestamp)
        );
        let chat = repository.create_chat(1_000).unwrap();
        assert_eq!(
            repository.begin_turn(
                &chat.id,
                MessageContent::user("too early").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                999,
            ),
            Err(HistoryError::InvalidTimestamp)
        );
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("on time").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        assert_eq!(
            repository.checkpoint_assistant(
                &turn.id,
                MessageContent::assistant("late").unwrap(),
                1_009,
            ),
            Err(HistoryError::InvalidTimestamp)
        );
        assert_eq!(
            repository.finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("late").unwrap(),
                None,
                1_009,
            ),
            Err(HistoryError::InvalidTimestamp)
        );
    }

    #[test]
    fn beginning_a_turn_cannot_regress_the_chat_timestamp() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        repository
            .rename_chat(&chat.id, Title::new("Renamed").unwrap(), 1_100)
            .unwrap();

        assert_eq!(
            repository.begin_turn(
                &chat.id,
                MessageContent::user("too early").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_050,
            ),
            Err(HistoryError::InvalidTimestamp)
        );
    }

    #[test]
    fn finalizing_a_checkpointed_turn_cannot_regress_its_timestamp() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("stream this").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        repository
            .checkpoint_assistant(
                &turn.id,
                MessageContent::assistant("partial").unwrap(),
                1_020,
            )
            .unwrap();

        assert_eq!(
            repository.finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("complete").unwrap(),
                None,
                1_015,
            ),
            Err(HistoryError::InvalidTimestamp)
        );
        let restored = repository.get_turn(&turn.id).unwrap();
        assert_eq!(restored.state, TurnState::Streaming);
        assert_eq!(restored.updated_at_ms, 1_020);
        assert_eq!(
            repository.messages_for_turn(&turn.id).unwrap()[1]
                .content
                .as_str(),
            "partial"
        );
    }

    #[test]
    fn checkpoint_rolls_back_when_the_assistant_message_is_missing() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("checkpoint").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        repository
            .connection
            .execute(
                "DELETE FROM messages WHERE turn_id = ?1 AND role = 'assistant'",
                [turn.id.as_str()],
            )
            .unwrap();

        assert_eq!(
            repository.checkpoint_assistant(
                &turn.id,
                MessageContent::assistant("partial").unwrap(),
                1_020,
            ),
            Err(HistoryError::CorruptDatabase)
        );
        let restored = repository.get_turn(&turn.id).unwrap();
        assert_eq!(restored.state, TurnState::Queued);
        assert_eq!(restored.updated_at_ms, 1_010);
    }

    #[test]
    fn finalize_rolls_back_when_the_assistant_message_is_missing() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        let turn = repository
            .begin_turn(
                &chat.id,
                MessageContent::user("finalize").unwrap(),
                TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                1_010,
            )
            .unwrap();
        repository
            .connection
            .execute(
                "DELETE FROM messages WHERE turn_id = ?1 AND role = 'assistant'",
                [turn.id.as_str()],
            )
            .unwrap();

        assert_eq!(
            repository.finalize_turn(
                &turn.id,
                TurnState::Completed,
                MessageContent::assistant("complete").unwrap(),
                None,
                1_020,
            ),
            Err(HistoryError::CorruptDatabase)
        );
        let restored = repository.get_turn(&turn.id).unwrap();
        assert_eq!(restored.state, TurnState::Queued);
        assert_eq!(restored.updated_at_ms, 1_010);
    }

    #[test]
    fn turns_use_ordinal_keyset_pages_without_returning_message_content() {
        let directory = tempfile::tempdir().unwrap();
        let mut repository = ChatHistoryRepository::open(&history_path(&directory)).unwrap();
        let chat = repository.create_chat(1_000).unwrap();
        for (index, created_at_ms) in [1_010, 1_020, 1_030].into_iter().enumerate() {
            let turn = repository
                .begin_turn(
                    &chat.id,
                    MessageContent::user(&format!("turn {index}")).unwrap(),
                    TurnProvenance::new("gemma-3-4b-it-q4", None, None).unwrap(),
                    created_at_ms,
                )
                .unwrap();
            repository
                .finalize_turn(
                    &turn.id,
                    TurnState::Completed,
                    MessageContent::assistant("finished").unwrap(),
                    None,
                    created_at_ms + 1,
                )
                .unwrap();
        }

        let first = repository.list_turns(&chat.id, 2, None).unwrap();
        assert_eq!(
            first
                .turns
                .iter()
                .map(|turn| turn.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(first.next_after.is_some());
        let second = repository
            .list_turns(&chat.id, 2, first.next_after.as_ref())
            .unwrap();
        assert_eq!(
            second
                .turns
                .iter()
                .map(|turn| turn.ordinal)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(second.next_after.is_none());
    }
}
