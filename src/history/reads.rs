use super::identity::{decode_id, encode_id, parse_nonnegative};
use super::{content, schema, statistics, HistoryError, HistoryErrorKind};
use loxa_ipc::{
    AttemptExecution, AttemptSave, AttemptSummary, ContentRange, ContentSource, TurnCursor,
    TurnPage, TurnSummary,
};
use rusqlite::{params, Connection, OptionalExtension};

const MAX_PAGE_ITEMS: u16 = 50;
const MAX_PAGE_BYTES: usize = 24 * 1024;
const MAX_RANGE_BYTES: usize = 24 * 1024;
const MAX_ATTEMPT_BYTES: i64 = 16 * 1024 * 1024;
const MAX_USER_BYTES: usize = 32 * 1024;

pub(super) const LIST_TURNS_AFTER_SQL: &str =
    "SELECT t.id, t.ordinal, length(CAST(t.user_text AS BLOB)),
            t.selected_attempt_id, a.id, a.turn_id, a.attempt_number,
            a.execution_outcome, a.save_outcome, a.saved_end, a.generated_end,
            a.terminal_saved_end, a.failure_code, a.created_ms, a.updated_ms,
            s.attempt_id, s.qualified_input_tokens, s.qualified_output_tokens,
            s.service_first_output_latency_ms,
            s.qualified_engine_decode_tokens_per_second, s.service_total_duration_ms,
            s.stop_reason
     FROM turns t LEFT JOIN attempts a ON a.id = t.selected_attempt_id
     LEFT JOIN attempt_statistics s ON s.attempt_id = a.id
     WHERE t.conversation_id = ?1 AND t.ordinal < ?2
     ORDER BY t.ordinal DESC LIMIT ?3";

const LIST_TURNS_FIRST_SQL: &str = "SELECT t.id, t.ordinal, length(CAST(t.user_text AS BLOB)),
            t.selected_attempt_id, a.id, a.turn_id, a.attempt_number,
            a.execution_outcome, a.save_outcome, a.saved_end, a.generated_end,
            a.terminal_saved_end, a.failure_code, a.created_ms, a.updated_ms,
            s.attempt_id, s.qualified_input_tokens, s.qualified_output_tokens,
            s.service_first_output_latency_ms,
            s.qualified_engine_decode_tokens_per_second, s.service_total_duration_ms,
            s.stop_reason
     FROM turns t LEFT JOIN attempts a ON a.id = t.selected_attempt_id
     LEFT JOIN attempt_statistics s ON s.attempt_id = a.id
     WHERE t.conversation_id = ?1
     ORDER BY t.ordinal DESC LIMIT ?2";

pub(super) fn list_turns(
    connection: &Connection,
    conversation_id: &str,
    cursor: Option<TurnCursor>,
    limit: u16,
) -> Result<TurnPage, HistoryError> {
    if limit == 0 || limit > MAX_PAGE_ITEMS {
        return Err(invalid("turn page limit must be between 1 and 50"));
    }
    let conversation_id = decode_id(conversation_id)?;
    require_conversation(connection, conversation_id)?;
    let cursor = cursor
        .as_ref()
        .map(|value| parse_positive(&value.ordinal, "invalid turn cursor"))
        .transpose()?;
    let mut statement = connection
        .prepare(if cursor.is_some() {
            LIST_TURNS_AFTER_SQL
        } else {
            LIST_TURNS_FIRST_SQL
        })
        .map_err(schema::classify_sql_error)?;
    let mut rows = if let Some(cursor) = cursor {
        statement
            .query(params![
                conversation_id.as_slice(),
                cursor,
                i64::from(limit) + 1
            ])
            .map_err(schema::classify_sql_error)?
    } else {
        statement
            .query(params![conversation_id.as_slice(), i64::from(limit) + 1])
            .map_err(schema::classify_sql_error)?
    };
    let mut turns = Vec::with_capacity(usize::from(limit));
    let mut backing = turns
        .capacity()
        .saturating_mul(std::mem::size_of::<TurnSummary>());
    let mut has_more = false;
    while let Some(row) = rows.next().map_err(schema::classify_sql_error)? {
        let turn = turn_row(row).map_err(|_| corrupt("stored turn metadata is invalid"))?;
        if turns.len() == usize::from(limit) {
            has_more = true;
            break;
        }
        backing = backing.saturating_add(turn_backing(&turn));
        if backing > MAX_PAGE_BYTES {
            has_more = true;
            break;
        }
        turns.push(turn);
    }
    while encoded_size(&turns, has_more)? > MAX_PAGE_BYTES {
        if turns.pop().is_none() {
            return Err(HistoryError::new(
                HistoryErrorKind::LimitExceeded,
                "one turn row exceeds the page byte budget",
            ));
        }
        has_more = true;
    }
    turns.shrink_to_fit();
    let next = if has_more {
        Some(TurnCursor {
            ordinal: turns
                .last()
                .ok_or_else(|| corrupt("bounded turn page made no progress"))?
                .ordinal
                .clone(),
        })
    } else {
        None
    };
    Ok(TurnPage { turns, next })
}

pub(super) fn read_content_range(
    connection: &Connection,
    source: ContentSource,
    start: &str,
    prefix_end: &str,
) -> Result<ContentRange, HistoryError> {
    let start = parse_nonnegative(start, "invalid content range start")?;
    let prefix_end = parse_nonnegative(prefix_end, "invalid content range prefix end")?;
    match source {
        ContentSource::Assistant { attempt_id } => {
            let range = content::read_attempt_range(
                connection,
                decode_id(&attempt_id)?,
                start as u64,
                prefix_end as u64,
            )?;
            Ok(ContentRange {
                start: range.start.to_string(),
                end: range.end.to_string(),
                prefix_end: range.prefix_end.to_string(),
                content: range.content,
            })
        }
        ContentSource::User { turn_id } => {
            read_user_range(connection, decode_id(&turn_id)?, start, prefix_end)
        }
    }
}

fn read_user_range(
    connection: &Connection,
    turn_id: [u8; 16],
    start: i64,
    prefix_end: i64,
) -> Result<ContentRange, HistoryError> {
    let result = connection
        .query_row(
            "SELECT t.user_text FROM turns t
             JOIN conversations c ON c.id = t.conversation_id
             WHERE t.id = ?1 AND c.deleted = 0",
            [turn_id.as_slice()],
            |row| {
                let bytes = match row.get_ref(0)? {
                    rusqlite::types::ValueRef::Text(bytes)
                        if !bytes.is_empty() && bytes.len() <= MAX_USER_BYTES =>
                    {
                        bytes
                    }
                    _ => return Err(invalid_row(0)),
                };
                let text = std::str::from_utf8(bytes).map_err(|_| invalid_row(0))?;
                if prefix_end != bytes.len() as i64 || start > prefix_end {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "invalid captured user content range".into(),
                    ));
                }
                let start = start as usize;
                if !text.is_char_boundary(start) {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "user content range does not start at a UTF-8 boundary".into(),
                    ));
                }
                let target = bytes.len().min(start.saturating_add(MAX_RANGE_BYTES));
                let mut end = target;
                while end > start && !text.is_char_boundary(end) {
                    end -= 1;
                }
                Ok(ContentRange {
                    start: start.to_string(),
                    end: end.to_string(),
                    prefix_end: bytes.len().to_string(),
                    content: text[start..end].to_owned(),
                })
            },
        )
        .optional();
    match result {
        Ok(Some(range)) => Ok(range),
        Ok(None) => Err(not_found("turn was not found")),
        Err(rusqlite::Error::InvalidParameterName(context)) => Err(invalid(context)),
        Err(_) => Err(corrupt("stored user content is invalid")),
    }
}

fn require_conversation(
    connection: &Connection,
    conversation_id: [u8; 16],
) -> Result<(), HistoryError> {
    connection
        .query_row(
            "SELECT 1 FROM conversations WHERE id = ?1 AND deleted = 0",
            [conversation_id.as_slice()],
            |_| Ok(()),
        )
        .optional()
        .map_err(schema::classify_sql_error)?
        .ok_or_else(|| not_found("conversation was not found"))
}

fn turn_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnSummary> {
    let id = fixed_id(row, 0)?;
    let ordinal = positive(row.get(1)?, 1)?;
    let user_text_length: i64 = row.get(2)?;
    if !(1..=MAX_USER_BYTES as i64).contains(&user_text_length) {
        return Err(invalid_row(2));
    }
    let user_text_end = user_text_length.to_string();
    let selected_attempt = match row.get_ref(3)? {
        rusqlite::types::ValueRef::Null
            if matches!(row.get_ref(4)?, rusqlite::types::ValueRef::Null)
                && matches!(row.get_ref(5)?, rusqlite::types::ValueRef::Null) =>
        {
            None
        }
        rusqlite::types::ValueRef::Blob(selected) if selected.len() == 16 => {
            let attempt_id = fixed_id(row, 4)?;
            let attempt_turn_id = fixed_id(row, 5)?;
            if selected != attempt_id || attempt_turn_id != id {
                return Err(invalid_row(4));
            }
            let execution = match row.get::<_, i64>(7)? {
                0 => AttemptExecution::Pending,
                1 => AttemptExecution::Completed,
                2 => AttemptExecution::Stopped,
                3 => AttemptExecution::Failed,
                4 => AttemptExecution::Interrupted,
                _ => return Err(invalid_row(7)),
            };
            let save = match row.get::<_, i64>(8)? {
                0 => AttemptSave::Open,
                1 => AttemptSave::Saved,
                2 => AttemptSave::Failed,
                3 => AttemptSave::Interrupted,
                _ => return Err(invalid_row(8)),
            };
            let saved_end_value = row.get(9)?;
            let generated_end_value = optional_offset_value(row, 10)?;
            let terminal_saved_end_value = optional_offset_value(row, 11)?;
            if generated_end_value.is_some_and(|end| saved_end_value > end)
                || (save == AttemptSave::Saved
                    && (generated_end_value != Some(saved_end_value)
                        || terminal_saved_end_value != Some(saved_end_value)))
                || (save != AttemptSave::Saved && terminal_saved_end_value.is_some())
                || (save == AttemptSave::Saved && execution == AttemptExecution::Pending)
            {
                return Err(invalid_row(9));
            }
            let created_value: i64 = row.get(13)?;
            let updated_value: i64 = row.get(14)?;
            let created_ms = nonnegative(created_value, 13)?;
            let updated_ms = nonnegative(updated_value, 14)?;
            if updated_value < created_value {
                return Err(invalid_row(14));
            }
            let statistics = statistics::read(row, 15, attempt_id)?
                .map(|statistics| statistics.to_wire(execution, save, 15))
                .transpose()?;
            Some(AttemptSummary {
                id: encode_id(attempt_id),
                attempt_number: positive(row.get(6)?, 6)?,
                execution,
                save,
                saved_end: offset(saved_end_value, 9)?,
                generated_end: generated_end_value.map(|value| value.to_string()),
                terminal_saved_end: terminal_saved_end_value.map(|value| value.to_string()),
                failure_code: optional_ascii(row, 12, 64)?,
                statistics,
                created_ms,
                updated_ms,
            })
        }
        _ => return Err(invalid_row(3)),
    };
    Ok(TurnSummary {
        id: encode_id(id),
        ordinal,
        user_text_end,
        selected_attempt,
    })
}

fn fixed_id(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<[u8; 16]> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
            Ok(bytes.try_into().expect("checked history identity"))
        }
        _ => Err(invalid_row(index)),
    }
}

fn positive(value: i64, index: usize) -> rusqlite::Result<String> {
    (value > 0)
        .then(|| value.to_string())
        .ok_or_else(|| invalid_row(index))
}

fn nonnegative(value: i64, index: usize) -> rusqlite::Result<String> {
    (value >= 0)
        .then(|| value.to_string())
        .ok_or_else(|| invalid_row(index))
}

fn offset(value: i64, index: usize) -> rusqlite::Result<String> {
    (0..=MAX_ATTEMPT_BYTES)
        .contains(&value)
        .then(|| value.to_string())
        .ok_or_else(|| invalid_row(index))
}

fn optional_offset_value(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<i64>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Integer(value) if (0..=MAX_ATTEMPT_BYTES).contains(&value) => {
            Ok(Some(value))
        }
        _ => Err(invalid_row(index)),
    }
}

fn optional_ascii(
    row: &rusqlite::Row<'_>,
    index: usize,
    max: usize,
) -> rusqlite::Result<Option<String>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Text(bytes)
            if !bytes.is_empty() && bytes.len() <= max && bytes.is_ascii() =>
        {
            Ok(Some(
                std::str::from_utf8(bytes)
                    .expect("checked ASCII")
                    .to_owned(),
            ))
        }
        _ => Err(invalid_row(index)),
    }
}

fn parse_positive(value: &str, context: &'static str) -> Result<i64, HistoryError> {
    let value = parse_nonnegative(value, context)?;
    if value == 0 {
        Err(invalid(context))
    } else {
        Ok(value)
    }
}

fn encoded_size(turns: &[TurnSummary], with_cursor: bool) -> Result<usize, HistoryError> {
    serde_json::to_vec(&TurnPage {
        turns: turns.to_vec(),
        next: with_cursor.then(|| TurnCursor {
            ordinal: turns
                .last()
                .map_or_else(|| "1".into(), |turn| turn.ordinal.clone()),
        }),
    })
    .map(|value| value.len())
    .map_err(|_| corrupt("turn page cannot be encoded"))
}

fn turn_backing(turn: &TurnSummary) -> usize {
    let base = turn
        .id
        .capacity()
        .saturating_add(turn.ordinal.capacity())
        .saturating_add(turn.user_text_end.capacity());
    turn.selected_attempt.as_ref().map_or(base, |attempt| {
        base.saturating_add(attempt.id.capacity())
            .saturating_add(attempt.attempt_number.capacity())
            .saturating_add(attempt.saved_end.capacity())
            .saturating_add(attempt.generated_end.as_ref().map_or(0, String::capacity))
            .saturating_add(
                attempt
                    .terminal_saved_end
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(attempt.failure_code.as_ref().map_or(0, String::capacity))
            .saturating_add(attempt.statistics.as_ref().map_or(0, |statistics| {
                statistics
                    .service_first_output_latency_ms
                    .as_ref()
                    .map_or(0, String::capacity)
                    .saturating_add(statistics.service_total_duration_ms.capacity())
            }))
            .saturating_add(attempt.created_ms.capacity())
            .saturating_add(attempt.updated_ms.capacity())
    })
}

fn invalid_row(index: usize) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnType(index, "history".into(), rusqlite::types::Type::Null)
}

fn invalid(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::InvalidInput, context)
}

fn not_found(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::NotFound, context)
}

fn corrupt(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Corrupt, context)
}
