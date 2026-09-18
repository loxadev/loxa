use super::admission::{require_previous_turn_resolved, PromptBasis, PromptReference};
use super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection};

const MAX_PROMPT_REFERENCES: usize = 512;
const MAX_RAW_PROMPT_BYTES: usize = 32 * 1024 * 1024;
const MAX_ASSISTANT_PREFIX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ASSISTANT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_MODEL_ID_BYTES: usize = 120;
const MAX_SYSTEM_INSTRUCTION_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PromptRole {
    User,
    Assistant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PromptMessage {
    pub(crate) role: PromptRole,
    pub(crate) content: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PromptPreparation {
    pub(crate) model_id: String,
    pub(crate) system_instruction: String,
    pub(crate) max_output_tokens: i64,
    pub(crate) basis: PromptBasis,
    pub(crate) messages: Vec<PromptMessage>,
}

pub(super) fn prepare(
    connection: &Connection,
    conversation_id: [u8; 16],
    expected_conversation_revision: i64,
    expected_profile_revision: i64,
    current_user_text: String,
) -> Result<PromptPreparation, HistoryError> {
    if current_user_text.is_empty()
        || current_user_text.len() > 32 * 1024
        || current_user_text.capacity() > 32 * 1024
    {
        return Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "current user text is invalid",
        ));
    }
    let (model_id, system_instruction, max_output_tokens, revision, profile_revision) = {
        let mut statement = connection
            .prepare(
                "SELECT model_id, system_instruction, max_output_tokens, revision, profile_revision
             FROM conversations WHERE id = ?1 AND deleted = 0",
            )
            .map_err(schema::classify_sql_error)?;
        let mut rows = statement
            .query([conversation_id.as_slice()])
            .map_err(schema::classify_sql_error)?;
        let row = rows
            .next()
            .map_err(schema::classify_sql_error)?
            .ok_or_else(|| {
                HistoryError::new(HistoryErrorKind::NotFound, "conversation was not found")
            })?;
        let values = (
            bounded_text(
                row,
                0,
                1,
                MAX_MODEL_ID_BYTES,
                "stored model identity is invalid",
            )?,
            bounded_text(
                row,
                1,
                0,
                MAX_SYSTEM_INSTRUCTION_BYTES,
                "stored system instruction is invalid",
            )?,
            row.get::<_, i64>(2).map_err(schema::classify_sql_error)?,
            row.get::<_, i64>(3).map_err(schema::classify_sql_error)?,
            row.get::<_, i64>(4).map_err(schema::classify_sql_error)?,
        );
        drop(rows);
        drop(statement);
        values
    };
    if !(1..=i64::from(i32::MAX)).contains(&max_output_tokens) {
        return Err(corrupt("stored output reservation is invalid"));
    }
    if revision != expected_conversation_revision || profile_revision != expected_profile_revision {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation or profile revision changed",
        ));
    }
    require_previous_turn_resolved(connection, conversation_id)?;

    let mut statement = connection
        .prepare(
            "SELECT t.id, t.user_text, t.selected_attempt_id, a.saved_end
             FROM turns t LEFT JOIN attempts a
               ON a.id = t.selected_attempt_id AND a.turn_id = t.id
             WHERE t.conversation_id = ?1 ORDER BY t.ordinal",
        )
        .map_err(schema::classify_sql_error)?;
    let mut rows = statement
        .query([conversation_id.as_slice()])
        .map_err(schema::classify_sql_error)?;
    let mut raw_bytes = checked_prompt_size(0, system_instruction.capacity())?;
    raw_bytes = checked_prompt_size(raw_bytes, current_user_text.capacity())?;
    let mut reference_count = 0usize;
    let mut retained = Vec::new();
    while let Some(row) = rows.next().map_err(schema::classify_sql_error)? {
        let turn_id = bounded_id(row, 0, "stored turn identity is invalid")?;
        let user_text = bounded_prompt_text(
            row,
            1,
            1,
            32 * 1024,
            &mut raw_bytes,
            "stored user text is invalid",
        )?;
        let attempt_id = optional_id(row, 2, "stored selected attempt identity is invalid")?;
        let saved_end = match (
            attempt_id,
            row.get::<_, Option<i64>>(3)
                .map_err(schema::classify_sql_error)?,
        ) {
            (Some(attempt_id), Some(saved_end)) => {
                let saved_end = usize::try_from(saved_end)
                    .map_err(|_| corrupt("stored saved output end is invalid"))?;
                if saved_end > MAX_ASSISTANT_PREFIX_BYTES {
                    return Err(corrupt("stored saved output end exceeds 16 MiB"));
                }
                Some((attempt_id, saved_end))
            }
            (None, None) => None,
            _ => return Err(corrupt("stored selected attempt is incomplete")),
        };
        retained.push((turn_id, user_text, saved_end));
        reference_count = reference_count
            .checked_add(1 + usize::from(saved_end.is_some_and(|(_, end)| end > 0)))
            .ok_or_else(|| limit("prompt reference count overflow"))?;
        if reference_count > MAX_PROMPT_REFERENCES {
            return Err(limit("prompt contains more than 512 retained references"));
        }
    }
    drop(rows);
    drop(statement);

    let mut basis = PromptBasis {
        references: Vec::with_capacity(reference_count),
    };
    let mut messages = Vec::with_capacity(reference_count.saturating_add(1));
    for (turn_id, user_text, selected) in retained {
        basis.references.push(PromptReference {
            turn_id,
            attempt_id: None,
            prefix_end: user_text.len() as u64,
        });
        messages.push(PromptMessage {
            role: PromptRole::User,
            content: user_text,
        });
        if let Some((attempt_id, saved_end)) = selected {
            if saved_end == 0 {
                continue;
            }
            raw_bytes = checked_prompt_size(raw_bytes, saved_end)?;
            let assistant = read_attempt(connection, attempt_id, saved_end)?;
            basis.references.push(PromptReference {
                turn_id,
                attempt_id: Some(attempt_id),
                prefix_end: saved_end as u64,
            });
            messages.push(PromptMessage {
                role: PromptRole::Assistant,
                content: assistant,
            });
        }
    }
    messages.push(PromptMessage {
        role: PromptRole::User,
        content: current_user_text,
    });
    Ok(PromptPreparation {
        model_id,
        system_instruction,
        max_output_tokens,
        basis,
        messages,
    })
}

fn read_attempt(
    connection: &Connection,
    attempt_id: [u8; 16],
    capacity: usize,
) -> Result<String, HistoryError> {
    if capacity > MAX_ASSISTANT_PREFIX_BYTES {
        return Err(corrupt("stored attempt prefix exceeds 16 MiB"));
    }
    let mut content = String::with_capacity(capacity);
    if content.capacity() > capacity || content.capacity() > MAX_ASSISTANT_PREFIX_BYTES {
        return Err(corrupt("stored attempt prefix backing exceeds 16 MiB"));
    }
    if capacity == 0 {
        return Ok(content);
    }

    let prefix_end = i64::try_from(capacity)
        .map_err(|_| corrupt("stored attempt prefix exceeds the storage range"))?;
    let mut statement = connection
        .prepare(
            "SELECT start_offset, end_offset, content FROM attempt_chunks
             WHERE attempt_id = ?1 AND start_offset < ?2
             ORDER BY start_offset",
        )
        .map_err(schema::classify_sql_error)?;
    let mut rows = statement
        .query(params![attempt_id.as_slice(), prefix_end])
        .map_err(schema::classify_sql_error)?;
    let mut cursor = 0usize;
    while let Some(row) = rows.next().map_err(schema::classify_sql_error)? {
        let start = row
            .get::<_, i64>(0)
            .map_err(schema::classify_sql_error)
            .and_then(stored_attempt_offset)?;
        let end = row
            .get::<_, i64>(1)
            .map_err(schema::classify_sql_error)
            .and_then(stored_attempt_offset)?;
        let bytes = match row.get_ref(2).map_err(schema::classify_sql_error)? {
            rusqlite::types::ValueRef::Text(bytes)
                if !bytes.is_empty() && bytes.len() <= MAX_ASSISTANT_CHUNK_BYTES =>
            {
                bytes
            }
            _ => return Err(corrupt("stored attempt content chunk is invalid")),
        };
        let text = std::str::from_utf8(bytes)
            .map_err(|_| corrupt("stored attempt content chunk is not UTF-8"))?;
        let expected_end = start
            .checked_add(bytes.len())
            .ok_or_else(|| corrupt("stored attempt content offset overflow"))?;
        if start != cursor || end != expected_end || end > capacity {
            return Err(corrupt("stored attempt content is not contiguous"));
        }
        content.push_str(text);
        if content.len() != end
            || content.capacity() > capacity
            || content.capacity() > MAX_ASSISTANT_PREFIX_BYTES
        {
            return Err(corrupt(
                "stored attempt content exceeded its captured prefix",
            ));
        }
        cursor = end;
    }
    if cursor != capacity || content.len() != capacity || content.capacity() > capacity {
        return Err(corrupt("stored attempt content length changed"));
    }
    Ok(content)
}

fn stored_attempt_offset(value: i64) -> Result<usize, HistoryError> {
    usize::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_ASSISTANT_PREFIX_BYTES)
        .ok_or_else(|| corrupt("stored attempt content offset is invalid"))
}

fn checked_prompt_size(current: usize, additional: usize) -> Result<usize, HistoryError> {
    let next = current
        .checked_add(additional)
        .ok_or_else(|| limit("raw prompt exceeds 32 MiB"))?;
    if next > MAX_RAW_PROMPT_BYTES {
        return Err(limit("raw prompt exceeds 32 MiB"));
    }
    Ok(next)
}

fn bounded_id(
    row: &rusqlite::Row<'_>,
    index: usize,
    context: &'static str,
) -> Result<[u8; 16], HistoryError> {
    match row.get_ref(index).map_err(schema::classify_sql_error)? {
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
            Ok(bytes.try_into().expect("checked identity length"))
        }
        _ => Err(corrupt(context)),
    }
}

fn optional_id(
    row: &rusqlite::Row<'_>,
    index: usize,
    context: &'static str,
) -> Result<Option<[u8; 16]>, HistoryError> {
    match row.get_ref(index).map_err(schema::classify_sql_error)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
            Ok(Some(bytes.try_into().expect("checked identity length")))
        }
        _ => Err(corrupt(context)),
    }
}

fn bounded_text(
    row: &rusqlite::Row<'_>,
    index: usize,
    minimum: usize,
    maximum: usize,
    context: &'static str,
) -> Result<String, HistoryError> {
    match row.get_ref(index).map_err(schema::classify_sql_error)? {
        rusqlite::types::ValueRef::Text(bytes) if (minimum..=maximum).contains(&bytes.len()) => {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| corrupt(context))
        }
        _ => Err(corrupt(context)),
    }
}

fn bounded_prompt_text(
    row: &rusqlite::Row<'_>,
    index: usize,
    minimum: usize,
    maximum: usize,
    raw_bytes: &mut usize,
    context: &'static str,
) -> Result<String, HistoryError> {
    match row.get_ref(index).map_err(schema::classify_sql_error)? {
        rusqlite::types::ValueRef::Text(bytes) if (minimum..=maximum).contains(&bytes.len()) => {
            let text = std::str::from_utf8(bytes).map_err(|_| corrupt(context))?;
            *raw_bytes = checked_prompt_size(*raw_bytes, bytes.len())?;
            Ok(text.to_owned())
        }
        _ => Err(corrupt(context)),
    }
}

fn limit(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::LimitExceeded, context)
}

fn corrupt(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Corrupt, context)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTEMPT_ID: [u8; 16] = [7; 16];

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE attempt_chunks (
                    attempt_id BLOB NOT NULL,
                    start_offset INTEGER NOT NULL,
                    end_offset INTEGER NOT NULL,
                    content TEXT NOT NULL
                )",
            )
            .unwrap();
        connection
    }

    fn insert_chunk(connection: &Connection, start: i64, end: i64, content: &str) {
        connection
            .execute(
                "INSERT INTO attempt_chunks (attempt_id, start_offset, end_offset, content)
                 VALUES (?1, ?2, ?3, ?4)",
                params![ATTEMPT_ID.as_slice(), start, end, content],
            )
            .unwrap();
    }

    #[test]
    fn attempt_prefix_reader_preserves_contiguous_multichunk_utf8() {
        let connection = connection();
        let chunks = ["café", "你好", " 👋"];
        let mut cursor = 0i64;
        for chunk in chunks {
            let end = cursor + i64::try_from(chunk.len()).unwrap();
            insert_chunk(&connection, cursor, end, chunk);
            cursor = end;
        }

        let content =
            read_attempt(&connection, ATTEMPT_ID, usize::try_from(cursor).unwrap()).unwrap();
        assert_eq!(content, "café你好 👋");
        assert_eq!(content.capacity(), content.len());
    }

    #[test]
    fn attempt_prefix_reader_rejects_corrupt_ranges() {
        type StoredChunk<'a> = (i64, i64, &'a str);
        type CorruptionCase<'a> = (&'a [StoredChunk<'a>], usize);

        let cases: &[CorruptionCase<'_>] = &[
            (&[(1, 2, "a")], 2),
            (&[(0, 1, "a"), (0, 1, "b")], 1),
            (&[(0, 2, "a")], 2),
            (&[(0, 2, "ab")], 1),
            (&[(0, 1, "a")], 2),
        ];
        for (chunks, prefix_end) in cases {
            let connection = connection();
            for (start, end, content) in *chunks {
                insert_chunk(&connection, *start, *end, content);
            }
            assert_eq!(
                read_attempt(&connection, ATTEMPT_ID, *prefix_end)
                    .unwrap_err()
                    .kind(),
                HistoryErrorKind::Corrupt
            );
        }
    }
}
