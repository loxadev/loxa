use super::super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) const MAX_CONTENT_RANGE_BYTES: usize = 24 * 1024;
const MAX_SUFFIX_BYTES: usize = 64 * 1024;
const MAX_ATTEMPT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RANGE_CHUNKS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContentRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) prefix_end: u64,
    pub(crate) content: String,
}

pub(crate) fn capture_attempt_prefix(
    connection: &Connection,
    attempt_id: [u8; 16],
) -> Result<u64, HistoryError> {
    connection
        .query_row(
            "SELECT a.saved_end FROM attempts a
             JOIN turns t ON t.id = a.turn_id
             JOIN conversations c ON c.id = t.conversation_id
             WHERE a.id = ?1 AND c.deleted = 0",
            [attempt_id.as_slice()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(schema::classify_sql_error)?
        .ok_or_else(|| not_found("attempt was not found"))
        .and_then(from_i64)
}

pub(crate) fn read_attempt_range(
    connection: &Connection,
    attempt_id: [u8; 16],
    start: u64,
    prefix_end: u64,
) -> Result<ContentRange, HistoryError> {
    let captured = capture_attempt_prefix(connection, attempt_id)?;
    if start > prefix_end || prefix_end > captured {
        return Err(invalid("invalid captured attempt range"));
    }
    let target = prefix_end.min(start.saturating_add(MAX_CONTENT_RANGE_BYTES as u64));
    let mut statement = connection
        .prepare(
            "SELECT start_offset, end_offset, content FROM attempt_chunks
             WHERE attempt_id = ?1
               AND start_offset >= COALESCE((
                    SELECT MAX(start_offset) FROM attempt_chunks
                    WHERE attempt_id = ?1 AND start_offset <= ?2
               ), ?2)
               AND start_offset < ?3
             ORDER BY start_offset LIMIT 16",
        )
        .map_err(schema::classify_sql_error)?;
    let mut rows = statement
        .query(params![
            attempt_id.as_slice(),
            to_i64(start)?,
            to_i64(target)?
        ])
        .map_err(schema::classify_sql_error)?;
    let mut cursor = start;
    let mut output = String::with_capacity((target - start) as usize);
    let mut examined = 0;
    while cursor < target {
        let Some(row) = rows.next().map_err(schema::classify_sql_error)? else {
            break;
        };
        examined += 1;
        let chunk_start = from_i64(row.get(0).map_err(schema::classify_sql_error)?)?;
        let chunk_end = from_i64(row.get(1).map_err(schema::classify_sql_error)?)?;
        let bytes = match row.get_ref(2).map_err(schema::classify_sql_error)? {
            rusqlite::types::ValueRef::Text(bytes)
                if !bytes.is_empty() && bytes.len() <= MAX_SUFFIX_BYTES =>
            {
                bytes
            }
            _ => return Err(corrupt("attempt content chunk is invalid")),
        };
        if chunk_end != chunk_start.saturating_add(bytes.len() as u64)
            || chunk_start > cursor
            || cursor > chunk_end
        {
            return Err(corrupt("attempt content is not contiguous"));
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| corrupt("attempt content chunk is not UTF-8"))?;
        let local_start = (cursor - chunk_start) as usize;
        if !text.is_char_boundary(local_start) {
            return Err(invalid("attempt range does not start at a UTF-8 boundary"));
        }
        let available = bytes.len() - local_start;
        let wanted = ((target - cursor) as usize).min(available);
        let mut local_end = local_start + wanted;
        while local_end > local_start && !text.is_char_boundary(local_end) {
            local_end -= 1;
        }
        if local_end == local_start {
            break;
        }
        output.push_str(&text[local_start..local_end]);
        cursor += (local_end - local_start) as u64;
        if cursor < chunk_end && cursor < target {
            break;
        }
        if examined == MAX_RANGE_CHUNKS {
            break;
        }
    }
    if cursor == start && start < target {
        return Err(corrupt("captured attempt content is missing"));
    }
    Ok(ContentRange {
        start,
        end: cursor,
        prefix_end,
        content: output,
    })
}

fn to_i64(value: u64) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_| invalid("history offset exceeds the storage range"))
}

fn from_i64(value: i64) -> Result<u64, HistoryError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_ATTEMPT_BYTES)
        .ok_or_else(|| corrupt("stored history offset is invalid"))
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
