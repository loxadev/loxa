use super::identity::{
    decode_id, encode_id, next_updated_ms, parse_nonnegative, random_id, unix_time_ms,
};
use super::{schema, HistoryError, HistoryErrorKind};
use loxa_ipc::{DraftCommand, DraftReply, DraftSnapshot, MAX_DRAFT_TEXT_BYTES};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

const MAX_DRAFTS_PER_ROOT: i64 = 256;
const MAX_DRAFTS_PER_CLIENT: i64 = 32;

pub(super) fn execute(
    connection: &mut Connection,
    operation: DraftCommand,
) -> Result<DraftReply, HistoryError> {
    match operation {
        DraftCommand::CreateScope {
            desktop_client_id,
            conversation_id,
        } => create_scope(connection, &desktop_client_id, conversation_id.as_deref())
            .map(DraftReply::Snapshot),
        DraftCommand::SaveSnapshot {
            draft_id,
            desktop_client_id,
            revision,
            text,
        } => save_snapshot(connection, &draft_id, &desktop_client_id, &revision, &text)
            .map(DraftReply::Snapshot),
        DraftCommand::ReadScope {
            draft_id,
            desktop_client_id,
        } => read_scope(connection, &draft_id, &desktop_client_id).map(DraftReply::Snapshot),
        DraftCommand::DiscardScope {
            draft_id,
            desktop_client_id,
        } => {
            discard_scope(connection, &draft_id, &desktop_client_id)?;
            Ok(DraftReply::Discarded { draft_id })
        }
    }
}

fn create_scope(
    connection: &mut Connection,
    desktop_client_id: &str,
    conversation_id: Option<&str>,
) -> Result<DraftSnapshot, HistoryError> {
    let client = decode_id(desktop_client_id)?;
    let conversation = conversation_id.map(decode_id).transpose()?;
    let transaction = connection
        .transaction()
        .map_err(schema::classify_sql_error)?;
    if let Some(conversation) = conversation {
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM conversations WHERE id = ?1 AND deleted = 0)",
                [conversation.as_slice()],
                |row| row.get(0),
            )
            .map_err(schema::classify_sql_error)?;
        if !exists {
            return Err(not_found("conversation was not found"));
        }
    }
    let (total, for_client): (i64, i64) = transaction
        .query_row(
            "SELECT COUNT(*), SUM(CASE WHEN desktop_client_id = ?1 THEN 1 ELSE 0 END) FROM drafts",
            [client.as_slice()],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        )
        .map_err(schema::classify_sql_error)?;
    if total >= MAX_DRAFTS_PER_ROOT || for_client >= MAX_DRAFTS_PER_CLIENT {
        return Err(HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "draft inventory is full",
        ));
    }
    let id = random_id()?;
    let now = unix_time_ms()?;
    let empty_hash = Sha256::digest([]);
    transaction
        .execute(
            "INSERT INTO drafts (
                id, desktop_client_id, conversation_id, revision, consumed_revision,
                text, text_hash, updated_ms
             ) VALUES (?1, ?2, ?3, 0, 0, '', ?4, ?5)",
            params![
                id.as_slice(),
                client.as_slice(),
                conversation.as_ref().map(<[u8; 16]>::as_slice),
                empty_hash.as_slice(),
                now,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    transaction.commit().map_err(schema::classify_sql_error)?;
    Ok(snapshot(id, client, conversation, 0, 0, String::new(), now))
}

fn save_snapshot(
    connection: &Connection,
    draft_id: &str,
    desktop_client_id: &str,
    revision: &str,
    text: &str,
) -> Result<DraftSnapshot, HistoryError> {
    if text.len() > MAX_DRAFT_TEXT_BYTES {
        return Err(HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "draft text exceeds 32 KiB",
        ));
    }
    let id = decode_id(draft_id)?;
    let client = decode_id(desktop_client_id)?;
    let revision = parse_nonnegative(revision, "invalid draft revision")?;
    let existing =
        read_row(connection, id, client)?.ok_or_else(|| not_found("draft was not found"))?;
    if revision < existing.revision {
        return Err(conflict("draft revision is stale"));
    }
    let hash = Sha256::digest(text.as_bytes());
    if revision == existing.revision {
        if hash.as_slice() == existing.text_hash.as_slice() && text == existing.text {
            return Ok(existing.into_snapshot());
        }
        return Err(conflict("draft revision has different text"));
    }
    if revision <= existing.consumed_revision {
        return Err(conflict("draft revision was already consumed"));
    }
    let updated = next_updated_ms(existing.updated_ms)?;
    let changed = connection
        .execute(
            "UPDATE drafts
             SET revision = ?1, text = ?2, text_hash = ?3, updated_ms = ?4
             WHERE id = ?5 AND desktop_client_id = ?6 AND revision = ?7
               AND (conversation_id IS NULL OR EXISTS(
                    SELECT 1 FROM conversations
                    WHERE conversations.id = drafts.conversation_id AND deleted = 0))",
            params![
                revision,
                text,
                hash.as_slice(),
                updated,
                id.as_slice(),
                client.as_slice(),
                existing.revision,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed != 1 {
        return Err(conflict("draft changed during save"));
    }
    read_row(connection, id, client)?
        .map(DraftRow::into_snapshot)
        .ok_or_else(|| not_found("draft was not found"))
}

fn read_scope(
    connection: &Connection,
    draft_id: &str,
    desktop_client_id: &str,
) -> Result<DraftSnapshot, HistoryError> {
    let id = decode_id(draft_id)?;
    let client = decode_id(desktop_client_id)?;
    read_row(connection, id, client)?
        .map(DraftRow::into_snapshot)
        .ok_or_else(|| not_found("draft was not found"))
}

fn discard_scope(
    connection: &Connection,
    draft_id: &str,
    desktop_client_id: &str,
) -> Result<(), HistoryError> {
    let id = decode_id(draft_id)?;
    let client = decode_id(desktop_client_id)?;
    let changed = connection
        .execute(
            "DELETE FROM drafts
             WHERE id = ?1 AND desktop_client_id = ?2
               AND (conversation_id IS NULL OR EXISTS(
                    SELECT 1 FROM conversations
                    WHERE conversations.id = drafts.conversation_id AND deleted = 0))",
            params![id.as_slice(), client.as_slice()],
        )
        .map_err(schema::classify_sql_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(not_found("draft was not found"))
    }
}

pub(super) struct SubmittedDraft<'a> {
    pub(super) id: [u8; 16],
    pub(super) desktop_client_id: [u8; 16],
    pub(super) revision: i64,
    pub(super) text: &'a str,
}

pub(super) fn consume_submitted(
    transaction: &Transaction<'_>,
    draft: &SubmittedDraft<'_>,
    conversation_id: [u8; 16],
) -> Result<(), HistoryError> {
    let existing = read_row(transaction, draft.id, draft.desktop_client_id)?
        .ok_or_else(|| not_found("draft was not found"))?;
    if let Some(bound) = existing.conversation_id {
        if bound != conversation_id {
            return Err(conflict("draft belongs to another conversation"));
        }
    }
    if draft.revision <= existing.consumed_revision {
        return Err(conflict("draft revision was already consumed"));
    }
    if draft.revision == existing.revision
        && (existing.text_hash.as_slice() != Sha256::digest(draft.text.as_bytes()).as_slice()
            || existing.text != draft.text)
    {
        return Err(conflict("draft revision has different text"));
    }
    let new_revision = existing.revision.max(draft.revision);
    let keep_newer = existing.revision > draft.revision;
    let (text, hash) = if keep_newer {
        (existing.text, existing.text_hash)
    } else {
        (String::new(), Sha256::digest([]).to_vec())
    };
    let updated = next_updated_ms(existing.updated_ms)?;
    let changed = transaction
        .execute(
            "UPDATE drafts
             SET conversation_id = ?1, revision = ?2, consumed_revision = ?3,
                 text = ?4, text_hash = ?5, updated_ms = ?6
             WHERE id = ?7 AND desktop_client_id = ?8 AND revision = ?9
               AND consumed_revision = ?10
               AND (conversation_id IS NULL OR conversation_id = ?1)",
            params![
                conversation_id.as_slice(),
                new_revision,
                draft.revision.max(existing.consumed_revision),
                text,
                hash,
                updated,
                draft.id.as_slice(),
                draft.desktop_client_id.as_slice(),
                existing.revision,
                existing.consumed_revision,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(conflict("draft changed during admission"))
    }
}

struct DraftRow {
    id: [u8; 16],
    desktop_client_id: [u8; 16],
    conversation_id: Option<[u8; 16]>,
    revision: i64,
    consumed_revision: i64,
    text: String,
    text_hash: Vec<u8>,
    updated_ms: i64,
}

impl DraftRow {
    fn into_snapshot(self) -> DraftSnapshot {
        snapshot(
            self.id,
            self.desktop_client_id,
            self.conversation_id,
            self.revision,
            self.consumed_revision,
            self.text,
            self.updated_ms,
        )
    }
}

fn read_row(
    connection: &Connection,
    id: [u8; 16],
    client: [u8; 16],
) -> Result<Option<DraftRow>, HistoryError> {
    connection
        .query_row(
            "SELECT id, desktop_client_id, conversation_id, revision, consumed_revision,
                    text, text_hash, updated_ms
             FROM drafts
             WHERE id = ?1 AND desktop_client_id = ?2
               AND (conversation_id IS NULL OR EXISTS(
                    SELECT 1 FROM conversations
                    WHERE conversations.id = drafts.conversation_id AND deleted = 0))",
            params![id.as_slice(), client.as_slice()],
            |row| {
                use rusqlite::types::ValueRef;

                let id = blob_id(row, 0)?;
                let desktop_client_id = blob_id(row, 1)?;
                let conversation_id = match row.get_ref(2)? {
                    ValueRef::Null => None,
                    ValueRef::Blob(bytes) if bytes.len() == 16 => {
                        Some(bytes.try_into().expect("checked length"))
                    }
                    value => {
                        return Err(invalid_column(
                            2,
                            value.data_type(),
                            "invalid draft conversation identity",
                        ));
                    }
                };
                let revision = nonnegative_integer(row, 3, "invalid draft revision")?;
                let consumed_revision =
                    nonnegative_integer(row, 4, "invalid consumed draft revision")?;
                if consumed_revision > revision {
                    return Err(invalid_column(
                        4,
                        rusqlite::types::Type::Integer,
                        "invalid consumed draft revision",
                    ));
                }
                let text = match row.get_ref(5)? {
                    ValueRef::Text(bytes) if bytes.len() <= MAX_DRAFT_TEXT_BYTES => {
                        std::str::from_utf8(bytes).map_err(|_| {
                            invalid_column(5, rusqlite::types::Type::Text, "invalid draft text")
                        })?
                    }
                    value => {
                        return Err(invalid_column(5, value.data_type(), "invalid draft text"));
                    }
                };
                let text_hash = match row.get_ref(6)? {
                    ValueRef::Blob(bytes) if bytes.len() == 32 => bytes,
                    value => {
                        return Err(invalid_column(
                            6,
                            value.data_type(),
                            "invalid draft text hash",
                        ));
                    }
                };
                if text_hash != Sha256::digest(text.as_bytes()).as_slice() {
                    return Err(invalid_column(
                        6,
                        rusqlite::types::Type::Blob,
                        "draft text hash does not match its text",
                    ));
                }
                let updated_ms = nonnegative_integer(row, 7, "invalid draft update time")?;
                Ok(DraftRow {
                    id,
                    desktop_client_id,
                    conversation_id,
                    revision,
                    consumed_revision,
                    text: text.to_owned(),
                    text_hash: text_hash.to_vec(),
                    updated_ms,
                })
            },
        )
        .optional()
        .map_err(schema::classify_sql_error)
}

fn blob_id(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<[u8; 16]> {
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
            Ok(bytes.try_into().expect("checked length"))
        }
        value => Err(invalid_column(
            column,
            value.data_type(),
            "invalid draft identity",
        )),
    }
}

fn nonnegative_integer(
    row: &rusqlite::Row<'_>,
    column: usize,
    context: &'static str,
) -> rusqlite::Result<i64> {
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Integer(value) if value >= 0 => Ok(value),
        value => Err(invalid_column(column, value.data_type(), context)),
    }
}

fn invalid_column(
    column: usize,
    kind: rusqlite::types::Type,
    context: &'static str,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        kind,
        std::io::Error::new(std::io::ErrorKind::InvalidData, context).into(),
    )
}

fn snapshot(
    id: [u8; 16],
    client: [u8; 16],
    conversation: Option<[u8; 16]>,
    revision: i64,
    consumed_revision: i64,
    text: String,
    updated_ms: i64,
) -> DraftSnapshot {
    DraftSnapshot {
        id: encode_id(id),
        desktop_client_id: encode_id(client),
        conversation_id: conversation.map(encode_id),
        revision: revision.to_string(),
        consumed_revision: consumed_revision.to_string(),
        text,
        updated_ms: updated_ms.to_string(),
    }
}

fn conflict(context: &'static str) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Conflict, context)
}

fn not_found(context: &'static str) -> HistoryError {
    HistoryError::new(HistoryErrorKind::NotFound, context)
}
