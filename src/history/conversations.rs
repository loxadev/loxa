use super::identity::{
    decode_id, encode_id, next_updated_ms, parse_nonnegative, parse_revision, random_id,
    unix_time_ms,
};
use super::{HistoryError, HistoryErrorKind};
use crate::runtime_identity::RuntimeIdentity;
use loxa_ipc::{
    ConversationCursor, ConversationPage, ConversationSummary, GenerationSettings, HistoryCommand,
    HistoryReply,
};
use rusqlite::{params, Connection, OptionalExtension};

mod binding;
use binding::{source_columns, Binding};
mod profile;

const MAX_TITLE_BYTES: usize = 256;
const MAX_PAGE_ITEMS: u16 = 50;
const MAX_PAGE_BYTES: usize = 24 * 1024;
const MAX_CURSOR_BACKING_BYTES: usize = 32 + 19;
const DEFAULT_TITLE: &str = "New chat";

#[cfg(test)]
pub(super) fn execute(
    connection: &mut Connection,
    models_root: &std::path::Path,
    runtime_identity: RuntimeIdentity,
    operation: HistoryCommand,
) -> Result<HistoryReply, HistoryError> {
    execute_with_generation(connection, models_root, runtime_identity, operation, None)
}

pub(super) fn execute_with_generation(
    connection: &mut Connection,
    models_root: &std::path::Path,
    runtime_identity: RuntimeIdentity,
    operation: HistoryCommand,
    generation: Option<GenerationSettings>,
) -> Result<HistoryReply, HistoryError> {
    match operation {
        HistoryCommand::GetHistoryStatus => Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "history status is not a database operation",
        )),
        HistoryCommand::CreateConversation { model_id } => {
            let binding = Binding::load(models_root, &model_id, runtime_identity)?;
            create(connection, binding, generation.unwrap_or_default())
                .map(HistoryReply::Conversation)
        }
        HistoryCommand::RenameConversation {
            conversation_id,
            expected_revision,
            title,
        } => {
            let id = decode_id(&conversation_id)?;
            let expected = parse_revision(&expected_revision)?;
            validate_title(&title)?;
            rename(connection, id, expected, &title).map(HistoryReply::Conversation)
        }
        HistoryCommand::ListConversations { cursor, limit } => {
            list(connection, cursor, limit).map(HistoryReply::ConversationPage)
        }
        HistoryCommand::ListTurns {
            conversation_id,
            cursor,
            limit,
        } => super::reads::list_turns(connection, &conversation_id, cursor, limit)
            .map(HistoryReply::TurnPage),
        HistoryCommand::ReadContentRange {
            source,
            start,
            prefix_end,
        } => super::reads::read_content_range(connection, source, &start, &prefix_end)
            .map(HistoryReply::ContentRange),
        HistoryCommand::DeleteConversation {
            conversation_id,
            expected_revision,
        } => {
            let id = decode_id(&conversation_id)?;
            let expected = parse_revision(&expected_revision)?;
            let revision = begin_delete(connection, id, expected)?;
            // The tombstone is the logical deletion boundary. A later bounded
            // purge failure remains resumable and must not erase that fact from
            // the acknowledgement.
            let purge_complete = super::purge::delete_batch(connection).unwrap_or(false);
            Ok(HistoryReply::ConversationDeleted {
                conversation_id,
                revision: revision.to_string(),
                purge_complete,
            })
        }
    }
}

pub(super) fn execute_profile(
    connection: &mut Connection,
    operation: loxa_ipc::ServiceSettingsCommand,
    reset_default: Option<GenerationSettings>,
) -> Result<loxa_ipc::ConversationProfile, HistoryError> {
    profile::execute(connection, operation, reset_default)
}

pub(super) fn resume_delete(connection: &mut Connection) -> Result<bool, HistoryError> {
    super::purge::delete_batch(connection)
}

fn create(
    connection: &mut Connection,
    binding: Binding,
    generation: GenerationSettings,
) -> Result<ConversationSummary, HistoryError> {
    crate::config::validate_generation(&generation)
        .map_err(|error| HistoryError::new(HistoryErrorKind::InvalidInput, error))?;
    let id = random_id()?;
    let now = unix_time_ms()?;
    let (primary_kind, primary_repo, primary_revision, primary_source_filename) =
        source_columns(&binding.primary.source);
    let (
        draft_filename,
        draft_sha,
        draft_size,
        draft_kind,
        draft_repo,
        draft_revision,
        draft_source,
    ) = match &binding.draft {
        Some(draft) => {
            let (kind, repo, revision, source) = source_columns(&draft.source);
            (
                Some(draft.local_filename.as_str()),
                Some(draft.sha256.as_slice()),
                Some(draft.size),
                Some(kind),
                repo,
                revision,
                Some(source),
            )
        }
        None => (None, None, None, None, None, None, None),
    };
    let transaction = connection.transaction().map_err(sql_error)?;
    transaction
        .execute(
            "INSERT INTO conversations (
                id, model_id, manifest_version, binding_profile,
                qualified_profile, qualified_engine, qualified_engine_build,
                primary_filename, primary_sha256, primary_size, primary_source_kind,
                primary_source_repo, primary_source_revision, primary_source_filename,
                draft_filename, draft_sha256, draft_size, draft_source_kind,
                draft_source_repo, draft_source_revision, draft_source_filename,
                title, system_instruction, max_output_tokens,
                created_ms, updated_ms, revision, profile_revision, deleted
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18, ?19, ?20, ?21,
                ?22, ?23, ?24, ?25, ?25, 1, 1, 0
             )",
            params![
                id.as_slice(),
                binding.model_id,
                binding.manifest_version,
                binding.effective_profile,
                binding.qualified_profile,
                binding.qualified_engine,
                binding.qualified_engine_build,
                binding.primary.local_filename,
                binding.primary.sha256.as_slice(),
                binding.primary.size,
                primary_kind,
                primary_repo,
                primary_revision,
                primary_source_filename,
                draft_filename,
                draft_sha,
                draft_size,
                draft_kind,
                draft_repo,
                draft_revision,
                draft_source,
                DEFAULT_TITLE,
                generation.system_instruction,
                generation.max_output_tokens,
                now,
            ],
        )
        .map_err(sql_error)?;
    super::sampling::write_conversation(&transaction, id, &generation).map_err(sql_error)?;
    transaction.commit().map_err(sql_error)?;
    Ok(summary(
        id,
        binding.model_id,
        DEFAULT_TITLE.into(),
        now,
        now,
        1,
        1,
    ))
}

fn rename(
    connection: &Connection,
    id: [u8; 16],
    expected_revision: i64,
    title: &str,
) -> Result<ConversationSummary, HistoryError> {
    let existing = read_summary(connection, id)?.ok_or_else(not_found)?;
    if existing.revision != expected_revision.to_string() {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation revision changed",
        ));
    }
    let updated = next_updated_ms(existing.updated_ms.parse().map_err(|_| corrupt_record())?)?;
    let changed = connection
        .execute(
            "UPDATE conversations
             SET title = ?1, updated_ms = ?2, revision = revision + 1
             WHERE id = ?3 AND deleted = 0 AND revision = ?4 AND revision < 9223372036854775807",
            params![title, updated, id.as_slice(), expected_revision],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation revision changed",
        ));
    }
    read_summary(connection, id)?.ok_or_else(corrupt_record)
}

fn list(
    connection: &Connection,
    cursor: Option<ConversationCursor>,
    limit: u16,
) -> Result<ConversationPage, HistoryError> {
    if limit == 0 || limit > MAX_PAGE_ITEMS {
        return Err(HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "conversation page limit must be between 1 and 50",
        ));
    }
    let parsed_cursor = cursor
        .as_ref()
        .map(|cursor| {
            Ok((
                parse_nonnegative(&cursor.updated_ms, "invalid conversation cursor")?,
                decode_id(&cursor.conversation_id)?,
            ))
        })
        .transpose()?;
    let fetch = i64::from(limit) + 1;
    let mut statement = connection
        .prepare(
            "SELECT id, model_id, title, created_ms, updated_ms, revision, profile_revision
             FROM conversations
             WHERE deleted = 0
               AND (?1 IS NULL OR updated_ms < ?1 OR (updated_ms = ?1 AND id < ?2))
             ORDER BY updated_ms DESC, id DESC
             LIMIT ?3",
        )
        .map_err(sql_error)?;
    let (cursor_time, cursor_id): (Option<i64>, Option<Vec<u8>>) = parsed_cursor
        .map(|(time, id)| (Some(time), Some(id.to_vec())))
        .unwrap_or((None, None));
    let rows = statement
        .query_map(params![cursor_time, cursor_id, fetch], row_summary)
        .map_err(sql_error)?;
    let mut summaries = Vec::with_capacity(usize::from(limit));
    let mut backing_bytes = usize::from(limit)
        .checked_mul(std::mem::size_of::<ConversationSummary>())
        .and_then(|bytes| bytes.checked_add(MAX_CURSOR_BACKING_BYTES))
        .ok_or_else(|| {
            HistoryError::new(
                HistoryErrorKind::LimitExceeded,
                "history page backing overflow",
            )
        })?;
    let mut has_more = false;
    for row in rows {
        let item = row.map_err(sql_error)?;
        if summaries.len() == usize::from(limit) {
            has_more = true;
            break;
        }
        let item_bytes = summary_backing_bytes(&item);
        if backing_bytes.saturating_add(item_bytes) > MAX_PAGE_BYTES {
            has_more = true;
            break;
        }
        backing_bytes += item_bytes;
        summaries.push(item);
    }
    while encoded_page_size(&summaries, has_more)? > MAX_PAGE_BYTES {
        if summaries.pop().is_none() {
            return Err(HistoryError::new(
                HistoryErrorKind::LimitExceeded,
                "one conversation row exceeds the page byte budget",
            ));
        }
        has_more = true;
    }
    let summaries = summaries.into_boxed_slice().into_vec();
    let next = if has_more {
        summaries.last().map(|item| ConversationCursor {
            updated_ms: item.updated_ms.clone(),
            conversation_id: item.id.clone(),
        })
    } else {
        None
    };
    Ok(ConversationPage {
        conversations: summaries,
        next,
    })
}

fn summary_backing_bytes(item: &ConversationSummary) -> usize {
    item.id
        .capacity()
        .saturating_add(item.model_id.capacity())
        .saturating_add(item.title.capacity())
        .saturating_add(item.created_ms.capacity())
        .saturating_add(item.updated_ms.capacity())
        .saturating_add(item.revision.capacity())
        .saturating_add(item.profile_revision.capacity())
}

fn begin_delete(
    connection: &Connection,
    id: [u8; 16],
    expected_revision: i64,
) -> Result<i64, HistoryError> {
    let existing = read_summary(connection, id)?.ok_or_else(not_found)?;
    if existing.revision != expected_revision.to_string() {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation revision changed",
        ));
    }
    let updated = next_updated_ms(existing.updated_ms.parse().map_err(|_| corrupt_record())?)?;
    let changed = connection
        .execute(
            "UPDATE conversations
             SET deleted = 1, updated_ms = ?1, revision = revision + 1
             WHERE id = ?2 AND deleted = 0 AND revision = ?3 AND revision < 9223372036854775807",
            params![updated, id.as_slice(), expected_revision],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation revision changed",
        ));
    }
    expected_revision.checked_add(1).ok_or_else(|| {
        HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "conversation revision overflow",
        )
    })
}

fn read_summary(
    connection: &Connection,
    id: [u8; 16],
) -> Result<Option<ConversationSummary>, HistoryError> {
    connection
        .query_row(
            "SELECT id, model_id, title, created_ms, updated_ms, revision, profile_revision
             FROM conversations WHERE id = ?1 AND deleted = 0",
            [id.as_slice()],
            row_summary,
        )
        .optional()
        .map_err(sql_error)
}

fn row_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConversationSummary> {
    use rusqlite::types::ValueRef;

    let id = match row.get_ref(0)? {
        ValueRef::Blob(bytes) if bytes.len() == 16 => bytes.try_into().expect("checked length"),
        value => {
            return Err(invalid_column(
                0,
                value.data_type(),
                "invalid conversation identity",
            ))
        }
    };
    let model_id = match row.get_ref(1)? {
        ValueRef::Text(bytes) if !bytes.is_empty() && bytes.len() <= 120 => {
            std::str::from_utf8(bytes)
                .ok()
                .filter(|value| crate::paths::validate_id(value).is_ok())
                .ok_or_else(|| invalid_column(1, value_type(row, 1), "invalid model identity"))?
        }
        value => {
            return Err(invalid_column(
                1,
                value.data_type(),
                "invalid model identity",
            ))
        }
    };
    let title = match row.get_ref(2)? {
        ValueRef::Text(bytes) if !bytes.is_empty() && bytes.len() <= MAX_TITLE_BYTES => {
            std::str::from_utf8(bytes)
                .map_err(|_| invalid_column(2, rusqlite::types::Type::Text, "invalid title"))?
        }
        value => return Err(invalid_column(2, value.data_type(), "invalid title")),
    };
    let created_ms = positive_or_zero(row, 3, "invalid creation time")?;
    let updated_ms = positive_or_zero(row, 4, "invalid update time")?;
    let revision = positive(row, 5, "invalid conversation revision")?;
    let profile_revision = positive(row, 6, "invalid profile revision")?;
    if updated_ms < created_ms {
        return Err(invalid_column(
            4,
            rusqlite::types::Type::Integer,
            "invalid update time",
        ));
    }
    Ok(summary(
        id,
        model_id.to_owned(),
        title.to_owned(),
        created_ms,
        updated_ms,
        revision,
        profile_revision,
    ))
}

fn value_type(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::types::Type {
    row.get_ref(column)
        .map_or(rusqlite::types::Type::Null, |value| value.data_type())
}

fn positive_or_zero(
    row: &rusqlite::Row<'_>,
    column: usize,
    context: &'static str,
) -> rusqlite::Result<i64> {
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Integer(value) if value >= 0 => Ok(value),
        value => Err(invalid_column(column, value.data_type(), context)),
    }
}

fn positive(
    row: &rusqlite::Row<'_>,
    column: usize,
    context: &'static str,
) -> rusqlite::Result<i64> {
    match row.get_ref(column)? {
        rusqlite::types::ValueRef::Integer(value) if value > 0 => Ok(value),
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

fn summary(
    id: [u8; 16],
    model_id: String,
    title: String,
    created_ms: i64,
    updated_ms: i64,
    revision: i64,
    profile_revision: i64,
) -> ConversationSummary {
    ConversationSummary {
        id: encode_id(id),
        model_id,
        title,
        created_ms: created_ms.to_string(),
        updated_ms: updated_ms.to_string(),
        revision: revision.to_string(),
        profile_revision: profile_revision.to_string(),
    }
}

fn validate_title(title: &str) -> Result<(), HistoryError> {
    if title.is_empty() || title.len() > MAX_TITLE_BYTES {
        Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "conversation title must contain 1 to 256 UTF-8 bytes",
        ))
    } else {
        Ok(())
    }
}

fn encoded_page_size(items: &[ConversationSummary], has_more: bool) -> Result<usize, HistoryError> {
    let next = if has_more {
        items.last().map(|item| ConversationCursor {
            updated_ms: item.updated_ms.clone(),
            conversation_id: item.id.clone(),
        })
    } else {
        None
    };
    serde_json::to_vec(&ConversationPage {
        conversations: items.to_vec(),
        next,
    })
    .map(|bytes| bytes.len())
    .map_err(|_| HistoryError::new(HistoryErrorKind::Io, "history page encoding failed"))
}

fn not_found() -> HistoryError {
    HistoryError::new(HistoryErrorKind::NotFound, "conversation was not found")
}

fn corrupt_record() -> HistoryError {
    HistoryError::new(HistoryErrorKind::Corrupt, "conversation record is invalid")
}

fn sql_error(error: rusqlite::Error) -> HistoryError {
    super::schema::classify_sql_error(error)
}
