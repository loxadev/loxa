use super::types::{
    AdmissionKind, PreparedAdmission, PromptBasis, MAX_SYSTEM_TEXT_BYTES, MAX_USER_TEXT_BYTES,
};
use super::{conflict, invalid};
use crate::history::{schema, HistoryError, HistoryErrorKind};
use crate::runtime_fingerprint::EffectiveProfile;
use rusqlite::{params, Connection, OptionalExtension, Transaction};

pub(super) fn validate_prepared(prepared: &PreparedAdmission) -> Result<(), HistoryError> {
    if prepared.expected_conversation_revision <= 0
        || prepared.expected_profile_revision <= 0
        || prepared.operation_generation <= 0
    {
        return Err(invalid(
            "admission revisions and generation must be positive",
        ));
    }
    if prepared.owner_epoch.is_empty() || prepared.owner_epoch.len() > 128 {
        return Err(invalid("invalid admission owner epoch"));
    }
    if prepared.system_instruction.len() > MAX_SYSTEM_TEXT_BYTES
        || !(1..=i64::from(i32::MAX)).contains(&prepared.max_output_tokens)
        || !(crate::runtime_fingerprint::SERVICE_MIN_CONTEXT
            ..=crate::runtime_fingerprint::SERVICE_MAX_CONTEXT)
            .contains(&prepared.effective_context)
        || prepared.effective_context > prepared.runtime_fingerprint.effective_context()
    {
        return Err(invalid("invalid generation profile"));
    }
    prepared
        .runtime_fingerprint
        .validate_service_lease(prepared.runtime_fingerprint.model_id())
        .map_err(invalid)?;
    if let AdmissionKind::Send { user_text, draft } = &prepared.kind {
        if user_text.is_empty() || user_text.len() > MAX_USER_TEXT_BYTES {
            return Err(invalid("user text must contain 1 byte to 32 KiB"));
        }
        if let Some(draft) = draft {
            if draft.revision < 0 {
                return Err(invalid("invalid submitted draft snapshot"));
            }
        }
    }
    Ok(())
}

pub(super) struct ConversationAdmission {
    pub(super) model_id: String,
    pub(super) binding_profile: i64,
    pub(super) primary_filename: String,
    pub(super) primary_sha256: Vec<u8>,
    pub(super) primary_size: i64,
    pub(super) draft_filename: Option<String>,
    pub(super) draft_sha256: Option<Vec<u8>>,
    pub(super) draft_size: Option<i64>,
    pub(super) title: String,
    pub(super) system_instruction: String,
    pub(super) max_output_tokens: i64,
    pub(super) updated_ms: i64,
    pub(super) revision: i64,
    pub(super) profile_revision: i64,
}

pub(super) fn read_conversation(
    transaction: &Transaction<'_>,
    id: [u8; 16],
) -> Result<Option<ConversationAdmission>, HistoryError> {
    transaction
        .query_row(
            "SELECT model_id, binding_profile, primary_filename, primary_sha256, primary_size,
                    draft_filename, draft_sha256, draft_size, title, system_instruction,
                    max_output_tokens, updated_ms, revision, profile_revision
            FROM conversations WHERE id = ?1 AND deleted = 0",
            [id.as_slice()],
            |row| {
                Ok(ConversationAdmission {
                    model_id: bounded_text(row, 0, 1, 120, "invalid model identity")?,
                    binding_profile: row.get(1)?,
                    primary_filename: bounded_text(row, 2, 1, 1024, "invalid primary filename")?,
                    primary_sha256: bounded_blob(row, 3, 32, "invalid primary digest")?,
                    primary_size: row.get(4)?,
                    draft_filename: optional_bounded_text(
                        row,
                        5,
                        1,
                        1024,
                        "invalid draft filename",
                    )?,
                    draft_sha256: optional_bounded_blob(row, 6, 32, "invalid draft digest")?,
                    draft_size: row.get(7)?,
                    title: bounded_text(row, 8, 1, 256, "invalid conversation title")?,
                    system_instruction: bounded_text(
                        row,
                        9,
                        0,
                        MAX_SYSTEM_TEXT_BYTES,
                        "invalid system instruction",
                    )?,
                    max_output_tokens: row.get(10)?,
                    updated_ms: row.get(11)?,
                    revision: row.get(12)?,
                    profile_revision: row.get(13)?,
                })
            },
        )
        .optional()
        .map_err(schema::classify_sql_error)
}

fn bounded_text(
    row: &rusqlite::Row<'_>,
    index: usize,
    minimum: usize,
    maximum: usize,
    context: &'static str,
) -> rusqlite::Result<String> {
    use rusqlite::types::ValueRef;

    match row.get_ref(index)? {
        ValueRef::Text(bytes) if (minimum..=maximum).contains(&bytes.len()) => {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|error| invalid_column(index, rusqlite::types::Type::Text, context, error))
        }
        value => Err(invalid_column(
            index,
            value.data_type(),
            context,
            std::io::Error::new(std::io::ErrorKind::InvalidData, context),
        )),
    }
}

fn optional_bounded_text(
    row: &rusqlite::Row<'_>,
    index: usize,
    minimum: usize,
    maximum: usize,
    context: &'static str,
) -> rusqlite::Result<Option<String>> {
    if matches!(row.get_ref(index)?, rusqlite::types::ValueRef::Null) {
        Ok(None)
    } else {
        bounded_text(row, index, minimum, maximum, context).map(Some)
    }
}

fn bounded_blob(
    row: &rusqlite::Row<'_>,
    index: usize,
    length: usize,
    context: &'static str,
) -> rusqlite::Result<Vec<u8>> {
    use rusqlite::types::ValueRef;

    match row.get_ref(index)? {
        ValueRef::Blob(bytes) if bytes.len() == length => Ok(bytes.to_vec()),
        value => Err(invalid_column(
            index,
            value.data_type(),
            context,
            std::io::Error::new(std::io::ErrorKind::InvalidData, context),
        )),
    }
}

fn optional_bounded_blob(
    row: &rusqlite::Row<'_>,
    index: usize,
    length: usize,
    context: &'static str,
) -> rusqlite::Result<Option<Vec<u8>>> {
    if matches!(row.get_ref(index)?, rusqlite::types::ValueRef::Null) {
        Ok(None)
    } else {
        bounded_blob(row, index, length, context).map(Some)
    }
}

fn invalid_column(
    index: usize,
    value_type: rusqlite::types::Type,
    context: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    let _ = context;
    rusqlite::Error::FromSqlConversionFailure(index, value_type, Box::new(source))
}

pub(super) fn validate_conversation(
    prepared: &PreparedAdmission,
    conversation: &ConversationAdmission,
) -> Result<(), HistoryError> {
    if conversation.revision != prepared.expected_conversation_revision
        || conversation.profile_revision != prepared.expected_profile_revision
    {
        return Err(conflict("conversation or profile revision changed"));
    }
    let fingerprint = &prepared.runtime_fingerprint;
    let expected_profile = match fingerprint.effective_profile() {
        EffectiveProfile::Generic => 0,
        EffectiveProfile::Gemma4Mtp => 1,
        EffectiveProfile::PrimaryOnly => {
            return Err(conflict(
                "ready runtime does not match the conversation bundle",
            ));
        }
    };
    let primary_size = i64::try_from(fingerprint.primary_size())
        .map_err(|_| invalid("runtime artifact size exceeds storage range"))?;
    let draft_size = fingerprint
        .draft_size()
        .map(i64::try_from)
        .transpose()
        .map_err(|_| invalid("runtime artifact size exceeds storage range"))?;
    if conversation.model_id != fingerprint.model_id()
        || conversation.binding_profile != expected_profile
        || conversation.primary_filename != fingerprint.primary_local_filename()
        || conversation.primary_sha256 != decode_sha256(fingerprint.primary_sha256())?
        || conversation.primary_size != primary_size
        || conversation.draft_filename.as_deref() != fingerprint.draft_local_filename()
        || conversation.draft_sha256.as_deref()
            != fingerprint
                .draft_sha256()
                .map(decode_sha256)
                .transpose()?
                .as_deref()
        || conversation.draft_size != draft_size
    {
        return Err(conflict(
            "ready runtime does not match the conversation binding",
        ));
    }
    if conversation.system_instruction != prepared.system_instruction
        || conversation.max_output_tokens != prepared.max_output_tokens
    {
        return Err(conflict("generation profile changed"));
    }
    Ok(())
}

pub(super) fn require_previous_turn_resolved(
    connection: &Connection,
    conversation_id: [u8; 16],
) -> Result<(), HistoryError> {
    let blocked: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM attempts a
                WHERE a.id = (
                    SELECT selected_attempt_id FROM turns
                    WHERE conversation_id = ?1 ORDER BY ordinal DESC LIMIT 1)
                  AND (a.execution_outcome = 0 OR a.save_outcome IN (0, 2)))",
            [conversation_id.as_slice()],
            |row| row.get(0),
        )
        .map_err(schema::classify_sql_error)?;
    if blocked {
        Err(HistoryError::new(
            HistoryErrorKind::Busy,
            "conversation has unresolved generation or saving",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn validate_prompt_basis(
    transaction: &Transaction<'_>,
    conversation_id: [u8; 16],
    basis: &PromptBasis,
    excluded_attempt: Option<[u8; 16]>,
) -> Result<(), HistoryError> {
    for (index, reference) in basis.references.iter().enumerate() {
        if basis.references[..index]
            .iter()
            .any(|prior| prior == reference)
        {
            return Err(invalid("prompt basis contains a duplicate reference"));
        }
        let prefix_end = i64::try_from(reference.prefix_end)
            .map_err(|_| invalid("prompt basis prefix exceeds the storage range"))?;
        let valid = match reference.attempt_id {
            None => valid_user_prefix(
                transaction,
                reference.turn_id,
                conversation_id,
                reference.prefix_end,
            )?,
            Some(attempt_id) if Some(attempt_id) == excluded_attempt => false,
            Some(attempt_id) => transaction
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM attempts a JOIN turns t ON t.id = a.turn_id
                        WHERE t.id = ?1 AND t.conversation_id = ?2 AND a.id = ?3
                          AND a.saved_end >= ?4)",
                    params![
                        reference.turn_id.as_slice(),
                        conversation_id.as_slice(),
                        attempt_id.as_slice(),
                        prefix_end,
                    ],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(schema::classify_sql_error)?,
        };
        if !valid {
            return Err(conflict(
                "prompt basis does not match retained conversation facts",
            ));
        }
    }
    Ok(())
}

fn valid_user_prefix(
    transaction: &Transaction<'_>,
    turn_id: [u8; 16],
    conversation_id: [u8; 16],
    prefix_end: u64,
) -> Result<bool, HistoryError> {
    use rusqlite::types::{Type, ValueRef};

    transaction
        .query_row(
            "SELECT user_text FROM turns WHERE id = ?1 AND conversation_id = ?2",
            params![turn_id.as_slice(), conversation_id.as_slice()],
            |row| {
                let bytes = match row.get_ref(0)? {
                    ValueRef::Text(bytes)
                        if !bytes.is_empty() && bytes.len() <= MAX_USER_TEXT_BYTES =>
                    {
                        bytes
                    }
                    value => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            value.data_type(),
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "invalid retained user text",
                            )
                            .into(),
                        ));
                    }
                };
                let text = std::str::from_utf8(bytes).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, error.into())
                })?;
                Ok(usize::try_from(prefix_end)
                    .ok()
                    .is_some_and(|end| end <= bytes.len() && text.is_char_boundary(end)))
            },
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(schema::classify_sql_error)
}

pub(super) struct RetryTarget {
    pub(super) turn_id: [u8; 16],
    pub(super) next_attempt_number: i64,
}

pub(super) fn read_retry_target(
    transaction: &Transaction<'_>,
    conversation_id: [u8; 16],
    prior_attempt_id: [u8; 16],
) -> Result<Option<RetryTarget>, HistoryError> {
    transaction
        .query_row(
            "SELECT t.id, a.attempt_number + 1
             FROM turns t JOIN attempts a ON a.id = t.selected_attempt_id
             WHERE t.conversation_id = ?1 AND t.ordinal = (
                    SELECT MAX(ordinal) FROM turns WHERE conversation_id = ?1)
               AND a.id = ?2 AND a.execution_outcome != 0
               AND a.save_outcome IN (1, 3)
               AND a.attempt_number < 9223372036854775807",
            params![conversation_id.as_slice(), prior_attempt_id.as_slice()],
            |row| {
                let turn_id = match row.get_ref(0)? {
                    rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
                        bytes.try_into().expect("checked turn identity")
                    }
                    value => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            value.data_type(),
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "invalid turn identity",
                            )
                            .into(),
                        ))
                    }
                };
                Ok(RetryTarget {
                    turn_id,
                    next_attempt_number: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(schema::classify_sql_error)
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, HistoryError> {
    if value.len() != 64 {
        return Err(invalid("invalid runtime artifact SHA-256"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex(pair[0])?;
            let low = hex(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex(byte: u8) -> Result<u8, HistoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid("invalid runtime artifact SHA-256")),
    }
}
