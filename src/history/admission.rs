use super::drafts::{consume_submitted, SubmittedDraft};
use super::identity::{next_updated_ms, random_id};
use super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

mod types;
mod validation;

#[cfg(test)]
pub(crate) use types::DraftSubmission;
pub(crate) use types::{
    AdmissionKind, CommittedAdmission, PreparedAdmission, PromptBasis, PromptReference,
};
use validation::{
    read_conversation, read_retry_target, require_previous_turn_resolved, validate_conversation,
    validate_prepared, validate_prompt_basis,
};

pub(super) fn lookup_submission(
    connection: &Connection,
    submission_id: [u8; 16],
    submission_hash: [u8; 32],
) -> Result<Option<CommittedAdmission>, HistoryError> {
    lookup_in(connection, submission_id, submission_hash)
}

pub(super) fn reconcile_submission(
    connection: &Connection,
    submission_id: [u8; 16],
    submission_hash: [u8; 32],
) -> Result<Option<CommittedAdmission>, HistoryError> {
    if !connection.is_autocommit() {
        return Err(HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history admission transaction is still unresolved",
        ));
    }
    let committed = lookup_in(connection, submission_id, submission_hash)?;
    let (busy, _log, _checkpointed): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(FULL)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(schema::classify_sql_error)?;
    if busy != 0 || !connection.is_autocommit() {
        return Err(HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history admission durability is not yet reconciled",
        ));
    }
    Ok(committed)
}

pub(super) fn admit_send(
    connection: &mut Connection,
    prepared: &PreparedAdmission,
) -> Result<CommittedAdmission, HistoryError> {
    if !matches!(prepared.kind, AdmissionKind::Send { .. }) {
        return Err(invalid("send admission has the wrong operation kind"));
    }
    admit(connection, prepared, None)
}

#[cfg(test)]
pub(super) fn admit_send_with_commit_barrier(
    connection: &mut Connection,
    prepared: &PreparedAdmission,
    barrier: Option<&std::sync::Barrier>,
) -> Result<CommittedAdmission, HistoryError> {
    if !matches!(prepared.kind, AdmissionKind::Send { .. }) {
        return Err(invalid("send admission has the wrong operation kind"));
    }
    admit(connection, prepared, barrier)
}

pub(super) fn admit_retry(
    connection: &mut Connection,
    prepared: &PreparedAdmission,
) -> Result<CommittedAdmission, HistoryError> {
    if !matches!(prepared.kind, AdmissionKind::Retry { .. }) {
        return Err(invalid("retry admission has the wrong operation kind"));
    }
    admit(connection, prepared, None)
}

pub(super) fn stop_before_execution(
    connection: &Connection,
    committed: &CommittedAdmission,
) -> Result<(), HistoryError> {
    let changed = connection
        .execute(
            "UPDATE attempts
             SET execution_outcome = 2, save_outcome = 1, generated_end = 0,
                 terminal_saved_end = 0, updated_ms = CASE
                     WHEN updated_ms < 9223372036854775807 THEN updated_ms + 1 ELSE updated_ms END
             WHERE id = ?1 AND submission_id = ?2 AND operation_generation = ?3
               AND execution_outcome = 0 AND save_outcome = 0 AND saved_end = 0",
            params![
                committed.attempt_id.as_slice(),
                committed.submission_id.as_slice(),
                committed.operation_generation,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed == 1 {
        return Ok(());
    }
    let already_stopped: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts
             WHERE id = ?1 AND submission_id = ?2 AND operation_generation = ?3
               AND execution_outcome = 2 AND save_outcome = 1 AND saved_end = 0
               AND generated_end = 0 AND terminal_saved_end = 0)",
            params![
                committed.attempt_id.as_slice(),
                committed.submission_id.as_slice(),
                committed.operation_generation,
            ],
            |row| row.get(0),
        )
        .map_err(schema::classify_sql_error)?;
    if already_stopped {
        Ok(())
    } else {
        Err(conflict(
            "admitted attempt changed before cancellation was saved",
        ))
    }
}

fn admit(
    connection: &mut Connection,
    prepared: &PreparedAdmission,
    commit_barrier: Option<&std::sync::Barrier>,
) -> Result<CommittedAdmission, HistoryError> {
    #[cfg(not(test))]
    let _ = commit_barrier;
    validate_prepared(prepared)?;
    if let Some(committed) =
        lookup_submission(connection, prepared.submission_id, prepared.submission_hash)?
    {
        return Ok(committed);
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(schema::classify_sql_error)?;
    if let Some(committed) = lookup_in(
        &transaction,
        prepared.submission_id,
        prepared.submission_hash,
    )? {
        transaction.commit().map_err(schema::classify_sql_error)?;
        return Ok(committed);
    }
    let conversation = read_conversation(&transaction, prepared.conversation_id)?
        .ok_or_else(|| not_found("conversation was not found"))?;
    validate_conversation(&prepared, &conversation)?;
    let excluded_attempt = match &prepared.kind {
        AdmissionKind::Retry { prior_attempt_id } => Some(*prior_attempt_id),
        AdmissionKind::Send { .. } => None,
    };
    validate_prompt_basis(
        &transaction,
        prepared.conversation_id,
        &prepared.prompt_basis,
        excluded_attempt,
    )?;
    let post_revision = conversation.revision.checked_add(1).ok_or_else(|| {
        HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "conversation revision overflow",
        )
    })?;
    let now = next_updated_ms(conversation.updated_ms)?;
    let attempt_id = random_id()?;

    let (turn_id, attempt_number, prior_attempt_id) = match &prepared.kind {
        AdmissionKind::Send { user_text, draft } => {
            require_previous_turn_resolved(&transaction, prepared.conversation_id)?;
            let turn_id = random_id()?;
            let ordinal: i64 = transaction
                .query_row(
                    "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM turns WHERE conversation_id = ?1",
                    [prepared.conversation_id.as_slice()],
                    |row| row.get(0),
                )
                .map_err(schema::classify_sql_error)?;
            transaction
                .execute(
                    "INSERT INTO turns (id, conversation_id, ordinal, user_text, selected_attempt_id)
                     VALUES (?1, ?2, ?3, ?4, NULL)",
                    params![
                        turn_id.as_slice(),
                        prepared.conversation_id.as_slice(),
                        ordinal,
                        user_text,
                    ],
                )
                .map_err(schema::classify_sql_error)?;
            if let Some(draft) = draft {
                consume_submitted(
                    &transaction,
                    &SubmittedDraft {
                        id: draft.id,
                        desktop_client_id: draft.desktop_client_id,
                        revision: draft.revision,
                        text: user_text,
                    },
                    prepared.conversation_id,
                )?;
            }
            (turn_id, 1, None)
        }
        AdmissionKind::Retry { prior_attempt_id } => {
            let eligible =
                read_retry_target(&transaction, prepared.conversation_id, *prior_attempt_id)?
                    .ok_or_else(|| {
                        conflict("retry target is not the latest selected terminal attempt")
                    })?;
            (
                eligible.turn_id,
                eligible.next_attempt_number,
                Some(*prior_attempt_id),
            )
        }
    };
    transaction
        .execute(
            "INSERT INTO attempts (
                id, turn_id, attempt_number, submission_id, submission_hash,
                admitted_conversation_revision, admitted_profile_revision, prior_attempt_id,
                owner_epoch, operation_generation, model_id, applied_engine_build,
                applied_engine_version, runtime_fingerprint, effective_context,
                system_instruction, max_output_tokens, prompt_basis, execution_outcome,
                save_outcome, saved_end, generated_end, terminal_saved_end, failure_code,
                created_ms, updated_ms
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, 0, 0, 0, NULL, NULL, NULL, ?19, ?19
             )",
            params![
                attempt_id.as_slice(),
                turn_id.as_slice(),
                attempt_number,
                prepared.submission_id.as_slice(),
                prepared.submission_hash.as_slice(),
                post_revision,
                prepared.expected_profile_revision,
                prior_attempt_id.as_ref().map(<[u8; 16]>::as_slice),
                prepared.owner_epoch,
                prepared.operation_generation,
                prepared.runtime_fingerprint.model_id(),
                prepared.runtime_identity.build(),
                prepared.runtime_identity.version_line(),
                &prepared.runtime_fingerprint_json,
                i64::from(prepared.runtime_fingerprint.effective_context()),
                prepared.system_instruction,
                prepared.max_output_tokens,
                &prepared.prompt_basis_json,
                now,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    let selected = transaction
        .execute(
            "UPDATE turns SET selected_attempt_id = ?1
             WHERE id = ?2 AND (selected_attempt_id IS NULL OR selected_attempt_id = ?3)",
            params![
                attempt_id.as_slice(),
                turn_id.as_slice(),
                prior_attempt_id.as_ref().map(<[u8; 16]>::as_slice),
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if selected != 1 {
        return Err(conflict("selected attempt changed during admission"));
    }
    let title = match &prepared.kind {
        AdmissionKind::Send { user_text, .. } if conversation.title == "New chat" => {
            derived_title(user_text)
        }
        _ => conversation.title,
    };
    let changed = transaction
        .execute(
            "UPDATE conversations
             SET title = ?1, revision = ?2, updated_ms = ?3
             WHERE id = ?4 AND deleted = 0 AND revision = ?5 AND profile_revision = ?6",
            params![
                title,
                post_revision,
                now,
                prepared.conversation_id.as_slice(),
                prepared.expected_conversation_revision,
                prepared.expected_profile_revision,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed != 1 {
        return Err(conflict("conversation changed during admission"));
    }
    #[cfg(test)]
    if let Some(barrier) = commit_barrier {
        barrier.wait();
        barrier.wait();
    }
    transaction.commit().map_err(|_| {
        HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history admission commit outcome is unknown",
        )
    })?;
    Ok(CommittedAdmission {
        conversation_id: prepared.conversation_id,
        turn_id,
        attempt_id,
        submission_id: prepared.submission_id,
        pre_conversation_revision: prepared.expected_conversation_revision,
        post_conversation_revision: post_revision,
        profile_revision: prepared.expected_profile_revision,
        operation_generation: prepared.operation_generation,
    })
}

fn lookup_in(
    connection: &Connection,
    submission_id: [u8; 16],
    submission_hash: [u8; 32],
) -> Result<Option<CommittedAdmission>, HistoryError> {
    let row = connection
        .query_row(
            "SELECT a.submission_hash, t.conversation_id, t.id, a.id,
                    a.admitted_conversation_revision, a.admitted_profile_revision,
                    a.operation_generation
             FROM attempts a JOIN turns t ON t.id = a.turn_id
             JOIN conversations c ON c.id = t.conversation_id
             WHERE a.submission_id = ?1 AND c.deleted = 0",
            [submission_id.as_slice()],
            |row| {
                Ok((
                    decode_blob_column::<32>(row, 0, "invalid submission hash")?,
                    decode_blob_column::<16>(row, 1, "invalid conversation identity")?,
                    decode_blob_column::<16>(row, 2, "invalid turn identity")?,
                    decode_blob_column::<16>(row, 3, "invalid attempt identity")?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()
        .map_err(schema::classify_sql_error)?;
    let Some((hash, conversation_id, turn_id, attempt_id, post, profile, generation)) = row else {
        return Ok(None);
    };
    if hash != submission_hash {
        return Err(conflict(
            "submission identity was reused with a different payload",
        ));
    }
    let pre = post.checked_sub(1).ok_or_else(|| {
        HistoryError::new(
            HistoryErrorKind::Corrupt,
            "admission revision record is invalid",
        )
    })?;
    Ok(Some(CommittedAdmission {
        conversation_id,
        turn_id,
        attempt_id,
        submission_id,
        pre_conversation_revision: pre,
        post_conversation_revision: post,
        profile_revision: profile,
        operation_generation: generation,
    }))
}

fn decode_blob_column<const N: usize>(
    row: &rusqlite::Row<'_>,
    index: usize,
    context: &'static str,
) -> rusqlite::Result<[u8; N]> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == N => {
            Ok(bytes.try_into().expect("checked fixed-width blob"))
        }
        value => Err(rusqlite::Error::FromSqlConversionFailure(
            index,
            value.data_type(),
            std::io::Error::new(std::io::ErrorKind::InvalidData, context).into(),
        )),
    }
}

fn derived_title(text: &str) -> String {
    let candidate = text.lines().next().unwrap_or(text).trim();
    if candidate.is_empty() {
        return "New chat".into();
    }
    let mut end = candidate.len().min(80);
    while !candidate.is_char_boundary(end) {
        end -= 1;
    }
    candidate[..end].to_owned()
}

fn invalid(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::InvalidInput, context)
}

fn conflict(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Conflict, context)
}

fn not_found(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::NotFound, context)
}

fn limit(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::LimitExceeded, context)
}
