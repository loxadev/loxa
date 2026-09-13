use super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

mod range;
#[cfg(test)]
pub(super) use range::capture_attempt_prefix;
pub(super) use range::read_attempt_range;
#[cfg(test)]
pub(crate) use range::ContentRange;

pub(crate) const MAX_SUFFIX_BYTES: usize = 64 * 1024;
const MAX_ATTEMPT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionOutcome {
    Completed,
    #[cfg_attr(not(test), allow(dead_code))]
    Stopped,
    #[cfg_attr(not(test), allow(dead_code))]
    Failed,
}

impl ExecutionOutcome {
    fn stored(self) -> i64 {
        match self {
            Self::Completed => 1,
            Self::Stopped => 2,
            Self::Failed => 3,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SuffixInput {
    pub(crate) attempt_id: [u8; 16],
    pub(crate) owner_epoch: String,
    pub(crate) operation_generation: i64,
    pub(crate) expected_saved_end: u64,
    pub(crate) content: String,
}

impl SuffixInput {
    pub(crate) fn validate(&self, allow_empty: bool) -> Result<(), HistoryError> {
        if self.owner_epoch.is_empty()
            || self.owner_epoch.capacity() > 128
            || self.operation_generation <= 0
            || self.expected_saved_end > MAX_ATTEMPT_BYTES
            || self.content.capacity() > MAX_SUFFIX_BYTES
            || (!allow_empty && self.content.is_empty())
        {
            return Err(invalid("invalid bounded history suffix"));
        }
        self.expected_saved_end
            .checked_add(self.content.len() as u64)
            .filter(|end| *end <= MAX_ATTEMPT_BYTES)
            .ok_or_else(|| invalid("history suffix exceeds the attempt limit"))?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FinalizationInput {
    pub(crate) suffix: SuffixInput,
    pub(crate) execution_outcome: ExecutionOutcome,
    pub(crate) generated_end: u64,
    pub(crate) failure_code: Option<String>,
}

impl FinalizationInput {
    pub(crate) fn validate(&self) -> Result<(), HistoryError> {
        self.suffix.validate(true)?;
        validate_finalization(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SuffixCommit {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) current_saved_end: u64,
}

pub(super) fn append_suffix(
    connection: &mut Connection,
    input: &SuffixInput,
) -> Result<SuffixCommit, HistoryError> {
    input.validate(false)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(schema::classify_sql_error)?;
    let attempt = read_attempt(&transaction, input)?;
    if let Some(commit) = reconcile_chunk(&transaction, &attempt, input)? {
        transaction.commit().map_err(schema::classify_sql_error)?;
        schema::durable_checkpoint(connection)?;
        return Ok(commit);
    }
    if attempt.execution_outcome != 0 || attempt.save_outcome != 0 {
        return Err(conflict("attempt is not open for suffix persistence"));
    }
    if attempt.saved_end != input.expected_saved_end {
        return Err(conflict("history suffix is not contiguous"));
    }
    let end = suffix_end(input)?;
    transaction
        .execute(
            "INSERT INTO attempt_chunks (attempt_id, start_offset, end_offset, content)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                input.attempt_id.as_slice(),
                to_i64(input.expected_saved_end)?,
                to_i64(end)?,
                input.content,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    let changed = transaction
        .execute(
            "UPDATE attempts SET saved_end = ?1,
                    updated_ms = CASE WHEN updated_ms < 9223372036854775807
                        THEN updated_ms + 1 ELSE updated_ms END
             WHERE id = ?2 AND saved_end = ?3 AND execution_outcome = 0 AND save_outcome = 0",
            params![
                to_i64(end)?,
                input.attempt_id.as_slice(),
                to_i64(input.expected_saved_end)?,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed != 1 {
        return Err(conflict("attempt changed during suffix persistence"));
    }
    transaction.commit().map_err(|_| {
        HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history suffix commit outcome is unknown",
        )
    })?;
    Ok(SuffixCommit {
        start: input.expected_saved_end,
        end,
        current_saved_end: end,
    })
}

pub(super) fn finalize(
    connection: &mut Connection,
    input: &FinalizationInput,
) -> Result<SuffixCommit, HistoryError> {
    input.validate()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(schema::classify_sql_error)?;
    let attempt = read_attempt(&transaction, &input.suffix)?;
    if attempt.execution_outcome != 0 || attempt.save_outcome != 0 {
        let commit = reconcile_finalization(&transaction, &attempt, input)?;
        transaction.commit().map_err(schema::classify_sql_error)?;
        schema::durable_checkpoint(connection)?;
        return Ok(commit);
    }
    if attempt.saved_end != input.suffix.expected_saved_end {
        return Err(conflict("final history suffix is not contiguous"));
    }
    let end = suffix_end(&input.suffix)?;
    if !input.suffix.content.is_empty() {
        transaction
            .execute(
                "INSERT INTO attempt_chunks (attempt_id, start_offset, end_offset, content)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    input.suffix.attempt_id.as_slice(),
                    to_i64(input.suffix.expected_saved_end)?,
                    to_i64(end)?,
                    input.suffix.content,
                ],
            )
            .map_err(schema::classify_sql_error)?;
    }
    transaction
        .execute(
            "INSERT INTO attempt_finalizations (attempt_id, start_offset, end_offset)
             VALUES (?1, ?2, ?3)",
            params![
                input.suffix.attempt_id.as_slice(),
                to_i64(input.suffix.expected_saved_end)?,
                to_i64(end)?,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    let changed = transaction
        .execute(
            "UPDATE attempts
             SET execution_outcome = ?1, save_outcome = 1, saved_end = ?2,
                 generated_end = ?2, terminal_saved_end = ?2, failure_code = ?3,
                 updated_ms = CASE WHEN updated_ms < 9223372036854775807
                     THEN updated_ms + 1 ELSE updated_ms END
             WHERE id = ?4 AND saved_end = ?5 AND execution_outcome = 0 AND save_outcome = 0",
            params![
                input.execution_outcome.stored(),
                to_i64(end)?,
                input.failure_code,
                input.suffix.attempt_id.as_slice(),
                to_i64(input.suffix.expected_saved_end)?,
            ],
        )
        .map_err(schema::classify_sql_error)?;
    if changed != 1 {
        return Err(conflict("attempt changed during finalization"));
    }
    transaction.commit().map_err(|_| {
        HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history finalization commit outcome is unknown",
        )
    })?;
    Ok(SuffixCommit {
        start: input.suffix.expected_saved_end,
        end,
        current_saved_end: end,
    })
}

struct AttemptState {
    execution_outcome: i64,
    save_outcome: i64,
    saved_end: u64,
    generated_end: Option<u64>,
    terminal_saved_end: Option<u64>,
    failure_code: Option<String>,
}

fn read_attempt(
    connection: &Connection,
    input: &SuffixInput,
) -> Result<AttemptState, HistoryError> {
    let attempt = connection
        .query_row(
            "SELECT a.owner_epoch, a.operation_generation, a.execution_outcome,
                    a.save_outcome, a.saved_end, a.generated_end, a.terminal_saved_end,
                    a.failure_code
             FROM attempts a JOIN turns t ON t.id = a.turn_id
             JOIN conversations c ON c.id = t.conversation_id
             WHERE a.id = ?1 AND c.deleted = 0",
            [input.attempt_id.as_slice()],
            |row| {
                let owner_epoch = match row.get_ref(0)? {
                    rusqlite::types::ValueRef::Text(bytes)
                        if !bytes.is_empty() && bytes.len() <= 128 =>
                    {
                        std::str::from_utf8(bytes)
                            .map(str::to_owned)
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    0,
                                    rusqlite::types::Type::Text,
                                    error.into(),
                                )
                            })?
                    }
                    value => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            value.data_type(),
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "invalid attempt owner epoch",
                            )
                            .into(),
                        ))
                    }
                };
                Ok((
                    owner_epoch,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    bounded_failure_code(row, 7)?,
                ))
            },
        )
        .optional()
        .map_err(schema::classify_sql_error)?
        .ok_or_else(|| not_found("attempt was not found"))?;
    if attempt.0 != input.owner_epoch || attempt.1 != input.operation_generation {
        return Err(conflict("attempt persistence identity changed"));
    }
    Ok(AttemptState {
        execution_outcome: attempt.2,
        save_outcome: attempt.3,
        saved_end: from_i64(attempt.4)?,
        generated_end: attempt.5.map(from_i64).transpose()?,
        terminal_saved_end: attempt.6.map(from_i64).transpose()?,
        failure_code: attempt.7,
    })
}

fn reconcile_chunk(
    connection: &Connection,
    attempt: &AttemptState,
    input: &SuffixInput,
) -> Result<Option<SuffixCommit>, HistoryError> {
    let mut statement = connection
        .prepare(
            "SELECT end_offset, content FROM attempt_chunks
             WHERE attempt_id = ?1 AND start_offset = ?2",
        )
        .map_err(schema::classify_sql_error)?;
    let mut rows = statement
        .query(params![
            input.attempt_id.as_slice(),
            to_i64(input.expected_saved_end)?,
        ])
        .map_err(schema::classify_sql_error)?;
    let Some(row) = rows.next().map_err(schema::classify_sql_error)? else {
        return Ok(None);
    };
    let end = from_i64(row.get(0).map_err(schema::classify_sql_error)?)?;
    let content = match row.get_ref(1).map_err(schema::classify_sql_error)? {
        rusqlite::types::ValueRef::Text(bytes)
            if !bytes.is_empty() && bytes.len() <= MAX_SUFFIX_BYTES =>
        {
            bytes
        }
        _ => return Err(corrupt("committed history suffix is invalid")),
    };
    if content != input.content.as_bytes() || end != suffix_end(input)? {
        return Err(conflict("history suffix replay changed committed bytes"));
    }
    Ok(Some(SuffixCommit {
        start: input.expected_saved_end,
        end,
        current_saved_end: attempt.saved_end,
    }))
}

fn reconcile_finalization(
    connection: &Connection,
    attempt: &AttemptState,
    input: &FinalizationInput,
) -> Result<SuffixCommit, HistoryError> {
    let expected_end = suffix_end(&input.suffix)?;
    let final_range = connection
        .query_row(
            "SELECT start_offset, end_offset FROM attempt_finalizations WHERE attempt_id = ?1",
            [input.suffix.attempt_id.as_slice()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(schema::classify_sql_error)?
        .ok_or_else(|| corrupt("terminal history identity is missing"))?;
    if from_i64(final_range.0)? != input.suffix.expected_saved_end
        || from_i64(final_range.1)? != expected_end
        || attempt.execution_outcome != input.execution_outcome.stored()
        || attempt.save_outcome != 1
        || attempt.generated_end != Some(input.generated_end)
        || attempt.terminal_saved_end != Some(input.generated_end)
        || attempt.failure_code != input.failure_code
        || attempt.saved_end != input.generated_end
        || expected_end != input.generated_end
    {
        return Err(conflict(
            "terminal history replay changed committed outcome",
        ));
    }
    if !input.suffix.content.is_empty() {
        reconcile_chunk(connection, attempt, &input.suffix)?
            .ok_or_else(|| corrupt("terminal history suffix is missing"))?;
    }
    Ok(SuffixCommit {
        start: input.suffix.expected_saved_end,
        end: expected_end,
        current_saved_end: attempt.saved_end,
    })
}

fn bounded_failure_code(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<String>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Text(bytes) if !bytes.is_empty() && bytes.len() <= 64 => {
            std::str::from_utf8(bytes)
                .map(|value| Some(value.to_owned()))
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        index,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })
        }
        value => Err(rusqlite::Error::FromSqlConversionFailure(
            index,
            value.data_type(),
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid failure code").into(),
        )),
    }
}

fn validate_finalization(input: &FinalizationInput) -> Result<(), HistoryError> {
    let end = suffix_end(&input.suffix)?;
    if input.generated_end != end {
        return Err(invalid("generated end does not match the final suffix"));
    }
    match (&input.execution_outcome, &input.failure_code) {
        (ExecutionOutcome::Completed, None)
        | (ExecutionOutcome::Stopped, None)
        | (ExecutionOutcome::Stopped, Some(_))
        | (ExecutionOutcome::Failed, Some(_)) => {}
        (ExecutionOutcome::Completed, Some(_)) | (ExecutionOutcome::Failed, None) => {
            return Err(invalid("invalid terminal failure code"))
        }
    }
    if input.failure_code.as_ref().is_some_and(|code| {
        code.is_empty() || code.len() > 64 || code.capacity() > 64 || !code.is_ascii()
    }) {
        return Err(invalid("invalid terminal failure code"));
    }
    Ok(())
}

fn suffix_end(input: &SuffixInput) -> Result<u64, HistoryError> {
    input
        .expected_saved_end
        .checked_add(input.content.len() as u64)
        .filter(|end| *end <= MAX_ATTEMPT_BYTES)
        .ok_or_else(|| invalid("history suffix exceeds the attempt limit"))
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

fn conflict(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Conflict, context)
}

fn not_found(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::NotFound, context)
}

fn corrupt(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Corrupt, context)
}
