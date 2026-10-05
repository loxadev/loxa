use super::content::ExecutionOutcome;
use super::{HistoryError, HistoryErrorKind};
use loxa_ipc::{AttemptExecution, AttemptSave, AttemptStopReason, EngineDecodeRate};
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AttemptStatistics {
    pub(crate) qualified_input_tokens: Option<u32>,
    pub(crate) qualified_output_tokens: Option<u32>,
    pub(crate) service_first_output_latency_ms: Option<u64>,
    pub(crate) qualified_engine_decode_tokens_per_second: Option<f64>,
    pub(crate) service_total_duration_ms: u64,
    pub(crate) stop_reason: AttemptStopReason,
}

impl AttemptStatistics {
    pub(crate) fn validate_for(&self, outcome: ExecutionOutcome) -> Result<(), HistoryError> {
        if self.service_total_duration_ms > i64::MAX as u64
            || self
                .service_first_output_latency_ms
                .is_some_and(|value| value > self.service_total_duration_ms)
            || self
                .qualified_engine_decode_tokens_per_second
                .is_some_and(|rate| {
                    !rate.is_finite()
                        || rate <= 0.0
                        || rate >= 1.0e308
                        || self
                            .qualified_output_tokens
                            .is_none_or(|tokens| tokens == 0)
                })
            || !matches!(
                (outcome, self.stop_reason),
                (
                    ExecutionOutcome::Completed,
                    AttemptStopReason::Completed | AttemptStopReason::OutputLimit
                ) | (ExecutionOutcome::Stopped, AttemptStopReason::UserStop)
                    | (ExecutionOutcome::Failed, AttemptStopReason::Failure)
            )
        {
            return Err(HistoryError::new(
                HistoryErrorKind::InvalidInput,
                "invalid terminal attempt statistics",
            ));
        }
        Ok(())
    }

    pub(crate) fn to_wire(
        &self,
        execution: AttemptExecution,
        save: AttemptSave,
        index: usize,
    ) -> rusqlite::Result<loxa_ipc::AttemptStatistics> {
        if save != AttemptSave::Saved
            || !matches!(
                (execution, self.stop_reason),
                (
                    AttemptExecution::Completed,
                    AttemptStopReason::Completed | AttemptStopReason::OutputLimit
                ) | (AttemptExecution::Stopped, AttemptStopReason::UserStop)
                    | (AttemptExecution::Failed, AttemptStopReason::Failure)
            )
        {
            return Err(invalid_row(index));
        }
        Ok(loxa_ipc::AttemptStatistics {
            qualified_input_tokens: self.qualified_input_tokens,
            qualified_output_tokens: self.qualified_output_tokens,
            service_first_output_latency_ms: self
                .service_first_output_latency_ms
                .map(|value| value.to_string()),
            qualified_engine_decode_tokens_per_second: self
                .qualified_engine_decode_tokens_per_second
                .map(|rate| EngineDecodeRate::new(rate).expect("validated engine decode rate")),
            service_total_duration_ms: self.service_total_duration_ms.to_string(),
            stop_reason: self.stop_reason,
        })
    }
}

pub(super) fn insert(
    connection: &Connection,
    attempt_id: [u8; 16],
    statistics: &AttemptStatistics,
) -> Result<(), HistoryError> {
    connection
        .execute(
            "INSERT INTO attempt_statistics (
                attempt_id, qualified_input_tokens, qualified_output_tokens,
                service_first_output_latency_ms,
                qualified_engine_decode_tokens_per_second, service_total_duration_ms,
                stop_reason
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                attempt_id.as_slice(),
                statistics.qualified_input_tokens.map(i64::from),
                statistics.qualified_output_tokens.map(i64::from),
                statistics
                    .service_first_output_latency_ms
                    .map(to_i64)
                    .transpose()?,
                statistics.qualified_engine_decode_tokens_per_second,
                to_i64(statistics.service_total_duration_ms)?,
                stop_reason_stored(statistics.stop_reason),
            ],
        )
        .map_err(super::schema::classify_sql_error)?;
    Ok(())
}

pub(super) fn read(
    row: &rusqlite::Row<'_>,
    index: usize,
    expected_attempt_id: [u8; 16],
) -> rusqlite::Result<Option<AttemptStatistics>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => {
            for offset in 1..=6 {
                if !matches!(
                    row.get_ref(index + offset)?,
                    rusqlite::types::ValueRef::Null
                ) {
                    return Err(invalid_row(index + offset));
                }
            }
            Ok(None)
        }
        rusqlite::types::ValueRef::Blob(bytes) if bytes == expected_attempt_id.as_slice() => {
            let input = optional_token_count(row, index + 1)?;
            let output = optional_token_count(row, index + 2)?;
            let first_output = optional_nonnegative(row, index + 3)?;
            let rate = optional_finite_rate(row, index + 4)?;
            let total = nonnegative_integer(row, index + 5)?;
            if first_output.is_some_and(|value| value > total)
                || rate.is_some_and(|_| output.is_none_or(|tokens| tokens == 0))
            {
                return Err(invalid_row(index));
            }
            Ok(Some(AttemptStatistics {
                qualified_input_tokens: input,
                qualified_output_tokens: output,
                service_first_output_latency_ms: first_output,
                qualified_engine_decode_tokens_per_second: rate,
                service_total_duration_ms: total,
                stop_reason: stop_reason(row.get(index + 6)?, index + 6)?,
            }))
        }
        _ => Err(invalid_row(index)),
    }
}

pub(super) fn read_for_attempt(
    connection: &Connection,
    attempt_id: [u8; 16],
) -> Result<Option<AttemptStatistics>, HistoryError> {
    connection
        .query_row(
            "SELECT attempt_id, qualified_input_tokens, qualified_output_tokens,
                    service_first_output_latency_ms,
                    qualified_engine_decode_tokens_per_second,
                    service_total_duration_ms, stop_reason
             FROM attempt_statistics WHERE attempt_id = ?1",
            [attempt_id.as_slice()],
            |row| read(row, 0, attempt_id),
        )
        .optional()
        .map(Option::flatten)
        .map_err(super::schema::classify_sql_error)
}

fn optional_token_count(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u32>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Integer(value) if (0..=i64::from(u32::MAX)).contains(&value) => {
            Ok(Some(value as u32))
        }
        _ => Err(invalid_row(index)),
    }
}

fn optional_nonnegative(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        _ => Err(invalid_row(index)),
    }
}

fn nonnegative_integer(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Integer(value) if value >= 0 => Ok(value as u64),
        _ => Err(invalid_row(index)),
    }
}

fn optional_finite_rate(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<f64>> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Real(value)
            if value.is_finite() && value > 0.0 && value < 1.0e308 =>
        {
            Ok(Some(value))
        }
        _ => Err(invalid_row(index)),
    }
}

fn stop_reason(value: i64, index: usize) -> rusqlite::Result<AttemptStopReason> {
    match value {
        1 => Ok(AttemptStopReason::Completed),
        2 => Ok(AttemptStopReason::OutputLimit),
        3 => Ok(AttemptStopReason::UserStop),
        4 => Ok(AttemptStopReason::Failure),
        _ => Err(invalid_row(index)),
    }
}

fn stop_reason_stored(reason: AttemptStopReason) -> i64 {
    match reason {
        AttemptStopReason::Completed => 1,
        AttemptStopReason::OutputLimit => 2,
        AttemptStopReason::UserStop => 3,
        AttemptStopReason::Failure => 4,
    }
}

fn to_i64(value: u64) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_| {
        HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "attempt statistic exceeds the storage range",
        )
    })
}

fn invalid_row(index: usize) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnType(
        index,
        "attempt_statistics".into(),
        rusqlite::types::Type::Null,
    )
}
