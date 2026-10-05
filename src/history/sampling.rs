use loxa_ipc::{EffectiveSamplingSettings, GenerationSettings, SamplingValue};
use rusqlite::types::{Type, ValueRef};
use rusqlite::{params, Row, Transaction};

pub(super) fn write_conversation(
    transaction: &Transaction<'_>,
    conversation_id: [u8; 16],
    generation: &GenerationSettings,
) -> rusqlite::Result<()> {
    match (generation.temperature, generation.top_p) {
        (None, None) => {
            transaction.execute(
                "DELETE FROM conversation_sampling WHERE conversation_id = ?1",
                [conversation_id.as_slice()],
            )?;
        }
        (temperature, top_p) => {
            transaction.execute(
                "INSERT INTO conversation_sampling (conversation_id, temperature, top_p)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (conversation_id) DO UPDATE
                 SET temperature = excluded.temperature, top_p = excluded.top_p",
                params![
                    conversation_id.as_slice(),
                    temperature.map(SamplingValue::get),
                    top_p.map(SamplingValue::get),
                ],
            )?;
        }
    }
    Ok(())
}

pub(super) fn write_attempt(
    transaction: &Transaction<'_>,
    attempt_id: [u8; 16],
    sampling: EffectiveSamplingSettings,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO attempt_sampling (attempt_id, temperature, top_p) VALUES (?1, ?2, ?3)",
        params![
            attempt_id.as_slice(),
            sampling.temperature.get(),
            sampling.top_p.get(),
        ],
    )?;
    Ok(())
}

pub(super) fn read_conversation(
    row: &Row<'_>,
    start: usize,
    expected_id: [u8; 16],
) -> rusqlite::Result<(Option<SamplingValue>, Option<SamplingValue>)> {
    match row.get_ref(start)? {
        ValueRef::Null => {
            if !matches!(row.get_ref(start + 1)?, ValueRef::Null)
                || !matches!(row.get_ref(start + 2)?, ValueRef::Null)
            {
                return Err(invalid(start));
            }
            Ok((None, None))
        }
        ValueRef::Blob(id) if id == expected_id => {
            let temperature = optional_value(row, start + 1, SamplingKind::Temperature)?;
            let top_p = optional_value(row, start + 2, SamplingKind::TopP)?;
            if temperature.is_none() && top_p.is_none() {
                return Err(invalid(start));
            }
            Ok((temperature, top_p))
        }
        value => Err(invalid_type(start, value.data_type())),
    }
}

pub(super) fn read_attempt(
    row: &Row<'_>,
    start: usize,
    expected_id: [u8; 16],
) -> rusqlite::Result<Option<EffectiveSamplingSettings>> {
    match row.get_ref(start)? {
        ValueRef::Null => {
            if !matches!(row.get_ref(start + 1)?, ValueRef::Null)
                || !matches!(row.get_ref(start + 2)?, ValueRef::Null)
            {
                return Err(invalid(start));
            }
            Ok(None)
        }
        ValueRef::Blob(id) if id == expected_id => Ok(Some(EffectiveSamplingSettings {
            temperature: required_value(row, start + 1, SamplingKind::Temperature)?,
            top_p: required_value(row, start + 2, SamplingKind::TopP)?,
        })),
        value => Err(invalid_type(start, value.data_type())),
    }
}

enum SamplingKind {
    Temperature,
    TopP,
}

fn optional_value(
    row: &Row<'_>,
    index: usize,
    kind: SamplingKind,
) -> rusqlite::Result<Option<SamplingValue>> {
    match row.get_ref(index)? {
        ValueRef::Null => Ok(None),
        ValueRef::Real(value) => checked(value, kind).map(Some).ok_or_else(|| invalid(index)),
        value => Err(invalid_type(index, value.data_type())),
    }
}

fn required_value(
    row: &Row<'_>,
    index: usize,
    kind: SamplingKind,
) -> rusqlite::Result<SamplingValue> {
    optional_value(row, index, kind)?.ok_or_else(|| invalid(index))
}

fn checked(value: f64, kind: SamplingKind) -> Option<SamplingValue> {
    let value = SamplingValue::new(value)?;
    match kind {
        SamplingKind::Temperature if value.get() >= 0.0 => Some(value),
        SamplingKind::TopP if (0.0..=1.0).contains(&value.get()) => Some(value),
        _ => None,
    }
}

fn invalid(index: usize) -> rusqlite::Error {
    invalid_type(index, Type::Null)
}

fn invalid_type(index: usize, kind: Type) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnType(index, "sampling".into(), kind)
}
