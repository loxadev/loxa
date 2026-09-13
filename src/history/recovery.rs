use super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection, TransactionBehavior};

pub(super) fn recover_interrupted(
    connection: &mut Connection,
    current_owner_epoch: &str,
) -> Result<bool, HistoryError> {
    if current_owner_epoch.is_empty() || current_owner_epoch.len() > 128 {
        return Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "invalid recovery owner epoch",
        ));
    }
    // Take the global bound from the partial unresolved index before applying
    // any epoch or parent filters. Otherwise corrupt/deleted/current-epoch rows
    // could make startup scan an unbounded prefix or hide a second owner.
    let mut statement = connection
        .prepare(
            "SELECT id FROM attempts
             WHERE execution_outcome = 0 OR save_outcome IN (0, 2)
             ORDER BY id LIMIT 2",
        )
        .map_err(schema::classify_sql_error)?;
    let candidates = statement
        .query_map([], |row| {
            let id = match row.get_ref(0)? {
                rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
                    bytes.try_into().expect("checked attempt identity")
                }
                value => {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        value.data_type(),
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid unresolved attempt identity",
                        )
                        .into(),
                    ))
                }
            };
            Ok(id)
        })
        .map_err(schema::classify_sql_error)?
        .collect::<Result<Vec<[u8; 16]>, _>>()
        .map_err(schema::classify_sql_error)?;
    drop(statement);
    match candidates.as_slice() {
        [] => Ok(false),
        [attempt_id] => {
            let facts = connection
                .query_row(
                    "SELECT a.owner_epoch, c.deleted FROM attempts a
                     LEFT JOIN turns t ON t.id = a.turn_id
                     LEFT JOIN conversations c ON c.id = t.conversation_id
                     WHERE a.id = ?1",
                    [attempt_id.as_slice()],
                    |row| {
                        let owner = match row.get_ref(0)? {
                            rusqlite::types::ValueRef::Text(bytes)
                                if !bytes.is_empty() && bytes.len() <= 128 =>
                            {
                                std::str::from_utf8(bytes).map_err(|_| {
                                    rusqlite::Error::InvalidColumnType(
                                        0,
                                        "owner_epoch".into(),
                                        rusqlite::types::Type::Text,
                                    )
                                })?
                            }
                            value => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    0,
                                    "owner_epoch".into(),
                                    value.data_type(),
                                ))
                            }
                        };
                        let deleted = match row.get_ref(1)? {
                            rusqlite::types::ValueRef::Integer(value) if matches!(value, 0 | 1) => {
                                value
                            }
                            value => {
                                return Err(rusqlite::Error::InvalidColumnType(
                                    1,
                                    "deleted".into(),
                                    value.data_type(),
                                ))
                            }
                        };
                        Ok((owner == current_owner_epoch, deleted))
                    },
                )
                .map_err(|_| {
                    HistoryError::new(
                        HistoryErrorKind::Corrupt,
                        "unresolved attempt has invalid owner or parent facts",
                    )
                })?;
            if facts.0 || facts.1 != 0 {
                return Err(HistoryError::new(
                    HistoryErrorKind::Corrupt,
                    "unresolved attempt has impossible startup ownership",
                ));
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(schema::classify_sql_error)?;
            let changed = transaction
                .execute(
                    "UPDATE attempts
                     SET execution_outcome = CASE
                             WHEN execution_outcome = 0 THEN 4 ELSE execution_outcome END,
                         save_outcome = CASE
                             WHEN save_outcome IN (0, 2) THEN 3 ELSE save_outcome END,
                         updated_ms = CASE WHEN updated_ms < 9223372036854775807
                             THEN updated_ms + 1 ELSE updated_ms END
                     WHERE id = ?1 AND owner_epoch != ?2
                       AND (execution_outcome = 0 OR save_outcome IN (0, 2))",
                    params![attempt_id.as_slice(), current_owner_epoch],
                )
                .map_err(schema::classify_sql_error)?;
            if changed != 1 {
                return Err(HistoryError::new(
                    HistoryErrorKind::Corrupt,
                    "unresolved attempt changed during recovery",
                ));
            }
            transaction.commit().map_err(schema::classify_sql_error)?;
            Ok(true)
        }
        _ => Err(HistoryError::new(
            HistoryErrorKind::Corrupt,
            "multiple unresolved history generations require recovery",
        )),
    }
}
