use super::{schema, HistoryError, HistoryErrorKind};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

const CONTENT_BATCH: i64 = 16;
const METADATA_BATCH: i64 = 128;

pub(super) const NEXT_ATTEMPT_SQL: &str =
    "SELECT a.id, t.id FROM turns t INDEXED BY turns_conversation
     JOIN attempts a INDEXED BY attempts_latest ON a.turn_id = t.id
     WHERE t.conversation_id = ?1
       AND NOT EXISTS (
           SELECT 1 FROM attempts child INDEXED BY attempts_prior
           WHERE child.prior_attempt_id = a.id
       )
     ORDER BY t.id, a.attempt_number DESC LIMIT 1";

pub(super) const DELETE_CHUNKS_SQL: &str =
    "DELETE FROM attempt_chunks WHERE (attempt_id, start_offset) IN (
         SELECT attempt_id, start_offset FROM attempt_chunks
         WHERE attempt_id = ?1 ORDER BY start_offset LIMIT ?2
     )";

pub(super) const DELETE_DRAFTS_SQL: &str = "DELETE FROM drafts WHERE id IN (
         SELECT id FROM drafts INDEXED BY drafts_conversation
         WHERE conversation_id = ?1 ORDER BY id LIMIT ?2
     )";

pub(super) const DELETE_TURNS_SQL: &str = "DELETE FROM turns WHERE id IN (
         SELECT id FROM turns INDEXED BY turns_conversation
         WHERE conversation_id = ?1 ORDER BY id LIMIT ?2
     )";

pub(super) const CLEAR_SELECTED_SQL: &str =
    "UPDATE turns SET selected_attempt_id = NULL WHERE id = ?1 AND selected_attempt_id = ?2";

pub(super) const DELETE_ATTEMPT_SQL: &str = "DELETE FROM attempts WHERE id = ?1
     AND NOT EXISTS (
         SELECT 1 FROM attempts child INDEXED BY attempts_prior
         WHERE child.prior_attempt_id = ?1
     )";

pub(super) const DELETE_EMPTY_TURN_SQL: &str = "DELETE FROM turns WHERE id = ?1
     AND NOT EXISTS (SELECT 1 FROM attempts WHERE turn_id = ?1)";

pub(super) fn delete_batch(connection: &mut Connection) -> Result<bool, HistoryError> {
    let Some(conversation_id) = next_deleted(connection)? else {
        return Ok(true);
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(schema::classify_sql_error)?;

    let attempt = transaction
        .query_row(NEXT_ATTEMPT_SQL, [conversation_id.as_slice()], |row| {
            Ok((fixed_id(row, 0)?, fixed_id(row, 1)?))
        })
        .optional()
        .map_err(schema::classify_sql_error)?;
    if let Some((attempt_id, turn_id)) = attempt {
        let chunks = transaction
            .execute(
                DELETE_CHUNKS_SQL,
                params![attempt_id.as_slice(), CONTENT_BATCH],
            )
            .map_err(schema::classify_sql_error)?;
        if chunks > 0 {
            transaction.commit().map_err(schema::classify_sql_error)?;
            return Ok(false);
        }

        transaction
            .execute(
                "DELETE FROM attempt_finalizations WHERE attempt_id = ?1",
                [attempt_id.as_slice()],
            )
            .map_err(schema::classify_sql_error)?;
        transaction
            .execute(
                "DELETE FROM attempt_statistics WHERE attempt_id = ?1",
                [attempt_id.as_slice()],
            )
            .map_err(schema::classify_sql_error)?;
        transaction
            .execute(
                CLEAR_SELECTED_SQL,
                params![turn_id.as_slice(), attempt_id.as_slice()],
            )
            .map_err(schema::classify_sql_error)?;
        let deleted = transaction
            .execute(DELETE_ATTEMPT_SQL, [attempt_id.as_slice()])
            .map_err(schema::classify_sql_error)?;
        if deleted != 1 {
            return Err(corrupt("selected purge attempt could not be removed"));
        }
        transaction
            .execute(DELETE_EMPTY_TURN_SQL, [turn_id.as_slice()])
            .map_err(schema::classify_sql_error)?;
        transaction.commit().map_err(schema::classify_sql_error)?;
        return Ok(false);
    }

    let drafts = transaction
        .execute(
            DELETE_DRAFTS_SQL,
            params![conversation_id.as_slice(), METADATA_BATCH],
        )
        .map_err(schema::classify_sql_error)?;
    if drafts > 0 {
        transaction.commit().map_err(schema::classify_sql_error)?;
        return Ok(false);
    }

    let turns = transaction
        .execute(
            DELETE_TURNS_SQL,
            params![conversation_id.as_slice(), METADATA_BATCH],
        )
        .map_err(schema::classify_sql_error)?;
    if turns > 0 {
        transaction.commit().map_err(schema::classify_sql_error)?;
        return Ok(false);
    }

    let deleted = transaction
        .execute(
            "DELETE FROM conversations WHERE id = ?1 AND deleted = 1
               AND NOT EXISTS (SELECT 1 FROM turns WHERE conversation_id = ?1)
               AND NOT EXISTS (SELECT 1 FROM drafts WHERE conversation_id = ?1)",
            [conversation_id.as_slice()],
        )
        .map_err(schema::classify_sql_error)?;
    if deleted != 1 {
        return Err(corrupt(
            "deleted conversation cannot make bounded purge progress",
        ));
    }
    transaction.commit().map_err(schema::classify_sql_error)?;
    Ok(next_deleted(connection)?.is_none())
}

fn next_deleted(connection: &Connection) -> Result<Option<[u8; 16]>, HistoryError> {
    connection
        .query_row(
            "SELECT id FROM conversations WHERE deleted = 1
             ORDER BY updated_ms DESC, id DESC LIMIT 1",
            [],
            |row| fixed_id(row, 0),
        )
        .optional()
        .map_err(schema::classify_sql_error)
}

fn fixed_id(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<[u8; 16]> {
    match row.get_ref(index)? {
        rusqlite::types::ValueRef::Blob(bytes) if bytes.len() == 16 => {
            Ok(bytes.try_into().expect("checked history identity"))
        }
        value => Err(rusqlite::Error::FromSqlConversionFailure(
            index,
            value.data_type(),
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid history identity").into(),
        )),
    }
}

fn corrupt(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::Corrupt, context)
}
