use super::{HistoryError, HistoryErrorKind};
use rusqlite::limits::Limit;
use rusqlite::{Connection, ErrorCode, OpenFlags};
use std::fs;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod definitions;
use definitions::{
    CREATE_ATTEMPTS_LATEST_V2, CREATE_ATTEMPTS_PRIOR_V3, CREATE_ATTEMPTS_RECOVERY_V3,
    CREATE_ATTEMPTS_UNRESOLVED_V2, CREATE_ATTEMPTS_V2, CREATE_ATTEMPT_CHUNKS_V3,
    CREATE_ATTEMPT_FINALIZATIONS_V3, CREATE_CONVERSATIONS_V1, CREATE_DRAFTS_CLIENT_CONVERSATION_V2,
    CREATE_DRAFTS_CLIENT_UNBOUND_V2, CREATE_DRAFTS_CONVERSATION_V3, CREATE_DRAFTS_V2,
    CREATE_RECENCY_INDEX_V1, CREATE_TURNS_CONVERSATION_V3, CREATE_TURNS_SELECTED_ATTEMPT_V3,
    CREATE_TURNS_V2,
};

const APPLICATION_ID: i64 = 0x4c4f5841;
const SCHEMA_VERSION: i64 = 3;
const DATABASE_FILENAME: &str = "app.sqlite";
const PRIVATE_MODE: u32 = 0o600;

#[derive(Debug)]
pub(super) struct StoreInfo {
    pub(super) schema_version: u32,
    pub(super) sqlite_version: String,
    pub(super) sqlite_source_id: String,
}

#[derive(Debug)]
pub(super) struct StoreOpenError {
    error: HistoryError,
    pub(super) connection: Option<Box<Connection>>,
}

impl StoreOpenError {
    fn before_open(error: HistoryError) -> Self {
        Self {
            error,
            connection: None,
        }
    }

    fn after_open(error: HistoryError, connection: Connection) -> Self {
        Self {
            error,
            connection: Some(Box::new(connection)),
        }
    }

    pub(super) fn context(&self) -> &str {
        self.error.context()
    }

    #[cfg(test)]
    pub(super) fn close_for_test(mut self) -> HistoryError {
        if let Some(connection) = self.connection.take() {
            let _ = (*connection).close();
        }
        self.error
    }
}

pub(super) fn open_store(
    root: &Path,
    interrupted: Arc<AtomicBool>,
    interrupt_on_drain: Arc<AtomicBool>,
) -> Result<(Connection, StoreInfo), StoreOpenError> {
    let root_identity = validate_root(root).map_err(StoreOpenError::before_open)?;
    let database = root.join(DATABASE_FILENAME);
    let was_missing = validate_known_entries(root).map_err(StoreOpenError::before_open)?;
    // Only this startup's exclusive private creation may initialize a store. A
    // pre-existing empty file can be truncated or foreign and stays untouched.
    let new_store = was_missing;
    let created_identity = if new_store {
        Some(create_private_database(&database).map_err(StoreOpenError::before_open)?)
    } else {
        None
    };
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let mut connection = Connection::open_with_flags(&database, flags)
        .map_err(classify_sql_error)
        .map_err(StoreOpenError::before_open)?;
    let opened = (|| {
        install_progress_handler(&connection, 1000, interrupted, interrupt_on_drain)?;
        apply_limits(&mut connection)?;
        apply_preflight_settings(&connection)?;

        if let Some(created) = &created_identity {
            let current = crate::safe_file::regular_path_identity(&database)
                .map_err(|_| unsafe_path("new history database identity is unavailable"))?;
            if !created.same_stable_file(&current) {
                return Err(unsafe_path("new history database identity changed"));
            }
        }
        let existing_version = if new_store {
            0
        } else {
            let version = verify_existing_identity(&connection)?;
            verify_schema(&connection, version)?;
            version
        };

        apply_and_verify_settings(&connection)?;
        if new_store {
            initialize_schema(&mut connection)?;
        }
        let mut version = if new_store { 1 } else { existing_version };
        if version == 1 {
            migrate_v1_to_v2(&mut connection)?;
            version = 2;
        }
        if version == 2 {
            migrate_v2_to_v3(&mut connection)?;
        }
        verify_schema(&connection, SCHEMA_VERSION)?;
        revalidate_root(root, &root_identity)?;
        validate_known_entries(root)?;

        Ok(StoreInfo {
            schema_version: u32::try_from(schema_version(&connection)?).map_err(|_| {
                HistoryError::new(
                    HistoryErrorKind::UnsupportedSchema,
                    "history schema version is unsupported",
                )
            })?,
            sqlite_version: connection
                .query_row("SELECT sqlite_version()", [], |row| row.get(0))
                .map_err(classify_sql_error)?,
            sqlite_source_id: connection
                .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
                .map_err(classify_sql_error)?,
        })
    })();
    match opened {
        Ok(info) => Ok((connection, info)),
        Err(error) => Err(StoreOpenError::after_open(error, connection)),
    }
}

pub(super) fn install_progress_handler(
    connection: &Connection,
    instructions: i32,
    draining: Arc<AtomicBool>,
    interrupt_on_drain: Arc<AtomicBool>,
) -> Result<(), HistoryError> {
    connection
        .progress_handler(
            instructions,
            Some(move || {
                interrupt_on_drain.load(Ordering::Acquire) && draining.load(Ordering::Acquire)
            }),
        )
        .map_err(classify_sql_error)
}

fn create_private_database(
    path: &Path,
) -> Result<crate::safe_file::RegularFileIdentity, HistoryError> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(PRIVATE_MODE)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| unsafe_path("history database could not be created exclusively"))?;
    crate::safe_file::regular_file_identity(&file, path)
        .map_err(|_| unsafe_path("new history database identity is unavailable"))
}

fn verify_existing_identity(connection: &Connection) -> Result<i64, HistoryError> {
    let application_id: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(classify_sql_error)?;
    let version = schema_version(connection)?;
    if application_id != APPLICATION_ID {
        return Err(HistoryError::new(
            HistoryErrorKind::UnsupportedSchema,
            "database is not a Loxa history store",
        ));
    }
    if !(1..=SCHEMA_VERSION).contains(&version) {
        return Err(HistoryError::new(
            HistoryErrorKind::UnsupportedSchema,
            format!("unsupported history schema version {version}"),
        ));
    }
    Ok(version)
}

fn initialize_schema(connection: &mut Connection) -> Result<(), HistoryError> {
    let transaction = connection.transaction().map_err(classify_sql_error)?;
    transaction
        .execute_batch(CREATE_CONVERSATIONS_V1)
        .map_err(classify_sql_error)?;
    transaction
        .execute_batch(CREATE_RECENCY_INDEX_V1)
        .map_err(classify_sql_error)?;
    transaction
        .pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(classify_sql_error)?;
    transaction
        .pragma_update(None, "user_version", 1)
        .map_err(classify_sql_error)?;
    transaction.commit().map_err(classify_sql_error)
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), HistoryError> {
    let transaction = connection.transaction().map_err(classify_sql_error)?;
    for definition in [
        CREATE_TURNS_V2,
        CREATE_ATTEMPTS_V2,
        CREATE_DRAFTS_V2,
        CREATE_ATTEMPTS_UNRESOLVED_V2,
        CREATE_ATTEMPTS_LATEST_V2,
        CREATE_DRAFTS_CLIENT_CONVERSATION_V2,
        CREATE_DRAFTS_CLIENT_UNBOUND_V2,
    ] {
        transaction
            .execute_batch(definition)
            .map_err(classify_sql_error)?;
    }
    transaction
        .pragma_update(None, "user_version", 2)
        .map_err(classify_sql_error)?;
    transaction.commit().map_err(classify_sql_error)
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), HistoryError> {
    migrate_v2_to_v3_inner(connection, false)
}

fn migrate_v2_to_v3_inner(
    connection: &mut Connection,
    inject_fault_after_recovery_index: bool,
) -> Result<(), HistoryError> {
    let transaction = connection.transaction().map_err(classify_sql_error)?;
    transaction
        .execute_batch(CREATE_ATTEMPT_CHUNKS_V3)
        .map_err(classify_sql_error)?;
    transaction
        .execute_batch(CREATE_ATTEMPTS_RECOVERY_V3)
        .map_err(classify_sql_error)?;
    if inject_fault_after_recovery_index {
        transaction
            .execute_batch("SELECT * FROM injected_missing_migration_object")
            .map_err(classify_sql_error)?;
    }
    transaction
        .execute_batch(CREATE_ATTEMPT_FINALIZATIONS_V3)
        .map_err(classify_sql_error)?;
    for definition in [
        CREATE_TURNS_CONVERSATION_V3,
        CREATE_TURNS_SELECTED_ATTEMPT_V3,
        CREATE_ATTEMPTS_PRIOR_V3,
        CREATE_DRAFTS_CONVERSATION_V3,
    ] {
        transaction
            .execute_batch(definition)
            .map_err(classify_sql_error)?;
    }
    transaction
        .pragma_update(None, "user_version", 3)
        .map_err(classify_sql_error)?;
    transaction.commit().map_err(classify_sql_error)
}

#[cfg(test)]
pub(super) fn fail_v2_to_v3_after_recovery_index(
    connection: &mut Connection,
) -> Result<(), HistoryError> {
    migrate_v2_to_v3_inner(connection, true)
}

fn verify_schema(connection: &Connection, version: i64) -> Result<(), HistoryError> {
    connection
        .prepare(
            "SELECT id, model_id, manifest_version, binding_profile,
                    qualified_profile, qualified_engine, qualified_engine_build,
                    primary_filename, primary_sha256, primary_size, primary_source_kind,
                    primary_source_repo, primary_source_revision, primary_source_filename,
                    draft_filename, draft_sha256, draft_size, draft_source_kind,
                    draft_source_repo, draft_source_revision, draft_source_filename,
                    title, system_instruction, max_output_tokens,
                    created_ms, updated_ms, revision, profile_revision, deleted
             FROM conversations WHERE 0",
        )
        .map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::Corrupt,
                "history schema is missing required conversation fields",
            )
        })?;
    if version >= 2 {
        verify_v2_fields(connection)?;
    }
    if version == 3 {
        verify_v3_fields(connection)?;
    }
    let expected_definitions: &[(&str, &str, &str)] = if version == 1 {
        &[
            ("index", "conversations_recency", CREATE_RECENCY_INDEX_V1),
            ("table", "conversations", CREATE_CONVERSATIONS_V1),
        ]
    } else if version == 2 {
        &[
            ("index", "attempts_latest", CREATE_ATTEMPTS_LATEST_V2),
            (
                "index",
                "attempts_unresolved",
                CREATE_ATTEMPTS_UNRESOLVED_V2,
            ),
            ("index", "conversations_recency", CREATE_RECENCY_INDEX_V1),
            (
                "index",
                "drafts_client_conversation",
                CREATE_DRAFTS_CLIENT_CONVERSATION_V2,
            ),
            (
                "index",
                "drafts_client_unbound",
                CREATE_DRAFTS_CLIENT_UNBOUND_V2,
            ),
            ("table", "attempts", CREATE_ATTEMPTS_V2),
            ("table", "conversations", CREATE_CONVERSATIONS_V1),
            ("table", "drafts", CREATE_DRAFTS_V2),
            ("table", "turns", CREATE_TURNS_V2),
        ]
    } else {
        &[
            ("index", "attempts_latest", CREATE_ATTEMPTS_LATEST_V2),
            ("index", "attempts_prior", CREATE_ATTEMPTS_PRIOR_V3),
            ("index", "attempts_recovery", CREATE_ATTEMPTS_RECOVERY_V3),
            (
                "index",
                "attempts_unresolved",
                CREATE_ATTEMPTS_UNRESOLVED_V2,
            ),
            ("index", "conversations_recency", CREATE_RECENCY_INDEX_V1),
            (
                "index",
                "drafts_client_conversation",
                CREATE_DRAFTS_CLIENT_CONVERSATION_V2,
            ),
            (
                "index",
                "drafts_client_unbound",
                CREATE_DRAFTS_CLIENT_UNBOUND_V2,
            ),
            (
                "index",
                "drafts_conversation",
                CREATE_DRAFTS_CONVERSATION_V3,
            ),
            ("index", "turns_conversation", CREATE_TURNS_CONVERSATION_V3),
            (
                "index",
                "turns_selected_attempt",
                CREATE_TURNS_SELECTED_ATTEMPT_V3,
            ),
            ("table", "attempt_chunks", CREATE_ATTEMPT_CHUNKS_V3),
            (
                "table",
                "attempt_finalizations",
                CREATE_ATTEMPT_FINALIZATIONS_V3,
            ),
            ("table", "attempts", CREATE_ATTEMPTS_V2),
            ("table", "conversations", CREATE_CONVERSATIONS_V1),
            ("table", "drafts", CREATE_DRAFTS_V2),
            ("table", "turns", CREATE_TURNS_V2),
        ]
    };
    for (kind, name, definition) in expected_definitions {
        if normalize_sql(&schema_sql(connection, kind, name)?) != normalize_sql(definition) {
            return Err(HistoryError::new(
                HistoryErrorKind::Corrupt,
                format!("history schema definition does not match schema version {version}"),
            ));
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name
             FROM sqlite_schema
             WHERE type IN ('table', 'index', 'view', 'trigger')
               AND name NOT GLOB 'sqlite_*'
             ORDER BY type, name
             LIMIT 17",
        )
        .map_err(classify_sql_error)?;
    let objects = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(classify_sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(classify_sql_error)?;
    let expected = expected_definitions
        .iter()
        .map(|(kind, name, _)| {
            let table = match *name {
                "attempts_latest"
                | "attempts_prior"
                | "attempts_recovery"
                | "attempts_unresolved" => "attempts",
                "conversations_recency" => "conversations",
                "drafts_client_conversation" | "drafts_client_unbound" | "drafts_conversation" => {
                    "drafts"
                }
                "turns_conversation" | "turns_selected_attempt" => "turns",
                name => name,
            };
            ((*kind).to_owned(), (*name).to_owned(), table.to_owned())
        })
        .collect::<Vec<_>>();
    if objects != expected {
        return Err(HistoryError::new(
            HistoryErrorKind::Corrupt,
            "history schema contains unexpected objects",
        ));
    }
    Ok(())
}

fn verify_v2_fields(connection: &Connection) -> Result<(), HistoryError> {
    for query in [
        "SELECT id, conversation_id, ordinal, user_text, selected_attempt_id FROM turns WHERE 0",
        "SELECT id, turn_id, attempt_number, submission_id, submission_hash,
                admitted_conversation_revision, admitted_profile_revision, prior_attempt_id,
                owner_epoch, operation_generation, model_id, applied_engine_build,
                applied_engine_version, runtime_fingerprint, effective_context,
                system_instruction, max_output_tokens, prompt_basis, execution_outcome,
                save_outcome, saved_end, generated_end, terminal_saved_end, failure_code,
                created_ms, updated_ms FROM attempts WHERE 0",
        "SELECT id, desktop_client_id, conversation_id, revision, consumed_revision,
                text, text_hash, updated_ms FROM drafts WHERE 0",
    ] {
        connection.prepare(query).map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::Corrupt,
                "history schema is missing required schema-2 fields",
            )
        })?;
    }
    Ok(())
}

fn verify_v3_fields(connection: &Connection) -> Result<(), HistoryError> {
    for query in [
        "SELECT attempt_id, start_offset, end_offset, content FROM attempt_chunks WHERE 0",
        "SELECT attempt_id, start_offset, end_offset FROM attempt_finalizations WHERE 0",
    ] {
        connection.prepare(query).map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::Corrupt,
                "history schema is missing required schema-3 fields",
            )
        })?;
    }
    Ok(())
}

pub(super) fn durable_checkpoint(connection: &Connection) -> Result<(), HistoryError> {
    if !connection.is_autocommit() {
        return Err(HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history transaction is still unresolved",
        ));
    }
    let (busy, _log, _checkpointed): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(FULL)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(classify_sql_error)?;
    if busy != 0 || !connection.is_autocommit() {
        return Err(HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history durability is not yet reconciled",
        ));
    }
    Ok(())
}

fn schema_sql(
    connection: &Connection,
    kind: &'static str,
    name: &'static str,
) -> Result<String, HistoryError> {
    connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2",
            [kind, name],
            |row| row.get(0),
        )
        .map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::Corrupt,
                "history schema is missing a required definition",
            )
        })
}

fn normalize_sql(sql: &str) -> String {
    sql.trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn apply_limits(connection: &mut Connection) -> Result<(), HistoryError> {
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_LENGTH, 256 * 1024),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, 64 * 1024),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 128),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, 64),
        (Limit::SQLITE_LIMIT_ATTACHED, 0),
    ] {
        connection
            .set_limit(limit, value)
            .map_err(classify_sql_error)?;
    }
    Ok(())
}

fn apply_preflight_settings(connection: &Connection) -> Result<(), HistoryError> {
    connection
        .execute_batch("PRAGMA trusted_schema = OFF;")
        .map_err(classify_sql_error)?;
    verify_integer_pragma(connection, "trusted_schema", 0)
}

fn apply_and_verify_settings(connection: &Connection) -> Result<(), HistoryError> {
    connection
        .busy_timeout(Duration::from_millis(250))
        .map_err(classify_sql_error)?;
    let journal: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(classify_sql_error)?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err(HistoryError::new(
            HistoryErrorKind::Io,
            "history database could not enable WAL",
        ));
    }
    connection
        .execute_batch(
            "PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;
             PRAGMA trusted_schema = OFF;
             PRAGMA mmap_size = 0;
             PRAGMA cache_size = -2048;
             PRAGMA wal_autocheckpoint = 256;",
        )
        .map_err(classify_sql_error)?;
    #[cfg(target_os = "macos")]
    connection
        .execute_batch("PRAGMA fullfsync = ON;")
        .map_err(classify_sql_error)?;

    verify_integer_pragma(connection, "synchronous", 2)?;
    verify_integer_pragma(connection, "foreign_keys", 1)?;
    verify_integer_pragma(connection, "trusted_schema", 0)?;
    verify_integer_pragma(connection, "mmap_size", 0)?;
    verify_integer_pragma(connection, "wal_autocheckpoint", 256)?;
    #[cfg(target_os = "macos")]
    verify_integer_pragma(connection, "fullfsync", 1)?;
    Ok(())
}

fn verify_integer_pragma(
    connection: &Connection,
    pragma: &'static str,
    expected: i64,
) -> Result<(), HistoryError> {
    let sql = format!("PRAGMA {pragma}");
    let actual: i64 = connection
        .query_row(&sql, [], |row| row.get(0))
        .map_err(classify_sql_error)?;
    if actual != expected {
        return Err(HistoryError::new(
            HistoryErrorKind::Io,
            format!("history database did not retain required {pragma} setting"),
        ));
    }
    Ok(())
}

fn schema_version(connection: &Connection) -> Result<i64, HistoryError> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(classify_sql_error)
}

struct RootIdentity {
    device: u64,
    inode: u64,
    uid: u32,
}

fn validate_root(root: &Path) -> Result<RootIdentity, HistoryError> {
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(unsafe_path("history root must be an absolute clean path"));
    }
    let canonical =
        fs::canonicalize(root).map_err(|_| unsafe_path("history root is unavailable"))?;
    if canonical != root {
        return Err(unsafe_path("history root must be canonical"));
    }
    let metadata = fs::symlink_metadata(root)
        .map_err(|_| unsafe_path("history root metadata is unavailable"))?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != current_uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(unsafe_path("history root is not a private user directory"));
    }
    Ok(RootIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        uid: metadata.uid(),
    })
}

fn revalidate_root(root: &Path, expected: &RootIdentity) -> Result<(), HistoryError> {
    let current = validate_root(root)?;
    if current.device != expected.device
        || current.inode != expected.inode
        || current.uid != expected.uid
    {
        return Err(unsafe_path("history root identity changed"));
    }
    Ok(())
}

fn validate_known_entries(root: &Path) -> Result<bool, HistoryError> {
    let database = root.join(DATABASE_FILENAME);
    let mut database_missing = false;
    for path in known_paths(root) {
        match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_file()
                    && metadata.uid() == current_uid()
                    && metadata.nlink() == 1
                    && metadata.permissions().mode() & 0o777 == PRIVATE_MODE => {}
            Ok(_) => return Err(unsafe_path("history database path is unsafe")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if path == database {
                    database_missing = true;
                }
            }
            Err(_) => return Err(unsafe_path("history database metadata is unavailable")),
        }
    }
    if database_missing
        && known_paths(root)
            .into_iter()
            .skip(1)
            .any(|path| path.exists())
    {
        return Err(unsafe_path(
            "history database sidecar exists without its database",
        ));
    }
    Ok(database_missing)
}

fn known_paths(root: &Path) -> [PathBuf; 4] {
    [
        root.join(DATABASE_FILENAME),
        root.join(format!("{DATABASE_FILENAME}-wal")),
        root.join(format!("{DATABASE_FILENAME}-shm")),
        root.join(format!("{DATABASE_FILENAME}-journal")),
    ]
}

fn current_uid() -> u32 {
    unsafe { libc::geteuid() }
}

fn unsafe_path(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::UnsafePath, context)
}

pub(super) fn classify_sql_error(error: rusqlite::Error) -> HistoryError {
    let kind = match &error {
        rusqlite::Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => HistoryErrorKind::Busy,
            ErrorCode::ReadOnly => HistoryErrorKind::ReadOnly,
            ErrorCode::DiskFull => HistoryErrorKind::DiskFull,
            ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase => HistoryErrorKind::Corrupt,
            ErrorCode::OperationInterrupted => HistoryErrorKind::Interrupted,
            _ => HistoryErrorKind::Io,
        },
        rusqlite::Error::FromSqlConversionFailure(_, _, _) => HistoryErrorKind::Corrupt,
        _ => HistoryErrorKind::Io,
    };
    HistoryError::new(kind, "history database operation failed")
}

#[cfg(test)]
pub(super) fn application_id() -> i64 {
    APPLICATION_ID
}

#[cfg(test)]
pub(super) fn create_v1_fixture(path: &Path) {
    let connection = Connection::open(path).expect("create schema-1 fixture");
    connection
        .execute_batch(CREATE_CONVERSATIONS_V1)
        .expect("create schema-1 conversations");
    connection
        .execute_batch(CREATE_RECENCY_INDEX_V1)
        .expect("create schema-1 recency index");
    connection
        .pragma_update(None, "application_id", APPLICATION_ID)
        .expect("set schema-1 application id");
    connection
        .pragma_update(None, "user_version", 1)
        .expect("set schema-1 version");
    connection.close().expect("close schema-1 fixture");
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_MODE))
        .expect("make schema-1 fixture private");
}

#[cfg(test)]
pub(super) fn create_v2_fixture(path: &Path) {
    create_v1_fixture(path);
    let mut connection = Connection::open(path).expect("open schema-2 fixture");
    migrate_v1_to_v2(&mut connection).expect("migrate schema-2 fixture");
    connection.close().expect("close schema-2 fixture");
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_MODE))
        .expect("make schema-2 fixture private");
}
