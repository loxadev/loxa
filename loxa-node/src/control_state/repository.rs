use super::schema::{schema_checksum, MIGRATION_NAME, SCHEMA_V1, SCHEMA_VERSION};
use loxa_protocol::v2::DecimalU64;
use loxa_protocol::v2::{
    EventId, OperationId, SlotId, StreamEpoch, V2Node, V2Operation, V2OperationErrorCode,
    V2OperationKind, V2OperationProgress, V2OperationStatus, V2PublicError, V2Slot,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use rusqlite::backup::Backup;
use rusqlite::config::DbConfig;
use rusqlite::limits::Limit;
use rusqlite::{
    Connection, Error as SqlError, ErrorCode, OpenFlags, Transaction, TransactionBehavior,
};
use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
mod descriptor_vfs {
    use rusqlite::ffi;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};
    use std::os::raw::c_int;
    use std::sync::{Mutex, OnceLock};

    static OPENS: OnceLock<Mutex<HashMap<usize, (usize, c_int)>>> = OnceLock::new();
    static OPEN_OVERRIDE_LOCK: Mutex<()> = Mutex::new(());
    static ACTIVE_OPEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    static NEXT_NAME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    thread_local! {
        static BOUND_FD: Cell<Option<c_int>> = const { Cell::new(None) };
    }

    fn opens() -> &'static Mutex<HashMap<usize, (usize, c_int)>> {
        OPENS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    unsafe extern "C" fn bound_posix_open(
        path: *const std::os::raw::c_char,
        flags: c_int,
        mode: c_int,
    ) -> c_int {
        if let Some(fd) = BOUND_FD.with(Cell::take) {
            let command = if flags & libc::O_CLOEXEC != 0 {
                libc::F_DUPFD_CLOEXEC
            } else {
                libc::F_DUPFD
            };
            return unsafe { libc::fcntl(fd, command, 0) };
        }
        let pointer = ACTIVE_OPEN.load(std::sync::atomic::Ordering::Acquire);
        if pointer == 0 {
            return -1;
        }
        let open: unsafe extern "C" fn(*const std::os::raw::c_char, c_int, c_int) -> c_int =
            unsafe { std::mem::transmute(pointer) };
        unsafe { open(path, flags, mode) }
    }

    struct OpenOverride<'a> {
        vfs: *mut ffi::sqlite3_vfs,
        name: &'a CStr,
        original: ffi::sqlite3_syscall_ptr,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl OpenOverride<'_> {
        unsafe fn install(vfs: *mut ffi::sqlite3_vfs) -> Result<Self, ()> {
            let lock = OPEN_OVERRIDE_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let get = unsafe { (*vfs).xGetSystemCall }.ok_or(())?;
            let set = unsafe { (*vfs).xSetSystemCall }.ok_or(())?;
            let name = c"open";
            let original = unsafe { get(vfs, name.as_ptr()) }.ok_or(())?;
            ACTIVE_OPEN.store(
                original as *const () as usize,
                std::sync::atomic::Ordering::Release,
            );
            let replacement: ffi::sqlite3_syscall_ptr = Some(unsafe {
                std::mem::transmute::<
                    unsafe extern "C" fn(*const std::os::raw::c_char, c_int, c_int) -> c_int,
                    unsafe extern "C" fn(),
                >(bound_posix_open)
            });
            if unsafe { set(vfs, name.as_ptr(), replacement) } != ffi::SQLITE_OK {
                ACTIVE_OPEN.store(0, std::sync::atomic::Ordering::Release);
                return Err(());
            }
            Ok(Self {
                vfs,
                name,
                original: Some(original),
                _lock: lock,
            })
        }
    }

    impl Drop for OpenOverride<'_> {
        fn drop(&mut self) {
            if let Some(set) = unsafe { (*self.vfs).xSetSystemCall } {
                unsafe { set(self.vfs, self.name.as_ptr(), self.original) };
            }
            ACTIVE_OPEN.store(0, std::sync::atomic::Ordering::Release);
            BOUND_FD.with(|slot| slot.set(None));
        }
    }

    unsafe extern "C" fn bound_open(
        vfs: *mut ffi::sqlite3_vfs,
        name: ffi::sqlite3_filename,
        file: *mut ffi::sqlite3_file,
        flags: c_int,
        output_flags: *mut c_int,
    ) -> c_int {
        let (original, descriptor) = {
            let entries = match opens().lock() {
                Ok(entries) => entries,
                Err(_) => return ffi::SQLITE_CANTOPEN,
            };
            let Some((original, descriptor)) = entries.get(&(vfs as usize)) else {
                return ffi::SQLITE_CANTOPEN;
            };
            (*original as *mut ffi::sqlite3_vfs, *descriptor)
        };
        match unsafe { (*original).xOpen } {
            Some(open) => {
                let override_guard = if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
                    match unsafe { OpenOverride::install(original) } {
                        Ok(guard) => {
                            BOUND_FD.with(|slot| slot.set(Some(descriptor)));
                            Some(guard)
                        }
                        Err(()) => return ffi::SQLITE_CANTOPEN,
                    }
                } else {
                    None
                };
                let result = unsafe { open(original, name, file, flags, output_flags) };
                drop(override_guard);
                result
            }
            None => ffi::SQLITE_CANTOPEN,
        }
    }

    pub(super) struct Registration {
        vfs: Box<ffi::sqlite3_vfs>,
        name: CString,
    }

    impl Registration {
        pub(super) fn new(descriptor: c_int) -> Result<Self, ()> {
            unsafe { ffi::sqlite3_initialize() };
            let original = unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) };
            if original.is_null() {
                return Err(());
            }
            let name = CString::new(format!(
                "loxa-bound-{}-{}",
                std::process::id(),
                NEXT_NAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .map_err(|_| ())?;
            let mut vfs = Box::new(unsafe { *original });
            vfs.pNext = std::ptr::null_mut();
            vfs.zName = name.as_ptr();
            vfs.xOpen = Some(bound_open);
            let pointer = (&mut *vfs) as *mut ffi::sqlite3_vfs;
            opens()
                .lock()
                .map_err(|_| ())?
                .insert(pointer as usize, (original as usize, descriptor));
            if unsafe { ffi::sqlite3_vfs_register(pointer, 0) } != ffi::SQLITE_OK {
                if let Ok(mut entries) = opens().lock() {
                    entries.remove(&(pointer as usize));
                }
                return Err(());
            }
            Ok(Self { vfs, name })
        }

        pub(super) fn name(&self) -> &std::ffi::CStr {
            self.name.as_c_str()
        }
    }

    impl Drop for Registration {
        fn drop(&mut self) {
            let pointer = (&mut *self.vfs) as *mut ffi::sqlite3_vfs;
            unsafe { ffi::sqlite3_vfs_unregister(pointer) };
            if let Ok(mut entries) = opens().lock() {
                entries.remove(&(pointer as usize));
            }
        }
    }

    #[cfg(test)]
    pub(super) fn with_open_override_for_test<T>(work: impl FnOnce() -> T) -> T {
        unsafe { ffi::sqlite3_initialize() };
        let original = unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) };
        let guard = unsafe { OpenOverride::install(original) }.expect("install test override");
        let value = work();
        drop(guard);
        value
    }

    #[cfg(test)]
    pub(super) fn open_override_is_installed() -> bool {
        unsafe { ffi::sqlite3_initialize() };
        let vfs = unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) };
        let Some(get) = (unsafe { (*vfs).xGetSystemCall }) else {
            return false;
        };
        let name = c"open";
        let current = unsafe { get(vfs, name.as_ptr()) };
        current.is_some_and(|function| function as *const () == bound_posix_open as *const ())
    }
}

const SQLITE_LENGTH_LIMIT: i32 = 4 * 1024 * 1024;
const SQLITE_SQL_LENGTH_LIMIT: i32 = 1024 * 1024;
const SQLITE_COLUMN_LIMIT: i32 = 64;
const SQLITE_EXPR_DEPTH_LIMIT: i32 = 100;
const SQLITE_COMPOUND_SELECT_LIMIT: i32 = 16;
const SQLITE_VDBE_OP_LIMIT: i32 = 100_000;
const SQLITE_FUNCTION_ARG_LIMIT: i32 = 32;
const SQLITE_ATTACHED_LIMIT: i32 = 0;
const SQLITE_LIKE_PATTERN_LIMIT: i32 = 8 * 1024;
const SQLITE_VARIABLE_LIMIT: i32 = 64;
const SQLITE_TRIGGER_DEPTH_LIMIT: i32 = 16;
const SQLITE_WORKER_THREADS_LIMIT: i32 = 0;

type SchemaObject = (String, String, String, Option<String>);

pub(crate) trait ControlIdGenerator {
    fn new_slot_id(&mut self) -> SlotId;
    fn new_stream_epoch(&mut self) -> StreamEpoch;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RepositoryErrorClass {
    UnsafePath,
    Corrupt,
    UnsupportedSchema,
    IdentityMismatch,
    Database,
    Durability,
    Overflow,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct RepositoryError {
    class: RepositoryErrorClass,
}

impl RepositoryError {
    fn new(class: RepositoryErrorClass) -> Self {
        Self { class }
    }

    #[cfg(test)]
    fn corrupt() -> Self {
        Self::new(RepositoryErrorClass::Corrupt)
    }

    pub(crate) fn class(&self) -> RepositoryErrorClass {
        self.class
    }
}

impl fmt::Debug for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepositoryError")
            .field("class", &self.class)
            .finish()
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.class {
            RepositoryErrorClass::UnsafePath => "unsafe control-state path",
            RepositoryErrorClass::Corrupt => "corrupt control-state database",
            RepositoryErrorClass::UnsupportedSchema => "unsupported control-state schema",
            RepositoryErrorClass::IdentityMismatch => "control-state identity mismatch",
            RepositoryErrorClass::Database => "control-state database failure",
            RepositoryErrorClass::Durability => "control-state durability failure",
            RepositoryErrorClass::Overflow => "control-state counter overflow",
        })
    }
}

impl std::error::Error for RepositoryError {}

impl From<SqlError> for RepositoryError {
    fn from(error: SqlError) -> Self {
        map_sql_error(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidationSummary {
    pub(crate) node_rows: usize,
    pub(crate) slot_rows: usize,
    pub(crate) slot_name: String,
    pub(crate) revision: u64,
    pub(crate) cursor: u64,
    pub(crate) event_rows: usize,
    pub(crate) node_id: NodeId,
    pub(crate) slot_id: SlotId,
    pub(crate) epoch: StreamEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RestoreSummary {
    pub(crate) epoch: StreamEpoch,
    pub(crate) revision: u64,
    pub(crate) cursor: u64,
    pub(crate) event_rows: usize,
}

pub(crate) struct ControlRepository {
    connection: ValidatedConnection,
    path: PathBuf,
    expected_node_id: NodeId,
    slot_id: SlotId,
    stream_epoch: StreamEpoch,
    #[cfg(unix)]
    _directory_guard: fs::File,
    #[cfg(unix)]
    _main_guard: fs::File,
}

struct ValidatedConnection {
    connection: Connection,
    #[cfg(unix)]
    _registration: descriptor_vfs::Registration,
}

impl std::ops::Deref for ValidatedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl std::ops::DerefMut for ValidatedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl fmt::Debug for ControlRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlRepository")
            .field("slot_id", &self.slot_id)
            .field("stream_epoch", &self.stream_epoch)
            .finish_non_exhaustive()
    }
}

struct PreparedStorage {
    path: PathBuf,
    #[cfg(unix)]
    directory_guard: fs::File,
    #[cfg(unix)]
    main_guard: fs::File,
}

#[derive(Clone, Debug)]
struct MigrationRollbackProof {
    node_id: NodeId,
    slot_id: SlotId,
    epoch: StreamEpoch,
    revision: u64,
    cursor: u64,
    digest: [u8; 32],
}

impl ControlRepository {
    pub(crate) fn open_or_create(
        path: &Path,
        node_id: NodeId,
        ids: &mut dyn ControlIdGenerator,
    ) -> Result<Self, RepositoryError> {
        let prepared = prepare_storage_path(path)?;
        validate_auxiliary_files(path)?;
        let mut connection = open_validated_connection_after(&prepared, false, || {})?;
        configure_defensively(&connection)?;
        let initialized =
            open_existing_or_initialize_in_one_transaction(&mut connection, node_id, ids)?;
        let summary = validate_connection(&connection, Some(node_id))?;
        let repository = Self {
            connection,
            path: path.to_owned(),
            expected_node_id: node_id,
            slot_id: summary.slot_id,
            stream_epoch: summary.epoch,
            #[cfg(unix)]
            _directory_guard: prepared.directory_guard,
            #[cfg(unix)]
            _main_guard: prepared.main_guard,
        };
        if initialized {
            repository.checkpoint()?;
            repository.publish_migration_backup()?;
        }
        Ok(repository)
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub(crate) fn stream_epoch(&self) -> StreamEpoch {
        self.stream_epoch
    }

    pub(crate) fn transaction<T>(
        &mut self,
        work: impl FnOnce(&Transaction<'_>) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = work(&transaction)?;
        transaction.commit()?;
        Ok(value)
    }

    pub(crate) fn validate_all(&self) -> Result<ValidationSummary, RepositoryError> {
        validate_connection(&self.connection, Some(self.expected_node_id))
    }

    pub(crate) fn migration_backup_path(&self) -> Result<PathBuf, RepositoryError> {
        migration_backup_path(&self.path)
    }

    pub(crate) fn validate_backup(
        &self,
        backup: &Path,
    ) -> Result<ValidationSummary, RepositoryError> {
        validate_database_file(backup, Some(self.expected_node_id))
    }

    pub(crate) fn backup_before_migration(&self) -> Result<PathBuf, RepositoryError> {
        self.checkpoint()?;
        self.publish_migration_backup()
    }

    fn migration_rollback_proof(&self) -> Result<MigrationRollbackProof, RepositoryError> {
        let backup = self.migration_backup_path()?;
        let summary = validate_database_file(&backup, Some(self.expected_node_id))?;
        Ok(MigrationRollbackProof {
            node_id: self.expected_node_id,
            slot_id: summary.slot_id,
            epoch: summary.epoch,
            revision: summary.revision,
            cursor: summary.cursor,
            digest: digest_private_file(&backup)?,
        })
    }

    pub(crate) fn restore_offline(
        backup: &Path,
        destination: &Path,
    ) -> Result<RestoreSummary, RepositoryError> {
        let source_summary = validate_database_file(backup, None)?;
        let prepared_destination = prepare_destination_parent(destination)?;
        validate_optional_private_file(destination, None)?;
        ensure_auxiliary_files_absent(destination)?;
        let temporary = unique_temporary_path(destination, "restore")?;
        let result = (|| {
            copy_database(backup, &temporary)?;
            rotate_lineage(&temporary, source_summary.node_id)?;
            validate_database_file(&temporary, Some(source_summary.node_id))?;
            sync_private_file(&temporary)?;
            atomic_replace(&temporary, destination)?;
            sync_directory(&prepared_destination)?;
            let reopened = validate_database_file(destination, Some(source_summary.node_id))?;
            Ok(RestoreSummary {
                epoch: reopened.epoch,
                revision: reopened.revision,
                cursor: reopened.cursor,
                event_rows: reopened.event_rows,
            })
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn restore_verified_migration_backup(
        backup: &Path,
        destination: &Path,
        proof: &MigrationRollbackProof,
    ) -> Result<(), RepositoryError> {
        if migration_backup_path(destination)? != backup {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
        let backup_summary = validate_database_file(backup, Some(proof.node_id))?;
        if backup_summary.slot_id != proof.slot_id
            || backup_summary.epoch != proof.epoch
            || backup_summary.revision != proof.revision
            || backup_summary.cursor != proof.cursor
            || digest_private_file(backup)? != proof.digest
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        // The failed migration may have left an unreadable database, so the
        // rollback proof is the exact retained sibling path plus a fully
        // validated backup. Do not require the failed destination to parse.
        validate_optional_private_file(destination, None)?;
        ensure_auxiliary_files_absent(destination)?;
        let parent = prepare_destination_parent(destination)?;
        let temporary = unique_temporary_path(destination, "rollback")?;
        let result = (|| {
            copy_database(backup, &temporary)?;
            let copied = validate_database_file(&temporary, Some(proof.node_id))?;
            if copied.slot_id != proof.slot_id
                || copied.epoch != proof.epoch
                || copied.revision != proof.revision
                || copied.cursor != proof.cursor
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
            sync_private_file(&temporary)?;
            atomic_replace(&temporary, destination)?;
            sync_directory(&parent)?;
            let restored = validate_database_file(destination, Some(proof.node_id))?;
            if restored.slot_id != proof.slot_id
                || restored.epoch != proof.epoch
                || restored.revision != proof.revision
                || restored.cursor != proof.cursor
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn checkpoint(&self) -> Result<(), RepositoryError> {
        let (busy, _log, _checkpointed): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(FULL)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy != 0 {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        Ok(())
    }

    fn publish_migration_backup(&self) -> Result<PathBuf, RepositoryError> {
        let backup = self.migration_backup_path()?;
        validate_optional_private_file(&backup, None)?;
        let temporary = unique_temporary_path(&backup, "backup")?;
        let result = (|| {
            backup_connection(&self.connection, &temporary)?;
            validate_database_file(&temporary, Some(self.expected_node_id))?;
            sync_private_file(&temporary)?;
            atomic_replace(&temporary, &backup)?;
            let parent = backup
                .parent()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            sync_directory_path(parent)?;
            validate_database_file(&backup, Some(self.expected_node_id))?;
            Ok(backup.clone())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

fn open_existing_or_initialize_in_one_transaction(
    connection: &mut Connection,
    node_id: NodeId,
    ids: &mut dyn ControlIdGenerator,
) -> Result<bool, RepositoryError> {
    let has_schema: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
        [],
        |row| row.get(0),
    )?;
    if has_schema {
        return Ok(false);
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let slot_id = ids.new_slot_id();
    let stream_epoch = ids.new_stream_epoch();
    transaction.execute_batch(SCHEMA_V1)?;
    let applied_at_ms = current_unix_ms()?;
    transaction.execute(
        "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, ?4)",
        (SCHEMA_VERSION, MIGRATION_NAME, schema_checksum(), applied_at_ms),
    )?;
    transaction.execute(
        "INSERT INTO control_meta(singleton, node_id, slot_id, stream_epoch, revision, cursor, schema_version, migration_source, last_committed_at_unix_ms) VALUES(1, ?1, ?2, ?3, '1', '1', 1, 'fresh', ?4)",
        (node_id.to_string(), slot_id.to_string(), stream_epoch.to_string(), applied_at_ms.to_string()),
    )?;
    transaction.execute(
        "INSERT INTO node_state(singleton, node_id, node_instance_id, control_endpoint, status, can_load, can_unload, can_download) VALUES(1, ?1, NULL, NULL, 'unpublished', 0, 0, 0)",
        [node_id.to_string()],
    )?;
    transaction.execute(
        "INSERT INTO slot_state(singleton, slot_id, name, status, model_id, operation_id, updated_revision, updated_at_unix_ms) VALUES(1, ?1, 'default', 'unloaded', NULL, NULL, '1', ?2)",
        (slot_id.to_string(), applied_at_ms.to_string()),
    )?;
    transaction.execute(
        "INSERT INTO events(event_id, stream_epoch, sequence, revision, node_instance_id, v1_sequence, event_kind, payload_json) VALUES(?1, ?2, '1', '1', NULL, NULL, 'initialized', ?3)",
        (
            EventId::new_v4().to_string(),
            stream_epoch.to_string(),
            unloaded_slot_payload(node_id, slot_id)?,
        ),
    )?;
    transaction.commit()?;
    Ok(true)
}

fn validate_connection(
    connection: &Connection,
    expected_node_id: Option<NodeId>,
) -> Result<ValidationSummary, RepositoryError> {
    quick_check(connection)?;
    validate_schema_shape(connection)?;
    validate_migration_ledger(connection)?;

    let raw_meta: (String, String, String, String, String, i64, String) = connection
        .query_row(
            "SELECT node_id, slot_id, stream_epoch, revision, cursor, schema_version, last_committed_at_unix_ms FROM control_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .map_err(classify_missing_row)?;
    if raw_meta.5 != SCHEMA_VERSION {
        return Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        ));
    }
    let node_id = NodeId::from_str(&raw_meta.0)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    if expected_node_id.is_some_and(|expected| expected != node_id) {
        return Err(RepositoryError::new(RepositoryErrorClass::IdentityMismatch));
    }
    let slot_id = SlotId::from_str(&raw_meta.1)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let epoch = StreamEpoch::from_str(&raw_meta.2)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let revision = parse_canonical_u64(&raw_meta.3)?;
    let cursor = parse_canonical_u64(&raw_meta.4)?;
    let last_committed_at = parse_canonical_u64(&raw_meta.6)?;

    let node_rows = count_rows(connection, "node_state")?;
    let slot_rows = count_rows(connection, "slot_state")?;
    let event_rows = count_rows(connection, "events")?;
    if node_rows != 1 || slot_rows != 1 || event_rows == 0 {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let stored_node: (String, Option<String>, Option<String>, String, i64, i64, i64) = connection.query_row(
        "SELECT node_id, node_instance_id, control_endpoint, status, can_load, can_unload, can_download FROM node_state WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
    )?;
    if stored_node.0 != node_id.to_string()
        || ![stored_node.4, stored_node.5, stored_node.6]
            .into_iter()
            .all(|value| matches!(value, 0 | 1))
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let node_instance = stored_node
        .1
        .as_deref()
        .map(NodeInstanceId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    if stored_node
        .2
        .as_deref()
        .is_some_and(|endpoint| !valid_control_endpoint(endpoint))
        || match stored_node.3.as_str() {
            "unpublished" => node_instance.is_some() || stored_node.2.is_some(),
            "running" | "stopping" => node_instance.is_none() || stored_node.2.is_none(),
            "recovery" => false,
            _ => true,
        }
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let stored_slot: (String, String, String, Option<String>, Option<String>, String, String) = connection.query_row(
        "SELECT slot_id, name, status, model_id, operation_id, updated_revision, updated_at_unix_ms FROM slot_state WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
    )?;
    let slot_operation_id = stored_slot
        .4
        .as_deref()
        .map(OperationId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let slot_state_valid = match stored_slot.2.as_str() {
        "unloaded" => stored_slot.3.is_none() && slot_operation_id.is_none(),
        "loading" => {
            stored_slot.3.as_deref().is_some_and(valid_model_id) && slot_operation_id.is_some()
        }
        "ready" => {
            stored_slot.3.as_deref().is_some_and(valid_model_id) && slot_operation_id.is_none()
        }
        "unloading" => {
            stored_slot.3.as_deref().is_some_and(valid_model_id) && slot_operation_id.is_some()
        }
        "recovery" => slot_operation_id.is_none(),
        _ => false,
    };
    if stored_slot.0 != slot_id.to_string()
        || stored_slot.1 != "default"
        || !slot_state_valid
        || stored_slot
            .3
            .as_deref()
            .is_some_and(|model| !valid_model_id(model))
        || parse_canonical_u64(&stored_slot.5)? > revision
        || parse_canonical_u64(&stored_slot.6)? > last_committed_at
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    validate_operation_rows(connection, node_id, slot_id, revision, last_committed_at)?;
    validate_event_rows(
        connection,
        node_id,
        slot_id,
        epoch,
        revision,
        cursor,
        last_committed_at,
    )?;
    Ok(ValidationSummary {
        node_rows,
        slot_rows,
        slot_name: stored_slot.1,
        revision,
        cursor,
        event_rows,
        node_id,
        slot_id,
        epoch,
    })
}

fn validate_schema_shape(connection: &Connection) -> Result<(), RepositoryError> {
    let expected_connection = Connection::open_in_memory()?;
    expected_connection.execute_batch(SCHEMA_V1)?;
    let expected = collect_schema_shape(&expected_connection)?;
    let actual = collect_schema_shape(connection)?;
    if actual != expected {
        return Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        ));
    }
    Ok(())
}

fn collect_schema_shape(connection: &Connection) -> Result<Vec<SchemaObject>, RepositoryError> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
    )?;
    let shape = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(RepositoryError::from)?;
    Ok(shape)
}

fn validate_migration_ledger(connection: &Connection) -> Result<(), RepositoryError> {
    let rows = count_rows(connection, "loxa_schema_migrations")?;
    if rows != 1 {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let (version, name, checksum): (i64, String, String) = connection.query_row(
        "SELECT version, name, checksum FROM loxa_schema_migrations",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if version > SCHEMA_VERSION {
        return Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        ));
    }
    if version != SCHEMA_VERSION || name != MIGRATION_NAME || checksum != schema_checksum() {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn validate_operation_rows(
    connection: &Connection,
    node_id: NodeId,
    slot_id: SlotId,
    revision: u64,
    last_committed_at: u64,
) -> Result<(), RepositoryError> {
    let mut statement = connection.prepare(
        "SELECT operation_id, slot_id, admitting_node_instance_id, v1_ordinal, kind, status, model_id, progress_current, progress_total, error_code, error_message, created_revision, updated_revision, created_at_unix_ms, updated_at_unix_ms FROM operations",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let operation_id = OperationId::from_str(&row.get::<_, String>(0)?)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        let stored_slot_id = SlotId::from_str(&row.get::<_, String>(1)?)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        NodeInstanceId::from_str(&row.get::<_, String>(2)?)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        let v1_ordinal: Option<i64> = row.get(3)?;
        let kind_text: String = row.get(4)?;
        let status_text: String = row.get(5)?;
        let model_id: Option<String> = row.get(6)?;
        let progress_current: Option<String> = row.get(7)?;
        let progress_total: Option<String> = row.get(8)?;
        let error_code: Option<String> = row.get(9)?;
        let error_message: Option<String> = row.get(10)?;
        let created = parse_canonical_u64(&row.get::<_, String>(11)?)?;
        let updated = parse_canonical_u64(&row.get::<_, String>(12)?)?;
        let created_at = parse_canonical_u64(&row.get::<_, String>(13)?)?;
        let updated_at = parse_canonical_u64(&row.get::<_, String>(14)?)?;
        let kind: V2OperationKind = parse_closed_string(&kind_text)?;
        let status: V2OperationStatus = parse_closed_string(&status_text)?;
        let progress = match (progress_current, progress_total) {
            (None, None) => None,
            (Some(current), Some(total)) => Some(V2OperationProgress {
                completed_bytes: DecimalU64::new(parse_canonical_u64(&current)?),
                total_bytes: Some(DecimalU64::new(parse_canonical_u64(&total)?)),
            }),
            _ => return Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
        };
        let error = match (error_code, error_message) {
            (None, None) => None,
            (Some(code), Some(message)) => Some(V2PublicError {
                code: parse_closed_string::<V2OperationErrorCode>(&code)?,
                message,
            }),
            _ => return Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
        };
        let operation = V2Operation {
            operation_id,
            node_id,
            kind,
            status,
            slot_id: (kind != V2OperationKind::Download).then_some(stored_slot_id),
            model_id,
            progress,
            error,
            created_revision: DecimalU64::new(created),
            updated_revision: DecimalU64::new(updated),
            created_at_unix_ms: DecimalU64::new(created_at),
            updated_at_unix_ms: DecimalU64::new(updated_at),
        };
        if stored_slot_id != slot_id
            || v1_ordinal.is_some_and(|ordinal| ordinal < 1)
            || operation.validate().is_err()
            || created > updated
            || updated > revision
            || created_at > updated_at
            || updated_at > last_committed_at
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
    }
    Ok(())
}

fn validate_event_rows(
    connection: &Connection,
    node_id: NodeId,
    slot_id: SlotId,
    epoch: StreamEpoch,
    revision: u64,
    cursor: u64,
    last_committed_at: u64,
) -> Result<(), RepositoryError> {
    let mut statement = connection.prepare(
        "SELECT event_id, stream_epoch, sequence, revision, node_instance_id, v1_sequence, event_kind, payload_json FROM events",
    )?;
    let mut rows = statement.query([])?;
    let mut highest = 0_u64;
    let mut sequences = BTreeSet::new();
    while let Some(row) = rows.next()? {
        EventId::from_str(&row.get::<_, String>(0)?)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        let row_epoch = StreamEpoch::from_str(&row.get::<_, String>(1)?)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        let sequence = parse_canonical_u64(&row.get::<_, String>(2)?)?;
        let event_revision = parse_canonical_u64(&row.get::<_, String>(3)?)?;
        let node_instance: Option<String> = row.get(4)?;
        let parsed_node_instance = node_instance
            .as_deref()
            .map(NodeInstanceId::from_str)
            .transpose()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
        let v1_sequence: Option<i64> = row.get(5)?;
        let event_kind: String = row.get(6)?;
        let payload_json: String = row.get(7)?;
        validate_event_payload(
            &event_kind,
            &payload_json,
            node_id,
            slot_id,
            parsed_node_instance,
            event_revision,
            last_committed_at,
        )?;
        if row_epoch != epoch
            || sequence == 0
            || !sequences.insert(sequence)
            || event_revision > revision
            || v1_sequence.is_some_and(|value| value < 1)
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        highest = highest.max(sequence);
    }
    if highest != cursor {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn parse_closed_string<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, RepositoryError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))
}

fn validate_event_payload(
    event_kind: &str,
    payload_json: &str,
    node_id: NodeId,
    slot_id: SlotId,
    node_instance_id: Option<NodeInstanceId>,
    event_revision: u64,
    last_committed_at: u64,
) -> Result<(), RepositoryError> {
    match event_kind {
        "initialized" | "slot_changed" => {
            let slot: V2Slot = serde_json::from_str(payload_json)
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
            if slot.node_id != node_id || slot.slot_id != slot_id {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
        }
        "node_changed" => {
            let node: V2Node = serde_json::from_str(payload_json)
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
            if node.node_id != node_id || Some(node.node_instance_id) != node_instance_id {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
        }
        "operation_changed" => {
            let operation: V2Operation = serde_json::from_str(payload_json)
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
            if operation.node_id != node_id
                || operation.updated_revision.get() != event_revision
                || operation.updated_at_unix_ms.get() > last_committed_at
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
        }
        _ => return Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
    Ok(())
}

fn valid_bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_model_id(value: &str) -> bool {
    valid_bounded_text(value, 256)
}

fn valid_control_endpoint(value: &str) -> bool {
    if !value.is_ascii() || value.len() > 256 || value.contains(['@', '?', '#']) {
        return false;
    }
    let Some(authority) = value.strip_prefix("http://") else {
        return false;
    };
    let port = authority
        .strip_prefix("127.0.0.1:")
        .or_else(|| authority.strip_prefix("localhost:"))
        .or_else(|| authority.strip_prefix("[::1]:"));
    port.is_some_and(|port| {
        !port.is_empty()
            && port.bytes().all(|byte| byte.is_ascii_digit())
            && port.parse::<u16>().is_ok_and(|port| port != 0)
    })
}

fn rotate_lineage(path: &Path, node_id: NodeId) -> Result<(), RepositoryError> {
    let prepared = prepare_existing_storage_path(path)?;
    let mut connection = open_validated_connection_after(&prepared, false, || {})?;
    configure_for_offline_mutation(&connection)?;
    let summary = validate_connection(&connection, Some(node_id))?;
    let next_revision = summary
        .revision
        .checked_add(1)
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Overflow))?;
    let new_epoch = StreamEpoch::new_v4();
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute("DELETE FROM events", [])?;
    transaction.execute(
        "UPDATE control_meta SET stream_epoch = ?1, revision = ?2, cursor = '1' WHERE singleton = 1",
        (new_epoch.to_string(), next_revision.to_string()),
    )?;
    transaction.execute(
        "INSERT INTO events(event_id, stream_epoch, sequence, revision, node_instance_id, v1_sequence, event_kind, payload_json) VALUES(?1, ?2, '1', ?3, NULL, NULL, 'initialized', ?4)",
        (
            EventId::new_v4().to_string(),
            new_epoch.to_string(),
            next_revision.to_string(),
            unloaded_slot_payload(node_id, summary.slot_id)?,
        ),
    )?;
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(connection);
    validate_database_file(path, Some(node_id))?;
    Ok(())
}

fn unloaded_slot_payload(node_id: NodeId, slot_id: SlotId) -> Result<String, RepositoryError> {
    serde_json::to_string(&V2Slot {
        slot_id,
        node_id,
        name: "default".to_owned(),
        status: loxa_protocol::v2::V2SlotStatus::Unloaded,
        model_id: None,
        operation_id: None,
        error: None,
    })
    .map_err(|_| RepositoryError::new(RepositoryErrorClass::Database))
}

fn quick_check(connection: &Connection) -> Result<(), RepositoryError> {
    let result: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if result != "ok" {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn count_rows(connection: &Connection, table: &'static str) -> Result<usize, RepositoryError> {
    let sql = match table {
        "node_state" => "SELECT COUNT(*) FROM node_state",
        "slot_state" => "SELECT COUNT(*) FROM slot_state",
        "events" => "SELECT COUNT(*) FROM events",
        "loxa_schema_migrations" => "SELECT COUNT(*) FROM loxa_schema_migrations",
        _ => return Err(RepositoryError::new(RepositoryErrorClass::Database)),
    };
    let count: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    usize::try_from(count).map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))
}

fn parse_canonical_u64(value: &str) -> Result<u64, RepositoryError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    value
        .parse()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))
}

fn configure_defensively(connection: &Connection) -> Result<(), RepositoryError> {
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON;
         PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         PRAGMA busy_timeout=2000;
         PRAGMA trusted_schema=OFF;
         PRAGMA secure_delete=ON;
         PRAGMA temp_store=MEMORY;
         PRAGMA mmap_size=0;",
    )?;
    apply_limits(connection)?;
    let foreign_keys: i64 = connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    let defensive = connection.db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)?;
    if foreign_keys != 1 || !defensive {
        return Err(RepositoryError::new(RepositoryErrorClass::Database));
    }
    Ok(())
}

fn configure_for_offline_mutation(connection: &Connection) -> Result<(), RepositoryError> {
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON;
         PRAGMA synchronous=FULL;
         PRAGMA busy_timeout=2000;
         PRAGMA trusted_schema=OFF;
         PRAGMA secure_delete=ON;
         PRAGMA temp_store=MEMORY;
         PRAGMA mmap_size=0;",
    )?;
    apply_limits(connection)
}

fn apply_limits(connection: &Connection) -> Result<(), RepositoryError> {
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_LENGTH, SQLITE_LENGTH_LIMIT),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, SQLITE_SQL_LENGTH_LIMIT),
        (Limit::SQLITE_LIMIT_COLUMN, SQLITE_COLUMN_LIMIT),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, SQLITE_EXPR_DEPTH_LIMIT),
        (
            Limit::SQLITE_LIMIT_COMPOUND_SELECT,
            SQLITE_COMPOUND_SELECT_LIMIT,
        ),
        (Limit::SQLITE_LIMIT_VDBE_OP, SQLITE_VDBE_OP_LIMIT),
        (Limit::SQLITE_LIMIT_FUNCTION_ARG, SQLITE_FUNCTION_ARG_LIMIT),
        (Limit::SQLITE_LIMIT_ATTACHED, SQLITE_ATTACHED_LIMIT),
        (
            Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
            SQLITE_LIKE_PATTERN_LIMIT,
        ),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, SQLITE_VARIABLE_LIMIT),
        (
            Limit::SQLITE_LIMIT_TRIGGER_DEPTH,
            SQLITE_TRIGGER_DEPTH_LIMIT,
        ),
        (
            Limit::SQLITE_LIMIT_WORKER_THREADS,
            SQLITE_WORKER_THREADS_LIMIT,
        ),
    ] {
        connection.set_limit(limit, value)?;
    }
    Ok(())
}

fn validate_database_file(
    path: &Path,
    expected_node_id: Option<NodeId>,
) -> Result<ValidationSummary, RepositoryError> {
    let prepared = prepare_existing_storage_path(path)?;
    validate_auxiliary_files(path)?;
    let connection = open_validated_connection_after(&prepared, true, || {})?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
    apply_limits(&connection)?;
    let summary = validate_connection(&connection, expected_node_id)?;
    Ok(summary)
}

fn copy_database(source: &Path, destination: &Path) -> Result<(), RepositoryError> {
    let source_storage = prepare_existing_storage_path(source)?;
    let source_connection = open_validated_connection_after(&source_storage, true, || {})?;
    backup_connection(&source_connection, destination)?;
    Ok(())
}

fn backup_connection(source: &Connection, destination: &Path) -> Result<(), RepositoryError> {
    let file = create_private_file(destination)?;
    drop(file);
    let destination_storage = prepare_existing_storage_path(destination)?;
    let mut destination_connection =
        open_validated_connection_after(&destination_storage, false, || {})?;
    {
        let backup = Backup::new(source, &mut destination_connection)?;
        backup.run_to_completion(128, Duration::from_millis(1), None)?;
    }
    destination_connection.execute_batch("PRAGMA journal_mode=DELETE;")?;
    drop(destination_connection);
    sync_private_file(destination)
}

fn connection_flags(read_only: bool) -> OpenFlags {
    if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NOFOLLOW
            | OpenFlags::SQLITE_OPEN_EXRESCODE
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NOFOLLOW
            | OpenFlags::SQLITE_OPEN_EXRESCODE
    }
}

#[cfg(unix)]
fn open_validated_connection_after(
    prepared: &PreparedStorage,
    read_only: bool,
    after_validation: impl FnOnce(),
) -> Result<ValidatedConnection, RepositoryError> {
    use std::os::fd::AsRawFd;
    validate_file_metadata(
        &prepared
            .main_guard
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    after_validation();
    let registration = descriptor_vfs::Registration::new(prepared.main_guard.as_raw_fd())
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Database))?;
    let connection = Connection::open_with_flags_and_vfs(
        &prepared.path,
        connection_flags(read_only),
        registration.name(),
    )
    .map_err(map_sql_error)?;
    Ok(ValidatedConnection {
        connection,
        _registration: registration,
    })
}

#[cfg(not(unix))]
fn open_validated_connection_after(
    prepared: &PreparedStorage,
    read_only: bool,
    after_validation: impl FnOnce(),
) -> Result<ValidatedConnection, RepositoryError> {
    after_validation();
    let connection = Connection::open_with_flags(&prepared.path, connection_flags(read_only))
        .map_err(map_sql_error)?;
    Ok(ValidatedConnection { connection })
}

fn migration_backup_path(path: &Path) -> Result<PathBuf, RepositoryError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_os_string();
    name.push(".pre-migration.bak");
    Ok(path.with_file_name(name))
}

fn auxiliary_path(path: &Path, suffix: &str) -> Result<PathBuf, RepositoryError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_os_string();
    name.push(suffix);
    Ok(path.with_file_name(name))
}

fn unique_temporary_path(path: &Path, label: &str) -> Result<PathBuf, RepositoryError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_os_string();
    name.push(format!(".{label}-{}.tmp", StreamEpoch::new_v4()));
    Ok(path.with_file_name(name))
}

fn validate_auxiliary_files(path: &Path) -> Result<(), RepositoryError> {
    for suffix in ["-wal", "-shm"] {
        validate_optional_private_file(&auxiliary_path(path, suffix)?, None)?;
    }
    Ok(())
}

fn ensure_auxiliary_files_absent(path: &Path) -> Result<(), RepositoryError> {
    for suffix in ["-wal", "-shm"] {
        if validate_optional_private_file(&auxiliary_path(path, suffix)?, None)?.is_some() {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn prepare_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    path.file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard = open_secure_directory(parent, true)?;
    let (main_guard, _created) = open_or_create_private_file(path)?;
    Ok(PreparedStorage {
        path: path.to_owned(),
        directory_guard,
        main_guard,
    })
}

#[cfg(not(unix))]
fn prepare_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    fs::create_dir_all(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let (_file, _created) = open_or_create_private_file(path)?;
    Ok(PreparedStorage {
        path: path.to_owned(),
    })
}

#[cfg(unix)]
fn prepare_existing_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard = open_secure_directory(parent, false)?;
    let main_guard = validate_optional_private_file(path, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    Ok(PreparedStorage {
        path: path.to_owned(),
        directory_guard,
        main_guard,
    })
}

#[cfg(not(unix))]
fn prepare_existing_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    validate_optional_private_file(path, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    Ok(PreparedStorage {
        path: path.to_owned(),
    })
}

fn prepare_destination_parent(path: &Path) -> Result<PathBuf, RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    #[cfg(unix)]
    drop(open_secure_directory(parent, true)?);
    #[cfg(not(unix))]
    fs::create_dir_all(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    Ok(parent.to_owned())
}

#[cfg(unix)]
fn open_secure_directory(path: &Path, create: bool) -> Result<fs::File, RepositoryError> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let open = || {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options.open(path)
    };
    let directory = match open() {
        Ok(directory) => directory,
        Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
            }
            open().map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        }
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    };
    validate_directory_metadata(
        &directory
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    Ok(directory)
}

fn open_or_create_private_file(path: &Path) -> Result<(fs::File, bool), RepositoryError> {
    if let Some(file) = validate_optional_private_file(path, None)? {
        return Ok((file, false));
    }
    match create_private_file(path) {
        Ok(file) => {
            file.sync_all()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
            let metadata = file
                .metadata()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            let guard = validate_optional_private_file(path, Some(&metadata))?
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            Ok((guard, true))
        }
        Err(error) if error.class() == RepositoryErrorClass::UnsafePath => {
            let file = validate_optional_private_file(path, None)?.ok_or(error)?;
            Ok((file, false))
        }
        Err(error) => Err(error),
    }
}

fn create_private_file(path: &Path) -> Result<fs::File, RepositoryError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(
        &file
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    Ok(file)
}

fn validate_optional_private_file(
    path: &Path,
    expected: Option<&fs::Metadata>,
) -> Result<Option<fs::File>, RepositoryError> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    };
    validate_file_metadata(&before)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let opened = file
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let after = fs::symlink_metadata(path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(&opened)?;
    validate_file_metadata(&after)?;
    if !same_file_identity(&before, &opened)
        || !same_file_identity(&opened, &after)
        || expected.is_some_and(|expected| !same_file_identity(expected, &opened))
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(Some(file))
}

#[cfg(unix)]
fn validate_directory_metadata(metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(RepositoryError::new(RepositoryErrorClass::UnsafePath))
    }
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

fn sync_private_file(path: &Path) -> Result<(), RepositoryError> {
    let file = validate_optional_private_file(path, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    file.sync_all()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))
}

fn digest_private_file(path: &Path) -> Result<[u8; 32], RepositoryError> {
    use sha2::{Digest, Sha256};
    let mut file = validate_optional_private_file(path, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn atomic_replace(source: &Path, destination: &Path) -> Result<(), RepositoryError> {
    atomic_replace_after(source, destination, || {})
}

fn atomic_replace_after(
    source: &Path,
    destination: &Path,
    before_rename: impl FnOnce(),
) -> Result<(), RepositoryError> {
    let source_guard = validate_optional_private_file(source, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let source_identity = source_guard
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_optional_private_file(source, Some(&source_identity))?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_optional_private_file(destination, None)?;
    before_rename();
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;
        let source_parent = source
            .parent()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        if destination.parent() != Some(source_parent) {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
        let directory = open_secure_directory(source_parent, false)?;
        let source_name = std::ffi::CString::new(
            source
                .file_name()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
                .as_bytes(),
        )
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        let destination_name = std::ffi::CString::new(
            destination
                .file_name()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
                .as_bytes(),
        )
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        if unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                source_name.as_ptr(),
                directory.as_raw_fd(),
                destination_name.as_ptr(),
            )
        } != 0
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
    }
    #[cfg(not(unix))]
    fs::rename(source, destination)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    validate_optional_private_file(destination, Some(&source_identity))?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    Ok(())
}

fn sync_directory(parent: &Path) -> Result<(), RepositoryError> {
    sync_directory_path(parent)
}

fn sync_directory_path(parent: &Path) -> Result<(), RepositoryError> {
    #[cfg(unix)]
    {
        let directory = open_secure_directory(parent, false)?;
        directory
            .sync_all()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        Ok(())
    }
}

fn current_unix_ms() -> Result<i64, RepositoryError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Database))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Overflow))
}

fn classify_missing_row(error: SqlError) -> RepositoryError {
    match error {
        SqlError::QueryReturnedNoRows => RepositoryError::new(RepositoryErrorClass::Corrupt),
        other => map_sql_error(other),
    }
}

fn map_sql_error(error: SqlError) -> RepositoryError {
    match error {
        SqlError::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase,
                ..
            },
            _,
        ) => RepositoryError::new(RepositoryErrorClass::Corrupt),
        SqlError::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::ReadOnly | ErrorCode::PermissionDenied,
                ..
            },
            _,
        ) => RepositoryError::new(RepositoryErrorClass::UnsafePath),
        _ => RepositoryError::new(RepositoryErrorClass::Database),
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlIdGenerator, ControlRepository, RepositoryError, RepositoryErrorClass};
    use loxa_protocol::v2::{SlotId, StreamEpoch};
    use loxa_protocol::NodeId;
    use rusqlite::{config::DbConfig, limits::Limit};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    const NODE_ID: &str = "11111111-1111-4111-8111-111111111111";
    const SLOT_ID: &str = "22222222-2222-4222-8222-222222222222";
    const STREAM_EPOCH: &str = "33333333-3333-4333-8333-333333333333";

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let unique = format!(
                "loxa-control-state-{label}-{}-{}",
                std::process::id(),
                StreamEpoch::new_v4()
            );
            let path = std::env::temp_dir().join(unique);
            fs::create_dir(&path).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            }
            // macOS exposes `/var` as a symlink to `/private/var`; SQLite's
            // NOFOLLOW flag intentionally rejects that alias. Exercise the
            // repository through the descriptor-equivalent canonical path.
            Self(fs::canonicalize(path).unwrap())
        }

        fn database(&self) -> PathBuf {
            self.0.join("control-state.sqlite3")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct CountingIds {
        slot_id: Option<SlotId>,
        stream_epoch: Option<StreamEpoch>,
        calls: usize,
    }

    impl CountingIds {
        fn fixed() -> Self {
            Self {
                slot_id: Some(SlotId::from_str(SLOT_ID).unwrap()),
                stream_epoch: Some(StreamEpoch::from_str(STREAM_EPOCH).unwrap()),
                calls: 0,
            }
        }

        fn calls(&self) -> usize {
            self.calls
        }
    }

    impl ControlIdGenerator for CountingIds {
        fn new_slot_id(&mut self) -> SlotId {
            self.calls += 1;
            self.slot_id.take().expect("unexpected slot ID generation")
        }

        fn new_stream_epoch(&mut self) -> StreamEpoch {
            self.calls += 1;
            self.stream_epoch
                .take()
                .expect("unexpected stream epoch generation")
        }
    }

    fn node_id() -> NodeId {
        NodeId::from_str(NODE_ID).unwrap()
    }

    fn create_repository(path: &Path) -> ControlRepository {
        ControlRepository::open_or_create(path, node_id(), &mut CountingIds::fixed()).unwrap()
    }

    #[test]
    fn new_repository_has_exact_singleton_node_and_default_slot() {
        let directory = TestDirectory::new("singletons");
        let repository = create_repository(&directory.database());
        let summary = repository.validate_all().unwrap();
        assert_eq!(summary.node_rows, 1);
        assert_eq!(summary.slot_rows, 1);
        assert_eq!(summary.slot_name, "default");
        assert_eq!(summary.revision, 1);
        assert_eq!(summary.cursor, 1);
        assert_eq!(summary.event_rows, 1);
    }

    #[test]
    fn repository_enforces_required_pragmas_defensive_mode_and_limits() {
        let directory = TestDirectory::new("defensive-configuration");
        let repository = create_repository(&directory.database());
        let pragma = |sql| {
            repository
                .connection
                .query_row(sql, [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        let journal_mode: String = repository
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        assert_eq!(pragma("PRAGMA foreign_keys"), 1);
        assert_eq!(pragma("PRAGMA synchronous"), 2);
        assert!((1..=2_000).contains(&pragma("PRAGMA busy_timeout")));
        assert_eq!(pragma("PRAGMA trusted_schema"), 0);
        assert_eq!(pragma("PRAGMA secure_delete"), 1);
        assert_eq!(pragma("PRAGMA temp_store"), 2);
        assert_eq!(pragma("PRAGMA mmap_size"), 0);
        assert!(repository
            .connection
            .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
            .unwrap());
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_LENGTH)
                .unwrap(),
            super::SQLITE_LENGTH_LIMIT
        );
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_ATTACHED)
                .unwrap(),
            super::SQLITE_ATTACHED_LIMIT
        );
        assert_eq!(
            repository
                .connection
                .limit(Limit::SQLITE_LIMIT_WORKER_THREADS)
                .unwrap(),
            super::SQLITE_WORKER_THREADS_LIMIT
        );
    }

    #[test]
    fn existing_repository_loads_ids_without_generating_replacements() {
        let directory = TestDirectory::new("persisted-ids");
        let path = directory.database();
        drop(create_repository(&path));

        let mut ids = CountingIds::default();
        let repository = ControlRepository::open_or_create(&path, node_id(), &mut ids).unwrap();
        assert_eq!(ids.calls(), 0);
        assert_eq!(repository.slot_id().to_string(), SLOT_ID);
        assert_eq!(repository.stream_epoch().to_string(), STREAM_EPOCH);
    }

    #[test]
    fn existing_repository_rejects_a_different_stable_node_without_generating_ids() {
        let directory = TestDirectory::new("node-mismatch");
        let path = directory.database();
        drop(create_repository(&path));
        let different_node = NodeId::from_str("66666666-6666-4666-8666-666666666666").unwrap();
        let mut ids = CountingIds::default();
        let error = ControlRepository::open_or_create(&path, different_node, &mut ids).unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::IdentityMismatch);
        assert_eq!(ids.calls(), 0);
    }

    #[test]
    fn new_repository_generates_slot_and_epoch_inside_initial_transaction() {
        let directory = TestDirectory::new("new-ids");
        let mut ids = CountingIds::fixed();
        let repository =
            ControlRepository::open_or_create(&directory.database(), node_id(), &mut ids).unwrap();
        assert_eq!(ids.calls(), 2);
        assert_eq!(repository.slot_id().to_string(), SLOT_ID);
        assert_eq!(repository.stream_epoch().to_string(), STREAM_EPOCH);
    }

    #[test]
    fn rejects_a_changed_migration_checksum_without_mutation() {
        let directory = TestDirectory::new("ledger-checksum");
        let path = directory.database();
        let mut repository = create_repository(&path);
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE loxa_schema_migrations SET checksum = 'modified' WHERE version = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        drop(repository);

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
    }

    #[test]
    fn rejects_changed_schema_shape_even_with_an_unchanged_ledger() {
        let directory = TestDirectory::new("schema-shape");
        let path = directory.database();
        let mut repository = create_repository(&path);
        repository
            .transaction(|transaction| {
                transaction.execute("DROP INDEX one_active_lifecycle_operation", [])?;
                Ok(())
            })
            .unwrap();
        drop(repository);

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsupportedSchema);
    }

    #[cfg(unix)]
    #[test]
    fn repository_rejects_symlink_hardlink_and_broad_permissions() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let broad_directory = TestDirectory::new("broad-directory");
        fs::set_permissions(&broad_directory.0, fs::Permissions::from_mode(0o755)).unwrap();
        let error = ControlRepository::open_or_create(
            &broad_directory.database(),
            node_id(),
            &mut CountingIds::fixed(),
        )
        .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);

        let broad_file = TestDirectory::new("broad-file");
        fs::write(broad_file.database(), []).unwrap();
        fs::set_permissions(broad_file.database(), fs::Permissions::from_mode(0o644)).unwrap();
        let error = ControlRepository::open_or_create(
            &broad_file.database(),
            node_id(),
            &mut CountingIds::fixed(),
        )
        .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);

        let symlinked = TestDirectory::new("symlink");
        let target = symlinked.0.join("target.sqlite3");
        fs::write(&target, []).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, symlinked.database()).unwrap();
        let error = ControlRepository::open_or_create(
            &symlinked.database(),
            node_id(),
            &mut CountingIds::fixed(),
        )
        .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);

        let hardlinked = TestDirectory::new("hardlink");
        let path = hardlinked.database();
        drop(create_repository(&path));
        fs::hard_link(&path, hardlinked.0.join("second-link.sqlite3")).unwrap();
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
    }

    #[test]
    fn fresh_repository_publishes_a_validated_migration_backup() {
        let directory = TestDirectory::new("initial-backup");
        let repository = create_repository(&directory.database());
        let backup = repository.migration_backup_path().unwrap();
        assert!(backup.exists());
        assert_eq!(repository.validate_backup(&backup).unwrap().revision, 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(backup).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn offline_restore_rotates_epoch_clears_events_and_advances_revision() {
        let source = TestDirectory::new("restore-source");
        let mut repository = create_repository(&source.database());
        let restored_payload =
            super::unloaded_slot_payload(node_id(), repository.slot_id()).unwrap();
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '41', cursor = '41' WHERE singleton = 1",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO events(event_id, stream_epoch, sequence, revision, event_kind, payload_json) VALUES(?1, ?2, '41', '41', 'slot_changed', ?3)",
                    ["44444444-4444-4444-8444-444444444444", STREAM_EPOCH, &restored_payload],
                )?;
                Ok(())
            })
            .unwrap();
        let backup = repository.backup_before_migration().unwrap();
        let old_epoch = repository.stream_epoch();
        drop(repository);

        let destination = TestDirectory::new("restore-destination");
        let restored =
            ControlRepository::restore_offline(&backup, &destination.database()).unwrap();
        assert_ne!(restored.epoch, old_epoch);
        assert_eq!(restored.cursor, 1);
        assert_eq!(restored.revision, 42);
        assert_eq!(restored.event_rows, 1);
    }

    #[test]
    fn failed_migration_restores_only_the_verified_same_boundary_backup() {
        let directory = TestDirectory::new("migration-rollback");
        let path = directory.database();
        let mut repository = create_repository(&path);
        let original_epoch = repository.stream_epoch();
        let backup = repository.backup_before_migration().unwrap();
        let proof = repository.migration_rollback_proof().unwrap();
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '9' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        drop(repository);

        ControlRepository::restore_verified_migration_backup(&backup, &path, &proof).unwrap();
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        assert_eq!(reopened.stream_epoch(), original_epoch);
        assert_eq!(reopened.validate_all().unwrap().revision, 1);
    }

    #[test]
    fn failed_migration_rejects_a_substituted_valid_sibling_backup() {
        let original = TestDirectory::new("migration-original");
        let original_path = original.database();
        let repository = create_repository(&original_path);
        let retained = repository.backup_before_migration().unwrap();
        let proof = repository.migration_rollback_proof().unwrap();

        let sibling = TestDirectory::new("migration-sibling");
        let sibling_repository = create_repository(&sibling.database());
        let sibling_backup = sibling_repository.backup_before_migration().unwrap();
        drop(sibling_repository);
        fs::copy(sibling_backup, &retained).unwrap();

        drop(repository);
        let error =
            ControlRepository::restore_verified_migration_backup(&retained, &original_path, &proof)
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_main_open_remains_bound_to_the_validated_descriptor_during_path_swap() {
        let directory = TestDirectory::new("descriptor-bound-open");
        let path = directory.database();
        drop(create_repository(&path));
        let replacement = directory.0.join("replacement.sqlite3");
        fs::write(&replacement, b"not the validated database").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();

        let prepared = super::prepare_existing_storage_path(&path).unwrap();
        let connection = super::open_validated_connection_after(&prepared, true, || {
            let displaced = directory.0.join("displaced.sqlite3");
            fs::rename(&path, displaced).unwrap();
            fs::rename(&replacement, &path).unwrap();
        })
        .unwrap();

        let summary = super::validate_connection(&connection, Some(node_id())).unwrap();
        assert_eq!(summary.node_id, node_id());
        assert!(!super::descriptor_vfs::open_override_is_installed());
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_open_preserves_sibling_wal_and_shm_and_reopens_after_checkpoint() {
        let directory = TestDirectory::new("descriptor-wal-reopen");
        let path = directory.database();
        let mut repository = create_repository(&path);
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '2' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(super::auxiliary_path(&path, "-wal").unwrap().exists());
        assert!(super::auxiliary_path(&path, "-shm").unwrap().exists());
        repository.checkpoint().unwrap();
        drop(repository);

        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        assert_eq!(reopened.validate_all().unwrap().revision, 2);
    }

    #[cfg(unix)]
    #[test]
    fn scoped_open_override_does_not_redirect_unrelated_sqlite_and_restores_on_unwind() {
        let unrelated = TestDirectory::new("unrelated-sqlite");
        let unrelated_path = unrelated.0.join("unrelated.sqlite3");
        super::descriptor_vfs::with_open_override_for_test(|| {
            let connection = rusqlite::Connection::open(&unrelated_path).unwrap();
            connection
                .execute_batch("CREATE TABLE independent(value INTEGER);")
                .unwrap();
        });
        assert!(!super::descriptor_vfs::open_override_is_installed());

        let panic = std::panic::catch_unwind(|| {
            super::descriptor_vfs::with_open_override_for_test(|| panic!("fault injection"));
        });
        assert!(panic.is_err());
        assert!(!super::descriptor_vfs::open_override_is_installed());
        let connection = rusqlite::Connection::open(&unrelated_path).unwrap();
        let rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM independent", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[cfg(unix)]
    #[test]
    fn atomic_replace_rejects_a_source_inode_swap_at_the_rename_boundary() {
        use std::os::unix::fs::PermissionsExt;
        let directory = TestDirectory::new("replace-source-swap");
        let source = directory.0.join("source.sqlite3");
        let replacement = directory.0.join("replacement.sqlite3");
        let destination = directory.0.join("destination.sqlite3");
        for (path, contents) in [
            (&source, b"validated".as_slice()),
            (&replacement, b"substituted".as_slice()),
            (&destination, b"old".as_slice()),
        ] {
            fs::write(path, contents).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let error = super::atomic_replace_after(&source, &destination, || {
            fs::rename(&source, directory.0.join("displaced.sqlite3")).unwrap();
            fs::rename(&replacement, &source).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
    }

    #[test]
    fn validate_all_rejects_every_logically_invalid_persisted_domain() {
        let corruptions = [
            "UPDATE node_state SET node_instance_id = '44444444-4444-4444-8444-44444444444A'",
            "UPDATE node_state SET node_instance_id = '44444444-4444-4444-8444-444444444444', control_endpoint = 'https://127.0.0.1:8080', status = 'running'",
            "UPDATE slot_state SET status = 'loading', model_id = ' bad ', operation_id = NULL",
            "UPDATE slot_state SET status = 'loading', operation_id = '55555555-5555-4555-8555-55555555555A'",
            "INSERT INTO operations(operation_id, slot_id, admitting_node_instance_id, kind, status, created_revision, updated_revision, created_at_unix_ms, updated_at_unix_ms) VALUES('55555555-5555-4555-8555-555555555555', '22222222-2222-4222-8222-222222222222', '44444444-4444-4444-8444-444444444444', 'cancel', 'succeeded', '1', '1', '1', '1')",
            "INSERT INTO operations(operation_id, slot_id, admitting_node_instance_id, kind, status, model_id, progress_current, progress_total, created_revision, updated_revision, created_at_unix_ms, updated_at_unix_ms) VALUES('55555555-5555-4555-8555-555555555555', '22222222-2222-4222-8222-222222222222', '44444444-4444-4444-8444-444444444444', 'download', 'running', 'model', '2', '1', '1', '1', '1', '1')",
            "INSERT INTO operations(operation_id, slot_id, admitting_node_instance_id, kind, status, error_code, error_message, created_revision, updated_revision, created_at_unix_ms, updated_at_unix_ms) VALUES('55555555-5555-4555-8555-555555555555', '22222222-2222-4222-8222-222222222222', '44444444-4444-4444-8444-444444444444', 'unload', 'failed', 'download_failed', 'bad kind correlation', '1', '1', '1', '1')",
            "UPDATE events SET payload_json = '{not-json' WHERE sequence = '1'",
            "UPDATE events SET event_kind = 'node_changed', node_instance_id = NULL WHERE sequence = '1'",
        ];

        for (index, sql) in corruptions.into_iter().enumerate() {
            let directory = TestDirectory::new(&format!("logical-corruption-{index}"));
            let mut repository = create_repository(&directory.database());
            repository
                .transaction(|transaction| {
                    transaction.execute(sql, [])?;
                    Ok(())
                })
                .unwrap();
            let error = repository.validate_all().unwrap_err();
            assert_eq!(error.class(), RepositoryErrorClass::Corrupt, "{sql}");
        }
    }

    #[test]
    fn corruption_is_refused_instead_of_silently_restored() {
        let directory = TestDirectory::new("corruption");
        let path = directory.database();
        drop(create_repository(&path));
        fs::write(&path, b"not a sqlite database").unwrap();

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
    }

    #[test]
    fn corrupt_backup_is_refused_without_replacing_the_destination() {
        let corrupt = TestDirectory::new("corrupt-backup");
        let backup = corrupt.0.join("control-state.sqlite3.pre-migration.bak");
        fs::write(&backup, b"not a sqlite database").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&backup, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let destination = TestDirectory::new("corrupt-backup-destination");
        let destination_path = destination.database();
        let original = create_repository(&destination_path);
        let original_epoch = original.stream_epoch();
        drop(original);

        let error = ControlRepository::restore_offline(&backup, &destination_path).unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
        let reopened = ControlRepository::open_or_create(
            &destination_path,
            node_id(),
            &mut CountingIds::default(),
        )
        .unwrap();
        assert_eq!(reopened.stream_epoch(), original_epoch);
    }

    #[test]
    fn schema_rejects_noncanonical_counters_inside_transactions() {
        let directory = TestDirectory::new("counter-checks");
        let mut repository = create_repository(&directory.database());
        let error = repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '01' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Database);
        assert_eq!(repository.validate_all().unwrap().revision, 1);
    }

    #[test]
    fn transaction_rolls_back_on_repository_error() {
        let directory = TestDirectory::new("transaction-rollback");
        let mut repository = create_repository(&directory.database());
        let result: Result<(), RepositoryError> = repository.transaction(|transaction| {
            transaction.execute(
                "UPDATE control_meta SET revision = '9' WHERE singleton = 1",
                [],
            )?;
            Err(RepositoryError::corrupt())
        });
        assert_eq!(result.unwrap_err().class(), RepositoryErrorClass::Corrupt);
        assert_eq!(repository.validate_all().unwrap().revision, 1);
    }
}
