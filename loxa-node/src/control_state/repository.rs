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
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
#[path = "test_support/mod.rs"]
mod test_support;

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
    AlreadyOwned,
    Corrupt,
    UnsupportedSchema,
    IdentityMismatch,
    Database,
    Durability,
    Overflow,
    UnsupportedPlatform,
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
            RepositoryErrorClass::AlreadyOwned => "control-state database already owned",
            RepositoryErrorClass::Corrupt => "corrupt control-state database",
            RepositoryErrorClass::UnsupportedSchema => "unsupported control-state schema",
            RepositoryErrorClass::IdentityMismatch => "control-state identity mismatch",
            RepositoryErrorClass::Database => "control-state database failure",
            RepositoryErrorClass::Durability => "control-state durability failure",
            RepositoryErrorClass::Overflow => "control-state counter overflow",
            RepositoryErrorClass::UnsupportedPlatform => {
                "control-state repository unsupported on this platform"
            }
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
    connection: Option<Connection>,
    path: PathBuf,
    identity: FileIdentity,
    expected_node_id: NodeId,
    slot_id: SlotId,
    stream_epoch: StreamEpoch,
    directory_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    live_claim: Option<LiveDatabaseClaim>,
}

struct ValidatedConnection {
    connection: Option<Connection>,
    directory_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    live_claim: Option<LiveDatabaseClaim>,
    identity: FileIdentity,
}

impl std::ops::Deref for ValidatedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("connection retained")
    }
}

impl std::ops::DerefMut for ValidatedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().expect("connection retained")
    }
}

impl ValidatedConnection {
    fn close(mut self) -> Result<(), RepositoryError> {
        let connection = self
            .connection
            .take()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match connection.close() {
            Ok(()) => {
                drop(self.main_guard.take());
                drop(self.directory_guard.take());
                self.live_claim
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
                    .release_after_proven_close()
            }
            Err((connection, error)) => {
                self.connection = Some(connection);
                let mapped = map_sql_error(error);
                Err(quarantine_validated_connection(
                    &mut self,
                    mapped,
                    CloseUncertainty::CheckpointOrClose,
                ))
            }
        }
    }

    fn into_repository_parts(
        mut self,
    ) -> (
        Connection,
        fs::File,
        fs::File,
        LiveDatabaseClaim,
        FileIdentity,
    ) {
        (
            self.connection.take().expect("connection retained"),
            self.directory_guard
                .take()
                .expect("directory guard retained"),
            self.main_guard.take().expect("main guard retained"),
            self.live_claim.take().expect("live claim retained"),
            self.identity,
        )
    }
}

impl Drop for ValidatedConnection {
    fn drop(&mut self) {
        if self.connection.is_some() {
            let _ = quarantine_validated_connection(
                self,
                RepositoryError::new(RepositoryErrorClass::Durability),
                CloseUncertainty::ImplicitDrop,
            );
        }
    }
}

fn retain_poisoned_owner(
    connection: Option<Connection>,
    main_guard: Option<fs::File>,
    directory_guard: Option<fs::File>,
    mut claim: LiveDatabaseClaim,
    reason: CloseUncertainty,
    error: RepositoryError,
) -> RepositoryError {
    let _ = claim.poison(reason);
    let owner = PoisonedDatabaseOwner {
        connection,
        main_guard,
        directory_guard,
        claim,
    };
    let _retained_until_exit: &'static mut PoisonedDatabaseOwner = Box::leak(Box::new(owner));
    error
}

fn retain_quarantined_owner(
    connection: Connection,
    main_guard: fs::File,
    directory_guard: fs::File,
    reservation: ClaimReservation,
    error: RepositoryError,
) -> RepositoryError {
    let owner = PoisonedReservationOwner {
        connection,
        main_guard,
        directory_guard,
        reservation,
    };
    let _retained_until_exit: &'static mut PoisonedReservationOwner = Box::leak(Box::new(owner));
    error
}

fn quarantine_validated_connection(
    owner: &mut ValidatedConnection,
    error: RepositoryError,
    reason: CloseUncertainty,
) -> RepositoryError {
    let Some(claim) = owner.live_claim.take() else {
        return error;
    };
    retain_poisoned_owner(
        owner.connection.take(),
        owner.main_guard.take(),
        owner.directory_guard.take(),
        claim,
        reason,
        error,
    )
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
    canonical_path: PathBuf,
    canonical_parent: PathBuf,
    identity: FileIdentity,
    directory_identity: FileIdentity,
    directory_guard: fs::File,
    main_guard: fs::File,
    reservation: ClaimReservation,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaimToken(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseUncertainty {
    CheckpointOrClose,
    ImplicitDrop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseEvent {
    Checkpoint,
    SqliteClosed,
    MainGuardClosed,
    DirectoryGuardClosed,
    ClaimReleased,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseFault {
    None,
    #[cfg(test)]
    Checkpoint,
    #[cfg(test)]
    ReturnedConnection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimState {
    Quarantined { token: ClaimToken },
    Live { token: ClaimToken },
    Poisoned { reason: CloseUncertainty },
}

struct ClaimReservation {
    identity: FileIdentity,
    token: ClaimToken,
    active: bool,
}

struct LiveDatabaseClaim {
    identity: FileIdentity,
    token: ClaimToken,
}

struct ConnectionOpenSpec {
    canonical_path: PathBuf,
    flags: OpenFlags,
    vfs: &'static str,
}

struct SqliteOwnedText(*mut std::os::raw::c_char);

impl Drop for SqliteOwnedText {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { rusqlite::ffi::sqlite3_free(self.0.cast()) };
        }
    }
}

struct PoisonedDatabaseOwner {
    connection: Option<Connection>,
    main_guard: Option<fs::File>,
    directory_guard: Option<fs::File>,
    claim: LiveDatabaseClaim,
}

struct PoisonedReservationOwner {
    connection: Connection,
    main_guard: fs::File,
    directory_guard: fs::File,
    reservation: ClaimReservation,
}

static NEXT_CLAIM_TOKEN: AtomicU64 = AtomicU64::new(1);
static DATABASE_CLAIMS: OnceLock<Mutex<BTreeMap<FileIdentity, ClaimState>>> = OnceLock::new();
#[cfg(test)]
thread_local! {
    static FAIL_CLAIM_TRANSITIONS: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static FAIL_NEXT_UNCOMMITTED_CLOSE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoragePreflight {
    Production,
    #[cfg(test)]
    UnsupportedPlatform,
    #[cfg(test)]
    MissingUnixExcl,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenBoundarySwap {
    Parent,
    Main,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenTrace {
    PlatformPreflight,
    VfsPreflight,
    PathIdentityWithoutMainFd,
    NewFileCreatedWithMainGuard,
    ClaimQuarantined,
    MainGuardOpened,
    SqliteOpened,
    PostOpenValidated,
    ClaimLive,
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

fn claims() -> &'static Mutex<BTreeMap<FileIdentity, ClaimState>> {
    DATABASE_CLAIMS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

impl ClaimReservation {
    fn reserve(identity: FileIdentity) -> Result<Self, RepositoryError> {
        let token = ClaimToken(NEXT_CLAIM_TOKEN.fetch_add(1, Ordering::Relaxed));
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        if claims.contains_key(&identity) {
            return Err(RepositoryError::new(RepositoryErrorClass::AlreadyOwned));
        }
        claims.insert(identity, ClaimState::Quarantined { token });
        Ok(Self {
            identity,
            token,
            active: true,
        })
    }

    fn transition_to_live(&mut self) -> Result<LiveDatabaseClaim, RepositoryError> {
        #[cfg(test)]
        if FAIL_CLAIM_TRANSITIONS.with(|remaining| {
            let count = remaining.get();
            remaining.set(count.saturating_sub(1));
            count > 0
        }) {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match claims.get(&self.identity) {
            Some(ClaimState::Quarantined { token }) if *token == self.token => {}
            _ => return Err(RepositoryError::new(RepositoryErrorClass::Durability)),
        }
        claims.insert(self.identity, ClaimState::Live { token: self.token });
        self.active = false;
        Ok(LiveDatabaseClaim {
            identity: self.identity,
            token: self.token,
        })
    }
}

#[cfg(test)]
fn fail_next_claim_transition_for_test() {
    FAIL_CLAIM_TRANSITIONS.with(|remaining| remaining.set(1));
}

#[cfg(test)]
fn fail_next_uncommitted_close_and_two_claim_transitions_for_test() {
    FAIL_CLAIM_TRANSITIONS.with(|remaining| remaining.set(2));
    FAIL_NEXT_UNCOMMITTED_CLOSE.with(|fault| fault.set(true));
}

impl Drop for ClaimReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut claims) = claims().lock() {
            if matches!(
                claims.get(&self.identity),
                Some(ClaimState::Quarantined { token }) if *token == self.token
            ) {
                claims.remove(&self.identity);
            }
        }
    }
}

impl LiveDatabaseClaim {
    fn release_after_proven_close(self) -> Result<(), RepositoryError> {
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match claims.get(&self.identity) {
            Some(ClaimState::Live { token }) if *token == self.token => {
                claims.remove(&self.identity);
                Ok(())
            }
            _ => Err(RepositoryError::new(RepositoryErrorClass::Durability)),
        }
    }

    fn poison(&mut self, reason: CloseUncertainty) -> Result<(), RepositoryError> {
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match claims.get(&self.identity) {
            Some(ClaimState::Live { token }) if *token == self.token => {
                claims.insert(self.identity, ClaimState::Poisoned { reason });
                Ok(())
            }
            Some(ClaimState::Poisoned { .. }) => Ok(()),
            _ => Err(RepositoryError::new(RepositoryErrorClass::Durability)),
        }
    }
}

impl ConnectionOpenSpec {
    fn for_existing(canonical_path: PathBuf, read_only: bool) -> Result<Self, RepositoryError> {
        ensure_supported_storage_platform()?;
        ensure_unix_excl_vfs()?;
        Ok(Self {
            canonical_path,
            flags: connection_flags(read_only),
            vfs: "unix-excl",
        })
    }
}

fn ensure_supported_storage_platform() -> Result<(), RepositoryError> {
    if cfg!(any(target_os = "macos", target_os = "linux")) {
        Ok(())
    } else {
        Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedPlatform,
        ))
    }
}

fn ensure_unix_excl_vfs() -> Result<(), RepositoryError> {
    let name = CString::new("unix-excl")
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Database))?;
    if unsafe { rusqlite::ffi::sqlite3_initialize() } != rusqlite::ffi::SQLITE_OK {
        return Err(RepositoryError::new(RepositoryErrorClass::Database));
    }
    if unsafe { rusqlite::ffi::sqlite3_vfs_find(name.as_ptr()) }.is_null() {
        Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedPlatform,
        ))
    } else {
        Ok(())
    }
}

fn connected_vfs_name(connection: &Connection) -> Result<String, RepositoryError> {
    let mut name = std::ptr::null_mut::<std::os::raw::c_char>();
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_VFSNAME,
            (&mut name as *mut *mut std::os::raw::c_char).cast(),
        )
    };
    decode_sqlite_owned_vfs_name(rc, name)
}

fn decode_sqlite_owned_vfs_name(
    rc: std::os::raw::c_int,
    name: *mut std::os::raw::c_char,
) -> Result<String, RepositoryError> {
    let name = SqliteOwnedText(name);
    if rc != rusqlite::ffi::SQLITE_OK || name.0.is_null() {
        return Err(RepositoryError::new(RepositoryErrorClass::Database));
    }
    let bytes = unsafe { CStr::from_ptr(name.0) }.to_bytes().to_vec();
    String::from_utf8(bytes).map_err(|_| RepositoryError::new(RepositoryErrorClass::Database))
}

#[cfg(test)]
fn decode_non_ok_allocated_vfsname_for_test() -> Result<String, RepositoryError> {
    let bytes = b"unix-excl\0";
    let allocation = unsafe { rusqlite::ffi::sqlite3_malloc(bytes.len() as i32) }.cast::<u8>();
    assert!(!allocation.is_null());
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), allocation, bytes.len()) };
    decode_sqlite_owned_vfs_name(rusqlite::ffi::SQLITE_ERROR, allocation.cast())
}

#[cfg(test)]
fn open_and_query_connected_vfs(spec: ConnectionOpenSpec) -> Result<String, RepositoryError> {
    let connection = Connection::open_with_flags_and_vfs(spec.canonical_path, spec.flags, spec.vfs)
        .map_err(map_sql_error)?;
    let name = connected_vfs_name(&connection)?;
    connection
        .close()
        .map_err(|(_, error)| map_sql_error(error))?;
    Ok(name)
}

#[cfg(test)]
fn open_with_preflight_for_test(
    path: &Path,
    preflight: StoragePreflight,
) -> Result<(), RepositoryError> {
    let prepared = prepare_storage_path_traced_with_preflight(path, None, preflight)?;
    open_validated_connection_after(prepared, false, || {})?.close()
}

#[cfg(test)]
fn open_trace_for_test(path: &Path) -> Result<Vec<OpenTrace>, RepositoryError> {
    let mut trace = Vec::new();
    let prepared = prepare_storage_path_traced(path, Some(&mut trace))?;
    let connection = open_validated_connection_traced(prepared, false, || {}, Some(&mut trace))?;
    connection.close()?;
    Ok(trace)
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
fn open_with_boundary_swap_for_test(swap: OpenBoundarySwap) -> Result<(), RepositoryError> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    let root = test_support::storage::TestRoot::new("open-boundary");
    let path = root.path().join("control-state.sqlite3");
    fs::write(&path, b"").map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let prepared = prepare_existing_storage_path(&path)?;
    open_validated_connection_after(prepared, false, || match swap {
        OpenBoundarySwap::Main => {
            let displaced = root.path().join("displaced.sqlite3");
            fs::rename(&path, &displaced).expect("displace main at open boundary");
            fs::copy(&displaced, &path).expect("replace main at open boundary");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("secure replacement main");
        }
        OpenBoundarySwap::Parent => {
            let parent = root.path().to_owned();
            let displaced = parent.with_extension("displaced");
            fs::rename(&parent, &displaced).expect("displace parent at open boundary");
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(&parent)
                .expect("replace parent at open boundary");
            fs::copy(displaced.join("control-state.sqlite3"), &path)
                .expect("replace main under replacement parent");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("secure replacement main");
        }
    })?
    .close()
}

#[cfg(all(test, unix))]
fn prepare_with_parent_auxiliary_swap_for_test() -> Result<(), RepositoryError> {
    use std::os::unix::fs::DirBuilderExt;

    let root = test_support::storage::TestRoot::new("prepare-aux-boundary");
    let path = root.path().join("control-state.sqlite3");
    let canonical_parent = fs::canonicalize(root.path())
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let displaced = canonical_parent.with_extension("prepare-aux-boundary-displaced");
    let result = prepare_storage_path_traced_with_preflight_after_directory_bound(
        &path,
        None,
        StoragePreflight::Production,
        |bound_parent| {
            assert_eq!(bound_parent, canonical_parent);
            fs::rename(bound_parent, &displaced).expect("displace guarded parent");
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(bound_parent)
                .expect("create replacement parent");
            fs::create_dir(
                auxiliary_path(&bound_parent.join("control-state.sqlite3"), "-wal").unwrap(),
            )
            .expect("create unsafe replacement WAL");
        },
    )
    .map(drop);
    assert!(
        !path.exists(),
        "main created under unchecked replacement parent"
    );
    let _ = fs::remove_dir_all(&displaced);
    result
}

impl ControlRepository {
    pub(crate) fn open_or_create(
        path: &Path,
        node_id: NodeId,
        ids: &mut dyn ControlIdGenerator,
    ) -> Result<Self, RepositoryError> {
        ensure_supported_storage_platform()?;
        ensure_unix_excl_vfs()?;
        let prepared = prepare_storage_path(path)?;
        let canonical_path = prepared.canonical_path.clone();
        let mut opened = open_validated_connection_after(prepared, false, || {})?;
        let setup = (|| {
            configure_defensively(&opened)?;
            let initialized =
                open_existing_or_initialize_in_one_transaction(&mut opened, node_id, ids)?;
            let summary = validate_connection(&opened, Some(node_id))?;
            Ok((initialized, summary))
        })();
        let (initialized, summary) = match setup {
            Ok(value) => value,
            Err(error) => {
                return match opened.close() {
                    Ok(()) => Err(error),
                    Err(close_error) => Err(close_error),
                };
            }
        };
        let (connection, directory_guard, main_guard, live_claim, identity) =
            opened.into_repository_parts();
        let repository = Self {
            connection: Some(connection),
            path: canonical_path,
            identity,
            expected_node_id: node_id,
            slot_id: summary.slot_id,
            stream_epoch: summary.epoch,
            directory_guard: Some(directory_guard),
            main_guard: Some(main_guard),
            live_claim: Some(live_claim),
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
            .as_mut()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = work(&transaction)?;
        transaction.commit()?;
        Ok(value)
    }

    pub(crate) fn validate_all(&self) -> Result<ValidationSummary, RepositoryError> {
        validate_connection(self.connection_ref()?, Some(self.expected_node_id))
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
            self.connection_ref()?
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
            backup_connection(self.connection_ref()?, &temporary)?;
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

    fn connection_ref(&self) -> Result<&Connection, RepositoryError> {
        self.connection
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))
    }

    pub(crate) fn close(self) -> Result<(), RepositoryError> {
        self.close_inner(CloseFault::None, |_| {})
    }

    fn close_inner(
        mut self,
        fault: CloseFault,
        mut observe: impl FnMut(CloseEvent),
    ) -> Result<(), RepositoryError> {
        #[cfg(not(test))]
        let _ = fault;
        observe(CloseEvent::Checkpoint);
        #[cfg(test)]
        if fault == CloseFault::Checkpoint {
            let error = RepositoryError::new(RepositoryErrorClass::Durability);
            observe(CloseEvent::Poisoned);
            return Err(quarantine_repository_until_exit(
                &mut self,
                error,
                CloseUncertainty::CheckpointOrClose,
            ));
        }
        if let Err(error) = self.checkpoint() {
            observe(CloseEvent::Poisoned);
            return Err(quarantine_repository_until_exit(
                &mut self,
                error,
                CloseUncertainty::CheckpointOrClose,
            ));
        }
        let connection = self
            .connection
            .take()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
        #[cfg(test)]
        if fault == CloseFault::ReturnedConnection {
            let mut statement = std::ptr::null_mut();
            let rc = unsafe {
                rusqlite::ffi::sqlite3_prepare_v2(
                    connection.handle(),
                    c"SELECT 1".as_ptr(),
                    -1,
                    &mut statement,
                    std::ptr::null_mut(),
                )
            };
            assert_eq!(rc, rusqlite::ffi::SQLITE_OK);
            assert!(!statement.is_null());
            // Deliberately do not finalize: sqlite3_close must return BUSY and
            // rusqlite must return the still-live Connection for quarantine.
        }
        match connection.close() {
            Ok(()) => {
                observe(CloseEvent::SqliteClosed);
                drop(self.main_guard.take());
                observe(CloseEvent::MainGuardClosed);
                drop(self.directory_guard.take());
                observe(CloseEvent::DirectoryGuardClosed);
                self.live_claim
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
                    .release_after_proven_close()?;
                observe(CloseEvent::ClaimReleased);
                Ok(())
            }
            Err((connection, error)) => {
                self.connection = Some(connection);
                let error = map_sql_error(error);
                observe(CloseEvent::Poisoned);
                Err(quarantine_repository_until_exit(
                    &mut self,
                    error,
                    CloseUncertainty::CheckpointOrClose,
                ))
            }
        }
    }

    #[cfg(test)]
    fn close_trace_for_test(
        self,
        fault: CloseFault,
    ) -> (Result<(), RepositoryError>, Vec<CloseEvent>) {
        let mut trace = Vec::new();
        let result = self.close_inner(fault, |event| trace.push(event));
        (result, trace)
    }
}

fn quarantine_repository_until_exit(
    repository: &mut ControlRepository,
    error: RepositoryError,
    reason: CloseUncertainty,
) -> RepositoryError {
    let Some(claim) = repository.live_claim.take() else {
        return error;
    };
    retain_poisoned_owner(
        repository.connection.take(),
        repository.main_guard.take(),
        repository.directory_guard.take(),
        claim,
        reason,
        error,
    )
}

impl Drop for ControlRepository {
    fn drop(&mut self) {
        if self.connection.is_some() {
            let _ = quarantine_repository_until_exit(
                self,
                RepositoryError::new(RepositoryErrorClass::Durability),
                CloseUncertainty::ImplicitDrop,
            );
        }
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
    let mut connection = open_validated_connection_after(prepared, false, || {})?;
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
    connection.close()?;
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
    let connection = open_validated_connection_after(prepared, true, || {})?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
    apply_limits(&connection)?;
    let summary = validate_connection(&connection, expected_node_id)?;
    connection.close()?;
    Ok(summary)
}

fn copy_database(source: &Path, destination: &Path) -> Result<(), RepositoryError> {
    let source_storage = prepare_existing_storage_path(source)?;
    let source_connection = open_validated_connection_after(source_storage, true, || {})?;
    backup_connection(&source_connection, destination)?;
    source_connection.close()?;
    Ok(())
}

fn backup_connection(source: &Connection, destination: &Path) -> Result<(), RepositoryError> {
    let destination_storage = prepare_storage_path(destination)?;
    let mut destination_connection =
        open_validated_connection_after(destination_storage, false, || {})?;
    {
        let backup = Backup::new(source, &mut destination_connection)?;
        backup.run_to_completion(128, Duration::from_millis(1), None)?;
    }
    destination_connection.execute_batch("PRAGMA journal_mode=DELETE;")?;
    destination_connection.close()?;
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

fn open_validated_connection_after(
    prepared: PreparedStorage,
    read_only: bool,
    after_validation: impl FnOnce(),
) -> Result<ValidatedConnection, RepositoryError> {
    open_validated_connection_traced(prepared, read_only, after_validation, None)
}

fn open_validated_connection_traced(
    mut prepared: PreparedStorage,
    read_only: bool,
    after_validation: impl FnOnce(),
    #[cfg(test)] mut trace: Option<&mut Vec<OpenTrace>>,
    #[cfg(not(test))] _trace: Option<&mut Vec<()>>,
) -> Result<ValidatedConnection, RepositoryError> {
    validate_prepared_storage_continuity(&prepared)?;
    after_validation();
    let spec = ConnectionOpenSpec::for_existing(prepared.canonical_path.clone(), read_only)?;
    let connection =
        Connection::open_with_flags_and_vfs(&spec.canonical_path, spec.flags, spec.vfs)
            .map_err(map_sql_error)?;
    #[cfg(test)]
    if let Some(trace) = trace.as_deref_mut() {
        trace.push(OpenTrace::SqliteOpened);
    }
    if let Err(error) = validate_prepared_storage_continuity(&prepared) {
        return Err(dispose_uncommitted_open(connection, prepared, error));
    }
    let connected_vfs = match connected_vfs_name(&connection) {
        Ok(name) => name,
        Err(error) => return Err(dispose_uncommitted_open(connection, prepared, error)),
    };
    if connected_vfs != spec.vfs {
        return Err(dispose_uncommitted_open(
            connection,
            prepared,
            RepositoryError::new(RepositoryErrorClass::Database),
        ));
    }
    #[cfg(test)]
    if let Some(trace) = trace.as_deref_mut() {
        trace.push(OpenTrace::PostOpenValidated);
    }
    let live_claim = match prepared.reservation.transition_to_live() {
        Ok(claim) => claim,
        Err(error) => return Err(dispose_uncommitted_open(connection, prepared, error)),
    };
    let PreparedStorage {
        identity,
        directory_guard,
        main_guard,
        ..
    } = prepared;
    #[cfg(test)]
    if let Some(trace) = trace {
        trace.push(OpenTrace::ClaimLive);
    }
    Ok(ValidatedConnection {
        connection: Some(connection),
        directory_guard: Some(directory_guard),
        main_guard: Some(main_guard),
        live_claim: Some(live_claim),
        identity,
    })
}

fn dispose_uncommitted_open(
    connection: Connection,
    prepared: PreparedStorage,
    error: RepositoryError,
) -> RepositoryError {
    #[cfg(test)]
    if FAIL_NEXT_UNCOMMITTED_CLOSE.with(|fault| fault.replace(false)) {
        let mut statement = std::ptr::null_mut();
        let rc = unsafe {
            rusqlite::ffi::sqlite3_prepare_v2(
                connection.handle(),
                c"SELECT 1".as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, rusqlite::ffi::SQLITE_OK);
        assert!(!statement.is_null());
    }
    match connection.close() {
        Ok(()) => error,
        Err((connection, _)) => {
            let mut prepared = prepared;
            match prepared.reservation.transition_to_live() {
                Ok(claim) => retain_poisoned_owner(
                    Some(connection),
                    Some(prepared.main_guard),
                    Some(prepared.directory_guard),
                    claim,
                    CloseUncertainty::CheckpointOrClose,
                    error,
                ),
                Err(_) => retain_quarantined_owner(
                    connection,
                    prepared.main_guard,
                    prepared.directory_guard,
                    prepared.reservation,
                    error,
                ),
            }
        }
    }
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
    validate_auxiliary_files_for_owner(path, effective_user_id())
}

fn validate_auxiliary_files_for_owner(
    path: &Path,
    effective_user_id: u32,
) -> Result<(), RepositoryError> {
    for suffix in ["-wal", "-journal"] {
        validate_optional_private_file_for_owner(
            &auxiliary_path(path, suffix)?,
            None,
            effective_user_id,
        )?;
    }
    validate_optional_private_file_for_owner(
        &migration_backup_path(path)?,
        None,
        effective_user_id,
    )?;
    let shm = auxiliary_path(path, "-shm")?;
    if validate_optional_private_file_for_owner(&shm, None, effective_user_id)?.is_some() {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
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

fn prepare_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    prepare_storage_path_traced(path, None)
}

fn prepare_storage_path_traced(
    path: &Path,
    #[cfg(test)] trace: Option<&mut Vec<OpenTrace>>,
    #[cfg(not(test))] trace: Option<&mut Vec<()>>,
) -> Result<PreparedStorage, RepositoryError> {
    prepare_storage_path_traced_with_preflight(path, trace, StoragePreflight::Production)
}

fn prepare_storage_path_traced_with_preflight(
    path: &Path,
    #[cfg(test)] trace: Option<&mut Vec<OpenTrace>>,
    #[cfg(not(test))] trace: Option<&mut Vec<()>>,
    preflight: StoragePreflight,
) -> Result<PreparedStorage, RepositoryError> {
    prepare_storage_path_traced_with_preflight_after_directory_bound(path, trace, preflight, |_| {})
}

fn prepare_storage_path_traced_with_preflight_after_directory_bound(
    path: &Path,
    #[cfg(test)] mut trace: Option<&mut Vec<OpenTrace>>,
    #[cfg(not(test))] _trace: Option<&mut Vec<()>>,
    preflight: StoragePreflight,
    after_directory_bound: impl FnOnce(&Path),
) -> Result<PreparedStorage, RepositoryError> {
    match preflight {
        StoragePreflight::Production => ensure_supported_storage_platform()?,
        #[cfg(test)]
        StoragePreflight::UnsupportedPlatform => {
            return Err(RepositoryError::new(
                RepositoryErrorClass::UnsupportedPlatform,
            ));
        }
        #[cfg(test)]
        StoragePreflight::MissingUnixExcl => {
            ensure_supported_storage_platform()?;
        }
    }
    #[cfg(test)]
    if let Some(trace) = trace.as_deref_mut() {
        trace.push(OpenTrace::PlatformPreflight);
    }
    match preflight {
        StoragePreflight::Production => ensure_unix_excl_vfs()?,
        #[cfg(test)]
        StoragePreflight::MissingUnixExcl => {
            return Err(RepositoryError::new(
                RepositoryErrorClass::UnsupportedPlatform,
            ));
        }
        #[cfg(test)]
        StoragePreflight::UnsupportedPlatform => unreachable!(),
    }
    #[cfg(test)]
    if let Some(trace) = trace.as_deref_mut() {
        trace.push(OpenTrace::VfsPreflight);
    }
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard = open_secure_directory(parent, true)?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let canonical_path = canonical_parent.join(file_name);
    let directory_identity = file_identity(
        &directory_guard
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    after_directory_bound(&canonical_parent);
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    validate_auxiliary_files(&canonical_path)?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    let existing = match fs::symlink_metadata(&canonical_path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    };
    let (identity, main_guard, reservation) = if let Some(metadata) = existing {
        validate_file_metadata(&metadata)?;
        let identity = file_identity(&metadata)?;
        #[cfg(test)]
        if let Some(trace) = trace.as_deref_mut() {
            trace.push(OpenTrace::PathIdentityWithoutMainFd);
        }
        let reservation = ClaimReservation::reserve(identity)?;
        #[cfg(test)]
        if let Some(trace) = trace.as_deref_mut() {
            trace.push(OpenTrace::ClaimQuarantined);
        }
        let main_guard = open_existing_main_guard(&canonical_path, identity)?;
        (identity, main_guard, reservation)
    } else {
        let main_guard = create_private_file(&canonical_path)?;
        main_guard
            .sync_all()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        directory_guard
            .sync_all()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        let identity = file_identity(
            &main_guard
                .metadata()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
        )?;
        #[cfg(test)]
        if let Some(trace) = trace.as_deref_mut() {
            trace.push(OpenTrace::NewFileCreatedWithMainGuard);
        }
        let reservation = ClaimReservation::reserve(identity)?;
        #[cfg(test)]
        if let Some(trace) = trace.as_deref_mut() {
            trace.push(OpenTrace::ClaimQuarantined);
        }
        (identity, main_guard, reservation)
    };
    #[cfg(test)]
    if let Some(trace) = trace {
        trace.push(OpenTrace::MainGuardOpened);
    }
    Ok(PreparedStorage {
        canonical_path,
        canonical_parent,
        identity,
        directory_identity,
        directory_guard,
        main_guard,
        reservation,
    })
}

fn prepare_existing_storage_path(path: &Path) -> Result<PreparedStorage, RepositoryError> {
    ensure_supported_storage_platform()?;
    ensure_unix_excl_vfs()?;
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard = open_secure_directory(parent, false)?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let canonical_path = canonical_parent.join(file_name);
    let directory_identity = file_identity(
        &directory_guard
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    validate_auxiliary_files(&canonical_path)?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    let metadata = fs::symlink_metadata(&canonical_path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(&metadata)?;
    let identity = file_identity(&metadata)?;
    let reservation = ClaimReservation::reserve(identity)?;
    let main_guard = open_existing_main_guard(&canonical_path, identity)?;
    Ok(PreparedStorage {
        canonical_path,
        canonical_parent,
        identity,
        directory_identity,
        directory_guard,
        main_guard,
        reservation,
    })
}

fn open_existing_main_guard(
    path: &Path,
    expected: FileIdentity,
) -> Result<fs::File, RepositoryError> {
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
    if file_identity(&opened)? != expected || file_identity(&after)? != expected {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(file)
}

fn validate_bound_directory_continuity(
    canonical_parent: &Path,
    directory_identity: FileIdentity,
    directory_guard: &fs::File,
) -> Result<(), RepositoryError> {
    if fs::canonicalize(canonical_parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        != canonical_parent
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    let directory_path_metadata = fs::symlink_metadata(canonical_parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard_metadata = directory_guard
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_directory_metadata(&directory_path_metadata)?;
    validate_directory_metadata(&directory_guard_metadata)?;
    if file_identity(&directory_path_metadata)? != directory_identity
        || file_identity(&directory_guard_metadata)? != directory_identity
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

fn validate_prepared_storage_continuity(prepared: &PreparedStorage) -> Result<(), RepositoryError> {
    validate_bound_directory_continuity(
        &prepared.canonical_parent,
        prepared.directory_identity,
        &prepared.directory_guard,
    )?;
    let path_metadata = fs::symlink_metadata(&prepared.canonical_path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let guard_metadata = prepared
        .main_guard
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(&path_metadata)?;
    validate_file_metadata(&guard_metadata)?;
    if file_identity(&path_metadata)? != prepared.identity
        || file_identity(&guard_metadata)? != prepared.identity
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> Result<FileIdentity, RepositoryError> {
    use std::os::unix::fs::MetadataExt;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn file_identity(_metadata: &fs::Metadata) -> Result<FileIdentity, RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
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

#[cfg(not(unix))]
fn open_secure_directory(_path: &Path, _create: bool) -> Result<fs::File, RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
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
    validate_optional_private_file_for_owner(path, expected, effective_user_id())
}

fn validate_optional_private_file_for_owner(
    path: &Path,
    expected: Option<&fs::Metadata>,
    effective_user_id: u32,
) -> Result<Option<fs::File>, RepositoryError> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    };
    validate_file_metadata_for_owner(&before, effective_user_id)?;
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
    validate_file_metadata_for_owner(&opened, effective_user_id)?;
    validate_file_metadata_for_owner(&after, effective_user_id)?;
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

#[cfg(not(unix))]
fn validate_directory_metadata(_metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
}

#[cfg(unix)]
fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    validate_file_metadata_for_owner(metadata, effective_user_id())
}

#[cfg(unix)]
fn validate_file_metadata_for_owner(
    metadata: &fs::Metadata,
    effective_user_id: u32,
) -> Result<(), RepositoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_file()
        || metadata.uid() != effective_user_id
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

#[cfg(unix)]
fn effective_user_id() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn effective_user_id() -> u32 {
    0
}

#[cfg(not(unix))]
fn validate_file_metadata(metadata: &fs::Metadata) -> Result<(), RepositoryError> {
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(RepositoryError::new(RepositoryErrorClass::UnsafePath))
    }
}

#[cfg(not(unix))]
fn validate_file_metadata_for_owner(
    metadata: &fs::Metadata,
    _effective_user_id: u32,
) -> Result<(), RepositoryError> {
    validate_file_metadata(metadata)
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
    use super::test_support::storage::{
        apply_auxiliary_defect, family_snapshot, AuxiliaryDefect, AuxiliaryKind,
    };
    use super::{ControlIdGenerator, ControlRepository, RepositoryError, RepositoryErrorClass};
    use loxa_protocol::v2::{SlotId, StreamEpoch};
    use loxa_protocol::NodeId;
    use rusqlite::{config::DbConfig, limits::Limit};
    use std::fs;
    use std::io::{BufRead, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::str::FromStr;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

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

    fn commit_revision_two(repository: &mut ControlRepository) {
        let payload =
            super::unloaded_slot_payload(node_id(), repository.slot_id()).expect("slot payload");
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '2', cursor = '2' WHERE singleton = 1",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO events(event_id, stream_epoch, sequence, revision, event_kind, payload_json) VALUES('77777777-7777-4777-8777-777777777777', ?1, '2', '2', 'slot_changed', ?2)",
                    [STREAM_EPOCH, &payload],
                )?;
                Ok(())
            })
            .unwrap();
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ChildProbe {
        Opened,
        DatabaseLocked,
    }

    fn probe_from_subprocess(path: &Path) -> ChildProbe {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", "probe")
            .env("LOXA_REPOSITORY_PATH", path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = wait_child_with_timeout(child, Duration::from_secs(15));
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.contains("PROBE_OPENED") {
            ChildProbe::Opened
        } else if stdout.contains("PROBE_LOCKED") {
            ChildProbe::DatabaseLocked
        } else {
            panic!(
                "repository probe did not report a result\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    fn spawn_repository_owner(path: &Path) -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", "owner")
            .env("LOXA_REPOSITORY_PATH", path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn wait_child_with_timeout(mut child: Child, timeout: Duration) -> std::process::Output {
        let deadline = Instant::now() + timeout;
        loop {
            if child.try_wait().unwrap().is_some() {
                return child.wait_with_output().unwrap();
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "repository helper timed out\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    #[ignore = "exact subprocess entrypoint"]
    fn repository_process_helper() {
        let mode = std::env::var("LOXA_REPOSITORY_HELPER").expect("helper mode");
        let path = PathBuf::from(std::env::var_os("LOXA_REPOSITORY_PATH").expect("helper path"));
        match mode.as_str() {
            "probe" => {
                let outcome = ControlRepository::open_or_create(
                    &path,
                    node_id(),
                    &mut CountingIds::default(),
                );
                match outcome {
                    Ok(mut repository) => {
                        repository
                            .transaction(|transaction| {
                                transaction.execute(
                                    "UPDATE control_meta SET revision = revision WHERE singleton = 1",
                                    [],
                                )?;
                                Ok(())
                            })
                            .unwrap();
                        repository.close().unwrap();
                        println!("PROBE_OPENED");
                    }
                    Err(error) if error.class() == RepositoryErrorClass::Database => {
                        println!("PROBE_LOCKED");
                    }
                    Err(error) => panic!("unexpected probe error: {error:?}"),
                }
            }
            "owner" => {
                let repository = ControlRepository::open_or_create(
                    &path,
                    node_id(),
                    &mut CountingIds::default(),
                )
                .unwrap();
                println!("OWNER_READY");
                std::io::stdout().flush().unwrap();
                let mut release = String::new();
                std::io::stdin().read_line(&mut release).unwrap();
                repository.close().unwrap();
            }
            _ => panic!("unknown helper mode"),
        }
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn production_open_spec_is_exact_for_read_write_and_read_only() {
        let directory = TestDirectory::new("production-open-spec");
        let path = directory.database();
        fs::write(&path, b"").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        for read_only in [false, true] {
            let spec = super::ConnectionOpenSpec::for_existing(path.clone(), read_only).unwrap();
            assert_eq!(spec.vfs, "unix-excl");
            assert!(spec
                .flags
                .contains(rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW));
            assert!(spec
                .flags
                .contains(rusqlite::OpenFlags::SQLITE_OPEN_EXRESCODE));
            assert!(!spec.flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_CREATE));
            assert_eq!(
                spec.flags
                    .contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY),
                read_only
            );
            assert_eq!(
                spec.flags
                    .contains(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE),
                !read_only
            );
            assert_eq!(
                super::open_and_query_connected_vfs(spec).unwrap(),
                "unix-excl"
            );
        }
    }

    #[test]
    fn non_ok_vfsname_with_non_null_sqlite_allocation_is_freed_and_rejected() {
        let error = super::decode_non_ok_allocated_vfsname_for_test().unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Database);
    }

    #[test]
    fn unsupported_platform_or_missing_vfs_refuses_before_filesystem_mutation() {
        for preflight in [
            super::StoragePreflight::UnsupportedPlatform,
            super::StoragePreflight::MissingUnixExcl,
        ] {
            let root = std::env::temp_dir().join(format!(
                "loxa-control-state-preflight-{}-{}",
                std::process::id(),
                StreamEpoch::new_v4()
            ));
            assert!(super::open_with_preflight_for_test(
                &root.join("state/control-state.sqlite3"),
                preflight
            )
            .is_err());
            assert!(!root.exists());
        }
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn parent_or_main_change_across_sqlite_open_fails_before_first_statement() {
        for swap in [
            super::OpenBoundarySwap::Parent,
            super::OpenBoundarySwap::Main,
        ] {
            assert_eq!(
                super::open_with_boundary_swap_for_test(swap)
                    .unwrap_err()
                    .class(),
                RepositoryErrorClass::UnsafePath
            );
        }
    }

    #[test]
    fn missing_main_at_sqlite_open_boundary_is_not_recreated() {
        let directory = TestDirectory::new("missing-main-open-boundary");
        let path = directory.database();
        let prepared = super::prepare_storage_path(&path).unwrap();
        fs::remove_file(&path).unwrap();
        assert!(super::open_validated_connection_after(prepared, false, || {}).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn claim_is_quarantined_before_main_descriptor_open() {
        let directory = TestDirectory::new("claim-open-order");
        create_repository(&directory.database()).close().unwrap();
        assert_eq!(
            super::open_trace_for_test(&directory.database()).unwrap(),
            [
                super::OpenTrace::PlatformPreflight,
                super::OpenTrace::VfsPreflight,
                super::OpenTrace::PathIdentityWithoutMainFd,
                super::OpenTrace::ClaimQuarantined,
                super::OpenTrace::MainGuardOpened,
                super::OpenTrace::SqliteOpened,
                super::OpenTrace::PostOpenValidated,
                super::OpenTrace::ClaimLive,
            ]
        );
    }

    #[test]
    fn new_file_trace_labels_retained_creation_guard_before_claim() {
        let directory = TestDirectory::new("new-file-claim-open-order");
        assert_eq!(
            super::open_trace_for_test(&directory.database()).unwrap(),
            [
                super::OpenTrace::PlatformPreflight,
                super::OpenTrace::VfsPreflight,
                super::OpenTrace::NewFileCreatedWithMainGuard,
                super::OpenTrace::ClaimQuarantined,
                super::OpenTrace::MainGuardOpened,
                super::OpenTrace::SqliteOpened,
                super::OpenTrace::PostOpenValidated,
                super::OpenTrace::ClaimLive,
            ]
        );
    }

    #[test]
    fn claim_transition_failure_explicitly_closes_and_releases_after_guards() {
        let directory = TestDirectory::new("claim-transition-failure");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        super::fail_next_claim_transition_for_test();
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Durability);
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        reopened.close().unwrap();
        assert_eq!(probe_from_subprocess(&path), ChildProbe::Opened);
    }

    #[test]
    fn close_and_claim_transition_failure_retains_the_quarantined_owner() {
        let directory = TestDirectory::new("combined-uncommitted-close-transition-failure");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        super::fail_next_uncommitted_close_and_two_claim_transitions_for_test();
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Durability);
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
        assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);
    }

    #[test]
    fn wal_full_sync_reopens_without_filesystem_shm() {
        let directory = TestDirectory::new("wal-no-shm");
        let path = directory.database();
        let mut repository = create_repository(&path);
        assert!(!super::auxiliary_path(&path, "-shm").unwrap().exists());
        commit_revision_two(&mut repository);
        let journal_mode: String = repository
            .connection_ref()
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let synchronous: i64 = repository
            .connection_ref()
            .unwrap()
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        assert_eq!(synchronous, 2);
        assert!(!super::auxiliary_path(&path, "-shm").unwrap().exists());
        repository.close().unwrap();
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        assert_eq!(reopened.validate_all().unwrap().revision, 2);
        assert!(!super::auxiliary_path(&path, "-shm").unwrap().exists());
        reopened.close().unwrap();
    }

    #[test]
    fn second_in_process_live_repository_is_rejected_until_owner_closes() {
        let directory = TestDirectory::new("same-process-owner");
        let path = directory.database();
        let mut first = create_repository(&path);
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
        commit_revision_two(&mut first);
        assert_eq!(first.validate_all().unwrap().revision, 2);
        assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);
        first.close().unwrap();
        assert_eq!(probe_from_subprocess(&path), ChildProbe::Opened);
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn second_process_is_excluded_after_post_open_validation_until_owner_closes() {
        let directory = TestDirectory::new("second-process-owner");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        let mut child = spawn_repository_owner(&path);
        let stdout = child.stdout.take().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stdout);
            let mut ready = String::new();
            let mut ready_sent = false;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                if !ready_sent {
                    ready.push_str(&line);
                    if line.contains("OWNER_READY") {
                        let _ = ready_tx.send(ready.clone());
                        ready_sent = true;
                    }
                }
            }
            if !ready_sent {
                let _ = ready_tx.send(ready);
            }
        });
        let ready = ready_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| {
                let _ = child.kill();
                panic!("repository owner did not become READY before timeout")
            });
        assert!(ready.contains("OWNER_READY"), "{ready}");
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Database);
        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        let output = wait_child_with_timeout(child, Duration::from_secs(10));
        assert!(
            output.status.success(),
            "child stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        reopened.close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_auxiliary_with_missing_main_refuses_without_creating_main() {
        for kind in AuxiliaryKind::ALL {
            let directory = TestDirectory::new(&format!("missing-main-{kind:?}"));
            let path = directory.database();
            apply_auxiliary_defect(&path, kind, AuxiliaryDefect::WrongMode);
            let before = family_snapshot(&path);
            let error =
                ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::fixed())
                    .unwrap_err();
            assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
            assert_eq!(family_snapshot(&path), before, "{kind:?}");
            assert!(!path.exists(), "{kind:?} created the missing main");
        }
    }

    #[cfg(unix)]
    #[test]
    fn replacement_parent_auxiliaries_are_checked_before_main_creation() {
        let error = super::prepare_with_parent_auxiliary_swap_for_test().unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_wal_journal_backup_and_any_stale_shm_fail_before_mutation() {
        use std::os::unix::fs::PermissionsExt;

        for kind in AuxiliaryKind::ALL {
            for defect in AuxiliaryDefect::ALL {
                let directory = TestDirectory::new(&format!("unsafe-family-{kind:?}-{defect:?}"));
                let path = directory.database();
                create_repository(&path).close().unwrap();
                if kind != AuxiliaryKind::Backup {
                    fs::remove_file(super::migration_backup_path(&path).unwrap()).unwrap();
                }
                apply_auxiliary_defect(&path, kind, defect);
                let before = family_snapshot(&path);
                let result = if defect == AuxiliaryDefect::WrongOwner {
                    // Unprivileged tests cannot chown to another UID. Exercise the
                    // same production metadata path with a deterministic mismatched
                    // effective UID instead.
                    let actual = super::effective_user_id();
                    let mismatched = if actual == u32::MAX { 0 } else { actual + 1 };
                    super::validate_auxiliary_files_for_owner(&path, mismatched)
                        .map(|_| unreachable!("wrong-owner family accepted"))
                } else {
                    ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                        .map(|repository| {
                            let _ = repository.close();
                        })
                };
                assert!(result.is_err(), "{kind:?} {defect:?}");
                assert_eq!(family_snapshot(&path), before, "{kind:?} {defect:?}");
            }
        }

        let directory = TestDirectory::new("stale-shm");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        let shm = super::auxiliary_path(&path, "-shm").unwrap();
        fs::write(&shm, b"stale").unwrap();
        fs::set_permissions(&shm, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default(),)
                .is_err()
        );
        assert_eq!(fs::read(shm).unwrap(), b"stale");
    }

    #[test]
    fn unrelated_sqlite_database_remains_openable_while_repository_is_live() {
        let repository_directory = TestDirectory::new("repository-live-unrelated");
        let repository = create_repository(&repository_directory.database());
        let unrelated = TestDirectory::new("unrelated-sqlite");
        let connection = rusqlite::Connection::open(unrelated.0.join("unrelated.sqlite3")).unwrap();
        connection
            .execute("CREATE TABLE proof(value INTEGER)", [])
            .unwrap();
        assert!(repository.validate_all().is_ok());
        connection.close().unwrap();
        repository.close().unwrap();
    }

    #[test]
    fn implicit_drop_poisons_the_claim_until_process_exit() {
        let directory = TestDirectory::new("implicit-drop-poison");
        let path = directory.database();
        let repository = create_repository(&path);
        drop(repository);
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
        assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);
    }

    #[test]
    fn successful_close_orders_checkpoint_sqlite_guards_and_claim_release() {
        let directory = TestDirectory::new("close-success-trace");
        let path = directory.database();
        let (result, trace) =
            create_repository(&path).close_trace_for_test(super::CloseFault::None);
        result.unwrap();
        assert_eq!(
            trace,
            [
                super::CloseEvent::Checkpoint,
                super::CloseEvent::SqliteClosed,
                super::CloseEvent::MainGuardClosed,
                super::CloseEvent::DirectoryGuardClosed,
                super::CloseEvent::ClaimReleased,
            ]
        );
        assert_eq!(probe_from_subprocess(&path), ChildProbe::Opened);
    }

    #[test]
    fn checkpoint_and_returned_connection_close_uncertainty_poison_owners() {
        for fault in [
            super::CloseFault::Checkpoint,
            super::CloseFault::ReturnedConnection,
        ] {
            let directory = TestDirectory::new(&format!("close-poison-{fault:?}"));
            let path = directory.database();
            let (result, trace) = create_repository(&path).close_trace_for_test(fault);
            assert!(result.is_err());
            assert_eq!(
                trace,
                [super::CloseEvent::Checkpoint, super::CloseEvent::Poisoned]
            );
            let error =
                ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                    .unwrap_err();
            assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
            assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);
        }
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
                .connection_ref()
                .unwrap()
                .query_row(sql, [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        let journal_mode: String = repository
            .connection_ref()
            .unwrap()
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
            .connection_ref()
            .unwrap()
            .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
            .unwrap());
        assert_eq!(
            repository
                .connection_ref()
                .unwrap()
                .limit(Limit::SQLITE_LIMIT_LENGTH)
                .unwrap(),
            super::SQLITE_LENGTH_LIMIT
        );
        assert_eq!(
            repository
                .connection_ref()
                .unwrap()
                .limit(Limit::SQLITE_LIMIT_ATTACHED)
                .unwrap(),
            super::SQLITE_ATTACHED_LIMIT
        );
        assert_eq!(
            repository
                .connection_ref()
                .unwrap()
                .limit(Limit::SQLITE_LIMIT_WORKER_THREADS)
                .unwrap(),
            super::SQLITE_WORKER_THREADS_LIMIT
        );
    }

    #[test]
    fn existing_repository_loads_ids_without_generating_replacements() {
        let directory = TestDirectory::new("persisted-ids");
        let path = directory.database();
        create_repository(&path).close().unwrap();

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
        create_repository(&path).close().unwrap();
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
        repository.close().unwrap();

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
        repository.close().unwrap();

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
        create_repository(&path).close().unwrap();
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
        repository.close().unwrap();

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
        repository.close().unwrap();

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
        sibling_repository.close().unwrap();
        fs::copy(sibling_backup, &retained).unwrap();

        repository.close().unwrap();
        let error =
            ControlRepository::restore_verified_migration_backup(&retained, &original_path, &proof)
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
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
        create_repository(&path).close().unwrap();
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
        original.close().unwrap();

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
