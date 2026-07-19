use super::schema::{
    migration_2_checksum, schema_checksum, MIGRATION_2, MIGRATION_2_NAME, MIGRATION_NAME,
    SCHEMA_V1, SCHEMA_VERSION,
};
use loxa_protocol::v2::DecimalU64;
use loxa_protocol::v2::{
    EventId, OperationId, SlotId, StreamEpoch, V2ControlEvent, V2EventEntity, V2Node, V2Operation,
    V2OperationErrorCode, V2OperationKind, V2OperationProgress, V2OperationStatus, V2PublicError,
    V2Slot, V2SlotErrorCode, V2SlotStatus, V2_SCHEMA_VERSION,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use rusqlite::backup::Backup;
use rusqlite::config::DbConfig;
use rusqlite::limits::Limit;
use rusqlite::{
    params, Connection, Error as SqlError, ErrorCode, OpenFlags, OptionalExtension, Transaction,
    TransactionBehavior,
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
use super::state_machine::test_support;

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
    fn new_initial_event_id(&mut self) -> EventId {
        EventId::new_v4()
    }
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
    state_machine_tag: Option<&'static str>,
}

impl RepositoryError {
    fn new(class: RepositoryErrorClass) -> Self {
        Self {
            class,
            state_machine_tag: None,
        }
    }

    #[cfg(test)]
    fn corrupt() -> Self {
        Self::new(RepositoryErrorClass::Corrupt)
    }

    pub(crate) fn class(&self) -> RepositoryErrorClass {
        self.class
    }

    pub(super) fn tagged_for_state_machine(tag: &'static str) -> Self {
        Self {
            class: RepositoryErrorClass::Database,
            state_machine_tag: Some(tag),
        }
    }

    pub(super) fn state_machine_tag(&self) -> Option<&'static str> {
        self.state_machine_tag
    }

    pub(super) fn corrupt_for_state_machine() -> Self {
        Self::new(RepositoryErrorClass::Corrupt)
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct ScalarProvenance {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) owner_pid: u32,
    pub(crate) owner_process_start_time_unix_s: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScalarSource {
    Fresh,
    PriorDeadChildlessModelFreeUnloadedV4(ScalarProvenance),
}

impl ScalarSource {
    pub(crate) fn from_managed(source: loxa_core::supervisor::ManagedScalarSource) -> Option<Self> {
        match source {
            loxa_core::supervisor::ManagedScalarSource::Fresh => Some(Self::Fresh),
            loxa_core::supervisor::ManagedScalarSource::PriorDeadChildlessModelFreeUnloadedV4(
                provenance,
            ) => Some(Self::PriorDeadChildlessModelFreeUnloadedV4(
                ScalarProvenance {
                    schema_version: provenance.schema_version,
                    run_id: provenance.run_id,
                    owner_pid: provenance.owner_pid,
                    owner_process_start_time_unix_s: provenance.owner_process_start_time_unix_s,
                },
            )),
            loxa_core::supervisor::ManagedScalarSource::ExistingDatabase => None,
        }
    }

    fn durable_text(&self) -> Result<String, RepositoryError> {
        match self {
            Self::Fresh => Ok("fresh".to_owned()),
            Self::PriorDeadChildlessModelFreeUnloadedV4(provenance) => {
                if provenance.schema_version != 4 || provenance.run_id.is_empty() {
                    return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
                }
                serde_json::to_string(provenance)
                    .map(|json| format!("prior_dead_childless_model_free_unloaded_v4:{json}"))
                    .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))
            }
        }
    }
}

struct StoredSlotRow {
    slot_id: String,
    name: String,
    status: String,
    model_id: Option<String>,
    operation_id: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    updated_revision: String,
    updated_at_unix_ms: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredSlotIntent {
    pub(crate) desired_kind: DesiredKind,
    pub(crate) desired_model_id: Option<String>,
    pub(crate) desired_revision: u64,
    pub(crate) operation_id: Option<OperationId>,
    pub(crate) reconciliation: ReconciliationState,
    pub(crate) reason: Option<IntentReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DesiredKind {
    Unloaded,
    Loaded,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReconciliationState {
    Settled,
    Applying,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntentReason {
    PreexistingRecovery,
    MigrationAmbiguousLoading,
    MigrationOperationMismatch,
    ChildEvidenceUncertain,
    CompensationFailed,
    DurableCommitUncertain,
}

pub(crate) struct ControlRepository {
    connection: Option<Connection>,
    path: PathBuf,
    identity: FileIdentity,
    expected_node_id: NodeId,
    slot_id: SlotId,
    stream_epoch: StreamEpoch,
    directory_guard: Option<fs::File>,
    family_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    live_claim: Option<LiveDatabaseClaim>,
}

struct ValidatedConnection {
    connection: Option<Connection>,
    directory_guard: Option<fs::File>,
    family_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    live_claim: Option<LiveDatabaseClaim>,
    identity: FileIdentity,
}

/// A validated SQLite image while its connection and exclusive ownership are live.
///
/// Every field that owns authority is optional because the consuming close path and
/// `Drop` must move the authority into a fail-closed quarantine without moving fields
/// directly out of a type that implements `Drop`.
struct OpenValidatedImage {
    path: Option<PathBuf>,
    identity: FileIdentity,
    connection: Option<Connection>,
    directory_guard: Option<fs::File>,
    family_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    live_claim: Option<LiveDatabaseClaim>,
    summary: ValidationSummary,
    schema_version: i64,
}

/// A standalone, sidecar-free SQLite image whose inode remains reserved and guarded.
struct ClosedImage {
    path: Option<PathBuf>,
    identity: FileIdentity,
    directory_guard: Option<fs::File>,
    family_guard: Option<fs::File>,
    main_guard: Option<fs::File>,
    reservation: Option<ClaimReservation>,
    summary: ValidationSummary,
    schema_version: i64,
}

/// A destination whose existing image is quiesced or whose absence is bound to a
/// retained parent-directory identity.
enum QuiescedDestination {
    Existing(ClosedImage),
    Vacant(GuardedVacantDestination),
}

struct GuardedVacantDestination {
    path: PathBuf,
    canonical_parent: PathBuf,
    directory_identity: FileIdentity,
    directory_guard: fs::File,
    family_guard: Option<fs::File>,
}

impl Drop for GuardedVacantDestination {
    fn drop(&mut self) {
        let Some(guard) = self.family_guard.take() else {
            return;
        };
        if unlock_family_guard(&guard).is_err() {
            let _retained_until_exit: &'static mut fs::File = Box::leak(Box::new(guard));
        }
    }
}

#[derive(Clone, Copy)]
enum DestinationInstallExpectation {
    Vacant,
    Existing(FileIdentity),
}

struct AtomicInstallError {
    error: RepositoryError,
    renamed: bool,
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
                let claim = self
                    .live_claim
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
                let family_guard = self
                    .family_guard
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
                release_live_family_after_close(claim, family_guard)
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
        fs::File,
        LiveDatabaseClaim,
        FileIdentity,
    ) {
        (
            self.connection.take().expect("connection retained"),
            self.directory_guard
                .take()
                .expect("directory guard retained"),
            self.family_guard.take().expect("family guard retained"),
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

impl Drop for OpenValidatedImage {
    fn drop(&mut self) {
        if let Some(claim) = self.live_claim.take() {
            let _ = retain_poisoned_owner(
                self.connection.take(),
                self.main_guard.take(),
                self.directory_guard.take(),
                self.family_guard.take(),
                claim,
                CloseUncertainty::ImplicitDrop,
                RepositoryError::new(RepositoryErrorClass::Durability),
            );
        }
    }
}

impl ClosedImage {
    fn path(&self) -> Result<&Path, RepositoryError> {
        self.path
            .as_deref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))
    }

    fn release(mut self) -> Result<(), RepositoryError> {
        drop(self.main_guard.take());
        drop(self.directory_guard.take());
        let reservation = self
            .reservation
            .take()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
        let family_guard = self
            .family_guard
            .take()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
        release_reserved_family_after_close(reservation, family_guard)
    }
}

impl Drop for ClosedImage {
    fn drop(&mut self) {
        drop(self.main_guard.take());
        drop(self.directory_guard.take());
        if let Some(reservation) = self.reservation.take() {
            if let Some(family_guard) = self.family_guard.take() {
                let _ = release_reserved_family_after_close(reservation, family_guard);
            }
            return;
        }
        if let Some(family_guard) = self.family_guard.take() {
            if unlock_family_guard(&family_guard).is_err() {
                let _retained_until_exit: &'static mut fs::File = Box::leak(Box::new(family_guard));
            }
        }
    }
}

fn retain_poisoned_owner(
    connection: Option<Connection>,
    main_guard: Option<fs::File>,
    directory_guard: Option<fs::File>,
    family_guard: Option<fs::File>,
    mut claim: LiveDatabaseClaim,
    reason: CloseUncertainty,
    error: RepositoryError,
) -> RepositoryError {
    let _ = claim.poison(reason);
    let owner = PoisonedDatabaseOwner {
        connection,
        main_guard,
        directory_guard,
        family_guard,
        claim,
    };
    let _retained_until_exit: &'static mut PoisonedDatabaseOwner = Box::leak(Box::new(owner));
    error
}

fn retain_quarantined_owner(
    connection: Connection,
    main_guard: fs::File,
    directory_guard: fs::File,
    family_guard: fs::File,
    reservation: ClaimReservation,
    error: RepositoryError,
) -> RepositoryError {
    let owner = PoisonedReservationOwner {
        connection,
        main_guard,
        directory_guard,
        family_guard,
        reservation,
    };
    let _retained_until_exit: &'static mut PoisonedReservationOwner = Box::leak(Box::new(owner));
    error
}

fn retain_poisoned_family_reservation(
    family_guard: fs::File,
    mut reservation: ClaimReservation,
    error: RepositoryError,
) -> RepositoryError {
    let _ = reservation.poison(CloseUncertainty::CheckpointOrClose);
    let owner = PoisonedFamilyReservationOwner {
        family_guard,
        reservation,
    };
    let _retained_until_exit: &'static mut PoisonedFamilyReservationOwner =
        Box::leak(Box::new(owner));
    error
}

fn release_live_family_after_close(
    mut claim: LiveDatabaseClaim,
    family_guard: fs::File,
) -> Result<(), RepositoryError> {
    if let Err(error) = unlock_family_guard(&family_guard) {
        return Err(retain_poisoned_owner(
            None,
            None,
            None,
            Some(family_guard),
            claim,
            CloseUncertainty::CheckpointOrClose,
            error,
        ));
    }
    if let Err(error) = claim.release_after_proven_close() {
        return Err(retain_poisoned_owner(
            None,
            None,
            None,
            Some(family_guard),
            claim,
            CloseUncertainty::CheckpointOrClose,
            error,
        ));
    }
    drop(family_guard);
    Ok(())
}

fn release_reserved_family_after_close(
    mut reservation: ClaimReservation,
    family_guard: fs::File,
) -> Result<(), RepositoryError> {
    if let Err(error) = unlock_family_guard(&family_guard) {
        return Err(retain_poisoned_family_reservation(
            family_guard,
            reservation,
            error,
        ));
    }
    if let Err(error) = reservation.release_after_guards_closed() {
        return Err(retain_poisoned_family_reservation(
            family_guard,
            reservation,
            error,
        ));
    }
    drop(family_guard);
    Ok(())
}

fn quarantine_closed_image_until_exit(
    image: &mut ClosedImage,
    error: RepositoryError,
) -> RepositoryError {
    let Some(mut reservation) = image.reservation.take() else {
        return error;
    };
    let _ = reservation.poison(CloseUncertainty::CheckpointOrClose);
    let Some(main_guard) = image.main_guard.take() else {
        return error;
    };
    let Some(directory_guard) = image.directory_guard.take() else {
        return error;
    };
    let Some(family_guard) = image.family_guard.take() else {
        return error;
    };
    let owner = PoisonedClosedImageOwner {
        main_guard,
        directory_guard,
        family_guard,
        reservation,
    };
    let _retained_until_exit: &'static mut PoisonedClosedImageOwner = Box::leak(Box::new(owner));
    error
}

fn quarantine_closed_image_connection_until_exit(
    image: &mut ClosedImage,
    connection: Connection,
    error: RepositoryError,
) -> RepositoryError {
    let Some(mut reservation) = image.reservation.take() else {
        return error;
    };
    let _ = reservation.poison(CloseUncertainty::CheckpointOrClose);
    let Some(main_guard) = image.main_guard.take() else {
        return error;
    };
    let Some(directory_guard) = image.directory_guard.take() else {
        return error;
    };
    let Some(family_guard) = image.family_guard.take() else {
        return error;
    };
    let owner = PoisonedReservationOwner {
        connection,
        main_guard,
        directory_guard,
        family_guard,
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
        owner.family_guard.take(),
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
    family_guard: fs::File,
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
enum ImageCloseEvent {
    CheckpointTruncated,
    JournalModeDelete,
    SqliteClosed,
    SidecarsAbsent,
    ReadOnlyValidated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestoreBoundary {
    BeforeSourceCopy,
    AfterSourceCopy,
    SourceCheckpointTruncated,
    SourceJournalModeDelete,
    SourceSqliteClosed,
    SourceSidecarsAbsent,
    SourceReadOnlyValidated,
    DestinationCheckpointTruncated,
    DestinationJournalModeDelete,
    DestinationSqliteClosed,
    DestinationSidecarsAbsent,
    DestinationReadOnlyValidated,
    BeforeRename,
    AfterRename,
    AfterDirectorySync,
    ReopenValidated,
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
    family_guard: Option<fs::File>,
    claim: LiveDatabaseClaim,
}

struct PoisonedReservationOwner {
    connection: Connection,
    main_guard: fs::File,
    directory_guard: fs::File,
    family_guard: fs::File,
    reservation: ClaimReservation,
}

struct PoisonedClosedImageOwner {
    main_guard: fs::File,
    directory_guard: fs::File,
    family_guard: fs::File,
    reservation: ClaimReservation,
}

struct PoisonedFamilyReservationOwner {
    family_guard: fs::File,
    reservation: ClaimReservation,
}

static NEXT_CLAIM_TOKEN: AtomicU64 = AtomicU64::new(1);
static DATABASE_CLAIMS: OnceLock<Mutex<BTreeMap<FileIdentity, ClaimState>>> = OnceLock::new();
#[cfg(test)]
thread_local! {
    static FAIL_CLAIM_TRANSITIONS: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static FAIL_NEXT_UNCOMMITTED_CLOSE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_FAMILY_UNLOCK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_ATOMIC_INSTALL_POSTFLIGHT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static RECONCILIATION_TRANSACTION_FAULT: std::cell::Cell<Option<ReconciliationTransactionFault>> = const { std::cell::Cell::new(None) };
    static MIGRATION_STATEMENT_FAULT: std::cell::Cell<Option<MigrationStatementFault>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MigrationStatementFault {
    AfterStatement(usize),
}

#[cfg(test)]
pub(crate) struct MigrationStatementFaultGuard {
    previous: Option<MigrationStatementFault>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
impl Drop for MigrationStatementFaultGuard {
    fn drop(&mut self) {
        MIGRATION_STATEMENT_FAULT.with(|armed| armed.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn arm_migration_statement_fault_for_test(
    fault: MigrationStatementFault,
) -> MigrationStatementFaultGuard {
    let previous = MIGRATION_STATEMENT_FAULT.with(|armed| armed.replace(Some(fault)));
    MigrationStatementFaultGuard {
        previous,
        _not_send: std::marker::PhantomData,
    }
}

#[cfg(test)]
fn fail_at_migration_statement_for_test(completed: usize) -> Result<(), RepositoryError> {
    let fail = MIGRATION_STATEMENT_FAULT.with(|armed| {
        if armed.get() == Some(MigrationStatementFault::AfterStatement(completed)) {
            armed.set(None);
            true
        } else {
            false
        }
    });
    if fail {
        Err(RepositoryError::new(RepositoryErrorClass::Durability))
    } else {
        Ok(())
    }
}

#[cfg(not(test))]
fn fail_at_migration_statement_for_test(_completed: usize) -> Result<(), RepositoryError> {
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReconciliationTransactionFault {
    BeforeTransaction,
    BeforeCommit,
    AfterCommit,
}

#[cfg(test)]
pub(super) fn arm_reconciliation_transaction_fault_for_test(fault: ReconciliationTransactionFault) {
    RECONCILIATION_TRANSACTION_FAULT.with(|armed| armed.set(Some(fault)));
}

#[cfg(test)]
fn take_reconciliation_transaction_fault_for_test() -> Option<ReconciliationTransactionFault> {
    RECONCILIATION_TRANSACTION_FAULT.with(|armed| armed.take())
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

    fn release_after_guards_closed(&mut self) -> Result<(), RepositoryError> {
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match claims.get(&self.identity) {
            Some(ClaimState::Quarantined { token }) if *token == self.token => {
                claims.remove(&self.identity);
                self.active = false;
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
            Some(ClaimState::Quarantined { token }) if *token == self.token => {
                claims.insert(self.identity, ClaimState::Poisoned { reason });
                self.active = false;
                Ok(())
            }
            Some(ClaimState::Poisoned { .. }) => {
                self.active = false;
                Ok(())
            }
            _ => Err(RepositoryError::new(RepositoryErrorClass::Durability)),
        }
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

#[cfg(test)]
fn fail_next_atomic_install_postflight_for_test() {
    FAIL_ATOMIC_INSTALL_POSTFLIGHT.with(|fault| fault.set(true));
}

#[cfg(test)]
fn fail_next_family_unlock_for_test() {
    FAIL_NEXT_FAMILY_UNLOCK.with(|fault| fault.set(true));
}

#[cfg(test)]
fn atomic_install_postflight_fault_is_pending_for_test() -> bool {
    FAIL_ATOMIC_INSTALL_POSTFLIGHT.with(std::cell::Cell::get)
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
    fn transition_to_quarantined(&mut self) -> Result<ClaimReservation, RepositoryError> {
        let mut claims = claims()
            .lock()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        match claims.get(&self.identity) {
            Some(ClaimState::Live { token }) if *token == self.token => {
                claims.insert(self.identity, ClaimState::Quarantined { token: self.token });
                Ok(ClaimReservation {
                    identity: self.identity,
                    token: self.token,
                    active: true,
                })
            }
            _ => Err(RepositoryError::new(RepositoryErrorClass::Durability)),
        }
    }

    fn release_after_proven_close(&mut self) -> Result<(), RepositoryError> {
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

#[cfg(all(
    not(any(target_os = "macos", target_os = "linux")),
    feature = "unsupported-platform-ci"
))]
pub(super) fn production_preflight_for_unsupported_platform_ci(
    path: &Path,
) -> Result<(), RepositoryError> {
    prepare_storage_path(path).map(drop)
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
                open_existing_or_initialize_in_one_transaction(&mut opened, node_id, None, ids)?;
            let migrated =
                migrate_to_current_schema(&mut opened, &canonical_path, node_id, !initialized)?;
            let summary = validate_connection(&opened, Some(node_id))?;
            if migrated {
                sync_and_reopen_validate_migration(&opened, &canonical_path, node_id, &summary)?;
                if !initialized {
                    publish_migration_backup_for_schema(
                        &opened,
                        &canonical_path,
                        node_id,
                        SCHEMA_VERSION,
                        1,
                    )?;
                }
            } else {
                ensure_current_migration_backup(&opened, &canonical_path, node_id, &summary)?;
            }
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
        let (connection, directory_guard, family_guard, main_guard, live_claim, identity) =
            opened.into_repository_parts();
        let repository = Self {
            connection: Some(connection),
            path: canonical_path,
            identity,
            expected_node_id: node_id,
            slot_id: summary.slot_id,
            stream_epoch: summary.epoch,
            directory_guard: Some(directory_guard),
            family_guard: Some(family_guard),
            main_guard: Some(main_guard),
            live_claim: Some(live_claim),
        };
        if initialized {
            repository.checkpoint()?;
            repository.publish_migration_backup()?;
        }
        Ok(repository)
    }

    pub(crate) fn open_or_migrate(
        path: &Path,
        node_id: NodeId,
        first_migration_source: Option<ScalarSource>,
        ids: &mut dyn ControlIdGenerator,
    ) -> Result<Self, RepositoryError> {
        if first_migration_source.is_none()
            && matches!(fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        ensure_supported_storage_platform()?;
        ensure_unix_excl_vfs()?;
        let prepared = prepare_storage_path(path)?;
        let canonical_path = prepared.canonical_path.clone();
        let mut opened = open_validated_connection_after(prepared, false, || {})?;
        let setup = (|| {
            configure_defensively(&opened)?;
            let initialized = open_existing_or_initialize_in_one_transaction(
                &mut opened,
                node_id,
                Some(first_migration_source.as_ref()),
                ids,
            )?;
            let migrated =
                migrate_to_current_schema(&mut opened, &canonical_path, node_id, !initialized)?;
            let summary = validate_connection(&opened, Some(node_id))?;
            if migrated {
                sync_and_reopen_validate_migration(&opened, &canonical_path, node_id, &summary)?;
                if !initialized {
                    publish_migration_backup_for_schema(
                        &opened,
                        &canonical_path,
                        node_id,
                        SCHEMA_VERSION,
                        1,
                    )?;
                }
            } else {
                ensure_current_migration_backup(&opened, &canonical_path, node_id, &summary)?;
            }
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
        let (connection, directory_guard, family_guard, main_guard, live_claim, identity) =
            opened.into_repository_parts();
        let repository = Self {
            connection: Some(connection),
            path: canonical_path,
            identity,
            expected_node_id: node_id,
            slot_id: summary.slot_id,
            stream_epoch: summary.epoch,
            directory_guard: Some(directory_guard),
            family_guard: Some(family_guard),
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

    pub(crate) fn node_id(&self) -> NodeId {
        self.expected_node_id
    }

    pub(crate) fn read_transaction<T>(
        &self,
        work: impl FnOnce(&Connection) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        self.validate_ownership()?;
        work(self.connection_ref()?)
    }

    pub(crate) fn transaction<T>(
        &mut self,
        work: impl FnOnce(&Transaction<'_>) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        self.validate_ownership()?;
        #[cfg(test)]
        let fault = take_reconciliation_transaction_fault_for_test();
        #[cfg(test)]
        if fault == Some(ReconciliationTransactionFault::BeforeTransaction) {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        let transaction = self
            .connection
            .as_mut()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = work(&transaction)?;
        #[cfg(test)]
        if fault == Some(ReconciliationTransactionFault::BeforeCommit) {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        transaction.commit()?;
        #[cfg(test)]
        if fault == Some(ReconciliationTransactionFault::AfterCommit) {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        Ok(value)
    }

    pub(crate) fn validate_all(&self) -> Result<ValidationSummary, RepositoryError> {
        self.validate_ownership()?;
        validate_connection(self.connection_ref()?, Some(self.expected_node_id))
    }

    pub(crate) fn stored_slot_intent(&self) -> Result<StoredSlotIntent, RepositoryError> {
        self.validate_ownership()?;
        read_stored_slot_intent(self.connection_ref()?)
    }

    pub(crate) fn requires_specialized_migration_recovery(&self) -> Result<bool, RepositoryError> {
        self.validate_ownership()?;
        let connection = self.connection_ref()?;
        let control_revision = connection
            .query_row(
                "SELECT revision FROM control_meta WHERE singleton=1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(classify_missing_row)
            .and_then(|revision| parse_canonical_u64(&revision))?;
        let slot: StoredSlotRow = connection
            .query_row(
                "SELECT slot_id,name,status,model_id,operation_id,error_code,error_message,updated_revision,updated_at_unix_ms FROM slot_state WHERE singleton=1",
                [],
                |row| Ok(StoredSlotRow { slot_id: row.get(0)?, name: row.get(1)?, status: row.get(2)?, model_id: row.get(3)?, operation_id: row.get(4)?, error_code: row.get(5)?, error_message: row.get(6)?, updated_revision: row.get(7)?, updated_at_unix_ms: row.get(8)? }),
            )
            .map_err(classify_missing_row)?;
        let intent = read_stored_slot_intent(connection)?;
        Ok(exact_migration_recovery(connection, control_revision, &slot, &intent)?.is_some())
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
        let image = close_into_image(open_validated_image(&backup, Some(self.expected_node_id))?)?;
        let summary = image.summary.clone();
        let digest = digest_file_handle(
            image
                .main_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        )?;
        let proof = MigrationRollbackProof {
            node_id: self.expected_node_id,
            slot_id: summary.slot_id,
            epoch: summary.epoch,
            revision: summary.revision,
            cursor: summary.cursor,
            digest,
        };
        image.release()?;
        Ok(proof)
    }

    pub(crate) fn restore_offline(
        backup: &Path,
        destination: &Path,
    ) -> Result<RestoreSummary, RepositoryError> {
        Self::restore_offline_after(backup, destination, |_| Ok(()))
    }

    fn restore_offline_after(
        backup: &Path,
        destination: &Path,
        mut observe: impl FnMut(RestoreBoundary) -> Result<(), RepositoryError>,
    ) -> Result<RestoreSummary, RepositoryError> {
        ensure_auxiliary_files_absent(backup)?;
        let backup_image = close_into_image(open_validated_image(backup, None)?)?;
        let source_summary = backup_image.summary.clone();
        let temporary = unique_temporary_path(destination, "restore")?;
        observe(RestoreBoundary::BeforeSourceCopy)?;
        let result = (|| {
            copy_closed_image(&backup_image, &temporary)?;
            observe(RestoreBoundary::AfterSourceCopy)?;
            let source = rotate_lineage_after(&temporary, source_summary.node_id, |event| {
                observe(match event {
                    ImageCloseEvent::CheckpointTruncated => {
                        RestoreBoundary::SourceCheckpointTruncated
                    }
                    ImageCloseEvent::JournalModeDelete => RestoreBoundary::SourceJournalModeDelete,
                    ImageCloseEvent::SqliteClosed => RestoreBoundary::SourceSqliteClosed,
                    ImageCloseEvent::SidecarsAbsent => RestoreBoundary::SourceSidecarsAbsent,
                    ImageCloseEvent::ReadOnlyValidated => RestoreBoundary::SourceReadOnlyValidated,
                })
            })?;
            source
                .main_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
                .sync_all()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
            backup_image.release()?;
            let quiesced_destination = quiesce_destination_after(destination, |event| {
                observe(match event {
                    ImageCloseEvent::CheckpointTruncated => {
                        RestoreBoundary::DestinationCheckpointTruncated
                    }
                    ImageCloseEvent::JournalModeDelete => {
                        RestoreBoundary::DestinationJournalModeDelete
                    }
                    ImageCloseEvent::SqliteClosed => RestoreBoundary::DestinationSqliteClosed,
                    ImageCloseEvent::SidecarsAbsent => RestoreBoundary::DestinationSidecarsAbsent,
                    ImageCloseEvent::ReadOnlyValidated => {
                        RestoreBoundary::DestinationReadOnlyValidated
                    }
                })
            })?;
            let reopened = install_closed_image_after(
                source,
                quiesced_destination,
                source_summary.node_id,
                &mut observe,
            )?;
            Ok(RestoreSummary {
                epoch: reopened.epoch,
                revision: reopened.revision,
                cursor: reopened.cursor,
                event_rows: reopened.event_rows,
            })
        })();
        finish_with_temporary_cleanup(result, &temporary)
    }

    fn restore_verified_migration_backup(
        backup: &Path,
        destination: &Path,
        proof: &MigrationRollbackProof,
    ) -> Result<(), RepositoryError> {
        Self::restore_verified_migration_backup_after(backup, destination, proof, |_| Ok(()))
    }

    fn rollback_failed_migration(
        backup: &Path,
        destination: &Path,
        proof: &MigrationRollbackProof,
        original_migration_error: RepositoryError,
    ) -> RepositoryError {
        match Self::restore_verified_migration_backup(backup, destination, proof) {
            Ok(()) => original_migration_error,
            Err(rollback_error) => rollback_error,
        }
    }

    fn restore_verified_migration_backup_after(
        backup: &Path,
        destination: &Path,
        proof: &MigrationRollbackProof,
        mut observe: impl FnMut(RestoreBoundary) -> Result<(), RepositoryError>,
    ) -> Result<(), RepositoryError> {
        if migration_backup_path(destination)? != backup {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
        ensure_auxiliary_files_absent(backup)?;
        let backup_image = close_into_image(open_validated_image(backup, Some(proof.node_id))?)?;
        let backup_summary = backup_image.summary.clone();
        if backup_summary.slot_id != proof.slot_id
            || backup_summary.epoch != proof.epoch
            || backup_summary.revision != proof.revision
            || backup_summary.cursor != proof.cursor
            || digest_file_handle(
                backup_image
                    .main_guard
                    .as_ref()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            )? != proof.digest
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        // Rollback is automatic only while both lineages remain fully
        // interpretable. Corrupt or incomplete destination families require
        // explicit operator archival outside the state directory.
        let temporary = unique_temporary_path(destination, "rollback")?;
        observe(RestoreBoundary::BeforeSourceCopy)?;
        let result = (|| {
            copy_closed_image(&backup_image, &temporary)?;
            observe(RestoreBoundary::AfterSourceCopy)?;
            let copied_image = open_validated_image(&temporary, Some(proof.node_id))?;
            let copied = copied_image.summary.clone();
            if copied.slot_id != proof.slot_id
                || copied.epoch != proof.epoch
                || copied.revision != proof.revision
                || copied.cursor != proof.cursor
                || digest_file_handle(
                    copied_image
                        .main_guard
                        .as_ref()
                        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
                )? != proof.digest
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
            let source = close_into_image_traced(copied_image, |event| {
                observe(match event {
                    ImageCloseEvent::CheckpointTruncated => {
                        RestoreBoundary::SourceCheckpointTruncated
                    }
                    ImageCloseEvent::JournalModeDelete => RestoreBoundary::SourceJournalModeDelete,
                    ImageCloseEvent::SqliteClosed => RestoreBoundary::SourceSqliteClosed,
                    ImageCloseEvent::SidecarsAbsent => RestoreBoundary::SourceSidecarsAbsent,
                    ImageCloseEvent::ReadOnlyValidated => RestoreBoundary::SourceReadOnlyValidated,
                })
            })?;
            source
                .main_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
                .sync_all()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
            backup_image.release()?;
            let quiesced_destination = quiesce_destination_after(destination, |event| {
                observe(match event {
                    ImageCloseEvent::CheckpointTruncated => {
                        RestoreBoundary::DestinationCheckpointTruncated
                    }
                    ImageCloseEvent::JournalModeDelete => {
                        RestoreBoundary::DestinationJournalModeDelete
                    }
                    ImageCloseEvent::SqliteClosed => RestoreBoundary::DestinationSqliteClosed,
                    ImageCloseEvent::SidecarsAbsent => RestoreBoundary::DestinationSidecarsAbsent,
                    ImageCloseEvent::ReadOnlyValidated => {
                        RestoreBoundary::DestinationReadOnlyValidated
                    }
                })
            })?;
            let restored = install_closed_image_after(
                source,
                quiesced_destination,
                proof.node_id,
                &mut observe,
            )?;
            if restored.slot_id != proof.slot_id
                || restored.epoch != proof.epoch
                || restored.revision != proof.revision
                || restored.cursor != proof.cursor
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
            Ok(())
        })();
        finish_with_temporary_cleanup(result, &temporary)
    }

    fn checkpoint(&self) -> Result<(), RepositoryError> {
        self.validate_ownership()?;
        let (busy, _log, _checkpointed): (i64, i64, i64) =
            self.connection_ref()?
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy != 0 {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        Ok(())
    }

    fn validate_ownership(&self) -> Result<(), RepositoryError> {
        validate_guarded_image(
            &self.path,
            self.identity,
            self.directory_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            self.family_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            self.main_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        )
    }

    fn publish_migration_backup(&self) -> Result<PathBuf, RepositoryError> {
        let backup = self.migration_backup_path()?;
        let temporary = unique_temporary_path(&backup, "backup")?;
        let result = (|| {
            backup_connection(self.connection_ref()?, &temporary)?;
            let source = close_into_image(open_validated_image(
                &temporary,
                Some(self.expected_node_id),
            )?)?;
            source
                .main_guard
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
                .sync_all()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
            let destination = quiesce_destination_after(&backup, |_| Ok(()))?;
            install_closed_image_after(source, destination, self.expected_node_id, |_| Ok(()))?;
            Ok(backup.clone())
        })();
        finish_with_temporary_cleanup(result, &temporary)
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
                let claim = self
                    .live_claim
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
                let family_guard = self
                    .family_guard
                    .take()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
                release_live_family_after_close(claim, family_guard)?;
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
        repository.family_guard.take(),
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
    strict_source: Option<Option<&ScalarSource>>,
    ids: &mut dyn ControlIdGenerator,
) -> Result<bool, RepositoryError> {
    let has_schema: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
        [],
        |row| row.get(0),
    )?;
    if has_schema {
        if strict_source.is_some_and(|source| source.is_some()) {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        return Ok(false);
    }

    let source = match strict_source {
        Some(Some(source)) => source.durable_text()?,
        Some(None) => return Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
        None => ScalarSource::Fresh.durable_text()?,
    };

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let slot_id = ids.new_slot_id();
    let stream_epoch = ids.new_stream_epoch();
    transaction.execute_batch(SCHEMA_V1)?;
    let applied_at_ms = current_unix_ms()?;
    transaction.execute(
        "INSERT INTO loxa_schema_migrations(version, name, checksum, applied_at_ms) VALUES(?1, ?2, ?3, ?4)",
        (1_i64, MIGRATION_NAME, schema_checksum(), applied_at_ms),
    )?;
    transaction.execute(
        "INSERT INTO control_meta(singleton, node_id, slot_id, stream_epoch, revision, cursor, schema_version, migration_source, last_committed_at_unix_ms) VALUES(1, ?1, ?2, ?3, '1', '1', 1, ?4, ?5)",
        (node_id.to_string(), slot_id.to_string(), stream_epoch.to_string(), source, applied_at_ms.to_string()),
    )?;
    transaction.execute(
        "INSERT INTO node_state(singleton, node_id, node_instance_id, control_endpoint, status, model_download, slot_load, slot_unload, operation_cancel, operation_stream) VALUES(1, ?1, NULL, NULL, 'unpublished', 0, 0, 0, 0, 0)",
        [node_id.to_string()],
    )?;
    transaction.execute(
        "INSERT INTO slot_state(singleton, slot_id, name, status, model_id, operation_id, updated_revision, updated_at_unix_ms) VALUES(1, ?1, 'default', 'unloaded', NULL, NULL, '1', ?2)",
        (slot_id.to_string(), applied_at_ms.to_string()),
    )?;
    let event_id = ids.new_initial_event_id();
    transaction.execute(
        "INSERT INTO events(event_id, stream_epoch, sequence, revision, node_instance_id, v1_sequence, event_kind, payload_json) VALUES(?1, ?2, '1', '1', NULL, NULL, 'initialized', ?3)",
        (
            event_id.to_string(),
            stream_epoch.to_string(),
            slot_event_payload(
                event_id,
                stream_epoch,
                1,
                1,
                u64::try_from(applied_at_ms)
                    .map_err(|_| RepositoryError::new(RepositoryErrorClass::Overflow))?,
                node_id,
                slot_id,
            )?,
        ),
    )?;
    transaction.commit()?;
    Ok(true)
}

fn migrate_to_current_schema(
    connection: &mut Connection,
    path: &Path,
    expected_node_id: NodeId,
    backup_v1: bool,
) -> Result<bool, RepositoryError> {
    let version: i64 = connection
        .query_row(
            "SELECT MAX(version) FROM loxa_schema_migrations",
            [],
            |row| row.get(0),
        )
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    match version {
        1 => {
            validate_connection_for_schema(connection, Some(expected_node_id), 1)?;
            if backup_v1 {
                publish_migration_backup_for_schema(connection, path, expected_node_id, 1, 1)?;
            }
            migrate_v1_to_v2(connection, current_unix_ms_as_u64()?)?;
            Ok(true)
        }
        SCHEMA_VERSION => Ok(false),
        version if version > SCHEMA_VERSION => Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        )),
        _ => Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
}

fn current_unix_ms_as_u64() -> Result<u64, RepositoryError> {
    u64::try_from(current_unix_ms()?)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Overflow))
}

pub(super) fn migrate_v1_to_v2(
    connection: &mut Connection,
    applied_at_ms: u64,
) -> Result<(), RepositoryError> {
    connection.execute_batch("PRAGMA foreign_keys=OFF;")?;
    let foreign_keys: i64 = connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    if foreign_keys != 0 {
        return Err(RepositoryError::new(RepositoryErrorClass::Database));
    }

    let migration = (|| {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        quick_check(&transaction)?;
        validate_migration_ledger_for_version(&transaction, 1)?;
        validate_schema_shape_for_version(&transaction, 1)?;
        let intent = derive_v1_slot_intent_backfill(&transaction)?;
        fail_at_migration_statement_for_test(0)?;

        let slot_id: String = transaction.query_row(
            "SELECT slot_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let desired_kind = desired_kind_text(intent.desired_kind);
        let desired_revision = intent.desired_revision.to_string();
        let operation_id = intent
            .operation_id
            .map(|operation_id| operation_id.to_string());
        let reconciliation = reconciliation_state_text(intent.reconciliation);
        let reason = intent.reason.map(intent_reason_text);
        let checksum = migration_2_checksum();
        let applied_at_ms = i64::try_from(applied_at_ms)
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Overflow))?;

        let statements = MIGRATION_2
            .split_inclusive(';')
            .filter(|statement| statement.ends_with(';'))
            .collect::<Vec<_>>();
        if statements.len() != 11 {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        for (index, sql) in statements.into_iter().enumerate() {
            let mut statement = transaction.prepare(sql)?;
            match statement.parameter_count() {
                0 => {
                    statement.execute([])?;
                }
                7 => {
                    statement.execute(params![
                        &slot_id,
                        desired_kind,
                        intent.desired_model_id.as_deref(),
                        &desired_revision,
                        operation_id.as_deref(),
                        reconciliation,
                        reason,
                    ])?;
                }
                9 => {
                    statement.execute(params![
                        &slot_id,
                        desired_kind,
                        intent.desired_model_id.as_deref(),
                        &desired_revision,
                        operation_id.as_deref(),
                        reconciliation,
                        reason,
                        &checksum,
                        applied_at_ms,
                    ])?;
                }
                _ => return Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
            }
            fail_at_migration_statement_for_test(index + 1)?;
        }
        require_foreign_key_check_clean(&transaction)?;
        validate_connection_for_schema(&transaction, None, SCHEMA_VERSION)?;
        transaction.commit()?;

        let (busy, _log, _checkpointed): (i64, i64, i64) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        Ok(())
    })();

    let restore_foreign_keys = connection.execute_batch("PRAGMA foreign_keys=ON;");
    let restored: Result<i64, SqlError> =
        connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0));
    if restore_foreign_keys.is_err() || restored != Ok(1) {
        return Err(RepositoryError::new(RepositoryErrorClass::Durability));
    }
    migration?;
    validate_connection_for_schema(connection, None, SCHEMA_VERSION)?;
    Ok(())
}

fn derive_v1_slot_intent_backfill(
    transaction: &Transaction<'_>,
) -> Result<StoredSlotIntent, RepositoryError> {
    let control_revision = transaction
        .query_row(
            "SELECT revision FROM control_meta WHERE singleton=1",
            [],
            |row| row.get::<_, String>(0),
        )
        .map_err(classify_missing_row)
        .and_then(|revision| parse_canonical_u64(&revision))?;
    let slot: (String, String, Option<String>, Option<String>, String) = transaction
        .query_row(
            "SELECT slot_id,status,model_id,operation_id,updated_revision FROM slot_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .map_err(classify_missing_row)?;
    let slot_id = SlotId::from_str(&slot.0)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let updated_revision = parse_canonical_u64(&slot.4)?;
    if updated_revision > control_revision {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }

    let operation_id = slot
        .3
        .as_deref()
        .map(OperationId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let operation = match operation_id {
        Some(operation_id) => transaction
            .query_row(
                "SELECT slot_id,kind,status,model_id,created_revision FROM operations WHERE operation_id=?1",
                [operation_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?,
        None => None,
    };
    let retained_operation_id = operation.as_ref().and(operation_id);
    let active = |status: &str| matches!(status, "queued" | "running" | "cancelling");

    match slot.1.as_str() {
        "unloaded" => Ok(StoredSlotIntent {
            desired_kind: DesiredKind::Unloaded,
            desired_model_id: None,
            desired_revision: updated_revision,
            operation_id: None,
            reconciliation: ReconciliationState::Settled,
            reason: None,
        }),
        "ready" => {
            let model_id = slot
                .2
                .filter(|model_id| valid_model_id(model_id))
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
            Ok(StoredSlotIntent {
                desired_kind: DesiredKind::Loaded,
                desired_model_id: Some(model_id),
                desired_revision: updated_revision,
                operation_id: None,
                reconciliation: ReconciliationState::Settled,
                reason: None,
            })
        }
        "loading" => {
            let exact = operation.as_ref().and_then(
                |(operation_slot, kind, status, model_id, created_revision)| {
                    let created_revision = parse_canonical_u64(created_revision).ok()?;
                    (operation_slot == &slot_id.to_string()
                        && kind == "load"
                        && active(status)
                        && model_id.as_deref().is_some_and(valid_model_id)
                        && created_revision >= updated_revision
                        && created_revision <= control_revision)
                        .then(|| (model_id.clone().unwrap(), created_revision))
                },
            );
            if let Some((model_id, created_revision)) = exact {
                Ok(StoredSlotIntent {
                    desired_kind: DesiredKind::Loaded,
                    desired_model_id: Some(model_id),
                    desired_revision: created_revision,
                    operation_id,
                    reconciliation: ReconciliationState::Applying,
                    reason: None,
                })
            } else {
                Ok(StoredSlotIntent {
                    desired_kind: DesiredKind::Unknown,
                    desired_model_id: None,
                    desired_revision: updated_revision,
                    operation_id: retained_operation_id,
                    reconciliation: ReconciliationState::RecoveryRequired,
                    reason: Some(if operation.is_none() {
                        IntentReason::MigrationAmbiguousLoading
                    } else {
                        IntentReason::MigrationOperationMismatch
                    }),
                })
            }
        }
        "unloading" => {
            let exact_revision = operation.as_ref().and_then(
                |(operation_slot, kind, status, _model_id, created_revision)| {
                    let created_revision = parse_canonical_u64(created_revision).ok()?;
                    (operation_slot == &slot_id.to_string()
                        && kind == "unload"
                        && active(status)
                        && created_revision >= updated_revision
                        && created_revision <= control_revision)
                        .then_some(created_revision)
                },
            );
            if let Some(created_revision) = exact_revision {
                Ok(StoredSlotIntent {
                    desired_kind: DesiredKind::Unloaded,
                    desired_model_id: None,
                    desired_revision: created_revision,
                    operation_id,
                    reconciliation: ReconciliationState::Applying,
                    reason: None,
                })
            } else {
                Ok(StoredSlotIntent {
                    desired_kind: DesiredKind::Unknown,
                    desired_model_id: None,
                    desired_revision: updated_revision,
                    operation_id: retained_operation_id,
                    reconciliation: ReconciliationState::RecoveryRequired,
                    reason: Some(IntentReason::MigrationOperationMismatch),
                })
            }
        }
        "recovery" => Ok(StoredSlotIntent {
            desired_kind: DesiredKind::Unknown,
            desired_model_id: None,
            desired_revision: updated_revision,
            operation_id: None,
            reconciliation: ReconciliationState::RecoveryRequired,
            reason: Some(IntentReason::PreexistingRecovery),
        }),
        _ => Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
}

fn require_foreign_key_check_clean(connection: &Connection) -> Result<(), RepositoryError> {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.query([])?.next()?.is_some() {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn desired_kind_text(kind: DesiredKind) -> &'static str {
    match kind {
        DesiredKind::Unloaded => "unloaded",
        DesiredKind::Loaded => "loaded",
        DesiredKind::Unknown => "unknown",
    }
}

fn reconciliation_state_text(state: ReconciliationState) -> &'static str {
    match state {
        ReconciliationState::Settled => "settled",
        ReconciliationState::Applying => "applying",
        ReconciliationState::RecoveryRequired => "recovery_required",
    }
}

fn intent_reason_text(reason: IntentReason) -> &'static str {
    match reason {
        IntentReason::PreexistingRecovery => "preexisting_recovery",
        IntentReason::MigrationAmbiguousLoading => "migration_ambiguous_loading",
        IntentReason::MigrationOperationMismatch => "migration_operation_mismatch",
        IntentReason::ChildEvidenceUncertain => "child_evidence_uncertain",
        IntentReason::CompensationFailed => "compensation_failed",
        IntentReason::DurableCommitUncertain => "durable_commit_uncertain",
    }
}

fn parse_desired_kind(value: &str) -> Result<DesiredKind, RepositoryError> {
    match value {
        "unloaded" => Ok(DesiredKind::Unloaded),
        "loaded" => Ok(DesiredKind::Loaded),
        "unknown" => Ok(DesiredKind::Unknown),
        _ => Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
}

fn parse_reconciliation_state(value: &str) -> Result<ReconciliationState, RepositoryError> {
    match value {
        "settled" => Ok(ReconciliationState::Settled),
        "applying" => Ok(ReconciliationState::Applying),
        "recovery_required" => Ok(ReconciliationState::RecoveryRequired),
        _ => Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
}

fn parse_intent_reason(value: &str) -> Result<IntentReason, RepositoryError> {
    match value {
        "preexisting_recovery" => Ok(IntentReason::PreexistingRecovery),
        "migration_ambiguous_loading" => Ok(IntentReason::MigrationAmbiguousLoading),
        "migration_operation_mismatch" => Ok(IntentReason::MigrationOperationMismatch),
        "child_evidence_uncertain" => Ok(IntentReason::ChildEvidenceUncertain),
        "compensation_failed" => Ok(IntentReason::CompensationFailed),
        "durable_commit_uncertain" => Ok(IntentReason::DurableCommitUncertain),
        _ => Err(RepositoryError::new(RepositoryErrorClass::Corrupt)),
    }
}

fn read_stored_slot_intent(connection: &Connection) -> Result<StoredSlotIntent, RepositoryError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM slot_intent", [], |row| row.get(0))?;
    if count != 1 {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let raw: (
        String,
        Option<String>,
        String,
        Option<String>,
        String,
        Option<String>,
    ) = connection
        .query_row(
            "SELECT desired_kind,desired_model_id,desired_revision,operation_id,reconciliation_state,reason_code FROM slot_intent WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .map_err(classify_missing_row)?;
    Ok(StoredSlotIntent {
        desired_kind: parse_desired_kind(&raw.0)?,
        desired_model_id: raw.1,
        desired_revision: parse_canonical_u64(&raw.2)?,
        operation_id: raw
            .3
            .as_deref()
            .map(OperationId::from_str)
            .transpose()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?,
        reconciliation: parse_reconciliation_state(&raw.4)?,
        reason: raw.5.as_deref().map(parse_intent_reason).transpose()?,
    })
}

fn validate_slot_intent(
    connection: &Connection,
    slot_id: SlotId,
    control_revision: u64,
    slot: &StoredSlotRow,
    intent: &StoredSlotIntent,
    migration_recovery: Option<ExactMigrationRecovery>,
) -> Result<(), RepositoryError> {
    let stored_slot_id: String = connection
        .query_row(
            "SELECT slot_id FROM slot_intent WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(classify_missing_row)?;
    if stored_slot_id != slot_id.to_string() {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    if intent.desired_revision > control_revision
        || intent
            .desired_model_id
            .as_deref()
            .is_some_and(|model_id| !valid_model_id(model_id))
        || (intent.desired_kind == DesiredKind::Loaded) != intent.desired_model_id.is_some()
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    match intent.reconciliation {
        ReconciliationState::Settled => {
            if intent.operation_id.is_some()
                || intent.reason.is_some()
                || intent.desired_kind == DesiredKind::Unknown
                || !settled_intent_matches_observed_slot(intent, slot)?
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
        }
        ReconciliationState::Applying => {
            if intent.operation_id.is_none()
                || intent.reason.is_some()
                || intent.desired_kind == DesiredKind::Unknown
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
            validate_applying_slot_intent(connection, slot_id, intent, slot)?;
        }
        ReconciliationState::RecoveryRequired => {
            if intent.reason.is_none()
                || (matches!(
                    intent.reason,
                    Some(
                        IntentReason::MigrationAmbiguousLoading
                            | IntentReason::MigrationOperationMismatch
                    )
                ) && migration_recovery.is_none())
            {
                return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactMigrationRecovery {
    AmbiguousLoading,
    OperationMismatch {
        retained_operation_id: Option<OperationId>,
    },
}

impl ExactMigrationRecovery {
    fn retained_operation_id(self) -> Option<OperationId> {
        match self {
            Self::AmbiguousLoading => None,
            Self::OperationMismatch {
                retained_operation_id,
            } => retained_operation_id,
        }
    }
}

fn exact_migration_recovery(
    connection: &Connection,
    control_revision: u64,
    slot: &StoredSlotRow,
    intent: &StoredSlotIntent,
) -> Result<Option<ExactMigrationRecovery>, RepositoryError> {
    let slot_revision = parse_canonical_u64(&slot.updated_revision)?;
    if intent.reconciliation != ReconciliationState::RecoveryRequired
        || intent.desired_kind != DesiredKind::Unknown
        || intent.desired_model_id.is_some()
        || intent.desired_revision != slot_revision
        || !matches!(slot.status.as_str(), "loading" | "unloading")
    {
        return Ok(None);
    }

    let slot_operation_id = slot
        .operation_id
        .as_deref()
        .map(OperationId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let operation: Option<(String, String, String, Option<String>, String)> = connection
        .query_row(
            "SELECT slot_id,kind,status,model_id,created_revision FROM operations WHERE operation_id=?1",
            [slot_operation_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    let retained_operation_id = operation.as_ref().map(|_| slot_operation_id);
    if intent.operation_id != retained_operation_id {
        return Ok(None);
    }

    match intent.reason {
        Some(IntentReason::MigrationAmbiguousLoading)
            if slot.status == "loading" && operation.is_none() =>
        {
            Ok(Some(ExactMigrationRecovery::AmbiguousLoading))
        }
        Some(IntentReason::MigrationOperationMismatch) => {
            let exact_correlation = operation.as_ref().is_some_and(
                |(operation_slot, kind, status, model_id, created_revision)| {
                    let Ok(created_revision) = parse_canonical_u64(created_revision) else {
                        return false;
                    };
                    let active = matches!(status.as_str(), "queued" | "running" | "cancelling");
                    let disposition_matches = match slot.status.as_str() {
                        "loading" => {
                            kind == "load" && model_id.as_deref().is_some_and(valid_model_id)
                        }
                        "unloading" => kind == "unload",
                        _ => false,
                    };
                    operation_slot == &slot.slot_id
                        && active
                        && disposition_matches
                        && created_revision >= slot_revision
                        && created_revision <= control_revision
                },
            );
            let missing_operation_is_mismatch = slot.status == "unloading" && operation.is_none();
            if !exact_correlation && (operation.is_some() || missing_operation_is_mismatch) {
                Ok(Some(ExactMigrationRecovery::OperationMismatch {
                    retained_operation_id,
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn settled_intent_matches_observed_slot(
    intent: &StoredSlotIntent,
    slot: &StoredSlotRow,
) -> Result<bool, RepositoryError> {
    let slot_revision = parse_canonical_u64(&slot.updated_revision)?;
    if intent.desired_revision != slot_revision {
        return Ok(false);
    }
    Ok(match slot.status.as_str() {
        "unloaded" => intent.desired_kind == DesiredKind::Unloaded,
        "ready" => {
            intent.desired_kind == DesiredKind::Loaded
                && intent.desired_model_id.as_deref() == slot.model_id.as_deref()
        }
        _ => false,
    })
}

fn validate_applying_slot_intent(
    connection: &Connection,
    slot_id: SlotId,
    intent: &StoredSlotIntent,
    slot: &StoredSlotRow,
) -> Result<(), RepositoryError> {
    let operation_id = intent
        .operation_id
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let slot_operation_id = slot
        .operation_id
        .as_deref()
        .map(OperationId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let operation: (String, String, Option<String>, String, String) = connection
        .query_row(
            "SELECT slot_id,kind,model_id,status,created_revision FROM operations WHERE operation_id=?1",
            [operation_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .map_err(classify_missing_row)?;
    let created_revision = parse_canonical_u64(&operation.4)?;
    let slot_revision = parse_canonical_u64(&slot.updated_revision)?;
    let active = matches!(operation.3.as_str(), "queued" | "running" | "cancelling");
    let disposition_matches = match intent.desired_kind {
        DesiredKind::Loaded => {
            slot.status == "loading"
                && operation.1 == "load"
                && operation.2.as_deref() == intent.desired_model_id.as_deref()
        }
        DesiredKind::Unloaded => {
            slot.status == "unloading" && operation.1 == "unload" && operation.2.is_none()
        }
        DesiredKind::Unknown => false,
    };
    if slot_operation_id != Some(operation_id)
        || operation.0 != slot_id.to_string()
        || !active
        || created_revision != intent.desired_revision
        || intent.desired_revision < slot_revision
        || !disposition_matches
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn validate_connection(
    connection: &Connection,
    expected_node_id: Option<NodeId>,
) -> Result<ValidationSummary, RepositoryError> {
    validate_connection_for_schema(connection, expected_node_id, SCHEMA_VERSION)
}

fn validate_connection_for_schema(
    connection: &Connection,
    expected_node_id: Option<NodeId>,
    expected_schema_version: i64,
) -> Result<ValidationSummary, RepositoryError> {
    quick_check(connection)?;
    require_foreign_key_check_clean(connection)?;
    reject_newer_migration_version(connection)?;
    validate_schema_shape_for_version(connection, expected_schema_version)?;
    validate_migration_ledger_for_version(connection, expected_schema_version)?;

    let raw_meta: (String, String, String, String, String, i64, String, String) = connection
        .query_row(
            "SELECT node_id, slot_id, stream_epoch, revision, cursor, schema_version, migration_source, last_committed_at_unix_ms FROM control_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
        )
        .map_err(classify_missing_row)?;
    if raw_meta.5 != expected_schema_version {
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
    validate_migration_source(&raw_meta.6)?;
    let last_committed_at = parse_canonical_u64(&raw_meta.7)?;

    let node_rows = count_rows(connection, "node_state")?;
    let slot_rows = count_rows(connection, "slot_state")?;
    let event_rows = count_rows(connection, "events")?;
    if node_rows != 1 || slot_rows != 1 || event_rows == 0 {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let stored_node: (String, Option<String>, Option<String>, String, i64, i64, i64, i64, i64) = connection.query_row(
        "SELECT node_id, node_instance_id, control_endpoint, status, model_download, slot_load, slot_unload, operation_cancel, operation_stream FROM node_state WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?)),
    )?;
    if stored_node.0 != node_id.to_string()
        || ![
            stored_node.4,
            stored_node.5,
            stored_node.6,
            stored_node.7,
            stored_node.8,
        ]
        .into_iter()
        .all(|value| matches!(value, 0 | 1))
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    if stored_node.3 == "unpublished"
        && [
            stored_node.4,
            stored_node.5,
            stored_node.6,
            stored_node.7,
            stored_node.8,
        ] != [0, 0, 0, 0, 0]
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
    let stored_slot: StoredSlotRow = connection.query_row(
        "SELECT slot_id, name, status, model_id, operation_id, error_code, error_message, updated_revision, updated_at_unix_ms FROM slot_state WHERE singleton = 1",
        [],
        |row| Ok(StoredSlotRow { slot_id: row.get(0)?, name: row.get(1)?, status: row.get(2)?, model_id: row.get(3)?, operation_id: row.get(4)?, error_code: row.get(5)?, error_message: row.get(6)?, updated_revision: row.get(7)?, updated_at_unix_ms: row.get(8)? }),
    )?;
    let slot_operation_id = stored_slot
        .operation_id
        .as_deref()
        .map(OperationId::from_str)
        .transpose()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let slot_state_valid = match stored_slot.status.as_str() {
        "unloaded" => stored_slot.model_id.is_none() && slot_operation_id.is_none(),
        "loading" => slot_operation_id.is_some(),
        "ready" => {
            stored_slot.model_id.as_deref().is_some_and(valid_model_id)
                && slot_operation_id.is_none()
        }
        "unloading" => {
            stored_slot.model_id.as_deref().is_some_and(valid_model_id)
                && slot_operation_id.is_some()
        }
        "recovery" => {
            slot_operation_id.is_none()
                && stored_slot.error_code.as_deref() == Some("lifecycle_recovery_required")
                && stored_slot
                    .error_message
                    .as_deref()
                    .is_some_and(|message| valid_bounded_text(message, 256))
        }
        _ => false,
    };
    if stored_slot.slot_id != slot_id.to_string()
        || stored_slot.name != "default"
        || !slot_state_valid
        || stored_slot
            .model_id
            .as_deref()
            .is_some_and(|model| !valid_model_id(model))
        || (stored_slot.status != "recovery"
            && (stored_slot.error_code.is_some() || stored_slot.error_message.is_some()))
        || parse_canonical_u64(&stored_slot.updated_revision)? > revision
        || parse_canonical_u64(&stored_slot.updated_at_unix_ms)? > last_committed_at
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let stored_intent = (expected_schema_version == SCHEMA_VERSION)
        .then(|| read_stored_slot_intent(connection))
        .transpose()?;
    let migration_recovery = stored_intent
        .as_ref()
        .map(|intent| exact_migration_recovery(connection, revision, &stored_slot, intent))
        .transpose()?
        .flatten();
    validate_operation_rows(
        connection,
        node_id,
        slot_id,
        revision,
        last_committed_at,
        expected_schema_version,
        migration_recovery,
    )?;
    if expected_schema_version == SCHEMA_VERSION {
        validate_slot_operation_reference(
            connection,
            stored_slot.status.as_str(),
            slot_operation_id,
            migration_recovery.is_some(),
        )?;
    }
    validate_event_rows(
        connection,
        node_id,
        slot_id,
        epoch,
        revision,
        cursor,
        last_committed_at,
    )?;
    if expected_schema_version == SCHEMA_VERSION {
        validate_slot_intent(
            connection,
            slot_id,
            revision,
            &stored_slot,
            stored_intent
                .as_ref()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Corrupt))?,
            migration_recovery,
        )?;
    }
    Ok(ValidationSummary {
        node_rows,
        slot_rows,
        slot_name: stored_slot.name,
        revision,
        cursor,
        event_rows,
        node_id,
        slot_id,
        epoch,
    })
}

fn validate_migration_source(value: &str) -> Result<(), RepositoryError> {
    if value == "fresh" {
        return Ok(());
    }
    let Some(json) = value.strip_prefix("prior_dead_childless_model_free_unloaded_v4:") else {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    };
    let provenance: ScalarProvenance = serde_json::from_str(json)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    if provenance.schema_version != 4 || provenance.run_id.is_empty() {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn validate_schema_shape_for_version(
    connection: &Connection,
    expected_schema_version: i64,
) -> Result<(), RepositoryError> {
    let expected_connection = Connection::open_in_memory()?;
    expected_connection.execute_batch(SCHEMA_V1)?;
    if expected_schema_version == SCHEMA_VERSION {
        for sql in MIGRATION_2
            .split_inclusive(';')
            .filter(|statement| statement.ends_with(';'))
        {
            let mut statement = expected_connection.prepare(sql)?;
            if statement.parameter_count() != 0 {
                break;
            }
            statement.execute([])?;
        }
    } else if expected_schema_version != 1 {
        return Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        ));
    }
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

fn validate_migration_ledger_for_version(
    connection: &Connection,
    expected_schema_version: i64,
) -> Result<(), RepositoryError> {
    let rows = count_rows(connection, "loxa_schema_migrations")?;
    let expected_rows = usize::try_from(expected_schema_version)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsupportedSchema))?;
    if rows != expected_rows {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    let mut statement = connection
        .prepare("SELECT version,name,checksum FROM loxa_schema_migrations ORDER BY version")?;
    let ledger = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut expected = vec![(1, MIGRATION_NAME.to_owned(), schema_checksum())];
    if expected_schema_version == SCHEMA_VERSION {
        expected.push((
            SCHEMA_VERSION,
            MIGRATION_2_NAME.to_owned(),
            migration_2_checksum(),
        ));
    }
    if ledger != expected {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    Ok(())
}

fn reject_newer_migration_version(connection: &Connection) -> Result<(), RepositoryError> {
    let maximum: i64 = connection
        .query_row(
            "SELECT MAX(version) FROM loxa_schema_migrations",
            [],
            |row| row.get(0),
        )
        .map_err(classify_missing_row)?;
    if maximum > SCHEMA_VERSION {
        Err(RepositoryError::new(
            RepositoryErrorClass::UnsupportedSchema,
        ))
    } else {
        Ok(())
    }
}

fn validate_operation_rows(
    connection: &Connection,
    node_id: NodeId,
    slot_id: SlotId,
    revision: u64,
    last_committed_at: u64,
    expected_schema_version: i64,
    migration_recovery: Option<ExactMigrationRecovery>,
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
            (Some(current), None) => Some(V2OperationProgress {
                completed_bytes: DecimalU64::new(parse_canonical_u64(&current)?),
                total_bytes: None,
            }),
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
        let migration_recovery_load = if kind == V2OperationKind::Load
            && operation.model_id.is_none()
            && (expected_schema_version == 1
                || migration_recovery.and_then(ExactMigrationRecovery::retained_operation_id)
                    == Some(operation_id))
        {
            let mut with_validation_target = operation.clone();
            with_validation_target.model_id = Some("migration-validation-probe".to_owned());
            with_validation_target.validate().is_ok()
        } else {
            false
        };
        if stored_slot_id != slot_id
            || v1_ordinal.is_some_and(|ordinal| ordinal < 1)
            || (operation.validate().is_err() && !migration_recovery_load)
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

fn validate_slot_operation_reference(
    connection: &Connection,
    slot_status: &str,
    operation_id: Option<OperationId>,
    allow_migration_mismatch: bool,
) -> Result<(), RepositoryError> {
    let Some(operation_id) = operation_id else {
        if matches!(slot_status, "loading" | "unloading") && !allow_migration_mismatch {
            return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
        }
        return Ok(());
    };
    let operation: Option<(String, String)> = connection
        .query_row(
            "SELECT kind,status FROM operations WHERE operation_id=?1",
            [operation_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let valid = operation.is_some_and(|(kind, status)| {
        matches!(status.as_str(), "queued" | "running" | "cancelling")
            && matches!(
                (slot_status, kind.as_str()),
                ("loading", "load") | ("unloading", "unload")
            )
    });
    if !valid && !allow_migration_mismatch {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
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
        let event_id = EventId::from_str(&row.get::<_, String>(0)?)
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
            EventRowIdentity {
                event_id,
                epoch: row_epoch,
                sequence,
                revision: event_revision,
                v1_sequence,
            },
            node_id,
            slot_id,
            parsed_node_instance,
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

#[derive(Clone, Copy)]
struct EventRowIdentity {
    event_id: EventId,
    epoch: StreamEpoch,
    sequence: u64,
    revision: u64,
    v1_sequence: Option<i64>,
}

fn validate_event_payload(
    event_kind: &str,
    payload_json: &str,
    row: EventRowIdentity,
    node_id: NodeId,
    slot_id: SlotId,
    node_instance_id: Option<NodeInstanceId>,
    last_committed_at: u64,
) -> Result<(), RepositoryError> {
    let event: V2ControlEvent = serde_json::from_str(payload_json)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Corrupt))?;
    let expected_kind = match event.entity {
        V2EventEntity::Node => "node_changed",
        V2EventEntity::Slot => {
            if row.sequence == 1 {
                "initialized"
            } else {
                "slot_changed"
            }
        }
        V2EventEntity::Operation => "operation_changed",
    };
    let operation_correlation_valid = event.operation.as_ref().is_none_or(|operation| {
        operation.updated_revision.get() == row.revision
            && operation.updated_at_unix_ms == event.committed_at_unix_ms
    });
    let instance_sequence_correlation_valid = match event.entity {
        V2EventEntity::Operation => node_instance_id.is_some() && row.v1_sequence.is_some(),
        V2EventEntity::Node | V2EventEntity::Slot => row.v1_sequence.is_none(),
    };
    if event_kind != expected_kind
        || event.event_id != row.event_id
        || event.epoch != row.epoch
        || event.sequence.get() != row.sequence
        || event.revision.get() != row.revision
        || event.node_id != node_id
        || event.node_instance_id != node_instance_id
        || event.slot_id.is_some_and(|value| value != slot_id)
        || event.committed_at_unix_ms.get() > last_committed_at
        || !operation_correlation_valid
        || !instance_sequence_correlation_valid
        || event.validate().is_err()
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
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

fn rotate_lineage_after(
    path: &Path,
    node_id: NodeId,
    observe: impl FnMut(ImageCloseEvent) -> Result<(), RepositoryError>,
) -> Result<ClosedImage, RepositoryError> {
    let mut image = open_validated_image(path, Some(node_id))?;
    let connection = image
        .connection
        .as_mut()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let summary = image.summary.clone();
    let next_revision = summary
        .revision
        .checked_add(1)
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Overflow))?;
    let new_epoch = StreamEpoch::new_v4();
    let event_id = EventId::new_v4();
    let committed_at = u64::try_from(current_unix_ms()?)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Overflow))?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute("DELETE FROM events", [])?;
    transaction.execute(
        "UPDATE control_meta SET stream_epoch = ?1, revision = ?2, cursor = '1', last_committed_at_unix_ms = ?3 WHERE singleton = 1",
        (
            new_epoch.to_string(),
            next_revision.to_string(),
            committed_at.max(summary.revision).to_string(),
        ),
    )?;
    transaction.execute(
        "INSERT INTO events(event_id, stream_epoch, sequence, revision, node_instance_id, v1_sequence, event_kind, payload_json) VALUES(?1, ?2, '1', ?3, NULL, NULL, 'initialized', ?4)",
        (
            event_id.to_string(),
            new_epoch.to_string(),
            next_revision.to_string(),
            slot_event_payload(
                event_id,
                new_epoch,
                1,
                next_revision,
                committed_at,
                node_id,
                summary.slot_id,
            )?,
        ),
    )?;
    transaction.commit()?;
    image.summary = validate_connection(connection, Some(node_id))?;
    close_into_image_traced(image, observe)
}

fn slot_event_payload(
    event_id: EventId,
    epoch: StreamEpoch,
    sequence: u64,
    revision: u64,
    committed_at_unix_ms: u64,
    node_id: NodeId,
    slot_id: SlotId,
) -> Result<String, RepositoryError> {
    let slot = V2Slot {
        slot_id,
        node_id,
        name: "default".to_owned(),
        status: V2SlotStatus::Unloaded,
        model_id: None,
        operation_id: None,
        error: None,
    };
    serde_json::to_string(&V2ControlEvent {
        schema_version: V2_SCHEMA_VERSION,
        event_id,
        epoch,
        sequence: DecimalU64::new(sequence),
        revision: DecimalU64::new(revision),
        committed_at_unix_ms: DecimalU64::new(committed_at_unix_ms),
        entity: V2EventEntity::Slot,
        entity_id: slot_id.to_string(),
        node_id,
        node_instance_id: None,
        slot_id: Some(slot_id),
        operation_id: None,
        node: None,
        slot: Some(slot),
        operation: None,
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
    validate_database_file_for_schema(path, expected_node_id, SCHEMA_VERSION)
}

fn validate_database_file_for_schema(
    path: &Path,
    expected_node_id: Option<NodeId>,
    expected_schema_version: i64,
) -> Result<ValidationSummary, RepositoryError> {
    let prepared = prepare_existing_storage_path(path)?;
    let connection = open_validated_connection_after(prepared, true, || {})?;
    let validation = (|| {
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        apply_limits(&connection)?;
        validate_connection_for_schema(&connection, expected_node_id, expected_schema_version)
    })();
    let close = connection.close();
    let summary = validation?;
    close?;
    Ok(summary)
}

fn sync_and_reopen_validate_migration(
    opened: &ValidatedConnection,
    path: &Path,
    expected_node_id: NodeId,
    expected_summary: &ValidationSummary,
) -> Result<(), RepositoryError> {
    validate_guarded_image(
        path,
        opened.identity,
        opened
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        opened
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        opened
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )?;
    opened
        .main_guard
        .as_ref()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
        .sync_all()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    opened
        .directory_guard
        .as_ref()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
        .sync_all()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;

    let spec = ConnectionOpenSpec::for_existing(path.to_owned(), true)?;
    let connection =
        Connection::open_with_flags_and_vfs(path, spec.flags, spec.vfs).map_err(map_sql_error)?;
    let validation = (|| {
        if connected_vfs_name(&connection)? != spec.vfs {
            return Err(RepositoryError::new(RepositoryErrorClass::Database));
        }
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        apply_limits(&connection)?;
        validate_connection(&connection, Some(expected_node_id))
    })();
    let close = connection
        .close()
        .map_err(|(_, error)| map_sql_error(error));
    let summary = validation?;
    close?;
    if &summary != expected_summary {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    validate_guarded_image(
        path,
        opened.identity,
        opened
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        opened
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        opened
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )
}

fn ensure_current_migration_backup(
    opened: &ValidatedConnection,
    path: &Path,
    expected_node_id: NodeId,
    expected_summary: &ValidationSummary,
) -> Result<(), RepositoryError> {
    let backup = migration_backup_path(path)?;
    let destination_schema_version = match fs::symlink_metadata(&backup) {
        Ok(_) => {
            match validate_database_file_for_schema(&backup, Some(expected_node_id), SCHEMA_VERSION)
            {
                Ok(_) => return Ok(()),
                Err(current_error) => {
                    let legacy_validation =
                        validate_database_file_for_schema(&backup, Some(expected_node_id), 1);
                    if legacy_validation.is_err() {
                        return Err(current_error);
                    }
                    1
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => SCHEMA_VERSION,
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    };

    let (busy, _log, _checkpointed): (i64, i64, i64) =
        opened.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 {
        return Err(RepositoryError::new(RepositoryErrorClass::Durability));
    }
    sync_and_reopen_validate_migration(opened, path, expected_node_id, expected_summary)?;
    publish_migration_backup_for_schema(
        opened,
        path,
        expected_node_id,
        SCHEMA_VERSION,
        destination_schema_version,
    )
}

fn publish_migration_backup_for_schema(
    connection: &Connection,
    path: &Path,
    expected_node_id: NodeId,
    source_schema_version: i64,
    destination_schema_version: i64,
) -> Result<(), RepositoryError> {
    let backup = migration_backup_path(path)?;
    let temporary = unique_temporary_path(&backup, "backup")?;
    let result = (|| {
        backup_connection(connection, &temporary)?;
        let source = close_into_image(open_validated_image_for_schema(
            &temporary,
            Some(expected_node_id),
            source_schema_version,
        )?)?;
        source
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
            .sync_all()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
        let destination =
            quiesce_destination_for_schema_after(&backup, destination_schema_version, |_| Ok(()))?;
        install_closed_image_after(source, destination, expected_node_id, |_| Ok(()))?;
        Ok(())
    })();
    finish_with_temporary_cleanup(result, &temporary)
}

fn open_validated_image(
    path: &Path,
    expected_node_id: Option<NodeId>,
) -> Result<OpenValidatedImage, RepositoryError> {
    open_validated_image_for_schema(path, expected_node_id, SCHEMA_VERSION)
}

fn open_validated_image_for_schema(
    path: &Path,
    expected_node_id: Option<NodeId>,
    schema_version: i64,
) -> Result<OpenValidatedImage, RepositoryError> {
    let prepared = prepare_existing_storage_path(path)?;
    let canonical_path = prepared.canonical_path.clone();
    let mut opened = open_validated_connection_after(prepared, false, || {})?;
    configure_for_offline_mutation(&opened)?;
    let summary = validate_connection_for_schema(&opened, expected_node_id, schema_version)?;
    Ok(OpenValidatedImage {
        path: Some(canonical_path),
        identity: opened.identity,
        connection: opened.connection.take(),
        directory_guard: opened.directory_guard.take(),
        family_guard: opened.family_guard.take(),
        main_guard: opened.main_guard.take(),
        live_claim: opened.live_claim.take(),
        summary,
        schema_version,
    })
}

fn require_checkpoint_not_busy(
    connection: &Connection,
    pragma: &'static str,
) -> Result<(), RepositoryError> {
    let (busy, _log, _checkpointed): (i64, i64, i64) = connection.query_row(pragma, [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?;
    if busy == 0 {
        Ok(())
    } else {
        Err(RepositoryError::new(RepositoryErrorClass::Durability))
    }
}

fn require_delete_journal_mode(connection: &Connection) -> Result<(), RepositoryError> {
    let returned: String =
        connection.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
    if returned.eq_ignore_ascii_case("delete") {
        Ok(())
    } else {
        Err(RepositoryError::new(RepositoryErrorClass::Durability))
    }
}

fn validate_guarded_image(
    path: &Path,
    identity: FileIdentity,
    directory_guard: &fs::File,
    family_guard: &fs::File,
    main_guard: &fs::File,
) -> Result<(), RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_identity = file_identity(
        &directory_guard
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, directory_guard)?;
    validate_family_guard(path, family_guard)?;
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let guard_metadata = main_guard
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(&path_metadata)?;
    validate_file_metadata(&guard_metadata)?;
    if file_identity(&path_metadata)? != identity || file_identity(&guard_metadata)? != identity {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

fn close_into_image(image: OpenValidatedImage) -> Result<ClosedImage, RepositoryError> {
    close_into_image_traced(image, |_| Ok(()))
}

fn close_into_image_traced(
    mut image: OpenValidatedImage,
    mut observe: impl FnMut(ImageCloseEvent) -> Result<(), RepositoryError>,
) -> Result<ClosedImage, RepositoryError> {
    let connection = image
        .connection
        .as_ref()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    require_checkpoint_not_busy(connection, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    observe(ImageCloseEvent::CheckpointTruncated)?;
    require_delete_journal_mode(connection)?;
    observe(ImageCloseEvent::JournalModeDelete)?;
    let connection = image
        .connection
        .take()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    if let Err((connection, error)) = connection.close() {
        image.connection = Some(connection);
        return Err(retain_poisoned_owner(
            image.connection.take(),
            image.main_guard.take(),
            image.directory_guard.take(),
            image.family_guard.take(),
            image
                .live_claim
                .take()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            CloseUncertainty::CheckpointOrClose,
            map_sql_error(error),
        ));
    }
    if let Err(error) = observe(ImageCloseEvent::SqliteClosed) {
        return Err(retain_poisoned_owner(
            image.connection.take(),
            image.main_guard.take(),
            image.directory_guard.take(),
            image.family_guard.take(),
            image
                .live_claim
                .take()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            CloseUncertainty::CheckpointOrClose,
            error,
        ));
    }
    let reservation = image
        .live_claim
        .as_mut()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
        .transition_to_quarantined()?;
    let _former_live_claim = image.live_claim.take();
    let mut closed = ClosedImage {
        path: image.path.take(),
        identity: image.identity,
        directory_guard: image.directory_guard.take(),
        family_guard: image.family_guard.take(),
        main_guard: image.main_guard.take(),
        reservation: Some(reservation),
        summary: image.summary.clone(),
        schema_version: image.schema_version,
    };
    let path = closed.path()?.to_owned();
    if let Err(error) = validate_guarded_image(
        &path,
        closed.identity,
        closed
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        closed
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        closed
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    ) {
        return Err(quarantine_closed_image_until_exit(&mut closed, error));
    }
    if let Err(error) = ensure_auxiliary_files_absent(&path) {
        return Err(quarantine_closed_image_until_exit(&mut closed, error));
    }
    if let Err(error) = observe(ImageCloseEvent::SidecarsAbsent) {
        return Err(quarantine_closed_image_until_exit(&mut closed, error));
    }
    validate_closed_image_read_only(&mut closed)?;
    if let Err(error) = observe(ImageCloseEvent::ReadOnlyValidated) {
        return Err(quarantine_closed_image_until_exit(&mut closed, error));
    }
    Ok(closed)
}

fn close_into_destination_traced(
    image: OpenValidatedImage,
    observe: impl FnMut(ImageCloseEvent) -> Result<(), RepositoryError>,
) -> Result<QuiescedDestination, RepositoryError> {
    close_into_image_traced(image, observe).map(QuiescedDestination::Existing)
}

fn quiesce_destination_after(
    path: &Path,
    observe: impl FnMut(ImageCloseEvent) -> Result<(), RepositoryError>,
) -> Result<QuiescedDestination, RepositoryError> {
    quiesce_destination_for_schema_after(path, SCHEMA_VERSION, observe)
}

fn quiesce_destination_for_schema_after(
    path: &Path,
    schema_version: i64,
    observe: impl FnMut(ImageCloseEvent) -> Result<(), RepositoryError>,
) -> Result<QuiescedDestination, RepositoryError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            ensure_auxiliary_files_absent(path)?;
            close_into_destination_traced(
                open_validated_image_for_schema(path, None, schema_version)?,
                observe,
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = prepare_destination_parent(path)?;
            let directory_guard = open_secure_directory(&parent, false)?;
            let canonical_parent = fs::canonicalize(&parent)
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            let directory_identity = file_identity(
                &directory_guard
                    .metadata()
                    .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
            )?;
            validate_bound_directory_continuity(
                &canonical_parent,
                directory_identity,
                &directory_guard,
            )?;
            let family_guard = open_family_guard(path)?;
            ensure_auxiliary_files_absent(path)?;
            if fs::symlink_metadata(path).is_ok() {
                return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
            }
            Ok(QuiescedDestination::Vacant(GuardedVacantDestination {
                path: path.to_owned(),
                canonical_parent,
                directory_identity,
                directory_guard,
                family_guard: Some(family_guard),
            }))
        }
        Err(_) => Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
    }
}

fn validate_quiesced_destination(destination: &QuiescedDestination) -> Result<(), RepositoryError> {
    match destination {
        QuiescedDestination::Existing(image) => {
            let path = image.path()?;
            validate_guarded_image(
                path,
                image.identity,
                image
                    .directory_guard
                    .as_ref()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
                image
                    .family_guard
                    .as_ref()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
                image
                    .main_guard
                    .as_ref()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            )?;
            ensure_auxiliary_files_absent(path)
        }
        QuiescedDestination::Vacant(vacant) => {
            validate_bound_directory_continuity(
                &vacant.canonical_parent,
                vacant.directory_identity,
                &vacant.directory_guard,
            )?;
            validate_family_guard(
                &vacant.path,
                vacant
                    .family_guard
                    .as_ref()
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
            )?;
            ensure_auxiliary_files_absent(&vacant.path)?;
            match fs::symlink_metadata(&vacant.path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                _ => Err(RepositoryError::new(RepositoryErrorClass::UnsafePath)),
            }
        }
    }
}

fn transfer_destination_family_guard_after_rename(
    source: &mut ClosedImage,
    destination: &mut QuiescedDestination,
    source_path: &Path,
) -> Result<(), RepositoryError> {
    let destination_family_guard = match destination {
        QuiescedDestination::Existing(image) => image.family_guard.take(),
        QuiescedDestination::Vacant(vacant) => vacant.family_guard.take(),
    }
    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let source_family_guard = source
        .family_guard
        .replace(destination_family_guard)
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    remove_guarded_family_lock(source_path, source_family_guard)
}

fn install_closed_image_after(
    mut source: ClosedImage,
    mut destination: QuiescedDestination,
    expected_node_id: NodeId,
    mut observe: impl FnMut(RestoreBoundary) -> Result<(), RepositoryError>,
) -> Result<ValidationSummary, RepositoryError> {
    let source_schema_version = source.schema_version;
    let source_path = source.path()?.to_owned();
    let destination_path = match &destination {
        QuiescedDestination::Existing(image) => image.path()?.to_owned(),
        QuiescedDestination::Vacant(vacant) => vacant.path.clone(),
    };
    let destination_expectation = match &destination {
        QuiescedDestination::Existing(image) => {
            DestinationInstallExpectation::Existing(image.identity)
        }
        QuiescedDestination::Vacant(_) => DestinationInstallExpectation::Vacant,
    };
    if source_path.parent() != destination_path.parent() {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    validate_guarded_image(
        &source_path,
        source.identity,
        source
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        source
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        source
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )?;
    ensure_auxiliary_files_absent(&source_path)?;
    validate_quiesced_destination(&destination)?;

    observe(RestoreBoundary::BeforeRename)?;
    if let Err(failure) = atomic_install_closed_image(
        &source_path,
        source.identity,
        &destination_path,
        destination_expectation,
    ) {
        if !failure.renamed {
            return Err(failure.error);
        }
        if let Err(transfer_error) = transfer_destination_family_guard_after_rename(
            &mut source,
            &mut destination,
            &source_path,
        ) {
            return Err(quarantine_closed_image_until_exit(
                &mut source,
                transfer_error,
            ));
        }
        return Err(quarantine_closed_image_until_exit(
            &mut source,
            failure.error,
        ));
    }

    // The destination lock protects the logical database family across the
    // pathname replacement. The source lock protected only the temporary
    // pathname and must not be mistaken for destination authority afterward.
    if let Err(error) =
        transfer_destination_family_guard_after_rename(&mut source, &mut destination, &source_path)
    {
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }
    if let Err(error) = observe(RestoreBoundary::AfterRename) {
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }
    let Some(parent) = destination_path.parent() else {
        let error = RepositoryError::new(RepositoryErrorClass::UnsafePath);
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    };
    if let Err(error) = sync_directory_path(parent) {
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }
    if let Err(error) = observe(RestoreBoundary::AfterDirectorySync) {
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }
    let installed = match fs::symlink_metadata(&destination_path) {
        Ok(metadata) => metadata,
        Err(_) => {
            let error = RepositoryError::new(RepositoryErrorClass::UnsafePath);
            return Err(quarantine_closed_image_until_exit(&mut source, error));
        }
    };
    let installed_identity = match file_identity(&installed) {
        Ok(identity) => identity,
        Err(error) => return Err(quarantine_closed_image_until_exit(&mut source, error)),
    };
    if installed_identity != source.identity {
        let error = RepositoryError::new(RepositoryErrorClass::Durability);
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }
    if let Err(error) = ensure_auxiliary_files_absent(&destination_path) {
        return Err(quarantine_closed_image_until_exit(&mut source, error));
    }

    // The obsolete destination inode is now unreachable. Close its guard before
    // releasing the token-checked reservation.
    if let QuiescedDestination::Existing(image) = &mut destination {
        drop(image.main_guard.take());
        drop(image.directory_guard.take());
        let Some(reservation) = image.reservation.take() else {
            let error = RepositoryError::new(RepositoryErrorClass::Durability);
            return Err(quarantine_closed_image_until_exit(&mut source, error));
        };
        let mut reservation = reservation;
        let release = reservation.release_after_guards_closed();
        if let Err(error) = release {
            return Err(quarantine_closed_image_until_exit(&mut source, error));
        }
    }

    let spec = match ConnectionOpenSpec::for_existing(destination_path.clone(), true) {
        Ok(spec) => spec,
        Err(error) => return Err(quarantine_closed_image_until_exit(&mut source, error)),
    };
    let connection =
        match Connection::open_with_flags_and_vfs(&destination_path, spec.flags, spec.vfs) {
            Ok(connection) => connection,
            Err(error) => {
                let error = map_sql_error(error);
                return Err(quarantine_closed_image_until_exit(&mut source, error));
            }
        };
    let connected_vfs = match connected_vfs_name(&connection) {
        Ok(vfs) => vfs,
        Err(error) => {
            return Err(quarantine_closed_image_connection_until_exit(
                &mut source,
                connection,
                error,
            ));
        }
    };
    if connected_vfs != spec.vfs {
        let error = RepositoryError::new(RepositoryErrorClass::Database);
        return Err(quarantine_closed_image_connection_until_exit(
            &mut source,
            connection,
            error,
        ));
    }
    if let Err(error) = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true) {
        let error = map_sql_error(error);
        return Err(quarantine_closed_image_connection_until_exit(
            &mut source,
            connection,
            error,
        ));
    }
    if let Err(error) = connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;") {
        let error = map_sql_error(error);
        return Err(quarantine_closed_image_connection_until_exit(
            &mut source,
            connection,
            error,
        ));
    }
    if let Err(error) = apply_limits(&connection) {
        return Err(quarantine_closed_image_connection_until_exit(
            &mut source,
            connection,
            error,
        ));
    }
    let summary = match validate_connection_for_schema(
        &connection,
        Some(expected_node_id),
        source_schema_version,
    ) {
        Ok(summary) => summary,
        Err(error) => {
            return Err(quarantine_closed_image_connection_until_exit(
                &mut source,
                connection,
                error,
            ));
        }
    };
    if let Err(error) = observe(RestoreBoundary::ReopenValidated) {
        return Err(quarantine_closed_image_connection_until_exit(
            &mut source,
            connection,
            error,
        ));
    }
    let live_claim = match source
        .reservation
        .as_mut()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))
        .and_then(ClaimReservation::transition_to_live)
    {
        Ok(claim) => claim,
        Err(error) => {
            return Err(quarantine_closed_image_connection_until_exit(
                &mut source,
                connection,
                error,
            ));
        }
    };
    drop(source.reservation.take());
    let reopened = ValidatedConnection {
        connection: Some(connection),
        directory_guard: source.directory_guard.take(),
        family_guard: source.family_guard.take(),
        main_guard: source.main_guard.take(),
        live_claim: Some(live_claim),
        identity: source.identity,
    };
    // The retained source guard now proves the installed destination pathname.
    validate_guarded_image(
        &destination_path,
        reopened.identity,
        reopened
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        reopened
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        reopened
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )?;
    reopened.close()?;
    Ok(summary)
}

fn validate_closed_image_read_only(image: &mut ClosedImage) -> Result<(), RepositoryError> {
    let path = image.path()?.to_owned();
    if let Err(error) = validate_guarded_image(
        &path,
        image.identity,
        image
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        image
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        image
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    ) {
        return Err(quarantine_closed_image_until_exit(image, error));
    }
    if let Err(error) = ensure_auxiliary_files_absent(&path) {
        return Err(quarantine_closed_image_until_exit(image, error));
    }
    let spec = match ConnectionOpenSpec::for_existing(path.clone(), true) {
        Ok(spec) => spec,
        Err(error) => return Err(quarantine_closed_image_until_exit(image, error)),
    };
    let connection = match Connection::open_with_flags_and_vfs(&path, spec.flags, spec.vfs) {
        Ok(connection) => connection,
        Err(error) => {
            return Err(quarantine_closed_image_until_exit(
                image,
                map_sql_error(error),
            ));
        }
    };
    let validation = (|| {
        if connected_vfs_name(&connection)? != spec.vfs {
            return Err(RepositoryError::new(RepositoryErrorClass::Database));
        }
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        apply_limits(&connection)?;
        validate_connection_for_schema(
            &connection,
            Some(image.summary.node_id),
            image.schema_version,
        )
    })();
    match connection.close() {
        Ok(()) => {}
        Err((connection, error)) => {
            return Err(quarantine_closed_image_connection_until_exit(
                image,
                connection,
                map_sql_error(error),
            ));
        }
    }
    let summary = match validation {
        Ok(summary) => summary,
        Err(error) => return Err(quarantine_closed_image_until_exit(image, error)),
    };
    if let Err(error) = validate_guarded_image(
        &path,
        image.identity,
        image
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        image
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        image
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    ) {
        return Err(quarantine_closed_image_until_exit(image, error));
    }
    if let Err(error) = ensure_auxiliary_files_absent(&path) {
        return Err(quarantine_closed_image_until_exit(image, error));
    }
    if summary != image.summary {
        let error = RepositoryError::new(RepositoryErrorClass::Corrupt);
        return Err(quarantine_closed_image_until_exit(image, error));
    }
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
        family_guard,
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
        family_guard: Some(family_guard),
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
                    Some(prepared.family_guard),
                    claim,
                    CloseUncertainty::CheckpointOrClose,
                    error,
                ),
                Err(_) => retain_quarantined_owner(
                    connection,
                    prepared.main_guard,
                    prepared.directory_guard,
                    prepared.family_guard,
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

fn family_lock_path(path: &Path) -> Result<PathBuf, RepositoryError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_os_string();
    name.push(".owner.lock");
    Ok(path.with_file_name(name))
}

#[cfg(unix)]
fn open_family_guard(path: &Path) -> Result<fs::File, RepositoryError> {
    let lock_path = family_lock_path(path)?;
    let (guard, created) = open_or_create_private_file(&lock_path)?;
    validate_family_guard(path, &guard)?;
    try_lock_family_guard(&guard)?;
    validate_family_guard(path, &guard)?;
    if created {
        let parent = lock_path
            .parent()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        sync_directory_path(parent)?;
    }
    Ok(guard)
}

#[cfg(unix)]
fn try_lock_family_guard(guard: &fs::File) -> Result<(), RepositoryError> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(guard.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let raw = std::io::Error::last_os_error().raw_os_error();
    Err(
        if raw == Some(libc::EWOULDBLOCK) || raw == Some(libc::EAGAIN) {
            RepositoryError::new(RepositoryErrorClass::AlreadyOwned)
        } else {
            RepositoryError::new(RepositoryErrorClass::Durability)
        },
    )
}

#[cfg(not(unix))]
fn try_lock_family_guard(_guard: &fs::File) -> Result<(), RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
}

#[cfg(unix)]
fn unlock_family_guard(guard: &fs::File) -> Result<(), RepositoryError> {
    use std::os::fd::AsRawFd;

    #[cfg(test)]
    if FAIL_NEXT_FAMILY_UNLOCK.with(|fault| fault.replace(false)) {
        return Err(RepositoryError::new(RepositoryErrorClass::Durability));
    }

    let result = unsafe { libc::flock(guard.as_raw_fd(), libc::LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(RepositoryError::new(RepositoryErrorClass::Durability))
    }
}

#[cfg(not(unix))]
fn unlock_family_guard(_guard: &fs::File) -> Result<(), RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
}

#[cfg(not(unix))]
fn open_family_guard(_path: &Path) -> Result<fs::File, RepositoryError> {
    Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ))
}

fn validate_family_guard(path: &Path, guard: &fs::File) -> Result<(), RepositoryError> {
    let lock_path = family_lock_path(path)?;
    let path_metadata = fs::symlink_metadata(&lock_path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let guard_metadata = guard
        .metadata()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    validate_file_metadata(&path_metadata)?;
    validate_file_metadata(&guard_metadata)?;
    if path_metadata.len() != 0
        || guard_metadata.len() != 0
        || file_identity(&path_metadata)? != file_identity(&guard_metadata)?
    {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    Ok(())
}

fn remove_guarded_family_lock(path: &Path, guard: fs::File) -> Result<(), RepositoryError> {
    validate_family_guard(path, &guard)?;
    let lock_path = family_lock_path(path)?;
    fs::remove_file(&lock_path)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let parent = lock_path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    sync_directory_path(parent)?;
    if let Err(error) = unlock_family_guard(&guard) {
        let _retained_until_exit: &'static mut fs::File = Box::leak(Box::new(guard));
        return Err(error);
    }
    drop(guard);
    Ok(())
}

fn unique_temporary_path(path: &Path, label: &str) -> Result<PathBuf, RepositoryError> {
    let mut name = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_os_string();
    name.push(format!(".{label}-{}.tmp", StreamEpoch::new_v4()));
    Ok(path.with_file_name(name))
}

fn finish_with_temporary_cleanup<T>(
    result: Result<T, RepositoryError>,
    temporary: &Path,
) -> Result<T, RepositoryError> {
    match result {
        Ok(value) => Ok(value),
        Err(original) => match cleanup_unclaimed_temporary_family(temporary) {
            Ok(()) => Err(original),
            Err(cleanup) if cleanup.class() == RepositoryErrorClass::AlreadyOwned => {
                // The caller's own quarantined temporary can still own the
                // lock. Do not expose that internal path as a user-visible
                // ownership conflict; cleanup uncertainty is durability.
                Err(RepositoryError::new(RepositoryErrorClass::Durability))
            }
            Err(cleanup) => Err(cleanup),
        },
    }
}

fn cleanup_unclaimed_temporary_family(path: &Path) -> Result<(), RepositoryError> {
    cleanup_unclaimed_temporary_family_after(path, || {})
}

fn cleanup_unclaimed_temporary_family_after(
    path: &Path,
    after_family_lock: impl FnOnce(),
) -> Result<(), RepositoryError> {
    let family_guard = open_family_guard(path)?;
    after_family_lock();
    ensure_auxiliary_files_absent(path).map_err(|_| {
        // A temporary sidecar means an SQLite owner may still be uncertain.
        RepositoryError::new(RepositoryErrorClass::Durability)
    })?;
    let file = match validate_optional_private_file(path, None) {
        Ok(Some(file)) => file,
        Ok(None) => return remove_guarded_family_lock(path, family_guard),
        Err(_) => return Err(RepositoryError::new(RepositoryErrorClass::Durability)),
    };
    let identity = file_identity(
        &file
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )?;
    let claimed = claims()
        .lock()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?
        .contains_key(&identity);
    drop(file);
    if claimed {
        return Err(RepositoryError::new(RepositoryErrorClass::Durability));
    }
    fs::remove_file(path).map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?;
    sync_directory_path(parent)?;
    remove_guarded_family_lock(path, family_guard)
}

fn recover_orphaned_temporary_families(path: &Path) -> Result<(), RepositoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let base = path
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_string_lossy();
    let backup_base = migration_backup_path(path)?
        .file_name()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
        .to_string_lossy()
        .into_owned();
    let prefixes = [
        format!("{base}.restore-"),
        format!("{base}.rollback-"),
        format!("{backup_base}.backup-"),
    ];
    let mut temporaries = BTreeSet::new();
    let entries =
        fs::read_dir(parent).map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    for entry in entries {
        let entry = entry.map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let main_name = name.strip_suffix(".owner.lock").unwrap_or(&name);
        if main_name.ends_with(".tmp")
            && prefixes.iter().any(|prefix| main_name.starts_with(prefix))
        {
            temporaries.insert(parent.join(main_name));
        }
    }
    for temporary in temporaries {
        match cleanup_unclaimed_temporary_family(&temporary) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.class(),
                    RepositoryErrorClass::AlreadyOwned | RepositoryErrorClass::Durability
                ) =>
            {
                // Active or sidecar-bearing recovery state is retained for
                // operator/restart reconciliation. It is non-authoritative and
                // must not block the separately locked destination family.
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
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
    for suffix in ["-wal", "-journal", "-shm"] {
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
    let family_guard = open_family_guard(&canonical_path)?;
    recover_orphaned_temporary_families(&canonical_path)?;
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
        family_guard,
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
    let family_guard = open_family_guard(&canonical_path)?;
    recover_orphaned_temporary_families(&canonical_path)?;
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
        family_guard,
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
    validate_family_guard(&prepared.canonical_path, &prepared.family_guard)?;
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
    let file = validate_optional_private_file(path, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    digest_file_handle(&file)
}

fn digest_file_handle(file: &fs::File) -> Result<[u8; 32], RepositoryError> {
    use sha2::{Digest, Sha256};

    let mut file = file
        .try_clone()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
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

fn copy_closed_image(source: &ClosedImage, destination: &Path) -> Result<(), RepositoryError> {
    let source_path = source.path()?;
    validate_guarded_image(
        source_path,
        source.identity,
        source
            .directory_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        source
            .family_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
        source
            .main_guard
            .as_ref()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?,
    )?;
    ensure_auxiliary_files_absent(source_path)?;
    ensure_auxiliary_files_absent(destination)?;
    if fs::symlink_metadata(destination).is_ok() {
        return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_guard = open_secure_directory(parent, false)?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    let directory_identity = file_identity(
        &directory_guard
            .metadata()
            .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
    )?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    let mut input = source
        .main_guard
        .as_ref()
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::Durability))?
        .try_clone()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    input
        .seek(SeekFrom::Start(0))
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    let mut output = create_private_file(destination)?;
    std::io::copy(&mut input, &mut output)
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    output
        .sync_all()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    directory_guard
        .sync_all()
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::Durability))?;
    validate_bound_directory_continuity(&canonical_parent, directory_identity, &directory_guard)?;
    let copied = validate_optional_private_file(destination, None)?
        .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
    if digest_file_handle(&copied)?
        != digest_file_handle(source.main_guard.as_ref().expect("source guard retained"))?
    {
        return Err(RepositoryError::new(RepositoryErrorClass::Corrupt));
    }
    ensure_auxiliary_files_absent(destination)
}

fn atomic_replace(source: &Path, destination: &Path) -> Result<(), RepositoryError> {
    atomic_replace_after(source, destination, || {})
}

fn atomic_install_closed_image(
    source: &Path,
    source_identity: FileIdentity,
    destination: &Path,
    expectation: DestinationInstallExpectation,
) -> Result<(), AtomicInstallError> {
    let preflight = (|| {
        let source_file = validate_optional_private_file(source, None)?
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        if file_identity(
            &source_file
                .metadata()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
        )? != source_identity
        {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
        ensure_auxiliary_files_absent(source)?;
        match expectation {
            DestinationInstallExpectation::Vacant => {
                if fs::symlink_metadata(destination).is_ok() {
                    return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
                }
            }
            DestinationInstallExpectation::Existing(expected) => {
                let destination_file = validate_optional_private_file(destination, None)?
                    .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
                if file_identity(
                    &destination_file
                        .metadata()
                        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
                )? != expected
                {
                    return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
                }
            }
        }
        ensure_auxiliary_files_absent(destination)
    })();
    if let Err(error) = preflight {
        return Err(AtomicInstallError {
            error,
            renamed: false,
        });
    }

    #[cfg(unix)]
    let rename_result = (|| {
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;

        let source_parent = source
            .parent()
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        if destination.parent() != Some(source_parent) {
            return Err(RepositoryError::new(RepositoryErrorClass::UnsafePath));
        }
        let directory = open_secure_directory(source_parent, false)?;
        let source_name = CString::new(
            source
                .file_name()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
                .as_bytes(),
        )
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        let destination_name = CString::new(
            destination
                .file_name()
                .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?
                .as_bytes(),
        )
        .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;

        // Vacant installation is kernel-enforced no-replace. Existing-image
        // replacement is serialized by the retained private destination-family
        // flock and checked by exact inode immediately before this syscall.
        // A non-cooperating process with the same OS account can still rename
        // account-owned files; that is the binding local-account compromise
        // boundary, not a second supported Loxa owner.
        #[cfg(target_os = "linux")]
        let rc = unsafe {
            libc::renameat2(
                directory.as_raw_fd(),
                source_name.as_ptr(),
                directory.as_raw_fd(),
                destination_name.as_ptr(),
                if matches!(expectation, DestinationInstallExpectation::Vacant) {
                    libc::RENAME_NOREPLACE
                } else {
                    0
                },
            )
        };
        #[cfg(target_os = "macos")]
        let rc = if matches!(expectation, DestinationInstallExpectation::Vacant) {
            let source_path = CString::new(source.as_os_str().as_bytes())
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            let destination_path = CString::new(destination.as_os_str().as_bytes())
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
            unsafe {
                libc::renamex_np(
                    source_path.as_ptr(),
                    destination_path.as_ptr(),
                    libc::RENAME_EXCL,
                )
            }
        } else {
            unsafe {
                libc::renameat(
                    directory.as_raw_fd(),
                    source_name.as_ptr(),
                    directory.as_raw_fd(),
                    destination_name.as_ptr(),
                )
            }
        };
        if rc == 0 {
            Ok(())
        } else {
            let error = std::io::Error::last_os_error();
            Err(RepositoryError::new(
                if matches!(error.kind(), std::io::ErrorKind::AlreadyExists) {
                    RepositoryErrorClass::UnsafePath
                } else {
                    RepositoryErrorClass::Durability
                },
            ))
        }
    })();
    #[cfg(not(unix))]
    let rename_result: Result<(), RepositoryError> = Err(RepositoryError::new(
        RepositoryErrorClass::UnsupportedPlatform,
    ));
    if let Err(error) = rename_result {
        return Err(AtomicInstallError {
            error,
            renamed: false,
        });
    }

    #[cfg(test)]
    if FAIL_ATOMIC_INSTALL_POSTFLIGHT.with(|fault| fault.replace(false)) {
        return Err(AtomicInstallError {
            error: RepositoryError::new(RepositoryErrorClass::Durability),
            renamed: true,
        });
    }

    let postflight = (|| {
        let installed = validate_optional_private_file(destination, None)?
            .ok_or_else(|| RepositoryError::new(RepositoryErrorClass::UnsafePath))?;
        if file_identity(
            &installed
                .metadata()
                .map_err(|_| RepositoryError::new(RepositoryErrorClass::UnsafePath))?,
        )? != source_identity
        {
            return Err(RepositoryError::new(RepositoryErrorClass::Durability));
        }
        ensure_auxiliary_files_absent(destination)
    })();
    postflight.map_err(|error| AtomicInstallError {
        error,
        renamed: true,
    })
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
    use crate::control_state::state_machine::test_support::storage::{
        apply_auxiliary_defect, family_snapshot, AuxiliaryDefect, AuxiliaryKind,
    };
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
        initial_event_id: Option<loxa_protocol::v2::EventId>,
        calls: usize,
    }

    impl CountingIds {
        fn fixed() -> Self {
            Self {
                slot_id: Some(SlotId::from_str(SLOT_ID).unwrap()),
                stream_epoch: Some(StreamEpoch::from_str(STREAM_EPOCH).unwrap()),
                initial_event_id: Some(
                    loxa_protocol::v2::EventId::from_str("66666666-6666-4666-8666-666666666666")
                        .unwrap(),
                ),
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

        fn new_initial_event_id(&mut self) -> loxa_protocol::v2::EventId {
            self.calls += 1;
            self.initial_event_id
                .take()
                .unwrap_or_else(loxa_protocol::v2::EventId::new_v4)
        }
    }

    fn node_id() -> NodeId {
        NodeId::from_str(NODE_ID).unwrap()
    }

    fn create_repository(path: &Path) -> ControlRepository {
        ControlRepository::open_or_create(path, node_id(), &mut CountingIds::fixed()).unwrap()
    }

    fn file_digest(path: &Path) -> [u8; 32] {
        super::digest_file_handle(&fs::File::open(path).unwrap()).unwrap()
    }

    fn logical_lineage(path: &Path) -> ([u8; 32], super::ValidationSummary) {
        use rusqlite::types::ValueRef;
        use sha2::{Digest, Sha256};

        let image = super::open_validated_image(path, Some(node_id())).unwrap();
        let summary = image.summary.clone();
        let connection = image.connection.as_ref().unwrap();
        let mut digest = Sha256::new();
        for table in [
            "loxa_schema_migrations",
            "control_meta",
            "node_state",
            "slot_state",
            "operations",
            "events",
        ] {
            digest.update(table.as_bytes());
            let sql = format!("SELECT * FROM {table} ORDER BY rowid");
            let mut statement = connection.prepare(&sql).unwrap();
            let column_count = statement.column_count();
            let mut rows = statement.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                for column in 0..column_count {
                    match row.get_ref(column).unwrap() {
                        ValueRef::Null => digest.update([0]),
                        ValueRef::Integer(value) => {
                            digest.update([1]);
                            digest.update(value.to_be_bytes());
                        }
                        ValueRef::Real(value) => {
                            digest.update([2]);
                            digest.update(value.to_bits().to_be_bytes());
                        }
                        ValueRef::Text(value) => {
                            digest.update([3]);
                            digest.update(value.len().to_be_bytes());
                            digest.update(value);
                        }
                        ValueRef::Blob(value) => {
                            digest.update([4]);
                            digest.update(value.len().to_be_bytes());
                            digest.update(value);
                        }
                    }
                }
            }
        }
        super::close_into_image(image).unwrap().release().unwrap();
        (digest.finalize().into(), summary)
    }

    fn assert_no_temporary_restore_artifacts(directory: &Path) {
        let artifacts: Vec<_> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| {
                (name.contains(".restore-")
                    || name.contains(".rollback-")
                    || name.contains(".backup-"))
                    && (name.contains(".tmp") || name.ends_with(".owner.lock"))
            })
            .collect();
        assert!(artifacts.is_empty(), "temporary artifacts: {artifacts:?}");
    }

    fn commit_revision_two(repository: &mut ControlRepository) {
        let payload = super::slot_event_payload(
            loxa_protocol::v2::EventId::from_str("77777777-7777-4777-8777-777777777777").unwrap(),
            repository.stream_epoch(),
            2,
            2,
            0,
            node_id(),
            repository.slot_id(),
        )
        .expect("slot payload");
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

    struct RestoreFaultOutcome {
        reached: bool,
        exit_code: i32,
        destination_summary: super::ValidationSummary,
        destination_digest: [u8; 32],
        old_destination_digest: [u8; 32],
        backup_lineage_digest: [u8; 32],
        backup_digest_unchanged: bool,
        backup_summary: super::ValidationSummary,
        destination_sidecars_absent: bool,
        present_sidecars: Vec<&'static str>,
        temporary_artifacts_absent: bool,
        output: String,
    }

    fn restore_boundaries() -> [super::RestoreBoundary; 16] {
        [
            super::RestoreBoundary::BeforeSourceCopy,
            super::RestoreBoundary::AfterSourceCopy,
            super::RestoreBoundary::SourceCheckpointTruncated,
            super::RestoreBoundary::SourceJournalModeDelete,
            super::RestoreBoundary::SourceSqliteClosed,
            super::RestoreBoundary::SourceSidecarsAbsent,
            super::RestoreBoundary::SourceReadOnlyValidated,
            super::RestoreBoundary::DestinationCheckpointTruncated,
            super::RestoreBoundary::DestinationJournalModeDelete,
            super::RestoreBoundary::DestinationSqliteClosed,
            super::RestoreBoundary::DestinationSidecarsAbsent,
            super::RestoreBoundary::DestinationReadOnlyValidated,
            super::RestoreBoundary::BeforeRename,
            super::RestoreBoundary::AfterRename,
            super::RestoreBoundary::AfterDirectorySync,
            super::RestoreBoundary::ReopenValidated,
        ]
    }

    fn is_pre_rename(point: super::RestoreBoundary) -> bool {
        !matches!(
            point,
            super::RestoreBoundary::AfterRename
                | super::RestoreBoundary::AfterDirectorySync
                | super::RestoreBoundary::ReopenValidated
        )
    }

    fn assert_canonical_singleton(summary: &super::ValidationSummary) {
        assert_eq!(summary.node_rows, 1);
        assert_eq!(summary.slot_rows, 1);
        assert_eq!(summary.slot_name, "default");
        assert_eq!(summary.node_id, node_id());
    }

    fn assert_temporary_recovery_disposition(
        outcome: &RestoreFaultOutcome,
        point: super::RestoreBoundary,
    ) {
        assert!(
            outcome.temporary_artifacts_absent
                || point == super::RestoreBoundary::SourceCheckpointTruncated,
            "unexpected retained temporary at {point:?}"
        );
    }

    fn run_restore_fault_subprocess(point: super::RestoreBoundary) -> RestoreFaultOutcome {
        run_restore_subprocess(point, "restore_crash")
    }

    fn run_restore_returned_failure_subprocess(
        point: super::RestoreBoundary,
    ) -> RestoreFaultOutcome {
        run_restore_subprocess(point, "restore_failure")
    }

    fn assert_fault_matrix_subprocess(mode: &'static str) {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = wait_child_with_timeout(child, Duration::from_secs(300));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "repository fault matrix failed: {mode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("FAULT_MATRIX_COMPLETE:{mode}")),
            "repository fault matrix did not report completion: {mode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    fn run_restore_subprocess(
        point: super::RestoreBoundary,
        mode: &'static str,
    ) -> RestoreFaultOutcome {
        let source = TestDirectory::new(&format!("{mode}-source-{point:?}"));
        let mut source_repository = create_repository(&source.database());
        let payload = super::slot_event_payload(
            loxa_protocol::v2::EventId::from_str("44444444-4444-4444-8444-444444444444").unwrap(),
            source_repository.stream_epoch(),
            41,
            41,
            0,
            node_id(),
            source_repository.slot_id(),
        )
        .unwrap();
        source_repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '41', cursor = '41' WHERE singleton = 1",
                    [],
                )?;
                transaction.execute(
                    "INSERT INTO events(event_id, stream_epoch, sequence, revision, event_kind, payload_json) VALUES('44444444-4444-4444-8444-444444444444', ?1, '41', '41', 'slot_changed', ?2)",
                    [STREAM_EPOCH, &payload],
                )?;
                Ok(())
            })
            .unwrap();
        let backup = source_repository.backup_before_migration().unwrap();
        source_repository.close().unwrap();
        let (backup_lineage_digest, backup_summary) = logical_lineage(&backup);
        let backup_digest_before = file_digest(&backup);

        let destination = TestDirectory::new(&format!("{mode}-destination-{point:?}"));
        let destination_path = destination.database();
        create_repository(&destination_path).close().unwrap();
        let (old_destination_digest, _) = logical_lineage(&destination_path);

        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", mode)
            .env("LOXA_RESTORE_BACKUP", &backup)
            .env("LOXA_RESTORE_DESTINATION", &destination_path)
            .env("LOXA_RESTORE_BOUNDARY", format!("{point:?}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = wait_child_with_timeout(child, Duration::from_secs(15));
        if mode == "restore_failure" {
            ControlRepository::open_or_create(
                &destination_path,
                node_id(),
                &mut CountingIds::default(),
            )
            .unwrap()
            .close()
            .unwrap();
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let present_sidecars: Vec<_> = ["-wal", "-journal", "-shm"]
            .into_iter()
            .filter(|suffix| {
                super::auxiliary_path(&destination_path, suffix)
                    .unwrap()
                    .exists()
            })
            .collect();
        let destination_sidecars_absent = present_sidecars.is_empty();
        let (destination_digest, destination_summary) = logical_lineage(&destination_path);
        let backup_digest_unchanged = file_digest(&backup) == backup_digest_before;
        let temporary_artifacts_absent = fs::read_dir(&destination.0).unwrap().all(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            !((name.contains(".restore-") || name.contains(".rollback-"))
                && (name.contains(".tmp") || name.ends_with(".owner.lock")))
        });
        RestoreFaultOutcome {
            reached: stdout.contains(&format!("RESTORE_REACHED:{point:?}")),
            exit_code: output.status.code().unwrap_or(-1),
            destination_summary,
            destination_digest,
            old_destination_digest,
            backup_lineage_digest,
            backup_digest_unchanged,
            backup_summary,
            destination_sidecars_absent,
            present_sidecars,
            temporary_artifacts_absent,
            output: format!(
                "stdout:\n{}\nstderr:\n{}",
                stdout,
                String::from_utf8_lossy(&output.stderr)
            ),
        }
    }

    fn run_migration_rollback_fault_subprocess(
        point: super::RestoreBoundary,
    ) -> RestoreFaultOutcome {
        run_migration_rollback_subprocess(point, "rollback_crash")
    }

    fn run_migration_rollback_returned_failure_subprocess(
        point: super::RestoreBoundary,
    ) -> RestoreFaultOutcome {
        run_migration_rollback_subprocess(point, "rollback_failure")
    }

    fn run_migration_rollback_subprocess(
        point: super::RestoreBoundary,
        mode: &'static str,
    ) -> RestoreFaultOutcome {
        let directory = TestDirectory::new(&format!("{mode}-{point:?}"));
        let destination_path = directory.database();
        let repository = create_repository(&destination_path);
        let backup = repository.backup_before_migration().unwrap();
        repository.close().unwrap();
        let (backup_lineage_digest, backup_summary) = logical_lineage(&backup);
        let backup_digest_before = file_digest(&backup);
        let mut failed = ControlRepository::open_or_create(
            &destination_path,
            node_id(),
            &mut CountingIds::default(),
        )
        .unwrap();
        failed
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '9' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        failed.close().unwrap();
        let (old_destination_digest, _) = logical_lineage(&destination_path);

        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", mode)
            .env("LOXA_RESTORE_BACKUP", &backup)
            .env("LOXA_RESTORE_DESTINATION", &destination_path)
            .env("LOXA_RESTORE_BOUNDARY", format!("{point:?}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = wait_child_with_timeout(child, Duration::from_secs(15));
        if mode == "rollback_failure" {
            ControlRepository::open_or_create(
                &destination_path,
                node_id(),
                &mut CountingIds::default(),
            )
            .unwrap()
            .close()
            .unwrap();
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let present_sidecars: Vec<_> = ["-wal", "-journal", "-shm"]
            .into_iter()
            .filter(|suffix| {
                super::auxiliary_path(&destination_path, suffix)
                    .unwrap()
                    .exists()
            })
            .collect();
        let destination_sidecars_absent = present_sidecars.is_empty();
        let (destination_digest, destination_summary) = logical_lineage(&destination_path);
        let backup_digest_unchanged = file_digest(&backup) == backup_digest_before;
        let temporary_artifacts_absent = fs::read_dir(&directory.0).unwrap().all(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            !((name.contains(".restore-") || name.contains(".rollback-"))
                && (name.contains(".tmp") || name.ends_with(".owner.lock")))
        });
        RestoreFaultOutcome {
            reached: stdout.contains(&format!("RESTORE_REACHED:{point:?}")),
            exit_code: output.status.code().unwrap_or(-1),
            destination_summary,
            destination_digest,
            old_destination_digest,
            backup_lineage_digest,
            backup_digest_unchanged,
            backup_summary,
            destination_sidecars_absent,
            present_sidecars,
            temporary_artifacts_absent,
            output: format!(
                "stdout:\n{}\nstderr:\n{}",
                stdout,
                String::from_utf8_lossy(&output.stderr)
            ),
        }
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
        match mode.as_str() {
            "restore_crash_matrix" => {
                assert_restore_fault_matrix();
                println!("FAULT_MATRIX_COMPLETE:{mode}");
            }
            "restore_failure_matrix" => {
                assert_returned_restore_failure_matrix();
                println!("FAULT_MATRIX_COMPLETE:{mode}");
            }
            "rollback_crash_matrix" => {
                assert_migration_rollback_fault_matrix();
                println!("FAULT_MATRIX_COMPLETE:{mode}");
            }
            "rollback_failure_matrix" => {
                assert_returned_migration_rollback_failure_matrix();
                println!("FAULT_MATRIX_COMPLETE:{mode}");
            }
            "probe" => {
                let path =
                    PathBuf::from(std::env::var_os("LOXA_REPOSITORY_PATH").expect("helper path"));
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
                    Err(error)
                        if matches!(
                            error.class(),
                            RepositoryErrorClass::Database | RepositoryErrorClass::AlreadyOwned
                        ) =>
                    {
                        println!("PROBE_LOCKED");
                    }
                    Err(error) => panic!("unexpected probe error: {error:?}"),
                }
            }
            "owner" => {
                let path =
                    PathBuf::from(std::env::var_os("LOXA_REPOSITORY_PATH").expect("helper path"));
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
            "restore_postflight_failure_owner" => {
                let backup =
                    PathBuf::from(std::env::var_os("LOXA_RESTORE_BACKUP").expect("restore backup"));
                let destination = PathBuf::from(
                    std::env::var_os("LOXA_RESTORE_DESTINATION").expect("restore destination"),
                );
                super::fail_next_atomic_install_postflight_for_test();
                let error = ControlRepository::restore_offline(&backup, &destination).unwrap_err();
                assert_eq!(error.class(), RepositoryErrorClass::Durability);
                assert!(!super::atomic_install_postflight_fault_is_pending_for_test());
                println!("OWNER_READY");
                std::io::stdout().flush().unwrap();
                let mut release = String::new();
                std::io::stdin().read_line(&mut release).unwrap();
            }
            "restore_crash" | "restore_failure" => {
                let inject_returned_failure = mode == "restore_failure";
                let backup =
                    PathBuf::from(std::env::var_os("LOXA_RESTORE_BACKUP").expect("restore backup"));
                let destination = PathBuf::from(
                    std::env::var_os("LOXA_RESTORE_DESTINATION").expect("restore destination"),
                );
                let expected = std::env::var("LOXA_RESTORE_BOUNDARY").expect("restore boundary");
                let result =
                    ControlRepository::restore_offline_after(&backup, &destination, |boundary| {
                        if format!("{boundary:?}") == expected {
                            println!("RESTORE_REACHED:{boundary:?}");
                            std::io::stdout().flush().unwrap();
                            if inject_returned_failure {
                                return Err(RepositoryError::new(RepositoryErrorClass::Durability));
                            }
                            unsafe { libc::_exit(86) };
                        }
                        Ok(())
                    });
                if inject_returned_failure {
                    let error = result.expect_err("returned restore failure");
                    println!("RESTORE_RETURNED:{:?}", error.class());
                } else {
                    panic!("restore crash boundary was not reached: {result:?}");
                }
            }
            "rollback_crash" | "rollback_failure" => {
                let inject_returned_failure = mode == "rollback_failure";
                let backup = PathBuf::from(
                    std::env::var_os("LOXA_RESTORE_BACKUP").expect("rollback backup"),
                );
                let destination = PathBuf::from(
                    std::env::var_os("LOXA_RESTORE_DESTINATION").expect("rollback destination"),
                );
                let expected = std::env::var("LOXA_RESTORE_BOUNDARY").expect("rollback boundary");
                let mut repository = ControlRepository::open_or_create(
                    &destination,
                    node_id(),
                    &mut CountingIds::default(),
                )
                .unwrap();
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
                let result = ControlRepository::restore_verified_migration_backup_after(
                    &backup,
                    &destination,
                    &proof,
                    |boundary| {
                        if format!("{boundary:?}") == expected {
                            println!("RESTORE_REACHED:{boundary:?}");
                            std::io::stdout().flush().unwrap();
                            if inject_returned_failure {
                                return Err(RepositoryError::new(RepositoryErrorClass::Durability));
                            }
                            unsafe { libc::_exit(86) };
                        }
                        Ok(())
                    },
                );
                if inject_returned_failure {
                    let error = result.expect_err("returned rollback failure");
                    println!("RESTORE_RETURNED:{:?}", error.class());
                } else {
                    panic!("rollback crash boundary was not reached: {result:?}");
                }
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
        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
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

    #[test]
    fn postrename_postflight_failure_retains_destination_family_exclusion_until_exit() {
        let source = TestDirectory::new("postflight-owner-source");
        let mut source_repository = create_repository(&source.database());
        source_repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET revision = '41', cursor = '1' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let backup = source_repository.backup_before_migration().unwrap();
        source_repository.close().unwrap();
        let destination = TestDirectory::new("postflight-owner-destination");
        let path = destination.database();
        create_repository(&path).close().unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "control_state::repository::tests::repository_process_helper",
                "--nocapture",
            ])
            .env("LOXA_REPOSITORY_HELPER", "restore_postflight_failure_owner")
            .env("LOXA_RESTORE_BACKUP", &backup)
            .env("LOXA_RESTORE_DESTINATION", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
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
        let ready = ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(ready.contains("OWNER_READY"), "{ready}");
        assert!(child.try_wait().unwrap().is_none(), "owner exited early");
        assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);

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
        assert_eq!(reopened.validate_all().unwrap().revision, 42);
        reopened.close().unwrap();
        assert_no_temporary_restore_artifacts(&destination.0);
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
    fn successful_close_explicitly_unlocks_a_duplicated_family_guard() {
        let directory = TestDirectory::new("close-explicit-unlock-duplicate");
        let path = directory.database();
        let repository = create_repository(&path);
        let duplicate = repository
            .family_guard
            .as_ref()
            .unwrap()
            .try_clone()
            .unwrap();

        repository.close().unwrap();

        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        reopened.close().unwrap();
        drop(duplicate);
    }

    #[test]
    fn family_unlock_failure_retains_and_poisons_exact_ownership() {
        let directory = TestDirectory::new("close-explicit-unlock-failure");
        let path = directory.database();
        let repository = create_repository(&path);
        super::fail_next_family_unlock_for_test();

        let error = repository.close().unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::Durability);
        let reopen =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(reopen.class(), RepositoryErrorClass::AlreadyOwned);
        assert_eq!(probe_from_subprocess(&path), ChildProbe::DatabaseLocked);
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
        assert_eq!(ids.calls(), 3);
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
        let before = file_digest(&path);

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
        assert_eq!(file_digest(&path), before);
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
        let before = file_digest(&path);

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsupportedSchema);
        assert_eq!(file_digest(&path), before);
    }

    #[test]
    fn former_intermediate_task3a_schema_and_checksum_fail_closed_unchanged() {
        use sha2::{Digest, Sha256};

        let directory = TestDirectory::new("former-task3a-schema");
        let path = directory.database();
        let mut repository = create_repository(&path);
        repository
            .transaction(|transaction| {
                transaction.execute_batch(
                    "DROP TABLE slot_intent; DROP TABLE events; DROP TABLE operations; DROP TABLE slot_state; DROP TABLE node_state; DROP TABLE control_meta; DROP TABLE loxa_schema_migrations;",
                )?;
                transaction.execute_batch(
                    super::super::schema::FORMER_INTERMEDIATE_TASK3A_SCHEMA_V1,
                )?;
                let digest = Sha256::digest(
                    super::super::schema::FORMER_INTERMEDIATE_TASK3A_SCHEMA_V1.as_bytes(),
                );
                let checksum = digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                transaction.execute(
                    "INSERT INTO loxa_schema_migrations VALUES(1,'capacity_one_control_state',?1,1)",
                    [checksum],
                )?;
                transaction.execute(
                    "INSERT INTO control_meta VALUES(1,?1,?2,?3,'1','1',1,'fresh','1')",
                    [NODE_ID, SLOT_ID, STREAM_EPOCH],
                )?;
                transaction.execute(
                    "INSERT INTO node_state VALUES(1,?1,NULL,NULL,'unpublished',0,0,0)",
                    [NODE_ID],
                )?;
                transaction.execute(
                    "INSERT INTO slot_state VALUES(1,?1,'default','unloaded',NULL,NULL,'1','1')",
                    [SLOT_ID],
                )?;
                transaction.execute(
                    "INSERT INTO events VALUES('88888888-8888-4888-8888-888888888888',?1,'1','1',NULL,NULL,'initialized','{}')",
                    [STREAM_EPOCH],
                )?;
                Ok(())
            })
            .unwrap();
        repository.close().unwrap();
        let before = family_snapshot(&path);

        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsupportedSchema);
        assert_eq!(family_snapshot(&path), before);
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

    #[cfg(unix)]
    #[test]
    fn family_lock_rejects_unsafe_metadata_and_detects_path_substitution() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        for defect in ["symlink", "hardlink", "broad"] {
            let directory = TestDirectory::new(&format!("family-lock-{defect}"));
            let path = directory.database();
            let lock = super::family_lock_path(&path).unwrap();
            let target = directory.0.join("target.lock");
            fs::write(&target, []).unwrap();
            fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
            match defect {
                "symlink" => symlink(&target, &lock).unwrap(),
                "hardlink" => fs::hard_link(&target, &lock).unwrap(),
                "broad" => {
                    fs::rename(&target, &lock).unwrap();
                    fs::set_permissions(&lock, fs::Permissions::from_mode(0o644)).unwrap();
                }
                _ => unreachable!(),
            }
            let error =
                ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::fixed())
                    .unwrap_err();
            assert_eq!(error.class(), RepositoryErrorClass::UnsafePath, "{defect}");
            assert!(!path.exists(), "{defect}");
        }

        let substituted = TestDirectory::new("family-lock-substitution");
        let path = substituted.database();
        let repository = create_repository(&path);
        let lock = super::family_lock_path(&path).unwrap();
        fs::rename(&lock, substituted.0.join("displaced.lock")).unwrap();
        fs::write(&lock, []).unwrap();
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
        let error = repository.validate_all().unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
    }

    #[cfg(unix)]
    #[test]
    fn nonempty_family_lock_is_rejected_before_database_family_mutation() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TestDirectory::new("nonempty-family-lock");
        let path = directory.database();
        let lock = super::family_lock_path(&path).unwrap();
        fs::write(&lock, b"unexpected owner-lock bytes").unwrap();
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();

        let error = ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::fixed())
            .unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
        assert!(!path.exists());
        assert_eq!(fs::read(&lock).unwrap(), b"unexpected owner-lock bytes");
        assert_eq!(fs::metadata(&lock).unwrap().len(), 27);
    }

    #[cfg(unix)]
    #[test]
    fn temporary_cleanup_never_unlinks_a_held_family_lock() {
        let directory = TestDirectory::new("held-temporary-lock");
        let temporary = directory.0.join("control-state.sqlite3.restore-held.tmp");
        fs::write(&temporary, b"retained").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
        let guard = super::open_family_guard(&temporary).unwrap();
        let lock = super::family_lock_path(&temporary).unwrap();

        let error = super::cleanup_unclaimed_temporary_family(&temporary).unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
        assert!(temporary.exists());
        assert!(lock.exists());
        drop(guard);
        super::cleanup_unclaimed_temporary_family(&temporary).unwrap();
        assert!(!temporary.exists());
        assert!(!lock.exists());

        let raced = directory.0.join("control-state.sqlite3.restore-race.tmp");
        fs::write(&raced, b"retained").unwrap();
        fs::set_permissions(&raced, fs::Permissions::from_mode(0o600)).unwrap();
        super::cleanup_unclaimed_temporary_family_after(&raced, || {
            assert_eq!(probe_from_subprocess(&raced), ChildProbe::DatabaseLocked);
        })
        .unwrap();
        assert!(!raced.exists());
        assert!(!super::family_lock_path(&raced).unwrap().exists());
    }

    #[cfg(unix)]
    #[test]
    fn repository_startup_recovers_unclaimed_restore_rollback_and_backup_temporaries() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TestDirectory::new("startup-temp-recovery");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        let names = [
            "control-state.sqlite3.restore-orphan.tmp",
            "control-state.sqlite3.rollback-orphan.tmp",
            "control-state.sqlite3.pre-migration.bak.backup-orphan.tmp",
        ];
        for name in names {
            let temporary = directory.0.join(name);
            fs::write(&temporary, b"orphan").unwrap();
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
            drop(super::open_family_guard(&temporary).unwrap());
        }
        let lock_only = directory
            .0
            .join("control-state.sqlite3.restore-lock-only.tmp");
        drop(super::open_family_guard(&lock_only).unwrap());

        ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
            .unwrap()
            .close()
            .unwrap();

        assert_no_temporary_restore_artifacts(&directory.0);
    }

    #[cfg(unix)]
    #[test]
    fn repository_startup_retains_uncertain_orphan_sidecars_without_blocking_destination() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TestDirectory::new("startup-uncertain-temp");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        let temporary = directory
            .0
            .join("control-state.sqlite3.restore-uncertain.tmp");
        fs::write(&temporary, b"uncertain").unwrap();
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
        drop(super::open_family_guard(&temporary).unwrap());
        let journal = super::auxiliary_path(&temporary, "-journal").unwrap();
        fs::write(&journal, b"uncertain journal").unwrap();
        fs::set_permissions(&journal, fs::Permissions::from_mode(0o600)).unwrap();

        ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
            .unwrap()
            .close()
            .unwrap();

        assert!(temporary.exists());
        assert!(super::family_lock_path(&temporary).unwrap().exists());
        assert!(journal.exists());
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
    fn backup_publication_respects_the_retained_backup_family_lock_and_cleans_temporary_lock() {
        let directory = TestDirectory::new("backup-publication-lock");
        let repository = create_repository(&directory.database());
        let backup = repository.migration_backup_path().unwrap();
        let before = file_digest(&backup);
        let held =
            super::close_into_image(super::open_validated_image(&backup, Some(node_id())).unwrap())
                .unwrap();

        let error = repository.backup_before_migration().unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);
        assert_eq!(file_digest(&backup), before);
        assert_no_temporary_restore_artifacts(&directory.0);
        held.release().unwrap();
        repository.backup_before_migration().unwrap();
        assert_no_temporary_restore_artifacts(&directory.0);
        repository.close().unwrap();
    }

    #[test]
    fn offline_restore_rotates_epoch_clears_events_and_advances_revision() {
        let source = TestDirectory::new("restore-source");
        let mut repository = create_repository(&source.database());
        let restored_payload = super::slot_event_payload(
            loxa_protocol::v2::EventId::from_str("44444444-4444-4444-8444-444444444444").unwrap(),
            repository.stream_epoch(),
            41,
            41,
            0,
            node_id(),
            repository.slot_id(),
        )
        .unwrap();
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
        assert_no_temporary_restore_artifacts(&destination.0);
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
        assert_no_temporary_restore_artifacts(&directory.0);
        let reopened =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap();
        assert_eq!(reopened.stream_epoch(), original_epoch);
        assert_eq!(reopened.validate_all().unwrap().revision, 1);
    }

    #[test]
    fn migration_rollback_returns_original_error_unless_rollback_itself_fails() {
        let success = TestDirectory::new("rollback-original-error");
        let success_path = success.database();
        let repository = create_repository(&success_path);
        let backup = repository.backup_before_migration().unwrap();
        let proof = repository.migration_rollback_proof().unwrap();
        repository.close().unwrap();
        let original = RepositoryError::new(RepositoryErrorClass::UnsupportedSchema);

        let reported =
            ControlRepository::rollback_failed_migration(&backup, &success_path, &proof, original);
        assert_eq!(reported, original);
        assert!(backup.exists());

        let failure = TestDirectory::new("rollback-error-precedence");
        let failure_path = failure.database();
        let failure_repository = create_repository(&failure_path);
        let failure_backup = failure_repository.backup_before_migration().unwrap();
        let failure_proof = failure_repository.migration_rollback_proof().unwrap();
        failure_repository.close().unwrap();
        fs::write(&failure_backup, b"corrupt retained backup").unwrap();

        let reported = ControlRepository::rollback_failed_migration(
            &failure_backup,
            &failure_path,
            &failure_proof,
            original,
        );
        assert_eq!(reported.class(), RepositoryErrorClass::Corrupt);
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

    #[cfg(unix)]
    #[test]
    fn restore_rejects_backup_substitution_after_its_guard_is_bound() {
        use std::os::unix::fs::PermissionsExt;

        let source = TestDirectory::new("bound-backup-substitution");
        let repository = create_repository(&source.database());
        let backup = repository.backup_before_migration().unwrap();
        repository.close().unwrap();
        let replacement = source.0.join("replacement.sqlite3");
        fs::copy(&backup, &replacement).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        let destination = TestDirectory::new("bound-backup-substitution-destination");

        let error = ControlRepository::restore_offline_after(
            &backup,
            &destination.database(),
            |boundary| {
                if boundary == super::RestoreBoundary::BeforeSourceCopy {
                    fs::rename(&backup, source.0.join("displaced-backup.sqlite3")).unwrap();
                    fs::rename(&replacement, &backup).unwrap();
                }
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
        assert!(!destination.database().exists());
        assert_no_temporary_restore_artifacts(&destination.0);
    }

    #[cfg(unix)]
    #[test]
    fn vacant_restore_uses_no_replace_and_retains_a_racing_destination() {
        use std::os::unix::fs::PermissionsExt;

        let source = TestDirectory::new("vacant-no-replace-source");
        let repository = create_repository(&source.database());
        let backup = repository.backup_before_migration().unwrap();
        repository.close().unwrap();
        let destination = TestDirectory::new("vacant-no-replace-destination");
        let path = destination.database();

        let error = ControlRepository::restore_offline_after(&backup, &path, |boundary| {
            if boundary == super::RestoreBoundary::BeforeRename {
                fs::write(&path, b"racing destination").unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
        assert_eq!(fs::read(&path).unwrap(), b"racing destination");
        assert_no_temporary_restore_artifacts(&destination.0);
    }

    #[cfg(unix)]
    #[test]
    fn restore_refuses_a_sidecar_created_at_the_rename_boundary() {
        use std::os::unix::fs::PermissionsExt;

        let source = TestDirectory::new("rename-sidecar-source");
        let repository = create_repository(&source.database());
        let backup = repository.backup_before_migration().unwrap();
        repository.close().unwrap();
        let destination = TestDirectory::new("rename-sidecar-destination");
        let path = destination.database();
        create_repository(&path).close().unwrap();
        let before = logical_lineage(&path).0;
        let journal = super::auxiliary_path(&path, "-journal").unwrap();

        let error = ControlRepository::restore_offline_after(&backup, &path, |boundary| {
            if boundary == super::RestoreBoundary::BeforeRename {
                fs::write(&journal, b"racing journal").unwrap();
                fs::set_permissions(&journal, fs::Permissions::from_mode(0o600)).unwrap();
            }
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
        assert_eq!(fs::read(&journal).unwrap(), b"racing journal");
        fs::remove_file(&journal).unwrap();
        assert_eq!(logical_lineage(&path).0, before);
        assert_no_temporary_restore_artifacts(&destination.0);
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
            let mutation = repository.transaction(|transaction| {
                transaction.execute(sql, [])?;
                Ok(())
            });
            match mutation {
                Ok(()) => {
                    let error = repository.validate_all().unwrap_err();
                    assert_eq!(error.class(), RepositoryErrorClass::Corrupt, "{sql}");
                }
                Err(error) => {
                    assert_eq!(error.class(), RepositoryErrorClass::Database, "{sql}");
                    repository.validate_all().unwrap();
                }
            }
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

    #[cfg(unix)]
    #[test]
    fn corrupt_destination_with_any_sidecar_is_refused_unchanged() {
        use std::os::unix::fs::PermissionsExt;

        for suffix in ["-wal", "-journal", "-shm"] {
            let source = TestDirectory::new(&format!("restore-sidecar-source-{suffix}"));
            let source_repository = create_repository(&source.database());
            let backup = source_repository.backup_before_migration().unwrap();
            source_repository.close().unwrap();

            let destination = TestDirectory::new(&format!("restore-sidecar-destination-{suffix}"));
            let destination_path = destination.database();
            create_repository(&destination_path).close().unwrap();
            let sidecar = super::auxiliary_path(&destination_path, suffix).unwrap();
            fs::write(&sidecar, b"retained-sidecar").unwrap();
            fs::set_permissions(&sidecar, fs::Permissions::from_mode(0o600)).unwrap();
            let before = family_snapshot(&destination_path);

            let error = ControlRepository::restore_offline(&backup, &destination_path)
                .expect_err("a destination family with any sidecar must be refused");

            assert_eq!(error.class(), RepositoryErrorClass::UnsafePath, "{suffix}");
            assert_eq!(family_snapshot(&destination_path), before, "{suffix}");
            assert_no_temporary_restore_artifacts(&destination.0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn restore_source_family_must_be_sidecar_free_before_copy() {
        use std::os::unix::fs::PermissionsExt;

        let source = TestDirectory::new("restore-source-journal");
        let source_repository = create_repository(&source.database());
        let backup = source_repository.backup_before_migration().unwrap();
        source_repository.close().unwrap();
        let journal = super::auxiliary_path(&backup, "-journal").unwrap();
        fs::write(&journal, b"").unwrap();
        fs::set_permissions(&journal, fs::Permissions::from_mode(0o600)).unwrap();
        let before = family_snapshot(&backup);
        let destination = TestDirectory::new("restore-source-journal-destination");

        let error = ControlRepository::restore_offline(&backup, &destination.database())
            .expect_err("restore source must be standalone before it is copied");

        assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
        assert_eq!(family_snapshot(&backup), before);
        assert!(!destination.database().exists());
    }

    #[test]
    fn checkpoint_truncates_the_wal_before_an_offline_transition() {
        let directory = TestDirectory::new("checkpoint-truncate");
        let mut repository = create_repository(&directory.database());
        repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE control_meta SET last_committed_at_unix_ms = '2' WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let wal = super::auxiliary_path(&directory.database(), "-wal").unwrap();
        assert!(fs::metadata(&wal).unwrap().len() > 0);

        repository.checkpoint().unwrap();

        assert_eq!(fs::metadata(&wal).unwrap().len(), 0);
        repository.close().unwrap();
    }

    #[test]
    fn closed_image_guards_keep_the_identity_claimed_until_safe_release() {
        let directory = TestDirectory::new("closed-image-claim");
        let path = directory.database();
        create_repository(&path).close().unwrap();

        let image = super::open_validated_image(&path, Some(node_id())).unwrap();
        let closed = super::close_into_image(image).unwrap();
        let error =
            ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
                .unwrap_err();
        assert_eq!(error.class(), RepositoryErrorClass::AlreadyOwned);

        closed.release().unwrap();
        ControlRepository::open_or_create(&path, node_id(), &mut CountingIds::default())
            .unwrap()
            .close()
            .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn corrupt_destination_is_refused_unchanged_before_restore() {
        use std::os::unix::fs::PermissionsExt;

        let source = TestDirectory::new("restore-corrupt-destination-source");
        let source_repository = create_repository(&source.database());
        let backup = source_repository.backup_before_migration().unwrap();
        source_repository.close().unwrap();

        let destination = TestDirectory::new("restore-corrupt-destination");
        let destination_path = destination.database();
        fs::write(&destination_path, b"not a control state database").unwrap();
        fs::set_permissions(&destination_path, fs::Permissions::from_mode(0o600)).unwrap();
        let before = family_snapshot(&destination_path);

        let error = ControlRepository::restore_offline(&backup, &destination_path)
            .expect_err("corrupt destinations require operator action");

        assert_eq!(error.class(), RepositoryErrorClass::Corrupt);
        assert_eq!(family_snapshot(&destination_path), before);
        assert_no_temporary_restore_artifacts(&destination.0);
    }

    #[test]
    fn restore_source_is_checkpointed_switched_closed_and_validated_standalone() {
        let directory = TestDirectory::new("restore-source-order");
        let path = directory.database();
        create_repository(&path).close().unwrap();
        let image = super::open_validated_image(&path, Some(node_id())).unwrap();
        let mut trace = Vec::new();

        let closed = super::close_into_image_traced(image, |event| {
            trace.push(event);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            trace,
            [
                super::ImageCloseEvent::CheckpointTruncated,
                super::ImageCloseEvent::JournalModeDelete,
                super::ImageCloseEvent::SqliteClosed,
                super::ImageCloseEvent::SidecarsAbsent,
                super::ImageCloseEvent::ReadOnlyValidated,
            ]
        );
        assert_eq!(closed.summary.revision, 1);
        for suffix in ["-wal", "-journal", "-shm"] {
            assert!(!super::auxiliary_path(&path, suffix).unwrap().exists());
        }
        closed.release().unwrap();
    }

    #[test]
    fn every_restore_fault_keeps_the_destination_as_a_complete_old_or_new_lineage() {
        assert_fault_matrix_subprocess("restore_crash_matrix");
    }

    fn assert_restore_fault_matrix() {
        for point in [
            super::RestoreBoundary::BeforeSourceCopy,
            super::RestoreBoundary::AfterSourceCopy,
            super::RestoreBoundary::SourceCheckpointTruncated,
            super::RestoreBoundary::SourceJournalModeDelete,
            super::RestoreBoundary::SourceSqliteClosed,
            super::RestoreBoundary::SourceSidecarsAbsent,
            super::RestoreBoundary::SourceReadOnlyValidated,
            super::RestoreBoundary::DestinationCheckpointTruncated,
            super::RestoreBoundary::DestinationJournalModeDelete,
            super::RestoreBoundary::DestinationSqliteClosed,
            super::RestoreBoundary::DestinationSidecarsAbsent,
            super::RestoreBoundary::DestinationReadOnlyValidated,
            super::RestoreBoundary::BeforeRename,
            super::RestoreBoundary::AfterRename,
            super::RestoreBoundary::AfterDirectorySync,
            super::RestoreBoundary::ReopenValidated,
        ] {
            let outcome = run_restore_fault_subprocess(point);
            assert!(
                outcome.reached,
                "fault point was not reached: {point:?}\n{}",
                outcome.output
            );
            assert_eq!(outcome.exit_code, 86, "{point:?}");
            assert_canonical_singleton(&outcome.destination_summary);
            assert!(outcome.backup_digest_unchanged, "{point:?}");
            assert_temporary_recovery_disposition(&outcome, point);
            assert!(
                matches!(outcome.destination_summary.revision, 1 | 42),
                "{point:?}"
            );
            if point == super::RestoreBoundary::DestinationCheckpointTruncated {
                assert!(
                    outcome.present_sidecars.is_empty() || outcome.present_sidecars == ["-wal"],
                    "{point:?}: {:?}",
                    outcome.present_sidecars
                );
            } else {
                assert!(
                    outcome.destination_sidecars_absent,
                    "{point:?}: {:?}",
                    outcome.present_sidecars
                );
            }
            if matches!(
                point,
                super::RestoreBoundary::BeforeSourceCopy
                    | super::RestoreBoundary::AfterSourceCopy
                    | super::RestoreBoundary::SourceCheckpointTruncated
                    | super::RestoreBoundary::SourceJournalModeDelete
                    | super::RestoreBoundary::SourceSqliteClosed
                    | super::RestoreBoundary::SourceSidecarsAbsent
                    | super::RestoreBoundary::SourceReadOnlyValidated
                    | super::RestoreBoundary::DestinationCheckpointTruncated
                    | super::RestoreBoundary::DestinationJournalModeDelete
                    | super::RestoreBoundary::DestinationSqliteClosed
                    | super::RestoreBoundary::DestinationSidecarsAbsent
                    | super::RestoreBoundary::DestinationReadOnlyValidated
                    | super::RestoreBoundary::BeforeRename
            ) {
                assert_eq!(outcome.destination_summary.revision, 1, "{point:?}");
                assert_eq!(
                    outcome.destination_digest, outcome.old_destination_digest,
                    "{point:?}"
                );
            } else {
                assert_eq!(outcome.destination_summary.cursor, 1, "{point:?}");
                assert_eq!(outcome.destination_summary.event_rows, 1, "{point:?}");
                assert_eq!(
                    outcome.destination_summary.slot_id, outcome.backup_summary.slot_id,
                    "{point:?}"
                );
                assert_ne!(
                    outcome.destination_summary.epoch, outcome.backup_summary.epoch,
                    "{point:?}"
                );
            }
        }
    }

    #[test]
    fn every_returned_restore_failure_preserves_old_or_quarantines_exact_new_lineage() {
        assert_fault_matrix_subprocess("restore_failure_matrix");
    }

    fn assert_returned_restore_failure_matrix() {
        for point in restore_boundaries() {
            let outcome = run_restore_returned_failure_subprocess(point);
            assert!(outcome.reached, "{point:?}\n{}", outcome.output);
            assert_eq!(outcome.exit_code, 0, "{point:?}\n{}", outcome.output);
            assert_canonical_singleton(&outcome.destination_summary);
            assert!(outcome.backup_digest_unchanged, "{point:?}");
            assert_temporary_recovery_disposition(&outcome, point);
            assert!(
                outcome.output.contains("RESTORE_RETURNED:Durability"),
                "{point:?}\n{}",
                outcome.output
            );
            assert_eq!(
                outcome.destination_summary.revision,
                if is_pre_rename(point) { 1 } else { 42 },
                "{point:?}\n{}",
                outcome.output
            );
            assert!(outcome.destination_sidecars_absent, "{point:?}");
            if is_pre_rename(point) {
                assert_eq!(outcome.destination_digest, outcome.old_destination_digest);
            } else {
                assert_eq!(outcome.destination_summary.cursor, 1);
                assert_eq!(outcome.destination_summary.event_rows, 1);
                assert_eq!(
                    outcome.destination_summary.slot_id,
                    outcome.backup_summary.slot_id
                );
                assert_ne!(
                    outcome.destination_summary.epoch,
                    outcome.backup_summary.epoch
                );
            }
        }
    }

    #[test]
    fn every_migration_rollback_fault_keeps_one_complete_lineage() {
        assert_fault_matrix_subprocess("rollback_crash_matrix");
    }

    fn assert_migration_rollback_fault_matrix() {
        for point in [
            super::RestoreBoundary::BeforeSourceCopy,
            super::RestoreBoundary::AfterSourceCopy,
            super::RestoreBoundary::SourceCheckpointTruncated,
            super::RestoreBoundary::SourceJournalModeDelete,
            super::RestoreBoundary::SourceSqliteClosed,
            super::RestoreBoundary::SourceSidecarsAbsent,
            super::RestoreBoundary::SourceReadOnlyValidated,
            super::RestoreBoundary::DestinationCheckpointTruncated,
            super::RestoreBoundary::DestinationJournalModeDelete,
            super::RestoreBoundary::DestinationSqliteClosed,
            super::RestoreBoundary::DestinationSidecarsAbsent,
            super::RestoreBoundary::DestinationReadOnlyValidated,
            super::RestoreBoundary::BeforeRename,
            super::RestoreBoundary::AfterRename,
            super::RestoreBoundary::AfterDirectorySync,
            super::RestoreBoundary::ReopenValidated,
        ] {
            let outcome = run_migration_rollback_fault_subprocess(point);
            assert!(
                outcome.reached,
                "fault point was not reached: {point:?}\n{}",
                outcome.output
            );
            assert_eq!(outcome.exit_code, 86, "{point:?}");
            assert_canonical_singleton(&outcome.destination_summary);
            assert!(outcome.backup_digest_unchanged, "{point:?}");
            assert_temporary_recovery_disposition(&outcome, point);
            assert!(
                matches!(outcome.destination_summary.revision, 1 | 9),
                "{point:?}"
            );
            if point == super::RestoreBoundary::DestinationCheckpointTruncated {
                assert!(
                    outcome.present_sidecars.is_empty() || outcome.present_sidecars == ["-wal"],
                    "{point:?}: {:?}",
                    outcome.present_sidecars
                );
            } else {
                assert!(
                    outcome.destination_sidecars_absent,
                    "{point:?}: {:?}",
                    outcome.present_sidecars
                );
            }
            if matches!(
                point,
                super::RestoreBoundary::BeforeSourceCopy
                    | super::RestoreBoundary::AfterSourceCopy
                    | super::RestoreBoundary::SourceCheckpointTruncated
                    | super::RestoreBoundary::SourceJournalModeDelete
                    | super::RestoreBoundary::SourceSqliteClosed
                    | super::RestoreBoundary::SourceSidecarsAbsent
                    | super::RestoreBoundary::SourceReadOnlyValidated
                    | super::RestoreBoundary::DestinationCheckpointTruncated
                    | super::RestoreBoundary::DestinationJournalModeDelete
                    | super::RestoreBoundary::DestinationSqliteClosed
                    | super::RestoreBoundary::DestinationSidecarsAbsent
                    | super::RestoreBoundary::DestinationReadOnlyValidated
                    | super::RestoreBoundary::BeforeRename
            ) {
                assert_eq!(outcome.destination_summary.revision, 9, "{point:?}");
                assert_eq!(outcome.destination_digest, outcome.old_destination_digest);
            } else {
                assert_eq!(outcome.destination_summary, outcome.backup_summary);
                assert_eq!(outcome.destination_digest, outcome.backup_lineage_digest);
            }
        }
    }

    #[test]
    fn every_returned_rollback_failure_preserves_failed_or_quarantines_backup_lineage() {
        assert_fault_matrix_subprocess("rollback_failure_matrix");
    }

    fn assert_returned_migration_rollback_failure_matrix() {
        for point in restore_boundaries() {
            let outcome = run_migration_rollback_returned_failure_subprocess(point);
            assert!(outcome.reached, "{point:?}\n{}", outcome.output);
            assert_eq!(outcome.exit_code, 0, "{point:?}\n{}", outcome.output);
            assert_canonical_singleton(&outcome.destination_summary);
            assert!(outcome.backup_digest_unchanged, "{point:?}");
            assert!(
                outcome.output.contains("RESTORE_RETURNED:Durability"),
                "{point:?}\n{}",
                outcome.output
            );
            assert_eq!(
                outcome.destination_summary.revision,
                if is_pre_rename(point) { 9 } else { 1 },
                "{point:?}\n{}",
                outcome.output
            );
            assert!(outcome.destination_sidecars_absent, "{point:?}");
            assert_temporary_recovery_disposition(&outcome, point);
            if is_pre_rename(point) {
                assert_eq!(outcome.destination_digest, outcome.old_destination_digest);
            } else {
                assert_eq!(outcome.destination_summary, outcome.backup_summary);
                assert_eq!(outcome.destination_digest, outcome.backup_lineage_digest);
            }
        }
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
