//! Storage-family fixtures shared by the control-state repository tests.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TestRoot(PathBuf);

impl TestRoot {
    pub(crate) fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-control-state-{label}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).expect("create storage test root");
        Self(fs::canonicalize(path).expect("canonicalize storage test root"))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
        let displaced = self.0.with_extension("displaced");
        let _ = fs::remove_dir_all(displaced);
    }
}

pub(crate) fn assert_private_repository_parent(path: &Path) {
    let parent = fs::canonicalize(path.parent().expect("repository path has a parent"))
        .expect("canonicalize repository parent");
    let temp = fs::canonicalize(std::env::temp_dir()).expect("canonicalize system temp directory");
    assert_ne!(
        parent, temp,
        "repository fixture must not use the shared temp directory directly"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let metadata = fs::metadata(parent).expect("read repository parent metadata");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuxiliaryKind {
    Wal,
    Journal,
    Backup,
    Shm,
}

impl AuxiliaryKind {
    pub(crate) const ALL: [Self; 4] = [Self::Wal, Self::Journal, Self::Backup, Self::Shm];

    pub(crate) fn path(self, main: &Path) -> PathBuf {
        let suffix = match self {
            Self::Wal => "-wal",
            Self::Journal => "-journal",
            Self::Backup => ".pre-migration.bak",
            Self::Shm => "-shm",
        };
        main.with_file_name(format!(
            "{}{}",
            main.file_name().expect("main filename").to_string_lossy(),
            suffix
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuxiliaryDefect {
    Symlink,
    NonRegular,
    HardLinked,
    WrongMode,
    WrongOwner,
}

impl AuxiliaryDefect {
    pub(crate) const ALL: [Self; 5] = [
        Self::Symlink,
        Self::NonRegular,
        Self::HardLinked,
        Self::WrongMode,
        Self::WrongOwner,
    ];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FamilySnapshot {
    main: EntrySnapshot,
    auxiliary: Vec<(AuxiliaryKind, EntrySnapshot)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EntrySnapshot {
    Missing,
    File {
        bytes: Vec<u8>,
        mode: u32,
        owner: u32,
        links: u64,
        device: u64,
        inode: u64,
    },
    Directory {
        mode: u32,
        owner: u32,
        device: u64,
        inode: u64,
    },
    Symlink {
        target: PathBuf,
        target_snapshot: Box<EntrySnapshot>,
    },
}

pub(crate) fn family_snapshot(main: &Path) -> FamilySnapshot {
    FamilySnapshot {
        main: entry_snapshot(main),
        auxiliary: AuxiliaryKind::ALL
            .into_iter()
            .map(|kind| (kind, entry_snapshot(&kind.path(main))))
            .collect(),
    }
}

#[cfg(unix)]
fn entry_snapshot(path: &Path) -> EntrySnapshot {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let Ok(metadata) = fs::symlink_metadata(path) else {
        return EntrySnapshot::Missing;
    };
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(path).expect("read auxiliary symlink");
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().expect("symlink parent").join(&target)
        };
        return EntrySnapshot::Symlink {
            target,
            target_snapshot: Box::new(entry_snapshot(&resolved)),
        };
    }
    if metadata.file_type().is_dir() {
        return EntrySnapshot::Directory {
            mode: metadata.permissions().mode() & 0o777,
            owner: metadata.uid(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
    }
    EntrySnapshot::File {
        bytes: fs::read(path).expect("read auxiliary file"),
        mode: metadata.permissions().mode() & 0o777,
        owner: metadata.uid(),
        links: metadata.nlink(),
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn entry_snapshot(path: &Path) -> EntrySnapshot {
    fs::read(path).map_or(EntrySnapshot::Missing, |bytes| EntrySnapshot::File {
        bytes,
        mode: 0,
        owner: 0,
        links: 1,
        device: 0,
        inode: 0,
    })
}

#[cfg(unix)]
pub(crate) fn apply_auxiliary_defect(main: &Path, kind: AuxiliaryKind, defect: AuxiliaryDefect) {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let path = kind.path(main);
    if defect == AuxiliaryDefect::WrongOwner {
        if !path.exists() {
            fs::write(&path, b"owner-policy-probe").expect("create owner policy probe");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("secure owner policy probe");
        }
        return;
    }
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if metadata.file_type().is_dir() {
            fs::remove_dir(&path).expect("remove prior auxiliary directory");
        } else {
            fs::remove_file(&path).expect("remove prior auxiliary file");
        }
    }
    let target = path.with_extension(format!(
        "defect-{}",
        NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    match defect {
        AuxiliaryDefect::Symlink => {
            fs::write(&target, b"symlink target").expect("create symlink target");
            fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
                .expect("secure symlink target");
            symlink(&target, &path).expect("create auxiliary symlink");
        }
        AuxiliaryDefect::NonRegular => {
            let mut builder = fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
            builder.create(&path).expect("create auxiliary directory");
        }
        AuxiliaryDefect::HardLinked => {
            fs::write(&target, b"hard-link target").expect("create hard-link target");
            fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
                .expect("secure hard-link target");
            fs::hard_link(&target, &path).expect("create auxiliary hard link");
        }
        AuxiliaryDefect::WrongMode => {
            fs::write(&path, b"wrong-mode auxiliary").expect("create wrong-mode auxiliary");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
                .expect("set broad auxiliary mode");
        }
        AuxiliaryDefect::WrongOwner => unreachable!(),
    }
}

#[cfg(test)]
pub(super) mod slice4_migration {
    use super::TestRoot;
    use crate::control_state::repository::{
        arm_migration_statement_fault_for_test, migrate_v1_to_v2, ControlIdGenerator,
        ControlRepository, DesiredKind, IntentReason, MigrationStatementFault, ReconciliationState,
        RepositoryErrorClass, StoredSlotIntent,
    };
    use crate::control_state::schema::{
        migration_2_checksum, schema_checksum, MIGRATION_2_NAME, SCHEMA_V1,
    };
    use loxa_protocol::v2::{
        DecimalU64, EventId, SlotId, StreamEpoch, V2ControlEvent, V2EventEntity, V2Slot,
        V2SlotStatus, V2_SCHEMA_VERSION,
    };
    use loxa_protocol::NodeId;
    use rusqlite::{params, Connection};
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    pub(super) const NODE_ID: &str = "81111111-1111-4111-8111-111111111111";
    pub(super) const SLOT_ID: &str = "82222222-2222-4222-8222-222222222222";
    const EPOCH: &str = "83333333-3333-4333-8333-333333333333";
    const EVENT_ID: &str = "85555555-5555-4555-8555-555555555555";
    const INSTANCE_ID: &str = "84444444-4444-4444-8444-444444444444";
    const OPERATION_ID: &str = "86666666-6666-4666-8666-666666666666";

    #[derive(Default)]
    struct NoIds;

    impl ControlIdGenerator for NoIds {
        fn new_slot_id(&mut self) -> SlotId {
            panic!("v1 migration must not generate a slot ID")
        }

        fn new_stream_epoch(&mut self) -> StreamEpoch {
            panic!("v1 migration must not generate a stream epoch")
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(super) struct SlotSnapshot {
        pub(super) status: String,
        pub(super) model_id: Option<String>,
        pub(super) operation_id: Option<String>,
        pub(super) updated_revision: u64,
    }

    #[derive(Debug)]
    pub(crate) struct OpenedV2 {
        pub(crate) repository: ControlRepository,
        pub(crate) intent: StoredSlotIntent,
        pub(super) slot: SlotSnapshot,
    }

    pub(crate) struct V1Fixture {
        _root: TestRoot,
        pub(super) path: PathBuf,
    }

    impl V1Fixture {
        pub(crate) fn new(label: &str) -> Self {
            let root = TestRoot::new(&format!("slice4-migration-{label}"));
            let path = root.path().join("control-state.sqlite3");
            create_v1_database(&path);
            Self { _root: root, path }
        }

        pub(super) fn connection(&self) -> Connection {
            Connection::open(&self.path).unwrap()
        }

        pub(super) fn set_v1_slot(
            &self,
            status: &str,
            model_id: Option<&str>,
            operation: Option<OperationFixture<'_>>,
        ) {
            let connection = self.connection();
            connection.execute("DELETE FROM operations", []).unwrap();
            let operation_id = operation.as_ref().map(|operation| operation.operation_id);
            if let Some(operation) = operation {
                connection
                    .execute(
                        "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,v1_ordinal,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES(?1,?2,?3,1,?4,?5,?6,?7,?7,?7,?7)",
                        params![operation.operation_id, operation.slot_id, INSTANCE_ID, operation.kind, operation.status, operation.model_id, operation.created_revision.to_string()],
                    )
                    .unwrap();
            }
            let (error_code, error_message) = if status == "recovery" {
                (
                    Some("lifecycle_recovery_required"),
                    Some("preexisting recovery evidence"),
                )
            } else {
                (None, None)
            };
            connection
                .execute(
                    "UPDATE slot_state SET status=?1,model_id=?2,operation_id=?3,error_code=?4,error_message=?5,updated_revision='8',updated_at_unix_ms='8' WHERE singleton=1",
                    params![status, model_id, operation_id, error_code, error_message],
                )
                .unwrap();
        }

        pub(super) fn set_v1_slot_loading(
            &self,
            observed_model_id: Option<&str>,
            operation: OperationFixture<'_>,
        ) {
            self.set_v1_slot("loading", observed_model_id, Some(operation));
        }

        pub(crate) fn reopen(&self) -> Result<OpenedV2, RepositoryErrorClass> {
            let repository = ControlRepository::open_or_create(
                &self.path,
                NodeId::from_str(NODE_ID).unwrap(),
                &mut NoIds,
            )
            .map_err(|error| error.class())?;
            let intent = repository
                .stored_slot_intent()
                .map_err(|error| error.class())?;
            let slot = repository
                .read_transaction(|connection| {
                    let raw: (String, Option<String>, Option<String>, String) = connection
                        .query_row(
                            "SELECT status,model_id,operation_id,updated_revision FROM slot_state WHERE singleton=1",
                            [],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                        )?;
                    Ok(SlotSnapshot {
                        status: raw.0,
                        model_id: raw.1,
                        operation_id: raw.2,
                        updated_revision: raw.3.parse().unwrap(),
                    })
                })
                .map_err(|error| error.class())?;
            Ok(OpenedV2 {
                repository,
                intent,
                slot,
            })
        }

        pub(crate) fn raw_schema_version(&self) -> i64 {
            self.connection()
                .query_row(
                    "SELECT schema_version FROM control_meta WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        }

        fn logical_snapshot(&self) -> Vec<String> {
            logical_snapshot(&self.connection())
        }
    }

    #[derive(Clone, Copy)]
    pub(super) struct OperationFixture<'a> {
        operation_id: &'a str,
        slot_id: &'a str,
        kind: &'a str,
        status: &'a str,
        model_id: Option<&'a str>,
        created_revision: u64,
    }

    pub(super) fn load_operation(model_id: &str, revision: u64) -> OperationFixture<'_> {
        OperationFixture {
            operation_id: OPERATION_ID,
            slot_id: SLOT_ID,
            kind: "load",
            status: "running",
            model_id: Some(model_id),
            created_revision: revision,
        }
    }

    fn unload_operation(revision: u64) -> OperationFixture<'static> {
        OperationFixture {
            operation_id: OPERATION_ID,
            slot_id: SLOT_ID,
            kind: "unload",
            status: "running",
            model_id: None,
            created_revision: revision,
        }
    }

    fn create_v1_database(path: &Path) {
        let connection = Connection::open(path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        connection.execute_batch("PRAGMA foreign_keys=ON").unwrap();
        connection.execute_batch(SCHEMA_V1).unwrap();
        connection
            .execute(
                "INSERT INTO loxa_schema_migrations(version,name,checksum,applied_at_ms) VALUES(1,'capacity_one_control_state',?1,1)",
                [schema_checksum()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO control_meta VALUES(1,?1,?2,?3,'20','1',1,'fresh','20')",
                [NODE_ID, SLOT_ID, EPOCH],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO node_state VALUES(1,?1,NULL,NULL,'unpublished',0,0,0,0,0)",
                [NODE_ID],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO slot_state VALUES(1,?1,'default','unloaded',NULL,NULL,NULL,NULL,'8','8')",
                [SLOT_ID],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO events VALUES(?1,?2,'1','1',NULL,NULL,'initialized',?3)",
                [EVENT_ID, EPOCH, &initial_event_payload()],
            )
            .unwrap();
    }

    fn initial_event_payload() -> String {
        let node_id = NodeId::from_str(NODE_ID).unwrap();
        let slot_id = SlotId::from_str(SLOT_ID).unwrap();
        serde_json::to_string(&V2ControlEvent {
            schema_version: V2_SCHEMA_VERSION,
            event_id: EventId::from_str(EVENT_ID).unwrap(),
            epoch: StreamEpoch::from_str(EPOCH).unwrap(),
            sequence: DecimalU64::new(1),
            revision: DecimalU64::new(1),
            committed_at_unix_ms: DecimalU64::new(1),
            entity: V2EventEntity::Slot,
            entity_id: SLOT_ID.to_owned(),
            node_id,
            node_instance_id: None,
            slot_id: Some(slot_id),
            operation_id: None,
            node: None,
            slot: Some(V2Slot {
                slot_id,
                node_id,
                name: "default".to_owned(),
                status: V2SlotStatus::Unloaded,
                model_id: None,
                operation_id: None,
                error: None,
            }),
            operation: None,
        })
        .unwrap()
    }

    fn logical_snapshot(connection: &Connection) -> Vec<String> {
        let mut snapshot = Vec::new();
        let mut schema = connection
            .prepare(
                "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name",
            )
            .unwrap();
        for row in schema
            .query_map([], |row| {
                Ok(format!(
                    "{}|{}|{}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?.unwrap_or_default()
                ))
            })
            .unwrap()
        {
            snapshot.push(row.unwrap());
        }
        for (table, columns) in [
            (
                "loxa_schema_migrations",
                "version,name,checksum,applied_at_ms",
            ),
            (
                "control_meta",
                "singleton,node_id,slot_id,stream_epoch,revision,cursor,schema_version,migration_source,last_committed_at_unix_ms",
            ),
            (
                "node_state",
                "singleton,node_id,ifnull(node_instance_id,''),ifnull(control_endpoint,''),status,model_download,slot_load,slot_unload,operation_cancel,operation_stream",
            ),
            (
                "slot_state",
                "singleton,slot_id,name,status,ifnull(model_id,''),ifnull(operation_id,''),ifnull(error_code,''),ifnull(error_message,''),updated_revision,updated_at_unix_ms",
            ),
            (
                "operations",
                "operation_id,slot_id,admitting_node_instance_id,ifnull(v1_ordinal,''),kind,status,ifnull(model_id,''),created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms",
            ),
            (
                "events",
                "event_id,stream_epoch,sequence,revision,ifnull(node_instance_id,''),ifnull(v1_sequence,''),event_kind,payload_json",
            ),
        ] {
            let sql = format!("SELECT concat_ws('|',{columns}) FROM {table} ORDER BY rowid");
            let mut statement = connection.prepare(&sql).unwrap();
            for row in statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
            {
                snapshot.push(format!("{table}:{}", row.unwrap()));
            }
        }
        let has_intent: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='slot_intent')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if has_intent {
            let mut statement = connection
                .prepare(
                    "SELECT concat_ws('|',singleton,slot_id,desired_kind,ifnull(desired_model_id,''),desired_revision,ifnull(operation_id,''),reconciliation_state,ifnull(reason_code,'')) FROM slot_intent ORDER BY rowid",
                )
                .unwrap();
            for row in statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
            {
                snapshot.push(format!("slot_intent:{}", row.unwrap()));
            }
        }
        snapshot
    }

    fn assert_intent(
        actual: &StoredSlotIntent,
        desired_kind: DesiredKind,
        desired_model_id: Option<&str>,
        desired_revision: u64,
        operation_id: Option<&str>,
        reconciliation: ReconciliationState,
        reason: Option<IntentReason>,
    ) {
        assert_eq!(actual.desired_kind, desired_kind);
        assert_eq!(actual.desired_model_id.as_deref(), desired_model_id);
        assert_eq!(actual.desired_revision, desired_revision);
        assert_eq!(
            actual
                .operation_id
                .map(|operation_id| operation_id.to_string()),
            operation_id.map(str::to_owned)
        );
        assert_eq!(actual.reconciliation, reconciliation);
        assert_eq!(actual.reason, reason);
    }

    #[test]
    fn immutable_migration_checksums_are_pinned() {
        assert_eq!(
            schema_checksum(),
            "31697e7af6ac718e4d514590dfc81efd2c36a3a4bbf2c18966e8f2ae9479e45b"
        );
        assert_eq!(
            migration_2_checksum(),
            "372759908b8c6931b75426d37a9cef3876119788477e528e0cd4b9ccd5712546"
        );
        assert_eq!(MIGRATION_2_NAME, "execution_lane_intent");
    }

    #[test]
    fn fresh_repository_is_schema_two_with_exact_ordered_ledger() {
        let root = TestRoot::new("slice4-fresh-schema-two");
        let path = root.path().join("control-state.sqlite3");
        struct FreshIds;
        impl ControlIdGenerator for FreshIds {
            fn new_slot_id(&mut self) -> SlotId {
                SlotId::from_str(SLOT_ID).unwrap()
            }
            fn new_stream_epoch(&mut self) -> StreamEpoch {
                StreamEpoch::from_str(EPOCH).unwrap()
            }
            fn new_initial_event_id(&mut self) -> EventId {
                EventId::from_str(EVENT_ID).unwrap()
            }
        }
        let repository = ControlRepository::open_or_create(
            &path,
            NodeId::from_str(NODE_ID).unwrap(),
            &mut FreshIds,
        )
        .unwrap();
        repository
            .read_transaction(|connection| {
                let version: i64 =
                    connection.query_row("SELECT schema_version FROM control_meta", [], |row| {
                        row.get(0)
                    })?;
                let rows = connection
                    .prepare(
                        "SELECT version,name,checksum FROM loxa_schema_migrations ORDER BY version",
                    )?
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                assert_eq!(version, 2);
                assert_eq!(rows.len(), 2);
                assert_eq!(
                    rows[0],
                    (
                        1,
                        "capacity_one_control_state".to_owned(),
                        schema_checksum()
                    )
                );
                assert_eq!(
                    rows[1],
                    (2, MIGRATION_2_NAME.to_owned(), migration_2_checksum())
                );
                Ok(())
            })
            .unwrap();
        repository.close().unwrap();
    }

    #[test]
    fn every_valid_v1_slot_state_has_an_exact_backfill() {
        let unloaded = V1Fixture::new("unloaded");
        let opened = unloaded.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Unloaded,
            None,
            8,
            None,
            ReconciliationState::Settled,
            None,
        );
        opened.repository.close().unwrap();

        let ready = V1Fixture::new("ready");
        ready.set_v1_slot("ready", Some("ready-model"), None);
        let opened = ready.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Loaded,
            Some("ready-model"),
            8,
            None,
            ReconciliationState::Settled,
            None,
        );
        opened.repository.close().unwrap();

        let loading = V1Fixture::new("loading");
        loading.set_v1_slot_loading(None, load_operation("load-target", 9));
        let opened = loading.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Loaded,
            Some("load-target"),
            9,
            Some(OPERATION_ID),
            ReconciliationState::Applying,
            None,
        );
        opened.repository.close().unwrap();

        let unloading = V1Fixture::new("unloading");
        unloading.set_v1_slot("unloading", Some("ready-model"), Some(unload_operation(10)));
        let opened = unloading.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Unloaded,
            None,
            10,
            Some(OPERATION_ID),
            ReconciliationState::Applying,
            None,
        );
        opened.repository.close().unwrap();

        let recovery = V1Fixture::new("recovery");
        recovery.set_v1_slot("recovery", Some("uncertain-model"), None);
        let opened = recovery.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Unknown,
            None,
            8,
            None,
            ReconciliationState::RecoveryRequired,
            Some(IntentReason::PreexistingRecovery),
        );
        opened.repository.close().unwrap();
    }

    #[test]
    fn replacement_load_uses_the_operation_target_and_preserves_observed_truth() {
        let fixture = V1Fixture::new("replacement-load");
        fixture.set_v1_slot_loading(Some("prior-model"), load_operation("target-model", 9));
        let opened = fixture.reopen().unwrap();
        assert_eq!(opened.intent.desired_kind, DesiredKind::Loaded);
        assert_eq!(
            opened.intent.desired_model_id.as_deref(),
            Some("target-model")
        );
        assert_eq!(opened.intent.desired_revision, 9);
        assert_eq!(opened.slot.model_id.as_deref(), Some("prior-model"));
        opened.repository.close().unwrap();
    }

    #[test]
    fn missing_and_mismatched_v1_operations_fail_closed() {
        let missing = V1Fixture::new("missing-load-operation");
        missing.set_v1_slot_loading(None, load_operation("target", 9));
        {
            let connection = missing.connection();
            connection.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            connection.execute("DELETE FROM operations", []).unwrap();
        }
        let opened = missing.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Unknown,
            None,
            8,
            None,
            ReconciliationState::RecoveryRequired,
            Some(IntentReason::MigrationAmbiguousLoading),
        );
        opened.repository.close().unwrap();

        for (label, mut operation) in [
            ("wrong-kind", load_operation("target", 9)),
            ("terminal-status", load_operation("target", 9)),
            ("missing-target", load_operation("target", 9)),
        ] {
            match label {
                "wrong-kind" => operation.kind = "download",
                "terminal-status" => operation.status = "succeeded",
                "missing-target" => operation.model_id = None,
                _ => unreachable!(),
            }
            let fixture = V1Fixture::new(label);
            fixture.set_v1_slot_loading(None, operation);
            let opened = fixture.reopen().unwrap();
            assert_intent(
                &opened.intent,
                DesiredKind::Unknown,
                None,
                8,
                Some(OPERATION_ID),
                ReconciliationState::RecoveryRequired,
                Some(IntentReason::MigrationOperationMismatch),
            );
            opened.repository.close().unwrap();
        }

        let missing_unload = V1Fixture::new("missing-unload-operation");
        missing_unload.set_v1_slot("unloading", Some("ready-model"), Some(unload_operation(10)));
        missing_unload
            .connection()
            .execute("DELETE FROM operations", [])
            .unwrap();
        let opened = missing_unload.reopen().unwrap();
        assert_intent(
            &opened.intent,
            DesiredKind::Unknown,
            None,
            8,
            None,
            ReconciliationState::RecoveryRequired,
            Some(IntentReason::MigrationOperationMismatch),
        );
        opened.repository.close().unwrap();
    }

    #[test]
    fn unrelated_terminal_null_model_load_is_not_a_migration_exception() {
        let fixture = V1Fixture::new("unrelated-null-model-load");
        let mut missing_target = load_operation("target", 9);
        missing_target.model_id = None;
        fixture.set_v1_slot_loading(None, missing_target);
        let mut opened = fixture.reopen().unwrap();

        opened
            .repository
            .transaction(|transaction| {
                transaction.execute(
                    "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES('87777777-7777-4777-8777-777777777777',?1,?2,'load','succeeded',NULL,'10','10','10','10')",
                    [SLOT_ID, INSTANCE_ID],
                )?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            opened.repository.validate_all().unwrap_err().class(),
            RepositoryErrorClass::Corrupt
        );
        opened.repository.close().unwrap();
    }

    #[test]
    fn settled_intent_must_match_observed_disposition_and_revision() {
        let disposition = V1Fixture::new("settled-disposition-drift");
        disposition.set_v1_slot("ready", Some("ready-model"), None);
        let mut opened = disposition.reopen().unwrap();
        opened
            .repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE slot_intent SET desired_kind='unloaded',desired_model_id=NULL WHERE singleton=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            opened.repository.validate_all().unwrap_err().class(),
            RepositoryErrorClass::Corrupt
        );
        opened.repository.close().unwrap();

        let revision = V1Fixture::new("settled-revision-drift");
        revision.set_v1_slot("ready", Some("ready-model"), None);
        let mut opened = revision.reopen().unwrap();
        opened
            .repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE slot_intent SET desired_revision='7' WHERE singleton=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            opened.repository.validate_all().unwrap_err().class(),
            RepositoryErrorClass::Corrupt
        );
        opened.repository.close().unwrap();
    }

    #[test]
    fn applying_intent_must_match_the_exact_active_lifecycle_operation() {
        let model = V1Fixture::new("applying-model-drift");
        model.set_v1_slot_loading(None, load_operation("target", 9));
        let mut opened = model.reopen().unwrap();
        opened
            .repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE slot_intent SET desired_model_id='other-target' WHERE singleton=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            opened.repository.validate_all().unwrap_err().class(),
            RepositoryErrorClass::Corrupt
        );
        opened.repository.close().unwrap();

        let revision = V1Fixture::new("applying-created-revision-drift");
        revision.set_v1_slot_loading(None, load_operation("target", 9));
        let mut opened = revision.reopen().unwrap();
        opened
            .repository
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE slot_intent SET desired_revision='8' WHERE singleton=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            opened.repository.validate_all().unwrap_err().class(),
            RepositoryErrorClass::Corrupt
        );
        opened.repository.close().unwrap();

        for (label, kind, status, operation_id) in [
            (
                "applying-kind-drift",
                "download",
                "running",
                "87777777-7777-4777-8777-777777777777",
            ),
            (
                "applying-status-drift",
                "load",
                "succeeded",
                "88888888-8888-4888-8888-888888888888",
            ),
        ] {
            let fixture = V1Fixture::new(label);
            fixture.set_v1_slot_loading(None, load_operation("target", 9));
            let mut opened = fixture.reopen().unwrap();
            opened
                .repository
                .transaction(|transaction| {
                    transaction.execute(
                        "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES(?1,?2,?3,?4,?5,'target','10','10','10','10')",
                        [operation_id, SLOT_ID, INSTANCE_ID, kind, status],
                    )?;
                    transaction.execute(
                        "UPDATE slot_intent SET operation_id=?1,desired_revision='10' WHERE singleton=1",
                        [operation_id],
                    )?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                opened.repository.validate_all().unwrap_err().class(),
                RepositoryErrorClass::Corrupt
            );
            opened.repository.close().unwrap();
        }
    }

    #[test]
    fn a_second_open_is_a_migration_noop() {
        let fixture = V1Fixture::new("repeat-open");
        let first = fixture.reopen().unwrap();
        let before = first
            .repository
            .read_transaction(|connection| Ok(logical_snapshot(connection)))
            .unwrap();
        first.repository.close().unwrap();
        let second = fixture.reopen().unwrap();
        let after = second
            .repository
            .read_transaction(|connection| Ok(logical_snapshot(connection)))
            .unwrap();
        assert_eq!(after, before);
        second.repository.close().unwrap();
    }

    #[test]
    fn every_migration_statement_boundary_rolls_back_to_exact_v1() {
        for completed_statements in 0..=11 {
            let fixture = V1Fixture::new(&format!("rollback-{completed_statements}"));
            fixture.set_v1_slot_loading(None, load_operation("target", 9));
            let before = fixture.logical_snapshot();
            let _fault = arm_migration_statement_fault_for_test(
                MigrationStatementFault::AfterStatement(completed_statements),
            );
            let error = fixture.reopen().unwrap_err();
            assert_eq!(error, RepositoryErrorClass::Durability);
            assert_eq!(fixture.logical_snapshot(), before);
            assert_eq!(fixture.raw_schema_version(), 1);
            let ledger_rows: i64 = fixture
                .connection()
                .query_row("SELECT COUNT(*) FROM loxa_schema_migrations", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(ledger_rows, 1);
        }
    }

    #[test]
    fn migration_fault_scope_does_not_leak_across_parallel_worker_reuse() {
        let corrupt = V1Fixture::new("fault-scope-corrupt");
        corrupt
            .connection()
            .execute(
                "UPDATE loxa_schema_migrations SET checksum='tampered' WHERE version=1",
                [],
            )
            .unwrap();
        {
            let _fault =
                arm_migration_statement_fault_for_test(MigrationStatementFault::AfterStatement(0));
            assert_eq!(corrupt.reopen().unwrap_err(), RepositoryErrorClass::Corrupt);
        }

        let healthy = V1Fixture::new("fault-scope-healthy");
        let opened = healthy
            .reopen()
            .expect("a worker reused after a pre-hook failure must be clean");
        opened.repository.close().unwrap();
    }

    #[test]
    fn foreign_keys_are_restored_after_success_and_rollback() {
        let rollback = V1Fixture::new("foreign-keys-rollback");
        let mut connection = rollback.connection();
        connection.execute_batch("PRAGMA foreign_keys=ON").unwrap();
        let _fault =
            arm_migration_statement_fault_for_test(MigrationStatementFault::AfterStatement(5));
        assert_eq!(
            migrate_v1_to_v2(&mut connection, 30).unwrap_err().class(),
            RepositoryErrorClass::Durability
        );
        let foreign_keys: i64 = connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
        assert_eq!(rollback.raw_schema_version(), 1);

        let success = V1Fixture::new("foreign-keys-success");
        let mut connection = success.connection();
        connection.execute_batch("PRAGMA foreign_keys=ON").unwrap();
        migrate_v1_to_v2(&mut connection, 30).unwrap();
        let foreign_keys: i64 = connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
        assert_eq!(success.raw_schema_version(), 2);
    }

    #[test]
    fn checksum_drift_newer_schema_shape_corruption_and_foreign_key_failure_are_unchanged() {
        let checksum = V1Fixture::new("checksum-drift");
        checksum
            .connection()
            .execute("UPDATE loxa_schema_migrations SET checksum='drifted'", [])
            .unwrap();
        let before = checksum.logical_snapshot();
        assert_eq!(
            checksum.reopen().unwrap_err(),
            RepositoryErrorClass::Corrupt
        );
        assert_eq!(checksum.logical_snapshot(), before);

        let shape = V1Fixture::new("shape-drift");
        shape
            .connection()
            .execute("DROP INDEX one_active_lifecycle_operation", [])
            .unwrap();
        let before = shape.logical_snapshot();
        assert_eq!(
            shape.reopen().unwrap_err(),
            RepositoryErrorClass::UnsupportedSchema
        );
        assert_eq!(shape.logical_snapshot(), before);

        let foreign_key = V1Fixture::new("foreign-key-check");
        {
            let connection = foreign_key.connection();
            connection.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            connection
                .execute(
                    "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES(?1,'89999999-9999-4999-8999-999999999999',?2,'download','succeeded','model','9','9','9','9')",
                    [OPERATION_ID, INSTANCE_ID],
                )
                .unwrap();
        }
        let before = foreign_key.logical_snapshot();
        assert_eq!(
            foreign_key.reopen().unwrap_err(),
            RepositoryErrorClass::Corrupt
        );
        assert_eq!(foreign_key.logical_snapshot(), before);
    }

    #[test]
    fn a_newer_migration_version_is_unsupported_and_unchanged() {
        let fixture = V1Fixture::new("newer-version");
        {
            let connection = fixture.connection();
            connection
                .execute_batch(
                    "CREATE TABLE loxa_schema_migrations_new(version INTEGER PRIMARY KEY CHECK(version IN (1,2,3)),name TEXT NOT NULL,checksum TEXT NOT NULL,applied_at_ms INTEGER NOT NULL CHECK(applied_at_ms >= 0)) STRICT;
                     INSERT INTO loxa_schema_migrations_new SELECT * FROM loxa_schema_migrations;
                     DROP TABLE loxa_schema_migrations;
                     ALTER TABLE loxa_schema_migrations_new RENAME TO loxa_schema_migrations;",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO loxa_schema_migrations VALUES(3,'future','future',3)",
                    [],
                )
                .unwrap();
        }
        let before = fixture.logical_snapshot();
        assert_eq!(
            fixture.reopen().unwrap_err(),
            RepositoryErrorClass::UnsupportedSchema
        );
        assert_eq!(fixture.logical_snapshot(), before);
    }

    #[test]
    fn migration_two_checksum_and_shape_drift_fail_closed_unchanged() {
        let checksum = V1Fixture::new("migration-two-checksum-drift");
        checksum.reopen().unwrap().repository.close().unwrap();
        checksum
            .connection()
            .execute(
                "UPDATE loxa_schema_migrations SET checksum='drifted-v2' WHERE version=2",
                [],
            )
            .unwrap();
        let before = checksum.logical_snapshot();
        assert_eq!(
            checksum.reopen().unwrap_err(),
            RepositoryErrorClass::Corrupt
        );
        assert_eq!(checksum.logical_snapshot(), before);

        let shape = V1Fixture::new("migration-two-shape-drift");
        shape.reopen().unwrap().repository.close().unwrap();
        shape
            .connection()
            .execute("ALTER TABLE slot_intent ADD COLUMN leaked TEXT", [])
            .unwrap();
        let before = shape.logical_snapshot();
        assert_eq!(
            shape.reopen().unwrap_err(),
            RepositoryErrorClass::UnsupportedSchema
        );
        assert_eq!(shape.logical_snapshot(), before);
    }

    #[test]
    fn migrated_backup_restores_schema_two_and_the_exact_intent() {
        let fixture = V1Fixture::new("backup-restore");
        fixture.set_v1_slot_loading(None, load_operation("target", 9));
        let opened = fixture.reopen().unwrap();
        let backup = opened.repository.backup_before_migration().unwrap();
        opened.repository.close().unwrap();

        let destination = TestRoot::new("slice4-migration-restore-destination");
        let destination_path = destination.path().join("control-state.sqlite3");
        ControlRepository::restore_offline(&backup, &destination_path).unwrap();
        let restored = ControlRepository::open_or_create(
            &destination_path,
            NodeId::from_str(NODE_ID).unwrap(),
            &mut NoIds,
        )
        .unwrap();
        let intent = restored.stored_slot_intent().unwrap();
        assert_eq!(intent.desired_kind, DesiredKind::Loaded);
        assert_eq!(intent.desired_model_id.as_deref(), Some("target"));
        assert_eq!(intent.desired_revision, 9);
        restored.close().unwrap();
    }

    #[test]
    fn v2_reopen_repairs_stale_v1_backup_at_post_commit_boundaries() {
        for boundary in ["post-commit", "post-sync", "backup-temporary"] {
            let fixture = V1Fixture::new(&format!("stale-backup-{boundary}"));
            fixture.set_v1_slot_loading(None, load_operation("target", 9));
            let mut backup_name = fixture.path.file_name().unwrap().to_os_string();
            backup_name.push(".pre-migration.bak");
            let backup = fixture.path.with_file_name(backup_name);
            std::fs::copy(&fixture.path, &backup).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&backup, std::fs::Permissions::from_mode(0o600)).unwrap();
            }

            let mut connection = fixture.connection();
            migrate_v1_to_v2(&mut connection, 30).unwrap();
            connection.close().unwrap();
            assert_eq!(fixture.raw_schema_version(), 2);
            assert_eq!(
                Connection::open(&backup)
                    .unwrap()
                    .query_row(
                        "SELECT schema_version FROM control_meta WHERE singleton=1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1
            );

            if boundary != "post-commit" {
                std::fs::File::open(&fixture.path)
                    .unwrap()
                    .sync_all()
                    .unwrap();
                std::fs::File::open(fixture.path.parent().unwrap())
                    .unwrap()
                    .sync_all()
                    .unwrap();
            }
            let orphaned_temporary = (boundary == "backup-temporary").then(|| {
                let mut name = backup.file_name().unwrap().to_os_string();
                name.push(".backup-crash.tmp");
                let temporary = backup.with_file_name(name);
                std::fs::copy(&fixture.path, &temporary).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                }
                temporary
            });

            let opened = fixture.reopen().unwrap();
            opened
                .repository
                .validate_backup(&backup)
                .expect("v2 reopen must replace the stale v1 migration backup");
            assert_eq!(
                Connection::open(&backup)
                    .unwrap()
                    .query_row(
                        "SELECT schema_version FROM control_meta WHERE singleton=1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                2
            );
            if let Some(temporary) = orphaned_temporary {
                assert!(!temporary.exists());
            }
            opened.repository.close().unwrap();
        }
    }
}
