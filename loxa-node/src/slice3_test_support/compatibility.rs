use crate::control_state::state_machine::{AdmissionRequest, InstancePublication, MutationIds};
use crate::control_state::{ControlIdGenerator, ControlRepository, RepositoryErrorClass};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, SlotId, StreamEpoch, V2NodeCapabilities, V2OperationProgress,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct CapturedDiagnosticsFixture {
    pub(crate) rendered: String,
    pub(crate) operation_ids: Vec<OperationId>,
    pub(crate) event_ids: Vec<EventId>,
    pub(crate) committed_operation_ids: BTreeSet<OperationId>,
    pub(crate) committed_event_ids: BTreeSet<EventId>,
}

pub(crate) fn captured_diagnostics_fixture(
    capture: impl FnOnce(&str) -> String,
) -> CapturedDiagnosticsFixture {
    let root = FixtureRoot::new("diagnostics-authority");
    let path = root.path().join("control-state.sqlite3");
    let node_id = NodeId::new_v4();
    let instance_id = NodeInstanceId::new_v4();
    let mut ids = FixtureIds;
    let mut repository =
        ControlRepository::open_or_create(&path, node_id, &mut ids).expect("open repository");
    repository
        .publish_instance(
            InstancePublication {
                node_instance_id: instance_id,
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: V2NodeCapabilities {
                    model_download: true,
                    slot_load: true,
                    slot_unload: true,
                    operation_cancel: true,
                    operation_stream: true,
                },
                now_unix_ms: 10,
            },
            &mut ids,
        )
        .expect("publish durable instance");
    let admission = repository
        .admit(
            instance_id,
            AdmissionRequest::Download {
                model_id: "diagnostics-fixture".into(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            },
            11,
            &mut ids,
        )
        .expect("commit durable operation");
    let state = repository.committed_state().expect("read committed state");
    let rendered = capture(&admission.operation_id.to_string());
    let records = rendered
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("valid diagnostics JSONL"))
        .collect::<Vec<_>>();
    let operation_ids = records
        .iter()
        .filter_map(|record| record.get("operation_id").and_then(Value::as_str))
        .map(|id| OperationId::from_str(id).expect("typed diagnostic operation ID"))
        .collect();
    let event_ids = records
        .iter()
        .filter_map(|record| record.get("event_id").and_then(Value::as_str))
        .map(|id| EventId::from_str(id).expect("typed diagnostic event ID"))
        .collect();
    let committed_operation_ids = state
        .operations
        .into_iter()
        .map(|operation| operation.operation_id)
        .collect();
    let committed_event_ids = state
        .events
        .into_iter()
        .map(|event| event.event_id)
        .collect();
    repository.close().expect("close repository");

    CapturedDiagnosticsFixture {
        rendered,
        operation_ids,
        event_ids,
        committed_operation_ids,
        committed_event_ids,
    }
}

struct FixtureIds;

impl ControlIdGenerator for FixtureIds {
    fn new_slot_id(&mut self) -> SlotId {
        SlotId::new_v4()
    }

    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::new_v4()
    }
}

impl MutationIds for FixtureIds {
    fn new_operation_id(&mut self) -> OperationId {
        OperationId::new_v4()
    }

    fn new_event_id(&mut self) -> EventId {
        EventId::new_v4()
    }
}

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-slice3-compatibility-{label}-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).expect("create compatibility root");
        Self(fs::canonicalize(path).expect("canonicalize compatibility root"))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DirectoryFamilySnapshot {
    directory: DirectoryMetadataSnapshot,
    entries: BTreeMap<PathBuf, FamilyEntrySnapshot>,
}

#[derive(Debug, Eq, PartialEq)]
struct DirectoryMetadataSnapshot {
    mode: u32,
    owner: u32,
    group: u32,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug, Eq, PartialEq)]
struct FamilyEntrySnapshot {
    bytes: Vec<u8>,
    mode: u32,
    owner: u32,
    group: u32,
    links: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[cfg(unix)]
fn directory_family_snapshot(directory: &Path) -> DirectoryFamilySnapshot {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let directory_metadata =
        fs::symlink_metadata(directory).expect("read family directory metadata");
    let directory_snapshot = DirectoryMetadataSnapshot {
        mode: directory_metadata.permissions().mode() & 0o777,
        owner: directory_metadata.uid(),
        group: directory_metadata.gid(),
        device: directory_metadata.dev(),
        inode: directory_metadata.ino(),
        modified_seconds: directory_metadata.mtime(),
        modified_nanoseconds: directory_metadata.mtime_nsec(),
        changed_seconds: directory_metadata.ctime(),
        changed_nanoseconds: directory_metadata.ctime_nsec(),
    };
    let entries = fs::read_dir(directory)
        .expect("read database-family directory")
        .map(|entry| {
            let entry = entry.expect("read database-family entry");
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("read database-family metadata");
            assert!(
                metadata.is_file(),
                "unexpected non-file family entry: {path:?}"
            );
            (
                path.file_name().expect("family filename").into(),
                FamilyEntrySnapshot {
                    bytes: fs::read(&path).expect("read database-family bytes"),
                    mode: metadata.permissions().mode() & 0o777,
                    owner: metadata.uid(),
                    group: metadata.gid(),
                    links: metadata.nlink(),
                    device: metadata.dev(),
                    inode: metadata.ino(),
                    modified_seconds: metadata.mtime(),
                    modified_nanoseconds: metadata.mtime_nsec(),
                    changed_seconds: metadata.ctime(),
                    changed_nanoseconds: metadata.ctime_nsec(),
                },
            )
        })
        .collect();
    DirectoryFamilySnapshot {
        directory: directory_snapshot,
        entries,
    }
}

#[cfg(unix)]
#[test]
fn nonempty_owner_lock_is_rejected_without_truncation_or_sqlite_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let root = FixtureRoot::new("nonempty-owner-lock");
    let path = root.path().join("control-state.sqlite3");
    let node_id = NodeId::new_v4();
    ControlRepository::open_or_create(&path, node_id, &mut FixtureIds)
        .expect("create repository fixture")
        .close()
        .expect("close repository fixture");
    let owner_lock = path.with_file_name("control-state.sqlite3.owner.lock");
    fs::write(&owner_lock, b"not-authority").expect("prepopulate nonempty owner lock");
    fs::set_permissions(&owner_lock, fs::Permissions::from_mode(0o600)).expect("secure owner lock");
    let before = directory_family_snapshot(root.path());

    let error = ControlRepository::open_or_create(&path, node_id, &mut FixtureIds)
        .expect_err("nonempty owner lock must fail closed");

    assert_eq!(error.class(), RepositoryErrorClass::UnsafePath);
    assert_eq!(directory_family_snapshot(root.path()), before);
}
