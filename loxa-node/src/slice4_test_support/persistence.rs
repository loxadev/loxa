use crate::control_state::state_machine::{
    AdmissionRequest, MutationIds, RestartEvidence, Transition,
};
use crate::control_state::{
    ownership_unavailable_recovery_for_test, ControlIdGenerator, ControlRepository,
};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, SlotId, StreamEpoch, V2NodeCapabilities, V2OperationProgress,
    V2OperationStatus, V2SlotStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

const NODE_ID: &str = "91111111-1111-4111-8111-111111111111";
const SLOT_ID: &str = "92222222-2222-4222-8222-222222222222";
const EPOCH: &str = "93333333-3333-4333-8333-333333333333";
const EVENT_ID: &str = "95555555-5555-4555-8555-555555555555";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-{label}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Ids;

impl ControlIdGenerator for Ids {
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

#[derive(Default)]
struct MutationSequence {
    operation: u64,
    event: u64,
}

impl MutationIds for MutationSequence {
    fn new_operation_id(&mut self) -> OperationId {
        self.operation += 1;
        OperationId::from_str(&format!("9aaaaaaa-0000-4000-8000-{:012x}", self.operation)).unwrap()
    }

    fn new_event_id(&mut self) -> EventId {
        self.event += 1;
        EventId::from_str(&format!("9bbbbbbb-0000-4000-8000-{:012x}", self.event)).unwrap()
    }
}

fn publish(repository: &mut ControlRepository, ids: &mut MutationSequence) -> NodeInstanceId {
    let instance = NodeInstanceId::from_str("94444444-4444-4444-8444-444444444444").unwrap();
    repository
        .publish_instance(
            crate::control_state::state_machine::InstancePublication {
                node_instance_id: instance,
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
            ids,
        )
        .unwrap();
    instance
}

fn zero_progress() -> V2OperationProgress {
    V2OperationProgress {
        completed_bytes: DecimalU64::new(0),
        total_bytes: None,
    }
}

#[test]
fn fresh_persistence_has_one_desired_row_without_a_second_observed_projection() {
    let root = TestRoot::new("slice4-persistence-authority");
    let path = root.path().join("control-state.sqlite3");
    let repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();

    repository
        .read_transaction(|connection| {
            let intent: (String, Option<String>, String, Option<String>, String) = connection
                .query_row(
                    "SELECT desired_kind,desired_model_id,desired_revision,operation_id,reconciliation_state FROM slot_intent WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )?;
            assert_eq!(
                intent,
                (
                    "unloaded".to_owned(),
                    None,
                    "1".to_owned(),
                    None,
                    "settled".to_owned(),
                )
            );
            let intent_columns = connection
                .prepare("PRAGMA table_info(slot_intent)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            assert!(!intent_columns.iter().any(|column| {
                matches!(column.as_str(), "status" | "model_id" | "updated_revision")
            }));
            Ok(())
        })
        .unwrap();
    repository.close().unwrap();
}

#[test]
fn restart_terminalizes_each_old_operation_once_and_consumes_lifecycle_intent_in_order() {
    let root = TestRoot::new("slice4-restart-order");
    let path = root.path().join("control-state.sqlite3");
    let mut repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();
    let mut ids = MutationSequence::default();
    let instance = publish(&mut repository, &mut ids);
    let first_download = repository
        .admit(
            instance,
            AdmissionRequest::Download {
                model_id: "download-a".into(),
                progress: zero_progress(),
            },
            11,
            &mut ids,
        )
        .unwrap();
    let load = repository
        .admit(
            instance,
            AdmissionRequest::Load {
                model_id: "candidate".into(),
            },
            12,
            &mut ids,
        )
        .unwrap();
    repository
        .observe(
            instance,
            Transition::Started {
                operation_id: load.operation_id,
                progress: None,
            },
            13,
            &mut ids,
        )
        .unwrap();
    let last_download = repository
        .admit(
            instance,
            AdmissionRequest::Download {
                model_id: "download-b".into(),
                progress: zero_progress(),
            },
            14,
            &mut ids,
        )
        .unwrap();
    let captured_intent = repository.committed_state().unwrap().intent;
    let before_revision = repository.committed_state().unwrap().revision.get();

    let reconciled = repository
        .reconcile_restart(
            RestartEvidence {
                lifecycle: ownership_unavailable_recovery_for_test(),
                captured_intent,
            },
            20,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 3);
    assert_eq!(
        reconciled
            .receipts
            .iter()
            .map(|receipt| receipt.revision.get())
            .collect::<Vec<_>>(),
        vec![
            before_revision + 1,
            before_revision + 2,
            before_revision + 3
        ]
    );
    let state = repository.committed_state().unwrap();
    assert_eq!(state.slot.status, V2SlotStatus::Recovery);
    for operation_id in [
        first_download.operation_id,
        load.operation_id,
        last_download.operation_id,
    ] {
        assert_eq!(
            state
                .operations
                .iter()
                .find(|operation| operation.operation_id == operation_id)
                .unwrap()
                .status,
            V2OperationStatus::Failed
        );
    }
    assert_eq!(
        state
            .events
            .iter()
            .rev()
            .take(3)
            .map(|event| event.operation_id.unwrap())
            .collect::<Vec<_>>(),
        vec![
            last_download.operation_id,
            load.operation_id,
            first_download.operation_id,
        ]
    );

    let second = repository
        .reconcile_restart(
            RestartEvidence {
                lifecycle: ownership_unavailable_recovery_for_test(),
                captured_intent: state.intent,
            },
            21,
            &mut ids,
        )
        .unwrap();
    assert!(second.receipts.is_empty());
    repository.close().unwrap();
}

#[test]
fn restart_without_old_lifecycle_uses_one_slot_intent_only_transaction_when_needed() {
    let root = TestRoot::new("slice4-restart-slot-only");
    let path = root.path().join("control-state.sqlite3");
    let mut repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();
    let mut ids = MutationSequence::default();
    publish(&mut repository, &mut ids);
    let captured_intent = repository.committed_state().unwrap().intent;
    let before = repository.committed_state().unwrap().revision.get();
    let reconciled = repository
        .reconcile_restart(
            RestartEvidence {
                lifecycle: ownership_unavailable_recovery_for_test(),
                captured_intent,
            },
            20,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 1);
    assert_eq!(reconciled.receipts[0].revision.get(), before + 1);
    assert_eq!(
        repository.committed_state().unwrap().slot.status,
        V2SlotStatus::Recovery
    );
    repository.close().unwrap();
}

#[test]
fn safe_migration_recovery_shape_is_consumed_without_replaying_the_old_operation() {
    let root = TestRoot::new("slice4-safe-migration-recovery");
    let path = root.path().join("control-state.sqlite3");
    let mut repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();
    let mut ids = MutationSequence::default();
    let instance = publish(&mut repository, &mut ids);
    let old_download = repository
        .admit(
            instance,
            AdmissionRequest::Download {
                model_id: "legacy-wrong-kind".into(),
                progress: zero_progress(),
            },
            11,
            &mut ids,
        )
        .unwrap();
    repository
        .transaction(|transaction| {
            transaction.execute(
                "UPDATE slot_state SET status='loading',model_id=NULL,operation_id=?1 WHERE singleton=1",
                [old_download.operation_id.to_string()],
            )?;
            transaction.execute(
                "UPDATE slot_intent SET desired_kind='unknown',desired_model_id=NULL,desired_revision='1',operation_id=?1,reconciliation_state='recovery_required',reason_code='migration_operation_mismatch' WHERE singleton=1",
                [old_download.operation_id.to_string()],
            )?;
            Ok(())
        })
        .unwrap();
    repository.validate_all().unwrap();
    assert!(repository.specialized_migration_recovery_is_safe().unwrap());
    let captured_intent = repository.committed_state().unwrap().intent;
    let reconciled = repository
        .reconcile_restart(
            RestartEvidence {
                lifecycle: ownership_unavailable_recovery_for_test(),
                captured_intent,
            },
            20,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 2);
    let state = repository.committed_state().unwrap();
    assert_eq!(state.slot.status, V2SlotStatus::Recovery);
    assert_eq!(
        state
            .operations
            .iter()
            .find(|operation| operation.operation_id == old_download.operation_id)
            .unwrap()
            .status,
        V2OperationStatus::Failed
    );
    repository.validate_all().unwrap();
    repository.close().unwrap();
}

#[test]
fn retained_null_model_load_migration_recovery_remains_fail_closed_and_nonmutating() {
    let root = TestRoot::new("slice4-unsafe-migration-recovery");
    let path = root.path().join("control-state.sqlite3");
    let mut repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();
    let mut ids = MutationSequence::default();
    let instance = publish(&mut repository, &mut ids);
    let old_load = repository
        .admit(
            instance,
            AdmissionRequest::Load {
                model_id: "lost-target".into(),
            },
            11,
            &mut ids,
        )
        .unwrap();
    repository
        .transaction(|transaction| {
            transaction.execute(
                "UPDATE operations SET model_id=NULL WHERE operation_id=?1",
                [old_load.operation_id.to_string()],
            )?;
            transaction.execute(
                "UPDATE slot_intent SET desired_kind='unknown',desired_model_id=NULL,desired_revision=?1,operation_id=?2,reconciliation_state='recovery_required',reason_code='migration_operation_mismatch' WHERE singleton=1",
                rusqlite::params![old_load.revision.to_string(), old_load.operation_id.to_string()],
            )?;
            Ok(())
        })
        .unwrap();
    repository.validate_all().unwrap();
    let before = repository
        .read_transaction(|connection| {
            Ok(connection.query_row(
                "SELECT status,model_id FROM operations WHERE operation_id=?1",
                [old_load.operation_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )?)
        })
        .unwrap();
    assert!(!repository.specialized_migration_recovery_is_safe().unwrap());
    let after = repository
        .read_transaction(|connection| {
            Ok(connection.query_row(
                "SELECT status,model_id FROM operations WHERE operation_id=?1",
                [old_load.operation_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )?)
        })
        .unwrap();
    assert_eq!(after, before);
    repository.close().unwrap();
}
