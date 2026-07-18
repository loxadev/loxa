use crate::control_state::repository::{
    ControlIdGenerator, ControlRepository, DesiredKind, ReconciliationState,
};
use crate::control_state::state_machine::test_support::storage::TestRoot;
use crate::control_state::state_machine::{
    AdmissionRequest, MutationIds, Transition, TransitionError,
};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, SlotId, StreamEpoch, V2OperationError, V2OperationErrorCode,
    V2OperationProgress, V2OperationStatus, V2SlotStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::PathBuf;
use std::str::FromStr;

const NODE_ID: &str = "11111111-1111-4111-8111-111111111111";
const SLOT_ID: &str = "22222222-2222-4222-8222-222222222222";
const EPOCH: &str = "33333333-3333-4333-8333-333333333333";
const INSTANCE: &str = "44444444-4444-4444-8444-444444444444";
const INITIAL_EVENT: &str = "55555555-5555-4555-8555-555555555555";

struct InitialIds;

impl ControlIdGenerator for InitialIds {
    fn new_slot_id(&mut self) -> SlotId {
        SlotId::from_str(SLOT_ID).unwrap()
    }

    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::from_str(EPOCH).unwrap()
    }

    fn new_initial_event_id(&mut self) -> EventId {
        EventId::from_str(INITIAL_EVENT).unwrap()
    }
}

#[derive(Default)]
struct DeterministicMutationIds {
    operation_calls: u64,
    event_calls: u64,
}

impl MutationIds for DeterministicMutationIds {
    fn new_operation_id(&mut self) -> OperationId {
        self.operation_calls += 1;
        OperationId::from_str(&format!(
            "aaaaaaaa-0000-4000-8000-{:012x}",
            self.operation_calls
        ))
        .unwrap()
    }

    fn new_event_id(&mut self) -> EventId {
        self.event_calls += 1;
        EventId::from_str(&format!(
            "bbbbbbbb-0000-4000-8000-{:012x}",
            self.event_calls
        ))
        .unwrap()
    }
}

struct MachineFixture {
    _root: TestRoot,
    repository: Option<ControlRepository>,
    ids: DeterministicMutationIds,
    now: u64,
    path: PathBuf,
}

impl MachineFixture {
    fn new() -> Self {
        let root = TestRoot::new("state-machine");
        let path = root.path().join("control-state.sqlite3");
        let mut repository = ControlRepository::open_or_create(
            &path,
            NodeId::from_str(NODE_ID).unwrap(),
            &mut InitialIds,
        )
        .unwrap();
        repository
            .transaction(|tx| {
                tx.execute(
                    "UPDATE node_state SET node_instance_id=?1,control_endpoint='http://127.0.0.1:19431',status='running',model_download=1,slot_load=1,slot_unload=1,operation_cancel=1,operation_stream=1 WHERE singleton=1",
                    [INSTANCE],
                )?;
                Ok(())
            })
            .unwrap();
        Self {
            _root: root,
            repository: Some(repository),
            ids: DeterministicMutationIds::default(),
            now: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            path,
        }
    }

    fn instance(&self) -> NodeInstanceId {
        NodeInstanceId::from_str(INSTANCE).unwrap()
    }

    fn advance(&mut self, millis: u64) {
        self.now += millis;
    }

    fn admit(
        &mut self,
        request: AdmissionRequest,
    ) -> Result<crate::control_state::state_machine::CommittedAdmission, TransitionError> {
        let instance = self.instance();
        self.repository
            .as_mut()
            .unwrap()
            .admit(instance, request, self.now, &mut self.ids)
    }

    fn admit_with_event_limit(
        &mut self,
        request: AdmissionRequest,
        limit: usize,
    ) -> Result<crate::control_state::state_machine::CommittedAdmission, TransitionError> {
        let instance = self.instance();
        self.repository.as_mut().unwrap().admit_with_event_limit(
            instance,
            request,
            self.now,
            &mut self.ids,
            limit,
        )
    }

    fn observe(
        &mut self,
        transition: Transition,
    ) -> Result<crate::control_state::state_machine::CommitReceipt, TransitionError> {
        let instance = self.instance();
        self.repository
            .as_mut()
            .unwrap()
            .observe(instance, transition, self.now, &mut self.ids)
    }

    fn observe_with_event_limit(
        &mut self,
        transition: Transition,
        limit: usize,
    ) -> Result<crate::control_state::state_machine::CommitReceipt, TransitionError> {
        let instance = self.instance();
        self.repository.as_mut().unwrap().observe_with_event_limit(
            instance,
            transition,
            self.now,
            &mut self.ids,
            limit,
        )
    }

    fn state(&self) -> crate::control_state::state_machine::CommittedState {
        self.repository.as_ref().unwrap().committed_state().unwrap()
    }

    fn validate_all(&self) {
        self.repository.as_ref().unwrap().validate_all().unwrap();
    }

    fn reopen(&mut self) {
        self.repository.take().unwrap().close().unwrap();
        self.repository = Some(
            ControlRepository::open_or_create(
                &self.path,
                NodeId::from_str(NODE_ID).unwrap(),
                &mut InitialIds,
            )
            .unwrap(),
        );
    }
}

impl Drop for MachineFixture {
    fn drop(&mut self) {
        if let Some(repository) = self.repository.take() {
            repository.close().unwrap();
        }
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}.owner.lock", self.path.display()));
        let _ = std::fs::remove_file(format!("{}.migration.bak", self.path.display()));
        let _ = std::fs::remove_file(format!("{}.migration.bak.owner.lock", self.path.display()));
    }
}

#[test]
fn machine_fixture_repository_parent_is_private() {
    let machine = MachineFixture::new();
    super::storage::assert_private_repository_parent(&machine.path);
}

fn zero_progress() -> V2OperationProgress {
    V2OperationProgress {
        completed_bytes: DecimalU64::new(0),
        total_bytes: None,
    }
}

fn download(model: &str) -> AdmissionRequest {
    AdmissionRequest::Download {
        model_id: model.to_owned(),
        progress: zero_progress(),
    }
}

fn assert_rejected_without_mutation(
    machine: &mut MachineFixture,
    transition: Transition,
    expected: TransitionError,
) {
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(machine.observe(transition).unwrap_err(), expected);
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn terminal_transition_and_slot_observation_commit_atomically_once() {
    let mut machine = MachineFixture::new();
    let admission = machine
        .admit(AdmissionRequest::Load {
            model_id: "model-a".into(),
        })
        .unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admission.operation_id,
            progress: None,
        })
        .unwrap();
    let receipt = machine
        .observe(Transition::Succeeded {
            operation_id: admission.operation_id,
            observed_model_id: Some("model-a".into()),
        })
        .unwrap();
    let state = machine.state();
    assert_eq!(state.revision, receipt.revision);
    assert_eq!(state.cursor, receipt.cursor);
    assert_eq!(state.slot.status, V2SlotStatus::Ready);
    assert_eq!(state.slot.model_id.as_deref(), Some("model-a"));
    assert_eq!(state.operations[0].status, V2OperationStatus::Succeeded);
    let intent = machine
        .repository
        .as_ref()
        .unwrap()
        .stored_slot_intent()
        .unwrap();
    assert_eq!(intent.desired_kind, DesiredKind::Loaded);
    assert_eq!(intent.desired_model_id.as_deref(), Some("model-a"));
    assert_eq!(intent.desired_revision, receipt.revision.get());
    assert_eq!(intent.operation_id, None);
    assert_eq!(intent.reconciliation, ReconciliationState::Settled);
    assert_eq!(intent.reason, None);
    assert_eq!(
        state
            .events
            .iter()
            .filter(|event| event.revision == receipt.revision)
            .count(),
        1
    );
    machine.validate_all();
}

#[test]
fn lifecycle_admission_atomically_writes_applying_intent() {
    let mut load = MachineFixture::new();
    let admitted = load
        .admit(AdmissionRequest::Load {
            model_id: "model-a".into(),
        })
        .unwrap();
    let intent = load
        .repository
        .as_ref()
        .unwrap()
        .stored_slot_intent()
        .unwrap();
    assert_eq!(intent.desired_kind, DesiredKind::Loaded);
    assert_eq!(intent.desired_model_id.as_deref(), Some("model-a"));
    assert_eq!(intent.desired_revision, admitted.revision.get());
    assert_eq!(intent.operation_id, Some(admitted.operation_id));
    assert_eq!(intent.reconciliation, ReconciliationState::Applying);
    assert_eq!(intent.reason, None);
    assert_eq!(load.state().slot.status, V2SlotStatus::Loading);
    load.validate_all();

    let mut unload = MachineFixture::new();
    unload
        .repository
        .as_mut()
        .unwrap()
        .transaction(|transaction| {
            transaction.execute(
                "UPDATE slot_state SET status='ready',model_id='model-a' WHERE singleton=1",
                [],
            )?;
            transaction.execute(
                "UPDATE slot_intent SET desired_kind='loaded',desired_model_id='model-a' WHERE singleton=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    unload.validate_all();
    let admitted = unload.admit(AdmissionRequest::Unload).unwrap();
    let intent = unload
        .repository
        .as_ref()
        .unwrap()
        .stored_slot_intent()
        .unwrap();
    assert_eq!(intent.desired_kind, DesiredKind::Unloaded);
    assert_eq!(intent.desired_model_id, None);
    assert_eq!(intent.desired_revision, admitted.revision.get());
    assert_eq!(intent.operation_id, Some(admitted.operation_id));
    assert_eq!(intent.reconciliation, ReconciliationState::Applying);
    assert_eq!(intent.reason, None);
    let state = unload.state();
    assert_eq!(state.slot.status, V2SlotStatus::Unloading);
    assert_eq!(state.operations[0].model_id, None);
    unload.validate_all();
}

#[test]
fn repeating_identical_terminal_transition_is_noop_but_contradiction_fails() {
    let mut machine = MachineFixture::new();
    let admission = machine.admit(download("model-a")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admission.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    let terminal = Transition::Succeeded {
        operation_id: admission.operation_id,
        observed_model_id: None,
    };
    machine.observe(terminal.clone()).unwrap();
    let before = machine.state();
    assert!(machine.observe(terminal).unwrap().is_noop());
    assert_eq!(machine.state(), before);
    assert_eq!(
        machine
            .observe(Transition::Cancelled {
                operation_id: admission.operation_id
            })
            .unwrap_err(),
        TransitionError::Contradiction
    );
}

#[test]
fn lifecycle_exclusivity_does_not_serialize_downloads() {
    let mut machine = MachineFixture::new();
    machine.admit(download("model-a")).unwrap();
    machine.admit(download("model-b")).unwrap();
    machine
        .admit(AdmissionRequest::Load {
            model_id: "model-a".into(),
        })
        .unwrap();
    assert_eq!(
        machine.admit(AdmissionRequest::Unload).unwrap_err(),
        TransitionError::LifecycleConflict
    );
}

#[test]
fn same_model_download_conflicts_before_id_allocation() {
    let mut machine = MachineFixture::new();
    machine.admit(download("model-a")).unwrap();
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine.admit(download("model-a")).unwrap_err(),
        TransitionError::SameModelConflict
    );
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn loading_null_model_unknown_total_and_public_download_projection_are_valid() {
    let mut machine = MachineFixture::new();
    let download = machine.admit(download("model-a")).unwrap();
    let load = machine
        .admit(AdmissionRequest::Load {
            model_id: "model-b".into(),
        })
        .unwrap();
    machine
        .observe(Transition::Started {
            operation_id: load.operation_id,
            progress: None,
        })
        .unwrap();
    let state = machine.state();
    assert_eq!(state.slot.status, V2SlotStatus::Loading);
    assert_eq!(state.slot.model_id, None);
    let download = state
        .operations
        .iter()
        .find(|op| op.operation_id == download.operation_id)
        .unwrap();
    assert_eq!(download.slot_id, None);
    assert_eq!(download.progress.as_ref().unwrap().total_bytes, None);
}

#[test]
fn every_commit_embeds_one_complete_bounded_event_or_changes_nothing() {
    let mut machine = MachineFixture::new();
    let admission = machine.admit(download("model-a")).unwrap();
    let state = machine.state();
    let event = state.events.last().unwrap();
    assert_eq!(event.operation_id, Some(admission.operation_id));
    assert!(event.operation.is_some());
    assert!(serde_json::to_vec(event).unwrap().len() <= 16 * 1024);
}

#[test]
fn cancellation_edges_are_closed_and_atomic() {
    let mut machine = MachineFixture::new();
    let queued = machine.admit(download("model-a")).unwrap();
    machine
        .observe(Transition::Cancelled {
            operation_id: queued.operation_id,
        })
        .unwrap();
    let running = machine.admit(download("model-b")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: running.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    machine
        .observe(Transition::Cancelling {
            operation_id: running.operation_id,
        })
        .unwrap();
    assert_eq!(
        machine
            .observe(Transition::Succeeded {
                operation_id: running.operation_id,
                observed_model_id: None
            })
            .unwrap_err(),
        TransitionError::Contradiction
    );
    machine
        .observe(Transition::Failed {
            operation_id: running.operation_id,
            error: V2OperationError {
                code: V2OperationErrorCode::DownloadFailed,
                message: "cancel failed".into(),
            },
        })
        .unwrap();
}

#[test]
fn durable_progress_owns_monotonicity_and_commit_thresholds() {
    let mut machine = MachineFixture::new();
    let admission = machine.admit(download("model-a")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admission.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    assert!(machine
        .observe(Transition::Progress {
            operation_id: admission.operation_id,
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(100),
                total_bytes: None
            }
        })
        .unwrap()
        .is_noop());
    assert!(machine
        .observe(Transition::Progress {
            operation_id: admission.operation_id,
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(1024 * 1024),
                total_bytes: None
            }
        })
        .unwrap()
        .committed());
    machine.advance(500);
    assert!(machine
        .observe(Transition::Progress {
            operation_id: admission.operation_id,
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(1024 * 1024 + 1),
                total_bytes: None
            }
        })
        .unwrap()
        .committed());
    assert!(machine
        .observe(Transition::Progress {
            operation_id: admission.operation_id,
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(1024 * 1024 + 1),
                total_bytes: Some(DecimalU64::new(2 * 1024 * 1024))
            }
        })
        .unwrap()
        .committed());
    assert_eq!(
        machine
            .observe(Transition::Progress {
                operation_id: admission.operation_id,
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(1),
                    total_bytes: None
                }
            })
            .unwrap_err(),
        TransitionError::Contradiction
    );
}

#[test]
fn recovery_slot_error_round_trips_and_non_recovery_error_is_rejected() {
    let mut machine = MachineFixture::new();
    machine
        .repository
        .as_mut()
        .unwrap()
        .transaction(|tx| {
            tx.execute(
                "UPDATE slot_state SET status='recovery',model_id=NULL,operation_id=NULL,error_code='lifecycle_recovery_required',error_message='restart left lifecycle truth uncertain' WHERE singleton=1",
                [],
            )?;
            tx.execute(
                "UPDATE slot_intent SET desired_kind='unknown',desired_model_id=NULL,operation_id=NULL,reconciliation_state='recovery_required',reason_code='child_evidence_uncertain' WHERE singleton=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    machine.validate_all();
    let slot = machine.state().slot;
    assert_eq!(slot.status, V2SlotStatus::Recovery);
    assert_eq!(
        slot.error.unwrap().code,
        loxa_protocol::v2::V2SlotErrorCode::LifecycleRecoveryRequired
    );
    let mutation = machine.repository.as_mut().unwrap().transaction(|tx| {
        tx.execute(
            "UPDATE slot_state SET status='unloaded' WHERE singleton=1",
            [],
        )?;
        Ok(())
    });
    assert!(mutation.is_err());
}

#[test]
fn illegal_slot_admission_rejects_before_ids_or_rows_change() {
    let mut machine = MachineFixture::new();
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine.admit(AdmissionRequest::Unload).unwrap_err(),
        TransitionError::Contradiction
    );
    assert_eq!(machine.state(), before);
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
}

#[test]
fn started_download_progress_cannot_regress_or_replace_known_total() {
    let mut machine = MachineFixture::new();
    let admitted = machine
        .admit(AdmissionRequest::Download {
            model_id: "model-a".into(),
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(10),
                total_bytes: Some(DecimalU64::new(100)),
            },
        })
        .unwrap();
    let before = machine.state();
    let calls = machine.ids.event_calls;
    assert_eq!(
        machine
            .observe(Transition::Started {
                operation_id: admitted.operation_id,
                progress: Some(V2OperationProgress {
                    completed_bytes: DecimalU64::new(9),
                    total_bytes: Some(DecimalU64::new(100)),
                }),
            })
            .unwrap_err(),
        TransitionError::Contradiction
    );
    assert_eq!(machine.state(), before);
    assert_eq!(machine.ids.event_calls, calls);
}

#[test]
fn v1_operation_alias_is_prefixed_and_instance_scoped() {
    let mut machine = MachineFixture::new();
    assert_eq!(
        machine.admit(download("model-a")).unwrap().v1_operation_id,
        "op-1"
    );
    assert_eq!(
        machine.admit(download("model-b")).unwrap().v1_operation_id,
        "op-2"
    );
}

#[test]
fn committed_v1_projection_is_current_instance_only_and_survives_reopen() {
    let mut machine = MachineFixture::new();
    let old = machine.admit(download("old-instance")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: old.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();

    let replacement = NodeInstanceId::from_str("46666666-6666-4666-8666-666666666666").unwrap();
    machine
        .repository
        .as_mut()
        .unwrap()
        .transaction(|tx| {
            tx.execute(
                "UPDATE node_state SET node_instance_id=?1 WHERE singleton=1",
                [replacement.to_string()],
            )?;
            Ok(())
        })
        .unwrap();
    let current = machine
        .repository
        .as_mut()
        .unwrap()
        .admit(
            replacement,
            download("current-instance"),
            machine.now,
            &mut machine.ids,
        )
        .unwrap();
    assert_eq!(current.v1_operation_id, "op-1");

    let before = machine.state().current_instance_v1;
    assert_eq!(before.cursor, 1);
    assert_eq!(before.operations.len(), 1);
    assert_eq!(
        before.operations[0].operation.operation_id,
        current.operation_id
    );
    assert_eq!(before.operations[0].v1_operation_id, "op-1");
    assert_eq!(before.events.len(), 1);
    assert_eq!(before.events[0].sequence, 1);
    assert_eq!(before.events[0].v1_operation_id, "op-1");
    assert_eq!(
        before.events[0].operation.operation_id,
        current.operation_id
    );
    assert!(!before.cursor_gap(0));

    machine.reopen();
    assert_eq!(machine.state().current_instance_v1, before);
}

#[test]
fn committed_v1_projection_retains_latest_128_ordered_events_with_gap_metadata() {
    let mut machine = MachineFixture::new();
    let admission = machine.admit(download("retained-v1-events")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admission.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    for step in 1..=130_u64 {
        machine.advance(501);
        machine
            .observe(Transition::Progress {
                operation_id: admission.operation_id,
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(step * 1_048_576),
                    total_bytes: None,
                },
            })
            .unwrap();
    }

    let before = machine.state().current_instance_v1;
    assert_eq!(before.cursor, 132);
    assert_eq!(before.events.len(), 128);
    assert_eq!(before.events.first().unwrap().sequence, 5);
    assert_eq!(before.events.last().unwrap().sequence, 132);
    assert!(before
        .events
        .windows(2)
        .all(|pair| pair[0].sequence + 1 == pair[1].sequence));
    assert!(before.cursor_gap(0));
    assert!(!before.cursor_gap(4));
    assert!(!before.cursor_gap(132));

    machine.reopen();
    assert_eq!(machine.state().current_instance_v1, before);
}

#[test]
fn committed_state_decodes_all_five_capabilities_and_unpublished_is_none() {
    let root = TestRoot::new("unpublished-state");
    let path = root.path().join("control-state.sqlite3");
    let repository = ControlRepository::open_or_create(
        &path,
        NodeId::from_str(NODE_ID).unwrap(),
        &mut InitialIds,
    )
    .unwrap();
    assert!(repository.committed_state().unwrap().node.is_none());
    repository.close().unwrap();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.owner.lock", path.display()));
    let _ = std::fs::remove_file(format!("{}.migration.bak", path.display()));
    let _ = std::fs::remove_file(format!("{}.migration.bak.owner.lock", path.display()));

    let machine = MachineFixture::new();
    let node = machine.state().node.unwrap();
    assert!(node.capabilities.model_download);
    assert!(node.capabilities.slot_load);
    assert!(node.capabilities.slot_unload);
    assert!(node.capabilities.operation_cancel);
    assert!(node.capabilities.operation_stream);
}

#[test]
fn active_limit_is_exactly_128_and_rejects_before_ids() {
    let mut machine = MachineFixture::new();
    for index in 0..128 {
        machine.admit(download(&format!("model-{index}"))).unwrap();
    }
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine.admit(download("model-over-limit")).unwrap_err(),
        TransitionError::ActiveLimit
    );
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn terminal_retention_keeps_newest_128_and_full_events_survive_pruning() {
    let mut machine = MachineFixture::new();
    let mut admitted = Vec::new();
    for index in 0..129 {
        let operation = machine
            .admit(download(&format!("terminal-{index}")))
            .unwrap();
        machine
            .observe(Transition::Cancelled {
                operation_id: operation.operation_id,
            })
            .unwrap();
        admitted.push(operation.operation_id);
    }
    let state = machine.state();
    assert_eq!(state.operations.len(), 128);
    assert!(!state
        .operations
        .iter()
        .any(|op| op.operation_id == admitted[0]));
    assert!(state
        .operations
        .iter()
        .any(|op| op.operation_id == admitted[128]));
    assert!(state.events.iter().all(|event| event.validate().is_ok()));
}

#[test]
fn terminal_retention_equal_revision_tie_breaks_by_operation_id_and_keeps_event() {
    let mut machine = MachineFixture::new();
    let lowest = OperationId::from_str("70000000-0000-4000-8000-000000000001").unwrap();
    machine.repository.as_mut().unwrap().transaction(|tx| {
        for index in 1..=129_i64 {
            let operation_id = format!("70000000-0000-4000-8000-{index:012x}");
            tx.execute(
                "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,v1_ordinal,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES(?1,?2,?3,?4,'download','succeeded',?5,'1','2','1','2')",
                rusqlite::params![operation_id, SLOT_ID, INSTANCE, index, format!("tie-{index}")],
            )
            .expect("insert equal-revision terminal seed");
        }
        let operation = loxa_protocol::v2::V2Operation {
            operation_id: lowest,
            node_id: NodeId::from_str(NODE_ID).unwrap(),
            kind: loxa_protocol::v2::V2OperationKind::Download,
            status: V2OperationStatus::Succeeded,
            slot_id: None,
            model_id: Some("tie-1".into()),
            progress: None,
            error: None,
            created_revision: DecimalU64::new(1),
            updated_revision: DecimalU64::new(2),
            created_at_unix_ms: DecimalU64::new(1),
            updated_at_unix_ms: DecimalU64::new(2),
        };
        let event = super::super::operation_event(
            super::super::EventPosition {
                event_id: EventId::from_str("77777777-7777-4777-8777-777777777777").unwrap(),
                epoch: StreamEpoch::from_str(EPOCH).unwrap(),
                sequence: 2,
                revision: 2,
            },
            2,
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            &operation,
            None,
        )
        .expect("build complete retained event");
        let payload = serde_json::to_string(&event)
            .map_err(|_| crate::control_state::repository::RepositoryError::corrupt_for_state_machine())?;
        tx.execute(
            "UPDATE control_meta SET revision='2',cursor='2' WHERE singleton=1",
            [],
        )
        .expect("advance cursor for retained event");
        tx.execute(
            "INSERT INTO events(event_id,stream_epoch,sequence,revision,node_instance_id,v1_sequence,event_kind,payload_json) VALUES(?1,?2,'2','2',?3,1,'operation_changed',?4)",
            rusqlite::params![event.event_id.to_string(), EPOCH, INSTANCE, payload],
        )
        .expect("insert complete retained event");
        Ok(())
    }).unwrap();

    machine.admit(download("trigger-tie-prune")).unwrap();
    let state = machine.state();
    assert_eq!(
        state
            .operations
            .iter()
            .filter(|operation| operation.status == V2OperationStatus::Succeeded)
            .count(),
        128
    );
    assert!(!state
        .operations
        .iter()
        .any(|operation| operation.operation_id == lowest));
    assert!(state.events.iter().any(|event| {
        event
            .operation
            .as_ref()
            .is_some_and(|operation| operation.operation_id == lowest)
    }));
    machine.validate_all();
}

#[test]
fn event_retention_keeps_newest_1024_by_numeric_sequence() {
    let mut machine = MachineFixture::new();
    let admitted = machine.admit(download("event-retention")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admitted.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    for index in 1..=1025_u64 {
        machine
            .observe(Transition::Progress {
                operation_id: admitted.operation_id,
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(index * 1024 * 1024),
                    total_bytes: None,
                },
            })
            .unwrap();
    }
    let state = machine.state();
    assert_eq!(state.events.len(), 1024);
    assert_eq!(state.events.last().unwrap().sequence, state.cursor);
    assert!(state
        .events
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence));
}

#[test]
fn terminal_same_model_download_can_be_retried_with_next_v1_alias() {
    let mut machine = MachineFixture::new();
    let first = machine.admit(download("retry-model")).unwrap();
    machine
        .observe(Transition::Cancelled {
            operation_id: first.operation_id,
        })
        .unwrap();
    assert_eq!(
        machine
            .admit(download("retry-model"))
            .unwrap()
            .v1_operation_id,
        "op-2"
    );
}

#[test]
fn initial_and_mutation_rows_decode_as_full_events() {
    let mut machine = MachineFixture::new();
    let initial = machine.state().events.remove(0);
    assert_eq!(initial.entity, loxa_protocol::v2::V2EventEntity::Slot);
    assert!(initial.slot.is_some());
    assert!(initial.validate().is_ok());
    machine.admit(download("full-event")).unwrap();
    machine.validate_all();
    assert!(machine
        .state()
        .events
        .iter()
        .all(|event| event.validate().is_ok()));
}

#[test]
fn injected_transaction_failure_preserves_complete_logical_snapshot() {
    let mut machine = MachineFixture::new();
    let before = machine.state();
    let failure: Result<(), _> = machine.repository.as_mut().unwrap().transaction(|tx| {
        tx.execute(
            "UPDATE control_meta SET revision='99',cursor='99' WHERE singleton=1",
            [],
        )?;
        Err(
            crate::control_state::repository::RepositoryError::tagged_for_state_machine(
                "injected-task3a-rollback",
            ),
        )
    });
    assert!(failure.is_err());
    assert_eq!(machine.state(), before);
}

#[test]
fn dangling_or_mismatched_slot_operation_fails_closed_on_validation() {
    let mut machine = MachineFixture::new();
    machine
        .repository
        .as_mut()
        .unwrap()
        .transaction(|tx| {
            tx.execute(
                "UPDATE slot_state SET status='loading',operation_id='99999999-9999-4999-8999-999999999999' WHERE singleton=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine
            .admit(download("must-not-mutate-corrupt-state"))
            .unwrap_err(),
        TransitionError::CorruptState
    );
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
    assert_eq!(machine.state(), before);
    assert!(machine.repository.as_ref().unwrap().validate_all().is_err());
}

#[test]
fn operation_event_row_requires_instance_v1_and_exact_payload_revision_time() {
    for corruption in [
        "UPDATE events SET node_instance_id=NULL WHERE event_kind='operation_changed'",
        "UPDATE events SET v1_sequence=NULL WHERE event_kind='operation_changed'",
        "UPDATE events SET revision='1' WHERE event_kind='operation_changed'",
    ] {
        let mut machine = MachineFixture::new();
        machine.admit(download("event-corruption")).unwrap();
        machine
            .repository
            .as_mut()
            .unwrap()
            .transaction(|tx| {
                tx.execute(corruption, [])?;
                Ok(())
            })
            .unwrap();
        assert!(machine.repository.as_ref().unwrap().validate_all().is_err());
    }
}

#[test]
fn closed_transition_rejections_and_noops_preserve_rows_and_ids() {
    let mut queued = MachineFixture::new();
    let queued_id = queued
        .admit(download("queued-matrix"))
        .unwrap()
        .operation_id;
    assert_rejected_without_mutation(
        &mut queued,
        Transition::Progress {
            operation_id: queued_id,
            progress: zero_progress(),
        },
        TransitionError::IllegalTransition,
    );
    assert_rejected_without_mutation(
        &mut queued,
        Transition::Succeeded {
            operation_id: queued_id,
            observed_model_id: None,
        },
        TransitionError::IllegalTransition,
    );

    let mut running = MachineFixture::new();
    let running_id = running
        .admit(download("running-matrix"))
        .unwrap()
        .operation_id;
    running
        .observe(Transition::Started {
            operation_id: running_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    assert_rejected_without_mutation(
        &mut running,
        Transition::Started {
            operation_id: running_id,
            progress: Some(V2OperationProgress {
                completed_bytes: DecimalU64::new(1),
                total_bytes: None,
            }),
        },
        TransitionError::IllegalTransition,
    );

    let mut cancelling = MachineFixture::new();
    let cancelling_id = cancelling
        .admit(download("cancelling-matrix"))
        .unwrap()
        .operation_id;
    cancelling
        .observe(Transition::Cancelling {
            operation_id: cancelling_id,
        })
        .unwrap();
    let before = cancelling.state();
    let calls = cancelling.ids.event_calls;
    assert!(cancelling
        .observe(Transition::Cancelling {
            operation_id: cancelling_id,
        })
        .unwrap()
        .is_noop());
    assert_eq!(cancelling.ids.event_calls, calls);
    assert_eq!(cancelling.state(), before);
    assert_rejected_without_mutation(
        &mut cancelling,
        Transition::Progress {
            operation_id: cancelling_id,
            progress: zero_progress(),
        },
        TransitionError::IllegalTransition,
    );
    assert_rejected_without_mutation(
        &mut cancelling,
        Transition::Succeeded {
            operation_id: cancelling_id,
            observed_model_id: None,
        },
        TransitionError::Contradiction,
    );
}

#[derive(Clone, Copy, Debug)]
enum MatrixStatus {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
enum MatrixAction {
    Started,
    Progress,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
enum MatrixExpected {
    Commit,
    Noop,
    Reject(TransitionError),
}

fn matrix_error() -> V2OperationError {
    V2OperationError {
        code: V2OperationErrorCode::DownloadFailed,
        message: "matrix failure".into(),
    }
}

fn matrix_fixture(status: MatrixStatus) -> (MachineFixture, OperationId) {
    let mut machine = MachineFixture::new();
    let operation_id = machine
        .admit(download("matrix-model"))
        .unwrap()
        .operation_id;
    if matches!(
        status,
        MatrixStatus::Running | MatrixStatus::Cancelling | MatrixStatus::Succeeded
    ) {
        machine
            .observe(Transition::Started {
                operation_id,
                progress: Some(zero_progress()),
            })
            .unwrap();
    }
    match status {
        MatrixStatus::Queued | MatrixStatus::Running => {}
        MatrixStatus::Cancelling => {
            machine
                .observe(Transition::Cancelling { operation_id })
                .unwrap();
        }
        MatrixStatus::Succeeded => {
            machine
                .observe(Transition::Succeeded {
                    operation_id,
                    observed_model_id: None,
                })
                .unwrap();
        }
        MatrixStatus::Failed => {
            machine
                .observe(Transition::Failed {
                    operation_id,
                    error: matrix_error(),
                })
                .unwrap();
        }
        MatrixStatus::Cancelled => {
            machine
                .observe(Transition::Cancelled { operation_id })
                .unwrap();
        }
    }
    (machine, operation_id)
}

fn matrix_transition(action: MatrixAction, operation_id: OperationId) -> Transition {
    match action {
        MatrixAction::Started => Transition::Started {
            operation_id,
            progress: Some(zero_progress()),
        },
        MatrixAction::Progress => Transition::Progress {
            operation_id,
            progress: zero_progress(),
        },
        MatrixAction::Cancelling => Transition::Cancelling { operation_id },
        MatrixAction::Succeeded => Transition::Succeeded {
            operation_id,
            observed_model_id: None,
        },
        MatrixAction::Failed => Transition::Failed {
            operation_id,
            error: matrix_error(),
        },
        MatrixAction::Cancelled => Transition::Cancelled { operation_id },
    }
}

fn matrix_expected(status: MatrixStatus, action: MatrixAction) -> MatrixExpected {
    use MatrixAction as A;
    use MatrixExpected as E;
    use MatrixStatus as S;
    match (status, action) {
        (S::Queued, A::Started | A::Cancelling | A::Failed | A::Cancelled)
        | (S::Running, A::Cancelling | A::Succeeded | A::Failed | A::Cancelled)
        | (S::Cancelling, A::Failed | A::Cancelled) => E::Commit,
        (S::Running, A::Started | A::Progress)
        | (S::Cancelling, A::Cancelling)
        | (S::Succeeded, A::Succeeded)
        | (S::Failed, A::Failed)
        | (S::Cancelled, A::Cancelling | A::Cancelled) => E::Noop,
        (S::Queued, A::Progress | A::Succeeded) | (S::Cancelling, A::Started | A::Progress) => {
            E::Reject(TransitionError::IllegalTransition)
        }
        _ => E::Reject(TransitionError::Contradiction),
    }
}

#[test]
fn full_closed_status_by_transition_matrix_has_exact_commit_noop_reject_semantics() {
    let statuses = [
        MatrixStatus::Queued,
        MatrixStatus::Running,
        MatrixStatus::Cancelling,
        MatrixStatus::Succeeded,
        MatrixStatus::Failed,
        MatrixStatus::Cancelled,
    ];
    let actions = [
        MatrixAction::Started,
        MatrixAction::Progress,
        MatrixAction::Cancelling,
        MatrixAction::Succeeded,
        MatrixAction::Failed,
        MatrixAction::Cancelled,
    ];
    for status in statuses {
        for action in actions {
            let (mut machine, operation_id) = matrix_fixture(status);
            let before = machine.state();
            let calls = (machine.ids.operation_calls, machine.ids.event_calls);
            let result = machine.observe(matrix_transition(action, operation_id));
            match matrix_expected(status, action) {
                MatrixExpected::Commit => {
                    assert!(result.unwrap().committed(), "{status:?} x {action:?}");
                    assert_ne!(machine.state(), before, "{status:?} x {action:?}");
                }
                MatrixExpected::Noop => {
                    assert!(result.unwrap().is_noop(), "{status:?} x {action:?}");
                    assert_eq!(machine.state(), before, "{status:?} x {action:?}");
                    assert_eq!(
                        (machine.ids.operation_calls, machine.ids.event_calls),
                        calls,
                        "{status:?} x {action:?}"
                    );
                }
                MatrixExpected::Reject(expected) => {
                    assert_eq!(result.unwrap_err(), expected, "{status:?} x {action:?}");
                    assert_eq!(machine.state(), before, "{status:?} x {action:?}");
                    assert_eq!(
                        (machine.ids.operation_calls, machine.ids.event_calls),
                        calls,
                        "{status:?} x {action:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn all_32_capability_combinations_decode_exactly() {
    let mut machine = MachineFixture::new();
    for mask in 0..32_i64 {
        machine
            .repository
            .as_mut()
            .unwrap()
            .transaction(|tx| {
                tx.execute(
                    "UPDATE node_state SET model_download=?1,slot_load=?2,slot_unload=?3,operation_cancel=?4,operation_stream=?5 WHERE singleton=1",
                    rusqlite::params![mask & 1, (mask >> 1) & 1, (mask >> 2) & 1, (mask >> 3) & 1, (mask >> 4) & 1],
                )?;
                Ok(())
            })
            .unwrap();
        machine.reopen();
        let capabilities = machine.state().node.unwrap().capabilities;
        assert_eq!(capabilities.model_download, mask & 1 != 0);
        assert_eq!(capabilities.slot_load, mask & 2 != 0);
        assert_eq!(capabilities.slot_unload, mask & 4 != 0);
        assert_eq!(capabilities.operation_cancel, mask & 8 != 0);
        assert_eq!(capabilities.operation_stream, mask & 16 != 0);
    }
}

#[test]
fn oversized_event_rolls_back_all_durable_rows() {
    let mut machine = MachineFixture::new();
    let empty = machine.state();
    let empty_calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine
            .admit_with_event_limit(download("oversized-admission"), 1)
            .unwrap_err(),
        TransitionError::EventTooLarge
    );
    assert_eq!(machine.state(), empty);
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        empty_calls
    );
    let admitted = machine.admit(download("oversized-event")).unwrap();
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine
            .observe_with_event_limit(
                Transition::Started {
                    operation_id: admitted.operation_id,
                    progress: Some(zero_progress()),
                },
                1,
            )
            .unwrap_err(),
        TransitionError::EventTooLarge
    );
    assert_eq!(machine.state(), before);
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
}

#[test]
fn corrupt_reconstructed_domain_rejects_committed_state_and_noop_before_ids() {
    let mut machine = MachineFixture::new();
    let admitted = machine.admit(download("decode-corruption")).unwrap();
    machine
        .observe(Transition::Started {
            operation_id: admitted.operation_id,
            progress: Some(zero_progress()),
        })
        .unwrap();
    machine
        .observe(Transition::Succeeded {
            operation_id: admitted.operation_id,
            observed_model_id: None,
        })
        .unwrap();
    machine
        .repository
        .as_mut()
        .unwrap()
        .transaction(|tx| {
            tx.execute(
                "UPDATE operations SET model_id=NULL WHERE operation_id=?1",
                [admitted.operation_id.to_string()],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(machine
        .repository
        .as_ref()
        .unwrap()
        .committed_state()
        .is_err());
    let calls = machine.ids.event_calls;
    assert!(machine
        .observe(Transition::Succeeded {
            operation_id: admitted.operation_id,
            observed_model_id: None,
        })
        .is_err());
    assert_eq!(machine.ids.event_calls, calls);
}

#[test]
fn node_and_slot_events_require_null_v1_sequence() {
    let mut machine = MachineFixture::new();
    machine
        .repository
        .as_mut()
        .unwrap()
        .transaction(|tx| {
            tx.execute(
                "UPDATE events SET v1_sequence=1 WHERE event_kind='initialized'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(machine.repository.as_ref().unwrap().validate_all().is_err());
}

#[test]
fn v1_ordinal_overflow_rejects_before_id_allocation() {
    let mut machine = MachineFixture::new();
    machine.repository.as_mut().unwrap().transaction(|tx| {
        tx.execute(
            "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,v1_ordinal,kind,status,model_id,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES('99999999-9999-4999-8999-999999999999',?1,?2,9223372036854775807,'download','succeeded','ordinal-seed','1','1','1','1')",
            rusqlite::params![SLOT_ID, INSTANCE],
        )?;
        Ok(())
    }).unwrap();
    let before = machine.state();
    let calls = (machine.ids.operation_calls, machine.ids.event_calls);
    assert_eq!(
        machine.admit(download("ordinal-overflow")).unwrap_err(),
        TransitionError::Overflow
    );
    assert_eq!(
        (machine.ids.operation_calls, machine.ids.event_calls),
        calls
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn v1_event_sequence_overflow_rolls_back_before_event_id_allocation() {
    let mut machine = MachineFixture::new();
    let admitted = machine.admit(download("sequence-overflow")).unwrap();
    machine.repository.as_mut().unwrap().transaction(|tx| {
        tx.execute(
            "UPDATE events SET v1_sequence=9223372036854775807 WHERE event_kind='operation_changed'",
            [],
        )?;
        Ok(())
    }).unwrap();
    let before = machine.state();
    let calls = machine.ids.event_calls;
    assert_eq!(
        machine
            .observe(Transition::Started {
                operation_id: admitted.operation_id,
                progress: Some(zero_progress()),
            })
            .unwrap_err(),
        TransitionError::Overflow
    );
    assert_eq!(machine.ids.event_calls, calls);
    assert_eq!(machine.state(), before);
}
