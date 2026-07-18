use super::recovery::{
    decide, ReconciledControlState, RecoveryDecision, RecoveryEvidence, SlotRecoveryError,
};
use super::repository::{
    ControlRepository, DesiredKind, IntentReason, ReconciliationState, RepositoryError,
    RepositoryErrorClass,
};
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, StreamEpoch, V2ControlEvent, V2EventEntity, V2Node,
    V2NodeCapabilities, V2NodeStatus, V2Operation, V2OperationError, V2OperationErrorCode,
    V2OperationKind, V2OperationProgress, V2OperationStatus, V2PublicError, V2Slot,
    V2SlotErrorCode, V2SlotStatus, V2_SCHEMA_VERSION,
};
use loxa_protocol::NodeInstanceId;
use rusqlite::{OptionalExtension, Row, Transaction as SqlTransaction};
use std::str::FromStr;

const MAX_ACTIVE_OPERATIONS: i64 = 128;
const MAX_TERMINAL_OPERATIONS: i64 = 128;
const MAX_EVENTS: i64 = 1024;
const MAX_EVENT_BYTES: usize = 16 * 1024;
const PROGRESS_BYTES_THRESHOLD: u64 = 1024 * 1024;
const PROGRESS_TIME_THRESHOLD_MS: u64 = 500;

fn placeholder_operation_id() -> OperationId {
    OperationId::from_str("eeeeeeee-eeee-4eee-aeee-eeeeeeeeeeee")
        .expect("fixed canonical placeholder operation ID")
}

fn placeholder_event_id() -> EventId {
    EventId::from_str("ffffffff-ffff-4fff-bfff-ffffffffffff")
        .expect("fixed canonical placeholder event ID")
}

pub(crate) trait MutationIds {
    fn new_operation_id(&mut self) -> OperationId;
    fn new_event_id(&mut self) -> EventId;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionRequest {
    Download {
        model_id: String,
        progress: V2OperationProgress,
    },
    Load {
        model_id: String,
    },
    Unload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Transition {
    Started {
        operation_id: OperationId,
        progress: Option<V2OperationProgress>,
    },
    Progress {
        operation_id: OperationId,
        progress: V2OperationProgress,
    },
    Cancelling {
        operation_id: OperationId,
    },
    Succeeded {
        operation_id: OperationId,
        observed_model_id: Option<String>,
    },
    Failed {
        operation_id: OperationId,
        error: V2OperationError,
    },
    Cancelled {
        operation_id: OperationId,
    },
}

impl Transition {
    fn operation_id(&self) -> OperationId {
        match self {
            Self::Started { operation_id, .. }
            | Self::Progress { operation_id, .. }
            | Self::Cancelling { operation_id }
            | Self::Succeeded { operation_id, .. }
            | Self::Failed { operation_id, .. }
            | Self::Cancelled { operation_id } => *operation_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitReceipt {
    pub(crate) epoch: StreamEpoch,
    pub(crate) revision: DecimalU64,
    pub(crate) cursor: DecimalU64,
    pub(crate) event_id: Option<EventId>,
}

impl CommitReceipt {
    pub(crate) fn committed(&self) -> bool {
        self.event_id.is_some()
    }

    pub(crate) fn is_noop(&self) -> bool {
        self.event_id.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedAdmission {
    pub(crate) epoch: StreamEpoch,
    pub(crate) operation_id: OperationId,
    pub(crate) revision: DecimalU64,
    pub(crate) v1_operation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedState {
    pub(crate) revision: DecimalU64,
    pub(crate) cursor: DecimalU64,
    pub(crate) last_committed_at_unix_ms: DecimalU64,
    pub(crate) node: Option<V2Node>,
    pub(crate) slot: V2Slot,
    pub(crate) operations: Vec<V2Operation>,
    pub(crate) events: Vec<V2ControlEvent>,
    pub(crate) current_instance_v1: CurrentInstanceV1State,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CurrentInstanceV1State {
    pub(crate) cursor: u64,
    pub(crate) operations: Vec<CurrentInstanceV1Operation>,
    pub(crate) events: Vec<CurrentInstanceV1Event>,
}

impl CurrentInstanceV1State {
    pub(crate) fn cursor_gap(&self, requested: u64) -> bool {
        self.events
            .first()
            .is_some_and(|event| requested.saturating_add(1) < event.sequence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CurrentInstanceV1Operation {
    pub(crate) v1_operation_id: String,
    pub(crate) operation: V2Operation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CurrentInstanceV1Event {
    pub(crate) sequence: u64,
    pub(crate) v1_operation_id: String,
    pub(crate) operation: V2Operation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstancePublication {
    pub(crate) node_instance_id: NodeInstanceId,
    pub(crate) control_endpoint: String,
    pub(crate) capabilities: V2NodeCapabilities,
    pub(crate) now_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransitionError {
    ActiveLimit,
    LifecycleConflict,
    SameModelConflict,
    OperationNotFound,
    IllegalTransition,
    Contradiction,
    EventTooLarge,
    Overflow,
    CorruptState,
    Repository(RepositoryErrorClass),
}

impl From<RepositoryError> for TransitionError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value.class())
    }
}

impl ControlRepository {
    pub(crate) fn admit(
        &mut self,
        node_instance_id: NodeInstanceId,
        request: AdmissionRequest,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<CommittedAdmission, TransitionError> {
        self.admit_with_event_limit(node_instance_id, request, now_unix_ms, ids, MAX_EVENT_BYTES)
    }

    fn admit_with_event_limit(
        &mut self,
        node_instance_id: NodeInstanceId,
        request: AdmissionRequest,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
        event_limit: usize,
    ) -> Result<CommittedAdmission, TransitionError> {
        let node_id = self.node_id();
        let slot_id = self.slot_id();
        let epoch = self.stream_epoch();
        self.transaction(|tx| {
            require_published_instance(tx, node_instance_id)?;
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let effective_now = now_unix_ms.max(last_committed);
            let (kind, model_id, progress) = match &request {
                AdmissionRequest::Download { model_id, progress } => {
                    progress.validate().map_err(|_| transition_repository_error())?;
                    (V2OperationKind::Download, Some(model_id.clone()), Some(progress.clone()))
                }
                AdmissionRequest::Load { model_id } => {
                    (V2OperationKind::Load, Some(model_id.clone()), None)
                }
                AdmissionRequest::Unload => (V2OperationKind::Unload, None, None),
            };
            if model_id.as_deref().is_some_and(|value| !valid_model_id(value)) {
                return Err(transition_repository_error());
            }
            let active: i64 = tx.query_row(
                "SELECT COUNT(*) FROM operations WHERE status IN ('queued','running','cancelling')",
                [],
                |row| row.get(0),
            )?;
            if active >= MAX_ACTIVE_OPERATIONS {
                return Err(tagged_error(TransitionError::ActiveLimit));
            }
            if kind == V2OperationKind::Download {
                let same: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM operations WHERE kind='download' AND model_id=?1 AND status IN ('queued','running','cancelling'))",
                    [model_id.as_deref()],
                    |row| row.get(0),
                )?;
                if same {
                    return Err(tagged_error(TransitionError::SameModelConflict));
                }
            } else {
                let lifecycle: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM operations WHERE kind IN ('load','unload') AND status IN ('queued','running','cancelling'))",
                    [],
                    |row| row.get(0),
                )?;
                if lifecycle {
                    return Err(tagged_error(TransitionError::LifecycleConflict));
                }
            }
            let slot = read_slot(tx, node_id, slot_id)?;
            validate_slot_operation_correlation(tx, &slot)?;
            let legal_slot = match kind {
                V2OperationKind::Download => slot.status != V2SlotStatus::Recovery,
                V2OperationKind::Load => {
                    matches!(slot.status, V2SlotStatus::Unloaded | V2SlotStatus::Ready)
                }
                V2OperationKind::Unload => {
                    slot.status == V2SlotStatus::Ready && slot.model_id.is_some()
                }
            };
            if !legal_slot {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            let max_ordinal: Option<i64> = tx.query_row(
                "SELECT MAX(v1_ordinal) FROM operations WHERE admitting_node_instance_id=?1",
                [node_instance_id.to_string()],
                |row| row.get(0),
            )?;
            let ordinal = max_ordinal
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(overflow_error)?;
            let v1_sequence = next_v1_sequence(tx, node_instance_id)?;
            let preflight_operation = V2Operation {
                operation_id: placeholder_operation_id(),
                node_id,
                kind,
                status: V2OperationStatus::Queued,
                slot_id: (kind != V2OperationKind::Download).then_some(slot_id),
                model_id,
                progress,
                error: None,
                created_revision: DecimalU64::new(next_revision),
                updated_revision: DecimalU64::new(next_revision),
                created_at_unix_ms: DecimalU64::new(effective_now),
                updated_at_unix_ms: DecimalU64::new(effective_now),
            };
            let mut preflight_slot = slot.clone();
            if kind != V2OperationKind::Download {
                apply_slot_start(&preflight_operation, &mut preflight_slot)?;
            }
            let preflight_event = operation_event(
                EventPosition {
                    event_id: placeholder_event_id(),
                    epoch,
                    sequence: next_cursor,
                    revision: next_revision,
                },
                effective_now,
                node_instance_id,
                &preflight_operation,
                (kind != V2OperationKind::Download).then_some(preflight_slot),
            )?;
            serialize_event_with_limit(&preflight_event, event_limit)?;
            let operation_id = ids.new_operation_id();
            let event_id = ids.new_event_id();
            let operation = V2Operation {
                operation_id,
                ..preflight_operation
            };
            let mut admitted_slot = slot;
            if kind != V2OperationKind::Download {
                apply_slot_start(&operation, &mut admitted_slot)?;
            }
            let event = operation_event(
                EventPosition {
                    event_id,
                    epoch,
                    sequence: next_cursor,
                    revision: next_revision,
                },
                effective_now,
                node_instance_id,
                &operation,
                (kind != V2OperationKind::Download).then_some(admitted_slot.clone()),
            )?;
            let payload = serialize_event_with_limit(&event, event_limit)?;
            tx.execute(
                "INSERT INTO operations(operation_id,slot_id,admitting_node_instance_id,v1_ordinal,kind,status,model_id,progress_current,progress_total,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms) VALUES(?1,?2,?3,?4,?5,'queued',?6,?7,?8,?9,?9,?10,?10)",
                rusqlite::params![operation_id.to_string(), slot_id.to_string(), node_instance_id.to_string(), ordinal, kind_text(kind), operation.model_id, operation.progress.as_ref().map(|p| p.completed_bytes.to_string()), operation.progress.as_ref().and_then(|p| p.total_bytes).map(|v| v.to_string()), next_revision.to_string(), effective_now.to_string()],
            )?;
            if kind != V2OperationKind::Download {
                write_slot(tx, &admitted_slot, next_revision, effective_now)?;
                write_intent(
                    tx,
                    if kind == V2OperationKind::Load {
                        DesiredKind::Loaded
                    } else {
                        DesiredKind::Unloaded
                    },
                    operation.model_id.as_deref(),
                    next_revision,
                    Some(operation_id),
                    ReconciliationState::Applying,
                    None,
                )?;
            }
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, Some(v1_sequence), &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommittedAdmission {
                epoch,
                operation_id,
                revision: DecimalU64::new(next_revision),
                v1_operation_id: format!("op-{ordinal}"),
            })
        })
        .map_err(map_tagged_error)
    }

    pub(crate) fn observe(
        &mut self,
        node_instance_id: NodeInstanceId,
        transition: Transition,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<CommitReceipt, TransitionError> {
        self.observe_with_event_limit(
            node_instance_id,
            transition,
            now_unix_ms,
            ids,
            MAX_EVENT_BYTES,
        )
    }

    pub(crate) fn publish_instance(
        &mut self,
        publication: InstancePublication,
        ids: &mut dyn MutationIds,
    ) -> Result<CommitReceipt, TransitionError> {
        let node_id = self.node_id();
        let epoch = self.stream_epoch();
        self.transaction(|tx| {
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            let effective_now = publication.now_unix_ms.max(last_committed);
            let node = V2Node {
                node_id,
                node_instance_id: publication.node_instance_id,
                control_endpoint: publication.control_endpoint.clone(),
                status: V2NodeStatus::Running,
                slot_capacity: 1,
                capabilities: publication.capabilities.clone(),
            };
            serde_json::to_value(&node)
                .map_err(|_| tagged_error(TransitionError::Contradiction))?;
            let event_id = ids.new_event_id();
            let event = V2ControlEvent {
                schema_version: V2_SCHEMA_VERSION,
                event_id,
                epoch,
                sequence: DecimalU64::new(next_cursor),
                revision: DecimalU64::new(next_revision),
                committed_at_unix_ms: DecimalU64::new(effective_now),
                entity: V2EventEntity::Node,
                entity_id: node_id.to_string(),
                node_id,
                node_instance_id: Some(publication.node_instance_id),
                slot_id: None,
                operation_id: None,
                node: Some(node),
                slot: None,
                operation: None,
            };
            event
                .validate()
                .map_err(|_| tagged_error(TransitionError::Contradiction))?;
            let payload = serialize_event(&event)?;
            tx.execute(
                "UPDATE node_state SET node_instance_id=?1,control_endpoint=?2,status='running',model_download=?3,slot_load=?4,slot_unload=?5,operation_cancel=?6,operation_stream=?7 WHERE singleton=1",
                rusqlite::params![
                    publication.node_instance_id.to_string(),
                    publication.control_endpoint,
                    publication.capabilities.model_download as i64,
                    publication.capabilities.slot_load as i64,
                    publication.capabilities.slot_unload as i64,
                    publication.capabilities.operation_cancel as i64,
                    publication.capabilities.operation_stream as i64,
                ],
            )?;
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, None, &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommitReceipt {
                epoch,
                revision: DecimalU64::new(next_revision),
                cursor: DecimalU64::new(next_cursor),
                event_id: Some(event_id),
            })
        })
        .map_err(map_tagged_error)
    }

    pub(crate) fn begin_stopping(
        &mut self,
        node_instance_id: NodeInstanceId,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<CommitReceipt, TransitionError> {
        let node_id = self.node_id();
        let epoch = self.stream_epoch();
        self.transaction(|tx| {
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let mut node = read_node_connection(tx, node_id)?
                .ok_or_else(|| tagged_error(TransitionError::Contradiction))?;
            if node.node_instance_id != node_instance_id {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            if node.status == V2NodeStatus::Stopping {
                return Ok(CommitReceipt {
                    epoch,
                    revision: DecimalU64::new(revision),
                    cursor: DecimalU64::new(cursor),
                    event_id: None,
                });
            }
            if node.status != V2NodeStatus::Running {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            let effective_now = now_unix_ms.max(last_committed);
            node.status = V2NodeStatus::Stopping;
            let event_id = ids.new_event_id();
            let event = V2ControlEvent {
                schema_version: V2_SCHEMA_VERSION,
                event_id,
                epoch,
                sequence: DecimalU64::new(next_cursor),
                revision: DecimalU64::new(next_revision),
                committed_at_unix_ms: DecimalU64::new(effective_now),
                entity: V2EventEntity::Node,
                entity_id: node_id.to_string(),
                node_id,
                node_instance_id: Some(node_instance_id),
                slot_id: None,
                operation_id: None,
                node: Some(node.clone()),
                slot: None,
                operation: None,
            };
            event
                .validate()
                .map_err(|_| tagged_error(TransitionError::Contradiction))?;
            let payload = serialize_event(&event)?;
            tx.execute(
                "UPDATE node_state SET status='stopping' WHERE singleton=1 AND node_instance_id=?1 AND status='running'",
                [node_instance_id.to_string()],
            )?;
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, None, &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommitReceipt {
                epoch,
                revision: DecimalU64::new(next_revision),
                cursor: DecimalU64::new(next_cursor),
                event_id: Some(event_id),
            })
        })
        .map_err(map_tagged_error)
    }

    pub(crate) fn reconcile_offline(
        &mut self,
        evidence: RecoveryEvidence,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<ReconciledControlState, TransitionError> {
        let decision = decide(evidence);
        let interrupted: Vec<(OperationId, V2OperationStatus)> = self.read_transaction(|connection| {
            let mut statement = connection.prepare(
                "SELECT operation_id,status FROM operations WHERE status IN ('queued','running','cancelling') ORDER BY length(created_revision),created_revision,operation_id",
            )?;
            let rows = statement
                .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            rows.into_iter()
                .map(|(id, status)| {
                    Ok((
                        OperationId::from_str(&id)
                            .map_err(|_| RepositoryError::corrupt_for_state_machine())?,
                        parse_operation_status(&status)?,
                    ))
                })
                .collect()
        })?;
        let mut receipts = Vec::with_capacity(interrupted.len() + 1);
        for (operation_id, status) in interrupted {
            let (code, message) = match status {
                V2OperationStatus::Queued => (
                    V2OperationErrorCode::NodeRestartedBeforeStart,
                    "node restarted before the operation started",
                ),
                V2OperationStatus::Running => (
                    V2OperationErrorCode::NodeRestarted,
                    "node restarted while the operation was running",
                ),
                V2OperationStatus::Cancelling => (
                    V2OperationErrorCode::CancellationOutcomeUnknown,
                    "node restarted before cancellation was confirmed",
                ),
                _ => return Err(TransitionError::CorruptState),
            };
            receipts.push(self.terminalize_interrupted_operation(
                operation_id,
                V2OperationError {
                    code,
                    message: message.to_owned(),
                },
                &decision,
                now_unix_ms,
                ids,
            )?);
        }

        let ready_model = match &decision {
            RecoveryDecision::Ready { authority } => Some(authority.model_id().to_owned()),
            _ => None,
        };
        if let Some(receipt) = self.reconcile_slot_if_changed(&decision, now_unix_ms, ids)? {
            receipts.push(receipt);
        }
        let ready_authority = match decision {
            RecoveryDecision::Ready { authority } => Some(authority),
            _ => None,
        };
        debug_assert_eq!(ready_model.is_some(), ready_authority.is_some());
        Ok(ReconciledControlState {
            receipts,
            ready_authority,
        })
    }

    fn terminalize_interrupted_operation(
        &mut self,
        operation_id: OperationId,
        error: V2OperationError,
        decision: &RecoveryDecision,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<CommitReceipt, TransitionError> {
        let node_id = self.node_id();
        let slot_id = self.slot_id();
        let epoch = self.stream_epoch();
        self.transaction(|tx| {
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let mut operation = read_operation(tx, node_id, slot_id, operation_id)?
                .ok_or_else(|| tagged_error(TransitionError::OperationNotFound))?;
            if !matches!(
                operation.status,
                V2OperationStatus::Queued
                    | V2OperationStatus::Running
                    | V2OperationStatus::Cancelling
            ) {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            let admitting_instance: String = tx.query_row(
                "SELECT admitting_node_instance_id FROM operations WHERE operation_id=?1",
                [operation_id.to_string()],
                |row| row.get(0),
            )?;
            let admitting_instance = NodeInstanceId::from_str(&admitting_instance)
                .map_err(|_| tagged_error(TransitionError::CorruptState))?;
            let effective_now = now_unix_ms
                .max(last_committed)
                .max(operation.updated_at_unix_ms.get());
            let mut slot = read_slot(tx, node_id, slot_id)?;
            validate_slot_operation_correlation(tx, &slot)?;
            let previous_slot = slot.clone();
            operation.status = V2OperationStatus::Failed;
            operation.error = Some(error);
            if slot.operation_id == Some(operation_id) {
                apply_recovery_decision_to_slot(&mut slot, decision)?;
            }
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            operation.updated_revision = DecimalU64::new(next_revision);
            operation.updated_at_unix_ms = DecimalU64::new(effective_now);
            let slot_changed = slot != previous_slot;
            let v1_sequence = next_v1_sequence(tx, admitting_instance)?;
            let event_id = ids.new_event_id();
            let event = operation_event(
                EventPosition {
                    event_id,
                    epoch,
                    sequence: next_cursor,
                    revision: next_revision,
                },
                effective_now,
                admitting_instance,
                &operation,
                slot_changed.then_some(slot.clone()),
            )?;
            let payload = serialize_event(&event)?;
            write_operation(tx, &operation, slot_id)?;
            if slot_changed {
                write_slot(tx, &slot, next_revision, effective_now)?;
                write_intent_for_observed_slot(tx, &slot, next_revision)?;
            }
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, Some(v1_sequence), &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommitReceipt {
                epoch,
                revision: DecimalU64::new(next_revision),
                cursor: DecimalU64::new(next_cursor),
                event_id: Some(event_id),
            })
        })
        .map_err(map_tagged_error)
    }

    fn reconcile_slot_if_changed(
        &mut self,
        decision: &RecoveryDecision,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
    ) -> Result<Option<CommitReceipt>, TransitionError> {
        let node_id = self.node_id();
        let slot_id = self.slot_id();
        let epoch = self.stream_epoch();
        let current =
            self.read_transaction(|connection| read_slot_connection(connection, node_id, slot_id))?;
        let mut target = current.clone();
        apply_recovery_decision_to_slot(&mut target, decision)?;
        if target == current {
            return Ok(None);
        }
        self.transaction(|tx| {
            let observed = read_slot(tx, node_id, slot_id)?;
            if observed != current {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            let effective_now = now_unix_ms.max(last_committed);
            let committed = target.clone();
            let event_id = ids.new_event_id();
            let event = V2ControlEvent {
                schema_version: V2_SCHEMA_VERSION,
                event_id,
                epoch,
                sequence: DecimalU64::new(next_cursor),
                revision: DecimalU64::new(next_revision),
                committed_at_unix_ms: DecimalU64::new(effective_now),
                entity: V2EventEntity::Slot,
                entity_id: slot_id.to_string(),
                node_id,
                node_instance_id: None,
                slot_id: Some(slot_id),
                operation_id: None,
                node: None,
                slot: Some(committed.clone()),
                operation: None,
            };
            event
                .validate()
                .map_err(|_| tagged_error(TransitionError::Contradiction))?;
            let payload = serialize_event(&event)?;
            write_slot(tx, &committed, next_revision, effective_now)?;
            write_intent_for_observed_slot(tx, &committed, next_revision)?;
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, None, &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommitReceipt {
                epoch,
                revision: DecimalU64::new(next_revision),
                cursor: DecimalU64::new(next_cursor),
                event_id: Some(event_id),
            })
        })
        .map(Some)
        .map_err(map_tagged_error)
    }

    fn observe_with_event_limit(
        &mut self,
        node_instance_id: NodeInstanceId,
        transition: Transition,
        now_unix_ms: u64,
        ids: &mut dyn MutationIds,
        event_limit: usize,
    ) -> Result<CommitReceipt, TransitionError> {
        let node_id = self.node_id();
        let slot_id = self.slot_id();
        let epoch = self.stream_epoch();
        self.transaction(|tx| {
            require_observable_instance(tx, node_instance_id)?;
            let (revision, cursor, last_committed) = read_meta(tx)?;
            let mut operation = read_operation(tx, node_id, slot_id, transition.operation_id())?
                .ok_or_else(|| tagged_error(TransitionError::OperationNotFound))?;
            let effective_now = now_unix_ms
                .max(last_committed)
                .max(operation.updated_at_unix_ms.get());
            let mut slot = read_slot(tx, node_id, slot_id)?;
            validate_slot_operation_correlation(tx, &slot)?;
            let previous_slot = slot.clone();
            let changed = apply_transition(&transition, &mut operation, &mut slot, effective_now)?;
            if !changed {
                return Ok(CommitReceipt {
                    epoch,
                    revision: DecimalU64::new(revision),
                    cursor: DecimalU64::new(cursor),
                    event_id: None,
                });
            }
            let next_revision = revision.checked_add(1).ok_or_else(overflow_error)?;
            let next_cursor = cursor.checked_add(1).ok_or_else(overflow_error)?;
            operation.updated_revision = DecimalU64::new(next_revision);
            operation.updated_at_unix_ms = DecimalU64::new(effective_now);
            let slot_changed = slot != previous_slot;
            let v1_sequence = next_v1_sequence(tx, node_instance_id)?;
            let preflight_event = operation_event(
                EventPosition {
                    event_id: placeholder_event_id(),
                    epoch,
                    sequence: next_cursor,
                    revision: next_revision,
                },
                effective_now,
                node_instance_id,
                &operation,
                slot_changed.then_some(slot.clone()),
            )?;
            serialize_event_with_limit(&preflight_event, event_limit)?;
            let event_id = ids.new_event_id();
            let event = operation_event(
                EventPosition {
                    event_id,
                    epoch,
                    sequence: next_cursor,
                    revision: next_revision,
                },
                effective_now,
                node_instance_id,
                &operation,
                slot_changed.then_some(slot.clone()),
            )?;
            let payload = serialize_event_with_limit(&event, event_limit)?;
            write_operation(tx, &operation, slot_id)?;
            if slot_changed {
                write_slot(tx, &slot, next_revision, effective_now)?;
                write_intent_for_observed_slot(tx, &slot, next_revision)?;
            }
            update_meta(tx, next_revision, next_cursor, effective_now)?;
            insert_event(tx, &event, Some(v1_sequence), &payload)?;
            prune(tx)?;
            validate_written_event(tx, event_id, &event)?;
            Ok(CommitReceipt {
                epoch,
                revision: DecimalU64::new(next_revision),
                cursor: DecimalU64::new(next_cursor),
                event_id: Some(event_id),
            })
        })
        .map_err(map_tagged_error)
    }

    pub(crate) fn committed_state(&self) -> Result<CommittedState, RepositoryError> {
        let node_id = self.node_id();
        let slot_id = self.slot_id();
        self.read_transaction(|connection| {
            let (revision, cursor, last_committed_at_unix_ms) = read_meta_connection(connection)?;
            let node = read_node_connection(connection, node_id)?;
            let slot = read_slot_connection(connection, node_id, slot_id)?;
            let mut operations = Vec::new();
            let mut statement = connection.prepare("SELECT operation_id FROM operations ORDER BY length(created_revision),created_revision,operation_id")?;
            let ids = statement.query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            for id in ids {
                let operation_id = OperationId::from_str(&id).map_err(|_| RepositoryError::corrupt_for_state_machine())?;
                operations.push(read_operation_connection(connection, node_id, slot_id, operation_id)?
                    .ok_or_else(RepositoryError::corrupt_for_state_machine)?);
            }
            let mut statement = connection.prepare("SELECT payload_json FROM events ORDER BY length(sequence),sequence,event_id")?;
            let payloads = statement.query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let events = payloads.into_iter().map(|payload| serde_json::from_str(&payload)
                .map_err(|_| RepositoryError::corrupt_for_state_machine())).collect::<Result<Vec<_>, _>>()?;
            let current_instance_v1 = read_current_instance_v1(
                connection,
                node.as_ref().map(|node| node.node_instance_id),
                node_id,
                slot_id,
            )?;
            Ok(CommittedState {
                revision: DecimalU64::new(revision),
                cursor: DecimalU64::new(cursor),
                last_committed_at_unix_ms: DecimalU64::new(last_committed_at_unix_ms),
                node,
                slot,
                operations,
                events,
                current_instance_v1,
            })
        })
    }
}

fn read_current_instance_v1(
    connection: &rusqlite::Connection,
    current_instance: Option<NodeInstanceId>,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
) -> Result<CurrentInstanceV1State, RepositoryError> {
    let Some(current_instance) = current_instance else {
        return Ok(CurrentInstanceV1State::default());
    };
    let instance = current_instance.to_string();
    let mut operation_rows = connection.prepare(
        "SELECT operation_id,v1_ordinal FROM operations WHERE admitting_node_instance_id=?1 AND v1_ordinal IS NOT NULL ORDER BY v1_ordinal,operation_id",
    )?;
    let operation_rows = operation_rows
        .query_map([&instance], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut operations = Vec::with_capacity(operation_rows.len());
    for (operation_id, ordinal) in operation_rows {
        let operation_id = OperationId::from_str(&operation_id)
            .map_err(|_| RepositoryError::corrupt_for_state_machine())?;
        let ordinal = u64::try_from(ordinal)
            .ok()
            .filter(|ordinal| *ordinal > 0)
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?;
        let operation = read_operation_connection(connection, node_id, slot_id, operation_id)?
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?;
        operations.push(CurrentInstanceV1Operation {
            v1_operation_id: format!("op-{ordinal}"),
            operation,
        });
    }

    let mut event_rows = connection.prepare(
        "SELECT v1_sequence,payload_json FROM events WHERE node_instance_id=?1 AND v1_sequence IS NOT NULL ORDER BY v1_sequence DESC,event_id DESC",
    )?;
    let event_rows = event_rows
        .query_map([&instance], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut events: Vec<CurrentInstanceV1Event> = Vec::with_capacity(event_rows.len().min(128));
    for (sequence, payload) in event_rows {
        let sequence = u64::try_from(sequence)
            .ok()
            .filter(|sequence| *sequence > 0)
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?;
        let event: V2ControlEvent = serde_json::from_str(&payload)
            .map_err(|_| RepositoryError::corrupt_for_state_machine())?;
        if event.node_instance_id != Some(current_instance) {
            return Err(RepositoryError::corrupt_for_state_machine());
        }
        let operation = event
            .operation
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?;
        let ordinal = connection
            .query_row(
                "SELECT v1_ordinal FROM operations WHERE operation_id=?1 AND admitting_node_instance_id=?2",
                rusqlite::params![operation.operation_id.to_string(), &instance],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .and_then(|ordinal| u64::try_from(ordinal).ok())
            .filter(|ordinal| *ordinal > 0);
        let Some(ordinal) = ordinal else {
            if events.is_empty() {
                continue;
            }
            break;
        };
        if events
            .last()
            .is_some_and(|newer| sequence.checked_add(1) != Some(newer.sequence))
        {
            break;
        }
        events.push(CurrentInstanceV1Event {
            sequence,
            v1_operation_id: format!("op-{ordinal}"),
            operation,
        });
        if events.len() == 128 {
            break;
        }
    }
    events.reverse();
    let cursor = events.last().map_or(0, |event| event.sequence);
    Ok(CurrentInstanceV1State {
        cursor,
        operations,
        events,
    })
}

fn apply_transition(
    transition: &Transition,
    operation: &mut V2Operation,
    slot: &mut V2Slot,
    effective_now: u64,
) -> Result<bool, RepositoryError> {
    use V2OperationStatus as S;
    if operation.status == S::Succeeded
        || operation.status == S::Failed
        || operation.status == S::Cancelled
    {
        return terminal_replay(transition, operation);
    }
    match transition {
        Transition::Started { progress, .. } => match operation.status {
            S::Queued => {
                if operation.kind == V2OperationKind::Download {
                    if let Some(progress) = progress {
                        progress
                            .validate()
                            .map_err(|_| tagged_error(TransitionError::Contradiction))?;
                        let admitted = operation
                            .progress
                            .as_ref()
                            .ok_or_else(|| tagged_error(TransitionError::CorruptState))?;
                        if progress.completed_bytes < admitted.completed_bytes
                            || (admitted.total_bytes.is_some()
                                && progress.total_bytes != admitted.total_bytes)
                        {
                            return Err(tagged_error(TransitionError::Contradiction));
                        }
                        operation.progress = Some(progress.clone());
                    }
                } else if progress.is_some() {
                    return Err(tagged_error(TransitionError::Contradiction));
                }
                operation.status = S::Running;
                apply_slot_start(operation, slot)?;
                Ok(true)
            }
            S::Running if operation.progress == *progress => Ok(false),
            _ => Err(tagged_error(TransitionError::IllegalTransition)),
        },
        Transition::Progress { progress, .. } => {
            if operation.kind != V2OperationKind::Download || operation.status != S::Running {
                return Err(tagged_error(TransitionError::IllegalTransition));
            }
            progress
                .validate()
                .map_err(|_| tagged_error(TransitionError::Contradiction))?;
            let old = operation
                .progress
                .as_ref()
                .ok_or_else(|| tagged_error(TransitionError::CorruptState))?;
            if progress.completed_bytes < old.completed_bytes
                || old.total_bytes.is_some() && progress.total_bytes != old.total_bytes
                || progress
                    .total_bytes
                    .is_some_and(|total| progress.completed_bytes > total)
            {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            if progress == old {
                return Ok(false);
            }
            let bytes_advanced = progress
                .completed_bytes
                .get()
                .saturating_sub(old.completed_bytes.get());
            let total_became_known = old.total_bytes.is_none() && progress.total_bytes.is_some();
            let time_elapsed = effective_now.saturating_sub(operation.updated_at_unix_ms.get());
            if bytes_advanced < PROGRESS_BYTES_THRESHOLD
                && time_elapsed < PROGRESS_TIME_THRESHOLD_MS
                && !total_became_known
            {
                return Ok(false);
            }
            operation.progress = Some(progress.clone());
            Ok(true)
        }
        Transition::Cancelling { .. } => match operation.status {
            S::Queued | S::Running => {
                operation.status = S::Cancelling;
                Ok(true)
            }
            S::Cancelling => Ok(false),
            _ => Err(tagged_error(TransitionError::IllegalTransition)),
        },
        Transition::Succeeded {
            observed_model_id, ..
        } => {
            if operation.status == S::Cancelling {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            if operation.status != S::Running {
                return Err(tagged_error(TransitionError::IllegalTransition));
            }
            match operation.kind {
                V2OperationKind::Load => {
                    let observed = observed_model_id
                        .as_ref()
                        .ok_or_else(|| tagged_error(TransitionError::Contradiction))?;
                    if operation.model_id.as_ref() != Some(observed) {
                        return Err(tagged_error(TransitionError::Contradiction));
                    }
                    slot.status = V2SlotStatus::Ready;
                    slot.model_id = Some(observed.clone());
                    slot.operation_id = None;
                    slot.error = None;
                }
                V2OperationKind::Unload => {
                    if observed_model_id.is_some() {
                        return Err(tagged_error(TransitionError::Contradiction));
                    }
                    slot.status = V2SlotStatus::Unloaded;
                    slot.model_id = None;
                    slot.operation_id = None;
                    slot.error = None;
                }
                V2OperationKind::Download => {
                    if observed_model_id.is_some() {
                        return Err(tagged_error(TransitionError::Contradiction));
                    }
                }
            }
            operation.status = S::Succeeded;
            operation.error = None;
            Ok(true)
        }
        Transition::Failed { error, .. } => {
            if !matches!(operation.status, S::Queued | S::Running | S::Cancelling) {
                return Err(tagged_error(TransitionError::IllegalTransition));
            }
            restore_slot_after_lifecycle(operation, slot)?;
            operation.status = S::Failed;
            operation.error = Some(error.clone());
            Ok(true)
        }
        Transition::Cancelled { .. } => {
            if !matches!(operation.status, S::Queued | S::Running | S::Cancelling) {
                return Err(tagged_error(TransitionError::IllegalTransition));
            }
            restore_slot_after_lifecycle(operation, slot)?;
            operation.status = S::Cancelled;
            operation.error = None;
            Ok(true)
        }
    }
}

fn apply_recovery_decision_to_slot(
    slot: &mut V2Slot,
    decision: &RecoveryDecision,
) -> Result<(), RepositoryError> {
    slot.operation_id = None;
    match decision {
        RecoveryDecision::Unloaded => {
            slot.status = V2SlotStatus::Unloaded;
            slot.model_id = None;
            slot.error = None;
        }
        RecoveryDecision::Ready { authority } => {
            slot.status = V2SlotStatus::Ready;
            slot.model_id = Some(authority.model_id().to_owned());
            slot.error = None;
        }
        RecoveryDecision::Recovery {
            error: SlotRecoveryError::LifecycleRecoveryRequired,
        } => {
            slot.status = V2SlotStatus::Recovery;
            slot.model_id = None;
            slot.error = Some(V2PublicError {
                code: V2SlotErrorCode::LifecycleRecoveryRequired,
                message: "exact lifecycle ownership could not be recovered".into(),
            });
        }
    }
    slot.validate()
        .map_err(|_| tagged_error(TransitionError::Contradiction))
}

fn terminal_replay(
    transition: &Transition,
    operation: &V2Operation,
) -> Result<bool, RepositoryError> {
    let identical = match (operation.status, transition) {
        (
            V2OperationStatus::Succeeded,
            Transition::Succeeded {
                observed_model_id, ..
            },
        ) => match operation.kind {
            V2OperationKind::Load => operation.model_id == *observed_model_id,
            _ => observed_model_id.is_none(),
        },
        (V2OperationStatus::Failed, Transition::Failed { error, .. }) => {
            operation.error.as_ref() == Some(error)
        }
        (V2OperationStatus::Cancelled, Transition::Cancelled { .. }) => true,
        (V2OperationStatus::Cancelled, Transition::Cancelling { .. }) => true,
        _ => false,
    };
    if identical {
        Ok(false)
    } else {
        Err(tagged_error(TransitionError::Contradiction))
    }
}

fn apply_slot_start(operation: &V2Operation, slot: &mut V2Slot) -> Result<(), RepositoryError> {
    match operation.kind {
        V2OperationKind::Download => {}
        V2OperationKind::Load => {
            if slot.status == V2SlotStatus::Loading {
                if slot.operation_id == Some(operation.operation_id) {
                    return Ok(());
                }
                return Err(tagged_error(TransitionError::Contradiction));
            }
            if !matches!(slot.status, V2SlotStatus::Unloaded | V2SlotStatus::Ready) {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            slot.status = V2SlotStatus::Loading;
            slot.operation_id = Some(operation.operation_id);
            slot.error = None;
        }
        V2OperationKind::Unload => {
            if slot.status == V2SlotStatus::Unloading {
                if slot.operation_id == Some(operation.operation_id) && slot.model_id.is_some() {
                    return Ok(());
                }
                return Err(tagged_error(TransitionError::Contradiction));
            }
            if slot.status != V2SlotStatus::Ready || slot.model_id.is_none() {
                return Err(tagged_error(TransitionError::Contradiction));
            }
            slot.status = V2SlotStatus::Unloading;
            slot.operation_id = Some(operation.operation_id);
            slot.error = None;
        }
    }
    Ok(())
}

fn restore_slot_after_lifecycle(
    operation: &V2Operation,
    slot: &mut V2Slot,
) -> Result<(), RepositoryError> {
    if operation.kind == V2OperationKind::Load {
        slot.status = if slot.model_id.is_some() {
            V2SlotStatus::Ready
        } else {
            V2SlotStatus::Unloaded
        };
        slot.operation_id = None;
        slot.error = None;
    } else if operation.kind == V2OperationKind::Unload {
        if slot.model_id.is_none() {
            return Err(tagged_error(TransitionError::CorruptState));
        }
        slot.status = V2SlotStatus::Ready;
        slot.operation_id = None;
        slot.error = None;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct EventPosition {
    event_id: EventId,
    epoch: StreamEpoch,
    sequence: u64,
    revision: u64,
}

fn operation_event(
    position: EventPosition,
    committed_at: u64,
    instance: NodeInstanceId,
    operation: &V2Operation,
    slot: Option<V2Slot>,
) -> Result<V2ControlEvent, RepositoryError> {
    let event = V2ControlEvent {
        schema_version: V2_SCHEMA_VERSION,
        event_id: position.event_id,
        epoch: position.epoch,
        sequence: DecimalU64::new(position.sequence),
        revision: DecimalU64::new(position.revision),
        committed_at_unix_ms: DecimalU64::new(committed_at),
        entity: V2EventEntity::Operation,
        entity_id: operation.operation_id.to_string(),
        node_id: operation.node_id,
        node_instance_id: Some(instance),
        slot_id: operation.slot_id,
        operation_id: Some(operation.operation_id),
        node: None,
        slot,
        operation: Some(operation.clone()),
    };
    event
        .validate()
        .map_err(|_| tagged_error(TransitionError::CorruptState))?;
    Ok(event)
}

fn serialize_event(event: &V2ControlEvent) -> Result<String, RepositoryError> {
    serialize_event_with_limit(event, MAX_EVENT_BYTES)
}

fn serialize_event_with_limit(
    event: &V2ControlEvent,
    maximum_bytes: usize,
) -> Result<String, RepositoryError> {
    let payload = serde_json::to_string(event).map_err(|_| transition_repository_error())?;
    if payload.len() > maximum_bytes {
        return Err(tagged_error(TransitionError::EventTooLarge));
    }
    Ok(payload)
}

fn require_published_instance(
    tx: &SqlTransaction<'_>,
    instance: NodeInstanceId,
) -> Result<(), RepositoryError> {
    require_instance_status(tx, instance, |status| status == "running")
}

fn require_observable_instance(
    tx: &SqlTransaction<'_>,
    instance: NodeInstanceId,
) -> Result<(), RepositoryError> {
    require_instance_status(tx, instance, |status| {
        matches!(status, "running" | "stopping")
    })
}

fn require_instance_status(
    tx: &SqlTransaction<'_>,
    instance: NodeInstanceId,
    accepts: impl FnOnce(&str) -> bool,
) -> Result<(), RepositoryError> {
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT node_instance_id,status FROM node_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if row
        .as_ref()
        .is_none_or(|(id, status)| id != &instance.to_string() || !accepts(status))
    {
        return Err(tagged_error(TransitionError::CorruptState));
    }
    Ok(())
}

fn read_meta(tx: &SqlTransaction<'_>) -> Result<(u64, u64, u64), RepositoryError> {
    read_meta_connection(tx)
}
fn read_meta_connection(
    connection: &rusqlite::Connection,
) -> Result<(u64, u64, u64), RepositoryError> {
    let raw: (String, String, String) = connection.query_row(
        "SELECT revision,cursor,last_committed_at_unix_ms FROM control_meta WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok((parse_u64(&raw.0)?, parse_u64(&raw.1)?, parse_u64(&raw.2)?))
}

fn read_operation(
    tx: &SqlTransaction<'_>,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
    operation_id: OperationId,
) -> Result<Option<V2Operation>, RepositoryError> {
    read_operation_connection(tx, node_id, slot_id, operation_id)
}
fn read_operation_connection(
    connection: &rusqlite::Connection,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
    operation_id: OperationId,
) -> Result<Option<V2Operation>, RepositoryError> {
    let operation = connection.query_row("SELECT kind,status,model_id,progress_current,progress_total,error_code,error_message,created_revision,updated_revision,created_at_unix_ms,updated_at_unix_ms FROM operations WHERE operation_id=?1", [operation_id.to_string()], |row| decode_operation_row(row, operation_id, node_id, slot_id)).optional().map_err(RepositoryError::from)?;
    if operation
        .as_ref()
        .is_some_and(|operation| operation.validate().is_err())
    {
        return Err(RepositoryError::corrupt_for_state_machine());
    }
    Ok(operation)
}

fn decode_operation_row(
    row: &Row<'_>,
    operation_id: OperationId,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
) -> rusqlite::Result<V2Operation> {
    let kind: String = row.get(0)?;
    let status: String = row.get(1)?;
    let current: Option<String> = row.get(3)?;
    let total: Option<String> = row.get(4)?;
    let error_code: Option<String> = row.get(5)?;
    let error_message: Option<String> = row.get(6)?;
    let kind = parse_kind(&kind).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let status = parse_status(&status).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let progress = match (current, total) {
        (None, None) => None,
        (Some(c), t) => Some(V2OperationProgress {
            completed_bytes: DecimalU64::new(
                parse_u64(&c).map_err(|_| rusqlite::Error::InvalidQuery)?,
            ),
            total_bytes: t
                .map(|v| parse_u64(&v).map(DecimalU64::new))
                .transpose()
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
        }),
        (None, Some(_)) => return Err(rusqlite::Error::InvalidQuery),
    };
    let error = match (error_code, error_message) {
        (None, None) => None,
        (Some(code), Some(message)) => Some(V2OperationError {
            code: serde_json::from_value(serde_json::Value::String(code))
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
            message,
        }),
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(V2Operation {
        operation_id,
        node_id,
        kind,
        status,
        slot_id: (kind != V2OperationKind::Download).then_some(slot_id),
        model_id: row.get(2)?,
        progress,
        error,
        created_revision: DecimalU64::new(
            parse_u64(&row.get::<_, String>(7)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        updated_revision: DecimalU64::new(
            parse_u64(&row.get::<_, String>(8)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        created_at_unix_ms: DecimalU64::new(
            parse_u64(&row.get::<_, String>(9)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        updated_at_unix_ms: DecimalU64::new(
            parse_u64(&row.get::<_, String>(10)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
    })
}

fn read_slot(
    tx: &SqlTransaction<'_>,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
) -> Result<V2Slot, RepositoryError> {
    read_slot_connection(tx, node_id, slot_id)
}
fn read_slot_connection(
    connection: &rusqlite::Connection,
    node_id: loxa_protocol::NodeId,
    slot_id: loxa_protocol::v2::SlotId,
) -> Result<V2Slot, RepositoryError> {
    let raw: (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = connection.query_row(
        "SELECT status,model_id,operation_id,error_code,error_message FROM slot_state WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?;
    let error = match (raw.3.as_deref(), raw.4) {
        (None, None) => None,
        (Some("lifecycle_recovery_required"), Some(message)) => Some(V2PublicError {
            code: V2SlotErrorCode::LifecycleRecoveryRequired,
            message,
        }),
        _ => return Err(RepositoryError::corrupt_for_state_machine()),
    };
    let slot = V2Slot {
        slot_id,
        node_id,
        name: "default".into(),
        status: parse_slot_status(&raw.0)?,
        model_id: raw.1,
        operation_id: raw
            .2
            .map(|v| OperationId::from_str(&v))
            .transpose()
            .map_err(|_| RepositoryError::corrupt_for_state_machine())?,
        error,
    };
    slot.validate()
        .map_err(|_| RepositoryError::corrupt_for_state_machine())?;
    Ok(slot)
}

fn read_node_connection(
    connection: &rusqlite::Connection,
    node_id: loxa_protocol::NodeId,
) -> Result<Option<V2Node>, RepositoryError> {
    let raw: (
        Option<String>, Option<String>, String, i64, i64, i64, i64, i64,
    ) = connection.query_row(
        "SELECT node_instance_id,control_endpoint,status,model_download,slot_load,slot_unload,operation_cancel,operation_stream FROM node_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
    )?;
    let bits = [raw.3, raw.4, raw.5, raw.6, raw.7];
    if !bits.into_iter().all(|bit| matches!(bit, 0 | 1)) {
        return Err(RepositoryError::corrupt_for_state_machine());
    }
    if raw.2 == "unpublished" {
        if raw.0.is_some() || raw.1.is_some() || bits != [0, 0, 0, 0, 0] {
            return Err(RepositoryError::corrupt_for_state_machine());
        }
        return Ok(None);
    }
    let status = match raw.2.as_str() {
        "running" => V2NodeStatus::Running,
        "stopping" => V2NodeStatus::Stopping,
        _ => return Err(RepositoryError::corrupt_for_state_machine()),
    };
    let node = V2Node {
        node_id,
        node_instance_id: raw
            .0
            .as_deref()
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?
            .parse()
            .map_err(|_| RepositoryError::corrupt_for_state_machine())?,
        control_endpoint: raw
            .1
            .ok_or_else(RepositoryError::corrupt_for_state_machine)?,
        status,
        slot_capacity: 1,
        capabilities: V2NodeCapabilities {
            model_download: raw.3 == 1,
            slot_load: raw.4 == 1,
            slot_unload: raw.5 == 1,
            operation_cancel: raw.6 == 1,
            operation_stream: raw.7 == 1,
        },
    };
    serde_json::to_value(&node).map_err(|_| RepositoryError::corrupt_for_state_machine())?;
    Ok(Some(node))
}

fn validate_slot_operation_correlation(
    tx: &SqlTransaction<'_>,
    slot: &V2Slot,
) -> Result<(), RepositoryError> {
    let Some(operation_id) = slot.operation_id else {
        return Ok(());
    };
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT kind,status FROM operations WHERE operation_id=?1",
            [operation_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let valid = row.is_some_and(|(kind, status)| {
        matches!(status.as_str(), "queued" | "running" | "cancelling")
            && matches!(
                (slot.status, kind.as_str()),
                (V2SlotStatus::Loading, "load") | (V2SlotStatus::Unloading, "unload")
            )
    });
    if !valid {
        return Err(tagged_error(TransitionError::CorruptState));
    }
    Ok(())
}

fn write_operation(
    tx: &SqlTransaction<'_>,
    op: &V2Operation,
    physical_slot: loxa_protocol::v2::SlotId,
) -> Result<(), RepositoryError> {
    tx.execute("UPDATE operations SET status=?2,model_id=?3,progress_current=?4,progress_total=?5,error_code=?6,error_message=?7,updated_revision=?8,updated_at_unix_ms=?9 WHERE operation_id=?1",rusqlite::params![op.operation_id.to_string(),status_text(op.status),op.model_id,op.progress.as_ref().map(|p|p.completed_bytes.to_string()),op.progress.as_ref().and_then(|p|p.total_bytes).map(|v|v.to_string()),op.error.as_ref().map(|e|error_code_text(e.code)),op.error.as_ref().map(|e|e.message.clone()),op.updated_revision.to_string(),op.updated_at_unix_ms.to_string()])?;
    let _: loxa_protocol::v2::SlotId = physical_slot;
    Ok(())
}

fn write_slot(
    tx: &SqlTransaction<'_>,
    slot: &V2Slot,
    revision: u64,
    now: u64,
) -> Result<(), RepositoryError> {
    tx.execute("UPDATE slot_state SET status=?1,model_id=?2,operation_id=?3,error_code=?4,error_message=?5,updated_revision=?6,updated_at_unix_ms=?7 WHERE singleton=1",rusqlite::params![slot_status_text(slot.status),slot.model_id,slot.operation_id.map(|id|id.to_string()),slot.error.as_ref().map(|_|"lifecycle_recovery_required"),slot.error.as_ref().map(|error|error.message.clone()),revision.to_string(),now.to_string()])?;
    Ok(())
}

fn write_intent(
    tx: &SqlTransaction<'_>,
    desired_kind: DesiredKind,
    desired_model_id: Option<&str>,
    desired_revision: u64,
    operation_id: Option<OperationId>,
    reconciliation: ReconciliationState,
    reason: Option<IntentReason>,
) -> Result<(), RepositoryError> {
    let updated = tx.execute(
        "UPDATE slot_intent SET desired_kind=?1,desired_model_id=?2,desired_revision=?3,operation_id=?4,reconciliation_state=?5,reason_code=?6 WHERE singleton=1",
        rusqlite::params![
            match desired_kind {
                DesiredKind::Unloaded => "unloaded",
                DesiredKind::Loaded => "loaded",
                DesiredKind::Unknown => "unknown",
            },
            desired_model_id,
            desired_revision.to_string(),
            operation_id.map(|id| id.to_string()),
            match reconciliation {
                ReconciliationState::Settled => "settled",
                ReconciliationState::Applying => "applying",
                ReconciliationState::RecoveryRequired => "recovery_required",
            },
            reason.map(|reason| match reason {
                IntentReason::PreexistingRecovery => "preexisting_recovery",
                IntentReason::MigrationAmbiguousLoading => "migration_ambiguous_loading",
                IntentReason::MigrationOperationMismatch => "migration_operation_mismatch",
                IntentReason::ChildEvidenceUncertain => "child_evidence_uncertain",
                IntentReason::CompensationFailed => "compensation_failed",
                IntentReason::DurableCommitUncertain => "durable_commit_uncertain",
            }),
        ],
    )?;
    if updated != 1 {
        return Err(tagged_error(TransitionError::CorruptState));
    }
    Ok(())
}

fn write_intent_for_observed_slot(
    tx: &SqlTransaction<'_>,
    slot: &V2Slot,
    revision: u64,
) -> Result<(), RepositoryError> {
    match slot.status {
        V2SlotStatus::Unloaded => write_intent(
            tx,
            DesiredKind::Unloaded,
            None,
            revision,
            None,
            ReconciliationState::Settled,
            None,
        ),
        V2SlotStatus::Ready => write_intent(
            tx,
            DesiredKind::Loaded,
            slot.model_id.as_deref(),
            revision,
            None,
            ReconciliationState::Settled,
            None,
        ),
        V2SlotStatus::Recovery => write_intent(
            tx,
            DesiredKind::Unknown,
            None,
            revision,
            None,
            ReconciliationState::RecoveryRequired,
            Some(IntentReason::ChildEvidenceUncertain),
        ),
        V2SlotStatus::Loading | V2SlotStatus::Unloading => {
            Err(tagged_error(TransitionError::CorruptState))
        }
    }
}

fn update_meta(
    tx: &SqlTransaction<'_>,
    revision: u64,
    cursor: u64,
    now: u64,
) -> Result<(), RepositoryError> {
    tx.execute("UPDATE control_meta SET revision=?1,cursor=?2,last_committed_at_unix_ms=?3 WHERE singleton=1",[revision.to_string(),cursor.to_string(),now.to_string()])?;
    Ok(())
}
fn next_v1_sequence(
    tx: &SqlTransaction<'_>,
    instance: NodeInstanceId,
) -> Result<i64, RepositoryError> {
    let maximum: Option<i64> = tx.query_row(
        "SELECT MAX(v1_sequence) FROM events WHERE node_instance_id=?1",
        [instance.to_string()],
        |r| r.get(0),
    )?;
    maximum
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(overflow_error)
}
fn insert_event(
    tx: &SqlTransaction<'_>,
    event: &V2ControlEvent,
    v1: Option<i64>,
    payload: &str,
) -> Result<(), RepositoryError> {
    let kind = match event.entity {
        V2EventEntity::Node => "node_changed",
        V2EventEntity::Slot => "slot_changed",
        V2EventEntity::Operation => "operation_changed",
    };
    tx.execute("INSERT INTO events(event_id,stream_epoch,sequence,revision,node_instance_id,v1_sequence,event_kind,payload_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",rusqlite::params![event.event_id.to_string(),event.epoch.to_string(),event.sequence.to_string(),event.revision.to_string(),event.node_instance_id.map(|id|id.to_string()),v1,kind,payload])?;
    Ok(())
}
fn prune(tx: &SqlTransaction<'_>) -> Result<(), RepositoryError> {
    tx.execute("DELETE FROM operations WHERE operation_id IN (SELECT operation_id FROM operations WHERE status IN ('succeeded','failed','cancelled') ORDER BY length(updated_revision) DESC,updated_revision DESC,operation_id DESC LIMIT -1 OFFSET ?1)",[MAX_TERMINAL_OPERATIONS])?;
    tx.execute("DELETE FROM events WHERE event_id IN (SELECT event_id FROM events ORDER BY length(sequence) DESC,sequence DESC,event_id DESC LIMIT -1 OFFSET ?1)",[MAX_EVENTS])?;
    Ok(())
}
fn validate_written_event(
    tx: &SqlTransaction<'_>,
    id: EventId,
    expected: &V2ControlEvent,
) -> Result<(), RepositoryError> {
    let payload: String = tx.query_row(
        "SELECT payload_json FROM events WHERE event_id=?1",
        [id.to_string()],
        |r| r.get(0),
    )?;
    let actual: V2ControlEvent =
        serde_json::from_str(&payload).map_err(|_| transition_repository_error())?;
    if &actual != expected {
        return Err(tagged_error(TransitionError::CorruptState));
    }
    Ok(())
}

fn kind_text(v: V2OperationKind) -> &'static str {
    match v {
        V2OperationKind::Download => "download",
        V2OperationKind::Load => "load",
        V2OperationKind::Unload => "unload",
    }
}
fn status_text(v: V2OperationStatus) -> &'static str {
    match v {
        V2OperationStatus::Queued => "queued",
        V2OperationStatus::Running => "running",
        V2OperationStatus::Cancelling => "cancelling",
        V2OperationStatus::Succeeded => "succeeded",
        V2OperationStatus::Failed => "failed",
        V2OperationStatus::Cancelled => "cancelled",
    }
}

fn parse_operation_status(value: &str) -> Result<V2OperationStatus, RepositoryError> {
    match value {
        "queued" => Ok(V2OperationStatus::Queued),
        "running" => Ok(V2OperationStatus::Running),
        "cancelling" => Ok(V2OperationStatus::Cancelling),
        "succeeded" => Ok(V2OperationStatus::Succeeded),
        "failed" => Ok(V2OperationStatus::Failed),
        "cancelled" => Ok(V2OperationStatus::Cancelled),
        _ => Err(RepositoryError::corrupt_for_state_machine()),
    }
}
fn slot_status_text(v: V2SlotStatus) -> &'static str {
    match v {
        V2SlotStatus::Unloaded => "unloaded",
        V2SlotStatus::Loading => "loading",
        V2SlotStatus::Ready => "ready",
        V2SlotStatus::Unloading => "unloading",
        V2SlotStatus::Recovery => "recovery",
    }
}
fn error_code_text(v: loxa_protocol::v2::V2OperationErrorCode) -> &'static str {
    match v {
        loxa_protocol::v2::V2OperationErrorCode::DownloadFailed => "download_failed",
        loxa_protocol::v2::V2OperationErrorCode::LoadFailed => "load_failed",
        loxa_protocol::v2::V2OperationErrorCode::UnloadFailed => "unload_failed",
        loxa_protocol::v2::V2OperationErrorCode::NodeRestartedBeforeStart => {
            "node_restarted_before_start"
        }
        loxa_protocol::v2::V2OperationErrorCode::NodeRestarted => "node_restarted",
        loxa_protocol::v2::V2OperationErrorCode::CancellationOutcomeUnknown => {
            "cancellation_outcome_unknown"
        }
    }
}
fn parse_kind(v: &str) -> Result<V2OperationKind, RepositoryError> {
    match v {
        "download" => Ok(V2OperationKind::Download),
        "load" => Ok(V2OperationKind::Load),
        "unload" => Ok(V2OperationKind::Unload),
        _ => Err(RepositoryError::corrupt_for_state_machine()),
    }
}
fn parse_status(v: &str) -> Result<V2OperationStatus, RepositoryError> {
    match v {
        "queued" => Ok(V2OperationStatus::Queued),
        "running" => Ok(V2OperationStatus::Running),
        "cancelling" => Ok(V2OperationStatus::Cancelling),
        "succeeded" => Ok(V2OperationStatus::Succeeded),
        "failed" => Ok(V2OperationStatus::Failed),
        "cancelled" => Ok(V2OperationStatus::Cancelled),
        _ => Err(RepositoryError::corrupt_for_state_machine()),
    }
}
fn parse_slot_status(v: &str) -> Result<V2SlotStatus, RepositoryError> {
    match v {
        "unloaded" => Ok(V2SlotStatus::Unloaded),
        "loading" => Ok(V2SlotStatus::Loading),
        "ready" => Ok(V2SlotStatus::Ready),
        "unloading" => Ok(V2SlotStatus::Unloading),
        "recovery" => Ok(V2SlotStatus::Recovery),
        _ => Err(RepositoryError::corrupt_for_state_machine()),
    }
}
fn parse_u64(v: &str) -> Result<u64, RepositoryError> {
    if v.is_empty() || (v.len() > 1 && v.starts_with('0')) || !v.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(RepositoryError::corrupt_for_state_machine());
    }
    v.parse()
        .map_err(|_| RepositoryError::corrupt_for_state_machine())
}
fn valid_model_id(v: &str) -> bool {
    !v.is_empty() && v.len() <= 256 && v.trim() == v && !v.chars().any(char::is_control)
}

const TAG_ACTIVE: &str = "__transition_active_limit";
const TAG_LIFECYCLE: &str = "__transition_lifecycle_conflict";
const TAG_SAME: &str = "__transition_same_model_conflict";
const TAG_NOT_FOUND: &str = "__transition_not_found";
const TAG_ILLEGAL: &str = "__transition_illegal";
const TAG_CONTRADICTION: &str = "__transition_contradiction";
const TAG_LARGE: &str = "__transition_event_too_large";
const TAG_OVERFLOW: &str = "__transition_overflow";
const TAG_CORRUPT: &str = "__transition_corrupt";
fn tagged_error(error: TransitionError) -> RepositoryError {
    RepositoryError::tagged_for_state_machine(match error {
        TransitionError::ActiveLimit => TAG_ACTIVE,
        TransitionError::LifecycleConflict => TAG_LIFECYCLE,
        TransitionError::SameModelConflict => TAG_SAME,
        TransitionError::OperationNotFound => TAG_NOT_FOUND,
        TransitionError::IllegalTransition => TAG_ILLEGAL,
        TransitionError::Contradiction => TAG_CONTRADICTION,
        TransitionError::EventTooLarge => TAG_LARGE,
        TransitionError::Overflow => TAG_OVERFLOW,
        TransitionError::CorruptState => TAG_CORRUPT,
        TransitionError::Repository(_) => TAG_CORRUPT,
    })
}
fn map_tagged_error(error: RepositoryError) -> TransitionError {
    match error.state_machine_tag() {
        Some(TAG_ACTIVE) => TransitionError::ActiveLimit,
        Some(TAG_LIFECYCLE) => TransitionError::LifecycleConflict,
        Some(TAG_SAME) => TransitionError::SameModelConflict,
        Some(TAG_NOT_FOUND) => TransitionError::OperationNotFound,
        Some(TAG_ILLEGAL) => TransitionError::IllegalTransition,
        Some(TAG_CONTRADICTION) => TransitionError::Contradiction,
        Some(TAG_LARGE) => TransitionError::EventTooLarge,
        Some(TAG_OVERFLOW) => TransitionError::Overflow,
        Some(TAG_CORRUPT) => TransitionError::CorruptState,
        _ => TransitionError::Repository(error.class()),
    }
}
fn transition_repository_error() -> RepositoryError {
    tagged_error(TransitionError::CorruptState)
}
fn overflow_error() -> RepositoryError {
    tagged_error(TransitionError::Overflow)
}

#[cfg(test)]
#[path = "test_support/mod.rs"]
pub(super) mod test_support;
