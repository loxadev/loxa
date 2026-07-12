#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_enforce_legal_transitions_and_safe_cancellation() {
        let mut store = OperationStore::new(3);
        let id = store.enqueue(OperationKind::Download, Some("gemma-3-4b-it-q4".into()), 10);
        store.start(&id, 11).unwrap();
        store.progress(&id, 4, Some(8), 12).unwrap();
        assert_eq!(
            store.cancel(&id, CancellationSafety::Safe, 13).unwrap(),
            OperationStatus::Cancelled
        );
        assert_eq!(
            store.succeed(&id, 14).unwrap_err(),
            OperationError::Terminal
        );

        let load = store.enqueue(OperationKind::Load, Some("gemma-3-4b-it-q4".into()), 15);
        store.start(&load, 16).unwrap();
        assert_eq!(
            store
                .cancel(&load, CancellationSafety::UnsafeAfterCommit, 17)
                .unwrap_err(),
            OperationError::CancellationNotSafe
        );
    }

    #[test]
    fn retention_sequences_and_reconnect_snapshots_are_bounded_and_monotonic() {
        let mut store = OperationStore::new(2);
        let first = store.enqueue(OperationKind::Download, Some("a".into()), 1);
        store.fail(&first, "failed", 2).unwrap();
        let second = store.enqueue(OperationKind::Download, Some("b".into()), 3);
        store.start(&second, 4).unwrap();
        store.succeed(&second, 4).unwrap();
        let third = store.enqueue(OperationKind::Unload, None, 5);
        store.start(&third, 6).unwrap();
        store.succeed(&third, 6).unwrap();
        let snapshot = store.snapshot_since(0);
        assert!(snapshot.cursor_gap);
        assert_eq!(snapshot.operations.len(), 2);
        assert!(
            snapshot
                .events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        assert_eq!(snapshot.operations[0].id, second);
    }

    #[test]
    fn dropping_event_subscription_removes_sender() {
        let mut store = OperationStore::new(2);
        let subscription = store.subscribe();
        assert_eq!(store.subscriber_count(), 1);
        drop(subscription);
        assert_eq!(store.subscriber_count(), 0);
    }

    #[test]
    fn conflicting_active_operation_is_rejected_and_never_evicted() {
        let mut store = OperationStore::new(1);
        let active = store.enqueue(OperationKind::Download, Some("a".into()), 1);
        store.start(&active, 2).unwrap();
        assert_eq!(
            store
                .enqueue_unique(OperationKind::Download, Some("a".into()), 3)
                .unwrap_err(),
            OperationError::Conflict
        );
        let completed = store.enqueue(OperationKind::Unload, None, 4);
        store.start(&completed, 5).unwrap();
        store.succeed(&completed, 6).unwrap();
        let snapshot = store.snapshot_since(0);
        assert!(snapshot.operations.iter().any(|item| item.id == active));
    }

    #[test]
    fn slow_event_subscriber_is_disconnected_before_queue_can_grow_unbounded() {
        let mut store = OperationStore::new(1);
        let _subscription = store.subscribe();
        for index in 0..4 {
            store.enqueue(OperationKind::Unload, None, index);
        }
        assert_eq!(store.subscriber_count(), 0);
    }
}
use super::contracts::{
    ControlEvent, OperationKind, OperationProgress, OperationStatus, OperationView,
    ReconnectSnapshot,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, mpsc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancellationSafety {
    Safe,
    UnsafeAfterCommit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationError {
    Missing,
    Conflict,
    IllegalTransition,
    Terminal,
    CancellationNotSafe,
    InvalidProgress,
}

#[derive(Default)]
struct Subscribers {
    next_id: u64,
    senders: HashMap<u64, mpsc::SyncSender<ControlEvent>>,
}

pub struct EventSubscription {
    pub receiver: mpsc::Receiver<ControlEvent>,
    id: u64,
    subscribers: Arc<Mutex<Subscribers>>,
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.subscribers.lock().unwrap().senders.remove(&self.id);
    }
}

pub struct OperationStore {
    capacity: usize,
    next_id: u64,
    next_sequence: u64,
    operations: VecDeque<OperationView>,
    events: VecDeque<ControlEvent>,
    subscribers: Arc<Mutex<Subscribers>>,
}

impl OperationStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_id: 1,
            next_sequence: 1,
            operations: VecDeque::new(),
            events: VecDeque::new(),
            subscribers: Arc::new(Mutex::new(Subscribers::default())),
        }
    }

    fn enqueue(&mut self, kind: OperationKind, model_id: Option<String>, now: u64) -> String {
        let id = format!("op-{}", self.next_id);
        self.next_id += 1;
        let view = OperationView {
            id: id.clone(),
            kind,
            status: OperationStatus::Queued,
            model_id,
            progress: None,
            error: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        self.operations.push_back(view);
        self.trim();
        self.publish(&id);
        id
    }

    pub fn enqueue_unique(
        &mut self,
        kind: OperationKind,
        model_id: Option<String>,
        now: u64,
    ) -> Result<String, OperationError> {
        let active = self.operations.iter().any(|item| {
            item.kind == kind
                && item.model_id == model_id
                && matches!(
                    item.status,
                    OperationStatus::Queued | OperationStatus::Running
                )
        });
        if active {
            return Err(OperationError::Conflict);
        }
        Ok(self.enqueue(kind, model_id, now))
    }

    pub fn start(&mut self, id: &str, now: u64) -> Result<(), OperationError> {
        self.transition(id, OperationStatus::Running, None, now)
    }
    pub fn succeed(&mut self, id: &str, now: u64) -> Result<(), OperationError> {
        self.transition(id, OperationStatus::Succeeded, None, now)
    }
    pub fn fail(&mut self, id: &str, message: &str, now: u64) -> Result<(), OperationError> {
        self.transition(id, OperationStatus::Failed, Some(message.to_owned()), now)
    }
    pub fn recovery_required(
        &mut self,
        id: &str,
        message: &str,
        now: u64,
    ) -> Result<(), OperationError> {
        self.transition(
            id,
            OperationStatus::RecoveryRequired,
            Some(message.to_owned()),
            now,
        )
    }

    pub fn progress(
        &mut self,
        id: &str,
        completed: u64,
        total: Option<u64>,
        now: u64,
    ) -> Result<(), OperationError> {
        let operation = self.find_mut(id)?;
        if operation.status != OperationStatus::Running {
            return Err(OperationError::IllegalTransition);
        }
        if total.is_some_and(|total| completed > total)
            || operation
                .progress
                .as_ref()
                .is_some_and(|old| completed < old.completed_bytes)
        {
            return Err(OperationError::InvalidProgress);
        }
        operation.progress = Some(OperationProgress {
            completed_bytes: completed,
            total_bytes: total,
        });
        operation.updated_at_unix_ms = now;
        self.publish(id);
        Ok(())
    }

    pub fn cancel(
        &mut self,
        id: &str,
        safety: CancellationSafety,
        now: u64,
    ) -> Result<OperationStatus, OperationError> {
        if safety == CancellationSafety::UnsafeAfterCommit {
            return Err(OperationError::CancellationNotSafe);
        }
        self.transition(id, OperationStatus::Cancelled, None, now)?;
        Ok(OperationStatus::Cancelled)
    }

    pub fn snapshot_since(&self, cursor: u64) -> ReconnectSnapshot {
        let oldest = self
            .events
            .front()
            .map_or(self.next_sequence, |event| event.sequence);
        ReconnectSnapshot {
            cursor: self.next_sequence.saturating_sub(1),
            cursor_gap: cursor.saturating_add(1) < oldest,
            operations: self.operations.iter().cloned().collect(),
            events: self
                .events
                .iter()
                .filter(|event| event.sequence > cursor)
                .cloned()
                .collect(),
        }
    }

    pub fn subscribe(&mut self) -> EventSubscription {
        let (sender, receiver) = mpsc::sync_channel(self.capacity);
        let mut subscribers = self.subscribers.lock().unwrap();
        let id = subscribers.next_id;
        subscribers.next_id += 1;
        subscribers.senders.insert(id, sender);
        drop(subscribers);
        EventSubscription {
            receiver,
            id,
            subscribers: self.subscribers.clone(),
        }
    }
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().unwrap().senders.len()
    }

    fn transition(
        &mut self,
        id: &str,
        next: OperationStatus,
        error: Option<String>,
        now: u64,
    ) -> Result<(), OperationError> {
        let operation = self.find_mut(id)?;
        if matches!(
            operation.status,
            OperationStatus::Succeeded
                | OperationStatus::Failed
                | OperationStatus::Cancelled
                | OperationStatus::RecoveryRequired
        ) {
            return Err(OperationError::Terminal);
        }
        let legal = matches!(
            (operation.status, next),
            (
                OperationStatus::Queued,
                OperationStatus::Running | OperationStatus::Cancelled | OperationStatus::Failed
            ) | (
                OperationStatus::Running,
                OperationStatus::Succeeded
                    | OperationStatus::Failed
                    | OperationStatus::Cancelled
                    | OperationStatus::RecoveryRequired
            )
        );
        if !legal {
            return Err(OperationError::IllegalTransition);
        }
        operation.status = next;
        operation.error = error;
        operation.updated_at_unix_ms = now;
        self.publish(id);
        self.trim();
        Ok(())
    }
    fn find_mut(&mut self, id: &str) -> Result<&mut OperationView, OperationError> {
        self.operations
            .iter_mut()
            .find(|item| item.id == id)
            .ok_or(OperationError::Missing)
    }
    fn publish(&mut self, id: &str) {
        let Some(operation) = self.operations.iter().find(|item| item.id == id).cloned() else {
            return;
        };
        let event = ControlEvent {
            sequence: self.next_sequence,
            operation,
        };
        self.next_sequence += 1;
        self.events.push_back(event.clone());
        while self.events.len() > self.capacity {
            self.events.pop_front();
        }
        self.subscribers
            .lock()
            .unwrap()
            .senders
            .retain(|_, sender| sender.try_send(event.clone()).is_ok());
    }
    fn trim(&mut self) {
        while self
            .operations
            .iter()
            .filter(|item| is_terminal(item.status))
            .count()
            > self.capacity
        {
            let Some(index) = self
                .operations
                .iter()
                .position(|item| is_terminal(item.status))
            else {
                break;
            };
            self.operations.remove(index);
        }
    }
}

fn is_terminal(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::Succeeded
            | OperationStatus::Failed
            | OperationStatus::Cancelled
            | OperationStatus::RecoveryRequired
    )
}
