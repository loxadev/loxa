use crate::artifact_coordinator::{ArtifactMutationLease, ArtifactReadLease};
use crate::download_scheduler::DownloadContinuation;
use crate::lifecycle_controller::LifecycleMailboxInner;
use crate::operation_cancellation::OperationCancellation;
use loxa_core::model_inventory::{
    StableVerificationIdentity, StableVerificationInput, VerifiedArtifact,
};
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::collections::{HashMap, VecDeque};
use std::mem::ManuallyDrop;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::JoinHandle;

pub(crate) const VERIFICATION_WORKERS: usize = 2;
pub(crate) const VERIFICATION_DOWNLOAD_WAITING: usize = 7;
pub(crate) const VERIFICATION_LIFECYCLE_WAITING: usize = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum VerificationClass {
    Download,
    Lifecycle,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum VerificationAlgorithm {
    Sha256,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum VerificationFormatPolicy {
    CurrentRecipeV1,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VerificationKey {
    stable: StableVerificationIdentity,
    algorithm: VerificationAlgorithm,
    expected_digest: [u8; 32],
    format_policy: VerificationFormatPolicy,
}

impl VerificationKey {
    pub(crate) fn new(stable: StableVerificationIdentity, expected_digest: [u8; 32]) -> Self {
        Self {
            stable,
            algorithm: VerificationAlgorithm::Sha256,
            expected_digest,
            format_policy: VerificationFormatPolicy::CurrentRecipeV1,
        }
    }

    fn matches(&self, input: &StableVerificationInput) -> bool {
        self.stable == input.stable && self.expected_digest == input.expected_sha256
    }
}

pub(crate) enum VerificationResult {
    Verified(VerifiedArtifact),
    Failed {
        kind: std::io::ErrorKind,
        message: String,
    },
    Cancelled,
}

impl From<std::io::Result<VerifiedArtifact>> for VerificationResult {
    fn from(result: std::io::Result<VerifiedArtifact>) -> Self {
        match result {
            Ok(evidence) => Self::Verified(evidence),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => Self::Cancelled,
            Err(error) => Self::Failed {
                kind: error.kind(),
                message: error.to_string(),
            },
        }
    }
}

pub(crate) struct VerificationWaiter {
    key: VerificationKey,
    waiter_id: u64,
    owner: Weak<VerificationShared>,
    released: bool,
}

impl Drop for VerificationWaiter {
    fn drop(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_waiter(&self.key, self.waiter_id);
            }
            self.released = true;
        }
    }
}

pub(crate) struct VerificationAdmissionReservation {
    key: VerificationKey,
    class: VerificationClass,
    owner: Weak<VerificationShared>,
    state: VerificationReservationState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationReservationState {
    Reserved,
    Bound,
    Poisoned,
}

pub(crate) struct LifecycleVerificationContinuation {
    pub(crate) operation_id: OperationId,
    pub(crate) admission_revision: DecimalU64,
    pub(crate) cancellation: OperationCancellation,
    pub(crate) artifact: ArtifactReadLease,
}

pub(crate) struct DownloadVerificationOwnership {
    pub(crate) operation_id: OperationId,
    pub(crate) admission_revision: DecimalU64,
    pub(crate) cancellation: OperationCancellation,
    pub(crate) artifact: ArtifactMutationLease,
}

pub(crate) struct DownloadVerificationOutcome {
    pub(crate) ownership: DownloadVerificationOwnership,
    pub(crate) result: VerificationResult,
}

pub(crate) struct LifecycleVerificationOutcome {
    pub(crate) ownership: LifecycleVerificationContinuation,
    pub(crate) result: VerificationResult,
}

pub(super) struct RetainedCompletionCell<T> {
    state: Mutex<CompletionTransferState<T>>,
}

enum CompletionTransferState<T> {
    Reserved,
    Ready(ManuallyDrop<T>),
    Acknowledged,
    Poisoned(ManuallyDrop<T>),
}

pub(crate) struct RetainedCompletion<T> {
    cell: Arc<RetainedCompletionCell<T>>,
}

impl<T> RetainedCompletion<T> {
    pub(crate) fn lock_ready(&self) -> Option<ReadyCompletionGuard<'_, T>> {
        let state = self
            .cell
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(&*state, CompletionTransferState::Ready(_)) {
            Some(ReadyCompletionGuard { state })
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(self) {
        let mut state = self
            .cell
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *state, CompletionTransferState::Acknowledged);
        if let CompletionTransferState::Poisoned(mut outcome) = previous {
            // SAFETY: this test-only fatal owner is the sole explicit disposer.
            unsafe { ManuallyDrop::drop(&mut outcome) };
        } else {
            *state = previous;
        }
    }
}

impl<T> std::fmt::Debug for RetainedCompletion<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedCompletion")
            .finish_non_exhaustive()
    }
}

pub(crate) struct ReadyCompletionGuard<'a, T> {
    state: MutexGuard<'a, CompletionTransferState<T>>,
}

impl<T> ReadyCompletionGuard<'_, T> {
    pub(crate) fn outcome_mut(&mut self) -> &mut T {
        match &mut *self.state {
            CompletionTransferState::Ready(outcome) => outcome,
            _ => unreachable!("ready completion guard changed state"),
        }
    }

    pub(crate) fn acknowledge(&mut self) {
        let previous = std::mem::replace(&mut *self.state, CompletionTransferState::Acknowledged);
        if let CompletionTransferState::Ready(mut outcome) = previous {
            // SAFETY: acknowledgement is the single explicit release boundary.
            unsafe { ManuallyDrop::drop(&mut outcome) };
        } else {
            unreachable!("only a ready completion can be acknowledged");
        }
    }

    pub(crate) fn poison(&mut self) {
        let previous = std::mem::replace(&mut *self.state, CompletionTransferState::Acknowledged);
        if let CompletionTransferState::Ready(outcome) = previous {
            *self.state = CompletionTransferState::Poisoned(outcome);
        } else {
            unreachable!("only a ready completion can be poisoned");
        }
    }
}

pub(crate) struct DownloadVerificationCompletion {
    cell: Arc<RetainedCompletionCell<DownloadVerificationOutcome>>,
    destination: Weak<DownloadCompletionQueue>,
}

pub(crate) struct LifecycleVerificationCompletion {
    cell: Arc<RetainedCompletionCell<LifecycleVerificationOutcome>>,
    destination: Weak<LifecycleMailboxInner>,
}

impl DownloadVerificationCompletion {
    fn new(
        cell: Arc<RetainedCompletionCell<DownloadVerificationOutcome>>,
        destination: Weak<DownloadCompletionQueue>,
    ) -> Self {
        Self { cell, destination }
    }

    pub(crate) fn publish(
        self,
        outcome: DownloadVerificationOutcome,
    ) -> Result<(), RetainedCompletion<DownloadVerificationOutcome>> {
        publish_completion(
            self.cell,
            self.destination.upgrade(),
            outcome,
            |destination| destination.notify_ready(),
        )
    }
}

impl LifecycleVerificationCompletion {
    pub(super) fn new(
        cell: Arc<RetainedCompletionCell<LifecycleVerificationOutcome>>,
        destination: Weak<LifecycleMailboxInner>,
    ) -> Self {
        Self { cell, destination }
    }

    pub(crate) fn publish(
        self,
        outcome: LifecycleVerificationOutcome,
    ) -> Result<(), RetainedCompletion<LifecycleVerificationOutcome>> {
        publish_completion(
            self.cell,
            self.destination.upgrade(),
            outcome,
            |destination| destination.notify_verification_ready(),
        )
    }
}

fn publish_completion<T, D>(
    cell: Arc<RetainedCompletionCell<T>>,
    destination: Option<Arc<D>>,
    outcome: T,
    notify: impl FnOnce(&D) -> bool,
) -> Result<(), RetainedCompletion<T>> {
    {
        let mut state = cell
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(&*state, CompletionTransferState::Reserved) {
            return Err(poisoned_completion(outcome));
        }
        *state = CompletionTransferState::Ready(ManuallyDrop::new(outcome));
    }
    if destination.as_deref().is_some_and(notify) {
        return Ok(());
    }
    poison_ready_cell(&cell);
    Err(RetainedCompletion { cell })
}

fn poisoned_completion<T>(outcome: T) -> RetainedCompletion<T> {
    RetainedCompletion {
        cell: Arc::new(RetainedCompletionCell {
            state: Mutex::new(CompletionTransferState::Poisoned(ManuallyDrop::new(
                outcome,
            ))),
        }),
    }
}

fn poison_ready_cell<T>(cell: &Arc<RetainedCompletionCell<T>>) {
    let mut state = cell
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = std::mem::replace(&mut *state, CompletionTransferState::Acknowledged);
    if let CompletionTransferState::Ready(outcome) = previous {
        *state = CompletionTransferState::Poisoned(outcome);
    } else {
        *state = previous;
    }
}

pub(super) struct CompletionDestination<T> {
    capacity: usize,
    cells: Mutex<VecDeque<Arc<RetainedCompletionCell<T>>>>,
    changed: Condvar,
}

impl<T> CompletionDestination<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cells: Mutex::new(VecDeque::with_capacity(capacity)),
            changed: Condvar::new(),
        }
    }

    pub(super) fn reserve(&self) -> Option<Arc<RetainedCompletionCell<T>>> {
        let mut cells = self.cells.lock().ok()?;
        cells.retain(|cell| {
            !matches!(
                &*cell
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                CompletionTransferState::Acknowledged
            )
        });
        if cells.len() >= self.capacity {
            return None;
        }
        let cell = Arc::new(RetainedCompletionCell {
            state: Mutex::new(CompletionTransferState::Reserved),
        });
        cells.push_back(cell.clone());
        Some(cell)
    }

    pub(super) fn notify_ready(&self) -> bool {
        if self.cells.is_poisoned() {
            return false;
        }
        self.changed.notify_all();
        true
    }

    fn ready(&self) -> Option<RetainedCompletion<T>> {
        let cells = self.cells.lock().ok()?;
        cells.iter().find_map(|cell| {
            let ready = matches!(
                &*cell
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                CompletionTransferState::Ready(_)
            );
            ready.then(|| RetainedCompletion { cell: cell.clone() })
        })
    }

    #[cfg(test)]
    fn dispose_poisoned(&self) {
        let cells = self
            .cells
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for cell in cells.iter() {
            let mut state = cell
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = std::mem::replace(&mut *state, CompletionTransferState::Acknowledged);
            match previous {
                CompletionTransferState::Poisoned(mut outcome) => {
                    // SAFETY: this test-only fatal owner is the sole explicit disposer.
                    unsafe { ManuallyDrop::drop(&mut outcome) };
                }
                other => *state = other,
            }
        }
    }
}

pub(crate) struct DownloadCompletionQueue {
    completions: CompletionDestination<DownloadVerificationOutcome>,
}

impl DownloadCompletionQueue {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            completions: CompletionDestination::new(capacity),
        })
    }

    pub(crate) fn reserve(self: &Arc<Self>) -> Option<DownloadVerificationCompletion> {
        let cell = self.completions.reserve()?;
        Some(DownloadVerificationCompletion::new(
            cell,
            Arc::downgrade(self),
        ))
    }

    fn notify_ready(&self) -> bool {
        self.completions.notify_ready()
    }

    pub(crate) fn ready(&self) -> Option<RetainedCompletion<DownloadVerificationOutcome>> {
        self.completions.ready()
    }

    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(&self) {
        self.completions.dispose_poisoned();
    }
}

pub(crate) enum VerificationReserveOutcome {
    Reserved(VerificationAdmissionReservation),
    Backpressure,
    Stopping,
}

pub(crate) struct VerificationBindFailure {
    pub(crate) reservation: VerificationAdmissionReservation,
    pub(crate) input: StableVerificationInput,
    pub(crate) continuation: DownloadContinuation,
    pub(crate) completion: DownloadVerificationCompletion,
}

pub(crate) struct LifecycleVerificationBindFailure {
    pub(crate) reservation: VerificationAdmissionReservation,
    pub(crate) input: StableVerificationInput,
    pub(crate) continuation: LifecycleVerificationContinuation,
    pub(crate) completion: LifecycleVerificationCompletion,
}

impl VerificationAdmissionReservation {
    #[allow(clippy::result_large_err)]
    pub(crate) fn bind_download(
        mut self,
        input: StableVerificationInput,
        continuation: DownloadContinuation,
        completion: DownloadVerificationCompletion,
    ) -> Result<VerificationWaiter, VerificationBindFailure> {
        if !self.key.matches(&input) || self.class != VerificationClass::Download {
            return Err(VerificationBindFailure {
                reservation: self,
                input,
                continuation,
                completion,
            });
        }
        let Some(owner) = self.owner.upgrade() else {
            return Err(VerificationBindFailure {
                reservation: self,
                input,
                continuation,
                completion,
            });
        };
        let waiter_id =
            match owner.try_bind_download(self.key.clone(), input, continuation, completion) {
                Ok(waiter_id) => waiter_id,
                Err((input, continuation, completion)) => {
                    return Err(VerificationBindFailure {
                        reservation: self,
                        input,
                        continuation,
                        completion,
                    });
                }
            };
        self.state = VerificationReservationState::Bound;
        Ok(VerificationWaiter {
            key: self.key.clone(),
            waiter_id,
            owner: Arc::downgrade(&owner),
            released: false,
        })
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn bind_lifecycle(
        mut self,
        input: StableVerificationInput,
        continuation: LifecycleVerificationContinuation,
        completion: LifecycleVerificationCompletion,
    ) -> Result<VerificationWaiter, LifecycleVerificationBindFailure> {
        if !self.key.matches(&input) || self.class != VerificationClass::Lifecycle {
            return Err(LifecycleVerificationBindFailure {
                reservation: self,
                input,
                continuation,
                completion,
            });
        }
        let Some(owner) = self.owner.upgrade() else {
            return Err(LifecycleVerificationBindFailure {
                reservation: self,
                input,
                continuation,
                completion,
            });
        };
        let waiter_id =
            match owner.try_bind_lifecycle(self.key.clone(), input, continuation, completion) {
                Ok(waiter_id) => waiter_id,
                Err((input, continuation, completion)) => {
                    return Err(LifecycleVerificationBindFailure {
                        reservation: self,
                        input,
                        continuation,
                        completion,
                    });
                }
            };
        self.state = VerificationReservationState::Bound;
        Ok(VerificationWaiter {
            key: self.key.clone(),
            waiter_id,
            owner: Arc::downgrade(&owner),
            released: false,
        })
    }
}

impl Drop for VerificationAdmissionReservation {
    fn drop(&mut self) {
        if self.state != VerificationReservationState::Reserved {
            return;
        }
        if let Some(owner) = self.owner.upgrade() {
            owner.release_reservation(&self.key, self.class);
        }
    }
}

pub(crate) struct VerificationShared {
    state: Mutex<VerificationState>,
    changed: Condvar,
}

impl VerificationShared {
    #[allow(clippy::result_large_err)]
    fn try_bind_download(
        &self,
        key: VerificationKey,
        input: StableVerificationInput,
        continuation: DownloadContinuation,
        completion: DownloadVerificationCompletion,
    ) -> Result<
        u64,
        (
            StableVerificationInput,
            DownloadContinuation,
            DownloadVerificationCompletion,
        ),
    > {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                poisoned.into_inner().sealed = true;
                return Err((input, continuation, completion));
            }
        };
        let reservation = (key.clone(), VerificationClass::Download);
        if state.stopping
            || state.sealed
            || state
                .reservations
                .get(&reservation)
                .is_none_or(|count| *count == 0)
        {
            return Err((input, continuation, completion));
        }
        let (ownership, permit) = continuation.into_verification_parts();
        let waiter_id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.bound.insert(
            waiter_id,
            (
                key.clone(),
                BoundVerification::Download {
                    input,
                    ownership,
                    completion,
                },
            ),
        );
        state.consume_reservation(&key, VerificationClass::Download);
        permit.release();
        self.changed.notify_all();
        Ok(waiter_id)
    }

    #[allow(clippy::result_large_err)]
    fn try_bind_lifecycle(
        &self,
        key: VerificationKey,
        input: StableVerificationInput,
        continuation: LifecycleVerificationContinuation,
        completion: LifecycleVerificationCompletion,
    ) -> Result<
        u64,
        (
            StableVerificationInput,
            LifecycleVerificationContinuation,
            LifecycleVerificationCompletion,
        ),
    > {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                poisoned.into_inner().sealed = true;
                return Err((input, continuation, completion));
            }
        };
        let reservation = (key.clone(), VerificationClass::Lifecycle);
        if state.stopping
            || state.sealed
            || state
                .reservations
                .get(&reservation)
                .is_none_or(|count| *count == 0)
        {
            return Err((input, continuation, completion));
        }
        let waiter_id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.bound.insert(
            waiter_id,
            (
                key.clone(),
                BoundVerification::Lifecycle {
                    input,
                    continuation,
                    completion,
                },
            ),
        );
        state.consume_reservation(&key, VerificationClass::Lifecycle);
        self.changed.notify_all();
        Ok(waiter_id)
    }

    fn release_reservation(&self, key: &VerificationKey, class: VerificationClass) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                poisoned.into_inner().sealed = true;
                return;
            }
        };
        state.consume_reservation(key, class);
        self.changed.notify_all();
    }

    fn release_waiter(&self, key: &VerificationKey, waiter_id: u64) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                poisoned.into_inner().sealed = true;
                return;
            }
        };
        if state
            .bound
            .get(&waiter_id)
            .is_some_and(|(known, _)| known == key)
        {
            state.bound.remove(&waiter_id);
            self.changed.notify_all();
        }
    }
}

struct VerificationState {
    next_waiter_id: u64,
    reservations: HashMap<(VerificationKey, VerificationClass), usize>,
    bound: HashMap<u64, (VerificationKey, BoundVerification)>,
    stopping: bool,
    sealed: bool,
}

impl VerificationState {
    fn consume_reservation(&mut self, key: &VerificationKey, class: VerificationClass) {
        let entry = (key.clone(), class);
        if let Some(count) = self.reservations.get_mut(&entry) {
            *count -= 1;
            if *count == 0 {
                self.reservations.remove(&entry);
            }
        }
    }
}

enum BoundVerification {
    Download {
        input: StableVerificationInput,
        ownership: DownloadVerificationOwnership,
        completion: DownloadVerificationCompletion,
    },
    Lifecycle {
        input: StableVerificationInput,
        continuation: LifecycleVerificationContinuation,
        completion: LifecycleVerificationCompletion,
    },
}

pub(crate) struct VerificationSchedulerHandle {
    shared: Arc<VerificationShared>,
}

pub(crate) struct VerificationSchedulerOwner {
    handles: Vec<JoinHandle<()>>,
    completions: Vec<std::sync::mpsc::Receiver<VerificationWorkerExit>>,
    shared: Arc<VerificationShared>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationWorkerExit {
    Stopped,
    Panicked,
}
