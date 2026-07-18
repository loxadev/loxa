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

struct RetainedCompletionCell<T> {
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
            Some(ReadyCompletionGuard { state: Some(state) })
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(self) {
        let outcome = {
            let mut state = self
                .cell
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous = std::mem::replace(&mut *state, CompletionTransferState::Acknowledged);
            if let CompletionTransferState::Poisoned(outcome) = previous {
                Some(ManuallyDrop::into_inner(outcome))
            } else {
                *state = previous;
                None
            }
        };
        drop(outcome);
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
    state: Option<MutexGuard<'a, CompletionTransferState<T>>>,
}

impl<T> ReadyCompletionGuard<'_, T> {
    pub(crate) fn outcome_mut(&mut self) -> &mut T {
        match &mut **self.state.as_mut().expect("ready guard owns its lock") {
            CompletionTransferState::Ready(outcome) => outcome,
            _ => unreachable!("ready completion guard changed state"),
        }
    }

    pub(crate) fn acknowledge(mut self) {
        let mut state = self.state.take().expect("ready guard owns its lock");
        let previous = std::mem::replace(&mut *state, CompletionTransferState::Acknowledged);
        let outcome = if let CompletionTransferState::Ready(outcome) = previous {
            ManuallyDrop::into_inner(outcome)
        } else {
            unreachable!("only a ready completion can be acknowledged");
        };
        drop(state);
        drop(outcome);
    }

    pub(crate) fn poison(&mut self) {
        let state = self.state.as_mut().expect("ready guard owns its lock");
        let previous = std::mem::replace(&mut **state, CompletionTransferState::Acknowledged);
        if let CompletionTransferState::Ready(outcome) = previous {
            **state = CompletionTransferState::Poisoned(outcome);
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
            self.cell.clone(),
            self.destination.upgrade(),
            outcome,
            |destination| destination.notify_ready(),
        )
    }
}

impl LifecycleVerificationCompletion {
    pub(super) fn reserve(
        completions: &CompletionDestination<LifecycleVerificationOutcome>,
        destination: &Arc<LifecycleMailboxInner>,
    ) -> Option<Self> {
        Some(Self {
            cell: completions.reserve()?,
            destination: Arc::downgrade(destination),
        })
    }

    pub(super) fn rollback_from(
        &self,
        completions: &CompletionDestination<LifecycleVerificationOutcome>,
    ) {
        completions.remove_reserved(&self.cell);
    }

    pub(crate) fn publish(
        self,
        outcome: LifecycleVerificationOutcome,
    ) -> Result<(), RetainedCompletion<LifecycleVerificationOutcome>> {
        publish_completion(
            self.cell.clone(),
            self.destination.upgrade(),
            outcome,
            |destination| destination.notify_verification_ready(),
        )
    }
}

impl Drop for DownloadVerificationCompletion {
    fn drop(&mut self) {
        if rollback_reserved_cell(&self.cell) {
            if let Some(destination) = self.destination.upgrade() {
                destination.completions.remove_reserved(&self.cell);
            }
        }
    }
}

impl Drop for LifecycleVerificationCompletion {
    fn drop(&mut self) {
        if rollback_reserved_cell(&self.cell) {
            if let Some(destination) = self.destination.upgrade() {
                destination.rollback_verification(self);
            }
        }
    }
}

fn rollback_reserved_cell<T>(cell: &Arc<RetainedCompletionCell<T>>) -> bool {
    let Ok(mut state) = cell.state.lock() else {
        return false;
    };
    if matches!(&*state, CompletionTransferState::Reserved) {
        *state = CompletionTransferState::Acknowledged;
        true
    } else {
        false
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

    fn reserve(&self) -> Option<Arc<RetainedCompletionCell<T>>> {
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

    fn remove_reserved(&self, target: &Arc<RetainedCompletionCell<T>>) {
        let removed = {
            let mut cells = match self.cells.lock() {
                Ok(cells) => cells,
                Err(_) => return,
            };
            let Some(index) = cells.iter().position(|cell| Arc::ptr_eq(cell, target)) else {
                return;
            };
            cells.remove(index)
        };
        drop(removed);
        self.changed.notify_all();
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for cell in cells {
            RetainedCompletion { cell }.dispose_poisoned_for_test();
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

struct VerificationShared {
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
        drop(state);
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
        drop(state);
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
        drop(state);
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
        let removed = if state
            .bound
            .get(&waiter_id)
            .is_some_and(|(known, _)| known == key)
        {
            state.bound.remove(&waiter_id)
        } else {
            None
        };
        drop(state);
        if removed.is_some() {
            drop(removed);
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
    #[cfg(test)]
    DropProbe(VerificationLockDropProbe),
}

#[cfg(test)]
struct VerificationLockDropProbe {
    owner: Weak<VerificationShared>,
    observed_unlocked: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl Drop for VerificationLockDropProbe {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        let unlocked = self
            .owner
            .upgrade()
            .is_some_and(|owner| owner.state.try_lock().is_ok());
        self.observed_unlocked.store(unlocked, Ordering::SeqCst);
    }
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

#[cfg(test)]
mod lock_order_tests {
    use super::*;
    use crate::artifact_coordinator::{ArtifactKey, ArtifactMutationCoordinator};
    use crate::download_scheduler::DownloadContinuation;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "loxa-verification-lock-{label}-{}-{}",
                std::process::id(),
                OperationId::new_v4()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn shared() -> Arc<VerificationShared> {
        Arc::new(VerificationShared {
            state: Mutex::new(VerificationState {
                next_waiter_id: 1,
                reservations: HashMap::new(),
                bound: HashMap::new(),
                stopping: false,
                sealed: false,
            }),
            changed: Condvar::new(),
        })
    }

    #[test]
    fn acknowledgement_drops_payload_after_releasing_cell_lock() {
        struct Probe {
            cell: Weak<RetainedCompletionCell<Probe>>,
            observed_unlocked: Arc<AtomicBool>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let unlocked = self
                    .cell
                    .upgrade()
                    .is_some_and(|cell| cell.state.try_lock().is_ok());
                self.observed_unlocked.store(unlocked, Ordering::SeqCst);
            }
        }

        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let cell = Arc::new(RetainedCompletionCell {
            state: Mutex::new(CompletionTransferState::Reserved),
        });
        *cell.state.lock().unwrap() = CompletionTransferState::Ready(ManuallyDrop::new(Probe {
            cell: Arc::downgrade(&cell),
            observed_unlocked: Arc::clone(&observed_unlocked),
        }));
        RetainedCompletion { cell }
            .lock_ready()
            .unwrap()
            .acknowledge();

        assert!(observed_unlocked.load(Ordering::SeqCst));
    }

    #[test]
    fn waiter_release_drops_bound_ownership_after_releasing_scheduler_lock() {
        let owner = shared();
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let dir = TestDir::new("waiter");
        let path = dir.0.join("model.gguf");
        std::fs::write(&path, b"artifact").unwrap();
        let key = VerificationKey::new(
            StableVerificationInput::open(&path, [1; 32])
                .unwrap()
                .stable,
            [1; 32],
        );
        owner.state.lock().unwrap().bound.insert(
            7,
            (
                key.clone(),
                BoundVerification::DropProbe(VerificationLockDropProbe {
                    owner: Arc::downgrade(&owner),
                    observed_unlocked: Arc::clone(&observed_unlocked),
                }),
            ),
        );

        owner.release_waiter(&key, 7);

        assert!(observed_unlocked.load(Ordering::SeqCst));
    }

    #[test]
    fn download_bind_releases_worker_permit_after_releasing_scheduler_lock() {
        let dir = TestDir::new("bind");
        let path = dir.0.join("model.gguf");
        std::fs::write(&path, b"artifact").unwrap();
        let input = StableVerificationInput::open(&path, [2; 32]).unwrap();
        let key = VerificationKey::new(input.stable.clone(), [2; 32]);
        let owner = shared();
        owner
            .state
            .lock()
            .unwrap()
            .reservations
            .insert((key.clone(), VerificationClass::Download), 1);
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let weak_owner = Arc::downgrade(&owner);
        let observed = Arc::clone(&observed_unlocked);
        let artifact_key = ArtifactKey::from_destination(&path).unwrap();
        let artifact = ArtifactMutationCoordinator::new()
            .try_acquire_mutation(artifact_key)
            .unwrap();
        let continuation = DownloadContinuation::with_release_probe_for_test(
            OperationId::new_v4(),
            DecimalU64::new(1),
            OperationCancellation::new(),
            artifact,
            Box::new(move || {
                let unlocked = weak_owner
                    .upgrade()
                    .is_some_and(|owner| owner.state.try_lock().is_ok());
                observed.store(unlocked, Ordering::SeqCst);
            }),
        );
        let completion = DownloadCompletionQueue::new(1).reserve().unwrap();

        let waiter_id = match owner.try_bind_download(key.clone(), input, continuation, completion)
        {
            Ok(waiter_id) => waiter_id,
            Err(_) => panic!("valid download binding rejected"),
        };
        assert!(observed_unlocked.load(Ordering::SeqCst));
        owner.release_waiter(&key, waiter_id);
    }

    #[test]
    fn poisoned_reserved_completion_fails_closed_without_releasing_capacity() {
        let queue = DownloadCompletionQueue::new(1);
        let completion = queue.reserve().unwrap();
        let cell = completion.cell.clone();
        let _ = std::panic::catch_unwind(|| {
            let _guard = cell.state.lock().unwrap();
            panic!("poison reserved completion cell");
        });

        drop(completion);

        assert!(queue.reserve().is_none());
    }
}
