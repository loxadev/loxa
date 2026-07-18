use crate::artifact_coordinator::{ArtifactMutationLease, ArtifactReadLease};
use crate::download_scheduler::DownloadContinuation;
use crate::lifecycle_controller::LifecycleMailboxInner;
use crate::operation_cancellation::OperationCancellation;
use loxa_core::model_inventory::{
    verify_opened_artifact, StableVerificationIdentity, StableVerificationInput,
    VerificationCancellation, VerifiedArtifact,
};
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::collections::{HashMap, VecDeque};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::JoinHandle;
use std::time::Instant;

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
    #[cfg(test)]
    DifferentForTest,
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
        self.stable == input.stable
            && self.algorithm == VerificationAlgorithm::Sha256
            && self.expected_digest == input.expected_sha256
            && self.format_policy == VerificationFormatPolicy::CurrentRecipeV1
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

impl VerificationResult {
    fn clone_for_delivery(&self) -> Self {
        match self {
            Self::Verified(evidence) => Self::Verified(evidence.clone()),
            Self::Failed { kind, message } => Self::Failed {
                kind: *kind,
                message: message.clone(),
            },
            Self::Cancelled => Self::Cancelled,
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
        let state = match self.cell.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                poison_ready_state(&mut state);
                return None;
            }
        };
        if matches!(&*state, CompletionTransferState::Ready(_)) {
            Some(ReadyCompletionGuard { state: Some(state) })
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(self) {
        let outcome = {
            let mut state = match self.cell.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    poison_ready_state(&mut state);
                    state
                }
            };
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
        let mut state = match cell.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                poison_ready_state(&mut state);
                return Err(poisoned_completion(outcome));
            }
        };
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
    let mut state = match cell.state.lock() {
        Ok(state) => state,
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            poison_ready_state(&mut state);
            return;
        }
    };
    poison_ready_state(&mut state);
}

fn poison_ready_state<T>(state: &mut CompletionTransferState<T>) {
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
        cells.retain(|cell| match cell.state.lock() {
            Ok(state) => !matches!(&*state, CompletionTransferState::Acknowledged),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                poison_ready_state(&mut state);
                true
            }
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

    pub(super) fn ready(&self) -> Option<RetainedCompletion<T>> {
        let cells = self.cells.lock().ok()?;
        cells.iter().find_map(|cell| {
            let ready = match cell.state.lock() {
                Ok(state) => matches!(&*state, CompletionTransferState::Ready(_)),
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    poison_ready_state(&mut state);
                    false
                }
            };
            ready.then(|| RetainedCompletion { cell: cell.clone() })
        })
    }

    #[cfg(test)]
    pub(super) fn dispose_poisoned(&self) {
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
    #[cfg(test)]
    worker_hook: Option<VerificationWorkerHook>,
    #[cfg(test)]
    finish_hook: Option<VerificationWorkerHook>,
}

#[cfg(test)]
type VerificationWorkerHook = Arc<dyn Fn(&VerificationKey) + Send + Sync>;

#[derive(Clone)]
struct JobCancellation(Arc<AtomicBool>);

impl JobCancellation {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl VerificationCancellation for JobCancellation {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerificationJobState {
    Queued,
    Running,
}

struct VerificationJob {
    state: VerificationJobState,
    waiter_ids: Vec<u64>,
    cancellation: JobCancellation,
}

enum RetainedVerificationCompletion {
    Download(RetainedCompletion<DownloadVerificationOutcome>),
    Lifecycle(RetainedCompletion<LifecycleVerificationOutcome>),
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
        if state
            .jobs
            .get(&key)
            .is_some_and(|job| job.cancellation.is_cancelled())
        {
            return Err((input, continuation, completion));
        }
        let Some(waiter_id) = state.allocate_waiter_id() else {
            state.sealed = true;
            return Err((input, continuation, completion));
        };
        let (ownership, permit) = continuation.into_verification_parts();
        state.bound.insert(
            waiter_id,
            (
                key.clone(),
                BoundVerification::Download {
                    input: Some(input),
                    ownership,
                    completion,
                },
            ),
        );
        state
            .jobs
            .entry(key.clone())
            .or_insert_with(|| VerificationJob {
                state: VerificationJobState::Queued,
                waiter_ids: Vec::new(),
                cancellation: JobCancellation::new(),
            })
            .waiter_ids
            .push(waiter_id);
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
        if state
            .jobs
            .get(&key)
            .is_some_and(|job| job.cancellation.is_cancelled())
        {
            return Err((input, continuation, completion));
        }
        let Some(waiter_id) = state.allocate_waiter_id() else {
            state.sealed = true;
            return Err((input, continuation, completion));
        };
        state.bound.insert(
            waiter_id,
            (
                key.clone(),
                BoundVerification::Lifecycle {
                    input: Some(input),
                    continuation,
                    completion,
                },
            ),
        );
        state
            .jobs
            .entry(key.clone())
            .or_insert_with(|| VerificationJob {
                state: VerificationJobState::Queued,
                waiter_ids: Vec::new(),
                cancellation: JobCancellation::new(),
            })
            .waiter_ids
            .push(waiter_id);
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
        let empty_queued_job = if removed.is_some() {
            let mut remove_job = false;
            if let Some(job) = state.jobs.get_mut(key) {
                job.waiter_ids.retain(|known| *known != waiter_id);
                if job.waiter_ids.is_empty() {
                    job.cancellation.cancel();
                    remove_job = job.state == VerificationJobState::Queued;
                }
            }
            remove_job.then(|| state.jobs.remove(key)).flatten()
        } else {
            None
        };
        drop(state);
        if removed.is_some() {
            drop(removed);
            drop(empty_queued_job);
            self.changed.notify_all();
        }
    }

    fn stop(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                state.stopping = true;
                for job in state.jobs.values() {
                    job.cancellation.cancel();
                }
                drop(state);
                self.changed.notify_all();
                return;
            }
        };
        state.stopping = true;
        for job in state.jobs.values() {
            job.cancellation.cancel();
        }
        drop(state);
        self.changed.notify_all();
    }

    fn seal_after_worker_panic(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sealed = true;
        state.stopping = true;
        for job in state.jobs.values() {
            job.cancellation.cancel();
        }
        drop(state);
        self.changed.notify_all();
    }

    fn retain_completions(&self, retained: Vec<RetainedVerificationCompletion>) {
        if retained.is_empty() {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sealed = true;
        state.stopping = true;
        for job in state.jobs.values() {
            job.cancellation.cancel();
        }
        state.retained.extend(retained);
        drop(state);
        self.changed.notify_all();
    }
}

struct VerificationState {
    next_waiter_id: u64,
    reservations: HashMap<(VerificationKey, VerificationClass), usize>,
    bound: HashMap<u64, (VerificationKey, BoundVerification)>,
    jobs: HashMap<VerificationKey, VerificationJob>,
    retained: Vec<RetainedVerificationCompletion>,
    stopping: bool,
    sealed: bool,
}

impl VerificationState {
    fn allocate_waiter_id(&mut self) -> Option<u64> {
        let start = self.next_waiter_id;
        loop {
            let waiter_id = self.next_waiter_id;
            self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
            if !self.bound.contains_key(&waiter_id) {
                return Some(waiter_id);
            }
            if self.next_waiter_id == start {
                return None;
            }
        }
    }

    fn consume_reservation(&mut self, key: &VerificationKey, class: VerificationClass) {
        let entry = (key.clone(), class);
        if let Some(count) = self.reservations.get_mut(&entry) {
            *count -= 1;
            if *count == 0 {
                self.reservations.remove(&entry);
            }
        }
    }

    fn key_has_owner(&self, key: &VerificationKey, class: VerificationClass) -> bool {
        self.reservations
            .get(&(key.clone(), class))
            .is_some_and(|count| *count > 0)
            || self
                .bound
                .values()
                .any(|(known, bound)| known == key && bound.class() == class)
    }

    fn unique_download_keys(&self) -> usize {
        let mut keys = std::collections::HashSet::new();
        for ((key, class), count) in &self.reservations {
            if *class == VerificationClass::Download && *count > 0 {
                keys.insert(key.clone());
            }
        }
        for (key, bound) in self.bound.values() {
            if bound.class() == VerificationClass::Download {
                keys.insert(key.clone());
            }
        }
        keys.len()
    }

    fn lifecycle_owners(&self) -> usize {
        self.reservations
            .iter()
            .filter(|((_, class), _)| *class == VerificationClass::Lifecycle)
            .map(|(_, count)| *count)
            .sum::<usize>()
            + self
                .bound
                .values()
                .filter(|(_, bound)| bound.class() == VerificationClass::Lifecycle)
                .count()
    }

    fn download_key_capacity(&self) -> usize {
        let lifecycle_only_keys = self
            .reservations
            .keys()
            .map(|(key, _)| key)
            .chain(self.bound.values().map(|(key, _)| key))
            .filter(|key| {
                self.key_has_owner(key, VerificationClass::Lifecycle)
                    && !self.key_has_owner(key, VerificationClass::Download)
            })
            .collect::<std::collections::HashSet<_>>()
            .len();
        VERIFICATION_WORKERS + VERIFICATION_DOWNLOAD_WAITING - lifecycle_only_keys
    }
}

enum BoundVerification {
    Download {
        input: Option<StableVerificationInput>,
        ownership: DownloadVerificationOwnership,
        completion: DownloadVerificationCompletion,
    },
    Lifecycle {
        input: Option<StableVerificationInput>,
        continuation: LifecycleVerificationContinuation,
        completion: LifecycleVerificationCompletion,
    },
    #[cfg(test)]
    DropProbe(VerificationLockDropProbe),
}

impl BoundVerification {
    fn class(&self) -> VerificationClass {
        match self {
            Self::Download { .. } => VerificationClass::Download,
            Self::Lifecycle { .. } => VerificationClass::Lifecycle,
            #[cfg(test)]
            Self::DropProbe(_) => VerificationClass::Download,
        }
    }

    fn admission_order(&self) -> (DecimalU64, OperationId) {
        match self {
            Self::Download { ownership, .. } => {
                (ownership.admission_revision, ownership.operation_id)
            }
            Self::Lifecycle { continuation, .. } => {
                (continuation.admission_revision, continuation.operation_id)
            }
            #[cfg(test)]
            Self::DropProbe(_) => (DecimalU64::new(u64::MAX), OperationId::new_v4()),
        }
    }

    fn take_input(&mut self) -> Option<StableVerificationInput> {
        match self {
            Self::Download { input, .. } | Self::Lifecycle { input, .. } => input.take(),
            #[cfg(test)]
            Self::DropProbe(_) => None,
        }
    }
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

#[derive(Clone)]
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

struct VerificationWork {
    key: VerificationKey,
    input: StableVerificationInput,
    cancellation: JobCancellation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationShutdownReason {
    DeadlineExceeded,
    CompletionDisconnected,
    WorkerPanicked,
    WorkerJoinPanicked,
    RetainedOwnership,
}

#[must_use]
pub(crate) struct VerificationShutdownFailure {
    reason: VerificationShutdownReason,
    owner: ManuallyDrop<VerificationSchedulerOwner>,
}

impl std::fmt::Debug for VerificationShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerificationShutdownFailure")
            .field("reason", &self.reason)
            .field("retains_unjoined_workers", &self.retains_unjoined_workers())
            .finish()
    }
}

impl VerificationShutdownFailure {
    pub(crate) fn reason(&self) -> VerificationShutdownReason {
        self.reason
    }

    pub(crate) fn retains_unjoined_workers(&self) -> bool {
        !self.owner.handles.is_empty()
    }

    pub(crate) fn into_owner(self) -> VerificationSchedulerOwner {
        ManuallyDrop::into_inner(self.owner)
    }
}

impl VerificationSchedulerHandle {
    pub(crate) fn reserve(
        &self,
        key: VerificationKey,
        class: VerificationClass,
    ) -> VerificationReserveOutcome {
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                state.stopping = true;
                for job in state.jobs.values() {
                    job.cancellation.cancel();
                }
                drop(state);
                self.shared.changed.notify_all();
                return VerificationReserveOutcome::Stopping;
            }
        };
        if state.stopping || state.sealed {
            return VerificationReserveOutcome::Stopping;
        }
        if state
            .jobs
            .get(&key)
            .is_some_and(|job| job.cancellation.is_cancelled())
        {
            return VerificationReserveOutcome::Backpressure;
        }
        let has_download_owner = state.key_has_owner(&key, VerificationClass::Download);
        if class == VerificationClass::Lifecycle
            && state.lifecycle_owners() >= VERIFICATION_LIFECYCLE_WAITING
        {
            return VerificationReserveOutcome::Backpressure;
        }
        if class == VerificationClass::Download
            && !has_download_owner
            && state.unique_download_keys() >= state.download_key_capacity()
        {
            return VerificationReserveOutcome::Backpressure;
        }
        *state.reservations.entry((key.clone(), class)).or_insert(0) += 1;
        VerificationReserveOutcome::Reserved(VerificationAdmissionReservation {
            key,
            class,
            owner: Arc::downgrade(&self.shared),
            state: VerificationReservationState::Reserved,
        })
    }

    pub(crate) fn stop(&self) {
        self.shared.stop();
    }

    #[cfg(test)]
    fn counts_for_test(&self) -> VerificationCounts {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut counts = VerificationCounts {
            running: 0,
            download_queued: 0,
            lifecycle_queued: 0,
            jobs: state.jobs.len(),
            bound_waiters: state.bound.len(),
            retained_completions: state.retained.len(),
        };
        for job in state.jobs.values() {
            if job.state == VerificationJobState::Running {
                counts.running += 1;
            } else if job.waiter_ids.iter().any(|waiter_id| {
                state
                    .bound
                    .get(waiter_id)
                    .is_some_and(|(_, bound)| bound.class() == VerificationClass::Lifecycle)
            }) {
                counts.lifecycle_queued += 1;
            } else {
                counts.download_queued += 1;
            }
        }
        counts
    }

    #[cfg(test)]
    fn waiters_for_key_for_test(&self, key: &VerificationKey) -> usize {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .jobs
            .get(key)
            .map_or(0, |job| job.waiter_ids.len())
    }

    #[cfg(test)]
    fn job_cancellation_for_test(&self, key: &VerificationKey) -> Option<JobCancellation> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .jobs
            .get(key)
            .map(|job| job.cancellation.clone())
    }

    #[cfg(test)]
    fn job_cancellations_for_test(&self) -> Vec<JobCancellation> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .jobs
            .values()
            .map(|job| job.cancellation.clone())
            .collect()
    }

    #[cfg(test)]
    fn dispose_retained_for_test(&self) {
        let retained = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut state.retained)
        };
        dispose_retained_for_test(retained);
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VerificationCounts {
    running: usize,
    download_queued: usize,
    lifecycle_queued: usize,
    jobs: usize,
    bound_waiters: usize,
    retained_completions: usize,
}

impl VerificationSchedulerOwner {
    pub(crate) fn start(
    ) -> std::io::Result<(VerificationSchedulerHandle, VerificationSchedulerOwner)> {
        Self::start_inner(
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
    }

    #[cfg(test)]
    fn start_with_worker_hook_for_test(
        hook: impl Fn(&VerificationKey) + Send + Sync + 'static,
    ) -> (VerificationSchedulerHandle, VerificationSchedulerOwner) {
        Self::start_inner(Some(Arc::new(hook)), None).expect("verification test workers start")
    }

    #[cfg(test)]
    fn start_with_worker_and_finish_hooks_for_test(
        worker_hook: impl Fn(&VerificationKey) + Send + Sync + 'static,
        finish_hook: impl Fn(&VerificationKey) + Send + Sync + 'static,
    ) -> (VerificationSchedulerHandle, VerificationSchedulerOwner) {
        Self::start_inner(Some(Arc::new(worker_hook)), Some(Arc::new(finish_hook)))
            .expect("verification test workers start")
    }

    fn start_inner(
        #[cfg(test)] worker_hook: Option<VerificationWorkerHook>,
        #[cfg(test)] finish_hook: Option<VerificationWorkerHook>,
    ) -> std::io::Result<(VerificationSchedulerHandle, VerificationSchedulerOwner)> {
        let shared = Arc::new(VerificationShared {
            state: Mutex::new(VerificationState {
                next_waiter_id: 1,
                reservations: HashMap::new(),
                bound: HashMap::new(),
                jobs: HashMap::new(),
                retained: Vec::new(),
                stopping: false,
                sealed: false,
            }),
            changed: Condvar::new(),
            #[cfg(test)]
            worker_hook,
            #[cfg(test)]
            finish_hook,
        });
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(VERIFICATION_WORKERS);
        let mut completions = Vec::with_capacity(VERIFICATION_WORKERS);
        for worker_index in 0..VERIFICATION_WORKERS {
            let (completion_tx, completion_rx) = std::sync::mpsc::sync_channel(1);
            let worker_shared = shared.clone();
            let handle = match std::thread::Builder::new()
                .name(format!("loxa-verification-{worker_index}"))
                .spawn(move || {
                    let ran_cleanly =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            verification_worker_loop(&worker_shared)
                        }));
                    let exit = match ran_cleanly {
                        Ok(true) => VerificationWorkerExit::Stopped,
                        Ok(false) | Err(_) => {
                            worker_shared.seal_after_worker_panic();
                            VerificationWorkerExit::Panicked
                        }
                    };
                    let _ = completion_tx.send(exit);
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    shared.stop();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            };
            handles.push(handle);
            completions.push(completion_rx);
        }
        Ok((
            VerificationSchedulerHandle {
                shared: shared.clone(),
            },
            VerificationSchedulerOwner {
                handles,
                completions,
                shared,
            },
        ))
    }

    pub(crate) fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<Vec<VerificationWorkerExit>, VerificationShutdownFailure> {
        self.shared.stop();
        let mut exits = Vec::with_capacity(self.completions.len());
        let mut failure = None;
        while !self.completions.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.completions[0].recv_timeout(remaining) {
                Ok(exit) => {
                    if exit == VerificationWorkerExit::Panicked && failure.is_none() {
                        failure = Some(VerificationShutdownReason::WorkerPanicked);
                    }
                    exits.push(exit);
                    self.completions.remove(0);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if failure.is_none() {
                        failure = Some(VerificationShutdownReason::CompletionDisconnected);
                    }
                    self.completions.remove(0);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    return Err(VerificationShutdownFailure {
                        reason: VerificationShutdownReason::DeadlineExceeded,
                        owner: ManuallyDrop::new(self),
                    });
                }
            }
        }
        for handle in self.handles.drain(..) {
            if handle.join().is_err() && failure.is_none() {
                failure = Some(VerificationShutdownReason::WorkerJoinPanicked);
            }
        }
        let state_uncertain = match self.shared.state.lock() {
            Ok(state) => {
                state.sealed
                    || !state.jobs.is_empty()
                    || !state.bound.is_empty()
                    || !state.retained.is_empty()
            }
            Err(_) => true,
        };
        if state_uncertain && failure.is_none() {
            failure = Some(VerificationShutdownReason::RetainedOwnership);
        }
        if let Some(reason) = failure {
            Err(VerificationShutdownFailure {
                reason,
                owner: ManuallyDrop::new(self),
            })
        } else {
            Ok(exits)
        }
    }
}

impl Drop for VerificationSchedulerOwner {
    fn drop(&mut self) {
        if !self.handles.is_empty() || !self.completions.is_empty() {
            std::process::abort();
        }
        let retain = match self.shared.state.lock() {
            Ok(state) => state.sealed || !state.bound.is_empty() || !state.retained.is_empty(),
            Err(_) => true,
        };
        if retain {
            std::mem::forget(self.shared.clone());
        }
    }
}

#[cfg(test)]
impl VerificationShutdownFailure {
    fn dispose_for_test(self) {
        let owner = self.into_owner();
        assert!(owner.handles.is_empty(), "fatal test owner must be joined");
        let shared = owner.shared.clone();
        let (bound, retained) = {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.jobs.clear();
            state.reservations.clear();
            let bound = std::mem::take(&mut state.bound);
            let retained = std::mem::take(&mut state.retained);
            state.sealed = false;
            (bound, retained)
        };
        drop(bound);
        dispose_retained_for_test(retained);
        drop(owner);
        drop(shared);
    }
}

#[cfg(test)]
fn dispose_retained_for_test(retained: Vec<RetainedVerificationCompletion>) {
    for completion in retained {
        match completion {
            RetainedVerificationCompletion::Download(completion) => {
                completion.dispose_poisoned_for_test();
            }
            RetainedVerificationCompletion::Lifecycle(completion) => {
                completion.dispose_poisoned_for_test();
            }
        }
    }
}

fn verification_worker_loop(shared: &Arc<VerificationShared>) -> bool {
    let mut claimed = None;
    loop {
        let work = match claimed.take() {
            Some(work) => work,
            None => match take_next_work(shared) {
                Ok(Some(work)) => work,
                Ok(None) => return true,
                Err(()) => return false,
            },
        };
        #[cfg(test)]
        if let Some(hook) = &shared.worker_hook {
            hook(&work.key);
        }
        let result =
            VerificationResult::from(verify_opened_artifact(work.input, &work.cancellation));
        claimed = match finish_work(shared, &work.key, result) {
            Ok(next) => next,
            Err(()) => return false,
        };
        #[cfg(test)]
        if let Some(hook) = &shared.finish_hook {
            hook(&work.key);
        }
    }
}

fn take_next_work(shared: &VerificationShared) -> Result<Option<VerificationWork>, ()> {
    let mut state = match shared.state.lock() {
        Ok(state) => state,
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            state.sealed = true;
            state.stopping = true;
            for job in state.jobs.values() {
                job.cancellation.cancel();
            }
            return Err(());
        }
    };
    loop {
        match state.claim_next_work() {
            Ok(Some(work)) => return Ok(Some(work)),
            Ok(None) => {}
            Err(()) => return Err(()),
        }
        if state.stopping {
            return Ok(None);
        }
        state = match shared.changed.wait(state) {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                state.stopping = true;
                for job in state.jobs.values() {
                    job.cancellation.cancel();
                }
                return Err(());
            }
        };
    }
}

fn finish_work(
    shared: &VerificationShared,
    key: &VerificationKey,
    result: VerificationResult,
) -> Result<Option<VerificationWork>, ()> {
    let (completed, claimed) = {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                state.stopping = true;
                for job in state.jobs.values() {
                    job.cancellation.cancel();
                }
                return Err(());
            }
        };
        let Some(job) = state.jobs.remove(key) else {
            return Ok(None);
        };
        let mut completed = Vec::with_capacity(job.waiter_ids.len());
        for waiter_id in job.waiter_ids {
            if let Some((known_key, bound)) = state.bound.remove(&waiter_id) {
                if known_key == *key {
                    completed.push(bound);
                } else {
                    state.bound.insert(waiter_id, (known_key, bound));
                }
            }
        }
        let claimed = state.claim_next_work();
        (completed, claimed)
    };
    shared.changed.notify_all();

    let mut retained = Vec::new();
    for bound in completed {
        match bound {
            BoundVerification::Download {
                input,
                ownership,
                completion,
            } => {
                drop(input);
                let outcome = DownloadVerificationOutcome {
                    ownership,
                    result: result.clone_for_delivery(),
                };
                if let Err(completion) = completion.publish(outcome) {
                    retained.push(RetainedVerificationCompletion::Download(completion));
                }
            }
            BoundVerification::Lifecycle {
                input,
                continuation,
                completion,
            } => {
                drop(input);
                let outcome = LifecycleVerificationOutcome {
                    ownership: continuation,
                    result: result.clone_for_delivery(),
                };
                if let Err(completion) = completion.publish(outcome) {
                    retained.push(RetainedVerificationCompletion::Lifecycle(completion));
                }
            }
            #[cfg(test)]
            BoundVerification::DropProbe(probe) => drop(probe),
        }
    }
    shared.retain_completions(retained);
    claimed
}

impl VerificationState {
    fn claim_next_work(&mut self) -> Result<Option<VerificationWork>, ()> {
        let Some((key, waiter_id)) = self.next_queued_job() else {
            return Ok(None);
        };
        let input = self
            .bound
            .get_mut(&waiter_id)
            .and_then(|(_, bound)| bound.take_input());
        let Some(input) = input else {
            self.sealed = true;
            self.stopping = true;
            for job in self.jobs.values() {
                job.cancellation.cancel();
            }
            return Err(());
        };
        let job = self.jobs.get_mut(&key).expect("queued job exists");
        job.state = VerificationJobState::Running;
        Ok(Some(VerificationWork {
            key,
            input,
            cancellation: job.cancellation.clone(),
        }))
    }

    fn next_queued_job(&self) -> Option<(VerificationKey, u64)> {
        self.jobs
            .iter()
            .filter(|(_, job)| {
                job.state == VerificationJobState::Queued && !job.waiter_ids.is_empty()
            })
            .filter_map(|(key, job)| {
                let lifecycle = job
                    .waiter_ids
                    .iter()
                    .filter_map(|waiter_id| {
                        let (_, bound) = self.bound.get(waiter_id)?;
                        (bound.class() == VerificationClass::Lifecycle)
                            .then(|| (bound.admission_order(), *waiter_id))
                    })
                    .min_by_key(|(order, _)| *order);
                let class_priority = usize::from(lifecycle.is_none());
                let selected = lifecycle.or_else(|| {
                    job.waiter_ids
                        .iter()
                        .filter_map(|waiter_id| {
                            let (_, bound) = self.bound.get(waiter_id)?;
                            Some((bound.admission_order(), *waiter_id))
                        })
                        .min_by_key(|(order, _)| *order)
                })?;
                Some((class_priority, selected.0, selected.1, key.clone()))
            })
            .min_by_key(|(class_priority, order, waiter_id, _)| {
                (*class_priority, *order, *waiter_id)
            })
            .map(|(_, _, waiter_id, key)| (key, waiter_id))
    }
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
                jobs: HashMap::new(),
                retained: Vec::new(),
                stopping: false,
                sealed: false,
            }),
            changed: Condvar::new(),
            worker_hook: None,
            finish_hook: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_coordinator::{
        ArtifactAcquireError, ArtifactKey, ArtifactMutationCoordinator,
    };
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "loxa-verification-scheduler-{label}-{}-{}",
                std::process::id(),
                OperationId::new_v4()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct WorkerGate {
        state: Mutex<WorkerGateState>,
        changed: Condvar,
    }

    #[derive(Default)]
    struct WorkerGateState {
        started: Vec<VerificationKey>,
        permits: usize,
    }

    impl WorkerGate {
        fn enter(&self, key: &VerificationKey) {
            let mut state = self.state.lock().unwrap();
            state.started.push(key.clone());
            self.changed.notify_all();
            while state.permits == 0 {
                state = self.changed.wait(state).unwrap();
            }
            state.permits -= 1;
        }

        fn wait_started(&self, count: usize) -> Vec<VerificationKey> {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut state = self.state.lock().unwrap();
            while state.started.len() < count {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "workers did not reach test gate");
                let (next, timeout) = self.changed.wait_timeout(state, remaining).unwrap();
                state = next;
                assert!(!timeout.timed_out(), "workers did not reach test gate");
            }
            state.started.clone()
        }

        fn release(&self, count: usize) {
            let mut state = self.state.lock().unwrap();
            state.permits += count;
            drop(state);
            self.changed.notify_all();
        }
    }

    struct DownloadFixture {
        waiter: Option<VerificationWaiter>,
        queue: Arc<DownloadCompletionQueue>,
        coordinator: ArtifactMutationCoordinator,
        artifact_key: ArtifactKey,
        operation_id: OperationId,
        cancellation: OperationCancellation,
    }

    struct LifecycleFixture {
        waiter: Option<VerificationWaiter>,
        cell: Arc<RetainedCompletionCell<LifecycleVerificationOutcome>>,
        _mailbox: Arc<LifecycleMailboxInner>,
        _coordinator: ArtifactMutationCoordinator,
    }

    fn input(path: &Path, expected: [u8; 32]) -> (VerificationKey, StableVerificationInput) {
        let input = StableVerificationInput::open(path, expected).unwrap();
        let key = VerificationKey::new(input.stable.clone(), expected);
        (key, input)
    }

    fn submit_download(
        handle: &VerificationSchedulerHandle,
        path: &Path,
        expected: [u8; 32],
        revision: u64,
    ) -> (VerificationKey, DownloadFixture) {
        let (key, input) = input(path, expected);
        let reservation = match handle.reserve(key.clone(), VerificationClass::Download) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("download verification reservation rejected"),
        };
        let coordinator = ArtifactMutationCoordinator::new();
        let artifact_key = ArtifactKey::from_destination(path).unwrap();
        let artifact = coordinator
            .try_acquire_mutation(artifact_key.clone())
            .unwrap();
        let operation_id = OperationId::new_v4();
        let cancellation = OperationCancellation::new();
        let continuation = DownloadContinuation::with_release_probe_for_test(
            operation_id,
            DecimalU64::new(revision),
            cancellation.clone(),
            artifact,
            Box::new(|| {}),
        );
        let queue = DownloadCompletionQueue::new(1);
        let completion = queue.reserve().unwrap();
        let waiter = reservation
            .bind_download(input, continuation, completion)
            .unwrap_or_else(|_| panic!("valid download verification bind rejected"));
        (
            key,
            DownloadFixture {
                waiter: Some(waiter),
                queue,
                coordinator,
                artifact_key,
                operation_id,
                cancellation,
            },
        )
    }

    fn submit_lifecycle(
        handle: &VerificationSchedulerHandle,
        path: &Path,
        expected: [u8; 32],
        revision: u64,
    ) -> (VerificationKey, LifecycleFixture) {
        let (key, input) = input(path, expected);
        let reservation = match handle.reserve(key.clone(), VerificationClass::Lifecycle) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("lifecycle verification reservation rejected"),
        };
        let coordinator = ArtifactMutationCoordinator::new();
        let artifact_key = ArtifactKey::from_destination(path).unwrap();
        let artifact = coordinator.try_acquire_read(artifact_key).unwrap();
        let continuation = LifecycleVerificationContinuation {
            operation_id: OperationId::new_v4(),
            admission_revision: DecimalU64::new(revision),
            cancellation: OperationCancellation::new(),
            artifact,
        };
        let mailbox = LifecycleMailboxInner::new(1);
        let completion = mailbox.reserve_verification().unwrap();
        let cell = completion.cell.clone();
        let waiter = reservation
            .bind_lifecycle(input, continuation, completion)
            .unwrap_or_else(|_| panic!("valid lifecycle verification bind rejected"));
        (
            key,
            LifecycleFixture {
                waiter: Some(waiter),
                cell,
                _mailbox: mailbox,
                _coordinator: coordinator,
            },
        )
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !predicate() {
            assert!(Instant::now() < deadline, "condition did not become true");
            std::thread::yield_now();
        }
    }

    fn shutdown(
        owner: VerificationSchedulerOwner,
    ) -> Result<Vec<VerificationWorkerExit>, VerificationShutdownFailure> {
        owner.shutdown(Instant::now() + Duration::from_secs(5))
    }

    fn acknowledge_download(fixture: &DownloadFixture) -> VerificationResult {
        let retained = loop {
            if let Some(retained) = fixture.queue.ready() {
                break retained;
            }
            std::thread::yield_now();
        };
        let mut ready = retained.lock_ready().unwrap();
        let result = ready.outcome_mut().result.clone_for_delivery();
        ready.acknowledge();
        result
    }

    fn acknowledge_lifecycle(fixture: &LifecycleFixture) -> VerificationResult {
        let retained = RetainedCompletion {
            cell: fixture.cell.clone(),
        };
        let mut ready = loop {
            if let Some(ready) = retained.lock_ready() {
                break ready;
            }
            std::thread::yield_now();
        };
        let result = ready.outcome_mut().result.clone_for_delivery();
        ready.acknowledge();
        result
    }

    #[test]
    fn exact_capacity_is_two_running_seven_general_and_one_lifecycle_reserved() {
        let dir = TestDir::new("capacity");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let mut downloads = Vec::new();
        for index in 0..VERIFICATION_WORKERS {
            let path = dir.file(&format!("running-{index}.gguf"), b"running");
            downloads.push(submit_download(&handle, &path, [index as u8; 32], index as u64 + 1).1);
        }
        gate.wait_started(VERIFICATION_WORKERS);
        for index in 0..VERIFICATION_DOWNLOAD_WAITING {
            let path = dir.file(&format!("queued-{index}.gguf"), b"queued");
            downloads
                .push(submit_download(&handle, &path, [index as u8 + 8; 32], index as u64 + 10).1);
        }
        let lifecycle_path = dir.file("lifecycle.gguf", b"lifecycle");
        let lifecycle = submit_lifecycle(&handle, &lifecycle_path, [31; 32], 100).1;

        let counts = handle.counts_for_test();
        assert_eq!(counts.running, VERIFICATION_WORKERS);
        assert_eq!(counts.download_queued, VERIFICATION_DOWNLOAD_WAITING);
        assert_eq!(counts.lifecycle_queued, VERIFICATION_LIFECYCLE_WAITING);
        assert_eq!(counts.bound_waiters, 10);

        let extra_path = dir.file("extra.gguf", b"extra");
        let (extra_key, _) = input(&extra_path, [90; 32]);
        assert!(matches!(
            handle.reserve(extra_key, VerificationClass::Download),
            VerificationReserveOutcome::Backpressure
        ));
        let second_lifecycle_path = dir.file("second-lifecycle.gguf", b"lifecycle-2");
        let (second_lifecycle_key, _) = input(&second_lifecycle_path, [91; 32]);
        assert!(matches!(
            handle.reserve(second_lifecycle_key, VerificationClass::Lifecycle),
            VerificationReserveOutcome::Backpressure
        ));
        assert_eq!(handle.counts_for_test(), counts);

        gate.release(32);
        assert!(shutdown(owner).is_ok());
        for fixture in &downloads {
            acknowledge_download(fixture);
        }
        acknowledge_lifecycle(&lifecycle);
    }

    #[test]
    fn pending_lifecycle_reservation_holds_general_capacity_before_bind() {
        let dir = TestDir::new("pending-lifecycle-capacity");
        let (handle, owner) = VerificationSchedulerOwner::start().unwrap();
        let lifecycle_path = dir.file("lifecycle.gguf", b"lifecycle");
        let (lifecycle_key, _) = input(&lifecycle_path, [1; 32]);
        let lifecycle = match handle.reserve(lifecycle_key, VerificationClass::Lifecycle) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("lifecycle reservation rejected"),
        };

        let mut downloads = Vec::new();
        for index in 0..(VERIFICATION_WORKERS + VERIFICATION_DOWNLOAD_WAITING - 1) {
            let path = dir.file(&format!("download-{index}.gguf"), b"download");
            let (key, _) = input(&path, [index as u8 + 2; 32]);
            downloads.push(match handle.reserve(key, VerificationClass::Download) {
                VerificationReserveOutcome::Reserved(reservation) => reservation,
                _ => panic!("download reservation {index} rejected"),
            });
        }
        let extra = dir.file("extra.gguf", b"extra");
        let (extra_key, _) = input(&extra, [99; 32]);
        assert!(matches!(
            handle.reserve(extra_key, VerificationClass::Download),
            VerificationReserveOutcome::Backpressure
        ));

        drop(downloads);
        drop(lifecycle);
        assert!(shutdown(owner).is_ok());
    }

    #[test]
    fn download_attaching_to_lifecycle_key_obeys_capacity_and_survives_lifecycle_cancel() {
        let dir = TestDir::new("mixed-key-capacity");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let path = dir.file("mixed.gguf", b"mixed");
        let (mixed_key, mut lifecycle) = submit_lifecycle(&handle, &path, [1; 32], 1);
        gate.wait_started(1);

        let mut downloads = Vec::new();
        for index in 0..(VERIFICATION_WORKERS + VERIFICATION_DOWNLOAD_WAITING - 1) {
            downloads.push(
                submit_download(
                    &handle,
                    &dir.file(&format!("download-{index}.gguf"), b"download"),
                    [index as u8 + 2; 32],
                    index as u64 + 2,
                )
                .1,
            );
            if index == 0 {
                gate.wait_started(2);
            }
        }
        let attachment_was_backpressured = matches!(
            handle.reserve(mixed_key.clone(), VerificationClass::Download),
            VerificationReserveOutcome::Backpressure
        );

        drop(downloads.last_mut().unwrap().waiter.take());
        let mixed_download = submit_download(&handle, &path, [1; 32], 100).1;
        drop(lifecycle.waiter.take());
        assert_eq!(handle.waiters_for_key_for_test(&mixed_key), 1);

        let replacement = submit_download(
            &handle,
            &dir.file("replacement.gguf", b"replacement"),
            [90; 32],
            101,
        )
        .1;
        let extra = dir.file("extra.gguf", b"extra");
        let (extra_key, _) = input(&extra, [91; 32]);
        assert!(matches!(
            handle.reserve(extra_key, VerificationClass::Download),
            VerificationReserveOutcome::Backpressure
        ));

        gate.release(64);
        assert!(shutdown(owner).is_ok());
        for fixture in downloads.iter().take(downloads.len() - 1) {
            acknowledge_download(fixture);
        }
        acknowledge_download(&mixed_download);
        acknowledge_download(&replacement);
        assert!(attachment_was_backpressured);
    }

    #[test]
    fn completion_promotes_queued_work_before_reserve_observes_the_free_slot() {
        let dir = TestDir::new("finish-promote-reserve");
        let worker_gate = Arc::new(WorkerGate::default());
        let finish_gate = Arc::new(WorkerGate::default());
        let (handle, owner) =
            VerificationSchedulerOwner::start_with_worker_and_finish_hooks_for_test(
                {
                    let worker_gate = worker_gate.clone();
                    move |key| worker_gate.enter(key)
                },
                {
                    let finish_gate = finish_gate.clone();
                    move |key| finish_gate.enter(key)
                },
            );
        let mut downloads = Vec::new();
        for index in 0..(VERIFICATION_WORKERS + VERIFICATION_DOWNLOAD_WAITING) {
            downloads.push(
                submit_download(
                    &handle,
                    &dir.file(&format!("download-{index}.gguf"), b"download"),
                    [index as u8 + 1; 32],
                    index as u64 + 1,
                )
                .1,
            );
            if index + 1 == VERIFICATION_WORKERS {
                worker_gate.wait_started(VERIFICATION_WORKERS);
            }
        }

        worker_gate.release(1);
        finish_gate.wait_started(1);
        let extra = submit_download(&handle, &dir.file("extra.gguf", b"extra"), [99; 32], 100).1;
        let counts = handle.counts_for_test();

        finish_gate.release(64);
        worker_gate.release(64);
        assert!(shutdown(owner).is_ok());
        for fixture in &downloads {
            acknowledge_download(fixture);
        }
        acknowledge_download(&extra);
        assert_eq!(counts.running, VERIFICATION_WORKERS);
        assert_eq!(counts.download_queued, VERIFICATION_DOWNLOAD_WAITING);
    }

    #[test]
    fn running_lifecycle_keeps_the_eighth_waiting_position_out_of_general_use() {
        let dir = TestDir::new("running-lifecycle-capacity");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let lifecycle = submit_lifecycle(
            &handle,
            &dir.file("lifecycle.gguf", b"lifecycle"),
            [1; 32],
            1,
        )
        .1;
        gate.wait_started(1);

        let mut downloads = Vec::new();
        for index in 0..(1 + VERIFICATION_DOWNLOAD_WAITING) {
            downloads.push(
                submit_download(
                    &handle,
                    &dir.file(&format!("download-{index}.gguf"), b"download"),
                    [index as u8 + 2; 32],
                    index as u64 + 2,
                )
                .1,
            );
            if index == 0 {
                gate.wait_started(2);
            }
        }
        let extra = dir.file("extra-download.gguf", b"download");
        let (extra_key, _) = input(&extra, [99; 32]);
        let backpressured = matches!(
            handle.reserve(extra_key, VerificationClass::Download),
            VerificationReserveOutcome::Backpressure
        );
        let counts = handle.counts_for_test();

        gate.release(32);
        assert!(shutdown(owner).is_ok());
        acknowledge_lifecycle(&lifecycle);
        for fixture in &downloads {
            acknowledge_download(fixture);
        }
        assert!(backpressured);
        assert_eq!(counts.running, 2);
        assert_eq!(counts.download_queued, VERIFICATION_DOWNLOAD_WAITING);
        assert_eq!(counts.lifecycle_queued, 0);
    }

    #[test]
    fn backpressure_allocates_no_extra_blocked_download_waiter_population() {
        let dir = TestDir::new("backpressure");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let mut fixtures = Vec::new();
        for index in 0..(VERIFICATION_WORKERS + VERIFICATION_DOWNLOAD_WAITING) {
            let path = dir.file(&format!("model-{index}.gguf"), b"artifact");
            fixtures.push(submit_download(&handle, &path, [index as u8; 32], index as u64 + 1).1);
            if index + 1 == VERIFICATION_WORKERS {
                gate.wait_started(VERIFICATION_WORKERS);
            }
        }
        let before = handle.counts_for_test();
        for index in 0..=crate::download_scheduler::DOWNLOAD_WORKERS {
            let path = dir.file(&format!("blocked-{index}.gguf"), b"blocked");
            let (key, _) = input(&path, [200 + index as u8; 32]);
            assert!(matches!(
                handle.reserve(key, VerificationClass::Download),
                VerificationReserveOutcome::Backpressure
            ));
            assert_eq!(handle.counts_for_test(), before);
        }
        assert_eq!(crate::download_scheduler::DOWNLOAD_WORKERS, 2);

        gate.release(32);
        assert!(shutdown(owner).is_ok());
        for fixture in &fixtures {
            acknowledge_download(fixture);
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn strong_opened_identity_distinguishes_equal_length_and_mtime_files() {
        let dir = TestDir::new("strong-identity");
        let first = dir.file("first.gguf", b"same-bytes");
        let second = dir.file("second.gguf", b"same-bytes");
        let modified = std::fs::metadata(&first).unwrap().modified().unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&second)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        let (first_key, _) = input(&first, [1; 32]);
        let (second_key, _) = input(&second, [1; 32]);
        assert_ne!(first_key, second_key);

        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let first_fixture = submit_download(&handle, &first, [1; 32], 1).1;
        let second_fixture = submit_download(&handle, &second, [1; 32], 2).1;
        let started = gate.wait_started(2);
        assert_eq!(started.len(), 2);
        assert_ne!(started[0], started[1]);
        gate.release(2);
        assert!(shutdown(owner).is_ok());
        acknowledge_download(&first_fixture);
        acknowledge_download(&second_fixture);
    }

    #[cfg(not(any(unix, windows)))]
    #[test]
    fn unsupported_platform_identity_fails_closed_before_admission() {
        let dir = TestDir::new("unsupported-identity");
        let path = dir.file("model.gguf", b"artifact");
        let error = StableVerificationInput::open(&path, [1; 32]).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }

    #[test]
    fn identical_complete_key_single_flights_but_digest_and_policy_conflicts_do_not() {
        let dir = TestDir::new("single-flight");
        let path = dir.file("model.gguf", b"artifact");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let (key, leader) = submit_download(&handle, &path, [1; 32], 1);
        gate.wait_started(1);
        let (_, follower) = submit_download(&handle, &path, [1; 32], 2);
        assert_eq!(handle.counts_for_test().jobs, 1);
        assert_eq!(handle.waiters_for_key_for_test(&key), 2);
        assert_eq!(gate.wait_started(1).len(), 1);

        let (digest_conflict, digest_fixture) = submit_download(&handle, &path, [2; 32], 3);
        assert_ne!(key, digest_conflict);
        gate.wait_started(2);

        let mut policy_conflict = key.clone();
        policy_conflict.format_policy = VerificationFormatPolicy::DifferentForTest;
        let (policy_input_key, policy_input) = input(&path, [1; 32]);
        assert_eq!(policy_input_key, key);
        let reservation = match handle.reserve(policy_conflict, VerificationClass::Download) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("conflicting policy needs an independent reservation"),
        };
        let coordinator = ArtifactMutationCoordinator::new();
        let artifact = coordinator
            .try_acquire_mutation(ArtifactKey::from_destination(&path).unwrap())
            .unwrap();
        let continuation = DownloadContinuation::with_release_probe_for_test(
            OperationId::new_v4(),
            DecimalU64::new(4),
            OperationCancellation::new(),
            artifact,
            Box::new(|| {}),
        );
        let queue = DownloadCompletionQueue::new(1);
        let completion = queue.reserve().unwrap();
        assert!(reservation
            .bind_download(policy_input, continuation, completion)
            .is_err());

        gate.release(8);
        assert!(shutdown(owner).is_ok());
        acknowledge_download(&leader);
        acknowledge_download(&follower);
        acknowledge_download(&digest_fixture);
    }

    #[cfg(unix)]
    #[test]
    fn path_swap_fails_closed_and_does_not_join_the_replacement_identity() {
        let dir = TestDir::new("path-swap");
        let target = dir.file("model.gguf", b"original");
        let replacement = dir.file("replacement.gguf", b"different");
        let expected = [
            0x06, 0x82, 0xc5, 0xf2, 0x07, 0x6f, 0x09, 0x9c, 0x34, 0xcf, 0xdd, 0x15, 0xa9, 0xe0,
            0x63, 0x84, 0x9e, 0xd4, 0x37, 0xa4, 0x96, 0x77, 0xe6, 0xfc, 0xc5, 0xb4, 0x19, 0x8c,
            0x76, 0x57, 0x5b, 0xe5,
        ];
        let original_input = StableVerificationInput::open(&target, expected).unwrap();
        let original_key = VerificationKey::new(original_input.stable.clone(), expected);
        let moved = dir.0.join("original.gguf");
        std::fs::rename(&target, &moved).unwrap();
        std::fs::rename(&replacement, &target).unwrap();
        let replacement_input = StableVerificationInput::open(&target, expected).unwrap();
        let replacement_key = VerificationKey::new(replacement_input.stable.clone(), expected);
        assert_ne!(original_key, replacement_key);

        let (handle, owner) = VerificationSchedulerOwner::start().unwrap();
        let reservation = match handle.reserve(original_key, VerificationClass::Download) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("original opened identity rejected"),
        };
        let coordinator = ArtifactMutationCoordinator::new();
        let artifact_key = ArtifactKey::from_destination(&target).unwrap();
        let artifact = coordinator
            .try_acquire_mutation(artifact_key.clone())
            .unwrap();
        let continuation = DownloadContinuation::with_release_probe_for_test(
            OperationId::new_v4(),
            DecimalU64::new(1),
            OperationCancellation::new(),
            artifact,
            Box::new(|| {}),
        );
        let queue = DownloadCompletionQueue::new(1);
        let completion = queue.reserve().unwrap();
        let _waiter = reservation
            .bind_download(original_input, continuation, completion)
            .unwrap_or_else(|_| panic!("opened original bind rejected"));
        wait_until(|| queue.ready().is_some());
        let retained = queue.ready().unwrap();
        let mut ready = retained.lock_ready().unwrap();
        assert!(matches!(
            &ready.outcome_mut().result,
            VerificationResult::Failed {
                kind: std::io::ErrorKind::InvalidData,
                ..
            }
        ));
        ready.acknowledge();
        assert!(coordinator.try_acquire_mutation(artifact_key).is_ok());
        assert!(shutdown(owner).is_ok());
    }

    #[test]
    fn follower_cancel_preserves_job_and_last_waiter_cancel_stops_hashing() {
        let dir = TestDir::new("waiter-cancel");
        let path = dir.file("model.gguf", b"artifact");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let (key, mut leader) = submit_download(&handle, &path, [1; 32], 1);
        gate.wait_started(1);
        let (_, mut follower) = submit_download(&handle, &path, [1; 32], 2);
        let cancellation = handle.job_cancellation_for_test(&key).unwrap();

        drop(follower.waiter.take());
        assert!(!cancellation.is_cancelled());
        assert_eq!(handle.waiters_for_key_for_test(&key), 1);
        drop(leader.waiter.take());
        assert!(cancellation.is_cancelled());
        assert_eq!(handle.waiters_for_key_for_test(&key), 0);

        gate.release(1);
        assert!(shutdown(owner).is_ok());
        assert!(leader.queue.ready().is_none());
        assert!(follower.queue.ready().is_none());
    }

    #[test]
    fn lifecycle_is_next_but_bounded_priority_cannot_starve_download_fifo() {
        let dir = TestDir::new("priority");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let first = submit_download(&handle, &dir.file("first.gguf", b"1"), [1; 32], 1).1;
        let second = submit_download(&handle, &dir.file("second.gguf", b"2"), [2; 32], 2).1;
        gate.wait_started(2);
        let (download_key, queued_download) =
            submit_download(&handle, &dir.file("download.gguf", b"3"), [3; 32], 3);
        let (lifecycle_key, lifecycle) =
            submit_lifecycle(&handle, &dir.file("lifecycle.gguf", b"4"), [4; 32], 4);

        gate.release(1);
        let started = gate.wait_started(3);
        assert_eq!(started[2], lifecycle_key);
        gate.release(1);
        let started = gate.wait_started(4);
        assert_eq!(started[3], download_key);

        gate.release(8);
        assert!(shutdown(owner).is_ok());
        acknowledge_download(&first);
        acknowledge_download(&second);
        acknowledge_download(&queued_download);
        acknowledge_lifecycle(&lifecycle);
    }

    #[test]
    fn bind_failure_returns_every_move_only_input_without_releasing_the_permit() {
        let dir = TestDir::new("bind-failure");
        let path = dir.file("model.gguf", b"artifact");
        let (key, mut input) = input(&path, [1; 32]);
        input.expected_sha256 = [2; 32];
        let (handle, owner) = VerificationSchedulerOwner::start().unwrap();
        let reservation = match handle.reserve(key, VerificationClass::Download) {
            VerificationReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("reservation rejected"),
        };
        let coordinator = ArtifactMutationCoordinator::new();
        let artifact_key = ArtifactKey::from_destination(&path).unwrap();
        let artifact = coordinator
            .try_acquire_mutation(artifact_key.clone())
            .unwrap();
        let releases = Arc::new(AtomicUsize::new(0));
        let observed = releases.clone();
        let operation_id = OperationId::new_v4();
        let admission_revision = DecimalU64::new(1);
        let continuation = DownloadContinuation::with_release_probe_for_test(
            operation_id,
            admission_revision,
            OperationCancellation::new(),
            artifact,
            Box::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let queue = DownloadCompletionQueue::new(1);
        let completion = queue.reserve().unwrap();
        let failure = match reservation.bind_download(input, continuation, completion) {
            Err(failure) => failure,
            Ok(_) => panic!("conflicting digest must fail bind"),
        };
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert_eq!(failure.input.expected_sha256, [2; 32]);
        assert_eq!(failure.continuation.operation_id, operation_id);
        assert_eq!(failure.continuation.admission_revision, admission_revision);
        assert_eq!(
            coordinator
                .try_acquire_mutation(artifact_key.clone())
                .unwrap_err(),
            ArtifactAcquireError::Busy
        );
        drop(failure);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert!(coordinator.try_acquire_mutation(artifact_key).is_ok());
        assert!(shutdown(owner).is_ok());
    }

    #[test]
    fn completion_ack_panic_unknown_ack_and_lost_destination_retain_ownership() {
        let dir = TestDir::new("completion-faults");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let path = dir.file("lost.gguf", b"lost");
        let (_, lost) = submit_download(&handle, &path, [1; 32], 1);
        gate.wait_started(1);
        let lost_key = lost.artifact_key.clone();
        let lost_coordinator = lost.coordinator.clone();
        drop(lost.queue);
        gate.release(1);
        wait_until(|| handle.counts_for_test().retained_completions == 1);
        assert_eq!(
            lost_coordinator
                .try_acquire_mutation(lost_key.clone())
                .unwrap_err(),
            ArtifactAcquireError::Busy
        );
        handle.dispose_retained_for_test();
        assert!(lost_coordinator.try_acquire_mutation(lost_key).is_ok());

        let fatal = shutdown(owner).expect_err("destination loss seals scheduler");
        fatal.dispose_for_test();

        let (handle, owner) = VerificationSchedulerOwner::start().unwrap();
        let acknowledged = submit_download(&handle, &dir.file("ack.gguf", b"ack"), [2; 32], 2).1;
        wait_until(|| acknowledged.queue.ready().is_some());
        let retained = acknowledged.queue.ready().unwrap();
        retained.lock_ready().unwrap().acknowledge();
        assert!(acknowledged
            .coordinator
            .try_acquire_mutation(acknowledged.artifact_key.clone())
            .is_ok());

        let unknown = submit_download(&handle, &dir.file("unknown.gguf", b"unknown"), [3; 32], 3).1;
        wait_until(|| unknown.queue.ready().is_some());
        unknown
            .queue
            .ready()
            .unwrap()
            .lock_ready()
            .unwrap()
            .poison();
        assert_eq!(
            unknown
                .coordinator
                .try_acquire_mutation(unknown.artifact_key.clone())
                .unwrap_err(),
            ArtifactAcquireError::Busy
        );
        unknown.queue.dispose_poisoned_for_test();
        assert!(unknown
            .coordinator
            .try_acquire_mutation(unknown.artifact_key.clone())
            .is_ok());

        let panicked = submit_download(&handle, &dir.file("panic.gguf", b"panic"), [4; 32], 4).1;
        wait_until(|| panicked.queue.ready().is_some());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let retained = panicked.queue.ready().unwrap();
            let mut ready = retained.lock_ready().unwrap();
            let _ = ready.outcome_mut();
            panic!("injected completion receiver panic");
        }));
        assert!(result.is_err());
        assert!(panicked.queue.ready().is_none());
        assert!(panicked.queue.reserve().is_none());
        assert_eq!(
            panicked
                .coordinator
                .try_acquire_mutation(panicked.artifact_key.clone())
                .unwrap_err(),
            ArtifactAcquireError::Busy
        );
        panicked.queue.dispose_poisoned_for_test();
        assert!(panicked
            .coordinator
            .try_acquire_mutation(panicked.artifact_key.clone())
            .is_ok());
        assert!(shutdown(owner).is_ok());
    }

    #[test]
    fn shutdown_cancels_every_job_rejects_admission_and_joins_both_workers() {
        let dir = TestDir::new("shutdown");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let mut fixtures = Vec::new();
        for index in 0..4 {
            fixtures.push(
                submit_download(
                    &handle,
                    &dir.file(&format!("model-{index}.gguf"), b"artifact"),
                    [index as u8; 32],
                    index + 1,
                )
                .1,
            );
        }
        gate.wait_started(2);
        handle.stop();
        for cancellation in handle.job_cancellations_for_test() {
            assert!(cancellation.is_cancelled());
        }
        let extra = dir.file("extra.gguf", b"extra");
        let (key, _) = input(&extra, [99; 32]);
        assert!(matches!(
            handle.reserve(key, VerificationClass::Download),
            VerificationReserveOutcome::Stopping
        ));
        gate.release(16);
        let exits = shutdown(owner).expect("clean cancellation joins workers");
        assert_eq!(exits, vec![VerificationWorkerExit::Stopped; 2]);
        for fixture in &fixtures {
            assert!(matches!(
                acknowledge_download(fixture),
                VerificationResult::Cancelled
            ));
        }
    }

    #[test]
    fn shutdown_deadline_retains_unjoined_owner_for_the_fatal_runtime() {
        let dir = TestDir::new("shutdown-deadline");
        let gate = Arc::new(WorkerGate::default());
        let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
            let gate = gate.clone();
            move |key| gate.enter(key)
        });
        let fixture = submit_download(&handle, &dir.file("model.gguf", b"artifact"), [1; 32], 1).1;
        gate.wait_started(1);

        let failure = owner
            .shutdown(Instant::now() + Duration::from_millis(20))
            .expect_err("blocked verifier must retain its unjoined owner at deadline");
        assert_eq!(
            failure.reason(),
            VerificationShutdownReason::DeadlineExceeded
        );
        assert!(failure.retains_unjoined_workers());

        gate.release(1);
        let owner = failure.into_owner();
        assert!(owner
            .shutdown(Instant::now() + Duration::from_secs(5))
            .is_ok());
        assert!(matches!(
            acknowledge_download(&fixture),
            VerificationResult::Cancelled
        ));
    }

    #[test]
    fn dropping_live_owner_aborts_promptly_instead_of_blocking_on_worker() {
        const CHILD: &str = "LOXA_VERIFICATION_OWNER_DROP_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let dir = TestDir::new("owner-drop-child");
            let gate = Arc::new(WorkerGate::default());
            let (handle, owner) = VerificationSchedulerOwner::start_with_worker_hook_for_test({
                let gate = gate.clone();
                move |key| gate.enter(key)
            });
            let _fixture =
                submit_download(&handle, &dir.file("model.gguf", b"artifact"), [1; 32], 1).1;
            gate.wait_started(1);
            drop(owner);
            panic!("dropping a live owner returned instead of aborting");
        }

        let executable = std::env::current_exe().unwrap();
        let mut child = std::process::Command::new(executable)
            .arg("verification_scheduler::tests::dropping_live_owner_aborts_promptly_instead_of_blocking_on_worker")
            .arg("--exact")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD, "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("dropping a live owner blocked instead of aborting promptly");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(!status.success(), "live owner drop unexpectedly succeeded");
    }
}
