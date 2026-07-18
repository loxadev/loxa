use crate::actor::MutationCancellation;
use crate::model_lifecycle::{
    EngineLifecycleDriver, GatewayPublisher, LaunchPlan, LifecycleError, ModelLifecycle,
};
use crate::verification_scheduler::{
    CompletionDestination, LifecycleVerificationCompletion, LifecycleVerificationOutcome,
    RetainedCompletion, VerificationResult,
};
use loxa_core::model_inventory::VerifiedArtifact;
use loxa_core::supervisor::ObservedChildExit;
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) const LIFECYCLE_NORMAL_CAPACITY: usize = 4;

pub(crate) enum LifecycleCommand {
    Load {
        operation_id: OperationId,
        model_id: String,
        revision: DecimalU64,
    },
    Unload {
        operation_id: OperationId,
        revision: DecimalU64,
    },
    Cancel {
        operation_id: OperationId,
    },
    VerificationFinished {
        operation_id: OperationId,
        result: VerificationResult,
    },
    ChildExited(ObservedChildExit),
    Shutdown {
        deadline: Instant,
    },
}

impl LifecycleCommand {
    fn is_normal(&self) -> bool {
        matches!(
            self,
            Self::Load { .. } | Self::Unload { .. } | Self::VerificationFinished { .. }
        )
    }
}

struct LifecycleMailboxState {
    shutdown: Option<Instant>,
    child_exit: Option<ObservedChildExit>,
    cancel: Option<OperationId>,
    normal: VecDeque<LifecycleNormalEntry>,
    reserved_normal: usize,
    sealed: bool,
    fatal: bool,
    fatal_notified: bool,
    active: Option<(OperationId, MutationCancellation)>,
}

enum LifecycleNormalEntry {
    Command(LifecycleCommand),
    Verification,
}

pub(crate) struct LifecycleMailboxInner {
    state: Mutex<LifecycleMailboxState>,
    changed: Condvar,
    verification: CompletionDestination<LifecycleVerificationOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleSubmitError {
    Stopping,
    Full,
    InvalidReservation,
    ConflictingCancel,
    ConflictingChildExit,
    Poisoned,
}

pub(crate) struct LifecycleNormalReservation {
    mailbox: Arc<LifecycleMailboxInner>,
    reserved: bool,
}

impl LifecycleNormalReservation {
    pub(crate) fn submit(mut self, command: LifecycleCommand) -> Result<(), LifecycleSubmitError> {
        if !command.is_normal() {
            return Err(LifecycleSubmitError::InvalidReservation);
        }
        let mut state = self
            .mailbox
            .state
            .lock()
            .map_err(|_| LifecycleSubmitError::Poisoned)?;
        if state.sealed || state.shutdown.is_some() {
            return Err(LifecycleSubmitError::Stopping);
        }
        if state.reserved_normal == 0 || state.normal.len() >= LIFECYCLE_NORMAL_CAPACITY {
            state.sealed = true;
            return Err(LifecycleSubmitError::InvalidReservation);
        }
        state.reserved_normal -= 1;
        state
            .normal
            .push_back(LifecycleNormalEntry::Command(command));
        self.reserved = false;
        drop(state);
        self.mailbox.changed.notify_all();
        Ok(())
    }

    pub(crate) fn into_verification_completion(
        mut self,
    ) -> Result<LifecycleVerificationCompletion, LifecycleSubmitError> {
        let completion = self
            .mailbox
            .reserve_verification()
            .ok_or(LifecycleSubmitError::Full)?;
        let mut state = self
            .mailbox
            .state
            .lock()
            .map_err(|_| LifecycleSubmitError::Poisoned)?;
        if state.sealed || state.shutdown.is_some() || state.reserved_normal == 0 {
            drop(state);
            drop(completion);
            return Err(LifecycleSubmitError::Stopping);
        }
        state.reserved_normal -= 1;
        state.normal.push_back(LifecycleNormalEntry::Verification);
        self.reserved = false;
        Ok(completion)
    }
}

impl Drop for LifecycleNormalReservation {
    fn drop(&mut self) {
        if !self.reserved {
            return;
        }
        let Ok(mut state) = self.mailbox.state.lock() else {
            return;
        };
        if state.reserved_normal > 0 {
            state.reserved_normal -= 1;
        } else {
            state.sealed = true;
        }
        drop(state);
        self.mailbox.changed.notify_all();
    }
}

impl LifecycleMailboxInner {
    pub(crate) fn new(verification_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LifecycleMailboxState {
                shutdown: None,
                child_exit: None,
                cancel: None,
                normal: VecDeque::with_capacity(LIFECYCLE_NORMAL_CAPACITY),
                reserved_normal: 0,
                sealed: false,
                fatal: false,
                fatal_notified: false,
                active: None,
            }),
            changed: Condvar::new(),
            verification: CompletionDestination::new(verification_capacity),
        })
    }

    pub(crate) fn reserve_normal(self: &Arc<Self>) -> Option<LifecycleNormalReservation> {
        let mut state = self.state.lock().ok()?;
        if state.sealed
            || state.shutdown.is_some()
            || state.normal.len() + state.reserved_normal >= LIFECYCLE_NORMAL_CAPACITY
        {
            return None;
        }
        state.reserved_normal += 1;
        Some(LifecycleNormalReservation {
            mailbox: Arc::clone(self),
            reserved: true,
        })
    }

    pub(crate) fn reserve_verification(
        self: &Arc<Self>,
    ) -> Option<LifecycleVerificationCompletion> {
        LifecycleVerificationCompletion::reserve(&self.verification, self)
    }

    pub(crate) fn request_cancel(
        &self,
        operation_id: OperationId,
    ) -> Result<(), LifecycleSubmitError> {
        let cancellation = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LifecycleSubmitError::Poisoned)?;
            if state.sealed || state.shutdown.is_some() {
                return Err(LifecycleSubmitError::Stopping);
            }
            match &state.cancel {
                Some(known) if *known == operation_id => return Ok(()),
                Some(_) => return Err(LifecycleSubmitError::ConflictingCancel),
                None => state.cancel = Some(operation_id),
            }
            state.active.as_ref().and_then(|(known, cancellation)| {
                (*known == operation_id).then(|| cancellation.clone())
            })
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn observe_child_exit(
        &self,
        exit: ObservedChildExit,
    ) -> Result<(), LifecycleSubmitError> {
        let (result, active) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LifecycleSubmitError::Poisoned)?;
            if state.sealed {
                return Err(LifecycleSubmitError::Stopping);
            }
            match &state.child_exit {
                Some(known) if *known == exit => (Ok(()), None),
                Some(_) => {
                    state.sealed = true;
                    state.fatal = true;
                    (
                        Err(LifecycleSubmitError::ConflictingChildExit),
                        state.active.as_ref().map(|(_, active)| active.clone()),
                    )
                }
                None => {
                    state.child_exit = Some(exit);
                    (Ok(()), None)
                }
            }
        };
        if let Some(active) = active {
            active.cancel();
        }
        self.changed.notify_all();
        result
    }

    fn request_owner_shutdown(&self, deadline: Instant) -> Result<(), LifecycleSubmitError> {
        let active = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LifecycleSubmitError::Poisoned)?;
            state.sealed = true;
            state.shutdown = Some(state.shutdown.map_or(deadline, |known| known.min(deadline)));
            state.active.as_ref().map(|(_, active)| active.clone())
        };
        if let Some(active) = active {
            active.cancel();
        }
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn is_sealed(&self) -> bool {
        self.state.lock().map_or(true, |state| state.sealed)
    }

    pub(super) fn notify_verification_ready(&self) -> bool {
        let notified = self.verification.notify_ready();
        self.changed.notify_all();
        notified
    }

    pub(super) fn rollback_verification(&self, completion: &LifecycleVerificationCompletion) {
        completion.rollback_from(&self.verification);
        if let Ok(mut state) = self.state.lock() {
            if let Some(index) = state
                .normal
                .iter()
                .position(|entry| matches!(entry, LifecycleNormalEntry::Verification))
            {
                state.normal.remove(index);
            } else {
                state.sealed = true;
            }
        }
        self.changed.notify_all();
    }

    fn ready_verification(&self) -> Option<RetainedCompletion<LifecycleVerificationOutcome>> {
        self.verification.ready()
    }

    fn settle_ready_verification_for_shutdown(
        &self,
        acknowledgement: LifecycleCancelAcknowledgement,
    ) -> bool {
        let Some(completion) = self.ready_verification() else {
            return false;
        };
        let Some(ticket) = completion.take_ready() else {
            return false;
        };
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                drop(ticket);
                return false;
            }
        };
        let Some(index) = state
            .normal
            .iter()
            .position(|entry| matches!(entry, LifecycleNormalEntry::Verification))
        else {
            state.sealed = true;
            state.fatal = true;
            drop(state);
            ticket.poison();
            return false;
        };
        state.normal.remove(index);
        drop(state);
        match acknowledgement {
            LifecycleCancelAcknowledgement::DurablyConfirmed => {
                ticket.acknowledge();
                true
            }
            LifecycleCancelAcknowledgement::Unknown => {
                ticket.poison();
                false
            }
        }
    }

    fn take_next(&self) -> Result<MailboxItem, LifecycleSubmitError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LifecycleSubmitError::Poisoned)?;
        loop {
            if let Some(deadline) = state.shutdown.take() {
                return Ok(MailboxItem::Command(LifecycleCommand::Shutdown {
                    deadline,
                }));
            }
            if state.fatal {
                if !state.fatal_notified {
                    state.fatal_notified = true;
                    return Ok(MailboxItem::Fatal);
                }
                (state, _) = self
                    .changed
                    .wait_timeout(state, Duration::from_millis(10))
                    .map_err(|_| LifecycleSubmitError::Poisoned)?;
                continue;
            }
            if let Some(exit) = state.child_exit.take() {
                return Ok(MailboxItem::Command(LifecycleCommand::ChildExited(exit)));
            }
            if let Some(operation_id) = state.cancel.take() {
                return Ok(MailboxItem::Command(LifecycleCommand::Cancel {
                    operation_id,
                }));
            }
            if matches!(
                state.normal.front(),
                Some(LifecycleNormalEntry::Verification)
            ) {
                drop(state);
                if let Some(completion) = self.ready_verification() {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| LifecycleSubmitError::Poisoned)?;
                    if matches!(
                        state.normal.front(),
                        Some(LifecycleNormalEntry::Verification)
                    ) {
                        state.normal.pop_front();
                        return Ok(MailboxItem::Verification(completion));
                    }
                    state.sealed = true;
                    return Err(LifecycleSubmitError::Poisoned);
                }
                state = self
                    .state
                    .lock()
                    .map_err(|_| LifecycleSubmitError::Poisoned)?;
                (state, _) = self
                    .changed
                    .wait_timeout(state, Duration::from_millis(10))
                    .map_err(|_| LifecycleSubmitError::Poisoned)?;
                continue;
            }
            if let Some(LifecycleNormalEntry::Command(command)) = state.normal.pop_front() {
                return Ok(MailboxItem::Command(command));
            }
            drop(state);
            if let Some(completion) = self.ready_verification() {
                return Ok(MailboxItem::Verification(completion));
            }
            state = self
                .state
                .lock()
                .map_err(|_| LifecycleSubmitError::Poisoned)?;
            (state, _) = self
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| LifecycleSubmitError::Poisoned)?;
        }
    }

    fn set_active(&self, operation_id: OperationId, cancellation: MutationCancellation) {
        let mut state = self.state.lock().expect("lifecycle mailbox poisoned");
        state.active = Some((operation_id, cancellation));
    }

    fn clear_active(&self, operation_id: &OperationId) {
        let mut state = self.state.lock().expect("lifecycle mailbox poisoned");
        if state
            .active
            .as_ref()
            .is_some_and(|(known, _)| known == operation_id)
        {
            state.active = None;
        }
    }

    fn seal_fatal(&self) {
        let active = {
            let mut state = self.state.lock().expect("lifecycle mailbox poisoned");
            state.sealed = true;
            state.fatal = true;
            state.active.as_ref().map(|(_, active)| active.clone())
        };
        if let Some(active) = active {
            active.cancel();
        }
        self.verification.poison_ready();
        self.changed.notify_all();
    }

    fn is_fatal(&self) -> bool {
        self.state.lock().map_or(true, |state| state.fatal)
    }
}

enum MailboxItem {
    Command(LifecycleCommand),
    Verification(RetainedCompletion<LifecycleVerificationOutcome>),
    Fatal,
}

#[derive(Clone)]
pub(crate) struct LifecycleControllerHandle {
    mailbox: Arc<LifecycleMailboxInner>,
}

impl LifecycleControllerHandle {
    pub(crate) fn reserve_normal(&self) -> Option<LifecycleNormalReservation> {
        self.mailbox.reserve_normal()
    }

    pub(crate) fn cancel(&self, operation_id: OperationId) -> Result<(), LifecycleSubmitError> {
        self.mailbox.request_cancel(operation_id)
    }

    pub(crate) fn child_exited(&self, exit: ObservedChildExit) -> Result<(), LifecycleSubmitError> {
        self.mailbox.observe_child_exit(exit)
    }

    #[cfg(test)]
    pub(crate) fn is_sealed_for_test(&self) -> bool {
        self.mailbox.is_sealed()
    }
}

pub(crate) struct LifecycleCompletion {
    operation_id: Option<OperationId>,
    result: Result<(), LifecycleError>,
}

impl LifecycleCompletion {
    pub(crate) fn operation_id(&self) -> Option<&OperationId> {
        self.operation_id.as_ref()
    }

    pub(crate) fn result(&self) -> &Result<(), LifecycleError> {
        &self.result
    }
}

pub(crate) struct LifecycleControllerOwner {
    mailbox: Arc<LifecycleMailboxInner>,
    worker: Option<JoinHandle<()>>,
    completions: mpsc::Receiver<LifecycleCompletion>,
    worker_exit: mpsc::Receiver<LifecycleWorkerExit>,
    fallback_exit: Arc<Mutex<Option<LifecycleWorkerExit>>>,
    retained_exit: Option<LifecycleWorkerExit>,
}

trait RetainedLifecycleResources: Send {}

impl<T: Send> RetainedLifecycleResources for T {}

struct LifecycleWorkerResources<D, G, W>
where
    D: EngineLifecycleDriver,
    G: GatewayPublisher,
{
    lifecycle: ModelLifecycle<D, G>,
    workflow: W,
}

struct LifecycleWorkerExit {
    completion: LifecycleCompletion,
    resources: Box<dyn RetainedLifecycleResources>,
}

pub(crate) struct LifecycleControllerStartFailure {
    error: std::io::Error,
    resources: ManuallyDrop<Box<dyn RetainedLifecycleResources>>,
}

impl std::fmt::Debug for LifecycleControllerStartFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LifecycleControllerStartFailure")
            .field("error", &self.error)
            .field("retains_resources", &true)
            .finish()
    }
}

impl LifecycleControllerStartFailure {
    #[cfg(test)]
    pub(crate) fn dispose_for_test(self) {
        drop(ManuallyDrop::into_inner(self.resources));
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_LIFECYCLE_SPAWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) struct LifecycleLoadRequest {
    pub(crate) operation_id: OperationId,
    pub(crate) model_id: String,
    pub(crate) revision: DecimalU64,
}

pub(crate) enum LifecycleLoadSubmission {
    Ready(LaunchPlan),
    Verifying,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleCancelAcknowledgement {
    DurablyConfirmed,
    Unknown,
}

pub(crate) trait LifecycleLoadWorkflow: Send {
    fn submit_load(
        &mut self,
        request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError>;

    fn resume_verified(
        &mut self,
        request: &LifecycleLoadRequest,
        evidence: &VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError>;

    fn cancel(&mut self, _operation_id: &OperationId) -> LifecycleCancelAcknowledgement {
        LifecycleCancelAcknowledgement::Unknown
    }

    /// Returns true only after the result's durable/readiness acknowledgement is known.
    fn acknowledge(
        &mut self,
        _request: &LifecycleLoadRequest,
        _result: Result<(), &LifecycleError>,
    ) -> bool {
        true
    }
}

struct DirectLoadWorkflow<R>(R);

impl<R> LifecycleLoadWorkflow for DirectLoadWorkflow<R>
where
    R: FnMut(&str) -> Result<LaunchPlan, LifecycleError> + Send,
{
    fn submit_load(
        &mut self,
        request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        drop(completion);
        (self.0)(&request.model_id).map(LifecycleLoadSubmission::Ready)
    }

    fn resume_verified(
        &mut self,
        request: &LifecycleLoadRequest,
        _evidence: &VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        (self.0)(&request.model_id)
    }
}

struct PendingVerifiedLoad {
    request: LifecycleLoadRequest,
    cancellation: MutationCancellation,
}

pub(crate) struct LifecycleControllerShutdownFailure {
    owner: ManuallyDrop<Box<LifecycleControllerOwner>>,
}

impl std::fmt::Debug for LifecycleControllerShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LifecycleControllerShutdownFailure")
            .field("retains_worker", &self.owner.worker.is_some())
            .finish()
    }
}

impl LifecycleControllerShutdownFailure {
    pub(crate) fn into_owner(self) -> LifecycleControllerOwner {
        *ManuallyDrop::into_inner(self.owner)
    }
}

impl LifecycleControllerOwner {
    #[cfg(test)]
    pub(crate) fn fail_next_spawn_for_test() {
        FAIL_NEXT_LIFECYCLE_SPAWN.set(true);
    }
    pub(crate) fn start<D, G, R>(
        lifecycle: ModelLifecycle<D, G>,
        resolve: R,
    ) -> Result<(LifecycleControllerHandle, Self), LifecycleControllerStartFailure>
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
        R: FnMut(&str) -> Result<LaunchPlan, LifecycleError> + Send + 'static,
    {
        Self::start_with_workflow(lifecycle, DirectLoadWorkflow(resolve))
    }

    pub(crate) fn start_with_workflow<D, G, W>(
        lifecycle: ModelLifecycle<D, G>,
        workflow: W,
    ) -> Result<(LifecycleControllerHandle, Self), LifecycleControllerStartFailure>
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
        W: LifecycleLoadWorkflow + 'static,
    {
        let mailbox = LifecycleMailboxInner::new(LIFECYCLE_NORMAL_CAPACITY);
        let worker_mailbox = Arc::clone(&mailbox);
        let (completion_tx, completion_rx) = mpsc::channel();
        let (resources_tx, resources_rx) =
            mpsc::sync_channel::<LifecycleWorkerResources<D, G, W>>(0);
        let (exit_tx, exit_rx) = mpsc::channel();
        let fallback_exit = Arc::new(Mutex::new(None));
        let worker_fallback = Arc::clone(&fallback_exit);
        let resources = LifecycleWorkerResources {
            lifecycle,
            workflow,
        };
        #[cfg(test)]
        if FAIL_NEXT_LIFECYCLE_SPAWN.replace(false) {
            return Err(LifecycleControllerStartFailure {
                error: std::io::Error::other("injected lifecycle spawn failure"),
                resources: ManuallyDrop::new(Box::new(resources)),
            });
        }
        let worker = match std::thread::Builder::new()
            .name("loxa-lifecycle".into())
            .spawn(move || {
                let Ok(mut resources) = resources_rx.recv() else {
                    return;
                };
                let lifecycle = &mut resources.lifecycle;
                let workflow = &mut resources.workflow;
                let mut cancelled_before_start = None;
                let mut pending_verified_load: Option<PendingVerifiedLoad> = None;
                let termination = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
                    let item = match worker_mailbox.take_next() {
                        Ok(item) => item,
                        Err(_) => {
                            lifecycle.request_stop();
                            return lifecycle.shutdown().and(Err(
                                LifecycleError::RecoveryRequired {
                                    replacement: "lifecycle mailbox failed".into(),
                                    rollback: "controller ownership retained".into(),
                                },
                            ));
                        }
                    };
                    match item {
                        MailboxItem::Fatal => {
                            if let Some(pending) = pending_verified_load.take() {
                                let _ = workflow.cancel(&pending.request.operation_id);
                            }
                            continue;
                        }
                        MailboxItem::Command(LifecycleCommand::Shutdown { deadline: _ }) => {
                            if let Some(pending) = &pending_verified_load {
                                let acknowledgement =
                                    workflow.cancel(&pending.request.operation_id);
                                if !worker_mailbox
                                    .settle_ready_verification_for_shutdown(acknowledgement)
                                {
                                    worker_mailbox.seal_fatal();
                                    let _ = lifecycle.shutdown();
                                    return Err(LifecycleError::RecoveryRequired {
                                        replacement: "lifecycle verification shutdown is uncertain"
                                            .into(),
                                        rollback: "continuation ownership retained".into(),
                                    });
                                }
                            }
                            return lifecycle.shutdown();
                        }
                        MailboxItem::Command(LifecycleCommand::ChildExited(exit)) => {
                            if let Some(pending) = pending_verified_load.take() {
                                let _ = workflow.cancel(&pending.request.operation_id);
                            }
                            process_child_exit(lifecycle, exit);
                            worker_mailbox.seal_fatal();
                        }
                        MailboxItem::Command(LifecycleCommand::Cancel { operation_id }) => {
                            let _ = workflow.cancel(&operation_id);
                            if pending_verified_load
                                .as_ref()
                                .is_none_or(|pending| pending.request.operation_id != operation_id)
                            {
                                cancelled_before_start = Some(operation_id);
                            }
                        }
                        MailboxItem::Command(LifecycleCommand::Load {
                            operation_id,
                            model_id,
                            revision,
                        }) => {
                            let cancellation = MutationCancellation::new();
                            if cancelled_before_start.as_ref() == Some(&operation_id) {
                                cancellation.cancel();
                                cancelled_before_start = None;
                            }
                            worker_mailbox.set_active(operation_id, cancellation.clone());
                            let request = LifecycleLoadRequest {
                                operation_id,
                                model_id,
                                revision,
                            };
                            let submission = worker_mailbox
                                .reserve_normal()
                                .ok_or(LifecycleError::Stopping)
                                .and_then(|reservation| {
                                    reservation
                                        .into_verification_completion()
                                        .map_err(|_| LifecycleError::Stopping)
                                })
                                .and_then(|completion| workflow.submit_load(&request, completion));
                            match submission {
                                Ok(LifecycleLoadSubmission::Verifying) => {
                                    pending_verified_load = Some(PendingVerifiedLoad {
                                        request,
                                        cancellation,
                                    });
                                }
                                Ok(LifecycleLoadSubmission::Ready(plan)) => {
                                    let mut result = lifecycle.load(plan, &cancellation);
                                    let acknowledged =
                                        workflow.acknowledge(&request, result.as_ref().map(|_| ()));
                                    if !acknowledged {
                                        worker_mailbox.seal_fatal();
                                        let unknown = unknown_acknowledgement();
                                        lifecycle.fail_supervision(unknown_acknowledgement());
                                        result = Err(unknown);
                                    }
                                    if lifecycle.recovery_required()
                                        || matches!(
                                            result,
                                            Err(LifecycleError::RecoveryRequired { .. })
                                        )
                                    {
                                        worker_mailbox.seal_fatal();
                                    }
                                    worker_mailbox.clear_active(&operation_id);
                                    lifecycle.complete_operation();
                                    let _ = completion_tx.send(LifecycleCompletion {
                                        operation_id: Some(operation_id),
                                        result,
                                    });
                                }
                                Err(error) => {
                                    if matches!(error, LifecycleError::RecoveryRequired { .. }) {
                                        worker_mailbox.seal_fatal();
                                    }
                                    worker_mailbox.clear_active(&operation_id);
                                    lifecycle.complete_operation();
                                    let _ = completion_tx.send(LifecycleCompletion {
                                        operation_id: Some(operation_id),
                                        result: Err(error),
                                    });
                                }
                            }
                        }
                        MailboxItem::Command(LifecycleCommand::Unload {
                            operation_id,
                            revision: _,
                        }) => {
                            let cancellation = MutationCancellation::new();
                            if cancelled_before_start.as_ref() == Some(&operation_id) {
                                cancellation.cancel();
                                cancelled_before_start = None;
                            }
                            worker_mailbox.set_active(operation_id, cancellation.clone());
                            let result = lifecycle.unload(&cancellation);
                            if lifecycle.recovery_required()
                                || matches!(result, Err(LifecycleError::RecoveryRequired { .. }))
                            {
                                worker_mailbox.seal_fatal();
                            }
                            worker_mailbox.clear_active(&operation_id);
                            lifecycle.complete_operation();
                            let _ = completion_tx.send(LifecycleCompletion {
                                operation_id: Some(operation_id),
                                result,
                            });
                        }
                        MailboxItem::Command(LifecycleCommand::VerificationFinished {
                            operation_id,
                            result,
                        }) => {
                            let result = verification_result(result);
                            let _ = completion_tx.send(LifecycleCompletion {
                                operation_id: Some(operation_id),
                                result,
                            });
                        }
                        MailboxItem::Verification(completion) => {
                            let Some(mut ready) = completion.take_ready() else {
                                continue;
                            };
                            let operation_id = ready.outcome_mut().ownership.operation_id;
                            let Some(pending) = pending_verified_load.take() else {
                                ready.poison();
                                worker_mailbox.seal_fatal();
                                lifecycle.fail_supervision(unknown_acknowledgement());
                                continue;
                            };
                            if pending.request.operation_id != operation_id {
                                pending_verified_load = Some(pending);
                                ready.poison();
                                worker_mailbox.seal_fatal();
                                lifecycle.fail_supervision(unknown_acknowledgement());
                                continue;
                            }
                            let mut result = match &ready.outcome_mut().result {
                                VerificationResult::Verified(evidence) => workflow
                                    .resume_verified(&pending.request, evidence)
                                    .and_then(|plan| lifecycle.load(plan, &pending.cancellation)),
                                VerificationResult::Cancelled => Err(LifecycleError::Cancelled),
                                VerificationResult::Failed { .. } => {
                                    Err(LifecycleError::ModelNotVerified)
                                }
                            };
                            let acknowledged =
                                workflow.acknowledge(&pending.request, result.as_ref().map(|_| ()));
                            if acknowledged {
                                ready.acknowledge();
                            } else {
                                ready.poison();
                                worker_mailbox.seal_fatal();
                                lifecycle.fail_supervision(unknown_acknowledgement());
                                result = Err(unknown_acknowledgement());
                            }
                            if lifecycle.recovery_required()
                                || matches!(result, Err(LifecycleError::RecoveryRequired { .. }))
                            {
                                worker_mailbox.seal_fatal();
                            }
                            worker_mailbox.clear_active(&operation_id);
                            lifecycle.complete_operation();
                            let _ = completion_tx.send(LifecycleCompletion {
                                operation_id: Some(operation_id),
                                result,
                            });
                        }
                    }
                }));
                let result = match termination {
                    Ok(result) => result,
                    Err(_) => {
                        worker_mailbox.seal_fatal();
                        Err(LifecycleError::RecoveryRequired {
                            replacement: "lifecycle worker panicked".into(),
                            rollback: "exact lifecycle ownership retained".into(),
                        })
                    }
                };
                let exit = LifecycleWorkerExit {
                    completion: LifecycleCompletion {
                        operation_id: None,
                        result,
                    },
                    resources: Box::new(resources),
                };
                if let Err(disconnected) = exit_tx.send(exit) {
                    let mut fallback = worker_fallback
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    *fallback = Some(disconnected.0);
                }
            }) {
            Ok(worker) => worker,
            Err(error) => {
                return Err(LifecycleControllerStartFailure {
                    error,
                    resources: ManuallyDrop::new(Box::new(resources)),
                });
            }
        };
        if let Err(failed) = resources_tx.send(resources) {
            let _ = worker.join();
            return Err(LifecycleControllerStartFailure {
                error: std::io::Error::other("lifecycle ownership handoff failed"),
                resources: ManuallyDrop::new(Box::new(failed.0)),
            });
        }
        let handle = LifecycleControllerHandle {
            mailbox: Arc::clone(&mailbox),
        };
        Ok((
            handle,
            Self {
                mailbox,
                worker: Some(worker),
                completions: completion_rx,
                worker_exit: exit_rx,
                fallback_exit,
                retained_exit: None,
            },
        ))
    }

    pub(crate) fn recv_completion_timeout(
        &self,
        timeout: Duration,
    ) -> Result<LifecycleCompletion, mpsc::RecvTimeoutError> {
        self.completions.recv_timeout(timeout)
    }

    #[cfg(test)]
    pub(crate) fn dispose_fatal_for_test(mut self) {
        if let Ok(mut state) = self.mailbox.state.lock() {
            state.fatal = false;
        }
        self.mailbox.verification.dispose_poisoned();
        drop(self.retained_exit.take());
        let fallback = self
            .fallback_exit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(fallback);
    }

    #[cfg(test)]
    pub(crate) fn disconnect_worker_exit_for_test(&mut self) {
        let (_, disconnected) = mpsc::channel();
        self.worker_exit = disconnected;
    }

    pub(crate) fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<(), LifecycleControllerShutdownFailure> {
        let _ = self.mailbox.request_owner_shutdown(deadline);
        while self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            if Instant::now() >= deadline {
                return Err(LifecycleControllerShutdownFailure {
                    owner: ManuallyDrop::new(Box::new(self)),
                });
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            return Err(LifecycleControllerShutdownFailure {
                owner: ManuallyDrop::new(Box::new(self)),
            });
        }
        let direct_exit = self.worker_exit.try_recv();
        let disconnected = matches!(direct_exit, Err(mpsc::TryRecvError::Disconnected));
        let mut exit = direct_exit.ok().or_else(|| {
            self.fallback_exit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        });
        let valid_shutdown = exit.as_ref().is_some_and(|exit| {
            exit.completion.operation_id.is_none()
                && exit.completion.result.is_ok()
                && !self.mailbox.is_fatal()
                && !disconnected
        });
        if !valid_shutdown {
            self.retained_exit = exit.take();
            return Err(LifecycleControllerShutdownFailure {
                owner: ManuallyDrop::new(Box::new(self)),
            });
        }
        drop(exit);
        Ok(())
    }
}

impl Drop for LifecycleControllerOwner {
    fn drop(&mut self) {
        let _ = self.mailbox.request_owner_shutdown(Instant::now());
        let joined = self
            .worker
            .take()
            .is_none_or(|worker| worker.join().is_ok());
        let direct_exit = self.worker_exit.try_recv();
        let disconnected = matches!(direct_exit, Err(mpsc::TryRecvError::Disconnected));
        let exit = direct_exit.ok().or_else(|| {
            self.fallback_exit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        });
        let valid_shutdown = joined
            && !disconnected
            && exit.as_ref().is_some_and(|exit| {
                exit.completion.operation_id.is_none()
                    && exit.completion.result.is_ok()
                    && !self.mailbox.is_fatal()
            });
        if !valid_shutdown {
            if let Some(exit) = exit {
                std::mem::forget(exit);
            }
            if let Some(exit) = self.retained_exit.take() {
                std::mem::forget(exit);
            }
        }
    }
}

fn verification_result(result: VerificationResult) -> Result<(), LifecycleError> {
    match result {
        VerificationResult::Verified(_) => Ok(()),
        VerificationResult::Cancelled => Err(LifecycleError::Cancelled),
        VerificationResult::Failed { .. } => Err(LifecycleError::ModelNotVerified),
    }
}

fn verification_result_ref(result: &VerificationResult) -> Result<(), LifecycleError> {
    match result {
        VerificationResult::Verified(_) => Ok(()),
        VerificationResult::Cancelled => Err(LifecycleError::Cancelled),
        VerificationResult::Failed { .. } => Err(LifecycleError::ModelNotVerified),
    }
}

fn unknown_acknowledgement() -> LifecycleError {
    LifecycleError::RecoveryRequired {
        replacement: "lifecycle acknowledgement is uncertain".into(),
        rollback: "admission sealed and ownership retained".into(),
    }
}

fn process_child_exit<D, G>(lifecycle: &mut ModelLifecycle<D, G>, exit: ObservedChildExit)
where
    D: EngineLifecycleDriver,
    G: GatewayPublisher,
{
    lifecycle.fail_observed_child_exit(match &exit {
        ObservedChildExit::RequestedStop => "requested-stop",
        ObservedChildExit::Interrupted => "interrupted",
        ObservedChildExit::Restart { .. } => "restart-decision-without-operation",
        ObservedChildExit::Exhausted { .. } => "restart-budget-exhausted",
        ObservedChildExit::RecoveryRequired => "recovery-required",
    });
}
