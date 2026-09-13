use super::{history_error, maybe_begin_history_drain};
use crate::history::{
    CommittedAdmission, ExecutionOutcome, FinalizationInput, SuffixCommit, SuffixInput,
};
use crate::service::coordinator::state::AdmissionReservation;
use crate::service::coordinator::Shared;
use loxa_ipc::{ErrorCategory, ServiceError};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;

pub(in crate::service::coordinator) type OutputObserver =
    watch::Receiver<Option<Result<SuffixCommit, loxa_ipc::ServiceError>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service::coordinator) enum OutputSavePhase {
    Open { saved_end: u64 },
    Saving { saved_end: u64 },
    SaveFailed { saved_end: u64 },
    Saved { saved_end: u64 },
}

#[derive(Clone)]
#[cfg_attr(not(test), allow(dead_code))] // M4 constructs this handle after generation admission.
pub(in crate::service::coordinator) struct GenerationOutput {
    shared: Arc<Shared>,
    reservation: Arc<AdmissionReservation>,
}

pub(in crate::service::coordinator) struct OutputState {
    committed: CommittedAdmission,
    saved_end: u64,
    current: Option<OutputIntent>,
    pending: Option<OutputIntent>,
    failed: bool,
    closed: bool,
    next_sequence: u64,
    status: OutputSavePhase,
}

#[derive(Clone)]
struct OutputIntent {
    sequence: u64,
    kind: OutputIntentKind,
    outcome: watch::Sender<Option<Result<SuffixCommit, ServiceError>>>,
}

#[derive(Clone)]
enum OutputIntentKind {
    Checkpoint(Arc<SuffixInput>),
    Final(Arc<FinalizationInput>),
}

impl OutputState {
    pub(in crate::service::coordinator) fn new(committed: CommittedAdmission) -> Self {
        Self {
            committed,
            saved_end: 0,
            current: None,
            pending: None,
            failed: false,
            closed: false,
            next_sequence: 1,
            status: OutputSavePhase::Open { saved_end: 0 },
        }
    }

    pub(in crate::service::coordinator) fn matches(&self, committed: &CommittedAdmission) -> bool {
        !self.closed && self.committed == *committed
    }
}

#[cfg_attr(not(test), allow(dead_code))] // M4 drives persistence through this handle.
impl GenerationOutput {
    pub(super) fn new(shared: Arc<Shared>, reservation: Arc<AdmissionReservation>) -> Self {
        Self {
            shared,
            reservation,
        }
    }

    pub(super) fn status(&self) -> Result<OutputSavePhase, ServiceError> {
        let output = self
            .reservation
            .output
            .lock()
            .map_err(|_| internal("output persistence lock is poisoned"))?;
        output
            .as_ref()
            .map(|output| output.status.clone())
            .ok_or_else(|| conflict("output persistence is no longer active"))
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.reservation.cancelled.load(Ordering::Acquire)
    }

    pub(super) fn checkpoint(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<OutputObserver, ServiceError> {
        input.validate(false).map_err(history_error)?;
        self.enqueue(OutputIntentKind::Checkpoint(input))
    }

    pub(super) fn finalize(
        &self,
        input: Arc<FinalizationInput>,
    ) -> Result<OutputObserver, ServiceError> {
        input.validate().map_err(history_error)?;
        self.enqueue(OutputIntentKind::Final(input))
    }

    pub(super) fn retry_save(&self) -> Result<OutputObserver, ServiceError> {
        let intent = {
            let state = self.shared.state();
            if !state.admission_is_current(&self.reservation) {
                return Err(conflict("output persistence is no longer authoritative"));
            }
            let mut output = self
                .reservation
                .output
                .lock()
                .map_err(|_| internal("output persistence lock is poisoned"))?;
            let output = output
                .as_mut()
                .ok_or_else(|| conflict("output persistence is no longer active"))?;
            if output.closed {
                return Err(conflict("output persistence is already finalized"));
            }
            if !output.failed {
                return Err(conflict("output persistence has no failed save to retry"));
            }
            let intent = output
                .current
                .as_ref()
                .cloned()
                .ok_or_else(|| internal("failed output has no retained intent"))?;
            output.failed = false;
            intent.outcome.send_replace(None);
            output.status = OutputSavePhase::Saving {
                saved_end: output.saved_end,
            };
            drop(state);
            intent
        };
        let observer = intent.outcome.subscribe();
        dispatch(
            Arc::clone(&self.shared),
            Arc::clone(&self.reservation),
            intent,
        );
        Ok(observer)
    }

    fn enqueue(&self, kind: OutputIntentKind) -> Result<OutputObserver, ServiceError> {
        let mut dispatch_now = None;
        let observer = {
            let state = self.shared.state();
            if !state.admission_is_current(&self.reservation) {
                return Err(conflict("output persistence is no longer authoritative"));
            }
            let cancelled = self.reservation.cancelled.load(Ordering::Acquire);
            if cancelled
                && (matches!(&kind, OutputIntentKind::Checkpoint(_))
                    || matches!(
                        &kind,
                        OutputIntentKind::Final(input)
                            if input.execution_outcome == ExecutionOutcome::Completed
                    ))
            {
                return Err(unavailable("generation output has been cancelled"));
            }
            let mut output = self
                .reservation
                .output
                .lock()
                .map_err(|_| internal("output persistence lock is poisoned"))?;
            let output = output
                .as_mut()
                .ok_or_else(|| conflict("output persistence is no longer active"))?;
            if output.closed {
                return Err(conflict("output persistence is already finalized"));
            }
            validate_identity(&self.shared, output, &kind)?;
            if output.failed {
                return Err(unavailable(
                    "output save failed; retry the exact retained suffix",
                ));
            }
            if output.pending.is_some() {
                return Err(busy("output persistence already has a pending suffix"));
            }
            let expected_start = output
                .current
                .as_ref()
                .map_or(output.saved_end, OutputIntent::end);
            if kind.start() != expected_start {
                return Err(conflict(
                    "output suffix does not follow the retained prefix",
                ));
            }
            if output.current.as_ref().is_some_and(OutputIntent::is_final) {
                return Err(conflict("output finalization is already in flight"));
            }
            let (outcome, observer) = watch::channel(None);
            let intent = OutputIntent {
                sequence: output.next_sequence,
                kind,
                outcome,
            };
            output.next_sequence = output.next_sequence.saturating_add(1);
            if output.current.is_none() {
                output.current = Some(intent.clone());
                output.status = OutputSavePhase::Saving {
                    saved_end: output.saved_end,
                };
                dispatch_now = Some(intent);
            } else {
                output.pending = Some(intent);
            }
            drop(state);
            observer
        };
        if let Some(intent) = dispatch_now {
            dispatch(
                Arc::clone(&self.shared),
                Arc::clone(&self.reservation),
                intent,
            );
        }
        Ok(observer)
    }
}

fn dispatch(shared: Arc<Shared>, reservation: Arc<AdmissionReservation>, intent: OutputIntent) {
    let completion = match &intent.kind {
        OutputIntentKind::Checkpoint(input) => shared.history.try_append_suffix(Arc::clone(input)),
        OutputIntentKind::Final(input) => shared.history.try_finalize(Arc::clone(input)),
    };
    match completion {
        Ok(completion) => {
            tokio::spawn(async move {
                let result = match completion.await {
                    Ok(completion) => {
                        let result = completion.result.map_err(history_error);
                        drop(completion.permit);
                        result
                    }
                    Err(_) => Err(unavailable(
                        "history owner stopped before acknowledging output persistence",
                    )),
                };
                resolve(shared, reservation, intent, result);
            });
        }
        Err(error) => resolve(shared, reservation, intent, Err(history_error(error))),
    }
}

fn resolve(
    shared: Arc<Shared>,
    reservation: Arc<AdmissionReservation>,
    intent: OutputIntent,
    result: Result<SuffixCommit, ServiceError>,
) {
    let outcome = intent.outcome.clone();
    let mut next = None;
    let mut terminal = false;
    let mut published = result.clone();
    {
        let mut output = reservation
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(output) = output.as_mut() else {
            return;
        };
        if !output
            .current
            .as_ref()
            .is_some_and(|current| current.sequence == intent.sequence)
        {
            return;
        }
        match &result {
            Ok(commit)
                if commit.start == intent.start()
                    && commit.end == intent.end()
                    && commit.current_saved_end >= commit.end =>
            {
                output.saved_end = commit.current_saved_end;
                terminal = intent.is_final();
                output.current = output.pending.take();
                output.failed = false;
                if terminal {
                    if output.current.is_some() {
                        published = Err(internal(
                            "output persisted a finalization before a pending suffix",
                        ));
                        output.failed = true;
                        terminal = false;
                    } else {
                        output.closed = true;
                        output.status = OutputSavePhase::Saved {
                            saved_end: output.saved_end,
                        };
                    }
                } else if let Some(current) = &output.current {
                    output.status = OutputSavePhase::Saving {
                        saved_end: output.saved_end,
                    };
                    next = Some(current.clone());
                } else {
                    output.status = OutputSavePhase::Open {
                        saved_end: output.saved_end,
                    };
                }
            }
            Ok(_) => {
                published = Err(internal("history owner returned a mismatched suffix range"));
                output.failed = true;
                reservation.cancelled.store(true, Ordering::Release);
                output.status = OutputSavePhase::SaveFailed {
                    saved_end: output.saved_end,
                };
            }
            Err(_) => {
                output.failed = true;
                reservation.cancelled.store(true, Ordering::Release);
                output.status = OutputSavePhase::SaveFailed {
                    saved_end: output.saved_end,
                };
            }
        }
    }
    drop(intent);
    if terminal {
        {
            let mut state = shared.state();
            state.finish_admission(&reservation);
            outcome.send_replace(Some(published));
        }
        maybe_begin_history_drain(&shared);
    } else {
        outcome.send_replace(Some(published));
        if let Some(next) = next {
            dispatch(shared, reservation, next);
        }
    }
}

fn validate_identity(
    shared: &Shared,
    output: &OutputState,
    kind: &OutputIntentKind,
) -> Result<(), ServiceError> {
    let input = kind.suffix();
    if input.attempt_id != output.committed.attempt_id
        || input.operation_generation != output.committed.operation_generation
        || input.owner_epoch != shared.boot_epoch
    {
        return Err(conflict(
            "output suffix does not identify the admitted generation",
        ));
    }
    Ok(())
}

impl OutputIntent {
    fn start(&self) -> u64 {
        self.kind.start()
    }

    fn end(&self) -> u64 {
        self.kind.end()
    }

    fn is_final(&self) -> bool {
        matches!(self.kind, OutputIntentKind::Final(_))
    }
}

impl OutputIntentKind {
    fn suffix(&self) -> &SuffixInput {
        match self {
            Self::Checkpoint(input) => input,
            Self::Final(input) => &input.suffix,
        }
    }

    fn start(&self) -> u64 {
        self.suffix().expected_saved_end
    }

    fn end(&self) -> u64 {
        self.start() + self.suffix().content.len() as u64
    }
}

fn busy(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Busy, context)
}

fn conflict(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Conflict, context)
}

fn unavailable(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::ServiceUnavailable, context)
}

fn internal(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Internal, context)
}
