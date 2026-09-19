use super::{history_error, maybe_begin_history_drain};
use crate::history::{
    CommittedAdmission, ExecutionOutcome, FinalizationInput, SuffixCommit, SuffixInput,
};
use crate::service::coordinator::state::{AdmissionReservation, CancellationCause};
use crate::service::coordinator::Shared;
use loxa_ipc::{ErrorCategory, ServiceError};
use std::sync::Arc;
use tokio::sync::watch;

mod status;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service::coordinator) struct ExecutionFact {
    pub(in crate::service::coordinator) outcome: ExecutionOutcome,
    pub(in crate::service::coordinator) failure_code: Option<String>,
    pub(in crate::service::coordinator) generated_end: u64,
}

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
    execution: Option<ExecutionFact>,
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
            execution: None,
        }
    }

    fn publish_execution(
        &mut self,
        reservation: &AdmissionReservation,
        proposed: ExecutionOutcome,
        failure_code: Option<&str>,
        generated_end: u64,
    ) -> Result<ExecutionFact, ServiceError> {
        if let Some(fact) = &self.execution {
            return Ok(fact.clone());
        }
        if matches!(
            (proposed, failure_code),
            (ExecutionOutcome::Completed, Some(_)) | (ExecutionOutcome::Failed, None)
        ) || failure_code
            .is_some_and(|code| code.is_empty() || code.len() > 64 || !code.is_ascii())
        {
            return Err(internal("invalid generation failure code"));
        }
        let cancellation_can_select = proposed != ExecutionOutcome::Failed
            || matches!(failure_code, Some("stopped" | "output_save"));
        let (outcome, failure_code) = match reservation.cancellation_cause() {
            Some(CancellationCause::Requested) if cancellation_can_select => {
                (ExecutionOutcome::Stopped, Some("stopped"))
            }
            Some(CancellationCause::OutputSave) if cancellation_can_select => {
                (ExecutionOutcome::Failed, Some("output_save"))
            }
            Some(CancellationCause::EngineFailure) if cancellation_can_select => {
                (ExecutionOutcome::Failed, Some("engine_transport"))
            }
            _ => (proposed, failure_code),
        };
        let fact = ExecutionFact {
            outcome,
            failure_code: failure_code.map(str::to_owned),
            generated_end,
        };
        self.execution = Some(fact.clone());
        Ok(fact)
    }

    pub(in crate::service::coordinator) fn matches(&self, committed: &CommittedAdmission) -> bool {
        !self.closed && self.committed == *committed
    }
}

impl GenerationOutput {
    pub(in crate::service::coordinator) fn new(
        shared: Arc<Shared>,
        reservation: Arc<AdmissionReservation>,
    ) -> Self {
        Self {
            shared,
            reservation,
        }
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn status(&self) -> Result<OutputSavePhase, ServiceError> {
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

    #[cfg(test)]
    pub(in crate::service::coordinator) fn is_cancelled(&self) -> bool {
        self.reservation.is_cancelled()
    }

    pub(in crate::service::coordinator) fn cancel_output_save(&self) {
        self.reservation.cancel_output_save();
    }

    pub(in crate::service::coordinator) fn publish_execution(
        &self,
        proposed: ExecutionOutcome,
        failure_code: Option<&str>,
        generated_end: u64,
    ) -> Result<ExecutionFact, ServiceError> {
        let state = self.shared.state();
        if !state.admission_is_current(&self.reservation) {
            return Err(conflict("generation execution is no longer authoritative"));
        }
        let mut output = self
            .reservation
            .output
            .lock()
            .map_err(|_| internal("output persistence lock is poisoned"))?;
        let output = output
            .as_mut()
            .filter(|output| !output.closed)
            .ok_or_else(|| conflict("generation execution is no longer active"))?;
        output.publish_execution(&self.reservation, proposed, failure_code, generated_end)
    }

    pub(in crate::service::coordinator) fn report_capacity_saturation(&self) {
        let mut output = self
            .reservation
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(output) = output.as_mut().filter(|output| !output.closed) {
            output.status = OutputSavePhase::SaveFailed {
                saved_end: output.saved_end,
            };
        }
        self.reservation.cancel_output_save();
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn checkpoint(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<OutputObserver, ServiceError> {
        self.checkpoint_owned(input).map_err(|(error, _)| error)
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn finalize(
        &self,
        input: Arc<FinalizationInput>,
    ) -> Result<OutputObserver, ServiceError> {
        self.finalize_owned(input).map_err(|(error, _)| error)
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn checkpoint_owned(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<OutputObserver, (ServiceError, Arc<SuffixInput>)> {
        if let Err(error) = input.validate(false) {
            return Err((history_error(error), input));
        }
        self.enqueue(OutputIntentKind::Checkpoint(Arc::clone(&input)), false)
            .map(|(observer, _)| observer)
            .map_err(|(error, _)| (error, input))
    }

    pub(in crate::service::coordinator) fn checkpoint_generation_owned(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<OutputObserver, (ServiceError, Arc<SuffixInput>, bool)> {
        if let Err(error) = input.validate(false) {
            return Err((history_error(error), input, false));
        }
        self.enqueue(OutputIntentKind::Checkpoint(Arc::clone(&input)), false)
            .map(|(observer, _)| observer)
            .map_err(|(error, cancelled)| (error, input, cancelled))
    }

    pub(in crate::service::coordinator) fn checkpoint_terminal_owned(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<OutputObserver, (ServiceError, Arc<SuffixInput>)> {
        if let Err(error) = input.validate(false) {
            return Err((history_error(error), input));
        }
        self.enqueue(OutputIntentKind::Checkpoint(Arc::clone(&input)), true)
            .map(|(observer, _)| observer)
            .map_err(|(error, _)| (error, input))
    }

    #[cfg(test)]
    pub(in crate::service::coordinator) fn finalize_owned(
        &self,
        input: Arc<FinalizationInput>,
    ) -> Result<OutputObserver, (ServiceError, Arc<FinalizationInput>)> {
        self.finalize_generation_owned(input)
            .map(|(observer, _)| observer)
    }

    pub(in crate::service::coordinator) fn finalize_generation_owned(
        &self,
        input: Arc<FinalizationInput>,
    ) -> Result<(OutputObserver, ExecutionOutcome), (ServiceError, Arc<FinalizationInput>)> {
        if let Err(error) = input.validate() {
            return Err((history_error(error), input));
        }
        self.enqueue(OutputIntentKind::Final(Arc::clone(&input)), true)
            .map(|(observer, outcome)| {
                (
                    observer,
                    outcome.expect("generation finalization has an execution outcome"),
                )
            })
            .map_err(|(error, _)| (error, input))
    }

    pub(in crate::service::coordinator) fn retry_save(
        &self,
    ) -> Result<OutputObserver, ServiceError> {
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

    fn enqueue(
        &self,
        mut kind: OutputIntentKind,
        allow_cancelled_checkpoint: bool,
    ) -> Result<(OutputObserver, Option<ExecutionOutcome>), (ServiceError, bool)> {
        let mut cancelled_rejection = false;
        let result = (|| {
            let mut dispatch_now = None;
            let (observer, selected_outcome) = {
                let state = self.shared.state();
                if !state.admission_is_current(&self.reservation) {
                    return Err(conflict("output persistence is no longer authoritative"));
                }
                let cancelled = self.reservation.is_cancelled();
                if cancelled
                    && !allow_cancelled_checkpoint
                    && matches!(kind, OutputIntentKind::Checkpoint(_))
                {
                    cancelled_rejection = true;
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
                validate_identity(&self.shared, output, kind.suffix())?;
                if output.failed && !matches!(&kind, OutputIntentKind::Final(_)) {
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
                if let OutputIntentKind::Final(input) = &mut kind {
                    input.validate().map_err(history_error)?;
                    // An unpublished low-level final intent must pass every
                    // ownership/range check before it can freeze execution.
                    let fact = output.publish_execution(
                        &self.reservation,
                        input.execution_outcome,
                        input.failure_code.as_deref(),
                        input.generated_end,
                    )?;
                    if input.generated_end != fact.generated_end {
                        return Err(conflict("final output does not match frozen execution"));
                    }
                    if input.execution_outcome != fact.outcome
                        || input.failure_code != fact.failure_code
                    {
                        let input = Arc::make_mut(input);
                        input.execution_outcome = fact.outcome;
                        input.failure_code = fact.failure_code;
                    }
                }
                let selected_outcome = match &kind {
                    OutputIntentKind::Final(input) => Some(input.execution_outcome),
                    OutputIntentKind::Checkpoint(_) => None,
                };
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
                (observer, selected_outcome)
            };
            if let Some(intent) = dispatch_now {
                dispatch(
                    Arc::clone(&self.shared),
                    Arc::clone(&self.reservation),
                    intent,
                );
            }
            Ok((observer, selected_outcome))
        })();
        result.map_err(|error| (error, cancelled_rejection))
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
                let save_failed = matches!(output.status, OutputSavePhase::SaveFailed { .. });
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
                } else if save_failed {
                    output.status = OutputSavePhase::SaveFailed {
                        saved_end: output.saved_end,
                    };
                } else {
                    output.status = OutputSavePhase::Open {
                        saved_end: output.saved_end,
                    };
                }
            }
            Ok(_) => {
                published = Err(internal("history owner returned a mismatched suffix range"));
                output.failed = true;
                reservation.cancel_output_save();
                output.status = OutputSavePhase::SaveFailed {
                    saved_end: output.saved_end,
                };
            }
            Err(_) => {
                output.failed = true;
                reservation.cancel_output_save();
                output.status = OutputSavePhase::SaveFailed {
                    saved_end: output.saved_end,
                };
            }
        }
    }
    drop(intent);
    if terminal {
        reservation.mark_durable_terminal();
        {
            let mut state = shared.state();
            state.finish_admission_if_resolved(&reservation);
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
    input: &SuffixInput,
) -> Result<(), ServiceError> {
    if input.attempt_id != output.committed.attempt_id
        || input.operation_generation != output.committed.operation_generation
        || input.owner_epoch != output.committed.owner_epoch
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
