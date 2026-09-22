use super::{OutputSavePhase, OutputState};
use crate::history::ExecutionOutcome;
use crate::service::coordinator::{state::CancellationCause, Coordinator};
use loxa_ipc::{
    AttemptSummary, ErrorCategory, GenerationExecutionPhase, GenerationSavePhase, GenerationStatus,
    GenerationTarget, ServiceError,
};
use tokio::sync::OwnedSemaphorePermit;

impl Coordinator {
    pub(in crate::service) fn generation_status(
        &self,
        target: &GenerationTarget,
    ) -> Result<Option<GenerationStatus>, ServiceError> {
        loxa_ipc::ServiceCommand::GetGenerationStatus {
            target: target.clone(),
        }
        .validate_shape()
        .map_err(|message| ServiceError::new(ErrorCategory::InvalidRequest, message))?;
        let GenerationTarget::Accepted {
            boot_epoch,
            submission_id,
            operation_generation,
        } = target
        else {
            return Err(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "generation status requires an accepted target",
            ));
        };
        let state = self.shared.state();
        let Some(reservation) = state.current_admission() else {
            return Ok(None);
        };
        if boot_epoch != &self.shared.boot_epoch
            || submission_id != &crate::history::encode_id(reservation.submission_id)
            || operation_generation != &reservation.operation_generation.to_string()
        {
            return Ok(None);
        }
        let output = reservation.output.lock().map_err(|_| {
            ServiceError::new(
                ErrorCategory::Internal,
                "output persistence lock is poisoned",
            )
        })?;
        let Some(output) = output.as_ref() else {
            return Ok(None);
        };
        Ok(Some(output.status_snapshot(&reservation)))
    }

    pub(in crate::service) fn subscribe_generation_status(
        &self,
        target: &GenerationTarget,
        attempt_id: &str,
    ) -> Result<Option<tokio::sync::watch::Receiver<Option<GenerationStatus>>>, ServiceError> {
        let attempt_id = crate::history::decode_id(attempt_id).map_err(super::history_error)?;
        let GenerationTarget::Accepted {
            boot_epoch,
            submission_id,
            operation_generation,
        } = target
        else {
            return Err(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "generation observation requires an accepted target",
            ));
        };
        let state = self.shared.state();
        let Some(reservation) = state.current_admission() else {
            return Ok(None);
        };
        if boot_epoch != &self.shared.boot_epoch
            || submission_id != &crate::history::encode_id(reservation.submission_id)
            || operation_generation != &reservation.operation_generation.to_string()
        {
            return Ok(None);
        }
        let output = reservation.output.lock().map_err(|_| {
            ServiceError::new(
                ErrorCategory::Internal,
                "output persistence lock is poisoned",
            )
        })?;
        let Some(output) = output.as_ref() else {
            return Ok(None);
        };
        if output.committed.attempt_id != attempt_id {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "attempt does not identify the accepted generation",
            ));
        }
        Ok(Some(reservation.subscribe_generation_status()))
    }

    pub(in crate::service) async fn observed_attempt(
        &self,
        target: &GenerationTarget,
        attempt_id: &str,
    ) -> (
        Result<AttemptSummary, ServiceError>,
        Option<OwnedSemaphorePermit>,
    ) {
        let GenerationTarget::Accepted {
            boot_epoch,
            submission_id,
            operation_generation,
        } = target
        else {
            return (
                Err(ServiceError::new(
                    ErrorCategory::InvalidRequest,
                    "generation observation requires an accepted target",
                )),
                None,
            );
        };
        let observation = crate::history::ObservedAttempt {
            attempt_id: match crate::history::decode_id(attempt_id) {
                Ok(id) => id,
                Err(error) => return (Err(super::history_error(error)), None),
            },
            owner_epoch: boot_epoch.clone(),
            submission_id: match crate::history::decode_id(submission_id) {
                Ok(id) => id,
                Err(error) => return (Err(super::history_error(error)), None),
            },
            operation_generation: match crate::history::parse_revision(operation_generation) {
                Ok(generation) => generation,
                Err(error) => return (Err(super::history_error(error)), None),
            },
        };
        match self.shared.history.read_observed_attempt(observation).await {
            Ok(completion) => (
                completion.result.map_err(super::history_error),
                Some(completion.permit),
            ),
            Err(error) => (Err(super::history_error(error)), None),
        }
    }
}

impl OutputState {
    pub(in crate::service::coordinator) fn status_snapshot(
        &self,
        reservation: &crate::service::coordinator::state::AdmissionReservation,
    ) -> GenerationStatus {
        let target = GenerationTarget::Accepted {
            boot_epoch: self.committed.owner_epoch.clone(),
            submission_id: crate::history::encode_id(self.committed.submission_id),
            operation_generation: self.committed.operation_generation.to_string(),
        };
        let cause = reservation.cancellation_cause();
        let execution = match &self.execution {
            Some(_) if !reservation.engine_is_quiescent() => GenerationExecutionPhase::Finalizing,
            Some(fact) => match fact.outcome {
                ExecutionOutcome::Completed => GenerationExecutionPhase::Completed,
                ExecutionOutcome::Stopped => GenerationExecutionPhase::Stopped,
                ExecutionOutcome::Failed => GenerationExecutionPhase::Failed,
            },
            None if cause.is_some() => GenerationExecutionPhase::Cancelling,
            None => GenerationExecutionPhase::Working,
        };
        let failure_code = self.execution.as_ref().map_or_else(
            || {
                cause.map(|cause| {
                    match cause {
                        CancellationCause::Requested => "stopped",
                        CancellationCause::OutputSave => "output_save",
                        CancellationCause::EngineFailure => "engine_transport",
                    }
                    .to_owned()
                })
            },
            |fact| fact.failure_code.clone(),
        );
        GenerationStatus {
            target,
            attempt_id: crate::history::encode_id(self.committed.attempt_id),
            execution,
            save: self.save_phase(),
            saved_end: self.saved_end.to_string(),
            generated_end: self
                .execution
                .as_ref()
                .map(|fact| fact.generated_end.to_string()),
            terminal_saved_end: self.closed.then(|| self.saved_end.to_string()),
            failure_code,
        }
    }

    fn save_phase(&self) -> GenerationSavePhase {
        match self.status {
            OutputSavePhase::Saved { .. } => GenerationSavePhase::Saved,
            OutputSavePhase::SaveFailed { .. } => GenerationSavePhase::SaveFailed,
            _ if self.failed => GenerationSavePhase::SaveFailed,
            OutputSavePhase::Saving { .. } => GenerationSavePhase::Saving,
            OutputSavePhase::Open { .. } if self.execution.is_some() => GenerationSavePhase::Saving,
            OutputSavePhase::Open { .. } => GenerationSavePhase::Open,
        }
    }
}
