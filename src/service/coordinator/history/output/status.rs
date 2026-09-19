use super::{OutputSavePhase, OutputState};
use crate::history::ExecutionOutcome;
use crate::service::coordinator::{state::CancellationCause, Coordinator};
use loxa_ipc::{
    ErrorCategory, GenerationExecutionPhase, GenerationSavePhase, GenerationStatus,
    GenerationTarget, ServiceError,
};

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
        let cause = reservation.cancellation_cause();
        let execution = match &output.execution {
            Some(_) if !reservation.engine_is_quiescent() => GenerationExecutionPhase::Finalizing,
            Some(fact) => match fact.outcome {
                ExecutionOutcome::Completed => GenerationExecutionPhase::Completed,
                ExecutionOutcome::Stopped => GenerationExecutionPhase::Stopped,
                ExecutionOutcome::Failed => GenerationExecutionPhase::Failed,
            },
            None if cause.is_some() => GenerationExecutionPhase::Cancelling,
            None => GenerationExecutionPhase::Working,
        };
        let failure_code = output.execution.as_ref().map_or_else(
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
        Ok(Some(GenerationStatus {
            target: target.clone(),
            attempt_id: crate::history::encode_id(output.committed.attempt_id),
            execution,
            save: output.save_phase(),
            saved_end: output.saved_end.to_string(),
            generated_end: output
                .execution
                .as_ref()
                .map(|fact| fact.generated_end.to_string()),
            terminal_saved_end: output.closed.then(|| output.saved_end.to_string()),
            failure_code,
        }))
    }
}

impl OutputState {
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
