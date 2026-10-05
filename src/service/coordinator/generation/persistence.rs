use super::super::history::{AttemptMeasurements, GenerationOutput, OutputObserver};
use crate::history::{
    CommittedAdmission, ExecutionOutcome, FinalizationInput, SuffixInput, MAX_SUFFIX_BYTES,
};
use loxa_ipc::{ErrorCategory, ServiceError};
use std::sync::Arc;
use std::time::Duration;

const MIN_RETRY_DELAY: Duration = Duration::from_millis(25);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(in crate::service::coordinator) struct OutputPipeline {
    output: GenerationOutput,
    committed: CommittedAdmission,
    enqueued_end: u64,
    checkpoint: Option<OutputObserver>,
    // OutputState.current and this tail are the only two canonical 64 KiB
    // backings. A decoded SSE event is separate parser-owned backing.
    retained_tail: Option<String>,
}

#[derive(Debug)]
pub(in crate::service::coordinator) enum SaveChunkFailure {
    Cancelled,
    Failed,
}

impl OutputPipeline {
    pub(in crate::service::coordinator) fn new(
        output: GenerationOutput,
        committed: CommittedAdmission,
    ) -> Self {
        Self {
            output,
            committed,
            enqueued_end: 0,
            checkpoint: None,
            retained_tail: None,
        }
    }

    pub(in crate::service::coordinator) fn save_chunk(
        &mut self,
        content: String,
    ) -> Result<(), SaveChunkFailure> {
        self.validate_chunk(&content).map_err(|_| {
            self.output.cancel_output_save();
            SaveChunkFailure::Failed
        })?;
        if self.retained_tail.is_some() {
            self.output.cancel_output_save();
            return Err(SaveChunkFailure::Failed);
        }
        if let Some(checkpoint) = self.checkpoint.as_mut() {
            let resolved = { checkpoint.borrow().clone() };
            match resolved {
                Some(Ok(_)) => self.checkpoint = None,
                Some(Err(error)) => {
                    self.retained_tail = Some(content);
                    self.output.cancel_output_save();
                    drop(error);
                    return Err(SaveChunkFailure::Failed);
                }
                None => {
                    self.retained_tail = Some(content);
                    self.output.report_capacity_saturation();
                    return Err(SaveChunkFailure::Failed);
                }
            }
        }
        self.enqueue_chunk(content, false)
            .map_err(|(_error, cancelled)| {
                if cancelled {
                    SaveChunkFailure::Cancelled
                } else {
                    self.output.cancel_output_save();
                    SaveChunkFailure::Failed
                }
            })
    }

    pub(super) fn checkpoint_pending(&self) -> bool {
        self.checkpoint.is_some()
    }

    pub(super) async fn wait_checkpoint(&mut self) -> Result<(), SaveChunkFailure> {
        // Keep the observer owned here when a body frame or Stop cancels this wait.
        let Some(observer) = self.checkpoint.as_mut() else {
            return std::future::pending().await;
        };
        loop {
            let resolved = { observer.borrow().clone() };
            match resolved {
                Some(Ok(_)) => {
                    self.checkpoint = None;
                    return Ok(());
                }
                Some(Err(_)) => break,
                None if observer.changed().await.is_err() => break,
                None => {}
            }
        }
        self.output.cancel_output_save();
        Err(SaveChunkFailure::Failed)
    }

    pub(in crate::service::coordinator) async fn finish(
        &mut self,
        decoder: &mut super::parser::SseDecoder,
        proposed_outcome: ExecutionOutcome,
        failure_code: Option<&'static str>,
        measurements: AttemptMeasurements,
    ) -> Result<ExecutionOutcome, ServiceError> {
        let generated_end = u64::try_from(decoder.generated_end()).unwrap_or(u64::MAX);
        let fact = self
            .output
            .publish_execution(proposed_outcome, failure_code, generated_end, measurements)
            .inspect_err(|_| self.output.cancel_output_save())?;
        self.settle_checkpoint().await;
        let final_suffix = loop {
            let content = self
                .retained_tail
                .take()
                .or_else(|| decoder.take_remaining_chunk());
            let Some(content) = content else {
                break String::new();
            };
            let end = self
                .enqueued_end
                .checked_add(content.len() as u64)
                .expect("generated output offset remains bounded");
            if end == generated_end {
                break content;
            }
            debug_assert!(end < generated_end);
            self.persist_terminal_chunk(content).await;
        };

        debug_assert_eq!(generated_end, self.enqueued_end + final_suffix.len() as u64);
        let input = Arc::new(FinalizationInput {
            suffix: SuffixInput {
                attempt_id: self.committed.attempt_id,
                owner_epoch: self.committed.owner_epoch.clone(),
                operation_generation: self.committed.operation_generation,
                expected_saved_end: self.enqueued_end,
                content: final_suffix,
            },
            execution_outcome: fact.outcome,
            generated_end: fact.generated_end,
            failure_code: fact.failure_code,
            statistics: Some(fact.statistics),
        });
        let mut delay = MIN_RETRY_DELAY;
        let (mut observer, selected_outcome) = loop {
            match self.output.finalize_generation_owned(Arc::clone(&input)) {
                Ok(accepted) => break accepted,
                Err((_error, _returned)) => {
                    self.output.cancel_output_save();
                    tokio::time::sleep(delay).await;
                    delay = next_delay(delay);
                }
            }
        };
        self.settle_observer(&mut observer).await;
        Ok(selected_outcome)
    }

    fn enqueue_chunk(
        &mut self,
        content: String,
        terminal: bool,
    ) -> Result<(), (ServiceError, bool)> {
        self.validate_chunk(&content)
            .map_err(|error| (error, false))?;
        let start = self.enqueued_end;
        let input = Arc::new(SuffixInput {
            attempt_id: self.committed.attempt_id,
            owner_epoch: self.committed.owner_epoch.clone(),
            operation_generation: self.committed.operation_generation,
            expected_saved_end: start,
            content,
        });
        let end = start
            .checked_add(input.content.len() as u64)
            .ok_or_else(|| (internal("generated output offset overflow"), false))?;
        if terminal {
            match self.output.checkpoint_terminal_owned(input) {
                Ok(observer) => {
                    self.enqueued_end = end;
                    self.checkpoint = Some(observer);
                    Ok(())
                }
                Err((error, returned)) => {
                    self.retained_tail = Some(recover_suffix(returned));
                    Err((error, false))
                }
            }
        } else {
            match self.output.checkpoint_generation_owned(input) {
                Ok(observer) => {
                    self.enqueued_end = end;
                    self.checkpoint = Some(observer);
                    Ok(())
                }
                Err((error, returned, cancelled)) => {
                    self.retained_tail = Some(recover_suffix(returned));
                    Err((error, cancelled))
                }
            }
        }
    }

    async fn persist_terminal_chunk(&mut self, content: String) {
        self.settle_checkpoint().await;
        let mut content = Some(content);
        let mut delay = MIN_RETRY_DELAY;
        loop {
            let owned = content.take().expect("terminal suffix remains owned");
            match self.enqueue_chunk(owned, true) {
                Ok(()) => {
                    self.settle_checkpoint().await;
                    return;
                }
                Err((_error, _cancelled)) => {
                    self.output.cancel_output_save();
                    content = self.retained_tail.take();
                    tokio::time::sleep(delay).await;
                    delay = next_delay(delay);
                    self.settle_checkpoint().await;
                }
            }
        }
    }

    async fn settle_checkpoint(&mut self) {
        let Some(mut observer) = self.checkpoint.take() else {
            return;
        };
        self.settle_observer(&mut observer).await;
    }

    async fn settle_observer(&self, observer: &mut OutputObserver) {
        let mut delay = MIN_RETRY_DELAY;
        loop {
            let result = loop {
                if let Some(result) = observer.borrow().clone() {
                    break result;
                }
                if observer.changed().await.is_err() {
                    break Err(unavailable(
                        "output persistence owner stopped before resolving a save",
                    ));
                }
            };
            if result.is_ok() {
                return;
            }
            self.output.cancel_output_save();
            tokio::time::sleep(delay).await;
            delay = next_delay(delay);
            if let Ok(retry) = self.output.retry_save() {
                *observer = retry;
            }
        }
    }

    fn validate_chunk(&self, content: &String) -> Result<(), ServiceError> {
        if content.is_empty() || content.capacity() > MAX_SUFFIX_BYTES {
            return Err(internal("generation produced an invalid checkpoint chunk"));
        }
        Ok(())
    }
}

fn next_delay(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(MAX_RETRY_DELAY)
}

fn recover_suffix(input: Arc<SuffixInput>) -> String {
    Arc::try_unwrap(input)
        .map(|input| input.content)
        .unwrap_or_else(|input| input.content.clone())
}

fn unavailable(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::ServiceUnavailable, context)
}

fn internal(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Internal, context)
}
