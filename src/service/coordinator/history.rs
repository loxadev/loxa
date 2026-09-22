#[cfg(test)]
use super::state::AdmissionClaim;
use super::state::{AdmissionRecoveryAction, AdmissionReservation, CancellationCause};
use super::{history_error, Coordinator};
use crate::history::{
    AdmissionKind, CommittedAdmission, ExecutionOutcome, FinalizationInput, HistoryError,
    HistoryErrorKind, PreparedAdmission, PromptBasis, SuffixInput,
};
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;

mod output;
#[cfg(test)]
pub(in crate::service::coordinator) use output::OutputSavePhase;
pub(in crate::service::coordinator) use output::{
    AttemptMeasurements, GenerationOutput, OutputObserver, OutputState,
};

const MAX_ADMISSION_BACKING_BYTES: usize = 256 * 1024;

#[derive(Clone)]
pub(super) struct AdmissionInput {
    pub(super) conversation_id: [u8; 16],
    pub(super) submission_id: [u8; 16],
    pub(super) expected_conversation_revision: i64,
    pub(super) expected_profile_revision: i64,
    #[cfg(test)]
    pub(super) submission_hash: Option<[u8; 32]>,
    pub(super) effective_context: Option<u32>,
    pub(super) system_instruction: String,
    pub(super) max_output_tokens: i64,
    pub(super) prompt_basis: PromptBasis,
    pub(super) kind: AdmissionKind,
}

impl AdmissionInput {
    fn validate_backing(&self) -> Result<(), loxa_ipc::ServiceError> {
        let kind = match &self.kind {
            AdmissionKind::Send { user_text, .. } => user_text.capacity(),
            AdmissionKind::Retry { .. } => 0,
        };
        let retained = std::mem::size_of::<Self>()
            .saturating_add(self.system_instruction.capacity())
            .saturating_add(
                self.prompt_basis
                    .references
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::history::PromptReference>()),
            )
            .saturating_add(kind);
        if retained > MAX_ADMISSION_BACKING_BYTES {
            return Err(loxa_ipc::ServiceError::new(
                loxa_ipc::ErrorCategory::InvalidRequest,
                "history admission backing exceeds 256 KiB",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn semantic_hash(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"loxa-history-admission-v1\0");
        hash.update(self.conversation_id);
        hash.update(self.expected_conversation_revision.to_le_bytes());
        hash.update(self.expected_profile_revision.to_le_bytes());
        hash.update(self.max_output_tokens.to_le_bytes());
        update_bytes(&mut hash, self.system_instruction.as_bytes());
        hash.update((self.prompt_basis.references.len() as u64).to_le_bytes());
        for reference in &self.prompt_basis.references {
            hash.update(reference.turn_id);
            match reference.attempt_id {
                Some(attempt_id) => {
                    hash.update([1]);
                    hash.update(attempt_id);
                }
                None => hash.update([0]),
            }
            hash.update(reference.prefix_end.to_le_bytes());
        }
        match &self.kind {
            AdmissionKind::Send { user_text, draft } => {
                hash.update([0]);
                update_bytes(&mut hash, user_text.as_bytes());
                match draft {
                    Some(draft) => {
                        hash.update([1]);
                        hash.update(draft.id);
                        hash.update(draft.desktop_client_id);
                        hash.update(draft.revision.to_le_bytes());
                    }
                    None => hash.update([0]),
                }
            }
            AdmissionKind::Retry { prior_attempt_id } => {
                hash.update([1]);
                hash.update(prior_attempt_id);
            }
        }
        hash.finalize().into()
    }
}

#[cfg(test)]
fn update_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}

pub(super) type AdmissionResult = Result<CommittedAdmission, loxa_ipc::ServiceError>;
pub(super) type AdmissionObserver = watch::Receiver<Option<AdmissionResult>>;

impl Coordinator {
    #[cfg(test)]
    pub(super) async fn admit_history_generation(
        &self,
        input: AdmissionInput,
    ) -> Result<AdmissionObserver, loxa_ipc::ServiceError> {
        input.validate_backing()?;
        let submission_hash = input
            .submission_hash
            .unwrap_or_else(|| input.semantic_hash());
        if let Some(reservation) = self
            .shared
            .state()
            .matching_admission(input.submission_id, submission_hash)?
        {
            let observer = reservation.subscribe();
            maybe_resume_admission(Arc::clone(&self.shared), reservation);
            return Ok(observer);
        }
        #[cfg(test)]
        wait_at_barrier(&self.shared.admission_pre_lookup_barrier);
        if let Some(committed) = self
            .shared
            .history
            .lookup_submission(input.submission_id, submission_hash)
            .await
            .map_err(history_error)?
        {
            if let Some(reservation) = self
                .shared
                .state()
                .matching_admission(input.submission_id, submission_hash)?
            {
                let observer = reservation.subscribe();
                maybe_resume_admission(Arc::clone(&self.shared), reservation);
                return Ok(observer);
            }
            let (_sender, receiver) = watch::channel(Some(Ok(committed)));
            return Ok(receiver);
        }
        #[cfg(test)]
        wait_at_lookup_barrier(&self.shared.admission_lookup_barrier);
        let reservation = match self.shared.state().reserve_admission(
            input.conversation_id,
            input.submission_id,
            submission_hash,
            input.expected_conversation_revision,
            input.expected_profile_revision,
        )? {
            AdmissionClaim::Existing(reservation) => {
                let observer = reservation.subscribe();
                maybe_resume_admission(Arc::clone(&self.shared), Arc::clone(&reservation));
                return Ok(observer);
            }
            AdmissionClaim::Fresh(reservation) => reservation,
        };
        self.dispatch_reserved_admission(input, submission_hash, reservation)
    }

    pub(super) fn dispatch_reserved_admission(
        &self,
        input: AdmissionInput,
        submission_hash: [u8; 32],
        reservation: Arc<AdmissionReservation>,
    ) -> Result<AdmissionObserver, loxa_ipc::ServiceError> {
        if let Err(error) = input.validate_backing() {
            reservation.publish(Err(error.clone()));
            self.shared.state().finish_admission(&reservation);
            maybe_begin_history_drain(&self.shared);
            return Err(error);
        }
        let observer = reservation.subscribe();
        let prepared = match PreparedAdmission::new(
            input.conversation_id,
            input.submission_id,
            submission_hash,
            input.expected_conversation_revision,
            input.expected_profile_revision,
            self.shared.boot_epoch.clone(),
            reservation.operation_generation,
            Arc::clone(&reservation.fingerprint),
            self.shared.runtime_identity,
            input
                .effective_context
                .unwrap_or_else(|| reservation.fingerprint.effective_context()),
            input.system_instruction,
            input.max_output_tokens,
            input.prompt_basis,
            input.kind,
        ) {
            Ok(prepared) => Arc::new(prepared),
            Err(error) => {
                let mapped = history_error(error);
                reservation.publish(Err(mapped.clone()));
                self.shared.state().finish_admission(&reservation);
                maybe_begin_history_drain(&self.shared);
                return Err(mapped);
            }
        };
        reservation.retain_prepared(Arc::clone(&prepared));

        #[cfg(test)]
        self.wait_at_admission_dispatch_barrier();

        let completion = match self.shared.history.try_admit(prepared) {
            Ok(completion) => completion,
            Err(error) => {
                let mapped = history_error(error);
                reservation.publish(Err(mapped.clone()));
                self.shared.state().finish_admission(&reservation);
                maybe_begin_history_drain(&self.shared);
                return Err(mapped);
            }
        };
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            match completion.await {
                Ok(completion) => {
                    let result = completion.result;
                    drop(completion.permit);
                    #[cfg(test)]
                    wait_at_barrier(&shared.admission_completion_barrier);
                    #[cfg(all(test, target_os = "macos"))]
                    super::native_test_gate::pause_once(&shared.native_admission_completion_gate)
                        .await;
                    resolve_completion(&shared, &reservation, result).await;
                }
                Err(_) => reservation.admission_unknown(),
            }
            maybe_begin_history_drain(&shared);
        });
        Ok(observer)
    }

    pub(super) fn generation_output(
        &self,
        committed: &CommittedAdmission,
    ) -> Result<GenerationOutput, loxa_ipc::ServiceError> {
        let reservation = self.shared.state().current_admission().ok_or_else(|| {
            loxa_ipc::ServiceError::new(
                loxa_ipc::ErrorCategory::Conflict,
                "admitted generation output is no longer active",
            )
        })?;
        let output = reservation.output.lock().map_err(|_| {
            loxa_ipc::ServiceError::new(
                loxa_ipc::ErrorCategory::Internal,
                "output persistence lock is poisoned",
            )
        })?;
        let exact = output
            .as_ref()
            .is_some_and(|output| output.matches(committed));
        drop(output);
        if !exact {
            return Err(loxa_ipc::ServiceError::new(
                loxa_ipc::ErrorCategory::Conflict,
                "admitted generation identity changed",
            ));
        }
        Ok(GenerationOutput::new(Arc::clone(&self.shared), reservation))
    }
}

#[cfg(test)]
fn wait_at_barrier(slot: &std::sync::Mutex<Option<Arc<std::sync::Barrier>>>) {
    let barrier = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(barrier) = barrier {
        barrier.wait();
        barrier.wait();
    }
}

#[cfg(test)]
fn wait_at_lookup_barrier(slot: &std::sync::Mutex<Option<(Arc<std::sync::Barrier>, usize)>>) {
    let barrier = {
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((barrier, remaining)) = slot.as_mut() else {
            return;
        };
        let barrier = Arc::clone(barrier);
        *remaining -= 1;
        if *remaining == 0 {
            *slot = None;
        }
        barrier
    };
    barrier.wait();
}

#[cfg(test)]
impl Coordinator {
    fn wait_at_admission_dispatch_barrier(&self) {
        wait_at_barrier(&self.shared.admission_dispatch_barrier);
    }
}

#[cfg(test)]
mod tests;

async fn resolve_completion(
    shared: &Arc<super::Shared>,
    reservation: &Arc<AdmissionReservation>,
    result: Result<CommittedAdmission, HistoryError>,
) {
    let committed = match result {
        Ok(committed) => committed,
        Err(error) if error.kind() == HistoryErrorKind::OutcomeUnknown => {
            reservation.admission_unknown();
            return;
        }
        Err(error) => {
            reservation.publish(Err(history_error(error)));
            shared.state().finish_admission(reservation);
            return;
        }
    };
    resolve_committed(shared, reservation, committed).await;
}

async fn resolve_committed(
    shared: &Arc<super::Shared>,
    reservation: &Arc<AdmissionReservation>,
    committed: CommittedAdmission,
) {
    let exact = committed.conversation_id == reservation.conversation_id
        && committed.submission_id == reservation.submission_id
        && committed.pre_conversation_revision == reservation.expected_conversation_revision
        && committed.profile_revision == reservation.expected_profile_revision
        && committed.owner_epoch == shared.boot_epoch
        && committed.operation_generation == reservation.operation_generation;
    let terminal = {
        let state = shared.state();
        if !exact || !state.admission_is_current(reservation) {
            reservation.admission_unknown();
            return;
        }
        reservation.mark_admission_accepted();
        let cancelled = reservation.is_cancelled() || shared.draining.load(Ordering::Acquire);
        if !cancelled && !reservation.install_output(committed.clone()) {
            reservation.admission_unknown();
            return;
        }
        cancelled.then(|| cancelled_terminal(reservation, &committed))
    };
    if let Some(terminal) = terminal {
        reservation.retain_stop(committed.clone(), Arc::clone(&terminal));
        resolve_stop(shared, reservation, committed, terminal).await;
    } else {
        #[cfg(test)]
        wait_at_barrier(&shared.output_handoff_barrier);
        reservation.publish(Ok(committed));
    }
}

async fn resolve_stop(
    shared: &Arc<super::Shared>,
    reservation: &Arc<AdmissionReservation>,
    committed: CommittedAdmission,
    terminal: Arc<FinalizationInput>,
) {
    if persist_cancelled_admission(shared, Arc::clone(&terminal))
        .await
        .is_none()
    {
        reservation.stop_unknown();
        return;
    }
    reservation.mark_durable_terminal();
    shared.state().finish_admission_if_resolved(reservation);
    reservation.publish(Ok(committed));
}

async fn persist_cancelled_admission(
    shared: &super::Shared,
    terminal: Arc<FinalizationInput>,
) -> Option<()> {
    let completion = shared.history.try_finalize(terminal).ok()?.await.ok()?;
    let result = completion.result;
    drop(completion.permit);
    result.ok().map(|_| ())
}

fn cancelled_terminal(
    reservation: &AdmissionReservation,
    committed: &CommittedAdmission,
) -> Arc<FinalizationInput> {
    let failed = reservation.cancellation_cause() == Some(CancellationCause::EngineFailure);
    Arc::new(FinalizationInput {
        suffix: SuffixInput {
            attempt_id: committed.attempt_id,
            owner_epoch: committed.owner_epoch.clone(),
            operation_generation: committed.operation_generation,
            expected_saved_end: 0,
            content: String::new(),
        },
        execution_outcome: if failed {
            ExecutionOutcome::Failed
        } else {
            ExecutionOutcome::Stopped
        },
        generated_end: 0,
        failure_code: Some(
            if failed {
                "engine_transport"
            } else {
                "stopped"
            }
            .into(),
        ),
        statistics: Some(crate::history::AttemptStatistics {
            qualified_input_tokens: None,
            qualified_output_tokens: None,
            service_first_output_latency_ms: None,
            qualified_engine_decode_tokens_per_second: None,
            service_total_duration_ms: duration_ms(reservation.accepted_elapsed()),
            stop_reason: if failed {
                loxa_ipc::AttemptStopReason::Failure
            } else {
                loxa_ipc::AttemptStopReason::UserStop
            },
        }),
    })
}

fn duration_ms(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn maybe_resume_admission(
    shared: Arc<super::Shared>,
    reservation: Arc<AdmissionReservation>,
) {
    let Some(action) = reservation.take_recovery_action() else {
        return;
    };
    tokio::spawn(async move {
        match action {
            AdmissionRecoveryAction::Lookup(prepared) => {
                let completion = shared
                    .history
                    .try_reconcile_submission(prepared.submission_id(), prepared.submission_hash());
                match completion {
                    Ok(completion) => match completion.await {
                        Ok(Ok(Some(committed))) => {
                            resolve_committed(&shared, &reservation, committed).await
                        }
                        Ok(Ok(None)) => {
                            reservation.publish(Err(loxa_ipc::ServiceError::new(
                                loxa_ipc::ErrorCategory::ServiceUnavailable,
                                "history admission did not commit; retry the exact submission",
                            )));
                            shared.state().finish_admission(&reservation);
                        }
                        Ok(Err(_)) | Err(_) => reservation.admission_unknown(),
                    },
                    Err(_) => reservation.admission_unknown(),
                }
            }
            AdmissionRecoveryAction::Stop(_prepared, committed, terminal) => {
                resolve_stop(&shared, &reservation, committed, terminal).await;
            }
        }
        maybe_begin_history_drain(&shared);
    });
}

pub(super) fn maybe_begin_history_drain(shared: &Arc<super::Shared>) {
    if shared.draining.load(Ordering::Acquire) && shared.state().history_close_is_safe() {
        shared.history.begin_drain();
    }
}
