use super::history::{AdmissionInput, AdmissionObserver};
use super::state::{AdmissionClaim, AdmissionReservation};
use super::{history_error, Coordinator, PendingGenerationConnection};
use crate::history::{
    decode_id, encode_id, parse_revision, AdmissionKind, CommittedAdmission, DraftSubmission,
    PromptPreparation,
};
use loxa_ipc::{
    ErrorCategory, GenerationAccepted, GenerationCommand, GenerationReply, ServiceError,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::oneshot;

#[cfg(all(test, target_os = "macos"))]
mod native_acceptance;
pub(super) mod parser;
pub(super) mod persistence;
mod preflight;
#[cfg(test)]
mod qualification_fixture;
mod relay;
mod transport;

#[cfg(all(test, target_os = "macos"))]
pub(in crate::service) use native_acceptance::run_bundled_generation_acceptance;

impl Coordinator {
    pub(in crate::service) async fn generation_send(
        &self,
        command: GenerationCommand,
        pending: PendingGenerationConnection,
    ) -> Result<GenerationReply, ServiceError> {
        let submitted = match SubmittedSend::parse(command) {
            Ok(submitted) => submitted,
            Err(error) => {
                self.finish_generation_connection(&pending);
                return Err(error);
            }
        };
        let binding = {
            let state = self.shared.state();
            state.bind_pending_generation(
                &pending.pending,
                submitted.submission_id,
                submitted.submission_hash,
            )
        };
        if let Err(error) = binding {
            self.finish_generation_connection(&pending);
            return Err(error);
        }

        let coordinator = self.clone();
        let (reply, completion) = oneshot::channel();
        tokio::spawn(async move {
            let result = drive_send(&coordinator, submitted, pending).await;
            let _ = reply.send(result);
        });
        completion.await.map_err(|_| {
            ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation owner stopped before resolving Send",
            )
        })?
    }
}

struct SubmittedSend {
    conversation_id: [u8; 16],
    submission_id: [u8; 16],
    expected_conversation_revision: i64,
    expected_profile_revision: i64,
    user_text: String,
    draft: Option<DraftSubmission>,
    submission_hash: [u8; 32],
}

impl SubmittedSend {
    fn parse(command: GenerationCommand) -> Result<Self, ServiceError> {
        let GenerationCommand::Send {
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            user_text,
            draft,
        } = command
        else {
            return Err(invalid("generation connection expected Send"));
        };
        let conversation_id = decode_id(&conversation_id).map_err(history_error)?;
        let submission_id = decode_id(&submission_id).map_err(history_error)?;
        let expected_conversation_revision =
            parse_revision(&expected_conversation_revision).map_err(history_error)?;
        let expected_profile_revision =
            parse_revision(&expected_profile_revision).map_err(history_error)?;
        let draft = draft
            .map(|draft| {
                Ok(DraftSubmission {
                    id: decode_id(&draft.id).map_err(history_error)?,
                    desktop_client_id: decode_id(&draft.desktop_client_id)
                        .map_err(history_error)?,
                    revision: parse_nonnegative(&draft.revision)?,
                })
            })
            .transpose()?;
        let submission_hash = stable_submission_hash(
            conversation_id,
            expected_conversation_revision,
            expected_profile_revision,
            &user_text,
            draft.as_ref(),
        );
        Ok(Self {
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            user_text,
            draft,
            submission_hash,
        })
    }
}

async fn drive_send(
    coordinator: &Coordinator,
    submitted: SubmittedSend,
    pending: PendingGenerationConnection,
) -> Result<GenerationReply, ServiceError> {
    let lookup = coordinator
        .shared
        .history
        .lookup_submission(submitted.submission_id, submitted.submission_hash)
        .await
        .map_err(history_error);
    match lookup {
        Ok(Some(committed)) => {
            let reply =
                accepted_after_reconciliation(coordinator, committed, submitted.submission_hash)
                    .await;
            coordinator.finish_generation_connection(&pending);
            return reply;
        }
        Err(error) => {
            coordinator.finish_generation_connection(&pending);
            return Err(error);
        }
        Ok(None) => {}
    }

    let claim = {
        let mut state = coordinator.shared.state();
        state.reserve_pending_admission(
            &pending.pending,
            submitted.conversation_id,
            submitted.submission_id,
            submitted.submission_hash,
            submitted.expected_conversation_revision,
            submitted.expected_profile_revision,
        )
    }?;
    let reservation = match claim {
        AdmissionClaim::Existing(reservation) => {
            super::history::maybe_resume_admission(
                Arc::clone(&coordinator.shared),
                Arc::clone(&reservation),
            );
            let committed = wait_for_admission(reservation.subscribe()).await?;
            return Ok(accepted(&committed));
        }
        AdmissionClaim::Fresh(reservation) => reservation,
    };

    let prompt = match coordinator
        .shared
        .history
        .prepare_prompt(
            submitted.conversation_id,
            submitted.expected_conversation_revision,
            submitted.expected_profile_revision,
            submitted.user_text.clone(),
        )
        .await
    {
        Ok(completion) => {
            let result = completion.result.map_err(history_error);
            drop(completion.permit);
            result
        }
        Err(error) => Err(history_error(error)),
    };
    let prompt = match prompt {
        Ok(prompt) => prompt,
        Err(error) => return fail_before_admission(coordinator, &reservation, error),
    };
    if reservation.is_cancelled() {
        return fail_before_admission(
            coordinator,
            &reservation,
            ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation was stopped during prompt preparation",
            ),
        );
    }
    if prompt.model_id != reservation.fingerprint.model_id() {
        return fail_before_admission(
            coordinator,
            &reservation,
            ServiceError::new(
                ErrorCategory::Conflict,
                "conversation model does not match the ready runtime",
            ),
        );
    }

    let qualified = match preflight::qualify(
        coordinator.shared.runtime_identity,
        &reservation,
        &prompt,
    )
    .await
    {
        Ok(qualified) => qualified,
        Err(error) => return fail_before_admission(coordinator, &reservation, error),
    };
    if reservation.is_cancelled() {
        return fail_before_admission(
            coordinator,
            &reservation,
            ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation was stopped before history admission",
            ),
        );
    }

    let PromptPreparation {
        model_id,
        system_instruction,
        max_output_tokens,
        basis,
        messages,
    } = prompt;
    drop(model_id);
    drop(messages);
    let input = AdmissionInput {
        conversation_id: submitted.conversation_id,
        submission_id: submitted.submission_id,
        expected_conversation_revision: submitted.expected_conversation_revision,
        expected_profile_revision: submitted.expected_profile_revision,
        #[cfg(test)]
        submission_hash: Some(submitted.submission_hash),
        effective_context: Some(qualified.actual_context()),
        system_instruction,
        max_output_tokens,
        prompt_basis: basis,
        kind: AdmissionKind::Send {
            user_text: submitted.user_text,
            draft: submitted.draft,
        },
    };
    let observer = match coordinator.dispatch_reserved_admission(
        input,
        submitted.submission_hash,
        Arc::clone(&reservation),
    ) {
        Ok(observer) => observer,
        Err(error) => return fail_before_admission(coordinator, &reservation, error),
    };
    let committed = wait_for_admission(observer).await?;
    tokio::spawn(relay::run(
        coordinator.clone(),
        Arc::clone(&reservation),
        committed.clone(),
        qualified,
    ));
    Ok(accepted(&committed))
}

async fn accepted_after_reconciliation(
    coordinator: &Coordinator,
    committed: CommittedAdmission,
    submission_hash: [u8; 32],
) -> Result<GenerationReply, ServiceError> {
    let reservation = coordinator
        .shared
        .state()
        .matching_admission(committed.submission_id, submission_hash)?;
    let Some(reservation) = reservation else {
        return Ok(accepted(&committed));
    };
    super::history::maybe_resume_admission(
        Arc::clone(&coordinator.shared),
        Arc::clone(&reservation),
    );
    let resolved = wait_for_admission(reservation.subscribe()).await?;
    Ok(accepted(&resolved))
}

async fn wait_for_admission(
    mut observer: AdmissionObserver,
) -> Result<CommittedAdmission, ServiceError> {
    loop {
        if let Some(result) = observer.borrow().clone() {
            return result;
        }
        observer.changed().await.map_err(|_| {
            ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "generation admission owner stopped before publishing an outcome",
            )
        })?;
    }
}

fn fail_before_admission(
    coordinator: &Coordinator,
    reservation: &Arc<AdmissionReservation>,
    error: ServiceError,
) -> Result<GenerationReply, ServiceError> {
    reservation.publish(Err(error.clone()));
    coordinator.shared.state().finish_admission(reservation);
    super::history::maybe_begin_history_drain(&coordinator.shared);
    Err(error)
}

fn accepted(committed: &CommittedAdmission) -> GenerationReply {
    GenerationReply::Accepted(GenerationAccepted {
        boot_epoch: committed.owner_epoch.clone(),
        conversation_id: encode_id(committed.conversation_id),
        turn_id: encode_id(committed.turn_id),
        attempt_id: encode_id(committed.attempt_id),
        submission_id: encode_id(committed.submission_id),
        pre_conversation_revision: committed.pre_conversation_revision.to_string(),
        post_conversation_revision: committed.post_conversation_revision.to_string(),
        profile_revision: committed.profile_revision.to_string(),
        operation_generation: committed.operation_generation.to_string(),
    })
}

pub(super) fn stable_submission_hash(
    conversation_id: [u8; 16],
    expected_conversation_revision: i64,
    expected_profile_revision: i64,
    user_text: &str,
    draft: Option<&DraftSubmission>,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"loxa-generation-send-v1\0");
    hash.update(conversation_id);
    hash.update(expected_conversation_revision.to_le_bytes());
    hash.update(expected_profile_revision.to_le_bytes());
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
    hash.finalize().into()
}

fn update_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}

fn parse_nonnegative(value: &str) -> Result<i64, ServiceError> {
    if value.is_empty()
        || value.len() > 19
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("invalid draft revision"));
    }
    value
        .parse::<i64>()
        .map_err(|_| invalid("invalid draft revision"))
}

fn invalid(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::InvalidRequest, context)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash_depends_only_on_submitted_fields() {
        let original = stable_submission_hash([1; 16], 2, 3, "hello", None);
        assert_eq!(
            original,
            stable_submission_hash([1; 16], 2, 3, "hello", None)
        );
        assert_ne!(
            original,
            stable_submission_hash([1; 16], 2, 3, "changed", None)
        );
    }

    #[test]
    fn retained_acceptance_preserves_the_admitting_boot_epoch() {
        let committed = CommittedAdmission {
            conversation_id: [1; 16],
            turn_id: [2; 16],
            attempt_id: [3; 16],
            submission_id: [4; 16],
            pre_conversation_revision: 7,
            post_conversation_revision: 8,
            profile_revision: 9,
            owner_epoch: "prior-service-boot".into(),
            operation_generation: 10,
        };
        let GenerationReply::Accepted(reply) = accepted(&committed) else {
            panic!("retained generation did not return Accepted");
        };
        assert_eq!(reply.boot_epoch, "prior-service-boot");
        assert_eq!(reply.operation_generation, "10");
    }
}
