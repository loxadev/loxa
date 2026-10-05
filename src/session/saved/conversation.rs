use loxa_ipc::{
    ConnectMode, ConversationProfile, GenerationAccepted, GenerationCommand, GenerationSettings,
    GenerationSettingsPatch, GenerationTarget, HistoryCommand, HistoryReply, ServiceClient,
    ServiceSettingsCommand, ServiceSettingsReply,
};

pub(super) struct Conversation {
    pub(super) id: String,
    pub(super) revision: String,
    pub(super) profile_revision: String,
    pub(super) last: Option<AcceptedAttempt>,
}

#[derive(Clone)]
pub(super) struct AcceptedAttempt {
    pub(super) attempt_id: String,
    pub(super) target: GenerationTarget,
    pub(super) raw_cursor: u64,
}

impl Conversation {
    pub(super) async fn create(
        client: &ServiceClient,
        model_id: &str,
        max_tokens: Option<u32>,
    ) -> Result<Self, String> {
        let summary = match client
            .history_request(
                ConnectMode::ObserveExisting,
                HistoryCommand::CreateConversation {
                    model_id: model_id.into(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
        {
            HistoryReply::Conversation(summary) => summary,
            _ => return Err("service returned the wrong conversation reply".into()),
        };
        let mut conversation = Self {
            id: summary.id,
            revision: summary.revision,
            profile_revision: summary.profile_revision,
            last: None,
        };
        if let Some(max_output_tokens) = max_tokens {
            let profile = match client
                .settings_request(
                    ConnectMode::ObserveExisting,
                    ServiceSettingsCommand::PatchConversationProfile {
                        conversation_id: conversation.id.clone(),
                        expected_conversation_revision: conversation.revision.clone(),
                        expected_profile_revision: conversation.profile_revision.clone(),
                        patch: GenerationSettingsPatch::Fields {
                            system_instruction: None,
                            max_output_tokens: Some(max_output_tokens),
                            temperature: None,
                            top_p: None,
                        },
                    },
                )
                .await
                .map_err(|error| error.to_string())?
            {
                ServiceSettingsReply::Conversation(profile) => profile,
                _ => return Err("service returned the wrong conversation profile reply".into()),
            };
            conversation.apply_profile(&profile)?;
        }
        Ok(conversation)
    }

    pub(super) async fn refresh_profile(
        &mut self,
        client: &ServiceClient,
    ) -> Result<GenerationSettings, String> {
        let profile = match client
            .settings_request(
                ConnectMode::ObserveExisting,
                ServiceSettingsCommand::GetConversationProfile {
                    conversation_id: self.id.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
        {
            ServiceSettingsReply::Conversation(profile) => profile,
            _ => return Err("service returned the wrong conversation profile reply".into()),
        };
        self.apply_profile(&profile)?;
        Ok(profile.generation)
    }

    pub(super) fn send_command(
        &self,
        submission_id: String,
        user_text: String,
    ) -> GenerationCommand {
        GenerationCommand::Send {
            conversation_id: self.id.clone(),
            submission_id,
            expected_conversation_revision: self.revision.clone(),
            expected_profile_revision: self.profile_revision.clone(),
            user_text,
            draft: None,
        }
    }

    pub(super) fn retry_command(&self, submission_id: String) -> Option<GenerationCommand> {
        Some(GenerationCommand::Retry {
            conversation_id: self.id.clone(),
            submission_id,
            expected_conversation_revision: self.revision.clone(),
            expected_profile_revision: self.profile_revision.clone(),
            prior_attempt_id: self.last.as_ref()?.attempt_id.clone(),
        })
    }

    pub(super) fn accept(&mut self, accepted: &GenerationAccepted) -> Result<(), String> {
        if accepted.conversation_id != self.id
            || accepted.pre_conversation_revision != self.revision
            || accepted.profile_revision != self.profile_revision
        {
            return Err("service accepted a different conversation revision".into());
        }
        self.revision
            .clone_from(&accepted.post_conversation_revision);
        self.last = Some(AcceptedAttempt {
            attempt_id: accepted.attempt_id.clone(),
            target: accepted.target(),
            raw_cursor: 0,
        });
        Ok(())
    }

    fn apply_profile(&mut self, profile: &ConversationProfile) -> Result<(), String> {
        if profile.conversation_id != self.id {
            return Err("service returned a different conversation profile".into());
        }
        self.revision.clone_from(&profile.conversation_revision);
        self.profile_revision.clone_from(&profile.profile_revision);
        Ok(())
    }
}

pub(super) fn submission_id() -> Result<String, String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| "could not create a generation submission identity".to_string())?;
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_keeps_the_payload_and_revisions_while_retry_targets_the_exact_attempt() {
        let mut conversation = Conversation {
            id: "11".repeat(16),
            revision: "1".into(),
            profile_revision: "2".into(),
            last: None,
        };
        let send = conversation.send_command("22".repeat(16), "hello".into());
        assert!(matches!(&send, GenerationCommand::Send {
            conversation_id, submission_id, expected_conversation_revision,
            expected_profile_revision, user_text, draft: None,
        } if conversation_id == &conversation.id
            && submission_id == &"22".repeat(16)
            && expected_conversation_revision == "1"
            && expected_profile_revision == "2"
            && user_text == "hello"));
        let accepted = GenerationAccepted {
            boot_epoch: "boot".into(),
            conversation_id: conversation.id.clone(),
            turn_id: "33".repeat(16),
            attempt_id: "44".repeat(16),
            submission_id: "22".repeat(16),
            pre_conversation_revision: "1".into(),
            post_conversation_revision: "2".into(),
            profile_revision: "2".into(),
            operation_generation: "3".into(),
        };
        conversation.accept(&accepted).unwrap();
        assert_eq!(
            conversation.last.as_ref().unwrap().target,
            accepted.target()
        );
        assert_eq!(conversation.last.as_ref().unwrap().raw_cursor, 0);
        assert!(matches!(conversation.retry_command("55".repeat(16)), Some(
            GenerationCommand::Retry {
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
                prior_attempt_id,
                ..
            }) if submission_id == "55".repeat(16)
                && expected_conversation_revision == "2"
                && expected_profile_revision == "2"
                && prior_attempt_id == accepted.attempt_id
        ));
        let mut wrong = accepted;
        wrong.pre_conversation_revision = "1".into();
        assert!(conversation.accept(&wrong).is_err());
    }
}
