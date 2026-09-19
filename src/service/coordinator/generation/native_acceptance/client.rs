use super::MODEL_ID;
use loxa_ipc::{
    AttemptExecution, AttemptSave, ClientError, ConnectMode, ContentSource, DraftCommand,
    DraftReply, DraftSnapshot, ErrorCategory, GenerationAccepted, GenerationCommand,
    GenerationDraft, GenerationReply, GenerationSettingsPatch, HistoryCommand, HistoryReply,
    ServiceClient, ServiceSettingsCommand, ServiceSettingsReply,
};
use std::time::Duration;

const WAIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(super) struct Conversation {
    pub(super) id: String,
    revision: String,
    profile_revision: String,
}

pub(super) async fn create_conversation(
    client: &ServiceClient,
    max_output_tokens: u32,
) -> Result<Conversation, String> {
    let conversation = match client
        .history_request(
            ConnectMode::ObserveExisting,
            HistoryCommand::CreateConversation {
                model_id: MODEL_ID.into(),
            },
        )
        .await
        .map_err(client_error)?
    {
        HistoryReply::Conversation(conversation) => conversation,
        _ => return Err("native conversation creation returned the wrong reply".into()),
    };
    let profile = match client
        .settings_request(
            ConnectMode::ObserveExisting,
            ServiceSettingsCommand::PatchConversationProfile {
                conversation_id: conversation.id,
                expected_conversation_revision: conversation.revision,
                expected_profile_revision: conversation.profile_revision,
                patch: GenerationSettingsPatch::Fields {
                    system_instruction: None,
                    max_output_tokens: Some(max_output_tokens),
                },
            },
        )
        .await
        .map_err(client_error)?
    {
        ServiceSettingsReply::Conversation(profile) => profile,
        _ => return Err("native conversation profile returned the wrong reply".into()),
    };
    Ok(Conversation {
        id: profile.conversation_id,
        revision: profile.conversation_revision,
        profile_revision: profile.profile_revision,
    })
}

pub(super) async fn create_draft(
    client: &ServiceClient,
    conversation: &Conversation,
    text: &str,
) -> Result<DraftSnapshot, String> {
    let desktop_client_id = conversation.id.clone();
    let draft = match client
        .draft_request(
            ConnectMode::ObserveExisting,
            DraftCommand::CreateScope {
                desktop_client_id: desktop_client_id.clone(),
                conversation_id: Some(conversation.id.clone()),
            },
        )
        .await
        .map_err(client_error)?
    {
        DraftReply::Snapshot(draft) => draft,
        _ => return Err("native draft creation returned the wrong reply".into()),
    };
    match client
        .draft_request(
            ConnectMode::ObserveExisting,
            DraftCommand::SaveSnapshot {
                draft_id: draft.id,
                desktop_client_id,
                revision: "1".into(),
                text: text.into(),
            },
        )
        .await
        .map_err(client_error)?
    {
        DraftReply::Snapshot(draft) => Ok(draft),
        _ => Err("native draft save returned the wrong reply".into()),
    }
}

pub(super) async fn read_draft(
    client: &ServiceClient,
    expected: &DraftSnapshot,
) -> Result<DraftSnapshot, String> {
    match client
        .draft_request(
            ConnectMode::ObserveExisting,
            DraftCommand::ReadScope {
                draft_id: expected.id.clone(),
                desktop_client_id: expected.desktop_client_id.clone(),
            },
        )
        .await
        .map_err(client_error)?
    {
        DraftReply::Snapshot(draft) => Ok(draft),
        _ => Err("native draft read returned the wrong reply".into()),
    }
}

pub(super) fn require_same_draft(
    before: &DraftSnapshot,
    after: &DraftSnapshot,
) -> Result<(), String> {
    if before.id == after.id
        && before.revision == after.revision
        && before.consumed_revision == after.consumed_revision
        && before.text == after.text
    {
        Ok(())
    } else {
        Err("rejected native generation changed its draft content or revision".into())
    }
}

pub(super) async fn send(
    client: ServiceClient,
    conversation: Conversation,
    user_text: &str,
    draft: Option<&DraftSnapshot>,
    submission: u8,
) -> Result<GenerationAccepted, ClientError> {
    let pending = client
        .prepare_generation(ConnectMode::ObserveExisting)
        .await?;
    let reply = pending
        .send(GenerationCommand::Send {
            conversation_id: conversation.id,
            submission_id: format!("{submission:02x}").repeat(16),
            expected_conversation_revision: conversation.revision,
            expected_profile_revision: conversation.profile_revision,
            user_text: user_text.into(),
            draft: draft.map(|draft| GenerationDraft {
                id: draft.id.clone(),
                desktop_client_id: draft.desktop_client_id.clone(),
                revision: draft.revision.clone(),
            }),
        })
        .await?;
    match reply {
        GenerationReply::Accepted(accepted) => Ok(accepted),
        GenerationReply::Stopping { .. } => Err(ClientError::Transport(
            "native generation Send returned Stopping".into(),
        )),
    }
}

pub(super) fn require_rejected(
    result: Result<GenerationAccepted, ClientError>,
    expected: ErrorCategory,
    label: &str,
) -> Result<(), String> {
    match result {
        Err(ClientError::Rejected(error)) if error.category == expected => Ok(()),
        Err(error) => Err(format!("{label} returned {error}")),
        Ok(_) => Err(format!("{label} was unexpectedly accepted")),
    }
}

pub(super) async fn wait_for_saved(
    client: &ServiceClient,
    conversation: &Conversation,
    accepted: &GenerationAccepted,
    label: &str,
) -> Result<String, String> {
    let attempt = wait_for_attempt(client, conversation, accepted, label).await?;
    if attempt.execution != AttemptExecution::Completed || attempt.save != AttemptSave::Saved {
        return Err(format!(
            "{label} native generation did not complete durably: {:?}/{:?}/{:?}",
            attempt.execution, attempt.save, attempt.failure_code
        ));
    }
    read_assistant(client, &attempt)
        .await
        .map_err(|error| format!("{label}: {error}"))
}

pub(super) async fn wait_for_stopped(
    client: &ServiceClient,
    conversation: &Conversation,
    accepted: &GenerationAccepted,
    label: &str,
) -> Result<String, String> {
    let attempt = wait_for_attempt(client, conversation, accepted, label).await?;
    if attempt.execution != AttemptExecution::Stopped
        || attempt.save != AttemptSave::Saved
        || attempt.failure_code.as_deref() != Some("stopped")
    {
        return Err(format!(
            "{label} native generation has the wrong terminal state: {:?}/{:?}/{:?}",
            attempt.execution, attempt.save, attempt.failure_code
        ));
    }
    read_assistant(client, &attempt)
        .await
        .map_err(|error| format!("{label}: {error}"))
}

async fn wait_for_attempt(
    client: &ServiceClient,
    conversation: &Conversation,
    accepted: &GenerationAccepted,
    label: &str,
) -> Result<loxa_ipc::AttemptSummary, String> {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let turns = list_turns(client, &conversation.id)
            .await
            .map_err(|error| format!("{label}: {error}"))?;
        let mut selected_attempts = 0usize;
        let mut matching = None;
        for attempt in turns.into_iter().filter_map(|turn| turn.selected_attempt) {
            selected_attempts += 1;
            if matching.is_none() && attempt.id == accepted.attempt_id {
                matching = Some(attempt);
            }
        }
        if let Some(attempt) = matching.as_ref() {
            if attempt.execution != AttemptExecution::Pending && attempt.save != AttemptSave::Open {
                return Ok(matching.expect("the terminal attempt is present"));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(match matching.as_ref() {
                Some(attempt) => format!(
                    "{label} native generation attempt did not become terminal: \
                     attempt_present=true execution={:?} save={:?} failure_code={:?} \
                     saved_end={} generated_end={:?} terminal_saved_end={:?} \
                     selected_attempts={selected_attempts}",
                    attempt.execution,
                    attempt.save,
                    attempt.failure_code,
                    attempt.saved_end,
                    attempt.generated_end,
                    attempt.terminal_saved_end,
                ),
                None => format!(
                    "{label} native generation attempt did not become terminal: \
                     attempt_present=false selected_attempts={selected_attempts}"
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) async fn list_turns(
    client: &ServiceClient,
    conversation_id: &str,
) -> Result<Vec<loxa_ipc::TurnSummary>, String> {
    match client
        .history_request(
            ConnectMode::ObserveExisting,
            HistoryCommand::ListTurns {
                conversation_id: conversation_id.into(),
                cursor: None,
                limit: 10,
            },
        )
        .await
        .map_err(client_error)?
    {
        HistoryReply::TurnPage(page) => Ok(page.turns),
        _ => Err("native turn listing returned the wrong reply".into()),
    }
}

async fn read_assistant(
    client: &ServiceClient,
    attempt: &loxa_ipc::AttemptSummary,
) -> Result<String, String> {
    if attempt.generated_end.as_ref() != Some(&attempt.saved_end)
        || attempt.terminal_saved_end.as_ref() != Some(&attempt.saved_end)
    {
        return Err("native generation terminal content ends disagree".into());
    }
    let expected_end = attempt
        .saved_end
        .parse::<u64>()
        .map_err(|_| "native saved end is invalid".to_string())?;
    let mut start = 0u64;
    let mut content = String::new();
    while start < expected_end {
        let range = match client
            .history_request(
                ConnectMode::ObserveExisting,
                HistoryCommand::ReadContentRange {
                    source: ContentSource::Assistant {
                        attempt_id: attempt.id.clone(),
                    },
                    start: start.to_string(),
                    prefix_end: expected_end.to_string(),
                },
            )
            .await
            .map_err(client_error)?
        {
            HistoryReply::ContentRange(range) => range,
            _ => return Err("native content read returned the wrong reply".into()),
        };
        let range_start = range
            .start
            .parse::<u64>()
            .map_err(|_| "native content range start is invalid".to_string())?;
        let range_end = range
            .end
            .parse::<u64>()
            .map_err(|_| "native content range end is invalid".to_string())?;
        if range_start != start
            || range_end <= start
            || range_end > expected_end
            || range.content.len() as u64 != range_end - range_start
        {
            return Err("native saved output is not contiguous".into());
        }
        content.push_str(&range.content);
        start = range_end;
    }
    if content.len() as u64 != expected_end {
        return Err("native saved output bytes do not match their terminal end".into());
    }
    Ok(content)
}

pub(super) fn client_error(error: ClientError) -> String {
    error.to_string()
}
