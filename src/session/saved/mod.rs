mod conversation;
mod output;
mod signals;

#[cfg(test)]
mod ipc_tests;
#[cfg(test)]
mod tests;

use self::conversation::{submission_id, Conversation};
use self::output::{OutputError, TerminalOutput};
use self::signals::{Interrupt, SessionSignals};
use super::{new_editor, prompt_input, InputEvent};
use crate::ui;
use loxa_ipc::{
    AttemptExecution, AttemptSave, ClientError, ConnectMode, ContentSource, ErrorCategory,
    GenerationAccepted, GenerationCommand, GenerationExecutionPhase, GenerationObservation,
    GenerationReply, GenerationSavePhase, GenerationSettings, GenerationTarget, HistoryCommand,
    HistoryReply, OperationTarget, ServiceClient, ServiceSettingsCommand, ServiceSettingsReply,
};
use std::io::Write;
use std::time::Duration;

const COMMANDS: [(&str, &str); 6] = [
    ("/new", "Start a saved conversation"),
    ("/clear", "Start a saved conversation"),
    ("/retry", "Retry the last accepted attempt"),
    ("/settings", "Show effective conversation settings"),
    ("/help", "Show available commands"),
    ("/exit", "Exit chat"),
];
const STOP_WAIT: Duration = Duration::from_secs(6);

pub(crate) fn run_service(
    client: ServiceClient,
    model_id: String,
    target: OperationTarget,
    max_tokens: Option<u32>,
) -> Result<i32, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(crate::service::attachment::validate_target(
        &client, &model_id, &target,
    ))?;
    let mut editor = new_editor(&COMMANDS, false)?;
    let mut signals = SessionSignals::install()?;
    let mut conversation: Option<Conversation> = None;
    let ready = ui::success();
    let model_style = ui::accent();
    let dim = ui::muted();
    anstream::println!("{ready}Ready{ready:#} · {model_style}{model_id}{model_style:#}");
    anstream::println!(
        "{dim}Enter / for commands · \\ then Enter for a new line · Ctrl-D or Ctrl-C to exit{dim:#}"
    );
    loop {
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
        let terminal = signals.enter_idle()?;
        let input = prompt_input(&mut editor);
        signals.leave_idle();
        if signals.was_interrupted() || matches!(input, InputEvent::Interrupted) {
            signals.exit_now(Some(terminal));
        }
        let line = match input {
            InputEvent::Line(line) => line,
            InputEvent::Canceled => return Ok(0),
            InputEvent::Error(error) => {
                return Err(format!("failed to read terminal input: {error}"))
            }
            InputEvent::Interrupted => unreachable!(),
        };
        let trimmed = line.trim();
        match trimmed {
            "" => continue,
            "/" | "/?" | "/help" => {
                let accent = ui::accent();
                anstream::println!("{accent}Available commands{accent:#}");
                for (command, description) in COMMANDS {
                    anstream::println!("  {accent}{command:<9}{accent:#} {description}");
                }
                continue;
            }
            "/exit" => return Ok(0),
            "/new" | "/clear" => {
                conversation = None;
                let success = ui::success();
                anstream::println!("{success}New conversation ready.{success:#}");
                continue;
            }
            "/settings" => {
                let (settings, pending) = if let Some(active) = conversation.as_mut() {
                    (runtime.block_on(active.refresh_profile(&client))?, false)
                } else {
                    let mut settings = service_settings(&runtime, &client)?;
                    if let Some(max_tokens) = max_tokens {
                        settings.max_output_tokens = max_tokens;
                    }
                    (settings, true)
                };
                print_settings(&settings, pending);
                continue;
            }
            "/retry"
                if conversation
                    .as_ref()
                    .and_then(|item| item.last.as_ref())
                    .is_none() =>
            {
                anstream::eprintln!("No accepted attempt to retry.");
                continue;
            }
            command if command.starts_with('/') && command != "/retry" => {
                anstream::eprintln!(
                    "Unknown command: {}. Type /help for commands.",
                    ui::sanitize_terminal(command)
                );
                continue;
            }
            _ => {}
        }

        if conversation.is_none() {
            conversation =
                Some(runtime.block_on(Conversation::create(&client, &model_id, max_tokens))?);
        }
        let active = conversation.as_mut().expect("conversation created");
        let command = if trimmed == "/retry" {
            active
                .retry_command(submission_id()?)
                .expect("retry has an accepted attempt")
        } else {
            active.send_command(submission_id()?, trimmed.to_owned())
        };
        let current_submission = match &command {
            GenerationCommand::Send { submission_id, .. }
            | GenerationCommand::Retry { submission_id, .. } => submission_id.clone(),
            GenerationCommand::Stop { .. } => unreachable!("terminal input cannot issue Stop"),
        };
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
        let tag = match signals.begin_generation() {
            Ok(tag) => tag,
            Err(_) => signals.exit_now(None),
        };
        let output = match {
            let _entered = runtime.enter();
            TerminalOutput::stdout()
        } {
            Ok(output) => output,
            Err(_) => signals.exit_with_code(1, None),
        };
        let result = runtime.block_on(run_generation(
            &client, &target, active, command, &output, &signals, tag,
        ));
        if signals.interrupted_now(tag) == Some(Interrupt::Current)
            && !matches!(result, Err(ChatError::Interrupted))
        {
            if let Some(attempt) = &active.last {
                if matches!(&attempt.target, GenerationTarget::Accepted { submission_id, .. } if submission_id == &current_submission)
                {
                    runtime.block_on(stop_exact(&client, attempt.target.clone()));
                }
            }
        }
        if output.restore().is_err() {
            signals.exit_with_code(1, None);
        }
        signals.end_generation(tag);
        if signals.was_interrupted() || matches!(result, Err(ChatError::Interrupted)) {
            signals.exit_now(None);
        }
        match result {
            Ok(()) => {}
            Err(ChatError::Output) => signals.exit_with_code(1, None),
            Err(ChatError::Turn(error)) => {
                anstream::eprintln!("Error: {}", ui::sanitize_terminal(&error));
            }
            Err(ChatError::Service(error)) => return Err(error),
            Err(ChatError::Interrupted) => unreachable!(),
        }
    }
}

fn service_settings(
    runtime: &tokio::runtime::Runtime,
    client: &ServiceClient,
) -> Result<GenerationSettings, String> {
    match runtime
        .block_on(client.settings_request(
            ConnectMode::ObserveExisting,
            ServiceSettingsCommand::GetServiceSettings,
        ))
        .map_err(|error| error.to_string())?
    {
        ServiceSettingsReply::Service(settings) => Ok(settings.generation),
        _ => Err("service returned the wrong settings reply".into()),
    }
}

fn print_settings(settings: &GenerationSettings, pending: bool) {
    let sampling = |value: Option<loxa_ipc::SamplingValue>| {
        value
            .map(|value| value.get().to_string())
            .unwrap_or_else(|| "automatic (resolved when generating)".into())
    };
    if pending {
        anstream::println!("Settings for the next conversation:");
    }
    anstream::println!("Max output tokens: {}", settings.max_output_tokens);
    anstream::println!(
        "Temperature: {} · Top P: {}",
        sampling(settings.temperature),
        sampling(settings.top_p)
    );
    if settings.system_instruction.is_empty() {
        anstream::println!("System instruction: none");
    } else {
        anstream::println!(
            "System instruction:\n{}",
            ui::sanitize_terminal(&settings.system_instruction)
        );
    }
}

#[derive(Debug)]
enum ChatError {
    Interrupted,
    Output,
    Turn(String),
    Service(String),
}

#[derive(Debug, Eq, PartialEq)]
enum TerminalOutcome {
    Pending,
    Completed,
    Stopped,
    ExecutionFailed,
    SaveFailed,
}

fn observation_progress(
    observation: GenerationObservation,
) -> Result<(u64, TerminalOutcome), ChatError> {
    let (saved_end, outcome) = match observation {
        GenerationObservation::Live { status } => {
            let outcome = match (status.execution, status.save) {
                (GenerationExecutionPhase::Completed, GenerationSavePhase::Saved) => {
                    TerminalOutcome::Completed
                }
                (GenerationExecutionPhase::Stopped, GenerationSavePhase::Saved) => {
                    TerminalOutcome::Stopped
                }
                (GenerationExecutionPhase::Failed, GenerationSavePhase::Saved) => {
                    TerminalOutcome::ExecutionFailed
                }
                _ => TerminalOutcome::Pending,
            };
            (status.saved_end, outcome)
        }
        GenerationObservation::Durable { attempt } => {
            let outcome = match (attempt.execution, attempt.save) {
                (AttemptExecution::Pending, _) => TerminalOutcome::Pending,
                (_, AttemptSave::Failed | AttemptSave::Interrupted) => TerminalOutcome::SaveFailed,
                (AttemptExecution::Completed, AttemptSave::Saved) => TerminalOutcome::Completed,
                (AttemptExecution::Stopped, AttemptSave::Saved) => TerminalOutcome::Stopped,
                (AttemptExecution::Failed | AttemptExecution::Interrupted, AttemptSave::Saved) => {
                    TerminalOutcome::ExecutionFailed
                }
                _ => TerminalOutcome::Pending,
            };
            (attempt.saved_end, outcome)
        }
    };
    let end = saved_end
        .parse::<u64>()
        .map_err(|_| ChatError::Service("invalid saved output offset".into()))?;
    Ok((end, outcome))
}

fn checked_range(
    range: &loxa_ipc::ContentRange,
    cursor: u64,
    captured_end: u64,
) -> Result<u64, ChatError> {
    let start = range
        .start
        .parse::<u64>()
        .map_err(|_| ChatError::Service("invalid content start".into()))?;
    let next = range
        .end
        .parse::<u64>()
        .map_err(|_| ChatError::Service("invalid content end".into()))?;
    let prefix = range
        .prefix_end
        .parse::<u64>()
        .map_err(|_| ChatError::Service("invalid content prefix".into()))?;
    if start != cursor
        || next <= start
        || next > captured_end
        || prefix != captured_end
        || next - start != range.content.len() as u64
    {
        return Err(ChatError::Service(
            "saved output range is not contiguous".into(),
        ));
    }
    Ok(next)
}

async fn run_generation(
    client: &ServiceClient,
    runtime_target: &OperationTarget,
    conversation: &mut Conversation,
    command: GenerationCommand,
    output: &TerminalOutput,
    signals: &SessionSignals,
    tag: u64,
) -> Result<(), ChatError> {
    let accepted = submit(client, runtime_target, &command, signals, tag).await?;
    conversation.accept(&accepted).map_err(ChatError::Service)?;
    observe(client, conversation, output, signals, tag).await
}

async fn submit(
    client: &ServiceClient,
    runtime_target: &OperationTarget,
    command: &GenerationCommand,
    signals: &SessionSignals,
    tag: u64,
) -> Result<GenerationAccepted, ChatError> {
    for replay in 0..2 {
        let prepare =
            client.prepare_generation_at(ConnectMode::ObserveExisting, runtime_target.clone());
        let pending = tokio::select! {
            biased;
            interrupt = signals.interrupted(tag) => return Err(interrupt_error(interrupt)),
            result = prepare => result.map_err(submission_error)?,
        };
        let pending_target = pending.target().clone();
        let send = pending.send(command.clone());
        tokio::pin!(send);
        let result = tokio::select! {
            biased;
            interrupt = signals.interrupted(tag) => {
                if interrupt == Interrupt::Current {
                    stop_exact(client, pending_target.clone()).await;
                    if let Ok(Ok(GenerationReply::Accepted(accepted))) = tokio::time::timeout(STOP_WAIT, &mut send).await {
                        if accepted_matches(command, runtime_target, &accepted) {
                            stop_exact(client, accepted.target()).await;
                        }
                    }
                }
                return Err(ChatError::Interrupted);
            }
            result = &mut send => result,
        };
        match result {
            Ok(GenerationReply::Accepted(accepted)) => {
                if !accepted_matches(command, runtime_target, &accepted) {
                    return Err(ChatError::Service(
                        "service accepted a different generation identity".into(),
                    ));
                }
                if matches!(signals.interrupted_now(tag), Some(Interrupt::Current)) {
                    stop_exact(client, accepted.target()).await;
                    return Err(ChatError::Interrupted);
                }
                return Ok(accepted);
            }
            Ok(_) => {
                return Err(ChatError::Service(
                    "service returned the wrong generation reply".into(),
                ))
            }
            Err(ClientError::Transport(_)) if replay == 0 => continue,
            Err(error) => return Err(submission_error(error)),
        }
    }
    unreachable!()
}

fn submission_error(error: ClientError) -> ChatError {
    match &error {
        ClientError::Rejected(rejected)
            if matches!(
                rejected.category,
                ErrorCategory::Busy | ErrorCategory::InvalidRequest | ErrorCategory::Conflict
            ) =>
        {
            ChatError::Turn(error.to_string())
        }
        _ => ChatError::Service(error.to_string()),
    }
}

fn accepted_matches(
    command: &GenerationCommand,
    runtime_target: &OperationTarget,
    accepted: &GenerationAccepted,
) -> bool {
    let (conversation_id, submission_id, expected_conversation_revision, expected_profile_revision) =
        match command {
            GenerationCommand::Send {
                conversation_id,
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
                ..
            }
            | GenerationCommand::Retry {
                conversation_id,
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
                ..
            } => (
                conversation_id,
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
            ),
            GenerationCommand::Stop { .. } => return false,
        };
    accepted.submission_id == *submission_id
        && accepted.conversation_id == *conversation_id
        && accepted.pre_conversation_revision == *expected_conversation_revision
        && accepted.profile_revision == *expected_profile_revision
        && accepted.boot_epoch == runtime_target.boot_epoch
        && accepted.operation_generation == runtime_target.generation
        && expected_conversation_revision
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_add(1))
            .is_some_and(|next| accepted.post_conversation_revision == next.to_string())
}

async fn stop_exact(client: &ServiceClient, target: GenerationTarget) {
    let _ = tokio::time::timeout(
        STOP_WAIT,
        client.generation_request(
            ConnectMode::ObserveExisting,
            GenerationCommand::Stop { target },
        ),
    )
    .await;
}

fn interrupt_error(_interrupt: Interrupt) -> ChatError {
    ChatError::Interrupted
}

async fn observe(
    client: &ServiceClient,
    conversation: &mut Conversation,
    output: &TerminalOutput,
    signals: &SessionSignals,
    tag: u64,
) -> Result<(), ChatError> {
    let attempt = conversation
        .last
        .as_mut()
        .expect("accepted attempt recorded");
    let mut subscription = tokio::select! {
        biased;
        interrupt = signals.interrupted(tag) => {
            if interrupt == Interrupt::Current { stop_exact(client, attempt.target.clone()).await; }
            return Err(ChatError::Interrupted);
        }
        result = client.subscribe_generation(ConnectMode::ObserveExisting, attempt.target.clone(), attempt.attempt_id.clone()) => {
            result.map_err(|error| ChatError::Service(error.to_string()))?
        }
    };
    loop {
        let observation = tokio::select! {
            biased;
            interrupt = signals.interrupted(tag) => {
                if interrupt == Interrupt::Current { stop_exact(client, attempt.target.clone()).await; }
                return Err(ChatError::Interrupted);
            }
            result = subscription.next_observation() => result.map_err(|error| ChatError::Service(error.to_string()))?,
        };
        let (end, outcome) = observation_progress(observation)?;
        if end < attempt.raw_cursor {
            return Err(ChatError::Service("saved output offset regressed".into()));
        }
        while attempt.raw_cursor < end {
            let range = tokio::select! {
                biased;
                interrupt = signals.interrupted(tag) => {
                    if interrupt == Interrupt::Current { stop_exact(client, attempt.target.clone()).await; }
                    return Err(ChatError::Interrupted);
                }
                result = client.history_request(ConnectMode::ObserveExisting, HistoryCommand::ReadContentRange {
                    source: ContentSource::Assistant { attempt_id: attempt.attempt_id.clone() },
                    start: attempt.raw_cursor.to_string(),
                    prefix_end: end.to_string(),
                }) => result.map_err(|error| ChatError::Service(error.to_string()))?,
            };
            let HistoryReply::ContentRange(range) = range else {
                return Err(ChatError::Service(
                    "service returned the wrong content reply".into(),
                ));
            };
            let next = checked_range(&range, attempt.raw_cursor, end)?;
            match output.write_sanitized(&range.content, signals, tag).await {
                Ok(()) => attempt.raw_cursor = next,
                Err(OutputError::Interrupted(interrupt)) => {
                    if interrupt == Interrupt::Current {
                        stop_exact(client, attempt.target.clone()).await;
                    }
                    return Err(ChatError::Interrupted);
                }
                Err(OutputError::Failed) => return Err(ChatError::Output),
            }
        }
        if outcome != TerminalOutcome::Pending {
            match output.write_all(b"\n", signals, tag).await {
                Ok(()) => {}
                Err(OutputError::Interrupted(interrupt)) => {
                    if interrupt == Interrupt::Current {
                        stop_exact(client, attempt.target.clone()).await;
                    }
                    return Err(ChatError::Interrupted);
                }
                Err(OutputError::Failed) => return Err(ChatError::Output),
            }
            if outcome == TerminalOutcome::SaveFailed {
                return Err(ChatError::Turn(
                    "generation output could not be saved".into(),
                ));
            }
            if outcome == TerminalOutcome::ExecutionFailed {
                return Err(ChatError::Turn(
                    "generation failed; saved output is above".into(),
                ));
            }
            if outcome == TerminalOutcome::Stopped {
                return Err(ChatError::Turn(
                    "generation stopped; saved output is above".into(),
                ));
            }
            return Ok(());
        }
    }
}
