mod client;
mod evidence;
mod harness;

use super::qualification_fixture;
use client::{
    create_conversation, create_draft, list_turns, read_draft, require_rejected,
    require_same_draft, send, wait_for_saved, wait_for_stopped,
};
use harness::{load_model, only_engine_endpoint, wait_for_unloaded, NativeEngine, NativeService};
use loxa_ipc::{
    AttemptExecution, AttemptSave, ConnectMode, ErrorCategory, GenerationCommand, GenerationReply,
    GenerationTarget,
};
use sha2::{Digest, Sha256};
use std::path::Path;

pub(super) use qualification_fixture::{CONTEXT_TOKENS, MODEL_ID, MODEL_SHA256, MODEL_SIZE};
const GENERATION_TOKENS: u32 = 2;

pub(in crate::service) async fn run_bundled_generation_acceptance(
    app: &Path,
    source_model: &Path,
) -> Result<String, String> {
    qualification_fixture::clear_observations();
    let mut fixture = NativeService::start(app, source_model).await?;
    let result = exercise(&mut fixture)
        .await
        .map_err(|error| format!("{error}; {}", observation_diagnostics()));
    let cleanup = fixture.shutdown(true).await;
    match (result, cleanup) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
    }
}

async fn exercise(fixture: &mut NativeService) -> Result<String, String> {
    let prompt: &'static str = "Reply with the English word hello. Input: café 你好 👋.";
    let first = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let busy = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let busy_draft = create_draft(&fixture.client, &busy, "busy draft remains exact").await?;

    // The SQLite worker has returned the commit and released its permit at
    // this pause. The second Send can complete its retained-submission lookup
    // before it observes the active admission and returns Busy.
    let mut completion = fixture.coordinator.pause_native_admission_completion();
    let first_send = tokio::spawn(send(fixture.client.clone(), first.clone(), prompt, None, 1));
    completion.wait_reached().await?;
    let pending_turns = list_turns(&fixture.client, &first.id).await?;
    let [pending_turn] = pending_turns.as_slice() else {
        return Err("lost-reply gate did not retain exactly one durable turn".into());
    };
    let pending_attempt = pending_turn
        .selected_attempt
        .as_ref()
        .ok_or_else(|| "lost-reply gate did not retain its selected attempt".to_string())?;
    if pending_attempt.attempt_number != "1"
        || pending_attempt.execution != AttemptExecution::Pending
        || pending_attempt.save != AttemptSave::Open
        || pending_attempt.saved_end != "0"
        || pending_attempt.generated_end.is_some()
        || pending_attempt.terminal_saved_end.is_some()
    {
        return Err("lost-reply gate crossed the output execution boundary".into());
    }
    first_send.abort();
    match first_send.await {
        Err(error) if error.is_cancelled() => {}
        Err(error) => return Err(format!("first native Send did not cancel cleanly: {error}")),
        Ok(_) => return Err("first native Send received its reply before disconnect".into()),
    }
    let busy_result = send(
        fixture.client.clone(),
        busy.clone(),
        "this request must remain busy",
        Some(&busy_draft),
        2,
    )
    .await;
    let busy_after = read_draft(&fixture.client, &busy_draft).await;
    require_rejected(busy_result, ErrorCategory::Busy, "concurrent generation")?;
    require_same_draft(&busy_draft, &busy_after?)?;
    completion.release();
    // Replay the original request, including its pre-admission revisions.
    let first_accepted = send(fixture.client.clone(), first.clone(), prompt, None, 1)
        .await
        .map_err(client::client_error)?;
    if first_accepted.turn_id != pending_turn.id || first_accepted.attempt_id != pending_attempt.id
    {
        return Err("native replay changed the original durable admission identities".into());
    }
    let first_output = wait_for_saved(&fixture.client, &first, &first_accepted, "first").await?;
    if first_output.is_empty() {
        return Err("first native generation saved no assistant bytes".into());
    }
    let completed_turns = list_turns(&fixture.client, &first.id).await?;
    if !matches!(completed_turns.as_slice(), [turn]
        if turn.id == pending_turn.id && turn.selected_attempt.as_ref().is_some_and(|attempt|
            attempt.id == pending_attempt.id && attempt.attempt_number == "1"))
    {
        return Err("native replay did not preserve one canonical turn and attempt".into());
    }

    let second = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let second_accepted = send(fixture.client.clone(), second.clone(), prompt, None, 3)
        .await
        .map_err(client::client_error)?;
    let second_output =
        wait_for_saved(&fixture.client, &second, &second_accepted, "cached").await?;
    if second_output.is_empty() {
        return Err("cached native generation saved no assistant bytes".into());
    }

    let overflow = create_conversation(&fixture.client, CONTEXT_TOKENS).await?;
    let overflow_draft =
        create_draft(&fixture.client, &overflow, "overflow draft remains exact").await?;
    let overflow_result = send(
        fixture.client.clone(),
        overflow.clone(),
        "one token still exceeds a fully reserved context",
        Some(&overflow_draft),
        4,
    )
    .await;
    require_rejected(
        overflow_result,
        ErrorCategory::InvalidRequest,
        "context overflow",
    )?;
    require_same_draft(
        &overflow_draft,
        &read_draft(&fixture.client, &overflow_draft).await?,
    )?;
    if !list_turns(&fixture.client, &overflow.id).await?.is_empty() {
        return Err("context overflow created a durable turn".into());
    }

    let stopped = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let mut execution = fixture.coordinator.pause_native_generation_execution();
    let stopped_accepted = send(
        fixture.client.clone(),
        stopped.clone(),
        "This request must stop after durable admission.",
        None,
        5,
    )
    .await
    .map_err(client::client_error)?;
    execution.wait_reached().await?;
    let endpoint = only_engine_endpoint(&fixture.root.join("run/service"))?;
    let target = GenerationTarget::Accepted {
        boot_epoch: stopped_accepted.boot_epoch.clone(),
        submission_id: stopped_accepted.submission_id.clone(),
        operation_generation: stopped_accepted.operation_generation.clone(),
    };
    let stop_result = fixture
        .client
        .generation_request(
            ConnectMode::ObserveExisting,
            GenerationCommand::Stop {
                target: target.clone(),
            },
        )
        .await;
    execution.release();
    match stop_result.map_err(client::client_error)? {
        GenerationReply::Stopping { target: returned } if returned == target => {}
        _ => return Err("generation Stop returned the wrong target".into()),
    }
    wait_for_stopped(&fixture.client, &stopped, &stopped_accepted, "stopped").await?;
    wait_for_unloaded(&fixture.client).await?;
    if endpoint.exists() {
        return Err("stopped exact engine endpoint survived verified cleanup".into());
    }
    load_model(&fixture.client).await?;
    stop_after_content_and_reject_stale_target(fixture, prompt).await?;

    let observations = qualification_fixture::observations();
    evidence::validate(&observations, &fixture.runtime_evidence)?;
    Ok(evidence::render(
        &observations,
        &fixture.runtime_evidence,
        true,
    ))
}

async fn stop_after_content_and_reject_stale_target(
    fixture: &NativeService,
    prompt: &str,
) -> Result<(), String> {
    let stopped = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let engine = NativeEngine::capture(fixture)?;
    let mut output = fixture.coordinator.pause_native_generation_output();
    let accepted = send(fixture.client.clone(), stopped.clone(), prompt, None, 6)
        .await
        .map_err(client::client_error)?;
    output.wait_reached().await?;
    let target = accepted.target();
    let working = fixture
        .client
        .generation_status(&target)
        .await
        .map_err(client::client_error)?
        .ok_or_else(|| "native output gate lost its active status".to_string())?;
    if working.attempt_id != accepted.attempt_id
        || working.execution != loxa_ipc::GenerationExecutionPhase::Working
        || working.generated_end.is_some()
    {
        return Err("native output gate returned the wrong active status".into());
    }
    let stop_result = fixture
        .client
        .generation_request(
            ConnectMode::ObserveExisting,
            GenerationCommand::Stop {
                target: target.clone(),
            },
        )
        .await;
    match stop_result.map_err(client::client_error)? {
        GenerationReply::Stopping { target: returned } if returned == target => {}
        _ => return Err("native output Stop returned the wrong target".into()),
    }
    let cancelling = fixture
        .client
        .generation_status(&target)
        .await
        .map_err(client::client_error)?
        .ok_or_else(|| "native Stop lost its unresolved active status".to_string())?;
    if cancelling.attempt_id != accepted.attempt_id
        || cancelling.execution != loxa_ipc::GenerationExecutionPhase::Cancelling
        || cancelling.failure_code.as_deref() != Some("stopped")
    {
        return Err("native Stop returned the wrong cancelling status".into());
    }
    output.release();
    let saved = wait_for_stopped(&fixture.client, &stopped, &accepted, "output Stop").await?;
    let (generated_end, prefix) = qualification_fixture::observations()
        .into_iter()
        .find_map(|observation| {
            observation
                .output_gate_prefix
                .map(|prefix| (observation.generated_end, prefix))
        })
        .ok_or_else(|| "native Stop did not observe nonterminal content".to_string())?;
    let saved_prefix = saved
        .get(..prefix.bytes)
        .filter(|prefix| !prefix.is_empty())
        .ok_or_else(|| "native Stop did not retain its observed UTF-8 prefix".to_string())?;
    if qualification_fixture::encode_digest(Sha256::digest(saved_prefix.as_bytes()).into())
        != prefix.sha256
        || generated_end != saved.len()
    {
        return Err("native Stop changed or lost decoded output bytes".into());
    }
    wait_for_unloaded(&fixture.client).await?;
    engine.require_removed()?;
    fixture.wait_for_admission_release().await?;

    load_model(&fixture.client).await?;
    let replacement_engine = NativeEngine::capture(fixture)?;
    let replacement = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let mut execution = fixture.coordinator.pause_native_generation_execution();
    let replacement_accepted = send(fixture.client.clone(), replacement.clone(), prompt, None, 7)
        .await
        .map_err(client::client_error)?;
    execution.wait_reached().await?;
    if accepted.operation_generation == replacement_accepted.operation_generation {
        return Err("native replacement reused the stopped runtime generation".into());
    }
    let stale_result = fixture
        .client
        .generation_request(
            ConnectMode::ObserveExisting,
            GenerationCommand::Stop { target },
        )
        .await;
    execution.release();
    match stale_result {
        Err(loxa_ipc::ClientError::Rejected(error))
            if error.category == ErrorCategory::Conflict => {}
        Err(error) => return Err(format!("stale native Stop returned {error}")),
        Ok(_) => return Err("stale native Stop was unexpectedly accepted".into()),
    }
    let replacement_output = wait_for_saved(
        &fixture.client,
        &replacement,
        &replacement_accepted,
        "replacement",
    )
    .await?;
    if replacement_output.is_empty() {
        return Err("native replacement saved no assistant bytes".into());
    }
    fixture.wait_for_admission_release().await?;
    replacement_engine.require_current(fixture)
}

fn observation_diagnostics() -> String {
    let observations = qualification_fixture::observations();
    let postcommit = observations
        .iter()
        .filter(|observation| observation.postcommit_gate_entered)
        .count();
    let dispatched = observations.iter().fold(0usize, |total, observation| {
        total.saturating_add(observation.generation_dispatches)
    });
    let basis = observations
        .iter()
        .filter(|observation| observation.basis_carried_to_execution)
        .count();
    let usage = observations
        .iter()
        .filter(|observation| observation.completion_prompt_tokens.is_some())
        .count();
    let cached_usage = observations
        .iter()
        .filter(|observation| observation.completion_cached_tokens.is_some())
        .count();
    let quiescent = observations
        .iter()
        .filter(|observation| observation.quiescent_after_terminal)
        .count();
    let statuses = observations
        .iter()
        .filter_map(|observation| observation.response_status)
        .collect::<Vec<_>>();
    let data_frames = observations.iter().fold(0usize, |total, observation| {
        total.saturating_add(observation.data_frames)
    });
    let data_bytes = observations.iter().fold(0usize, |total, observation| {
        total.saturating_add(observation.data_bytes)
    });
    let generated_ends = observations
        .iter()
        .map(|observation| observation.generated_end)
        .collect::<Vec<_>>();
    let drivers_completed = observations
        .iter()
        .filter(|observation| observation.http_driver_completed)
        .count();
    let driver_errors = observations
        .iter()
        .filter(|observation| observation.http_driver_error)
        .count();
    format!(
        "qualification observations: total={} postcommit={postcommit} dispatched={dispatched} \
         basis={basis} statuses={statuses:?} data_frames={data_frames} data_bytes={data_bytes} \
         generated_ends={generated_ends:?} usage={usage} cached_usage={cached_usage} \
         drivers_completed={drivers_completed} driver_errors={driver_errors} \
         quiescent={quiescent}",
        observations.len(),
    )
}
