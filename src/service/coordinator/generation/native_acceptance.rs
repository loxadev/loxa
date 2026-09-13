mod client;
mod evidence;
mod harness;

use super::qualification_fixture;
use client::{
    create_conversation, create_draft, list_turns, read_draft, require_rejected,
    require_same_draft, send, wait_for_saved, wait_for_stopped,
};
use harness::{load_model, only_engine_endpoint, wait_for_unloaded, NativeService};
use loxa_ipc::{ConnectMode, ErrorCategory, GenerationCommand, GenerationReply, GenerationTarget};
use std::path::Path;

pub(super) use qualification_fixture::{CONTEXT_TOKENS, MODEL_ID, MODEL_SHA256, MODEL_SIZE};
const GENERATION_TOKENS: u32 = 32;

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
    let first = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let busy = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let busy_draft = create_draft(&fixture.client, &busy, "busy draft remains exact").await?;

    // The SQLite worker has returned the commit and released its permit at
    // this pause. The second Send can complete its retained-submission lookup
    // before it observes the active admission and returns Busy.
    let mut completion = fixture.coordinator.pause_native_admission_completion();
    let first_send = tokio::spawn(send(
        fixture.client.clone(),
        first.clone(),
        "Reply briefly to café 你好 👋.",
        None,
        1,
    ));
    completion.wait_reached().await?;
    let busy_result = send(
        fixture.client.clone(),
        busy.clone(),
        "this request must remain busy",
        Some(&busy_draft),
        2,
    )
    .await;
    let busy_after = read_draft(&fixture.client, &busy_draft).await;
    completion.release();
    let first_accepted = first_send
        .await
        .map_err(|error| format!("first native generation task failed: {error}"))?
        .map_err(client::client_error)?;
    require_rejected(busy_result, ErrorCategory::Busy, "concurrent generation")?;
    require_same_draft(&busy_draft, &busy_after?)?;
    let first_output = wait_for_saved(&fixture.client, &first, &first_accepted, "first").await?;
    if first_output.is_empty() {
        return Err("first native generation saved no assistant bytes".into());
    }

    let second = create_conversation(&fixture.client, GENERATION_TOKENS).await?;
    let second_accepted = send(
        fixture.client.clone(),
        second.clone(),
        "Reply briefly to café 你好 👋.",
        None,
        3,
    )
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

    let observations = qualification_fixture::observations();
    evidence::validate(&observations, &fixture.runtime_evidence)?;
    Ok(evidence::render(
        &observations,
        &fixture.runtime_evidence,
        true,
    ))
}

fn observation_diagnostics() -> String {
    let observations = qualification_fixture::observations();
    let postcommit = observations
        .iter()
        .filter(|observation| observation.postcommit_gate_entered)
        .count();
    let dispatched = observations
        .iter()
        .filter(|observation| observation.generation_dispatched)
        .count();
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
    let generated_end = observations.iter().fold(0usize, |total, observation| {
        total.saturating_add(observation.generated_end)
    });
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
         generated_end={generated_end} usage={usage} cached_usage={cached_usage} \
         drivers_completed={drivers_completed} driver_errors={driver_errors} \
         quiescent={quiescent}",
        observations.len(),
    )
}
