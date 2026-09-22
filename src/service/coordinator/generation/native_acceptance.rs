mod client;
mod evidence;
mod harness;

use super::qualification_fixture;
use client::{
    create_conversation, create_draft, list_turns, read_draft, require_rejected,
    require_same_draft, retry, send, wait_for_saved, wait_for_stopped,
};
use harness::{
    load_model, only_engine_endpoint, runtime_status, wait_for_unloaded, NativeEngine,
    NativeService,
};
use loxa_ipc::{
    AttemptExecution, AttemptSave, ConnectMode, ErrorCategory, GenerationCommand, GenerationReply,
    OperationTarget, OptionalU32Patch, ReplyOutcome, RuntimePhase, ServiceCommand,
    ServiceSettingsApplication, ServiceSettingsCommand, ServiceSettingsPatch, ServiceSettingsReply,
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
    let Some(sampling) = pending_attempt.effective_sampling else {
        return Err("lost-reply gate did not persist effective sampling".into());
    };
    if sampling.temperature.get() != qualification_fixture::TEMPERATURE
        || sampling.top_p.get() != f64::from(qualification_fixture::TOP_P as f32)
    {
        return Err("lost-reply gate persisted the wrong effective sampling".into());
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

    let cached_retry = retry(fixture.client.clone(), first.clone(), &first_accepted, 3)
        .await
        .map_err(client::client_error)?;
    let expected_cached_revision = next_revision(&first_accepted.post_conversation_revision)?;
    if cached_retry.turn_id != first_accepted.turn_id
        || cached_retry.attempt_id == first_accepted.attempt_id
        || cached_retry.pre_conversation_revision != first_accepted.post_conversation_revision
        || cached_retry.post_conversation_revision != expected_cached_revision
    {
        return Err("cached native Retry changed the wrong durable facts".into());
    }
    let cached_output =
        wait_for_saved(&fixture.client, &first, &cached_retry, "cached Retry").await?;
    if cached_output.is_empty() {
        return Err("cached native Retry saved no assistant bytes".into());
    }
    let cached_turns = list_turns(&fixture.client, &first.id).await?;
    if !matches!(cached_turns.as_slice(), [turn]
        if turn.id == first_accepted.turn_id && turn.selected_attempt.as_ref().is_some_and(|attempt|
            attempt.id == cached_retry.attempt_id && attempt.attempt_number == "2"))
    {
        return Err(
            "cached native Retry created an extra user turn or selected the wrong attempt".into(),
        );
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

    let mut retry_completion = fixture.coordinator.pause_native_admission_completion();
    let mut retry_execution = fixture.coordinator.pause_native_generation_execution();
    let retry_client = fixture.client.clone();
    let retry_conversation = first.clone();
    let retry_prior = cached_retry.clone();
    let first_retry =
        tokio::spawn(async move { retry(retry_client, retry_conversation, &retry_prior, 5).await });
    retry_completion.wait_reached().await?;
    let retry_turns = list_turns(&fixture.client, &first.id).await?;
    let [retry_turn] = retry_turns.as_slice() else {
        return Err("native Retry did not preserve one durable user turn".into());
    };
    let retry_attempt = retry_turn
        .selected_attempt
        .as_ref()
        .ok_or_else(|| "native Retry did not select its durable attempt".to_string())?;
    let retry_sampling = retry_attempt
        .effective_sampling
        .ok_or_else(|| "native Retry did not freeze effective sampling".to_string())?;
    if retry_turn.id != first_accepted.turn_id
        || retry_attempt.id == cached_retry.attempt_id
        || retry_attempt.attempt_number != "3"
        || retry_attempt.execution != AttemptExecution::Pending
        || retry_attempt.save != AttemptSave::Open
        || retry_sampling.temperature.get() != qualification_fixture::TEMPERATURE
        || retry_sampling.top_p.get() != f64::from(qualification_fixture::TOP_P as f32)
    {
        return Err("native Retry committed the wrong attempt facts".into());
    }
    first_retry.abort();
    match first_retry.await {
        Err(error) if error.is_cancelled() => {}
        Err(error) => return Err(format!("native Retry did not cancel cleanly: {error}")),
        Ok(_) => return Err("native Retry received its reply before disconnect".into()),
    }
    retry_completion.release();
    let retried = retry(fixture.client.clone(), first.clone(), &cached_retry, 5)
        .await
        .map_err(client::client_error)?;
    let expected_retried_revision = next_revision(&cached_retry.post_conversation_revision)?;
    if retried.turn_id != first_accepted.turn_id
        || retried.attempt_id == cached_retry.attempt_id
        || retried.pre_conversation_revision != cached_retry.post_conversation_revision
        || retried.post_conversation_revision != expected_retried_revision
    {
        return Err("native Retry changed its durable turn or revision facts".into());
    }
    retry_execution.wait_reached().await?;
    require_rejected(
        retry(fixture.client.clone(), first.clone(), &retried, 5).await,
        ErrorCategory::Conflict,
        "changed native Retry replay",
    )?;
    let endpoint = only_engine_endpoint(&fixture.root.join("run/service"))?;
    let target = retried.target();
    let stop_result = fixture
        .client
        .generation_request(
            ConnectMode::ObserveExisting,
            GenerationCommand::Stop {
                target: target.clone(),
            },
        )
        .await;
    retry_execution.release();
    match stop_result.map_err(client::client_error)? {
        GenerationReply::Stopping { target: returned } if returned == target => {}
        _ => return Err("native Retry Stop returned the wrong target".into()),
    }
    wait_for_stopped(&fixture.client, &first, &retried, "retried").await?;
    let retried_turns = list_turns(&fixture.client, &first.id).await?;
    if !matches!(retried_turns.as_slice(), [turn]
        if turn.id == first_accepted.turn_id && turn.selected_attempt.as_ref().is_some_and(|attempt|
            attempt.id == retried.attempt_id && attempt.attempt_number == "3"))
    {
        return Err("native Retry created an extra user turn or selected the wrong attempt".into());
    }
    wait_for_unloaded(&fixture.client).await?;
    if endpoint.exists() {
        return Err("stopped exact engine endpoint survived verified cleanup".into());
    }
    load_model(&fixture.client).await?;
    stop_after_content_and_reject_stale_target(fixture, prompt).await?;
    exercise_reload_lifecycle(fixture).await?;

    let observations = qualification_fixture::observations();
    evidence::validate(&observations, &fixture.runtime_evidence)?;
    Ok(evidence::render(
        &observations,
        &fixture.runtime_evidence,
        true,
    ))
}

fn next_revision(revision: &str) -> Result<String, String> {
    revision
        .parse::<u64>()
        .ok()
        .and_then(|revision| revision.checked_add(1))
        .map(|revision| revision.to_string())
        .ok_or_else(|| "native generation returned an invalid conversation revision".into())
}

async fn exercise_reload_lifecycle(fixture: &mut NativeService) -> Result<(), String> {
    let old = ready_target(&fixture.client).await?;
    let settings = service_settings(&fixture.client).await?;
    let settings = match fixture
        .client
        .settings_request(
            ConnectMode::ObserveExisting,
            ServiceSettingsCommand::PatchServiceSettings {
                expected_revision: settings.revision,
                patch: ServiceSettingsPatch {
                    ctx: Some(OptionalU32Patch::Set {
                        value: CONTEXT_TOKENS,
                    }),
                    port: None,
                    generation: None,
                },
            },
        )
        .await
        .map_err(client::client_error)?
    {
        ServiceSettingsReply::Service(settings) => settings,
        _ => return Err("native context patch returned the wrong settings reply".into()),
    };

    let engine = NativeEngine::capture(fixture)?;
    let queue_probe = fixture.coordinator.occupy_native_owner_queue();
    match fixture
        .client
        .reload(
            ConnectMode::ObserveExisting,
            old.clone(),
            settings.revision.clone(),
        )
        .await
    {
        Err(loxa_ipc::ClientError::Rejected(error))
            if error.category == ErrorCategory::ServiceUnavailable => {}
        Err(error) => return Err(format!("native failed-enqueue Reload returned {error}")),
        Ok(_) => return Err("native failed-enqueue Reload was unexpectedly accepted".into()),
    }
    engine.require_current(fixture)?;
    if ready_target(&fixture.client).await? != old {
        return Err("failed Reload enqueue changed the Ready operation".into());
    }
    unload(&fixture.client, old).await?;
    wait_for_unloaded(&fixture.client).await?;
    engine.require_removed()?;
    queue_probe
        .recv_timeout(std::time::Duration::from_secs(1))
        .map_err(|_| "runtime owner did not consume the queue probe".to_string())?;

    load_model(&fixture.client).await?;
    let old = ready_target(&fixture.client).await?;
    let engine = NativeEngine::capture(fixture)?;
    fixture.coordinator.fail_next_native_runtime_termination();
    let termination_successor = fixture
        .client
        .reload(
            ConnectMode::ObserveExisting,
            old.clone(),
            settings.revision.clone(),
        )
        .await
        .map_err(client::client_error)?;
    wait_for_cleanup_failed(&fixture.client, &old).await?;
    assert_applied_target(&fixture.client, &old, "termination failure").await?;
    unload(&fixture.client, accepted_target(&termination_successor)).await?;
    wait_for_unloaded(&fixture.client).await?;
    engine.require_removed()?;

    load_model(&fixture.client).await?;
    let old = ready_target(&fixture.client).await?;
    let engine = NativeEngine::capture(fixture)?;
    fixture.coordinator.fail_next_native_intent_clear();
    let clear_successor = fixture
        .client
        .reload(
            ConnectMode::ObserveExisting,
            old.clone(),
            settings.revision.clone(),
        )
        .await
        .map_err(client::client_error)?;
    wait_for_cleanup_failed(&fixture.client, &old).await?;
    engine.require_removed()?;
    if !matches!(
        service_settings(&fixture.client).await?.application,
        ServiceSettingsApplication::NotApplied
    ) {
        return Err("intent-clear failure retained applied runtime facts after engine exit".into());
    }
    unload(&fixture.client, accepted_target(&clear_successor)).await?;
    wait_for_unloaded(&fixture.client).await?;

    load_model(&fixture.client).await?;
    let old = ready_target(&fixture.client).await?;
    let successor = fixture
        .client
        .reload(ConnectMode::ObserveExisting, old, settings.revision.clone())
        .await
        .map_err(client::client_error)?;
    let successor_target = accepted_target(&successor);
    wait_for_ready(&fixture.client, &successor_target).await?;
    match service_settings(&fixture.client).await?.application {
        ServiceSettingsApplication::Applied {
            target,
            settings_revision,
            context_preference,
            requested_context,
            observed_context,
            reload_required,
            ..
        } if target == successor_target
            && settings_revision == settings.revision
            && context_preference == Some(CONTEXT_TOKENS)
            && requested_context == CONTEXT_TOKENS
            && observed_context == Some(CONTEXT_TOKENS)
            && !reload_required => {}
        _ => return Err("successful native Reload published the wrong applied settings".into()),
    }

    let old = successor_target;
    let engine = NativeEngine::capture(fixture)?;
    let mut launch = fixture.coordinator.pause_native_reload_launch();
    let cancelled = fixture
        .client
        .reload(ConnectMode::ObserveExisting, old, settings.revision.clone())
        .await
        .map_err(client::client_error)?;
    launch.wait_reached().await?;
    engine.require_removed()?;
    require_no_launch_artifacts(fixture)?;
    unload(&fixture.client, accepted_target(&cancelled)).await?;
    launch.release();
    wait_for_unloaded(&fixture.client).await?;
    require_no_launch_artifacts(fixture)?;

    load_model(&fixture.client).await?;
    let old = ready_target(&fixture.client).await?;
    let engine = NativeEngine::capture(fixture)?;
    let mut launch = fixture.coordinator.pause_native_reload_launch();
    fixture
        .client
        .reload(ConnectMode::ObserveExisting, old, settings.revision)
        .await
        .map_err(client::client_error)?;
    launch.wait_reached().await?;
    engine.require_removed()?;
    require_no_launch_artifacts(fixture)?;
    fixture.stop_service().await?;
    launch.release();
    if !matches!(fixture.coordinator.status().phase, RuntimePhase::Draining) {
        return Err("StopService did not fence the promoted Reload successor".into());
    }
    Ok(())
}

async fn service_settings(
    client: &loxa_ipc::ServiceClient,
) -> Result<loxa_ipc::ServiceSettings, String> {
    match client
        .settings_request(
            ConnectMode::ObserveExisting,
            ServiceSettingsCommand::GetServiceSettings,
        )
        .await
        .map_err(client::client_error)?
    {
        ServiceSettingsReply::Service(settings) => Ok(settings),
        _ => Err("native settings read returned a conversation profile".into()),
    }
}

fn accepted_target(accepted: &loxa_ipc::Accepted) -> OperationTarget {
    OperationTarget {
        boot_epoch: accepted.boot_epoch.clone(),
        task_id: accepted.task_id.clone(),
        generation: accepted.generation.clone(),
    }
}

async fn ready_target(client: &loxa_ipc::ServiceClient) -> Result<OperationTarget, String> {
    let status = runtime_status(client).await?;
    let RuntimePhase::Ready {
        task_id,
        generation,
        ..
    } = status.phase
    else {
        return Err("native runtime is not Ready".into());
    };
    Ok(OperationTarget {
        boot_epoch: status.boot_epoch,
        task_id,
        generation,
    })
}

async fn wait_for_ready(
    client: &loxa_ipc::ServiceClient,
    target: &OperationTarget,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status = runtime_status(client).await?;
        match status.phase {
            RuntimePhase::Ready {
                task_id,
                generation,
                ..
            } if status.boot_epoch == target.boot_epoch
                && task_id == target.task_id
                && generation == target.generation =>
            {
                return Ok(())
            }
            RuntimePhase::CleanupFailed { .. } | RuntimePhase::RecoveryRequired { .. } => {
                return Err("native Reload entered cleanup recovery".into());
            }
            _ if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            _ => return Err("native Reload successor did not become Ready".into()),
        }
    }
}

async fn wait_for_cleanup_failed(
    client: &loxa_ipc::ServiceClient,
    retiring: &OperationTarget,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status = runtime_status(client).await?;
        match status.phase {
            RuntimePhase::CleanupFailed {
                task_id,
                generation,
                ..
            } if status.boot_epoch == retiring.boot_epoch
                && task_id == retiring.task_id
                && generation == retiring.generation =>
            {
                return Ok(())
            }
            _ if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            _ => return Err("native Reload did not retain the failed retiring operation".into()),
        }
    }
}

async fn assert_applied_target(
    client: &loxa_ipc::ServiceClient,
    expected: &OperationTarget,
    label: &str,
) -> Result<(), String> {
    if matches!(
        service_settings(client).await?.application,
        ServiceSettingsApplication::Applied { target, .. } if target == *expected
    ) {
        Ok(())
    } else {
        Err(format!(
            "{label} did not retain the retiring applied receipt"
        ))
    }
}

async fn unload(client: &loxa_ipc::ServiceClient, target: OperationTarget) -> Result<(), String> {
    match client
        .request(
            ConnectMode::ObserveExisting,
            ServiceCommand::Unload {
                target: target.clone(),
            },
        )
        .await
        .map_err(client::client_error)?
    {
        ReplyOutcome::Accepted(accepted) if accepted_target(&accepted) == target => Ok(()),
        _ => Err("native Reload cleanup Unload returned the wrong reply".into()),
    }
}

fn require_no_launch_artifacts(fixture: &NativeService) -> Result<(), String> {
    let control = fixture.root.join("run/service");
    if control.join("launch-intent.json").exists()
        || std::fs::read_dir(&control)
            .map_err(|error| error.to_string())?
            .any(|entry| {
                entry
                    .ok()
                    .and_then(|entry| entry.file_name().into_string().ok())
                    .is_some_and(|name| name.starts_with("engine-") && name.ends_with(".sock"))
            })
    {
        return Err("cancelled Reload published successor launch artifacts".into());
    }
    Ok(())
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
