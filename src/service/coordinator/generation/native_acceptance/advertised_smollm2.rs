use super::client::{
    create_conversation, create_draft, list_turns, read_draft, require_rejected,
    require_same_draft, reserve_full_context, retry, send, wait_for_saved,
};
use super::harness::{NativeEngine, NativeService};
use super::{evidence, qualification_fixture, CONTEXT_TOKENS, GENERATION_TOKENS};
use serde_json::{json, Value};
use std::path::Path;

const SYSTEM: &str = "Reply briefly in English. Preserve café, 你好 and 👋 when asked.";
const FIRST_USER: &str = "Reply with the English word hello.";
const NEXT_USER: &str = "Now reply briefly about this Unicode input: café 你好 👋.";
const OVERFLOW_USER: &str =
    "This next café 你好 👋 message must be rejected with all output reserved.";
const MAX_REPORT_BYTES: usize = 256 * 1024;

pub(in crate::service) async fn run_advertised_smollm2_acceptance(
    app: &Path,
    source_model: &Path,
    output: &Path,
) -> Result<String, String> {
    qualification_fixture::clear_observations();
    let model = qualification_fixture::advertised_smollm2_manifest();
    let mut fixture =
        match NativeService::start_with_manifest(app, source_model, model.clone()).await {
            Ok(fixture) => fixture,
            Err(error) => {
                write_report(
                    output,
                    &json!({
                        "status": "candidate_failed",
                        "model": model,
                        "startup_error": error,
                        "preflights": qualification_fixture::observations(),
                    }),
                )?;
                return Err(error);
            }
        };
    let exercised = exercise(&fixture).await;
    let final_engine = exercised
        .as_ref()
        .ok()
        .map(|_| NativeEngine::capture(&fixture));
    let cleanup = fixture
        .shutdown(true)
        .await
        .and_then(|()| match final_engine {
            Some(engine) => engine?.require_removed(),
            None => Ok(()),
        });
    let passed = exercised.is_ok() && cleanup.is_ok();
    let report = json!({
        "status": if passed { "candidate_passed" } else { "candidate_failed" },
        "model": model,
        "runtime": fixture.runtime_evidence,
        "evidence": exercised.as_ref().ok(),
        "error": exercised.as_ref().err(),
        "shutdown_error": cleanup.as_ref().err(),
        "exact_final_engine_cleanup": passed,
        "preflights": qualification_fixture::observations(),
    });
    let encoded = write_report(output, &report)?;
    match (exercised, cleanup) {
        (Ok(_), Ok(())) => Ok(encoded),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
    }
}

async fn exercise(fixture: &NativeService) -> Result<Value, String> {
    let conversation = create_conversation(
        &fixture.client,
        &fixture.model.id,
        GENERATION_TOKENS,
        Some(SYSTEM),
    )
    .await?;
    let first = send(
        fixture.client.clone(),
        conversation.clone(),
        FIRST_USER,
        None,
        1,
    )
    .await
    .map_err(super::client::client_error)?;
    let first_output = wait_for_saved(&fixture.client, &conversation, &first, "Q4 first").await?;
    if first_output.is_empty() {
        return Err("Q4 first generation saved no assistant content".into());
    }
    fixture.wait_for_admission_release().await?;
    let conversation = conversation.after(&first)?;
    let next = send(
        fixture.client.clone(),
        conversation.clone(),
        NEXT_USER,
        None,
        2,
    )
    .await
    .map_err(super::client::client_error)?;
    let next_output = wait_for_saved(&fixture.client, &conversation, &next, "Q4 history").await?;
    if next_output.is_empty() {
        return Err("Q4 full-history generation saved no assistant content".into());
    }
    fixture.wait_for_admission_release().await?;
    let cached = retry(fixture.client.clone(), conversation.clone(), &next, 3)
        .await
        .map_err(super::client::client_error)?;
    let cached_output =
        wait_for_saved(&fixture.client, &conversation, &cached, "Q4 cached Retry").await?;
    if cached_output.is_empty()
        || cached.turn_id != next.turn_id
        || cached.attempt_id == next.attempt_id
    {
        return Err("Q4 Retry failed to replace the selected assistant attempt".into());
    }
    fixture.wait_for_admission_release().await?;
    let conversation = reserve_full_context(&fixture.client, conversation.after(&cached)?).await?;
    let before = list_turns(&fixture.client, &conversation.id).await?;
    if before.len() != 2
        || !before.iter().any(|turn| {
            turn.id == next.turn_id
                && turn.selected_attempt.as_ref().is_some_and(|attempt| {
                    attempt.id == cached.attempt_id && attempt.attempt_number == "2"
                })
        })
    {
        return Err("Q4 conversation did not retain two turns and the selected Retry".into());
    }
    let draft = create_draft(&fixture.client, &conversation, OVERFLOW_USER).await?;
    require_rejected(
        send(
            fixture.client.clone(),
            conversation.clone(),
            OVERFLOW_USER,
            Some(&draft),
            4,
        )
        .await,
        loxa_ipc::ErrorCategory::InvalidRequest,
        "Q4 full-history overflow",
    )?;
    require_same_draft(&draft, &read_draft(&fixture.client, &draft).await?)?;
    if list_turns(&fixture.client, &conversation.id).await? != before {
        return Err("Q4 full-history overflow changed durable turns or attempts".into());
    }
    fixture.wait_for_admission_release().await?;
    super::stop_after_content_and_reject_stale_target(fixture, NEXT_USER).await?;

    let observations = qualification_fixture::observations();
    let [first, history, cached, overflow, stopped, replacement] = observations.as_slice() else {
        return Err(format!(
            "Q4 recorded {} preflights instead of six",
            observations.len()
        ));
    };
    evidence::validate_basis(&observations, &fixture.runtime_evidence, &fixture.model)?;
    for observation in [first, history, cached, replacement] {
        evidence::validate_completion(observation)?;
    }
    if first.input_tokens == 0
        || history.input_tokens <= first.input_tokens
        || cached.input_tokens != history.input_tokens
        || !cached
            .completion_cached_tokens
            .is_some_and(|count| count > 0)
    {
        return Err("Q4 history or cached Retry lacks exact native count/usage evidence".into());
    }
    if overflow.input_tokens == 0
        || overflow.max_output_tokens != CONTEXT_TOKENS
        || overflow.postcommit_gate_entered
        || overflow.generation_dispatches != 0
        || overflow.completion_prompt_tokens.is_some()
        || overflow.completion_cached_tokens.is_some()
        || overflow.quiescent_after_terminal
    {
        return Err("Q4 full-history overflow crossed durable admission or dispatch".into());
    }
    evidence::validate_output_stop(stopped)?;
    let first_messages = json!([
        {"role": "system", "content": SYSTEM}, {"role": "user", "content": FIRST_USER},
    ]);
    let history_messages = json!([
        {"role": "system", "content": SYSTEM}, {"role": "user", "content": FIRST_USER},
        {"role": "assistant", "content": first_output}, {"role": "user", "content": NEXT_USER},
    ]);
    let overflow_messages = json!([
        {"role": "system", "content": SYSTEM}, {"role": "user", "content": FIRST_USER},
        {"role": "assistant", "content": first_output}, {"role": "user", "content": NEXT_USER},
        {"role": "assistant", "content": cached_output}, {"role": "user", "content": OVERFLOW_USER},
    ]);
    for (observation, expected) in [
        (first, &first_messages),
        (history, &history_messages),
        (cached, &history_messages),
        (overflow, &overflow_messages),
    ] {
        let request: Value =
            serde_json::from_str(&observation.request_body).map_err(|error| error.to_string())?;
        if request["messages"] != *expected
            || request["model"] != fixture.model.id
            || request["max_completion_tokens"] != observation.max_output_tokens
        {
            return Err(
                "Q4 token counting omitted or changed exact system/history/user bytes".into(),
            );
        }
    }
    Ok(json!({
        "template_file": "chat-template.jinja",
        "template_bytes": first.template.len(),
        "template_sha256": first.template_sha256,
        "actual_context": first.actual_context,
        "full_history_messages": history_messages,
        "overflow_messages": overflow_messages,
        "overflow_turns_before": before.len(),
        "overflow_turns_after": before.len(),
        "overflow_draft_unchanged": true,
        "nonterminal_stop_prefix": stopped.output_gate_prefix,
        "nonterminal_stop_saved_prefix_verified": true,
        "stale_stop_rejected_and_replacement_preserved": true,
    }))
}

fn write_report(output: &Path, report: &Value) -> Result<String, String> {
    let encoded = serde_json::to_string_pretty(report).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_REPORT_BYTES {
        return Err("Q4 qualification report exceeded 256 KiB".into());
    }
    std::fs::create_dir_all(output).map_err(|error| error.to_string())?;
    if let Some(first) = qualification_fixture::observations().first() {
        std::fs::write(output.join("chat-template.jinja"), &first.template)
            .map_err(|error| error.to_string())?;
    }
    std::fs::write(output.join("qualification.json"), &encoded)
        .map_err(|error| error.to_string())?;
    Ok(encoded)
}
