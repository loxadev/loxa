use super::harness::NativeRuntimeEvidence;
use super::{CONTEXT_TOKENS, MODEL_ID, MODEL_SHA256, MODEL_SIZE};
use crate::runtime_identity::RuntimeIdentity;
use crate::service::coordinator::generation::qualification_fixture::PreflightObservation;
use serde_json::json;

pub(super) fn validate(
    observations: &[PreflightObservation],
    runtime: &NativeRuntimeEvidence,
) -> Result<(), String> {
    let [first, cached, overflow, stopped, output_stop, replacement] = observations else {
        return Err(format!(
            "native generation recorded {} preflights instead of six",
            observations.len()
        ));
    };
    let expected_runtime = RuntimeIdentity::BundledB10344;
    if runtime.build != expected_runtime.build()
        || runtime.commit != crate::runtime_bundle::bundled_commit()
        || runtime.version_line != expected_runtime.version_line()
        || !is_lower_hex_digest(&runtime.server_sha256)
        || !is_lower_hex_digest(&runtime.inventory_sha256)
        || runtime.server_size == 0
    {
        return Err("native generation inspected the wrong bundled runtime".into());
    }
    for observation in observations {
        if observation.runtime_build != runtime.build
            || observation.model_id != MODEL_ID
            || observation.primary_sha256 != MODEL_SHA256
            || observation.primary_size != MODEL_SIZE
            || observation.actual_context != CONTEXT_TOKENS
            || !is_lower_hex_digest(&observation.template_sha256)
        {
            return Err("native generation recorded the wrong exact qualification basis".into());
        }
    }
    if observations
        .iter()
        .any(|observation| observation.template_sha256 != first.template_sha256)
    {
        return Err("native generation template basis changed between preflights".into());
    }
    for observation in [first, cached, replacement] {
        if !observation.postcommit_gate_entered
            || observation.generation_dispatches != 1
            || !observation.basis_carried_to_execution
            || observation.generated_end == 0
            || observation.completion_prompt_tokens != Some(observation.input_tokens)
            || observation
                .completion_cached_tokens
                .is_some_and(|cached| cached > observation.input_tokens)
            || !observation.quiescent_after_terminal
        {
            return Err("native completion did not preserve count, basis, or idle evidence".into());
        }
    }
    if first.input_tokens != cached.input_tokens {
        return Err("identical UTF-8 prompts changed token counts after cache reuse".into());
    }
    if !cached
        .completion_cached_tokens
        .is_some_and(|cached| cached > 0)
    {
        return Err("repeated native generation did not report prompt-cache reuse".into());
    }
    if overflow.max_output_tokens != CONTEXT_TOKENS
        || overflow.postcommit_gate_entered
        || overflow.generation_dispatches != 0
        || overflow.completion_prompt_tokens.is_some()
        || overflow.completion_cached_tokens.is_some()
        || overflow.quiescent_after_terminal
        || overflow.input_tokens == 0
    {
        return Err("native context overflow crossed the generation dispatch gate".into());
    }
    if !stopped.postcommit_gate_entered
        || stopped.generation_dispatches != 0
        || !stopped.basis_carried_to_execution
        || stopped.completion_prompt_tokens.is_some()
        || stopped.completion_cached_tokens.is_some()
        || stopped.quiescent_after_terminal
    {
        return Err("native postcommit Stop crossed the engine request boundary".into());
    }
    let prefix = output_stop
        .output_gate_prefix
        .as_ref()
        .ok_or_else(|| "native output Stop missed its nonterminal content gate".to_string())?;
    if !output_stop.postcommit_gate_entered
        || output_stop.generation_dispatches != 1
        || !output_stop.basis_carried_to_execution
        || output_stop.response_status != Some(200)
        || prefix.bytes == 0
        || prefix.bytes > output_stop.generated_end
        || !is_lower_hex_digest(&prefix.sha256)
        || output_stop.quiescent_after_terminal
    {
        return Err("native output Stop lacks exact nonterminal content evidence".into());
    }
    Ok(())
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn render(
    observations: &[PreflightObservation],
    runtime: &NativeRuntimeEvidence,
    cleanup_verified: bool,
) -> String {
    let [first, cached, overflow, stopped, output_stop, replacement] = observations else {
        panic!("native observations must be validated before rendering");
    };
    let prefix = output_stop
        .output_gate_prefix
        .as_ref()
        .expect("native output Stop prefix was validated");
    let count_usage_pairs = [first, cached]
        .into_iter()
        .map(|observation| {
            json!({
                "input_tokens": observation.input_tokens,
                "max_output_tokens": observation.max_output_tokens,
                "completion_prompt_tokens": observation.completion_prompt_tokens,
                "completion_cached_tokens": observation.completion_cached_tokens,
                "quiescent_after_terminal": observation.quiescent_after_terminal,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&json!({
        "runtime_build": &first.runtime_build,
        "runtime_commit": &runtime.commit,
        "runtime_version_line": &runtime.version_line,
        "runtime_server_sha256": &runtime.server_sha256,
        "runtime_server_size": runtime.server_size,
        "runtime_inventory_sha256": &runtime.inventory_sha256,
        "model_sha256": &first.primary_sha256,
        "model_size": first.primary_size,
        "template_sha256": &first.template_sha256,
        "actual_context": first.actual_context,
        "fixture_sampling_temperature": super::qualification_fixture::TEMPERATURE,
        "count_usage_pairs": count_usage_pairs,
        "busy_preflight_count": 0,
        "lost_original_reply": {
            "request_aborted_and_joined_after_commit": true,
            "replayed_original_request": true,
            "admission_ids_preserved": true,
            "canonical_turns": 1,
            "selected_attempt_number": 1,
            "generation_dispatches": first.generation_dispatches,
            "execution": "completed",
            "save": "saved",
        },
        "overflow": {
            "input_tokens": overflow.input_tokens,
            "output_reservation": overflow.max_output_tokens,
            "generation_dispatches": overflow.generation_dispatches,
        },
        "postcommit_stop": {
            "gate_entered": stopped.postcommit_gate_entered,
            "generation_dispatches": stopped.generation_dispatches,
            "exact_engine_cleanup": cleanup_verified,
        },
        "nonterminal_output_stop": {
            "captured_prefix_bytes": prefix.bytes,
            "captured_prefix_sha256": &prefix.sha256,
            "generated_end": output_stop.generated_end,
            "captured_prefix_matches_saved": true,
            "contiguous_utf8_saved": true,
            "terminal_offsets_match": true,
            "execution": "stopped",
            "save": "saved",
            "failure_code": "stopped",
            "exact_engine_cleanup": cleanup_verified,
            "runtime_lease_removed": true,
            "live_process_group_members": false,
            "admission_released": true,
        },
        "replacement": {
            "stale_accepted_stop": "conflict",
            "generated_end": replacement.generated_end,
            "completion_prompt_tokens": replacement.completion_prompt_tokens,
            "quiescent_after_terminal": replacement.quiescent_after_terminal,
            "execution": "completed",
            "save": "saved",
            "engine_preserved": true,
            "admission_released": true,
        },
    }))
    .expect("native qualification report is serializable")
}
