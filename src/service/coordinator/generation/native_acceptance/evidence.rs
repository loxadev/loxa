use super::harness::NativeRuntimeEvidence;
use super::{CONTEXT_TOKENS, MODEL_ID, MODEL_SHA256, MODEL_SIZE};
use crate::runtime_identity::RuntimeIdentity;
use crate::service::coordinator::generation::qualification_fixture::PreflightObservation;
use serde_json::json;

pub(super) fn validate(
    observations: &[PreflightObservation],
    runtime: &NativeRuntimeEvidence,
) -> Result<(), String> {
    if observations.len() != 4 {
        return Err(format!(
            "native generation recorded {} preflights instead of four",
            observations.len()
        ));
    }
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
        .any(|observation| observation.template_sha256 != observations[0].template_sha256)
    {
        return Err("native generation template basis changed between preflights".into());
    }
    for observation in &observations[..2] {
        if !observation.postcommit_gate_entered
            || !observation.generation_dispatched
            || !observation.basis_carried_to_execution
            || observation.completion_prompt_tokens != Some(observation.input_tokens)
            || observation
                .completion_cached_tokens
                .is_some_and(|cached| cached > observation.input_tokens)
            || !observation.quiescent_after_terminal
        {
            return Err("native completion did not preserve count, basis, or idle evidence".into());
        }
    }
    if observations[0].input_tokens != observations[1].input_tokens {
        return Err("identical UTF-8 prompts changed token counts after cache reuse".into());
    }
    if !observations[1]
        .completion_cached_tokens
        .is_some_and(|cached| cached > 0)
    {
        return Err("repeated native generation did not report prompt-cache reuse".into());
    }
    let overflow = &observations[2];
    if overflow.max_output_tokens != CONTEXT_TOKENS
        || overflow.postcommit_gate_entered
        || overflow.generation_dispatched
        || overflow.completion_prompt_tokens.is_some()
        || overflow.completion_cached_tokens.is_some()
        || overflow.quiescent_after_terminal
        || overflow.input_tokens == 0
    {
        return Err("native context overflow crossed the generation dispatch gate".into());
    }
    let stopped = &observations[3];
    if !stopped.postcommit_gate_entered
        || stopped.generation_dispatched
        || !stopped.basis_carried_to_execution
        || stopped.completion_prompt_tokens.is_some()
        || stopped.completion_cached_tokens.is_some()
        || stopped.quiescent_after_terminal
    {
        return Err("native postcommit Stop crossed the engine request boundary".into());
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
    serde_json::to_string_pretty(&json!({
        "runtime_build": &observations[0].runtime_build,
        "runtime_commit": &runtime.commit,
        "runtime_version_line": &runtime.version_line,
        "runtime_server_sha256": &runtime.server_sha256,
        "runtime_server_size": runtime.server_size,
        "runtime_inventory_sha256": &runtime.inventory_sha256,
        "model_sha256": &observations[0].primary_sha256,
        "model_size": observations[0].primary_size,
        "template_sha256": &observations[0].template_sha256,
        "actual_context": observations[0].actual_context,
        "count_usage_pairs": observations[..2]
            .iter()
            .map(|observation| json!({
                "input_tokens": observation.input_tokens,
                "completion_prompt_tokens": observation.completion_prompt_tokens,
                "completion_cached_tokens": observation.completion_cached_tokens,
                "quiescent_after_terminal": observation.quiescent_after_terminal,
            }))
            .collect::<Vec<_>>(),
        "busy_preflight_count": 0,
        "overflow": {
            "input_tokens": observations[2].input_tokens,
            "output_reservation": observations[2].max_output_tokens,
            "generation_dispatched": observations[2].generation_dispatched,
        },
        "postcommit_stop": {
            "gate_entered": observations[3].postcommit_gate_entered,
            "generation_dispatched": observations[3].generation_dispatched,
            "exact_engine_cleanup": cleanup_verified,
        },
    }))
    .expect("native qualification report is serializable")
}
