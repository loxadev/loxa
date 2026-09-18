use crate::catalog::Manifest;
use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
use sha2::{Digest, Sha256};

pub(super) const MODEL_ID: &str = "loxa-generation-fixture";
pub(super) const MODEL_SHA256: &str =
    "741ad12b64088fedc17c33aacb22e48be1972ef36a39f03666dd68bd15614fb9";
pub(super) const MODEL_SIZE: u64 = 88_202_080;
pub(super) const CONTEXT_TOKENS: u32 = 4096;

#[cfg(target_os = "macos")]
pub(super) fn manifest() -> Manifest {
    manifest_with_sha(MODEL_SHA256)
}

fn manifest_with_sha(sha256: &str) -> Manifest {
    serde_json::from_value(serde_json::json!({
        "version": 1,
        "id": MODEL_ID,
        "repo": "bartowski/SmolLM2-135M-Instruct-GGUF",
        "revision": "09816acd5d99df7be770d85ea30822623dab342c",
        "remote_filename": "SmolLM2-135M-Instruct-Q2_K.gguf",
        "local_filename": "model.gguf",
        "sha256": sha256,
        "size": MODEL_SIZE,
    }))
    .expect("fixed generation fixture manifest is valid")
}

pub(super) fn fingerprint(sha256: &str) -> RuntimeFingerprint {
    RuntimeFingerprint::from_manifest_for_service(
        &manifest_with_sha(sha256),
        CONTEXT_TOKENS,
        EffectiveProfile::Generic,
    )
    .expect("fixed generation fixture fingerprint is valid")
}

pub(super) fn template_digest(
    fingerprint: &RuntimeFingerprint,
    template: &[u8],
) -> Option<[u8; 32]> {
    (fingerprint.model_id() == MODEL_ID
        && fingerprint.effective_profile() == EffectiveProfile::Generic
        && fingerprint.primary_sha256() == MODEL_SHA256
        && fingerprint.primary_size() == MODEL_SIZE
        && fingerprint.draft_sha256().is_none()
        && fingerprint.draft_size().is_none())
    .then(|| Sha256::digest(template).into())
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug)]
pub(super) struct PreflightObservation {
    pub(super) id: u64,
    pub(super) runtime_build: String,
    pub(super) model_id: String,
    pub(super) primary_sha256: String,
    pub(super) primary_size: u64,
    pub(super) template_sha256: String,
    pub(super) actual_context: u32,
    pub(super) input_tokens: u32,
    pub(super) max_output_tokens: u32,
    pub(super) postcommit_gate_entered: bool,
    pub(super) generation_dispatched: bool,
    pub(super) basis_carried_to_execution: bool,
    pub(super) response_status: Option<u16>,
    pub(super) data_frames: usize,
    pub(super) data_bytes: usize,
    pub(super) generated_end: usize,
    pub(super) http_driver_completed: bool,
    pub(super) http_driver_error: bool,
    pub(super) completion_prompt_tokens: Option<u32>,
    pub(super) completion_cached_tokens: Option<u32>,
    pub(super) quiescent_after_terminal: bool,
}

#[cfg(target_os = "macos")]
const MAX_OBSERVATIONS: usize = 8;
#[cfg(target_os = "macos")]
static NEXT_OBSERVATION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
#[cfg(target_os = "macos")]
static OBSERVATIONS: std::sync::LazyLock<std::sync::Mutex<Vec<PreflightObservation>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::with_capacity(4)));

#[cfg(target_os = "macos")]
pub(super) fn record_preflight(
    runtime_build: &str,
    fingerprint: &RuntimeFingerprint,
    template_sha256: [u8; 32],
    actual_context: u32,
    input_tokens: u32,
    max_output_tokens: i64,
) -> Option<u64> {
    use std::sync::atomic::Ordering;

    let max_output_tokens = u32::try_from(max_output_tokens).ok()?;
    let mut observations = OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if observations.len() == MAX_OBSERVATIONS {
        return None;
    }
    let id = NEXT_OBSERVATION_ID.fetch_add(1, Ordering::Relaxed);
    observations.push(PreflightObservation {
        id,
        runtime_build: runtime_build.into(),
        model_id: fingerprint.model_id().into(),
        primary_sha256: fingerprint.primary_sha256().into(),
        primary_size: fingerprint.primary_size(),
        template_sha256: encode_digest(template_sha256),
        actual_context,
        input_tokens,
        max_output_tokens,
        postcommit_gate_entered: false,
        generation_dispatched: false,
        basis_carried_to_execution: false,
        response_status: None,
        data_frames: 0,
        data_bytes: 0,
        generated_end: 0,
        http_driver_completed: false,
        http_driver_error: false,
        completion_prompt_tokens: None,
        completion_cached_tokens: None,
        quiescent_after_terminal: false,
    });
    Some(id)
}

#[cfg(target_os = "macos")]
pub(super) fn clear_observations() {
    OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

#[cfg(target_os = "macos")]
pub(super) fn observations() -> Vec<PreflightObservation> {
    OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

#[cfg(target_os = "macos")]
pub(super) fn record_postcommit(id: Option<u64>, template_sha256: [u8; 32]) {
    with_observation(id, |observation| {
        observation.postcommit_gate_entered = true;
        observation.basis_carried_to_execution =
            observation.template_sha256 == encode_digest(template_sha256);
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_dispatch(id: Option<u64>) {
    with_observation(id, |observation| {
        observation.generation_dispatched = true;
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_response(id: Option<u64>, status: u16) {
    with_observation(id, |observation| {
        observation.response_status = Some(status);
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_data_frame(id: Option<u64>, bytes: usize) {
    with_observation(id, |observation| {
        observation.data_frames = observation.data_frames.saturating_add(1);
        observation.data_bytes = observation.data_bytes.saturating_add(bytes);
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_parse_progress(
    id: Option<u64>,
    generated_end: usize,
    prompt_tokens: Option<u32>,
) {
    with_observation(id, |observation| {
        observation.generated_end = generated_end;
        if prompt_tokens.is_some() {
            observation.completion_prompt_tokens = prompt_tokens;
        }
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_http_driver(id: Option<u64>, failed: bool) {
    with_observation(id, |observation| {
        observation.http_driver_completed = true;
        observation.http_driver_error = failed;
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_usage(
    id: Option<u64>,
    prompt_tokens: Option<u32>,
    cached_tokens: Option<u32>,
) {
    with_observation(id, |observation| {
        observation.completion_prompt_tokens = prompt_tokens;
        observation.completion_cached_tokens = cached_tokens;
    });
}

#[cfg(target_os = "macos")]
pub(super) fn record_quiescence(id: Option<u64>) {
    with_observation(id, |observation| {
        observation.quiescent_after_terminal = true;
    });
}

#[cfg(target_os = "macos")]
fn with_observation(id: Option<u64>, update: impl FnOnce(&mut PreflightObservation)) {
    let Some(id) = id else {
        return;
    };
    let mut observations = OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(observation) = observations
        .iter_mut()
        .find(|observation| observation.id == id)
    {
        update(observation);
    }
}

#[cfg(target_os = "macos")]
pub(super) fn encode_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}
