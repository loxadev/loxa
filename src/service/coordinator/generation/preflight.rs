use super::super::state::AdmissionReservation;
use super::transport::PreparedEngineRequest;
use crate::history::PromptPreparation;
use crate::runtime_fingerprint::RuntimeFingerprint;
use crate::runtime_identity::RuntimeIdentity;
use hyper::body::Bytes;
use hyper::Method;
use loxa_ipc::{ErrorCategory, ServiceError};
use serde::Deserialize;
use std::time::Duration;

const MAX_TEMPLATE_BASIS_BYTES: usize = 64 * 1024;
// One total control budget leaves headroom under the client's five-second reply wait.
const PREFLIGHT_CONTROL_TIMEOUT: Duration = Duration::from_secs(4);

pub(super) struct QualifiedRequest {
    pub(super) request: PreparedEngineRequest,
    actual_context: u32,
    input_tokens: u32,
    #[cfg(all(test, target_os = "macos"))]
    template_sha256: [u8; 32],
    #[cfg(all(test, target_os = "macos"))]
    observation_id: Option<u64>,
}

impl QualifiedRequest {
    pub(super) fn actual_context(&self) -> u32 {
        self.actual_context
    }

    pub(super) fn input_tokens(&self) -> u32 {
        self.input_tokens
    }

    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn template_sha256(&self) -> [u8; 32] {
        self.template_sha256
    }

    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn observation_id(&self) -> Option<u64> {
        self.observation_id
    }
}

pub(super) async fn qualify(
    runtime_identity: RuntimeIdentity,
    reservation: &AdmissionReservation,
    prompt: &PromptPreparation,
) -> Result<QualifiedRequest, ServiceError> {
    tokio::time::timeout(
        PREFLIGHT_CONTROL_TIMEOUT,
        qualify_engine(runtime_identity, reservation, prompt),
    )
    .await
    .map_err(|_| {
        ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "engine preflight timed out before generation admission",
        )
    })?
}

async fn qualify_engine(
    runtime_identity: RuntimeIdentity,
    reservation: &AdmissionReservation,
    prompt: &PromptPreparation,
) -> Result<QualifiedRequest, ServiceError> {
    let props = super::transport::bounded_json_request(
        &reservation.engine,
        reservation,
        Method::GET,
        "/props",
        Bytes::new(),
    )
    .await?;
    let props: Props = serde_json::from_slice(&props).map_err(|_| {
        ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "engine properties response is invalid",
        )
    })?;
    let Some(qualification) =
        exact_preflight_qualification(runtime_identity, &reservation.fingerprint, &props)
    else {
        return Err(ServiceError::new(
            ErrorCategory::UnsupportedCapability,
            "engine prompt template and tokenizer basis is not qualified",
        ));
    };
    let request = PreparedEngineRequest::new(prompt)?;
    let counted = super::transport::bounded_json_request(
        &reservation.engine,
        reservation,
        Method::POST,
        "/v1/chat/completions/input_tokens",
        request.body.clone(),
    )
    .await?;
    let counted: InputTokens = serde_json::from_slice(&counted).map_err(|_| {
        ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "engine token-count response is invalid",
        )
    })?;
    if counted.object != "response.input_tokens" {
        return Err(ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "engine token-count response has the wrong object type",
        ));
    }
    #[cfg(all(test, target_os = "macos"))]
    let observation_id = super::qualification_fixture::record_preflight(
        runtime_identity.build(),
        &reservation.fingerprint,
        qualification.template_sha256,
        qualification.actual_context,
        counted.input_tokens,
        prompt.max_output_tokens,
    );
    validate_context(
        qualification.actual_context,
        counted.input_tokens,
        prompt.max_output_tokens,
    )?;
    Ok(QualifiedRequest {
        request,
        actual_context: qualification.actual_context,
        input_tokens: counted.input_tokens,
        #[cfg(all(test, target_os = "macos"))]
        template_sha256: qualification.template_sha256,
        #[cfg(all(test, target_os = "macos"))]
        observation_id,
    })
}

struct PreflightQualification {
    actual_context: u32,
    #[cfg(test)]
    template_sha256: [u8; 32],
}

fn exact_preflight_qualification(
    runtime_identity: RuntimeIdentity,
    fingerprint: &RuntimeFingerprint,
    props: &Props,
) -> Option<PreflightQualification> {
    if runtime_identity != RuntimeIdentity::BundledB10344
        || fingerprint
            .validate_service_lease(fingerprint.model_id())
            .is_err()
        || props.model_alias != fingerprint.model_id()
        || props.total_slots != 1
        || !props.endpoint_slots
        || props.default_generation_settings.n_ctx != fingerprint.effective_context()
        || props.chat_template.is_empty()
        || props.chat_template.len() > MAX_TEMPLATE_BASIS_BYTES
        || props.chat_template.capacity() > MAX_TEMPLATE_BASIS_BYTES
    {
        return None;
    }

    #[cfg(test)]
    if let Some(template_sha256) =
        super::qualification_fixture::template_digest(fingerprint, props.chat_template.as_bytes())
    {
        return Some(PreflightQualification {
            actual_context: props.default_generation_settings.n_ctx,
            template_sha256,
        });
    }

    // No production runtime, model, and template basis is qualified yet.
    None
}

#[derive(Deserialize)]
struct Props {
    default_generation_settings: DefaultGenerationSettings,
    total_slots: u32,
    model_alias: String,
    endpoint_slots: bool,
    chat_template: String,
}

#[derive(Deserialize)]
struct DefaultGenerationSettings {
    n_ctx: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputTokens {
    input_tokens: u32,
    object: String,
}

fn validate_context(
    actual_context: u32,
    input_tokens: u32,
    max_output_tokens: i64,
) -> Result<(), ServiceError> {
    let output = u32::try_from(max_output_tokens).map_err(|_| {
        ServiceError::new(
            ErrorCategory::InvalidRequest,
            "conversation output reservation is invalid",
        )
    })?;
    if input_tokens
        .checked_add(output)
        .is_none_or(|required| required > actual_context)
    {
        return Err(ServiceError::new(
            ErrorCategory::InvalidRequest,
            "qualified prompt and output reservation exceed the engine context",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_props() -> Props {
        Props {
            default_generation_settings: DefaultGenerationSettings { n_ctx: 4096 },
            total_slots: 1,
            model_alias: super::super::qualification_fixture::MODEL_ID.into(),
            endpoint_slots: true,
            chat_template: "{{ messages }}".into(),
        }
    }

    #[test]
    fn actual_context_includes_the_output_reservation() {
        assert!(validate_context(4096, 3584, 512).is_ok());
        assert_eq!(
            validate_context(4096, 3585, 512).unwrap_err().category,
            ErrorCategory::InvalidRequest
        );
        assert!(validate_context(4096, u32::MAX, 1).is_err());
    }

    #[test]
    fn token_count_shape_is_closed() {
        let valid: InputTokens =
            serde_json::from_str(r#"{"input_tokens":12,"object":"response.input_tokens"}"#)
                .unwrap();
        assert_eq!(valid.input_tokens, 12);
        assert_eq!(valid.object, "response.input_tokens");
        assert!(serde_json::from_str::<InputTokens>(
            r#"{"input_tokens":12,"object":"response.input_tokens","extra":1}"#
        )
        .is_err());
    }

    #[test]
    fn fixture_candidate_is_scoped_to_the_exact_runtime_model_and_props() {
        let fingerprint = super::super::qualification_fixture::fingerprint(
            super::super::qualification_fixture::MODEL_SHA256,
        );
        let props = fixture_props();
        let qualified =
            exact_preflight_qualification(RuntimeIdentity::BundledB10344, &fingerprint, &props)
                .unwrap();
        assert_eq!(qualified.actual_context, 4096);
        let expected_template_sha256 = super::super::qualification_fixture::template_digest(
            &fingerprint,
            props.chat_template.as_bytes(),
        )
        .unwrap();
        assert_eq!(qualified.template_sha256, expected_template_sha256,);

        assert!(exact_preflight_qualification(
            RuntimeIdentity::LegacyCliB10121,
            &fingerprint,
            &props,
        )
        .is_none());
        assert!(exact_preflight_qualification(
            RuntimeIdentity::BundledB10344,
            &super::super::qualification_fixture::fingerprint(&"a5".repeat(32)),
            &props,
        )
        .is_none());

        let mut wrong_props = fixture_props();
        wrong_props.total_slots = 2;
        assert!(exact_preflight_qualification(
            RuntimeIdentity::BundledB10344,
            &fingerprint,
            &wrong_props,
        )
        .is_none());
        wrong_props = fixture_props();
        wrong_props.default_generation_settings.n_ctx = 8192;
        assert!(exact_preflight_qualification(
            RuntimeIdentity::BundledB10344,
            &fingerprint,
            &wrong_props,
        )
        .is_none());
        wrong_props = fixture_props();
        wrong_props.chat_template.clear();
        assert!(exact_preflight_qualification(
            RuntimeIdentity::BundledB10344,
            &fingerprint,
            &wrong_props,
        )
        .is_none());
    }
}
