use super::super::state::AdmissionReservation;
use super::transport::PreparedEngineRequest;
use crate::history::PromptPreparation;
use crate::runtime_fingerprint::RuntimeFingerprint;
use crate::runtime_identity::RuntimeIdentity;
use hyper::body::Bytes;
use hyper::Method;
use loxa_ipc::{EffectiveSamplingSettings, ErrorCategory, SamplingValue, ServiceError};
use serde::Deserialize;
use std::time::Duration;

const MAX_TEMPLATE_BASIS_BYTES: usize = 64 * 1024;
// One total control budget leaves headroom under the client's five-second reply wait.
const PREFLIGHT_CONTROL_TIMEOUT: Duration = Duration::from_secs(4);

pub(super) struct QualifiedRequest {
    pub(super) request: PreparedEngineRequest,
    actual_context: u32,
    input_tokens: u32,
    effective_sampling: EffectiveSamplingSettings,
    #[cfg(all(test, target_os = "macos"))]
    template_sha256: [u8; 32],
    #[cfg(all(test, target_os = "macos"))]
    observation_id: Option<u64>,
}

impl QualifiedRequest {
    #[cfg(test)]
    pub(super) fn for_test() -> Self {
        Self {
            request: PreparedEngineRequest {
                body: Bytes::from_static(b"{}"),
            },
            actual_context: 4096,
            input_tokens: 1,
            effective_sampling: fallback_sampling(),
            #[cfg(target_os = "macos")]
            template_sha256: [0; 32],
            #[cfg(target_os = "macos")]
            observation_id: None,
        }
    }

    pub(super) fn actual_context(&self) -> u32 {
        self.actual_context
    }

    pub(super) fn input_tokens(&self) -> u32 {
        self.input_tokens
    }

    pub(super) fn effective_sampling(&self) -> EffectiveSamplingSettings {
        self.effective_sampling
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
    let effective_sampling = resolve_sampling(prompt, &props.default_generation_settings.params)?;
    let request = PreparedEngineRequest::new(prompt, effective_sampling)?;
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
        effective_sampling,
        #[cfg(all(test, target_os = "macos"))]
        template_sha256: qualification.template_sha256,
        #[cfg(all(test, target_os = "macos"))]
        observation_id,
    })
}

const FALLBACK_TEMPERATURE: f32 = 0.8;
const FALLBACK_TOP_P: f32 = 0.95;

fn resolve_sampling(
    prompt: &PromptPreparation,
    reported: &ReportedSampling,
) -> Result<EffectiveSamplingSettings, ServiceError> {
    resolve_sampling_values(prompt.temperature, prompt.top_p, reported)
}

fn resolve_sampling_values(
    temperature: Option<SamplingValue>,
    top_p: Option<SamplingValue>,
    reported: &ReportedSampling,
) -> Result<EffectiveSamplingSettings, ServiceError> {
    let fallback = fallback_sampling();
    Ok(EffectiveSamplingSettings {
        temperature: resolve_sampling_value(
            temperature,
            reported.temperature,
            fallback.temperature,
            "temperature",
            |value| value >= 0.0,
        )?,
        top_p: resolve_sampling_value(top_p, reported.top_p, fallback.top_p, "top P", |value| {
            (0.0..=1.0).contains(&value)
        })?,
    })
}

fn fallback_sampling() -> EffectiveSamplingSettings {
    EffectiveSamplingSettings {
        temperature: SamplingValue::new(f64::from(FALLBACK_TEMPERATURE)).expect("finite fallback"),
        top_p: SamplingValue::new(f64::from(FALLBACK_TOP_P)).expect("finite fallback"),
    }
}

fn resolve_sampling_value(
    explicit: Option<SamplingValue>,
    reported: Option<f64>,
    fallback: SamplingValue,
    name: &'static str,
    in_range: fn(f64) -> bool,
) -> Result<SamplingValue, ServiceError> {
    if let Some(value) = explicit {
        return canonical_engine_value(value.get(), in_range).ok_or_else(|| {
            ServiceError::new(
                ErrorCategory::InvalidRequest,
                format!("conversation {name} is outside the engine request range"),
            )
        });
    }
    Ok(reported
        .and_then(|value| canonical_engine_value(value, in_range))
        .unwrap_or(fallback))
}

fn canonical_engine_value(value: f64, in_range: fn(f64) -> bool) -> Option<SamplingValue> {
    if !value.is_finite() || !in_range(value) {
        return None;
    }
    let narrowed = value as f32;
    if !narrowed.is_finite() || (value > 0.0 && narrowed == 0.0) {
        return None;
    }
    SamplingValue::new(f64::from(narrowed))
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
    params: ReportedSampling,
}

#[derive(Deserialize)]
struct ReportedSampling {
    temperature: Option<f64>,
    top_p: Option<f64>,
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
            default_generation_settings: DefaultGenerationSettings {
                n_ctx: 4096,
                params: ReportedSampling {
                    temperature: None,
                    top_p: None,
                },
            },
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
    fn props_sampling_defaults_resolve_with_explicit_priority_and_fallbacks() {
        let props: Props = serde_json::from_str(
            r#"{
                "default_generation_settings": {
                    "params": {
                        "seed": -1,
                        "temperature": 0.65,
                        "top_k": 40,
                        "top_p": 0.7,
                        "min_p": 0.05
                    },
                    "n_ctx": 4096
                },
                "total_slots": 1,
                "model_alias": "loxa-generation-fixture",
                "endpoint_slots": true,
                "chat_template": "{{ messages }}",
                "modalities": {"vision": false}
            }"#,
        )
        .unwrap();
        let explicit = resolve_sampling_values(
            Some(SamplingValue::new(0.0).unwrap()),
            None,
            &props.default_generation_settings.params,
        )
        .unwrap();
        assert_eq!(explicit.temperature.get(), 0.0);
        assert_eq!(explicit.top_p.get(), f64::from(0.7_f32));

        let reported =
            resolve_sampling_values(None, None, &props.default_generation_settings.params).unwrap();
        assert_eq!(reported.temperature.get(), f64::from(0.65_f32));
        assert_eq!(reported.top_p.get(), f64::from(0.7_f32));

        let partial = ReportedSampling {
            temperature: Some(0.4),
            top_p: None,
        };
        let partial = resolve_sampling_values(None, None, &partial).unwrap();
        assert_eq!(partial.temperature.get(), f64::from(0.4_f32));
        assert_eq!(partial.top_p.get(), f64::from(FALLBACK_TOP_P));

        let unusable = ReportedSampling {
            temperature: Some(f64::MAX),
            top_p: Some(f64::MIN_POSITIVE),
        };
        let fallback = resolve_sampling_values(None, None, &unusable).unwrap();
        assert_eq!(fallback.temperature.get(), f64::from(0.8_f32));
        assert_eq!(fallback.top_p.get(), f64::from(0.95_f32));
        let out_of_range = ReportedSampling {
            temperature: Some(-1.0),
            top_p: Some(1.1),
        };
        assert_eq!(
            resolve_sampling_values(None, None, &out_of_range).unwrap(),
            fallback
        );

        assert!(resolve_sampling_values(
            Some(SamplingValue::new(f64::MAX).unwrap()),
            None,
            &props.default_generation_settings.params,
        )
        .is_err());
        assert!(resolve_sampling_values(
            Some(SamplingValue::new(-1.0).unwrap()),
            None,
            &props.default_generation_settings.params,
        )
        .is_err());
        assert!(resolve_sampling_values(
            None,
            Some(SamplingValue::new(f64::MIN_POSITIVE).unwrap()),
            &props.default_generation_settings.params,
        )
        .is_err());
        assert!(resolve_sampling_values(
            None,
            Some(SamplingValue::new(1.1).unwrap()),
            &props.default_generation_settings.params,
        )
        .is_err());
        assert!(serde_json::from_str::<Props>(
            r#"{"default_generation_settings":{"n_ctx":4096},"total_slots":1,
                "model_alias":"loxa-generation-fixture","endpoint_slots":true,
                "chat_template":"{{ messages }}"}"#,
        )
        .is_err());
        assert!(serde_json::from_str::<Props>(
            r#"{"default_generation_settings":{"params":{"temperature":"hot"},"n_ctx":4096},
                "total_slots":1,"model_alias":"loxa-generation-fixture",
                "endpoint_slots":true,"chat_template":"{{ messages }}"}"#,
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
