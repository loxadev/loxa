pub mod managed_llama;
pub mod ollama;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

pub const CANDIDATE_IDENTITY_SCHEMA_VERSION: u32 = 1;
pub const ARTIFACT_IDENTITY_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    ManagedLlamaCpp,
    Ollama,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOwnership {
    Controlled,
    Attached,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "checkpoint_attested_by", rename_all = "snake_case")]
pub enum CheckpointAttestation {
    Registry {
        reference: String,
    },
    Basename {
        value: String,
    },
    MetadataComposite {
        architecture: String,
        parameter_count: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactIdentity {
    pub schema_version: u32,
    pub artifact_id: String,
    pub digest_sha256: String,
    pub base_checkpoint: String,
    pub checkpoint_attestation: CheckpointAttestation,
    pub format: String,
    pub quantization: String,
    pub tokenizer_evidence: Vec<String>,
    pub template_evidence: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineRevision {
    Known(String),
    Unknown { hidden: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineIdentity {
    pub schema_version: u32,
    pub engine_kind: String,
    pub provider_version: String,
    pub engine_revision: EngineRevision,
    pub evidence: Vec<String>,
    pub invalidation_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GenerationSettings {
    pub schema_version: u32,
    pub context_tokens: u32,
    pub temperature: f32,
    pub seed: u64,
    pub concurrency: u32,
    pub max_output_tokens: u32,
}

impl GenerationSettings {
    pub fn pinned_v1() -> Self {
        Self {
            schema_version: 1,
            context_tokens: 4096,
            temperature: 0.0,
            seed: 42,
            concurrency: 1,
            max_output_tokens: 256,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateSpec {
    pub schema_version: u32,
    pub candidate_id: String,
    pub provider_kind: ProviderKind,
    pub ownership: ProviderOwnership,
    pub endpoint: String,
    pub artifact: ArtifactIdentity,
    pub engine: EngineIdentity,
    pub settings: GenerationSettings,
}

impl CandidateSpec {
    pub fn fingerprint(&self) -> String {
        let mut bytes = Vec::new();
        append_u32(&mut bytes, self.schema_version);
        append_str(&mut bytes, &self.candidate_id);
        append_str(
            &mut bytes,
            match self.provider_kind {
                ProviderKind::ManagedLlamaCpp => "managed_llama_cpp",
                ProviderKind::Ollama => "ollama",
            },
        );
        append_str(&mut bytes, &self.endpoint);
        append_str(
            &mut bytes,
            match self.ownership {
                ProviderOwnership::Controlled => "controlled",
                ProviderOwnership::Attached => "attached",
            },
        );
        append_u32(&mut bytes, self.artifact.schema_version);
        append_str(&mut bytes, &self.artifact.artifact_id);
        append_str(&mut bytes, &self.artifact.digest_sha256);
        append_str(&mut bytes, &self.artifact.base_checkpoint);
        match &self.artifact.checkpoint_attestation {
            CheckpointAttestation::Registry { reference } => {
                append_str(&mut bytes, "registry");
                append_str(&mut bytes, reference);
            }
            CheckpointAttestation::Basename { value } => {
                append_str(&mut bytes, "basename");
                append_str(&mut bytes, value);
            }
            CheckpointAttestation::MetadataComposite {
                architecture,
                parameter_count,
            } => {
                append_str(&mut bytes, "metadata_composite");
                append_str(&mut bytes, architecture);
                append_u64(&mut bytes, *parameter_count);
            }
        }
        append_str(&mut bytes, &self.artifact.format);
        append_str(&mut bytes, &self.artifact.quantization);
        append_strings(&mut bytes, &self.artifact.tokenizer_evidence);
        append_strings(&mut bytes, &self.artifact.template_evidence);
        append_u32(&mut bytes, self.engine.schema_version);
        append_str(&mut bytes, &self.engine.engine_kind);
        append_str(&mut bytes, &self.engine.provider_version);
        match &self.engine.engine_revision {
            EngineRevision::Known(revision) => {
                append_str(&mut bytes, "known");
                append_str(&mut bytes, revision);
            }
            EngineRevision::Unknown { hidden } => {
                append_str(&mut bytes, "unknown");
                append_str(&mut bytes, if *hidden { "hidden" } else { "unverified" });
            }
        }
        append_strings(&mut bytes, &self.engine.evidence);
        append_strings(&mut bytes, &self.engine.invalidation_keys);
        append_u32(&mut bytes, self.settings.schema_version);
        append_u32(&mut bytes, self.settings.context_tokens);
        append_str(&mut bytes, &self.settings.temperature.to_bits().to_string());
        append_str(&mut bytes, &self.settings.seed.to_string());
        append_u32(&mut bytes, self.settings.concurrency);
        append_u32(&mut bytes, self.settings.max_output_tokens);

        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub fn validate_pinned(&self) -> Result<(), ProviderError> {
        let (
            expected_candidate_id,
            expected_ownership,
            expected_endpoint,
            expected_artifact_id,
            expected_digest,
            expected_base,
            expected_engine_kind,
        ) = match self.provider_kind {
            ProviderKind::ManagedLlamaCpp => (
                "gemma-3-4b-it-q4",
                ProviderOwnership::Controlled,
                "managed://loxa-supervisor/llama-server",
                "gemma-3-4b-it-q4",
                "04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19",
                "google/gemma-3-4b-it",
                "llama.cpp",
            ),
            ProviderKind::Ollama => (
                "ollama-gemma3-4b-it-q4-k-m",
                ProviderOwnership::Attached,
                "http://127.0.0.1:11434",
                "gemma3:4b-it-q4_K_M",
                "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a",
                "google/gemma-3-4b-it",
                "ollama-managed-gguf-engine",
            ),
        };
        if self.schema_version != 1
            || self.artifact.schema_version != ARTIFACT_IDENTITY_SCHEMA_VERSION
            || self.engine.schema_version != 1
            || self.settings.schema_version != 1
        {
            return Err(ProviderError::IdentityMismatch("schema version".into()));
        }
        if self.candidate_id != expected_candidate_id {
            return Err(ProviderError::IdentityMismatch("candidate id".into()));
        }
        if self.ownership != expected_ownership {
            return Err(ProviderError::IdentityMismatch("provider ownership".into()));
        }
        if self.endpoint != expected_endpoint {
            return Err(ProviderError::IdentityMismatch("provider endpoint".into()));
        }
        if self.artifact.artifact_id != expected_artifact_id {
            return Err(ProviderError::IdentityMismatch("artifact id".into()));
        }
        if self.artifact.digest_sha256 != expected_digest {
            return Err(ProviderError::IdentityMismatch("artifact digest".into()));
        }
        if self.artifact.base_checkpoint != expected_base {
            return Err(ProviderError::IdentityMismatch("base checkpoint".into()));
        }
        match (&self.provider_kind, &self.artifact.checkpoint_attestation) {
            (ProviderKind::ManagedLlamaCpp, CheckpointAttestation::Registry { reference })
                if reference == "loxa-registry:gemma-3-4b-it-q4" => {}
            (ProviderKind::Ollama, CheckpointAttestation::Basename { value })
                if value == "gemma-3-4b-it" => {}
            (
                ProviderKind::Ollama,
                CheckpointAttestation::MetadataComposite {
                    architecture,
                    parameter_count,
                },
            ) if architecture == "gemma3" && *parameter_count == 4_299_915_632 => {}
            _ => {
                return Err(ProviderError::IdentityMismatch(
                    "checkpoint attestation".into(),
                ));
            }
        }
        if self.artifact.format != "gguf" {
            return Err(ProviderError::IdentityMismatch("artifact format".into()));
        }
        if self.artifact.quantization != "Q4_K_M" {
            return Err(ProviderError::IdentityMismatch(
                "artifact quantization".into(),
            ));
        }
        if self.artifact.tokenizer_evidence.is_empty() || self.artifact.template_evidence.is_empty()
        {
            return Err(ProviderError::IdentityMismatch("artifact evidence".into()));
        }
        if self.engine.engine_kind != expected_engine_kind || self.engine.evidence.is_empty() {
            return Err(ProviderError::IdentityMismatch("engine evidence".into()));
        }
        if self.engine.provider_version.trim().is_empty() {
            return Err(ProviderError::IdentityMismatch("provider version".into()));
        }
        let provider_key = format!("provider_version={}", self.engine.provider_version);
        if !self.engine.invalidation_keys.contains(&provider_key) {
            return Err(ProviderError::IdentityMismatch(
                "provider version invalidation key".into(),
            ));
        }
        if self.provider_kind == ProviderKind::Ollama {
            match &self.engine.engine_revision {
                EngineRevision::Known(revision) if !revision.trim().is_empty() => {
                    let evidence = format!("ollama_api_show:engine_revision={revision}");
                    if !self.engine.evidence.contains(&evidence) {
                        return Err(ProviderError::IdentityMismatch(
                            "observed engine revision evidence".into(),
                        ));
                    }
                    let revision_key = format!("engine_revision={revision}");
                    if !self.engine.invalidation_keys.contains(&revision_key) {
                        return Err(ProviderError::IdentityMismatch(
                            "engine revision invalidation key".into(),
                        ));
                    }
                }
                EngineRevision::Unknown { hidden: true } => {
                    let evidence = "ollama_api_show:engine_revision=unknown;hidden=true".into();
                    if !self.engine.evidence.contains(&evidence) {
                        return Err(ProviderError::IdentityMismatch(
                            "hidden engine revision disclosure".into(),
                        ));
                    }
                }
                EngineRevision::Known(_) | EngineRevision::Unknown { hidden: false } => {
                    return Err(ProviderError::IdentityMismatch(
                        "engine revision is not verified".into(),
                    ));
                }
            }
        }
        if self.settings != GenerationSettings::pinned_v1() {
            return Err(ProviderError::IdentityMismatch(
                "generation settings".into(),
            ));
        }
        Ok(())
    }
}

fn append_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn append_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn append_str(output: &mut Vec<u8>, value: &str) {
    append_u32(output, value.len() as u32);
    output.extend_from_slice(value.as_bytes());
}

fn append_strings(output: &mut Vec<u8>, values: &[String]) {
    append_u32(output, values.len() as u32);
    for value in values {
        append_str(output, value);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderError {
    IdentityMismatch(String),
    AmbiguousIdentity(String),
    InvalidEndpoint(String),
    Unreachable,
    Timeout,
    RedirectRejected,
    HttpStatus(u16),
    MalformedResponse {
        endpoint: &'static str,
        detail: String,
    },
    Transport(String),
    NotWired(String),
    Lifecycle(String),
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityMismatch(detail) => write!(formatter, "identity mismatch: {detail}"),
            Self::AmbiguousIdentity(detail) => write!(formatter, "ambiguous identity: {detail}"),
            Self::InvalidEndpoint(detail) => {
                write!(formatter, "invalid provider endpoint: {detail}")
            }
            Self::Unreachable => write!(formatter, "provider is unreachable"),
            Self::Timeout => write!(formatter, "provider request timed out"),
            Self::RedirectRejected => write!(formatter, "provider redirect rejected"),
            Self::HttpStatus(status) => write!(formatter, "provider returned HTTP {status}"),
            Self::MalformedResponse { endpoint, detail } => {
                write!(formatter, "malformed response from {endpoint}: {detail}")
            }
            Self::Transport(detail) => write!(formatter, "provider transport failed: {detail}"),
            Self::NotWired(detail) => write!(formatter, "provider operation not wired: {detail}"),
            Self::Lifecycle(detail) => write!(formatter, "provider lifecycle failed: {detail}"),
        }
    }
}

impl std::error::Error for ProviderError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMessage {
    pub role: ProviderRole,
    pub content: String,
}

impl ProviderMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::User,
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTiming {
    pub schema_version: u32,
    pub total_duration_ns: Option<u64>,
    pub load_duration_ns: Option<u64>,
    pub prompt_eval_count: Option<u64>,
    pub prompt_eval_duration_ns: Option<u64>,
    pub eval_count: Option<u64>,
    pub eval_duration_ns: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedTurn {
    pub schema_version: u32,
    pub content: String,
    pub timing: ProviderTiming,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlledRun {
    pub schema_version: u32,
    pub run_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub schema_version: u32,
    pub healthy: bool,
    pub provider_version: Option<String>,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderActivityObservation {
    pub schema_version: u32,
    pub target_active: bool,
    pub unrelated_activity: Vec<String>,
    pub evidence: Vec<String>,
}

pub trait ProviderAdapter {
    fn inspect_candidate(&self) -> Result<CandidateSpec, ProviderError>;

    fn verify_health(&mut self) -> Result<ProviderHealth, ProviderError> {
        Err(ProviderError::NotWired("health observation".into()))
    }

    fn observe_activity(&self) -> Result<ProviderActivityObservation, ProviderError> {
        Err(ProviderError::NotWired("activity observation".into()))
    }

    fn prepare_controlled_run(&mut self) -> Result<ControlledRun, ProviderError>;
    fn cold_readiness_duration_ns(&self, _run: &ControlledRun) -> Option<u64> {
        None
    }

    fn run_turn(
        &mut self,
        run: &ControlledRun,
        messages: &[ProviderMessage],
    ) -> Result<NormalizedTurn, ProviderError>;

    fn finish_controlled_run(&mut self, run: ControlledRun) -> Result<(), ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactIdentity, CandidateSpec, CheckpointAttestation, EngineIdentity, EngineRevision,
        GenerationSettings, ProviderKind, ProviderOwnership, ARTIFACT_IDENTITY_SCHEMA_VERSION,
    };
    use crate::provider::managed_llama::managed_candidate_spec;

    fn ollama_spec(provider_version: &str, revision: EngineRevision) -> CandidateSpec {
        let (revision_evidence, revision_key) = match &revision {
            EngineRevision::Known(value) => (
                format!("ollama_api_show:engine_revision={value}"),
                format!("engine_revision={value}"),
            ),
            EngineRevision::Unknown { hidden } => (
                format!("ollama_api_show:engine_revision=unknown;hidden={hidden}"),
                format!("engine_revision=unknown;hidden={hidden}"),
            ),
        };
        CandidateSpec {
            schema_version: 1,
            candidate_id: "ollama-gemma3-4b-it-q4-k-m".into(),
            provider_kind: ProviderKind::Ollama,
            ownership: ProviderOwnership::Attached,
            endpoint: "http://127.0.0.1:11434".into(),
            artifact: ArtifactIdentity {
                schema_version: ARTIFACT_IDENTITY_SCHEMA_VERSION,
                artifact_id: "gemma3:4b-it-q4_K_M".into(),
                digest_sha256: "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a"
                    .into(),
                base_checkpoint: "google/gemma-3-4b-it".into(),
                checkpoint_attestation: CheckpointAttestation::Basename {
                    value: "gemma-3-4b-it".into(),
                },
                format: "gguf".into(),
                quantization: "Q4_K_M".into(),
                tokenizer_evidence: vec!["/api/show:model_info.tokenizer.ggml.model=gemma".into()],
                template_evidence: vec!["/api/show:template={{ .Messages }}".into()],
            },
            engine: EngineIdentity {
                schema_version: 1,
                engine_kind: "ollama-managed-gguf-engine".into(),
                provider_version: provider_version.into(),
                engine_revision: revision,
                evidence: vec![
                    "/api/show:model_info.general.architecture=gemma3".into(),
                    revision_evidence,
                ],
                invalidation_keys: vec![
                    format!("provider_version={provider_version}"),
                    revision_key,
                ],
            },
            settings: GenerationSettings::pinned_v1(),
        }
    }

    #[test]
    fn fingerprint_is_deterministic() {
        let spec = managed_candidate_spec("llama-server 1.2.3", "rev-abc").unwrap();
        assert_eq!(spec.fingerprint(), spec.fingerprint());
    }

    #[test]
    fn managed_identity_uses_exact_registry_digest() {
        let spec = managed_candidate_spec("llama-server 1.2.3", "rev-abc").unwrap();
        assert_eq!(
            spec.artifact.digest_sha256,
            "04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19"
        );
    }

    #[test]
    fn ollama_identity_uses_exact_pinned_digest() {
        let spec = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        assert_eq!(
            spec.artifact.digest_sha256,
            "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a"
        );
    }

    #[test]
    fn artifact_validation_rejects_digest_mismatch() {
        let mut spec = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        spec.artifact.digest_sha256 = "00".repeat(32);
        assert!(spec
            .validate_pinned()
            .unwrap_err()
            .to_string()
            .contains("digest"));
    }

    #[test]
    fn artifact_validation_rejects_base_checkpoint_mismatch() {
        let mut spec = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        spec.artifact.base_checkpoint = "tag-derived/guess".into();
        assert!(spec
            .validate_pinned()
            .unwrap_err()
            .to_string()
            .contains("base checkpoint"));
    }

    #[test]
    fn provider_version_invalidates_fingerprint() {
        let a = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        let b = ollama_spec("0.12.0", EngineRevision::Known("rev-abc".into()));
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn metadata_composite_identity_fingerprints_exact_observed_shape_and_artifact() {
        let mut spec = ollama_spec("0.11.0", EngineRevision::Unknown { hidden: true });
        spec.artifact.checkpoint_attestation = CheckpointAttestation::MetadataComposite {
            architecture: "gemma3".into(),
            parameter_count: 4_299_915_632,
        };
        let fingerprint = spec.fingerprint();
        let serialized = serde_json::to_string(&spec).unwrap();
        assert!(serialized.contains("\"checkpoint_attested_by\":\"metadata_composite\""));
        for expected in [
            "metadata_composite",
            "gemma3",
            "4299915632",
            "Q4_K_M",
            "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a",
        ] {
            assert!(
                serialized.contains(expected),
                "missing identity: {expected}"
            );
        }

        let mut changed = spec.clone();
        changed.artifact.digest_sha256 = "00".repeat(32);
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = spec.clone();
        changed.artifact.quantization = "Q8_0".into();
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = spec.clone();
        changed.artifact.checkpoint_attestation = CheckpointAttestation::MetadataComposite {
            architecture: "other".into(),
            parameter_count: 4_299_915_632,
        };
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = spec;
        changed.artifact.checkpoint_attestation = CheckpointAttestation::MetadataComposite {
            architecture: "gemma3".into(),
            parameter_count: 4_299_915_631,
        };
        assert_ne!(fingerprint, changed.fingerprint());
    }

    #[test]
    fn pinned_settings_include_common_output_budget() {
        assert_eq!(GenerationSettings::pinned_v1().max_output_tokens, 256);
    }

    #[test]
    fn previous_artifact_identity_schema_is_rejected() {
        let mut spec = ollama_spec("0.11.0", EngineRevision::Unknown { hidden: true });
        spec.artifact.schema_version = 1;

        assert!(spec
            .validate_pinned()
            .unwrap_err()
            .to_string()
            .contains("schema version"));
    }

    #[test]
    fn every_added_identity_field_invalidates_fingerprint() {
        let base = ollama_spec("0.11.0", EngineRevision::Unknown { hidden: true });
        let fingerprint = base.fingerprint();

        let mut changed = base.clone();
        changed.endpoint.push_str("/changed");
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base.clone();
        changed.artifact.checkpoint_attestation = CheckpointAttestation::MetadataComposite {
            architecture: "gemma3".into(),
            parameter_count: 4_299_915_632,
        };
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base.clone();
        changed.artifact.tokenizer_evidence.push("changed".into());
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base.clone();
        changed.artifact.template_evidence.push("changed".into());
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base.clone();
        changed.engine.evidence.push("changed".into());
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base.clone();
        changed.engine.invalidation_keys.push("changed".into());
        assert_ne!(fingerprint, changed.fingerprint());

        let mut changed = base;
        changed.settings.max_output_tokens += 1;
        assert_ne!(fingerprint, changed.fingerprint());
    }

    #[test]
    fn pinned_validation_rejects_execution_plan_drift() {
        let valid = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        valid.validate_pinned().unwrap();
        let mut invalid = Vec::new();

        let mut changed = valid.clone();
        changed.candidate_id = "other".into();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.ownership = ProviderOwnership::Controlled;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.endpoint = "http://127.0.0.1:9999".into();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.artifact.format = "safetensors".into();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.artifact.quantization = "Q8_0".into();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.settings.max_output_tokens = 255;
        invalid.push(changed);
        let mut changed = valid;
        changed.engine.provider_version.clear();
        invalid.push(changed);

        for changed in invalid {
            assert!(changed.validate_pinned().is_err(), "accepted plan drift");
        }
    }

    #[test]
    fn ollama_hidden_engine_revision_is_explicit_admitted_and_provider_version_bound() {
        let known = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        let unknown = ollama_spec("0.11.0", EngineRevision::Unknown { hidden: true });
        assert_ne!(known.fingerprint(), unknown.fingerprint());
        let serialized = serde_json::to_string(&unknown).unwrap();
        assert!(serialized.contains("unknown"));
        assert!(serialized.contains("hidden"));
        unknown.validate_pinned().unwrap();
        assert!(unknown
            .engine
            .invalidation_keys
            .contains(&"provider_version=0.11.0".into()));
        let changed_version = ollama_spec("0.12.0", EngineRevision::Unknown { hidden: true });
        assert_ne!(unknown.fingerprint(), changed_version.fingerprint());
    }

    #[test]
    fn ollama_unverified_nonhidden_engine_revision_is_rejected() {
        let unknown = ollama_spec("0.11.0", EngineRevision::Unknown { hidden: false });
        assert!(unknown
            .validate_pinned()
            .unwrap_err()
            .to_string()
            .contains("engine revision"));
    }

    #[test]
    fn engine_kind_is_explicit_and_not_inferred_from_model_tag() {
        let mut spec = ollama_spec("0.11.0", EngineRevision::Known("rev-abc".into()));
        spec.artifact.artifact_id = "not-an-engine-name:latest".into();
        assert_eq!(spec.engine.engine_kind, "ollama-managed-gguf-engine");
    }

    #[test]
    fn serialized_identity_cannot_contain_sensitive_runtime_material() {
        let serialized = serde_json::to_string(&ollama_spec(
            "0.11.0",
            EngineRevision::Known("rev-abc".into()),
        ))
        .unwrap()
        .to_ascii_lowercase();
        for forbidden in [
            "prompt",
            "response",
            "credential",
            "authorization",
            "user_file",
            "secret",
        ] {
            assert!(!serialized.contains(forbidden), "leaked key: {forbidden}");
        }
    }
}
