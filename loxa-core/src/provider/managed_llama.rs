use super::{
    ArtifactIdentity, CandidateSpec, CheckpointAttestation, ControlledRun, EngineIdentity,
    EngineRevision, GenerationSettings, NormalizedTurn, ProviderActivityObservation,
    ProviderAdapter, ProviderError, ProviderHealth, ProviderKind, ProviderMessage,
    ProviderOwnership, ProviderTiming, ARTIFACT_IDENTITY_SCHEMA_VERSION,
    CANDIDATE_IDENTITY_SCHEMA_VERSION,
};
use crate::{download, registry, supervisor};
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MANAGED_REGISTRY_ID: &str = "gemma-3-4b-it-q4";

pub struct ManagedLlamaAdapter {
    provider_version: String,
    engine_revision: String,
    session: Option<supervisor::ManagedCalibrationSession>,
    transport: Box<dyn ManagedTransport>,
    state_path: PathBuf,
    prepare_duration_ns: Option<u64>,
    binary_path: Option<PathBuf>,
    binary_digest: Option<String>,
}

pub trait ManagedTransport {
    fn chat(
        &self,
        endpoint: &str,
        alias: &str,
        messages: &[ProviderMessage],
    ) -> Result<ManagedHttpResponse, ProviderError>;
}

pub struct ManagedHttpResponse {
    pub status: u16,
    pub body: String,
}

struct HttpManagedTransport {
    client: Client,
}

impl ManagedTransport for HttpManagedTransport {
    fn chat(
        &self,
        endpoint: &str,
        alias: &str,
        messages: &[ProviderMessage],
    ) -> Result<ManagedHttpResponse, ProviderError> {
        let url = format!("{endpoint}/v1/chat/completions");
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(
                json!({
                    "model": alias, "messages": messages, "stream": false, "temperature": 0,
                    "seed": 42, "max_tokens": 256
                })
                .to_string(),
            )
            .send()
            .map_err(map_reqwest)?;
        let status = response.status().as_u16();
        let body = response.text().map_err(map_reqwest)?;
        Ok(ManagedHttpResponse { status, body })
    }
}

impl ManagedLlamaAdapter {
    pub fn new(
        provider_version: impl Into<String>,
        engine_revision: impl Into<String>,
    ) -> Result<Self, ProviderError> {
        let provider_version = normalize_provider_version(&provider_version.into());
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(30))
            .redirect(Policy::none())
            .build()
            .map_err(|error| ProviderError::Transport(error.to_string()))?;
        Ok(Self {
            provider_version,
            engine_revision: engine_revision.into(),
            session: None,
            transport: Box::new(HttpManagedTransport { client }),
            state_path: supervisor::runtime_state_path(),
            prepare_duration_ns: None,
            binary_path: None,
            binary_digest: None,
        })
    }

    pub fn discover_verified() -> Result<Self, ProviderError> {
        let binary = supervisor::detect_llama_server().map_err(map_supervisor)?;
        let version = supervisor::llama_server_version(&binary).map_err(map_supervisor)?;
        let binary_digest = sha256_file(&binary)?;
        let entry = registry::find(MANAGED_REGISTRY_ID).ok_or_else(|| {
            ProviderError::IdentityMismatch("managed registry entry is missing".into())
        })?;
        let model = download::model_dir().join(entry.filename);
        let digest = sha256_file(&model)?;
        if digest != entry.sha256 {
            return Err(ProviderError::IdentityMismatch(
                "managed artifact digest".into(),
            ));
        }
        let mut adapter = Self::new(version, binary_digest.clone())?;
        adapter.binary_path = Some(binary);
        adapter.binary_digest = Some(binary_digest);
        Ok(adapter)
    }
}

fn sha256_file(path: &std::path::Path) -> Result<String, ProviderError> {
    let mut file = File::open(path).map_err(|e| ProviderError::IdentityMismatch(e.to_string()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|e| ProviderError::IdentityMismatch(e.to_string()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>())
}

fn normalize_provider_version(version: &str) -> String {
    version.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn managed_candidate_spec(
    provider_version: &str,
    engine_revision: &str,
) -> Result<CandidateSpec, ProviderError> {
    let provider_version = normalize_provider_version(provider_version);
    let entry = registry::find(MANAGED_REGISTRY_ID).ok_or_else(|| {
        ProviderError::IdentityMismatch("managed registry entry is missing".into())
    })?;
    let spec = CandidateSpec {
        schema_version: CANDIDATE_IDENTITY_SCHEMA_VERSION,
        candidate_id: MANAGED_REGISTRY_ID.into(),
        provider_kind: ProviderKind::ManagedLlamaCpp,
        ownership: ProviderOwnership::Controlled,
        endpoint: "managed://loxa-supervisor/llama-server".into(),
        artifact: ArtifactIdentity {
            schema_version: ARTIFACT_IDENTITY_SCHEMA_VERSION,
            artifact_id: entry.id.into(),
            digest_sha256: entry.sha256.into(),
            base_checkpoint: "google/gemma-3-4b-it".into(),
            checkpoint_attestation: CheckpointAttestation::Registry {
                reference: "loxa-registry:gemma-3-4b-it-q4".into(),
            },
            format: "gguf".into(),
            quantization: entry.quant.into(),
            tokenizer_evidence: vec![
                format!("registry_reference={}", entry.repo),
                "declared_base_checkpoint=google/gemma-3-4b-it".into(),
            ],
            template_evidence: vec![
                format!("registry_file={}", entry.filename),
                "template_metadata_unavailable_in_registry".into(),
            ],
        },
        engine: EngineIdentity {
            schema_version: 1,
            engine_kind: "llama.cpp".into(),
            provider_version: provider_version.clone(),
            engine_revision: if engine_revision.trim().is_empty() {
                EngineRevision::Unknown { hidden: false }
            } else {
                EngineRevision::Known(engine_revision.into())
            },
            evidence: vec![
                format!("managed_provider_version={provider_version}"),
                format!("managed_engine_revision={engine_revision}"),
            ],
            invalidation_keys: vec![
                format!("provider_version={provider_version}"),
                format!("engine_revision={engine_revision}"),
            ],
        },
        settings: GenerationSettings::pinned_v1(),
    };
    spec.validate_pinned()?;
    Ok(spec)
}

impl ProviderAdapter for ManagedLlamaAdapter {
    fn inspect_candidate(&self) -> Result<CandidateSpec, ProviderError> {
        if let (Some(path), Some(expected_digest)) = (&self.binary_path, &self.binary_digest) {
            if sha256_file(path)? != *expected_digest {
                return Err(ProviderError::IdentityMismatch(
                    "managed binary digest changed".into(),
                ));
            }
            let version = supervisor::llama_server_version(path).map_err(map_supervisor)?;
            let version = normalize_provider_version(&version);
            if version != self.provider_version {
                return Err(ProviderError::IdentityMismatch(
                    "managed provider version changed".into(),
                ));
            }
        }
        managed_candidate_spec(&self.provider_version, &self.engine_revision)
    }

    fn verify_health(&mut self) -> Result<ProviderHealth, ProviderError> {
        let Some(session) = self.session.as_mut() else {
            return Ok(ProviderHealth {
                schema_version: 1,
                healthy: false,
                provider_version: Some(self.provider_version.clone()),
                evidence: vec!["managed session not prepared".into()],
            });
        };
        session.ensure_running().map_err(map_supervisor)?;
        Ok(ProviderHealth {
            schema_version: 1,
            healthy: true,
            provider_version: Some(normalize_provider_version(
                &session.server().llama_server_version,
            )),
            evidence: vec![format!("generation_alias={}", session.generation_alias())],
        })
    }

    fn observe_activity(&self) -> Result<ProviderActivityObservation, ProviderError> {
        let state = supervisor::read_runtime_state(&self.state_path).map_err(map_supervisor)?;
        let (target_active, unrelated_activity, evidence) = match state {
            supervisor::RuntimeStateRead::Missing => {
                (false, vec![], vec!["managed_state_missing".into()])
            }
            supervisor::RuntimeStateRead::Loaded(runs) if runs.is_empty() => {
                (false, vec![], vec!["managed_state_empty".into()])
            }
            supervisor::RuntimeStateRead::Loaded(runs) => (
                true,
                vec![format!("active_managed_runs={}", runs.len())],
                vec!["managed_state_active".into()],
            ),
            supervisor::RuntimeStateRead::Legacy(_) => (
                false,
                vec!["legacy_managed_state".into()],
                vec!["managed_state_legacy".into()],
            ),
            supervisor::RuntimeStateRead::Corrupt(_) => (
                false,
                vec!["corrupt_managed_state".into()],
                vec!["managed_state_corrupt".into()],
            ),
        };
        Ok(ProviderActivityObservation {
            schema_version: 1,
            target_active,
            unrelated_activity,
            evidence,
        })
    }

    fn prepare_controlled_run(&mut self) -> Result<ControlledRun, ProviderError> {
        if self.session.is_some() {
            return Err(ProviderError::Lifecycle(
                "managed session already prepared".into(),
            ));
        }
        let started = Instant::now();
        let session = supervisor::ManagedCalibrationSession::start(
            &supervisor::runtime_state_path(),
            &download::model_dir(),
            MANAGED_REGISTRY_ID,
            4096,
        )
        .map_err(map_supervisor)?;
        self.prepare_duration_ns = started.elapsed().as_nanos().try_into().ok();
        let run_id = session.run().run_id.clone();
        self.session = Some(session);
        Ok(ControlledRun {
            schema_version: 1,
            run_id,
        })
    }

    fn cold_readiness_duration_ns(&self, _run: &ControlledRun) -> Option<u64> {
        self.prepare_duration_ns
    }

    fn run_turn(
        &mut self,
        run: &ControlledRun,
        messages: &[ProviderMessage],
    ) -> Result<NormalizedTurn, ProviderError> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| ProviderError::Lifecycle("managed session not prepared".into()))?;
        if session.run().run_id != run.run_id {
            return Err(ProviderError::Lifecycle(
                "controlled run identity mismatch".into(),
            ));
        }
        session.ensure_running().map_err(map_supervisor)?;
        let started = Instant::now();
        let response =
            self.transport
                .chat(&session.endpoint(), session.generation_alias(), messages)?;
        if (300..400).contains(&response.status) {
            return Err(ProviderError::RedirectRejected);
        }
        if !(200..300).contains(&response.status) {
            return Err(ProviderError::HttpStatus(response.status));
        }
        let parsed: ManagedChatResponse =
            serde_json::from_str(&response.body).map_err(|e| ProviderError::MalformedResponse {
                endpoint: "/v1/chat/completions",
                detail: e.to_string(),
            })?;
        let content = parsed
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| ProviderError::MalformedResponse {
                endpoint: "/v1/chat/completions",
                detail: "missing first choice".into(),
            })?
            .message
            .content;
        Ok(NormalizedTurn {
            schema_version: 1,
            content,
            timing: ProviderTiming {
                schema_version: 1,
                total_duration_ns: Some(started.elapsed().as_nanos().min(u64::MAX as u128) as u64),
                prompt_eval_count: parsed.usage.as_ref().map(|u| u.prompt_tokens),
                eval_count: parsed.usage.as_ref().map(|u| u.completion_tokens),
                ..ProviderTiming::default()
            },
        })
    }

    fn finish_controlled_run(&mut self, run: ControlledRun) -> Result<(), ProviderError> {
        let session = self
            .session
            .take()
            .ok_or_else(|| ProviderError::Lifecycle("managed session not prepared".into()))?;
        if session.run().run_id != run.run_id {
            self.session = Some(session);
            return Err(ProviderError::Lifecycle(
                "controlled run identity mismatch".into(),
            ));
        }
        match session.finish().map_err(map_supervisor)? {
            supervisor::OwnerTerminalOutcome::RequestedStop => Ok(()),
            supervisor::OwnerTerminalOutcome::Interrupted => Err(ProviderError::Lifecycle(
                "managed teardown was interrupted".into(),
            )),
            supervisor::OwnerTerminalOutcome::RecoveryRequired => Err(ProviderError::Lifecycle(
                "managed teardown requires recovery".into(),
            )),
        }
    }
}

fn map_supervisor(error: supervisor::SupervisorError) -> ProviderError {
    ProviderError::Lifecycle(error.to_string())
}
fn map_reqwest(error: reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        ProviderError::Timeout
    } else if error.is_connect() {
        ProviderError::Unreachable
    } else {
        ProviderError::Transport(error.to_string())
    }
}

#[derive(Deserialize)]
struct ManagedChatResponse {
    choices: Vec<ManagedChoice>,
    usage: Option<ManagedUsage>,
}
#[derive(Deserialize)]
struct ManagedChoice {
    message: ManagedMessage,
}
#[derive(Deserialize)]
struct ManagedMessage {
    content: String,
}
#[derive(Deserialize)]
struct ManagedUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::{managed_candidate_spec, ManagedLlamaAdapter};
    use crate::provider::{EngineRevision, ProviderAdapter, ProviderKind, ProviderOwnership};

    #[test]
    fn managed_adapter_is_usable_through_object_safe_boundary() {
        let adapter: Box<dyn ProviderAdapter> =
            Box::new(ManagedLlamaAdapter::new("llama-server 1.2.3", "rev-abc").unwrap());
        let candidate = adapter.inspect_candidate().unwrap();
        assert_eq!(candidate.provider_kind, ProviderKind::ManagedLlamaCpp);
        assert_eq!(candidate.ownership, ProviderOwnership::Controlled);
    }

    #[test]
    fn managed_adapter_exposes_typed_health_and_activity() {
        let mut adapter = ManagedLlamaAdapter::new("llama-server 1.2.3", "rev-abc").unwrap();
        assert!(!adapter.verify_health().unwrap().healthy);
        assert!(!adapter.observe_activity().unwrap().target_active);
    }

    #[test]
    fn managed_version_normalization_stabilizes_fingerprint_and_preserves_drift() {
        let engine_revision = "f".repeat(64);
        let live_version = concat!(
            "version: 9910 (f5525f7e7)\n",
            "built with AppleClang 17.0.0.17000013 for arm64-apple-darwin24.6.0"
        );
        let normalized_version = concat!(
            "version: 9910 (f5525f7e7) ",
            "built with AppleClang 17.0.0.17000013 for arm64-apple-darwin24.6.0"
        );
        let drifted_version = live_version.replacen("9910", "9911", 1);

        let adapter = ManagedLlamaAdapter::new(live_version, engine_revision.clone()).unwrap();
        assert_eq!(adapter.provider_version, normalized_version);

        let live = managed_candidate_spec(live_version, &engine_revision).unwrap();
        let normalized = managed_candidate_spec(normalized_version, &engine_revision).unwrap();
        let drifted = managed_candidate_spec(&drifted_version, &engine_revision).unwrap();

        assert_eq!(live.engine.provider_version, normalized_version);
        assert_eq!(
            live.engine.engine_revision,
            EngineRevision::Known(engine_revision)
        );
        assert_eq!(live.fingerprint(), normalized.fingerprint());
        assert_ne!(live.fingerprint(), drifted.fingerprint());
    }
}
