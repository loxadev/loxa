use super::{
    ArtifactIdentity, CandidateSpec, CheckpointAttestation, ControlledRun, EngineIdentity,
    EngineRevision, GenerationSettings, NormalizedTurn, ProviderActivityObservation,
    ProviderAdapter, ProviderError, ProviderHealth, ProviderMessage, ProviderOwnership,
    ProviderTiming, ARTIFACT_IDENTITY_SCHEMA_VERSION, CANDIDATE_IDENTITY_SCHEMA_VERSION,
};
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub const OLLAMA_MODEL_TAG: &str = "gemma3:4b-it-q4_K_M";
pub const OLLAMA_MODEL_DIGEST: &str =
    "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a";
const OLLAMA_ENDPOINT: &str = "http://127.0.0.1:11434";
const EXPECTED_BASE_CHECKPOINT: &str = "gemma-3-4b-it";
const EXPECTED_ARCHITECTURE: &str = "gemma3";
const EXPECTED_PARAMETER_COUNT: u64 = 4_299_915_632;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OllamaRequest {
    Version,
    Tags,
    Show,
    Ps,
    Chat { messages: Vec<ProviderMessage> },
}

impl OllamaRequest {
    pub fn method_path(&self) -> (&'static str, &'static str) {
        match self {
            Self::Version => ("GET", "/api/version"),
            Self::Tags => ("GET", "/api/tags"),
            Self::Show => ("POST", "/api/show"),
            Self::Ps => ("GET", "/api/ps"),
            Self::Chat { .. } => ("POST", "/api/chat"),
        }
    }

    pub fn json_body(&self) -> Value {
        match self {
            Self::Show => json!({ "model": OLLAMA_MODEL_TAG, "verbose": true }),
            Self::Chat { messages } => json!({
                "model": OLLAMA_MODEL_TAG,
                "messages": messages,
                "stream": false,
                "options": {
                    "num_ctx": 4096,
                    "temperature": 0,
                    "seed": 42,
                    "num_predict": 256
                }
            }),
            Self::Version | Self::Tags | Self::Ps => Value::Null,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OllamaResponse {
    pub status: u16,
    pub body: String,
}

impl OllamaResponse {
    pub fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OllamaTransportError {
    Unreachable,
    Timeout,
    Other(String),
}

pub trait OllamaTransport {
    fn execute(&self, request: &OllamaRequest) -> Result<OllamaResponse, OllamaTransportError>;
}

pub struct HttpOllamaTransport {
    endpoint: Url,
    client: Client,
}

impl HttpOllamaTransport {
    pub fn new(endpoint: &str) -> Result<Self, ProviderError> {
        let endpoint = validated_endpoint(endpoint)?;
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(30))
            .redirect(Policy::none())
            .build()
            .map_err(|error| ProviderError::Transport(error.to_string()))?;
        Ok(Self { endpoint, client })
    }
}

impl OllamaTransport for HttpOllamaTransport {
    fn execute(&self, request: &OllamaRequest) -> Result<OllamaResponse, OllamaTransportError> {
        let (_, path) = request.method_path();
        let url = self
            .endpoint
            .join(path)
            .map_err(|error| OllamaTransportError::Other(error.to_string()))?;
        let builder = match request {
            OllamaRequest::Version | OllamaRequest::Tags | OllamaRequest::Ps => {
                self.client.get(url)
            }
            OllamaRequest::Show | OllamaRequest::Chat { .. } => self
                .client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(request.json_body().to_string()),
        };
        let response = builder.send().map_err(|error| {
            if error.is_timeout() {
                OllamaTransportError::Timeout
            } else if error.is_connect() {
                OllamaTransportError::Unreachable
            } else {
                OllamaTransportError::Other(error.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .map_err(|error| OllamaTransportError::Other(error.to_string()))?;
        Ok(OllamaResponse { status, body })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OllamaActivityObservation {
    pub schema_version: u32,
    pub target_loaded: bool,
    pub unrelated_models: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OllamaInspection {
    pub schema_version: u32,
    pub candidate: CandidateSpec,
    pub activity: OllamaActivityObservation,
}

pub struct OllamaAdapter<T: OllamaTransport> {
    transport: T,
}

fn engine_evidence(provider_version: &str, revision: &EngineRevision) -> Vec<String> {
    let revision_evidence = match revision {
        EngineRevision::Known(revision) => {
            format!("ollama_api_show:engine_revision={revision}")
        }
        EngineRevision::Unknown { hidden } => {
            format!("ollama_api_show:engine_revision=unknown;hidden={hidden}")
        }
    };
    vec![
        format!("ollama_api_version:version={provider_version}"),
        "ollama_api_tags:details.family=gemma3".into(),
        "ollama_api_show:model_info.general.architecture=gemma3".into(),
        revision_evidence,
    ]
}

fn engine_invalidation_keys(provider_version: &str, revision: &EngineRevision) -> Vec<String> {
    let mut keys = vec![format!("provider_version={provider_version}")];
    if let EngineRevision::Known(revision) = revision {
        keys.push(format!("engine_revision={revision}"));
    }
    keys
}

impl<T: OllamaTransport> OllamaAdapter<T> {
    pub fn new(endpoint: &str, transport: T) -> Result<Self, ProviderError> {
        validated_endpoint(endpoint)?;
        Ok(Self { transport })
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn inspect(&self) -> Result<OllamaInspection, ProviderError> {
        let version: VersionResponse = self.request_json(OllamaRequest::Version)?;
        let tags: TagsResponse = self.request_json(OllamaRequest::Tags)?;
        let mut model = unique_exact_tag(tags)?;
        model.digest = canonicalize_sha256(&model.digest, "artifact digest")?;
        require_identity(&model.digest, OLLAMA_MODEL_DIGEST, "artifact digest")?;
        require_identity(&model.details.format, "gguf", "artifact format")?;
        require_identity(
            &model.details.quantization_level,
            "Q4_K_M",
            "artifact quantization",
        )?;
        require_identity(&model.details.family, "gemma3", "model family")?;

        let show: ShowResponse = self.request_json(OllamaRequest::Show)?;
        let engine_revision = match show.engine_revision.as_deref() {
            Some(revision) => EngineRevision::Known(
                require_present_evidence(Some(revision), "engine revision")?.into(),
            ),
            None => EngineRevision::Unknown { hidden: true },
        };
        require_identity(&show.details.format, "gguf", "show format")?;
        require_identity(
            &show.details.quantization_level,
            "Q4_K_M",
            "show quantization",
        )?;
        require_identity(&show.details.family, "gemma3", "show family")?;
        let template_evidence = template_evidence(&show.template)?;
        let checkpoint_attestation = checkpoint_attestation(&show.model_info)?;
        let tokenizer_model =
            require_present_evidence(show.model_info.tokenizer_model.as_deref(), "tokenizer")?;

        let tags_after_show: TagsResponse = self.request_json(OllamaRequest::Tags)?;
        let mut model_after_show = unique_exact_tag(tags_after_show)?;
        model_after_show.digest = canonicalize_sha256(&model_after_show.digest, "artifact digest")?;
        if model_after_show != model {
            return Err(ProviderError::IdentityMismatch(
                "tag identity changed during inspection".into(),
            ));
        }

        let ps: PsResponse = self.request_json(OllamaRequest::Ps)?;
        let target_loaded = ps.models.iter().any(is_exact_loaded_target);
        let unrelated_models = ps
            .models
            .into_iter()
            .filter(|loaded| !is_exact_loaded_target(loaded))
            .map(|loaded| loaded.name)
            .collect();

        let candidate = CandidateSpec {
            schema_version: CANDIDATE_IDENTITY_SCHEMA_VERSION,
            candidate_id: "ollama-gemma3-4b-it-q4-k-m".into(),
            provider_kind: super::ProviderKind::Ollama,
            ownership: ProviderOwnership::Attached,
            endpoint: OLLAMA_ENDPOINT.into(),
            artifact: ArtifactIdentity {
                schema_version: ARTIFACT_IDENTITY_SCHEMA_VERSION,
                artifact_id: OLLAMA_MODEL_TAG.into(),
                digest_sha256: model.digest,
                base_checkpoint: "google/gemma-3-4b-it".into(),
                checkpoint_attestation,
                format: model.details.format,
                quantization: model.details.quantization_level,
                tokenizer_evidence: vec![format!(
                    "ollama_api_show:model_info.tokenizer.ggml.model={tokenizer_model}"
                )],
                template_evidence: vec![template_evidence],
            },
            engine: EngineIdentity {
                schema_version: 1,
                engine_kind: "ollama-managed-gguf-engine".into(),
                provider_version: version.version.clone(),
                evidence: engine_evidence(&version.version, &engine_revision),
                invalidation_keys: engine_invalidation_keys(&version.version, &engine_revision),
                engine_revision,
            },
            settings: GenerationSettings::pinned_v1(),
        };
        candidate.validate_pinned()?;
        Ok(OllamaInspection {
            schema_version: 1,
            candidate,
            activity: OllamaActivityObservation {
                schema_version: 1,
                target_loaded,
                unrelated_models,
            },
        })
    }

    pub fn chat(&self, messages: &[ProviderMessage]) -> Result<NormalizedTurn, ProviderError> {
        let response: ChatResponse = self.request_json(OllamaRequest::Chat {
            messages: messages.to_vec(),
        })?;
        require_identity(&response.model, OLLAMA_MODEL_TAG, "chat model")?;
        if !response.done {
            return Err(ProviderError::MalformedResponse {
                endpoint: "/api/chat",
                detail: "non-streaming response was not complete".into(),
            });
        }
        Ok(NormalizedTurn {
            schema_version: 1,
            content: response.message.content,
            timing: ProviderTiming {
                schema_version: 1,
                total_duration_ns: response.total_duration,
                load_duration_ns: response.load_duration,
                prompt_eval_count: response.prompt_eval_count,
                prompt_eval_duration_ns: response.prompt_eval_duration,
                eval_count: response.eval_count,
                eval_duration_ns: response.eval_duration,
            },
        })
    }

    fn request_json<R: for<'de> Deserialize<'de>>(
        &self,
        request: OllamaRequest,
    ) -> Result<R, ProviderError> {
        let endpoint = request.method_path().1;
        let response = self
            .transport
            .execute(&request)
            .map_err(map_transport_error)?;
        if (300..400).contains(&response.status) {
            return Err(ProviderError::RedirectRejected);
        }
        if !(200..300).contains(&response.status) {
            return Err(ProviderError::HttpStatus(response.status));
        }
        serde_json::from_str(&response.body).map_err(|error| ProviderError::MalformedResponse {
            endpoint,
            detail: error.to_string(),
        })
    }
}

impl<T: OllamaTransport> ProviderAdapter for OllamaAdapter<T> {
    fn inspect_candidate(&self) -> Result<CandidateSpec, ProviderError> {
        Ok(self.inspect()?.candidate)
    }

    fn verify_health(&mut self) -> Result<ProviderHealth, ProviderError> {
        let candidate = self.inspect_candidate()?;
        Ok(ProviderHealth {
            schema_version: 1,
            healthy: true,
            provider_version: Some(candidate.engine.provider_version.clone()),
            evidence: vec![format!(
                "exact_candidate_fingerprint={}",
                candidate.fingerprint()
            )],
        })
    }

    fn observe_activity(&self) -> Result<ProviderActivityObservation, ProviderError> {
        let ps: PsResponse = self.request_json(OllamaRequest::Ps)?;
        let target_active = ps.models.iter().any(is_exact_loaded_target);
        let unrelated_activity = ps
            .models
            .into_iter()
            .filter(|m| !is_exact_loaded_target(m))
            .map(|m| format!("{}@{}", m.name, m.digest))
            .collect();
        Ok(ProviderActivityObservation {
            schema_version: 1,
            target_active,
            unrelated_activity,
            evidence: vec!["GET /api/ps exact snapshot".into()],
        })
    }

    fn prepare_controlled_run(&mut self) -> Result<ControlledRun, ProviderError> {
        Ok(ControlledRun {
            schema_version: 1,
            run_id: "ollama-attached-v1".into(),
        })
    }

    fn run_turn(
        &mut self,
        _run: &ControlledRun,
        messages: &[ProviderMessage],
    ) -> Result<NormalizedTurn, ProviderError> {
        self.chat(messages)
    }

    fn finish_controlled_run(&mut self, _run: ControlledRun) -> Result<(), ProviderError> {
        Ok(())
    }
}

fn validated_endpoint(endpoint: &str) -> Result<Url, ProviderError> {
    let url =
        Url::parse(endpoint).map_err(|error| ProviderError::InvalidEndpoint(error.to_string()))?;
    if endpoint != OLLAMA_ENDPOINT
        || url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port() != Some(11434)
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ProviderError::InvalidEndpoint(
            "expected exactly http://127.0.0.1:11434".into(),
        ));
    }
    Ok(url)
}

fn map_transport_error(error: OllamaTransportError) -> ProviderError {
    match error {
        OllamaTransportError::Unreachable => ProviderError::Unreachable,
        OllamaTransportError::Timeout => ProviderError::Timeout,
        OllamaTransportError::Other(detail) => ProviderError::Transport(detail),
    }
}

fn require_identity(actual: &str, expected: &str, field: &str) -> Result<(), ProviderError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ProviderError::IdentityMismatch(format!(
            "{field}: expected {expected}, got {actual}"
        )))
    }
}

fn canonicalize_sha256(digest: &str, field: &str) -> Result<String, ProviderError> {
    let bare = digest.strip_prefix("sha256:").unwrap_or(digest);
    if bare.len() != 64
        || !bare
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProviderError::IdentityMismatch(format!(
            "{field}: expected a canonical sha256 digest"
        )));
    }
    Ok(bare.into())
}

fn require_present_evidence<'a>(
    actual: Option<&'a str>,
    field: &str,
) -> Result<&'a str, ProviderError> {
    actual
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ProviderError::IdentityMismatch(format!(
                "{field}: required official /api/show evidence is missing"
            ))
        })
}

fn template_evidence(template: &str) -> Result<String, ProviderError> {
    if template.is_empty() {
        return Err(ProviderError::IdentityMismatch(
            "chat template evidence is empty".into(),
        ));
    }
    let digest = Sha256::digest(template.as_bytes());
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "ollama_api_show:template_sha256={digest};bytes={}",
        template.len()
    ))
}

fn unique_exact_tag(tags: TagsResponse) -> Result<TagModel, ProviderError> {
    let mut matches = tags
        .models
        .into_iter()
        .filter(|model| model.name == OLLAMA_MODEL_TAG && model.model == OLLAMA_MODEL_TAG);
    let model = matches.next().ok_or_else(|| {
        ProviderError::IdentityMismatch(format!("exact tag {OLLAMA_MODEL_TAG} is missing"))
    })?;
    if matches.next().is_some() {
        return Err(ProviderError::AmbiguousIdentity(format!(
            "duplicate exact tag {OLLAMA_MODEL_TAG}"
        )));
    }
    Ok(model)
}

fn is_exact_loaded_target(model: &LoadedModel) -> bool {
    model.digest == OLLAMA_MODEL_DIGEST
}

#[derive(Deserialize)]
struct VersionResponse {
    version: String,
}

#[derive(Deserialize)]
struct TagsResponse {
    models: Vec<TagModel>,
}

#[derive(Deserialize, PartialEq, Eq)]
struct TagModel {
    name: String,
    model: String,
    digest: String,
    details: ModelDetails,
}

#[derive(Deserialize, PartialEq, Eq)]
struct ModelDetails {
    parent_model: String,
    format: String,
    family: String,
    families: Vec<String>,
    parameter_size: String,
    quantization_level: String,
}

#[derive(Deserialize)]
struct ShowResponse {
    engine_revision: Option<String>,
    template: String,
    details: ModelDetails,
    model_info: ModelInfo,
}

#[derive(Default, Deserialize)]
struct ModelInfo {
    #[serde(rename = "general.basename")]
    general_basename: Option<String>,
    #[serde(rename = "general.name")]
    general_name: Option<String>,
    #[serde(rename = "general.architecture")]
    general_architecture: Option<String>,
    #[serde(rename = "general.parameter_count")]
    general_parameter_count: Option<u64>,
    #[serde(rename = "tokenizer.ggml.model")]
    tokenizer_model: Option<String>,
}

fn checkpoint_attestation(model_info: &ModelInfo) -> Result<CheckpointAttestation, ProviderError> {
    let architecture = require_present_evidence(
        model_info.general_architecture.as_deref(),
        "model architecture",
    )?;
    require_identity(architecture, EXPECTED_ARCHITECTURE, "model architecture")?;

    let basename = present_checkpoint_field(
        model_info.general_basename.as_deref(),
        "base checkpoint basename",
    )?;
    let name =
        present_checkpoint_field(model_info.general_name.as_deref(), "base checkpoint name")?;
    if let (Some(basename), Some(name)) = (basename, name) {
        if basename != name {
            return Err(ProviderError::AmbiguousIdentity(
                "conflicting base checkpoint basename and name".into(),
            ));
        }
    }
    if let Some(value) = basename.or(name) {
        require_identity(value, EXPECTED_BASE_CHECKPOINT, "base checkpoint")?;
        return Ok(CheckpointAttestation::Basename {
            value: value.into(),
        });
    }

    let parameter_count = model_info.general_parameter_count.ok_or_else(|| {
        ProviderError::IdentityMismatch("parameter count: required evidence is missing".into())
    })?;
    if parameter_count != EXPECTED_PARAMETER_COUNT {
        return Err(ProviderError::IdentityMismatch(format!(
            "parameter count: expected {EXPECTED_PARAMETER_COUNT}, observed {parameter_count}"
        )));
    }
    Ok(CheckpointAttestation::MetadataComposite {
        architecture: architecture.into(),
        parameter_count,
    })
}

fn present_checkpoint_field<'a>(
    value: Option<&'a str>,
    field: &str,
) -> Result<Option<&'a str>, ProviderError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(ProviderError::IdentityMismatch(format!(
            "{field}: observed value is blank"
        ))),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

#[derive(Deserialize)]
struct PsResponse {
    models: Vec<LoadedModel>,
}

#[derive(Deserialize)]
struct LoadedModel {
    name: String,
    digest: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    model: String,
    message: ChatMessage,
    done: bool,
    total_duration: Option<u64>,
    load_duration: Option<u64>,
    prompt_eval_count: Option<u64>,
    prompt_eval_duration: Option<u64>,
    eval_count: Option<u64>,
    eval_duration: Option<u64>,
}

#[derive(Deserialize)]
struct ChatMessage {
    content: String,
}

#[cfg(test)]
mod tests {
    use super::{
        OllamaAdapter, OllamaRequest, OllamaResponse, OllamaTransport, OllamaTransportError,
        OLLAMA_MODEL_DIGEST, OLLAMA_MODEL_TAG,
    };
    use crate::provider::{
        CheckpointAttestation, EngineRevision, ProviderAdapter, ProviderError, ProviderMessage,
    };
    use std::cell::RefCell;
    use std::collections::VecDeque;

    const VERSION: &str = r#"{"version":"0.11.0"}"#;
    const TAGS: &str = r#"{
        "models": [{
            "name": "gemma3:4b-it-q4_K_M",
            "model": "gemma3:4b-it-q4_K_M",
            "modified_at": "2026-01-01T00:00:00Z",
            "size": 3338801804,
            "digest": "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a",
            "details": {
                "parent_model": "",
                "format": "gguf",
                "family": "gemma3",
                "families": ["gemma3"],
                "parameter_size": "4.3B",
                "quantization_level": "Q4_K_M"
            }
        }]
    }"#;
    const SHOW: &str = r#"{
        "license": "Gemma Terms of Use",
        "engine_revision": "ollama-engine-build-7",
        "modelfile": "FROM /models/gemma-3-4b-it-Q4_K_M.gguf",
        "parameters": "temperature 0",
        "template": "{{- range .Messages }}{{ .Role }}: {{ .Content }}\n{{- end }}",
        "details": {
            "parent_model": "",
            "format": "gguf",
            "family": "gemma3",
            "families": ["gemma3"],
            "parameter_size": "4.3B",
            "quantization_level": "Q4_K_M"
        },
        "model_info": {
            "general.name": "gemma-3-4b-it",
            "general.architecture": "gemma3",
            "general.parameter_count": 4299915632,
            "tokenizer.ggml.model": "llama"
        },
        "capabilities": ["completion", "tools"]
    }"#;
    const PS_EMPTY: &str = r#"{"models":[]}"#;

    #[derive(Default)]
    struct ScriptedTransport {
        results: RefCell<VecDeque<Result<OllamaResponse, OllamaTransportError>>>,
        requests: RefCell<Vec<OllamaRequest>>,
    }

    impl ScriptedTransport {
        fn from_bodies(bodies: &[&str]) -> Self {
            Self {
                results: RefCell::new(
                    bodies
                        .iter()
                        .map(|body| Ok(OllamaResponse::ok(*body)))
                        .collect(),
                ),
                requests: RefCell::new(Vec::new()),
            }
        }

        fn from_result(result: Result<OllamaResponse, OllamaTransportError>) -> Self {
            Self {
                results: RefCell::new(VecDeque::from([result])),
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl OllamaTransport for ScriptedTransport {
        fn execute(&self, request: &OllamaRequest) -> Result<OllamaResponse, OllamaTransportError> {
            self.requests.borrow_mut().push(request.clone());
            self.results
                .borrow_mut()
                .pop_front()
                .expect("complete fixture response")
        }
    }

    fn inspect_with(
        tags: &str,
        show: &str,
        ps: &str,
    ) -> Result<super::OllamaInspection, ProviderError> {
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_bodies(&[VERSION, tags, show, tags, ps]),
        )?;
        adapter.inspect()
    }

    #[test]
    fn parses_all_inspection_endpoints_into_verified_identity() {
        let transport = ScriptedTransport::from_bodies(&[VERSION, TAGS, SHOW, TAGS, PS_EMPTY]);
        let adapter = OllamaAdapter::new("http://127.0.0.1:11434", transport).unwrap();

        let inspection = adapter.inspect().unwrap();

        assert_eq!(inspection.candidate.artifact.artifact_id, OLLAMA_MODEL_TAG);
        assert_eq!(
            inspection.candidate.artifact.digest_sha256,
            OLLAMA_MODEL_DIGEST
        );
        assert_eq!(
            inspection.candidate.artifact.base_checkpoint,
            "google/gemma-3-4b-it"
        );
        assert_eq!(
            inspection.candidate.artifact.checkpoint_attestation,
            CheckpointAttestation::Basename {
                value: "gemma-3-4b-it".into()
            }
        );
        assert_eq!(inspection.candidate.artifact.format, "gguf");
        assert_eq!(inspection.candidate.artifact.quantization, "Q4_K_M");
        assert_eq!(inspection.candidate.endpoint, "http://127.0.0.1:11434");
        assert_eq!(
            inspection.candidate.artifact.tokenizer_evidence,
            ["ollama_api_show:model_info.tokenizer.ggml.model=llama"]
        );
        assert_eq!(
            inspection.candidate.artifact.template_evidence,
            [concat!(
                "ollama_api_show:template_sha256=",
                "c18169ef197353927df82b637b4f7d829f0e1030141b0302f3833501ea53539e",
                ";bytes=60"
            )]
        );
        assert!(!inspection.candidate.artifact.template_evidence[0].contains("range"));
        assert_eq!(
            inspection.candidate.engine.engine_kind,
            "ollama-managed-gguf-engine"
        );
        assert!(inspection
            .candidate
            .engine
            .evidence
            .iter()
            .any(|item| item.contains("general.architecture=gemma3")));
        assert!(inspection
            .candidate
            .engine
            .evidence
            .contains(&"ollama_api_version:version=0.11.0".into()));
        assert!(inspection
            .candidate
            .engine
            .evidence
            .contains(&"ollama_api_tags:details.family=gemma3".into()));
        assert!(inspection
            .candidate
            .engine
            .evidence
            .contains(&"ollama_api_show:model_info.general.architecture=gemma3".into()));
        assert_eq!(
            inspection.candidate.engine.invalidation_keys,
            [
                "provider_version=0.11.0",
                "engine_revision=ollama-engine-build-7"
            ]
        );
        assert_eq!(inspection.candidate.engine.provider_version, "0.11.0");
        assert_eq!(
            inspection.candidate.engine.engine_revision,
            EngineRevision::Known("ollama-engine-build-7".into())
        );
        assert!(inspection
            .candidate
            .engine
            .evidence
            .contains(&"ollama_api_show:engine_revision=ollama-engine-build-7".into()));
        assert!(!inspection.activity.target_loaded);
        assert!(inspection.activity.unrelated_models.is_empty());

        let requests = adapter.transport().requests.borrow();
        assert_eq!(
            requests
                .iter()
                .map(OllamaRequest::method_path)
                .collect::<Vec<_>>(),
            [
                ("GET", "/api/version"),
                ("GET", "/api/tags"),
                ("POST", "/api/show"),
                ("GET", "/api/tags"),
                ("GET", "/api/ps"),
            ]
        );
    }

    #[test]
    fn rejects_empty_template_evidence() {
        let show = SHOW.replace(
            "{{- range .Messages }}{{ .Role }}: {{ .Content }}\\n{{- end }}",
            "",
        );
        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();
        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("chat template evidence is empty"))
        );
    }

    #[test]
    fn does_not_infer_engine_revision_from_model_tag() {
        let show = SHOW.replace(
            "        \"engine_revision\": \"ollama-engine-build-7\",\n",
            "",
        );

        let inspection = inspect_with(TAGS, &show, PS_EMPTY).unwrap();

        assert_eq!(
            inspection.candidate.engine.engine_revision,
            EngineRevision::Unknown { hidden: true }
        );
        assert!(inspection
            .candidate
            .engine
            .evidence
            .contains(&"ollama_api_show:engine_revision=unknown;hidden=true".into()));
        assert_eq!(
            inspection.candidate.engine.invalidation_keys,
            ["provider_version=0.11.0"]
        );
        assert!(!inspection
            .candidate
            .engine
            .evidence
            .iter()
            .any(|item| item.contains(OLLAMA_MODEL_TAG)));
    }

    #[test]
    fn rejects_blank_observed_engine_revision() {
        let show = SHOW.replace(
            "\"engine_revision\": \"ollama-engine-build-7\"",
            "\"engine_revision\": \"   \"",
        );

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("engine revision") && message.contains("missing"))
        );
    }

    #[test]
    fn rejects_missing_provider_version() {
        let version = r#"{}"#;
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_bodies(&[version, TAGS, SHOW, TAGS, PS_EMPTY]),
        )
        .unwrap();

        assert!(adapter.inspect().is_err());
    }

    #[test]
    fn rejects_missing_artifact_digest() {
        let tags = TAGS.replace(
            "            \"digest\": \"a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a\",\n",
            "",
        );

        assert!(inspect_with(&tags, SHOW, PS_EMPTY).is_err());
    }

    #[test]
    fn admits_exact_metadata_composite_when_basename_is_absent() {
        let show = SHOW.replace("\n            \"general.name\": \"gemma-3-4b-it\",", "");
        let inspection = inspect_with(TAGS, &show, PS_EMPTY).unwrap();

        assert_eq!(
            inspection.candidate.artifact.checkpoint_attestation,
            CheckpointAttestation::MetadataComposite {
                architecture: "gemma3".into(),
                parameter_count: 4_299_915_632,
            }
        );
    }

    #[test]
    fn accepts_explicit_general_basename_attestation() {
        let show = SHOW
            .replace("\n            \"general.name\": \"gemma-3-4b-it\",", "")
            .replace(
                "            \"general.architecture\": \"gemma3\",",
                "            \"general.basename\": \"gemma-3-4b-it\",\n            \"general.architecture\": \"gemma3\",",
            );

        let inspection = inspect_with(TAGS, &show, PS_EMPTY).unwrap();

        assert_eq!(
            inspection.candidate.artifact.checkpoint_attestation,
            CheckpointAttestation::Basename {
                value: "gemma-3-4b-it".into()
            }
        );
    }

    #[test]
    fn conflicting_present_basename_refuses_without_composite_fallback() {
        let show = SHOW.replace(
            "\"general.name\": \"gemma-3-4b-it\"",
            "\"general.name\": \"other-checkpoint\"",
        );

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("base checkpoint"))
        );
    }

    #[test]
    fn present_blank_basename_refuses_without_composite_fallback() {
        let show = SHOW.replace(
            "\"general.name\": \"gemma-3-4b-it\"",
            "\"general.name\": \"   \"",
        );

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("base checkpoint") && message.contains("blank"))
        );
    }

    #[test]
    fn conflicting_general_basename_and_name_refuses_as_ambiguous() {
        let show = SHOW.replace(
            "\"general.name\": \"gemma-3-4b-it\"",
            "\"general.basename\": \"other-checkpoint\",\n            \"general.name\": \"gemma-3-4b-it\"",
        );

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::AmbiguousIdentity(message) if message.contains("basename") && message.contains("name"))
        );
    }

    #[test]
    fn conflicting_composite_architecture_refuses() {
        let show = SHOW
            .replace("\n            \"general.name\": \"gemma-3-4b-it\",", "")
            .replace(
                "\"general.architecture\": \"gemma3\"",
                "\"general.architecture\": \"other\"",
            );

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("model architecture"))
        );
    }

    #[test]
    fn missing_composite_architecture_refuses() {
        let show = SHOW
            .replace("\n            \"general.name\": \"gemma-3-4b-it\",", "")
            .replace("\n            \"general.architecture\": \"gemma3\",", "");

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("model architecture") && message.contains("missing"))
        );
    }

    #[test]
    fn missing_composite_parameter_count_refuses() {
        let show = SHOW
            .replace("\n            \"general.name\": \"gemma-3-4b-it\",", "")
            .replace("\n            \"general.parameter_count\": 4299915632,", "");

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("parameter count") && message.contains("missing"))
        );
    }

    #[test]
    fn conflicting_composite_parameter_count_refuses() {
        let show = SHOW
            .replace("\n            \"general.name\": \"gemma-3-4b-it\",", "")
            .replace("4299915632", "4299915631");

        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();

        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("parameter count"))
        );
    }

    #[test]
    fn rejects_missing_tokenizer_evidence() {
        let show = SHOW.replace(",\n            \"tokenizer.ggml.model\": \"llama\"", "");
        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();
        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("tokenizer") && message.contains("missing"))
        );
    }

    #[test]
    fn rejects_empty_tokenizer_evidence() {
        let show = SHOW.replace(
            "\"tokenizer.ggml.model\": \"llama\"",
            "\"tokenizer.ggml.model\": \"\"",
        );
        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();
        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("tokenizer") && message.contains("missing"))
        );
    }

    #[test]
    fn rejects_whitespace_only_tokenizer_evidence() {
        let show = SHOW.replace(
            "\"tokenizer.ggml.model\": \"llama\"",
            "\"tokenizer.ggml.model\": \"  \\t\"",
        );
        let error = inspect_with(TAGS, &show, PS_EMPTY).unwrap_err();
        assert!(
            matches!(error, ProviderError::IdentityMismatch(message) if message.contains("tokenizer") && message.contains("missing"))
        );
    }

    #[test]
    fn health_is_typed_and_reverifies_exact_identity() {
        let transport = ScriptedTransport::from_bodies(&[VERSION, TAGS, SHOW, TAGS, PS_EMPTY]);
        let mut adapter = OllamaAdapter::new("http://127.0.0.1:11434", transport).unwrap();
        let health = adapter.verify_health().unwrap();
        assert!(health.healthy);
        assert_eq!(health.provider_version.as_deref(), Some("0.11.0"));
    }

    #[test]
    fn activity_observation_is_an_exact_ps_snapshot_only() {
        let ps = r#"{"models":[{"name":"gemma3:4b","model":"gemma3:4b","digest":"a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a"}]}"#;
        let transport = ScriptedTransport::from_bodies(&[ps]);
        let adapter = OllamaAdapter::new("http://127.0.0.1:11434", transport).unwrap();
        let activity = adapter.observe_activity().unwrap();
        assert!(activity.target_active);
        assert!(activity.unrelated_activity.is_empty());
        assert_eq!(
            adapter.transport().requests.borrow().as_slice(),
            &[OllamaRequest::Ps]
        );
    }

    #[test]
    fn ollama_adapter_is_usable_through_object_safe_boundary() {
        let adapter: Box<dyn ProviderAdapter> = Box::new(
            OllamaAdapter::new(
                "http://127.0.0.1:11434",
                ScriptedTransport::from_bodies(&[VERSION, TAGS, SHOW, TAGS, PS_EMPTY]),
            )
            .unwrap(),
        );
        assert_eq!(
            adapter.inspect_candidate().unwrap().artifact.artifact_id,
            OLLAMA_MODEL_TAG
        );
    }

    #[test]
    fn parses_chat_content_and_optional_timing_counters() {
        let chat = r#"{
            "model":"gemma3:4b-it-q4_K_M",
            "created_at":"2026-01-01T00:00:00Z",
            "message":{"role":"assistant","content":"{\"answer\":\"ok\"}"},
            "done":true,
            "done_reason":"stop",
            "total_duration":123,
            "load_duration":4,
            "prompt_eval_count":10,
            "prompt_eval_duration":11,
            "eval_count":12,
            "eval_duration":13
        }"#;
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_bodies(&[chat]),
        )
        .unwrap();

        let turn = adapter
            .chat(&[ProviderMessage::user("answer without tools")])
            .unwrap();

        assert_eq!(turn.content, r#"{"answer":"ok"}"#);
        assert_eq!(turn.timing.total_duration_ns, Some(123));
        assert_eq!(turn.timing.load_duration_ns, Some(4));
        assert_eq!(turn.timing.prompt_eval_count, Some(10));
        assert_eq!(turn.timing.prompt_eval_duration_ns, Some(11));
        assert_eq!(turn.timing.eval_count, Some(12));
        assert_eq!(turn.timing.eval_duration_ns, Some(13));

        let request = adapter.transport().requests.borrow()[0].clone();
        let body = request.json_body();
        assert_eq!(request.method_path(), ("POST", "/api/chat"));
        assert_eq!(body["stream"], false);
        assert_eq!(body["options"]["num_ctx"], 4096);
        assert_eq!(body["options"]["temperature"], 0);
        assert_eq!(body["options"]["seed"], 42);
        assert_eq!(body["options"]["num_predict"], 256);
    }

    #[test]
    fn maps_unreachable_transport_failure() {
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_result(Err(OllamaTransportError::Unreachable)),
        )
        .unwrap();
        assert!(matches!(adapter.inspect(), Err(ProviderError::Unreachable)));
    }

    #[test]
    fn maps_timeout_transport_failure() {
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_result(Err(OllamaTransportError::Timeout)),
        )
        .unwrap();
        assert!(matches!(adapter.inspect(), Err(ProviderError::Timeout)));
    }

    #[test]
    fn rejects_redirect_response() {
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_result(Ok(OllamaResponse {
                status: 302,
                body: String::new(),
            })),
        )
        .unwrap();
        assert!(matches!(
            adapter.inspect(),
            Err(ProviderError::RedirectRejected)
        ));
    }

    #[test]
    fn rejects_malformed_json() {
        let error = inspect_with("not json", SHOW, PS_EMPTY).unwrap_err();
        assert!(matches!(error, ProviderError::MalformedResponse { .. }));
    }

    #[test]
    fn rejects_duplicate_exact_tag_as_ambiguous() {
        let duplicate = TAGS.replace("]\n    }", ",{\"name\":\"gemma3:4b-it-q4_K_M\",\"model\":\"gemma3:4b-it-q4_K_M\",\"modified_at\":\"2026-01-01T00:00:00Z\",\"size\":3338801804,\"digest\":\"a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a\",\"details\":{\"parent_model\":\"\",\"format\":\"gguf\",\"family\":\"gemma3\",\"families\":[\"gemma3\"],\"parameter_size\":\"4.3B\",\"quantization_level\":\"Q4_K_M\"}}]\n    }");
        let error = inspect_with(&duplicate, SHOW, PS_EMPTY).unwrap_err();
        assert!(matches!(error, ProviderError::AmbiguousIdentity(_)));
    }

    #[test]
    fn rejects_unexpected_digest() {
        let wrong = TAGS.replace(OLLAMA_MODEL_DIGEST, &"00".repeat(32));
        let error = inspect_with(&wrong, SHOW, PS_EMPTY).unwrap_err();
        assert!(error.to_string().contains("digest"));
    }

    #[test]
    fn canonicalizes_explicit_sha256_tag_digest() {
        let prefixed = TAGS.replace(
            OLLAMA_MODEL_DIGEST,
            &format!("sha256:{OLLAMA_MODEL_DIGEST}"),
        );

        let inspection = inspect_with(&prefixed, SHOW, PS_EMPTY).unwrap();

        assert_eq!(
            inspection.candidate.artifact.digest_sha256,
            OLLAMA_MODEL_DIGEST
        );
    }

    #[test]
    fn reports_unrelated_loaded_model_without_claiming_target_activity() {
        let ps = r#"{"models":[{"name":"qwen3:8b","model":"qwen3:8b","size":5220000000,"digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","details":{"parent_model":"","format":"gguf","family":"qwen3","families":["qwen3"],"parameter_size":"8B","quantization_level":"Q4_K_M"},"expires_at":"2026-01-01T00:05:00Z","size_vram":5220000000}]}"#;
        let inspection = inspect_with(TAGS, SHOW, ps).unwrap();
        assert!(!inspection.activity.target_loaded);
        assert_eq!(inspection.activity.unrelated_models, ["qwen3:8b"]);
    }

    #[test]
    fn ps_correlates_target_activity_by_exact_digest_despite_canonical_name() {
        let wrong_digest = r#"{"models":[{"name":"gemma3:4b-it-q4_K_M","model":"gemma3:4b-it-q4_K_M","digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}"#;
        let inspection = inspect_with(TAGS, SHOW, wrong_digest).unwrap();
        assert!(!inspection.activity.target_loaded);
        assert_eq!(inspection.activity.unrelated_models, [OLLAMA_MODEL_TAG]);

        let canonical_name = format!(
            r#"{{"models":[{{"name":"gemma3:4b","model":"gemma3:4b","digest":"{OLLAMA_MODEL_DIGEST}"}}]}}"#
        );
        let inspection = inspect_with(TAGS, SHOW, &canonical_name).unwrap();
        assert!(inspection.activity.target_loaded);
        assert!(inspection.activity.unrelated_models.is_empty());

        let exact = format!(
            r#"{{"models":[{{"name":"{OLLAMA_MODEL_TAG}","model":"{OLLAMA_MODEL_TAG}","digest":"{OLLAMA_MODEL_DIGEST}"}}]}}"#
        );
        let inspection = inspect_with(TAGS, SHOW, &exact).unwrap();
        assert!(inspection.activity.target_loaded);
        assert!(inspection.activity.unrelated_models.is_empty());
    }

    #[test]
    fn rejects_tag_identity_change_between_show_and_activity_probe() {
        let changed = TAGS.replace(OLLAMA_MODEL_DIGEST, &"00".repeat(32));
        let adapter = OllamaAdapter::new(
            "http://127.0.0.1:11434",
            ScriptedTransport::from_bodies(&[VERSION, TAGS, SHOW, &changed, PS_EMPTY]),
        )
        .unwrap();
        let error = adapter.inspect().unwrap_err();
        assert!(error.to_string().contains("changed during inspection"));
    }

    #[test]
    fn endpoint_must_be_explicit_http_loopback_without_extra_path() {
        for invalid in [
            "https://127.0.0.1:11434",
            "http://localhost:11434",
            "http://0.0.0.0:11434",
            "http://127.0.0.1:11434/proxy",
            "http://127.0.0.1:11434?token=secret",
            "http://[::1]:11434",
        ] {
            assert!(matches!(
                OllamaAdapter::new(invalid, ScriptedTransport::default()),
                Err(ProviderError::InvalidEndpoint(_))
            ));
        }
    }

    #[test]
    fn request_type_exposes_only_read_only_inspection_and_chat_routes() {
        let requests = [
            OllamaRequest::Version,
            OllamaRequest::Tags,
            OllamaRequest::Show,
            OllamaRequest::Ps,
            OllamaRequest::Chat { messages: vec![] },
        ];
        assert_eq!(
            requests.map(|request| request.method_path()),
            [
                ("GET", "/api/version"),
                ("GET", "/api/tags"),
                ("POST", "/api/show"),
                ("GET", "/api/ps"),
                ("POST", "/api/chat"),
            ]
        );
    }
}
