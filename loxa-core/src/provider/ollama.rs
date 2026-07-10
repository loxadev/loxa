use super::{
    ArtifactIdentity, CandidateSpec, ControlledRun, EngineIdentity, EngineRevision,
    GenerationSettings, NormalizedTurn, ProviderActivityObservation, ProviderAdapter,
    ProviderError, ProviderHealth, ProviderMessage, ProviderOwnership, ProviderTiming,
    CANDIDATE_IDENTITY_SCHEMA_VERSION,
};
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

pub const OLLAMA_MODEL_TAG: &str = "gemma3:4b-it-q4_K_M";
pub const OLLAMA_MODEL_DIGEST: &str =
    "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a";
const OLLAMA_ENDPOINT: &str = "http://127.0.0.1:11434";

pub fn provisional_candidate_spec() -> CandidateSpec {
    CandidateSpec {
        schema_version: 1,
        candidate_id: "ollama-gemma3-4b-it-q4-k-m".into(),
        provider_kind: super::ProviderKind::Ollama,
        ownership: ProviderOwnership::Attached,
        endpoint: OLLAMA_ENDPOINT.into(),
        artifact: ArtifactIdentity {
            schema_version: 1,
            artifact_id: OLLAMA_MODEL_TAG.into(),
            digest_sha256: OLLAMA_MODEL_DIGEST.into(),
            base_checkpoint: "google/gemma-3-4b-it".into(),
            format: "gguf".into(),
            quantization: "Q4_K_M".into(),
            tokenizer_evidence: vec!["provisional_expected_tokenizer=gemma".into()],
            template_evidence: vec!["provisional_expected_template=ollama_gemma3".into()],
        },
        engine: EngineIdentity {
            schema_version: 1,
            engine_kind: "ollama-managed-gguf-engine".into(),
            provider_version: "unverified".into(),
            engine_revision: EngineRevision::Unknown,
            evidence: vec!["provisional_expected_engine=ollama_managed_gguf".into()],
            invalidation_keys: vec!["provider_version=unverified".into()],
        },
        settings: GenerationSettings::pinned_v1(),
    }
}

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
        let model = unique_exact_tag(tags)?;
        require_identity(&model.digest, OLLAMA_MODEL_DIGEST, "artifact digest")?;
        require_identity(&model.details.format, "gguf", "artifact format")?;
        require_identity(
            &model.details.quantization_level,
            "Q4_K_M",
            "artifact quantization",
        )?;
        require_identity(&model.details.family, "gemma3", "model family")?;

        let show: ShowResponse = self.request_json(OllamaRequest::Show)?;
        require_identity(&show.details.format, "gguf", "show format")?;
        require_identity(
            &show.details.quantization_level,
            "Q4_K_M",
            "show quantization",
        )?;
        require_identity(&show.details.family, "gemma3", "show family")?;
        require_identity(&show.template, "{{ .Messages }}", "chat template")?;
        require_identity(
            show.model_info.general_name.as_deref().unwrap_or_default(),
            "gemma-3-4b-it",
            "base checkpoint",
        )?;
        require_identity(
            show.model_info
                .general_architecture
                .as_deref()
                .unwrap_or_default(),
            "gemma3",
            "model architecture",
        )?;
        require_identity(
            show.model_info
                .tokenizer_model
                .as_deref()
                .unwrap_or_default(),
            "gemma",
            "tokenizer",
        )?;

        let tags_after_show: TagsResponse = self.request_json(OllamaRequest::Tags)?;
        let model_after_show = unique_exact_tag(tags_after_show)?;
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
                schema_version: 1,
                artifact_id: OLLAMA_MODEL_TAG.into(),
                digest_sha256: model.digest,
                base_checkpoint: "google/gemma-3-4b-it".into(),
                format: model.details.format,
                quantization: model.details.quantization_level,
                tokenizer_evidence: vec!["/api/show:model_info.tokenizer.ggml.model=gemma".into()],
                template_evidence: vec!["/api/show:template={{ .Messages }}".into()],
            },
            engine: EngineIdentity {
                schema_version: 1,
                engine_kind: "ollama-managed-gguf-engine".into(),
                provider_version: version.version.clone(),
                engine_revision: EngineRevision::Unknown,
                evidence: vec![
                    format!("/api/version:version={}", version.version),
                    "/api/tags:details.family=gemma3".into(),
                    "/api/show:model_info.general.architecture=gemma3".into(),
                ],
                invalidation_keys: vec![format!("provider_version={}", version.version)],
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
    model.name == OLLAMA_MODEL_TAG
        && model.model == OLLAMA_MODEL_TAG
        && model.digest == OLLAMA_MODEL_DIGEST
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
    template: String,
    details: ModelDetails,
    model_info: ModelInfo,
}

#[derive(Default, Deserialize)]
struct ModelInfo {
    #[serde(rename = "general.name")]
    general_name: Option<String>,
    #[serde(rename = "general.architecture")]
    general_architecture: Option<String>,
    #[serde(rename = "tokenizer.ggml.model")]
    tokenizer_model: Option<String>,
}

#[derive(Deserialize)]
struct PsResponse {
    models: Vec<LoadedModel>,
}

#[derive(Deserialize)]
struct LoadedModel {
    name: String,
    model: String,
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
    use crate::provider::{EngineRevision, ProviderAdapter, ProviderError, ProviderMessage};
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
        "modelfile": "FROM /models/gemma-3-4b-it-Q4_K_M.gguf",
        "parameters": "temperature 0",
        "template": "{{ .Messages }}",
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
            "tokenizer.ggml.model": "gemma"
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
        assert_eq!(inspection.candidate.artifact.format, "gguf");
        assert_eq!(inspection.candidate.artifact.quantization, "Q4_K_M");
        assert_eq!(inspection.candidate.endpoint, "http://127.0.0.1:11434");
        assert_eq!(
            inspection.candidate.artifact.tokenizer_evidence,
            ["/api/show:model_info.tokenizer.ggml.model=gemma"]
        );
        assert_eq!(
            inspection.candidate.artifact.template_evidence,
            ["/api/show:template={{ .Messages }}"]
        );
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
            .contains(&"/api/version:version=0.11.0".into()));
        assert_eq!(
            inspection.candidate.engine.invalidation_keys,
            ["provider_version=0.11.0"]
        );
        assert_eq!(inspection.candidate.engine.provider_version, "0.11.0");
        assert_eq!(
            inspection.candidate.engine.engine_revision,
            EngineRevision::Unknown
        );
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
    fn health_is_typed_and_reverifies_exact_identity() {
        let transport = ScriptedTransport::from_bodies(&[VERSION, TAGS, SHOW, TAGS, PS_EMPTY]);
        let mut adapter = OllamaAdapter::new("http://127.0.0.1:11434", transport).unwrap();
        let health = adapter.verify_health().unwrap();
        assert!(health.healthy);
        assert_eq!(health.provider_version.as_deref(), Some("0.11.0"));
    }

    #[test]
    fn activity_observation_is_an_exact_ps_snapshot_only() {
        let ps = r#"{"models":[{"name":"gemma3:4b-it-q4_K_M","model":"gemma3:4b-it-q4_K_M","digest":"a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a"}]}"#;
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
    fn reports_unrelated_loaded_model_without_claiming_target_activity() {
        let ps = r#"{"models":[{"name":"qwen3:8b","model":"qwen3:8b","size":5220000000,"digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","details":{"parent_model":"","format":"gguf","family":"qwen3","families":["qwen3"],"parameter_size":"8B","quantization_level":"Q4_K_M"},"expires_at":"2026-01-01T00:05:00Z","size_vram":5220000000}]}"#;
        let inspection = inspect_with(TAGS, SHOW, ps).unwrap();
        assert!(!inspection.activity.target_loaded);
        assert_eq!(inspection.activity.unrelated_models, ["qwen3:8b"]);
    }

    #[test]
    fn ps_requires_exact_name_model_and_digest_for_target_activity() {
        let wrong_digest = r#"{"models":[{"name":"gemma3:4b-it-q4_K_M","model":"gemma3:4b-it-q4_K_M","digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}"#;
        let inspection = inspect_with(TAGS, SHOW, wrong_digest).unwrap();
        assert!(!inspection.activity.target_loaded);
        assert_eq!(inspection.activity.unrelated_models, [OLLAMA_MODEL_TAG]);

        let wrong_model = format!(
            r#"{{"models":[{{"name":"{OLLAMA_MODEL_TAG}","model":"other:latest","digest":"{OLLAMA_MODEL_DIGEST}"}}]}}"#
        );
        let inspection = inspect_with(TAGS, SHOW, &wrong_model).unwrap();
        assert!(!inspection.activity.target_loaded);
        assert_eq!(inspection.activity.unrelated_models, [OLLAMA_MODEL_TAG]);

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
