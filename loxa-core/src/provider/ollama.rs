use super::transport::{JsonTransport, ReqwestJsonTransport, StreamFraming, TimedJsonEvent};
use super::{InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError, ToolCall};
use crate::plan::{CandidateIdentity, ProviderKind};
use serde_json::{json, Value};

pub struct OllamaAdapter {
    identity: CandidateIdentity,
    base_url: String,
    model: String,
    transport: Box<dyn JsonTransport>,
}

impl OllamaAdapter {
    pub fn new(
        identity: CandidateIdentity,
        base_url: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::with_transport(
            identity,
            base_url,
            model,
            Box::new(ReqwestJsonTransport::new()),
        )
    }

    pub(crate) fn with_transport(
        identity: CandidateIdentity,
        base_url: impl Into<String>,
        model: impl Into<String>,
        transport: Box<dyn JsonTransport>,
    ) -> Self {
        Self {
            identity,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
            transport,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn validate_identity(&self) -> Result<(), ProviderError> {
        let errors = self.identity.identity_errors();
        if !errors.is_empty() {
            return Err(ProviderError::Identity(format!(
                "ollama candidate identity is missing: {}",
                errors.join(", ")
            )));
        }
        if self.identity.provider != ProviderKind::Ollama {
            return Err(ProviderError::Identity(
                "ollama adapter requires provider kind ollama".into(),
            ));
        }
        if self.model.trim().is_empty() {
            return Err(ProviderError::Identity(
                "ollama requested model is empty".into(),
            ));
        }
        validate_sha256_digest(&self.identity.artifact_digest).map(|_| ())
    }

    fn validate_sampling(&self) -> Result<(), ProviderError> {
        if self.identity.sampling.temperature_milli != 0
            || self.identity.sampling.top_p_milli != 1000
            || self.identity.sampling.seed != 1
        {
            return Err(ProviderError::Identity(
                "ollama invocation requires deterministic sampling: temperature=0, top_p=1, seed=1"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl ProviderAdapter for OllamaAdapter {
    fn identity(&self) -> &CandidateIdentity {
        &self.identity
    }

    fn inspect(&mut self) -> Result<(), ProviderError> {
        self.validate_identity()?;

        let version = self.transport.get_json(&self.endpoint("/api/version"))?;
        let tags = self.transport.get_json(&self.endpoint("/api/tags"))?;
        let show = self
            .transport
            .post_json(&self.endpoint("/api/show"), &json!({"model": self.model}))?;

        let observed_version = version
            .get("version")
            .and_then(Value::as_str)
            .filter(|version| !version.trim().is_empty())
            .ok_or_else(|| {
                ProviderError::Identity("ollama version response is missing version".into())
            })?;
        if observed_version != self.identity.provider_version {
            return Err(ProviderError::Identity(format!(
                "ollama version mismatch: expected {}, observed {observed_version}",
                self.identity.provider_version
            )));
        }

        let models = tags
            .get("models")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ProviderError::Protocol("ollama model inventory is missing models array".into())
            })?;
        let mut exact_models = models
            .iter()
            .filter(|model| model.get("name").and_then(Value::as_str) == Some(self.model.as_str()));
        let exact_model = exact_models.next().ok_or_else(|| {
            ProviderError::Identity(format!(
                "ollama model inventory has no exact model {}",
                self.model
            ))
        })?;
        if exact_models.next().is_some() {
            return Err(ProviderError::Identity(format!(
                "ollama model inventory has duplicate exact model {}",
                self.model
            )));
        }
        let observed_digest = exact_model
            .get("digest")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::Identity("ollama model inventory is missing digest".into())
            })?;
        let expected_hex = validate_sha256_digest(&self.identity.artifact_digest)?;
        let observed_hex = validate_sha256_digest(observed_digest)?;
        if !expected_hex.eq_ignore_ascii_case(observed_hex) {
            return Err(ProviderError::Identity(format!(
                "ollama model digest mismatch for {}",
                self.model
            )));
        }

        let template_present = show
            .get("template")
            .and_then(Value::as_str)
            .is_some_and(|template| !template.trim().is_empty());
        let details_present = show
            .get("details")
            .and_then(Value::as_object)
            .is_some_and(|details| !details.is_empty());
        if !template_present || !details_present {
            return Err(ProviderError::Identity(
                "ollama show response requires non-empty template and details".into(),
            ));
        }

        Ok(())
    }

    fn invoke(
        &mut self,
        request: &InvocationRequest,
    ) -> Result<InvocationObservation, ProviderError> {
        self.validate_identity()?;
        self.validate_sampling()?;

        let messages = request
            .messages
            .iter()
            .map(|message| json!({"role": message.role, "content": message.content}))
            .collect::<Vec<_>>();
        let tools = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters
                    }
                })
            })
            .collect::<Vec<_>>();
        let body = json!({
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "stream": true,
            "options": {
                "temperature": 0.0,
                "top_p": 1.0,
                "seed": 1,
                "num_predict": request.max_tokens
            }
        });
        let events = self.transport.post_json_stream(
            &self.endpoint("/api/chat"),
            &body,
            StreamFraming::JsonLines,
        )?;

        normalize_events(events)
    }
}

fn validate_sha256_digest(digest: &str) -> Result<&str, ProviderError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(ProviderError::Identity(
            "ollama model digest must use explicit sha256:<hex> representation".into(),
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ProviderError::Identity(
            "ollama model digest must use explicit sha256:<hex> representation".into(),
        ));
    }
    Ok(hex)
}

fn normalize_events(events: Vec<TimedJsonEvent>) -> Result<InvocationObservation, ProviderError> {
    let terminal = events
        .last()
        .ok_or_else(|| ProviderError::Protocol("ollama chat stream returned no events".into()))?;
    if terminal.value.get("done").and_then(Value::as_bool) != Some(true) {
        return Err(ProviderError::Protocol(
            "ollama chat stream ended without terminal done: true".into(),
        ));
    }

    let mut content = String::new();
    let mut tool_calls = Vec::new();
    let mut ttft_ns = None;

    for event in &events {
        let Some(message) = event.value.get("message") else {
            continue;
        };
        if let Some(fragment) = message.get("content").and_then(Value::as_str) {
            if !fragment.is_empty() {
                ttft_ns.get_or_insert(event.elapsed_ns);
                content.push_str(fragment);
            }
        }
        if let Some(calls) = message.get("tool_calls") {
            let calls = calls.as_array().ok_or_else(|| {
                ProviderError::Protocol("ollama message tool_calls is not an array".into())
            })?;
            if !calls.is_empty() {
                ttft_ns.get_or_insert(event.elapsed_ns);
            }
            for call in calls {
                let function = call.get("function").ok_or_else(|| {
                    ProviderError::Protocol("ollama tool call is missing function".into())
                })?;
                let name = function
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .ok_or_else(|| {
                        ProviderError::Protocol("ollama tool call has no function name".into())
                    })?;
                let arguments = normalize_arguments(function.get("arguments"))?;
                tool_calls.push(ToolCall {
                    name: name.to_string(),
                    arguments,
                });
            }
        }
    }

    let prompt_tokens = terminal
        .value
        .get("prompt_eval_count")
        .and_then(Value::as_u64);
    let completion_tokens = terminal.value.get("eval_count").and_then(Value::as_u64);
    let prompt_rate = rate_per_second(
        prompt_tokens,
        terminal
            .value
            .get("prompt_eval_duration")
            .and_then(Value::as_u64),
    );
    let decode_rate = rate_per_second(
        completion_tokens,
        terminal.value.get("eval_duration").and_then(Value::as_u64),
    );
    let total_duration_ns = terminal
        .value
        .get("total_duration")
        .and_then(Value::as_u64)
        .unwrap_or(terminal.elapsed_ns);
    let raw_events = events.into_iter().map(|event| event.value).collect();

    Ok(InvocationObservation {
        content: (!content.is_empty()).then_some(content),
        tool_calls,
        prompt_tokens,
        completion_tokens,
        ttft_ns,
        total_duration_ns,
        prompt_rate,
        decode_rate,
        raw_events,
    })
}

fn normalize_arguments(arguments: Option<&Value>) -> Result<Value, ProviderError> {
    match arguments {
        Some(Value::String(arguments)) => serde_json::from_str(arguments).map_err(|error| {
            ProviderError::Protocol(format!(
                "ollama tool-call arguments are not valid JSON: {error}"
            ))
        }),
        Some(Value::Object(arguments)) => Ok(Value::Object(arguments.clone())),
        _ => Err(ProviderError::Protocol(
            "ollama tool-call arguments are missing or invalid".into(),
        )),
    }
}

fn rate_per_second(tokens: Option<u64>, duration_ns: Option<u64>) -> Option<f64> {
    match (tokens, duration_ns) {
        (Some(tokens), Some(duration_ns)) if duration_ns > 0 => {
            Some(tokens as f64 * 1_000_000_000.0 / duration_ns as f64)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::OllamaAdapter;
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
    use crate::provider::transport::{
        parse_json_lines_stream, validate_success_status, JsonTransport, StreamFraming,
        TimedJsonEvent,
    };
    use crate::provider::{
        ChatMessage, InvocationRequest, ProviderAdapter, ProviderError, ToolCall, ToolDefinition,
    };
    use reqwest::StatusCode;
    use serde_json::{json, Value};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io::{BufReader, Read};
    use std::rc::Rc;

    enum ExpectedRequest {
        Get {
            url: String,
            response: Result<Value, ProviderError>,
        },
        Post {
            url: String,
            body: Value,
            response: Result<Value, ProviderError>,
        },
        Stream {
            url: String,
            body: Value,
            framing: StreamFraming,
            response: Result<Vec<TimedJsonEvent>, ProviderError>,
        },
    }

    struct FakeTransport {
        expected: Rc<RefCell<VecDeque<ExpectedRequest>>>,
    }

    impl JsonTransport for FakeTransport {
        fn get_json(&mut self, url: &str) -> Result<Value, ProviderError> {
            match self.expected.borrow_mut().pop_front() {
                Some(ExpectedRequest::Get {
                    url: expected_url,
                    response,
                }) => {
                    assert_eq!(url, expected_url);
                    response
                }
                _ => panic!("unexpected GET {url}"),
            }
        }

        fn post_json(&mut self, url: &str, body: &Value) -> Result<Value, ProviderError> {
            match self.expected.borrow_mut().pop_front() {
                Some(ExpectedRequest::Post {
                    url: expected_url,
                    body: expected_body,
                    response,
                }) => {
                    assert_eq!(url, expected_url);
                    assert_eq!(body, &expected_body);
                    response
                }
                _ => panic!("unexpected POST {url}"),
            }
        }

        fn post_json_stream(
            &mut self,
            url: &str,
            body: &Value,
            framing: StreamFraming,
        ) -> Result<Vec<TimedJsonEvent>, ProviderError> {
            match self.expected.borrow_mut().pop_front() {
                Some(ExpectedRequest::Stream {
                    url: expected_url,
                    body: expected_body,
                    framing: expected_framing,
                    response,
                }) => {
                    assert_eq!(url, expected_url);
                    assert_eq!(body, &expected_body);
                    assert_eq!(framing, expected_framing);
                    response
                }
                _ => panic!("unexpected streaming POST {url}"),
            }
        }
    }

    struct FragmentedReader {
        bytes: Vec<u8>,
        offset: usize,
        max_chunk: usize,
    }

    impl FragmentedReader {
        fn new(bytes: impl Into<Vec<u8>>, max_chunk: usize) -> Self {
            Self {
                bytes: bytes.into(),
                offset: 0,
                max_chunk,
            }
        }
    }

    impl Read for FragmentedReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let remaining = self.bytes.len().saturating_sub(self.offset);
            let count = remaining.min(output.len()).min(self.max_chunk);
            output[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
            self.offset += count;
            Ok(count)
        }
    }

    fn fake_transport(
        expected: Vec<ExpectedRequest>,
    ) -> (
        Box<dyn JsonTransport>,
        Rc<RefCell<VecDeque<ExpectedRequest>>>,
    ) {
        let expected = Rc::new(RefCell::new(expected.into()));
        (
            Box::new(FakeTransport {
                expected: Rc::clone(&expected),
            }),
            expected,
        )
    }

    fn assert_fixtures_consumed(expected: &Rc<RefCell<VecDeque<ExpectedRequest>>>) {
        assert!(
            expected.borrow().is_empty(),
            "adapter did not consume every fixture"
        );
    }

    fn identity() -> CandidateIdentity {
        CandidateIdentity {
            candidate_id: "attached-gemma3-4b".into(),
            provider: ProviderKind::Ollama,
            provider_version: "0.9.1".into(),
            engine_revision: Some("ollama-0.9.1-build-7".into()),
            model_id: "gemma3:4b".into(),
            artifact_digest:
                "sha256:04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19".into(),
            tokenizer_digest: "sha256:test-tokenizer".into(),
            chat_template_digest: "sha256:test-template".into(),
            context_tokens: 8192,
            required_free_memory_bytes: 2_863_378_119,
            sampling: SamplingPolicy {
                temperature_milli: 0,
                top_p_milli: 1000,
                seed: 1,
            },
        }
    }

    fn inspection_fixtures(
        identity: &CandidateIdentity,
        version: Value,
        tags: Value,
        show: Value,
    ) -> Vec<ExpectedRequest> {
        vec![
            ExpectedRequest::Get {
                url: "http://127.0.0.1:11434/api/version".into(),
                response: Ok(version),
            },
            ExpectedRequest::Get {
                url: "http://127.0.0.1:11434/api/tags".into(),
                response: Ok(tags),
            },
            ExpectedRequest::Post {
                url: "http://127.0.0.1:11434/api/show".into(),
                body: json!({"model": identity.model_id}),
                response: Ok(show),
            },
        ]
    }

    fn valid_tags(identity: &CandidateIdentity) -> Value {
        json!({
            "models": [{
                "name": identity.model_id,
                "model": identity.model_id,
                "digest": identity.artifact_digest,
                "details": {"quantization_level": "Q4_K_M"}
            }]
        })
    }

    fn valid_show() -> Value {
        json!({
            "template": "{{ .System }} {{ .Prompt }}",
            "details": {"family": "gemma3", "parameter_size": "4.3B"}
        })
    }

    fn weather_request() -> InvocationRequest {
        InvocationRequest {
            messages: vec![ChatMessage::user("What is the weather in Paris?")],
            tools: vec![ToolDefinition::weather()],
            max_tokens: 128,
        }
    }

    fn tool_fixture() -> Value {
        json!({
            "type": "function",
            "function": {
                "name": "weather",
                "description": "Get the weather for a city.",
                "parameters": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"]
                }
            }
        })
    }

    #[test]
    fn ollama_inspection_requires_version_digest_template_and_engine_revision() {
        let candidate = identity();
        let fixtures = inspection_fixtures(
            &candidate,
            json!({"version": "0.9.1"}),
            valid_tags(&candidate),
            valid_show(),
        );
        let (transport, expected) = fake_transport(fixtures);
        let mut adapter = OllamaAdapter::with_transport(
            candidate.clone(),
            "http://127.0.0.1:11434/",
            candidate.model_id.clone(),
            transport,
        );

        assert_eq!(adapter.inspect(), Ok(()));
        assert_fixtures_consumed(&expected);

        for (version, show) in [
            (json!({"version": ""}), valid_show()),
            (
                json!({"version": "0.9.1"}),
                json!({"template": "", "details": {"family": "gemma3"}}),
            ),
            (
                json!({"version": "0.9.1"}),
                json!({"template": "present", "details": {}}),
            ),
        ] {
            let fixtures = inspection_fixtures(&candidate, version, valid_tags(&candidate), show);
            let (transport, expected) = fake_transport(fixtures);
            let mut adapter = OllamaAdapter::with_transport(
                candidate.clone(),
                "http://127.0.0.1:11434",
                candidate.model_id.clone(),
                transport,
            );

            assert!(matches!(adapter.inspect(), Err(ProviderError::Identity(_))));
            assert_fixtures_consumed(&expected);
        }

        let mut missing_revision = candidate.clone();
        missing_revision.engine_revision = None;
        let (transport, expected) = fake_transport(vec![]);
        let mut adapter = OllamaAdapter::with_transport(
            missing_revision,
            "http://127.0.0.1:11434",
            candidate.model_id.clone(),
            transport,
        );
        assert!(matches!(adapter.inspect(), Err(ProviderError::Identity(_))));
        assert_fixtures_consumed(&expected);
    }

    #[test]
    fn ollama_chat_translation_normalizes_tool_calls_and_durations() {
        let body = json!({
            "model": "gemma3:4b",
            "messages": [{"role": "user", "content": "What is the weather in Paris?"}],
            "tools": [tool_fixture()],
            "stream": true,
            "options": {
                "temperature": 0.0,
                "top_p": 1.0,
                "seed": 1,
                "num_predict": 128
            }
        });
        let events = vec![
            TimedJsonEvent::new(
                8,
                json!({"message": {"role": "assistant", "content": ""}, "done": false}),
            ),
            TimedJsonEvent::new(
                13,
                json!({"message": {"role": "assistant", "content": "Let me check. "}, "done": false}),
            ),
            TimedJsonEvent::new(
                21,
                json!({"message": {"role": "assistant", "content": "", "tool_calls": [
                    {"function": {"name": "weather", "arguments": {"city": "Paris"}}}
                ]}, "done": false}),
            ),
            TimedJsonEvent::new(
                34,
                json!({
                    "message": {"role": "assistant", "content": "Done."},
                    "done": true,
                    "total_duration": 50_000_000,
                    "prompt_eval_count": 20,
                    "prompt_eval_duration": 40_000_000,
                    "eval_count": 8,
                    "eval_duration": 80_000_000
                }),
            ),
        ];
        let raw_events = events
            .iter()
            .map(|event| event.value.clone())
            .collect::<Vec<_>>();
        let (transport, expected) = fake_transport(vec![ExpectedRequest::Stream {
            url: "http://127.0.0.1:11434/api/chat".into(),
            body,
            framing: StreamFraming::JsonLines,
            response: Ok(events),
        }]);
        let candidate = identity();
        let mut adapter = OllamaAdapter::with_transport(
            candidate.clone(),
            "http://127.0.0.1:11434/",
            candidate.model_id.clone(),
            transport,
        );

        let observation = adapter.invoke(&weather_request()).expect("ollama invoke");

        assert_eq!(observation.content, Some("Let me check. Done.".into()));
        assert_eq!(
            observation.tool_calls,
            vec![ToolCall {
                name: "weather".into(),
                arguments: json!({"city": "Paris"}),
            }]
        );
        assert_eq!(observation.prompt_tokens, Some(20));
        assert_eq!(observation.completion_tokens, Some(8));
        assert_eq!(observation.ttft_ns, Some(13));
        assert_eq!(observation.total_duration_ns, 50_000_000);
        assert_eq!(observation.prompt_rate, Some(500.0));
        assert_eq!(observation.decode_rate, Some(100.0));
        assert_eq!(observation.raw_events, raw_events);
        assert_fixtures_consumed(&expected);
    }

    #[test]
    fn ollama_stream_rejects_redirect_malformed_json_and_missing_done() {
        assert!(matches!(
            validate_success_status(StatusCode::TEMPORARY_REDIRECT),
            Err(ProviderError::Protocol(message)) if message.contains("redirect")
        ));

        let malformed = FragmentedReader::new(b"{not-json}\n".to_vec(), 2);
        assert!(matches!(
            parse_json_lines_stream(BufReader::new(malformed)),
            Err(ProviderError::Protocol(message)) if message.contains("malformed JSON")
        ));

        let missing_done = FragmentedReader::new(
            b"{\"message\":{\"content\":\"partial\"},\"done\":false}\n".to_vec(),
            3,
        );
        assert!(matches!(
            parse_json_lines_stream(BufReader::new(missing_done)),
            Err(ProviderError::Protocol(message)) if message.contains("done: true")
        ));

        let complete = FragmentedReader::new(
            b"{\"message\":{\"content\":\"ok\"},\"done\":false}\n{\"done\":true}\n".to_vec(),
            1,
        );
        let events = parse_json_lines_stream(BufReader::new(complete)).expect("JSON lines");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].value["message"]["content"], "ok");
        assert_eq!(events[1].value["done"], true);
    }

    #[test]
    fn ollama_inspection_rejects_model_digest_mismatch_and_tag_inference() {
        let candidate = identity();
        for tags in [
            json!({"models": [{
                "name": "gemma3:4b",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }]}),
            json!({"models": [{
                "name": "gemma3:4b",
                "digest": "04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19"
            }]}),
            json!({"models": [{
                "name": "gemma3:4b-latest",
                "digest": candidate.artifact_digest
            }]}),
        ] {
            let fixtures =
                inspection_fixtures(&candidate, json!({"version": "0.9.1"}), tags, valid_show());
            let (transport, _expected) = fake_transport(fixtures);
            let mut adapter = OllamaAdapter::with_transport(
                candidate.clone(),
                "http://127.0.0.1:11434",
                candidate.model_id.clone(),
                transport,
            );

            assert!(matches!(adapter.inspect(), Err(ProviderError::Identity(_))));
        }

        let mut inferred = candidate.clone();
        inferred.model_id = "gemma3:4b-mlx".into();
        inferred.engine_revision = None;
        let (transport, expected) = fake_transport(vec![]);
        let mut adapter = OllamaAdapter::with_transport(
            inferred.clone(),
            "http://127.0.0.1:11434",
            inferred.model_id,
            transport,
        );
        assert!(matches!(
            adapter.inspect(),
            Err(ProviderError::Identity(message)) if message.contains("engine_revision")
        ));
        assert_fixtures_consumed(&expected);
    }
}
