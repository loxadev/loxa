use super::transport::{JsonTransport, ReqwestJsonTransport, StreamFraming, TimedJsonEvent};
use super::{InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError, ToolCall};
use crate::plan::CandidateIdentity;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub struct LlamaAdapter {
    identity: CandidateIdentity,
    base_url: String,
    expected_alias: String,
    transport: Box<dyn JsonTransport>,
}

impl LlamaAdapter {
    pub fn new(
        identity: CandidateIdentity,
        base_url: impl Into<String>,
        expected_alias: impl Into<String>,
    ) -> Self {
        Self::with_transport(
            identity,
            base_url,
            expected_alias,
            Box::new(ReqwestJsonTransport::new()),
        )
    }

    pub(crate) fn with_transport(
        identity: CandidateIdentity,
        base_url: impl Into<String>,
        expected_alias: impl Into<String>,
        transport: Box<dyn JsonTransport>,
    ) -> Self {
        Self {
            identity,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            expected_alias: expected_alias.into(),
            transport,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

impl ProviderAdapter for LlamaAdapter {
    fn identity(&self) -> &CandidateIdentity {
        &self.identity
    }

    fn inspect(&mut self) -> Result<(), ProviderError> {
        let response = self.transport.get_json(&self.endpoint("/v1/models"))?;
        let models = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ProviderError::Protocol("llama model inventory is missing data array".into())
            })?;
        let exact_match = models.len() == 1
            && models[0].get("id").and_then(Value::as_str) == Some(&self.expected_alias);
        if !exact_match {
            return Err(ProviderError::Protocol(format!(
                "llama model inventory must contain only exact alias {}",
                self.expected_alias
            )));
        }
        Ok(())
    }

    fn invoke(
        &mut self,
        request: &InvocationRequest,
    ) -> Result<InvocationObservation, ProviderError> {
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
            "model": self.expected_alias,
            "messages": messages,
            "tools": tools,
            "max_tokens": request.max_tokens,
            "temperature": f64::from(self.identity.sampling.temperature_milli) / 1000.0,
            "top_p": f64::from(self.identity.sampling.top_p_milli) / 1000.0,
            "seed": self.identity.sampling.seed,
            "stream": true,
            "stream_options": {"include_usage": true}
        });
        let events = self.transport.post_json_stream(
            &self.endpoint("/v1/chat/completions"),
            &body,
            StreamFraming::SseData,
        )?;

        normalize_events(events)
    }
}

#[derive(Default)]
struct ToolCallFragments {
    name: String,
    arguments: String,
}

fn normalize_events(events: Vec<TimedJsonEvent>) -> Result<InvocationObservation, ProviderError> {
    let mut content = String::new();
    let mut tool_fragments = BTreeMap::<u64, ToolCallFragments>::new();
    let mut prompt_tokens = None;
    let mut completion_tokens = None;
    let mut prompt_rate = None;
    let mut decode_rate = None;
    let mut ttft_ns = None;

    for event in &events {
        if let Some(delta) = event
            .value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("delta"))
        {
            if let Some(fragment) = delta.get("content").and_then(Value::as_str) {
                if !fragment.is_empty() {
                    ttft_ns.get_or_insert(event.elapsed_ns);
                    content.push_str(fragment);
                }
            }
            if let Some(calls) = delta.get("tool_calls") {
                let calls = calls.as_array().ok_or_else(|| {
                    ProviderError::Protocol("llama tool_calls delta is not an array".into())
                })?;
                for call in calls {
                    let index = call.get("index").and_then(Value::as_u64).ok_or_else(|| {
                        ProviderError::Protocol("llama tool-call delta is missing index".into())
                    })?;
                    let function = call.get("function").ok_or_else(|| {
                        ProviderError::Protocol("llama tool-call delta is missing function".into())
                    })?;
                    let name = function.get("name").and_then(Value::as_str).unwrap_or("");
                    let arguments = function
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !name.is_empty() || !arguments.is_empty() {
                        ttft_ns.get_or_insert(event.elapsed_ns);
                    }
                    let fragments = tool_fragments.entry(index).or_default();
                    fragments.name.push_str(name);
                    fragments.arguments.push_str(arguments);
                }
            }
        }

        if let Some(usage) = event.value.get("usage") {
            prompt_tokens = usage.get("prompt_tokens").and_then(Value::as_u64);
            completion_tokens = usage.get("completion_tokens").and_then(Value::as_u64);
        }
        if let Some(timings) = event.value.get("timings") {
            prompt_rate = rate_per_second(
                timings.get("prompt_n").and_then(Value::as_f64),
                timings.get("prompt_ms").and_then(Value::as_f64),
            );
            decode_rate = rate_per_second(
                timings.get("predicted_n").and_then(Value::as_f64),
                timings.get("predicted_ms").and_then(Value::as_f64),
            );
        }
    }

    let tool_calls = tool_fragments
        .into_values()
        .map(|fragments| {
            if fragments.name.is_empty() {
                return Err(ProviderError::Protocol(
                    "llama tool call has no function name".into(),
                ));
            }
            let arguments = serde_json::from_str(&fragments.arguments).map_err(|error| {
                ProviderError::Protocol(format!(
                    "llama tool-call arguments are not complete JSON: {error}"
                ))
            })?;
            Ok(ToolCall {
                name: fragments.name,
                arguments,
            })
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    let total_duration_ns = events.last().map_or(0, |event| event.elapsed_ns);
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

fn rate_per_second(tokens: Option<f64>, milliseconds: Option<f64>) -> Option<f64> {
    match (tokens, milliseconds) {
        (Some(tokens), Some(milliseconds)) if milliseconds > 0.0 => {
            Some(tokens * 1000.0 / milliseconds)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::LlamaAdapter;
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
    use crate::provider::transport::{
        parse_sse_stream, validate_success_status, JsonTransport, StreamFraming, TimedJsonEvent,
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

        fn post_json(&mut self, url: &str, _body: &Value) -> Result<Value, ProviderError> {
            panic!("unexpected POST {url}")
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
            candidate_id: "managed-gemma3-4b".into(),
            provider: ProviderKind::ManagedLlama,
            provider_version: "9910-f5525f7e7".into(),
            engine_revision: Some("f5525f7e7".into()),
            model_id: "gemma-3-4b-it-q4".into(),
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
    fn llama_inspection_requires_exact_single_model_alias() {
        for (models, should_pass) in [
            (json!({"data": [{"id": "loxa-run-123-g1"}]}), true),
            (json!({"data": []}), false),
            (
                json!({"data": [{"id": "loxa-run-123-g1"}, {"id": "other"}]}),
                false,
            ),
            (json!({"data": [{"id": "loxa-run-123-g10"}]}), false),
        ] {
            let (transport, fixtures) = fake_transport(vec![ExpectedRequest::Get {
                url: "http://127.0.0.1:8080/v1/models".into(),
                response: Ok(models),
            }]);
            let mut adapter = LlamaAdapter::with_transport(
                identity(),
                "http://127.0.0.1:8080",
                "loxa-run-123-g1",
                transport,
            );

            let result = adapter.inspect();

            assert_eq!(result.is_ok(), should_pass);
            if !should_pass {
                assert!(matches!(result, Err(ProviderError::Protocol(_))));
            }
            assert_fixtures_consumed(&fixtures);
        }
    }

    #[test]
    fn llama_chat_translation_normalizes_fragmented_tool_calls_and_timings() {
        let body = json!({
            "model": "loxa-run-123-g1",
            "messages": [{"role": "user", "content": "What is the weather in Paris?"}],
            "tools": [tool_fixture()],
            "max_tokens": 128,
            "temperature": 0.0,
            "top_p": 1.0,
            "seed": 1,
            "stream": true,
            "stream_options": {"include_usage": true}
        });
        let events = vec![
            TimedJsonEvent::new(10, json!({"choices": [{"delta": {"role": "assistant"}}]})),
            TimedJsonEvent::new(15, json!({"choices": [{"delta": {"content": "Let "}}]})),
            TimedJsonEvent::new(
                18,
                json!({"choices": [{"delta": {"content": "me check."}}]}),
            ),
            TimedJsonEvent::new(
                20,
                json!({"choices": [{"delta": {"tool_calls": [
                    {"index": 1, "function": {"name": "wea", "arguments": "{\"city\":\"Par"}},
                    {"index": 0, "function": {"name": "wea", "arguments": "{\"city\":\"Ro"}}
                ]}}]}),
            ),
            TimedJsonEvent::new(
                30,
                json!({"choices": [{"delta": {"tool_calls": [
                    {"index": 0, "function": {"name": "ther", "arguments": "me\"}"}},
                    {"index": 1, "function": {"name": "ther", "arguments": "is\"}"}}
                ]}}]}),
            ),
            TimedJsonEvent::new(
                40,
                json!({
                    "choices": [],
                    "usage": {"prompt_tokens": 20, "completion_tokens": 8},
                    "timings": {"prompt_n": 20, "prompt_ms": 40.0, "predicted_n": 8, "predicted_ms": 80.0}
                }),
            ),
        ];
        let raw_events = events
            .iter()
            .map(|event| event.value.clone())
            .collect::<Vec<_>>();
        let (transport, fixtures) = fake_transport(vec![ExpectedRequest::Stream {
            url: "http://127.0.0.1:8080/v1/chat/completions".into(),
            body,
            framing: StreamFraming::SseData,
            response: Ok(events),
        }]);
        let mut adapter = LlamaAdapter::with_transport(
            identity(),
            "http://127.0.0.1:8080/",
            "loxa-run-123-g1",
            transport,
        );

        let observation = adapter.invoke(&weather_request()).expect("llama invoke");

        assert_eq!(observation.content, Some("Let me check.".into()));
        assert_eq!(
            observation.tool_calls,
            vec![
                ToolCall {
                    name: "weather".into(),
                    arguments: json!({"city": "Rome"}),
                },
                ToolCall {
                    name: "weather".into(),
                    arguments: json!({"city": "Paris"}),
                }
            ]
        );
        assert_eq!(observation.prompt_tokens, Some(20));
        assert_eq!(observation.completion_tokens, Some(8));
        assert_eq!(observation.ttft_ns, Some(15));
        assert_eq!(observation.total_duration_ns, 40);
        assert_eq!(observation.prompt_rate, Some(500.0));
        assert_eq!(observation.decode_rate, Some(100.0));
        assert_eq!(observation.raw_events, raw_events);
        assert_fixtures_consumed(&fixtures);
    }

    #[test]
    fn llama_stream_rejects_redirect_malformed_json_and_missing_done() {
        assert!(matches!(
            validate_success_status(StatusCode::TEMPORARY_REDIRECT),
            Err(ProviderError::Protocol(message)) if message.contains("redirect")
        ));

        let malformed = FragmentedReader::new(b"data: {not-json}\n\ndata: [DONE]\n\n".to_vec(), 2);
        assert!(matches!(
            parse_sse_stream(BufReader::new(malformed)),
            Err(ProviderError::Protocol(message)) if message.contains("malformed JSON")
        ));

        let missing_done = FragmentedReader::new(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n".to_vec(),
            3,
        );
        assert!(matches!(
            parse_sse_stream(BufReader::new(missing_done)),
            Err(ProviderError::Protocol(message)) if message.contains("[DONE]")
        ));

        let complete = FragmentedReader::new(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n".to_vec(),
            1,
        );
        let events = parse_sse_stream(BufReader::new(complete)).expect("fragmented SSE");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].value["choices"][0]["delta"]["content"], "ok");
    }
}
