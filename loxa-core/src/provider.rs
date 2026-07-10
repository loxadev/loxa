use crate::plan::CandidateIdentity;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_string(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

impl ToolDefinition {
    pub fn weather() -> Self {
        Self {
            name: "weather".to_string(),
            description: "Get the weather for a city.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                },
                "required": ["city"]
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationRequest {
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolDefinition>,
    pub max_tokens: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InvocationObservation {
    pub content: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub ttft_ns: Option<u64>,
    pub total_duration_ns: u64,
    pub prompt_rate: Option<f64>,
    pub decode_rate: Option<f64>,
    pub raw_events: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderError {
    Unavailable,
    Identity(String),
    Protocol(String),
    Timeout,
    Io(String),
}

impl Display for ProviderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("provider unavailable"),
            Self::Identity(message) => write!(formatter, "provider identity error: {message}"),
            Self::Protocol(message) => write!(formatter, "provider protocol error: {message}"),
            Self::Timeout => formatter.write_str("provider invocation timed out"),
            Self::Io(message) => write!(formatter, "provider I/O error: {message}"),
        }
    }
}

impl Error for ProviderError {}

pub trait ProviderAdapter {
    fn identity(&self) -> &CandidateIdentity;
    fn inspect(&mut self) -> Result<(), ProviderError>;
    fn invoke(
        &mut self,
        request: &InvocationRequest,
    ) -> Result<InvocationObservation, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::{
        ChatMessage, InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError,
        ToolCall, ToolDefinition,
    };
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};

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

    #[test]
    fn normalized_invocation_represents_tool_calls_and_measurements() {
        let request = InvocationRequest {
            messages: vec![ChatMessage::user("What is the weather in Paris?")],
            tools: vec![ToolDefinition::weather()],
            max_tokens: 128,
        };
        let observation = InvocationObservation {
            content: None,
            tool_calls: vec![ToolCall {
                name: "weather".into(),
                arguments: serde_json::json!({"city":"Paris"}),
            }],
            prompt_tokens: Some(20),
            completion_tokens: Some(8),
            ttft_ns: Some(250_000),
            total_duration_ns: 1_000_000,
            prompt_rate: None,
            decode_rate: None,
            raw_events: vec![serde_json::json!({"tool_call":"weather"})],
        };

        assert_eq!(
            request.messages[0],
            ChatMessage::user("What is the weather in Paris?")
        );
        assert_eq!(request.tools[0].name, "weather");
        assert_eq!(observation.tool_calls[0].name, "weather");
        assert_eq!(observation.ttft_ns, Some(250_000));
        assert_eq!(observation.raw_events.len(), 1);
    }

    #[test]
    fn provider_errors_are_distinct_error_categories() {
        fn require_error<T: std::error::Error>() {}

        require_error::<ProviderError>();
        assert!(matches!(
            ProviderError::Unavailable,
            ProviderError::Unavailable
        ));
        assert!(matches!(
            ProviderError::Identity("missing digest".into()),
            ProviderError::Identity(message) if message == "missing digest"
        ));
        assert!(matches!(
            ProviderError::Protocol("invalid response".into()),
            ProviderError::Protocol(message) if message == "invalid response"
        ));
        assert!(matches!(ProviderError::Timeout, ProviderError::Timeout));
        assert!(matches!(
            ProviderError::Io("connection reset".into()),
            ProviderError::Io(message) if message == "connection reset"
        ));
    }

    #[test]
    fn provider_adapter_is_usable_as_a_trait_object() {
        struct StubAdapter {
            identity: CandidateIdentity,
        }

        impl ProviderAdapter for StubAdapter {
            fn identity(&self) -> &CandidateIdentity {
                &self.identity
            }

            fn inspect(&mut self) -> Result<(), ProviderError> {
                Ok(())
            }

            fn invoke(
                &mut self,
                _request: &InvocationRequest,
            ) -> Result<InvocationObservation, ProviderError> {
                Err(ProviderError::Unavailable)
            }
        }

        let mut adapter: Box<dyn ProviderAdapter> = Box::new(StubAdapter {
            identity: identity(),
        });

        assert_eq!(adapter.identity().candidate_id, "managed-gemma3-4b");
        assert_eq!(adapter.inspect(), Ok(()));
        assert!(matches!(
            adapter.invoke(&InvocationRequest {
                messages: vec![],
                tools: vec![],
                max_tokens: 1,
            }),
            Err(ProviderError::Unavailable)
        ));
    }
}
