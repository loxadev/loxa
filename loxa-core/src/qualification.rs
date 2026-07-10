use crate::provider::{
    ChatMessage, InvocationObservation, InvocationRequest, ProviderAdapter, ToolCall,
    ToolDefinition,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;

const PASS_REASON: &str = "structural requirements satisfied";

#[derive(Clone, Debug, PartialEq)]
pub struct QualificationCase {
    pub id: String,
    pub description: String,
    pub request: InvocationRequest,
    expectation: CaseExpectation,
}

#[derive(Clone, Debug, PartialEq)]
enum CaseExpectation {
    Tool(&'static str),
    NoTool,
    MultiTurnTicket,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QualificationResult {
    pub case_id: String,
    pub passed: bool,
    pub reason: String,
    pub elapsed_ns: u64,
    pub observations: Vec<InvocationObservation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QualificationReport {
    pub results: Vec<QualificationResult>,
}

impl QualificationReport {
    pub fn passed(&self) -> bool {
        self.results.iter().all(|result| result.passed)
    }
}

pub fn qualification_cases() -> Vec<QualificationCase> {
    vec![
        QualificationCase {
            id: "weather_required_city".into(),
            description: "Calls weather with its required city argument.".into(),
            request: InvocationRequest {
                messages: vec![ChatMessage::user("What is the weather in Paris?")],
                tools: vec![weather_definition()],
                max_tokens: 128,
            },
            expectation: CaseExpectation::Tool("weather"),
        },
        QualificationCase {
            id: "no_tool_needed".into(),
            description: "Answers directly when no tool is needed.".into(),
            request: InvocationRequest {
                messages: vec![ChatMessage::user("Reply with the word ready.")],
                tools: vec![weather_definition()],
                max_tokens: 32,
            },
            expectation: CaseExpectation::NoTool,
        },
        QualificationCase {
            id: "weather_optional_units".into(),
            description: "Accepts the optional weather units argument.".into(),
            request: InvocationRequest {
                messages: vec![ChatMessage::user(
                    "What is the weather in Tokyo in celsius?",
                )],
                tools: vec![weather_definition()],
                max_tokens: 128,
            },
            expectation: CaseExpectation::Tool("weather"),
        },
        QualificationCase {
            id: "weather_argument_types".into(),
            description: "Uses the required weather argument type.".into(),
            request: InvocationRequest {
                messages: vec![ChatMessage::user("What is the weather in Madrid?")],
                tools: vec![weather_definition()],
                max_tokens: 128,
            },
            expectation: CaseExpectation::Tool("weather"),
        },
        QualificationCase {
            id: "multi_turn_ticket_context".into(),
            description: "Consumes a simulated ticket result in a follow-up turn.".into(),
            request: InvocationRequest {
                messages: vec![ChatMessage::user("Look up ticket LOXA-42.")],
                tools: vec![lookup_ticket_definition()],
                max_tokens: 128,
            },
            expectation: CaseExpectation::MultiTurnTicket,
        },
    ]
}

pub fn qualify_provider(provider: &mut dyn ProviderAdapter) -> QualificationReport {
    QualificationReport {
        results: qualification_cases()
            .iter()
            .map(|case| evaluate_case(provider, case))
            .collect(),
    }
}

fn evaluate_case(
    provider: &mut dyn ProviderAdapter,
    case: &QualificationCase,
) -> QualificationResult {
    let started = Instant::now();
    let first = match provider.invoke(&case.request) {
        Ok(observation) => observation,
        Err(error) => {
            return result(
                case,
                false,
                format!("provider error: {error}"),
                started,
                vec![],
            )
        }
    };

    match case.expectation {
        CaseExpectation::Tool(name) => match validate_single_tool_call(&first, name) {
            Ok(()) => result(case, true, PASS_REASON.into(), started, vec![first]),
            Err(reason) => result(case, false, reason, started, vec![first]),
        },
        CaseExpectation::NoTool => match validate_no_tool_response(&first) {
            Ok(()) => result(case, true, PASS_REASON.into(), started, vec![first]),
            Err(reason) => result(case, false, reason, started, vec![first]),
        },
        CaseExpectation::MultiTurnTicket => {
            evaluate_multi_turn_ticket(provider, case, first, started)
        }
    }
}

fn evaluate_multi_turn_ticket(
    provider: &mut dyn ProviderAdapter,
    case: &QualificationCase,
    first: InvocationObservation,
    started: Instant,
) -> QualificationResult {
    if let Err(reason) = validate_single_tool_call(&first, "lookup_ticket") {
        return result(case, false, reason, started, vec![first]);
    }

    let tool_call = &first.tool_calls[0];
    let tool_result = json!({
        "ticket_id": "LOXA-42",
        "status": "resolved",
        "summary": "Selector qualification complete"
    });
    let mut messages = case.request.messages.clone();
    messages.push(ChatMessage {
        role: "assistant".into(),
        content: json!({
            "tool_calls": [{
                "name": tool_call.name,
                "arguments": tool_call.arguments
            }]
        })
        .to_string(),
    });
    messages.push(ChatMessage {
        role: "tool".into(),
        content: tool_result.to_string(),
    });
    messages.push(ChatMessage::user(
        "Give a concise summary of the ticket result.",
    ));

    let second_request = InvocationRequest {
        messages,
        tools: vec![],
        max_tokens: 128,
    };
    let second = match provider.invoke(&second_request) {
        Ok(observation) => observation,
        Err(error) => {
            return result(
                case,
                false,
                format!("provider error: {error}"),
                started,
                vec![first],
            )
        }
    };
    let observations = vec![first, second];

    match validate_no_tool_response(&observations[1]) {
        Ok(()) => result(case, true, PASS_REASON.into(), started, observations),
        Err(reason) => result(case, false, reason, started, observations),
    }
}

fn validate_single_tool_call(
    observation: &InvocationObservation,
    expected_name: &str,
) -> Result<(), String> {
    if observation.tool_calls.len() != 1 {
        return Err(format!(
            "expected exactly one `{expected_name}` tool call, observed {}",
            observation.tool_calls.len()
        ));
    }

    let call = &observation.tool_calls[0];
    if call.name != expected_name {
        return Err(format!(
            "expected `{expected_name}` tool call, observed `{}`",
            call.name
        ));
    }

    validate_tool_arguments(call)
}

fn validate_no_tool_response(observation: &InvocationObservation) -> Result<(), String> {
    if !observation.tool_calls.is_empty() {
        return Err(format!(
            "expected no tool calls, observed {}",
            observation.tool_calls.len()
        ));
    }
    if observation
        .content
        .as_deref()
        .is_none_or(|content| content.trim().is_empty())
    {
        return Err("expected a non-empty text response".into());
    }

    Ok(())
}

fn validate_tool_arguments(call: &ToolCall) -> Result<(), String> {
    let object = call
        .arguments
        .as_object()
        .ok_or_else(|| format!("{} arguments must be an object", call.name))?;

    match call.name.as_str() {
        "weather" => {
            reject_unknown_fields(object, "weather", &["city", "units"])?;
            required_string(object, "weather", "city")?;
            if let Some(units) = object.get("units") {
                let units = units
                    .as_str()
                    .ok_or_else(|| "weather argument `units` must be a string".to_string())?;
                if !matches!(units, "celsius" | "fahrenheit") {
                    return Err("weather argument `units` must be `celsius` or `fahrenheit`".into());
                }
            }
            Ok(())
        }
        "lookup_ticket" => {
            reject_unknown_fields(object, "lookup_ticket", &["ticket_id"])?;
            required_string(object, "lookup_ticket", "ticket_id")?;
            Ok(())
        }
        name => Err(format!("unknown tool `{name}`")),
    }
}

fn reject_unknown_fields(
    object: &serde_json::Map<String, Value>,
    tool: &str,
    allowed: &[&str],
) -> Result<(), String> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(format!("{tool} arguments contain unknown field `{field}`"));
    }
    Ok(())
}

fn required_string(
    object: &serde_json::Map<String, Value>,
    tool: &str,
    field: &str,
) -> Result<(), String> {
    match object.get(field) {
        None => Err(format!("{tool} arguments require `{field}`")),
        Some(Value::String(_)) => Ok(()),
        Some(_) => Err(format!("{tool} argument `{field}` must be a string")),
    }
}

fn result(
    case: &QualificationCase,
    passed: bool,
    reason: String,
    started: Instant,
    observations: Vec<InvocationObservation>,
) -> QualificationResult {
    QualificationResult {
        case_id: case.id.clone(),
        passed,
        reason,
        elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        observations,
    }
}

fn weather_definition() -> ToolDefinition {
    ToolDefinition {
        name: "weather".into(),
        description: "Get the weather for a city.".into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "city": {"type": "string"},
                "units": {
                    "type": "string",
                    "enum": ["celsius", "fahrenheit"]
                }
            },
            "required": ["city"],
            "additionalProperties": false
        }),
    }
}

fn lookup_ticket_definition() -> ToolDefinition {
    ToolDefinition {
        name: "lookup_ticket".into(),
        description: "Look up a support ticket.".into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticket_id": {"type": "string"}
            },
            "required": ["ticket_id"],
            "additionalProperties": false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_case, qualification_cases, qualify_provider};
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
    use crate::provider::{
        InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError, ToolCall,
    };
    use std::collections::VecDeque;

    struct ScriptedAdapter {
        identity: CandidateIdentity,
        responses: VecDeque<Result<InvocationObservation, ProviderError>>,
        requests: Vec<InvocationRequest>,
    }

    impl ScriptedAdapter {
        fn new(responses: Vec<Result<InvocationObservation, ProviderError>>) -> Self {
            Self {
                identity: CandidateIdentity {
                    candidate_id: "qualification-test".into(),
                    provider: ProviderKind::ManagedLlama,
                    provider_version: "test".into(),
                    engine_revision: Some("test".into()),
                    model_id: "test-model".into(),
                    artifact_digest: "sha256:test-artifact".into(),
                    tokenizer_digest: "sha256:test-tokenizer".into(),
                    chat_template_digest: "sha256:test-template".into(),
                    context_tokens: 8_192,
                    required_free_memory_bytes: 1,
                    sampling: SamplingPolicy {
                        temperature_milli: 0,
                        top_p_milli: 1_000,
                        seed: 1,
                    },
                },
                responses: responses.into(),
                requests: Vec::new(),
            }
        }
    }

    impl ProviderAdapter for ScriptedAdapter {
        fn identity(&self) -> &CandidateIdentity {
            &self.identity
        }

        fn inspect(&mut self) -> Result<(), ProviderError> {
            Ok(())
        }

        fn invoke(
            &mut self,
            request: &InvocationRequest,
        ) -> Result<InvocationObservation, ProviderError> {
            self.requests.push(request.clone());
            self.responses
                .pop_front()
                .expect("qualification made an unexpected invocation")
        }
    }

    fn observation(content: Option<&str>, tool_calls: Vec<ToolCall>) -> InvocationObservation {
        InvocationObservation {
            content: content.map(str::to_string),
            tool_calls,
            prompt_tokens: Some(10),
            completion_tokens: Some(4),
            ttft_ns: Some(100),
            total_duration_ns: 500,
            prompt_rate: Some(2.0),
            decode_rate: Some(3.0),
            raw_events: vec![],
        }
    }

    fn case(id: &str) -> super::QualificationCase {
        qualification_cases()
            .into_iter()
            .find(|case| case.id == id)
            .unwrap_or_else(|| panic!("missing qualification case {id}"))
    }

    #[test]
    fn qualification_requires_weather_tool_with_required_city() {
        let mut provider = ScriptedAdapter::new(vec![Ok(observation(
            None,
            vec![ToolCall {
                name: "weather".into(),
                arguments: serde_json::json!({"city": "Paris"}),
            }],
        ))]);
        let mut missing_city_provider = ScriptedAdapter::new(vec![Ok(observation(
            None,
            vec![ToolCall {
                name: "weather".into(),
                arguments: serde_json::json!({}),
            }],
        ))]);
        let required_city_case = case("weather_required_city");

        let result = evaluate_case(&mut provider, &required_city_case);
        let missing_city = evaluate_case(&mut missing_city_provider, &required_city_case);

        assert!(result.passed, "{}", result.reason);
        assert_eq!(result.observations[0].tool_calls[0].name, "weather");
        assert_eq!(
            result.observations[0].tool_calls[0].arguments["city"],
            "Paris"
        );
        assert!(!missing_city.passed);
        assert_eq!(missing_city.reason, "weather arguments require `city`");
    }

    #[test]
    fn qualification_accepts_a_no_tool_response_when_no_tool_is_needed() {
        let mut provider = ScriptedAdapter::new(vec![Ok(observation(Some("ready"), vec![]))]);

        let result = evaluate_case(&mut provider, &case("no_tool_needed"));

        assert!(result.passed, "{}", result.reason);
        assert!(result.observations[0].tool_calls.is_empty());
    }

    #[test]
    fn qualification_accepts_optional_units_but_rejects_unknown_arguments() {
        let valid = ToolCall {
            name: "weather".into(),
            arguments: serde_json::json!({"city": "Tokyo", "units": "celsius"}),
        };
        let invalid = ToolCall {
            name: "weather".into(),
            arguments: serde_json::json!({
                "city": "Tokyo",
                "units": "celsius",
                "country": "Japan"
            }),
        };
        let optional_units_case = case("weather_optional_units");
        let mut valid_provider = ScriptedAdapter::new(vec![Ok(observation(None, vec![valid]))]);
        let mut invalid_provider = ScriptedAdapter::new(vec![Ok(observation(None, vec![invalid]))]);

        let accepted = evaluate_case(&mut valid_provider, &optional_units_case);
        let rejected = evaluate_case(&mut invalid_provider, &optional_units_case);

        assert!(accepted.passed, "{}", accepted.reason);
        assert!(!rejected.passed);
        assert_eq!(
            rejected.reason,
            "weather arguments contain unknown field `country`"
        );
    }

    #[test]
    fn qualification_rejects_invalid_required_argument_type() {
        let mut provider = ScriptedAdapter::new(vec![Ok(observation(
            None,
            vec![ToolCall {
                name: "weather".into(),
                arguments: serde_json::json!({"city": 42}),
            }],
        ))]);

        let result = evaluate_case(&mut provider, &case("weather_argument_types"));

        assert!(!result.passed);
        assert_eq!(result.reason, "weather argument `city` must be a string");
    }

    #[test]
    fn qualification_preserves_multi_turn_tool_result_context() {
        let mut provider = ScriptedAdapter::new(vec![
            Ok(observation(
                None,
                vec![ToolCall {
                    name: "lookup_ticket".into(),
                    arguments: serde_json::json!({"ticket_id": "LOXA-42"}),
                }],
            )),
            Ok(observation(Some("LOXA-42 is resolved."), vec![])),
        ]);

        let result = evaluate_case(&mut provider, &case("multi_turn_ticket_context"));
        let expected_tool_result = serde_json::json!({
            "ticket_id": "LOXA-42",
            "status": "resolved",
            "summary": "Selector qualification complete"
        })
        .to_string();

        assert!(result.passed, "{}", result.reason);
        assert_eq!(result.observations.len(), 2);
        assert_eq!(provider.requests.len(), 2);
        assert!(provider.requests[1].messages.iter().any(|message| {
            message.role == "tool" && message.content.as_str() == expected_tool_result.as_str()
        }));
    }

    #[test]
    fn qualification_provider_error_fails_the_case_without_panicking() {
        let failures = (0..qualification_cases().len())
            .map(|_| Err(ProviderError::Timeout))
            .collect();
        let mut provider = ScriptedAdapter::new(failures);

        let report = qualify_provider(&mut provider);

        assert!(!report.passed());
        assert!(report.results.iter().all(|result| !result.passed));
        assert_eq!(
            report.results[0].reason,
            "provider error: provider invocation timed out"
        );
    }
}
