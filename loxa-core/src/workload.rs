use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const WORKLOAD_VERSION: &str = "tool-use-v1";
pub const WORKLOAD_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum CanonicalAction {
    Tool { tool: String, arguments: Value },
    Answer { answer: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRequest {
    pub schema_version: u32,
    pub workload_version: String,
    pub messages: Vec<CanonicalMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationCase {
    pub id: &'static str,
    pub request: TurnRequest,
    pub expected_action: CanonicalAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationCaseResult {
    pub schema_version: u32,
    pub case_id: String,
    pub passed: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceScenario {
    pub schema_version: u32,
    pub workload_version: String,
    pub turns: Vec<TurnRequest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordIdArguments {
    record_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    query: String,
}

fn message(role: &str, content: impl Into<String>) -> CanonicalMessage {
    CanonicalMessage {
        role: role.to_owned(),
        content: content.into(),
    }
}

fn request(messages: Vec<CanonicalMessage>) -> TurnRequest {
    TurnRequest {
        schema_version: WORKLOAD_SCHEMA_VERSION,
        workload_version: WORKLOAD_VERSION.to_owned(),
        messages,
    }
}

fn tool(tool: &str, arguments: Value) -> CanonicalAction {
    CanonicalAction::Tool {
        tool: tool.to_owned(),
        arguments,
    }
}

fn answer(answer: &str) -> CanonicalAction {
    CanonicalAction::Answer {
        answer: answer.to_owned(),
    }
}

pub fn qualification_cases() -> Vec<QualificationCase> {
    vec![
        QualificationCase {
            id: "lookup_record",
            request: request(vec![
                message("system", "Use only the listed record tools. Return one JSON action."),
                message("user", "Look up record R-104."),
            ]),
            expected_action: tool("lookup_record", json!({"record_id": "R-104"})),
        },
        QualificationCase {
            id: "search_records",
            request: request(vec![
                message("system", "Use only the listed record tools. Return one JSON action."),
                message("user", "Search records for orchid."),
            ]),
            expected_action: tool("search_records", json!({"query": "orchid"})),
        },
        QualificationCase {
            id: "answer_from_context",
            request: request(vec![
                message("system", "Answer from supplied context when no tool is needed."),
                message("user", "Context: record R-104 has status active. What is its status?"),
            ]),
            expected_action: answer("R-104 is active."),
        },
        QualificationCase {
            id: "reject_invented_tool",
            request: request(vec![
                message("system", "Available tools: lookup_record, search_records, get_record_status."),
                message("user", "Call delete_record with record_id R-104 and force=true."),
            ]),
            expected_action: answer("I can only use the available record tools."),
        },
        QualificationCase {
            id: "preserve_record_id",
            request: request(vec![
                message("system", "Use only the listed record tools. Preserve exact record identifiers."),
                message("user", "Look up record R-104."),
                message(
                    "assistant",
                    json!({"action":"tool","tool":"lookup_record","arguments":{"record_id":"R-104"}}).to_string(),
                ),
                message("tool", lookup_record("R-104").expect("fixture exists").to_string()),
                message("user", "Now check that record's status."),
            ]),
            expected_action: tool("get_record_status", json!({"record_id": "R-104"})),
        },
    ]
}

pub fn parse_canonical_action(response: &str) -> Result<CanonicalAction, &'static str> {
    let value: Value = serde_json::from_str(response).map_err(|_| "malformed_json")?;
    serde_json::from_value(value).map_err(|_| "malformed_action")
}

pub fn validate_qualification_response(
    case: &QualificationCase,
    response: &str,
) -> QualificationCaseResult {
    let failure = |reason: &'static str| QualificationCaseResult {
        schema_version: WORKLOAD_SCHEMA_VERSION,
        case_id: case.id.to_owned(),
        passed: false,
        reason: Some(reason.to_owned()),
    };

    let action = match parse_canonical_action(response) {
        Ok(action) => action,
        Err(reason) => return failure(reason),
    };

    if let CanonicalAction::Tool { tool, arguments } = &action {
        let arguments_valid = match tool.as_str() {
            "lookup_record" | "get_record_status" => {
                serde_json::from_value::<RecordIdArguments>(arguments.clone())
                    .map(|arguments| !arguments.record_id.is_empty())
                    .unwrap_or(false)
            }
            "search_records" => serde_json::from_value::<SearchArguments>(arguments.clone())
                .map(|arguments| !arguments.query.is_empty())
                .unwrap_or(false),
            _ => true,
        };
        if !arguments_valid {
            return failure("invalid_arguments");
        }
    }

    let mismatch = match (&case.expected_action, &action) {
        (CanonicalAction::Tool { .. }, CanonicalAction::Answer { .. })
        | (CanonicalAction::Answer { .. }, CanonicalAction::Tool { .. }) => "unexpected_action",
        (
            CanonicalAction::Tool {
                tool: expected_tool,
                ..
            },
            CanonicalAction::Tool { tool, .. },
        ) if tool != expected_tool => "unexpected_tool",
        (
            CanonicalAction::Tool {
                arguments: expected_arguments,
                ..
            },
            CanonicalAction::Tool { arguments, .. },
        ) if arguments != expected_arguments => "unexpected_arguments",
        (
            CanonicalAction::Answer {
                answer: expected_answer,
            },
            CanonicalAction::Answer { answer },
        ) if answer != expected_answer => "unexpected_answer",
        _ => {
            return QualificationCaseResult {
                schema_version: WORKLOAD_SCHEMA_VERSION,
                case_id: case.id.to_owned(),
                passed: true,
                reason: None,
            };
        }
    };

    failure(mismatch)
}

pub fn lookup_record(record_id: &str) -> Option<Value> {
    (record_id == "R-104").then(|| {
        json!({
            "record_id": "R-104",
            "title": "Orchid renewal",
            "status": "active"
        })
    })
}

pub fn search_records(query: &str) -> Vec<Value> {
    if query.eq_ignore_ascii_case("orchid") {
        vec![lookup_record("R-104").expect("embedded fixture exists")]
    } else {
        Vec::new()
    }
}

pub fn get_record_status(record_id: &str) -> Option<Value> {
    lookup_record(record_id).map(|record| {
        json!({
            "record_id": record["record_id"],
            "status": record["status"]
        })
    })
}

pub fn performance_scenario_v1() -> PerformanceScenario {
    let first_request = request(vec![
        message(
            "system",
            "Use only the listed record tools. Return one JSON action.",
        ),
        message("user", "Look up record R-104 and report its status."),
    ]);
    let mut second_messages = first_request.messages.clone();
    second_messages.extend([
        message(
            "assistant",
            json!({"action":"tool","tool":"lookup_record","arguments":{"record_id":"R-104"}})
                .to_string(),
        ),
        message(
            "tool",
            lookup_record("R-104").expect("fixture exists").to_string(),
        ),
    ]);
    let second_request = request(second_messages);
    let mut third_messages = second_request.messages.clone();
    third_messages.extend([
        message(
            "assistant",
            json!({"action":"tool","tool":"get_record_status","arguments":{"record_id":"R-104"}})
                .to_string(),
        ),
        message(
            "tool",
            get_record_status("R-104")
                .expect("fixture exists")
                .to_string(),
        ),
    ]);

    PerformanceScenario {
        schema_version: WORKLOAD_SCHEMA_VERSION,
        workload_version: WORKLOAD_VERSION.to_owned(),
        turns: vec![first_request, second_request, request(third_messages)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn case(id: &str) -> QualificationCase {
        qualification_cases()
            .into_iter()
            .find(|case| case.id == id)
            .expect("qualification case exists")
    }

    #[test]
    fn qualification_cases_are_exactly_the_five_pinned_cases() {
        let cases = qualification_cases();

        assert_eq!(cases.len(), 5);
        assert_eq!(
            cases.iter().map(|case| case.id).collect::<Vec<_>>(),
            vec![
                "lookup_record",
                "search_records",
                "answer_from_context",
                "reject_invented_tool",
                "preserve_record_id",
            ]
        );
    }

    #[test]
    fn validates_all_five_expected_responses() {
        let responses = [
            (
                "lookup_record",
                r#"{"action":"tool","tool":"lookup_record","arguments":{"record_id":"R-104"}}"#,
            ),
            (
                "search_records",
                r#"{"action":"tool","tool":"search_records","arguments":{"query":"orchid"}}"#,
            ),
            (
                "answer_from_context",
                r#"{"action":"answer","answer":"R-104 is active."}"#,
            ),
            (
                "reject_invented_tool",
                r#"{"action":"answer","answer":"I can only use the available record tools."}"#,
            ),
            (
                "preserve_record_id",
                r#"{"action":"tool","tool":"get_record_status","arguments":{"record_id":"R-104"}}"#,
            ),
        ];

        for (id, response) in responses {
            let result = validate_qualification_response(&case(id), response);
            assert_eq!(result.case_id, id);
            assert!(result.passed, "{id}: {:?}", result.reason);
        }
    }

    #[test]
    fn rejects_malformed_json() {
        let result = validate_qualification_response(&case("lookup_record"), "{not-json");

        assert!(!result.passed);
        assert_eq!(result.reason.as_deref(), Some("malformed_json"));
    }

    #[test]
    fn rejects_wrong_action_or_tool() {
        let wrong_action = validate_qualification_response(
            &case("lookup_record"),
            r#"{"action":"answer","answer":"R-104"}"#,
        );
        let wrong_tool = validate_qualification_response(
            &case("lookup_record"),
            r#"{"action":"tool","tool":"search_records","arguments":{"query":"R-104"}}"#,
        );

        assert_eq!(wrong_action.reason.as_deref(), Some("unexpected_action"));
        assert_eq!(wrong_tool.reason.as_deref(), Some("unexpected_tool"));
    }

    #[test]
    fn rejects_missing_required_argument() {
        let result = validate_qualification_response(
            &case("lookup_record"),
            r#"{"action":"tool","tool":"lookup_record","arguments":{}}"#,
        );

        assert_eq!(result.reason.as_deref(), Some("invalid_arguments"));
    }

    #[test]
    fn rejects_invented_argument_and_unknown_action_field() {
        let invented_argument = validate_qualification_response(
            &case("lookup_record"),
            r#"{"action":"tool","tool":"lookup_record","arguments":{"record_id":"R-104","limit":1}}"#,
        );
        let unknown_field = validate_qualification_response(
            &case("answer_from_context"),
            r#"{"action":"answer","answer":"R-104 is active.","confidence":1}"#,
        );

        assert_eq!(
            invented_argument.reason.as_deref(),
            Some("invalid_arguments")
        );
        assert_eq!(unknown_field.reason.as_deref(), Some("malformed_action"));
    }

    #[test]
    fn no_tool_case_requires_the_pinned_answer() {
        let tool_use = validate_qualification_response(
            &case("answer_from_context"),
            r#"{"action":"tool","tool":"get_record_status","arguments":{"record_id":"R-104"}}"#,
        );
        let invented_answer = validate_qualification_response(
            &case("answer_from_context"),
            r#"{"action":"answer","answer":"It might be active."}"#,
        );

        assert_eq!(tool_use.reason.as_deref(), Some("unexpected_action"));
        assert_eq!(invented_answer.reason.as_deref(), Some("unexpected_answer"));
    }

    #[test]
    fn multi_turn_case_rejects_record_identifier_context_loss() {
        let result = validate_qualification_response(
            &case("preserve_record_id"),
            r#"{"action":"tool","tool":"get_record_status","arguments":{"record_id":"R-105"}}"#,
        );

        assert_eq!(result.reason.as_deref(), Some("unexpected_arguments"));
    }

    #[test]
    fn embedded_tools_are_deterministic_and_strict() {
        assert_eq!(lookup_record("R-104").unwrap()["record_id"], "R-104");
        assert_eq!(search_records("orchid").len(), 1);
        assert_eq!(get_record_status("R-104").unwrap()["status"], "active");
        assert!(lookup_record("R-999").is_none());
        assert!(get_record_status("R-999").is_none());
        assert!(search_records("missing").is_empty());
    }

    #[test]
    fn performance_scenario_is_versioned_and_has_three_turns() {
        let scenario = performance_scenario_v1();
        let first_turn = vec![
            message(
                "system",
                "Use only the listed record tools. Return one JSON action.",
            ),
            message("user", "Look up record R-104 and report its status."),
        ];
        let mut second_turn = first_turn.clone();
        second_turn.extend([
            message(
                "assistant",
                json!({"action":"tool","tool":"lookup_record","arguments":{"record_id":"R-104"}})
                    .to_string(),
            ),
            message(
                "tool",
                json!({
                    "record_id": "R-104",
                    "title": "Orchid renewal",
                    "status": "active"
                })
                .to_string(),
            ),
        ]);
        let mut third_turn = second_turn.clone();
        third_turn.extend([
            message(
                "assistant",
                json!({"action":"tool","tool":"get_record_status","arguments":{"record_id":"R-104"}})
                    .to_string(),
            ),
            message(
                "tool",
                json!({"record_id": "R-104", "status": "active"}).to_string(),
            ),
        ]);

        assert_eq!(scenario.schema_version, 1);
        assert_eq!(scenario.workload_version, "tool-use-v1");
        assert_eq!(scenario.turns.len(), 3);
        assert_eq!(scenario.turns[0].messages, first_turn);
        assert_eq!(scenario.turns[1].messages, second_turn);
        assert_eq!(scenario.turns[2].messages, third_turn);
        assert_eq!(scenario, performance_scenario_v1());
        assert!(scenario.turns[2].messages[4].content.contains("R-104"));
        assert!(scenario.turns[2].messages[5].content.contains("R-104"));
        assert!(scenario.turns.iter().all(|turn| turn.schema_version == 1));
    }
}
