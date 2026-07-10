use crate::plan::CandidateIdentity;
use crate::provider::{
    ChatMessage, InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError,
    ToolCall, ToolDefinition,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Instant;

pub const CALIBRATION_EVIDENCE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateOwnership {
    Managed,
    Attached,
}

pub trait CalibrationCandidate: ProviderAdapter {
    fn ownership(&self) -> CandidateOwnership;
    fn prepare(&mut self) -> Result<(), ProviderError>;
    fn finish(&mut self) -> Result<(), ProviderError>;
    fn isolation_check(&mut self) -> Result<(), ProviderError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CalibrationMeasurement {
    pub candidate_id: String,
    pub wall_time_ns: u128,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub ttft_ns: Option<u64>,
    pub prompt_rate: Option<f64>,
    pub decode_rate: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateEvidence {
    pub identity: CandidateIdentity,
    pub ownership: CandidateOwnership,
    pub qualified: bool,
    pub available_memory_before_bytes: u64,
    pub failure: Option<String>,
    pub warmup: Option<CalibrationMeasurement>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PairedObservation {
    pub pair_index: u8,
    pub managed: CalibrationMeasurement,
    pub attached: CalibrationMeasurement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CalibrationEvidence {
    pub schema_version: u32,
    pub managed: CandidateEvidence,
    pub attached: CandidateEvidence,
    pub pairs: Vec<PairedObservation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CalibrationOutcome {
    Completed {
        evidence: CalibrationEvidence,
    },
    Uncontrolled {
        reason: String,
    },
    Failed {
        evidence: CalibrationEvidence,
        reason: String,
    },
}

pub fn run_calibration<F>(
    managed: &mut dyn CalibrationCandidate,
    attached: &mut dyn CalibrationCandidate,
    mut available_memory: F,
) -> CalibrationOutcome
where
    F: FnMut() -> u64,
{
    if managed.ownership() != CandidateOwnership::Managed
        || attached.ownership() != CandidateOwnership::Attached
    {
        return CalibrationOutcome::Uncontrolled {
            reason: "calibration requires one managed and one attached candidate".into(),
        };
    }

    if let Err(error) = attached.isolation_check() {
        return CalibrationOutcome::Uncontrolled {
            reason: error.to_string(),
        };
    }
    if let Err(error) = managed.isolation_check() {
        return CalibrationOutcome::Uncontrolled {
            reason: error.to_string(),
        };
    }

    let managed_memory = available_memory();
    let attached_memory = available_memory();
    let mut evidence = CalibrationEvidence {
        schema_version: CALIBRATION_EVIDENCE_SCHEMA_VERSION,
        managed: candidate_evidence(managed, managed_memory),
        attached: candidate_evidence(attached, attached_memory),
        pairs: vec![],
    };

    if let Err(error) = managed.prepare().and_then(|_| managed.inspect()) {
        evidence.managed.failure = Some(error.to_string());
        return failed_after_managed_cleanup(managed, evidence, error.to_string());
    }
    if let Err(error) = attached.prepare().and_then(|_| attached.inspect()) {
        evidence.attached.failure = Some(error.to_string());
        return failed_after_managed_cleanup(managed, evidence, error.to_string());
    }

    let result = calibrate_prepared(managed, attached, &mut evidence);
    if let Err((ownership, error)) = &result {
        let candidate = match ownership {
            CandidateOwnership::Managed => &mut evidence.managed,
            CandidateOwnership::Attached => &mut evidence.attached,
        };
        candidate.failure = Some(error.to_string());
    }
    let cleanup = managed.finish();

    match (result, cleanup) {
        (Ok(()), Ok(())) => CalibrationOutcome::Completed { evidence },
        (Err((_, error)), Ok(())) => CalibrationOutcome::Failed {
            evidence,
            reason: error.to_string(),
        },
        (Ok(()), Err(error)) => CalibrationOutcome::Failed {
            evidence,
            reason: format!("managed cleanup failed: {error}"),
        },
        (Err((_, error)), Err(cleanup)) => CalibrationOutcome::Failed {
            evidence,
            reason: format!("{error}; managed cleanup failed: {cleanup}"),
        },
    }
}

fn candidate_evidence(
    candidate: &dyn CalibrationCandidate,
    available_memory_before_bytes: u64,
) -> CandidateEvidence {
    CandidateEvidence {
        identity: candidate.identity().clone(),
        ownership: candidate.ownership(),
        qualified: true,
        available_memory_before_bytes,
        failure: None,
        warmup: None,
    }
}

fn failed_after_managed_cleanup(
    managed: &mut dyn CalibrationCandidate,
    evidence: CalibrationEvidence,
    reason: String,
) -> CalibrationOutcome {
    match managed.finish() {
        Ok(()) => CalibrationOutcome::Failed { evidence, reason },
        Err(error) => CalibrationOutcome::Failed {
            evidence,
            reason: format!("{reason}; managed cleanup failed: {error}"),
        },
    }
}

fn calibrate_prepared(
    managed: &mut dyn CalibrationCandidate,
    attached: &mut dyn CalibrationCandidate,
    evidence: &mut CalibrationEvidence,
) -> Result<(), (CandidateOwnership, ProviderError)> {
    evidence.managed.warmup =
        Some(measure(managed).map_err(|error| (CandidateOwnership::Managed, error))?);
    evidence.attached.warmup =
        Some(measure(attached).map_err(|error| (CandidateOwnership::Attached, error))?);

    for pair_index in 0..5 {
        let (managed_observation, attached_observation) = if pair_index % 2 == 0 {
            (
                measure(managed).map_err(|error| (CandidateOwnership::Managed, error))?,
                measure(attached).map_err(|error| (CandidateOwnership::Attached, error))?,
            )
        } else {
            let attached_observation =
                measure(attached).map_err(|error| (CandidateOwnership::Attached, error))?;
            let managed_observation =
                measure(managed).map_err(|error| (CandidateOwnership::Managed, error))?;
            (managed_observation, attached_observation)
        };
        evidence.pairs.push(PairedObservation {
            pair_index,
            managed: managed_observation,
            attached: attached_observation,
        });
    }
    Ok(())
}

fn measure(
    candidate: &mut dyn CalibrationCandidate,
) -> Result<CalibrationMeasurement, ProviderError> {
    let started = Instant::now();
    let first_request = InvocationRequest {
        messages: vec![ChatMessage::user("Look up ticket TICKET-42.")],
        tools: vec![lookup_ticket_definition()],
        max_tokens: 128,
    };
    let first = candidate.invoke(&first_request)?;
    let call = exact_ticket_call(&first)?;
    let call_id = call.id.clone().unwrap_or_default();
    let mut messages = first_request.messages;
    messages.push(ChatMessage::assistant_tool_calls(vec![call.clone()]));
    messages.push(ChatMessage::tool_result(
        call_id,
        call.name,
        json!({"ticket_id":"TICKET-42","status":"resolved"}).to_string(),
    ));
    messages.push(ChatMessage::user("Give a concise final answer."));
    let second = candidate.invoke(&InvocationRequest {
        messages,
        tools: vec![],
        max_tokens: 128,
    })?;
    if second.content.as_deref().is_none_or(str::is_empty) || !second.tool_calls.is_empty() {
        return Err(ProviderError::Protocol(
            "calibration final response must be non-empty and contain no tool calls".into(),
        ));
    }

    Ok(CalibrationMeasurement {
        candidate_id: candidate.identity().candidate_id.clone(),
        wall_time_ns: started.elapsed().as_nanos(),
        prompt_tokens: sum_options(first.prompt_tokens, second.prompt_tokens),
        completion_tokens: sum_options(first.completion_tokens, second.completion_tokens),
        ttft_ns: first.ttft_ns,
        prompt_rate: second.prompt_rate.or(first.prompt_rate),
        decode_rate: second.decode_rate.or(first.decode_rate),
    })
}

fn sum_options(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn exact_ticket_call(observation: &InvocationObservation) -> Result<ToolCall, ProviderError> {
    if observation.tool_calls.len() != 1 {
        return Err(ProviderError::Protocol(
            "calibration requires exactly one lookup_ticket call".into(),
        ));
    }
    let call = observation.tool_calls[0].clone();
    if call.name != "lookup_ticket"
        || call
            .arguments
            .get("ticket_id")
            .and_then(|value| value.as_str())
            != Some("TICKET-42")
    {
        return Err(ProviderError::Protocol(
            "calibration requires lookup_ticket for TICKET-42".into(),
        ));
    }
    Ok(call)
}

fn lookup_ticket_definition() -> ToolDefinition {
    ToolDefinition {
        name: "lookup_ticket".into(),
        description: "Look up a support ticket by ID.".into(),
        parameters: json!({
            "type":"object",
            "properties":{"ticket_id":{"type":"string"}},
            "required":["ticket_id"],
            "additionalProperties":false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ProviderKind, SamplingPolicy};
    use std::cell::RefCell;
    use std::rc::Rc;

    struct FakeCandidate {
        identity: CandidateIdentity,
        ownership: CandidateOwnership,
        events: Rc<RefCell<Vec<String>>>,
        calls: usize,
        fail_at: Option<usize>,
        isolation_error: bool,
    }

    impl ProviderAdapter for FakeCandidate {
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
            self.calls += 1;
            self.events
                .borrow_mut()
                .push(format!("{}:invoke", self.identity.candidate_id));
            if self.fail_at == Some(self.calls) {
                return Err(ProviderError::Unavailable);
            }
            let odd = self.calls % 2 == 1;
            Ok(InvocationObservation {
                content: (!odd).then(|| "TICKET-42 resolved".into()),
                tool_calls: if odd {
                    vec![ToolCall {
                        id: Some("call-1".into()),
                        name: "lookup_ticket".into(),
                        arguments: json!({"ticket_id":"TICKET-42"}),
                    }]
                } else {
                    vec![]
                },
                prompt_tokens: Some(10),
                completion_tokens: Some(5),
                ttft_ns: Some(1),
                total_duration_ns: 2,
                prompt_rate: Some(3.0),
                decode_rate: Some(4.0),
                raw_events: vec![],
            })
        }
    }

    impl CalibrationCandidate for FakeCandidate {
        fn ownership(&self) -> CandidateOwnership {
            self.ownership
        }
        fn prepare(&mut self) -> Result<(), ProviderError> {
            self.events
                .borrow_mut()
                .push(format!("{}:prepare", self.identity.candidate_id));
            Ok(())
        }
        fn finish(&mut self) -> Result<(), ProviderError> {
            self.events
                .borrow_mut()
                .push(format!("{}:finish", self.identity.candidate_id));
            Ok(())
        }
        fn isolation_check(&mut self) -> Result<(), ProviderError> {
            self.events
                .borrow_mut()
                .push(format!("{}:isolation", self.identity.candidate_id));
            if self.isolation_error {
                Err(ProviderError::Protocol("uncontrolled state".into()))
            } else {
                Ok(())
            }
        }
    }

    fn identity(id: &str, provider: ProviderKind) -> CandidateIdentity {
        CandidateIdentity {
            candidate_id: id.into(),
            provider,
            provider_version: "1".into(),
            engine_revision: Some("rev".into()),
            model_id: "model".into(),
            artifact_digest: "sha256:a".into(),
            tokenizer_digest: "sha256:b".into(),
            chat_template_digest: "sha256:c".into(),
            context_tokens: 8192,
            required_free_memory_bytes: 100,
            sampling: SamplingPolicy {
                temperature_milli: 0,
                top_p_milli: 1000,
                seed: 1,
            },
        }
    }

    fn candidates(events: Rc<RefCell<Vec<String>>>) -> (FakeCandidate, FakeCandidate) {
        (
            FakeCandidate {
                identity: identity("managed", ProviderKind::ManagedLlama),
                ownership: CandidateOwnership::Managed,
                events: events.clone(),
                calls: 0,
                fail_at: None,
                isolation_error: false,
            },
            FakeCandidate {
                identity: identity("attached", ProviderKind::Ollama),
                ownership: CandidateOwnership::Attached,
                events,
                calls: 0,
                fail_at: None,
                isolation_error: false,
            },
        )
    }

    #[test]
    fn calibration_runs_one_warmup_then_five_counterbalanced_pairs_at_concurrency_one() {
        let events = Rc::new(RefCell::new(vec![]));
        let (mut managed, mut attached) = candidates(events.clone());
        assert!(matches!(
            run_calibration(&mut managed, &mut attached, || 1_000),
            CalibrationOutcome::Completed { .. }
        ));
        let invocations = events
            .borrow()
            .iter()
            .filter(|event| event.ends_with(":invoke"))
            .cloned()
            .collect::<Vec<_>>();
        let sample_order = invocations
            .chunks_exact(2)
            .map(|chunk| chunk[0].split(':').next().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            sample_order,
            vec![
                "managed", "attached", "managed", "attached", "attached", "managed", "managed",
                "attached", "attached", "managed", "managed", "attached"
            ]
        );
    }

    #[test]
    fn calibration_never_finishes_or_unloads_the_attached_candidate() {
        let events = Rc::new(RefCell::new(vec![]));
        let (mut managed, mut attached) = candidates(events.clone());
        let _ = run_calibration(&mut managed, &mut attached, || 1_000);
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event == "attached:finish"));
    }

    #[test]
    fn calibration_finishes_the_managed_candidate_on_success() {
        let events = Rc::new(RefCell::new(vec![]));
        let (mut managed, mut attached) = candidates(events.clone());
        let _ = run_calibration(&mut managed, &mut attached, || 1_000);
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| *event == "managed:finish")
                .count(),
            1
        );
    }

    #[test]
    fn calibration_finishes_the_managed_candidate_after_provider_failure() {
        let events = Rc::new(RefCell::new(vec![]));
        let (mut managed, mut attached) = candidates(events.clone());
        managed.fail_at = Some(1);
        let outcome = run_calibration(&mut managed, &mut attached, || 1_000);
        let CalibrationOutcome::Failed { evidence, .. } = outcome else {
            panic!("expected failed calibration");
        };
        assert!(evidence.managed.failure.is_some());
        assert!(evidence.attached.failure.is_none());
        assert!(events
            .borrow()
            .iter()
            .any(|event| event == "managed:finish"));
    }

    #[test]
    fn calibration_returns_uncontrolled_before_any_invocation() {
        let events = Rc::new(RefCell::new(vec![]));
        let (mut managed, mut attached) = candidates(events.clone());
        attached.isolation_error = true;
        assert!(matches!(
            run_calibration(&mut managed, &mut attached, || 1_000),
            CalibrationOutcome::Uncontrolled { .. }
        ));
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.ends_with(":invoke")));
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.ends_with(":prepare")));
    }
}
