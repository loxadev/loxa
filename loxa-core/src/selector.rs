use crate::calibration::{CalibrationEvidence, CandidateEvidence};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum SelectorVerdict {
    Selected { candidate_id: String },
    NoVerifiedPlan,
    NoMaterialWinner { baseline_id: String },
}

pub fn select_plan(evidence: &CalibrationEvidence) -> SelectorVerdict {
    let managed_passes = hard_gates_pass(&evidence.managed);
    let attached_passes = hard_gates_pass(&evidence.attached);
    match (managed_passes, attached_passes) {
        (false, false) => SelectorVerdict::NoVerifiedPlan,
        (true, false) => selected(&evidence.managed),
        (false, true) => selected(&evidence.attached),
        (true, true) => compare_qualified(evidence),
    }
}

fn selected(candidate: &CandidateEvidence) -> SelectorVerdict {
    SelectorVerdict::Selected {
        candidate_id: candidate.identity.candidate_id.clone(),
    }
}

fn hard_gates_pass(candidate: &CandidateEvidence) -> bool {
    candidate.qualified
        && candidate
            .qualification
            .as_ref()
            .is_some_and(crate::qualification::QualificationReport::passed)
        && candidate.failure.is_none()
        && candidate.identity.identity_errors().is_empty()
        && candidate.available_memory_before_bytes >= candidate.identity.required_free_memory_bytes
}

fn compare_qualified(evidence: &CalibrationEvidence) -> SelectorVerdict {
    if evidence.pairs.len() != 5 {
        return SelectorVerdict::NoVerifiedPlan;
    }
    let mut managed_times = evidence
        .pairs
        .iter()
        .map(|pair| pair.managed.wall_time_ns)
        .collect::<Vec<_>>();
    let mut attached_times = evidence
        .pairs
        .iter()
        .map(|pair| pair.attached.wall_time_ns)
        .collect::<Vec<_>>();
    managed_times.sort_unstable();
    attached_times.sort_unstable();
    let managed_median = managed_times[2];
    let attached_median = attached_times[2];
    let attached_wins = evidence
        .pairs
        .iter()
        .filter(|pair| pair.attached.wall_time_ns < pair.managed.wall_time_ns)
        .count();
    let ninety_percent = (managed_median / 10) * 9 + ((managed_median % 10) * 9) / 10;
    let materially_faster = attached_median <= ninety_percent;
    if materially_faster && attached_wins >= 4 {
        selected(&evidence.attached)
    } else {
        SelectorVerdict::NoMaterialWinner {
            baseline_id: evidence.managed.identity.candidate_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calibration::{
        CalibrationMeasurement, CandidateOwnership, PairedObservation,
        CALIBRATION_EVIDENCE_SCHEMA_VERSION,
    };
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
    use crate::qualification::{QualificationReport, QualificationResult};

    fn passing_qualification() -> QualificationReport {
        QualificationReport {
            results: [
                "weather_required_city",
                "no_tool_needed",
                "weather_optional_units",
                "weather_argument_types",
                "multi_turn_ticket_context",
            ]
            .into_iter()
            .map(|case_id| QualificationResult {
                case_id: case_id.into(),
                passed: true,
                reason: "structural requirements satisfied".into(),
                elapsed_ns: 1,
                observations: vec![],
            })
            .collect(),
        }
    }

    fn candidate(id: &str, ownership: CandidateOwnership, qualified: bool) -> CandidateEvidence {
        CandidateEvidence {
            identity: CandidateIdentity {
                candidate_id: id.into(),
                provider: if ownership == CandidateOwnership::Managed {
                    ProviderKind::ManagedLlama
                } else {
                    ProviderKind::Ollama
                },
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
            },
            ownership,
            qualified,
            qualification: qualified.then(passing_qualification),
            available_memory_before_bytes: 1_000,
            failure: None,
            warmup: None,
        }
    }

    fn measurement(id: &str, wall_time_ns: u128) -> CalibrationMeasurement {
        CalibrationMeasurement {
            candidate_id: id.into(),
            wall_time_ns,
            prompt_tokens: None,
            completion_tokens: None,
            ttft_ns: None,
            prompt_rate: None,
            decode_rate: None,
        }
    }
    fn evidence(
        managed: CandidateEvidence,
        attached: CandidateEvidence,
        times: &[(u128, u128)],
    ) -> CalibrationEvidence {
        CalibrationEvidence {
            schema_version: CALIBRATION_EVIDENCE_SCHEMA_VERSION,
            managed,
            attached,
            pairs: times
                .iter()
                .enumerate()
                .map(|(index, (managed, attached))| PairedObservation {
                    pair_index: index as u8,
                    managed: measurement("managed", *managed),
                    attached: measurement("attached", *attached),
                })
                .collect(),
            verdict: None,
        }
    }
    fn default_times() -> Vec<(u128, u128)> {
        vec![(100, 80), (100, 80), (100, 80), (100, 80), (100, 110)]
    }

    #[test]
    fn selector_returns_no_verified_plan_when_both_fail_hard_gates() {
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, false),
            candidate("attached", CandidateOwnership::Attached, false),
            &default_times(),
        );
        assert_eq!(select_plan(&input), SelectorVerdict::NoVerifiedPlan);
    }

    #[test]
    fn selector_selects_the_only_qualified_candidate() {
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, true),
            candidate("attached", CandidateOwnership::Attached, false),
            &default_times(),
        );
        assert_eq!(
            select_plan(&input),
            SelectorVerdict::Selected {
                candidate_id: "managed".into()
            }
        );
    }

    #[test]
    fn selector_selects_attached_only_when_ten_percent_faster_and_four_of_five() {
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, true),
            candidate("attached", CandidateOwnership::Attached, true),
            &default_times(),
        );
        assert_eq!(
            select_plan(&input),
            SelectorVerdict::Selected {
                candidate_id: "attached".into()
            }
        );
    }

    #[test]
    fn selector_returns_no_material_winner_at_exactly_below_ten_percent() {
        let times = vec![(100, 91), (100, 91), (100, 91), (100, 91), (100, 110)];
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, true),
            candidate("attached", CandidateOwnership::Attached, true),
            &times,
        );
        assert_eq!(
            select_plan(&input),
            SelectorVerdict::NoMaterialWinner {
                baseline_id: "managed".into()
            }
        );
    }

    #[test]
    fn selector_returns_no_material_winner_when_attached_wins_only_three_pairs() {
        let times = vec![(100, 80), (100, 80), (100, 80), (100, 120), (100, 120)];
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, true),
            candidate("attached", CandidateOwnership::Attached, true),
            &times,
        );
        assert_eq!(
            select_plan(&input),
            SelectorVerdict::NoMaterialWinner {
                baseline_id: "managed".into()
            }
        );
    }

    #[test]
    fn selector_threshold_comparison_does_not_overflow() {
        let times = vec![(u128::MAX, u128::MAX - 1); 5];
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, true),
            candidate("attached", CandidateOwnership::Attached, true),
            &times,
        );
        assert_eq!(
            select_plan(&input),
            SelectorVerdict::NoMaterialWinner {
                baseline_id: "managed".into()
            }
        );
    }

    #[test]
    fn selector_rejects_incomplete_identity_before_performance_comparison() {
        let mut attached = candidate("attached", CandidateOwnership::Attached, true);
        attached.identity.engine_revision = None;
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, false),
            attached,
            &default_times(),
        );
        assert_eq!(select_plan(&input), SelectorVerdict::NoVerifiedPlan);
    }

    #[test]
    fn selector_rejects_candidate_without_required_free_memory_headroom() {
        let mut attached = candidate("attached", CandidateOwnership::Attached, true);
        attached.available_memory_before_bytes = 99;
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, false),
            attached,
            &default_times(),
        );
        assert_eq!(select_plan(&input), SelectorVerdict::NoVerifiedPlan);
    }

    #[test]
    fn selector_rejects_qualified_boolean_without_passing_report() {
        let mut attached = candidate("attached", CandidateOwnership::Attached, true);
        attached.qualification = None;
        let input = evidence(
            candidate("managed", CandidateOwnership::Managed, false),
            attached,
            &default_times(),
        );
        assert_eq!(select_plan(&input), SelectorVerdict::NoVerifiedPlan);
    }
}
