use serde::{Deserialize, Serialize};

pub const SELECTOR_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredRepetition {
    pub schema_version: u32,
    pub repetition: u8,
    pub end_to_end_duration_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateQualification {
    pub schema_version: u32,
    pub candidate_id: String,
    pub passed: bool,
    pub reasons: Vec<String>,
    pub measured_repetitions: Vec<MeasuredRepetition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case", deny_unknown_fields)]
pub enum SelectorVerdict {
    Selected {
        schema_version: u32,
        candidate_id: String,
        reason: String,
    },
    NoVerifiedPlan {
        schema_version: u32,
        reasons: Vec<String>,
    },
    NoMaterialWinner {
        schema_version: u32,
        baseline_candidate_id: String,
        reason: String,
    },
}

pub fn select_v1(
    managed_a: &CandidateQualification,
    attached_b: &CandidateQualification,
) -> SelectorVerdict {
    match (
        has_verified_qualification(managed_a),
        has_verified_qualification(attached_b),
    ) {
        (false, false) => SelectorVerdict::NoVerifiedPlan {
            schema_version: SELECTOR_SCHEMA_VERSION,
            reasons: qualification_failure_reasons(managed_a, attached_b),
        },
        (true, false) => selected(
            &managed_a.candidate_id,
            "only the managed candidate passed all qualification cases",
        ),
        (false, true) => selected(
            &attached_b.candidate_id,
            "only the attached candidate passed all qualification cases",
        ),
        (true, true) => select_by_paired_measurements(managed_a, attached_b),
    }
}

fn has_verified_qualification(candidate: &CandidateQualification) -> bool {
    candidate.schema_version == SELECTOR_SCHEMA_VERSION && candidate.passed
}

fn selected(candidate_id: &str, reason: &str) -> SelectorVerdict {
    SelectorVerdict::Selected {
        schema_version: SELECTOR_SCHEMA_VERSION,
        candidate_id: candidate_id.to_owned(),
        reason: reason.to_owned(),
    }
}

fn no_material_winner(baseline_candidate_id: &str, reason: &str) -> SelectorVerdict {
    SelectorVerdict::NoMaterialWinner {
        schema_version: SELECTOR_SCHEMA_VERSION,
        baseline_candidate_id: baseline_candidate_id.to_owned(),
        reason: reason.to_owned(),
    }
}

fn qualification_failure_reasons(
    managed_a: &CandidateQualification,
    attached_b: &CandidateQualification,
) -> Vec<String> {
    [managed_a, attached_b]
        .into_iter()
        .flat_map(|candidate| {
            if candidate.schema_version != SELECTOR_SCHEMA_VERSION {
                vec![format!(
                    "{}: unsupported_qualification_schema_version: {}",
                    candidate.candidate_id, candidate.schema_version
                )]
            } else if candidate.reasons.is_empty() {
                vec![format!("{}: qualification_failed", candidate.candidate_id)]
            } else {
                candidate
                    .reasons
                    .iter()
                    .map(|reason| format!("{}: {reason}", candidate.candidate_id))
                    .collect()
            }
        })
        .collect()
}

fn select_by_paired_measurements(
    managed_a: &CandidateQualification,
    attached_b: &CandidateQualification,
) -> SelectorVerdict {
    let Some(pairs) = five_successful_pairs(managed_a, attached_b) else {
        return no_material_winner(
            &managed_a.candidate_id,
            "five comparable successful paired repetitions were not available",
        );
    };

    let mut a_durations = pairs.iter().map(|(a, _)| *a).collect::<Vec<_>>();
    let mut b_durations = pairs.iter().map(|(_, b)| *b).collect::<Vec<_>>();
    a_durations.sort_unstable();
    b_durations.sort_unstable();
    let a_median = a_durations[2];
    let b_median = b_durations[2];
    let b_pair_wins = pairs.iter().filter(|(a, b)| b < a).count();
    let b_is_ten_percent_faster = u128::from(b_median) * 10 <= u128::from(a_median) * 9;

    if b_is_ten_percent_faster && b_pair_wins >= 4 {
        selected(
            &attached_b.candidate_id,
            "attached candidate median was at least 10% lower and it won at least 4 of 5 paired repetitions",
        )
    } else {
        no_material_winner(
            &managed_a.candidate_id,
            "attached candidate did not clear both the 10% median and 4-of-5 paired-win thresholds",
        )
    }
}

fn five_successful_pairs(
    managed_a: &CandidateQualification,
    attached_b: &CandidateQualification,
) -> Option<Vec<(u64, u64)>> {
    if managed_a.measured_repetitions.len() != 5 || attached_b.measured_repetitions.len() != 5 {
        return None;
    }

    (1..=5)
        .map(|repetition| {
            let a = unique_successful_repetition(managed_a, repetition)?;
            let b = unique_successful_repetition(attached_b, repetition)?;
            Some((a, b))
        })
        .collect()
}

fn unique_successful_repetition(candidate: &CandidateQualification, repetition: u8) -> Option<u64> {
    let mut matching = candidate.measured_repetitions.iter().filter(|measurement| {
        measurement.schema_version == SELECTOR_SCHEMA_VERSION
            && measurement.repetition == repetition
    });
    let duration = matching.next()?.end_to_end_duration_ns?;
    if duration == 0 || matching.next().is_some() {
        return None;
    }
    Some(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        candidate_id: &str,
        passed: bool,
        reasons: &[&str],
        durations: &[Option<u64>],
    ) -> CandidateQualification {
        CandidateQualification {
            schema_version: 1,
            candidate_id: candidate_id.to_owned(),
            passed,
            reasons: reasons.iter().map(|reason| (*reason).to_owned()).collect(),
            measured_repetitions: durations
                .iter()
                .enumerate()
                .map(|(index, duration)| MeasuredRepetition {
                    schema_version: 1,
                    repetition: (index + 1) as u8,
                    end_to_end_duration_ns: *duration,
                })
                .collect(),
        }
    }

    #[test]
    fn selects_a_when_only_a_passes_qualification() {
        let a = candidate("managed-a", true, &[], &[]);
        let b = candidate("attached-b", false, &["case_2_failed"], &[]);

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::Selected { candidate_id, .. } if candidate_id == "managed-a"
        ));
    }

    #[test]
    fn selects_b_when_only_b_passes_qualification() {
        let a = candidate("managed-a", false, &["case_4_failed"], &[]);
        let b = candidate("attached-b", true, &[], &[]);

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::Selected { candidate_id, .. } if candidate_id == "attached-b"
        ));
    }

    #[test]
    fn returns_no_verified_plan_when_neither_passes() {
        let a = candidate("managed-a", false, &["case_1_failed"], &[]);
        let b = candidate("attached-b", false, &["case_3_failed"], &[]);

        let SelectorVerdict::NoVerifiedPlan { reasons, .. } = select_v1(&a, &b) else {
            panic!("expected no verified plan");
        };
        assert_eq!(
            reasons,
            vec![
                "managed-a: case_1_failed".to_owned(),
                "attached-b: case_3_failed".to_owned()
            ]
        );
    }

    #[test]
    fn unsupported_b_is_unverified_when_a_fails() {
        let a = candidate("managed-a", false, &["case_1_failed"], &[]);
        let mut b = candidate("attached-b", true, &[], &[]);
        b.schema_version = 99;

        let SelectorVerdict::NoVerifiedPlan { reasons, .. } = select_v1(&a, &b) else {
            panic!("unsupported B must not be selected");
        };
        assert_eq!(
            reasons,
            vec![
                "managed-a: case_1_failed".to_owned(),
                "attached-b: unsupported_qualification_schema_version: 99".to_owned(),
            ]
        );
    }

    #[test]
    fn unsupported_a_is_unverified_when_b_fails() {
        let mut a = candidate("managed-a", true, &[], &[]);
        a.schema_version = 42;
        let b = candidate("attached-b", false, &["case_3_failed"], &[]);

        let SelectorVerdict::NoVerifiedPlan { reasons, .. } = select_v1(&a, &b) else {
            panic!("unsupported A must not be selected");
        };
        assert_eq!(
            reasons,
            vec![
                "managed-a: unsupported_qualification_schema_version: 42".to_owned(),
                "attached-b: case_3_failed".to_owned(),
            ]
        );
    }

    #[test]
    fn nominally_passed_unsupported_b_cannot_compete_with_verified_a() {
        let a = candidate("managed-a", true, &[], &[Some(100); 5]);
        let mut b = candidate("attached-b", true, &[], &[Some(1); 5]);
        b.schema_version = 2;

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::Selected { candidate_id, .. } if candidate_id == "managed-a"
        ));
    }

    #[test]
    fn nominally_passed_unsupported_a_cannot_compete_with_verified_b() {
        let mut a = candidate("managed-a", true, &[], &[Some(1); 5]);
        a.schema_version = 2;
        let b = candidate("attached-b", true, &[], &[Some(100); 5]);

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::Selected { candidate_id, .. } if candidate_id == "attached-b"
        ));
    }

    #[test]
    fn two_nominal_passes_with_unsupported_schemas_have_no_verified_plan() {
        let mut a = candidate("managed-a", true, &[], &[Some(100); 5]);
        a.schema_version = 2;
        let mut b = candidate("attached-b", true, &[], &[Some(1); 5]);
        b.schema_version = 3;

        let SelectorVerdict::NoVerifiedPlan { reasons, .. } = select_v1(&a, &b) else {
            panic!("unsupported qualifications must not reach timing selection");
        };
        assert_eq!(
            reasons,
            vec![
                "managed-a: unsupported_qualification_schema_version: 2".to_owned(),
                "attached-b: unsupported_qualification_schema_version: 3".to_owned(),
            ]
        );
    }

    #[test]
    fn selects_b_when_median_is_ten_percent_lower_and_b_wins_four_pairs() {
        let a = candidate(
            "managed-a",
            true,
            &[],
            &[Some(100), Some(100), Some(100), Some(100), Some(80)],
        );
        let b = candidate(
            "attached-b",
            true,
            &[],
            &[Some(90), Some(90), Some(90), Some(90), Some(90)],
        );

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::Selected { candidate_id, .. } if candidate_id == "attached-b"
        ));
    }

    #[test]
    fn retains_a_when_median_clears_ten_percent_but_b_wins_only_three_pairs() {
        let a = candidate(
            "managed-a",
            true,
            &[],
            &[Some(50), Some(50), Some(100), Some(100), Some(100)],
        );
        let b = candidate(
            "attached-b",
            true,
            &[],
            &[Some(60), Some(60), Some(90), Some(90), Some(90)],
        );

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::NoMaterialWinner { baseline_candidate_id, .. }
                if baseline_candidate_id == "managed-a"
        ));
    }

    #[test]
    fn retains_a_when_b_wins_all_pairs_but_is_under_ten_percent_faster() {
        let a = candidate("managed-a", true, &[], &[Some(100); 5]);
        let b = candidate("attached-b", true, &[], &[Some(91); 5]);

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::NoMaterialWinner { baseline_candidate_id, .. }
                if baseline_candidate_id == "managed-a"
        ));
    }

    #[test]
    fn retains_a_for_tied_or_noisy_measurements() {
        let a = candidate(
            "managed-a",
            true,
            &[],
            &[Some(100), Some(80), Some(120), Some(100), Some(100)],
        );
        let b = candidate(
            "attached-b",
            true,
            &[],
            &[Some(90), Some(90), Some(110), Some(110), Some(100)],
        );

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::NoMaterialWinner { baseline_candidate_id, .. }
                if baseline_candidate_id == "managed-a"
        ));
    }

    #[test]
    fn missing_or_failed_measurement_is_not_treated_as_zero() {
        let a = candidate("managed-a", true, &[], &[Some(100); 5]);
        let b = candidate(
            "attached-b",
            true,
            &[],
            &[Some(1), Some(1), Some(1), Some(1), None],
        );

        let verdict = select_v1(&a, &b);
        assert!(matches!(
            verdict,
            SelectorVerdict::NoMaterialWinner { baseline_candidate_id, .. }
                if baseline_candidate_id == "managed-a"
        ));
    }

    #[test]
    fn zero_duration_is_not_accepted_as_a_successful_measurement() {
        let a = candidate("managed-a", true, &[], &[Some(100); 5]);
        let b = candidate("attached-b", true, &[], &[Some(0); 5]);

        assert!(matches!(
            select_v1(&a, &b),
            SelectorVerdict::NoMaterialWinner {
                baseline_candidate_id,
                ..
            } if baseline_candidate_id == "managed-a"
        ));
    }

    #[test]
    fn verdict_is_deterministic_and_serializes_with_version() {
        let a = candidate("managed-a", true, &[], &[Some(100); 5]);
        let b = candidate("attached-b", true, &[], &[Some(90); 5]);

        let first = select_v1(&a, &b);
        let second = select_v1(&a, &b);

        assert_eq!(first, second);
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            r#"{"verdict":"selected","schema_version":1,"candidate_id":"attached-b","reason":"attached candidate median was at least 10% lower and it won at least 4 of 5 paired repetitions"}"#
        );
    }
}
