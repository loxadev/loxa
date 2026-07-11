use crate::evidence::{
    CalibrationEvidence, CandidateEvidence, DisclosedDifference, EvidenceError, EvidenceVerdict,
    HostFingerprint, IsolationObservation, MeasurementEvidence, QualificationEvidence,
    SelectionDisposition, SelectionRecord, EVIDENCE_SCHEMA_VERSION, SELECTION_SCHEMA_VERSION,
};
use crate::provider::{ControlledRun, ProviderAdapter, ProviderError, ProviderMessage};
use crate::selector::{self, CandidateQualification, MeasuredRepetition, SelectorVerdict};
use crate::workload::{self, QualificationCaseResult};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const WARMUPS: u8 = 1;
pub const MEASURED_PAIRS: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreflightObservation {
    pub code: String,
    pub controlled: bool,
}

pub trait HostProbe {
    fn preflight(&mut self) -> Vec<PreflightObservation>;
    fn available_memory_bytes(&mut self) -> u64;
    fn fingerprint(&mut self) -> HostFingerprint;
    fn background_code(&mut self) -> String {
        "background_load_observed".into()
    }
    fn thermal_code(&mut self) -> Option<String> {
        Some("thermal_state_unknown".into())
    }
}

pub struct SystemHostProbe;
impl HostProbe for SystemHostProbe {
    fn preflight(&mut self) -> Vec<PreflightObservation> {
        let h = crate::hardware::HardwareReport::detect();
        let load = sysinfo::System::load_average().one;
        let load_controlled =
            h.logical_cores > 0 && load.is_finite() && load <= h.logical_cores as f64 * 0.5;
        let thermal_code =
            macos_thermal_code().unwrap_or_else(|| "thermal_probe_unavailable".into());
        let thermal_nominal = thermal_code == "thermal_nominal";
        vec![
            PreflightObservation {
                code: "memory_headroom".into(),
                controlled: h.ram_available_bytes >= 6 * 1024 * 1024 * 1024,
            },
            PreflightObservation {
                code: "background_load_controlled".into(),
                controlled: load_controlled,
            },
            PreflightObservation {
                code: thermal_code,
                controlled: thermal_nominal,
            },
            PreflightObservation {
                code: "isolation_controlled".into(),
                controlled: load_controlled && thermal_nominal,
            },
            PreflightObservation {
                code: "disk_headroom".into(),
                controlled: h
                    .root_disk_available_bytes
                    .is_some_and(|v| v >= 2 * 1024 * 1024 * 1024),
            },
        ]
    }
    fn available_memory_bytes(&mut self) -> u64 {
        crate::hardware::HardwareReport::detect().ram_available_bytes
    }
    fn fingerprint(&mut self) -> HostFingerprint {
        let h = crate::hardware::HardwareReport::detect();
        HostFingerprint {
            schema_version: 1,
            os_name: sanitize_fact(&h.os_name),
            os_version: sanitize_fact(&h.os_version),
            hardware_model: sanitize_fact(&h.chip),
            physical_cores: h.physical_cores,
            logical_cores: h.logical_cores,
            memory_total_bytes: h.ram_total_bytes,
            memory_available_bytes: h.ram_available_bytes,
            root_disk_total_bytes: h.root_disk_total_bytes,
            root_disk_available_bytes: h.root_disk_available_bytes,
        }
    }
    fn background_code(&mut self) -> String {
        let h = crate::hardware::HardwareReport::detect();
        let load = sysinfo::System::load_average().one;
        if h.logical_cores > 0 && load.is_finite() && load <= h.logical_cores as f64 * 0.5 {
            "background_load_controlled"
        } else {
            "background_load_uncontrolled"
        }
        .into()
    }
    fn thermal_code(&mut self) -> Option<String> {
        macos_thermal_code()
    }
}

fn macos_thermal_code() -> Option<String> {
    if std::env::consts::OS != "macos" {
        return Some("thermal_probe_unavailable".into());
    }
    let mut child = match Command::new("pmset")
        .args(["-g", "therm"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return Some("thermal_probe_unavailable".into()),
    };
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(500) {
        if child.try_wait().ok().flatten().is_some() {
            let output = match child.wait_with_output() {
                Ok(output) => output,
                Err(_) => return Some("thermal_probe_unavailable".into()),
            };
            if !output.status.success() {
                return Some("thermal_probe_unavailable".into());
            }
            let text = String::from_utf8_lossy(&output.stdout);
            return Some(parse_macos_thermal_output(&text).into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    Some("thermal_probe_timeout".into())
}

fn parse_macos_thermal_output(text: &str) -> &'static str {
    let cpu = thermal_limit(text, "CPU_Speed_Limit");
    let scheduler = thermal_limit(text, "Scheduler_Limit");
    match (cpu, scheduler) {
        (Some(100), Some(100)) => "thermal_nominal",
        (Some(value), _) | (_, Some(value)) if value < 100 => "thermal_throttled",
        _ => "thermal_probe_parse_changed",
    }
}

fn thermal_limit(text: &str, name: &str) -> Option<u32> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == name)
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

#[derive(Debug)]
pub enum CalibrationError {
    Isolation(Vec<String>),
    Provider(ProviderError),
    IdentityChanged,
    Evidence(EvidenceError),
    OperationAndTeardown {
        operation: Box<CalibrationError>,
        teardown: ProviderError,
    },
    Aborted {
        kind: String,
        evidence_path: PathBuf,
    },
}
impl From<EvidenceError> for CalibrationError {
    fn from(e: EvidenceError) -> Self {
        Self::Evidence(e)
    }
}

#[derive(Debug)]
pub struct CalibrationOutcome {
    pub evidence: CalibrationEvidence,
    pub evidence_path: Option<PathBuf>,
    pub verdict: SelectorVerdict,
}

pub struct CalibrationRunner;
impl CalibrationRunner {
    pub fn run(
        host: &mut dyn HostProbe,
        a: &mut dyn ProviderAdapter,
        b: &mut dyn ProviderAdapter,
    ) -> Result<CalibrationOutcome, CalibrationError> {
        let started = now_ms();
        // Inspect both candidates before host/isolation refusal. Candidate evidence must never
        // contain a provisional identity presented as though it had been verified.
        let inspected_a = a.inspect_candidate();
        let inspected_b = b.inspect_candidate();
        let ai = inspected_a.map_err(CalibrationError::Provider)?;
        let bi = inspected_b.map_err(CalibrationError::Provider)?;
        ai.validate_pinned().map_err(CalibrationError::Provider)?;
        bi.validate_pinned().map_err(CalibrationError::Provider)?;
        let preflight = host.preflight();
        let mut isolation = preflight
            .iter()
            .map(|o| isolation_observation(&o.code, o.controlled))
            .collect::<Vec<_>>();
        let rejected = preflight
            .iter()
            .filter(|o| !o.controlled)
            .map(|o| o.code.clone())
            .collect::<Vec<_>>();
        if !rejected.is_empty() {
            let verdict = SelectorVerdict::NoVerifiedPlan {
                schema_version: 1,
                reasons: vec![format!("{}: isolation_preflight_failed", ai.candidate_id)],
            };
            let evidence = base_evidence(
                started,
                host.fingerprint(),
                ai,
                bi,
                vec![],
                vec![],
                isolation,
                EvidenceVerdict::from_selector(&verdict)?,
                rejected
                    .into_iter()
                    .chain(std::iter::once("isolation_preflight_failed".into()))
                    .collect(),
            );
            return Ok(CalibrationOutcome {
                evidence,
                evidence_path: None,
                verdict,
            });
        }
        if let Err(error) = check_provider_activity("candidate_a", a, &mut isolation)
            .and_then(|_| check_provider_preflight("candidate_b", b, &mut isolation))
        {
            let code = calibration_error_code(&error);
            isolation.push(isolation_observation(code, false));
            return aborted_outcome(
                started,
                host.fingerprint(),
                ai,
                bi,
                isolation,
                vec![],
                vec![],
                code,
            );
        }
        let (aq, bq, measurements) = match execute(host, a, b) {
            Ok(execution) => execution,
            Err(error) => {
                let code = calibration_error_code(&error);
                isolation.push(isolation_observation(code, false));
                if matches!(error, CalibrationError::OperationAndTeardown { .. }) {
                    isolation.push(isolation_observation("operation_failed", false));
                    isolation.push(isolation_observation("teardown_failed", false));
                }
                return aborted_outcome(
                    started,
                    host.fingerprint(),
                    ai,
                    bi,
                    isolation,
                    vec![],
                    vec![],
                    code,
                );
            }
        };
        isolation.extend(measurements.3.clone());
        let a_fingerprint = ai.fingerprint();
        let b_fingerprint = bi.fingerprint();
        if !measurements.3.is_empty() {
            return aborted_outcome(
                started,
                host.fingerprint(),
                ai,
                bi,
                isolation,
                vec![
                    qualification_evidence(&a_fingerprint, &aq, &measurements.0),
                    qualification_evidence(&b_fingerprint, &bq, &measurements.1),
                ],
                measurements.2,
                "teardown_failed",
            );
        }
        if measurements
            .2
            .iter()
            .any(|measurement| measurement.failure_code.as_deref() == Some("isolation_lost"))
        {
            isolation.push(isolation_observation("measurement_isolation_lost", false));
            return aborted_outcome(
                started,
                host.fingerprint(),
                ai,
                bi,
                isolation,
                vec![
                    qualification_evidence(&a_fingerprint, &aq, &measurements.0),
                    qualification_evidence(&b_fingerprint, &bq, &measurements.1),
                ],
                measurements.2,
                "isolation_lost",
            );
        }
        let final_a = a.inspect_candidate();
        let final_b = b.inspect_candidate();
        if final_a.as_ref().map(|c| c.fingerprint()).ok().as_deref() != Some(a_fingerprint.as_str())
            || final_b.as_ref().map(|c| c.fingerprint()).ok().as_deref()
                != Some(b_fingerprint.as_str())
        {
            let code = if final_a.is_err() || final_b.is_err() {
                "identity_inspection_failed"
            } else {
                "identity_changed"
            };
            isolation.push(isolation_observation(code, false));
            return aborted_outcome(
                started,
                host.fingerprint(),
                ai,
                bi,
                isolation,
                vec![
                    qualification_evidence(&a_fingerprint, &aq, &measurements.0),
                    qualification_evidence(&b_fingerprint, &bq, &measurements.1),
                ],
                measurements.2,
                code,
            );
        }
        let verdict = selector::select_v1(&aq, &bq);
        let evidence = base_evidence(
            started,
            host.fingerprint(),
            ai.clone(),
            bi.clone(),
            vec![
                qualification_evidence(&ai.fingerprint(), &aq, &measurements.0),
                qualification_evidence(&bi.fingerprint(), &bq, &measurements.1),
            ],
            measurements.2,
            isolation,
            EvidenceVerdict::from_selector(&verdict)?,
            vec![verdict_code(&verdict).into()],
        );
        Ok(CalibrationOutcome {
            evidence,
            evidence_path: None,
            verdict,
        })
    }

    pub fn run_and_persist(
        host: &mut dyn HostProbe,
        a: &mut dyn ProviderAdapter,
        b: &mut dyn ProviderAdapter,
        evidence_dir: &Path,
        selection_path: &Path,
    ) -> Result<CalibrationOutcome, CalibrationError> {
        let mut outcome = Self::run(host, a, b)?;
        let path = crate::evidence::write_evidence_new(evidence_dir, &outcome.evidence)?;
        if matches!(outcome.verdict, SelectorVerdict::NoVerifiedPlan { .. })
            && outcome
                .evidence
                .explanation_codes
                .iter()
                .any(|code| code != "no_verified_plan")
        {
            let kind = outcome
                .evidence
                .explanation_codes
                .first()
                .cloned()
                .unwrap_or_else(|| "calibration_aborted".into());
            return Err(CalibrationError::Aborted {
                kind,
                evidence_path: path,
            });
        }
        if let Some(selection) = selection_for(&outcome.evidence, &outcome.verdict) {
            crate::evidence::write_selection_atomic(selection_path, &selection)?;
        }
        outcome.evidence_path = Some(path);
        Ok(outcome)
    }
}

pub fn run_pinned_calibration() -> Result<CalibrationOutcome, CalibrationError> {
    use crate::provider::ollama::{HttpOllamaTransport, OllamaAdapter};
    const ENDPOINT: &str = "http://127.0.0.1:11434";
    let transport = HttpOllamaTransport::new(ENDPOINT).map_err(CalibrationError::Provider)?;
    let mut attached =
        OllamaAdapter::new(ENDPOINT, transport).map_err(CalibrationError::Provider)?;
    let mut managed = crate::provider::managed_llama::ManagedLlamaAdapter::discover_verified()
        .map_err(CalibrationError::Provider)?;
    let mut host = SystemHostProbe;
    CalibrationRunner::run_and_persist(
        &mut host,
        &mut managed,
        &mut attached,
        &crate::evidence::evidence_dir(),
        &crate::evidence::selection_path(),
    )
}

type Execution = (
    CandidateQualification,
    CandidateQualification,
    (
        Vec<QualificationCaseResult>,
        Vec<QualificationCaseResult>,
        Vec<MeasurementEvidence>,
        Vec<IsolationObservation>,
    ),
);
fn execute(
    host: &mut dyn HostProbe,
    a: &mut dyn ProviderAdapter,
    b: &mut dyn ProviderAdapter,
) -> Result<Execution, CalibrationError> {
    let a_identity = a.inspect_candidate().map_err(CalibrationError::Provider)?;
    let b_identity = b.inspect_candidate().map_err(CalibrationError::Provider)?;
    let a_fingerprint = a_identity.fingerprint();
    let b_fingerprint = b_identity.fingerprint();
    let (a_cases, a_qualification_teardown_failed) = qualify_isolated(a);
    let (b_cases, b_qualification_teardown_failed) = qualify_isolated(b);
    let mut aq = qualification(&a_identity.candidate_id, &a_cases);
    let mut bq = qualification(&b_identity.candidate_id, &b_cases);
    let mut raw = Vec::new();
    let mut a_cold = None;
    let mut b_cold = None;
    let a_run = if aq.passed && !a_qualification_teardown_failed {
        a.prepare_controlled_run().ok()
    } else {
        None
    };
    let b_run = if bq.passed && !b_qualification_teardown_failed {
        b.prepare_controlled_run().ok()
    } else {
        None
    };
    if aq.passed && a_run.is_none() {
        aq.passed = false;
        aq.reasons.push("performance_prepare_failed".into());
    }
    if bq.passed && b_run.is_none() {
        bq.passed = false;
        bq.reasons.push("performance_prepare_failed".into());
    }
    if aq.passed {
        let warmup = measure(
            host,
            a,
            a_run.as_ref().expect("prepared performance run"),
            &a_fingerprint,
            0,
            0,
            false,
        );
        a_cold = warmup.cold_readiness_duration_ns;
        if warmup.failure_code.is_some() {
            aq.passed = false;
            aq.reasons.push("warmup_failed".into());
        }
        raw.push(warmup);
    }
    if bq.passed {
        let warmup = measure(
            host,
            b,
            b_run.as_ref().expect("prepared performance run"),
            &b_fingerprint,
            0,
            0,
            false,
        );
        b_cold = warmup.cold_readiness_duration_ns;
        if warmup.failure_code.is_some() {
            bq.passed = false;
            bq.reasons.push("warmup_failed".into());
        }
        raw.push(warmup);
    }
    for rep in 1..=MEASURED_PAIRS {
        let positions = if rep % 2 == 1 { (1, 2) } else { (2, 1) };
        let a_first = rep % 2 == 1;
        for choose_a in [a_first, !a_first] {
            if choose_a && aq.passed {
                let mut am = measure(
                    host,
                    a,
                    a_run.as_ref().expect("prepared performance run"),
                    &a_fingerprint,
                    rep,
                    positions.0,
                    true,
                );
                if rep == 1 {
                    am.cold_readiness_duration_ns = a_cold;
                }
                aq.measured_repetitions.push(selector_measurement(rep, &am));
                raw.push(am);
            } else if !choose_a && bq.passed {
                let mut bm = measure(
                    host,
                    b,
                    b_run.as_ref().expect("prepared performance run"),
                    &b_fingerprint,
                    rep,
                    positions.1,
                    true,
                );
                if rep == 1 {
                    bm.cold_readiness_duration_ns = b_cold;
                }
                bq.measured_repetitions.push(selector_measurement(rep, &bm));
                raw.push(bm);
            }
        }
    }
    let mut lifecycle = Vec::new();
    let a_teardown_failed = a_qualification_teardown_failed
        || a_run.is_some_and(|run| a.finish_controlled_run(run).is_err());
    let b_teardown_failed = b_qualification_teardown_failed
        || b_run.is_some_and(|run| b.finish_controlled_run(run).is_err());
    if a_teardown_failed {
        lifecycle.push(isolation_observation("candidate_a_teardown_failed", false));
    }
    if b_teardown_failed {
        lifecycle.push(isolation_observation("candidate_b_teardown_failed", false));
    }
    Ok((aq, bq, (a_cases, b_cases, raw, lifecycle)))
}

#[cfg(test)]
fn with_run<T>(
    provider: &mut dyn ProviderAdapter,
    operation: impl FnOnce(&mut dyn ProviderAdapter, &ControlledRun) -> Result<T, CalibrationError>,
) -> Result<T, CalibrationError> {
    let run = provider
        .prepare_controlled_run()
        .map_err(CalibrationError::Provider)?;
    let result = operation(provider, &run);
    let finish = provider
        .finish_controlled_run(run)
        .map_err(CalibrationError::Provider);
    match (result, finish) {
        (Err(operation), Err(CalibrationError::Provider(teardown))) => {
            Err(CalibrationError::OperationAndTeardown {
                operation: Box::new(operation),
                teardown,
            })
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn qualify(
    provider: &mut dyn ProviderAdapter,
    run: &ControlledRun,
) -> Vec<QualificationCaseResult> {
    workload::qualification_cases()
        .into_iter()
        .map(
            |case| match provider.run_turn(run, &messages(&case.request.messages)) {
                Ok(turn) => workload::validate_qualification_response(&case, &turn.content),
                Err(e) => QualificationCaseResult {
                    schema_version: 1,
                    case_id: case.id.into(),
                    passed: false,
                    reason: Some(provider_code(&e).into()),
                },
            },
        )
        .collect()
}
fn qualify_isolated(provider: &mut dyn ProviderAdapter) -> (Vec<QualificationCaseResult>, bool) {
    let run = match provider.prepare_controlled_run() {
        Ok(run) => run,
        Err(error) => return (failed_qualification_cases(provider_code(&error)), false),
    };
    let cases = qualify(provider, &run);
    let teardown_failed = provider.finish_controlled_run(run).is_err();
    (cases, teardown_failed)
}
fn failed_qualification_cases(code: &str) -> Vec<QualificationCaseResult> {
    workload::qualification_cases()
        .into_iter()
        .map(|case| QualificationCaseResult {
            schema_version: 1,
            case_id: case.id.into(),
            passed: false,
            reason: Some(code.into()),
        })
        .collect()
}

fn measure(
    host: &mut dyn HostProbe,
    provider: &mut dyn ProviderAdapter,
    run: &ControlledRun,
    candidate_fingerprint: &str,
    repetition: u8,
    order_position: u8,
    retain: bool,
) -> MeasurementEvidence {
    let managed_readiness = provider.cold_readiness_duration_ns(run);
    let before = host.available_memory_bytes();
    let background_before = host.background_code();
    let thermal_before = host.thermal_code();
    let started = Instant::now();
    let mut timing = None;
    let mut failure = None;
    let scenario = workload::performance_scenario_v1();
    let mut transcript = scenario.turns[0].messages.clone();
    for step in 0..3 {
        match provider.run_turn(run, &messages(&transcript)) {
            Ok(t) => {
                timing = Some(merge_timing(timing.take(), t.timing));
                let action = match workload::parse_canonical_action(&t.content) {
                    Ok(action) => action,
                    Err(code) => {
                        failure = Some(code.into());
                        break;
                    }
                };
                transcript.push(workload::CanonicalMessage {
                    role: "assistant".into(),
                    content: t.content,
                });
                match (step, action) {
                    (0, workload::CanonicalAction::Tool { tool, arguments })
                        if tool == "lookup_record"
                            && arguments == serde_json::json!({"record_id":"R-104"}) =>
                    {
                        transcript.push(workload::CanonicalMessage {
                            role: "tool".into(),
                            content: workload::lookup_record("R-104")
                                .expect("embedded fixture")
                                .to_string(),
                        });
                    }
                    (1, workload::CanonicalAction::Tool { tool, arguments })
                        if tool == "get_record_status"
                            && arguments == serde_json::json!({"record_id":"R-104"}) =>
                    {
                        transcript.push(workload::CanonicalMessage {
                            role: "tool".into(),
                            content: workload::get_record_status("R-104")
                                .expect("embedded fixture")
                                .to_string(),
                        });
                    }
                    (2, workload::CanonicalAction::Answer { answer })
                        if answer == "R-104 is active." => {}
                    _ => {
                        failure = Some("performance_action_mismatch".into());
                        break;
                    }
                }
            }
            Err(e) => {
                failure = Some(provider_code(&e).into());
                break;
            }
        }
    }
    let duration = started.elapsed().as_nanos().try_into().ok();
    let after = host.available_memory_bytes();
    let background_code = host.background_code();
    let thermal_code = host.thermal_code();
    let isolation_controlled = background_before == "background_load_controlled"
        && thermal_before.as_deref() == Some("thermal_nominal")
        && background_code == "background_load_controlled"
        && thermal_code.as_deref() == Some("thermal_nominal");
    if !isolation_controlled {
        failure = Some("isolation_lost".into());
    }
    MeasurementEvidence {
        schema_version: 1,
        candidate_fingerprint: candidate_fingerprint.into(),
        repetition,
        order_position,
        cold_readiness_duration_ns: (!retain)
            .then(|| {
                timing
                    .as_ref()
                    .and_then(|t| t.load_duration_ns)
                    .or(managed_readiness)
            })
            .flatten(),
        end_to_end_duration_ns: (failure.is_none() && retain).then_some(duration).flatten(),
        outcome_code: if failure.is_none() {
            "success"
        } else {
            "failure"
        }
        .into(),
        failure_code: failure,
        prompt_token_count: timing.as_ref().and_then(|t| t.prompt_eval_count),
        output_token_count: timing.as_ref().and_then(|t| t.eval_count),
        prompt_eval_duration_ns: timing.as_ref().and_then(|t| t.prompt_eval_duration_ns),
        decode_duration_ns: timing.as_ref().and_then(|t| t.eval_duration_ns),
        host_available_memory_delta_bytes: Some(
            i128::from(after)
                .saturating_sub(i128::from(before))
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        ),
        background_observation_code: background_code,
        thermal_observation_code: thermal_code,
        isolation_controlled,
    }
}

fn merge_timing(
    previous: Option<crate::provider::ProviderTiming>,
    next: crate::provider::ProviderTiming,
) -> crate::provider::ProviderTiming {
    fn add(a: Option<u64>, b: Option<u64>) -> Option<u64> {
        match (a, b) {
            (Some(a), Some(b)) => a.checked_add(b),
            _ => None,
        }
    }
    let Some(previous) = previous else {
        return next;
    };
    crate::provider::ProviderTiming {
        schema_version: 1,
        total_duration_ns: add(previous.total_duration_ns, next.total_duration_ns),
        load_duration_ns: add(previous.load_duration_ns, next.load_duration_ns),
        prompt_eval_count: add(previous.prompt_eval_count, next.prompt_eval_count),
        prompt_eval_duration_ns: add(
            previous.prompt_eval_duration_ns,
            next.prompt_eval_duration_ns,
        ),
        eval_count: add(previous.eval_count, next.eval_count),
        eval_duration_ns: add(previous.eval_duration_ns, next.eval_duration_ns),
    }
}

fn qualification(id: &str, cases: &[QualificationCaseResult]) -> CandidateQualification {
    CandidateQualification {
        schema_version: 1,
        candidate_id: id.into(),
        passed: cases.len() == 5 && cases.iter().all(|c| c.passed),
        reasons: cases
            .iter()
            .filter_map(|c| c.reason.as_ref().map(|r| format!("{}_{}", c.case_id, r)))
            .collect(),
        measured_repetitions: vec![],
    }
}
fn selector_measurement(rep: u8, m: &MeasurementEvidence) -> MeasuredRepetition {
    MeasuredRepetition {
        schema_version: 1,
        repetition: rep,
        end_to_end_duration_ns: m.end_to_end_duration_ns,
    }
}
fn qualification_evidence(
    fingerprint: &str,
    q: &CandidateQualification,
    cases: &[QualificationCaseResult],
) -> QualificationEvidence {
    QualificationEvidence {
        schema_version: 1,
        candidate_fingerprint: fingerprint.into(),
        case_results: cases.to_vec(),
        failure_codes: q.reasons.clone(),
    }
}
fn candidate_evidence(identity: crate::provider::CandidateSpec) -> CandidateEvidence {
    CandidateEvidence {
        schema_version: 1,
        fingerprint: identity.fingerprint(),
        identity,
    }
}
#[allow(clippy::too_many_arguments)]
fn base_evidence(
    started_at_unix_ms: u64,
    host: HostFingerprint,
    a: crate::provider::CandidateSpec,
    b: crate::provider::CandidateSpec,
    qualifications: Vec<QualificationEvidence>,
    measurements: Vec<MeasurementEvidence>,
    isolation_observations: Vec<IsolationObservation>,
    verdict: EvidenceVerdict,
    explanation_codes: Vec<String>,
) -> CalibrationEvidence {
    CalibrationEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        protocol_version: "calibration-v1".into(),
        workload_version: workload::WORKLOAD_VERSION.into(),
        policy_version: "selector-v1".into(),
        started_at_unix_ms,
        ended_at_unix_ms: now_ms(),
        host,
        candidates: [candidate_evidence(a), candidate_evidence(b)],
        disclosed_differences: vec![
            difference("provider_kind", "managed_llama_cpp", "attached_ollama"),
            difference("artifact_reference", "registry_artifact", "ollama_tag"),
            difference(
                "artifact_digest",
                "managed_registry_digest",
                "ollama_manifest_digest",
            ),
            difference("engine_kind", "llama_cpp", "ollama_managed_gguf_engine"),
            difference(
                "engine_version",
                "managed_binary_version",
                "ollama_provider_version",
            ),
            difference(
                "engine_revision",
                "managed_binary_digest",
                "revision_unknown",
            ),
            difference("ownership_lifecycle", "loxa_managed", "owner_attached"),
            difference("cache_residency_regime", "controlled_warm", "attached_warm"),
            difference(
                "performance_residency_window",
                "both_candidates_resident",
                "both_candidates_resident",
            ),
            difference(
                "native_metrics_comparability",
                "openai_usage_task_aggregate_diagnostic",
                "ollama_usage_task_aggregate_diagnostic",
            ),
        ],
        qualifications,
        measurements,
        isolation_observations,
        verdict,
        explanation_codes,
    }
}

#[allow(clippy::too_many_arguments)]
fn aborted_outcome(
    started: u64,
    host: HostFingerprint,
    a: crate::provider::CandidateSpec,
    b: crate::provider::CandidateSpec,
    isolation: Vec<IsolationObservation>,
    qualifications: Vec<QualificationEvidence>,
    measurements: Vec<MeasurementEvidence>,
    code: &str,
) -> Result<CalibrationOutcome, CalibrationError> {
    let verdict = SelectorVerdict::NoVerifiedPlan {
        schema_version: 1,
        reasons: vec![format!("{}: {code}", a.candidate_id)],
    };
    let evidence = base_evidence(
        started,
        host,
        a,
        b,
        qualifications,
        measurements,
        isolation,
        EvidenceVerdict::from_selector(&verdict)?,
        vec![code.into()],
    );
    Ok(CalibrationOutcome {
        evidence,
        evidence_path: None,
        verdict,
    })
}

fn calibration_error_code(error: &CalibrationError) -> &'static str {
    match error {
        CalibrationError::Isolation(_) => "isolation_failed",
        CalibrationError::Provider(error) => provider_code(error),
        CalibrationError::IdentityChanged => "identity_changed",
        CalibrationError::Evidence(_) => "evidence_error",
        CalibrationError::OperationAndTeardown { .. } => "operation_and_teardown_failed",
        CalibrationError::Aborted { .. } => "calibration_aborted",
    }
}
fn difference(code: &str, a: &str, b: &str) -> DisclosedDifference {
    DisclosedDifference {
        schema_version: 1,
        difference_code: code.into(),
        candidate_a_fact: a.into(),
        candidate_b_fact: b.into(),
    }
}
fn isolation_observation(code: &str, controlled: bool) -> IsolationObservation {
    IsolationObservation {
        schema_version: 1,
        check_code: code.into(),
        outcome_code: if controlled {
            "controlled"
        } else {
            "uncontrolled"
        }
        .into(),
        observed_fact: if controlled {
            "requirement_satisfied"
        } else {
            "requirement_failed"
        }
        .into(),
    }
}
fn messages(ms: &[workload::CanonicalMessage]) -> Vec<ProviderMessage> {
    ms.iter()
        .map(|m| ProviderMessage {
            role: match m.role.as_str() {
                "system" => crate::provider::ProviderRole::System,
                "assistant" => crate::provider::ProviderRole::Assistant,
                "tool" => crate::provider::ProviderRole::Tool,
                _ => crate::provider::ProviderRole::User,
            },
            content: m.content.clone(),
        })
        .collect()
}
fn provider_code(e: &ProviderError) -> &'static str {
    match e {
        ProviderError::Timeout => "timeout",
        ProviderError::Unreachable => "provider_unreachable",
        ProviderError::IdentityMismatch(_) => "identity_mismatch",
        ProviderError::Lifecycle(_) => "process_crash",
        _ => "provider_error",
    }
}
fn verdict_code(v: &SelectorVerdict) -> &'static str {
    match v {
        SelectorVerdict::Selected { .. } => "candidate_selected",
        SelectorVerdict::NoVerifiedPlan { .. } => "no_verified_plan",
        SelectorVerdict::NoMaterialWinner { .. } => "no_material_winner",
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn sanitize_fact(s: &str) -> String {
    s.to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || "_-.:".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}
fn selection_for(e: &CalibrationEvidence, v: &SelectorVerdict) -> Option<SelectionRecord> {
    let (id, disposition) = match v {
        SelectorVerdict::Selected { candidate_id, .. } => {
            (candidate_id, SelectionDisposition::Selected)
        }
        SelectorVerdict::NoMaterialWinner {
            baseline_candidate_id,
            ..
        } => (
            baseline_candidate_id,
            SelectionDisposition::RetainedBaseline,
        ),
        SelectorVerdict::NoVerifiedPlan { .. } => return None,
    };
    let c = e
        .candidates
        .iter()
        .find(|c| c.identity.candidate_id == *id)?;
    Some(SelectionRecord {
        schema_version: SELECTION_SCHEMA_VERSION,
        recorded_at_unix_ms: e.ended_at_unix_ms,
        evidence_schema_version: e.schema_version,
        candidate_id: id.clone(),
        candidate_fingerprint: c.fingerprint.clone(),
        disposition,
        reason_code: match EvidenceVerdict::from_selector(v).ok()? {
            EvidenceVerdict::Selected { reason_code, .. }
            | EvidenceVerdict::NoMaterialWinner { reason_code, .. } => reason_code,
            _ => return None,
        },
    })
}

fn check_provider_preflight(
    label: &str,
    provider: &mut dyn ProviderAdapter,
    isolation: &mut Vec<IsolationObservation>,
) -> Result<(), CalibrationError> {
    let health = provider
        .verify_health()
        .map_err(CalibrationError::Provider)?;
    isolation.push(isolation_observation(
        &format!("{label}_health"),
        health.healthy,
    ));
    let activity = provider
        .observe_activity()
        .map_err(CalibrationError::Provider)?;
    let controlled = activity.unrelated_activity.is_empty() && !activity.target_active;
    isolation.push(isolation_observation(
        &format!("{label}_activity"),
        controlled,
    ));
    if !health.healthy || !controlled {
        return Err(CalibrationError::Isolation(vec![format!(
            "{label}_uncontrolled"
        )]));
    }
    Ok(())
}

fn check_provider_activity(
    label: &str,
    provider: &dyn ProviderAdapter,
    isolation: &mut Vec<IsolationObservation>,
) -> Result<(), CalibrationError> {
    let activity = provider
        .observe_activity()
        .map_err(CalibrationError::Provider)?;
    let controlled = activity.unrelated_activity.is_empty() && !activity.target_active;
    isolation.push(isolation_observation(
        &format!("{label}_activity"),
        controlled,
    ));
    if !controlled {
        return Err(CalibrationError::Isolation(vec![format!(
            "{label}_uncontrolled"
        )]));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        managed_llama::managed_candidate_spec, ArtifactIdentity, CandidateSpec, EngineIdentity,
        EngineRevision, GenerationSettings, NormalizedTurn, ProviderActivityObservation,
        ProviderHealth, ProviderKind, ProviderOwnership, ProviderTiming,
    };
    use std::collections::VecDeque;
    use std::fs;

    struct CleanupProvider {
        active: bool,
        finishes: usize,
    }
    impl ProviderAdapter for CleanupProvider {
        fn inspect_candidate(&self) -> Result<crate::provider::CandidateSpec, ProviderError> {
            Err(ProviderError::Unreachable)
        }
        fn prepare_controlled_run(&mut self) -> Result<ControlledRun, ProviderError> {
            assert!(!self.active);
            self.active = true;
            Ok(ControlledRun {
                schema_version: 1,
                run_id: "test".into(),
            })
        }
        fn run_turn(
            &mut self,
            _: &ControlledRun,
            _: &[ProviderMessage],
        ) -> Result<NormalizedTurn, ProviderError> {
            Ok(NormalizedTurn {
                schema_version: 1,
                content: String::new(),
                timing: ProviderTiming::default(),
            })
        }
        fn finish_controlled_run(&mut self, _: ControlledRun) -> Result<(), ProviderError> {
            assert!(self.active);
            self.active = false;
            self.finishes += 1;
            Ok(())
        }
    }
    #[test]
    fn pinned_counts_are_exact() {
        assert_eq!((WARMUPS, MEASURED_PAIRS), (1, 5));
    }
    #[test]
    fn selection_is_absent_for_no_verified_plan() {
        let v = SelectorVerdict::NoVerifiedPlan {
            schema_version: 1,
            reasons: vec!["a: qualification_failed".into()],
        };
        assert_eq!(verdict_code(&v), "no_verified_plan");
    }

    #[test]
    fn controlled_run_is_finished_when_operation_fails() {
        let mut provider = CleanupProvider {
            active: false,
            finishes: 0,
        };
        let result: Result<(), CalibrationError> =
            with_run(&mut provider, |_, _| Err(CalibrationError::IdentityChanged));
        assert!(matches!(result, Err(CalibrationError::IdentityChanged)));
        assert!(!provider.active);
        assert_eq!(provider.finishes, 1);
    }

    struct ScriptedProvider {
        spec: CandidateSpec,
        responses: VecDeque<Result<NormalizedTurn, ProviderError>>,
        prepares: usize,
        finishes: usize,
        turns: usize,
        drift_after_turns: Option<usize>,
        fail_first_finish: bool,
    }

    impl ProviderAdapter for ScriptedProvider {
        fn inspect_candidate(&self) -> Result<CandidateSpec, ProviderError> {
            let mut spec = self.spec.clone();
            if self.drift_after_turns.is_some_and(|n| self.turns >= n) {
                spec.engine.provider_version.push_str("-drift");
                spec.engine.invalidation_keys =
                    vec![format!("provider_version={}", spec.engine.provider_version)];
            }
            Ok(spec)
        }
        fn verify_health(&mut self) -> Result<ProviderHealth, ProviderError> {
            Ok(ProviderHealth {
                schema_version: 1,
                healthy: true,
                provider_version: Some("test".into()),
                evidence: vec!["fixture".into()],
            })
        }
        fn observe_activity(&self) -> Result<ProviderActivityObservation, ProviderError> {
            Ok(ProviderActivityObservation {
                schema_version: 1,
                target_active: false,
                unrelated_activity: vec![],
                evidence: vec!["fixture".into()],
            })
        }
        fn prepare_controlled_run(&mut self) -> Result<ControlledRun, ProviderError> {
            self.prepares += 1;
            Ok(ControlledRun {
                schema_version: 1,
                run_id: format!("run-{}", self.prepares),
            })
        }
        fn run_turn(
            &mut self,
            _: &ControlledRun,
            _: &[ProviderMessage],
        ) -> Result<NormalizedTurn, ProviderError> {
            self.turns += 1;
            self.responses
                .pop_front()
                .unwrap_or_else(|| Ok(turn("{\"action\":\"answer\",\"answer\":\"done\"}", None)))
        }
        fn finish_controlled_run(&mut self, _: ControlledRun) -> Result<(), ProviderError> {
            self.finishes += 1;
            if self.fail_first_finish && self.finishes == 1 {
                return Err(ProviderError::Lifecycle("fixture teardown".into()));
            }
            Ok(())
        }
    }

    struct TestHost {
        controlled: bool,
    }
    impl HostProbe for TestHost {
        fn preflight(&mut self) -> Vec<PreflightObservation> {
            vec![PreflightObservation {
                code: "fixture_isolation".into(),
                controlled: self.controlled,
            }]
        }
        fn available_memory_bytes(&mut self) -> u64 {
            8 << 30
        }
        fn fingerprint(&mut self) -> HostFingerprint {
            HostFingerprint {
                schema_version: 1,
                os_name: "test_os".into(),
                os_version: "1".into(),
                hardware_model: "test_mac".into(),
                physical_cores: 4,
                logical_cores: 4,
                memory_total_bytes: 16 << 30,
                memory_available_bytes: 8 << 30,
                root_disk_total_bytes: Some(100 << 30),
                root_disk_available_bytes: Some(50 << 30),
            }
        }
        fn background_code(&mut self) -> String {
            "background_load_controlled".into()
        }
        fn thermal_code(&mut self) -> Option<String> {
            Some("thermal_nominal".into())
        }
    }

    struct IsolationLossHost {
        background_calls: usize,
    }

    struct ThermalRefusalHost;
    impl HostProbe for ThermalRefusalHost {
        fn preflight(&mut self) -> Vec<PreflightObservation> {
            vec![PreflightObservation {
                code: "thermal_probe_parse_changed".into(),
                controlled: false,
            }]
        }
        fn available_memory_bytes(&mut self) -> u64 {
            8 << 30
        }
        fn fingerprint(&mut self) -> HostFingerprint {
            TestHost { controlled: true }.fingerprint()
        }
    }
    impl HostProbe for IsolationLossHost {
        fn preflight(&mut self) -> Vec<PreflightObservation> {
            vec![PreflightObservation {
                code: "fixture_isolation".into(),
                controlled: true,
            }]
        }
        fn available_memory_bytes(&mut self) -> u64 {
            8 << 30
        }
        fn fingerprint(&mut self) -> HostFingerprint {
            TestHost { controlled: true }.fingerprint()
        }
        fn background_code(&mut self) -> String {
            self.background_calls += 1;
            if self.background_calls == 2 {
                "background_load_uncontrolled"
            } else {
                "background_load_controlled"
            }
            .into()
        }
        fn thermal_code(&mut self) -> Option<String> {
            Some("thermal_nominal".into())
        }
    }

    fn ollama_spec() -> CandidateSpec {
        CandidateSpec {
            schema_version: 1,
            candidate_id: "ollama-gemma3-4b-it-q4-k-m".into(),
            provider_kind: ProviderKind::Ollama,
            ownership: ProviderOwnership::Attached,
            endpoint: "http://127.0.0.1:11434".into(),
            artifact: ArtifactIdentity {
                schema_version: 1,
                artifact_id: "gemma3:4b-it-q4_K_M".into(),
                digest_sha256: "a2af6cc3eb7fa8be8504abaf9b04e88f17a119ec3f04a3addf55f92841195f5a"
                    .into(),
                base_checkpoint: "google/gemma-3-4b-it".into(),
                format: "gguf".into(),
                quantization: "Q4_K_M".into(),
                tokenizer_evidence: vec!["fixture".into()],
                template_evidence: vec!["fixture".into()],
            },
            engine: EngineIdentity {
                schema_version: 1,
                engine_kind: "ollama-managed-gguf-engine".into(),
                provider_version: "test".into(),
                engine_revision: EngineRevision::Known("fixture-observed-revision".into()),
                evidence: vec!["ollama_api_show:engine_revision=fixture-observed-revision".into()],
                invalidation_keys: vec![
                    "provider_version=test".into(),
                    "engine_revision=fixture-observed-revision".into(),
                ],
            },
            settings: GenerationSettings::pinned_v1(),
        }
    }
    fn turn(content: &str, duration: Option<u64>) -> NormalizedTurn {
        NormalizedTurn {
            schema_version: 1,
            content: content.into(),
            timing: ProviderTiming {
                schema_version: 1,
                total_duration_ns: duration,
                ..ProviderTiming::default()
            },
        }
    }
    fn successful_script(duration: u64) -> VecDeque<Result<NormalizedTurn, ProviderError>> {
        let mut out = workload::qualification_cases()
            .into_iter()
            .map(|c| {
                Ok(turn(
                    &serde_json::to_string(&c.expected_action).unwrap(),
                    None,
                ))
            })
            .collect::<VecDeque<_>>();
        for _ in 0..6 {
            out.push_back(Ok(turn("{\"action\":\"tool\",\"tool\":\"lookup_record\",\"arguments\":{\"record_id\":\"R-104\"}}", Some(duration))));
            out.push_back(Ok(turn("{\"action\":\"tool\",\"tool\":\"get_record_status\",\"arguments\":{\"record_id\":\"R-104\"}}", Some(duration))));
            out.push_back(Ok(turn(
                "{\"action\":\"answer\",\"answer\":\"R-104 is active.\"}",
                Some(duration),
            )));
        }
        out
    }
    fn provider(spec: CandidateSpec, duration: u64) -> ScriptedProvider {
        ScriptedProvider {
            spec,
            responses: successful_script(duration),
            prepares: 0,
            finishes: 0,
            turns: 0,
            drift_after_turns: None,
            fail_first_finish: false,
        }
    }

    #[test]
    fn calibration_runs_one_warmup_then_five_counterbalanced_pairs_at_concurrency_one() {
        let mut host = TestHost { controlled: true };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        let a_fingerprint = a.spec.fingerprint();
        let b_fingerprint = b.spec.fingerprint();
        assert_eq!(
            (a.spec.settings.concurrency, b.spec.settings.concurrency),
            (1, 1)
        );
        let outcome = CalibrationRunner::run(&mut host, &mut a, &mut b).unwrap();
        assert_eq!(outcome.evidence.qualifications.len(), 2);
        assert!(outcome
            .evidence
            .qualifications
            .iter()
            .all(|q| q.case_results.len() == 5));
        assert_eq!((a.turns, b.turns), (23, 23));
        assert_eq!((a.prepares, b.prepares), (2, 2));
        assert_eq!((a.finishes, b.finishes), (2, 2));
        assert_eq!(outcome.evidence.measurements.len(), 12);

        let warmups = outcome
            .evidence
            .measurements
            .iter()
            .filter(|measurement| measurement.repetition == 0)
            .map(|measurement| {
                (
                    measurement.candidate_fingerprint.as_str(),
                    measurement.order_position,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            warmups,
            vec![(a_fingerprint.as_str(), 0), (b_fingerprint.as_str(), 0)]
        );

        let measured = outcome
            .evidence
            .measurements
            .iter()
            .filter(|measurement| measurement.repetition > 0)
            .collect::<Vec<_>>();
        assert!(measured.len() >= 10);
        assert_eq!(measured.len(), usize::from(MEASURED_PAIRS) * 2);
        assert!(measured.chunks_exact(2).all(|pair| {
            pair[0].repetition == pair[1].repetition
                && pair[0].order_position == 1
                && pair[1].order_position == 2
        }));
        let candidate_order = measured
            .iter()
            .map(|measurement| measurement.candidate_fingerprint.as_str())
            .collect::<Vec<_>>();
        let expected_order = (1..=MEASURED_PAIRS)
            .flat_map(|repetition| {
                if repetition % 2 == 1 {
                    [a_fingerprint.as_str(), b_fingerprint.as_str()]
                } else {
                    [b_fingerprint.as_str(), a_fingerprint.as_str()]
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(candidate_order, expected_order);
    }

    #[test]
    fn raw_failure_and_missing_metrics_are_retained_without_zero_fill() {
        let mut host = TestHost { controlled: true };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        b.responses[8] = Err(ProviderError::Timeout);
        let outcome = CalibrationRunner::run(&mut host, &mut a, &mut b).unwrap();
        let failed = outcome
            .evidence
            .measurements
            .iter()
            .find(|m| m.failure_code.as_deref() == Some("timeout"))
            .unwrap();
        assert_eq!(failed.end_to_end_duration_ns, None);
        assert_eq!(failed.prompt_token_count, None);
        assert_eq!(failed.decode_duration_ns, None);
    }

    #[test]
    fn identity_drift_aborts_selection_and_finishes_every_prepared_run() {
        let mut host = TestHost { controlled: true };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        b.drift_after_turns = Some(1);
        let outcome = CalibrationRunner::run(&mut host, &mut a, &mut b).unwrap();
        assert!(matches!(
            outcome.verdict,
            SelectorVerdict::NoVerifiedPlan { .. }
        ));
        assert_eq!(outcome.evidence.measurements.len(), 12);
        assert!(outcome
            .evidence
            .explanation_codes
            .contains(&"identity_changed".into()));
        assert_eq!(a.prepares, a.finishes);
        assert_eq!(b.prepares, b.finishes);
    }

    #[test]
    fn uncontrolled_preflight_persists_abort_evidence_before_inference_without_selection() {
        let root = std::env::temp_dir().join(format!("loxa-calibration-abort-{}", now_ms()));
        let evidence_dir = root.join("evidence");
        let selection = root.join("selection.json");
        let mut host = TestHost { controlled: false };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        let error = CalibrationRunner::run_and_persist(
            &mut host,
            &mut a,
            &mut b,
            &evidence_dir,
            &selection,
        )
        .unwrap_err();
        let CalibrationError::Aborted { evidence_path, .. } = error else {
            panic!("expected typed abort")
        };
        let evidence =
            crate::evidence::read_evidence_json(&fs::read(&evidence_path).unwrap()).unwrap();
        assert!(evidence
            .isolation_observations
            .iter()
            .any(|o| o.outcome_code == "uncontrolled"));
        assert!(evidence
            .explanation_codes
            .contains(&"isolation_preflight_failed".into()));
        assert_eq!(evidence.candidates[0].identity, a.spec);
        assert_eq!(evidence.candidates[1].identity, b.spec);
        assert_eq!(
            evidence.candidates[0].fingerprint,
            evidence.candidates[0].identity.fingerprint()
        );
        assert_eq!(
            evidence.candidates[1].fingerprint,
            evidence.candidates[1].identity.fingerprint()
        );
        assert!(evidence_path.is_file());
        assert!(!selection.exists());
        assert_eq!((a.turns, b.turns), (0, 0));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn thermal_output_has_bounded_nominal_throttled_and_parse_changed_codes() {
        assert_eq!(
            parse_macos_thermal_output("CPU_Speed_Limit = 100\nScheduler_Limit = 100\n"),
            "thermal_nominal"
        );
        assert_eq!(
            parse_macos_thermal_output("CPU_Speed_Limit = 80\nScheduler_Limit = 100\n"),
            "thermal_throttled"
        );
        assert_eq!(
            parse_macos_thermal_output("CPU_Speed_Limit = 100\nScheduler_Limit = 70\n"),
            "thermal_throttled"
        );
        assert_eq!(
            parse_macos_thermal_output("CPU_Speed_Limit = 80\n"),
            "thermal_throttled"
        );
        assert_eq!(
            parse_macos_thermal_output("pmset output changed"),
            "thermal_probe_parse_changed"
        );
    }

    #[test]
    fn thermal_refusal_persists_and_surfaces_the_exact_bounded_reason() {
        let root = std::env::temp_dir().join(format!("loxa-thermal-abort-{}", now_ms()));
        let mut host = ThermalRefusalHost;
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        let error = CalibrationRunner::run_and_persist(
            &mut host,
            &mut a,
            &mut b,
            &root.join("evidence"),
            &root.join("selection.json"),
        )
        .unwrap_err();
        let CalibrationError::Aborted {
            kind,
            evidence_path,
        } = error
        else {
            panic!("expected typed abort")
        };
        assert_eq!(kind, "thermal_probe_parse_changed");
        let evidence =
            crate::evidence::read_evidence_json(&fs::read(evidence_path).unwrap()).unwrap();
        assert_eq!(
            evidence.explanation_codes,
            vec![
                "thermal_probe_parse_changed".to_owned(),
                "isolation_preflight_failed".to_owned()
            ]
        );
        assert_eq!((a.turns, b.turns), (0, 0));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn persistence_orders_evidence_before_selected_record() {
        let root = std::env::temp_dir().join(format!("loxa-calibration-{}", now_ms()));
        let evidence_dir = root.join("evidence");
        let selection = root.join("selection.json");
        let mut host = TestHost { controlled: true };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        let outcome = CalibrationRunner::run_and_persist(
            &mut host,
            &mut a,
            &mut b,
            &evidence_dir,
            &selection,
        )
        .unwrap();
        assert!(outcome.evidence_path.as_ref().is_some_and(|p| p.is_file()));
        assert!(selection.is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn qualification_failures_survive_teardown_failure_as_separate_fact() {
        let mut host = TestHost { controlled: true };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        for response in a.responses.iter_mut().take(5) {
            *response = Err(ProviderError::Timeout);
        }
        for response in b.responses.iter_mut().take(5) {
            *response = Err(ProviderError::Timeout);
        }
        a.fail_first_finish = true;
        let outcome = CalibrationRunner::run(&mut host, &mut a, &mut b).unwrap();
        assert!(matches!(
            outcome.verdict,
            SelectorVerdict::NoVerifiedPlan { .. }
        ));
        assert!(outcome.evidence.qualifications[0]
            .case_results
            .iter()
            .all(|case| case.reason.as_deref() == Some("timeout")));
        assert!(outcome
            .evidence
            .isolation_observations
            .iter()
            .any(|fact| fact.check_code == "candidate_a_teardown_failed"
                && fact.outcome_code == "uncontrolled"));
        assert!(selection_for(&outcome.evidence, &outcome.verdict).is_none());
    }

    #[test]
    fn isolation_lost_persists_typed_abort_without_selection() {
        let root =
            std::env::temp_dir().join(format!("loxa-calibration-isolation-loss-{}", now_ms()));
        let evidence_dir = root.join("evidence");
        let selection = root.join("selection.json");
        let mut host = IsolationLossHost {
            background_calls: 0,
        };
        let mut a = provider(managed_candidate_spec("test", "rev").unwrap(), 100);
        let mut b = provider(ollama_spec(), 80);
        let error = CalibrationRunner::run_and_persist(
            &mut host,
            &mut a,
            &mut b,
            &evidence_dir,
            &selection,
        )
        .unwrap_err();
        let CalibrationError::Aborted {
            kind,
            evidence_path,
        } = error
        else {
            panic!("expected typed abort")
        };
        assert_eq!(kind, "isolation_lost");
        let evidence =
            crate::evidence::read_evidence_json(&fs::read(evidence_path).unwrap()).unwrap();
        assert!(evidence
            .measurements
            .iter()
            .any(|m| m.failure_code.as_deref() == Some("isolation_lost")));
        assert!(!selection.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
