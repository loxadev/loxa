use crate::provider::{CandidateSpec, CANDIDATE_IDENTITY_SCHEMA_VERSION};
use crate::selector::{SelectorVerdict, SELECTOR_SCHEMA_VERSION};
use crate::workload::{QualificationCaseResult, WORKLOAD_SCHEMA_VERSION, WORKLOAD_VERSION};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const EVIDENCE_SCHEMA_VERSION: u32 = 1;
pub const SELECTION_SCHEMA_VERSION: u32 = 1;
pub const CALIBRATION_PROTOCOL_VERSION: &str = "calibration-v1";
pub const SELECTION_POLICY_VERSION: &str = "selector-v1";

static UNIQUE_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationEvidence {
    pub schema_version: u32,
    pub protocol_version: String,
    pub workload_version: String,
    pub policy_version: String,
    pub started_at_unix_ms: u64,
    pub ended_at_unix_ms: u64,
    pub host: HostFingerprint,
    pub candidates: [CandidateEvidence; 2],
    pub disclosed_differences: Vec<DisclosedDifference>,
    pub qualifications: Vec<QualificationEvidence>,
    pub measurements: Vec<MeasurementEvidence>,
    pub isolation_observations: Vec<IsolationObservation>,
    pub verdict: EvidenceVerdict,
    pub explanation_codes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFingerprint {
    pub schema_version: u32,
    pub os_name: String,
    pub os_version: String,
    pub hardware_model: String,
    pub physical_cores: usize,
    pub logical_cores: usize,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    pub root_disk_total_bytes: Option<u64>,
    pub root_disk_available_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvidence {
    pub schema_version: u32,
    pub identity: CandidateSpec,
    pub fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisclosedDifference {
    pub schema_version: u32,
    pub difference_code: String,
    pub candidate_a_fact: String,
    pub candidate_b_fact: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationEvidence {
    pub schema_version: u32,
    pub candidate_fingerprint: String,
    pub case_results: Vec<QualificationCaseResult>,
    pub failure_codes: Vec<String>,
}

impl QualificationEvidence {
    pub fn passed_current_contract(&self) -> bool {
        self.schema_version == EVIDENCE_SCHEMA_VERSION
            && self.case_results.len() == 5
            && self
                .case_results
                .iter()
                .all(|case| case.schema_version == WORKLOAD_SCHEMA_VERSION && case.passed)
            && self.failure_codes.is_empty()
    }
}

#[cfg(test)]
mod qualification_status_tests {
    use super::*;
    use crate::workload::QualificationCaseResult;

    fn recorded(count: usize) -> QualificationEvidence {
        QualificationEvidence {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            candidate_fingerprint: "a".repeat(64),
            case_results: (0..count)
                .map(|index| QualificationCaseResult {
                    schema_version: WORKLOAD_SCHEMA_VERSION,
                    case_id: format!("case-{index}"),
                    passed: true,
                    reason: None,
                })
                .collect(),
            failure_codes: vec![],
        }
    }

    #[test]
    fn semantic_pass_requires_current_versions_five_passes_and_no_failures() {
        assert!(recorded(5).passed_current_contract());
        assert!(!recorded(4).passed_current_contract());

        let mut failed_case = recorded(5);
        failed_case.case_results[2].passed = false;
        assert!(!failed_case.passed_current_contract());

        let mut failure_code = recorded(5);
        failure_code
            .failure_codes
            .push("qualification_failed".into());
        assert!(!failure_code.passed_current_contract());

        let mut old_evidence = recorded(5);
        old_evidence.schema_version = 0;
        assert!(!old_evidence.passed_current_contract());

        let mut old_workload = recorded(5);
        old_workload.case_results[0].schema_version = 0;
        assert!(!old_workload.passed_current_contract());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementEvidence {
    pub schema_version: u32,
    pub candidate_fingerprint: String,
    pub repetition: u8,
    pub order_position: u8,
    pub cold_readiness_duration_ns: Option<u64>,
    pub end_to_end_duration_ns: Option<u64>,
    pub outcome_code: String,
    pub failure_code: Option<String>,
    pub prompt_token_count: Option<u64>,
    pub output_token_count: Option<u64>,
    pub prompt_eval_duration_ns: Option<u64>,
    pub decode_duration_ns: Option<u64>,
    pub host_available_memory_delta_bytes: Option<i64>,
    pub background_observation_code: String,
    pub thermal_observation_code: Option<String>,
    pub isolation_controlled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolationObservation {
    pub schema_version: u32,
    pub check_code: String,
    pub outcome_code: String,
    pub observed_fact: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvidenceVerdict {
    Selected {
        schema_version: u32,
        candidate_id: String,
        reason_code: String,
    },
    NoVerifiedPlan {
        schema_version: u32,
        reason_codes: Vec<String>,
    },
    NoMaterialWinner {
        schema_version: u32,
        baseline_candidate_id: String,
        reason_code: String,
    },
}

impl EvidenceVerdict {
    pub fn from_selector(verdict: &SelectorVerdict) -> Result<Self, EvidenceError> {
        match verdict {
            SelectorVerdict::Selected {
                schema_version,
                candidate_id,
                reason,
            } => Ok(Self::Selected {
                schema_version: *schema_version,
                candidate_id: candidate_id.clone(),
                reason_code: selector_reason_code(reason)?.into(),
            }),
            SelectorVerdict::NoVerifiedPlan {
                schema_version,
                reasons,
            } => Ok(Self::NoVerifiedPlan {
                schema_version: *schema_version,
                reason_codes: reasons
                    .iter()
                    .map(|reason| selector_qualification_reason_code(reason).map(str::to_owned))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            SelectorVerdict::NoMaterialWinner {
                schema_version,
                baseline_candidate_id,
                reason,
            } => Ok(Self::NoMaterialWinner {
                schema_version: *schema_version,
                baseline_candidate_id: baseline_candidate_id.clone(),
                reason_code: selector_reason_code(reason)?.into(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionDisposition {
    Selected,
    RetainedBaseline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionRecord {
    pub schema_version: u32,
    pub recorded_at_unix_ms: u64,
    pub evidence_schema_version: u32,
    pub candidate_id: String,
    pub candidate_fingerprint: String,
    pub disposition: SelectionDisposition,
    pub reason_code: String,
}

#[derive(Debug)]
pub enum EvidenceError {
    UnsupportedSchema { record: &'static str, version: u32 },
    InvalidRecord(String),
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema { record, version } => {
                write!(formatter, "unsupported {record} schema version: {version}")
            }
            Self::InvalidRecord(detail) => write!(formatter, "invalid evidence record: {detail}"),
            Self::Json(error) => write!(formatter, "evidence JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "evidence persistence failed: {error}"),
        }
    }
}

impl std::error::Error for EvidenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for EvidenceError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<io::Error> for EvidenceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn evidence_dir() -> PathBuf {
    home_dir().join(".loxa").join("evidence")
}

pub fn selection_path() -> PathBuf {
    home_dir().join(".loxa").join("selection.json")
}

fn home_dir() -> PathBuf {
    let current = env::current_dir().unwrap_or_else(|_| env::temp_dir());
    env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| absolute_from(&current, &home))
        .unwrap_or(current)
}

pub fn read_evidence_json(bytes: &[u8]) -> Result<CalibrationEvidence, EvidenceError> {
    let evidence: CalibrationEvidence = serde_json::from_slice(bytes)?;
    validate_evidence(&evidence)?;
    Ok(evidence)
}

pub fn read_selection_json(bytes: &[u8]) -> Result<SelectionRecord, EvidenceError> {
    let selection: SelectionRecord = serde_json::from_slice(bytes)?;
    validate_selection(&selection)?;
    Ok(selection)
}

fn validate_evidence(evidence: &CalibrationEvidence) -> Result<(), EvidenceError> {
    require_schema("evidence", evidence.schema_version, EVIDENCE_SCHEMA_VERSION)?;
    if evidence.protocol_version != CALIBRATION_PROTOCOL_VERSION
        || evidence.workload_version != WORKLOAD_VERSION
        || evidence.policy_version != SELECTION_POLICY_VERSION
    {
        return Err(EvidenceError::InvalidRecord(
            "unsupported protocol, workload, or policy version".into(),
        ));
    }
    if evidence.ended_at_unix_ms < evidence.started_at_unix_ms {
        return Err(EvidenceError::InvalidRecord(
            "end timestamp precedes start timestamp".into(),
        ));
    }
    require_schema(
        "host",
        evidence.host.schema_version,
        EVIDENCE_SCHEMA_VERSION,
    )?;
    reject_forbidden_material(
        "host fingerprint",
        [
            evidence.host.os_name.as_str(),
            evidence.host.os_version.as_str(),
            evidence.host.hardware_model.as_str(),
        ],
    )?;
    let mut fingerprints = Vec::with_capacity(2);
    for candidate in &evidence.candidates {
        require_schema(
            "candidate evidence",
            candidate.schema_version,
            EVIDENCE_SCHEMA_VERSION,
        )?;
        require_schema(
            "candidate identity",
            candidate.identity.schema_version,
            CANDIDATE_IDENTITY_SCHEMA_VERSION,
        )?;
        candidate.identity.validate_pinned().map_err(|error| {
            EvidenceError::InvalidRecord(format!("candidate identity is not pinned: {error}"))
        })?;
        if candidate.fingerprint != candidate.identity.fingerprint() {
            return Err(EvidenceError::InvalidRecord(
                "candidate fingerprint does not match identity".into(),
            ));
        }
        validate_fingerprint("candidate fingerprint", &candidate.fingerprint)?;
        validate_candidate_material(&candidate.identity)?;
        fingerprints.push(candidate.fingerprint.as_str());
    }
    if fingerprints[0] == fingerprints[1] {
        return Err(EvidenceError::InvalidRecord(
            "candidate identities must be distinct".into(),
        ));
    }
    for difference in &evidence.disclosed_differences {
        require_schema(
            "disclosed difference",
            difference.schema_version,
            EVIDENCE_SCHEMA_VERSION,
        )?;
        validate_code("difference code", &difference.difference_code)?;
        validate_code("candidate A disclosed fact", &difference.candidate_a_fact)?;
        validate_code("candidate B disclosed fact", &difference.candidate_b_fact)?;
    }
    for qualification in &evidence.qualifications {
        require_schema(
            "qualification evidence",
            qualification.schema_version,
            EVIDENCE_SCHEMA_VERSION,
        )?;
        validate_fingerprint(
            "qualification candidate fingerprint",
            &qualification.candidate_fingerprint,
        )?;
        require_known_fingerprint(&fingerprints, &qualification.candidate_fingerprint)?;
        validate_codes("qualification failure code", &qualification.failure_codes)?;
        for result in &qualification.case_results {
            require_schema(
                "qualification result",
                result.schema_version,
                WORKLOAD_SCHEMA_VERSION,
            )?;
            validate_code("qualification case id", &result.case_id)?;
            if let Some(reason) = &result.reason {
                validate_code("qualification result reason", reason)?;
            }
        }
    }
    for measurement in &evidence.measurements {
        require_schema(
            "measurement evidence",
            measurement.schema_version,
            EVIDENCE_SCHEMA_VERSION,
        )?;
        validate_fingerprint(
            "measurement candidate fingerprint",
            &measurement.candidate_fingerprint,
        )?;
        require_known_fingerprint(&fingerprints, &measurement.candidate_fingerprint)?;
        validate_code("measurement outcome code", &measurement.outcome_code)?;
        if let Some(code) = &measurement.failure_code {
            validate_code("measurement failure code", code)?;
        }
        validate_code(
            "background observation code",
            &measurement.background_observation_code,
        )?;
        if let Some(code) = &measurement.thermal_observation_code {
            validate_code("thermal observation code", code)?;
        }
    }
    for observation in &evidence.isolation_observations {
        require_schema(
            "isolation observation",
            observation.schema_version,
            EVIDENCE_SCHEMA_VERSION,
        )?;
        validate_code("isolation check code", &observation.check_code)?;
        validate_code("isolation outcome code", &observation.outcome_code)?;
        validate_code("isolation observed fact", &observation.observed_fact)?;
    }
    validate_verdict(&evidence.verdict)?;
    validate_codes("explanation code", &evidence.explanation_codes)?;
    Ok(())
}

fn validate_verdict(verdict: &EvidenceVerdict) -> Result<(), EvidenceError> {
    let schema_version = match verdict {
        EvidenceVerdict::Selected {
            schema_version,
            candidate_id,
            reason_code,
        } => {
            validate_code("selected candidate id", candidate_id)?;
            validate_code("selector reason code", reason_code)?;
            *schema_version
        }
        EvidenceVerdict::NoVerifiedPlan {
            schema_version,
            reason_codes,
        } => {
            validate_codes("selector reason code", reason_codes)?;
            *schema_version
        }
        EvidenceVerdict::NoMaterialWinner {
            schema_version,
            baseline_candidate_id,
            reason_code,
        } => {
            validate_code("baseline candidate id", baseline_candidate_id)?;
            validate_code("selector reason code", reason_code)?;
            *schema_version
        }
    };
    require_schema("selector verdict", schema_version, SELECTOR_SCHEMA_VERSION)
}

fn validate_selection(selection: &SelectionRecord) -> Result<(), EvidenceError> {
    require_schema(
        "selection",
        selection.schema_version,
        SELECTION_SCHEMA_VERSION,
    )?;
    require_schema(
        "selection evidence reference",
        selection.evidence_schema_version,
        EVIDENCE_SCHEMA_VERSION,
    )?;
    validate_code("selection candidate id", &selection.candidate_id)?;
    validate_fingerprint(
        "selection candidate fingerprint",
        &selection.candidate_fingerprint,
    )?;
    validate_code("selection reason code", &selection.reason_code)?;
    Ok(())
}

fn validate_code(label: &str, value: &str) -> Result<(), EvidenceError> {
    const MAX_CODE_BYTES: usize = 128;
    const FORBIDDEN_CODES: &[&str] = &[
        "prompt",
        "response",
        "content",
        "tool_output",
        "token",
        "token_text",
        "credential",
        "authorization",
        "secret",
        "user_file",
    ];
    if value.is_empty()
        || value.len() > MAX_CODE_BYTES
        || FORBIDDEN_CODES.contains(&value)
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.:".contains(&byte)
        })
    {
        return Err(EvidenceError::InvalidRecord(format!(
            "{label} must be 1..={MAX_CODE_BYTES} ASCII code characters"
        )));
    }
    Ok(())
}

fn validate_codes(label: &str, values: &[String]) -> Result<(), EvidenceError> {
    for value in values {
        validate_code(label, value)?;
    }
    Ok(())
}

fn validate_fingerprint(label: &str, value: &str) -> Result<(), EvidenceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(EvidenceError::InvalidRecord(format!(
            "{label} must be a lowercase SHA-256 fingerprint"
        )));
    }
    Ok(())
}

fn require_known_fingerprint(known: &[&str], value: &str) -> Result<(), EvidenceError> {
    if known.contains(&value) {
        Ok(())
    } else {
        Err(EvidenceError::InvalidRecord(
            "evidence references an unknown candidate fingerprint".into(),
        ))
    }
}

fn validate_candidate_material(candidate: &CandidateSpec) -> Result<(), EvidenceError> {
    reject_forbidden_material(
        "candidate identity",
        [
            candidate.candidate_id.as_str(),
            candidate.endpoint.as_str(),
            candidate.artifact.artifact_id.as_str(),
            candidate.artifact.base_checkpoint.as_str(),
            candidate.artifact.format.as_str(),
            candidate.artifact.quantization.as_str(),
            candidate.engine.engine_kind.as_str(),
            candidate.engine.provider_version.as_str(),
        ]
        .into_iter()
        .chain(
            candidate
                .artifact
                .tokenizer_evidence
                .iter()
                .map(String::as_str),
        )
        .chain(
            candidate
                .artifact
                .template_evidence
                .iter()
                .map(String::as_str),
        )
        .chain(candidate.engine.evidence.iter().map(String::as_str))
        .chain(
            candidate
                .engine
                .invalidation_keys
                .iter()
                .map(String::as_str),
        ),
    )
}

fn reject_forbidden_material<'a>(
    label: &str,
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), EvidenceError> {
    const MAX_TEXT_BYTES: usize = 512;
    const FORBIDDEN: &[&str] = &[
        "prompt",
        "response",
        "content",
        "tool_output",
        "tool output",
        "token_text",
        "token text",
        "credential",
        "authorization",
        "secret",
        "user_file",
        "user file",
    ];
    const SECRET_PREFIXES: &[&str] = &["sk-", "sk_", "hf_", "ghp_", "github_pat_"];
    const CREDENTIAL_QUERY_SHAPES: &[&str] = &[
        "?token=",
        "&token=",
        "?api_key=",
        "&api_key=",
        "?access_token=",
        "&access_token=",
        "?password=",
        "&password=",
    ];
    for value in values {
        let normalized = value.to_ascii_lowercase();
        let path_like = normalized.starts_with('/')
            || is_windows_absolute_path(value)
            || normalized.starts_with("~/")
            || normalized.starts_with("$home/")
            || normalized.starts_with("${home}/")
            || normalized.contains("/users/")
            || normalized.contains("/home/")
            || normalized.contains("\\users\\")
            || normalized.starts_with("file://");
        let aws_access_id = value.len() == 20
            && (value.starts_with("AKIA") || value.starts_with("ASIA"))
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
        let header_or_key = normalized.starts_with("bearer ")
            || normalized.contains("authorization:")
            || SECRET_PREFIXES
                .iter()
                .any(|prefix| normalized.starts_with(prefix))
            || aws_access_id;
        let credential_uri = uri_has_user_info(&normalized)
            || CREDENTIAL_QUERY_SHAPES
                .iter()
                .any(|shape| normalized.contains(shape));
        let pem = normalized.contains("-----begin ") || normalized.contains("-----end ");
        let bytes = value.as_bytes();
        let long_hex = value.len() >= 40 && bytes.iter().all(u8::is_ascii_hexdigit);
        let long_alphanumeric = value.len() >= 40
            && bytes.iter().all(u8::is_ascii_alphanumeric)
            && bytes.iter().any(u8::is_ascii_alphabetic)
            && bytes.iter().any(u8::is_ascii_digit);
        let long_base64 = value.len() >= 40
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
            && bytes.iter().any(u8::is_ascii_uppercase)
            && bytes.iter().any(u8::is_ascii_lowercase);
        let high_entropy_token = long_hex || long_alphanumeric || long_base64;
        if value.is_empty()
            || value.len() > MAX_TEXT_BYTES
            || value.chars().any(char::is_control)
            || FORBIDDEN.iter().any(|needle| normalized.contains(needle))
            || path_like
            || header_or_key
            || credential_uri
            || pem
            || high_entropy_token
        {
            return Err(EvidenceError::InvalidRecord(format!(
                "{label} contains forbidden material"
            )));
        }
    }
    Ok(())
}

fn is_windows_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn uri_has_user_info(value: &str) -> bool {
    let Some((_, remainder)) = value.split_once("://") else {
        return false;
    };
    let authority = remainder.split(['/', '?', '#']).next().unwrap_or_default();
    authority.contains('@') && authority.contains(':')
}

fn selector_reason_code(reason: &str) -> Result<&'static str, EvidenceError> {
    match reason {
        "only the managed candidate passed all qualification cases" => {
            Ok("only_managed_candidate_qualified")
        }
        "only the attached candidate passed all qualification cases" => {
            Ok("only_attached_candidate_qualified")
        }
        "attached candidate median was at least 10% lower and it won at least 4 of 5 paired repetitions" => {
            Ok("attached_candidate_material_winner")
        }
        "five comparable successful paired repetitions were not available" => {
            Ok("insufficient_comparable_pairs")
        }
        "attached candidate did not clear both the 10% median and 4-of-5 paired-win thresholds" => {
            Ok("attached_candidate_thresholds_not_met")
        }
        code if validate_code("selector reason code", code).is_ok() => Ok("selector_reason_code"),
        _ => Err(EvidenceError::InvalidRecord(
            "unsupported selector explanation".into(),
        )),
    }
}

fn selector_qualification_reason_code(reason: &str) -> Result<&str, EvidenceError> {
    let code = reason.rsplit_once(": ").map_or(reason, |(_, code)| code);
    validate_code("selector qualification reason", code)?;
    Ok(code)
}

fn require_schema(record: &'static str, actual: u32, expected: u32) -> Result<(), EvidenceError> {
    if actual == expected {
        Ok(())
    } else {
        Err(EvidenceError::UnsupportedSchema {
            record,
            version: actual,
        })
    }
}

pub fn write_evidence_new(
    directory: &Path,
    evidence: &CalibrationEvidence,
) -> Result<PathBuf, EvidenceError> {
    validate_evidence(evidence)?;
    let absolute_directory = absolute_path(directory)?;
    fs::create_dir_all(&absolute_directory)?;
    let final_path = reserve_unique_evidence_path(&absolute_directory)?;
    let result = write_json_via_temp(&final_path, evidence, || Ok(()));
    if result.is_err() {
        let _ = fs::remove_file(&final_path);
    }
    result.map(|()| final_path)
}

pub fn write_selection_atomic(
    path: &Path,
    selection: &SelectionRecord,
) -> Result<(), EvidenceError> {
    write_selection_atomic_with_hook(path, selection, || Ok(()))
}

fn write_selection_atomic_with_hook<F>(
    path: &Path,
    selection: &SelectionRecord,
    pre_rename_hook: F,
) -> Result<(), EvidenceError>
where
    F: FnOnce() -> io::Result<()>,
{
    validate_selection(selection)?;
    let absolute_path = absolute_path(path)?;
    let parent = absolute_path
        .parent()
        .ok_or_else(|| EvidenceError::InvalidRecord("selection path has no parent".into()))?;
    fs::create_dir_all(parent)?;
    write_json_via_temp(&absolute_path, selection, pre_rename_hook)
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn absolute_from(current: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        current.join(path)
    }
}

fn reserve_unique_evidence_path(directory: &Path) -> Result<PathBuf, EvidenceError> {
    loop {
        let path = directory.join(format!("evidence-{}.json", unique_suffix()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                drop(file);
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

fn write_json_via_temp<T, F>(
    final_path: &Path,
    record: &T,
    pre_rename_hook: F,
) -> Result<(), EvidenceError>
where
    T: Serialize,
    F: FnOnce() -> io::Result<()>,
{
    let parent = final_path
        .parent()
        .ok_or_else(|| EvidenceError::InvalidRecord("record path has no parent".into()))?;
    let (temp_path, mut temp_file) = create_sibling_temp(parent)?;
    let result = (|| -> Result<(), EvidenceError> {
        let bytes = serde_json::to_vec_pretty(record)?;
        temp_file.write_all(&bytes)?;
        temp_file.flush()?;
        temp_file.sync_all()?;
        pre_rename_hook()?;
        drop(temp_file);
        fs::rename(&temp_path, final_path)?;
        sync_parent_directory(final_path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn create_sibling_temp(parent: &Path) -> Result<(PathBuf, File), EvidenceError> {
    loop {
        let path = parent.join(format!(".loxa-tmp-{}", unique_suffix()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let count = UNIQUE_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{nanos}-{count}", std::process::id())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::managed_llama::managed_candidate_spec;
    use crate::provider::{
        ArtifactIdentity, EngineIdentity, EngineRevision, GenerationSettings, ProviderKind,
        ProviderOwnership,
    };
    use crate::workload::QualificationCaseResult;
    use serde_json::Value;
    use std::fs;
    use std::io;
    use tempfile::tempdir;

    fn sample_evidence() -> CalibrationEvidence {
        let managed = managed_candidate_spec("1.2.3", "abc123").expect("valid candidate");
        let attached = CandidateSpec {
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
                tokenizer_evidence: vec!["ollama_show_tokenizer=verified".into()],
                template_evidence: vec!["ollama_show_template=verified".into()],
            },
            engine: EngineIdentity {
                schema_version: 1,
                engine_kind: "ollama-managed-gguf-engine".into(),
                provider_version: "0.9.0".into(),
                engine_revision: EngineRevision::Unknown,
                evidence: vec!["ollama_version=0.9.0".into()],
                invalidation_keys: vec!["provider_version=0.9.0".into()],
            },
            settings: GenerationSettings::pinned_v1(),
        };
        attached
            .validate_pinned()
            .expect("valid attached candidate");
        let managed_fingerprint = managed.fingerprint();
        let attached_fingerprint = attached.fingerprint();
        CalibrationEvidence {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            protocol_version: "calibration-v1".into(),
            workload_version: "tool-use-v1".into(),
            policy_version: "selector-v1".into(),
            started_at_unix_ms: 1_700_000_000_000,
            ended_at_unix_ms: 1_700_000_001_000,
            host: HostFingerprint {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                os_name: "test-os".into(),
                os_version: "1".into(),
                hardware_model: "test-machine".into(),
                physical_cores: 4,
                logical_cores: 8,
                memory_total_bytes: 16_000,
                memory_available_bytes: 8_000,
                root_disk_total_bytes: Some(100_000),
                root_disk_available_bytes: Some(50_000),
            },
            candidates: [
                CandidateEvidence {
                    schema_version: EVIDENCE_SCHEMA_VERSION,
                    identity: managed,
                    fingerprint: managed_fingerprint.clone(),
                },
                CandidateEvidence {
                    schema_version: EVIDENCE_SCHEMA_VERSION,
                    identity: attached,
                    fingerprint: attached_fingerprint,
                },
            ],
            disclosed_differences: vec![DisclosedDifference {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                difference_code: "provider_engine".into(),
                candidate_a_fact: "llama_cpp".into(),
                candidate_b_fact: "ollama".into(),
            }],
            qualifications: vec![QualificationEvidence {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                candidate_fingerprint: managed_fingerprint.clone(),
                case_results: vec![QualificationCaseResult {
                    schema_version: 1,
                    case_id: "lookup_record".into(),
                    passed: true,
                    reason: None,
                }],
                failure_codes: Vec::new(),
            }],
            measurements: vec![MeasurementEvidence {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                candidate_fingerprint: managed_fingerprint,
                repetition: 1,
                order_position: 1,
                cold_readiness_duration_ns: Some(10),
                end_to_end_duration_ns: Some(100),
                outcome_code: "success".into(),
                failure_code: None,
                prompt_token_count: Some(12),
                output_token_count: Some(7),
                prompt_eval_duration_ns: Some(20),
                decode_duration_ns: Some(30),
                host_available_memory_delta_bytes: Some(-512),
                background_observation_code: "idle".into(),
                thermal_observation_code: Some("nominal".into()),
                isolation_controlled: true,
            }],
            isolation_observations: vec![IsolationObservation {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                check_code: "no_competing_inference".into(),
                outcome_code: "passed".into(),
                observed_fact: "none_detected".into(),
            }],
            verdict: EvidenceVerdict::NoMaterialWinner {
                schema_version: 1,
                baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                reason_code: "thresholds_not_met".into(),
            },
            explanation_codes: vec!["baseline_retained".into()],
        }
    }

    fn sample_selection() -> SelectionRecord {
        let fingerprint = managed_candidate_spec("1.2.3", "abc123")
            .expect("valid candidate")
            .fingerprint();
        SelectionRecord {
            schema_version: SELECTION_SCHEMA_VERSION,
            recorded_at_unix_ms: 1_700_000_001_000,
            evidence_schema_version: EVIDENCE_SCHEMA_VERSION,
            candidate_id: "gemma-3-4b-it-q4".into(),
            candidate_fingerprint: fingerprint,
            disposition: SelectionDisposition::RetainedBaseline,
            reason_code: "no_material_winner".into(),
        }
    }

    #[test]
    fn round_trip_preserves_exact_evidence_schema_version() {
        let evidence = sample_evidence();
        let json = serde_json::to_vec_pretty(&evidence).expect("serialize evidence");
        let parsed = read_evidence_json(&json).expect("validated evidence");

        assert_eq!(parsed, evidence);
        assert_eq!(parsed.schema_version, EVIDENCE_SCHEMA_VERSION);
    }

    #[test]
    fn unsupported_evidence_and_selection_schemas_are_rejected() {
        let mut evidence = serde_json::to_value(sample_evidence()).expect("evidence value");
        evidence["schema_version"] = Value::from(99);
        let evidence_bytes = serde_json::to_vec(&evidence).expect("evidence bytes");
        assert!(matches!(
            read_evidence_json(&evidence_bytes),
            Err(EvidenceError::UnsupportedSchema {
                record: "evidence",
                version: 99
            })
        ));

        let mut selection = serde_json::to_value(sample_selection()).expect("selection value");
        selection["schema_version"] = Value::from(88);
        let selection_bytes = serde_json::to_vec(&selection).expect("selection bytes");
        assert!(matches!(
            read_selection_json(&selection_bytes),
            Err(EvidenceError::UnsupportedSchema {
                record: "selection",
                version: 88
            })
        ));
    }

    #[test]
    fn unsupported_nested_schemas_are_rejected() {
        let cases = [
            (vec!["host", "schema_version"], "host"),
            (
                vec!["candidates", "0", "schema_version"],
                "candidate evidence",
            ),
            (
                vec!["qualifications", "0", "schema_version"],
                "qualification evidence",
            ),
            (
                vec!["measurements", "0", "schema_version"],
                "measurement evidence",
            ),
            (
                vec!["isolation_observations", "0", "schema_version"],
                "isolation observation",
            ),
            (vec!["verdict", "schema_version"], "selector verdict"),
        ];

        for (path, expected_record) in cases {
            let mut value = serde_json::to_value(sample_evidence()).expect("evidence value");
            let mut cursor = &mut value;
            for segment in &path[..path.len() - 1] {
                cursor = if let Ok(index) = segment.parse::<usize>() {
                    &mut cursor[index]
                } else {
                    &mut cursor[*segment]
                };
            }
            cursor[path[path.len() - 1]] = Value::from(77);
            let bytes = serde_json::to_vec(&value).expect("evidence bytes");
            assert!(matches!(
                read_evidence_json(&bytes),
                Err(EvidenceError::UnsupportedSchema {
                    record,
                    version: 77
                }) if record == expected_record
            ));
        }
    }

    #[test]
    fn unknown_top_level_and_nested_fields_are_rejected() {
        let mut top_level = serde_json::to_value(sample_evidence()).expect("evidence value");
        top_level["unexpected"] = Value::Bool(true);
        assert!(matches!(
            read_evidence_json(&serde_json::to_vec(&top_level).expect("bytes")),
            Err(EvidenceError::Json(_))
        ));

        let mut nested = serde_json::to_value(sample_evidence()).expect("evidence value");
        nested["measurements"][0]["unexpected"] = Value::Bool(true);
        assert!(matches!(
            read_evidence_json(&serde_json::to_vec(&nested).expect("bytes")),
            Err(EvidenceError::Json(_))
        ));
    }

    #[test]
    fn malicious_evidence_is_rejected_before_any_final_file_is_created() {
        let payloads = [
            "prompt",
            "response",
            "content",
            "tool_output",
            "token",
            "token_text",
            "credential",
            "authorization",
            "secret",
            "user_file",
            "prompt: reveal this",
            "response body",
            "tool_output=private",
            "token_text=private",
            "credential=private",
            "authorization: bearer private",
            "secret=private",
            "user file material",
            "AKIAIOSFODNN7EXAMPLE",
            "ASIAIOSFODNN7EXAMPLE",
            "/tmp/private-notes.txt",
            "/var/tmp/user-data.json",
            "C:\\private\\notes.txt",
            "C:/private/notes.txt",
        ];

        for payload in payloads {
            let directory = tempdir().expect("temp directory");
            let mut evidence = sample_evidence();
            evidence.explanation_codes = vec![payload.into()];
            assert!(matches!(
                write_evidence_new(directory.path(), &evidence),
                Err(EvidenceError::InvalidRecord(_))
            ));
            assert_eq!(
                fs::read_dir(directory.path())
                    .expect("read directory")
                    .count(),
                0
            );
        }
    }

    #[test]
    fn malicious_selection_is_rejected_before_prior_bytes_are_replaced() {
        let payloads = [
            "prompt",
            "response",
            "content",
            "tool_output",
            "token",
            "token_text",
            "credential",
            "authorization",
            "secret",
            "user_file",
            "prompt: reveal this",
            "response body",
            "tool_output=private",
            "token_text=private",
            "credential=private",
            "authorization: bearer private",
            "secret=private",
            "user file material",
            "AKIAIOSFODNN7EXAMPLE",
            "ASIAIOSFODNN7EXAMPLE",
            "/tmp/private-notes.txt",
            "/var/tmp/user-data.json",
            "C:\\private\\notes.txt",
            "C:/private/notes.txt",
        ];

        for payload in payloads {
            let directory = tempdir().expect("temp directory");
            let path = directory.path().join("selection.json");
            fs::write(&path, b"prior selection").expect("seed prior selection");
            let mut selection = sample_selection();
            selection.reason_code = payload.into();
            assert!(matches!(
                write_selection_atomic(&path, &selection),
                Err(EvidenceError::InvalidRecord(_))
            ));
            assert_eq!(fs::read(&path).expect("read prior"), b"prior selection");
        }
    }

    #[test]
    fn every_bare_forbidden_normalized_value_is_rejected_by_the_code_validator() {
        for value in [
            "prompt",
            "response",
            "content",
            "tool_output",
            "token",
            "token_text",
            "credential",
            "authorization",
            "secret",
            "user_file",
        ] {
            assert!(matches!(
                validate_code("test code", value),
                Err(EvidenceError::InvalidRecord(_))
            ));
        }
    }

    #[test]
    fn sensitive_text_shapes_are_rejected_without_rejecting_truthful_hardware_names() {
        for value in [
            "line one\nline two",
            "/Users/alice/private/notes.txt",
            "~/private/notes.txt",
            "file:///tmp/private.txt",
            "Authorization: Bearer abc123",
            "Bearer abc123",
            "sk-proj-abcdefghijklmnopqrstuvwxyz012345",
            "sk_abcdefghijklmnopqrstuvwxyz0123456789",
            "hf_abcdefghijklmnopqrstuvwxyz0123456789",
            "ghp_abcdefghijklmnopqrstuvwxyz0123456789",
            "github_pat_abcdefghijklmnopqrstuvwxyz0123456789",
            "AKIAIOSFODNN7EXAMPLE",
            "ASIAIOSFODNN7EXAMPLE",
            "-----BEGIN PRIVATE KEY-----",
            "/tmp/private-notes.txt",
            "/var/tmp/user-data.json",
            "C:\\private\\notes.txt",
            "C:/private/notes.txt",
            "https://alice:password@example.invalid/resource",
            "https://example.invalid/resource?token=abc123",
            "QWxhZGRpbjpvcGVuIHNlc2FtZV9hYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5eg==",
        ] {
            assert!(matches!(
                reject_forbidden_material("test text", [value]),
                Err(EvidenceError::InvalidRecord(_))
            ));
        }

        reject_forbidden_material(
            "host fingerprint",
            ["macOS 15.5", "MacBook Pro (Apple M4 Max)"],
        )
        .expect("truthful OS and hardware facts remain allowed");
    }

    #[test]
    fn raw_keys_paths_and_content_shapes_are_rejected_before_evidence_creation() {
        let payloads = [
            "sk-proj-abcdefghijklmnopqrstuvwxyz012345",
            "AKIAIOSFODNN7EXAMPLE",
            "ASIAIOSFODNN7EXAMPLE",
            "/Users/alice/private/notes.txt",
            "/tmp/private-notes.txt",
            "/var/tmp/user-data.json",
            "C:\\private\\notes.txt",
            "C:/private/notes.txt",
            "private document contents: quarterly payroll",
        ];

        for payload in payloads {
            let host_directory = tempdir().expect("host temp directory");
            let mut host_evidence = sample_evidence();
            host_evidence.host.hardware_model = payload.into();
            assert!(matches!(
                write_evidence_new(host_directory.path(), &host_evidence),
                Err(EvidenceError::InvalidRecord(_))
            ));
            assert_eq!(
                fs::read_dir(host_directory.path())
                    .expect("read host directory")
                    .count(),
                0
            );

            let candidate_directory = tempdir().expect("candidate temp directory");
            let mut candidate_evidence = sample_evidence();
            candidate_evidence.candidates[0]
                .identity
                .engine
                .evidence
                .push(payload.into());
            candidate_evidence.candidates[0].fingerprint =
                candidate_evidence.candidates[0].identity.fingerprint();
            assert!(matches!(
                write_evidence_new(candidate_directory.path(), &candidate_evidence),
                Err(EvidenceError::InvalidRecord(_))
            ));
            assert_eq!(
                fs::read_dir(candidate_directory.path())
                    .expect("read candidate directory")
                    .count(),
                0
            );
        }
    }

    #[test]
    fn arbitrary_candidate_identity_material_is_rejected_even_with_matching_fingerprint() {
        let directory = tempdir().expect("temp directory");
        let mut evidence = sample_evidence();
        evidence.candidates[0]
            .identity
            .engine
            .evidence
            .push("secret=user_file_material".into());
        evidence.candidates[0].fingerprint = evidence.candidates[0].identity.fingerprint();

        assert!(matches!(
            write_evidence_new(directory.path(), &evidence),
            Err(EvidenceError::InvalidRecord(_))
        ));
        assert_eq!(
            fs::read_dir(directory.path())
                .expect("read directory")
                .count(),
            0
        );
    }

    #[test]
    fn evidence_requires_two_distinct_pinned_candidate_identities() {
        let directory = tempdir().expect("temp directory");
        let mut evidence = sample_evidence();
        evidence.candidates[1] = evidence.candidates[0].clone();
        assert!(matches!(
            write_evidence_new(directory.path(), &evidence),
            Err(EvidenceError::InvalidRecord(_))
        ));
    }

    #[test]
    fn default_and_injected_paths_are_absolute() {
        assert!(evidence_dir().is_absolute());
        assert!(selection_path().is_absolute());

        let current = env::current_dir().expect("current directory");
        let directory = tempfile::Builder::new()
            .prefix("task-3-relative-")
            .tempdir_in(&current)
            .expect("relative temp directory");
        let relative = directory
            .path()
            .strip_prefix(&current)
            .expect("path under current directory");
        let path = write_evidence_new(relative, &sample_evidence()).expect("relative write");
        assert!(path.is_absolute());
    }

    #[test]
    fn previous_evidence_is_preserved_and_new_path_is_unique_and_absolute() {
        let directory = tempdir().expect("temp directory");
        let prior = directory.path().join("evidence-existing.json");
        fs::write(&prior, b"prior evidence").expect("seed prior evidence");

        let first = write_evidence_new(directory.path(), &sample_evidence()).expect("first write");
        let second =
            write_evidence_new(directory.path(), &sample_evidence()).expect("second write");

        assert!(first.is_absolute());
        assert!(second.is_absolute());
        assert_ne!(first, second);
        assert_eq!(fs::read(&prior).expect("read prior"), b"prior evidence");
        assert_eq!(
            read_evidence_json(&fs::read(first).expect("read first")).expect("parse first"),
            sample_evidence()
        );
    }

    #[test]
    fn pre_rename_failure_preserves_selection_and_cleans_sibling_temp() {
        let directory = tempdir().expect("temp directory");
        let path = directory.path().join("selection.json");
        let prior = b"prior selection bytes";
        fs::write(&path, prior).expect("seed selection");

        let error = write_selection_atomic_with_hook(&path, &sample_selection(), || {
            Err(io::Error::other("injected failure"))
        })
        .expect_err("hook must fail");

        assert!(matches!(error, EvidenceError::Io(_)));
        assert_eq!(fs::read(&path).expect("read selection"), prior);
        let siblings = fs::read_dir(directory.path())
            .expect("read directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(siblings, vec!["selection.json"]);
    }

    #[test]
    fn atomic_selection_success_replaces_and_round_trips() {
        let directory = tempdir().expect("temp directory");
        let path = directory.path().join("selection.json");
        fs::write(&path, b"old bytes").expect("seed selection");

        write_selection_atomic(&path, &sample_selection()).expect("atomic write");

        let parsed =
            read_selection_json(&fs::read(path).expect("read selection")).expect("parse selection");
        assert_eq!(parsed, sample_selection());
    }

    #[test]
    fn serialized_evidence_recursively_excludes_sensitive_keys_and_values() {
        let value = serde_json::to_value(sample_evidence()).expect("serialize evidence");
        assert_no_leakage(&value);

        let measurement = &value["measurements"][0];
        assert_eq!(measurement["prompt_token_count"], 12);
        assert_eq!(measurement["output_token_count"], 7);
    }

    fn assert_no_leakage(value: &Value) {
        const FORBIDDEN: &[&str] = &[
            "prompt",
            "response",
            "content",
            "tool_output",
            "token_text",
            "credential",
            "authorization",
            "secret",
            "user_file",
            "user file",
        ];
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    let normalized = key.to_ascii_lowercase();
                    let permitted_numeric_metric = matches!(
                        normalized.as_str(),
                        "prompt_token_count" | "prompt_eval_duration_ns"
                    ) && (child.is_number() || child.is_null());
                    assert!(
                        permitted_numeric_metric
                            || !FORBIDDEN.iter().any(|needle| normalized.contains(needle)),
                        "forbidden serialized key: {key}"
                    );
                    assert_no_leakage(child);
                }
            }
            Value::Array(values) => values.iter().for_each(assert_no_leakage),
            Value::String(string) => {
                let normalized = string.to_ascii_lowercase();
                assert!(
                    !FORBIDDEN.iter().any(|needle| normalized.contains(needle)),
                    "forbidden serialized value: {string}"
                );
            }
            _ => {}
        }
    }

    #[test]
    fn unavailable_provider_metrics_remain_null_after_round_trip() {
        let mut evidence = sample_evidence();
        let measurement = &mut evidence.measurements[0];
        measurement.prompt_token_count = None;
        measurement.output_token_count = None;
        measurement.prompt_eval_duration_ns = None;
        measurement.decode_duration_ns = None;

        let json = serde_json::to_vec(&evidence).expect("serialize evidence");
        let value: Value = serde_json::from_slice(&json).expect("json value");
        assert!(value["measurements"][0]["prompt_token_count"].is_null());
        assert!(value["measurements"][0]["output_token_count"].is_null());
        assert!(value["measurements"][0]["prompt_eval_duration_ns"].is_null());
        assert!(value["measurements"][0]["decode_duration_ns"].is_null());

        let parsed = read_evidence_json(&json).expect("validated evidence");
        assert_eq!(parsed.measurements[0].prompt_token_count, None);
        assert_eq!(parsed.measurements[0].decode_duration_ns, None);
    }

    #[cfg(unix)]
    #[test]
    fn parent_directory_sync_succeeds_on_supported_unix_directory() {
        let directory = tempdir().expect("temp directory");
        let path = directory.path().join("record.json");
        fs::write(&path, b"record").expect("seed record");

        sync_parent_directory(&path).expect("sync parent directory");
    }
}
