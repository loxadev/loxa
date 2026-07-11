use crate::calibration::{CalibrationEvidence, CALIBRATION_EVIDENCE_SCHEMA_VERSION};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn write_evidence_atomic(destination: &Path, evidence: &CalibrationEvidence) -> io::Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (destination, evidence);
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic evidence replacement is currently supported only on Unix",
        ));
    }

    #[cfg(unix)]
    write_evidence_atomic_with_hook(destination, evidence, || Ok(()))
}

#[cfg(unix)]
fn write_evidence_atomic_with_hook<F>(
    destination: &Path,
    evidence: &CalibrationEvidence,
    before_rename: F,
) -> io::Result<()>
where
    F: FnOnce() -> io::Result<()>,
{
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let (mut file, temporary_path) = create_temporary_file(parent, destination)?;
    let mut cleanup = TemporaryFile::new(temporary_path);

    let mut versioned = evidence.clone();
    versioned.schema_version = CALIBRATION_EVIDENCE_SCHEMA_VERSION;
    sanitize_failures(&mut versioned);
    let bytes = serde_json::to_vec_pretty(&versioned).map_err(io::Error::other)?;
    file.write_all(&bytes)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    before_rename()?;

    fs::rename(cleanup.path(), destination)?;
    cleanup.disarm();
    sync_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn sanitize_failures(evidence: &mut CalibrationEvidence) {
    for candidate in [&mut evidence.managed, &mut evidence.attached] {
        if let Some(failure) = candidate.failure.as_deref() {
            candidate.failure = Some(stable_failure_category(failure).to_string());
        }
    }
}

#[cfg(unix)]
fn stable_failure_category(failure: &str) -> &'static str {
    if failure.starts_with("provider identity error:") {
        "provider identity failure"
    } else if failure.starts_with("provider protocol error:") {
        "provider protocol failure"
    } else if failure.starts_with("provider I/O error:") {
        "provider I/O failure"
    } else if failure == "provider unavailable" {
        "provider unavailable"
    } else if failure == "provider invocation timed out" {
        "provider timeout"
    } else {
        "candidate hard-gate failure"
    }
}

fn create_temporary_file(parent: &Path, destination: &Path) -> io::Result<(File, PathBuf)> {
    let destination_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination has no file name")
        })?;

    loop {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{destination_name}.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

struct TemporaryFile {
    path: PathBuf,
    armed: bool,
}

impl TemporaryFile {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::{write_evidence_atomic, write_evidence_atomic_with_hook};
    use crate::calibration::{
        CalibrationEvidence, CandidateEvidence, CandidateOwnership,
        CALIBRATION_EVIDENCE_SCHEMA_VERSION,
    };
    use crate::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
    use std::io;

    fn evidence() -> CalibrationEvidence {
        let candidate = |candidate_id: &str, provider, ownership| CandidateEvidence {
            identity: CandidateIdentity {
                candidate_id: candidate_id.into(),
                provider,
                provider_version: "1.0".into(),
                engine_revision: Some("revision".into()),
                model_id: "model".into(),
                artifact_digest: "sha256:artifact".into(),
                tokenizer_digest: "sha256:tokenizer".into(),
                chat_template_digest: "sha256:template".into(),
                context_tokens: 4096,
                required_free_memory_bytes: 100,
                sampling: SamplingPolicy {
                    temperature_milli: 0,
                    top_p_milli: 1000,
                    seed: 1,
                },
            },
            ownership,
            qualified: false,
            qualification: None,
            available_memory_before_bytes: 1_000,
            failure: None,
            warmup: None,
        };
        CalibrationEvidence {
            schema_version: CALIBRATION_EVIDENCE_SCHEMA_VERSION,
            managed: candidate(
                "managed",
                ProviderKind::ManagedLlama,
                CandidateOwnership::Managed,
            ),
            attached: candidate(
                "attached",
                ProviderKind::Ollama,
                CandidateOwnership::Attached,
            ),
            pairs: vec![],
            verdict: None,
        }
    }

    #[test]
    fn writes_schema_version_one_json_that_round_trips() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");

        write_evidence_atomic(&path, &evidence()).unwrap();

        let bytes = std::fs::read(path).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(
            serde_json::from_slice::<CalibrationEvidence>(&bytes).unwrap(),
            evidence()
        );
    }

    #[test]
    fn creates_missing_parent_directories() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing/parents/evidence.json");

        write_evidence_atomic(&path, &evidence()).unwrap();

        assert!(path.is_file());
    }

    #[test]
    fn leaves_no_temporary_file_after_success() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");

        write_evidence_atomic(&path, &evidence()).unwrap();

        let entries = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec!["evidence.json"]);
    }

    #[test]
    fn preserves_original_and_removes_temp_when_pre_rename_hook_fails() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        let original = b"original evidence";
        std::fs::write(&path, original).unwrap();

        let error = write_evidence_atomic_with_hook(&path, &evidence(), || {
            Err(io::Error::other("injected failure"))
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let entries = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec!["evidence.json"]);
    }

    #[test]
    fn replaces_free_form_failure_text_with_a_stable_private_category() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        let mut input = evidence();
        input.managed.failure =
            Some("provider protocol error: prompt included private-user-ticket-99".into());

        write_evidence_atomic(&path, &input).unwrap();

        let bytes = std::fs::read_to_string(path).unwrap();
        assert!(!bytes.contains("private-user-ticket-99"));
        let persisted: CalibrationEvidence = serde_json::from_str(&bytes).unwrap();
        assert_eq!(
            persisted.managed.failure.as_deref(),
            Some("provider protocol failure")
        );
    }
}
