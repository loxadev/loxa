use super::{validate_decimal, AttemptSummary, GenerationTarget};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationExecutionPhase {
    Working,
    Cancelling,
    /// Final execution facts are known; engine quiescence is not yet confirmed.
    Finalizing,
    Completed,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationSavePhase {
    Open,
    Saving,
    SaveFailed,
    Saved,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationStatus {
    pub target: GenerationTarget,
    pub attempt_id: String,
    pub execution: GenerationExecutionPhase,
    pub save: GenerationSavePhase,
    pub saved_end: String,
    /// Unknown until the service freezes execution, independently of saving.
    pub generated_end: Option<String>,
    pub terminal_saved_end: Option<String>,
    pub failure_code: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GenerationObservation {
    Live { status: GenerationStatus },
    Durable { attempt: AttemptSummary },
}

impl GenerationObservation {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Live { status } => status.validate_shape(),
            Self::Durable { attempt } => attempt.validate_shape(),
        }
    }
}

impl GenerationStatus {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        self.target.validate_shape()?;
        if !matches!(self.target, GenerationTarget::Accepted { .. }) {
            return Err("generation status requires an accepted target");
        }
        if self.attempt_id.len() != 32
            || !self
                .attempt_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("invalid generation attempt identity");
        }
        let saved = offset(&self.saved_end)?;
        let generated = self.generated_end.as_deref().map(offset).transpose()?;
        let terminal = self.terminal_saved_end.as_deref().map(offset).transpose()?;
        if generated.is_some_and(|end| saved > end) {
            return Err("saved output exceeds generated output");
        }
        let working = matches!(
            self.execution,
            GenerationExecutionPhase::Working | GenerationExecutionPhase::Cancelling
        );
        if working == generated.is_some() {
            return Err("generation execution has inconsistent final offsets");
        }
        if self.save == GenerationSavePhase::Saved {
            if generated != Some(saved) || terminal != Some(saved) {
                return Err("saved generation has inconsistent terminal offsets");
            }
        } else if terminal.is_some() {
            return Err("unsaved generation has a terminal saved offset");
        }
        if self.failure_code.as_ref().is_some_and(|code| {
            code.is_empty() || code.len() > 64 || code.capacity() > 64 || !code.is_ascii()
        }) {
            return Err("invalid generation failure code");
        }
        Ok(())
    }
}

fn offset(value: &str) -> Result<u64, &'static str> {
    validate_decimal(value, "invalid generation content offset")?;
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value <= super::history::MAX_ATTEMPT_CONTENT_BYTES)
        .ok_or("generation content offset exceeds its bound")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_and_save_are_independent_but_final_offsets_are_exact() {
        let mut status = GenerationStatus {
            target: GenerationTarget::Accepted {
                boot_epoch: "boot".into(),
                submission_id: "11".repeat(16),
                operation_generation: "1".into(),
            },
            attempt_id: "22".repeat(16),
            execution: GenerationExecutionPhase::Finalizing,
            save: GenerationSavePhase::Saved,
            saved_end: "6".into(),
            generated_end: Some("6".into()),
            terminal_saved_end: Some("6".into()),
            failure_code: None,
        };
        status.validate_shape().unwrap();
        status.terminal_saved_end = Some("5".into());
        assert!(status.validate_shape().is_err());
        status.terminal_saved_end = None;
        status.execution = GenerationExecutionPhase::Completed;
        status.save = GenerationSavePhase::SaveFailed;
        status.validate_shape().unwrap();
        status.save = GenerationSavePhase::Saving;
        status.validate_shape().unwrap();
        status.execution = GenerationExecutionPhase::Working;
        assert!(status.validate_shape().is_err());
        status.generated_end = None;
        status.validate_shape().unwrap();
        status.saved_end = (super::super::history::MAX_ATTEMPT_CONTENT_BYTES + 1).to_string();
        assert!(status.validate_shape().is_err());
    }

    #[test]
    fn durable_observation_requires_a_valid_attempt() {
        let attempt = AttemptSummary {
            id: "22".repeat(16),
            attempt_number: "1".into(),
            execution: super::super::AttemptExecution::Stopped,
            save: super::super::AttemptSave::Saved,
            saved_end: "0".into(),
            generated_end: Some("0".into()),
            terminal_saved_end: Some("0".into()),
            failure_code: Some("stopped".into()),
            statistics: None,
            effective_sampling: None,
            created_ms: "1".into(),
            updated_ms: "2".into(),
        };
        GenerationObservation::Durable {
            attempt: attempt.clone(),
        }
        .validate_shape()
        .unwrap();
        let mut invalid = attempt;
        invalid.terminal_saved_end = None;
        assert!(GenerationObservation::Durable { attempt: invalid }
            .validate_shape()
            .is_err());
    }
}
