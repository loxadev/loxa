use super::{validate_decimal, validate_identifier, MAX_ID_BYTES};
use serde::{Deserialize, Serialize};

pub const MAX_GENERATION_USER_TEXT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationHello {
    pub connection: GenerationConnection,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationConnection {
    Request,
    Control,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationHelloAck {
    pub pending_nonce: String,
}

impl GenerationHelloAck {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.pending_nonce, "invalid pending generation nonce")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationCommand {
    Send {
        conversation_id: String,
        submission_id: String,
        expected_conversation_revision: String,
        expected_profile_revision: String,
        user_text: String,
        draft: Option<GenerationDraft>,
    },
    Retry {
        conversation_id: String,
        submission_id: String,
        expected_conversation_revision: String,
        expected_profile_revision: String,
        prior_attempt_id: String,
    },
    Stop {
        target: GenerationTarget,
    },
}

impl GenerationCommand {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Send {
                conversation_id,
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
                user_text,
                draft,
            } => {
                validate_hex_id(conversation_id, "invalid conversation identity")?;
                validate_hex_id(submission_id, "invalid submission identity")?;
                validate_positive_decimal(
                    expected_conversation_revision,
                    "invalid conversation revision",
                )?;
                validate_positive_decimal(expected_profile_revision, "invalid profile revision")?;
                if user_text.is_empty()
                    || user_text.len() > MAX_GENERATION_USER_TEXT_BYTES
                    || user_text.capacity() > MAX_GENERATION_USER_TEXT_BYTES
                {
                    return Err("user text must contain 1 byte to 32 KiB");
                }
                if let Some(draft) = draft {
                    draft.validate_shape()?;
                }
                Ok(())
            }
            Self::Retry {
                conversation_id,
                submission_id,
                expected_conversation_revision,
                expected_profile_revision,
                prior_attempt_id,
            } => {
                validate_hex_id(conversation_id, "invalid conversation identity")?;
                validate_hex_id(submission_id, "invalid submission identity")?;
                validate_positive_decimal(
                    expected_conversation_revision,
                    "invalid conversation revision",
                )?;
                validate_positive_decimal(expected_profile_revision, "invalid profile revision")?;
                validate_hex_id(prior_attempt_id, "invalid prior attempt identity")
            }
            Self::Stop { target } => target.validate_shape(),
        }
    }

    pub fn is_stop(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }

    pub(crate) fn is_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationDraft {
    pub id: String,
    pub desktop_client_id: String,
    pub revision: String,
}

impl GenerationDraft {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.id, "invalid draft identity")?;
        validate_hex_id(&self.desktop_client_id, "invalid desktop client identity")?;
        validate_decimal(&self.revision, "invalid draft revision")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationTarget {
    Pending {
        boot_epoch: String,
        pending_nonce: String,
    },
    Accepted {
        boot_epoch: String,
        submission_id: String,
        operation_generation: String,
    },
}

impl GenerationTarget {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Pending {
                boot_epoch,
                pending_nonce,
            } => {
                validate_identifier(boot_epoch, "invalid service boot epoch")?;
                validate_hex_id(pending_nonce, "invalid pending generation nonce")
            }
            Self::Accepted {
                boot_epoch,
                submission_id,
                operation_generation,
            } => {
                validate_identifier(boot_epoch, "invalid service boot epoch")?;
                validate_hex_id(submission_id, "invalid submission identity")?;
                validate_positive_decimal(operation_generation, "invalid operation generation")
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationReply {
    Accepted(GenerationAccepted),
    Stopping { target: GenerationTarget },
}

impl GenerationReply {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Accepted(accepted) => accepted.validate_shape(),
            Self::Stopping { target } => target.validate_shape(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationAccepted {
    pub boot_epoch: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub attempt_id: String,
    pub submission_id: String,
    pub pre_conversation_revision: String,
    pub post_conversation_revision: String,
    pub profile_revision: String,
    pub operation_generation: String,
}

impl GenerationAccepted {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_identifier(&self.boot_epoch, "invalid service boot epoch")?;
        validate_hex_id(&self.conversation_id, "invalid conversation identity")?;
        validate_hex_id(&self.turn_id, "invalid turn identity")?;
        validate_hex_id(&self.attempt_id, "invalid attempt identity")?;
        validate_hex_id(&self.submission_id, "invalid submission identity")?;
        validate_positive_decimal(
            &self.pre_conversation_revision,
            "invalid pre-admission conversation revision",
        )?;
        validate_positive_decimal(
            &self.post_conversation_revision,
            "invalid post-admission conversation revision",
        )?;
        validate_positive_decimal(&self.profile_revision, "invalid profile revision")?;
        validate_positive_decimal(&self.operation_generation, "invalid operation generation")
    }

    pub fn target(&self) -> GenerationTarget {
        GenerationTarget::Accepted {
            boot_epoch: self.boot_epoch.clone(),
            submission_id: self.submission_id.clone(),
            operation_generation: self.operation_generation.clone(),
        }
    }
}

fn validate_hex_id(value: &str, context: &'static str) -> Result<(), &'static str> {
    if value.len() != 32
        || value.len() > MAX_ID_BYTES
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(context);
    }
    Ok(())
}

fn validate_positive_decimal(value: &str, context: &'static str) -> Result<(), &'static str> {
    validate_decimal(value, context)?;
    if value == "0" {
        return Err(context);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_shape_bounds_submitted_identity_before_service_work() {
        let command = GenerationCommand::Send {
            conversation_id: "a".repeat(32),
            submission_id: "b".repeat(32),
            expected_conversation_revision: "1".into(),
            expected_profile_revision: "2".into(),
            user_text: "hello".into(),
            draft: Some(GenerationDraft {
                id: "c".repeat(32),
                desktop_client_id: "d".repeat(32),
                revision: "0".into(),
            }),
        };
        command.validate_shape().unwrap();

        let mut oversized = command;
        let GenerationCommand::Send { user_text, .. } = &mut oversized else {
            unreachable!();
        };
        *user_text = "x".repeat(MAX_GENERATION_USER_TEXT_BYTES + 1);
        assert_eq!(
            oversized.validate_shape(),
            Err("user text must contain 1 byte to 32 KiB")
        );

        let retry = GenerationCommand::Retry {
            conversation_id: "a".repeat(32),
            submission_id: "e".repeat(32),
            expected_conversation_revision: "3".into(),
            expected_profile_revision: "2".into(),
            prior_attempt_id: "f".repeat(32),
        };
        retry.validate_shape().unwrap();
        let mut invalid = retry;
        let GenerationCommand::Retry {
            prior_attempt_id, ..
        } = &mut invalid
        else {
            unreachable!();
        };
        prior_attempt_id.push('0');
        assert_eq!(
            invalid.validate_shape(),
            Err("invalid prior attempt identity")
        );
    }
}
