use super::{history::validate_hex_id, validate_decimal};
use serde::{Deserialize, Serialize};

pub const MAX_DRAFT_TEXT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DraftCommand {
    CreateScope {
        desktop_client_id: String,
        conversation_id: Option<String>,
    },
    SaveSnapshot {
        draft_id: String,
        desktop_client_id: String,
        revision: String,
        text: String,
    },
    ReadScope {
        draft_id: String,
        desktop_client_id: String,
    },
    DiscardScope {
        draft_id: String,
        desktop_client_id: String,
    },
}

impl DraftCommand {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::CreateScope {
                desktop_client_id,
                conversation_id,
            } => {
                validate_hex_id(desktop_client_id)?;
                if let Some(id) = conversation_id {
                    validate_hex_id(id)?;
                }
                Ok(())
            }
            Self::SaveSnapshot {
                draft_id,
                desktop_client_id,
                revision,
                text,
            } => {
                validate_hex_id(draft_id)?;
                validate_hex_id(desktop_client_id)?;
                validate_decimal(revision, "invalid draft revision")?;
                validate_text(text)
            }
            Self::ReadScope {
                draft_id,
                desktop_client_id,
            }
            | Self::DiscardScope {
                draft_id,
                desktop_client_id,
            } => {
                validate_hex_id(draft_id)?;
                validate_hex_id(desktop_client_id)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DraftReply {
    Snapshot(DraftSnapshot),
    Discarded { draft_id: String },
}

impl DraftReply {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Snapshot(snapshot) => snapshot.validate_shape(),
            Self::Discarded { draft_id } => validate_hex_id(draft_id),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DraftSnapshot {
    pub id: String,
    pub desktop_client_id: String,
    pub conversation_id: Option<String>,
    pub revision: String,
    pub consumed_revision: String,
    pub text: String,
    pub updated_ms: String,
}

impl DraftSnapshot {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.id)?;
        validate_hex_id(&self.desktop_client_id)?;
        if let Some(id) = &self.conversation_id {
            validate_hex_id(id)?;
        }
        validate_decimal(&self.revision, "invalid draft revision")?;
        validate_decimal(&self.consumed_revision, "invalid consumed draft revision")?;
        let revision = self
            .revision
            .parse::<u64>()
            .map_err(|_| "invalid draft revision")?;
        let consumed_revision = self
            .consumed_revision
            .parse::<u64>()
            .map_err(|_| "invalid consumed draft revision")?;
        if consumed_revision > revision {
            return Err("consumed draft revision exceeds current revision");
        }
        validate_decimal(&self.updated_ms, "invalid draft update time")?;
        validate_text(&self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_rejects_consumed_revision_above_revision() {
        let snapshot = DraftSnapshot {
            id: "11".repeat(16),
            desktop_client_id: "22".repeat(16),
            conversation_id: None,
            revision: "4".into(),
            consumed_revision: "5".into(),
            text: String::new(),
            updated_ms: "1".into(),
        };

        assert_eq!(
            snapshot.validate_shape(),
            Err("consumed draft revision exceeds current revision")
        );
    }
}

fn validate_text(text: &str) -> Result<(), &'static str> {
    if text.len() > MAX_DRAFT_TEXT_BYTES {
        Err("draft text exceeds 32 KiB")
    } else {
        Ok(())
    }
}
