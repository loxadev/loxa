use super::{
    history::{positive_bounded_decimal, validate_hex_id},
    validate_decimal,
};
use serde::{Deserialize, Deserializer, Serialize};

pub const MAX_SYSTEM_INSTRUCTION_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationSettings {
    pub system_instruction: String,
    pub max_output_tokens: u32,
}

impl Default for GenerationSettings {
    fn default() -> Self {
        Self {
            system_instruction: String::new(),
            max_output_tokens: 512,
        }
    }
}

impl GenerationSettings {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        if self.system_instruction.len() > MAX_SYSTEM_INSTRUCTION_BYTES
            || self.system_instruction.capacity() > MAX_SYSTEM_INSTRUCTION_BYTES
        {
            return Err("system instruction exceeds the byte limit");
        }
        if self.max_output_tokens == 0 || self.max_output_tokens > i32::MAX as u32 {
            return Err("invalid maximum output token request");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionalU32Patch {
    Set { value: u32 },
    Clear,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OptionalU16Patch {
    Set { value: u16 },
    Clear,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationSettingsPatch {
    Fields {
        #[serde(
            default,
            deserialize_with = "deserialize_present",
            skip_serializing_if = "Option::is_none"
        )]
        system_instruction: Option<String>,
        #[serde(
            default,
            deserialize_with = "deserialize_present",
            skip_serializing_if = "Option::is_none"
        )]
        max_output_tokens: Option<u32>,
    },
    Reset,
}

impl GenerationSettingsPatch {
    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Fields {
                system_instruction,
                max_output_tokens,
            } => {
                if system_instruction.is_none() && max_output_tokens.is_none() {
                    return Err("generation settings patch is empty");
                }
                if system_instruction.as_ref().is_some_and(|value| {
                    value.len() > MAX_SYSTEM_INSTRUCTION_BYTES
                        || value.capacity() > MAX_SYSTEM_INSTRUCTION_BYTES
                }) {
                    return Err("system instruction exceeds the byte limit");
                }
                if max_output_tokens.is_some_and(|value| value == 0 || value > i32::MAX as u32) {
                    return Err("invalid maximum output token request");
                }
                Ok(())
            }
            Self::Reset => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSettingsPatch {
    #[serde(
        default,
        deserialize_with = "deserialize_present",
        skip_serializing_if = "Option::is_none"
    )]
    pub ctx: Option<OptionalU32Patch>,
    #[serde(
        default,
        deserialize_with = "deserialize_present",
        skip_serializing_if = "Option::is_none"
    )]
    pub port: Option<OptionalU16Patch>,
    #[serde(
        default,
        deserialize_with = "deserialize_present",
        skip_serializing_if = "Option::is_none"
    )]
    pub generation: Option<GenerationSettingsPatch>,
}

impl ServiceSettingsPatch {
    fn validate_shape(&self) -> Result<(), &'static str> {
        if self.ctx.is_none() && self.port.is_none() && self.generation.is_none() {
            return Err("service settings patch is empty");
        }
        if let Some(generation) = &self.generation {
            generation.validate_shape()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceSettingsDurability {
    Baseline,
    Saving,
    Saved,
    SaveFailed,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceSettingsApplication {
    NotApplied,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSettings {
    pub revision: String,
    pub ctx: Option<u32>,
    pub port: Option<u16>,
    pub generation: GenerationSettings,
    pub durability: ServiceSettingsDurability,
    pub application: ServiceSettingsApplication,
}

impl ServiceSettings {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_decimal(&self.revision, "invalid service settings revision")?;
        self.generation.validate_shape()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationProfile {
    pub conversation_id: String,
    pub conversation_revision: String,
    pub profile_revision: String,
    pub generation: GenerationSettings,
}

impl ConversationProfile {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_hex_id(&self.conversation_id)?;
        positive_bounded_decimal(
            &self.conversation_revision,
            i64::MAX as u64,
            "invalid conversation revision",
        )?;
        positive_bounded_decimal(
            &self.profile_revision,
            i64::MAX as u64,
            "invalid profile revision",
        )?;
        self.generation.validate_shape()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceSettingsCommand {
    GetServiceSettings,
    PatchServiceSettings {
        expected_revision: String,
        patch: ServiceSettingsPatch,
    },
    RetryServiceSettingsSave,
    GetConversationProfile {
        conversation_id: String,
    },
    PatchConversationProfile {
        conversation_id: String,
        expected_conversation_revision: String,
        expected_profile_revision: String,
        patch: GenerationSettingsPatch,
    },
}

impl ServiceSettingsCommand {
    pub fn requires_ready_history(&self) -> bool {
        matches!(
            self,
            Self::GetConversationProfile { .. } | Self::PatchConversationProfile { .. }
        )
    }

    pub(crate) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::GetServiceSettings | Self::RetryServiceSettingsSave => Ok(()),
            Self::PatchServiceSettings {
                expected_revision,
                patch,
            } => {
                validate_decimal(expected_revision, "invalid service settings revision")?;
                patch.validate_shape()
            }
            Self::GetConversationProfile { conversation_id } => validate_hex_id(conversation_id),
            Self::PatchConversationProfile {
                conversation_id,
                expected_conversation_revision,
                expected_profile_revision,
                patch,
            } => {
                validate_hex_id(conversation_id)?;
                positive_bounded_decimal(
                    expected_conversation_revision,
                    i64::MAX as u64,
                    "invalid conversation revision",
                )?;
                positive_bounded_decimal(
                    expected_profile_revision,
                    i64::MAX as u64,
                    "invalid profile revision",
                )?;
                patch.validate_shape()
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceSettingsReply {
    Service(ServiceSettings),
    Conversation(ConversationProfile),
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl ServiceSettingsReply {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Service(settings) => settings.validate_shape(),
            Self::Conversation(profile) => profile.validate_shape(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_patches_round_trip_with_omissions_and_reject_null_or_unknown_fields() {
        let patch = ServiceSettingsPatch {
            ctx: Some(OptionalU32Patch::Clear),
            port: None,
            generation: Some(GenerationSettingsPatch::Fields {
                system_instruction: Some("system".into()),
                max_output_tokens: None,
            }),
        };
        let encoded = serde_json::to_value(&patch).unwrap();
        assert!(encoded.get("port").is_none());
        assert!(encoded["generation"].get("max_output_tokens").is_none());
        assert_eq!(
            serde_json::from_value::<ServiceSettingsPatch>(encoded).unwrap(),
            patch
        );

        for invalid in [
            serde_json::json!({"ctx": null}),
            serde_json::json!({"generation": {"type":"fields","max_output_tokens": null}}),
            serde_json::json!({"ctx": {"type":"clear"}, "extra": true}),
        ] {
            assert!(serde_json::from_value::<ServiceSettingsPatch>(invalid).is_err());
        }
    }

    #[test]
    fn conversation_profile_revisions_are_positive_signed_sql_values() {
        let profile = |conversation_revision: &str, profile_revision: &str| {
            ServiceSettingsReply::Conversation(ConversationProfile {
                conversation_id: "a".repeat(32),
                conversation_revision: conversation_revision.into(),
                profile_revision: profile_revision.into(),
                generation: GenerationSettings::default(),
            })
        };
        assert!(profile("1", &i64::MAX.to_string()).validate_shape().is_ok());
        for (conversation, profile_revision) in [
            ("0", "1"),
            ("1", "0"),
            ("18446744073709551615", "1"),
            ("1", "18446744073709551615"),
        ] {
            assert!(profile(conversation, profile_revision)
                .validate_shape()
                .is_err());
        }

        let command = |conversation: &str, profile_revision: &str| {
            ServiceSettingsCommand::PatchConversationProfile {
                conversation_id: "b".repeat(32),
                expected_conversation_revision: conversation.into(),
                expected_profile_revision: profile_revision.into(),
                patch: GenerationSettingsPatch::Fields {
                    system_instruction: None,
                    max_output_tokens: Some(1),
                },
            }
        };
        assert!(command("1", "1").validate_shape().is_ok());
        assert!(command("0", "1").validate_shape().is_err());
        assert!(command("1", "18446744073709551615")
            .validate_shape()
            .is_err());
    }
}
