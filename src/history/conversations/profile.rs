use super::{invalid_column, not_found, positive, positive_or_zero, sql_error};
use crate::history::identity::{decode_id, next_updated_ms, parse_revision};
use crate::history::{HistoryError, HistoryErrorKind};
use loxa_ipc::{ConversationProfile, GenerationSettings, ServiceSettingsCommand};
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn execute(
    connection: &mut Connection,
    operation: ServiceSettingsCommand,
    reset_default: Option<GenerationSettings>,
) -> Result<ConversationProfile, HistoryError> {
    match operation {
        ServiceSettingsCommand::GetConversationProfile { conversation_id } => {
            let id = decode_id(&conversation_id)?;
            read(connection, id, conversation_id)?.ok_or_else(not_found)
        }
        ServiceSettingsCommand::PatchConversationProfile {
            conversation_id,
            expected_conversation_revision,
            expected_profile_revision,
            patch: fields,
        } => {
            let id = decode_id(&conversation_id)?;
            let expected_conversation = parse_revision(&expected_conversation_revision)?;
            let expected_profile = parse_revision(&expected_profile_revision)?;
            patch_profile(
                connection,
                id,
                conversation_id,
                expected_conversation,
                expected_profile,
                fields,
                reset_default,
            )
        }
        ServiceSettingsCommand::GetServiceSettings
        | ServiceSettingsCommand::PatchServiceSettings { .. }
        | ServiceSettingsCommand::RetryServiceSettingsSave => Err(HistoryError::new(
            HistoryErrorKind::InvalidInput,
            "service settings command is not a conversation profile operation",
        )),
    }
}

struct StoredProfile {
    revision: i64,
    profile_revision: i64,
    updated_ms: i64,
    generation: GenerationSettings,
}

fn read(
    connection: &Connection,
    id: [u8; 16],
    conversation_id: String,
) -> Result<Option<ConversationProfile>, HistoryError> {
    read_stored(connection, id)
        .map(|profile| profile.map(|profile| to_wire(conversation_id, profile)))
}

fn read_stored(
    connection: &Connection,
    id: [u8; 16],
) -> Result<Option<StoredProfile>, HistoryError> {
    connection
        .query_row(
            "SELECT revision, profile_revision, updated_ms, system_instruction, max_output_tokens
             FROM conversations WHERE id = ?1 AND deleted = 0",
            [id.as_slice()],
            |row| {
                use rusqlite::types::ValueRef;

                let revision = positive(row, 0, "invalid conversation revision")?;
                let profile_revision = positive(row, 1, "invalid profile revision")?;
                let updated_ms = positive_or_zero(row, 2, "invalid update time")?;
                let system_instruction = match row.get_ref(3)? {
                    ValueRef::Text(bytes) if bytes.len() <= 16 * 1024 => std::str::from_utf8(bytes)
                        .map_err(|_| {
                            invalid_column(
                                3,
                                rusqlite::types::Type::Text,
                                "invalid system instruction",
                            )
                        })?,
                    value => {
                        return Err(invalid_column(
                            3,
                            value.data_type(),
                            "invalid system instruction",
                        ))
                    }
                };
                let max_output_tokens = match row.get_ref(4)? {
                    ValueRef::Integer(value) if (1..=i64::from(i32::MAX)).contains(&value) => {
                        u32::try_from(value).expect("positive i32 fits u32")
                    }
                    value => {
                        return Err(invalid_column(
                            4,
                            value.data_type(),
                            "invalid maximum output tokens",
                        ))
                    }
                };
                Ok(StoredProfile {
                    revision,
                    profile_revision,
                    updated_ms,
                    generation: GenerationSettings {
                        system_instruction: system_instruction.to_owned(),
                        max_output_tokens,
                    },
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

#[allow(clippy::too_many_arguments)]
fn patch_profile(
    connection: &mut Connection,
    id: [u8; 16],
    conversation_id: String,
    expected_conversation: i64,
    expected_profile: i64,
    patch: loxa_ipc::GenerationSettingsPatch,
    reset_default: Option<GenerationSettings>,
) -> Result<ConversationProfile, HistoryError> {
    let transaction = connection.transaction().map_err(sql_error)?;
    let mut existing = read_stored(&transaction, id)?.ok_or_else(not_found)?;
    if existing.revision != expected_conversation || existing.profile_revision != expected_profile {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation profile revision changed",
        ));
    }
    match patch {
        loxa_ipc::GenerationSettingsPatch::Reset => {
            existing.generation = reset_default.ok_or_else(|| {
                HistoryError::new(
                    HistoryErrorKind::InvalidInput,
                    "conversation profile reset has no captured service default",
                )
            })?;
        }
        fields => crate::config::apply_generation_patch(&mut existing.generation, fields)
            .map_err(|error| HistoryError::new(HistoryErrorKind::InvalidInput, error.context))?,
    }
    crate::config::validate_generation(&existing.generation)
        .map_err(|error| HistoryError::new(HistoryErrorKind::InvalidInput, error))?;
    let revision = expected_conversation.checked_add(1).ok_or_else(|| {
        HistoryError::new(
            HistoryErrorKind::LimitExceeded,
            "conversation revision overflow",
        )
    })?;
    let profile_revision = expected_profile.checked_add(1).ok_or_else(|| {
        HistoryError::new(HistoryErrorKind::LimitExceeded, "profile revision overflow")
    })?;
    let updated_ms = next_updated_ms(existing.updated_ms)?;
    let changed = transaction
        .execute(
            "UPDATE conversations
             SET system_instruction = ?1, max_output_tokens = ?2, updated_ms = ?3,
                 revision = revision + 1, profile_revision = profile_revision + 1
             WHERE id = ?4 AND deleted = 0 AND revision = ?5 AND profile_revision = ?6
               AND revision < 9223372036854775807
               AND profile_revision < 9223372036854775807",
            params![
                existing.generation.system_instruction,
                existing.generation.max_output_tokens,
                updated_ms,
                id.as_slice(),
                expected_conversation,
                expected_profile,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(HistoryError::new(
            HistoryErrorKind::Conflict,
            "conversation profile revision changed",
        ));
    }
    transaction.commit().map_err(sql_error)?;
    Ok(ConversationProfile {
        conversation_id,
        conversation_revision: revision.to_string(),
        profile_revision: profile_revision.to_string(),
        generation: existing.generation,
    })
}

fn to_wire(conversation_id: String, profile: StoredProfile) -> ConversationProfile {
    ConversationProfile {
        conversation_id,
        conversation_revision: profile.revision.to_string(),
        profile_revision: profile.profile_revision.to_string(),
        generation: profile.generation,
    }
}
