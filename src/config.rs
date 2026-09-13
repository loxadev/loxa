use loxa_ipc::GenerationSettings;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::io;
use std::path::Path;

mod file;
mod policy;

pub(crate) use policy::{apply_generation_patch, SettingsExit, SettingsObserver, SettingsOwner};

pub(crate) const MAX_CONFIG_BYTES: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Config {
    pub ctx: Option<u32>,
    pub port: Option<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoadedSettings {
    pub(crate) config: Config,
    pub(crate) revision: u64,
    pub(crate) generation: GenerationSettings,
    pub(crate) v2: bool,
}

impl Default for LoadedSettings {
    fn default() -> Self {
        Self {
            config: Config::default(),
            revision: 0,
            generation: GenerationSettings::default(),
            v2: false,
        }
    }
}

enum Field {
    Version,
    Revision,
    Ctx,
    Port,
    Generation,
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(FieldVisitor)
    }
}

struct FieldVisitor;

impl Visitor<'_> for FieldVisitor {
    type Value = Field;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a supported config field")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "version" => Ok(Field::Version),
            "revision" => Ok(Field::Revision),
            "ctx" => Ok(Field::Ctx),
            "port" => Ok(Field::Port),
            "generation" => Ok(Field::Generation),
            _ => Err(E::custom("unknown config field")),
        }
    }
}

enum GenerationField {
    SystemInstruction,
    MaxOutputTokens,
}

impl<'de> Deserialize<'de> for GenerationField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(GenerationFieldVisitor)
    }
}

struct GenerationFieldVisitor;

impl Visitor<'_> for GenerationFieldVisitor {
    type Value = GenerationField;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a supported generation field")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "system_instruction" => Ok(GenerationField::SystemInstruction),
            "max_output_tokens" => Ok(GenerationField::MaxOutputTokens),
            _ => Err(E::custom("unknown generation field")),
        }
    }
}

struct StrictGeneration(GenerationSettings);

impl<'de> Deserialize<'de> for StrictGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(GenerationVisitor)
    }
}

struct GenerationVisitor;

impl<'de> Visitor<'de> for GenerationVisitor {
    type Value = StrictGeneration;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict generation settings object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut system_instruction = None;
        let mut max_output_tokens = None;
        while let Some(field) = map.next_key::<GenerationField>()? {
            match field {
                GenerationField::SystemInstruction if system_instruction.is_none() => {
                    system_instruction = Some(map.next_value::<String>()?);
                }
                GenerationField::MaxOutputTokens if max_output_tokens.is_none() => {
                    max_output_tokens = Some(map.next_value::<u32>()?);
                }
                GenerationField::SystemInstruction => {
                    return Err(serde::de::Error::duplicate_field("system_instruction"))
                }
                GenerationField::MaxOutputTokens => {
                    return Err(serde::de::Error::duplicate_field("max_output_tokens"))
                }
            }
        }
        let generation = GenerationSettings {
            system_instruction: system_instruction
                .ok_or_else(|| serde::de::Error::missing_field("system_instruction"))?,
            max_output_tokens: max_output_tokens
                .ok_or_else(|| serde::de::Error::missing_field("max_output_tokens"))?,
        };
        validate_generation(&generation).map_err(serde::de::Error::custom)?;
        Ok(StrictGeneration(generation))
    }
}

struct StrictDocument(LoadedSettings);

impl<'de> Deserialize<'de> for StrictDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DocumentVisitor)
    }
}

struct DocumentVisitor;

impl<'de> Visitor<'de> for DocumentVisitor {
    type Value = StrictDocument;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict Loxa configuration document")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut version = None;
        let mut revision = None;
        let mut ctx = None;
        let mut port = None;
        let mut generation = None;
        let mut saw_ctx = false;
        let mut saw_port = false;
        while let Some(field) = map.next_key::<Field>()? {
            match field {
                Field::Version if version.is_none() => version = Some(map.next_value::<u32>()?),
                Field::Revision if revision.is_none() => revision = Some(map.next_value::<u64>()?),
                Field::Ctx if !saw_ctx => {
                    saw_ctx = true;
                    ctx = Some(map.next_value::<u32>()?);
                }
                Field::Port if !saw_port => {
                    saw_port = true;
                    port = Some(map.next_value::<u16>()?);
                }
                Field::Generation if generation.is_none() => {
                    generation = Some(map.next_value::<StrictGeneration>()?.0);
                }
                Field::Version => return Err(serde::de::Error::duplicate_field("version")),
                Field::Revision => return Err(serde::de::Error::duplicate_field("revision")),
                Field::Ctx => return Err(serde::de::Error::duplicate_field("ctx")),
                Field::Port => return Err(serde::de::Error::duplicate_field("port")),
                Field::Generation => return Err(serde::de::Error::duplicate_field("generation")),
            }
        }
        let version = version.ok_or_else(|| serde::de::Error::missing_field("version"))?;
        let loaded = match version {
            1 if revision.is_none() && generation.is_none() => LoadedSettings {
                config: Config { ctx, port },
                ..LoadedSettings::default()
            },
            2 => {
                let revision = revision
                    .filter(|value| *value > 0)
                    .ok_or_else(|| serde::de::Error::custom("invalid config revision"))?;
                let generation =
                    generation.ok_or_else(|| serde::de::Error::missing_field("generation"))?;
                validate_generation(&generation).map_err(serde::de::Error::custom)?;
                LoadedSettings {
                    config: Config { ctx, port },
                    revision,
                    generation,
                    v2: true,
                }
            }
            1 => {
                return Err(serde::de::Error::custom(
                    "version 1 config contains version 2 fields",
                ))
            }
            _ => return Err(serde::de::Error::custom("unsupported config version")),
        };
        Ok(StrictDocument(loaded))
    }
}

fn parse(bytes: &[u8]) -> Result<LoadedSettings, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let document = StrictDocument::deserialize(&mut deserializer).map_err(bounded_json_error)?;
    deserializer.end().map_err(bounded_json_error)?;
    Ok(document.0)
}

fn bounded_json_error(error: serde_json::Error) -> String {
    format!(
        "invalid config document ({:?} at line {} column {})",
        error.classify(),
        error.line(),
        error.column()
    )
}

pub(crate) fn validate_generation(generation: &GenerationSettings) -> Result<(), &'static str> {
    if generation.system_instruction.len() > 16 * 1024
        || generation.system_instruction.capacity() > 16 * 1024
    {
        return Err("system instruction exceeds the byte limit");
    }
    if generation.max_output_tokens == 0 || generation.max_output_tokens > i32::MAX as u32 {
        return Err("invalid maximum output token request");
    }
    Ok(())
}

fn read(path: &Path, private: bool) -> Result<LoadedSettings, String> {
    let bytes = if private {
        crate::safe_file::read_private_regular_file_bounded(path, MAX_CONFIG_BYTES)
    } else {
        crate::safe_file::read_regular_file_bounded(path, MAX_CONFIG_BYTES)
    };
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(LoadedSettings::default())
        }
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    parse(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

pub fn load(path: &Path) -> Result<Config, String> {
    read(path, false).map(|loaded| loaded.config)
}

pub(crate) fn load_private(path: &Path) -> Result<LoadedSettings, String> {
    read(path, true)
}

pub fn resolve_value<T: Copy>(cli: Option<T>, config: Option<T>, default: T) -> T {
    cli.or(config).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[test]
    fn missing_config_returns_defaults_without_creating_a_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        assert_eq!(load(&path).unwrap(), Config::default());
        assert!(!path.exists());
    }

    #[test]
    fn strict_v1_and_v2_preserve_the_legacy_projection() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, br#"{"version":1,"ctx":8192}"#).unwrap();
        assert_eq!(
            load(&path).unwrap(),
            Config {
                ctx: Some(8192),
                port: None,
            }
        );
        std::fs::write(
            &path,
            br#"{"version":2,"revision":7,"ctx":0,"port":0,"generation":{"system_instruction":"system","max_output_tokens":42}}"#,
        )
        .unwrap();
        assert_eq!(
            load(&path).unwrap(),
            Config {
                ctx: Some(0),
                port: Some(0),
            }
        );
        let loaded = read(&path, false).unwrap();
        assert_eq!(loaded.revision, 7);
        assert_eq!(loaded.generation.system_instruction, "system");
        assert_eq!(loaded.generation.max_output_tokens, 42);
    }

    #[test]
    fn malformed_unknown_null_duplicate_and_newer_documents_are_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        for json in [
            br#"{"version":"#.as_slice(),
            br#"{}"#,
            br#"{"version":1,"extra":true}"#,
            br#"{"version":3}"#,
            br#"{"version":1,"ctx":"8192"}"#,
            br#"{"version":1,"ctx":null}"#,
            br#"{"version":1,"port":70000}"#,
            br#"{"version":1,"revision":1}"#,
            br#"{"version":2,"revision":0,"generation":{"system_instruction":"","max_output_tokens":512}}"#,
            br#"{"version":2,"revision":1,"generation":null}"#,
            br#"{"version":1,"version":1}"#,
        ] {
            std::fs::write(&path, json).unwrap();
            assert!(
                load(&path).is_err(),
                "accepted {}",
                String::from_utf8_lossy(json)
            );
        }
    }

    #[test]
    fn legacy_reader_accepts_0644_but_private_owner_requires_0600() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, br#"{"version":1,"ctx":8192}"#).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(load(&path).unwrap().ctx, Some(8192));
        assert!(load_private(&path).unwrap_err().contains("0600"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(load_private(&path).unwrap().config.ctx, Some(8192));
    }

    #[test]
    fn oversized_document_and_non_regular_path_are_rejected_without_blocking() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, vec![b'x'; MAX_CONFIG_BYTES + 1]).unwrap();
        assert!(load(&path).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(load(&path).unwrap_err().contains("regular file"));
    }

    #[test]
    fn each_runtime_field_resolves_cli_then_config_then_default_and_keeps_zero() {
        assert_eq!(resolve_value(Some(0_u32), Some(8192), 4096), 0);
        assert_eq!(resolve_value(None, Some(8192_u32), 4096), 8192);
        assert_eq!(resolve_value(None, None, 4096_u32), 4096);
        assert_eq!(resolve_value(Some(0_u16), Some(1234), 99), 0);
        assert_eq!(resolve_value(None, Some(1234_u16), 99), 1234);
        assert_eq!(resolve_value(None, None, 0_u16), 0);
    }
}
