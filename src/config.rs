use serde::{Deserialize, Deserializer};
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Config {
    pub ctx: Option<u32>,
    pub port: Option<u16>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    version: u32,
    #[serde(default, deserialize_with = "deserialize_optional")]
    ctx: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_optional")]
    port: Option<u16>,
}

fn deserialize_optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub fn load(path: &Path) -> Result<Config, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Config::default()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    if !metadata.file_type().is_file() {
        return Err(format!("config is not a regular file: {}", path.display()));
    }
    let file: ConfigFile = serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?,
    )
    .map_err(|error| format!("{}: {error}", path.display()))?;
    if file.version != 1 {
        return Err(format!(
            "unsupported config version {}; expected 1",
            file.version
        ));
    }
    Ok(Config {
        ctx: file.ctx,
        port: file.port,
    })
}

pub fn resolve_value<T: Copy>(cli: Option<T>, config: Option<T>, default: T) -> T {
    cli.or(config).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn missing_config_returns_defaults_without_creating_a_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");

        assert_eq!(load(&path).unwrap(), Config::default());
        assert!(!path.exists());
    }

    #[test]
    fn valid_partial_config_preserves_only_supplied_values() {
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
    }

    #[test]
    fn malformed_unknown_wrong_version_and_wrong_types_are_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        for json in [
            br#"{"version":"#.as_slice(),
            br#"{}"#,
            br#"{"version":1,"extra":true}"#,
            br#"{"version":2}"#,
            br#"{"version":1,"ctx":"8192"}"#,
            br#"{"version":1,"ctx":null}"#,
            br#"{"version":1,"port":70000}"#,
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
    fn non_regular_config_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::create_dir(&path).unwrap();

        let error = load(&path).unwrap_err();
        assert!(error.contains("regular file"), "{error}");
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
