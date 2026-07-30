use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppPaths {
    pub root: PathBuf,
    pub models: PathBuf,
    pub config: PathBuf,
    pub runtimes: PathBuf,
    pub managed_server: PathBuf,
}

impl AppPaths {
    pub fn from_env() -> Result<Self, String> {
        let explicit = std::env::var_os("LOXA_HOME").map(PathBuf::from);
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from);
        Self::from_values(explicit.as_deref(), home.as_deref())
    }

    pub fn from_values(explicit: Option<&Path>, home: Option<&Path>) -> Result<Self, String> {
        let root = match explicit {
            Some(path) if !path.as_os_str().is_empty() => path.to_path_buf(),
            _ => home
                .filter(|path| !path.as_os_str().is_empty())
                .map(|path| path.join(".loxa"))
                .ok_or_else(|| "set LOXA_HOME, HOME, or USERPROFILE".to_string())?,
        };
        if !root.is_absolute() {
            return Err("Loxa home must be an absolute path".into());
        }
        let runtimes = root.join("runtimes");
        Ok(Self {
            models: root.join("models"),
            config: root.join("config.json"),
            managed_server: runtimes.join("llama.cpp/b10121/llama-server"),
            runtimes,
            root,
        })
    }

    pub fn model_dir(&self, id: &str) -> Result<PathBuf, String> {
        validate_id(id)?;
        Ok(self.models.join(id))
    }
}

pub(crate) fn validate_id(id: &str) -> Result<(), String> {
    if !id.is_empty()
        && id.len() <= 120
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !id.starts_with('-')
        && !id.ends_with('-')
    {
        Ok(())
    } else {
        Err(format!("invalid model id {id:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_home_wins_and_all_paths_derive_from_one_root() {
        let paths =
            AppPaths::from_values(Some(Path::new("/custom")), Some(Path::new("/home"))).unwrap();
        assert_eq!(paths.root, Path::new("/custom"));
        assert_eq!(paths.models, Path::new("/custom/models"));
        assert_eq!(paths.config, Path::new("/custom/config.json"));
        assert_eq!(paths.runtimes, Path::new("/custom/runtimes"));
        assert_eq!(
            paths.managed_server,
            Path::new("/custom/runtimes/llama.cpp/b10121/llama-server")
        );
        assert_eq!(
            paths.model_dir("demo").unwrap(),
            Path::new("/custom/models/demo")
        );
        assert!(paths.model_dir("../escape").is_err());
    }
}
