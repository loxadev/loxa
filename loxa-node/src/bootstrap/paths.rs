use loxa_core::{download, supervisor};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct NodePaths {
    pub models_dir: PathBuf,
    pub state_path: PathBuf,
    pub logs_dir: PathBuf,
}

impl NodePaths {
    pub fn detect() -> Self {
        Self {
            models_dir: download::model_dir(),
            state_path: supervisor::runtime_state_path(),
            logs_dir: supervisor::runtime_logs_dir(),
        }
    }

    pub(crate) fn log_path(&self, id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
        self.logs_dir
            .join(format!("{id}-{port}-{started_at_unix_s}.log"))
    }

    pub(crate) fn loxa_dir(&self) -> io::Result<&Path> {
        let state_dir = self
            .state_path
            .parent()
            .ok_or_else(|| io::Error::other("runtime state path has no parent directory"))?;
        if state_dir.file_name().is_some_and(|name| name == "run") {
            state_dir
                .parent()
                .ok_or_else(|| io::Error::other("runtime run path has no Loxa directory"))
        } else {
            Ok(state_dir)
        }
    }

    pub(crate) fn history_path(&self) -> io::Result<PathBuf> {
        Ok(self
            .loxa_dir()?
            .join("history")
            .join("chat-history.sqlite3"))
    }
}

#[cfg(test)]
mod tests {
    use super::NodePaths;
    use std::path::PathBuf;

    #[test]
    fn derives_private_history_beside_run_directory() {
        let root = PathBuf::from("/tmp/loxa-path-contract");
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("run/managed.json"),
            logs_dir: root.join("run/logs"),
        };
        assert_eq!(
            paths.history_path().unwrap(),
            root.join("history/chat-history.sqlite3")
        );
    }

    #[test]
    fn keeps_non_run_state_parent_as_loxa_root() {
        let root = PathBuf::from("/tmp/loxa-path-flat-contract");
        let paths = NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("managed.json"),
            logs_dir: root.join("logs"),
        };
        assert_eq!(paths.loxa_dir().unwrap(), root.as_path());
    }
}
