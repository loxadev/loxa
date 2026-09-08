mod attempt;
mod discard;
mod entry;
mod prefix;
mod publication;
mod repair;
mod staging;

use super::http::TransferError;
use super::{perform_artifact_operation, DownloadDirectoryAuthority, IntegrityAuthority};
#[cfg(test)]
pub(super) use crate::verification::file::hex;
pub(super) use attempt::{download_once, DownloadRequest};
#[cfg(test)]
pub(super) use discard::discard_artifact_bytes_controlled;
pub(super) use discard::discard_artifact_bytes_inner;
pub(super) use publication::normalize_complete_restart;
pub(super) use staging::prove_existing_part_for_pause;

pub(super) enum ArtifactTransferError {
    RemoteBeforeBody(TransferError),
    Transfer(TransferError),
    Remote {
        error: TransferError,
        retained_bytes: u64,
    },
    Integrity {
        retained_bytes: u64,
        authority: IntegrityAuthority,
    },
    Durability,
    DiskExhausted {
        retained_bytes: u64,
    },
}

impl From<TransferError> for ArtifactTransferError {
    fn from(error: TransferError) -> Self {
        Self::Transfer(error)
    }
}

impl From<String> for ArtifactTransferError {
    fn from(message: String) -> Self {
        Self::Transfer(TransferError::fatal(message))
    }
}

impl From<&str> for ArtifactTransferError {
    fn from(message: &str) -> Self {
        Self::Transfer(TransferError::fatal(message))
    }
}

#[cfg(test)]
mod tests {
    use super::entry::{open_part_entry, open_restart_entry};
    use super::prefix::remove_restart_after_authoritative_part_is_durable;
    use crate::safe_file::{open_directory, regular_file_identity};

    #[cfg(unix)]
    #[test]
    fn restart_cleanup_targets_the_pinned_directory_after_a_path_swap() {
        let root = tempfile::tempdir().unwrap();
        let model_dir = root.path().join("model");
        let moved_model_dir = root.path().join("moved-model");
        let part_path = model_dir.join("model.gguf.part");
        let restart_path = model_dir.join("model.gguf.part.restart");
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&part_path, b"abcdef").unwrap();
        std::fs::write(&restart_path, b"XYZ").unwrap();
        let (directory, directory_identity) = open_directory(&model_dir).unwrap();
        let part = open_part_entry(&directory, &part_path).unwrap();
        let part_identity = regular_file_identity(&part, &part_path).unwrap();
        let restart = open_restart_entry(&directory, &restart_path).unwrap();
        let restart_identity = regular_file_identity(&restart, &restart_path).unwrap();

        std::fs::rename(&model_dir, &moved_model_dir).unwrap();
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&restart_path, b"replacement").unwrap();
        std::fs::write(model_dir.join("replacement-witness"), b"current").unwrap();

        let mut artifact_operation = super::super::perform_artifact_operation;
        let retained = remove_restart_after_authoritative_part_is_durable(
            (&directory, &directory_identity, &moved_model_dir),
            (&part, &part_identity, 6, &part_path),
            (&restart, &restart_identity, 3, &restart_path),
            &mut artifact_operation,
        );

        assert_eq!(
            std::fs::read(moved_model_dir.join("model.gguf.part")).unwrap(),
            b"abcdef"
        );
        assert_eq!(
            (
                moved_model_dir.join("model.gguf.part.restart").exists(),
                std::fs::read(&restart_path).ok(),
            ),
            (false, Some(b"replacement".to_vec()))
        );
        assert_eq!(
            std::fs::read(model_dir.join("replacement-witness")).unwrap(),
            b"current"
        );
        assert!(!model_dir.join("model.gguf.part").exists());
        assert!(matches!(retained, Ok(6)));
    }
}
