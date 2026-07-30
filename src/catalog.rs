use crate::paths::validate_id;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct PullLock {
    _file: fs::File,
}

impl PullLock {
    pub fn acquire(model_dir: &Path) -> Result<Self, String> {
        ensure_catalog_directory(model_dir)?;
        let path = model_dir.join(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => "another pull is already active for this model".into(),
            TryLockError::Error(error) => format!("{}: {error}", path.display()),
        })?;
        Ok(Self { _file: file })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub id: String,
    pub repo: String,
    pub revision: String,
    pub remote_filename: String,
    pub local_filename: String,
    pub sha256: String,
    pub size: u64,
}

impl Manifest {
    pub fn validate(&self) -> Result<(), String> {
        validate_id(&self.id)?;
        validate_repo(&self.repo)?;
        validate_hex(&self.revision, 40, "revision")?;
        validate_filename(&self.remote_filename)?;
        validate_filename(&self.local_filename)?;
        if self.local_filename != "model.gguf" {
            return Err("local filename must be model.gguf".into());
        }
        validate_hex(&self.sha256, 64, "SHA-256")?;
        if self.version != 1 || self.size == 0 {
            return Err("invalid manifest version or size".into());
        }
        Ok(())
    }

    pub fn artifact_path(&self, models_root: &Path) -> PathBuf {
        models_root.join(&self.id).join(&self.local_filename)
    }
}

pub fn load_catalog(models_root: &Path) -> Result<Vec<Manifest>, String> {
    if !models_root.exists() {
        return Ok(Vec::new());
    }
    let mut manifests = Vec::new();
    for item in fs::read_dir(models_root).map_err(|error| error.to_string())? {
        let item = item.map_err(|error| error.to_string())?;
        let path = item.path();
        if !item
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err(format!("unexpected catalog entry {}", path.display()));
        }
        let manifest_path = path.join("manifest.json");
        let metadata = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                validate_incomplete_dir(&path)?;
                continue;
            }
            Err(error) => return Err(format!("{}: {error}", manifest_path.display())),
        };
        if !metadata.file_type().is_file() {
            return Err(format!(
                "manifest is not a regular file: {}",
                manifest_path.display()
            ));
        }
        let manifest: Manifest = serde_json::from_slice(
            &fs::read(&manifest_path)
                .map_err(|error| format!("{}: {error}", manifest_path.display()))?,
        )
        .map_err(|error| format!("{}: {error}", manifest_path.display()))?;
        manifest.validate()?;
        if path.file_name().and_then(|name| name.to_str()) != Some(manifest.id.as_str()) {
            return Err(format!("manifest id does not match {}", path.display()));
        }
        if manifests
            .iter()
            .any(|existing: &Manifest| existing.id == manifest.id)
        {
            return Err(format!("duplicate model id {}", manifest.id));
        }
        manifests.push(manifest);
    }
    manifests.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(manifests)
}

pub fn prepare_pull(model_dir: &Path, manifest: &Manifest) -> Result<(), String> {
    manifest.validate()?;
    if model_dir.file_name().and_then(|name| name.to_str()) != Some(manifest.id.as_str()) {
        return Err("pending manifest id does not match model directory".into());
    }
    ensure_catalog_directory(model_dir)?;
    let pending_path = model_dir.join("pending.json");
    if pending_path.exists() {
        let existing: Manifest = serde_json::from_slice(
            &fs::read(&pending_path)
                .map_err(|error| format!("{}: {error}", pending_path.display()))?,
        )
        .map_err(|error| format!("{}: {error}", pending_path.display()))?;
        existing.validate()?;
        return if existing == *manifest {
            Ok(())
        } else {
            Err(format!(
                "model id {} has an incomplete pull for a different artifact",
                manifest.id
            ))
        };
    }
    remove_unidentified_transfer_state(model_dir)?;
    write_manifest_atomic(model_dir, "pending.json", manifest)?;
    Ok(())
}

pub fn publish_manifest(models_root: &Path, manifest: &Manifest) -> Result<PathBuf, String> {
    manifest.validate()?;
    let dir = models_root.join(&manifest.id);
    ensure_catalog_directory(&dir)?;
    crate::download::verify_regular(
        &manifest.artifact_path(models_root),
        manifest.size,
        &manifest.sha256,
    )?;
    let final_path = dir.join("manifest.json");
    if final_path.exists() {
        let existing: Manifest =
            serde_json::from_slice(&fs::read(&final_path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if existing == *manifest {
            finish_pending(&dir)?;
            return Ok(final_path);
        }
        return Err(format!("manifest already exists for {}", manifest.id));
    }
    write_manifest_atomic(&dir, "manifest.json", manifest)?;
    finish_pending(&dir)?;
    Ok(final_path)
}

fn ensure_catalog_directory(dir: &Path) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let metadata =
        fs::symlink_metadata(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(format!("unsafe model directory {}", dir.display()))
    }
}

fn write_manifest_atomic(dir: &Path, name: &str, manifest: &Manifest) -> Result<(), String> {
    let final_path = dir.join(name);
    let temp_path = dir.join(format!("{name}.tmp"));
    remove_regular_if_present(&temp_path)?;
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temp_path, &final_path).map_err(|error| error.to_string())?;
    fs::File::open(dir)
        .and_then(|parent| parent.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn remove_unidentified_transfer_state(dir: &Path) -> Result<(), String> {
    let mut removed = false;
    for name in [
        "model.gguf",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ] {
        removed |= remove_regular_if_present(&dir.join(name))?;
    }
    if removed {
        fs::File::open(dir)
            .and_then(|parent| parent.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn remove_regular_if_present(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(path).map_err(|error| error.to_string())?;
            Ok(true)
        }
        Ok(_) => Err(format!("unsafe temporary path {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn finish_pending(dir: &Path) -> Result<(), String> {
    let pending = dir.join("pending.json");
    if pending.exists() {
        fs::remove_file(&pending).map_err(|error| error.to_string())?;
        fs::File::open(dir)
            .and_then(|parent| parent.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn validate_incomplete_dir(dir: &Path) -> Result<(), String> {
    let id = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid incomplete model directory {}", dir.display()))?;
    validate_id(id)?;
    for item in fs::read_dir(dir).map_err(|error| error.to_string())? {
        let item = item.map_err(|error| error.to_string())?;
        let name = item
            .file_name()
            .to_str()
            .ok_or_else(|| format!("invalid incomplete model entry {}", item.path().display()))?
            .to_string();
        let allowed = matches!(
            name.as_str(),
            ".lock"
                | "pending.json"
                | "pending.json.tmp"
                | "manifest.json.tmp"
                | "model.gguf"
                | "model.gguf.part"
                | "model.gguf.part.restart"
                | "model.gguf.invalid"
        );
        if !allowed
            || !item
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
        {
            return Err(format!(
                "unexpected incomplete model entry {}",
                item.path().display()
            ));
        }
    }
    Ok(())
}

fn validate_repo(repo: &str) -> Result<(), String> {
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() == 2
        && parts
            .iter()
            .all(|part| !part.is_empty() && *part != "." && *part != "..")
    {
        Ok(())
    } else {
        Err("repository must be owner/repo".into())
    }
}

fn validate_filename(filename: &str) -> Result<(), String> {
    let lower = filename.to_ascii_lowercase();
    if !filename.is_empty()
        && !filename.contains(['/', '\\'])
        && !filename.contains("..")
        && lower.ends_with(".gguf")
        && !lower.contains("-of-")
    {
        Ok(())
    } else {
        Err(format!("invalid single-file GGUF name {filename:?}"))
    }
}

fn validate_hex(value: &str, len: usize, label: &str) -> Result<(), String> {
    if value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(format!("invalid {label}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn manifest(id: &str) -> Manifest {
        Manifest {
            version: 1,
            id: id.into(),
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            remote_filename: "demo-Q4_K_M.gguf".into(),
            local_filename: "model.gguf".into(),
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size: 3,
        }
    }

    fn write_artifact(models_root: &Path, id: &str) {
        let model_dir = models_root.join(id);
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    }

    #[test]
    fn validates_and_atomically_publishes_fresh_deterministic_catalog() {
        let dir = tempdir().unwrap();
        write_artifact(dir.path(), "zeta");
        write_artifact(dir.path(), "alpha");
        publish_manifest(dir.path(), &manifest("zeta")).unwrap();
        publish_manifest(dir.path(), &manifest("alpha")).unwrap();
        publish_manifest(dir.path(), &manifest("alpha")).unwrap();

        let entries = load_catalog(dir.path()).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert!(!dir.path().join("alpha/manifest.json.tmp").exists());

        let mut invalid = manifest("../bad");
        assert!(invalid.validate().is_err());
        invalid = manifest("valid");
        invalid.remote_filename = "part-00001-of-00002.gguf".into();
        assert!(invalid.validate().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_second_pull_lock_is_rejected_until_the_first_is_released() {
        let dir = tempdir().unwrap();
        let first = PullLock::acquire(dir.path()).unwrap();
        assert!(PullLock::acquire(dir.path()).is_err());
        drop(first);
        PullLock::acquire(dir.path()).unwrap();
    }

    #[test]
    fn pull_lock_reports_lock_path_when_lock_is_a_directory() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let lock_path = model_dir.join(".lock");
        std::fs::create_dir_all(&lock_path).unwrap();

        let error = match PullLock::acquire(&model_dir) {
            Ok(_) => panic!("a directory cannot be acquired as a pull lock"),
            Err(error) => error,
        };

        assert!(error.contains(&lock_path.display().to_string()), "{error}");
    }

    #[test]
    fn an_existing_unlocked_lock_file_does_not_block_acquisition() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join(".lock"), b"stale").unwrap();

        PullLock::acquire(&model_dir).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn symlinked_model_directory_is_rejected_before_catalog_mutation() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let models = root.path().join("models");
        let outside = root.path().join("outside");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("model.gguf"), b"artifact").unwrap();
        std::fs::write(outside.join("model.gguf.part"), b"partial").unwrap();
        std::fs::write(outside.join("model.gguf.part.restart"), b"restart").unwrap();
        std::fs::write(outside.join("model.gguf.invalid"), b"invalid").unwrap();
        let model_dir = models.join("demo");
        symlink(&outside, &model_dir).unwrap();
        let expected = manifest("demo");

        assert!(PullLock::acquire(&model_dir).is_err());
        assert!(prepare_pull(&model_dir, &expected).is_err());
        assert!(publish_manifest(&models, &expected).is_err());

        assert!(!outside.join(".lock").exists());
        assert!(!outside.join("pending.json").exists());
        assert!(!outside.join("manifest.json").exists());
        assert_eq!(
            std::fs::read(outside.join("model.gguf")).unwrap(),
            b"artifact"
        );
        assert_eq!(
            std::fs::read(outside.join("model.gguf.part")).unwrap(),
            b"partial"
        );
        assert_eq!(
            std::fs::read(outside.join("model.gguf.part.restart")).unwrap(),
            b"restart"
        );
        assert_eq!(
            std::fs::read(outside.join("model.gguf.invalid")).unwrap(),
            b"invalid"
        );
    }

    #[test]
    fn incomplete_pull_is_hidden_and_can_only_resume_the_same_artifact() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let expected = manifest("demo");

        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.gguf.part"), b"unidentified").unwrap();
        prepare_pull(&model_dir, &expected).unwrap();
        assert!(!model_dir.join("model.gguf.part").exists());
        std::fs::write(model_dir.join("model.gguf.part"), b"partial").unwrap();
        assert!(load_catalog(root.path()).unwrap().is_empty());
        prepare_pull(&model_dir, &expected).unwrap();
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
            b"partial"
        );

        let mut different = expected.clone();
        different.sha256 = "b".repeat(64);
        assert!(prepare_pull(&model_dir, &different)
            .unwrap_err()
            .contains("different artifact"));

        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
        publish_manifest(root.path(), &expected).unwrap();
        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
        assert!(!model_dir.join("pending.json").exists());
    }

    #[test]
    fn stale_atomic_temps_do_not_poison_or_block_an_incomplete_pull() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let expected = manifest("demo");
        std::fs::create_dir_all(&model_dir).unwrap();

        std::fs::write(model_dir.join("pending.json.tmp"), b"interrupted").unwrap();
        prepare_pull(&model_dir, &expected).unwrap();
        assert!(!model_dir.join("pending.json.tmp").exists());

        std::fs::write(model_dir.join("manifest.json.tmp"), b"interrupted").unwrap();
        assert!(load_catalog(root.path()).unwrap().is_empty());
        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
        publish_manifest(root.path(), &expected).unwrap();

        assert!(!model_dir.join("manifest.json.tmp").exists());
        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    }

    #[test]
    fn missing_or_checksum_invalid_artifact_is_not_published() {
        let root = tempdir().unwrap();
        let missing = manifest("missing");

        assert!(publish_manifest(root.path(), &missing).is_err());
        assert!(!root.path().join("missing/manifest.json").exists());

        let invalid = manifest("invalid");
        let invalid_dir = root.path().join("invalid");
        std::fs::create_dir_all(&invalid_dir).unwrap();
        std::fs::write(invalid_dir.join("model.gguf"), b"xyz").unwrap();

        assert!(publish_manifest(root.path(), &invalid).is_err());
        assert!(!invalid_dir.join("manifest.json").exists());
        assert!(load_catalog(root.path()).unwrap().is_empty());
    }
}
