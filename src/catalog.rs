use crate::paths::validate_id;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct ModelLock {
    _file: fs::File,
}

impl ModelLock {
    pub fn acquire(model_dir: &Path) -> Result<Self, String> {
        ensure_catalog_directory(model_dir)?;
        let path = model_dir.join(".lock");
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options
            .open(&path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if !metadata.file_type().is_file() {
            return Err(format!("unsafe model lock {}", path.display()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                return Err(format!("unsafe model lock {}", path.display()));
            }
        }
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => "model is busy in another Loxa command".into(),
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
            continue;
        }
        let manifest_path = path.join("manifest.json");
        let metadata = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
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
    reject_unidentified_transfer_state(model_dir)?;
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

pub fn remove_model(models_root: &Path, manifest: &Manifest) -> Result<(), String> {
    manifest.validate()?;
    let model_dir = models_root.join(&manifest.id);
    if model_dir.file_name().and_then(|name| name.to_str()) != Some(manifest.id.as_str()) {
        return Err("model id does not match model directory".into());
    }
    let _lock = ModelLock::acquire(&model_dir)?;
    let current = load_catalog(models_root)?
        .into_iter()
        .find(|entry| entry.id == manifest.id)
        .ok_or_else(|| format!("unknown model id {}", manifest.id))?;
    if current != *manifest {
        return Err(format!("model {} changed before removal", manifest.id));
    }

    const OWNED_ENTRIES: [&str; 9] = [
        ".lock",
        "manifest.json",
        "manifest.json.tmp",
        "pending.json",
        "pending.json.tmp",
        "model.gguf",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ];
    for entry in fs::read_dir(&model_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| format!("unexpected model entry {}", entry.path().display()))?;
        if !OWNED_ENTRIES.contains(&name) {
            return Err(format!("unexpected model entry {}", entry.path().display()));
        }
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_file()
        {
            return Err(format!("unsafe model entry {}", entry.path().display()));
        }
        if name == "pending.json" {
            let pending: Manifest =
                serde_json::from_slice(&fs::read(entry.path()).map_err(|error| error.to_string())?)
                    .map_err(|error| format!("{}: {error}", entry.path().display()))?;
            pending.validate()?;
            if pending != *manifest {
                return Err(format!(
                    "model {} has recovery state for a different artifact",
                    manifest.id
                ));
            }
        }
    }

    for name in [
        "model.gguf",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
        "pending.json",
        "pending.json.tmp",
        "manifest.json.tmp",
    ] {
        remove_regular_if_present(&model_dir.join(name))?;
    }
    fs::File::open(&model_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    let manifest_path = model_dir.join("manifest.json");
    fs::remove_file(&manifest_path)
        .map_err(|error| format!("{}: {error}", manifest_path.display()))?;
    fs::File::open(&model_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
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

fn reject_unidentified_transfer_state(dir: &Path) -> Result<(), String> {
    for name in [
        "model.gguf",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ] {
        let path = dir.join(name);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(format!(
                    "unidentified transfer state at {}; move or remove the colliding path before retrying",
                    path.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
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

fn validate_repo(repo: &str) -> Result<(), String> {
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty() && *part != "." && *part != ".." && !part.chars().any(char::is_control)
        })
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
        && !filename.chars().any(char::is_control)
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

    #[test]
    fn foreign_store_entries_do_not_hide_managed_models() {
        let root = tempdir().unwrap();
        let expected = manifest("smollm2-135m");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        std::fs::write(root.path().join("legacy.gguf.part"), b"partial").unwrap();
        std::fs::write(root.path().join("legacy.gguf"), b"model").unwrap();
        let foreign = root.path().join("source-checkout");
        std::fs::create_dir(&foreign).unwrap();
        std::fs::write(foreign.join("model.safetensors"), b"weights").unwrap();

        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    }

    #[cfg(unix)]
    #[test]
    fn a_second_model_lock_is_rejected_until_the_first_is_released() {
        let dir = tempdir().unwrap();
        let first = ModelLock::acquire(dir.path()).unwrap();
        assert!(ModelLock::acquire(dir.path()).is_err());
        drop(first);
        ModelLock::acquire(dir.path()).unwrap();
    }

    #[test]
    fn model_lock_reports_lock_path_when_lock_is_a_directory() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let lock_path = model_dir.join(".lock");
        std::fs::create_dir_all(&lock_path).unwrap();

        let error = match ModelLock::acquire(&model_dir) {
            Ok(_) => panic!("a directory cannot be acquired as a model lock"),
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

        ModelLock::acquire(&model_dir).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn symlinked_or_hard_linked_model_lock_is_rejected() {
        use std::os::unix::fs::symlink;

        for link in ["symlink", "hard-link"] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("demo");
            let outside = root.path().join("outside");
            std::fs::create_dir_all(&model_dir).unwrap();
            std::fs::write(&outside, b"keep").unwrap();
            let lock = model_dir.join(".lock");
            if link == "symlink" {
                symlink(&outside, &lock).unwrap();
            } else {
                std::fs::hard_link(&outside, &lock).unwrap();
            }

            assert!(ModelLock::acquire(&model_dir).is_err());
            assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
        }
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

        assert!(ModelLock::acquire(&model_dir).is_err());
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

        prepare_pull(&model_dir, &expected).unwrap();
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
    fn unidentified_transfer_state_is_refused_without_mutation() {
        for name in [
            "model.gguf",
            "model.gguf.part",
            "model.gguf.part.restart",
            "model.gguf.invalid",
        ] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("demo");
            let path = model_dir.join(name);
            std::fs::create_dir_all(&model_dir).unwrap();
            std::fs::write(&path, b"unidentified bytes").unwrap();

            let error = prepare_pull(&model_dir, &manifest("demo")).unwrap_err();

            assert!(error.contains(&path.display().to_string()), "{error}");
            assert!(error.contains("move or remove"), "{error}");
            assert_eq!(std::fs::read(&path).unwrap(), b"unidentified bytes");
            assert!(!model_dir.join("pending.json").exists());
        }
    }

    #[test]
    fn manifest_rejects_control_characters_in_remote_identity() {
        let mut invalid_repo = manifest("demo");
        invalid_repo.repo = "owner/repo\u{1b}".into();
        assert!(invalid_repo.validate().is_err());

        let mut invalid_filename = manifest("demo");
        invalid_filename.remote_filename = "demo-\u{85}Q4_K_M.gguf".into();
        assert!(invalid_filename.validate().is_err());
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

    #[test]
    fn removing_a_model_deletes_owned_state_and_keeps_a_stable_lock_anchor() {
        let root = tempdir().unwrap();
        let expected = manifest("demo");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        std::fs::write(root.path().join("foreign.gguf"), b"keep").unwrap();

        remove_model(root.path(), &expected).unwrap();

        let model_dir = root.path().join("demo");
        assert!(model_dir.is_dir());
        assert!(!model_dir.join("manifest.json").exists());
        assert!(!model_dir.join("model.gguf").exists());
        assert!(model_dir.join(".lock").is_file());
        ModelLock::acquire(&model_dir).unwrap();
        assert_eq!(
            std::fs::read(root.path().join("foreign.gguf")).unwrap(),
            b"keep"
        );
        assert!(load_catalog(root.path()).unwrap().is_empty());
    }

    #[test]
    fn removing_a_model_cleans_only_known_recovery_state() {
        let root = tempdir().unwrap();
        let expected = manifest("demo");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        let model_dir = root.path().join("demo");
        for name in [
            "pending.json.tmp",
            "manifest.json.tmp",
            "model.gguf.part",
            "model.gguf.part.restart",
            "model.gguf.invalid",
        ] {
            std::fs::write(model_dir.join(name), b"recovery").unwrap();
        }
        std::fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec(&expected).unwrap(),
        )
        .unwrap();

        remove_model(root.path(), &expected).unwrap();

        assert_eq!(
            std::fs::read_dir(&model_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            [".lock"]
        );
        assert!(load_catalog(root.path()).unwrap().is_empty());
    }

    #[test]
    fn removing_a_model_refuses_mismatched_pending_state() {
        let root = tempdir().unwrap();
        let expected = manifest("demo");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        let mut pending = expected.clone();
        pending.sha256 = "b".repeat(64);
        std::fs::write(
            root.path().join("demo/pending.json"),
            serde_json::to_vec(&pending).unwrap(),
        )
        .unwrap();

        let error = remove_model(root.path(), &expected).unwrap_err();

        assert!(error.contains("different artifact"), "{error}");
        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    }

    #[test]
    fn removing_a_model_refuses_unexpected_entries_without_mutation() {
        let root = tempdir().unwrap();
        let expected = manifest("demo");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        let unexpected = root.path().join("demo/notes.txt");
        std::fs::write(&unexpected, b"keep").unwrap();

        let error = remove_model(root.path(), &expected).unwrap_err();

        assert!(error.contains("unexpected model entry"), "{error}");
        assert_eq!(std::fs::read(&unexpected).unwrap(), b"keep");
        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    }

    #[cfg(unix)]
    #[test]
    fn removing_a_busy_model_is_rejected() {
        let root = tempdir().unwrap();
        let expected = manifest("demo");
        write_artifact(root.path(), &expected.id);
        publish_manifest(root.path(), &expected).unwrap();
        let lock = ModelLock::acquire(&root.path().join("demo")).unwrap();

        let error = remove_model(root.path(), &expected).unwrap_err();

        assert!(error.contains("busy"), "{error}");
        assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
        drop(lock);
    }
}
