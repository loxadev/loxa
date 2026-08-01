use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::{Manifest, Origin};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub id: String,
    pub path: PathBuf,
    pub filename: String,
    pub size: u64,
    pub kind: CandidateKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateKind {
    Runnable,
    Auxiliary,
}

pub fn discover(models_root: &Path) -> Result<Vec<Candidate>, String> {
    if !models_root.exists() {
        return Ok(Vec::new());
    }
    let mut grouped = BTreeMap::<String, Vec<Candidate>>::new();
    for entry in fs::read_dir(models_root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        let Some(filename) = name.to_str() else {
            continue;
        };
        let Some(id) = candidate_id(filename) else {
            continue;
        };
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if !metadata.file_type().is_file() || metadata.len() <= 8 {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                continue;
            }
        }
        if !has_supported_gguf_header(&path)? {
            continue;
        }
        let candidate = Candidate {
            id: id.clone(),
            path,
            filename: filename.to_owned(),
            size: metadata.len(),
            kind: classify(filename),
        };
        grouped.entry(id).or_default().push(candidate);
    }
    let mut candidates = BTreeMap::new();
    for (base, group) in grouped {
        if group.len() == 1 {
            let candidate = group.into_iter().next().expect("group is nonempty");
            candidates.insert(candidate.id.clone(), candidate);
            continue;
        }
        for mut candidate in group {
            candidate.id = disambiguated_id(&base, &candidate.filename);
            if candidates.insert(candidate.id.clone(), candidate).is_some() {
                return Err("local GGUF filename identity collision".into());
            }
        }
    }
    Ok(candidates.into_values().collect())
}

pub fn adopt(models_root: &Path, candidate: &Candidate) -> Result<Manifest, String> {
    let current = discover(models_root)?
        .into_iter()
        .find(|entry| entry.id == candidate.id && entry.path == candidate.path)
        .ok_or_else(|| {
            format!(
                "local GGUF candidate {} changed or disappeared",
                candidate.id
            )
        })?;
    if &current != candidate {
        return Err(format!(
            "local GGUF candidate {} changed or disappeared",
            candidate.id
        ));
    }
    if candidate.kind != CandidateKind::Runnable {
        return Err(format!(
            "local GGUF auxiliary {} is not runnable",
            candidate.id
        ));
    }
    let before = FileIdentity::read(&candidate.path)?;
    let sha256 = sha256(&candidate.path)?;
    if FileIdentity::read(&candidate.path)? != before {
        return Err(format!(
            "local GGUF candidate {} changed while hashing",
            candidate.id
        ));
    }
    let manifest = Manifest {
        version: 2,
        id: candidate.id.clone(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(Origin::Local),
        source_filename: Some(candidate.filename.clone()),
        local_filename: "model.gguf".into(),
        sha256,
        size: candidate.size,
    };
    manifest.validate()?;

    if super::load_catalog(models_root)?
        .iter()
        .any(|entry| entry.id == manifest.id)
    {
        return Err(format!("model id {} is already installed", manifest.id));
    }
    let model_dir = models_root.join(&manifest.id);
    let _lock = super::ModelLock::acquire(&model_dir)?;
    ensure_empty_adoption_directory(&model_dir)?;
    if FileIdentity::read(&candidate.path)? != before {
        return Err(format!(
            "local GGUF candidate {} changed before adoption",
            candidate.id
        ));
    }
    super::prepare_pull(&model_dir, &manifest)?;
    let destination = manifest.artifact_path(models_root);
    if fs::symlink_metadata(&destination).is_ok() {
        return Err(format!("model id {} has an existing artifact", manifest.id));
    }
    if FileIdentity::read(&candidate.path)? != before {
        fs::remove_file(model_dir.join("pending.json")).map_err(|error| error.to_string())?;
        return Err(format!(
            "local GGUF candidate {} changed before adoption",
            candidate.id
        ));
    }
    fs::rename(&candidate.path, &destination).map_err(|error| {
        format!(
            "failed to adopt {} into {}: {error}",
            candidate.path.display(),
            destination.display()
        )
    })?;
    super::publish_manifest(models_root, &manifest)?;
    Ok(manifest)
}

pub fn recover_pending(models_root: &Path) -> Result<(), String> {
    if !models_root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(models_root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            continue;
        }
        let model_dir = entry.path();
        let manifest_path = model_dir.join("manifest.json");
        let pending_path = model_dir.join("pending.json");
        if manifest_path.exists() {
            let manifest = read_local_pending(&manifest_path)?;
            let pending = read_local_pending(&pending_path)?;
            if manifest.is_some() && manifest == pending {
                let _lock = super::ModelLock::acquire(&model_dir)?;
                fs::remove_file(&pending_path).map_err(|error| error.to_string())?;
            }
            continue;
        }
        let pending = match read_local_pending(&pending_path)? {
            Some(pending) => pending,
            None => continue,
        };
        if model_dir.file_name().and_then(|name| name.to_str()) != Some(pending.id.as_str()) {
            return Err(format!(
                "pending manifest id does not match {}",
                model_dir.display()
            ));
        }
        let _lock = super::ModelLock::acquire(&model_dir)?;
        let artifact = pending.artifact_path(models_root);
        if artifact.exists() {
            FileIdentity::read(&artifact)?;
            super::publish_manifest(models_root, &pending)?;
            continue;
        }
        let source = models_root.join(
            pending
                .source_filename
                .as_deref()
                .expect("validated local manifest has source filename"),
        );
        if discover(models_root)?
            .iter()
            .any(|candidate| candidate.id == pending.id && candidate.path == source)
        {
            fs::remove_file(&pending_path).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn read_local_pending(path: &Path) -> Result<Option<Manifest>, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    if !metadata.file_type().is_file() {
        return Ok(None);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Ok(None);
        }
    }
    let manifest: Manifest = match serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?,
    ) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(None),
    };
    match (manifest.version, manifest.origin.as_ref()) {
        (2, Some(Origin::Local)) => {
            manifest.validate()?;
            Ok(Some(manifest))
        }
        _ => Ok(None),
    }
}

fn candidate_id(filename: &str) -> Option<String> {
    let extension = filename.len().checked_sub(5)?;
    if !filename[extension..].eq_ignore_ascii_case(".gguf") {
        return None;
    }
    let stem = &filename[..extension];
    let lower = stem.to_ascii_lowercase();
    if stem.is_empty()
        || stem.starts_with('.')
        || lower.contains("-of-")
        || [".part", ".tmp", ".restart", ".invalid"]
            .iter()
            .any(|suffix| lower.ends_with(suffix))
    {
        return None;
    }
    let id = stem
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() {
                byte.to_ascii_lowercase() as char
            } else {
                '-'
            }
        })
        .collect::<String>();
    let id = id.trim_matches('-');
    (!id.is_empty() && id.len() <= 120).then(|| id.to_owned())
}

fn classify(filename: &str) -> CandidateKind {
    let auxiliary = filename
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|part| {
            part.eq_ignore_ascii_case("mtp")
                || part.eq_ignore_ascii_case("draft")
                || part.eq_ignore_ascii_case("mmproj")
        });
    if auxiliary {
        CandidateKind::Auxiliary
    } else {
        CandidateKind::Runnable
    }
}

fn has_supported_gguf_header(path: &Path) -> Result<bool, String> {
    let mut header = [0_u8; 8];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let version = u32::from_le_bytes(header[4..].try_into().expect("header has four bytes"));
    Ok(&header[..4] == b"GGUF" && matches!(version, 2 | 3))
}

fn disambiguated_id(base: &str, filename: &str) -> String {
    let digest = Sha256::digest(filename.as_bytes());
    let suffix = hex(&digest[..4]);
    let limit = 120usize.saturating_sub(suffix.len() + 1);
    format!("{}-{suffix}", &base[..base.len().min(limit)])
        .trim_end_matches('-')
        .to_owned()
}

fn ensure_empty_adoption_directory(model_dir: &Path) -> Result<(), String> {
    for entry in fs::read_dir(model_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if entry.file_name() != ".lock" {
            return Err(format!(
                "model id {} already has local state",
                model_dir.display()
            ));
        }
    }
    Ok(())
}

#[derive(Eq, PartialEq)]
struct FileIdentity {
    size: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
}

impl FileIdentity {
    fn read(path: &Path) -> Result<Self, String> {
        let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
        if !metadata.file_type().is_file() || metadata.len() <= 8 {
            return Err(format!("unsafe local GGUF candidate {}", path.display()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                return Err(format!("unsafe local GGUF candidate {}", path.display()));
            }
            Ok(Self {
                size: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                size: metadata.len(),
            })
        }
    }
}

fn sha256(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex(hash.finalize().as_ref()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gguf(version: u32) -> Vec<u8> {
        let mut bytes = b"GGUF".to_vec();
        bytes.extend(version.to_le_bytes());
        bytes.extend(b"payload");
        bytes
    }

    #[test]
    fn discovers_an_immediate_regular_gguf_without_mutating_the_store() {
        let root = tempfile::tempdir().unwrap();
        let model = root.path().join("Gemma 4.Q4_K_M.gguf");
        std::fs::write(&model, gguf(3)).unwrap();

        let candidates = discover(root.path()).unwrap();

        assert_eq!(
            candidates,
            vec![Candidate {
                id: "gemma-4-q4-k-m".into(),
                path: model.clone(),
                filename: "Gemma 4.Q4_K_M.gguf".into(),
                size: 15,
                kind: CandidateKind::Runnable,
            }]
        );
        assert!(model.is_file());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn adoption_hashes_then_moves_a_candidate_into_a_truthful_local_manifest() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Gemma 4.gguf");
        std::fs::write(&source, gguf(3)).unwrap();
        let candidate = discover(root.path()).unwrap().pop().unwrap();

        let manifest = adopt(root.path(), &candidate).unwrap();

        assert_eq!(manifest.version, 2);
        assert_eq!(manifest.id, "gemma-4");
        assert_eq!(manifest.size, 15);
        assert!(!source.exists());
        let model_dir = root.path().join("gemma-4");
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf")).unwrap(),
            gguf(3)
        );
        let json = std::fs::read_to_string(model_dir.join("manifest.json")).unwrap();
        assert!(json.contains("\"origin\": \"local\""), "{json}");
        assert!(!json.contains("\"repo\""), "{json}");
    }

    #[test]
    fn colliding_friendly_filenames_receive_distinct_candidate_ids() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("Gemma 4.gguf"), gguf(3)).unwrap();
        std::fs::write(root.path().join("Gemma-4.gguf"), gguf(3)).unwrap();

        let candidates = discover(root.path()).unwrap();

        assert_eq!(candidates.len(), 2);
        assert_ne!(candidates[0].id, candidates[1].id);
        assert!(candidates
            .iter()
            .all(|candidate| candidate.id.starts_with("gemma-4-")));
    }

    #[test]
    fn ignores_temporary_split_nested_and_non_gguf_entries() {
        let root = tempfile::tempdir().unwrap();
        for name in [
            "model.part.gguf",
            "model.tmp.gguf",
            "model-of-00001.gguf",
            ".hidden.gguf",
            "model.bin",
        ] {
            std::fs::write(root.path().join(name), gguf(3)).unwrap();
        }
        let nested = root.path().join("qualification");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("nested.gguf"), gguf(3)).unwrap();

        assert!(discover(root.path()).unwrap().is_empty());
    }

    #[test]
    fn classifies_mtp_and_mmproj_files_as_auxiliaries() {
        let root = tempfile::tempdir().unwrap();
        for name in [
            "mtp-gemma.gguf",
            "draft-gemma.gguf",
            "mmproj_gemma.gguf",
            "gemma-mtp.gguf",
            "gemma-draft.gguf",
            "gemma-mmproj.gguf",
        ] {
            std::fs::write(root.path().join(name), gguf(3)).unwrap();
        }
        std::fs::write(root.path().join("drafting-model.gguf"), gguf(3)).unwrap();
        std::fs::write(root.path().join("Qwen.gguf.part"), gguf(3)).unwrap();

        let candidates = discover(root.path()).unwrap();

        assert_eq!(
            candidates
                .iter()
                .filter(|candidate| candidate.kind == CandidateKind::Runnable)
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            ["drafting-model"]
        );
        assert_eq!(
            candidates
                .iter()
                .filter(|candidate| candidate.kind == CandidateKind::Auxiliary)
                .count(),
            6
        );
    }

    #[test]
    fn recovery_publishes_a_verified_local_pending_adoption_after_a_crash() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Gemma 4.gguf");
        let bytes = gguf(3);
        std::fs::write(&source, &bytes).unwrap();
        let candidate = discover(root.path()).unwrap().pop().unwrap();
        let manifest = Manifest {
            version: 2,
            id: candidate.id.clone(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: Some(Origin::Local),
            source_filename: Some(candidate.filename.clone()),
            local_filename: "model.gguf".into(),
            sha256: sha256(&source).unwrap(),
            size: bytes.len() as u64,
        };
        let model_dir = root.path().join(&candidate.id);
        super::super::prepare_pull(&model_dir, &manifest).unwrap();
        std::fs::rename(&source, model_dir.join("model.gguf")).unwrap();

        recover_pending(root.path()).unwrap();

        assert!(model_dir.join("manifest.json").is_file());
        assert!(!model_dir.join("pending.json").exists());
        assert_eq!(
            super::super::load_catalog(root.path()).unwrap(),
            vec![manifest]
        );
    }

    #[test]
    fn recovery_removes_only_a_pending_marker_matching_a_published_local_manifest() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Gemma 4.gguf");
        std::fs::write(&source, gguf(3)).unwrap();
        let candidate = discover(root.path()).unwrap().pop().unwrap();
        let manifest = adopt(root.path(), &candidate).unwrap();
        let model_dir = root.path().join(&manifest.id);
        std::fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

        recover_pending(root.path()).unwrap();

        assert!(model_dir.join("manifest.json").is_file());
        assert!(!model_dir.join("pending.json").exists());
    }

    #[test]
    fn recovery_keeps_a_pending_marker_that_differs_from_the_published_manifest() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Gemma 4.gguf");
        std::fs::write(&source, gguf(3)).unwrap();
        let candidate = discover(root.path()).unwrap().pop().unwrap();
        let manifest = adopt(root.path(), &candidate).unwrap();
        let model_dir = root.path().join(&manifest.id);
        let mut pending = manifest.clone();
        pending.source_filename = Some("different.gguf".into());
        std::fs::write(
            model_dir.join("pending.json"),
            serde_json::to_vec(&pending).unwrap(),
        )
        .unwrap();

        recover_pending(root.path()).unwrap();

        assert!(model_dir.join("pending.json").is_file());
    }
}
