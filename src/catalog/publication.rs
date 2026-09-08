//! Pending-manifest preparation and catalog publication entry points.
mod verified;
pub(crate) use verified::publish_manifest_verified;
#[cfg(test)]
pub(super) use verified::{
    publish_manifest_verified_with_hook, publish_manifest_verified_with_recovery,
    VerifiedPublicationPoint,
};

use super::{ensure_catalog_directory, remove_regular_if_present, Manifest};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

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
    publish_manifest_with_verifier(models_root, manifest, |path, size, sha256| {
        crate::verification::file::verify_regular(path, size, sha256)
    })
}

pub(super) fn publish_manifest_with_verifier<F>(
    models_root: &Path,
    manifest: &Manifest,
    mut verify: F,
) -> Result<PathBuf, String>
where
    F: FnMut(&Path, u64, &str) -> Result<(), String>,
{
    manifest.validate()?;
    if let Some(artifacts) = manifest.artifacts.as_deref() {
        for artifact in artifacts {
            verify(
                &models_root
                    .join(&manifest.id)
                    .join(&artifact.local_filename),
                artifact.size,
                &artifact.sha256,
            )?;
        }
    } else {
        verify(
            &manifest.artifact_path(models_root),
            manifest.size,
            &manifest.sha256,
        )?;
    }
    let dir = models_root.join(&manifest.id);
    ensure_catalog_directory(&dir)?;
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
