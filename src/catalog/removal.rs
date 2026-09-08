//! Remove only the validated model and its recognized recovery entries.
use super::{
    load_catalog, removable_bundle_debris, remove_regular_if_present, Manifest, ModelLock,
};
use std::fs;
use std::path::Path;

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

    const OWNED_ENTRIES: [&str; 10] = [
        ".lock",
        "manifest.json",
        "manifest.json.tmp",
        "pending.json",
        "pending.json.tmp",
        "verification-receipt.json",
        ".verification-receipt.json.tmp",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ];
    let declared_artifacts = manifest
        .artifacts
        .as_deref()
        .map(|artifacts| {
            artifacts
                .iter()
                .map(|artifact| artifact.local_filename.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![manifest.local_filename.as_str()]);
    let mut remove_after_validation = Vec::new();
    for entry in fs::read_dir(&model_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| format!("unexpected model entry {}", entry.path().display()))?;
        let is_declared_artifact = declared_artifacts.contains(&name);
        let is_bundle_debris = removable_bundle_debris(&entry.path(), name, manifest);
        if !OWNED_ENTRIES.contains(&name) && !is_declared_artifact && !is_bundle_debris {
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
        if name != ".lock" && name != "manifest.json" {
            remove_after_validation.push(entry.path());
        }
    }

    for path in remove_after_validation {
        remove_regular_if_present(&path)?;
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
