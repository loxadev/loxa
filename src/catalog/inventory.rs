//! Catalog observation and the existing qualified-bundle reconciliation entry.
use super::{local, Manifest, MAX_CATALOG_MANIFEST_BYTES};
use std::fs;
use std::path::Path;

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
        let bytes = match crate::safe_file::read_regular_file_bounded(
            &manifest_path,
            MAX_CATALOG_MANIFEST_BYTES,
        ) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("{}: {error}", manifest_path.display())),
        };
        let manifest: Manifest = serde_json::from_slice(&bytes)
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

pub(crate) fn load_model_manifest(
    models_root: &Path,
    model_id: &str,
) -> Result<Option<Manifest>, String> {
    const MAX_HISTORY_MANIFEST_BYTES: usize = 256 * 1024;

    crate::paths::validate_id(model_id)?;
    let model_dir = models_root.join(model_id);
    let (directory, identity) = match crate::safe_file::open_directory(&model_dir) {
        Ok(opened) => opened,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("model catalog contains unsafe local state".into()),
    };
    let manifest_path = model_dir.join("manifest.json");
    let bytes =
        crate::safe_file::read_regular_file_bounded(&manifest_path, MAX_HISTORY_MANIFEST_BYTES);
    crate::safe_file::ensure_directory_descriptor_matches_path(&directory, &identity, &model_dir)
        .map_err(|_| "model directory changed while its manifest was read".to_string())?;
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("model manifest is unavailable or exceeds its byte limit".into()),
    };
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| "model manifest is malformed".to_string())?;
    manifest.validate()?;
    if manifest.id != model_id {
        return Err("model manifest identity does not match its directory".into());
    }
    Ok(Some(manifest))
}

pub(crate) fn load_reconciled_catalog(models_root: &Path) -> Result<Vec<Manifest>, String> {
    load_reconciled_catalog_with(models_root, local::reconcile_qualified_bundle)
}

pub(super) fn load_reconciled_catalog_with<F>(
    models_root: &Path,
    reconcile: F,
) -> Result<Vec<Manifest>, String>
where
    F: FnOnce(&Path) -> Result<Option<Manifest>, String>,
{
    match reconcile(models_root) {
        Ok(Some(manifest)) => {
            tracing::info!(target: "loxa::catalog", event = "bundle_reconciled", model_id = %manifest.id)
        }
        Ok(None) => {
            tracing::debug!(target: "loxa::catalog", event = "bundle_reconciliation_not_needed")
        }
        Err(_) => tracing::warn!(target: "loxa::catalog", event = "bundle_reconciliation_failed"),
    }
    load_catalog(models_root)
}
