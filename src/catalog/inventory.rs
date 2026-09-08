//! Catalog observation and the existing qualified-bundle reconciliation entry.
use super::{local, Manifest};
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
        let bytes = match crate::safe_file::read_regular_file(&manifest_path) {
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
