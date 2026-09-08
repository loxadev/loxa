//! Catalog schema, inventory, and guarded mutation entry points.
use std::fs;
use std::path::Path;

mod inventory;
pub mod local;
mod lock;
mod manifest;
mod publication;
mod removal;
mod transaction;
pub(crate) mod transfer;

pub use inventory::load_catalog;
pub(crate) use inventory::load_reconciled_catalog;
pub use lock::ModelLock;
pub(crate) use lock::{model_is_busy, ModelLockError};
pub use manifest::{
    is_qualified_gemma4_bundle, Artifact, ArtifactProvenance, ArtifactRef, ArtifactRole, Manifest,
    Origin, RuntimeQualification, GEMMA4_BUNDLED_LLAMA_BUILD, GEMMA4_DRAFT_SHA256,
    GEMMA4_DRAFT_SIZE, GEMMA4_LEGACY_LLAMA_BUILD, GEMMA4_LLAMA_BUILD, GEMMA4_MODEL_SHA256,
    GEMMA4_MODEL_SIZE, GEMMA4_MTP_PROFILE,
};
#[cfg(test)]
pub(crate) use manifest::{TEST_LLAMA_BUILD, TEST_MTP_PROFILE};
pub(crate) use publication::publish_manifest_verified;
pub use publication::{prepare_pull, publish_manifest};
pub use removal::remove_model;

#[cfg(test)]
use inventory::load_reconciled_catalog_with;
#[cfg(test)]
use publication::{
    publish_manifest_verified_with_hook, publish_manifest_verified_with_recovery,
    publish_manifest_with_verifier, VerifiedPublicationPoint,
};
pub(crate) use transaction::{
    bundle_pending, cleanup_completed_bundle_debris, move_no_replace_and_sync,
    prepare_bundle_upgrade, removable_bundle_debris, replace_manifest_atomic, BundlePending,
    NoReplaceRename,
};
#[cfg(test)]
pub(crate) use transaction::{replace_manifest_atomic_with_hook, ManifestPublicationPoint};

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

#[cfg(test)]
mod tests;
