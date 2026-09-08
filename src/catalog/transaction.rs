use super::{ArtifactProvenance, ArtifactRole, Manifest};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NoReplaceRename {
    Renamed,
    Exists,
}

pub(crate) enum BundlePending {
    Absent,
    Valid(Box<Manifest>),
    UnsafeOrInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestPublicationPoint {
    BeforeExchange,
    AfterExchange,
}

pub(crate) fn rename_no_replace(
    source: &Path,
    destination: &Path,
) -> Result<NoReplaceRename, String> {
    let source_parent = source
        .parent()
        .ok_or_else(|| format!("missing source parent for {}", source.display()))?;
    let source_name = source
        .file_name()
        .ok_or_else(|| format!("missing source filename for {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| format!("missing destination parent for {}", destination.display()))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| format!("missing destination filename for {}", destination.display()))?;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        use rustix::fs::{renameat_with, RenameFlags};
        use rustix::io::Errno;

        let source_dir = fs::File::open(source_parent)
            .map_err(|error| format!("{}: {error}", source_parent.display()))?;
        let destination_dir = fs::File::open(destination_parent)
            .map_err(|error| format!("{}: {error}", destination_parent.display()))?;
        match renameat_with(
            &source_dir,
            source_name,
            &destination_dir,
            destination_name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => Ok(NoReplaceRename::Renamed),
            Err(Errno::EXIST) => Ok(NoReplaceRename::Exists),
            Err(error) => Err(format!(
                "cannot rename {} into {} without replacement: {error}",
                source.display(),
                destination.display()
            )),
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (
            source_parent,
            source_name,
            destination_parent,
            destination_name,
        );
        Err("bundle upgrades require a macOS or Linux no-replace rename primitive".into())
    }
}

pub(crate) fn move_no_replace_and_sync(
    source: &Path,
    destination: &Path,
) -> Result<NoReplaceRename, String> {
    let source_parent = source
        .parent()
        .ok_or_else(|| format!("missing source parent for {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| format!("missing destination parent for {}", destination.display()))?;
    let result = rename_no_replace(source, destination)?;
    if result == NoReplaceRename::Renamed {
        sync_catalog_directory(destination_parent)?;
        if source_parent != destination_parent {
            sync_catalog_directory(source_parent)?;
        }
    }
    Ok(result)
}

fn rename_exchange(left: &Path, right: &Path) -> Result<(), String> {
    let left_parent = left
        .parent()
        .ok_or_else(|| format!("missing source parent for {}", left.display()))?;
    let left_name = left
        .file_name()
        .ok_or_else(|| format!("missing source filename for {}", left.display()))?;
    let right_parent = right
        .parent()
        .ok_or_else(|| format!("missing destination parent for {}", right.display()))?;
    let right_name = right
        .file_name()
        .ok_or_else(|| format!("missing destination filename for {}", right.display()))?;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        use rustix::fs::{renameat_with, RenameFlags};

        let left_dir = fs::File::open(left_parent)
            .map_err(|error| format!("{}: {error}", left_parent.display()))?;
        let right_dir = fs::File::open(right_parent)
            .map_err(|error| format!("{}: {error}", right_parent.display()))?;
        renameat_with(
            &left_dir,
            left_name,
            &right_dir,
            right_name,
            RenameFlags::EXCHANGE,
        )
        .map_err(|error| {
            format!(
                "cannot atomically exchange {} with {}: {error}",
                left.display(),
                right.display()
            )
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (left_parent, left_name, right_parent, right_name);
        Err("bundle upgrades require a macOS or Linux exchange rename primitive".into())
    }
}

pub(crate) fn prepare_bundle_upgrade(
    model_dir: &Path,
    replacement: &Manifest,
) -> Result<bool, String> {
    replacement.validate()?;
    if model_dir.file_name().and_then(|name| name.to_str()) != Some(replacement.id.as_str()) {
        return Err("bundle manifest id does not match model directory".into());
    }
    let pending_path = model_dir.join("bundle.pending.json");
    let temp_path = write_bundle_manifest_temp(model_dir, "bundle-pending", replacement)?;
    match rename_no_replace(&temp_path, &pending_path) {
        Ok(NoReplaceRename::Renamed) => {
            sync_catalog_directory(model_dir)?;
            Ok(true)
        }
        Ok(NoReplaceRename::Exists) => {
            remove_manifest_temp_if_matches(&temp_path, replacement);
            Ok(read_regular_manifest(&pending_path).is_ok_and(|existing| {
                existing
                    .as_ref()
                    .is_some_and(|existing| existing == replacement)
            }))
        }
        Err(_) => {
            remove_manifest_temp_if_matches(&temp_path, replacement);
            Ok(false)
        }
    }
}

pub(crate) fn replace_manifest_atomic(
    models_root: &Path,
    expected: &Manifest,
    replacement: &Manifest,
) -> Result<bool, String> {
    replace_manifest_atomic_with_observer(models_root, expected, replacement, |_| Ok(()))
}

#[cfg(test)]
pub(crate) fn replace_manifest_atomic_with_hook<F>(
    models_root: &Path,
    expected: &Manifest,
    replacement: &Manifest,
    observer: F,
) -> Result<bool, String>
where
    F: FnMut(ManifestPublicationPoint) -> Result<(), String>,
{
    replace_manifest_atomic_with_observer(models_root, expected, replacement, observer)
}

fn replace_manifest_atomic_with_observer<F>(
    models_root: &Path,
    expected: &Manifest,
    replacement: &Manifest,
    mut observer: F,
) -> Result<bool, String>
where
    F: FnMut(ManifestPublicationPoint) -> Result<(), String>,
{
    replacement.validate()?;
    if !replacement_artifacts_are_verified(models_root, replacement)? {
        return Ok(false);
    }
    let model_dir = models_root.join(&replacement.id);
    let manifest_path = model_dir.join("manifest.json");
    if !read_regular_manifest(&manifest_path)
        .is_ok_and(|current| current.as_ref() == Some(expected))
    {
        return Ok(false);
    }
    let temp_path = write_bundle_manifest_temp(&model_dir, "bundle-manifest", replacement)?;
    if let Err(error) = observer(ManifestPublicationPoint::BeforeExchange) {
        remove_manifest_temp_if_matches(&temp_path, replacement);
        return Err(error);
    }
    if rename_exchange(&manifest_path, &temp_path).is_err() {
        remove_manifest_temp_if_matches(&temp_path, replacement);
        return Ok(false);
    }
    if let Err(error) = sync_catalog_directory(&model_dir) {
        return rollback_exchange(
            &model_dir,
            &manifest_path,
            &temp_path,
            format!("cannot persist exchanged manifest: {error}"),
        );
    }
    if !read_regular_manifest(&temp_path).is_ok_and(|current| current.as_ref() == Some(expected)) {
        return rollback_exchange(
            &model_dir,
            &manifest_path,
            &temp_path,
            "manifest changed during bundle publication".into(),
        );
    }
    if !replacement_artifacts_are_verified(models_root, replacement)? {
        return rollback_exchange(
            &model_dir,
            &manifest_path,
            &temp_path,
            "bundle artifact changed during manifest publication".into(),
        );
    }
    observer(ManifestPublicationPoint::AfterExchange)?;
    if !read_regular_manifest(&manifest_path)
        .is_ok_and(|current| current.as_ref() == Some(replacement))
    {
        return Err("manifest changed after bundle publication; retained recovery state".into());
    }
    let _ = cleanup_completed_bundle_debris(&model_dir, replacement);
    Ok(true)
}

fn replacement_artifacts_are_verified(
    models_root: &Path,
    replacement: &Manifest,
) -> Result<bool, String> {
    for artifact in replacement
        .artifacts
        .as_deref()
        .ok_or("replacement is not a bundle")?
    {
        if crate::verification::file::verify_regular(
            &models_root
                .join(&replacement.id)
                .join(&artifact.local_filename),
            artifact.size,
            &artifact.sha256,
        )
        .is_err()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn rollback_exchange(
    model_dir: &Path,
    manifest_path: &Path,
    predecessor_path: &Path,
    reason: String,
) -> Result<bool, String> {
    rename_exchange(manifest_path, predecessor_path)
        .map_err(|error| format!("{reason}; rollback exchange failed: {error}"))?;
    sync_catalog_directory(model_dir)
        .map_err(|error| format!("{reason}; rollback directory sync failed: {error}"))?;
    Err(format!(
        "{reason}; restored the prior manifest and retained transaction state"
    ))
}

pub(crate) fn bundle_pending(model_dir: &Path) -> BundlePending {
    let path = model_dir.join("bundle.pending.json");
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => BundlePending::Absent,
        Err(_) => BundlePending::UnsafeOrInvalid,
        Ok(_) => match read_regular_manifest(&path) {
            Ok(Some(manifest)) => BundlePending::Valid(Box::new(manifest)),
            Ok(None) | Err(_) => BundlePending::UnsafeOrInvalid,
        },
    }
}

static BUNDLE_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_bundle_manifest_temp(
    dir: &Path,
    purpose: &str,
    manifest: &Manifest,
) -> Result<PathBuf, String> {
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?;
    for _ in 0..64 {
        let sequence = BUNDLE_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".{purpose}-{}-{sequence}.tmp", std::process::id()));
        let mut file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        let write_result = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("{}: {error}", path.display()));
        if let Err(error) = write_result {
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        return Ok(path);
    }
    Err(format!(
        "could not allocate a unique {purpose} transaction file"
    ))
}

fn read_regular_manifest(path: &Path) -> Result<Option<Manifest>, String> {
    let bytes = match crate::safe_file::read_regular_file(path) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    let manifest: Manifest = match serde_json::from_slice(&bytes) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(None),
    };
    if manifest.validate().is_err() {
        return Ok(None);
    }
    Ok(Some(manifest))
}

pub(crate) fn cleanup_completed_bundle_debris(
    model_dir: &Path,
    complete: &Manifest,
) -> Result<(), String> {
    if !is_complete_bundle(complete) {
        return Ok(());
    }
    let mut removed = false;
    for entry in fs::read_dir(model_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if removable_bundle_debris(&entry.path(), name, complete) {
            fs::remove_file(entry.path()).map_err(|error| error.to_string())?;
            removed = true;
        }
    }
    if removed {
        sync_catalog_directory(model_dir)?;
    }
    Ok(())
}

pub(crate) fn removable_bundle_debris(path: &Path, name: &str, complete: &Manifest) -> bool {
    if !is_complete_bundle(complete) {
        return false;
    }
    let Some(kind) = bundle_debris_kind(name) else {
        return false;
    };
    let Some(existing) = read_regular_manifest(path).ok().flatten() else {
        return false;
    };
    match kind {
        BundleDebris::Pending | BundleDebris::PendingTemp => existing == *complete,
        BundleDebris::PredecessorTemp => {
            existing == *complete || compatible_predecessor(&existing, complete)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BundleDebris {
    Pending,
    PendingTemp,
    PredecessorTemp,
}

fn bundle_debris_kind(name: &str) -> Option<BundleDebris> {
    if name == "bundle.pending.json" {
        return Some(BundleDebris::Pending);
    }
    let (prefix, kind) = [
        (".bundle-pending-", BundleDebris::PendingTemp),
        (".bundle-manifest-", BundleDebris::PredecessorTemp),
    ]
    .into_iter()
    .find(|(prefix, _)| name.starts_with(prefix))?;
    let suffix = name.strip_prefix(prefix)?.strip_suffix(".tmp")?;
    let (process, sequence) = suffix.rsplit_once('-')?;
    (is_decimal(process) && is_decimal(sequence)).then_some(kind)
}

fn is_decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_complete_bundle(manifest: &Manifest) -> bool {
    manifest.version == 3 && manifest.draft_artifact().is_some()
}

fn compatible_predecessor(predecessor: &Manifest, complete: &Manifest) -> bool {
    predecessor.draft_artifact().is_none()
        && predecessor.id == complete.id
        && predecessor.primary_artifact().local_filename
            == complete.primary_artifact().local_filename
        && predecessor.primary_artifact().sha256 == complete.primary_artifact().sha256
        && predecessor.primary_artifact().size == complete.primary_artifact().size
        && matching_primary_provenance(predecessor, complete)
}

fn matching_primary_provenance(predecessor: &Manifest, complete: &Manifest) -> bool {
    let Some(complete_model) = complete.artifacts.as_deref().and_then(|artifacts| {
        artifacts
            .iter()
            .find(|artifact| artifact.role == ArtifactRole::Model)
    }) else {
        return false;
    };
    match predecessor.version {
        2 => matches!(
            &complete_model.provenance,
            ArtifactProvenance::Local { source_filename }
                if predecessor.source_filename.as_deref() == Some(source_filename)
        ),
        3 => predecessor
            .artifacts
            .as_deref()
            .and_then(|artifacts| {
                artifacts
                    .iter()
                    .find(|artifact| artifact.role == ArtifactRole::Model)
            })
            .is_some_and(|model| model.provenance == complete_model.provenance),
        _ => false,
    }
}

fn remove_manifest_temp_if_matches(path: &Path, expected: &Manifest) {
    if read_regular_manifest(path).is_ok_and(|existing| existing.as_ref() == Some(expected)) {
        let _ = fs::remove_file(path);
    }
}

fn sync_catalog_directory(dir: &Path) -> Result<(), String> {
    fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())
}
