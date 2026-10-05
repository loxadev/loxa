//! Remove only the validated model and its recognized recovery entries.
use super::Manifest;
#[cfg(unix)]
use super::{transaction, ModelLock, MAX_CATALOG_MANIFEST_BYTES};
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{self, Read};
use std::path::Path;

pub fn remove_model(models_root: &Path, manifest: &Manifest) -> Result<(), String> {
    #[cfg(unix)]
    {
        remove_model_with_hook(models_root, manifest, |_| Ok(()))
    }
    #[cfg(not(unix))]
    {
        let _ = models_root;
        manifest.validate()?;
        Err("model removal requires Unix directory-relative operations".into())
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RemovalPoint<'a> {
    AfterAudit,
    BeforeUnlink(&'a str),
}

#[cfg(unix)]
pub(super) fn remove_model_with_hook(
    models_root: &Path,
    manifest: &Manifest,
    mut hook: impl FnMut(RemovalPoint<'_>) -> Result<(), String>,
) -> Result<(), String> {
    manifest.validate()?;
    let model_dir = models_root.join(&manifest.id);
    if model_dir.file_name().and_then(|name| name.to_str()) != Some(manifest.id.as_str()) {
        return Err("model id does not match model directory".into());
    }
    let lock = ModelLock::acquire(&model_dir)?;
    let directory = lock.model_directory();
    let mut installed = match RemovalEntry::capture(directory, &model_dir, "manifest.json") {
        Ok(entry) => entry,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(format!("unknown model id {}", manifest.id));
        }
        Err(error) => {
            return Err(format!(
                "{}: {error}",
                model_dir.join("manifest.json").display()
            ))
        }
    };
    let current = installed.read_manifest(directory, &model_dir)?;
    if current.id != manifest.id {
        return Err(format!(
            "manifest id does not match {}",
            model_dir.display()
        ));
    }
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
    let mut entries = rustix::fs::Dir::read_from(directory).map_err(|error| error.to_string())?;
    let mut saw_manifest = false;
    while let Some(entry) = entries.read() {
        use std::os::unix::ffi::OsStrExt as _;

        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        let path = model_dir.join(std::ffi::OsStr::from_bytes(name.to_bytes()));
        let name = name
            .to_str()
            .map_err(|_| format!("unexpected model entry {}", path.display()))?;
        let is_declared_artifact = declared_artifacts.contains(&name);
        let bundle_kind = transaction::bundle_debris_kind(name, manifest);
        if !OWNED_ENTRIES.contains(&name) && !is_declared_artifact && bundle_kind.is_none() {
            return Err(format!("unexpected model entry {}", path.display()));
        }
        if name == "manifest.json" {
            installed.revalidate(directory, &model_dir)?;
            saw_manifest = true;
            continue;
        }
        let mut entry = RemovalEntry::capture(directory, &model_dir, name)
            .map_err(|error| format!("unsafe model entry {}: {error}", path.display()))?;
        if let Some(kind) = bundle_kind {
            let existing = entry
                .read_manifest(directory, &model_dir)
                .map_err(|_| format!("unexpected model entry {}", path.display()))?;
            if !transaction::removable_bundle_debris_manifest(kind, &existing, manifest) {
                return Err(format!("unexpected model entry {}", path.display()));
            }
        }
        if name == "pending.json" {
            let pending = entry.read_manifest(directory, &model_dir)?;
            if pending != *manifest {
                return Err(format!(
                    "model {} has recovery state for a different artifact",
                    manifest.id
                ));
            }
        }
        if name != ".lock" {
            remove_after_validation.push(entry);
        }
    }
    if !saw_manifest {
        return Err(format!("model {} changed before removal", manifest.id));
    }

    hook(RemovalPoint::AfterAudit)?;
    lock.revalidate()?;
    installed.revalidate(directory, &model_dir)?;
    for entry in &remove_after_validation {
        entry.revalidate(directory, &model_dir)?;
    }
    for entry in &remove_after_validation {
        entry.unlink(&lock, &model_dir, &mut hook)?;
    }
    directory.sync_all().map_err(|error| error.to_string())?;
    installed.unlink(&lock, &model_dir, &mut hook)?;
    directory.sync_all().map_err(|error| error.to_string())?;
    lock.revalidate()?;
    Ok(())
}

#[cfg(unix)]
struct RemovalEntry {
    name: String,
    file: fs::File,
    identity: crate::safe_file::RegularFileIdentity,
}

#[cfg(unix)]
impl RemovalEntry {
    fn capture(directory: &fs::File, model_dir: &Path, name: &str) -> io::Result<Self> {
        let file = open_entry(directory, name)?;
        let identity = crate::safe_file::regular_file_identity(&file, &model_dir.join(name))?;
        Ok(Self {
            name: name.into(),
            file,
            identity,
        })
    }

    fn revalidate(&self, directory: &fs::File, model_dir: &Path) -> Result<(), String> {
        let path = model_dir.join(&self.name);
        let resolved = open_entry(directory, &self.name)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        crate::safe_file::ensure_regular_descriptors_match(
            &self.file,
            &self.identity,
            &resolved,
            &path,
        )
        .map_err(|error| format!("{}: {error}", path.display()))
    }

    fn read_manifest(
        &mut self,
        directory: &fs::File,
        model_dir: &Path,
    ) -> Result<Manifest, String> {
        let path = model_dir.join(&self.name);
        let limit = MAX_CATALOG_MANIFEST_BYTES as u64;
        if self.identity.size() > limit {
            return Err(format!("{} exceeds its byte limit", path.display()));
        }
        let mut bytes = Vec::new();
        self.file
            .by_ref()
            .take(limit + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        self.revalidate(directory, model_dir)?;
        if bytes.len() > MAX_CATALOG_MANIFEST_BYTES {
            return Err(format!("{} exceeds its byte limit", path.display()));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn unlink(
        &self,
        lock: &ModelLock,
        model_dir: &Path,
        hook: &mut impl FnMut(RemovalPoint<'_>) -> Result<(), String>,
    ) -> Result<(), String> {
        lock.revalidate_for(model_dir)?;
        self.revalidate(lock.model_directory(), model_dir)?;
        hook(RemovalPoint::BeforeUnlink(&self.name))?;
        // The directory remains authoritative if its path changes after the check.
        // Child identity checks do not make unlink an atomic inode comparison.
        rustix::fs::unlinkat(
            lock.model_directory(),
            self.name.as_str(),
            rustix::fs::AtFlags::empty(),
        )
        .map_err(|error| format!("{}: {error}", model_dir.join(&self.name).display()))
    }
}

#[cfg(unix)]
fn open_entry(directory: &fs::File, name: &str) -> io::Result<fs::File> {
    rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map(fs::File::from)
    .map_err(Into::into)
}
