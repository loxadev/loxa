//! Captured-file publication, exact visible-manifest checks and guarded rollback.
use crate::catalog::{transfer, Manifest, ModelLock};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) fn publish_manifest_verified(
    models_root: &Path,
    manifest: &Manifest,
    model_lock: &ModelLock,
    verified: &crate::verification::file::VerifiedRegularFile,
) -> Result<PathBuf, String> {
    publish_manifest_verified_inner(
        models_root,
        manifest,
        model_lock,
        verified,
        |_| Ok(()),
        transfer::recover_installed_completion,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::catalog) enum VerifiedPublicationPoint {
    AfterProof,
    AfterManifestVisible,
}

#[cfg(test)]
pub(in crate::catalog) fn publish_manifest_verified_with_hook<F>(
    models_root: &Path,
    manifest: &Manifest,
    model_lock: &ModelLock,
    verified: &crate::verification::file::VerifiedRegularFile,
    hook: F,
) -> Result<PathBuf, String>
where
    F: FnMut(VerifiedPublicationPoint) -> Result<(), String>,
{
    publish_manifest_verified_inner(
        models_root,
        manifest,
        model_lock,
        verified,
        hook,
        transfer::recover_installed_completion,
    )
}

#[cfg(test)]
pub(in crate::catalog) fn publish_manifest_verified_with_recovery<F, R>(
    models_root: &Path,
    manifest: &Manifest,
    model_lock: &ModelLock,
    verified: &crate::verification::file::VerifiedRegularFile,
    hook: F,
    recover: R,
) -> Result<PathBuf, String>
where
    F: FnMut(VerifiedPublicationPoint) -> Result<(), String>,
    R: FnMut(
        &ModelLock,
        &Manifest,
        &transfer::CatalogTransferPlan,
    ) -> Result<(), transfer::CatalogMutationError>,
{
    publish_manifest_verified_inner(models_root, manifest, model_lock, verified, hook, recover)
}

fn publish_manifest_verified_inner<F, R>(
    models_root: &Path,
    manifest: &Manifest,
    model_lock: &ModelLock,
    verified: &crate::verification::file::VerifiedRegularFile,
    mut hook: F,
    mut recover: R,
) -> Result<PathBuf, String>
where
    F: FnMut(VerifiedPublicationPoint) -> Result<(), String>,
    R: FnMut(
        &ModelLock,
        &Manifest,
        &transfer::CatalogTransferPlan,
    ) -> Result<(), transfer::CatalogMutationError>,
{
    manifest.validate()?;
    if !matches!(manifest.version, 1 | 2) || manifest.artifacts.is_some() {
        return Err(
            "captured publication requires a validated single-file v1 or v2 manifest".into(),
        );
    }
    let model_dir = models_root.join(&manifest.id);
    model_lock.revalidate_for(&model_dir)?;
    let primary = manifest.primary_artifact();
    let primary_path = manifest.artifact_path(models_root);
    verified.proves(&primary_path, primary.size, primary.sha256)?;
    hook(VerifiedPublicationPoint::AfterProof)?;
    model_lock.revalidate_for(&model_dir)?;
    verified.proves(&primary_path, primary.size, primary.sha256)?;
    if manifest.version == 1 {
        publish_remote_manifest_at(
            model_lock,
            &model_dir,
            manifest,
            verified,
            &primary_path,
            &mut hook,
            &mut recover,
        )?;
    } else {
        let _published = publish_manifest_at(model_lock, &model_dir, manifest, true)?;
    }
    model_lock.revalidate_for(&model_dir)?;
    Ok(model_dir.join("manifest.json"))
}

fn publish_remote_manifest_at<F, R>(
    model_lock: &ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    verified: &crate::verification::file::VerifiedRegularFile,
    primary_path: &Path,
    hook: &mut F,
    recover: &mut R,
) -> Result<(), String>
where
    F: FnMut(VerifiedPublicationPoint) -> Result<(), String>,
    R: FnMut(
        &ModelLock,
        &Manifest,
        &transfer::CatalogTransferPlan,
    ) -> Result<(), transfer::CatalogMutationError>,
{
    use transfer::CatalogTransferState;

    let plan = transfer::plan_transfer(model_lock, manifest);
    match plan.state() {
        CatalogTransferState::Installed => Ok(()),
        CatalogTransferState::InstalledCompletionDebris => {
            match recover(model_lock, manifest, &plan) {
                Ok(()) => Ok(()),
                Err(transfer::CatalogMutationError::Changed)
                    if transfer::plan_transfer(model_lock, manifest).state()
                        == CatalogTransferState::Installed =>
                {
                    Ok(())
                }
                Err(_) => Err("remote manifest completion state changed".into()),
            }
        }
        CatalogTransferState::MatchingPending => {
            let published = publish_manifest_at(model_lock, model_dir, manifest, false)?;
            let before_cleanup = (|| {
                hook(VerifiedPublicationPoint::AfterManifestVisible)?;
                model_lock.revalidate_for(model_dir)?;
                let primary = manifest.primary_artifact();
                verified.proves(primary_path, primary.size, primary.sha256)?;
                Ok::<_, String>(transfer::plan_transfer(model_lock, manifest))
            })();
            let completion = match before_cleanup {
                Ok(completion) => completion,
                Err(error) => {
                    published.rollback(model_lock.model_directory())?;
                    return Err(error);
                }
            };
            match completion.state() {
                CatalogTransferState::Installed => Ok(()),
                CatalogTransferState::InstalledCompletionDebris => {
                    match recover(model_lock, manifest, &completion) {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            let current = transfer::plan_transfer(model_lock, manifest);
                            if current.state() == CatalogTransferState::Installed {
                                return match error {
                                    transfer::CatalogMutationError::Changed => Ok(()),
                                    transfer::CatalogMutationError::Durability => {
                                        Err("remote manifest completion durability failed".into())
                                    }
                                };
                            }
                            published.rollback(model_lock.model_directory())?;
                            Err("remote manifest completion state changed".into())
                        }
                    }
                }
                CatalogTransferState::Fresh
                | CatalogTransferState::MatchingPending
                | CatalogTransferState::ArtifactConflict
                | CatalogTransferState::Unsafe => {
                    published.rollback(model_lock.model_directory())?;
                    Err("remote manifest completion state changed".into())
                }
            }
        }
        CatalogTransferState::Fresh
        | CatalogTransferState::ArtifactConflict
        | CatalogTransferState::Unsafe => Err("remote manifest publication state changed".into()),
    }
}

struct PublishedManifest {
    file: fs::File,
    identity: crate::safe_file::RegularFileIdentity,
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl PublishedManifest {
    fn revalidate(&self, directory: &fs::File) -> Result<(), String> {
        use std::os::fd::{AsRawFd, FromRawFd};

        // SAFETY: `directory` is a retained live directory descriptor and the
        // entry name is a fixed NUL-terminated catalog name.
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                c"manifest.json".as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if descriptor == -1 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        // SAFETY: `openat` returned a new owned descriptor which has not been wrapped.
        let resolved = unsafe { fs::File::from_raw_fd(descriptor) };
        crate::safe_file::ensure_regular_descriptors_match(
            &self.file,
            &self.identity,
            &resolved,
            Path::new("manifest.json"),
        )
        .map_err(|_| "published manifest changed".to_string())
    }

    fn rollback(&self, directory: &fs::File) -> Result<(), String> {
        self.revalidate(directory)?;
        unlinkat_if_present(directory, b"manifest.json\0")?;
        directory.sync_all().map_err(|error| error.to_string())
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn publish_manifest_at(
    model_lock: &ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    remove_pending: bool,
) -> Result<PublishedManifest, String> {
    use rustix::fs::{renameat_with, RenameFlags};
    use rustix::io::Errno;
    use std::os::fd::{AsRawFd, FromRawFd};

    const TEMP: &[u8] = b"manifest.json.tmp\0";
    const PENDING: &[u8] = b"pending.json\0";
    let directory = model_lock.model_directory();
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?;
    model_lock.revalidate_for(model_dir)?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            TEMP.as_ptr().cast(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600 as libc::c_uint,
        )
    };
    if descriptor == -1 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let mut temporary = unsafe { fs::File::from_raw_fd(descriptor) };
    if let Err(error) = temporary
        .write_all(&bytes)
        .and_then(|()| temporary.sync_all())
    {
        drop(temporary);
        let _ = unlinkat_if_present(directory, TEMP);
        return Err(error.to_string());
    }
    model_lock.revalidate_for(model_dir)?;
    match renameat_with(
        directory,
        "manifest.json.tmp",
        directory,
        "manifest.json",
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::EXIST) => {
            let _ = unlinkat_if_present(directory, TEMP);
            return Err(format!("manifest already exists for {}", manifest.id));
        }
        Err(error) => {
            let _ = unlinkat_if_present(directory, TEMP);
            return Err(error.to_string());
        }
    }
    directory.sync_all().map_err(|error| error.to_string())?;
    let identity = crate::safe_file::regular_file_identity(&temporary, Path::new("manifest.json"))
        .map_err(|error| error.to_string())?;
    let published = PublishedManifest {
        file: temporary,
        identity,
    };
    published.revalidate(directory)?;
    model_lock.revalidate_for(model_dir)?;
    if remove_pending {
        unlinkat_if_present(directory, PENDING)?;
        directory.sync_all().map_err(|error| error.to_string())?;
    }
    model_lock.revalidate_for(model_dir)?;
    Ok(published)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn unlinkat_if_present(directory: &fs::File, name: &[u8]) -> Result<(), String> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr().cast(), 0) };
    if result == -1 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error.to_string());
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn publish_manifest_at(
    _model_lock: &ModelLock,
    _model_dir: &Path,
    _manifest: &Manifest,
    _remove_pending: bool,
) -> Result<PublishedManifest, String> {
    Err("captured publication requires macOS or Linux".into())
}
