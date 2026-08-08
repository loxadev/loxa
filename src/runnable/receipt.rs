use crate::catalog::{Manifest, ModelLock};
use crate::download::VerifiedRegularFile;
use std::path::Path;

const RECEIPT_NAME: &str = "verification-receipt.json";

#[cfg(unix)]
mod platform {
    use super::{Manifest, ModelLock, Path, VerifiedRegularFile, RECEIPT_NAME};
    use serde::{Deserialize, Serialize};
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;

    const MAX_RECEIPT_BYTES: u64 = 4096;
    const RECEIPT_NAME_C: &[u8] = b"verification-receipt.json\0";
    const TEMP_NAME_C: &[u8] = b".verification-receipt.json.tmp\0";

    #[derive(Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Receipt {
        version: u8,
        manifest: ManifestIdentity,
        directory: DirectoryIdentity,
        primary: RegularFileIdentity,
        #[serde(skip_serializing_if = "Option::is_none")]
        draft: Option<RegularFileIdentity>,
    }

    #[derive(Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct ManifestIdentity {
        primary_sha256: String,
        primary_size: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        draft_sha256: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        draft_size: Option<u64>,
    }

    #[derive(Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct DirectoryIdentity {
        device: u64,
        inode: u64,
    }

    #[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct RegularFileIdentity {
        size: u64,
        device: u64,
        inode: u64,
        links: u64,
        modified_seconds: i64,
        modified_nanoseconds: i64,
        changed_seconds: i64,
        changed_nanoseconds: i64,
    }

    struct CapturedArtifacts {
        receipt: Receipt,
        primary: CapturedFile,
        draft: Option<CapturedFile>,
    }

    struct CapturedFile {
        file: File,
        opened: crate::safe_file::RegularFileIdentity,
        path: PathBuf,
        identity: RegularFileIdentity,
    }

    struct CapturedReceipt {
        file: File,
        opened: crate::safe_file::RegularFileIdentity,
        path: PathBuf,
        receipt: Receipt,
    }

    struct CapturedVerifiedArtifacts<'a> {
        receipt: Receipt,
        primary: (&'a Path, &'a VerifiedRegularFile),
        draft: Option<(&'a Path, &'a VerifiedRegularFile)>,
    }

    pub(crate) fn refresh(
        model_lock: &ModelLock,
        manifest: &Manifest,
        primary_path: &Path,
        primary: &VerifiedRegularFile,
        draft: Option<(&Path, &VerifiedRegularFile)>,
    ) -> Result<(), String> {
        let captured =
            CapturedVerifiedArtifacts::capture(model_lock, manifest, primary_path, primary, draft)?;
        let bytes = serde_json::to_vec(&captured.receipt).map_err(|error| error.to_string())?;
        write_atomically(model_lock, &bytes, || captured.revalidate(model_lock))
    }

    pub(crate) fn matches(
        model_lock: &ModelLock,
        model_dir: &Path,
        manifest: &Manifest,
        primary: &Path,
        draft: Option<&Path>,
    ) -> Result<bool, String> {
        model_lock.revalidate()?;
        let Some(receipt) = read(model_dir) else {
            return Ok(false);
        };
        let current = match CapturedArtifacts::capture(model_lock, manifest, primary, draft) {
            Ok(current) => current,
            Err(_) => return Ok(false),
        };
        if receipt.revalidate().is_err() || current.revalidate(model_lock).is_err() {
            return Ok(false);
        }
        Ok(receipt.receipt == current.receipt)
    }

    pub(crate) fn discard(model_lock: &ModelLock, _model_dir: &Path) -> Result<(), String> {
        unlink_if_present(model_lock, RECEIPT_NAME_C)
    }

    impl CapturedArtifacts {
        fn capture(
            model_lock: &ModelLock,
            manifest: &Manifest,
            primary: &Path,
            draft: Option<&Path>,
        ) -> Result<Self, String> {
            model_lock.revalidate()?;
            let primary = CapturedFile::open(primary)?;
            let draft = draft.map(CapturedFile::open).transpose()?;
            let receipt = Receipt {
                version: 1,
                manifest: ManifestIdentity::from_manifest(manifest),
                directory: DirectoryIdentity::from_lock(model_lock)?,
                primary: primary.identity.clone(),
                draft: draft.as_ref().map(|file| file.identity.clone()),
            };
            let captured = Self {
                receipt,
                primary,
                draft,
            };
            captured.revalidate(model_lock)?;
            Ok(captured)
        }

        fn revalidate(&self, model_lock: &ModelLock) -> Result<(), String> {
            model_lock.revalidate()?;
            self.primary.revalidate()?;
            if let Some(draft) = &self.draft {
                draft.revalidate()?;
            }
            model_lock.revalidate()
        }
    }

    impl<'a> CapturedVerifiedArtifacts<'a> {
        fn capture(
            model_lock: &ModelLock,
            manifest: &Manifest,
            primary_path: &'a Path,
            primary: &'a VerifiedRegularFile,
            draft: Option<(&'a Path, &'a VerifiedRegularFile)>,
        ) -> Result<Self, String> {
            model_lock.revalidate()?;
            let primary_metadata = primary.revalidate_for(primary_path)?;
            let primary_identity =
                RegularFileIdentity::from_metadata(&primary_metadata, primary_path)?;
            let draft_identity = draft
                .map(|(path, file)| {
                    file.revalidate_for(path)
                        .and_then(|metadata| RegularFileIdentity::from_metadata(&metadata, path))
                })
                .transpose()?;
            let captured = Self {
                receipt: Receipt {
                    version: 1,
                    manifest: ManifestIdentity::from_manifest(manifest),
                    directory: DirectoryIdentity::from_lock(model_lock)?,
                    primary: primary_identity,
                    draft: draft_identity,
                },
                primary: (primary_path, primary),
                draft,
            };
            captured.revalidate(model_lock)?;
            Ok(captured)
        }

        fn revalidate(&self, model_lock: &ModelLock) -> Result<(), String> {
            model_lock.revalidate()?;
            self.primary.1.revalidate_for(self.primary.0)?;
            if let Some((path, file)) = self.draft {
                file.revalidate_for(path)?;
            }
            model_lock.revalidate()
        }
    }

    impl CapturedFile {
        fn open(path: &Path) -> Result<Self, String> {
            let (file, opened) = crate::safe_file::open_regular_file(path)
                .map_err(|error| format!("{}: {error}", path.display()))?;
            let identity = RegularFileIdentity::from_metadata(
                &file
                    .metadata()
                    .map_err(|error| format!("{}: {error}", path.display()))?,
                path,
            )?;
            let captured = Self {
                file,
                opened,
                path: path.to_owned(),
                identity,
            };
            captured.revalidate()?;
            Ok(captured)
        }

        fn revalidate(&self) -> Result<(), String> {
            crate::safe_file::ensure_descriptor_matches_path(&self.file, &self.opened, &self.path)
                .map_err(|error| format!("{}: {error}", self.path.display()))
        }
    }

    impl CapturedReceipt {
        fn revalidate(&self) -> Result<(), String> {
            crate::safe_file::ensure_descriptor_matches_path(&self.file, &self.opened, &self.path)
                .map_err(|error| format!("{}: {error}", self.path.display()))
        }
    }

    fn read(model_dir: &Path) -> Option<CapturedReceipt> {
        let path = model_dir.join(RECEIPT_NAME);
        let (mut file, opened) = crate::safe_file::open_regular_file(&path).ok()?;
        if file.metadata().ok()?.len() > MAX_RECEIPT_BYTES {
            return None;
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_RECEIPT_BYTES + 1)
            .read_to_end(&mut bytes)
            .ok()?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return None;
        }
        let receipt = serde_json::from_slice(&bytes).ok()?;
        let captured = CapturedReceipt {
            file,
            opened,
            path,
            receipt,
        };
        captured.revalidate().ok()?;
        Some(captured)
    }

    fn write_atomically(
        model_lock: &ModelLock,
        bytes: &[u8],
        mut revalidate: impl FnMut() -> Result<(), String>,
    ) -> Result<(), String> {
        revalidate()?;
        unlink_if_present(model_lock, TEMP_NAME_C)?;
        let mut temporary = create_temp(model_lock)?;
        if let Err(error) = temporary
            .write_all(bytes)
            .and_then(|_| temporary.sync_all())
        {
            drop(temporary);
            let _ = unlink_if_present(model_lock, TEMP_NAME_C);
            return Err(error.to_string());
        }
        drop(temporary);
        revalidate()?;
        let directory = model_lock.model_directory();
        let result = unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                TEMP_NAME_C.as_ptr().cast(),
                directory.as_raw_fd(),
                RECEIPT_NAME_C.as_ptr().cast(),
            )
        };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            let _ = unlink_if_present(model_lock, TEMP_NAME_C);
            return Err(error.to_string());
        }
        directory.sync_all().map_err(|error| error.to_string())?;
        model_lock.revalidate()
    }

    fn create_temp(model_lock: &ModelLock) -> Result<File, String> {
        model_lock.revalidate()?;
        let descriptor = unsafe {
            libc::openat(
                model_lock.model_directory().as_raw_fd(),
                TEMP_NAME_C.as_ptr().cast(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600 as libc::c_uint,
            )
        };
        if descriptor == -1 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }

    fn unlink_if_present(model_lock: &ModelLock, name: &[u8]) -> Result<(), String> {
        model_lock.revalidate()?;
        let result = unsafe {
            libc::unlinkat(
                model_lock.model_directory().as_raw_fd(),
                name.as_ptr().cast(),
                0,
            )
        };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.to_string());
            }
        }
        model_lock.revalidate()
    }

    impl ManifestIdentity {
        fn from_manifest(manifest: &Manifest) -> Self {
            let primary = manifest.primary_artifact();
            let draft = manifest.draft_artifact();
            Self {
                primary_sha256: primary.sha256.into(),
                primary_size: primary.size,
                draft_sha256: draft.map(|artifact| artifact.sha256.into()),
                draft_size: draft.map(|artifact| artifact.size),
            }
        }
    }

    impl DirectoryIdentity {
        fn from_lock(model_lock: &ModelLock) -> Result<Self, String> {
            let metadata = model_lock
                .model_directory()
                .metadata()
                .map_err(|error| error.to_string())?;
            if !metadata.file_type().is_dir() {
                return Err("unsafe model directory for verification receipt".into());
            }
            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
    }

    impl RegularFileIdentity {
        fn from_metadata(metadata: &fs::Metadata, path: &Path) -> Result<Self, String> {
            if !metadata.file_type().is_file() || metadata.nlink() != 1 {
                return Err(format!("unsafe model artifact {}", path.display()));
            }
            Ok(Self {
                size: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                links: metadata.nlink(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
            })
        }
    }
}

#[cfg(unix)]
pub(super) use platform::{discard, matches, refresh};

#[cfg(not(unix))]
pub(super) fn refresh(
    _model_lock: &ModelLock,
    _manifest: &Manifest,
    _primary_path: &Path,
    _primary: &VerifiedRegularFile,
    _draft: Option<(&Path, &VerifiedRegularFile)>,
) -> Result<(), String> {
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn matches(
    _model_lock: &ModelLock,
    _model_dir: &Path,
    _manifest: &Manifest,
    _primary: &Path,
    _draft: Option<&Path>,
) -> Result<bool, String> {
    Ok(false)
}

#[cfg(not(unix))]
pub(super) fn discard(_model_lock: &ModelLock, _model_dir: &Path) -> Result<(), String> {
    Ok(())
}
