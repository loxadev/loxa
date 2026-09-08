mod hash;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use hash::{content_hash_count, reset_content_hash_count};
pub(crate) use hash::{
    hash_local_gguf_captured, verify_regular, verify_regular_captured,
    verify_regular_captured_cancellable, verify_regular_entry_controlled, CapturedVerification,
    VerificationOutcome,
};

use crate::safe_file::{
    ensure_regular_descriptors_match, regular_file_identity, RegularFileIdentity,
};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegularEntryError {
    Interrupted,
    Unsafe,
}

pub(crate) struct CapturedRegularEntry {
    pub(crate) length: u64,
    pub(crate) verified: Option<VerifiedRegularFile>,
    // Keep the re-opened descriptor alive until the caller finishes its entry.
    _resolved: File,
}

/// Grants a proof only after hashing and rechecking the opened entry.
pub(crate) fn inspect_regular_entry(
    directory: &File,
    path: &Path,
    expected: Option<(u64, &str)>,
    should_pause: &impl Fn() -> bool,
    open_entry: impl Fn(&File, &Path) -> std::io::Result<Option<File>>,
    mut after_open: impl FnMut(),
) -> Result<Option<CapturedRegularEntry>, RegularEntryError> {
    let Some(mut file) = open_entry(directory, path).map_err(|_| RegularEntryError::Unsafe)? else {
        return Ok(None);
    };
    let identity = regular_file_identity(&file, path).map_err(|_| RegularEntryError::Unsafe)?;
    after_open();
    let length = file
        .metadata()
        .map_err(|_| RegularEntryError::Unsafe)?
        .len();
    let actual = if expected.is_some_and(|(size, _)| length == size) {
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            if should_pause() {
                return Err(RegularEntryError::Interrupted);
            }
            let read = file
                .read(&mut buffer)
                .map_err(|_| RegularEntryError::Unsafe)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        Some(hex(hash.finalize().as_ref()))
    } else {
        None
    };
    let resolved = open_entry(directory, path)
        .map_err(|_| RegularEntryError::Unsafe)?
        .ok_or(RegularEntryError::Unsafe)?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, path)
        .map_err(|_| RegularEntryError::Unsafe)?;
    let verified = actual
        .filter(|actual| expected.is_some_and(|(_, sha256)| actual == &sha256.to_ascii_lowercase()))
        .map(|actual| {
            VerifiedRegularFile::from_captured_hash(file, identity.clone(), path.to_owned(), actual)
        });
    Ok(Some(CapturedRegularEntry {
        length,
        verified,
        _resolved: resolved,
    }))
}

/// A hash proof bound to the captured descriptor, identity, path, and digest.
#[derive(Debug)]
pub(crate) struct VerifiedRegularFile {
    file: File,
    identity: RegularFileIdentity,
    path: PathBuf,
    sha256: String,
}

impl VerifiedRegularFile {
    fn from_captured_hash(
        file: File,
        identity: RegularFileIdentity,
        path: PathBuf,
        sha256: String,
    ) -> Self {
        Self {
            file,
            identity,
            path,
            sha256,
        }
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Checks the proof against an independently resolved entry and expected artifact.
    pub(crate) fn resolve_proven_entry(
        &self,
        expected_path: &Path,
        expected_size: u64,
        expected_sha256: &str,
        resolve: impl FnOnce() -> std::io::Result<File>,
    ) -> Result<File, ()> {
        if self.path != expected_path || self.sha256 != expected_sha256.to_ascii_lowercase() {
            return Err(());
        }
        let resolved = resolve().map_err(|_| ())?;
        ensure_regular_descriptors_match(&self.file, &self.identity, &resolved, expected_path)
            .map_err(|_| ())?;
        if resolved.metadata().map_err(|_| ())?.len() != expected_size {
            return Err(());
        }
        Ok(resolved)
    }

    /// Retains the captured identity through the caller's ordered rename fences.
    pub(crate) fn rebind_after_rename_with_fences(
        mut self,
        destination: PathBuf,
        resolve: impl FnOnce(&Path) -> Result<File, ()>,
        finish: impl FnOnce() -> Result<(), ()>,
    ) -> Result<Self, ()> {
        // This identity must precede the caller's sync/open and remain the
        // comparison authority through all of its remaining transaction fences.
        let current = regular_file_identity(&self.file, &destination).map_err(|_| ())?;
        if !self.identity.same_file_after_rename(&current) {
            return Err(());
        }
        let resolved = resolve(&destination)?;
        ensure_regular_descriptors_match(&self.file, &current, &resolved, &destination)
            .map_err(|_| ())?;
        finish()?;
        self.identity = current;
        self.path = destination;
        Ok(self)
    }

    pub(crate) fn revalidate_for(&self, expected_path: &Path) -> Result<fs::Metadata, String> {
        if self.path != expected_path {
            return Err("verified model artifact path mismatch".into());
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let resolved = options
            .open(expected_path)
            .map_err(|error| format!("{}: {error}", expected_path.display()))?;
        ensure_regular_descriptors_match(&self.file, &self.identity, &resolved, expected_path)
            .map_err(|_| {
                format!(
                    "model artifact changed after hashing {}",
                    expected_path.display()
                )
            })?;
        self.file
            .metadata()
            .map_err(|error| format!("{}: {error}", expected_path.display()))
    }

    pub(crate) fn proves(
        &self,
        expected_path: &Path,
        expected_size: u64,
        expected_sha256: &str,
    ) -> Result<(), String> {
        if self.sha256 != expected_sha256.to_ascii_lowercase() {
            return Err("verified model artifact checksum proof mismatch".into());
        }
        let metadata = self.revalidate_for(expected_path)?;
        if metadata.len() != expected_size {
            return Err(format!(
                "invalid model artifact {}",
                expected_path.display()
            ));
        }
        Ok(())
    }

    /// Checks a caller-resolved descriptor against the retained proof.
    pub(crate) fn proves_resolved(
        &self,
        expected_path: &Path,
        expected_size: u64,
        expected_sha256: &str,
        resolved: File,
    ) -> Result<(), String> {
        if self.path != expected_path
            || self.sha256 != expected_sha256.to_ascii_lowercase()
            || self.identity.size() != expected_size
        {
            return Err("verified model artifact proof mismatch".into());
        }
        ensure_regular_descriptors_match(&self.file, &self.identity, &resolved, expected_path)
            .map_err(|_| {
                format!(
                    "model artifact changed after hashing {}",
                    expected_path.display()
                )
            })
    }

    pub(crate) fn rebind_after_rename(self, destination: &Path) -> Result<Self, String> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let resolved = options
            .open(destination)
            .map_err(|error| format!("{}: {error}", destination.display()))?;
        self.rebind_after_rename_resolved(destination, resolved)
    }

    /// Rebinds only when the resolved destination is the same captured file.
    pub(crate) fn rebind_after_rename_resolved(
        mut self,
        destination: &Path,
        resolved: File,
    ) -> Result<Self, String> {
        let current = regular_file_identity(&self.file, destination)
            .map_err(|error| format!("{}: {error}", destination.display()))?;
        if !self.identity.same_file_after_rename(&current) {
            return Err(format!(
                "model artifact changed while moving {}",
                destination.display()
            ));
        }
        ensure_regular_descriptors_match(&self.file, &current, &resolved, destination).map_err(
            |_| {
                format!(
                    "model artifact changed after moving {}",
                    destination.display()
                )
            },
        )?;
        self.identity = current;
        self.path = destination.to_owned();
        Ok(self)
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}
