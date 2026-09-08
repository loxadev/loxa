use super::{hex, VerifiedRegularFile};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, DirectoryIdentity, RegularFileIdentity,
};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;

#[cfg(test)]
thread_local! {
    static CONTENT_HASH_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) fn verify_regular_captured(
    path: &Path,
    size: u64,
    sha256: &str,
) -> Result<VerifiedRegularFile, String> {
    match verify_regular_captured_cancellable(path, size, sha256, &|| false)? {
        CapturedVerification::Verified(verified) => Ok(verified),
        CapturedVerification::Cancelled => Err("artifact verification interrupted".into()),
    }
}

pub(crate) enum CapturedVerification {
    Verified(VerifiedRegularFile),
    Cancelled,
}

pub(crate) fn verify_regular_captured_cancellable(
    path: &Path,
    size: u64,
    sha256: &str,
    cancelled: &impl Fn() -> bool,
) -> Result<CapturedVerification, String> {
    match verify_regular_with_observer(path, size, sha256, None, cancelled, |_| Ok(()))? {
        VerificationOutcome::Verified(verified) => Ok(CapturedVerification::Verified(verified)),
        VerificationOutcome::Interrupted => Ok(CapturedVerification::Cancelled),
        VerificationOutcome::ChecksumMismatch => Err("model artifact checksum mismatch".into()),
    }
}

pub(crate) fn verify_regular(path: &Path, size: u64, sha256: &str) -> Result<(), String> {
    verify_regular_captured(path, size, sha256).map(|_| ())
}

pub(crate) fn hash_local_gguf_captured(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    directory_path: &Path,
    path: &Path,
    size: u64,
) -> Result<VerifiedRegularFile, String> {
    if path.parent() != Some(directory_path) {
        return Err(format!("unsafe local GGUF candidate {}", path.display()));
    }
    let name = path
        .file_name()
        .ok_or_else(|| format!("unsafe local GGUF candidate {}", path.display()))?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, directory_path)
        .map_err(|error| format!("{}: {error}", directory_path.display()))?;
    let mut file = open_regular_entry(directory, name, path)?;
    let opened = regular_file_identity(&file, path).map_err(|error| error.to_string())?;
    if file.metadata().map_err(|error| error.to_string())?.len() != size || size <= 8 {
        return Err(format!("unsafe local GGUF candidate {}", path.display()));
    }
    let actual = hash_descriptor(&mut file, path, true, &|| false)?
        .ok_or_else(|| "artifact verification interrupted".to_string())?;
    let resolved = open_regular_entry(directory, name, path)?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, path).map_err(|_| {
        format!(
            "local GGUF candidate changed while hashing {}",
            path.display()
        )
    })?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, directory_path)
        .map_err(|error| format!("{}: {error}", directory_path.display()))?;
    Ok(VerifiedRegularFile {
        file,
        identity: opened,
        path: path.to_owned(),
        sha256: actual,
    })
}

#[derive(Debug)]
pub(crate) enum VerificationOutcome {
    Verified(VerifiedRegularFile),
    Interrupted,
    ChecksumMismatch,
}

pub(crate) fn verify_regular_entry_controlled(
    directory: &File,
    path: &Path,
    open_entry: fn(&File, &Path) -> std::io::Result<File>,
    size: u64,
    sha256: &str,
    expected_staging: Option<(&File, &RegularFileIdentity)>,
    should_pause: &impl Fn() -> bool,
) -> Result<VerificationOutcome, String> {
    let mut file =
        open_entry(directory, path).map_err(|error| format!("{}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.len() != size {
        return Err(format!("invalid model artifact {}", path.display()));
    }
    let opened = regular_file_identity(&file, path).map_err(|error| error.to_string())?;
    if let Some((staging, expected)) = expected_staging {
        ensure_regular_descriptors_match(staging, expected, &file, path)
            .map_err(|_| format!("model artifact changed before hashing {}", path.display()))?;
    }
    let Some(actual) = hash_descriptor(&mut file, path, false, should_pause)? else {
        return Ok(VerificationOutcome::Interrupted);
    };
    let resolved =
        open_entry(directory, path).map_err(|error| format!("{}: {error}", path.display()))?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, path)
        .map_err(|_| format!("model artifact changed while hashing {}", path.display()))?;
    if actual != sha256.to_ascii_lowercase() {
        return Ok(VerificationOutcome::ChecksumMismatch);
    }
    Ok(VerificationOutcome::Verified(VerifiedRegularFile {
        file,
        identity: opened,
        path: path.to_owned(),
        sha256: actual,
    }))
}

pub(super) fn verify_regular_with_observer<F>(
    path: &Path,
    size: u64,
    sha256: &str,
    expected_staging: Option<(&File, &RegularFileIdentity)>,
    should_pause: &impl Fn() -> bool,
    mut observer: F,
) -> Result<VerificationOutcome, String>
where
    F: FnMut(&Path) -> Result<(), String>,
{
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.len() != size {
        return Err(format!("invalid model artifact {}", path.display()));
    }
    let opened = regular_file_identity(&file, path).map_err(|error| error.to_string())?;
    if let Some((staging, expected)) = expected_staging {
        ensure_regular_descriptors_match(staging, expected, &file, path)
            .map_err(|_| format!("model artifact changed before hashing {}", path.display()))?;
    }
    observer(path)?;
    let Some(actual) = hash_descriptor(&mut file, path, false, should_pause)? else {
        return Ok(VerificationOutcome::Interrupted);
    };
    let mut resolved_options = OpenOptions::new();
    resolved_options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        resolved_options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let resolved = resolved_options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    ensure_regular_descriptors_match(&file, &opened, &resolved, path)
        .map_err(|_| format!("model artifact changed while hashing {}", path.display()))?;
    if actual != sha256.to_ascii_lowercase() {
        return Ok(VerificationOutcome::ChecksumMismatch);
    }
    Ok(VerificationOutcome::Verified(VerifiedRegularFile {
        file,
        identity: opened,
        path: path.to_owned(),
        sha256: actual,
    }))
}

fn hash_descriptor(
    file: &mut File,
    path: &Path,
    require_gguf: bool,
    should_pause: &impl Fn() -> bool,
) -> Result<Option<String>, String> {
    #[cfg(test)]
    CONTENT_HASH_COUNT.with(|count| count.set(count.get() + 1));
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut first = true;
    loop {
        if should_pause() {
            return Ok(None);
        }
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        if first && require_gguf {
            if read < 8 {
                return Err(format!("unsupported GGUF header {}", path.display()));
            }
            let version = u32::from_le_bytes(
                buffer[4..8]
                    .try_into()
                    .expect("GGUF version prefix has four bytes"),
            );
            if &buffer[..4] != b"GGUF" || !matches!(version, 2 | 3) {
                return Err(format!("unsupported GGUF header {}", path.display()));
            }
        }
        first = false;
        hash.update(&buffer[..read]);
    }
    Ok(Some(hex(hash.finalize().as_ref())))
}

#[cfg(test)]
pub(crate) fn reset_content_hash_count() {
    CONTENT_HASH_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn content_hash_count() -> usize {
    CONTENT_HASH_COUNT.with(std::cell::Cell::get)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn open_regular_entry(
    directory: &File,
    name: &std::ffi::OsStr,
    path: &Path,
) -> Result<File, String> {
    use rustix::fs::{openat, Mode, OFlags};

    let descriptor = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(File::from(descriptor))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn open_regular_entry(
    _directory: &File,
    _name: &std::ffi::OsStr,
    path: &Path,
) -> Result<File, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))
}
