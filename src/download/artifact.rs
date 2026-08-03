use super::http::{artifact_url, Transfer, TransferError, Transport};
use super::{DownloadOutcome, ProgressUpdate};
use crate::huggingface::ResolvedFile;
use reqwest::StatusCode;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub(super) async fn download_once(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
    progress: &mut impl FnMut(ProgressUpdate),
) -> Result<DownloadOutcome, TransferError> {
    fs::create_dir_all(model_dir).map_err(|error| error.to_string())?;
    if !fs::symlink_metadata(model_dir)
        .map_err(|error| error.to_string())?
        .file_type()
        .is_dir()
    {
        return Err(format!("unsafe model directory {}", model_dir.display()).into());
    }
    let final_path = model_dir.join("model.gguf");
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let invalid_path = model_dir.join("model.gguf.invalid");
    if final_path.exists() {
        reject_non_regular_if_present(&final_path)?;
        progress(ProgressUpdate::Verifying);
        if verify_regular(&final_path, spec.size, &spec.sha256).is_ok() {
            finish_repair(model_dir, &invalid_path, &restart_path)?;
            progress(ProgressUpdate::Seed(spec.size));
            return Ok(DownloadOutcome::AlreadyInstalled(final_path));
        }
        if invalid_path.exists() {
            return Err("a prior corrupt artifact repair is still pending".into());
        }
        fs::rename(&final_path, &invalid_path).map_err(|error| error.to_string())?;
    }
    reject_unsafe_transfer_if_present(&part_path)?;
    reject_unsafe_transfer_if_present(&restart_path)?;
    reject_non_regular_if_present(&invalid_path)?;
    let mut offset = fs::metadata(&part_path).map(|meta| meta.len()).unwrap_or(0);
    if offset > spec.size {
        fs::remove_file(&part_path).map_err(|error| error.to_string())?;
        offset = 0;
    }
    progress(ProgressUpdate::Seed(offset));
    if offset == spec.size && offset > 0 {
        sync_transfer_file(&part_path)?;
        progress(ProgressUpdate::Verifying);
        if let Err(error) = verify_regular(&part_path, spec.size, &spec.sha256) {
            fs::remove_file(&part_path).map_err(|remove| remove.to_string())?;
            return Err(error.into());
        }
        fs::rename(&part_path, &final_path).map_err(|error| error.to_string())?;
        finish_repair(model_dir, &invalid_path, &restart_path)?;
        return Ok(DownloadOutcome::Pulled(final_path));
    }
    let mut transfer = transport
        .get(&artifact_url(spec)?, (offset > 0).then_some(offset))
        .await?;
    let ignored_range = offset > 0 && transfer.status == StatusCode::OK;
    let (target, append) = if ignored_range {
        progress(ProgressUpdate::Seed(0));
        (&restart_path, false)
    } else {
        if transfer.status == StatusCode::PARTIAL_CONTENT {
            validate_content_range(transfer.content_range.as_deref(), offset, spec.size)?;
        } else if transfer.status != StatusCode::OK || offset > 0 {
            return Err(format!("unexpected artifact HTTP {}", transfer.status).into());
        }
        (&part_path, offset > 0)
    };
    let expected_written = if append {
        spec.size - offset
    } else {
        spec.size
    };
    let read_limit = expected_written
        .checked_add(1)
        .ok_or_else(|| "artifact is too large".to_string())?;
    let mut options = OpenOptions::new();
    options
        .create(true)
        .write(true)
        .append(append)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut output = options
        .open(target)
        .map_err(|error| format!("{}: {error}", target.display()))?;
    reject_unsafe_open_transfer(&output, target)?;
    if !append {
        output
            .set_len(0)
            .map_err(|error| format!("{}: {error}", target.display()))?;
    }
    let progress_offset = if append { offset } else { 0 };
    let copied = copy_bounded(&mut transfer, &mut output, target, read_limit, |copied| {
        progress(ProgressUpdate::Position(progress_offset + copied))
    })
    .await?;
    if copied > expected_written {
        drop(output);
        fs::remove_file(target).map_err(|error| format!("{}: {error}", target.display()))?;
        return Err(format!(
            "artifact size mismatch: expected {expected_written} downloaded bytes, got more"
        )
        .into());
    }
    output
        .sync_all()
        .map_err(|error| format!("{}: {error}", target.display()))?;
    if copied != expected_written {
        return Err(TransferError::retryable(format!(
            "artifact size mismatch: expected {expected_written} downloaded bytes, got {copied}"
        )));
    }
    progress(ProgressUpdate::Verifying);
    if let Err(error) = verify_regular(target, spec.size, &spec.sha256) {
        fs::remove_file(target).map_err(|remove| remove.to_string())?;
        return Err(error.into());
    }
    if ignored_range {
        fs::remove_file(&part_path).map_err(|error| error.to_string())?;
        fs::rename(&restart_path, &part_path).map_err(|error| error.to_string())?;
    }
    fs::rename(&part_path, &final_path).map_err(|error| error.to_string())?;
    finish_repair(model_dir, &invalid_path, &restart_path)?;
    Ok(DownloadOutcome::Pulled(final_path))
}

async fn copy_bounded(
    transfer: &mut Transfer,
    output: &mut File,
    target: &Path,
    limit: u64,
    mut progress: impl FnMut(u64),
) -> Result<u64, TransferError> {
    let mut copied = 0;
    while copied < limit {
        let remaining = (limit - copied).min(64 * 1024) as usize;
        let chunk = match transfer.chunk(remaining).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(error) => {
                output
                    .sync_all()
                    .map_err(|sync| format!("{}: {sync}", target.display()))?;
                return Err(error);
            }
        };
        if chunk.is_empty() {
            continue;
        }
        let read = chunk.len();
        output
            .write_all(&chunk[..read])
            .map_err(|error| format!("{}: {error}", target.display()))?;
        copied += read as u64;
        progress(copied);
    }
    Ok(copied)
}

fn sync_transfer_file(path: &Path) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    reject_unsafe_open_transfer(&file, path)?;
    file.sync_all()
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn finish_repair(model_dir: &Path, invalid_path: &Path, restart_path: &Path) -> Result<(), String> {
    for path in [invalid_path, restart_path] {
        if path.exists() {
            fs::remove_file(path).map_err(|error| error.to_string())?;
        }
    }
    File::open(model_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn verify_regular(path: &Path, size: u64, sha256: &str) -> Result<(), String> {
    verify_regular_with_observer(path, size, sha256, |_| Ok(()))
}

fn verify_regular_with_observer<F>(
    path: &Path,
    size: u64,
    sha256: &str,
    mut observer: F,
) -> Result<(), String>
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
    let opened = RegularIdentity::from_metadata(
        &file
            .metadata()
            .map_err(|error| format!("{}: {error}", path.display()))?,
        path,
        size,
    )?;
    observer(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let after = RegularIdentity::from_metadata(
        &file
            .metadata()
            .map_err(|error| format!("{}: {error}", path.display()))?,
        path,
        size,
    )?;
    if after != opened {
        return Err(format!(
            "model artifact changed while hashing {}",
            path.display()
        ));
    }
    let resolved = RegularIdentity::from_metadata(
        &fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?,
        path,
        size,
    )?;
    if resolved != opened {
        return Err(format!(
            "model artifact changed while hashing {}",
            path.display()
        ));
    }
    let actual = hex(hash.finalize().as_ref());
    if actual != sha256.to_ascii_lowercase() {
        return Err("model artifact checksum mismatch".into());
    }
    Ok(())
}

#[derive(Eq, PartialEq)]
struct RegularIdentity {
    size: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
}

impl RegularIdentity {
    fn from_metadata(
        metadata: &std::fs::Metadata,
        path: &Path,
        expected_size: u64,
    ) -> Result<Self, String> {
        if !metadata.file_type().is_file() || metadata.len() != expected_size {
            return Err(format!("invalid model artifact {}", path.display()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                return Err(format!("unsafe model artifact {}", path.display()));
            }
            Ok(Self {
                size: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                size: metadata.len(),
            })
        }
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn reject_non_regular_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(format!("unsafe artifact path {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn reject_unsafe_transfer_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(format!("unsafe artifact path {}", path.display()));
                }
            }
            Ok(())
        }
        Ok(_) => Err(format!("unsafe artifact path {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn reject_unsafe_open_transfer(file: &File, path: &Path) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe artifact path {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe artifact path {}", path.display()));
        }
    }
    Ok(())
}

fn validate_content_range(value: Option<&str>, offset: u64, total: u64) -> Result<(), String> {
    let value = value.ok_or_else(|| "206 response omitted Content-Range".to_string())?;
    let rest = value
        .strip_prefix("bytes ")
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (range, observed_total) = rest
        .split_once('/')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let start = start.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let end = end.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let observed_total = observed_total
        .parse::<u64>()
        .map_err(|_| "invalid Content-Range")?;
    if start == offset && observed_total == total && end.checked_add(1) == Some(total) {
        Ok(())
    } else {
        Err("Content-Range does not match requested artifact".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn verify_regular_rejects_a_hard_link_even_when_its_bytes_match() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.gguf");
        let linked = root.path().join("linked.gguf");
        let bytes = b"verified artifact";
        std::fs::write(&source, bytes).unwrap();
        std::fs::hard_link(&source, &linked).unwrap();
        let checksum = hex(Sha256::digest(bytes).as_ref());

        assert!(verify_regular(&linked, bytes.len() as u64, &checksum).is_err());
        assert_eq!(std::fs::read(source).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn verify_regular_rejects_a_symlink_even_when_its_target_matches() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.gguf");
        let linked = root.path().join("linked.gguf");
        let bytes = b"verified artifact";
        std::fs::write(&source, bytes).unwrap();
        symlink(&source, &linked).unwrap();
        let checksum = hex(Sha256::digest(bytes).as_ref());

        assert!(verify_regular(&linked, bytes.len() as u64, &checksum).is_err());
        assert_eq!(std::fs::read(source).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn verify_regular_rejects_an_in_place_rewrite_with_restored_mtime() {
        use std::os::unix::fs::MetadataExt;

        let root = tempfile::tempdir().unwrap();
        let artifact = root.path().join("model.gguf");
        let bytes = b"verified artifact";
        std::fs::write(&artifact, bytes).unwrap();
        let original = std::fs::metadata(&artifact).unwrap();
        let original_modified = original.modified().unwrap();
        let checksum = hex(Sha256::digest(bytes).as_ref());

        let error =
            verify_regular_with_observer(&artifact, bytes.len() as u64, &checksum, |path| {
                std::fs::write(path, bytes).unwrap();
                OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                    .unwrap();
                let restored = std::fs::metadata(path).unwrap();
                assert_eq!(restored.mtime(), original.mtime());
                assert_eq!(restored.mtime_nsec(), original.mtime_nsec());
                Ok(())
            })
            .unwrap_err();

        assert!(error.contains("changed while hashing"), "{error}");
    }
}
