use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    _private: (),
}

impl DirectoryIdentity {
    fn from_metadata(metadata: &fs::Metadata, path: &Path) -> io::Result<Self> {
        if !metadata.file_type().is_dir() {
            return Err(unsafe_file_error(path, "expected a directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self { _private: () })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegularFileIdentity {
    size: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    links: u64,
    #[cfg(unix)]
    owner: u32,
    #[cfg(unix)]
    mode: u32,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
}

impl RegularFileIdentity {
    fn from_metadata(metadata: &fs::Metadata, path: &Path) -> io::Result<Self> {
        if !metadata.file_type().is_file() {
            return Err(unsafe_file_error(path, "expected a regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            if metadata.nlink() != 1 {
                return Err(unsafe_file_error(path, "expected a single-link file"));
            }
            Ok(Self {
                size: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                links: metadata.nlink(),
                owner: metadata.uid(),
                mode: metadata.mode() & 0o777,
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

    pub(crate) fn same_file_after_rename(&self, current: &Self) -> bool {
        #[cfg(unix)]
        {
            self.size == current.size
                && self.device == current.device
                && self.inode == current.inode
                && self.links == current.links
                && self.owner == current.owner
                && self.mode == current.mode
                && self.modified_seconds == current.modified_seconds
                && self.modified_nanoseconds == current.modified_nanoseconds
        }
        #[cfg(not(unix))]
        {
            self == current
        }
    }

    pub(crate) fn same_stable_file(&self, current: &Self) -> bool {
        #[cfg(unix)]
        {
            self.size == current.size
                && self.device == current.device
                && self.inode == current.inode
                && self.links == current.links
                && self.owner == current.owner
                && self.mode == current.mode
        }
        #[cfg(not(unix))]
        {
            self == current
        }
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn require_private_user_file(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        if self.owner != unsafe { libc::geteuid() } || self.mode != 0o600 || self.links != 1 {
            return Err(unsafe_file_error(
                path,
                "expected a private user-owned 0600 single-link file",
            ));
        }
        Ok(())
    }
}

pub(crate) fn read_regular_file(path: &Path) -> io::Result<Vec<u8>> {
    read_regular_file_with_after_open(path, || {})
}

pub(crate) fn read_regular_file_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    read_regular_file_bounded_with_policy(path, limit, false)
}

pub(crate) fn read_private_regular_file_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    read_regular_file_bounded_with_policy(path, limit, true)
}

fn read_regular_file_bounded_with_policy(
    path: &Path,
    limit: usize,
    require_private: bool,
) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path)?;
    let opened = regular_file_identity(&file, path)?;
    if require_private {
        opened.require_private_user_file(path)?;
    }
    let mut bytes = Vec::with_capacity(limit.min(8_192));
    let mut buffer = [0_u8; 8_192];
    while bytes.len() <= limit {
        let remaining = limit.saturating_add(1).saturating_sub(bytes.len());
        if remaining == 0 {
            break;
        }
        let requested = remaining.min(buffer.len());
        let read = file.read(&mut buffer[..requested])?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    ensure_descriptor_matches_path(&file, &opened, path)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} exceeds its byte limit", path.display()),
        ));
    }
    Ok(bytes)
}

pub(crate) fn open_directory(path: &Path) -> io::Result<(File, DirectoryIdentity)> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let identity = DirectoryIdentity::from_metadata(&file.metadata()?, path)?;
    Ok((file, identity))
}

pub(crate) fn directory_identity(file: &File, path: &Path) -> io::Result<DirectoryIdentity> {
    DirectoryIdentity::from_metadata(&file.metadata()?, path)
}

pub(crate) fn ensure_directory_descriptor_matches_path(
    file: &File,
    opened: &DirectoryIdentity,
    path: &Path,
) -> io::Result<()> {
    let after = DirectoryIdentity::from_metadata(&file.metadata()?, path)?;
    if &after != opened {
        return Err(changed_while_open(path, "directory"));
    }
    let resolved = DirectoryIdentity::from_metadata(&fs::symlink_metadata(path)?, path)?;
    if &resolved != opened {
        return Err(changed_while_open(path, "directory"));
    }
    Ok(())
}

pub(crate) fn open_regular_file(path: &Path) -> io::Result<(File, RegularFileIdentity)> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let identity = regular_file_identity(&file, path)?;
    Ok((file, identity))
}

pub(crate) fn regular_file_identity(file: &File, path: &Path) -> io::Result<RegularFileIdentity> {
    RegularFileIdentity::from_metadata(&file.metadata()?, path)
}

pub(crate) fn regular_path_identity(path: &Path) -> io::Result<RegularFileIdentity> {
    RegularFileIdentity::from_metadata(&fs::symlink_metadata(path)?, path)
}

pub(crate) fn ensure_regular_descriptors_match(
    opened_file: &File,
    opened: &RegularFileIdentity,
    resolved_file: &File,
    path: &Path,
) -> io::Result<()> {
    let after = regular_file_identity(opened_file, path)?;
    if &after != opened {
        return Err(changed_while_open(path, "file"));
    }
    let resolved = regular_file_identity(resolved_file, path)?;
    if &resolved != opened {
        return Err(changed_while_open(path, "file"));
    }
    Ok(())
}

pub(crate) fn ensure_descriptor_matches_path(
    file: &File,
    opened: &RegularFileIdentity,
    path: &Path,
) -> io::Result<()> {
    let after = RegularFileIdentity::from_metadata(&file.metadata()?, path)?;
    if &after != opened {
        return Err(changed_while_reading(path));
    }
    let resolved = RegularFileIdentity::from_metadata(&fs::symlink_metadata(path)?, path)?;
    if &resolved != opened {
        return Err(changed_while_reading(path));
    }
    Ok(())
}

fn read_regular_file_with_after_open(
    path: &Path,
    after_open: impl FnOnce(),
) -> io::Result<Vec<u8>> {
    let (mut file, opened) = open_regular_file(path)?;
    after_open();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    ensure_descriptor_matches_path(&file, &opened, path)?;
    Ok(bytes)
}

fn unsafe_file_error(path: &Path, message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{message}: {}", path.display()),
    )
}

fn changed_while_reading(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("file changed while reading {}", path.display()),
    )
}

fn changed_while_open(path: &Path, kind: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{kind} changed while open {}", path.display()),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{open_regular_file, read_regular_file_bounded, read_regular_file_with_after_open};
    use std::fs;
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn rejects_path_substitution_after_descriptor_open() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let path = root.path().join("manifest.json");
        let outside = root.path().join("outside.json");
        fs::write(&path, b"trusted").unwrap();
        fs::write(&outside, b"untrusted").unwrap();

        let error = read_regular_file_with_after_open(&path, || {
            fs::remove_file(&path).unwrap();
            symlink(&outside, &path).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "{error}"
        );
        assert_eq!(fs::read(&outside).unwrap(), b"untrusted");
    }

    #[test]
    fn rejects_regular_replacement_after_descriptor_open() {
        let root = tempdir().unwrap();
        let path = root.path().join("foreground.json");
        let replacement = root.path().join("replacement.json");
        fs::write(&path, b"trusted").unwrap();
        fs::write(&replacement, b"untrusted").unwrap();

        let error = read_regular_file_with_after_open(&path, || {
            fs::rename(&replacement, &path).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "{error}"
        );
        assert_eq!(fs::read(&path).unwrap(), b"untrusted");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_hard_linked_regular_files() {
        let root = tempdir().unwrap();
        let path = root.path().join("manifest.json");
        let alias = root.path().join("alias.json");
        fs::write(&path, b"trusted").unwrap();
        fs::hard_link(&path, &alias).unwrap();

        let error = read_regular_file_with_after_open(&path, || {}).unwrap_err();

        assert!(error.to_string().contains("single-link"), "{error}");
    }

    #[cfg(unix)]
    pub(crate) fn assert_fifo_rejected_without_writer(
        path: &std::path::Path,
        read: impl FnOnce(&std::path::Path) -> bool + Send + 'static,
    ) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: the test owns this path and keeps its NUL-terminated bytes alive.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let worker_path = path.to_owned();
        let (finished_tx, finished_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let rejected = read(&worker_path);
            let _ = finished_tx.send(());
            rejected
        });
        let finished_without_writer = finished_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        // Release a regressed blocking open before joining it. The extra reader
        // makes opening the nonblocking writer safe even before the worker runs.
        let _release = (!finished_without_writer).then(|| {
            let reader = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path)
                .unwrap();
            let writer = fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path)
                .unwrap();
            (reader, writer)
        });
        let rejected = worker.join().unwrap();

        assert!(finished_without_writer, "FIFO open waited for a writer");
        assert!(rejected, "FIFO was accepted as a regular file");
    }

    #[cfg(unix)]
    #[test]
    fn regular_file_open_and_bounded_read_reject_a_fifo_without_waiting() {
        for bounded in [false, true] {
            let root = tempdir().unwrap();
            assert_fifo_rejected_without_writer(&root.path().join("manifest.json"), move |path| {
                let result = if bounded {
                    read_regular_file_bounded(path, 1024).map(|_| ())
                } else {
                    open_regular_file(path).map(|_| ())
                };
                matches!(result, Err(error) if error.kind() == std::io::ErrorKind::InvalidData)
            });
        }
    }
}
