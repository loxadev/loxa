use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegularFileIdentity {
    size: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    links: u64,
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
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                size: metadata.len(),
            })
        }
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }
}

pub(crate) fn read_regular_file(path: &Path) -> io::Result<Vec<u8>> {
    read_regular_file_with_after_open(path, || {})
}

pub(crate) fn open_regular_file(path: &Path) -> io::Result<(File, RegularFileIdentity)> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let identity = RegularFileIdentity::from_metadata(&file.metadata()?, path)?;
    Ok((file, identity))
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

#[cfg(test)]
mod tests {
    use super::read_regular_file_with_after_open;
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
}
