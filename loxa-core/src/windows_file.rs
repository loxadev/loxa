use std::fs::{File, Metadata, OpenOptions};
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::path::Path;

const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
const FILE_ATTRIBUTE_REPARSE_POINT: u64 = 0x0000_0400;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct FileIdentity {
    pub(crate) volume_serial: u32,
    pub(crate) file_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileInformation {
    pub(crate) identity: FileIdentity,
    pub(crate) number_of_links: u64,
    pub(crate) last_write_time: u64,
    is_reparse_point: bool,
}

pub(crate) fn information(file: &File) -> io::Result<FileInformation> {
    let information = winapi_util::file::information(file)?;
    let volume_serial = u32::try_from(information.volume_serial_number())
        .map_err(|_| io::Error::other("Windows volume serial exceeded u32"))?;
    let file_index = information.file_index();
    if volume_serial == 0 || file_index == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows file identity is unavailable",
        ));
    }
    Ok(FileInformation {
        identity: FileIdentity {
            volume_serial,
            file_index,
        },
        number_of_links: information.number_of_links(),
        last_write_time: information.last_write_time().unwrap_or_default(),
        is_reparse_point: information.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0,
    })
}

pub(crate) fn open_path_no_follow(
    path: &Path,
    allow_directory: bool,
) -> io::Result<(File, Metadata, FileInformation)> {
    let before = std::fs::symlink_metadata(path)?;
    if before.file_type().is_symlink()
        || (!allow_directory && !before.file_type().is_file())
        || (allow_directory && !before.file_type().is_file() && !before.file_type().is_dir())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "path is not an allowed regular file or directory",
        ));
    }

    let mut flags = FILE_FLAG_OPEN_REPARSE_POINT;
    if allow_directory {
        flags |= FILE_FLAG_BACKUP_SEMANTICS;
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)?;
    let opened = file.metadata()?;
    if (!allow_directory && !opened.file_type().is_file())
        || (allow_directory && !opened.file_type().is_file() && !opened.file_type().is_dir())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "opened path changed file type",
        ));
    }
    let information = information(&file)?;
    if information.is_reparse_point {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows reparse points are not trusted artifact paths",
        ));
    }
    Ok((file, opened, information))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn handle_and_no_follow_path_report_the_same_strong_identity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("model.gguf");
        fs::write(&path, b"model").unwrap();
        let direct = File::open(&path).unwrap();
        let direct_information = information(&direct).unwrap();
        let (_, metadata, path_information) = open_path_no_follow(&path, false).unwrap();

        assert!(metadata.file_type().is_file());
        assert_eq!(direct_information.identity, path_information.identity);
        assert_eq!(direct_information.number_of_links, 1);
    }

    #[test]
    fn hardlink_count_and_distinct_file_identity_are_not_guessed_from_metadata() {
        let directory = tempdir().unwrap();
        let original = directory.path().join("original.gguf");
        let linked = directory.path().join("linked.gguf");
        let distinct = directory.path().join("distinct.gguf");
        fs::write(&original, b"same bytes").unwrap();
        fs::hard_link(&original, &linked).unwrap();
        fs::write(&distinct, b"same bytes").unwrap();

        let original_information = information(&File::open(&original).unwrap()).unwrap();
        let linked_information = information(&File::open(&linked).unwrap()).unwrap();
        let distinct_information = information(&File::open(&distinct).unwrap()).unwrap();

        assert_eq!(original_information.identity, linked_information.identity);
        assert_eq!(original_information.number_of_links, 2);
        assert_ne!(original_information.identity, distinct_information.identity);
    }
}
