use super::ArtifactTransferError;
use crate::safe_file::{
    ensure_regular_descriptors_match, regular_file_identity, RegularFileIdentity,
};
use std::fs::File;
#[cfg(not(unix))]
use std::fs::OpenOptions;
use std::path::Path;

pub(super) type OpenArtifactEntry = fn(&File, &Path) -> std::io::Result<File>;

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn rename_part_to_final_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part",
        directory,
        "model.gguf",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn rename_final_to_invalid_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf",
        directory,
        "model.gguf.invalid",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn rename_restart_to_final_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part.restart",
        directory,
        "model.gguf",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn rename_restart_to_part_no_replace(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part.restart",
        directory,
        "model.gguf.part",
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn rename_restart_to_part_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace restart normalization is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn rename_restart_to_final_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace restart promotion is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn rename_part_to_final_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative no-replace promotion is unsupported",
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn rename_final_to_invalid_no_replace(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative corrupt-artifact quarantine is unsupported",
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn exchange_part_and_restart(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags};

    renameat_with(
        directory,
        "model.gguf.part",
        directory,
        "model.gguf.part.restart",
        RenameFlags::EXCHANGE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn exchange_part_and_restart(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative prefix exchange is unsupported",
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn unlink_invalid_entry(directory: &File) -> std::io::Result<()> {
    use rustix::fs::{unlinkat, AtFlags};

    unlinkat(directory, "model.gguf.invalid", AtFlags::empty())
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn unlink_invalid_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative repair cleanup is unsupported",
    ))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn unlink_repair_entry_if_present(directory: &File, name: &[u8]) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the retained live model-directory descriptor and
    // callers provide only fixed NUL-terminated repair entry names.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr().cast(), 0) };
    if result == -1 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn unlink_repair_entry_if_present(
    _directory: &File,
    _name: &[u8],
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative repair cleanup is unsupported",
    ))
}

pub(super) fn capture_artifact_entry(
    directory: &File,
    path: &Path,
    open_entry: OpenArtifactEntry,
) -> Result<Option<(File, RegularFileIdentity)>, ArtifactTransferError> {
    let file = match open_entry(directory, path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ArtifactTransferError::Durability),
    };
    let identity =
        regular_file_identity(&file, path).map_err(|_| ArtifactTransferError::Durability)?;
    let resolved = open_entry(directory, path).map_err(|_| ArtifactTransferError::Durability)?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, path)
        .map_err(|_| ArtifactTransferError::Durability)?;
    Ok(Some((file, identity)))
}

pub(super) fn reject_unsafe_artifact_entry_if_present(
    directory: &File,
    path: &Path,
    open_entry: OpenArtifactEntry,
) -> Result<(), String> {
    let file = match open_entry(directory, path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    let identity = regular_file_identity(&file, path).map_err(|error| error.to_string())?;
    let resolved =
        open_entry(directory, path).map_err(|error| format!("{}: {error}", path.display()))?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, path)
        .map_err(|_| format!("unsafe artifact path {}", path.display()))
}

#[cfg(unix)]
pub(super) fn create_part_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    open_artifact_entry_for_write(directory, c"model.gguf.part", libc::O_CREAT | libc::O_EXCL)
}

#[cfg(not(unix))]
pub(super) fn create_part_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

#[cfg(unix)]
pub(super) fn create_restart_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    open_artifact_entry_for_write(
        directory,
        c"model.gguf.part.restart",
        libc::O_CREAT | libc::O_EXCL,
    )
}

#[cfg(not(unix))]
pub(super) fn create_restart_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).write(true).open(path)
}

#[cfg(unix)]
pub(super) fn open_part_entry_for_write(
    directory: &File,
    _path: &Path,
    append: bool,
) -> std::io::Result<File> {
    open_artifact_entry_for_write(
        directory,
        c"model.gguf.part",
        if append { libc::O_APPEND } else { 0 },
    )
}

#[cfg(not(unix))]
pub(super) fn open_part_entry_for_write(
    _directory: &File,
    path: &Path,
    append: bool,
) -> std::io::Result<File> {
    OpenOptions::new().write(true).append(append).open(path)
}

#[cfg(unix)]
fn open_artifact_entry_for_write(
    directory: &File,
    name: &std::ffi::CStr,
    extra_flags: libc::c_int,
) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model-directory descriptor, callers
    // provide only closed artifact literals, and a successful descriptor is
    // transferred to `File` exactly once.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_WRONLY | extra_flags,
            0o600,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
pub(super) fn open_final_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
pub(super) fn open_final_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.open(path)
}

#[cfg(unix)]
pub(super) fn open_part_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.part".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
pub(super) fn open_part_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
pub(super) fn open_restart_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model directory descriptor, the name
    // is the closed restart literal, and a successful descriptor is owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.part.restart".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
pub(super) fn open_restart_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
pub(super) fn open_invalid_entry(directory: &File, _path: &Path) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `directory` is the pinned model directory descriptor, the name
    // is the closed invalid-artifact literal, and a successful descriptor is
    // owned below.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"model.gguf.invalid".as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(not(unix))]
pub(super) fn open_invalid_entry(_directory: &File, path: &Path) -> std::io::Result<File> {
    crate::safe_file::open_regular_file(path).map(|(file, _)| file)
}

#[cfg(unix)]
pub(super) fn unlink_part_entry(directory: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the pinned model directory descriptor and the
    // NUL-terminated name is the one closed authoritative part literal.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), c"model.gguf.part".as_ptr(), 0) };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
pub(super) fn unlink_part_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative artifact cleanup is unsupported",
    ))
}

#[cfg(unix)]
pub(super) fn unlink_restart_entry(directory: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `directory` is the pinned model directory descriptor and the
    // NUL-terminated name is the one closed restart literal.
    let result = unsafe {
        libc::unlinkat(
            directory.as_raw_fd(),
            c"model.gguf.part.restart".as_ptr(),
            0,
        )
    };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
pub(super) fn unlink_restart_entry(_directory: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "descriptor-relative artifact cleanup is unsupported",
    ))
}

pub(super) fn reject_unsafe_open_transfer(file: &File, path: &Path) -> Result<(), String> {
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
