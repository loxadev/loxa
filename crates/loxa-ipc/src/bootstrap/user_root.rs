//! Explicit permission repair of one retained, user-owned data directory.

use crate::platform::current_uid;
use std::ffi::CString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

const PRIVATE_MODE: u32 = 0o700;
const DIRECTORY_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;

/// Inspecting an existing root does not create entries or change permissions.
#[derive(Debug)]
pub enum UserRootInspection {
    Private(PrivateUserRoot),
    RepairRequired(RootPermissionRepair),
}

impl UserRootInspection {
    /// Requires protected ancestry, an absolute canonical path, a user-owned
    /// directory, and no development-root marker or macOS extended ACL.
    /// Missing roots remain missing.
    pub fn inspect(path: &Path) -> Result<Self, UserRootError> {
        let directory = open_directory_path(path)?;
        let identity = RootIdentity::from_metadata(&directory.metadata()?, current_uid())?;
        let root = RootDirectory {
            directory,
            path: path.to_owned(),
            identity,
        };
        let observed_mode = root.current_mode()?;
        if observed_mode == PRIVATE_MODE {
            Ok(Self::Private(PrivateUserRoot { root }))
        } else {
            Ok(Self::RepairRequired(RootPermissionRepair {
                root,
                observed_mode,
            }))
        }
    }
}

/// A private root whose descriptor remains open for subsequent revalidation.
#[derive(Debug)]
pub struct PrivateUserRoot {
    root: RootDirectory,
}

impl PrivateUserRoot {
    pub fn path(&self) -> &Path {
        &self.root.path
    }

    /// Retained descriptor for bootstrap operations relative to this root.
    /// Revalidate before and after operations that also rely on its pathname.
    pub fn directory(&self) -> &File {
        &self.root.directory
    }

    pub fn validate_current(&self) -> Result<(), UserRootError> {
        if self.root.current_mode()? != PRIVATE_MODE {
            return Err(UserRootError::PermissionsChanged);
        }
        Ok(())
    }
}

/// Retains the exact directory shown by a caller's permission confirmation.
/// Dropping this value declines repair without changing the directory.
#[derive(Debug)]
pub struct RootPermissionRepair {
    root: RootDirectory,
    observed_mode: u32,
}

impl RootPermissionRepair {
    pub fn path(&self) -> &Path {
        &self.root.path
    }

    /// Permission and special bits observed during inspection.
    pub fn observed_mode(&self) -> u32 {
        self.observed_mode
    }

    /// Call only after explicit confirmation for this path and observed mode.
    /// Changes only the retained root descriptor to 0700, never its children.
    pub fn confirm(self) -> Result<PrivateUserRoot, UserRootError> {
        self.confirm_with_before_chmod(|| {})
    }

    fn confirm_with_before_chmod(
        self,
        before_chmod: impl FnOnce(),
    ) -> Result<PrivateUserRoot, UserRootError> {
        let mode = self.root.current_mode()?;
        if mode != PRIVATE_MODE {
            if mode != self.observed_mode {
                return Err(UserRootError::PermissionsChanged);
            }
            before_chmod();
            // The retained descriptor stays authoritative even if the pathname
            // changes after validation. A replacement must never receive chmod.
            self.root
                .directory
                .set_permissions(fs::Permissions::from_mode(PRIVATE_MODE))?;
        }
        let private = PrivateUserRoot { root: self.root };
        private.validate_current()?;
        Ok(private)
    }
}

#[derive(Debug)]
pub enum UserRootError {
    InvalidPath,
    UnsafePath,
    UnsafeAncestry,
    ForeignOwner,
    DevelopmentRoot,
    IdentityChanged,
    PermissionsChanged,
    ExtendedAcl,
    Io(io::Error),
}

impl fmt::Display for UserRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath => {
                formatter.write_str("data root must be an absolute canonical path")
            }
            Self::UnsafePath => {
                formatter.write_str("data root path contains a symlink or is not a directory")
            }
            Self::UnsafeAncestry => formatter.write_str(
                "data root ancestry is not protected from other users; manual access-permission review is required",
            ),
            Self::ForeignOwner => formatter.write_str("data root is not owned by the current user"),
            Self::DevelopmentRoot => {
                formatter.write_str("data root is reserved for background-service development")
            }
            Self::IdentityChanged => formatter.write_str("data root identity changed"),
            Self::PermissionsChanged => {
                formatter.write_str("data root permissions changed; inspect it again before repair")
            }
            Self::ExtendedAcl => formatter.write_str(
                "data root has an extended ACL; manual access-permission review is required",
            ),
            Self::Io(error) => write!(formatter, "could not inspect or repair data root: {error}"),
        }
    }
}

impl std::error::Error for UserRootError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for UserRootError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RootIdentity {
    device: u64,
    inode: u64,
    uid: u32,
}

impl RootIdentity {
    fn from_metadata(metadata: &fs::Metadata, expected_uid: u32) -> Result<Self, UserRootError> {
        if !metadata.file_type().is_dir() {
            return Err(UserRootError::UnsafePath);
        }
        if metadata.uid() != expected_uid {
            return Err(UserRootError::ForeignOwner);
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
        })
    }
}

#[derive(Debug)]
struct RootDirectory {
    directory: File,
    path: PathBuf,
    identity: RootIdentity,
}

impl RootDirectory {
    fn current_mode(&self) -> Result<u32, UserRootError> {
        let retained = self.directory.metadata()?;
        if RootIdentity::from_metadata(&retained, current_uid())? != self.identity {
            return Err(UserRootError::IdentityChanged);
        }
        let resolved = match open_directory_path(&self.path) {
            Err(UserRootError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(UserRootError::IdentityChanged);
            }
            result => result?,
        };
        let resolved = resolved.metadata()?;
        if RootIdentity::from_metadata(&resolved, current_uid())? != self.identity {
            return Err(UserRootError::IdentityChanged);
        }
        let mode = retained.mode() & 0o7777;
        if resolved.mode() & 0o7777 != mode {
            return Err(UserRootError::PermissionsChanged);
        }
        reject_development_marker(&self.directory)?;
        #[cfg(target_os = "macos")]
        reject_extended_acl(&self.directory)?;
        Ok(mode)
    }
}

#[cfg(target_os = "macos")]
pub(super) fn reject_extended_acl(directory: &File) -> Result<(), UserRootError> {
    // Darwin getattrlist packs a u32 length followed by 4-byte-aligned attributes.
    // EXTENDED_SECURITY is an attrreference_t whose payload is empty only when
    // no extended ACL is present. A nonempty payload is never parsed or removed.
    #[repr(C)]
    struct ExtendedSecurityHeader {
        length: u32,
        security: libc::attrreference_t,
    }
    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_EXTENDED_SECURITY,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut header = ExtendedSecurityHeader {
        length: 0,
        security: libc::attrreference_t {
            attr_dataoffset: 0,
            attr_length: 0,
        },
    };
    let header_size = std::mem::size_of_val(&header);
    // SAFETY: the descriptor remains live and both pointers refer to writable
    // C-layout values of the supplied sizes. FULLSIZE exposes a truncated ACL
    // payload through the fixed header without allocating or reading that payload.
    let result = unsafe {
        libc::fgetattrlist(
            directory.as_raw_fd(),
            (&mut attributes as *mut libc::attrlist).cast(),
            (&mut header as *mut ExtendedSecurityHeader).cast(),
            header_size,
            libc::FSOPT_REPORT_FULLSIZE,
        )
    };
    if result != 0 {
        return Err(UserRootError::Io(io::Error::last_os_error()));
    }
    if header.security.attr_length != 0 {
        return Err(UserRootError::ExtendedAcl);
    }
    if header.length as usize != header_size
        || header.security.attr_dataoffset != std::mem::size_of::<libc::attrreference_t>() as i32
    {
        return Err(UserRootError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid data root ACL metadata",
        )));
    }
    Ok(())
}

fn open_directory_path(path: &Path) -> Result<File, UserRootError> {
    let normalized: PathBuf = path.components().collect();
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.as_os_str() != normalized.as_os_str()
        || path.as_os_str().as_bytes().len() > crate::protocol::MAX_PATH_BYTES
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(UserRootError::InvalidPath);
    }
    let mut directory = OpenOptions::new()
        .read(true)
        .custom_flags(DIRECTORY_FLAGS)
        .open("/")?;
    for part in path.components() {
        let Component::Normal(name) = part else {
            continue;
        };
        validate_ancestor(&directory)?;
        let name = CString::new(name.as_bytes()).map_err(|_| UserRootError::InvalidPath)?;
        // SAFETY: the parent descriptor and NUL-terminated component remain
        // alive during openat. Every component forbids following a symlink.
        let descriptor =
            unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), DIRECTORY_FLAGS) };
        if descriptor == -1 {
            let error = io::Error::last_os_error();
            return Err(match error.raw_os_error() {
                Some(libc::ELOOP | libc::ENOTDIR) => UserRootError::UnsafePath,
                _ => UserRootError::Io(error),
            });
        }
        // SAFETY: openat returned a newly owned descriptor.
        directory = unsafe { File::from_raw_fd(descriptor) };
    }
    Ok(directory)
}

fn validate_ancestor(directory: &File) -> Result<(), UserRootError> {
    let metadata = directory.metadata()?;
    validate_ancestor_permissions(metadata.uid(), metadata.mode())?;
    #[cfg(target_os = "macos")]
    reject_mutating_ancestor_acl(directory)?;
    Ok(())
}

fn validate_ancestor_permissions(owner: u32, mode: u32) -> Result<(), UserRootError> {
    // A foreign owner can chmod even a currently read-only parent. A sticky
    // parent protects its root/current-user-owned child from other users.
    if (owner != 0 && owner != current_uid()) || (mode & 0o022 != 0 && mode & 0o1000 == 0) {
        return Err(UserRootError::UnsafeAncestry);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn reject_mutating_ancestor_acl(directory: &File) -> Result<(), UserRootError> {
    // sys/kauth.h defines a 44-byte kauth_filesec header and at most 128
    // 24-byte ACEs. fgetattrlist returns this native-endian, 4-byte-aligned
    // payload after its 12-byte length/reference header (XNU vfs_attrlist.c).
    #[repr(C)]
    struct SecurityBuffer {
        length: u32,
        reference: libc::attrreference_t,
        payload: [u8; 44 + 128 * 24],
    }
    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_EXTENDED_SECURITY,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut buffer = SecurityBuffer {
        length: 0,
        reference: libc::attrreference_t {
            attr_dataoffset: 0,
            attr_length: 0,
        },
        payload: [0; 44 + 128 * 24],
    };
    // SAFETY: the live descriptor and writable C-layout buffers match the
    // supplied sizes. FULLSIZE makes truncation visible without any retry.
    if unsafe {
        libc::fgetattrlist(
            directory.as_raw_fd(),
            (&mut attributes as *mut libc::attrlist).cast(),
            (&mut buffer as *mut SecurityBuffer).cast(),
            std::mem::size_of_val(&buffer),
            libc::FSOPT_REPORT_FULLSIZE,
        )
    } != 0
    {
        return Err(UserRootError::Io(io::Error::last_os_error()));
    }
    let payload_length = buffer.reference.attr_length as usize;
    if buffer.reference.attr_dataoffset != 8
        || payload_length > buffer.payload.len()
        || buffer.length as usize != 12 + payload_length
    {
        return Err(UserRootError::UnsafeAncestry);
    }
    validate_ancestor_acl_payload(&buffer.payload[..payload_length])
}

#[cfg(target_os = "macos")]
fn validate_ancestor_acl_payload(payload: &[u8]) -> Result<(), UserRootError> {
    if payload.is_empty() {
        return Ok(());
    }
    if payload.len() < 44 {
        return Err(UserRootError::UnsafeAncestry);
    }
    let word = |offset| u32::from_ne_bytes(payload[offset..offset + 4].try_into().unwrap());
    let count = match word(36) {
        u32::MAX => 0, // KAUTH_FILESEC_NOACL: the header has no ACEs.
        count => count as usize,
    };
    if word(0) != 0x012c_c16d
        || payload[4..36].iter().any(|byte| *byte != 0)
        || count > 128
        || payload.len() != 44 + count * 24
        || word(40) & !0x0003_ffff != 0
    {
        return Err(UserRootError::UnsafeAncestry);
    }
    // Admit only recognized deny or read/search permits, independent of UUID.
    // In XNU, explicit DELETE/DELETE_CHILD allows outrank the sticky bit.
    const READ_RIGHTS: u32 =
        (1 << 1) | (1 << 3) | (1 << 7) | (1 << 9) | (1 << 11) | (1 << 20) | (1 << 22) | (1 << 24);
    for index in 0..count {
        let flags = word(44 + index * 24 + 16);
        let rights = word(44 + index * 24 + 20);
        if flags & !0x01ff != 0
            || !matches!(flags & 0xf, 1 | 2)
            || (flags & 0xf == 1 && rights & !READ_RIGHTS != 0)
        {
            return Err(UserRootError::UnsafeAncestry);
        }
    }
    Ok(())
}

fn reject_development_marker(directory: &File) -> Result<(), UserRootError> {
    let name =
        CString::new(super::DEVELOPMENT_MARKER_FILENAME).map_err(|_| UserRootError::InvalidPath)?;
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the directory/name remain alive and metadata has space for stat.
    // A marker of any type, including a dangling symlink, reserves this root.
    let result = unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Err(UserRootError::DevelopmentRoot);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(UserRootError::Io(error))
    }
}

#[cfg(test)]
mod tests;
