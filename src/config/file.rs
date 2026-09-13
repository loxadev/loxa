use crate::safe_file::{
    ensure_descriptor_matches_path, ensure_directory_descriptor_matches_path,
    regular_file_identity, regular_path_identity, DirectoryIdentity, RegularFileIdentity,
};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(1);
const PRIVATE_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteFault {
    None,
    BeforeRename,
    AfterRename,
}

#[derive(Debug)]
pub(super) struct WriteFailure {
    pub(super) outcome_unknown: bool,
    pub(super) context: String,
}

pub(super) fn replace(path: &Path, bytes: &[u8], fault: WriteFault) -> Result<(), WriteFailure> {
    if bytes.len() > super::MAX_CONFIG_BYTES {
        return Err(failure(false, "encoded config exceeds its byte limit"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| failure(false, "config has no parent directory"))?;
    let (directory, directory_identity) = crate::safe_file::open_directory(parent)
        .map_err(|error| io_failure(false, "open config directory", error))?;
    let destination = capture_destination(path)?;
    let (temporary_path, mut temporary) = create_temporary(parent, path)?;
    let written = (|| {
        temporary
            .write_all(bytes)
            .map_err(|error| io_failure(false, "write config", error))?;
        durable_file_sync(&temporary).map_err(|error| io_failure(false, "sync config", error))?;
        let identity = regular_file_identity(&temporary, &temporary_path)
            .map_err(|error| io_failure(false, "validate temporary config", error))?;
        identity
            .require_private_user_file(&temporary_path)
            .map_err(|error| io_failure(false, "validate temporary config", error))?;
        ensure_descriptor_matches_path(&temporary, &identity, &temporary_path)
            .map_err(|error| io_failure(false, "revalidate temporary config", error))?;
        validate_before_rename(
            &directory,
            &directory_identity,
            parent,
            path,
            destination.as_ref(),
        )?;
        if fault == WriteFault::BeforeRename {
            return Err(failure(false, "injected config failure before rename"));
        }
        fs::rename(&temporary_path, path)
            .map_err(|error| io_failure(false, "replace config", error))?;
        let installed = regular_path_identity(path)
            .map_err(|error| io_failure(true, "validate replaced config", error))?;
        if !identity.same_file_after_rename(&installed) {
            return Err(failure(
                true,
                "replaced config identity changed after rename",
            ));
        }
        installed
            .require_private_user_file(path)
            .map_err(|error| io_failure(true, "validate replaced config", error))?;
        if fault == WriteFault::AfterRename {
            return Err(failure(
                true,
                "injected config failure after rename and before directory sync",
            ));
        }
        sync_directory(&directory, &directory_identity, parent)
            .map_err(|error| io_failure(true, "sync config directory", error))?;
        ensure_descriptor_matches_path(&temporary, &installed, path)
            .map_err(|error| io_failure(true, "revalidate replaced config", error))
    })();
    if written.as_ref().is_err_and(|error| !error.outcome_unknown) {
        let current = regular_file_identity(&temporary, &temporary_path);
        if current.as_ref().is_ok_and(|identity| {
            identity.require_private_user_file(&temporary_path).is_ok()
                && ensure_descriptor_matches_path(&temporary, identity, &temporary_path).is_ok()
        }) {
            let _ = fs::remove_file(&temporary_path);
        }
    }
    written
}

pub(super) fn reconcile(path: &Path, expected: &[u8]) -> Result<(), WriteFailure> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut opened_file = options
        .open(path)
        .map_err(|error| io_failure(true, "open uncertain config", error))?;
    let opened = regular_file_identity(&opened_file, path)
        .map_err(|error| io_failure(true, "validate uncertain config", error))?;
    opened
        .require_private_user_file(path)
        .map_err(|error| io_failure(true, "validate uncertain config", error))?;
    let mut bytes = Vec::with_capacity(expected.len().min(super::MAX_CONFIG_BYTES));
    Read::by_ref(&mut opened_file)
        .take(super::MAX_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_failure(true, "read uncertain config", error))?;
    if bytes.len() > super::MAX_CONFIG_BYTES {
        return Err(failure(true, "uncertain config exceeds its byte limit"));
    }
    ensure_descriptor_matches_path(&opened_file, &opened, path)
        .map_err(|error| io_failure(true, "revalidate uncertain config", error))?;
    if bytes != expected {
        return Err(failure(
            true,
            "uncertain config does not match the retained candidate",
        ));
    }
    durable_file_sync(&opened_file)
        .map_err(|error| io_failure(true, "sync uncertain config", error))?;
    ensure_descriptor_matches_path(&opened_file, &opened, path)
        .map_err(|error| io_failure(true, "revalidate uncertain config", error))?;
    let parent = path
        .parent()
        .ok_or_else(|| failure(true, "config has no parent directory"))?;
    let (directory, identity) = crate::safe_file::open_directory(parent)
        .map_err(|error| io_failure(true, "open config directory", error))?;
    ensure_descriptor_matches_path(&opened_file, &opened, path)
        .map_err(|error| io_failure(true, "revalidate uncertain config", error))?;
    sync_directory(&directory, &identity, parent)
        .map_err(|error| io_failure(true, "sync config directory", error))?;
    ensure_descriptor_matches_path(&opened_file, &opened, path)
        .map_err(|error| io_failure(true, "revalidate uncertain config", error))
}

fn capture_destination(path: &Path) -> Result<Option<RegularFileIdentity>, WriteFailure> {
    match regular_path_identity(path) {
        Ok(identity) => {
            identity
                .require_private_user_file(path)
                .map_err(|error| io_failure(false, "validate current config", error))?;
            Ok(Some(identity))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_failure(false, "validate current config", error)),
    }
}

fn validate_before_rename(
    directory: &File,
    directory_identity: &DirectoryIdentity,
    parent: &Path,
    path: &Path,
    expected: Option<&RegularFileIdentity>,
) -> Result<(), WriteFailure> {
    ensure_directory_descriptor_matches_path(directory, directory_identity, parent)
        .map_err(|error| io_failure(false, "revalidate config directory", error))?;
    match (expected, regular_path_identity(path)) {
        (Some(expected), Ok(current)) if expected == &current => Ok(()),
        (None, Err(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        _ => Err(failure(false, "config changed before durable replace")),
    }
}

fn create_temporary(parent: &Path, path: &Path) -> Result<(PathBuf, File), WriteFailure> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| failure(false, "config filename is invalid"))?;
    for _ in 0..16 {
        let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), sequence));
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .mode(PRIVATE_MODE)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        match options.open(&temporary) {
            Ok(file) => {
                file.set_permissions(fs::Permissions::from_mode(PRIVATE_MODE))
                    .map_err(|error| io_failure(false, "secure temporary config", error))?;
                return Ok((temporary, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_failure(false, "create temporary config", error)),
        }
    }
    Err(failure(
        false,
        "temporary config name capacity is exhausted",
    ))
}

fn sync_directory(directory: &File, identity: &DirectoryIdentity, parent: &Path) -> io::Result<()> {
    ensure_directory_descriptor_matches_path(directory, identity, parent)?;
    directory.sync_all()?;
    ensure_directory_descriptor_matches_path(directory, identity, parent)
}

fn durable_file_sync(file: &File) -> io::Result<()> {
    file.sync_all()
}

fn io_failure(outcome_unknown: bool, action: &str, error: io::Error) -> WriteFailure {
    failure(outcome_unknown, format!("{action} failed: {error}"))
}

fn failure(outcome_unknown: bool, context: impl Into<String>) -> WriteFailure {
    WriteFailure {
        outcome_unknown,
        context: context.into(),
    }
}
