//! Stage naming, owner-record format, and errno-only child claims before exec.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{FileExt as _, OpenOptionsExt as _, PermissionsExt};
use std::path::Path;
#[cfg(all(test, target_os = "macos"))]
use std::path::PathBuf;

pub(super) const STAGING_PREFIX: &str = ".bundled-runtime-exec-";
pub(super) const BUILD_STAGING_PREFIX: &str = ".bundled-runtime-build-";
pub(super) const STAGE_RECORD_NAME: &str = ".loxa-execution-stage-v1";
const STAGE_RECORD_MAGIC: &[u8; 16] = b"LOXA-STAGE-v1\0\0\0";
pub(super) const STAGE_RECORD_LEN: usize = 80;
const STAGE_OWNER_PID_OFFSET: usize = 16;
const STAGE_OWNER_START_OFFSET: usize = 20;
pub(super) const STAGE_CHILD_PID_OFFSET: usize = 28;
const STAGE_CHILD_GROUP_OFFSET: usize = 32;
pub(super) const STAGE_TOKEN_OFFSET: usize = 36;
const STAGE_TOKEN_LEN: usize = 32;
pub(crate) const STAGE_STATE_OFFSET: usize = STAGE_TOKEN_OFFSET + STAGE_TOKEN_LEN;
type StageRecordFields = (u32, u64, Option<(u32, i32)>, bool);

pub(super) fn execution_stage_root<'a>(run: &Path, execution_server: &'a Path) -> Option<&'a Path> {
    if execution_server.file_name()? != "llama-server" {
        return None;
    }
    let macos = execution_server.parent()?;
    if macos.file_name()? != "MacOS" {
        return None;
    }
    let contents = macos.parent()?;
    if contents.file_name()? != "Contents" {
        return None;
    }
    let stage = contents.parent()?;
    if stage.parent()? != run {
        return None;
    }
    execution_stage_token(stage).map(|_| stage)
}

pub(super) fn execution_stage_token(stage: &Path) -> Option<&str> {
    let token = stage.file_name()?.to_str()?.strip_prefix(STAGING_PREFIX)?;
    (token.len() == STAGE_TOKEN_LEN
        && token
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)))
    .then_some(token)
}

pub(super) fn execution_build_owner(build: &Path) -> Option<(u32, u64)> {
    let name = build
        .file_name()?
        .to_str()?
        .strip_prefix(BUILD_STAGING_PREFIX)?;
    let mut fields = name.split('-');
    let owner_pid_text = fields.next()?;
    let owner_start_text = fields.next()?;
    let token = fields.next()?;
    if fields.next().is_some()
        || token.len() != STAGE_TOKEN_LEN
        || !token
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return None;
    }
    let owner_pid = owner_pid_text.parse::<u32>().ok()?;
    let owner_start = owner_start_text.parse::<u64>().ok()?;
    (owner_pid >= 2
        && owner_start > 0
        && owner_pid.to_string() == owner_pid_text
        && owner_start.to_string() == owner_start_text)
        .then_some((owner_pid, owner_start))
}

pub(super) fn read_stage_record(record: &File) -> Result<[u8; STAGE_RECORD_LEN], String> {
    let mut bytes = [0_u8; STAGE_RECORD_LEN];
    record
        .read_exact_at(&mut bytes, 0)
        .map_err(|_| "prepared runtime recovery record is unreadable".to_string())?;
    Ok(bytes)
}

pub(super) fn parse_stage_record(
    bytes: &[u8; STAGE_RECORD_LEN],
    token: &str,
) -> Result<StageRecordFields, String> {
    if &bytes[..STAGE_RECORD_MAGIC.len()] != STAGE_RECORD_MAGIC
        || &bytes[STAGE_TOKEN_OFFSET..STAGE_TOKEN_OFFSET + STAGE_TOKEN_LEN] != token.as_bytes()
        || !matches!(bytes[STAGE_STATE_OFFSET], 0 | 1)
        || bytes[STAGE_STATE_OFFSET + 1..]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err("prepared runtime recovery record is invalid".into());
    }
    let owner_pid = u32::from_le_bytes(
        bytes[STAGE_OWNER_PID_OFFSET..STAGE_OWNER_START_OFFSET]
            .try_into()
            .expect("fixed owner pid field"),
    );
    let owner_start = u64::from_le_bytes(
        bytes[STAGE_OWNER_START_OFFSET..STAGE_CHILD_PID_OFFSET]
            .try_into()
            .expect("fixed owner start field"),
    );
    let child_pid = u32::from_le_bytes(
        bytes[STAGE_CHILD_PID_OFFSET..STAGE_CHILD_GROUP_OFFSET]
            .try_into()
            .expect("fixed child pid field"),
    );
    let child_group = i32::from_le_bytes(
        bytes[STAGE_CHILD_GROUP_OFFSET..STAGE_TOKEN_OFFSET]
            .try_into()
            .expect("fixed child group field"),
    );
    if owner_pid == 0
        || owner_start == 0
        || !matches!((child_pid, child_group), (0, 0) | (2.., 2..))
        || (child_pid != 0 && i32::try_from(child_pid).ok() != Some(child_group))
    {
        return Err("prepared runtime recovery record is invalid".into());
    }
    Ok((
        owner_pid,
        owner_start,
        (child_pid != 0).then_some((child_pid, child_group)),
        bytes[STAGE_STATE_OFFSET] == 1,
    ))
}

#[cfg(all(test, target_os = "macos"))]
pub(super) fn create_execution_stage_record(stage: &Path, token: &str) -> Result<File, String> {
    let owner_pid = std::process::id();
    let owner_start = crate::process_inspection::current_process_start_identity()?;
    create_execution_stage_record_for_owner(stage, token, owner_pid, owner_start)
}

pub(super) fn create_execution_stage_record_for_owner(
    stage: &Path,
    token: &str,
    owner_pid: u32,
    owner_start: u64,
) -> Result<File, String> {
    let mut bytes = [0_u8; STAGE_RECORD_LEN];
    bytes[..STAGE_RECORD_MAGIC.len()].copy_from_slice(STAGE_RECORD_MAGIC);
    bytes[STAGE_OWNER_PID_OFFSET..STAGE_OWNER_START_OFFSET]
        .copy_from_slice(&owner_pid.to_le_bytes());
    bytes[STAGE_OWNER_START_OFFSET..STAGE_CHILD_PID_OFFSET]
        .copy_from_slice(&owner_start.to_le_bytes());
    bytes[STAGE_TOKEN_OFFSET..STAGE_TOKEN_OFFSET + STAGE_TOKEN_LEN]
        .copy_from_slice(token.as_bytes());

    let path = stage.join(STAGE_RECORD_NAME);
    let mut options = OpenOptions::new();
    options
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut record = options
        .open(&path)
        .map_err(|_| "prepared runtime owner record could not be created".to_string())?;
    // SAFETY: the descriptor is a newly created private regular file retained
    // by the execution-stage guard for its complete lifetime.
    if unsafe { libc::flock(record.as_raw_fd(), libc::LOCK_EX) } == -1 {
        return Err("prepared runtime owner record could not be locked".into());
    }
    #[cfg(all(test, target_os = "macos"))]
    if stage_construction_checkpoint_is_for_test("mid-record") {
        record
            .write_all(&bytes[..STAGE_RECORD_LEN / 2])
            .and_then(|()| record.sync_all())
            .expect("partial stage record test checkpoint could not be published");
        stage_construction_checkpoint_for_test("mid-record", stage);
    }
    record
        .write_all(&bytes)
        .and_then(|()| record.sync_all())
        .map_err(|_| "prepared runtime owner record could not be published".to_string())?;
    record
        .set_permissions(fs::Permissions::from_mode(0o400))
        .and_then(|()| record.sync_all())
        .map_err(|_| "prepared runtime owner record could not be sealed".to_string())?;
    Ok(record)
}

#[cfg(all(test, target_os = "macos"))]
fn stage_construction_checkpoint_is_for_test(phase: &str) -> bool {
    std::env::var("LOXA_TEST_STAGE_CONSTRUCTION_KILL_PHASE").as_deref() == Ok(phase)
}

#[cfg(all(test, target_os = "macos"))]
pub(super) fn stage_construction_checkpoint_for_test(phase: &str, stage: &Path) {
    let kill = stage_construction_checkpoint_is_for_test(phase);
    let pause = std::env::var("LOXA_TEST_STAGE_CONSTRUCTION_PAUSE_PHASE").as_deref() == Ok(phase);
    if !kill && !pause {
        return;
    }
    let ready = PathBuf::from(
        std::env::var_os("LOXA_TEST_STAGE_CONSTRUCTION_READY")
            .expect("stage construction checkpoint requires a ready path"),
    );
    fs::write(
        ready,
        stage
            .file_name()
            .expect("stage construction checkpoint has a file name")
            .as_encoded_bytes(),
    )
    .expect("stage construction checkpoint could not be published");
    if pause {
        loop {
            std::thread::park();
        }
    }
    // SAFETY: this test-only subprocess deliberately models abrupt constructor death.
    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    unreachable!("SIGKILL returned in the stage-construction subprocess");
}

pub(super) fn write_stage_child_claim(record_fd: libc::c_int) -> std::io::Result<()> {
    // SAFETY: these calls only observe the current post-fork process and group.
    let child_pid = unsafe { libc::getpid() };
    // SAFETY: getpgrp has no caller-side preconditions.
    let child_group = unsafe { libc::getpgrp() };
    if child_pid <= 1 || child_group != child_pid {
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
    let child_pid = match u32::try_from(child_pid) {
        Ok(child_pid) => child_pid,
        Err(_) => return Err(std::io::Error::from_raw_os_error(libc::EOVERFLOW)),
    };
    let mut claim = [0_u8; 8];
    claim[..4].copy_from_slice(&child_pid.to_le_bytes());
    claim[4..].copy_from_slice(&child_group.to_le_bytes());
    let mut written = 0;
    while written < claim.len() {
        // SAFETY: `record_fd` is the retained writable stage record; the byte
        // slice remains valid for this call, and the fixed offset is in-bounds.
        let result = unsafe {
            libc::pwrite(
                record_fd,
                claim[written..].as_ptr().cast(),
                claim.len() - written,
                (STAGE_CHILD_PID_OFFSET + written) as libc::off_t,
            )
        };
        if result == -1 {
            let error = errno_only_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if result == 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }
        written += result as usize;
    }
    // SAFETY: fsync acts only on the retained writable stage record. It makes
    // the completed fixed-size claim visible before exec closes this CLOEXEC fd.
    if unsafe { libc::fsync(record_fd) } == -1 {
        return Err(errno_only_error());
    }
    Ok(())
}

pub(super) fn errno_only_error() -> std::io::Error {
    #[cfg(target_os = "macos")]
    // SAFETY: `__error` returns the calling thread's errno location.
    let errno = unsafe { *libc::__error() };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    // SAFETY: `__errno_location` returns the calling thread's errno location.
    let errno = unsafe { *libc::__errno_location() };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EIO);
    std::io::Error::from_raw_os_error(errno)
}

pub(super) fn random_stage_token() -> Result<String, String> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|_| "prepared runtime staging randomness is unavailable".to_string())?;
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut token, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(token)
}
