//! Descriptor-backed recovery capabilities and exact stage cleanup.

use std::fs::{self, File};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::record::{
    execution_build_owner, execution_stage_root, execution_stage_token, parse_stage_record,
    read_stage_record, STAGE_RECORD_LEN, STAGE_RECORD_NAME,
};

pub(crate) struct RecoverableExecutionStage {
    root: PathBuf,
    server: PathBuf,
    stage: File,
    stage_identity: crate::safe_file::DirectoryIdentity,
    record: File,
    record_identity: crate::safe_file::RegularFileIdentity,
    record_path: PathBuf,
    token: String,
    owner_pid: u32,
    owner_start: u64,
    child: Option<(u32, i32)>,
    abandoned: bool,
    recovery_locked: bool,
}

pub(crate) struct RecoverableExecutionBuild {
    root: PathBuf,
    build: File,
    build_identity: crate::safe_file::DirectoryIdentity,
    owner_pid: u32,
    owner_start: u64,
}

impl RecoverableExecutionBuild {
    pub(crate) fn owner(&self) -> (u32, u64) {
        (self.owner_pid, self.owner_start)
    }

    pub(crate) fn ensure_current(&self) -> Result<(), String> {
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &self.build,
            &self.build_identity,
            &self.root,
        )
        .map_err(|_| "prepared runtime construction changed during recovery".to_string())
    }

    pub(crate) fn cleanup(self) -> Result<(), String> {
        self.ensure_current()?;
        fs::remove_dir_all(&self.root)
            .map_err(|_| "prepared runtime construction could not be cleaned up".to_string())
    }
}

impl RecoverableExecutionStage {
    pub(crate) fn owner(&self) -> (u32, u64) {
        (self.owner_pid, self.owner_start)
    }

    pub(crate) fn child(&self) -> Option<(u32, i32)> {
        self.child
    }

    pub(crate) fn is_abandoned(&self) -> bool {
        self.abandoned
    }

    pub(crate) fn server(&self) -> &Path {
        &self.server
    }

    pub(crate) fn lock_and_refresh(&mut self) -> Result<bool, String> {
        // SAFETY: the descriptor is the opened single-link stage record and is
        // retained by this recovery capability until reconciliation completes.
        let result = unsafe { libc::flock(self.record.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if matches!(
                error.raw_os_error(),
                Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
            ) {
                return Ok(false);
            }
            return Err("prepared runtime recovery record could not be locked".into());
        }
        self.recovery_locked = true;
        let bytes = read_stage_record(&self.record)?;
        let (owner_pid, owner_start, child, abandoned) = parse_stage_record(&bytes, &self.token)?;
        if (owner_pid, owner_start) != (self.owner_pid, self.owner_start) {
            return Err("prepared runtime recovery owner changed while locking".into());
        }
        self.child = child;
        self.abandoned = abandoned;
        self.record_identity =
            crate::safe_file::regular_file_identity(&self.record, &self.record_path).map_err(
                |_| "prepared runtime recovery record changed while locking".to_string(),
            )?;
        Ok(true)
    }

    pub(crate) fn ensure_current(&self) -> Result<(), String> {
        if !self.recovery_locked {
            return Err("prepared runtime recovery record is not locked".into());
        }
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &self.stage,
            &self.stage_identity,
            &self.root,
        )
        .map_err(|_| "prepared runtime recovery stage changed".to_string())?;
        crate::safe_file::ensure_descriptor_matches_path(
            &self.record,
            &self.record_identity,
            &self.record_path,
        )
        .map_err(|_| "prepared runtime recovery record changed".to_string())
    }

    pub(crate) fn cleanup(self) -> Result<(), String> {
        self.ensure_current()?;
        fs::remove_dir_all(&self.root)
            .map_err(|_| "prepared runtime recovery stage could not be cleaned up".to_string())
    }
}

pub(crate) fn cleanup_execution_stage(run: &Path, execution_server: &Path) -> Result<(), String> {
    let Some(stage) = execution_stage_root(run, execution_server) else {
        return Ok(());
    };
    let metadata = match fs::symlink_metadata(stage) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("prepared runtime stage is unavailable for cleanup".into()),
    };
    // SAFETY: `geteuid` has no arguments or caller-side preconditions.
    let current_euid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_dir()
        || metadata.uid() != current_euid
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("prepared runtime stage is unsafe for cleanup".into());
    }
    fs::remove_dir_all(stage)
        .map_err(|_| "prepared runtime stage could not be cleaned up".to_string())
}

pub(crate) fn recoverable_execution_builds(
    run: &Path,
) -> Result<Vec<RecoverableExecutionBuild>, String> {
    let entries = fs::read_dir(run)
        .map_err(|_| "prepared runtime recovery directory is unavailable".to_string())?;
    let mut builds = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|_| "prepared runtime recovery directory is unreadable".to_string())?;
        let root = entry.path();
        let Some((owner_pid, owner_start)) = execution_build_owner(&root) else {
            continue;
        };
        let metadata = match fs::symlink_metadata(&root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("prepared runtime construction is unavailable".into()),
        };
        // SAFETY: geteuid has no arguments or caller-side preconditions.
        let current_euid = unsafe { libc::geteuid() };
        if !metadata.file_type().is_dir()
            || metadata.uid() != current_euid
            || metadata.permissions().mode() & 0o777 != 0o700
        {
            continue;
        }
        let (build, build_identity) = crate::safe_file::open_directory(&root)
            .map_err(|_| "prepared runtime construction is unsafe".to_string())?;
        builds.push(RecoverableExecutionBuild {
            root,
            build,
            build_identity,
            owner_pid,
            owner_start,
        });
    }
    Ok(builds)
}

pub(crate) fn recoverable_execution_stages(
    run: &Path,
) -> Result<Vec<RecoverableExecutionStage>, String> {
    let entries = fs::read_dir(run)
        .map_err(|_| "prepared runtime recovery directory is unavailable".to_string())?;
    let mut stages = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|_| "prepared runtime recovery directory is unreadable".to_string())?;
        let root = entry.path();
        let Some(token) = execution_stage_token(&root).map(str::to_owned) else {
            continue;
        };
        let metadata = match fs::symlink_metadata(&root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                return Err("prepared runtime recovery stage is unavailable".into());
            }
        };
        // SAFETY: geteuid has no arguments or caller-side preconditions.
        let current_euid = unsafe { libc::geteuid() };
        if !metadata.file_type().is_dir()
            || metadata.uid() != current_euid
            || metadata.permissions().mode() & 0o777 != 0o700
        {
            continue;
        }
        let (stage, stage_identity) = crate::safe_file::open_directory(&root)
            .map_err(|_| "prepared runtime recovery stage is unsafe".to_string())?;
        let descriptor = match rustix::fs::openat(
            &stage,
            STAGE_RECORD_NAME,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(rustix::io::Errno::NOENT) => continue,
            Err(_) => {
                return Err("prepared runtime recovery record is unsafe".into());
            }
        };
        let record = File::from(descriptor);
        let record_path = root.join(STAGE_RECORD_NAME);
        let record_metadata = record
            .metadata()
            .map_err(|_| "prepared runtime recovery record is unreadable".to_string())?;
        if !record_metadata.file_type().is_file()
            || record_metadata.uid() != current_euid
            || record_metadata.nlink() != 1
            || record_metadata.permissions().mode() & 0o777 != 0o400
            || record_metadata.len() != STAGE_RECORD_LEN as u64
        {
            return Err("prepared runtime recovery record is unsafe".into());
        }
        let bytes = read_stage_record(&record)?;
        let (owner_pid, owner_start, child, abandoned) = parse_stage_record(&bytes, &token)?;
        let record_identity = crate::safe_file::regular_file_identity(&record, &record_path)
            .map_err(|_| "prepared runtime recovery record is unsafe".to_string())?;
        stages.push(RecoverableExecutionStage {
            server: root.join("Contents/MacOS/llama-server"),
            root,
            stage,
            stage_identity,
            record,
            record_identity,
            record_path,
            token,
            owner_pid,
            owner_start,
            child,
            abandoned,
            recovery_locked: false,
        });
    }
    Ok(stages)
}

pub(crate) fn execution_stage_is_abandoned(
    run: &Path,
    execution_server: &Path,
) -> Result<bool, String> {
    let Some(expected_root) = execution_stage_root(run, execution_server) else {
        return Ok(false);
    };
    Ok(recoverable_execution_stages(run)?
        .into_iter()
        .any(|stage| stage.root == expected_root && stage.abandoned))
}
