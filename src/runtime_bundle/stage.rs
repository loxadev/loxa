//! Prepared-stage ownership, publication, execution, and retention.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{FileExt as _, OpenOptionsExt as _, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::paths::AppPaths;
use crate::runtime_identity::RuntimeIdentity;

use super::capture::{capture_source_closure, CapturedRegular};
use super::inventory::{embedded_contents, read_regular, validate_embedded_runtime, SYMLINKS};
#[cfg(all(test, target_os = "macos"))]
use super::record::{
    create_execution_stage_record, execution_stage_token, stage_construction_checkpoint_for_test,
};
use super::record::{
    create_execution_stage_record_for_owner, errno_only_error, random_stage_token,
    write_stage_child_claim, BUILD_STAGING_PREFIX, STAGE_STATE_OFFSET, STAGING_PREFIX,
};

pub(super) struct ExecutionStage {
    root: PathBuf,
    contents: File,
    pub(super) owner: File,
    cleanup_on_drop: AtomicBool,
}

impl Drop for ExecutionStage {
    fn drop(&mut self) {
        if self.cleanup_on_drop.load(Ordering::SeqCst) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

#[derive(Clone)]
pub(crate) struct PreparedRuntime(pub(super) Arc<ExecutionStage>);

impl std::fmt::Debug for PreparedRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRuntime")
            .field("server", &self.execution_server())
            .finish()
    }
}

impl PartialEq for PreparedRuntime {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PreparedRuntime {}

impl PreparedRuntime {
    #[cfg(all(test, target_os = "macos"))]
    pub(crate) fn for_test(root: PathBuf) -> Result<Self, String> {
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        let token = execution_stage_token(&root)
            .ok_or_else(|| "test execution stage name is invalid".to_string())?;
        let owner = create_execution_stage_record(&root, token)?;
        let contents_path = root.join("Contents");
        let (contents, _) =
            crate::safe_file::open_directory(&contents_path).map_err(|error| error.to_string())?;
        Ok(Self(Arc::new(ExecutionStage {
            root,
            contents,
            owner,
            cleanup_on_drop: AtomicBool::new(true),
        })))
    }

    pub(crate) fn command(&self) -> Command {
        use std::os::unix::process::CommandExt as _;

        let contents_fd = self.0.contents.as_raw_fd();
        let owner_fd = self.0.owner.as_raw_fd();
        let mut command = Command::new(self.execution_server());
        // SAFETY: `fchdir` is async-signal-safe, the descriptor is retained by
        // this capability through `spawn`, and the closure captures only an fd.
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(contents_fd) == -1 {
                    return Err(errno_only_error());
                }
                write_stage_child_claim(owner_fd)
            });
        }
        command
    }

    pub(crate) fn execution_server(&self) -> PathBuf {
        self.0.root.join("Contents/MacOS/llama-server")
    }

    pub(crate) fn abandon(&self) -> Result<(), String> {
        self.0.cleanup_on_drop.store(false, Ordering::SeqCst);
        self.0
            .owner
            .write_all_at(&[1], STAGE_STATE_OFFSET as u64)
            .and_then(|()| self.0.owner.sync_all())
            .map_err(|_| "prepared runtime abandonment could not be published".to_string())?;
        // SAFETY: this releases only the advisory lock held by this retained
        // stage-record descriptor after the abandoned state is durable.
        if unsafe { libc::flock(self.0.owner.as_raw_fd(), libc::LOCK_UN) } == -1 {
            return Err("prepared runtime abandonment lock could not be released".into());
        }
        Ok(())
    }
}

pub(crate) fn prepare_embedded_runtime(paths: &AppPaths) -> Result<PreparedRuntime, String> {
    prepare_embedded_runtime_with_hooks(paths, || {}, || {})
}

#[cfg(all(test, target_os = "macos"))]
pub(super) fn prepare_embedded_runtime_with_after_inventory_open(
    paths: &AppPaths,
    after_inventory_open: impl FnOnce(),
) -> Result<PreparedRuntime, String> {
    prepare_embedded_runtime_with_hooks(paths, after_inventory_open, || {})
}

#[cfg(all(test, target_os = "macos"))]
pub(super) fn prepare_embedded_runtime_with_after_capture_fence(
    paths: &AppPaths,
    after_capture_fence: impl FnOnce(),
) -> Result<PreparedRuntime, String> {
    prepare_embedded_runtime_with_hooks(paths, || {}, after_capture_fence)
}

fn prepare_embedded_runtime_with_hooks(
    paths: &AppPaths,
    after_inventory_open: impl FnOnce(),
    after_capture_fence: impl FnOnce(),
) -> Result<PreparedRuntime, String> {
    let captured = capture_source_closure(paths, after_inventory_open, after_capture_fence)?;

    let (stage, owner) = create_execution_stage(&paths.run, &captured)?;
    let staged_paths = AppPaths {
        root: paths.root.clone(),
        models: paths.models.clone(),
        config: paths.config.clone(),
        run: paths.run.clone(),
        logs: paths.logs.clone(),
        runtimes: paths.runtimes.clone(),
        managed_server: stage.join("Contents/MacOS/llama-server"),
        runtime_identity: RuntimeIdentity::BundledB10344,
        runtime_inventory: Some(
            stage.join("Contents/Resources/loxa-runtime/b10344/inventory.json"),
        ),
    };
    if let Err(error) = validate_execution_stage_against_capture(&staged_paths, &captured) {
        let _ = fs::remove_dir_all(&stage);
        return Err(error);
    }
    let contents_path = stage.join("Contents");
    let (contents, _) = crate::safe_file::open_directory(&contents_path)
        .map_err(|_| "prepared runtime Contents directory is unavailable".to_string())?;
    Ok(PreparedRuntime(Arc::new(ExecutionStage {
        root: stage,
        contents,
        owner,
        cleanup_on_drop: AtomicBool::new(true),
    })))
}

fn validate_execution_stage_against_capture(
    paths: &AppPaths,
    captured: &[CapturedRegular],
) -> Result<(), String> {
    let contents = embedded_contents(paths)?;
    for regular in captured {
        let destination = contents.join(&regular.relative);
        let bytes = read_regular(&destination, &regular.relative)?;
        if bytes != regular.bytes {
            return Err(format!(
                "prepared runtime file does not match captured authority: {}",
                regular.relative
            ));
        }
        let mode = fs::metadata(&destination)
            .map_err(|_| format!("prepared runtime file is unavailable: {}", regular.relative))?
            .permissions()
            .mode()
            & 0o777;
        if mode != regular.mode & !0o222 {
            return Err(format!(
                "prepared runtime file permissions do not match captured authority: {}",
                regular.relative
            ));
        }
    }
    validate_embedded_runtime(paths)
}

pub(super) fn create_execution_stage(
    run: &Path,
    captured: &[CapturedRegular],
) -> Result<(PathBuf, File), String> {
    use std::os::unix::fs::{symlink, DirBuilderExt as _};

    fs::create_dir_all(run)
        .map_err(|_| "prepared runtime staging directory is unavailable".to_string())?;
    let owner_pid = std::process::id();
    let owner_start = crate::runtime::current_process_start_identity()?;
    let (build, stage, token) = (0..64)
        .find_map(|_| {
            let token = match random_stage_token() {
                Ok(token) => token,
                Err(error) => return Some(Err(error)),
            };
            let candidate = run.join(format!(
                "{BUILD_STAGING_PREFIX}{owner_pid}-{owner_start}-{token}"
            ));
            let stage = run.join(format!("{STAGING_PREFIX}{token}"));
            if stage.exists() {
                return None;
            }
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&candidate) {
                Ok(()) => Some(Ok((candidate, stage, token))),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                Err(_) => Some(Err(
                    "prepared runtime staging directory is unavailable".to_string()
                )),
            }
        })
        .transpose()?
        .ok_or_else(|| "prepared runtime staging directory is unavailable".to_string())?;

    let mut published = false;
    let result = (|| {
        #[cfg(all(test, target_os = "macos"))]
        stage_construction_checkpoint_for_test("after-mkdir", &build);
        for relative in [
            "Contents",
            "Contents/MacOS",
            "Contents/Frameworks",
            "Contents/Resources",
            "Contents/Resources/loxa-runtime",
            "Contents/Resources/loxa-runtime/b10344",
        ] {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(build.join(relative))
                .map_err(|_| "prepared runtime layout could not be created".to_string())?;
        }
        for regular in captured {
            publish_captured_regular(&build.join("Contents"), regular)?;
        }
        for (relative, target) in SYMLINKS {
            symlink(target, build.join("Contents").join(relative))
                .map_err(|_| "prepared runtime symlink could not be created".to_string())?;
        }
        let owner =
            create_execution_stage_record_for_owner(&build, &token, owner_pid, owner_start)?;
        #[cfg(all(test, target_os = "macos"))]
        stage_construction_checkpoint_for_test("after-seal", &build);
        let (build_directory, build_identity) = crate::safe_file::open_directory(&build)
            .map_err(|_| "prepared runtime construction is unavailable".to_string())?;
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &build_directory,
            &build_identity,
            &build,
        )
        .map_err(|_| "prepared runtime construction changed before publication".to_string())?;
        build_directory
            .sync_all()
            .map_err(|_| "prepared runtime construction could not be synchronized".to_string())?;
        let (run_directory, run_identity) = crate::safe_file::open_directory(run)
            .map_err(|_| "prepared runtime staging directory is unavailable".to_string())?;
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &run_directory,
            &run_identity,
            run,
        )
        .map_err(|_| "prepared runtime staging directory changed".to_string())?;
        use rustix::fs::{renameat_with, RenameFlags};
        renameat_with(
            &run_directory,
            build.file_name().expect("construction has a file name"),
            &run_directory,
            stage.file_name().expect("execution stage has a file name"),
            RenameFlags::NOREPLACE,
        )
        .map_err(|_| "prepared runtime construction could not be published".to_string())?;
        published = true;
        run_directory.sync_all().map_err(|_| {
            "prepared runtime staging directory could not be synchronized".to_string()
        })?;
        Ok(owner)
    })();
    match result {
        Ok(owner) => Ok((stage, owner)),
        Err(error) => {
            let _ = fs::remove_dir_all(&build);
            if published {
                let _ = fs::remove_dir_all(&stage);
            }
            Err(error)
        }
    }
}

fn publish_captured_regular(contents: &Path, regular: &CapturedRegular) -> Result<(), String> {
    publish_regular_bytes(contents, &regular.relative, regular.mode, &regular.bytes)
}

pub(super) fn publish_regular_bytes(
    contents: &Path,
    relative: &str,
    source_mode: u32,
    bytes: &[u8],
) -> Result<(), String> {
    let destination = contents.join(relative);
    let destination_mode = source_mode & !0o222;

    let mut options = OpenOptions::new();
    options.create_new(true).write(true).mode(destination_mode);
    let mut destination_file = options
        .open(&destination)
        .map_err(|_| "prepared runtime file could not be created".to_string())?;
    destination_file
        .write_all(bytes)
        .and_then(|()| {
            destination_file.set_permissions(fs::Permissions::from_mode(destination_mode))
        })
        .and_then(|()| destination_file.sync_all())
        .map_err(|_| "prepared runtime file could not be copied".to_string())
}
