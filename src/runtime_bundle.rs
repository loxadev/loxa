use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{FileExt as _, MetadataExt, OpenOptionsExt as _, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::paths::AppPaths;
use crate::runtime_identity::RuntimeIdentity;

const INVENTORY_SCHEMA: u32 = 1;
const BUILD: &str = "b10344";
const COMMIT: &str = "7a20b417f4526cae073bd997af5020cea3e7ccbe";
const VERSION_LINE: &str = "version: 10344 (7a20b417f)";
const ARCHITECTURE: &str = "arm64";
const MINIMUM_MACOS: &str = "13.3";
const MAXIMUM_MINOS: u32 = (13 << 16) | (3 << 8);
const LICENSE_SHA256: &str = "94f29bbed6a22c35b992c5c6ebf0e7c92f13b836b90f36f461c9cf2f0f1d010d";
const PROVENANCE_SHA256: &str = "402029fbca52d7835acea49f515e83c3213e234bfbc78a474f7a0435afc95721";
const RELOCATION: &str = "install_name_tool -add_rpath @executable_path/../Frameworks llama-server";
const STAGING_PREFIX: &str = ".bundled-runtime-exec-";
const BUILD_STAGING_PREFIX: &str = ".bundled-runtime-build-";
const STAGE_RECORD_NAME: &str = ".loxa-execution-stage-v1";
const STAGE_RECORD_MAGIC: &[u8; 16] = b"LOXA-STAGE-v1\0\0\0";
const STAGE_RECORD_LEN: usize = 80;
const STAGE_OWNER_PID_OFFSET: usize = 16;
const STAGE_OWNER_START_OFFSET: usize = 20;
const STAGE_CHILD_PID_OFFSET: usize = 28;
const STAGE_CHILD_GROUP_OFFSET: usize = 32;
const STAGE_TOKEN_OFFSET: usize = 36;
const STAGE_TOKEN_LEN: usize = 32;
pub(crate) const STAGE_STATE_OFFSET: usize = STAGE_TOKEN_OFFSET + STAGE_TOKEN_LEN;
type StageRecordFields = (u32, u64, Option<(u32, i32)>, bool);

const MACH_O_FILES: &[&str] = &[
    "MacOS/llama-server",
    "Frameworks/libllama-server-impl.dylib",
    "Frameworks/libllama-common.0.0.10344.dylib",
    "Frameworks/libmtmd.0.0.10344.dylib",
    "Frameworks/libllama.0.0.10344.dylib",
    "Frameworks/libggml.0.19.0.dylib",
    "Frameworks/libggml-cpu.0.19.0.dylib",
    "Frameworks/libggml-blas.0.19.0.dylib",
    "Frameworks/libggml-metal.0.19.0.dylib",
    "Frameworks/libggml-rpc.0.19.0.dylib",
    "Frameworks/libggml-base.0.19.0.dylib",
];

const DATA_FILES: &[&str] = &[
    "Resources/loxa-runtime/b10344/LICENSE",
    "Resources/loxa-runtime/b10344/upstream-provenance.json",
    "Resources/loxa-runtime/b10344/normalized-inventory.json",
];

const SYMLINKS: &[(&str, &str)] = &[
    (
        "Frameworks/libllama-common.0.dylib",
        "libllama-common.0.0.10344.dylib",
    ),
    ("Frameworks/libmtmd.0.dylib", "libmtmd.0.0.10344.dylib"),
    ("Frameworks/libllama.0.dylib", "libllama.0.0.10344.dylib"),
    ("Frameworks/libggml.0.dylib", "libggml.0.19.0.dylib"),
    ("Frameworks/libggml-cpu.0.dylib", "libggml-cpu.0.19.0.dylib"),
    (
        "Frameworks/libggml-blas.0.dylib",
        "libggml-blas.0.19.0.dylib",
    ),
    (
        "Frameworks/libggml-metal.0.dylib",
        "libggml-metal.0.19.0.dylib",
    ),
    ("Frameworks/libggml-rpc.0.dylib", "libggml-rpc.0.19.0.dylib"),
    (
        "Frameworks/libggml-base.0.dylib",
        "libggml-base.0.19.0.dylib",
    ),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    schema_version: u32,
    runtime: InventoryRuntime,
    regular_files: Vec<InventoryRegular>,
    symlinks: Vec<InventorySymlink>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryRuntime {
    build: String,
    commit: String,
    version_line: String,
    architecture: String,
    minimum_macos: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryRegular {
    path: String,
    size: u64,
    sha256: String,
    mach_o: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventorySymlink {
    path: String,
    target: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizedInventory {
    schema_version: u32,
    relocation: String,
    regular_files: Vec<InventoryRegular>,
}

struct MachOInfo {
    minimum_macos: u32,
    rpaths: Vec<String>,
    dependencies: Vec<String>,
    install_name: Option<String>,
}

struct CapturedRegular {
    relative: String,
    file: File,
    identity: crate::safe_file::RegularFileIdentity,
    source_path: PathBuf,
    bytes: Vec<u8>,
    mode: u32,
}

struct SourceDirectory {
    file: File,
    identity: crate::safe_file::DirectoryIdentity,
    path: PathBuf,
}

struct SourceCapabilities {
    contents: SourceDirectory,
    macos: SourceDirectory,
    frameworks: SourceDirectory,
    resources: SourceDirectory,
    runtime_parent: SourceDirectory,
    runtime: SourceDirectory,
}

struct ExecutionStage {
    root: PathBuf,
    contents: File,
    owner: File,
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
pub(crate) struct PreparedRuntime(Arc<ExecutionStage>);

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

fn execution_stage_root<'a>(run: &Path, execution_server: &'a Path) -> Option<&'a Path> {
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

fn execution_stage_token(stage: &Path) -> Option<&str> {
    let token = stage.file_name()?.to_str()?.strip_prefix(STAGING_PREFIX)?;
    (token.len() == STAGE_TOKEN_LEN
        && token
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)))
    .then_some(token)
}

fn execution_build_owner(build: &Path) -> Option<(u32, u64)> {
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

fn read_stage_record(record: &File) -> Result<[u8; STAGE_RECORD_LEN], String> {
    let mut bytes = [0_u8; STAGE_RECORD_LEN];
    record
        .read_exact_at(&mut bytes, 0)
        .map_err(|_| "prepared runtime recovery record is unreadable".to_string())?;
    Ok(bytes)
}

fn parse_stage_record(
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

pub(crate) fn prepare_embedded_runtime(paths: &AppPaths) -> Result<PreparedRuntime, String> {
    prepare_embedded_runtime_with_hooks(paths, || {}, || {})
}

#[cfg(all(test, target_os = "macos"))]
fn prepare_embedded_runtime_with_after_inventory_open(
    paths: &AppPaths,
    after_inventory_open: impl FnOnce(),
) -> Result<PreparedRuntime, String> {
    prepare_embedded_runtime_with_hooks(paths, after_inventory_open, || {})
}

#[cfg(all(test, target_os = "macos"))]
fn prepare_embedded_runtime_with_after_capture_fence(
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

pub fn validate_embedded_runtime(paths: &AppPaths) -> Result<(), String> {
    let contents = embedded_contents(paths)?;
    let inventory_path = contents.join("Resources/loxa-runtime/b10344/inventory.json");

    let inventory_bytes = read_regular(&inventory_path, "inventory")?;
    if inventory_bytes.len() > 128 * 1024 {
        return Err("embedded runtime inventory is too large".into());
    }
    let inventory: Inventory = serde_json::from_slice(&inventory_bytes)
        .map_err(|_| "embedded runtime inventory is invalid".to_string())?;
    validate_inventory_identity(&inventory)?;
    let regular = validate_inventory_entries(&inventory)?;
    let symlinks = validate_inventory_symlinks(&inventory)?;

    let framework_names = MACH_O_FILES[1..]
        .iter()
        .chain(SYMLINKS.iter().map(|(path, _)| path))
        .map(|path| {
            Path::new(path)
                .file_name()
                .expect("fixed framework filename")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<BTreeSet<_>>();
    for (relative, entry) in regular {
        let path = contents.join(relative);
        let bytes = read_regular(&path, relative)?;
        if bytes.len() as u64 != entry.size {
            return Err(format!("embedded runtime size mismatch for {relative}"));
        }
        if digest(&bytes) != entry.sha256 {
            return Err(format!("embedded runtime SHA-256 mismatch for {relative}"));
        }
        let executable = fs::symlink_metadata(&path)
            .map_err(|_| format!("embedded runtime file is missing: {relative}"))?
            .permissions()
            .mode()
            & 0o111
            != 0;
        if entry.mach_o != MACH_O_FILES.contains(&relative) {
            return Err(format!(
                "embedded runtime file kind is invalid for {relative}"
            ));
        }
        if entry.mach_o {
            if !executable {
                return Err(format!(
                    "embedded runtime Mach-O is not executable: {relative}"
                ));
            }
            let info = parse_mach_o(&bytes, relative)?;
            validate_mach_o(relative, &info, &framework_names)?;
        } else if executable {
            return Err(format!(
                "embedded runtime data file is executable: {relative}"
            ));
        }
    }

    for (relative, expected_target) in symlinks {
        let path = contents.join(relative);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| format!("embedded runtime symlink is missing: {relative}"))?;
        if !metadata.file_type().is_symlink() {
            return Err(format!("embedded runtime symlink is unsafe: {relative}"));
        }
        let target = fs::read_link(&path)
            .map_err(|_| format!("embedded runtime symlink is unreadable: {relative}"))?;
        if target != Path::new(expected_target)
            || target.is_absolute()
            || target.components().count() != 1
        {
            return Err(format!(
                "embedded runtime symlink target is unsafe: {relative}"
            ));
        }
    }

    validate_exact_directory(
        &contents.join("Frameworks"),
        MACH_O_FILES[1..]
            .iter()
            .map(|path| Path::new(path).file_name().expect("fixed filename"))
            .chain(
                SYMLINKS
                    .iter()
                    .map(|(path, _)| Path::new(path).file_name().expect("fixed filename")),
            ),
        "Frameworks",
    )?;
    validate_exact_directory(
        &contents.join("Resources/loxa-runtime/b10344"),
        [
            "LICENSE",
            "upstream-provenance.json",
            "normalized-inventory.json",
            "inventory.json",
        ]
        .iter()
        .map(Path::new),
        "runtime resources",
    )?;

    let license = contents.join("Resources/loxa-runtime/b10344/LICENSE");
    if digest(&read_regular(&license, "LICENSE")?) != LICENSE_SHA256 {
        return Err("embedded runtime LICENSE does not match upstream".into());
    }
    let provenance = contents.join("Resources/loxa-runtime/b10344/upstream-provenance.json");
    if digest(&read_regular(&provenance, "provenance")?) != PROVENANCE_SHA256 {
        return Err("embedded runtime provenance does not match the frozen release".into());
    }
    validate_normalized_inventory(contents)?;
    Ok(())
}

fn embedded_contents(paths: &AppPaths) -> Result<&Path, String> {
    if paths.runtime_identity != RuntimeIdentity::BundledB10344 {
        return Err("embedded runtime validation requires bundled b10344 mode".into());
    }
    let inventory_path = paths
        .runtime_inventory
        .as_deref()
        .ok_or("bundled runtime inventory path is missing")?;
    let contents = paths
        .managed_server
        .parent()
        .and_then(Path::parent)
        .ok_or("bundled runtime path is not inside Contents/MacOS")?;
    let expected_inventory = contents.join("Resources/loxa-runtime/b10344/inventory.json");
    if inventory_path != expected_inventory
        || paths.managed_server != contents.join("MacOS/llama-server")
    {
        return Err("bundled runtime layout does not match the application executable".into());
    }
    Ok(contents)
}

fn capture_source_closure(
    paths: &AppPaths,
    after_inventory_open: impl FnOnce(),
    after_capture_fence: impl FnOnce(),
) -> Result<Vec<CapturedRegular>, String> {
    let source = open_source_capabilities(paths)?;
    let mut captured = vec![capture_regular_at(
        &source.runtime,
        std::ffi::OsStr::new("inventory.json"),
        "Resources/loxa-runtime/b10344/inventory.json",
        after_inventory_open,
    )?];
    let inventory_bytes = &captured[0].bytes;
    if inventory_bytes.len() > 128 * 1024 {
        return Err("embedded runtime inventory is too large".into());
    }
    let inventory: Inventory = serde_json::from_slice(inventory_bytes)
        .map_err(|_| "embedded runtime inventory is invalid".to_string())?;
    validate_inventory_identity(&inventory)?;
    let regular = validate_inventory_entries(&inventory)?;
    let symlinks = validate_inventory_symlinks(&inventory)?;
    let framework_names = MACH_O_FILES[1..]
        .iter()
        .chain(SYMLINKS.iter().map(|(path, _)| path))
        .map(|path| {
            Path::new(path)
                .file_name()
                .expect("fixed framework filename")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<BTreeSet<_>>();

    for (relative, entry) in regular {
        let (directory, name) = source_file_location(&source, relative)?;
        let file = capture_regular_at(directory, name, relative, || {})?;
        if file.bytes.len() as u64 != entry.size {
            return Err(format!("embedded runtime size mismatch for {relative}"));
        }
        if digest(&file.bytes) != entry.sha256 {
            return Err(format!("embedded runtime SHA-256 mismatch for {relative}"));
        }
        let executable = file.mode & 0o111 != 0;
        if entry.mach_o != MACH_O_FILES.contains(&relative) {
            return Err(format!(
                "embedded runtime file kind is invalid for {relative}"
            ));
        }
        if entry.mach_o {
            if !executable {
                return Err(format!(
                    "embedded runtime Mach-O is not executable: {relative}"
                ));
            }
            let info = parse_mach_o(&file.bytes, relative)?;
            validate_mach_o(relative, &info, &framework_names)?;
        } else if executable {
            return Err(format!(
                "embedded runtime data file is executable: {relative}"
            ));
        }
        captured.push(file);
    }

    for (relative, expected_target) in symlinks {
        let name = Path::new(relative)
            .file_name()
            .expect("validated framework symlink path");
        let target = rustix::fs::readlinkat(&source.frameworks.file, name, Vec::new())
            .map_err(|_| format!("embedded runtime symlink is unreadable: {relative}"))?;
        if target.as_bytes() != expected_target.as_bytes() {
            return Err(format!(
                "embedded runtime symlink target is unsafe: {relative}"
            ));
        }
    }

    validate_exact_directory_descriptor(
        &source.frameworks.file,
        MACH_O_FILES[1..]
            .iter()
            .map(|path| Path::new(path).file_name().expect("fixed filename"))
            .chain(
                SYMLINKS
                    .iter()
                    .map(|(path, _)| Path::new(path).file_name().expect("fixed filename")),
            ),
        "Frameworks",
    )?;
    validate_exact_directory_descriptor(
        &source.runtime.file,
        [
            "LICENSE",
            "upstream-provenance.json",
            "normalized-inventory.json",
            "inventory.json",
        ],
        "runtime resources",
    )?;

    let captured_bytes = |relative: &str| {
        captured
            .iter()
            .find(|file| file.relative == relative)
            .map(|file| file.bytes.as_slice())
            .expect("inventory requires every captured regular")
    };
    if digest(captured_bytes("Resources/loxa-runtime/b10344/LICENSE")) != LICENSE_SHA256 {
        return Err("embedded runtime LICENSE does not match upstream".into());
    }
    if digest(captured_bytes(
        "Resources/loxa-runtime/b10344/upstream-provenance.json",
    )) != PROVENANCE_SHA256
    {
        return Err("embedded runtime provenance does not match the frozen release".into());
    }
    validate_normalized_inventory_bytes(captured_bytes(
        "Resources/loxa-runtime/b10344/normalized-inventory.json",
    ))?;
    fence_source_capabilities(&source, &captured)?;
    after_capture_fence();
    Ok(captured)
}

fn open_source_capabilities(paths: &AppPaths) -> Result<SourceCapabilities, String> {
    let contents_path = embedded_contents(paths)?.to_path_buf();
    let (contents_file, contents_identity) = crate::safe_file::open_directory(&contents_path)
        .map_err(|_| "embedded runtime Contents directory is unsafe".to_string())?;
    let contents = SourceDirectory {
        file: contents_file,
        identity: contents_identity,
        path: contents_path,
    };
    let macos = open_directory_at(&contents, "MacOS", "MacOS")?;
    let frameworks = open_directory_at(&contents, "Frameworks", "Frameworks")?;
    let resources = open_directory_at(&contents, "Resources", "Resources")?;
    let runtime_parent = open_directory_at(&resources, "loxa-runtime", "runtime resources")?;
    let runtime = open_directory_at(&runtime_parent, "b10344", "runtime resources")?;
    Ok(SourceCapabilities {
        contents,
        macos,
        frameworks,
        resources,
        runtime_parent,
        runtime,
    })
}

fn open_directory_at(
    parent: &SourceDirectory,
    name: &str,
    label: &str,
) -> Result<SourceDirectory, String> {
    let descriptor = rustix::fs::openat(
        &parent.file,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| format!("embedded runtime {label} directory is unsafe"))?;
    let file = File::from(descriptor);
    let path = parent.path.join(name);
    let identity = crate::safe_file::directory_identity(&file, &path)
        .map_err(|_| format!("embedded runtime {label} directory is unsafe"))?;
    Ok(SourceDirectory {
        file,
        identity,
        path,
    })
}

fn source_file_location<'a>(
    source: &'a SourceCapabilities,
    relative: &'a str,
) -> Result<(&'a SourceDirectory, &'a std::ffi::OsStr), String> {
    let path = Path::new(relative);
    let name = path
        .file_name()
        .ok_or_else(|| "embedded runtime inventory contains an unsafe path".to_string())?;
    if path.parent() == Some(Path::new("MacOS")) {
        Ok((&source.macos, name))
    } else if path.parent() == Some(Path::new("Frameworks")) {
        Ok((&source.frameworks, name))
    } else if path.parent() == Some(Path::new("Resources/loxa-runtime/b10344")) {
        Ok((&source.runtime, name))
    } else {
        Err("embedded runtime inventory contains an unsafe path".into())
    }
}

fn capture_regular_at(
    directory: &SourceDirectory,
    name: &std::ffi::OsStr,
    relative: &str,
    after_open: impl FnOnce(),
) -> Result<CapturedRegular, String> {
    let descriptor = rustix::fs::openat(
        &directory.file,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| format!("embedded runtime file is unsafe: {relative}"))?;
    let mut file = File::from(descriptor);
    let source_path = directory.path.join(name);
    let identity = crate::safe_file::regular_file_identity(&file, &source_path)
        .map_err(|_| format!("embedded runtime file is unsafe: {relative}"))?;
    let metadata = file
        .metadata()
        .map_err(|_| format!("embedded runtime file is unreadable: {relative}"))?;
    after_open();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| format!("embedded runtime file changed while reading: {relative}"))?;
    if crate::safe_file::regular_file_identity(&file, &source_path)
        .map_err(|_| format!("embedded runtime file changed while reading: {relative}"))?
        != identity
    {
        return Err(format!(
            "embedded runtime file changed while reading: {relative}"
        ));
    }
    Ok(CapturedRegular {
        relative: relative.to_owned(),
        file,
        identity,
        source_path,
        bytes,
        mode: metadata.permissions().mode() & 0o777,
    })
}

fn validate_exact_directory_descriptor<I, S>(
    directory: &File,
    expected: I,
    label: &str,
) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    use std::os::unix::ffi::OsStringExt as _;

    let expected = expected
        .into_iter()
        .map(|entry| PathBuf::from(entry.as_ref()))
        .collect::<BTreeSet<_>>();
    let mut stream = rustix::fs::Dir::read_from(directory)
        .map_err(|_| format!("embedded runtime {label} directory is unreadable"))?;
    let mut entries = BTreeSet::new();
    while let Some(entry) = stream.read() {
        let entry =
            entry.map_err(|_| format!("embedded runtime {label} directory is unreadable"))?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            entries.insert(PathBuf::from(std::ffi::OsString::from_vec(name.to_vec())));
        }
    }
    if entries != expected {
        return Err(format!(
            "embedded runtime {label} directory contains a missing or unexpected entry"
        ));
    }
    Ok(())
}

fn fence_source_capabilities(
    source: &SourceCapabilities,
    captured: &[CapturedRegular],
) -> Result<(), String> {
    for directory in [
        &source.contents,
        &source.macos,
        &source.frameworks,
        &source.resources,
        &source.runtime_parent,
        &source.runtime,
    ] {
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &directory.file,
            &directory.identity,
            &directory.path,
        )
        .map_err(|_| "embedded runtime source changed while capturing".to_string())?;
    }
    for regular in captured {
        crate::safe_file::ensure_descriptor_matches_path(
            &regular.file,
            &regular.identity,
            &regular.source_path,
        )
        .map_err(|_| "embedded runtime source changed while capturing".to_string())?;
    }
    Ok(())
}

fn create_execution_stage(
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

#[cfg(all(test, target_os = "macos"))]
fn create_execution_stage_record(stage: &Path, token: &str) -> Result<File, String> {
    let owner_pid = std::process::id();
    let owner_start = crate::runtime::current_process_start_identity()?;
    create_execution_stage_record_for_owner(stage, token, owner_pid, owner_start)
}

fn create_execution_stage_record_for_owner(
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
fn stage_construction_checkpoint_for_test(phase: &str, stage: &Path) {
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

fn write_stage_child_claim(record_fd: libc::c_int) -> std::io::Result<()> {
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

fn errno_only_error() -> std::io::Error {
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

fn random_stage_token() -> Result<String, String> {
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

fn publish_captured_regular(contents: &Path, regular: &CapturedRegular) -> Result<(), String> {
    publish_regular_bytes(contents, &regular.relative, regular.mode, &regular.bytes)
}

fn publish_regular_bytes(
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

fn validate_inventory_identity(inventory: &Inventory) -> Result<(), String> {
    let runtime = &inventory.runtime;
    if inventory.schema_version != INVENTORY_SCHEMA
        || runtime.build != BUILD
        || runtime.commit != COMMIT
        || runtime.version_line != VERSION_LINE
        || runtime.architecture != ARCHITECTURE
        || runtime.minimum_macos != MINIMUM_MACOS
    {
        return Err("embedded runtime inventory has the wrong version or identity".into());
    }
    Ok(())
}

fn validate_inventory_entries(
    inventory: &Inventory,
) -> Result<BTreeMap<&str, &InventoryRegular>, String> {
    let expected = MACH_O_FILES
        .iter()
        .chain(DATA_FILES)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();
    for entry in &inventory.regular_files {
        validate_relative_path(&entry.path)?;
        validate_sha256(&entry.sha256)?;
        if entry.size == 0 || entries.insert(entry.path.as_str(), entry).is_some() {
            return Err("embedded runtime inventory contains an invalid or duplicate file".into());
        }
    }
    if entries.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err("embedded runtime inventory has a missing or unexpected regular file".into());
    }
    Ok(entries)
}

fn validate_inventory_symlinks(inventory: &Inventory) -> Result<BTreeMap<&str, &str>, String> {
    let expected = SYMLINKS.iter().copied().collect::<BTreeMap<_, _>>();
    let mut entries = BTreeMap::new();
    for entry in &inventory.symlinks {
        validate_relative_path(&entry.path)?;
        if Path::new(&entry.target).components().count() != 1
            || entries
                .insert(entry.path.as_str(), entry.target.as_str())
                .is_some()
        {
            return Err("embedded runtime inventory contains an unsafe symlink".into());
        }
    }
    if entries != expected {
        return Err("embedded runtime inventory has a missing or unexpected symlink".into());
    }
    Ok(entries)
}

fn validate_normalized_inventory(contents: &Path) -> Result<(), String> {
    let path = contents.join("Resources/loxa-runtime/b10344/normalized-inventory.json");
    let bytes = read_regular(&path, "normalized inventory")?;
    validate_normalized_inventory_bytes(&bytes)
}

fn validate_normalized_inventory_bytes(bytes: &[u8]) -> Result<(), String> {
    let inventory: NormalizedInventory = serde_json::from_slice(bytes)
        .map_err(|_| "embedded normalized inventory is invalid".to_string())?;
    if inventory.schema_version != INVENTORY_SCHEMA || inventory.relocation != RELOCATION {
        return Err("embedded normalized inventory has invalid packaging metadata".into());
    }
    let mut paths = BTreeSet::new();
    for entry in &inventory.regular_files {
        validate_relative_path(&entry.path)?;
        validate_sha256(&entry.sha256)?;
        if !entry.mach_o || entry.size == 0 || !paths.insert(entry.path.as_str()) {
            return Err("embedded normalized inventory has an invalid code entry".into());
        }
    }
    if paths != MACH_O_FILES.iter().copied().collect() {
        return Err("embedded normalized inventory has a missing or unexpected code file".into());
    }
    Ok(())
}

fn validate_exact_directory<I, S>(directory: &Path, expected: I, label: &str) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let expected = expected
        .into_iter()
        .map(|entry| PathBuf::from(entry.as_ref()))
        .collect::<BTreeSet<PathBuf>>();
    let entries = fs::read_dir(directory)
        .map_err(|_| format!("embedded runtime {label} directory is missing"))?
        .map(|entry| {
            entry
                .map(|entry| PathBuf::from(entry.file_name()))
                .map_err(|_| format!("embedded runtime {label} directory is unreadable"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if entries != expected {
        return Err(format!(
            "embedded runtime {label} directory contains a missing or unexpected entry"
        ));
    }
    Ok(())
}

fn read_regular(path: &Path, label: &str) -> Result<Vec<u8>, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| format!("embedded runtime {label} is missing"))?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(format!(
            "embedded runtime {label} is not a safe regular file"
        ));
    }
    crate::safe_file::read_regular_file(path)
        .map_err(|_| format!("embedded runtime {label} changed while reading"))
}

fn validate_relative_path(path: &str) -> Result<(), String> {
    let parsed = Path::new(path);
    if path.is_empty()
        || parsed.is_absolute()
        || parsed.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
        || path.contains('\\')
    {
        return Err("embedded runtime inventory contains an unsafe path".into());
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("embedded runtime inventory contains an invalid SHA-256".into());
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn parse_mach_o(bytes: &[u8], relative: &str) -> Result<MachOInfo, String> {
    if bytes.len() < 32
        || u32::from_le_bytes(bytes[0..4].try_into().expect("four-byte slice")) != 0xfeedfacf
        || u32::from_le_bytes(bytes[4..8].try_into().expect("four-byte slice")) != 0x0100000c
    {
        return Err(format!(
            "embedded runtime Mach-O must be thin arm64: {relative}"
        ));
    }
    let command_count = read_u32(bytes, 16, relative)? as usize;
    let command_bytes = read_u32(bytes, 20, relative)? as usize;
    let command_end = 32_usize
        .checked_add(command_bytes)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| format!("embedded runtime Mach-O load commands are invalid: {relative}"))?;
    let mut offset = 32_usize;
    let mut minimum_macos = None;
    let mut rpaths = Vec::new();
    let mut dependencies = Vec::new();
    let mut install_name = None;
    for _ in 0..command_count {
        let kind = read_u32(bytes, offset, relative)?;
        let size = read_u32(bytes, offset + 4, relative)? as usize;
        let next = offset
            .checked_add(size)
            .filter(|next| size >= 8 && *next <= command_end)
            .ok_or_else(|| {
                format!("embedded runtime Mach-O load command is invalid: {relative}")
            })?;
        match kind {
            0x32 => {
                if read_u32(bytes, offset + 8, relative)? != 1 || minimum_macos.is_some() {
                    return Err(format!(
                        "embedded runtime Mach-O platform metadata is invalid: {relative}"
                    ));
                }
                minimum_macos = Some(read_u32(bytes, offset + 12, relative)?);
            }
            0x8000001c => rpaths.push(read_load_string(bytes, offset, next, relative)?),
            0x0c | 0x80000018 | 0x8000001f | 0x80000023 => {
                dependencies.push(read_load_string(bytes, offset, next, relative)?)
            }
            0x0d => install_name = Some(read_load_string(bytes, offset, next, relative)?),
            _ => {}
        }
        offset = next;
    }
    if offset != command_end {
        return Err(format!(
            "embedded runtime Mach-O load command size is invalid: {relative}"
        ));
    }
    Ok(MachOInfo {
        minimum_macos: minimum_macos.ok_or_else(|| {
            format!("embedded runtime Mach-O is missing LC_BUILD_VERSION: {relative}")
        })?,
        rpaths,
        dependencies,
        install_name,
    })
}

fn validate_mach_o(
    relative: &str,
    info: &MachOInfo,
    framework_names: &BTreeSet<String>,
) -> Result<(), String> {
    if info.minimum_macos > MAXIMUM_MINOS {
        return Err(format!(
            "embedded runtime minimum macOS exceeds {MINIMUM_MACOS}: {relative}"
        ));
    }
    if relative == "MacOS/llama-server" {
        if !info
            .rpaths
            .iter()
            .any(|path| path == "@executable_path/../Frameworks")
        {
            return Err("embedded llama-server is missing the Frameworks rpath".into());
        }
        if info.install_name.is_some() {
            return Err("embedded llama-server has an unexpected install name".into());
        }
    } else {
        if !info.rpaths.iter().any(|path| path == "@loader_path") {
            return Err(format!(
                "embedded runtime dylib has no loader rpath: {relative}"
            ));
        }
        let install_name = info
            .install_name
            .as_deref()
            .ok_or_else(|| format!("embedded runtime dylib has no install name: {relative}"))?;
        let Some(name) = install_name.strip_prefix("@rpath/") else {
            return Err(format!(
                "embedded runtime dylib install name is unsafe: {relative}"
            ));
        };
        if !framework_names.contains(name) {
            return Err(format!(
                "embedded runtime dylib install name is unresolved: {relative}"
            ));
        }
    }
    for dependency in &info.dependencies {
        if dependency.starts_with("/System/Library/") || dependency.starts_with("/usr/lib/") {
            continue;
        }
        let Some(name) = dependency.strip_prefix("@rpath/") else {
            return Err(format!("embedded runtime dependency is unsafe: {relative}"));
        };
        if !framework_names.contains(name) {
            return Err(format!(
                "embedded runtime dependency is unresolved: {relative}"
            ));
        }
    }
    Ok(())
}

fn read_load_string(
    bytes: &[u8],
    command: usize,
    command_end: usize,
    relative: &str,
) -> Result<String, String> {
    let string_offset = read_u32(bytes, command + 8, relative)? as usize;
    let start = command
        .checked_add(string_offset)
        .filter(|start| *start < command_end)
        .ok_or_else(|| format!("embedded runtime Mach-O string is invalid: {relative}"))?;
    let end = bytes[start..command_end]
        .iter()
        .position(|byte| *byte == 0)
        .map(|length| start + length)
        .ok_or_else(|| format!("embedded runtime Mach-O string is unterminated: {relative}"))?;
    std::str::from_utf8(&bytes[start..end])
        .map(str::to_owned)
        .map_err(|_| format!("embedded runtime Mach-O string is invalid UTF-8: {relative}"))
}

fn read_u32(bytes: &[u8], offset: usize, relative: &str) -> Result<u32, String> {
    let end = offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| format!("embedded runtime Mach-O is truncated: {relative}"))?;
    Ok(u32::from_le_bytes(
        bytes[offset..end].try_into().expect("four-byte slice"),
    ))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn restrictive_umask_publish_child() {
        let Some(contents) = std::env::var_os("LOXA_TEST_RESTRICTIVE_UMASK_CONTENTS") else {
            return;
        };
        // SAFETY: this exact-filter subprocess runs only this test, so changing
        // its process umask cannot race another test or escape the child.
        unsafe { libc::umask(0o077) };
        let contents = PathBuf::from(contents);
        fs::create_dir_all(&contents).unwrap();
        publish_regular_bytes(&contents, "read-only", 0o444, b"captured").unwrap();
        assert_eq!(
            fs::metadata(contents.join("read-only"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
    }

    #[test]
    fn captured_read_only_mode_survives_a_restrictive_service_umask() {
        let root = tempfile::tempdir().unwrap();
        let contents = root.path().join("Contents");
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime_bundle::tests::restrictive_umask_publish_child",
                "--nocapture",
            ])
            .env("LOXA_TEST_RESTRICTIVE_UMASK_CONTENTS", &contents)
            .output()
            .unwrap();

        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            fs::metadata(contents.join("read-only"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
    }

    #[test]
    fn interrupted_stage_constructor_child() {
        let Some(run) = std::env::var_os("LOXA_TEST_STAGE_CONSTRUCTION_RUN") else {
            return;
        };
        let result = create_execution_stage(Path::new(&run), &[]);
        panic!("stage construction checkpoint did not terminate the child: {result:?}");
    }

    #[test]
    fn interrupted_construction_is_never_published_and_next_acquire_recovers_it() {
        use std::os::unix::process::ExitStatusExt as _;

        let root = tempfile::tempdir().unwrap();
        let run = root.path().join("run");
        fs::create_dir_all(&run).unwrap();
        let lookalike = run.join(".bundled-runtime-build-2-3-0123456789abcdef0123456789abcdeg");
        fs::create_dir(&lookalike).unwrap();

        for phase in ["after-mkdir", "mid-record", "after-seal"] {
            let ready = root.path().join(format!("{phase}.ready"));
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_bundle::tests::interrupted_stage_constructor_child",
                    "--nocapture",
                ])
                .env("LOXA_TEST_STAGE_CONSTRUCTION_RUN", &run)
                .env("LOXA_TEST_STAGE_CONSTRUCTION_KILL_PHASE", phase)
                .env("LOXA_TEST_STAGE_CONSTRUCTION_READY", &ready)
                .output()
                .unwrap();
            assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
            let name = String::from_utf8(fs::read(&ready).unwrap()).unwrap();
            let interrupted = run.join(&name);
            assert!(interrupted.is_dir(), "{phase} checkpoint was not retained");

            drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());

            assert!(
                name.starts_with(BUILD_STAGING_PREFIX),
                "{phase} exposed an executable stage before construction committed: {name}"
            );
            assert!(
                !interrupted.exists(),
                "{phase} interrupted construction was not recovered"
            );
            assert!(lookalike.is_dir(), "{phase} recovery removed a lookalike");
        }
    }

    #[test]
    fn live_constructor_is_preserved_until_its_exact_owner_dies() {
        let root = tempfile::tempdir().unwrap();
        let run = root.path().join("run");
        fs::create_dir_all(&run).unwrap();
        let ready = root.path().join("live.ready");
        let lookalike = run.join(".bundled-runtime-build-2-3-0123456789abcdef0123456789abcdeg");
        fs::create_dir(&lookalike).unwrap();
        let mut constructor = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime_bundle::tests::interrupted_stage_constructor_child",
                "--nocapture",
            ])
            .env("LOXA_TEST_STAGE_CONSTRUCTION_RUN", &run)
            .env("LOXA_TEST_STAGE_CONSTRUCTION_PAUSE_PHASE", "after-mkdir")
            .env("LOXA_TEST_STAGE_CONSTRUCTION_READY", &ready)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.is_file() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            ready.is_file(),
            "live constructor did not publish its checkpoint"
        );
        let name = String::from_utf8(fs::read(&ready).unwrap()).unwrap();
        let build = run.join(&name);

        drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
        assert!(build.is_dir(), "recovery removed a live exact constructor");
        assert!(
            lookalike.is_dir(),
            "recovery removed a construction lookalike"
        );

        constructor.kill().unwrap();
        let _ = constructor.wait().unwrap();
        drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
        assert!(
            !build.exists(),
            "recovery retained a dead exact constructor"
        );
        assert!(
            lookalike.is_dir(),
            "dead-owner recovery removed a lookalike"
        );
    }

    #[test]
    fn pre_exec_group_refusal_is_prompt_errno_only_and_cleans_the_stage() {
        let root = tempfile::tempdir().unwrap();
        let run = root.path().join("run");
        let stage = run.join(".bundled-runtime-exec-44444444444444444444444444444444");
        let server = stage.join("Contents/MacOS/llama-server");
        fs::create_dir_all(server.parent().unwrap()).unwrap();
        fs::copy("/usr/bin/true", &server).unwrap();
        fs::set_permissions(&server, fs::Permissions::from_mode(0o500)).unwrap();
        let prepared = PreparedRuntime::for_test(stage.clone()).unwrap();
        let mut command = prepared.command();
        command
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        let started = std::time::Instant::now();
        let error = command.spawn().unwrap_err();
        let elapsed = started.elapsed();
        let record = read_stage_record(&prepared.0.owner).unwrap();
        drop(command);
        drop(prepared);

        assert_eq!(error.raw_os_error(), Some(libc::EPERM), "{error:?}");
        assert!(elapsed < std::time::Duration::from_secs(1), "{elapsed:?}");
        assert_eq!(
            &record[STAGE_CHILD_PID_OFFSET..STAGE_TOKEN_OFFSET],
            &[0; 8],
            "failed pre_exec published a child claim"
        );
        assert!(
            !stage.exists(),
            "failed pre_exec leaked its execution stage"
        );
        drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
    }

    #[test]
    #[ignore = "requires a finalized built app"]
    fn source_replacement_after_inventory_open_cannot_redefine_the_captured_closure() {
        let built_app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Loxa.app");
        let replacement = root.path().join("replacement.app");
        for destination in [&source, &replacement] {
            let copied = Command::new("/usr/bin/ditto")
                .args([built_app.as_os_str(), destination.as_os_str()])
                .status()
                .unwrap();
            assert!(copied.success());
        }

        let replacement_resources = replacement.join("Contents/Resources/loxa-runtime/b10344");
        let normalized_path = replacement_resources.join("normalized-inventory.json");
        let mut normalized = fs::read(&normalized_path).unwrap();
        normalized.push(b' ');
        fs::write(&normalized_path, &normalized).unwrap();
        let inventory_path = replacement_resources.join("inventory.json");
        let mut inventory: serde_json::Value =
            serde_json::from_slice(&fs::read(&inventory_path).unwrap()).unwrap();
        let entry = inventory["regular_files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| {
                entry["path"] == "Resources/loxa-runtime/b10344/normalized-inventory.json"
            })
            .unwrap();
        entry["size"] = serde_json::json!(normalized.len() as u64);
        entry["sha256"] = serde_json::json!(digest(&normalized));
        let mut inventory_bytes = serde_json::to_vec_pretty(&inventory).unwrap();
        inventory_bytes.push(b'\n');
        fs::write(&inventory_path, inventory_bytes).unwrap();

        let paths = AppPaths::from_application_values(
            &source.join("Contents/MacOS/loxa-app"),
            Some(&root.path().join("loxa-home")),
            None,
        )
        .unwrap();
        let inspected = root.path().join("inspected.app");
        let error = prepare_embedded_runtime_with_after_inventory_open(&paths, || {
            fs::rename(&source, &inspected).unwrap();
            fs::rename(&replacement, &source).unwrap();
        })
        .unwrap_err();

        assert_eq!(error, "embedded runtime source changed while capturing");
        assert_eq!(
            fs::read(
                inspected.join("Contents/Resources/loxa-runtime/b10344/normalized-inventory.json")
            )
            .unwrap()
            .len()
                + 1,
            normalized.len()
        );
    }

    #[test]
    #[ignore = "requires a finalized built app"]
    fn in_place_source_mutation_after_the_fence_cannot_redefine_published_bytes() {
        let built_app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("Loxa.app");
        let copied = Command::new("/usr/bin/ditto")
            .args([built_app.as_os_str(), source.as_os_str()])
            .status()
            .unwrap();
        assert!(copied.success());

        let helper = source.join("Contents/MacOS/llama-server");
        let inventory = source.join("Contents/Resources/loxa-runtime/b10344/inventory.json");
        let original_helper = fs::read(&helper).unwrap();
        let original_inventory = fs::read(&inventory).unwrap();
        let paths = AppPaths::from_application_values(
            &source.join("Contents/MacOS/loxa-app"),
            Some(&root.path().join("loxa-home")),
            None,
        )
        .unwrap();

        let prepared = prepare_embedded_runtime_with_after_capture_fence(&paths, || {
            for path in [&helper, &inventory] {
                fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            let mut changed_helper = original_helper.clone();
            changed_helper.extend_from_slice(b"post-fence replacement");
            fs::write(&helper, &changed_helper).unwrap();

            let mut changed_inventory: serde_json::Value =
                serde_json::from_slice(&original_inventory).unwrap();
            let helper_entry = changed_inventory["regular_files"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["path"] == "MacOS/llama-server")
                .unwrap();
            helper_entry["size"] = serde_json::json!(changed_helper.len() as u64);
            helper_entry["sha256"] = serde_json::json!(digest(&changed_helper));
            let mut changed_inventory = serde_json::to_vec_pretty(&changed_inventory).unwrap();
            changed_inventory.push(b'\n');
            fs::write(&inventory, changed_inventory).unwrap();
        })
        .unwrap();

        let staged_helper = prepared.execution_server();
        let staged_inventory = staged_helper
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join("Resources/loxa-runtime/b10344/inventory.json");
        assert!(
            fs::read(staged_helper).unwrap() == original_helper,
            "the published helper did not use the already-captured bytes"
        );
        assert!(
            fs::read(staged_inventory).unwrap() == original_inventory,
            "the published inventory did not use the original captured authority"
        );
        assert_ne!(fs::read(helper).unwrap(), original_helper);
        assert_ne!(fs::read(inventory).unwrap(), original_inventory);
    }
}
