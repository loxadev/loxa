use crate::platform::{current_uid, peer_executable};
use crate::PeerCredentials;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const DEVELOPMENT_MARKER_FILENAME: &str = "loxa-development-root.json";
const ORIGIN_FILENAME: &str = "service-origin.json";
const INSTANCE_LOCK_FILENAME: &str = "service-instance.lock";
const MARKER_SCHEMA: u32 = 1;
const ORIGIN_SCHEMA: u32 = 1;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 103;
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevelopmentRoot {
    root: PathBuf,
    control_dir: PathBuf,
    socket_path: PathBuf,
    root_identity: String,
}

pub struct DevelopmentInstanceLock {
    _file: File,
}

impl DevelopmentRoot {
    pub fn load(root: &Path, forbidden_root: Option<&Path>) -> Result<Self, String> {
        require_absolute_clean_path(root, "development data root")?;
        let canonical = fs::canonicalize(root)
            .map_err(|error| format!("could not open development data root: {error}"))?;
        if canonical != root {
            return Err("development data root must be its canonical path".into());
        }
        validate_private_directory(&canonical, "development data root")?;
        if let Some(forbidden) = forbidden_root {
            if paths_alias(&canonical, forbidden)? {
                return Err("development data root must not be the normal Loxa data root".into());
            }
        }
        let marker: DevelopmentMarker = read_record(&canonical.join(DEVELOPMENT_MARKER_FILENAME))?;
        let root_metadata = fs::metadata(&canonical).map_err(|error| error.to_string())?;
        if marker.schema_version != MARKER_SCHEMA
            || marker.root_identity
                != root_identity(
                    &canonical,
                    root_metadata.uid(),
                    root_metadata.dev(),
                    root_metadata.ino(),
                )
        {
            return Err("development data root marker is invalid".into());
        }
        let run_dir = canonical.join("run");
        validate_private_directory(&run_dir, "service run directory")?;
        let control_dir = run_dir.join("service");
        validate_private_directory(&control_dir, "service control directory")?;
        let socket_path = control_dir.join("control.sock");
        validate_service_socket_paths(&canonical)?;
        Ok(Self {
            root: canonical,
            control_dir,
            socket_path,
            root_identity: marker.root_identity,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn control_dir(&self) -> &Path {
        &self.control_dir
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn root_identity(&self) -> &str {
        &self.root_identity
    }

    pub fn validate_current(&self) -> Result<(), String> {
        let current = Self::load(&self.root, None)
            .map_err(|_| "development data root identity changed".to_string())?;
        if current != *self {
            return Err("development data root identity changed".into());
        }
        Ok(())
    }

    pub fn acquire_instance(&self) -> Result<DevelopmentInstanceLock, String> {
        acquire_instance_lock(&self.control_dir.join(INSTANCE_LOCK_FILENAME))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DevelopmentMarker {
    schema_version: u32,
    root_identity: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginRecord {
    schema_version: u32,
    executable: PathBuf,
    executable_device: u64,
    executable_inode: u64,
    executable_size: u64,
    executable_sha256: String,
    build: String,
}

impl OriginRecord {
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn executable_sha256(&self) -> &str {
        &self.executable_sha256
    }

    pub fn build(&self) -> &str {
        &self.build
    }

    pub fn validate_current(&self) -> Result<(), String> {
        if self.schema_version != ORIGIN_SCHEMA
            || self.build.is_empty()
            || self.build.len() > crate::protocol::MAX_BUILD_BYTES
            || !self.executable.is_absolute()
            || !is_lower_hex_64(&self.executable_sha256)
        {
            return Err("service origin record is invalid".into());
        }
        let canonical = fs::canonicalize(&self.executable)
            .map_err(|_| "recorded service installation needs repair".to_string())?;
        if canonical != self.executable {
            return Err("recorded service origin moved or is no longer canonical".into());
        }
        let file = open_regular(&canonical)?;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        if metadata.dev() != self.executable_device
            || metadata.ino() != self.executable_inode
            || metadata.len() != self.executable_size
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err("recorded service origin was replaced; explicit repair is required".into());
        }
        let digest = sha256_file(file)?;
        if digest != self.executable_sha256 {
            return Err(
                "recorded service origin bytes changed; explicit repair is required".into(),
            );
        }
        Ok(())
    }

    fn from_executable(executable: &Path, build: &str) -> Result<Self, String> {
        if build.is_empty() || build.len() > crate::protocol::MAX_BUILD_BYTES {
            return Err("invalid service build identity".into());
        }
        let executable = fs::canonicalize(executable)
            .map_err(|error| format!("could not resolve service executable: {error}"))?;
        require_absolute_clean_path(&executable, "service executable")?;
        let file = open_regular(&executable)?;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err("service executable is writable by another user".into());
        }
        let executable_sha256 = sha256_file(file)?;
        Ok(Self {
            schema_version: ORIGIN_SCHEMA,
            executable,
            executable_device: metadata.dev(),
            executable_inode: metadata.ino(),
            executable_size: metadata.len(),
            executable_sha256,
            build: build.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct ClientBootstrap {
    root: DevelopmentRoot,
    origin: OriginRecord,
}

impl ClientBootstrap {
    pub fn load(root: &Path, forbidden_root: Option<&Path>) -> Result<Self, String> {
        let root = DevelopmentRoot::load(root, forbidden_root)?;
        let origin: OriginRecord = read_record(&root.control_dir.join(ORIGIN_FILENAME))?;
        origin.validate_current()?;
        Ok(Self { root, origin })
    }

    pub fn load_expected(
        root: &Path,
        forbidden_root: Option<&Path>,
        expected_root_identity: &str,
    ) -> Result<Self, String> {
        let bootstrap = Self::load(root, forbidden_root)?;
        if bootstrap.root.root_identity != expected_root_identity {
            return Err("development data root identity changed".into());
        }
        Ok(bootstrap)
    }

    pub fn root(&self) -> &DevelopmentRoot {
        &self.root
    }

    pub fn origin(&self) -> &OriginRecord {
        &self.origin
    }

    pub fn validate_peer(&self, peer: PeerCredentials) -> Result<(), String> {
        if peer.uid != current_uid() {
            return Err("service socket belongs to another user".into());
        }
        if peer.pid == 0 {
            return Err("service socket peer has no process identity".into());
        }
        let peer_path = peer_executable(peer.pid)?;
        let peer_path = fs::canonicalize(peer_path)
            .map_err(|_| "service socket peer executable is unavailable".to_string())?;
        if peer_path != self.origin.executable {
            return Err("service socket peer is not the recorded Loxa origin".into());
        }
        let metadata = fs::metadata(&peer_path).map_err(|error| error.to_string())?;
        if metadata.dev() != self.origin.executable_device
            || metadata.ino() != self.origin.executable_inode
            || metadata.len() != self.origin.executable_size
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err("service socket peer origin identity changed".into());
        }
        Ok(())
    }

    pub fn launch_service(&self) -> Result<(), String> {
        self.origin.validate_current()?;
        self.root.validate_current()?;
        let mut child = Command::new(&self.origin.executable)
            .arg("__service-launch")
            .arg("--data-root")
            .arg(&self.root.root)
            .arg("--root-identity")
            .arg(&self.root.root_identity)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                format!("could not start the recorded Loxa service origin: {error}")
            })?;
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match child.try_wait().map_err(|error| error.to_string())? {
                Some(status) if status.success() => return Ok(()),
                Some(_) => {
                    return Err(
                        "recorded Loxa service launcher exited before starting the service".into(),
                    )
                }
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("recorded Loxa service launcher did not detach in time".into());
                }
                None => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

pub fn initialize_development_root(
    root: &Path,
    forbidden_root: &Path,
    executable: &Path,
    build: &str,
) -> Result<ClientBootstrap, String> {
    require_absolute_clean_path(root, "development data root")?;
    require_absolute_clean_path(forbidden_root, "normal Loxa data root")?;
    let canonical = canonicalize_existing_or_parent(root)?;
    if canonical != root {
        return Err("development data root must be its canonical path".into());
    }
    if paths_alias(&canonical, forbidden_root)? {
        return Err("development data root must not be the normal Loxa data root".into());
    }
    validate_service_socket_paths(&canonical)?;

    let existed = root.exists();
    if existed {
        validate_private_directory(root, "development data root")?;
        let marker_path = root.join(DEVELOPMENT_MARKER_FILENAME);
        if marker_path.exists() {
            ClientBootstrap::load(root, Some(forbidden_root))?;
        } else if fs::read_dir(root)
            .map_err(|error| error.to_string())?
            .next()
            .is_some()
        {
            return Err("unmarked development data root must be empty".into());
        }
    } else {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(format!("could not create development data root: {error}")),
        }
    }
    validate_private_directory(root, "development data root")?;
    if !existed
        && fs::read_dir(root)
            .map_err(|error| error.to_string())?
            .next()
            .is_some()
    {
        return Err("development data root changed during initialization".into());
    }
    let control_dir = root.join("run/service");
    create_private_directories(root, &control_dir)?;
    let _instance = acquire_instance_lock(&control_dir.join(INSTANCE_LOCK_FILENAME))?;

    let marker_path = root.join(DEVELOPMENT_MARKER_FILENAME);
    if marker_path.exists() {
        // Re-read all marker, directory, origin and executable evidence while
        // holding the same instance inode used by the service.
        let existing = ClientBootstrap::load(root, Some(forbidden_root))?;
        let requested = OriginRecord::from_executable(executable, build)?;
        if existing.origin != requested {
            return Err("development service origin differs; explicit repair is required".into());
        }
        return Ok(existing);
    }
    require_initializer_only_tree(root, &control_dir)?;

    let canonical = fs::canonicalize(root).map_err(|error| error.to_string())?;
    let root_metadata = fs::metadata(&canonical).map_err(|error| error.to_string())?;
    let root_identity = root_identity(
        &canonical,
        root_metadata.uid(),
        root_metadata.dev(),
        root_metadata.ino(),
    );
    let origin = OriginRecord::from_executable(executable, build)?;
    write_record_atomic(&control_dir.join(ORIGIN_FILENAME), &origin)?;
    write_record_atomic(
        &marker_path,
        &DevelopmentMarker {
            schema_version: MARKER_SCHEMA,
            root_identity,
        },
    )?;
    ClientBootstrap::load(root, Some(forbidden_root))
}

fn create_private_directories(root: &Path, target: &Path) -> Result<(), String> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| "service control directory escaped development root".to_string())?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err("invalid service control directory".into());
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(_) => validate_private_directory(&current, "service control directory")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder
                    .create(&current)
                    .map_err(|error| error.to_string())?;
                validate_private_directory(&current, "service control directory")?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

fn validate_private_directory(path: &Path, label: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| format!("{label}: {error}"))?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != current_uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(format!(
            "{label} is not a private user-owned 0700 directory"
        ));
    }
    Ok(())
}

fn require_initializer_only_tree(root: &Path, control_dir: &Path) -> Result<(), String> {
    let expected = [
        (root, vec![root.join("run")]),
        (&root.join("run"), vec![control_dir.to_path_buf()]),
        (control_dir, vec![control_dir.join(INSTANCE_LOCK_FILENAME)]),
    ];
    for (directory, allowed) in expected {
        for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            if !allowed.contains(&path) {
                return Err("unmarked development data root must be empty".into());
            }
        }
    }
    Ok(())
}

fn validate_socket_path_length(path: &Path) -> Result<(), String> {
    let length = path.as_os_str().as_bytes().len();
    if length > MAX_UNIX_SOCKET_PATH_BYTES {
        Err(format!(
            "service socket path is too long; choose a shorter development root ({length} bytes, maximum {MAX_UNIX_SOCKET_PATH_BYTES})"
        ))
    } else {
        Ok(())
    }
}

fn validate_service_socket_paths(root: &Path) -> Result<(), String> {
    let control = root.join("run/service");
    validate_socket_path_length(&control.join("control.sock"))?;
    validate_socket_path_length(&control.join("engine-ffffffffffffffff.sock"))
}

fn root_identity(path: &Path, uid: u32, device: u64, inode: u64) -> String {
    let mut identity = Sha256::new();
    identity.update(path.as_os_str().as_bytes());
    identity.update([0]);
    identity.update(uid.to_le_bytes());
    identity.update(device.to_le_bytes());
    identity.update(inode.to_le_bytes());
    hex_digest(&identity.finalize())
}

fn acquire_instance_lock(path: &Path) -> Result<DevelopmentInstanceLock, String> {
    let mut options = OpenOptions::new();
    options
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let file = options.open(path).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file()
        || metadata.nlink() != 1
        || metadata.uid() != current_uid()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("service instance lock is not a private regular file".into());
    }
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err("another Loxa background service owns this development root".into())
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(error.to_string()),
    }
    ensure_opened_path_unchanged(path, &metadata, &file)?;
    Ok(DevelopmentInstanceLock { _file: file })
}

fn require_absolute_clean_path(path: &Path, label: &str) -> Result<(), String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        Err(format!("{label} must be an absolute normalized path"))
    } else if path.as_os_str().as_bytes().len() > crate::protocol::MAX_PATH_BYTES {
        Err(format!("{label} is too long"))
    } else {
        Ok(())
    }
}

fn paths_alias(left: &Path, right: &Path) -> Result<bool, String> {
    let left = canonicalize_existing_or_parent(left)?;
    let right = canonicalize_existing_or_parent(right)?;
    Ok(left == right)
}

fn canonicalize_existing_or_parent(path: &Path) -> Result<PathBuf, String> {
    if path.exists() {
        return fs::canonicalize(path).map_err(|error| error.to_string());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "path has no parent".to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "path has no final component".to_string())?;
    fs::canonicalize(parent)
        .map(|parent| parent.join(name))
        .map_err(|error| error.to_string())
}

fn read_record<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let mut file = open_regular(path)?;
    let opened = file.metadata().map_err(|error| error.to_string())?;
    if opened.permissions().mode() & 0o077 != 0 {
        return Err(format!("record is not private: {}", path.display()));
    }
    let mut bytes = Vec::with_capacity(1024);
    Read::by_ref(&mut file)
        .take((MAX_RECORD_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(format!("record is too large: {}", path.display()));
    }
    let current = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if opened.dev() != current.dev() || opened.ino() != current.ino() {
        return Err(format!("record changed while reading: {}", path.display()));
    }
    serde_json::from_slice(&bytes).map_err(|_| format!("record is invalid: {}", path.display()))
}

fn write_record_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let mut bytes = [0_u8; MAX_RECORD_BYTES];
    let written = {
        let capacity = bytes.len();
        let mut remaining = bytes.as_mut_slice();
        serde_json::to_writer_pretty(&mut remaining, value)
            .map_err(|_| "record is too large".to_string())?;
        remaining
            .write_all(b"\n")
            .map_err(|_| "record is too large".to_string())?;
        capacity - remaining.len()
    };
    let parent = path
        .parent()
        .ok_or_else(|| "record path has no parent".to_string())?;
    let temporary = loop {
        let name = format!(
            ".service-record-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        );
        let candidate = parent.join(name);
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        match options.open(&candidate) {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.to_string()),
        }
    };
    let (temporary_path, mut file) = temporary;
    file.write_all(&bytes[..written])
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            let _ = fs::remove_file(&temporary_path);
            error.to_string()
        })?;
    drop(file);
    if let Err(error) = fs::hard_link(&temporary_path, path) {
        let _ = fs::remove_file(&temporary_path);
        return Err(format!("{}: {error}", path.display()));
    }
    fs::remove_file(&temporary_path).map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn open_regular(path: &Path) -> Result<File, String> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let file = options.open(path).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 || metadata.uid() != current_uid() {
        return Err(format!("unsafe record file: {}", path.display()));
    }
    Ok(file)
}

fn ensure_opened_path_unchanged(
    path: &Path,
    opened: &fs::Metadata,
    file: &File,
) -> Result<(), String> {
    let current = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    let descriptor = file.metadata().map_err(|error| error.to_string())?;
    if opened.dev() != descriptor.dev()
        || opened.ino() != descriptor.ino()
        || opened.dev() != current.dev()
        || opened.ino() != current.ino()
    {
        Err(format!("path changed while opening: {}", path.display()))
    } else {
        Ok(())
    }
}

fn sha256_file(mut file: File) -> Result<String, String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_digest(&hasher.finalize()))
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn marked_root_is_private_canonical_and_cannot_alias_forbidden_root() {
        let root = tempfile::Builder::new()
            .prefix("loxa-ipc-")
            .tempdir_in("/tmp")
            .unwrap();
        let canonical_root = fs::canonicalize(root.path()).unwrap();
        let forbidden = canonical_root.join("normal");
        let development = canonical_root.join("development");
        fs::create_dir(&forbidden).unwrap();
        fs::set_permissions(&forbidden, fs::Permissions::from_mode(0o700)).unwrap();
        initialize_development_root(
            &development,
            &forbidden,
            &std::env::current_exe().unwrap(),
            "test-build",
        )
        .unwrap();
        assert!(DevelopmentRoot::load(&development, Some(&forbidden)).is_ok());
        assert!(initialize_development_root(
            &forbidden,
            &forbidden,
            &std::env::current_exe().unwrap(),
            "test-build"
        )
        .is_err());
    }

    #[test]
    fn root_identity_uses_exact_non_utf8_path_bytes() {
        let first = PathBuf::from(OsString::from_vec(b"development-\xff".to_vec()));
        let second = PathBuf::from(OsString::from_vec(b"development-\xfe".to_vec()));
        assert_ne!(
            root_identity(&first, 1, 2, 3),
            root_identity(&second, 1, 2, 3)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn marker_binds_non_utf8_root_to_its_exact_path_bytes() {
        let parent = tempfile::tempdir_in("/tmp").unwrap();
        let parent = fs::canonicalize(parent.path()).unwrap();
        let forbidden = parent.join("normal");
        let original = parent.join(OsString::from_vec(b"development-\xff".to_vec()));
        let moved = parent.join(OsString::from_vec(b"development-\xfe".to_vec()));
        fs::create_dir(&forbidden).unwrap();
        fs::set_permissions(&forbidden, fs::Permissions::from_mode(0o700)).unwrap();
        initialize_development_root(
            &original,
            &forbidden,
            &std::env::current_exe().unwrap(),
            "test-build",
        )
        .unwrap();

        fs::rename(&original, &moved).unwrap();

        assert!(DevelopmentRoot::load(&moved, Some(&forbidden)).is_err());
    }
}
