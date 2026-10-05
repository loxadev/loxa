//! User-service installation within one retained, validated private root.

use super::*;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct UserServiceRoot {
    pub(super) paths: DevelopmentRoot,
    root: PrivateUserRoot,
    pub(super) run: File,
    control: File,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UserMarker {
    schema_version: u32,
    root_identity: String,
}

pub(super) fn initialize(
    root: PrivateUserRoot,
    executable: &Path,
    build: &str,
) -> Result<ClientBootstrap, String> {
    root.validate_current().map_err(|error| error.to_string())?;
    let requested = OriginRecord::from_executable(executable, build)?;
    let root = UserServiceRoot::open(root, true)?;
    let _instance = root.acquire_instance()?;
    root.validate_directories()?;
    if entry_exists(root.root.directory(), USER_SERVICE_MARKER_FILENAME)? {
        let existing = finish_load(root)?;
        if existing.origin != requested {
            return Err("user service origin differs; explicit repair is required".into());
        }
        return Ok(existing);
    }
    // A failed marker publication can leave the exact origin behind. Under
    // this instance lock, resume only that bounded partial installation.
    let mut has_origin = false;
    for entry in fs::read_dir(&root.paths.control_dir).map_err(|error| error.to_string())? {
        let name = entry.map_err(|error| error.to_string())?.file_name();
        if name == ORIGIN_FILENAME {
            has_origin = true;
        } else if name != INSTANCE_LOCK_FILENAME {
            return Err("unmarked user service control directory has unexpected entries".into());
        }
    }
    root.validate_directories()?;
    if has_origin {
        let existing: OriginRecord = read_record_at(&root.control, ORIGIN_FILENAME)?;
        existing.validate_current()?;
        if existing != requested {
            return Err("partial user service origin differs; explicit repair is required".into());
        }
        // The prior publication may have linked the origin before its
        // directory sync failed. Retry durability before publishing the marker.
        root.control.sync_all().map_err(|error| error.to_string())?;
    } else {
        write_record_at(&root.control, ORIGIN_FILENAME, &requested)?;
    }
    root.validate_directories()?;
    write_record_at(
        root.root.directory(),
        USER_SERVICE_MARKER_FILENAME,
        &UserMarker {
            schema_version: MARKER_SCHEMA,
            root_identity: root.paths.root_identity.clone(),
        },
    )?;
    finish_load(root)
}

pub(super) fn load(path: &Path) -> Result<ClientBootstrap, String> {
    let root = match UserRootInspection::inspect(path).map_err(|error| error.to_string())? {
        UserRootInspection::Private(root) => root,
        UserRootInspection::RepairRequired(_) => {
            return Err("user data root requires explicit permission-repair confirmation".into())
        }
    };
    finish_load(UserServiceRoot::open(root, false)?)
}

fn finish_load(root: UserServiceRoot) -> Result<ClientBootstrap, String> {
    root.validate_current()?;
    let origin: OriginRecord = read_record_at(&root.control, ORIGIN_FILENAME)?;
    origin.validate_current()?;
    root.validate_current()?;
    Ok(ClientBootstrap {
        root: ServiceRoot {
            kind: ServiceRootKind::User(Arc::new(root)),
        },
        origin,
    })
}

impl UserServiceRoot {
    fn open(root: PrivateUserRoot, create: bool) -> Result<Self, String> {
        root.validate_current().map_err(|error| error.to_string())?;
        validate_service_socket_paths(root.path())?;
        let metadata = root
            .directory()
            .metadata()
            .map_err(|error| error.to_string())?;
        let identity = root_identity(root.path(), metadata.uid(), metadata.dev(), metadata.ino());
        let run = open_directory_at(root.directory(), "run", create, false)?;
        root.validate_current().map_err(|error| error.to_string())?;
        let control = open_directory_at(&run, "service", create, true)?;
        let control_dir = root.path().join("run/service");
        let state = Self {
            paths: DevelopmentRoot {
                root: root.path().to_owned(),
                socket_path: control_dir.join("control.sock"),
                control_dir,
                root_identity: identity,
            },
            root,
            run,
            control,
        };
        state.validate_directories()?;
        Ok(state)
    }

    fn validate_directories(&self) -> Result<(), String> {
        self.root
            .validate_current()
            .map_err(|error| error.to_string())?;
        let run = open_directory_at(self.root.directory(), "run", false, false)?;
        ensure_same_directory(&self.run, &run, false)?;
        let control = open_directory_at(&run, "service", false, true)?;
        ensure_same_directory(&self.control, &control, true)?;
        self.root
            .validate_current()
            .map_err(|error| error.to_string())
    }

    pub(super) fn validate_current(&self) -> Result<(), String> {
        self.validate_directories()?;
        let marker: UserMarker =
            read_record_at(self.root.directory(), USER_SERVICE_MARKER_FILENAME)?;
        if marker.schema_version != MARKER_SCHEMA
            || marker.root_identity != self.paths.root_identity
        {
            return Err("user service root marker is invalid".into());
        }
        self.validate_directories()
    }

    pub(super) fn acquire_instance(&self) -> Result<DevelopmentInstanceLock, String> {
        self.validate_directories()?;
        let file = open_entry(
            &self.control,
            INSTANCE_LOCK_FILENAME,
            libc::O_RDWR | libc::O_CREAT,
            0o600,
        )?;
        let instance = lock_instance_file(file)?;
        ensure_entry_matches(&self.control, INSTANCE_LOCK_FILENAME, &instance._file)?;
        self.validate_directories()?;
        Ok(instance)
    }
}

fn open_directory_at(
    parent: &File,
    name: &str,
    create: bool,
    private: bool,
) -> Result<File, String> {
    let directory = match open_entry(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0) {
        Ok(directory) => directory,
        Err(_) if create && !entry_exists(parent, name)? => {
            let name_c = CString::new(name).map_err(|error| error.to_string())?;
            // SAFETY: the retained parent and NUL-terminated name remain live.
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(error.to_string());
                }
            }
            open_entry(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
        }
        Err(error) => return Err(error),
    };
    validate_directory(&directory, private)?;
    Ok(directory)
}

fn validate_directory(directory: &File, private: bool) -> Result<(), String> {
    let metadata = directory.metadata().map_err(|error| error.to_string())?;
    let mode = metadata.mode() & 0o7777;
    if !metadata.file_type().is_dir()
        || metadata.uid() != current_uid()
        || (private && mode != 0o700)
        || (!private && mode & 0o7022 != 0)
    {
        return Err("unsafe user service directory permissions or ownership".into());
    }
    #[cfg(target_os = "macos")]
    user_root::reject_extended_acl(directory).map_err(|error| error.to_string())?;
    Ok(())
}

fn ensure_same_directory(retained: &File, current: &File, private: bool) -> Result<(), String> {
    validate_directory(retained, private)?;
    let retained = retained.metadata().map_err(|error| error.to_string())?;
    let current = current.metadata().map_err(|error| error.to_string())?;
    if retained.dev() != current.dev() || retained.ino() != current.ino() {
        return Err("user service directory identity changed".into());
    }
    Ok(())
}

fn open_entry(parent: &File, name: &str, flags: libc::c_int, mode: u32) -> Result<File, String> {
    let name = CString::new(name).map_err(|error| error.to_string())?;
    // SAFETY: both inputs remain live; successful openat returns an owned FD.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            mode as libc::c_uint,
        )
    };
    if fd == -1 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    // SAFETY: this descriptor was newly returned by openat.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn entry_exists(parent: &File, name: &str) -> Result<bool, String> {
    let name = CString::new(name).map_err(|error| error.to_string())?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the retained descriptor/name remain live and metadata has stat space.
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == 0
    {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(error.to_string())
    }
}

fn ensure_entry_matches(parent: &File, name: &str, file: &File) -> Result<(), String> {
    let current = open_entry(parent, name, libc::O_RDONLY, 0)?;
    let opened = file.metadata().map_err(|error| error.to_string())?;
    let current = current.metadata().map_err(|error| error.to_string())?;
    validate_regular_file(&opened)?;
    validate_regular_file(&current)?;
    if opened.dev() != current.dev() || opened.ino() != current.ino() {
        return Err("user service record identity changed".into());
    }
    Ok(())
}

fn read_record_at<T: serde::de::DeserializeOwned>(parent: &File, name: &str) -> Result<T, String> {
    let mut file = open_entry(parent, name, libc::O_RDONLY, 0)?;
    let record = read_record_contents(&mut file)?;
    ensure_entry_matches(parent, name, &file)?;
    Ok(record)
}

fn write_record_at<T: Serialize>(parent: &File, name: &str, value: &T) -> Result<(), String> {
    let (bytes, written) = record_bytes(value)?;
    let (temporary, mut file) = loop {
        let temporary = format!(
            ".service-record-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        );
        match open_entry(
            parent,
            &temporary,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        ) {
            Ok(file) => break (temporary, file),
            Err(_) if entry_exists(parent, &temporary)? => continue,
            Err(error) => return Err(error),
        }
    };
    let temporary = CString::new(temporary).map_err(|error| error.to_string())?;
    let name = CString::new(name).map_err(|error| error.to_string())?;
    let result = file
        .write_all(&bytes[..written])
        .and_then(|()| file.sync_all());
    let result = result.and_then(|()| {
        // SAFETY: names and retained parent remain live. linkat never replaces
        // an existing destination; both source and destination use the same FD.
        if unsafe {
            libc::linkat(
                parent.as_raw_fd(),
                temporary.as_ptr(),
                parent.as_raw_fd(),
                name.as_ptr(),
                0,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    });
    // SAFETY: cleanup is confined to the temporary entry in the retained parent.
    let removed = unsafe { libc::unlinkat(parent.as_raw_fd(), temporary.as_ptr(), 0) };
    result.map_err(|error| error.to_string())?;
    if removed != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    parent.sync_all().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
