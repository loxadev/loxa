use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

const INTENT_SCHEMA: u32 = 1;
const MAX_INTENT_BYTES: usize = 16 * 1024;
const INTENT_FILENAME: &str = "launch-intent.json";

#[derive(Default)]
pub(super) struct ClearProgress {
    unlinked: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchIntent {
    schema_version: u32,
    root_identity: String,
    machine_boot_id: String,
    boot_epoch: String,
    task_id: u64,
    generation: u64,
    model_id: String,
    service_pid: u32,
    service_start_identity: u64,
}

impl LaunchIntent {
    pub(super) fn new(
        root_identity: &str,
        machine_boot_id: &str,
        boot_epoch: &str,
        task_id: u64,
        generation: u64,
        model_id: &str,
    ) -> Result<Self, String> {
        crate::paths::validate_id(model_id)?;
        Ok(Self {
            schema_version: INTENT_SCHEMA,
            root_identity: root_identity.to_owned(),
            machine_boot_id: machine_boot_id.to_owned(),
            boot_epoch: boot_epoch.to_owned(),
            task_id,
            generation,
            model_id: model_id.to_owned(),
            service_pid: std::process::id(),
            service_start_identity: crate::runtime::current_process_start_identity()?,
        })
    }

    fn validate(&self, root_identity: &str) -> Result<(), String> {
        if self.schema_version != INTENT_SCHEMA
            || self.root_identity != root_identity
            || self.machine_boot_id.is_empty()
            || self.machine_boot_id.len() > 160
            || self.boot_epoch.is_empty()
            || self.boot_epoch.len() > 160
            || self.task_id == 0
            || self.generation == 0
            || self.service_pid == 0
            || self.service_start_identity == 0
        {
            return Err("invalid retained service launch intent".into());
        }
        crate::paths::validate_id(&self.model_id)
    }
}

pub(super) fn boot_evidence(root_identity: &str) -> Result<(String, String), String> {
    let machine_boot_id = machine_boot_id()?;
    let start = crate::runtime::current_process_start_identity()?;
    let material = format!(
        "{root_identity}\0{machine_boot_id}\0{}\0{start}",
        std::process::id()
    );
    let boot_epoch = hex_digest(&Sha256::digest(material.as_bytes()));
    Ok((machine_boot_id, boot_epoch))
}

pub(super) fn audit_retained(control_dir: &Path, root_identity: &str) -> Result<(), String> {
    let path = control_dir.join(INTENT_FILENAME);
    let bytes = match read_private_record(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "service recovery required; launch intent could not be audited: {error}"
            ))
        }
    };
    let intent: LaunchIntent = serde_json::from_slice(&bytes)
        .map_err(|_| "service recovery required; launch intent is invalid".to_string())?;
    intent.validate(root_identity).map_err(|error| {
        format!("service recovery required; retained launch intent failed validation: {error}")
    })?;
    Err(format!(
        "service recovery required; retained launch intent {}:{} for model {} on boot {}",
        intent.task_id, intent.generation, intent.model_id, intent.machine_boot_id
    ))
}

pub(super) fn publish(control_dir: &Path, intent: &LaunchIntent) -> Result<(), String> {
    let path = control_dir.join(INTENT_FILENAME);
    if path.exists() {
        return Err("a retained service launch intent requires recovery".into());
    }
    let bytes = serde_json::to_vec_pretty(intent).map_err(|error| error.to_string())?;
    if bytes.len() + 1 > MAX_INTENT_BYTES {
        return Err("service launch intent is too large".into());
    }
    let temporary = control_dir.join(format!(
        ".launch-intent-{}-{}.tmp",
        std::process::id(),
        intent.task_id
    ));
    let mut options = OpenOptions::new();
    options
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let mut file = options
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    let result = file
        .write_all(&bytes)
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all());
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    drop(file);
    if let Err(error) = fs::hard_link(&temporary, &path) {
        let _ = fs::remove_file(&temporary);
        return Err(format!("{}: {error}", path.display()));
    }
    fs::remove_file(&temporary).map_err(|error| error.to_string())?;
    File::open(control_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

pub(super) fn clear(
    control_dir: &Path,
    expected: &LaunchIntent,
    progress: &mut ClearProgress,
) -> Result<(), String> {
    clear_with_directory_sync(control_dir, expected, progress, |directory| {
        File::open(directory).and_then(|directory| directory.sync_all())
    })
}

fn clear_with_directory_sync(
    control_dir: &Path,
    expected: &LaunchIntent,
    progress: &mut ClearProgress,
    mut sync_directory: impl FnMut(&Path) -> std::io::Result<()>,
) -> Result<(), String> {
    let path = control_dir.join(INTENT_FILENAME);
    if !progress.unlinked {
        let bytes = read_private_record(&path).map_err(|error| error.to_string())?;
        let current: LaunchIntent = serde_json::from_slice(&bytes)
            .map_err(|_| "service launch intent is invalid".to_string())?;
        if &current != expected {
            return Err("service launch intent changed unexpectedly".into());
        }
        fs::remove_file(&path).map_err(|error| format!("{}: {error}", path.display()))?;
        progress.unlinked = true;
    } else {
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => return Err("service launch intent reappeared during directory sync".into()),
            Err(error) => return Err(error.to_string()),
        }
    }
    sync_directory(control_dir).map_err(|error| error.to_string())
}

fn read_private_record(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let mut file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.file_type().is_file()
        || opened.nlink() != 1
        || opened.uid() != unsafe { libc::geteuid() }
        || opened.permissions().mode() & 0o777 != 0o600
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsafe service launch intent",
        ));
    }
    let mut bytes = Vec::with_capacity(1024);
    Read::by_ref(&mut file)
        .take((MAX_INTENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_INTENT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "service launch intent is too large",
        ));
    }
    let current = fs::symlink_metadata(path)?;
    if current.dev() != opened.dev() || current.ino() != opened.ino() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "service launch intent changed while reading",
        ));
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn machine_boot_id() -> Result<String, String> {
    let mut file =
        File::open("/proc/sys/kernel/random/boot_id").map_err(|error| error.to_string())?;
    let mut value = String::new();
    Read::by_ref(&mut file)
        .take(65)
        .read_to_string(&mut value)
        .map_err(|error| error.to_string())?;
    let value = value.trim();
    if value.len() != 36
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return Err("invalid Linux boot identity".into());
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(target_os = "macos")]
fn machine_boot_id() -> Result<String, String> {
    let mut value = [0_u8; 64];
    let mut length = value.len();
    // SAFETY: sysctlbyname writes at most `length` bytes to the fixed buffer.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            value.as_mut_ptr().cast::<libc::c_void>(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || length == 0 || length > value.len() {
        return Err("failed to read macOS boot identity".into());
    }
    let end = value[..length]
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(length);
    let value = std::str::from_utf8(&value[..end])
        .map_err(|_| "invalid macOS boot identity".to_string())?;
    if value.len() != 36
        || !value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
    {
        return Err("invalid macOS boot identity".into());
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn machine_boot_id() -> Result<String, String> {
    Err("service boot identity is unsupported on this platform".into())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> LaunchIntent {
        LaunchIntent {
            schema_version: INTENT_SCHEMA,
            root_identity: "a".repeat(64),
            machine_boot_id: "machine-boot".into(),
            boot_epoch: "service-boot".into(),
            task_id: 1,
            generation: 1,
            model_id: "demo".into(),
            service_pid: std::process::id(),
            service_start_identity: 1,
        }
    }

    #[test]
    fn clear_retries_directory_sync_after_verified_unlink() {
        let directory = tempfile::tempdir().unwrap();
        let expected = intent();
        publish(directory.path(), &expected).unwrap();
        let path = directory.path().join(INTENT_FILENAME);
        let mut progress = ClearProgress::default();

        let error = clear_with_directory_sync(directory.path(), &expected, &mut progress, |_| {
            Err(std::io::Error::other("injected directory sync failure"))
        })
        .unwrap_err();
        assert!(error.contains("injected directory sync failure"), "{error}");
        assert!(!path.exists());

        clear_with_directory_sync(directory.path(), &expected, &mut progress, |directory| {
            File::open(directory).and_then(|directory| directory.sync_all())
        })
        .unwrap();
        assert!(!path.exists());
    }
}
