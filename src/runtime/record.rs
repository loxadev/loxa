use super::lease::{decode_lease, encode_lease, RuntimeLease};
use super::process::{process_group_has_live_members, process_snapshot};
use std::fs::{self, OpenOptions};
use std::io::{Read as _, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

pub(super) const MAX_RUNTIME_RECORD_BYTES: usize = 64 * 1024;
static LEASE_STATE_IO: Mutex<()> = Mutex::new(());
pub(super) fn lock_lease_state() -> Result<MutexGuard<'static, ()>, String> {
    LEASE_STATE_IO
        .lock()
        .map_err(|_| "runtime lease state lock is poisoned".to_string())
}

pub(super) fn lease_is_absent(path: &Path) -> bool {
    matches!(
        fs::symlink_metadata(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    )
}

pub(crate) fn clear_terminated_owned_lease(
    run_dir: &Path,
    child_pid: u32,
    child_pgid: i32,
) -> Result<(), String> {
    // foreground.lock excludes other Loxa processes; this guard serializes the
    // signal watcher with publication and cleanup inside the owning process.
    let _state_guard = lock_lease_state()?;
    let state_path = run_dir.join("foreground.json");
    let lease = match read_lease(&state_path) {
        Ok(lease) => lease,
        Err(_error) if !state_path.exists() => return Ok(()),
        Err(error) => return Err(error),
    };
    let owner_pid = std::process::id();
    let owner = process_snapshot(owner_pid)?
        .ok_or_else(|| "failed to identify the Loxa process".to_string())?;
    if lease.owner_pid != owner_pid
        || lease.owner_start_time != owner.start_identity
        || lease.child_pid != child_pid
        || lease.child_pgid != child_pgid
    {
        return Err(format!(
            "runtime lease changed unexpectedly: {}",
            state_path.display()
        ));
    }
    cleanup_recorded_execution_stage(run_dir, &lease)?;
    match fs::remove_file(&state_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("{}: {error}", state_path.display())),
    }
}

pub(super) fn cleanup_recorded_execution_stage(
    run_dir: &Path,
    lease: &RuntimeLease,
) -> Result<(), String> {
    if lease.managed_source.is_none() {
        return Ok(());
    }
    if process_group_has_live_members(lease.child_pgid)? {
        return Err("llama-server process group is still active during stage cleanup".into());
    }
    #[cfg(unix)]
    {
        crate::runtime_bundle::cleanup_execution_stage(run_dir, &lease.server)
    }
    #[cfg(not(unix))]
    {
        let _ = (run_dir, lease);
        Ok(())
    }
}

pub(super) fn ensure_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(format!(
            "runtime path is not a directory: {}",
            path.display()
        ))
    }
}

pub(super) fn read_lease(path: &Path) -> Result<RuntimeLease, String> {
    decode_lease(&read_regular_file(path)?).map_err(|error| format!("{}: {error}", path.display()))
}

pub(super) fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options
        .open(path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    let opened = crate::safe_file::regular_file_identity(&file, path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    let mut bytes = Vec::with_capacity(4 * 1024);
    (&mut file)
        .take((MAX_RUNTIME_RECORD_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    crate::safe_file::ensure_descriptor_matches_path(&file, &opened, path)
        .map_err(|error| map_runtime_lease_read_error(path, error))?;
    if bytes.len() > MAX_RUNTIME_RECORD_BYTES {
        return Err(format!(
            "runtime record exceeds the {MAX_RUNTIME_RECORD_BYTES}-byte limit: {}",
            path.display()
        ));
    }
    Ok(bytes)
}

fn map_runtime_lease_read_error(path: &Path, error: std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::InvalidData {
        format!("unsafe runtime lease {}", path.display())
    } else {
        format!("{}: {error}", path.display())
    }
}

pub(super) fn write_lease(path: &Path, lease: &RuntimeLease) -> Result<(), String> {
    let temporary = path.with_extension("json.tmp");
    let bytes = encode_lease(lease)?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| format!("{}: {error}", temporary.display()))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("{}: {error}", temporary.display()))?;
    fs::rename(&temporary, path).map_err(|error| format!("{}: {error}", path.display()))
}
