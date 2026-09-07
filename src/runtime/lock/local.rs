use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

static LOCAL_FOREGROUND_LOCKS: Mutex<Vec<LocalForegroundLockKey>> = Mutex::new(Vec::new());
pub(in crate::runtime) static LOCAL_FOREGROUND_LOCK_OPERATIONS: Mutex<()> = Mutex::new(());
pub(in crate::runtime) fn lock_local_foreground_operation(
) -> Result<MutexGuard<'static, ()>, String> {
    LOCAL_FOREGROUND_LOCK_OPERATIONS
        .lock()
        .map_err(|_| "local foreground lock operation is poisoned".to_string())
}

pub(in crate::runtime) struct LocalForegroundLock {
    key: Option<LocalForegroundLockKey>,
}

#[derive(Clone, Eq, PartialEq)]
struct LocalForegroundLockKey {
    path: PathBuf,
    identity: Option<LocalForegroundLockIdentity>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct LocalForegroundLockIdentity {
    device: u64,
    inode: u64,
}

impl LocalForegroundLockKey {
    fn from_path(path: &Path) -> Self {
        let identity = fs::symlink_metadata(path)
            .ok()
            .and_then(|metadata| foreground_lock_identity(&metadata));
        Self {
            path: path.to_path_buf(),
            identity,
        }
    }

    fn conflicts_with(&self, other: &Self) -> bool {
        self.path == other.path
            || matches!(
                (self.identity, other.identity),
                (Some(left), Some(right)) if left == right
            )
    }
}

fn foreground_lock_identity(metadata: &fs::Metadata) -> Option<LocalForegroundLockIdentity> {
    use std::os::unix::fs::MetadataExt;

    if !metadata.file_type().is_file() {
        return None;
    }
    Some(LocalForegroundLockIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

impl LocalForegroundLock {
    pub(in crate::runtime) fn reserve(path: &Path) -> Result<Self, String> {
        let key = LocalForegroundLockKey::from_path(path);
        let mut held = LOCAL_FOREGROUND_LOCKS
            .lock()
            .map_err(|_| "local foreground lock registry is poisoned".to_string())?;
        if held.iter().any(|existing| existing.conflicts_with(&key)) {
            return Err("another Loxa runtime is active".into());
        }
        held.push(key.clone());
        Ok(Self { key: Some(key) })
    }

    pub(in crate::runtime) fn bind_to_file(&mut self, file: &File) -> Result<(), String> {
        let metadata = file
            .metadata()
            .map_err(|error| format!("runtime lock metadata: {error}"))?;
        let identity =
            foreground_lock_identity(&metadata).ok_or_else(|| "unsafe runtime lock".to_string())?;
        let path = self
            .key
            .as_ref()
            .ok_or_else(|| "local foreground lock reservation is missing".to_string())?
            .path
            .clone();
        let bound = LocalForegroundLockKey {
            path,
            identity: Some(identity),
        };
        let mut held = LOCAL_FOREGROUND_LOCKS
            .lock()
            .map_err(|_| "local foreground lock registry is poisoned".to_string())?;
        let index = held
            .iter()
            .position(|existing| self.key.as_ref() == Some(existing))
            .ok_or_else(|| "local foreground lock reservation is missing".to_string())?;
        if held
            .iter()
            .enumerate()
            .any(|(other, existing)| other != index && existing.conflicts_with(&bound))
        {
            return Err("another Loxa runtime is active".into());
        }
        held[index] = bound.clone();
        self.key = Some(bound);
        Ok(())
    }

    pub(in crate::runtime) fn is_held(path: &Path) -> Result<bool, String> {
        let candidate = LocalForegroundLockKey::from_path(path);
        LOCAL_FOREGROUND_LOCKS
            .lock()
            .map(|held| {
                held.iter()
                    .any(|existing| existing.conflicts_with(&candidate))
            })
            .map_err(|_| "local foreground lock registry is poisoned".to_string())
    }

    pub(super) fn release(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        if let Ok(mut held) = LOCAL_FOREGROUND_LOCKS.lock() {
            if let Some(index) = held.iter().position(|existing| existing == &key) {
                held.swap_remove(index);
            }
        }
    }
}

impl Drop for LocalForegroundLock {
    fn drop(&mut self) {
        self.release();
    }
}
