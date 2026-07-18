use crate::operation_cancellation::OperationCancellation;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ArtifactKey {
    canonical_destination: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactKeyError {
    AmbiguousDestination,
    UnsafeDestination,
}

impl ArtifactKey {
    pub(crate) fn from_destination(destination: &Path) -> Result<Self, ArtifactKeyError> {
        let file_name = destination
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or(ArtifactKeyError::AmbiguousDestination)?;
        if destination
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(ArtifactKeyError::UnsafeDestination);
        }
        match std::fs::symlink_metadata(destination) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => return Err(ArtifactKeyError::UnsafeDestination),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ArtifactKeyError::AmbiguousDestination),
        }
        let parent = destination
            .parent()
            .ok_or(ArtifactKeyError::AmbiguousDestination)?;
        let canonical_parent =
            std::fs::canonicalize(parent).map_err(|_| ArtifactKeyError::AmbiguousDestination)?;
        Ok(Self {
            canonical_destination: canonical_parent.join(file_name),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactAcquireError {
    Busy,
    Cancelled,
    Poisoned,
    Sealed,
}

#[derive(Clone)]
pub(crate) struct ArtifactMutationCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl ArtifactMutationCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState::default()),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn try_acquire_mutation(
        &self,
        key: ArtifactKey,
    ) -> Result<ArtifactMutationLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        state.ensure_available(&key)?;
        if state.access.contains_key(&key) {
            return Err(ArtifactAcquireError::Busy);
        }
        state
            .access
            .insert(key.clone(), ArtifactAccessState::Mutating);
        Ok(ArtifactMutationLease {
            key,
            owner: Arc::downgrade(&self.inner),
            released: false,
        })
    }

    pub(crate) fn acquire_mutation(
        &self,
        key: ArtifactKey,
        cancellation: &OperationCancellation,
    ) -> Result<ArtifactMutationLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        loop {
            if cancellation.is_cancel_requested() {
                return Err(ArtifactAcquireError::Cancelled);
            }
            state.ensure_available(&key)?;
            if !state.access.contains_key(&key) {
                state
                    .access
                    .insert(key.clone(), ArtifactAccessState::Mutating);
                return Ok(ArtifactMutationLease {
                    key,
                    owner: Arc::downgrade(&self.inner),
                    released: false,
                });
            }
            match self
                .inner
                .changed
                .wait_timeout(state, Duration::from_millis(10))
            {
                Ok((next, _)) => state = next,
                Err(poisoned) => {
                    let (mut next, _) = poisoned.into_inner();
                    next.sealed = true;
                    return Err(ArtifactAcquireError::Sealed);
                }
            }
        }
    }

    pub(crate) fn try_acquire_read(
        &self,
        key: ArtifactKey,
    ) -> Result<ArtifactReadLease, ArtifactAcquireError> {
        let mut state = self.inner.lock_state()?;
        state.ensure_available(&key)?;
        match state.access.get_mut(&key) {
            Some(ArtifactAccessState::Mutating) => return Err(ArtifactAcquireError::Busy),
            Some(ArtifactAccessState::Readers(readers)) => *readers += 1,
            None => {
                state
                    .access
                    .insert(key.clone(), ArtifactAccessState::Readers(1));
            }
        }
        Ok(ArtifactReadLease {
            key,
            owner: Arc::downgrade(&self.inner),
            released: false,
        })
    }

    pub(crate) fn seal(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sealed = true;
        self.inner.changed.notify_all();
    }
}

pub(crate) struct ArtifactMutationLease {
    key: ArtifactKey,
    owner: Weak<CoordinatorInner>,
    released: bool,
}

impl fmt::Debug for ArtifactMutationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactMutationLease")
            .field("key", &self.key)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl ArtifactMutationLease {
    pub(crate) fn poison(mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.poison(&self.key);
        }
        self.released = true;
    }
}

impl Drop for ArtifactMutationLease {
    fn drop(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_mutation(&self.key);
            }
            self.released = true;
        }
    }
}

pub(crate) struct ArtifactReadLease {
    key: ArtifactKey,
    owner: Weak<CoordinatorInner>,
    released: bool,
}

impl fmt::Debug for ArtifactReadLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactReadLease")
            .field("key", &self.key)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl Drop for ArtifactReadLease {
    fn drop(&mut self) {
        if !self.released {
            if let Some(owner) = self.owner.upgrade() {
                owner.release_read(&self.key);
            }
            self.released = true;
        }
    }
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
    changed: Condvar,
}

impl CoordinatorInner {
    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CoordinatorState>, ArtifactAcquireError> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                Err(ArtifactAcquireError::Sealed)
            }
        }
    }

    fn release_mutation(&self, key: &ArtifactKey) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                return;
            }
        };
        if !state.poisoned.contains(key)
            && matches!(state.access.get(key), Some(ArtifactAccessState::Mutating))
        {
            state.access.remove(key);
            self.changed.notify_all();
        }
    }

    fn release_read(&self, key: &ArtifactKey) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.sealed = true;
                return;
            }
        };
        if state.poisoned.contains(key) {
            return;
        }
        if let Some(ArtifactAccessState::Readers(readers)) = state.access.get_mut(key) {
            *readers -= 1;
            if *readers == 0 {
                state.access.remove(key);
                self.changed.notify_all();
            }
        }
    }

    fn poison(&self, key: &ArtifactKey) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.poisoned.insert(key.clone());
        state.sealed = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct CoordinatorState {
    access: HashMap<ArtifactKey, ArtifactAccessState>,
    poisoned: HashSet<ArtifactKey>,
    sealed: bool,
}

impl CoordinatorState {
    fn ensure_available(&self, key: &ArtifactKey) -> Result<(), ArtifactAcquireError> {
        if self.poisoned.contains(key) {
            Err(ArtifactAcquireError::Poisoned)
        } else if self.sealed {
            Err(ArtifactAcquireError::Sealed)
        } else {
            Ok(())
        }
    }
}

enum ArtifactAccessState {
    Mutating,
    Readers(usize),
}
