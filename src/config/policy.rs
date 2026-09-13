use super::file::{self, WriteFailure, WriteFault};
use super::{validate_generation, Config, LoadedSettings, MAX_CONFIG_BYTES};
use loxa_ipc::{
    ErrorCategory, GenerationSettings, GenerationSettingsPatch, OptionalU16Patch, OptionalU32Patch,
    ServiceError, ServiceSettings, ServiceSettingsApplication, ServiceSettingsDurability,
    ServiceSettingsPatch,
};
use serde::Serialize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use tokio::sync::watch;

pub(crate) type SettingsObserver = watch::Receiver<Option<Result<ServiceSettings, ServiceError>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettingsExit {
    Running,
    FlushPending,
    WriteCompleted,
    FlushFailed,
    Drained,
}

#[derive(Clone)]
pub(crate) struct SettingsOwner {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    state: Mutex<State>,
    exit: watch::Sender<SettingsExit>,
    #[cfg(test)]
    next_fault: Mutex<WriteFault>,
    #[cfg(test)]
    stall: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    panic_next: Mutex<bool>,
    #[cfg(test)]
    fail_spawn_next: Mutex<bool>,
    #[cfg(test)]
    finish_publish_stall: Mutex<Option<Arc<std::sync::Barrier>>>,
}

struct State {
    committed: LoadedSettings,
    retained: Option<RetainedWrite>,
    completed: Option<Result<(), WriteFailure>>,
    phase: Phase,
    draining: bool,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Idle,
    Saving,
    Joining,
    SaveFailed,
    OutcomeUnknown,
}

struct RetainedWrite {
    candidate: LoadedSettings,
    encoded: Arc<[u8]>,
    outcome: watch::Sender<Option<Result<ServiceSettings, ServiceError>>>,
    retry_mode: WriteMode,
}

#[derive(Serialize)]
struct SavedDocument<'a> {
    version: u32,
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ctx: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    generation: &'a GenerationSettings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteMode {
    Replace,
    Reconcile,
}

impl SettingsOwner {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        let committed = super::load_private(path)?;
        let (exit, _) = watch::channel(SettingsExit::Running);
        Ok(Self {
            inner: Arc::new(Inner {
                path: path.to_owned(),
                state: Mutex::new(State {
                    committed,
                    retained: None,
                    completed: None,
                    phase: Phase::Idle,
                    draining: false,
                    thread: None,
                }),
                exit,
                #[cfg(test)]
                next_fault: Mutex::new(WriteFault::None),
                #[cfg(test)]
                stall: Mutex::new(None),
                #[cfg(test)]
                panic_next: Mutex::new(false),
                #[cfg(test)]
                fail_spawn_next: Mutex::new(false),
                #[cfg(test)]
                finish_publish_stall: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> ServiceSettings {
        let state = self.lock_state_recover();
        wire_snapshot(&state.committed, durability(&state))
    }

    pub(crate) fn capture_generation(&self) -> Result<GenerationSettings, ServiceError> {
        let state = self.lock_state()?;
        if state.retained.is_some() || state.phase != Phase::Idle || state.thread.is_some() {
            return Err(unavailable(
                "service settings durability must resolve before capturing defaults",
            ));
        }
        Ok(state.committed.generation.clone())
    }

    pub(crate) fn capture_config(&self) -> Result<Config, ServiceError> {
        let state = self.lock_state()?;
        if state.retained.is_some() || state.phase != Phase::Idle || state.thread.is_some() {
            return Err(unavailable(
                "service settings durability must resolve before loading a runtime",
            ));
        }
        Ok(state.committed.config)
    }

    pub(crate) fn patch(
        &self,
        expected_revision: &str,
        patch: ServiceSettingsPatch,
    ) -> Result<SettingsObserver, ServiceError> {
        let expected = parse_revision(expected_revision)?;
        let mut state = self.lock_state()?;
        if state.draining {
            return Err(unavailable("service settings owner is draining"));
        }
        if state.phase == Phase::Saving || state.phase == Phase::Joining || state.thread.is_some() {
            return Err(busy("a service settings save is already active"));
        }
        if state.retained.is_some() {
            return Err(unavailable(
                "the retained service settings save must be retried first",
            ));
        }
        if state.committed.revision != expected {
            return Err(conflict("service settings revision changed"));
        }
        let mut candidate = state.committed.clone();
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or_else(|| conflict("service settings revision overflow"))?;
        candidate.v2 = true;
        apply_patch(&mut candidate, patch)?;
        validate_generation(&candidate.generation).map_err(invalid)?;
        let encoded = encode(&candidate)?;
        let (outcome, observer) = watch::channel(None);
        state.retained = Some(RetainedWrite {
            candidate,
            encoded,
            outcome,
            retry_mode: WriteMode::Replace,
        });
        state.phase = Phase::Saving;
        self.spawn_locked(&mut state, WriteMode::Replace);
        Ok(observer)
    }

    pub(crate) fn retry(&self) -> Result<SettingsObserver, ServiceError> {
        let mut state = self.lock_state()?;
        if state.thread.is_some() || matches!(state.phase, Phase::Saving | Phase::Joining) {
            return Err(busy("a service settings save is already active"));
        }
        let mode = match state.phase {
            Phase::SaveFailed | Phase::OutcomeUnknown => {
                state
                    .retained
                    .as_ref()
                    .ok_or_else(|| internal("failed settings save has no retained candidate"))?
                    .retry_mode
            }
            Phase::Idle | Phase::Saving | Phase::Joining => {
                return Err(conflict("service settings has no failed save to retry"))
            }
        };
        let retained = state
            .retained
            .as_ref()
            .ok_or_else(|| internal("failed settings save has no retained candidate"))?;
        retained.outcome.send_replace(None);
        let observer = retained.outcome.subscribe();
        state.phase = Phase::Saving;
        self.inner.exit.send_replace(if state.draining {
            SettingsExit::FlushPending
        } else {
            SettingsExit::Running
        });
        self.spawn_locked(&mut state, mode);
        Ok(observer)
    }

    pub(crate) fn finish_write(&self) -> Result<(), ServiceError> {
        let handle = {
            let mut state = self.lock_state()?;
            if state.phase != Phase::Joining {
                return Err(conflict("settings write has not completed"));
            }
            state
                .thread
                .take()
                .ok_or_else(|| internal("completed settings write has no retained thread"))?
        };
        let join_result = handle.join();
        let mut state = self.lock_state()?;
        if state.phase != Phase::Joining || state.thread.is_some() {
            return Err(internal("settings write lifecycle changed while joining"));
        }
        let result = if join_result.is_err() {
            Err(WriteFailure {
                outcome_unknown: false,
                context: "settings write thread panicked".into(),
            })
        } else {
            state.completed.take().ok_or_else(|| {
                internal("settings write thread ended without recording a completion")
            })?
        };
        let mut retained = state
            .retained
            .take()
            .ok_or_else(|| internal("completed settings write has no retained candidate"))?;
        match result {
            Ok(()) => {
                state.committed = retained.candidate;
                state.phase = Phase::Idle;
                let snapshot = wire_snapshot(&state.committed, ServiceSettingsDurability::Saved);
                retained.outcome.send_replace(Some(Ok(snapshot)));
            }
            Err(error) => {
                let remains_unknown =
                    error.outcome_unknown || retained.retry_mode == WriteMode::Reconcile;
                let category = if remains_unknown {
                    state.phase = Phase::OutcomeUnknown;
                    retained.retry_mode = WriteMode::Reconcile;
                    ErrorCategory::OutcomeUnknown
                } else {
                    state.phase = Phase::SaveFailed;
                    ErrorCategory::ServiceUnavailable
                };
                retained
                    .outcome
                    .send_replace(Some(Err(ServiceError::new(category, error.context))));
                state.retained = Some(retained);
            }
        }
        let exit = exit_for(&state);
        #[cfg(test)]
        if let Some(barrier) = self
            .inner
            .finish_publish_stall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            barrier.wait();
            barrier.wait();
        }
        self.inner.exit.send_replace(exit);
        Ok(())
    }

    pub(crate) fn exit_receiver(&self) -> watch::Receiver<SettingsExit> {
        self.inner.exit.subscribe()
    }

    pub(crate) fn begin_drain(&self) {
        let mut state = self.lock_state_recover();
        state.draining = true;
        let exit = exit_for(&state);
        self.inner.exit.send_replace(exit);
    }

    pub(crate) fn join(&self) -> Result<(), String> {
        if self.lock_state().map_err(|error| error.context)?.phase == Phase::Joining {
            self.finish_write().map_err(|error| error.context)?;
        }
        let state = self.lock_state().map_err(|error| error.context)?;
        if state.thread.is_some() {
            return Err("settings write thread is still active".into());
        }
        if *self.inner.exit.borrow() != SettingsExit::Drained {
            return Err("settings owner has not completed its durable drain".into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_before_rename(&self) {
        *self
            .inner
            .next_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WriteFault::BeforeRename;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_after_rename(&self) {
        *self
            .inner
            .next_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WriteFault::AfterRename;
    }

    #[cfg(test)]
    pub(crate) fn stall_next(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .inner
            .stall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    pub(crate) fn panic_next(&self) {
        *self
            .inner
            .panic_next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_spawn(&self) {
        *self
            .inner
            .fail_spawn_next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }

    #[cfg(test)]
    pub(crate) fn stall_finish_publish(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .inner
            .finish_publish_stall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, State>, ServiceError> {
        self.inner
            .state
            .lock()
            .map_err(|_| internal("settings owner lock is poisoned"))
    }

    fn lock_state_recover(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn spawn_locked(&self, state: &mut State, mode: WriteMode) {
        debug_assert!(state.thread.is_none());
        #[cfg(test)]
        let fail_spawn = std::mem::take(
            &mut *self
                .inner
                .fail_spawn_next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        #[cfg(not(test))]
        let fail_spawn = false;
        let spawned = if fail_spawn {
            Err(std::io::Error::other(
                "injected settings thread spawn failure",
            ))
        } else {
            let inner = Arc::clone(&self.inner);
            std::thread::Builder::new()
                .name("loxa-settings-write".into())
                .spawn(move || execute_write(inner, mode))
        };
        match spawned {
            Ok(handle) => state.thread = Some(handle),
            Err(error) => {
                let remains_unknown = mode == WriteMode::Reconcile;
                state.phase = if remains_unknown {
                    Phase::OutcomeUnknown
                } else {
                    Phase::SaveFailed
                };
                let retained = state
                    .retained
                    .as_ref()
                    .expect("saving settings retains its candidate");
                retained.outcome.send_replace(Some(Err(ServiceError::new(
                    if remains_unknown {
                        ErrorCategory::OutcomeUnknown
                    } else {
                        ErrorCategory::ServiceUnavailable
                    },
                    format!("could not start settings save: {error}"),
                ))));
                self.inner.exit.send_replace(if state.draining {
                    SettingsExit::FlushFailed
                } else {
                    SettingsExit::Running
                });
            }
        }
    }
}

fn execute_write(inner: Arc<Inner>, mode: WriteMode) {
    let result =
        catch_unwind(AssertUnwindSafe(|| perform_write(&inner, mode))).unwrap_or_else(|_| {
            Err(WriteFailure {
                outcome_unknown: false,
                context: "settings write thread panicked".into(),
            })
        });
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.completed = Some(result);
    state.phase = Phase::Joining;
    inner.exit.send_replace(SettingsExit::WriteCompleted);
}

fn perform_write(inner: &Inner, mode: WriteMode) -> Result<(), WriteFailure> {
    #[cfg(test)]
    if let Some(barrier) = inner
        .stall
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        barrier.wait();
        barrier.wait();
    }
    #[cfg(test)]
    if std::mem::take(
        &mut *inner
            .panic_next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    ) {
        panic!("injected settings write panic");
    }
    let (encoded, fault) = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let encoded = Arc::clone(
            &state
                .retained
                .as_ref()
                .expect("saving settings retains its candidate")
                .encoded,
        );
        #[cfg(test)]
        let fault = std::mem::replace(
            &mut *inner
                .next_fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            WriteFault::None,
        );
        #[cfg(not(test))]
        let fault = WriteFault::None;
        (encoded, fault)
    };
    match mode {
        WriteMode::Replace => file::replace(&inner.path, &encoded, fault),
        WriteMode::Reconcile => file::reconcile(&inner.path, &encoded),
    }
}

fn exit_for(state: &State) -> SettingsExit {
    if !state.draining {
        return SettingsExit::Running;
    }
    match state.phase {
        Phase::Idle if state.thread.is_none() => SettingsExit::Drained,
        Phase::Saving => SettingsExit::FlushPending,
        Phase::Joining => SettingsExit::WriteCompleted,
        Phase::SaveFailed | Phase::OutcomeUnknown => SettingsExit::FlushFailed,
        Phase::Idle => SettingsExit::FlushFailed,
    }
}

fn apply_patch(
    candidate: &mut LoadedSettings,
    patch: ServiceSettingsPatch,
) -> Result<(), ServiceError> {
    if patch.ctx.is_none() && patch.port.is_none() && patch.generation.is_none() {
        return Err(invalid("service settings patch is empty"));
    }
    if let Some(ctx) = patch.ctx {
        candidate.config.ctx = match ctx {
            OptionalU32Patch::Set { value } => Some(value),
            OptionalU32Patch::Clear => None,
        };
    }
    if let Some(port) = patch.port {
        candidate.config.port = match port {
            OptionalU16Patch::Set { value } => Some(value),
            OptionalU16Patch::Clear => None,
        };
    }
    if let Some(generation) = patch.generation {
        apply_generation_patch(&mut candidate.generation, generation)?;
    }
    Ok(())
}

pub(crate) fn apply_generation_patch(
    generation: &mut GenerationSettings,
    patch: GenerationSettingsPatch,
) -> Result<(), ServiceError> {
    match patch {
        GenerationSettingsPatch::Fields {
            system_instruction,
            max_output_tokens,
        } => {
            if system_instruction.is_none() && max_output_tokens.is_none() {
                return Err(invalid("generation settings patch is empty"));
            }
            if let Some(value) = system_instruction {
                generation.system_instruction = value;
            }
            if let Some(value) = max_output_tokens {
                generation.max_output_tokens = value;
            }
        }
        GenerationSettingsPatch::Reset => *generation = GenerationSettings::default(),
    }
    validate_generation(generation).map_err(invalid)
}

fn encode(candidate: &LoadedSettings) -> Result<Arc<[u8]>, ServiceError> {
    let encoded = serde_json::to_vec(&SavedDocument {
        version: 2,
        revision: candidate.revision,
        ctx: candidate.config.ctx,
        port: candidate.config.port,
        generation: &candidate.generation,
    })
    .map_err(|_| internal("service settings candidate cannot be encoded"))?;
    if encoded.len() > MAX_CONFIG_BYTES {
        return Err(invalid("encoded service settings exceed the byte limit"));
    }
    Ok(encoded.into())
}

fn durability(state: &State) -> ServiceSettingsDurability {
    match state.phase {
        Phase::Saving | Phase::Joining => ServiceSettingsDurability::Saving,
        Phase::SaveFailed => ServiceSettingsDurability::SaveFailed,
        Phase::OutcomeUnknown => ServiceSettingsDurability::OutcomeUnknown,
        Phase::Idle if state.committed.v2 => ServiceSettingsDurability::Saved,
        Phase::Idle => ServiceSettingsDurability::Baseline,
    }
}

fn wire_snapshot(
    loaded: &LoadedSettings,
    durability: ServiceSettingsDurability,
) -> ServiceSettings {
    ServiceSettings {
        revision: loaded.revision.to_string(),
        ctx: loaded.config.ctx,
        port: loaded.config.port,
        generation: loaded.generation.clone(),
        durability,
        application: ServiceSettingsApplication::NotApplied,
    }
}

fn parse_revision(value: &str) -> Result<u64, ServiceError> {
    if value.is_empty()
        || value.len() > 20
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("invalid service settings revision"));
    }
    value
        .parse()
        .map_err(|_| invalid("invalid service settings revision"))
}

fn invalid(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::InvalidRequest, context)
}

fn conflict(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Conflict, context)
}

fn busy(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Busy, context)
}

fn unavailable(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::ServiceUnavailable, context)
}

fn internal(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::Internal, context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    fn ctx(value: u32) -> ServiceSettingsPatch {
        ServiceSettingsPatch {
            ctx: Some(OptionalU32Patch::Set { value }),
            port: None,
            generation: None,
        }
    }

    fn wait_for(owner: &SettingsOwner, expected: SettingsExit) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while *owner.inner.exit.borrow() != expected {
            assert!(
                Instant::now() < deadline,
                "settings owner did not reach {expected:?}"
            );
            std::thread::yield_now();
        }
    }

    fn finish(owner: &SettingsOwner) {
        wait_for(owner, SettingsExit::WriteCompleted);
        owner.finish_write().unwrap();
    }

    #[test]
    fn save_is_acknowledged_only_after_join_and_an_immediate_next_patch_is_admitted() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("config.json");
        let owner = SettingsOwner::open(&path).unwrap();

        let first = owner.patch("0", ctx(8192)).unwrap();
        wait_for(&owner, SettingsExit::WriteCompleted);
        assert!(first.borrow().is_none());
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::Saving
        );
        owner.finish_write().unwrap();
        assert_eq!(
            first.borrow().as_ref().unwrap().as_ref().unwrap().revision,
            "1"
        );

        let second = owner
            .patch(
                "1",
                ServiceSettingsPatch {
                    ctx: None,
                    port: Some(OptionalU16Patch::Set { value: 4000 }),
                    generation: None,
                },
            )
            .unwrap();
        finish(&owner);
        assert_eq!(
            second.borrow().as_ref().unwrap().as_ref().unwrap().revision,
            "2"
        );
        let loaded = super::super::load_private(&path).unwrap();
        assert_eq!(loaded.config.ctx, Some(8192));
        assert_eq!(loaded.config.port, Some(4000));
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        owner.begin_drain();
        assert_eq!(*owner.inner.exit.borrow(), SettingsExit::Drained);
        owner.join().unwrap();
    }

    #[test]
    fn stop_waits_for_the_joined_write_before_publishing_saved_and_drained() {
        let directory = tempdir().unwrap();
        let owner = SettingsOwner::open(&directory.path().join("config.json")).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        owner.stall_next(Arc::clone(&barrier));
        let observer = owner.patch("0", ctx(4096)).unwrap();
        owner.begin_drain();
        assert_eq!(*owner.inner.exit.borrow(), SettingsExit::FlushPending);
        assert!(observer.borrow().is_none());

        barrier.wait();
        barrier.wait();
        wait_for(&owner, SettingsExit::WriteCompleted);
        assert!(observer.borrow().is_none());
        owner.finish_write().unwrap();
        assert_eq!(*owner.inner.exit.borrow(), SettingsExit::Drained);
        assert_eq!(
            observer
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .revision,
            "1"
        );
        owner.join().unwrap();
    }

    #[test]
    fn joined_ack_and_exit_publication_are_serialized_before_the_next_patch() {
        let directory = tempdir().unwrap();
        let owner = SettingsOwner::open(&directory.path().join("config.json")).unwrap();
        let first = owner.patch("0", ctx(4096)).unwrap();
        wait_for(&owner, SettingsExit::WriteCompleted);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        owner.stall_finish_publish(Arc::clone(&barrier));
        let finisher = {
            let owner = owner.clone();
            std::thread::spawn(move || owner.finish_write())
        };
        barrier.wait();
        assert!(first.borrow().as_ref().unwrap().is_ok());

        let (sent, received) = mpsc::sync_channel(1);
        let patcher = {
            let owner = owner.clone();
            std::thread::spawn(move || {
                let result = owner.patch("1", ctx(8192));
                let _ = sent.send(result);
            })
        };
        assert!(matches!(
            received.recv_timeout(Duration::from_millis(25)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        barrier.wait();
        finisher.join().unwrap().unwrap();
        let second = received
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        patcher.join().unwrap();
        finish(&owner);
        assert_eq!(
            second.borrow().as_ref().unwrap().as_ref().unwrap().revision,
            "2"
        );
    }

    #[test]
    fn failed_and_uncertain_writes_retain_the_candidate_for_the_exact_retry() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("config.json");
        let owner = SettingsOwner::open(&path).unwrap();

        owner.fail_next_before_rename();
        let failed = owner.patch("0", ctx(8192)).unwrap();
        finish(&owner);
        assert_eq!(
            failed
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::ServiceUnavailable
        );
        assert_eq!(owner.snapshot().revision, "0");
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::SaveFailed
        );
        let retried = owner.retry().unwrap();
        finish(&owner);
        assert_eq!(
            retried
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .revision,
            "1"
        );

        owner.fail_next_after_rename();
        let uncertain = owner.patch("1", ctx(16384)).unwrap();
        finish(&owner);
        assert_eq!(
            uncertain
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::OutcomeUnknown
        );
        assert_eq!(owner.snapshot().revision, "1");
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::OutcomeUnknown
        );

        owner.panic_next();
        let panic_reconcile = owner.retry().unwrap();
        finish(&owner);
        assert_eq!(
            panic_reconcile
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::OutcomeUnknown
        );
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::OutcomeUnknown
        );

        owner.fail_next_spawn();
        let spawn_reconcile = owner.retry().unwrap();
        assert_eq!(
            spawn_reconcile
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::OutcomeUnknown
        );
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::OutcomeUnknown
        );

        let candidate = std::fs::read(&path).unwrap();
        std::fs::write(&path, br#"{"version":1}"#).unwrap();
        let mismatched = owner.retry().unwrap();
        finish(&owner);
        assert_eq!(
            mismatched
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::OutcomeUnknown
        );
        assert_eq!(std::fs::read(&path).unwrap(), br#"{"version":1}"#);
        std::fs::write(&path, candidate).unwrap();
        let reconciled = owner.retry().unwrap();
        finish(&owner);
        assert_eq!(
            reconciled
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .revision,
            "2"
        );
        assert_eq!(
            super::super::load_private(&path).unwrap().config.ctx,
            Some(16384)
        );
    }

    #[test]
    fn spawn_and_worker_panics_become_retained_save_failures() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("config.json");
        let owner = SettingsOwner::open(&path).unwrap();

        owner.fail_next_spawn();
        let spawn_failed = owner.patch("0", ctx(4096)).unwrap();
        assert_eq!(
            spawn_failed
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::ServiceUnavailable
        );
        assert_eq!(
            owner.snapshot().durability,
            ServiceSettingsDurability::SaveFailed
        );
        let retried = owner.retry().unwrap();
        finish(&owner);
        assert!(retried.borrow().as_ref().unwrap().is_ok());

        owner.panic_next();
        let panicked = owner.patch("1", ctx(8192)).unwrap();
        finish(&owner);
        assert_eq!(
            panicked
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .category,
            ErrorCategory::ServiceUnavailable
        );
        assert_eq!(owner.snapshot().revision, "1");
        let retried = owner.retry().unwrap();
        finish(&owner);
        assert_eq!(
            retried
                .borrow()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .revision,
            "2"
        );
    }
}
