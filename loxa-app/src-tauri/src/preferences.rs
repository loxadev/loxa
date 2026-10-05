mod file;
mod schema;

use file::{WriteFailure, WriteFault};
use schema::{encode, load, validate};
pub(crate) use schema::{Preferences, PreferencesPatch};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::JoinHandle;

pub(crate) const MAX_PREFERENCES_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreferenceErrorKind {
    InvalidInput,
    Conflict,
    Busy,
    Unavailable,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreferenceError {
    pub(crate) kind: PreferenceErrorKind,
    pub(crate) context: String,
}

impl PreferenceError {
    fn new(kind: PreferenceErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PreferenceAcknowledgement {
    result: Arc<Mutex<Option<Result<Preferences, PreferenceError>>>>,
}

impl PreferenceAcknowledgement {
    fn new() -> Self {
        Self {
            result: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(dead_code, reason = "reserved for preferences UI integration")]
    pub(crate) fn result(&self) -> Option<Result<Preferences, PreferenceError>> {
        self.result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn publish(&self, result: Result<Preferences, PreferenceError>) {
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreferencesExit {
    Drained,
    Pending,
    Failed,
}

#[derive(Clone)]
pub(crate) struct PreferencesOwner {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub(crate) struct WeakPreferencesOwner {
    inner: Weak<Inner>,
}

impl WeakPreferencesOwner {
    pub(crate) fn upgrade(&self) -> Option<PreferencesOwner> {
        self.inner.upgrade().map(|inner| PreferencesOwner { inner })
    }
}

struct Inner {
    path: PathBuf,
    state: Mutex<State>,
    changed: Condvar,
    completion_notifier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    next_fault: Mutex<WriteFault>,
    #[cfg(test)]
    stall: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    notifier_tail_stall: Mutex<Option<Arc<std::sync::Barrier>>>,
}

struct State {
    committed: Preferences,
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
    Finishing,
    SaveFailed,
    OutcomeUnknown,
}

struct RetainedWrite {
    candidate: Preferences,
    encoded: Arc<[u8]>,
    acknowledgement: PreferenceAcknowledgement,
    retry_mode: WriteMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteMode {
    Replace,
    Reconcile,
}

impl PreferencesOwner {
    pub(crate) fn open(path: &Path) -> Result<Self, PreferenceError> {
        let committed = load(path)?;
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
                changed: Condvar::new(),
                completion_notifier: Mutex::new(None),
                #[cfg(test)]
                next_fault: Mutex::new(WriteFault::None),
                #[cfg(test)]
                stall: Mutex::new(None),
                #[cfg(test)]
                notifier_tail_stall: Mutex::new(None),
            }),
        })
    }

    #[allow(dead_code, reason = "reserved for preferences UI integration")]
    pub(crate) fn snapshot(&self) -> Preferences {
        self.lock_state().committed.clone()
    }

    pub(crate) fn downgrade(&self) -> WeakPreferencesOwner {
        WeakPreferencesOwner {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(crate) fn set_completion_notifier(&self, notifier: Arc<dyn Fn() + Send + Sync>) {
        *self
            .inner
            .completion_notifier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(notifier);
    }

    pub(crate) fn clear_completion_notifier(&self) {
        self.inner
            .completion_notifier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(crate) fn finish_notified_write(&self) -> bool {
        let handle = {
            let mut state = self.lock_state();
            if state.draining || state.phase != Phase::Joining {
                return true;
            }
            let Some(handle) = state.thread.as_ref() else {
                return true;
            };
            if !handle.is_finished() {
                return false;
            }
            let handle = state
                .thread
                .take()
                .expect("checked completed writer thread");
            state.phase = Phase::Finishing;
            handle
        };
        let _ = self.finish_joined_thread(handle);
        true
    }

    #[allow(dead_code, reason = "reserved for preferences UI integration")]
    pub(crate) fn patch(
        &self,
        expected_revision: u64,
        patch: PreferencesPatch,
    ) -> Result<PreferenceAcknowledgement, PreferenceError> {
        let mut state = self.lock_state();
        if state.draining {
            return Err(error(
                PreferenceErrorKind::Unavailable,
                "desktop preferences are draining",
            ));
        }
        if matches!(
            state.phase,
            Phase::Saving | Phase::Joining | Phase::Finishing
        ) || state.thread.is_some()
        {
            return Err(error(
                PreferenceErrorKind::Busy,
                "a desktop preference save is already active",
            ));
        }
        if state.retained.is_some() {
            return Err(error(
                PreferenceErrorKind::Unavailable,
                "the retained desktop preference save must be retried first",
            ));
        }
        if state.committed.revision != expected_revision {
            return Err(error(
                PreferenceErrorKind::Conflict,
                "desktop preference revision changed",
            ));
        }
        if patch.is_empty() {
            return Err(error(
                PreferenceErrorKind::InvalidInput,
                "desktop preference patch is empty",
            ));
        }
        let mut candidate = state.committed.clone();
        candidate.revision = candidate.revision.checked_add(1).ok_or_else(|| {
            error(
                PreferenceErrorKind::Conflict,
                "desktop preference revision overflow",
            )
        })?;
        patch.apply(&mut candidate);
        validate(&candidate)?;
        let encoded = encode(&candidate)?;
        let acknowledgement = PreferenceAcknowledgement::new();
        state.retained = Some(RetainedWrite {
            candidate,
            encoded,
            acknowledgement: acknowledgement.clone(),
            retry_mode: WriteMode::Replace,
        });
        state.phase = Phase::Saving;
        self.spawn_locked(&mut state, WriteMode::Replace);
        Ok(acknowledgement)
    }

    pub(crate) fn begin_drain(&self) -> PreferencesExit {
        let mut state = self.lock_state();
        state.draining = true;
        exit_state(&state)
    }

    pub(crate) fn retry_for_exit(&self) -> Result<PreferencesExit, PreferenceError> {
        let mut state = self.lock_state();
        if !state.draining {
            return Err(error(
                PreferenceErrorKind::Conflict,
                "desktop preference drain has not begun",
            ));
        }
        if matches!(
            state.phase,
            Phase::Saving | Phase::Joining | Phase::Finishing
        ) || state.thread.is_some()
        {
            return Ok(PreferencesExit::Pending);
        }
        let mode = match state.phase {
            Phase::SaveFailed | Phase::OutcomeUnknown => {
                state
                    .retained
                    .as_ref()
                    .ok_or_else(|| {
                        error(
                            PreferenceErrorKind::Unavailable,
                            "failed desktop preference save has no retained candidate",
                        )
                    })?
                    .retry_mode
            }
            Phase::Idle => return Ok(PreferencesExit::Drained),
            Phase::Saving | Phase::Joining | Phase::Finishing => unreachable!(),
        };
        let acknowledgement = PreferenceAcknowledgement::new();
        state
            .retained
            .as_mut()
            .expect("failed desktop preference save retains its candidate")
            .acknowledgement = acknowledgement;
        state.phase = Phase::Saving;
        self.spawn_locked(&mut state, mode);
        Ok(exit_state(&state))
    }

    pub(crate) fn resolve_exit_blocking(&self) -> Result<(), PreferenceError> {
        loop {
            let state = self.lock_state();
            if !state.draining {
                return Err(error(
                    PreferenceErrorKind::Conflict,
                    "desktop preference drain has not begun",
                ));
            }
            match state.phase {
                Phase::Idle if state.thread.is_none() => return Ok(()),
                Phase::Saving => {
                    drop(
                        self.inner
                            .changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                    );
                }
                Phase::Joining => {
                    drop(state);
                    self.finish_completed_write()?;
                }
                Phase::Finishing => {
                    drop(
                        self.inner
                            .changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                    );
                }
                Phase::SaveFailed => {
                    return Err(error(
                        PreferenceErrorKind::Unavailable,
                        "desktop preferences could not be saved",
                    ))
                }
                Phase::OutcomeUnknown => {
                    return Err(error(
                        PreferenceErrorKind::OutcomeUnknown,
                        "desktop preference durability is unknown",
                    ))
                }
                Phase::Idle => {
                    return Err(error(
                        PreferenceErrorKind::Unavailable,
                        "desktop preference writer did not quiesce",
                    ))
                }
            }
        }
    }

    pub(crate) fn finish_ready_drain(&self) -> Result<(), PreferenceError> {
        let state = self.lock_state();
        if state.draining && state.phase == Phase::Idle && state.thread.is_none() {
            Ok(())
        } else {
            Err(error(
                PreferenceErrorKind::Unavailable,
                "desktop preference drain is unresolved",
            ))
        }
    }

    fn finish_completed_write(&self) -> Result<(), PreferenceError> {
        let handle = {
            let mut state = self.lock_state();
            if state.phase != Phase::Joining {
                return Err(error(
                    PreferenceErrorKind::Conflict,
                    "desktop preference write has not completed",
                ));
            }
            let handle = state.thread.take().ok_or_else(|| {
                error(
                    PreferenceErrorKind::Unavailable,
                    "completed desktop preference write has no thread",
                )
            })?;
            state.phase = Phase::Finishing;
            handle
        };
        self.finish_joined_thread(handle)
    }

    fn finish_joined_thread(&self, handle: JoinHandle<()>) -> Result<(), PreferenceError> {
        let joined = handle.join();
        let mut state = self.lock_state();
        if state.phase != Phase::Finishing || state.thread.is_some() {
            return Err(error(
                PreferenceErrorKind::Unavailable,
                "desktop preference write lifecycle changed while joining",
            ));
        }
        let result = if joined.is_err() {
            Err(WriteFailure {
                outcome_unknown: false,
                context: "desktop preference write thread panicked".into(),
            })
        } else {
            state.completed.take().ok_or_else(|| {
                error(
                    PreferenceErrorKind::Unavailable,
                    "desktop preference writer ended without a result",
                )
            })?
        };
        let mut retained = state.retained.take().ok_or_else(|| {
            error(
                PreferenceErrorKind::Unavailable,
                "completed desktop preference write has no retained candidate",
            )
        })?;
        match result {
            Ok(()) => {
                state.committed = retained.candidate;
                state.phase = Phase::Idle;
                retained
                    .acknowledgement
                    .publish(Ok(state.committed.clone()));
            }
            Err(failure) => {
                let unknown =
                    failure.outcome_unknown || retained.retry_mode == WriteMode::Reconcile;
                let kind = if unknown {
                    state.phase = Phase::OutcomeUnknown;
                    retained.retry_mode = WriteMode::Reconcile;
                    PreferenceErrorKind::OutcomeUnknown
                } else {
                    state.phase = Phase::SaveFailed;
                    PreferenceErrorKind::Unavailable
                };
                retained
                    .acknowledgement
                    .publish(Err(PreferenceError::new(kind, failure.context)));
                state.retained = Some(retained);
            }
        }
        self.inner.changed.notify_all();
        Ok(())
    }

    fn spawn_locked(&self, state: &mut State, mode: WriteMode) {
        debug_assert!(state.thread.is_none());
        let inner = Arc::clone(&self.inner);
        match std::thread::Builder::new()
            .name("loxa-desktop-preferences-write".into())
            .spawn(move || execute_write(inner, mode))
        {
            Ok(thread) => state.thread = Some(thread),
            Err(failure) => {
                let unknown = mode == WriteMode::Reconcile;
                state.phase = if unknown {
                    Phase::OutcomeUnknown
                } else {
                    Phase::SaveFailed
                };
                let retained = state
                    .retained
                    .as_mut()
                    .expect("saving desktop preferences retains its candidate");
                if unknown {
                    retained.retry_mode = WriteMode::Reconcile;
                }
                retained.acknowledgement.publish(Err(error(
                    if unknown {
                        PreferenceErrorKind::OutcomeUnknown
                    } else {
                        PreferenceErrorKind::Unavailable
                    },
                    format!("could not start desktop preference save: {failure}"),
                )));
                self.inner.changed.notify_all();
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn stall_next(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .inner
            .stall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    fn stall_after_notifier(&self, barrier: Arc<std::sync::Barrier>) {
        *self
            .inner
            .notifier_tail_stall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier);
    }

    #[cfg(test)]
    fn fail_next_before_rename(&self) {
        *self
            .inner
            .next_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WriteFault::BeforeRename;
    }

    #[cfg(test)]
    fn fail_next_after_rename(&self) {
        *self
            .inner
            .next_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WriteFault::AfterRename;
    }

    #[cfg(test)]
    fn finish_write_for_test(&self) {
        loop {
            let state = self.lock_state();
            match state.phase {
                Phase::Saving => {
                    drop(
                        self.inner
                            .changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                    );
                }
                Phase::Joining => {
                    drop(state);
                    self.finish_completed_write().unwrap();
                    return;
                }
                _ => panic!("desktop preference test write was not active"),
            }
        }
    }
}

fn execute_write(inner: Arc<Inner>, mode: WriteMode) {
    let result =
        catch_unwind(AssertUnwindSafe(|| perform_write(&inner, mode))).unwrap_or_else(|_| {
            Err(WriteFailure {
                outcome_unknown: false,
                context: "desktop preference writer panicked".into(),
            })
        });
    {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.completed = Some(result);
        state.phase = Phase::Joining;
        inner.changed.notify_all();
    }
    let notifier = inner
        .completion_notifier
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(notifier) = notifier {
        notifier();
    }
    #[cfg(test)]
    if let Some(barrier) = inner
        .notifier_tail_stall
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        barrier.wait();
        barrier.wait();
    }
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
    let (encoded, fault) = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let encoded = Arc::clone(
            &state
                .retained
                .as_ref()
                .expect("saving desktop preferences retains its candidate")
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

fn exit_state(state: &State) -> PreferencesExit {
    match state.phase {
        Phase::Idle if state.thread.is_none() => PreferencesExit::Drained,
        Phase::Saving | Phase::Joining | Phase::Finishing => PreferencesExit::Pending,
        Phase::SaveFailed | Phase::OutcomeUnknown => PreferencesExit::Failed,
        Phase::Idle => PreferencesExit::Failed,
    }
}

fn error(kind: PreferenceErrorKind, context: impl Into<String>) -> PreferenceError {
    PreferenceError::new(kind, context)
}

#[cfg(test)]
mod tests {
    use super::schema::{Appearance, MotionPreference, ReadingWidth, SendKey};
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::mpsc;
    use std::time::Duration;

    fn all_fields() -> PreferencesPatch {
        PreferencesPatch {
            appearance: Some(Appearance::Dark),
            chat_text_size_px: Some(18),
            motion: Some(MotionPreference::Reduce),
            spellcheck: Some(false),
            send_key: Some(SendKey::CommandEnter),
            reading_width: Some(ReadingWidth::Wide),
            tail_follow: Some(false),
        }
    }

    #[test]
    fn defaults_and_all_typed_fields_round_trip_through_a_private_store() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("desktop-preferences.json");
        let owner = PreferencesOwner::open(&path).unwrap();
        assert_eq!(owner.snapshot(), Preferences::default());

        let acknowledgement = owner.patch(0, all_fields()).unwrap();
        owner.finish_write_for_test();
        let saved = acknowledgement.result().unwrap().unwrap();
        assert_eq!(saved.revision, 1);
        assert_eq!(saved.appearance, Appearance::Dark);
        assert_eq!(saved.chat_text_size_px, 18);
        assert_eq!(saved.motion, MotionPreference::Reduce);
        assert!(!saved.spellcheck);
        assert_eq!(saved.send_key, SendKey::CommandEnter);
        assert_eq!(saved.reading_width, ReadingWidth::Wide);
        assert!(!saved.tail_follow);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(PreferencesOwner::open(&path).unwrap().snapshot(), saved);
    }

    #[test]
    fn strict_reader_rejects_unknown_null_invalid_and_oversized_documents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("desktop-preferences.json");
        let valid = br#"{"version":1,"revision":1,"appearance":"system","chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#;
        let invalid = [
            br#"{"version":2,"revision":1,"appearance":"system","chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#.as_slice(),
            br#"{"version":1,"revision":0,"appearance":"system","chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#.as_slice(),
            br#"{"version":1,"revision":1,"appearance":null,"chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#.as_slice(),
            br#"{"version":1,"revision":1,"appearance":"system","chat_text_size_px":19,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#.as_slice(),
            br#"{"version":1,"revision":1,"appearance":"system","chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true,"extra":1}"#.as_slice(),
        ];
        for document in invalid {
            fs::write(&path, document).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            assert!(matches!(
                PreferencesOwner::open(&path),
                Err(PreferenceError {
                    kind: PreferenceErrorKind::InvalidInput,
                    ..
                })
            ));
        }

        fs::write(&path, valid).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            PreferencesOwner::open(&path),
            Err(PreferenceError {
                kind: PreferenceErrorKind::Unavailable,
                ..
            })
        ));
        fs::write(&path, vec![b' '; MAX_PREFERENCES_BYTES + 1]).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(PreferencesOwner::open(&path).is_err());
    }

    #[test]
    fn native_reader_rejects_links_fifo_and_directory_without_touching_a_valid_file() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let valid = directory.path().join("valid.json");
        let document = br#"{"version":1,"revision":1,"appearance":"system","chat_text_size_px":14,"motion":"system","spellcheck":true,"send_key":"enter","reading_width":"comfortable","tail_follow":true}"#;
        fs::write(&valid, document).unwrap();
        fs::set_permissions(&valid, fs::Permissions::from_mode(0o600)).unwrap();
        let assert_unavailable = |path: &Path| {
            assert!(matches!(
                PreferencesOwner::open(path),
                Err(PreferenceError {
                    kind: PreferenceErrorKind::Unavailable,
                    ..
                })
            ));
        };

        let symbolic = directory.path().join("symbolic.json");
        symlink(&valid, &symbolic).unwrap();
        assert_unavailable(&symbolic);

        let hard_source = directory.path().join("hard-source.json");
        let hard_alias = directory.path().join("hard-alias.json");
        fs::write(&hard_source, document).unwrap();
        fs::set_permissions(&hard_source, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&hard_source, &hard_alias).unwrap();
        assert_unavailable(&hard_alias);

        let fifo = directory.path().join("fifo.json");
        let fifo_path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
        assert_unavailable(&fifo);

        let nested_directory = directory.path().join("directory.json");
        fs::create_dir(&nested_directory).unwrap();
        assert_unavailable(&nested_directory);

        assert_eq!(
            PreferencesOwner::open(&valid).unwrap().snapshot().revision,
            1
        );
        assert_eq!(fs::read(&valid).unwrap(), document);
    }

    #[test]
    fn one_writer_and_revision_checks_precede_durable_acknowledgement() {
        let directory = tempfile::tempdir().unwrap();
        let owner = PreferencesOwner::open(&directory.path().join("preferences.json")).unwrap();
        assert!(matches!(
            owner.patch(0, PreferencesPatch::default()),
            Err(PreferenceError {
                kind: PreferenceErrorKind::InvalidInput,
                ..
            })
        ));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        owner.stall_next(Arc::clone(&barrier));
        let acknowledgement = owner
            .patch(
                0,
                PreferencesPatch {
                    chat_text_size_px: Some(15),
                    ..PreferencesPatch::default()
                },
            )
            .unwrap();
        barrier.wait();
        assert!(acknowledgement.result().is_none());
        assert!(matches!(
            owner.patch(
                0,
                PreferencesPatch {
                    spellcheck: Some(false),
                    ..PreferencesPatch::default()
                }
            ),
            Err(PreferenceError {
                kind: PreferenceErrorKind::Busy,
                ..
            })
        ));
        barrier.wait();
        owner.finish_write_for_test();
        assert_eq!(acknowledgement.result().unwrap().unwrap().revision, 1);
        assert!(matches!(
            owner.patch(
                0,
                PreferencesPatch {
                    spellcheck: Some(false),
                    ..PreferencesPatch::default()
                }
            ),
            Err(PreferenceError {
                kind: PreferenceErrorKind::Conflict,
                ..
            })
        ));
    }

    #[test]
    fn ordinary_completion_notifier_never_joins_a_writer_with_tail_work() {
        let directory = tempfile::tempdir().unwrap();
        let owner = PreferencesOwner::open(&directory.path().join("preferences.json")).unwrap();
        let tail = Arc::new(std::sync::Barrier::new(2));
        owner.stall_after_notifier(Arc::clone(&tail));
        let (notified, notification) = mpsc::sync_channel(1);
        owner.set_completion_notifier(Arc::new(move || notified.send(()).unwrap()));
        let acknowledgement = owner
            .patch(
                0,
                PreferencesPatch {
                    chat_text_size_px: Some(15),
                    ..PreferencesPatch::default()
                },
            )
            .unwrap();
        notification
            .recv_timeout(Duration::from_secs(2))
            .expect("ordinary completion notification was not delivered");

        let started = std::time::Instant::now();
        assert!(!owner.finish_notified_write());
        assert!(started.elapsed() < Duration::from_millis(25));
        assert!(acknowledgement.result().is_none());

        tail.wait();
        tail.wait();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !owner.finish_notified_write() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(acknowledgement.result().unwrap().unwrap().revision, 1);
    }

    #[test]
    fn completion_notifier_with_a_weak_owner_does_not_retain_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let owner = PreferencesOwner::open(&directory.path().join("preferences.json")).unwrap();
        let weak = owner.downgrade();
        let notification_owner = weak.clone();
        owner.set_completion_notifier(Arc::new(move || {
            let _ = notification_owner.upgrade();
        }));

        assert!(weak.upgrade().is_some());
        drop(owner);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn exit_waits_off_thread_for_the_exact_pending_write() {
        let directory = tempfile::tempdir().unwrap();
        let owner = PreferencesOwner::open(&directory.path().join("preferences.json")).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        owner.stall_next(Arc::clone(&barrier));
        let acknowledgement = owner.patch(0, all_fields()).unwrap();
        barrier.wait();
        assert_eq!(owner.begin_drain(), PreferencesExit::Pending);
        let (sent, received) = mpsc::sync_channel(1);
        let resolver = {
            let owner = owner.clone();
            std::thread::spawn(move || sent.send(owner.resolve_exit_blocking()).unwrap())
        };
        assert!(matches!(
            received.recv_timeout(Duration::from_millis(25)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        barrier.wait();
        received
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        resolver.join().unwrap();
        assert_eq!(acknowledgement.result().unwrap().unwrap().revision, 1);
        owner.finish_ready_drain().unwrap();
    }

    #[test]
    fn failed_and_uncertain_exit_writes_retain_the_exact_candidate_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preferences.json");
        let owner = PreferencesOwner::open(&path).unwrap();
        owner.fail_next_before_rename();
        let failed = owner.patch(0, all_fields()).unwrap();
        assert_eq!(owner.begin_drain(), PreferencesExit::Pending);
        assert!(matches!(
            owner.resolve_exit_blocking(),
            Err(PreferenceError {
                kind: PreferenceErrorKind::Unavailable,
                ..
            })
        ));
        assert!(failed.result().unwrap().is_err());
        assert_eq!(owner.retry_for_exit().unwrap(), PreferencesExit::Pending);
        owner.resolve_exit_blocking().unwrap();
        assert_eq!(owner.snapshot().revision, 1);

        let owner = PreferencesOwner::open(&path).unwrap();
        owner.fail_next_after_rename();
        let uncertain = owner
            .patch(
                1,
                PreferencesPatch {
                    appearance: Some(Appearance::Light),
                    ..PreferencesPatch::default()
                },
            )
            .unwrap();
        assert_eq!(owner.begin_drain(), PreferencesExit::Pending);
        assert!(matches!(
            owner.resolve_exit_blocking(),
            Err(PreferenceError {
                kind: PreferenceErrorKind::OutcomeUnknown,
                ..
            })
        ));
        assert!(uncertain.result().unwrap().is_err());
        let candidate = fs::read(&path).unwrap();
        let mismatched = br#"{"version":1,"revision":1,"appearance":"dark","chat_text_size_px":18,"motion":"reduce","spellcheck":false,"send_key":"command_enter","reading_width":"wide","tail_follow":false}"#;
        fs::write(&path, mismatched).unwrap();
        assert_eq!(owner.retry_for_exit().unwrap(), PreferencesExit::Pending);
        assert!(matches!(
            owner.resolve_exit_blocking(),
            Err(PreferenceError {
                kind: PreferenceErrorKind::OutcomeUnknown,
                ..
            })
        ));
        assert_eq!(fs::read(&path).unwrap(), mismatched);

        fs::write(&path, candidate).unwrap();
        assert_eq!(owner.retry_for_exit().unwrap(), PreferencesExit::Pending);
        owner.resolve_exit_blocking().unwrap();
        assert_eq!(owner.snapshot().revision, 2);
        assert_eq!(owner.snapshot().appearance, Appearance::Light);
    }
}
