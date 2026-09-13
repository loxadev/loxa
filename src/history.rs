use loxa_ipc::{ConversationProfile, DraftReply, GenerationSettings, HistoryReply, HistoryStatus};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};

mod admission;
mod content;
mod conversations;
mod drafts;
mod identity;
mod prompt;
mod purge;
mod reads;
mod recovery;
mod schema;
mod worker;

pub(crate) use admission::{
    AdmissionKind, CommittedAdmission, DraftSubmission, PreparedAdmission, PromptBasis,
    PromptReference,
};
#[cfg(test)]
pub(crate) use content::ContentRange;
pub(crate) use content::{
    ExecutionOutcome, FinalizationInput, SuffixCommit, SuffixInput, MAX_SUFFIX_BYTES,
};
pub(crate) use identity::{decode_id, encode_id, parse_revision};
#[cfg(test)]
pub(crate) use prompt::PromptMessage;
pub(crate) use prompt::{PromptPreparation, PromptRole};

const COMMAND_CAPACITY: usize = 12;
const ORDINARY_CAPACITY: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoryErrorKind {
    NotFound,
    Conflict,
    Busy,
    LimitExceeded,
    InvalidInput,
    UnsupportedSchema,
    UnsafePath,
    Corrupt,
    ReadOnly,
    DiskFull,
    Io,
    Interrupted,
    WorkerUnavailable,
    OutcomeUnknown,
}

#[derive(Debug)]
pub(crate) struct HistoryError {
    kind: HistoryErrorKind,
    context: String,
}

impl HistoryError {
    pub(crate) fn new(kind: HistoryErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }

    pub(crate) fn kind(&self) -> HistoryErrorKind {
        self.kind
    }

    pub(crate) fn context(&self) -> &str {
        &self.context
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoryExit {
    Running,
    Drained,
    Failed,
}

#[derive(Debug)]
pub(crate) struct HistoryCompletion {
    pub(crate) result: Result<HistoryReply, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct DraftCompletion {
    pub(crate) result: Result<DraftReply, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct ProfileCompletion {
    pub(crate) result: Result<ConversationProfile, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct PromptCompletion {
    pub(crate) result: Result<PromptPreparation, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct AdmissionCompletion {
    pub(crate) result: Result<CommittedAdmission, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct RequiredCompletion {
    pub(crate) result: Result<(), HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct SuffixCompletion {
    pub(crate) result: Result<SuffixCommit, HistoryError>,
    pub(crate) permit: OwnedSemaphorePermit,
}

enum HistoryCommand {
    Execute {
        operation: loxa_ipc::HistoryCommand,
        generation: Option<GenerationSettings>,
        reply: oneshot::Sender<HistoryCompletion>,
        permit: OwnedSemaphorePermit,
    },
    ExecuteProfile {
        operation: loxa_ipc::ServiceSettingsCommand,
        reset_default: Option<GenerationSettings>,
        reply: oneshot::Sender<ProfileCompletion>,
        permit: OwnedSemaphorePermit,
    },
    ExecuteDraft {
        operation: loxa_ipc::DraftCommand,
        reply: oneshot::Sender<DraftCompletion>,
        permit: OwnedSemaphorePermit,
    },
    PreparePrompt {
        conversation_id: [u8; 16],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
        current_user_text: String,
        reply: oneshot::Sender<PromptCompletion>,
        permit: OwnedSemaphorePermit,
    },
    LookupSubmission {
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        reply: oneshot::Sender<Result<Option<CommittedAdmission>, HistoryError>>,
        permit: OwnedSemaphorePermit,
    },
    ReconcileSubmission {
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        reply: oneshot::Sender<Result<Option<CommittedAdmission>, HistoryError>>,
        permit: OwnedSemaphorePermit,
    },
    Admit {
        prepared: Arc<PreparedAdmission>,
        reply: oneshot::Sender<AdmissionCompletion>,
        permit: OwnedSemaphorePermit,
    },
    StopBeforeExecution {
        committed: CommittedAdmission,
        reply: oneshot::Sender<RequiredCompletion>,
        permit: OwnedSemaphorePermit,
    },
    AppendSuffix {
        input: Arc<SuffixInput>,
        reply: oneshot::Sender<SuffixCompletion>,
        permit: OwnedSemaphorePermit,
    },
    Finalize {
        input: Arc<FinalizationInput>,
        reply: oneshot::Sender<SuffixCompletion>,
        permit: OwnedSemaphorePermit,
    },
    Drain,
    #[cfg(test)]
    Stall(Arc<std::sync::Barrier>),
    #[cfg(test)]
    FailNextClose,
    #[cfg(test)]
    SetAdmissionCommitBarrier {
        barrier: Arc<std::sync::Barrier>,
        ready: SyncSender<()>,
    },
    #[cfg(test)]
    DropNextAdmissionReply(SyncSender<()>),
    #[cfg(test)]
    DropNextPersistenceReply(SyncSender<()>),
    #[cfg(test)]
    FailNextStopBeforeExecution(SyncSender<()>),
    #[cfg(test)]
    SetProgressInterval {
        instructions: i32,
        ready: SyncSender<()>,
    },
}

#[derive(Clone)]
pub(crate) struct HistoryHandle {
    commands: SyncSender<HistoryCommand>,
    ordinary: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    persistence: Arc<Semaphore>,
    draining: Arc<AtomicBool>,
    lifecycle_active: Arc<Mutex<bool>>,
    #[cfg(test)]
    drain_dispatches: Arc<AtomicUsize>,
    status: watch::Sender<HistoryStatus>,
    exit: watch::Sender<HistoryExit>,
}

pub(crate) struct HistoryOwner {
    handle: HistoryHandle,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl HistoryOwner {
    pub(crate) fn start(
        root: &Path,
        models_root: PathBuf,
        runtime_identity: crate::runtime_identity::RuntimeIdentity,
        draining: Arc<AtomicBool>,
        owner_epoch: String,
    ) -> Result<Self, HistoryError> {
        let (commands, receiver) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (status, _) = watch::channel(HistoryStatus::opening());
        let (exit, _) = watch::channel(HistoryExit::Running);
        let handle = HistoryHandle {
            commands,
            ordinary: Arc::new(Semaphore::new(ORDINARY_CAPACITY)),
            admission: Arc::new(Semaphore::new(1)),
            persistence: Arc::new(Semaphore::new(2)),
            draining,
            lifecycle_active: Arc::new(Mutex::new(false)),
            #[cfg(test)]
            drain_dispatches: Arc::new(AtomicUsize::new(0)),
            status,
            exit,
        };
        let worker_handle = handle.clone();
        let root = root.to_owned();
        let thread = std::thread::Builder::new()
            .name("loxa-service-history-owner".into())
            .spawn(move || {
                worker::run(
                    worker_handle,
                    receiver,
                    root,
                    models_root,
                    runtime_identity,
                    owner_epoch,
                )
            })
            .map_err(|error| {
                HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    format!("could not start history owner: {error}"),
                )
            })?;
        Ok(Self {
            handle,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(crate) fn handle(&self) -> HistoryHandle {
        self.handle.clone()
    }

    pub(crate) fn join(&self) -> Result<(), HistoryError> {
        let thread = self
            .thread
            .lock()
            .map_err(|_| {
                HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner join lock is poisoned",
                )
            })?
            .take();
        if let Some(thread) = thread {
            thread.join().map_err(|_| {
                HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner thread panicked",
                )
            })?;
        }
        Ok(())
    }
}

impl HistoryHandle {
    pub(crate) fn status(&self) -> HistoryStatus {
        self.status.borrow().clone()
    }

    pub(crate) fn exit_receiver(&self) -> watch::Receiver<HistoryExit> {
        self.exit.subscribe()
    }

    #[cfg(test)]
    pub(crate) async fn execute(
        &self,
        operation: loxa_ipc::HistoryCommand,
    ) -> Result<HistoryCompletion, HistoryError> {
        self.execute_with_generation(operation, None).await
    }

    pub(crate) async fn execute_with_generation(
        &self,
        operation: loxa_ipc::HistoryCommand,
        generation: Option<GenerationSettings>,
    ) -> Result<HistoryCompletion, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.ordinary)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history capacity is full"))?;
        let (reply, completion) = oneshot::channel();
        let command = HistoryCommand::Execute {
            operation,
            generation,
            reply,
            permit,
        };
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        completion.await.map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history owner stopped before completing the request",
            )
        })
    }

    pub(crate) async fn execute_profile(
        &self,
        operation: loxa_ipc::ServiceSettingsCommand,
        reset_default: Option<GenerationSettings>,
    ) -> Result<ProfileCompletion, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.ordinary)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history capacity is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::ExecuteProfile {
                operation,
                reset_default,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        completion.await.map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history owner stopped before completing the profile request",
            )
        })
    }

    pub(crate) async fn execute_draft(
        &self,
        operation: loxa_ipc::DraftCommand,
    ) -> Result<DraftCompletion, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.ordinary)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history capacity is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::ExecuteDraft {
                operation,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        completion.await.map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history owner stopped before completing the request",
            )
        })
    }

    pub(crate) async fn lookup_submission(
        &self,
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
    ) -> Result<Option<CommittedAdmission>, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.ordinary)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history capacity is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::LookupSubmission {
                submission_id,
                submission_hash,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        let result = completion.await.map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history owner stopped before submission lookup completed",
            )
        })?;
        result
    }

    pub(crate) async fn prepare_prompt(
        &self,
        conversation_id: [u8; 16],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
        current_user_text: String,
    ) -> Result<PromptCompletion, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.ordinary)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history capacity is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::PreparePrompt {
                conversation_id,
                expected_conversation_revision,
                expected_profile_revision,
                current_user_text,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        completion.await.map_err(|_| {
            HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history owner stopped before prompt preparation completed",
            )
        })
    }

    pub(crate) fn try_admit(
        &self,
        prepared: Arc<PreparedAdmission>,
    ) -> Result<oneshot::Receiver<AdmissionCompletion>, HistoryError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(HistoryError::new(
                HistoryErrorKind::WorkerUnavailable,
                "history is draining",
            ));
        }
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history admission is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::Admit {
                prepared,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::WorkerUnavailable,
                    "history owner is unavailable",
                ),
            })?;
        Ok(completion)
    }

    pub(crate) fn try_reconcile_submission(
        &self,
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
    ) -> Result<oneshot::Receiver<Result<Option<CommittedAdmission>, HistoryError>>, HistoryError>
    {
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| HistoryError::new(HistoryErrorKind::Busy, "history admission is full"))?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::ReconcileSubmission {
                submission_id,
                submission_hash,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::OutcomeUnknown,
                    "history owner cannot reconcile the admission",
                ),
            })?;
        Ok(completion)
    }

    pub(crate) fn try_stop_before_execution(
        &self,
        committed: CommittedAdmission,
    ) -> Result<oneshot::Receiver<RequiredCompletion>, HistoryError> {
        let permit = Arc::clone(&self.persistence)
            .try_acquire_owned()
            .map_err(|_| {
                HistoryError::new(HistoryErrorKind::Busy, "history terminal slot is full")
            })?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::StopBeforeExecution {
                committed,
                reply,
                permit,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
                }
                TrySendError::Disconnected(_) => HistoryError::new(
                    HistoryErrorKind::OutcomeUnknown,
                    "history owner cannot save cancelled admission",
                ),
            })?;
        Ok(completion)
    }

    pub(crate) fn try_append_suffix(
        &self,
        input: Arc<SuffixInput>,
    ) -> Result<oneshot::Receiver<SuffixCompletion>, HistoryError> {
        input.validate(false)?;
        let permit = Arc::clone(&self.persistence)
            .try_acquire_owned()
            .map_err(|_| {
                HistoryError::new(HistoryErrorKind::Busy, "history suffix slots are full")
            })?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::AppendSuffix {
                input,
                reply,
                permit,
            })
            .map_err(required_send_error)?;
        Ok(completion)
    }

    pub(crate) fn try_finalize(
        &self,
        input: Arc<FinalizationInput>,
    ) -> Result<oneshot::Receiver<SuffixCompletion>, HistoryError> {
        input.validate()?;
        let permit = Arc::clone(&self.persistence)
            .try_acquire_owned()
            .map_err(|_| {
                HistoryError::new(HistoryErrorKind::Busy, "history terminal slot is full")
            })?;
        let (reply, completion) = oneshot::channel();
        self.commands
            .try_send(HistoryCommand::Finalize {
                input,
                reply,
                permit,
            })
            .map_err(required_send_error)?;
        Ok(completion)
    }

    pub(crate) fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
        let mut active = self
            .lifecycle_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.status();
        if *active {
            return;
        }
        *active = true;
        self.status.send_replace(HistoryStatus::flush_pending(
            current.schema_version,
            current.sqlite_version,
            current.sqlite_source_id,
        ));
        if self.commands.try_send(HistoryCommand::Drain).is_err() {
            *active = false;
            self.status.send_replace(HistoryStatus::flush_failed(
                current.schema_version,
                "history owner could not accept the drain request",
            ));
        } else {
            #[cfg(test)]
            self.drain_dispatches.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn close_failed(&self, schema_version: u32, context: impl Into<String>) {
        let mut active = self
            .lifecycle_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = false;
        self.status
            .send_replace(HistoryStatus::flush_failed(schema_version, context));
    }

    #[cfg(test)]
    pub(crate) fn stall(&self, barrier: Arc<std::sync::Barrier>) {
        self.commands
            .try_send(HistoryCommand::Stall(barrier))
            .expect("history test stall must fit the bounded queue");
    }

    #[cfg(test)]
    pub(crate) fn ordinary_available(&self) -> usize {
        self.ordinary.available_permits()
    }

    #[cfg(test)]
    pub(crate) fn fail_next_close(&self) {
        self.commands
            .try_send(HistoryCommand::FailNextClose)
            .expect("history close fault must fit the bounded queue");
    }

    #[cfg(test)]
    pub(crate) fn set_admission_commit_barrier(&self, barrier: Arc<std::sync::Barrier>) {
        let (ready, received) = mpsc::sync_channel(0);
        self.commands
            .try_send(HistoryCommand::SetAdmissionCommitBarrier { barrier, ready })
            .expect("history admission commit hook must fit the bounded queue");
        received
            .recv()
            .expect("history owner must install the admission commit hook");
    }

    #[cfg(test)]
    pub(crate) fn drop_next_admission_reply(&self) {
        let (ready, received) = mpsc::sync_channel(0);
        self.commands
            .try_send(HistoryCommand::DropNextAdmissionReply(ready))
            .expect("history admission reply hook must fit the bounded queue");
        received
            .recv()
            .expect("history owner must install the admission reply hook");
    }

    #[cfg(test)]
    pub(crate) fn fail_next_stop_before_execution(&self) {
        let (ready, received) = mpsc::sync_channel(0);
        self.commands
            .try_send(HistoryCommand::FailNextStopBeforeExecution(ready))
            .expect("history terminal fault hook must fit the bounded queue");
        received
            .recv()
            .expect("history owner must install the terminal fault hook");
    }

    #[cfg(test)]
    pub(crate) fn drop_next_persistence_reply(&self) {
        let (ready, received) = mpsc::sync_channel(0);
        self.commands
            .try_send(HistoryCommand::DropNextPersistenceReply(ready))
            .expect("history persistence reply hook must fit the bounded queue");
        received
            .recv()
            .expect("history owner must install the persistence reply hook");
    }

    #[cfg(test)]
    pub(crate) fn set_progress_interval(&self, instructions: i32) {
        let (ready, received) = mpsc::sync_channel(0);
        self.commands
            .try_send(HistoryCommand::SetProgressInterval {
                instructions,
                ready,
            })
            .expect("history progress hook must fit the bounded queue");
        received
            .recv()
            .expect("history owner must install the progress hook");
    }

    #[cfg(test)]
    pub(crate) fn drain_dispatches(&self) -> usize {
        self.drain_dispatches.load(Ordering::Relaxed)
    }
}

fn required_send_error<T>(error: TrySendError<T>) -> HistoryError {
    match error {
        TrySendError::Full(_) => {
            HistoryError::new(HistoryErrorKind::Busy, "history capacity is full")
        }
        TrySendError::Disconnected(_) => HistoryError::new(
            HistoryErrorKind::OutcomeUnknown,
            "history owner cannot determine the durable outcome",
        ),
    }
}

#[cfg(test)]
mod tests;
