use super::{
    admission, conversations, drafts, prompt, reads, schema, AdmissionCompletion, AdmissionKind,
    AttemptCompletion, DraftCompletion, HistoryCommand, HistoryCompletion, HistoryErrorKind,
    HistoryExit, HistoryHandle, ProfileCompletion, PromptCompletion, SuffixCompletion,
};
use loxa_ipc::HistoryStatus;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvError};
use std::sync::Arc;

pub(super) fn run(
    handle: HistoryHandle,
    receiver: Receiver<HistoryCommand>,
    root: PathBuf,
    models_root: PathBuf,
    runtime_identity: crate::runtime_identity::RuntimeIdentity,
    owner_epoch: String,
) {
    let completion = WorkerCompletion::new(handle.clone());
    let interrupt_on_drain = Arc::new(AtomicBool::new(true));
    let mut connection = match schema::open_store(
        &root,
        handle.draining.clone(),
        Arc::clone(&interrupt_on_drain),
    ) {
        Ok((mut connection, info)) => {
            let recovery =
                super::recovery::recover_interrupted(&mut connection, &owner_epoch).map(|_| ());
            let recovery = recovery.and_then(|()| loop {
                match conversations::resume_delete(&mut connection) {
                    Ok(true) => break Ok(()),
                    Ok(false) => {}
                    Err(error) => break Err(error),
                }
            });
            match recovery {
                Ok(()) => {
                    handle.status.send_replace(HistoryStatus::ready(
                        info.schema_version,
                        info.sqlite_version,
                        info.sqlite_source_id,
                    ));
                    Some(WorkerStore::Ready(connection))
                }
                Err(error) => {
                    handle
                        .status
                        .send_replace(HistoryStatus::unavailable(error.context()));
                    Some(WorkerStore::Unavailable(connection))
                }
            }
        }
        Err(error) => {
            handle
                .status
                .send_replace(HistoryStatus::unavailable(error.context()));
            error
                .connection
                .map(|connection| WorkerStore::Unavailable(*connection))
        }
    };

    #[cfg(test)]
    let mut fail_next_close = false;
    #[cfg(test)]
    let mut admission_commit_barrier: Option<std::sync::Arc<std::sync::Barrier>> = None;
    #[cfg(test)]
    let mut drop_next_admission_reply = false;
    #[cfg(test)]
    let mut drop_next_persistence_reply = false;
    let mut pending = VecDeque::with_capacity(super::COMMAND_CAPACITY);
    loop {
        match next_command(&receiver, &mut pending) {
            Ok(HistoryCommand::Execute {
                operation,
                generation,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => conversations::execute_with_generation(
                        store,
                        &models_root,
                        runtime_identity,
                        operation,
                        generation,
                    ),
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(HistoryCompletion { result, permit });
            }
            Ok(HistoryCommand::ExecuteProfile {
                operation,
                reset_default,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => {
                        conversations::execute_profile(store, operation, reset_default)
                    }
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(ProfileCompletion { result, permit });
            }
            Ok(HistoryCommand::ReadObservedAttempt {
                observation,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_ref() {
                    Some(WorkerStore::Ready(store)) => {
                        reads::get_observed_attempt(store, &observation)
                    }
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(AttemptCompletion { result, permit });
            }
            Ok(HistoryCommand::Drain) => match connection.take() {
                #[cfg(test)]
                Some(store) if fail_next_close => {
                    fail_next_close = false;
                    connection = Some(store);
                    let schema_version = handle.status.borrow().schema_version;
                    handle.close_failed(
                        schema_version,
                        "history close failed: injected close failure",
                    );
                }
                Some(store) => match store.into_connection().close() {
                    Ok(()) => {
                        completion.drained();
                        return;
                    }
                    Err((store, error)) => {
                        connection = Some(WorkerStore::Unavailable(store));
                        let schema_version = handle.status.borrow().schema_version;
                        handle
                            .close_failed(schema_version, format!("history close failed: {error}"));
                    }
                },
                None => {
                    completion.drained();
                    return;
                }
            },
            Ok(HistoryCommand::ExecuteDraft {
                operation,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => drafts::execute(store, operation),
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(DraftCompletion { result, permit });
            }
            Ok(HistoryCommand::PreparePrompt {
                conversation_id,
                expected_conversation_revision,
                expected_profile_revision,
                request,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_ref() {
                    Some(WorkerStore::Ready(store)) => prompt::prepare(
                        store,
                        conversation_id,
                        expected_conversation_revision,
                        expected_profile_revision,
                        request,
                    ),
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(PromptCompletion { result, permit });
            }
            Ok(HistoryCommand::LookupSubmission {
                submission_id,
                submission_hash,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(true, Ordering::Release);
                let result = match connection.as_ref() {
                    Some(WorkerStore::Ready(store)) => {
                        admission::lookup_submission(store, submission_id, submission_hash)
                    }
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::WorkerUnavailable,
                        "history store is unavailable",
                    )),
                };
                let _ = reply.send(result);
                drop(permit);
            }
            Ok(HistoryCommand::ReconcileSubmission {
                submission_id,
                submission_hash,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(false, Ordering::Release);
                let result = match connection.as_ref() {
                    Some(WorkerStore::Ready(store)) => {
                        admission::reconcile_submission(store, submission_id, submission_hash)
                    }
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::OutcomeUnknown,
                        "history owner cannot reconcile the admission",
                    )),
                };
                let _ = reply.send(result);
                drop(permit);
            }
            Ok(HistoryCommand::Admit {
                prepared,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(false, Ordering::Release);
                #[cfg(test)]
                let commit_barrier = admission_commit_barrier.take();
                #[cfg(test)]
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => match prepared.kind() {
                        AdmissionKind::Send { .. } => admission::admit_send_with_commit_barrier(
                            store,
                            &prepared,
                            commit_barrier.as_deref(),
                        ),
                        AdmissionKind::Retry { .. } => admission::admit_retry(store, &prepared),
                    },
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::OutcomeUnknown,
                        "history owner cannot determine the admission outcome",
                    )),
                };
                #[cfg(not(test))]
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => match prepared.kind() {
                        AdmissionKind::Send { .. } => admission::admit_send(store, &prepared),
                        AdmissionKind::Retry { .. } => admission::admit_retry(store, &prepared),
                    },
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::OutcomeUnknown,
                        "history owner cannot determine the admission outcome",
                    )),
                };
                #[cfg(test)]
                if drop_next_admission_reply {
                    drop_next_admission_reply = false;
                    drop(result);
                    drop(permit);
                } else {
                    let _ = reply.send(AdmissionCompletion { result, permit });
                }
                #[cfg(not(test))]
                let _ = reply.send(AdmissionCompletion { result, permit });
            }
            Ok(HistoryCommand::AppendSuffix {
                input,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(false, Ordering::Release);
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => super::content::append_suffix(store, &input),
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::OutcomeUnknown,
                        "history owner cannot determine the suffix outcome",
                    )),
                };
                drop(input);
                #[cfg(test)]
                if drop_next_persistence_reply {
                    drop_next_persistence_reply = false;
                    drop(result);
                    drop(permit);
                } else {
                    let _ = reply.send(SuffixCompletion { result, permit });
                }
                #[cfg(not(test))]
                let _ = reply.send(SuffixCompletion { result, permit });
            }
            Ok(HistoryCommand::Finalize {
                input,
                reply,
                permit,
            }) => {
                interrupt_on_drain.store(false, Ordering::Release);
                let result = match connection.as_mut() {
                    Some(WorkerStore::Ready(store)) => super::content::finalize(store, &input),
                    Some(WorkerStore::Unavailable(_)) | None => Err(super::HistoryError::new(
                        HistoryErrorKind::OutcomeUnknown,
                        "history owner cannot determine the finalization outcome",
                    )),
                };
                drop(input);
                #[cfg(test)]
                if drop_next_persistence_reply {
                    drop_next_persistence_reply = false;
                    drop(result);
                    drop(permit);
                } else {
                    let _ = reply.send(SuffixCompletion { result, permit });
                }
                #[cfg(not(test))]
                let _ = reply.send(SuffixCompletion { result, permit });
            }
            #[cfg(test)]
            Ok(HistoryCommand::Stall(barrier)) => {
                barrier.wait();
                barrier.wait();
            }
            #[cfg(test)]
            Ok(HistoryCommand::FailNextClose) => fail_next_close = true,
            #[cfg(test)]
            Ok(HistoryCommand::SetAdmissionCommitBarrier { barrier, ready }) => {
                admission_commit_barrier = Some(barrier);
                let _ = ready.send(());
            }
            #[cfg(test)]
            Ok(HistoryCommand::DropNextAdmissionReply(ready)) => {
                drop_next_admission_reply = true;
                let _ = ready.send(());
            }
            #[cfg(test)]
            Ok(HistoryCommand::DropNextPersistenceReply(ready)) => {
                drop_next_persistence_reply = true;
                let _ = ready.send(());
            }
            #[cfg(test)]
            Ok(HistoryCommand::SetProgressInterval {
                instructions,
                ready,
            }) => {
                if let Some(store) = connection.as_ref() {
                    let _ = schema::install_progress_handler(
                        store.connection(),
                        instructions,
                        handle.draining.clone(),
                        Arc::clone(&interrupt_on_drain),
                    );
                }
                let _ = ready.send(());
            }
            Err(RecvError) => {
                if let Some(store) = connection.take() {
                    if store.into_connection().close().is_err() {
                        return;
                    }
                }
                completion.drained();
                return;
            }
        }
    }
}

fn next_command(
    receiver: &Receiver<HistoryCommand>,
    pending: &mut VecDeque<HistoryCommand>,
) -> Result<HistoryCommand, RecvError> {
    if pending.is_empty() {
        pending.push_back(receiver.recv()?);
    }
    while pending.len() < super::COMMAND_CAPACITY {
        let Ok(command) = receiver.try_recv() else {
            break;
        };
        pending.push_back(command);
    }
    let index = pending.iter().position(is_required).unwrap_or_default();
    Ok(pending
        .remove(index)
        .expect("pending history command exists"))
}

fn is_required(command: &HistoryCommand) -> bool {
    matches!(
        command,
        HistoryCommand::Admit { .. }
            | HistoryCommand::ReconcileSubmission { .. }
            | HistoryCommand::AppendSuffix { .. }
            | HistoryCommand::Finalize { .. }
    )
}

enum WorkerStore {
    Ready(rusqlite::Connection),
    Unavailable(rusqlite::Connection),
}

impl WorkerStore {
    #[cfg(test)]
    fn connection(&self) -> &rusqlite::Connection {
        match self {
            Self::Ready(connection) | Self::Unavailable(connection) => connection,
        }
    }

    fn into_connection(self) -> rusqlite::Connection {
        match self {
            Self::Ready(connection) | Self::Unavailable(connection) => connection,
        }
    }
}

struct WorkerCompletion {
    handle: HistoryHandle,
    complete: bool,
}

impl WorkerCompletion {
    fn new(handle: HistoryHandle) -> Self {
        Self {
            handle,
            complete: false,
        }
    }

    fn drained(mut self) {
        self.complete = true;
        self.handle.exit.send_replace(HistoryExit::Drained);
    }
}

impl Drop for WorkerCompletion {
    fn drop(&mut self) {
        if !self.complete {
            self.handle.status.send_replace(HistoryStatus::unavailable(
                "history owner stopped unexpectedly",
            ));
            self.handle.exit.send_replace(HistoryExit::Failed);
        }
    }
}
