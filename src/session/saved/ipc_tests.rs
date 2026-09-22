use super::conversation::{AcceptedAttempt, Conversation};
use super::output::TerminalOutput;
use super::signals::SessionSignals;
use super::{run_generation, submit, ChatError};
use futures_util::{SinkExt, StreamExt};
use loxa_ipc::{
    decode_with_limit, encode_with_limit, framed, initialize_development_root, set_frame_limit,
    AttemptExecution, AttemptSave, AttemptSummary, Capability, ClientEnvelope, ClientError,
    ConnectMode, ContentRange, ContentSource, ErrorCategory, GenerationAccepted, GenerationCommand,
    GenerationExecutionPhase, GenerationHelloAck, GenerationObservation, GenerationReply,
    GenerationSavePhase, GenerationStatus, GenerationTarget, HelloAck, HistoryCommand,
    HistoryReply, OperationTarget, ProtocolVersion, Reply, ReplyOutcome, ServerEnvelope,
    ServiceClient, ServiceCommand, ServiceError, HISTORY_SCHEMA_VERSION, MAX_FRAME_BYTES,
    MAX_HISTORY_FRAME_BYTES,
};
use std::fs;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};

const BOOT: &str = "fake-boot";
const CONVERSATION: &str = "11111111111111111111111111111111";
const SEND_ATTEMPT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const RETRY_ATTEMPT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn operation_target() -> OperationTarget {
    OperationTarget {
        boot_epoch: BOOT.into(),
        task_id: "1".into(),
        generation: "1".into(),
    }
}

#[derive(Clone, Copy)]
enum Mode {
    OldPeer,
    ReopenedAck,
    Replay,
    Rejected,
    PendingStop,
    AcceptedStop,
    EarlySaveFailure,
    Retry,
    Failed,
    Stopped,
    ObserverFailure,
    OutputFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Seen {
    Generation(GenerationCommand),
    Subscribe(GenerationTarget, String),
    Range(ContentSource, String, String),
    EarlySaveFailure,
    TerminalSaveFailure,
}

struct State {
    mode: Mode,
    seen: Vec<Seen>,
    sends: usize,
    pending_nonces: u64,
    errors: Vec<String>,
}

struct FakePeer {
    _directory: TempDir,
    root: PathBuf,
    client: ServiceClient,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    terminal_release: Arc<Notify>,
    server: tokio::task::JoinHandle<()>,
}

impl FakePeer {
    async fn start(mode: Mode) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("ls-")
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(directory.path()).unwrap().join("dev");
        let forbidden = fs::canonicalize(directory.path()).unwrap().join("normal");
        fs::create_dir(&forbidden).unwrap();
        let executable = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let bootstrap =
            initialize_development_root(&root, &forbidden, &executable, crate::service::BUILD_ID)
                .unwrap();
        let listener = UnixListener::bind(bootstrap.root().socket_path()).unwrap();
        fs::set_permissions(
            bootstrap.root().socket_path(),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let client = ServiceClient::load(&root, None, crate::service::BUILD_ID).unwrap();
        let state = Arc::new(Mutex::new(State {
            mode,
            seen: Vec::new(),
            sends: 0,
            pending_nonces: 0,
            errors: Vec::new(),
        }));
        let changed = Arc::new(Notify::new());
        let stopped_pending = Arc::new(Notify::new());
        let terminal_release = Arc::new(Notify::new());
        let server_state = Arc::clone(&state);
        let server_changed = Arc::clone(&changed);
        let server_terminal_release = Arc::clone(&terminal_release);
        let server = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let bootstrap = bootstrap.clone();
                        let state = Arc::clone(&server_state);
                        let changed = Arc::clone(&server_changed);
                        let stopped_pending = Arc::clone(&stopped_pending);
                        let terminal_release = Arc::clone(&server_terminal_release);
                        handlers.spawn(async move { handle(stream, &bootstrap, state, changed, stopped_pending, terminal_release).await });
                    }
                    result = handlers.join_next(), if !handlers.is_empty() => {
                        if let Some(result) = result {
                            let error = match result {
                                Ok(Ok(())) => continue,
                                Ok(Err(error)) => error,
                                Err(error) => error.to_string(),
                            };
                            server_state.lock().await.errors.push(error);
                            server_changed.notify_waiters();
                        }
                    }
                }
            }
        });
        Self {
            _directory: directory,
            root,
            client,
            state,
            changed,
            terminal_release,
            server,
        }
    }

    async fn seen(&self) -> Vec<Seen> {
        let state = self.state.lock().await;
        assert!(
            state.errors.is_empty(),
            "fake peer errors: {:?}",
            state.errors
        );
        state.seen.clone()
    }

    async fn wait_for(&self, predicate: impl Fn(&[Seen]) -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let state = self.state.lock().await;
                assert!(
                    state.errors.is_empty(),
                    "fake peer errors: {:?}",
                    state.errors
                );
                if predicate(&state.seen) {
                    return;
                }
                drop(state);
                notified.await;
            }
        })
        .await
        .expect("fake peer did not observe the expected client operation");
    }
}

impl Drop for FakePeer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn record(state: &Arc<Mutex<State>>, changed: &Notify, item: Seen) {
    state.lock().await.seen.push(item);
    changed.notify_waiters();
}

async fn handle(
    stream: UnixStream,
    bootstrap: &loxa_ipc::ClientBootstrap,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    stopped_pending: Arc<Notify>,
    terminal_release: Arc<Notify>,
) -> Result<(), String> {
    let mut transport = framed(stream);
    let ClientEnvelope::Hello(hello) = receive(&mut transport, MAX_FRAME_BYTES).await? else {
        return Err("expected hello".into());
    };
    if matches!(state.lock().await.mode, Mode::OldPeer) && hello.protocol == ProtocolVersion::V1_6 {
        send(
            &mut transport,
            &ServerEnvelope::HelloRejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "protocol 1.6 is unsupported",
            )),
            MAX_FRAME_BYTES,
        )
        .await?;
        return Ok(());
    }
    let generation = if hello
        .generation
        .is_some_and(|request| request.connection == loxa_ipc::GenerationConnection::Request)
    {
        let mut state = state.lock().await;
        state.pending_nonces += 1;
        Some(GenerationHelloAck {
            pending_nonce: format!("{:032x}", state.pending_nonces),
        })
    } else {
        None
    };
    let ack = HelloAck {
        protocol: hello.protocol,
        capabilities: vec![
            Capability::Status,
            Capability::Load,
            Capability::Unload,
            Capability::StopService,
            Capability::EngineUnixSocket,
            Capability::History,
            Capability::Drafts,
            Capability::Settings,
        ],
        build: crate::service::BUILD_ID.into(),
        storage_schema: HISTORY_SCHEMA_VERSION,
        boot_epoch: if matches!(state.lock().await.mode, Mode::ReopenedAck) {
            "new-boot"
        } else {
            BOOT
        }
        .into(),
        root_identity: bootstrap.root().root_identity().into(),
        service_pid: std::process::id(),
        origin_sha256: bootstrap.origin().executable_sha256().into(),
        generation,
    };
    send(
        &mut transport,
        &ServerEnvelope::HelloAck(ack),
        MAX_FRAME_BYTES,
    )
    .await?;
    let limit = if hello.protocol == ProtocolVersion::V1_0 {
        MAX_FRAME_BYTES
    } else {
        MAX_HISTORY_FRAME_BYTES
    };
    set_frame_limit(&mut transport, limit)?;
    match receive(&mut transport, limit).await? {
        ClientEnvelope::Request(request) => {
            let request_id = request.request_id;
            let outcome = match request.command {
                ServiceCommand::GenerationAt { target, command } => {
                    assert_eq!(target, operation_target());
                    record(&state, &changed, Seen::Generation(command.clone())).await;
                    match command {
                        command @ (GenerationCommand::Send { .. }
                        | GenerationCommand::Retry { .. }) => {
                            let (mode, count) = {
                                let mut state = state.lock().await;
                                state.sends += 1;
                                (state.mode, state.sends)
                            };
                            if matches!(mode, Mode::Replay) && count == 1 {
                                return Ok(());
                            }
                            if matches!(mode, Mode::PendingStop) {
                                tokio::time::timeout(
                                    std::time::Duration::from_secs(3),
                                    stopped_pending.notified(),
                                )
                                .await
                                .map_err(|_| "pending Stop did not arrive")?;
                            }
                            if matches!(mode, Mode::Rejected) {
                                ReplyOutcome::Rejected(ServiceError::new(
                                    ErrorCategory::Conflict,
                                    "revision changed",
                                ))
                            } else {
                                ReplyOutcome::Generation {
                                    reply: GenerationReply::Accepted(accepted(&command, &target)),
                                }
                            }
                        }
                        GenerationCommand::Stop { .. } => {
                            return Err("GenerationAt cannot Stop".into())
                        }
                    }
                }
                ServiceCommand::Generation {
                    command: GenerationCommand::Stop { target },
                } => {
                    record(
                        &state,
                        &changed,
                        Seen::Generation(GenerationCommand::Stop {
                            target: target.clone(),
                        }),
                    )
                    .await;
                    stopped_pending.notify_one();
                    ReplyOutcome::Generation {
                        reply: GenerationReply::Stopping { target },
                    }
                }
                ServiceCommand::History {
                    command: HistoryCommand::CreateConversation { model_id },
                } => ReplyOutcome::History {
                    reply: HistoryReply::Conversation(loxa_ipc::ConversationSummary {
                        id: CONVERSATION.into(),
                        model_id,
                        title: "New conversation".into(),
                        created_ms: "1".into(),
                        updated_ms: "1".into(),
                        revision: "1".into(),
                        profile_revision: "1".into(),
                    }),
                },
                ServiceCommand::Status => ReplyOutcome::Status(loxa_ipc::ServiceStatus {
                    runtime: loxa_ipc::RuntimeStatus {
                        boot_epoch: BOOT.into(),
                        state_revision: "1".into(),
                        phase: loxa_ipc::RuntimePhase::Ready {
                            task_id: "1".into(),
                            generation: "1".into(),
                            model_id: "demo".into(),
                            engine_pid: std::process::id(),
                        },
                    },
                    diagnostics: loxa_ipc::DiagnosticsStatus {
                        available: true,
                        enqueue_drops: 0,
                        sink_failures: 0,
                        sink_discards: 0,
                        at_capacity: false,
                        sink_failed: false,
                    },
                }),
                ServiceCommand::History {
                    command:
                        HistoryCommand::ReadContentRange {
                            source,
                            start,
                            prefix_end,
                        },
                } => {
                    record(
                        &state,
                        &changed,
                        Seen::Range(source, start.clone(), prefix_end.clone()),
                    )
                    .await;
                    ReplyOutcome::History {
                        reply: HistoryReply::ContentRange(ContentRange {
                            start,
                            end: "2".into(),
                            prefix_end,
                            content: "ok".into(),
                        }),
                    }
                }
                _ => return Err("unexpected fake peer request".into()),
            };
            send(
                &mut transport,
                &ServerEnvelope::Reply(Reply {
                    request_id,
                    outcome,
                }),
                limit,
            )
            .await?;
        }
        ClientEnvelope::SubscribeGeneration {
            target, attempt_id, ..
        } => {
            record(
                &state,
                &changed,
                Seen::Subscribe(target.clone(), attempt_id.clone()),
            )
            .await;
            let mode = state.lock().await.mode;
            if matches!(mode, Mode::ObserverFailure) {
                return Ok(());
            }
            let observation = match mode {
                Mode::Retry => GenerationObservation::Durable {
                    attempt: durable(&attempt_id),
                },
                Mode::AcceptedStop => GenerationObservation::Live {
                    status: status(
                        target.clone(),
                        &attempt_id,
                        GenerationExecutionPhase::Working,
                        GenerationSavePhase::Open,
                        "2",
                    ),
                },
                Mode::EarlySaveFailure => GenerationObservation::Live {
                    status: status(
                        target.clone(),
                        &attempt_id,
                        GenerationExecutionPhase::Cancelling,
                        GenerationSavePhase::SaveFailed,
                        "0",
                    ),
                },
                Mode::Failed | Mode::Stopped => GenerationObservation::Live {
                    status: status(
                        target.clone(),
                        &attempt_id,
                        if matches!(mode, Mode::Failed) {
                            GenerationExecutionPhase::Failed
                        } else {
                            GenerationExecutionPhase::Stopped
                        },
                        GenerationSavePhase::Saved,
                        "2",
                    ),
                },
                _ => GenerationObservation::Live {
                    status: status(
                        target.clone(),
                        &attempt_id,
                        GenerationExecutionPhase::Completed,
                        GenerationSavePhase::Saved,
                        "2",
                    ),
                },
            };
            send(
                &mut transport,
                &ServerEnvelope::GenerationSnapshot { observation },
                limit,
            )
            .await?;
            if matches!(mode, Mode::AcceptedStop) {
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    stopped_pending.notified(),
                )
                .await
                .map_err(|_| "accepted Stop did not arrive")?;
            }
            if matches!(mode, Mode::EarlySaveFailure) {
                record(&state, &changed, Seen::EarlySaveFailure).await;
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    terminal_release.notified(),
                )
                .await
                .map_err(|_| "terminal transition was not released")?;
                send(
                    &mut transport,
                    &ServerEnvelope::GenerationSnapshot {
                        observation: GenerationObservation::Live {
                            status: status(
                                target.clone(),
                                &attempt_id,
                                GenerationExecutionPhase::Stopped,
                                GenerationSavePhase::SaveFailed,
                                "0",
                            ),
                        },
                    },
                    limit,
                )
                .await?;
                record(&state, &changed, Seen::TerminalSaveFailure).await;
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    terminal_release.notified(),
                )
                .await
                .map_err(|_| "saved transition was not released")?;
                send(
                    &mut transport,
                    &ServerEnvelope::GenerationSnapshot {
                        observation: GenerationObservation::Live {
                            status: status(
                                target,
                                &attempt_id,
                                GenerationExecutionPhase::Stopped,
                                GenerationSavePhase::Saved,
                                "0",
                            ),
                        },
                    },
                    limit,
                )
                .await?;
            }
        }
        _ => return Err("unexpected fake peer envelope".into()),
    }
    Ok(())
}

fn accepted(command: &GenerationCommand, target: &OperationTarget) -> GenerationAccepted {
    let (conversation_id, submission_id, pre, profile, retry) = match command {
        GenerationCommand::Send {
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            ..
        } => (
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            false,
        ),
        GenerationCommand::Retry {
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            ..
        } => (
            conversation_id,
            submission_id,
            expected_conversation_revision,
            expected_profile_revision,
            true,
        ),
        GenerationCommand::Stop { .. } => unreachable!(),
    };
    GenerationAccepted {
        boot_epoch: target.boot_epoch.clone(),
        conversation_id: conversation_id.clone(),
        turn_id: "33333333333333333333333333333333".into(),
        attempt_id: if retry { RETRY_ATTEMPT } else { SEND_ATTEMPT }.into(),
        submission_id: submission_id.clone(),
        pre_conversation_revision: pre.clone(),
        post_conversation_revision: (pre.parse::<u64>().unwrap() + 1).to_string(),
        profile_revision: profile.clone(),
        operation_generation: target.generation.clone(),
    }
}

fn status(
    target: GenerationTarget,
    attempt_id: &str,
    execution: GenerationExecutionPhase,
    save: GenerationSavePhase,
    end: &str,
) -> GenerationStatus {
    let done = execution != GenerationExecutionPhase::Working;
    GenerationStatus {
        target,
        attempt_id: attempt_id.into(),
        execution,
        save,
        saved_end: end.into(),
        generated_end: done.then(|| end.into()),
        terminal_saved_end: (save == GenerationSavePhase::Saved).then(|| end.into()),
        failure_code: None,
    }
}

fn durable(attempt_id: &str) -> AttemptSummary {
    AttemptSummary {
        id: attempt_id.into(),
        attempt_number: "2".into(),
        execution: AttemptExecution::Completed,
        save: AttemptSave::Saved,
        saved_end: "2".into(),
        generated_end: Some("2".into()),
        terminal_saved_end: Some("2".into()),
        failure_code: None,
        statistics: None,
        effective_sampling: None,
        created_ms: "1".into(),
        updated_ms: "2".into(),
    }
}

async fn receive(
    transport: &mut loxa_ipc::IpcFramed,
    limit: usize,
) -> Result<ClientEnvelope, String> {
    let frame = transport
        .next()
        .await
        .ok_or("fake peer connection closed")?
        .map_err(|error| error.to_string())?;
    decode_with_limit(&frame, limit)
}

async fn send(
    transport: &mut loxa_ipc::IpcFramed,
    envelope: &ServerEnvelope,
    limit: usize,
) -> Result<(), String> {
    transport
        .send(encode_with_limit(envelope, limit)?.freeze())
        .await
        .map_err(|error| error.to_string())
}

fn conversation() -> Conversation {
    Conversation {
        id: CONVERSATION.into(),
        revision: "1".into(),
        profile_revision: "1".into(),
        last: None,
    }
}

fn output_pty() -> (TerminalOutput, OwnedFd) {
    let mut master = -1;
    let mut slave = -1;
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        0
    );
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    (TerminalOutput::from_fd(slave).unwrap(), master)
}

#[tokio::test]
async fn older_peer_rejects_bound_generation_before_submission() {
    let peer = FakePeer::start(Mode::OldPeer).await;
    let result = peer
        .client
        .prepare_generation_at(ConnectMode::ObserveExisting, operation_target())
        .await;
    assert!(
        matches!(result, Err(ClientError::Rejected(error)) if error.category == ErrorCategory::IncompatibleProtocol)
    );
    assert!(peer.seen().await.is_empty());
}

#[tokio::test]
async fn reopened_peer_accepts_replayed_old_boot_and_observes_old_attempt() {
    let peer = FakePeer::start(Mode::ReopenedAck).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    run_generation(
        &peer.client,
        &operation_target(),
        &mut conversation,
        command,
        &output,
        &signals,
        tag,
    )
    .await
    .unwrap();
    assert_eq!(
        conversation.last.as_ref().unwrap().target,
        GenerationTarget::Accepted {
            boot_epoch: BOOT.into(),
            submission_id: "22222222222222222222222222222222".into(),
            operation_generation: "1".into()
        }
    );
    assert!(peer.seen().await.iter().any(|item| matches!(item, Seen::Subscribe(GenerationTarget::Accepted { boot_epoch, .. }, _) if boot_epoch == BOOT)));
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn correctable_submission_rejection_keeps_the_conversation_and_sends_no_stop() {
    let peer = FakePeer::start(Mode::Rejected).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let result = run_generation(
        &peer.client,
        &operation_target(),
        &mut conversation,
        command,
        &output,
        &signals,
        tag,
    )
    .await;
    assert!(matches!(result, Err(ChatError::Turn(_))));
    assert_eq!(conversation.revision, "1");
    assert!(conversation.last.is_none());
    assert!(!peer
        .seen()
        .await
        .iter()
        .any(|item| matches!(item, Seen::Generation(GenerationCommand::Stop { .. }))));
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn early_save_failure_waits_for_terminal_execution_before_returning_to_prompt() {
    let peer = FakePeer::start(Mode::EarlySaveFailure).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let target = operation_target();
    let result = {
        let generation = run_generation(
            &peer.client,
            &target,
            &mut conversation,
            command,
            &output,
            &signals,
            tag,
        );
        tokio::pin!(generation);
        tokio::select! {
            biased;
            _ = &mut generation => panic!("early SaveFailed completed before terminal execution"),
            _ = peer.wait_for(|seen| seen.contains(&Seen::EarlySaveFailure)) => {},
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut generation)
                .await
                .is_err()
        );
        peer.terminal_release.notify_one();
        tokio::select! {
            biased;
            _ = &mut generation => panic!("terminal SaveFailed completed before the saved retry"),
            _ = peer.wait_for(|seen| seen.contains(&Seen::TerminalSaveFailure)) => {},
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut generation)
                .await
                .is_err()
        );
        peer.terminal_release.notify_one();
        generation.await
    };
    assert!(matches!(result, Err(ChatError::Turn(error)) if error.contains("stopped")));
    assert_eq!(conversation.last.as_ref().unwrap().attempt_id, SEND_ATTEMPT);
    assert!(!peer
        .seen()
        .await
        .iter()
        .any(|item| matches!(item, Seen::Generation(GenerationCommand::Stop { .. }))));
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn lost_ack_replays_the_same_submission_payload_and_revisions() {
    let peer = FakePeer::start(Mode::Replay).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let accepted = submit(&peer.client, &operation_target(), &command, &signals, tag)
        .await
        .unwrap();
    conversation.accept(&accepted).unwrap();
    let sends: Vec<_> = peer
        .seen()
        .await
        .into_iter()
        .filter_map(|seen| match seen {
            Seen::Generation(command @ GenerationCommand::Send { .. }) => Some(command),
            _ => None,
        })
        .collect();
    assert_eq!(sends, [command.clone(), command]);
    assert_eq!(accepted.submission_id, "22222222222222222222222222222222");
    signals.end_generation(tag);
}

#[tokio::test]
async fn interrupt_before_acceptance_stops_pending_then_the_exact_acknowledged_attempt() {
    let peer = FakePeer::start(Mode::PendingStop).await;
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let command =
        conversation().send_command("22222222222222222222222222222222".into(), "hello".into());
    let target = operation_target();
    let (result, ()) = tokio::join!(
        submit(&peer.client, &target, &command, &signals, tag),
        async {
            peer.wait_for(|seen| {
                seen.iter()
                    .any(|item| matches!(item, Seen::Generation(GenerationCommand::Send { .. })))
            })
            .await;
            signals.interrupt_current_for_test();
        },
    );
    assert!(matches!(result, Err(ChatError::Interrupted)));
    let stops: Vec<_> = peer
        .seen()
        .await
        .into_iter()
        .filter_map(|item| match item {
            Seen::Generation(GenerationCommand::Stop { target }) => Some(target),
            _ => None,
        })
        .collect();
    assert_eq!(stops.len(), 2);
    assert!(
        matches!(&stops[0], GenerationTarget::Pending { boot_epoch, .. } if boot_epoch == BOOT)
    );
    assert_eq!(
        stops[1],
        GenerationTarget::Accepted {
            boot_epoch: BOOT.into(),
            submission_id: "22222222222222222222222222222222".into(),
            operation_generation: "1".into()
        }
    );
}

#[tokio::test]
async fn interrupt_during_observation_stops_only_the_accepted_target() {
    let peer = FakePeer::start(Mode::AcceptedStop).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let target = operation_target();
    let (result, ()) = tokio::join!(
        run_generation(
            &peer.client,
            &target,
            &mut conversation,
            command,
            &output,
            &signals,
            tag
        ),
        async {
            peer.wait_for(|seen| seen.iter().any(|item| matches!(item, Seen::Subscribe(..))))
                .await;
            signals.interrupt_current_for_test();
        },
    );
    assert!(matches!(result, Err(ChatError::Interrupted)));
    let expected = conversation.last.as_ref().unwrap().target.clone();
    let stops: Vec<_> = peer
        .seen()
        .await
        .into_iter()
        .filter_map(|item| match item {
            Seen::Generation(GenerationCommand::Stop { target }) => Some(target),
            _ => None,
        })
        .collect();
    assert_eq!(stops, [expected]);
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn retry_observes_and_reads_the_new_exact_attempt() {
    let peer = FakePeer::start(Mode::Retry).await;
    let mut conversation = conversation();
    conversation.revision = "2".into();
    conversation.last = Some(AcceptedAttempt {
        attempt_id: SEND_ATTEMPT.into(),
        target: GenerationTarget::Accepted {
            boot_epoch: BOOT.into(),
            submission_id: "22222222222222222222222222222222".into(),
            operation_generation: "1".into(),
        },
        raw_cursor: 2,
    });
    let command = conversation
        .retry_command("44444444444444444444444444444444".into())
        .unwrap();
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    run_generation(
        &peer.client,
        &operation_target(),
        &mut conversation,
        command.clone(),
        &output,
        &signals,
        tag,
    )
    .await
    .unwrap();
    assert_eq!(
        conversation.last.as_ref().unwrap().attempt_id,
        RETRY_ATTEMPT
    );
    assert_eq!(conversation.last.as_ref().unwrap().raw_cursor, 2);
    let seen = peer.seen().await;
    assert!(seen.contains(&Seen::Generation(command)));
    assert!(seen.contains(&Seen::Subscribe(
        conversation.last.as_ref().unwrap().target.clone(),
        RETRY_ATTEMPT.into()
    )));
    assert!(seen.contains(&Seen::Range(
        ContentSource::Assistant {
            attempt_id: RETRY_ATTEMPT.into()
        },
        "0".into(),
        "2".into()
    )));
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn observer_failure_detaches_without_stop() {
    let peer = FakePeer::start(Mode::ObserverFailure).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let result = run_generation(
        &peer.client,
        &operation_target(),
        &mut conversation,
        command,
        &output,
        &signals,
        tag,
    )
    .await;
    assert!(matches!(result, Err(ChatError::Service(_))));
    assert!(conversation.last.is_some());
    assert!(!peer
        .seen()
        .await
        .iter()
        .any(|item| matches!(item, Seen::Generation(GenerationCommand::Stop { .. }))));
    output.restore().unwrap();
    drop(master);
}

#[tokio::test]
async fn saved_failure_or_stop_preserves_the_accepted_attempt_for_retry() {
    for mode in [Mode::Failed, Mode::Stopped] {
        let peer = FakePeer::start(mode).await;
        let mut conversation = conversation();
        let command =
            conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
        let (output, master) = output_pty();
        let mut signals = SessionSignals::install().unwrap();
        let tag = signals.begin_generation().unwrap();
        let result = run_generation(
            &peer.client,
            &operation_target(),
            &mut conversation,
            command,
            &output,
            &signals,
            tag,
        )
        .await;
        assert!(matches!(result, Err(ChatError::Turn(_))));
        assert_eq!(conversation.last.as_ref().unwrap().attempt_id, SEND_ATTEMPT);
        assert_eq!(conversation.last.as_ref().unwrap().raw_cursor, 2);
        assert!(
            matches!(conversation.retry_command("44444444444444444444444444444444".into()), Some(GenerationCommand::Retry { prior_attempt_id, .. }) if prior_attempt_id == SEND_ATTEMPT)
        );
        assert!(!peer
            .seen()
            .await
            .iter()
            .any(|item| matches!(item, Seen::Generation(GenerationCommand::Stop { .. }))));
        output.restore().unwrap();
        drop(master);
    }
}

#[tokio::test]
async fn terminal_output_failure_detaches_without_stop() {
    let peer = FakePeer::start(Mode::OutputFailure).await;
    let mut conversation = conversation();
    let command =
        conversation.send_command("22222222222222222222222222222222".into(), "hello".into());
    let (output, master) = output_pty();
    drop(master);
    let mut signals = SessionSignals::install().unwrap();
    let tag = signals.begin_generation().unwrap();
    let result = run_generation(
        &peer.client,
        &operation_target(),
        &mut conversation,
        command,
        &output,
        &signals,
        tag,
    )
    .await;
    assert!(matches!(result, Err(ChatError::Output)));
    assert!(conversation.last.is_some());
    assert!(!peer
        .seen()
        .await
        .iter()
        .any(|item| matches!(item, Seen::Generation(GenerationCommand::Stop { .. }))));
    let _ = output.restore();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saved_chat_pty_send_output_sigint_stops_the_exact_attempt_and_restores_flags() {
    let peer = FakePeer::start(Mode::AcceptedStop).await;
    let mut pty = super::tests::Pty::spawn_saved(&peer.root);
    pty.wait_for_prompts(1);
    pty.write_input(b"hello\n");
    peer.wait_for(|seen| seen.iter().any(|item| matches!(item, Seen::Subscribe(..))))
        .await;
    pty.wait_for_text("ok");
    pty.signal_sigint();
    peer.wait_for(|seen| {
        seen.iter().any(|item| {
            matches!(
                item,
                Seen::Generation(GenerationCommand::Stop {
                    target: GenerationTarget::Accepted { .. }
                })
            )
        })
    })
    .await;
    pty.finish();
    let seen = peer.seen().await;
    let submission_id = seen
        .iter()
        .find_map(|item| match item {
            Seen::Generation(GenerationCommand::Send { submission_id, .. }) => {
                Some(submission_id.clone())
            }
            _ => None,
        })
        .expect("child submitted a Send");
    let stops: Vec<_> = seen
        .into_iter()
        .filter_map(|item| match item {
            Seen::Generation(GenerationCommand::Stop { target }) => Some(target),
            _ => None,
        })
        .collect();
    assert_eq!(
        stops,
        [GenerationTarget::Accepted {
            boot_epoch: BOOT.into(),
            submission_id,
            operation_generation: "1".into()
        }]
    );
}
