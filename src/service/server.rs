use super::coordinator::{Coordinator, OwnerExit};
mod connection;

use crate::config::SettingsExit;
use crate::history::HistoryExit;
use connection::{classify_overload_connection, handle_connection, send_frame};
#[cfg(test)]
use connection::{receive_frame, receive_frame_with_limit, send_frame_with_limit};
use loxa_ipc::{Capability, ClientBootstrap, Reply, ReplyOutcome, ServerEnvelope};
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

const MAX_CONNECTIONS: usize = 16;
const MAX_SUBSCRIPTIONS: usize = 8;
const MAX_PROTECTED_STOPS: usize = 1;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const OVERLOAD_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(500);
const OVERLOAD_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const OVERLOAD_REPLY_TIMEOUT: Duration = Duration::from_millis(500);
const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
const LEGACY_CAPABILITIES: [Capability; 5] = [
    Capability::Status,
    Capability::Load,
    Capability::Unload,
    Capability::StopService,
    Capability::EngineUnixSocket,
];

struct NegotiatedHello {
    protocol: loxa_ipc::ProtocolVersion,
    capabilities: Vec<Capability>,
    storage_schema: u32,
    frame_limit: usize,
}

pub(super) async fn run(
    bootstrap: ClientBootstrap,
    coordinator: Coordinator,
) -> Result<(), String> {
    let (listener, socket) = match bind_control_socket(bootstrap.root().socket_path()) {
        Ok(bound) => bound,
        Err(error) => {
            coordinator.drain_after_server_failure();
            let mut owner_exit = coordinator.owner_exit_receiver();
            let mut history_exit = coordinator.history_exit_receiver();
            let mut settings_exit = coordinator.settings_exit_receiver();
            wait_for_owner_resolution(
                &coordinator,
                &mut owner_exit,
                &mut history_exit,
                &mut settings_exit,
            )
            .await;
            coordinator.join_owner()?;
            return Err(error);
        }
    };
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let subscriptions = Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS));
    let protected_stops = Arc::new(Semaphore::new(MAX_PROTECTED_STOPS));
    let mut connections = JoinSet::new();
    // One replaceable classifier lets a prompt StopService request reach the
    // coordinator even when all normal connections are waiting on a hello or
    // request. It never runs model work. A decoded StopService request carries
    // a separate permit into the non-preemptible reply task below.
    let mut overload_classifier = JoinSet::new();
    let mut owner_exit = coordinator.owner_exit_receiver();
    let mut history_exit = coordinator.history_exit_receiver();
    let mut settings_exit = coordinator.settings_exit_receiver();
    let mut owner_failed = false;
    loop {
        while let Some(completed) = connections.try_join_next() {
            if completed.is_err() {
                tracing::warn!(event = "service_connection_task_failed");
            }
        }
        while let Some(completed) = overload_classifier.try_join_next() {
            dispatch_overload_result(completed, &coordinator, &mut connections);
        }
        tokio::select! {
            biased;
            changed = owner_exit.changed() => {
                match (changed, *owner_exit.borrow()) {
                    (_, OwnerExit::Failed) | (Err(_), _) => {
                        owner_failed = true;
                        coordinator.drain_after_server_failure();
                        coordinator.release_runtime_if_durable();
                        if durability_is_drained(&history_exit, &settings_exit) {
                            break;
                        }
                    }
                    (_, OwnerExit::Drained) => break,
                    (Ok(()), OwnerExit::Running | OwnerExit::Quiesced) => {
                        coordinator.release_runtime_if_durable();
                    }
                }
            }
            changed = history_exit.changed() => {
                if changed.is_err() || *history_exit.borrow() == HistoryExit::Failed {
                    tracing::warn!(event = "service_history_owner_failed");
                }
                coordinator.release_runtime_if_durable();
                if owner_failed && durability_is_drained(&history_exit, &settings_exit) {
                    break;
                }
            }
            changed = settings_exit.changed() => {
                if changed.is_err() {
                    tracing::warn!(event = "service_settings_owner_failed");
                } else if *settings_exit.borrow() == SettingsExit::WriteCompleted {
                    if coordinator.finish_settings_write().is_err() {
                        tracing::warn!(event = "service_settings_join_failed");
                    }
                } else if *settings_exit.borrow() == SettingsExit::FlushFailed {
                    tracing::warn!(event = "service_settings_flush_failed");
                }
                coordinator.release_runtime_if_durable();
                if owner_failed && durability_is_drained(&history_exit, &settings_exit) {
                    break;
                }
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if completed.is_some_and(|result| result.is_err()) {
                    tracing::warn!(event = "service_connection_task_failed");
                }
            }
            completed = overload_classifier.join_next(), if !overload_classifier.is_empty() => {
                if let Some(completed) = completed {
                    dispatch_overload_result(completed, &coordinator, &mut connections);
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(_) => {
                        tracing::warn!(event = "service_listener_accept_failed");
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                };
                let bootstrap = bootstrap.clone();
                let coordinator = coordinator.clone();
                let subscriptions = Arc::clone(&subscriptions);
                match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => {
                        connections.spawn(async move {
                            let _permit = permit;
                            if let Err(_error) =
                                handle_connection(stream, bootstrap, coordinator, subscriptions).await
                            {
                                tracing::debug!(
                                    event = "service_connection_closed",
                                    failure = "transport_or_protocol"
                                );
                            }
                        });
                    }
                    Err(_) => {
                        // A later overflow connection replaces a stalled
                        // classifier. Reap it before admitting the replacement,
                        // so the overload lane owns at most one transport.
                        overload_classifier.abort_all();
                        while let Some(completed) = overload_classifier.join_next().await {
                            dispatch_overload_result(completed, &coordinator, &mut connections);
                        }
                        let protected_stops = Arc::clone(&protected_stops);
                        overload_classifier.spawn(async move {
                            classify_overload_connection(
                                stream,
                                bootstrap,
                                coordinator,
                                protected_stops,
                            )
                            .await
                        });
                    }
                }
            }
        }
    }
    drop(listener);
    coordinator.announce_server_stop();
    overload_classifier.abort_all();
    while overload_classifier.join_next().await.is_some() {}
    if tokio::time::timeout(CONNECTION_DRAIN_TIMEOUT, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    drop(socket);
    coordinator.join_owner()?;
    if owner_failed {
        return Err("runtime owner stopped without completing service drain".into());
    }
    Ok(())
}

struct ClassifiedStop {
    transport: loxa_ipc::IpcFramed,
    request_id: String,
    _permit: OwnedSemaphorePermit,
}

fn dispatch_overload_result(
    completed: Result<Result<Option<ClassifiedStop>, String>, tokio::task::JoinError>,
    coordinator: &Coordinator,
    connections: &mut JoinSet<()>,
) {
    let classified = match completed {
        Ok(Ok(Some(classified))) => classified,
        Ok(Ok(None)) => return,
        Ok(Err(_error)) => {
            tracing::debug!(
                event = "service_overload_connection_closed",
                failure = "transport_or_protocol"
            );
            return;
        }
        Err(error) if error.is_cancelled() => return,
        Err(_) => {
            tracing::warn!(event = "service_overload_classifier_failed");
            return;
        }
    };
    let ClassifiedStop {
        transport,
        request_id,
        _permit,
    } = classified;
    let outcome = match coordinator.stop_service() {
        Ok(accepted) => ReplyOutcome::Accepted(accepted),
        Err(error) => ReplyOutcome::Rejected(error),
    };
    let reply = Reply {
        request_id,
        outcome,
    };
    connections.spawn(async move {
        let mut transport = transport;
        let _permit = _permit;
        if let Err(_error) = send_frame(
            &mut transport,
            &ServerEnvelope::Reply(reply),
            OVERLOAD_REPLY_TIMEOUT,
        )
        .await
        {
            tracing::debug!(event = "service_stop_reply_failed", failure = "transport");
        }
    });
}

async fn wait_for_owner_resolution(
    coordinator: &Coordinator,
    owner_exit: &mut tokio::sync::watch::Receiver<OwnerExit>,
    history_exit: &mut tokio::sync::watch::Receiver<HistoryExit>,
    settings_exit: &mut tokio::sync::watch::Receiver<SettingsExit>,
) {
    loop {
        coordinator.release_runtime_if_durable();
        let owner_resolved = matches!(*owner_exit.borrow(), OwnerExit::Drained | OwnerExit::Failed);
        if owner_resolved && durability_is_drained(history_exit, settings_exit) {
            return;
        }
        tokio::select! {
            changed = owner_exit.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = history_exit.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = settings_exit.changed() => {
                if changed.is_err() {
                    return;
                }
                let settings_state = *settings_exit.borrow();
                if settings_state == SettingsExit::WriteCompleted
                    && coordinator.finish_settings_write().is_err() {
                    return;
                }
            }
        }
    }
}

fn durability_is_drained(
    history_exit: &tokio::sync::watch::Receiver<HistoryExit>,
    settings_exit: &tokio::sync::watch::Receiver<SettingsExit>,
) -> bool {
    let history = *history_exit.borrow();
    let settings = *settings_exit.borrow();
    history == HistoryExit::Drained && settings == SettingsExit::Drained
}

struct ControlSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for ControlSocket {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn bind_control_socket(path: &Path) -> Result<(UnixListener, ControlSocket), String> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0 =>
        {
            fs::remove_file(path).map_err(|error| format!("{}: {error}", path.display()))?;
        }
        Ok(_) => return Err("service control socket path contains unsafe retained state".into()),
        Err(error) => return Err(format!("{}: {error}", path.display())),
    }
    let listener = UnixListener::bind(path).map_err(|error| error.to_string())?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())?;
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("service control socket is not a private user-owned socket".into());
    }
    Ok((
        listener,
        ControlSocket {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use loxa_ipc::{
        framed, initialize_development_root, set_frame_limit, ClientEnvelope, ConnectMode,
        ErrorCategory, Hello, Request, ServiceClient, ServiceCommand, HISTORY_SCHEMA_VERSION,
        MAX_FRAME_BYTES, MAX_HISTORY_FRAME_BYTES,
    };
    use std::sync::Barrier;
    use tempfile::TempDir;
    use tokio::net::UnixStream;

    struct ServerFixture {
        _directory: TempDir,
        _diagnostics: Option<loxa_diagnostics::Diagnostics>,
        bootstrap: ClientBootstrap,
        coordinator: Coordinator,
        run_dir: PathBuf,
    }

    impl ServerFixture {
        async fn start() -> Self {
            Self::start_with_diagnostics(false).await
        }

        async fn start_with_diagnostics(with_diagnostics: bool) -> Self {
            let directory = tempfile::Builder::new()
                .prefix("loxa-service-pressure-")
                .tempdir_in("/tmp")
                .unwrap();
            let directory_path = fs::canonicalize(directory.path()).unwrap();
            let root = directory_path.join("dev");
            let forbidden_root = directory_path.join("normal");
            fs::create_dir(&forbidden_root).unwrap();
            let executable = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
            let bootstrap = initialize_development_root(
                &root,
                &forbidden_root,
                &executable,
                super::super::BUILD_ID,
            )
            .unwrap();
            let paths = crate::paths::AppPaths::from_values(Some(&root), None).unwrap();
            let run_dir = paths.run.clone();
            let diagnostics = with_diagnostics
                .then(|| {
                    loxa_diagnostics::init(&paths.logs, loxa_diagnostics::ProcessRole::Service)
                })
                .transpose()
                .unwrap();
            let diagnostics_health = diagnostics
                .as_ref()
                .map(loxa_diagnostics::Diagnostics::health_handle);
            let ownership =
                crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&root.join("run"))
                    .unwrap_or_else(|_| panic!("acquire isolated service runtime ownership"));
            let coordinator = Coordinator::start(
                paths,
                bootstrap.root().control_dir().to_owned(),
                bootstrap.root().root_identity().to_owned(),
                "test-machine-boot".into(),
                "test-service-boot".into(),
                ownership,
                tokio::runtime::Handle::current(),
                None,
                diagnostics_health,
            )
            .unwrap();
            Self {
                _directory: directory,
                _diagnostics: diagnostics,
                bootstrap,
                coordinator,
                run_dir,
            }
        }

        async fn stop_through_overload(self, complete_hello: bool) {
            let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
            let subscriptions = Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS));
            let protected_stops = Arc::new(Semaphore::new(MAX_PROTECTED_STOPS));
            let mut connections = JoinSet::new();
            let mut pressure = Vec::new();
            for _ in 0..MAX_CONNECTIONS {
                let (client, server) = UnixStream::pair().unwrap();
                let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
                let bootstrap = self.bootstrap.clone();
                let coordinator = self.coordinator.clone();
                let subscriptions = Arc::clone(&subscriptions);
                connections.spawn(async move {
                    let _permit = permit;
                    let _ = handle_connection(server, bootstrap, coordinator, subscriptions).await;
                });
                let mut transport = framed(client);
                if complete_hello {
                    send_frame(
                        &mut transport,
                        &ClientEnvelope::Hello(Hello::current(
                            super::super::BUILD_ID,
                            self.bootstrap.root().root_identity(),
                        )),
                        HANDSHAKE_TIMEOUT,
                    )
                    .await
                    .unwrap();
                    let reply: ServerEnvelope = receive_frame(&mut transport, HANDSHAKE_TIMEOUT)
                        .await
                        .unwrap();
                    assert!(matches!(reply, ServerEnvelope::HelloAck(_)));
                }
                pressure.push(transport);
            }
            assert!(Arc::clone(&permits).try_acquire_owned().is_err());

            // This connection occupies the only overload classifier without
            // sending a hello. The next prompt connection must replace and
            // fully reap it before being classified.
            let (stalled_client, stalled_server) = UnixStream::pair().unwrap();
            let mut stalled = framed(stalled_client);
            let mut overload_classifier = JoinSet::new();
            let bootstrap = self.bootstrap.clone();
            let coordinator = self.coordinator.clone();
            let stop_permits = Arc::clone(&protected_stops);
            overload_classifier.spawn(async move {
                classify_overload_connection(stalled_server, bootstrap, coordinator, stop_permits)
                    .await
            });
            tokio::task::yield_now().await;

            let (prompt_client, prompt_server) = UnixStream::pair().unwrap();
            overload_classifier.abort_all();
            while overload_classifier.join_next().await.is_some() {}
            assert!(
                tokio::time::timeout(Duration::from_millis(100), stalled.next())
                    .await
                    .is_ok(),
                "replaced overload transport remained open"
            );
            let bootstrap = self.bootstrap.clone();
            let coordinator = self.coordinator.clone();
            overload_classifier.spawn(async move {
                classify_overload_connection(prompt_server, bootstrap, coordinator, protected_stops)
                    .await
            });
            let mut prompt = framed(prompt_client);
            send_frame(
                &mut prompt,
                &ClientEnvelope::Hello(Hello::current(
                    super::super::BUILD_ID,
                    self.bootstrap.root().root_identity(),
                )),
                OVERLOAD_HANDSHAKE_TIMEOUT,
            )
            .await
            .unwrap();
            let hello: ServerEnvelope = receive_frame(&mut prompt, OVERLOAD_HANDSHAKE_TIMEOUT)
                .await
                .unwrap();
            assert!(matches!(hello, ServerEnvelope::HelloAck(_)));
            send_frame(
                &mut prompt,
                &ClientEnvelope::Request(Request::new("stop-test", ServiceCommand::StopService)),
                OVERLOAD_REQUEST_TIMEOUT,
            )
            .await
            .unwrap();
            let classified = overload_classifier.join_next().await.unwrap();
            dispatch_overload_result(classified, &self.coordinator, &mut connections);
            let reply: ServerEnvelope = receive_frame(&mut prompt, OVERLOAD_REPLY_TIMEOUT)
                .await
                .unwrap();
            assert!(matches!(
                reply,
                ServerEnvelope::Reply(Reply {
                    outcome: ReplyOutcome::Accepted(_),
                    ..
                })
            ));

            drop(pressure);
            self.coordinator.announce_server_stop();
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            let mut owner_exit = self.coordinator.owner_exit_receiver();
            let mut history_exit = self.coordinator.history_exit_receiver();
            let mut settings_exit = self.coordinator.settings_exit_receiver();
            wait_for_owner_resolution(
                &self.coordinator,
                &mut owner_exit,
                &mut history_exit,
                &mut settings_exit,
            )
            .await;
            assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
            self.coordinator.join_owner().unwrap();
        }
    }

    async fn subscribe_client(
        bootstrap: &ClientBootstrap,
        client: UnixStream,
        request_id: &str,
    ) -> loxa_ipc::IpcFramed {
        let mut transport = framed(client);
        send_frame(
            &mut transport,
            &ClientEnvelope::Hello(Hello::current(
                super::super::BUILD_ID,
                bootstrap.root().root_identity(),
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();
        let hello: ServerEnvelope = receive_frame(&mut transport, HANDSHAKE_TIMEOUT)
            .await
            .unwrap();
        assert!(matches!(hello, ServerEnvelope::HelloAck(_)));
        send_frame(
            &mut transport,
            &ClientEnvelope::Subscribe {
                request_id: request_id.into(),
            },
            REQUEST_TIMEOUT,
        )
        .await
        .unwrap();
        let snapshot: ServerEnvelope = receive_frame(&mut transport, REQUEST_TIMEOUT)
            .await
            .unwrap();
        assert!(matches!(snapshot, ServerEnvelope::Snapshot(_)));
        transport
    }

    async fn history_client(
        bootstrap: &ClientBootstrap,
        client: UnixStream,
    ) -> loxa_ipc::IpcFramed {
        let mut transport = framed(client);
        send_frame(
            &mut transport,
            &ClientEnvelope::Hello(Hello::history(
                super::super::BUILD_ID,
                bootstrap.root().root_identity(),
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();
        let hello: ServerEnvelope = receive_frame(&mut transport, HANDSHAKE_TIMEOUT)
            .await
            .unwrap();
        let ServerEnvelope::HelloAck(hello) = hello else {
            panic!("expected history hello acknowledgement");
        };
        assert_eq!(hello.protocol, loxa_ipc::ProtocolVersion::CURRENT);
        assert_eq!(hello.storage_schema, HISTORY_SCHEMA_VERSION);
        assert!(hello.capabilities.contains(&Capability::History));
        set_frame_limit(&mut transport, MAX_HISTORY_FRAME_BYTES).unwrap();
        transport
    }

    async fn wait_for_history_ready(coordinator: &Coordinator) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !coordinator.history_is_ready() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(coordinator.history_is_ready());
    }

    async fn persist_settings(
        coordinator: &Coordinator,
        command: loxa_ipc::ServiceSettingsCommand,
    ) -> loxa_ipc::ServiceSettings {
        let mut exit = coordinator.settings_exit_receiver();
        let task = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.settings(command).await })
        };
        loop {
            if *exit.borrow() == SettingsExit::WriteCompleted {
                coordinator.finish_settings_write().unwrap();
                break;
            }
            exit.changed().await.unwrap();
        }
        let (reply, permit) = task.await.unwrap();
        assert!(permit.is_none());
        match reply.unwrap() {
            loxa_ipc::ServiceSettingsReply::Service(settings) => settings,
            _ => panic!("settings save returned a conversation profile"),
        }
    }

    fn install_history_model(root: &Path) {
        use crate::catalog::{Manifest, Origin};

        let models = root.join("models");
        let model = models.join("demo");
        fs::create_dir_all(&model).unwrap();
        fs::write(model.join("model.gguf"), b"GGUF").unwrap();
        crate::catalog::publish_manifest(
            &models,
            &Manifest {
                version: 2,
                id: "demo".into(),
                repo: None,
                revision: None,
                remote_filename: None,
                origin: Some(Origin::Local),
                source_filename: Some("source.gguf".into()),
                local_filename: "model.gguf".into(),
                sha256: "b83633aa785344791618f2fddf131b010ea04912a60430760b070bad293f65bd".into(),
                size: 4,
                artifacts: None,
                profile: None,
                runtime: None,
            },
        )
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_status_reports_live_diagnostics_health() {
        let fixture = ServerFixture::start_with_diagnostics(true).await;
        let (client, server) = UnixStream::pair().unwrap();
        let handler = tokio::spawn(handle_connection(
            server,
            fixture.bootstrap.clone(),
            fixture.coordinator.clone(),
            Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS)),
        ));
        let mut transport = framed(client);
        send_frame(
            &mut transport,
            &ClientEnvelope::Hello(Hello::current(
                super::super::BUILD_ID,
                fixture.bootstrap.root().root_identity(),
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(matches!(
            receive_frame::<ServerEnvelope>(&mut transport, HANDSHAKE_TIMEOUT)
                .await
                .unwrap(),
            ServerEnvelope::HelloAck(_)
        ));
        send_frame(
            &mut transport,
            &ClientEnvelope::Request(Request::new("health-status", ServiceCommand::Status)),
            REQUEST_TIMEOUT,
        )
        .await
        .unwrap();
        let response: ServerEnvelope = receive_frame(&mut transport, REQUEST_TIMEOUT)
            .await
            .unwrap();
        let ServerEnvelope::Reply(Reply {
            outcome: ReplyOutcome::Status(status),
            ..
        }) = response
        else {
            panic!("expected an explicit status response");
        };
        assert!(status.diagnostics.available);
        assert!(!status.diagnostics.at_capacity);
        assert!(!status.diagnostics.sink_failed);
        assert_eq!(status.diagnostics.sink_failures, 0);
        assert!(matches!(
            status.runtime.phase,
            loxa_ipc::RuntimePhase::Unloaded
        ));
        handler.await.unwrap().unwrap();

        fixture.coordinator.stop_service().unwrap();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinator_settings_capture_globals_for_new_conversations_and_profile_reset() {
        let fixture = ServerFixture::start().await;
        wait_for_history_ready(&fixture.coordinator).await;
        install_history_model(fixture.bootstrap.root().root());

        let initial = persist_settings(
            &fixture.coordinator,
            loxa_ipc::ServiceSettingsCommand::PatchServiceSettings {
                expected_revision: "0".into(),
                patch: loxa_ipc::ServiceSettingsPatch {
                    ctx: None,
                    port: None,
                    generation: Some(loxa_ipc::GenerationSettingsPatch::Fields {
                        system_instruction: Some("initial global".into()),
                        max_output_tokens: Some(700),
                    }),
                },
            },
        )
        .await;
        assert_eq!(initial.revision, "1");
        assert_eq!(
            initial.durability,
            loxa_ipc::ServiceSettingsDurability::Saved
        );
        assert_eq!(
            initial.application,
            loxa_ipc::ServiceSettingsApplication::NotApplied
        );

        let (created, permit) = fixture
            .coordinator
            .history(loxa_ipc::HistoryCommand::CreateConversation {
                model_id: "demo".into(),
            })
            .await;
        drop(permit);
        let created = match created.unwrap() {
            loxa_ipc::HistoryReply::Conversation(conversation) => conversation,
            _ => panic!("create returned the wrong history reply"),
        };
        let (profile, permit) = fixture
            .coordinator
            .settings(loxa_ipc::ServiceSettingsCommand::GetConversationProfile {
                conversation_id: created.id.clone(),
            })
            .await;
        drop(permit);
        let profile = match profile.unwrap() {
            loxa_ipc::ServiceSettingsReply::Conversation(profile) => profile,
            _ => panic!("profile read returned service settings"),
        };
        assert_eq!(profile.generation.system_instruction, "initial global");
        assert_eq!(profile.generation.max_output_tokens, 700);

        persist_settings(
            &fixture.coordinator,
            loxa_ipc::ServiceSettingsCommand::PatchServiceSettings {
                expected_revision: "1".into(),
                patch: loxa_ipc::ServiceSettingsPatch {
                    ctx: None,
                    port: None,
                    generation: Some(loxa_ipc::GenerationSettingsPatch::Fields {
                        system_instruction: Some("current global".into()),
                        max_output_tokens: Some(900),
                    }),
                },
            },
        )
        .await;
        let (unchanged, permit) = fixture
            .coordinator
            .settings(loxa_ipc::ServiceSettingsCommand::GetConversationProfile {
                conversation_id: created.id.clone(),
            })
            .await;
        drop(permit);
        assert!(matches!(
            unchanged.unwrap(),
            loxa_ipc::ServiceSettingsReply::Conversation(ref profile)
                if profile.generation.system_instruction == "initial global"
                    && profile.generation.max_output_tokens == 700
        ));

        let (reset, permit) = fixture
            .coordinator
            .settings(loxa_ipc::ServiceSettingsCommand::PatchConversationProfile {
                conversation_id: created.id,
                expected_conversation_revision: "1".into(),
                expected_profile_revision: "1".into(),
                patch: loxa_ipc::GenerationSettingsPatch::Reset,
            })
            .await;
        drop(permit);
        assert!(matches!(
            reset.unwrap(),
            loxa_ipc::ServiceSettingsReply::Conversation(ref profile)
                if profile.conversation_revision == "2"
                    && profile.profile_revision == "2"
                    && profile.generation.system_instruction == "current global"
                    && profile.generation.max_output_tokens == 900
        ));

        fixture.coordinator.stop_service().unwrap();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_protocol_freezes_a_conversation_and_stop_bypasses_stalled_sql() {
        let fixture = ServerFixture::start().await;
        wait_for_history_ready(&fixture.coordinator).await;
        install_history_model(fixture.bootstrap.root().root());

        let (client, server) = UnixStream::pair().unwrap();
        let handler = tokio::spawn(handle_connection(
            server,
            fixture.bootstrap.clone(),
            fixture.coordinator.clone(),
            Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS)),
        ));
        let mut client = history_client(&fixture.bootstrap, client).await;
        send_frame_with_limit(
            &mut client,
            &ClientEnvelope::Request(Request::new(
                "create-history",
                ServiceCommand::History {
                    command: loxa_ipc::HistoryCommand::CreateConversation {
                        model_id: "demo".into(),
                    },
                },
            )),
            REQUEST_TIMEOUT,
            MAX_HISTORY_FRAME_BYTES,
        )
        .await
        .unwrap();
        let reply: ServerEnvelope =
            match receive_frame_with_limit(&mut client, REQUEST_TIMEOUT, MAX_HISTORY_FRAME_BYTES)
                .await
            {
                Ok(reply) => reply,
                Err(error) => panic!(
                    "history create transport failed: {error}; server: {:?}",
                    handler.await
                ),
            };
        assert!(matches!(
            reply,
            ServerEnvelope::Reply(Reply {
                outcome: ReplyOutcome::History {
                    reply: loxa_ipc::HistoryReply::Conversation(_)
                },
                ..
            })
        ));
        handler.await.unwrap().unwrap();

        let barrier = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_history_for_test(Arc::clone(&barrier));
        barrier.wait();
        let (client, server) = UnixStream::pair().unwrap();
        let handler = tokio::spawn(handle_connection(
            server,
            fixture.bootstrap.clone(),
            fixture.coordinator.clone(),
            Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS)),
        ));
        let mut client = history_client(&fixture.bootstrap, client).await;
        send_frame_with_limit(
            &mut client,
            &ClientEnvelope::Request(Request::new(
                "list-history",
                ServiceCommand::History {
                    command: loxa_ipc::HistoryCommand::ListConversations {
                        cursor: None,
                        limit: 1,
                    },
                },
            )),
            REQUEST_TIMEOUT,
            MAX_HISTORY_FRAME_BYTES,
        )
        .await
        .unwrap();
        let admission_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while fixture.coordinator.history_ordinary_available_for_test() == 8
            && tokio::time::Instant::now() < admission_deadline
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(fixture.coordinator.history_ordinary_available_for_test(), 7);

        let started = std::time::Instant::now();
        fixture.coordinator.stop_service().unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(
            fixture.coordinator.history_status().phase,
            loxa_ipc::HistoryPhase::FlushPending
        );
        barrier.wait();
        let reply: ServerEnvelope =
            receive_frame_with_limit(&mut client, REQUEST_TIMEOUT, MAX_HISTORY_FRAME_BYTES)
                .await
                .unwrap();
        assert!(matches!(
            reply,
            ServerEnvelope::Reply(Reply {
                outcome: ReplyOutcome::History {
                    reply: loxa_ipc::HistoryReply::ConversationPage(_)
                },
                ..
            })
        ));
        handler.await.unwrap().unwrap();

        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_typed_client_round_trips_legacy_history_drafts_and_settings() {
        let fixture = ServerFixture::start().await;
        wait_for_history_ready(&fixture.coordinator).await;
        install_history_model(fixture.bootstrap.root().root());
        let mut server = tokio::spawn(run(fixture.bootstrap.clone(), fixture.coordinator.clone()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !fixture.bootstrap.root().socket_path().exists()
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if !fixture.bootstrap.root().socket_path().exists() {
            let outcome = tokio::time::timeout(Duration::from_secs(1), &mut server)
                .await
                .expect("server did not report its bind failure");
            panic!("server did not bind its control socket: {outcome:?}");
        }

        let client = ServiceClient::load(
            fixture.bootstrap.root().root(),
            None,
            super::super::BUILD_ID,
        )
        .unwrap();
        assert!(matches!(
            client
                .request(ConnectMode::ObserveExisting, ServiceCommand::Status)
                .await
                .unwrap(),
            ReplyOutcome::Status(_)
        ));
        assert_eq!(
            client
                .history_status(ConnectMode::ObserveExisting)
                .await
                .unwrap()
                .phase,
            loxa_ipc::HistoryPhase::Ready
        );
        let conversation = match client
            .history_request(
                ConnectMode::ObserveExisting,
                loxa_ipc::HistoryCommand::CreateConversation {
                    model_id: "demo".into(),
                },
            )
            .await
            .unwrap()
        {
            loxa_ipc::HistoryReply::Conversation(conversation) => conversation,
            _ => panic!("typed history client returned the wrong reply"),
        };

        let desktop_client_id = "11".repeat(16);
        let draft = match client
            .draft_request(
                ConnectMode::ObserveExisting,
                loxa_ipc::DraftCommand::CreateScope {
                    desktop_client_id: desktop_client_id.clone(),
                    conversation_id: Some(conversation.id),
                },
            )
            .await
            .unwrap()
        {
            loxa_ipc::DraftReply::Snapshot(snapshot) => snapshot,
            _ => panic!("typed draft client returned the wrong reply"),
        };
        let text = "x".repeat(loxa_ipc::MAX_DRAFT_TEXT_BYTES);
        let save = loxa_ipc::DraftCommand::SaveSnapshot {
            draft_id: draft.id,
            desktop_client_id,
            revision: "1".into(),
            text: text.clone(),
        };
        assert!(serde_json::to_vec(&save).unwrap().len() > MAX_FRAME_BYTES);
        let saved = match client
            .draft_request(ConnectMode::ObserveExisting, save)
            .await
            .unwrap()
        {
            loxa_ipc::DraftReply::Snapshot(snapshot) => snapshot,
            _ => panic!("typed draft save returned the wrong reply"),
        };
        assert_eq!(saved.revision, "1");
        assert_eq!(saved.text, text);

        assert!(matches!(
            client
                .settings_request(
                    ConnectMode::ObserveExisting,
                    loxa_ipc::ServiceSettingsCommand::GetServiceSettings,
                )
                .await
                .unwrap(),
            loxa_ipc::ServiceSettingsReply::Service(_)
        ));
        assert!(matches!(
            client
                .request(ConnectMode::ObserveExisting, ServiceCommand::StopService,)
                .await
                .unwrap(),
            ReplyOutcome::Accepted(_)
        ));
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server did not stop after the typed Stop request")
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fresh_stop_survives_pending_hello_pressure_and_replaces_stalled_classifier() {
        ServerFixture::start()
            .await
            .stop_through_overload(false)
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fresh_stop_survives_post_hello_request_pressure_and_replaces_stalled_classifier() {
        ServerFixture::start()
            .await
            .stop_through_overload(true)
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_panic_is_signaled_without_claiming_a_completed_drain() {
        let fixture = ServerFixture::start().await;
        let barrier = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_history_for_test(Arc::clone(&barrier));
        barrier.wait();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        fixture.coordinator.panic_owner_for_test();
        tokio::time::timeout(Duration::from_secs(1), owner_exit.changed())
            .await
            .expect("runtime owner failure was not signaled")
            .unwrap();
        assert_eq!(*owner_exit.borrow(), OwnerExit::Failed);
        assert!(matches!(
            fixture.coordinator.status().phase,
            loxa_ipc::RuntimePhase::Unloaded
        ));
        assert!(
            crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir)
                .is_err()
        );
        fixture.coordinator.drain_after_server_failure();
        barrier.wait();
        assert!(fixture.coordinator.join_owner().is_err());
        match crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir) {
            Ok(ownership) => drop(ownership),
            Err(_) => panic!("durability resolution must release runtime ownership"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settings_completion_is_joined_before_owner_resolution_returns() {
        let fixture = ServerFixture::start().await;
        let barrier = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_next_settings_write_for_test(Arc::clone(&barrier));
        let settings_task = {
            let coordinator = fixture.coordinator.clone();
            tokio::spawn(async move {
                coordinator
                    .settings(loxa_ipc::ServiceSettingsCommand::PatchServiceSettings {
                        expected_revision: "0".into(),
                        patch: loxa_ipc::ServiceSettingsPatch {
                            ctx: Some(loxa_ipc::OptionalU32Patch::Set { value: 8192 }),
                            port: None,
                            generation: None,
                        },
                    })
                    .await
                    .0
            })
        };
        barrier.wait();
        fixture.coordinator.stop_service().unwrap();
        barrier.wait();

        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_for_owner_resolution(
                &fixture.coordinator,
                &mut owner_exit,
                &mut history_exit,
                &mut settings_exit,
            ),
        )
        .await
        .expect("settings completion deadlocked its own watch publication");
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        assert!(matches!(
            settings_task.await.unwrap().unwrap(),
            loxa_ipc::ServiceSettingsReply::Service(ref settings)
                if settings.revision == "1"
                    && settings.durability == loxa_ipc::ServiceSettingsDurability::Saved
        ));
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_gate_rejects_a_settings_patch_before_private_drain_begins() {
        let fixture = ServerFixture::start().await;
        let barrier = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_settings_drain_for_test(Arc::clone(&barrier));
        let stop = {
            let coordinator = fixture.coordinator.clone();
            std::thread::spawn(move || coordinator.stop_service())
        };
        barrier.wait();
        let (result, permit) = fixture
            .coordinator
            .settings(loxa_ipc::ServiceSettingsCommand::PatchServiceSettings {
                expected_revision: "0".into(),
                patch: loxa_ipc::ServiceSettingsPatch {
                    ctx: Some(loxa_ipc::OptionalU32Patch::Set { value: 8192 }),
                    port: None,
                    generation: None,
                },
            })
            .await;
        assert!(permit.is_none());
        assert!(matches!(
            result,
            Err(loxa_ipc::ServiceError {
                category: ErrorCategory::ServiceUnavailable,
                ..
            })
        ));
        barrier.wait();
        stop.join().unwrap().unwrap();

        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opening_handshake_cannot_use_sql_backed_profile_settings() {
        let fixture = ServerFixture::start().await;
        let mut capabilities = LEGACY_CAPABILITIES.to_vec();
        capabilities.push(Capability::Settings);
        let negotiated = NegotiatedHello {
            protocol: loxa_ipc::ProtocolVersion::CURRENT,
            capabilities,
            storage_schema: 0,
            frame_limit: MAX_HISTORY_FRAME_BYTES,
        };
        let (reply, permit) = connection::execute_request(
            &fixture.coordinator,
            Request::new(
                "profile-before-ready",
                ServiceCommand::Settings {
                    command: loxa_ipc::ServiceSettingsCommand::GetConversationProfile {
                        conversation_id: "00".repeat(16),
                    },
                },
            ),
            &negotiated,
        )
        .await;
        assert!(permit.is_none());
        assert!(matches!(
            reply.outcome,
            ReplyOutcome::Rejected(loxa_ipc::ServiceError {
                category: ErrorCategory::ServiceUnavailable,
                ..
            })
        ));

        let (reply, permit) = connection::execute_request(
            &fixture.coordinator,
            Request::new(
                "global-before-ready",
                ServiceCommand::Settings {
                    command: loxa_ipc::ServiceSettingsCommand::GetServiceSettings,
                },
            ),
            &negotiated,
        )
        .await;
        assert!(permit.is_none());
        assert!(matches!(reply.outcome, ReplyOutcome::Settings { .. }));

        fixture.coordinator.stop_service().unwrap();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_panic_waits_for_an_in_flight_settings_save_to_drain() {
        let fixture = ServerFixture::start().await;
        let (probe_listener, probe_socket) =
            bind_control_socket(fixture.bootstrap.root().socket_path()).unwrap();
        drop(probe_listener);
        drop(probe_socket);
        let mut server = tokio::spawn(run(fixture.bootstrap.clone(), fixture.coordinator.clone()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !fixture.bootstrap.root().socket_path().exists()
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(fixture.bootstrap.root().socket_path().exists());

        let barrier = Arc::new(Barrier::new(2));
        fixture
            .coordinator
            .stall_next_settings_write_for_test(Arc::clone(&barrier));
        let settings_task = {
            let coordinator = fixture.coordinator.clone();
            tokio::spawn(async move {
                coordinator
                    .settings(loxa_ipc::ServiceSettingsCommand::PatchServiceSettings {
                        expected_revision: "0".into(),
                        patch: loxa_ipc::ServiceSettingsPatch {
                            ctx: Some(loxa_ipc::OptionalU32Patch::Set { value: 4096 }),
                            port: None,
                            generation: None,
                        },
                    })
                    .await
                    .0
            })
        };
        barrier.wait();
        fixture.coordinator.panic_owner_for_test();
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut server)
            .await
            .is_err());
        assert!(
            crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir)
                .is_err()
        );

        barrier.wait();
        assert!(matches!(
            settings_task.await.unwrap().unwrap(),
            loxa_ipc::ServiceSettingsReply::Service(ref settings)
                if settings.revision == "1"
                    && settings.durability == loxa_ipc::ServiceSettingsDurability::Saved
        ));
        assert!(tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server did not finish after durable owners drained")
            .unwrap()
            .is_err());
        match crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir) {
            Ok(ownership) => drop(ownership),
            Err(_) => panic!("settings drain must release runtime ownership after owner failure"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_panic_keeps_control_open_for_a_failed_history_close_retry() {
        let fixture = ServerFixture::start().await;
        wait_for_history_ready(&fixture.coordinator).await;
        let (probe_listener, probe_socket) =
            bind_control_socket(fixture.bootstrap.root().socket_path()).unwrap();
        drop(probe_listener);
        drop(probe_socket);
        fixture.coordinator.fail_next_history_close_for_test();
        let mut server = tokio::spawn(run(fixture.bootstrap.clone(), fixture.coordinator.clone()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !fixture.bootstrap.root().socket_path().exists()
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if !fixture.bootstrap.root().socket_path().exists() {
            let outcome = tokio::time::timeout(Duration::from_secs(1), &mut server)
                .await
                .expect("server did not report its bind failure");
            panic!("server did not bind its control socket: {outcome:?}");
        }

        fixture.coordinator.panic_owner_for_test();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while fixture.coordinator.history_status().phase != loxa_ipc::HistoryPhase::FlushFailed
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            fixture.coordinator.history_status().phase,
            loxa_ipc::HistoryPhase::FlushFailed
        );
        assert!(
            crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir)
                .is_err()
        );

        let stream = UnixStream::connect(fixture.bootstrap.root().socket_path())
            .await
            .unwrap();
        let mut client = framed(stream);
        send_frame(
            &mut client,
            &ClientEnvelope::Hello(Hello::current(
                super::super::BUILD_ID,
                fixture.bootstrap.root().root_identity(),
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(matches!(
            receive_frame::<ServerEnvelope>(&mut client, HANDSHAKE_TIMEOUT)
                .await
                .unwrap(),
            ServerEnvelope::HelloAck(_)
        ));
        send_frame(
            &mut client,
            &ClientEnvelope::Request(Request::new("retry-stop", ServiceCommand::StopService)),
            REQUEST_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(matches!(
            receive_frame::<ServerEnvelope>(&mut client, REQUEST_TIMEOUT)
                .await
                .unwrap(),
            ServerEnvelope::Reply(Reply {
                outcome: ReplyOutcome::Accepted(_),
                ..
            })
        ));

        assert!(server.await.unwrap().is_err());
        match crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&fixture.run_dir) {
            Ok(ownership) => drop(ownership),
            Err(_) => panic!("resolved failed drain must release runtime ownership"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_subscription_disconnects_release_all_capacity_for_a_fresh_subscriber() {
        let fixture = ServerFixture::start().await;
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let subscriptions = Arc::new(Semaphore::new(MAX_SUBSCRIPTIONS));
        let mut connections = JoinSet::new();

        for index in 0..MAX_SUBSCRIPTIONS {
            let (client, server) = UnixStream::pair().unwrap();
            let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
            let bootstrap = fixture.bootstrap.clone();
            let coordinator = fixture.coordinator.clone();
            let subscription_permits = Arc::clone(&subscriptions);
            connections.spawn(async move {
                let _permit = permit;
                handle_connection(server, bootstrap, coordinator, subscription_permits).await
            });
            let client =
                subscribe_client(&fixture.bootstrap, client, &format!("sub-{index}")).await;
            drop(client);
        }
        for _ in 0..MAX_SUBSCRIPTIONS {
            tokio::time::timeout(Duration::from_secs(1), connections.join_next())
                .await
                .expect("disconnected subscription handler did not exit")
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(subscriptions.available_permits(), MAX_SUBSCRIPTIONS);

        let (fresh_client, fresh_server) = UnixStream::pair().unwrap();
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let bootstrap = fixture.bootstrap.clone();
        let coordinator = fixture.coordinator.clone();
        let subscription_permits = Arc::clone(&subscriptions);
        connections.spawn(async move {
            let _permit = permit;
            handle_connection(fresh_server, bootstrap, coordinator, subscription_permits).await
        });
        let fresh = subscribe_client(&fixture.bootstrap, fresh_client, "fresh-sub").await;
        drop(fresh);
        tokio::time::timeout(Duration::from_secs(1), connections.join_next())
            .await
            .expect("fresh disconnected subscription handler did not exit")
            .unwrap()
            .unwrap()
            .unwrap();

        fixture.coordinator.stop_service().unwrap();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_missing_model_publishes_a_typed_terminal_failure_and_allows_retry() {
        let fixture = ServerFixture::start().await;
        let mut snapshots = fixture.coordinator.subscribe();

        let first = fixture
            .coordinator
            .load("missing-model".into())
            .await
            .unwrap();
        loop {
            let status = snapshots.borrow().clone();
            if let loxa_ipc::RuntimePhase::LoadFailed {
                task_id,
                generation,
                model_id,
                category,
            } = status.phase
            {
                assert_eq!(task_id, first.task_id);
                assert_eq!(generation, first.generation);
                assert_eq!(model_id, "missing-model");
                assert_eq!(category, ErrorCategory::ModelUnavailable);
                break;
            }
            tokio::time::timeout(Duration::from_secs(1), snapshots.changed())
                .await
                .expect("accepted load failure was not published")
                .unwrap();
        }

        let second = fixture
            .coordinator
            .load("another-missing-model".into())
            .await
            .unwrap();
        assert_ne!(second.task_id, first.task_id);
        assert_ne!(second.generation, first.generation);

        fixture.coordinator.stop_service().unwrap();
        let mut owner_exit = fixture.coordinator.owner_exit_receiver();
        let mut history_exit = fixture.coordinator.history_exit_receiver();
        let mut settings_exit = fixture.coordinator.settings_exit_receiver();
        wait_for_owner_resolution(
            &fixture.coordinator,
            &mut owner_exit,
            &mut history_exit,
            &mut settings_exit,
        )
        .await;
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }
}
