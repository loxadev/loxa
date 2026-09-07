use super::coordinator::{Coordinator, OwnerExit};
use futures_util::{SinkExt, StreamExt};
use loxa_ipc::{
    decode, encode, framed, peer_credentials, Capability, ClientBootstrap, ClientEnvelope,
    ErrorCategory, Hello, HelloAck, Reply, ReplyOutcome, Request, RuntimeStatus, ServerEnvelope,
    ServiceCommand, ServiceError, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
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
const STORAGE_SCHEMA: u32 = 1;
const CAPABILITIES: [Capability; 5] = [
    Capability::Status,
    Capability::Load,
    Capability::Unload,
    Capability::StopService,
    Capability::EngineUnixSocket,
];

pub(super) async fn run(
    bootstrap: ClientBootstrap,
    coordinator: Coordinator,
) -> Result<(), String> {
    let (listener, socket) = match bind_control_socket(bootstrap.root().socket_path()) {
        Ok(bound) => bound,
        Err(error) => {
            coordinator.drain_after_server_failure();
            let mut owner_exit = coordinator.owner_exit_receiver();
            while *owner_exit.borrow() == OwnerExit::Running {
                if owner_exit.changed().await.is_err() {
                    break;
                }
            }
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
                        break;
                    }
                    (_, OwnerExit::Drained) => break,
                    (Ok(()), OwnerExit::Running) => {}
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

async fn classify_overload_connection(
    stream: UnixStream,
    bootstrap: ClientBootstrap,
    coordinator: Coordinator,
    protected_stops: Arc<Semaphore>,
) -> Result<Option<ClassifiedStop>, String> {
    let peer = peer_credentials(&stream)?;
    if peer.uid != unsafe { libc::geteuid() } || peer.pid == 0 {
        return Err("service client is not an authenticated local user process".into());
    }
    let mut transport = framed(stream);
    let first: ClientEnvelope = receive_frame(&mut transport, OVERLOAD_HANDSHAKE_TIMEOUT).await?;
    first.validate_shape().map_err(str::to_owned)?;
    let ClientEnvelope::Hello(hello) = first else {
        return Err("the first overload service envelope must be a hello".into());
    };
    validate_hello(&hello, &bootstrap).map_err(|error| error.context)?;
    send_hello_ack(
        &mut transport,
        &bootstrap,
        &coordinator,
        OVERLOAD_HANDSHAKE_TIMEOUT,
    )
    .await?;

    let envelope: ClientEnvelope = receive_frame(&mut transport, OVERLOAD_REQUEST_TIMEOUT).await?;
    envelope.validate_shape().map_err(str::to_owned)?;
    let ClientEnvelope::Request(request) = envelope else {
        return Err("overload service lane accepts only requests".into());
    };
    if !matches!(request.command, ServiceCommand::StopService) {
        let reply = Reply {
            request_id: request.request_id,
            outcome: ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::Busy,
                "service control capacity is full",
            )),
        };
        send_frame(
            &mut transport,
            &ServerEnvelope::Reply(reply),
            OVERLOAD_REPLY_TIMEOUT,
        )
        .await?;
        return Ok(None);
    }
    let permit = protected_stops
        .try_acquire_owned()
        .map_err(|_| "protected service stop capacity is full".to_string())?;
    Ok(Some(ClassifiedStop {
        transport,
        request_id: request.request_id,
        _permit: permit,
    }))
}

async fn handle_connection(
    stream: UnixStream,
    bootstrap: ClientBootstrap,
    coordinator: Coordinator,
    subscriptions: Arc<Semaphore>,
) -> Result<(), String> {
    // Kernel credentials are checked before constructing the framed transport,
    // so unauthenticated peers cannot make us read even a length prefix. The
    // private 0700 root admits compatible desktop and CLI executables for this
    // user; clients separately prove that this server is the recorded origin.
    let peer = peer_credentials(&stream)?;
    if peer.uid != unsafe { libc::geteuid() } || peer.pid == 0 {
        return Err("service client is not an authenticated local user process".into());
    }
    let mut transport = framed(stream);
    let first: ClientEnvelope = receive_frame(&mut transport, HANDSHAKE_TIMEOUT).await?;
    first.validate_shape().map_err(str::to_owned)?;
    let ClientEnvelope::Hello(hello) = first else {
        send_frame(
            &mut transport,
            &ServerEnvelope::HelloRejected(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "the first service envelope must be a hello",
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await?;
        return Ok(());
    };
    match validate_hello(&hello, &bootstrap) {
        Ok(()) => {
            send_hello_ack(&mut transport, &bootstrap, &coordinator, HANDSHAKE_TIMEOUT).await?;
        }
        Err(error) => {
            send_frame(
                &mut transport,
                &ServerEnvelope::HelloRejected(error),
                HANDSHAKE_TIMEOUT,
            )
            .await?;
            return Ok(());
        }
    }

    let envelope: ClientEnvelope = receive_frame(&mut transport, REQUEST_TIMEOUT).await?;
    envelope.validate_shape().map_err(str::to_owned)?;
    match envelope {
        ClientEnvelope::Request(request) => {
            let reply = execute_request(&coordinator, request).await;
            let send_result = send_frame(
                &mut transport,
                &ServerEnvelope::Reply(reply),
                REQUEST_TIMEOUT,
            )
            .await;
            send_result?;
        }
        ClientEnvelope::Subscribe { .. } => {
            let _subscription = subscriptions
                .try_acquire_owned()
                .map_err(|_| "service subscription capacity is full".to_string())?;
            stream_snapshots(transport, &coordinator).await?;
        }
        ClientEnvelope::Hello(_) => {
            return Err("service received a second hello envelope".into());
        }
    }
    Ok(())
}

async fn send_hello_ack(
    transport: &mut loxa_ipc::IpcFramed,
    bootstrap: &ClientBootstrap,
    coordinator: &Coordinator,
    deadline: Duration,
) -> Result<(), String> {
    send_frame(
        transport,
        &ServerEnvelope::HelloAck(HelloAck {
            protocol: loxa_ipc::ProtocolVersion::CURRENT,
            capabilities: CAPABILITIES.to_vec(),
            build: bootstrap.origin().build().to_owned(),
            storage_schema: STORAGE_SCHEMA,
            boot_epoch: coordinator.boot_epoch().to_owned(),
            root_identity: bootstrap.root().root_identity().to_owned(),
            service_pid: std::process::id(),
            origin_sha256: bootstrap.origin().executable_sha256().to_owned(),
        }),
        deadline,
    )
    .await
}

fn validate_hello(hello: &Hello, bootstrap: &ClientBootstrap) -> Result<(), ServiceError> {
    if hello.protocol.major != PROTOCOL_MAJOR || hello.protocol.minor > PROTOCOL_MINOR {
        return Err(ServiceError::new(
            ErrorCategory::IncompatibleProtocol,
            "client protocol is incompatible with this service",
        ));
    }
    if hello.root_identity != bootstrap.root().root_identity() {
        return Err(ServiceError::new(
            ErrorCategory::HomeMismatch,
            "client and service development roots differ",
        ));
    }
    if hello
        .required_capabilities
        .iter()
        .find(|capability| !CAPABILITIES.contains(capability))
        .is_some()
    {
        return Err(ServiceError::new(
            ErrorCategory::UnsupportedCapability,
            "service does not support a required client capability",
        ));
    }
    Ok(())
}

async fn execute_request(coordinator: &Coordinator, request: Request) -> Reply {
    let outcome = match request.command {
        ServiceCommand::Status => ReplyOutcome::Status(coordinator.status_report()),
        ServiceCommand::Load { model_id } => match coordinator.load(model_id).await {
            Ok(accepted) => ReplyOutcome::Accepted(accepted),
            Err(error) => ReplyOutcome::Rejected(error),
        },
        ServiceCommand::Unload { target } => match coordinator.unload(&target) {
            Ok(accepted) => ReplyOutcome::Accepted(accepted),
            Err(error) => ReplyOutcome::Rejected(error),
        },
        ServiceCommand::StopService => match coordinator.stop_service() {
            Ok(accepted) => ReplyOutcome::Accepted(accepted),
            Err(error) => ReplyOutcome::Rejected(error),
        },
    };
    Reply {
        request_id: request.request_id,
        outcome,
    }
}

async fn stream_snapshots(
    transport: loxa_ipc::IpcFramed,
    coordinator: &Coordinator,
) -> Result<(), String> {
    // Registration happens before the initial borrow, closing the Status ->
    // Subscribe race without requiring an unbounded replay log.
    let mut snapshots = coordinator.subscribe();
    let initial = snapshots.borrow_and_update().clone();
    let (mut writer, mut reader) = transport.split();
    send_snapshot(&mut writer, initial).await?;
    let mut stop = coordinator.server_stop_receiver();
    loop {
        tokio::select! {
            biased;
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            changed = snapshots.changed() => {
                changed.map_err(|_| "service snapshot publisher closed".to_string())?;
                let snapshot = snapshots.borrow_and_update().clone();
                send_snapshot(&mut writer, snapshot).await?;
            }
            incoming = reader.next() => {
                return match incoming {
                    None => Ok(()),
                    Some(Ok(_)) => Err("service subscription received an unexpected client frame".into()),
                    Some(Err(_)) => Err("service subscription transport failed".into()),
                };
            }
        }
    }
}

async fn send_snapshot<S>(transport: &mut S, status: RuntimeStatus) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    send_frame(
        transport,
        &ServerEnvelope::Snapshot(status),
        REQUEST_TIMEOUT,
    )
    .await
}

async fn send_frame<S, T: serde::Serialize>(
    transport: &mut S,
    value: &T,
    deadline: Duration,
) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    let bytes = encode(value)?;
    tokio::time::timeout(deadline, transport.send(bytes.freeze()))
        .await
        .map_err(|_| "service write timed out".to_string())?
        .map_err(|error| error.to_string())
}

async fn receive_frame<T: serde::de::DeserializeOwned>(
    transport: &mut loxa_ipc::IpcFramed,
    deadline: Duration,
) -> Result<T, String> {
    let bytes = tokio::time::timeout(deadline, transport.next())
        .await
        .map_err(|_| "service read timed out".to_string())?
        .ok_or_else(|| "client closed the service connection".to_string())?
        .map_err(|error| error.to_string())?;
    decode(&bytes)
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
    use loxa_ipc::initialize_development_root;
    use tempfile::TempDir;

    struct ServerFixture {
        _directory: TempDir,
        _diagnostics: Option<loxa_diagnostics::Diagnostics>,
        bootstrap: ClientBootstrap,
        coordinator: Coordinator,
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
            while *owner_exit.borrow() == OwnerExit::Running {
                owner_exit.changed().await.unwrap();
            }
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
        while *owner_exit.borrow() == OwnerExit::Running {
            owner_exit.changed().await.unwrap();
        }
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
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
        assert!(fixture.coordinator.join_owner().is_err());
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
        while *owner_exit.borrow() == OwnerExit::Running {
            owner_exit.changed().await.unwrap();
        }
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
        while *owner_exit.borrow() == OwnerExit::Running {
            owner_exit.changed().await.unwrap();
        }
        assert_eq!(*owner_exit.borrow(), OwnerExit::Drained);
        fixture.coordinator.join_owner().unwrap();
    }
}
