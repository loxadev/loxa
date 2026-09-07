use crate::bootstrap::ClientBootstrap;
use crate::codec::{decode, encode, framed};
use crate::protocol::{
    ClientEnvelope, ErrorCategory, Hello, Reply, Request, ServerEnvelope, ServiceCommand,
    ServiceError,
};
use crate::{peer_credentials, ReplyOutcome};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static BOOTSTRAP_LANE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::const_new(1)));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectMode {
    ObserveExisting,
    EnsureStarted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    Absent,
    Transport(String),
    Rejected(ServiceError),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => formatter.write_str("Loxa background service is not running"),
            Self::Transport(message) => formatter.write_str(message),
            Self::Rejected(error) => formatter.write_str(&error.context),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone, Debug)]
pub struct ServiceClient {
    bootstrap: ClientBootstrap,
    client_build: String,
}

impl ServiceClient {
    pub fn load(
        data_root: &Path,
        forbidden_root: Option<&Path>,
        client_build: impl Into<String>,
    ) -> Result<Self, String> {
        let client_build = client_build.into();
        if client_build.is_empty() || client_build.len() > crate::protocol::MAX_BUILD_BYTES {
            return Err("invalid client build identity".into());
        }
        Ok(Self {
            bootstrap: ClientBootstrap::load(data_root, forbidden_root)?,
            client_build,
        })
    }

    pub async fn load_async(
        data_root: &Path,
        forbidden_root: Option<&Path>,
        client_build: impl Into<String>,
    ) -> Result<Self, String> {
        let data_root = data_root.to_path_buf();
        let forbidden_root = forbidden_root.map(Path::to_path_buf);
        let client_build = client_build.into();
        run_bootstrap(move || Self::load(&data_root, forbidden_root.as_deref(), client_build)).await
    }

    pub fn bootstrap(&self) -> &ClientBootstrap {
        &self.bootstrap
    }

    pub async fn hello(&self, mode: ConnectMode) -> Result<crate::HelloAck, ClientError> {
        let connection = self.connect(mode).await?;
        Ok(connection.hello)
    }

    pub async fn request(
        &self,
        mode: ConnectMode,
        command: ServiceCommand,
    ) -> Result<ReplyOutcome, ClientError> {
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let expects_status = matches!(command, ServiceCommand::Status);
        let mut connection = self.connect(mode).await?;
        let expected_boot_epoch = connection.hello.boot_epoch.clone();
        let request_id = format!(
            "{}-{}",
            std::process::id(),
            NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
        );
        let request = Request::new(request_id.clone(), command);
        send_frame(
            &mut connection.framed,
            &ClientEnvelope::Request(request),
            REQUEST_TIMEOUT,
        )
        .await?;
        let envelope = receive_envelope(&mut connection.framed, REQUEST_TIMEOUT).await?;
        let ServerEnvelope::Reply(Reply {
            request_id: response_id,
            outcome,
        }) = envelope
        else {
            return Err(ClientError::Transport(
                "service returned an unexpected envelope".into(),
            ));
        };
        if response_id != request_id {
            return Err(ClientError::Transport(
                "service reply request identity changed".into(),
            ));
        }
        match outcome {
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            ReplyOutcome::Status(status) if expects_status => {
                validate_projection(&status.runtime, &expected_boot_epoch)?;
                Ok(ReplyOutcome::Status(status))
            }
            ReplyOutcome::Accepted(accepted) if !expects_status => {
                if accepted.boot_epoch != expected_boot_epoch {
                    return Err(ClientError::Transport(
                        "service acceptance boot epoch changed".into(),
                    ));
                }
                Ok(ReplyOutcome::Accepted(accepted))
            }
            ReplyOutcome::Status(_) | ReplyOutcome::Accepted(_) => Err(ClientError::Transport(
                "service returned an outcome for a different command".into(),
            )),
        }
    }

    /// Opens the long-lived state stream. The first snapshot is captured by the
    /// service at subscription registration, before later revisions can be
    /// published, so callers do not have to close a status/subscription gap.
    pub async fn subscribe(&self, mode: ConnectMode) -> Result<ServiceSubscription, ClientError> {
        let mut connection = self.connect(mode).await?;
        let request_id = format!(
            "{}-{}",
            std::process::id(),
            NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
        );
        send_frame(
            &mut connection.framed,
            &ClientEnvelope::Subscribe { request_id },
            REQUEST_TIMEOUT,
        )
        .await?;
        let expected_boot_epoch = connection.hello.boot_epoch;
        let initial =
            snapshot_from(receive_envelope(&mut connection.framed, REQUEST_TIMEOUT).await?)?;
        let initial_revision = validate_projection(&initial, &expected_boot_epoch)?;
        Ok(ServiceSubscription {
            framed: connection.framed,
            initial: Some(initial),
            expected_boot_epoch,
            last_revision: initial_revision,
        })
    }

    async fn connect(&self, mode: ConnectMode) -> Result<Connection, ClientError> {
        match self.connect_once().await {
            Ok(connection) => return Ok(connection),
            Err(ClientError::Absent) if mode == ConnectMode::ObserveExisting => {
                return Err(ClientError::Absent)
            }
            Err(ClientError::Absent) => {}
            Err(error) => return Err(error),
        }
        let bootstrap = self.bootstrap.clone();
        run_bootstrap(move || {
            bootstrap.root().validate_current().map_err(|_| {
                ClientError::Rejected(ServiceError::new(
                    ErrorCategory::HomeMismatch,
                    "development data root identity changed; restart Loxa",
                ))
            })?;
            bootstrap.launch_service().map_err(ClientError::Transport)
        })
        .await?;
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            match self.connect_once().await {
                Ok(connection) => return Ok(connection),
                Err(ClientError::Absent) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(25)).await;
                }
                Err(ClientError::Absent) => {
                    return Err(ClientError::Transport(
                        "Loxa background service did not become ready".into(),
                    ))
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn connect_once(&self) -> Result<Connection, ClientError> {
        validate_socket_path(self.bootstrap.root().socket_path())?;
        let stream = match timeout(
            CONNECT_TIMEOUT,
            UnixStream::connect(self.bootstrap.root().socket_path()),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                return Err(ClientError::Absent)
            }
            Ok(Err(error)) => return Err(ClientError::Transport(error.to_string())),
            Err(_) => return Err(ClientError::Transport("service connect timed out".into())),
        };
        let peer = peer_credentials(&stream).map_err(ClientError::Transport)?;
        self.bootstrap
            .validate_peer(peer)
            .map_err(ClientError::Transport)?;
        let mut framed = framed(stream);
        send_frame(
            &mut framed,
            &ClientEnvelope::Hello(Hello::current(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
            )),
            HANDSHAKE_TIMEOUT,
        )
        .await?;
        let envelope = receive_envelope(&mut framed, HANDSHAKE_TIMEOUT).await?;
        let hello = match envelope {
            ServerEnvelope::HelloAck(hello) => hello,
            ServerEnvelope::HelloRejected(error) => return Err(ClientError::Rejected(error)),
            ServerEnvelope::Reply(_) | ServerEnvelope::Snapshot(_) => {
                return Err(ClientError::Transport(
                    "service replied before the handshake completed".into(),
                ))
            }
        };
        if !protocol_is_compatible(hello.protocol, crate::ProtocolVersion::CURRENT)
            || hello.root_identity != self.bootstrap.root().root_identity()
            || hello.service_pid != peer.pid
            || hello.origin_sha256 != self.bootstrap.origin().executable_sha256()
            || !crate::protocol::REQUIRED_CAPABILITIES
                .iter()
                .all(|required| hello.capabilities.contains(required))
        {
            return Err(ClientError::Transport(
                "service handshake identity or compatibility check failed".into(),
            ));
        }
        Ok(Connection { framed, hello })
    }
}

fn protocol_is_compatible(
    actual: crate::ProtocolVersion,
    required: crate::ProtocolVersion,
) -> bool {
    actual.major == required.major && actual.minor >= required.minor
}

pub struct ServiceSubscription {
    framed: crate::codec::IpcFramed,
    initial: Option<crate::RuntimeStatus>,
    expected_boot_epoch: String,
    last_revision: u64,
}

impl ServiceSubscription {
    pub async fn next_snapshot(&mut self) -> Result<crate::RuntimeStatus, ClientError> {
        if let Some(initial) = self.initial.take() {
            return Ok(initial);
        }
        loop {
            let frame = self
                .framed
                .next()
                .await
                .ok_or_else(|| ClientError::Transport("service closed the subscription".into()))?
                .map_err(|error| ClientError::Transport(error.to_string()))?;
            let status = snapshot_from(decode_envelope(&frame)?)?;
            let revision = validate_projection(&status, &self.expected_boot_epoch)?;
            if revision < self.last_revision {
                return Err(ClientError::Transport(
                    "service subscription state revision regressed".into(),
                ));
            }
            if revision == self.last_revision {
                continue;
            }
            self.last_revision = revision;
            return Ok(status);
        }
    }
}

struct Connection {
    framed: crate::codec::IpcFramed,
    hello: crate::HelloAck,
}

async fn send_frame<T: serde::Serialize>(
    framed: &mut crate::codec::IpcFramed,
    value: &T,
    deadline: Duration,
) -> Result<(), ClientError> {
    let bytes = encode(value).map_err(ClientError::Transport)?;
    timeout(deadline, framed.send(bytes.freeze()))
        .await
        .map_err(|_| ClientError::Transport("service write timed out".into()))?
        .map_err(|error| ClientError::Transport(error.to_string()))
}

async fn receive_envelope(
    framed: &mut crate::codec::IpcFramed,
    deadline: Duration,
) -> Result<ServerEnvelope, ClientError> {
    let frame = timeout(deadline, framed.next())
        .await
        .map_err(|_| ClientError::Transport("service response timed out".into()))?
        .ok_or_else(|| ClientError::Transport("service closed the connection".into()))?
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    decode_envelope(&frame)
}

fn decode_envelope(frame: &[u8]) -> Result<ServerEnvelope, ClientError> {
    let envelope: ServerEnvelope = decode(frame).map_err(ClientError::Transport)?;
    envelope
        .validate_shape()
        .map_err(|error| ClientError::Transport(error.into()))?;
    Ok(envelope)
}

fn snapshot_from(envelope: ServerEnvelope) -> Result<crate::RuntimeStatus, ClientError> {
    let ServerEnvelope::Snapshot(status) = envelope else {
        return Err(ClientError::Transport(
            "service returned a non-snapshot subscription envelope".into(),
        ));
    };
    Ok(status)
}

fn validate_socket_path(path: &Path) -> Result<(), ClientError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ClientError::Absent)
        }
        Err(error) => return Err(ClientError::Transport(error.to_string())),
    };
    if !metadata.file_type().is_socket()
        || metadata.uid() != crate::platform::current_uid()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(ClientError::Transport(
            "service socket path is not a private user-owned socket".into(),
        ));
    }
    Ok(())
}

fn validate_projection(
    status: &crate::RuntimeStatus,
    expected_epoch: &str,
) -> Result<u64, ClientError> {
    if status.boot_epoch != expected_epoch {
        return Err(ClientError::Transport(
            "service subscription boot epoch changed".into(),
        ));
    }
    status
        .state_revision
        .parse::<u64>()
        .map_err(|_| ClientError::Transport("invalid service state revision".into()))
}

async fn run_bootstrap<T, E, F>(work: F) -> Result<T, E>
where
    T: Send + 'static,
    E: From<String> + Send + 'static,
    F: FnOnce() -> Result<T, E> + Send + 'static,
{
    let permit = Arc::clone(&BOOTSTRAP_LANE)
        .try_acquire_owned()
        .map_err(|_| E::from("service bootstrap is already in progress".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|error| E::from(format!("service bootstrap worker failed: {error}")))?
}

impl From<String> for ClientError {
    fn from(message: String) -> Self {
        Self::Transport(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::initialize_development_root;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn stale_client_refuses_a_replaced_root_before_spawning_the_recorded_origin() {
        let parent = tempfile::Builder::new()
            .prefix("loxa-ipc-client-")
            .tempdir_in("/tmp")
            .unwrap();
        let parent = fs::canonicalize(parent.path()).unwrap();
        let forbidden = parent.join("normal");
        let root = parent.join("development");
        let moved = parent.join("development-moved");
        let origin = parent.join("origin.sh");
        let witness = parent.join("origin-spawned");
        fs::create_dir(&forbidden).unwrap();
        fs::set_permissions(&forbidden, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(&origin, format!("#!/bin/sh\n: > '{}'\n", witness.display())).unwrap();
        fs::set_permissions(&origin, fs::Permissions::from_mode(0o700)).unwrap();

        initialize_development_root(&root, &forbidden, &origin, "test-build").unwrap();
        let client = ServiceClient::load(&root, Some(&forbidden), "test-build").unwrap();
        fs::rename(&root, &moved).unwrap();
        initialize_development_root(&root, &forbidden, &origin, "test-build").unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(client.hello(ConnectMode::EnsureStarted));
        assert!(matches!(
            result,
            Err(ClientError::Rejected(ServiceError {
                category: ErrorCategory::HomeMismatch,
                ..
            }))
        ));
        assert!(
            !witness.exists(),
            "stale client spawned the recorded origin"
        );
        assert!(!root.join("run/service/control.sock").exists());
    }
}
