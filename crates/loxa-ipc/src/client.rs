use crate::bootstrap::ClientBootstrap;
use crate::codec::{
    decode_with_limit, encode_with_limit, framed, set_frame_limit, MAX_FRAME_BYTES,
    MAX_HISTORY_FRAME_BYTES,
};
use crate::protocol::{
    Capability, ClientEnvelope, DraftCommand, DraftReply, ErrorCategory, GenerationCommand,
    GenerationReply, Hello, HistoryCommand, HistoryReply, HistoryStatus, Reply, Request,
    ServerEnvelope, ServiceCommand, ServiceError, ServiceSettingsCommand, ServiceSettingsReply,
    HISTORY_SCHEMA_VERSION,
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

pub struct PendingGenerationRequest {
    connection: Connection,
    target: crate::GenerationTarget,
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
        if matches!(
            &command,
            ServiceCommand::History { .. }
                | ServiceCommand::Draft { .. }
                | ServiceCommand::Settings { .. }
                | ServiceCommand::Generation { .. }
                | ServiceCommand::GetGenerationStatus { .. }
        ) {
            return Err(ClientError::Transport(
                "this command requires an explicit versioned client entry point".into(),
            ));
        }
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let expects_status = matches!(command, ServiceCommand::Status);
        let mut connection = self.connect(mode).await?;
        let expected_boot_epoch = connection.hello.boot_epoch.clone();
        let outcome = connection.request(command).await?;
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
            ReplyOutcome::Status(_)
            | ReplyOutcome::Accepted(_)
            | ReplyOutcome::History { .. }
            | ReplyOutcome::Draft { .. }
            | ReplyOutcome::Settings { .. }
            | ReplyOutcome::Generation { .. }
            | ReplyOutcome::GenerationStatus { .. } => Err(ClientError::Transport(
                "service returned an outcome for a different command".into(),
            )),
        }
    }

    /// Observe an accepted attempt without starting the service or waiting for history.
    /// After admission is released, use its accepted IDs to read durable history.
    pub async fn generation_status(
        &self,
        target: &crate::GenerationTarget,
    ) -> Result<Option<crate::GenerationStatus>, ClientError> {
        target
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let mut connection = self
            .connect_for(
                ConnectMode::ObserveExisting,
                ClientContract::GenerationStatus,
            )
            .await?;
        match connection
            .request(ServiceCommand::GetGenerationStatus {
                target: target.clone(),
            })
            .await?
        {
            ReplyOutcome::GenerationStatus { snapshot } => {
                if let Some(snapshot) = &snapshot {
                    let current_boot = matches!(
                        &snapshot.target,
                        crate::GenerationTarget::Accepted { boot_epoch, .. }
                            if boot_epoch == &connection.hello.boot_epoch
                    );
                    if &snapshot.target != target || !current_boot {
                        return Err(ClientError::Transport(
                            "service returned a different generation snapshot".into(),
                        ));
                    }
                }
                Ok(snapshot)
            }
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different generation status request".into(),
            )),
        }
    }

    pub async fn generation_request(
        &self,
        mode: ConnectMode,
        command: GenerationCommand,
    ) -> Result<GenerationReply, ClientError> {
        if !command.is_stop() {
            return Err(ClientError::Transport(
                "generation Send requires a prepared generation connection".into(),
            ));
        }
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let contract = ClientContract::GenerationControl;
        let mut connection = self.connect_for(mode, contract).await?;
        if connection.hello.storage_schema != HISTORY_SCHEMA_VERSION
            || !connection.hello.capabilities.contains(&Capability::History)
        {
            return Err(ClientError::Transport(
                "service generation history is not ready with a supported schema".into(),
            ));
        }
        let outcome = connection
            .request(ServiceCommand::Generation { command })
            .await?;
        match outcome {
            ReplyOutcome::Generation { reply } => Ok(reply),
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different generation command".into(),
            )),
        }
    }

    pub async fn prepare_generation(
        &self,
        mode: ConnectMode,
    ) -> Result<PendingGenerationRequest, ClientError> {
        let connection = self
            .connect_for(mode, ClientContract::GenerationRequest)
            .await?;
        if connection.hello.storage_schema != HISTORY_SCHEMA_VERSION
            || !connection.hello.capabilities.contains(&Capability::History)
        {
            return Err(ClientError::Transport(
                "service generation history is not ready with a supported schema".into(),
            ));
        }
        let generation = connection.hello.generation.as_ref().ok_or_else(|| {
            ClientError::Transport("service did not issue a pending generation identity".into())
        })?;
        let target = crate::GenerationTarget::Pending {
            boot_epoch: connection.hello.boot_epoch.clone(),
            pending_nonce: generation.pending_nonce.clone(),
        };
        Ok(PendingGenerationRequest { connection, target })
    }

    pub async fn draft_request(
        &self,
        mode: ConnectMode,
        command: DraftCommand,
    ) -> Result<DraftReply, ClientError> {
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let mut connection = self.connect_for(mode, ClientContract::History).await?;
        if !connection.hello.capabilities.contains(&Capability::Drafts)
            || connection.hello.storage_schema != HISTORY_SCHEMA_VERSION
        {
            return Err(ClientError::Transport(
                "service drafts are not ready with a supported schema".into(),
            ));
        }
        let outcome = connection
            .request(ServiceCommand::Draft { command })
            .await?;
        match outcome {
            ReplyOutcome::Draft { reply } => Ok(reply),
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different draft command".into(),
            )),
        }
    }

    pub async fn settings_request(
        &self,
        mode: ConnectMode,
        command: ServiceSettingsCommand,
    ) -> Result<ServiceSettingsReply, ClientError> {
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let mut connection = self.connect_for(mode, ClientContract::History).await?;
        validate_settings_contract(&command, &connection.hello)?;
        let outcome = connection
            .request(ServiceCommand::Settings { command })
            .await?;
        match outcome {
            ReplyOutcome::Settings { reply } => Ok(reply),
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different settings command".into(),
            )),
        }
    }

    pub async fn history_status(&self, mode: ConnectMode) -> Result<HistoryStatus, ClientError> {
        match self
            .history_request_with_contract(
                mode,
                HistoryCommand::GetHistoryStatus,
                ClientContract::HistoryStatus,
            )
            .await?
        {
            HistoryReply::Status(status) => Ok(status),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different history command".into(),
            )),
        }
    }

    pub async fn history_request(
        &self,
        mode: ConnectMode,
        command: HistoryCommand,
    ) -> Result<HistoryReply, ClientError> {
        if matches!(command, HistoryCommand::GetHistoryStatus) {
            return self
                .history_request_with_contract(mode, command, ClientContract::HistoryStatus)
                .await;
        }
        self.history_request_with_contract(mode, command, ClientContract::History)
            .await
    }

    async fn history_request_with_contract(
        &self,
        mode: ConnectMode,
        command: HistoryCommand,
        contract: ClientContract,
    ) -> Result<HistoryReply, ClientError> {
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let mut connection = self.connect_for(mode, contract).await?;
        if contract == ClientContract::History
            && (!connection.hello.capabilities.contains(&Capability::History)
                || connection.hello.storage_schema != HISTORY_SCHEMA_VERSION)
        {
            return Err(ClientError::Transport(
                "service history is not ready with a supported schema".into(),
            ));
        }
        let outcome = connection
            .request(ServiceCommand::History { command })
            .await?;
        match outcome {
            ReplyOutcome::History { reply } => Ok(reply),
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different history command".into(),
            )),
        }
    }

    /// Opens the long-lived state stream. The first snapshot is captured by the
    /// service at subscription registration, before later revisions can be
    /// published, so callers do not have to close a status/subscription gap.
    pub async fn subscribe(&self, mode: ConnectMode) -> Result<ServiceSubscription, ClientError> {
        let mut connection = self.connect(mode).await?;
        let request_id = next_request_id();
        send_frame(
            &mut connection.framed,
            &ClientEnvelope::Subscribe { request_id },
            REQUEST_TIMEOUT,
            connection.frame_limit,
        )
        .await?;
        let expected_boot_epoch = connection.hello.boot_epoch;
        let initial = snapshot_from(
            receive_envelope(
                &mut connection.framed,
                REQUEST_TIMEOUT,
                connection.frame_limit,
            )
            .await?,
        )?;
        let initial_revision = validate_projection(&initial, &expected_boot_epoch)?;
        Ok(ServiceSubscription {
            framed: connection.framed,
            initial: Some(initial),
            expected_boot_epoch,
            last_revision: initial_revision,
            frame_limit: connection.frame_limit,
        })
    }

    async fn connect(&self, mode: ConnectMode) -> Result<Connection, ClientError> {
        self.connect_for(mode, ClientContract::Legacy).await
    }

    async fn connect_for(
        &self,
        mode: ConnectMode,
        contract: ClientContract,
    ) -> Result<Connection, ClientError> {
        match self.connect_once(contract).await {
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
            match self.connect_once(contract).await {
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

    async fn connect_once(&self, contract: ClientContract) -> Result<Connection, ClientError> {
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
        let requested_hello = match contract {
            ClientContract::Legacy => Hello::current(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
            ),
            ClientContract::HistoryStatus => Hello::history_status(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
            ),
            ClientContract::History => Hello::history(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
            ),
            ClientContract::GenerationStatus => Hello::generation_status(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
            ),
            ClientContract::GenerationRequest => Hello::generation(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
                crate::GenerationConnection::Request,
            ),
            ClientContract::GenerationControl => Hello::generation(
                self.client_build.clone(),
                self.bootstrap.root().root_identity(),
                crate::GenerationConnection::Control,
            ),
        };
        send_frame(
            &mut framed,
            &ClientEnvelope::Hello(requested_hello.clone()),
            HANDSHAKE_TIMEOUT,
            MAX_FRAME_BYTES,
        )
        .await?;
        let envelope = receive_envelope(&mut framed, HANDSHAKE_TIMEOUT, MAX_FRAME_BYTES).await?;
        let hello = match envelope {
            ServerEnvelope::HelloAck(hello) => hello,
            ServerEnvelope::HelloRejected(error) => return Err(ClientError::Rejected(error)),
            ServerEnvelope::Reply(_) | ServerEnvelope::Snapshot(_) => {
                return Err(ClientError::Transport(
                    "service replied before the handshake completed".into(),
                ))
            }
        };
        let generation_shape_matches = match contract {
            ClientContract::GenerationRequest => {
                hello.generation.is_some() && hello.protocol == crate::ProtocolVersion::V1_2
            }
            ClientContract::GenerationControl => {
                hello.generation.is_none() && hello.protocol == crate::ProtocolVersion::V1_2
            }
            ClientContract::GenerationStatus
            | ClientContract::Legacy
            | ClientContract::HistoryStatus
            | ClientContract::History => hello.generation.is_none(),
        };
        if !protocol_is_compatible(hello.protocol, requested_hello.protocol)
            || !generation_shape_matches
            || hello.root_identity != self.bootstrap.root().root_identity()
            || hello.service_pid != peer.pid
            || hello.origin_sha256 != self.bootstrap.origin().executable_sha256()
            || !requested_hello
                .required_capabilities
                .iter()
                .all(|required| hello.capabilities.contains(required))
        {
            return Err(ClientError::Transport(
                "service handshake identity or compatibility check failed".into(),
            ));
        }
        let frame_limit = if hello.protocol.minor == 0 {
            MAX_FRAME_BYTES
        } else {
            MAX_HISTORY_FRAME_BYTES
        };
        set_frame_limit(&mut framed, frame_limit).map_err(ClientError::Transport)?;
        Ok(Connection {
            framed,
            hello,
            frame_limit,
        })
    }
}

fn protocol_is_compatible(
    actual: crate::ProtocolVersion,
    required: crate::ProtocolVersion,
) -> bool {
    actual.major == required.major && actual.minor >= required.minor
}

fn validate_settings_contract(
    command: &ServiceSettingsCommand,
    hello: &crate::HelloAck,
) -> Result<(), ClientError> {
    if !hello.capabilities.contains(&Capability::Settings) {
        return Err(ClientError::Transport(
            "service settings are not ready".into(),
        ));
    }
    if command.requires_ready_history()
        && (!hello.capabilities.contains(&Capability::History)
            || hello.storage_schema != HISTORY_SCHEMA_VERSION)
    {
        return Err(ClientError::Transport(
            "conversation profiles are not ready with a supported schema; reconnect after history opens"
                .into(),
        ));
    }
    Ok(())
}

pub struct ServiceSubscription {
    framed: crate::codec::IpcFramed,
    initial: Option<crate::RuntimeStatus>,
    expected_boot_epoch: String,
    last_revision: u64,
    frame_limit: usize,
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
            let status = snapshot_from(decode_envelope(&frame, self.frame_limit)?)?;
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
    frame_limit: usize,
}

impl Connection {
    async fn request(&mut self, command: ServiceCommand) -> Result<ReplyOutcome, ClientError> {
        let request_id = next_request_id();
        send_frame(
            &mut self.framed,
            &ClientEnvelope::Request(Request::new(request_id.clone(), command)),
            REQUEST_TIMEOUT,
            self.frame_limit,
        )
        .await?;
        let envelope =
            receive_envelope(&mut self.framed, REQUEST_TIMEOUT, self.frame_limit).await?;
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
        Ok(outcome)
    }
}

impl PendingGenerationRequest {
    pub fn target(&self) -> &crate::GenerationTarget {
        &self.target
    }

    pub async fn send(
        mut self,
        command: GenerationCommand,
    ) -> Result<GenerationReply, ClientError> {
        if command.is_stop() {
            return Err(ClientError::Transport(
                "prepared generation connection accepts only Send".into(),
            ));
        }
        command
            .validate_shape()
            .map_err(|error| ClientError::Transport(error.into()))?;
        let outcome = self
            .connection
            .request(ServiceCommand::Generation { command })
            .await?;
        match outcome {
            ReplyOutcome::Generation { reply } => Ok(reply),
            ReplyOutcome::Rejected(error) => Err(ClientError::Rejected(error)),
            _ => Err(ClientError::Transport(
                "service returned an outcome for a different generation command".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientContract {
    Legacy,
    HistoryStatus,
    History,
    GenerationRequest,
    GenerationControl,
    GenerationStatus,
}

async fn send_frame<T: serde::Serialize>(
    framed: &mut crate::codec::IpcFramed,
    value: &T,
    deadline: Duration,
    frame_limit: usize,
) -> Result<(), ClientError> {
    let bytes = encode_with_limit(value, frame_limit).map_err(ClientError::Transport)?;
    timeout(deadline, framed.send(bytes.freeze()))
        .await
        .map_err(|_| ClientError::Transport("service write timed out".into()))?
        .map_err(|error| ClientError::Transport(error.to_string()))
}

async fn receive_envelope(
    framed: &mut crate::codec::IpcFramed,
    deadline: Duration,
    frame_limit: usize,
) -> Result<ServerEnvelope, ClientError> {
    let frame = timeout(deadline, framed.next())
        .await
        .map_err(|_| ClientError::Transport("service response timed out".into()))?
        .ok_or_else(|| ClientError::Transport("service closed the connection".into()))?
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    decode_envelope(&frame, frame_limit)
}

fn decode_envelope(frame: &[u8], frame_limit: usize) -> Result<ServerEnvelope, ClientError> {
    let envelope: ServerEnvelope =
        decode_with_limit(frame, frame_limit).map_err(ClientError::Transport)?;
    envelope
        .validate_shape()
        .map_err(|error| ClientError::Transport(error.into()))?;
    Ok(envelope)
}

fn next_request_id() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
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
    use bytes::Bytes;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    enum TestResponse {
        Matching(Box<ReplyOutcome>),
        MismatchedId,
        UnexpectedEnvelope,
        MalformedJson,
        InvalidShape,
        PeerEof,
    }

    fn settings_hello(capabilities: Vec<Capability>, storage_schema: u32) -> crate::HelloAck {
        crate::HelloAck {
            protocol: crate::ProtocolVersion::CURRENT,
            capabilities,
            build: "test-build".into(),
            storage_schema,
            boot_epoch: "test-boot".into(),
            root_identity: "test-root".into(),
            service_pid: 1,
            origin_sha256: "00".repeat(32),
            generation: None,
        }
    }

    fn request_with_response(response: TestResponse) -> Result<ReplyOutcome, ClientError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (client, server) = UnixStream::pair().unwrap();
            let mut connection = Connection {
                framed: framed(client),
                hello: settings_hello(Vec::new(), 0),
                frame_limit: MAX_FRAME_BYTES,
            };
            let peer = tokio::spawn(async move {
                let mut server = framed(server);
                let frame = server.next().await.unwrap().unwrap();
                let request: ClientEnvelope = decode_with_limit(&frame, MAX_FRAME_BYTES).unwrap();
                let ClientEnvelope::Request(request) = request else {
                    panic!("connection helper sent a non-request envelope");
                };
                match response {
                    TestResponse::Matching(outcome) => {
                        send_frame(
                            &mut server,
                            &ServerEnvelope::Reply(Reply {
                                request_id: request.request_id,
                                outcome: *outcome,
                            }),
                            REQUEST_TIMEOUT,
                            MAX_FRAME_BYTES,
                        )
                        .await
                        .unwrap();
                    }
                    TestResponse::MismatchedId => {
                        send_frame(
                            &mut server,
                            &ServerEnvelope::Reply(Reply {
                                request_id: "different-request".into(),
                                outcome: ReplyOutcome::Rejected(ServiceError::new(
                                    ErrorCategory::Busy,
                                    "busy",
                                )),
                            }),
                            REQUEST_TIMEOUT,
                            MAX_FRAME_BYTES,
                        )
                        .await
                        .unwrap();
                    }
                    TestResponse::UnexpectedEnvelope => {
                        send_frame(
                            &mut server,
                            &ServerEnvelope::HelloRejected(ServiceError::new(
                                ErrorCategory::Busy,
                                "busy",
                            )),
                            REQUEST_TIMEOUT,
                            MAX_FRAME_BYTES,
                        )
                        .await
                        .unwrap();
                    }
                    TestResponse::MalformedJson => {
                        server.send(Bytes::from_static(b"{")).await.unwrap();
                    }
                    TestResponse::InvalidShape => {
                        let bytes = encode_with_limit(
                            &ServerEnvelope::Reply(Reply {
                                request_id: String::new(),
                                outcome: ReplyOutcome::Rejected(ServiceError::new(
                                    ErrorCategory::Busy,
                                    "busy",
                                )),
                            }),
                            MAX_FRAME_BYTES,
                        )
                        .unwrap();
                        server.send(bytes.freeze()).await.unwrap();
                    }
                    TestResponse::PeerEof => {}
                }
            });
            let result = connection.request(ServiceCommand::Status).await;
            peer.await.unwrap();
            result
        })
    }

    #[test]
    fn connection_request_returns_a_matching_valid_reply() {
        let expected = ReplyOutcome::Rejected(ServiceError::new(ErrorCategory::Busy, "busy"));
        assert_eq!(
            request_with_response(TestResponse::Matching(Box::new(expected.clone()))).unwrap(),
            expected
        );
    }

    #[test]
    fn connection_request_rejects_wrong_envelopes_and_request_identities() {
        assert!(matches!(
            request_with_response(TestResponse::MismatchedId),
            Err(ClientError::Transport(message)) if message.contains("request identity changed")
        ));
        assert!(matches!(
            request_with_response(TestResponse::UnexpectedEnvelope),
            Err(ClientError::Transport(message)) if message.contains("unexpected envelope")
        ));
    }

    #[test]
    fn connection_request_rejects_malformed_invalid_and_closed_peer_replies() {
        assert!(matches!(
            request_with_response(TestResponse::MalformedJson),
            Err(ClientError::Transport(_))
        ));
        assert!(matches!(
            request_with_response(TestResponse::InvalidShape),
            Err(ClientError::Transport(message)) if message.contains("invalid reply request identity")
        ));
        assert!(matches!(
            request_with_response(TestResponse::PeerEof),
            Err(ClientError::Transport(message)) if message.contains("closed the connection")
        ));
    }

    #[test]
    fn profile_settings_require_a_ready_history_handshake() {
        let global = ServiceSettingsCommand::GetServiceSettings;
        let profile = ServiceSettingsCommand::GetConversationProfile {
            conversation_id: "00".repeat(16),
        };
        let opening = settings_hello(vec![Capability::Settings], 0);
        assert!(validate_settings_contract(&global, &opening).is_ok());
        assert!(matches!(
            validate_settings_contract(&profile, &opening),
            Err(ClientError::Transport(message))
                if message.contains("reconnect after history opens")
        ));

        let missing_settings = settings_hello(vec![Capability::History], HISTORY_SCHEMA_VERSION);
        for command in [&global, &profile] {
            assert!(matches!(
                validate_settings_contract(command, &missing_settings),
                Err(ClientError::Transport(message)) if message.contains("settings are not ready")
            ));
        }

        let wrong_schema = settings_hello(
            vec![Capability::Settings, Capability::History],
            HISTORY_SCHEMA_VERSION + 1,
        );
        assert!(validate_settings_contract(&global, &wrong_schema).is_ok());
        assert!(matches!(
            validate_settings_contract(&profile, &wrong_schema),
            Err(ClientError::Transport(message))
                if message.contains("reconnect after history opens")
        ));

        let ready = settings_hello(
            vec![Capability::Settings, Capability::History],
            HISTORY_SCHEMA_VERSION,
        );
        assert!(validate_settings_contract(&profile, &ready).is_ok());
    }

    #[test]
    fn stale_client_refuses_a_replaced_root_before_spawning_the_recorded_origin() {
        let parent = tempfile::Builder::new()
            .prefix("li-")
            .tempdir_in("/tmp")
            .unwrap();
        let parent = fs::canonicalize(parent.path()).unwrap();
        let forbidden = parent.join("normal");
        let root = parent.join("dev");
        let moved = parent.join("dev-moved");
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
