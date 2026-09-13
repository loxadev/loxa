use super::super::coordinator::Coordinator;
use futures_util::{SinkExt, StreamExt};
use loxa_ipc::{
    decode_with_limit, encode_with_limit, framed, peer_credentials, set_frame_limit, Capability,
    ClientBootstrap, ClientEnvelope, ErrorCategory, Hello, HelloAck, Reply, ReplyOutcome, Request,
    RuntimeStatus, ServerEnvelope, ServiceCommand, ServiceError, HISTORY_SCHEMA_VERSION,
    MAX_FRAME_BYTES, MAX_HISTORY_FRAME_BYTES, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{
    ClassifiedStop, NegotiatedHello, HANDSHAKE_TIMEOUT, LEGACY_CAPABILITIES,
    OVERLOAD_HANDSHAKE_TIMEOUT, OVERLOAD_REPLY_TIMEOUT, OVERLOAD_REQUEST_TIMEOUT, REQUEST_TIMEOUT,
};

pub(super) async fn classify_overload_connection(
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
    if hello.protocol != loxa_ipc::ProtocolVersion::V1_0 {
        return Err("overload service lane accepts only protocol 1.0 control".into());
    }
    let negotiated =
        negotiate_hello(&hello, &bootstrap, &coordinator).map_err(|error| error.context)?;
    send_hello_ack(
        &mut transport,
        &bootstrap,
        &coordinator,
        &negotiated,
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

pub(super) async fn handle_connection(
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
    let negotiated = match negotiate_hello(&hello, &bootstrap, &coordinator) {
        Ok(negotiated) => {
            send_hello_ack(
                &mut transport,
                &bootstrap,
                &coordinator,
                &negotiated,
                HANDSHAKE_TIMEOUT,
            )
            .await?;
            negotiated
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
    };

    let frame_limit = negotiated.frame_limit;
    set_frame_limit(&mut transport, frame_limit)?;

    let envelope: ClientEnvelope =
        receive_frame_with_limit(&mut transport, REQUEST_TIMEOUT, frame_limit).await?;
    envelope.validate_shape().map_err(str::to_owned)?;
    match envelope {
        ClientEnvelope::Request(request) => {
            let (reply, _history_permit) =
                execute_request(&coordinator, request, &negotiated).await;
            let send_result = send_frame_with_limit(
                &mut transport,
                &ServerEnvelope::Reply(reply),
                REQUEST_TIMEOUT,
                frame_limit,
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
    negotiated: &NegotiatedHello,
    deadline: Duration,
) -> Result<(), String> {
    send_frame(
        transport,
        &ServerEnvelope::HelloAck(HelloAck {
            protocol: negotiated.protocol,
            capabilities: negotiated.capabilities.clone(),
            build: bootstrap.origin().build().to_owned(),
            storage_schema: negotiated.storage_schema,
            boot_epoch: coordinator.boot_epoch().to_owned(),
            root_identity: bootstrap.root().root_identity().to_owned(),
            service_pid: std::process::id(),
            origin_sha256: bootstrap.origin().executable_sha256().to_owned(),
        }),
        deadline,
    )
    .await
}

fn negotiate_hello(
    hello: &Hello,
    bootstrap: &ClientBootstrap,
    coordinator: &Coordinator,
) -> Result<NegotiatedHello, ServiceError> {
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
    let capabilities = available_capabilities(coordinator, hello.protocol.minor);
    if hello
        .required_capabilities
        .iter()
        .any(|capability| !capabilities.contains(capability))
    {
        return Err(ServiceError::new(
            ErrorCategory::UnsupportedCapability,
            "service does not support a required client capability",
        ));
    }
    let storage_schema = if hello.protocol.minor == 0 {
        1
    } else if capabilities.contains(&Capability::History) {
        HISTORY_SCHEMA_VERSION
    } else {
        0
    };
    Ok(NegotiatedHello {
        protocol: hello.protocol,
        capabilities,
        storage_schema,
        frame_limit: if hello.protocol.minor == 0 {
            MAX_FRAME_BYTES
        } else {
            MAX_HISTORY_FRAME_BYTES
        },
    })
}

fn available_capabilities(coordinator: &Coordinator, minor: u16) -> Vec<Capability> {
    let mut capabilities = LEGACY_CAPABILITIES.to_vec();
    if minor >= 1 {
        capabilities.push(Capability::Settings);
        if coordinator.history_is_ready() {
            capabilities.push(Capability::History);
            capabilities.push(Capability::Drafts);
        }
    }
    capabilities
}

pub(super) async fn execute_request(
    coordinator: &Coordinator,
    request: Request,
    negotiated: &NegotiatedHello,
) -> (Reply, Option<OwnedSemaphorePermit>) {
    let mut history_permit = None;
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
        ServiceCommand::History { command: _command } if negotiated.protocol.minor == 0 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "history requires service protocol 1.1",
            ))
        }
        ServiceCommand::History { command }
            if command.requires_ready_history()
                && (!negotiated.capabilities.contains(&Capability::History)
                    || negotiated.storage_schema != HISTORY_SCHEMA_VERSION) =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service history is not ready",
            ))
        }
        ServiceCommand::History { command } => match coordinator.history(command).await {
            (Ok(reply), permit) => {
                history_permit = permit;
                ReplyOutcome::History { reply }
            }
            (Err(error), permit) => {
                history_permit = permit;
                ReplyOutcome::Rejected(error)
            }
        },
        ServiceCommand::Draft { command: _command } if negotiated.protocol.minor == 0 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "drafts require service protocol 1.1",
            ))
        }
        ServiceCommand::Draft { command: _ }
            if !negotiated.capabilities.contains(&Capability::Drafts)
                || negotiated.storage_schema != HISTORY_SCHEMA_VERSION =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service drafts are not ready",
            ))
        }
        ServiceCommand::Draft { command } => match coordinator.draft(command).await {
            (Ok(reply), permit) => {
                history_permit = permit;
                ReplyOutcome::Draft { reply }
            }
            (Err(error), permit) => {
                history_permit = permit;
                ReplyOutcome::Rejected(error)
            }
        },
        ServiceCommand::Settings { command: _ } if negotiated.protocol.minor == 0 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "settings require service protocol 1.1",
            ))
        }
        ServiceCommand::Settings { command: _ }
            if !negotiated.capabilities.contains(&Capability::Settings) =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service settings are not ready",
            ))
        }
        ServiceCommand::Settings { command }
            if command.requires_ready_history()
                && (!negotiated.capabilities.contains(&Capability::History)
                    || negotiated.storage_schema != HISTORY_SCHEMA_VERSION) =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "conversation profiles are not ready; reconnect after history opens",
            ))
        }
        ServiceCommand::Settings { command } => match coordinator.settings(command).await {
            (Ok(reply), permit) => {
                history_permit = permit;
                ReplyOutcome::Settings { reply }
            }
            (Err(error), permit) => {
                history_permit = permit;
                ReplyOutcome::Rejected(error)
            }
        },
    };
    (
        Reply {
            request_id: request.request_id,
            outcome,
        },
        history_permit,
    )
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

pub(super) async fn send_frame<S, T: serde::Serialize>(
    transport: &mut S,
    value: &T,
    deadline: Duration,
) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    send_frame_with_limit(transport, value, deadline, MAX_FRAME_BYTES).await
}

pub(super) async fn send_frame_with_limit<S, T: serde::Serialize>(
    transport: &mut S,
    value: &T,
    deadline: Duration,
    frame_limit: usize,
) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    let bytes = encode_with_limit(value, frame_limit)?;
    tokio::time::timeout(deadline, transport.send(bytes.freeze()))
        .await
        .map_err(|_| "service write timed out".to_string())?
        .map_err(|error| error.to_string())
}

pub(super) async fn receive_frame<T: serde::de::DeserializeOwned>(
    transport: &mut loxa_ipc::IpcFramed,
    deadline: Duration,
) -> Result<T, String> {
    receive_frame_with_limit(transport, deadline, MAX_FRAME_BYTES).await
}

pub(super) async fn receive_frame_with_limit<T: serde::de::DeserializeOwned>(
    transport: &mut loxa_ipc::IpcFramed,
    deadline: Duration,
    frame_limit: usize,
) -> Result<T, String> {
    let bytes = tokio::time::timeout(deadline, transport.next())
        .await
        .map_err(|_| "service read timed out".to_string())?
        .ok_or_else(|| "client closed the service connection".to_string())?
        .map_err(|error| error.to_string())?;
    decode_with_limit(&bytes, frame_limit)
}
