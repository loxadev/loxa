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
    ClassifiedStop, ClassifiedStopAction, NegotiatedHello, HANDSHAKE_TIMEOUT, LEGACY_CAPABILITIES,
    OVERLOAD_HANDSHAKE_TIMEOUT, OVERLOAD_REPLY_TIMEOUT, OVERLOAD_REQUEST_TIMEOUT, REQUEST_TIMEOUT,
};

mod observation;

#[cfg(test)]
pub(in crate::service) async fn stream_generation_observation_for_test(
    transport: loxa_ipc::IpcFramed,
    coordinator: &Coordinator,
    target: loxa_ipc::GenerationTarget,
    attempt_id: String,
) -> Result<(), String> {
    observation::stream(
        transport,
        coordinator,
        "observation-test".into(),
        target,
        attempt_id,
    )
    .await
}

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
    let legacy_control =
        hello.protocol == loxa_ipc::ProtocolVersion::V1_0 && hello.generation.is_none();
    let generation_control = hello.protocol == loxa_ipc::ProtocolVersion::V1_2
        && hello.generation.as_ref().is_some_and(|generation| {
            generation.connection == loxa_ipc::GenerationConnection::Control
        });
    if !legacy_control && !generation_control {
        return Err("overload service lane accepts only bounded Stop control".into());
    }
    let negotiated =
        negotiate_hello(&hello, &bootstrap, &coordinator).map_err(|error| error.context)?;
    send_hello_ack(
        &mut transport,
        &bootstrap,
        &coordinator,
        &negotiated,
        None,
        OVERLOAD_HANDSHAKE_TIMEOUT,
    )
    .await?;

    let envelope: ClientEnvelope = receive_frame(&mut transport, OVERLOAD_REQUEST_TIMEOUT).await?;
    envelope.validate_shape().map_err(str::to_owned)?;
    let ClientEnvelope::Request(request) = envelope else {
        return Err("overload service lane accepts only requests".into());
    };
    let action = match request.command {
        ServiceCommand::StopService if legacy_control => Some(ClassifiedStopAction::Service),
        ServiceCommand::Generation {
            command: loxa_ipc::GenerationCommand::Stop { target },
        } if generation_control => Some(ClassifiedStopAction::Generation(target)),
        _ => None,
    };
    let Some(action) = action else {
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
    };
    let permit = protected_stops
        .try_acquire_owned()
        .map_err(|_| "protected service stop capacity is full".to_string())?;
    Ok(Some(ClassifiedStop {
        transport,
        request_id: request.request_id,
        action,
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
    let (negotiated, mut pending_generation) =
        match negotiate_hello(&hello, &bootstrap, &coordinator) {
            Ok(negotiated) => {
                let pending =
                    if negotiated.generation == Some(loxa_ipc::GenerationConnection::Request) {
                        match coordinator.register_generation_connection() {
                            Ok(pending) => Some(pending),
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
                    } else {
                        None
                    };
                if let Err(error) = send_hello_ack(
                    &mut transport,
                    &bootstrap,
                    &coordinator,
                    &negotiated,
                    pending.as_ref(),
                    HANDSHAKE_TIMEOUT,
                )
                .await
                {
                    if let Some(pending) = pending.as_ref() {
                        coordinator.finish_generation_connection(pending);
                    }
                    return Err(error);
                }
                (negotiated, pending)
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
                execute_request(&coordinator, request, &negotiated, &mut pending_generation).await;
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
        ClientEnvelope::SubscribeGeneration {
            request_id,
            target,
            attempt_id,
        } => {
            let _subscription = subscriptions
                .try_acquire_owned()
                .map_err(|_| "service subscription capacity is full".to_string())?;
            if negotiated.protocol.minor < 5 {
                send_frame_with_limit(
                    &mut transport,
                    &ServerEnvelope::Reply(Reply {
                        request_id,
                        outcome: ReplyOutcome::Rejected(ServiceError::new(
                            ErrorCategory::IncompatibleProtocol,
                            "generation observation requires service protocol 1.5",
                        )),
                    }),
                    REQUEST_TIMEOUT,
                    frame_limit,
                )
                .await?;
            } else if negotiated.generation.is_some() {
                send_frame_with_limit(
                    &mut transport,
                    &ServerEnvelope::Reply(Reply {
                        request_id,
                        outcome: ReplyOutcome::Rejected(ServiceError::new(
                            ErrorCategory::InvalidRequest,
                            "generation observation requires an ordinary history connection",
                        )),
                    }),
                    REQUEST_TIMEOUT,
                    frame_limit,
                )
                .await?;
            } else if !negotiated.capabilities.contains(&Capability::History)
                || negotiated.storage_schema != HISTORY_SCHEMA_VERSION
            {
                send_frame_with_limit(
                    &mut transport,
                    &ServerEnvelope::Reply(Reply {
                        request_id,
                        outcome: ReplyOutcome::Rejected(ServiceError::new(
                            ErrorCategory::ServiceUnavailable,
                            "service generation history is not ready",
                        )),
                    }),
                    REQUEST_TIMEOUT,
                    frame_limit,
                )
                .await?;
            } else {
                observation::stream(transport, &coordinator, request_id, target, attempt_id)
                    .await?;
            }
        }
        ClientEnvelope::Hello(_) => {
            return Err("service received a second hello envelope".into());
        }
    }
    if let Some(pending) = pending_generation.as_ref() {
        coordinator.finish_generation_connection(pending);
    }
    Ok(())
}

async fn send_hello_ack(
    transport: &mut loxa_ipc::IpcFramed,
    bootstrap: &ClientBootstrap,
    coordinator: &Coordinator,
    negotiated: &NegotiatedHello,
    pending: Option<&super::super::coordinator::PendingGenerationConnection>,
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
            generation: pending.map(|pending| loxa_ipc::GenerationHelloAck {
                pending_nonce: pending.nonce().to_owned(),
            }),
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
        generation: hello.generation.as_ref().map(|hello| hello.connection),
    })
}

fn available_capabilities(coordinator: &Coordinator, minor: u16) -> Vec<Capability> {
    let mut capabilities = LEGACY_CAPABILITIES.to_vec();
    if minor >= 5 {
        capabilities.push(Capability::Settings);
    }
    if minor >= 1 && coordinator.history_is_ready() {
        capabilities.push(Capability::History);
        capabilities.push(Capability::Drafts);
    }
    capabilities
}

pub(super) async fn execute_request(
    coordinator: &Coordinator,
    request: Request,
    negotiated: &NegotiatedHello,
    pending_generation: &mut Option<super::super::coordinator::PendingGenerationConnection>,
) -> (Reply, Option<OwnedSemaphorePermit>) {
    let mut history_permit = None;
    let outcome = match request.command {
        ServiceCommand::Status => ReplyOutcome::Status(coordinator.status_report()),
        ServiceCommand::Load { model_id } => match coordinator.load(model_id).await {
            Ok(accepted) => ReplyOutcome::Accepted(accepted),
            Err(error) => ReplyOutcome::Rejected(error),
        },
        ServiceCommand::Reload { .. } if negotiated.protocol.minor < 5 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "Reload requires service protocol 1.5",
            ))
        }
        ServiceCommand::Reload { .. }
            if !negotiated.capabilities.contains(&Capability::Settings) =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service settings are not ready",
            ))
        }
        ServiceCommand::Reload {
            target,
            expected_settings_revision,
        } => match coordinator.reload(&target, &expected_settings_revision) {
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
        ServiceCommand::GetGenerationStatus { .. } if negotiated.protocol.minor < 3 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "generation status requires service protocol 1.3",
            ))
        }
        ServiceCommand::GetGenerationStatus { target } => {
            match coordinator.generation_status(&target) {
                Ok(snapshot) => ReplyOutcome::GenerationStatus { snapshot },
                Err(error) => ReplyOutcome::Rejected(error),
            }
        }
        ServiceCommand::History { command: _command } if negotiated.protocol.minor == 0 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "history requires service protocol 1.1",
            ))
        }
        ServiceCommand::History {
            command: loxa_ipc::HistoryCommand::GetHistoryStatus,
        } if negotiated.protocol.minor < 5 => ReplyOutcome::Rejected(ServiceError::new(
            ErrorCategory::IncompatibleProtocol,
            "history status requires service protocol 1.5",
        )),
        ServiceCommand::History {
            command:
                loxa_ipc::HistoryCommand::ListTurns { .. } | loxa_ipc::HistoryCommand::GetAttempt { .. },
        } if negotiated.protocol.minor < 5 => ReplyOutcome::Rejected(ServiceError::new(
            ErrorCategory::IncompatibleProtocol,
            "attempt metadata requires service protocol 1.5",
        )),
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
        ServiceCommand::Settings { command: _ } if negotiated.protocol.minor < 5 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "sampling settings require service protocol 1.5",
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
        ServiceCommand::Generation { command: _ } if negotiated.protocol.minor < 2 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "generation requires service protocol 1.2",
            ))
        }
        ServiceCommand::Generation {
            command: loxa_ipc::GenerationCommand::Retry { .. },
        } if negotiated.protocol.minor < 5 => ReplyOutcome::Rejected(ServiceError::new(
            ErrorCategory::IncompatibleProtocol,
            "generation Retry requires service protocol 1.5",
        )),
        ServiceCommand::Generation {
            command:
                loxa_ipc::GenerationCommand::Send { .. } | loxa_ipc::GenerationCommand::Retry { .. },
        } if negotiated.generation != Some(loxa_ipc::GenerationConnection::Request) => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "generation request requires a prepared generation connection",
            ))
        }
        ServiceCommand::Generation {
            command: loxa_ipc::GenerationCommand::Stop { .. },
        } if negotiated.generation != Some(loxa_ipc::GenerationConnection::Control) => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "generation Stop requires a control connection",
            ))
        }
        ServiceCommand::Generation { command } => {
            let result = match command {
                command @ (loxa_ipc::GenerationCommand::Send { .. }
                | loxa_ipc::GenerationCommand::Retry { .. }) => match pending_generation.take() {
                    Some(pending) => coordinator.generation_request(command, pending).await,
                    None => Err(ServiceError::new(
                        ErrorCategory::Conflict,
                        "pending generation connection is no longer current",
                    )),
                },
                loxa_ipc::GenerationCommand::Stop { target } => coordinator
                    .stop_generation(&target)
                    .map(|()| loxa_ipc::GenerationReply::Stopping { target }),
            };
            match result {
                Ok(reply) => ReplyOutcome::Generation { reply },
                Err(error) => ReplyOutcome::Rejected(error),
            }
        }
        ServiceCommand::GenerationAt { .. } if negotiated.protocol.minor < 6 => {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::IncompatibleProtocol,
                "runtime-bound generation requires service protocol 1.6",
            ))
        }
        ServiceCommand::GenerationAt { .. }
            if negotiated.generation != Some(loxa_ipc::GenerationConnection::Request) =>
        {
            ReplyOutcome::Rejected(ServiceError::new(
                ErrorCategory::InvalidRequest,
                "runtime-bound generation requires a prepared request connection",
            ))
        }
        ServiceCommand::GenerationAt { target, command } => {
            let result = match pending_generation.take() {
                Some(pending) => {
                    coordinator
                        .generation_request_at(command, pending, target)
                        .await
                }
                None => Err(ServiceError::new(
                    ErrorCategory::Conflict,
                    "pending generation connection is no longer current",
                )),
            };
            match result {
                Ok(reply) => ReplyOutcome::Generation { reply },
                Err(error) => ReplyOutcome::Rejected(error),
            }
        }
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
