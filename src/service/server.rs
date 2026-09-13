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
mod tests;
