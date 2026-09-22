use super::{send_frame, Coordinator};
use crate::service::server::REQUEST_TIMEOUT;
use futures_util::StreamExt;
use loxa_ipc::{
    GenerationExecutionPhase, GenerationObservation, GenerationSavePhase, Reply, ReplyOutcome,
    ServerEnvelope, ServiceError,
};

pub(super) async fn stream(
    transport: loxa_ipc::IpcFramed,
    coordinator: &Coordinator,
    request_id: String,
    target: loxa_ipc::GenerationTarget,
    attempt_id: String,
) -> Result<(), String> {
    let statuses = match coordinator.subscribe_generation_status(&target, &attempt_id) {
        Ok(statuses) => statuses,
        Err(error) => {
            let mut transport = transport;
            return send_rejection(&mut transport, request_id, error).await;
        }
    };
    let (mut writer, mut reader) = transport.split();
    let mut stop = coordinator.server_stop_receiver();
    if let Some(mut statuses) = statuses {
        loop {
            let status = { statuses.borrow_and_update().clone() };
            if let Some(status) = status {
                let terminal = is_durably_terminal(&status);
                send_observation(&mut writer, GenerationObservation::Live { status }).await?;
                if terminal {
                    break;
                }
            }
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
                changed = statuses.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                incoming = reader.next() => {
                    return match incoming {
                        None => Ok(()),
                        Some(Ok(_)) => Err("generation subscription received an unexpected client frame".into()),
                        Some(Err(_)) => Err("generation subscription transport failed".into()),
                    };
                }
            }
        }
    }

    let (result, _history_permit) = coordinator.observed_attempt(&target, &attempt_id).await;
    match result {
        Ok(attempt) => {
            send_observation(&mut writer, GenerationObservation::Durable { attempt }).await
        }
        Err(error) => send_rejection(&mut writer, request_id, error).await,
    }
}

fn is_durably_terminal(status: &loxa_ipc::GenerationStatus) -> bool {
    matches!(
        status.execution,
        GenerationExecutionPhase::Completed
            | GenerationExecutionPhase::Stopped
            | GenerationExecutionPhase::Failed
    ) && status.save == GenerationSavePhase::Saved
}

async fn send_observation<S>(
    transport: &mut S,
    observation: GenerationObservation,
) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    send_frame(
        transport,
        &ServerEnvelope::GenerationSnapshot(observation),
        REQUEST_TIMEOUT,
    )
    .await
}

async fn send_rejection<S>(
    transport: &mut S,
    request_id: String,
    error: ServiceError,
) -> Result<(), String>
where
    S: futures_util::Sink<bytes::Bytes> + Unpin,
    S::Error: std::fmt::Display,
{
    send_frame(
        transport,
        &ServerEnvelope::Reply(Reply {
            request_id,
            outcome: ReplyOutcome::Rejected(error),
        }),
        REQUEST_TIMEOUT,
    )
    .await
}
