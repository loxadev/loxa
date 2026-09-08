//! Read-only attachment to the exact engine operation selected by service status.

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use loxa_ipc::{ConnectMode, OperationTarget, ReplyOutcome, RuntimePhase, ServiceClient};
use std::cell::Cell;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) async fn validate_target(
    client: &ServiceClient,
    model: &str,
    target: &OperationTarget,
) -> Result<(), String> {
    observe_ready(client, Some(model), Some(target))
        .await
        .map(|_| ())
}

pub(crate) async fn chat(
    client: &ServiceClient,
    target: &OperationTarget,
    model: &str,
    body: Vec<u8>,
    mut consume: impl FnMut(&[u8]) -> Result<bool, String>,
) -> Result<(), String> {
    transfer(
        client,
        Some(model),
        Some(target),
        Method::POST,
        "/v1/chat/completions",
        body,
        |status, data| {
            if !status.is_success() {
                return Err(format!("service engine returned HTTP {status}"));
            }
            consume(data)
        },
    )
    .await
}

pub(super) async fn api(
    client: &ServiceClient,
    model_id: Option<&str>,
    expected: Option<&OperationTarget>,
    method: Method,
    path: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    transfer(client, model_id, expected, method, path, body, |_, data| {
        let mut output = std::io::stdout().lock();
        output
            .write_all(data)
            .and_then(|()| output.flush())
            .map(|()| false)
            .map_err(|error| error.to_string())
    })
    .await
}

async fn transfer(
    client: &ServiceClient,
    model_id: Option<&str>,
    expected: Option<&OperationTarget>,
    method: Method,
    path: &str,
    body: Vec<u8>,
    consume: impl FnMut(hyper::StatusCode, &[u8]) -> Result<bool, String>,
) -> Result<(), String> {
    let attached = observe_ready(client, model_id, expected).await?;
    let authenticated = Cell::new(None);
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        crate::runner::service_transport::connect_authenticated(
            &attached.socket,
            attached.engine_pid,
            &authenticated,
        ),
    )
    .await
    .map_err(|_| "service engine connection timed out".to_string())??
    .ok_or_else(|| "the selected service engine is no longer available".to_string())?;

    // A copied command must not silently follow an unload/reload while it was
    // connecting.  This check happens before Hyper can write request bytes.
    let current = observe_ready(client, model_id, Some(&attached.target)).await?;
    if current.engine_pid != attached.engine_pid || current.socket != attached.socket {
        return Err(
            "the selected service engine changed while connecting; copy a new command from Loxa"
                .into(),
        );
    }

    let request = transfer_http(stream, method, path, body, consume);
    let monitor = async {
        loop {
            tokio::time::sleep(TARGET_POLL_INTERVAL).await;
            let current = observe_ready(client, model_id, Some(&attached.target)).await?;
            if current.engine_pid != attached.engine_pid || current.socket != attached.socket {
                return Err(
                    "the selected service engine changed; copy a new command from Loxa".into(),
                );
            }
        }
    };
    // Drop the losing branch and its attachment. Never unload or follow another engine.
    tokio::time::timeout(HTTP_TIMEOUT, async {
        tokio::select! {
            result = request => result,
            result = monitor => result,
        }
    })
    .await
    .map_err(|_| "service API request timed out after 120000 ms".to_string())?
}

async fn transfer_http(
    stream: tokio::net::UnixStream,
    method: Method,
    path: &str,
    body: Vec<u8>,
    mut consume: impl FnMut(hyper::StatusCode, &[u8]) -> Result<bool, String>,
) -> Result<(), String> {
    let io = TokioIo::new(stream);
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_headers(64)
        .max_buf_size(64 * 1024)
        .handshake(io)
        .await
        .map_err(|error| format!("service engine HTTP handshake failed: {error}"))?;
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONNECTION, "close")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| error.to_string())?;
    let transfer = async move {
        let mut response = sender
            .send_request(request)
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let mut seen = 0usize;
        while let Some(frame) = response.body_mut().frame().await {
            let data = frame.map_err(|error| error.to_string())?.into_data().ok();
            let Some(data) = data else { continue };
            seen = seen.saturating_add(data.len());
            if seen > MAX_RESPONSE_BYTES {
                return Err(format!(
                    "service API response exceeds {MAX_RESPONSE_BYTES} bytes"
                ));
            }
            if consume(status, &data)? {
                return Ok(());
            }
        }
        if !status.is_success() {
            return Err(format!("service engine returned HTTP {status}"));
        }
        Ok(())
    };
    let request = async {
        tokio::pin!(transfer);
        tokio::select! {
            result = &mut transfer => result,
            result = connection => {
                result.map_err(|error| format!("service engine HTTP connection failed: {error}"))?;
                transfer.await
            }
        }
    };
    request.await
}

struct AttachedEngine {
    target: OperationTarget,
    engine_pid: u32,
    socket: PathBuf,
}

async fn observe_ready(
    client: &ServiceClient,
    model_id: Option<&str>,
    expected: Option<&OperationTarget>,
) -> Result<AttachedEngine, String> {
    let ReplyOutcome::Status(status) = client
        .request(
            ConnectMode::ObserveExisting,
            loxa_ipc::ServiceCommand::Status,
        )
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("service returned an invalid status outcome".into());
    };
    let RuntimePhase::Ready {
        task_id,
        generation,
        model_id: active,
        engine_pid,
    } = status.runtime.phase
    else {
        return Err("the selected service model is not ready".into());
    };
    if model_id.is_some_and(|expected_model| expected_model != active) {
        return Err("the selected service model is no longer loaded".into());
    }
    let target = OperationTarget {
        boot_epoch: status.runtime.boot_epoch,
        task_id,
        generation,
    };
    if expected.is_some_and(|value| value != &target) {
        return Err("the copied service target is stale; copy a new command from Loxa".into());
    }
    let generation = target
        .generation
        .parse::<u64>()
        .map_err(|_| "invalid service generation".to_string())?;
    Ok(AttachedEngine {
        socket: client
            .bootstrap()
            .root()
            .control_dir()
            .join(format!("engine-{generation:016x}.sock")),
        target,
        engine_pid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn completed_sse_drops_an_open_http_response() {
        let (client, mut engine) = tokio::net::UnixStream::pair().unwrap();
        let server = async {
            let mut request = [0; 2048];
            assert!(engine.read(&mut request).await.unwrap() > 0);
            engine.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
            // Deliberately omit the terminating HTTP chunk. SSE completion is
            // sufficient to close only this client attachment.
            let data = b"data: [DONE]\n\n";
            engine
                .write_all(format!("{:x}\r\n", data.len()).as_bytes())
                .await
                .unwrap();
            engine.write_all(data).await.unwrap();
            engine.write_all(b"\r\n").await.unwrap();
            assert_eq!(engine.read(&mut request).await.unwrap(), 0);
        };
        let request = transfer_http(client, Method::GET, "/test", Vec::new(), |_, data| {
            assert_eq!(data, b"data: [DONE]\n\n");
            Ok(true)
        });
        let ((), result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(server, request)
        })
        .await
        .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn api_error_body_is_delivered_before_failure() {
        let (client, mut engine) = tokio::net::UnixStream::pair().unwrap();
        let server = async {
            let mut request = [0; 2048];
            assert!(engine.read(&mut request).await.unwrap() > 0);
            engine
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .unwrap();
        };
        let mut received = Vec::new();
        let request = transfer_http(client, Method::GET, "/test", Vec::new(), |status, data| {
            assert_eq!(status, hyper::StatusCode::BAD_REQUEST);
            received.extend_from_slice(data);
            Ok(false)
        });
        let ((), result) = tokio::join!(server, request);
        assert!(result.unwrap_err().contains("400 Bad Request"));
        assert_eq!(received, b"{}");
    }
}
