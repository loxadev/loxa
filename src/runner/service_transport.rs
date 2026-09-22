use super::owned::readiness::{models_body_has_alias, MAX_MODELS_BODY};
use super::owned::StartupStop;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

const UNIX_READINESS_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_READINESS_HEADERS: usize = 64;
const MAX_READINESS_HTTP_BUFFER: usize = 64 * 1024;
const MAX_PROPERTIES_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
struct Properties {
    default_generation_settings: DefaultGenerationSettings,
    total_slots: u32,
    model_alias: String,
    endpoint_slots: bool,
}

#[derive(Deserialize)]
struct DefaultGenerationSettings {
    n_ctx: u32,
}

pub(crate) fn observe_context(
    runtime: &tokio::runtime::Handle,
    endpoint: &Path,
    pid: u32,
    model_id: &str,
) -> Option<u32> {
    let body = runtime
        .block_on(get_authenticated_bounded(
            endpoint,
            pid,
            "/props",
            MAX_PROPERTIES_BYTES,
        ))
        .ok()??;
    parse_context(&body, model_id)
}

fn parse_context(body: &[u8], model_id: &str) -> Option<u32> {
    let properties: Properties = serde_json::from_slice(body).ok()?;
    (properties.model_alias == model_id && properties.total_slots == 1 && properties.endpoint_slots)
        .then_some(properties.default_generation_settings.n_ctx)
}

async fn get_authenticated_bounded(
    path: &Path,
    expected_pid: u32,
    uri: &'static str,
    max_body: usize,
) -> Result<Option<Vec<u8>>, String> {
    tokio::time::timeout(UNIX_READINESS_ATTEMPT_TIMEOUT, async {
        let mut authenticated = None;
        let Some(stream) = connect_authenticated(path, expected_pid, &mut authenticated).await?
        else {
            return Ok(None);
        };
        let io = TokioIo::new(stream);
        let mut builder = hyper::client::conn::http1::Builder::new();
        builder
            .max_headers(MAX_READINESS_HEADERS)
            .max_buf_size(MAX_READINESS_HTTP_BUFFER);
        let (mut sender, connection) = builder
            .handshake(io)
            .await
            .map_err(|error| error.to_string())?;
        let request = Request::builder()
            .method(hyper::Method::GET)
            .uri(uri)
            .header(hyper::header::HOST, "localhost")
            .header(hyper::header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .map_err(|error| error.to_string())?;
        let exchange = async move {
            let mut response = sender
                .send_request(request)
                .await
                .map_err(|error| error.to_string())?;
            if !response.status().is_success() {
                return Err(format!("{uri} returned HTTP {}", response.status()));
            }
            if response
                .headers()
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > max_body)
            {
                return Err(format!("{uri} response exceeds {max_body} bytes"));
            }
            let mut body = Vec::with_capacity(max_body);
            while let Some(frame) = response.body_mut().frame().await {
                let frame = frame.map_err(|error| error.to_string())?;
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                if body.len().saturating_add(data.len()) > max_body {
                    return Err(format!("{uri} response exceeds {max_body} bytes"));
                }
                body.extend_from_slice(&data);
            }
            Ok(body)
        };
        let (driver, response) = tokio::join!(connection.without_shutdown(), exchange);
        driver.map_err(|error| error.to_string())?;
        response.map(Some)
    })
    .await
    .map_err(|_| format!("{uri} request timed out"))?
}

pub(super) fn require_absent_unix_endpoint(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(format!(
            "service engine endpoint already exists: {}",
            path.display()
        )),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UnixEndpointIdentity {
    device: u64,
    inode: u64,
}

pub(super) enum UnixReadiness {
    Pending,
    Ready,
    Stopped(StartupStop),
}

pub(super) struct UnixReadinessPoll {
    pub(super) endpoint_identity: Option<UnixEndpointIdentity>,
    pub(super) outcome: Result<UnixReadiness, String>,
}

fn private_unix_endpoint(path: &Path) -> Result<Option<UnixEndpointIdentity>, String> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

    let metadata = match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(metadata) => metadata,
        Err(error) => return Err(format!("{}: {error}", path.display())),
    };
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("service engine endpoint is not a private user-owned socket".into());
    }
    Ok(Some(UnixEndpointIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }))
}

async fn wait_for_startup_stop<F>(stop: &F) -> StartupStop
where
    F: Fn() -> Option<StartupStop>,
{
    loop {
        if let Some(stop) = stop() {
            return stop;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn readiness_unix_attempt(
    path: &Path,
    id: &str,
    expected_pid: u32,
    authenticated: &mut Option<UnixEndpointIdentity>,
) -> Result<bool, String> {
    let Some(stream) = connect_authenticated(path, expected_pid, authenticated).await? else {
        return Ok(false);
    };

    let io = TokioIo::new(stream);
    let mut builder = hyper::client::conn::http1::Builder::new();
    builder
        .max_headers(MAX_READINESS_HEADERS)
        .max_buf_size(MAX_READINESS_HTTP_BUFFER);
    let (mut sender, connection) = match builder.handshake(io).await {
        Ok(connection) => connection,
        Err(_) => return Ok(false),
    };
    let request = Request::builder()
        .method(hyper::Method::GET)
        .uri("/v1/models")
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONNECTION, "close")
        .body(Empty::<Bytes>::new())
        .map_err(|error| error.to_string())?;
    let request = async move {
        let mut response = match sender.send_request(request).await {
            Ok(response) => response,
            Err(_) => return Ok(false),
        };
        if !response.status().is_success() {
            return Ok(false);
        }
        if response
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_MODELS_BODY)
        {
            return Err(format!(
                "/v1/models response is too large (limit {MAX_MODELS_BODY} bytes)"
            ));
        }
        let mut body = Vec::new();
        while let Some(frame) = response.body_mut().frame().await {
            let frame = frame.map_err(|error| error.to_string())?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            if body.len().saturating_add(data.len()) > MAX_MODELS_BODY {
                return Err(format!(
                    "/v1/models response is too large (limit {MAX_MODELS_BODY} bytes)"
                ));
            }
            body.extend_from_slice(&data);
        }
        let body = std::str::from_utf8(&body).map_err(|error| error.to_string())?;
        Ok(models_body_has_alias(body, id))
    };

    // Poll the maintained HTTP driver and request together. Dropping this joined
    // future on the outer deadline cancels connect, headers, body and driver as
    // one bounded operation; no detached task can keep service shutdown waiting.
    let (driver, response) = tokio::join!(connection, request);
    if driver.is_err() && response.as_ref().is_ok_and(|ready| *ready) {
        return Ok(false);
    }
    response
}

/// Connect only after proving the pathname and kernel peer are the exact
/// private engine selected by the service status projection.  Callers must
/// still re-read that projection after this returns, before writing HTTP.
pub(crate) async fn connect_authenticated(
    path: &Path,
    expected_pid: u32,
    authenticated: &mut Option<UnixEndpointIdentity>,
) -> Result<Option<tokio::net::UnixStream>, String> {
    let Some(before) = private_unix_endpoint(path)? else {
        return Ok(None);
    };
    let stream = match tokio::net::UnixStream::connect(path).await {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            return Ok(None)
        }
        Err(error) => return Err(error.to_string()),
    };

    // Authenticate the connected kernel peer and revalidate the pathname before
    // Hyper is allowed to write the first HTTP byte.
    let peer = loxa_ipc::peer_credentials(&stream)?;
    if peer.uid != unsafe { libc::geteuid() } || peer.pid != expected_pid {
        return Err("service engine endpoint peer is not the exact owned child".into());
    }
    let after = private_unix_endpoint(path)?
        .ok_or_else(|| "service engine endpoint disappeared during authentication".to_string())?;
    if after != before {
        return Err("service engine endpoint changed during authentication".into());
    }
    *authenticated = Some(after);
    Ok(Some(stream))
}

pub(super) fn readiness_unix<F>(
    runtime: &tokio::runtime::Handle,
    path: &Path,
    id: &str,
    expected_pid: u32,
    stop: &F,
) -> UnixReadinessPoll
where
    F: Fn() -> Option<StartupStop>,
{
    let mut authenticated = None;
    let outcome = runtime.block_on(async {
        tokio::select! {
            biased;
            stop = wait_for_startup_stop(stop) => Ok(UnixReadiness::Stopped(stop)),
            result = tokio::time::timeout(
                UNIX_READINESS_ATTEMPT_TIMEOUT,
                readiness_unix_attempt(path, id, expected_pid, &mut authenticated),
            ) => match result {
                Ok(Ok(true)) => Ok(UnixReadiness::Ready),
                Ok(Ok(false)) | Err(_) => Ok(UnixReadiness::Pending),
                Ok(Err(error)) => Err(error),
            },
        }
    });
    UnixReadinessPoll {
        endpoint_identity: authenticated,
        outcome,
    }
}

pub(super) fn authenticate_unix_endpoint(
    runtime: &tokio::runtime::Handle,
    path: &Path,
    expected_pid: u32,
) -> Result<Option<UnixEndpointIdentity>, String> {
    let mut authenticated = None;
    let result = runtime.block_on(async {
        tokio::time::timeout(
            UNIX_READINESS_ATTEMPT_TIMEOUT,
            connect_authenticated(path, expected_pid, &mut authenticated),
        )
        .await
    });
    match result {
        Ok(Ok(_)) | Err(_) => Ok(authenticated),
        Ok(Err(error)) => Err(error),
    }
}

pub(super) fn remove_owned_unix_endpoint(
    path: &Path,
    expected: Option<UnixEndpointIdentity>,
) -> Result<(), String> {
    let current = private_unix_endpoint(path)?;
    match (current, expected) {
        (None, _) => Ok(()),
        (Some(current), Some(expected)) if current == expected => {
            std::fs::remove_file(path).map_err(|error| format!("{}: {error}", path.display()))
        }
        (Some(_), Some(_)) => {
            Err("refusing to remove a substituted service engine endpoint".into())
        }
        (Some(_), None) => {
            Err("refusing to remove a service engine endpoint that was never authenticated".into())
        }
    }
}

#[cfg(test)]
mod properties_tests {
    use super::parse_context;

    #[test]
    fn observation_requires_the_exact_model_and_single_slot_shape() {
        let valid = br#"{
            "default_generation_settings":{"n_ctx":8192,"params":{"temperature":0.8}},
            "total_slots":1,
            "model_alias":"demo",
            "endpoint_slots":true,
            "chat_template":"{{ messages }}"
        }"#;
        assert_eq!(parse_context(valid, "demo"), Some(8192));
        assert_eq!(parse_context(valid, "other"), None);

        for invalid in [
            br#"{"default_generation_settings":{"n_ctx":8192},"total_slots":2,"model_alias":"demo","endpoint_slots":true}"#.as_slice(),
            br#"{"default_generation_settings":{"n_ctx":8192},"total_slots":1,"model_alias":"demo","endpoint_slots":false}"#.as_slice(),
            br#"{"default_generation_settings":{"n_ctx":"large"},"total_slots":1,"model_alias":"demo","endpoint_slots":true}"#.as_slice(),
        ] {
            assert_eq!(parse_context(invalid, "demo"), None);
        }
    }
}
