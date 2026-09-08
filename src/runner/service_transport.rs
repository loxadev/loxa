use super::owned::readiness::{models_body_has_alias, MAX_MODELS_BODY};
use super::owned::StartupStop;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::cell::Cell;
use std::path::Path;
use std::time::Duration;

const UNIX_READINESS_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_READINESS_HEADERS: usize = 64;
const MAX_READINESS_HTTP_BUFFER: usize = 64 * 1024;

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
    authenticated: &Cell<Option<UnixEndpointIdentity>>,
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
    authenticated: &Cell<Option<UnixEndpointIdentity>>,
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
    authenticated.set(Some(after));
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
    let authenticated = Cell::new(None);
    let outcome = runtime.block_on(async {
        tokio::select! {
            biased;
            stop = wait_for_startup_stop(stop) => Ok(UnixReadiness::Stopped(stop)),
            result = tokio::time::timeout(
                UNIX_READINESS_ATTEMPT_TIMEOUT,
                readiness_unix_attempt(path, id, expected_pid, &authenticated),
            ) => match result {
                Ok(Ok(true)) => Ok(UnixReadiness::Ready),
                Ok(Ok(false)) | Err(_) => Ok(UnixReadiness::Pending),
                Ok(Err(error)) => Err(error),
            },
        }
    });
    UnixReadinessPoll {
        endpoint_identity: authenticated.get(),
        outcome,
    }
}

pub(super) fn authenticate_unix_endpoint(
    runtime: &tokio::runtime::Handle,
    path: &Path,
    expected_pid: u32,
) -> Result<Option<UnixEndpointIdentity>, String> {
    let authenticated = Cell::new(None);
    let result = runtime.block_on(async {
        tokio::time::timeout(
            UNIX_READINESS_ATTEMPT_TIMEOUT,
            connect_authenticated(path, expected_pid, &authenticated),
        )
        .await
    });
    match result {
        Ok(Ok(_)) | Err(_) => Ok(authenticated.get()),
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
