use crate::control::auth::ControlToken;
use crate::control::contracts::{
    ControlErrorBody, ModelRequest, NodeIdentityProofResponse, OperationAccepted, OperationStatus,
    OperationView, CONTROL_PROTOCOL_VERSION,
};
use crate::model_inventory::VerifiedRecipeInventoryEntry;
use std::fmt;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

const MAX_PROOF_BYTES: usize = 16 * 1024;
const MAX_CONTROL_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    Transport(String),
    PeerProof,
    Rejected(String),
    OperationFailed(String),
    OperationCancelled,
    OperationTimeout,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => write!(f, "control transport failed: {message}"),
            Self::PeerProof => {
                f.write_str("managed node peer proof failed; refusing to send credentials")
            }
            Self::Rejected(message) => write!(f, "control request rejected: {message}"),
            Self::OperationFailed(message) => write!(f, "operation failed: {message}"),
            Self::OperationCancelled => f.write_str("operation cancelled"),
            Self::OperationTimeout => f.write_str("operation did not finish before the deadline"),
        }
    }
}

impl std::error::Error for ClientError {}

pub struct LiveControlClient {
    address: SocketAddr,
    runtime_identity: String,
    token: ControlToken,
    timeout: Duration,
}

impl fmt::Debug for LiveControlClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveControlClient")
            .field("address", &self.address)
            .field("runtime_identity", &self.runtime_identity)
            .field("token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl LiveControlClient {
    pub fn connect(
        address: SocketAddr,
        token: ControlToken,
        expected_runtime_identity: &str,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(ClientError::PeerProof);
        }
        drop(open_and_prove(
            address,
            &token,
            expected_runtime_identity,
            timeout,
        )?);
        Ok(Self {
            address,
            runtime_identity: expected_runtime_identity.to_owned(),
            token,
            timeout,
        })
    }

    pub fn download(&self, model_id: &str) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/download", Some(model_id))
    }

    pub fn load(&self, model_id: &str) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/load", Some(model_id))
    }

    pub fn unload(&self) -> Result<String, ClientError> {
        self.start("/loxa/v1/models/unload", None)
    }

    fn start(&self, path: &str, model_id: Option<&str>) -> Result<String, ClientError> {
        let body = match model_id {
            Some(model_id) => serde_json::to_vec(
                &ModelRequest::known(model_id)
                    .map_err(|_| ClientError::Rejected("unknown model id".into()))?,
            )
            .map_err(|error| ClientError::Transport(error.to_string()))?,
            None => b"{}".to_vec(),
        };
        let response = self.authenticated("POST", path, &body)?;
        serde_json::from_slice::<OperationAccepted>(&response)
            .map(|accepted| accepted.operation_id)
            .map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn operation(&self, operation_id: &str) -> Result<OperationView, ClientError> {
        if operation_id.is_empty() || operation_id.contains(['/', '?', '#']) {
            return Err(ClientError::Rejected("invalid operation id".into()));
        }
        let response =
            self.authenticated("GET", &format!("/loxa/v1/operations/{operation_id}"), &[])?;
        serde_json::from_slice(&response).map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn models(&self) -> Result<Vec<VerifiedRecipeInventoryEntry>, ClientError> {
        let response = self.authenticated("GET", "/loxa/v1/models", &[])?;
        serde_json::from_slice(&response).map_err(|error| ClientError::Transport(error.to_string()))
    }

    pub fn wait_terminal(&self, operation_id: &str, timeout: Duration) -> Result<(), ClientError> {
        let deadline = Instant::now() + timeout;
        loop {
            let operation = self.operation(operation_id)?;
            match operation.status {
                OperationStatus::Succeeded => return Ok(()),
                OperationStatus::Failed => {
                    return Err(ClientError::OperationFailed(
                        operation.error.unwrap_or_else(|| "unknown failure".into()),
                    ));
                }
                OperationStatus::Cancelled => return Err(ClientError::OperationCancelled),
                OperationStatus::Queued | OperationStatus::Running => {}
            }
            if Instant::now() >= deadline {
                return Err(ClientError::OperationTimeout);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn authenticated(&self, method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, ClientError> {
        let mut stream = open_and_prove(
            self.address,
            &self.token,
            &self.runtime_identity,
            self.timeout,
        )?;
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.address.ip(),
            self.address.port(),
            self.token.expose_for_authorization(),
            body.len()
        )
        .and_then(|_| stream.write_all(body))
        .map_err(|error| ClientError::Transport(error.to_string()))?;
        let response = read_response(&mut stream, MAX_CONTROL_BYTES)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if !(200..300).contains(&response.status) {
            let message = serde_json::from_slice::<ControlErrorBody>(&response.body)
                .map(|error| sanitize_error(&error.message))
                .unwrap_or_else(|_| format!("HTTP {}", response.status));
            return Err(ClientError::Rejected(message));
        }
        Ok(response.body)
    }
}

fn open_and_prove(
    address: SocketAddr,
    token: &ControlToken,
    expected_runtime_identity: &str,
    timeout: Duration,
) -> Result<TcpStream, ClientError> {
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| ClientError::PeerProof)?;
    let nonce = encode_hex(&nonce);
    let timeout = timeout.max(Duration::from_millis(1));
    let deadline = Instant::now() + timeout;
    let mut stream =
        TcpStream::connect_timeout(&address, timeout).map_err(|_| ClientError::PeerProof)?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|_| ClientError::PeerProof)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|_| ClientError::PeerProof)?;
    let host = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    write!(stream, "GET /loxa/v1/node HTTP/1.1\r\nHost: {host}:{}\r\nX-Loxa-Challenge: {nonce}\r\nConnection: keep-alive\r\n\r\n", address.port()).map_err(|_| ClientError::PeerProof)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ClientError::PeerProof)?;
    stream
        .set_read_timeout(Some(remaining.max(Duration::from_millis(1))))
        .map_err(|_| ClientError::PeerProof)?;
    let response =
        read_response(&mut stream, MAX_PROOF_BYTES).map_err(|_| ClientError::PeerProof)?;
    if response.status != 200 {
        return Err(ClientError::PeerProof);
    }
    let value: NodeIdentityProofResponse =
        serde_json::from_slice(&response.body).map_err(|_| ClientError::PeerProof)?;
    if value.protocol_version != CONTROL_PROTOCOL_VERSION
        || value.runtime_identity != expected_runtime_identity
        || matches!(
            value.status,
            crate::control::contracts::NodeStatus::RecoveryRequired
                | crate::control::contracts::NodeStatus::Error
        )
        || !token.verify_node_identity_proof(
            &nonce,
            &value.node_id,
            &value.runtime_identity,
            value.status,
            &value.challenge_proof,
        )
    {
        return Err(ClientError::PeerProof);
    }
    Ok(stream)
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn read_response(stream: &mut TcpStream, max_body: usize) -> std::io::Result<HttpResponse> {
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        if headers.len() >= 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response headers too large",
            ));
        }
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        headers.push(byte[0]);
    }
    let text = std::str::from_utf8(&headers).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid response headers")
    })?;
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HTTP status")
        })?;
    let mut length = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid content length")
                })?);
            }
            if name.eq_ignore_ascii_case("transfer-encoding") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "transfer encoding is unsupported",
                ));
            }
        }
    }
    let length = length.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing content length")
    })?;
    if length > max_body {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response body too large",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(HttpResponse { status, body })
}

fn sanitize_error(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::auth::ControlToken;
    use crate::control::contracts::{NodeStatus, OperationStatus};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    fn token() -> (tempfile::TempDir, ControlToken) {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let token = ControlToken::load_or_create(&dir.path().join("control.token")).unwrap();
        (dir, token)
    }

    fn serve(
        token: ControlToken,
        responses: Vec<String>,
    ) -> (
        std::net::SocketAddr,
        Arc<Mutex<Vec<String>>>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let worker = std::thread::spawn(move || {
            for body in responses {
                let (mut socket, _) = listener.accept().unwrap();
                let request = read_request(&mut socket);
                captured.lock().unwrap().push(request.clone());
                if body.contains("\"challenge_proof\"") {
                    send_response(&mut socket, 200, &body, "close");
                    continue;
                }
                let proof_body = proof_body(&token, &request);
                if body == "__PROOF__" {
                    send_response(&mut socket, 200, &proof_body, "close");
                    continue;
                }
                send_response(&mut socket, 200, &proof_body, "keep-alive");
                let authenticated = read_request(&mut socket);
                captured.lock().unwrap().push(authenticated);
                let (status, body) = body
                    .split_once(':')
                    .and_then(|(status, body)| {
                        status.parse::<u16>().ok().map(|status| (status, body))
                    })
                    .unwrap_or((200, body.as_str()));
                send_response(&mut socket, status, body, "close");
            }
        });
        (address, seen, worker)
    }

    fn read_request(socket: &mut std::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut chunk = [0; 1];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut chunk).unwrap();
            request.push(chunk[0]);
        }
        let headers = String::from_utf8(request.clone()).unwrap();
        let content_length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        socket.read_exact(&mut body).unwrap();
        request.extend_from_slice(&body);
        String::from_utf8(request).unwrap()
    }

    fn proof_body(token: &ControlToken, request: &str) -> String {
        let nonce = request
            .lines()
            .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
            .unwrap();
        let proof = token
            .node_identity_proof(nonce, "node", "runtime", NodeStatus::Unloaded)
            .unwrap();
        format!(
            r#"{{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"{proof}"}}"#
        )
    }

    fn send_response(socket: &mut std::net::TcpStream, status: u16, body: &str, connection: &str) {
        write!(
            socket,
            "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    }

    #[test]
    fn bearer_is_sent_only_after_nonce_bound_peer_proof() {
        let (_dir, token) = token();
        let accepted = r#"{"operation_id":"op-1"}"#.to_string();
        let (address, seen, worker) = serve(token.clone(), vec!["__PROOF__".into(), accepted]);
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(client.load("gemma-3-4b-it-q4").unwrap(), "op-1");
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
        assert!(!requests[1].to_ascii_lowercase().contains("authorization:"));
        assert!(requests[2]
            .to_ascii_lowercase()
            .contains("authorization: bearer "));
        assert!(!requests[2].contains("X-Loxa-Challenge"));
    }

    #[test]
    fn wrong_peer_proof_fails_before_any_authenticated_request() {
        let (_dir, token) = token();
        let bad = r#"{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"0000000000000000000000000000000000000000000000000000000000000000"}"#.to_string();
        let (address, seen, worker) = serve(token.clone(), vec![bad]);
        assert!(matches!(
            LiveControlClient::connect(
                address,
                token,
                "runtime",
                std::time::Duration::from_secs(1)
            ),
            Err(ClientError::PeerProof)
        ));
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
    }

    #[test]
    fn valid_proof_from_a_replacement_runtime_is_rejected() {
        let (_dir, token) = token();
        let (address, seen, worker) = serve(token.clone(), vec!["__PROOF__".into()]);
        assert!(matches!(
            LiveControlClient::connect(
                address,
                token,
                "different-runtime",
                std::time::Duration::from_secs(1)
            ),
            Err(ClientError::PeerProof)
        ));
        worker.join().unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn polling_reports_each_terminal_operation_state() {
        for (status, expected) in [
            ("succeeded", Ok(())),
            ("failed", Err(ClientError::OperationFailed("boom".into()))),
            ("cancelled", Err(ClientError::OperationCancelled)),
        ] {
            let (_dir, token) = token();
            let operation = format!(
                r#"{{"id":"op","kind":"load","status":"{status}","model_id":"gemma-3-4b-it-q4","progress":null,"error":{},"created_at_unix_ms":1,"updated_at_unix_ms":2}}"#,
                if status == "failed" {
                    "\"boom\""
                } else {
                    "null"
                }
            );
            let (address, _, worker) = serve(token.clone(), vec!["__PROOF__".into(), operation]);
            let client = LiveControlClient::connect(
                address,
                token,
                "runtime",
                std::time::Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(
                client.wait_terminal("op", std::time::Duration::ZERO),
                expected
            );
            worker.join().unwrap();
        }
        let _ = OperationStatus::Succeeded;
    }

    #[test]
    fn typed_control_errors_are_bounded_sanitized_and_actionable() {
        let (_dir, token) = token();
        let error = r#"409:{"code":"operation_conflict","message":"a model operation is already active\nretry later"}"#.to_string();
        let (address, _, worker) = serve(token.clone(), vec!["__PROOF__".into(), error]);
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(
            client.load("gemma-3-4b-it-q4"),
            Err(ClientError::Rejected(
                "a model operation is already activeretry later".into()
            ))
        );
        worker.join().unwrap();
    }

    #[test]
    fn every_polling_reconnect_reproves_on_the_socket_that_receives_bearer() {
        let (_dir, token) = token();
        let operation = |status: &str| {
            format!(
                r#"{{"id":"op","kind":"load","status":"{status}","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}}"#
            )
        };
        let (address, seen, worker) = serve(
            token.clone(),
            vec![
                "__PROOF__".into(),
                operation("running"),
                operation("succeeded"),
            ],
        );
        let client = LiveControlClient::connect(
            address,
            token,
            "runtime",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            client.operation("op").unwrap().status,
            OperationStatus::Running
        );
        assert_eq!(
            client.operation("op").unwrap().status,
            OperationStatus::Succeeded
        );
        worker.join().unwrap();
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 5);
        for (proof, authenticated) in [(1, 2), (3, 4)] {
            assert!(requests[proof].contains("X-Loxa-Challenge:"));
            assert!(!requests[proof]
                .to_ascii_lowercase()
                .contains("authorization:"));
            assert!(requests[authenticated]
                .to_ascii_lowercase()
                .contains("authorization: bearer "));
        }
    }
}
