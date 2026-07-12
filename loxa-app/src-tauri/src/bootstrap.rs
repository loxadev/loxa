use loxa_core::control::auth::ControlToken;
#[cfg(test)]
use loxa_core::control::contracts::NodeStatus;
use loxa_core::control::contracts::{CONTROL_PROTOCOL_VERSION, NodeIdentityProofResponse};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MAX_PEER_RESPONSE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Ownership {
    None,
    Attached,
    Owned,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapSnapshot {
    pub ownership: Ownership,
    pub endpoint: String,
    pub child_running: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VerifiedPeerIdentity {
    address: SocketAddr,
    node_id: String,
    runtime_identity: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartNodeRequest {
    pub endpoint: String,
}

struct OwnedNode {
    child: Child,
    #[cfg(test)]
    fail_termination_once: bool,
    #[cfg(test)]
    fail_inspection_once: bool,
    #[cfg(test)]
    exit_before_signal_once: bool,
}

pub struct BootstrapState {
    endpoint: String,
    ownership: Ownership,
    owned: Option<OwnedNode>,
    error: Option<String>,
    credential_path: PathBuf,
    verified_peer: Option<VerifiedPeerIdentity>,
    #[cfg(test)]
    fail_startup_inspection_once: bool,
}

impl Default for BootstrapState {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8080".into(),
            ownership: Ownership::None,
            owned: None,
            error: None,
            credential_path: default_credential_path(),
            verified_peer: None,
            #[cfg(test)]
            fail_startup_inspection_once: false,
        }
    }
}

pub type SharedBootstrapState = Arc<Mutex<BootstrapState>>;

#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    pub executable: Option<PathBuf>,
    pub credential_path: PathBuf,
    pub startup_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            executable: Some(private_node_executable()),
            credential_path: default_credential_path(),
            startup_timeout: Duration::from_secs(15),
            poll_interval: Duration::from_millis(100),
        }
    }
}

impl BootstrapState {
    pub fn read_control_token(
        &mut self,
        endpoint: &str,
        proof_timeout: Duration,
    ) -> Result<String, String> {
        self.refresh_child();
        let requested_address =
            parse_loopback_endpoint(endpoint).map_err(|_| token_read_error())?;
        let current_address =
            parse_loopback_endpoint(&self.endpoint).map_err(|_| token_read_error())?;
        let expected = self
            .verified_peer
            .clone()
            .filter(|peer| {
                self.ownership != Ownership::None
                    && peer.address == requested_address
                    && peer.address == current_address
            })
            .ok_or_else(token_read_error)?;
        let token = ControlToken::load(&self.credential_path).map_err(|_| token_read_error())?;
        let proved = prove_compatible_with_token(requested_address, proof_timeout, &token);
        if proved.as_ref() != Some(&expected) {
            self.verified_peer = None;
            return Err(token_read_error());
        }
        Ok(token.expose_for_authorization())
    }

    pub fn snapshot(&mut self) -> BootstrapSnapshot {
        self.refresh_child();
        self.current_snapshot()
    }

    pub fn start_with_config(
        &mut self,
        request: StartNodeRequest,
        config: &BootstrapConfig,
    ) -> Result<BootstrapSnapshot, String> {
        self.refresh_child();
        if self.owned.is_some() {
            if request.endpoint == self.endpoint {
                return Ok(self.current_snapshot());
            }
            return self.fail(format!(
                "an exact app-owned child is already retained at {}; stop it before targeting {}",
                self.endpoint, request.endpoint
            ));
        }
        let address = match parse_loopback_endpoint(&request.endpoint) {
            Ok(address) => address,
            Err(error) => return self.fail(error),
        };
        self.endpoint = request.endpoint;
        self.credential_path = config.credential_path.clone();
        self.verified_peer = None;
        self.error = None;

        if let Some(peer) = prove_compatible(address, config.poll_interval, &config.credential_path)
        {
            self.verified_peer = Some(peer);
            self.ownership = Ownership::Attached;
            return Ok(self.current_snapshot());
        }

        let executable = config
            .executable
            .clone()
            .unwrap_or_else(private_node_executable);
        let child = match Command::new(&executable)
            .arg("--port")
            .arg(address.port().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                return self.fail(format!(
                    "failed to start private loxa-node executable {}: {error}",
                    executable.display()
                ));
            }
        };
        self.owned = Some(OwnedNode {
            child,
            #[cfg(test)]
            fail_termination_once: false,
            #[cfg(test)]
            fail_inspection_once: std::mem::take(&mut self.fail_startup_inspection_once),
            #[cfg(test)]
            exit_before_signal_once: false,
        });
        self.ownership = Ownership::Owned;

        let deadline = Instant::now() + config.startup_timeout;
        loop {
            let inspection = inspect_owned_node(
                self.owned
                    .as_mut()
                    .expect("owned child retained while starting"),
            );
            match inspection {
                Ok(Some(status)) => {
                    self.owned = None;
                    self.ownership = Ownership::None;
                    self.verified_peer = None;
                    let message = format!("loxa-node exited before startup with status {status}");
                    self.error = Some(message.clone());
                    return Err(message);
                }
                Ok(None) => {}
                Err(error) => {
                    let owned = self
                        .owned
                        .take()
                        .expect("failed inspection retains exact child");
                    return self.preserve_after_termination_failure(
                        owned,
                        format!("failed to inspect exact owned child during startup: {error}"),
                    );
                }
            }
            if let Some(peer) =
                prove_compatible(address, config.poll_interval, &config.credential_path)
            {
                self.verified_peer = Some(peer);
                return Ok(self.current_snapshot());
            }
            if Instant::now() >= deadline {
                let message = format!(
                    "loxa-node startup timed out after {} ms",
                    config.startup_timeout.as_millis()
                );
                return match self.cleanup_owned(&message) {
                    Ok(()) => self.fail(message),
                    Err(error) => Err(error),
                };
            }
            thread::sleep(config.poll_interval);
        }
    }

    pub fn attach_with_config(
        &mut self,
        endpoint: String,
        config: &BootstrapConfig,
    ) -> Result<BootstrapSnapshot, String> {
        self.refresh_child();
        if self.owned.is_some() {
            if endpoint == self.endpoint {
                return Ok(self.current_snapshot());
            }
            return self.fail(format!(
                "an exact app-owned child is already retained at {}; stop it before attaching to {}",
                self.endpoint, endpoint
            ));
        }
        let address = match parse_loopback_endpoint(&endpoint) {
            Ok(address) => address,
            Err(error) => return self.fail(error),
        };
        self.endpoint = endpoint;
        self.credential_path = config.credential_path.clone();
        self.verified_peer = None;
        let deadline = Instant::now() + config.startup_timeout;
        loop {
            if let Some(peer) =
                prove_compatible(address, config.poll_interval, &config.credential_path)
            {
                self.verified_peer = Some(peer);
                if self.owned.is_none() {
                    self.ownership = Ownership::Attached;
                }
                self.error = None;
                return Ok(self.current_snapshot());
            }
            if Instant::now() >= deadline {
                self.ownership = if self.owned.is_some() {
                    Ownership::Owned
                } else {
                    Ownership::None
                };
                if self.ownership == Ownership::None {
                    self.verified_peer = None;
                }
                let message = format!(
                    "attach timed out after {} ms",
                    config.startup_timeout.as_millis()
                );
                self.error = Some(message.clone());
                return Err(message);
            }
            thread::sleep(config.poll_interval);
        }
    }

    pub fn stop_owned(&mut self) -> Result<BootstrapSnapshot, String> {
        let Some(mut owned) = self.owned.take() else {
            return Err("no exact app-owned child is retained".into());
        };
        match owned.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = terminate_owned_node(&mut owned) {
                    return self.preserve_after_termination_failure(owned, error);
                }
            }
            Err(error) => {
                return self.preserve_after_termination_failure(
                    owned,
                    format!("ownership could not be proven: {error}"),
                );
            }
        }
        self.ownership = Ownership::None;
        self.verified_peer = None;
        self.error = None;
        Ok(self.current_snapshot())
    }

    pub fn close_window(&mut self) {
        self.refresh_child();
        if self.ownership == Ownership::Attached {
            self.ownership = Ownership::None;
            self.verified_peer = None;
        }
    }

    pub fn exit_app(&mut self) -> Result<(), String> {
        if self.owned.is_some() {
            self.cleanup_owned("application exit cleanup failed")?;
        }
        Ok(())
    }

    fn refresh_child(&mut self) {
        let Some(owned) = self.owned.as_mut() else {
            return;
        };
        match owned.child.try_wait() {
            Ok(Some(_)) => {
                self.owned = None;
                let peer = parse_loopback_endpoint(&self.endpoint)
                    .ok()
                    .and_then(|address| {
                        prove_compatible(address, Duration::from_millis(500), &self.credential_path)
                    });
                self.ownership = if peer.is_some() {
                    Ownership::Attached
                } else {
                    Ownership::None
                };
                self.verified_peer = peer;
            }
            Ok(None) => {}
            Err(error) => {
                self.error = Some(format!(
                    "ownership could not be proven; retained child was preserved: {error}"
                ));
            }
        }
    }

    fn cleanup_owned(&mut self, context: &str) -> Result<(), String> {
        if let Some(mut owned) = self.owned.take() {
            match owned.child.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if let Err(error) = terminate_owned_node(&mut owned) {
                        return self.preserve_after_termination_failure(
                            owned,
                            format!("{context}: {error}"),
                        );
                    }
                }
                Err(error) => {
                    return self.preserve_after_termination_failure(
                        owned,
                        format!("{context}: failed to inspect exact owned child: {error}"),
                    );
                }
            }
        }
        self.ownership = Ownership::None;
        self.verified_peer = None;
        Ok(())
    }

    fn preserve_after_termination_failure<T>(
        &mut self,
        owned: OwnedNode,
        error: String,
    ) -> Result<T, String> {
        let message = format!("recovery required; exact owned child was preserved: {error}");
        self.owned = Some(owned);
        self.ownership = Ownership::Owned;
        self.error = Some(message.clone());
        Err(message)
    }

    fn fail<T>(&mut self, message: String) -> Result<T, String> {
        self.error = Some(message.clone());
        Err(message)
    }

    fn current_snapshot(&self) -> BootstrapSnapshot {
        BootstrapSnapshot {
            ownership: self.ownership.clone(),
            endpoint: self.endpoint.clone(),
            child_running: self.owned.is_some(),
            error: self.error.clone(),
        }
    }
}

fn terminate_owned_node(owned: &mut OwnedNode) -> Result<(), String> {
    #[cfg(test)]
    if std::mem::take(&mut owned.fail_termination_once) {
        return Err("injected termination failure".into());
    }
    #[cfg(all(test, unix))]
    if std::mem::take(&mut owned.exit_before_signal_once) {
        let pid = i32::try_from(owned.child.id())
            .map_err(|_| "owned child PID exceeded i32".to_string())?;
        if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
            return Err(format!(
                "failed to inject child exit race: {}",
                std::io::Error::last_os_error()
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
    terminate_exact_child(&mut owned.child)
}

fn inspect_owned_node(owned: &mut OwnedNode) -> std::io::Result<Option<std::process::ExitStatus>> {
    #[cfg(test)]
    if std::mem::take(&mut owned.fail_inspection_once) {
        return Err(std::io::Error::other("injected child inspection failure"));
    }
    owned.child.try_wait()
}

fn terminate_exact_child(child: &mut Child) -> Result<(), String> {
    #[cfg(unix)]
    {
        let pid =
            i32::try_from(child.id()).map_err(|_| "owned child PID exceeded i32".to_string())?;
        // Safety invariant: this module keeps the only Child handle and every wait/try_wait runs
        // synchronously while BootstrapState is mutex-guarded; no background or concurrent waiter
        // can reap it. If it exits after the caller's last Ok(None), Unix retains it as a zombie
        // and reserves its PID until our try_wait reaps it, so this signal cannot hit a replacement.
        // SIGINT lets `loxa serve` run supervisor cleanup instead of abandoning managed state.
        if unsafe { libc::kill(pid, libc::SIGINT) } != 0 {
            return Err(format!(
                "failed to signal exact owned child: {}",
                std::io::Error::last_os_error()
            ));
        }
        if wait_for_child_exit(child, Duration::from_secs(5))? {
            return Ok(());
        }
    }
    child
        .kill()
        .map_err(|error| format!("failed to stop exact owned child: {error}"))?;
    if wait_for_child_exit(child, Duration::from_secs(1))? {
        Ok(())
    } else {
        Err("exact owned child did not exit after forced termination".into())
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> Result<bool, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(true),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => return Ok(false),
            Err(error) => return Err(format!("failed to inspect exact owned child: {error}")),
        }
    }
}

fn private_node_executable() -> PathBuf {
    let file_name = if cfg!(windows) {
        "loxa-node.exe"
    } else {
        "loxa-node"
    };
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join(file_name)))
        .unwrap_or_else(|| PathBuf::from("__missing_private_loxa_node__").join(file_name))
}

fn default_credential_path() -> PathBuf {
    let home = if cfg!(windows) {
        std::env::var_os("USERPROFILE")
    } else {
        std::env::var_os("HOME")
    };
    home.map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("__missing_loxa_home__"))
        .join(".loxa/control.token")
}

fn token_read_error() -> String {
    "The local Loxa control credential is unavailable or unsafe.".to_string()
}

fn parse_loopback_endpoint(endpoint: &str) -> Result<SocketAddr, String> {
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| "endpoint must use http on loopback".to_string())?;
    if authority.contains('/') || authority.contains('?') || authority.contains('#') {
        return Err("endpoint must contain only a loopback host and port".into());
    }
    let address: SocketAddr = authority
        .parse()
        .map_err(|_| "endpoint must contain a numeric loopback address and port".to_string())?;
    if !address.ip().is_ipv4() || !address.ip().is_loopback() {
        return Err("endpoint must use an IPv4 loopback address".into());
    }
    if address.port() == 0 {
        return Err("endpoint port must be between 1 and 65535".into());
    }
    Ok(address)
}

#[cfg(test)]
fn probe_compatible(
    address: SocketAddr,
    timeout: Duration,
    credential_path: &std::path::Path,
) -> bool {
    prove_compatible(address, timeout, credential_path).is_some()
}

fn prove_compatible(
    address: SocketAddr,
    timeout: Duration,
    credential_path: &std::path::Path,
) -> Option<VerifiedPeerIdentity> {
    let Ok(token) = ControlToken::load(credential_path) else {
        return None;
    };
    prove_compatible_with_token(address, timeout, &token)
}

fn prove_compatible_with_token(
    address: SocketAddr,
    timeout: Duration,
    token: &ControlToken,
) -> Option<VerifiedPeerIdentity> {
    let mut nonce_bytes = [0_u8; 32];
    if getrandom::fill(&mut nonce_bytes).is_err() {
        return None;
    }
    let nonce = encode_hex(&nonce_bytes);
    let deadline = Instant::now() + timeout.max(Duration::from_millis(1));
    let timeout = timeout.max(Duration::from_millis(1));
    let Ok(mut stream) = TcpStream::connect_timeout(&address, timeout) else {
        return None;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let host = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    if write!(
        stream,
        "GET /loxa/v1/node HTTP/1.1\r\nHost: {host}:{}\r\nX-Loxa-Challenge: {nonce}\r\nConnection: close\r\n\r\n",
        address.port(),
    )
    .is_err()
    {
        return None;
    }
    let mut response = Vec::with_capacity(1024);
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        let _ = stream.set_read_timeout(Some(remaining.max(Duration::from_millis(1))));
        let mut chunk = [0_u8; 1024];
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => {
                if response.len() + count > MAX_PEER_RESPONSE_BYTES {
                    return None;
                }
                response.extend_from_slice(&chunk[..count]);
            }
            Err(_) => return None,
        }
    }
    let body_offset = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let headers = &response[..body_offset];
    if !headers.starts_with(b"HTTP/1.1 200 ") && !headers.starts_with(b"HTTP/1.0 200 ") {
        return None;
    }
    let value =
        serde_json::from_slice::<NodeIdentityProofResponse>(&response[body_offset + 4..]).ok()?;
    let proof_is_valid = token.verify_node_identity_proof(
        &nonce,
        &value.node_id,
        &value.runtime_identity,
        value.status,
        &value.challenge_proof,
    );
    if value.protocol_version != CONTROL_PROTOCOL_VERSION
        || value.node_id.is_empty()
        || value.runtime_identity.is_empty()
        || !proof_is_valid
    {
        return None;
    }
    Some(VerifiedPeerIdentity {
        address,
        node_id: value.node_id,
        runtime_identity: value.runtime_identity,
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[tauri::command]
pub fn bootstrap_snapshot(state: tauri::State<'_, SharedBootstrapState>) -> BootstrapSnapshot {
    state.lock().expect("bootstrap state poisoned").snapshot()
}

#[tauri::command]
pub fn start_node(
    request: StartNodeRequest,
    state: tauri::State<'_, SharedBootstrapState>,
) -> Result<BootstrapSnapshot, String> {
    state
        .lock()
        .map_err(|_| "bootstrap state poisoned".to_string())?
        .start_with_config(request, &BootstrapConfig::default())
}

#[tauri::command]
pub fn attach_node(
    endpoint: String,
    state: tauri::State<'_, SharedBootstrapState>,
) -> Result<BootstrapSnapshot, String> {
    state
        .lock()
        .map_err(|_| "bootstrap state poisoned".to_string())?
        .attach_with_config(endpoint, &BootstrapConfig::default())
}

#[tauri::command]
pub fn stop_owned_node(
    state: tauri::State<'_, SharedBootstrapState>,
) -> Result<BootstrapSnapshot, String> {
    state
        .lock()
        .map_err(|_| "bootstrap state poisoned".to_string())?
        .stop_owned()
}

#[tauri::command]
pub fn read_control_token(
    endpoint: String,
    state: tauri::State<'_, SharedBootstrapState>,
) -> Result<String, String> {
    state
        .lock()
        .map_err(|_| "bootstrap state poisoned".to_string())?
        .read_control_token(&endpoint, Duration::from_millis(500))
}

pub fn window_closed(state: &SharedBootstrapState) {
    if let Ok(mut state) = state.lock() {
        state.close_window();
    }
}

pub fn handle_exit_event<W: Write>(state: &SharedBootstrapState, stderr: &mut W) -> bool {
    let result = match state.lock() {
        Ok(mut state) => state.exit_app(),
        Err(_) => Err("recovery required; bootstrap state lock is poisoned".into()),
    };
    match result {
        Ok(()) => true,
        Err(error) => {
            let _ = writeln!(stderr, "loxa desktop exit cleanup failed: {error}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};

    const TEST_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    static TOKEN_PATH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn test_token_path() -> PathBuf {
        let path = std::env::temp_dir()
            .join(format!(
                "loxa-probe-token-{}-{}",
                std::process::id(),
                TOKEN_PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            ))
            .join("control.token");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, format!("{TEST_SECRET}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                path.parent().unwrap(),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        path
    }

    fn serve_probe_response(
        body: Vec<u8>,
        drip: Option<Duration>,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut chunk = [0_u8; 1024];
                let count = stream.read(&mut chunk).unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8_lossy(&request);
            assert!(!request.contains("Authorization:"));
            let nonce = request
                .lines()
                .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                .unwrap();
            let token = ControlToken::load(&test_token_path()).unwrap();
            let proof = token
                .node_identity_proof(nonce, "node", "runtime", NodeStatus::Unloaded)
                .unwrap();
            let body = String::from_utf8(body)
                .unwrap()
                .replace("__PROOF__", &proof)
                .into_bytes();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            if let Some(delay) = drip {
                for byte in body {
                    if stream.write_all(&[byte]).is_err() {
                        break;
                    }
                    thread::sleep(delay);
                }
            } else {
                let _ = stream.write_all(&body);
            }
        });
        (address, worker)
    }

    fn identity_json(version: u32) -> Vec<u8> {
        format!(r#"{{"protocol_version":{version},"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"__PROOF__"}}"#).into_bytes()
    }

    fn serve_identity_response(
        node_id: &str,
        runtime_identity: &str,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let node_id = node_id.to_owned();
        let runtime_identity = runtime_identity.to_owned();
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut chunk = [0_u8; 1024];
                let count = stream.read(&mut chunk).unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8_lossy(&request);
            assert!(!request.contains("Authorization:"));
            let nonce = request
                .lines()
                .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                .unwrap();
            let token = ControlToken::load(&test_token_path()).unwrap();
            let proof = token
                .node_identity_proof(nonce, &node_id, &runtime_identity, NodeStatus::Unloaded)
                .unwrap();
            let body = format!(
                r#"{{"protocol_version":1,"node_id":"{node_id}","runtime_identity":"{runtime_identity}","status":"unloaded","challenge_proof":"{proof}"}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (address, worker)
    }

    #[test]
    fn authenticated_probe_rejects_legacy_spoofs_wrong_versions_and_extra_fields() {
        let token = test_token_path();
        for body in [
            br#"{"node_id":"spoof","health":"ready","model":"loxa"}"#.to_vec(),
            identity_json(0),
            br#"{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"__PROOF__","extra":true}"#.to_vec(),
        ] {
            let (address, worker) = serve_probe_response(body, None);
            assert!(!probe_compatible(address, Duration::from_secs(1), &token));
            worker.join().unwrap();
        }
        let malformed = token.with_file_name("wrong.token");
        std::fs::write(&malformed, "wrong\n").unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        assert!(!probe_compatible(
            address,
            Duration::from_secs(1),
            &malformed
        ));
    }

    #[test]
    fn authenticated_probe_accepts_only_the_closed_current_identity() {
        let token = test_token_path();
        let (address, worker) = serve_probe_response(identity_json(1), None);
        assert!(probe_compatible(address, Duration::from_secs(1), &token));
        worker.join().unwrap();
    }

    #[test]
    fn narrow_control_token_read_requires_a_fresh_exact_peer_proof() {
        let credential_path = test_token_path();
        let mut disconnected = BootstrapState {
            credential_path: credential_path.clone(),
            ..BootstrapState::default()
        };
        assert!(
            disconnected
                .read_control_token("http://127.0.0.1:8080", Duration::from_secs(1))
                .is_err()
        );

        let (address, worker) = serve_identity_response("node", "runtime");
        let endpoint = format!("http://{address}");
        let mut attached = BootstrapState {
            endpoint: endpoint.clone(),
            ownership: Ownership::Attached,
            credential_path,
            verified_peer: Some(VerifiedPeerIdentity {
                address,
                node_id: "node".into(),
                runtime_identity: "runtime".into(),
            }),
            ..BootstrapState::default()
        };
        assert_eq!(
            attached
                .read_control_token(&endpoint, Duration::from_secs(1))
                .unwrap(),
            TEST_SECRET
        );
        worker.join().unwrap();
        assert!(
            attached
                .read_control_token("http://127.0.0.1:1", Duration::from_millis(10))
                .is_err()
        );
    }

    #[test]
    fn narrow_control_token_read_rejects_a_valid_replacement_identity() {
        let (address, worker) = serve_identity_response("replacement", "new-runtime");
        let endpoint = format!("http://{address}");
        let mut state = BootstrapState {
            endpoint: endpoint.clone(),
            ownership: Ownership::Attached,
            credential_path: test_token_path(),
            verified_peer: Some(VerifiedPeerIdentity {
                address,
                node_id: "original".into(),
                runtime_identity: "old-runtime".into(),
            }),
            ..BootstrapState::default()
        };
        assert!(
            state
                .read_control_token(&endpoint, Duration::from_secs(1))
                .is_err()
        );
        assert!(state.verified_peer.is_none());
        worker.join().unwrap();
    }

    #[test]
    fn narrow_control_token_read_never_offers_the_secret_to_an_unproven_spoof() {
        let (address, worker) = serve_probe_response(
            br#"{"node_id":"spoof","health":"ready","model":"loxa"}"#.to_vec(),
            None,
        );
        let endpoint = format!("http://{address}");
        let mut state = BootstrapState {
            endpoint: endpoint.clone(),
            ownership: Ownership::Attached,
            credential_path: test_token_path(),
            verified_peer: Some(VerifiedPeerIdentity {
                address,
                node_id: "node".into(),
                runtime_identity: "runtime".into(),
            }),
            ..BootstrapState::default()
        };

        assert!(
            state
                .read_control_token(&endpoint, Duration::from_secs(1))
                .is_err()
        );
        worker.join().unwrap();
    }

    #[test]
    fn narrow_control_token_read_sanitizes_credential_failures() {
        let parent = std::env::temp_dir().join(format!(
            "loxa-token-read-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&parent).unwrap();
        let path = parent.join("control.token");
        std::fs::write(&path, format!("{TEST_SECRET}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut state = BootstrapState {
            endpoint: "http://127.0.0.1:8080".into(),
            ownership: Ownership::Attached,
            credential_path: path.clone(),
            verified_peer: Some(VerifiedPeerIdentity {
                address: "127.0.0.1:8080".parse().unwrap(),
                node_id: "node".into(),
                runtime_identity: "runtime".into(),
            }),
            ..BootstrapState::default()
        };
        std::fs::write(&path, "unsafe-secret\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let error = state
            .read_control_token("http://127.0.0.1:8080", Duration::from_millis(10))
            .unwrap_err();
        assert_eq!(
            error,
            "The local Loxa control credential is unavailable or unsafe."
        );
        assert!(!error.contains("unsafe-secret"));
        assert!(!error.contains(path.to_string_lossy().as_ref()));
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn a_valid_old_proof_is_rejected_when_replayed_for_a_fresh_nonce() {
        let token = ControlToken::load(&test_token_path()).unwrap();
        let old_nonce = "01".repeat(32);
        let fresh_nonce = "02".repeat(32);
        let proof = token
            .node_identity_proof(&old_nonce, "node", "runtime", NodeStatus::Unloaded)
            .unwrap();
        assert!(!token.verify_node_identity_proof(
            &fresh_nonce,
            "node",
            "runtime",
            NodeStatus::Unloaded,
            &proof,
        ));
    }

    #[test]
    fn probe_rejects_oversize_and_absolute_deadline_drip_responses() {
        let token = test_token_path();
        let (address, worker) = serve_probe_response(vec![b'x'; MAX_PEER_RESPONSE_BYTES + 1], None);
        assert!(!probe_compatible(address, Duration::from_secs(1), &token));
        worker.join().unwrap();

        let (address, worker) =
            serve_probe_response(identity_json(1), Some(Duration::from_millis(20)));
        let began = Instant::now();
        assert!(!probe_compatible(
            address,
            Duration::from_millis(80),
            &token
        ));
        assert!(began.elapsed() < Duration::from_millis(300));
        worker.join().unwrap();
    }

    fn state_with_sleeping_child() -> BootstrapState {
        let child = Command::new("sleep").arg("30").spawn().unwrap();
        BootstrapState {
            endpoint: "http://127.0.0.1:18080".into(),
            ownership: Ownership::Owned,
            owned: Some(OwnedNode {
                child,
                fail_termination_once: true,
                fail_inspection_once: false,
                exit_before_signal_once: false,
            }),
            error: None,
            credential_path: default_credential_path(),
            verified_peer: None,
            fail_startup_inspection_once: false,
        }
    }

    #[test]
    fn termination_failure_preserves_handle_and_safe_retry() {
        let mut state = state_with_sleeping_child();
        let error = state.stop_owned().unwrap_err();
        assert!(error.contains("recovery required"), "{error}");
        let failed = state.snapshot();
        assert_eq!(failed.ownership, Ownership::Owned);
        assert!(failed.child_running);
        assert_eq!(failed.error.as_deref(), Some(error.as_str()));

        let stopped = state.stop_owned().unwrap();
        assert_eq!(stopped.ownership, Ownership::None);
        assert!(!stopped.child_running);
    }

    #[test]
    fn exit_cleanup_failure_is_bounded_observable_and_retryable() {
        let mut state = state_with_sleeping_child();
        let began = Instant::now();
        let error = state.exit_app().unwrap_err();
        assert!(began.elapsed() < Duration::from_secs(1));
        assert!(error.contains("recovery required"), "{error}");
        assert!(state.snapshot().child_running);
        state.stop_owned().unwrap();
    }

    #[test]
    fn timeout_cleanup_failure_is_bounded_and_retains_error_and_handle() {
        let mut state = state_with_sleeping_child();
        let began = Instant::now();
        let error = state.cleanup_owned("loxa startup timed out").unwrap_err();
        assert!(began.elapsed() < Duration::from_secs(1));
        assert!(error.contains("startup timed out"), "{error}");
        let snapshot = state.snapshot();
        assert_eq!(snapshot.ownership, Ownership::Owned);
        assert!(snapshot.child_running);
        assert_eq!(snapshot.error.as_deref(), Some(error.as_str()));
        state.stop_owned().unwrap();
    }

    #[test]
    fn startup_inspection_failure_preserves_exact_child_and_recovery_error() {
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake loxa executable");
        let mut state = BootstrapState {
            fail_startup_inspection_once: true,
            ..BootstrapState::default()
        };
        let error = state
            .start_with_config(
                StartNodeRequest {
                    endpoint: "http://127.0.0.1:49191".into(),
                },
                &BootstrapConfig {
                    executable: Some(fixture),
                    credential_path: default_credential_path(),
                    startup_timeout: Duration::from_millis(200),
                    poll_interval: Duration::from_millis(10),
                },
            )
            .unwrap_err();
        assert!(error.contains("recovery required"), "{error}");
        assert!(error.contains("inspect"), "{error}");
        let snapshot = state.snapshot();
        assert_eq!(snapshot.ownership, Ownership::Owned);
        assert!(snapshot.child_running);
        assert_eq!(snapshot.error.as_deref(), Some(error.as_str()));
        state.stop_owned().unwrap();
    }

    #[test]
    fn exit_event_reports_cleanup_failure_deterministically() {
        let state = Arc::new(Mutex::new(state_with_sleeping_child()));
        let mut stderr = Vec::new();
        assert!(!handle_exit_event(&state, &mut stderr));
        let evidence = String::from_utf8(stderr).unwrap();
        assert!(
            evidence.contains("desktop exit cleanup failed"),
            "{evidence}"
        );
        assert!(evidence.contains("recovery required"), "{evidence}");
        let mut state = state.lock().unwrap();
        assert!(state.snapshot().child_running);
        state.stop_owned().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn child_exit_between_observation_and_signal_is_reaped_without_retargeting() {
        let mut state = state_with_sleeping_child();
        let owned = state.owned.as_mut().unwrap();
        owned.fail_termination_once = false;
        owned.exit_before_signal_once = true;
        let stopped = state.stop_owned().unwrap();
        assert_eq!(stopped.ownership, Ownership::None);
        assert!(!stopped.child_running);
    }
}
