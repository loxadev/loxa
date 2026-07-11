use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartNodeRequest {
    pub endpoint: String,
    pub model: String,
    pub engine: String,
}

struct OwnedNode {
    child: Child,
}

pub struct BootstrapState {
    endpoint: String,
    ownership: Ownership,
    owned: Option<OwnedNode>,
    error: Option<String>,
}

impl Default for BootstrapState {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8080".into(),
            ownership: Ownership::None,
            owned: None,
            error: None,
        }
    }
}

pub type SharedBootstrapState = Arc<Mutex<BootstrapState>>;

#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    pub executable: Option<PathBuf>,
    pub startup_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            executable: std::env::var_os("LOXA_EXECUTABLE").map(PathBuf::from),
            startup_timeout: Duration::from_secs(15),
            poll_interval: Duration::from_millis(100),
        }
    }
}

impl BootstrapState {
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
        let address = parse_loopback_endpoint(&request.endpoint)?;
        validate_engine(&request.engine)?;
        if request.model.is_empty() {
            return Err("model must not be empty".into());
        }
        self.endpoint = request.endpoint;
        self.error = None;

        if self.owned.is_some() {
            return Ok(self.current_snapshot());
        }
        if probe_ready(address, config.poll_interval) {
            self.ownership = Ownership::Attached;
            return Ok(self.current_snapshot());
        }

        let executable = config
            .executable
            .clone()
            .unwrap_or_else(|| PathBuf::from("loxa"));
        let child = Command::new(&executable)
            .arg("serve")
            .arg("--model")
            .arg(&request.model)
            .arg("--port")
            .arg(address.port().to_string())
            .arg("--engine")
            .arg(&request.engine)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                format!(
                    "failed to start loxa executable {}: {error}",
                    executable.display()
                )
            })?;
        self.owned = Some(OwnedNode { child });
        self.ownership = Ownership::Owned;

        let deadline = Instant::now() + config.startup_timeout;
        loop {
            if let Some(status) = self
                .owned
                .as_mut()
                .expect("owned child retained while starting")
                .child
                .try_wait()
                .map_err(|error| format!("failed to inspect owned child: {error}"))?
            {
                self.owned = None;
                self.ownership = Ownership::None;
                let message = format!("loxa exited before readiness with status {status}");
                self.error = Some(message.clone());
                return Err(message);
            }
            if probe_ready(address, config.poll_interval) {
                return Ok(self.current_snapshot());
            }
            if Instant::now() >= deadline {
                self.cleanup_owned();
                let message = format!(
                    "loxa startup timed out after {} ms",
                    config.startup_timeout.as_millis()
                );
                self.error = Some(message.clone());
                return Err(message);
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
        let address = parse_loopback_endpoint(&endpoint)?;
        self.endpoint = endpoint;
        let deadline = Instant::now() + config.startup_timeout;
        loop {
            if probe_ready(address, config.poll_interval) {
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
                terminate_exact_child(&mut owned.child)?;
            }
            Err(error) => {
                self.owned = Some(owned);
                self.ownership = Ownership::Owned;
                let message =
                    format!("ownership could not be proven; preserved retained child: {error}");
                self.error = Some(message.clone());
                return Err(message);
            }
        }
        self.ownership = Ownership::None;
        self.error = None;
        Ok(self.current_snapshot())
    }

    pub fn close_window(&mut self) {
        self.refresh_child();
        if self.ownership == Ownership::Attached {
            self.ownership = Ownership::None;
        }
    }

    pub fn exit_app(&mut self) {
        if self.owned.is_some() {
            self.cleanup_owned();
        }
    }

    fn refresh_child(&mut self) {
        let Some(owned) = self.owned.as_mut() else {
            return;
        };
        match owned.child.try_wait() {
            Ok(Some(_)) => {
                self.owned = None;
                self.ownership = parse_loopback_endpoint(&self.endpoint)
                    .ok()
                    .filter(|address| probe_ready(*address, Duration::from_millis(100)))
                    .map_or(Ownership::None, |_| Ownership::Attached);
            }
            Ok(None) => {}
            Err(error) => {
                self.error = Some(format!(
                    "ownership could not be proven; retained child was preserved: {error}"
                ));
            }
        }
    }

    fn cleanup_owned(&mut self) {
        if let Some(mut owned) = self.owned.take() {
            if matches!(owned.child.try_wait(), Ok(None)) {
                let _ = terminate_exact_child(&mut owned.child);
            }
            let _ = owned.child.wait();
        }
        self.ownership = Ownership::None;
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

fn terminate_exact_child(child: &mut Child) -> Result<(), String> {
    #[cfg(unix)]
    {
        let pid =
            i32::try_from(child.id()).map_err(|_| "owned child PID exceeded i32".to_string())?;
        // The retained, still-running Child is the identity proof. SIGINT lets `loxa serve`
        // run its supervisor cleanup instead of abandoning its managed backend state.
        if unsafe { libc::kill(pid, libc::SIGINT) } != 0 {
            return Err(format!(
                "failed to signal exact owned child: {}",
                std::io::Error::last_os_error()
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => thread::sleep(Duration::from_millis(20)),
                Err(error) => return Err(format!("failed to inspect exact owned child: {error}")),
            }
        }
    }
    child
        .kill()
        .map_err(|error| format!("failed to stop exact owned child: {error}"))?;
    child
        .wait()
        .map_err(|error| format!("failed to reap exact owned child: {error}"))?;
    Ok(())
}

impl Drop for BootstrapState {
    fn drop(&mut self) {
        self.exit_app();
    }
}

fn validate_engine(engine: &str) -> Result<(), String> {
    match engine {
        "llama-cpp" | "py-mlx-lm" => Ok(()),
        _ => Err("engine must be llama-cpp or py-mlx-lm".into()),
    }
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
    if !address.ip().is_loopback() {
        return Err("endpoint must use a loopback address".into());
    }
    Ok(address)
}

fn probe_ready(address: SocketAddr, timeout: Duration) -> bool {
    let timeout = timeout.max(Duration::from_millis(1));
    let Ok(mut stream) = TcpStream::connect_timeout(&address, timeout) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let host = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    if write!(
        stream,
        "GET /loxa/status HTTP/1.1\r\nHost: {host}:{}\r\nConnection: close\r\n\r\n",
        address.port()
    )
    .is_err()
    {
        return false;
    }
    let mut response = Vec::new();
    if stream.read_to_end(&mut response).is_err() {
        return false;
    }
    let Some(body_offset) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let headers = &response[..body_offset];
    if !headers.starts_with(b"HTTP/1.1 200 ") && !headers.starts_with(b"HTTP/1.0 200 ") {
        return false;
    }
    serde_json::from_slice::<serde_json::Value>(&response[body_offset + 4..])
        .ok()
        .and_then(|value| {
            value
                .get("health")
                .and_then(|health| health.as_str())
                .map(str::to_owned)
        })
        .is_some_and(|health| health == "ready")
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

pub fn window_closed(state: &SharedBootstrapState) {
    if let Ok(mut state) = state.lock() {
        state.close_window();
    }
}
