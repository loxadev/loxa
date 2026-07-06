use std::env;
use std::fmt;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallState {
    Installed,
    NotInstalled,
}

impl InstallState {
    fn from_detected(installed: bool) -> Self {
        if installed {
            Self::Installed
        } else {
            Self::NotInstalled
        }
    }
}

impl fmt::Display for InstallState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self {
            Self::Installed => "installed",
            Self::NotInstalled => "not installed",
        };
        f.write_str(status)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Running,
    NotRunning,
}

impl RunState {
    fn from_detected(running: bool) -> Self {
        if running {
            Self::Running
        } else {
            Self::NotRunning
        }
    }
}

impl fmt::Display for RunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self {
            Self::Running => "running",
            Self::NotRunning => "not running",
        };
        f.write_str(status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDetection {
    pub install_state: InstallState,
    pub run_state: RunState,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedTool {
    pub name: String,
    pub detection: ToolDetection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalToolsReport {
    pub tools: Vec<DetectedTool>,
}

impl LocalToolsReport {
    pub fn detect() -> Self {
        Self {
            tools: vec![
                DetectedTool {
                    name: "Ollama".to_string(),
                    detection: detect_ollama(),
                },
                DetectedTool {
                    name: "LM Studio".to_string(),
                    detection: detect_lm_studio(),
                },
            ],
        }
    }
}

pub fn detect_ollama() -> ToolDetection {
    let mut evidence = Vec::new();
    let mut installed = false;

    if let Some(path) = find_on_path("ollama") {
        installed = true;
        evidence.push(format!("binary found on PATH: {}", path.display()));
    } else {
        evidence.push("binary not found on PATH: ollama".to_string());
    }

    if let Some(path) = home_path(".ollama") {
        if path.is_dir() {
            installed = true;
            evidence.push(format!("directory found: {}", path.display()));
        } else {
            evidence.push(format!("directory not found: {}", path.display()));
        }
    } else {
        evidence.push("home directory unknown; could not check ~/.ollama".to_string());
    }

    let running = can_connect("127.0.0.1:11434");
    evidence.push(port_evidence("127.0.0.1:11434", running));

    ToolDetection {
        install_state: InstallState::from_detected(installed),
        run_state: RunState::from_detected(running),
        evidence,
    }
}

pub fn detect_lm_studio() -> ToolDetection {
    let mut evidence = Vec::new();
    let mut installed = false;

    let app_path = Path::new("/Applications/LM Studio.app");
    if app_path.exists() {
        installed = true;
        evidence.push(format!("app found: {}", app_path.display()));
    } else {
        evidence.push(format!("app not found: {}", app_path.display()));
    }

    if let Some(path) = home_path(".lmstudio") {
        if path.is_dir() {
            installed = true;
            evidence.push(format!("directory found: {}", path.display()));
        } else {
            evidence.push(format!("directory not found: {}", path.display()));
        }
    } else {
        evidence.push("home directory unknown; could not check ~/.lmstudio".to_string());
    }

    if let Some(path) = find_on_path("lms") {
        installed = true;
        evidence.push(format!("binary found on PATH: {}", path.display()));
    } else {
        evidence.push("binary not found on PATH: lms".to_string());
    }

    let running = can_connect("127.0.0.1:1234");
    evidence.push(port_evidence("127.0.0.1:1234", running));

    ToolDetection {
        install_state: InstallState::from_detected(installed),
        run_state: RunState::from_detected(running),
        evidence,
    }
}

fn home_path(child: &str) -> Option<PathBuf> {
    env::var_os("HOME").map(|home| PathBuf::from(home).join(child))
}

fn find_on_path(binary: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;

    env::split_paths(&paths)
        .map(|path| path.join(binary))
        .find(|candidate| candidate.is_file())
}

fn can_connect(addr: &str) -> bool {
    let Ok(addr) = addr.parse::<SocketAddr>() else {
        return false;
    };

    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}

fn port_evidence(addr: &str, running: bool) -> String {
    if running {
        format!("port reachable: {addr}")
    } else {
        format!("port not reachable: {addr}")
    }
}
