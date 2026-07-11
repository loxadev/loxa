use std::env;
use std::ffi::OsStr;
use std::fmt;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::engine::py_mlx_lm::{self, PyMlxLmError};

const MLX_LM_DEFAULT_EXTERNAL_ADDRESS: &str = "127.0.0.1:8080";

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
                DetectedTool {
                    name: "Python MLX (external)".to_string(),
                    detection: detect_py_mlx_lm(),
                },
            ],
        }
    }
}

pub fn detect_py_mlx_lm() -> ToolDetection {
    let environment_override = env::var_os("LOXA_MLX_LM_SERVER");
    let path = env::var_os("PATH");
    detect_py_mlx_lm_with(
        PyMlxLmDetectionContext {
            os: env::consts::OS,
            arch: env::consts::ARCH,
            environment_override: environment_override.as_deref(),
            path: path.as_deref(),
            reachability_scope: "external default endpoint",
            address: MLX_LM_DEFAULT_EXTERNAL_ADDRESS,
        },
        py_mlx_lm::detect_version,
        can_connect,
    )
}

pub struct PyMlxLmDetectionContext<'a> {
    pub os: &'a str,
    pub arch: &'a str,
    pub environment_override: Option<&'a OsStr>,
    pub path: Option<&'a OsStr>,
    pub reachability_scope: &'a str,
    pub address: &'a str,
}

pub fn detect_py_mlx_lm_with(
    context: PyMlxLmDetectionContext<'_>,
    version_runner: impl FnOnce(&Path) -> Result<String, PyMlxLmError>,
    connectivity: impl FnOnce(&str) -> bool,
) -> ToolDetection {
    let PyMlxLmDetectionContext {
        os,
        arch,
        environment_override,
        path,
        reachability_scope,
        address,
    } = context;
    let mut evidence = Vec::new();
    match py_mlx_lm::validate_platform(os, arch) {
        Ok(()) => evidence.push(format!("platform compatible: {os}/{arch}")),
        Err(_) => evidence.push(format!(
            "platform incompatible: {os}/{arch} (requires Apple Silicon macOS)"
        )),
    }

    evidence.push(format!("required version: {}", py_mlx_lm::REQUIRED_VERSION));

    let mut installed = false;
    match py_mlx_lm::discover_server(environment_override, path) {
        Ok(server) => {
            installed = true;
            evidence.push(format!("server path: {}", server.display()));
            match py_mlx_lm::discover_version_command(&server, path) {
                Ok(command) => match version_runner(&command) {
                    Ok(detected) => match py_mlx_lm::validate_version_output(&detected) {
                        Ok(version) => {
                            evidence.push(format!("detected version: {version}"));
                        }
                        Err(PyMlxLmError::VersionMismatch { detected, .. }) => {
                            evidence.push(format!("detected version: {detected} (mismatch)"));
                            evidence.push(mlx_remediation());
                        }
                        Err(error) => {
                            evidence.push(format!("version check failed: {error}"));
                            evidence.push(mlx_remediation());
                        }
                    },
                    Err(PyMlxLmError::VersionMismatch { detected, .. }) => {
                        evidence.push(format!("detected version: {detected} (mismatch)"));
                        evidence.push(mlx_remediation());
                    }
                    Err(error) => {
                        evidence.push(format!("version check failed: {error}"));
                        evidence.push(mlx_remediation());
                    }
                },
                Err(PyMlxLmError::VersionCommandNotFound) => {
                    evidence.push("mlx_lm version command not found".to_string());
                    evidence.push(mlx_remediation());
                }
                Err(error) => {
                    evidence.push(format!("version check failed: {error}"));
                    evidence.push(mlx_remediation());
                }
            }
        }
        Err(PyMlxLmError::ServerNotFound) => {
            evidence.push("mlx_lm.server not found".to_string());
            evidence.push(mlx_remediation());
        }
        Err(error) => {
            evidence.push(format!("server discovery failed: {error}"));
            evidence.push(mlx_remediation());
        }
    }

    let running = connectivity(address);
    evidence.push(if running {
        format!("{reachability_scope} reachable: {address}")
    } else {
        format!("{reachability_scope} not running at {address} (port not reachable)")
    });

    ToolDetection {
        install_state: InstallState::from_detected(installed),
        run_state: RunState::from_detected(running),
        evidence,
    }
}

fn mlx_remediation() -> String {
    format!(
        "remediation: `uv tool install mlx-lm=={}`",
        py_mlx_lm::REQUIRED_VERSION
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::py_mlx_lm::{PyMlxLmError, REQUIRED_VERSION};
    use std::ffi::OsStr;
    use std::fs;
    use tempfile::TempDir;

    fn evidence_contains(detection: &ToolDetection, expected: &str) {
        assert!(
            detection
                .evidence
                .iter()
                .any(|evidence| evidence.contains(expected)),
            "missing evidence `{expected}` in {:?}",
            detection.evidence
        );
    }

    fn detection_context<'a>(
        os: &'a str,
        arch: &'a str,
        environment_override: Option<&'a OsStr>,
        path: Option<&'a OsStr>,
        reachability_scope: &'a str,
        address: &'a str,
    ) -> PyMlxLmDetectionContext<'a> {
        PyMlxLmDetectionContext {
            os,
            arch,
            environment_override,
            path,
            reachability_scope,
            address,
        }
    }

    #[test]
    fn mlx_detection_reports_absent_server_and_exact_remediation() {
        let detection = detect_py_mlx_lm_with(
            detection_context(
                "macos",
                "aarch64",
                None,
                None,
                "test endpoint",
                "127.0.0.1:8080",
            ),
            |_| panic!("version runner must not run without a server"),
            |_| false,
        );

        assert_eq!(detection.install_state, InstallState::NotInstalled);
        assert_eq!(detection.run_state, RunState::NotRunning);
        evidence_contains(&detection, "mlx_lm.server not found");
        evidence_contains(&detection, &format!("required version: {REQUIRED_VERSION}"));
        evidence_contains(
            &detection,
            &format!("uv tool install mlx-lm=={REQUIRED_VERSION}"),
        );
        evidence_contains(&detection, "test endpoint not running at 127.0.0.1:8080");
    }

    #[test]
    fn mlx_detection_reports_present_server_path_and_exact_version() {
        let temp = TempDir::new().expect("temp dir");
        let server = temp.path().join("mlx_lm.server");
        let version_command = temp.path().join("mlx_lm");
        fs::write(&server, b"server").expect("write server");
        fs::write(&version_command, b"command").expect("write version command");

        let detection = detect_py_mlx_lm_with(
            detection_context(
                "macos",
                "aarch64",
                Some(server.as_os_str()),
                None,
                "test endpoint",
                "127.0.0.1:8080",
            ),
            |command| {
                assert_eq!(command, version_command);
                Ok(REQUIRED_VERSION.to_string())
            },
            |_| false,
        );

        assert_eq!(detection.install_state, InstallState::Installed);
        evidence_contains(&detection, &format!("server path: {}", server.display()));
        evidence_contains(&detection, &format!("detected version: {REQUIRED_VERSION}"));
    }

    #[test]
    fn mlx_detection_reports_mismatched_version_and_exact_remediation() {
        let temp = TempDir::new().expect("temp dir");
        let server = temp.path().join("mlx_lm.server");
        let version_command = temp.path().join("mlx_lm");
        fs::write(&server, b"server").expect("write server");
        fs::write(&version_command, b"command").expect("write version command");

        let detection = detect_py_mlx_lm_with(
            detection_context(
                "macos",
                "aarch64",
                Some(server.as_os_str()),
                None,
                "test endpoint",
                "127.0.0.1:8080",
            ),
            |_| Ok("0.31.2".to_string()),
            |_| false,
        );

        assert_eq!(detection.install_state, InstallState::Installed);
        evidence_contains(&detection, "detected version: 0.31.2 (mismatch)");
        evidence_contains(
            &detection,
            &format!("uv tool install mlx-lm=={REQUIRED_VERSION}"),
        );
    }

    #[test]
    fn mlx_detection_reports_bounded_version_timeout() {
        let temp = TempDir::new().expect("temp dir");
        let server = temp.path().join("mlx_lm.server");
        let version_command = temp.path().join("mlx_lm");
        fs::write(&server, b"server").expect("write server");
        fs::write(&version_command, b"command").expect("write version command");

        let detection = detect_py_mlx_lm_with(
            detection_context(
                "macos",
                "aarch64",
                Some(server.as_os_str()),
                None,
                "test endpoint",
                "127.0.0.1:8080",
            ),
            |_| Err(PyMlxLmError::VersionTimeout { pid: 42 }),
            |_| false,
        );

        evidence_contains(
            &detection,
            "version check failed: mlx_lm --version timed out",
        );
        evidence_contains(
            &detection,
            &format!("uv tool install mlx-lm=={REQUIRED_VERSION}"),
        );
    }

    #[test]
    fn mlx_detection_reports_unsupported_platform_without_mutation() {
        let detection = detect_py_mlx_lm_with(
            detection_context(
                "linux",
                "x86_64",
                None,
                Some(OsStr::new("")),
                "test endpoint",
                "127.0.0.1:8080",
            ),
            |_| panic!("version runner must not run without a server"),
            |_| false,
        );

        evidence_contains(
            &detection,
            "platform incompatible: linux/x86_64 (requires Apple Silicon macOS)",
        );
    }

    #[test]
    fn mlx_detection_reports_reachable_server() {
        let detection = detect_py_mlx_lm_with(
            detection_context(
                "macos",
                "aarch64",
                None,
                None,
                "injected candidate endpoint",
                "127.0.0.1:49152",
            ),
            |_| panic!("version runner must not run without a server"),
            |address| address == "127.0.0.1:49152",
        );

        assert_eq!(detection.run_state, RunState::Running);
        evidence_contains(
            &detection,
            "injected candidate endpoint reachable: 127.0.0.1:49152",
        );
    }
}
