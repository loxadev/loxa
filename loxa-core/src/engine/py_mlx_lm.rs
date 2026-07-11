use super::{EngineLaunchSpec, ReadinessStrategy};
use std::env;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const REQUIRED_VERSION: &str = "0.31.3";
pub const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
pub const UPSTREAM_MODEL: &str = "default_model";

#[derive(Debug)]
pub enum PyMlxLmError {
    UnsupportedPlatform { os: String, arch: String },
    ModelDirectory(PathBuf),
    ServerNotFound,
    VersionCommandNotFound,
    VersionTimeout { pid: u32 },
    VersionCommandFailed { status: Option<i32>, stderr: String },
    VersionMismatch { expected: String, detected: String },
    Io(io::Error),
}

impl fmt::Display for PyMlxLmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform { os, arch } => write!(
                formatter,
                "py-mlx-lm requires Apple Silicon macOS; detected {os}/{arch}"
            ),
            Self::ModelDirectory(path) => write!(
                formatter,
                "py-mlx-lm model path must be an existing directory: {}",
                path.display()
            ),
            Self::ServerNotFound => formatter.write_str(
                "mlx_lm.server not found; run `uv tool install mlx-lm==0.31.3`",
            ),
            Self::VersionCommandNotFound => formatter.write_str(
                "mlx_lm version command not found; run `uv tool install mlx-lm==0.31.3`",
            ),
            Self::VersionTimeout { pid } => {
                write!(formatter, "mlx_lm --version timed out (pid {pid} was reaped)")
            }
            Self::VersionCommandFailed { status, stderr } => write!(
                formatter,
                "mlx_lm --version failed with status {}: {stderr}",
                status
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            ),
            Self::VersionMismatch { expected, detected } => write!(
                formatter,
                "mlx-lm version mismatch: expected {expected}, detected {detected}; run `uv tool install mlx-lm=={expected}`"
            ),
            Self::Io(error) => write!(formatter, "io error: {error}"),
        }
    }
}

impl Error for PyMlxLmError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for PyMlxLmError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn validate_current_platform() -> Result<(), PyMlxLmError> {
    validate_platform(env::consts::OS, env::consts::ARCH)
}

pub fn validate_platform(os: &str, arch: &str) -> Result<(), PyMlxLmError> {
    if os == "macos" && arch == "aarch64" {
        Ok(())
    } else {
        Err(PyMlxLmError::UnsupportedPlatform {
            os: os.to_string(),
            arch: arch.to_string(),
        })
    }
}

pub fn canonicalize_model_dir(path: &Path) -> Result<PathBuf, PyMlxLmError> {
    if !path.is_dir() {
        return Err(PyMlxLmError::ModelDirectory(path.to_path_buf()));
    }
    fs::canonicalize(path).map_err(PyMlxLmError::Io)
}

pub fn discover_server_from_environment() -> Result<PathBuf, PyMlxLmError> {
    discover_server(
        env::var_os("LOXA_MLX_LM_SERVER").as_deref(),
        env::var_os("PATH").as_deref(),
    )
}

pub fn discover_server(
    environment_override: Option<&OsStr>,
    path: Option<&OsStr>,
) -> Result<PathBuf, PyMlxLmError> {
    if let Some(candidate) = environment_override.map(PathBuf::from) {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    find_on_path("mlx_lm.server", path).ok_or(PyMlxLmError::ServerNotFound)
}

pub fn discover_version_command(
    server: &Path,
    path: Option<&OsStr>,
) -> Result<PathBuf, PyMlxLmError> {
    if let Some(parent) = server.parent() {
        let sibling = parent.join("mlx_lm");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }
    find_on_path("mlx_lm", path).ok_or(PyMlxLmError::VersionCommandNotFound)
}

fn find_on_path(binary: &str, path: Option<&OsStr>) -> Option<PathBuf> {
    path.into_iter()
        .flat_map(env::split_paths)
        .map(|directory| directory.join(binary))
        .find(|candidate| candidate.is_file())
}

pub fn detect_version(version_command: &Path) -> Result<String, PyMlxLmError> {
    let output = run_version_command(version_command, VERSION_TIMEOUT)?;
    validate_version_output(&output)
}

pub fn run_version_command(
    version_command: &Path,
    timeout: Duration,
) -> Result<String, PyMlxLmError> {
    let mut child = Command::new(version_command)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            let output = child.wait_with_output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !output.status.success() {
                return Err(PyMlxLmError::VersionCommandFailed {
                    status: output.status.code(),
                    stderr: stderr.trim().to_string(),
                });
            }
            let rendered = if stdout.trim().is_empty() {
                stderr.trim()
            } else {
                stdout.trim()
            };
            return Ok(rendered.to_string());
        }
        if started.elapsed() >= timeout {
            let pid = child.id();
            let kill = child.kill();
            let wait = child.wait();
            if let Err(error) = kill {
                wait?;
                return Err(PyMlxLmError::Io(error));
            }
            wait?;
            return Err(PyMlxLmError::VersionTimeout { pid });
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub fn validate_version_output(output: &str) -> Result<String, PyMlxLmError> {
    let detected = output.trim();
    if detected == REQUIRED_VERSION {
        Ok(detected.to_string())
    } else {
        Err(PyMlxLmError::VersionMismatch {
            expected: REQUIRED_VERSION.to_string(),
            detected: detected.to_string(),
        })
    }
}

pub fn launch_spec(server: &Path, model: &Path, port: u16, version: &str) -> EngineLaunchSpec {
    EngineLaunchSpec {
        program: server.to_path_buf(),
        args: vec![
            OsString::from("--model"),
            model.as_os_str().to_owned(),
            OsString::from("--host"),
            OsString::from("127.0.0.1"),
            OsString::from("--port"),
            OsString::from(port.to_string()),
        ],
        port,
        engine_name: "mlx-lm".to_string(),
        engine_version: version.to_string(),
        runtime_model: model.display().to_string(),
        upstream_model: UPSTREAM_MODEL.to_string(),
        readiness: ReadinessStrategy::ChatCompletionProbe {
            request_model: UPSTREAM_MODEL.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    fn file(path: &Path) {
        fs::write(path, b"test").expect("write test file");
    }

    #[test]
    fn rejects_non_apple_silicon_macos_platforms() {
        assert!(validate_platform("linux", "aarch64").is_err());
        assert!(validate_platform("macos", "x86_64").is_err());
        assert!(validate_platform("macos", "aarch64").is_ok());
    }

    #[test]
    fn canonicalize_model_requires_a_directory_and_preserves_spaces() {
        let temp = TempDir::new().expect("temp dir");
        let missing = temp.path().join("missing");
        let regular_file = temp.path().join("model.txt");
        let model = temp.path().join("model with spaces");
        file(&regular_file);
        fs::create_dir(&model).expect("create model dir");

        assert!(canonicalize_model_dir(&missing).is_err());
        assert!(canonicalize_model_dir(&regular_file).is_err());
        assert_eq!(
            canonicalize_model_dir(&model).expect("canonical model"),
            fs::canonicalize(model).expect("expected canonical path")
        );
    }

    #[test]
    fn server_discovery_prefers_valid_environment_override_then_path() {
        let temp = TempDir::new().expect("temp dir");
        let env_server = temp.path().join("env mlx_lm.server");
        let path_dir = temp.path().join("bin");
        let path_server = path_dir.join("mlx_lm.server");
        fs::create_dir(&path_dir).expect("create bin");
        file(&env_server);
        file(&path_server);
        let path = std::env::join_paths([path_dir]).expect("join PATH");

        assert_eq!(
            discover_server(Some(env_server.as_os_str()), Some(path.as_os_str()))
                .expect("environment discovery"),
            env_server
        );
        assert_eq!(
            discover_server(Some(OsStr::new("/missing")), Some(path.as_os_str()))
                .expect("PATH discovery"),
            path_server
        );
    }

    #[test]
    fn version_command_discovery_prefers_server_sibling_then_path() {
        let temp = TempDir::new().expect("temp dir");
        let server_dir = temp.path().join("server-bin");
        let path_dir = temp.path().join("path-bin");
        fs::create_dir(&server_dir).expect("create server bin");
        fs::create_dir(&path_dir).expect("create path bin");
        let server = server_dir.join("mlx_lm.server");
        let sibling = server_dir.join("mlx_lm");
        let path_binary = path_dir.join("mlx_lm");
        file(&server);
        file(&sibling);
        file(&path_binary);
        let path = std::env::join_paths([path_dir]).expect("join PATH");

        assert_eq!(
            discover_version_command(&server, Some(path.as_os_str())).expect("sibling command"),
            sibling
        );
        fs::remove_file(&sibling).expect("remove sibling");
        assert_eq!(
            discover_version_command(&server, Some(path.as_os_str())).expect("PATH command"),
            path_binary
        );
    }

    #[test]
    fn exact_version_validation_accepts_only_required_version() {
        assert_eq!(
            validate_version_output("0.31.3\n").expect("required version"),
            REQUIRED_VERSION
        );
        assert!(validate_version_output("0.31.2\n").is_err());
        assert!(validate_version_output("mlx_lm 0.31.3\n").is_err());
        assert!(validate_version_output("0.31.3 3.12.0\n").is_err());
        assert!(validate_version_output("unknown\n").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn version_execution_is_direct() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("mlx_lm");
        fs::write(&version, "#!/bin/sh\nprintf '0.31.3\\n'\n").expect("write executable");
        let mut permissions = fs::metadata(&version).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&version, permissions).expect("set executable");

        assert_eq!(
            run_version_command(&version, Duration::from_secs(5)).expect("version output"),
            "0.31.3"
        );
    }

    #[cfg(unix)]
    #[test]
    fn nonzero_version_exit_preserves_status_and_stderr_without_validating_stdout() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("mlx_lm");
        fs::write(
            &version,
            "#!/bin/sh\nprintf '0.31.3\\n'\nprintf 'broken environment\\n' >&2\nexit 7\n",
        )
        .expect("write executable");
        let mut permissions = fs::metadata(&version).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&version, permissions).expect("set executable");

        let error = run_version_command(&version, Duration::from_secs(5))
            .expect_err("nonzero exit must fail");
        assert!(matches!(
            error,
            PyMlxLmError::VersionCommandFailed {
                status: Some(7),
                ref stderr,
            } if stderr == "broken environment"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn slow_version_process_times_out_promptly_and_is_gone_after_reap() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("mlx_lm");
        fs::write(&version, "#!/bin/sh\nwhile :; do :; done\n").expect("write executable");
        let mut permissions = fs::metadata(&version).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&version, permissions).expect("set executable");

        let started = Instant::now();
        let error = run_version_command(&version, Duration::from_millis(75))
            .expect_err("slow process must time out");
        assert!(started.elapsed() < Duration::from_secs(1));
        let PyMlxLmError::VersionTimeout { pid } = error else {
            panic!("expected timeout with reaped pid, got {error}");
        };
        let status = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("probe timed-out pid");
        assert!(
            !status.success(),
            "timed-out version process {pid} survived"
        );
    }

    #[test]
    fn launch_spec_uses_direct_loopback_arguments_without_shell() {
        let server = Path::new("/tmp/bin/mlx_lm.server");
        let model = Path::new("/tmp/model with spaces");
        let spec = launch_spec(server, model, 8123, REQUIRED_VERSION);

        assert_eq!(spec.program, server);
        assert_eq!(
            spec.args,
            vec![
                OsString::from("--model"),
                model.as_os_str().to_owned(),
                OsString::from("--host"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("8123"),
            ]
        );
        assert_ne!(spec.program.file_name(), Some(OsStr::new("sh")));
        assert!(!spec.args.iter().any(|arg| arg == "-c"));
        assert_eq!(spec.runtime_model, model.display().to_string());
        assert_eq!(spec.upstream_model, "default_model");
    }
}
