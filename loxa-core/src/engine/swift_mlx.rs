use std::env;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const REQUIRED_VERSION: &str = "0.1.0";
const SERVER_ENV_VAR: &str = "LOXA_SWIFT_MLX_SERVER";
const SERVER_BINARY: &str = "loxa-mlx";
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
const RANDOM_DEVICE: &str = "/dev/urandom";

#[derive(Clone, PartialEq, Eq)]
pub struct SwiftMlxLaunchConfig {
    pub program: PathBuf,
    pub model: PathBuf,
    pub port: u16,
    pub engine_token: String,
    pub engine_version: String,
}

impl SwiftMlxLaunchConfig {
    pub fn args(&self) -> Vec<OsString> {
        vec![
            OsString::from("serve"),
            OsString::from("--model"),
            self.model.as_os_str().to_owned(),
            OsString::from("--host"),
            OsString::from("127.0.0.1"),
            OsString::from("--port"),
            OsString::from(self.port.to_string()),
            OsString::from("--engine-token"),
            OsString::from(&self.engine_token),
        ]
    }
}

impl fmt::Debug for SwiftMlxLaunchConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SwiftMlxLaunchConfig")
            .field("program", &self.program)
            .field("model", &self.model)
            .field("port", &self.port)
            .field("engine_token", &"[REDACTED]")
            .field("engine_version", &self.engine_version)
            .finish()
    }
}

#[derive(Debug)]
pub enum SwiftMlxError {
    UnsupportedPlatform { os: String, arch: String },
    ModelDirectory(PathBuf),
    ServerNotFound,
    NotExecutable(PathBuf),
    VersionTimeout { pid: u32 },
    VersionCommandFailed { status: Option<i32>, stderr: String },
    VersionMismatch { expected: String, detected: String },
    RandomToken(io::Error),
    Io(io::Error),
}

impl fmt::Display for SwiftMlxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform { os, arch } => write!(
                formatter,
                "Swift MLX requires Apple Silicon macOS; detected {os}/{arch}"
            ),
            Self::ModelDirectory(path) => write!(
                formatter,
                "Swift MLX model path must be an existing directory: {}",
                path.display()
            ),
            Self::ServerNotFound => write!(
                formatter,
                "loxa-mlx executable not found; set {SERVER_ENV_VAR} or add loxa-mlx to PATH"
            ),
            Self::NotExecutable(path) => write!(
                formatter,
                "loxa-mlx path is not an executable file: {}",
                path.display()
            ),
            Self::VersionTimeout { pid } => write!(
                formatter,
                "loxa-mlx --version timed out (pid {pid} was reaped)"
            ),
            Self::VersionCommandFailed { status, stderr } => write!(
                formatter,
                "loxa-mlx --version failed with status {}: {stderr}",
                status
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            ),
            Self::VersionMismatch { expected, detected } => write!(
                formatter,
                "loxa-mlx version mismatch: expected {expected}, detected {detected}"
            ),
            Self::RandomToken(error) => write!(
                formatter,
                "could not generate an engine token from operating-system randomness: {error}"
            ),
            Self::Io(error) => write!(formatter, "io error: {error}"),
        }
    }
}

impl Error for SwiftMlxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RandomToken(error) | Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SwiftMlxError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn validate_current_platform() -> Result<(), SwiftMlxError> {
    validate_platform(env::consts::OS, env::consts::ARCH)
}

pub fn validate_platform(os: &str, arch: &str) -> Result<(), SwiftMlxError> {
    if os == "macos" && arch == "aarch64" {
        Ok(())
    } else {
        Err(SwiftMlxError::UnsupportedPlatform {
            os: os.to_string(),
            arch: arch.to_string(),
        })
    }
}

pub fn canonicalize_model_dir(path: &Path) -> Result<PathBuf, SwiftMlxError> {
    if !path.is_dir() {
        return Err(SwiftMlxError::ModelDirectory(path.to_path_buf()));
    }
    fs::canonicalize(path).map_err(SwiftMlxError::Io)
}

pub fn discover_server_from_environment() -> Result<PathBuf, SwiftMlxError> {
    discover_server(
        env::var_os(SERVER_ENV_VAR).as_deref(),
        env::var_os("PATH").as_deref(),
    )
}

pub fn discover_server(
    override_path: Option<&OsStr>,
    path: Option<&OsStr>,
) -> Result<PathBuf, SwiftMlxError> {
    if let Some(candidate) = override_path.map(PathBuf::from) {
        if is_executable_file(&candidate) {
            return Ok(candidate);
        }
    }

    find_on_path(SERVER_BINARY, path).ok_or(SwiftMlxError::ServerNotFound)
}

fn find_on_path(binary: &str, path: Option<&OsStr>) -> Option<PathBuf> {
    path.into_iter()
        .flat_map(env::split_paths)
        .map(|directory| directory.join(binary))
        .find(|candidate| is_executable_file(candidate))
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}

pub fn detect_version(program: &Path) -> Result<String, SwiftMlxError> {
    if !is_executable_file(program) {
        return Err(SwiftMlxError::NotExecutable(program.to_path_buf()));
    }
    let output = run_version_command(program, VERSION_TIMEOUT)?;
    validate_version_output(&output)
}

fn run_version_command(program: &Path, timeout: Duration) -> Result<String, SwiftMlxError> {
    let mut child = Command::new(program)
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
                return Err(SwiftMlxError::VersionCommandFailed {
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
            let kill_result = child.kill();
            child.wait()?;
            kill_result?;
            return Err(SwiftMlxError::VersionTimeout { pid });
        }

        thread::sleep(Duration::from_millis(25));
    }
}

pub fn validate_version_output(output: &str) -> Result<String, SwiftMlxError> {
    let detected = output.trim();
    if detected == REQUIRED_VERSION {
        Ok(detected.to_string())
    } else {
        Err(SwiftMlxError::VersionMismatch {
            expected: REQUIRED_VERSION.to_string(),
            detected: detected.to_string(),
        })
    }
}

pub fn generate_engine_token() -> Result<String, SwiftMlxError> {
    let mut bytes = [0_u8; 32];
    fs::File::open(RANDOM_DEVICE)
        .and_then(|mut random| random.read_exact(&mut bytes))
        .map_err(SwiftMlxError::RandomToken)?;

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(HEX[(byte >> 4) as usize] as char);
        token.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn file(path: &Path, contents: impl AsRef<[u8]>) {
        fs::write(path, contents).expect("write test file");
    }

    fn executable_file(path: &Path, contents: impl AsRef<[u8]>) {
        file(path, contents);
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(path)
                .expect("executable metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("set executable permissions");
        }
    }

    #[test]
    fn accepts_only_apple_silicon_macos() {
        assert!(validate_platform("macos", "aarch64").is_ok());
        assert!(validate_platform("linux", "aarch64").is_err());
        assert!(validate_platform("macos", "x86_64").is_err());
        assert!(validate_platform("windows", "x86_64").is_err());
    }

    #[test]
    fn current_platform_validation_matches_compile_target() {
        assert_eq!(
            validate_current_platform().is_ok(),
            cfg!(all(target_os = "macos", target_arch = "aarch64"))
        );
    }

    #[test]
    fn canonicalize_model_requires_a_directory_and_preserves_spaces() {
        let temp = TempDir::new().expect("temp dir");
        let missing = temp.path().join("missing");
        let regular_file = temp.path().join("model.txt");
        let model = temp.path().join("model with spaces");
        file(&regular_file, b"not a directory");
        fs::create_dir(&model).expect("create model directory");

        assert!(canonicalize_model_dir(&missing).is_err());
        assert!(canonicalize_model_dir(&regular_file).is_err());
        assert_eq!(
            canonicalize_model_dir(&model).expect("canonical model"),
            fs::canonicalize(&model).expect("expected canonical path")
        );
    }

    #[test]
    fn environment_override_name_is_exact() {
        assert_eq!(SERVER_ENV_VAR, "LOXA_SWIFT_MLX_SERVER");
    }

    #[test]
    fn server_discovery_prefers_executable_override_then_path() {
        let temp = TempDir::new().expect("temp dir");
        let environment_server = temp.path().join("environment loxa-mlx");
        let path_dir = temp.path().join("bin");
        let path_server = path_dir.join("loxa-mlx");
        fs::create_dir(&path_dir).expect("create PATH directory");
        executable_file(&environment_server, b"test");
        executable_file(&path_server, b"test");
        let path = std::env::join_paths([path_dir]).expect("join PATH");

        assert_eq!(
            discover_server(Some(environment_server.as_os_str()), Some(path.as_os_str()))
                .expect("environment discovery"),
            environment_server
        );
        assert_eq!(
            discover_server(Some(OsStr::new("/missing")), Some(path.as_os_str()))
                .expect("PATH discovery"),
            path_server
        );
    }

    #[cfg(unix)]
    #[test]
    fn server_discovery_rejects_directories_and_non_executable_files() {
        let temp = TempDir::new().expect("temp dir");
        let override_directory = temp.path().join("directory");
        let path_dir = temp.path().join("bin");
        let path_server = path_dir.join("loxa-mlx");
        fs::create_dir(&override_directory).expect("create override directory");
        fs::create_dir(&path_dir).expect("create PATH directory");
        file(&path_server, b"not executable");
        let path = std::env::join_paths([path_dir]).expect("join PATH");

        assert!(matches!(
            discover_server(Some(override_directory.as_os_str()), Some(path.as_os_str())),
            Err(SwiftMlxError::ServerNotFound)
        ));

        let mut permissions = fs::metadata(&path_server)
            .expect("server metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path_server, permissions).expect("make server executable");
        assert_eq!(
            discover_server(Some(override_directory.as_os_str()), Some(path.as_os_str()))
                .expect("valid PATH fallback"),
            path_server
        );
    }

    #[test]
    fn exact_version_validation_accepts_only_required_version() {
        assert_eq!(REQUIRED_VERSION, "0.1.0");
        assert_eq!(
            validate_version_output("  0.1.0\n").expect("required version"),
            REQUIRED_VERSION
        );
        for invalid in [
            "0.0.9",
            "v0.1.0",
            "loxa-mlx 0.1.0",
            "0.1.0 6.1.0",
            "0.1",
            "unknown",
            "",
        ] {
            assert!(
                validate_version_output(invalid).is_err(),
                "unexpectedly accepted {invalid:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn detect_version_executes_direct_bounded_version_argument() {
        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("loxa mlx version");
        executable_file(
            &version,
            b"#!/bin/sh\n[ \"$#\" -eq 1 ] && [ \"$1\" = \"--version\" ] || exit 9\nprintf '0.1.0\\n'\n",
        );

        assert_eq!(VERSION_TIMEOUT, Duration::from_secs(5));
        assert_eq!(detect_version(&version).expect("version output"), "0.1.0");
    }

    #[cfg(unix)]
    #[test]
    fn detect_version_rejects_non_executable_files_before_spawning() {
        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("loxa-mlx");
        file(&version, b"0.1.0\n");

        assert!(matches!(
            detect_version(&version),
            Err(SwiftMlxError::NotExecutable(path)) if path == version
        ));
    }

    #[cfg(unix)]
    #[test]
    fn nonzero_version_process_is_reaped_and_reports_status_and_stderr() {
        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("loxa-mlx");
        executable_file(
            &version,
            b"#!/bin/sh\nprintf '%s\\n' \"$$\" >&2\nprintf 'broken environment\\n' >&2\nexit 7\n",
        );

        let error = run_version_command(&version, Duration::from_secs(5))
            .expect_err("nonzero version command must fail");
        let SwiftMlxError::VersionCommandFailed {
            status: Some(7),
            stderr,
        } = error
        else {
            panic!("expected status 7 failure, got {error}");
        };
        assert!(stderr.ends_with("broken environment"));
        let pid = stderr
            .lines()
            .next()
            .expect("pid line")
            .parse::<u32>()
            .expect("numeric pid");
        assert_process_is_gone(pid);
    }

    #[cfg(unix)]
    #[test]
    fn slow_version_process_times_out_promptly_and_is_gone_after_reap() {
        let temp = TempDir::new().expect("temp dir");
        let version = temp.path().join("loxa-mlx");
        executable_file(&version, b"#!/bin/sh\nwhile :; do :; done\n");

        let started = Instant::now();
        let error = run_version_command(&version, Duration::from_millis(75))
            .expect_err("slow process must time out");
        assert!(started.elapsed() < Duration::from_secs(1));
        let SwiftMlxError::VersionTimeout { pid } = error else {
            panic!("expected timeout with reaped pid, got {error}");
        };
        assert_process_is_gone(pid);
    }

    #[cfg(unix)]
    fn assert_process_is_gone(pid: u32) {
        let status = Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("probe child pid");
        assert!(!status.success(), "version process {pid} survived");
    }

    #[test]
    fn engine_tokens_are_random_lowercase_hex_encoded_32_byte_values() {
        let first = generate_engine_token().expect("first engine token");
        let second = generate_engine_token().expect("second engine token");

        for token in [&first, &second] {
            assert_eq!(token.len(), 64);
            assert!(
                token
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "token was not lowercase hexadecimal"
            );
        }
        assert_ne!(first, second);
    }

    #[test]
    fn launch_arguments_are_exact_and_preserve_model_path_spaces() {
        let token = "ab".repeat(32);
        let model = PathBuf::from("/tmp/canonical model path");
        let config = SwiftMlxLaunchConfig {
            program: PathBuf::from("/tmp/loxa-mlx"),
            model: model.clone(),
            port: 8123,
            engine_token: token.clone(),
            engine_version: REQUIRED_VERSION.to_string(),
        };

        assert_eq!(
            config.args(),
            vec![
                OsString::from("serve"),
                OsString::from("--model"),
                model.into_os_string(),
                OsString::from("--host"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("8123"),
                OsString::from("--engine-token"),
                OsString::from(token),
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn launch_arguments_preserve_non_utf8_model_path() {
        let model = PathBuf::from(OsString::from_vec(vec![
            b'/', b't', b'm', b'p', b'/', b'm', b'o', b'd', b'e', b'l', 0x80,
        ]));
        let config = SwiftMlxLaunchConfig {
            program: PathBuf::from("/tmp/loxa-mlx"),
            model: model.clone(),
            port: 8123,
            engine_token: "cd".repeat(32),
            engine_version: REQUIRED_VERSION.to_string(),
        };

        assert_eq!(config.args()[2], model.into_os_string());
    }

    #[test]
    fn debug_output_redacts_engine_token() {
        let token = "ef".repeat(32);
        let config = SwiftMlxLaunchConfig {
            program: PathBuf::from("/tmp/loxa-mlx"),
            model: PathBuf::from("/tmp/model"),
            port: 8123,
            engine_token: token.clone(),
            engine_version: REQUIRED_VERSION.to_string(),
        };

        let rendered = format!("{config:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains(&token));
    }
}
