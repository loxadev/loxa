use super::{
    no_prepared_runtime_guard, spawn_reader_thread, ChildProcessGuard, ChildTerminationMode,
    LaunchProfile, PersistentSignalPolicy, PreparedRuntimeGuard, ValidatedManagedRuntime,
};
use crate::paths::AppPaths;
use crate::runtime_identity::RuntimeIdentity;
use std::ffi::OsStr;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub(super) const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const PREPARED_VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const VERSION_OUTPUT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_VERSION_OUTPUT: u64 = 4096;
pub fn discover_server(
    explicit: Option<&Path>,
    environment: Option<&OsStr>,
    managed: &Path,
    path: Option<&OsStr>,
) -> Result<PathBuf, String> {
    discover_server_with_requirement(
        explicit,
        environment,
        managed,
        path,
        None,
        RuntimeIdentity::LegacyCliB10121,
    )
}

pub(crate) fn discover_from_process(
    explicit: Option<&Path>,
    managed: &Path,
    profile: &LaunchProfile,
    runtime_identity: RuntimeIdentity,
) -> Result<PathBuf, String> {
    discover_server_with_requirement(
        explicit,
        std::env::var_os("LOXA_LLAMA_SERVER").as_deref(),
        managed,
        std::env::var_os("PATH").as_deref(),
        profile.required_version(runtime_identity),
        runtime_identity,
    )
}

pub(crate) fn validate_managed_server(
    path: &Path,
    runtime_identity: RuntimeIdentity,
) -> Result<PathBuf, String> {
    validate_managed_candidate(path, runtime_identity)?;
    Ok(path.to_path_buf())
}

pub fn validate_managed_runtime(paths: &AppPaths) -> Result<ValidatedManagedRuntime, String> {
    if paths.runtime_identity.is_bundled() {
        #[cfg(unix)]
        {
            let prepared = crate::runtime_bundle::prepare_embedded_runtime(paths)?;
            let runtime = ValidatedManagedRuntime::bundled(paths.managed_server.clone(), prepared);
            validate_prepared_managed_runtime(&runtime, paths.runtime_identity)?;
            return Ok(runtime);
        }
        #[cfg(not(unix))]
        return Err("bundled b10344 runtime is supported only on Unix".into());
    }
    validate_managed_server(&paths.managed_server, paths.runtime_identity)
        .map(ValidatedManagedRuntime::path)
}

fn validate_prepared_managed_runtime(
    runtime: &ValidatedManagedRuntime,
    runtime_identity: RuntimeIdentity,
) -> Result<(), String> {
    let first_line = probe_validated_version(runtime)
        .and_then(managed_version_first_line)
        .map_err(|error| {
            format!(
                "managed llama-server bundle is damaged at {}: {error}",
                runtime.source_server.display()
            )
        })?;
    let expected = runtime_identity.version_line();
    if first_line != expected {
        return Err(format!(
            "managed llama-server bundle is damaged at {}: expected --version first line {expected:?}, found {first_line:?}",
            runtime.source_server.display()
        ));
    }
    Ok(())
}

fn discover_server_with_requirement(
    explicit: Option<&Path>,
    environment: Option<&OsStr>,
    managed: &Path,
    path: Option<&OsStr>,
    required_version: Option<&str>,
    managed_identity: RuntimeIdentity,
) -> Result<PathBuf, String> {
    if let Some(server) = explicit {
        validate_candidate_with_requirement(server, "--server", required_version)?;
        return Ok(server.to_path_buf());
    }
    if let Some(server) = environment {
        let server = PathBuf::from(server);
        validate_candidate_with_requirement(&server, "LOXA_LLAMA_SERVER", required_version)?;
        return Ok(server);
    }
    match std::fs::symlink_metadata(managed) {
        Ok(_) => {
            validate_managed_candidate(managed, managed_identity)?;
            return Ok(managed.to_path_buf());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "managed llama-server bundle is damaged at {}: {error}",
                managed.display()
            ))
        }
    }
    if let Some(path) = path {
        for dir in std::env::split_paths(path) {
            let candidate = dir.join(executable_name());
            let valid = match required_version {
                Some(expected) => executable(&candidate) && version_matches(&candidate, expected),
                None => executable(&candidate) && probe_version(&candidate).is_ok(),
            };
            if valid {
                return Ok(candidate);
            }
        }
    }
    if let Some(expected) = required_version {
        return Err(format!(
            "qualified llama-server not found; expected --version first line {expected:?}"
        ));
    }
    Err("llama-server not found; install it with `brew install llama.cpp`".into())
}

fn validate_managed_candidate(
    path: &Path,
    runtime_identity: RuntimeIdentity,
) -> Result<(), String> {
    if !executable(path) {
        return Err(format!(
            "managed llama-server bundle is damaged at {}: runtime is not executable",
            path.display()
        ));
    }
    let first_line = probe_version(path)
        .and_then(managed_version_first_line)
        .map_err(|error| {
            format!(
                "managed llama-server bundle is damaged at {}: {error}",
                path.display()
            )
        })?;
    let expected = runtime_identity.version_line();
    if first_line != expected {
        return Err(format!(
            "managed llama-server bundle is damaged at {}: expected --version first line {expected:?}, found {first_line:?}",
            path.display()
        ));
    }
    Ok(())
}

fn validate_candidate_with_requirement(
    path: &Path,
    source: &str,
    required_version: Option<&str>,
) -> Result<(), String> {
    if let Some(expected) = required_version {
        return validate_exact_candidate(path, source, expected);
    }
    if !executable(path) {
        return Err(format!("{source} is not executable: {}", path.display()));
    }
    probe_version(path).map(|_| ()).map_err(|error| {
        format!(
            "{source} failed --version probe for {}: {error}",
            path.display()
        )
    })
}

fn validate_exact_candidate(path: &Path, source: &str, expected: &str) -> Result<(), String> {
    if !executable(path) {
        return Err(format!("{source} is not executable: {}", path.display()));
    }
    let first_line = probe_version(path)
        .and_then(managed_version_first_line)
        .map_err(|error| {
            format!(
                "{source} failed exact --version probe for {}: {error}",
                path.display()
            )
        })?;
    if first_line == expected {
        Ok(())
    } else {
        Err(format!(
            "{source} must report exact --version first line {expected:?}, found {first_line:?}"
        ))
    }
}

fn version_matches(path: &Path, expected: &str) -> bool {
    probe_version(path)
        .and_then(managed_version_first_line)
        .is_ok_and(|first_line| first_line == expected)
}

#[derive(Debug)]
pub(super) struct VersionProbeOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn probe_version(path: &Path) -> Result<VersionProbeOutput, String> {
    probe_version_with_timeout(path, VERSION_PROBE_TIMEOUT)
}

pub(super) fn probe_validated_version(
    runtime: &ValidatedManagedRuntime,
) -> Result<VersionProbeOutput, String> {
    probe_version_command(
        runtime.command(),
        PREPARED_VERSION_PROBE_TIMEOUT,
        runtime.process_guard(),
    )
}

#[cfg(all(test, target_os = "macos"))]
pub(super) fn probe_validated_version_with_timeout_for_test(
    runtime: &ValidatedManagedRuntime,
    timeout: Duration,
) -> Result<VersionProbeOutput, String> {
    probe_version_command(runtime.command(), timeout, runtime.process_guard())
}

pub(super) fn probe_version_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<VersionProbeOutput, String> {
    probe_version_command(Command::new(path), timeout, no_prepared_runtime_guard())
}

fn probe_version_command(
    mut command: Command,
    timeout: Duration,
    prepared: PreparedRuntimeGuard,
) -> Result<VersionProbeOutput, String> {
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = ChildProcessGuard::spawn(
        &mut command,
        PersistentSignalPolicy::CallerManaged,
        ChildTerminationMode::Immediate,
        prepared,
        None,
    )?;
    let stdout = child
        .child_mut()
        .stdout
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let stderr = child
        .child_mut()
        .stderr
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let (output_sender, output_receiver) = mpsc::sync_channel(2);
    let stdout_sender = output_sender.clone();
    let stdout_reader = spawn_reader_thread("loxa-version-stdout", move || {
        let mut output = Vec::new();
        let result = stdout
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = stdout_sender.send((false, result));
    })?;
    let stderr_reader = spawn_reader_thread("loxa-version-stderr", move || {
        let mut output = Vec::new();
        let result = stderr
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = output_sender.send((true, result));
    })?;

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child
            .child_mut()
            .try_wait()
            .map_err(|error| error.to_string())?
        {
            break status;
        }
        if Instant::now() >= deadline {
            if let Err(cleanup) = child.terminate() {
                return Err(format!(
                    "timed out after {} ms; cleanup failed: {cleanup}",
                    timeout.as_millis()
                ));
            }
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(format!("timed out after {} ms", timeout.as_millis()));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let output_deadline = Instant::now() + VERSION_OUTPUT_TIMEOUT;
    let mut stdout = None;
    let mut stderr = None;
    for _ in 0..2 {
        let remaining = output_deadline.saturating_duration_since(Instant::now());
        let (is_stderr, output) = match output_receiver.recv_timeout(remaining) {
            Ok(output) => output,
            Err(_) => {
                if let Err(cleanup) = child.terminate() {
                    return Err(format!(
                        "timed out reading --version output; cleanup failed: {cleanup}"
                    ));
                }
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!(
                    "timed out reading --version output after {} ms",
                    VERSION_OUTPUT_TIMEOUT.as_millis()
                ));
            }
        };
        if is_stderr {
            stderr = Some(output);
        } else {
            stdout = Some(output);
        }
    }
    stdout_reader
        .join()
        .map_err(|_| "--version stdout reader panicked".to_string())?;
    stderr_reader
        .join()
        .map_err(|_| "--version stderr reader panicked".to_string())?;
    if !status.success() {
        return Err(format!("{status}"));
    }
    Ok(VersionProbeOutput {
        stdout: stdout.transpose()?.unwrap_or_default(),
        stderr: stderr.transpose()?.unwrap_or_default(),
    })
}

pub(super) fn managed_version_first_line(output: VersionProbeOutput) -> Result<String, String> {
    match (output.stdout.is_empty(), output.stderr.is_empty()) {
        (false, true) => Ok(first_line(&output.stdout)),
        (true, false) => Ok(first_line(&output.stderr)),
        (true, true) => Err("--version produced no output".into()),
        (false, false) => {
            Err("--version output is ambiguous: wrote to both stdout and stderr".into())
        }
    }
}

fn first_line(output: &[u8]) -> String {
    String::from_utf8_lossy(output)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn executable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn executable_name() -> &'static str {
    if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    }
}
