use reqwest::blocking::Client;
use serde::Deserialize;
use std::ffi::{OsStr, OsString};
use std::io::Read as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const VERSION_OUTPUT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_VERSION_OUTPUT: u64 = 4096;
const MANAGED_VERSION: &str = "version: 10121 (555881ebc)";

pub fn discover_server(
    explicit: Option<&Path>,
    environment: Option<&OsStr>,
    managed: &Path,
    path: Option<&OsStr>,
) -> Result<PathBuf, String> {
    if let Some(server) = explicit {
        validate_candidate(server, "--server")?;
        return Ok(server.to_path_buf());
    }
    if let Some(server) = environment {
        let server = PathBuf::from(server);
        validate_candidate(&server, "LOXA_LLAMA_SERVER")?;
        return Ok(server);
    }
    match std::fs::symlink_metadata(managed) {
        Ok(_) => {
            if !executable(managed) {
                return Err(format!(
                    "managed llama-server bundle is damaged at {}: runtime is not executable",
                    managed.display()
                ));
            }
            let first_line = probe_version(managed)
                .and_then(managed_version_first_line)
                .map_err(|error| {
                    format!(
                        "managed llama-server bundle is damaged at {}: {error}",
                        managed.display()
                    )
                })?;
            if first_line != MANAGED_VERSION {
                return Err(format!(
                    "managed llama-server bundle is damaged at {}: expected --version first line {MANAGED_VERSION:?}, found {first_line:?}",
                    managed.display()
                ));
            }
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
            if executable(&candidate) && probe_version(&candidate).is_ok() {
                return Ok(candidate);
            }
        }
    }
    Err("llama-server not found; install it with `brew install llama.cpp`".into())
}

pub fn discover_from_process(explicit: Option<&Path>, managed: &Path) -> Result<PathBuf, String> {
    discover_server(
        explicit,
        std::env::var_os("LOXA_LLAMA_SERVER").as_deref(),
        managed,
        std::env::var_os("PATH").as_deref(),
    )
}

fn validate_candidate(path: &Path, source: &str) -> Result<(), String> {
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

#[derive(Debug)]
struct VersionProbeOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn probe_version(path: &Path) -> Result<VersionProbeOutput, String> {
    probe_version_with_timeout(path, VERSION_PROBE_TIMEOUT)
}

fn probe_version_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<VersionProbeOutput, String> {
    let mut child = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let (sender, receiver) = mpsc::sync_channel(2);
    let stdout_sender = sender.clone();
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = stdout
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = stdout_sender.send((false, result));
    });
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = stderr
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = sender.send((true, result));
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("timed out after {} ms", timeout.as_millis()));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if !status.success() {
        return Err(format!("{status}"));
    }
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    for _ in 0..2 {
        let (is_stderr, output) = receiver
            .recv_timeout(VERSION_OUTPUT_TIMEOUT)
            .map_err(|_| "timed out reading --version output".to_string())?;
        if is_stderr {
            stderr = output?;
        } else {
            stdout = output?;
        }
    }
    Ok(VersionProbeOutput { stdout, stderr })
}

fn managed_version_first_line(output: VersionProbeOutput) -> Result<String, String> {
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

pub fn build_args(model: &Path, id: &str, port: u16, ctx: u32) -> Vec<OsString> {
    vec![
        "--model".into(),
        model.as_os_str().to_owned(),
        "--alias".into(),
        id.into(),
        "--host".into(),
        "127.0.0.1".into(),
        "--port".into(),
        port.to_string().into(),
        "--ctx-size".into(),
        ctx.to_string().into(),
        "--n-gpu-layers".into(),
        "99".into(),
    ]
}

pub fn run(
    server: &Path,
    model: &Path,
    id: &str,
    requested_port: u16,
    ctx: u32,
) -> Result<i32, String> {
    let reservation =
        TcpListener::bind(("127.0.0.1", requested_port)).map_err(|error| error.to_string())?;
    let port = reservation
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    let args = build_args(model, id, port, ctx);
    let _signal_guard = install_signal_handlers()?;
    let mut command = Command::new(server);
    command
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setpgid is async-signal-safe and the closure performs no allocation.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
    }
    drop(reservation);
    let mut child = OwnedChild::new(command.spawn().map_err(|error| error.to_string())?);
    let startup_deadline = Instant::now() + STARTUP_TIMEOUT;
    let client = readiness_client()?;
    loop {
        if let Some(status) = child
            .child_mut()
            .try_wait()
            .map_err(|error| error.to_string())?
        {
            let code = exit_code(status);
            child.terminate()?;
            return Ok(code);
        }
        if let Some(signal) = received_signal() {
            child.terminate()?;
            return Ok(128 + signal);
        }
        if readiness(&client, port, id) {
            println!("ready: http://127.0.0.1:{port} (model {id})");
            break;
        }
        if Instant::now() >= startup_deadline {
            child.terminate()?;
            return Err("llama-server did not become ready within 120 seconds".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    loop {
        if let Some(status) = child
            .child_mut()
            .try_wait()
            .map_err(|error| error.to_string())?
        {
            let code = exit_code(status);
            child.terminate()?;
            return Ok(code);
        }
        if let Some(signal) = received_signal() {
            child.terminate()?;
            return Ok(128 + signal);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn readiness_client() -> Result<Client, String> {
    Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(250))
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|error| error.to_string())
}

fn readiness(client: &Client, port: u16, id: &str) -> bool {
    client
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
        .ok()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.text().ok())
        .is_some_and(|body| models_body_has_alias(&body, id))
}

#[derive(Deserialize)]
struct Models {
    data: Vec<Model>,
}

#[derive(Deserialize)]
struct Model {
    id: String,
}

pub fn models_body_has_alias(body: &str, id: &str) -> bool {
    serde_json::from_str::<Models>(body)
        .is_ok_and(|models| models.data.iter().any(|model| model.id == id))
}

#[cfg(unix)]
struct SignalGuard {
    previous_int: libc::sighandler_t,
    previous_term: libc::sighandler_t,
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        // SAFETY: restores both handler values returned by signal.
        unsafe {
            libc::signal(libc::SIGTERM, self.previous_term);
            libc::signal(libc::SIGINT, self.previous_int);
        }
    }
}

#[cfg(not(unix))]
struct SignalGuard;

fn install_signal_handlers() -> Result<SignalGuard, String> {
    RECEIVED_SIGNAL.store(0, Ordering::SeqCst);
    #[cfg(unix)]
    {
        extern "C" fn handle(signal: libc::c_int) {
            RECEIVED_SIGNAL.store(signal, Ordering::SeqCst);
        }
        // SAFETY: handler only stores to a lock-free atomic and has static lifetime.
        let handler = handle as *const () as libc::sighandler_t;
        let previous_int = unsafe { libc::signal(libc::SIGINT, handler) };
        if previous_int == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error().to_string());
        }
        // SAFETY: handler only stores to a lock-free atomic and has static lifetime.
        let previous_term = unsafe { libc::signal(libc::SIGTERM, handler) };
        if previous_term == libc::SIG_ERR {
            let error = std::io::Error::last_os_error().to_string();
            // SAFETY: restores the SIGINT handler installed immediately above.
            unsafe {
                libc::signal(libc::SIGINT, previous_int);
            }
            return Err(error);
        }
        Ok(SignalGuard {
            previous_int,
            previous_term,
        })
    }
    #[cfg(not(unix))]
    {
        Ok(SignalGuard)
    }
}

fn received_signal() -> Option<i32> {
    match RECEIVED_SIGNAL.load(Ordering::SeqCst) {
        0 => None,
        signal => Some(signal),
    }
}

#[cfg(unix)]
fn terminate_owned(child: &mut Child) -> Result<(), String> {
    let group = i32::try_from(child.id()).map_err(|_| "invalid child process id")?;
    // SAFETY: the negative PID targets only the process group created for this child.
    unsafe {
        libc::kill(-group, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_exists(group)? {
            child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // SAFETY: the negative PID targets only the exact owned process group.
    unsafe {
        libc::kill(-group, libc::SIGKILL);
    }
    child.wait().map_err(|error| error.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !process_group_exists(group)? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err("owned process group survived SIGKILL".into())
}

#[cfg(unix)]
fn process_group_exists(group: i32) -> Result<bool, String> {
    // SAFETY: signal 0 probes existence without delivering a signal.
    let result = unsafe { libc::kill(-group, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error.to_string()),
    }
}

#[cfg(not(unix))]
fn terminate_owned(child: &mut Child) -> Result<(), String> {
    child.kill().map_err(|error| error.to_string())?;
    child.wait().map_err(|error| error.to_string())?;
    Ok(())
}

struct OwnedChild {
    child: Option<Child>,
}

impl OwnedChild {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("owned child is present")
    }

    fn terminate(&mut self) -> Result<(), String> {
        if let Some(mut child) = self.child.take() {
            terminate_owned(&mut child)
        } else {
            Ok(())
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        128 + status.signal().unwrap_or(1)
    }
    #[cfg(not(unix))]
    {
        1
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::io::{Read, Write};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[cfg(unix)]
    static RUN_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(unix)]
    struct TestSignalGuard {
        previous_int: libc::sighandler_t,
        previous_term: libc::sighandler_t,
    }

    #[cfg(unix)]
    impl TestSignalGuard {
        fn install() -> Self {
            extern "C" fn keep_test_process_safe(_: libc::c_int) {}

            let handler = keep_test_process_safe as *const () as libc::sighandler_t;
            // SAFETY: the no-op handler has static lifetime and is restored by this guard.
            let previous_int = unsafe { libc::signal(libc::SIGINT, handler) };
            assert_ne!(previous_int, libc::SIG_ERR);
            // SAFETY: the no-op handler has static lifetime and is restored by this guard.
            let previous_term = unsafe { libc::signal(libc::SIGTERM, handler) };
            if previous_term == libc::SIG_ERR {
                // SAFETY: restores the handler value returned above for SIGINT.
                unsafe {
                    libc::signal(libc::SIGINT, previous_int);
                }
                panic!("failed to install test SIGTERM handler");
            }
            Self {
                previous_int,
                previous_term,
            }
        }
    }

    #[cfg(unix)]
    impl Drop for TestSignalGuard {
        fn drop(&mut self) {
            // SAFETY: restores the exact handler values replaced by this guard.
            unsafe {
                libc::signal(libc::SIGTERM, self.previous_term);
                libc::signal(libc::SIGINT, self.previous_int);
            }
        }
    }

    #[cfg(unix)]
    fn write_executable_script(path: &Path, script: &[u8]) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, script).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    fn write_version_script(path: &Path, first_line: &str, exit_code: i32) {
        assert!(!first_line.contains('\''));
        write_executable_script(
            path,
            format!("#!/bin/sh\nprintf '%s\\n' '{first_line}' >&2\nexit {exit_code}\n").as_bytes(),
        );
    }

    #[cfg(unix)]
    fn write_dual_stream_version_script(path: &Path) {
        write_executable_script(
            path,
            b"#!/bin/sh\nprintf '%s\\n' 'version: usable'\nprintf '%s\\n' 'harmless warning' >&2\n",
        );
    }

    #[cfg(unix)]
    fn run_with_signal(signal: libc::c_int) -> i32 {
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let started = dir.path().join("started");
        write_executable_script(
            &server,
            b"#!/bin/sh\nprintf ready > \"$2\"\nsleep 1\nexit 7\n",
        );
        let marker = started.clone();
        let sender = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !marker.exists() && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(marker.exists(), "server did not start");
            // SAFETY: TestSignalGuard and the production guard keep this process safe.
            assert_eq!(unsafe { libc::kill(libc::getpid(), signal) }, 0);
        });

        let code = run(&server, &started, "demo", 0, 1);
        sender.join().unwrap();
        code.unwrap()
    }

    #[test]
    fn argv_and_readiness_are_exact_and_generic() {
        let args = build_args(
            std::path::Path::new("/models/model.gguf"),
            "demo",
            1234,
            8192,
        );
        assert_eq!(
            args,
            [
                "--model",
                "/models/model.gguf",
                "--alias",
                "demo",
                "--host",
                "127.0.0.1",
                "--port",
                "1234",
                "--ctx-size",
                "8192",
                "--n-gpu-layers",
                "99"
            ]
            .map(OsStr::new)
        );
        assert!(models_body_has_alias(r#"{"data":[{"id":"demo"}]}"#, "demo"));
        assert!(!models_body_has_alias(
            r#"{"data":[{"id":"demo-extra"}]}"#,
            "demo"
        ));
        assert_eq!(STARTUP_TIMEOUT, Duration::from_secs(120));
    }

    #[cfg(unix)]
    #[test]
    fn discovery_uses_explicit_then_environment_then_managed_then_path() {
        let dir = tempdir().unwrap();
        let explicit = dir.path().join("explicit");
        let environment = dir.path().join("environment");
        let managed = dir.path().join("managed");
        let path_dir = dir.path().join("path");
        std::fs::create_dir(&path_dir).unwrap();
        let path_server = path_dir.join("llama-server");
        write_version_script(&explicit, "explicit", 0);
        write_version_script(&environment, "environment", 0);
        write_version_script(&managed, "version: 10121 (555881ebc)", 0);
        write_version_script(&path_server, "path", 0);
        let search_path = std::env::join_paths([&path_dir]).unwrap();

        assert_eq!(
            discover_server(
                Some(&explicit),
                Some(environment.as_os_str()),
                &managed,
                Some(search_path.as_os_str()),
            )
            .unwrap(),
            explicit
        );
        assert_eq!(
            discover_server(
                None,
                Some(environment.as_os_str()),
                &managed,
                Some(search_path.as_os_str()),
            )
            .unwrap(),
            environment
        );
        assert_eq!(
            discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
            managed
        );
        std::fs::remove_file(&managed).unwrap();
        assert_eq!(
            discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
            path_server
        );
    }

    #[cfg(unix)]
    #[test]
    fn successful_dual_stream_explicit_candidate_is_accepted() {
        let dir = tempdir().unwrap();
        let explicit = dir.path().join("explicit");
        let managed = dir.path().join("missing-managed");
        write_dual_stream_version_script(&explicit);

        assert_eq!(
            discover_server(Some(&explicit), None, &managed, None).unwrap(),
            explicit
        );
    }

    #[cfg(unix)]
    #[test]
    fn successful_dual_stream_environment_candidate_is_accepted() {
        let dir = tempdir().unwrap();
        let environment = dir.path().join("environment");
        let managed = dir.path().join("missing-managed");
        write_dual_stream_version_script(&environment);

        assert_eq!(
            discover_server(None, Some(environment.as_os_str()), &managed, None).unwrap(),
            environment
        );
    }

    #[cfg(unix)]
    #[test]
    fn successful_dual_stream_path_candidate_is_accepted() {
        let dir = tempdir().unwrap();
        let managed = dir.path().join("missing-managed");
        let path_server = dir.path().join("llama-server");
        write_dual_stream_version_script(&path_server);
        let search_path = std::env::join_paths([dir.path()]).unwrap();

        assert_eq!(
            discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
            path_server
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_and_environment_candidates_fail_immediately_when_invalid() {
        let dir = tempdir().unwrap();
        let missing_explicit = dir.path().join("missing-explicit");
        let missing_environment = dir.path().join("missing-environment");
        let failed_explicit = dir.path().join("failed-explicit");
        let failed_environment = dir.path().join("failed-environment");
        let managed = dir.path().join("managed");
        write_version_script(&failed_explicit, "explicit", 1);
        write_version_script(&failed_environment, "environment", 1);
        write_version_script(&managed, "version: 10121 (555881ebc)", 0);

        let explicit_error =
            discover_server(Some(&missing_explicit), None, &managed, None).unwrap_err();
        assert!(explicit_error.contains("--server"), "{explicit_error}");
        let explicit_error =
            discover_server(Some(&failed_explicit), None, &managed, None).unwrap_err();
        assert!(explicit_error.contains("--server"), "{explicit_error}");

        let environment_error =
            discover_server(None, Some(missing_environment.as_os_str()), &managed, None)
                .unwrap_err();
        assert!(
            environment_error.contains("LOXA_LLAMA_SERVER"),
            "{environment_error}"
        );
        let environment_error =
            discover_server(None, Some(failed_environment.as_os_str()), &managed, None)
                .unwrap_err();
        assert!(
            environment_error.contains("LOXA_LLAMA_SERVER"),
            "{environment_error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn managed_runtime_requires_exact_identity_and_reports_damage() {
        let dir = tempdir().unwrap();
        let managed = dir.path().join("managed");
        std::fs::write(&managed, b"not executable").unwrap();

        let error = discover_server(None, None, &managed, None).unwrap_err();
        assert!(
            error.contains("managed llama-server bundle is damaged"),
            "{error}"
        );
        assert!(error.contains("not executable"), "{error}");

        write_version_script(&managed, "version: 10090 (not-the-managed-build)", 0);
        let error = discover_server(None, None, &managed, None).unwrap_err();
        assert!(
            error.contains("managed llama-server bundle is damaged"),
            "{error}"
        );
        assert!(error.contains("version: 10121 (555881ebc)"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn managed_runtime_rejects_ambiguous_dual_stream_identity() {
        let dir = tempdir().unwrap();
        let managed = dir.path().join("managed");
        write_executable_script(
            &managed,
            b"#!/bin/sh\nprintf '%s\\n' 'version: 10090 (wrong)' \nprintf '%s\\n' 'version: 10121 (555881ebc)' >&2\n",
        );

        let error = discover_server(None, None, &managed, None).unwrap_err();

        assert!(
            error.contains("managed llama-server bundle is damaged"),
            "{error}"
        );
        assert!(error.contains("ambiguous"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn version_probes_are_bounded() {
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        write_executable_script(&server, b"#!/bin/sh\nwhile :; do :; done\n");
        let started = Instant::now();

        let error = probe_version_with_timeout(&server, Duration::from_millis(50)).unwrap_err();

        assert!(error.contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[test]
    fn missing_runtime_recommends_homebrew_without_environment_setup() {
        let dir = tempdir().unwrap();
        let managed = dir.path().join("missing-managed");

        let error = discover_server(None, None, &managed, None).unwrap_err();

        assert!(error.contains("brew install llama.cpp"), "{error}");
        assert!(!error.contains("LOXA_LLAMA_SERVER"), "{error}");
    }

    #[test]
    fn readiness_rejects_a_redirect_to_an_alias_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let redirected = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server_redirected = Arc::clone(&redirected);
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1];
            first.read_exact(&mut request).unwrap();
            first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            drop(first);

            listener.set_nonblocking(true).unwrap();
            while !server_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut second, _)) => {
                        server_redirected.store(true, Ordering::SeqCst);
                        second.set_nonblocking(false).unwrap();
                        second.read_exact(&mut request).unwrap();
                        let body = r#"{"data":[{"id":"demo"}]}"#;
                        write!(
                            second,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .unwrap();
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("redirect server failed: {error}"),
                }
            }
        });

        let ready = readiness(&readiness_client().unwrap(), port, "demo");
        stop.store(true, Ordering::SeqCst);
        server.join().unwrap();

        assert!(!ready, "readiness followed a loopback redirect");
        assert!(!redirected.load(Ordering::SeqCst));
    }

    #[cfg(unix)]
    #[test]
    fn installed_handlers_map_sigterm_and_sigint_to_shell_exit_codes() {
        let _lock = RUN_TEST_LOCK.lock().unwrap();
        let _test_signal_guard = TestSignalGuard::install();

        let term = run_with_signal(libc::SIGTERM);
        let interrupt = run_with_signal(libc::SIGINT);

        assert_eq!((term, interrupt), (143, 130));
    }

    #[cfg(unix)]
    #[test]
    fn leader_exit_cleans_surviving_process_group_before_returning_status() {
        let _lock = RUN_TEST_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let child_ready = dir.path().join("child-ready");
        let group_file = dir.path().join("group");
        write_executable_script(
            &server,
            b"#!/bin/sh\n(\n  trap '' TERM\n  printf ready > \"$2\"\n  while :; do sleep 1; done\n) &\nwhile [ ! -f \"$2\" ]; do :; done\nprintf '%s\\n' \"$$\" > \"$4\"\nexit 7\n",
        );

        let status = run(&server, &child_ready, group_file.to_str().unwrap(), 0, 1).unwrap();
        let group = std::fs::read_to_string(&group_file)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        let survived = process_group_exists(group).unwrap();
        if survived {
            // SAFETY: the script recorded the exact process group created by run.
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }

        assert_eq!(status, 7);
        assert!(!survived, "leader status returned while its group survived");
    }

    #[cfg(unix)]
    #[test]
    fn teardown_waits_for_the_exact_process_group_to_disappear() {
        use std::os::unix::process::CommandExt;
        let dir = tempdir().unwrap();
        let ready = dir.path().join("ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("(trap '' TERM; echo ready > \"$1\"; while :; do sleep 1; done) & wait")
            .arg("sh")
            .arg(&ready);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let mut child = command.spawn().unwrap();
        let pgid = i32::try_from(child.id()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready.exists() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(ready.exists());
        terminate_owned(&mut child).unwrap();
        assert!(!process_group_exists(pgid).unwrap());
    }
}
