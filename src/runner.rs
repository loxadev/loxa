use crate::ui;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const VERSION_OUTPUT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_VERSION_OUTPUT: u64 = 4096;
const MAX_MODELS_BODY: usize = 1024 * 1024;
const MAX_DIAGNOSTIC_TAIL: usize = 4096;
const MAX_ANNOUNCEMENT_LINE: usize = 8192;
const MAX_PENDING_ANNOUNCEMENTS: usize = 64;
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
    let mut command = Command::new(path);
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
    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let group = i32::try_from(child.id()).map_err(|_| "invalid child process id".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture --version output".to_string())?;
    let (output_sender, output_receiver) = mpsc::sync_channel(2);
    let stdout_sender = output_sender.clone();
    let stdout_reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = stdout
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = stdout_sender.send((false, result));
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = stderr
            .take(MAX_VERSION_OUTPUT)
            .read_to_end(&mut output)
            .map(|_| output)
            .map_err(|error| error.to_string());
        let _ = output_sender.send((true, result));
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            if let Err(cleanup) = terminate_probe(&mut child, group) {
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
                if let Err(cleanup) = terminate_probe(&mut child, group) {
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

#[cfg(unix)]
fn terminate_probe(child: &mut Child, group: i32) -> Result<(), String> {
    signal_process_group(group, libc::SIGKILL)?;
    let _ = child.wait().map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(not(unix))]
fn terminate_probe(child: &mut Child, _group: i32) -> Result<(), String> {
    let _ = child.kill();
    let _ = child.wait().map_err(|error| error.to_string())?;
    Ok(())
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
        "--jinja".into(),
        "--reasoning".into(),
        "off".into(),
    ]
}

fn resolve_requested_port(requested: u16) -> Result<u16, String> {
    if requested != 0 {
        return Ok(requested);
    }
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("failed to choose a local port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("failed to read the selected local port: {error}"))
}

pub fn run(
    server: &Path,
    model: &Path,
    id: &str,
    requested_port: u16,
    ctx: u32,
) -> Result<i32, String> {
    let starting = ui::spinner(format!("Starting {id}"));
    let started = start_foreground(server, model, id, requested_port, ctx);
    starting.finish_and_clear();
    let mut server = match started? {
        ForegroundStart::Ready(server) => server,
        ForegroundStart::Stopped(exit) => return Ok(report_exit(exit)),
    };
    let success = ui::success();
    let accent = ui::accent();
    let muted = ui::muted();
    anstream::println!(
        "{success}Ready{success:#} {accent}http://127.0.0.1:{}{accent:#} {muted}(model {id}){muted:#}",
        server.port()
    );
    loop {
        if let Some(exit) = server.poll()? {
            return Ok(report_exit(exit));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) enum ForegroundStart {
    Ready(ForegroundServer),
    Stopped(ServerExit),
}

pub(crate) struct ForegroundServer {
    server: OwnedServer,
    signal: &'static AtomicUsize,
}

pub(crate) fn start_foreground(
    server: &Path,
    model: &Path,
    id: &str,
    requested_port: u16,
    ctx: u32,
) -> Result<ForegroundStart, String> {
    let signal = foreground_signal_flag()?;
    match OwnedServer::start(
        server,
        model,
        id,
        requested_port,
        ctx,
        STARTUP_TIMEOUT,
        || received_signal(signal),
    )? {
        StartOutcome::Ready(server) => {
            Ok(ForegroundStart::Ready(ForegroundServer { server, signal }))
        }
        StartOutcome::Exited(exit) => Ok(ForegroundStart::Stopped(exit)),
        StartOutcome::Signaled(signal) => Ok(ForegroundStart::Stopped(ServerExit {
            code: 128 + signal,
            diagnostic: None,
        })),
    }
}

impl ForegroundServer {
    pub(crate) fn port(&self) -> u16 {
        self.server.port()
    }

    /// Polls for child exit first, then a foreground signal. Cleanup is complete
    /// before a status is returned.
    pub(crate) fn poll(&mut self) -> Result<Option<ServerExit>, String> {
        if let Some(exit) = self.server.try_wait()? {
            return Ok(Some(exit));
        }
        if let Some(signal) = received_signal(self.signal) {
            self.server.terminate()?;
            return Ok(Some(ServerExit {
                code: 128 + signal,
                diagnostic: None,
            }));
        }
        Ok(None)
    }

    pub(crate) fn terminate(&mut self) -> Result<(), String> {
        self.server.terminate()
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

fn readiness(client: &Client, port: u16, id: &str) -> Result<bool, String> {
    let response = match client
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
    {
        Ok(response) => response,
        Err(_) => return Ok(false),
    };
    if !response.status().is_success() {
        return Ok(false);
    }
    models_reader_has_alias(response, id)
}

fn validate_announcement_line(line: &str) -> Result<u16, String> {
    if !line.contains("listening") {
        return Err("line is not a listening announcement".into());
    }
    let candidates = line
        .split_ascii_whitespace()
        .map(|part| {
            part.trim_matches(|character: char| {
                matches!(character, ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}')
            })
        })
        .filter(|part| part.contains("://"))
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Err(format!(
            "listening announcement must contain exactly one URL, found {}",
            candidates.len()
        ));
    }
    let url = reqwest::Url::parse(candidates[0])
        .map_err(|error| format!("invalid listening URL: {error}"))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("listening URL must be an exact loopback HTTP endpoint".into());
    }
    let port = url
        .port()
        .ok_or_else(|| "listening URL must include an explicit port".to_string())?;
    if port == 0 {
        return Err("listening URL port must be nonzero".into());
    }
    Ok(port)
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

fn models_reader_has_alias(mut reader: impl std::io::Read, id: &str) -> Result<bool, String> {
    let mut body = Vec::new();
    reader
        .by_ref()
        .take((MAX_MODELS_BODY + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|error| error.to_string())?;
    if body.len() > MAX_MODELS_BODY {
        return Err(format!(
            "/v1/models response is too large (limit {MAX_MODELS_BODY} bytes)"
        ));
    }
    let body = std::str::from_utf8(&body).map_err(|error| error.to_string())?;
    Ok(models_body_has_alias(body, id))
}

#[cfg(unix)]
static PROCESS_SIGNAL_FLAG: OnceLock<Result<Arc<AtomicUsize>, String>> = OnceLock::new();

#[cfg(unix)]
fn foreground_signal_flag() -> Result<&'static AtomicUsize, String> {
    let registration = PROCESS_SIGNAL_FLAG.get_or_init(|| {
        let received = Arc::new(AtomicUsize::new(0));
        signal_hook::flag::register_usize(
            libc::SIGINT,
            Arc::clone(&received),
            libc::SIGINT as usize,
        )
        .map_err(|error| error.to_string())?;
        signal_hook::flag::register_usize(
            libc::SIGTERM,
            Arc::clone(&received),
            libc::SIGTERM as usize,
        )
        .map_err(|error| error.to_string())?;
        Ok(received)
    });
    let signal = registration.as_ref().map_err(Clone::clone)?;
    signal.store(0, Ordering::SeqCst);
    Ok(signal)
}

#[cfg(unix)]
fn received_signal(signal: &AtomicUsize) -> Option<i32> {
    match signal.load(Ordering::SeqCst) {
        0 => None,
        signal => i32::try_from(signal).ok(),
    }
}

#[cfg(not(unix))]
static PROCESS_SIGNAL_FLAG: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(unix))]
fn foreground_signal_flag() -> Result<&'static AtomicUsize, String> {
    PROCESS_SIGNAL_FLAG.store(0, Ordering::SeqCst);
    Ok(&PROCESS_SIGNAL_FLAG)
}

#[cfg(not(unix))]
fn received_signal(_signal: &AtomicUsize) -> Option<i32> {
    None
}

type AnnouncementOutput = (mpsc::SyncSender<Result<u16, String>>, Arc<AtomicBool>);

fn spawn_output_reader<R>(
    mut reader: R,
    announcement_output: Option<AnnouncementOutput>,
) -> std::thread::JoinHandle<Result<Vec<u8>, String>>
where
    R: std::io::Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut diagnostic_tail = VecDeque::with_capacity(MAX_DIAGNOSTIC_TAIL);
        let mut line = Vec::new();
        let mut line_too_long = false;
        let mut buffer = [0_u8; 4096];
        loop {
            let count = reader
                .read(&mut buffer)
                .map_err(|error| error.to_string())?;
            if count == 0 {
                if let Some((announcements, overflow)) = &announcement_output {
                    if !line.is_empty() || line_too_long {
                        publish_announcement(&line, line_too_long, announcements, overflow);
                    }
                }
                return Ok(diagnostic_tail.into_iter().collect());
            }
            for &byte in &buffer[..count] {
                if diagnostic_tail.len() == MAX_DIAGNOSTIC_TAIL {
                    diagnostic_tail.pop_front();
                }
                diagnostic_tail.push_back(byte);
                if let Some((announcements, overflow)) = &announcement_output {
                    if byte == b'\n' {
                        publish_announcement(&line, line_too_long, announcements, overflow);
                        line.clear();
                        line_too_long = false;
                    } else if line.len() < MAX_ANNOUNCEMENT_LINE {
                        line.push(byte);
                    } else {
                        line_too_long = true;
                    }
                }
            }
        }
    })
}

fn publish_announcement(
    line: &[u8],
    line_too_long: bool,
    announcements: &mpsc::SyncSender<Result<u16, String>>,
    overflow: &AtomicBool,
) {
    let line = String::from_utf8_lossy(line);
    if !line.contains("listening") {
        return;
    }
    let announcement = if line_too_long {
        Err(format!(
            "listening announcement exceeds {MAX_ANNOUNCEMENT_LINE} bytes"
        ))
    } else {
        validate_announcement_line(&line)
    };
    if let Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) =
        announcements.try_send(announcement)
    {
        overflow.store(true, Ordering::SeqCst);
    }
}

enum StartOutcome {
    Ready(OwnedServer),
    Exited(ServerExit),
    Signaled(i32),
}

#[derive(Debug)]
pub(crate) struct ServerExit {
    code: i32,
    diagnostic: Option<String>,
}

pub(crate) fn report_exit(exit: ServerExit) -> i32 {
    if let Some(diagnostic) = exit.diagnostic {
        eprintln!("{diagnostic}");
    }
    exit.code
}

pub struct OwnedServer {
    child: Option<Child>,
    group: i32,
    port: u16,
    announcements: mpsc::Receiver<Result<u16, String>>,
    announcement_overflow: Arc<AtomicBool>,
    announced_port: Option<u16>,
    stdout_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stdout_tail: Vec<u8>,
    stderr_tail: Vec<u8>,
}

impl OwnedServer {
    #[allow(clippy::too_many_arguments)]
    fn start<F>(
        server: &Path,
        model: &Path,
        id: &str,
        requested_port: u16,
        ctx: u32,
        timeout: Duration,
        signal: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<i32>,
    {
        let requested_port = resolve_requested_port(requested_port)?;
        let client = readiness_client()?;
        let mut command = Command::new(server);
        command
            .args(build_args(model, id, requested_port, ctx))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let group = i32::try_from(child.id()).map_err(|_| {
            let _ = child.kill();
            let _ = child.wait();
            "invalid child process id".to_string()
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "failed to capture llama-server stdout".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "failed to capture llama-server stderr".to_string())?;
        let (announcement_sender, announcements) = mpsc::sync_channel(MAX_PENDING_ANNOUNCEMENTS);
        let announcement_overflow = Arc::new(AtomicBool::new(false));
        let stdout_reader = spawn_output_reader(stdout, None);
        let stderr_reader = spawn_output_reader(
            stderr,
            Some((announcement_sender, Arc::clone(&announcement_overflow))),
        );
        let mut owned = Self {
            child: Some(child),
            group,
            port: 0,
            announcements,
            announcement_overflow,
            announced_port: None,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
        };
        let deadline = Instant::now() + timeout;
        loop {
            if let Err(error) = owned.collect_announcements() {
                return owned.fail_start(error);
            }
            if let Some(signal) = signal() {
                owned.terminate()?;
                return Ok(StartOutcome::Signaled(signal));
            }
            if let Some(status) = owned
                .child_mut()
                .try_wait()
                .map_err(|error| error.to_string())?
            {
                let code = exit_code(status);
                owned.terminate()?;
                return Ok(StartOutcome::Exited(owned.server_exit(code)));
            }
            if let Some(port) = owned.announced_port {
                if requested_port != 0 && port != requested_port {
                    return owned.fail_start(format!(
                        "llama-server announced port {port}, expected {requested_port}"
                    ));
                }
                match readiness(&client, port, id) {
                    Ok(true) => {
                        if let Err(error) = owned.collect_announcements() {
                            return owned.fail_start(error);
                        }
                        owned.port = port;
                        return Ok(StartOutcome::Ready(owned));
                    }
                    Ok(false) => {}
                    Err(error) => return owned.fail_start(error),
                }
            }
            if Instant::now() >= deadline {
                let error = if owned.announced_port.is_none() {
                    format!(
                        "llama-server did not announce a listening endpoint within {} ms",
                        timeout.as_millis()
                    )
                } else {
                    format!(
                        "llama-server did not become ready within {} ms",
                        timeout.as_millis()
                    )
                };
                return owned.fail_start(error);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn fail_start(mut self, error: String) -> Result<StartOutcome, String> {
        self.terminate()
            .map_err(|cleanup| format!("{error}; cleanup failed: {cleanup}"))?;
        Err(self.with_diagnostic(error))
    }

    fn collect_announcements(&mut self) -> Result<(), String> {
        for announcement in self.announcements.try_iter() {
            let port = announcement?;
            match self.announced_port {
                Some(existing) if existing != port => {
                    return Err(format!(
                        "conflicting listening announcements: ports {existing} and {port}"
                    ));
                }
                Some(_) => {}
                None => self.announced_port = Some(port),
            }
        }
        if self.announcement_overflow.load(Ordering::SeqCst) {
            return Err("llama-server announcement state overflowed".into());
        }
        Ok(())
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("owned server child is present")
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    fn try_wait(&mut self) -> Result<Option<ServerExit>, String> {
        if let Err(error) = self.collect_announcements() {
            self.terminate()
                .map_err(|cleanup| format!("{error}; cleanup failed: {cleanup}"))?;
            return Err(self.with_diagnostic(error));
        }
        let Some(status) = self
            .child_mut()
            .try_wait()
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let code = exit_code(status);
        self.terminate()?;
        Ok(Some(self.server_exit(code)))
    }

    pub fn terminate(&mut self) -> Result<(), String> {
        if let Some(child) = self.child.as_mut() {
            terminate_owned_group(child, self.group)?;
            self.child.take();
        }
        self.join_output_readers()
    }

    fn join_output_readers(&mut self) -> Result<(), String> {
        let stdout = join_output_reader(&mut self.stdout_reader, &mut self.stdout_tail);
        let stderr = join_output_reader(&mut self.stderr_reader, &mut self.stderr_tail);
        stdout.and(stderr)
    }

    fn with_diagnostic(&self, error: String) -> String {
        match self.diagnostic() {
            Some(diagnostic) => format!("{error}: {diagnostic}"),
            None => error,
        }
    }

    fn server_exit(&self, code: i32) -> ServerExit {
        ServerExit {
            code,
            diagnostic: self.diagnostic(),
        }
    }

    fn diagnostic(&self) -> Option<String> {
        let tail = if self.stderr_tail.is_empty() {
            &self.stdout_tail
        } else {
            &self.stderr_tail
        };
        let diagnostic = String::from_utf8_lossy(tail);
        let diagnostic = diagnostic.trim();
        if diagnostic.is_empty() {
            None
        } else {
            Some(diagnostic.to_string())
        }
    }

    #[cfg(test)]
    fn output_readers_owned(&self) -> bool {
        self.stdout_reader.is_some() || self.stderr_reader.is_some()
    }
}

fn join_output_reader(
    reader: &mut Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    tail: &mut Vec<u8>,
) -> Result<(), String> {
    let Some(reader) = reader.take() else {
        return Ok(());
    };
    *tail = reader
        .join()
        .map_err(|_| "llama-server output reader panicked".to_string())??;
    Ok(())
}

impl Drop for OwnedServer {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

#[cfg(unix)]
fn terminate_owned_group(child: &mut Child, group: i32) -> Result<(), String> {
    signal_process_group(group, libc::SIGTERM)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let _ = child.try_wait().map_err(|error| error.to_string())?;
        if !process_group_exists(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    signal_process_group(group, libc::SIGKILL)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !process_group_exists(group)? {
            let _ = child.wait().map_err(|error| error.to_string())?;
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err("owned process group survived SIGKILL".into())
}

#[cfg(unix)]
fn signal_process_group(group: i32, signal: i32) -> Result<(), String> {
    // SAFETY: the negative PID targets only the exact process group created for this child.
    let result = unsafe { libc::kill(-group, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error.to_string())
    }
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
fn terminate_owned_group(child: &mut Child, _group: i32) -> Result<(), String> {
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(error) => return Err(error.to_string()),
    }
    let _ = child.wait().map_err(|error| error.to_string())?;
    Ok(())
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
    use std::net::TcpListener;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[cfg(unix)]
    static RUN_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(unix)]
    fn process_test_lock() -> std::sync::MutexGuard<'static, ()> {
        RUN_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    fn serve_models(alias: &'static str) -> (u16, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            assert!(
                request.starts_with(b"GET /v1/models HTTP/1.1\r\n"),
                "{}",
                String::from_utf8_lossy(&request)
            );
            let body = format!(r#"{{"data":[{{"id":"{alias}"}}]}}"#);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (port, server)
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
            // SAFETY: the production signal subscription is installed before the child starts.
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
                "99",
                "--jinja",
                "--reasoning",
                "off"
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

    #[test]
    fn announcement_validation_accepts_only_an_exact_loopback_http_endpoint() {
        assert_eq!(
            validate_announcement_line(
                "0.00.000.000 I srv  llama_server: listening on http://127.0.0.1:43123",
            )
            .unwrap(),
            43123
        );
        assert!(validate_announcement_line("listening on http://0.0.0.0:43123").is_err());
    }

    #[test]
    fn models_readiness_body_is_bounded_and_requires_the_exact_alias() {
        assert!(models_reader_has_alias(
            std::io::Cursor::new(br#"{"data":[{"id":"demo"}]}"#),
            "demo",
        )
        .unwrap());
        assert!(!models_reader_has_alias(
            std::io::Cursor::new(br#"{"data":[{"id":"demo-extra"}]}"#),
            "demo",
        )
        .unwrap());

        let oversized = vec![b' '; MAX_MODELS_BODY + 1];
        let error = models_reader_has_alias(std::io::Cursor::new(oversized), "demo").unwrap_err();
        assert!(error.contains("too large"), "{error}");
    }

    #[test]
    fn automatic_port_is_concrete_and_explicit_port_is_preserved() {
        let automatic = resolve_requested_port(0).unwrap();

        assert_ne!(automatic, 0);
        assert_eq!(resolve_requested_port(43123).unwrap(), 43123);
    }

    #[cfg(unix)]
    #[test]
    fn owned_server_passes_runtime_args_and_owns_both_output_drains() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let argv = dir.path().join("argv");
        let (port, http) = serve_models("demo");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '0.00 I srv: listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n",
                argv.display()
            )
            .as_bytes(),
        );

        let outcome = OwnedServer::start(
            &server_path,
            Path::new("/models/model.gguf"),
            "demo",
            port,
            8192,
            Duration::from_secs(2),
            || None,
        )
        .unwrap();
        let mut server = match outcome {
            StartOutcome::Ready(server) => server,
            _ => panic!("server did not become ready"),
        };
        http.join().unwrap();

        assert_eq!(
            std::fs::read_to_string(&argv).unwrap(),
            format!(
                "--model\n/models/model.gguf\n--alias\ndemo\n--host\n127.0.0.1\n--port\n{port}\n--ctx-size\n8192\n--n-gpu-layers\n99\n--jinja\n--reasoning\noff\n"
            )
        );
        assert_eq!(server.port(), port);
        assert!(server.output_readers_owned());
        server.terminate().unwrap();
        assert!(!server.output_readers_owned());
        server.terminate().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn startup_timeout_and_explicit_port_mismatch_clean_the_owned_group() {
        let _lock = process_test_lock();
        for (requested_port, announced_port, expected) in [
            (0, None, "did not announce"),
            (43124, Some(43123), "expected 43124"),
        ] {
            let dir = tempdir().unwrap();
            let server_path = dir.path().join("server");
            let group_path = dir.path().join("group");
            let announcement = announced_port.map_or_else(String::new, |port| {
                format!("printf 'listening on http://127.0.0.1:{port}\\n' >&2\n")
            });
            write_executable_script(
                &server_path,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$$\" > '{}'\n{announcement}while :; do sleep 1; done\n",
                    group_path.display()
                )
                .as_bytes(),
            );

            let error = match OwnedServer::start(
                &server_path,
                Path::new("/models/model.gguf"),
                "demo",
                requested_port,
                1,
                Duration::from_millis(500),
                || None,
            ) {
                Err(error) => error,
                Ok(_) => panic!("server unexpectedly started"),
            };
            assert!(error.contains(expected), "{error}");
            let group = std::fs::read_to_string(&group_path)
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap();
            assert!(!process_group_exists(group).unwrap());
        }
    }

    #[cfg(unix)]
    #[test]
    fn stdout_listening_line_is_drain_only() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        write_executable_script(
            &server_path,
            b"#!/bin/sh\nprintf 'listening on http://127.0.0.1:43123\\n'\nwhile :; do sleep 1; done\n",
        );

        let error = match OwnedServer::start(
            &server_path,
            Path::new("/models/model.gguf"),
            "demo",
            0,
            1,
            Duration::from_millis(500),
            || None,
        ) {
            Err(error) => error,
            Ok(_) => panic!("stdout announcement unexpectedly started the server"),
        };

        assert!(error.contains("did not announce"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn post_ready_poll_returns_diagnostic_after_descendant_cleanup() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let release = dir.path().join("release");
        let (port, http) = serve_models("demo");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\n(trap '' TERM; while :; do sleep 1; done) &\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile [ ! -f '{}' ]; do sleep 1; done\nprintf 'fatal: post-ready model crash\\n' >&2\nexit 7\n",
                release.display()
            )
            .as_bytes(),
        );
        let outcome = OwnedServer::start(
            &server_path,
            Path::new("/models/model.gguf"),
            "demo",
            port,
            1,
            Duration::from_secs(2),
            || None,
        )
        .unwrap();
        let server = match outcome {
            StartOutcome::Ready(server) => server,
            _ => panic!("server did not become ready"),
        };
        let group = server.group;
        let mut server = ForegroundServer {
            server,
            signal: foreground_signal_flag().unwrap(),
        };
        http.join().unwrap();
        std::fs::write(release, b"go").unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let exit = loop {
            match server.poll().unwrap() {
                None => {}
                Some(exit) => break exit,
            }
            assert!(Instant::now() < deadline, "leader did not exit");
            std::thread::yield_now();
        };

        assert_eq!(exit.code, 7);
        assert!(
            exit.diagnostic
                .as_deref()
                .is_some_and(|text| text.contains("fatal: post-ready model crash")),
            "{exit:?}"
        );
        assert!(!process_group_exists(group).unwrap());
        assert!(!server.server.output_readers_owned());
    }

    #[cfg(unix)]
    #[test]
    fn discovery_uses_explicit_then_environment_then_managed_then_path() {
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
        let _lock = process_test_lock();
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
    fn managed_runtime_allows_a_slow_cold_version_probe() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let managed = dir.path().join("managed");
        write_executable_script(
            &managed,
            b"#!/bin/sh\nsleep 4\nprintf '%s\\n' 'version: 10121 (555881ebc)' >&2\n",
        );

        assert_eq!(
            discover_server(None, None, &managed, None).unwrap(),
            managed
        );
    }

    #[cfg(unix)]
    #[test]
    fn version_probes_are_bounded() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        write_executable_script(&server, b"#!/bin/sh\n(sleep 2) &\nexit 0\n");
        let started = Instant::now();

        let error = probe_version_with_timeout(&server, VERSION_PROBE_TIMEOUT).unwrap_err();

        assert!(error.contains("timed out reading"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
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

        let ready = readiness(&readiness_client().unwrap(), port, "demo").unwrap();
        stop.store(true, Ordering::SeqCst);
        server.join().unwrap();

        assert!(!ready, "readiness followed a loopback redirect");
        assert!(!redirected.load(Ordering::SeqCst));
    }

    #[cfg(unix)]
    #[test]
    fn installed_handlers_map_sigterm_and_sigint_to_shell_exit_codes() {
        let _lock = process_test_lock();

        let term = run_with_signal(libc::SIGTERM);
        let interrupt = run_with_signal(libc::SIGINT);

        assert_eq!((term, interrupt), (143, 130));
    }

    #[cfg(unix)]
    #[test]
    fn signal_registration_is_reused_and_reset_for_each_foreground_run() {
        let _lock = process_test_lock();
        let first = foreground_signal_flag().unwrap();
        first.store(libc::SIGTERM as usize, Ordering::SeqCst);

        let second = foreground_signal_flag().unwrap();

        assert!(std::ptr::eq(first, second));
        assert_eq!(second.load(Ordering::SeqCst), 0);
    }

    #[cfg(unix)]
    #[test]
    fn leader_exit_cleans_surviving_process_group_before_returning_status() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let child_ready = dir.path().join("child-ready");
        let group_file = dir.path().join("group");
        write_executable_script(
            &server,
            b"#!/bin/sh\n(\n  trap '' TERM\n  printf ready > \"$2\"\n  while :; do sleep 1; done\n) &\nwhile [ ! -f \"$2\" ]; do :; done\nprintf '%s\\n' \"$$\" > \"$4\"\nprintf 'fatal: model load failed\\n' >&2\nexit 7\n",
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
    fn pre_ready_diagnostic_uses_stdout_fallback_and_caps_oversized_output() {
        let _lock = process_test_lock();
        for (stream, diagnostic) in [
            ("stdout", "stdout model failure"),
            ("stderr", "bounded tail marker"),
        ] {
            let dir = tempdir().unwrap();
            let server = dir.path().join("server");
            let output = if stream == "stdout" {
                format!("#!/bin/sh\nprintf '{diagnostic}\\n'\nexit 8\n")
            } else {
                format!(
                    "#!/bin/sh\nprintf '{}{diagnostic}\\n' >&2\nexit 9\n",
                    "x".repeat(8192)
                )
            };
            write_executable_script(&server, output.as_bytes());

            let outcome = OwnedServer::start(
                &server,
                Path::new("/models/model.gguf"),
                "demo",
                0,
                1,
                Duration::from_secs(2),
                || None,
            )
            .unwrap();
            let exit = match outcome {
                StartOutcome::Exited(exit) => exit,
                _ => panic!("expected pre-ready exit"),
            };
            let retained = exit.diagnostic.as_deref().unwrap_or_default();

            assert_eq!(exit.code, if stream == "stdout" { 8 } else { 9 });
            assert!(retained.contains(diagnostic), "{retained}");
            assert!(
                retained.len() <= MAX_DIAGNOSTIC_TAIL,
                "diagnostic was not capped: {}",
                retained.len()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn teardown_waits_for_the_exact_process_group_to_disappear() {
        let _lock = process_test_lock();
        use std::os::unix::process::CommandExt;
        let dir = tempdir().unwrap();
        let ready = dir.path().join("ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("(trap '' TERM; echo ready > \"$1\"; while :; do sleep 1; done) & wait")
            .arg("sh")
            .arg(&ready);
        command.process_group(0);
        let mut child = command.spawn().unwrap();
        let pgid = i32::try_from(child.id()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready.exists() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(ready.exists());
        terminate_owned_group(&mut child, pgid).unwrap();
        assert!(!process_group_exists(pgid).unwrap());
    }
}
