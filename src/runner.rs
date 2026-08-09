use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
use crate::ui;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(unix)]
use std::sync::atomic::AtomicI32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
const PERSISTENT_SLEEP_IDLE_SECONDS: u64 = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LaunchPolicy {
    Foreground,
    PersistentApp,
}

impl LaunchPolicy {
    pub(crate) fn sleep_idle_seconds(self) -> Option<u64> {
        match self {
            Self::Foreground => None,
            Self::PersistentApp => Some(PERSISTENT_SLEEP_IDLE_SECONDS),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LaunchProfile {
    Generic,
    Gemma4Mtp {
        draft: Option<PathBuf>,
        #[cfg(test)]
        test_required_version: Option<String>,
    },
}

impl LaunchProfile {
    pub(crate) fn generic() -> Self {
        Self::Generic
    }

    pub(crate) fn gemma4_mtp(draft: Option<PathBuf>) -> Self {
        Self::Gemma4Mtp {
            draft,
            #[cfg(test)]
            test_required_version: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn gemma4_mtp_for_test(draft: Option<PathBuf>, version: String) -> Self {
        Self::Gemma4Mtp {
            draft,
            test_required_version: Some(version),
        }
    }

    fn required_version(&self) -> Option<&str> {
        match self {
            Self::Generic => None,
            Self::Gemma4Mtp {
                #[cfg(test)]
                test_required_version,
                ..
            } => {
                #[cfg(test)]
                {
                    test_required_version.as_deref().or(Some(MANAGED_VERSION))
                }
                #[cfg(not(test))]
                {
                    Some(MANAGED_VERSION)
                }
            }
        }
    }

    fn draft(&self) -> Option<&Path> {
        match self {
            Self::Generic => None,
            Self::Gemma4Mtp { draft, .. } => draft.as_deref(),
        }
    }

    fn effective_name(&self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Gemma4Mtp { draft: Some(_), .. } => "gemma4_mtp",
            Self::Gemma4Mtp { draft: None, .. } => "gemma4_primary",
        }
    }

    pub(crate) fn effective_profile(&self) -> EffectiveProfile {
        match self {
            Self::Generic => EffectiveProfile::Generic,
            Self::Gemma4Mtp { draft: Some(_), .. } => EffectiveProfile::Gemma4Mtp,
            Self::Gemma4Mtp { draft: None, .. } => EffectiveProfile::PrimaryOnly,
        }
    }

    fn primary_only(&self) -> Option<Self> {
        match self {
            Self::Generic => None,
            Self::Gemma4Mtp { draft: None, .. } => None,
            Self::Gemma4Mtp { draft: Some(_), .. } => {
                let mut primary = self.clone();
                let Self::Gemma4Mtp { draft, .. } = &mut primary else {
                    unreachable!("matched Gemma MTP profile")
                };
                *draft = None;
                Some(primary)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Launch {
    pub(crate) server: PathBuf,
    pub(crate) model: PathBuf,
    pub(crate) id: String,
    pub(crate) requested_port: u16,
    pub(crate) ctx: u32,
    pub(crate) profile: LaunchProfile,
    pub(crate) policy: LaunchPolicy,
}

impl Launch {
    fn generic(server: &Path, model: &Path, id: &str, requested_port: u16, ctx: u32) -> Self {
        Self {
            server: server.to_path_buf(),
            model: model.to_path_buf(),
            id: id.into(),
            requested_port,
            ctx,
            profile: LaunchProfile::Generic,
            policy: LaunchPolicy::Foreground,
        }
    }

    pub(crate) fn primary_only(&self) -> Option<Self> {
        Some(Self {
            profile: self.profile.primary_only()?,
            ..self.clone()
        })
    }
}

pub fn discover_server(
    explicit: Option<&Path>,
    environment: Option<&OsStr>,
    managed: &Path,
    path: Option<&OsStr>,
) -> Result<PathBuf, String> {
    discover_server_with_requirement(explicit, environment, managed, path, None)
}

pub(crate) fn discover_from_process(
    explicit: Option<&Path>,
    managed: &Path,
    profile: &LaunchProfile,
) -> Result<PathBuf, String> {
    discover_server_with_requirement(
        explicit,
        std::env::var_os("LOXA_LLAMA_SERVER").as_deref(),
        managed,
        std::env::var_os("PATH").as_deref(),
        profile.required_version(),
    )
}

#[allow(
    dead_code,
    reason = "managed admission is wired by the follow-on host task"
)]
pub(crate) fn validate_managed_server(path: &Path) -> Result<PathBuf, String> {
    validate_managed_candidate(path)?;
    Ok(path.to_path_buf())
}

fn discover_server_with_requirement(
    explicit: Option<&Path>,
    environment: Option<&OsStr>,
    managed: &Path,
    path: Option<&OsStr>,
    required_version: Option<&str>,
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
            validate_managed_candidate(managed)?;
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

fn validate_managed_candidate(path: &Path) -> Result<(), String> {
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
    if first_line != MANAGED_VERSION {
        return Err(format!(
            "managed llama-server bundle is damaged at {}: expected --version first line {MANAGED_VERSION:?}, found {first_line:?}",
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

pub(crate) fn build_args(launch: &Launch, port: u16) -> Vec<OsString> {
    let mtp = matches!(&launch.profile, LaunchProfile::Gemma4Mtp { .. });
    let mut args = vec![
        "--model".into(),
        launch.model.as_os_str().to_owned(),
        "--alias".into(),
        launch.id.clone().into(),
        "--host".into(),
        "127.0.0.1".into(),
        "--cors-origins".into(),
        "localhost".into(),
        "--no-ui".into(),
        "--port".into(),
        port.to_string().into(),
        "--ctx-size".into(),
        launch.ctx.to_string().into(),
        "--n-gpu-layers".into(),
        if mtp { "all" } else { "99" }.into(),
    ];
    if mtp {
        args.extend(["--fit".into(), "off".into()]);
    }
    args.extend(["--jinja".into(), "--reasoning".into(), "off".into()]);
    if let Some(draft) = launch.profile.draft() {
        args.extend([
            "--spec-draft-model".into(),
            draft.as_os_str().to_owned(),
            "--spec-type".into(),
            "draft-mtp".into(),
            "--spec-draft-n-max".into(),
            "4".into(),
            "--n-gpu-layers-draft".into(),
            "all".into(),
        ]);
    }
    if let Some(seconds) = launch.policy.sleep_idle_seconds() {
        args.extend(["--sleep-idle-seconds".into(), seconds.to_string().into()]);
    }
    args
}

#[allow(
    dead_code,
    reason = "persistent attachment is consumed by the follow-on session task"
)]
pub(crate) fn build_persistent_args_for_fingerprint(
    models_root: &Path,
    fingerprint: &RuntimeFingerprint,
    port: u16,
) -> Result<Vec<OsString>, String> {
    fingerprint.validate_persistent_lease(fingerprint.model_id())?;
    if port == 0 {
        return Err("persistent runtime port must be nonzero".into());
    }
    let model_dir = models_root.join(fingerprint.model_id());
    let profile = match fingerprint.effective_profile() {
        EffectiveProfile::Generic => LaunchProfile::generic(),
        EffectiveProfile::Gemma4Mtp => LaunchProfile::gemma4_mtp(Some(
            model_dir.join(
                fingerprint
                    .draft_local_filename()
                    .ok_or_else(|| "MTP runtime fingerprint is missing its draft".to_string())?,
            ),
        )),
        EffectiveProfile::PrimaryOnly => LaunchProfile::gemma4_mtp(None),
    };
    Ok(build_args(
        &Launch {
            server: PathBuf::new(),
            model: model_dir.join(fingerprint.primary_local_filename()),
            id: fingerprint.model_id().to_owned(),
            requested_port: port,
            ctx: fingerprint.effective_context(),
            profile,
            policy: LaunchPolicy::PersistentApp,
        },
        port,
    ))
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
    run_dir: &Path,
) -> Result<i32, String> {
    run_launch(
        &Launch::generic(server, model, id, requested_port, ctx),
        run_dir,
    )
}

pub(crate) fn run_launch(launch: &Launch, run_dir: &Path) -> Result<i32, String> {
    let starting = ui::spinner(format!("Starting {}", launch.id));
    let started = start_foreground(launch, run_dir);
    starting.finish_and_clear();
    let mut server = match started? {
        ForegroundStart::Ready(server) => server,
        ForegroundStart::Stopped(exit) => return Ok(report_exit(exit)),
    };
    anstream::println!("{}", ready_line(server.port(), &launch.id));
    loop {
        if let Some(exit) = server.poll()? {
            return Ok(report_exit(exit));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn ready_line(port: u16, id: &str) -> String {
    let success = ui::success();
    let accent = ui::accent();
    let muted = ui::muted();
    format!(
        "{success}Ready{success:#} {accent}http://127.0.0.1:{port}{accent:#} {muted}(model {id}){muted:#}"
    )
}

pub(crate) enum ForegroundStart {
    Ready(ForegroundServer),
    Stopped(ServerExit),
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupInterruption {
    Cancelled,
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
pub(crate) enum PersistentStart {
    Ready(Box<PersistentServer>),
    Stopped(ServerExit),
    Interrupted(StartupInterruption),
}

pub(crate) struct ForegroundServer {
    server: Box<OwnedServer>,
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
pub(crate) struct PersistentServer {
    server: Box<OwnedServer>,
    runnable: crate::runnable::Runnable,
}

pub(crate) fn start_foreground(launch: &Launch, run_dir: &Path) -> Result<ForegroundStart, String> {
    install_termination_watcher(run_dir)?;
    start_foreground_with(launch, run_dir, process_termination_signal)
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
pub(crate) fn start_persistent<F>(
    mut runnable: crate::runnable::Runnable,
    run_dir: &Path,
    cancelled: F,
) -> Result<PersistentStart, String>
where
    F: Fn() -> bool,
{
    if cancelled() {
        return Ok(persistent_interrupted());
    }
    let launch_started = Instant::now();
    tracing::info!(
        event = "server_starting",
        model_id = %runnable.launch().id,
        requested_port = runnable.launch().requested_port,
        context_size = runnable.launch().ctx
    );
    let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
    if cancelled() {
        return Ok(persistent_interrupted());
    }
    match start_persistent_attempt(&runnable, ownership, &cancelled)? {
        StartOutcome::Ready(server) => {
            finish_persistent_ready(server, runnable, launch_started, false, &cancelled)
        }
        StartOutcome::Exited(exit) => {
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            if runnable.primary_only().is_none() {
                tracing::warn!(
                    event = "server_stopped_before_ready",
                    model_id = %runnable.launch().id,
                    exit_code = exit.code
                );
                return Ok(PersistentStart::Stopped(exit));
            }
            report_mtp_draft_start_failure(runnable.launch(), "exited", exit.diagnostic.as_deref());
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            tracing::info!(
                event = "gemma_mtp_primary_retry",
                model_id = %runnable.launch().id,
                attempt = 2_u8
            );
            let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            match start_persistent_attempt(&runnable, ownership, &cancelled)? {
                StartOutcome::Ready(server) => {
                    finish_persistent_ready(server, runnable, launch_started, true, &cancelled)
                }
                StartOutcome::Exited(exit) => Ok(PersistentStart::Stopped(exit)),
                StartOutcome::Interrupted(interruption) => {
                    Ok(PersistentStart::Interrupted(interruption))
                }
                StartOutcome::Signaled(_) => {
                    Err("persistent startup returned an invalid signal interruption".into())
                }
            }
        }
        StartOutcome::Interrupted(interruption) => Ok(PersistentStart::Interrupted(interruption)),
        StartOutcome::Signaled(_) => {
            Err("persistent startup returned an invalid signal interruption".into())
        }
    }
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
fn persistent_interrupted() -> PersistentStart {
    PersistentStart::Interrupted(StartupInterruption::Cancelled)
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
fn finish_persistent_ready<F>(
    mut server: Box<OwnedServer>,
    runnable: crate::runnable::Runnable,
    launch_started: Instant,
    fallback: bool,
    cancelled: &F,
) -> Result<PersistentStart, String>
where
    F: Fn() -> bool,
{
    if cancelled() {
        server.terminate()?;
        return Ok(persistent_interrupted());
    }
    if fallback {
        tracing::info!(
            event = "server_ready",
            model_id = %runnable.launch().id,
            port = server.port(),
            elapsed_ms = launch_started.elapsed().as_millis() as u64,
            effective_profile = runnable.launch().profile.effective_name(),
            fallback = "primary_only",
        );
    } else {
        tracing::info!(
            event = "server_ready",
            model_id = %runnable.launch().id,
            port = server.port(),
            elapsed_ms = launch_started.elapsed().as_millis() as u64,
            effective_profile = runnable.launch().profile.effective_name(),
        );
    }
    Ok(PersistentStart::Ready(Box::new(PersistentServer {
        server,
        runnable,
    })))
}

fn start_foreground_with<F>(
    launch: &Launch,
    run_dir: &Path,
    signal: F,
) -> Result<ForegroundStart, String>
where
    F: Fn() -> Option<i32>,
{
    let launch_started = Instant::now();
    tracing::info!(
        event = "server_starting",
        model_id = %launch.id,
        requested_port = launch.requested_port,
        context_size = launch.ctx
    );
    let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
    match start_owned_attempt(launch, ownership, &signal) {
        Ok(StartOutcome::Ready(server)) => {
            tracing::info!(
                event = "server_ready",
                model_id = %launch.id,
                port = server.port(),
                elapsed_ms = launch_started.elapsed().as_millis() as u64,
                effective_profile = launch.profile.effective_name(),
            );
            Ok(ForegroundStart::Ready(ForegroundServer { server }))
        }
        Ok(StartOutcome::Exited(exit)) => {
            if let Some(stopped) = stopped_for_signal(&signal) {
                return Ok(stopped);
            }
            let Some(primary) = launch.primary_only() else {
                tracing::warn!(
                    event = "server_stopped_before_ready",
                    model_id = %launch.id,
                    exit_code = exit.code
                );
                return Ok(ForegroundStart::Stopped(exit));
            };
            report_mtp_draft_start_failure(launch, "exited", exit.diagnostic.as_deref());
            start_mtp_primary_retry(&primary, run_dir, &signal, launch_started)
        }
        Ok(StartOutcome::Signaled(signal)) => Ok(ForegroundStart::Stopped(ServerExit {
            code: 128 + signal,
            diagnostic: None,
        })),
        Ok(StartOutcome::Interrupted(_)) => {
            Err("foreground startup returned an invalid cancellation interruption".into())
        }
        Err(error) => Err(error),
    }
}

fn stopped_for_signal<F>(signal: &F) -> Option<ForegroundStart>
where
    F: Fn() -> Option<i32>,
{
    signal().map(|signal| {
        ForegroundStart::Stopped(ServerExit {
            code: 128 + signal,
            diagnostic: None,
        })
    })
}

fn start_owned_attempt<F>(
    launch: &Launch,
    ownership: crate::runtime::RuntimeOwnership,
    signal: &F,
) -> Result<StartOutcome, String>
where
    F: Fn() -> Option<i32>,
{
    OwnedServer::start_with_ownership(launch, STARTUP_TIMEOUT, ownership, signal)
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
fn start_persistent_attempt<F>(
    runnable: &crate::runnable::Runnable,
    ownership: crate::runtime::RuntimeOwnership,
    cancelled: &F,
) -> Result<StartOutcome, String>
where
    F: Fn() -> bool,
{
    OwnedServer::start_with_persistent_ownership(
        runnable.launch(),
        runnable.fingerprint(),
        STARTUP_TIMEOUT,
        ownership,
        cancelled,
    )
}

fn report_mtp_draft_start_failure(
    launch: &Launch,
    outcome: &'static str,
    diagnostic: Option<&str>,
) {
    tracing::warn!(
        event = "gemma_mtp_draft_start_failed",
        model_id = %launch.id,
        outcome
    );
    if diagnostic.is_some() && tracing::enabled!(target: "loxa::runner", tracing::Level::DEBUG) {
        tracing::debug!(
            target: "loxa::runner",
            event = "gemma_mtp_draft_start_diagnostic",
            model_id = %launch.id,
            diagnostic_present = true
        );
    }
    anstream::eprintln!("Warning: MTP draft startup failed; retrying the primary model only.");
}

fn start_mtp_primary_retry<F>(
    launch: &Launch,
    run_dir: &Path,
    signal: &F,
    launch_started: Instant,
) -> Result<ForegroundStart, String>
where
    F: Fn() -> Option<i32>,
{
    tracing::info!(
        event = "gemma_mtp_primary_retry",
        model_id = %launch.id,
        attempt = 2_u8
    );
    let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
    match start_owned_attempt(launch, ownership, signal)? {
        StartOutcome::Ready(server) => {
            tracing::info!(
                event = "server_ready",
                model_id = %launch.id,
                port = server.port(),
                elapsed_ms = launch_started.elapsed().as_millis() as u64,
                effective_profile = launch.profile.effective_name(),
                fallback = "primary_only",
            );
            Ok(ForegroundStart::Ready(ForegroundServer { server }))
        }
        StartOutcome::Exited(exit) => {
            tracing::warn!(
                event = "server_stopped_before_ready",
                model_id = %launch.id,
                exit_code = exit.code
            );
            Ok(ForegroundStart::Stopped(exit))
        }
        StartOutcome::Signaled(signal) => Ok(ForegroundStart::Stopped(ServerExit {
            code: 128 + signal,
            diagnostic: None,
        })),
        StartOutcome::Interrupted(_) => {
            Err("foreground startup returned an invalid cancellation interruption".into())
        }
    }
}

#[cfg(test)]
fn start_foreground_with_signal<F>(
    launch: &Launch,
    run_dir: &Path,
    signal: F,
) -> Result<ForegroundStart, String>
where
    F: Fn() -> Option<i32>,
{
    start_foreground_with(launch, run_dir, signal)
}

impl ForegroundServer {
    pub(crate) fn port(&self) -> u16 {
        self.server.port()
    }

    /// Polls for child exit. Unix termination signals are handled by the
    /// process-level watcher so this method never has to wake terminal input.
    pub(crate) fn poll(&mut self) -> Result<Option<ServerExit>, String> {
        if let Some(exit) = self.server.try_wait()? {
            return Ok(Some(exit));
        }
        Ok(None)
    }

    pub(crate) fn terminate(&mut self) -> Result<(), String> {
        self.server.terminate()
    }
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
impl PersistentServer {
    pub(crate) fn port(&self) -> u16 {
        self.server.port()
    }

    pub(crate) fn fingerprint(&self) -> &crate::runtime_fingerprint::RuntimeFingerprint {
        self.runnable.fingerprint()
    }

    pub(crate) fn poll(&mut self) -> Result<Option<ServerExit>, String> {
        self.server.try_wait()
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

#[allow(
    dead_code,
    reason = "persistent attachment is consumed by the follow-on session task"
)]
pub(crate) fn probe_model_alias(port: u16, id: &str) -> Result<bool, String> {
    readiness(&readiness_client()?, port, id)
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
static PROCESS_SIGNAL_WATCHER: OnceLock<Result<(), String>> = OnceLock::new();

#[cfg(unix)]
static PROCESS_TERMINATION_SIGNAL: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
static ACTIVE_SERVER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
const STARTING_SERVER: u64 = u64::MAX;

#[cfg(unix)]
fn install_termination_watcher(run_dir: &Path) -> Result<(), String> {
    let run_dir = run_dir.to_path_buf();
    PROCESS_SIGNAL_WATCHER
        .get_or_init(move || {
            let mut termination =
                signal_hook::iterator::Signals::new([libc::SIGINT, libc::SIGTERM, libc::SIGHUP])
                    .map_err(|error| error.to_string())?;
            std::thread::Builder::new()
                .name("loxa-signal".into())
                .spawn(move || {
                    if let Some(signal) = termination.forever().next() {
                        PROCESS_TERMINATION_SIGNAL.store(signal, Ordering::SeqCst);
                        let mut active = ACTIVE_SERVER.load(Ordering::SeqCst);
                        while active == STARTING_SERVER {
                            std::thread::sleep(Duration::from_millis(10));
                            active = ACTIVE_SERVER.load(Ordering::SeqCst);
                        }
                        if let Some((pid, group)) = unpack_server_identity(active) {
                            while crate::runtime::terminate_stale_process_group(group).is_err() {
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            if crate::runtime::clear_terminated_owned_lease(&run_dir, pid, group)
                                .is_err()
                            {
                                tracing::warn!(event = "signal_runtime_lease_cleanup_failed");
                            }
                        }
                        std::process::exit(128 + signal);
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(())
        })
        .as_ref()
        .map_err(Clone::clone)
        .copied()
}

#[cfg(unix)]
fn process_termination_signal() -> Option<i32> {
    let signal = PROCESS_TERMINATION_SIGNAL.load(Ordering::SeqCst);
    (signal != 0).then_some(signal)
}

#[cfg(test)]
#[cfg(unix)]
struct ProcessTerminationSignalReset;

#[cfg(test)]
#[cfg(unix)]
impl Drop for ProcessTerminationSignalReset {
    fn drop(&mut self) {
        PROCESS_TERMINATION_SIGNAL.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[cfg(unix)]
fn reset_process_termination_signal_for_test() -> ProcessTerminationSignalReset {
    PROCESS_TERMINATION_SIGNAL.store(0, Ordering::SeqCst);
    ProcessTerminationSignalReset
}

#[cfg(not(unix))]
fn process_termination_signal() -> Option<i32> {
    None
}

#[cfg(unix)]
fn activate_server(pid: u32, group: i32) {
    ACTIVE_SERVER.store(pack_server_identity(pid, group), Ordering::SeqCst);
}

#[cfg(unix)]
fn mark_server_starting() {
    ACTIVE_SERVER.store(STARTING_SERVER, Ordering::SeqCst);
}

#[cfg(unix)]
fn clear_server_starting() {
    let _ = ACTIVE_SERVER.compare_exchange(STARTING_SERVER, 0, Ordering::SeqCst, Ordering::SeqCst);
}

#[cfg(unix)]
fn deactivate_server(pid: u32, group: i32) {
    let _ = ACTIVE_SERVER.compare_exchange(
        pack_server_identity(pid, group),
        0,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
}

#[cfg(unix)]
fn pack_server_identity(pid: u32, group: i32) -> u64 {
    debug_assert!(group > 1);
    (u64::from(pid) << 32) | u64::from(u32::try_from(group).expect("positive process group"))
}

#[cfg(unix)]
fn unpack_server_identity(identity: u64) -> Option<(u32, i32)> {
    let pid = u32::try_from(identity >> 32).ok()?;
    let group = i32::try_from(identity as u32).ok()?;
    (pid != 0 && group > 1).then_some((pid, group))
}

#[cfg(not(unix))]
fn activate_server(_pid: u32, _group: i32) {}

#[cfg(not(unix))]
fn deactivate_server(_pid: u32, _group: i32) {}

#[cfg(not(unix))]
fn mark_server_starting() {}

#[cfg(not(unix))]
fn clear_server_starting() {}

#[cfg(not(unix))]
fn install_termination_watcher(_run_dir: &Path) -> Result<(), String> {
    Ok(())
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
    Ready(Box<OwnedServer>),
    Exited(ServerExit),
    Signaled(i32),
    Interrupted(StartupInterruption),
}

#[allow(
    dead_code,
    reason = "persistent startup is wired by the follow-on host task"
)]
#[derive(Clone, Copy)]
enum StartupStop {
    Signal(i32),
    Interrupted(StartupInterruption),
}

impl From<StartupStop> for StartOutcome {
    fn from(stop: StartupStop) -> Self {
        match stop {
            StartupStop::Signal(signal) => Self::Signaled(signal),
            StartupStop::Interrupted(interruption) => Self::Interrupted(interruption),
        }
    }
}

fn requested_start_outcome<F>(
    server: &mut OwnedServer,
    stop: &F,
) -> Result<Option<StartOutcome>, String>
where
    F: Fn() -> Option<StartupStop>,
{
    let Some(stop) = stop() else {
        return Ok(None);
    };
    server.terminate()?;
    Ok(Some(stop.into()))
}

#[derive(Debug)]
pub(crate) struct ServerExit {
    code: i32,
    diagnostic: Option<String>,
}

pub(crate) fn report_exit(exit: ServerExit) -> i32 {
    if let Some(diagnostic) = exit.diagnostic {
        let diagnostic = ui::sanitize_terminal(&diagnostic);
        eprintln!("{diagnostic}");
    }
    exit.code
}

pub struct OwnedServer {
    child: Option<Child>,
    group: i32,
    port: u16,
    runtime: Option<crate::runtime::RuntimeOwnership>,
    announcements: mpsc::Receiver<Result<u16, String>>,
    announcement_overflow: Arc<AtomicBool>,
    announced_port: Option<u16>,
    stdout_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stdout_tail: Vec<u8>,
    stderr_tail: Vec<u8>,
}

impl OwnedServer {
    #[cfg(test)]
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
        let launch = Launch::generic(server, model, id, requested_port, ctx);
        Self::start_inner(&launch, timeout, None, || signal().map(StartupStop::Signal))
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_ownership<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: crate::runtime::RuntimeOwnership,
        signal: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<i32>,
    {
        Self::start_inner(
            launch,
            timeout,
            Some((runtime, crate::runtime::RuntimeLeasePublication::Foreground)),
            || signal().map(StartupStop::Signal),
        )
    }

    #[allow(
        dead_code,
        reason = "persistent startup is wired by the follow-on host task"
    )]
    fn start_with_persistent_ownership<F>(
        launch: &Launch,
        fingerprint: &crate::runtime_fingerprint::RuntimeFingerprint,
        timeout: Duration,
        runtime: crate::runtime::RuntimeOwnership,
        cancelled: &F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> bool,
    {
        Self::start_inner(
            launch,
            timeout,
            Some((
                runtime,
                crate::runtime::RuntimeLeasePublication::PersistentApp(fingerprint),
            )),
            || cancelled().then_some(StartupStop::Interrupted(StartupInterruption::Cancelled)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        let requested_port = resolve_requested_port(launch.requested_port)?;
        let client = readiness_client()?;
        let mut command = Command::new(&launch.server);
        command
            .args(build_args(launch, requested_port))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        mark_server_starting();
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                clear_server_starting();
                return Err(error.to_string());
            }
        };
        let group = match i32::try_from(child.id()) {
            Ok(group) => group,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                clear_server_starting();
                return Err("invalid child process id".into());
            }
        };
        activate_server(child.id(), group);
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
        let (runtime, publication) = match runtime {
            Some((runtime, publication)) => (Some(runtime), Some(publication)),
            None => (None, None),
        };
        let mut owned = Self {
            child: Some(child),
            group,
            port: 0,
            runtime,
            announcements,
            announcement_overflow,
            announced_port: None,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
        };
        if let Some(runtime) = owned.runtime.as_mut() {
            let publication = publication.expect("owned runtime publication is present");
            if let Err(error) = runtime.record(
                owned.child.as_ref().expect("owned child is present").id(),
                group,
                &launch.id,
                requested_port,
                publication,
            ) {
                return owned.fail_start(error);
            }
        }
        if launch.policy == LaunchPolicy::PersistentApp {
            if let Some(outcome) = requested_start_outcome(&mut owned, &stop)? {
                return Ok(outcome);
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Err(error) = owned.collect_announcements() {
                return owned.fail_start(error);
            }
            if let Some(outcome) = requested_start_outcome(&mut owned, &stop)? {
                return Ok(outcome);
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
                match readiness(&client, port, &launch.id) {
                    Ok(true) => {
                        if let Err(error) = owned.collect_announcements() {
                            return owned.fail_start(error);
                        }
                        owned.port = port;
                        if launch.policy == LaunchPolicy::PersistentApp {
                            if let Some(outcome) = requested_start_outcome(&mut owned, &stop)? {
                                return Ok(outcome);
                            }
                        }
                        return Ok(StartOutcome::Ready(Box::new(owned)));
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
            let pid = child.id();
            tracing::info!(event = "server_terminating", pid, port = self.port);
            terminate_owned_group(child, self.group)?;
            deactivate_server(pid, self.group);
            self.child.take();
            tracing::info!(event = "server_terminated", pid, port = self.port);
        }
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.clear()?;
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
    crate::runtime::terminate_process_group(child, group)
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
#[cfg(test)]
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
    use crate::catalog::{Artifact, ArtifactProvenance, ArtifactRole, Manifest};
    use crate::runnable::Runnable;
    use crate::runtime_fingerprint::RuntimeFingerprint;
    use std::ffi::OsStr;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[cfg(unix)]
    static RUN_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(unix)]
    fn process_test_lock() -> std::sync::MutexGuard<'static, ()> {
        RUN_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[derive(Clone)]
    struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedLogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
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

    fn serve_models(alias: &'static str) -> (u16, std::thread::JoinHandle<()>) {
        serve_models_with_ready_action(alias, || {})
    }

    fn serve_models_with_ready_action<F>(
        alias: &'static str,
        ready: F,
    ) -> (u16, std::thread::JoinHandle<()>)
    where
        F: FnOnce() + Send + 'static,
    {
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
            ready();
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
    fn test_mtp_launch(server: &Path, port: u16) -> Launch {
        Launch {
            server: server.to_path_buf(),
            model: PathBuf::from("/models/model.gguf"),
            id: "demo".into(),
            requested_port: port,
            ctx: 8192,
            profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
            policy: LaunchPolicy::Foreground,
        }
    }

    #[cfg(unix)]
    fn persistent_runnable(root: &Path, server: &Path, port: u16) -> Runnable {
        let model_dir = root.join("models/demo");
        std::fs::create_dir_all(&model_dir).unwrap();
        let manifest = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some("owner/repo".into()),
            revision: Some("0".repeat(40)),
            remote_filename: Some("model.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "a".repeat(64),
            size: 1,
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let policy = LaunchPolicy::PersistentApp;
        let launch = Launch {
            server: server.to_path_buf(),
            model: model_dir.join("model.gguf"),
            id: manifest.id.clone(),
            requested_port: port,
            ctx: 4096,
            profile: LaunchProfile::generic(),
            policy,
        };
        let fingerprint = RuntimeFingerprint::from_manifest(
            &manifest,
            launch.ctx,
            launch.profile.effective_profile(),
            policy.sleep_idle_seconds(),
        )
        .unwrap();
        Runnable::for_test(
            crate::catalog::ModelLock::acquire(&model_dir).unwrap(),
            launch,
            fingerprint,
        )
    }

    #[cfg(unix)]
    fn persistent_mtp_runnable(root: &Path, server: &Path, port: u16) -> Runnable {
        let model_dir = root.join("models/demo");
        std::fs::create_dir_all(&model_dir).unwrap();
        let manifest = Manifest {
            version: 3,
            id: "demo".into(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "a".repeat(64),
            size: 1,
            artifacts: Some(vec![
                Artifact {
                    role: ArtifactRole::Model,
                    local_filename: "model.gguf".into(),
                    sha256: "a".repeat(64),
                    size: 1,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "model-source.gguf".into(),
                    },
                },
                Artifact {
                    role: ArtifactRole::Draft,
                    local_filename: "draft.gguf".into(),
                    sha256: "b".repeat(64),
                    size: 1,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "draft-source.gguf".into(),
                    },
                },
            ]),
            profile: Some(crate::catalog::TEST_MTP_PROFILE.into()),
            runtime: Some(crate::catalog::RuntimeQualification {
                engine: "llama.cpp".into(),
                build: crate::catalog::TEST_LLAMA_BUILD.into(),
            }),
        };
        let policy = LaunchPolicy::PersistentApp;
        let launch = Launch {
            server: server.to_path_buf(),
            model: model_dir.join("model.gguf"),
            id: manifest.id.clone(),
            requested_port: port,
            ctx: 8192,
            profile: LaunchProfile::gemma4_mtp(Some(model_dir.join("draft.gguf"))),
            policy,
        };
        let fingerprint = RuntimeFingerprint::from_manifest(
            &manifest,
            launch.ctx,
            launch.profile.effective_profile(),
            policy.sleep_idle_seconds(),
        )
        .unwrap();
        Runnable::for_test(
            crate::catalog::ModelLock::acquire(&model_dir).unwrap(),
            launch,
            fingerprint,
        )
    }

    #[cfg(unix)]
    fn write_persistent_test_server(path: &Path, announced: &Path, ready: &Path) {
        let executable = std::env::current_exe().unwrap();
        for value in [executable.as_path(), announced, ready] {
            assert!(!value.to_string_lossy().contains('\''));
        }
        write_executable_script(
            path,
            format!(
                "#!/bin/sh\nport=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--port' ]; then shift; port=\"$1\"; fi\n  shift\ndone\nexport LOXA_PERSISTENT_SERVER_CHILD=1\nexport LOXA_PERSISTENT_SERVER_PORT=\"$port\"\nexport LOXA_PERSISTENT_ANNOUNCED='{}'\nexport LOXA_PERSISTENT_READY='{}'\nexec '{}' --exact runner::tests::persistent_server_child --nocapture\n",
                announced.display(),
                ready.display(),
                executable.display(),
            )
            .as_bytes(),
        );
    }

    #[cfg(unix)]
    fn write_mtp_persistent_test_server(path: &Path, argv: &Path, announced: &Path, ready: &Path) {
        let executable = std::env::current_exe().unwrap();
        for value in [executable.as_path(), argv, announced, ready] {
            assert!(!value.to_string_lossy().contains('\''));
        }
        write_executable_script(
            path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'draft startup failed\\n' >&2; exit 42 ;;\nesac\nport=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--port' ]; then shift; port=\"$1\"; fi\n  shift\ndone\nexport LOXA_PERSISTENT_SERVER_CHILD=1\nexport LOXA_PERSISTENT_SERVER_PORT=\"$port\"\nexport LOXA_PERSISTENT_ANNOUNCED='{}'\nexport LOXA_PERSISTENT_READY='{}'\nexec '{}' --exact runner::tests::persistent_server_child --nocapture\n",
                argv.display(),
                announced.display(),
                ready.display(),
                executable.display(),
            )
            .as_bytes(),
        );
    }

    #[cfg(unix)]
    #[test]
    fn persistent_server_child() {
        if std::env::var_os("LOXA_PERSISTENT_SERVER_CHILD").is_none() {
            return;
        }
        let port = std::env::var("LOXA_PERSISTENT_SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let announced = PathBuf::from(std::env::var_os("LOXA_PERSISTENT_ANNOUNCED").unwrap());
        let ready = PathBuf::from(std::env::var_os("LOXA_PERSISTENT_READY").unwrap());
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        eprintln!("test server listening on http://127.0.0.1:{port}");
        std::fs::write(announced, b"announced").unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        std::fs::write(ready, b"ready").unwrap();
        let body = r#"{"data":[{"id":"demo"}]}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        drop(stream);
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn argv_and_readiness_are_exact_and_generic() {
        let launch = Launch::generic(
            Path::new("/servers/llama-server"),
            std::path::Path::new("/models/model.gguf"),
            "demo",
            1234,
            8192,
        );
        let args = build_args(&launch, 1234);
        assert_eq!(
            args,
            [
                "--model",
                "/models/model.gguf",
                "--alias",
                "demo",
                "--host",
                "127.0.0.1",
                "--cors-origins",
                "localhost",
                "--no-ui",
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
    fn attachment_argv_rebuilds_each_persistent_profile_from_the_exact_fingerprint() {
        let models = Path::new("/models");
        let generic: crate::runtime_fingerprint::RuntimeFingerprint =
            serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "model_id": "demo",
                "effective_context": 4096,
                "effective_profile": "generic",
                "sleep_policy": 300,
                "primary": {
                    "local_filename": "model.gguf",
                    "sha256": "a".repeat(64),
                    "size": 7,
                },
                "draft": null,
            }))
            .unwrap();
        let mtp: crate::runtime_fingerprint::RuntimeFingerprint =
            serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "model_id": "demo",
                "effective_context": 8192,
                "effective_profile": "gemma4_mtp",
                "sleep_policy": 300,
                "primary": {
                    "local_filename": "model.gguf",
                    "sha256": "a".repeat(64),
                    "size": 7,
                },
                "draft": {
                    "local_filename": "draft.gguf",
                    "sha256": "b".repeat(64),
                    "size": 5,
                },
            }))
            .unwrap();
        let primary_only = mtp.primary_only().unwrap();

        let generic_expected = [
            "--model",
            "/models/demo/model.gguf",
            "--alias",
            "demo",
            "--host",
            "127.0.0.1",
            "--cors-origins",
            "localhost",
            "--no-ui",
            "--port",
            "43123",
            "--ctx-size",
            "4096",
            "--n-gpu-layers",
            "99",
            "--jinja",
            "--reasoning",
            "off",
            "--sleep-idle-seconds",
            "300",
        ]
        .map(OsString::from)
        .to_vec();
        let mtp_expected = [
            "--model",
            "/models/demo/model.gguf",
            "--alias",
            "demo",
            "--host",
            "127.0.0.1",
            "--cors-origins",
            "localhost",
            "--no-ui",
            "--port",
            "43124",
            "--ctx-size",
            "8192",
            "--n-gpu-layers",
            "all",
            "--fit",
            "off",
            "--jinja",
            "--reasoning",
            "off",
            "--spec-draft-model",
            "/models/demo/draft.gguf",
            "--spec-type",
            "draft-mtp",
            "--spec-draft-n-max",
            "4",
            "--n-gpu-layers-draft",
            "all",
            "--sleep-idle-seconds",
            "300",
        ]
        .map(OsString::from)
        .to_vec();
        let primary_expected = [
            "--model",
            "/models/demo/model.gguf",
            "--alias",
            "demo",
            "--host",
            "127.0.0.1",
            "--cors-origins",
            "localhost",
            "--no-ui",
            "--port",
            "43125",
            "--ctx-size",
            "8192",
            "--n-gpu-layers",
            "all",
            "--fit",
            "off",
            "--jinja",
            "--reasoning",
            "off",
            "--sleep-idle-seconds",
            "300",
        ]
        .map(OsString::from)
        .to_vec();

        assert_eq!(
            build_persistent_args_for_fingerprint(models, &generic, 43123).unwrap(),
            generic_expected
        );
        assert_eq!(
            build_persistent_args_for_fingerprint(models, &mtp, 43124).unwrap(),
            mtp_expected
        );
        assert_eq!(
            build_persistent_args_for_fingerprint(models, &primary_only, 43125).unwrap(),
            primary_expected
        );
    }

    #[test]
    fn persistent_argv_adds_one_sleep_policy_without_changing_foreground_or_fallback() {
        let foreground = Launch::generic(
            Path::new("/servers/llama-server"),
            Path::new("/models/model.gguf"),
            "demo",
            1234,
            8192,
        );
        let persistent = Launch {
            policy: LaunchPolicy::PersistentApp,
            ..foreground.clone()
        };
        let foreground_mtp = Launch {
            profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
            ..foreground.clone()
        };
        let foreground_fallback = foreground_mtp.primary_only().unwrap();

        let persistent_args = build_args(&persistent, 1234);
        let sleep_positions = persistent_args
            .iter()
            .enumerate()
            .filter_map(|(index, argument)| (argument == "--sleep-idle-seconds").then_some(index))
            .collect::<Vec<_>>();
        assert_eq!(sleep_positions.len(), 1);
        assert_eq!(persistent_args[sleep_positions[0] + 1], "300");

        for launch in [&foreground, &foreground_mtp, &foreground_fallback] {
            assert!(
                !build_args(launch, 1234)
                    .iter()
                    .any(|argument| argument == "--sleep-idle-seconds"),
                "foreground launch policy gained a persistent sleep argument"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn foreground_does_not_repoll_the_signal_callback_after_readiness() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let ready = dir.path().join("ready");
        let run_dir = dir.path().join("run");
        let ready_for_server = ready.clone();
        let (port, http) = serve_models_with_ready_action("demo", move || {
            std::fs::write(ready_for_server, b"ready").unwrap();
        });
        write_executable_script(
            &server,
            format!(
                "#!/bin/sh\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n"
            )
            .as_bytes(),
        );
        let launch = Launch::generic(&server, Path::new("/models/model.gguf"), "demo", port, 4096);
        let polls_after_ready = AtomicUsize::new(0);

        let started = start_foreground_with_signal(&launch, &run_dir, || {
            ready.exists().then(|| {
                polls_after_ready.fetch_add(1, Ordering::SeqCst);
                libc::SIGINT
            })
        })
        .unwrap();
        http.join().unwrap();

        let mut foreground = match started {
            ForegroundStart::Ready(server) => server,
            ForegroundStart::Stopped(exit) => {
                panic!("post-readiness signal poll changed parent behavior: {exit:?}")
            }
        };
        assert_eq!(polls_after_ready.load(Ordering::SeqCst), 0);
        foreground.terminate().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn persistent_cancellation_before_spawn_and_after_ownership_is_closed() {
        let _lock = process_test_lock();
        for cancel_on_call in [1, 2] {
            let dir = tempdir().unwrap();
            let server = dir.path().join("server");
            let spawned = dir.path().join("spawned");
            let run_dir = dir.path().join("run");
            write_executable_script(
                &server,
                format!("#!/bin/sh\nprintf spawned > '{}'\n", spawned.display()).as_bytes(),
            );
            let runnable = persistent_runnable(dir.path(), &server, 0);
            let calls = AtomicUsize::new(0);

            let started = start_persistent(runnable, &run_dir, || {
                calls.fetch_add(1, Ordering::SeqCst) + 1 == cancel_on_call
            })
            .unwrap();

            assert!(matches!(
                started,
                PersistentStart::Interrupted(StartupInterruption::Cancelled)
            ));
            assert_eq!(calls.load(Ordering::SeqCst), cancel_on_call);
            assert!(!spawned.exists());
            assert!(!run_dir.join("foreground.json").exists());
            drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
            drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
        }
    }

    #[cfg(unix)]
    #[test]
    fn persistent_cancellation_after_lease_cleans_group_listener_and_ownership() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let run_dir = dir.path().join("run");
        let port = resolve_requested_port(0).unwrap();
        write_executable_script(&server, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
        let runnable = persistent_runnable(dir.path(), &server, port);
        let group = Mutex::new(None);

        let started = start_persistent(runnable, &run_dir, || {
            if let Some((_, active_group)) =
                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
            {
                *group.lock().unwrap() = Some(active_group);
            }
            run_dir.join("foreground.json").is_file()
        })
        .unwrap();

        assert!(matches!(
            started,
            PersistentStart::Interrupted(StartupInterruption::Cancelled)
        ));
        let group = group.into_inner().unwrap().expect("owned process group");
        assert!(!process_group_exists(group).unwrap());
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(!run_dir.join("foreground.json").exists());
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn persistent_cancellation_after_announcement_closes_the_child_listener() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let announced = dir.path().join("announced");
        let ready = dir.path().join("ready");
        let run_dir = dir.path().join("run");
        let port = resolve_requested_port(0).unwrap();
        write_persistent_test_server(&server, &announced, &ready);
        let runnable = persistent_runnable(dir.path(), &server, port);
        let group = Mutex::new(None);

        let started = start_persistent(runnable, &run_dir, || {
            if let Some((_, active_group)) =
                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
            {
                *group.lock().unwrap() = Some(active_group);
            }
            announced.is_file()
        })
        .unwrap();

        assert!(matches!(
            started,
            PersistentStart::Interrupted(StartupInterruption::Cancelled)
        ));
        let group = group.into_inner().unwrap().expect("owned process group");
        assert!(!process_group_exists(group).unwrap());
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(!run_dir.join("foreground.json").exists());
        assert!(
            !ready.exists(),
            "readiness must not race past announcement cancellation"
        );
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn persistent_cancellation_after_readiness_cleans_every_owned_resource() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let announced = dir.path().join("announced");
        let ready = dir.path().join("ready");
        let run_dir = dir.path().join("run");
        let port = resolve_requested_port(0).unwrap();
        write_persistent_test_server(&server, &announced, &ready);
        let runnable = persistent_runnable(dir.path(), &server, port);
        let group = Mutex::new(None);

        let started = start_persistent(runnable, &run_dir, || {
            if let Some((_, active_group)) =
                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
            {
                *group.lock().unwrap() = Some(active_group);
            }
            ready.is_file()
        })
        .unwrap();

        assert!(matches!(
            started,
            PersistentStart::Interrupted(StartupInterruption::Cancelled)
        ));
        assert!(announced.is_file());
        assert!(ready.is_file());
        let group = group.into_inner().unwrap().expect("owned process group");
        assert!(!process_group_exists(group).unwrap());
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(!run_dir.join("foreground.json").exists());
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn persistent_publication_carries_the_exact_whole_fingerprint() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let announced = dir.path().join("announced");
        let ready = dir.path().join("ready");
        let run_dir = dir.path().join("run");
        write_persistent_test_server(&server, &announced, &ready);
        let runnable = persistent_runnable(dir.path(), &server, 0);
        let expected_fingerprint = serde_json::to_value(runnable.fingerprint()).unwrap();

        let started = start_persistent(runnable, &run_dir, || false).unwrap();
        let mut server = match started {
            PersistentStart::Ready(server) => server,
            PersistentStart::Stopped(exit) => panic!("persistent server stopped: {exit:?}"),
            PersistentStart::Interrupted(interruption) => {
                panic!("persistent server was interrupted: {interruption:?}")
            }
        };
        let lease: serde_json::Value =
            serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap())
                .unwrap();

        assert_eq!(lease["version"], 2);
        assert_eq!(lease["owner_mode"], "persistent_app");
        assert_eq!(lease["fingerprint"], expected_fingerprint);
        assert_eq!(lease["fingerprint"]["sleep_policy"], 300);
        assert_eq!(lease["model_id"], lease["fingerprint"]["model_id"]);

        server.terminate().unwrap();
        assert!(!run_dir.join("foreground.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn persistent_cancellation_at_mtp_retry_never_spawns_the_primary() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let argv = dir.path().join("argv");
        let primary = dir.path().join("primary");
        let run_dir = dir.path().join("run");
        write_executable_script(
            &server,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) exit 42 ;;\nesac\nprintf primary > '{}'\nwhile :; do sleep 1; done\n",
                argv.display(),
                primary.display(),
            )
            .as_bytes(),
        );
        let runnable = persistent_mtp_runnable(dir.path(), &server, 0);
        let saw_child = AtomicBool::new(false);
        let group = Mutex::new(None);

        let started = start_persistent(runnable, &run_dir, || {
            if let Some((_, active_group)) =
                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
            {
                saw_child.store(true, Ordering::SeqCst);
                *group.lock().unwrap() = Some(active_group);
                false
            } else {
                saw_child.load(Ordering::SeqCst) && argv.is_file()
            }
        })
        .unwrap();

        assert!(matches!(
            started,
            PersistentStart::Interrupted(StartupInterruption::Cancelled)
        ));
        let argv = std::fs::read_to_string(&argv).unwrap();
        assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
        assert_eq!(
            argv.lines()
                .filter(|line| *line == "--spec-draft-model")
                .count(),
            1
        );
        assert!(!primary.exists());
        let group = group.into_inner().unwrap().expect("owned process group");
        assert!(!process_group_exists(group).unwrap());
        assert!(!run_dir.join("foreground.json").exists());
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn persistent_mtp_fallback_transforms_fingerprint_and_keeps_sleep_policy() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let argv = dir.path().join("argv");
        let announced = dir.path().join("announced");
        let ready = dir.path().join("ready");
        let run_dir = dir.path().join("run");
        write_mtp_persistent_test_server(&server, &argv, &announced, &ready);
        let runnable = persistent_mtp_runnable(dir.path(), &server, 0);

        let started = start_persistent(runnable, &run_dir, || false).unwrap();
        let mut server = match started {
            PersistentStart::Ready(server) => server,
            PersistentStart::Stopped(exit) => panic!("persistent MTP fallback stopped: {exit:?}"),
            PersistentStart::Interrupted(interruption) => {
                panic!("persistent MTP fallback was interrupted: {interruption:?}")
            }
        };
        let port = server.port();

        assert_eq!(
            server.fingerprint().effective_profile(),
            EffectiveProfile::PrimaryOnly
        );
        assert!(server.fingerprint().draft().is_none());
        assert_eq!(server.fingerprint().sleep_policy(), Some(300));
        let lease: serde_json::Value =
            serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap())
                .unwrap();
        assert_eq!(lease["owner_mode"], "persistent_app");
        assert_eq!(
            lease["fingerprint"],
            serde_json::to_value(server.fingerprint()).unwrap()
        );
        assert_eq!(lease["fingerprint"]["effective_profile"], "primary_only");
        assert_eq!(lease["fingerprint"]["draft"], serde_json::Value::Null);
        assert_eq!(lease["fingerprint"]["sleep_policy"], 300);
        assert!(server.poll().unwrap().is_none());
        let argv = std::fs::read_to_string(&argv).unwrap();
        let argv = argv.lines().collect::<Vec<_>>();
        assert_eq!(
            argv.iter()
                .filter(|argument| **argument == "--sleep-idle-seconds")
                .count(),
            2,
            "both persistent MTP attempts need exactly one sleep policy"
        );
        for index in argv
            .iter()
            .enumerate()
            .filter_map(|(index, argument)| (*argument == "--sleep-idle-seconds").then_some(index))
        {
            assert_eq!(argv[index + 1], "300");
        }
        assert_eq!(
            argv.iter()
                .filter(|argument| **argument == "--spec-draft-model")
                .count(),
            1
        );
        server.terminate().unwrap();
        drop(server);

        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(!run_dir.join("foreground.json").exists());
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }

    #[test]
    fn ready_line_displays_the_port_before_the_model_id() {
        let line = ready_line(58922, "gemma-4-12b-it-qat-ud-q4-k-xl");

        assert!(line.contains("http://127.0.0.1:58922"), "{line}");
        assert!(
            line.contains("(model gemma-4-12b-it-qat-ud-q4-k-xl)"),
            "{line}"
        );
        assert!(!line.contains("127.0.0.1:gemma-"), "{line}");
        assert!(!line.contains("(model 58922)"), "{line}");
    }

    #[test]
    fn termination_signal_at_fallback_boundary_stops_without_retrying() {
        let stopped = stopped_for_signal(&|| Some(libc::SIGINT));

        assert!(matches!(
            stopped,
            Some(ForegroundStart::Stopped(ServerExit { code: 130, .. }))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn argv_is_exact_for_the_known_mtp_profile() {
        let launch = Launch {
            server: PathBuf::from("/servers/llama-server"),
            model: PathBuf::from("/models/model.gguf"),
            id: "gemma4".into(),
            requested_port: 1234,
            ctx: 8192,
            profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
            policy: LaunchPolicy::Foreground,
        };

        assert_eq!(
            build_args(&launch, 1234),
            [
                "--model",
                "/models/model.gguf",
                "--alias",
                "gemma4",
                "--host",
                "127.0.0.1",
                "--cors-origins",
                "localhost",
                "--no-ui",
                "--port",
                "1234",
                "--ctx-size",
                "8192",
                "--n-gpu-layers",
                "all",
                "--fit",
                "off",
                "--jinja",
                "--reasoning",
                "off",
                "--spec-draft-model",
                "/models/draft.gguf",
                "--spec-type",
                "draft-mtp",
                "--spec-draft-n-max",
                "4",
                "--n-gpu-layers-draft",
                "all",
            ]
            .map(OsStr::new)
        );
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
                "--model\n/models/model.gguf\n--alias\ndemo\n--host\n127.0.0.1\n--cors-origins\nlocalhost\n--no-ui\n--port\n{port}\n--ctx-size\n8192\n--n-gpu-layers\n99\n--jinja\n--reasoning\noff\n"
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
    fn managed_server_publishes_and_clears_its_runtime_lease() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let run_dir = dir.path().join("run");
        let (port, http) = serve_models("demo");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n"
            )
            .as_bytes(),
        );
        let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
        let launch = Launch::generic(
            &server_path,
            Path::new("/models/model.gguf"),
            "demo",
            port,
            8192,
        );

        let outcome =
            OwnedServer::start_with_ownership(&launch, Duration::from_secs(2), ownership, || None)
                .unwrap();
        let mut server = match outcome {
            StartOutcome::Ready(server) => server,
            _ => panic!("server did not become ready"),
        };
        http.join().unwrap();

        let lease: serde_json::Value =
            serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap())
                .unwrap();
        assert_eq!(lease["version"], 2);
        assert_eq!(lease["owner_mode"], "foreground");
        assert_eq!(lease["fingerprint"], serde_json::Value::Null);
        server.terminate().unwrap();
        assert!(!run_dir.join("foreground.json").exists());
    }

    #[test]
    fn mtp_draft_child_diagnostics_never_enter_debug_structured_output() {
        let sensitive_markers = [
            "LOXA_TEST_PROMPT_MARKER",
            "LOXA_TEST_RESPONSE_MARKER",
            "LOXA_TEST_TOKEN_MARKER",
            "LOXA_TEST_AUTHORIZATION_MARKER",
            "LOXA_TEST_BODY_MARKER",
        ];
        let diagnostic = format!(
            "prompt={} response={} token={} authorization={} body={}",
            sensitive_markers[0],
            sensitive_markers[1],
            sensitive_markers[2],
            sensitive_markers[3],
            sensitive_markers[4],
        );
        let launch = Launch {
            server: PathBuf::from("/servers/llama-server"),
            model: PathBuf::from("/models/model.gguf"),
            id: "demo".into(),
            requested_port: 1234,
            ctx: 8192,
            profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
            policy: LaunchPolicy::Foreground,
        };
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || SharedLogWriter(writer.clone()))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            report_mtp_draft_start_failure(&launch, "exited", Some(&diagnostic));
        });

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        let events = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();

        for marker in sensitive_markers {
            assert!(
                !output.contains(marker),
                "debug structured output leaked {marker}"
            );
        }

        let failure = events
            .iter()
            .find(|event| event["event"] == "gemma_mtp_draft_start_failed")
            .expect("MTP failure classification event");
        assert_eq!(failure["outcome"], "exited");

        let diagnostic = events
            .iter()
            .find(|event| event["event"] == "gemma_mtp_draft_start_diagnostic")
            .expect("MTP diagnostic-presence event");
        assert_eq!(diagnostic["diagnostic_present"], true);
        assert!(diagnostic.get("diagnostic").is_none(), "{diagnostic}");
    }

    #[cfg(unix)]
    #[test]
    fn mtp_start_failure_retries_once_with_the_primary_and_clears_its_lease() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let argv = dir.path().join("argv");
        let run_dir = dir.path().join("run");
        let (port, http) = serve_models("demo");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'draft startup failed\\n' >&2; exit 42 ;;\nesac\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n",
                argv.display()
            )
            .as_bytes(),
        );
        let launch = test_mtp_launch(&server_path, port);

        let started = start_foreground_with_signal(&launch, &run_dir, || None).unwrap();
        let mut server = match started {
            ForegroundStart::Ready(server) => server,
            ForegroundStart::Stopped(exit) => panic!("MTP did not retry: {exit:?}"),
        };
        http.join().unwrap();

        let argv = std::fs::read_to_string(&argv).unwrap();
        assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 2);
        assert_eq!(
            argv.lines()
                .filter(|line| *line == "--spec-draft-model")
                .count(),
            1
        );
        assert!(run_dir.join("foreground.json").is_file());
        server.terminate().unwrap();
        assert!(!run_dir.join("foreground.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn mtp_start_error_does_not_spawn_a_primary_retry() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let argv = dir.path().join("argv");
        let primary = dir.path().join("primary");
        let run_dir = dir.path().join("run");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'listening on http://0.0.0.0:43123\\n' >&2; while :; do sleep 1; done ;;\n  *) printf primary > '{}'; exit 99 ;;\nesac\n",
                argv.display(),
                primary.display(),
            )
            .as_bytes(),
        );
        let launch = test_mtp_launch(&server_path, 43123);

        let error = match start_foreground_with_signal(&launch, &run_dir, || None) {
            Err(error) => error,
            Ok(_) => panic!("MTP startup error unexpectedly retried the primary model"),
        };

        assert!(error.contains("exact loopback HTTP endpoint"), "{error}");
        let argv = std::fs::read_to_string(&argv).unwrap();
        assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
        assert!(!primary.exists());
        assert!(!run_dir.join("foreground.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn process_termination_signal_during_mtp_startup_never_retries() {
        let _lock = process_test_lock();
        let _termination_signal = reset_process_termination_signal_for_test();
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let argv = dir.path().join("argv");
        let marker = dir.path().join("started");
        let run_dir = dir.path().join("run");
        write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf started > '{}'\nwhile :; do sleep 1; done\n",
                argv.display(),
                marker.display(),
            )
            .as_bytes(),
        );
        let launch = test_mtp_launch(&server_path, 43123);

        assert_eq!(process_termination_signal(), None);
        let started = start_foreground_with(&launch, &run_dir, || {
            if marker.exists() {
                PROCESS_TERMINATION_SIGNAL.store(libc::SIGINT, Ordering::SeqCst);
            }
            process_termination_signal()
        })
        .unwrap();

        assert!(matches!(
            started,
            ForegroundStart::Stopped(ServerExit { code: 130, .. })
        ));
        let argv = std::fs::read_to_string(&argv).unwrap();
        assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
        assert_eq!(
            argv.lines()
                .filter(|line| *line == "--spec-draft-model")
                .count(),
            1
        );
        assert!(!run_dir.join("foreground.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn startup_timeout_and_explicit_port_mismatch_clean_the_owned_group() {
        let _lock = process_test_lock();
        {
            let dir = tempdir().unwrap();
            let server_path = dir.path().join("server");
            write_executable_script(&server_path, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
            let (group_sender, group_receiver) = mpsc::sync_channel(1);
            let (release_sender, release) = mpsc::sync_channel(1);

            std::thread::scope(|scope| {
                let start = scope.spawn(move || {
                    OwnedServer::start(
                        &server_path,
                        Path::new("/models/model.gguf"),
                        "demo",
                        0,
                        1,
                        Duration::ZERO,
                        || {
                            let (_, group) =
                                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
                                    .expect("server did not publish its owned process group");
                            group_sender.send(group).unwrap();
                            release.recv().unwrap();
                            None
                        },
                    )
                });
                let group = group_receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server did not reach startup polling");
                release_sender.send(()).unwrap();
                let error = match start.join().unwrap() {
                    Err(error) => error,
                    Ok(_) => panic!("server unexpectedly started"),
                };
                assert!(error.contains("did not announce"), "{error}");
                assert!(!process_group_exists(group).unwrap());
            });
        }

        {
            let dir = tempdir().unwrap();
            let server_path = dir.path().join("server");
            let release_path = dir.path().join("release");
            write_executable_script(
                &server_path,
                format!(
                    "#!/bin/sh\nwhile [ ! -f '{}' ]; do :; done\nprintf 'listening on http://127.0.0.1:43123\\n' >&2\nwhile :; do sleep 1; done\n",
                    release_path.display(),
                )
                .as_bytes(),
            );
            let (group_sender, group_receiver) = mpsc::sync_channel(1);
            let (release_sender, release) = mpsc::sync_channel(1);

            std::thread::scope(|scope| {
                let start = scope.spawn(move || {
                    let first_poll = AtomicBool::new(true);
                    OwnedServer::start(
                        &server_path,
                        Path::new("/models/model.gguf"),
                        "demo",
                        43124,
                        1,
                        Duration::from_secs(5),
                        || {
                            if first_poll.swap(false, Ordering::SeqCst) {
                                let (_, group) =
                                    unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
                                        .expect("server did not publish its owned process group");
                                group_sender.send(group).unwrap();
                                release.recv().unwrap();
                            }
                            None
                        },
                    )
                });
                let group = group_receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server did not reach startup polling");
                std::fs::write(&release_path, b"release").unwrap();
                release_sender.send(()).unwrap();
                let error = match start.join().unwrap() {
                    Err(error) => error,
                    Ok(_) => panic!("server unexpectedly started"),
                };
                assert!(error.contains("expected 43124"), "{error}");
                assert!(!process_group_exists(group).unwrap());
            });
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
        let mut server = ForegroundServer { server };
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
    fn managed_launch_admission_keeps_exact_probe_without_publishing_inventory_evidence() {
        let _lock = process_test_lock();
        let dir = tempdir().unwrap();
        let managed = dir.path().join("managed");
        write_version_script(&managed, "version: 10121 (555881ebc)", 0);

        assert_eq!(
            discover_from_process(None, &managed, &LaunchProfile::gemma4_mtp(None)).unwrap(),
            managed
        );
        assert!(
            !managed.with_extension("qualification.json").exists(),
            "launch admission must not publish managed runtime inventory evidence"
        );
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

        let status = run(
            &server,
            &child_ready,
            group_file.to_str().unwrap(),
            0,
            1,
            &dir.path().join("run"),
        )
        .unwrap();
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
