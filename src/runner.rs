use crate::runtime_fingerprint::{
    EffectiveProfile, RuntimeFingerprint, PERSISTENT_SLEEP_IDLE_SECONDS,
};
use crate::runtime_identity::RuntimeIdentity;
use crate::ui;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(all(test, unix))]
use std::sync::atomic::AtomicI32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod discovery;
mod signal;
pub(crate) use discovery::discover_from_process;
#[cfg(all(test, target_os = "macos"))]
use discovery::probe_validated_version_with_timeout_for_test;
#[allow(unused_imports)]
pub(crate) use discovery::validate_managed_server;
pub use discovery::{discover_server, validate_managed_runtime};
#[cfg(test)]
use discovery::{
    managed_version_first_line, probe_validated_version, probe_version_with_timeout,
    VERSION_PROBE_TIMEOUT,
};
use signal::{
    activate_server, clear_server_starting, deactivate_server, install_termination_watcher,
    mark_server_starting, process_termination_signal,
};
#[cfg(all(test, unix))]
use signal::{
    pack_server_identity, reset_process_termination_signal_for_test, unpack_server_identity,
    ACTIVE_SERVER, PROCESS_TERMINATION_SIGNAL,
};

#[cfg(unix)]
type PreparedRuntimeGuard = Option<crate::runtime_bundle::PreparedRuntime>;
#[cfg(not(unix))]
type PreparedRuntimeGuard = ();

#[cfg(unix)]
fn no_prepared_runtime_guard() -> PreparedRuntimeGuard {
    None
}

#[cfg(not(unix))]
fn no_prepared_runtime_guard() -> PreparedRuntimeGuard {}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_MODELS_BODY: usize = 1024 * 1024;
const MAX_DIAGNOSTIC_TAIL: usize = 4096;
const MAX_ANNOUNCEMENT_LINE: usize = 8192;
const MAX_PENDING_ANNOUNCEMENTS: usize = 64;
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

    fn required_version(&self, runtime_identity: RuntimeIdentity) -> Option<&str> {
        match self {
            Self::Generic => None,
            Self::Gemma4Mtp {
                #[cfg(test)]
                test_required_version,
                ..
            } => {
                #[cfg(test)]
                {
                    test_required_version
                        .as_deref()
                        .or(Some(runtime_identity.version_line()))
                }
                #[cfg(not(test))]
                {
                    Some(runtime_identity.version_line())
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
    pub(crate) managed_runtime: Option<ValidatedManagedRuntime>,
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
            managed_runtime: None,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedManagedRuntime {
    source_server: PathBuf,
    #[cfg(unix)]
    prepared: Option<crate::runtime_bundle::PreparedRuntime>,
}

impl ValidatedManagedRuntime {
    fn path(source_server: PathBuf) -> Self {
        Self {
            source_server,
            #[cfg(unix)]
            prepared: None,
        }
    }

    #[cfg(unix)]
    fn bundled(source_server: PathBuf, prepared: crate::runtime_bundle::PreparedRuntime) -> Self {
        Self {
            source_server,
            prepared: Some(prepared),
        }
    }

    pub fn source_server(&self) -> &Path {
        &self.source_server
    }

    #[cfg(all(test, target_os = "macos"))]
    pub(crate) fn execution_server(&self) -> PathBuf {
        #[cfg(unix)]
        if let Some(prepared) = &self.prepared {
            return prepared.execution_server();
        }
        self.source_server.clone()
    }

    fn command(&self) -> Command {
        #[cfg(unix)]
        if let Some(prepared) = &self.prepared {
            return prepared.command();
        }
        Command::new(&self.source_server)
    }

    #[cfg(unix)]
    fn process_guard(&self) -> PreparedRuntimeGuard {
        self.prepared.clone()
    }

    #[cfg(not(unix))]
    fn process_guard(&self) -> PreparedRuntimeGuard {}
}

impl Launch {
    fn server_command(&self) -> Command {
        self.managed_runtime.as_ref().map_or_else(
            || Command::new(&self.server),
            ValidatedManagedRuntime::command,
        )
    }

    fn managed_source_server(&self) -> Option<&Path> {
        self.managed_runtime
            .as_ref()
            .map(ValidatedManagedRuntime::source_server)
    }
}

#[derive(Clone, Copy)]
enum ChildTerminationMode {
    Graceful,
    Immediate,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum PersistentSignalPolicy {
    /// Register this child with the CLI process-exit watcher.
    ForegroundExit,
    /// Leave process-exit handling to the long-lived caller; this path never
    /// installs the watcher or registers the child with it.
    CallerManaged,
}

struct ChildProcessGuard {
    child: Option<Child>,
    group: i32,
    active_server: bool,
    termination: ChildTerminationMode,
    prepared: PreparedRuntimeGuard,
    runtime: Option<crate::runtime::RuntimeChildOwnership>,
}

impl ChildProcessGuard {
    fn spawn(
        command: &mut Command,
        signal_policy: PersistentSignalPolicy,
        termination: ChildTerminationMode,
        prepared: PreparedRuntimeGuard,
        mut runtime: Option<crate::runtime::RuntimeChildOwnership>,
    ) -> Result<Self, String> {
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let group = match i32::try_from(child.id()) {
            Ok(group) => group,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("invalid child process id".into());
            }
        };
        if let Some(runtime) = runtime.as_mut() {
            runtime.child_spawned();
        }
        let active_server = signal_policy == PersistentSignalPolicy::ForegroundExit;
        if active_server {
            activate_server(child.id(), group);
        }
        #[cfg(all(test, unix))]
        LAST_GUARDED_GROUP.store(group, Ordering::SeqCst);
        Ok(Self {
            child: Some(child),
            group,
            active_server,
            termination,
            prepared,
            runtime,
        })
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("guarded child is present").id()
    }

    fn active_id(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn group(&self) -> i32 {
        self.group
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("guarded child is present")
    }

    fn terminate(&mut self) -> Result<(), String> {
        if let Some(child) = self.child.as_mut() {
            let pid = child.id();
            match self.termination {
                ChildTerminationMode::Graceful => terminate_owned_group(child, self.group)?,
                ChildTerminationMode::Immediate => terminate_probe(child, self.group)?,
            }
            if self.active_server {
                deactivate_server(pid, self.group);
            }
            self.child.take();
        }
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.clear()?;
        }
        Ok(())
    }

    fn runtime_mut(&mut self) -> Option<&mut crate::runtime::RuntimeChildOwnership> {
        self.runtime.as_mut()
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        if self.terminate().is_err() && self.child.is_some() {
            #[cfg(unix)]
            if let Some(prepared) = &self.prepared {
                if prepared.abandon().is_ok() && self.active_server {
                    if let Some(child) = self.child.as_ref() {
                        deactivate_server(child.id(), self.group);
                    }
                    self.active_server = false;
                }
            }
        }
    }
}

#[cfg(unix)]
fn terminate_probe(child: &mut Child, group: i32) -> Result<(), String> {
    crate::runtime::terminate_process_group_immediately(child, group)
}

#[cfg(not(unix))]
fn terminate_probe(child: &mut Child, _group: i32) -> Result<(), String> {
    let _ = child.kill();
    let _ = child.wait().map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
enum ReaderSpawnFault {
    Fail,
    Panic,
}

#[cfg(test)]
static READER_SPAWN_FAULT: std::sync::Mutex<Option<ReaderSpawnFault>> = std::sync::Mutex::new(None);

#[cfg(all(test, unix))]
static LAST_GUARDED_GROUP: AtomicI32 = AtomicI32::new(0);

#[cfg(test)]
struct ReaderSpawnFaultReset;

#[cfg(test)]
impl Drop for ReaderSpawnFaultReset {
    fn drop(&mut self) {
        *READER_SPAWN_FAULT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
fn install_reader_spawn_fault_for_test(fault: ReaderSpawnFault) -> ReaderSpawnFaultReset {
    #[cfg(unix)]
    LAST_GUARDED_GROUP.store(0, Ordering::SeqCst);
    *READER_SPAWN_FAULT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(fault);
    ReaderSpawnFaultReset
}

#[cfg(test)]
fn inject_reader_spawn_fault_for_test() -> Result<(), String> {
    let fault = READER_SPAWN_FAULT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    let Some(fault) = fault else {
        return Ok(());
    };
    match fault {
        ReaderSpawnFault::Fail => Err("injected output reader spawn failure".into()),
        ReaderSpawnFault::Panic => panic!("injected output reader spawn panic"),
    }
}

fn spawn_reader_thread<T, F>(
    name: &'static str,
    read: F,
) -> Result<std::thread::JoinHandle<T>, String>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    #[cfg(test)]
    inject_reader_spawn_fault_for_test()?;
    std::thread::Builder::new()
        .name(name.into())
        .spawn(read)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
fn kill_owner_after_spawn_before_lease_for_test() {
    let (Some(after), Some(ready)) = (
        std::env::var_os("LOXA_TEST_POST_SPAWN_KILL_AFTER"),
        std::env::var_os("LOXA_TEST_POST_SPAWN_KILL_READY"),
    ) else {
        return;
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !Path::new(&after).is_file() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        Path::new(&after).is_file(),
        "post-spawn child witness was not published"
    );
    std::fs::write(ready, b"prelease").expect("pre-lease kill witness could not be published");
    // SAFETY: this test-only subprocess deliberately models abrupt owner death.
    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    unreachable!("SIGKILL returned in the pre-lease owner subprocess");
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
            managed_runtime: None,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupInterruption {
    Cancelled,
}

#[derive(Debug)]
pub(crate) enum PersistentStartError {
    Conflict,
    Failed(String),
}

impl From<String> for PersistentStartError {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

impl From<crate::runtime::RuntimeOwnershipAcquireError> for PersistentStartError {
    fn from(error: crate::runtime::RuntimeOwnershipAcquireError) -> Self {
        match error {
            crate::runtime::RuntimeOwnershipAcquireError::Conflict => Self::Conflict,
            crate::runtime::RuntimeOwnershipAcquireError::Failed(message) => Self::Failed(message),
        }
    }
}

pub(crate) enum PersistentStart {
    Ready(Box<PersistentServer>),
    Stopped(ServerExit),
    Interrupted(StartupInterruption),
    CleanupFailed(Box<PersistentServer>),
}

pub(crate) struct ForegroundServer {
    server: Box<OwnedServer>,
}

pub(crate) struct PersistentServer {
    server: Box<OwnedServer>,
    runnable: crate::runnable::Runnable,
}

pub(crate) fn start_foreground(launch: &Launch, run_dir: &Path) -> Result<ForegroundStart, String> {
    install_termination_watcher(run_dir)?;
    start_foreground_with(launch, run_dir, process_termination_signal)
}

pub(crate) fn start_persistent<F>(
    runnable: crate::runnable::Runnable,
    run_dir: &Path,
    cancelled: F,
) -> Result<PersistentStart, PersistentStartError>
where
    F: Fn() -> bool,
{
    if cancelled() {
        return Ok(persistent_interrupted());
    }
    install_termination_watcher(run_dir).map_err(PersistentStartError::Failed)?;
    let ownership = crate::runtime::RuntimeOwnership::acquire_persistent(run_dir)?;
    start_persistent_with_ownership(
        runnable,
        &ownership,
        cancelled,
        PersistentSignalPolicy::ForegroundExit,
    )
}

pub(crate) fn start_persistent_with_ownership<F>(
    mut runnable: crate::runnable::Runnable,
    ownership: &crate::runtime::RuntimeOwnership,
    cancelled: F,
    signal_policy: PersistentSignalPolicy,
) -> Result<PersistentStart, PersistentStartError>
where
    F: Fn() -> bool,
{
    // The caller keeps this common owner across attempts. Each attempt borrows
    // its sole child slot and returns it only after verified child cleanup.
    let launch_started = Instant::now();
    tracing::info!(
        event = "server_starting",
        model_id = %runnable.launch().id,
        requested_port = runnable.launch().requested_port,
        context_size = runnable.launch().ctx
    );
    if cancelled() {
        return Ok(persistent_interrupted());
    }
    match start_persistent_attempt(&runnable, ownership, signal_policy, &cancelled)? {
        StartOutcome::Ready(server) => {
            finish_persistent_ready(server, runnable, launch_started, false, &cancelled)
                .map_err(PersistentStartError::from)
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
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            match start_persistent_attempt(&runnable, ownership, signal_policy, &cancelled)? {
                StartOutcome::Ready(server) => {
                    finish_persistent_ready(server, runnable, launch_started, true, &cancelled)
                        .map_err(PersistentStartError::from)
                }
                StartOutcome::Exited(exit) => Ok(PersistentStart::Stopped(exit)),
                StartOutcome::Interrupted(interruption) => {
                    Ok(PersistentStart::Interrupted(interruption))
                }
                StartOutcome::CleanupFailed(server) => {
                    Ok(persistent_cleanup_failed(server, runnable))
                }
                StartOutcome::Signaled(_) => Err(PersistentStartError::Failed(
                    "persistent startup returned an invalid signal interruption".into(),
                )),
            }
        }
        StartOutcome::Interrupted(interruption) => Ok(PersistentStart::Interrupted(interruption)),
        StartOutcome::CleanupFailed(server) => Ok(persistent_cleanup_failed(server, runnable)),
        StartOutcome::Signaled(_) => Err(PersistentStartError::Failed(
            "persistent startup returned an invalid signal interruption".into(),
        )),
    }
}

fn persistent_interrupted() -> PersistentStart {
    PersistentStart::Interrupted(StartupInterruption::Cancelled)
}

fn persistent_cleanup_failed(
    server: Box<OwnedServer>,
    runnable: crate::runnable::Runnable,
) -> PersistentStart {
    PersistentStart::CleanupFailed(Box::new(PersistentServer { server, runnable }))
}

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
        if server.terminate().is_err() {
            return Ok(PersistentStart::CleanupFailed(Box::new(PersistentServer {
                server,
                runnable,
            })));
        }
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
        Ok(StartOutcome::CleanupFailed(_)) => {
            Err("foreground startup returned an invalid cleanup failure".into())
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

fn start_persistent_attempt<F>(
    runnable: &crate::runnable::Runnable,
    ownership: &crate::runtime::RuntimeOwnership,
    signal_policy: PersistentSignalPolicy,
    cancelled: &F,
) -> Result<StartOutcome, String>
where
    F: Fn() -> bool,
{
    let child_ownership = ownership.reserve_child()?;
    OwnedServer::start_with_persistent_ownership(
        runnable.launch(),
        runnable.fingerprint(),
        STARTUP_TIMEOUT,
        child_ownership,
        signal_policy,
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
        StartOutcome::CleanupFailed(_) => {
            Err("foreground startup returned an invalid cleanup failure".into())
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

type AnnouncementOutput = (mpsc::SyncSender<Result<u16, String>>, Arc<AtomicBool>);

fn spawn_output_reader<R>(
    mut reader: R,
    announcement_output: Option<AnnouncementOutput>,
) -> Result<std::thread::JoinHandle<Result<Vec<u8>, String>>, String>
where
    R: std::io::Read + Send + 'static,
{
    spawn_reader_thread("loxa-server-output", move || {
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
    CleanupFailed(Box<OwnedServer>),
}

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

enum RequestedStartOutcome {
    Continue,
    Completed(StartOutcome),
    CleanupFailed,
}

fn requested_start_outcome<F>(
    server: &mut OwnedServer,
    stop: &F,
) -> Result<RequestedStartOutcome, String>
where
    F: Fn() -> Option<StartupStop>,
{
    let Some(stop) = stop() else {
        return Ok(RequestedStartOutcome::Continue);
    };
    match server.terminate() {
        Ok(()) => Ok(RequestedStartOutcome::Completed(stop.into())),
        Err(_cleanup) if matches!(stop, StartupStop::Interrupted(_)) => {
            Ok(RequestedStartOutcome::CleanupFailed)
        }
        Err(cleanup) => Err(cleanup),
    }
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
    child: Option<ChildProcessGuard>,
    #[cfg(test)]
    group: i32,
    port: u16,
    announcements: mpsc::Receiver<Result<u16, String>>,
    announcement_overflow: Arc<AtomicBool>,
    announced_port: Option<u16>,
    stdout_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stdout_tail: Vec<u8>,
    stderr_tail: Vec<u8>,
    retain_cleanup_failure: bool,
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
        let child_ownership = runtime.reserve_child()?;
        Self::start_inner(
            launch,
            timeout,
            Some((
                child_ownership,
                crate::runtime::RuntimeLeasePublication::Foreground,
            )),
            || signal().map(StartupStop::Signal),
        )
    }

    fn start_with_persistent_ownership<F>(
        launch: &Launch,
        fingerprint: &crate::runtime_fingerprint::RuntimeFingerprint,
        timeout: Duration,
        runtime: crate::runtime::RuntimeChildOwnership,
        signal_policy: PersistentSignalPolicy,
        cancelled: &F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> bool,
    {
        Self::start_inner_with_policy(
            launch,
            timeout,
            Some((
                runtime,
                crate::runtime::RuntimeLeasePublication::PersistentApp(fingerprint),
            )),
            signal_policy,
            || cancelled().then_some(StartupStop::Interrupted(StartupInterruption::Cancelled)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeChildOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        Self::start_inner_with_policy(
            launch,
            timeout,
            runtime,
            PersistentSignalPolicy::ForegroundExit,
            stop,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner_with_policy<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeChildOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        signal_policy: PersistentSignalPolicy,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        let requested_port = resolve_requested_port(launch.requested_port)?;
        let client = readiness_client()?;
        let mut command = launch.server_command();
        let prepared = launch.managed_runtime.as_ref().map_or_else(
            no_prepared_runtime_guard,
            ValidatedManagedRuntime::process_guard,
        );
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
        if signal_policy == PersistentSignalPolicy::ForegroundExit {
            mark_server_starting();
        }
        let retain_cleanup_failure =
            runtime.is_some() && launch.policy == LaunchPolicy::PersistentApp;
        let (runtime, publication) = match runtime {
            Some((runtime, publication)) => (Some(runtime), Some(publication)),
            None => (None, None),
        };
        let child = match ChildProcessGuard::spawn(
            &mut command,
            signal_policy,
            ChildTerminationMode::Graceful,
            prepared,
            runtime,
        ) {
            Ok(child) => child,
            Err(error) => {
                if signal_policy == PersistentSignalPolicy::ForegroundExit {
                    clear_server_starting();
                }
                return Err(error);
            }
        };
        #[cfg(test)]
        kill_owner_after_spawn_before_lease_for_test();
        let group = child.group();
        let (announcement_sender, announcements) = mpsc::sync_channel(MAX_PENDING_ANNOUNCEMENTS);
        let announcement_overflow = Arc::new(AtomicBool::new(false));
        let mut owned = Self {
            child: Some(child),
            #[cfg(test)]
            group,
            port: 0,
            announcements,
            announcement_overflow,
            announced_port: None,
            stdout_reader: None,
            stderr_reader: None,
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
            retain_cleanup_failure,
        };
        let stdout = match owned.child_mut().stdout.take() {
            Some(stdout) => stdout,
            None => return owned.fail_start("failed to capture llama-server stdout".into()),
        };
        let stderr = match owned.child_mut().stderr.take() {
            Some(stderr) => stderr,
            None => return owned.fail_start("failed to capture llama-server stderr".into()),
        };
        owned.stdout_reader = match spawn_output_reader(stdout, None) {
            Ok(reader) => Some(reader),
            Err(error) => return owned.fail_start(error),
        };
        owned.stderr_reader = match spawn_output_reader(
            stderr,
            Some((
                announcement_sender,
                Arc::clone(&owned.announcement_overflow),
            )),
        ) {
            Ok(reader) => Some(reader),
            Err(error) => return owned.fail_start(error),
        };
        let child_pid = owned.child.as_ref().expect("owned child is present").id();
        if let Some(runtime) = owned
            .child
            .as_mut()
            .and_then(ChildProcessGuard::runtime_mut)
        {
            let publication = publication.expect("owned runtime publication is present");
            if let Err(error) = runtime.record(
                child_pid,
                group,
                &launch.id,
                requested_port,
                launch.managed_source_server(),
                publication,
            ) {
                return owned.fail_start(error);
            }
        }
        if launch.policy == LaunchPolicy::PersistentApp {
            match requested_start_outcome(&mut owned, &stop)? {
                RequestedStartOutcome::Continue => {}
                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                RequestedStartOutcome::CleanupFailed => {
                    return Ok(StartOutcome::CleanupFailed(Box::new(owned)))
                }
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Err(error) = owned.collect_announcements() {
                return owned.fail_start(error);
            }
            match requested_start_outcome(&mut owned, &stop)? {
                RequestedStartOutcome::Continue => {}
                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                RequestedStartOutcome::CleanupFailed => {
                    return Ok(StartOutcome::CleanupFailed(Box::new(owned)))
                }
            }
            let status = match owned.child_mut().try_wait() {
                Ok(status) => status,
                Err(error) => return owned.fail_start(error.to_string()),
            };
            if let Some(status) = status {
                let code = exit_code(status);
                if let Err(cleanup) = owned.terminate() {
                    return owned.finish_cleanup_failure(cleanup);
                }
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
                            match requested_start_outcome(&mut owned, &stop)? {
                                RequestedStartOutcome::Continue => {}
                                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                                RequestedStartOutcome::CleanupFailed => {
                                    return Ok(StartOutcome::CleanupFailed(Box::new(owned)))
                                }
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
        match self.terminate() {
            Ok(()) => Err(self.with_diagnostic(error)),
            Err(cleanup) => {
                self.finish_cleanup_failure(format!("{error}; cleanup failed: {cleanup}"))
            }
        }
    }

    fn finish_cleanup_failure(self, error: String) -> Result<StartOutcome, String> {
        if self.retain_cleanup_failure {
            tracing::warn!(event = "server_start_cleanup_failed");
            Ok(StartOutcome::CleanupFailed(Box::new(self)))
        } else {
            Err(error)
        }
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
        self.child
            .as_mut()
            .expect("owned server child is present")
            .child_mut()
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
        let Some(child) = self.child.as_mut() else {
            return Err("owned server child is no longer present".into());
        };
        if child.active_id().is_none() {
            return Err("owned server child cleanup is incomplete".into());
        }
        let Some(status) = child
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
            let pid = child.active_id();
            if let Some(pid) = pid {
                tracing::info!(event = "server_terminating", pid, port = self.port);
            }
            child.terminate()?;
            if let Some(pid) = pid {
                tracing::info!(event = "server_terminated", pid, port = self.port);
            }
        }
        self.join_output_readers()?;
        self.child.take();
        Ok(())
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

#[cfg(test)]
mod tests;
