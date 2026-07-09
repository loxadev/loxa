use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
use loxa_core::supervisor::{
    self, InterruptStatus, LogDrainingChild, ManagedChild, ManagedServer, ObservedChildExit,
    RestartPolicy, RuntimeStateRead, SpawnedServer, SupervisorError,
};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "loxa", version, about = "Measured local AI infrastructure")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    Doctor,
    Pull {
        id: String,
    },
    List,
    Rm {
        id: String,
    },
    Run {
        id: String,
        #[arg(long)]
        ctx: Option<u32>,
        #[arg(long)]
        port: Option<u16>,
    },
    Ps,
    Stop {
        target: String,
    },
}

#[derive(Clone, Debug)]
struct CliPaths {
    models_dir: PathBuf,
    state_path: PathBuf,
    logs_dir: PathBuf,
}

impl CliPaths {
    fn detect() -> Self {
        Self {
            models_dir: download::model_dir(),
            state_path: supervisor::runtime_state_path(),
            logs_dir: supervisor::runtime_logs_dir(),
        }
    }

    fn log_path(&self, id: &str, port: u16, started_at_unix_s: u64) -> PathBuf {
        self.logs_dir
            .join(format!("{id}-{port}-{started_at_unix_s}.log"))
    }
}

enum RunOutcome {
    Exit(ExitCode),
    Restart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupPoll {
    Pending,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupWaitOutcome {
    Ready,
    Interrupted,
}

struct RunSession<'a> {
    id: &'a str,
    server: &'a ManagedServer,
    log_path: &'a Path,
    state_path: &'a Path,
}

trait InterruptSource {
    fn interrupted(&self) -> bool;
}

static CTRL_C_RECEIVED: AtomicBool = AtomicBool::new(false);

fn clear_ctrl_c_received() {
    CTRL_C_RECEIVED.store(false, Ordering::SeqCst);
}

fn set_ctrl_c_received() {
    CTRL_C_RECEIVED.store(true, Ordering::SeqCst);
}

fn ctrl_c_received() -> bool {
    CTRL_C_RECEIVED.load(Ordering::SeqCst)
}

#[cfg(unix)]
extern "C" fn handle_sigint(_signal: std::ffi::c_int) {
    set_ctrl_c_received();
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    run(cli, io::stdout(), io::stderr())
}

fn run<W: Write, E: Write>(cli: Cli, mut stdout: W, mut stderr: E) -> ExitCode {
    let paths = CliPaths::detect();
    run_with_paths(cli, &paths, &mut stdout, &mut stderr)
}

fn run_with_paths<W: Write, E: Write>(
    cli: Cli,
    paths: &CliPaths,
    mut stdout: W,
    mut stderr: E,
) -> ExitCode {
    let result = match cli.command {
        Command::Doctor => print_doctor(&mut stdout),
        Command::Pull { id } => pull_model(&id, &mut stdout, &mut stderr),
        Command::List => print_list(&mut stdout),
        Command::Rm { id } => remove_model(&id, &mut stdout, &mut stderr),
        Command::Run { id, ctx, port } => {
            run_model(&id, ctx, port, paths, &mut stdout, &mut stderr)
        }
        Command::Ps => print_managed_servers(paths, &mut stdout),
        Command::Stop { target } => stop_managed_servers(&target, paths, &mut stdout, &mut stderr),
    };

    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}");
            ExitCode::from(1)
        }
    }
}

fn pull_model<W: Write, E: Write>(
    id: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let Some(entry) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    let dir = download::model_dir();
    match download::download(entry, &dir) {
        Ok(path) => {
            writeln!(stdout, "{}", path.display())?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            writeln!(stderr, "pull failed for {id}: {error}")?;
            Ok(ExitCode::from(1))
        }
    }
}

fn print_list<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    let dir = download::model_dir();
    let rows = REGISTRY
        .iter()
        .map(|entry| {
            (
                entry,
                bytes_to_gb_string(entry.size_bytes),
                model_status(entry, &dir).to_string(),
            )
        })
        .collect::<Vec<_>>();

    let id_width = rows
        .iter()
        .map(|(entry, _, _)| entry.id.len())
        .chain([2])
        .max()
        .unwrap_or(2);
    let params_width = rows
        .iter()
        .map(|(entry, _, _)| entry.params.len())
        .chain([6])
        .max()
        .unwrap_or(6);
    let quant_width = rows
        .iter()
        .map(|(entry, _, _)| entry.quant.len())
        .chain([5])
        .max()
        .unwrap_or(5);
    let size_width = rows
        .iter()
        .map(|(_, size, _)| size.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let license_width = rows
        .iter()
        .map(|(entry, _, _)| entry.license.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let status_width = rows
        .iter()
        .map(|(_, _, status)| status.len())
        .chain([6])
        .max()
        .unwrap_or(6);

    writeln!(
        stdout,
        "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
        "id",
        "params",
        "quant",
        "size GB",
        "license",
        "status",
    )?;

    for (entry, size, status) in rows {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id,
            entry.params,
            entry.quant,
            size,
            entry.license,
            status,
        )?;
    }

    Ok(ExitCode::SUCCESS)
}

fn remove_model<W: Write, E: Write>(
    id: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let Some(entry) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    let dir = download::model_dir();
    let removed = remove_model_files(entry, &dir)?;
    if removed.is_empty() {
        writeln!(stdout, "nothing present for {id}")?;
    } else {
        for path in removed {
            writeln!(stdout, "removed {}", path.display())?;
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn run_model<W: Write, E: Write>(
    id: &str,
    ctx: Option<u32>,
    port: Option<u16>,
    paths: &CliPaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let Some(_) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    ensure_runtime_state_is_mutable(&paths.state_path)?;

    let signal_guard = SignalGuard::install()?;
    let mut restart_policy = RestartPolicy::default();

    loop {
        if signal_guard.interrupted() {
            return Ok(ExitCode::from(130));
        }

        let (entry, model_path) = match supervisor::resolve_model_path(id, &paths.models_dir) {
            Ok(resolved) => resolved,
            Err(SupervisorError::ModelNotDownloaded(_)) => {
                writeln!(
                    stderr,
                    "model not downloaded for {id}; run `loxa pull {id}`"
                )?;
                return Ok(ExitCode::from(1));
            }
            Err(error) => return Err(supervisor_error_to_io(error)),
        };
        if signal_guard.interrupted() {
            return Ok(ExitCode::from(130));
        }
        let llama_server_path =
            supervisor::detect_llama_server().map_err(supervisor_error_to_io)?;
        if signal_guard.interrupted() {
            return Ok(ExitCode::from(130));
        }
        let llama_server_version =
            supervisor::llama_server_version(&llama_server_path).map_err(supervisor_error_to_io)?;
        if signal_guard.interrupted() {
            return Ok(ExitCode::from(130));
        }
        let selected_port =
            supervisor::choose_localhost_port(port).map_err(supervisor_error_to_io)?;
        let started_at_unix_s = unix_timestamp_now();
        let spec = supervisor::ServerSpec {
            entry,
            model_path,
            llama_server_path,
            port: selected_port,
            ctx_tokens: ctx.unwrap_or(supervisor::DEFAULT_CTX_TOKENS),
        };
        let log_path = paths.log_path(id, spec.port, started_at_unix_s);
        let mut child =
            supervisor::spawn_llama_server(&spec, &log_path).map_err(supervisor_error_to_io)?;
        let server = ManagedServer {
            id: id.to_string(),
            pid: child.pid(),
            port: spec.port,
            model_path: spec.model_path.clone(),
            started_at_unix_s,
            llama_server_version,
            process_start_time_unix_s: supervisor::process_start_time(child.pid()),
        };

        if signal_guard.interrupted() {
            supervisor::cleanup_after_ctrl_c(
                &mut child,
                &paths.state_path,
                server.identity(),
                supervisor::CTRL_C_GRACE_PERIOD,
            )
            .map_err(supervisor_error_to_io)?;
            let _ = child.join_log_drains();
            return Ok(ExitCode::from(130));
        }

        supervisor::persist_managed_server_or_cleanup(
            &mut child,
            &paths.state_path,
            server.clone(),
            supervisor::CTRL_C_GRACE_PERIOD,
        )
        .map_err(supervisor_error_to_io)?;

        match wait_for_startup(
            &mut child,
            &server,
            &log_path,
            &paths.state_path,
            &signal_guard,
            |child, timeout, interval| match supervisor::wait_for_health_or_exit(
                child,
                server.port,
                &log_path,
                timeout,
                interval,
            ) {
                Ok(()) => Ok(StartupPoll::Ready),
                Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                Err(error) => Err(error),
            },
        ) {
            Ok(StartupWaitOutcome::Ready) => {
                print_run_ready(stdout, &server)?;
                match supervise_running_server(
                    RunSession {
                        id,
                        server: &server,
                        log_path: &log_path,
                        state_path: &paths.state_path,
                    },
                    &mut child,
                    &signal_guard,
                    &mut restart_policy,
                    stdout,
                    stderr,
                )? {
                    RunOutcome::Exit(exit_code) => return Ok(exit_code),
                    RunOutcome::Restart => continue,
                }
            }
            Ok(StartupWaitOutcome::Interrupted) => return Ok(ExitCode::from(130)),
            Err(SupervisorError::ChildExitedEarly(log_tail)) => {
                match supervisor::decide_observed_child_exit(
                    log_tail,
                    &paths.state_path,
                    server.identity(),
                    &signal_guard,
                    &mut restart_policy,
                )
                .map_err(supervisor_error_to_io)?
                {
                    ObservedChildExit::Interrupted => return Ok(ExitCode::from(130)),
                    ObservedChildExit::Restart => {
                        writeln!(
                            stdout,
                            "llama-server exited before becoming healthy; restarting once..."
                        )?;
                        continue;
                    }
                    ObservedChildExit::Crash { log_tail } => {
                        writeln!(
                            stderr,
                            "llama-server exited before becoming healthy for {id}"
                        )?;
                        write_log_tail(stderr, &log_tail)?;
                        return Ok(ExitCode::from(1));
                    }
                }
            }
            Err(error) => {
                let _ = supervisor::cleanup_after_ctrl_c(
                    &mut child,
                    &paths.state_path,
                    server.identity(),
                    supervisor::CTRL_C_GRACE_PERIOD,
                );
                let _ = child.join_log_drains();
                return match error {
                    SupervisorError::HealthTimeout => {
                        writeln!(
                            stderr,
                            "llama-server did not become healthy within {} seconds",
                            supervisor::HEALTH_TIMEOUT.as_secs()
                        )?;
                        writeln!(stderr, "log file: {}", log_path.display())?;
                        Ok(ExitCode::from(1))
                    }
                    other => Err(supervisor_error_to_io(other)),
                };
            }
        }
    }
}

fn print_managed_servers<W: Write>(paths: &CliPaths, stdout: &mut W) -> io::Result<ExitCode> {
    let state =
        supervisor::read_runtime_state(&paths.state_path).map_err(supervisor_error_to_io)?;
    let servers = match state {
        RuntimeStateRead::Missing => {
            writeln!(stdout, "no managed sidecars")?;
            return Ok(ExitCode::SUCCESS);
        }
        RuntimeStateRead::Corrupt(message) => {
            writeln!(stdout, "managed sidecar state is corrupt: {message}")?;
            return Ok(ExitCode::SUCCESS);
        }
        RuntimeStateRead::Loaded(servers) => servers,
    };

    if servers.is_empty() {
        writeln!(stdout, "no managed sidecars")?;
        return Ok(ExitCode::SUCCESS);
    }

    let inspections = supervisor::inspect_managed_servers(&servers);
    writeln!(
        stdout,
        "id                  pid    port   status               model"
    )?;
    for inspection in inspections {
        writeln!(
            stdout,
            "{:<19} {:>6}  {:>5}  {:<19} {}",
            inspection.server.id,
            inspection.server.pid,
            inspection.server.port,
            inspection_status(&inspection),
            inspection.server.model_path.display(),
        )?;
    }

    Ok(ExitCode::SUCCESS)
}

fn stop_managed_servers<W: Write, E: Write>(
    target: &str,
    paths: &CliPaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let state =
        supervisor::read_runtime_state(&paths.state_path).map_err(supervisor_error_to_io)?;
    let servers = match state {
        RuntimeStateRead::Missing => {
            if target == "all" {
                writeln!(stdout, "no managed sidecars")?;
                return Ok(ExitCode::SUCCESS);
            }
            writeln!(stderr, "no managed sidecar found for {target}")?;
            return Ok(ExitCode::from(1));
        }
        RuntimeStateRead::Corrupt(message) => {
            writeln!(stderr, "managed sidecar state is corrupt: {message}")?;
            return Ok(ExitCode::from(1));
        }
        RuntimeStateRead::Loaded(servers) => servers,
    };

    if servers.is_empty() {
        if target == "all" {
            writeln!(stdout, "no managed sidecars")?;
            return Ok(ExitCode::SUCCESS);
        }
        writeln!(stderr, "no managed sidecar found for {target}")?;
        return Ok(ExitCode::from(1));
    }

    let matching = if target == "all" {
        servers
    } else {
        servers
            .into_iter()
            .filter(|server| server.id == target)
            .collect::<Vec<_>>()
    };

    if matching.is_empty() {
        writeln!(stderr, "no managed sidecar found for {target}")?;
        return Ok(ExitCode::from(1));
    }

    let mut had_failure = false;
    for server in matching {
        match supervisor::stop_managed_server(
            &server,
            &paths.state_path,
            supervisor::CTRL_C_GRACE_PERIOD,
        ) {
            Ok(result) if result.was_running && result.pid_alive && !result.removed_state => {
                had_failure = true;
                writeln!(
                    stderr,
                    "failed to fully stop {} (pid {}, port {})",
                    server.id, server.pid, server.port
                )?;
            }
            Ok(result) => {
                render_stop_result(stdout, &server, result)?;
            }
            Err(error) => {
                had_failure = true;
                writeln!(
                    stderr,
                    "failed to stop {} (pid {}, port {}): {}",
                    server.id, server.pid, server.port, error
                )?;
            }
        }
    }

    if had_failure {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn wait_for_startup<C, I, W>(
    child: &mut C,
    server: &ManagedServer,
    _log_path: &Path,
    state_path: &Path,
    interrupt: &I,
    mut wait_step: W,
) -> Result<StartupWaitOutcome, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
    I: InterruptSource,
    W: FnMut(&mut C, Duration, Duration) -> Result<StartupPoll, SupervisorError>,
{
    let started = std::time::Instant::now();

    loop {
        if InterruptSource::interrupted(interrupt) {
            supervisor::cleanup_after_ctrl_c(
                child,
                state_path,
                server.identity(),
                supervisor::CTRL_C_GRACE_PERIOD,
            )?;
            child.join_log_drains()?;
            return Ok(StartupWaitOutcome::Interrupted);
        }

        let remaining = supervisor::HEALTH_TIMEOUT.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(SupervisorError::HealthTimeout);
        }

        let step_timeout = remaining.min(supervisor::HEALTH_POLL_INTERVAL);
        match wait_step(child, step_timeout, step_timeout) {
            Ok(StartupPoll::Ready) => return Ok(StartupWaitOutcome::Ready),
            Ok(StartupPoll::Pending) => {}
            Err(error) => return Err(error),
        }
    }
}

fn supervise_running_server<W: Write, E: Write, I: InterruptSource + InterruptStatus>(
    session: RunSession<'_>,
    child: &mut SpawnedServer,
    interrupt: &I,
    restart_policy: &mut RestartPolicy,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<RunOutcome> {
    loop {
        if InterruptSource::interrupted(interrupt) {
            supervisor::cleanup_after_ctrl_c(
                child,
                session.state_path,
                session.server.identity(),
                supervisor::CTRL_C_GRACE_PERIOD,
            )
            .map_err(supervisor_error_to_io)?;
            let _ = child.join_log_drains();
            return Ok(RunOutcome::Exit(ExitCode::from(130)));
        }

        if child.try_wait()?.is_some() {
            let crash = supervisor::child_exited_early_with_drains(child, session.log_path)
                .map_err(supervisor_error_to_io)?;
            let log_tail = crash_tail(&crash).to_string();
            match supervisor::decide_observed_child_exit(
                log_tail,
                session.state_path,
                session.server.identity(),
                interrupt,
                restart_policy,
            )
            .map_err(supervisor_error_to_io)?
            {
                ObservedChildExit::Interrupted => return Ok(RunOutcome::Exit(ExitCode::from(130))),
                ObservedChildExit::Restart => {
                    writeln!(
                        stdout,
                        "llama-server exited unexpectedly; restarting once..."
                    )?;
                    return Ok(RunOutcome::Restart);
                }
                ObservedChildExit::Crash { log_tail } => {
                    writeln!(
                        stderr,
                        "llama-server exited unexpectedly for {}",
                        session.id
                    )?;
                    write_log_tail(stderr, &log_tail)?;
                    return Ok(RunOutcome::Exit(ExitCode::from(1)));
                }
            }
        }

        std::thread::sleep(Duration::from_millis(250));
    }
}

fn print_run_ready<W: Write>(stdout: &mut W, server: &ManagedServer) -> io::Result<()> {
    writeln!(stdout, "model id: {}", server.id)?;
    writeln!(stdout, "pid: {}", server.pid)?;
    writeln!(stdout, "port: {}", server.port)?;
    writeln!(stdout, "model path: {}", server.model_path.display())?;
    writeln!(
        stdout,
        "health url: http://127.0.0.1:{}/health",
        server.port
    )?;
    stdout.flush()
}

fn ensure_runtime_state_is_mutable(state_path: &Path) -> io::Result<()> {
    match supervisor::read_runtime_state(state_path).map_err(supervisor_error_to_io)? {
        RuntimeStateRead::Corrupt(message) => Err(io::Error::other(format!(
            "managed sidecar state is corrupt: {message}"
        ))),
        RuntimeStateRead::Missing | RuntimeStateRead::Loaded(_) => Ok(()),
    }
}

fn render_stop_result<W: Write>(
    stdout: &mut W,
    server: &ManagedServer,
    result: supervisor::StopResult,
) -> io::Result<()> {
    if result.was_running {
        if result.forced {
            writeln!(
                stdout,
                "stopped {} (pid {}, port {}) after force kill",
                server.id, server.pid, server.port
            )
        } else {
            writeln!(
                stdout,
                "stopped {} (pid {}, port {})",
                server.id, server.pid, server.port
            )
        }
    } else {
        writeln!(
            stdout,
            "removed stale entry for {} (pid {}, port {})",
            server.id, server.pid, server.port
        )
    }
}

fn inspection_status(inspection: &supervisor::ManagedServerInspection) -> String {
    if !inspection.stale {
        return "running".to_string();
    }

    if inspection.pid_alive && inspection.port_alive && !inspection.process_identity_matches {
        return "stale (identity)".to_string();
    }

    match (inspection.pid_alive, inspection.port_alive) {
        (false, false) => "stale (pid dead, port dead)".to_string(),
        (false, true) => "stale (pid dead)".to_string(),
        (true, false) => "stale (port dead)".to_string(),
        (true, true) => "running".to_string(),
    }
}

fn supervisor_error_to_io(error: SupervisorError) -> io::Error {
    io::Error::other(error.to_string())
}

fn crash_tail(error: &SupervisorError) -> &str {
    match error {
        SupervisorError::ChildExitedEarly(message) => message.as_str(),
        _ => "",
    }
}

fn write_log_tail<W: Write>(writer: &mut W, log_tail: &str) -> io::Result<()> {
    if log_tail.trim().is_empty() {
        return Ok(());
    }

    writeln!(writer, "{log_tail}")
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(unix)]
struct SignalGuard {
    previous: usize,
}

#[cfg(unix)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        const SIG_ERR: usize = usize::MAX;

        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }

        clear_ctrl_c_received();
        let previous = unsafe { signal(SIGINT, handle_sigint as *const () as usize) };
        if previous == SIG_ERR {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { previous })
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(unix)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(unix)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        use std::ffi::c_int;

        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }

        const SIGINT: c_int = 2;
        let _ = unsafe { signal(SIGINT, self.previous) };
    }
}

#[cfg(windows)]
struct SignalGuard;

#[cfg(windows)]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const TRUE: Bool = 1;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        clear_ctrl_c_received();
        let registered = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), TRUE) };
        if registered == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self)
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(windows)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const FALSE: Bool = 0;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        let _ = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), FALSE) };
    }
}

#[cfg(windows)]
unsafe extern "system" fn handle_console_ctrl(control_type: u32) -> i32 {
    const CTRL_C_EVENT: u32 = 0;
    const CTRL_BREAK_EVENT: u32 = 1;

    match control_type {
        CTRL_C_EVENT | CTRL_BREAK_EVENT => {
            set_ctrl_c_received();
            1
        }
        _ => 0,
    }
}

#[cfg(not(any(unix, windows)))]
struct SignalGuard;

#[cfg(not(any(unix, windows)))]
impl SignalGuard {
    fn install() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Ctrl-C cleanup is unsupported on this platform",
        ))
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

fn write_unknown_id<W: Write>(id: &str, stderr: &mut W) -> io::Result<()> {
    writeln!(stderr, "unknown model id: {id}")?;
    writeln!(stderr, "valid ids: {}", valid_ids())
}

fn print_doctor<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    write_doctor(stdout)?;
    Ok(ExitCode::SUCCESS)
}

fn write_doctor<W: Write>(stdout: &mut W) -> io::Result<()> {
    let hardware = HardwareReport::detect();
    let tools = LocalToolsReport::detect();

    writeln!(stdout, "Machine")?;
    writeln!(stdout, "  {:<16} {}", "Chip:", hardware.chip)?;
    writeln!(
        stdout,
        "  {:<16} {} physical / {} logical",
        "Cores:", hardware.physical_cores, hardware.logical_cores
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB available / {:.1} GB used",
        "RAM:",
        bytes_to_gb(hardware.ram_total_bytes),
        bytes_to_gb(hardware.ram_available_bytes),
        bytes_to_gb(hardware.ram_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB used",
        "Swap:",
        bytes_to_gb(hardware.swap_total_bytes),
        bytes_to_gb(hardware.swap_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} total / {} available",
        "Disk (/):",
        optional_bytes_to_gb(hardware.root_disk_total_bytes),
        optional_bytes_to_gb(hardware.root_disk_available_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} {}",
        "OS:", hardware.os_name, hardware.os_version
    )?;
    writeln!(stdout)?;
    writeln!(stdout, "Detected tools")?;
    for tool in &tools.tools {
        write_tool(stdout, tool)?;
    }

    Ok(())
}

fn write_tool<W: Write>(stdout: &mut W, tool: &DetectedTool) -> io::Result<()> {
    let detection = &tool.detection;
    let evidence = if detection.evidence.is_empty() {
        "unknown".to_string()
    } else {
        detection.evidence.join("; ")
    };

    writeln!(
        stdout,
        "  {:<10} {:<13} {:<11} {}",
        tool.name, detection.install_state, detection.run_state, evidence
    )
}

fn bytes_to_gb_string(bytes: u64) -> String {
    format!("{:.1}", bytes_to_gb(bytes))
}

fn valid_ids() -> String {
    REGISTRY
        .iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>()
        .join(", ")
}

fn model_paths(entry: &ModelEntry, dir: &Path) -> (PathBuf, PathBuf) {
    (
        dir.join(entry.filename),
        dir.join(format!("{}.part", entry.filename)),
    )
}

#[derive(Debug, PartialEq, Eq)]
enum ModelStatus {
    Downloaded,
    Partial,
    NotDownloaded,
}

impl fmt::Display for ModelStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModelStatus::Downloaded => write!(f, "downloaded"),
            ModelStatus::Partial => write!(f, "partial"),
            ModelStatus::NotDownloaded => write!(f, "not downloaded"),
        }
    }
}

fn model_status(entry: &ModelEntry, dir: &Path) -> ModelStatus {
    let (final_path, part_path) = model_paths(entry, dir);

    if final_path.exists() {
        ModelStatus::Downloaded
    } else if part_path.exists() {
        ModelStatus::Partial
    } else {
        ModelStatus::NotDownloaded
    }
}

fn remove_model_files(entry: &ModelEntry, dir: &Path) -> io::Result<Vec<PathBuf>> {
    let (final_path, part_path) = model_paths(entry, dir);
    let mut removed = Vec::new();

    for path in [final_path, part_path] {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }

    Ok(removed)
}

fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn optional_bytes_to_gb(bytes: Option<u64>) -> String {
    bytes
        .map(|bytes| format!("{:.1} GB", bytes_to_gb(bytes)))
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::registry::REGISTRY;
    use loxa_core::supervisor::LogDrainingChild;
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn clap_parses_all_subcommands() {
        assert!(Cli::try_parse_from(["loxa", "doctor"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "list"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "pull", "gemma-3-4b-it-q4"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "rm", "gemma-3-4b-it-q4"]).is_ok());
        assert!(matches!(
            Cli::try_parse_from(["loxa", "run", "gemma-3-4b-it-q4", "--ctx", "4096", "--port", "9000"]),
            Ok(Cli {
                command: Command::Run {
                    id,
                    ctx: Some(4096),
                    port: Some(9000),
                },
            }) if id == "gemma-3-4b-it-q4"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "ps"]),
            Ok(Cli {
                command: Command::Ps,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "stop", "all"]),
            Ok(Cli {
                command: Command::Stop { target },
            }) if target == "all"
        ));
    }

    #[test]
    fn unknown_pull_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Pull {
                id: "missing-model".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_rm_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Rm {
                id: "missing-model".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_run_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Run {
                id: "missing-model".to_string(),
                ctx: None,
                port: None,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: PathBuf::from("/tmp/unused-models"),
            state_path: PathBuf::from("/tmp/unused-managed.json"),
            logs_dir: PathBuf::from("/tmp/unused-logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn model_not_downloaded_run_error_tells_user_to_pull() {
        let temp = TempDir::new("loxa-run-not-downloaded");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("model not downloaded"));
        assert!(stderr.contains("loxa pull gemma-3-4b-it-q4"));
    }

    #[test]
    fn ps_renders_clear_message_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-ps-empty");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn ps_marks_stale_entries() {
        let temp = TempDir::new("loxa-ps-stale");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let stale = loxa_core::supervisor::ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 999_999,
            port: 65_530,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 1_700_000_000,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        loxa_core::supervisor::write_runtime_state(&temp.path().join("managed.json"), &[stale])
            .expect("write stale state");
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("stale"));
        assert!(stdout.contains("gemma-3-4b-it-q4"));
    }

    #[test]
    fn ps_reports_corrupt_state_without_failing() {
        let temp = TempDir::new("loxa-ps-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn run_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-run-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn startup_interrupt_after_state_write_cleans_up_and_returns_130() {
        let temp = TempDir::new("loxa-run-interrupt");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        let other = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 888,
            port: 8082,
            model_path: temp.path().join("other.gguf"),
            started_at_unix_s: 790,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(2),
        };
        supervisor::write_runtime_state(&state_path, &[server.clone(), other.clone()])
            .expect("seed runtime state");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = wait_for_startup(
            &mut child,
            &server,
            temp.path().join("startup.log").as_path(),
            &state_path,
            &signal,
            |_, _, _| Ok(StartupPoll::Pending),
        )
        .expect("startup wait outcome");

        assert_eq!(outcome, StartupWaitOutcome::Interrupted);
        assert!(child.events.borrow().contains(&"terminate"));
        assert!(child.events.borrow().contains(&"join_log_drains"));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("runtime state after interrupt"),
            RuntimeStateRead::Loaded(vec![other])
        );
    }

    #[test]
    fn stop_all_is_idempotent_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-stop-all-empty");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn stop_all_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-stop-all-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = CliPaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn model_status_prioritizes_downloaded_then_partial_then_not_downloaded() {
        let temp = TempDir::new("loxa-status");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());

        assert_eq!(model_status(entry, temp.path()), ModelStatus::NotDownloaded);

        fs::write(&part_path, b"partial").expect("write part file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::write(&final_path, b"final").expect("write final file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Downloaded);
    }

    #[test]
    fn remove_model_files_deletes_final_and_part_then_returns_empty_when_absent() {
        let temp = TempDir::new("loxa-rm");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());
        fs::write(&final_path, b"final").expect("write final file");
        fs::write(&part_path, b"partial").expect("write part file");

        let removed = remove_model_files(entry, temp.path()).expect("remove model files");

        assert_eq!(removed, vec![final_path.clone(), part_path.clone()]);
        assert!(!final_path.exists());
        assert!(!part_path.exists());

        let removed = remove_model_files(entry, temp.path()).expect("remove absent model files");
        assert!(removed.is_empty());
    }

    #[test]
    fn bytes_to_gb_string_uses_one_decimal() {
        assert_eq!(bytes_to_gb_string(0), "0.0");
        assert_eq!(bytes_to_gb_string(1_073_741_824), "1.0");
        assert_eq!(bytes_to_gb_string(1_610_612_736), "1.5");
    }

    #[test]
    fn ctrl_c_flag_helpers_round_trip() {
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());

        set_ctrl_c_received();
        assert!(ctrl_c_received());

        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct FakeInterruptSource {
        states: Vec<bool>,
        index: Cell<usize>,
    }

    impl FakeInterruptSource {
        fn new(states: Vec<bool>) -> Self {
            Self {
                states,
                index: Cell::new(0),
            }
        }
    }

    impl InterruptSource for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            let index = self.index.get();
            let value = self
                .states
                .get(index)
                .copied()
                .or_else(|| self.states.last().copied())
                .unwrap_or(false);
            if index + 1 < self.states.len() {
                self.index.set(index + 1);
            }
            value
        }
    }

    struct FakeStartupChild {
        events: RefCell<Vec<&'static str>>,
        wait_results: RefCell<Vec<Option<i32>>>,
    }

    impl FakeStartupChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self {
                events: RefCell::new(Vec::new()),
                wait_results: RefCell::new(wait_results),
            }
        }
    }

    impl ManagedChild for FakeStartupChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.borrow_mut().push("try_wait");
            let mut wait_results = self.wait_results.borrow_mut();
            if wait_results.len() > 1 {
                Ok(wait_results.remove(0))
            } else {
                Ok(wait_results.first().copied().unwrap_or(Some(0)))
            }
        }
    }

    impl LogDrainingChild for FakeStartupChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("join_log_drains");
            Ok(())
        }
    }
}
