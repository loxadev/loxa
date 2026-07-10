use clap::Parser;
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
use loxa_core::supervisor::{
    self, InterruptStatus, LogDrainingChild, ManagedChild, ManagedServer, ObservedChildExit,
    RuntimeStateRead, SpawnedServer, SupervisorError,
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum RunOutcome {
    RequestedStop,
    Interrupted,
    Restart { run: supervisor::ManagedRun },
    Exhausted { log_tail: String },
    RecoveryRequired,
}

#[derive(Debug)]
enum SpawnBoundary<T> {
    Spawned {
        run: supervisor::ManagedRun,
        value: T,
    },
    RequestedStop,
}

#[derive(Debug)]
enum OwnedReplacementPreparation<R, D> {
    Prepared {
        run: supervisor::ManagedRun,
        resolved: R,
        detected: D,
    },
    RequestedStop,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupPoll {
    Pending,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupWaitOutcome {
    Ready,
    RequestedStop,
    Interrupted,
    RecoveryRequired,
}

#[derive(Debug)]
enum StartupWaitFailure {
    BeforeTeardown(SupervisorError),
    AfterTeardown(SupervisorError),
    AfterChildReaped {
        log_tail: String,
        diagnostics_error: Option<String>,
    },
}

struct RunSession<'a> {
    id: &'a str,
    state_identity: &'a supervisor::ManagedRunIdentity,
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
    let owner_pid = std::process::id();
    let owner_process_start_time_unix_s =
        supervisor::process_start_time(owner_pid).ok_or_else(|| {
            supervisor_error_to_io(SupervisorError::ProcessIdentityUnavailable(owner_pid))
        })?;
    let run_id = format!("run-{owner_pid}-{owner_process_start_time_unix_s}");
    let mut replacement_run: Option<supervisor::ManagedRun> = None;

    loop {
        let owned_replacement = replacement_run.take();
        let started_at_unix_s = unix_timestamp_now();
        let (entry, model_path, llama_server_path, starting_run, initial_generation) =
            if let Some(run) = owned_replacement {
                let preparation = prepare_owned_replacement_run(
                    &paths.state_path,
                    run,
                    &signal_guard,
                    || supervisor::resolve_model_path(id, &paths.models_dir),
                    supervisor::detect_llama_server,
                );
                let preparation = match preparation {
                    Ok(preparation) => preparation,
                    Err(SupervisorError::ModelNotDownloaded(_)) => {
                        writeln!(
                            stderr,
                            "model not downloaded for {id}; run `loxa pull {id}`"
                        )?;
                        return Ok(ExitCode::from(1));
                    }
                    Err(error) => return Err(supervisor_error_to_io(error)),
                };
                match preparation {
                    OwnedReplacementPreparation::Prepared {
                        run,
                        resolved: (entry, model_path),
                        detected: llama_server_path,
                    } => (entry, model_path, llama_server_path, run, false),
                    OwnedReplacementPreparation::RequestedStop => return Ok(ExitCode::SUCCESS),
                    OwnedReplacementPreparation::Interrupted => {
                        return Ok(ExitCode::from(130));
                    }
                }
            } else {
                if signal_guard.interrupted() {
                    return Ok(ExitCode::from(130));
                }
                let (entry, model_path) =
                    match supervisor::resolve_model_path(id, &paths.models_dir) {
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
                let selected_port =
                    supervisor::choose_localhost_port(port).map_err(supervisor_error_to_io)?;
                let log_path = paths.log_path(id, selected_port, started_at_unix_s);
                (
                    entry,
                    model_path,
                    llama_server_path,
                    supervisor::ManagedRun {
                        schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
                        run_id: run_id.clone(),
                        model_id: id.to_string(),
                        owner_pid,
                        owner_process_start_time_unix_s,
                        stop_requested: false,
                        lifecycle: supervisor::RunLifecycle::Starting,
                        generation: 0,
                        generation_alias: format!("loxa-{run_id}-g0"),
                        port: selected_port,
                        log_path,
                        child_pid: None,
                        child_process_start_time_unix_s: None,
                        child_pgid: None,
                    },
                    true,
                )
            };
        let log_path = starting_run.log_path.clone();
        let spec = supervisor::ServerSpec {
            entry,
            model_path,
            llama_server_path,
            port: starting_run.port,
            ctx_tokens: ctx.unwrap_or(supervisor::DEFAULT_CTX_TOKENS),
        };
        let prepare = || supervisor::llama_server_version(&spec.llama_server_path);
        let spawn = |llama_server_version| {
            let child = supervisor::spawn_llama_server(&spec, &log_path)?;
            Ok((llama_server_version, child))
        };
        let boundary = if initial_generation {
            prepare_and_spawn_after_starting_run_persisted(
                &paths.state_path,
                starting_run,
                prepare,
                spawn,
            )
        } else {
            prepare_and_spawn_after_persisted_starting_run(
                &paths.state_path,
                starting_run,
                prepare,
                spawn,
            )
        }
        .map_err(supervisor_error_to_io)?;
        let SpawnBoundary::Spawned {
            run: starting_run,
            value: (llama_server_version, mut child),
        } = boundary
        else {
            return Ok(ExitCode::SUCCESS);
        };
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
            let outcome = supervisor::finish_owner_teardown_with(
                &paths.state_path,
                &starting_run.identity(),
                supervisor::OwnerTeardownDecision::Interrupted,
                |decision| owner_teardown_child(&mut child, decision),
            )
            .map_err(supervisor_error_to_io)?;
            return Ok(match outcome {
                supervisor::OwnerTerminalOutcome::RequestedStop => ExitCode::SUCCESS,
                supervisor::OwnerTerminalOutcome::Interrupted => ExitCode::from(130),
                supervisor::OwnerTerminalOutcome::RecoveryRequired => ExitCode::from(1),
            });
        }

        let run = supervisor::persist_managed_server_or_cleanup(
            &mut child,
            &paths.state_path,
            starting_run,
            server.clone(),
            supervisor::CTRL_C_GRACE_PERIOD,
        )
        .map_err(supervisor_error_to_io)?;
        let state_identity = run.identity();

        if let Some(outcome) = observe_attached_stop_with(
            &mut child,
            &paths.state_path,
            &state_identity,
            || {},
            owner_teardown_child,
        )
        .map_err(supervisor_error_to_io)?
        {
            return Ok(match outcome {
                supervisor::OwnerTerminalOutcome::RequestedStop => ExitCode::SUCCESS,
                supervisor::OwnerTerminalOutcome::Interrupted => ExitCode::from(130),
                supervisor::OwnerTerminalOutcome::RecoveryRequired => ExitCode::from(1),
            });
        }

        match wait_for_startup(
            &mut child,
            &state_identity,
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
            owner_teardown_child,
        ) {
            Ok(StartupWaitOutcome::Ready) => {
                print_run_ready(stdout, &server)?;
                match supervise_running_server(
                    RunSession {
                        id,
                        state_identity: &state_identity,
                        log_path: &log_path,
                        state_path: &paths.state_path,
                    },
                    &mut child,
                    &signal_guard,
                    stdout,
                    stderr,
                )? {
                    RunOutcome::RequestedStop => return Ok(ExitCode::SUCCESS),
                    RunOutcome::Interrupted => return Ok(ExitCode::from(130)),
                    RunOutcome::Restart { run } => {
                        replacement_run = Some(run);
                        continue;
                    }
                    RunOutcome::Exhausted { .. } | RunOutcome::RecoveryRequired => {
                        return Ok(ExitCode::from(1))
                    }
                }
            }
            Ok(StartupWaitOutcome::RequestedStop) => return Ok(ExitCode::SUCCESS),
            Ok(StartupWaitOutcome::Interrupted) => return Ok(ExitCode::from(130)),
            Ok(StartupWaitOutcome::RecoveryRequired) => return Ok(ExitCode::from(1)),
            Err(StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            }) => {
                let log_tail = diagnostics_error
                    .map(|error| format!("crash diagnostics unavailable: {error}"))
                    .unwrap_or(log_tail);
                match supervisor::decide_observed_child_exit(
                    log_tail,
                    &paths.state_path,
                    &state_identity,
                    &signal_guard,
                    |decision| owner_teardown_child(&mut child, decision),
                )
                .map_err(supervisor_error_to_io)?
                {
                    ObservedChildExit::RequestedStop => return Ok(ExitCode::SUCCESS),
                    ObservedChildExit::Interrupted => return Ok(ExitCode::from(130)),
                    ObservedChildExit::Restart { run } => {
                        let run = retain_restart_after_best_effort_announcement(
                            stdout,
                            "llama-server exited before becoming healthy; restarting once...",
                            run,
                        );
                        replacement_run = Some(run);
                        continue;
                    }
                    ObservedChildExit::Exhausted { log_tail } => {
                        writeln!(
                            stderr,
                            "llama-server exited before becoming healthy for {id}"
                        )?;
                        write_log_tail(stderr, &log_tail)?;
                        return Ok(ExitCode::from(1));
                    }
                    ObservedChildExit::RecoveryRequired => return Ok(ExitCode::from(1)),
                }
            }
            Err(StartupWaitFailure::AfterTeardown(error)) => {
                return Err(supervisor_error_to_io(error));
            }
            Err(StartupWaitFailure::BeforeTeardown(error)) => {
                let _ = supervisor::cleanup_after_ctrl_c(
                    &mut child,
                    &paths.state_path,
                    &state_identity,
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

fn retain_restart_after_best_effort_announcement<W: Write>(
    stdout: &mut W,
    message: &str,
    run: supervisor::ManagedRun,
) -> supervisor::ManagedRun {
    let _ = writeln!(stdout, "{message}");
    run
}

fn prepare_owned_replacement_run<R, D, I, RF, DF>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    interrupt: &I,
    resolve: RF,
    detect: DF,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError>
where
    I: InterruptSource,
    RF: FnOnce() -> Result<R, SupervisorError>,
    DF: FnOnce() -> Result<D, SupervisorError>,
{
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    let resolved = match resolve() {
        Ok(resolved) => resolved,
        Err(error) => return finish_owned_replacement_error(state_path, &run, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    let detected = match detect() {
        Ok(detected) => detected,
        Err(error) => return finish_owned_replacement_error(state_path, &run, error),
    };
    if InterruptSource::interrupted(interrupt) {
        return finish_owned_replacement_interrupt(state_path, &run);
    }

    Ok(OwnedReplacementPreparation::Prepared {
        run,
        resolved,
        detected,
    })
}

fn finish_owned_replacement_interrupt<R, D>(
    state_path: &Path,
    run: &supervisor::ManagedRun,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => {
            Ok(OwnedReplacementPreparation::RequestedStop)
        }
        supervisor::ChildlessFinishOutcome::Finished => {
            Ok(OwnedReplacementPreparation::Interrupted)
        }
    }
}

fn finish_owned_replacement_error<R, D>(
    state_path: &Path,
    run: &supervisor::ManagedRun,
    error: SupervisorError,
) -> Result<OwnedReplacementPreparation<R, D>, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => {
            Ok(OwnedReplacementPreparation::RequestedStop)
        }
        supervisor::ChildlessFinishOutcome::Finished => Err(error),
    }
}

fn prepare_and_spawn_after_starting_run_persisted<P, T, PF, SF>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    prepare: PF,
    spawn: SF,
) -> Result<SpawnBoundary<T>, SupervisorError>
where
    PF: FnOnce() -> Result<P, SupervisorError>,
    SF: FnOnce(P) -> Result<T, SupervisorError>,
{
    let run = supervisor::create_starting_run(state_path, run)?;
    prepare_and_spawn_after_persisted_starting_run(state_path, run, prepare, spawn)
}

fn prepare_and_spawn_after_persisted_starting_run<P, T, PF, SF>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    prepare: PF,
    spawn: SF,
) -> Result<SpawnBoundary<T>, SupervisorError>
where
    PF: FnOnce() -> Result<P, SupervisorError>,
    SF: FnOnce(P) -> Result<T, SupervisorError>,
{
    let prepared = match prepare() {
        Ok(prepared) => prepared,
        Err(error) => return finish_childless_spawn_error(state_path, &run, error),
    };
    let run = match supervisor::prepare_starting_run_for_spawn(state_path, &run.identity())? {
        supervisor::PreSpawnDecision::Spawn(run) => run,
        supervisor::PreSpawnDecision::RequestedStop => return Ok(SpawnBoundary::RequestedStop),
    };
    match spawn(prepared) {
        Ok(value) => Ok(SpawnBoundary::Spawned { run, value }),
        Err(error) => finish_childless_spawn_error(state_path, &run, error),
    }
}

fn finish_childless_spawn_error<T>(
    state_path: &Path,
    run: &supervisor::ManagedRun,
    error: SupervisorError,
) -> Result<SpawnBoundary<T>, SupervisorError> {
    match supervisor::finish_childless_runtime_state_run(state_path, &run.identity())? {
        supervisor::ChildlessFinishOutcome::RequestedStop => Ok(SpawnBoundary::RequestedStop),
        supervisor::ChildlessFinishOutcome::Finished => Err(error),
    }
}

#[cfg(test)]
fn spawn_after_starting_run_persisted<T, F>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    spawn: F,
) -> Result<SpawnBoundary<T>, SupervisorError>
where
    F: FnOnce() -> Result<T, SupervisorError>,
{
    spawn_after_starting_run_persisted_with_hook(state_path, run, || {}, spawn)
}

#[cfg(test)]
fn spawn_after_starting_run_persisted_with_hook<T, H, F>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    before_spawn: H,
    spawn: F,
) -> Result<SpawnBoundary<T>, SupervisorError>
where
    H: FnOnce(),
    F: FnOnce() -> Result<T, SupervisorError>,
{
    let run = supervisor::create_starting_run(state_path, run)?;
    spawn_after_persisted_starting_run_with_hook(state_path, run, before_spawn, spawn)
}

#[cfg(test)]
fn spawn_after_persisted_starting_run_with_hook<T, H, F>(
    state_path: &Path,
    run: supervisor::ManagedRun,
    before_spawn: H,
    spawn: F,
) -> Result<SpawnBoundary<T>, SupervisorError>
where
    H: FnOnce(),
    F: FnOnce() -> Result<T, SupervisorError>,
{
    before_spawn();
    prepare_and_spawn_after_persisted_starting_run(state_path, run, || Ok(()), |_| spawn())
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
        SupervisorError::ChildExitedEarly(message)
        | SupervisorError::ChildReapedDiagnosticsFailed(message) => message.as_str(),
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
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn persist_run_for_server(state_path: &Path, server: &ManagedServer) -> supervisor::ManagedRun {
        let run_id = format!("test-run-{}", server.pid);
        let mut run = starting_run_for_test(state_path, &run_id);
        run.model_id = server.id.clone();
        run.port = server.port;
        run.generation_alias = format!("loxa-{run_id}-g0");
        run.log_path = state_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{run_id}.log"));
        supervisor::create_starting_run(state_path, run.clone()).expect("create starting run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(server.pid);
        run.child_process_start_time_unix_s = server.process_start_time_unix_s;
        assert!(
            supervisor::update_runtime_state_run(state_path, &starting_identity, run.clone())
                .expect("attach test child")
        );
        run
    }

    fn starting_run_for_test(state_path: &Path, run_id: &str) -> supervisor::ManagedRun {
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: "gemma-3-4b-it-q4".to_string(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            port: 8080,
            log_path: state_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn request_stop_for_test(
        state_path: &Path,
        identity: &supervisor::ManagedRunIdentity,
    ) -> supervisor::ManagedRun {
        let mut run = supervisor::current_runtime_state_run(state_path, identity)
            .expect("read current run before test stop");
        run.stop_requested = true;
        supervisor::update_runtime_state_run_committed(state_path, identity, run)
            .expect("commit test stop")
            .expect("exact test stop")
    }

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
        persist_run_for_server(&temp.path().join("managed.json"), &stale);
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
    fn pre_spawn_boundary_rejects_a_second_run_before_spawn_is_reached() {
        let temp = TempDir::new("loxa-pre-spawn-second-run");
        let state_path = temp.path().join("managed.json");
        let first = starting_run_for_test(&state_path, "run-1");
        supervisor::create_starting_run(&state_path, first).expect("create first run");
        let second = starting_run_for_test(&state_path, "run-2");
        let spawn_reached = Cell::new(false);

        let error = spawn_after_starting_run_persisted(&state_path, second, || {
            spawn_reached.set(true);
            Ok(())
        })
        .expect_err("second run must be rejected before spawn");

        assert!(matches!(error, SupervisorError::ActiveRun(run_id) if run_id == "run-1"));
        assert!(!spawn_reached.get());
    }

    #[test]
    fn pre_spawn_boundary_rejects_a_legacy_array_before_spawn_is_reached() {
        let temp = TempDir::new("loxa-pre-spawn-legacy-array");
        let state_path = temp.path().join("managed.json");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(&state_path, "[]").expect("write legacy array");
        let run = starting_run_for_test(&state_path, "run-1");
        let spawn_reached = Cell::new(false);

        let error = spawn_after_starting_run_persisted(&state_path, run, || {
            spawn_reached.set(true);
            Ok(())
        })
        .expect_err("legacy array must be rejected before spawn");

        assert!(matches!(error, SupervisorError::LegacyRuntimeState(path) if path == state_path));
        assert!(!spawn_reached.get());
    }

    #[test]
    fn pre_spawn_boundary_rejects_a_legacy_sentinel_before_spawn_is_reached() {
        let temp = TempDir::new("loxa-pre-spawn-legacy-sentinel");
        let state_path = temp.path().join("managed.json");
        let sentinel_path = state_path.with_file_name("managed.json.lock");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(&sentinel_path, "legacy owner\n").expect("write legacy sentinel");
        let run = starting_run_for_test(&state_path, "run-1");
        let spawn_reached = Cell::new(false);

        let error = spawn_after_starting_run_persisted(&state_path, run, || {
            spawn_reached.set(true);
            Ok(())
        })
        .expect_err("legacy sentinel must be rejected before spawn");

        assert!(
            matches!(error, SupervisorError::LegacyRuntimeState(path) if path == sentinel_path)
        );
        assert!(!spawn_reached.get());
        assert!(!state_path.exists());
    }

    #[test]
    fn pre_spawn_boundary_closure_error_exact_finishes_the_starting_record() {
        let temp = TempDir::new("loxa-pre-spawn-closure-error");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let closure_reached = Cell::new(false);

        let error = spawn_after_starting_run_persisted(&state_path, run, || {
            closure_reached.set(true);
            Err::<(), _>(SupervisorError::NoFreePort)
        })
        .expect_err("closure failure must be returned after exact cleanup");

        assert!(closure_reached.get());
        assert!(matches!(error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read cleaned state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
        let envelope = fs::read_to_string(&state_path).expect("read empty v2 envelope");
        assert!(envelope.contains("\"schema_version\": 2"));
        assert!(envelope.contains("\"runs\": []"));
    }

    #[test]
    fn pre_spawn_boundary_cleanup_conflict_overrides_the_closure_error() {
        let temp = TempDir::new("loxa-pre-spawn-cleanup-conflict");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let starting_identity = run.identity();
        let mut newer_generation = run.clone();
        newer_generation.generation = 1;
        newer_generation.generation_alias = "loxa-run-1-g1".to_string();
        let closure_reached = Cell::new(false);

        let error = spawn_after_starting_run_persisted(&state_path, run, || {
            closure_reached.set(true);
            assert!(supervisor::update_runtime_state_run(
                &state_path,
                &starting_identity,
                newer_generation.clone(),
            )
            .expect("advance generation inside boundary"));
            Err::<(), _>(SupervisorError::NoFreePort)
        })
        .expect_err("cleanup conflict must replace the closure error");

        assert!(closure_reached.get());
        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read preserved newer state"),
            RuntimeStateRead::Loaded(vec![newer_generation])
        );
    }

    #[test]
    fn pre_spawn_boundary_stop_on_childless_initial_record_skips_spawn_and_finishes() {
        let temp = TempDir::new("loxa-pre-spawn-stop-initial");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let identity = run.identity();
        let events = RefCell::new(Vec::new());
        let spawn_count = Cell::new(0_u8);

        let outcome = spawn_after_starting_run_persisted_with_hook(
            &state_path,
            run,
            || {
                events.borrow_mut().push("initial_persisted");
                request_stop_for_test(&state_path, &identity);
            },
            || {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect("requested stop outcome");

        assert!(matches!(outcome, SpawnBoundary::RequestedStop));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(events.into_inner(), vec!["initial_persisted"]);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read finished state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn restart_boundary_stop_immediately_before_replacement_spawn_skips_spawn() {
        let temp = TempDir::new("loxa-pre-spawn-stop-replacement");
        let state_path = temp.path().join("managed.json");
        let mut replacement = starting_run_for_test(&state_path, "run-1");
        replacement.generation = 1;
        replacement.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, replacement.clone())
            .expect("publish generation one");
        let identity = replacement.identity();
        let events = RefCell::new(vec!["generation_one_published"]);
        let spawn_count = Cell::new(0_u8);

        let outcome = spawn_after_persisted_starting_run_with_hook(
            &state_path,
            replacement,
            || {
                events.borrow_mut().push("before_replacement_spawn");
                request_stop_for_test(&state_path, &identity);
            },
            || {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect("requested stop outcome");

        assert!(matches!(outcome, SpawnBoundary::RequestedStop));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            events.into_inner(),
            vec!["generation_one_published", "before_replacement_spawn"]
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read finished state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn initial_stop_committed_during_version_probe_blocks_os_spawn() {
        let temp = TempDir::new("loxa-version-stop-initial");
        let state_path = temp.path().join("managed.json");
        let run = starting_run_for_test(&state_path, "run-1");
        let identity = run.identity();
        let probe_entered = Arc::new(Barrier::new(2));
        let stop_committed = Arc::new(Barrier::new(2));
        let stopper_path = state_path.clone();
        let entered_for_stopper = Arc::clone(&probe_entered);
        let committed_for_stopper = Arc::clone(&stop_committed);
        let stopper = thread::spawn(move || {
            entered_for_stopper.wait();
            request_stop_for_test(&stopper_path, &identity);
            committed_for_stopper.wait();
        });
        let spawn_count = Cell::new(0_u8);

        let outcome = prepare_and_spawn_after_starting_run_persisted(
            &state_path,
            run,
            || {
                probe_entered.wait();
                stop_committed.wait();
                Ok("test-version".to_string())
            },
            |_| {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect("requested stop boundary");
        stopper.join().expect("stopper joins");

        assert!(matches!(outcome, SpawnBoundary::RequestedStop));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn replacement_stop_committed_during_version_probe_blocks_os_spawn() {
        let temp = TempDir::new("loxa-version-stop-replacement");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let identity = run.identity();
        let probe_entered = Arc::new(Barrier::new(2));
        let stop_committed = Arc::new(Barrier::new(2));
        let stopper_path = state_path.clone();
        let entered_for_stopper = Arc::clone(&probe_entered);
        let committed_for_stopper = Arc::clone(&stop_committed);
        let stopper = thread::spawn(move || {
            entered_for_stopper.wait();
            request_stop_for_test(&stopper_path, &identity);
            committed_for_stopper.wait();
        });
        let spawn_count = Cell::new(0_u8);

        let outcome = prepare_and_spawn_after_persisted_starting_run(
            &state_path,
            run,
            || {
                probe_entered.wait();
                stop_committed.wait();
                Ok("test-version".to_string())
            },
            |_| {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect("requested stop boundary");
        stopper.join().expect("stopper joins");

        assert!(matches!(outcome, SpawnBoundary::RequestedStop));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_resolution_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-resolution-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Err::<(), _>(SupervisorError::NoFreePort),
            || -> Result<(), SupervisorError> {
                panic!("detection must not run after resolution failure")
            },
        )
        .expect_err("resolution failure");

        assert!(matches!(error, SupervisorError::NoFreePort));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_detection_failure_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-detection-failure");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false]);

        let error = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Err::<(), _>(SupervisorError::LlamaServerNotFound),
        )
        .expect_err("detection failure");

        assert!(matches!(error, SupervisorError::LlamaServerNotFound));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_resolution_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-resolution");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || -> Result<(), SupervisorError> { panic!("detection must not run after interrupt") },
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_interrupt_after_detection_exact_finishes_childless_run() {
        let temp = TempDir::new("loxa-replacement-interrupt-after-detection");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, false, true]);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || Ok("resolved"),
            || Ok("detected"),
        )
        .expect("interrupt outcome");

        assert!(matches!(outcome, OwnedReplacementPreparation::Interrupted));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn published_replacement_concurrent_stop_and_interrupt_prefers_requested_stop() {
        let temp = TempDir::new("loxa-replacement-stop-interrupt");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.generation = 1;
        run.generation_alias = "loxa-run-1-g1".to_string();
        supervisor::create_starting_run(&state_path, run.clone()).expect("publish generation one");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let identity = run.identity();
        let spawn_count = Cell::new(0_u8);

        let outcome = prepare_owned_replacement_run(
            &state_path,
            run,
            &signal,
            || {
                request_stop_for_test(&state_path, &identity);
                Ok("resolved")
            },
            || Ok("detected"),
        )
        .expect("requested stop outcome");
        if matches!(outcome, OwnedReplacementPreparation::Prepared { .. }) {
            spawn_count.set(spawn_count.get() + 1);
        }

        assert!(matches!(
            outcome,
            OwnedReplacementPreparation::RequestedStop
        ));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn attachment_boundary_stop_during_and_immediately_after_tears_down_once_without_generation_two(
    ) {
        for stop_during_attachment in [true, false] {
            let temp = TempDir::new("loxa-attachment-stop-boundary");
            let state_path = temp.path().join("managed.json");
            let mut starting = starting_run_for_test(&state_path, "run-1");
            starting.generation = 1;
            starting.generation_alias = "loxa-run-1-g1".to_string();
            supervisor::create_starting_run(&state_path, starting.clone())
                .expect("publish generation one");
            if stop_during_attachment {
                starting = request_stop_for_test(&state_path, &starting.identity());
            }
            let server = ManagedServer {
                id: starting.model_id.clone(),
                pid: 778,
                port: starting.port,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(222),
            };
            let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
            let attached = supervisor::persist_managed_server_or_cleanup(
                &mut child,
                &state_path,
                starting,
                server,
                Duration::from_millis(10),
            )
            .expect("attach replacement");
            let events = RefCell::new(Vec::new());
            let identity = attached.identity();

            let outcome = observe_attached_stop_with(
                &mut child,
                &state_path,
                &attached.identity(),
                || {
                    events.borrow_mut().push("after_attachment");
                    if !stop_during_attachment {
                        request_stop_for_test(&state_path, &identity);
                    }
                },
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    supervisor::TeardownConfirmation::Confirmed
                },
            )
            .expect("attachment stop outcome");

            assert_eq!(
                outcome,
                Some(supervisor::OwnerTerminalOutcome::RequestedStop)
            );
            assert_eq!(
                events.into_inner(),
                vec!["after_attachment", "requested_stop_teardown"]
            );
            assert_eq!(
                supervisor::read_runtime_state(&state_path).expect("read terminal state"),
                RuntimeStateRead::Loaded(Vec::new())
            );
        }
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
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);

        let outcome = wait_for_startup(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            |_, _, _| Ok(StartupPoll::Pending),
            owner_teardown_child,
        )
        .expect("startup wait outcome");

        assert_eq!(outcome, StartupWaitOutcome::Interrupted);
        assert!(child.events.borrow().contains(&"terminate"));
        assert!(child.events.borrow().contains(&"join_log_drains"));
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("runtime state after interrupt"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn startup_polling_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-startup-stop-poll");
            let state_path = temp.path().join("managed.json");
            let server = ManagedServer {
                id: "gemma-3-4b-it-q4".to_string(),
                pid: 777,
                port: 8081,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(111),
            };
            let run = persist_run_for_server(&state_path, &server);
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
            let events = RefCell::new(Vec::new());
            let identity = run.identity();

            let outcome = wait_for_startup(
                &mut child,
                &identity,
                &state_path,
                &signal,
                |_, _, _| {
                    events.borrow_mut().push("startup_poll");
                    request_stop_for_test(&state_path, &identity);
                    Ok(StartupPoll::Pending)
                },
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    confirmation
                },
            )
            .expect("startup stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                StartupWaitOutcome::RequestedStop
            } else {
                StartupWaitOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert_eq!(
                events.into_inner(),
                vec!["startup_poll", "requested_stop_teardown"]
            );
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read startup state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_loop_external_stop_confirmed_and_unconfirmed_results_are_gated() {
        for confirmation in [
            supervisor::TeardownConfirmation::Confirmed,
            supervisor::TeardownConfirmation::Unconfirmed,
        ] {
            let temp = TempDir::new("loxa-running-stop-poll");
            let state_path = temp.path().join("managed.json");
            let server = ManagedServer {
                id: "gemma-3-4b-it-q4".to_string(),
                pid: 777,
                port: 8081,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(111),
            };
            let run = persist_run_for_server(&state_path, &server);
            let stopped = request_stop_for_test(&state_path, &run.identity());
            let signal = FakeInterruptSource::new(vec![false]);
            let mut child = FakeStartupChild::with_wait_results(vec![None]);
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let events = RefCell::new(Vec::new());

            let outcome = supervise_running_server_with(
                RunSession {
                    id: &server.id,
                    state_identity: &stopped.identity(),
                    log_path: stopped.log_path.as_path(),
                    state_path: &state_path,
                },
                &mut child,
                &signal,
                &mut stdout,
                &mut stderr,
                |_, decision| {
                    assert_eq!(decision, supervisor::OwnerTeardownDecision::RequestedStop);
                    events.borrow_mut().push("requested_stop_teardown");
                    confirmation
                },
                |_| panic!("running loop must return before sleeping"),
            )
            .expect("running stop outcome");

            let expected = if confirmation == supervisor::TeardownConfirmation::Confirmed {
                RunOutcome::RequestedStop
            } else {
                RunOutcome::RecoveryRequired
            };
            assert_eq!(outcome, expected);
            assert_eq!(events.into_inner(), vec!["requested_stop_teardown"]);
            let RuntimeStateRead::Loaded(runs) =
                supervisor::read_runtime_state(&state_path).expect("read running state")
            else {
                panic!("expected loaded state");
            };
            assert_eq!(
                runs.is_empty(),
                confirmation == supervisor::TeardownConfirmation::Confirmed
            );
            if confirmation == supervisor::TeardownConfirmation::Unconfirmed {
                assert!(runs[0].stop_requested);
            }
        }
    }

    #[test]
    fn running_reaped_exit_with_concurrent_stop_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-stop");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let identity = run.identity();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &identity,
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            |child, decision| {
                assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                request_stop_for_test(&state_path, &identity);
                owner_teardown_child(child, decision)
            },
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("reaped stop outcome");

        assert_eq!(outcome, RunOutcome::RequestedStop);
        assert_eq!(
            child.events.into_inner(),
            vec!["try_wait", "join_log_drains"]
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_reaped_exit_with_concurrent_interrupt_never_resignals_child() {
        let temp = TempDir::new("loxa-running-reaped-interrupt");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let signal = FakeInterruptSource::new(vec![false, true]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            owner_teardown_child,
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("reaped interrupt outcome");

        assert_eq!(outcome, RunOutcome::Interrupted);
        assert_eq!(
            child.events.into_inner(),
            vec!["try_wait", "join_log_drains"]
        );
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn running_restart_announcement_broken_pipe_retains_handoff_and_stop_wins() {
        let temp = TempDir::new("loxa-running-restart-broken-pipe");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        fs::write(&run.log_path, b"reaped crash\n").expect("write crash log");
        let generation_one_identity = supervisor::ManagedRunIdentity {
            run_id: run.run_id.clone(),
            generation: 1,
            child_pid: None,
            child_process_start_time_unix_s: None,
        };
        let signal = FakeInterruptSource::new(vec![false, false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(1)]);
        let mut stdout = BrokenPipeWriter::new(|| {
            request_stop_for_test(&state_path, &generation_one_identity);
        });
        let mut stderr = Vec::new();

        let outcome = supervise_running_server_with(
            RunSession {
                id: &server.id,
                state_identity: &run.identity(),
                log_path: run.log_path.as_path(),
                state_path: &state_path,
            },
            &mut child,
            &signal,
            &mut stdout,
            &mut stderr,
            owner_teardown_child,
            |_| panic!("reaped exit must return before sleeping"),
        )
        .expect("restart announcement BrokenPipe must not drop the owned handoff");
        let RunOutcome::Restart { run: replacement } = outcome else {
            panic!("expected owned replacement handoff, got {outcome:?}");
        };
        let spawn_count = Cell::new(0_u8);

        let boundary = prepare_and_spawn_after_persisted_starting_run(
            &state_path,
            replacement,
            || Ok(()),
            |_| {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect("committed stop must win after the non-fatal announcement");

        assert!(matches!(boundary, SpawnBoundary::RequestedStop));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(stdout.write_attempts, 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn startup_finalization_error_after_teardown_does_not_retry_cleanup() {
        let temp = TempDir::new("loxa-startup-finalization-error");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let stopped = request_stop_for_test(&state_path, &run.identity());
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child = FakeStartupChild::with_wait_results(vec![Some(0)]);
        let finalizations = Cell::new(0_u8);

        let error = wait_for_startup_with_finalizer(
            &mut child,
            &stopped.identity(),
            &state_path,
            &signal,
            |_, _, _| panic!("durable stop must win before startup polling"),
            owner_teardown_child,
            |_, _, _, confirmation| {
                assert_eq!(confirmation, supervisor::TeardownConfirmation::Confirmed);
                finalizations.set(finalizations.get() + 1);
                Err(SupervisorError::Io(io::Error::other(
                    "injected exact-finalization failure",
                )))
            },
        )
        .expect_err("finalization failure");

        assert!(matches!(error, StartupWaitFailure::AfterTeardown(_)));
        assert_eq!(finalizations.get(), 1);
        assert_eq!(
            child.events.into_inner(),
            vec!["terminate", "try_wait", "join_log_drains"]
        );
        let RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).expect("read preserved stopped state")
        else {
            panic!("expected loaded state");
        };
        assert_eq!(runs, vec![stopped]);
    }

    fn assert_startup_reaped_diagnostic_failure_never_resignals(drain_fails: bool) {
        let temp = TempDir::new("loxa-startup-reaped-diagnostics");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 65_535,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        if drain_fails {
            fs::write(
                &run.log_path,
                b"diagnostics exist but drain joining failed\n",
            )
            .expect("write crash log for drain failure");
        }
        let signal = FakeInterruptSource::new(vec![false]);
        let mut child =
            FakeStartupChild::with_wait_results_and_drain_error(vec![Some(1)], drain_fails);

        let failure = wait_for_startup(
            &mut child,
            &run.identity(),
            &state_path,
            &signal,
            |child, _, _| match supervisor::wait_for_health_or_exit(
                child,
                server.port,
                &run.log_path,
                Duration::ZERO,
                Duration::ZERO,
            ) {
                Ok(()) => Ok(StartupPoll::Ready),
                Err(SupervisorError::HealthTimeout) => Ok(StartupPoll::Pending),
                Err(error) => Err(error),
            },
            owner_teardown_child,
        )
        .expect_err("reaped child diagnostic failure");

        let observed_exit = match failure {
            StartupWaitFailure::AfterChildReaped {
                log_tail,
                diagnostics_error,
            } => Some(
                supervisor::decide_observed_child_exit(
                    diagnostics_error
                        .map(|error| format!("crash diagnostics unavailable: {error}"))
                        .unwrap_or(log_tail),
                    &state_path,
                    &run.identity(),
                    &signal,
                    |decision| owner_teardown_child(&mut child, decision),
                )
                .expect("transition already-reaped startup child"),
            ),
            StartupWaitFailure::BeforeTeardown(_) => {
                let _ = supervisor::cleanup_after_ctrl_c(
                    &mut child,
                    &state_path,
                    &run.identity(),
                    supervisor::CTRL_C_GRACE_PERIOD,
                );
                let _ = child.join_log_drains();
                None
            }
            StartupWaitFailure::AfterTeardown(_) => None,
        };

        let events = child.events.into_inner();
        assert_eq!(
            events.iter().filter(|event| **event == "terminate").count(),
            0,
            "an already-reaped PID must never be terminated again: {events:?}"
        );
        assert_eq!(
            events.iter().filter(|event| **event == "kill").count(),
            0,
            "an already-reaped PID must never be killed again: {events:?}"
        );
        assert!(matches!(
            observed_exit,
            Some(ObservedChildExit::Restart { .. })
        ));
    }

    #[test]
    fn startup_reaped_drain_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(true);
    }

    #[test]
    fn startup_reaped_log_tail_failure_never_resignals_child() {
        assert_startup_reaped_diagnostic_failure_never_resignals(false);
    }

    #[test]
    fn startup_restart_announcement_broken_pipe_retains_handoff_and_cas_conflict_wins() {
        let temp = TempDir::new("loxa-startup-restart-broken-pipe");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = persist_run_for_server(&state_path, &server);
        let signal = FakeInterruptSource::new(vec![false]);
        let observed = supervisor::decide_observed_child_exit(
            "startup crash".to_string(),
            &state_path,
            &run.identity(),
            &signal,
            |decision| {
                assert_eq!(decision, supervisor::OwnerTeardownDecision::UnexpectedExit);
                supervisor::TeardownConfirmation::Confirmed
            },
        )
        .expect("publish owned generation one");
        let ObservedChildExit::Restart { run: replacement } = observed else {
            panic!("expected replacement handoff, got {observed:?}");
        };
        let replacement_identity = replacement.identity();
        let newer_run = starting_run_for_test(&state_path, "newer-run");
        let newer_for_write = newer_run.clone();
        let mut stdout = BrokenPipeWriter::new(|| {
            assert!(
                supervisor::finish_runtime_state_run(&state_path, &replacement_identity)
                    .expect("exact-finish generation one during failed announcement")
            );
            supervisor::create_starting_run(&state_path, newer_for_write)
                .expect("publish newer state during failed announcement");
        });

        let replacement = retain_restart_after_best_effort_announcement(
            &mut stdout,
            "llama-server exited before becoming healthy; restarting once...",
            replacement,
        );
        let spawn_count = Cell::new(0_u8);
        let error = prepare_and_spawn_after_persisted_starting_run(
            &state_path,
            replacement,
            || Ok(()),
            |_| {
                spawn_count.set(spawn_count.get() + 1);
                Ok(())
            },
        )
        .expect_err("newer exact state must beat the stale generation-one handoff");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(spawn_count.get(), 0);
        assert_eq!(stdout.write_attempts, 1);
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read newer state"),
            RuntimeStateRead::Loaded(vec![newer_run])
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

    impl InterruptStatus for FakeInterruptSource {
        fn interrupted(&self) -> bool {
            InterruptSource::interrupted(self)
        }
    }

    struct FakeStartupChild {
        events: RefCell<Vec<&'static str>>,
        wait_results: RefCell<Vec<Option<i32>>>,
        drain_error: bool,
    }

    impl FakeStartupChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self::with_wait_results_and_drain_error(wait_results, false)
        }

        fn with_wait_results_and_drain_error(
            wait_results: Vec<Option<i32>>,
            drain_error: bool,
        ) -> Self {
            Self {
                events: RefCell::new(Vec::new()),
                wait_results: RefCell::new(wait_results),
                drain_error,
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
            if self.drain_error {
                Err(SupervisorError::Io(io::Error::other(
                    "injected log-drain join failure",
                )))
            } else {
                Ok(())
            }
        }
    }

    struct BrokenPipeWriter<F> {
        on_write: Option<F>,
        write_attempts: usize,
    }

    impl<F> BrokenPipeWriter<F> {
        fn new(on_write: F) -> Self {
            Self {
                on_write: Some(on_write),
                write_attempts: 0,
            }
        }
    }

    impl<F: FnOnce()> Write for BrokenPipeWriter<F> {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            self.write_attempts += 1;
            if let Some(on_write) = self.on_write.take() {
                on_write();
            }
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected restart announcement failure",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
