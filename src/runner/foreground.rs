use super::launch::{report_mtp_draft_start_failure, Launch};
use super::owned::{report_exit, OwnedServer, ServerExit, StartOutcome};
use super::signal::{install_termination_watcher, process_termination_signal};
use super::STARTUP_TIMEOUT;
use crate::ui;
use std::path::Path;
use std::time::{Duration, Instant};

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

pub(super) fn ready_line(port: u16, id: &str) -> String {
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

pub(crate) struct ForegroundServer {
    pub(super) server: Box<OwnedServer>,
}

pub(crate) fn start_foreground(launch: &Launch, run_dir: &Path) -> Result<ForegroundStart, String> {
    install_termination_watcher(run_dir)?;
    start_foreground_with(launch, run_dir, process_termination_signal)
}

pub(super) fn start_foreground_with<F>(
    launch: &Launch,
    run_dir: &Path,
    signal: F,
) -> Result<ForegroundStart, String>
where
    F: Fn() -> Option<i32>,
{
    let launch_started = Instant::now();
    tracing::info!(target: "loxa::runner",
        event = "server_starting",
        model_id = %launch.id,
        requested_port = launch.requested_port,
        context_size = launch.ctx
    );
    let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
    match OwnedServer::start_with_ownership(launch, STARTUP_TIMEOUT, ownership, &signal) {
        Ok(StartOutcome::Ready(server)) => {
            tracing::info!(target: "loxa::runner",
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
                tracing::warn!(target: "loxa::runner",
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

pub(super) fn stopped_for_signal<F>(signal: &F) -> Option<ForegroundStart>
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

fn start_mtp_primary_retry<F>(
    launch: &Launch,
    run_dir: &Path,
    signal: &F,
    launch_started: Instant,
) -> Result<ForegroundStart, String>
where
    F: Fn() -> Option<i32>,
{
    tracing::info!(target: "loxa::runner",
        event = "gemma_mtp_primary_retry",
        model_id = %launch.id,
        attempt = 2_u8
    );
    let ownership = crate::runtime::RuntimeOwnership::acquire(run_dir)?;
    match OwnedServer::start_with_ownership(launch, STARTUP_TIMEOUT, ownership, signal)? {
        StartOutcome::Ready(server) => {
            tracing::info!(target: "loxa::runner",
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
            tracing::warn!(target: "loxa::runner",
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
pub(super) fn start_foreground_with_signal<F>(
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
