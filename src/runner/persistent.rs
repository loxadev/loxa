use super::child::{ChildProcessGuard, PersistentSignalPolicy};
use super::launch::{report_mtp_draft_start_failure, LaunchPolicy};
use super::owned::{OwnedServer, ServerExit, StartOutcome, StartupInterruption};
use super::signal::install_termination_watcher;
use super::STARTUP_TIMEOUT;
use std::path::Path;
use std::time::Instant;

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

pub(crate) struct PersistentServer {
    // Fields drop in this order: server teardown precedes the runnable's ModelLock release.
    pub(super) server: Box<OwnedServer>,
    runnable: crate::runnable::Runnable,
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
    tracing::info!(target: "loxa::runner",
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
                tracing::warn!(target: "loxa::runner",
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
            tracing::info!(target: "loxa::runner",
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

pub(crate) fn start_service_with_ownership<F>(
    mut runnable: crate::runnable::Runnable,
    ownership: &crate::runtime::RuntimeOwnership,
    endpoint: &Path,
    runtime_handle: &tokio::runtime::Handle,
    cancelled: F,
) -> Result<PersistentStart, PersistentStartError>
where
    F: Fn() -> bool,
{
    if runnable.launch().policy != LaunchPolicy::Service {
        return Err(PersistentStartError::Failed(
            "service start requires the service launch policy".into(),
        ));
    }
    if cancelled() {
        return Ok(persistent_interrupted());
    }
    let launch_started = Instant::now();
    let child_ownership = ownership.reserve_child()?;
    let started = OwnedServer::start_with_service_ownership(
        runnable.launch(),
        runnable.fingerprint(),
        endpoint,
        runtime_handle,
        STARTUP_TIMEOUT,
        child_ownership,
        &cancelled,
    )?;
    match started {
        StartOutcome::Ready(server) => {
            finish_persistent_ready(server, runnable, launch_started, false, &cancelled)
                .map_err(PersistentStartError::from)
        }
        StartOutcome::Exited(exit) => {
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            if runnable.primary_only_for_service().is_none() {
                return Ok(PersistentStart::Stopped(exit));
            }
            report_mtp_draft_start_failure(runnable.launch(), "exited", exit.diagnostic.as_deref());
            if cancelled() {
                return Ok(persistent_interrupted());
            }
            tracing::info!(target: "loxa::runner",
                event = "gemma_mtp_primary_retry",
                model_id = %runnable.launch().id,
                attempt = 2_u8
            );
            let child_ownership = ownership.reserve_child()?;
            match OwnedServer::start_with_service_ownership(
                runnable.launch(),
                runnable.fingerprint(),
                endpoint,
                runtime_handle,
                STARTUP_TIMEOUT,
                child_ownership,
                &cancelled,
            )? {
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
                    "service startup returned an invalid signal interruption".into(),
                )),
            }
        }
        StartOutcome::Interrupted(interruption) => Ok(PersistentStart::Interrupted(interruption)),
        StartOutcome::CleanupFailed(server) => Ok(persistent_cleanup_failed(server, runnable)),
        StartOutcome::Signaled(_) => Err(PersistentStartError::Failed(
            "service startup returned an invalid signal interruption".into(),
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
        tracing::info!(target: "loxa::runner",
            event = "server_ready",
            model_id = %runnable.launch().id,
            port = server.port(),
            elapsed_ms = launch_started.elapsed().as_millis() as u64,
            effective_profile = runnable.launch().profile.effective_name(),
            fallback = "primary_only",
        );
    } else {
        tracing::info!(target: "loxa::runner",
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

impl PersistentServer {
    pub(crate) fn port(&self) -> u16 {
        self.server.port()
    }

    pub(crate) fn fingerprint(&self) -> &crate::runtime_fingerprint::RuntimeFingerprint {
        self.runnable.fingerprint()
    }

    pub(crate) fn pid(&self) -> Option<u32> {
        self.server
            .child
            .as_ref()
            .and_then(ChildProcessGuard::active_id)
    }

    pub(crate) fn poll(&mut self) -> Result<Option<ServerExit>, String> {
        self.server.try_wait()
    }

    pub(crate) fn terminate(&mut self) -> Result<(), String> {
        self.server.terminate()
    }
}
