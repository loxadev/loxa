use super::child::ChildProcessGuard;
use super::output::join_output_reader;
#[cfg(unix)]
use super::service_transport::{
    authenticate_unix_endpoint, remove_owned_unix_endpoint, UnixEndpointIdentity,
};
use crate::ui;
use std::path::PathBuf;
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

pub(super) mod readiness;
mod startup;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupInterruption {
    Cancelled,
}

pub(super) enum StartOutcome {
    Ready(Box<OwnedServer>),
    Exited(ServerExit),
    Signaled(i32),
    Interrupted(StartupInterruption),
    CleanupFailed(Box<OwnedServer>),
}

#[derive(Clone, Copy)]
pub(super) enum StartupStop {
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

#[derive(Debug)]
pub(crate) struct ServerExit {
    pub(super) code: i32,
    pub(super) diagnostic: Option<String>,
}

pub(crate) fn report_exit(exit: ServerExit) -> i32 {
    if let Some(diagnostic) = exit.diagnostic {
        let diagnostic = ui::sanitize_terminal(&diagnostic);
        eprintln!("{diagnostic}");
    }
    exit.code
}

pub struct OwnedServer {
    pub(super) child: Option<ChildProcessGuard>,
    #[cfg(test)]
    pub(super) group: i32,
    port: u16,
    announcements: mpsc::Receiver<Result<u16, String>>,
    announcement_overflow: Arc<AtomicBool>,
    announced_port: Option<u16>,
    stdout_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    stdout_tail: Vec<u8>,
    stderr_tail: Vec<u8>,
    retain_cleanup_failure: bool,
    unix_socket: Option<PathBuf>,
    #[cfg(unix)]
    unix_socket_identity: Option<UnixEndpointIdentity>,
    #[cfg(unix)]
    service_runtime: Option<tokio::runtime::Handle>,
}

impl OwnedServer {
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
            tracing::warn!(target: "loxa::runner", event = "server_start_cleanup_failed");
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

    #[cfg(unix)]
    fn record_unix_endpoint_identity(
        &mut self,
        identity: UnixEndpointIdentity,
    ) -> Result<(), String> {
        match self.unix_socket_identity {
            Some(expected) if expected != identity => {
                Err("service engine endpoint identity changed between requests".into())
            }
            Some(_) => Ok(()),
            None => {
                self.unix_socket_identity = Some(identity);
                Ok(())
            }
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn try_wait(&mut self) -> Result<Option<ServerExit>, String> {
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
        #[cfg(unix)]
        if self.unix_socket_identity.is_none() {
            let endpoint = self.unix_socket.clone();
            let runtime = self.service_runtime.clone();
            let pid = self.child.as_ref().and_then(ChildProcessGuard::active_id);
            if let (Some(endpoint), Some(runtime), Some(pid)) = (endpoint, runtime, pid) {
                if let Ok(Some(identity)) = authenticate_unix_endpoint(&runtime, &endpoint, pid) {
                    self.record_unix_endpoint_identity(identity)?;
                }
            }
        }
        if let Some(child) = self.child.as_mut() {
            let pid = child.active_id();
            if let Some(pid) = pid {
                tracing::info!(target: "loxa::runner", event = "server_terminating", pid, port = self.port);
            }
            child.terminate()?;
            if let Some(pid) = pid {
                tracing::info!(target: "loxa::runner", event = "server_terminated", pid, port = self.port);
            }
        }
        self.join_output_readers()?;
        if let Some(endpoint) = &self.unix_socket {
            #[cfg(unix)]
            remove_owned_unix_endpoint(endpoint, self.unix_socket_identity)?;
        }
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
    pub(super) fn output_readers_owned(&self) -> bool {
        self.stdout_reader.is_some() || self.stderr_reader.is_some()
    }
}

impl Drop for OwnedServer {
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
