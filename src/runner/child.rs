use super::launch::PreparedRuntimeGuard;
use super::signal::{activate_server, deactivate_server};
use std::process::{Child, Command};
#[cfg(all(test, unix))]
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Clone, Copy)]
pub(super) enum ChildTerminationMode {
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

/// Keeps the child, execution stage, and runtime reservation together until cleanup.
pub(super) struct ChildProcessGuard {
    child: Option<Child>,
    group: i32,
    active_server: bool,
    termination: ChildTerminationMode,
    prepared: PreparedRuntimeGuard,
    runtime: Option<crate::runtime::RuntimeChildOwnership>,
}

impl ChildProcessGuard {
    pub(super) fn spawn(
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

    pub(super) fn id(&self) -> u32 {
        self.child.as_ref().expect("guarded child is present").id()
    }

    pub(super) fn active_id(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    pub(super) fn group(&self) -> i32 {
        self.group
    }

    pub(super) fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("guarded child is present")
    }

    pub(super) fn terminate(&mut self) -> Result<(), String> {
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
            if let Some(prepared) = &self.prepared {
                runtime.clear_preserving_prepared_stage(prepared)?;
            } else {
                runtime.clear()?;
            }
        }
        Ok(())
    }

    pub(super) fn runtime_mut(&mut self) -> Option<&mut crate::runtime::RuntimeChildOwnership> {
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

#[cfg(all(test, unix))]
pub(super) static LAST_GUARDED_GROUP: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
pub(super) fn terminate_owned_group(child: &mut Child, group: i32) -> Result<(), String> {
    crate::runtime::terminate_process_group(child, group)
}

#[cfg(unix)]
#[cfg(test)]
pub(super) fn process_group_exists(group: i32) -> Result<bool, String> {
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
pub(super) fn terminate_owned_group(child: &mut Child, _group: i32) -> Result<(), String> {
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(error) => return Err(error.to_string()),
    }
    let _ = child.wait().map_err(|error| error.to_string())?;
    Ok(())
}
