use super::lease::{LeaseOwnerMode, RuntimeLease};
use super::process::{command_has_unique_option, process_group, process_snapshot};
use super::{foreground_lock_is_held, lease_is_absent, read_lease};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const OBSERVER_TEARDOWN_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeProvenance {
    Managed,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeOwner {
    Legacy,
    Foreground,
    PersistentApp,
    Service,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ForegroundObservation {
    Idle,
    Starting,
    Running {
        provenance: RuntimeProvenance,
        owner: RuntimeOwner,
        model_id: String,
        port: u16,
    },
    Stopping,
    Error,
}

// Observation owns only its prior view and grace timer, never runtime authority.
pub(crate) struct ForegroundObserver {
    run_dir: PathBuf,
    previously_running: bool,
    mismatch_started: Option<Instant>,
}

impl ForegroundObserver {
    pub(crate) fn new(run_dir: PathBuf) -> Self {
        Self {
            run_dir,
            previously_running: false,
            mismatch_started: None,
        }
    }

    pub(crate) fn observe(&mut self, managed_server: &Path) -> ForegroundObservation {
        let lock_path = self.run_dir.join("foreground.lock");
        let lease_path = self.run_dir.join("foreground.json");
        match foreground_lock_is_held(&lock_path) {
            Ok(false) => {
                self.previously_running = false;
                self.mismatch_started = None;
                if lease_is_absent(&lease_path) {
                    ForegroundObservation::Idle
                } else {
                    ForegroundObservation::Error
                }
            }
            Err(_) => {
                self.previously_running = false;
                self.mismatch_started = None;
                ForegroundObservation::Error
            }
            Ok(true) => self.observe_held_lease(&lease_path, managed_server),
        }
    }

    fn observe_held_lease(
        &mut self,
        lease_path: &Path,
        managed_server: &Path,
    ) -> ForegroundObservation {
        let lease = match read_observed_lease(lease_path) {
            ObservedLease::Absent | ObservedLease::Invalid => {
                self.mismatch_started = None;
                return if self.previously_running {
                    ForegroundObservation::Stopping
                } else {
                    ForegroundObservation::Starting
                };
            }
            ObservedLease::Valid(lease) => lease,
        };
        let provenance = match exact_live_provenance(&lease, managed_server) {
            Ok(Some(provenance)) => provenance,
            Ok(None) | Err(_) => return self.observe_identity_mismatch(),
        };
        self.previously_running = true;
        self.mismatch_started = None;
        let owner = match lease.owner_mode {
            None => RuntimeOwner::Legacy,
            Some(LeaseOwnerMode::Foreground) => RuntimeOwner::Foreground,
            Some(LeaseOwnerMode::PersistentApp) => RuntimeOwner::PersistentApp,
            Some(LeaseOwnerMode::Service) => RuntimeOwner::Service,
        };
        ForegroundObservation::Running {
            provenance,
            owner,
            model_id: lease.model_id.clone(),
            port: lease.port,
        }
    }

    fn observe_identity_mismatch(&mut self) -> ForegroundObservation {
        if !self.previously_running {
            return ForegroundObservation::Error;
        }
        let started = self.mismatch_started.get_or_insert_with(Instant::now);
        if started.elapsed() < OBSERVER_TEARDOWN_GRACE {
            ForegroundObservation::Stopping
        } else {
            self.previously_running = false;
            ForegroundObservation::Error
        }
    }
}

enum ObservedLease {
    Absent,
    Invalid,
    Valid(Box<RuntimeLease>),
}

fn read_observed_lease(path: &Path) -> ObservedLease {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ObservedLease::Absent,
        Err(_) => ObservedLease::Invalid,
        Ok(_) => match read_lease(path) {
            Ok(lease) => ObservedLease::Valid(Box::new(lease)),
            Err(_) => ObservedLease::Invalid,
        },
    }
}

pub(super) fn exact_live_provenance(
    lease: &RuntimeLease,
    managed_server: &Path,
) -> Result<Option<RuntimeProvenance>, String> {
    let Some(owner) = process_snapshot(lease.owner_pid)? else {
        return Ok(None);
    };
    if owner.start_identity != lease.owner_start_time {
        return Ok(None);
    }
    let Some(child) = process_snapshot(lease.child_pid)? else {
        return Ok(None);
    };
    if child.start_identity != lease.child_start_time
        || child.executable != lease.server
        || process_group(lease.child_pid)? != lease.child_pgid
        || !command_has_unique_option(&child.command, "--alias", OsStr::new(&lease.model_id))
        || !command_has_unique_option(
            &child.command,
            "--port",
            OsStr::new(&lease.port.to_string()),
        )
    {
        return Ok(None);
    }
    Ok(Some(
        if lease.attributed_managed_source() == managed_server {
            RuntimeProvenance::Managed
        } else {
            RuntimeProvenance::External
        },
    ))
}
