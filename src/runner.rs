//! Launch policy and lifecycle for exact owned engine processes.
//! Child guards retain runtime authority through verified teardown.

mod arguments;
mod child;
mod discovery;
mod foreground;
mod launch;
mod output;
mod owned;
mod persistent;
#[cfg(unix)]
mod service_transport;
mod signal;

pub(crate) use arguments::build_persistent_args_for_fingerprint;
pub(crate) use discovery::discover_from_process;
#[allow(unused_imports)]
pub(crate) use discovery::validate_managed_server;
pub use discovery::{discover_server, validate_managed_runtime};
pub(crate) use discovery::{
    revalidate_managed_runtime_for_service, validate_managed_runtime_for_service, VersionProbeError,
};
pub use foreground::run;
pub(crate) use foreground::{run_launch, start_foreground, ForegroundServer, ForegroundStart};
pub use launch::ValidatedManagedRuntime;
pub(crate) use launch::{Launch, LaunchPolicy, LaunchProfile};
pub use owned::readiness::models_body_has_alias;
pub(crate) use owned::readiness::probe_model_alias;
pub use owned::OwnedServer;
pub(crate) use owned::{report_exit, StartupInterruption};
pub(crate) use persistent::{
    start_persistent, start_service_with_ownership, PersistentServer, PersistentStart,
    PersistentStartError,
};

const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[cfg(test)]
use {
    arguments::build_args, child::PersistentSignalPolicy, owned::ServerExit,
    persistent::start_persistent_with_ownership,
};

#[cfg(test)]
mod tests;
