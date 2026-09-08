#[cfg(test)]
mod tests;

use crate::catalog::Manifest;
use crate::cli::RuntimeArgs;
use crate::paths::AppPaths;
use crate::runnable::expected_persistent_fingerprints;
use crate::runner::{report_exit, ForegroundServer};
use crate::runtime::AttachedRuntime;
use crate::runtime::{PersistentRuntimeLookup, RuntimePresence};
use crate::runtime_fingerprint::RuntimeFingerprint;
use std::path::Path;

const ATTACHED_RUNTIME_UNAVAILABLE: &str =
    "the attached runtime is no longer available; restart chat";
const CHAT_OVERRIDE_CONFLICT: &str =
    "runtime overrides cannot be used while chat is attached to the running model";
const CHAT_ACTIVE_RUNTIME_CONFLICT: &str =
    "another Loxa runtime is active; unload it from the Loxa menu or stop its owning command, then retry chat";
const CHAT_CONFIGURATION_INVALID: &str = "chat runtime configuration is invalid";

pub(super) enum ChatRuntime {
    Owned {
        server: ForegroundServer,
        model_id: String,
        terminated: bool,
    },
    Attached(AttachedRuntime),
    Service {
        model_id: String,
        client: Box<loxa_ipc::ServiceClient>,
        target: loxa_ipc::OperationTarget,
        observer: tokio::runtime::Runtime,
        last_validated: std::time::Instant,
    },
    #[cfg(test)]
    OwnedTest {
        model_id: String,
        port: u16,
        poll: Box<dyn FnMut() -> Result<Option<i32>, String>>,
        terminate: Box<dyn FnMut() -> Result<(), String>>,
        terminated: bool,
    },
}

pub(crate) enum ChatRoute {
    Attached(AttachedRuntime),
    Foreground,
}

pub(crate) fn route_chat(
    manifest: Option<&Manifest>,
    runtime: &RuntimeArgs,
    paths: &AppPaths,
) -> Result<ChatRoute, String> {
    route_chat_with(
        manifest,
        runtime,
        paths,
        crate::runtime::lookup_persistent_runtime,
        crate::runtime::lookup_runtime_presence,
    )
}

fn route_chat_with<Lookup, Presence>(
    manifest: Option<&Manifest>,
    runtime: &RuntimeArgs,
    paths: &AppPaths,
    lookup: Lookup,
    presence: Presence,
) -> Result<ChatRoute, String>
where
    Lookup: FnOnce(&Path, &Path, &Path, &[RuntimeFingerprint]) -> PersistentRuntimeLookup,
    Presence: FnOnce(&Path) -> RuntimePresence,
{
    let state = if let Some(manifest) = manifest {
        let candidates = expected_persistent_fingerprints(manifest, paths)
            .map_err(|_| CHAT_CONFIGURATION_INVALID.to_string())?;
        match lookup(
            &paths.run,
            &paths.models,
            &paths.managed_server,
            candidates.as_slice(),
        ) {
            PersistentRuntimeLookup::NoRuntime => RuntimePresence::NoRuntime,
            PersistentRuntimeLookup::ActiveButNotAttachable => RuntimePresence::Active,
            PersistentRuntimeLookup::Attached(attached) => {
                if has_explicit_runtime_override(runtime) {
                    return Err(CHAT_OVERRIDE_CONFLICT.into());
                }
                return Ok(ChatRoute::Attached(attached));
            }
        }
    } else {
        presence(&paths.run)
    };
    match state {
        RuntimePresence::NoRuntime => Ok(ChatRoute::Foreground),
        RuntimePresence::Active => Err(CHAT_ACTIVE_RUNTIME_CONFLICT.into()),
    }
}

fn has_explicit_runtime_override(runtime: &RuntimeArgs) -> bool {
    runtime.ctx.is_some() || runtime.port.is_some() || runtime.server.is_some()
}

pub(super) fn with_runtime_teardown(
    mut runtime: ChatRuntime,
    operation: impl FnOnce(&mut ChatRuntime) -> Result<i32, String>,
) -> Result<i32, String> {
    let outcome = operation(&mut runtime);
    runtime.terminate_owned()?;
    outcome
}

impl ChatRuntime {
    pub(super) fn owned(server: ForegroundServer, model_id: String) -> Self {
        Self::Owned {
            server,
            model_id,
            terminated: false,
        }
    }

    pub(super) fn attached(runtime: AttachedRuntime) -> Self {
        Self::Attached(runtime)
    }
    pub(super) fn service(
        model_id: String,
        client: loxa_ipc::ServiceClient,
        target: loxa_ipc::OperationTarget,
    ) -> Result<Self, String> {
        let observer = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        observer.block_on(crate::service::attachment::validate_target(
            &client, &model_id, &target,
        ))?;
        Ok(Self::Service {
            model_id,
            client: Box::new(client),
            target,
            observer,
            last_validated: std::time::Instant::now(),
        })
    }

    #[cfg(test)]
    pub(super) fn owned_for_test(
        model_id: &str,
        port: u16,
        poll: impl FnMut() -> Result<Option<i32>, String> + 'static,
        terminate: impl FnMut() -> Result<(), String> + 'static,
    ) -> Self {
        Self::OwnedTest {
            model_id: model_id.to_owned(),
            port,
            poll: Box::new(poll),
            terminate: Box::new(terminate),
            terminated: false,
        }
    }

    pub(super) fn model_id(&self) -> &str {
        match self {
            Self::Owned { model_id, .. } => model_id,
            Self::Attached(runtime) => runtime.model_id(),
            Self::Service { model_id, .. } => model_id,
            #[cfg(test)]
            Self::OwnedTest { model_id, .. } => model_id,
        }
    }

    pub(super) fn port(&self) -> Result<u16, String> {
        match self {
            Self::Owned { server, .. } => Ok(server.port()),
            Self::Attached(runtime) => Ok(runtime.port()),
            Self::Service { .. } => Err("service chat has no TCP endpoint".into()),
            #[cfg(test)]
            Self::OwnedTest { port, .. } => Ok(*port),
        }
    }

    pub(super) fn is_attached(&self) -> bool {
        matches!(self, Self::Attached(_) | Self::Service { .. })
    }

    pub(super) fn poll(&mut self) -> Result<Option<i32>, String> {
        match self {
            Self::Owned { server, .. } => server.poll().map(|exit| exit.map(report_exit)),
            Self::Attached(runtime) => runtime
                .revalidate()
                .map(|()| None)
                .map_err(|_| ATTACHED_RUNTIME_UNAVAILABLE.to_string()),
            Self::Service {
                model_id,
                client,
                target,
                observer,
                last_validated,
            } => {
                if last_validated.elapsed() >= crate::service::attachment::TARGET_POLL_INTERVAL {
                    observer.block_on(crate::service::attachment::validate_target(
                        client, model_id, target,
                    ))?;
                    *last_validated = std::time::Instant::now();
                }
                Ok(None)
            }
            #[cfg(test)]
            Self::OwnedTest { poll, .. } => poll(),
        }
    }

    pub(super) fn terminate_owned(&mut self) -> Result<(), String> {
        match self {
            Self::Owned {
                server, terminated, ..
            } => {
                if *terminated {
                    return Ok(());
                }
                *terminated = true;
                server.terminate()
            }
            Self::Attached(_) => Ok(()),
            Self::Service { .. } => Ok(()),
            #[cfg(test)]
            Self::OwnedTest {
                terminate,
                terminated,
                ..
            } => {
                if *terminated {
                    return Ok(());
                }
                *terminated = true;
                terminate()
            }
        }
    }
}
