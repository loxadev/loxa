use crate::runtime_fingerprint::{EffectiveProfile, PERSISTENT_SLEEP_IDLE_SECONDS};
use crate::runtime_identity::RuntimeIdentity;
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(unix)]
pub(super) type PreparedRuntimeGuard = Option<crate::runtime_bundle::PreparedRuntime>;
#[cfg(not(unix))]
pub(super) type PreparedRuntimeGuard = ();

#[cfg(unix)]
pub(super) fn no_prepared_runtime_guard() -> PreparedRuntimeGuard {
    None
}

#[cfg(not(unix))]
pub(super) fn no_prepared_runtime_guard() -> PreparedRuntimeGuard {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LaunchPolicy {
    Foreground,
    PersistentApp,
    Service,
}

impl LaunchPolicy {
    pub(crate) fn sleep_idle_seconds(self) -> Option<u64> {
        match self {
            Self::Foreground => None,
            Self::PersistentApp => Some(PERSISTENT_SLEEP_IDLE_SECONDS),
            Self::Service => None,
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

    pub(super) fn required_version(&self, runtime_identity: RuntimeIdentity) -> Option<&str> {
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

    pub(super) fn draft(&self) -> Option<&Path> {
        match self {
            Self::Generic => None,
            Self::Gemma4Mtp { draft, .. } => draft.as_deref(),
        }
    }

    pub(super) fn effective_name(&self) -> &'static str {
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
    pub(super) fn generic(
        server: &Path,
        model: &Path,
        id: &str,
        requested_port: u16,
        ctx: u32,
    ) -> Self {
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
    pub(super) fn path(source_server: PathBuf) -> Self {
        Self {
            source_server,
            #[cfg(unix)]
            prepared: None,
        }
    }

    #[cfg(unix)]
    pub(super) fn bundled(
        source_server: PathBuf,
        prepared: crate::runtime_bundle::PreparedRuntime,
    ) -> Self {
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

    pub(super) fn command(&self) -> Command {
        #[cfg(unix)]
        if let Some(prepared) = &self.prepared {
            return prepared.command();
        }
        Command::new(&self.source_server)
    }

    #[cfg(unix)]
    pub(super) fn process_guard(&self) -> PreparedRuntimeGuard {
        self.prepared.clone()
    }

    pub(crate) fn revalidate_for_service_reuse(
        &self,
        paths: &crate::paths::AppPaths,
    ) -> Result<(), String> {
        #[cfg(unix)]
        if let Some(prepared) = &self.prepared {
            return prepared.revalidate_for_service_reuse(paths);
        }
        Err("managed runtime is not a bundled prepared runtime".into())
    }

    #[cfg(not(unix))]
    pub(super) fn process_guard(&self) -> PreparedRuntimeGuard {}
}

impl Launch {
    pub(super) fn server_command(&self) -> Command {
        self.managed_runtime.as_ref().map_or_else(
            || Command::new(&self.server),
            ValidatedManagedRuntime::command,
        )
    }

    pub(super) fn managed_source_server(&self) -> Option<&Path> {
        self.managed_runtime
            .as_ref()
            .map(ValidatedManagedRuntime::source_server)
    }

    pub(crate) fn managed_runtime(&self) -> Option<&ValidatedManagedRuntime> {
        self.managed_runtime.as_ref()
    }
}

pub(super) fn report_mtp_draft_start_failure(
    launch: &Launch,
    outcome: &'static str,
    diagnostic: Option<&str>,
) {
    tracing::warn!(target: "loxa::runner",
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
