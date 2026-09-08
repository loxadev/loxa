use crate::paths::AppPaths;
use crate::runner::{PersistentServer, PersistentStart, PersistentStartError, StartupInterruption};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::fmt;
use std::io::Read as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MAX_PROPS_BODY: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiRuntimeEndpoint {
    model_id: String,
    port: u16,
}

impl ApiRuntimeEndpoint {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApiStartCancellation(Arc<AtomicBool>);

impl ApiStartCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiStopOutcome {
    Stopped,
    AlreadyStopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApiStopError;

impl fmt::Display for ApiStopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the API runtime could not be stopped")
    }
}

impl std::error::Error for ApiStopError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiStartOutcome {
    Started(ApiRuntimeEndpoint),
    AlreadyRunning(ApiRuntimeEndpoint),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiStartError {
    Conflict,
    Cancelled,
    ModelUnavailable,
    StartupFailed,
}

impl fmt::Display for ApiStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "another Loxa model operation is active",
            Self::Cancelled => "API startup was cancelled",
            Self::ModelUnavailable => "the selected installed model is unavailable",
            Self::StartupFailed => "the API runtime could not be started",
        })
    }
}

impl std::error::Error for ApiStartError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiRuntimeActivity {
    Loaded,
    Sleeping,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiRuntimeProbe {
    Activity(ApiRuntimeActivity),
    Stopped,
    CleanupFailed,
}

enum OwnedRuntimePoll {
    Running(u16),
    Stopped,
    CleanupFailed,
}

pub struct ApiRuntimeHost {
    paths: AppPaths,
    runtime: Option<Box<PersistentServer>>,
    prepared_runtime: Option<crate::runner::ValidatedManagedRuntime>,
    preparation_cleanup_failed: bool,
    #[cfg(test)]
    fail_next_stop: bool,
}

impl ApiRuntimeHost {
    pub fn new(paths: AppPaths) -> Self {
        Self {
            paths,
            runtime: None,
            prepared_runtime: None,
            preparation_cleanup_failed: false,
            #[cfg(test)]
            fail_next_stop: false,
        }
    }

    pub fn endpoint(&self) -> Option<ApiRuntimeEndpoint> {
        self.runtime.as_ref().map(|runtime| ApiRuntimeEndpoint {
            model_id: runtime.fingerprint().model_id().to_owned(),
            port: runtime.port(),
        })
    }

    pub fn preparation_requires_recovery(&self) -> bool {
        self.preparation_cleanup_failed
    }

    pub fn stop(&mut self) -> Result<ApiStopOutcome, ApiStopError> {
        if self.preparation_cleanup_failed {
            // Only the existing identity-checked recovery may release a failed
            // preparation barrier. It leaves evidence intact on any uncertainty.
            let ownership = crate::runtime::RuntimeOwnership::acquire_persistent(&self.paths.run)
                .map_err(|_| ApiStopError)?;
            drop(ownership);
            self.preparation_cleanup_failed = false;
        }
        let Some(mut runtime) = self.runtime.take() else {
            return Ok(ApiStopOutcome::AlreadyStopped);
        };
        if self.terminate_runtime(&mut runtime).is_err() {
            self.runtime = Some(runtime);
            return Err(ApiStopError);
        }
        Ok(ApiStopOutcome::Stopped)
    }

    /// Qualify the bundled engine before the first model load, without acquiring
    /// a model or the common runtime lock. The caller runs this off the UI thread.
    pub fn prepare_runtime(
        &mut self,
        cancellation: &ApiStartCancellation,
    ) -> Result<(), ApiStartError> {
        if self.preparation_cleanup_failed {
            return Err(ApiStartError::StartupFailed);
        }
        if !self.paths.runtime_identity.is_bundled() || self.prepared_runtime.is_some() {
            return Ok(());
        }
        let runtime = crate::runner::prepare_managed_runtime(&self.paths, None, &|| {
            cancellation.is_cancelled()
        })
        .map_err(|error| self.map_preparation_error(error.into()))?;
        self.prepared_runtime = Some(runtime);
        Ok(())
    }

    fn map_preparation_error(
        &mut self,
        error: crate::runnable::ManagedRunnableError,
    ) -> ApiStartError {
        use crate::runnable::ManagedRunnableError;
        match error {
            ManagedRunnableError::Conflict => ApiStartError::Conflict,
            ManagedRunnableError::Cancelled => ApiStartError::Cancelled,
            ManagedRunnableError::ModelUnavailable(_diagnostic) => ApiStartError::ModelUnavailable,
            ManagedRunnableError::CleanupFailed(_diagnostic) => {
                self.preparation_cleanup_failed = true;
                self.prepared_runtime = None;
                ApiStartError::StartupFailed
            }
            ManagedRunnableError::StartupFailed(_diagnostic) => {
                self.prepared_runtime = None;
                ApiStartError::StartupFailed
            }
        }
    }

    pub fn start(
        &mut self,
        model_id: &str,
        cancellation: &ApiStartCancellation,
    ) -> Result<ApiStartOutcome, ApiStartError> {
        self.start_inner(model_id, cancellation, || {}, || {})
    }

    fn start_inner(
        &mut self,
        model_id: &str,
        cancellation: &ApiStartCancellation,
        after_admission: impl FnOnce(),
        after_ready: impl FnOnce(),
    ) -> Result<ApiStartOutcome, ApiStartError> {
        if self.preparation_cleanup_failed {
            return Err(ApiStartError::StartupFailed);
        }
        if let Some(endpoint) = self.endpoint() {
            return if endpoint.model_id() == model_id {
                Ok(ApiStartOutcome::AlreadyRunning(endpoint))
            } else {
                Err(ApiStartError::Conflict)
            };
        }
        if cancellation.is_cancelled() {
            return Err(ApiStartError::Cancelled);
        }
        let manifest = crate::catalog::load_catalog(&self.paths.models)
            .map_err(|_| ApiStartError::ModelUnavailable)?
            .into_iter()
            .find(|manifest| manifest.id == model_id)
            .ok_or(ApiStartError::ModelUnavailable)?;
        let runnable = crate::runnable::resolve_managed_runnable_for_host_reusing(
            manifest,
            &self.paths,
            self.prepared_runtime.clone(),
            &|| cancellation.is_cancelled(),
        )
        .map_err(|error| self.map_preparation_error(error))?;
        if self.paths.runtime_identity.is_bundled() {
            self.prepared_runtime = runnable.managed_runtime().cloned();
        }
        after_admission();
        if cancellation.is_cancelled() {
            return Err(ApiStartError::Cancelled);
        }
        let started = crate::runner::start_persistent(runnable, &self.paths.run, || {
            cancellation.is_cancelled()
        })
        .map_err(Self::map_start_error)?;
        let mut runtime = match started {
            PersistentStart::Ready(runtime) => runtime,
            PersistentStart::Stopped(_exit) => return Err(ApiStartError::StartupFailed),
            PersistentStart::Interrupted(StartupInterruption::Cancelled) => {
                return Err(ApiStartError::Cancelled)
            }
            PersistentStart::CleanupFailed(runtime) => {
                self.runtime = Some(runtime);
                return Err(ApiStartError::StartupFailed);
            }
        };
        after_ready();
        if cancellation.is_cancelled() {
            if self.terminate_runtime(&mut runtime).is_err() {
                self.runtime = Some(runtime);
                return Err(ApiStartError::StartupFailed);
            }
            return Err(ApiStartError::Cancelled);
        }
        let endpoint = ApiRuntimeEndpoint {
            model_id: runtime.fingerprint().model_id().to_owned(),
            port: runtime.port(),
        };
        self.runtime = Some(runtime);
        Ok(ApiStartOutcome::Started(endpoint))
    }

    fn map_start_error(error: PersistentStartError) -> ApiStartError {
        match error {
            PersistentStartError::Conflict => ApiStartError::Conflict,
            PersistentStartError::Failed(_diagnostic) => ApiStartError::StartupFailed,
        }
    }

    pub fn activity(&mut self) -> ApiRuntimeActivity {
        match self.probe() {
            ApiRuntimeProbe::Activity(activity) => activity,
            ApiRuntimeProbe::Stopped | ApiRuntimeProbe::CleanupFailed => {
                ApiRuntimeActivity::Unknown
            }
        }
    }

    pub fn probe(&mut self) -> ApiRuntimeProbe {
        self.probe_with(probe_activity)
    }

    fn probe_with(&mut self, probe: impl FnOnce(u16) -> ApiRuntimeActivity) -> ApiRuntimeProbe {
        let port = match self.poll_owned_runtime() {
            OwnedRuntimePoll::Running(port) => port,
            OwnedRuntimePoll::Stopped => return ApiRuntimeProbe::Stopped,
            OwnedRuntimePoll::CleanupFailed => return ApiRuntimeProbe::CleanupFailed,
        };
        let activity = probe(port);
        match self.poll_owned_runtime() {
            OwnedRuntimePoll::Running(_) => ApiRuntimeProbe::Activity(activity),
            OwnedRuntimePoll::Stopped => ApiRuntimeProbe::Stopped,
            OwnedRuntimePoll::CleanupFailed => ApiRuntimeProbe::CleanupFailed,
        }
    }

    fn poll_owned_runtime(&mut self) -> OwnedRuntimePoll {
        if self.preparation_cleanup_failed {
            return OwnedRuntimePoll::CleanupFailed;
        }
        let (port, result) = match self.runtime.as_mut() {
            Some(runtime) => (runtime.port(), runtime.poll()),
            None => return OwnedRuntimePoll::Stopped,
        };
        match result {
            Ok(None) => OwnedRuntimePoll::Running(port),
            Ok(Some(_exit)) => {
                self.runtime.take();
                OwnedRuntimePoll::Stopped
            }
            Err(_) => OwnedRuntimePoll::CleanupFailed,
        }
    }

    fn terminate_runtime(&mut self, runtime: &mut PersistentServer) -> Result<(), ApiStopError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_stop) {
            return Err(ApiStopError);
        }
        runtime.terminate().map_err(|_| ApiStopError)
    }

    #[cfg(test)]
    fn fail_next_stop_for_test(&mut self) {
        self.fail_next_stop = true;
    }

    #[cfg(test)]
    fn start_with_test_hooks(
        &mut self,
        model_id: &str,
        cancellation: &ApiStartCancellation,
        after_admission: impl FnOnce(),
        after_ready: impl FnOnce(),
    ) -> Result<ApiStartOutcome, ApiStartError> {
        self.start_inner(model_id, cancellation, after_admission, after_ready)
    }
}

impl Drop for ApiRuntimeHost {
    fn drop(&mut self) {
        if let Some(mut runtime) = self.runtime.take() {
            let _ = runtime.terminate();
        }
    }
}

#[derive(Deserialize)]
struct Props {
    is_sleeping: bool,
}

fn probe_activity(port: u16) -> ApiRuntimeActivity {
    let client = match Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(250))
        .timeout(Duration::from_secs(1))
        .build()
    {
        Ok(client) => client,
        Err(_) => return ApiRuntimeActivity::Unknown,
    };
    let mut response = match client.get(format!("http://127.0.0.1:{port}/props")).send() {
        Ok(response) => response,
        Err(_) => return ApiRuntimeActivity::Unknown,
    };
    if !response.status().is_success() {
        return ApiRuntimeActivity::Unknown;
    }
    let mut body = Vec::new();
    if response
        .by_ref()
        .take((MAX_PROPS_BODY + 1) as u64)
        .read_to_end(&mut body)
        .is_err()
        || body.len() > MAX_PROPS_BODY
    {
        return ApiRuntimeActivity::Unknown;
    }
    match serde_json::from_slice::<Props>(&body) {
        Ok(Props { is_sleeping: true }) => ApiRuntimeActivity::Sleeping,
        Ok(Props { is_sleeping: false }) => ApiRuntimeActivity::Loaded,
        Err(_) => ApiRuntimeActivity::Unknown,
    }
}

#[cfg(test)]
mod tests;
