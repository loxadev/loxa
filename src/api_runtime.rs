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

    fn is_cancelled(&self) -> bool {
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

pub struct ApiRuntimeHost {
    paths: AppPaths,
    runtime: Option<Box<PersistentServer>>,
    #[cfg(test)]
    fail_next_stop: bool,
}

impl ApiRuntimeHost {
    pub fn new(paths: AppPaths) -> Self {
        Self {
            paths,
            runtime: None,
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

    pub fn stop(&mut self) -> Result<ApiStopOutcome, ApiStopError> {
        let Some(mut runtime) = self.runtime.take() else {
            return Ok(ApiStopOutcome::AlreadyStopped);
        };
        if self.terminate_runtime(&mut runtime).is_err() {
            self.runtime = Some(runtime);
            return Err(ApiStopError);
        }
        Ok(ApiStopOutcome::Stopped)
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
        let runnable = crate::runnable::resolve_managed_runnable_for_host(manifest, &self.paths)
            .map_err(|error| match error {
                crate::runnable::ManagedRunnableError::Conflict => ApiStartError::Conflict,
                crate::runnable::ManagedRunnableError::ModelUnavailable(_diagnostic) => {
                    ApiStartError::ModelUnavailable
                }
                crate::runnable::ManagedRunnableError::StartupFailed(_diagnostic) => {
                    ApiStartError::StartupFailed
                }
            })?;
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
        self.activity_with(probe_activity)
    }

    fn activity_with(
        &mut self,
        probe: impl FnOnce(u16) -> ApiRuntimeActivity,
    ) -> ApiRuntimeActivity {
        let Some(runtime) = self.runtime.as_mut() else {
            return ApiRuntimeActivity::Unknown;
        };
        if !matches!(runtime.poll(), Ok(None)) {
            return ApiRuntimeActivity::Unknown;
        }
        let activity = probe(runtime.port());
        if !matches!(runtime.poll(), Ok(None)) {
            return ApiRuntimeActivity::Unknown;
        }
        activity
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
