use loxa::api_runtime::ApiRuntimeActivity;

use super::api_runtime::{ApiRuntimeController, ApiRuntimeNotice, ApiRuntimePhase};
use super::presentation::{ObservedRuntime, ObservedRuntimeOwner};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiPresentation {
    phase: ApiPresentationPhase,
    status_label: String,
    curl_command: Option<String>,
    active_model_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApiPresentationPhase {
    Idle,
    CliRuntime,
    Starting,
    Ready,
    Stopping,
    CleanupFailed,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiPrimaryActionKind {
    Start,
    Stop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ApiPrimaryAction {
    kind: ApiPrimaryActionKind,
    enabled: bool,
    disabled_reason: Option<&'static str>,
}

impl ApiPrimaryAction {
    pub(crate) fn kind(self) -> ApiPrimaryActionKind {
        self.kind
    }

    pub(crate) fn title(self) -> &'static str {
        match self.kind {
            ApiPrimaryActionKind::Start => "Start API",
            ApiPrimaryActionKind::Stop => "Stop API",
        }
    }

    pub(crate) fn is_enabled(self) -> bool {
        self.enabled
    }

    pub(crate) fn disabled_reason(self) -> Option<&'static str> {
        self.disabled_reason
    }
}

impl ApiPresentation {
    pub(crate) fn from_controller(controller: &ApiRuntimeController) -> Self {
        Self::from_state(
            controller.phase(),
            controller.notice(),
            controller.active_model_id(),
        )
    }

    pub(crate) fn from_controller_with_observed_runtime(
        controller: &ApiRuntimeController,
        observed_runtime: Option<&ObservedRuntime>,
    ) -> Self {
        Self::from_state_with_observed_runtime(
            controller.phase(),
            controller.notice(),
            controller.active_model_id(),
            observed_runtime,
        )
    }

    pub(super) fn from_state(
        phase: &ApiRuntimePhase,
        notice: Option<ApiRuntimeNotice>,
        active_model_id: Option<&str>,
    ) -> Self {
        let (presentation_phase, status_label, curl_command) = match phase {
            ApiRuntimePhase::Idle => (
                ApiPresentationPhase::Idle,
                notice
                    .map(|notice| notice.message().to_owned())
                    .unwrap_or_else(|| "API idle".into()),
                None,
            ),
            ApiRuntimePhase::Starting { .. } => {
                (ApiPresentationPhase::Starting, "Starting API…".into(), None)
            }
            ApiRuntimePhase::Ready {
                endpoint, activity, ..
            } => {
                let state = match activity {
                    ApiRuntimeActivity::Loaded => "Model loaded",
                    ApiRuntimeActivity::Sleeping => "Model sleeping",
                    ApiRuntimeActivity::Unknown => "API ready",
                };
                (
                    ApiPresentationPhase::Ready,
                    format!("{state} · 127.0.0.1:{}", endpoint.port()),
                    Some(format!(
                        "curl http://127.0.0.1:{}/v1/models",
                        endpoint.port()
                    )),
                )
            }
            ApiRuntimePhase::Stopping => {
                (ApiPresentationPhase::Stopping, "Stopping API…".into(), None)
            }
            ApiRuntimePhase::CleanupFailed => (
                ApiPresentationPhase::CleanupFailed,
                "Stop failed · Try Stop API again".into(),
                None,
            ),
            ApiRuntimePhase::ControllerFailed => (
                ApiPresentationPhase::Unavailable,
                "API unavailable · Quit and reopen Loxa".into(),
                None,
            ),
        };
        Self {
            phase: presentation_phase,
            status_label,
            curl_command,
            active_model_id: active_model_id.map(str::to_owned),
        }
    }

    pub(super) fn from_state_with_observed_runtime(
        phase: &ApiRuntimePhase,
        notice: Option<ApiRuntimeNotice>,
        active_model_id: Option<&str>,
        observed_runtime: Option<&ObservedRuntime>,
    ) -> Self {
        let presentation = Self::from_state(phase, notice, active_model_id);
        let Some(runtime) = observed_runtime.filter(|runtime| {
            presentation.phase == ApiPresentationPhase::Idle
                && runtime.owner() == ObservedRuntimeOwner::Foreground
        }) else {
            return presentation;
        };
        let port = runtime.port();

        Self {
            phase: ApiPresentationPhase::CliRuntime,
            status_label: format!("CLI runtime · 127.0.0.1:{port}"),
            curl_command: Some(format!("curl http://127.0.0.1:{port}/v1/models")),
            active_model_id: Some(runtime.model_id().into()),
        }
    }

    pub(crate) fn status_label(&self) -> &str {
        &self.status_label
    }

    pub(crate) fn curl_command(&self) -> Option<&str> {
        self.curl_command.as_deref()
    }

    pub(crate) fn active_model_id(&self) -> Option<&str> {
        self.active_model_id.as_deref()
    }

    pub(crate) fn primary_action(&self, selected_model_id: &str) -> ApiPrimaryAction {
        if self.phase == ApiPresentationPhase::Unavailable {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some("Quit and reopen Loxa."),
            };
        }
        if self.phase == ApiPresentationPhase::CliRuntime {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some("Stop the CLI runtime first"),
            };
        }
        if self
            .active_model_id()
            .is_some_and(|active| active != selected_model_id)
        {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some("Stop the current API first"),
            };
        }
        match (self.phase, self.active_model_id()) {
            (
                ApiPresentationPhase::Starting
                | ApiPresentationPhase::Ready
                | ApiPresentationPhase::CleanupFailed,
                Some(_),
            ) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Stop,
                enabled: true,
                disabled_reason: None,
            },
            (ApiPresentationPhase::Stopping, Some(_)) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Stop,
                enabled: false,
                disabled_reason: None,
            },
            (ApiPresentationPhase::Idle, None) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: true,
                disabled_reason: None,
            },
            _ => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: None,
            },
        }
    }

    pub(crate) fn can_update_retained_from(&self, previous: &Self) -> bool {
        self == previous
            || (self.phase == ApiPresentationPhase::Ready
                && previous.phase == ApiPresentationPhase::Ready
                && self.curl_command == previous.curl_command
                && self.active_model_id == previous.active_model_id)
    }

    #[cfg(test)]
    pub(crate) fn idle() -> Self {
        Self::from_state(&ApiRuntimePhase::Idle, None, None)
    }

    #[cfg(test)]
    pub(crate) fn ready(model_id: &str, port: u16, activity: ApiRuntimeActivity) -> Self {
        Self::from_state(
            &ApiRuntimePhase::Ready {
                generation: 1,
                endpoint: super::api_runtime::ApiEndpoint::new(model_id.into(), port),
                activity,
            },
            None,
            Some(model_id),
        )
    }
}

#[cfg(test)]
#[path = "api_presentation_tests.rs"]
mod tests;
