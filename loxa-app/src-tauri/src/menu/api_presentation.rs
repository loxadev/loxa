use loxa::api_runtime::ApiRuntimeActivity;

use super::api_runtime::{
    ApiRuntimeController, ApiRuntimeKind, ApiRuntimeNotice, ApiRuntimePhase, ApiRuntimeView,
};
use super::presentation::{ObservedRuntime, ObservedRuntimeOwner};

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn shell_quote_path(value: &std::path::Path) -> String {
    shell_quote(&value.to_string_lossy())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiPresentation {
    phase: ApiPresentationPhase,
    status_label: String,
    curl_command: Option<String>,
    chat_command: Option<String>,
    active_model_id: Option<String>,
    shared_service: bool,
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
    shared_service: bool,
}

impl ApiPrimaryAction {
    pub(crate) fn kind(self) -> ApiPrimaryActionKind {
        self.kind
    }

    pub(crate) fn title(self) -> &'static str {
        match (self.kind, self.shared_service) {
            (ApiPrimaryActionKind::Start, false) => "Start API",
            (ApiPrimaryActionKind::Stop, false) => "Stop API",
            (ApiPrimaryActionKind::Start, true) => "Load",
            (ApiPrimaryActionKind::Stop, true) => "Unload",
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
        let mut presentation = Self::from_runtime_view(controller.view());
        presentation.attach_service_commands(controller);
        presentation
    }

    fn from_runtime_view(view: ApiRuntimeView<'_>) -> Self {
        match view.kind() {
            ApiRuntimeKind::Legacy => {
                Self::from_state(view.phase(), view.notice(), view.active_model_id())
            }
            ApiRuntimeKind::Service => Self::from_service_state(
                view.phase(),
                view.notice(),
                view.active_model_id(),
                view.initialized(),
            ),
        }
    }

    pub(crate) fn from_controller_with_observed_runtime(
        controller: &ApiRuntimeController,
        observed_runtime: Option<&ObservedRuntime>,
    ) -> Self {
        let view = controller.view();
        if view.kind() == ApiRuntimeKind::Service {
            let mut presentation = Self::from_runtime_view(view);
            presentation.attach_service_commands(controller);
            return presentation;
        }
        Self::from_state_with_observed_runtime(
            view.phase(),
            view.notice(),
            view.active_model_id(),
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
            chat_command: None,
            active_model_id: active_model_id.map(str::to_owned),
            shared_service: false,
        }
    }

    fn from_service_state(
        phase: &ApiRuntimePhase,
        notice: Option<ApiRuntimeNotice>,
        active_model_id: Option<&str>,
        initialized: bool,
    ) -> Self {
        let (presentation_phase, status_label) = if !initialized {
            (
                ApiPresentationPhase::Unavailable,
                "Checking background service…".into(),
            )
        } else {
            match phase {
                ApiRuntimePhase::Idle => (
                    ApiPresentationPhase::Idle,
                    notice
                        .map(|notice| match notice {
                            ApiRuntimeNotice::ServiceAbsent => "Background service stopped",
                            ApiRuntimeNotice::Conflict => {
                                "Another background service model operation is active"
                            }
                            ApiRuntimeNotice::ModelUnavailable => {
                                "The selected installed model is unavailable"
                            }
                            ApiRuntimeNotice::StartFailed => {
                                "Could not load the model in the background service"
                            }
                            ApiRuntimeNotice::UnexpectedStop => {
                                "The background service unloaded the model"
                            }
                            ApiRuntimeNotice::CleanupFailed => {
                                "Could not unload the model. Try Unload again."
                            }
                            ApiRuntimeNotice::ControllerFailed => {
                                "Background service unavailable · Quit and reopen Loxa"
                            }
                        })
                        .unwrap_or("Background service idle")
                        .into(),
                ),
                ApiRuntimePhase::Starting { .. } => (
                    ApiPresentationPhase::Starting,
                    "Loading model in background service…".into(),
                ),
                ApiRuntimePhase::Ready { .. } => (
                    ApiPresentationPhase::Ready,
                    "Model loaded in background service".into(),
                ),
                ApiRuntimePhase::Stopping => {
                    (ApiPresentationPhase::Stopping, "Unloading model…".into())
                }
                ApiRuntimePhase::CleanupFailed => (
                    ApiPresentationPhase::CleanupFailed,
                    "Unload failed · Try Unload again".into(),
                ),
                ApiRuntimePhase::ControllerFailed => (
                    ApiPresentationPhase::Unavailable,
                    "Background service unavailable · Quit and reopen Loxa".into(),
                ),
            }
        };
        Self {
            phase: presentation_phase,
            status_label,
            curl_command: None,
            chat_command: None,
            active_model_id: active_model_id.map(str::to_owned),
            shared_service: true,
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
            chat_command: None,
            active_model_id: Some(runtime.model_id().into()),
            shared_service: false,
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

    #[cfg(test)]
    pub(crate) fn can_copy_chat(&self) -> bool {
        self.chat_command.is_some() || !self.shared_service
    }

    pub(crate) fn can_copy_chat_for(&self, model_id: &str) -> bool {
        !self.shared_service
            || (self.chat_command.is_some() && self.active_model_id() == Some(model_id))
    }

    pub(crate) fn chat_command(&self) -> Option<&str> {
        self.chat_command.as_deref()
    }

    fn attach_service_commands(&mut self, controller: &ApiRuntimeController) {
        if !self.shared_service || self.phase != ApiPresentationPhase::Ready {
            return;
        }
        let Some((executable, root)) = controller.service_command_context() else {
            return;
        };
        let ApiRuntimePhase::Ready { endpoint, .. } = controller.view().phase() else {
            return;
        };
        let Some(target) = endpoint.service_target() else {
            return;
        };
        let prefix = format!(
            "{} service-dev --data-root {}",
            shell_quote_path(executable),
            shell_quote_path(root)
        );
        let target = format!(
            "--boot-epoch {} --task-id {} --generation {}",
            shell_quote(&target.boot_epoch),
            shell_quote(&target.task_id),
            shell_quote(&target.generation)
        );
        self.curl_command = Some(format!(
            "{prefix} api {} models {target}",
            shell_quote(&endpoint.model_id)
        ));
        self.chat_command = Some(format!(
            "{prefix} chat {} {target}",
            shell_quote(&endpoint.model_id)
        ));
    }

    pub(crate) fn primary_action(&self, selected_model_id: &str) -> ApiPrimaryAction {
        if self.phase == ApiPresentationPhase::Unavailable {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some("Quit and reopen Loxa."),
                shared_service: self.shared_service,
            };
        }
        if self.phase == ApiPresentationPhase::CliRuntime {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some("Stop the CLI runtime first"),
                shared_service: self.shared_service,
            };
        }
        if self
            .active_model_id()
            .is_some_and(|active| active != selected_model_id)
        {
            return ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: Some(if self.shared_service {
                    "Unload the current model first"
                } else {
                    "Stop the current API first"
                }),
                shared_service: self.shared_service,
            };
        }
        match (self.phase, self.active_model_id()) {
            (ApiPresentationPhase::CleanupFailed, None) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Stop,
                enabled: true,
                disabled_reason: None,
                shared_service: self.shared_service,
            },
            (
                ApiPresentationPhase::Starting
                | ApiPresentationPhase::Ready
                | ApiPresentationPhase::CleanupFailed,
                Some(_),
            ) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Stop,
                enabled: true,
                disabled_reason: None,
                shared_service: self.shared_service,
            },
            (ApiPresentationPhase::Stopping, _) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Stop,
                enabled: false,
                disabled_reason: None,
                shared_service: self.shared_service,
            },
            (ApiPresentationPhase::Idle, None) => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: true,
                disabled_reason: None,
                shared_service: self.shared_service,
            },
            _ => ApiPrimaryAction {
                kind: ApiPrimaryActionKind::Start,
                enabled: false,
                disabled_reason: None,
                shared_service: self.shared_service,
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
