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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiPresentation {
    phase_label: &'static str,
    detail_label: Option<String>,
    curl_command: Option<String>,
    active_model_id: Option<String>,
    action_kind: ApiPrimaryActionKind,
    action_enabled: bool,
}

impl ApiPresentation {
    pub(crate) fn from_controller(
        _controller: &crate::menu::api_runtime::ApiRuntimeController,
    ) -> Self {
        Self::idle()
    }

    pub(crate) fn from_controller_with_observed_runtime(
        _controller: &crate::menu::api_runtime::ApiRuntimeController,
        _observed_runtime: Option<&crate::menu::presentation::ObservedRuntime>,
    ) -> Self {
        Self::idle()
    }

    pub(crate) fn idle() -> Self {
        Self {
            phase_label: "API: Idle",
            detail_label: None,
            curl_command: None,
            active_model_id: None,
            action_kind: ApiPrimaryActionKind::Start,
            action_enabled: true,
        }
    }

    pub(crate) fn ready(
        model_id: &str,
        port: u16,
        activity: loxa::api_runtime::ApiRuntimeActivity,
    ) -> Self {
        let phase_label = match activity {
            loxa::api_runtime::ApiRuntimeActivity::Loaded => "API: Ready · Model loaded",
            loxa::api_runtime::ApiRuntimeActivity::Sleeping => "API: Ready · Model sleeping",
            loxa::api_runtime::ApiRuntimeActivity::Unknown => "API: Ready",
        };
        Self {
            phase_label,
            detail_label: Some(format!("API · 127.0.0.1:{port}")),
            curl_command: Some(format!("curl http://127.0.0.1:{port}/v1/models")),
            active_model_id: Some(model_id.into()),
            action_kind: ApiPrimaryActionKind::Stop,
            action_enabled: true,
        }
    }

    pub(crate) fn stopping(model_id: &str) -> Self {
        Self {
            phase_label: "API: Stopping",
            detail_label: None,
            curl_command: None,
            active_model_id: Some(model_id.into()),
            action_kind: ApiPrimaryActionKind::Stop,
            action_enabled: false,
        }
    }

    pub(crate) fn phase_label(&self) -> &'static str {
        self.phase_label
    }

    pub(crate) fn detail_label(&self) -> Option<&str> {
        self.detail_label.as_deref()
    }

    pub(crate) fn curl_command(&self) -> Option<&str> {
        self.curl_command.as_deref()
    }

    pub(crate) fn active_model_id(&self) -> Option<&str> {
        self.active_model_id.as_deref()
    }

    pub(crate) fn primary_action(&self, selected_model_id: &str) -> ApiPrimaryAction {
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
        ApiPrimaryAction {
            kind: self.action_kind,
            enabled: self.action_enabled,
            disabled_reason: None,
        }
    }

    pub(crate) fn can_update_retained_from(&self, previous: &Self) -> bool {
        self == previous
            || (self.curl_command.is_some()
                && self.curl_command == previous.curl_command
                && self.active_model_id == previous.active_model_id)
    }
}
