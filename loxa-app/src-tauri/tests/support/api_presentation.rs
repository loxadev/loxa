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
    status_label: String,
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
            status_label: "API idle".into(),
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
        let state = match activity {
            loxa::api_runtime::ApiRuntimeActivity::Loaded => "Model loaded",
            loxa::api_runtime::ApiRuntimeActivity::Sleeping => "Model sleeping",
            loxa::api_runtime::ApiRuntimeActivity::Unknown => "API ready",
        };
        Self {
            status_label: format!("{state} · 127.0.0.1:{port}"),
            curl_command: Some(format!("curl http://127.0.0.1:{port}/v1/models")),
            active_model_id: Some(model_id.into()),
            action_kind: ApiPrimaryActionKind::Stop,
            action_enabled: true,
        }
    }

    pub(crate) fn stopping(model_id: &str) -> Self {
        Self {
            status_label: "Stopping API…".into(),
            curl_command: None,
            active_model_id: Some(model_id.into()),
            action_kind: ApiPrimaryActionKind::Stop,
            action_enabled: false,
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

    pub(crate) fn active_section_title(&self) -> Option<&'static str> {
        self.active_model_id()?;
        Some(if self.action_enabled { "Running" } else { "Stopping" })
    }

    pub(crate) fn can_copy_chat(&self) -> bool {
        true
    }

    pub(crate) fn can_copy_chat_for(&self, _model_id: &str) -> bool { true }

    pub(crate) fn chat_command(&self) -> Option<&str> { None }

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
