use loxa::api_runtime::ApiRuntimeActivity;

use super::ApiPresentation;

#[test]
fn service_command_shell_quotes_apostrophes() {
    assert_eq!(
        super::shell_quote("/tmp/Mahad's Loxa"),
        "'/tmp/Mahad'\"'\"'s Loxa'"
    );
}
use crate::menu::api_runtime::{ApiEndpoint, ApiRuntimeNotice, ApiRuntimePhase};
use crate::menu::presentation::{ObservedRuntime, ObservedRuntimeOwner};

#[test]
fn every_api_phase_has_one_compact_status_line_and_ready_only_curl() {
    let endpoint = ApiEndpoint::new("demo".into(), 43123);
    let cases = [
        (ApiRuntimePhase::Idle, None, "API idle", None),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::Conflict),
            "Another Loxa model operation is active",
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::ModelUnavailable),
            "The selected installed model is unavailable",
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::StartFailed),
            "Could not start the API",
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::UnexpectedStop),
            "The API stopped unexpectedly",
            None,
        ),
        (
            ApiRuntimePhase::Starting {
                generation: 7,
                model_id: "demo".into(),
            },
            None,
            "Starting API…",
            None,
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint: endpoint.clone(),
                activity: ApiRuntimeActivity::Loaded,
            },
            None,
            "Model loaded · 127.0.0.1:43123",
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint: endpoint.clone(),
                activity: ApiRuntimeActivity::Sleeping,
            },
            None,
            "Model sleeping · 127.0.0.1:43123",
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint,
                activity: ApiRuntimeActivity::Unknown,
            },
            None,
            "API ready · 127.0.0.1:43123",
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (ApiRuntimePhase::Stopping, None, "Stopping API…", None),
        (
            ApiRuntimePhase::CleanupFailed,
            Some(ApiRuntimeNotice::CleanupFailed),
            "Stop failed · Try Stop API again",
            None,
        ),
        (
            ApiRuntimePhase::ControllerFailed,
            Some(ApiRuntimeNotice::ControllerFailed),
            "API unavailable · Quit and reopen Loxa",
            None,
        ),
    ];

    for (phase, notice, status, curl) in cases {
        let presentation = ApiPresentation::from_state(&phase, notice, Some("demo"));
        assert_eq!(presentation.status_label(), status);
        assert_eq!(presentation.curl_command(), curl);
        assert_eq!(presentation.active_model_id(), Some("demo"));
    }
}

#[test]
fn selected_model_primary_action_is_phase_and_identity_aware() {
    let endpoint = ApiEndpoint::new("alpha".into(), 43123);
    let rows = [
        (
            ApiRuntimePhase::CleanupFailed,
            None,
            "alpha",
            "Stop API",
            true,
            None,
        ),
        (
            ApiRuntimePhase::Stopping,
            None,
            "alpha",
            "Stop API",
            false,
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            None,
            "alpha",
            "Start API",
            true,
            None,
        ),
        (
            ApiRuntimePhase::Starting {
                generation: 1,
                model_id: "alpha".into(),
            },
            Some("alpha"),
            "alpha",
            "Stop API",
            true,
            None,
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 1,
                endpoint: endpoint.clone(),
                activity: ApiRuntimeActivity::Unknown,
            },
            Some("alpha"),
            "alpha",
            "Stop API",
            true,
            None,
        ),
        (
            ApiRuntimePhase::Stopping,
            Some("alpha"),
            "alpha",
            "Stop API",
            false,
            None,
        ),
        (
            ApiRuntimePhase::CleanupFailed,
            Some("alpha"),
            "alpha",
            "Stop API",
            true,
            None,
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 1,
                endpoint,
                activity: ApiRuntimeActivity::Loaded,
            },
            Some("alpha"),
            "beta",
            "Start API",
            false,
            Some("Stop the current API first"),
        ),
        (
            ApiRuntimePhase::ControllerFailed,
            None,
            "alpha",
            "Start API",
            false,
            Some("Quit and reopen Loxa."),
        ),
    ];

    for (phase, active, selected, title, enabled, disabled_reason) in rows {
        let presentation = ApiPresentation::from_state(&phase, None, active);
        let action = presentation.primary_action(selected);
        assert_eq!(action.title(), title, "phase: {phase:?}");
        assert_eq!(action.is_enabled(), enabled, "phase: {phase:?}");
        assert_eq!(
            action.disabled_reason(),
            disabled_reason,
            "phase: {phase:?}"
        );
    }
}

#[test]
fn only_ready_activity_changes_are_retained_in_place() {
    let endpoint = ApiEndpoint::new("alpha".into(), 43123);
    let sleeping = ApiPresentation::from_state(
        &ApiRuntimePhase::Ready {
            generation: 1,
            endpoint: endpoint.clone(),
            activity: ApiRuntimeActivity::Sleeping,
        },
        None,
        Some("alpha"),
    );
    let loaded = ApiPresentation::from_state(
        &ApiRuntimePhase::Ready {
            generation: 1,
            endpoint,
            activity: ApiRuntimeActivity::Loaded,
        },
        None,
        Some("alpha"),
    );
    let stopping = ApiPresentation::from_state(&ApiRuntimePhase::Stopping, None, Some("alpha"));

    assert!(loaded.can_update_retained_from(&sleeping));
    assert!(!stopping.can_update_retained_from(&loaded));
}

#[test]
fn shared_service_presentation_uses_load_unload_and_never_exposes_loopback() {
    let checking = ApiPresentation::from_service_state(&ApiRuntimePhase::Idle, None, None, false);
    assert_eq!(checking.status_label(), "Checking background service…");
    assert_eq!(checking.curl_command(), None);
    assert!(!checking.can_copy_chat());
    let action = checking.primary_action("alpha");
    assert_eq!(action.title(), "Load");
    assert!(!action.is_enabled());

    let idle = ApiPresentation::from_service_state(&ApiRuntimePhase::Idle, None, None, true);
    assert_eq!(idle.status_label(), "Background service idle");
    let action = idle.primary_action("alpha");
    assert_eq!(action.title(), "Load");
    assert!(action.is_enabled());

    let absent = ApiPresentation::from_service_state(
        &ApiRuntimePhase::Idle,
        Some(ApiRuntimeNotice::ServiceAbsent),
        None,
        true,
    );
    assert_eq!(absent.status_label(), "Background service stopped");
    assert!(absent.primary_action("alpha").is_enabled());

    let ready = ApiPresentation::from_service_state(
        &ApiRuntimePhase::Ready {
            generation: 7,
            endpoint: ApiEndpoint::new("alpha".into(), 0),
            activity: ApiRuntimeActivity::Unknown,
        },
        None,
        Some("alpha"),
        true,
    );
    assert_eq!(ready.status_label(), "Model loaded in background service");
    assert_eq!(ready.curl_command(), None);
    assert!(!ready.can_copy_chat_for("alpha"));
    assert!(!ready.status_label().contains("127.0.0.1"));
    assert_eq!(ready.primary_action("alpha").title(), "Unload");
    let other = ready.primary_action("beta");
    assert_eq!(other.title(), "Load");
    assert!(!other.is_enabled());
    assert_eq!(
        other.disabled_reason(),
        Some("Unload the current model first")
    );
}

#[test]
fn observed_cli_runtime_is_read_only_and_never_overwrites_menu_owned_phases() {
    let foreground =
        ObservedRuntime::new(ObservedRuntimeOwner::Foreground, "alpha".into(), 43123).unwrap();
    let cli = ApiPresentation::from_state_with_observed_runtime(
        &ApiRuntimePhase::Idle,
        None,
        None,
        Some(&foreground),
    );
    assert_eq!(cli.status_label(), "CLI runtime · 127.0.0.1:43123");
    assert_eq!(
        cli.curl_command(),
        Some("curl http://127.0.0.1:43123/v1/models")
    );
    assert_eq!(cli.active_model_id(), Some("alpha"));
    let action = cli.primary_action("alpha");
    assert_eq!(action.title(), "Start API");
    assert!(!action.is_enabled());
    assert_eq!(action.disabled_reason(), Some("Stop the CLI runtime first"));

    let starting = ApiPresentation::from_state_with_observed_runtime(
        &ApiRuntimePhase::Starting {
            generation: 7,
            model_id: "alpha".into(),
        },
        None,
        Some("alpha"),
        Some(&foreground),
    );
    assert_eq!(starting.status_label(), "Starting API…");
    assert_eq!(starting.curl_command(), None);

    let ready = ApiPresentation::from_state_with_observed_runtime(
        &ApiRuntimePhase::Ready {
            generation: 7,
            endpoint: ApiEndpoint::new("alpha".into(), 43124),
            activity: ApiRuntimeActivity::Loaded,
        },
        None,
        Some("alpha"),
        Some(&foreground),
    );
    assert_eq!(ready.status_label(), "Model loaded · 127.0.0.1:43124");
    assert_eq!(
        ready.curl_command(),
        Some("curl http://127.0.0.1:43124/v1/models")
    );

    for owner in [
        ObservedRuntimeOwner::PersistentApp,
        ObservedRuntimeOwner::Legacy,
    ] {
        let other = ObservedRuntime::new(owner, "alpha".into(), 43123).unwrap();
        let idle = ApiPresentation::from_state_with_observed_runtime(
            &ApiRuntimePhase::Idle,
            None,
            None,
            Some(&other),
        );
        assert_eq!(idle.status_label(), "API idle");
        assert_eq!(idle.curl_command(), None);
    }
}
