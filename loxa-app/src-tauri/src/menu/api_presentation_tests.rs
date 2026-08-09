use loxa::api_runtime::ApiRuntimeActivity;

use super::ApiPresentation;
use crate::menu::api_runtime::{ApiEndpoint, ApiRuntimeNotice, ApiRuntimePhase};

#[test]
fn every_api_phase_has_exact_header_copy_and_ready_only_endpoint() {
    let endpoint = ApiEndpoint::new("demo".into(), 43123);
    let cases = [
        (ApiRuntimePhase::Idle, None, "API: Idle", None, None),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::Conflict),
            "API: Idle",
            Some("Another Loxa model operation is active"),
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::ModelUnavailable),
            "API: Idle",
            Some("The selected installed model is unavailable"),
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::StartFailed),
            "API: Idle",
            Some("Could not start the API"),
            None,
        ),
        (
            ApiRuntimePhase::Idle,
            Some(ApiRuntimeNotice::UnexpectedStop),
            "API: Idle",
            Some("The API stopped unexpectedly"),
            None,
        ),
        (
            ApiRuntimePhase::Starting {
                generation: 7,
                model_id: "demo".into(),
            },
            None,
            "API: Starting",
            None,
            None,
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint: endpoint.clone(),
                activity: ApiRuntimeActivity::Loaded,
            },
            None,
            "API: Ready · Model loaded",
            Some("API · 127.0.0.1:43123"),
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint: endpoint.clone(),
                activity: ApiRuntimeActivity::Sleeping,
            },
            None,
            "API: Ready · Model sleeping",
            Some("API · 127.0.0.1:43123"),
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (
            ApiRuntimePhase::Ready {
                generation: 7,
                endpoint,
                activity: ApiRuntimeActivity::Unknown,
            },
            None,
            "API: Ready",
            Some("API · 127.0.0.1:43123"),
            Some("curl http://127.0.0.1:43123/v1/models"),
        ),
        (ApiRuntimePhase::Stopping, None, "API: Stopping", None, None),
        (
            ApiRuntimePhase::CleanupFailed,
            Some(ApiRuntimeNotice::CleanupFailed),
            "API: Stop failed",
            Some("Could not stop the API. Try Stop API again."),
            None,
        ),
        (
            ApiRuntimePhase::ControllerFailed,
            Some(ApiRuntimeNotice::ControllerFailed),
            "API: Unavailable",
            Some("Quit and reopen Loxa."),
            None,
        ),
    ];

    for (phase, notice, phase_label, detail, curl) in cases {
        let presentation = ApiPresentation::from_state(&phase, notice, Some("demo"));
        assert_eq!(presentation.phase_label(), phase_label);
        assert_eq!(presentation.detail_label(), detail);
        assert_eq!(presentation.curl_command(), curl);
        assert_eq!(presentation.active_model_id(), Some("demo"));
    }
}

#[test]
fn selected_model_primary_action_is_phase_and_identity_aware() {
    let endpoint = ApiEndpoint::new("alpha".into(), 43123);
    let rows = [
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
