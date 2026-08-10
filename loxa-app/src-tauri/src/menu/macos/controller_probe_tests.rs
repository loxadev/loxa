use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use loxa::api_runtime::{ApiRuntimeActivity, ApiStartCancellation};

use super::controller::{
    drain_runtime_controller, project_api_presentation, request_runtime_probe,
};
use crate::menu::api_presentation::ApiPresentation;
use crate::menu::api_runtime::{
    idle_controller_for_exit_test, run_runtime_worker, ApiEndpoint, ApiRuntimeController,
    ApiRuntimePhase, RuntimeHost, RuntimeHostStart,
};
use crate::menu::presentation::{Fixture, ObservedRuntimeOwner};

struct ProbeHost {
    endpoint: Option<ApiEndpoint>,
    activities: Arc<AtomicUsize>,
}

impl RuntimeHost for ProbeHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        self.endpoint.clone()
    }

    fn start(&mut self, model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        let endpoint = ApiEndpoint::new(model_id.into(), 43123);
        self.endpoint = Some(endpoint.clone());
        RuntimeHostStart::Ready(endpoint)
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.endpoint = None;
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        self.activities.fetch_add(1, Ordering::AcqRel);
        ApiRuntimeActivity::Loaded
    }
}

#[test]
fn timer_drain_never_probes_and_popover_open_coalesces_one_probe() {
    let activities = Arc::new(AtomicUsize::new(0));
    let host = ProbeHost {
        endpoint: None,
        activities: Arc::clone(&activities),
    };
    let mut controller = ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-probe-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    });
    assert!(controller.request_start("demo".into()));
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let changed = controller.drain();
        if activities.load(Ordering::Acquire) == 1
            && changed
            && matches!(controller.phase(), ApiRuntimePhase::Ready { .. })
        {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    let mut api = ApiPresentation::from_controller(&controller);

    for _ in 0..8 {
        let _ = drain_runtime_controller(Some(&mut controller), &mut api);
    }
    assert_eq!(activities.load(Ordering::Acquire), 1);
    assert!(request_runtime_probe(Some(&mut controller)));
    assert!(!request_runtime_probe(Some(&mut controller)));

    let deadline = Instant::now() + Duration::from_secs(2);
    while activities.load(Ordering::Acquire) != 2 {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    while !drain_runtime_controller(Some(&mut controller), &mut api) {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(activities.load(Ordering::Acquire), 2);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn generic_cli_runtime_projects_as_read_only_when_the_menu_controller_is_idle() {
    let mut controller = idle_controller_for_exit_test();
    let snapshot = Fixture::Running
        .snapshot()
        .with_observed_runtime(ObservedRuntimeOwner::Foreground, "demo".into(), 43123)
        .unwrap();

    let api = project_api_presentation(&controller, &snapshot);
    assert_eq!(api.status_label(), "CLI runtime · 127.0.0.1:43123");
    assert_eq!(
        api.curl_command(),
        Some("curl http://127.0.0.1:43123/v1/models")
    );
    let start = api.primary_action("demo");
    assert!(!start.is_enabled());
    assert_eq!(start.disabled_reason(), Some("Stop the CLI runtime first"));

    let stale_persistent = Fixture::Running
        .snapshot()
        .with_observed_runtime(ObservedRuntimeOwner::PersistentApp, "demo".into(), 43123)
        .unwrap();
    let idle = project_api_presentation(&controller, &stale_persistent);
    assert_eq!(idle.status_label(), "API idle");
    assert_eq!(idle.curl_command(), None);

    controller.shutdown_and_join().unwrap();
}
