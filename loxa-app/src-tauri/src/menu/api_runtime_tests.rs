use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use loxa::api_runtime::{
    ApiRuntimeActivity, ApiRuntimeHost, ApiRuntimeProbe, ApiStartCancellation, ApiStartError,
};
use loxa::paths::AppPaths;

use super::{
    run_runtime_worker, ApiEndpoint, ApiRuntimeController, ApiRuntimeNotice, ApiRuntimePhase,
    RuntimeHost, RuntimeHostStart, RuntimeMessage, RuntimeRequest,
};
use crate::menu::api_presentation::ApiPresentation;

struct LifecycleHost {
    starts: Arc<AtomicUsize>,
    activities: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    activity_started: mpsc::Sender<()>,
    release_activity: mpsc::Receiver<()>,
    endpoint: Option<ApiEndpoint>,
}

impl RuntimeHost for LifecycleHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        self.endpoint.clone()
    }

    fn start(&mut self, model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        self.starts.fetch_add(1, Ordering::AcqRel);
        let endpoint = ApiEndpoint::new(model_id.into(), 43123);
        self.endpoint = Some(endpoint.clone());
        RuntimeHostStart::Ready(endpoint)
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.stops.fetch_add(1, Ordering::AcqRel);
        self.endpoint = None;
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        self.activities.fetch_add(1, Ordering::AcqRel);
        let _ = self.activity_started.send(());
        let _ = self.release_activity.recv_timeout(Duration::from_secs(2));
        ApiRuntimeActivity::Loaded
    }
}

fn drain_until(
    controller: &mut ApiRuntimeController,
    deadline: Instant,
    predicate: impl Fn(&ApiRuntimePhase) -> bool,
) {
    while !predicate(controller.phase()) {
        controller.drain();
        assert!(Instant::now() < deadline, "runtime transition timed out");
        std::thread::yield_now();
    }
}

#[test]
fn lifecycle_publishes_ready_before_one_activity_and_rejects_duplicate_work() {
    let starts = Arc::new(AtomicUsize::new(0));
    let activities = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let (activity_started, activity_receiver) = mpsc::channel();
    let (release_activity, release_receiver) = mpsc::channel();
    let host = LifecycleHost {
        starts: Arc::clone(&starts),
        activities: Arc::clone(&activities),
        stops: Arc::clone(&stops),
        activity_started,
        release_activity: release_receiver,
        endpoint: None,
    };
    let mut controller = ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-runtime-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    });

    assert!(controller.request_start("demo".into()));
    assert_eq!(
        controller.phase(),
        &ApiRuntimePhase::Starting {
            generation: 1,
            model_id: "demo".into(),
        }
    );
    assert!(!controller.request_start("demo".into()));
    assert_eq!(
        activity_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(())
    );

    controller.drain();
    assert_eq!(
        controller.phase(),
        &ApiRuntimePhase::Ready {
            generation: 1,
            endpoint: ApiEndpoint::new("demo".into(), 43123),
            activity: ApiRuntimeActivity::Unknown,
        },
        "Ready must be observable before the bounded activity probe returns"
    );
    assert!(!controller.request_start("other".into()));
    assert_eq!(starts.load(Ordering::Acquire), 1);
    assert_eq!(activities.load(Ordering::Acquire), 1);
    assert_eq!(
        controller.endpoint(),
        Some(&ApiEndpoint::new("demo".into(), 43123))
    );

    release_activity.send(()).unwrap();
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| {
            matches!(
                phase,
                ApiRuntimePhase::Ready {
                    activity: ApiRuntimeActivity::Loaded,
                    ..
                }
            )
        },
    );
    assert!(controller.request_probe());
    assert!(!controller.request_probe());
    assert_eq!(
        activity_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(())
    );
    assert_eq!(activities.load(Ordering::Acquire), 2);
    release_activity.send(()).unwrap();
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| {
            matches!(
                phase,
                ApiRuntimePhase::Ready {
                    activity: ApiRuntimeActivity::Loaded,
                    ..
                }
            )
        },
    );
    assert!(controller.request_stop());
    assert_eq!(controller.endpoint(), None);
    assert!(!controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    assert_eq!(stops.load(Ordering::Acquire), 1);
    assert!(!controller.request_stop());
    assert_eq!(stops.load(Ordering::Acquire), 1);
    controller.shutdown_and_join().unwrap();
}

struct StartFailureHost(RuntimeHostStart);

impl RuntimeHost for StartFailureHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        None
    }

    fn start(&mut self, _model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        self.0.clone()
    }

    fn stop(&mut self) -> Result<(), ()> {
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        panic!("a failed start has no activity probe")
    }
}

#[test]
fn start_failures_without_owned_runtime_roll_back_to_idle_with_closed_notices() {
    for (outcome, expected_notice) in [
        (RuntimeHostStart::Conflict, Some(ApiRuntimeNotice::Conflict)),
        (
            RuntimeHostStart::ModelUnavailable,
            Some(ApiRuntimeNotice::ModelUnavailable),
        ),
        (
            RuntimeHostStart::StartupFailed,
            Some(ApiRuntimeNotice::StartFailed),
        ),
        (RuntimeHostStart::Cancelled, None),
    ] {
        let host = StartFailureHost(outcome);
        let mut controller = ApiRuntimeController::assemble(move |requests, messages| {
            std::thread::Builder::new()
                .name("loxa-menu-api-runtime-start-failure-test".into())
                .spawn(move || run_runtime_worker(Ok(host), requests, messages))
                .map_err(|_| ())
        });
        assert!(controller.request_start("demo".into()));
        drain_until(
            &mut controller,
            Instant::now() + Duration::from_secs(2),
            |phase| matches!(phase, ApiRuntimePhase::Idle),
        );
        assert_eq!(controller.notice(), expected_notice);
        assert_eq!(controller.active_model_id(), None);
        assert_eq!(controller.owned_endpoint(), None);
        controller.shutdown_and_join().unwrap();
    }
}

struct CancelBlockingHost {
    witness: ApiRuntimeHost,
    start_entered: mpsc::Sender<()>,
    release_start: mpsc::Receiver<()>,
    cancellation_seen: mpsc::Sender<bool>,
    stops: Arc<AtomicUsize>,
}

impl RuntimeHost for CancelBlockingHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        None
    }

    fn start(&mut self, _model_id: &str, cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        let _ = self.start_entered.send(());
        let _ = self.release_start.recv_timeout(Duration::from_secs(2));
        let deadline = Instant::now() + Duration::from_secs(1);
        let cancelled = loop {
            if matches!(
                self.witness.start("missing", cancellation),
                Err(ApiStartError::Cancelled)
            ) {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        let _ = self.cancellation_seen.send(cancelled);
        if cancelled {
            RuntimeHostStart::Cancelled
        } else {
            RuntimeHostStart::ModelUnavailable
        }
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.stops.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        panic!("a cancelled start has no activity probe")
    }
}

fn cancel_blocking_controller() -> (
    ApiRuntimeController,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
    mpsc::Receiver<bool>,
    Arc<AtomicUsize>,
) {
    static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);
    let root = PathBuf::from(format!(
        "/tmp/loxa-menu-cancel-{}-{}",
        std::process::id(),
        NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    let paths = AppPaths::from_values(Some(&root), None).unwrap();
    let (start_entered, entered_receiver) = mpsc::channel();
    let (release_start, release_receiver) = mpsc::channel();
    let (cancellation_seen, cancellation_receiver) = mpsc::channel();
    let stops = Arc::new(AtomicUsize::new(0));
    let host = CancelBlockingHost {
        witness: ApiRuntimeHost::new(paths),
        start_entered,
        release_start: release_receiver,
        cancellation_seen,
        stops: Arc::clone(&stops),
    };
    let controller = ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-runtime-cancel-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    });
    (
        controller,
        entered_receiver,
        release_start,
        cancellation_receiver,
        stops,
    )
}

#[test]
fn stop_cancels_a_blocked_start_before_the_queued_stop_and_ignores_late_start_result() {
    let (mut controller, entered, release, cancelled, stops) = cancel_blocking_controller();
    assert!(controller.request_start("demo".into()));
    assert_eq!(entered.recv_timeout(Duration::from_secs(2)), Ok(()));

    assert!(controller.request_stop());
    assert!(matches!(controller.phase(), ApiRuntimePhase::Stopping));
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert_eq!(controller.owned_endpoint(), None);
    release.send(()).unwrap();
    assert_eq!(cancelled.recv_timeout(Duration::from_secs(2)), Ok(true));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );

    assert_eq!(stops.load(Ordering::Acquire), 1);
    assert_eq!(controller.notice(), None);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn shutdown_cancels_a_blocked_start_and_joins_only_after_the_queued_stop() {
    let (mut controller, entered, release, cancelled, stops) = cancel_blocking_controller();
    assert!(controller.request_start("demo".into()));
    assert_eq!(entered.recv_timeout(Duration::from_secs(2)), Ok(()));

    let shutdown = std::thread::spawn(move || {
        let result = controller.shutdown_and_join();
        (result, controller)
    });
    release.send(()).unwrap();
    assert_eq!(cancelled.recv_timeout(Duration::from_secs(2)), Ok(true));
    let (result, controller) = shutdown.join().unwrap();

    assert_eq!(result, Ok(()));
    assert_eq!(stops.load(Ordering::Acquire), 1);
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
}

#[test]
fn prepare_shutdown_marks_a_blocked_start_stopping_without_queueing_a_duplicate_stop() {
    let (mut controller, entered, release, cancelled, stops) = cancel_blocking_controller();
    assert!(controller.request_start("demo".into()));
    assert_eq!(entered.recv_timeout(Duration::from_secs(2)), Ok(()));

    assert!(controller.prepare_shutdown());
    assert!(matches!(controller.phase(), ApiRuntimePhase::Stopping));
    assert_eq!(controller.active_model_id(), Some("demo"));

    let shutdown = std::thread::spawn(move || {
        let result = controller.shutdown_and_join();
        (result, controller)
    });
    release.send(()).unwrap();
    assert_eq!(cancelled.recv_timeout(Duration::from_secs(2)), Ok(true));
    let (result, controller) = shutdown.join().unwrap();

    assert_eq!(result, Ok(()));
    assert_eq!(stops.load(Ordering::Acquire), 1);
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
}

struct RetainedHost {
    endpoint: Option<ApiEndpoint>,
    start_returns_error: bool,
    remaining_stop_failures: Arc<AtomicUsize>,
    stop_calls: Arc<AtomicUsize>,
    activity: ApiRuntimeActivity,
}

impl RuntimeHost for RetainedHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        self.endpoint.clone()
    }

    fn start(&mut self, model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        let endpoint = ApiEndpoint::new(model_id.into(), 43124);
        self.endpoint = Some(endpoint.clone());
        if self.start_returns_error {
            RuntimeHostStart::StartupFailed
        } else {
            RuntimeHostStart::Ready(endpoint)
        }
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.stop_calls.fetch_add(1, Ordering::AcqRel);
        if self
            .remaining_stop_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(());
        }
        self.endpoint = None;
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        self.activity
    }
}

fn retained_controller(
    start_returns_error: bool,
    stop_failures: usize,
) -> (ApiRuntimeController, Arc<AtomicUsize>) {
    let stop_calls = Arc::new(AtomicUsize::new(0));
    let host = RetainedHost {
        endpoint: None,
        start_returns_error,
        remaining_stop_failures: Arc::new(AtomicUsize::new(stop_failures)),
        stop_calls: Arc::clone(&stop_calls),
        activity: ApiRuntimeActivity::Sleeping,
    };
    let controller = ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-runtime-retained-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    });
    (controller, stop_calls)
}

struct ProbeOutcomeHost {
    endpoint: Option<ApiEndpoint>,
    outcome: Option<ApiRuntimeProbe>,
}

impl RuntimeHost for ProbeOutcomeHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        self.endpoint.clone()
    }

    fn start(&mut self, model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        let endpoint = ApiEndpoint::new(model_id.into(), 43125);
        self.endpoint = Some(endpoint.clone());
        RuntimeHostStart::Ready(endpoint)
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.endpoint = None;
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        panic!("the worker must consume the closed probe result")
    }

    fn probe(&mut self) -> ApiRuntimeProbe {
        let outcome = self.outcome.take().expect("one automatic probe");
        if matches!(outcome, ApiRuntimeProbe::Stopped) {
            self.endpoint = None;
        }
        outcome
    }
}

fn probe_outcome_controller(outcome: ApiRuntimeProbe) -> ApiRuntimeController {
    let host = ProbeOutcomeHost {
        endpoint: None,
        outcome: Some(outcome),
    };
    ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-probe-outcome-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    })
}

#[test]
fn unexpected_child_exit_clears_runtime_and_presents_one_static_idle_notice() {
    let mut controller = probe_outcome_controller(ApiRuntimeProbe::Stopped);
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );

    assert_eq!(controller.notice(), Some(ApiRuntimeNotice::UnexpectedStop));
    assert_eq!(controller.active_model_id(), None);
    assert_eq!(controller.owned_endpoint(), None);
    let presentation = ApiPresentation::from_controller(&controller);
    assert_eq!(presentation.phase_label(), "API: Idle");
    assert_eq!(
        presentation.detail_label(),
        Some("The API stopped unexpectedly")
    );
    assert_eq!(presentation.curl_command(), None);
    assert!(presentation.primary_action("demo").is_enabled());
    controller.shutdown_and_join().unwrap();
}

#[test]
fn dead_child_cleanup_failure_retains_endpoint_and_stop_retry() {
    let mut controller = probe_outcome_controller(ApiRuntimeProbe::CleanupFailed);
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::CleanupFailed),
    );

    assert_eq!(
        controller.owned_endpoint(),
        Some(&ApiEndpoint::new("demo".into(), 43125))
    );
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert_eq!(controller.notice(), Some(ApiRuntimeNotice::CleanupFailed));
    assert!(controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    controller.shutdown_and_join().unwrap();
}

#[test]
fn live_child_props_failure_remains_ready_unknown() {
    let mut controller =
        probe_outcome_controller(ApiRuntimeProbe::Activity(ApiRuntimeActivity::Unknown));
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| {
            matches!(
                phase,
                ApiRuntimePhase::Ready {
                    activity: ApiRuntimeActivity::Unknown,
                    ..
                }
            )
        },
    );
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert_eq!(controller.notice(), None);
    assert!(controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    controller.shutdown_and_join().unwrap();
}

struct BlockingStoppedProbeHost {
    endpoint: Option<ApiEndpoint>,
    probe_entered: mpsc::Sender<()>,
    release_probe: mpsc::Receiver<()>,
}

impl RuntimeHost for BlockingStoppedProbeHost {
    fn endpoint(&self) -> Option<ApiEndpoint> {
        self.endpoint.clone()
    }

    fn start(&mut self, model_id: &str, _cancellation: &ApiStartCancellation) -> RuntimeHostStart {
        let endpoint = ApiEndpoint::new(model_id.into(), 43126);
        self.endpoint = Some(endpoint.clone());
        RuntimeHostStart::Ready(endpoint)
    }

    fn stop(&mut self) -> Result<(), ()> {
        self.endpoint = None;
        Ok(())
    }

    fn activity(&mut self) -> ApiRuntimeActivity {
        panic!("the worker must consume the closed probe result")
    }

    fn probe(&mut self) -> ApiRuntimeProbe {
        let _ = self.probe_entered.send(());
        let _ = self.release_probe.recv_timeout(Duration::from_secs(2));
        self.endpoint = None;
        ApiRuntimeProbe::Stopped
    }
}

#[test]
fn stop_queued_behind_a_probe_detecting_exit_still_completes() {
    let (probe_entered, entered) = mpsc::channel();
    let (release, release_probe) = mpsc::channel();
    let host = BlockingStoppedProbeHost {
        endpoint: None,
        probe_entered,
        release_probe,
    };
    let mut controller = ApiRuntimeController::assemble(move |requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-probe-stop-race-test".into())
            .spawn(move || run_runtime_worker(Ok(host), requests, messages))
            .map_err(|_| ())
    });

    assert!(controller.request_start("demo".into()));
    assert_eq!(entered.recv_timeout(Duration::from_secs(2)), Ok(()));
    controller.drain();
    assert!(matches!(controller.phase(), ApiRuntimePhase::Ready { .. }));
    assert!(controller.request_stop());
    release.send(()).unwrap();
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    controller.shutdown_and_join().unwrap();
}

#[test]
fn every_start_error_with_an_owned_endpoint_becomes_cleanup_failed_and_is_retryable() {
    let (mut controller, stops) = retained_controller(true, 0);
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::CleanupFailed),
    );

    assert_eq!(
        controller.owned_endpoint(),
        Some(&ApiEndpoint::new("demo".into(), 43124))
    );
    assert_eq!(controller.endpoint(), None);
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert_eq!(controller.notice(), Some(ApiRuntimeNotice::CleanupFailed));
    assert!(controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    assert_eq!(stops.load(Ordering::Acquire), 1);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn stop_failure_retains_the_exact_endpoint_and_worker_for_retry() {
    let (mut controller, stops) = retained_controller(false, 1);
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Ready { .. }),
    );
    assert!(controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::CleanupFailed),
    );
    assert_eq!(
        controller.owned_endpoint(),
        Some(&ApiEndpoint::new("demo".into(), 43124))
    );
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert!(controller.request_stop());
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    assert_eq!(stops.load(Ordering::Acquire), 2);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn shutdown_failure_keeps_sender_worker_and_endpoint_until_exact_retry_succeeds() {
    let (mut controller, stops) = retained_controller(false, 1);
    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Ready { .. }),
    );

    assert!(controller.shutdown_and_join().is_err());
    assert!(matches!(controller.phase(), ApiRuntimePhase::CleanupFailed));
    assert_eq!(
        controller.owned_endpoint(),
        Some(&ApiEndpoint::new("demo".into(), 43124))
    );
    assert_eq!(stops.load(Ordering::Acquire), 1);

    assert_eq!(controller.shutdown_and_join(), Ok(()));
    assert_eq!(stops.load(Ordering::Acquire), 2);
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
}

#[test]
fn stale_generation_and_phase_messages_cannot_replace_newer_controller_state() {
    let (mut controller, _stops) = retained_controller(false, 0);
    controller.apply_message(RuntimeMessage::Started {
        generation: 77,
        outcome: RuntimeHostStart::Ready(ApiEndpoint::new("stale".into(), 1)),
    });
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));

    assert!(controller.request_start("demo".into()));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Ready { .. }),
    );
    let current = controller.phase().clone();
    controller.apply_message(RuntimeMessage::Activity {
        generation: 0,
        activity: ApiRuntimeActivity::Loaded,
    });
    controller.apply_message(RuntimeMessage::Started {
        generation: 0,
        outcome: RuntimeHostStart::Ready(ApiEndpoint::new("stale".into(), 2)),
    });
    assert_eq!(controller.phase(), &current);

    assert!(controller.request_stop());
    controller.apply_message(RuntimeMessage::Started {
        generation: 1,
        outcome: RuntimeHostStart::Ready(ApiEndpoint::new("late".into(), 3)),
    });
    controller.apply_message(RuntimeMessage::Activity {
        generation: 1,
        activity: ApiRuntimeActivity::Sleeping,
    });
    assert!(matches!(controller.phase(), ApiRuntimePhase::Stopping));
    drain_until(
        &mut controller,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::Idle),
    );
    controller.shutdown_and_join().unwrap();
}

#[test]
fn spawn_and_host_initialization_fail_closed_with_static_controller_notice() {
    let mut spawn_failed = ApiRuntimeController::assemble(|_requests, _messages| Err(()));
    assert!(matches!(
        spawn_failed.phase(),
        ApiRuntimePhase::ControllerFailed
    ));
    assert_eq!(
        spawn_failed.notice().map(ApiRuntimeNotice::message),
        Some("The API controller is unavailable. Quit and reopen Loxa.")
    );
    assert!(!spawn_failed.request_start("demo".into()));
    assert_eq!(spawn_failed.shutdown_and_join(), Ok(()));

    let mut init_failed = ApiRuntimeController::assemble(|requests, messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-runtime-init-failure-test".into())
            .spawn(move || {
                run_runtime_worker::<RetainedHost>(Err(()), requests, messages);
            })
            .map_err(|_| ())
    });
    drain_until(
        &mut init_failed,
        Instant::now() + Duration::from_secs(2),
        |phase| matches!(phase, ApiRuntimePhase::ControllerFailed),
    );
    assert_eq!(
        init_failed.shutdown_and_join(),
        Ok(()),
        "a worker that proved no RuntimeHost existed must answer clean Shutdown"
    );
    assert!(init_failed.worker.is_none());
    assert!(init_failed.request_sender.is_none());
}

#[test]
fn channel_failure_after_start_admission_retains_unknown_worker_authority() {
    let (start_admitted, start_admitted_receiver) = mpsc::channel();
    let mut controller = ApiRuntimeController::assemble(move |requests, _messages| {
        std::thread::Builder::new()
            .name("loxa-menu-api-runtime-channel-failure-test".into())
            .spawn(move || {
                if matches!(requests.recv(), Ok(RuntimeRequest::Start { .. })) {
                    let _ = start_admitted.send(());
                }
            })
            .map_err(|_| ())
    });
    assert!(controller.request_start("demo".into()));
    assert_eq!(
        start_admitted_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(())
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while controller
        .worker
        .as_ref()
        .is_some_and(|worker| !worker.is_finished())
    {
        assert!(Instant::now() < deadline, "test worker did not exit");
        std::thread::yield_now();
    }

    assert_eq!(
        controller.shutdown_and_join(),
        Err(super::ApiRuntimeShutdownError)
    );
    assert!(controller.worker.is_some());
    assert!(controller.request_sender.is_some());
    assert_eq!(controller.active_model_id(), Some("demo"));
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::ControllerFailed
    ));
}

#[test]
fn production_controller_owns_one_exactly_named_joinable_worker() {
    let mut controller = ApiRuntimeController::start();
    assert_eq!(
        controller
            .worker
            .as_ref()
            .and_then(|worker| worker.thread().name()),
        Some("loxa-menu-api-runtime")
    );
    assert_eq!(controller.shutdown_and_join(), Ok(()));
    assert!(controller.worker.is_none());
}

#[test]
fn runtime_notices_are_closed_static_user_text() {
    assert_eq!(
        ApiRuntimeNotice::Conflict.message(),
        "Another Loxa model operation is active"
    );
    assert_eq!(
        ApiRuntimeNotice::ModelUnavailable.message(),
        "The selected installed model is unavailable"
    );
    assert_eq!(
        ApiRuntimeNotice::StartFailed.message(),
        "Could not start the API"
    );
    assert_eq!(
        ApiRuntimeNotice::CleanupFailed.message(),
        "Could not stop the API. Try Stop API again."
    );
    assert_eq!(
        ApiRuntimeNotice::UnexpectedStop.message(),
        "The API stopped unexpectedly"
    );
}
