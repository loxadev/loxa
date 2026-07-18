use crate::artifact_coordinator::{ArtifactKey, ArtifactMutationCoordinator};
use crate::download_scheduler::{
    BoundDownload, DownloadExecutor, DownloadKey, DownloadReserveOutcome, DownloadSchedulerOwner,
    DownloadSubmitOutcome, DownloadWorkerPermit,
};
use crate::lifecycle_controller::{
    LifecycleCancelAcknowledgement, LifecycleCommand, LifecycleControllerHandle,
    LifecycleControllerOwner, LifecycleLoadRequest, LifecycleLoadSubmission, LifecycleLoadWorkflow,
    LifecycleMailboxInner, LifecycleSubmitError, LIFECYCLE_NORMAL_CAPACITY,
};
use crate::model_lifecycle::{
    EngineLifecycleDriver, ExactStopFailure, GatewayPublisher, LaunchPlan, LifecycleError,
    LifecycleSignals, ModelLifecycle, SessionCorrelation, StableNodeOwner, StartedSession,
};
use crate::operation_cancellation::OperationCancellation;
use crate::verification_scheduler::{
    LifecycleVerificationCompletion, LifecycleVerificationContinuation,
    LifecycleVerificationOutcome, VerificationResult,
};
use loxa_core::supervisor::ObservedChildExit;
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-lifecycle-controller-{label}-{}-{}",
            std::process::id(),
            OperationId::new_v4()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn load(id: OperationId, model: &str, revision: u64) -> LifecycleCommand {
    LifecycleCommand::Load {
        operation_id: id,
        model_id: model.into(),
        revision: DecimalU64::new(revision),
    }
}

#[test]
fn normal_capacity_is_reserved_before_submission_and_rolls_back_by_raii() {
    let mailbox = LifecycleMailboxInner::new(LIFECYCLE_NORMAL_CAPACITY);
    let mut reservations = (0..LIFECYCLE_NORMAL_CAPACITY)
        .map(|_| mailbox.reserve_normal().expect("normal position"))
        .collect::<Vec<_>>();
    assert!(mailbox.reserve_normal().is_none());
    reservations.pop();
    assert!(mailbox.reserve_normal().is_some());
}

#[test]
fn lifecycle_verification_conversion_keeps_its_normal_position_until_rollback() {
    let mailbox = LifecycleMailboxInner::new(LIFECYCLE_NORMAL_CAPACITY);
    let completion = mailbox
        .reserve_normal()
        .unwrap()
        .into_verification_completion()
        .unwrap();
    let reservations = (1..LIFECYCLE_NORMAL_CAPACITY)
        .map(|_| mailbox.reserve_normal().expect("remaining normal position"))
        .collect::<Vec<_>>();
    assert!(mailbox.reserve_normal().is_none());
    drop(completion);
    assert!(mailbox.reserve_normal().is_some());
    drop(reservations);
}

#[test]
fn exact_child_exit_coalesces_but_conflict_seals_normal_admission() {
    let mailbox = LifecycleMailboxInner::new(LIFECYCLE_NORMAL_CAPACITY);
    assert_eq!(
        mailbox.observe_child_exit(ObservedChildExit::RequestedStop),
        Ok(())
    );
    assert_eq!(
        mailbox.observe_child_exit(ObservedChildExit::RequestedStop),
        Ok(())
    );
    assert_eq!(
        mailbox.observe_child_exit(ObservedChildExit::Interrupted),
        Err(LifecycleSubmitError::ConflictingChildExit)
    );
    assert!(mailbox.is_sealed());
    assert!(mailbox.reserve_normal().is_none());
}

#[derive(Clone)]
struct TestDriver {
    events: Arc<Mutex<Vec<&'static str>>>,
    live: Arc<AtomicUsize>,
    ready_entered: Option<mpsc::Sender<()>>,
    stop_error: bool,
    panic_stop: bool,
}

struct TestSession(Arc<AtomicUsize>);

impl Drop for TestSession {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl EngineLifecycleDriver for TestDriver {
    type Session = TestSession;

    fn start(
        &mut self,
        owner: &StableNodeOwner,
        plan: &LaunchPlan,
        generation: u64,
    ) -> Result<StartedSession<Self::Session>, LifecycleError> {
        self.events.lock().unwrap().push("start");
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(StartedSession {
            value: TestSession(Arc::clone(&self.live)),
            correlation: SessionCorrelation {
                generation,
                child_pid: 42,
                child_process_start_time_unix_s: 7,
                server_id: "server".into(),
                model_id: plan.model_id.clone(),
                port: 9000,
                committed_run_id: owner.run_id.clone(),
                owner_pid: owner.pid,
                owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                gateway_port: owner.gateway_port,
                generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                engine_version: "test".into(),
            },
        })
    }

    fn wait_ready(
        &mut self,
        _session: &mut StartedSession<Self::Session>,
        signals: LifecycleSignals<'_>,
    ) -> Result<(), LifecycleError> {
        self.events.lock().unwrap().push("ready");
        if let Some(entered) = self.ready_entered.take() {
            entered.send(()).unwrap();
            while !signals.cancellation_requested() && !signals.stop_requested() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if signals.cancellation_requested() {
            Err(LifecycleError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn stop_exact<'a>(
        &mut self,
        session: &'a mut StartedSession<Self::Session>,
    ) -> Result<(), ExactStopFailure<'a, Self::Session>> {
        self.events.lock().unwrap().push("stop");
        if self.panic_stop {
            panic!("injected lifecycle driver panic");
        }
        if self.stop_error {
            Err(ExactStopFailure::new(
                LifecycleError::TeardownFailed("injected teardown failure".into()),
                session,
            ))
        } else {
            Ok(())
        }
    }
}

struct TestGateway;

impl GatewayPublisher for TestGateway {
    fn withdraw(&mut self) {}
    fn publish(&mut self, _plan: &LaunchPlan, _session: &SessionCorrelation) {}
}

#[test]
fn controller_is_the_only_exact_session_owner_and_shutdown_joins_it() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events: Arc::clone(&events),
            live: Arc::clone(&live),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start(lifecycle, |model_id| {
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: model_id.into(),
            engine: "llama-cpp".into(),
        })
    })
    .unwrap();
    let operation_id = OperationId::new_v4();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(operation_id, "model", 1))
        .unwrap();
    let completion = owner
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("load completion");
    assert_eq!(completion.operation_id(), Some(&operation_id));
    assert_eq!(live.load(Ordering::SeqCst), 1);

    owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect("bounded lifecycle join");
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(&*events.lock().unwrap(), &["start", "ready", "stop"]);
}

#[test]
fn cancel_priority_interrupts_active_readiness_without_waiting_for_the_worker_loop() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let live = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events,
            live: Arc::clone(&live),
            ready_entered: Some(entered_tx),
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start(lifecycle, |model_id| {
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: model_id.into(),
            engine: "llama-cpp".into(),
        })
    })
    .unwrap();
    let operation_id = OperationId::new_v4();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(operation_id, "model", 1))
        .unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("readiness entered");
    let started = Instant::now();
    handle.cancel(operation_id).unwrap();
    let completion = owner
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("cancelled completion");
    assert_eq!(completion.operation_id(), Some(&operation_id));
    assert!(matches!(
        completion.result(),
        Err(LifecycleError::Cancelled)
    ));
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(live.load(Ordering::SeqCst), 0);
    owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

struct RollbackSession {
    model_id: String,
    live: Arc<AtomicUsize>,
}

impl Drop for RollbackSession {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

struct RollbackDriver {
    starts: usize,
    fail_starts: Vec<usize>,
    events: Arc<Mutex<Vec<String>>>,
    live: Arc<AtomicUsize>,
}

impl EngineLifecycleDriver for RollbackDriver {
    type Session = RollbackSession;

    fn start(
        &mut self,
        owner: &StableNodeOwner,
        plan: &LaunchPlan,
        generation: u64,
    ) -> Result<StartedSession<Self::Session>, LifecycleError> {
        self.starts += 1;
        self.events
            .lock()
            .unwrap()
            .push(format!("start:{}", plan.model_id));
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(StartedSession {
            value: RollbackSession {
                model_id: plan.model_id.clone(),
                live: Arc::clone(&self.live),
            },
            correlation: SessionCorrelation {
                generation,
                child_pid: 100 + generation as u32,
                child_process_start_time_unix_s: 200 + generation,
                server_id: format!("server-{generation}"),
                model_id: plan.model_id.clone(),
                port: 9000 + generation as u16,
                committed_run_id: owner.run_id.clone(),
                owner_pid: owner.pid,
                owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                gateway_port: owner.gateway_port,
                generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                engine_version: "test".into(),
            },
        })
    }

    fn wait_ready(
        &mut self,
        session: &mut StartedSession<Self::Session>,
        _signals: LifecycleSignals<'_>,
    ) -> Result<(), LifecycleError> {
        if self.fail_starts.contains(&self.starts) {
            return Err(LifecycleError::ReadinessFailed(format!(
                "{} failed",
                session.value.model_id
            )));
        }
        Ok(())
    }

    fn stop_exact<'a>(
        &mut self,
        session: &'a mut StartedSession<Self::Session>,
    ) -> Result<(), ExactStopFailure<'a, Self::Session>> {
        self.events
            .lock()
            .unwrap()
            .push(format!("stop:{}", session.value.model_id));
        Ok(())
    }
}

fn rollback_controller(
    fail_starts: Vec<usize>,
) -> (
    crate::lifecycle_controller::LifecycleControllerHandle,
    LifecycleControllerOwner,
    Arc<Mutex<Vec<String>>>,
    Arc<AtomicUsize>,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        RollbackDriver {
            starts: 0,
            fail_starts,
            events: Arc::clone(&events),
            live: Arc::clone(&live),
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start(lifecycle, |model_id| {
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: model_id.into(),
            engine: "llama-cpp".into(),
        })
    })
    .unwrap();
    (handle, owner, events, live)
}

#[test]
fn replacement_failure_runs_exactly_one_prior_model_rollback() {
    let (handle, owner, events, live) = rollback_controller(vec![2]);
    for (revision, model) in [(1, "prior"), (2, "replacement")] {
        let operation_id = OperationId::new_v4();
        handle
            .reserve_normal()
            .unwrap()
            .submit(load(operation_id, model, revision))
            .unwrap();
        let completion = owner
            .recv_completion_timeout(Duration::from_secs(1))
            .unwrap();
        if model == "replacement" {
            assert!(matches!(
                completion.result(),
                Err(LifecycleError::ReadinessFailed(_))
            ));
        }
    }
    assert_eq!(
        &*events.lock().unwrap(),
        &[
            "start:prior",
            "stop:prior",
            "start:replacement",
            "stop:replacement",
            "start:prior"
        ]
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
    owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

#[test]
fn rollback_failure_enters_recovery_without_a_second_rollback() {
    let (handle, owner, events, live) = rollback_controller(vec![2, 3]);
    for (revision, model) in [(1, "prior"), (2, "replacement")] {
        let operation_id = OperationId::new_v4();
        handle
            .reserve_normal()
            .unwrap()
            .submit(load(operation_id, model, revision))
            .unwrap();
        let completion = owner
            .recv_completion_timeout(Duration::from_secs(1))
            .unwrap();
        if model == "replacement" {
            assert!(matches!(
                completion.result(),
                Err(LifecycleError::RecoveryRequired { .. })
            ));
        }
    }
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.as_str() == "start:prior")
            .count(),
        2
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(handle.reserve_normal().is_none());
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("rollback recovery retains fatal controller ownership");
    failure.into_owner().dispose_fatal_for_test();
}

struct UnknownAcknowledgement;

impl LifecycleLoadWorkflow for UnknownAcknowledgement {
    fn submit_load(
        &mut self,
        request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        drop(completion);
        Ok(LifecycleLoadSubmission::Ready(LaunchPlan {
            model_id: request.model_id.clone(),
            artifact_path: request.model_id.clone().into(),
            engine: "llama-cpp".into(),
        }))
    }

    fn resume_verified(
        &mut self,
        _request: &LifecycleLoadRequest,
        _evidence: &loxa_core::model_inventory::VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        unreachable!()
    }

    fn acknowledge(
        &mut self,
        _request: &LifecycleLoadRequest,
        _result: Result<(), &LifecycleError>,
    ) -> bool {
        false
    }
}

#[test]
fn candidate_ready_unknown_acknowledgement_seals_admission_and_withdraws_authority() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events,
            live,
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) =
        LifecycleControllerOwner::start_with_workflow(lifecycle, UnknownAcknowledgement).unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    let completion = owner
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        completion.result(),
        Err(LifecycleError::RecoveryRequired { .. })
    ));
    assert!(handle.reserve_normal().is_none());
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("unknown acknowledgement retains fatal ownership");
    failure.into_owner().dispose_fatal_for_test();
}

#[test]
fn operationless_child_crash_reaps_exact_owner_seals_and_does_not_restart_or_verify() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let live = Arc::new(AtomicUsize::new(0));
    let resolutions = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events: Arc::clone(&events),
            live: Arc::clone(&live),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let resolve_count = Arc::clone(&resolutions);
    let (handle, owner) = LifecycleControllerOwner::start(lifecycle, move |model_id| {
        resolve_count.fetch_add(1, Ordering::SeqCst);
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: model_id.into(),
            engine: "llama-cpp".into(),
        })
    })
    .unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    owner
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 1);

    handle
        .child_exited(ObservedChildExit::RecoveryRequired)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while live.load(Ordering::SeqCst) != 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "start")
            .count(),
        1
    );
    assert!(handle.reserve_normal().is_none());
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("operationless crash retains fatal lifecycle ownership");
    failure.into_owner().dispose_fatal_for_test();
}

struct DropProbeWorkflow(Arc<AtomicUsize>);

impl Drop for DropProbeWorkflow {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl LifecycleLoadWorkflow for DropProbeWorkflow {
    fn submit_load(
        &mut self,
        _request: &LifecycleLoadRequest,
        _completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        unreachable!()
    }

    fn resume_verified(
        &mut self,
        _request: &LifecycleLoadRequest,
        _evidence: &loxa_core::model_inventory::VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        unreachable!()
    }
}

#[test]
fn injected_thread_spawn_failure_returns_resources_without_dropping_them() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(AtomicUsize::new(0)),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    LifecycleControllerOwner::fail_next_spawn_for_test();
    let failure = match LifecycleControllerOwner::start_with_workflow(
        lifecycle,
        DropProbeWorkflow(Arc::clone(&dropped)),
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("injected spawn failure accepted"),
    };
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    failure.dispose_for_test();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

struct PanicAcknowledge;

impl LifecycleLoadWorkflow for PanicAcknowledge {
    fn submit_load(
        &mut self,
        request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        drop(completion);
        Ok(LifecycleLoadSubmission::Ready(LaunchPlan {
            model_id: request.model_id.clone(),
            artifact_path: request.model_id.clone().into(),
            engine: "llama-cpp".into(),
        }))
    }

    fn resume_verified(
        &mut self,
        _request: &LifecycleLoadRequest,
        _evidence: &loxa_core::model_inventory::VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        unreachable!()
    }

    fn acknowledge(
        &mut self,
        _request: &LifecycleLoadRequest,
        _result: Result<(), &LifecycleError>,
    ) -> bool {
        panic!("injected workflow callback panic")
    }
}

struct RetainedReadyWorkflow(Arc<AtomicUsize>);

impl Drop for RetainedReadyWorkflow {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl LifecycleLoadWorkflow for RetainedReadyWorkflow {
    fn submit_load(
        &mut self,
        request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        drop(completion);
        Ok(LifecycleLoadSubmission::Ready(LaunchPlan {
            model_id: request.model_id.clone(),
            artifact_path: request.model_id.clone().into(),
            engine: "llama-cpp".into(),
        }))
    }

    fn resume_verified(
        &mut self,
        _request: &LifecycleLoadRequest,
        _evidence: &loxa_core::model_inventory::VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        unreachable!()
    }
}

#[test]
fn callback_panic_seals_handle_and_retains_exact_session_until_fatal_disposal() {
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::clone(&live),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) =
        LifecycleControllerOwner::start_with_workflow(lifecycle, PanicAcknowledge).unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !handle.is_sealed_for_test() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(handle.reserve_normal().is_none());
    assert_eq!(live.load(Ordering::SeqCst), 1);
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("panicked lifecycle worker retained");
    assert_eq!(live.load(Ordering::SeqCst), 1);
    failure.into_owner().dispose_fatal_for_test();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn driver_panic_is_caught_and_workflow_resources_remain_in_fatal_envelope() {
    let workflow_dropped = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner-driver-panic".into(),
            pid: 21,
            process_start_time_unix_s: 22,
            gateway_port: 8082,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::clone(&live),
            ready_entered: None,
            stop_error: false,
            panic_stop: true,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start_with_workflow(
        lifecycle,
        RetainedReadyWorkflow(Arc::clone(&workflow_dropped)),
    )
    .unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    owner
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("driver panic retained by worker envelope");
    assert_eq!(workflow_dropped.load(Ordering::SeqCst), 0);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    failure.into_owner().dispose_fatal_for_test();
    assert_eq!(workflow_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn drop_fallback_leaks_uncertain_worker_resources_instead_of_releasing_ownership() {
    let workflow_dropped = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner-drop-panic".into(),
            pid: 31,
            process_start_time_unix_s: 32,
            gateway_port: 8083,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(AtomicUsize::new(0)),
            ready_entered: None,
            stop_error: false,
            panic_stop: true,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start_with_workflow(
        lifecycle,
        RetainedReadyWorkflow(Arc::clone(&workflow_dropped)),
    )
    .unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    owner
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();

    drop(owner);

    assert_eq!(workflow_dropped.load(Ordering::SeqCst), 0);
}

#[test]
fn teardown_error_and_worker_exit_disconnect_never_report_success() {
    let live = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8080,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::clone(&live),
            ready_entered: None,
            stop_error: true,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start(lifecycle, |model_id| {
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: model_id.into(),
            engine: "llama-cpp".into(),
        })
    })
    .unwrap();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(OperationId::new_v4(), "model", 1))
        .unwrap();
    owner
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("teardown uncertainty retained");
    assert_eq!(live.load(Ordering::SeqCst), 1);
    failure.into_owner().dispose_fatal_for_test();
    assert_eq!(live.load(Ordering::SeqCst), 0);

    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner-2".into(),
            pid: 3,
            process_start_time_unix_s: 4,
            gateway_port: 8081,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(AtomicUsize::new(0)),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (_, mut owner) = LifecycleControllerOwner::start(lifecycle, |_model_id| {
        Err(LifecycleError::ModelNotVerified)
    })
    .unwrap();
    owner.disconnect_worker_exit_for_test();
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("worker-exit disconnect retained");
    failure.into_owner().dispose_fatal_for_test();
}

struct BlockingVerificationWorkflow {
    completion: mpsc::Sender<LifecycleVerificationCompletion>,
    release: mpsc::Receiver<()>,
    resumed: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
    cancel_acknowledgement: LifecycleCancelAcknowledgement,
}

impl LifecycleLoadWorkflow for BlockingVerificationWorkflow {
    fn submit_load(
        &mut self,
        _request: &LifecycleLoadRequest,
        completion: LifecycleVerificationCompletion,
    ) -> Result<LifecycleLoadSubmission, LifecycleError> {
        self.completion.send(completion).unwrap();
        self.release.recv().unwrap();
        Ok(LifecycleLoadSubmission::Verifying)
    }

    fn resume_verified(
        &mut self,
        request: &LifecycleLoadRequest,
        _evidence: &loxa_core::model_inventory::VerifiedArtifact,
    ) -> Result<LaunchPlan, LifecycleError> {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        Ok(LaunchPlan {
            model_id: request.model_id.clone(),
            artifact_path: request.model_id.clone().into(),
            engine: "llama-cpp".into(),
        })
    }

    fn cancel(&mut self, _operation_id: &OperationId) -> LifecycleCancelAcknowledgement {
        self.cancelled.fetch_add(1, Ordering::SeqCst);
        self.cancel_acknowledgement
    }
}

fn verifying_controller() -> (
    LifecycleControllerHandle,
    LifecycleControllerOwner,
    mpsc::Receiver<LifecycleVerificationCompletion>,
    mpsc::Sender<()>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    verifying_controller_with_cancel_ack(LifecycleCancelAcknowledgement::DurablyConfirmed)
}

fn verifying_controller_with_cancel_ack(
    cancel_acknowledgement: LifecycleCancelAcknowledgement,
) -> (
    LifecycleControllerHandle,
    LifecycleControllerOwner,
    mpsc::Receiver<LifecycleVerificationCompletion>,
    mpsc::Sender<()>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let (completion_tx, completion_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let resumed = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: "owner-verifying".into(),
            pid: 91,
            process_start_time_unix_s: 92,
            gateway_port: 8091,
        },
        TestDriver {
            events: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(AtomicUsize::new(0)),
            ready_entered: None,
            stop_error: false,
            panic_stop: false,
        },
        TestGateway,
    );
    let (handle, owner) = LifecycleControllerOwner::start_with_workflow(
        lifecycle,
        BlockingVerificationWorkflow {
            completion: completion_tx,
            release: release_rx,
            resumed: Arc::clone(&resumed),
            cancelled: Arc::clone(&cancelled),
            cancel_acknowledgement,
        },
    )
    .unwrap();
    (handle, owner, completion_rx, release_tx, resumed, cancelled)
}

fn publish_lifecycle_verification(
    completion: LifecycleVerificationCompletion,
    operation_id: OperationId,
    coordinator: &ArtifactMutationCoordinator,
    artifact_key: &ArtifactKey,
) {
    let artifact = coordinator.try_acquire_read(artifact_key.clone()).unwrap();
    completion
        .publish(LifecycleVerificationOutcome {
            ownership: LifecycleVerificationContinuation {
                operation_id,
                admission_revision: DecimalU64::new(1),
                cancellation: OperationCancellation::new(),
                artifact,
            },
            result: VerificationResult::Verified(loxa_core::model_inventory::VerifiedArtifact {
                size_bytes: 8,
                expected_sha256: "00".repeat(32),
                matches: true,
            }),
        })
        .unwrap();
}

#[test]
fn ready_verification_loses_to_child_exit_and_fatal_never_dispatches_normal_work() {
    let dir = TestDir::new("child-priority");
    let path = dir.0.join("model.gguf");
    std::fs::write(&path, b"artifact").unwrap();
    let artifact_key = ArtifactKey::from_destination(&path).unwrap();
    let coordinator = ArtifactMutationCoordinator::new();
    let (handle, owner, completion_rx, release, resumed, cancelled) = verifying_controller();
    let operation_id = OperationId::new_v4();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(operation_id, "model", 1))
        .unwrap();
    let completion = completion_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    publish_lifecycle_verification(completion, operation_id, &coordinator, &artifact_key);
    handle
        .child_exited(ObservedChildExit::RecoveryRequired)
        .unwrap();
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !handle.is_sealed_for_test() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    assert!(handle.reserve_normal().is_none());
    assert!(coordinator
        .try_acquire_mutation(artifact_key.clone())
        .is_err());
    let failure = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .expect_err("child-exit fatal ownership retained");
    failure.into_owner().dispose_fatal_for_test();
    assert!(coordinator.try_acquire_mutation(artifact_key).is_ok());
}

#[test]
fn owner_shutdown_outranks_ready_verification_and_cloneable_handle_cannot_shutdown() {
    let dir = TestDir::new("shutdown-priority");
    let path = dir.0.join("model.gguf");
    std::fs::write(&path, b"artifact").unwrap();
    let artifact_key = ArtifactKey::from_destination(&path).unwrap();
    let coordinator = ArtifactMutationCoordinator::new();
    let (handle, owner, completion_rx, release, resumed, cancelled) = verifying_controller();
    let operation_id = OperationId::new_v4();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(operation_id, "model", 1))
        .unwrap();
    let completion = completion_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    publish_lifecycle_verification(completion, operation_id, &coordinator, &artifact_key);
    let shutdown =
        std::thread::spawn(move || owner.shutdown(Instant::now() + Duration::from_secs(1)));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !handle.is_sealed_for_test() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    release.send(()).unwrap();
    assert!(shutdown.join().unwrap().is_ok());
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    assert!(coordinator.try_acquire_mutation(artifact_key).is_ok());
}

#[test]
fn unknown_durable_cancel_ack_retains_ready_verification_until_fatal_disposal() {
    let dir = TestDir::new("shutdown-unknown-cancel-ack");
    let path = dir.0.join("model.gguf");
    std::fs::write(&path, b"artifact").unwrap();
    let artifact_key = ArtifactKey::from_destination(&path).unwrap();
    let coordinator = ArtifactMutationCoordinator::new();
    let (handle, owner, completion_rx, release, resumed, cancelled) =
        verifying_controller_with_cancel_ack(LifecycleCancelAcknowledgement::Unknown);
    let operation_id = OperationId::new_v4();
    handle
        .reserve_normal()
        .unwrap()
        .submit(load(operation_id, "model", 1))
        .unwrap();
    let completion = completion_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    publish_lifecycle_verification(completion, operation_id, &coordinator, &artifact_key);
    let shutdown =
        std::thread::spawn(move || owner.shutdown(Instant::now() + Duration::from_secs(1)));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !handle.is_sealed_for_test() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    release.send(()).unwrap();
    let failure = shutdown
        .join()
        .unwrap()
        .expect_err("unknown durable cancel acknowledgement must retain fatal owner");
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    assert!(coordinator
        .try_acquire_mutation(artifact_key.clone())
        .is_err());
    failure.into_owner().dispose_fatal_for_test();
    assert!(coordinator.try_acquire_mutation(artifact_key).is_ok());
}

struct BlockingDownloadExecutor {
    started: mpsc::Sender<OperationId>,
    release: Arc<(Mutex<bool>, Condvar)>,
    active: Arc<AtomicUsize>,
}

impl DownloadExecutor for BlockingDownloadExecutor {
    fn execute(&self, bound: BoundDownload, permit: DownloadWorkerPermit) {
        self.active.fetch_add(1, Ordering::SeqCst);
        self.started.send(bound.operation_id()).unwrap();
        let (released, changed) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        drop(released);
        permit.release();
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn concurrent_download_key(sequence: u8, dir: &TestDir) -> DownloadKey {
    let artifact =
        ArtifactKey::from_destination(&dir.0.join(format!("model-{sequence}.gguf"))).unwrap();
    DownloadKey::new(
        &format!("model-{sequence}"),
        "hugging-face",
        &format!("publisher/repository-{sequence}"),
        Some("0123456789abcdef0123456789abcdef01234567"),
        &format!("weights/model-{sequence}.gguf"),
        Some([sequence; 32]),
        Some(u64::from(sequence)),
        artifact,
    )
    .unwrap()
}

#[test]
fn two_download_workers_remain_active_while_lifecycle_verification_completes() {
    let download_dir = TestDir::new("parallel-downloads");
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let active = Arc::new(AtomicUsize::new(0));
    let (download_handle, download_owner) =
        DownloadSchedulerOwner::spawn(Arc::new(BlockingDownloadExecutor {
            started: started_tx,
            release: Arc::clone(&release),
            active: Arc::clone(&active),
        }))
        .unwrap();
    let mut operations = Vec::new();
    for sequence in [1_u8, 2] {
        let reservation =
            match download_handle.reserve(concurrent_download_key(sequence, &download_dir)) {
                DownloadReserveOutcome::Reserved(reservation) => reservation,
                _ => panic!("fresh download reservation rejected"),
            };
        let operation_id = OperationId::new_v4();
        let bound = reservation
            .bind(
                operation_id,
                DecimalU64::new(u64::from(sequence)),
                OperationCancellation::new(),
            )
            .unwrap();
        assert_eq!(
            download_handle.submit(bound),
            DownloadSubmitOutcome::Submitted
        );
        operations.push(operation_id);
    }
    for _ in 0..2 {
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("both download workers started");
    }
    assert_eq!(active.load(Ordering::SeqCst), 2);

    let verification_dir = TestDir::new("parallel-verification");
    let artifact_path = verification_dir.0.join("model.gguf");
    std::fs::write(&artifact_path, b"artifact").unwrap();
    let artifact_key = ArtifactKey::from_destination(&artifact_path).unwrap();
    let coordinator = ArtifactMutationCoordinator::new();
    let (lifecycle_handle, lifecycle_owner, completion_rx, resume, resumed, _) =
        verifying_controller();
    let lifecycle_operation = OperationId::new_v4();
    lifecycle_handle
        .reserve_normal()
        .unwrap()
        .submit(load(lifecycle_operation, "model", 1))
        .unwrap();
    let verification = completion_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    publish_lifecycle_verification(
        verification,
        lifecycle_operation,
        &coordinator,
        &artifact_key,
    );
    resume.send(()).unwrap();
    let completion = lifecycle_owner
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("lifecycle verification completion");
    assert!(completion.result().is_ok());
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(
        active.load(Ordering::SeqCst),
        2,
        "lifecycle verification must not consume a download worker"
    );

    let (released, changed) = &*release;
    *released.lock().unwrap() = true;
    changed.notify_all();
    let deadline = Instant::now() + Duration::from_secs(1);
    while active.load(Ordering::SeqCst) != 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    for operation_id in operations {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut finished = download_handle.finish_committed(operation_id);
        while !finished && Instant::now() < deadline {
            std::thread::yield_now();
            finished = download_handle.finish_committed(operation_id);
        }
        assert!(finished, "download terminal ownership did not release");
    }
    lifecycle_owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
    download_handle.stop();
    download_owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}
