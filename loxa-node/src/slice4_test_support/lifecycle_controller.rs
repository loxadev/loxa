use crate::lifecycle_controller::{
    LifecycleCommand, LifecycleControllerOwner, LifecycleLoadRequest, LifecycleLoadSubmission,
    LifecycleLoadWorkflow, LifecycleMailboxInner, LifecycleSubmitError, LIFECYCLE_NORMAL_CAPACITY,
};
use crate::model_lifecycle::{
    EngineLifecycleDriver, GatewayPublisher, LaunchPlan, LifecycleError, LifecycleSignals,
    ModelLifecycle, SessionCorrelation, StableNodeOwner, StartedSession,
};
use crate::verification_scheduler::LifecycleVerificationCompletion;
use loxa_core::supervisor::ObservedChildExit;
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

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
fn priority_slots_outrank_queued_normal_and_shutdown_keeps_earliest_deadline() {
    let mailbox = LifecycleMailboxInner::new(LIFECYCLE_NORMAL_CAPACITY);
    let normal_id = OperationId::new_v4();
    mailbox
        .reserve_normal()
        .unwrap()
        .submit(load(normal_id, "normal", 1))
        .unwrap();
    let cancel_id = OperationId::new_v4();
    assert_eq!(mailbox.request_cancel(cancel_id), Ok(()));
    assert_eq!(mailbox.request_cancel(cancel_id), Ok(()));
    let late = Instant::now() + Duration::from_secs(2);
    let early = Instant::now() + Duration::from_secs(1);
    mailbox.request_shutdown(late).unwrap();
    mailbox.request_shutdown(early).unwrap();

    assert!(matches!(
        mailbox.take_next_for_test(),
        Some(LifecycleCommand::Shutdown { deadline }) if deadline == early
    ));
    assert!(matches!(
        mailbox.take_next_for_test(),
        Some(LifecycleCommand::Cancel { operation_id }) if operation_id == cancel_id
    ));
    assert!(matches!(
        mailbox.take_next_for_test(),
        Some(LifecycleCommand::Load { operation_id, .. }) if operation_id == normal_id
    ));
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

    fn stop_exact(&mut self, session: StartedSession<Self::Session>) -> Result<(), LifecycleError> {
        self.events.lock().unwrap().push("stop");
        drop(session);
        Ok(())
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

    fn stop_exact(&mut self, session: StartedSession<Self::Session>) -> Result<(), LifecycleError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("stop:{}", session.value.model_id));
        drop(session);
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
    owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
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
