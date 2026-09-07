use super::*;
use std::sync::{mpsc, Mutex};
use std::time::Duration;

fn target(accepted: &Accepted) -> OperationTarget {
    OperationTarget {
        boot_epoch: accepted.boot_epoch.clone(),
        task_id: accepted.task_id.clone(),
        generation: accepted.generation.clone(),
    }
}

#[test]
fn stop_during_blocked_start_cancels_without_waiting_and_prevents_late_ready() {
    for stop_service in [false, true] {
        let draining = Arc::new(AtomicBool::new(false));
        let mut state = CoordinatorState::new("boot".into(), None, Arc::clone(&draining));
        let operation = state.reserve_load("demo".into()).unwrap();
        let accepted = state.accept_start(&operation).unwrap();
        let state = Arc::new(Mutex::new(state));
        let worker_state = Arc::clone(&state);
        let (release, blocked) = mpsc::channel();
        let (finished, completion) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            // Simulates a synchronous startup holding runtime ownership, not
            // the transition lock. Cancellation must reach it independently.
            blocked.recv_timeout(Duration::from_secs(2)).unwrap();
            let cancelled = operation.cancel.load(Ordering::Acquire);
            let ready = worker_state
                .lock()
                .unwrap()
                .advance(&operation, OperationPhase::Ready { engine_pid: 42 });
            finished.send((cancelled, ready)).unwrap();
        });

        let (stop, observed) = {
            let mut state = state.lock().unwrap();
            let stop = if stop_service {
                state.stop_service().unwrap()
            } else {
                state.unload(&target(&accepted)).unwrap()
            };
            assert!(state.reserve_load("other".into()).is_err());
            (stop, state.snapshot())
        };
        assert_ne!(stop.state_revision, accepted.state_revision);
        assert_eq!(observed.state_revision, stop.state_revision);
        assert_eq!(draining.load(Ordering::Acquire), stop_service);
        if stop_service {
            assert!(matches!(observed.phase, RuntimePhase::Draining));
        } else {
            assert!(matches!(observed.phase, RuntimePhase::Stopping { .. }));
        }
        release.send(()).unwrap();
        assert_eq!(
            completion.recv_timeout(Duration::from_secs(2)).unwrap(),
            (true, false)
        );
        worker.join().unwrap();
        assert_eq!(state.lock().unwrap().snapshot(), observed);
    }
}

#[test]
fn cleanup_failure_keeps_admission_and_exact_identity_until_explicit_retry_completes() {
    let mut state = CoordinatorState::new("boot".into(), None, Arc::new(AtomicBool::new(false)));
    let operation = state.reserve_load("demo".into()).unwrap();
    let accepted = state.accept_start(&operation).unwrap();
    assert!(state.advance(&operation, OperationPhase::Ready { engine_pid: 42 }));
    state.unload(&target(&accepted)).unwrap();
    let attempted = operation.retry_cleanup.load(Ordering::Acquire);
    assert!(state.advance(&operation, OperationPhase::CleanupFailed));
    let failed = state.snapshot();
    assert!(matches!(failed.phase, RuntimePhase::CleanupFailed { .. }));
    assert_eq!(
        state.reserve_load("other".into()).err().unwrap().category,
        ErrorCategory::Busy
    );

    let exact = target(&accepted);
    for field in [0, 1, 2] {
        let mut stale = exact.clone();
        match field {
            0 => stale.boot_epoch = "another-boot".into(),
            1 => stale.task_id = "999".into(),
            _ => stale.generation = "999".into(),
        }
        assert_eq!(
            state.unload(&stale).unwrap_err().category,
            ErrorCategory::Conflict
        );
        assert_eq!(state.snapshot(), failed);
        assert_eq!(operation.retry_cleanup.load(Ordering::Acquire), attempted);
    }
    state.unload(&exact).unwrap();
    assert_eq!(
        operation.retry_cleanup.load(Ordering::Acquire),
        attempted + 1
    );
    assert!(state.reserve_load("other".into()).is_err());
    state.complete(&operation, None);
    assert!(matches!(state.snapshot().phase, RuntimePhase::Unloaded));

    let next = state.reserve_load("other".into()).unwrap();
    let accepted_next = state.accept_start(&next).unwrap();
    assert_ne!(accepted_next.task_id, accepted.task_id);
    assert_ne!(accepted_next.generation, accepted.generation);
    let starting = state.snapshot();
    state.complete(&operation, None);
    assert!(!state.advance(&operation, OperationPhase::Ready { engine_pid: 42 }));
    assert_eq!(
        state.snapshot(),
        starting,
        "late completion changed the next operation"
    );

    state.stop_service().unwrap();
    assert!(state.advance(&next, OperationPhase::CleanupFailed));
    let attempted = next.retry_cleanup.load(Ordering::Acquire);
    state.stop_service().unwrap();
    assert_eq!(next.retry_cleanup.load(Ordering::Acquire), attempted + 1);
    state.complete(&next, None);
    assert!(matches!(state.snapshot().phase, RuntimePhase::Draining));
    assert_eq!(
        state.reserve_load("later".into()).err().unwrap().category,
        ErrorCategory::ServiceUnavailable
    );
}
