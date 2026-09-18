use super::*;
use std::sync::{mpsc, Mutex};
use std::time::Duration;

fn fingerprint(model_id: &str) -> Arc<crate::runtime_fingerprint::RuntimeFingerprint> {
    let manifest = crate::catalog::Manifest {
        version: 2,
        id: model_id.into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(crate::catalog::Origin::Local),
        source_filename: Some("source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 1,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    Arc::new(
        crate::runtime_fingerprint::RuntimeFingerprint::from_manifest_for_service(
            &manifest,
            4096,
            crate::runtime_fingerprint::EffectiveProfile::Generic,
        )
        .unwrap(),
    )
}

fn target(accepted: &Accepted) -> OperationTarget {
    OperationTarget {
        boot_epoch: accepted.boot_epoch.clone(),
        task_id: accepted.task_id.clone(),
        generation: accepted.generation.clone(),
    }
}

fn ready_state() -> (CoordinatorState, Arc<OperationControl>) {
    let mut state = CoordinatorState::new("boot".into(), None, Arc::new(AtomicBool::new(false)));
    let operation = state.reserve_load("demo".into()).unwrap();
    state.accept_start(&operation).unwrap();
    assert!(state.advance(
        &operation,
        OperationPhase::Ready {
            engine: EngineDescriptor {
                pid: 42,
                endpoint: Arc::new("/tmp/engine.sock".into()),
            },
            fingerprint: fingerprint("demo"),
        }
    ));
    (state, operation)
}

#[test]
fn pending_stop_prevents_fresh_admission_without_tombstoning_a_retry() {
    let (mut state, _) = ready_state();
    let pending = state.register_pending_generation().unwrap();
    let target = loxa_ipc::GenerationTarget::Pending {
        boot_epoch: "boot".into(),
        pending_nonce: pending.nonce().into(),
    };
    assert!(state.cancel_generation(&target).unwrap().is_none());
    assert_eq!(
        state
            .reserve_pending_admission(&pending, [1; 16], [2; 16], [3; 32], 1, 1)
            .err()
            .unwrap()
            .category,
        ErrorCategory::ServiceUnavailable
    );
    assert_eq!(
        state.cancel_generation(&target).err().unwrap().category,
        ErrorCategory::NotFound
    );

    let retry = state.register_pending_generation().unwrap();
    assert!(matches!(
        state
            .reserve_pending_admission(&retry, [1; 16], [2; 16], [3; 32], 1, 1)
            .unwrap(),
        AdmissionClaim::Fresh(_)
    ));
}

#[test]
fn cancelled_duplicate_attaches_without_cancelling_the_canonical_admission() {
    let (mut state, _) = ready_state();
    let canonical = state.register_pending_generation().unwrap();
    let admission = match state
        .reserve_pending_admission(&canonical, [1; 16], [2; 16], [3; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(admission) => admission,
        AdmissionClaim::Existing(_) => panic!("canonical reservation was not fresh"),
    };

    let duplicate = state.register_pending_generation().unwrap();
    let duplicate_target = loxa_ipc::GenerationTarget::Pending {
        boot_epoch: "boot".into(),
        pending_nonce: duplicate.nonce().into(),
    };
    assert!(state
        .cancel_generation(&duplicate_target)
        .unwrap()
        .is_none());
    let attached = match state
        .reserve_pending_admission(&duplicate, [1; 16], [2; 16], [3; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Existing(admission) => admission,
        AdmissionClaim::Fresh(_) => panic!("duplicate received a fresh reservation"),
    };
    assert!(Arc::ptr_eq(&admission, &attached));
    assert!(!admission.is_cancelled());
    assert_eq!(
        state
            .cancel_generation(&duplicate_target)
            .err()
            .unwrap()
            .category,
        ErrorCategory::NotFound
    );

    let conflicting = state.register_pending_generation().unwrap();
    assert_eq!(
        state
            .reserve_pending_admission(&conflicting, [1; 16], [2; 16], [4; 32], 1, 1)
            .err()
            .unwrap()
            .category,
        ErrorCategory::Conflict
    );
    assert!(!state.pending_is_current(&conflicting));
    assert!(state.admission_is_current(&admission));
    assert!(!admission.is_cancelled());
}

#[test]
fn finished_pending_connection_cannot_reserve_again() {
    let (mut state, _) = ready_state();
    let stale = state.register_pending_generation().unwrap();
    assert!(state.pending_is_current(&stale));
    state.finish_pending_generation(&stale);
    let current = state.register_pending_generation().unwrap();
    assert_ne!(stale.nonce(), current.nonce());
    assert!(!state.pending_is_current(&stale));
    assert_eq!(
        state
            .reserve_pending_admission(&stale, [1; 16], [2; 16], [3; 32], 1, 1)
            .err()
            .unwrap()
            .category,
        ErrorCategory::Conflict
    );
    assert!(state.pending_is_current(&current));
    assert!(state.current_admission().is_none());
}

#[test]
fn pending_target_follows_its_reservation_and_capacity_waits_for_both_terminals() {
    let (mut state, _) = ready_state();
    let pending = state.register_pending_generation().unwrap();
    let pending_target = loxa_ipc::GenerationTarget::Pending {
        boot_epoch: "boot".into(),
        pending_nonce: pending.nonce().into(),
    };
    let admission = match state
        .reserve_pending_admission(&pending, [1; 16], [2; 16], [3; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(admission) => admission,
        AdmissionClaim::Existing(_) => panic!("fresh send attached unexpectedly"),
    };
    let accepted_target = loxa_ipc::GenerationTarget::Accepted {
        boot_epoch: "boot".into(),
        submission_id: crate::history::encode_id(admission.submission_id),
        operation_generation: admission.operation_generation.to_string(),
    };
    for target in [
        loxa_ipc::GenerationTarget::Pending {
            boot_epoch: "old-boot".into(),
            pending_nonce: pending.nonce().into(),
        },
        loxa_ipc::GenerationTarget::Accepted {
            boot_epoch: "old-boot".into(),
            submission_id: crate::history::encode_id(admission.submission_id),
            operation_generation: admission.operation_generation.to_string(),
        },
        loxa_ipc::GenerationTarget::Accepted {
            boot_epoch: "boot".into(),
            submission_id: crate::history::encode_id([9; 16]),
            operation_generation: admission.operation_generation.to_string(),
        },
        loxa_ipc::GenerationTarget::Accepted {
            boot_epoch: "boot".into(),
            submission_id: crate::history::encode_id(admission.submission_id),
            operation_generation: "999".into(),
        },
    ] {
        assert_eq!(
            state.cancel_generation(&target).err().unwrap().category,
            ErrorCategory::Conflict
        );
        assert!(!admission.is_cancelled());
    }
    for target in [&pending_target, &accepted_target] {
        let matched = state.cancel_generation(target).unwrap().unwrap();
        assert!(Arc::ptr_eq(&matched, &admission));
    }
    assert!(admission.is_cancelled());
    state.finish_admission(&admission);

    let admission = match state
        .reserve_admission([4; 16], [5; 16], [6; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(admission) => admission,
        AdmissionClaim::Existing(_) => panic!("fresh reservation attached unexpectedly"),
    };
    assert_eq!(
        state
            .cancel_generation(&pending_target)
            .err()
            .unwrap()
            .category,
        ErrorCategory::NotFound
    );
    assert_eq!(
        state
            .cancel_generation(&accepted_target)
            .err()
            .unwrap()
            .category,
        ErrorCategory::Conflict
    );
    assert!(!admission.is_cancelled());
    state.begin_generation_execution(&admission).unwrap();
    admission.mark_durable_terminal();
    assert!(!state.finish_admission_if_resolved(&admission));
    assert_eq!(
        state
            .reserve_admission([7; 16], [8; 16], [9; 32], 1, 1)
            .err()
            .unwrap()
            .category,
        ErrorCategory::Busy
    );
    admission.mark_engine_quiescent();
    assert!(state.finish_admission_if_resolved(&admission));
    assert!(matches!(
        state
            .reserve_admission([7; 16], [8; 16], [9; 32], 1, 1)
            .unwrap(),
        AdmissionClaim::Fresh(_)
    ));
}

#[test]
fn runtime_cleanup_releases_a_terminalized_admission_in_either_order() {
    for terminal_first in [false, true] {
        let (mut state, operation) = ready_state();
        let admission = match state
            .reserve_admission([1; 16], [2; 16], [3; 32], 1, 1)
            .unwrap()
        {
            AdmissionClaim::Fresh(admission) => admission,
            AdmissionClaim::Existing(_) => panic!("fresh admission attached unexpectedly"),
        };
        state.begin_generation_execution(&admission).unwrap();
        if terminal_first {
            admission.mark_durable_terminal();
            assert!(state.complete(&operation, None));
        } else {
            assert!(!state.complete(&operation, None));
            admission.mark_durable_terminal();
            assert!(state.finish_admission_if_resolved(&admission));
        }
        assert!(!state.admission_is_current(&admission));
        assert!(state.reserve_load("next".into()).is_ok());
    }
}

#[test]
fn idle_proof_loses_to_stop_and_a_cancelling_runtime_rejects_admission() {
    let (mut state, operation) = ready_state();
    let admission = match state
        .reserve_admission([1; 16], [2; 16], [3; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(admission) => admission,
        AdmissionClaim::Existing(_) => panic!("fresh admission attached unexpectedly"),
    };
    state.begin_generation_execution(&admission).unwrap();
    admission.request_cancel();
    operation.request_cleanup();
    assert!(matches!(
        state.begin_generation_execution(&admission),
        Err(error) if error.category == ErrorCategory::ServiceUnavailable
    ));
    assert!(!state.confirm_generation_quiescence(&admission));
    state.finish_admission(&admission);
    assert!(matches!(
        state.reserve_admission([4; 16], [5; 16], [6; 32], 1, 1),
        Err(error) if error.category == ErrorCategory::ServiceUnavailable
    ));
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
            let ready = worker_state.lock().unwrap().advance(
                &operation,
                OperationPhase::Ready {
                    engine: EngineDescriptor {
                        pid: 42,
                        endpoint: Arc::new("/tmp/engine.sock".into()),
                    },
                    fingerprint: fingerprint("demo"),
                },
            );
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
    assert!(state.advance(
        &operation,
        OperationPhase::Ready {
            engine: EngineDescriptor {
                pid: 42,
                endpoint: Arc::new("/tmp/engine.sock".into()),
            },
            fingerprint: fingerprint("demo"),
        }
    ));
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
    assert!(!state.advance(
        &operation,
        OperationPhase::Ready {
            engine: EngineDescriptor {
                pid: 42,
                endpoint: Arc::new("/tmp/engine.sock".into()),
            },
            fingerprint: fingerprint("demo"),
        }
    ));
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

#[test]
fn unresolved_output_fences_a_new_load_after_runtime_completion() {
    let mut state = CoordinatorState::new("boot".into(), None, Arc::new(AtomicBool::new(false)));
    let operation = state.reserve_load("demo".into()).unwrap();
    state.accept_start(&operation).unwrap();
    assert!(state.advance(
        &operation,
        OperationPhase::Ready {
            engine: EngineDescriptor {
                pid: 42,
                endpoint: Arc::new("/tmp/engine.sock".into()),
            },
            fingerprint: fingerprint("demo"),
        }
    ));
    let reservation = match state
        .reserve_admission([1; 16], [2; 16], [3; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(reservation) => reservation,
        AdmissionClaim::Existing(_) => panic!("fresh admission returned an existing reservation"),
    };

    state.complete(&operation, None);
    assert!(matches!(state.snapshot().phase, RuntimePhase::Unloaded));
    assert_eq!(
        state.reserve_load("other".into()).err().unwrap().category,
        ErrorCategory::Busy
    );

    state.finish_admission(&reservation);
    assert!(state.reserve_load("other".into()).is_ok());
}

#[test]
fn stale_runtime_completion_cannot_cancel_a_replacement_admission() {
    let (mut state, old_operation) = ready_state();
    state.complete(&old_operation, None);

    let replacement = state.reserve_load("replacement".into()).unwrap();
    state.accept_start(&replacement).unwrap();
    assert!(state.advance(
        &replacement,
        OperationPhase::Ready {
            engine: EngineDescriptor {
                pid: 84,
                endpoint: Arc::new("/tmp/replacement.sock".into()),
            },
            fingerprint: fingerprint("replacement"),
        }
    ));
    let admission = match state
        .reserve_admission([4; 16], [5; 16], [6; 32], 1, 1)
        .unwrap()
    {
        AdmissionClaim::Fresh(admission) => admission,
        AdmissionClaim::Existing(_) => panic!("replacement admission was not fresh"),
    };

    state.complete(&old_operation, None);
    assert!(!admission.is_cancelled());
    assert!(state.admission_is_current(&admission));
}
