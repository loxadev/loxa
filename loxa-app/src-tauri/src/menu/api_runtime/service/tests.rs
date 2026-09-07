use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use loxa::api_runtime::ApiStartCancellation;
use loxa_ipc::{OperationTarget, RuntimePhase, RuntimeStatus};

use super::{
    ServiceEvent, ServiceObservation, ServiceObservationSlot, ServiceRequest, ServiceShutdownReply,
};
use crate::menu::api_presentation::ApiPresentation;
use crate::menu::api_runtime::{
    ApiEndpoint, ApiRuntimeController, ApiRuntimeNotice, ApiRuntimePhase,
    StartOutcome as RuntimeHostStart,
};

fn service_target(epoch: &str, task_id: &str, generation: &str) -> OperationTarget {
    OperationTarget {
        boot_epoch: epoch.into(),
        task_id: task_id.into(),
        generation: generation.into(),
    }
}

fn service_observation(epoch: &str, phase: RuntimePhase) -> ServiceObservation {
    ServiceObservation::Present(RuntimeStatus {
        boot_epoch: epoch.into(),
        state_revision: "1".into(),
        phase,
    })
}

fn observed_starting(target: &OperationTarget, model_id: &str) -> ServiceObservation {
    service_observation(
        &target.boot_epoch,
        RuntimePhase::Starting {
            task_id: target.task_id.clone(),
            generation: target.generation.clone(),
            model_id: model_id.into(),
        },
    )
}

fn observed_ready(target: &OperationTarget, model_id: &str) -> ServiceObservation {
    service_observation(
        &target.boot_epoch,
        RuntimePhase::Ready {
            task_id: target.task_id.clone(),
            generation: target.generation.clone(),
            model_id: model_id.into(),
            engine_pid: 42,
        },
    )
}

fn observed_stopping(target: &OperationTarget, model_id: &str) -> ServiceObservation {
    service_observation(
        &target.boot_epoch,
        RuntimePhase::Stopping {
            task_id: target.task_id.clone(),
            generation: target.generation.clone(),
            model_id: model_id.into(),
        },
    )
}

fn observed_unloaded(epoch: &str) -> ServiceObservation {
    service_observation(epoch, RuntimePhase::Unloaded)
}

fn observed_service_controller() -> ApiRuntimeController {
    let disconnect = Arc::new(AtomicBool::new(false));
    ApiRuntimeController::assemble_service(
        false,
        disconnect,
        Arc::new(ServiceObservationSlot::default()),
        |requests, messages| {
            std::thread::Builder::new()
                .name("loxa-menu-service-observation-test".into())
                .spawn(move || {
                    let _messages = messages;
                    while let Ok(request) = requests.recv() {
                        if let ServiceRequest::Shutdown { reply } = request {
                            let _ = reply.send(ServiceShutdownReply {
                                generation: None,
                                result: Ok(()),
                                endpoint: None,
                            });
                            return;
                        }
                    }
                })
                .map_err(|_| ())
        },
    )
}

#[test]
fn absent_service_is_distinct_from_unloaded_and_load_remains_available() {
    let mut controller = observed_service_controller();
    assert!(!controller.service_initialized());

    assert!(controller
        .service_backend_mut()
        .apply_observed(ServiceObservation::Absent));

    assert!(controller.service_initialized());
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
    assert_eq!(controller.notice(), Some(ApiRuntimeNotice::ServiceAbsent));
    let presentation = ApiPresentation::from_controller(&controller);
    assert_eq!(presentation.status_label(), "Background service stopped");
    assert!(presentation.primary_action("demo").is_enabled());
    controller.shutdown_and_join().unwrap();
}

#[test]
fn authoritative_service_observations_converge_through_external_transitions() {
    let mut controller = observed_service_controller();
    let target = service_target("epoch-a", "9", "12");

    controller
        .service_backend_mut()
        .apply_observed(observed_starting(&target, "demo"));
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::Starting { .. }
    ));

    controller
        .service_backend_mut()
        .apply_observed(observed_ready(&target, "demo"));
    assert!(matches!(controller.phase(), ApiRuntimePhase::Ready { .. }));

    controller
        .service_backend_mut()
        .apply_observed(observed_stopping(&target, "demo"));
    assert!(matches!(controller.phase(), ApiRuntimePhase::Stopping));

    controller
        .service_backend_mut()
        .apply_observed(observed_unloaded("epoch-a"));
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
    controller.shutdown_and_join().unwrap();
}

#[test]
fn shared_stop_admission_captures_the_full_observed_target() {
    let disconnect = Arc::new(AtomicBool::new(false));
    let (captured, received) = mpsc::channel();
    let mut controller = ApiRuntimeController::assemble_service(
        true,
        disconnect,
        Arc::new(ServiceObservationSlot::default()),
        move |requests, _| {
            std::thread::Builder::new()
                .name("loxa-menu-service-target-test".into())
                .spawn(move || {
                    while let Ok(request) = requests.recv() {
                        match request {
                            ServiceRequest::Stop { target, .. } => {
                                let _ = captured.send(target);
                            }
                            ServiceRequest::Shutdown { reply } => {
                                let _ = reply.send(ServiceShutdownReply {
                                    generation: None,
                                    result: Ok(()),
                                    endpoint: None,
                                });
                                return;
                            }
                            ServiceRequest::Start { .. } | ServiceRequest::Probe => {}
                        }
                    }
                })
                .map_err(|_| ())
        },
    );
    let target = service_target("epoch-a", "4", "6");
    controller
        .service_backend_mut()
        .apply_observed(observed_ready(&target, "demo"));

    assert!(controller.request_stop());
    assert_eq!(
        received.recv_timeout(Duration::from_secs(2)),
        Ok(Some(target))
    );
    controller.shutdown_and_join().unwrap();
}

#[test]
fn shared_shutdown_disconnects_without_cancelling_or_relabeling_a_load() {
    let mut controller = observed_service_controller();
    let cancellation = ApiStartCancellation::new();
    controller.service_backend_mut().initialized = true;
    controller.service_backend_mut().generation = Some(3);
    controller.service_backend_mut().startup_cancellation = Some(cancellation.clone());
    controller.service_backend_mut().state.active_model_id = Some("demo".into());
    controller.service_backend_mut().state.phase = ApiRuntimePhase::Starting {
        generation: 3,
        model_id: "demo".into(),
    };

    assert!(!controller.prepare_shutdown());
    assert!(!cancellation.is_cancelled());
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::Starting { .. }
    ));
    assert!(controller
        .service_backend()
        .disconnect
        .load(Ordering::Acquire));

    controller.shutdown_and_join().unwrap();
    assert!(!cancellation.is_cancelled());
}

#[test]
fn service_observation_slot_keeps_only_the_latest_authoritative_snapshot() {
    let slot = ServiceObservationSlot::default();
    let first = service_target("epoch-a", "1", "1");
    let latest = service_target("epoch-a", "2", "2");

    slot.publish(observed_starting(&first, "first")).unwrap();
    slot.publish(observed_ready(&latest, "latest")).unwrap();

    assert!(matches!(
        slot.take().unwrap(),
        Some(ServiceObservation::Present(RuntimeStatus {
            phase: RuntimePhase::Ready { model_id, .. },
            ..
        })) if model_id == "latest"
    ));
    assert_eq!(slot.take(), Ok(None));
}

#[test]
fn service_observation_waits_for_its_command_completion_before_being_taken() {
    let mut controller = observed_service_controller();
    controller.service_backend_mut().initialized = true;
    assert!(controller.request_start("requested".into()));

    let latest_target = service_target("epoch-a", "8", "8");
    controller
        .service_backend()
        .observation
        .publish(observed_ready(&latest_target, "latest"))
        .unwrap();

    controller.drain();
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::Starting { model_id, .. } if model_id == "requested"
    ));

    let accepted_target = service_target("epoch-a", "7", "7");
    controller
        .service_backend_mut()
        .apply_event(ServiceEvent::Started {
            generation: 1,
            outcome: RuntimeHostStart::Ready(ApiEndpoint::service(
                "requested".into(),
                accepted_target,
            )),
        });
    controller.drain();
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::Ready { endpoint, .. } if endpoint.model_id == "latest"
    ));
    controller.shutdown_and_join().unwrap();
}

#[test]
fn cleanup_failed_start_accepts_same_epoch_external_unload() {
    let mut controller = observed_service_controller();
    controller.service_backend_mut().initialized = true;
    controller.service_backend_mut().generation = Some(1);
    controller.service_backend_mut().command_in_flight = true;
    controller.service_backend_mut().state.phase = ApiRuntimePhase::Starting {
        generation: 1,
        model_id: "demo".into(),
    };
    let target = service_target("epoch-a", "7", "1");

    assert!(controller
        .service_backend_mut()
        .apply_event(ServiceEvent::Started {
            generation: 1,
            outcome: RuntimeHostStart::CleanupFailed(ApiEndpoint::service(
                "demo".into(),
                target.clone(),
            )),
        }));
    assert!(matches!(controller.phase(), ApiRuntimePhase::CleanupFailed));
    assert_eq!(controller.service_backend().target.as_ref(), Some(&target));

    assert!(controller
        .service_backend_mut()
        .apply_observed(observed_unloaded(&target.boot_epoch)));
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
    assert_eq!(controller.notice(), None);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn cancelled_start_cleanup_failure_accepts_same_epoch_external_unload() {
    let mut controller = observed_service_controller();
    controller.service_backend_mut().initialized = true;
    controller.service_backend_mut().generation = Some(4);
    controller.service_backend_mut().command_in_flight = true;
    controller.service_backend_mut().state.phase = ApiRuntimePhase::Stopping;
    let target = service_target("epoch-a", "7", "4");

    assert!(controller
        .service_backend_mut()
        .apply_event(ServiceEvent::Stopped {
            generation: Some(4),
            result: Err(()),
            endpoint: Some(ApiEndpoint::service("demo".into(), target.clone())),
        }));
    assert!(matches!(controller.phase(), ApiRuntimePhase::CleanupFailed));
    assert_eq!(controller.service_backend().target.as_ref(), Some(&target));

    assert!(controller
        .service_backend_mut()
        .apply_observed(observed_unloaded(&target.boot_epoch)));
    assert!(matches!(controller.phase(), ApiRuntimePhase::Idle));
    assert_eq!(controller.notice(), None);
    controller.shutdown_and_join().unwrap();
}

#[test]
fn absent_or_restarted_service_cannot_erase_retained_runtime_authority() {
    let mut controller = observed_service_controller();
    let retained = service_target("epoch-a", "3", "4");
    controller
        .service_backend_mut()
        .apply_observed(observed_ready(&retained, "retained"));

    controller
        .service_backend_mut()
        .apply_observed(ServiceObservation::Absent);
    assert!(matches!(controller.phase(), ApiRuntimePhase::CleanupFailed));
    assert_eq!(controller.notice(), Some(ApiRuntimeNotice::CleanupFailed));
    assert_eq!(controller.active_model_id(), Some("retained"));

    let restarted = service_target("epoch-b", "3", "4");
    controller
        .service_backend_mut()
        .apply_observed(observed_ready(&restarted, "replacement"));
    assert!(matches!(controller.phase(), ApiRuntimePhase::CleanupFailed));
    assert_eq!(controller.active_model_id(), Some("retained"));
    controller.shutdown_and_join().unwrap();
}

#[test]
fn draining_or_recovery_required_service_is_unavailable_with_retained_authority() {
    for phase in [
        RuntimePhase::Draining,
        RuntimePhase::RecoveryRequired {
            reason: "repair required".into(),
        },
    ] {
        let mut controller = observed_service_controller();
        let retained = service_target("epoch-a", "3", "4");
        controller
            .service_backend_mut()
            .apply_observed(observed_ready(&retained, "retained"));

        controller
            .service_backend_mut()
            .apply_observed(service_observation("epoch-a", phase));

        assert!(matches!(
            controller.phase(),
            ApiRuntimePhase::ControllerFailed
        ));
        assert_eq!(
            controller.notice(),
            Some(ApiRuntimeNotice::ControllerFailed)
        );
        controller.shutdown_and_join().unwrap();
    }
}

#[test]
fn terminated_service_worker_takes_precedence_over_its_last_queued_snapshot() {
    let disconnect = Arc::new(AtomicBool::new(false));
    let observation = Arc::new(ServiceObservationSlot::default());
    let mut controller = ApiRuntimeController::assemble_service(
        true,
        disconnect,
        Arc::clone(&observation),
        |_requests, _messages| {
            std::thread::Builder::new()
                .name("loxa-menu-dead-service-worker-test".into())
                .spawn(|| {})
                .map_err(|_| ())
        },
    );
    let target = service_target("epoch-a", "1", "1");
    observation
        .publish(observed_ready(&target, "stale"))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while controller
        .service_backend()
        .worker
        .as_ref()
        .is_some_and(|worker| !worker.is_finished())
    {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }

    controller.drain();
    assert!(matches!(
        controller.phase(),
        ApiRuntimePhase::ControllerFailed
    ));
    assert_eq!(observation.take(), Ok(None));
    assert!(controller.shutdown_and_join().is_err());
}
