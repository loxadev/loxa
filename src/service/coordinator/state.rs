use super::OperationControl;
use loxa_ipc::{
    Accepted, ErrorCategory, OperationTarget, RuntimePhase, RuntimeStatus, ServiceError,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

// All admission and publication decisions run under Shared::state. The watch
// value is an observer copy: never read it back to decide a transition.
pub(super) struct CoordinatorState {
    current: Option<Arc<OperationControl>>,
    next_task_id: u64,
    next_generation: u64,
    revision: u64,
    phase: Phase,
    boot_epoch: String,
    // The owner reads this same signal without the state lock while blocked in
    // preparation/startup. Only transitions under the state lock set it.
    draining: Arc<AtomicBool>,
    snapshots: watch::Sender<RuntimeStatus>,
}

// Reservation precedes durable intent publication. During that interval the
// previous observer phase remains visible, but current already excludes Load.
// Terminal LoadFailed also retains its identity after releasing admission.
enum Phase {
    Unloaded,
    Operation {
        control: Arc<OperationControl>,
        phase: OperationPhase,
    },
    RecoveryRequired(String),
    Draining,
}

#[derive(Clone, Copy)]
pub(super) enum OperationPhase {
    Starting,
    Ready { engine_pid: u32 },
    Stopping,
    CleanupFailed,
    LoadFailed(ErrorCategory),
}

impl CoordinatorState {
    pub(super) fn new(
        boot_epoch: String,
        recovery: Option<String>,
        draining: Arc<AtomicBool>,
    ) -> Self {
        let phase = recovery.map_or(Phase::Unloaded, |reason| {
            Phase::RecoveryRequired(
                ServiceError::new(ErrorCategory::RecoveryRequired, reason).context,
            )
        });
        let initial = RuntimeStatus {
            boot_epoch: boot_epoch.clone(),
            state_revision: "0".into(),
            phase: phase.to_wire(),
        };
        let (snapshots, _) = watch::channel(initial);
        Self {
            current: None,
            next_task_id: 1,
            next_generation: 1,
            revision: 0,
            phase,
            boot_epoch,
            draining,
            snapshots,
        }
    }

    pub(super) fn snapshot(&self) -> RuntimeStatus {
        RuntimeStatus {
            boot_epoch: self.boot_epoch.clone(),
            state_revision: self.revision.to_string(),
            phase: self.phase.to_wire(),
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<RuntimeStatus> {
        self.snapshots.subscribe()
    }

    pub(super) fn reserve_load(
        &mut self,
        model_id: String,
    ) -> Result<Arc<OperationControl>, ServiceError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                ErrorCategory::ServiceUnavailable,
                "service is draining",
            ));
        }
        if let Phase::RecoveryRequired(reason) = &self.phase {
            return Err(ServiceError::new(
                ErrorCategory::RecoveryRequired,
                reason.clone(),
            ));
        }
        if self.current.is_some()
            || !matches!(
                self.phase,
                Phase::Unloaded
                    | Phase::Operation {
                        phase: OperationPhase::LoadFailed(_),
                        ..
                    }
            )
        {
            return Err(ServiceError::new(
                ErrorCategory::Busy,
                "another model operation is active",
            ));
        }
        let operation = Arc::new(OperationControl {
            task_id: self.next_task_id,
            generation: self.next_generation,
            model_id,
            cancel: AtomicBool::new(false),
            retry_cleanup: AtomicU64::new(0),
        });
        self.next_task_id = self.next_task_id.saturating_add(1);
        self.next_generation = self.next_generation.saturating_add(1);
        self.current = Some(Arc::clone(&operation));
        Ok(operation)
    }

    pub(super) fn accept_start(&mut self, operation: &Arc<OperationControl>) -> Option<Accepted> {
        self.advance(operation, OperationPhase::Starting)
            .then(|| self.accepted(Some(operation)))
    }

    pub(super) fn unload(&mut self, target: &OperationTarget) -> Result<Accepted, ServiceError> {
        let operation = self.current.as_ref().ok_or_else(|| {
            ServiceError::new(ErrorCategory::NotFound, "no model operation is active")
        })?;
        if target.boot_epoch != self.boot_epoch
            || target.task_id != operation.task_id.to_string()
            || target.generation != operation.generation.to_string()
        {
            return Err(ServiceError::new(
                ErrorCategory::Conflict,
                "unload target does not identify the active operation",
            ));
        }
        let operation = Arc::clone(operation);
        operation.request_cleanup();
        self.advance(&operation, OperationPhase::Stopping);
        Ok(self.accepted(Some(&operation)))
    }

    pub(super) fn stop_service(&mut self) -> Result<Accepted, ServiceError> {
        if let Phase::RecoveryRequired(reason) = &self.phase {
            return Err(ServiceError::new(
                ErrorCategory::RecoveryRequired,
                reason.clone(),
            ));
        }
        self.begin_draining();
        Ok(self.accepted(None))
    }

    pub(super) fn begin_draining(&mut self) {
        self.draining.store(true, Ordering::Release);
        if let Some(operation) = &self.current {
            operation.request_cleanup();
        }
        self.publish(Phase::Draining);
    }

    pub(super) fn advance(
        &mut self,
        expected: &Arc<OperationControl>,
        phase: OperationPhase,
    ) -> bool {
        if !self.is_current(expected) {
            return false;
        }
        // Stop and Ready serialize on this lock. Cleanup failure must remain
        // visible even during drain, because the owner still retains resources.
        if !matches!(phase, OperationPhase::CleanupFailed)
            && (self.draining.load(Ordering::Acquire)
                || matches!(
                    phase,
                    OperationPhase::Starting | OperationPhase::Ready { .. }
                ) && expected.cancel.load(Ordering::Acquire))
        {
            return false;
        }
        self.publish(Phase::Operation {
            control: Arc::clone(expected),
            phase,
        });
        true
    }

    pub(super) fn complete(
        &mut self,
        expected: &Arc<OperationControl>,
        failure: Option<ErrorCategory>,
    ) {
        let phase = failure.map_or(Phase::Unloaded, |category| Phase::Operation {
            control: Arc::clone(expected),
            phase: OperationPhase::LoadFailed(category),
        });
        self.release_and_publish(expected, phase);
    }

    pub(super) fn require_recovery(&mut self, expected: &Arc<OperationControl>, reason: &str) {
        self.release_and_publish(
            expected,
            Phase::RecoveryRequired(
                ServiceError::new(ErrorCategory::RecoveryRequired, reason).context,
            ),
        );
    }

    pub(super) fn release_reservation(&mut self, expected: &Arc<OperationControl>) {
        if self.is_current(expected) {
            self.current = None;
        }
    }

    fn release_and_publish(&mut self, expected: &Arc<OperationControl>, phase: Phase) {
        if !self.is_current(expected) {
            return;
        }
        self.current = None;
        if !self.draining.load(Ordering::Acquire) {
            self.publish(phase);
        }
    }

    fn is_current(&self, expected: &Arc<OperationControl>) -> bool {
        self.current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    }

    fn publish(&mut self, phase: Phase) {
        self.phase = phase;
        self.revision = self.revision.saturating_add(1);
        self.snapshots.send_replace(self.snapshot());
    }

    fn accepted(&self, operation: Option<&OperationControl>) -> Accepted {
        Accepted {
            boot_epoch: self.boot_epoch.clone(),
            task_id: operation
                .map_or(0, |operation| operation.task_id)
                .to_string(),
            generation: operation
                .map_or(0, |operation| operation.generation)
                .to_string(),
            state_revision: self.revision.to_string(),
        }
    }
}

impl Phase {
    fn to_wire(&self) -> RuntimePhase {
        match self {
            Self::Unloaded => RuntimePhase::Unloaded,
            Self::Draining => RuntimePhase::Draining,
            Self::RecoveryRequired(reason) => RuntimePhase::RecoveryRequired {
                reason: reason.clone(),
            },
            Self::Operation { control, phase } => {
                let task_id = control.task_id.to_string();
                let generation = control.generation.to_string();
                let model_id = control.model_id.clone();
                match phase {
                    OperationPhase::Starting => RuntimePhase::Starting {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::Ready { engine_pid } => RuntimePhase::Ready {
                        task_id,
                        generation,
                        model_id,
                        engine_pid: *engine_pid,
                    },
                    OperationPhase::Stopping => RuntimePhase::Stopping {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::CleanupFailed => RuntimePhase::CleanupFailed {
                        task_id,
                        generation,
                        model_id,
                    },
                    OperationPhase::LoadFailed(category) => RuntimePhase::LoadFailed {
                        task_id,
                        generation,
                        model_id,
                        category: *category,
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
