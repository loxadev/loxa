use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use super::{Coordinator, OwnerExit};
use crate::config::SettingsExit;
use crate::history::HistoryExit;

const NATIVE_GATE_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) struct NativeTestGate {
    reached: Semaphore,
    release: Semaphore,
}

pub(super) struct NativeTestGateControl {
    gate: Arc<NativeTestGate>,
    released: bool,
}

impl NativeTestGate {
    pub(super) fn pair() -> (Arc<Self>, NativeTestGateControl) {
        let gate = Arc::new(Self {
            reached: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let control = NativeTestGateControl {
            gate: Arc::clone(&gate),
            released: false,
        };
        (gate, control)
    }

    async fn pause(&self) {
        self.reached.add_permits(1);
        if let Ok(permit) = self.release.acquire().await {
            permit.forget();
        }
    }
}

impl NativeTestGateControl {
    pub(super) async fn wait_reached(&self) -> Result<(), String> {
        let permit = tokio::time::timeout(NATIVE_GATE_TIMEOUT, self.gate.reached.acquire())
            .await
            .map_err(|_| "native generation gate was not reached".to_string())?
            .map_err(|_| "native generation gate closed before it was reached".to_string())?;
        permit.forget();
        Ok(())
    }

    pub(super) fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.gate.release.add_permits(1);
        }
    }
}

impl Drop for NativeTestGateControl {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) async fn pause_once(slot: &std::sync::Mutex<Option<Arc<NativeTestGate>>>) {
    let gate = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        gate.pause().await;
    }
}

impl Coordinator {
    pub(super) fn pause_native_admission_completion(&self) -> NativeTestGateControl {
        let (gate, control) = NativeTestGate::pair();
        *self
            .shared
            .native_admission_completion_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
        control
    }

    pub(super) fn pause_native_generation_execution(&self) -> NativeTestGateControl {
        let (gate, control) = NativeTestGate::pair();
        *self
            .shared
            .native_generation_execution_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
        control
    }

    pub(super) async fn wait_at_native_generation_execution_gate(&self) {
        pause_once(&self.shared.native_generation_execution_gate).await;
    }

    pub(super) async fn wait_for_native_shutdown_resolution(&self) -> Result<(), String> {
        let mut owner = self.owner_exit_receiver();
        let mut history = self.history_exit_receiver();
        let mut settings = self.settings_exit_receiver();
        loop {
            self.release_runtime_if_durable();
            let owner_resolved = matches!(*owner.borrow(), OwnerExit::Drained | OwnerExit::Failed);
            let history_state = *history.borrow();
            let settings_state = *settings.borrow();
            if history_state == HistoryExit::Failed {
                return Err("native generation history owner failed during shutdown".into());
            }
            if settings_state == SettingsExit::FlushFailed {
                return Err("native generation settings owner failed during shutdown".into());
            }
            if settings_state == SettingsExit::WriteCompleted {
                self.finish_settings_write()
                    .map_err(|error| error.context)?;
                continue;
            }
            if owner_resolved
                && history_state == HistoryExit::Drained
                && settings_state == SettingsExit::Drained
            {
                return Ok(());
            }
            tokio::select! {
                changed = owner.changed() => {
                    changed.map_err(|_| "native generation runtime owner exit closed".to_string())?;
                }
                changed = history.changed() => {
                    changed.map_err(|_| "native generation history exit closed".to_string())?;
                }
                changed = settings.changed() => {
                    changed.map_err(|_| "native generation settings exit closed".to_string())?;
                }
            }
        }
    }
}
