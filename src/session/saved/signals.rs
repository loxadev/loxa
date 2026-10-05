use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Clone, Copy)]
pub(super) struct TerminalState(libc::termios);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Interrupt {
    Current,
    Stale,
}

enum Phase {
    Disarmed,
    Idle(TerminalState),
    Generation(u64),
    Interrupted(u64),
    Closed,
}

struct Shared {
    phase: Mutex<Phase>,
    notify: Notify,
}

pub(super) struct SessionSignals {
    shared: Arc<Shared>,
    handle: signal_hook::iterator::Handle,
    listener: Option<std::thread::JoinHandle<()>>,
    next_tag: u64,
}

impl SessionSignals {
    pub(super) fn install() -> Result<Self, String> {
        let mut signals = signal_hook::iterator::Signals::new([signal_hook::consts::SIGINT])
            .map_err(|error| format!("failed to install chat interrupt handler: {error}"))?;
        let handle = signals.handle();
        let shared = Arc::new(Shared {
            phase: Mutex::new(Phase::Disarmed),
            notify: Notify::new(),
        });
        let listener_shared = shared.clone();
        let listener = std::thread::spawn(move || {
            for _ in signals.forever() {
                let mut phase = listener_shared
                    .phase
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*phase {
                    Phase::Generation(tag) => {
                        *phase = Phase::Interrupted(*tag);
                        drop(phase);
                        listener_shared.notify.notify_waiters();
                    }
                    Phase::Idle(terminal) => {
                        restore_terminal(*terminal);
                        unsafe { libc::_exit(130) }
                    }
                    Phase::Disarmed => unsafe { libc::_exit(130) },
                    Phase::Closed => return,
                    Phase::Interrupted(_) => continue,
                }
            }
        });
        Ok(Self {
            shared,
            handle,
            listener: Some(listener),
            next_tag: 0,
        })
    }

    pub(super) fn enter_idle(&self) -> Result<TerminalState, String> {
        let terminal = capture_terminal()?;
        let mut phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Interrupted(_)) {
            drop(phase);
            self.exit_now(Some(terminal));
        }
        *phase = Phase::Idle(terminal);
        Ok(terminal)
    }

    pub(super) fn leave_idle(&self) {
        let mut phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Idle(_)) {
            *phase = Phase::Disarmed;
        }
    }

    pub(super) fn begin_generation(&mut self) -> Result<u64, Interrupt> {
        self.next_tag = self.next_tag.checked_add(1).ok_or(Interrupt::Stale)?;
        let tag = self.next_tag;
        let mut phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Interrupted(_)) {
            return Err(Interrupt::Stale);
        }
        *phase = Phase::Generation(tag);
        Ok(tag)
    }

    pub(super) fn end_generation(&self, tag: u64) {
        let mut phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*phase, Phase::Generation(current) if current == tag) {
            *phase = Phase::Disarmed;
        }
    }

    pub(super) async fn interrupted(&self, tag: u64) -> Interrupt {
        loop {
            let notified = self.shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(interrupt) = self.interrupted_now(tag) {
                return interrupt;
            }
            notified.await;
        }
    }

    pub(super) fn was_interrupted(&self) -> bool {
        let phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(*phase, Phase::Interrupted(_))
    }

    pub(super) fn interrupted_now(&self, tag: u64) -> Option<Interrupt> {
        let phase = self
            .shared
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *phase {
            Phase::Interrupted(current) if current == tag => Some(Interrupt::Current),
            Phase::Interrupted(_) => Some(Interrupt::Stale),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn interrupt_current_for_test(&self) {
        let mut phase = self.shared.phase.lock().unwrap();
        let Phase::Generation(tag) = *phase else {
            panic!("no active generation")
        };
        *phase = Phase::Interrupted(tag);
        drop(phase);
        self.shared.notify.notify_waiters();
    }

    pub(super) fn exit_now(&self, terminal: Option<TerminalState>) -> ! {
        self.exit_with_code(130, terminal)
    }

    pub(super) fn exit_with_code(&self, code: i32, terminal: Option<TerminalState>) -> ! {
        if let Some(terminal) = terminal {
            restore_terminal(terminal);
        }
        unsafe { libc::_exit(code) }
    }

    pub(super) fn close_and_join(&mut self) {
        {
            let mut phase = self
                .shared
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *phase = Phase::Closed;
        }
        self.handle.close();
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
    }
}

impl Drop for SessionSignals {
    fn drop(&mut self) {
        self.close_and_join();
    }
}

fn capture_terminal() -> Result<TerminalState, String> {
    let mut terminal = std::mem::MaybeUninit::<libc::termios>::uninit();
    if unsafe { libc::tcgetattr(libc::STDIN_FILENO, terminal.as_mut_ptr()) } == -1 {
        return Err("failed to capture terminal state before chat input".into());
    }
    Ok(TerminalState(unsafe { terminal.assume_init() }))
}

fn restore_terminal(terminal: TerminalState) {
    let _ = unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &terminal.0) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_generation_notification_cannot_become_current() {
        let mut signals = SessionSignals::install().unwrap();
        let first = signals.begin_generation().unwrap();
        signals.end_generation(first);
        let successor = signals.begin_generation().unwrap();
        *signals.shared.phase.lock().unwrap() = Phase::Interrupted(first);
        assert_eq!(signals.interrupted_now(successor), Some(Interrupt::Stale));
        signals.end_generation(successor);
    }
}
