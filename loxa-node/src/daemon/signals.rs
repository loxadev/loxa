use loxa_core::supervisor::InterruptStatus;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) trait InterruptSource {
    fn interrupted(&self) -> bool;
}

static CTRL_C_RECEIVED: AtomicBool = AtomicBool::new(false);

fn clear_ctrl_c_received() {
    CTRL_C_RECEIVED.store(false, Ordering::SeqCst);
}

fn set_ctrl_c_received() {
    CTRL_C_RECEIVED.store(true, Ordering::SeqCst);
}

fn ctrl_c_received() -> bool {
    CTRL_C_RECEIVED.load(Ordering::SeqCst)
}

#[cfg(unix)]
extern "C" fn handle_sigint(_signal: std::ffi::c_int) {
    set_ctrl_c_received();
}

trait SignalRegistration {
    type Installed;

    fn install(&self) -> io::Result<Self::Installed>;
    fn restore(&self, installed: &mut Self::Installed);
}

struct RegistrationGuard<R: SignalRegistration> {
    registration: R,
    installed: R::Installed,
}

impl<R: SignalRegistration> RegistrationGuard<R> {
    fn install(registration: R) -> io::Result<Self> {
        clear_ctrl_c_received();
        let installed = registration.install()?;
        Ok(Self {
            registration,
            installed,
        })
    }

    fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

impl<R: SignalRegistration> InterruptSource for RegistrationGuard<R> {
    fn interrupted(&self) -> bool {
        RegistrationGuard::interrupted(self)
    }
}

impl<R: SignalRegistration> InterruptStatus for RegistrationGuard<R> {
    fn interrupted(&self) -> bool {
        RegistrationGuard::interrupted(self)
    }
}

impl<R: SignalRegistration> Drop for RegistrationGuard<R> {
    fn drop(&mut self) {
        self.registration.restore(&mut self.installed);
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct UnixSignalRegistration;

#[cfg(unix)]
impl SignalRegistration for UnixSignalRegistration {
    type Installed = usize;

    fn install(&self) -> io::Result<Self::Installed> {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        const SIG_ERR: usize = usize::MAX;
        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }
        let previous = unsafe { signal(SIGINT, handle_sigint as *const () as usize) };
        if previous == SIG_ERR {
            return Err(io::Error::last_os_error());
        }
        Ok(previous)
    }

    fn restore(&self, previous: &mut Self::Installed) {
        use std::ffi::c_int;
        const SIGINT: c_int = 2;
        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }
        let _ = unsafe { signal(SIGINT, *previous) };
    }
}

#[cfg(unix)]
pub(crate) struct SignalGuard(RegistrationGuard<UnixSignalRegistration>);

#[cfg(unix)]
impl SignalGuard {
    pub(crate) fn install() -> io::Result<Self> {
        RegistrationGuard::install(UnixSignalRegistration).map(Self)
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.0.interrupted()
    }
}

#[cfg(unix)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(unix)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
#[derive(Clone, Copy)]
struct WindowsSignalRegistration;

#[cfg(windows)]
impl SignalRegistration for WindowsSignalRegistration {
    type Installed = ();

    fn install(&self) -> io::Result<Self::Installed> {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const TRUE: Bool = 1;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        let registered = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), TRUE) };
        if registered == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    fn restore(&self, _installed: &mut Self::Installed) {
        type Bool = i32;
        type Dword = u32;
        type HandlerRoutine = Option<unsafe extern "system" fn(Dword) -> Bool>;

        const FALSE: Bool = 0;

        unsafe extern "system" {
            fn SetConsoleCtrlHandler(handler: HandlerRoutine, add: Bool) -> Bool;
        }

        let _ = unsafe { SetConsoleCtrlHandler(Some(handle_console_ctrl), FALSE) };
    }
}

#[cfg(windows)]
pub(crate) struct SignalGuard(RegistrationGuard<WindowsSignalRegistration>);

#[cfg(windows)]
impl SignalGuard {
    pub(crate) fn install() -> io::Result<Self> {
        RegistrationGuard::install(WindowsSignalRegistration).map(Self)
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.0.interrupted()
    }
}

#[cfg(windows)]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(windows)]
unsafe extern "system" fn handle_console_ctrl(control_type: u32) -> i32 {
    const CTRL_C_EVENT: u32 = 0;
    const CTRL_BREAK_EVENT: u32 = 1;

    match control_type {
        CTRL_C_EVENT | CTRL_BREAK_EVENT => {
            set_ctrl_c_received();
            1
        }
        _ => 0,
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) struct SignalGuard;

#[cfg(not(any(unix, windows)))]
impl SignalGuard {
    pub(crate) fn install() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Ctrl-C cleanup is unsupported on this platform",
        ))
    }

    pub(crate) fn interrupted(&self) -> bool {
        ctrl_c_received()
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptSource for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(not(any(unix, windows)))]
impl InterruptStatus for SignalGuard {
    fn interrupted(&self) -> bool {
        SignalGuard::interrupted(self)
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) static SIGNAL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    pub(crate) fn clear_ctrl_c_received() {
        super::clear_ctrl_c_received();
    }

    pub(crate) fn set_ctrl_c_received() {
        super::set_ctrl_c_received();
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::SIGNAL_TEST_LOCK;
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn ctrl_c_flag_helpers_round_trip() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
        set_ctrl_c_received();
        assert!(ctrl_c_received());
        clear_ctrl_c_received();
        assert!(!ctrl_c_received());
    }

    #[cfg(unix)]
    #[test]
    fn unix_signal_handler_records_interrupt_without_platform_state_leaking() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();
        handle_sigint(2);
        assert!(ctrl_c_received());
        clear_ctrl_c_received();
    }

    #[derive(Clone, Default)]
    struct FakeSignalRegistration {
        calls: Arc<Mutex<Vec<bool>>>,
    }

    impl SignalRegistration for FakeSignalRegistration {
        type Installed = ();

        fn install(&self) -> io::Result<Self::Installed> {
            self.calls.lock().unwrap().push(true);
            Ok(())
        }

        fn restore(&self, _installed: &mut Self::Installed) {
            self.calls.lock().unwrap().push(false);
        }
    }

    #[test]
    fn portable_signal_registration_guard_installs_and_restores_exactly_once() {
        let registration = FakeSignalRegistration::default();
        let calls = registration.calls.clone();
        let guard = RegistrationGuard::install(registration).expect("install fake registration");
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        drop(guard);
        assert_eq!(*calls.lock().unwrap(), vec![true, false]);
    }

    #[cfg(unix)]
    #[test]
    fn signal_guard_can_install_drop_and_restore_repeatedly() {
        let _lock = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        clear_ctrl_c_received();

        let first = SignalGuard::install().expect("install first signal guard");
        assert!(!first.interrupted());
        drop(first);

        let second = SignalGuard::install().expect("restore then install second signal guard");
        assert!(!second.interrupted());
        drop(second);
        clear_ctrl_c_received();
    }
}
