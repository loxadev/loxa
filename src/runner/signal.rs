use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
#[cfg(unix)]
use std::sync::OnceLock;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
static PROCESS_SIGNAL_WATCHER: OnceLock<Result<(), String>> = OnceLock::new();

#[cfg(unix)]
pub(super) static PROCESS_TERMINATION_SIGNAL: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
pub(super) static ACTIVE_SERVER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
const STARTING_SERVER: u64 = u64::MAX;

#[cfg(unix)]
pub(super) fn install_termination_watcher(run_dir: &Path) -> Result<(), String> {
    let run_dir = run_dir.to_path_buf();
    PROCESS_SIGNAL_WATCHER
        .get_or_init(move || {
            let mut termination =
                signal_hook::iterator::Signals::new([libc::SIGINT, libc::SIGTERM, libc::SIGHUP])
                    .map_err(|error| error.to_string())?;
            std::thread::Builder::new()
                .name("loxa-signal".into())
                .spawn(move || {
                    if let Some(signal) = termination.forever().next() {
                        PROCESS_TERMINATION_SIGNAL.store(signal, Ordering::SeqCst);
                        let mut active = ACTIVE_SERVER.load(Ordering::SeqCst);
                        while active == STARTING_SERVER {
                            std::thread::sleep(Duration::from_millis(10));
                            active = ACTIVE_SERVER.load(Ordering::SeqCst);
                        }
                        if let Some((pid, group)) = unpack_server_identity(active) {
                            while crate::runtime::terminate_stale_process_group(group).is_err() {
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            if crate::runtime::clear_terminated_owned_lease(&run_dir, pid, group)
                                .is_err()
                            {
                                tracing::warn!(
                                    target: "loxa::runner",
                                    event = "signal_runtime_lease_cleanup_failed"
                                );
                            }
                        }
                        std::process::exit(128 + signal);
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(())
        })
        .as_ref()
        .map_err(Clone::clone)
        .copied()
}

#[cfg(unix)]
pub(super) fn process_termination_signal() -> Option<i32> {
    let signal = PROCESS_TERMINATION_SIGNAL.load(Ordering::SeqCst);
    (signal != 0).then_some(signal)
}

#[cfg(test)]
#[cfg(unix)]
pub(super) struct ProcessTerminationSignalReset;

#[cfg(test)]
#[cfg(unix)]
impl Drop for ProcessTerminationSignalReset {
    fn drop(&mut self) {
        PROCESS_TERMINATION_SIGNAL.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[cfg(unix)]
pub(super) fn reset_process_termination_signal_for_test() -> ProcessTerminationSignalReset {
    PROCESS_TERMINATION_SIGNAL.store(0, Ordering::SeqCst);
    ProcessTerminationSignalReset
}

#[cfg(not(unix))]
pub(super) fn process_termination_signal() -> Option<i32> {
    None
}

#[cfg(unix)]
pub(super) fn activate_server(pid: u32, group: i32) {
    ACTIVE_SERVER.store(pack_server_identity(pid, group), Ordering::SeqCst);
}

#[cfg(unix)]
pub(super) fn mark_server_starting() {
    ACTIVE_SERVER.store(STARTING_SERVER, Ordering::SeqCst);
}

#[cfg(unix)]
pub(super) fn clear_server_starting() {
    let _ = ACTIVE_SERVER.compare_exchange(STARTING_SERVER, 0, Ordering::SeqCst, Ordering::SeqCst);
}

#[cfg(unix)]
pub(super) fn deactivate_server(pid: u32, group: i32) {
    let _ = ACTIVE_SERVER.compare_exchange(
        pack_server_identity(pid, group),
        0,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
}

#[cfg(unix)]
pub(super) fn pack_server_identity(pid: u32, group: i32) -> u64 {
    debug_assert!(group > 1);
    (u64::from(pid) << 32) | u64::from(u32::try_from(group).expect("positive process group"))
}

#[cfg(unix)]
pub(super) fn unpack_server_identity(identity: u64) -> Option<(u32, i32)> {
    let pid = u32::try_from(identity >> 32).ok()?;
    let group = i32::try_from(identity as u32).ok()?;
    (pid != 0 && group > 1).then_some((pid, group))
}

#[cfg(not(unix))]
pub(super) fn activate_server(_pid: u32, _group: i32) {}

#[cfg(not(unix))]
pub(super) fn deactivate_server(_pid: u32, _group: i32) {}

#[cfg(not(unix))]
pub(super) fn mark_server_starting() {}

#[cfg(not(unix))]
pub(super) fn clear_server_starting() {}

#[cfg(not(unix))]
pub(super) fn install_termination_watcher(_run_dir: &Path) -> Result<(), String> {
    Ok(())
}
