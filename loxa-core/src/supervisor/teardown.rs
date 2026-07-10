use super::{BoundedLogWriter, SupervisorError};
#[cfg(unix)]
use std::ffi::c_int;
use std::io::{self, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OwnedProcessGroup {
    pgid: c_int,
    negative_pgid: c_int,
}

#[cfg(unix)]
impl OwnedProcessGroup {
    fn try_from_raw(raw: i64) -> io::Result<Self> {
        let pgid = c_int::try_from(raw).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process group {raw} is not representable as c_int"),
            )
        })?;
        if pgid <= 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process group {pgid} is not an owned process group"),
            ));
        }
        let negative_pgid = pgid.checked_neg().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("process group {pgid} cannot be safely negated"),
            )
        })?;
        Ok(Self {
            pgid,
            negative_pgid,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TeardownConfirmation {
    Confirmed,
    Unconfirmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ChildTeardownResult {
    pub(super) confirmation: TeardownConfirmation,
    pub(super) forced: bool,
}

#[derive(Clone, Copy)]
struct TeardownTiming {
    phase_one: Duration,
    phase_two: Duration,
    drains: Duration,
    interval: Duration,
}

impl TeardownTiming {
    fn production() -> Self {
        Self {
            phase_one: Duration::from_secs(5),
            phase_two: Duration::from_secs(5),
            drains: Duration::from_secs(5),
            interval: Duration::from_millis(50),
        }
    }

    #[cfg(test)]
    fn test() -> Self {
        Self {
            phase_one: Duration::from_secs(5),
            phase_two: Duration::from_secs(5),
            drains: Duration::from_secs(5),
            interval: Duration::from_secs(1),
        }
    }
}

pub fn teardown_managed_child<C>(
    child: &mut C,
    _grace_period: Duration,
) -> Result<TeardownConfirmation, SupervisorError>
where
    C: ManagedChild + LogDrainingChild,
{
    Ok(teardown_managed_child_result(child).confirmation)
}

pub(super) fn teardown_managed_child_result<C>(child: &mut C) -> ChildTeardownResult
where
    C: ManagedChild + LogDrainingChild,
{
    let started = std::time::Instant::now();
    #[cfg(unix)]
    {
        teardown_managed_child_with(
            child,
            &mut UnixProcessGroupControl,
            TeardownTiming::production(),
            || started.elapsed(),
            thread::sleep,
        )
    }
    #[cfg(not(unix))]
    {
        teardown_direct_child_with(
            child,
            TeardownTiming::production(),
            || started.elapsed(),
            thread::sleep,
        )
    }
}

#[cfg(any(not(unix), test))]
fn teardown_direct_child_with<C, N, S>(
    child: &mut C,
    timing: TeardownTiming,
    mut now: N,
    mut sleep: S,
) -> ChildTeardownResult
where
    C: ManagedChild + LogDrainingChild,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let _ = child.terminate();
    let mut leader_reaped = false;
    let mut reap_error = false;
    let phase_one = run_direct_child_phase(
        child,
        &mut leader_reaped,
        &mut reap_error,
        timing.phase_one,
        timing.interval,
        &mut now,
        &mut sleep,
    );
    if leader_reaped {
        return ChildTeardownResult {
            confirmation: if phase_one == PhaseResult::Complete
                && !reap_error
                && finish_drains_with_deadline(
                    child,
                    timing.drains,
                    timing.interval,
                    &mut now,
                    &mut sleep,
                ) {
                TeardownConfirmation::Confirmed
            } else {
                TeardownConfirmation::Unconfirmed
            },
            forced: false,
        };
    }

    let force_error = child.kill().is_err();
    let phase_two = run_direct_child_phase(
        child,
        &mut leader_reaped,
        &mut reap_error,
        timing.phase_two,
        timing.interval,
        &mut now,
        &mut sleep,
    );
    let physical_confirmed =
        phase_two == PhaseResult::Complete && leader_reaped && !force_error && !reap_error;
    let drains_confirmed = physical_confirmed
        && finish_drains_with_deadline(child, timing.drains, timing.interval, &mut now, &mut sleep);
    ChildTeardownResult {
        confirmation: if drains_confirmed {
            TeardownConfirmation::Confirmed
        } else {
            TeardownConfirmation::Unconfirmed
        },
        forced: true,
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GroupSignal {
    Term,
    Kill,
    Probe,
}

#[cfg(unix)]
trait ProcessGroupControl {
    fn signal_group(&mut self, group: OwnedProcessGroup, signal: GroupSignal) -> io::Result<()>;
}

#[cfg(unix)]
struct UnixProcessGroupControl;

#[cfg(unix)]
impl ProcessGroupControl for UnixProcessGroupControl {
    fn signal_group(&mut self, group: OwnedProcessGroup, signal: GroupSignal) -> io::Result<()> {
        unsafe extern "C" {
            fn kill(pid: c_int, signal: c_int) -> c_int;
        }

        let signal = match signal {
            GroupSignal::Term => 15,
            GroupSignal::Kill => 9,
            GroupSignal::Probe => 0,
        };
        if unsafe { kill(group.negative_pgid, signal) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct GroupState {
    group: OwnedProcessGroup,
    absent: bool,
    observation_error: bool,
}

#[cfg(unix)]
impl GroupState {
    fn new(group: OwnedProcessGroup) -> Self {
        Self {
            group,
            absent: false,
            observation_error: false,
        }
    }

    fn observe<G: ProcessGroupControl>(&mut self, control: &mut G, signal: GroupSignal) {
        if self.absent {
            return;
        }

        match control.signal_group(self.group, signal) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(3) => self.absent = true,
            Err(error) if error.raw_os_error() == Some(1) => {}
            Err(_) => self.observation_error = true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PhaseResult {
    Complete,
    Deadline,
    ClockStalled,
}

#[cfg(unix)]
fn teardown_managed_child_with<C, G, N, S>(
    child: &mut C,
    groups: &mut G,
    timing: TeardownTiming,
    mut now: N,
    mut sleep: S,
) -> ChildTeardownResult
where
    C: ManagedChild + LogDrainingChild,
    G: ProcessGroupControl,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let Some(raw_pgid) = child.owned_pgid() else {
        return teardown_missing_unix_group(child, timing, &mut now, &mut sleep);
    };
    let Ok(group) = OwnedProcessGroup::try_from_raw(i64::from(raw_pgid)) else {
        return teardown_missing_unix_group(child, timing, &mut now, &mut sleep);
    };

    let mut group = GroupState::new(group);
    let mut leader_reaped = false;
    let mut reap_error = false;
    group.observe(groups, GroupSignal::Term);

    let phase_one = run_unix_process_phase(
        child,
        groups,
        &mut group,
        &mut leader_reaped,
        &mut reap_error,
        timing.phase_one,
        timing.interval,
        &mut now,
        &mut sleep,
    );
    if phase_one == PhaseResult::Complete && !group.observation_error && !reap_error {
        let drains_confirmed = finish_drains_with_deadline(
            child,
            timing.drains,
            timing.interval,
            &mut now,
            &mut sleep,
        );
        return ChildTeardownResult {
            confirmation: if drains_confirmed {
                TeardownConfirmation::Confirmed
            } else {
                TeardownConfirmation::Unconfirmed
            },
            forced: false,
        };
    }

    let mut forced = false;
    if !group.absent {
        forced = true;
        group.observe(groups, GroupSignal::Kill);
    }
    let phase_two = run_unix_process_phase(
        child,
        groups,
        &mut group,
        &mut leader_reaped,
        &mut reap_error,
        timing.phase_two,
        timing.interval,
        &mut now,
        &mut sleep,
    );

    let physical_confirmed = phase_two == PhaseResult::Complete
        && leader_reaped
        && group.absent
        && !group.observation_error
        && !reap_error;
    let drains_confirmed = physical_confirmed
        && finish_drains_with_deadline(child, timing.drains, timing.interval, &mut now, &mut sleep);
    ChildTeardownResult {
        confirmation: if drains_confirmed {
            TeardownConfirmation::Confirmed
        } else {
            TeardownConfirmation::Unconfirmed
        },
        forced,
    }
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
fn run_unix_process_phase<C, G, N, S>(
    child: &mut C,
    groups: &mut G,
    group: &mut GroupState,
    leader_reaped: &mut bool,
    reap_error: &mut bool,
    timeout: Duration,
    interval: Duration,
    now: &mut N,
    sleep: &mut S,
) -> PhaseResult
where
    C: ManagedChild,
    G: ProcessGroupControl,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let started = now();
    loop {
        if !*leader_reaped {
            match child.try_wait() {
                Ok(Some(_)) => *leader_reaped = true,
                Ok(None) => {}
                Err(_) => *reap_error = true,
            }
        }
        group.observe(groups, GroupSignal::Probe);
        if *leader_reaped && group.absent {
            return PhaseResult::Complete;
        }

        let current = now();
        let elapsed = current.saturating_sub(started);
        if elapsed >= timeout {
            return PhaseResult::Deadline;
        }
        if interval.is_zero() {
            return PhaseResult::ClockStalled;
        }
        let duration = interval.min(timeout - elapsed);
        sleep(duration);
        if now() <= current {
            return PhaseResult::ClockStalled;
        }
    }
}

#[cfg(unix)]
fn teardown_missing_unix_group<C, N, S>(
    child: &mut C,
    timing: TeardownTiming,
    now: &mut N,
    sleep: &mut S,
) -> ChildTeardownResult
where
    C: ManagedChild,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let _ = child.terminate();
    let mut leader_reaped = false;
    let mut reap_error = false;
    let _ = run_direct_child_phase(
        child,
        &mut leader_reaped,
        &mut reap_error,
        timing.phase_one,
        timing.interval,
        now,
        sleep,
    );
    let mut forced = false;
    if !leader_reaped {
        forced = true;
        let _ = child.kill();
        let _ = run_direct_child_phase(
            child,
            &mut leader_reaped,
            &mut reap_error,
            timing.phase_two,
            timing.interval,
            now,
            sleep,
        );
    }
    ChildTeardownResult {
        confirmation: TeardownConfirmation::Unconfirmed,
        forced,
    }
}

fn run_direct_child_phase<C, N, S>(
    child: &mut C,
    leader_reaped: &mut bool,
    reap_error: &mut bool,
    timeout: Duration,
    interval: Duration,
    now: &mut N,
    sleep: &mut S,
) -> PhaseResult
where
    C: ManagedChild,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let started = now();
    loop {
        if !*leader_reaped {
            match child.try_wait() {
                Ok(Some(_)) => *leader_reaped = true,
                Ok(None) => {}
                Err(_) => *reap_error = true,
            }
        }
        if *leader_reaped {
            return PhaseResult::Complete;
        }
        let current = now();
        let elapsed = current.saturating_sub(started);
        if elapsed >= timeout {
            return PhaseResult::Deadline;
        }
        if interval.is_zero() {
            return PhaseResult::ClockStalled;
        }
        let duration = interval.min(timeout - elapsed);
        sleep(duration);
        if now() <= current {
            return PhaseResult::ClockStalled;
        }
    }
}

fn finish_drains_with_deadline<C, N, S>(
    child: &mut C,
    timeout: Duration,
    interval: Duration,
    now: &mut N,
    sleep: &mut S,
) -> bool
where
    C: LogDrainingChild,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    let started = now();
    loop {
        if child.log_drains_finished() {
            return child.join_log_drains().is_ok();
        }
        let current = now();
        let elapsed = current.saturating_sub(started);
        if elapsed >= timeout || interval.is_zero() {
            return false;
        }
        let duration = interval.min(timeout - elapsed);
        sleep(duration);
        if now() <= current {
            return false;
        }
    }
}

pub trait ManagedChild {
    fn pid(&self) -> u32;

    fn owned_pgid(&self) -> Option<i32> {
        None
    }

    fn terminate(&mut self) -> io::Result<()>;
    fn kill(&mut self) -> io::Result<()>;
    fn try_wait(&mut self) -> io::Result<Option<i32>>;
}

pub trait LogDrainingChild {
    fn log_drains_finished(&self) -> bool {
        true
    }

    fn join_log_drains(&mut self) -> Result<(), SupervisorError>;
}

pub struct SpawnedServer {
    child: Child,
    #[cfg(unix)]
    owned_process_group: Option<OwnedProcessGroup>,
    reaped_status: Option<i32>,
    log_drains: Vec<JoinHandle<io::Result<()>>>,
    initialization_error: Option<SupervisorError>,
}

impl SpawnedServer {
    fn from_spawned_child(child: Child) -> Self {
        #[cfg(unix)]
        let (owned_process_group, initialization_error) =
            match OwnedProcessGroup::try_from_raw(i64::from(child.id())) {
                Ok(group) => (Some(group), None),
                Err(error) => (None, Some(SupervisorError::Io(error))),
            };
        #[cfg(not(unix))]
        let initialization_error = None;

        Self {
            child,
            #[cfg(unix)]
            owned_process_group,
            reaped_status: None,
            log_drains: Vec::new(),
            initialization_error,
        }
    }

    fn record_initialization_error(&mut self, error: impl Into<SupervisorError>) {
        if self.initialization_error.is_none() {
            self.initialization_error = Some(error.into());
        }
    }

    fn initialize_log_drains_with<F>(&mut self, writer: Arc<Mutex<BoundedLogWriter>>, mut spawn: F)
    where
        F: FnMut(
            Box<dyn Read + Send>,
            Arc<Mutex<BoundedLogWriter>>,
        ) -> io::Result<JoinHandle<io::Result<()>>>,
    {
        match self.child.stdout.take() {
            Some(stdout) => match spawn(Box::new(stdout), Arc::clone(&writer)) {
                Ok(drain) => self.log_drains.push(drain),
                Err(error) => self.record_initialization_error(error),
            },
            None => self.record_initialization_error(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "managed child stdout pipe is missing after spawn",
            )),
        }
        match self.child.stderr.take() {
            Some(stderr) => match spawn(Box::new(stderr), writer) {
                Ok(drain) => self.log_drains.push(drain),
                Err(error) => self.record_initialization_error(error),
            },
            None => self.record_initialization_error(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "managed child stderr pipe is missing after spawn",
            )),
        }
    }

    pub fn take_initialization_error(&mut self) -> Option<SupervisorError> {
        self.initialization_error.take()
    }

    pub fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
        if !self.log_drains.iter().all(JoinHandle::is_finished) {
            return Err(SupervisorError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "log drains have not finished",
            )));
        }

        for drain in std::mem::take(&mut self.log_drains) {
            let result = drain
                .join()
                .map_err(|_| SupervisorError::Io(io::Error::other("log drain thread panicked")))?;
            result.map_err(SupervisorError::Io)?;
        }
        Ok(())
    }
}

impl ManagedChild for Child {
    fn pid(&self) -> u32 {
        self.id()
    }

    fn terminate(&mut self) -> io::Result<()> {
        super::signal_pid(self.id(), sysinfo::Signal::Term)
    }

    fn kill(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        {
            if super::signal_pid(self.id(), sysinfo::Signal::Kill).is_err() {
                return Child::kill(self);
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Child::kill(self)
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        Ok(self
            .try_wait()?
            .map(|status| status.code().unwrap_or_default()))
    }
}

impl ManagedChild for SpawnedServer {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn owned_pgid(&self) -> Option<i32> {
        #[cfg(unix)]
        {
            self.owned_process_group.map(|group| group.pgid)
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    fn terminate(&mut self) -> io::Result<()> {
        super::signal_pid(self.child.id(), sysinfo::Signal::Term)
    }

    fn kill(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        {
            if super::signal_pid(self.child.id(), sysinfo::Signal::Kill).is_err() {
                return Child::kill(&mut self.child);
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Child::kill(&mut self.child)
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        if let Some(status) = self.reaped_status {
            return Ok(Some(status));
        }
        let status = self
            .child
            .try_wait()?
            .map(|status| status.code().unwrap_or_default());
        if let Some(status) = status {
            self.reaped_status = Some(status);
        }
        Ok(status)
    }
}

impl LogDrainingChild for SpawnedServer {
    fn log_drains_finished(&self) -> bool {
        self.log_drains.iter().all(JoinHandle::is_finished)
    }

    fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
        SpawnedServer::join_log_drains(self)
    }
}

pub(super) fn spawn_managed_command(
    command: Command,
    writer: Arc<Mutex<BoundedLogWriter>>,
) -> Result<SpawnedServer, SupervisorError> {
    spawn_managed_command_with(command, writer, |reader, writer| {
        spawn_log_drain(reader, writer)
    })
}

fn spawn_managed_command_with<F>(
    mut command: Command,
    writer: Arc<Mutex<BoundedLogWriter>>,
    spawn: F,
) -> Result<SpawnedServer, SupervisorError>
where
    F: FnMut(
        Box<dyn Read + Send>,
        Arc<Mutex<BoundedLogWriter>>,
    ) -> io::Result<JoinHandle<io::Result<()>>>,
{
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);

    let child = command.spawn()?;
    let mut spawned = SpawnedServer::from_spawned_child(child);
    spawned.initialize_log_drains_with(writer, spawn);
    Ok(spawned)
}

pub(super) fn spawn_log_drain<R>(
    mut reader: R,
    writer: Arc<Mutex<BoundedLogWriter>>,
) -> io::Result<JoinHandle<io::Result<()>>>
where
    R: Read + Send + 'static,
{
    thread::Builder::new()
        .name("loxa-log-drain".to_string())
        .spawn(move || {
            let mut buffer = [0_u8; 8 * 1024];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    return Ok(());
                }

                let mut writer = writer
                    .lock()
                    .map_err(|_| io::Error::other("bounded log writer lock poisoned"))?;
                writer.write_chunk(&buffer[..read])?;
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::state::write_runtime_state;
    use crate::supervisor::{BoundedLogWriter, ManagedChild, TeardownConfirmation, MAX_LOG_BYTES};
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    #[cfg(target_os = "linux")]
    use std::fs;
    use std::fs::File;
    use std::io::Cursor;
    #[cfg(target_os = "linux")]
    use std::path::{Path, PathBuf};
    #[cfg(target_os = "linux")]
    use std::process::{Child, ExitStatus};
    use std::process::{Command, Stdio};
    #[cfg(target_os = "linux")]
    use std::rc::Rc;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Instant;
    #[cfg(target_os = "linux")]
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn owned_process_group_accepts_only_checked_positive_groups_above_one() {
        assert!(OwnedProcessGroup::try_from_raw(2).is_ok());
        assert!(OwnedProcessGroup::try_from_raw(i64::from(i32::MAX)).is_ok());

        for invalid in [i64::MIN, -2, -1, 0, 1, i64::from(i32::MAX) + 1] {
            assert!(
                OwnedProcessGroup::try_from_raw(invalid).is_err(),
                "invalid PGID {invalid} must never reach process-group FFI"
            );
        }
    }

    #[test]
    fn fake_managed_children_default_to_no_owned_process_group() {
        struct FakeChild;

        impl ManagedChild for FakeChild {
            fn pid(&self) -> u32 {
                7
            }

            fn terminate(&mut self) -> std::io::Result<()> {
                Ok(())
            }

            fn kill(&mut self) -> std::io::Result<()> {
                Ok(())
            }

            fn try_wait(&mut self) -> std::io::Result<Option<i32>> {
                Ok(Some(0))
            }
        }

        assert_eq!(FakeChild.owned_pgid(), None);
    }

    #[cfg(unix)]
    #[test]
    fn managed_spawn_creates_and_retains_the_child_owned_process_group() {
        unsafe extern "C" {
            fn getpgid(pid: std::ffi::c_int) -> std::ffi::c_int;
            fn kill(pid: std::ffi::c_int, signal: std::ffi::c_int) -> std::ffi::c_int;
        }

        let temp = tempdir().expect("tempdir");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("spawn.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let mut command = Command::new("/bin/sleep");
        command.arg("30");

        let mut spawned = spawn_managed_command(command, writer).expect("spawn managed child");
        let pid = spawned.pid();
        let pgid = spawned.owned_pgid().expect("owned process group");
        let pid = std::ffi::c_int::try_from(pid).expect("test pid fits c_int");

        assert!(pgid > 1);
        assert_eq!(pgid, pid);
        assert_eq!(unsafe { getpgid(pid) }, pgid);

        let group = OwnedProcessGroup::try_from_raw(i64::from(pgid)).expect("validated group");
        assert_eq!(unsafe { kill(group.negative_pgid, 9) }, 0);
        spawned.child.wait().expect("reap test child");
    }

    #[cfg(unix)]
    #[test]
    fn post_spawn_initialization_failure_retains_live_child_for_one_unified_teardown() {
        let temp = tempdir().expect("tempdir");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("initialization-error.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let drain_spawn_calls = Cell::new(0_u8);
        let mut spawned = spawn_managed_command_with(command, writer, |_, _| {
            drain_spawn_calls.set(drain_spawn_calls.get() + 1);
            Err(io::Error::other(
                "injected log-drain thread creation failure",
            ))
        })
        .expect("OS spawn still returns the owned child");
        let pid = spawned.pid();

        let error = spawned
            .take_initialization_error()
            .expect("retained initialization failure");
        assert!(error.to_string().contains("thread creation failure"));
        assert_eq!(drain_spawn_calls.get(), 2);
        assert_eq!(spawned.pid(), pid, "live child ownership is retained");
        assert_eq!(
            spawned.owned_pgid(),
            Some(c_int::try_from(pid).expect("pid fits c_int"))
        );

        let calls = Cell::new(0_u8);
        struct CountingControl<'a> {
            calls: &'a Cell<u8>,
            inner: UnixProcessGroupControl,
        }
        impl ProcessGroupControl for CountingControl<'_> {
            fn signal_group(
                &mut self,
                group: OwnedProcessGroup,
                signal: GroupSignal,
            ) -> io::Result<()> {
                if signal == GroupSignal::Term {
                    self.calls.set(self.calls.get() + 1);
                }
                self.inner.signal_group(group, signal)
            }
        }
        let started = Instant::now();
        let result = teardown_managed_child_with(
            &mut spawned,
            &mut CountingControl {
                calls: &calls,
                inner: UnixProcessGroupControl,
            },
            TeardownTiming {
                phase_one: Duration::from_secs(2),
                phase_two: Duration::from_secs(2),
                drains: Duration::from_secs(2),
                interval: Duration::from_millis(10),
            },
            || started.elapsed(),
            thread::sleep,
        );

        assert_eq!(calls.get(), 1);
        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
    }

    #[cfg(unix)]
    #[test]
    fn term_only_teardown_confirms_group_absence_reap_then_drains_in_order() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(std::time::Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(!result.forced);
        assert_eq!(
            events.into_inner(),
            vec![
                "term:77",
                "reap",
                "probe:77",
                "drains_finished",
                "drains_join"
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn teardown_uses_only_live_handle_pgid_not_different_persisted_diagnostic_value() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let persisted = crate::supervisor::ManagedRun {
            schema_version: crate::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "run-pgid-diagnostic".to_string(),
            model_id: "gemma-3-4b-it-q4".to_string(),
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: crate::supervisor::RunLifecycle::Running,
            generation: 0,
            generation_alias: "loxa-run-pgid-diagnostic-g0".to_string(),
            port: 8080,
            log_path: temp.path().join("managed.log"),
            child_pid: Some(77),
            child_process_start_time_unix_s: Some(2),
            child_pgid: Some(999),
        };
        write_runtime_state(&state_path, std::slice::from_ref(&persisted))
            .expect("persist conflicting diagnostic PGID");
        let crate::supervisor::RuntimeStateRead::Loaded(runs) =
            crate::supervisor::read_runtime_state(&state_path).expect("read persisted PGID")
        else {
            panic!("expected loaded runtime state");
        };
        let persisted_diagnostic_pgid = runs[0]
            .child_pgid
            .expect("persisted diagnostic process group");
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_ne!(persisted_diagnostic_pgid, child.owned_pgid().unwrap());
        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(group_events(&events)
            .iter()
            .all(|event| event.ends_with(":77")));
        let outcome = crate::supervisor::finish_owner_teardown_with(
            &state_path,
            &persisted.identity(),
            crate::supervisor::OwnerTeardownDecision::Interrupted,
            |_| result.confirmation,
        )
        .expect("finalize exact persisted state after physical confirmation");
        assert_eq!(
            outcome,
            crate::supervisor::OwnerTerminalOutcome::Interrupted
        );
        assert_eq!(
            crate::supervisor::read_runtime_state(&state_path).expect("read finalized state"),
            crate::supervisor::RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[cfg(unix)]
    #[test]
    fn teardown_escalates_to_group_kill_after_the_exact_phase_one_deadline() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut results = vec![Ok(())];
        results.extend((0..6).map(|_| Ok(())));
        results.push(Ok(()));
        results.push(Err(io::Error::from_raw_os_error(3)));
        let mut groups = FakeGroupControl::new(&events, results);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(result.forced);
        assert_eq!(now.get(), Duration::from_secs(5));
        let events = events.into_inner();
        let kill = events
            .iter()
            .position(|event| event == "kill:77")
            .expect("group KILL event");
        assert_eq!(
            events[..kill]
                .iter()
                .filter(|event| event.as_str() == "probe:77")
                .count(),
            6,
            "phase one polls immediately and at its exact deadline"
        );
        assert_eq!(events[kill + 1], "probe:77");
        assert_eq!(
            &events[events.len() - 2..],
            ["drains_finished", "drains_join"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn term_esrch_latches_absence_and_never_touches_that_numeric_group_again() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups = FakeGroupControl::new(&events, vec![Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(!result.forced);
        assert_eq!(group_events(&events), vec!["term:77"]);
    }

    #[cfg(unix)]
    #[test]
    fn probe_esrch_latches_absence_while_leader_reaping_continues_without_kill() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(None), Ok(Some(0))]);
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(!result.forced);
        assert_eq!(group_events(&events), vec!["term:77", "probe:77"]);
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| event.as_str() == "reap")
                .count(),
            2
        );
    }

    #[cfg(unix)]
    #[test]
    fn persistent_eperm_never_authorizes_drains_or_confirmation() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups = FakeGroupControl::new(
            &events,
            (0..14)
                .map(|_| Err(io::Error::from_raw_os_error(1)))
                .collect(),
        );
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(result.forced);
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn unknown_term_error_stays_unconfirmed_after_later_esrch() {
        assert_unknown_group_error_is_sticky(GroupSignal::Term);
    }

    #[cfg(unix)]
    #[test]
    fn unknown_kill_error_stays_unconfirmed_after_later_esrch() {
        assert_unknown_group_error_is_sticky(GroupSignal::Kill);
    }

    #[cfg(unix)]
    #[test]
    fn unknown_probe_error_stays_unconfirmed_after_later_esrch() {
        assert_unknown_group_error_is_sticky(GroupSignal::Probe);
    }

    #[cfg(unix)]
    fn assert_unknown_group_error_is_sticky(failing_signal: GroupSignal) {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let unknown = || Err(io::Error::from_raw_os_error(5));
        let mut results = Vec::new();
        match failing_signal {
            GroupSignal::Term => {
                results.push(unknown());
                results.push(Err(io::Error::from_raw_os_error(3)));
            }
            GroupSignal::Kill => {
                results.push(Ok(()));
                results.extend((0..6).map(|_| Ok(())));
                results.push(unknown());
                results.push(Err(io::Error::from_raw_os_error(3)));
            }
            GroupSignal::Probe => {
                results.push(Ok(()));
                results.push(unknown());
                results.extend((0..5).map(|_| Ok(())));
                results.push(Ok(()));
                results.push(Err(io::Error::from_raw_os_error(3)));
            }
        }
        let mut groups = FakeGroupControl::new(&events, results);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        let expected = match failing_signal {
            GroupSignal::Term => "term:77",
            GroupSignal::Kill => "kill:77",
            GroupSignal::Probe => "probe:77",
        };
        assert!(group_events(&events).contains(&expected.to_string()));
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn reaped_leader_with_persistent_group_presence_is_unconfirmed() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups = FakeGroupControl::new(&events, Vec::new());
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(result.forced);
        assert!(group_events(&events).contains(&"kill:77".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn absent_group_with_live_leader_spends_phase_two_only_reaping_and_never_kills() {
        let events = RefCell::new(Vec::new());
        let mut child =
            KernelFakeChild::new(&events, Some(77), (0..12).map(|_| Ok(None)).collect());
        let mut groups = FakeGroupControl::new(&events, vec![Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(!result.forced);
        assert_eq!(group_events(&events), vec!["term:77"]);
        assert_eq!(now.get(), Duration::from_secs(10));
    }

    #[cfg(unix)]
    #[test]
    fn try_wait_error_preserves_unconfirmed_state_even_after_group_absence() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(
            &events,
            Some(77),
            vec![
                Err(io::Error::other("injected try_wait failure")),
                Ok(Some(0)),
            ],
        );
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(
            events
                .borrow()
                .iter()
                .filter(|event| event.as_str() == "reap")
                .count()
                >= 2,
            "the leader must keep being polled after a transient try_wait error"
        );
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn exact_deadline_boundary_poll_can_confirm_without_late_kill() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut results = vec![Ok(())];
        results.extend((0..5).map(|_| Ok(())));
        results.push(Err(io::Error::from_raw_os_error(3)));
        let mut groups = FakeGroupControl::new(&events, results);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(!result.forced);
        assert_eq!(now.get(), Duration::from_secs(5));
        assert!(!group_events(&events).contains(&"kill:77".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn phase_two_starts_after_kill_and_includes_its_exact_deadline_poll() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut results = vec![Ok(())];
        results.extend((0..6).map(|_| Ok(())));
        results.push(Ok(()));
        results.extend((0..5).map(|_| Ok(())));
        results.push(Err(io::Error::from_raw_os_error(3)));
        let mut groups = FakeGroupControl::new(&events, results);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(result.forced);
        assert_eq!(now.get(), Duration::from_secs(10));
        let groups = group_events(&events);
        let kill = groups
            .iter()
            .position(|event| event == "kill:77")
            .expect("KILL event");
        assert_eq!(
            groups[kill + 1..]
                .iter()
                .filter(|event| event.as_str() == "probe:77")
                .count(),
            6
        );
    }

    #[cfg(unix)]
    #[test]
    fn every_sleep_is_clamped_to_the_remaining_phase_deadline() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups = FakeGroupControl::new(&events, Vec::new());
        let now = Cell::new(Duration::ZERO);
        let sleeps = RefCell::new(Vec::new());
        let timing = TeardownTiming {
            interval: Duration::from_secs(3),
            ..TeardownTiming::test()
        };

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            timing,
            || now.get(),
            |duration| {
                sleeps.borrow_mut().push(duration);
                now.set(now.get() + duration);
            },
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert_eq!(
            sleeps.into_inner(),
            vec![
                Duration::from_secs(3),
                Duration::from_secs(2),
                Duration::from_secs(3),
                Duration::from_secs(2),
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn successful_group_kill_without_esrch_remains_unconfirmed_and_skips_drains() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]);
        let mut groups = FakeGroupControl::new(&events, Vec::new());
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(result.forced);
        assert!(group_events(&events).contains(&"kill:77".to_string()));
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn blocked_log_drain_times_out_at_one_total_deadline_without_joining() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))])
            .with_drain_states(vec![false; 6]);
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert_eq!(now.get(), Duration::from_secs(5));
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| event.as_str() == "drains_finished")
                .count(),
            6
        );
        assert!(!events.borrow().iter().any(|event| event == "drains_join"));
    }

    #[cfg(unix)]
    #[test]
    fn completed_drain_error_makes_the_kernel_unconfirmed() {
        let events = RefCell::new(Vec::new());
        let mut child =
            KernelFakeChild::new(&events, Some(77), vec![Ok(Some(0))]).with_drain_error();
        let mut groups =
            FakeGroupControl::new(&events, vec![Ok(()), Err(io::Error::from_raw_os_error(3))]);
        let now = Cell::new(Duration::ZERO);

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        assert!(events.borrow().iter().any(|event| event == "drains_join"));
    }

    #[test]
    fn successful_log_drain_propagates_all_bytes_and_joins_only_after_finished() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("successful-drain.log");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(&log_path).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let handle = spawn_log_drain(Cursor::new(b"complete log\n".to_vec()), writer)
            .expect("spawn successful drain");
        let mut server = spawned_server_with_test_drains(vec![handle]);
        wait_until_test_drains_finish(&server);

        server.join_log_drains().expect("join successful drain");

        assert_eq!(
            std::fs::read(&log_path).expect("read drained log"),
            b"complete log\n"
        );
    }

    #[test]
    fn log_drain_reader_error_is_returned_after_the_finished_handle_is_joined() {
        struct FailingReader;

        impl Read for FailingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("injected reader failure"))
            }
        }

        let temp = tempdir().expect("tempdir");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("reader-error.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let handle = spawn_log_drain(FailingReader, writer).expect("spawn failing reader drain");
        let mut server = spawned_server_with_test_drains(vec![handle]);
        wait_until_test_drains_finish(&server);

        let error = server
            .join_log_drains()
            .expect_err("reader error must propagate");

        assert!(error.to_string().contains("injected reader failure"));
    }

    #[test]
    fn log_drain_writer_error_is_returned_after_the_finished_handle_is_joined() {
        let temp = tempdir().expect("tempdir");
        let log_path = temp.path().join("writer-error.log");
        std::fs::write(&log_path, b"").expect("create read-only handle target");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::open(&log_path).expect("open file without write access"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let handle = spawn_log_drain(Cursor::new(b"cannot write\n".to_vec()), writer)
            .expect("spawn writer-error drain");
        let mut server = spawned_server_with_test_drains(vec![handle]);
        wait_until_test_drains_finish(&server);

        let error = server
            .join_log_drains()
            .expect_err("writer error must propagate");

        assert!(matches!(error, SupervisorError::Io(_)));
    }

    #[test]
    fn panicked_log_drain_is_returned_without_joining_an_unfinished_handle() {
        let handle = thread::spawn(|| -> io::Result<()> {
            panic!("injected drain panic");
        });
        let mut server = spawned_server_with_test_drains(vec![handle]);
        wait_until_test_drains_finish(&server);

        let error = server
            .join_log_drains()
            .expect_err("drain panic must propagate");

        assert!(error.to_string().contains("log drain thread panicked"));
    }

    #[test]
    fn unfinished_log_drain_returns_immediately_without_an_unbounded_join() {
        struct BlockingReader {
            release: mpsc::Receiver<()>,
        }

        impl Read for BlockingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                self.release
                    .recv()
                    .map_err(|_| io::Error::other("release sender dropped"))?;
                Ok(0)
            }
        }

        let temp = tempdir().expect("tempdir");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("blocked.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let (release_tx, release_rx) = mpsc::channel();
        let handle = spawn_log_drain(
            BlockingReader {
                release: release_rx,
            },
            writer,
        )
        .expect("spawn blocked drain");
        let mut server = spawned_server_with_test_drains(vec![handle]);

        let error = server
            .join_log_drains()
            .expect_err("unfinished drain must not be joined");
        assert!(matches!(
            error,
            SupervisorError::Io(ref error) if error.kind() == io::ErrorKind::WouldBlock
        ));

        release_tx.send(()).expect("release blocked drain");
        wait_until_test_drains_finish(&server);
        server.join_log_drains().expect("join released drain");
    }

    fn spawned_server_with_test_drains(
        log_drains: Vec<JoinHandle<io::Result<()>>>,
    ) -> SpawnedServer {
        let mut child = Command::new(std::env::current_exe().expect("current test binary"))
            .arg("--list")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn inert test child");
        let status = child.wait().expect("reap inert test child");
        SpawnedServer {
            child,
            #[cfg(unix)]
            owned_process_group: None,
            reaped_status: Some(status.code().unwrap_or_default()),
            log_drains,
            initialization_error: None,
        }
    }

    fn wait_until_test_drains_finish(server: &SpawnedServer) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !server.log_drains_finished() {
            assert!(Instant::now() < deadline, "log drain did not finish");
            thread::yield_now();
        }
    }

    #[cfg(unix)]
    #[test]
    fn production_group_adapter_targets_the_validated_negative_pgid() {
        let temp = tempdir().expect("tempdir");
        let writer = Arc::new(Mutex::new(BoundedLogWriter {
            file: File::create(temp.path().join("adapter.log")).expect("create log"),
            remaining: MAX_LOG_BYTES,
            truncated: false,
        }));
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let mut spawned = spawn_managed_command(command, writer).expect("spawn managed child");
        let group = OwnedProcessGroup::try_from_raw(i64::from(
            spawned.owned_pgid().expect("owned process group"),
        ))
        .expect("validated group");
        let mut adapter = UnixProcessGroupControl;

        adapter
            .signal_group(group, GroupSignal::Probe)
            .expect("group is present");
        adapter
            .signal_group(group, GroupSignal::Kill)
            .expect("group KILL succeeds");
        spawned.child.wait().expect("reap killed leader");
        let error = adapter
            .signal_group(group, GroupSignal::Probe)
            .expect_err("reaped singleton group must be absent");

        assert_eq!(error.raw_os_error(), Some(3));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn real_unix_teardown_kills_term_ignoring_descendant_before_completion() {
        match std::env::var("LOXA_TEARDOWN_HELPER_ROLE").as_deref() {
            Ok("controller") => run_real_helper_controller(),
            Ok("leader") => run_real_helper_leader(),
            Ok("descendant") => run_real_helper_descendant(),
            Ok(role) => panic!("unknown teardown helper role {role}"),
            Err(_) => run_real_helper_test_process(),
        }
    }

    #[cfg(target_os = "linux")]
    const REAL_HELPER_TEST: &str =
        "supervisor::teardown::tests::real_unix_teardown_kills_term_ignoring_descendant_before_completion";

    #[cfg(target_os = "linux")]
    unsafe extern "C" fn real_helper_leader_term(_signal: c_int) {
        unsafe extern "C" {
            fn _exit(status: c_int) -> !;
        }
        unsafe { _exit(42) }
    }

    #[cfg(target_os = "linux")]
    unsafe extern "C" fn real_helper_descendant_term(_signal: c_int) {}

    #[cfg(target_os = "linux")]
    fn run_real_helper_test_process() {
        let temp = tempdir().expect("real helper tempdir");
        let root = temp.path();
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        );
        let mut controller = helper_command("controller", root, &nonce)
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn real-helper controller");

        let status = wait_for_helper_exit(&mut controller, Duration::from_secs(15));
        let Some(status) = status else {
            cleanup_reported_helper_group(root);
            let _ = controller.kill();
            let _ = controller.wait();
            panic!("real-helper controller exceeded its 15-second deadline");
        };
        if !status.success() {
            cleanup_reported_helper_group(root);
            panic!("real-helper controller failed with {status}");
        }
    }

    #[cfg(target_os = "linux")]
    fn run_real_helper_controller() {
        #[cfg(target_os = "linux")]
        install_linux_subreaper();

        use std::os::unix::process::CommandExt as _;

        let root = helper_root();
        let nonce = helper_nonce();
        let mut command = helper_command("leader", &root, &nonce);
        command.stdout(Stdio::null()).process_group(0);
        let leader = command.spawn().expect("spawn helper leader");
        let leader_pid = checked_test_pid(leader.id());
        let shared = Rc::new(RefCell::new(RealHelperState::default()));
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut guard = RealHelperGuard::new(leader, Rc::clone(&shared), Rc::clone(&events));

        let actual_pgid = raw_getpgid(leader_pid).expect("read leader process group");
        assert_eq!(
            actual_pgid, leader_pid,
            "helper leader owns its process group"
        );
        let group = OwnedProcessGroup::try_from_raw(i64::from(actual_pgid))
            .expect("validated helper process group");
        guard.set_group(group);
        publish_controller_info(&root, leader_pid, actual_pgid, None);

        let leader_ready = wait_for_artifact(
            &root.join("leader.ready"),
            Duration::from_secs(5),
            Some(&mut guard.child_mut().child),
        );
        let leader_fields = parse_helper_fields(&leader_ready, 3);
        assert_eq!(leader_fields[0], nonce);
        assert_eq!(parse_c_int(&leader_fields[1]), leader_pid);
        assert_eq!(parse_c_int(&leader_fields[2]), actual_pgid);

        publish_artifact(&root.join("leader.go"), &format!("{nonce}\n"));
        let aggregate = wait_for_artifact(
            &root.join("aggregate.ready"),
            Duration::from_secs(5),
            Some(&mut guard.child_mut().child),
        );
        let aggregate_fields = parse_helper_fields(&aggregate, 4);
        assert_eq!(aggregate_fields[0], nonce);
        assert_eq!(parse_c_int(&aggregate_fields[1]), leader_pid);
        let descendant_pid = parse_c_int(&aggregate_fields[2]);
        assert_eq!(parse_c_int(&aggregate_fields[3]), actual_pgid);
        assert_eq!(
            raw_getpgid(descendant_pid).expect("read descendant process group"),
            actual_pgid,
            "descendant remains in the owned helper group"
        );
        guard.set_descendant(descendant_pid);
        publish_controller_info(&root, leader_pid, actual_pgid, Some(descendant_pid));

        let mut groups = RecordingRealGroupControl {
            inner: UnixProcessGroupControl,
            shared: Rc::clone(&shared),
            events: Rc::clone(&events),
            #[cfg(target_os = "linux")]
            descendant_pid,
        };
        let started = Instant::now();
        let result = teardown_managed_child_with(
            guard.child_mut(),
            &mut groups,
            TeardownTiming {
                phase_one: Duration::from_millis(200),
                phase_two: Duration::from_secs(2),
                drains: Duration::from_secs(1),
                interval: Duration::from_millis(10),
            },
            || started.elapsed(),
            thread::sleep,
        );

        let Some(trace) = guard.disarm_after_confirmation(result) else {
            panic!("real helper teardown was not fully confirmed: {result:?}");
        };
        assert!(
            result.forced,
            "TERM-ignoring descendant requires group KILL"
        );
        assert_real_helper_trace(&trace);
    }

    #[cfg(target_os = "linux")]
    fn run_real_helper_leader() {
        install_test_signal_handler(15, real_helper_leader_term);
        let root = helper_root();
        let nonce = helper_nonce();
        let pid = checked_test_pid(std::process::id());
        let pgid = raw_getpgrp();
        assert_eq!(pid, pgid, "leader helper must be its process-group leader");
        publish_artifact(
            &root.join("leader.ready"),
            &format!("{nonce} {pid} {pgid}\n"),
        );

        let go = wait_for_artifact(&root.join("leader.go"), Duration::from_secs(5), None);
        assert_eq!(go.trim(), nonce);
        let descendant = helper_command("descendant", &root, &nonce)
            .env("LOXA_TEARDOWN_HELPER_EXPECTED_PGID", pgid.to_string())
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn helper descendant");
        let descendant_pid = checked_test_pid(descendant.id());
        let mut descendant_guard = DirectChildGuard::new(descendant);
        let ready = wait_for_artifact(
            &root.join("descendant.ready"),
            Duration::from_secs(5),
            Some(descendant_guard.child_mut()),
        );
        let fields = parse_helper_fields(&ready, 4);
        assert_eq!(fields[0], nonce);
        assert_eq!(parse_c_int(&fields[1]), descendant_pid);
        assert_eq!(parse_c_int(&fields[2]), pgid);
        assert_eq!(fields[3], "term-handler-installed");
        assert_eq!(
            raw_getpgid(descendant_pid).expect("leader verifies descendant membership"),
            pgid
        );
        publish_artifact(
            &root.join("aggregate.ready"),
            &format!("{nonce} {pid} {descendant_pid} {pgid}\n"),
        );
        descendant_guard.disarm();

        loop {
            thread::park();
        }
    }

    #[cfg(target_os = "linux")]
    fn run_real_helper_descendant() {
        install_test_signal_handler(15, real_helper_descendant_term);
        let root = helper_root();
        let nonce = helper_nonce();
        let expected_pgid = std::env::var("LOXA_TEARDOWN_HELPER_EXPECTED_PGID")
            .expect("expected helper PGID")
            .parse::<c_int>()
            .expect("numeric expected helper PGID");
        let pid = checked_test_pid(std::process::id());
        let pgid = raw_getpgrp();
        assert_eq!(pgid, expected_pgid, "descendant inherited the owned PGID");
        publish_artifact(
            &root.join("descendant.ready"),
            &format!("{nonce} {pid} {pgid} term-handler-installed\n"),
        );

        loop {
            thread::park();
        }
    }

    #[cfg(target_os = "linux")]
    fn helper_command(role: &str, root: &Path, nonce: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("current test binary"));
        command
            .arg("--exact")
            .arg(REAL_HELPER_TEST)
            .arg("--nocapture")
            .env("LOXA_TEARDOWN_HELPER_ROLE", role)
            .env("LOXA_TEARDOWN_HELPER_ROOT", root)
            .env("LOXA_TEARDOWN_HELPER_NONCE", nonce)
            .stdin(Stdio::null());
        command
    }

    #[cfg(target_os = "linux")]
    fn helper_root() -> PathBuf {
        PathBuf::from(
            std::env::var_os("LOXA_TEARDOWN_HELPER_ROOT").expect("helper root environment"),
        )
    }

    #[cfg(target_os = "linux")]
    fn helper_nonce() -> String {
        std::env::var("LOXA_TEARDOWN_HELPER_NONCE").expect("helper nonce environment")
    }

    #[cfg(target_os = "linux")]
    fn publish_artifact(path: &Path, contents: &str) {
        let temp = path.with_extension(format!("{}.tmp", std::process::id()));
        fs::write(&temp, contents).expect("write helper artifact temp file");
        fs::rename(&temp, path).expect("publish helper artifact atomically");
    }

    #[cfg(target_os = "linux")]
    fn wait_for_artifact(path: &Path, timeout: Duration, mut child: Option<&mut Child>) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            match fs::read_to_string(path) {
                Ok(contents) => return contents,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => panic!("read helper artifact {}: {error}", path.display()),
            }
            if let Some(child) = child.as_deref_mut() {
                if let Some(status) = child.try_wait().expect("poll helper child") {
                    panic!("helper child exited before {}: {status}", path.display());
                }
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for helper artifact {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(target_os = "linux")]
    fn parse_helper_fields(contents: &str, count: usize) -> Vec<String> {
        let fields = contents
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(
            fields.len(),
            count,
            "malformed helper artifact: {contents:?}"
        );
        fields
    }

    #[cfg(target_os = "linux")]
    fn parse_c_int(value: &str) -> c_int {
        value.parse::<c_int>().expect("numeric helper field")
    }

    #[cfg(target_os = "linux")]
    fn checked_test_pid(pid: u32) -> c_int {
        let pid = c_int::try_from(pid).expect("helper PID fits c_int");
        assert!(pid > 1, "helper PID must be strictly above one");
        pid
    }

    #[cfg(target_os = "linux")]
    fn raw_getpgid(pid: c_int) -> io::Result<c_int> {
        unsafe extern "C" {
            fn getpgid(pid: c_int) -> c_int;
        }
        let result = unsafe { getpgid(pid) };
        if result >= 0 {
            Ok(result)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "linux")]
    fn raw_getpgrp() -> c_int {
        unsafe extern "C" {
            fn getpgrp() -> c_int;
        }
        unsafe { getpgrp() }
    }

    #[cfg(target_os = "linux")]
    fn raw_kill_group(group: OwnedProcessGroup, signal: c_int) -> io::Result<()> {
        unsafe extern "C" {
            fn kill(pid: c_int, signal: c_int) -> c_int;
        }
        if unsafe { kill(group.negative_pgid, signal) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "linux")]
    fn install_test_signal_handler(signal_number: c_int, handler: unsafe extern "C" fn(c_int)) {
        unsafe extern "C" {
            fn signal(signal: c_int, handler: usize) -> usize;
        }
        let previous = unsafe { signal(signal_number, handler as *const () as usize) };
        assert_ne!(previous, usize::MAX, "install helper signal handler");
    }

    #[cfg(target_os = "linux")]
    fn wait_for_helper_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("poll helper process") {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(target_os = "linux")]
    fn publish_controller_info(
        root: &Path,
        leader_pid: c_int,
        pgid: c_int,
        descendant_pid: Option<c_int>,
    ) {
        publish_artifact(
            &root.join("controller.info"),
            &format!("{leader_pid} {pgid} {}\n", descendant_pid.unwrap_or(0)),
        );
    }

    #[cfg(target_os = "linux")]
    fn cleanup_reported_helper_group(root: &Path) {
        let Ok(contents) = fs::read_to_string(root.join("controller.info")) else {
            return;
        };
        let fields = parse_helper_fields(&contents, 3);
        let leader_pid = parse_c_int(&fields[0]);
        let pgid = parse_c_int(&fields[1]);
        let descendant_pid = parse_c_int(&fields[2]);
        let Ok(group) = OwnedProcessGroup::try_from_raw(i64::from(pgid)) else {
            return;
        };
        let leader_witness = raw_getpgid(leader_pid).ok() == Some(pgid);
        let descendant_witness =
            descendant_pid > 1 && raw_getpgid(descendant_pid).ok() == Some(pgid);
        if leader_witness || descendant_witness {
            let _ = raw_kill_group(group, 9);
        }
    }

    #[cfg(target_os = "linux")]
    fn install_linux_subreaper() {
        use std::ffi::c_ulong;
        unsafe extern "C" {
            fn prctl(option: c_int, ...) -> c_int;
        }
        const PR_SET_CHILD_SUBREAPER: c_int = 36;
        let result = unsafe {
            prctl(
                PR_SET_CHILD_SUBREAPER,
                1 as c_ulong,
                0 as c_ulong,
                0 as c_ulong,
                0 as c_ulong,
            )
        };
        assert_eq!(result, 0, "install Linux child subreaper");
    }

    #[cfg(target_os = "linux")]
    #[derive(Default)]
    struct RealHelperState {
        absent: bool,
        term_sent: bool,
        kill_sent: bool,
        leader_reaped: bool,
        #[cfg(target_os = "linux")]
        descendant_reaped: bool,
    }

    #[cfg(target_os = "linux")]
    struct RealHelperChild {
        child: Child,
        group: Option<OwnedProcessGroup>,
        reaped_status: Option<i32>,
        shared: Rc<RefCell<RealHelperState>>,
        events: Rc<RefCell<Vec<String>>>,
    }

    #[cfg(target_os = "linux")]
    impl ManagedChild for RealHelperChild {
        fn pid(&self) -> u32 {
            self.child.id()
        }

        fn owned_pgid(&self) -> Option<i32> {
            self.group.map(|group| group.pgid)
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.child.kill()
        }

        fn kill(&mut self) -> io::Result<()> {
            self.child.kill()
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            if let Some(status) = self.reaped_status {
                return Ok(Some(status));
            }
            let status = self
                .child
                .try_wait()?
                .map(|status| status.code().unwrap_or_default());
            if let Some(status) = status {
                self.reaped_status = Some(status);
                self.shared.borrow_mut().leader_reaped = true;
                self.events
                    .borrow_mut()
                    .push(format!("leader_reaped:{status}"));
            }
            Ok(status)
        }
    }

    #[cfg(target_os = "linux")]
    impl LogDrainingChild for RealHelperChild {
        fn log_drains_finished(&self) -> bool {
            self.events.borrow_mut().push("drains_finished".to_string());
            true
        }

        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("drains_join".to_string());
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    struct RecordingRealGroupControl {
        inner: UnixProcessGroupControl,
        shared: Rc<RefCell<RealHelperState>>,
        events: Rc<RefCell<Vec<String>>>,
        #[cfg(target_os = "linux")]
        descendant_pid: c_int,
    }

    #[cfg(target_os = "linux")]
    impl ProcessGroupControl for RecordingRealGroupControl {
        fn signal_group(
            &mut self,
            group: OwnedProcessGroup,
            signal: GroupSignal,
        ) -> io::Result<()> {
            if self.shared.borrow().absent {
                return Err(io::Error::other(
                    "attempted group operation after ESRCH absence latch",
                ));
            }
            #[cfg(target_os = "linux")]
            if signal == GroupSignal::Probe && self.shared.borrow().kill_sent {
                self.reap_linux_descendant_if_ready()?;
            }

            let result = self.inner.signal_group(group, signal);
            let mut shared = self.shared.borrow_mut();
            match (&result, signal) {
                (Ok(()), GroupSignal::Term) => {
                    shared.term_sent = true;
                    self.events.borrow_mut().push("term".to_string());
                }
                (Ok(()), GroupSignal::Kill) => {
                    shared.kill_sent = true;
                    self.events.borrow_mut().push("kill".to_string());
                }
                (Ok(()), GroupSignal::Probe) => {
                    self.events.borrow_mut().push("probe_present".to_string());
                }
                (Err(error), GroupSignal::Probe) if error.raw_os_error() == Some(3) => {
                    shared.absent = true;
                    self.events.borrow_mut().push("probe_absent".to_string());
                }
                (Err(error), _) if error.raw_os_error() == Some(3) => {
                    shared.absent = true;
                    self.events.borrow_mut().push("signal_absent".to_string());
                }
                _ => {}
            }
            result
        }
    }

    #[cfg(target_os = "linux")]
    impl RecordingRealGroupControl {
        fn reap_linux_descendant_if_ready(&mut self) -> io::Result<()> {
            if self.shared.borrow().descendant_reaped {
                return Ok(());
            }
            unsafe extern "C" {
                fn waitpid(pid: c_int, status: *mut c_int, options: c_int) -> c_int;
            }
            let mut status = 0;
            let result = unsafe { waitpid(self.descendant_pid, &mut status, 1) };
            if result == self.descendant_pid {
                self.shared.borrow_mut().descendant_reaped = true;
                self.events
                    .borrow_mut()
                    .push("descendant_reaped".to_string());
                return Ok(());
            }
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(10) && !self.shared.borrow().leader_reaped {
                return Ok(());
            }
            Err(error)
        }
    }

    #[cfg(target_os = "linux")]
    struct RealHelperGuard {
        child: Option<RealHelperChild>,
        group: Option<OwnedProcessGroup>,
        descendant_pid: Option<c_int>,
        shared: Rc<RefCell<RealHelperState>>,
        events: Rc<RefCell<Vec<String>>>,
        armed: bool,
    }

    #[cfg(target_os = "linux")]
    impl RealHelperGuard {
        fn new(
            child: Child,
            shared: Rc<RefCell<RealHelperState>>,
            events: Rc<RefCell<Vec<String>>>,
        ) -> Self {
            Self {
                child: Some(RealHelperChild {
                    child,
                    group: None,
                    reaped_status: None,
                    shared: Rc::clone(&shared),
                    events: Rc::clone(&events),
                }),
                group: None,
                descendant_pid: None,
                shared,
                events,
                armed: true,
            }
        }

        fn child_mut(&mut self) -> &mut RealHelperChild {
            self.child.as_mut().expect("helper guard child")
        }

        fn set_group(&mut self, group: OwnedProcessGroup) {
            self.group = Some(group);
            self.child_mut().group = Some(group);
        }

        fn set_descendant(&mut self, pid: c_int) {
            self.descendant_pid = Some(pid);
        }

        fn disarm_after_confirmation(
            &mut self,
            result: ChildTeardownResult,
        ) -> Option<Vec<String>> {
            let state = self.shared.borrow();
            if result.confirmation != TeardownConfirmation::Confirmed
                || !state.absent
                || !state.leader_reaped
            {
                return None;
            }
            drop(state);
            self.armed = false;
            self.group = None;
            self.descendant_pid = None;
            self.child.take();
            Some(self.events.borrow().clone())
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for RealHelperGuard {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            let Some(child) = self.child.as_mut() else {
                return;
            };
            let _ = child.try_wait();
            if let Some(group) = self.group {
                let state = self.shared.borrow();
                let witness = self.descendant_pid.and_then(|pid| raw_getpgid(pid).ok())
                    == Some(group.pgid)
                    || raw_getpgid(checked_test_pid(child.child.id())).ok() == Some(group.pgid);
                let may_kill = !state.absent && !state.kill_sent && witness;
                drop(state);
                if may_kill {
                    let _ = raw_kill_group(group, 9);
                }
            }
            if child.reaped_status.is_none() {
                let _ = child.child.kill();
                let _ = wait_for_helper_exit(&mut child.child, Duration::from_secs(2));
            }
            #[cfg(target_os = "linux")]
            if let Some(descendant_pid) = self.descendant_pid {
                let _ = bounded_reap_linux_descendant(descendant_pid, Duration::from_secs(2));
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn bounded_reap_linux_descendant(pid: c_int, timeout: Duration) -> io::Result<()> {
        unsafe extern "C" {
            fn waitpid(pid: c_int, status: *mut c_int, options: c_int) -> c_int;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let mut status = 0;
            let result = unsafe { waitpid(pid, &mut status, 1) };
            if result == pid {
                return Ok(());
            }
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(10) {
                    return Err(error);
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out reaping helper descendant",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(target_os = "linux")]
    struct DirectChildGuard {
        child: Option<Child>,
    }

    #[cfg(target_os = "linux")]
    impl DirectChildGuard {
        fn new(child: Child) -> Self {
            Self { child: Some(child) }
        }

        fn child_mut(&mut self) -> &mut Child {
            self.child.as_mut().expect("direct helper child")
        }

        fn disarm(&mut self) {
            self.child.take();
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for DirectChildGuard {
        fn drop(&mut self) {
            if let Some(child) = self.child.as_mut() {
                let _ = child.kill();
                let _ = wait_for_helper_exit(child, Duration::from_secs(2));
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_real_helper_trace(trace: &[String]) {
        let term = trace
            .iter()
            .position(|event| event == "term")
            .expect("TERM event");
        let leader = trace
            .iter()
            .position(|event| event == "leader_reaped:42")
            .expect("leader exit 42 reaped");
        let surviving_probe = trace
            .iter()
            .enumerate()
            .skip(leader + 1)
            .find(|(_, event)| event.as_str() == "probe_present")
            .map(|(index, _)| index)
            .expect("descendant survives TERM after leader reap");
        let kill = trace
            .iter()
            .position(|event| event == "kill")
            .expect("KILL event");
        let absent = trace
            .iter()
            .position(|event| event == "probe_absent")
            .expect("ESRCH absence event");
        let drains = trace
            .iter()
            .position(|event| event == "drains_join")
            .expect("drain completion");
        assert!(term < leader);
        assert!(leader < surviving_probe);
        assert!(surviving_probe < kill);
        assert!(kill < absent);
        assert!(absent < drains);
        assert_eq!(drains, trace.len() - 1, "drain join is the final event");
    }

    #[cfg(unix)]
    #[test]
    fn nonadvancing_clock_fails_safely_without_spinning_or_draining() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(None)]);
        let mut groups = FakeGroupControl::new(&events, Vec::new());

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            TeardownTiming::test(),
            || Duration::ZERO,
            |_| {},
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        let groups = group_events(&events);
        assert!(groups.contains(&"term:77".to_string()));
        assert!(groups.contains(&"kill:77".to_string()));
        assert!(groups.len() <= 4, "kernel must not spin");
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn zero_poll_interval_fails_safely_without_spinning_or_draining() {
        let events = RefCell::new(Vec::new());
        let mut child = KernelFakeChild::new(&events, Some(77), vec![Ok(None)]);
        let mut groups = FakeGroupControl::new(&events, Vec::new());
        let now = Cell::new(Duration::ZERO);
        let timing = TeardownTiming {
            interval: Duration::ZERO,
            ..TeardownTiming::test()
        };

        let result = teardown_managed_child_with(
            &mut child,
            &mut groups,
            timing,
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
        let groups = group_events(&events);
        assert!(groups.contains(&"term:77".to_string()));
        assert!(groups.contains(&"kill:77".to_string()));
        assert!(groups.len() <= 4, "kernel must not spin");
        assert!(!events
            .borrow()
            .iter()
            .any(|event| event.starts_with("drains")));
    }

    #[cfg(unix)]
    #[test]
    fn missing_or_invalid_unix_owned_pgid_uses_only_direct_best_effort_and_never_confirms() {
        for invalid in [None, Some(-1), Some(0), Some(1)] {
            let events = RefCell::new(Vec::new());
            let mut child = KernelFakeChild::new(&events, invalid, vec![Ok(Some(0))]);
            let mut groups = FakeGroupControl::new(&events, Vec::new());
            let now = Cell::new(Duration::ZERO);

            let result = teardown_managed_child_with(
                &mut child,
                &mut groups,
                TeardownTiming::test(),
                || now.get(),
                |duration| now.set(now.get() + duration),
            );

            assert_eq!(result.confirmation, TeardownConfirmation::Unconfirmed);
            assert_eq!(group_events(&events), Vec::<String>::new());
            assert_eq!(events.borrow()[0], "direct_term");
            assert!(!events
                .borrow()
                .iter()
                .any(|event| event.starts_with("drains")));
        }
    }

    #[test]
    fn direct_child_fallback_confirms_force_kill_after_unsupported_graceful_stop() {
        let events = RefCell::new(Vec::new());
        let mut child =
            KernelFakeChild::new(&events, None, vec![Ok(None), Ok(Some(9))]).with_terminate_error();
        let now = Cell::new(Duration::ZERO);
        let timing = TeardownTiming {
            phase_one: Duration::ZERO,
            phase_two: Duration::ZERO,
            ..TeardownTiming::test()
        };

        let result = teardown_direct_child_with(
            &mut child,
            timing,
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(result.forced);
        assert_eq!(
            events.into_inner(),
            vec![
                "direct_term",
                "reap",
                "direct_kill",
                "reap",
                "drains_finished",
                "drains_join",
            ]
        );
    }

    #[test]
    fn direct_child_fallback_never_force_kills_a_leader_reaped_during_graceful_phase() {
        let events = RefCell::new(Vec::new());
        let mut child =
            KernelFakeChild::new(&events, None, vec![Ok(Some(0))]).with_terminate_error();
        let now = Cell::new(Duration::ZERO);

        let result = teardown_direct_child_with(
            &mut child,
            TeardownTiming::test(),
            || now.get(),
            |duration| now.set(now.get() + duration),
        );

        assert_eq!(result.confirmation, TeardownConfirmation::Confirmed);
        assert!(!result.forced);
        assert_eq!(
            events.into_inner(),
            vec!["direct_term", "reap", "drains_finished", "drains_join"]
        );
    }

    fn group_events(events: &RefCell<Vec<String>>) -> Vec<String> {
        events
            .borrow()
            .iter()
            .filter(|event| {
                event.starts_with("term:")
                    || event.starts_with("kill:")
                    || event.starts_with("probe:")
            })
            .cloned()
            .collect()
    }

    struct KernelFakeChild<'a> {
        events: &'a RefCell<Vec<String>>,
        pgid: Option<i32>,
        wait_results: VecDeque<io::Result<Option<i32>>>,
        drain_states: RefCell<VecDeque<bool>>,
        drain_error: bool,
        terminate_error: bool,
        kill_error: bool,
    }

    impl<'a> KernelFakeChild<'a> {
        fn new(
            events: &'a RefCell<Vec<String>>,
            pgid: Option<i32>,
            wait_results: Vec<io::Result<Option<i32>>>,
        ) -> Self {
            Self {
                events,
                pgid,
                wait_results: wait_results.into(),
                drain_states: RefCell::new(vec![true].into()),
                drain_error: false,
                terminate_error: false,
                kill_error: false,
            }
        }

        fn with_drain_states(mut self, states: Vec<bool>) -> Self {
            self.drain_states = RefCell::new(states.into());
            self
        }

        fn with_drain_error(mut self) -> Self {
            self.drain_error = true;
            self
        }

        fn with_terminate_error(mut self) -> Self {
            self.terminate_error = true;
            self
        }
    }

    impl ManagedChild for KernelFakeChild<'_> {
        fn pid(&self) -> u32 {
            77
        }

        fn owned_pgid(&self) -> Option<i32> {
            self.pgid
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("direct_term".to_string());
            if self.terminate_error {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "graceful termination is unsupported",
                ))
            } else {
                Ok(())
            }
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.borrow_mut().push("direct_kill".to_string());
            if self.kill_error {
                Err(io::Error::other("injected direct kill failure"))
            } else {
                Ok(())
            }
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.borrow_mut().push("reap".to_string());
            self.wait_results.pop_front().unwrap_or(Ok(Some(0)))
        }
    }

    impl LogDrainingChild for KernelFakeChild<'_> {
        fn log_drains_finished(&self) -> bool {
            self.events.borrow_mut().push("drains_finished".to_string());
            let mut states = self.drain_states.borrow_mut();
            if states.len() > 1 {
                states.pop_front().unwrap_or(false)
            } else {
                states.front().copied().unwrap_or(false)
            }
        }

        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.borrow_mut().push("drains_join".to_string());
            if self.drain_error {
                Err(SupervisorError::Io(io::Error::other(
                    "injected drain failure",
                )))
            } else {
                Ok(())
            }
        }
    }

    #[cfg(unix)]
    struct FakeGroupControl<'a> {
        events: &'a RefCell<Vec<String>>,
        results: VecDeque<io::Result<()>>,
    }

    #[cfg(unix)]
    impl<'a> FakeGroupControl<'a> {
        fn new(events: &'a RefCell<Vec<String>>, results: Vec<io::Result<()>>) -> Self {
            Self {
                events,
                results: results.into(),
            }
        }
    }

    #[cfg(unix)]
    impl ProcessGroupControl for FakeGroupControl<'_> {
        fn signal_group(
            &mut self,
            group: OwnedProcessGroup,
            signal: GroupSignal,
        ) -> io::Result<()> {
            let signal = match signal {
                GroupSignal::Term => "term",
                GroupSignal::Kill => "kill",
                GroupSignal::Probe => "probe",
            };
            self.events
                .borrow_mut()
                .push(format!("{signal}:{}", group.pgid));
            self.results.pop_front().unwrap_or(Ok(()))
        }
    }
}
