use super::readiness::{readiness_client, requested_start_outcome, RequestedStartOutcome};
use super::{OwnedServer, StartOutcome, StartupInterruption, StartupStop};
use crate::runner::arguments::{build_args_for_endpoint, resolve_requested_port};
use crate::runner::child::{ChildProcessGuard, ChildTerminationMode, PersistentSignalPolicy};
use crate::runner::launch::{
    no_prepared_runtime_guard, Launch, LaunchPolicy, ValidatedManagedRuntime,
};
use crate::runner::output::{spawn_output_reader, MAX_PENDING_ANNOUNCEMENTS};
#[cfg(unix)]
use crate::runner::service_transport::require_absent_unix_endpoint;
use crate::runner::signal::{clear_server_starting, mark_server_starting};
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

impl OwnedServer {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runner) fn start<F>(
        server: &Path,
        model: &Path,
        id: &str,
        requested_port: u16,
        ctx: u32,
        timeout: Duration,
        signal: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<i32>,
    {
        let launch = Launch::generic(server, model, id, requested_port, ctx);
        Self::start_inner(&launch, timeout, None, || signal().map(StartupStop::Signal))
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runner) fn start_with_ownership<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: crate::runtime::RuntimeOwnership,
        signal: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<i32>,
    {
        let child_ownership = runtime.reserve_child()?;
        Self::start_inner(
            launch,
            timeout,
            Some((
                child_ownership,
                crate::runtime::RuntimeLeasePublication::Foreground,
            )),
            || signal().map(StartupStop::Signal),
        )
    }

    pub(in crate::runner) fn start_with_persistent_ownership<F>(
        launch: &Launch,
        fingerprint: &crate::runtime_fingerprint::RuntimeFingerprint,
        timeout: Duration,
        runtime: crate::runtime::RuntimeChildOwnership,
        signal_policy: PersistentSignalPolicy,
        cancelled: &F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> bool,
    {
        Self::start_inner_with_policy(
            launch,
            timeout,
            Some((
                runtime,
                crate::runtime::RuntimeLeasePublication::PersistentApp(fingerprint),
            )),
            signal_policy,
            || cancelled().then_some(StartupStop::Interrupted(StartupInterruption::Cancelled)),
        )
    }

    pub(in crate::runner) fn start_with_service_ownership<F>(
        launch: &Launch,
        fingerprint: &crate::runtime_fingerprint::RuntimeFingerprint,
        endpoint: &Path,
        runtime_handle: &tokio::runtime::Handle,
        timeout: Duration,
        runtime: crate::runtime::RuntimeChildOwnership,
        cancelled: &F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> bool,
    {
        Self::start_inner_with_policy_and_endpoint(
            launch,
            timeout,
            Some((
                runtime,
                crate::runtime::RuntimeLeasePublication::Service {
                    fingerprint,
                    endpoint,
                },
            )),
            PersistentSignalPolicy::CallerManaged,
            Some(endpoint),
            Some(runtime_handle),
            || cancelled().then_some(StartupStop::Interrupted(StartupInterruption::Cancelled)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runner) fn start_inner<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeChildOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        Self::start_inner_with_policy(
            launch,
            timeout,
            runtime,
            PersistentSignalPolicy::ForegroundExit,
            stop,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner_with_policy<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeChildOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        signal_policy: PersistentSignalPolicy,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        Self::start_inner_with_policy_and_endpoint(
            launch,
            timeout,
            runtime,
            signal_policy,
            None,
            None,
            stop,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner_with_policy_and_endpoint<F>(
        launch: &Launch,
        timeout: Duration,
        runtime: Option<(
            crate::runtime::RuntimeChildOwnership,
            crate::runtime::RuntimeLeasePublication<'_>,
        )>,
        signal_policy: PersistentSignalPolicy,
        unix_socket: Option<&Path>,
        service_runtime: Option<&tokio::runtime::Handle>,
        stop: F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        let requested_port = if unix_socket.is_some() {
            0
        } else {
            resolve_requested_port(launch.requested_port)?
        };
        if let Some(endpoint) = unix_socket {
            require_absent_unix_endpoint(endpoint)?;
        }
        if unix_socket.is_some() && service_runtime.is_none() {
            return Err("Unix service launch requires its owning runtime handle".into());
        }
        let client = unix_socket.is_none().then(readiness_client).transpose()?;
        let mut command = launch.server_command();
        let prepared = launch.managed_runtime.as_ref().map_or_else(
            no_prepared_runtime_guard,
            ValidatedManagedRuntime::process_guard,
        );
        command
            .args(build_args_for_endpoint(launch, requested_port, unix_socket))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if launch.policy == LaunchPolicy::Service {
            command.env_clear();
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
            if launch.policy == LaunchPolicy::Service {
                // The socket and every incidental engine-created file must be
                // private even when a launcher inherited a permissive umask.
                unsafe {
                    command.pre_exec(|| {
                        libc::umask(0o077);
                        Ok(())
                    });
                }
            }
        }
        if signal_policy == PersistentSignalPolicy::ForegroundExit {
            mark_server_starting();
        }
        let retain_cleanup_failure = runtime.is_some() && launch.policy != LaunchPolicy::Foreground;
        let (runtime, publication) = match runtime {
            Some((runtime, publication)) => (Some(runtime), Some(publication)),
            None => (None, None),
        };
        let child = match ChildProcessGuard::spawn(
            &mut command,
            signal_policy,
            ChildTerminationMode::Graceful,
            prepared,
            runtime,
        ) {
            Ok(child) => child,
            Err(error) => {
                if signal_policy == PersistentSignalPolicy::ForegroundExit {
                    clear_server_starting();
                }
                return Err(error);
            }
        };
        #[cfg(test)]
        kill_owner_after_spawn_before_lease_for_test();
        let group = child.group();
        let (announcement_sender, announcements) = mpsc::sync_channel(MAX_PENDING_ANNOUNCEMENTS);
        let announcement_overflow = Arc::new(AtomicBool::new(false));
        let mut owned = Self {
            child: Some(child),
            #[cfg(test)]
            group,
            port: 0,
            announcements,
            announcement_overflow,
            announced_port: None,
            stdout_reader: None,
            stderr_reader: None,
            stdout_tail: Vec::new(),
            stderr_tail: Vec::new(),
            retain_cleanup_failure,
            unix_socket: unix_socket.map(Path::to_path_buf),
            #[cfg(unix)]
            unix_socket_identity: None,
            #[cfg(unix)]
            service_runtime: service_runtime.cloned(),
        };
        let stdout = match owned.child_mut().stdout.take() {
            Some(stdout) => stdout,
            None => return owned.fail_start("failed to capture llama-server stdout".into()),
        };
        let stderr = match owned.child_mut().stderr.take() {
            Some(stderr) => stderr,
            None => return owned.fail_start("failed to capture llama-server stderr".into()),
        };
        owned.stdout_reader = match spawn_output_reader(stdout, None) {
            Ok(reader) => Some(reader),
            Err(error) => return owned.fail_start(error),
        };
        owned.stderr_reader = match spawn_output_reader(
            stderr,
            unix_socket.is_none().then(|| {
                (
                    announcement_sender,
                    Arc::clone(&owned.announcement_overflow),
                )
            }),
        ) {
            Ok(reader) => Some(reader),
            Err(error) => return owned.fail_start(error),
        };
        let child_pid = owned.child.as_ref().expect("owned child is present").id();
        if let Some(runtime) = owned
            .child
            .as_mut()
            .and_then(ChildProcessGuard::runtime_mut)
        {
            let publication = publication.expect("owned runtime publication is present");
            if let Err(error) = runtime.record(
                child_pid,
                group,
                &launch.id,
                requested_port,
                launch.managed_source_server(),
                publication,
            ) {
                return owned.fail_start(error);
            }
        }
        if launch.policy != LaunchPolicy::Foreground {
            match requested_start_outcome(&mut owned, &stop)? {
                RequestedStartOutcome::Continue => {}
                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                RequestedStartOutcome::CleanupFailed => {
                    return Ok(StartOutcome::CleanupFailed(Box::new(owned)))
                }
            }
        }
        owned.wait_until_ready(
            launch,
            timeout,
            requested_port,
            unix_socket,
            service_runtime,
            client.as_ref(),
            child_pid,
            &stop,
        )
    }
}

#[cfg(test)]
fn kill_owner_after_spawn_before_lease_for_test() {
    let (Some(after), Some(ready)) = (
        std::env::var_os("LOXA_TEST_POST_SPAWN_KILL_AFTER"),
        std::env::var_os("LOXA_TEST_POST_SPAWN_KILL_READY"),
    ) else {
        return;
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !Path::new(&after).is_file() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        Path::new(&after).is_file(),
        "post-spawn child witness was not published"
    );
    std::fs::write(ready, b"prelease").expect("pre-lease kill witness could not be published");
    // SAFETY: this test-only subprocess deliberately models abrupt owner death.
    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    unreachable!("SIGKILL returned in the pre-lease owner subprocess");
}
