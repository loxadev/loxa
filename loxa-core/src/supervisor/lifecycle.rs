use super::state::{self, ManagedRun, ManagedRunIdentity, RunLifecycle};
use super::{SupervisorError, TeardownConfirmation};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservedChildExit {
    RequestedStop,
    Interrupted,
    Restart { run: ManagedRun },
    Exhausted { log_tail: String },
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnexpectedExitPolicy {
    RestartOnce,
    FailFast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerTeardownDecision {
    RequestedStop,
    Interrupted,
    UnexpectedExit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerTerminalOutcome {
    RequestedStop,
    Interrupted,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostSpawnCleanupOutcome {
    Cleaned,
    RequestedStop,
    RecoveryRequired,
}

#[derive(Debug)]
pub enum SpawnStartingRunOutcome<T> {
    Spawned { run: ManagedRun, value: T },
    RequestedStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildlessFinishOutcome {
    Finished,
    RequestedStop,
}

pub trait InterruptStatus {
    fn interrupted(&self) -> bool;
}

pub(super) fn spawn_starting_run_with<T, F>(
    path: &Path,
    expected: &ManagedRunIdentity,
    spawn: F,
) -> Result<SpawnStartingRunOutcome<T>, SupervisorError>
where
    F: FnOnce() -> Result<T, SupervisorError>,
{
    let lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} is no longer present before spawn",
            expected.run_id, expected.generation
        )));
    };
    if current.identity() != *expected {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} no longer matches before spawn",
            expected.run_id, expected.generation
        )));
    }
    if current.child_pid.is_some()
        || current.child_process_start_time_unix_s.is_some()
        || current.child_pgid.is_some()
    {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} is not childless before spawn",
            expected.run_id, expected.generation
        )));
    }

    if current.stop_requested {
        state::write_runtime_state(path, &[])?;
        return Ok(SpawnStartingRunOutcome::RequestedStop);
    }

    let run = current.clone();
    let spawned = spawn();
    drop(lock);
    spawned.map(|value| SpawnStartingRunOutcome::Spawned { run, value })
}

pub fn finish_childless_runtime_state_run(
    path: &Path,
    expected: &ManagedRunIdentity,
) -> Result<ChildlessFinishOutcome, SupervisorError> {
    let _lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Err(SupervisorError::RunStateConflict(format!(
            "childless managed run {} generation {} is no longer present",
            expected.run_id, expected.generation
        )));
    };
    if current.identity() != *expected {
        return Err(SupervisorError::RunStateConflict(format!(
            "childless managed run {} generation {} no longer matches",
            expected.run_id, expected.generation
        )));
    }
    if current.child_pid.is_some()
        || current.child_process_start_time_unix_s.is_some()
        || current.child_pgid.is_some()
    {
        return Err(SupervisorError::RunStateConflict(format!(
            "managed run {} generation {} is not childless at terminal transition",
            expected.run_id, expected.generation
        )));
    }
    let outcome = if current.stop_requested {
        ChildlessFinishOutcome::RequestedStop
    } else {
        ChildlessFinishOutcome::Finished
    };
    state::write_runtime_state(path, &[])?;
    Ok(outcome)
}

pub fn finish_owner_teardown_with<T>(
    path: &Path,
    expected: &ManagedRunIdentity,
    decision: OwnerTeardownDecision,
    teardown: T,
) -> Result<OwnerTerminalOutcome, SupervisorError>
where
    T: FnOnce(OwnerTeardownDecision) -> TeardownConfirmation,
{
    if teardown(decision) == TeardownConfirmation::Unconfirmed {
        return Ok(OwnerTerminalOutcome::RecoveryRequired);
    }

    finish_confirmed_owner_teardown(path, expected, decision)
}

pub fn finish_post_spawn_failure_with<T>(
    path: &Path,
    expected: &ManagedRunIdentity,
    teardown: T,
) -> Result<PostSpawnCleanupOutcome, SupervisorError>
where
    T: FnOnce() -> TeardownConfirmation,
{
    if teardown() == TeardownConfirmation::Unconfirmed {
        return Ok(PostSpawnCleanupOutcome::RecoveryRequired);
    }

    let _lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Ok(PostSpawnCleanupOutcome::RecoveryRequired);
    };
    if current.identity() != *expected {
        return Ok(PostSpawnCleanupOutcome::RecoveryRequired);
    }
    let requested_stop = current.stop_requested;
    state::write_runtime_state(path, &[])?;
    Ok(if requested_stop {
        PostSpawnCleanupOutcome::RequestedStop
    } else {
        PostSpawnCleanupOutcome::Cleaned
    })
}

fn finish_confirmed_owner_teardown(
    path: &Path,
    expected: &ManagedRunIdentity,
    decision: OwnerTeardownDecision,
) -> Result<OwnerTerminalOutcome, SupervisorError> {
    let _lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Ok(OwnerTerminalOutcome::RecoveryRequired);
    };
    if current.identity() != *expected {
        return Ok(OwnerTerminalOutcome::RecoveryRequired);
    }
    let stop_requested = current.stop_requested;
    state::write_runtime_state(path, &[])?;

    Ok(
        if stop_requested || decision == OwnerTeardownDecision::RequestedStop {
            OwnerTerminalOutcome::RequestedStop
        } else if decision == OwnerTeardownDecision::Interrupted {
            OwnerTerminalOutcome::Interrupted
        } else {
            OwnerTerminalOutcome::RecoveryRequired
        },
    )
}

pub fn decide_observed_child_exit<I, T>(
    log_tail: String,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    teardown: T,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    T: FnOnce(OwnerTeardownDecision) -> TeardownConfirmation,
{
    decide_observed_child_exit_with_policy(
        log_tail,
        state_path,
        state_identity,
        interrupt,
        UnexpectedExitPolicy::RestartOnce,
        teardown,
    )
}

pub fn decide_observed_child_exit_with_policy<I, T>(
    log_tail: String,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    policy: UnexpectedExitPolicy,
    teardown: T,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    T: FnOnce(OwnerTeardownDecision) -> TeardownConfirmation,
{
    decide_observed_child_exit_with(
        log_tail,
        state_path,
        state_identity,
        interrupt,
        policy,
        || {},
        teardown,
    )
}

fn decide_observed_child_exit_with<I, H, T>(
    log_tail: String,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    policy: UnexpectedExitPolicy,
    after_observation: H,
    teardown: T,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    H: FnOnce(),
    T: FnOnce(OwnerTeardownDecision) -> TeardownConfirmation,
{
    after_observation();
    decide_observed_child_exit_with_diagnostics_and_policy(
        state_path,
        state_identity,
        interrupt,
        policy,
        || teardown(OwnerTeardownDecision::UnexpectedExit),
        || Ok(log_tail),
    )
}

#[cfg(test)]
fn decide_observed_child_exit_with_diagnostics<I, T, D>(
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    teardown: T,
    diagnostics: D,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    T: FnOnce() -> TeardownConfirmation,
    D: FnOnce() -> Result<String, SupervisorError>,
{
    decide_observed_child_exit_with_diagnostics_and_policy(
        state_path,
        state_identity,
        interrupt,
        UnexpectedExitPolicy::RestartOnce,
        teardown,
        diagnostics,
    )
}

pub(super) fn decide_observed_child_exit_with_diagnostics_and_policy<I, T, D>(
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    policy: UnexpectedExitPolicy,
    teardown: T,
    diagnostics: D,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    T: FnOnce() -> TeardownConfirmation,
    D: FnOnce() -> Result<String, SupervisorError>,
{
    if teardown() == TeardownConfirmation::Unconfirmed {
        return Ok(ObservedChildExit::RecoveryRequired);
    }
    let log_tail = diagnostics()
        .unwrap_or_else(|error| format!("crash diagnostics unavailable after teardown: {error}"));
    transition_after_confirmed_unexpected_exit(
        state_path,
        state_identity,
        log_tail,
        interrupt,
        policy,
    )
}

fn transition_after_confirmed_unexpected_exit<I: InterruptStatus>(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    log_tail: String,
    interrupt: &I,
    policy: UnexpectedExitPolicy,
) -> Result<ObservedChildExit, SupervisorError> {
    let _lock = state::acquire_runtime_state_lock_for_mutation(
        state_path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let mut runs = state::runtime_state_runs_for_mutation(state_path)?;
    let Some(current) = runs.first() else {
        return Ok(ObservedChildExit::RecoveryRequired);
    };
    if current.identity() != *expected {
        return Ok(ObservedChildExit::RecoveryRequired);
    }
    if current.stop_requested {
        state::write_runtime_state(state_path, &[])?;
        return Ok(ObservedChildExit::RequestedStop);
    }
    if interrupt.interrupted() {
        state::write_runtime_state(state_path, &[])?;
        return Ok(ObservedChildExit::Interrupted);
    }

    match current.generation {
        0 if policy == UnexpectedExitPolicy::FailFast => {
            state::write_runtime_state(state_path, &[])?;
            Ok(ObservedChildExit::Exhausted { log_tail })
        }
        0 => {
            let mut replacement = current.clone();
            replacement.lifecycle = RunLifecycle::Starting;
            replacement.generation = 1;
            replacement.generation_alias = format!("loxa-{}-g1", current.run_id);
            replacement.child_pid = None;
            replacement.child_process_start_time_unix_s = None;
            replacement.child_pgid = None;
            runs[0] = replacement.clone();
            state::write_runtime_state(state_path, &runs)?;
            Ok(ObservedChildExit::Restart { run: replacement })
        }
        1 => {
            state::write_runtime_state(state_path, &[])?;
            Ok(ObservedChildExit::Exhausted { log_tail })
        }
        _ => Ok(ObservedChildExit::RecoveryRequired),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::state::{
        create_starting_run, read_runtime_state, record_stop_request,
        record_stop_request_with_lock_options_and_hook, write_runtime_state, ManagedRun,
        RunLifecycle, RuntimeStateRead, StopRequestMatch, RUNTIME_STATE_SCHEMA_VERSION,
    };
    use crate::supervisor::{
        persist_managed_server_or_cleanup, LogDrainingChild, ManagedChild, ManagedServer,
        PersistManagedServerOutcome,
    };
    use std::cell::{Cell, RefCell};
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    fn managed_run_for(server: &ManagedServer) -> ManagedRun {
        ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: format!("test-run-{}", server.pid),
            model_id: server.id.clone(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Running,
            generation: 0,
            generation_alias: format!("loxa-test-run-{}-g0", server.pid),
            port: server.port,
            log_path: PathBuf::from(format!("/tmp/test-run-{}.log", server.pid)),
            child_pid: Some(server.pid),
            child_process_start_time_unix_s: server.process_start_time_unix_s,
            child_pgid: None,
        }
    }

    fn childless_starting_run(root: &Path, run_id: &str) -> ManagedRun {
        ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: "gemma-3-4b-it-q4".to_string(),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            port: 8080,
            log_path: root.join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    #[test]
    fn late_stop_serializes_after_true_spawn_boundary_for_initial_and_replacement() {
        for generation in [0, 1] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let mut run =
                childless_starting_run(temp.path(), &format!("run-spawn-boundary-g{generation}"));
            run.generation = generation;
            run.generation_alias = format!("loxa-{}-g{generation}", run.run_id);
            create_starting_run(&state_path, run.clone()).expect("publish childless generation");

            let spawn_boundary_entered = Arc::new(Barrier::new(2));
            let stop_probe_completed = Arc::new(Barrier::new(2));
            let spawned = Arc::new(AtomicBool::new(false));
            let stop_path = state_path.clone();
            let stop_boundary = Arc::clone(&spawn_boundary_entered);
            let stop_probe = Arc::clone(&stop_probe_completed);
            let spawned_for_stop = Arc::clone(&spawned);
            let stopper = thread::spawn(move || {
                stop_boundary.wait();
                let probe = record_stop_request_with_lock_options_and_hook(
                    &stop_path,
                    "all",
                    Duration::ZERO,
                    Duration::ZERO,
                    |_| Ok(()),
                );
                assert!(
                    matches!(
                        probe,
                        Err(SupervisorError::Io(ref error))
                            if error.kind() == io::ErrorKind::WouldBlock
                    ),
                    "the stop transaction must not commit between the final decision and OS spawn: {probe:?}"
                );
                stop_probe.wait();
                record_stop_request_with_lock_options_and_hook(
                    &stop_path,
                    "all",
                    Duration::from_secs(2),
                    Duration::from_millis(1),
                    |_| {
                        assert!(
                            spawned_for_stop.load(Ordering::SeqCst),
                            "a committed late stop must serialize after OS spawn"
                        );
                        Ok(())
                    },
                )
            });

            let outcome = spawn_starting_run_with(&state_path, &run.identity(), || {
                spawn_boundary_entered.wait();
                stop_probe_completed.wait();
                spawned.store(true, Ordering::SeqCst);
                Ok(())
            })
            .expect("spawn boundary succeeds");

            assert!(matches!(
                outcome,
                SpawnStartingRunOutcome::Spawned { value: (), .. }
            ));
            assert!(matches!(
                stopper
                    .join()
                    .expect("stopper joins")
                    .expect("late stop commits after spawn"),
                StopRequestMatch::Requested(_)
            ));
            let current = state::current_runtime_state_run(&state_path, &run.identity())
                .expect("read stopped childless generation");
            assert!(current.stop_requested);
        }
    }

    #[test]
    fn attachment_returns_committed_stop_and_requested_stop_finishes_once_when_confirmed() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let starting = childless_starting_run(temp.path(), "run-1");
        create_starting_run(&state_path, starting.clone()).expect("create starting run");
        let stop_has_lock = Arc::new(Barrier::new(2));
        let release_stop = Arc::new(Barrier::new(2));
        let stop_path = state_path.clone();
        let stop_has_lock_thread = Arc::clone(&stop_has_lock);
        let release_stop_thread = Arc::clone(&release_stop);
        let stopper = thread::spawn(move || {
            record_stop_request_with_lock_options_and_hook(
                &stop_path,
                "all",
                Duration::from_secs(2),
                Duration::from_millis(1),
                |_| {
                    stop_has_lock_thread.wait();
                    release_stop_thread.wait();
                    Ok(())
                },
            )
        });
        stop_has_lock.wait();
        let attach_path = state_path.clone();
        let attach_starting = starting.clone();
        let model_path = temp.path().join("model.gguf");
        let attacher = thread::spawn(move || {
            let server = ManagedServer {
                id: attach_starting.model_id.clone(),
                pid: 777,
                port: attach_starting.port,
                model_path,
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(111),
            };
            let mut child = FakeChild::default();
            persist_managed_server_or_cleanup(
                &mut child,
                &attach_path,
                attach_starting,
                server,
                Duration::from_millis(10),
            )
        });
        release_stop.wait();
        assert!(matches!(
            stopper
                .join()
                .expect("stopper joins")
                .expect("stop transaction"),
            StopRequestMatch::Requested(_)
        ));
        let attached = attacher
            .join()
            .expect("attacher joins")
            .expect("attachment succeeds");
        let PersistManagedServerOutcome::Attached(attached) = attached else {
            panic!("concurrent stop is merged into the attached run");
        };
        assert!(
            attached.stop_requested,
            "attachment returns committed state"
        );
        let decisions = Cell::new(0_u8);

        let outcome = finish_owner_teardown_with(
            &state_path,
            &attached.identity(),
            OwnerTeardownDecision::RequestedStop,
            |decision| {
                assert_eq!(decision, OwnerTeardownDecision::RequestedStop);
                decisions.set(decisions.get() + 1);
                TeardownConfirmation::Confirmed
            },
        )
        .expect("finish requested stop");

        assert_eq!(outcome, OwnerTerminalOutcome::RequestedStop);
        assert_eq!(decisions.get(), 1);
        assert_eq!(
            read_runtime_state(&state_path).expect("read finished state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn requested_stop_unconfirmed_teardown_preserves_full_state_for_recovery() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let mut run = managed_run_for(&server);
        run.stop_requested = true;
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed stopped run");

        let outcome = finish_owner_teardown_with(
            &state_path,
            &run.identity(),
            OwnerTeardownDecision::RequestedStop,
            |_| TeardownConfirmation::Unconfirmed,
        )
        .expect("recovery outcome");

        assert_eq!(outcome, OwnerTerminalOutcome::RecoveryRequired);
        assert_eq!(
            read_runtime_state(&state_path).expect("read preserved state"),
            RuntimeStateRead::Loaded(vec![run])
        );
    }

    #[test]
    fn one_unexpected_exit_advances_same_run_to_childless_generation_one_then_attaches() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let generation_zero = managed_run_for(&server);
        write_runtime_state(&state_path, std::slice::from_ref(&generation_zero))
            .expect("seed generation zero");
        let decisions = RefCell::new(Vec::new());

        let decision = decide_observed_child_exit_with(
            "first crash".to_string(),
            &state_path,
            &generation_zero.identity(),
            &NeverInterrupted,
            UnexpectedExitPolicy::RestartOnce,
            || {},
            |decision| {
                decisions.borrow_mut().push(decision);
                TeardownConfirmation::Confirmed
            },
        )
        .expect("restart decision");
        let ObservedChildExit::Restart {
            run: generation_one,
        } = decision
        else {
            panic!("expected generation-one restart");
        };
        assert_eq!(
            decisions.into_inner(),
            vec![OwnerTeardownDecision::UnexpectedExit]
        );
        assert_eq!(generation_one.run_id, generation_zero.run_id);
        assert_eq!(generation_one.generation, 1);
        assert_eq!(generation_one.generation_alias, "loxa-test-run-777-g1");
        assert_eq!(generation_one.lifecycle, RunLifecycle::Starting);
        assert_eq!(generation_one.child_pid, None);
        assert_eq!(generation_one.child_process_start_time_unix_s, None);
        assert_eq!(generation_one.child_pgid, None);
        assert_eq!(
            read_runtime_state(&state_path).expect("read generation one"),
            RuntimeStateRead::Loaded(vec![generation_one.clone()])
        );

        let replacement = ManagedServer {
            pid: 778,
            process_start_time_unix_s: Some(222),
            ..server
        };
        let mut child = FakeChild::default();
        let attached = persist_managed_server_or_cleanup(
            &mut child,
            &state_path,
            generation_one,
            replacement,
            Duration::from_millis(10),
        )
        .expect("attach generation-one child");
        let PersistManagedServerOutcome::Attached(attached) = attached else {
            panic!("generation-one child must attach");
        };
        assert_eq!(attached.run_id, generation_zero.run_id);
        assert_eq!(attached.generation, 1);
        assert_eq!(attached.child_pid, Some(778));
        assert_eq!(attached.child_process_start_time_unix_s, Some(222));
    }

    #[test]
    fn reaped_exit_with_concurrent_stop_confirms_unexpected_exit_once() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = managed_run_for(&server);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");
        let events = RefCell::new(Vec::new());

        let outcome = decide_observed_child_exit_with(
            "crash".to_string(),
            &state_path,
            &run.identity(),
            &NeverInterrupted,
            UnexpectedExitPolicy::RestartOnce,
            || {
                events.borrow_mut().push("exit_observed");
                assert!(matches!(
                    record_stop_request(&state_path, "all").expect("request stop at barrier"),
                    StopRequestMatch::Requested(_)
                ));
            },
            |decision| {
                assert_eq!(decision, OwnerTeardownDecision::UnexpectedExit);
                events.borrow_mut().push("reaped_exit_confirmation");
                TeardownConfirmation::Confirmed
            },
        )
        .expect("requested stop wins");

        assert_eq!(outcome, ObservedChildExit::RequestedStop);
        assert_eq!(
            events.into_inner(),
            vec!["exit_observed", "reaped_exit_confirmation"]
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read finished state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn second_unexpected_exit_exhausts_without_generation_two_and_gates_on_teardown_confirmation() {
        for confirmation in [
            TeardownConfirmation::Confirmed,
            TeardownConfirmation::Unconfirmed,
        ] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let server = ManagedServer {
                id: "gemma-3-4b-it-q4".to_string(),
                pid: 778,
                port: 8081,
                model_path: temp.path().join("model.gguf"),
                started_at_unix_s: 789,
                llama_server_version: "test".to_string(),
                process_start_time_unix_s: Some(222),
            };
            let mut generation_one = managed_run_for(&server);
            generation_one.generation = 1;
            generation_one.generation_alias = "loxa-test-run-778-g1".to_string();
            write_runtime_state(&state_path, std::slice::from_ref(&generation_one))
                .expect("seed generation one");

            let outcome = decide_observed_child_exit_with(
                "second crash".to_string(),
                &state_path,
                &generation_one.identity(),
                &NeverInterrupted,
                UnexpectedExitPolicy::RestartOnce,
                || {},
                |_| confirmation,
            )
            .expect("exhaustion outcome");

            match confirmation {
                TeardownConfirmation::Confirmed => {
                    assert_eq!(
                        outcome,
                        ObservedChildExit::Exhausted {
                            log_tail: "second crash".to_string(),
                        }
                    );
                    assert_eq!(
                        read_runtime_state(&state_path).expect("read exhausted state"),
                        RuntimeStateRead::Loaded(Vec::new())
                    );
                }
                TeardownConfirmation::Unconfirmed => {
                    assert_eq!(outcome, ObservedChildExit::RecoveryRequired);
                    assert_eq!(
                        read_runtime_state(&state_path).expect("read preserved generation one"),
                        RuntimeStateRead::Loaded(vec![generation_one])
                    );
                }
            }
        }
    }

    #[test]
    fn reaped_exit_with_concurrent_interrupt_confirms_unexpected_exit_once() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        write_runtime_state(&state_path, &[managed_run_for(&server)]).expect("seed runtime state");

        let decision = decide_observed_child_exit(
            "crash tail".to_string(),
            &state_path,
            &managed_run_for(&server).identity(),
            &AlwaysInterrupted,
            |decision| {
                assert_eq!(decision, OwnerTeardownDecision::UnexpectedExit);
                TeardownConfirmation::Confirmed
            },
        )
        .expect("child exit decision");

        assert_eq!(decision, ObservedChildExit::Interrupted);
        assert_eq!(
            read_runtime_state(&state_path).expect("runtime state after interrupt"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn interrupt_arriving_during_physical_teardown_is_sampled_at_finalization() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = managed_run_for(&server);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");
        let interrupt = MutableInterrupt(Cell::new(false));

        let outcome = decide_observed_child_exit(
            "crash tail".to_string(),
            &state_path,
            &run.identity(),
            &interrupt,
            |_| {
                interrupt.0.set(true);
                TeardownConfirmation::Confirmed
            },
        )
        .expect("finalized interrupt outcome");

        assert_eq!(outcome, ObservedChildExit::Interrupted);
        assert_eq!(
            read_runtime_state(&state_path).expect("read terminal state"),
            RuntimeStateRead::Loaded(Vec::new())
        );
    }

    #[test]
    fn unexpected_exit_orders_physical_teardown_then_diagnostics_then_finalization_sample() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 777,
            port: 8081,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 789,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(111),
        };
        let run = managed_run_for(&server);
        write_runtime_state(&state_path, std::slice::from_ref(&run)).expect("seed run");
        let events = RefCell::new(Vec::new());
        let interrupt = RecordingInterrupt(&events);

        let outcome = decide_observed_child_exit_with_diagnostics(
            &state_path,
            &run.identity(),
            &interrupt,
            || {
                events.borrow_mut().push("physical_teardown");
                TeardownConfirmation::Confirmed
            },
            || {
                events.borrow_mut().push("diagnostics");
                Ok("crash tail".to_string())
            },
        )
        .expect("restart outcome");

        assert!(matches!(outcome, ObservedChildExit::Restart { .. }));
        assert_eq!(
            events.into_inner(),
            vec![
                "physical_teardown",
                "diagnostics",
                "finalization_interrupt_sample"
            ]
        );
    }

    #[test]
    fn post_spawn_failure_finalizes_exact_state_only_after_confirmed_physical_cleanup() {
        for confirmation in [
            TeardownConfirmation::Confirmed,
            TeardownConfirmation::Unconfirmed,
        ] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let run = childless_starting_run(temp.path(), "run-1");
            create_starting_run(&state_path, run.clone()).expect("create starting run");
            let calls = Cell::new(0_u8);

            let outcome = finish_post_spawn_failure_with(&state_path, &run.identity(), || {
                calls.set(calls.get() + 1);
                confirmation
            })
            .expect("post-spawn cleanup outcome");

            assert_eq!(calls.get(), 1);
            let RuntimeStateRead::Loaded(runs) =
                read_runtime_state(&state_path).expect("read post-spawn state")
            else {
                panic!("expected loaded state");
            };
            if confirmation == TeardownConfirmation::Confirmed {
                assert_eq!(outcome, PostSpawnCleanupOutcome::Cleaned);
                assert!(runs.is_empty());
            } else {
                assert_eq!(outcome, PostSpawnCleanupOutcome::RecoveryRequired);
                assert_eq!(runs, vec![run]);
            }
        }
    }

    struct FakeChild {
        events: Vec<&'static str>,
        wait_results: Vec<Option<i32>>,
    }

    impl Default for FakeChild {
        fn default() -> Self {
            Self::with_wait_results(vec![None, Some(0)])
        }
    }

    impl FakeChild {
        fn with_wait_results(wait_results: Vec<Option<i32>>) -> Self {
            Self {
                events: Vec::new(),
                wait_results,
            }
        }
    }

    impl ManagedChild for FakeChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.push("try_wait");
            if self.wait_results.len() > 1 {
                Ok(self.wait_results.remove(0))
            } else {
                Ok(self.wait_results.first().copied().unwrap_or(Some(0)))
            }
        }
    }

    impl LogDrainingChild for FakeChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.push("join_log_drains");
            Ok(())
        }
    }

    struct NeverInterrupted;

    impl InterruptStatus for NeverInterrupted {
        fn interrupted(&self) -> bool {
            false
        }
    }

    struct AlwaysInterrupted;

    impl InterruptStatus for AlwaysInterrupted {
        fn interrupted(&self) -> bool {
            true
        }
    }

    struct MutableInterrupt(Cell<bool>);

    impl InterruptStatus for MutableInterrupt {
        fn interrupted(&self) -> bool {
            self.0.get()
        }
    }

    struct RecordingInterrupt<'a>(&'a RefCell<Vec<&'static str>>);

    impl InterruptStatus for RecordingInterrupt<'_> {
        fn interrupted(&self) -> bool {
            self.0.borrow_mut().push("finalization_interrupt_sample");
            false
        }
    }
}
