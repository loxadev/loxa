use super::state::{self, ManagedRun, ManagedRunIdentity, RunLifecycle};
use super::{SupervisorError, TeardownConfirmation};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservedChildExit {
    RequestedStop,
    Interrupted,
    Restart { run: ManagedRun },
    Exhausted { log_tail: String },
    RecoveryRequired,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareUnloadedOwnerOutcome {
    Prepared(ManagedRun),
    RequestedStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedOwnerCleanup {
    Childless,
    ConfirmedReaped,
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreUnloadedOwnerOutcome {
    Restored(ManagedRun),
    RequestedStop,
    RecoveryRequired,
}

pub trait InterruptStatus {
    fn interrupted(&self) -> bool;
}

pub fn prepare_unloaded_owner_for_model(
    path: &Path,
    expected_baseline: &ManagedRun,
    model_id: String,
    engine_port: u16,
    log_path: PathBuf,
) -> Result<PrepareUnloadedOwnerOutcome, SupervisorError> {
    validate_unloaded_owner_baseline(expected_baseline)?;
    let _lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let mut runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Err(SupervisorError::RunStateConflict(format!(
            "unloaded managed owner {} is no longer present",
            expected_baseline.run_id
        )));
    };
    if !managed_runs_match_except_monotonic_stop(current, expected_baseline) {
        return Err(SupervisorError::RunStateConflict(format!(
            "unloaded managed owner {} no longer matches its full baseline",
            expected_baseline.run_id
        )));
    }
    if current.stop_requested {
        return Ok(PrepareUnloadedOwnerOutcome::RequestedStop);
    }

    let mut prepared = current.clone();
    prepared.model_id = Some(model_id);
    prepared.lifecycle = RunLifecycle::Starting;
    prepared.port = engine_port;
    prepared.log_path = log_path;
    runs[0] = prepared.clone();
    state::write_runtime_state(path, &runs)?;
    Ok(PrepareUnloadedOwnerOutcome::Prepared(prepared))
}

pub fn restore_unloaded_owner_after_prepared_run(
    path: &Path,
    expected_current: &ManagedRun,
    expected_baseline: &ManagedRun,
    cleanup: PreparedOwnerCleanup,
) -> Result<RestoreUnloadedOwnerOutcome, SupervisorError> {
    validate_unloaded_owner_baseline(expected_baseline)?;
    validate_prepared_owner_pair(expected_current, expected_baseline)?;
    let _lock = state::acquire_runtime_state_lock_for_mutation(
        path,
        state::RUNTIME_STATE_LOCK_TIMEOUT,
        state::RUNTIME_STATE_LOCK_POLL_INTERVAL,
    )?;
    let mut runs = state::runtime_state_runs_for_mutation(path)?;
    let Some(current) = runs.first() else {
        return Ok(RestoreUnloadedOwnerOutcome::RecoveryRequired);
    };
    if !managed_runs_match_except_monotonic_stop(current, expected_current) {
        return Ok(RestoreUnloadedOwnerOutcome::RecoveryRequired);
    }

    if cleanup == PreparedOwnerCleanup::Uncertain {
        write_prepared_owner_recovery(path, &mut runs)?;
        return Ok(RestoreUnloadedOwnerOutcome::RecoveryRequired);
    }
    if cleanup == PreparedOwnerCleanup::Childless
        && (current.child_pid.is_some()
            || current.child_process_start_time_unix_s.is_some()
            || current.child_pgid.is_some())
    {
        write_prepared_owner_recovery(path, &mut runs)?;
        return Ok(RestoreUnloadedOwnerOutcome::RecoveryRequired);
    }
    if current.stop_requested {
        state::write_runtime_state(path, &[])?;
        return Ok(RestoreUnloadedOwnerOutcome::RequestedStop);
    }

    runs[0] = expected_baseline.clone();
    state::write_runtime_state(path, &runs)?;
    Ok(RestoreUnloadedOwnerOutcome::Restored(
        expected_baseline.clone(),
    ))
}

fn validate_prepared_owner_pair(
    current: &ManagedRun,
    baseline: &ManagedRun,
) -> Result<(), SupervisorError> {
    let valid = current.schema_version == state::RUNTIME_STATE_SCHEMA_VERSION
        && current.run_id == baseline.run_id
        && current.model_id.is_some()
        && current.owner_pid == baseline.owner_pid
        && current.owner_process_start_time_unix_s == baseline.owner_process_start_time_unix_s
        && current.control_port == baseline.control_port;
    if valid {
        Ok(())
    } else {
        Err(SupervisorError::RunStateConflict(format!(
            "managed run {} does not belong to its unloaded owner baseline",
            current.run_id
        )))
    }
}

fn write_prepared_owner_recovery(
    path: &Path,
    runs: &mut [ManagedRun],
) -> Result<(), SupervisorError> {
    runs[0].lifecycle = RunLifecycle::RecoveryRequired;
    state::write_runtime_state(path, runs)
}

fn validate_unloaded_owner_baseline(baseline: &ManagedRun) -> Result<(), SupervisorError> {
    let valid = baseline.schema_version == state::RUNTIME_STATE_SCHEMA_VERSION
        && baseline.model_id.is_none()
        && !baseline.stop_requested
        && baseline.lifecycle == RunLifecycle::Unloaded
        && baseline.generation == 0
        && baseline.generation_alias == format!("loxa-{}-g0", baseline.run_id)
        && baseline.control_port == Some(baseline.port)
        && !baseline.log_path.as_os_str().is_empty()
        && baseline.child_pid.is_none()
        && baseline.child_process_start_time_unix_s.is_none()
        && baseline.child_pgid.is_none();
    if valid {
        Ok(())
    } else {
        Err(SupervisorError::RunStateConflict(format!(
            "managed run {} is not an exact unloaded owner baseline",
            baseline.run_id
        )))
    }
}

fn managed_runs_match_except_monotonic_stop(current: &ManagedRun, expected: &ManagedRun) -> bool {
    let mut current = current.clone();
    current.stop_requested = expected.stop_requested;
    current == *expected
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
    decide_observed_child_exit_with(
        log_tail,
        state_path,
        state_identity,
        interrupt,
        || {},
        teardown,
    )
}

fn decide_observed_child_exit_with<I, H, T>(
    log_tail: String,
    state_path: &Path,
    state_identity: &ManagedRunIdentity,
    interrupt: &I,
    after_observation: H,
    teardown: T,
) -> Result<ObservedChildExit, SupervisorError>
where
    I: InterruptStatus,
    H: FnOnce(),
    T: FnOnce(OwnerTeardownDecision) -> TeardownConfirmation,
{
    after_observation();
    decide_observed_child_exit_with_diagnostics(
        state_path,
        state_identity,
        interrupt,
        || teardown(OwnerTeardownDecision::UnexpectedExit),
        || Ok(log_tail),
    )
}

pub(super) fn decide_observed_child_exit_with_diagnostics<I, T, D>(
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
    if teardown() == TeardownConfirmation::Unconfirmed {
        return Ok(ObservedChildExit::RecoveryRequired);
    }
    let log_tail = diagnostics()
        .unwrap_or_else(|error| format!("crash diagnostics unavailable after teardown: {error}"));
    transition_after_confirmed_unexpected_exit(state_path, state_identity, log_tail, interrupt)
}

fn transition_after_confirmed_unexpected_exit<I: InterruptStatus>(
    state_path: &Path,
    expected: &ManagedRunIdentity,
    log_tail: String,
    interrupt: &I,
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
            model_id: Some(server.id.clone()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Running,
            generation: 0,
            generation_alias: format!("loxa-test-run-{}-g0", server.pid),
            control_port: None,
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
            model_id: Some("gemma-3-4b-it-q4".to_string()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            control_port: None,
            port: 8080,
            log_path: root.join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn unloaded_owner(root: &Path, run_id: &str) -> ManagedRun {
        ManagedRun {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: None,
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            control_port: Some(8_080),
            port: 8_080,
            log_path: root.join(format!("{run_id}-unloaded.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    #[test]
    fn prepared_unloaded_owner_commits_the_exact_starting_field_table() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let baseline = unloaded_owner(temp.path(), "prepared-owner");
        write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
        let engine_log = temp.path().join("python-engine.log");

        let outcome = prepare_unloaded_owner_for_model(
            &state_path,
            &baseline,
            "mlx-community/model".to_string(),
            9_001,
            engine_log.clone(),
        )
        .expect("prepare unloaded owner");
        let PrepareUnloadedOwnerOutcome::Prepared(prepared) = outcome else {
            panic!("owner must be prepared");
        };

        assert_eq!(prepared.schema_version, baseline.schema_version);
        assert_eq!(prepared.run_id, baseline.run_id);
        assert_eq!(prepared.model_id.as_deref(), Some("mlx-community/model"));
        assert_eq!(prepared.owner_pid, baseline.owner_pid);
        assert_eq!(
            prepared.owner_process_start_time_unix_s,
            baseline.owner_process_start_time_unix_s
        );
        assert!(!prepared.stop_requested);
        assert_eq!(prepared.lifecycle, RunLifecycle::Starting);
        assert_eq!(prepared.generation, baseline.generation);
        assert_eq!(prepared.generation_alias, baseline.generation_alias);
        assert_eq!(prepared.control_port, baseline.control_port);
        assert_eq!(prepared.port, 9_001);
        assert_eq!(prepared.log_path, engine_log);
        assert_eq!(prepared.child_pid, None);
        assert_eq!(prepared.child_process_start_time_unix_s, None);
        assert_eq!(prepared.child_pgid, None);
        assert_eq!(
            read_runtime_state(&state_path).expect("read prepared owner"),
            RuntimeStateRead::Loaded(vec![prepared])
        );
    }

    #[test]
    fn preparation_rejects_full_baseline_mismatch_and_identity_cas_conflict() {
        for identity_conflict in [false, true] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let baseline = unloaded_owner(temp.path(), "prepared-conflict");
            let mut current = baseline.clone();
            if identity_conflict {
                current.generation = 1;
                current.generation_alias = "loxa-prepared-conflict-g1".to_string();
            } else {
                current.log_path = temp.path().join("different-unloaded.log");
            }
            write_runtime_state(&state_path, std::slice::from_ref(&current))
                .expect("seed conflict");

            let error = prepare_unloaded_owner_for_model(
                &state_path,
                &baseline,
                "model".to_string(),
                9_001,
                temp.path().join("engine.log"),
            )
            .expect_err("mismatched owner must fail closed");

            assert!(matches!(error, SupervisorError::RunStateConflict(_)));
            assert_eq!(
                read_runtime_state(&state_path).expect("read preserved conflict"),
                RuntimeStateRead::Loaded(vec![current])
            );
        }
    }

    #[test]
    fn preparation_observes_a_stop_race_without_clearing_or_removing_it() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let baseline = unloaded_owner(temp.path(), "stopped-before-prepare");
        write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
        record_stop_request(&state_path, "all").expect("request stop");

        let outcome = prepare_unloaded_owner_for_model(
            &state_path,
            &baseline,
            "model".to_string(),
            9_001,
            temp.path().join("engine.log"),
        )
        .expect("observe stop");

        assert!(matches!(
            outcome,
            PrepareUnloadedOwnerOutcome::RequestedStop
        ));
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read stopped owner")
        else {
            panic!("stopped owner must remain present");
        };
        assert_eq!(runs.len(), 1);
        assert!(runs[0].stop_requested);
        assert_eq!(runs[0].lifecycle, RunLifecycle::Unloaded);
    }

    #[test]
    fn prepared_owner_restores_exact_baseline_from_childless_or_confirmed_reaped_state() {
        for cleanup in [
            PreparedOwnerCleanup::Childless,
            PreparedOwnerCleanup::ConfirmedReaped,
        ] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let baseline = unloaded_owner(temp.path(), "restored-owner");
            write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
            let PrepareUnloadedOwnerOutcome::Prepared(mut prepared) =
                prepare_unloaded_owner_for_model(
                    &state_path,
                    &baseline,
                    "model".to_string(),
                    9_001,
                    temp.path().join("engine.log"),
                )
                .expect("prepare owner")
            else {
                panic!("owner must be prepared");
            };
            if cleanup == PreparedOwnerCleanup::ConfirmedReaped {
                let expected = prepared.identity();
                prepared.lifecycle = RunLifecycle::Running;
                prepared.child_pid = Some(777);
                prepared.child_process_start_time_unix_s = Some(111);
                prepared.child_pgid = Some(777);
                assert!(
                    state::update_runtime_state_run(&state_path, &expected, prepared.clone())
                        .expect("attach reaped child identity")
                );
            }

            let outcome = restore_unloaded_owner_after_prepared_run(
                &state_path,
                &prepared,
                &baseline,
                cleanup,
            )
            .expect("restore owner");

            assert_eq!(
                outcome,
                RestoreUnloadedOwnerOutcome::Restored(baseline.clone())
            );
            assert_eq!(
                read_runtime_state(&state_path).expect("read restored owner"),
                RuntimeStateRead::Loaded(vec![baseline])
            );
        }
    }

    #[test]
    fn uncertain_prepared_cleanup_writes_recovery_and_requested_stop_removes_only_when_safe() {
        for stop_requested in [false, true] {
            let temp = tempdir().expect("tempdir");
            let state_path = temp.path().join("managed.json");
            let baseline = unloaded_owner(temp.path(), "terminal-owner");
            write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
            let PrepareUnloadedOwnerOutcome::Prepared(mut prepared) =
                prepare_unloaded_owner_for_model(
                    &state_path,
                    &baseline,
                    "model".to_string(),
                    9_001,
                    temp.path().join("engine.log"),
                )
                .expect("prepare owner")
            else {
                panic!("owner must be prepared");
            };
            if stop_requested {
                record_stop_request(&state_path, "all").expect("request stop");
                prepared.stop_requested = true;
            }

            let outcome = restore_unloaded_owner_after_prepared_run(
                &state_path,
                &prepared,
                &baseline,
                if stop_requested {
                    PreparedOwnerCleanup::Childless
                } else {
                    PreparedOwnerCleanup::Uncertain
                },
            )
            .expect("terminal transition");

            if stop_requested {
                assert_eq!(outcome, RestoreUnloadedOwnerOutcome::RequestedStop);
                assert_eq!(
                    read_runtime_state(&state_path).expect("read removed stop"),
                    RuntimeStateRead::Loaded(Vec::new())
                );
            } else {
                assert_eq!(outcome, RestoreUnloadedOwnerOutcome::RecoveryRequired);
                let RuntimeStateRead::Loaded(runs) =
                    read_runtime_state(&state_path).expect("read recovery owner")
                else {
                    panic!("recovery owner must remain");
                };
                assert_eq!(runs.len(), 1);
                assert_eq!(runs[0].lifecycle, RunLifecycle::RecoveryRequired);
                assert_eq!(runs[0].run_id, prepared.run_id);
                assert_eq!(runs[0].model_id, prepared.model_id);
            }
        }
    }

    #[test]
    fn restoration_rejects_a_different_unloaded_owner_baseline() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let baseline = unloaded_owner(temp.path(), "owner-pair");
        write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
        let PrepareUnloadedOwnerOutcome::Prepared(prepared) = prepare_unloaded_owner_for_model(
            &state_path,
            &baseline,
            "model".to_string(),
            9_001,
            temp.path().join("engine.log"),
        )
        .expect("prepare owner") else {
            panic!("owner must be prepared");
        };
        let mut wrong_baseline = baseline.clone();
        wrong_baseline.owner_pid += 1;

        let error = restore_unloaded_owner_after_prepared_run(
            &state_path,
            &prepared,
            &wrong_baseline,
            PreparedOwnerCleanup::Childless,
        )
        .expect_err("different baseline owner must fail closed");

        assert!(matches!(error, SupervisorError::RunStateConflict(_)));
        assert_eq!(
            read_runtime_state(&state_path).expect("read preserved prepared owner"),
            RuntimeStateRead::Loaded(vec![prepared])
        );
    }

    #[test]
    fn contradictory_childless_cleanup_evidence_persists_recovery_required() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let baseline = unloaded_owner(temp.path(), "childless-evidence");
        write_runtime_state(&state_path, std::slice::from_ref(&baseline)).expect("seed owner");
        let PrepareUnloadedOwnerOutcome::Prepared(mut attached) = prepare_unloaded_owner_for_model(
            &state_path,
            &baseline,
            "model".to_string(),
            9_001,
            temp.path().join("engine.log"),
        )
        .expect("prepare owner") else {
            panic!("owner must be prepared");
        };
        let starting_identity = attached.identity();
        attached.lifecycle = RunLifecycle::Running;
        attached.child_pid = Some(777);
        attached.child_process_start_time_unix_s = Some(111);
        attached.child_pgid = Some(777);
        assert!(
            state::update_runtime_state_run(&state_path, &starting_identity, attached.clone())
                .expect("attach child")
        );

        let outcome = restore_unloaded_owner_after_prepared_run(
            &state_path,
            &attached,
            &baseline,
            PreparedOwnerCleanup::Childless,
        )
        .expect("record contradictory cleanup evidence");

        assert_eq!(outcome, RestoreUnloadedOwnerOutcome::RecoveryRequired);
        let RuntimeStateRead::Loaded(runs) =
            read_runtime_state(&state_path).expect("read recovery state")
        else {
            panic!("recovery row must remain");
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].lifecycle, RunLifecycle::RecoveryRequired);
        assert_eq!(runs[0].child_pid, attached.child_pid);
    }

    #[test]
    fn standalone_childless_finish_semantics_remain_unchanged() {
        let temp = tempdir().expect("tempdir");
        let state_path = temp.path().join("managed.json");
        let run = childless_starting_run(temp.path(), "standalone-run");
        create_starting_run(&state_path, run.clone()).expect("seed standalone run");

        assert_eq!(
            finish_childless_runtime_state_run(&state_path, &run.identity())
                .expect("finish standalone run"),
            ChildlessFinishOutcome::Finished
        );
        assert_eq!(
            read_runtime_state(&state_path).expect("read finished standalone run"),
            RuntimeStateRead::Loaded(Vec::new())
        );
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
                id: attach_starting.model_id.clone().expect("model id"),
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
