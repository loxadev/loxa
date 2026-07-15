use crate::engine_session::EngineSession;
use crate::model_lifecycle::{
    EngineLifecycleDriver, ExactSessionStatus, GatewayPublisher, LaunchPlan, LifecycleError,
    LifecycleSignals, SessionCorrelation, StableNodeOwner, StartedSession,
};
use loxa_core::engine::{EngineLaunchSpec, ReadinessStrategy};
use loxa_core::gateway::{EngineTarget, GatewayState};
use loxa_core::supervisor::{self, ManagedChild, ManagedServer, RunLifecycle};
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct ProductionEngineDriver {
    state_path: PathBuf,
    logs_dir: PathBuf,
    gateway_port: u16,
}

impl ProductionEngineDriver {
    pub(crate) fn new(state_path: PathBuf, logs_dir: PathBuf, gateway_port: u16) -> Self {
        Self {
            state_path,
            logs_dir,
            gateway_port,
        }
    }

    fn public_error(error: impl std::fmt::Display) -> LifecycleError {
        LifecycleError::StartFailed(error.to_string())
    }

    fn unloaded_run(
        &self,
        run: &loxa_core::supervisor::ManagedRun,
    ) -> loxa_core::supervisor::ManagedRun {
        let mut unloaded = run.clone();
        unloaded.model_id = None;
        unloaded.lifecycle = RunLifecycle::Unloaded;
        unloaded.port = self.gateway_port;
        unloaded.child_pid = None;
        unloaded.child_process_start_time_unix_s = None;
        unloaded.child_pgid = None;
        unloaded
    }

    fn exact_teardown_to_unloaded<C>(
        &self,
        child: &mut C,
        run: &loxa_core::supervisor::ManagedRun,
    ) -> Result<(), LifecycleError>
    where
        C: supervisor::ManagedChild + supervisor::LogDrainingChild,
    {
        let recovery = |reason: String| LifecycleError::RecoveryRequired {
            replacement: "engine lifecycle cleanup was not proven".into(),
            rollback: reason,
        };
        let confirmation =
            supervisor::teardown_managed_child(child, supervisor::CTRL_C_GRACE_PERIOD)
                .map_err(|error| recovery(error.to_string()))?;
        if confirmation != supervisor::TeardownConfirmation::Confirmed {
            return Err(recovery("engine teardown could not be confirmed".into()));
        }
        self.commit_unloaded(run, recovery)
    }

    fn commit_unloaded(
        &self,
        run: &loxa_core::supervisor::ManagedRun,
        recovery: impl Fn(String) -> LifecycleError,
    ) -> Result<(), LifecycleError> {
        supervisor::update_runtime_state_run_committed(
            &self.state_path,
            &run.identity(),
            self.unloaded_run(run),
        )
        .map_err(|error| recovery(error.to_string()))?
        .ok_or_else(|| recovery("engine generation changed during cleanup".into()))?;
        Ok(())
    }

    fn reconcile_childless_owner(
        &self,
        owner: &StableNodeOwner,
        starting: &loxa_core::supervisor::ManagedRun,
    ) -> Result<(), LifecycleError> {
        match supervisor::read_runtime_state(&self.state_path).map_err(Self::public_error)? {
            supervisor::RuntimeStateRead::Loaded(runs) if runs.is_empty() => {
                supervisor::create_starting_run(&self.state_path, self.unloaded_run(starting))
                    .map_err(Self::public_error)?;
                Ok(())
            }
            supervisor::RuntimeStateRead::Loaded(runs)
                if runs.first().is_some_and(|run| {
                    run.identity() == starting.identity()
                        && run.run_id == owner.run_id
                        && run.owner_pid == owner.pid
                        && run.owner_process_start_time_unix_s == owner.process_start_time_unix_s
                        && run.child_pid.is_none()
                }) =>
            {
                supervisor::update_runtime_state_run_committed(
                    &self.state_path,
                    &starting.identity(),
                    self.unloaded_run(starting),
                )
                .map_err(Self::public_error)?
                .ok_or_else(|| LifecycleError::RecoveryRequired {
                    replacement: "childless engine start failed".into(),
                    rollback: "stable owner changed during reconciliation".into(),
                })?;
                Ok(())
            }
            _ => Err(LifecycleError::RecoveryRequired {
                replacement: "childless engine start failed".into(),
                rollback: "stable owner could not be reconciled safely".into(),
            }),
        }
    }
}

impl EngineLifecycleDriver for ProductionEngineDriver {
    type Session = EngineSession<supervisor::SpawnedServer>;

    fn start(
        &mut self,
        owner: &StableNodeOwner,
        plan: &LaunchPlan,
        generation: u64,
    ) -> Result<StartedSession<Self::Session>, LifecycleError> {
        let generation = u32::try_from(generation)
            .map_err(|_| LifecycleError::StartFailed("engine generation overflow".into()))?;
        let state = supervisor::read_runtime_state(&self.state_path).map_err(Self::public_error)?;
        let loxa_core::supervisor::RuntimeStateRead::Loaded(runs) = state else {
            return Err(LifecycleError::StartFailed(
                "stable node owner state is unavailable".into(),
            ));
        };
        let current = runs
            .into_iter()
            .next()
            .filter(|run| {
                run.run_id == owner.run_id
                    && run.owner_pid == owner.pid
                    && run.owner_process_start_time_unix_s == owner.process_start_time_unix_s
                    && run.control_port == Some(self.gateway_port)
                    && run.child_pid.is_none()
                    && run.lifecycle == RunLifecycle::Unloaded
            })
            .ok_or_else(|| {
                LifecycleError::StartFailed("stable node owner is not safely unloaded".into())
            })?;
        if current.stop_requested {
            return Err(LifecycleError::Stopping);
        }
        let reservation = supervisor::reserve_localhost_port(None).map_err(Self::public_error)?;
        let engine_port = reservation.port();
        let alias = format!("loxa-{}-g{generation}", owner.run_id);
        let program = supervisor::detect_llama_server().map_err(Self::public_error)?;
        let version = supervisor::llama_server_version(&program).map_err(Self::public_error)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(Self::public_error)?
            .as_secs();
        let log_path = self
            .logs_dir
            .join(format!("{}-{engine_port}-{now}.log", plan.model_id));
        let mut starting = current.clone();
        starting.model_id = Some(plan.model_id.clone());
        starting.lifecycle = RunLifecycle::Starting;
        starting.generation = generation;
        starting.generation_alias = alias.clone();
        starting.port = engine_port;
        starting.log_path = log_path.clone();
        starting.child_pid = None;
        starting.child_process_start_time_unix_s = None;
        starting.child_pgid = None;
        let starting = supervisor::update_runtime_state_run_committed(
            &self.state_path,
            &current.identity(),
            starting,
        )
        .map_err(Self::public_error)?
        .ok_or_else(|| LifecycleError::StartFailed("stable node generation changed".into()))?;
        if starting.stop_requested {
            self.reconcile_childless_owner(owner, &starting)?;
            return Err(LifecycleError::Stopping);
        }
        let spec = EngineLaunchSpec {
            program,
            args: vec![
                OsString::from("--model"),
                plan.artifact_path.as_os_str().to_owned(),
                OsString::from("--alias"),
                OsString::from(&alias),
                OsString::from("--host"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from(engine_port.to_string()),
                OsString::from("--ctx-size"),
                OsString::from(supervisor::DEFAULT_CTX_TOKENS.to_string()),
                OsString::from("--gpu-layers"),
                OsString::from("auto"),
                OsString::from("--flash-attn"),
                OsString::from("auto"),
                OsString::from("--metrics"),
                OsString::from("--log-disable"),
            ],
            port: engine_port,
            engine_name: "llama.cpp".into(),
            engine_version: version.clone(),
            runtime_model: plan.artifact_path.display().to_string(),
            upstream_model: alias.clone(),
            readiness: ReadinessStrategy::LlamaModelAlias {
                expected_alias: alias.clone(),
            },
        };
        let spawn = match supervisor::spawn_starting_engine(
            &self.state_path,
            &starting.identity(),
            &spec,
            &log_path,
            reservation,
        ) {
            Ok(spawn) => spawn,
            Err(error) => {
                self.reconcile_childless_owner(owner, &starting)?;
                return Err(Self::public_error(error));
            }
        };
        let supervisor::SpawnStartingRunOutcome::Spawned {
            run: starting,
            value: mut child,
        } = spawn
        else {
            self.reconcile_childless_owner(owner, &starting)?;
            return Err(LifecycleError::Stopping);
        };
        if let Some(error) = child.take_initialization_error() {
            self.exact_teardown_to_unloaded(&mut child, &starting)?;
            return Err(Self::public_error(error));
        }
        let child_pid = child.pid();
        let child_start = match supervisor::process_start_time_with_retry(child_pid) {
            Some(start) => start,
            None => {
                self.exact_teardown_to_unloaded(&mut child, &starting)?;
                return Err(LifecycleError::StartFailed(
                    "engine process identity unavailable".into(),
                ));
            }
        };
        let server = ManagedServer {
            id: plan.model_id.clone(),
            pid: child_pid,
            port: engine_port,
            model_path: plan.artifact_path.clone(),
            started_at_unix_s: now,
            llama_server_version: version.clone(),
            process_start_time_unix_s: Some(child_start),
        };
        let mut running = starting.clone();
        running.model_id = Some(server.id.clone());
        running.lifecycle = RunLifecycle::Running;
        running.child_pid = Some(server.pid);
        running.child_process_start_time_unix_s = server.process_start_time_unix_s;
        running.child_pgid = child.owned_pgid();
        let run = match supervisor::update_runtime_state_run_committed(
            &self.state_path,
            &starting.identity(),
            running,
        ) {
            Ok(Some(run)) if !run.stop_requested => run,
            Ok(Some(run)) => {
                self.exact_teardown_to_unloaded(&mut child, &run)?;
                return Err(LifecycleError::Stopping);
            }
            Ok(None) => {
                self.exact_teardown_to_unloaded(&mut child, &starting)?;
                return Err(LifecycleError::Stopping);
            }
            Err(error) => {
                self.exact_teardown_to_unloaded(&mut child, &starting)?;
                return Err(Self::public_error(error));
            }
        };
        let committed_run = run.clone();
        let session =
            match EngineSession::new(child, run, server, "llama-server", child_pid, child_start) {
                Ok(session) => session,
                Err((mut child, error)) => {
                    self.exact_teardown_to_unloaded(&mut child, &committed_run)?;
                    return Err(LifecycleError::StartFailed(error.to_string()));
                }
            };
        Ok(StartedSession {
            correlation: SessionCorrelation {
                generation: u64::from(generation),
                child_pid,
                child_process_start_time_unix_s: child_start,
                server_id: format!("engine-{child_pid}-{child_start}"),
                model_id: plan.model_id.clone(),
                port: engine_port,
                committed_run_id: owner.run_id.clone(),
                owner_pid: owner.pid,
                owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                gateway_port: owner.gateway_port,
                generation_alias: alias,
                engine_version: version,
            },
            value: session,
        })
    }

    fn wait_ready(
        &mut self,
        session: &mut StartedSession<Self::Session>,
        signals: LifecycleSignals<'_>,
    ) -> Result<(), LifecycleError> {
        let readiness = ReadinessStrategy::LlamaModelAlias {
            expected_alias: session.correlation.generation_alias.clone(),
        };
        let started = std::time::Instant::now();
        while started.elapsed() < supervisor::HEALTH_TIMEOUT {
            if signals.stop_requested() {
                return Err(LifecycleError::Stopping);
            }
            match supervisor::wait_for_engine_ready_or_exit(
                session.value.child_mut(),
                session.correlation.port,
                &readiness,
                Duration::from_millis(250),
                Duration::from_millis(25),
            ) {
                Ok(()) => return Ok(()),
                Err(supervisor::SupervisorError::HealthTimeout) => {}
                Err(error) => return Err(LifecycleError::ReadinessFailed(error.to_string())),
            }
        }
        Err(LifecycleError::ReadinessFailed(
            "engine readiness timed out".into(),
        ))
    }

    fn stop_exact(&mut self, session: StartedSession<Self::Session>) -> Result<(), LifecycleError> {
        let (mut child, run, _, _) = session.value.into_parts();
        let confirmation =
            supervisor::teardown_managed_child(&mut child, supervisor::CTRL_C_GRACE_PERIOD)
                .map_err(|error| LifecycleError::TeardownFailed(error.to_string()))?;
        if confirmation != supervisor::TeardownConfirmation::Confirmed {
            return Err(LifecycleError::TeardownFailed(
                "engine teardown could not be confirmed".into(),
            ));
        }
        let mut unloaded = run.clone();
        unloaded.model_id = None;
        unloaded.lifecycle = RunLifecycle::Unloaded;
        unloaded.port = self.gateway_port;
        unloaded.child_pid = None;
        unloaded.child_process_start_time_unix_s = None;
        unloaded.child_pgid = None;
        supervisor::update_runtime_state_run_committed(&self.state_path, &run.identity(), unloaded)
            .map_err(|error| LifecycleError::TeardownFailed(error.to_string()))?
            .ok_or_else(|| LifecycleError::TeardownFailed("engine generation changed".into()))?;
        Ok(())
    }

    fn poll_exact(
        &mut self,
        session: &mut StartedSession<Self::Session>,
    ) -> Result<ExactSessionStatus, LifecycleError> {
        session
            .value
            .child_mut()
            .try_wait()
            .map(|status| {
                if status.is_some() {
                    ExactSessionStatus::Exited
                } else {
                    ExactSessionStatus::Running
                }
            })
            .map_err(|error| LifecycleError::TeardownFailed(error.to_string()))
    }

    fn finish_unexpected_exit(
        &mut self,
        session: StartedSession<Self::Session>,
    ) -> Result<bool, LifecycleError> {
        let owner_run_id = session.correlation.committed_run_id.clone();
        self.stop_exact(session)?;
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&self.state_path).map_err(Self::public_error)?
        else {
            return Err(LifecycleError::TeardownFailed(
                "stable owner state unavailable after unexpected exit".into(),
            ));
        };
        let current = runs
            .into_iter()
            .next()
            .filter(|run| run.run_id == owner_run_id && run.lifecycle == RunLifecycle::Unloaded)
            .ok_or_else(|| {
                LifecycleError::TeardownFailed(
                    "stable owner was not restored after unexpected exit".into(),
                )
            })?;
        Ok(!current.stop_requested)
    }

    fn mark_recovery_required(&mut self, owner: &StableNodeOwner) -> Result<(), LifecycleError> {
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&self.state_path).map_err(Self::public_error)?
        else {
            return Err(LifecycleError::TeardownFailed(
                "stable owner state unavailable for recovery marker".into(),
            ));
        };
        let current = runs
            .into_iter()
            .next()
            .filter(|run| {
                run.run_id == owner.run_id
                    && run.owner_pid == owner.pid
                    && run.owner_process_start_time_unix_s == owner.process_start_time_unix_s
            })
            .ok_or_else(|| {
                LifecycleError::TeardownFailed(
                    "stable owner identity changed before recovery marker".into(),
                )
            })?;
        let mut recovery = current.clone();
        recovery.lifecycle = RunLifecycle::RecoveryRequired;
        supervisor::update_runtime_state_run_committed(
            &self.state_path,
            &current.identity(),
            recovery,
        )
        .map_err(Self::public_error)?
        .ok_or_else(|| {
            LifecycleError::TeardownFailed(
                "stable owner generation changed before recovery marker".into(),
            )
        })?;
        Ok(())
    }
}

pub(crate) struct ProductionGatewayPublisher(pub(crate) GatewayState);

impl GatewayPublisher for ProductionGatewayPublisher {
    fn withdraw(&mut self) {
        self.0.withdraw();
    }

    fn publish(&mut self, plan: &LaunchPlan, session: &SessionCorrelation) {
        self.0.publish(EngineTarget {
            base_url: format!("http://127.0.0.1:{}", session.port),
            backend_alias: session.generation_alias.clone(),
            engine: plan.engine.clone(),
            engine_version: session.engine_version.clone(),
            model_id: plan.model_id.clone(),
            profile: format!("{}:{}", plan.engine, plan.model_id),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::process::{Command, Stdio};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new(label: &str) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "loxa-production-lifecycle-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn starting(owner: &StableNodeOwner, generation: u32) -> supervisor::ManagedRun {
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: owner.run_id.clone(),
            model_id: Some("model".into()),
            owner_pid: owner.pid,
            owner_process_start_time_unix_s: owner.process_start_time_unix_s,
            stop_requested: false,
            lifecycle: RunLifecycle::Starting,
            generation,
            generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
            control_port: Some(owner.gateway_port),
            port: 9000,
            log_path: PathBuf::from("engine.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    #[test]
    fn childless_start_failure_restores_same_owner_as_unloaded() {
        let dir = TestDir::new("restore");
        let state_path = dir.0.join("managed.json");
        let owner = StableNodeOwner {
            run_id: "owner".into(),
            pid: 11,
            process_start_time_unix_s: 22,
            gateway_port: 8080,
        };
        let run = starting(&owner, 3);
        supervisor::create_starting_run(&state_path, run.clone()).unwrap();
        let driver = ProductionEngineDriver::new(state_path.clone(), dir.0.clone(), 8080);
        driver.reconcile_childless_owner(&owner, &run).unwrap();
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).unwrap()
        else {
            panic!("owner state loaded")
        };
        assert_eq!(runs[0].run_id, "owner");
        assert_eq!(runs[0].generation, 3);
        assert_eq!(runs[0].lifecycle, RunLifecycle::Unloaded);
        assert_eq!(runs[0].port, 8080);
    }

    #[test]
    fn confirmed_childless_spawn_cleanup_recreates_same_owner() {
        let dir = TestDir::new("recreate");
        let state_path = dir.0.join("managed.json");
        let owner = StableNodeOwner {
            run_id: "owner".into(),
            pid: 11,
            process_start_time_unix_s: 22,
            gateway_port: 8080,
        };
        let run = starting(&owner, 4);
        supervisor::create_starting_run(&state_path, run.clone()).unwrap();
        supervisor::finish_runtime_state_run(&state_path, &run.identity()).unwrap();
        let driver = ProductionEngineDriver::new(state_path.clone(), dir.0.clone(), 8080);
        driver.reconcile_childless_owner(&owner, &run).unwrap();
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).unwrap()
        else {
            panic!("owner state loaded")
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].lifecycle, RunLifecycle::Unloaded);
        assert_eq!(runs[0].generation, 4);
    }

    #[test]
    fn committed_running_identity_is_used_to_restore_unloaded_owner() {
        let dir = TestDir::new("running-cleanup");
        let state_path = dir.0.join("managed.json");
        let owner = StableNodeOwner {
            run_id: "owner".into(),
            pid: 11,
            process_start_time_unix_s: 22,
            gateway_port: 8080,
        };
        let childless = starting(&owner, 5);
        supervisor::create_starting_run(&state_path, childless.clone()).unwrap();
        let mut run = childless.clone();
        run.lifecycle = RunLifecycle::Running;
        run.child_pid = Some(77);
        run.child_process_start_time_unix_s = Some(88);
        let run =
            supervisor::update_runtime_state_run_committed(&state_path, &childless.identity(), run)
                .unwrap()
                .unwrap();
        let driver = ProductionEngineDriver::new(state_path.clone(), dir.0.clone(), 8080);
        driver
            .commit_unloaded(&run, |reason| LifecycleError::RecoveryRequired {
                replacement: "test".into(),
                rollback: reason,
            })
            .unwrap();
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).unwrap()
        else {
            panic!("loaded")
        };
        assert_eq!(runs[0].lifecycle, RunLifecycle::Unloaded);
        assert_eq!(runs[0].child_pid, None);
        assert_eq!(runs[0].generation, 5);
    }

    #[test]
    fn recovery_marker_preserves_stable_owner_and_control_endpoint() {
        let dir = TestDir::new("recovery-marker");
        let state_path = dir.0.join("managed.json");
        let owner = StableNodeOwner {
            run_id: "owner".into(),
            pid: 11,
            process_start_time_unix_s: 22,
            gateway_port: 8080,
        };
        let mut run = starting(&owner, 6);
        run.model_id = None;
        run.lifecycle = RunLifecycle::Unloaded;
        run.port = owner.gateway_port;
        supervisor::create_starting_run(&state_path, run).unwrap();
        let mut driver = ProductionEngineDriver::new(state_path.clone(), dir.0.clone(), 8080);

        driver.mark_recovery_required(&owner).unwrap();

        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).unwrap()
        else {
            panic!("recovery state loaded")
        };
        assert_eq!(runs[0].run_id, owner.run_id);
        assert_eq!(runs[0].control_port, Some(owner.gateway_port));
        assert_eq!(runs[0].lifecycle, RunLifecycle::RecoveryRequired);
    }

    #[cfg(unix)]
    #[test]
    fn real_exact_child_exit_is_reaped_and_cas_restores_unloaded_with_stop_priority() {
        let dir = TestDir::new("real-exit-composition");
        let state_path = dir.0.join("managed.json");
        let owner = StableNodeOwner {
            run_id: "owner".into(),
            pid: 11,
            process_start_time_unix_s: 22,
            gateway_port: 8080,
        };
        let mut unloaded = starting(&owner, 1);
        unloaded.model_id = None;
        unloaded.lifecycle = RunLifecycle::Unloaded;
        unloaded.port = owner.gateway_port;
        supervisor::create_starting_run(&state_path, unloaded.clone()).unwrap();
        let child = Command::new("sh")
            .args([
                "-c",
                "printf 'stdout-drain-evidence\\n'; printf 'stderr-drain-evidence\\n' >&2; exit 0",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let child_pid = child.id();
        let child_start = 333;
        let mut running = unloaded.clone();
        running.model_id = Some("model".into());
        running.lifecycle = RunLifecycle::Running;
        running.port = 9001;
        running.log_path = dir.0.join("engine.log");
        running.child_pid = Some(child_pid);
        running.child_process_start_time_unix_s = Some(child_start);
        running.stop_requested = true;
        let running = supervisor::update_runtime_state_run_committed(
            &state_path,
            &unloaded.identity(),
            running,
        )
        .unwrap()
        .unwrap();
        let server = ManagedServer {
            id: "model".into(),
            pid: child_pid,
            port: 9001,
            model_path: PathBuf::from("model.gguf"),
            started_at_unix_s: 1,
            llama_server_version: "test".into(),
            process_start_time_unix_s: Some(child_start),
        };
        let session = EngineSession::new(
            supervisor::SpawnedServer::from_debug_child_for_composition_test(
                child,
                &running.log_path,
            )
            .unwrap(),
            running.clone(),
            server,
            "test-child",
            child_pid,
            child_start,
        );
        let session = match session {
            Ok(session) => session,
            Err(_) => panic!("exact child session must correlate"),
        };
        let mut started = StartedSession {
            value: session,
            correlation: SessionCorrelation {
                generation: 1,
                child_pid,
                child_process_start_time_unix_s: child_start,
                server_id: "child".into(),
                model_id: "model".into(),
                port: 9001,
                committed_run_id: owner.run_id.clone(),
                owner_pid: owner.pid,
                owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                gateway_port: owner.gateway_port,
                generation_alias: format!("loxa-{}-g1", owner.run_id),
                engine_version: "test".into(),
            },
        };
        let mut driver = ProductionEngineDriver::new(state_path.clone(), dir.0.clone(), 8080);
        for _ in 0..100 {
            if driver.poll_exact(&mut started).unwrap() == ExactSessionStatus::Exited {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            driver.poll_exact(&mut started).unwrap(),
            ExactSessionStatus::Exited
        );

        assert!(!driver.finish_unexpected_exit(started).unwrap());
        let supervisor::RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).unwrap()
        else {
            panic!("stable owner remains")
        };
        assert_eq!(runs[0].lifecycle, RunLifecycle::Unloaded);
        assert_eq!(runs[0].child_pid, None);
        assert_eq!(runs[0].port, owner.gateway_port);
        assert!(runs[0].stop_requested);
        let log = std::fs::read_to_string(&running.log_path).unwrap();
        assert!(log.contains("stdout-drain-evidence"));
        assert!(log.contains("stderr-drain-evidence"));
    }
}
