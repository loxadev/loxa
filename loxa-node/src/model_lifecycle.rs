use crate::actor::MutationCancellation;
use loxa_core::model_inventory::{ArtifactState, VerifiedRecipeInventoryEntry};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub const PUBLIC_MODEL_ALIAS: &str = "loxa";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StableNodeOwner {
    pub run_id: String,
    pub pid: u32,
    pub process_start_time_unix_s: u64,
    pub gateway_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaunchPlan {
    pub model_id: String,
    pub artifact_path: std::path::PathBuf,
    pub engine: String,
}

impl LaunchPlan {
    pub fn from_verified_inventory(
        entry: &VerifiedRecipeInventoryEntry,
        models_dir: &std::path::Path,
    ) -> Result<Self, LifecycleError> {
        if entry.artifact != ArtifactState::Downloaded {
            return Err(LifecycleError::ModelNotVerified);
        }
        if !entry.compatibility.compatible {
            return Err(LifecycleError::Incompatible(
                entry.compatibility.reason.clone(),
            ));
        }
        if !entry.engine.eligible {
            return Err(LifecycleError::EngineIneligible(
                entry.engine.reason.clone(),
            ));
        }
        if entry.engine.engine != "llama-cpp" {
            return Err(LifecycleError::EngineIneligible(format!(
                "unsupported managed engine {}",
                entry.engine.engine
            )));
        }
        Ok(Self {
            model_id: entry.id.clone(),
            artifact_path: models_dir.join(&entry.filename),
            engine: entry.engine.engine.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionCorrelation {
    pub generation: u64,
    pub child_pid: u32,
    pub child_process_start_time_unix_s: u64,
    pub server_id: String,
    pub model_id: String,
    pub port: u16,
    pub committed_run_id: String,
    pub owner_pid: u32,
    pub owner_process_start_time_unix_s: u64,
    pub gateway_port: u16,
    pub generation_alias: String,
    pub engine_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartedSession<S> {
    pub value: S,
    pub correlation: SessionCorrelation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeLifecycleStatus {
    Unloaded,
    Loading,
    Ready,
    Unloading,
    RecoveryRequired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleSnapshot {
    pub status: NodeLifecycleStatus,
    pub active_model_id: Option<String>,
    pub operation_id: Option<String>,
    pub generation: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    ModelNotVerified,
    Incompatible(String),
    EngineIneligible(String),
    Cancelled,
    CancellationNotSafe,
    Stopping,
    InvalidCandidate(String),
    StartFailed(String),
    ReadinessFailed(String),
    TeardownFailed(String),
    RecoveryRequired {
        replacement: String,
        rollback: String,
    },
}

pub trait EngineLifecycleDriver {
    type Session;

    fn start(
        &mut self,
        owner: &StableNodeOwner,
        plan: &LaunchPlan,
        generation: u64,
    ) -> Result<StartedSession<Self::Session>, LifecycleError>;

    fn wait_ready(
        &mut self,
        session: &mut StartedSession<Self::Session>,
        signals: LifecycleSignals<'_>,
    ) -> Result<(), LifecycleError>;

    fn stop_exact(&mut self, session: StartedSession<Self::Session>) -> Result<(), LifecycleError>;
}

#[derive(Clone, Copy)]
pub struct LifecycleSignals<'a> {
    cancellation: &'a MutationCancellation,
    stopping: &'a AtomicBool,
}

impl LifecycleSignals<'_> {
    pub fn stop_requested(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    pub fn cancellation_requested(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

pub trait GatewayPublisher {
    fn withdraw(&mut self);
    /// The production gateway implements publication as an infallible,
    /// in-process atomic target swap; no network or process work occurs here.
    fn publish(&mut self, plan: &LaunchPlan, session: &SessionCorrelation);
}

pub struct ModelLifecycle<D, G>
where
    D: EngineLifecycleDriver,
    G: GatewayPublisher,
{
    owner: StableNodeOwner,
    driver: D,
    gateway: G,
    current: Option<(LaunchPlan, StartedSession<D::Session>)>,
    generation: u64,
    status: NodeLifecycleStatus,
    error: Option<String>,
    stopping: Arc<AtomicBool>,
    destructive_commit: CancellationBoundary,
}

#[derive(Clone)]
pub(crate) struct CancellationBoundary(Arc<Mutex<bool>>);

impl CancellationBoundary {
    pub(crate) fn try_cancel(&self, cancel: impl FnOnce()) -> bool {
        let committed = self.0.lock().expect("cancellation boundary poisoned");
        if *committed {
            false
        } else {
            cancel();
            true
        }
    }

    fn set(&self, committed: bool) {
        *self.0.lock().expect("cancellation boundary poisoned") = committed;
    }

    fn is_safe(&self) -> bool {
        !*self.0.lock().expect("cancellation boundary poisoned")
    }
}

impl<D, G> ModelLifecycle<D, G>
where
    D: EngineLifecycleDriver,
    G: GatewayPublisher,
{
    pub fn new(owner: StableNodeOwner, driver: D, gateway: G) -> Self {
        Self {
            owner,
            driver,
            gateway,
            current: None,
            generation: 0,
            status: NodeLifecycleStatus::Unloaded,
            error: None,
            stopping: Arc::new(AtomicBool::new(false)),
            destructive_commit: CancellationBoundary(Arc::new(Mutex::new(false))),
        }
    }

    pub fn stop_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stopping)
    }

    pub fn request_stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    pub fn cancellation_is_safe(&self) -> bool {
        self.destructive_commit.is_safe()
    }

    pub(crate) fn complete_operation(&self) {
        self.destructive_commit.set(false);
    }

    pub(crate) fn destructive_commit_token(&self) -> CancellationBoundary {
        self.destructive_commit.clone()
    }

    pub fn snapshot(&self) -> LifecycleSnapshot {
        LifecycleSnapshot {
            status: self.status.clone(),
            active_model_id: self.current.as_ref().map(|(plan, _)| plan.model_id.clone()),
            operation_id: None,
            generation: self.generation,
            error: self.error.clone(),
        }
    }

    pub fn load(
        &mut self,
        plan: LaunchPlan,
        cancellation: &MutationCancellation,
    ) -> Result<(), LifecycleError> {
        self.ensure_precommit(cancellation)?;
        if self
            .current
            .as_ref()
            .is_some_and(|(active, _)| active.model_id == plan.model_id)
        {
            return Ok(());
        }

        let prior_plan = self.current.as_ref().map(|(prior, _)| prior.clone());
        self.status = NodeLifecycleStatus::Loading;
        self.error = None;
        self.gateway.withdraw();
        self.destructive_commit.set(true);

        if let Some((_, prior_session)) = self.current.take() {
            if let Err(error) = self.driver.stop_exact(prior_session) {
                self.require_recovery(format!("exact prior-generation teardown failed: {error:?}"));
                return Err(error);
            }
        }
        if let Err(error) = self.ensure_not_stopping() {
            self.status = NodeLifecycleStatus::Unloaded;
            return Err(error);
        }

        match self.start_ready(&plan, cancellation) {
            Ok(session) => {
                self.publish(plan, session);
                Ok(())
            }
            Err(replacement_error) => {
                let Some(prior_plan) = prior_plan else {
                    if matches!(replacement_error, LifecycleError::RecoveryRequired { .. }) {
                        self.require_recovery(format!("{replacement_error:?}"));
                    } else {
                        self.status = NodeLifecycleStatus::Unloaded;
                        self.error = Some(format!("{replacement_error:?}"));
                    }
                    return Err(replacement_error);
                };
                match self.start_ready(&prior_plan, cancellation) {
                    Ok(session) => {
                        self.publish(prior_plan, session);
                        Err(replacement_error)
                    }
                    Err(rollback_error) => {
                        self.status = NodeLifecycleStatus::RecoveryRequired;
                        self.error = Some(format!(
                            "replacement failed: {replacement_error:?}; rollback failed: {rollback_error:?}"
                        ));
                        Err(LifecycleError::RecoveryRequired {
                            replacement: format!("{replacement_error:?}"),
                            rollback: format!("{rollback_error:?}"),
                        })
                    }
                }
            }
        }
    }

    pub fn unload(&mut self, cancellation: &MutationCancellation) -> Result<(), LifecycleError> {
        self.ensure_precommit(cancellation)?;
        self.status = NodeLifecycleStatus::Unloading;
        self.error = None;
        self.gateway.withdraw();
        self.destructive_commit.set(true);
        if let Some((_, session)) = self.current.take() {
            if let Err(error) = self.driver.stop_exact(session) {
                self.require_recovery(format!(
                    "exact active-generation teardown failed: {error:?}"
                ));
                return Err(error);
            }
        }
        self.status = NodeLifecycleStatus::Unloaded;
        Ok(())
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), LifecycleError> {
        self.request_stop();
        self.gateway.withdraw();
        self.destructive_commit.set(true);
        if let Some((_, session)) = self.current.take() {
            if let Err(error) = self.driver.stop_exact(session) {
                self.require_recovery(format!("node-stop teardown failed: {error:?}"));
                return Err(error);
            }
        }
        self.status = NodeLifecycleStatus::Unloaded;
        self.destructive_commit.set(false);
        Ok(())
    }

    fn ensure_precommit(&self, cancellation: &MutationCancellation) -> Result<(), LifecycleError> {
        self.ensure_not_stopping()?;
        if cancellation.is_cancelled() {
            return Err(LifecycleError::Cancelled);
        }
        Ok(())
    }

    fn ensure_not_stopping(&self) -> Result<(), LifecycleError> {
        if self.stopping.load(Ordering::SeqCst) {
            Err(LifecycleError::Stopping)
        } else {
            Ok(())
        }
    }

    fn start_ready(
        &mut self,
        plan: &LaunchPlan,
        cancellation: &MutationCancellation,
    ) -> Result<StartedSession<D::Session>, LifecycleError> {
        self.ensure_not_stopping()?;
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| LifecycleError::InvalidCandidate("engine generation overflow".into()))?;
        let mut session = self.driver.start(&self.owner, plan, generation)?;
        self.generation = generation;
        if let Err(error) = validate_candidate(&self.owner, plan, generation, &session.correlation)
        {
            return match self.driver.stop_exact(session) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(LifecycleError::RecoveryRequired {
                    replacement: format!("{error:?}"),
                    rollback: format!("invalid-candidate cleanup failed: {cleanup:?}"),
                }),
            };
        }
        if let Err(error) = self.driver.wait_ready(
            &mut session,
            LifecycleSignals {
                cancellation,
                stopping: &self.stopping,
            },
        ) {
            return match self.driver.stop_exact(session) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(LifecycleError::RecoveryRequired {
                    replacement: format!("{error:?}"),
                    rollback: format!("candidate cleanup failed: {cleanup:?}"),
                }),
            };
        }
        if let Err(error) = self.ensure_not_stopping() {
            return match self.driver.stop_exact(session) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(LifecycleError::RecoveryRequired {
                    replacement: format!("{error:?}"),
                    rollback: format!("stop-race cleanup failed: {cleanup:?}"),
                }),
            };
        }
        Ok(session)
    }

    fn require_recovery(&mut self, error: String) {
        self.status = NodeLifecycleStatus::RecoveryRequired;
        self.error = Some(error);
    }

    fn publish(&mut self, plan: LaunchPlan, session: StartedSession<D::Session>) {
        self.gateway.publish(&plan, &session.correlation);
        self.status = NodeLifecycleStatus::Ready;
        self.error = None;
        self.current = Some((plan, session));
    }
}

fn validate_candidate(
    owner: &StableNodeOwner,
    plan: &LaunchPlan,
    generation: u64,
    candidate: &SessionCorrelation,
) -> Result<(), LifecycleError> {
    let valid = candidate.generation == generation
        && candidate.child_pid != 0
        && candidate.child_process_start_time_unix_s != 0
        && !candidate.server_id.is_empty()
        && candidate.model_id == plan.model_id
        && candidate.port != 0
        && candidate.committed_run_id == owner.run_id
        && candidate.owner_pid == owner.pid
        && candidate.owner_process_start_time_unix_s == owner.process_start_time_unix_s
        && candidate.gateway_port == owner.gateway_port
        && candidate.generation_alias == format!("loxa-{}-g{generation}", owner.run_id);
    if valid {
        Ok(())
    } else {
        Err(LifecycleError::InvalidCandidate(
            "spawned engine correlation does not match the committed node run".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[derive(Default)]
    struct FakeDriver {
        outcomes: VecDeque<Result<(), LifecycleError>>,
        stop_outcomes: VecDeque<Result<(), LifecycleError>>,
        started: Vec<(String, u64)>,
        stopped: Vec<SessionCorrelation>,
        stop_during_ready: bool,
        cancel_during_ready: bool,
    }

    impl EngineLifecycleDriver for FakeDriver {
        type Session = ();

        fn start(
            &mut self,
            owner: &StableNodeOwner,
            plan: &LaunchPlan,
            generation: u64,
        ) -> Result<StartedSession<Self::Session>, LifecycleError> {
            self.started.push((plan.model_id.clone(), generation));
            Ok(StartedSession {
                value: (),
                correlation: SessionCorrelation {
                    generation,
                    child_pid: 100 + generation as u32,
                    child_process_start_time_unix_s: 200 + generation,
                    server_id: format!("server-{generation}"),
                    model_id: plan.model_id.clone(),
                    port: 9000 + generation as u16,
                    committed_run_id: owner.run_id.clone(),
                    owner_pid: owner.pid,
                    owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                    gateway_port: owner.gateway_port,
                    generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                    engine_version: "test".into(),
                },
            })
        }

        fn wait_ready(
            &mut self,
            _: &mut StartedSession<Self::Session>,
            signals: LifecycleSignals<'_>,
        ) -> Result<(), LifecycleError> {
            if self.stop_during_ready {
                signals.stopping.store(true, Ordering::SeqCst);
            }
            if self.cancel_during_ready {
                signals.cancellation.cancel();
            }
            self.outcomes.pop_front().unwrap_or(Ok(()))
        }

        fn stop_exact(
            &mut self,
            session: StartedSession<Self::Session>,
        ) -> Result<(), LifecycleError> {
            self.stopped.push(session.correlation);
            self.stop_outcomes.pop_front().unwrap_or(Ok(()))
        }
    }

    #[derive(Default)]
    struct FakeGateway(Vec<String>);

    impl GatewayPublisher for FakeGateway {
        fn withdraw(&mut self) {
            self.0.push("withdraw".into());
        }
        fn publish(&mut self, plan: &LaunchPlan, session: &SessionCorrelation) {
            self.0.push(format!(
                "publish:{}:{}:{}",
                plan.model_id, PUBLIC_MODEL_ALIAS, session.port
            ));
        }
    }

    fn owner() -> StableNodeOwner {
        StableNodeOwner {
            run_id: "stable-owner".into(),
            pid: 10,
            process_start_time_unix_s: 20,
            gateway_port: 8080,
        }
    }

    fn plan(id: &str) -> LaunchPlan {
        LaunchPlan {
            model_id: id.into(),
            artifact_path: format!("{id}.gguf").into(),
            engine: "llama.cpp".into(),
        }
    }

    #[test]
    fn launch_plan_requires_authoritative_verified_compatible_engine_eligible_inventory() {
        let mut entry = VerifiedRecipeInventoryEntry {
            id: "a".into(),
            repo: "owner/repo".into(),
            revision: "pinned".into(),
            filename: "a.gguf".into(),
            sha256: "00".repeat(32),
            size_bytes: 4,
            license: "apache-2.0".into(),
            params: "tiny".into(),
            quant: "Q4".into(),
            min_free_mem_gb: 0.1,
            artifact: ArtifactState::Downloaded,
            compatibility: loxa_core::model_inventory::Compatibility {
                compatible: true,
                reason: "compatible".into(),
            },
            engine: loxa_core::model_inventory::EngineEligibility {
                engine: "llama-cpp".into(),
                eligible: true,
                reason: "eligible".into(),
            },
        };
        assert_eq!(
            LaunchPlan::from_verified_inventory(&entry, std::path::Path::new("models"))
                .unwrap()
                .artifact_path,
            std::path::PathBuf::from("models/a.gguf")
        );
        entry.artifact = ArtifactState::Partial { bytes: 2 };
        assert_eq!(
            LaunchPlan::from_verified_inventory(&entry, std::path::Path::new("models")),
            Err(LifecycleError::ModelNotVerified)
        );
        entry.artifact = ArtifactState::Downloaded;
        entry.engine.engine = "future-engine".into();
        assert!(matches!(
            LaunchPlan::from_verified_inventory(&entry, std::path::Path::new("models")),
            Err(LifecycleError::EngineIneligible(reason)) if reason.contains("future-engine")
        ));
    }

    #[test]
    fn publishes_only_after_readiness_and_keeps_public_alias_stable() {
        let mut lifecycle =
            ModelLifecycle::new(owner(), FakeDriver::default(), FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        assert_eq!(lifecycle.snapshot().active_model_id.as_deref(), Some("a"));
        assert_eq!(lifecycle.gateway.0, ["withdraw", "publish:a:loxa:9001"]);
    }

    #[test]
    fn switch_tears_down_exact_prior_generation_and_rolls_back_once() {
        let driver = FakeDriver {
            outcomes: VecDeque::from([
                Ok(()),
                Err(LifecycleError::ReadinessFailed("bad".into())),
                Ok(()),
            ]),
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        assert!(matches!(
            lifecycle.load(plan("b"), &MutationCancellation::new()),
            Err(LifecycleError::ReadinessFailed(_))
        ));
        assert_eq!(lifecycle.snapshot().active_model_id.as_deref(), Some("a"));
        assert_eq!(lifecycle.driver.stopped.len(), 2);
        assert_eq!(lifecycle.driver.stopped[0].generation, 1);
        assert_eq!(lifecycle.driver.stopped[1].generation, 2);
        assert_eq!(
            lifecycle.driver.started,
            [("a".into(), 1), ("b".into(), 2), ("a".into(), 3)]
        );
    }

    #[test]
    fn failed_replacement_and_single_rollback_requires_recovery() {
        let driver = FakeDriver {
            outcomes: VecDeque::from([
                Ok(()),
                Err(LifecycleError::ReadinessFailed("new".into())),
                Err(LifecycleError::ReadinessFailed("old".into())),
            ]),
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        assert!(matches!(
            lifecycle.load(plan("b"), &MutationCancellation::new()),
            Err(LifecycleError::RecoveryRequired { .. })
        ));
        assert_eq!(
            lifecycle.snapshot().status,
            NodeLifecycleStatus::RecoveryRequired
        );
        assert_eq!(lifecycle.driver.started.len(), 3);
    }

    #[test]
    fn unload_preserves_owner_and_gateway_but_reaps_exact_generation() {
        let mut lifecycle =
            ModelLifecycle::new(owner(), FakeDriver::default(), FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        lifecycle.unload(&MutationCancellation::new()).unwrap();
        assert_eq!(lifecycle.snapshot().status, NodeLifecycleStatus::Unloaded);
        assert_eq!(lifecycle.owner.run_id, "stable-owner");
        assert_eq!(lifecycle.driver.stopped[0].generation, 1);
        assert_eq!(lifecycle.gateway.0.last().unwrap(), "withdraw");
    }

    #[test]
    fn cancellation_is_safe_only_before_withdraw_and_stop_outranks_load() {
        let mut lifecycle =
            ModelLifecycle::new(owner(), FakeDriver::default(), FakeGateway::default());
        let cancelled = MutationCancellation::new();
        cancelled.cancel();
        assert_eq!(
            lifecycle.load(plan("a"), &cancelled),
            Err(LifecycleError::Cancelled)
        );
        assert!(lifecycle.gateway.0.is_empty());
        lifecycle.request_stop();
        assert_eq!(
            lifecycle.load(plan("a"), &MutationCancellation::new()),
            Err(LifecycleError::Stopping)
        );
    }

    #[test]
    fn rejects_stale_or_mismatched_candidate_before_publish() {
        struct BadDriver;
        impl EngineLifecycleDriver for BadDriver {
            type Session = ();
            fn start(
                &mut self,
                owner: &StableNodeOwner,
                plan: &LaunchPlan,
                generation: u64,
            ) -> Result<StartedSession<()>, LifecycleError> {
                Ok(StartedSession {
                    value: (),
                    correlation: SessionCorrelation {
                        generation: generation - 1,
                        child_pid: 1,
                        child_process_start_time_unix_s: 2,
                        server_id: "server".into(),
                        model_id: plan.model_id.clone(),
                        port: 9,
                        committed_run_id: owner.run_id.clone(),
                        owner_pid: owner.pid,
                        owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                        gateway_port: owner.gateway_port,
                        generation_alias: format!("loxa-{}-g{}", owner.run_id, generation - 1),
                        engine_version: "test".into(),
                    },
                })
            }
            fn wait_ready(
                &mut self,
                _: &mut StartedSession<()>,
                _: LifecycleSignals<'_>,
            ) -> Result<(), LifecycleError> {
                Ok(())
            }
            fn stop_exact(&mut self, _: StartedSession<()>) -> Result<(), LifecycleError> {
                Ok(())
            }
        }
        let mut lifecycle = ModelLifecycle::new(owner(), BadDriver, FakeGateway::default());
        assert!(matches!(
            lifecycle.load(plan("a"), &MutationCancellation::new()),
            Err(LifecycleError::InvalidCandidate(_))
        ));
        assert!(lifecycle
            .gateway
            .0
            .iter()
            .all(|event| !event.starts_with("publish")));
    }

    #[test]
    fn stop_during_readiness_reaps_candidate_and_never_publishes() {
        let driver = FakeDriver {
            stop_during_ready: true,
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        assert_eq!(
            lifecycle.load(plan("a"), &MutationCancellation::new()),
            Err(LifecycleError::Stopping)
        );
        assert_eq!(lifecycle.driver.stopped.len(), 1);
        assert!(lifecycle
            .gateway
            .0
            .iter()
            .all(|event| !event.starts_with("publish")));
    }

    #[test]
    fn cleanup_failure_fails_closed_as_recovery_required() {
        let driver = FakeDriver {
            outcomes: VecDeque::from([Err(LifecycleError::ReadinessFailed("bad".into()))]),
            stop_outcomes: VecDeque::from([Err(LifecycleError::TeardownFailed("stuck".into()))]),
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        assert!(matches!(
            lifecycle.load(plan("a"), &MutationCancellation::new()),
            Err(LifecycleError::RecoveryRequired { .. })
        ));
        assert_eq!(
            lifecycle.snapshot().status,
            NodeLifecycleStatus::RecoveryRequired
        );
    }

    #[test]
    fn unload_teardown_failure_fails_closed_as_recovery_required() {
        let driver = FakeDriver {
            stop_outcomes: VecDeque::from([Err(LifecycleError::TeardownFailed("stuck".into()))]),
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        assert!(matches!(
            lifecycle.unload(&MutationCancellation::new()),
            Err(LifecycleError::TeardownFailed(_))
        ));
        assert_eq!(
            lifecycle.snapshot().status,
            NodeLifecycleStatus::RecoveryRequired
        );
    }

    #[test]
    fn node_shutdown_with_ready_model_withdraws_and_reaps_exact_generation() {
        let mut lifecycle =
            ModelLifecycle::new(owner(), FakeDriver::default(), FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        lifecycle.shutdown().unwrap();
        assert_eq!(lifecycle.snapshot().status, NodeLifecycleStatus::Unloaded);
        assert_eq!(lifecycle.driver.stopped.len(), 1);
        assert_eq!(lifecycle.driver.stopped[0].generation, 1);
        assert_eq!(lifecycle.gateway.0.last().unwrap(), "withdraw");
    }

    #[test]
    fn switch_does_not_spawn_replacement_when_prior_exact_teardown_fails() {
        let driver = FakeDriver {
            stop_outcomes: VecDeque::from([Err(LifecycleError::TeardownFailed("stuck".into()))]),
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        lifecycle
            .load(plan("a"), &MutationCancellation::new())
            .unwrap();
        assert!(matches!(
            lifecycle.load(plan("b"), &MutationCancellation::new()),
            Err(LifecycleError::TeardownFailed(_))
        ));
        assert_eq!(lifecycle.driver.started, [("a".into(), 1)]);
        assert_eq!(
            lifecycle.snapshot().status,
            NodeLifecycleStatus::RecoveryRequired
        );
    }

    #[test]
    fn invalid_candidate_cleanup_failure_fails_closed_as_recovery_required() {
        struct BadCleanupDriver;
        impl EngineLifecycleDriver for BadCleanupDriver {
            type Session = ();
            fn start(
                &mut self,
                owner: &StableNodeOwner,
                plan: &LaunchPlan,
                generation: u64,
            ) -> Result<StartedSession<()>, LifecycleError> {
                Ok(StartedSession {
                    value: (),
                    correlation: SessionCorrelation {
                        generation: generation.saturating_sub(1),
                        child_pid: 1,
                        child_process_start_time_unix_s: 2,
                        server_id: "stale".into(),
                        model_id: plan.model_id.clone(),
                        port: 9,
                        committed_run_id: owner.run_id.clone(),
                        owner_pid: owner.pid,
                        owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                        gateway_port: owner.gateway_port,
                        generation_alias: "stale".into(),
                        engine_version: "test".into(),
                    },
                })
            }
            fn wait_ready(
                &mut self,
                _: &mut StartedSession<()>,
                _: LifecycleSignals<'_>,
            ) -> Result<(), LifecycleError> {
                Ok(())
            }
            fn stop_exact(&mut self, _: StartedSession<()>) -> Result<(), LifecycleError> {
                Err(LifecycleError::TeardownFailed("stuck".into()))
            }
        }
        let mut lifecycle = ModelLifecycle::new(owner(), BadCleanupDriver, FakeGateway::default());
        assert!(matches!(
            lifecycle.load(plan("a"), &MutationCancellation::new()),
            Err(LifecycleError::RecoveryRequired { .. })
        ));
        assert_eq!(
            lifecycle.snapshot().status,
            NodeLifecycleStatus::RecoveryRequired
        );
    }

    #[test]
    fn cancellation_after_destructive_commit_does_not_fake_cancel_a_ready_engine() {
        let driver = FakeDriver {
            cancel_during_ready: true,
            ..FakeDriver::default()
        };
        let mut lifecycle = ModelLifecycle::new(owner(), driver, FakeGateway::default());
        let cancellation = MutationCancellation::new();
        lifecycle.load(plan("a"), &cancellation).unwrap();
        assert!(cancellation.is_cancelled());
        assert_eq!(lifecycle.snapshot().status, NodeLifecycleStatus::Ready);
        assert_eq!(lifecycle.snapshot().active_model_id.as_deref(), Some("a"));
        assert!(!lifecycle.cancellation_is_safe());
        lifecycle.complete_operation();
        assert!(lifecycle.cancellation_is_safe());
    }
}
