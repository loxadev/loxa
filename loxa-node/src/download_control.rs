use crate::actor::{
    CancelOutcome, Mutation, MutationCancellation, MutationExecutor, NodeActor, NodeActorHandle,
    SubmitError,
};
use crate::control_state::state_machine::{
    AdmissionRequest, CommittedAdmission, CurrentInstanceV1State, Transition, TransitionError,
};
use crate::control_state::{ControlStateError, ControlStateHandle};
use crate::model_lifecycle::{
    EngineLifecycleDriver, GatewayPublisher, LaunchPlan, LifecycleError, LifecycleSnapshot,
    ModelLifecycle,
};
use loxa_core::control::contracts::{
    NodeSnapshot, NodeStatus, OperationKind, OperationStatus, OperationView, ReconnectSnapshot,
};
use loxa_core::control::operations::{
    project_durable_v1_counter, project_durable_v1_operation, CancellationSafety,
    EventSubscription, OperationError, OperationStore,
};
use loxa_core::download::{self, DownloadError, DownloadObserver, DownloadProgress};
use loxa_core::model_inventory::{
    VerificationCache, VerificationCancellation, VerifiedArtifact, VerifiedRecipeInventoryEntry,
};
use loxa_core::registry::{ModelEntry, REGISTRY};
use loxa_protocol::v2::{
    DecimalU64, OperationId, V2OperationError, V2OperationErrorCode, V2OperationKind,
    V2OperationProgress, V2OperationStatus, V2PublicError,
};
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

const OPERATION_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct DownloadControl {
    authority: AdmissionAuthority,
    actor: NodeActorHandle,
    models_dir: Arc<PathBuf>,
    verification_cache: Arc<VerificationCache>,
    recipes: &'static [ModelEntry],
    lifecycle_snapshot: Option<Arc<Mutex<LifecycleSnapshot>>>,
    lifecycle_destructive: Option<crate::model_lifecycle::CancellationBoundary>,
}

#[derive(Clone)]
enum AdmissionAuthority {
    // TODO(Slice 3 Task 7): remove this production authority once NodeBuilder always supplies
    // ControlStateHandle. It remains isolated for pre-Task-7 constructors and v1 regressions.
    Legacy(Arc<Mutex<OperationStore>>),
    Durable(DurableExecutionControl),
}

#[derive(Clone)]
pub(crate) struct DurableExecutionControl {
    control_state: ControlStateHandle,
    actor: NodeActorHandle,
    lifecycle_supported: bool,
    lifecycle_destructive: Option<crate::model_lifecycle::CancellationBoundary>,
    projection_healthy: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
enum ExecutionPersistence {
    Legacy(Arc<Mutex<OperationStore>>),
    Durable(ControlStateHandle),
}

pub struct DownloadControlWorker {
    actor: NodeActorHandle,
    worker: Option<JoinHandle<()>>,
    verification: Option<VerificationWorker>,
    lifecycle_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    durable_control_state: Option<ControlStateHandle>,
}

struct VerificationWorker {
    cancellation: MutationCancellation,
    worker: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadControlError {
    Conflict,
    WriterOverloaded,
    Missing,
    Terminal,
    Stopping,
    CancellationNotSafe,
    ModelUnavailable,
}

pub(crate) struct V1EventReceiver {
    receiver: tokio::sync::mpsc::Receiver<loxa_core::control::contracts::ControlEvent>,
}

impl V1EventReceiver {
    pub(crate) async fn recv(&mut self) -> Option<loxa_core::control::contracts::ControlEvent> {
        self.receiver.recv().await
    }
}

impl DownloadControl {
    pub(crate) fn durable_execution(&self) -> Option<DurableExecutionControl> {
        match &self.authority {
            AdmissionAuthority::Durable(durable) => Some(durable.clone()),
            AdmissionAuthority::Legacy(_) => None,
        }
    }

    pub(crate) async fn start_download_async(
        &self,
        model_id: &str,
    ) -> Result<String, DownloadControlError> {
        let recipe = find_recipe(self.recipes, model_id).ok_or(DownloadControlError::Missing)?;
        match &self.authority {
            AdmissionAuthority::Legacy(_) => self.start(model_id),
            AdmissionAuthority::Durable(durable) => durable
                .start_download(model_id, recipe.size_bytes)
                .await
                .map(|admission| admission.v1_operation_id),
        }
    }

    pub(crate) async fn start_load_async(
        &self,
        model_id: &str,
    ) -> Result<String, DownloadControlError> {
        if self.lifecycle_snapshot.is_none() {
            return Err(DownloadControlError::Missing);
        }
        let entry = self
            .inventory(loxa_core::model_inventory::current_available_memory_bytes())
            .into_iter()
            .find(|entry| entry.id == model_id)
            .ok_or(DownloadControlError::Missing)?;
        LaunchPlan::from_verified_inventory(&entry, &self.models_dir)
            .map_err(|_| DownloadControlError::ModelUnavailable)?;
        match &self.authority {
            AdmissionAuthority::Legacy(_) => self.start_load(model_id),
            AdmissionAuthority::Durable(durable) => durable
                .start_load(model_id)
                .await
                .map(|admission| admission.v1_operation_id),
        }
    }

    pub(crate) async fn start_unload_async(&self) -> Result<String, DownloadControlError> {
        if self.lifecycle_snapshot.is_none() {
            return Err(DownloadControlError::Missing);
        }
        match &self.authority {
            AdmissionAuthority::Legacy(_) => self.start_unload(),
            AdmissionAuthority::Durable(durable) => durable
                .start_unload()
                .await
                .map(|admission| admission.v1_operation_id),
        }
    }

    pub(crate) async fn cancel_async(
        &self,
        v1_operation_id: &str,
    ) -> Result<OperationStatus, DownloadControlError> {
        match &self.authority {
            AdmissionAuthority::Legacy(_) => self.cancel(v1_operation_id),
            AdmissionAuthority::Durable(durable) => durable.cancel(v1_operation_id).await,
        }
    }

    pub(crate) fn operation_checked(
        &self,
        v1_operation_id: &str,
    ) -> Result<Option<OperationView>, DownloadControlError> {
        match &self.authority {
            AdmissionAuthority::Legacy(operations) => Ok(operations
                .lock()
                .expect("operation store poisoned")
                .get(v1_operation_id)),
            AdmissionAuthority::Durable(durable) => durable.v1_operation(v1_operation_id),
        }
    }

    pub(crate) async fn subscribe_v1_with_snapshot(
        &self,
        cursor: u64,
    ) -> Result<(ReconnectSnapshot, V1EventReceiver), DownloadControlError> {
        match &self.authority {
            AdmissionAuthority::Legacy(operations) => {
                let (snapshot, subscription) = operations
                    .lock()
                    .expect("operation store poisoned")
                    .subscribe_with_snapshot(cursor);
                let (sender, receiver) = tokio::sync::mpsc::channel(OPERATION_CAPACITY);
                std::thread::spawn(move || loop {
                    match subscription
                        .receiver
                        .recv_timeout(std::time::Duration::from_millis(250))
                    {
                        Ok(event) => {
                            if sender.blocking_send(event).is_err() {
                                break;
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) if sender.is_closed() => {
                            break;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                });
                Ok((snapshot, V1EventReceiver { receiver }))
            }
            AdmissionAuthority::Durable(durable) => {
                durable.subscribe_v1_with_snapshot(cursor).await
            }
        }
    }

    pub fn spawn(models_dir: PathBuf) -> (Self, DownloadControlWorker) {
        Self::spawn_with_cache(
            models_dir,
            Arc::new(VerificationCache::default()),
            REGISTRY,
            Box::new(VerifiedDownloader),
        )
    }

    fn spawn_with_cache(
        models_dir: PathBuf,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        downloader: Box<dyn ModelDownloader>,
    ) -> (Self, DownloadControlWorker) {
        let operations = Arc::new(Mutex::new(OperationStore::new(OPERATION_CAPACITY)));
        let models_dir = Arc::new(models_dir);
        let verification_cancellation = MutationCancellation::new();
        let executor = DownloadExecutor {
            models_dir: (*models_dir).clone(),
            persistence: ExecutionPersistence::Legacy(Arc::clone(&operations)),
            downloader,
            recipes,
            verification_cancellation: verification_cancellation.clone(),
            verifier: Box::new(CacheArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            }),
            lifecycle: None,
        };
        let (actor, worker) = NodeActor::spawn(executor);
        let background_cancellation = verification_cancellation.clone();
        let background_models_dir = Arc::clone(&models_dir);
        let background_cache = Arc::clone(&verification_cache);
        let verification_worker = thread::spawn(move || {
            verify_existing_recipes(
                &background_models_dir,
                recipes,
                &background_cache,
                &background_cancellation,
            );
        });
        (
            Self {
                authority: AdmissionAuthority::Legacy(operations),
                actor: actor.clone(),
                models_dir,
                verification_cache,
                recipes,
                lifecycle_snapshot: None,
                lifecycle_destructive: None,
            },
            DownloadControlWorker {
                actor,
                worker: Some(worker),
                verification: Some(VerificationWorker {
                    cancellation: verification_cancellation,
                    worker: verification_worker,
                }),
                lifecycle_stop: None,
                durable_control_state: None,
            },
        )
    }

    #[allow(dead_code)] // Task 7 consumes this at NodeBuilder's composition boundary.
    pub(crate) fn spawn_with_control_state(
        models_dir: PathBuf,
        control_state: ControlStateHandle,
    ) -> (Self, DownloadControlWorker) {
        let verification_cache = Arc::new(VerificationCache::default());
        Self::spawn_with_control_state_components(
            models_dir,
            verification_cache,
            REGISTRY,
            Box::new(VerifiedDownloader),
            control_state,
            true,
        )
    }

    fn spawn_with_control_state_components(
        models_dir: PathBuf,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        downloader: Box<dyn ModelDownloader>,
        control_state: ControlStateHandle,
        verify_existing: bool,
    ) -> (Self, DownloadControlWorker) {
        let models_dir = Arc::new(models_dir);
        let verification_cancellation = MutationCancellation::new();
        let executor = DownloadExecutor {
            models_dir: (*models_dir).clone(),
            persistence: ExecutionPersistence::Durable(control_state.clone()),
            downloader,
            recipes,
            verification_cancellation: verification_cancellation.clone(),
            verifier: Box::new(CacheArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            }),
            lifecycle: None,
        };
        let (actor, worker) = NodeActor::spawn(executor);
        let verification = verify_existing.then(|| {
            let background_cancellation = verification_cancellation.clone();
            let background_models_dir = Arc::clone(&models_dir);
            let background_cache = Arc::clone(&verification_cache);
            let worker = thread::spawn(move || {
                verify_existing_recipes(
                    &background_models_dir,
                    recipes,
                    &background_cache,
                    &background_cancellation,
                );
            });
            VerificationWorker {
                cancellation: verification_cancellation,
                worker,
            }
        });
        (
            Self {
                authority: AdmissionAuthority::Durable(DurableExecutionControl {
                    control_state: control_state.clone(),
                    actor: actor.clone(),
                    lifecycle_supported: false,
                    lifecycle_destructive: None,
                    projection_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                }),
                actor: actor.clone(),
                models_dir,
                verification_cache,
                recipes,
                lifecycle_snapshot: None,
                lifecycle_destructive: None,
            },
            DownloadControlWorker {
                actor,
                worker: Some(worker),
                verification,
                lifecycle_stop: None,
                durable_control_state: Some(control_state),
            },
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn spawn_with_lifecycle<D, G>(
        models_dir: PathBuf,
        lifecycle: ModelLifecycle<D, G>,
    ) -> (Self, DownloadControlWorker)
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
    {
        Self::spawn_with_lifecycle_components(
            models_dir,
            lifecycle,
            Arc::new(VerificationCache::default()),
            REGISTRY,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn spawn_with_lifecycle_components<D, G>(
        models_dir: PathBuf,
        lifecycle: ModelLifecycle<D, G>,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
    ) -> (Self, DownloadControlWorker)
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
    {
        let restart_verifier: Box<dyn RestartArtifactVerifier> =
            Box::new(CacheRestartArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            });
        Self::spawn_with_lifecycle_components_and_verifier(
            models_dir,
            lifecycle,
            verification_cache,
            recipes,
            restart_verifier,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn spawn_with_lifecycle_components_and_verifier<D, G>(
        models_dir: PathBuf,
        lifecycle: ModelLifecycle<D, G>,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        restart_verifier: Box<dyn RestartArtifactVerifier>,
    ) -> (Self, DownloadControlWorker)
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
    {
        Self::spawn_with_lifecycle_components_verifier_and_control_state(
            models_dir,
            lifecycle,
            verification_cache,
            recipes,
            restart_verifier,
            None,
        )
    }

    #[allow(dead_code)] // Task 7 consumes this at NodeBuilder's composition boundary.
    pub(crate) fn spawn_with_lifecycle_and_control_state<D, G>(
        models_dir: PathBuf,
        lifecycle: ModelLifecycle<D, G>,
        control_state: ControlStateHandle,
    ) -> (Self, DownloadControlWorker)
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
    {
        let verification_cache = Arc::new(VerificationCache::default());
        let restart_verifier: Box<dyn RestartArtifactVerifier> =
            Box::new(CacheRestartArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            });
        Self::spawn_with_lifecycle_components_verifier_and_control_state(
            models_dir,
            lifecycle,
            verification_cache,
            REGISTRY,
            restart_verifier,
            Some(control_state),
        )
    }

    fn spawn_with_lifecycle_components_verifier_and_control_state<D, G>(
        models_dir: PathBuf,
        lifecycle: ModelLifecycle<D, G>,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        restart_verifier: Box<dyn RestartArtifactVerifier>,
        control_state: Option<ControlStateHandle>,
    ) -> (Self, DownloadControlWorker)
    where
        D: EngineLifecycleDriver + Send + 'static,
        D::Session: Send + 'static,
        G: GatewayPublisher + Send + 'static,
    {
        let models_dir = Arc::new(models_dir);
        let verification_cancellation = MutationCancellation::new();
        let lifecycle_snapshot = Arc::new(Mutex::new(lifecycle.snapshot()));
        let lifecycle_destructive = lifecycle.destructive_commit_token();
        let lifecycle_stop = lifecycle.stop_token();
        let legacy_operations = control_state
            .is_none()
            .then(|| Arc::new(Mutex::new(OperationStore::new(OPERATION_CAPACITY))));
        let persistence = match (&control_state, &legacy_operations) {
            (Some(control_state), None) => ExecutionPersistence::Durable(control_state.clone()),
            (None, Some(operations)) => ExecutionPersistence::Legacy(Arc::clone(operations)),
            _ => unreachable!("exactly one execution authority"),
        };
        let executor = DownloadExecutor {
            models_dir: (*models_dir).clone(),
            persistence,
            downloader: Box::new(VerifiedDownloader),
            recipes,
            verification_cancellation: verification_cancellation.clone(),
            verifier: Box::new(CacheArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            }),
            lifecycle: Some(Box::new(LifecycleExecutor {
                lifecycle,
                snapshot: Arc::clone(&lifecycle_snapshot),
                models_dir: (*models_dir).clone(),
                verification_cache: Arc::clone(&verification_cache),
                recipes,
                restart_verifier,
            })),
        };
        let (actor, worker) = NodeActor::spawn(executor);
        let background_cancellation = verification_cancellation.clone();
        let background_models_dir = Arc::clone(&models_dir);
        let background_cache = Arc::clone(&verification_cache);
        let verification_worker = thread::spawn(move || {
            verify_existing_recipes(
                &background_models_dir,
                recipes,
                &background_cache,
                &background_cancellation,
            );
        });
        let durable_control_state = control_state.clone();
        let authority = match (control_state, legacy_operations) {
            (Some(control_state), None) => AdmissionAuthority::Durable(DurableExecutionControl {
                control_state,
                actor: actor.clone(),
                lifecycle_supported: true,
                lifecycle_destructive: Some(lifecycle_destructive.clone()),
                projection_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }),
            (None, Some(operations)) => AdmissionAuthority::Legacy(operations),
            _ => unreachable!("exactly one admission authority"),
        };
        (
            Self {
                authority,
                actor: actor.clone(),
                models_dir,
                verification_cache,
                recipes,
                lifecycle_snapshot: Some(lifecycle_snapshot),
                lifecycle_destructive: Some(lifecycle_destructive),
            },
            DownloadControlWorker {
                actor,
                worker: Some(worker),
                verification: Some(VerificationWorker {
                    cancellation: verification_cancellation,
                    worker: verification_worker,
                }),
                lifecycle_stop: Some(lifecycle_stop),
                durable_control_state,
            },
        )
    }

    pub fn start(&self, model_id: &str) -> Result<String, DownloadControlError> {
        if find_recipe(self.recipes, model_id).is_none() {
            return Err(DownloadControlError::Missing);
        }
        let AdmissionAuthority::Legacy(operations) = &self.authority else {
            return Err(DownloadControlError::Stopping);
        };
        let now = now_ms();
        let id = self
            .legacy_store(operations)
            .lock()
            .expect("operation store poisoned")
            .enqueue_unique(OperationKind::Download, Some(model_id.to_owned()), now)
            .map_err(map_operation_error)?;
        match self.actor.submit(
            id.clone(),
            Mutation::Download {
                model_id: model_id.to_owned(),
            },
        ) {
            Ok(()) => Ok(id),
            Err(error) => {
                let message = match error {
                    SubmitError::Conflict => "download admission conflicted",
                    SubmitError::Stopping => "node is stopping",
                };
                let _ = self
                    .legacy_store(operations)
                    .lock()
                    .expect("operation store poisoned")
                    .fail(&id, message, now_ms());
                Err(match error {
                    SubmitError::Conflict => DownloadControlError::Conflict,
                    SubmitError::Stopping => DownloadControlError::Stopping,
                })
            }
        }
    }

    pub fn cancel(&self, id: &str) -> Result<OperationStatus, DownloadControlError> {
        let AdmissionAuthority::Legacy(store) = &self.authority else {
            return Err(DownloadControlError::Stopping);
        };
        let mut operations = store.lock().expect("operation store poisoned");
        let operation = operations.get(id).ok_or(DownloadControlError::Missing)?;
        if matches!(
            operation.status,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Cancelled
        ) {
            return Err(DownloadControlError::Terminal);
        }
        if matches!(operation.kind, OperationKind::Load | OperationKind::Unload) {
            let boundary = self
                .lifecycle_destructive
                .as_ref()
                .ok_or(DownloadControlError::Missing)?;
            if !boundary.try_cancel(|| {
                self.actor.cancel(id);
            }) {
                return Err(DownloadControlError::CancellationNotSafe);
            }
        } else {
            self.actor.cancel(id);
        }
        operations
            .cancel(id, CancellationSafety::Safe, now_ms())
            .map_err(map_operation_error)
    }

    pub fn start_load(&self, model_id: &str) -> Result<String, DownloadControlError> {
        if self.lifecycle_snapshot.is_none() {
            return Err(DownloadControlError::Missing);
        }
        let entry = self
            .inventory(loxa_core::model_inventory::current_available_memory_bytes())
            .into_iter()
            .find(|entry| entry.id == model_id)
            .ok_or(DownloadControlError::Missing)?;
        LaunchPlan::from_verified_inventory(&entry, &self.models_dir)
            .map_err(|_| DownloadControlError::ModelUnavailable)?;
        self.start_lifecycle(
            OperationKind::Load,
            Some(model_id),
            Mutation::Load {
                model_id: model_id.to_owned(),
            },
        )
    }

    pub(crate) fn start_startup_load(
        &self,
        model_id: &str,
    ) -> Result<String, DownloadControlError> {
        if self.lifecycle_snapshot.is_none() || find_recipe(self.recipes, model_id).is_none() {
            return Err(DownloadControlError::Missing);
        }
        match &self.authority {
            AdmissionAuthority::Legacy(_) => self.start_lifecycle(
                OperationKind::Load,
                Some(model_id),
                Mutation::Load {
                    model_id: model_id.to_owned(),
                },
            ),
            AdmissionAuthority::Durable(durable) => durable
                .start_load_blocking(
                    model_id,
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .map(|admission| admission.v1_operation_id),
        }
    }

    pub fn start_unload(&self) -> Result<String, DownloadControlError> {
        if self.lifecycle_snapshot.is_none() {
            return Err(DownloadControlError::Missing);
        }
        self.start_lifecycle(OperationKind::Unload, None, Mutation::Unload)
    }

    fn start_lifecycle(
        &self,
        kind: OperationKind,
        model_id: Option<&str>,
        mutation: Mutation,
    ) -> Result<String, DownloadControlError> {
        let AdmissionAuthority::Legacy(operations) = &self.authority else {
            return Err(DownloadControlError::Stopping);
        };
        let id = operations
            .lock()
            .expect("operation store poisoned")
            .enqueue_unique_lifecycle(kind, model_id.map(str::to_owned), now_ms())
            .map_err(map_operation_error)?;
        match self.actor.submit(id.clone(), mutation) {
            Ok(()) => Ok(id),
            Err(error) => {
                let message = match error {
                    SubmitError::Conflict => "model lifecycle admission conflicted",
                    SubmitError::Stopping => "node is stopping",
                };
                let _ = self
                    .legacy_store(operations)
                    .lock()
                    .expect("operation store poisoned")
                    .fail(&id, message, now_ms());
                Err(match error {
                    SubmitError::Conflict => DownloadControlError::Conflict,
                    SubmitError::Stopping => DownloadControlError::Stopping,
                })
            }
        }
    }

    pub fn lifecycle_snapshot(&self) -> Option<LifecycleSnapshot> {
        self.lifecycle_snapshot.as_ref().map(|snapshot| {
            snapshot
                .lock()
                .expect("lifecycle snapshot poisoned")
                .clone()
        })
    }

    pub(crate) fn node_snapshot_checked(&self) -> Result<NodeSnapshot, DownloadControlError> {
        match &self.authority {
            AdmissionAuthority::Legacy(_) => {
                let lifecycle = self.lifecycle_snapshot();
                Ok(NodeSnapshot {
                    status: legacy_lifecycle_status(lifecycle.as_ref()),
                    active_model_id: lifecycle
                        .as_ref()
                        .and_then(|snapshot| snapshot.active_model_id.clone()),
                    operation_id: lifecycle
                        .as_ref()
                        .and_then(|snapshot| snapshot.operation_id.clone()),
                    error: lifecycle.and_then(|snapshot| snapshot.error),
                })
            }
            AdmissionAuthority::Durable(durable) => {
                durable.ensure_healthy()?;
                let state = durable
                    .control_state
                    .read_snapshot()
                    .map_err(map_control_state_error)?;
                let operation_id = state.slot.operation_id.and_then(|operation_id| {
                    state
                        .current_instance_v1
                        .operations
                        .iter()
                        .find(|entry| entry.operation.operation_id == operation_id)
                        .map(|entry| entry.v1_operation_id.clone())
                });
                Ok(NodeSnapshot {
                    status: match state.slot.status {
                        loxa_protocol::v2::V2SlotStatus::Unloaded => NodeStatus::Unloaded,
                        loxa_protocol::v2::V2SlotStatus::Loading => NodeStatus::Loading,
                        loxa_protocol::v2::V2SlotStatus::Ready => NodeStatus::Ready,
                        loxa_protocol::v2::V2SlotStatus::Unloading => NodeStatus::Unloading,
                        loxa_protocol::v2::V2SlotStatus::Recovery => NodeStatus::RecoveryRequired,
                    },
                    active_model_id: state.slot.model_id.clone(),
                    operation_id,
                    error: state.slot.error.as_ref().map(|error| error.message.clone()),
                })
            }
        }
    }

    pub fn active_lifecycle_operation_id(&self) -> Option<String> {
        self.lifecycle_snapshot()
            .and_then(|snapshot| snapshot.operation_id)
    }

    pub fn operation(&self, id: &str) -> Option<OperationView> {
        match &self.authority {
            AdmissionAuthority::Legacy(operations) => {
                operations.lock().expect("operation store poisoned").get(id)
            }
            AdmissionAuthority::Durable(durable) => durable.v1_operation(id).ok().flatten(),
        }
    }

    pub fn snapshot_since(&self, cursor: u64) -> ReconnectSnapshot {
        match &self.authority {
            AdmissionAuthority::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .snapshot_since(cursor),
            AdmissionAuthority::Durable(durable) => durable
                .v1_snapshot_since(cursor)
                .expect("durable v1 projection must be checked by router"),
        }
    }

    pub fn subscribe(&self) -> EventSubscription {
        let AdmissionAuthority::Legacy(operations) = &self.authority else {
            panic!("durable subscriptions use subscribe_v1_with_snapshot")
        };
        operations
            .lock()
            .expect("operation store poisoned")
            .subscribe()
    }

    pub fn subscribe_with_snapshot(&self, cursor: u64) -> (ReconnectSnapshot, EventSubscription) {
        let AdmissionAuthority::Legacy(operations) = &self.authority else {
            panic!("durable subscriptions use subscribe_v1_with_snapshot")
        };
        operations
            .lock()
            .expect("operation store poisoned")
            .subscribe_with_snapshot(cursor)
    }

    pub fn inventory(&self, available_memory_bytes: u64) -> Vec<VerifiedRecipeInventoryEntry> {
        loxa_core::model_inventory::verified_recipe_inventory_with_cache(
            self.recipes,
            &self.models_dir,
            available_memory_bytes,
            &self.verification_cache,
        )
    }

    #[cfg(test)]
    pub(crate) fn spawn_fixture_for_test(
        models_dir: PathBuf,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        bytes: &'static [u8],
    ) -> (Self, DownloadControlWorker) {
        Self::spawn_with_cache(
            models_dir,
            verification_cache,
            recipes,
            Box::new(FixtureDownloader { bytes }),
        )
    }

    #[cfg(test)]
    pub(crate) fn spawn_durable_fixture_for_test(
        models_dir: PathBuf,
        verification_cache: Arc<VerificationCache>,
        recipes: &'static [ModelEntry],
        bytes: &'static [u8],
        control_state: ControlStateHandle,
    ) -> (Self, DownloadControlWorker) {
        Self::spawn_with_control_state_components(
            models_dir,
            verification_cache,
            recipes,
            Box::new(FixtureDownloader { bytes }),
            control_state,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn stop_actor(&self) {
        self.actor.stop();
    }

    #[cfg(test)]
    pub(crate) fn durable_execution_for_test(&self) -> DurableExecutionControl {
        let AdmissionAuthority::Durable(durable) = &self.authority else {
            panic!("fixture must use durable authority")
        };
        durable.clone()
    }

    fn legacy_store<'a>(
        &'a self,
        store: &'a Arc<Mutex<OperationStore>>,
    ) -> &'a Arc<Mutex<OperationStore>> {
        store
    }
}

impl DurableExecutionControl {
    #[cfg(test)]
    pub(crate) fn with_actor_for_test(
        control_state: ControlStateHandle,
        actor: NodeActorHandle,
    ) -> Self {
        Self {
            control_state,
            actor,
            lifecycle_supported: true,
            lifecycle_destructive: None,
            projection_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    pub(crate) async fn start_download(
        &self,
        model_id: &str,
        total_bytes: u64,
    ) -> Result<CommittedAdmission, DownloadControlError> {
        self.admit_and_submit(
            AdmissionRequest::Download {
                model_id: model_id.to_owned(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: Some(DecimalU64::new(total_bytes)),
                },
            },
            Mutation::Download {
                model_id: model_id.to_owned(),
            },
        )
        .await
    }

    pub(crate) async fn start_load(
        &self,
        model_id: &str,
    ) -> Result<CommittedAdmission, DownloadControlError> {
        if !self.lifecycle_supported {
            return Err(DownloadControlError::ModelUnavailable);
        }
        self.admit_and_submit(
            AdmissionRequest::Load {
                model_id: model_id.to_owned(),
            },
            Mutation::Load {
                model_id: model_id.to_owned(),
            },
        )
        .await
    }

    fn start_load_blocking(
        &self,
        model_id: &str,
        deadline: std::time::Instant,
    ) -> Result<CommittedAdmission, DownloadControlError> {
        if !self.lifecycle_supported {
            return Err(DownloadControlError::ModelUnavailable);
        }
        self.ensure_healthy()?;
        let admission = self
            .control_state
            .admit_blocking_until(
                AdmissionRequest::Load {
                    model_id: model_id.to_owned(),
                },
                deadline,
            )
            .map_err(map_admission_error)?;
        let mutation = Mutation::Load {
            model_id: model_id.to_owned(),
        };
        if let Err(error) = self
            .actor
            .submit(admission.operation_id.to_string(), mutation)
        {
            self.control_state
                .observe_required_blocking_until(
                    submission_failed_transition(admission.operation_id, V2OperationKind::Load),
                    deadline,
                )
                .map_err(map_control_state_error)?;
            return Err(map_submit_error(error));
        }
        Ok(admission)
    }

    pub(crate) async fn start_unload(&self) -> Result<CommittedAdmission, DownloadControlError> {
        if !self.lifecycle_supported {
            return Err(DownloadControlError::ModelUnavailable);
        }
        self.admit_and_submit(AdmissionRequest::Unload, Mutation::Unload)
            .await
    }

    async fn admit_and_submit(
        &self,
        request: AdmissionRequest,
        mutation: Mutation,
    ) -> Result<CommittedAdmission, DownloadControlError> {
        self.ensure_healthy()?;
        let kind = admission_kind(&request);
        let admission = self
            .control_state
            .admit(request)
            .await
            .map_err(map_admission_error)?;
        if let Err(error) = self
            .actor
            .submit(admission.operation_id.to_string(), mutation)
        {
            self.control_state
                .observe_required_async(submission_failed_transition(admission.operation_id, kind))
                .await
                .map_err(map_control_state_error)?;
            return Err(map_submit_error(error));
        }
        Ok(admission)
    }

    pub(crate) async fn cancel(
        &self,
        v1_operation_id: &str,
    ) -> Result<OperationStatus, DownloadControlError> {
        let operation = self
            .find_current_operation(v1_operation_id)?
            .ok_or(DownloadControlError::Missing)?;
        if matches!(
            operation.status,
            V2OperationStatus::Succeeded | V2OperationStatus::Failed | V2OperationStatus::Cancelled
        ) {
            return Err(DownloadControlError::Terminal);
        }
        let operation_id = operation.operation_id;
        let actor_outcome = if matches!(
            operation.kind,
            V2OperationKind::Load | V2OperationKind::Unload
        ) {
            let boundary = self
                .lifecycle_destructive
                .as_ref()
                .ok_or(DownloadControlError::Missing)?;
            let outcome = std::cell::Cell::new(CancelOutcome::Missing);
            if !boundary
                .try_cancel(|| outcome.set(self.actor.cancel_outcome(&operation_id.to_string())))
            {
                return Err(DownloadControlError::CancellationNotSafe);
            }
            outcome.get()
        } else {
            self.actor.cancel_outcome(&operation_id.to_string())
        };
        match actor_outcome {
            CancelOutcome::Requested => {}
            CancelOutcome::TerminalClaimed => {
                return self.await_terminal_cancel_result(v1_operation_id).await;
            }
            CancelOutcome::Missing => {
                return match self.find_current_operation(v1_operation_id)? {
                    Some(operation)
                        if matches!(
                            operation.status,
                            V2OperationStatus::Succeeded
                                | V2OperationStatus::Failed
                                | V2OperationStatus::Cancelled
                        ) =>
                    {
                        Err(DownloadControlError::Terminal)
                    }
                    _ => Err(DownloadControlError::Stopping),
                };
            }
        }
        self.control_state
            .observe_required_async(Transition::Cancelled { operation_id })
            .await
            .map_err(map_public_cancel_transition)?;
        Ok(OperationStatus::Cancelled)
    }

    async fn await_terminal_cancel_result(
        &self,
        v1_operation_id: &str,
    ) -> Result<OperationStatus, DownloadControlError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match self.find_current_operation(v1_operation_id)? {
                Some(operation)
                    if matches!(
                        operation.status,
                        V2OperationStatus::Succeeded
                            | V2OperationStatus::Failed
                            | V2OperationStatus::Cancelled
                    ) =>
                {
                    return Err(DownloadControlError::Terminal)
                }
                Some(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                _ => return Err(DownloadControlError::Stopping),
            }
        }
    }

    fn v1_operation(
        &self,
        v1_operation_id: &str,
    ) -> Result<Option<OperationView>, DownloadControlError> {
        let operation = self.find_current_operation(v1_operation_id)?;
        operation
            .as_ref()
            .map(|operation| project_durable_v1_operation(v1_operation_id, operation))
            .transpose()
            .map_err(|_| self.poison_projection())
    }

    fn find_current_operation(
        &self,
        v1_operation_id: &str,
    ) -> Result<Option<loxa_protocol::v2::V2Operation>, DownloadControlError> {
        self.ensure_healthy()?;
        let state = self
            .control_state
            .read_snapshot()
            .map_err(map_control_state_error)?;
        Ok(state
            .current_instance_v1
            .operations
            .iter()
            .find(|entry| entry.v1_operation_id == v1_operation_id)
            .map(|entry| entry.operation.clone()))
    }

    fn v1_snapshot_since(&self, cursor: u64) -> Result<ReconnectSnapshot, DownloadControlError> {
        self.ensure_healthy()?;
        let state = self
            .control_state
            .read_snapshot()
            .map_err(map_control_state_error)?;
        project_current_v1(&state.current_instance_v1, cursor).map_err(|_| self.poison_projection())
    }

    async fn subscribe_v1_with_snapshot(
        &self,
        cursor: u64,
    ) -> Result<(ReconnectSnapshot, V1EventReceiver), DownloadControlError> {
        self.ensure_healthy()?;
        let mut durable = self
            .control_state
            .subscribe(None, DecimalU64::new(now_ms()))
            .await
            .map_err(map_control_state_error)?;
        let initial = self.v1_snapshot_since(cursor)?;
        let mut delivered = initial.cursor;
        let control_state = self.control_state.clone();
        let projection_healthy = Arc::clone(&self.projection_healthy);
        let (sender, receiver) = tokio::sync::mpsc::channel(OPERATION_CAPACITY);
        tokio::spawn(async move {
            while let Some(event) = durable.events.recv().await {
                let Some(changed) = event.operation else {
                    continue;
                };
                let Ok(state) = control_state.read_snapshot() else {
                    break;
                };
                let Some(v1_event) = state.current_instance_v1.events.iter().find(|candidate| {
                    candidate.operation.operation_id == changed.operation_id
                        && candidate.operation.updated_revision == changed.updated_revision
                }) else {
                    projection_healthy.store(false, std::sync::atomic::Ordering::Release);
                    break;
                };
                if v1_event.sequence <= delivered {
                    continue;
                }
                let Ok(operation) =
                    project_durable_v1_operation(&v1_event.v1_operation_id, &v1_event.operation)
                else {
                    projection_healthy.store(false, std::sync::atomic::Ordering::Release);
                    break;
                };
                let Ok(sequence) = project_durable_v1_counter(v1_event.sequence) else {
                    projection_healthy.store(false, std::sync::atomic::Ordering::Release);
                    break;
                };
                delivered = sequence;
                if sender
                    .send(loxa_core::control::contracts::ControlEvent {
                        sequence: delivered,
                        operation,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok((initial, V1EventReceiver { receiver }))
    }

    fn ensure_healthy(&self) -> Result<(), DownloadControlError> {
        if self
            .projection_healthy
            .load(std::sync::atomic::Ordering::Acquire)
            && self.control_state.is_healthy()
        {
            Ok(())
        } else {
            Err(DownloadControlError::Stopping)
        }
    }

    fn poison_projection(&self) -> DownloadControlError {
        self.projection_healthy
            .store(false, std::sync::atomic::Ordering::Release);
        DownloadControlError::Stopping
    }
}

fn project_current_v1(
    state: &CurrentInstanceV1State,
    cursor: u64,
) -> Result<ReconnectSnapshot, loxa_core::control::contracts::DurableV1ProjectionError> {
    let projected_cursor = project_durable_v1_counter(state.cursor)?;
    let operations = state
        .operations
        .iter()
        .map(|entry| project_durable_v1_operation(&entry.v1_operation_id, &entry.operation))
        .collect::<Result<Vec<_>, _>>()?;
    let events = state
        .events
        .iter()
        .filter(|event| event.sequence > cursor)
        .map(|event| {
            Ok(loxa_core::control::contracts::ControlEvent {
                sequence: project_durable_v1_counter(event.sequence)?,
                operation: project_durable_v1_operation(&event.v1_operation_id, &event.operation)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ReconnectSnapshot {
        cursor: projected_cursor,
        cursor_gap: state.cursor_gap(cursor),
        operations,
        events,
    })
}

fn admission_kind(request: &AdmissionRequest) -> V2OperationKind {
    match request {
        AdmissionRequest::Download { .. } => V2OperationKind::Download,
        AdmissionRequest::Load { .. } => V2OperationKind::Load,
        AdmissionRequest::Unload => V2OperationKind::Unload,
    }
}

fn submission_failed_transition(operation_id: OperationId, kind: V2OperationKind) -> Transition {
    let (code, message) = match kind {
        V2OperationKind::Download => (
            V2OperationErrorCode::DownloadFailed,
            "download could not be submitted",
        ),
        V2OperationKind::Load => (
            V2OperationErrorCode::LoadFailed,
            "load could not be submitted",
        ),
        V2OperationKind::Unload => (
            V2OperationErrorCode::UnloadFailed,
            "unload could not be submitted",
        ),
    };
    Transition::Failed {
        operation_id,
        error: V2PublicError {
            code,
            message: message.into(),
        },
    }
}

fn map_submit_error(error: SubmitError) -> DownloadControlError {
    match error {
        SubmitError::Conflict => DownloadControlError::Conflict,
        SubmitError::Stopping => DownloadControlError::Stopping,
    }
}

fn map_control_state_error(error: ControlStateError) -> DownloadControlError {
    match error {
        ControlStateError::WriterOverloaded => DownloadControlError::WriterOverloaded,
        ControlStateError::Transition(TransitionError::ActiveLimit)
        | ControlStateError::Transition(TransitionError::LifecycleConflict)
        | ControlStateError::Transition(TransitionError::SameModelConflict) => {
            DownloadControlError::Conflict
        }
        ControlStateError::Transition(TransitionError::OperationNotFound) => {
            DownloadControlError::Missing
        }
        ControlStateError::Transition(TransitionError::IllegalTransition)
        | ControlStateError::Transition(TransitionError::Contradiction) => {
            DownloadControlError::Terminal
        }
        ControlStateError::DurableStateUnavailable
        | ControlStateError::UnknownCommit
        | ControlStateError::SnapshotTooLarge
        | ControlStateError::WorkerPanicked
        | ControlStateError::ShutdownDeadlineExceeded
        | ControlStateError::Transition(_)
        | ControlStateError::Repository(_) => DownloadControlError::Stopping,
    }
}

fn map_admission_error(error: ControlStateError) -> DownloadControlError {
    match error {
        ControlStateError::WriterOverloaded => DownloadControlError::WriterOverloaded,
        ControlStateError::Transition(TransitionError::ActiveLimit)
        | ControlStateError::Transition(TransitionError::LifecycleConflict)
        | ControlStateError::Transition(TransitionError::SameModelConflict)
        | ControlStateError::Transition(TransitionError::Contradiction) => {
            DownloadControlError::Conflict
        }
        other => map_control_state_error(other),
    }
}

fn map_public_cancel_transition(error: ControlStateError) -> DownloadControlError {
    match error {
        ControlStateError::Transition(TransitionError::IllegalTransition)
        | ControlStateError::Transition(TransitionError::Contradiction) => {
            DownloadControlError::Terminal
        }
        other => map_control_state_error(other),
    }
}

impl DownloadControlWorker {
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    pub fn stop_and_join(mut self) -> std::io::Result<()> {
        if let Some(stop) = &self.lifecycle_stop {
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.actor.stop();
        if let Some(verification) = &self.verification {
            verification.cancellation.cancel();
        }
        let actor_result = if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| std::io::Error::other("download actor worker panicked"))
        } else {
            Ok(())
        };
        let verification_result = if let Some(verification) = self.verification.take() {
            verification
                .worker
                .join()
                .map_err(|_| std::io::Error::other("verification worker panicked"))
        } else {
            Ok(())
        };
        let durable_result = if let Some(control_state) = self.durable_control_state.take() {
            thread::Builder::new()
                .name("loxa-durable-shutdown-observer".into())
                .spawn(move || terminalize_remaining_durable_operations(&control_state))
                .and_then(|worker| {
                    worker.join().map_err(|_| {
                        io::Error::other("durable operation shutdown observer panicked")
                    })?
                })
        } else {
            Ok(())
        };
        actor_result.and(verification_result).and(durable_result)
    }
}

fn terminalize_remaining_durable_operations(control_state: &ControlStateHandle) -> io::Result<()> {
    let active = control_state
        .read_snapshot()
        .map_err(|_| io::Error::other("durable operation shutdown observation failed"))?
        .operations
        .iter()
        .filter(|operation| {
            matches!(
                operation.status,
                V2OperationStatus::Queued
                    | V2OperationStatus::Running
                    | V2OperationStatus::Cancelling
            )
        })
        .map(|operation| (operation.operation_id, operation.kind, operation.status))
        .collect::<Vec<_>>();
    let now = std::time::Instant::now();
    let deadline = now
        .checked_add(std::time::Duration::from_secs(5))
        .unwrap_or(now);
    for (operation_id, kind, status) in active {
        let error = if status == V2OperationStatus::Cancelling {
            V2PublicError {
                code: V2OperationErrorCode::CancellationOutcomeUnknown,
                message: "node stopped before cancellation was confirmed".into(),
            }
        } else {
            operation_error(kind, "node is stopping")
        };
        control_state
            .observe_required_blocking_before(
                Transition::Failed {
                    operation_id,
                    error,
                },
                deadline,
            )
            .map_err(|error| {
                io::Error::other(format!(
                    "durable operation shutdown observation failed: {error:?}"
                ))
            })?;
    }
    Ok(())
}

struct DownloadExecutor {
    models_dir: PathBuf,
    persistence: ExecutionPersistence,
    downloader: Box<dyn ModelDownloader>,
    verification_cancellation: MutationCancellation,
    verifier: Box<dyn ArtifactVerifier>,
    recipes: &'static [ModelEntry],
    lifecycle: Option<Box<dyn LifecycleMutationExecutor>>,
}

trait LifecycleMutationExecutor: Send {
    fn execute(
        &mut self,
        operation_id: &str,
        mutation: &Mutation,
        cancellation: &MutationCancellation,
    ) -> Result<(), LifecycleError>;
    fn complete_operation(&mut self);
    fn shutdown(&mut self) -> Result<(), LifecycleError>;
    fn tick(&mut self);
}

struct LifecycleExecutor<D: EngineLifecycleDriver, G: GatewayPublisher> {
    lifecycle: ModelLifecycle<D, G>,
    snapshot: Arc<Mutex<LifecycleSnapshot>>,
    models_dir: PathBuf,
    verification_cache: Arc<VerificationCache>,
    recipes: &'static [ModelEntry],
    restart_verifier: Box<dyn RestartArtifactVerifier>,
}

trait RestartArtifactVerifier: Send {
    fn verify(
        &mut self,
        models_dir: &std::path::Path,
        recipe: &'static ModelEntry,
        cancellation: &dyn VerificationCancellation,
    ) -> std::io::Result<VerifiedArtifact>;
}

struct CacheRestartArtifactVerifier {
    cache: Arc<VerificationCache>,
}

impl RestartArtifactVerifier for CacheRestartArtifactVerifier {
    fn verify(
        &mut self,
        models_dir: &std::path::Path,
        recipe: &'static ModelEntry,
        cancellation: &dyn VerificationCancellation,
    ) -> std::io::Result<VerifiedArtifact> {
        self.cache
            .verify_recipe_with_cancellation(models_dir, recipe, cancellation)
    }
}

struct RestartVerificationCancellation {
    operation: MutationCancellation,
    stopping: Arc<std::sync::atomic::AtomicBool>,
}

impl VerificationCancellation for RestartVerificationCancellation {
    fn is_cancelled(&self) -> bool {
        self.operation.is_cancelled() || self.stopping.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<D, G> LifecycleMutationExecutor for LifecycleExecutor<D, G>
where
    D: EngineLifecycleDriver + Send + 'static,
    D::Session: Send + 'static,
    G: GatewayPublisher + Send + 'static,
{
    fn execute(
        &mut self,
        operation_id: &str,
        mutation: &Mutation,
        cancellation: &MutationCancellation,
    ) -> Result<(), LifecycleError> {
        {
            let mut snapshot = self.snapshot.lock().expect("lifecycle snapshot poisoned");
            snapshot.status = match mutation {
                Mutation::Load { .. } => crate::model_lifecycle::NodeLifecycleStatus::Loading,
                Mutation::Unload => crate::model_lifecycle::NodeLifecycleStatus::Unloading,
                Mutation::Download { .. } => snapshot.status.clone(),
            };
            snapshot.operation_id = Some(operation_id.to_owned());
            snapshot.error = None;
        }
        match mutation {
            Mutation::Load { model_id } => {
                let recipe =
                    find_recipe(self.recipes, model_id).ok_or(LifecycleError::ModelNotVerified)?;
                self.verification_cache
                    .verify_recipe_with_cancellation(&self.models_dir, recipe, cancellation)
                    .map_err(|_| LifecycleError::ModelNotVerified)?;
                let entry = loxa_core::model_inventory::verified_recipe_inventory_with_cache(
                    self.recipes,
                    &self.models_dir,
                    loxa_core::model_inventory::current_available_memory_bytes(),
                    &self.verification_cache,
                )
                .into_iter()
                .find(|entry| entry.id == *model_id)
                .ok_or(LifecycleError::ModelNotVerified)?;
                let plan = LaunchPlan::from_verified_inventory(&entry, &self.models_dir)?;
                self.lifecycle.load(plan, cancellation)
            }
            Mutation::Unload => self.lifecycle.unload(cancellation),
            Mutation::Download { .. } => Ok(()),
        }
    }

    fn shutdown(&mut self) -> Result<(), LifecycleError> {
        let result = self.lifecycle.shutdown();
        *self.snapshot.lock().expect("lifecycle snapshot poisoned") = self.lifecycle.snapshot();
        result
    }

    fn tick(&mut self) {
        if let Ok(Some(model_id)) = self.lifecycle.poll_ready_session() {
            let cancellation = MutationCancellation::new();
            let verification_cancellation = RestartVerificationCancellation {
                operation: cancellation.clone(),
                stopping: self.lifecycle.stop_token(),
            };
            let restart = find_recipe(self.recipes, &model_id)
                .ok_or(LifecycleError::ModelNotVerified)
                .and_then(|recipe| {
                    self.restart_verifier
                        .verify(&self.models_dir, recipe, &verification_cancellation)
                        .map_err(|_| LifecycleError::ModelNotVerified)?;
                    let entry = loxa_core::model_inventory::verified_recipe_inventory_with_cache(
                        self.recipes,
                        &self.models_dir,
                        loxa_core::model_inventory::current_available_memory_bytes(),
                        &self.verification_cache,
                    )
                    .into_iter()
                    .find(|entry| entry.id == model_id)
                    .ok_or(LifecycleError::ModelNotVerified)?;
                    LaunchPlan::from_verified_inventory(&entry, &self.models_dir)
                })
                .and_then(|plan| self.lifecycle.restart_verified(plan, &cancellation));
            if let Err(error) = restart {
                if self.lifecycle.stop_requested() {
                    self.lifecycle.finish_stopped_supervision();
                } else {
                    self.lifecycle.fail_supervision(error);
                }
            }
        }
        *self.snapshot.lock().expect("lifecycle snapshot poisoned") = self.lifecycle.snapshot();
    }

    fn complete_operation(&mut self) {
        *self.snapshot.lock().expect("lifecycle snapshot poisoned") = self.lifecycle.snapshot();
        self.lifecycle.complete_operation();
    }
}

trait ArtifactVerifier: Send {
    fn verify(
        &mut self,
        models_dir: &std::path::Path,
        recipe: &'static ModelEntry,
        cancellation: &MutationCancellation,
    ) -> io::Result<VerifiedArtifact>;

    fn invalidate(&mut self, models_dir: &std::path::Path, recipe: &'static ModelEntry);
}

struct CacheArtifactVerifier {
    cache: Arc<VerificationCache>,
}

impl ArtifactVerifier for CacheArtifactVerifier {
    fn verify(
        &mut self,
        models_dir: &std::path::Path,
        recipe: &'static ModelEntry,
        cancellation: &MutationCancellation,
    ) -> io::Result<VerifiedArtifact> {
        self.cache
            .verify_recipe_with_cancellation(models_dir, recipe, cancellation)
    }

    fn invalidate(&mut self, models_dir: &std::path::Path, recipe: &'static ModelEntry) {
        self.cache.invalidate_recipe(models_dir, recipe);
    }
}

impl VerificationCancellation for MutationCancellation {
    fn is_cancelled(&self) -> bool {
        MutationCancellation::is_cancelled(self)
    }
}

fn verify_existing_recipes(
    models_dir: &std::path::Path,
    recipes: &[ModelEntry],
    cache: &VerificationCache,
    cancellation: &MutationCancellation,
) {
    for recipe in recipes {
        if cancellation.is_cancelled() {
            break;
        }
        let _ = cache.verify_recipe_with_cancellation(models_dir, recipe, cancellation);
    }
}

trait ModelDownloader: Send {
    fn download(
        &mut self,
        recipe: &'static loxa_core::registry::ModelEntry,
        models_dir: &std::path::Path,
        observer: &mut dyn DownloadObserver,
    ) -> Result<(), DownloadError>;
}

struct VerifiedDownloader;

#[cfg(test)]
struct FixtureDownloader {
    bytes: &'static [u8],
}

impl ModelDownloader for VerifiedDownloader {
    fn download(
        &mut self,
        recipe: &'static loxa_core::registry::ModelEntry,
        models_dir: &std::path::Path,
        observer: &mut dyn DownloadObserver,
    ) -> Result<(), DownloadError> {
        download::download_with_observer(recipe, models_dir, observer).map(|_| ())
    }
}

#[cfg(test)]
impl ModelDownloader for FixtureDownloader {
    fn download(
        &mut self,
        recipe: &'static ModelEntry,
        models_dir: &std::path::Path,
        observer: &mut dyn DownloadObserver,
    ) -> Result<(), DownloadError> {
        std::fs::create_dir_all(models_dir)?;
        let part = models_dir.join(format!("{}.part", recipe.filename));
        std::fs::write(&part, self.bytes)?;
        observer.progress(DownloadProgress {
            downloaded_bytes: self.bytes.len() as u64,
            total_bytes: recipe.size_bytes,
        });
        if observer.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        std::fs::rename(part, models_dir.join(recipe.filename))?;
        Ok(())
    }
}

struct OperationObserver<'a> {
    id: &'a str,
    cancellation: &'a MutationCancellation,
    persistence: ExecutionPersistence,
}

impl DownloadObserver for OperationObserver<'_> {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn progress(&mut self, progress: DownloadProgress) {
        self.persistence.progress(
            self.id,
            progress.downloaded_bytes,
            Some(progress.total_bytes),
        );
    }
}

impl ExecutionPersistence {
    fn started(&self, id: &str, progress: Option<V2OperationProgress>) -> bool {
        match self {
            Self::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .start(id, now_ms())
                .is_ok(),
            Self::Durable(control_state) => self
                .durable_observe(control_state, id, |operation_id| Transition::Started {
                    operation_id,
                    progress,
                })
                .is_ok(),
        }
    }

    fn progress(&self, id: &str, completed: u64, total: Option<u64>) {
        match self {
            Self::Legacy(operations) => {
                let _ = operations
                    .lock()
                    .expect("operation store poisoned")
                    .progress(id, completed, total, now_ms());
            }
            Self::Durable(control_state) => {
                if let Ok(operation_id) = OperationId::from_str(id) {
                    let _ = control_state.try_observe_progress(Transition::Progress {
                        operation_id,
                        progress: V2OperationProgress {
                            completed_bytes: DecimalU64::new(completed),
                            total_bytes: total.map(DecimalU64::new),
                        },
                    });
                }
            }
        }
    }

    fn succeeded(&self, id: &str, observed_model_id: Option<String>) -> bool {
        match self {
            Self::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .succeed(id, now_ms())
                .is_ok(),
            Self::Durable(control_state) => self
                .durable_observe(control_state, id, |operation_id| Transition::Succeeded {
                    operation_id,
                    observed_model_id,
                })
                .is_ok(),
        }
    }

    fn failed(&self, id: &str, kind: V2OperationKind, message: &str) -> bool {
        match self {
            Self::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .fail(id, message, now_ms())
                .is_ok(),
            Self::Durable(control_state) => self
                .durable_observe(control_state, id, |operation_id| Transition::Failed {
                    operation_id,
                    error: operation_error(kind, message),
                })
                .is_ok(),
        }
    }

    fn cancelled(&self, id: &str) -> bool {
        match self {
            Self::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .cancel(id, CancellationSafety::Safe, now_ms())
                .is_ok(),
            Self::Durable(control_state) => self
                .durable_observe(control_state, id, |operation_id| Transition::Cancelled {
                    operation_id,
                })
                .is_ok(),
        }
    }

    fn is_cancelled(&self, id: &str) -> bool {
        match self {
            Self::Legacy(operations) => operations
                .lock()
                .expect("operation store poisoned")
                .get(id)
                .is_some_and(|operation| operation.status == OperationStatus::Cancelled),
            Self::Durable(control_state) => OperationId::from_str(id).is_ok_and(|operation_id| {
                control_state.read_snapshot().is_ok_and(|state| {
                    state.operations.iter().any(|operation| {
                        operation.operation_id == operation_id
                            && operation.status == V2OperationStatus::Cancelled
                    })
                })
            }),
        }
    }

    fn finish_lifecycle(&self, id: &str, mutation: &Mutation, result: &Result<(), LifecycleError>) {
        match result {
            Ok(()) => {
                let observed_model_id = match mutation {
                    Mutation::Load { model_id } => Some(model_id.clone()),
                    Mutation::Unload | Mutation::Download { .. } => None,
                };
                self.succeeded(id, observed_model_id);
            }
            Err(LifecycleError::Cancelled) => {
                self.cancelled(id);
            }
            Err(error) => {
                let kind = match mutation {
                    Mutation::Load { .. } => V2OperationKind::Load,
                    Mutation::Unload => V2OperationKind::Unload,
                    Mutation::Download { .. } => V2OperationKind::Download,
                };
                self.failed(id, kind, public_lifecycle_error(error));
            }
        }
    }

    fn durable_observe(
        &self,
        control_state: &ControlStateHandle,
        id: &str,
        transition: impl FnOnce(OperationId) -> Transition,
    ) -> Result<(), ()> {
        let operation_id = OperationId::from_str(id).map_err(|_| ())?;
        let now = std::time::Instant::now();
        let deadline = now
            .checked_add(std::time::Duration::from_secs(5))
            .unwrap_or(now);
        control_state
            .observe_required_blocking_until(transition(operation_id), deadline)
            .map(|_| ())
            .map_err(|_| ())
    }
}

fn operation_error(kind: V2OperationKind, message: &str) -> V2OperationError {
    V2PublicError {
        code: match kind {
            V2OperationKind::Download => V2OperationErrorCode::DownloadFailed,
            V2OperationKind::Load => V2OperationErrorCode::LoadFailed,
            V2OperationKind::Unload => V2OperationErrorCode::UnloadFailed,
        },
        message: message.into(),
    }
}

impl MutationExecutor for DownloadExecutor {
    fn execute(&mut self, id: &str, mutation: &Mutation, cancellation: &MutationCancellation) {
        if !matches!(mutation, Mutation::Download { .. }) {
            if !self.persistence.started(id, None) {
                return;
            }
            let result = self
                .lifecycle
                .as_mut()
                .ok_or_else(|| LifecycleError::StartFailed("model lifecycle unavailable".into()))
                .and_then(|lifecycle| lifecycle.execute(id, mutation, cancellation));
            let result = if cancellation.claim_terminal() {
                result
            } else {
                Err(LifecycleError::Cancelled)
            };
            self.persistence.finish_lifecycle(id, mutation, &result);
            if let Some(lifecycle) = self.lifecycle.as_mut() {
                lifecycle.complete_operation();
            }
            return;
        }
        let Mutation::Download { model_id } = mutation else {
            unreachable!("download mutation checked")
        };
        if !self.persistence.started(id, None) {
            return;
        }
        let Some(recipe) = find_recipe(self.recipes, model_id) else {
            self.persistence
                .failed(id, V2OperationKind::Download, "unknown registry model");
            return;
        };
        tracing::info!(
            target: "loxa_core::download",
            event_code = "download.started",
            component = "download",
            operation_id = id,
            recipe_id = recipe.id,
            state = "download",
        );
        let mut observer = OperationObserver {
            id,
            cancellation,
            persistence: self.persistence.clone(),
        };
        let result = self
            .downloader
            .download(recipe, &self.models_dir, &mut observer);
        let verification = match &result {
            Ok(()) => Some(self.verifier.verify(
                &self.models_dir,
                recipe,
                &self.verification_cancellation,
            )),
            Err(_) => {
                self.verifier.invalidate(&self.models_dir, recipe);
                None
            }
        };
        if !cancellation.claim_terminal() {
            if self.persistence.cancelled(id) {
                emit_download_terminal(id, model_id, "cancelled");
            }
            return;
        }
        if self.persistence.is_cancelled(id) {
            emit_download_terminal(id, model_id, "cancelled");
            return;
        }
        let terminal_result = match result {
            Ok(()) => match verification.expect("successful download was verified") {
                Ok(evidence)
                    if evidence.matches
                        && evidence.size_bytes == recipe.size_bytes
                        && evidence.expected_sha256 == recipe.sha256 =>
                {
                    self.persistence.succeeded(id, None).then_some("succeeded")
                }
                Ok(_) => self
                    .persistence
                    .failed(
                        id,
                        V2OperationKind::Download,
                        "downloaded artifact failed checksum verification",
                    )
                    .then_some("failed"),
                Err(_) if cancellation.is_cancelled() => {
                    self.persistence.cancelled(id).then_some("cancelled")
                }
                Err(_) => {
                    self.verifier.invalidate(&self.models_dir, recipe);
                    self.persistence
                        .failed(
                            id,
                            V2OperationKind::Download,
                            "downloaded artifact could not be verified safely",
                        )
                        .then_some("failed")
                }
            },
            Err(DownloadError::Cancelled) => self.persistence.cancelled(id).then_some("cancelled"),
            Err(error) => self
                .persistence
                .failed(id, V2OperationKind::Download, public_download_error(&error))
                .then_some("failed"),
        };
        if let Some(result_class) = terminal_result {
            emit_download_terminal(id, model_id, result_class);
        }
    }

    fn stop(&mut self) {
        if let Some(lifecycle) = &mut self.lifecycle {
            let _ = lifecycle.shutdown();
        }
    }

    fn tick(&mut self) {
        if let Some(lifecycle) = &mut self.lifecycle {
            lifecycle.tick();
        }
    }

    fn tick_interval(&self) -> Option<std::time::Duration> {
        self.lifecycle
            .as_ref()
            .map(|_| crate::actor::IDLE_TICK_INTERVAL)
    }
}

fn emit_download_terminal(operation_id: &str, recipe_id: &str, result_class: &'static str) {
    tracing::info!(
        target: "loxa_core::download",
        event_code = "download.terminal",
        component = "download",
        operation_id,
        recipe_id,
        state = "download",
        status = result_class,
        result_class,
    );
}

fn public_lifecycle_error(error: &LifecycleError) -> &'static str {
    match error {
        LifecycleError::ModelNotVerified => "model artifact is not downloaded and verified",
        LifecycleError::Incompatible(_) => "model is incompatible with this node",
        LifecycleError::EngineIneligible(_) => "model is not eligible for the selected engine",
        LifecycleError::Cancelled => "model operation cancelled",
        LifecycleError::CancellationNotSafe => "model operation passed its safe cancellation point",
        LifecycleError::Stopping => "node is stopping",
        LifecycleError::RecoveryRequired { .. } => "node recovery is required",
        LifecycleError::InvalidCandidate(_) => "engine candidate validation failed safely",
        LifecycleError::StartFailed(_) => "engine startup failed safely",
        LifecycleError::ReadinessFailed(_) => "engine readiness failed safely",
        LifecycleError::TeardownFailed(_) => "engine teardown failed safely",
    }
}

fn find_recipe(recipes: &'static [ModelEntry], model_id: &str) -> Option<&'static ModelEntry> {
    recipes.iter().find(|recipe| recipe.id == model_id)
}

fn public_download_error(error: &DownloadError) -> &'static str {
    match error {
        DownloadError::Cancelled => "download cancelled",
        DownloadError::AuthRequired => "Hugging Face authentication is required",
        DownloadError::Forbidden => "Hugging Face denied access to this model",
        DownloadError::ChecksumMismatch { .. } => {
            "downloaded artifact failed checksum verification"
        }
        DownloadError::SizeMismatch { .. } => "downloaded artifact has an unexpected size",
        DownloadError::InsufficientDiskSpace { .. } => "insufficient disk space for model download",
        DownloadError::InvalidFilename
        | DownloadError::UnsafeArtifactPath
        | DownloadError::InvalidContentRange
        | DownloadError::Http(_)
        | DownloadError::Io(_) => "model download failed safely",
    }
}

fn map_operation_error(error: OperationError) -> DownloadControlError {
    match error {
        OperationError::Conflict => DownloadControlError::Conflict,
        OperationError::Missing => DownloadControlError::Missing,
        OperationError::Terminal => DownloadControlError::Terminal,
        _ => DownloadControlError::Conflict,
    }
}

fn legacy_lifecycle_status(snapshot: Option<&LifecycleSnapshot>) -> NodeStatus {
    use crate::model_lifecycle::NodeLifecycleStatus;
    match snapshot.map(|snapshot| &snapshot.status) {
        None | Some(NodeLifecycleStatus::Unloaded) => NodeStatus::Unloaded,
        Some(NodeLifecycleStatus::Loading) => NodeStatus::Loading,
        Some(NodeLifecycleStatus::Ready) => NodeStatus::Ready,
        Some(NodeLifecycleStatus::Unloading) => NodeStatus::Unloading,
        Some(NodeLifecycleStatus::RecoveryRequired) => NodeStatus::RecoveryRequired,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
pub(crate) fn panicking_worker() -> DownloadControlWorker {
    struct PanicExecutor(std::sync::mpsc::Sender<()>);
    impl MutationExecutor for PanicExecutor {
        fn execute(&mut self, _: &str, _: &Mutation, _: &MutationCancellation) {
            self.0.send(()).unwrap();
            panic!("injected download worker panic");
        }
    }
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (actor, worker) = NodeActor::spawn(PanicExecutor(started_tx));
    actor
        .submit(
            "panic",
            Mutation::Download {
                model_id: "gemma-3-4b-it-q4".into(),
            },
        )
        .unwrap();
    started_rx.recv().unwrap();
    DownloadControlWorker {
        actor,
        worker: Some(worker),
        verification: None,
        lifecycle_stop: None,
        durable_control_state: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::model_inventory::{ArtifactState, VerificationCache, VerifiedArtifact};
    use loxa_core::registry::{self, ModelEntry};
    use std::collections::BTreeMap;
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn v1_snapshot_projection_rejects_an_unsafe_compatibility_cursor() {
        let state = CurrentInstanceV1State {
            cursor: 9_007_199_254_740_992,
            operations: Vec::new(),
            events: Vec::new(),
        };
        assert_eq!(
            project_current_v1(&state, 0),
            Err(loxa_core::control::contracts::DurableV1ProjectionError::UnsafeInteger)
        );
    }
    use tracing::field::{Field, Visit};
    use tracing::{Event, Metadata, Subscriber};

    #[derive(Clone, Debug)]
    struct CapturedEvent {
        target: String,
        level: tracing::Level,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct EventCapture(Arc<Mutex<Vec<CapturedEvent>>>);

    struct FieldCapture<'a>(&'a mut BTreeMap<String, String>);

    impl Visit for FieldCapture<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl Subscriber for EventCapture {
        fn register_callsite(
            &self,
            _: &'static Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::always()
        }
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            Some(tracing::metadata::LevelFilter::TRACE)
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut FieldCapture(&mut fields));
            self.0.lock().unwrap().push(CapturedEvent {
                target: event.metadata().target().to_owned(),
                level: *event.metadata().level(),
                fields,
            });
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn run_isolated_capture_test(test_name: &str, marker: &str) -> bool {
        let arguments: Vec<_> = std::env::args().collect();
        let exact_child = std::env::var_os(marker).as_deref()
            == Some(std::ffi::OsStr::new("child"))
            && arguments.iter().any(|argument| argument == "--exact")
            && arguments.iter().any(|argument| argument == test_name);
        if exact_child {
            return false;
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(marker, "child")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success()
                && stdout.contains("running 1 test")
                && stdout.contains("1 passed; 0 failed"),
            "isolated test did not run exactly once\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        true
    }

    struct NoopLifecycleDriver;
    impl EngineLifecycleDriver for NoopLifecycleDriver {
        type Session = ();
        fn start(
            &mut self,
            _: &crate::model_lifecycle::StableNodeOwner,
            _: &LaunchPlan,
            _: u64,
        ) -> Result<crate::model_lifecycle::StartedSession<()>, LifecycleError> {
            panic!("unload must not spawn an engine")
        }
        fn wait_ready(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<()>,
            _: crate::model_lifecycle::LifecycleSignals<'_>,
        ) -> Result<(), LifecycleError> {
            panic!("unload must not wait for readiness")
        }
        fn stop_exact<'a>(
            &mut self,
            _: &'a mut crate::model_lifecycle::StartedSession<()>,
        ) -> Result<(), crate::model_lifecycle::ExactStopFailure<'a, ()>> {
            Ok(())
        }
    }

    struct NoopGateway;
    impl GatewayPublisher for NoopGateway {
        fn withdraw(&mut self) {}
        fn publish(&mut self, _: &LaunchPlan, _: &crate::model_lifecycle::SessionCorrelation) {}
    }

    struct ReadyLifecycleDriver;
    impl EngineLifecycleDriver for ReadyLifecycleDriver {
        type Session = ();

        fn start(
            &mut self,
            owner: &crate::model_lifecycle::StableNodeOwner,
            plan: &LaunchPlan,
            generation: u64,
        ) -> Result<crate::model_lifecycle::StartedSession<()>, LifecycleError> {
            Ok(crate::model_lifecycle::StartedSession {
                value: (),
                correlation: crate::model_lifecycle::SessionCorrelation {
                    generation,
                    child_pid: 101,
                    child_process_start_time_unix_s: 202,
                    server_id: "fixture-server".into(),
                    model_id: plan.model_id.clone(),
                    port: 9_001,
                    committed_run_id: owner.run_id.clone(),
                    owner_pid: owner.pid,
                    owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                    gateway_port: owner.gateway_port,
                    generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                    engine_version: "fixture".into(),
                },
            })
        }

        fn wait_ready(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<()>,
            _: crate::model_lifecycle::LifecycleSignals<'_>,
        ) -> Result<(), LifecycleError> {
            Ok(())
        }

        fn stop_exact<'a>(
            &mut self,
            _: &'a mut crate::model_lifecycle::StartedSession<()>,
        ) -> Result<(), crate::model_lifecycle::ExactStopFailure<'a, ()>> {
            Ok(())
        }
    }

    struct RestartProbeDriver {
        starts: Arc<AtomicUsize>,
        exit_requested: Arc<std::sync::atomic::AtomicBool>,
    }

    impl EngineLifecycleDriver for RestartProbeDriver {
        type Session = ();

        fn start(
            &mut self,
            owner: &crate::model_lifecycle::StableNodeOwner,
            plan: &LaunchPlan,
            generation: u64,
        ) -> Result<crate::model_lifecycle::StartedSession<()>, LifecycleError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(crate::model_lifecycle::StartedSession {
                value: (),
                correlation: crate::model_lifecycle::SessionCorrelation {
                    generation,
                    child_pid: 100 + generation as u32,
                    child_process_start_time_unix_s: 200 + generation,
                    server_id: format!("fixture-server-{generation}"),
                    model_id: plan.model_id.clone(),
                    port: 9_000 + generation as u16,
                    committed_run_id: owner.run_id.clone(),
                    owner_pid: owner.pid,
                    owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                    gateway_port: owner.gateway_port,
                    generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                    engine_version: "fixture".into(),
                },
            })
        }

        fn wait_ready(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<()>,
            _: crate::model_lifecycle::LifecycleSignals<'_>,
        ) -> Result<(), LifecycleError> {
            Ok(())
        }

        fn stop_exact<'a>(
            &mut self,
            _: &'a mut crate::model_lifecycle::StartedSession<()>,
        ) -> Result<(), crate::model_lifecycle::ExactStopFailure<'a, ()>> {
            Ok(())
        }

        fn poll_exact(
            &mut self,
            _: &mut crate::model_lifecycle::StartedSession<Self::Session>,
        ) -> Result<crate::model_lifecycle::ExactSessionStatus, LifecycleError> {
            Ok(if self.exit_requested.swap(false, Ordering::SeqCst) {
                crate::model_lifecycle::ExactSessionStatus::Exited
            } else {
                crate::model_lifecycle::ExactSessionStatus::Running
            })
        }
    }

    struct CountingGateway(Arc<AtomicUsize>);
    impl GatewayPublisher for CountingGateway {
        fn withdraw(&mut self) {}
        fn publish(&mut self, _: &LaunchPlan, _: &crate::model_lifecycle::SessionCorrelation) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct RaceGateway {
        withdraws: Arc<AtomicUsize>,
        publishes: Arc<AtomicUsize>,
    }
    impl GatewayPublisher for RaceGateway {
        fn withdraw(&mut self) {
            self.withdraws.fetch_add(1, Ordering::SeqCst);
        }
        fn publish(&mut self, _: &LaunchPlan, _: &crate::model_lifecycle::SessionCorrelation) {
            self.publishes.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FakeDownloader {
        result: Option<Result<(), DownloadError>>,
    }

    struct ShutdownBlockingDownloader {
        entered: std::sync::mpsc::Sender<()>,
    }

    struct PanicExecutor(std::sync::mpsc::Sender<()>);

    struct FakeArtifactVerifier {
        calls: Arc<AtomicUsize>,
        result: Option<std::io::Result<VerifiedArtifact>>,
    }

    struct GatedArtifactVerifier {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
        cache: Arc<VerificationCache>,
    }

    struct GatedRestartVerifier {
        entered: std::sync::mpsc::Sender<()>,
        cache: Arc<VerificationCache>,
    }

    impl RestartArtifactVerifier for GatedRestartVerifier {
        fn verify(
            &mut self,
            models_dir: &std::path::Path,
            recipe: &'static ModelEntry,
            cancellation: &dyn VerificationCancellation,
        ) -> std::io::Result<VerifiedArtifact> {
            struct HashLoopGate<'a> {
                cancellation: &'a dyn VerificationCancellation,
                entered: &'a std::sync::mpsc::Sender<()>,
                checks: AtomicUsize,
            }

            impl VerificationCancellation for HashLoopGate<'_> {
                fn is_cancelled(&self) -> bool {
                    // The verification gate performs the first cancellation check. The
                    // second check is the checksum loop's first iteration, after a cache
                    // miss has acquired a verification permit and opened the artifact.
                    if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
                        self.entered.send(()).unwrap();
                        while !self.cancellation.is_cancelled() {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                    self.cancellation.is_cancelled()
                }
            }

            self.cache.verify_recipe_with_cancellation(
                models_dir,
                recipe,
                &HashLoopGate {
                    cancellation,
                    entered: &self.entered,
                    checks: AtomicUsize::new(0),
                },
            )
        }
    }

    impl ArtifactVerifier for FakeArtifactVerifier {
        fn verify(
            &mut self,
            _: &std::path::Path,
            _: &'static ModelEntry,
            _: &MutationCancellation,
        ) -> std::io::Result<VerifiedArtifact> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.take().expect("fake verification result")
        }

        fn invalidate(&mut self, _: &std::path::Path, _: &'static ModelEntry) {}
    }

    impl ArtifactVerifier for GatedArtifactVerifier {
        fn verify(
            &mut self,
            models_dir: &std::path::Path,
            recipe: &'static ModelEntry,
            cancellation: &MutationCancellation,
        ) -> std::io::Result<VerifiedArtifact> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            self.cache
                .verify_recipe_with_cancellation(models_dir, recipe, cancellation)
        }

        fn invalidate(&mut self, models_dir: &std::path::Path, recipe: &'static ModelEntry) {
            self.cache.invalidate_recipe(models_dir, recipe);
        }
    }

    impl MutationExecutor for PanicExecutor {
        fn execute(&mut self, _: &str, _: &Mutation, _: &MutationCancellation) {
            self.0.send(()).unwrap();
            panic!("injected download worker panic");
        }
    }

    impl ModelDownloader for FakeDownloader {
        fn download(
            &mut self,
            _: &'static loxa_core::registry::ModelEntry,
            _: &std::path::Path,
            observer: &mut dyn DownloadObserver,
        ) -> Result<(), DownloadError> {
            observer.progress(DownloadProgress {
                downloaded_bytes: 4,
                total_bytes: 10,
            });
            observer.progress(DownloadProgress {
                downloaded_bytes: 10,
                total_bytes: 10,
            });
            self.result.take().expect("fake result is configured")
        }
    }

    impl ModelDownloader for ShutdownBlockingDownloader {
        fn download(
            &mut self,
            _: &'static loxa_core::registry::ModelEntry,
            _: &std::path::Path,
            observer: &mut dyn DownloadObserver,
        ) -> Result<(), DownloadError> {
            self.entered.send(()).unwrap();
            while !observer.is_cancelled() {
                std::thread::yield_now();
            }
            Err(DownloadError::Cancelled)
        }
    }

    fn execute_fake(result: Result<(), DownloadError>) -> OperationView {
        let operations = Arc::new(Mutex::new(OperationStore::new(8)));
        let id = operations
            .lock()
            .unwrap()
            .enqueue_unique(OperationKind::Download, Some("gemma-3-4b-it-q4".into()), 1)
            .unwrap();
        let mut executor = DownloadExecutor {
            models_dir: PathBuf::from("/unused"),
            persistence: ExecutionPersistence::Legacy(Arc::clone(&operations)),
            downloader: Box::new(FakeDownloader {
                result: Some(result),
            }),
            verification_cancellation: MutationCancellation::new(),
            verifier: Box::new(FakeArtifactVerifier {
                calls: Arc::new(AtomicUsize::new(0)),
                result: Some(Ok(VerifiedArtifact {
                    size_bytes: registry::find("gemma-3-4b-it-q4").unwrap().size_bytes,
                    expected_sha256: registry::find("gemma-3-4b-it-q4").unwrap().sha256.into(),
                    matches: true,
                })),
            }),
            recipes: REGISTRY,
            lifecycle: None,
        };
        executor.execute(
            &id,
            &Mutation::Download {
                model_id: "gemma-3-4b-it-q4".into(),
            },
            &MutationCancellation::new(),
        );
        let view = operations.lock().unwrap().get(&id).unwrap();
        view
    }

    #[test]
    fn download_diagnostics_are_exact_bounded_and_do_not_copy_progress_or_errors() {
        const ISOLATED: &str = "LOXA_DOWNLOAD_DIAGNOSTICS_TEST_CHILD";
        if run_isolated_capture_test(
            "download_control::tests::download_diagnostics_are_exact_bounded_and_do_not_copy_progress_or_errors",
            ISOLATED,
        ) {
            return;
        }
        for (result, expected) in [
            (Ok(()), "succeeded"),
            (Err(DownloadError::Cancelled), "cancelled"),
            (
                Err(DownloadError::Http(
                    "SECRET_HF_TOKEN /private/owner/model raw transport error".into(),
                )),
                "failed",
            ),
        ] {
            let capture = EventCapture::default();
            let output = Arc::clone(&capture.0);
            let view = tracing::subscriber::with_default(capture, || execute_fake(result));
            let status = match view.status {
                OperationStatus::Succeeded => "succeeded",
                OperationStatus::Failed => "failed",
                OperationStatus::Cancelled => "cancelled",
                other => panic!("unexpected terminal status: {other:?}"),
            };
            assert_eq!(status, expected);
            let events = output.lock().unwrap();
            let diagnostic: Vec<_> = events
                .iter()
                .filter(|event| {
                    event
                        .fields
                        .get("event_code")
                        .is_some_and(|code| code.starts_with("download."))
                })
                .collect();
            assert_eq!(diagnostic.len(), 2, "{diagnostic:?}");
            assert_eq!(diagnostic[0].fields["event_code"], "download.started");
            assert_eq!(diagnostic[1].fields["event_code"], "download.terminal");
            assert_eq!(diagnostic[1].fields["result_class"], expected);
            assert!(diagnostic.iter().all(|event| {
                event.target == "loxa_core::download" && event.level == tracing::Level::INFO
            }));
            let rendered = format!("{diagnostic:?}");
            assert!(!rendered.contains("SECRET_HF_TOKEN"));
            assert!(!rendered.contains("/private/owner/model"));
            assert!(!rendered.contains("raw transport error"));
            assert!(!rendered.contains("download.progress"));
        }
    }

    #[test]
    fn accepts_only_known_ids_rejects_duplicates_and_cancels_cooperatively() {
        let dir = std::env::temp_dir().join(format!("loxa-download-control-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let (control, worker) = DownloadControl::spawn(dir.clone());
        assert_eq!(
            control.start("not-a-registry-model"),
            Err(DownloadControlError::Missing)
        );
        let id = control.start("gemma-3-4b-it-q4").unwrap();
        assert_eq!(
            control.start("gemma-3-4b-it-q4"),
            Err(DownloadControlError::Conflict)
        );
        assert_eq!(control.cancel(&id), Ok(OperationStatus::Cancelled));
        let resumed = control
            .start("gemma-3-4b-it-q4")
            .expect("cancel permits immediate resume without phantom conflict");
        assert_eq!(control.cancel(&resumed), Ok(OperationStatus::Cancelled));
        for _ in 0..100 {
            if control.operation(&id).unwrap().status == OperationStatus::Cancelled {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            control.operation(&id).unwrap().status,
            OperationStatus::Cancelled
        );
        assert!(!control.snapshot_since(0).events.is_empty());
        worker.stop_and_join().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn executor_publishes_monotonic_progress_and_success() {
        let view = execute_fake(Ok(()));
        assert_eq!(view.status, OperationStatus::Succeeded);
        assert_eq!(view.progress.unwrap().completed_bytes, 10);
    }

    #[test]
    fn executor_does_not_publish_success_until_inventory_verification_succeeds() {
        let operations = Arc::new(Mutex::new(OperationStore::new(8)));
        let id = operations
            .lock()
            .unwrap()
            .enqueue_unique(OperationKind::Download, Some("gemma-3-4b-it-q4".into()), 1)
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut executor = DownloadExecutor {
            models_dir: PathBuf::from("/unused"),
            persistence: ExecutionPersistence::Legacy(Arc::clone(&operations)),
            downloader: Box::new(FakeDownloader {
                result: Some(Ok(())),
            }),
            verification_cancellation: MutationCancellation::new(),
            verifier: Box::new(FakeArtifactVerifier {
                calls: Arc::clone(&calls),
                result: Some(Err(std::io::Error::other("verification unavailable"))),
            }),
            recipes: REGISTRY,
            lifecycle: None,
        };

        executor.execute(
            &id,
            &Mutation::Download {
                model_id: "gemma-3-4b-it-q4".into(),
            },
            &MutationCancellation::new(),
        );

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            operations.lock().unwrap().get(&id).unwrap().status,
            OperationStatus::Failed
        );
    }

    #[test]
    fn cancellation_after_promotion_does_not_cancel_authoritative_verification() {
        let operations = Arc::new(Mutex::new(OperationStore::new(8)));
        let model_id = "gemma-3-4b-it-q4";
        let recipe = Box::leak(Box::new(ModelEntry {
            id: model_id,
            repo: "owner/repo",
            revision: "main",
            filename: "fixture.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        }));
        let recipes: &'static [ModelEntry] = std::slice::from_ref(recipe);
        let models_dir = std::env::temp_dir().join(format!(
            "loxa-post-promotion-cancel-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir(&models_dir).unwrap();
        let id = operations
            .lock()
            .unwrap()
            .enqueue_unique(OperationKind::Download, Some(model_id.into()), 1)
            .unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let verification_cancellation = MutationCancellation::new();
        let cache = Arc::new(VerificationCache::default());
        let mut executor = DownloadExecutor {
            models_dir: models_dir.clone(),
            persistence: ExecutionPersistence::Legacy(Arc::clone(&operations)),
            downloader: Box::new(FixtureDownloader { bytes: b"good" }),
            verification_cancellation,
            verifier: Box::new(GatedArtifactVerifier {
                entered: entered_tx,
                release: release_rx,
                cache: Arc::clone(&cache),
            }),
            recipes,
            lifecycle: None,
        };
        let operation_cancellation = MutationCancellation::new();
        let worker_cancellation = operation_cancellation.clone();
        let worker_id = id.clone();
        let worker = std::thread::spawn(move || {
            executor.execute(
                &worker_id,
                &Mutation::Download {
                    model_id: model_id.into(),
                },
                &worker_cancellation,
            );
        });

        entered_rx.recv().unwrap();
        operation_cancellation.cancel();
        operations
            .lock()
            .unwrap()
            .cancel(&id, CancellationSafety::Safe, now_ms())
            .unwrap();
        release_tx.send(()).unwrap();
        worker.join().unwrap();

        assert_eq!(
            operations.lock().unwrap().get(&id).unwrap().status,
            OperationStatus::Cancelled
        );
        assert_eq!(
            cache.artifact_state(&models_dir, recipe),
            ArtifactState::Downloaded,
            "a late UI cancellation must not strand a promoted artifact as unverified"
        );
        std::fs::remove_dir_all(models_dir).unwrap();
    }

    #[test]
    fn successful_control_verification_and_restart_republish_downloaded_inventory_evidence() {
        let dir = std::env::temp_dir().join(format!("loxa-restart-verification-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "fixture",
            repo: "owner/repo",
            revision: "main",
            filename: "fixture.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        }));
        std::fs::write(dir.join(recipe.filename), b"good").unwrap();
        let cache = Arc::new(VerificationCache::default());
        assert!(matches!(
            cache.artifact_state(&dir, recipe),
            ArtifactState::Invalid { .. }
        ));
        let mut verifier = CacheArtifactVerifier {
            cache: Arc::clone(&cache),
        };
        assert!(
            verifier
                .verify(&dir, recipe, &MutationCancellation::new())
                .unwrap()
                .matches
        );
        assert_eq!(
            cache.artifact_state(&dir, recipe),
            ArtifactState::Downloaded
        );

        let restarted_cache = VerificationCache::default();
        verify_existing_recipes(
            &dir,
            std::slice::from_ref(recipe),
            &restarted_cache,
            &MutationCancellation::new(),
        );

        assert_eq!(
            restarted_cache.artifact_state(&dir, recipe),
            ArtifactState::Downloaded
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn restart_scan_never_blocks_control_construction() {
        struct BlockAfterPermit {
            calls: AtomicUsize,
            entered: std::sync::mpsc::Sender<()>,
            release: Arc<std::sync::atomic::AtomicBool>,
        }

        impl VerificationCancellation for BlockAfterPermit {
            fn is_cancelled(&self) -> bool {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    self.entered.send(()).unwrap();
                    while !self.release.load(Ordering::SeqCst) {
                        std::thread::yield_now();
                    }
                }
                false
            }
        }

        let dir = std::env::temp_dir().join(format!(
            "loxa-nonblocking-restart-scan-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir(&dir).unwrap();
        let blocker = Box::leak(Box::new(ModelEntry {
            id: "blocker",
            repo: "owner/repo",
            revision: "main",
            filename: "blocker.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        }));
        let restart = Box::leak(Box::new(ModelEntry {
            id: "restart",
            repo: "owner/repo",
            revision: "main",
            filename: "restart.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        }));
        std::fs::write(dir.join(blocker.filename), b"good").unwrap();
        std::fs::write(dir.join(restart.filename), b"good").unwrap();
        let cache = Arc::new(VerificationCache::with_limits(8, 1));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let verifier_cache = Arc::clone(&cache);
        let verifier_dir = dir.clone();
        let verifier_release = Arc::clone(&release);
        let occupied = std::thread::spawn(move || {
            verifier_cache
                .verify_recipe_with_cancellation(
                    &verifier_dir,
                    blocker,
                    &BlockAfterPermit {
                        calls: AtomicUsize::new(0),
                        entered: entered_tx,
                        release: verifier_release,
                    },
                )
                .unwrap()
        });
        entered_rx.recv().unwrap();

        let started = std::time::Instant::now();
        let (control, worker) = DownloadControl::spawn_with_cache(
            dir.clone(),
            Arc::clone(&cache),
            std::slice::from_ref(restart),
            Box::new(FixtureDownloader { bytes: b"good" }),
        );
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "restart verification must run behind the responsive control plane"
        );
        assert!(matches!(
            control.inventory(0)[0].artifact,
            ArtifactState::Invalid { .. }
        ));

        release.store(true, Ordering::SeqCst);
        occupied.join().unwrap();
        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failures_are_actionable_without_leaking_transport_or_hash_details() {
        let checksum = execute_fake(Err(DownloadError::ChecksumMismatch {
            expected: "secret-expected".into(),
            actual: "secret-actual".into(),
        }));
        assert_eq!(checksum.status, OperationStatus::Failed);
        assert_eq!(
            checksum.error.as_deref(),
            Some("downloaded artifact failed checksum verification")
        );
        let http = execute_fake(Err(DownloadError::Http(
            "https://token@example.invalid/private".into(),
        )));
        assert_eq!(http.error.as_deref(), Some("model download failed safely"));
        assert!(!http.error.unwrap().contains("token"));
    }

    #[test]
    fn event_subscription_is_bounded_and_disconnect_cleans_up() {
        let operations = Arc::new(Mutex::new(OperationStore::new(2)));
        let subscription = operations.lock().unwrap().subscribe();
        assert_eq!(operations.lock().unwrap().subscriber_count(), 1);
        drop(subscription);
        assert_eq!(operations.lock().unwrap().subscriber_count(), 0);
    }

    #[test]
    fn worker_panic_is_a_typed_join_failure() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (actor, worker) = NodeActor::spawn(PanicExecutor(started_tx));
        actor
            .submit(
                "panic",
                Mutation::Download {
                    model_id: "gemma-3-4b-it-q4".into(),
                },
            )
            .unwrap();
        started_rx.recv().unwrap();
        let runtime = DownloadControlWorker {
            actor,
            worker: Some(worker),
            verification: None,
            lifecycle_stop: None,
            durable_control_state: None,
        };
        assert_eq!(
            runtime.stop_and_join().unwrap_err().to_string(),
            "download actor worker panicked"
        );
    }

    #[test]
    fn actor_panic_still_cancels_and_joins_background_verification() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (actor, worker) = NodeActor::spawn(PanicExecutor(started_tx));
        actor
            .submit(
                "panic",
                Mutation::Download {
                    model_id: "gemma-3-4b-it-q4".into(),
                },
            )
            .unwrap();
        started_rx.recv().unwrap();

        let cancellation = MutationCancellation::new();
        let background_cancellation = cancellation.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let verification = std::thread::spawn(move || {
            while !background_cancellation.is_cancelled() {
                std::thread::yield_now();
            }
            release_rx.recv().unwrap();
        });
        let runtime = DownloadControlWorker {
            actor,
            worker: Some(worker),
            verification: Some(VerificationWorker {
                cancellation,
                worker: verification,
            }),
            lifecycle_stop: None,
            durable_control_state: None,
        };
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            result_tx.send(runtime.stop_and_join()).unwrap();
        });

        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            result_rx.recv().unwrap().unwrap_err().to_string(),
            "download actor worker panicked"
        );
        join.join().unwrap();
    }

    #[test]
    fn stopping_admission_retains_truthful_terminal_snapshot_and_event() {
        let (control, worker) = DownloadControl::spawn(std::env::temp_dir());
        control.stop_actor();
        assert_eq!(
            control.start("gemma-3-4b-it-q4"),
            Err(DownloadControlError::Stopping)
        );
        let snapshot = control.snapshot_since(0);
        let operation = snapshot.operations.last().unwrap();
        assert_eq!(operation.status, OperationStatus::Failed);
        assert_eq!(operation.error.as_deref(), Some("node is stopping"));
        assert!(snapshot
            .events
            .iter()
            .any(|event| event.operation.id == operation.id
                && event.operation.status == OperationStatus::Failed));
        worker.stop_and_join().unwrap();
    }

    #[test]
    fn startup_load_verifies_known_artifact_then_uses_the_serial_lifecycle_actor() {
        let dir = std::env::temp_dir().join(format!("loxa-startup-load-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "fixture",
            repo: "owner/repo",
            revision: "pinned",
            filename: "fixture.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.0,
        }));
        let recipes: &'static [ModelEntry] = std::slice::from_ref(recipe);
        std::fs::write(dir.join(recipe.filename), b"good").unwrap();
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8_080,
            },
            ReadyLifecycleDriver,
            NoopGateway,
        );
        let (control, worker) = DownloadControl::spawn_with_lifecycle_components(
            dir.clone(),
            lifecycle,
            Arc::new(VerificationCache::default()),
            recipes,
        );

        let id = control.start_startup_load("fixture").unwrap();
        let operation = (0..200)
            .find_map(|_| {
                let operation = control.operation(&id)?;
                if matches!(
                    operation.status,
                    OperationStatus::Succeeded | OperationStatus::Failed
                ) {
                    Some(operation)
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                    None
                }
            })
            .expect("startup load reaches a terminal operation");

        assert_eq!(operation.status, OperationStatus::Succeeded);
        assert_eq!(
            control
                .lifecycle_snapshot()
                .unwrap()
                .active_model_id
                .as_deref(),
            Some("fixture")
        );
        let unload = control.start_unload().unwrap();
        for _ in 0..200 {
            if control
                .operation(&unload)
                .is_some_and(|operation| operation.status == OperationStatus::Succeeded)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            control.operation(&unload).unwrap().status,
            OperationStatus::Succeeded
        );
        let reload = control.start_load("fixture").unwrap();
        for _ in 0..200 {
            if control
                .operation(&reload)
                .is_some_and(|operation| operation.status == OperationStatus::Succeeded)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            control.operation(&reload).unwrap().status,
            OperationStatus::Succeeded
        );
        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn automatic_restart_reverifies_mutated_artifact_before_any_spawn_or_publish() {
        let dir = std::env::temp_dir().join(format!("loxa-restart-reverify-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "fixture-restart",
            repo: "owner/repo",
            revision: "pinned",
            filename: "fixture.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.0,
        }));
        let recipes: &'static [ModelEntry] = std::slice::from_ref(recipe);
        std::fs::write(dir.join(recipe.filename), b"good").unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        let publishes = Arc::new(AtomicUsize::new(0));
        let exit_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8_080,
            },
            RestartProbeDriver {
                starts: Arc::clone(&starts),
                exit_requested: Arc::clone(&exit_requested),
            },
            CountingGateway(Arc::clone(&publishes)),
        );
        let (control, worker) = DownloadControl::spawn_with_lifecycle_components(
            dir.clone(),
            lifecycle,
            Arc::new(VerificationCache::default()),
            recipes,
        );
        let initial = control.start_startup_load(recipe.id).unwrap();
        for _ in 0..200 {
            if control
                .operation(&initial)
                .is_some_and(|operation| operation.status == OperationStatus::Succeeded)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(publishes.load(Ordering::SeqCst), 1);

        std::fs::write(dir.join(recipe.filename), b"evil!").unwrap();
        exit_requested.store(true, Ordering::SeqCst);
        for _ in 0..40 {
            if control.lifecycle_snapshot().is_some_and(|snapshot| {
                snapshot.status == crate::model_lifecycle::NodeLifecycleStatus::RecoveryRequired
            }) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        let snapshot = control.lifecycle_snapshot().unwrap();
        assert_eq!(
            snapshot.status,
            crate::model_lifecycle::NodeLifecycleStatus::RecoveryRequired
        );
        assert_eq!(snapshot.active_model_id, None);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(publishes.load(Ordering::SeqCst), 1);
        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn restart_verification_cancellation_is_connected_to_shared_lifecycle_stop() {
        let operation = MutationCancellation::new();
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancellation = RestartVerificationCancellation {
            operation: operation.clone(),
            stopping: Arc::clone(&stopping),
        };
        assert!(!cancellation.is_cancelled());
        stopping.store(true, Ordering::SeqCst);
        assert!(cancellation.is_cancelled());

        stopping.store(false, Ordering::SeqCst);
        operation.cancel();
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn actor_stop_cancels_in_progress_restart_hash_without_spawn_or_recovery() {
        let dir = std::env::temp_dir().join(format!(
            "loxa-stop-restart-hash-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "stop-restart-fixture",
            repo: "owner/repo",
            revision: "pinned",
            filename: "fixture.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.0,
        }));
        let recipes: &'static [ModelEntry] = std::slice::from_ref(recipe);
        std::fs::write(dir.join(recipe.filename), b"good").unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        let publishes = Arc::new(AtomicUsize::new(0));
        let exit_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let owner = crate::model_lifecycle::StableNodeOwner {
            run_id: "stable-owner".into(),
            pid: 1,
            process_start_time_unix_s: 2,
            gateway_port: 8_080,
        };
        let lifecycle = ModelLifecycle::new(
            owner.clone(),
            RestartProbeDriver {
                starts: Arc::clone(&starts),
                exit_requested: Arc::clone(&exit_requested),
            },
            CountingGateway(Arc::clone(&publishes)),
        );
        let verification_cache = Arc::new(VerificationCache::default());
        let (control, worker) = DownloadControl::spawn_with_lifecycle_components_and_verifier(
            dir.clone(),
            lifecycle,
            Arc::clone(&verification_cache),
            recipes,
            Box::new(GatedRestartVerifier {
                entered: entered_tx,
                cache: Arc::clone(&verification_cache),
            }),
        );
        let initial = control.start_startup_load(recipe.id).unwrap();
        for _ in 0..200 {
            if control
                .operation(&initial)
                .is_some_and(|operation| operation.status == OperationStatus::Succeeded)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(publishes.load(Ordering::SeqCst), 1);
        std::thread::sleep(Duration::from_millis(10));
        std::fs::write(dir.join(recipe.filename), b"evil").unwrap();
        assert_ne!(
            verification_cache.artifact_state(&dir, recipe),
            loxa_core::model_inventory::ArtifactState::Downloaded
        );
        exit_requested.store(true, Ordering::SeqCst);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("restart verification entered the checksum loop after a cache miss");

        let started = std::time::Instant::now();
        worker.stop_and_join().unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        let snapshot = control.lifecycle_snapshot().unwrap();
        assert_eq!(
            snapshot.status,
            crate::model_lifecycle::NodeLifecycleStatus::Unloaded
        );
        assert_ne!(
            snapshot.status,
            crate::model_lifecycle::NodeLifecycleStatus::RecoveryRequired
        );
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(publishes.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unified_actor_runs_unload_and_publishes_operation_events() {
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            NoopLifecycleDriver,
            NoopGateway,
        );
        let (control, worker) =
            DownloadControl::spawn_with_lifecycle(std::env::temp_dir(), lifecycle);
        let id = control.start_unload().unwrap();
        let operation = (0..100)
            .find_map(|_| {
                let operation = control.operation(&id)?;
                if matches!(
                    operation.status,
                    OperationStatus::Succeeded | OperationStatus::Failed
                ) {
                    Some(operation)
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                    None
                }
            })
            .expect("unload reaches terminal operation");
        assert_eq!(operation.status, OperationStatus::Succeeded);
        assert_eq!(
            control.lifecycle_snapshot().unwrap().status,
            crate::model_lifecycle::NodeLifecycleStatus::Unloaded
        );
        assert!(control
            .snapshot_since(0)
            .events
            .iter()
            .any(|event| event.operation.id == id
                && event.operation.status == OperationStatus::Succeeded));
        worker.stop_and_join().unwrap();
    }

    #[test]
    fn lifecycle_admission_keeps_one_exact_active_operation() {
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            NoopLifecycleDriver,
            NoopGateway,
        );
        let (control, worker) =
            DownloadControl::spawn_with_lifecycle(std::env::temp_dir(), lifecycle);
        let AdmissionAuthority::Legacy(operations) = &control.authority else {
            panic!("fixture uses legacy authority")
        };
        let first = operations
            .lock()
            .unwrap()
            .enqueue_unique_lifecycle(OperationKind::Load, Some("first".into()), now_ms())
            .unwrap();

        assert_eq!(control.start_unload(), Err(DownloadControlError::Conflict));
        assert_eq!(control.active_lifecycle_operation_id(), None);
        assert_eq!(
            control.operation(&first).unwrap().status,
            OperationStatus::Queued
        );
        assert_eq!(
            control
                .snapshot_since(0)
                .operations
                .iter()
                .filter(|operation| matches!(
                    operation.kind,
                    OperationKind::Load | OperationKind::Unload
                ))
                .count(),
            1
        );
        worker.stop_and_join().unwrap();
    }

    #[test]
    fn destructive_completion_stays_uncancellable_until_operation_is_terminal() {
        let mut lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            NoopLifecycleDriver,
            NoopGateway,
        );
        let boundary = lifecycle.destructive_commit_token();
        let mut operations = OperationStore::new(2);
        let id = operations
            .enqueue_unique_lifecycle(OperationKind::Unload, None, 1)
            .unwrap();
        operations.start(&id, 2).unwrap();

        lifecycle.unload(&MutationCancellation::new()).unwrap();
        assert!(!boundary.try_cancel(|| panic!("completed unload must not be cancelled")));
        operations.succeed(&id, 3).unwrap();
        lifecycle.complete_operation();
        assert_eq!(
            operations.cancel(&id, CancellationSafety::Safe, 4),
            Err(OperationError::Terminal)
        );
    }

    #[test]
    fn concurrent_control_cancel_and_destructive_commit_publish_one_truthful_terminal_state() {
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "race-fixture",
            repo: "owner/repo",
            revision: "pinned",
            filename: "race.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.0,
        }));
        let recipes: &'static [ModelEntry] = std::slice::from_ref(recipe);
        for kind in [OperationKind::Load, OperationKind::Unload] {
            for cancel_wins in [true, false] {
                let dir = std::env::temp_dir().join(format!(
                    "loxa-lifecycle-race-{}-{}-{}",
                    now_ms(),
                    kind as u8,
                    cancel_wins
                ));
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join(recipe.filename), b"good").unwrap();
                let cache = Arc::new(VerificationCache::default());
                cache.verify_recipe(&dir, recipe).unwrap();
                let starts = Arc::new(AtomicUsize::new(0));
                let withdraws = Arc::new(AtomicUsize::new(0));
                let publishes = Arc::new(AtomicUsize::new(0));
                let mut lifecycle = ModelLifecycle::new(
                    crate::model_lifecycle::StableNodeOwner {
                        run_id: "owner".into(),
                        pid: 1,
                        process_start_time_unix_s: 2,
                        gateway_port: 8080,
                    },
                    RestartProbeDriver {
                        starts: Arc::clone(&starts),
                        exit_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    },
                    RaceGateway {
                        withdraws: Arc::clone(&withdraws),
                        publishes: Arc::clone(&publishes),
                    },
                );
                if kind == OperationKind::Unload {
                    lifecycle
                        .load(
                            LaunchPlan {
                                model_id: recipe.id.into(),
                                artifact_path: dir.join(recipe.filename),
                                engine: "llama.cpp".into(),
                            },
                            &MutationCancellation::new(),
                        )
                        .unwrap();
                    lifecycle.complete_operation();
                }
                let baseline_starts = starts.load(Ordering::SeqCst);
                let baseline_withdraws = withdraws.load(Ordering::SeqCst);
                let baseline_publishes = publishes.load(Ordering::SeqCst);
                let (entered_tx, entered_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let (before_release, after_release) = if cancel_wins {
                    (Some(release_rx), None)
                } else {
                    (None, Some(release_rx))
                };
                lifecycle.set_destructive_test_hook(crate::model_lifecycle::DestructiveTestHook {
                    before_entered: cancel_wins.then_some(entered_tx.clone()),
                    before_release,
                    after_entered: (!cancel_wins).then_some(entered_tx),
                    after_release,
                });
                let (control, worker) = DownloadControl::spawn_with_lifecycle_components(
                    dir.clone(),
                    lifecycle,
                    cache,
                    recipes,
                );
                let id = if kind == OperationKind::Load {
                    control.start_load(recipe.id).unwrap()
                } else {
                    control.start_unload().unwrap()
                };
                entered_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("operation reaches destructive race hook");
                let cancelled = control.cancel(&id);
                release_tx.send(()).unwrap();
                let operation = (0..200)
                    .find_map(|_| {
                        let operation = control.operation(&id)?;
                        matches!(
                            operation.status,
                            OperationStatus::Succeeded
                                | OperationStatus::Failed
                                | OperationStatus::Cancelled
                        )
                        .then_some(operation)
                        .or_else(|| {
                            std::thread::sleep(Duration::from_millis(5));
                            None
                        })
                    })
                    .expect("operation reaches terminal state");

                if cancel_wins {
                    assert_eq!(cancelled, Ok(OperationStatus::Cancelled));
                    assert_eq!(operation.status, OperationStatus::Cancelled);
                    assert_eq!(starts.load(Ordering::SeqCst), baseline_starts);
                    assert_eq!(withdraws.load(Ordering::SeqCst), baseline_withdraws);
                    assert_eq!(publishes.load(Ordering::SeqCst), baseline_publishes);
                } else {
                    assert_eq!(cancelled, Err(DownloadControlError::CancellationNotSafe));
                    assert_eq!(operation.status, OperationStatus::Succeeded);
                    assert_eq!(withdraws.load(Ordering::SeqCst), baseline_withdraws + 1);
                    if kind == OperationKind::Load {
                        assert_eq!(starts.load(Ordering::SeqCst), baseline_starts + 1);
                        assert_eq!(publishes.load(Ordering::SeqCst), baseline_publishes + 1);
                    } else {
                        assert_eq!(starts.load(Ordering::SeqCst), baseline_starts);
                    }
                }
                assert_eq!(
                    control
                        .snapshot_since(0)
                        .events
                        .iter()
                        .filter(|event| event.operation.id == id
                            && matches!(
                                event.operation.status,
                                OperationStatus::Succeeded | OperationStatus::Cancelled
                            ))
                        .count(),
                    1
                );
                worker.stop_and_join().unwrap();
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }

    #[test]
    fn public_lifecycle_snapshot_correlates_transition_and_operation_under_one_lock() {
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            NoopLifecycleDriver,
            NoopGateway,
        );
        let snapshot = Arc::new(Mutex::new(lifecycle.snapshot()));
        let mut executor = LifecycleExecutor {
            lifecycle,
            snapshot: Arc::clone(&snapshot),
            models_dir: std::env::temp_dir(),
            verification_cache: Arc::new(VerificationCache::default()),
            recipes: REGISTRY,
            restart_verifier: Box::new(CacheRestartArtifactVerifier {
                cache: Arc::new(VerificationCache::default()),
            }),
        };

        executor
            .execute("op-unload", &Mutation::Unload, &MutationCancellation::new())
            .unwrap();
        let transitioning = snapshot.lock().unwrap().clone();
        assert_eq!(
            transitioning.status,
            crate::model_lifecycle::NodeLifecycleStatus::Unloading
        );
        assert_eq!(transitioning.operation_id.as_deref(), Some("op-unload"));

        executor.complete_operation();
        let completed = snapshot.lock().unwrap().clone();
        assert_eq!(
            completed.status,
            crate::model_lifecycle::NodeLifecycleStatus::Unloaded
        );
        assert_eq!(completed.operation_id, None);
    }

    #[test]
    fn lifecycle_admission_failure_never_leaves_a_queued_orphan() {
        let lifecycle = ModelLifecycle::new(
            crate::model_lifecycle::StableNodeOwner {
                run_id: "owner".into(),
                pid: 1,
                process_start_time_unix_s: 2,
                gateway_port: 8080,
            },
            NoopLifecycleDriver,
            NoopGateway,
        );
        let (control, worker) =
            DownloadControl::spawn_with_lifecycle(std::env::temp_dir(), lifecycle);
        control.stop_actor();
        assert_eq!(control.start_unload(), Err(DownloadControlError::Stopping));
        let snapshot = control.snapshot_since(0);
        let operation = snapshot.operations.last().unwrap();
        assert_eq!(operation.kind, OperationKind::Unload);
        assert_eq!(operation.status, OperationStatus::Failed);
        assert_eq!(operation.error.as_deref(), Some("node is stopping"));
        worker.stop_and_join().unwrap();
    }

    #[test]
    fn writer_overload_remains_distinct_at_the_durable_execution_boundary() {
        assert_eq!(
            map_control_state_error(ControlStateError::WriterOverloaded),
            DownloadControlError::WriterOverloaded
        );
    }

    #[test]
    fn legacy_control_cannot_yield_durable_execution_authority() {
        let (control, worker) = DownloadControl::spawn(std::env::temp_dir());
        assert!(control.durable_execution().is_none());
        worker.stop_and_join().unwrap();
    }

    #[test]
    fn node_builder_has_a_no_lifecycle_durable_constructor() {
        let _constructor: fn(
            PathBuf,
            ControlStateHandle,
        ) -> (DownloadControl, DownloadControlWorker) = DownloadControl::spawn_with_control_state;
    }

    #[tokio::test]
    async fn no_lifecycle_durable_authority_downloads_without_faking_slot_execution() {
        let root = std::env::temp_dir().join(format!(
            "loxa-durable-download-only-{}-{}",
            std::process::id(),
            loxa_protocol::v2::StreamEpoch::new_v4()
        ));
        let paths = crate::NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("run/managed.json"),
            logs_dir: root.join("run/logs"),
        };
        std::fs::create_dir_all(&paths.logs_dir).unwrap();
        let baseline = loxa_core::supervisor::ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "durable-download-only".into(),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: "loxa-durable-download-only-g0".into(),
            control_port: Some(19_432),
            port: 19_432,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
            .unwrap();
        let node_id = loxa_protocol::NodeId::new_v4();
        let fixture = crate::open_slice3_control_state_fixture(
            root.join("state/control-state.sqlite3"),
            node_id,
            paths,
            baseline,
        )
        .unwrap();
        fixture
            .handle
            .publish_instance(crate::control_state::InstancePublication {
                node_instance_id: loxa_protocol::NodeInstanceId::new_v4(),
                control_endpoint: "http://127.0.0.1:19432".into(),
                capabilities: loxa_protocol::v2::V2NodeCapabilities {
                    model_download: true,
                    slot_load: false,
                    slot_unload: false,
                    operation_cancel: true,
                    operation_stream: true,
                },
                now_unix_ms: 11,
            })
            .await
            .unwrap();
        let recipe = Box::leak(Box::new(ModelEntry {
            id: "download-only",
            repo: "owner/repo",
            revision: "pinned",
            filename: "download-only.gguf",
            sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
            size_bytes: 4,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.0,
        }));
        let (control, worker) = DownloadControl::spawn_durable_fixture_for_test(
            root.join("models"),
            Arc::new(VerificationCache::default()),
            std::slice::from_ref(recipe),
            b"good",
            fixture.handle.clone(),
        );
        assert!(control.durable_execution().is_some());
        assert_eq!(
            control.start_load_async(recipe.id).await,
            Err(DownloadControlError::Missing)
        );
        assert_eq!(
            control.start_unload_async().await,
            Err(DownloadControlError::Missing)
        );
        let durable = control.durable_execution().unwrap();
        let before_unsupported = fixture.handle.read_snapshot().unwrap();
        assert_eq!(
            durable.start_load(recipe.id).await,
            Err(DownloadControlError::ModelUnavailable)
        );
        assert_eq!(
            durable.start_unload().await,
            Err(DownloadControlError::ModelUnavailable)
        );
        let after_unsupported = fixture.handle.read_snapshot().unwrap();
        assert_eq!(after_unsupported.revision, before_unsupported.revision);
        assert_eq!(
            after_unsupported.operations, before_unsupported.operations,
            "unsupported lifecycle commands must not be durably admitted"
        );
        assert_eq!(
            control.start_download_async(recipe.id).await.unwrap(),
            "op-1"
        );
        assert_eq!(fixture.handle.read_snapshot().unwrap().operations.len(), 1);
        worker.stop_and_join().unwrap();
        fixture.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn successful_worker_shutdown_terminalizes_durably_queued_work_abandoned_by_actor_stop() {
        let root = std::env::temp_dir().join(format!(
            "loxa-durable-shutdown-{}-{}",
            std::process::id(),
            loxa_protocol::v2::StreamEpoch::new_v4()
        ));
        let paths = crate::NodePaths {
            models_dir: root.join("models"),
            state_path: root.join("run/managed.json"),
            logs_dir: root.join("run/logs"),
        };
        std::fs::create_dir_all(&paths.logs_dir).unwrap();
        let baseline = loxa_core::supervisor::ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "durable-shutdown".into(),
            model_id: None,
            owner_pid: std::process::id(),
            owner_process_start_time_unix_s: 1,
            stop_requested: false,
            lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
            generation: 0,
            generation_alias: "loxa-durable-shutdown-g0".into(),
            control_port: Some(19_433),
            port: 19_433,
            log_path: paths.logs_dir.join("owner.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
            .unwrap();
        let fixture = crate::open_slice3_control_state_fixture(
            root.join("state/control-state.sqlite3"),
            loxa_protocol::NodeId::new_v4(),
            paths,
            baseline,
        )
        .unwrap();
        fixture
            .handle
            .publish_instance(crate::control_state::InstancePublication {
                node_instance_id: loxa_protocol::NodeInstanceId::new_v4(),
                control_endpoint: "http://127.0.0.1:19433".into(),
                capabilities: loxa_protocol::v2::V2NodeCapabilities {
                    model_download: true,
                    slot_load: false,
                    slot_unload: false,
                    operation_cancel: true,
                    operation_stream: true,
                },
                now_unix_ms: 11,
            })
            .await
            .unwrap();
        let recipes: &'static [ModelEntry] = Box::leak(
            vec![
                ModelEntry {
                    id: "shutdown-running",
                    repo: "owner/repo",
                    revision: "pinned",
                    filename: "shutdown-running.gguf",
                    sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
                    size_bytes: 4,
                    license: "apache-2.0",
                    params: "tiny",
                    quant: "Q4",
                    min_free_mem_gb: 0.0,
                },
                ModelEntry {
                    id: "shutdown-queued",
                    repo: "owner/repo",
                    revision: "pinned",
                    filename: "shutdown-queued.gguf",
                    sha256: "770e607624d689265ca6c44884d0807d9b054d23c473c106c72be9de08b7376c",
                    size_bytes: 4,
                    license: "apache-2.0",
                    params: "tiny",
                    quant: "Q4",
                    min_free_mem_gb: 0.0,
                },
            ]
            .into_boxed_slice(),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (control, worker) = DownloadControl::spawn_with_control_state_components(
            root.join("models"),
            Arc::new(VerificationCache::default()),
            recipes,
            Box::new(ShutdownBlockingDownloader {
                entered: entered_tx,
            }),
            fixture.handle.clone(),
            false,
        );
        let durable = control.durable_execution().unwrap();
        let running = durable.start_download(recipes[0].id, 4).await.unwrap();
        entered_rx.recv().unwrap();
        let queued = durable.start_download(recipes[1].id, 4).await.unwrap();
        assert_eq!(
            fixture
                .handle
                .read_snapshot()
                .unwrap()
                .operations
                .iter()
                .find(|operation| operation.operation_id == queued.operation_id)
                .unwrap()
                .status,
            V2OperationStatus::Queued
        );
        fixture.handle.begin_stopping(12).await.unwrap();

        std::thread::spawn(move || worker.stop_and_join())
            .join()
            .unwrap()
            .unwrap();

        let state = fixture.handle.read_snapshot().unwrap();
        let running = state
            .operations
            .iter()
            .find(|operation| operation.operation_id == running.operation_id)
            .unwrap();
        assert!(matches!(
            running.status,
            V2OperationStatus::Cancelled | V2OperationStatus::Failed
        ));
        let queued = state
            .operations
            .iter()
            .find(|operation| operation.operation_id == queued.operation_id)
            .unwrap();
        assert_eq!(queued.status, V2OperationStatus::Failed);
        assert_eq!(
            queued.error.as_ref().map(|error| error.code),
            Some(V2OperationErrorCode::DownloadFailed)
        );
        assert_eq!(
            queued.error.as_ref().map(|error| error.message.as_str()),
            Some("node is stopping")
        );

        fixture.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
}
