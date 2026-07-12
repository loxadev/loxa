use crate::actor::{
    Mutation, MutationCancellation, MutationExecutor, NodeActor, NodeActorHandle, SubmitError,
};
use loxa_core::control::contracts::{
    OperationKind, OperationStatus, OperationView, ReconnectSnapshot,
};
use loxa_core::control::operations::{
    CancellationSafety, EventSubscription, OperationError, OperationStore,
};
use loxa_core::download::{self, DownloadError, DownloadObserver, DownloadProgress};
use loxa_core::model_inventory::{
    VerificationCache, VerificationCancellation, VerifiedArtifact, VerifiedRecipeInventoryEntry,
};
use loxa_core::registry::{ModelEntry, REGISTRY};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

const OPERATION_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct DownloadControl {
    operations: Arc<Mutex<OperationStore>>,
    actor: NodeActorHandle,
    models_dir: Arc<PathBuf>,
    verification_cache: Arc<VerificationCache>,
    recipes: &'static [ModelEntry],
}

pub struct DownloadControlWorker {
    actor: NodeActorHandle,
    worker: Option<JoinHandle<()>>,
    verification: Option<VerificationWorker>,
}

struct VerificationWorker {
    cancellation: MutationCancellation,
    worker: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadControlError {
    Conflict,
    Missing,
    Terminal,
    Stopping,
}

impl DownloadControl {
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
            operations: Arc::clone(&operations),
            downloader,
            recipes,
            verification_cancellation: verification_cancellation.clone(),
            verifier: Box::new(CacheArtifactVerifier {
                cache: Arc::clone(&verification_cache),
            }),
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
                operations,
                actor: actor.clone(),
                models_dir,
                verification_cache,
                recipes,
            },
            DownloadControlWorker {
                actor,
                worker: Some(worker),
                verification: Some(VerificationWorker {
                    cancellation: verification_cancellation,
                    worker: verification_worker,
                }),
            },
        )
    }

    pub fn start(&self, model_id: &str) -> Result<String, DownloadControlError> {
        if find_recipe(self.recipes, model_id).is_none() {
            return Err(DownloadControlError::Missing);
        }
        let now = now_ms();
        let id = self
            .operations
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
                    .operations
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
        let mut operations = self.operations.lock().expect("operation store poisoned");
        let operation = operations.get(id).ok_or(DownloadControlError::Missing)?;
        if matches!(
            operation.status,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Cancelled
        ) {
            return Err(DownloadControlError::Terminal);
        }
        self.actor.cancel(id);
        operations
            .cancel(id, CancellationSafety::Safe, now_ms())
            .map_err(map_operation_error)
    }

    pub fn operation(&self, id: &str) -> Option<OperationView> {
        self.operations
            .lock()
            .expect("operation store poisoned")
            .get(id)
    }

    pub fn snapshot_since(&self, cursor: u64) -> ReconnectSnapshot {
        self.operations
            .lock()
            .expect("operation store poisoned")
            .snapshot_since(cursor)
    }

    pub fn subscribe(&self) -> EventSubscription {
        self.operations
            .lock()
            .expect("operation store poisoned")
            .subscribe()
    }

    pub fn subscribe_with_snapshot(&self, cursor: u64) -> (ReconnectSnapshot, EventSubscription) {
        self.operations
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
    fn stop_actor(&self) {
        self.actor.stop();
    }
}

impl DownloadControlWorker {
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    pub fn stop_and_join(mut self) -> std::io::Result<()> {
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
        actor_result.and(verification_result)
    }
}

struct DownloadExecutor {
    models_dir: PathBuf,
    operations: Arc<Mutex<OperationStore>>,
    downloader: Box<dyn ModelDownloader>,
    verification_cancellation: MutationCancellation,
    verifier: Box<dyn ArtifactVerifier>,
    recipes: &'static [ModelEntry],
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
    operations: Arc<Mutex<OperationStore>>,
}

impl DownloadObserver for OperationObserver<'_> {
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn progress(&mut self, progress: DownloadProgress) {
        let _ = self
            .operations
            .lock()
            .expect("operation store poisoned")
            .progress(
                self.id,
                progress.downloaded_bytes,
                Some(progress.total_bytes),
                now_ms(),
            );
    }
}

impl MutationExecutor for DownloadExecutor {
    fn execute(&mut self, id: &str, mutation: &Mutation, cancellation: &MutationCancellation) {
        let Mutation::Download { model_id } = mutation else {
            return;
        };
        if self
            .operations
            .lock()
            .expect("operation store poisoned")
            .start(id, now_ms())
            .is_err()
        {
            return;
        }
        let Some(recipe) = find_recipe(self.recipes, model_id) else {
            let _ = self
                .operations
                .lock()
                .expect("operation store poisoned")
                .fail(id, "unknown registry model", now_ms());
            return;
        };
        let mut observer = OperationObserver {
            id,
            cancellation,
            operations: Arc::clone(&self.operations),
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
        let mut operations = self.operations.lock().expect("operation store poisoned");
        if operations
            .get(id)
            .is_some_and(|view| view.status == OperationStatus::Cancelled)
        {
            return;
        }
        match result {
            Ok(()) => match verification.expect("successful download was verified") {
                Ok(evidence)
                    if evidence.matches
                        && evidence.size_bytes == recipe.size_bytes
                        && evidence.expected_sha256 == recipe.sha256 =>
                {
                    let _ = operations.succeed(id, now_ms());
                }
                Ok(_) => {
                    let _ = operations.fail(
                        id,
                        "downloaded artifact failed checksum verification",
                        now_ms(),
                    );
                }
                Err(_) if cancellation.is_cancelled() => {
                    let _ = operations.cancel(id, CancellationSafety::Safe, now_ms());
                }
                Err(_) => {
                    self.verifier.invalidate(&self.models_dir, recipe);
                    let _ = operations.fail(
                        id,
                        "downloaded artifact could not be verified safely",
                        now_ms(),
                    );
                }
            },
            Err(DownloadError::Cancelled) => {
                let _ = operations.cancel(id, CancellationSafety::Safe, now_ms());
            }
            Err(error) => {
                let _ = operations.fail(id, public_download_error(&error), now_ms());
            }
        }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::model_inventory::{ArtifactState, VerificationCache, VerifiedArtifact};
    use loxa_core::registry::{self, ModelEntry};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct FakeDownloader {
        result: Option<Result<(), DownloadError>>,
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

    fn execute_fake(result: Result<(), DownloadError>) -> OperationView {
        let operations = Arc::new(Mutex::new(OperationStore::new(8)));
        let id = operations
            .lock()
            .unwrap()
            .enqueue_unique(OperationKind::Download, Some("gemma-3-4b-it-q4".into()), 1)
            .unwrap();
        let mut executor = DownloadExecutor {
            models_dir: PathBuf::from("/unused"),
            operations: Arc::clone(&operations),
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
            operations: Arc::clone(&operations),
            downloader: Box::new(FakeDownloader {
                result: Some(Ok(())),
            }),
            verification_cancellation: MutationCancellation::new(),
            verifier: Box::new(FakeArtifactVerifier {
                calls: Arc::clone(&calls),
                result: Some(Err(std::io::Error::other("verification unavailable"))),
            }),
            recipes: REGISTRY,
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
            operations: Arc::clone(&operations),
            downloader: Box::new(FixtureDownloader { bytes: b"good" }),
            verification_cancellation,
            verifier: Box::new(GatedArtifactVerifier {
                entered: entered_tx,
                release: release_rx,
                cache: Arc::clone(&cache),
            }),
            recipes,
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
}
