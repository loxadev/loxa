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
use loxa_core::registry;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

const OPERATION_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct DownloadControl {
    operations: Arc<Mutex<OperationStore>>,
    actor: NodeActorHandle,
}

pub struct DownloadControlWorker {
    actor: NodeActorHandle,
    worker: Option<JoinHandle<()>>,
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
        let operations = Arc::new(Mutex::new(OperationStore::new(OPERATION_CAPACITY)));
        let executor = DownloadExecutor {
            models_dir,
            operations: Arc::clone(&operations),
            downloader: Box::new(VerifiedDownloader),
        };
        let (actor, worker) = NodeActor::spawn(executor);
        (
            Self {
                operations,
                actor: actor.clone(),
            },
            DownloadControlWorker {
                actor,
                worker: Some(worker),
            },
        )
    }

    pub fn start(&self, model_id: &str) -> Result<String, DownloadControlError> {
        if registry::find(model_id).is_none() {
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
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| std::io::Error::other("download actor worker panicked"))?;
        }
        Ok(())
    }
}

struct DownloadExecutor {
    models_dir: PathBuf,
    operations: Arc<Mutex<OperationStore>>,
    downloader: Box<dyn ModelDownloader>,
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
        let Some(recipe) = registry::find(model_id) else {
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
        let mut operations = self.operations.lock().expect("operation store poisoned");
        if operations
            .get(id)
            .is_some_and(|view| view.status == OperationStatus::Cancelled)
        {
            return;
        }
        match result {
            Ok(_) => {
                let _ = operations.succeed(id, now_ms());
            }
            Err(DownloadError::Cancelled) => {
                let _ = operations.cancel(id, CancellationSafety::Safe, now_ms());
            }
            Err(error) => {
                let _ = operations.fail(id, public_download_error(&error), now_ms());
            }
        }
    }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct FakeDownloader {
        result: Option<Result<(), DownloadError>>,
    }

    struct PanicExecutor(std::sync::mpsc::Sender<()>);

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
        };
        assert_eq!(
            runtime.stop_and_join().unwrap_err().to_string(),
            "download actor worker panicked"
        );
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
