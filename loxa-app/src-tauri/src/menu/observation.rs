#![cfg_attr(all(debug_assertions, not(test)), allow(dead_code))]

use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(any(test, debug_assertions)))]
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(not(any(test, debug_assertions)))]
use loxa::app::AppService;
use loxa::app::{
    AppSnapshot, BundleSnapshot, BundleUnavailableReason, DownloadSnapshot, RecommendationSnapshot,
    RecommendationUnavailableReason, RuntimeInventorySnapshot, RuntimeSnapshot,
};

use crate::menu::presentation::{
    Bundle, Download, MenuSnapshot, Recommendation, RecommendationUnavailableReason as MenuReason,
    RecoveryReason, Runtime, RuntimeInventory,
};

const FRESHNESS: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreBundle {
    Absent,
    Partial,
    Verified { target_bytes: u64, draft_bytes: u64 },
    Unavailable(CoreBundleUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreBundleUnavailableReason {
    Invalid,
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreRecommendation {
    Hidden,
    Available { target_bytes: u64, draft_bytes: u64 },
    Unavailable(CoreRecommendationUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreRecommendationUnavailableReason {
    InsufficientMemory,
    InsufficientDisk,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreDownload {
    Idle,
    Paused {
        completed_bytes: u64,
        total_bytes: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreRuntime {
    Idle,
    Starting,
    Running,
    Stopping,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreRuntimeInventory {
    External,
    Missing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoreObservation {
    bundle: CoreBundle,
    recommendation: CoreRecommendation,
    download: CoreDownload,
    runtime: CoreRuntime,
    runtime_inventory: CoreRuntimeInventory,
}

impl From<AppSnapshot> for CoreObservation {
    fn from(snapshot: AppSnapshot) -> Self {
        let bundle = match snapshot.bundle() {
            BundleSnapshot::Absent => CoreBundle::Absent,
            BundleSnapshot::Partial(_) => CoreBundle::Partial,
            BundleSnapshot::Verified(bundle) => CoreBundle::Verified {
                target_bytes: bundle.target_bytes(),
                draft_bytes: bundle.draft_bytes(),
            },
            BundleSnapshot::Unavailable(reason) => CoreBundle::Unavailable(match reason {
                BundleUnavailableReason::Invalid => CoreBundleUnavailableReason::Invalid,
                BundleUnavailableReason::Busy => CoreBundleUnavailableReason::Busy,
            }),
        };
        let recommendation = match snapshot.recommendation() {
            RecommendationSnapshot::Hidden => CoreRecommendation::Hidden,
            RecommendationSnapshot::Available(bundle) => CoreRecommendation::Available {
                target_bytes: bundle.target_bytes(),
                draft_bytes: bundle.draft_bytes(),
            },
            RecommendationSnapshot::Unavailable(reason) => {
                CoreRecommendation::Unavailable(match reason {
                    RecommendationUnavailableReason::InsufficientMemory => {
                        CoreRecommendationUnavailableReason::InsufficientMemory
                    }
                    RecommendationUnavailableReason::InsufficientDisk => {
                        CoreRecommendationUnavailableReason::InsufficientDisk
                    }
                    RecommendationUnavailableReason::Unavailable => {
                        CoreRecommendationUnavailableReason::Unavailable
                    }
                })
            }
        };
        let download = match snapshot.download() {
            DownloadSnapshot::Idle => CoreDownload::Idle,
            DownloadSnapshot::Paused(download) => CoreDownload::Paused {
                completed_bytes: download.completed_bytes(),
                total_bytes: download.total_bytes(),
            },
        };
        let runtime = match snapshot.runtime() {
            RuntimeSnapshot::Idle => CoreRuntime::Idle,
            RuntimeSnapshot::Starting => CoreRuntime::Starting,
            RuntimeSnapshot::Running => CoreRuntime::Running,
            RuntimeSnapshot::Stopping => CoreRuntime::Stopping,
            RuntimeSnapshot::Error => CoreRuntime::Error,
        };
        let runtime_inventory = match snapshot.runtime_inventory() {
            RuntimeInventorySnapshot::External => CoreRuntimeInventory::External,
            RuntimeInventorySnapshot::Missing => CoreRuntimeInventory::Missing,
        };

        Self {
            bundle,
            recommendation,
            download,
            runtime,
            runtime_inventory,
        }
    }
}

pub(crate) fn map_app_snapshot(snapshot: AppSnapshot) -> MenuSnapshot {
    map_core_snapshot(snapshot.into())
}

fn map_core_snapshot(snapshot: CoreObservation) -> MenuSnapshot {
    let bundle = match snapshot.bundle {
        CoreBundle::Absent => Bundle::Absent,
        CoreBundle::Partial => Bundle::Partial,
        CoreBundle::Verified {
            target_bytes,
            draft_bytes,
        } => Bundle::verified(target_bytes, draft_bytes),
        CoreBundle::Unavailable(reason) => Bundle::recovery(match reason {
            CoreBundleUnavailableReason::Invalid => RecoveryReason::Invalid,
            CoreBundleUnavailableReason::Busy => RecoveryReason::Busy,
        }),
    };
    let recommendation = match snapshot.recommendation {
        CoreRecommendation::Hidden => Recommendation::Hidden,
        CoreRecommendation::Available {
            target_bytes,
            draft_bytes,
        } => Recommendation::available(target_bytes, draft_bytes),
        CoreRecommendation::Unavailable(reason) => {
            Recommendation::unavailable_without_size(match reason {
                CoreRecommendationUnavailableReason::InsufficientMemory => {
                    MenuReason::InsufficientMemory
                }
                CoreRecommendationUnavailableReason::InsufficientDisk => {
                    MenuReason::InsufficientDisk
                }
                CoreRecommendationUnavailableReason::Unavailable => MenuReason::Unavailable,
            })
        }
    };
    let download = match snapshot.download {
        CoreDownload::Idle => Download::Idle,
        CoreDownload::Paused {
            completed_bytes,
            total_bytes,
        } => Download::paused(completed_bytes, total_bytes),
    };
    let runtime = match snapshot.runtime {
        CoreRuntime::Idle => Runtime::Idle,
        CoreRuntime::Starting => Runtime::Starting,
        CoreRuntime::Running => Runtime::Running,
        CoreRuntime::Stopping => Runtime::Stopping,
        CoreRuntime::Error => Runtime::Error,
    };
    let runtime_inventory = match snapshot.runtime_inventory {
        CoreRuntimeInventory::External => RuntimeInventory::External,
        CoreRuntimeInventory::Missing => RuntimeInventory::Missing,
    };

    MenuSnapshot::new(bundle, recommendation, download, runtime, runtime_inventory)
        .expect("core app snapshots always map to canonical menu snapshots")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservationMessage {
    Snapshot(MenuSnapshot),
    Error(String),
}

pub(crate) struct RefreshAdmission {
    last_completed: Option<Instant>,
    in_flight: bool,
    accepting: bool,
}

impl RefreshAdmission {
    fn new() -> Self {
        Self {
            last_completed: None,
            in_flight: false,
            accepting: true,
        }
    }

    fn admit_startup(&mut self) -> bool {
        if !self.accepting || self.in_flight {
            return false;
        }
        self.in_flight = true;
        true
    }

    fn admit_popover(&mut self, now: Instant) -> bool {
        if !self.accepting || self.in_flight {
            return false;
        }
        let Some(last_completed) = self.last_completed else {
            return false;
        };
        if now.saturating_duration_since(last_completed) < FRESHNESS {
            return false;
        }
        self.in_flight = true;
        true
    }

    fn complete(&mut self, _message: &ObservationMessage, now: Instant) {
        self.in_flight = false;
        self.last_completed = Some(now);
    }

    fn close(&mut self) {
        self.accepting = false;
    }

    #[cfg(any(test, not(debug_assertions)))]
    fn is_shutting_down(&self) -> bool {
        !self.accepting
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRequest {
    Observe,
    Stop,
}

trait SnapshotSource {
    fn snapshot(&mut self) -> MenuSnapshot;
}

#[cfg(not(any(test, debug_assertions)))]
struct AppSnapshotSource(AppService);

#[cfg(not(any(test, debug_assertions)))]
impl SnapshotSource for AppSnapshotSource {
    fn snapshot(&mut self) -> MenuSnapshot {
        map_app_snapshot(self.0.snapshot())
    }
}

fn run_worker<S: SnapshotSource>(
    mut source: Result<S, String>,
    request_receiver: Receiver<WorkerRequest>,
    message_sender: Sender<ObservationMessage>,
    stopping: &AtomicBool,
) {
    while let Ok(request) = request_receiver.recv() {
        match request {
            WorkerRequest::Stop => break,
            WorkerRequest::Observe => {
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                let message = match &mut source {
                    Ok(source) => ObservationMessage::Snapshot(source.snapshot()),
                    Err(error) => ObservationMessage::Error(error.clone()),
                };
                if message_sender.send(message).is_err() {
                    break;
                }
            }
        }
    }
}

pub(crate) struct ObservationClient {
    request_sender: Option<Sender<WorkerRequest>>,
    receiver: Option<Receiver<ObservationMessage>>,
    admission: RefreshAdmission,
    stopping: Arc<AtomicBool>,
}

impl ObservationClient {
    #[cfg(not(any(test, debug_assertions)))]
    pub(crate) fn start() -> Self {
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, receiver) = mpsc::channel();
        let worker_sender = message_sender.clone();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let spawn = std::thread::Builder::new()
            .name("loxa-menu-observation".to_owned())
            .spawn(move || {
                run_worker(
                    AppService::from_env().map(AppSnapshotSource),
                    request_receiver,
                    worker_sender,
                    &worker_stopping,
                );
            });

        if let Err(error) = spawn {
            let _ = message_sender.send(ObservationMessage::Error(format!(
                "Failed to start the observation worker: {error}"
            )));
        }
        drop(message_sender);

        let mut client = Self {
            request_sender: Some(request_sender),
            receiver: Some(receiver),
            admission: RefreshAdmission::new(),
            stopping,
        };
        client.request_startup();
        client
    }

    #[cfg(not(any(test, debug_assertions)))]
    fn request_startup(&mut self) {
        if self.admission.admit_startup() {
            self.send_observation_request();
        }
    }

    pub(crate) fn request_popover_open(&mut self, now: Instant) -> bool {
        if !self.admission.admit_popover(now) {
            return false;
        }
        self.send_observation_request();
        true
    }

    fn send_observation_request(&mut self) {
        let Some(sender) = self.request_sender.as_ref() else {
            return;
        };
        let _ = sender.send(WorkerRequest::Observe);
    }

    #[cfg(any(test, not(debug_assertions)))]
    pub(crate) fn drain(&mut self, now: Instant) -> Option<ObservationMessage> {
        let drained = {
            let receiver = self.receiver.as_ref()?;
            drain_observations(receiver, self.admission.is_shutting_down())?
        };
        if drained.completed {
            self.admission.complete(&drained.message, now);
        }
        if drained.disconnected {
            self.admission.close();
            self.receiver.take();
            self.request_sender.take();
        }
        Some(drained.message)
    }

    pub(crate) fn shutdown(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.admission.close();
        self.receiver.take();
        if let Some(sender) = self.request_sender.take() {
            let _ = sender.send(WorkerRequest::Stop);
        }
    }

    #[cfg(test)]
    fn from_parts_for_test(
        request_sender: Sender<WorkerRequest>,
        receiver: Receiver<ObservationMessage>,
    ) -> Self {
        Self {
            request_sender: Some(request_sender),
            receiver: Some(receiver),
            admission: RefreshAdmission::new(),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DrainedObservation {
    message: ObservationMessage,
    completed: bool,
    disconnected: bool,
}

fn drain_observations(
    receiver: &Receiver<ObservationMessage>,
    shutting_down: bool,
) -> Option<DrainedObservation> {
    let mut pending = None;
    let mut completed = false;
    loop {
        match receiver.try_recv() {
            Ok(ObservationMessage::Snapshot(snapshot)) => {
                completed = true;
                if !matches!(pending, Some(ObservationMessage::Error(_))) {
                    pending = Some(ObservationMessage::Snapshot(snapshot));
                }
            }
            Ok(ObservationMessage::Error(error)) => {
                completed = true;
                pending = Some(ObservationMessage::Error(error));
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                if !shutting_down {
                    return Some(DrainedObservation {
                        message: match pending {
                            Some(ObservationMessage::Error(error)) => {
                                ObservationMessage::Error(error)
                            }
                            Some(ObservationMessage::Snapshot(_)) | None => {
                                ObservationMessage::Error(
                                    "The observation worker disconnected".to_owned(),
                                )
                            }
                        },
                        completed,
                        disconnected: true,
                    });
                }
                break;
            }
        }
    }
    pending.map(|message| DrainedObservation {
        message,
        completed,
        disconnected: false,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::{Duration, Instant};

    use loxa::app::AppSnapshot;

    use super::{
        drain_observations, map_app_snapshot, map_core_snapshot, run_worker, CoreBundle,
        CoreDownload, CoreObservation, CoreRecommendation, CoreRecommendationUnavailableReason,
        ObservationClient, ObservationMessage, RefreshAdmission, SnapshotSource, WorkerRequest,
    };
    use crate::menu::presentation::{Fixture, MenuSnapshot};

    fn assert_send_static<T: Send + 'static>() {}

    #[test]
    fn mapper_keeps_idle_paused_and_sizeless_unavailable_truthful() {
        let idle = map_core_snapshot(CoreObservation {
            bundle: CoreBundle::Absent,
            recommendation: CoreRecommendation::Unavailable(
                CoreRecommendationUnavailableReason::Unavailable,
            ),
            download: CoreDownload::Idle,
            runtime: super::CoreRuntime::Idle,
            runtime_inventory: super::CoreRuntimeInventory::Missing,
        });
        let unavailable = idle
            .recommendation_row()
            .expect("a core unavailable recommendation remains visible");
        assert_eq!(unavailable.subtitle(), None);
        assert_eq!(unavailable.size_detail(), None);
        assert_eq!(
            unavailable.disabled_reason(),
            Some("Unavailable on this Mac")
        );
        assert!(idle.transfer_row().is_none());

        let paused = map_core_snapshot(CoreObservation {
            bundle: CoreBundle::Partial,
            recommendation: CoreRecommendation::Hidden,
            download: CoreDownload::Paused {
                completed_bytes: 3,
                total_bytes: 5,
            },
            runtime: super::CoreRuntime::Running,
            runtime_inventory: super::CoreRuntimeInventory::External,
        });
        let transfer = paused
            .transfer_row()
            .expect("a core paused download remains a transfer row");
        assert_eq!(transfer.phase_label(), "Paused");
        assert_eq!(transfer.progress_detail(), "3 of 5 bytes");
        assert!(!transfer.has_fixture_action());

        let _mapper: fn(AppSnapshot) -> MenuSnapshot = map_app_snapshot;
    }

    #[test]
    fn refresh_admission_observes_once_then_gates_popover_requests_on_completed_freshness() {
        let started = Instant::now();
        let mut admission = RefreshAdmission::new();

        assert!(admission.admit_startup());
        assert!(!admission.admit_startup());
        assert!(!admission.admit_popover(started + Duration::from_secs(60)));

        admission.complete(
            &ObservationMessage::Snapshot(Fixture::Empty.snapshot()),
            started,
        );
        assert!(!admission.admit_popover(started + Duration::from_secs(59)));
        assert!(admission.admit_popover(started + Duration::from_secs(60)));
        assert!(!admission.admit_popover(started + Duration::from_secs(120)));

        admission.complete(
            &ObservationMessage::Error("LOXA_HOME is invalid".to_owned()),
            started + Duration::from_secs(60),
        );
        assert!(!admission.admit_popover(started + Duration::from_secs(119)));
        assert!(admission.admit_popover(started + Duration::from_secs(120)));
    }

    #[test]
    fn receiver_drain_coalesces_snapshots_preserves_errors_and_handles_disconnects() {
        let (sender, receiver) = mpsc::channel();
        sender
            .send(ObservationMessage::Snapshot(Fixture::Empty.snapshot()))
            .unwrap();
        sender
            .send(ObservationMessage::Snapshot(Fixture::Installed.snapshot()))
            .unwrap();
        sender
            .send(ObservationMessage::Error("setup failed".to_owned()))
            .unwrap();
        sender
            .send(ObservationMessage::Snapshot(Fixture::Running.snapshot()))
            .unwrap();

        assert_eq!(
            drain_observations(&receiver, false).map(|drained| drained.message),
            Some(ObservationMessage::Error("setup failed".to_owned()))
        );

        let (sender, receiver) = mpsc::channel();
        sender
            .send(ObservationMessage::Snapshot(Fixture::Empty.snapshot()))
            .unwrap();
        sender
            .send(ObservationMessage::Snapshot(Fixture::Installed.snapshot()))
            .unwrap();
        assert_eq!(
            drain_observations(&receiver, false).map(|drained| drained.message),
            Some(ObservationMessage::Snapshot(Fixture::Installed.snapshot()))
        );

        let (sender, receiver) = mpsc::channel::<ObservationMessage>();
        drop(sender);
        assert_eq!(
            drain_observations(&receiver, false).map(|drained| drained.message),
            Some(ObservationMessage::Error(
                "The observation worker disconnected".to_owned()
            ))
        );
        assert_eq!(drain_observations(&receiver, true), None);

        let (sender, receiver) = mpsc::channel();
        sender
            .send(ObservationMessage::Error("setup failed".to_owned()))
            .unwrap();
        drop(sender);
        let drained = drain_observations(&receiver, false)
            .expect("an explicit setup error remains visible after disconnect");
        assert_eq!(
            drained.message,
            ObservationMessage::Error("setup failed".to_owned())
        );
        assert!(drained.completed);
        assert!(drained.disconnected);
    }

    #[test]
    fn worker_reads_only_admitted_requests_and_checks_stop_between_reads() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        request_sender.send(WorkerRequest::Observe).unwrap();
        request_sender.send(WorkerRequest::Stop).unwrap();

        run_worker(
            Ok(FakeReader {
                reads: reads.clone(),
                snapshot: Fixture::Installed.snapshot(),
            }),
            request_receiver,
            message_sender,
            &AtomicBool::new(false),
        );

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            message_receiver.recv().unwrap(),
            ObservationMessage::Snapshot(Fixture::Installed.snapshot())
        );
        assert!(message_receiver.try_recv().is_err());

        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        request_sender.send(WorkerRequest::Observe).unwrap();
        request_sender.send(WorkerRequest::Stop).unwrap();
        run_worker::<FakeReader>(
            Err("set LOXA_HOME, HOME, or USERPROFILE".to_owned()),
            request_receiver,
            message_sender,
            &AtomicBool::new(false),
        );
        assert_eq!(
            message_receiver.recv().unwrap(),
            ObservationMessage::Error("set LOXA_HOME, HOME, or USERPROFILE".to_owned())
        );
    }

    #[test]
    fn shutdown_requested_before_worker_dispatch_skips_a_queued_observation() {
        let reads = Arc::new(AtomicUsize::new(0));
        let stopping = Arc::new(AtomicBool::new(true));
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        request_sender.send(WorkerRequest::Observe).unwrap();
        drop(request_sender);

        run_worker(
            Ok(StopSensitiveReader {
                reads: reads.clone(),
                stopping: stopping.clone(),
                snapshot: Fixture::Installed.snapshot(),
            }),
            request_receiver,
            message_sender,
            &stopping,
        );

        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(message_receiver.try_recv().is_err());
    }

    #[test]
    fn shutdown_closes_admission_drops_the_receiver_and_signals_stop_without_joining() {
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        let mut client = ObservationClient::from_parts_for_test(request_sender, message_receiver);

        client.shutdown();

        assert!(client.stopping.load(Ordering::Acquire));
        assert_eq!(request_receiver.recv().unwrap(), WorkerRequest::Stop);
        assert!(message_sender
            .send(ObservationMessage::Snapshot(Fixture::Empty.snapshot()))
            .is_err());
        assert!(!client.request_popover_open(Instant::now()));
    }

    #[test]
    fn unexpected_disconnect_renders_an_error_without_advancing_freshness() {
        let (request_sender, _request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        let mut client = ObservationClient::from_parts_for_test(request_sender, message_receiver);
        let now = Instant::now();
        drop(message_sender);

        assert_eq!(
            client.drain(now),
            Some(ObservationMessage::Error(
                "The observation worker disconnected".to_owned()
            ))
        );
        assert_eq!(client.admission.last_completed, None);
        assert!(!client.admission.accepting);
    }

    #[test]
    fn observation_messages_are_owned_send_values() {
        assert_send_static::<ObservationMessage>();
    }

    struct FakeReader {
        reads: Arc<AtomicUsize>,
        snapshot: MenuSnapshot,
    }

    impl SnapshotSource for FakeReader {
        fn snapshot(&mut self) -> MenuSnapshot {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.snapshot.clone()
        }
    }

    struct StopSensitiveReader {
        reads: Arc<AtomicUsize>,
        stopping: Arc<AtomicBool>,
        snapshot: MenuSnapshot,
    }

    impl SnapshotSource for StopSensitiveReader {
        fn snapshot(&mut self) -> MenuSnapshot {
            assert!(
                !self.stopping.load(Ordering::SeqCst),
                "snapshot started after stop was requested"
            );
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.snapshot.clone()
        }
    }
}
