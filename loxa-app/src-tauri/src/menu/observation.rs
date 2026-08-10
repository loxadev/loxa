use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use loxa::app::TransferControl;
#[cfg(not(test))]
use loxa::app::{
    AppService, DiscardCandidate, TransferDisposition, TransferPhase, TransferProgress,
    TransferSelected,
};
use loxa::app::{
    AppSnapshot, BundleSnapshot, BundleUnavailableReason, DownloadSnapshot, RecommendationSnapshot,
    RecommendationUnavailableReason, RuntimeInventorySnapshot, RuntimeOwnerSnapshot,
    RuntimeSnapshot,
};
#[cfg(not(test))]
use loxa::discovery::{CandidateDisposition, InspectRepository, SearchModels};
use loxa::huggingface::ResolvedFile;

use crate::menu::catalog::{
    CandidateItem, CandidateTransferIntent, CatalogEvent, CatalogTransferDisposition,
    RepositoryItem, TransferStage,
};
use crate::menu::incomplete::{DiscardFailure, IncompleteInventoryError, IncompleteItem};
use crate::menu::installed::{InstalledInventoryError, InstalledItem};
use crate::menu::presentation::{
    Bundle, Download, MenuSnapshot, ObservedRuntimeOwner, Recommendation,
    RecommendationUnavailableReason as MenuReason, RecoveryReason, Runtime, RuntimeInventory,
};

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
enum CoreRuntimeOwner {
    Legacy,
    Foreground,
    PersistentApp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoreObservation {
    bundle: CoreBundle,
    recommendation: CoreRecommendation,
    download: CoreDownload,
    runtime: CoreRuntime,
    runtime_port: Option<u16>,
    runtime_owner: Option<CoreRuntimeOwner>,
    runtime_model_id: Option<String>,
    bundle_model_id: String,
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
        let runtime_port = snapshot.runtime_port();
        let runtime_owner = snapshot.runtime_owner().map(|owner| match owner {
            RuntimeOwnerSnapshot::Legacy => CoreRuntimeOwner::Legacy,
            RuntimeOwnerSnapshot::Foreground => CoreRuntimeOwner::Foreground,
            RuntimeOwnerSnapshot::PersistentApp => CoreRuntimeOwner::PersistentApp,
        });
        let runtime_model_id = snapshot.runtime_model_id().map(str::to_owned);
        let bundle_model_id = snapshot.bundle_model_id().to_owned();
        let runtime_inventory = match snapshot.runtime_inventory() {
            RuntimeInventorySnapshot::External => CoreRuntimeInventory::External,
            RuntimeInventorySnapshot::Missing => CoreRuntimeInventory::Missing,
        };

        Self {
            bundle,
            recommendation,
            download,
            runtime,
            runtime_port,
            runtime_owner,
            runtime_model_id,
            bundle_model_id,
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

    let menu = MenuSnapshot::new(bundle, recommendation, download, runtime, runtime_inventory)
        .expect("core app snapshots always map to canonical menu snapshots")
        .with_bundle_model_id(snapshot.bundle_model_id)
        .expect("core app snapshots carry the fixed bundle identity");
    match (
        snapshot.runtime_port,
        snapshot.runtime_owner,
        snapshot.runtime_model_id,
    ) {
        (Some(port), Some(owner), Some(model_id)) => menu
            .with_observed_runtime(
                match owner {
                    CoreRuntimeOwner::Legacy => ObservedRuntimeOwner::Legacy,
                    CoreRuntimeOwner::Foreground => ObservedRuntimeOwner::Foreground,
                    CoreRuntimeOwner::PersistentApp => ObservedRuntimeOwner::PersistentApp,
                },
                model_id,
                port,
            )
            .expect("core running snapshots carry validated runtime identity"),
        (None, None, None) => menu,
        _ => panic!("core runtime identity must be complete"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservationMessage {
    Snapshot(MenuSnapshot),
    Error(String),
}

pub(crate) struct RefreshAdmission {
    in_flight: bool,
    pending_popover: bool,
    accepting: bool,
}

impl RefreshAdmission {
    fn new() -> Self {
        Self {
            in_flight: false,
            pending_popover: false,
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

    fn admit_popover(&mut self, _now: Instant) -> bool {
        if !self.accepting {
            return false;
        }
        if self.in_flight {
            self.pending_popover = true;
            return false;
        }
        self.in_flight = true;
        true
    }

    fn complete(&mut self, _now: Instant) -> bool {
        self.in_flight = false;
        if self.accepting && std::mem::take(&mut self.pending_popover) {
            self.in_flight = true;
            return true;
        }
        false
    }

    fn close(&mut self) {
        self.accepting = false;
        self.pending_popover = false;
    }

    fn is_shutting_down(&self) -> bool {
        !self.accepting
    }
}

enum BackendRequest {
    Observe {
        completion: Sender<()>,
    },
    Search {
        generation: u64,
        query: String,
    },
    Inspect {
        generation: u64,
        repo: String,
    },
    Transfer {
        generation: u64,
        repo: String,
        revision: String,
        path: String,
        artifact: Option<ResolvedFile>,
        intent: CandidateTransferIntent,
        control: TransferControl,
    },
    PrepareDiscard {
        model_id: String,
    },
    KeepDiscard {
        model_id: String,
    },
    ConfirmDiscard {
        model_id: String,
    },
    Stop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendMessage {
    Observation(ObservationMessage),
    Installed {
        result: Result<Vec<InstalledItem>, InstalledInventoryError>,
        pinned_model_id: Option<String>,
    },
    Incomplete(Result<Vec<IncompleteItem>, IncompleteInventoryError>),
    DiscardPrepared {
        model_id: String,
        result: Result<(), DiscardFailure>,
    },
    DiscardCompleted {
        model_id: String,
        result: Result<(), DiscardFailure>,
    },
    Catalog(CatalogEvent),
}

struct InspectedRepository {
    repo: String,
    revision: String,
    candidates: Vec<CandidateItem>,
}

impl InspectedRepository {
    fn new(repo: String, revision: String, candidates: Vec<CandidateItem>) -> Self {
        Self {
            repo,
            revision,
            candidates,
        }
    }
}

struct TransferCompletion {
    disposition: CatalogTransferDisposition,
    model_id: String,
}

struct BackendTransfer {
    repo: String,
    revision: String,
    path: String,
    artifact: Option<ResolvedFile>,
    intent: CandidateTransferIntent,
    control: TransferControl,
}

fn discard_service_failure() -> DiscardFailure {
    DiscardFailure::Unavailable
}

impl TransferCompletion {
    fn new(disposition: CatalogTransferDisposition, model_id: String) -> Self {
        Self {
            disposition,
            model_id,
        }
    }
}

trait BackendSource {
    fn snapshot(&mut self) -> MenuSnapshot;
    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError>;
    fn incomplete_transfers(&mut self) -> Result<Vec<IncompleteItem>, IncompleteInventoryError> {
        Ok(Vec::new())
    }
    fn search(&mut self, query: String) -> Result<Vec<RepositoryItem>, String>;
    fn inspect(&mut self, repo: String) -> Result<InspectedRepository, String>;
    fn transfer(
        &mut self,
        transfer: BackendTransfer,
        progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String>;
    fn prepare_discard(&mut self, _model_id: String) -> Result<(), DiscardFailure> {
        Err(DiscardFailure::Unavailable)
    }
    fn keep_discard(&mut self, _model_id: &str) {}
    fn confirm_discard(&mut self, _model_id: &str) -> Result<(), DiscardFailure> {
        Err(DiscardFailure::Unavailable)
    }
}

#[cfg(not(test))]
struct AppBackend {
    service: AppService,
    prepared_discard: Option<DiscardCandidate>,
}

#[cfg(not(test))]
impl AppBackend {
    fn new(service: AppService) -> Self {
        Self {
            service,
            prepared_discard: None,
        }
    }
}

#[cfg(not(test))]
impl BackendSource for AppBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        map_app_snapshot(self.service.snapshot())
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        self.service
            .installed_models()
            .map(|models| {
                models
                    .into_iter()
                    .map(|model| {
                        InstalledItem::new(
                            model.id().into(),
                            model.display_name().into(),
                            model.total_bytes(),
                        )
                    })
                    .collect()
            })
            .map_err(|_| InstalledInventoryError::RefreshFailed)
    }

    fn incomplete_transfers(&mut self) -> Result<Vec<IncompleteItem>, IncompleteInventoryError> {
        self.service
            .incomplete_transfers()
            .map(|inventory| {
                inventory
                    .entries()
                    .iter()
                    .map(|entry| {
                        IncompleteItem::new(
                            entry.model_id().into(),
                            entry.completed_bytes(),
                            entry.total_bytes(),
                        )
                    })
                    .collect()
            })
            .map_err(|_| IncompleteInventoryError::RefreshFailed)
    }

    fn search(&mut self, query: String) -> Result<Vec<RepositoryItem>, String> {
        let page = self
            .service
            .search_models(SearchModels::new(query))
            .map_err(|error| error.to_string())?;
        Ok(page
            .hits()
            .iter()
            .map(|hit| {
                let access = match hit.gated() {
                    loxa::discovery::GatedStatus::Public => None,
                    loxa::discovery::GatedStatus::AutomaticApproval => {
                        Some("Gated · automatic approval")
                    }
                    loxa::discovery::GatedStatus::ManualApproval => Some("Gated · manual approval"),
                    loxa::discovery::GatedStatus::Unknown => Some("Access unknown"),
                };
                let downloads = hit
                    .downloads()
                    .map(|downloads| format!("{downloads} downloads"));
                let detail = match (downloads, access) {
                    (Some(downloads), Some(access)) => Some(format!("{downloads} · {access}")),
                    (Some(downloads), None) => Some(downloads),
                    (None, Some(access)) => Some(access.into()),
                    (None, None) => None,
                };
                RepositoryItem::new(hit.repo().into(), detail)
            })
            .collect())
    }

    fn inspect(&mut self, repo: String) -> Result<InspectedRepository, String> {
        let plan = self
            .service
            .inspect_repository(InspectRepository::new(repo, None))
            .map_err(|error| error.to_string())?;
        let installed = self
            .service
            .installed_models()
            .map_err(|_| InstalledInventoryError::RefreshFailed.message().to_owned())?;
        let candidates = plan
            .candidates()
            .iter()
            .filter_map(|candidate| {
                if !matches!(
                    candidate.disposition(),
                    CandidateDisposition::EligibleForDownloadAndLocalValidation
                ) {
                    return None;
                }
                let identity = candidate.identity()?.clone();
                let installed_model_id = installed
                    .iter()
                    .find(|summary| summary.matches_remote(&identity))
                    .map(|summary| summary.id().to_owned());
                let item = CandidateItem::from_resolved(
                    candidate.display_path().into(),
                    candidate.size(),
                    identity,
                );
                Some(match installed_model_id {
                    Some(model_id) => item.with_installed_model_id(model_id),
                    None => item,
                })
            })
            .collect();
        Ok(InspectedRepository::new(
            plan.repo().into(),
            plan.commit().into(),
            candidates,
        ))
    }

    fn transfer(
        &mut self,
        transfer: BackendTransfer,
        progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        let BackendTransfer {
            repo,
            revision,
            path,
            artifact,
            intent,
            control,
        } = transfer;
        let _ = (repo, revision, path);
        let artifact = artifact.ok_or_else(|| "Selected artifact is unavailable".to_owned())?;
        let request = match intent {
            CandidateTransferIntent::New => TransferSelected::new(artifact, None),
            CandidateTransferIntent::InspectedInstalled(model_id) => {
                TransferSelected::for_installed(artifact, model_id)
            }
        };
        let result = self
            .service
            .transfer_selected(request, control, |update: TransferProgress| {
                let stage = match update.phase() {
                    TransferPhase::Transferring => TransferStage::Transferring,
                    TransferPhase::Verifying => TransferStage::Verifying,
                    TransferPhase::Publishing => TransferStage::Publishing,
                };
                progress(stage, update.transferred_bytes(), update.total_bytes());
            })
            .map_err(|error| error.to_string())?;
        let disposition = match result.disposition() {
            TransferDisposition::Installed => CatalogTransferDisposition::Installed,
            TransferDisposition::AlreadyInstalled => CatalogTransferDisposition::AlreadyInstalled,
            TransferDisposition::Paused => CatalogTransferDisposition::Paused,
            TransferDisposition::Interrupted => CatalogTransferDisposition::Interrupted,
        };
        Ok(TransferCompletion::new(
            disposition,
            result.model_id().into(),
        ))
    }

    fn prepare_discard(&mut self, model_id: String) -> Result<(), DiscardFailure> {
        self.prepared_discard = None;
        self.prepared_discard = Some(
            self.service
                .prepare_discard(model_id)
                .map_err(|_| DiscardFailure::Unavailable)?,
        );
        Ok(())
    }

    fn keep_discard(&mut self, model_id: &str) {
        if self
            .prepared_discard
            .as_ref()
            .is_some_and(|candidate| candidate.model_id() == model_id)
        {
            self.prepared_discard = None;
        }
    }

    fn confirm_discard(&mut self, model_id: &str) -> Result<(), DiscardFailure> {
        let Some(candidate) = self.prepared_discard.take() else {
            return Err(DiscardFailure::Unavailable);
        };
        if candidate.model_id() != model_id {
            self.prepared_discard = Some(candidate);
            return Err(DiscardFailure::Unavailable);
        }
        self.service
            .discard_transfer(candidate)
            .map_err(|_| discard_service_failure())
    }
}

fn installed_message<B: BackendSource>(
    source: &mut Result<B, String>,
    pinned_model_id: Option<String>,
) -> BackendMessage {
    let result = match source {
        Ok(source) => source.installed_models(),
        Err(_) => Err(InstalledInventoryError::RefreshFailed),
    };
    let pinned_model_id = if result.is_ok() {
        pinned_model_id
    } else {
        None
    };
    BackendMessage::Installed {
        pinned_model_id,
        result,
    }
}

fn incomplete_message<B: BackendSource>(source: &mut Result<B, String>) -> BackendMessage {
    BackendMessage::Incomplete(match source {
        Ok(source) => source.incomplete_transfers(),
        Err(_) => Err(IncompleteInventoryError::RefreshFailed),
    })
}

fn observation_message<B: BackendSource>(source: &mut Result<B, String>) -> BackendMessage {
    BackendMessage::Observation(match source {
        Ok(source) => ObservationMessage::Snapshot(source.snapshot()),
        Err(error) => ObservationMessage::Error(error.clone()),
    })
}

fn run_backend_worker<B: BackendSource>(
    mut source: Result<B, String>,
    request_receiver: Receiver<BackendRequest>,
    message_sender: Sender<BackendMessage>,
    stopping: &AtomicBool,
) {
    while let Ok(request) = request_receiver.recv() {
        if matches!(request, BackendRequest::Stop) || stopping.load(Ordering::Acquire) {
            break;
        }
        let mut observation_completion = None;
        let messages = match request {
            BackendRequest::Observe { completion } => {
                observation_completion = Some(completion);
                vec![
                    observation_message(&mut source),
                    installed_message(&mut source, None),
                    incomplete_message(&mut source),
                ]
            }
            BackendRequest::Search { generation, query } => {
                vec![BackendMessage::Catalog(match &mut source {
                    Ok(source) => match source.search(query) {
                        Ok(repositories) => CatalogEvent::Repositories {
                            generation,
                            repositories,
                        },
                        Err(message) => CatalogEvent::Failed {
                            generation,
                            message,
                        },
                    },
                    Err(message) => CatalogEvent::Failed {
                        generation,
                        message: message.clone(),
                    },
                })]
            }
            BackendRequest::Inspect { generation, repo } => {
                vec![BackendMessage::Catalog(match &mut source {
                    Ok(source) => match source.inspect(repo) {
                        Ok(inspection) => CatalogEvent::Candidates {
                            generation,
                            repo: inspection.repo,
                            revision: inspection.revision,
                            candidates: inspection.candidates,
                        },
                        Err(message) => CatalogEvent::Failed {
                            generation,
                            message,
                        },
                    },
                    Err(message) => CatalogEvent::Failed {
                        generation,
                        message: message.clone(),
                    },
                })]
            }
            BackendRequest::Transfer {
                generation,
                repo,
                revision,
                path,
                artifact,
                intent,
                control,
            } => {
                let (terminal, pinned_model_id) = match &mut source {
                    Ok(source) => {
                        let mut progress = |stage, transferred_bytes, total_bytes| {
                            let _ = message_sender.send(BackendMessage::Catalog(
                                CatalogEvent::Progress {
                                    generation,
                                    stage,
                                    transferred_bytes,
                                    total_bytes,
                                },
                            ));
                        };
                        match source.transfer(
                            BackendTransfer {
                                repo,
                                revision,
                                path,
                                artifact,
                                intent,
                                control,
                            },
                            &mut progress,
                        ) {
                            Ok(TransferCompletion {
                                disposition,
                                model_id,
                            }) => (
                                CatalogEvent::Completed {
                                    generation,
                                    disposition,
                                },
                                matches!(
                                    disposition,
                                    CatalogTransferDisposition::Installed
                                        | CatalogTransferDisposition::AlreadyInstalled
                                )
                                .then_some(model_id),
                            ),
                            Err(message) => (
                                CatalogEvent::Failed {
                                    generation,
                                    message,
                                },
                                None,
                            ),
                        }
                    }
                    Err(message) => (
                        CatalogEvent::Failed {
                            generation,
                            message: message.clone(),
                        },
                        None,
                    ),
                };
                let mut messages = vec![BackendMessage::Catalog(terminal)];
                if let Some(model_id) = pinned_model_id {
                    messages.push(installed_message(&mut source, Some(model_id)));
                }
                messages.push(incomplete_message(&mut source));
                messages
            }
            BackendRequest::PrepareDiscard { model_id } => {
                let result = match &mut source {
                    Ok(source) => source.prepare_discard(model_id.clone()),
                    Err(_) => Err(DiscardFailure::Unavailable),
                };
                vec![BackendMessage::DiscardPrepared { model_id, result }]
            }
            BackendRequest::KeepDiscard { model_id } => {
                if let Ok(source) = &mut source {
                    source.keep_discard(&model_id);
                }
                Vec::new()
            }
            BackendRequest::ConfirmDiscard { model_id } => {
                let result = match &mut source {
                    Ok(source) => source.confirm_discard(&model_id),
                    Err(_) => Err(DiscardFailure::Unavailable),
                };
                if message_sender
                    .send(BackendMessage::DiscardCompleted { model_id, result })
                    .is_err()
                {
                    return;
                }
                vec![
                    observation_message(&mut source),
                    installed_message(&mut source, None),
                    incomplete_message(&mut source),
                ]
            }
            BackendRequest::Stop => Vec::new(),
        };
        for message in messages {
            if message_sender.send(message).is_err() {
                return;
            }
        }
        if let Some(completion) = observation_completion {
            let _ = completion.send(());
        }
    }
}

pub(crate) struct BackendClient {
    request_sender: Option<Sender<BackendRequest>>,
    receiver: Option<Receiver<BackendMessage>>,
    admission: RefreshAdmission,
    observation_completion: Option<Receiver<()>>,
    active_transfer: Option<(u64, TransferControl)>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendShutdownError;

impl BackendClient {
    #[cfg(not(test))]
    pub(crate) fn start() -> Self {
        Self::assemble(|request_receiver, message_sender, stopping| {
            std::thread::Builder::new()
                .name("loxa-menu-backend".to_owned())
                .spawn(move || {
                    run_backend_worker(
                        AppService::from_env().map(AppBackend::new),
                        request_receiver,
                        message_sender,
                        &stopping,
                    );
                })
                .map_err(|error| format!("Failed to start the menu backend: {error}"))
        })
    }

    fn assemble(
        spawn: impl FnOnce(
            Receiver<BackendRequest>,
            Sender<BackendMessage>,
            Arc<AtomicBool>,
        ) -> Result<JoinHandle<()>, String>,
    ) -> Self {
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, receiver) = mpsc::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker = match spawn(
            request_receiver,
            message_sender.clone(),
            Arc::clone(&stopping),
        ) {
            Ok(worker) => Some(worker),
            Err(error) => {
                let _ = message_sender.send(BackendMessage::Observation(
                    ObservationMessage::Error(error),
                ));
                None
            }
        };
        drop(message_sender);

        let mut client = Self {
            request_sender: Some(request_sender),
            receiver: Some(receiver),
            admission: RefreshAdmission::new(),
            observation_completion: None,
            active_transfer: None,
            stopping,
            worker,
        };
        if client.admission.admit_startup() {
            let _ = client.send_observation();
        }
        client
    }

    pub(crate) fn request_popover_open(&mut self, now: Instant) -> bool {
        if !self.admission.admit_popover(now) {
            return false;
        }
        self.send_observation()
    }

    pub(crate) fn dispatch(&mut self, command: crate::menu::catalog::CatalogCommand) -> bool {
        let request = match command {
            crate::menu::catalog::CatalogCommand::Search { generation, query } => {
                BackendRequest::Search { generation, query }
            }
            crate::menu::catalog::CatalogCommand::Inspect { generation, repo } => {
                BackendRequest::Inspect { generation, repo }
            }
            crate::menu::catalog::CatalogCommand::Transfer {
                generation,
                repo,
                revision,
                path,
                artifact,
                intent,
            } => {
                if self.active_transfer.is_some() {
                    return false;
                }
                let control = TransferControl::new();
                self.active_transfer = Some((generation, control.clone()));
                BackendRequest::Transfer {
                    generation,
                    repo,
                    revision,
                    path,
                    artifact,
                    intent,
                    control,
                }
            }
        };
        if self.send(request) {
            true
        } else {
            self.active_transfer = None;
            false
        }
    }

    pub(crate) fn request_pause(&self, generation: u64) -> bool {
        let Some((active_generation, control)) = &self.active_transfer else {
            return false;
        };
        if *active_generation != generation {
            return false;
        }
        control.request_pause();
        true
    }

    pub(crate) fn pause_active_transfer(&self) -> bool {
        let Some((_, control)) = &self.active_transfer else {
            return false;
        };
        control.request_pause();
        true
    }

    pub(crate) fn prepare_discard(&mut self, model_id: String) -> bool {
        self.send(BackendRequest::PrepareDiscard { model_id })
    }

    pub(crate) fn keep_discard(&mut self, model_id: String) -> bool {
        self.send(BackendRequest::KeepDiscard { model_id })
    }

    pub(crate) fn confirm_discard(&mut self, model_id: String) -> bool {
        self.send(BackendRequest::ConfirmDiscard { model_id })
    }

    pub(crate) fn drain(&mut self, now: Instant) -> Vec<BackendMessage> {
        let mut messages = Vec::new();
        let mut disconnected = false;
        let Some(receiver) = self.receiver.as_ref() else {
            return messages;
        };
        loop {
            match receiver.try_recv() {
                Ok(message) => {
                    if let BackendMessage::Catalog(
                        event @ (CatalogEvent::Completed { .. } | CatalogEvent::Failed { .. }),
                    ) = &message
                    {
                        if self
                            .active_transfer
                            .as_ref()
                            .is_some_and(|(generation, _)| *generation == event.generation())
                        {
                            self.active_transfer = None;
                        }
                    }
                    messages.push(message);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        let observation_completed = self
            .observation_completion
            .as_ref()
            .is_some_and(|completion| completion.try_recv().is_ok());
        if observation_completed {
            self.observation_completion.take();
            if self.admission.complete(now) && !self.send_observation() {
                self.admission.close();
            }
        }
        if disconnected {
            if !self.admission.is_shutting_down()
                && !matches!(
                    messages.last(),
                    Some(BackendMessage::Observation(ObservationMessage::Error(_)))
                )
            {
                messages.push(BackendMessage::Observation(ObservationMessage::Error(
                    "The menu backend disconnected".into(),
                )));
            }
            self.admission.close();
            self.observation_completion.take();
            self.receiver.take();
            self.request_sender.take();
        }
        messages
    }

    pub(crate) fn shutdown_and_join(&mut self) -> Result<(), BackendShutdownError> {
        self.stopping.store(true, Ordering::Release);
        self.admission.close();
        self.observation_completion.take();
        self.pause_active_transfer();
        if let Some(sender) = self.request_sender.as_ref() {
            let _ = sender.send(BackendRequest::Stop);
        }
        let Some(worker) = self.worker.take() else {
            self.active_transfer.take();
            self.request_sender.take();
            self.receiver.take();
            return Ok(());
        };
        let joined = worker.join().is_ok();
        self.active_transfer.take();
        self.request_sender.take();
        self.receiver.take();
        if joined {
            Ok(())
        } else {
            Err(BackendShutdownError)
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown(&mut self) {
        let _ = self.shutdown_and_join();
    }

    fn send(&mut self, request: BackendRequest) -> bool {
        if self.stopping.load(Ordering::Acquire) {
            return false;
        }
        let Some(sender) = self.request_sender.as_ref() else {
            return false;
        };
        sender.send(request).is_ok()
    }

    fn send_observation(&mut self) -> bool {
        let (completion, receiver) = mpsc::channel();
        if !self.send(BackendRequest::Observe { completion }) {
            return false;
        }
        self.observation_completion = Some(receiver);
        true
    }
}

#[cfg(test)]
pub(crate) fn backend_client_panicking_on_shutdown() -> BackendClient {
    BackendClient::assemble(|requests, _messages, _stopping| {
        std::thread::Builder::new()
            .name("loxa-menu-backend-panic-test".into())
            .spawn(move || loop {
                match requests.recv() {
                    Ok(BackendRequest::Stop) => panic!("injected backend shutdown panic"),
                    Ok(_) => {}
                    Err(_) => break,
                }
            })
            .map_err(|error| error.to_string())
    })
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
