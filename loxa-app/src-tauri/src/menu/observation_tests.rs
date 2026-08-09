use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use loxa::app::{AppSnapshot, TransferControl};
use loxa::huggingface::ResolvedFile;

use super::{
    map_app_snapshot, map_core_snapshot, run_backend_worker, BackendClient, BackendMessage,
    BackendRequest, BackendSource, BackendTransfer, CoreBundle, CoreDownload, CoreObservation,
    CoreRecommendation, CoreRecommendationUnavailableReason, InspectedRepository,
    ObservationMessage, RefreshAdmission, TransferCompletion,
};
use crate::menu::catalog::{
    CandidateItem, CandidateTransferIntent, CatalogCommand, CatalogEvent,
    CatalogTransferDisposition, RepositoryItem, TransferStage,
};
use crate::menu::installed::{InstalledInventoryError, InstalledItem};
use crate::menu::presentation::{Fixture, MenuSnapshot};

#[test]
fn mapper_keeps_live_absent_and_paused_states_truthful() {
    let _mapper: fn(AppSnapshot) -> MenuSnapshot = map_app_snapshot;
    let idle = map_core_snapshot(CoreObservation {
        bundle: CoreBundle::Absent,
        recommendation: CoreRecommendation::Unavailable(
            CoreRecommendationUnavailableReason::Unavailable,
        ),
        download: CoreDownload::Idle,
        runtime: super::CoreRuntime::Idle,
        runtime_inventory: super::CoreRuntimeInventory::Missing,
    });
    assert_eq!(
        idle.recommendation_row().unwrap().disabled_reason(),
        Some("Unavailable on this Mac")
    );

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
    assert_eq!(
        paused.transfer_row().unwrap().progress_detail(),
        "3 of 5 bytes"
    );
}

#[test]
fn refresh_admission_observes_startup_then_uses_completed_freshness() {
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
    admission.close();
    assert!(admission.is_shutting_down());
}

#[test]
fn worker_routes_search_inspection_and_exact_transfer_as_owned_events() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    request_sender
        .send(BackendRequest::Search {
            generation: 7,
            query: "small model".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::Inspect {
            generation: 7,
            repo: "owner/model".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::Transfer {
            generation: 7,
            repo: "owner/model".into(),
            revision: expected_revision(),
            path: "model-q4.gguf".into(),
            artifact: None,
            intent: CandidateTransferIntent::InspectedInstalled("custom-model".into()),
            control: TransferControl::new(),
        })
        .unwrap();
    request_sender.send(BackendRequest::Stop).unwrap();

    run_backend_worker(
        Ok(FakeBackend),
        request_receiver,
        message_sender,
        &AtomicBool::new(false),
    );

    assert_eq!(
        message_receiver.into_iter().collect::<Vec<_>>(),
        [
            BackendMessage::Catalog(CatalogEvent::Repositories {
                generation: 7,
                repositories: vec![repository()],
            }),
            BackendMessage::Catalog(CatalogEvent::Candidates {
                generation: 7,
                repo: "owner/model".into(),
                revision: expected_revision(),
                candidates: vec![candidate()],
            }),
            BackendMessage::Catalog(CatalogEvent::Progress {
                generation: 7,
                stage: TransferStage::Transferring,
                transferred_bytes: 0,
                total_bytes: 42,
            }),
            BackendMessage::Catalog(CatalogEvent::Completed {
                generation: 7,
                disposition: CatalogTransferDisposition::AlreadyInstalled,
            }),
            BackendMessage::Installed {
                result: Ok(vec![installed_item("new-model", 42)]),
                pinned_model_id: Some("new-model".into()),
            },
        ]
    );
}

#[test]
fn candidate_command_and_worker_retain_the_exact_inspected_artifact_value() {
    let expected = loxa::huggingface::test_resolved_file_for(
        "owner/model",
        "model-q4.gguf",
        "ab".repeat(32),
        42,
    );
    let mut catalog = crate::menu::catalog::CatalogState::default();
    let search = catalog.submit_search("model").unwrap();
    assert!(catalog.apply(CatalogEvent::Repositories {
        generation: search.generation(),
        repositories: vec![repository()],
    }));
    let inspect = catalog.inspect_repository(0).unwrap();
    assert!(catalog.apply(CatalogEvent::Candidates {
        generation: inspect.generation(),
        repo: "owner/model".into(),
        revision: expected_revision(),
        candidates: vec![CandidateItem::from_resolved(
            "model-q4.gguf".into(),
            Some(42),
            expected.clone(),
        )
        .with_installed_model_id("custom-model".into())],
    }));
    assert!(catalog.select_candidate(0));
    let command = catalog.start_transfer().unwrap();

    let (artifact_sender, artifact_receiver) = mpsc::channel();
    let mut client = BackendClient::assemble(move |requests, messages, stopping| {
        std::thread::Builder::new()
            .name("loxa-menu-artifact-test-backend".into())
            .spawn(move || {
                run_backend_worker(
                    Ok(ArtifactCaptureBackend { artifact_sender }),
                    requests,
                    messages,
                    &stopping,
                );
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    });

    assert!(client.dispatch(command));
    let received = artifact_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the worker must receive the inspected artifact");
    assert_eq!(received, Some(expected));
    client.shutdown();
}

#[test]
fn worker_refreshes_inventory_immediately_after_installed_and_already_installed() {
    for disposition in [
        CatalogTransferDisposition::Installed,
        CatalogTransferDisposition::AlreadyInstalled,
    ] {
        let initial_inventory = if disposition == CatalogTransferDisposition::AlreadyInstalled {
            vec![
                installed_item("old-model", 41),
                installed_item("new-model", 42),
            ]
        } else {
            vec![installed_item("old-model", 41)]
        };
        let (request_sender, request_receiver) = mpsc::channel();
        let (message_sender, message_receiver) = mpsc::channel();
        request_sender.send(BackendRequest::Observe).unwrap();
        request_sender
            .send(BackendRequest::Transfer {
                generation: 11,
                repo: "owner/model".into(),
                revision: expected_revision(),
                path: "model-q4.gguf".into(),
                artifact: None,
                intent: CandidateTransferIntent::New,
                control: TransferControl::new(),
            })
            .unwrap();
        request_sender.send(BackendRequest::Stop).unwrap();

        run_backend_worker(
            Ok(RefreshBackend {
                disposition,
                inventory_reads: 0,
                fail_on_refresh: false,
            }),
            request_receiver,
            message_sender,
            &AtomicBool::new(false),
        );

        assert_eq!(
            message_receiver.into_iter().collect::<Vec<_>>(),
            [
                BackendMessage::Observation(ObservationMessage::Snapshot(
                    Fixture::Empty.snapshot(),
                )),
                BackendMessage::Installed {
                    result: Ok(initial_inventory),
                    pinned_model_id: None,
                },
                BackendMessage::Catalog(CatalogEvent::Progress {
                    generation: 11,
                    stage: TransferStage::Transferring,
                    transferred_bytes: 0,
                    total_bytes: 42,
                }),
                BackendMessage::Catalog(CatalogEvent::Completed {
                    generation: 11,
                    disposition,
                }),
                BackendMessage::Installed {
                    result: Ok(vec![
                        installed_item("old-model", 41),
                        installed_item("new-model", 42),
                    ]),
                    pinned_model_id: Some("new-model".into()),
                },
            ],
            "terminal disposition {disposition:?} must refresh without admission delay"
        );
    }
}

#[test]
fn worker_drops_the_completion_pin_when_terminal_inventory_refresh_fails() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    request_sender.send(BackendRequest::Observe).unwrap();
    request_sender
        .send(BackendRequest::Transfer {
            generation: 12,
            repo: "owner/model".into(),
            revision: expected_revision(),
            path: "model-q4.gguf".into(),
            artifact: None,
            intent: CandidateTransferIntent::New,
            control: TransferControl::new(),
        })
        .unwrap();
    request_sender.send(BackendRequest::Stop).unwrap();

    run_backend_worker(
        Ok(RefreshBackend {
            disposition: CatalogTransferDisposition::Installed,
            inventory_reads: 0,
            fail_on_refresh: true,
        }),
        request_receiver,
        message_sender,
        &AtomicBool::new(false),
    );

    assert_eq!(
        message_receiver.into_iter().collect::<Vec<_>>(),
        [
            BackendMessage::Observation(ObservationMessage::Snapshot(Fixture::Empty.snapshot())),
            BackendMessage::Installed {
                result: Ok(vec![installed_item("old-model", 41)]),
                pinned_model_id: None,
            },
            BackendMessage::Catalog(CatalogEvent::Progress {
                generation: 12,
                stage: TransferStage::Transferring,
                transferred_bytes: 0,
                total_bytes: 42,
            }),
            BackendMessage::Catalog(CatalogEvent::Completed {
                generation: 12,
                disposition: CatalogTransferDisposition::Installed,
            }),
            BackendMessage::Installed {
                result: Err(InstalledInventoryError::RefreshFailed),
                pinned_model_id: None,
            },
        ]
    );
}

#[test]
fn worker_maps_inventory_failures_to_the_closed_sanitized_error() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    request_sender.send(BackendRequest::Observe).unwrap();
    request_sender.send(BackendRequest::Stop).unwrap();

    run_backend_worker(
        Ok(FailingInventoryBackend),
        request_receiver,
        message_sender,
        &AtomicBool::new(false),
    );

    assert_eq!(
        message_receiver.into_iter().collect::<Vec<_>>(),
        [
            BackendMessage::Observation(ObservationMessage::Snapshot(Fixture::Empty.snapshot())),
            BackendMessage::Installed {
                result: Err(InstalledInventoryError::RefreshFailed),
                pinned_model_id: None,
            },
        ]
    );
}

#[test]
fn backend_client_dispatches_catalog_work_on_its_spawned_thread() {
    let caller = std::thread::current().id();
    let (thread_sender, thread_receiver) = mpsc::channel();
    let mut client = BackendClient::assemble(move |requests, messages, stopping| {
        std::thread::Builder::new()
            .name("loxa-menu-test-backend".into())
            .spawn(move || {
                run_backend_worker(
                    Ok(ThreadBackend { thread_sender }),
                    requests,
                    messages,
                    &stopping,
                );
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    });

    assert!(client.dispatch(CatalogCommand::Search {
        generation: 3,
        query: "small model".into(),
    }));
    let worker = thread_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the request must execute on the spawned backend thread");
    assert_ne!(worker, caller);
    client.shutdown();
}

#[test]
fn backend_client_pauses_only_the_exact_active_generation() {
    let captured_requests = Rc::new(RefCell::new(None));
    let spawn_requests = captured_requests.clone();
    let mut client = BackendClient::assemble(move |requests, _messages, _stopping| {
        *spawn_requests.borrow_mut() = Some(requests);
        Ok(())
    });
    assert!(client.dispatch(CatalogCommand::Transfer {
        generation: 9,
        repo: "owner/model".into(),
        revision: expected_revision(),
        path: "model-q4.gguf".into(),
        artifact: None,
        intent: CandidateTransferIntent::New,
    }));
    assert!(!client.request_pause(8));
    assert!(client.request_pause(9));
    client.shutdown();
}

#[test]
fn backend_client_drains_owned_catalog_events() {
    let mut client = BackendClient::assemble(|_requests, messages, _stopping| {
        messages
            .send(BackendMessage::Catalog(CatalogEvent::Repositories {
                generation: 4,
                repositories: vec![repository()],
            }))
            .map_err(|error| error.to_string())
    });

    let messages = client.drain(Instant::now());
    assert_eq!(
        messages.first(),
        Some(&BackendMessage::Catalog(CatalogEvent::Repositories {
            generation: 4,
            repositories: vec![repository()],
        }))
    );
    client.shutdown();
}

fn expected_revision() -> String {
    "0123456789abcdef0123456789abcdef01234567".into()
}

fn repository() -> RepositoryItem {
    RepositoryItem::new("owner/model".into(), Some("42 downloads".into()))
}

fn candidate() -> CandidateItem {
    CandidateItem::new("model-q4.gguf".into(), Some(42))
}

fn installed_item(id: &str, total_bytes: u64) -> InstalledItem {
    InstalledItem::new(id.into(), format!("{id}.gguf"), total_bytes)
}

struct FakeBackend;

impl BackendSource for FakeBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        Ok(vec![installed_item("new-model", 42)])
    }

    fn search(&mut self, query: String) -> Result<Vec<RepositoryItem>, String> {
        if query != "small model" {
            return Err("worker changed the search query".into());
        }
        Ok(vec![repository()])
    }

    fn inspect(&mut self, repo: String) -> Result<InspectedRepository, String> {
        if repo != "owner/model" {
            return Err("worker changed the repository".into());
        }
        Ok(InspectedRepository::new(
            repo,
            expected_revision(),
            vec![candidate()],
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
        let _ = (artifact, control);
        if (repo.as_str(), revision.as_str(), path.as_str())
            != ("owner/model", expected_revision().as_str(), "model-q4.gguf")
        {
            return Err("worker changed the explicit artifact choice".into());
        }
        if intent != CandidateTransferIntent::InspectedInstalled("custom-model".into()) {
            return Err("worker dropped the inspected-installed transfer intent".into());
        }
        progress(TransferStage::Transferring, 0, 42);
        Ok(TransferCompletion::new(
            CatalogTransferDisposition::AlreadyInstalled,
            "new-model".into(),
        ))
    }
}

struct ArtifactCaptureBackend {
    artifact_sender: mpsc::Sender<Option<ResolvedFile>>,
}

impl BackendSource for ArtifactCaptureBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        Ok(Vec::new())
    }

    fn search(&mut self, _query: String) -> Result<Vec<RepositoryItem>, String> {
        Err("search is outside this test".into())
    }

    fn inspect(&mut self, _repo: String) -> Result<InspectedRepository, String> {
        Err("inspection is outside this test".into())
    }

    fn transfer(
        &mut self,
        transfer: BackendTransfer,
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        self.artifact_sender
            .send(transfer.artifact)
            .map_err(|error| error.to_string())?;
        Ok(TransferCompletion::new(
            CatalogTransferDisposition::AlreadyInstalled,
            "custom-model".into(),
        ))
    }
}

struct RefreshBackend {
    disposition: CatalogTransferDisposition,
    inventory_reads: usize,
    fail_on_refresh: bool,
}

impl BackendSource for RefreshBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        self.inventory_reads += 1;
        match self.inventory_reads {
            2 if self.fail_on_refresh => Err(InstalledInventoryError::RefreshFailed),
            1 if self.disposition == CatalogTransferDisposition::AlreadyInstalled => Ok(vec![
                installed_item("old-model", 41),
                installed_item("new-model", 42),
            ]),
            1 => Ok(vec![installed_item("old-model", 41)]),
            2 => Ok(vec![
                installed_item("old-model", 41),
                installed_item("new-model", 42),
            ]),
            _ => Err(InstalledInventoryError::RefreshFailed),
        }
    }

    fn search(&mut self, _query: String) -> Result<Vec<RepositoryItem>, String> {
        Err("search is outside this test".into())
    }

    fn inspect(&mut self, _repo: String) -> Result<InspectedRepository, String> {
        Err("inspection is outside this test".into())
    }

    fn transfer(
        &mut self,
        _transfer: BackendTransfer,
        progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        progress(TransferStage::Transferring, 0, 42);
        Ok(TransferCompletion::new(
            self.disposition,
            "new-model".into(),
        ))
    }
}

struct FailingInventoryBackend;

impl BackendSource for FailingInventoryBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        Err(InstalledInventoryError::RefreshFailed)
    }

    fn search(&mut self, _query: String) -> Result<Vec<RepositoryItem>, String> {
        Err("search is outside this test".into())
    }

    fn inspect(&mut self, _repo: String) -> Result<InspectedRepository, String> {
        Err("inspection is outside this test".into())
    }

    fn transfer(
        &mut self,
        _transfer: BackendTransfer,
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        Err("transfer is outside this test".into())
    }
}

struct ThreadBackend {
    thread_sender: mpsc::Sender<std::thread::ThreadId>,
}

impl BackendSource for ThreadBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        Ok(Vec::new())
    }

    fn search(&mut self, _query: String) -> Result<Vec<RepositoryItem>, String> {
        self.thread_sender
            .send(std::thread::current().id())
            .map_err(|error| error.to_string())?;
        Ok(Vec::new())
    }

    fn inspect(&mut self, _repo: String) -> Result<InspectedRepository, String> {
        Err("inspection is outside this test".into())
    }

    fn transfer(
        &mut self,
        _transfer: BackendTransfer,
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        Err("transfer is outside this test".into())
    }
}
