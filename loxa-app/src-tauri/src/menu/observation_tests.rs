use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use loxa::app::{AppSnapshot, TransferControl};

use super::{
    map_app_snapshot, map_core_snapshot, run_backend_worker, BackendClient, BackendMessage,
    BackendRequest, BackendSource, CoreBundle, CoreDownload, CoreObservation, CoreRecommendation,
    CoreRecommendationUnavailableReason, InspectedRepository, ObservationMessage, RefreshAdmission,
};
use crate::menu::catalog::{
    CandidateItem, CatalogCommand, CatalogEvent, CatalogTransferDisposition, RepositoryItem,
    TransferStage,
};
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

struct FakeBackend;

impl BackendSource for FakeBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
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
        repo: String,
        revision: String,
        path: String,
        _control: TransferControl,
        progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<CatalogTransferDisposition, String> {
        if (repo.as_str(), revision.as_str(), path.as_str())
            != ("owner/model", expected_revision().as_str(), "model-q4.gguf")
        {
            return Err("worker changed the explicit artifact choice".into());
        }
        progress(TransferStage::Transferring, 0, 42);
        Ok(CatalogTransferDisposition::AlreadyInstalled)
    }
}

struct ThreadBackend {
    thread_sender: mpsc::Sender<std::thread::ThreadId>,
}

impl BackendSource for ThreadBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        Fixture::Empty.snapshot()
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
        _repo: String,
        _revision: String,
        _path: String,
        _control: TransferControl,
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<CatalogTransferDisposition, String> {
        Err("transfer is outside this test".into())
    }
}
