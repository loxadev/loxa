use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use loxa::app::{AppSnapshot, TransferControl};
use loxa::huggingface::ResolvedFile;

use super::{
    discard_service_failure, map_app_snapshot, map_core_snapshot, run_backend_worker,
    BackendClient, BackendMessage, BackendRequest, BackendSource, BackendTransfer, CoreBundle,
    CoreDownload, CoreObservation, CoreRecommendation, CoreRecommendationUnavailableReason,
    InspectedRepository, ObservationMessage, RefreshAdmission, TransferCompletion,
};
use crate::menu::catalog::{
    CandidateItem, CandidateTransferIntent, CatalogCommand, CatalogEvent,
    CatalogTransferDisposition, RepositoryItem, TransferStage,
};
use crate::menu::incomplete::{DiscardFailure, IncompleteInventoryError, IncompleteItem};
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
        runtime_port: None,
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
        runtime_port: Some(43123),
        runtime_inventory: super::CoreRuntimeInventory::External,
    });
    assert_eq!(
        paused.transfer_row().unwrap().progress_detail(),
        "3 of 5 bytes"
    );
    assert_eq!(
        paused.runtime_api_label().as_deref(),
        Some("API · 127.0.0.1:43123")
    );
}

#[test]
fn refresh_admission_requests_each_completed_reopen_and_coalesces_in_flight() {
    let started = Instant::now();
    let mut admission = RefreshAdmission::new();

    assert!(admission.admit_startup());
    assert!(!admission.admit_startup());
    assert!(!admission.admit_popover(started + Duration::from_millis(1)));

    let startup_completed = started + Duration::from_millis(2);
    assert!(admission.complete(startup_completed));
    assert!(!admission.admit_popover(startup_completed + Duration::from_millis(2)));
    assert!(!admission.admit_popover(startup_completed + Duration::from_millis(3)));

    let reopen_completed = startup_completed + Duration::from_millis(4);
    assert!(admission.complete(reopen_completed));

    assert!(!admission.complete(reopen_completed + Duration::from_millis(1)));
    assert!(admission.admit_popover(reopen_completed + Duration::from_millis(2)));
    assert!(!admission.complete(reopen_completed + Duration::from_millis(3)));
    admission.close();
    assert!(admission.is_shutting_down());
    assert!(!admission.admit_startup());
    assert!(!admission.admit_popover(reopen_completed + Duration::from_millis(4)));
}

#[test]
fn popover_open_during_startup_queues_a_fresh_observation_after_startup_finishes() {
    let (startup_started_sender, startup_started_receiver) = mpsc::channel();
    let (release_startup_sender, release_startup_receiver) = mpsc::channel();
    let (worker_handle_sender, worker_handle_receiver) = mpsc::channel();
    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let worker_snapshot_count = Arc::clone(&snapshot_count);
    let mut client = BackendClient::assemble(move |requests, messages, stopping| {
        let worker = std::thread::Builder::new()
            .name("loxa-menu-startup-refresh-test".into())
            .spawn(move || {
                run_backend_worker(
                    Ok(StartupRefreshBackend {
                        snapshot_count: worker_snapshot_count,
                        startup_started: startup_started_sender,
                        release_startup: Some(release_startup_receiver),
                    }),
                    requests,
                    messages,
                    &stopping,
                );
            })
            .map_err(|error| error.to_string())?;
        worker_handle_sender
            .send(worker)
            .map_err(|error| error.to_string())
    });
    let worker = worker_handle_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the startup refresh worker must start");

    assert_eq!(
        startup_started_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(())
    );
    assert!(
        !client.request_popover_open(Instant::now()),
        "the open must coalesce while startup observation is in flight"
    );
    release_startup_sender.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while snapshot_count.load(Ordering::Acquire) < 2 || client.admission.in_flight {
        let _ = client.drain(Instant::now());
        assert!(
            Instant::now() < deadline,
            "the coalesced popover open must trigger one fresh observation"
        );
        std::thread::yield_now();
    }

    client.shutdown();
    worker.join().unwrap();
    assert_eq!(snapshot_count.load(Ordering::Acquire), 2);
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
            BackendMessage::Incomplete(Ok(Vec::new())),
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
        let (completion, _completion_receiver) = mpsc::channel();
        request_sender
            .send(BackendRequest::Observe { completion })
            .unwrap();
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
                BackendMessage::Incomplete(Ok(Vec::new())),
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
                BackendMessage::Incomplete(Ok(Vec::new())),
            ],
            "terminal disposition {disposition:?} must refresh without admission delay"
        );
    }
}

#[test]
fn worker_drops_the_completion_pin_when_terminal_inventory_refresh_fails() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    let (completion, _completion_receiver) = mpsc::channel();
    request_sender
        .send(BackendRequest::Observe { completion })
        .unwrap();
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
            BackendMessage::Incomplete(Ok(Vec::new())),
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
            BackendMessage::Incomplete(Ok(Vec::new())),
        ]
    );
}

#[test]
fn worker_maps_inventory_failures_to_the_closed_sanitized_error() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    let (completion, _completion_receiver) = mpsc::channel();
    request_sender
        .send(BackendRequest::Observe { completion })
        .unwrap();
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
            BackendMessage::Incomplete(Ok(Vec::new())),
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

#[test]
fn worker_prepares_before_confirmation_keeps_without_consuming_and_discards_the_exact_candidate() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    let operations = Arc::new(Mutex::new(Vec::new()));
    request_sender
        .send(BackendRequest::PrepareDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::KeepDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::PrepareDiscard {
            model_id: "beta".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::ConfirmDiscard {
            model_id: "beta".into(),
        })
        .unwrap();
    request_sender.send(BackendRequest::Stop).unwrap();

    run_backend_worker(
        Ok(DiscardBackend {
            prepared: None,
            operations: operations.clone(),
            fail_inventory: false,
            refresh_barrier: None,
            snapshot_count: 0,
        }),
        request_receiver,
        message_sender,
        &AtomicBool::new(false),
    );

    assert_eq!(
        *operations.lock().unwrap(),
        [
            "prepare alpha",
            "keep alpha",
            "prepare beta",
            "discard beta"
        ]
    );
    assert_eq!(
        message_receiver.into_iter().collect::<Vec<_>>(),
        [
            BackendMessage::DiscardPrepared {
                model_id: "alpha".into(),
                result: Ok(()),
            },
            BackendMessage::DiscardPrepared {
                model_id: "beta".into(),
                result: Ok(()),
            },
            BackendMessage::DiscardCompleted {
                model_id: "beta".into(),
                result: Ok(()),
            },
            BackendMessage::Observation(ObservationMessage::Snapshot(Fixture::Empty.snapshot())),
            BackendMessage::Installed {
                result: Ok(Vec::new()),
                pinned_model_id: None,
            },
            BackendMessage::Incomplete(Ok(Vec::new())),
        ]
    );
}

#[test]
fn service_discard_failure_uses_neutral_retry_copy_instead_of_claiming_a_change() {
    let failure = discard_service_failure();

    assert_eq!(failure, DiscardFailure::Unavailable);
    let mut state = crate::menu::incomplete::IncompleteState::default();
    state.replace(vec![IncompleteItem::new("alpha".into(), 2, 10)]);
    assert_eq!(state.prepare_discard(0).as_deref(), Some("alpha"));
    assert!(state.prepared("alpha", Ok(())));
    assert_eq!(state.confirm_discard().as_deref(), Some("alpha"));
    assert!(state.completed("alpha", Err(failure)));
    assert_eq!(
        state.feedback_message(),
        Some("Could not finish discarding. Refresh and try again.")
    );
}

struct DiscardBackend {
    prepared: Option<String>,
    operations: Arc<Mutex<Vec<String>>>,
    fail_inventory: bool,
    refresh_barrier: Option<RefreshBarrier>,
    snapshot_count: usize,
}

struct RefreshBarrier {
    after_snapshot: usize,
    started: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl BackendSource for DiscardBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        self.snapshot_count += 1;
        let should_block = self
            .refresh_barrier
            .as_ref()
            .is_some_and(|barrier| barrier.after_snapshot == self.snapshot_count);
        if should_block {
            let barrier = self
                .refresh_barrier
                .take()
                .expect("the selected snapshot has one refresh barrier");
            let _ = barrier.started.send(());
            let _ = barrier.release.recv_timeout(Duration::from_secs(2));
        }
        Fixture::Empty.snapshot()
    }

    fn installed_models(&mut self) -> Result<Vec<InstalledItem>, InstalledInventoryError> {
        Ok(Vec::new())
    }

    fn incomplete_transfers(&mut self) -> Result<Vec<IncompleteItem>, IncompleteInventoryError> {
        if self.fail_inventory {
            Err(IncompleteInventoryError::RefreshFailed)
        } else {
            Ok(Vec::new())
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
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        Err("transfer is outside this test".into())
    }

    fn prepare_discard(&mut self, model_id: String) -> Result<(), DiscardFailure> {
        self.operations
            .lock()
            .unwrap()
            .push(format!("prepare {model_id}"));
        self.prepared = Some(model_id);
        Ok(())
    }

    fn keep_discard(&mut self, model_id: &str) {
        self.operations
            .lock()
            .unwrap()
            .push(format!("keep {model_id}"));
        if self.prepared.as_deref() == Some(model_id) {
            self.prepared = None;
        }
    }

    fn confirm_discard(&mut self, model_id: &str) -> Result<(), DiscardFailure> {
        if self.prepared.as_deref() != Some(model_id) {
            return Err(DiscardFailure::Unavailable);
        }
        self.operations
            .lock()
            .unwrap()
            .push(format!("discard {model_id}"));
        self.prepared = None;
        Ok(())
    }
}

#[test]
fn successful_discard_is_delivered_before_a_failed_inventory_refresh() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    request_sender
        .send(BackendRequest::PrepareDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();
    request_sender
        .send(BackendRequest::ConfirmDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();
    request_sender.send(BackendRequest::Stop).unwrap();

    run_backend_worker(
        Ok(DiscardBackend {
            prepared: None,
            operations: Arc::new(Mutex::new(Vec::new())),
            fail_inventory: true,
            refresh_barrier: None,
            snapshot_count: 0,
        }),
        request_receiver,
        message_sender,
        &AtomicBool::new(false),
    );

    let messages = message_receiver.into_iter().collect::<Vec<_>>();
    assert_eq!(
        messages,
        [
            BackendMessage::DiscardPrepared {
                model_id: "alpha".into(),
                result: Ok(()),
            },
            BackendMessage::DiscardCompleted {
                model_id: "alpha".into(),
                result: Ok(()),
            },
            BackendMessage::Observation(ObservationMessage::Snapshot(Fixture::Empty.snapshot())),
            BackendMessage::Installed {
                result: Ok(Vec::new()),
                pinned_model_id: None,
            },
            BackendMessage::Incomplete(Err(IncompleteInventoryError::RefreshFailed)),
        ]
    );
}

#[test]
fn discard_completion_crosses_the_channel_before_refresh_starts() {
    let (request_sender, request_receiver) = mpsc::channel();
    let (message_sender, message_receiver) = mpsc::channel();
    let (refresh_started_sender, refresh_started_receiver) = mpsc::channel();
    let (release_refresh_sender, release_refresh_receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        run_backend_worker(
            Ok(DiscardBackend {
                prepared: None,
                operations: Arc::new(Mutex::new(Vec::new())),
                fail_inventory: false,
                refresh_barrier: Some(RefreshBarrier {
                    after_snapshot: 1,
                    started: refresh_started_sender,
                    release: release_refresh_receiver,
                }),
                snapshot_count: 0,
            }),
            request_receiver,
            message_sender,
            &AtomicBool::new(false),
        );
    });

    request_sender
        .send(BackendRequest::PrepareDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();
    assert_eq!(
        message_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(BackendMessage::DiscardPrepared {
            model_id: "alpha".into(),
            result: Ok(()),
        })
    );
    request_sender
        .send(BackendRequest::ConfirmDiscard {
            model_id: "alpha".into(),
        })
        .unwrap();

    let refresh_started = refresh_started_receiver.recv_timeout(Duration::from_secs(2));
    let completion = message_receiver.recv_timeout(Duration::from_millis(250));
    let _ = release_refresh_sender.send(());
    request_sender.send(BackendRequest::Stop).unwrap();
    worker.join().unwrap();

    assert_eq!(refresh_started, Ok(()));
    assert_eq!(
        completion,
        Ok(BackendMessage::DiscardCompleted {
            model_id: "alpha".into(),
            result: Ok(()),
        })
    );
}

#[test]
fn discard_observation_interleaving_keeps_popover_observation_in_flight() {
    let (reopen_started_sender, reopen_started_receiver) = mpsc::channel();
    let (release_reopen_sender, release_reopen_receiver) = mpsc::channel();
    let (worker_handle_sender, worker_handle_receiver) = mpsc::channel();
    let mut client = BackendClient::assemble(move |requests, messages, stopping| {
        let worker = std::thread::Builder::new()
            .name("loxa-menu-discard-interleaving-test".into())
            .spawn(move || {
                run_backend_worker(
                    Ok(DiscardBackend {
                        prepared: None,
                        operations: Arc::new(Mutex::new(Vec::new())),
                        fail_inventory: false,
                        refresh_barrier: Some(RefreshBarrier {
                            after_snapshot: 3,
                            started: reopen_started_sender,
                            release: release_reopen_receiver,
                        }),
                        snapshot_count: 0,
                    }),
                    requests,
                    messages,
                    &stopping,
                );
            })
            .map_err(|error| error.to_string())?;
        worker_handle_sender
            .send(worker)
            .map_err(|error| error.to_string())
    });
    let worker = worker_handle_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the interleaving worker must start");

    let startup_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let _ = client.drain(Instant::now());
        if !client.admission.in_flight {
            break;
        }
        assert!(
            Instant::now() < startup_deadline,
            "the startup observation must complete"
        );
        std::thread::yield_now();
    }

    assert!(client.prepare_discard("alpha".into()));
    assert!(client.confirm_discard("alpha".into()));
    assert!(client.request_popover_open(Instant::now()));
    assert_eq!(
        reopen_started_receiver.recv_timeout(Duration::from_secs(2)),
        Ok(())
    );

    let interleaved_messages = client.drain(Instant::now());
    assert!(interleaved_messages.iter().any(|message| matches!(
        message,
        BackendMessage::DiscardCompleted {
            model_id,
            result: Ok(())
        } if model_id == "alpha"
    )));
    assert!(interleaved_messages
        .iter()
        .any(|message| matches!(message, BackendMessage::Observation(_))));
    let second_reopen_was_coalesced = !client.request_popover_open(Instant::now());

    client.shutdown();
    let _ = release_reopen_sender.send(());
    worker.join().unwrap();

    assert!(
        second_reopen_was_coalesced,
        "a discard-generated observation must not complete the admitted popover observation"
    );
}

struct FakeBackend;

struct StartupRefreshBackend {
    snapshot_count: Arc<AtomicUsize>,
    startup_started: mpsc::Sender<()>,
    release_startup: Option<mpsc::Receiver<()>>,
}

impl BackendSource for StartupRefreshBackend {
    fn snapshot(&mut self) -> MenuSnapshot {
        let observation = self.snapshot_count.fetch_add(1, Ordering::AcqRel) + 1;
        if observation == 1 {
            let _ = self.startup_started.send(());
            let release = self
                .release_startup
                .take()
                .expect("only the startup observation is blocked");
            let _ = release.recv_timeout(Duration::from_secs(2));
        }
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
        _transfer: BackendTransfer,
        _progress: &mut dyn FnMut(TransferStage, u64, u64),
    ) -> Result<TransferCompletion, String> {
        Err("transfer is outside this test".into())
    }
}

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
