use super::*;
use crate::service::coordinator::state::EngineDescriptor;
use loxa_ipc::{DraftCommand, DraftReply, DraftSnapshot, GenerationCommand, GenerationDraft};
use std::os::unix::fs::PermissionsExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

#[derive(Clone, Copy)]
enum Stall {
    Headers,
    Body,
}

#[tokio::test(flavor = "current_thread")]
async fn preflight_deadline_releases_capacity_without_consuming_the_draft() {
    for stall in [Stall::Headers, Stall::Body] {
        exercise_stalled_preflight(stall, false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn targeted_stop_interrupts_preflight_before_its_deadline() {
    for stall in [Stall::Headers, Stall::Body] {
        exercise_stalled_preflight(stall, true).await;
    }
}

async fn exercise_stalled_preflight(stall: Stall, stop: bool) {
    let directory = tempfile::Builder::new()
        .prefix("lp-")
        .tempdir_in("/tmp")
        .unwrap();
    let endpoint = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&endpoint).unwrap();
    fs::set_permissions(&endpoint, fs::Permissions::from_mode(0o600)).unwrap();
    let fixture = Fixture::start_with_engine(EngineDescriptor {
        pid: std::process::id(),
        endpoint: Arc::new(endpoint),
    })
    .await;
    let draft = draft_snapshot(
        &fixture,
        DraftCommand::CreateScope {
            desktop_client_id: "ab".repeat(16),
            conversation_id: Some(fixture.conversation_id.clone()),
        },
    )
    .await;
    let draft = draft_snapshot(
        &fixture,
        DraftCommand::SaveSnapshot {
            draft_id: draft.id,
            desktop_client_id: draft.desktop_client_id,
            revision: "1".into(),
            text: "Keep this draft after preflight fails.".into(),
        },
    )
    .await;

    // A second fresh Send must reach the same authenticated engine after the
    // first failure, proving that the production failure path releases capacity.
    for submission in [31, 32] {
        let pending = fixture
            .coordinator
            .register_generation_connection()
            .unwrap();
        let target = GenerationTarget::Pending {
            boot_epoch: fixture.coordinator.boot_epoch().into(),
            pending_nonce: pending.nonce().into(),
        };
        let command = GenerationCommand::Send {
            conversation_id: fixture.conversation_id.clone(),
            submission_id: crate::history::encode_id([submission; 16]),
            expected_conversation_revision: "1".into(),
            expected_profile_revision: "1".into(),
            user_text: draft.text.clone(),
            draft: Some(GenerationDraft {
                id: draft.id.clone(),
                desktop_client_id: draft.desktop_client_id.clone(),
                revision: draft.revision.clone(),
            }),
        };
        let coordinator = fixture.coordinator.clone();
        let send =
            tokio::spawn(async move { coordinator.generation_request(command, pending).await });
        let mut engine = receive_props(&listener).await;
        assert!(fixture.coordinator.admission_active_for_test());
        if submission == 31 {
            if matches!(stall, Stall::Body) {
                engine
                    .get_mut()
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{",
                    )
                    .await
                    .unwrap();
            }
            if stop {
                fixture.coordinator.stop_generation(&target).unwrap();
            } else {
                // Reach the real socket with real time first. Resume before any
                // further OS/SQLite waits so automatic time cannot outrun them.
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(4)).await;
                tokio::time::resume();
            }
        } else {
            engine
                .get_mut()
                .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        }
        let error = tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .expect("preflight owner did not resolve promptly")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::ServiceUnavailable);
        let expected = if submission == 32 {
            "engine rejected the qualified request"
        } else if stop {
            "generation was stopped during engine transport"
        } else {
            "engine preflight timed out before generation admission"
        };
        assert_eq!(error.context, expected);
        assert!(!fixture.coordinator.admission_active_for_test());
        let mut trailing = [0; 1];
        let closed = tokio::time::timeout(Duration::from_secs(1), engine.read(&mut trailing))
            .await
            .expect("preflight transport retained its socket");
        assert!(match closed {
            Ok(count) => count == 0,
            Err(error) => error.kind() == std::io::ErrorKind::ConnectionReset,
        });
    }

    let retained = draft_snapshot(
        &fixture,
        DraftCommand::ReadScope {
            draft_id: draft.id.clone(),
            desktop_client_id: draft.desktop_client_id.clone(),
        },
    )
    .await;
    assert_eq!(retained, draft);
    let (turns, permit) = fixture
        .coordinator
        .history(HistoryCommand::ListTurns {
            conversation_id: fixture.conversation_id.clone(),
            cursor: None,
            limit: 50,
        })
        .await;
    drop(permit);
    let HistoryReply::TurnPage(page) = turns.unwrap() else {
        panic!("expected a turn page");
    };
    assert!(page.turns.is_empty());
    assert_eq!(
        listener.into_std().unwrap().accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "preflight failure must not open a generation connection",
    );
    fixture.coordinator.stop_service().unwrap();
    fixture.finish_stopped().await;
}

async fn receive_props(listener: &UnixListener) -> BufReader<UnixStream> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        while !headers.ends_with("\r\n\r\n") {
            assert!(reader.read_line(&mut headers).await.unwrap() > 0);
            assert!(headers.len() <= 4096);
        }
        assert!(headers.starts_with("GET /props HTTP/1.1\r\n"));
        reader
    })
    .await
    .expect("fresh generation did not reach authenticated preflight")
}

async fn draft_snapshot(fixture: &Fixture, command: DraftCommand) -> DraftSnapshot {
    let (reply, permit) = fixture.coordinator.draft(command).await;
    drop(permit);
    let DraftReply::Snapshot(snapshot) = reply.unwrap() else {
        panic!("expected a draft snapshot");
    };
    snapshot
}
