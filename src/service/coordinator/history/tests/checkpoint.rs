use super::*;
use crate::service::coordinator::generation::{run_for_test, stream_for_test, StreamProgress};
use crate::service::coordinator::state::EngineDescriptor;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

mod lifecycle;
mod status;

#[tokio::test(flavor = "current_thread")]
async fn dirty_deadline_saves_utf8_during_silent_body_and_terminal_does_not_duplicate_it() {
    let mut relay = StreamingFixture::start().await;
    relay.delta("é").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(relay.progress.borrow().checkpoints, 0);
    tokio::time::resume();
    relay.delta("🙂").await;
    relay
        .advance_to_checkpoint(Duration::from_millis(150), 1)
        .await;

    relay.assert_saved("é🙂").await;
    assert!(!relay.task.is_finished());

    relay.delta("終").await;
    relay.complete_engine().await;
    relay
        .finish(ExecutionOutcome::Completed, None, "é🙂終", 2)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn delayed_checkpoint_keeps_one_tail_and_acknowledgement_flushes_it() {
    let mut relay = StreamingFixture::start().await;
    let mut stalled = HistoryStall::new(&relay.fixture.coordinator);
    relay.delta("é").await;
    relay
        .advance_to_checkpoint(Duration::from_millis(250), 1)
        .await;
    relay.delta("🙂").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(relay.progress.borrow().checkpoints, 1);
    assert!(!relay.output.is_cancelled());
    assert_eq!(
        relay.output.status().unwrap(),
        OutputSavePhase::Saving { saved_end: 0 }
    );
    tokio::time::resume();
    stalled.release();
    // No further body bytes arrive: acknowledgement must flush the overdue tail.
    relay
        .wait_progress(|progress| progress.checkpoints == 2)
        .await;
    relay.assert_saved("é🙂").await;
    relay.complete_engine().await;
    relay
        .finish(ExecutionOutcome::Completed, None, "é🙂", 2)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn prompt_only_terminal_usage_keeps_optional_output_statistics_unknown() {
    let mut relay = StreamingFixture::start().await;
    relay.delta("answer").await;
    relay
        .complete_engine_with_usage(r#"{"prompt_tokens":1}"#)
        .await;
    relay
        .finish_with_output_tokens(ExecutionOutcome::Completed, None, "answer", 1, None)
        .await;
}

struct StreamingFixture {
    _engine_directory: tempfile::TempDir,
    fixture: Fixture,
    committed: CommittedAdmission,
    output: GenerationOutput,
    engine: Option<UnixStream>,
    listener: Option<UnixListener>,
    full_run: bool,
    progress: watch::Receiver<StreamProgress>,
    task: tokio::task::JoinHandle<()>,
}

impl StreamingFixture {
    async fn start() -> Self {
        let mut fixture = Self::start_driver(false).await;
        fixture.start_body().await;
        fixture
    }

    async fn start_full_run() -> Self {
        Self::start_driver(true).await
    }

    async fn start_driver(full_run: bool) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("lcp-")
            .tempdir_in("/tmp")
            .unwrap();
        let endpoint = directory.path().join("engine.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        fs::set_permissions(&endpoint, fs::Permissions::from_mode(0o600)).unwrap();
        let engine = EngineDescriptor {
            pid: std::process::id(),
            endpoint: Arc::new(endpoint),
        };
        let fixture = Fixture::start_with_engine(engine.clone()).await;
        let committed = wait_for_admission(
            fixture
                .coordinator
                .admit_history_generation(fixture.input(41, "stream output"))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let output = fixture.coordinator.generation_output(&committed).unwrap();
        let reservation = fixture
            .coordinator
            .shared
            .state()
            .current_admission()
            .unwrap();
        let (progress_tx, progress) = watch::channel(StreamProgress::default());
        let task = if full_run {
            tokio::spawn(run_for_test(
                fixture.coordinator.clone(),
                reservation,
                committed.clone(),
                progress_tx,
            ))
        } else {
            let mut pipeline = OutputPipeline::new(output.clone(), committed.clone());
            tokio::spawn(async move {
                let mut decoder = SseDecoder::new();
                let (outcome, failure) = match stream_for_test(
                    &engine,
                    &reservation,
                    &mut decoder,
                    &mut pipeline,
                    progress_tx,
                )
                .await
                {
                    Ok(ExecutionOutcome::Stopped) => (ExecutionOutcome::Stopped, Some("stopped")),
                    Ok(outcome) => (outcome, None),
                    Err(code) => (ExecutionOutcome::Failed, Some(code)),
                };
                let mut measurements = AttemptMeasurements::new(1);
                measurements.freeze_duration(reservation.accepted_elapsed());
                pipeline
                    .finish(&mut decoder, outcome, failure, measurements)
                    .await
                    .unwrap();
            })
        };
        let engine =
            receive_request(&listener, "POST /v1/chat/completions HTTP/1.1\r\n", b"{}").await;
        Self {
            _engine_directory: directory,
            fixture,
            committed,
            output,
            engine: Some(engine),
            listener: Some(listener),
            full_run,
            progress,
            task,
        }
    }

    async fn start_body(&mut self) {
        self.engine
            .as_mut()
            .unwrap()
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
    }

    async fn respond_slots(&self, body: &str, content_length: usize) {
        let mut slots = receive_request(
            self.listener.as_ref().unwrap(),
            "GET /slots HTTP/1.1\r\n",
            b"",
        )
        .await;
        slots.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n{body}").as_bytes()).await.unwrap();
        slots.shutdown().await.unwrap();
    }

    async fn complete_synthetic_cleanup(&mut self) {
        use std::sync::atomic::Ordering;
        assert!(self.full_run);
        assert!(self.fixture.operation.cancel.load(Ordering::Acquire));
        assert!(self.fixture.operation.retry_cleanup.load(Ordering::Acquire) > 0);
        self.assert_transport_closed().await;
        drop(self.engine.take());
        drop(self.listener.take());
        fs::remove_file(self._engine_directory.path().join("engine.sock")).unwrap();
        // The fake endpoint's PID is this test process. Simulate owner completion
        // through the real state transition; native acceptance verifies OS cleanup.
        self.fixture
            .coordinator
            .shared
            .state()
            .complete(&self.fixture.operation, None);
        assert!(matches!(
            self.fixture.coordinator.status().phase,
            loxa_ipc::RuntimePhase::Unloaded
        ));
    }

    fn target(&self) -> GenerationTarget {
        GenerationTarget::Accepted {
            boot_epoch: self.committed.owner_epoch.clone(),
            submission_id: crate::history::encode_id(self.committed.submission_id),
            operation_generation: self.committed.operation_generation.to_string(),
        }
    }

    fn snapshot(&self) -> loxa_ipc::GenerationStatus {
        let snapshot = self
            .fixture
            .coordinator
            .generation_status(&self.target())
            .unwrap()
            .unwrap();
        loxa_ipc::Reply {
            request_id: "snapshot".into(),
            outcome: loxa_ipc::ReplyOutcome::GenerationStatus {
                snapshot: Some(snapshot.clone()),
            },
        }
        .validate_shape()
        .unwrap();
        snapshot
    }

    async fn wait_status(
        &self,
        ready: impl Fn(&loxa_ipc::GenerationStatus) -> bool,
    ) -> loxa_ipc::GenerationStatus {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = self.snapshot();
                if ready(&snapshot) {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("generation did not publish the expected status")
    }

    async fn assert_transport_closed(&mut self) {
        let mut byte = [0; 1];
        let closed = tokio::time::timeout(
            Duration::from_secs(2),
            self.engine.as_mut().unwrap().read(&mut byte),
        )
        .await
        .expect("cancellation retained the engine transport while SQLite was stalled");
        assert!(match closed {
            Ok(bytes) => bytes == 0,
            Err(error) => error.kind() == std::io::ErrorKind::ConnectionReset,
        });
    }

    async fn delta(&mut self, text: &str) {
        let end = self.progress.borrow().decoded_end + text.len();
        let data = serde_json::json!({"choices":[{"index":0,"delta":{"content":text}}]});
        self.engine
            .as_mut()
            .unwrap()
            .write_all(format!("data: {data}\n\n").as_bytes())
            .await
            .unwrap();
        self.wait_progress(|progress| progress.decoded_end == end)
            .await;
    }

    async fn wait_progress(&mut self, ready: impl Fn(&StreamProgress) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready(&self.progress.borrow()) {
                self.progress
                    .changed()
                    .await
                    .expect("relay stopped before reaching the test gate");
            }
        })
        .await
        .expect("relay did not reach the test gate");
    }

    async fn advance_to_checkpoint(&mut self, delay: Duration, count: usize) {
        tokio::time::pause();
        // Tokio rounds deadlines up to a millisecond tick. One extra tick
        // cannot hide the 100 ms delay caused by resetting the dirty deadline.
        tokio::time::advance(delay + Duration::from_millis(1)).await;
        let advanced = tokio::time::Instant::now();
        self.wait_progress(|progress| progress.checkpoints == count)
            .await;
        assert_eq!(
            tokio::time::Instant::now(),
            advanced,
            "checkpoint extended the first dirty deadline"
        );
        tokio::time::resume();
    }

    async fn assert_saved(&self, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (reply, permit) = self
                    .fixture
                    .coordinator
                    .history(HistoryCommand::ReadContentRange {
                        source: loxa_ipc::ContentSource::Assistant {
                            attempt_id: crate::history::encode_id(self.committed.attempt_id),
                        },
                        start: "0".into(),
                        prefix_end: expected.len().to_string(),
                    })
                    .await;
                drop(permit);
                match reply {
                    Ok(HistoryReply::ContentRange(range)) => {
                        assert_eq!(range.content, expected);
                        assert_eq!(range.end, expected.len().to_string());
                        return;
                    }
                    Err(error) if error.category == ErrorCategory::InvalidRequest => {}
                    other => panic!("unexpected content-range result: {other:?}"),
                }
            }
        })
        .await
        .expect("checkpoint did not become readable");
    }

    async fn complete_engine(&mut self) {
        self.complete_engine_with_usage(
            r#"{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}"#,
        )
        .await;
    }

    async fn complete_engine_with_usage(&mut self, usage: &str) {
        self.engine
            .as_mut()
            .unwrap()
            .write_all(format!(
                "data: {{\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\ndata: {{\"choices\":[],\"usage\":{usage}}}\n\ndata: [DONE]\n\n"
            ).as_bytes())
            .await
            .unwrap();
        self.engine.as_mut().unwrap().shutdown().await.unwrap();
    }

    async fn finish(
        self,
        expected: ExecutionOutcome,
        failure_code: Option<&str>,
        text: &str,
        chunks: i64,
    ) {
        self.finish_inner(expected, failure_code, text, chunks, None)
            .await;
    }

    async fn finish_with_output_tokens(
        self,
        expected: ExecutionOutcome,
        failure_code: Option<&str>,
        text: &str,
        chunks: i64,
        output_tokens: Option<i64>,
    ) {
        self.finish_inner(expected, failure_code, text, chunks, Some(output_tokens))
            .await;
    }

    async fn finish_inner(
        mut self,
        expected: ExecutionOutcome,
        failure_code: Option<&str>,
        text: &str,
        chunks: i64,
        expected_output_tokens: Option<Option<i64>>,
    ) {
        tokio::time::timeout(Duration::from_secs(2), &mut self.task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !self.fixture.coordinator.admission_active_for_test(),
            "relay returned with unresolved admission: {:?}",
            self.fixture.coordinator.generation_status(&self.target())
        );
        if self.full_run {
            if expected == ExecutionOutcome::Completed {
                assert!(!self
                    .fixture
                    .operation
                    .cancel
                    .load(std::sync::atomic::Ordering::Acquire));
                assert!(self._engine_directory.path().join("engine.sock").exists());
            } else {
                assert!(self.engine.is_none());
                assert!(self.listener.is_none());
                assert!(!self._engine_directory.path().join("engine.sock").exists());
            }
        }
        assert_eq!(
            self.fixture
                .coordinator
                .generation_status(&self.target())
                .unwrap(),
            None
        );
        if !text.is_empty() {
            self.assert_saved(text).await;
        }
        self.fixture.coordinator.stop_service().unwrap();
        self.fixture.finish_stopped().await;
        let connection = rusqlite::Connection::open(self.fixture.root.join("app.sqlite")).unwrap();
        let saved: (i64, i64, i64, i64, String) = connection.query_row(
            "SELECT a.saved_end, a.generated_end, a.terminal_saved_end,
                    (SELECT COUNT(*) FROM attempt_chunks),
                    COALESCE((SELECT GROUP_CONCAT(content, '') FROM (SELECT content FROM attempt_chunks ORDER BY start_offset)), '')
             FROM attempts a", [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        ).unwrap();
        let end = text.len() as i64;
        assert_eq!(saved, (end, end, end, chunks, text.into()));
        let terminal: (i64, i64, Option<String>) = connection
            .query_row(
                "SELECT execution_outcome, save_outcome, failure_code FROM attempts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let outcome = match expected {
            ExecutionOutcome::Completed => 1,
            ExecutionOutcome::Stopped => 2,
            ExecutionOutcome::Failed => 3,
        };
        let expected_terminal = (outcome, 1, failure_code.map(str::to_owned));
        assert_eq!(terminal, expected_terminal);
        let statistics: (Option<i64>, Option<i64>, Option<i64>, i64, i64) = connection
            .query_row(
                "SELECT qualified_input_tokens, qualified_output_tokens,
                        service_first_output_latency_ms, service_total_duration_ms, stop_reason
                 FROM attempt_statistics",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(statistics.0, Some(1));
        if let Some(expected_output_tokens) = expected_output_tokens {
            assert_eq!(statistics.1, expected_output_tokens);
        }
        if self.full_run && !text.is_empty() {
            assert!(statistics.2.is_some());
        }
        assert!(statistics.2.is_none_or(|first| first <= statistics.3));
        assert_eq!(
            statistics.4,
            match expected {
                ExecutionOutcome::Completed => 1,
                ExecutionOutcome::Stopped => 3,
                ExecutionOutcome::Failed => 4,
            }
        );
        connection.close().unwrap();
    }
}

async fn receive_request(
    listener: &UnixListener,
    expected_line: &str,
    expected_body: &[u8],
) -> UnixStream {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut request = BufReader::new(socket);
        let mut line = String::new();
        request.read_line(&mut line).await.unwrap();
        assert_eq!(line, expected_line);
        loop {
            line.clear();
            assert_ne!(request.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
        }
        let mut body = vec![0; expected_body.len()];
        request.read_exact(&mut body).await.unwrap();
        assert_eq!(body, expected_body);
        request.into_inner()
    })
    .await
    .expect("relay did not send the expected engine request")
}

struct HistoryStall(Option<Arc<Barrier>>);

impl HistoryStall {
    fn new(coordinator: &Coordinator) -> Self {
        let barrier = Arc::new(Barrier::new(2));
        coordinator.stall_history_for_test(Arc::clone(&barrier));
        barrier.wait();
        Self(Some(barrier))
    }

    fn release(&mut self) {
        if let Some(barrier) = self.0.take() {
            barrier.wait();
        }
    }
}

impl Drop for HistoryStall {
    fn drop(&mut self) {
        self.release();
    }
}
