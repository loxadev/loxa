use crate::gateway::{self, EngineTarget, GatewayServer, GatewayState, MODEL_ALIAS};
use axum::{routing::get, Router};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ControlVersion {
    V1Only,
    V1AndV2,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InferenceCompatibility {
    pub(crate) model_alias: String,
    pub(crate) response_bytes: Vec<u8>,
}

pub(crate) fn inference_compatibility_fixture(
    control_version: ControlVersion,
) -> InferenceCompatibility {
    let upstream = DeterministicUpstream::start();
    let state = GatewayState::new(NodeId::new_v4(), NodeInstanceId::new_v4());
    state.publish(EngineTarget {
        base_url: upstream.endpoint(),
        backend_alias: "loxa-slice3-fixture-g0".to_string(),
        engine: "deterministic-fixture".to_string(),
        engine_version: "1".to_string(),
        model_id: "slice3-fixture-model".to_string(),
        profile: "default".to_string(),
    });

    let app = match control_version {
        ControlVersion::V1Only => gateway::router(state.clone()),
        ControlVersion::V1AndV2 => gateway::router(state.clone()).merge(Router::new().route(
            "/loxa/v2/nodes",
            get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )),
    };
    let gateway = GatewayServer::start_with_router(0, state, app).expect("start real gateway");
    let response = reqwest::blocking::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/chat/completions",
            gateway.port()
        ))
        .json(&serde_json::json!({
            "model": MODEL_ALIAS,
            "messages": [{"role": "user", "content": "compatibility"}]
        }))
        .send()
        .expect("request real gateway");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response_bytes = response.bytes().expect("read raw gateway body").to_vec();
    let model_alias = serde_json::from_slice::<serde_json::Value>(&response_bytes)
        .expect("gateway response is JSON")
        .get("model")
        .and_then(serde_json::Value::as_str)
        .expect("gateway response has model alias")
        .to_string();
    gateway.shutdown().expect("stop real gateway");
    upstream.join();

    InferenceCompatibility {
        model_alias,
        response_bytes,
    }
}

struct DeterministicUpstream {
    address: std::net::SocketAddr,
    thread: thread::JoinHandle<()>,
}

impl DeterministicUpstream {
    fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("bind deterministic inference upstream");
        let address = listener.local_addr().expect("read upstream address");
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept gateway request");
            read_http_request(&mut stream);
            let body = br#"{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"slice3-compatible","role":"assistant"}}],"id":"chatcmpl-slice3","model":"loxa-slice3-fixture-g0","object":"chat.completion"}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .expect("write upstream response headers");
            stream
                .write_all(body)
                .expect("write upstream response body");
        });
        Self { address, thread }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn join(self) {
        self.thread.join().expect("join deterministic upstream");
    }
}

fn read_http_request(stream: &mut TcpStream) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    let mut expected_len = None;
    loop {
        let read = stream.read(&mut buffer).expect("read gateway request");
        assert_ne!(read, 0, "gateway closed an incomplete request");
        request.extend_from_slice(&buffer[..read]);
        if expected_len.is_none() {
            if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                let header_end = header_end + 4;
                let headers = std::str::from_utf8(&request[..header_end])
                    .expect("gateway request headers are UTF-8");
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length: ")
                            .or_else(|| line.strip_prefix("Content-Length: "))
                    })
                    .expect("gateway request has content length")
                    .parse::<usize>()
                    .expect("valid content length");
                expected_len = Some(header_end + content_length);
            }
        }
        if expected_len.is_some_and(|expected| request.len() >= expected) {
            return;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Mismatch {
    Pid,
    StartTime,
    RunId,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReplacementTeardown {
    pub(crate) foreign_child_signalled: bool,
    pub(crate) recovery_required: bool,
}

#[derive(Default)]
struct SignalRecordingChild {
    signal_count: usize,
}

impl crate::supervisor::ManagedChild for SignalRecordingChild {
    fn pid(&self) -> u32 {
        777
    }

    fn terminate(&mut self) -> std::io::Result<()> {
        self.signal_count += 1;
        Ok(())
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.signal_count += 1;
        Ok(())
    }

    fn try_wait(&mut self) -> std::io::Result<Option<i32>> {
        Ok(Some(0))
    }
}

impl crate::supervisor::LogDrainingChild for SignalRecordingChild {
    fn join_log_drains(&mut self) -> Result<(), crate::supervisor::SupervisorError> {
        Ok(())
    }
}

pub(crate) fn replacement_teardown_fixture(mismatch: Mismatch) -> ReplacementTeardown {
    use crate::supervisor::{
        teardown_owned_run, ManagedRun, OwnerTeardownDecision, OwnerTerminalOutcome, RunLifecycle,
        RUNTIME_STATE_SCHEMA_VERSION,
    };

    let temp = tempfile::tempdir().expect("create replacement fixture");
    let state_path = temp.path().join("managed.json");
    let expected = ManagedRun {
        schema_version: RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "owned-run".to_string(),
        model_id: Some("slice3-fixture-model".to_string()),
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: RunLifecycle::Running,
        generation: 0,
        generation_alias: "loxa-owned-run-g0".to_string(),
        control_port: Some(8080),
        port: 8081,
        log_path: PathBuf::from("fixture.log"),
        child_pid: Some(777),
        child_process_start_time_unix_s: Some(111),
        child_pgid: None,
    };
    let mut replacement = expected.clone();
    match mismatch {
        Mismatch::Pid => replacement.child_pid = Some(778),
        Mismatch::StartTime => replacement.child_process_start_time_unix_s = Some(112),
        Mismatch::RunId => {
            replacement.run_id = "replacement-run".to_string();
            replacement.generation_alias = "loxa-replacement-run-g0".to_string();
        }
    }
    crate::supervisor::write_runtime_state_for_slice3_fixture(&state_path, &[replacement]);

    let mut child = SignalRecordingChild::default();
    let outcome = teardown_owned_run(
        &mut child,
        &state_path,
        &expected.identity(),
        OwnerTeardownDecision::RequestedStop,
    )
    .expect("identity mismatch is a recovery outcome");
    ReplacementTeardown {
        foreign_child_signalled: child.signal_count != 0,
        recovery_required: outcome == OwnerTerminalOutcome::RecoveryRequired,
    }
}

pub(crate) fn missing_state_teardown_fixture() -> (usize, bool) {
    use crate::supervisor::{
        teardown_owned_run, ManagedRunIdentity, OwnerTeardownDecision, OwnerTerminalOutcome,
    };

    let temp = tempfile::tempdir().expect("create missing-state fixture");
    let mut child = SignalRecordingChild::default();
    let outcome = teardown_owned_run(
        &mut child,
        &temp.path().join("missing-managed.json"),
        &ManagedRunIdentity {
            run_id: "disappeared-run".to_string(),
            generation: 0,
            child_pid: Some(777),
            child_process_start_time_unix_s: Some(111),
        },
        OwnerTeardownDecision::RequestedStop,
    )
    .expect("missing state retains teardown outcome");
    (
        child.signal_count,
        outcome == OwnerTerminalOutcome::RecoveryRequired,
    )
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DurableFailure {
    Legacy,
    Corrupt,
    ReadError,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DurableFailureTeardown {
    pub(crate) signal_count: usize,
    pub(crate) failed_closed: bool,
    pub(crate) evidence_preserved: bool,
}

pub(crate) fn durable_failure_teardown_fixture(failure: DurableFailure) -> DurableFailureTeardown {
    use crate::supervisor::{
        teardown_owned_run, ManagedRunIdentity, OwnerTeardownDecision, OwnerTerminalOutcome,
    };
    use std::fs;

    let temp = tempfile::tempdir().expect("create durable-failure fixture");
    let state_path = temp.path().join("managed.json");
    match failure {
        DurableFailure::Legacy => fs::write(&state_path, b"[]").expect("seed legacy state"),
        DurableFailure::Corrupt => {
            fs::write(&state_path, b"{not-json").expect("seed corrupt state")
        }
        DurableFailure::ReadError => {
            fs::create_dir(&state_path).expect("seed unreadable state path")
        }
    }
    let before = match failure {
        DurableFailure::Legacy | DurableFailure::Corrupt => {
            Some(fs::read(&state_path).expect("capture durable evidence"))
        }
        DurableFailure::ReadError => None,
    };
    let mut child = SignalRecordingChild::default();
    let result = teardown_owned_run(
        &mut child,
        &state_path,
        &ManagedRunIdentity {
            run_id: "owned-run".to_string(),
            generation: 0,
            child_pid: Some(777),
            child_process_start_time_unix_s: Some(111),
        },
        OwnerTeardownDecision::RequestedStop,
    );
    let evidence_preserved = match before {
        Some(before) => fs::read(&state_path).is_ok_and(|after| after == before),
        None => state_path.is_dir(),
    };
    DurableFailureTeardown {
        signal_count: child.signal_count,
        failed_closed: matches!(result, Err(_) | Ok(OwnerTerminalOutcome::RecoveryRequired)),
        evidence_preserved,
    }
}
