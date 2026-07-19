use loxa_core::control::auth::ControlToken;
use loxa_core::control::client::LiveControlClient;
use loxa_core::control::contracts::NodeStatus;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CliCaseKind {
    Pull,
    List,
    Load,
    Unload,
    Cancel,
    Poll,
}

struct CliCase {
    kind: CliCaseKind,
    responses: Vec<&'static str>,
    expected_stdout: &'static str,
    expected_stderr: &'static str,
    expected_exit: ExitCode,
}

fn cases() -> Vec<CliCase> {
    vec![
        CliCase {
            kind: CliCaseKind::Pull,
            responses: vec![accepted(), succeeded("download")],
            expected_stdout: "download operation op-1 accepted\ndownload completed\n",
            expected_stderr: "",
            expected_exit: ExitCode::SUCCESS,
        },
        CliCase {
            kind: CliCaseKind::List,
            responses: vec!["[]"],
            expected_stdout: "id  status  compatible  engine\n",
            expected_stderr: "",
            expected_exit: ExitCode::SUCCESS,
        },
        CliCase {
            kind: CliCaseKind::Load,
            responses: vec![accepted(), succeeded("load")],
            expected_stdout: "load operation op-1 accepted\nload completed\n",
            expected_stderr: "",
            expected_exit: ExitCode::SUCCESS,
        },
        CliCase {
            kind: CliCaseKind::Unload,
            responses: vec![accepted(), succeeded("unload")],
            expected_stdout: "unload operation op-1 accepted\nunload completed\n",
            expected_stderr: "",
            expected_exit: ExitCode::SUCCESS,
        },
        CliCase {
            kind: CliCaseKind::Cancel,
            responses: vec![cancelled()],
            expected_stdout: "load operation op-1 accepted\n",
            expected_stderr: "error: operation cancelled\n",
            expected_exit: ExitCode::from(1),
        },
        CliCase {
            kind: CliCaseKind::Poll,
            responses: vec![queued(), succeeded("load")],
            expected_stdout: "",
            expected_stderr: "",
            expected_exit: ExitCode::SUCCESS,
        },
    ]
}

fn accepted() -> &'static str {
    r#"{"operation_id":"op-1"}"#
}

fn succeeded(kind: &str) -> &'static str {
    match kind {
        "download" => operation("download", "succeeded"),
        "load" => operation("load", "succeeded"),
        "unload" => operation("unload", "succeeded"),
        _ => unreachable!(),
    }
}

fn queued() -> &'static str {
    operation("load", "queued")
}

fn cancelled() -> &'static str {
    operation("load", "cancelled")
}

fn operation(kind: &str, status: &str) -> &'static str {
    match (kind, status) {
        ("download", "succeeded") => {
            r#"{"id":"op-1","kind":"download","status":"succeeded","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}"#
        }
        ("load", "succeeded") => {
            r#"{"id":"op-1","kind":"load","status":"succeeded","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}"#
        }
        ("unload", "succeeded") => {
            r#"{"id":"op-1","kind":"unload","status":"succeeded","model_id":null,"progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}"#
        }
        ("load", "queued") => {
            r#"{"id":"op-1","kind":"load","status":"queued","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":1}"#
        }
        ("load", "cancelled") => {
            r#"{"id":"op-1","kind":"load","status":"cancelled","model_id":"gemma-3-4b-it-q4","progress":null,"error":null,"created_at_unix_ms":1,"updated_at_unix_ms":2}"#
        }
        _ => unreachable!(),
    }
}

#[test]
fn existing_cli_live_pull_list_load_unload_cancel_poll_output_is_unchanged() {
    let kinds = cases().iter().map(|case| case.kind).collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            CliCaseKind::Pull,
            CliCaseKind::List,
            CliCaseKind::Load,
            CliCaseKind::Unload,
            CliCaseKind::Cancel,
            CliCaseKind::Poll,
        ]
    );
    for case in cases() {
        let fixture = PeerFixture::spawn(case.responses);
        let client = LiveControlClient::connect(
            fixture.address,
            fixture.token.clone(),
            "runtime",
            Duration::from_secs(1),
        )
        .unwrap();
        let (exit, stdout, stderr) = crate::cli::run_v1_compatibility_case(case.kind, &client);
        assert_eq!(String::from_utf8(stdout).unwrap(), case.expected_stdout);
        assert_eq!(String::from_utf8(stderr).unwrap(), case.expected_stderr);
        assert_eq!(exit, case.expected_exit);
        fixture.join();
    }
}

#[test]
fn pull_help_discloses_detach_and_global_cancel_without_runtime_output_changes() {
    let help = crate::cli::pull_long_help_for_test();
    assert!(help.contains("Leaving this CLI detaches observation"));
    assert!(help.contains("an explicit node cancellation is global for every observer"));
}

#[test]
fn cli_rejects_replaced_peer_before_followup_request() {
    let root = std::env::temp_dir().join(format!(
        "loxa-cli-replacement-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server_token = token.clone();
    let worker = std::thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        prove(&mut first, &server_token, "node", "runtime", "keep-alive");
        let request = read_request(&mut first);
        assert!(request.starts_with("GET /loxa/v1/models "));
        send(&mut first, "[]", "close");

        let (mut replacement, _) = listener.accept().unwrap();
        prove(
            &mut replacement,
            &server_token,
            "replacement-node",
            "runtime",
            "close",
        );
        replacement
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut byte = [0_u8; 1];
        matches!(replacement.read(&mut byte), Ok(0))
    });
    let client =
        LiveControlClient::connect(address, token, "runtime", Duration::from_secs(1)).unwrap();
    assert!(client.models().unwrap().is_empty());

    assert!(matches!(
        client.load("gemma-3-4b-it-q4"),
        Err(loxa_core::control::client::ClientError::PeerProof)
    ));
    assert!(
        worker.join().unwrap(),
        "replacement received mutation bytes"
    );
    std::fs::remove_dir_all(root).unwrap();
}

struct PeerFixture {
    address: std::net::SocketAddr,
    token: ControlToken,
    worker: Option<std::thread::JoinHandle<()>>,
    root: PathBuf,
}

impl PeerFixture {
    fn spawn(responses: Vec<&'static str>) -> Self {
        let root = std::env::temp_dir().join(format!(
            "loxa-cli-v1-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let token = ControlToken::load_or_create(&root.join("control.token")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_token = token.clone();
        let worker = std::thread::spawn(move || {
            for body in responses {
                let (mut socket, _) = listener.accept().unwrap();
                let proof_request = read_request(&mut socket);
                let nonce = proof_request
                    .lines()
                    .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                    .unwrap();
                let proof = server_token
                    .node_identity_proof(nonce, "node", "runtime", NodeStatus::Unloaded)
                    .unwrap();
                let proof_body = format!(
                    r#"{{"protocol_version":1,"node_id":"node","runtime_identity":"runtime","status":"unloaded","challenge_proof":"{proof}"}}"#
                );
                send(&mut socket, &proof_body, "keep-alive");
                let request = read_request(&mut socket);
                assert!(request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer "));
                send(&mut socket, body, "close");
            }
        });
        Self {
            address,
            token,
            worker: Some(worker),
            root,
        }
    }

    fn join(mut self) {
        self.worker.take().unwrap().join().unwrap();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn read_request(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        socket.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
    }
    let headers = String::from_utf8(bytes).unwrap();
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0_u8; length];
    socket.read_exact(&mut body).unwrap();
    headers
}

fn prove(
    socket: &mut TcpStream,
    token: &ControlToken,
    node_id: &str,
    runtime: &str,
    connection: &str,
) {
    let proof_request = read_request(socket);
    assert!(!proof_request
        .to_ascii_lowercase()
        .contains("authorization:"));
    let nonce = proof_request
        .lines()
        .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
        .unwrap();
    let proof = token
        .node_identity_proof(nonce, node_id, runtime, NodeStatus::Unloaded)
        .unwrap();
    let proof_body = format!(
        r#"{{"protocol_version":1,"node_id":"{node_id}","runtime_identity":"{runtime}","status":"unloaded","challenge_proof":"{proof}"}}"#
    );
    send(socket, &proof_body, connection);
}

fn send(socket: &mut TcpStream, body: &str, connection: &str) {
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}
