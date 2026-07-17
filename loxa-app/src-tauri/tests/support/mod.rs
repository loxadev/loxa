use loxa_core::control::auth::ControlToken;
use loxa_core::control::contracts::NodeStatus;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use loxa_app_lib::bootstrap::BootstrapConfig;

pub const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
pub const NODE_ID: &str = "123e4567-e89b-42d3-a456-426614174000";
pub const INSTANCE_ID: &str = "123e4567-e89b-42d3-a456-426614174001";
pub const REPLACEMENT_INSTANCE_ID: &str = "123e4567-e89b-42d3-a456-426614174099";
pub const EPOCH: &str = "123e4567-e89b-42d3-a456-426614174002";
pub const SLOT_ID: &str = "123e4567-e89b-42d3-a456-426614174003";
pub const REPLACEMENT_EPOCH: &str = "123e4567-e89b-42d3-a456-426614174055";

pub enum ScriptedResponse {
    Proof {
        node_id: &'static str,
        instance_id: &'static str,
    },
    Json {
        path: String,
        body: Vec<u8>,
    },
    ReplacementBeforeBearer,
}

impl ScriptedResponse {
    pub fn proof() -> Self {
        Self::Proof {
            node_id: NODE_ID,
            instance_id: INSTANCE_ID,
        }
    }

    pub fn replacement_proof() -> Self {
        Self::ReplacementBeforeBearer
    }

    pub fn json(path: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self::Json {
            path: path.into(),
            body: body.into(),
        }
    }
}

pub struct ScriptedPeer {
    pub endpoint: String,
    pub credential_path: PathBuf,
    worker: Option<JoinHandle<()>>,
}

impl ScriptedPeer {
    pub fn spawn(script: Vec<ScriptedResponse>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "loxa-tauri-v2-peer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let credential_path = directory.join("control.token");
        std::fs::write(&credential_path, format!("{TOKEN}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&credential_path, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        let token = ControlToken::load(&credential_path).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let response_endpoint = endpoint.clone();
        let worker = thread::spawn(move || {
            for response in script {
                let (mut stream, _) = listener.accept().unwrap();
                match response {
                    ScriptedResponse::Proof {
                        node_id,
                        instance_id,
                    } => {
                        let request = read_request(&mut stream);
                        assert!(request.starts_with("GET /loxa/v1/node HTTP/1.1\r\n"));
                        assert!(!request.to_ascii_lowercase().contains("authorization:"));
                        write_proof(&mut stream, &token, &request, node_id, instance_id, false);
                    }
                    ScriptedResponse::Json { path, mut body } => {
                        let proof_request = read_request(&mut stream);
                        assert!(proof_request.starts_with("GET /loxa/v1/node HTTP/1.1\r\n"));
                        assert!(
                            !proof_request
                                .to_ascii_lowercase()
                                .contains("authorization:")
                        );
                        write_proof(
                            &mut stream,
                            &token,
                            &proof_request,
                            NODE_ID,
                            INSTANCE_ID,
                            true,
                        );
                        let request = read_request(&mut stream);
                        assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
                        assert_eq!(
                            header(&request, "authorization"),
                            Some(format!("Bearer {TOKEN}").as_str())
                        );
                        body = String::from_utf8(body)
                            .unwrap()
                            .replace("__ENDPOINT__", &response_endpoint)
                            .into_bytes();
                        write_json(&mut stream, &body);
                    }
                    ScriptedResponse::ReplacementBeforeBearer => {
                        let request = read_request(&mut stream);
                        assert!(request.starts_with("GET /loxa/v1/node HTTP/1.1\r\n"));
                        assert!(!request.to_ascii_lowercase().contains("authorization:"));
                        write_proof(
                            &mut stream,
                            &token,
                            &request,
                            NODE_ID,
                            REPLACEMENT_INSTANCE_ID,
                            true,
                        );
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .unwrap();
                        let mut leaked = Vec::new();
                        stream.read_to_end(&mut leaked).unwrap();
                        assert!(
                            leaked.is_empty(),
                            "bearer bytes sent after replacement proof"
                        );
                    }
                }
            }
        });
        Self {
            endpoint,
            credential_path,
            worker: Some(worker),
        }
    }

    pub fn finish(mut self) {
        self.worker.take().unwrap().join().unwrap();
        let directory = self.credential_path.parent().unwrap().to_owned();
        std::fs::remove_file(&self.credential_path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}

fn write_proof(
    stream: &mut TcpStream,
    token: &ControlToken,
    request: &str,
    node_id: &str,
    instance_id: &str,
    keep_alive: bool,
) {
    let nonce = header(request, "x-loxa-challenge").unwrap();
    let proof = token
        .node_identity_proof(nonce, node_id, instance_id, NodeStatus::Unloaded)
        .unwrap();
    let body = format!(
        "{{\"protocol_version\":1,\"node_id\":\"{node_id}\",\"runtime_identity\":\"{instance_id}\",\"status\":\"unloaded\",\"challenge_proof\":\"{proof}\"}}"
    );
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n{body}",
        body.len(),
        if keep_alive { "keep-alive" } else { "close" }
    )
    .unwrap();
}

fn write_json(stream: &mut TcpStream, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    let _ = stream.write_all(body);
}

pub fn bootstrap_config(peer: &ScriptedPeer) -> BootstrapConfig {
    BootstrapConfig {
        executable: None,
        credential_path: peer.credential_path.clone(),
        startup_timeout: Duration::from_secs(1),
        poll_interval: Duration::from_millis(100),
        inherit_debug_stderr: false,
    }
}

fn read_request(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut chunk = [0_u8; 1024];
        let count = stream.read(&mut chunk).unwrap();
        assert_ne!(count, 0, "request ended before headers");
        request.extend_from_slice(&chunk[..count]);
        assert!(request.len() <= 16 * 1024, "request headers exceeded bound");
    }
    String::from_utf8(request).unwrap()
}

fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (candidate, value) = line.split_once(": ")?;
        candidate.eq_ignore_ascii_case(name).then_some(value)
    })
}

pub fn node_collection(revision: u64) -> Vec<u8> {
    format!(
        "{{\"schema_version\":2,\"epoch\":\"{EPOCH}\",\"revision\":\"{revision}\",\"generated_at_unix_ms\":\"1784246400600\",\"nodes\":[{{\"node_id\":\"{NODE_ID}\",\"node_instance_id\":\"{INSTANCE_ID}\",\"control_endpoint\":\"__ENDPOINT__\",\"status\":\"running\",\"slot_capacity\":1,\"capabilities\":{{\"model_download\":true,\"slot_load\":true,\"slot_unload\":true,\"operation_cancel\":true,\"operation_stream\":true}}}}]}}"
    )
    .into_bytes()
}

pub fn slot_collection(revision: u64) -> Vec<u8> {
    format!(
        "{{\"schema_version\":2,\"epoch\":\"{EPOCH}\",\"revision\":\"{revision}\",\"generated_at_unix_ms\":\"1784246400600\",\"node_id\":\"{NODE_ID}\",\"slots\":[{{\"slot_id\":\"{SLOT_ID}\",\"node_id\":\"{NODE_ID}\",\"name\":\"default\",\"status\":\"unloaded\",\"model_id\":null,\"operation_id\":null,\"error\":null}}]}}"
    )
    .into_bytes()
}

pub fn operation_collection(revision: u64) -> Vec<u8> {
    format!(
        "{{\"schema_version\":2,\"epoch\":\"{EPOCH}\",\"revision\":\"{revision}\",\"generated_at_unix_ms\":\"1784246400600\",\"operations\":[]}}"
    )
    .into_bytes()
}

pub fn active_load_operation_collection(revision: u64) -> Vec<u8> {
    format!(
        "{{\"schema_version\":2,\"epoch\":\"{EPOCH}\",\"revision\":\"{revision}\",\"generated_at_unix_ms\":\"1784246400600\",\"operations\":[{{\"operation_id\":\"123e4567-e89b-42d3-a456-426614174004\",\"node_id\":\"{NODE_ID}\",\"kind\":\"load\",\"status\":\"running\",\"slot_id\":\"{SLOT_ID}\",\"model_id\":\"model\",\"progress\":null,\"error\":null,\"created_revision\":\"{revision}\",\"updated_revision\":\"{revision}\",\"created_at_unix_ms\":\"1784246400500\",\"updated_at_unix_ms\":\"1784246400500\"}}]}}"
    )
    .into_bytes()
}

pub fn terminal_operation_collection(count: usize) -> Vec<u8> {
    let operations = (0..count)
        .map(|index| {
            let operation_id = format!("123e4567-e89b-42d3-a456-{index:012x}");
            format!(
                "{{\"operation_id\":\"{operation_id}\",\"node_id\":\"{NODE_ID}\",\"kind\":\"download\",\"status\":\"succeeded\",\"slot_id\":null,\"model_id\":\"model\",\"progress\":null,\"error\":null,\"created_revision\":\"1\",\"updated_revision\":\"1\",\"created_at_unix_ms\":\"1\",\"updated_at_unix_ms\":\"1\"}}"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"schema_version\":2,\"epoch\":\"{EPOCH}\",\"revision\":\"11\",\"generated_at_unix_ms\":\"1784246400600\",\"operations\":[{operations}]}}"
    )
    .into_bytes()
}

pub fn successful_state_script(revision: u64) -> Vec<ScriptedResponse> {
    successful_state_script_with_epoch(revision, EPOCH)
}

pub fn successful_state_script_with_epoch(revision: u64, epoch: &str) -> Vec<ScriptedResponse> {
    let replace_epoch = |body: Vec<u8>| {
        String::from_utf8(body)
            .unwrap()
            .replace(EPOCH, epoch)
            .into_bytes()
    };
    vec![
        ScriptedResponse::json("/loxa/v2/nodes", replace_epoch(node_collection(revision))),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            replace_epoch(slot_collection(revision)),
        ),
        ScriptedResponse::json(
            "/loxa/v2/operations",
            replace_epoch(operation_collection(revision)),
        ),
    ]
}
