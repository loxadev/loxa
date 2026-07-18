use super::auth::ControlToken;
use super::contracts::NodeStatus;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

pub(crate) const NODE_ID: &str = "00000000-0000-4000-8000-000000000001";
pub(crate) const INSTANCE_ID: &str = "00000000-0000-4000-8000-000000000002";

pub(crate) struct ScriptedPeer {
    pub(crate) address: SocketAddr,
    #[allow(dead_code)]
    pub(crate) requests: Arc<Mutex<Vec<String>>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ScriptedPeer {
    pub(crate) fn spawn(token: ControlToken, responses: Vec<(&'static str, &'static str)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted peer");
        let address = listener.local_addr().expect("scripted peer address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let worker = std::thread::spawn(move || {
            for (content_type, body) in responses {
                let (mut socket, _) = listener.accept().expect("accept proof connection");
                let proof_request = read_request(&mut socket);
                captured.lock().unwrap().push(proof_request.clone());
                let nonce = proof_request
                    .lines()
                    .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                    .expect("proof nonce");
                let proof = token
                    .node_identity_proof(nonce, NODE_ID, INSTANCE_ID, NodeStatus::Unloaded)
                    .expect("build proof");
                let proof_body = format!(
                    r#"{{"protocol_version":1,"node_id":"{NODE_ID}","runtime_identity":"{INSTANCE_ID}","status":"unloaded","challenge_proof":"{proof}"}}"#
                );
                write_response(&mut socket, "application/json", &proof_body, "keep-alive");
                let request = read_request(&mut socket);
                captured.lock().unwrap().push(request);
                write_response(&mut socket, content_type, body, "close");
            }
        });
        Self {
            address,
            requests,
            worker: Some(worker),
        }
    }

    pub(crate) fn join(mut self) {
        self.worker.take().unwrap().join().expect("scripted peer");
    }
}

fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        socket.read_exact(&mut byte).expect("read request");
        request.push(byte[0]);
    }
    String::from_utf8(request).expect("request UTF-8")
}

fn write_response(socket: &mut TcpStream, content_type: &str, body: &str, connection: &str) {
    let result = write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{body}",
        body.len()
    );
    if let Err(error) = result {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ),
            "write response: {error}"
        );
    }
}
