use serde::{Deserialize, Serialize};
use std::io::Read;
use std::sync::mpsc;
use std::time::Duration;

const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_ASSISTANT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BODY_BYTES: usize = 16 * 1024;
const EVENT_QUEUE_CAPACITY: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    Delta(String),
    Complete(String),
    Error(String),
}

pub struct Worker {
    events: mpsc::Receiver<Event>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    pub fn start(port: u16, model: String, messages: Vec<Message>) -> Result<Self, String> {
        let (sender, events) = mpsc::sync_channel(EVENT_QUEUE_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("loxa-chat-request".into())
            .spawn(move || {
                if let Err(error) = request(port, &model, &messages, &sender) {
                    let _ = sender.send(Event::Error(error));
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            events,
            thread: Some(thread),
        })
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Event, mpsc::RecvTimeoutError> {
        self.events.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<Event, mpsc::TryRecvError> {
        self.events.try_recv()
    }

    pub fn join(mut self) -> Result<(), String> {
        self.thread
            .take()
            .expect("chat worker thread is present")
            .join()
            .map_err(|_| "chat request worker panicked".to_string())
    }
}

#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    messages: &'a [Message],
    stream: bool,
}

fn request(
    port: u16,
    model: &str,
    messages: &[Message],
    sender: &mpsc::SyncSender<Event>,
) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| error.to_string())?;
    let mut response = client
        .post(format!("http://127.0.0.1:{port}/v1/chat/completions"))
        .json(&Request {
            model,
            messages,
            stream: true,
        })
        .send()
        .map_err(|error| format!("chat request failed: {error}"))?;
    if !response.status().is_success() {
        return Err(http_error(&mut response));
    }
    let assistant = decode_sse(&mut response, |delta| {
        sender
            .send(Event::Delta(delta))
            .map_err(|_| "chat event receiver closed".to_string())
    })?;
    sender
        .send(Event::Complete(assistant))
        .map_err(|_| "chat event receiver closed".to_string())
}

fn http_error(response: &mut reqwest::blocking::Response) -> String {
    let status = response.status();
    let mut body = Vec::new();
    if let Err(error) = response
        .take((MAX_ERROR_BODY_BYTES + 1) as u64)
        .read_to_end(&mut body)
    {
        return format!("llama-server returned HTTP {status}: failed to read error body: {error}");
    }
    if body.len() > MAX_ERROR_BODY_BYTES {
        return format!(
            "llama-server returned HTTP {status}: error body exceeds {MAX_ERROR_BODY_BYTES} bytes"
        );
    }
    if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(&body) {
        return format!(
            "llama-server returned HTTP {status}: {}",
            envelope.error.message
        );
    }
    let body = String::from_utf8_lossy(&body);
    let body = body.trim();
    if body.is_empty() {
        format!("llama-server returned HTTP {status}")
    } else {
        format!("llama-server returned HTTP {status}: {body}")
    }
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ApiError,
}

fn decode_sse<R, F>(reader: R, emit: F) -> Result<String, String>
where
    R: Read,
    F: FnMut(String) -> Result<(), String>,
{
    decode_sse_with_limits(reader, MAX_EVENT_BYTES, MAX_ASSISTANT_BYTES, emit)
}

fn decode_sse_with_limits<R, F>(
    mut reader: R,
    event_limit: usize,
    assistant_limit: usize,
    mut emit: F,
) -> Result<String, String>
where
    R: Read,
    F: FnMut(String) -> Result<(), String>,
{
    let mut buffer = [0_u8; 4096];
    let mut line = Vec::new();
    let mut data = Vec::new();
    let mut event_bytes = 0_usize;
    let mut assistant = String::new();

    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            if !line.is_empty() {
                process_line(&line, &mut data, event_limit)?;
            }
            if dispatch(&data, &mut assistant, assistant_limit, &mut emit)? {
                return Ok(assistant);
            }
            return Err("chat stream ended before [DONE]".into());
        }
        for &byte in &buffer[..count] {
            event_bytes = event_bytes
                .checked_add(1)
                .ok_or_else(|| "chat SSE event is too large".to_string())?;
            if event_bytes > event_limit {
                return Err(format!(
                    "chat SSE event is too large (limit {event_limit} bytes)"
                ));
            }
            if byte != b'\n' {
                line.push(byte);
                continue;
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                if dispatch(&data, &mut assistant, assistant_limit, &mut emit)? {
                    return Ok(assistant);
                }
                data.clear();
                event_bytes = 0;
            } else {
                process_line(&line, &mut data, event_limit)?;
            }
            line.clear();
        }
    }
}

fn process_line(line: &[u8], data: &mut Vec<u8>, event_limit: usize) -> Result<(), String> {
    if line.starts_with(b":") {
        return Ok(());
    }
    let Some(value) = line.strip_prefix(b"data:") else {
        return Ok(());
    };
    let value = value.strip_prefix(b" ").unwrap_or(value);
    if !data.is_empty() {
        data.push(b'\n');
    }
    data.extend_from_slice(value);
    if data.len() > event_limit {
        return Err(format!(
            "chat SSE event is too large (limit {event_limit} bytes)"
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct StreamPayload {
    #[serde(default)]
    choices: Vec<Choice>,
    error: Option<ApiError>,
}

#[derive(Deserialize)]
struct Choice {
    delta: Delta,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
}

#[derive(Deserialize)]
struct ApiError {
    message: String,
}

fn dispatch<F>(
    data: &[u8],
    assistant: &mut String,
    assistant_limit: usize,
    emit: &mut F,
) -> Result<bool, String>
where
    F: FnMut(String) -> Result<(), String>,
{
    if data.is_empty() {
        return Ok(false);
    }
    if data == b"[DONE]" {
        return Ok(true);
    }
    let payload: StreamPayload =
        serde_json::from_slice(data).map_err(|error| format!("invalid chat SSE JSON: {error}"))?;
    if let Some(error) = payload.error {
        return Err(format!("llama-server chat error: {}", error.message));
    }
    if let Some(content) = payload
        .choices
        .into_iter()
        .next()
        .and_then(|choice| choice.delta.content)
        .filter(|content| !content.is_empty())
    {
        if assistant.len().saturating_add(content.len()) > assistant_limit {
            return Err(format!(
                "assistant response is too large (limit {assistant_limit} bytes)"
            ));
        }
        emit(content.clone())?;
        assistant.push_str(&content);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_sse, decode_sse_with_limits, Event, Message, Role, Worker, MAX_ERROR_BODY_BYTES,
        MAX_EVENT_BYTES,
    };
    use std::io::{self, Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    struct Fragmented {
        bytes: Vec<u8>,
        chunks: Vec<usize>,
        offset: usize,
        chunk: usize,
    }

    impl Fragmented {
        fn new(bytes: &[u8], chunks: &[usize]) -> Self {
            Self {
                bytes: bytes.to_vec(),
                chunks: chunks.to_vec(),
                offset: 0,
                chunk: 0,
            }
        }
    }

    impl Read for Fragmented {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.offset == self.bytes.len() {
                return Ok(0);
            }
            let requested = self.chunks[self.chunk % self.chunks.len()];
            self.chunk += 1;
            let count = requested
                .min(output.len())
                .min(self.bytes.len() - self.offset);
            output[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
            self.offset += count;
            Ok(count)
        }
    }

    #[test]
    fn decodes_fragmented_utf8_crlf_comments_and_repeated_data() {
        let body = concat!(
            ": ping\r\n\r\n",
            "data: {\"choices\":[{\"delta\":\r\n",
            "data: {\"content\":\"hé\"}}]}\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n",
            "data: [DONE]\r\n\r\n"
        );
        let mut deltas = Vec::new();

        let complete = decode_sse(
            Fragmented::new(body.as_bytes(), &[1, 2, 3, 1, 5]),
            |delta| {
                deltas.push(delta);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(deltas, ["hé", "llo"]);
        assert_eq!(complete, "héllo");
    }

    #[test]
    fn ignores_role_only_empty_and_usage_events() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":null}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut deltas = Vec::new();

        let complete = decode_sse(body.as_bytes(), |delta| {
            deltas.push(delta);
            Ok(())
        })
        .unwrap();

        assert!(deltas.is_empty());
        assert!(complete.is_empty());
    }

    #[test]
    fn rejects_malformed_json_server_error_and_eof_before_done() {
        let malformed = decode_sse(
            b"data: {not-json}\n\n".as_slice(),
            |_| -> Result<(), String> { Ok(()) },
        )
        .unwrap_err();
        assert!(malformed.contains("invalid chat SSE JSON"));

        let server_error = decode_sse(
            b"data: {\"error\":{\"code\":500,\"message\":\"generation failed\",\"type\":\"server_error\"}}\n\n"
                .as_slice(),
            |_| -> Result<(), String> { Ok(()) },
        )
        .unwrap_err();
        assert!(server_error.contains("generation failed"));

        let eof = decode_sse(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n".as_slice(),
            |_| -> Result<(), String> { Ok(()) },
        )
        .unwrap_err();
        assert_eq!(eof, "chat stream ended before [DONE]");
    }

    #[test]
    fn rejects_an_event_over_the_explicit_bound() {
        let body = vec![b'a'; MAX_EVENT_BYTES + 1];
        let error = decode_sse(body.as_slice(), |_| -> Result<(), String> { Ok(()) }).unwrap_err();
        assert!(error.contains("chat SSE event is too large"));
        assert!(error.contains(&MAX_EVENT_BYTES.to_string()));
    }

    #[test]
    fn accepts_done_at_eof_without_a_final_blank_line() {
        let complete = decode_sse(b"data: [DONE]".as_slice(), |_| -> Result<(), String> {
            Ok(())
        })
        .unwrap();
        assert!(complete.is_empty());
    }

    #[test]
    fn reads_only_the_first_choice_delta() {
        let body = concat!(
            "data: {\"choices\":[",
            "{\"delta\":{\"content\":\"first\"}},",
            "{\"delta\":{\"content\":\"ignored\"}}",
            "]}\n\n",
            "data: [DONE]\n\n"
        );
        let mut deltas = Vec::new();

        let complete = decode_sse(body.as_bytes(), |delta| {
            deltas.push(delta);
            Ok(())
        })
        .unwrap();

        assert_eq!(deltas, ["first"]);
        assert_eq!(complete, "first");
    }

    #[test]
    fn bounds_completed_assistant_before_emitting_overflowing_delta() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"abc\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"def\"}}]}\n\n",
            "data: [DONE]\n\n"
        );
        let mut deltas = Vec::new();

        let error = decode_sse_with_limits(body.as_bytes(), 256, 5, |delta| {
            deltas.push(delta);
            Ok(())
        })
        .unwrap_err();

        assert_eq!(deltas, ["abc"]);
        assert!(error.contains("assistant response is too large"));
        assert!(error.contains('5'));
    }

    #[test]
    fn posts_exact_history_and_streams_deltas_before_completion() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/event-stream\r\n",
            "Connection: close\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"!\"}}]}\n\n",
            "data: [DONE]\n\n"
        );
        let (port, request, server) = serve_once(response.as_bytes().to_vec());
        let worker = Worker::start(
            port,
            "tiny".into(),
            vec![
                Message {
                    role: Role::User,
                    content: "Hello".into(),
                },
                Message {
                    role: Role::Assistant,
                    content: "Earlier".into(),
                },
                Message {
                    role: Role::User,
                    content: "Again".into(),
                },
            ],
        )
        .unwrap();

        assert_eq!(
            worker.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Delta("Hi".into())
        );
        assert_eq!(
            worker.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Delta("!".into())
        );
        assert_eq!(
            worker.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Complete("Hi!".into())
        );
        worker.join().unwrap();

        let request = request.recv_timeout(Duration::from_secs(2)).unwrap();
        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        assert_eq!(
            body,
            "{\"model\":\"tiny\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"},{\"role\":\"assistant\",\"content\":\"Earlier\"},{\"role\":\"user\",\"content\":\"Again\"}],\"stream\":true}"
        );
        server.join().unwrap();
    }

    #[test]
    fn surfaces_non_success_openai_error_message() {
        let response = concat!(
            "HTTP/1.1 400 Bad Request\r\n",
            "Content-Type: application/json\r\n",
            "Content-Length: 61\r\n",
            "Connection: close\r\n\r\n",
            "{\"error\":{\"message\":\"model is unavailable\",\"type\":\"invalid\"}}"
        );
        let (port, _request, server) = serve_once(response.as_bytes().to_vec());
        let worker = Worker::start(port, "tiny".into(), Vec::new()).unwrap();

        let Event::Error(error) = worker.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("expected worker error");
        };
        assert!(error.contains("HTTP 400"));
        assert!(error.contains("model is unavailable"));
        worker.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn bounds_non_success_response_text() {
        let body = vec![b'x'; MAX_ERROR_BODY_BYTES + 1];
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body)
        .collect();
        let (port, _request, server) = serve_once(response);
        let worker = Worker::start(port, "tiny".into(), Vec::new()).unwrap();

        let Event::Error(error) = worker.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("expected worker error");
        };
        assert!(error.contains("HTTP 500"));
        assert!(error.contains("error body exceeds 16384 bytes"));
        worker.join().unwrap();
        server.join().unwrap();
    }

    fn serve_once(response: Vec<u8>) -> (u16, mpsc::Receiver<String>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (request_sender, request) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request_bytes = Vec::new();
            let mut buffer = [0_u8; 4096];
            let header_end = loop {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request_bytes.extend_from_slice(&buffer[..count]);
                if let Some(position) = request_bytes
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = std::str::from_utf8(&request_bytes[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while request_bytes.len() < header_end + content_length {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request_bytes.extend_from_slice(&buffer[..count]);
            }
            request_sender
                .send(String::from_utf8(request_bytes).unwrap())
                .unwrap();
            stream.write_all(&response).unwrap();
        });
        (port, request, server)
    }
}
