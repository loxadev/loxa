mod protocol;

pub use protocol::{Event, Message, PromptProgress, Role, Timing};

use loxa_ipc::{OperationTarget, ServiceClient};
use protocol::{decode_sse, http_error, Request};
use std::sync::mpsc;
use std::time::Duration;

const EVENT_QUEUE_CAPACITY: usize = 64;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) fn service_request_json(
    model: &str,
    messages: &[Message],
    max_tokens: u32,
) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&Request::new(model, messages, max_tokens))
        .map_err(|error| error.to_string())
}

pub struct Worker {
    events: mpsc::Receiver<Event>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    pub fn start(
        port: u16,
        model: String,
        messages: Vec<Message>,
        max_tokens: u32,
    ) -> Result<Self, String> {
        Self::start_with_request_timeout(port, model, messages, max_tokens, REQUEST_TIMEOUT)
    }

    pub(crate) fn start_service(
        client: ServiceClient,
        target: OperationTarget,
        model: String,
        messages: Vec<Message>,
        max_tokens: u32,
    ) -> Result<Self, String> {
        let (sender, events) = mpsc::sync_channel(EVENT_QUEUE_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("loxa-service-chat-request".into())
            .spawn(move || {
                let result = (|| {
                    let body = service_request_json(&model, &messages, max_tokens)?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| error.to_string())?;
                    let mut decoder = protocol::SseDecoder::new();
                    let mut emit = |event| {
                        sender
                            .send(event)
                            .map_err(|_| "chat event receiver closed".to_string())
                    };
                    runtime.block_on(crate::service::attachment::chat(
                        &client,
                        &target,
                        &model,
                        body,
                        |data| decoder.push(data, &mut emit),
                    ))?;
                    decoder.finish(&mut emit)
                })();
                match result {
                    Ok(assistant) => {
                        let _ = sender.send(Event::Complete(assistant));
                    }
                    Err(error) => {
                        let _ = sender.send(Event::Error(error));
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            events,
            thread: Some(thread),
        })
    }

    fn start_with_request_timeout(
        port: u16,
        model: String,
        messages: Vec<Message>,
        max_tokens: u32,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        let (sender, events) = mpsc::sync_channel(EVENT_QUEUE_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("loxa-chat-request".into())
            .spawn(move || {
                if let Err(error) = request(
                    port,
                    &model,
                    &messages,
                    max_tokens,
                    request_timeout,
                    &sender,
                ) {
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

    pub fn join(self) -> Result<(), String> {
        let Self { events, thread } = self;
        drop(events);
        thread
            .expect("chat worker thread is present")
            .join()
            .map_err(|_| "chat request worker panicked".to_string())
    }

    /// Detach only when an attached runtime loses identity mid-request.
    /// The request thread is not cancelled, but remains bounded by `REQUEST_TIMEOUT`.
    pub(crate) fn detach_bounded(self) {
        let Self { events, thread } = self;
        drop(events);
        drop(thread);
    }

    #[cfg(test)]
    pub(crate) fn for_session_test(events_to_send: Vec<Event>, panic_on_join: bool) -> Self {
        let (sender, events) = mpsc::sync_channel(EVENT_QUEUE_CAPACITY);
        let thread = std::thread::spawn(move || {
            for event in events_to_send {
                if sender.send(event).is_err() {
                    return;
                }
            }
            assert!(!panic_on_join, "injected chat worker panic");
        });
        Self {
            events,
            thread: Some(thread),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_session_test_thread(work: impl FnOnce() + Send + 'static) -> Self {
        let (sender, events) = mpsc::sync_channel(EVENT_QUEUE_CAPACITY);
        let thread = std::thread::spawn(move || {
            work();
            drop(sender);
        });
        Self {
            events,
            thread: Some(thread),
        }
    }
}

fn request(
    port: u16,
    model: &str,
    messages: &[Message],
    max_tokens: u32,
    request_timeout: Duration,
    sender: &mpsc::SyncSender<Event>,
) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(request_timeout)
        .build()
        .map_err(|error| error.to_string())?;
    let mut response = client
        .post(format!("http://127.0.0.1:{port}/v1/chat/completions"))
        .json(&Request::new(model, messages, max_tokens))
        .send()
        .map_err(|error| {
            if error.is_timeout() {
                format!(
                    "chat request timed out after {} ms",
                    request_timeout.as_millis()
                )
            } else {
                format!("chat request failed: {error}")
            }
        })?;
    if !response.status().is_success() {
        return Err(http_error(&mut response));
    }
    let assistant = decode_sse(&mut response, |event| {
        sender
            .send(event)
            .map_err(|_| "chat event receiver closed".to_string())
    })?;
    sender
        .send(Event::Complete(assistant))
        .map_err(|_| "chat event receiver closed".to_string())
}

#[cfg(test)]
mod tests {
    use super::protocol::MAX_ERROR_BODY_BYTES;
    use super::{Event, Message, Role, Worker};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

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
            512,
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
            "{\"model\":\"tiny\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"},{\"role\":\"assistant\",\"content\":\"Earlier\"},{\"role\":\"user\",\"content\":\"Again\"}],\"stream\":true,\"max_tokens\":512,\"cache_prompt\":true,\"return_progress\":true,\"timings_per_token\":true,\"stream_options\":{\"include_usage\":true}}"
        );
        server.join().unwrap();
    }

    #[test]
    fn posts_bounded_cached_request_and_surfaces_progress_and_timing() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/event-stream\r\n",
            "Connection: close\r\n\r\n",
            "data: {\"prompt_progress\":{\"total\":3,\"cache\":1,\"processed\":2,\"time_ms\":9}}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n",
            "data: {\"timings\":{\"cache_n\":1,\"prompt_n\":3,\"prompt_ms\":9.0,\"predicted_n\":2,\"predicted_ms\":4.0}}\n\n",
            "data: [DONE]\n\n"
        );
        let (port, request, server) = serve_once(response.as_bytes().to_vec());
        let worker = Worker::start(
            port,
            "tiny".into(),
            vec![Message {
                role: Role::User,
                content: "Hello".into(),
            }],
            7,
        )
        .unwrap();

        let Event::PromptProgress(progress) = worker.recv_timeout(Duration::from_secs(2)).unwrap()
        else {
            panic!("expected prompt progress");
        };
        assert_eq!(
            (
                progress.total,
                progress.cache,
                progress.processed,
                progress.time_ms
            ),
            (3, 1, 2, 9)
        );
        assert_eq!(
            worker.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Delta("OK".into())
        );
        let Event::Timing(timing) = worker.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("expected final timing");
        };
        assert_eq!(
            (timing.cache_n, timing.prompt_n, timing.predicted_n),
            (1, 3, 2)
        );
        assert_eq!(
            worker.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Complete("OK".into())
        );
        worker.join().unwrap();

        let request = request.recv_timeout(Duration::from_secs(2)).unwrap();
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        assert_eq!(
            body,
            "{\"model\":\"tiny\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}],\"stream\":true,\"max_tokens\":7,\"cache_prompt\":true,\"return_progress\":true,\"timings_per_token\":true,\"stream_options\":{\"include_usage\":true}}"
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
        let worker = Worker::start(port, "tiny".into(), Vec::new(), 512).unwrap();

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
        let worker = Worker::start(port, "tiny".into(), Vec::new(), 512).unwrap();

        let Event::Error(error) = worker.recv_timeout(Duration::from_secs(2)).unwrap() else {
            panic!("expected worker error");
        };
        assert!(error.contains("HTTP 500"));
        assert!(error.contains("error body exceeds 16384 bytes"));
        worker.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn join_drops_a_full_event_queue_before_waiting_for_the_worker() {
        let mut response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/event-stream\r\n",
            "Connection: close\r\n\r\n"
        )
        .as_bytes()
        .to_vec();
        for _ in 0..70 {
            response
                .extend_from_slice(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
        }
        response.extend_from_slice(b"data: [DONE]\n\n");
        let (port, _request, server) = serve_once(response);
        let worker = Worker::start(port, "tiny".into(), Vec::new(), 512).unwrap();
        let (done_sender, done) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = done_sender.send(worker.join());
        });

        assert_eq!(
            done.recv_timeout(Duration::from_millis(500)).unwrap(),
            Ok(())
        );
        server.join().unwrap();
    }

    #[test]
    fn accepted_but_stalled_headers_hit_the_request_timeout() {
        let (port, accepted, release, server) = serve_stalled_headers();
        let worker = Worker::start_with_request_timeout(
            port,
            "tiny".into(),
            Vec::new(),
            512,
            Duration::from_millis(50),
        )
        .unwrap();
        accepted.recv_timeout(Duration::from_secs(2)).unwrap();

        let event = worker.recv_timeout(Duration::from_millis(500));
        let _ = release.send(());
        server.join().unwrap();
        worker.join().unwrap();

        let Event::Error(error) = event.unwrap() else {
            panic!("expected worker error");
        };
        assert!(error.to_ascii_lowercase().contains("timed out"), "{error}");
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

    fn serve_stalled_headers() -> (
        u16,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (accepted_sender, accepted) = mpsc::channel();
        let (release, release_receiver) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|part| part == b"\r\n\r\n") {
                    break;
                }
            }
            accepted_sender.send(()).unwrap();
            let _ = release_receiver.recv();
        });
        (port, accepted, release, server)
    }
}
