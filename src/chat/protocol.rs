use serde::{Deserialize, Serialize};
use std::io::Read;

pub(super) const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_ASSISTANT_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_ERROR_BODY_BYTES: usize = 16 * 1024;

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

#[derive(Debug, PartialEq)]
pub enum Event {
    PromptProgress(PromptProgress),
    Delta(String),
    Timing(Timing),
    Complete(String),
    Error(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PromptProgress {
    pub total: i32,
    pub cache: i32,
    pub processed: i32,
    pub time_ms: i64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Timing {
    #[serde(default)]
    pub cache_n: i32,
    #[serde(default)]
    pub prompt_n: i32,
    #[serde(default)]
    pub prompt_ms: f64,
    #[serde(default)]
    pub predicted_n: i32,
    #[serde(default)]
    pub predicted_ms: f64,
}

#[derive(Serialize)]
pub(super) struct Request<'a> {
    model: &'a str,
    messages: &'a [Message],
    stream: bool,
    max_tokens: u32,
    cache_prompt: bool,
    return_progress: bool,
    timings_per_token: bool,
    stream_options: StreamOptions,
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

impl<'a> Request<'a> {
    pub(super) fn new(model: &'a str, messages: &'a [Message], max_tokens: u32) -> Self {
        Self {
            model,
            messages,
            stream: true,
            max_tokens,
            cache_prompt: true,
            return_progress: true,
            timings_per_token: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
        }
    }
}

pub(super) fn http_error(response: &mut reqwest::blocking::Response) -> String {
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

pub(super) fn decode_sse<R, F>(reader: R, emit: F) -> Result<String, String>
where
    R: Read,
    F: FnMut(Event) -> Result<(), String>,
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
    F: FnMut(Event) -> Result<(), String>,
{
    let mut buffer = [0_u8; 4096];
    let mut decoder = SseDecoder::with_limits(event_limit, assistant_limit);

    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return decoder.finish(&mut emit);
        }
        if decoder.push(&buffer[..count], &mut emit)? {
            return decoder.finish(&mut emit);
        }
    }
}

pub(super) struct SseDecoder {
    line: Vec<u8>,
    data: Vec<u8>,
    data_seen: bool,
    event_bytes: usize,
    assistant: String,
    event_limit: usize,
    assistant_limit: usize,
    done: bool,
}

impl SseDecoder {
    pub(super) fn new() -> Self {
        Self::with_limits(MAX_EVENT_BYTES, MAX_ASSISTANT_BYTES)
    }
    fn with_limits(event_limit: usize, assistant_limit: usize) -> Self {
        Self {
            line: Vec::new(),
            data: Vec::new(),
            data_seen: false,
            event_bytes: 0,
            assistant: String::new(),
            event_limit,
            assistant_limit,
            done: false,
        }
    }
    pub(super) fn push<F>(&mut self, bytes: &[u8], emit: &mut F) -> Result<bool, String>
    where
        F: FnMut(Event) -> Result<(), String>,
    {
        if self.done {
            return Ok(true);
        }
        for &byte in bytes {
            self.event_bytes = self
                .event_bytes
                .checked_add(1)
                .ok_or_else(|| "chat SSE event is too large".to_string())?;
            if self.event_bytes > self.event_limit {
                return Err(format!(
                    "chat SSE event is too large (limit {} bytes)",
                    self.event_limit
                ));
            }
            if byte != b'\n' {
                self.line.push(byte);
                continue;
            }
            if self.line.last() == Some(&b'\r') {
                self.line.pop();
            }
            if self.line.is_empty() {
                self.done = dispatch(&self.data, &mut self.assistant, self.assistant_limit, emit)?;
                self.data.clear();
                self.data_seen = false;
                self.event_bytes = 0;
                if self.done {
                    return Ok(true);
                }
            } else {
                process_line(
                    &self.line,
                    &mut self.data,
                    &mut self.data_seen,
                    self.event_limit,
                )?;
            }
            self.line.clear();
        }
        Ok(self.done)
    }
    pub(super) fn finish<F>(mut self, emit: &mut F) -> Result<String, String>
    where
        F: FnMut(Event) -> Result<(), String>,
    {
        if !self.done {
            if !self.line.is_empty() {
                process_line(
                    &self.line,
                    &mut self.data,
                    &mut self.data_seen,
                    self.event_limit,
                )?;
            }
            self.done = dispatch(&self.data, &mut self.assistant, self.assistant_limit, emit)?;
        }
        if self.done {
            Ok(self.assistant)
        } else {
            Err("chat stream ended before [DONE]".into())
        }
    }
}

fn process_line(
    line: &[u8],
    data: &mut Vec<u8>,
    data_seen: &mut bool,
    event_limit: usize,
) -> Result<(), String> {
    if line.starts_with(b":") {
        return Ok(());
    }
    let Some(value) = line.strip_prefix(b"data:") else {
        return Ok(());
    };
    let value = value.strip_prefix(b" ").unwrap_or(value);
    if *data_seen {
        data.push(b'\n');
    }
    *data_seen = true;
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
    #[serde(default)]
    prompt_progress: Option<PromptProgress>,
    #[serde(default)]
    timings: Option<Timing>,
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
struct ErrorEnvelope {
    error: ApiError,
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
    F: FnMut(Event) -> Result<(), String>,
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
    if let Some(progress) = payload.prompt_progress {
        emit(Event::PromptProgress(progress))?;
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
        emit(Event::Delta(content.clone()))?;
        assistant.push_str(&content);
    }
    if let Some(timing) = payload.timings {
        emit(Event::Timing(timing))?;
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{decode_sse, decode_sse_with_limits, Event, MAX_EVENT_BYTES};
    use std::io::{self, Read};

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

    fn collect_delta(deltas: &mut Vec<String>, event: Event) -> Result<(), String> {
        if let Event::Delta(delta) = event {
            deltas.push(delta);
        }
        Ok(())
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
            |event| collect_delta(&mut deltas, event),
        )
        .unwrap();

        assert_eq!(deltas, ["hé", "llo"]);
        assert_eq!(complete, "héllo");
    }

    #[test]
    fn decodes_prompt_progress_content_final_timing_and_done() {
        let body = concat!(
            "data: {\"prompt_progress\":{\"total\":3,\"cache\":1,\"processed\":2,\"time_ms\":9}}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n",
            "data: {\"timings\":{\"cache_n\":1,\"prompt_n\":3,\"prompt_ms\":9.0,\"predicted_n\":2,\"predicted_ms\":4.0}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut events = Vec::new();

        let complete = decode_sse(body.as_bytes(), |event| {
            events.push(event);
            Ok(())
        })
        .unwrap();

        assert_eq!(complete, "OK");
        assert_eq!(events.len(), 3);
        let Event::PromptProgress(progress) = &events[0] else {
            panic!("expected prompt progress");
        };
        assert_eq!(
            (progress.total, progress.cache, progress.processed),
            (3, 1, 2)
        );
        assert_eq!(events[1], Event::Delta("OK".into()));
        let Event::Timing(timing) = &events[2] else {
            panic!("expected timing");
        };
        assert_eq!(
            (timing.cache_n, timing.prompt_n, timing.predicted_n),
            (1, 3, 2)
        );
    }

    #[test]
    fn accepts_b10121_signed_token_counters() {
        let body = concat!(
            "data: {\"timings\":{\"cache_n\":-1,\"prompt_n\":3,\"prompt_ms\":9.0,\"predicted_n\":1,\"predicted_ms\":4.0}}\n\n",
            "data: [DONE]\n\n"
        );
        let mut events = Vec::new();

        let result = decode_sse(body.as_bytes(), |event| {
            events.push(event);
            Ok(())
        });

        assert!(result.is_ok(), "{result:?}");
        let [Event::Timing(timing)] = events.as_slice() else {
            panic!("expected one timing event");
        };
        assert_eq!(
            (timing.cache_n, timing.prompt_n, timing.predicted_n),
            (-1, 3, 1)
        );
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

        let complete =
            decode_sse(body.as_bytes(), |event| collect_delta(&mut deltas, event)).unwrap();

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

        let complete =
            decode_sse(body.as_bytes(), |event| collect_delta(&mut deltas, event)).unwrap();

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

        let error = decode_sse_with_limits(body.as_bytes(), 256, 5, |event| {
            collect_delta(&mut deltas, event)
        })
        .unwrap_err();

        assert_eq!(deltas, ["abc"]);
        assert!(error.contains("assistant response is too large"));
        assert!(error.contains('5'));
    }

    #[test]
    fn preserves_empty_repeated_data_fields() {
        let error = decode_sse(
            b"data:\ndata: [DONE]\n\n".as_slice(),
            |_| -> Result<(), String> { Ok(()) },
        )
        .unwrap_err();

        assert!(error.contains("invalid chat SSE JSON"));
    }

    #[test]
    fn incremental_decoder_emits_delta_before_eof() {
        let mut decoder = super::SseDecoder::new();
        let mut events = Vec::new();
        assert!(!decoder
            .push(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
                &mut |event| {
                    events.push(event);
                    Ok(())
                }
            )
            .unwrap());
        assert!(matches!(events.as_slice(), [super::Event::Delta(value)] if value == "Hi"));
        assert!(decoder.push(b"data: [DONE]\n\n", &mut |_| Ok(())).unwrap());
        assert_eq!(decoder.finish(&mut |_| Ok(())).unwrap(), "Hi");
    }

    #[test]
    fn accepts_done_at_eof_after_a_trailing_newline() {
        assert_eq!(
            decode_sse(b"data: [DONE]\n".as_slice(), |_| Ok(())).unwrap(),
            ""
        );
    }
}
