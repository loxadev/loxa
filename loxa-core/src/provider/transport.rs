use super::ProviderError;
use reqwest::blocking::{Client, Response};
use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamFraming {
    SseData,
    // Reserved by the shared seam for the Ollama adapter implemented in Task 2B.
    #[allow(dead_code)]
    JsonLines,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TimedJsonEvent {
    pub(crate) elapsed_ns: u64,
    pub(crate) value: Value,
}

impl TimedJsonEvent {
    pub(crate) fn new(elapsed_ns: u64, value: Value) -> Self {
        Self { elapsed_ns, value }
    }
}

pub(crate) trait JsonTransport {
    fn get_json(&mut self, url: &str) -> Result<Value, ProviderError>;
    // Reserved by the shared seam for provider inspection endpoints in Task 2B.
    #[allow(dead_code)]
    fn post_json(&mut self, url: &str, body: &Value) -> Result<Value, ProviderError>;
    fn post_json_stream(
        &mut self,
        url: &str,
        body: &Value,
        framing: StreamFraming,
    ) -> Result<Vec<TimedJsonEvent>, ProviderError>;
}

pub(crate) struct ReqwestJsonTransport {
    client: Option<Client>,
    initialization_error: Option<String>,
}

impl ReqwestJsonTransport {
    pub(crate) fn new() -> Self {
        match Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
        {
            Ok(client) => Self {
                client: Some(client),
                initialization_error: None,
            },
            Err(error) => Self {
                client: None,
                initialization_error: Some(error.to_string()),
            },
        }
    }

    fn client(&self) -> Result<&Client, ProviderError> {
        self.client.as_ref().ok_or_else(|| {
            ProviderError::Io(format!(
                "failed to initialize provider HTTP client: {}",
                self.initialization_error
                    .as_deref()
                    .unwrap_or("unknown initialization failure")
            ))
        })
    }

    fn response_json(response: Response) -> Result<Value, ProviderError> {
        validate_success_status(response.status())?;
        let bytes = response.bytes().map_err(map_reqwest_error)?;
        parse_json_bytes(&bytes)
    }
}

impl JsonTransport for ReqwestJsonTransport {
    fn get_json(&mut self, url: &str) -> Result<Value, ProviderError> {
        let response = self.client()?.get(url).send().map_err(map_reqwest_error)?;
        Self::response_json(response)
    }

    fn post_json(&mut self, url: &str, body: &Value) -> Result<Value, ProviderError> {
        let bytes = serde_json::to_vec(body).map_err(|error| {
            ProviderError::Protocol(format!("failed to encode provider request JSON: {error}"))
        })?;
        let response = self
            .client()?
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(bytes)
            .send()
            .map_err(map_reqwest_error)?;
        Self::response_json(response)
    }

    fn post_json_stream(
        &mut self,
        url: &str,
        body: &Value,
        framing: StreamFraming,
    ) -> Result<Vec<TimedJsonEvent>, ProviderError> {
        let bytes = serde_json::to_vec(body).map_err(|error| {
            ProviderError::Protocol(format!("failed to encode provider request JSON: {error}"))
        })?;
        let started = Instant::now();
        let response = self
            .client()?
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(bytes)
            .send()
            .map_err(map_reqwest_error)?;
        validate_success_status(response.status())?;
        let reader = BufReader::new(response);

        match framing {
            StreamFraming::SseData => parse_sse_stream_started(reader, started),
            StreamFraming::JsonLines => parse_json_lines_stream(reader, started),
        }
    }
}

pub(crate) fn validate_success_status(status: StatusCode) -> Result<(), ProviderError> {
    if status.is_redirection() {
        return Err(ProviderError::Protocol(format!(
            "provider redirect status {status} is forbidden"
        )));
    }
    if !status.is_success() {
        return Err(ProviderError::Protocol(format!(
            "provider returned non-success HTTP status {status}"
        )));
    }
    Ok(())
}

pub(crate) fn parse_json_bytes(bytes: &[u8]) -> Result<Value, ProviderError> {
    serde_json::from_slice(bytes).map_err(|error| {
        ProviderError::Protocol(format!("provider returned malformed JSON: {error}"))
    })
}

#[cfg(test)]
pub(crate) fn parse_sse_stream<R: BufRead>(
    reader: R,
) -> Result<Vec<TimedJsonEvent>, ProviderError> {
    parse_sse_stream_started(reader, Instant::now())
}

fn parse_sse_stream_started<R: BufRead>(
    mut reader: R,
    started: Instant,
) -> Result<Vec<TimedJsonEvent>, ProviderError> {
    let mut events = Vec::new();
    let mut data_lines = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).map_err(map_io_error)?;
        if bytes_read == 0 {
            return Err(ProviderError::Protocol(
                "llama SSE stream ended before [DONE]".into(),
            ));
        }

        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            if data_lines.is_empty() {
                continue;
            }
            let data = data_lines.join("\n");
            data_lines.clear();
            if data.trim() == "[DONE]" {
                return Ok(events);
            }
            let value = parse_json_bytes(data.as_bytes())?;
            events.push(TimedJsonEvent::new(elapsed_ns(started), value));
            continue;
        }

        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.strip_prefix(' ').unwrap_or(data).to_string());
        }
    }
}

fn parse_json_lines_stream<R: BufRead>(
    mut reader: R,
    started: Instant,
) -> Result<Vec<TimedJsonEvent>, ProviderError> {
    let mut events = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).map_err(map_io_error)?;
        if bytes_read == 0 {
            return Err(ProviderError::Protocol(
                "JSON-lines stream ended before done: true".into(),
            ));
        }
        if line.trim().is_empty() {
            continue;
        }
        let value = parse_json_bytes(line.as_bytes())?;
        let done = value.get("done").and_then(Value::as_bool) == Some(true);
        events.push(TimedJsonEvent::new(elapsed_ns(started), value));
        if done {
            return Ok(events);
        }
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn map_reqwest_error(error: reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        ProviderError::Timeout
    } else if error.is_connect() {
        ProviderError::Unavailable
    } else {
        ProviderError::Io(format!("provider HTTP request failed: {error}"))
    }
}

fn map_io_error(error: std::io::Error) -> ProviderError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        ProviderError::Timeout
    } else {
        ProviderError::Io(format!("provider response read failed: {error}"))
    }
}
