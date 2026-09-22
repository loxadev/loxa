use super::super::state::{AdmissionReservation, EngineDescriptor};
#[cfg(test)]
use crate::history::PromptMessage;
use crate::history::{PromptPreparation, PromptRole};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use loxa_ipc::{EffectiveSamplingSettings, ErrorCategory, SamplingValue, ServiceError};
use serde::Serialize;
use std::io::{self, Write};

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONTROL_BODY_BYTES: usize = 64 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_HTTP_BUFFER: usize = 64 * 1024;

pub(super) struct PreparedEngineRequest {
    pub(super) body: Bytes,
}

impl PreparedEngineRequest {
    pub(super) fn new(
        prompt: &PromptPreparation,
        sampling: EffectiveSamplingSettings,
    ) -> Result<Self, ServiceError> {
        let mut messages = Vec::with_capacity(
            prompt
                .messages
                .len()
                .saturating_add(usize::from(!prompt.system_instruction.is_empty())),
        );
        if !prompt.system_instruction.is_empty() {
            messages.push(WireMessage {
                role: "system",
                content: &prompt.system_instruction,
            });
        }
        for message in &prompt.messages {
            messages.push(WireMessage {
                role: match message.role {
                    PromptRole::User => "user",
                    PromptRole::Assistant => "assistant",
                },
                content: &message.content,
            });
        }
        let max_completion_tokens = u32::try_from(prompt.max_output_tokens).map_err(|_| {
            invalid("conversation output reservation exceeds the engine request range")
        })?;
        let request = WireRequest {
            model: &prompt.model_id,
            messages,
            max_completion_tokens,
            stream: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
            temperature: sampling.temperature,
            top_p: sampling.top_p,
        };
        let raw_capacity = prompt
            .messages
            .iter()
            .try_fold(prompt.system_instruction.len(), |total, message| {
                total.checked_add(message.content.len())
            })
            .and_then(|total| total.checked_add(4096))
            .ok_or_else(|| invalid("engine request backing size overflow"))?
            .min(MAX_REQUEST_BODY_BYTES);
        let mut writer = CappedVec::new(raw_capacity, MAX_REQUEST_BODY_BYTES);
        serde_json::to_writer(&mut writer, &request)
            .map_err(|_| invalid("engine request exceeds 64 MiB after JSON encoding"))?;
        Ok(Self {
            body: Bytes::from(writer.finish()),
        })
    }
}

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
    max_completion_tokens: u32,
    stream: bool,
    stream_options: StreamOptions,
    temperature: SamplingValue,
    top_p: SamplingValue,
}

#[derive(Serialize)]
struct WireMessage<'a> {
    role: &'static str,
    content: &'a str,
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

pub(super) async fn bounded_json_request(
    engine: &EngineDescriptor,
    reservation: &AdmissionReservation,
    method: Method,
    uri: &'static str,
    body: Bytes,
) -> Result<Vec<u8>, ServiceError> {
    let mut authenticated = None;
    let stream = tokio::select! {
        biased;
        () = reservation.wait_cancelled() => return Err(stopped()),
        result = crate::runner::service_transport::connect_authenticated(
            &engine.endpoint,
            engine.pid,
            &mut authenticated,
        ) => result.map_err(unavailable)?,
    }
    .ok_or_else(|| unavailable("exact engine endpoint is absent"))?;
    let io = TokioIo::new(stream);
    let mut builder = hyper::client::conn::http1::Builder::new();
    builder
        .max_headers(MAX_HEADERS)
        .max_buf_size(MAX_HTTP_BUFFER);
    let (mut sender, connection) = tokio::select! {
        biased;
        () = reservation.wait_cancelled() => return Err(stopped()),
        result = builder.handshake(io) => result.map_err(|_| unavailable("engine HTTP handshake failed"))?,
    };
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::CONNECTION, "close")
        .body(Full::new(body))
        .map_err(|_| invalid("engine request could not be constructed"))?;
    let exchange = async move {
        let mut response = sender
            .send_request(request)
            .await
            .map_err(|_| unavailable("engine request failed"))?;
        if response.status() != StatusCode::OK {
            return Err(unavailable("engine rejected the qualified request"));
        }
        if response
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_CONTROL_BODY_BYTES)
        {
            return Err(unavailable("engine control response exceeds 64 KiB"));
        }
        let mut bytes = Vec::with_capacity(MAX_CONTROL_BODY_BYTES);
        while let Some(frame) = response.body_mut().frame().await {
            let frame = frame.map_err(|_| unavailable("engine response body failed"))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            if bytes.len().saturating_add(data.len()) > MAX_CONTROL_BODY_BYTES {
                return Err(unavailable("engine control response exceeds 64 KiB"));
            }
            bytes.extend_from_slice(&data);
        }
        Ok(bytes)
    };
    tokio::select! {
        biased;
        () = reservation.wait_cancelled() => Err(stopped()),
        // Drop this one-request socket after HTTP completion. On macOS a peer
        // that has already closed can make a redundant write shutdown fail.
        joined = async { tokio::join!(connection.without_shutdown(), exchange) } => {
            let (driver, response) = joined;
            if driver.is_err() && response.is_ok() {
                return Err(unavailable("engine HTTP driver failed"));
            }
            response
        }
    }
}

struct CappedVec {
    bytes: Vec<u8>,
    maximum: usize,
    #[cfg(test)]
    growths: usize,
}

impl CappedVec {
    fn new(capacity: usize, maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.min(maximum)),
            maximum,
            #[cfg(test)]
            growths: 0,
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for CappedVec {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "JSON size overflow"))?;
        if required > self.maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bounded JSON output exceeded its limit",
            ));
        }
        if required > self.bytes.capacity() {
            let target = required
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.maximum);
            #[cfg(test)]
            let previous_capacity = self.bytes.capacity();
            self.bytes
                .try_reserve_exact(target - self.bytes.len())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::OutOfMemory, "JSON allocation failed")
                })?;
            if self.bytes.capacity() > self.maximum {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bounded JSON backing exceeded its limit",
                ));
            }
            #[cfg(test)]
            if self.bytes.capacity() > previous_capacity {
                self.growths += 1;
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn stopped() -> ServiceError {
    ServiceError::new(
        ErrorCategory::ServiceUnavailable,
        "generation was stopped during engine transport",
    )
}

fn invalid(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::InvalidRequest, context)
}

fn unavailable(context: impl Into<String>) -> ServiceError {
    ServiceError::new(ErrorCategory::ServiceUnavailable, context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{PromptBasis, PromptRole};

    #[test]
    fn request_encoding_is_bounded_and_preserves_message_order() {
        let prompt = PromptPreparation {
            model_id: "demo".into(),
            system_instruction: "system".into(),
            max_output_tokens: 512,
            temperature: None,
            top_p: None,
            basis: PromptBasis {
                references: Vec::new(),
            },
            messages: vec![
                PromptMessage {
                    role: PromptRole::User,
                    content: "hello".into(),
                },
                PromptMessage {
                    role: PromptRole::Assistant,
                    content: "hi".into(),
                },
            ],
        };
        let encoded = PreparedEngineRequest::new(
            &prompt,
            EffectiveSamplingSettings {
                temperature: SamplingValue::new(0.0).unwrap(),
                top_p: SamplingValue::new(0.95).unwrap(),
            },
        )
        .unwrap()
        .body;
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][1]["content"], "hello");
        assert_eq!(value["messages"][2]["role"], "assistant");
        assert_eq!(value["max_completion_tokens"], 512);
        assert_eq!(value["stream"], true);
        assert_eq!(value["temperature"], 0.0);
        assert_eq!(value["top_p"], 0.95);
    }

    #[test]
    fn json_writer_refuses_to_cross_the_encoded_limit() {
        let mut writer = CappedVec::new(5, 8);
        assert!(writer.write_all(b"abcde").is_ok());
        assert!(writer.write_all(b"f").is_ok());
        assert!(writer.bytes.capacity() <= 8);
        assert!(writer.write_all(b"ghi").is_err());
        assert!(writer.bytes.capacity() <= 8);
        assert_eq!(writer.finish(), b"abcdef");
    }

    #[test]
    fn json_writer_geometrically_bounds_escaped_fragment_growth() {
        const MAXIMUM: usize = 512 * 1024;

        let source = "\n".repeat(200 * 1024);
        let mut writer = CappedVec::new(64, MAXIMUM);
        serde_json::to_writer(&mut writer, &source).unwrap();

        assert_eq!(writer.bytes.len(), source.len() * 2 + 2);
        assert_eq!(
            serde_json::from_slice::<String>(&writer.bytes).unwrap(),
            source
        );
        assert!(writer.bytes.capacity() <= MAXIMUM);
        assert!(writer.growths <= 14, "growth count was {}", writer.growths);
    }
}
