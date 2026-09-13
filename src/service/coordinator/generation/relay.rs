use super::super::state::AdmissionReservation;
use super::super::Coordinator;
use super::parser::SseDecoder;
use super::persistence::{OutputPipeline, SaveChunkFailure};
use super::preflight::QualifiedRequest;
use crate::history::{CommittedAdmission, ExecutionOutcome};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use loxa_ipc::{ErrorCategory, ServiceError};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

const MAX_HEADERS: usize = 64;
const MAX_HTTP_BUFFER: usize = 64 * 1024;
const DRIVER_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
const QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) async fn run(
    coordinator: Coordinator,
    reservation: Arc<AdmissionReservation>,
    committed: CommittedAdmission,
    qualified: QualifiedRequest,
) {
    let output = match coordinator.generation_output(&committed) {
        Ok(output) => output,
        Err(_) => {
            coordinator
                .shared
                .state()
                .require_generation_cleanup(&reservation);
            return;
        }
    };
    let mut pipeline = OutputPipeline::new(output, committed);
    let mut decoder = SseDecoder::new();
    let execution = {
        let state = coordinator.shared.state();
        state.begin_generation_execution(&reservation)
    };
    let engine = match execution {
        Ok(engine) => engine,
        Err(_) if reservation.is_cancelled() => {
            drop(qualified);
            pipeline
                .finish(&mut decoder, ExecutionOutcome::Stopped, Some("stopped"))
                .await;
            return;
        }
        Err(_) => {
            drop(qualified);
            coordinator
                .shared
                .state()
                .require_generation_cleanup(&reservation);
            pipeline
                .finish(
                    &mut decoder,
                    ExecutionOutcome::Failed,
                    Some("runtime_changed"),
                )
                .await;
            return;
        }
    };
    let expected_input_tokens = qualified.input_tokens();
    let qualified_template_sha256 = qualified.template_sha256();
    #[cfg(all(test, target_os = "macos"))]
    let observation_id = qualified.observation_id();
    #[cfg(all(test, target_os = "macos"))]
    {
        super::qualification_fixture::record_postcommit(observation_id, qualified_template_sha256);
        coordinator.wait_at_native_generation_execution_gate().await;
    }
    #[cfg(not(all(test, target_os = "macos")))]
    let _ = qualified_template_sha256;
    let stream = stream(
        &engine,
        &reservation,
        qualified.request.body,
        expected_input_tokens,
        #[cfg(all(test, target_os = "macos"))]
        observation_id,
        &mut decoder,
        &mut pipeline,
    )
    .await;
    #[cfg(all(test, target_os = "macos"))]
    super::qualification_fixture::record_usage(
        observation_id,
        decoder.prompt_tokens(),
        decoder.cached_prompt_tokens(),
    );
    let (mut outcome, mut failure_code) = match stream {
        Ok(StreamEnd::Completed) if !reservation.is_cancelled() => {
            (ExecutionOutcome::Completed, None)
        }
        Ok(StreamEnd::Completed | StreamEnd::Stopped) => {
            (ExecutionOutcome::Stopped, Some("stopped"))
        }
        Err(StreamFailure { code }) => (ExecutionOutcome::Failed, Some(code)),
    };
    if outcome == ExecutionOutcome::Completed {
        let idle = wait_for_idle(&engine, &reservation).await.is_ok();
        let quiescent = idle
            && coordinator
                .shared
                .state()
                .confirm_generation_quiescence(&reservation);
        if !quiescent {
            outcome = if reservation.is_cancelled() {
                ExecutionOutcome::Stopped
            } else {
                ExecutionOutcome::Failed
            };
            failure_code = Some(if outcome == ExecutionOutcome::Stopped {
                "stopped"
            } else {
                "engine_quiescence"
            });
        } else {
            #[cfg(all(test, target_os = "macos"))]
            super::qualification_fixture::record_quiescence(observation_id);
        }
    }
    if outcome != ExecutionOutcome::Completed {
        coordinator
            .shared
            .state()
            .require_generation_cleanup(&reservation);
    }
    let selected_outcome = pipeline.finish(&mut decoder, outcome, failure_code).await;
    if selected_outcome != ExecutionOutcome::Completed {
        coordinator
            .shared
            .state()
            .require_generation_cleanup(&reservation);
    }
}

enum StreamEnd {
    Completed,
    Stopped,
}

struct StreamFailure {
    code: &'static str,
}

async fn stream(
    engine: &super::super::state::EngineDescriptor,
    reservation: &AdmissionReservation,
    body: Bytes,
    expected_input_tokens: u32,
    #[cfg(all(test, target_os = "macos"))] observation_id: Option<u64>,
    decoder: &mut SseDecoder,
    pipeline: &mut OutputPipeline,
) -> Result<StreamEnd, StreamFailure> {
    let mut authenticated = None;
    let stream = tokio::select! {
        biased;
        () = reservation.wait_cancelled() => return Ok(StreamEnd::Stopped),
        result = crate::runner::service_transport::connect_authenticated(
            &engine.endpoint,
            engine.pid,
            &mut authenticated,
        ) => result.map_err(|_| failure("engine_transport"))?,
    }
    .ok_or_else(|| failure("engine_transport"))?;
    let io = TokioIo::new(stream);
    let mut builder = hyper::client::conn::http1::Builder::new();
    builder
        .max_headers(MAX_HEADERS)
        .max_buf_size(MAX_HTTP_BUFFER);
    let (mut sender, connection) = tokio::select! {
        biased;
        () = reservation.wait_cancelled() => return Ok(StreamEnd::Stopped),
        result = builder.handshake(io) => result.map_err(|_| failure("engine_transport"))?,
    };
    let request = Request::builder()
        .method(hyper::Method::POST)
        .uri("/v1/chat/completions")
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::ACCEPT, "text/event-stream")
        .header(hyper::header::CONNECTION, "close")
        .body(Full::new(body))
        .map_err(|_| failure("engine_request"))?;
    let mut driver = tokio::spawn(connection);
    let exchange = async {
        let mut response = tokio::select! {
            biased;
            () = reservation.wait_cancelled() => return Ok(StreamEnd::Stopped),
            result = async {
                #[cfg(all(test, target_os = "macos"))]
                super::qualification_fixture::record_dispatch(observation_id);
                sender.send_request(request).await
            } => {
                result.map_err(|_| failure("engine_transport"))?
            }
        };
        if response.status() != StatusCode::OK {
            return Err(failure("engine_rejected"));
        }
        while let Some(frame) = tokio::select! {
            biased;
            () = reservation.wait_cancelled() => return Ok(StreamEnd::Stopped),
            frame = response.body_mut().frame() => frame,
        } {
            let frame = frame.map_err(|_| failure("engine_transport"))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let mut offset = 0;
            loop {
                let step = decoder.push(&data[offset..]).map_err(|error| {
                    if error.contains("assistant output") {
                        failure("output_limit")
                    } else {
                        failure("engine_stream")
                    }
                })?;
                offset += step.consumed;
                let emitted = step.chunk.is_some();
                if let Some(chunk) = step.chunk {
                    match pipeline.save_chunk(chunk) {
                        Ok(()) => {}
                        Err(SaveChunkFailure::Cancelled) => return Ok(StreamEnd::Stopped),
                        Err(SaveChunkFailure::Failed) => {
                            return Err(failure("output_save"));
                        }
                    }
                }
                if step.done {
                    decoder.finish().map_err(|_| failure("engine_stream"))?;
                    if decoder.prompt_tokens() != Some(expected_input_tokens) {
                        return Err(failure("engine_usage"));
                    }
                    return Ok(StreamEnd::Completed);
                }
                if offset == data.len() && !emitted {
                    break;
                }
                if step.consumed == 0 && !emitted {
                    return Err(failure("engine_stream"));
                }
            }
        }
        decoder.finish().map_err(|_| failure("engine_stream"))?;
        Ok(StreamEnd::Completed)
    };
    let result = exchange.await;
    match tokio::time::timeout(DRIVER_JOIN_TIMEOUT, &mut driver).await {
        Ok(_) => {}
        Err(_) => {
            driver.abort();
            let _ = driver.await;
        }
    }
    result
}

async fn wait_for_idle(
    engine: &super::super::state::EngineDescriptor,
    reservation: &AdmissionReservation,
) -> Result<(), ServiceError> {
    tokio::time::timeout(QUIESCENCE_TIMEOUT, async {
        loop {
            let body = super::transport::bounded_json_request(
                engine,
                reservation,
                hyper::Method::GET,
                "/slots",
                Bytes::new(),
            )
            .await?;
            let slots: Vec<Slot> = serde_json::from_slice(&body).map_err(|_| {
                ServiceError::new(
                    ErrorCategory::ServiceUnavailable,
                    "engine slot response is invalid",
                )
            })?;
            if slots.len() == 1 && slots.iter().all(|slot| !slot.is_processing) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        ServiceError::new(
            ErrorCategory::ServiceUnavailable,
            "engine did not become quiescent after its terminal response",
        )
    })?
}

#[derive(Deserialize)]
struct Slot {
    is_processing: bool,
}

fn failure(code: &'static str) -> StreamFailure {
    StreamFailure { code }
}
