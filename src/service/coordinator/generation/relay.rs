use super::super::state::AdmissionReservation;
use super::super::Coordinator;
use super::parser::{DecodeError, EngineFinishReason, SseDecoder};
use super::persistence::{OutputPipeline, SaveChunkFailure};
use super::preflight::QualifiedRequest;
use crate::history::{CommittedAdmission, ExecutionOutcome};
use crate::service::coordinator::history::AttemptMeasurements;
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
const MAX_CHECKPOINT_DELAY: Duration = Duration::from_millis(250);
const DRIVER_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
const QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(test)]
#[derive(Clone, Copy, Default)]
pub(in crate::service::coordinator) struct StreamProgress {
    pub(in crate::service::coordinator) decoded_end: usize,
    pub(in crate::service::coordinator) checkpoints: usize,
}

pub(super) async fn run(
    coordinator: Coordinator,
    reservation: Arc<AdmissionReservation>,
    committed: CommittedAdmission,
    qualified: QualifiedRequest,
    #[cfg(test)] progress: Option<tokio::sync::watch::Sender<StreamProgress>>,
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
    let expected_input_tokens = qualified.input_tokens();
    let mut measurements = AttemptMeasurements::new(expected_input_tokens);
    let execution = {
        let state = coordinator.shared.state();
        state.begin_generation_execution(&reservation)
    };
    let engine = match execution {
        Ok(engine) => engine,
        Err(_) if reservation.is_cancelled() => {
            drop(qualified);
            measurements.freeze_duration(reservation.accepted_elapsed());
            if pipeline
                .finish(
                    &mut decoder,
                    ExecutionOutcome::Stopped,
                    Some("stopped"),
                    measurements,
                )
                .await
                .is_err()
            {
                coordinator
                    .shared
                    .state()
                    .require_generation_cleanup(&reservation);
            }
            return;
        }
        Err(_) => {
            drop(qualified);
            measurements.freeze_duration(reservation.accepted_elapsed());
            coordinator
                .shared
                .state()
                .require_generation_cleanup(&reservation);
            let _ = pipeline
                .finish(
                    &mut decoder,
                    ExecutionOutcome::Failed,
                    Some("runtime_changed"),
                    measurements,
                )
                .await;
            return;
        }
    };
    #[cfg(all(test, target_os = "macos"))]
    let observation_id = qualified.observation_id();
    #[cfg(all(test, target_os = "macos"))]
    {
        super::qualification_fixture::record_postcommit(
            observation_id,
            qualified.template_sha256(),
        );
        coordinator.wait_at_native_generation_execution_gate().await;
    }
    let stream = stream(
        &engine,
        &reservation,
        qualified.request.body,
        expected_input_tokens,
        #[cfg(all(test, target_os = "macos"))]
        observation_id,
        #[cfg(all(test, target_os = "macos"))]
        coordinator.take_native_generation_output_gate(),
        #[cfg(test)]
        progress,
        &mut decoder,
        &mut pipeline,
        &mut measurements,
    )
    .await;
    measurements.freeze_duration(reservation.accepted_elapsed());
    #[cfg(all(test, target_os = "macos"))]
    super::qualification_fixture::record_usage(
        observation_id,
        decoder.prompt_tokens(),
        decoder.cached_prompt_tokens(),
    );
    if matches!(stream, Ok(StreamEnd::Completed)) {
        measurements.record_terminal_usage(
            decoder.completion_tokens(),
            decoder.engine_decode_tokens_per_second(),
            decoder.finish_reason() == Some(EngineFinishReason::OutputLimit),
        );
    }
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
    let selected_outcome = pipeline
        .finish(&mut decoder, outcome, failure_code, measurements)
        .await;
    if !matches!(selected_outcome, Ok(ExecutionOutcome::Completed)) {
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

#[cfg_attr(all(test, target_os = "macos"), allow(clippy::too_many_arguments))]
async fn stream(
    engine: &super::super::state::EngineDescriptor,
    reservation: &AdmissionReservation,
    body: Bytes,
    expected_input_tokens: u32,
    #[cfg(all(test, target_os = "macos"))] observation_id: Option<u64>,
    #[cfg(all(test, target_os = "macos"))] mut output_gate: Option<
        Arc<super::super::native_test_gate::NativeTestGate>,
    >,
    #[cfg(test)] progress: Option<tokio::sync::watch::Sender<StreamProgress>>,
    decoder: &mut SseDecoder,
    pipeline: &mut OutputPipeline,
    measurements: &mut AttemptMeasurements,
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
    #[cfg(all(test, target_os = "macos"))]
    let driver_observation_id = observation_id;
    #[cfg(all(test, target_os = "macos"))]
    let mut driver = tokio::spawn(async move {
        let result = connection.await;
        super::qualification_fixture::record_http_driver(driver_observation_id, result.is_err());
        result
    });
    #[cfg(not(all(test, target_os = "macos")))]
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
        #[cfg(all(test, target_os = "macos"))]
        super::qualification_fixture::record_response(observation_id, response.status().as_u16());
        if response.status() != StatusCode::OK {
            return Err(failure("engine_rejected"));
        }
        let mut checkpoint_deadline = None;
        loop {
            // Keep an overdue deadline while SQL owns the other suffix slot;
            // its acknowledgement wakes us without polling an expired timer.
            let checkpoint_pending = pipeline.checkpoint_pending();
            let frame = tokio::select! {
                biased;
                () = reservation.wait_cancelled() => return Ok(StreamEnd::Stopped),
                result = pipeline.wait_checkpoint() => {
                    result.map_err(|_| failure("output_save"))?;
                    continue;
                }
                () = async {
                    match checkpoint_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if !checkpoint_pending => {
                    checkpoint_deadline = None;
                    if let Some(chunk) = decoder.take_remaining_chunk() {
                        match pipeline.save_chunk(chunk) {
                            Ok(()) => {}
                            Err(SaveChunkFailure::Cancelled) => return Ok(StreamEnd::Stopped),
                            Err(SaveChunkFailure::Failed) => return Err(failure("output_save")),
                        }
                        #[cfg(test)]
                        if let Some(progress) = &progress {
                            progress.send_modify(|progress| progress.checkpoints += 1);
                        }
                    }
                    continue;
                }
                frame = response.body_mut().frame() => frame,
            };
            let Some(frame) = frame else {
                break;
            };
            let frame = frame.map_err(|_| failure("engine_transport"))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            #[cfg(all(test, target_os = "macos"))]
            super::qualification_fixture::record_data_frame(observation_id, data.len());
            let mut offset = 0;
            loop {
                let input = &data[offset..];
                #[cfg(all(test, target_os = "macos"))]
                let input = if output_gate.is_some() {
                    // A Hyper frame may contain content and DONE. Only this
                    // one-shot gate needs to observe the first event separately.
                    &input[..input.len().min(1)]
                } else {
                    input
                };
                let generated_before = decoder.generated_end();
                let step = decoder.push(input);
                if generated_before == 0 && decoder.generated_end() > 0 {
                    measurements.observe_first_output(reservation.accepted_elapsed());
                }
                let step = step.map_err(decode_failure)?;
                offset += step.consumed;
                if decoder.has_pending_chunk() {
                    checkpoint_deadline
                        .get_or_insert_with(|| tokio::time::Instant::now() + MAX_CHECKPOINT_DELAY);
                } else {
                    checkpoint_deadline = None;
                }
                #[cfg(test)]
                if let Some(progress) = &progress {
                    progress.send_modify(|progress| progress.decoded_end = decoder.generated_end());
                }
                #[cfg(all(test, target_os = "macos"))]
                super::qualification_fixture::record_parse_progress(
                    observation_id,
                    decoder.generated_end(),
                    decoder.prompt_tokens(),
                );
                #[cfg(all(test, target_os = "macos"))]
                if decoder.generated_end() > 0 && !step.done {
                    if let Some(gate) = output_gate.take() {
                        let prefix = step
                            .chunk
                            .as_deref()
                            .unwrap_or_else(|| decoder.pending_chunk());
                        super::qualification_fixture::record_output_gate(observation_id, prefix);
                        gate.pause().await;
                    }
                }
                let emitted = step.chunk.is_some();
                if let Some(chunk) = step.chunk {
                    match pipeline.save_chunk(chunk) {
                        Ok(()) => {}
                        Err(SaveChunkFailure::Cancelled) => return Ok(StreamEnd::Stopped),
                        Err(SaveChunkFailure::Failed) => {
                            return Err(failure("output_save"));
                        }
                    }
                    #[cfg(test)]
                    if let Some(progress) = &progress {
                        progress.send_modify(|progress| progress.checkpoints += 1);
                    }
                }
                if step.done {
                    decoder.finish().map_err(decode_failure)?;
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
        decoder.finish().map_err(decode_failure)?;
        if decoder.prompt_tokens() != Some(expected_input_tokens) {
            return Err(failure("engine_usage"));
        }
        Ok(StreamEnd::Completed)
    };
    let result = exchange.await;
    measurements.freeze_duration(reservation.accepted_elapsed());
    match tokio::time::timeout(DRIVER_JOIN_TIMEOUT, &mut driver).await {
        Ok(_) => {}
        Err(_) => {
            driver.abort();
            let _ = driver.await;
        }
    }
    result
}

#[cfg(test)]
pub(in crate::service::coordinator) async fn run_for_test(
    coordinator: Coordinator,
    reservation: Arc<AdmissionReservation>,
    committed: CommittedAdmission,
    progress: tokio::sync::watch::Sender<StreamProgress>,
) {
    run(
        coordinator,
        reservation,
        committed,
        QualifiedRequest::for_test(),
        Some(progress),
    )
    .await;
}

#[cfg(test)]
pub(in crate::service::coordinator) async fn stream_for_test(
    engine: &super::super::state::EngineDescriptor,
    reservation: &AdmissionReservation,
    decoder: &mut SseDecoder,
    pipeline: &mut OutputPipeline,
    progress: tokio::sync::watch::Sender<StreamProgress>,
) -> Result<ExecutionOutcome, &'static str> {
    let mut measurements = AttemptMeasurements::new(1);
    match stream(
        engine,
        reservation,
        Bytes::from_static(b"{}"),
        1,
        #[cfg(target_os = "macos")]
        None,
        #[cfg(target_os = "macos")]
        None,
        Some(progress),
        decoder,
        pipeline,
        &mut measurements,
    )
    .await
    {
        Ok(StreamEnd::Completed) => Ok(ExecutionOutcome::Completed),
        Ok(StreamEnd::Stopped) => Ok(ExecutionOutcome::Stopped),
        Err(error) => Err(error.code),
    }
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

fn decode_failure(error: DecodeError) -> StreamFailure {
    match error {
        DecodeError::OutputLimit => failure("output_limit"),
        DecodeError::EventLimit | DecodeError::InvalidStream(_) => failure("engine_stream"),
    }
}
