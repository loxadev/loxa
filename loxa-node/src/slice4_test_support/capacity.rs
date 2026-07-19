use crate::artifact_coordinator::{ArtifactKey, ArtifactMutationCoordinator};
use crate::control_state::state_machine::{AdmissionRequest, InstancePublication};
use crate::download_scheduler::{
    BoundDownload, DownloadContinuation, DownloadExecutor, DownloadKey, DownloadReserveOutcome,
    DownloadSchedulerOwner, DownloadSubmitOutcome, DownloadWorkerPermit,
};
use crate::lifecycle_controller::{LifecycleCommand, LifecycleControllerOwner};
use crate::model_lifecycle::{
    CandidateSlot, EngineLifecycleDriver, ExactStopFailure, GatewayPublisher, LaunchPlan,
    LifecycleError, LifecycleSignals, ModelLifecycle, SessionCorrelation, StableNodeOwner,
    StartedSession,
};
use crate::operation_cancellation::OperationCancellation;
use crate::verification_scheduler::{
    CompletionWaitOutcome, DownloadCompletionQueue, VerificationClass, VerificationKey,
    VerificationReserveOutcome, VerificationResult, VerificationSchedulerOwner,
};
use crate::{open_slice3_control_state_fixture, NodePaths};
use loxa_core::model_inventory::StableVerificationInput;
use loxa_core::supervisor::{ManagedRun, RunLifecycle, RUNTIME_STATE_SCHEMA_VERSION};
use loxa_protocol::v2::{DecimalU64, OperationId, V2NodeCapabilities};
use loxa_protocol::{NodeId, NodeInstanceId};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const SAMPLE_DEADLINE: Duration = Duration::from_secs(120);
const MATRIX_DEADLINE: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Copy, Debug)]
struct CapacityMatrix {
    warmups: usize,
    measured_runs: usize,
    artifact_sizes: [u64; 2],
    worker_counts: [usize; 2],
}

#[derive(Clone, Copy, Debug)]
struct ShutdownStages {
    download_us: u128,
    verification_us: u128,
    lifecycle_us: u128,
    control_writer_us: u128,
}

impl ShutdownStages {
    fn total_us(self) -> u128 {
        self.download_us + self.verification_us + self.lifecycle_us + self.control_writer_us
    }
}

#[allow(dead_code)] // Every field is intentionally emitted through the manual Debug evidence report.
#[derive(Debug)]
struct CapacityCell {
    artifact_size: u64,
    worker_count: usize,
    transfer_bytes_per_second: Vec<u64>,
    verification_bytes_per_second: Vec<u64>,
    lifecycle_request_to_admission_us: Vec<u128>,
    lifecycle_admission_to_controller_start_us: Vec<u128>,
    shutdown_stages: Vec<ShutdownStages>,
    transfer_p50_bytes_per_second: u64,
    transfer_p95_bytes_per_second: u64,
    verification_p50_bytes_per_second: u64,
    verification_p95_bytes_per_second: u64,
    lifecycle_request_to_admission_p50_us: u128,
    lifecycle_request_to_admission_p95_us: u128,
    lifecycle_admission_to_controller_start_p50_us: u128,
    lifecycle_admission_to_controller_start_p95_us: u128,
    shutdown_total_p50_us: u128,
    shutdown_total_p95_us: u128,
}

#[allow(dead_code)] // Every field is intentionally emitted through the manual Debug evidence report.
#[derive(Debug)]
struct CapacityReport {
    host: String,
    os: String,
    architecture: String,
    filesystem: String,
    fixture_policy: &'static str,
    sync_policy: &'static str,
    measurement_limitations: &'static str,
    logical_fixture_bytes: [u64; 2],
    physical_fixture_bytes: [[u64; 2]; 2],
    warmups: usize,
    measured_runs: usize,
    cells: Vec<CapacityCell>,
}

impl CapacityReport {
    fn assert_approved_thresholds(&self) {
        for artifact_size in self.logical_fixture_bytes {
            let one = self
                .cells
                .iter()
                .find(|cell| cell.artifact_size == artifact_size && cell.worker_count == 1)
                .expect("one-worker capacity cell");
            let two = self
                .cells
                .iter()
                .find(|cell| cell.artifact_size == artifact_size && cell.worker_count == 2)
                .expect("two-worker capacity cell");
            assert!(
                u128::from(two.transfer_p50_bytes_per_second) * 100
                    >= u128::from(one.transfer_p50_bytes_per_second) * 80,
                "two-worker transfer throughput fell below 80% for {artifact_size} bytes: one={} two={}",
                one.transfer_p50_bytes_per_second,
                two.transfer_p50_bytes_per_second
            );
            assert!(
                u128::from(two.verification_p50_bytes_per_second) * 100
                    >= u128::from(one.verification_p50_bytes_per_second) * 80,
                "two-worker verification throughput fell below 80% for {artifact_size} bytes: one={} two={}",
                one.verification_p50_bytes_per_second,
                two.verification_p50_bytes_per_second
            );
            let allowed_increase = (one.shutdown_total_p95_us / 4).min(500_000);
            assert!(
                two.shutdown_total_p95_us
                    <= one.shutdown_total_p95_us.saturating_add(allowed_increase),
                "two-worker p95 shutdown exceeded the earlier of 25% or 500ms for {artifact_size} bytes: one={}us two={}us allowed_increase={}us",
                one.shutdown_total_p95_us,
                two.shutdown_total_p95_us,
                allowed_increase
            );
        }
    }
}

struct MatrixRoot(PathBuf);

impl MatrixRoot {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "loxa-capacity-{}-{}",
            std::process::id(),
            OperationId::new_v4()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for MatrixRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct TransferTask {
    source: PathBuf,
    destination: PathBuf,
}

struct CapacityTransferExecutor {
    tasks: Arc<Mutex<HashMap<OperationId, TransferTask>>>,
    completed: mpsc::Sender<(OperationId, io::Result<()>)>,
}

impl DownloadExecutor for CapacityTransferExecutor {
    fn execute(&self, bound: BoundDownload, permit: DownloadWorkerPermit) {
        let operation_id = bound.operation_id();
        let task = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&operation_id);
        let result = task
            .ok_or_else(|| io::Error::other("capacity transfer task missing"))
            .and_then(|task| copy_and_sync(&task.source, &task.destination));
        drop(permit);
        let _ = self.completed.send((operation_id, result));
    }
}

struct CapacityDriver {
    started: mpsc::Sender<Instant>,
}

impl EngineLifecycleDriver for CapacityDriver {
    type Session = ();

    fn start(
        &mut self,
        owner: &StableNodeOwner,
        plan: &LaunchPlan,
        generation: u64,
        candidate: &mut CandidateSlot<Self::Session>,
    ) -> Result<(), LifecycleError> {
        let _ = self.started.send(Instant::now());
        candidate
            .install(StartedSession {
                value: (),
                correlation: SessionCorrelation {
                    generation,
                    child_pid: std::process::id(),
                    child_process_start_time_unix_s: 1,
                    server_id: "capacity-fixture".into(),
                    model_id: plan.model_id.clone(),
                    port: 9_001,
                    committed_run_id: owner.run_id.clone(),
                    owner_pid: owner.pid,
                    owner_process_start_time_unix_s: owner.process_start_time_unix_s,
                    gateway_port: owner.gateway_port,
                    generation_alias: format!("loxa-{}-g{generation}", owner.run_id),
                    engine_version: "capacity-fixture".into(),
                },
            })
            .map_err(|_| LifecycleError::InvalidCandidate("capacity candidate occupied".into()))
    }

    fn wait_ready(
        &mut self,
        _session: &mut StartedSession<Self::Session>,
        _signals: LifecycleSignals<'_>,
    ) -> Result<(), LifecycleError> {
        Ok(())
    }

    fn stop_exact<'a>(
        &mut self,
        _session: &'a mut StartedSession<Self::Session>,
    ) -> Result<(), ExactStopFailure<'a, Self::Session>> {
        Ok(())
    }
}

struct CapacityGateway;

impl GatewayPublisher for CapacityGateway {
    fn withdraw(&mut self) {}

    fn publish(&mut self, _plan: &LaunchPlan, _session: &SessionCorrelation) {}
}

#[derive(Clone, Copy)]
struct LifecycleSample {
    request_to_admission_us: u128,
    admission_to_controller_start_us: u128,
    lifecycle_shutdown_us: u128,
    control_writer_shutdown_us: u128,
}

fn copy_and_sync(source: &Path, destination: &Path) -> io::Result<()> {
    let mut source = File::open(source)?;
    let mut destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        destination.write_all(&buffer[..read])?;
    }
    destination.sync_all()
}

fn system_sync() -> io::Result<()> {
    let status = std::process::Command::new("sync").status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("system sync failed"))
    }
}

fn create_fixture(path: &Path, size: u64) -> io::Result<[u8; 32]> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let block = vec![0_u8; 8 * 1024 * 1024];
    let mut digest = Sha256::new();
    let mut remaining = size;
    while remaining > 0 {
        let write = remaining.min(block.len() as u64) as usize;
        file.write_all(&block[..write])?;
        digest.update(&block[..write]);
        remaining -= write as u64;
    }
    file.sync_all()?;
    system_sync()?;
    Ok(digest.finalize().into())
}

fn physical_bytes(path: &Path) -> u64 {
    std::process::Command::new("du")
        .args(["-k", path.to_str().unwrap_or_default()])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|output| output.split_whitespace().next()?.parse::<u64>().ok())
        .map(|kib| kib.saturating_mul(1024))
        .unwrap_or(0)
}

fn operation(sequence: u64) -> OperationId {
    OperationId::from_str(&format!("7aaaaaaa-0000-4000-8000-{sequence:012x}")).unwrap()
}

fn download_key(destination: &Path, sequence: u64, size: u64) -> DownloadKey {
    DownloadKey::new(
        &format!("capacity-{sequence}"),
        "hugging-face",
        &format!("capacity/repository-{sequence}"),
        Some("0123456789abcdef0123456789abcdef01234567"),
        &format!("weights/capacity-{sequence}.gguf"),
        None,
        Some(size),
        ArtifactKey::from_destination(destination).unwrap(),
    )
    .unwrap()
}

fn transfer_sample(
    root: &Path,
    source: &Path,
    size: u64,
    worker_count: usize,
    sequence: u64,
) -> io::Result<(u64, u128)> {
    let destination_root = root.join(format!("transfer-{sequence}"));
    std::fs::create_dir(&destination_root)?;
    let tasks = Arc::new(Mutex::new(HashMap::new()));
    let (completed_tx, completed_rx) = mpsc::channel();
    let executor = Arc::new(CapacityTransferExecutor {
        tasks: Arc::clone(&tasks),
        completed: completed_tx,
    });
    let (handle, owner) =
        DownloadSchedulerOwner::spawn_with_worker_count_for_test(executor, worker_count)?;
    let sample_result = (|| {
        let mut operations = Vec::new();
        let mut bounds = Vec::new();
        for stream in 0..worker_count {
            let job_sequence = sequence * 10 + stream as u64;
            let operation_id = operation(job_sequence);
            let destination = destination_root.join(format!("stream-{stream}.gguf"));
            tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    operation_id,
                    TransferTask {
                        source: source.to_path_buf(),
                        destination: destination.clone(),
                    },
                );
            let reservation = match handle.reserve(download_key(&destination, job_sequence, size)) {
                DownloadReserveOutcome::Reserved(reservation) => reservation,
                _ => return Err(io::Error::other("capacity download reservation failed")),
            };
            bounds.push(
                reservation
                    .bind(
                        operation_id,
                        DecimalU64::new(job_sequence + 1),
                        OperationCancellation::new(),
                    )
                    .map_err(|_| io::Error::other("capacity download bind failed"))?,
            );
            operations.push(operation_id);
        }
        let started = Instant::now();
        for bound in bounds {
            if handle.submit(bound) != DownloadSubmitOutcome::Submitted {
                return Err(io::Error::other("capacity download submit failed"));
            }
        }
        let deadline = started + SAMPLE_DEADLINE;
        let mut first_error = None;
        for _ in 0..worker_count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (_, result) = completed_rx.recv_timeout(remaining).map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "capacity transfer timed out")
            })?;
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        system_sync()?;
        let elapsed = started.elapsed();
        for operation_id in operations {
            let finish_deadline = Instant::now() + Duration::from_secs(2);
            while !handle.finish_committed(operation_id) {
                if Instant::now() >= finish_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "capacity transfer did not release",
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(throughput(size * worker_count as u64, elapsed))
    })();
    let shutdown_started = Instant::now();
    let shutdown_result = owner
        .shutdown(shutdown_started + Duration::from_secs(5))
        .map_err(|failure| io::Error::other(format!("download shutdown: {:?}", failure.reason())));
    let shutdown_us = shutdown_started.elapsed().as_micros();
    let cleanup_result = std::fs::remove_dir_all(destination_root);
    let transfer = sample_result?;
    shutdown_result?;
    cleanup_result?;
    Ok((transfer, shutdown_us))
}

fn verification_sample(
    sources: &[PathBuf],
    expected_digest: [u8; 32],
    size: u64,
    worker_count: usize,
) -> io::Result<(u64, u128)> {
    assert_eq!(sources.len(), worker_count);
    let (handle, owner) =
        VerificationSchedulerOwner::start_with_worker_count_for_test(worker_count)?;
    let sample_result = (|| {
        let started = Instant::now();
        let completions = DownloadCompletionQueue::new(worker_count);
        let coordinator = ArtifactMutationCoordinator::new();
        let mut waiters = Vec::with_capacity(worker_count);
        for (stream, source) in sources.iter().enumerate() {
            let input = StableVerificationInput::open(source, expected_digest)?;
            let key = VerificationKey::new(input.stable.clone(), expected_digest);
            let reservation = match handle.reserve(key, VerificationClass::Download) {
                VerificationReserveOutcome::Reserved(reservation) => reservation,
                VerificationReserveOutcome::Backpressure => {
                    return Err(io::Error::other("capacity verification backpressured"));
                }
                VerificationReserveOutcome::Stopping => {
                    return Err(io::Error::other("capacity verification stopping"));
                }
            };
            let artifact_key = ArtifactKey::from_destination(source)
                .map_err(|error| io::Error::other(format!("capacity artifact key: {error:?}")))?;
            let artifact = coordinator
                .try_acquire_mutation(artifact_key)
                .map_err(|_| io::Error::other("capacity verification lease unavailable"))?;
            let continuation = DownloadContinuation::with_release_probe_for_test(
                operation(900_000 + stream as u64),
                DecimalU64::new(stream as u64 + 1),
                OperationCancellation::new(),
                artifact,
                Box::new(|| {}),
            );
            let completion = completions
                .reserve()
                .ok_or_else(|| io::Error::other("capacity completion queue full"))?;
            let waiter = match reservation.bind_download(input, continuation, completion) {
                Ok(waiter) => waiter,
                Err(failure) => {
                    failure.poison();
                    return Err(io::Error::other("capacity verification bind failed"));
                }
            };
            waiters.push(waiter);
        }
        let deadline = started + SAMPLE_DEADLINE;
        for _ in 0..worker_count {
            let retained = match completions.wait_ready_until(deadline) {
                CompletionWaitOutcome::Ready(retained) => retained,
                CompletionWaitOutcome::TimedOut => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "capacity verification timed out",
                    ));
                }
                CompletionWaitOutcome::Poisoned => {
                    return Err(io::Error::other(
                        "capacity verification completion poisoned",
                    ));
                }
            };
            let mut ready = retained
                .take_ready()
                .ok_or_else(|| io::Error::other("capacity verification completion not ready"))?;
            match &ready.outcome_mut().result {
                VerificationResult::Verified(_) => {}
                VerificationResult::Failed { kind, message } => {
                    return Err(io::Error::new(*kind, message.clone()));
                }
                VerificationResult::Cancelled => {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "capacity verification cancelled",
                    ));
                }
            }
            ready.acknowledge();
        }
        drop(waiters);
        Ok(throughput(size * worker_count as u64, started.elapsed()))
    })();
    let shutdown_started = Instant::now();
    let shutdown_result = shutdown_verification(owner);
    let shutdown_us = shutdown_started.elapsed().as_micros();
    let verification = sample_result?;
    shutdown_result?;
    Ok((verification, shutdown_us))
}

fn shutdown_verification(owner: VerificationSchedulerOwner) -> io::Result<()> {
    match owner.shutdown(Instant::now() + Duration::from_secs(5)) {
        Ok(_) => Ok(()),
        Err(first_failure) => {
            let first_reason = first_failure.reason();
            let owner = first_failure.into_owner();
            match owner.shutdown(Instant::now() + SAMPLE_DEADLINE) {
                Ok(_) => Err(io::Error::other(format!(
                    "verification scheduler required retry after {first_reason:?}"
                ))),
                Err(final_failure) if !final_failure.retains_unjoined_workers() => {
                    final_failure.dispose_for_test();
                    Err(io::Error::other(format!(
                        "verification scheduler failed after bounded retry: {first_reason:?}"
                    )))
                }
                Err(final_failure) => {
                    eprintln!(
                        "fatal: verification scheduler retained workers after bounded retry: {final_failure:?}"
                    );
                    std::process::abort();
                }
            }
        }
    }
}

fn lifecycle_sample(root: &Path, sequence: u64) -> io::Result<LifecycleSample> {
    let sample_root = root.join(format!("lifecycle-{sequence}"));
    let paths = NodePaths {
        models_dir: sample_root.join("models"),
        state_path: sample_root.join("run/managed.json"),
        logs_dir: sample_root.join("run/logs"),
    };
    std::fs::create_dir_all(&paths.logs_dir)?;
    let baseline = ManagedRun {
        schema_version: RUNTIME_STATE_SCHEMA_VERSION,
        run_id: format!("capacity-{sequence}"),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: format!("capacity-{sequence}-g0"),
        control_port: Some(19_440),
        port: 19_440,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone())
        .map_err(|error| io::Error::other(format!("create owner: {error:?}")))?;
    let fixture = open_slice3_control_state_fixture(
        sample_root.join("state/control-state.sqlite3"),
        NodeId::new_v4(),
        paths,
        baseline,
    )
    .map_err(|error| io::Error::other(format!("control fixture: {error:?}")))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(io::Error::other)?;
    runtime
        .block_on(fixture.handle.publish_instance(InstancePublication {
            node_instance_id: NodeInstanceId::new_v4(),
            control_endpoint: "http://127.0.0.1:19440".into(),
            capabilities: V2NodeCapabilities {
                model_download: true,
                slot_load: true,
                slot_unload: true,
                operation_cancel: true,
                operation_stream: true,
            },
            now_unix_ms: 11,
        }))
        .map_err(|error| io::Error::other(format!("publish fixture: {error:?}")))?;
    let (started_tx, started_rx) = mpsc::channel();
    let lifecycle = ModelLifecycle::new(
        StableNodeOwner {
            run_id: format!("capacity-{sequence}"),
            pid: std::process::id(),
            process_start_time_unix_s: 1,
            gateway_port: 19_440,
        },
        CapacityDriver {
            started: started_tx,
        },
        CapacityGateway,
    );
    let (controller, owner) = LifecycleControllerOwner::start(lifecycle, |model_id| {
        Ok(LaunchPlan {
            model_id: model_id.to_owned(),
            artifact_path: PathBuf::from("capacity.gguf"),
            engine: "llama-cpp".into(),
        })
    })
    .map_err(|failure| io::Error::other(format!("lifecycle start: {failure:?}")))?;
    let sample_result = (|| {
        let request_started = Instant::now();
        let admission = fixture
            .handle
            .admit_blocking_until(
                AdmissionRequest::Load {
                    model_id: "capacity-model".into(),
                },
                request_started + Duration::from_secs(5),
            )
            .map_err(|error| io::Error::other(format!("lifecycle admission: {error:?}")))?;
        let admitted_at = Instant::now();
        controller
            .reserve_normal()
            .ok_or_else(|| io::Error::other("lifecycle reservation failed"))?
            .submit(LifecycleCommand::Load {
                operation_id: admission.operation_id,
                model_id: "capacity-model".into(),
                revision: admission.revision,
            })
            .map_err(|error| io::Error::other(format!("lifecycle submit: {error:?}")))?;
        let controller_started_at =
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "lifecycle controller start timeout",
                    )
                })?;
        let completion = owner
            .recv_completion_timeout(Duration::from_secs(5))
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "lifecycle completion timeout"))?;
        if completion.operation_id() != Some(&admission.operation_id)
            || completion.result().is_err()
        {
            return Err(io::Error::other(format!(
                "lifecycle completion failed: operation={:?} result={:?}",
                completion.operation_id(),
                completion.result()
            )));
        }
        Ok((
            admitted_at.duration_since(request_started).as_micros(),
            controller_started_at
                .duration_since(admitted_at)
                .as_micros(),
        ))
    })();
    let lifecycle_shutdown_started = Instant::now();
    let lifecycle_shutdown_result = shutdown_lifecycle(owner);
    let lifecycle_shutdown_us = lifecycle_shutdown_started.elapsed().as_micros();
    let control_shutdown_started = Instant::now();
    runtime.block_on(fixture.shutdown());
    let control_writer_shutdown_us = control_shutdown_started.elapsed().as_micros();
    drop(runtime);
    let cleanup_result = std::fs::remove_dir_all(sample_root);
    let (request_to_admission_us, admission_to_controller_start_us) = sample_result?;
    lifecycle_shutdown_result?;
    cleanup_result?;
    Ok(LifecycleSample {
        request_to_admission_us,
        admission_to_controller_start_us,
        lifecycle_shutdown_us,
        control_writer_shutdown_us,
    })
}

fn shutdown_lifecycle(owner: LifecycleControllerOwner) -> io::Result<()> {
    match owner.shutdown(Instant::now() + Duration::from_secs(5)) {
        Ok(()) => Ok(()),
        Err(first_failure) => {
            let owner = first_failure.into_owner();
            match owner.shutdown(Instant::now() + SAMPLE_DEADLINE) {
                Ok(()) => Err(io::Error::other(
                    "lifecycle controller required bounded shutdown retry",
                )),
                Err(final_failure) if !final_failure.retains_worker() => {
                    final_failure.into_owner().dispose_fatal_for_test();
                    Err(io::Error::other(
                        "lifecycle controller failed after workers joined",
                    ))
                }
                Err(final_failure) => {
                    eprintln!(
                        "fatal: lifecycle controller retained its worker after bounded retry: {final_failure:?}"
                    );
                    std::process::abort();
                }
            }
        }
    }
}

fn throughput(bytes: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos().max(1);
    (u128::from(bytes) * 1_000_000_000 / nanos)
        .try_into()
        .unwrap_or(u64::MAX)
}

fn nearest_rank_u64(samples: &[u64], percentile: usize) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn nearest_rank_u128(samples: &[u128], percentile: usize) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn command_output(command: &str, arguments: &[&str]) -> String {
    std::process::Command::new(command)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|output| output.trim().to_owned())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".into())
}

fn hardware_profile() -> String {
    let profile = command_output("system_profiler", &["SPHardwareDataType"]);
    let selected = profile
        .lines()
        .map(str::trim)
        .filter(|line| {
            ["Model Name:", "Model Identifier:", "Chip:", "Memory:"]
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .collect::<Vec<_>>()
        .join("; ");
    if selected.is_empty() {
        "unavailable (hardware query denied)".into()
    } else {
        selected
    }
}

fn filesystem_profile(path: &Path) -> String {
    let device = command_output("df", &[path.to_str().unwrap_or_default()])
        .lines()
        .nth(1)
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or("unknown")
        .to_owned();
    command_output("mount", &[])
        .lines()
        .find(|line| line.starts_with(&device))
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{device} (filesystem type unavailable)"))
}

fn run_capacity_matrix(matrix: CapacityMatrix) -> io::Result<CapacityReport> {
    assert_eq!(matrix.warmups, 2);
    assert_eq!(matrix.measured_runs, 10);
    assert_eq!(
        matrix.artifact_sizes,
        [64 * 1024 * 1024, 1024 * 1024 * 1024]
    );
    assert_eq!(matrix.worker_counts, [1, 2]);
    let matrix_started = Instant::now();
    let root = MatrixRoot::new()?;
    let mut fixtures = Vec::new();
    let mut physical = Vec::new();
    for size in matrix.artifact_sizes {
        let first = root.0.join(format!("fixture-{size}-0.bin"));
        let second = root.0.join(format!("fixture-{size}-1.bin"));
        let digest = create_fixture(&first, size)?;
        let second_digest = create_fixture(&second, size)?;
        if second_digest != digest {
            return Err(io::Error::other("capacity fixtures differ"));
        }
        physical.push([physical_bytes(&first), physical_bytes(&second)]);
        fixtures.push((size, [first, second], digest));
    }
    let mut cells = Vec::new();
    let mut sequence = 1_u64;
    for (size, sources, digest) in &fixtures {
        for worker_count in matrix.worker_counts {
            let mut transfer_samples = Vec::new();
            let mut verification_samples = Vec::new();
            let mut admission_samples = Vec::new();
            let mut controller_samples = Vec::new();
            let mut shutdown_samples = Vec::new();
            for run in 0..(matrix.warmups + matrix.measured_runs) {
                if matrix_started.elapsed() >= MATRIX_DEADLINE {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "capacity matrix exceeded 30-minute bound",
                    ));
                }
                let (transfer, download_shutdown_us) =
                    transfer_sample(&root.0, &sources[0], *size, worker_count, sequence)?;
                let (verification, verification_shutdown_us) =
                    verification_sample(&sources[..worker_count], *digest, *size, worker_count)?;
                let lifecycle = lifecycle_sample(&root.0, sequence)?;
                if run >= matrix.warmups {
                    transfer_samples.push(transfer);
                    verification_samples.push(verification);
                    admission_samples.push(lifecycle.request_to_admission_us);
                    controller_samples.push(lifecycle.admission_to_controller_start_us);
                    shutdown_samples.push(ShutdownStages {
                        download_us: download_shutdown_us,
                        verification_us: verification_shutdown_us,
                        lifecycle_us: lifecycle.lifecycle_shutdown_us,
                        control_writer_us: lifecycle.control_writer_shutdown_us,
                    });
                }
                sequence += 1;
            }
            let shutdown_totals = shutdown_samples
                .iter()
                .map(|stages| stages.total_us())
                .collect::<Vec<_>>();
            cells.push(CapacityCell {
                artifact_size: *size,
                worker_count,
                transfer_p50_bytes_per_second: nearest_rank_u64(&transfer_samples, 50),
                transfer_p95_bytes_per_second: nearest_rank_u64(&transfer_samples, 95),
                verification_p50_bytes_per_second: nearest_rank_u64(&verification_samples, 50),
                verification_p95_bytes_per_second: nearest_rank_u64(&verification_samples, 95),
                lifecycle_request_to_admission_p50_us: nearest_rank_u128(&admission_samples, 50),
                lifecycle_request_to_admission_p95_us: nearest_rank_u128(&admission_samples, 95),
                lifecycle_admission_to_controller_start_p50_us: nearest_rank_u128(
                    &controller_samples,
                    50,
                ),
                lifecycle_admission_to_controller_start_p95_us: nearest_rank_u128(
                    &controller_samples,
                    95,
                ),
                shutdown_total_p50_us: nearest_rank_u128(&shutdown_totals, 50),
                shutdown_total_p95_us: nearest_rank_u128(&shutdown_totals, 95),
                transfer_bytes_per_second: transfer_samples,
                verification_bytes_per_second: verification_samples,
                lifecycle_request_to_admission_us: admission_samples,
                lifecycle_admission_to_controller_start_us: controller_samples,
                shutdown_stages: shutdown_samples,
            });
        }
    }
    Ok(CapacityReport {
        host: hardware_profile(),
        os: command_output("sw_vers", &["-productVersion"]),
        architecture: command_output("uname", &["-m"]),
        filesystem: filesystem_profile(&root.0),
        fixture_policy: "exact logical 64MiB/1GiB zero-filled files; every transfer uses a fresh absent destination",
        sync_policy: "fixture and destination File::sync_all followed by system sync; OS cache is not claimed purged",
        measurement_limitations: "APFS may compress zero-filled physical allocation; physical bytes for both distinct verification identities are reported. Transfer uses the actual DownloadScheduler with a controlled 64KiB copy executor. Verification uses actual VerificationScheduler admission, dispatch, hashing, retained completion and acknowledgement. Lifecycle timing isolates the actual SQLite writer and LifecycleController handoff with a no-process driver; it excludes artifact verification and real engine startup.",
        logical_fixture_bytes: matrix.artifact_sizes,
        physical_fixture_bytes: [physical[0], physical[1]],
        warmups: matrix.warmups,
        measured_runs: matrix.measured_runs,
        cells,
    })
}

#[test]
#[ignore = "manual Slice 4 capacity evidence"]
fn measured_scheduler_defaults() {
    let report = run_capacity_matrix(CapacityMatrix {
        warmups: 2,
        measured_runs: 10,
        artifact_sizes: [64 * 1024 * 1024, 1024 * 1024 * 1024],
        worker_counts: [1, 2],
    })
    .expect("capacity matrix completes");
    eprintln!("{report:#?}");
    report.assert_approved_thresholds();
}
