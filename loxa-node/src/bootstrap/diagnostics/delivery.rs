use loxa_core::diagnostics::{DiagnosticsAvailability, DiagnosticsHealth};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::fmt::MakeWriter;

pub(super) const QUEUE_DRAIN_DEADLINE: Duration = Duration::from_millis(350);
pub(super) const WORKER_GUARD_DEADLINE: Duration = Duration::from_millis(350);
pub(super) const DIAGNOSTICS_SHUTDOWN_BOUND: Duration = Duration::from_millis(800);
const QUEUE_PROGRESS_POLL: Duration = Duration::from_millis(2);
const DROP_WARNING_INTERVAL: Duration = Duration::from_secs(30);
pub(super) const FILE_LOGGING_UNAVAILABLE_WARNING: &str =
    "loxa diagnostics: diagnostics.file_unavailable; continuing with stderr";
pub(super) const SUBSCRIBER_UNAVAILABLE_WARNING: &str =
    "loxa diagnostics: diagnostics.subscriber_unavailable; continuing best effort";
pub(super) const STORAGE_DEGRADED_WARNING: &str =
    "loxa diagnostics: diagnostics.storage_degraded; continuing with stderr";
pub(super) const STORAGE_RECOVERED_WARNING: &str =
    "loxa diagnostics: diagnostics.storage_recovered";
pub(super) const SHUTDOWN_HELPER_UNAVAILABLE_WARNING: &str =
    "loxa diagnostics: diagnostics.shutdown_helper_unavailable; detaching file logging";

pub(super) trait DropWarningClock: Send + Sync + 'static {
    fn elapsed(&self) -> Duration;
}

pub(super) trait BypassWarningSink: Send + Sync + 'static {
    fn write_warning(&self, warning: &str) -> io::Result<()>;
}

struct MonotonicClock(Instant);

impl DropWarningClock for MonotonicClock {
    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

struct StderrWarningSink;

impl BypassWarningSink for StderrWarningSink {
    fn write_warning(&self, warning: &str) -> io::Result<()> {
        writeln!(io::stderr().lock(), "{warning}")
    }
}

pub(super) fn write_bypass_warning(warning: &str) {
    write_bypass_warning_to(&StderrWarningSink, warning);
}

pub(super) fn write_bypass_warning_to<S: BypassWarningSink + ?Sized>(sink: &S, warning: &str) {
    let _ = sink.write_warning(warning);
}

struct DropWarningState {
    reported_total: u64,
    last_warning_at: Option<Duration>,
}

pub(super) struct DropWarningReporter {
    clock: Arc<dyn DropWarningClock>,
    sink: Arc<dyn BypassWarningSink>,
    interval: Duration,
    state: Mutex<DropWarningState>,
}

impl DropWarningReporter {
    fn stderr() -> Self {
        Self {
            clock: Arc::new(MonotonicClock(Instant::now())),
            sink: Arc::new(StderrWarningSink),
            interval: DROP_WARNING_INTERVAL,
            state: Mutex::new(DropWarningState {
                reported_total: 0,
                last_warning_at: None,
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        clock: impl DropWarningClock,
        sink: impl BypassWarningSink,
        interval: Duration,
    ) -> Self {
        Self {
            clock: Arc::new(clock),
            sink: Arc::new(sink),
            interval,
            state: Mutex::new(DropWarningState {
                reported_total: 0,
                last_warning_at: None,
            }),
        }
    }

    fn observe(&self, total: u64, force: bool) {
        let now = self.clock.elapsed();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let delta = total.saturating_sub(state.reported_total);
        if delta == 0 {
            return;
        }
        let interval_elapsed = state
            .last_warning_at
            .is_none_or(|last| now.saturating_sub(last) >= self.interval);
        if !force && !interval_elapsed {
            return;
        }
        let warning =
            format!("loxa diagnostics: diagnostics.queue_dropped delta={delta} total={total}");
        write_bypass_warning_to(self.sink.as_ref(), &warning);
        state.reported_total = total;
        state.last_warning_at = Some(now);
    }
}

#[derive(Default)]
struct QueueCounters {
    submitted: AtomicU64,
    processed: AtomicU64,
}

pub(super) struct QueueProgress {
    counters: Arc<QueueCounters>,
    errors: ErrorCounter,
    health: DiagnosticsHealth,
    drop_reporter: DropWarningReporter,
    admission_open: Mutex<bool>,
    #[cfg(test)]
    admission_pause: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
    #[cfg(test)]
    shutdown_attempt: Mutex<Option<mpsc::SyncSender<()>>>,
}

impl QueueProgress {
    pub(super) fn dropped_total(&self) -> u64 {
        u64::try_from(self.errors.dropped_lines()).unwrap_or(u64::MAX)
    }

    pub(super) fn observe_drops(&self) {
        let total = self.dropped_total();
        self.health.observe_queue_dropped_total(total);
        self.drop_reporter.observe(total, false);
    }

    pub(super) fn begin_shutdown(&self) {
        #[cfg(test)]
        if let Some(attempt) = self
            .shutdown_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = attempt.send(());
        }
        *self
            .admission_open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
        let total = self.dropped_total();
        self.health.observe_queue_dropped_total(total);
        self.drop_reporter.observe(total, true);
    }

    pub(super) fn finalize_drops(&self) {
        let total = self.dropped_total();
        self.health.observe_queue_dropped_total(total);
        self.drop_reporter.observe(total, true);
    }

    #[cfg(test)]
    pub(super) fn pause_admission_for_test(&self, reached: Arc<Barrier>, release: Arc<Barrier>) {
        *self
            .admission_pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((reached, release));
    }

    #[cfg(test)]
    pub(super) fn signal_shutdown_attempt_for_test(&self, attempt: mpsc::SyncSender<()>) {
        *self
            .shutdown_attempt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(attempt);
    }

    #[cfg(test)]
    fn pause_after_open_for_test(&self) {
        let pause = self
            .admission_pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some((reached, release)) = pause {
            reached.wait();
            release.wait();
        }
    }

    fn is_drained(&self) -> bool {
        self.observe_drops();
        let submitted = self.counters.submitted.load(Ordering::Acquire);
        let accepted = submitted.saturating_sub(self.dropped_total());
        let processed = self.counters.processed.load(Ordering::Acquire);
        processed >= accepted
    }
}

#[derive(Clone)]
pub(super) struct QueueWriter {
    inner: NonBlocking,
    progress: Arc<QueueProgress>,
}

impl Write for QueueWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Closing admission and accounting accepted writes share this critical section. The
        // queue write itself stays outside it so runtime producers never hold the lock over I/O.
        let admitted = {
            let admission_open = self
                .progress
                .admission_open
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !*admission_open {
                false
            } else {
                #[cfg(test)]
                self.progress.pause_after_open_for_test();
                self.progress
                    .counters
                    .submitted
                    .fetch_add(1, Ordering::Release);
                true
            }
        };
        if !admitted {
            return Ok(bytes.len());
        }
        let result = self.inner.write(bytes);
        self.progress.observe_drops();
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'writer> MakeWriter<'writer> for QueueWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

struct ProcessedWriter<W> {
    inner: W,
    counters: Arc<QueueCounters>,
}

impl<W: Write> Write for ProcessedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write_all(bytes)?;
        self.counters.processed.fetch_add(1, Ordering::Release);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(super) struct ShutdownGuard {
    action: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl ShutdownGuard {
    fn worker(guard: WorkerGuard) -> Self {
        Self {
            action: Some(Box::new(move || drop(guard))),
        }
    }

    #[cfg(test)]
    pub(super) fn test_action(action: impl FnOnce() + Send + 'static) -> Self {
        Self {
            action: Some(Box::new(action)),
        }
    }

    fn run(mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

pub(super) fn non_blocking_writer_with_health<W: Write + Send + 'static>(
    writer: W,
    capacity: usize,
    health: DiagnosticsHealth,
) -> (QueueWriter, ShutdownGuard, Arc<QueueProgress>) {
    non_blocking_writer_with_reporter(writer, capacity, health, DropWarningReporter::stderr())
}

pub(super) fn non_blocking_writer_with_reporter<W: Write + Send + 'static>(
    writer: W,
    capacity: usize,
    health: DiagnosticsHealth,
    drop_reporter: DropWarningReporter,
) -> (QueueWriter, ShutdownGuard, Arc<QueueProgress>) {
    let counters = Arc::new(QueueCounters::default());
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(capacity)
        .lossy(true)
        .finish(ProcessedWriter {
            inner: writer,
            counters: Arc::clone(&counters),
        });
    let errors = writer.error_counter();
    let progress = Arc::new(QueueProgress {
        counters,
        errors,
        health,
        drop_reporter,
        admission_open: Mutex::new(true),
        #[cfg(test)]
        admission_pause: Mutex::new(None),
        #[cfg(test)]
        shutdown_attempt: Mutex::new(None),
    });
    progress.observe_drops();
    (
        QueueWriter {
            inner: writer,
            progress: Arc::clone(&progress),
        },
        ShutdownGuard::worker(guard),
        progress,
    )
}

pub(super) struct StorageReporter<W> {
    inner: W,
    health: DiagnosticsHealth,
    warning_sink: Arc<dyn BypassWarningSink>,
    degraded_episode: bool,
}

impl<W> StorageReporter<W> {
    pub(super) fn new(inner: W, health: DiagnosticsHealth) -> Self {
        Self {
            inner,
            health,
            warning_sink: Arc::new(StderrWarningSink),
            degraded_episode: false,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        inner: W,
        health: DiagnosticsHealth,
        warning_sink: impl BypassWarningSink,
    ) -> Self {
        Self {
            inner,
            health,
            warning_sink: Arc::new(warning_sink),
            degraded_episode: false,
        }
    }

    fn report_transition(&mut self) {
        match self.health.snapshot().availability {
            DiagnosticsAvailability::Degraded if !self.degraded_episode => {
                write_bypass_warning_to(self.warning_sink.as_ref(), STORAGE_DEGRADED_WARNING);
                self.degraded_episode = true;
            }
            DiagnosticsAvailability::Available if self.degraded_episode => {
                write_bypass_warning_to(self.warning_sink.as_ref(), STORAGE_RECOVERED_WARNING);
                self.degraded_episode = false;
            }
            _ => {}
        }
    }
}

impl<W: Write> Write for StorageReporter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(bytes);
        self.report_transition();
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = self.inner.flush();
        self.report_transition();
        result
    }
}
pub(super) fn wait_for_queue_drain(progress: &QueueProgress, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        if progress.is_drained() {
            return true;
        }
        let elapsed = started.elapsed();
        if elapsed >= deadline {
            return false;
        }
        std::thread::sleep(QUEUE_PROGRESS_POLL.min(deadline - elapsed));
    }
}

pub(super) fn drop_guard_with_deadline(guard: ShutdownGuard, deadline: Duration) {
    let (completed, receiver) = mpsc::sync_channel(0);
    let shared_guard = Arc::new(Mutex::new(Some(guard)));
    let worker_guard = Arc::clone(&shared_guard);
    let spawned = std::thread::Builder::new()
        .name("loxa-diagnostics-shutdown".to_owned())
        .spawn(move || {
            if let Some(guard) = worker_guard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                guard.run();
            }
            let _ = completed.send(());
        });
    match spawned {
        Ok(handle) => {
            drop(shared_guard);
            let _ = receiver.recv_timeout(deadline);
            drop(handle);
        }
        Err(_) => {
            write_bypass_warning(SHUTDOWN_HELPER_UNAVAILABLE_WARNING);
            std::mem::forget(shared_guard);
        }
    }
}
