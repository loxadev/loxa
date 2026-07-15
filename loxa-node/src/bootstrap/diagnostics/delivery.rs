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
#[derive(Default)]
struct QueueCounters {
    submitted: AtomicU64,
    processed: AtomicU64,
}

pub(super) struct QueueProgress {
    counters: Arc<QueueCounters>,
    errors: ErrorCounter,
    health: DiagnosticsHealth,
    admission_open: Mutex<bool>,
    #[cfg(test)]
    admission_pause: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

impl QueueProgress {
    pub(super) fn dropped_total(&self) -> u64 {
        u64::try_from(self.errors.dropped_lines()).unwrap_or(u64::MAX)
    }

    pub(super) fn observe_drops(&self) {
        self.health
            .observe_queue_dropped_total(self.dropped_total());
    }

    pub(super) fn begin_shutdown(&self) {
        *self
            .admission_open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
        self.observe_drops();
    }

    #[cfg(test)]
    pub(super) fn pause_admission_for_test(&self, reached: Arc<Barrier>, release: Arc<Barrier>) {
        *self
            .admission_pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((reached, release));
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
        admission_open: Mutex::new(true),
        #[cfg(test)]
        admission_pause: Mutex::new(None),
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
    degraded_episode: bool,
}

impl<W> StorageReporter<W> {
    pub(super) fn new(inner: W, health: DiagnosticsHealth) -> Self {
        Self {
            inner,
            health,
            degraded_episode: false,
        }
    }

    fn report_transition(&mut self) {
        match self.health.snapshot().availability {
            DiagnosticsAvailability::Degraded if !self.degraded_episode => {
                eprintln!("loxa diagnostics: file logging degraded; continuing with stderr");
                self.degraded_episode = true;
            }
            DiagnosticsAvailability::Available if self.degraded_episode => {
                eprintln!("loxa diagnostics: file logging recovered");
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
            eprintln!("loxa diagnostics: shutdown helper unavailable; detaching file logging");
            std::mem::forget(shared_guard);
        }
    }
}
