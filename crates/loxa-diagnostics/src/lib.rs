use std::ffi::OsString;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format as tracing_format;
use tracing_subscriber::prelude::*;

mod format;
mod writer;
use self::format::BoundedJsonFormatter;
use writer::{prepare_directory, reject_unowned_daily_entries, RetainedDailyWriter};

const DEFAULT_FILTER: &str = "loxa=info";
// The bounded channel queues at most approximately 4 MiB of record payload
// per process, excluding channel and allocation overhead.
const MAX_QUEUED_LOG_RECORDS: usize = 128;
const MAX_LOG_RECORD_BYTES: usize = 32 * 1024;
// This acknowledgement wait is additional to tracing-appender's own bounded
// WorkerGuard shutdown waits.
const WRITER_EXIT_TIMEOUT: Duration = Duration::from_millis(100);

static ACTIVE_LOG_DIR: OnceLock<PathBuf> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRole {
    Cli,
    Service,
    Desktop,
}

impl ProcessRole {
    fn directory(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Service => "service",
            Self::Desktop => "desktop",
        }
    }

    fn accepts_target(self, target: &str) -> bool {
        let core = target == "loxa" || target.starts_with("loxa::");
        core || (matches!(self, Self::Desktop)
            && (target == "loxa_app" || target.starts_with("loxa_app::")))
    }
}

/// An approximate live telemetry sample. The independent fields are not read
/// as one atomic transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticsHealth {
    pub enqueue_drops: usize,
    pub sink_failures: usize,
    pub sink_discards: usize,
    pub at_capacity: bool,
    pub sink_failed: bool,
    pub drain_incomplete: bool,
}

impl DiagnosticsHealth {
    pub fn is_available(self) -> bool {
        !self.at_capacity && !self.sink_failed
    }

    pub fn is_healthy(self) -> bool {
        self.enqueue_drops == 0
            && self.sink_failures == 0
            && self.sink_discards == 0
            && self.is_available()
            && !self.drain_incomplete
    }
}

// Relaxed independent atomics are sufficient for telemetry; stronger ordering
// would not make a multi-field snapshot transactional.
#[derive(Default)]
struct SinkHealth {
    failures: AtomicUsize,
    discards: AtomicUsize,
    at_capacity: AtomicBool,
    failed: AtomicBool,
    drain_incomplete: AtomicBool,
}

impl SinkHealth {
    fn snapshot(&self, enqueue_drops: usize) -> DiagnosticsHealth {
        DiagnosticsHealth {
            enqueue_drops,
            sink_failures: self.failures.load(Ordering::Relaxed),
            sink_discards: self.discards.load(Ordering::Relaxed),
            at_capacity: self.at_capacity.load(Ordering::Relaxed),
            sink_failed: self.failed.load(Ordering::Relaxed),
            drain_incomplete: self.drain_incomplete.load(Ordering::Relaxed),
        }
    }

    fn record_discard(&self) {
        saturating_increment(&self.discards);
    }

    fn record_capacity_discard(&self) {
        self.at_capacity.store(true, Ordering::Relaxed);
        self.record_discard();
    }

    fn clear_capacity(&self) {
        self.at_capacity.store(false, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failed.store(true, Ordering::Relaxed);
        saturating_increment(&self.failures);
        self.record_discard();
    }

    fn record_incomplete_drain(&self) {
        self.drain_incomplete.store(true, Ordering::Relaxed);
    }
}

fn saturating_increment(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

pub struct Diagnostics {
    _guard: WorkerGuard,
    enqueue_errors: ErrorCounter,
    sink_health: Arc<SinkHealth>,
    log_dir: PathBuf,
    writer_exit: Receiver<()>,
}

#[derive(Clone)]
pub struct DiagnosticsHealthHandle {
    enqueue_errors: ErrorCounter,
    sink_health: Arc<SinkHealth>,
}

impl DiagnosticsHealthHandle {
    pub fn health(&self) -> DiagnosticsHealth {
        self.sink_health
            .snapshot(self.enqueue_errors.dropped_lines())
    }
}

impl Diagnostics {
    pub fn health(&self) -> DiagnosticsHealth {
        self.sink_health
            .snapshot(self.enqueue_errors.dropped_lines())
    }

    pub fn health_handle(&self) -> DiagnosticsHealthHandle {
        DiagnosticsHealthHandle {
            enqueue_errors: self.enqueue_errors.clone(),
            sink_health: Arc::clone(&self.sink_health),
        }
    }

    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    pub fn finish(self) -> DiagnosticsHealth {
        let Self {
            _guard,
            enqueue_errors,
            sink_health,
            log_dir: _,
            writer_exit,
        } = self;
        drop(_guard);
        if writer_exit.recv_timeout(WRITER_EXIT_TIMEOUT).is_err() {
            sink_health.record_incomplete_drain();
        }
        sink_health.snapshot(enqueue_errors.dropped_lines())
    }
}

pub fn init(log_root: &Path, role: ProcessRole) -> Result<Diagnostics, String> {
    prepare_directory(log_root)?;
    let log_dir = log_root.join(role.directory());
    prepare_directory(&log_dir)?;
    reject_unowned_daily_entries(&log_dir)?;

    let filter = selected_filter(|name| std::env::var_os(name))
        .map_err(|source| format!("invalid diagnostics log filter in {source}"))?;
    let sink_health = Arc::new(SinkHealth::default());
    let (writer_exit_tx, writer_exit) = sync_channel(1);
    let appender = RetainedDailyWriter::new(&log_dir, Arc::clone(&sink_health), writer_exit_tx)?;
    let (writer, guard) = nonfatal_constructor(|| {
        NonBlockingBuilder::default()
            .buffered_lines_limit(MAX_QUEUED_LOG_RECORDS)
            .lossy(true)
            .thread_name("loxa-diagnostics")
            .finish(appender)
    })?;
    let enqueue_errors = writer.error_counter();
    let layer = tracing_subscriber::fmt::layer()
        .event_format(BoundedJsonFormatter)
        // BoundedJsonFormatter intentionally ignores spans, so avoid formatting
        // and storing span fields that no output path will read.
        .fmt_fields(tracing_format::debug_fn(|_, _, _| Ok(())))
        .with_writer(writer)
        .with_filter(filter)
        .with_filter(tracing_subscriber::filter::filter_fn(move |metadata| {
            role.accepts_target(metadata.target())
        }));
    tracing_subscriber::registry()
        .with(layer)
        .try_init()
        .map_err(|error| format!("failed to initialize diagnostics: {error}"))?;

    let _ = ACTIVE_LOG_DIR.set(log_dir.clone());
    Ok(Diagnostics {
        _guard: guard,
        enqueue_errors,
        sink_health,
        log_dir,
        writer_exit,
    })
}

fn nonfatal_constructor<T>(construct: impl FnOnce() -> T) -> Result<T, String> {
    // tracing-appender 0.2.5 panics if its writer thread cannot be spawned;
    // diagnostics initialization is deliberately nonfatal to the application.
    catch_unwind(AssertUnwindSafe(construct))
        .map_err(|_| "failed to initialize diagnostics worker".to_string())
}

pub fn active_log_dir() -> Option<&'static Path> {
    ACTIVE_LOG_DIR.get().map(PathBuf::as_path)
}

fn selected_filter<F>(mut read: F) -> Result<EnvFilter, &'static str>
where
    F: FnMut(&str) -> Option<OsString>,
{
    for name in ["LOXA_LOG", "RUST_LOG"] {
        if let Some(value) = read(name) {
            let value = value.into_string().map_err(|_| name)?;
            return EnvFilter::try_new(value).map_err(|_| name);
        }
    }
    Ok(EnvFilter::new(DEFAULT_FILTER))
}

#[cfg(test)]
mod tests;
