mod delivery;
mod encoder;

use self::delivery::{
    drop_guard_with_deadline, non_blocking_writer_with_health, wait_for_queue_drain, QueueProgress,
    ShutdownGuard, StorageReporter, DIAGNOSTICS_SHUTDOWN_BOUND, QUEUE_DRAIN_DEADLINE,
    WORKER_GUARD_DEADLINE,
};
#[cfg(test)]
use self::delivery::{
    non_blocking_writer_with_reporter, DropWarningClock, DropWarningReporter, DropWarningSink,
};
#[cfg(test)]
use self::encoder::debug_filter;
#[cfg(all(test, debug_assertions))]
use self::encoder::release_filter;
use self::encoder::{executable_filter, SafeHumanFormatter, SafeJsonFields, SafeJsonFormatter};
use loxa_core::diagnostics::{
    storage::prepare_logs_dir, BoundedJsonlWriter, DiagnosticsHealth, DiagnosticsHealthSnapshot,
    StorageConfig, SystemDiskSpace, LOG_QUEUE_CAPACITY,
};
use std::io;
use std::path::Path;
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

pub struct DiagnosticsBootstrap {
    guard: Option<ShutdownGuard>,
    health: DiagnosticsHealth,
    queue_progress: Option<Arc<QueueProgress>>,
}

impl DiagnosticsBootstrap {
    pub fn health(&self) -> DiagnosticsHealth {
        self.health.clone()
    }

    pub fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        if let Some(progress) = &self.queue_progress {
            progress.observe_drops();
        }
        self.health.snapshot()
    }
}

impl Drop for DiagnosticsBootstrap {
    fn drop(&mut self) {
        debug_assert!(QUEUE_DRAIN_DEADLINE + WORKER_GUARD_DEADLINE <= DIAGNOSTICS_SHUTDOWN_BOUND);
        let Some(guard) = self.guard.take() else {
            return;
        };
        let Some(progress) = &self.queue_progress else {
            std::mem::forget(guard);
            return;
        };
        progress.begin_shutdown();
        if !wait_for_queue_drain(progress, QUEUE_DRAIN_DEADLINE) {
            std::mem::forget(guard);
            return;
        }
        drop_guard_with_deadline(guard, WORKER_GUARD_DEADLINE);
    }
}
pub fn install_daemon_diagnostics(logs_dir: &Path) -> DiagnosticsBootstrap {
    let health = DiagnosticsHealth::new();

    let file_sink = open_file_sink(logs_dir, health.clone());

    let (guard, queue_progress) = match file_sink {
        Ok(file_sink) => {
            health.support_queue_drop_counter();
            let stderr_layer = tracing_subscriber::fmt::layer()
                .fmt_fields(SafeJsonFields::uncounted(health.clone()))
                .event_format(SafeHumanFormatter::uncounted(health.clone()))
                .with_ansi(false)
                .with_writer(io::stderr)
                .with_filter(executable_filter());
            let reporter = StorageReporter::new(file_sink, health.clone());
            let (file_writer, guard, progress) =
                non_blocking_writer_with_health(reporter, LOG_QUEUE_CAPACITY, health.clone());
            let file_layer = tracing_subscriber::fmt::layer()
                .fmt_fields(SafeJsonFields::new(health.clone()))
                .event_format(SafeJsonFormatter::new(health.clone()))
                .with_ansi(false)
                .with_writer(file_writer)
                .with_filter(executable_filter());
            let subscriber = tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer);
            if tracing::subscriber::set_global_default(subscriber).is_err() {
                eprintln!("loxa diagnostics: subscriber unavailable; continuing best effort");
                health.mark_unavailable();
            }
            (Some(guard), Some(progress))
        }
        Err(_) => {
            eprintln!("loxa diagnostics: file logging unavailable; continuing with stderr");
            health.mark_unavailable();
            let stderr_layer = tracing_subscriber::fmt::layer()
                .fmt_fields(SafeJsonFields::uncounted(health.clone()))
                .event_format(SafeHumanFormatter::new(health.clone()))
                .with_ansi(false)
                .with_writer(io::stderr)
                .with_filter(executable_filter());
            let subscriber = tracing_subscriber::registry().with(stderr_layer);
            if tracing::subscriber::set_global_default(subscriber).is_err() {
                eprintln!("loxa diagnostics: subscriber unavailable; continuing best effort");
            }
            (None, None)
        }
    };

    DiagnosticsBootstrap {
        guard,
        health,
        queue_progress,
    }
}

fn open_file_sink(
    logs_dir: &Path,
    health: DiagnosticsHealth,
) -> io::Result<BoundedJsonlWriter<SystemDiskSpace>> {
    let result = match prepare_logs_dir(logs_dir) {
        Ok(()) => BoundedJsonlWriter::new(
            StorageConfig::for_logs_dir(logs_dir),
            SystemDiskSpace,
            health.clone(),
        ),
        Err(error) => {
            health.support_storage_write_failures_counter();
            health.increment_storage_write_failures();
            Err(error)
        }
    };
    if result.is_err() {
        health.mark_unavailable();
    }
    result
}

pub fn emit_final_shutdown_diagnostic(result_class: &'static str) {
    tracing::info!(
        target: "loxa_node::shutdown",
        event_code = "shutdown.completed",
        component = "shutdown",
        result_class,
    );
}

#[cfg(test)]
mod tests;
