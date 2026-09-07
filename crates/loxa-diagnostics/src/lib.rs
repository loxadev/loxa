use std::ffi::OsString;
use std::fmt::{self, Write as FmtWrite};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write as IoWrite};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

const DEFAULT_FILTER: &str = "loxa=info";
const LOG_PREFIX: &str = "loxa.jsonl.";
// Retention is approximately 32 MiB per role after pruning. The bounded
// channel queues at most approximately 4 MiB of record payload per process,
// excluding channel and allocation overhead.
const MAX_LOG_FILES: usize = 8;
const MAX_LOG_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LOG_SCAN_ENTRIES: usize = 256;
const MAX_QUEUED_LOG_RECORDS: usize = 128;
const MAX_LOG_RECORD_BYTES: usize = 32 * 1024;
const MAX_EVENT_FIELDS: usize = 24;
const EVENT_METADATA_FIELDS: usize = 3;
const MAX_FIELD_NAME_BYTES: usize = 64;
const MAX_FIELD_VALUE_BYTES: usize = 160;
const TRUNCATION_MARKER: &str = "...";
// This acknowledgement wait is additional to tracing-appender's own bounded
// WorkerGuard shutdown waits.
const WRITER_EXIT_TIMEOUT: Duration = Duration::from_millis(100);
const OWNERSHIP_LINE: &[u8] = b"{\"event\":\"log_initialized\",\"application\":\"loxa\"}\n";
const TRUNCATED_RECORD: &[u8] =
    b"{\"event\":\"diagnostics_record_truncated\",\"level\":\"WARN\",\"target\":\"loxa\"}\n";

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
        .fmt_fields(format::debug_fn(|_, _, _| Ok(())))
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

#[derive(Clone, Copy, Debug)]
struct BoundedJsonFormatter;

impl<S, N> FormatEvent<S, N> for BoundedJsonFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        _context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let now = time::OffsetDateTime::now_utc();
        let timestamp_ms = now
            .unix_timestamp()
            .saturating_mul(1_000)
            .saturating_add(i64::from(now.millisecond()));
        let mut record = serde_json::Map::new();
        record.insert("timestamp_ms".into(), timestamp_ms.into());
        record.insert("level".into(), metadata.level().as_str().into());
        record.insert(
            "target".into(),
            bounded_text(metadata.target(), MAX_FIELD_VALUE_BYTES).into(),
        );
        let mut visitor = BoundedFieldVisitor {
            record: &mut record,
            omitted: false,
        };
        event.record(&mut visitor);
        if visitor.omitted {
            record.insert("fields_truncated".into(), true.into());
        }
        let mut buffer = [0_u8; MAX_LOG_RECORD_BYTES];
        let bytes = bounded_json_record(&record, &mut buffer);
        writer.write_str(std::str::from_utf8(bytes).map_err(|_| fmt::Error)?)
    }
}

struct BoundedFieldVisitor<'a> {
    record: &'a mut serde_json::Map<String, serde_json::Value>,
    omitted: bool,
}

impl BoundedFieldVisitor<'_> {
    fn insert(&mut self, field: &Field, build_value: impl FnOnce() -> serde_json::Value) {
        if self.record.len() >= MAX_EVENT_FIELDS + EVENT_METADATA_FIELDS {
            self.omitted = true;
            return;
        }
        let name = field.name();
        if name.len() > MAX_FIELD_NAME_BYTES
            || matches!(
                name,
                "timestamp_ms" | "level" | "target" | "fields_truncated"
            )
        {
            self.omitted = true;
            return;
        }
        self.record.insert(name.to_owned(), build_value());
    }
}

impl Visit for BoundedFieldVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, || {
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| bounded_text(&value.to_string(), MAX_FIELD_VALUE_BYTES).into())
        });
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, || value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, || value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, || value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, || bounded_text(value, MAX_FIELD_VALUE_BYTES).into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.insert(field, || {
            let mut bounded = BoundedText::new(MAX_FIELD_VALUE_BYTES);
            let _ = write!(&mut bounded, "{value:?}");
            bounded.finish().into()
        });
    }
}

struct BoundedText {
    value: String,
    limit: usize,
    truncated: bool,
}

impl BoundedText {
    fn new(limit: usize) -> Self {
        Self {
            value: String::with_capacity(limit),
            limit,
            truncated: false,
        }
    }

    fn finish(mut self) -> String {
        if self.truncated && self.limit >= TRUNCATION_MARKER.len() {
            while self.value.len() > self.limit - TRUNCATION_MARKER.len() {
                self.value.pop();
            }
            self.value.push_str(TRUNCATION_MARKER);
        }
        self.value
    }
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.truncated {
            return Err(fmt::Error);
        }
        let remaining = self.limit.saturating_sub(self.value.len());
        if value.len() <= remaining {
            self.value.push_str(value);
            return Ok(());
        }
        let mut boundary = remaining.min(value.len());
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.value.push_str(&value[..boundary]);
        self.truncated = true;
        Err(fmt::Error)
    }
}

fn bounded_text(value: &str, limit: usize) -> String {
    let mut bounded = BoundedText::new(limit);
    let _ = bounded.write_str(value);
    bounded.finish()
}

fn bounded_json_record<'a>(
    value: &serde_json::Map<String, serde_json::Value>,
    buffer: &'a mut [u8; MAX_LOG_RECORD_BYTES],
) -> &'a [u8] {
    let written = (|| {
        let capacity = buffer.len();
        let mut remaining = buffer.as_mut_slice();
        serde_json::to_writer(&mut remaining, value).ok()?;
        remaining.write_all(b"\n").ok()?;
        Some(capacity - remaining.len())
    })();
    written.map_or(TRUNCATED_RECORD, |written| &buffer[..written])
}

struct RetainedDailyWriter {
    file: File,
    log_dir: PathBuf,
    date: time::Date,
    at_capacity: bool,
    failed: bool,
    health: Arc<SinkHealth>,
    exit: Option<SyncSender<()>>,
}

impl RetainedDailyWriter {
    fn new(log_dir: &Path, health: Arc<SinkHealth>, exit: SyncSender<()>) -> Result<Self, String> {
        Self::new_at(
            log_dir,
            time::OffsetDateTime::now_utc().date(),
            health,
            Some(exit),
        )
    }

    fn new_at(
        log_dir: &Path,
        date: time::Date,
        health: Arc<SinkHealth>,
        exit: Option<SyncSender<()>>,
    ) -> Result<Self, String> {
        let file = open_daily_log(log_dir, date)?;
        prune_daily_logs(log_dir, MAX_LOG_FILES)?;
        Ok(Self {
            file,
            log_dir: log_dir.to_path_buf(),
            date,
            at_capacity: false,
            failed: false,
            health,
            exit,
        })
    }

    fn write_at(&mut self, date: time::Date, bytes: &[u8]) -> std::io::Result<usize> {
        if self.failed {
            self.health.record_discard();
            return Ok(bytes.len());
        }
        if bytes.len() > MAX_LOG_RECORD_BYTES {
            self.health.record_discard();
            return Ok(bytes.len());
        }
        if date != self.date {
            let rollover = (|| {
                let next = open_daily_log(&self.log_dir, date).map_err(std::io::Error::other)?;
                self.file.flush()?;
                prune_daily_logs(&self.log_dir, MAX_LOG_FILES).map_err(std::io::Error::other)?;
                Ok::<_, std::io::Error>(next)
            })();
            let Ok(next) = rollover else {
                self.disable_after_failure();
                return Ok(bytes.len());
            };
            self.file = next;
            self.date = date;
            self.at_capacity = false;
            self.health.clear_capacity();
        }
        if self.at_capacity {
            self.health.record_capacity_discard();
            return Ok(bytes.len());
        }
        match append_bounded_record(&mut self.file, bytes) {
            Ok(AppendOutcome::Written) => Ok(bytes.len()),
            Ok(AppendOutcome::AtCapacity) => {
                self.at_capacity = true;
                self.health.record_capacity_discard();
                Ok(bytes.len())
            }
            Err(_) => {
                self.disable_after_failure();
                Ok(bytes.len())
            }
        }
    }

    fn disable_after_failure(&mut self) {
        self.failed = true;
        self.health.record_failure();
    }
}

impl Drop for RetainedDailyWriter {
    fn drop(&mut self) {
        if let Some(exit) = self.exit.take() {
            let _ = exit.try_send(());
        }
    }
}

impl IoWrite for RetainedDailyWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write_at(time::OffsetDateTime::now_utc().date(), bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        if self.file.flush().is_err() {
            self.disable_after_failure();
        }
        Ok(())
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_at(time::OffsetDateTime::now_utc().date(), bytes)
            .map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AppendOutcome {
    Written,
    AtCapacity,
}

fn append_bounded_record(file: &mut File, bytes: &[u8]) -> std::io::Result<AppendOutcome> {
    lock_log_file(file)?;
    let result: std::io::Result<AppendOutcome> = (|| {
        let length = file.metadata()?.len();
        let bytes_u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length
            .checked_add(bytes_u64)
            .is_none_or(|next| next > MAX_LOG_FILE_BYTES)
        {
            return Ok(AppendOutcome::AtCapacity);
        }
        IoWrite::write_all(file, bytes)?;
        Ok(AppendOutcome::Written)
    })();
    let unlock = unlock_log_file(file);
    let outcome = result?;
    unlock?;
    Ok(outcome)
}

fn lock_log_file(file: &File) -> std::io::Result<()> {
    File::try_lock(file).map_err(std::io::Error::from)
}

fn unlock_log_file(file: &File) -> std::io::Result<()> {
    File::unlock(file)
}

fn open_daily_log(log_dir: &Path, date: time::Date) -> Result<File, String> {
    let path = log_dir.join(format!("{LOG_PREFIX}{date}"));
    let mut options = fs::OpenOptions::new();
    options.create_new(true).read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(OWNERSHIP_LINE)
                .and_then(|()| file.sync_all())
                .map_err(|error| format!("{}: {error}", path.display()))?;
            if is_safe_owned_file(&file) {
                Ok(file)
            } else {
                Err(format!(
                    "unowned diagnostics log collision {}",
                    path.display()
                ))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            open_safe_owned_log(&path)
        }
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

pub fn active_log_dir() -> Option<&'static Path> {
    ACTIVE_LOG_DIR.get().map(PathBuf::as_path)
}

fn prepare_directory(log_dir: &Path) -> Result<(), String> {
    match fs::symlink_metadata(log_dir) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(format!(
                "unsafe diagnostics directory {}",
                log_dir.display()
            ));
        }
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;

                // SAFETY: `geteuid` reads the effective UID and has no side effects.
                if metadata.uid() != unsafe { libc::geteuid() } {
                    return Err(format!(
                        "unsafe diagnostics directory {}",
                        log_dir.display()
                    ));
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(log_dir)
                .map_err(|error| format!("{}: {error}", log_dir.display()))?;
        }
        Err(error) => return Err(format!("{}: {error}", log_dir.display())),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(log_dir, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("{}: {error}", log_dir.display()))?;
    }
    Ok(())
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

fn prune_daily_logs(log_dir: &Path, keep: usize) -> Result<(), String> {
    let mut owned = Vec::new();
    for entry in bounded_directory_entries(log_dir)? {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_daily_log_name(&name) {
            continue;
        }
        if is_safe_owned_log(&entry.path()) {
            owned.push((name, entry.path()));
        }
    }
    owned.sort_by(|left, right| left.0.cmp(&right.0));
    let remove = owned.len().saturating_sub(keep);
    for (_, path) in owned.into_iter().take(remove) {
        fs::remove_file(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    Ok(())
}

fn reject_unowned_daily_entries(log_dir: &Path) -> Result<(), String> {
    for entry in bounded_directory_entries(log_dir)? {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if is_daily_log_name(&name) && !is_safe_owned_log(&entry.path()) {
            return Err(format!(
                "unowned diagnostics log collision {}",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

fn bounded_directory_entries(log_dir: &Path) -> Result<Vec<fs::DirEntry>, String> {
    let mut entries = Vec::with_capacity(MAX_LOG_SCAN_ENTRIES.min(32));
    for entry in fs::read_dir(log_dir).map_err(|error| error.to_string())? {
        if entries.len() == MAX_LOG_SCAN_ENTRIES {
            return Err(format!(
                "diagnostics directory exceeds the {MAX_LOG_SCAN_ENTRIES}-entry scan limit"
            ));
        }
        entries.push(entry.map_err(|error| error.to_string())?);
    }
    Ok(entries)
}

fn is_daily_log_name(name: &str) -> bool {
    let Some(date) = name.strip_prefix(LOG_PREFIX) else {
        return false;
    };
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) = (
        date[..4].parse::<i32>(),
        date[5..7].parse::<u8>(),
        date[8..].parse::<u8>(),
    ) else {
        return false;
    };
    let Ok(month) = time::Month::try_from(month) else {
        return false;
    };
    time::Date::from_calendar_date(year, month, day).is_ok()
}

fn is_safe_owned_log(path: &Path) -> bool {
    open_safe_owned_log(path).is_ok()
}

fn open_safe_owned_log(path: &Path) -> Result<File, String> {
    let mut options = fs::OpenOptions::new();
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if is_safe_owned_file(&file) {
        Ok(file)
    } else {
        Err(format!(
            "unowned diagnostics log collision {}",
            path.display()
        ))
    }
}

fn is_safe_owned_file(file: &File) -> bool {
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: `geteuid` reads the effective UID of this process and has no side effects.
        if metadata.nlink() != 1
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return false;
        }
    }
    let mut marker = Vec::with_capacity(OWNERSHIP_LINE.len());
    let Ok(mut reader) = file.try_clone() else {
        return false;
    };
    if reader.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    Read::by_ref(&mut reader)
        .take(OWNERSHIP_LINE.len() as u64)
        .read_to_end(&mut marker)
        .is_ok_and(|_| marker == OWNERSHIP_LINE)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    struct PanicDebug;

    impl fmt::Debug for PanicDebug {
        fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("rejected or unused fields must not be formatted")
        }
    }

    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl IoWrite for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("capture writer lock poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn prepare_current_log(root: &Path) -> Result<(), String> {
        open_daily_log(root, time::OffsetDateTime::now_utc().date()).map(|_| ())
    }

    fn prepared_current_log_path(root: &Path) -> PathBuf {
        prepare_current_log(root).expect("prepare current diagnostics log");
        fs::read_dir(root)
            .expect("current diagnostics entry")
            .map(|entry| entry.expect("diagnostics directory entry").path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(is_daily_log_name)
            })
            .expect("current diagnostics log path")
    }

    #[test]
    fn filter_precedence_uses_the_highest_priority_present_filter() {
        let filter = selected_filter(|name| match name {
            "LOXA_LOG" => Some("loxa=debug".into()),
            "RUST_LOG" => Some("loxa=trace".into()),
            _ => None,
        })
        .expect("valid highest-priority filter");
        assert_eq!(filter.to_string(), "loxa=debug");

        let filter = selected_filter(|name| (name == "RUST_LOG").then_some("loxa=trace".into()))
            .expect("valid RUST_LOG filter");
        assert_eq!(filter.to_string(), "loxa=trace");

        let filter = selected_filter(|_| None).expect("default filter");
        assert_eq!(filter.to_string(), DEFAULT_FILTER);
    }

    #[test]
    fn invalid_selected_filter_fails_without_falling_through() {
        let error = selected_filter(|name| match name {
            "LOXA_LOG" => Some("not a[filter".into()),
            "RUST_LOG" => Some("loxa=trace".into()),
            _ => None,
        })
        .unwrap_err();

        assert_eq!(error, "LOXA_LOG");

        let error = selected_filter(|name| (name == "RUST_LOG").then_some("not a[filter".into()))
            .unwrap_err();

        assert_eq!(error, "RUST_LOG");
    }

    #[test]
    fn diagnostics_worker_constructor_panics_become_setup_errors() {
        let error =
            nonfatal_constructor::<()>(|| panic!("synthetic constructor failure")).unwrap_err();

        assert_eq!(error, "failed to initialize diagnostics worker");
    }

    #[test]
    fn formatter_skips_reserved_and_span_debug_while_preserving_field_types() {
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer_output = Arc::clone(&output);
        let layer = tracing_subscriber::fmt::layer()
            .event_format(BoundedJsonFormatter)
            .fmt_fields(format::debug_fn(|_, _, _| Ok(())))
            .with_writer(move || CaptureWriter(Arc::clone(&writer_output)));
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("unused_span", unused = ?PanicDebug);
            let _entered = span.enter();
            tracing::info!(
                timestamp_ms = ?PanicDebug,
                signed = i128::MIN,
                unsigned = u128::MAX,
                count = 42_i64,
                ready = true
            );
        });

        let bytes = output.lock().unwrap();
        let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(record["timestamp_ms"].is_i64());
        assert_eq!(record["signed"], i128::MIN.to_string());
        assert_eq!(record["unsigned"], u128::MAX.to_string());
        assert_eq!(record["count"], 42);
        assert_eq!(record["ready"], true);
        assert_eq!(record["fields_truncated"], true);
    }

    #[test]
    fn retention_removes_only_old_regular_owned_logs() {
        let root = tempfile::tempdir().unwrap();
        for day in 1..=9 {
            let mut contents = OWNERSHIP_LINE.to_vec();
            contents.extend_from_slice(b"{\"event\":\"retained\"}\n");
            let path = root.path().join(format!("loxa.jsonl.2026-07-{day:02}"));
            fs::write(&path, contents).unwrap();
            #[cfg(unix)]
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::write(root.path().join("foreign.log"), b"foreign").unwrap();
        let foreign_daily = root.path().join("loxa.jsonl.2025-12-31");
        fs::write(&foreign_daily, b"foreign").unwrap();
        fs::write(root.path().join("loxa.jsonl.2026-13-01"), OWNERSHIP_LINE).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            root.path().join("foreign.log"),
            root.path().join("loxa.jsonl.2026-07-00"),
        )
        .unwrap();

        prune_daily_logs(root.path(), 7).unwrap();

        assert!(!root.path().join("loxa.jsonl.2026-07-01").exists());
        assert!(!root.path().join("loxa.jsonl.2026-07-02").exists());
        assert!(root.path().join("loxa.jsonl.2026-07-03").is_file());
        assert!(root.path().join("foreign.log").is_file());
        assert_eq!(fs::read(foreign_daily).unwrap(), b"foreign");
        assert!(root.path().join("loxa.jsonl.2026-13-01").is_file());
        #[cfg(unix)]
        assert!(root.path().join("loxa.jsonl.2026-07-00").is_symlink());
    }

    #[test]
    fn owned_log_with_an_event_reopens_without_replacing_its_contents() {
        let root = tempfile::tempdir().unwrap();
        let date = time::Date::from_calendar_date(2026, time::Month::July, 9).unwrap();
        let path = root.path().join(format!("{LOG_PREFIX}{date}"));
        let mut file = open_daily_log(root.path(), date).unwrap();
        file.write_all(b"{\"event\":\"ready\"}\n").unwrap();
        drop(file);

        let reopened = open_daily_log(root.path(), date).unwrap();
        drop(reopened);
        let contents = fs::read(path).unwrap();
        assert!(contents.starts_with(OWNERSHIP_LINE));
        assert!(contents.ends_with(b"{\"event\":\"ready\"}\n"));
    }

    #[test]
    fn unowned_daily_log_collision_is_refused_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let foreign = root.path().join("loxa.jsonl.2026-07-01");
        fs::write(&foreign, b"foreign").unwrap();

        assert!(reject_unowned_daily_entries(root.path()).is_err());
        assert_eq!(fs::read(foreign).unwrap(), b"foreign");
    }

    #[cfg(unix)]
    #[test]
    fn non_private_daily_log_is_refused_even_with_the_ownership_marker() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("loxa.jsonl.2026-07-01");
        fs::write(&path, OWNERSHIP_LINE).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o660)).unwrap();

        assert!(reject_unowned_daily_entries(root.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), OWNERSHIP_LINE);
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o660
        );
    }

    #[test]
    fn current_day_foreign_collision_is_refused_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let current = prepared_current_log_path(root.path());
        fs::remove_file(&current).unwrap();
        fs::write(&current, b"foreign").unwrap();

        assert!(prepare_current_log(root.path()).is_err());
        assert_eq!(fs::read(current).unwrap(), b"foreign");
    }

    #[test]
    fn rollover_refuses_a_foreign_next_day_without_writing_an_event() {
        let root = tempfile::tempdir().unwrap();
        let day_one = time::Date::from_calendar_date(2026, time::Month::August, 1).unwrap();
        let day_two = time::Date::from_calendar_date(2026, time::Month::August, 2).unwrap();
        let health = Arc::new(SinkHealth::default());
        let mut writer =
            RetainedDailyWriter::new_at(root.path(), day_one, Arc::clone(&health), None).unwrap();
        writer
            .write_at(day_one, b"{\"event\":\"day_one\"}\n")
            .unwrap();
        let next_path = root.path().join(format!("{LOG_PREFIX}{day_two}"));
        fs::write(&next_path, b"foreign").unwrap();

        let day_two_event = b"{\"event\":\"day_two\"}\n";
        assert_eq!(
            writer.write_at(day_two, day_two_event).unwrap(),
            day_two_event.len()
        );
        assert_eq!(fs::read(&next_path).unwrap(), b"foreign");
        assert!(
            fs::read_to_string(root.path().join(format!("{LOG_PREFIX}{day_one}")))
                .unwrap()
                .contains("day_one")
        );
        let snapshot = health.snapshot(0);
        assert!(snapshot.sink_failed);
        assert_eq!(snapshot.sink_failures, 1);
        assert_eq!(snapshot.sink_discards, 1);

        writer.write_at(day_one, b"discarded\n").unwrap();
        assert_eq!(health.snapshot(0).sink_discards, 2);
    }

    #[cfg(unix)]
    #[test]
    fn current_day_hard_link_with_marker_is_refused_without_mutation() {
        use std::os::unix::fs::MetadataExt;

        let root = tempfile::tempdir().unwrap();
        let current = prepared_current_log_path(root.path());
        fs::remove_file(&current).unwrap();
        let foreign = root.path().join("foreign");
        fs::write(&foreign, OWNERSHIP_LINE).unwrap();
        fs::hard_link(&foreign, &current).unwrap();

        assert!(prepare_current_log(root.path()).is_err());
        assert_eq!(fs::read(foreign).unwrap(), OWNERSHIP_LINE);
        assert_eq!(fs::metadata(current).unwrap().nlink(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn current_day_symlink_with_marker_is_refused_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let current = prepared_current_log_path(root.path());
        fs::remove_file(&current).unwrap();
        let foreign = root.path().join("foreign");
        fs::write(&foreign, OWNERSHIP_LINE).unwrap();
        std::os::unix::fs::symlink(&foreign, &current).unwrap();

        assert!(prepare_current_log(root.path()).is_err());
        assert_eq!(fs::read(foreign).unwrap(), OWNERSHIP_LINE);
        assert!(fs::symlink_metadata(current)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn diagnostics_directory_rejects_a_symlink_without_touching_its_target() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"foreign").unwrap();
        let logs = root.path().join("logs");
        std::os::unix::fs::symlink(&target, &logs).unwrap();

        assert!(prepare_directory(&logs).is_err());
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"foreign");
    }

    #[test]
    fn bounded_text_and_json_records_stop_before_their_byte_limits() {
        let value = "🙂".repeat(MAX_FIELD_VALUE_BYTES);
        let bounded = bounded_text(&value, MAX_FIELD_VALUE_BYTES);
        assert!(bounded.len() <= MAX_FIELD_VALUE_BYTES);
        assert!(bounded.ends_with("..."));

        let mut buffer = [0_u8; MAX_LOG_RECORD_BYTES];
        let mut oversized = serde_json::Map::new();
        oversized.insert("value".into(), "x".repeat(MAX_LOG_RECORD_BYTES).into());
        assert_eq!(
            bounded_json_record(&oversized, &mut buffer),
            TRUNCATED_RECORD
        );
    }

    #[test]
    fn daily_writer_drops_a_record_that_would_cross_the_file_cap() {
        let root = tempfile::tempdir().unwrap();
        let date = time::Date::from_calendar_date(2026, time::Month::August, 3).unwrap();
        let health = Arc::new(SinkHealth::default());
        let mut writer =
            RetainedDailyWriter::new_at(root.path(), date, Arc::clone(&health), None).unwrap();
        writer
            .file
            .set_len(MAX_LOG_FILE_BYTES - 2)
            .expect("prepare capped diagnostics file");

        assert_eq!(writer.write_at(date, b"abc\n").unwrap(), 4);
        assert_eq!(
            writer.file.metadata().unwrap().len(),
            MAX_LOG_FILE_BYTES - 2
        );
        let snapshot = health.snapshot(0);
        assert!(snapshot.at_capacity);
        assert!(!snapshot.sink_failed);
        assert_eq!(snapshot.sink_discards, 1);

        assert_eq!(writer.write_at(date, b"x\n").unwrap(), 2);
        assert_eq!(health.snapshot(0).sink_discards, 2);

        let next_date = time::Date::from_calendar_date(2026, time::Month::August, 4).unwrap();
        assert_eq!(writer.write_at(next_date, b"next day\n").unwrap(), 9);
        let recovered = health.snapshot(0);
        assert!(recovered.is_available());
        assert!(!recovered.is_healthy());
        assert_eq!(recovered.sink_discards, 2);
    }

    #[cfg(unix)]
    #[test]
    fn contended_log_lock_disables_the_sink_without_blocking() {
        let root = tempfile::tempdir().unwrap();
        let date = time::Date::from_calendar_date(2026, time::Month::August, 5).unwrap();
        let health = Arc::new(SinkHealth::default());
        let mut writer =
            RetainedDailyWriter::new_at(root.path(), date, Arc::clone(&health), None).unwrap();
        let path = root.path().join(format!("{LOG_PREFIX}{date}"));
        let holder = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .unwrap();
        lock_log_file(&holder).unwrap();

        let started = std::time::Instant::now();
        assert_eq!(writer.write_at(date, b"contended\n").unwrap(), 10);
        assert!(started.elapsed() < Duration::from_millis(100));
        let snapshot = health.snapshot(0);
        assert!(snapshot.sink_failed);
        assert_eq!(snapshot.sink_failures, 1);
        assert_eq!(snapshot.sink_discards, 1);

        unlock_log_file(&holder).unwrap();
    }

    #[test]
    fn writer_drop_signals_bounded_drain_completion() {
        let root = tempfile::tempdir().unwrap();
        let health = Arc::new(SinkHealth::default());
        let (exit_tx, exit_rx) = sync_channel(1);
        let appender = RetainedDailyWriter::new(root.path(), health, exit_tx).unwrap();
        let (mut writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1)
            .lossy(true)
            .finish(appender);
        writer.write_all(b"drained\n").unwrap();
        drop(writer);
        drop(guard);

        assert_eq!(exit_rx.recv_timeout(WRITER_EXIT_TIMEOUT), Ok(()));
    }

    #[test]
    fn incomplete_drain_is_reported_as_unhealthy() {
        let health = SinkHealth::default();
        health.record_incomplete_drain();

        let snapshot = health.snapshot(0);
        assert!(snapshot.drain_incomplete);
        assert!(!snapshot.is_healthy());
    }

    #[test]
    fn directory_scan_stops_at_its_fixed_entry_limit() {
        let root = tempfile::tempdir().unwrap();
        for index in 0..=MAX_LOG_SCAN_ENTRIES {
            fs::write(root.path().join(format!("entry-{index:03}")), b"").unwrap();
        }

        let error = bounded_directory_entries(root.path()).unwrap_err();
        assert!(error.contains("scan limit"), "{error}");
    }
}
