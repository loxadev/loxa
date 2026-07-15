use loxa_core::diagnostics::{
    sanitize_field, BoundedJsonlWriter, DiagnosticsAvailability, DiagnosticsHealth,
    DiagnosticsHealthSnapshot, StorageConfig, SystemDiskSpace, LOG_QUEUE_CAPACITY,
    MAX_RECORD_BYTES,
};
use serde_json::{Map, Value};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

const REJECTED_FIELD_MARKER: &str = "__loxa_rejected_field";
const FALLBACK_REJECTED_CODE: &str = "diagnostics.field_rejected";
const FALLBACK_TRUNCATED_CODE: &str = "diagnostics.record_truncated";

const ALLOWED_FIELDS: &[&str] = &[
    "backend_kind",
    "chat_id",
    "component",
    "count",
    "duration_ms",
    "event_code",
    "exit_class",
    "generation",
    "latency_ms",
    "method",
    "model_id",
    "operation_id",
    "ready",
    "recipe_id",
    "request_id",
    "result_class",
    "route",
    "runtime_identity",
    "stage",
    "state",
    "status",
    "turn_id",
];

const FORBIDDEN_FIELD_PARTS: &[&str] = &[
    "authorization",
    "body",
    "command",
    "cookie",
    "credential",
    "environment",
    "error",
    "header",
    "path",
    "prompt",
    "response",
    "secret",
    "token",
    "uri",
];

#[derive(Clone)]
struct SafeJsonFields {
    health: DiagnosticsHealth,
    count_health: bool,
}

impl SafeJsonFields {
    fn new(health: DiagnosticsHealth) -> Self {
        Self {
            health,
            count_health: true,
        }
    }

    fn uncounted(health: DiagnosticsHealth) -> Self {
        Self {
            health,
            count_health: false,
        }
    }
}

impl<'writer> FormatFields<'writer> for SafeJsonFields {
    fn format_fields<R>(&self, mut writer: Writer<'writer>, fields: R) -> fmt::Result
    where
        R: tracing_subscriber::field::RecordFields,
    {
        let mut visitor = SafeFieldVisitor::default();
        fields.record(&mut visitor);
        if visitor.rejected {
            if self.count_health {
                self.health.increment_forbidden_field_rejections();
            }
            visitor.values.clear();
            visitor
                .values
                .insert(REJECTED_FIELD_MARKER.to_owned(), Value::Bool(true));
        }
        let encoded = serde_json::to_string(&visitor.values).map_err(|_| fmt::Error)?;
        writer.write_str(&encoded)
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> fmt::Result {
        let mut visitor = SafeFieldVisitor {
            values: serde_json::from_str(&current.fields).unwrap_or_default(),
            rejected: false,
        };
        fields.record(&mut visitor);
        if visitor.rejected {
            if self.count_health {
                self.health.increment_forbidden_field_rejections();
            }
            visitor.values.clear();
            visitor
                .values
                .insert(REJECTED_FIELD_MARKER.to_owned(), Value::Bool(true));
        }
        current.fields = serde_json::to_string(&visitor.values).map_err(|_| fmt::Error)?;
        Ok(())
    }
}

#[derive(Default)]
struct SafeFieldVisitor {
    values: Map<String, Value>,
    rejected: bool,
}

impl SafeFieldVisitor {
    fn record_value(&mut self, field: &Field, value: Value) {
        if self.rejected {
            return;
        }
        let name = field.name();
        if !field_name_allowed(name) {
            self.rejected = true;
            self.values.clear();
            return;
        }
        self.values.insert(name.to_owned(), value);
    }

    fn record_string(&mut self, field: &Field, value: &str) {
        if string_value_forbidden(field.name(), value) {
            self.rejected = true;
            self.values.clear();
            return;
        }
        self.record_value(
            field,
            Value::String(sanitize_field(value).as_str().to_owned()),
        );
    }
}

impl Visit for SafeFieldVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, Value::Bool(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        match serde_json::Number::from_f64(value) {
            Some(value) => self.record_value(field, Value::Number(value)),
            None => self.rejected = true,
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, Value::Number(value.into()));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        match i64::try_from(value) {
            Ok(value) => self.record_i64(field, value),
            Err(_) => self.rejected = true,
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        match u64::try_from(value) {
            Ok(value) => self.record_u64(field, value),
            Err(_) => self.rejected = true,
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_string(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if matches!(field.name(), "request_id" | "method" | "route") {
            self.record_string(field, &format!("{value:?}"));
        } else {
            self.rejected = true;
            self.values.clear();
        }
    }
}

fn field_name_allowed(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    ALLOWED_FIELDS.contains(&name)
        && !FORBIDDEN_FIELD_PARTS
            .iter()
            .any(|forbidden| lowered.contains(forbidden))
}

fn string_value_forbidden(field_name: &str, value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    value.chars().any(char::is_control)
        || lowered.contains("bearer ")
        || lowered.contains("../")
        || lowered.contains("..\\")
        || (field_name != "route" && value.starts_with('/'))
        || value.starts_with('\\')
        || value.contains('?')
        || lowered.contains("token=")
        || lowered.contains("secret=")
        || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

#[derive(Clone)]
struct SafeJsonFormatter {
    health: DiagnosticsHealth,
    count_health: bool,
}

impl SafeJsonFormatter {
    fn new(health: DiagnosticsHealth) -> Self {
        health.support_records_truncated_counter();
        health.support_forbidden_field_rejections_counter();
        Self {
            health,
            count_health: true,
        }
    }

    fn uncounted(health: DiagnosticsHealth) -> Self {
        Self {
            health,
            count_health: false,
        }
    }
}

impl<S> FormatEvent<S, SafeJsonFields> for SafeJsonFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, SafeJsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let timestamp = current_timestamp()?;
        let mut fields = Map::new();
        let mut rejected = false;

        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let Some(formatted) = extensions.get::<FormattedFields<SafeJsonFields>>() else {
                    continue;
                };
                let span_fields: Map<String, Value> =
                    serde_json::from_str(&formatted.fields).unwrap_or_default();
                if span_fields.contains_key(REJECTED_FIELD_MARKER) {
                    rejected = true;
                    break;
                }
                fields.extend(span_fields);
            }
        }

        let mut visitor = SafeFieldVisitor::default();
        event.record(&mut visitor);
        if visitor.rejected {
            if self.count_health {
                self.health.increment_forbidden_field_rejections();
            }
            rejected = true;
        } else {
            fields.extend(visitor.values);
        }

        let record = if rejected {
            fallback_record(&timestamp, FALLBACK_REJECTED_CODE)
        } else {
            let metadata = event.metadata();
            let mut record = Map::new();
            record.insert("timestamp".to_owned(), Value::String(timestamp.clone()));
            record.insert(
                "level".to_owned(),
                Value::String(metadata.level().as_str().to_owned()),
            );
            record.insert(
                "target".to_owned(),
                Value::String(sanitize_field(metadata.target()).as_str().to_owned()),
            );
            record.extend(fields);
            let mut encoded = serde_json::to_string(&record).map_err(|_| fmt::Error)?;
            encoded.push('\n');
            if encoded.len() > MAX_RECORD_BYTES {
                if self.count_health {
                    self.health.increment_records_truncated();
                }
                fallback_record(&timestamp, FALLBACK_TRUNCATED_CODE)
            } else {
                encoded
            }
        };

        debug_assert!(record.len() <= MAX_RECORD_BYTES);
        writer.write_str(&record)
    }
}

fn current_timestamp() -> Result<String, fmt::Error> {
    let mut timestamp = String::new();
    SystemTime.format_time(&mut Writer::new(&mut timestamp))?;
    Ok(timestamp)
}

fn fallback_record(timestamp: &str, event_code: &str) -> String {
    let mut record = Map::new();
    record.insert("timestamp".to_owned(), Value::String(timestamp.to_owned()));
    record.insert("level".to_owned(), Value::String("WARN".to_owned()));
    record.insert(
        "target".to_owned(),
        Value::String("loxa_node::diagnostics".to_owned()),
    );
    record.insert(
        "event_code".to_owned(),
        Value::String(event_code.to_owned()),
    );
    record.insert(
        "component".to_owned(),
        Value::String("diagnostics".to_owned()),
    );
    let mut encoded = serde_json::to_string(&record).expect("static fallback record serializes");
    encoded.push('\n');
    encoded
}

#[cfg(any(test, debug_assertions))]
fn debug_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::WARN)
        .with_target("loxa", LevelFilter::DEBUG)
}

#[cfg(any(test, not(debug_assertions)))]
fn release_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::WARN)
        .with_target("loxa_node::lifecycle", LevelFilter::INFO)
        .with_target("loxa_node::shutdown", LevelFilter::INFO)
        .with_target("loxa_node::diagnostics", LevelFilter::INFO)
        .with_target("loxa_core::gateway", LevelFilter::INFO)
        .with_target("loxa_core::supervisor", LevelFilter::INFO)
        .with_target("loxa_core::engine", LevelFilter::INFO)
        .with_target("loxa_core::operation", LevelFilter::INFO)
        .with_target("loxa_core::download", LevelFilter::INFO)
        .with_target("loxa_node::chat", LevelFilter::INFO)
}

#[cfg(debug_assertions)]
fn executable_filter() -> Targets {
    debug_filter()
}

#[cfg(not(debug_assertions))]
fn executable_filter() -> Targets {
    release_filter()
}

fn non_blocking_writer<W: Write + Send + 'static>(
    writer: W,
    capacity: usize,
) -> (NonBlocking, WorkerGuard, ErrorCounter) {
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(capacity)
        .lossy(true)
        .finish(writer);
    let errors = writer.error_counter();
    (writer, guard, errors)
}

struct StorageReporter<W> {
    inner: W,
    health: DiagnosticsHealth,
    degraded_episode: bool,
}

impl<W> StorageReporter<W> {
    fn new(inner: W, health: DiagnosticsHealth) -> Self {
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

pub struct DiagnosticsBootstrap {
    guard: Option<WorkerGuard>,
    health: DiagnosticsHealth,
    queue_errors: Option<ErrorCounter>,
}

impl DiagnosticsBootstrap {
    pub fn health(&self) -> DiagnosticsHealth {
        self.health.clone()
    }

    pub fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        let mut snapshot = self.health.snapshot();
        if let Some(errors) = &self.queue_errors {
            snapshot.queue_dropped =
                Some(u64::try_from(errors.dropped_lines()).unwrap_or(u64::MAX));
        }
        snapshot
    }
}

impl Drop for DiagnosticsBootstrap {
    fn drop(&mut self) {
        drop(self.guard.take());
    }
}

pub fn install_daemon_diagnostics(logs_dir: &Path) -> DiagnosticsBootstrap {
    let health = DiagnosticsHealth::new();

    let file_sink = fs::create_dir_all(logs_dir).and_then(|()| {
        BoundedJsonlWriter::new(
            StorageConfig::for_logs_dir(logs_dir),
            SystemDiskSpace,
            health.clone(),
        )
    });

    let (guard, queue_errors) = match file_sink {
        Ok(file_sink) => {
            health.support_queue_drop_counter();
            let stderr_layer = tracing_subscriber::fmt::layer()
                .fmt_fields(SafeJsonFields::uncounted(health.clone()))
                .event_format(SafeJsonFormatter::uncounted(health.clone()))
                .with_ansi(false)
                .with_writer(io::stderr)
                .with_filter(executable_filter());
            let reporter = StorageReporter::new(file_sink, health.clone());
            let (file_writer, guard, errors) = non_blocking_writer(reporter, LOG_QUEUE_CAPACITY);
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
            (Some(guard), Some(errors))
        }
        Err(_) => {
            eprintln!("loxa diagnostics: file logging unavailable; continuing with stderr");
            health.mark_unavailable();
            let stderr_layer = tracing_subscriber::fmt::layer()
                .fmt_fields(SafeJsonFields::new(health.clone()))
                .event_format(SafeJsonFormatter::new(health.clone()))
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
        queue_errors,
    }
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
mod tests {
    use super::*;
    use loxa_core::diagnostics::{DiagnosticsHealth, MAX_DYNAMIC_FIELD_BYTES, MAX_RECORD_BYTES};
    use serde_json::Value;
    use std::fmt;
    use std::io::{self, Write};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};
    use tracing::Level;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;

    #[derive(Clone, Default)]
    struct Capture {
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    struct CaptureWriter(Capture);

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .writes
                .lock()
                .expect("capture poisoned")
                .push(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(self.clone())
        }
    }

    impl Capture {
        fn text(&self) -> String {
            let writes = self.writes.lock().expect("capture poisoned");
            String::from_utf8(writes.concat()).expect("capture is UTF-8")
        }

        fn write_count(&self) -> usize {
            self.writes.lock().expect("capture poisoned").len()
        }
    }

    fn capture_events(filter: Targets, emit: impl FnOnce(), health: DiagnosticsHealth) -> Capture {
        let capture = Capture::default();
        let layer = tracing_subscriber::fmt::layer()
            .fmt_fields(SafeJsonFields::new(health.clone()))
            .event_format(SafeJsonFormatter::new(health))
            .with_ansi(false)
            .with_writer(capture.clone())
            .with_filter(filter);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, emit);
        capture
    }

    fn parse_single_record(capture: &Capture) -> Value {
        let text = capture.text();
        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count(), 1, "{text}");
        serde_json::from_str(text.trim_end()).expect("valid JSON record")
    }

    #[test]
    fn encodes_stable_fields_as_one_complete_jsonl_write() {
        let health = DiagnosticsHealth::new();
        let capture = capture_events(
            debug_filter(),
            || {
                tracing::event!(
                    target: "loxa_node::test",
                    Level::INFO,
                    event_code = "node.listening",
                    component = "node",
                    model_id = "public-model",
                    status = 200_u64,
                    ready = true,
                );
            },
            health,
        );

        let record = parse_single_record(&capture);
        assert_eq!(capture.write_count(), 1);
        assert_eq!(record["level"], "INFO");
        assert_eq!(record["target"], "loxa_node::test");
        assert_eq!(record["event_code"], "node.listening");
        assert_eq!(record["component"], "node");
        assert_eq!(record["model_id"], "public-model");
        assert_eq!(record["status"], 200);
        assert_eq!(record["ready"], true);
        assert!(record["timestamp"]
            .as_str()
            .is_some_and(|value| value.ends_with('Z')));
    }

    #[test]
    fn includes_safely_encoded_active_span_fields() {
        let health = DiagnosticsHealth::new();
        let capture = capture_events(
            debug_filter(),
            || {
                let span = tracing::info_span!(
                    target: "loxa_node::http",
                    "http.request",
                    request_id = "0123456789abcdef0123456789abcdef",
                    method = "GET",
                    route = "/v1/models/{id}",
                );
                let _entered = span.enter();
                tracing::event!(
                    target: "loxa_node::http",
                    Level::INFO,
                    event_code = "http.request.completed",
                    component = "http",
                    status = 200_u64,
                );
            },
            health,
        );

        let record = parse_single_record(&capture);
        assert_eq!(record["request_id"], "0123456789abcdef0123456789abcdef");
        assert_eq!(record["method"], "GET");
        assert_eq!(record["route"], "/v1/models/{id}");
    }

    #[test]
    fn truncates_dynamic_strings_on_the_shared_byte_bound() {
        let health = DiagnosticsHealth::new();
        let oversized = "x".repeat(MAX_DYNAMIC_FIELD_BYTES + 80);
        let capture = capture_events(
            debug_filter(),
            || {
                tracing::event!(
                    target: "loxa_node::test",
                    Level::INFO,
                    event_code = "operation.terminal",
                    component = "operation",
                    model_id = oversized.as_str(),
                );
            },
            health,
        );

        let record = parse_single_record(&capture);
        assert!(record["model_id"].as_str().unwrap().len() <= MAX_DYNAMIC_FIELD_BYTES);
        assert!(record["model_id"].as_str().unwrap().ends_with('\u{2026}'));
    }

    #[test]
    fn forbidden_names_reject_records_without_serializing_values() {
        let health = DiagnosticsHealth::new();
        let capture = capture_events(
            debug_filter(),
            || {
                tracing::event!(target: "loxa_node::test", Level::INFO, authorization = "Bearer AUTH_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, control_token = "CONTROL_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, credential = "CREDENTIAL_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, path = "/private/PATH_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, request_body = "BODY_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, prompt = "PROMPT_SECRET");
                tracing::event!(target: "loxa_node::test", Level::INFO, response = "RESPONSE_SECRET");
            },
            health.clone(),
        );

        let text = capture.text();
        for secret in [
            "AUTH_SECRET",
            "CONTROL_SECRET",
            "CREDENTIAL_SECRET",
            "PATH_SECRET",
            "BODY_SECRET",
            "PROMPT_SECRET",
            "RESPONSE_SECRET",
        ] {
            assert!(!text.contains(secret), "leaked {secret}: {text}");
        }
        assert_eq!(text.lines().count(), 7);
        assert!(text
            .lines()
            .all(|line| line.contains("diagnostics.field_rejected")));
        assert_eq!(health.snapshot().forbidden_field_rejections, Some(7));
    }

    #[test]
    fn malicious_values_and_arbitrary_debug_fields_are_rejected() {
        struct SecretDebug;
        impl fmt::Debug for SecretDebug {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("DEBUG_SECRET")
            }
        }

        let health = DiagnosticsHealth::new();
        let capture = capture_events(
            debug_filter(),
            || {
                for value in [
                    "Bearer BEARER_SECRET",
                    "/private/ABSOLUTE_SECRET",
                    "../TRAVERSAL_SECRET",
                    "model?token=QUERY_SECRET",
                    "safe\nNEWLINE_SECRET",
                ] {
                    tracing::event!(
                        target: "loxa_node::test",
                        Level::INFO,
                        event_code = "operation.terminal",
                        component = "operation",
                        model_id = value,
                    );
                }
                tracing::event!(
                    target: "loxa_node::test",
                    Level::INFO,
                    event_code = "operation.terminal",
                    component = "operation",
                    arbitrary = ?SecretDebug,
                );
            },
            health,
        );

        let text = capture.text();
        for secret in [
            "BEARER_SECRET",
            "ABSOLUTE_SECRET",
            "TRAVERSAL_SECRET",
            "QUERY_SECRET",
            "NEWLINE_SECRET",
            "DEBUG_SECRET",
        ] {
            assert!(!text.contains(secret), "leaked {secret}: {text}");
        }
        assert_eq!(text.lines().count(), 6);
    }

    #[test]
    fn complete_records_never_exceed_the_shared_cap() {
        let health = DiagnosticsHealth::new();
        let large = "x".repeat(MAX_DYNAMIC_FIELD_BYTES);
        let capture = capture_events(
            debug_filter(),
            || {
                tracing::event!(
                    target: "loxa_node::test",
                    Level::INFO,
                    event_code = large.as_str(), component = large.as_str(),
                    request_id = large.as_str(), runtime_identity = large.as_str(),
                    operation_id = large.as_str(), chat_id = large.as_str(), turn_id = large.as_str(),
                    model_id = large.as_str(), recipe_id = large.as_str(), route = large.as_str(),
                    method = large.as_str(), result_class = large.as_str(), backend_kind = large.as_str(),
                    exit_class = large.as_str(), stage = large.as_str(), state = large.as_str(),
                );
            },
            health,
        );

        assert!(capture.text().len() <= MAX_RECORD_BYTES);
    }

    #[test]
    fn debug_filter_accepts_loxa_debug_and_dependency_warnings_only() {
        let capture = capture_events(
            debug_filter(),
            || {
                tracing::event!(target: "loxa_node::test", Level::DEBUG, event_code = "loxa.debug", component = "test");
                tracing::event!(target: "dependency", Level::DEBUG, event_code = "dependency.debug", component = "test");
                tracing::event!(target: "dependency", Level::WARN, event_code = "dependency.warn", component = "test");
            },
            DiagnosticsHealth::new(),
        );

        let text = capture.text();
        assert!(text.contains("loxa.debug"));
        assert!(!text.contains("dependency.debug"));
        assert!(text.contains("dependency.warn"));
    }

    #[test]
    fn release_filter_is_target_restricted_and_env_cannot_loosen_it() {
        std::env::set_var("RUST_LOG", "trace");
        let capture = capture_events(
            release_filter(),
            || {
                tracing::event!(target: "loxa_node::lifecycle", Level::TRACE, event_code = "release.trace", component = "node");
                tracing::event!(target: "loxa_node::lifecycle", Level::DEBUG, event_code = "release.debug", component = "node");
                tracing::event!(target: "loxa_node::test", Level::INFO, event_code = "release.arbitrary_info", component = "test");
                tracing::event!(target: "loxa_node::lifecycle", Level::INFO, event_code = "release.operational_info", component = "node");
                tracing::event!(target: "dependency", Level::WARN, event_code = "release.warn", component = "test");
                tracing::event!(target: "dependency", Level::ERROR, event_code = "release.error", component = "test");
            },
            DiagnosticsHealth::new(),
        );
        std::env::remove_var("RUST_LOG");

        let text = capture.text();
        assert!(!text.contains("release.trace"));
        assert!(!text.contains("release.debug"));
        assert!(!text.contains("release.arbitrary_info"));
        assert!(text.contains("release.operational_info"));
        assert!(text.contains("release.warn"));
        assert!(text.contains("release.error"));
    }

    #[derive(Clone)]
    struct SlowWriter {
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Write for SlowWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let (lock, ready) = &*self.gate;
            let mut released = lock.lock().expect("gate poisoned");
            while !*released {
                released = ready.wait(released).expect("gate poisoned");
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn lossy_queue_counts_drops_without_blocking_producers() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (mut writer, guard, errors) = non_blocking_writer(
            SlowWriter {
                gate: Arc::clone(&gate),
            },
            1,
        );

        let started = Instant::now();
        for _ in 0..100 {
            writer.write_all(b"{}\n").unwrap();
        }
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(errors.dropped_lines() > 0);
        let health = DiagnosticsHealth::new();
        health.support_queue_drop_counter();
        let bootstrap = DiagnosticsBootstrap {
            guard: Some(guard),
            health,
            queue_errors: Some(errors.clone()),
        };
        assert_eq!(
            bootstrap.health_snapshot().queue_dropped,
            Some(u64::try_from(errors.dropped_lines()).unwrap())
        );
        let shared_health = bootstrap.health();
        shared_health.mark_stale();
        assert_eq!(
            bootstrap.health_snapshot().availability,
            loxa_core::diagnostics::DiagnosticsAvailability::Stale
        );

        let (lock, ready) = &*gate;
        *lock.lock().expect("gate poisoned") = true;
        ready.notify_all();
        drop(writer);
        drop(bootstrap);
    }

    #[test]
    fn dropping_guard_flushes_the_final_formatted_record() {
        let capture = Capture::default();
        let (writer, guard, _) = non_blocking_writer(CaptureWriter(capture.clone()), 16);
        let health = DiagnosticsHealth::new();
        let layer = tracing_subscriber::fmt::layer()
            .fmt_fields(SafeJsonFields::new(health.clone()))
            .event_format(SafeJsonFormatter::new(health))
            .with_ansi(false)
            .with_writer(writer);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "loxa_node::shutdown",
                Level::INFO,
                event_code = "shutdown.completed",
                component = "shutdown",
                result_class = "success",
            );
        });

        drop(guard);
        assert!(capture.text().contains("shutdown.completed"));
    }
}
