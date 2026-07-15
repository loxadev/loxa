use loxa_core::diagnostics::{sanitize_field, DiagnosticsHealth, MAX_RECORD_BYTES};
use serde_json::{Map, Value};
use std::fmt;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

const REJECTED_FIELD_MARKER: &str = "__loxa_rejected_field";
const FALLBACK_REJECTED_CODE: &str = "diagnostics.field_rejected";
const FALLBACK_TRUNCATED_CODE: &str = "diagnostics.record_truncated";
const APPROVED_EVENT_CODES: &[&str] = &[
    "node.starting",
    "node.listening",
    "node.stopping",
    "node.stopped",
    "node.start_failed",
    "http.request.completed",
    "http.request.failed",
    "gateway.starting",
    "gateway.listening",
    "gateway.stop_requested",
    "gateway.stopped",
    "gateway.join_failed",
    "engine.spawn.started",
    "engine.spawn.succeeded",
    "engine.readiness.failed",
    "engine.exit.observed",
    "engine.teardown.confirmed",
    "engine.teardown.failed",
    "operation.started",
    "operation.terminal",
    "download.started",
    "download.terminal",
    "chat.turn.started",
    "chat.turn.terminal",
    "chat.turn.cancel_requested",
    "diagnostics.queue_dropped",
    "diagnostics.storage_degraded",
    "diagnostics.storage_recovered",
    "diagnostics.record_truncated",
    "diagnostics.field_rejected",
    "shutdown.requested",
    "shutdown.stage.completed",
    "shutdown.stage.failed",
    "shutdown.completed",
];

const APPROVED_COMPONENTS: &[&str] = &[
    "node",
    "http",
    "gateway",
    "engine",
    "supervisor",
    "operation",
    "download",
    "chat",
    "diagnostics",
    "shutdown",
];

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
pub(super) struct SafeJsonFields {
    health: DiagnosticsHealth,
    count_health: bool,
}

impl SafeJsonFields {
    pub(super) fn new(health: DiagnosticsHealth) -> Self {
        Self {
            health,
            count_health: true,
        }
    }

    pub(super) fn uncounted(health: DiagnosticsHealth) -> Self {
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

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {
        self.rejected = true;
        self.values.clear();
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
pub(super) struct SafeJsonFormatter {
    health: DiagnosticsHealth,
    count_health: bool,
}

impl SafeJsonFormatter {
    pub(super) fn new(health: DiagnosticsHealth) -> Self {
        health.support_records_truncated_counter();
        health.support_forbidden_field_rejections_counter();
        Self {
            health,
            count_health: true,
        }
    }

    pub(super) fn uncounted(health: DiagnosticsHealth) -> Self {
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
        let mut span_rejected = false;

        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let Some(formatted) = extensions.get::<FormattedFields<SafeJsonFields>>() else {
                    continue;
                };
                let span_fields: Map<String, Value> =
                    serde_json::from_str(&formatted.fields).unwrap_or_default();
                if span_fields.contains_key(REJECTED_FIELD_MARKER) {
                    span_rejected = true;
                    fields.clear();
                    break;
                }
                fields.extend(span_fields);
            }
        }

        let mut visitor = SafeFieldVisitor::default();
        event.record(&mut visitor);
        let envelope_rejected = !approved_event_envelope(&visitor.values);
        if (visitor.rejected || envelope_rejected) && self.count_health {
            self.health.increment_forbidden_field_rejections();
        }

        let correlation = if span_rejected {
            Map::new()
        } else {
            safe_span_correlation(&fields)
        };
        let rejected = span_rejected || visitor.rejected || envelope_rejected;

        let record = if rejected {
            fallback_record(&timestamp, FALLBACK_REJECTED_CODE, &correlation)
        } else {
            fields.extend(visitor.values);
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
                fallback_record(&timestamp, FALLBACK_TRUNCATED_CODE, &correlation)
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

fn approved_event_envelope(fields: &Map<String, Value>) -> bool {
    fields
        .get("event_code")
        .and_then(Value::as_str)
        .is_some_and(|value| APPROVED_EVENT_CODES.contains(&value))
        && fields
            .get("component")
            .and_then(Value::as_str)
            .is_some_and(|value| APPROVED_COMPONENTS.contains(&value))
}

fn safe_span_correlation(fields: &Map<String, Value>) -> Map<String, Value> {
    ["request_id", "method", "route"]
        .into_iter()
        .filter_map(|name| {
            fields
                .get(name)
                .cloned()
                .map(|value| (name.to_owned(), value))
        })
        .collect()
}

fn fallback_record(timestamp: &str, event_code: &str, correlation: &Map<String, Value>) -> String {
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
    record.extend(correlation.clone());
    let mut encoded = serde_json::to_string(&record).expect("static fallback record serializes");
    encoded.push('\n');
    encoded
}

#[cfg(any(test, debug_assertions))]
pub(super) fn debug_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::WARN)
        .with_target("loxa", LevelFilter::DEBUG)
}

#[cfg(any(test, not(debug_assertions)))]
pub(super) fn release_filter() -> Targets {
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
pub(super) fn executable_filter() -> Targets {
    debug_filter()
}

#[cfg(not(debug_assertions))]
pub(super) fn executable_filter() -> Targets {
    release_filter()
}
