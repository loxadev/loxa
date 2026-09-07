use super::MAX_LOG_RECORD_BYTES;
use std::fmt::{self, Write as FmtWrite};
use std::io::Write as IoWrite;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::registry::LookupSpan;

const MAX_EVENT_FIELDS: usize = 24;
const EVENT_METADATA_FIELDS: usize = 3;
const MAX_FIELD_NAME_BYTES: usize = 64;
const MAX_FIELD_VALUE_BYTES: usize = 160;
const TRUNCATION_MARKER: &str = "...";
const TRUNCATED_RECORD: &[u8] =
    b"{\"event\":\"diagnostics_record_truncated\",\"level\":\"WARN\",\"target\":\"loxa\"}\n";

#[derive(Clone, Copy, Debug)]
pub(super) struct BoundedJsonFormatter;

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

#[cfg(test)]
mod tests;
