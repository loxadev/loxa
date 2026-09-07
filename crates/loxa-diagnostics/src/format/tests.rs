use super::*;
use std::sync::Arc;
use tracing_subscriber::fmt::format;
use tracing_subscriber::prelude::*;

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
