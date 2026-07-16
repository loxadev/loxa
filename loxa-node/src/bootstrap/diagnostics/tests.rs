use super::*;
use loxa_core::diagnostics::{DiagnosticsHealth, MAX_DYNAMIC_FIELD_BYTES, MAX_RECORD_BYTES};
use serde_json::Value;
use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};
use tracing::Level;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

#[cfg(unix)]
#[test]
fn rejects_symlinked_logs_root_before_bootstrap_storage_creation() {
    use std::fs;
    use std::os::unix::fs::symlink;
    let root = std::env::temp_dir().join(format!(
        "loxa-bootstrap-diagnostics-symlink-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let outside = root.join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("sentinel"), b"keep").unwrap();
    let logs_link = root.join("logs");
    symlink(&outside, &logs_link).unwrap();
    let health = DiagnosticsHealth::new();

    let result = open_file_sink(&logs_link, health.clone());

    assert!(result.is_err());
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 1);
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
    assert_eq!(health.snapshot().storage_write_failures, Some(1));
    assert_eq!(
        health.snapshot().availability,
        loxa_core::diagnostics::DiagnosticsAvailability::Unavailable
    );
    fs::remove_file(logs_link).unwrap();
    fs::remove_dir_all(root).unwrap();
}

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
        .event_format(SafeJsonFormatter::new(health.clone()))
        .with_ansi(false)
        .with_writer(capture.clone())
        .with_filter(filter);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, emit);
    capture
}

fn capture_human_events(
    filter: Targets,
    emit: impl FnOnce(),
    health: DiagnosticsHealth,
) -> Capture {
    let capture = Capture::default();
    let (fields, formatter) = stderr_only_formatters(health);
    let layer = tracing_subscriber::fmt::layer()
        .fmt_fields(fields)
        .event_format(formatter)
        .with_ansi(false)
        .with_writer(capture.clone())
        .with_filter(filter);
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), emit);
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
fn debug_stderr_is_safe_human_readable_text() {
    let capture = capture_human_events(
        debug_filter(),
        || {
            tracing::event!(
                target: "loxa_node::test",
                Level::INFO,
                event_code = "node.listening",
                component = "node",
                model_id = "public-model",
                status = 200_u64,
            );
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    assert!(text.starts_with("INFO node.listening"), "{text}");
    assert!(text.contains("component=node"), "{text}");
    assert!(text.contains("model_id=public-model"), "{text}");
    assert!(text.contains("status=200"), "{text}");
    assert!(!text.trim_start().starts_with('{'), "{text}");
}

#[test]
fn debug_stderr_rejects_malicious_fields_without_rendering_secrets() {
    let capture = capture_human_events(
        debug_filter(),
        || {
            tracing::event!(
                target: "loxa_node::test",
                Level::INFO,
                event_code = "operation.terminal",
                component = "operation",
                authorization = "Bearer HUMAN_STDERR_SECRET",
            );
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    assert_eq!(
        text,
        "WARN diagnostics.field_rejected component=diagnostics\n"
    );
    assert!(!text.contains("HUMAN_STDERR_SECRET"));
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
fn debug_and_display_values_are_rejected_for_every_display_whitelisted_field() {
    struct Malicious(&'static str);
    impl fmt::Debug for Malicious {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "DEBUG_{}", self.0)
        }
    }
    impl fmt::Display for Malicious {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "DISPLAY_{}", self.0)
        }
    }

    let capture = capture_events(
        debug_filter(),
        || {
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                request_id = ?Malicious("REQUEST_ID"));
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                request_id = %Malicious("REQUEST_ID"));
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                method = ?Malicious("METHOD"));
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                method = %Malicious("METHOD"));
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                route = ?Malicious("ROUTE"));
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "operation",
                route = %Malicious("ROUTE"));
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    for secret in [
        "DEBUG_REQUEST_ID",
        "DISPLAY_REQUEST_ID",
        "DEBUG_METHOD",
        "DISPLAY_METHOD",
        "DEBUG_ROUTE",
        "DISPLAY_ROUTE",
    ] {
        assert!(!text.contains(secret), "leaked {secret}: {text}");
    }
    assert_eq!(text.lines().count(), 6);
    assert!(text
        .lines()
        .all(|line| line.contains("diagnostics.field_rejected")));
}

#[test]
fn missing_or_unapproved_event_envelopes_use_static_fallbacks() {
    let capture = capture_events(
        debug_filter(),
        || {
            tracing::event!(target: "loxa_node::test", Level::INFO,
                component = "operation", result_class = "success");
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", result_class = "success");
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "attacker.dynamic", component = "operation");
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "operation.terminal", component = "attacker");
            tracing::event!(target: "loxa_node::test", Level::INFO,
                event_code = "http.request.completed", component = "engine");
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    assert_eq!(text.lines().count(), 5);
    assert!(!text.contains("attacker.dynamic"));
    assert!(!text.contains("\"component\":\"attacker\""));
    assert!(!text.contains("\"component\":\"engine\""));
    assert!(text
        .lines()
        .all(|line| line.contains("diagnostics.field_rejected")));
}

#[test]
fn every_planned_event_code_component_pair_is_accepted() {
    let approved = [
        ("node.starting", "node"),
        ("node.listening", "node"),
        ("node.stopping", "node"),
        ("node.stopped", "node"),
        ("node.start_failed", "node"),
        ("node.identity_open_failed", "node"),
        ("identity_temp_cleanup_failed", "identity"),
        ("http.request.completed", "http"),
        ("http.request.failed", "http"),
        ("gateway.starting", "gateway"),
        ("gateway.listening", "gateway"),
        ("gateway.stop_requested", "gateway"),
        ("gateway.stopped", "gateway"),
        ("gateway.join_failed", "gateway"),
        ("engine.spawn.started", "engine"),
        ("engine.spawn.succeeded", "engine"),
        ("engine.readiness.failed", "engine"),
        ("engine.exit.observed", "engine"),
        ("engine.teardown.confirmed", "engine"),
        ("engine.teardown.failed", "engine"),
        ("operation.started", "operation"),
        ("operation.terminal", "operation"),
        ("download.started", "download"),
        ("download.terminal", "download"),
        ("chat.turn.started", "chat"),
        ("chat.turn.terminal", "chat"),
        ("chat.turn.cancel_requested", "chat"),
        ("diagnostics.queue_dropped", "diagnostics"),
        ("diagnostics.storage_degraded", "diagnostics"),
        ("diagnostics.storage_recovered", "diagnostics"),
        ("diagnostics.record_truncated", "diagnostics"),
        ("diagnostics.field_rejected", "diagnostics"),
        ("shutdown.requested", "shutdown"),
        ("shutdown.stage.completed", "shutdown"),
        ("shutdown.stage.failed", "shutdown"),
        ("shutdown.completed", "shutdown"),
    ];
    let approved_count = approved.len();
    let capture = capture_events(
        debug_filter(),
        || {
            for &(event_code, component) in &approved {
                tracing::event!(target: "loxa_node::test", Level::INFO, event_code, component);
            }
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    assert_eq!(text.lines().count(), approved_count);
    for (line, (event_code, component)) in text.lines().zip(approved) {
        let record: Value = serde_json::from_str(line).expect("valid approved record");
        assert_eq!(record["event_code"], event_code);
        assert_eq!(record["component"], component);
    }
}

#[test]
fn rejected_event_fallback_keeps_only_safe_span_correlation() {
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
            tracing::event!(target: "loxa_node::http", Level::INFO,
                event_code = "http.request.completed", component = "http",
                authorization = "Bearer EVENT_SECRET");
        },
        DiagnosticsHealth::new(),
    );

    let record = parse_single_record(&capture);
    assert_eq!(record["event_code"], "diagnostics.field_rejected");
    assert_eq!(record["request_id"], "0123456789abcdef0123456789abcdef");
    assert_eq!(record["method"], "GET");
    assert_eq!(record["route"], "/v1/models/{id}");
    assert!(!capture.text().contains("EVENT_SECRET"));
}

#[test]
fn rejected_span_is_omitted_from_static_fallback_context() {
    struct Malicious;
    impl fmt::Debug for Malicious {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("SPAN_SECRET")
        }
    }
    let capture = capture_events(
        debug_filter(),
        || {
            let span = tracing::info_span!(target: "loxa_node::http", "http.request",
                request_id = ?Malicious, method = "GET", route = "/v1/models/{id}");
            let _entered = span.enter();
            tracing::event!(target: "loxa_node::http", Level::INFO,
                event_code = "http.request.completed", component = "http",
                authorization = "Bearer EVENT_SECRET");
        },
        DiagnosticsHealth::new(),
    );

    let record = parse_single_record(&capture);
    assert_eq!(record["event_code"], "diagnostics.field_rejected");
    assert!(record.get("request_id").is_none());
    assert!(record.get("method").is_none());
    assert!(record.get("route").is_none());
    assert!(!capture.text().contains("SPAN_SECRET"));
    assert!(!capture.text().contains("EVENT_SECRET"));
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
                request_id = large.as_str(), node_id = large.as_str(),
                node_instance_id = large.as_str(),
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
fn production_identity_failures_keep_only_static_classes_without_health_rejection() {
    let health = DiagnosticsHealth::new();
    let capture = capture_events(
        release_filter(),
        || {
            tracing::event!(
                target: "loxa_node::lifecycle",
                Level::WARN,
                event_code = "node.identity_open_failed",
                component = "node",
                result_class = "failed",
                trigger_class = "identity_corrupt",
                cleanup_class = "owner_cleanup_failed",
            );
            tracing::event!(
                target: "loxa_node::identity::unix",
                Level::WARN,
                event_code = "identity_temp_cleanup_failed",
                component = "identity",
                result_class = "cleanup_failed",
            );
        },
        health.clone(),
    );

    let records = capture
        .text()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("valid production JSON"))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["event_code"], "node.identity_open_failed");
    assert_eq!(records[0]["component"], "node");
    assert_eq!(records[0]["result_class"], "failed");
    assert_eq!(records[0]["trigger_class"], "identity_corrupt");
    assert_eq!(records[0]["cleanup_class"], "owner_cleanup_failed");
    assert_eq!(records[1]["event_code"], "identity_temp_cleanup_failed");
    assert_eq!(records[1]["component"], "identity");
    assert_eq!(records[1]["result_class"], "cleanup_failed");
    for record in &records {
        assert!(record.as_object().unwrap().keys().all(|key| matches!(
            key.as_str(),
            "timestamp"
                | "level"
                | "target"
                | "event_code"
                | "component"
                | "result_class"
                | "trigger_class"
                | "cleanup_class"
        )));
    }
    assert_eq!(health.snapshot().forbidden_field_rejections, Some(0));
}

#[test]
fn deprecated_runtime_identity_field_is_rejected() {
    let health = DiagnosticsHealth::new();
    let capture = capture_events(
        release_filter(),
        || {
            tracing::event!(
                target: "loxa_node::lifecycle",
                Level::WARN,
                event_code = "node.start_failed",
                component = "node",
                runtime_identity = "deprecated-instance",
            );
        },
        health.clone(),
    );

    let record = parse_single_record(&capture);
    assert_eq!(record["event_code"], "diagnostics.field_rejected");
    assert!(record.get("runtime_identity").is_none());
    assert_eq!(health.snapshot().forbidden_field_rejections, Some(1));
}

#[test]
fn typed_node_identity_fields_are_allowlisted_scalars() {
    let capture = capture_events(
        debug_filter(),
        || {
            tracing::event!(
                target: "loxa_node::test",
                Level::INFO,
                event_code = "node.listening",
                component = "node",
                node_id = "123e4567-e89b-42d3-a456-426614174000",
                node_instance_id = "123e4567-e89b-42d3-b456-426614174001",
                result_class = "listening",
            );
        },
        DiagnosticsHealth::new(),
    );

    let record = parse_single_record(&capture);
    assert_eq!(record["event_code"], "node.listening");
    assert_eq!(record["node_id"], "123e4567-e89b-42d3-a456-426614174000");
    assert_eq!(
        record["node_instance_id"],
        "123e4567-e89b-42d3-b456-426614174001"
    );
}

#[test]
fn debug_filter_accepts_loxa_debug_and_dependency_warnings_only() {
    let capture = capture_events(
        debug_filter(),
        || {
            tracing::event!(target: "loxa_node::test", Level::DEBUG, event_code = "node.starting", component = "node");
            tracing::event!(target: "dependency", Level::DEBUG, event_code = "diagnostics.storage_degraded", component = "diagnostics");
            tracing::event!(target: "dependency", Level::WARN, event_code = "diagnostics.storage_recovered", component = "diagnostics");
        },
        DiagnosticsHealth::new(),
    );

    let text = capture.text();
    assert!(text.contains("node.starting"));
    assert!(!text.contains("diagnostics.storage_degraded"));
    assert!(text.contains("diagnostics.storage_recovered"));
}

#[test]
fn release_filter_is_target_restricted_and_env_cannot_loosen_it() {
    std::env::set_var("RUST_LOG", "trace");
    #[cfg(debug_assertions)]
    let filter = release_filter();
    #[cfg(not(debug_assertions))]
    let filter = executable_filter();
    let capture = capture_events(
        filter,
        || {
            tracing::event!(target: "loxa_node::lifecycle", Level::TRACE, event_code = "node.starting", component = "node", state = "trace");
            tracing::event!(target: "loxa_node::lifecycle", Level::DEBUG, event_code = "node.starting", component = "node", state = "debug");
            tracing::event!(target: "loxa_node::test", Level::INFO, event_code = "node.listening", component = "node", state = "arbitrary_info");
            tracing::event!(target: "loxa_node::lifecycle", Level::INFO, event_code = "node.listening", component = "node", state = "operational_info");
            tracing::event!(target: "dependency", Level::WARN, event_code = "diagnostics.storage_degraded", component = "diagnostics", state = "warn");
            tracing::event!(target: "dependency", Level::ERROR, event_code = "diagnostics.storage_degraded", component = "diagnostics", state = "error");
        },
        DiagnosticsHealth::new(),
    );
    std::env::remove_var("RUST_LOG");

    let text = capture.text();
    assert!(!text.contains("\"state\":\"trace\""));
    assert!(!text.contains("\"state\":\"debug\""));
    assert!(!text.contains("\"state\":\"arbitrary_info\""));
    assert!(text.contains("\"state\":\"operational_info\""));
    assert!(text.contains("\"state\":\"warn\""));
    assert!(text.contains("\"state\":\"error\""));
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
    let health = DiagnosticsHealth::new();
    let (mut writer, guard, progress) = non_blocking_writer_with_health(
        SlowWriter {
            gate: Arc::clone(&gate),
        },
        1,
        health.clone(),
    );

    let started = Instant::now();
    for _ in 0..100 {
        writer.write_all(b"{}\n").unwrap();
    }
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(progress.dropped_total() > 0);
    let bootstrap = DiagnosticsBootstrap {
        guard: Some(guard),
        health: health.clone(),
        queue_progress: Some(Arc::clone(&progress)),
    };
    assert_eq!(
        bootstrap.health_snapshot().queue_dropped,
        Some(progress.dropped_total())
    );
    let shared_health = bootstrap.health();
    let shared_queue_dropped = shared_health.snapshot().queue_dropped;
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
    assert_eq!(shared_queue_dropped, Some(progress.dropped_total()));
}

#[derive(Clone, Default)]
struct TestClock(Arc<AtomicU64>);

impl TestClock {
    fn advance(&self, duration: Duration) {
        self.0.fetch_add(
            u64::try_from(duration.as_millis()).unwrap(),
            Ordering::Release,
        );
    }
}

impl DropWarningClock for TestClock {
    fn elapsed(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::Acquire))
    }
}

#[derive(Clone, Default)]
struct WarningCapture(Arc<Mutex<Vec<String>>>);

impl BypassWarningSink for WarningCapture {
    fn write_warning(&self, warning: &str) -> io::Result<()> {
        self.0
            .lock()
            .expect("warning capture poisoned")
            .push(warning.to_owned());
        Ok(())
    }
}

impl WarningCapture {
    fn warnings(&self) -> Vec<String> {
        self.0.lock().expect("warning capture poisoned").clone()
    }
}

#[test]
fn real_queue_drops_emit_rate_limited_bypass_warnings_and_final_delta() {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let health = DiagnosticsHealth::new();
    let clock = TestClock::default();
    let warnings = WarningCapture::default();
    let reporter =
        DropWarningReporter::new_for_test(clock.clone(), warnings.clone(), Duration::from_secs(1));
    let (mut writer, guard, progress) = non_blocking_writer_with_reporter(
        SlowWriter {
            gate: Arc::clone(&gate),
        },
        1,
        health,
        reporter,
    );

    for _ in 0..100 {
        writer.write_all(b"first-wave\n").unwrap();
    }
    let first = warnings.warnings();
    assert_eq!(first.len(), 1, "{first:?}");
    let counts = first[0]
        .strip_prefix("loxa diagnostics: diagnostics.queue_dropped delta=")
        .and_then(|counts| counts.split_once(" total="))
        .map(|(delta, total)| (delta.parse::<u64>().unwrap(), total.parse::<u64>().unwrap()))
        .expect("stable numeric queue-drop warning");
    assert!(counts.0 > 0);
    assert!(counts.1 >= counts.0);
    assert!(first[0].len() < 128);

    for _ in 0..100 {
        writer.write_all(b"rate-limited-wave\n").unwrap();
    }
    assert_eq!(warnings.warnings().len(), 1);

    clock.advance(Duration::from_secs(1));
    progress.observe_drops();
    let periodic = warnings.warnings();
    assert_eq!(periodic.len(), 2, "{periodic:?}");
    assert!(periodic[1].contains("delta="));

    for _ in 0..100 {
        writer.write_all(b"final-wave\n").unwrap();
    }
    let before_shutdown = warnings.warnings().len();
    progress.begin_shutdown();
    let final_warnings = warnings.warnings();
    assert_eq!(
        final_warnings.len(),
        before_shutdown + 1,
        "{final_warnings:?}"
    );
    progress.finalize_drops();
    assert_eq!(warnings.warnings(), final_warnings);

    let (lock, ready) = &*gate;
    *lock.lock().expect("gate poisoned") = true;
    ready.notify_all();
    drop(writer);
    drop(guard);
}

#[derive(Clone, Default)]
struct FailingWarningSink(WarningCapture);

impl BypassWarningSink for FailingWarningSink {
    fn write_warning(&self, warning: &str) -> io::Result<()> {
        self.0.write_warning(warning)?;
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "injected closed stderr",
        ))
    }
}

#[test]
fn every_bypass_warning_class_ignores_closed_stderr() {
    let sink = FailingWarningSink::default();
    for warning in [
        FILE_LOGGING_UNAVAILABLE_WARNING,
        SUBSCRIBER_UNAVAILABLE_WARNING,
        STORAGE_DEGRADED_WARNING,
        STORAGE_RECOVERED_WARNING,
        SHUTDOWN_HELPER_UNAVAILABLE_WARNING,
    ] {
        write_bypass_warning_to(&sink, warning);
    }

    let warnings = sink.0.warnings();
    assert_eq!(warnings.len(), 5);
    assert!(warnings.iter().all(|warning| warning.len() < 160));
    assert!(warnings[0].contains("diagnostics.file_unavailable"));
    assert!(warnings[1].contains("diagnostics.subscriber_unavailable"));
    assert!(warnings[2].contains("diagnostics.storage_degraded"));
    assert!(warnings[3].contains("diagnostics.storage_recovered"));
    assert!(warnings[4].contains("diagnostics.shutdown_helper_unavailable"));
}

#[test]
fn storage_transition_bypass_uses_the_fallible_warning_sink() {
    let health = DiagnosticsHealth::new();
    let sink = FailingWarningSink::default();
    let mut reporter = StorageReporter::new_for_test(io::sink(), health.clone(), sink.clone());

    health.mark_degraded();
    reporter.write_all(b"degraded\n").unwrap();
    health.mark_available_at(std::time::SystemTime::UNIX_EPOCH);
    reporter.flush().unwrap();

    assert_eq!(
        sink.0.warnings(),
        vec![
            STORAGE_DEGRADED_WARNING.to_owned(),
            STORAGE_RECOVERED_WARNING.to_owned(),
        ]
    );
}

#[test]
fn stderr_only_rejected_record_increments_health_exactly_once() {
    let health = DiagnosticsHealth::new();
    let capture = capture_human_events(
        debug_filter(),
        || {
            tracing::event!(
                target: "loxa_node::test",
                Level::INFO,
                event_code = "operation.terminal",
                component = "operation",
                authorization = "Bearer COUNT_SECRET",
            );
        },
        health.clone(),
    );

    assert_eq!(
        capture.text(),
        "WARN diagnostics.field_rejected component=diagnostics\n"
    );
    assert_eq!(health.snapshot().forbidden_field_rejections, Some(1));
}

#[test]
fn stderr_only_rejected_span_increments_health_exactly_once() {
    let health = DiagnosticsHealth::new();
    let capture = capture_human_events(
        debug_filter(),
        || {
            let span = tracing::info_span!(
                target: "loxa_node::test",
                "operation",
                authorization = "Bearer SPAN_COUNT_SECRET",
            );
            let _entered = span.enter();
            tracing::event!(
                target: "loxa_node::test",
                Level::INFO,
                event_code = "operation.terminal",
                component = "operation",
            );
        },
        health.clone(),
    );

    assert_eq!(
        capture.text(),
        "WARN diagnostics.field_rejected component=diagnostics\n"
    );
    assert_eq!(health.snapshot().forbidden_field_rejections, Some(1));
}

#[test]
fn dropping_guard_flushes_the_final_formatted_record() {
    let capture = Capture::default();
    let health = DiagnosticsHealth::new();
    let (writer, guard, progress) =
        non_blocking_writer_with_health(CaptureWriter(capture.clone()), 16, health.clone());
    let layer = tracing_subscriber::fmt::layer()
        .fmt_fields(SafeJsonFields::new(health.clone()))
        .event_format(SafeJsonFormatter::new(health.clone()))
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

    let bootstrap = DiagnosticsBootstrap {
        guard: Some(guard),
        health,
        queue_progress: Some(progress),
    };
    let started = Instant::now();
    drop(bootstrap);
    assert!(started.elapsed() <= DIAGNOSTICS_SHUTDOWN_BOUND);
    assert!(capture.text().contains("shutdown.completed"));
}

#[test]
fn shutdown_waits_for_a_previously_open_admission_before_declaring_drain() {
    let capture = Capture::default();
    let health = DiagnosticsHealth::new();
    let (writer, worker_guard, progress) =
        non_blocking_writer_with_health(CaptureWriter(capture.clone()), 16, health.clone());
    let admission_reached = Arc::new(Barrier::new(2));
    let admission_release = Arc::new(Barrier::new(2));
    progress.pause_admission_for_test(
        Arc::clone(&admission_reached),
        Arc::clone(&admission_release),
    );

    let producer = std::thread::spawn(move || {
        let mut writer = writer;
        writer.write_all(b"admitted-before-close\n").unwrap();
    });
    admission_reached.wait();

    let (guard_started, guard_observed) = mpsc::sync_channel(1);
    let (shutdown_attempting_tx, shutdown_attempting_rx) = mpsc::sync_channel::<()>(1);
    progress.signal_shutdown_attempt_for_test(shutdown_attempting_tx);
    let bootstrap = DiagnosticsBootstrap {
        guard: Some(ShutdownGuard::test_action(move || {
            let _ = guard_started.send(());
            drop(worker_guard);
        })),
        health,
        queue_progress: Some(progress),
    };
    let shutdown_started = Instant::now();
    let shutdown = std::thread::spawn(move || drop(bootstrap));

    shutdown_attempting_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("shutdown attempts to close queue admission");
    let guard_started_before_admission_completed = guard_observed
        .recv_timeout(Duration::from_millis(50))
        .is_ok();
    admission_release.wait();
    producer.join().expect("producer joins");
    if !guard_started_before_admission_completed {
        guard_observed
            .recv_timeout(DIAGNOSTICS_SHUTDOWN_BOUND)
            .expect("guard starts after admitted write is processed");
    }
    shutdown.join().expect("shutdown joins");

    assert!(shutdown_started.elapsed() <= DIAGNOSTICS_SHUTDOWN_BOUND);
    assert!(capture.text().contains("admitted-before-close"));
    assert!(
        !guard_started_before_admission_completed,
        "shutdown declared the queue drained while an open admission was unaccounted"
    );
}

struct StalledWriter {
    state: Arc<(Mutex<(bool, bool)>, Condvar)>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl Write for StalledWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().expect("stall state poisoned");
        state.0 = true;
        ready.notify_all();
        while !state.1 {
            state = ready.wait(state).expect("stall state poisoned");
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for StalledWriter {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[test]
fn saturated_stalled_shutdown_is_bounded_without_invoking_worker_guard_drop() {
    let state = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let writer_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health = DiagnosticsHealth::new();
    let (mut writer, guard, progress) = non_blocking_writer_with_health(
        StalledWriter {
            state: Arc::clone(&state),
            dropped: Arc::clone(&writer_dropped),
        },
        1,
        health.clone(),
    );
    writer.write_all(b"first\n").unwrap();
    {
        let (lock, ready) = &*state;
        let mut stalled = lock.lock().expect("stall state poisoned");
        while !stalled.0 {
            stalled = ready.wait(stalled).expect("stall state poisoned");
        }
    }
    for _ in 0..100 {
        writer.write_all(b"queued\n").unwrap();
    }
    assert!(progress.dropped_total() > 0);
    drop(writer);
    let bootstrap = DiagnosticsBootstrap {
        guard: Some(guard),
        health,
        queue_progress: Some(progress),
    };

    let started = Instant::now();
    drop(bootstrap);
    assert!(started.elapsed() <= DIAGNOSTICS_SHUTDOWN_BOUND);

    let (lock, ready) = &*state;
    lock.lock().expect("stall state poisoned").1 = true;
    ready.notify_all();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !writer_dropped.load(std::sync::atomic::Ordering::Acquire),
        "upstream WorkerGuard::drop ran on the saturated queue"
    );
}
