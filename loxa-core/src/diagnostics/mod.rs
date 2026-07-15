use serde::Serialize;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

pub mod child_retention;
pub mod storage;
pub use storage::{BoundedJsonlWriter, DiskSpace, StorageConfig, SystemDiskSpace};

pub const MAX_DYNAMIC_FIELD_BYTES: usize = 256;
pub const MAX_RECORD_BYTES: usize = 8 * 1024;
pub const LOG_QUEUE_CAPACITY: usize = 1_024;

const TRUNCATION_MARKER: &str = "\u{2026}";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SafeField(String);

impl SafeField {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SafeField {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for SafeField {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

pub fn sanitize_field(value: &str) -> SafeField {
    let mut sanitized = value
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>();

    if sanitized.len() > MAX_DYNAMIC_FIELD_BYTES {
        let mut boundary = MAX_DYNAMIC_FIELD_BYTES - TRUNCATION_MARKER.len();
        while !sanitized.is_char_boundary(boundary) {
            boundary -= 1;
        }
        sanitized.truncate(boundary);
        sanitized.push_str(TRUNCATION_MARKER);
    }

    SafeField(sanitized)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsAvailability {
    Available,
    Degraded,
    Unavailable,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DiagnosticsHealthSnapshot {
    pub availability: DiagnosticsAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_dropped: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_truncated: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forbidden_field_rejections: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_write_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low_disk_suppressions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_write: Option<SystemTime>,
}

#[derive(Default)]
struct MonotonicCounter {
    supported: AtomicBool,
    value: AtomicU64,
}

impl MonotonicCounter {
    fn support(&self) {
        self.supported.store(true, Ordering::Relaxed);
    }

    fn increment(&self) {
        self.support();
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            });
    }

    fn observe_total(&self, observed: u64) {
        self.support();
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (observed > current).then_some(observed)
            });
    }

    fn snapshot(&self) -> Option<u64> {
        self.supported
            .load(Ordering::Relaxed)
            .then(|| self.value.load(Ordering::Relaxed))
    }
}

struct AvailabilityState {
    availability: DiagnosticsAvailability,
    last_successful_write: Option<SystemTime>,
}

impl Default for AvailabilityState {
    fn default() -> Self {
        Self {
            availability: DiagnosticsAvailability::Unavailable,
            last_successful_write: None,
        }
    }
}

#[derive(Default)]
struct DiagnosticsHealthInner {
    availability: Mutex<AvailabilityState>,
    queue_dropped: MonotonicCounter,
    records_truncated: MonotonicCounter,
    forbidden_field_rejections: MonotonicCounter,
    storage_write_failures: MonotonicCounter,
    rotation_failures: MonotonicCounter,
    retention_failures: MonotonicCounter,
    low_disk_suppressions: MonotonicCounter,
}

#[derive(Clone, Default)]
pub struct DiagnosticsHealth {
    inner: Arc<DiagnosticsHealthInner>,
}

macro_rules! counter_methods {
    ($support:ident, $increment:ident, $field:ident) => {
        pub fn $support(&self) {
            self.inner.$field.support();
        }

        pub fn $increment(&self) {
            self.inner.$field.increment();
        }
    };
}

impl DiagnosticsHealth {
    pub fn new() -> Self {
        Self::default()
    }

    counter_methods!(
        support_queue_drop_counter,
        increment_queue_dropped,
        queue_dropped
    );

    pub fn observe_queue_dropped_total(&self, observed: u64) {
        self.inner.queue_dropped.observe_total(observed);
    }
    counter_methods!(
        support_records_truncated_counter,
        increment_records_truncated,
        records_truncated
    );
    counter_methods!(
        support_forbidden_field_rejections_counter,
        increment_forbidden_field_rejections,
        forbidden_field_rejections
    );
    counter_methods!(
        support_storage_write_failures_counter,
        increment_storage_write_failures,
        storage_write_failures
    );
    counter_methods!(
        support_rotation_failures_counter,
        increment_rotation_failures,
        rotation_failures
    );
    counter_methods!(
        support_retention_failures_counter,
        increment_retention_failures,
        retention_failures
    );
    counter_methods!(
        support_low_disk_suppressions_counter,
        increment_low_disk_suppressions,
        low_disk_suppressions
    );

    pub fn mark_degraded(&self) {
        self.availability_state().availability = DiagnosticsAvailability::Degraded;
    }

    pub fn mark_available_at(&self, successful_write: SystemTime) {
        let mut state = self.availability_state();
        state.availability = DiagnosticsAvailability::Available;
        state.last_successful_write = Some(successful_write);
    }

    pub fn mark_unavailable(&self) {
        self.availability_state().availability = DiagnosticsAvailability::Unavailable;
    }

    pub fn mark_stale(&self) {
        self.availability_state().availability = DiagnosticsAvailability::Stale;
    }

    pub fn snapshot(&self) -> DiagnosticsHealthSnapshot {
        let (availability, last_successful_write) = {
            let state = self.availability_state();
            (state.availability, state.last_successful_write)
        };
        DiagnosticsHealthSnapshot {
            availability,
            queue_dropped: self.inner.queue_dropped.snapshot(),
            records_truncated: self.inner.records_truncated.snapshot(),
            forbidden_field_rejections: self.inner.forbidden_field_rejections.snapshot(),
            storage_write_failures: self.inner.storage_write_failures.snapshot(),
            rotation_failures: self.inner.rotation_failures.snapshot(),
            retention_failures: self.inner.retention_failures.snapshot(),
            low_disk_suppressions: self.inner.low_disk_suppressions.snapshot(),
            last_successful_write,
        }
    }

    fn availability_state(&self) -> MutexGuard<'_, AvailabilityState> {
        self.inner
            .availability
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn sanitizes_ascii_within_the_dynamic_field_limit() {
        let safe = sanitize_field(&"a".repeat(MAX_DYNAMIC_FIELD_BYTES + 10));

        assert!(safe.len() <= MAX_DYNAMIC_FIELD_BYTES);
        assert!(safe.is_char_boundary(safe.len()));
        assert!(safe.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn sanitizes_unicode_on_a_character_boundary() {
        let value = "\u{1f642}".repeat(100);
        let safe = sanitize_field(&value);

        assert!(safe.len() <= MAX_DYNAMIC_FIELD_BYTES);
        assert!(safe.is_char_boundary(safe.len()));
        assert!(safe.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn removes_control_characters() {
        let safe = sanitize_field("model\nname\t\u{0}\u{7}");

        assert_eq!(safe.as_ref(), "modelname");
    }

    #[test]
    fn health_begins_unavailable_with_unsupported_counters() {
        let snapshot = DiagnosticsHealth::new().snapshot();

        assert_eq!(snapshot.availability, DiagnosticsAvailability::Unavailable);
        assert_eq!(snapshot.queue_dropped, None);
        assert_eq!(snapshot.records_truncated, None);
        assert_eq!(snapshot.forbidden_field_rejections, None);
        assert_eq!(snapshot.storage_write_failures, None);
        assert_eq!(snapshot.rotation_failures, None);
        assert_eq!(snapshot.retention_failures, None);
        assert_eq!(snapshot.low_disk_suppressions, None);
        assert_eq!(snapshot.last_successful_write, None);
    }

    #[test]
    fn missing_counter_is_not_zero() {
        let health = DiagnosticsHealth::new();
        assert_eq!(health.snapshot().queue_dropped, None);

        health.support_queue_drop_counter();

        assert_eq!(health.snapshot().queue_dropped, Some(0));
    }

    #[test]
    fn observed_queue_drop_totals_auto_support_and_never_decrease() {
        let health = DiagnosticsHealth::new();

        health.observe_queue_dropped_total(7);
        assert_eq!(health.snapshot().queue_dropped, Some(7));
        health.observe_queue_dropped_total(3);
        assert_eq!(health.snapshot().queue_dropped, Some(7));
        health.observe_queue_dropped_total(11);
        assert_eq!(health.snapshot().queue_dropped, Some(11));
    }

    #[test]
    fn increments_auto_support_counters_and_are_monotonic() {
        let health = DiagnosticsHealth::new();

        health.increment_records_truncated();
        let first = health.snapshot().records_truncated;
        health.increment_records_truncated();
        let second = health.snapshot().records_truncated;

        assert_eq!(first, Some(1));
        assert_eq!(second, Some(2));
        assert!(second >= first);
    }

    #[test]
    fn records_availability_transitions_and_injected_success_time() {
        let health = DiagnosticsHealth::new();
        let successful_write = SystemTime::UNIX_EPOCH + Duration::from_secs(42);

        health.mark_degraded();
        assert_eq!(
            health.snapshot().availability,
            DiagnosticsAvailability::Degraded
        );
        health.mark_available_at(successful_write);
        assert_eq!(
            health.snapshot().availability,
            DiagnosticsAvailability::Available
        );
        assert_eq!(
            health.snapshot().last_successful_write,
            Some(successful_write)
        );
        health.mark_stale();
        assert_eq!(
            health.snapshot().availability,
            DiagnosticsAvailability::Stale
        );
        health.mark_unavailable();
        assert_eq!(
            health.snapshot().availability,
            DiagnosticsAvailability::Unavailable
        );
    }

    #[test]
    fn unsupported_values_are_omitted_instead_of_serialized_as_zero() {
        let value = serde_json::to_value(DiagnosticsHealth::new().snapshot())
            .expect("serialize diagnostics health snapshot");

        assert_eq!(value["availability"], "unavailable");
        for field in [
            "queue_dropped",
            "records_truncated",
            "forbidden_field_rejections",
            "storage_write_failures",
            "rotation_failures",
            "retention_failures",
            "low_disk_suppressions",
            "last_successful_write",
        ] {
            assert!(value.get(field).is_none(), "unexpected field: {field}");
        }
    }
}
