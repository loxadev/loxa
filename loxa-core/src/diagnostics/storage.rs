use super::DiagnosticsHealth;
use getrandom::fill as fill_random;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{DiskRefreshKind, Disks};

pub const SEGMENT_BYTES: u64 = 1024 * 1024;
pub const SEGMENTS_PER_INCARNATION: usize = 4;
pub const INCARNATIONS_TO_KEEP: usize = 2;
pub const MIN_FREE_DISK_BYTES: u64 = 64 * 1024 * 1024;
const DISK_CHECK_INTERVAL_SECONDS: u64 = 1;
const DEGRADED_RETRY_SECONDS: u64 = 30;

pub trait DiskSpace: Clone + Send + Sync + 'static {
    fn available_bytes(&self, path: &Path) -> io::Result<u64>;
}

pub trait Clock: Send + Sync + 'static {
    fn monotonic_seconds(&self) -> u64;
    fn system_time(&self) -> SystemTime;
}

struct SystemClock {
    started: Instant,
    wall_started: SystemTime,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            wall_started: SystemTime::now(),
        }
    }
}

impl Clock for SystemClock {
    fn monotonic_seconds(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn system_time(&self) -> SystemTime {
        self.wall_started + self.started.elapsed()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDiskSpace;

impl DiskSpace for SystemDiskSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64> {
        let path = path.canonicalize()?;
        let disks =
            Disks::new_with_refreshed_list_specifics(DiskRefreshKind::nothing().with_storage());
        disks
            .iter()
            .filter(|disk| path.starts_with(disk.mount_point()))
            .max_by_key(|disk| disk.mount_point().components().count())
            .map(|disk| disk.available_space())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "log filesystem not found"))
    }
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    daemon_root: PathBuf,
    incarnation: String,
    segment_bytes: u64,
    segments_per_incarnation: usize,
}

trait SegmentFile: Write + Send {
    fn rollback(&mut self, length: u64) -> io::Result<()>;
}

impl SegmentFile for File {
    fn rollback(&mut self, length: u64) -> io::Result<()> {
        self.set_len(length)?;
        self.seek(SeekFrom::Start(length)).map(|_| ())
    }
}

impl StorageConfig {
    pub fn for_logs_dir(logs_dir: &Path) -> Self {
        Self {
            daemon_root: logs_dir.join("daemon"),
            incarnation: new_incarnation_key(),
            segment_bytes: SEGMENT_BYTES,
            segments_per_incarnation: SEGMENTS_PER_INCARNATION,
        }
    }

    #[cfg(test)]
    fn for_test(
        logs_dir: &Path,
        incarnation: &str,
        segment_bytes: u64,
        segments_per_incarnation: usize,
    ) -> Self {
        Self {
            daemon_root: logs_dir.join("daemon"),
            incarnation: incarnation.to_owned(),
            segment_bytes,
            segments_per_incarnation,
        }
    }
}

pub struct BoundedJsonlWriter<D: DiskSpace> {
    config: StorageConfig,
    disk: D,
    health: DiagnosticsHealth,
    clock: Arc<dyn Clock>,
    incarnation_dir: PathBuf,
    segment: Option<Box<dyn SegmentFile>>,
    segment_index: u64,
    segment_bytes: u64,
    last_disk_check: Option<u64>,
    degraded_until: Option<u64>,
    degraded_cause: Option<DegradedCause>,
}

#[derive(Clone, Copy)]
enum DegradedCause {
    LowDisk,
    Storage,
}

impl<D: DiskSpace> BoundedJsonlWriter<D> {
    pub fn new(config: StorageConfig, disk: D, health: DiagnosticsHealth) -> io::Result<Self> {
        Self::new_with_clock(config, disk, health, SystemClock::default())
    }

    fn new_with_clock<C: Clock>(
        config: StorageConfig,
        disk: D,
        health: DiagnosticsHealth,
        clock: C,
    ) -> io::Result<Self> {
        health.support_storage_write_failures_counter();
        health.support_rotation_failures_counter();
        health.support_retention_failures_counter();
        health.support_low_disk_suppressions_counter();
        if let Err(error) = ensure_daemon_root(&config.daemon_root) {
            storage_failure(&health);
            return Err(error);
        }
        prune_incarnations(&config.daemon_root, &config.incarnation, &health);
        let incarnation_dir = config.daemon_root.join(&config.incarnation);
        if let Err(error) = fs::create_dir(&incarnation_dir) {
            storage_failure(&health);
            return Err(error);
        }
        Ok(Self {
            config,
            disk,
            health,
            clock: Arc::new(clock),
            incarnation_dir,
            segment: None,
            segment_index: 0,
            segment_bytes: 0,
            last_disk_check: None,
            degraded_until: None,
            degraded_cause: None,
        })
    }

    pub fn incarnation_dir(&self) -> &Path {
        &self.incarnation_dir
    }

    fn open_next_segment(&mut self) -> io::Result<()> {
        if let Some(mut segment) = self.segment.take() {
            if let Err(error) = segment.flush() {
                rotation_failure(&self.health);
                return Err(error);
            }
        }
        if !prune_segments(
            &self.incarnation_dir,
            self.config.segments_per_incarnation.saturating_sub(1),
            &self.health,
        ) {
            return Err(io::Error::other("unable to make bounded segment capacity"));
        }
        loop {
            let path = self.incarnation_dir.join(segment_name(self.segment_index));
            self.segment_index = self.segment_index.saturating_add(1);
            match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(file) => {
                    self.segment = Some(Box::new(file));
                    self.segment_bytes = 0;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    rotation_failure(&self.health);
                    return Err(error);
                }
            }
        }
    }

    fn record_suppressed(&mut self, rotation_required: bool) -> bool {
        let now = self.clock.monotonic_seconds();
        if self.degraded_until.is_some_and(|retry_at| now < retry_at) {
            self.increment_degraded_suppression();
            return true;
        }

        let periodic_check_due = self
            .last_disk_check
            .is_none_or(|last| now.saturating_sub(last) >= DISK_CHECK_INTERVAL_SECONDS);
        if !rotation_required && self.degraded_until.is_none() && !periodic_check_due {
            return false;
        }

        self.last_disk_check = Some(now);
        match self.disk.available_bytes(&self.config.daemon_root) {
            Ok(bytes) if bytes >= MIN_FREE_DISK_BYTES => {
                self.degraded_until = None;
                false
            }
            Ok(_) => {
                self.enter_degraded(DegradedCause::LowDisk, now);
                self.health.increment_low_disk_suppressions();
                true
            }
            Err(_) => {
                self.enter_degraded(DegradedCause::Storage, now);
                self.health.increment_storage_write_failures();
                true
            }
        }
    }

    fn enter_degraded(&mut self, cause: DegradedCause, now: u64) {
        self.degraded_cause = Some(cause);
        self.degraded_until = Some(now.saturating_add(DEGRADED_RETRY_SECONDS));
        self.health.mark_degraded();
    }

    fn increment_degraded_suppression(&self) {
        match self.degraded_cause.unwrap_or(DegradedCause::Storage) {
            DegradedCause::LowDisk => self.health.increment_low_disk_suppressions(),
            DegradedCause::Storage => self.health.increment_storage_write_failures(),
        }
    }

    #[cfg(test)]
    fn replace_segment_for_test(&mut self, segment: Box<dyn SegmentFile>) {
        self.segment = Some(segment);
    }
}

impl<D: DiskSpace> Write for BoundedJsonlWriter<D> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if buf.len() as u64 > self.config.segment_bytes {
            self.health.increment_storage_write_failures();
            self.health.mark_degraded();
            return Ok(buf.len());
        }
        let rotation_required = self.segment.is_none()
            || self.segment_bytes.saturating_add(buf.len() as u64) > self.config.segment_bytes;
        if self.record_suppressed(rotation_required) {
            return Ok(buf.len());
        }
        if rotation_required && self.open_next_segment().is_err() {
            self.enter_degraded(DegradedCause::Storage, self.clock.monotonic_seconds());
            return Ok(buf.len());
        }
        let segment = self.segment.as_mut().expect("segment opened");
        match segment.write_all(buf) {
            Ok(()) => {
                self.segment_bytes += buf.len() as u64;
                self.degraded_cause = None;
                self.health.mark_available_at(self.clock.system_time());
            }
            Err(_) => {
                self.health.increment_storage_write_failures();
                if segment.rollback(self.segment_bytes).is_err() {
                    self.health.increment_storage_write_failures();
                    self.segment = None;
                }
                self.enter_degraded(DegradedCause::Storage, self.clock.monotonic_seconds());
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(segment) = self.segment.as_mut() {
            if segment.flush().is_err() {
                self.health.increment_storage_write_failures();
                self.enter_degraded(DegradedCause::Storage, self.clock.monotonic_seconds());
            }
        }
        Ok(())
    }
}

fn ensure_daemon_root(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "daemon log root is not a regular directory",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(path),
        Err(error) => Err(error),
    }
}

fn new_incarnation_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut random = [0_u8; 8];
    let _ = fill_random(&mut random);
    let random = u64::from_le_bytes(random);
    format!(
        "incarnation-{nanos:020}-{:010}-{random:016x}",
        std::process::id()
    )
}

fn is_incarnation_name(name: &str) -> bool {
    let Some(suffix) = name.strip_prefix("incarnation-") else {
        return false;
    };
    let mut parts = suffix.split('-');
    let (Some(nanos), Some(pid), Some(random), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    nanos.len() == 20
        && nanos.bytes().all(|byte| byte.is_ascii_digit())
        && pid.len() == 10
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && random.len() == 16
        && random.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn segment_name(index: u64) -> String {
    format!("segment-{index:020}.jsonl")
}

fn is_segment_name(name: &str) -> bool {
    name.strip_prefix("segment-")
        .and_then(|value| value.strip_suffix(".jsonl"))
        .is_some_and(|index| index.len() == 20 && index.bytes().all(|byte| byte.is_ascii_digit()))
}

fn prune_incarnations(root: &Path, current: &str, health: &DiagnosticsHealth) {
    let Ok(entries) = fs::read_dir(root) else {
        retention_failure(health);
        return;
    };
    let mut recognized = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            retention_failure(health);
            continue;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            retention_failure(health);
            continue;
        };
        let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
            retention_failure(health);
            continue;
        };
        if is_incarnation_name(name) && metadata.file_type().is_dir() && name != current {
            recognized.push((name.to_owned(), entry.path()));
        } else {
            retention_failure(health);
        }
    }
    recognized.sort_by(|left, right| right.0.cmp(&left.0));
    for (_, path) in recognized
        .into_iter()
        .skip(INCARNATIONS_TO_KEEP.saturating_sub(1))
    {
        remove_recognized_incarnation(&path, health);
    }
}

fn remove_recognized_incarnation(path: &Path, health: &DiagnosticsHealth) {
    let Ok(entries) = fs::read_dir(path) else {
        retention_failure(health);
        return;
    };
    let mut safe = true;
    for entry in entries {
        let Ok(entry) = entry else {
            retention_failure(health);
            safe = false;
            continue;
        };
        let name = entry.file_name();
        let metadata = fs::symlink_metadata(entry.path());
        if name.to_str().is_some_and(is_segment_name)
            && metadata
                .as_ref()
                .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            if fs::remove_file(entry.path()).is_err() {
                retention_failure(health);
                safe = false;
            }
        } else {
            retention_failure(health);
            safe = false;
        }
    }
    if safe && fs::remove_dir(path).is_err() {
        retention_failure(health);
    }
}

fn prune_segments(path: &Path, keep: usize, health: &DiagnosticsHealth) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        rotation_failure(health);
        return false;
    };
    let mut segments = Vec::new();
    let mut complete_scan = true;
    for entry in entries {
        let Ok(entry) = entry else {
            rotation_failure(health);
            complete_scan = false;
            continue;
        };
        let name = entry.file_name();
        let metadata = fs::symlink_metadata(entry.path());
        if name.to_str().is_some_and(is_segment_name)
            && metadata
                .as_ref()
                .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            segments.push(entry.path());
        } else {
            rotation_failure(health);
            if metadata.is_err() {
                complete_scan = false;
            }
        }
    }
    segments.sort();
    let remove_count = segments.len().saturating_sub(keep);
    let mut removed = 0;
    for path in segments.into_iter().take(remove_count) {
        if fs::remove_file(path).is_err() {
            rotation_failure(health);
        } else {
            removed += 1;
        }
    }
    complete_scan && remove_count == removed
}

fn retention_failure(health: &DiagnosticsHealth) {
    health.increment_retention_failures();
    health.mark_degraded();
}

fn rotation_failure(health: &DiagnosticsHealth) {
    health.increment_rotation_failures();
    health.mark_degraded();
}

fn storage_failure(health: &DiagnosticsHealth) {
    health.increment_storage_write_failures();
    health.mark_degraded();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::DiagnosticsHealth;
    use std::collections::VecDeque;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    const INCARNATION_1: &str = "incarnation-00000000000000000001-0000000001-0000000000000001";
    const INCARNATION_2: &str = "incarnation-00000000000000000002-0000000001-0000000000000002";
    const INCARNATION_3: &str = "incarnation-00000000000000000003-0000000001-0000000000000003";

    #[derive(Clone)]
    struct FakeDiskSpace {
        responses: Arc<Mutex<VecDeque<std::io::Result<u64>>>>,
        calls: Arc<AtomicU64>,
    }

    impl FakeDiskSpace {
        fn available() -> Self {
            Self::with_available(u64::MAX)
        }

        fn with_available(bytes: u64) -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from([Ok(bytes)]))),
                calls: Arc::new(AtomicU64::new(0)),
            }
        }

        fn failing() -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from([Err(std::io::Error::other(
                    "injected query failure",
                ))]))),
                calls: Arc::new(AtomicU64::new(0)),
            }
        }

        fn push(&self, response: std::io::Result<u64>) {
            let mut responses = self.responses.lock().unwrap();
            responses.clear();
            responses.push_back(response);
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl DiskSpace for FakeDiskSpace {
        fn available_bytes(&self, _path: &Path) -> std::io::Result<u64> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut responses = self.responses.lock().unwrap();
            if responses.len() > 1 {
                responses.pop_front().unwrap()
            } else {
                responses
                    .front()
                    .map(|result| {
                        result
                            .as_ref()
                            .copied()
                            .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
                    })
                    .unwrap_or(Ok(u64::MAX))
            }
        }
    }

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl FakeClock {
        fn advance(&self, duration: Duration) {
            self.0.fetch_add(duration.as_secs(), Ordering::Relaxed);
        }
    }

    impl Clock for FakeClock {
        fn monotonic_seconds(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }

        fn system_time(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(self.monotonic_seconds())
        }
    }

    enum FailureMode {
        Write,
        PartialThenWrite,
        PartialThenWriteAndRollback,
        Flush,
    }

    struct FailingSegment {
        mode: FailureMode,
        writes: usize,
        rolled_back: Arc<AtomicU64>,
        write_calls: Arc<AtomicU64>,
    }

    impl Write for FailingSegment {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.write_calls.fetch_add(1, Ordering::Relaxed);
            match self.mode {
                FailureMode::Write => Err(std::io::Error::other("injected write failure")),
                FailureMode::PartialThenWrite | FailureMode::PartialThenWriteAndRollback
                    if self.writes == 0 =>
                {
                    self.writes += 1;
                    Ok(buf.len().min(2))
                }
                FailureMode::PartialThenWrite | FailureMode::PartialThenWriteAndRollback
                    if self.writes == 1 =>
                {
                    self.writes += 1;
                    Err(std::io::Error::other("injected partial write failure"))
                }
                FailureMode::PartialThenWriteAndRollback | FailureMode::Flush => Ok(buf.len()),
                FailureMode::PartialThenWrite => {
                    Err(std::io::Error::other("injected partial write failure"))
                }
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            match self.mode {
                FailureMode::Flush => Err(std::io::Error::other("injected flush failure")),
                _ => Ok(()),
            }
        }
    }

    impl SegmentFile for FailingSegment {
        fn rollback(&mut self, _length: u64) -> std::io::Result<()> {
            self.rolled_back.fetch_add(1, Ordering::Relaxed);
            match self.mode {
                FailureMode::PartialThenWriteAndRollback => {
                    Err(std::io::Error::other("injected rollback failure"))
                }
                _ => Ok(()),
            }
        }
    }

    fn recognized_segments(dir: &Path) -> Vec<PathBuf> {
        let mut segments = fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".jsonl"))
            })
            .collect::<Vec<_>>();
        segments.sort();
        segments
    }

    #[test]
    fn rotates_before_the_segment_byte_limit() {
        let temp = TempDir::new().unwrap();
        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 32, 4);
        let mut writer =
            BoundedJsonlWriter::new(config, FakeDiskSpace::available(), DiagnosticsHealth::new())
                .unwrap();

        writer.write_all(b"{\"event_code\":\"a\"}\n").unwrap();
        writer.write_all(b"{\"event_code\":\"b\"}\n").unwrap();
        writer.flush().unwrap();

        let segments = recognized_segments(writer.incarnation_dir());
        assert_eq!(segments.len(), 2);
        assert!(segments
            .iter()
            .all(|path| fs::metadata(path).unwrap().len() <= 32));
        for path in segments {
            let bytes = fs::read(path).unwrap();
            assert!(bytes.ends_with(b"\n"));
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                serde_json::from_slice::<serde_json::Value>(line).unwrap();
            }
        }
    }

    #[test]
    fn retains_only_the_newest_segments() {
        let temp = TempDir::new().unwrap();
        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 24, 2);
        let mut writer =
            BoundedJsonlWriter::new(config, FakeDiskSpace::available(), DiagnosticsHealth::new())
                .unwrap();

        for code in ["a", "b", "c"] {
            writeln!(writer, "{{\"event_code\":\"{code}\"}}").unwrap();
        }
        writer.flush().unwrap();

        let segments = recognized_segments(writer.incarnation_dir());
        assert_eq!(segments.len(), 2);
        assert!(!segments
            .iter()
            .any(|path| path.ends_with("segment-00000000000000000000.jsonl")));
    }

    #[test]
    fn retains_current_and_newest_previous_recognized_incarnation() {
        let temp = TempDir::new().unwrap();
        let daemon = temp.path().join("daemon");
        fs::create_dir_all(daemon.join(INCARNATION_1)).unwrap();
        fs::create_dir_all(daemon.join(INCARNATION_2)).unwrap();
        fs::create_dir_all(daemon.join("incarnation-not-valid")).unwrap();

        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 32, 4);
        let writer =
            BoundedJsonlWriter::new(config, FakeDiskSpace::available(), DiagnosticsHealth::new())
                .unwrap();

        assert!(!daemon.join(INCARNATION_1).exists());
        assert!(daemon.join(INCARNATION_2).exists());
        assert!(daemon.join("incarnation-not-valid").exists());
        assert_eq!(writer.incarnation_dir(), daemon.join(INCARNATION_3));
    }

    #[cfg(unix)]
    #[test]
    fn skips_symlink_and_unrecognized_entries_during_pruning() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let daemon = temp.path().join("daemon");
        fs::create_dir_all(&daemon).unwrap();
        fs::write(outside.path().join("sentinel"), b"keep").unwrap();
        symlink(outside.path(), daemon.join("incarnation-0000")).unwrap();
        fs::write(daemon.join("unexpected"), b"keep").unwrap();

        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 32, 4);
        let health = DiagnosticsHealth::new();
        let _writer =
            BoundedJsonlWriter::new(config, FakeDiskSpace::available(), health.clone()).unwrap();

        assert!(daemon.join("incarnation-0000").symlink_metadata().is_ok());
        assert!(daemon.join("unexpected").exists());
        assert_eq!(fs::read(outside.path().join("sentinel")).unwrap(), b"keep");
        assert_eq!(health.snapshot().retention_failures, Some(2));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    fn writer_with(
        temp: &TempDir,
        disk: FakeDiskSpace,
        clock: FakeClock,
        health: DiagnosticsHealth,
    ) -> BoundedJsonlWriter<FakeDiskSpace> {
        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 64, 4);
        BoundedJsonlWriter::new_with_clock(config, disk, health, clock).unwrap()
    }

    #[test]
    fn accepts_writes_at_exactly_the_disk_reserve() {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let mut writer = writer_with(
            &temp,
            FakeDiskSpace::with_available(MIN_FREE_DISK_BYTES),
            FakeClock::default(),
            health.clone(),
        );

        writer.write_all(b"{\"event_code\":\"safe\"}\n").unwrap();
        writer.flush().unwrap();

        assert_eq!(recognized_segments(writer.incarnation_dir()).len(), 1);
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Available
        );
        assert_eq!(health.snapshot().storage_write_failures, Some(0));
        assert_eq!(health.snapshot().rotation_failures, Some(0));
        assert_eq!(health.snapshot().retention_failures, Some(0));
        assert_eq!(health.snapshot().low_disk_suppressions, Some(0));
    }

    #[test]
    fn suppresses_writes_below_the_disk_reserve() {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let record = b"{\"event_code\":\"safe\"}\n";
        let mut writer = writer_with(
            &temp,
            FakeDiskSpace::with_available(MIN_FREE_DISK_BYTES - 1),
            FakeClock::default(),
            health.clone(),
        );

        assert_eq!(writer.write(record).unwrap(), record.len());

        assert!(recognized_segments(writer.incarnation_dir()).is_empty());
        assert_eq!(health.snapshot().low_disk_suppressions, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[test]
    fn query_failure_degrades_and_consumes_the_record() {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let record = b"{\"event_code\":\"safe\"}\n";
        let mut writer = writer_with(
            &temp,
            FakeDiskSpace::failing(),
            FakeClock::default(),
            health.clone(),
        );

        assert_eq!(writer.write(record).unwrap(), record.len());

        assert!(recognized_segments(writer.incarnation_dir()).is_empty());
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[test]
    fn degraded_writes_are_gated_for_thirty_seconds_then_recover() {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let disk = FakeDiskSpace::with_available(MIN_FREE_DISK_BYTES - 1);
        let clock = FakeClock::default();
        let mut writer = writer_with(&temp, disk.clone(), clock.clone(), health.clone());
        let record = b"{\"event_code\":\"safe\"}\n";

        writer.write_all(record).unwrap();
        disk.push(Ok(MIN_FREE_DISK_BYTES));
        clock.advance(Duration::from_secs(29));
        writer.write_all(record).unwrap();
        assert_eq!(disk.calls(), 1);
        assert_eq!(recognized_segments(writer.incarnation_dir()).len(), 0);

        clock.advance(Duration::from_secs(1));
        writer.write_all(record).unwrap();
        writer.flush().unwrap();

        assert_eq!(disk.calls(), 2);
        assert_eq!(recognized_segments(writer.incarnation_dir()).len(), 1);
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Available
        );
        assert_eq!(
            health.snapshot().last_successful_write,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30))
        );
    }

    fn writer_with_failing_segment(
        mode: FailureMode,
    ) -> (
        TempDir,
        BoundedJsonlWriter<FakeDiskSpace>,
        DiagnosticsHealth,
        Arc<AtomicU64>,
        Arc<AtomicU64>,
        FakeClock,
    ) {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let clock = FakeClock::default();
        let mut writer = writer_with(
            &temp,
            FakeDiskSpace::available(),
            clock.clone(),
            health.clone(),
        );
        writer.write_all(b"{\"event_code\":\"open\"}\n").unwrap();
        let rolled_back = Arc::new(AtomicU64::new(0));
        let write_calls = Arc::new(AtomicU64::new(0));
        writer.replace_segment_for_test(Box::new(FailingSegment {
            mode,
            writes: 0,
            rolled_back: rolled_back.clone(),
            write_calls: write_calls.clone(),
        }));
        (temp, writer, health, rolled_back, write_calls, clock)
    }

    #[test]
    fn write_failure_is_consumed_and_degrades_storage() {
        let (_temp, mut writer, health, _, _, _) = writer_with_failing_segment(FailureMode::Write);
        let record = b"{\"event_code\":\"fail\"}\n";

        assert_eq!(writer.write(record).unwrap(), record.len());
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[test]
    fn partial_write_is_rolled_back_and_consumed() {
        let (_temp, mut writer, health, rolled_back, _, _) =
            writer_with_failing_segment(FailureMode::PartialThenWrite);
        let record = b"{\"event_code\":\"partial\"}\n";

        assert_eq!(writer.write(record).unwrap(), record.len());
        assert_eq!(rolled_back.load(Ordering::Relaxed), 1);
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
    }

    #[test]
    fn flush_failure_is_consumed_and_degrades_storage() {
        let (_temp, mut writer, health, _, _, _) = writer_with_failing_segment(FailureMode::Flush);

        assert!(writer.flush().is_ok());
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[test]
    fn directory_creation_failure_is_reported_to_health() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("daemon"), b"not a directory").unwrap();
        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 64, 4);
        let health = DiagnosticsHealth::new();

        let result = BoundedJsonlWriter::new(config, FakeDiskSpace::available(), health.clone());

        assert!(result.is_err());
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlinked_daemon_root_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        fs::write(outside.path().join("sentinel"), b"keep").unwrap();
        symlink(outside.path(), temp.path().join("daemon")).unwrap();
        let config = StorageConfig::for_test(temp.path(), INCARNATION_3, 64, 4);
        let health = DiagnosticsHealth::new();

        let result = BoundedJsonlWriter::new(config, FakeDiskSpace::available(), health.clone());

        assert!(result.is_err());
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 1);
        assert_eq!(fs::read(outside.path().join("sentinel")).unwrap(), b"keep");
        assert_eq!(health.snapshot().storage_write_failures, Some(1));
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Degraded
        );
    }

    #[test]
    fn rollback_failure_abandons_the_corrupt_segment_before_recovery() {
        let (_temp, mut writer, health, rolled_back, write_calls, clock) =
            writer_with_failing_segment(FailureMode::PartialThenWriteAndRollback);
        let record = b"{\"event_code\":\"partial\"}\n";

        assert_eq!(writer.write(record).unwrap(), record.len());
        assert_eq!(rolled_back.load(Ordering::Relaxed), 1);
        assert_eq!(health.snapshot().storage_write_failures, Some(2));

        clock.advance(Duration::from_secs(DEGRADED_RETRY_SECONDS));
        assert_eq!(writer.write(record).unwrap(), record.len());
        writer.flush().unwrap();

        assert_eq!(write_calls.load(Ordering::Relaxed), 2);
        assert_eq!(recognized_segments(writer.incarnation_dir()).len(), 2);
        assert_eq!(
            health.snapshot().availability,
            crate::diagnostics::DiagnosticsAvailability::Available
        );
    }

    #[test]
    fn one_pruning_failure_is_counted_once_by_rotation() {
        let temp = TempDir::new().unwrap();
        let health = DiagnosticsHealth::new();
        let mut writer = writer_with(
            &temp,
            FakeDiskSpace::available(),
            FakeClock::default(),
            health.clone(),
        );
        fs::remove_dir(writer.incarnation_dir()).unwrap();
        let record = b"{\"event_code\":\"safe\"}\n";

        assert_eq!(writer.write(record).unwrap(), record.len());

        assert_eq!(health.snapshot().rotation_failures, Some(1));
    }
}
