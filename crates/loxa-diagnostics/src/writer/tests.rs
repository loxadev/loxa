use super::*;
use crate::WRITER_EXIT_TIMEOUT;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::sync::mpsc::sync_channel;
use std::time::Duration;
use tracing_appender::non_blocking::NonBlockingBuilder;

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
fn contended_log_lock_drops_one_record_and_later_writes_resume() {
    let root = tempfile::tempdir().unwrap();
    let date = time::Date::from_calendar_date(2026, time::Month::August, 5).unwrap();
    let health = Arc::new(SinkHealth::default());
    let mut writer =
        RetainedDailyWriter::new_at(root.path(), date, Arc::clone(&health), None).unwrap();
    let path = root.path().join(format!("{LOG_PREFIX}{date}"));
    let holder = fs::OpenOptions::new()
        .read(true)
        .append(true)
        .open(&path)
        .unwrap();
    lock_log_file(&holder).unwrap();

    let started = std::time::Instant::now();
    assert_eq!(writer.write_at(date, b"contended\n").unwrap(), 10);
    assert!(started.elapsed() < Duration::from_millis(100));
    let snapshot = health.snapshot(0);
    assert!(!snapshot.sink_failed);
    assert_eq!(snapshot.sink_failures, 0);
    assert_eq!(snapshot.sink_discards, 1);
    assert!(snapshot.is_available());
    assert!(!snapshot.is_healthy());

    unlock_log_file(&holder).unwrap();
    assert_eq!(writer.write_at(date, b"resumed\n").unwrap(), 8);
    let contents = fs::read_to_string(path).unwrap();
    assert!(contents.ends_with("resumed\n"));
    assert!(!contents.contains("contended"));
    assert_eq!(health.snapshot(0).sink_discards, 1);
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
fn directory_scan_stops_at_its_fixed_entry_limit() {
    let root = tempfile::tempdir().unwrap();
    for index in 0..=MAX_LOG_SCAN_ENTRIES {
        fs::write(root.path().join(format!("entry-{index:03}")), b"").unwrap();
    }

    let error = bounded_directory_entries(root.path()).unwrap_err();
    assert!(error.contains("scan limit"), "{error}");
}
