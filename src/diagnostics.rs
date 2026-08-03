use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

const DEFAULT_FILTER: &str = "loxa=info";
const LOG_PREFIX: &str = "loxa.jsonl.";
const MAX_LOG_FILES: usize = 8;
const OWNERSHIP_LINE: &[u8] = b"{\"event\":\"log_initialized\",\"application\":\"loxa\"}\n";

static ACTIVE_LOG_DIR: OnceLock<PathBuf> = OnceLock::new();

pub(crate) struct Diagnostics {
    _guard: WorkerGuard,
}

pub(crate) fn init(log_dir: &Path) -> Result<Diagnostics, String> {
    prepare_directory(log_dir)?;
    reject_unowned_daily_entries(log_dir)?;

    let (mut filter, invalid_source) = selected_filter(|name| std::env::var(name).ok());
    if invalid_source.is_some() {
        filter = filter.add_directive(
            "loxa::diagnostics=warn"
                .parse()
                .expect("static diagnostics directive is valid"),
        );
    }
    let appender = RetainedDailyWriter::new(log_dir)?;
    let (writer, guard) = NonBlockingBuilder::default().lossy(false).finish(appender);
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(filter)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target() == "loxa" || metadata.target().starts_with("loxa::")
        }));
    tracing_subscriber::registry()
        .with(layer)
        .try_init()
        .map_err(|error| format!("failed to initialize diagnostics: {error}"))?;

    let _ = ACTIVE_LOG_DIR.set(log_dir.to_path_buf());
    if let Some(source) = invalid_source {
        tracing::warn!(
            event = "invalid_log_filter",
            source,
            "ignored invalid log filter"
        );
    }
    Ok(Diagnostics { _guard: guard })
}

struct RetainedDailyWriter {
    file: File,
    log_dir: PathBuf,
    date: time::Date,
}

impl RetainedDailyWriter {
    fn new(log_dir: &Path) -> Result<Self, String> {
        Self::new_at(log_dir, time::OffsetDateTime::now_utc().date())
    }

    fn new_at(log_dir: &Path, date: time::Date) -> Result<Self, String> {
        let file = open_daily_log(log_dir, date)?;
        prune_daily_logs(log_dir, MAX_LOG_FILES)?;
        Ok(Self {
            file,
            log_dir: log_dir.to_path_buf(),
            date,
        })
    }

    fn write_at(&mut self, date: time::Date, bytes: &[u8]) -> std::io::Result<usize> {
        if date != self.date {
            let next = open_daily_log(&self.log_dir, date).map_err(std::io::Error::other)?;
            self.file.flush()?;
            self.file = next;
            self.date = date;
            prune_daily_logs(&self.log_dir, MAX_LOG_FILES).map_err(std::io::Error::other)?;
        }
        self.file.write(bytes)
    }
}

impl Write for RetainedDailyWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write_at(time::OffsetDateTime::now_utc().date(), bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
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

pub(crate) fn active_log_dir() -> Option<&'static Path> {
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
        Ok(_) => {}
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

fn selected_filter<F>(mut read: F) -> (EnvFilter, Option<&'static str>)
where
    F: FnMut(&str) -> Option<String>,
{
    let mut invalid_source = None;
    for name in ["LOXA_LOG", "RUST_LOG"] {
        if let Some(value) = read(name) {
            match EnvFilter::try_new(value) {
                Ok(filter) => return (filter, invalid_source),
                Err(_) => {
                    invalid_source.get_or_insert(if name == "LOXA_LOG" {
                        "LOXA_LOG"
                    } else {
                        "RUST_LOG"
                    });
                }
            }
        }
    }
    (EnvFilter::new(DEFAULT_FILTER), invalid_source)
}

fn prune_daily_logs(log_dir: &Path, keep: usize) -> Result<(), String> {
    let mut owned = Vec::new();
    for entry in fs::read_dir(log_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
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
    for entry in fs::read_dir(log_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
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

fn is_daily_log_name(name: &str) -> bool {
    let Some(date) = name.strip_prefix(LOG_PREFIX) else {
        return false;
    };
    let parts = date.split('-').collect::<Vec<_>>();
    let [year, month, day] = parts.as_slice() else {
        return false;
    };
    if year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) = (
        year.parse::<u32>(),
        month.parse::<u32>(),
        day.parse::<u32>(),
    ) else {
        return false;
    };
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&day)
}

fn is_safe_owned_log(path: &Path) -> bool {
    let mut options = fs::OpenOptions::new();
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let Ok(file) = options.open(path) else {
        return false;
    };
    is_safe_owned_file(&file)
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

        // SAFETY: `geteuid` reads the effective UID of this process and has no side effects.
        if metadata.nlink() != 1 || metadata.uid() != unsafe { libc::geteuid() } {
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
    let mut reader = BufReader::new(reader);
    reader
        .read_until(b'\n', &mut marker)
        .is_ok_and(|_| marker == OWNERSHIP_LINE)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn filter_precedence_and_invalid_fallback_are_deterministic() {
        let (filter, invalid) = selected_filter(|name| match name {
            "LOXA_LOG" => Some("loxa=debug".into()),
            "RUST_LOG" => Some("loxa=trace".into()),
            _ => None,
        });
        assert_eq!(filter.to_string(), "loxa=debug");
        assert_eq!(invalid, None);

        let (filter, invalid) =
            selected_filter(|name| (name == "LOXA_LOG").then_some("not a[filter".into()));
        assert_eq!(filter.to_string(), DEFAULT_FILTER);
        assert_eq!(invalid, Some("LOXA_LOG"));

        let (filter, invalid) = selected_filter(|name| match name {
            "LOXA_LOG" => Some("not a[filter".into()),
            "RUST_LOG" => Some("loxa=trace".into()),
            _ => None,
        });
        assert_eq!(filter.to_string(), "loxa=trace");
        assert_eq!(invalid, Some("LOXA_LOG"));
    }

    #[test]
    fn retention_removes_only_old_regular_owned_logs() {
        let root = tempfile::tempdir().unwrap();
        for day in 1..=9 {
            fs::write(
                root.path().join(format!("loxa.jsonl.2026-07-{day:02}")),
                OWNERSHIP_LINE,
            )
            .unwrap();
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
    fn unowned_daily_log_collision_is_refused_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let foreign = root.path().join("loxa.jsonl.2026-07-01");
        fs::write(&foreign, b"foreign").unwrap();

        assert!(reject_unowned_daily_entries(root.path()).is_err());
        assert_eq!(fs::read(foreign).unwrap(), b"foreign");
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
        let mut writer = RetainedDailyWriter::new_at(root.path(), day_one).unwrap();
        writer
            .write_at(day_one, b"{\"event\":\"day_one\"}\n")
            .unwrap();
        let next_path = root.path().join(format!("{LOG_PREFIX}{day_two}"));
        fs::write(&next_path, b"foreign").unwrap();

        assert!(writer
            .write_at(day_two, b"{\"event\":\"day_two\"}\n")
            .is_err());
        assert_eq!(fs::read(&next_path).unwrap(), b"foreign");
        assert!(
            fs::read_to_string(root.path().join(format!("{LOG_PREFIX}{day_one}")))
                .unwrap()
                .contains("day_one")
        );
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
}
