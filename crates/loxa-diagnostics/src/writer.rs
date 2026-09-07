use super::{SinkHealth, MAX_LOG_RECORD_BYTES};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

const LOG_PREFIX: &str = "loxa.jsonl.";
// Retention is approximately 32 MiB per role after pruning.
const MAX_LOG_FILES: usize = 8;
const MAX_LOG_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LOG_SCAN_ENTRIES: usize = 256;
const OWNERSHIP_LINE: &[u8] = b"{\"event\":\"log_initialized\",\"application\":\"loxa\"}\n";

pub(super) struct RetainedDailyWriter {
    file: File,
    log_dir: PathBuf,
    date: time::Date,
    at_capacity: bool,
    failed: bool,
    health: Arc<SinkHealth>,
    exit: Option<SyncSender<()>>,
}

impl RetainedDailyWriter {
    pub(super) fn new(
        log_dir: &Path,
        health: Arc<SinkHealth>,
        exit: SyncSender<()>,
    ) -> Result<Self, String> {
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

pub(super) fn prepare_directory(log_dir: &Path) -> Result<(), String> {
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

pub(super) fn reject_unowned_daily_entries(log_dir: &Path) -> Result<(), String> {
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
mod tests;
