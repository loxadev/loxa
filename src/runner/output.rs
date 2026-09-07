#[cfg(all(test, unix))]
use super::child::LAST_GUARDED_GROUP;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

pub(super) const MAX_DIAGNOSTIC_TAIL: usize = 4096;
const MAX_ANNOUNCEMENT_LINE: usize = 8192;
pub(super) const MAX_PENDING_ANNOUNCEMENTS: usize = 64;

#[cfg(test)]
pub(super) enum ReaderSpawnFault {
    Fail,
    Panic,
}

#[cfg(test)]
static READER_SPAWN_FAULT: std::sync::Mutex<Option<ReaderSpawnFault>> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(super) struct ReaderSpawnFaultReset;

#[cfg(test)]
impl Drop for ReaderSpawnFaultReset {
    fn drop(&mut self) {
        *READER_SPAWN_FAULT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
pub(super) fn install_reader_spawn_fault_for_test(
    fault: ReaderSpawnFault,
) -> ReaderSpawnFaultReset {
    #[cfg(unix)]
    LAST_GUARDED_GROUP.store(0, Ordering::SeqCst);
    *READER_SPAWN_FAULT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(fault);
    ReaderSpawnFaultReset
}

#[cfg(test)]
fn inject_reader_spawn_fault_for_test() -> Result<(), String> {
    let fault = READER_SPAWN_FAULT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    let Some(fault) = fault else {
        return Ok(());
    };
    match fault {
        ReaderSpawnFault::Fail => Err("injected output reader spawn failure".into()),
        ReaderSpawnFault::Panic => panic!("injected output reader spawn panic"),
    }
}

pub(super) fn spawn_reader_thread<T, F>(
    name: &'static str,
    read: F,
) -> Result<std::thread::JoinHandle<T>, String>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    #[cfg(test)]
    inject_reader_spawn_fault_for_test()?;
    std::thread::Builder::new()
        .name(name.into())
        .spawn(read)
        .map_err(|error| error.to_string())
}

pub(super) fn validate_announcement_line(line: &str) -> Result<u16, String> {
    if !line.contains("listening") {
        return Err("line is not a listening announcement".into());
    }
    let candidates = line
        .split_ascii_whitespace()
        .map(|part| {
            part.trim_matches(|character: char| {
                matches!(character, ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}')
            })
        })
        .filter(|part| part.contains("://"))
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Err(format!(
            "listening announcement must contain exactly one URL, found {}",
            candidates.len()
        ));
    }
    let url = reqwest::Url::parse(candidates[0])
        .map_err(|error| format!("invalid listening URL: {error}"))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("listening URL must be an exact loopback HTTP endpoint".into());
    }
    let port = url
        .port()
        .ok_or_else(|| "listening URL must include an explicit port".to_string())?;
    if port == 0 {
        return Err("listening URL port must be nonzero".into());
    }
    Ok(port)
}

type AnnouncementOutput = (mpsc::SyncSender<Result<u16, String>>, Arc<AtomicBool>);

pub(super) fn spawn_output_reader<R>(
    mut reader: R,
    announcement_output: Option<AnnouncementOutput>,
) -> Result<std::thread::JoinHandle<Result<Vec<u8>, String>>, String>
where
    R: std::io::Read + Send + 'static,
{
    spawn_reader_thread("loxa-server-output", move || {
        let mut diagnostic_tail = VecDeque::with_capacity(MAX_DIAGNOSTIC_TAIL);
        let mut line = Vec::new();
        let mut line_too_long = false;
        let mut buffer = [0_u8; 4096];
        loop {
            let count = reader
                .read(&mut buffer)
                .map_err(|error| error.to_string())?;
            if count == 0 {
                if let Some((announcements, overflow)) = &announcement_output {
                    if !line.is_empty() || line_too_long {
                        publish_announcement(&line, line_too_long, announcements, overflow);
                    }
                }
                return Ok(diagnostic_tail.into_iter().collect());
            }
            for &byte in &buffer[..count] {
                if diagnostic_tail.len() == MAX_DIAGNOSTIC_TAIL {
                    diagnostic_tail.pop_front();
                }
                diagnostic_tail.push_back(byte);
                if let Some((announcements, overflow)) = &announcement_output {
                    if byte == b'\n' {
                        publish_announcement(&line, line_too_long, announcements, overflow);
                        line.clear();
                        line_too_long = false;
                    } else if line.len() < MAX_ANNOUNCEMENT_LINE {
                        line.push(byte);
                    } else {
                        line_too_long = true;
                    }
                }
            }
        }
    })
}

fn publish_announcement(
    line: &[u8],
    line_too_long: bool,
    announcements: &mpsc::SyncSender<Result<u16, String>>,
    overflow: &AtomicBool,
) {
    let line = String::from_utf8_lossy(line);
    if !line.contains("listening") {
        return;
    }
    let announcement = if line_too_long {
        Err(format!(
            "listening announcement exceeds {MAX_ANNOUNCEMENT_LINE} bytes"
        ))
    } else {
        validate_announcement_line(&line)
    };
    if let Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) =
        announcements.try_send(announcement)
    {
        overflow.store(true, Ordering::SeqCst);
    }
}

pub(super) fn join_output_reader(
    reader: &mut Option<std::thread::JoinHandle<Result<Vec<u8>, String>>>,
    tail: &mut Vec<u8>,
) -> Result<(), String> {
    let Some(reader) = reader.take() else {
        return Ok(());
    };
    *tail = reader
        .join()
        .map_err(|_| "llama-server output reader panicked".to_string())??;
    Ok(())
}
