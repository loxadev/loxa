use crate::service::coordinator::Coordinator;
use loxa_ipc::RuntimePhase;
use std::io::Read;
use std::os::fd::{AsRawFd as _, RawFd};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const MAX_SAMPLE_BYTES: usize = 64 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) async fn capture(coordinator: &Coordinator) -> String {
    let (pid, start_identity) = match ready_process(coordinator) {
        Ok(process) => process,
        Err(error) => return format!("engine thread sample unavailable: {error}"),
    };
    let sampled = tokio::task::spawn_blocking(move || sample_process(pid)).await;
    let output = match sampled {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return format!("engine thread sample unavailable: {error}"),
        Err(_) => return "engine thread sample unavailable: collector task failed".into(),
    };
    let (after_pid, after_start_identity) = match ready_process(coordinator) {
        Ok(process) => process,
        Err(error) => return format!("engine thread sample unavailable after capture: {error}"),
    };
    if after_pid != pid || after_start_identity != start_identity {
        return "engine thread sample unavailable: exact engine identity changed".into();
    }
    let text = match String::from_utf8(output.bytes) {
        Ok(text) => text,
        Err(_) => return "engine thread sample unavailable: output was not UTF-8".into(),
    };
    format!(
        "engine thread sample pid={pid} outcome={} bytes={}\n{text}",
        output.outcome,
        text.len(),
    )
}

fn ready_process(coordinator: &Coordinator) -> Result<(u32, u64), &'static str> {
    let RuntimePhase::Ready { engine_pid, .. } = coordinator.status().phase else {
        return Err("runtime was not Ready");
    };
    let snapshot = crate::process_inspection::process_snapshot(engine_pid)
        .map_err(|_| "process inspection failed")?
        .ok_or("engine process was absent")?;
    Ok((engine_pid, snapshot.start_identity))
}

struct SampleOutput {
    bytes: Vec<u8>,
    outcome: &'static str,
}

fn sample_process(pid: u32) -> Result<SampleOutput, &'static str> {
    let child = Command::new("/usr/bin/sample")
        .arg(pid.to_string())
        .args(["1", "10", "-file", "/dev/stdout"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| "collector could not start")?;
    let mut child = SampleChild::new(child);
    let result = collect_sample(&mut child);
    if result.is_err() {
        let _ = child.stop();
    }
    result
}

fn collect_sample(child: &mut SampleChild) -> Result<SampleOutput, &'static str> {
    let mut stdout = child
        .child
        .stdout
        .take()
        .ok_or("collector stdout was absent")?;
    let mut stderr = child
        .child
        .stderr
        .take()
        .ok_or("collector stderr was absent")?;
    set_nonblocking(stdout.as_raw_fd())?;
    set_nonblocking(stderr.as_raw_fd())?;

    let deadline = Instant::now() + SAMPLE_TIMEOUT;
    let mut bytes = Vec::with_capacity(MAX_SAMPLE_BYTES);
    loop {
        let overflow = drain_pipe(&mut stdout, &mut bytes)? | drain_pipe(&mut stderr, &mut bytes)?;
        if overflow {
            child.stop()?;
            return Ok(SampleOutput {
                bytes,
                outcome: "truncated",
            });
        }
        if let Some(status) = child.try_wait()? {
            let overflow =
                drain_pipe(&mut stdout, &mut bytes)? | drain_pipe(&mut stderr, &mut bytes)?;
            return Ok(SampleOutput {
                bytes,
                outcome: if overflow {
                    "truncated"
                } else if status.success() {
                    "complete"
                } else {
                    "failed"
                },
            });
        }
        if Instant::now() >= deadline {
            child.stop()?;
            let overflow =
                drain_pipe(&mut stdout, &mut bytes)? | drain_pipe(&mut stderr, &mut bytes)?;
            return Ok(SampleOutput {
                bytes,
                outcome: if overflow { "truncated" } else { "timed_out" },
            });
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn set_nonblocking(fd: RawFd) -> Result<(), &'static str> {
    // SAFETY: fcntl only reads and updates flags on the owned pipe descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err("collector pipe flags could not be read");
    }
    // SAFETY: the descriptor remains owned by the corresponding Child pipe.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err("collector pipe could not be made nonblocking");
    }
    Ok(())
}

fn drain_pipe(
    pipe: &mut (impl Read + ?Sized),
    captured: &mut Vec<u8>,
) -> Result<bool, &'static str> {
    let mut buffer = [0_u8; 4096];
    loop {
        match pipe.read(&mut buffer) {
            Ok(0) => return Ok(false),
            Ok(count) => {
                let remaining = MAX_SAMPLE_BYTES.saturating_sub(captured.len());
                let retained = remaining.min(count);
                captured.extend_from_slice(&buffer[..retained]);
                if retained != count {
                    return Ok(true);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(_) => return Err("collector pipe read failed"),
        }
    }
}

struct SampleChild {
    child: Child,
    reaped: bool,
}

impl SampleChild {
    fn new(child: Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, &'static str> {
        let status = self
            .child
            .try_wait()
            .map_err(|_| "collector status failed")?;
        if status.is_some() {
            self.reaped = true;
        }
        Ok(status)
    }

    fn stop(&mut self) -> Result<(), &'static str> {
        if self.reaped {
            return Ok(());
        }
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        if self.child.kill().is_err() && self.try_wait()?.is_none() {
            return Err("collector could not be killed");
        }
        self.child
            .wait()
            .map_err(|_| "collector could not be reaped")?;
        self.reaped = true;
        Ok(())
    }
}

impl Drop for SampleChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.reaped = true;
        }
    }
}
