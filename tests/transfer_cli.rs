use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(unix)]
use std::process::{Child, ExitStatus, Stdio};
#[cfg(unix)]
use std::time::{Duration, Instant};

const MODEL_BYTES: &[u8] = b"abcdef";
const PART_BYTES: &[u8] = b"abc";

fn manifest(id: &str) -> loxa::catalog::Manifest {
    loxa::catalog::Manifest {
        version: 1,
        id: id.into(),
        repo: Some("owner/repo".into()),
        revision: Some("a".repeat(40)),
        remote_filename: Some("exact.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
        size: MODEL_BYTES.len() as u64,
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn model_dir(home: &Path, id: &str) -> PathBuf {
    home.join("models").join(id)
}

fn seed_pending(home: &Path, id: &str) -> PathBuf {
    let directory = model_dir(home, id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join(".lock"), b"").unwrap();
    std::fs::write(
        directory.join("pending.json"),
        serde_json::to_vec_pretty(&manifest(id)).unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join("model.gguf.part"), PART_BYTES).unwrap();
    directory
}

fn seed_installed(home: &Path, id: &str) -> PathBuf {
    let directory = model_dir(home, id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join(".lock"), b"").unwrap();
    std::fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest(id)).unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join("model.gguf"), MODEL_BYTES).unwrap();
    directory
}

fn isolated_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loxa"));
    command
        .env("LOXA_HOME", home)
        .env("HOME", home.join("process-home"))
        .env("USERPROFILE", home.join("process-userprofile"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env_remove("CLICOLOR_FORCE")
        .env_remove("FORCE_COLOR")
        .env_remove("HF_TOKEN")
        .env_remove("HUGGING_FACE_HUB_TOKEN");
    command
}

fn run(home: &Path, args: &[&str]) -> Output {
    isolated_command(home)
        .args(args)
        .output()
        .expect("run isolated loxa child")
}

fn snapshot(directory: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

#[cfg(unix)]
struct PtySession {
    child: Child,
    master: File,
    transcript: Vec<u8>,
}

#[cfg(unix)]
impl PtySession {
    fn spawn(home: &Path, args: &[&str]) -> Self {
        let mut master = -1;
        let mut slave = -1;
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let master = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        assert_ne!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) },
            -1
        );
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags, -1);
        assert_ne!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            -1
        );
        let mut command = isolated_command(home);
        command.args(args);
        command.stdin(Stdio::from(slave.try_clone().unwrap()));
        command.stdout(Stdio::from(slave.try_clone().unwrap()));
        command.stderr(Stdio::from(slave));
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY.into(), 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Self {
            child: command.spawn().expect("spawn PTY child"),
            master,
            transcript: Vec::new(),
        }
    }

    fn drain(&mut self) {
        let mut buffer = [0_u8; 4096];
        loop {
            match self.master.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => self.transcript.extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => panic!("read PTY: {error}"),
            }
        }
    }

    fn poll(&mut self) {
        let mut descriptor = libc::pollfd {
            fd: self.master.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert_ne!(unsafe { libc::poll(&mut descriptor, 1, 50) }, -1);
        self.drain();
    }

    fn terminate_and_reap(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }

    fn wait_for(&mut self, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.poll();
            if String::from_utf8_lossy(&self.transcript).contains(needle) {
                return;
            }
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    panic!(
                        "PTY child exited before {needle:?}: {}",
                        String::from_utf8_lossy(&self.transcript)
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    self.terminate_and_reap();
                    panic!("PTY child poll failed before {needle:?}: {error}");
                }
            }
            if Instant::now() >= deadline {
                self.terminate_and_reap();
                panic!(
                    "PTY child timed out before {needle:?}: {}",
                    String::from_utf8_lossy(&self.transcript)
                );
            }
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        self.master.write_all(bytes).unwrap();
    }

    fn finish(mut self) -> (ExitStatus, String) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.poll();
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.drain();
                    return (
                        status,
                        String::from_utf8_lossy(&self.transcript).into_owned(),
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    self.terminate_and_reap();
                    panic!("PTY child poll failed: {error}");
                }
            }
            if Instant::now() >= deadline {
                self.terminate_and_reap();
                panic!(
                    "PTY child timed out: {}",
                    String::from_utf8_lossy(&self.transcript)
                );
            }
        }
    }
}

#[cfg(unix)]
impl Drop for PtySession {
    fn drop(&mut self) {
        self.terminate_and_reap();
    }
}

#[test]
fn discard_help_exposes_only_required_id_and_yes_without_resume_cancel_jobs_or_picker() {
    let root = tempfile::tempdir().expect("temporary process environment");
    let output = run(&root.path().join("loxa-home"), &["discard", "--help"]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(output.stderr, b"");
    let help = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    assert!(
        help.contains("Usage: loxa discard [OPTIONS] <ID>"),
        "{help}"
    );
    assert!(help.contains("--yes"), "{help}");
    for excluded in ["resume", "cancel", "job", "picker", "model selection"] {
        assert!(!help.to_ascii_lowercase().contains(excluded), "{help}");
    }
}

#[test]
fn discard_missing_id_exits_2_before_paths_recovery_or_state() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let output = run(&home, &["discard"]);

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("required arguments were not provided"),
        "{stderr}"
    );
    assert!(stderr.contains("<ID>"), "{stderr}");
    assert!(!home.join("models").exists());
}

#[test]
fn noninteractive_discard_without_yes_exits_1_without_prepare_consume_or_mutation() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let output = run(&home, &["discard", "demo"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("non-interactive discard requires --yes"),
        "{stderr}"
    );
    assert!(!home.join("models").exists());
}

#[cfg(unix)]
#[test]
fn discard_default_no_decline_exits_0_and_drops_candidate_without_consume() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_pending(&home, "demo");
    let before = snapshot(&directory);
    let mut session = PtySession::spawn(&home, &["discard", "demo"]);
    session.wait_for("Discard incomplete download demo?");
    session.write(b"\n");
    let (status, output) = session.finish();

    assert_eq!(status.code(), Some(0), "{output:?}");
    assert_eq!(snapshot(&directory), before);
}

#[cfg(unix)]
#[test]
fn discard_prompt_sigint_exits_130_without_consume_or_mutation() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_pending(&home, "demo");
    let before = snapshot(&directory);
    let mut session = PtySession::spawn(&home, &["discard", "demo"]);
    session.wait_for("Discard incomplete download demo?");
    assert_eq!(
        unsafe { libc::kill(session.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    let (status, output) = session.finish();

    assert_eq!(status.code(), Some(130), "{output:?}");
    assert_eq!(snapshot(&directory), before);
}

#[test]
fn discard_yes_uses_the_same_prepare_consume_flow_and_prints_exact_success() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_pending(&home, "demo");
    let output = run(&home, &["discard", "demo", "--yes"]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(output.stdout, b"Discarded incomplete download demo\n");
    assert_eq!(output.stderr, b"");
    assert_eq!(
        snapshot(&directory),
        BTreeMap::from([(".lock".into(), vec![])])
    );
}

#[cfg(unix)]
#[test]
fn discard_prompt_window_change_refuses_with_static_rerun_guidance() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_pending(&home, "demo");
    let part = directory.join("model.gguf.part");
    let captured = directory.join("captured-part");
    let mut session = PtySession::spawn(&home, &["discard", "demo"]);
    session.wait_for("Discard incomplete download demo?");
    std::fs::rename(&part, &captured).unwrap();
    std::fs::write(&part, b"XYZ").unwrap();
    session.write(b"y\n");
    let (status, output) = session.finish();

    assert_eq!(status.code(), Some(1), "{output:?}");
    assert!(output.contains("incomplete transfer changed"), "{output:?}");
    assert!(output.contains("loxa discard 'demo'"), "{output:?}");
    assert_eq!(std::fs::read(captured).unwrap(), PART_BYTES);
    assert_eq!(std::fs::read(part).unwrap(), b"XYZ");
    assert!(directory.join("pending.json").exists());
}

#[test]
fn discard_fresh_installed_state_prints_separate_rm_guidance_without_removing_it() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_installed(&home, "demo");
    let before = snapshot(&directory);
    let output = run(&home, &["discard", "demo", "--yes"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("artifact is installed"), "{stderr}");
    assert!(stderr.contains("loxa rm 'demo'"), "{stderr}");
    assert!(!stderr.contains("loxa discard"), "{stderr}");
    assert_eq!(snapshot(&directory), before);
}

#[cfg(unix)]
#[test]
fn discard_errors_and_diagnostics_redact_hostile_pending_paths_tokens_urls_bodies_and_controls() {
    use std::os::fd::AsRawFd;

    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let hostile = "https://evil.invalid/private?token=secret\u{1b}[31m";

    let unsafe_dir = model_dir(&home, "unsafe");
    std::fs::create_dir_all(&unsafe_dir).unwrap();
    std::fs::write(unsafe_dir.join(".lock"), b"").unwrap();
    std::fs::write(unsafe_dir.join("pending.json"), hostile).unwrap();

    let different_dir = seed_pending(&home, "different");
    std::fs::write(
        different_dir.join("pending.json"),
        serde_json::to_vec_pretty(&manifest("other")).unwrap(),
    )
    .unwrap();

    let busy_dir = seed_pending(&home, "busy");
    let busy_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(busy_dir.join(".lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(busy_lock.as_raw_fd(), libc::LOCK_EX) },
        0
    );

    for (id, expected) in [
        ("missing", "No incomplete transfer exists"),
        ("unsafe", "Local model state is unsafe"),
        ("different", "different artifact"),
        ("busy", "Another transfer is already using this model"),
    ] {
        let output = run(&home, &["discard", id, "--yes"]);
        assert_eq!(output.status.code(), Some(1), "{id}: {output:?}");
        assert_eq!(output.stdout, b"");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(expected), "{id}: {stderr}");
        for secret in ["evil.invalid", "token=secret", "/private", "\u{1b}"] {
            assert!(!stderr.contains(secret), "{id}: {stderr:?}");
        }
    }
}

#[test]
fn rm_pending_only_id_remains_unknown_installed_model_and_leaves_state_intact() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let directory = seed_pending(&home, "demo");
    let before = snapshot(&directory);
    let output = run(&home, &["rm", "demo", "--yes"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(output.stdout, b"");
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("unknown model id demo"));
    assert_eq!(snapshot(&directory), before);
}

#[test]
fn rm_help_confirmation_and_installed_removal_behavior_are_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("loxa-home");
    let help = run(&home, &["rm", "--help"]);
    assert_eq!(help.status.code(), Some(0), "{help:?}");
    let help_stdout = String::from_utf8(help.stdout).unwrap();
    assert!(help_stdout.contains("Usage: loxa rm [OPTIONS] [ID]"));
    assert!(help_stdout.contains("--yes"));

    let directory = seed_installed(&home, "demo");
    let removed = run(&home, &["rm", "demo", "--yes"]);
    assert_eq!(removed.status.code(), Some(0), "{removed:?}");
    assert_eq!(removed.stdout, b"Removed demo\n");
    assert_eq!(removed.stderr, b"");
    assert_eq!(
        snapshot(&directory),
        BTreeMap::from([(".lock".into(), vec![])])
    );
}

#[test]
fn non_tty_transfer_preflight_output_is_plain_and_offline() {
    let root = tempfile::tempdir().expect("temporary process environment");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args([
            "pull",
            "owner/repo",
            "--file",
            "exact.gguf",
            "--name",
            "invalid/name",
        ])
        .env("LOXA_HOME", root.path().join("loxa-home"))
        .env("HOME", root.path().join("home"))
        .env("USERPROFILE", root.path().join("userprofile"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env_remove("CLICOLOR_FORCE")
        .env_remove("FORCE_COLOR")
        .output()
        .expect("run offline pull preflight");

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    let mut lines = stderr.lines();
    assert_eq!(
        lines.next(),
        Some("Error: invalid model id \"invalid/name\"")
    );
    assert!(
        lines
            .next()
            .is_some_and(|line| line.starts_with("Diagnostics: ")),
        "{stderr:?}"
    );
    assert_eq!(lines.next(), None, "{stderr:?}");
    for control in ['\u{1b}', '\r', '\u{8}'] {
        assert!(!stderr.contains(control), "{stderr:?}");
    }
    assert!(!root.path().join("loxa-home/models").exists());
}
