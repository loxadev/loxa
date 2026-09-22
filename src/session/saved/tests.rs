use super::signals::SessionSignals;
use crate::session::{new_editor, prompt_input, InputEvent};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn range_progress_counts_raw_utf8_bytes_and_rejects_mismatched_prefixes() {
    let mut range = loxa_ipc::ContentRange {
        start: "0".into(),
        end: "3".into(),
        prefix_end: "3".into(),
        content: "€".into(),
    };
    assert_eq!(super::checked_range(&range, 0, 3).unwrap(), 3);
    range.prefix_end = "4".into();
    assert!(super::checked_range(&range, 0, 3).is_err());
    range.prefix_end = "3".into();
    range.end = "2".into();
    assert!(super::checked_range(&range, 0, 3).is_err());
}

#[test]
fn finalizing_saved_and_completed_save_failed_do_not_finish_as_saved_chat() {
    use loxa_ipc::{
        GenerationExecutionPhase as Execution, GenerationObservation, GenerationSavePhase as Save,
        GenerationStatus, GenerationTarget,
    };
    let mut status = GenerationStatus {
        target: GenerationTarget::Accepted {
            boot_epoch: "boot".into(),
            submission_id: "11".repeat(16),
            operation_generation: "1".into(),
        },
        attempt_id: "22".repeat(16),
        execution: Execution::Finalizing,
        save: Save::Saved,
        saved_end: "0".into(),
        generated_end: Some("0".into()),
        terminal_saved_end: Some("0".into()),
        failure_code: None,
    };
    let (_, outcome) = super::observation_progress(GenerationObservation::Live {
        status: status.clone(),
    })
    .unwrap();
    assert_eq!(outcome, super::TerminalOutcome::Pending);
    status.execution = Execution::Completed;
    status.save = Save::SaveFailed;
    status.terminal_saved_end = None;
    let (_, outcome) = super::observation_progress(GenerationObservation::Live {
        status: status.clone(),
    })
    .unwrap();
    assert_eq!(outcome, super::TerminalOutcome::Pending);
    status.save = Save::Saved;
    status.terminal_saved_end = Some("0".into());
    let (_, outcome) = super::observation_progress(GenerationObservation::Live { status }).unwrap();
    assert_eq!(outcome, super::TerminalOutcome::Completed);
}

const CHILD: &str = "LOXA_SAVED_CHAT_PTY_CHILD";

fn pty_phase(phase: &str, pid: Option<u32>) {
    let current = std::thread::current();
    let _ = writeln!(
        std::io::stderr(),
        "saved-chat-pty phase={phase} pid={pid:?} thread={:?}",
        current.name()
    );
}

#[test]
fn child_idle_editor() {
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("idle")) {
        return;
    }
    let mut editor = new_editor(&super::COMMANDS, false).unwrap();
    let signals = SessionSignals::install().unwrap();
    for index in 0..2 {
        let original = signals.enter_idle().unwrap();
        if index == 1 {
            eprintln!("SECOND_READLINE_ARMED");
        }
        let input = prompt_input(&mut editor);
        signals.leave_idle();
        if signals.was_interrupted() || matches!(input, InputEvent::Interrupted) {
            signals.exit_now(Some(original));
        }
        assert!(matches!(input, InputEvent::Line(ref line) if line == "first"));
    }
}

#[test]
fn child_bracketed_paste() {
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("paste")) {
        return;
    }
    let mut editor = new_editor(&super::COMMANDS, false).unwrap();
    let signals = SessionSignals::install().unwrap();
    let _original = signals.enter_idle().unwrap();
    let input = prompt_input(&mut editor);
    signals.leave_idle();
    let expected = "héllo\n世界".repeat(128);
    assert!(matches!(input, InputEvent::Line(ref line) if line == &expected));
    eprintln!("PASTE_MATCHED");
}

#[test]
fn child_blocked_output() {
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("blocked")) {
        return;
    }
    use super::output::{OutputError, TerminalOutput};
    use super::signals::Interrupt;
    let mut signals = SessionSignals::install().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tag = signals.begin_generation().unwrap();
    eprintln!("BLOCKED_OUTPUT_ARMED");
    let output = {
        let _entered = runtime.enter();
        TerminalOutput::stdout().unwrap()
    };
    let result = runtime.block_on(output.write_all(&vec![b'x'; 1 << 20], &signals, tag));
    assert_eq!(result, Err(OutputError::Interrupted(Interrupt::Current)));
    output.restore().unwrap();
    signals.exit_now(None);
}

#[test]
fn child_run_service() {
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("saved")) {
        return;
    }
    let root = std::env::var_os("LOXA_SAVED_CHAT_TEST_ROOT").unwrap();
    let bootstrap_start = Instant::now();
    let client =
        loxa_ipc::ServiceClient::load(Path::new(&root), None, crate::service::BUILD_ID).unwrap();
    eprintln!(
        "SAVED_CHAT_CLIENT_READY {} ms {} bytes",
        bootstrap_start.elapsed().as_millis(),
        fs::metadata(std::env::current_exe().unwrap())
            .unwrap()
            .len()
    );
    super::run_service(
        client,
        "demo".into(),
        loxa_ipc::OperationTarget {
            boot_epoch: "fake-boot".into(),
            task_id: "1".into(),
            generation: "1".into(),
        },
        None,
    )
    .unwrap();
}

pub(super) struct Pty {
    child: Child,
    master: File,
    slave: File,
    transcript: Vec<u8>,
    original_slave_flags: libc::c_int,
}

impl Pty {
    fn spawn() -> Self {
        Self::spawn_named("idle", "session::saved::tests::child_idle_editor")
    }

    fn spawn_named(mode: &str, test: &str) -> Self {
        Self::spawn_named_with_root(mode, test, None)
    }

    pub(super) fn spawn_saved(root: &Path) -> Self {
        Self::spawn_named_with_root(
            "saved",
            "session::saved::tests::child_run_service",
            Some(root),
        )
    }

    fn spawn_named_with_root(mode: &str, test: &str, root: Option<&Path>) -> Self {
        let mut master = -1;
        let mut slave = -1;
        pty_phase("openpty-before", None);
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
        pty_phase("openpty-after", None);
        let master = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        for descriptor in [&master, &slave] {
            let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
            assert_ne!(flags, -1);
            assert_ne!(
                unsafe {
                    libc::fcntl(
                        descriptor.as_raw_fd(),
                        libc::F_SETFD,
                        flags | libc::FD_CLOEXEC,
                    )
                },
                -1
            );
        }
        let original_slave_flags = unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(original_slave_flags, -1);
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags, -1);
        assert_ne!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            -1
        );
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.arg("--exact").arg(test).arg("--nocapture");
        command.env(CHILD, mode);
        command.env("TERM", "xterm-256color");
        if let Some(root) = root {
            command.env("LOXA_SAVED_CHAT_TEST_ROOT", root);
        }
        command.stdin(Stdio::from(slave.try_clone().unwrap()));
        command.stdout(Stdio::from(slave.try_clone().unwrap()));
        command.stderr(Stdio::from(slave.try_clone().unwrap()));
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1
                    || libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        pty_phase("spawn-before", None);
        let child = command.spawn().expect("spawn saved Chat PTY child");
        pty_phase("spawn-after", Some(child.id()));
        Self {
            child,
            master,
            slave,
            transcript: Vec::new(),
            original_slave_flags,
        }
    }

    pub(super) fn wait_for_prompt_after(&mut self, start: usize) {
        self.wait_for_bytes(b"> ", start, Duration::from_secs(5));
    }

    pub(super) fn wait_for_bootstrap(&mut self) -> usize {
        // Authenticated client setup validates the full test executable before Chat starts.
        self.wait_for_bytes(b"SAVED_CHAT_CLIENT_READY ", 0, Duration::from_secs(45))
    }

    pub(super) fn wait_for_text(&mut self, needle: &str) -> usize {
        self.wait_for_bytes(needle.as_bytes(), 0, Duration::from_secs(5))
    }

    fn wait_for_bytes(&mut self, needle: &[u8], start: usize, limit: Duration) -> usize {
        let _ = writeln!(
            std::io::stderr(),
            "saved-chat-pty wait-before marker={:?} pid={} thread={:?}",
            String::from_utf8_lossy(needle),
            self.child.id(),
            std::thread::current().name()
        );
        let deadline = Instant::now() + limit;
        loop {
            let mut buffer = [0; 4096];
            match self.master.read(&mut buffer) {
                Ok(n) => self.transcript.extend_from_slice(&buffer[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.raw_os_error() == Some(libc::EIO) => {}
                Err(error) => panic!("read PTY: {error}"),
            }
            if let Some(index) = self.transcript[start..]
                .windows(needle.len())
                .position(|bytes| bytes == needle)
            {
                return start + index + needle.len();
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "child exited: {}",
                String::from_utf8_lossy(&self.transcript)
            );
            if Instant::now() >= deadline {
                pty_phase("wait-timeout", Some(self.child.id()));
                panic!(
                    "saved Chat marker {:?} timed out: {}",
                    String::from_utf8_lossy(needle),
                    String::from_utf8_lossy(&self.transcript)
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn write_input(&mut self, bytes: &[u8]) {
        self.master.write_all(bytes).unwrap();
    }

    pub(super) fn signal_sigint(&self) {
        assert_eq!(
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) },
            0
        );
    }

    pub(super) fn finish(self) {
        self.finish_with_code(130);
    }

    fn finish_with_code(mut self, expected_code: i32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        pty_phase("finish-wait-before", Some(self.child.id()));
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "saved Chat child did not exit");
            std::thread::sleep(Duration::from_millis(10));
        };
        pty_phase("finish-wait-after", Some(self.child.id()));
        assert_eq!(status.code(), Some(expected_code));
        pty_phase("restore-check-before", Some(self.child.id()));
        let mut terminal = std::mem::MaybeUninit::<libc::termios>::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(self.slave.as_raw_fd(), terminal.as_mut_ptr()) },
            0
        );
        let terminal = unsafe { terminal.assume_init() };
        assert_ne!(
            terminal.c_lflag & libc::ECHO,
            0,
            "terminal echo was not restored"
        );
        assert_ne!(
            terminal.c_lflag & libc::ICANON,
            0,
            "canonical input was not restored"
        );
        assert_eq!(
            unsafe { libc::fcntl(self.slave.as_raw_fd(), libc::F_GETFL) },
            self.original_slave_flags,
            "terminal output flags were not restored"
        );
        pty_phase("restore-check-after", Some(self.child.id()));
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        pty_phase("drop-before", Some(self.child.id()));
        if self.child.try_wait().ok().flatten().is_none() {
            pty_phase("drop-kill-before", Some(self.child.id()));
            let _ = self.child.kill();
            pty_phase("drop-kill-after", Some(self.child.id()));
            pty_phase("drop-wait-before", Some(self.child.id()));
            let _ = self.child.wait();
            pty_phase("drop-wait-after", Some(self.child.id()));
        }
    }
}

#[test]
fn second_idle_prompt_ctrl_c_exits_and_restores_terminal() {
    let mut pty = Pty::spawn();
    pty.wait_for_prompt_after(0);
    pty.write_input(b"first\n");
    let marker_end = pty.wait_for_text("SECOND_READLINE_ARMED");
    pty.wait_for_prompt_after(marker_end);
    pty.master.write_all(b"\x03").unwrap();
    pty.finish();
}

#[test]
fn batched_lines_survive_readline_return() {
    let mut pty = Pty::spawn();
    pty.wait_for_prompt_after(0);
    pty.write_input(b"first\nfirst\n");
    pty.finish_with_code(0);
}

#[test]
fn external_idle_sigint_exits_and_restores_terminal() {
    let mut pty = Pty::spawn();
    pty.wait_for_prompt_after(0);
    assert_eq!(
        unsafe { libc::kill(pty.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    pty.finish();
}

#[test]
fn prompt_transition_sigint_never_enters_a_successor_prompt() {
    let mut pty = Pty::spawn();
    pty.wait_for_prompt_after(0);
    pty.write_input(b"first\n");
    assert_eq!(
        unsafe { libc::kill(pty.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    pty.finish();
}

#[test]
fn blocked_partial_output_yields_to_sigint_and_restores_flags() {
    let mut pty = Pty::spawn_named("blocked", "session::saved::tests::child_blocked_output");
    pty.wait_for_text("BLOCKED_OUTPUT_ARMED");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        unsafe { libc::kill(pty.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    pty.finish();
}

#[test]
fn bulk_utf8_bracketed_multiline_paste_reaches_the_editor() {
    let mut pty = Pty::spawn_named("paste", "session::saved::tests::child_bracketed_paste");
    pty.wait_for_prompt_after(0);
    let paste = format!("\u{1b}[200~{}\u{1b}[201~\r", "héllo\n世界".repeat(128));
    pty.write_input(paste.as_bytes());
    pty.wait_for_text("PASTE_MATCHED");
    pty.finish_with_code(0);
}
