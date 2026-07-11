//! Opt-in proof that the real external `mlx_lm.server` works through Loxa.
//!
//! This test never installs Python or downloads a model. Run it on Apple
//! Silicon after setting both `LOXA_MLX_LM_SERVER` and `LOXA_MLX_TEST_MODEL`.

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
struct LiveMlxHarness;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
impl LiveMlxHarness {
    fn run() {
        live::run();
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[test]
#[ignore = "requires Apple Silicon, mlx-lm 0.31.3, and a local MLX model"]
fn real_py_mlx_serve_generates_and_cleans_up() {
    LiveMlxHarness::run();
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
mod live {
    use loxa_core::supervisor::{self, ManagedRun, RunLifecycle, RuntimeStateRead};
    use std::ffi::{c_int, OsStr, OsString};
    use std::fs;
    use std::io::{self, Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus, Output, Stdio};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const STARTUP_TIMEOUT: Duration = Duration::from_secs(150);
    const MANAGED_IDENTITY_TIMEOUT: Duration = Duration::from_secs(30);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
    const STOP_TIMEOUT: Duration = Duration::from_secs(25);
    const POLL_INTERVAL: Duration = Duration::from_millis(100);

    pub(super) fn run() {
        let server = required_path("LOXA_MLX_LM_SERVER");
        let model = required_path("LOXA_MLX_TEST_MODEL");
        assert!(server.is_file(), "LOXA_MLX_LM_SERVER must name a file");
        assert!(model.is_dir(), "LOXA_MLX_TEST_MODEL must name a directory");
        let model = fs::canonicalize(model).expect("canonicalize LOXA_MLX_TEST_MODEL");

        let home = TestHome::new();
        let gateway_port = reserve_loopback_port();
        let mut node = LiveNode::spawn(home.path(), &server, &model, gateway_port);

        let managed = wait_for_managed_identity(home.path(), &mut node);
        let status_body = wait_until_ready(&mut node, gateway_port);
        assert!(
            status_body.contains(r#""health":"ready""#),
            "gateway did not report ready: {status_body}"
        );
        assert!(
            status_body.contains(r#""name":"mlx-lm""#),
            "status did not identify mlx-lm: {status_body}"
        );
        assert!(
            status_body.contains(r#""version":"0.31.3""#),
            "status did not identify mlx-lm 0.31.3: {status_body}"
        );
        assert!(
            status_body.contains(&model.display().to_string()),
            "status did not identify the canonical runtime model: {status_body}"
        );

        let non_streaming = chat_completion(gateway_port, false);
        assert_eq!(non_streaming.status, 200, "{non_streaming:?}");
        assert!(
            non_streaming.body.contains(r#""choices""#),
            "non-streaming response has no choices: {}",
            non_streaming.body
        );
        assert!(
            has_nonempty_json_string(&non_streaming.body, "content"),
            "non-streaming response has no generated content: {}",
            non_streaming.body
        );
        assert!(
            non_streaming.body.contains(r#""model":"loxa""#),
            "public response did not normalize the model alias: {}",
            non_streaming.body
        );
        assert!(
            !non_streaming.body.contains("default_model"),
            "private model alias leaked: {}",
            non_streaming.body
        );

        let streaming = chat_completion(gateway_port, true);
        assert_eq!(streaming.status, 200, "{streaming:?}");
        assert!(
            streaming.body.contains("data:"),
            "streaming response was not SSE: {}",
            streaming.body
        );
        assert!(
            has_nonempty_json_string(&streaming.body, "content"),
            "streaming response has no generated content: {}",
            streaming.body
        );
        assert_eq!(
            streaming.body.matches("data: [DONE]").count(),
            1,
            "stream must contain exactly one terminal marker: {}",
            streaming.body
        );
        assert!(
            streaming.body.contains(r#""model":"loxa""#),
            "public stream did not normalize the model alias: {}",
            streaming.body
        );
        assert!(
            !streaming.body.contains("default_model"),
            "private model alias leaked in stream: {}",
            streaming.body
        );

        let state_path = home.path().join(".loxa/run/managed.json");
        let child_pid = managed.child_pid.expect("running state child pid");
        let child_pgid = managed.child_pgid.expect("running state child pgid");
        assert_eq!(
            child_pid,
            u32::try_from(child_pgid).expect("positive child pgid"),
            "the directly spawned Python leader must own its process group"
        );
        assert_eq!(managed.model_id, model.display().to_string());
        assert!(managed.log_path.is_file(), "managed log must exist");
        assert!(
            managed
                .log_path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with("py-mlx-lm-")),
            "managed log path must identify the backend: {}",
            managed.log_path.display()
        );

        let stop = node.stop_via_cli();
        assert!(
            stop.status.success(),
            "loxa stop failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&stop.stdout),
            String::from_utf8_lossy(&stop.stderr)
        );
        assert!(
            String::from_utf8_lossy(&stop.stdout).contains("stop completed"),
            "unexpected stop output: {}",
            String::from_utf8_lossy(&stop.stdout)
        );

        let owner = node.wait_for_exit(STOP_TIMEOUT);
        assert_eq!(
            owner.status.code(),
            Some(0),
            "serve owner failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&owner.stdout),
            String::from_utf8_lossy(&owner.stderr)
        );
        assert_process_absent(child_pid);
        assert_group_absent(child_pgid);
        node.disarm_owned_group();
        assert_eq!(
            supervisor::read_runtime_state(&state_path).expect("read cleaned runtime state"),
            RuntimeStateRead::Loaded(Vec::new()),
            "state may be emptied only after confirmed process cleanup"
        );
    }

    fn required_path(name: &str) -> PathBuf {
        std::env::var_os(name)
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("set {name} before running this ignored test"))
    }

    fn reserve_loopback_port() -> u16 {
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("reserve gateway port")
            .local_addr()
            .expect("reserved gateway address")
            .port()
    }

    fn wait_until_ready(node: &mut LiveNode, port: u16) -> String {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = node.try_wait().expect("observe loxa serve") {
                let output = node.finish_after_status(status);
                panic!(
                    "loxa serve exited before readiness\nstdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            match http_request(port, "GET", "/loxa/status", None, Duration::from_secs(2)) {
                Ok(response)
                    if response.status == 200 && response.body.contains(r#""health":"ready""#) =>
                {
                    return response.body;
                }
                Ok(_) | Err(_) => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for MLX readiness"
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn wait_for_managed_identity(home: &Path, node: &mut LiveNode) -> ManagedRun {
        let state_path = home.join(".loxa/run/managed.json");
        let deadline = Instant::now() + MANAGED_IDENTITY_TIMEOUT;
        loop {
            if let Some(status) = node.try_wait().expect("observe loxa serve") {
                let output = node.finish_after_status(status);
                panic!(
                    "loxa serve exited before managed child attachment\nstdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            let last_state = supervisor::read_runtime_state(&state_path)
                .expect("read isolated managed runtime state");
            if let RuntimeStateRead::Loaded(runs) = &last_state {
                if runs.len() == 1 {
                    let managed = &runs[0];
                    if managed.lifecycle == RunLifecycle::Running {
                        let child_pid = managed.child_pid.expect("running state child pid");
                        let child_pgid = managed.child_pgid.expect("running state child pgid");
                        assert_eq!(
                            child_pid,
                            u32::try_from(child_pgid).expect("positive child pgid"),
                            "the directly spawned Python leader must own its process group"
                        );
                        node.capture_owned_group(child_pgid);
                        return managed.clone();
                    }
                }
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for managed child attachment; last state: {last_state:?}"
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn chat_completion(port: u16, streaming: bool) -> HttpResponse {
        let body = format!(
            r#"{{"model":"loxa","messages":[{{"role":"user","content":"Reply with the word Hello."}}],"stream":{streaming},"temperature":0,"max_tokens":8}}"#
        );
        http_request(
            port,
            "POST",
            "/v1/chat/completions",
            Some(&body),
            REQUEST_TIMEOUT,
        )
        .expect("public chat completion request")
    }

    fn has_nonempty_json_string(body: &str, field: &str) -> bool {
        let marker = format!(r#""{field}":""#);
        body.match_indices(&marker).any(|(offset, _)| {
            body.as_bytes()
                .get(offset + marker.len())
                .is_some_and(|byte| *byte != b'"')
        })
    }

    #[derive(Debug)]
    struct HttpResponse {
        status: u16,
        body: String,
    }

    fn http_request(
        port: u16,
        method: &str,
        path: &str,
        body: Option<&str>,
        timeout: Duration,
    ) -> io::Result<HttpResponse> {
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let mut stream = TcpStream::connect_timeout(&address, timeout.min(Duration::from_secs(2)))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        let body = body.unwrap_or("");
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )?;
        stream.flush()?;

        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes)?;
        parse_http_response(&bytes)
    }

    fn parse_http_response(bytes: &[u8]) -> io::Result<HttpResponse> {
        let header_end = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP headers"))?;
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP status"))?;
        let raw_body = &bytes[header_end + 4..];
        let chunked = headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("transfer-encoding")
                    && value.to_ascii_lowercase().contains("chunked")
            })
        });
        let body = if chunked {
            decode_chunked(raw_body)?
        } else {
            raw_body.to_vec()
        };
        Ok(HttpResponse {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        })
    }

    fn decode_chunked(mut bytes: &[u8]) -> io::Result<Vec<u8>> {
        let mut decoded = Vec::new();
        loop {
            let line_end = bytes
                .windows(2)
                .position(|window| window == b"\r\n")
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing chunk size"))?;
            let size_text = std::str::from_utf8(&bytes[..line_end])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let size = usize::from_str_radix(
                size_text
                    .split(';')
                    .next()
                    .expect("chunk size segment")
                    .trim(),
                16,
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            bytes = &bytes[line_end + 2..];
            if size == 0 {
                return Ok(decoded);
            }
            if bytes.len() < size + 2 || &bytes[size..size + 2] != b"\r\n" {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated HTTP chunk",
                ));
            }
            decoded.extend_from_slice(&bytes[..size]);
            bytes = &bytes[size + 2..];
        }
    }

    fn assert_process_absent(pid: u32) {
        let pid = c_int::try_from(pid).expect("child pid fits c_int");
        assert_absent(pid, "Python leader");
    }

    fn assert_group_absent(pgid: i32) {
        assert!(pgid > 1, "refuse to probe unsafe process group {pgid}");
        assert_absent(-pgid, "Python process group");
    }

    fn assert_absent(target: c_int, label: &str) {
        unsafe extern "C" {
            fn kill(pid: c_int, signal: c_int) -> c_int;
        }
        if unsafe { kill(target, 0) } == 0 {
            panic!("{label} {target} is still present after stop");
        }
        let error = io::Error::last_os_error();
        assert_eq!(
            error.raw_os_error(),
            Some(3),
            "could not prove {label} {target} absent: {error}"
        );
    }

    struct TestHome {
        path: PathBuf,
    }

    impl TestHome {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("loxa-py-mlx-live-{}-{nonce}", std::process::id()));
            fs::create_dir(&path).expect("create isolated HOME");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestHome {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct LiveNode {
        child: Option<Child>,
        home: PathBuf,
        server: OsString,
        owned_pgid: Option<i32>,
    }

    impl LiveNode {
        fn spawn(home: &Path, server: &Path, model: &Path, gateway_port: u16) -> Self {
            let child = Command::new(env!("CARGO_BIN_EXE_loxa"))
                .args(["serve", "--engine", "py-mlx-lm", "--model"])
                .arg(model)
                .args(["--port", &gateway_port.to_string()])
                .env("HOME", home)
                .env("LOXA_MLX_LM_SERVER", server)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn loxa serve");
            Self {
                child: Some(child),
                home: home.to_path_buf(),
                server: server.as_os_str().to_owned(),
                owned_pgid: None,
            }
        }

        fn capture_owned_group(&mut self, pgid: i32) {
            self.owned_pgid = Some(pgid);
        }

        fn disarm_owned_group(&mut self) {
            self.owned_pgid = None;
        }

        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            self.child.as_mut().expect("live child").try_wait()
        }

        fn stop_via_cli(&self) -> Output {
            Command::new(env!("CARGO_BIN_EXE_loxa"))
                .args(["stop", "all"])
                .env("HOME", &self.home)
                .env("LOXA_MLX_LM_SERVER", &self.server)
                .output()
                .expect("run loxa stop all")
        }

        fn wait_for_exit(&mut self, timeout: Duration) -> Output {
            let deadline = Instant::now() + timeout;
            loop {
                if let Some(status) = self.try_wait().expect("wait for serve owner") {
                    return self.finish_after_status(status);
                }
                assert!(
                    Instant::now() < deadline,
                    "loxa serve did not exit after stop"
                );
                thread::sleep(POLL_INTERVAL);
            }
        }

        fn finish_after_status(&mut self, status: ExitStatus) -> Output {
            let mut child = self.child.take().expect("live child");
            let reaped_status = child.wait().expect("reap loxa serve owner");
            assert_eq!(reaped_status, status, "owner exit status changed at reap");
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            child
                .stdout
                .take()
                .expect("serve stdout")
                .read_to_end(&mut stdout)
                .expect("read serve stdout");
            child
                .stderr
                .take()
                .expect("serve stderr")
                .read_to_end(&mut stderr)
                .expect("read serve stderr");
            Output {
                status,
                stdout,
                stderr,
            }
        }
    }

    impl Drop for LiveNode {
        fn drop(&mut self) {
            let Some(child) = self.child.as_mut() else {
                force_known_group_down(self.owned_pgid);
                return;
            };
            let _ = Command::new(env!("CARGO_BIN_EXE_loxa"))
                .args(["stop", "all"])
                .env("HOME", &self.home)
                .env("LOXA_MLX_LM_SERVER", &self.server)
                .output();
            let deadline = Instant::now() + STOP_TIMEOUT;
            let mut clean_owner_exit = false;
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        clean_owner_exit = status.success();
                        break;
                    }
                    Ok(None) if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
                    Ok(None) | Err(_) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        break;
                    }
                }
            }
            if !clean_owner_exit {
                force_known_group_down(self.owned_pgid);
            }
        }
    }

    fn force_known_group_down(pgid: Option<i32>) {
        let Some(pgid) = pgid.filter(|pgid| *pgid > 1) else {
            return;
        };
        unsafe extern "C" {
            fn kill(pid: c_int, signal: c_int) -> c_int;
        }
        if unsafe { kill(-pgid, 0) } == 0 {
            let _ = unsafe { kill(-pgid, 9) };
        }
    }
}
