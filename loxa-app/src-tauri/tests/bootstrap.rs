use loxa_app_lib::bootstrap::{BootstrapConfig, BootstrapState, Ownership, StartNodeRequest};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake loxa executable")
}

fn fixture_with_spaces() -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "loxa fixture directory with spaces {}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let executable = directory.join("fake loxa executable");
    std::fs::copy(fixture(), &executable).unwrap();
    executable
}

fn port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn endpoint(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn config(executable: PathBuf) -> BootstrapConfig {
    BootstrapConfig {
        executable: Some(executable),
        startup_timeout: Duration::from_secs(2),
        poll_interval: Duration::from_millis(10),
    }
}

fn request(port: u16, model: impl Into<String>) -> StartNodeRequest {
    StartNodeRequest {
        endpoint: endpoint(port),
        model: model.into(),
        engine: "llama-cpp".into(),
    }
}

fn spawn_fixture(port: u16, model: &str) -> Child {
    Command::new(fixture())
        .args([
            "serve",
            "--model",
            model,
            "--port",
            &port.to_string(),
            "--engine",
            "llama-cpp",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            use std::io::{Read, Write};
            stream
                .write_all(
                    b"GET /loxa/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            if response
                .windows(b"\"health\":\"ready\"".len())
                .any(|window| window == b"\"health\":\"ready\"")
            {
                return;
            }
        }
        assert!(Instant::now() < deadline, "fixture did not become ready");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_bootstrap_ownership_matrix() {
    let missing = PathBuf::from("/definitely missing/loxa executable");
    let mut state = BootstrapState::default();
    let error = state
        .start_with_config(request(port(), "ready"), &config(missing))
        .unwrap_err();
    assert!(error.contains("executable"), "{error}");

    let p = port();
    let capture =
        std::env::temp_dir().join(format!("loxa args with spaces {}.txt", std::process::id()));
    let spaced_executable = fixture_with_spaces();
    let snapshot = state
        .start_with_config(
            request(p, capture.display().to_string()),
            &config(spaced_executable.clone()),
        )
        .unwrap();
    assert_eq!(snapshot.ownership, Ownership::Owned);
    let captured = std::fs::read_to_string(&capture).unwrap();
    assert!(
        captured
            .lines()
            .any(|arg| arg == capture.display().to_string())
    );
    state.stop_owned().unwrap();
    let _ = std::fs::remove_file(capture);
    let _ = std::fs::remove_file(&spaced_executable);
    let _ = std::fs::remove_dir(spaced_executable.parent().unwrap());

    let mut quick = config(fixture());
    quick.startup_timeout = Duration::from_millis(100);
    let error = state
        .start_with_config(request(port(), "timeout"), &quick)
        .unwrap_err();
    assert!(error.contains("timed out"), "{error}");
    let error = state
        .start_with_config(request(port(), "early-exit"), &config(fixture()))
        .unwrap_err();
    assert!(error.contains("23"), "{error}");

    let attached_port = port();
    let mut external = spawn_fixture(attached_port, "ready");
    wait_ready(attached_port);
    let snapshot = state
        .attach_with_config(endpoint(attached_port), &config(fixture()))
        .unwrap();
    assert_eq!(snapshot.ownership, Ownership::Attached);
    state.close_window();
    assert_eq!(state.snapshot().ownership, Ownership::None);
    assert!(
        external.try_wait().unwrap().is_none(),
        "close killed attached node"
    );
    assert!(state.stop_owned().is_err());
    assert!(
        external.try_wait().unwrap().is_none(),
        "stop killed attached node"
    );
    external.kill().unwrap();
    external.wait().unwrap();
}

#[test]
fn exact_child_cleanup_replacement_and_ten_cycles() {
    let mut state = BootstrapState::default();
    for _ in 0..10 {
        let p = port();
        let started = state
            .start_with_config(request(p, "ready"), &config(fixture()))
            .unwrap();
        assert_eq!(started.ownership, Ownership::Owned);
        let stopped = state.stop_owned().unwrap();
        assert_eq!(stopped.ownership, Ownership::None);
        assert!(!stopped.child_running);
        assert!(std::net::TcpStream::connect(("127.0.0.1", p)).is_err());
    }

    let p = port();
    state
        .start_with_config(request(p, "ready"), &config(fixture()))
        .unwrap();
    state.stop_owned().unwrap();
    let mut replacement = spawn_fixture(p, "ready");
    wait_ready(p);
    let snapshot = state.snapshot();
    assert_ne!(snapshot.ownership, Ownership::Owned);
    assert!(state.stop_owned().is_err());
    assert!(
        replacement.try_wait().unwrap().is_none(),
        "replacement was killed"
    );
    replacement.kill().unwrap();
    replacement.wait().unwrap();

    let p = port();
    state
        .start_with_config(request(p, "ready"), &config(fixture()))
        .unwrap();
    state.exit_app().unwrap();
    let snapshot = state.snapshot();
    assert_eq!(snapshot.ownership, Ownership::None);
    assert!(!snapshot.child_running);
}

#[test]
fn child_exit_clears_stale_ownership_and_preserves_replacement() {
    let mut state = BootstrapState::default();
    let p = port();
    let started = state
        .start_with_config(request(p, "exit-after-ready"), &config(fixture()))
        .unwrap();
    assert_eq!(started.ownership, Ownership::Owned);
    thread::sleep(Duration::from_millis(1_100));

    let mut replacement = spawn_fixture(p, "ready");
    wait_ready(p);
    let refreshed = state.snapshot();
    assert_eq!(refreshed.ownership, Ownership::Attached);
    assert!(!refreshed.child_running);
    assert!(state.stop_owned().is_err());
    assert!(
        replacement.try_wait().unwrap().is_none(),
        "stale ownership killed replacement"
    );
    replacement.kill().unwrap();
    replacement.wait().unwrap();
}

#[test]
fn rejects_untyped_or_unsafe_start_inputs() {
    let mut state = BootstrapState::default();
    for engine in ["", "metal", "llama-cpp; touch /tmp/no"] {
        let mut req = request(port(), "ready");
        req.engine = engine.into();
        assert!(state.start_with_config(req, &config(fixture())).is_err());
    }
    for endpoint in [
        "http://example.com:8080",
        "https://127.0.0.1:8080",
        "http://127.0.0.1/no-port",
        "http://[::1]:8080",
        "http://127.0.0.1:0",
    ] {
        let req = StartNodeRequest {
            endpoint: endpoint.into(),
            model: "ready".into(),
            engine: "llama-cpp".into(),
        };
        assert!(state.start_with_config(req, &config(fixture())).is_err());
    }
}

#[test]
fn rejects_ipv6_loopback_and_port_zero_during_validation() {
    let mut state = BootstrapState::default();
    let initial = state.snapshot();
    let began = Instant::now();
    let ipv6_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: "http://[::1]:8080".into(),
                model: "ready".into(),
                engine: "llama-cpp".into(),
            },
            &config(fixture()),
        )
        .unwrap_err();
    assert!(ipv6_error.contains("IPv4"), "{ipv6_error}");
    let zero_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: "http://127.0.0.1:0".into(),
                model: "ready".into(),
                engine: "llama-cpp".into(),
            },
            &config(fixture()),
        )
        .unwrap_err();
    assert!(zero_error.contains("between 1 and 65535"), "{zero_error}");
    assert!(began.elapsed() < Duration::from_secs(1));
    assert_eq!(state.snapshot().endpoint, initial.endpoint);
}

#[test]
fn owned_child_cannot_be_retargeted_by_start_or_attach() {
    let mut state = BootstrapState::default();
    let original_port = port();
    let original = state
        .start_with_config(request(original_port, "ready"), &config(fixture()))
        .unwrap();
    let different_endpoint = endpoint(port());

    let start_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: different_endpoint.clone(),
                model: "ready".into(),
                engine: "llama-cpp".into(),
            },
            &config(fixture()),
        )
        .unwrap_err();
    assert!(start_error.contains("owned"), "{start_error}");
    let after_start = state.snapshot();
    assert_eq!(after_start.endpoint, original.endpoint);
    assert_eq!(after_start.ownership, Ownership::Owned);

    let attach_error = state
        .attach_with_config(different_endpoint, &config(fixture()))
        .unwrap_err();
    assert!(attach_error.contains("owned"), "{attach_error}");
    let after_attach = state.snapshot();
    assert_eq!(after_attach.endpoint, original.endpoint);
    assert_eq!(after_attach.ownership, Ownership::Owned);
    state.stop_owned().unwrap();
}

#[test]
fn input_and_spawn_failures_are_visible_without_corrupting_state() {
    let mut state = BootstrapState::default();
    let initial = state.snapshot();

    let invalid = StartNodeRequest {
        endpoint: "http://127.0.0.1:0".into(),
        model: "ready".into(),
        engine: "llama-cpp".into(),
    };
    let input_error = state
        .start_with_config(invalid, &config(fixture()))
        .unwrap_err();
    let after_input = state.snapshot();
    assert_eq!(after_input.endpoint, initial.endpoint);
    assert_eq!(after_input.ownership, Ownership::None);
    assert_eq!(after_input.error.as_deref(), Some(input_error.as_str()));

    let requested_endpoint = endpoint(port());
    let spawn_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: requested_endpoint.clone(),
                model: "ready".into(),
                engine: "llama-cpp".into(),
            },
            &config(PathBuf::from("/missing/loxa")),
        )
        .unwrap_err();
    let after_spawn = state.snapshot();
    assert_eq!(after_spawn.endpoint, requested_endpoint);
    assert_eq!(after_spawn.ownership, Ownership::None);
    assert_eq!(after_spawn.error.as_deref(), Some(spawn_error.as_str()));
}
