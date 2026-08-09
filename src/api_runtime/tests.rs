use super::*;
use crate::catalog::{Manifest, ModelLock};
use crate::paths::AppPaths;
use crate::runtime::RuntimeOwnership;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

#[cfg(unix)]
static API_RUNTIME_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(unix)]
fn process_test_lock() -> std::sync::MutexGuard<'static, ()> {
    API_RUNTIME_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(unix)]
fn write_executable(path: &Path, bytes: &[u8]) {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
struct InstalledFixture {
    _root: TempDir,
    paths: AppPaths,
    launches: PathBuf,
    mode: PathBuf,
    requests: PathBuf,
}

#[cfg(unix)]
impl InstalledFixture {
    fn new() -> Self {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let launches = root.path().join("launches");
        let mode = root.path().join("server-mode");
        let requests = root.path().join("requests");
        fs::create_dir_all(paths.managed_server.parent().unwrap()).unwrap();
        Self::write_managed_server(&paths.managed_server, &launches, &mode, &requests);
        let fixture = Self {
            _root: root,
            paths,
            launches,
            mode,
            requests,
        };
        fixture.install("demo");
        fixture
    }

    fn install(&self, id: &str) -> Manifest {
        let bytes = format!("model bytes for {id}").into_bytes();
        let digest = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let manifest = Manifest {
            version: 1,
            id: id.to_owned(),
            repo: Some("owner/repo".into()),
            revision: Some("0".repeat(40)),
            remote_filename: Some("model.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: digest,
            size: bytes.len() as u64,
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let model_dir = self.paths.models.join(id);
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(model_dir.join("model.gguf"), bytes).unwrap();
        fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        drop(ModelLock::acquire(&model_dir).unwrap());
        manifest
    }

    fn model_dir(&self, id: &str) -> PathBuf {
        self.paths.models.join(id)
    }

    fn launch_count(&self) -> usize {
        fs::read_to_string(&self.launches)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn set_mode(&self, mode: &str) {
        fs::write(&self.mode, mode).unwrap();
    }

    fn request_lines(&self) -> Vec<String> {
        fs::read_to_string(&self.requests)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn props_request_count(&self) -> usize {
        self.request_lines()
            .iter()
            .filter(|line| line.as_str() == "GET /props HTTP/1.1")
            .count()
    }

    fn write_managed_server(server: &Path, launches: &Path, mode: &Path, requests: &Path) {
        let executable = std::env::current_exe().unwrap();
        for path in [server, launches, mode, requests, executable.as_path()] {
            assert!(!path.to_string_lossy().contains('\''));
        }
        write_executable(
            server,
            format!(
                "#!/bin/sh\nif [ \"$1\" = '--version' ]; then\n  printf '%s\\n' 'version: 10121 (555881ebc)' >&2\n  exit 0\nfi\nprintf '%s\\n' launch >> '{}'\nmode=''\nif [ -f '{}' ]; then IFS= read -r mode < '{}'; fi\nif [ \"$mode\" = 'startup-fail' ]; then\n  printf '%s\\n' 'HOSTILE-startup-detail\\033[31m' >&2\n  exit 7\nfi\nport=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--port' ]; then shift; port=\"$1\"; fi\n  shift\ndone\nexport LOXA_API_RUNTIME_CHILD=1\nexport LOXA_API_RUNTIME_PORT=\"$port\"\nexport LOXA_API_RUNTIME_ALIAS='demo'\nexport LOXA_API_RUNTIME_MODE='{}'\nexport LOXA_API_RUNTIME_REQUESTS='{}'\nexec '{}' --exact api_runtime::tests::managed_server_child --nocapture\n",
                launches.display(),
                mode.display(),
                mode.display(),
                mode.display(),
                requests.display(),
                executable.display(),
            )
            .as_bytes(),
        );
    }
}

#[cfg(unix)]
fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while request.len() < 8192 && !request.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
    }
    request
}

#[cfg(unix)]
fn write_response(stream: &mut TcpStream, status: &str, headers: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
}

#[cfg(unix)]
#[test]
fn managed_server_child() {
    if std::env::var_os("LOXA_API_RUNTIME_CHILD").is_none() {
        return;
    }
    let port = std::env::var("LOXA_API_RUNTIME_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let alias = std::env::var("LOXA_API_RUNTIME_ALIAS").unwrap();
    let mode = PathBuf::from(std::env::var_os("LOXA_API_RUNTIME_MODE").unwrap());
    let requests = PathBuf::from(std::env::var_os("LOXA_API_RUNTIME_REQUESTS").unwrap());
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    eprintln!("test server listening on http://127.0.0.1:{port}");

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let request = read_request(&mut stream);
        let request_line = request
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default();
        let mut log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&requests)
            .unwrap();
        log.write_all(request_line).unwrap();
        log.write_all(b"\n").unwrap();

        if request.starts_with(b"GET /v1/models HTTP/1.1\r\n") {
            if fs::read_to_string(&mode).is_ok_and(|selected| selected.trim() == "block-ready") {
                loop {
                    thread::sleep(Duration::from_secs(1));
                }
            }
            let body = format!(r#"{{"data":[{{"id":"{alias}"}}]}}"#);
            write_response(
                &mut stream,
                "200 OK",
                "Content-Type: application/json\r\n",
                body.as_bytes(),
            );
            continue;
        }
        if request.starts_with(b"GET /props HTTP/1.1\r\n") {
            let selected = fs::read_to_string(&mode).unwrap_or_else(|_| "loaded".into());
            match selected.trim() {
                "sleeping" => write_response(
                    &mut stream,
                    "200 OK",
                    "Content-Type: application/json\r\n",
                    br#"{"is_sleeping":true}"#,
                ),
                "missing" => write_response(
                    &mut stream,
                    "200 OK",
                    "Content-Type: application/json\r\n",
                    br#"{"other":false}"#,
                ),
                "wrong-type" => write_response(
                    &mut stream,
                    "200 OK",
                    "Content-Type: application/json\r\n",
                    br#"{"is_sleeping":"false"}"#,
                ),
                "invalid" => write_response(
                    &mut stream,
                    "200 OK",
                    "Content-Type: application/json\r\n",
                    b"not-json",
                ),
                "oversized" => {
                    let mut body = br#"{"is_sleeping":false}"#.to_vec();
                    body.resize(1024 * 1024 + 1, b' ');
                    write_response(
                        &mut stream,
                        "200 OK",
                        "Content-Type: application/json\r\n",
                        &body,
                    );
                }
                "redirect" => write_response(&mut stream, "302 Found", "Location: /props\r\n", b""),
                "timeout" => thread::sleep(Duration::from_secs(2)),
                _ => write_response(
                    &mut stream,
                    "200 OK",
                    "Content-Type: application/json\r\n",
                    br#"{"is_sleeping":false}"#,
                ),
            }
            continue;
        }
        write_response(&mut stream, "404 Not Found", "", b"");
    }
}

#[cfg(unix)]
fn wait_for(timeout: Duration, predicate: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn child_pid(paths: &AppPaths) -> u32 {
    serde_json::from_slice::<serde_json::Value>(
        &fs::read(paths.run.join("foreground.json")).unwrap(),
    )
    .unwrap()["child_pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .unwrap()
}

#[cfg(unix)]
fn kill_child(pid: u32) {
    let pid = i32::try_from(pid).unwrap();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    thread::sleep(Duration::from_millis(20));
}

#[test]
fn construction_is_empty_and_empty_stop_is_idempotent() {
    let root = tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let mut host = ApiRuntimeHost::new(paths);

    assert_eq!(host.endpoint(), None);
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::AlreadyStopped);
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::AlreadyStopped);
    assert_eq!(host.endpoint(), None);
}

#[test]
fn empty_activity_is_unknown_without_calling_http() {
    let root = tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let mut host = ApiRuntimeHost::new(paths);
    let requests = Cell::new(0_u8);

    let activity = host.activity_with(|_| {
        requests.set(requests.get() + 1);
        ApiRuntimeActivity::Loaded
    });

    assert_eq!(activity, ApiRuntimeActivity::Unknown);
    assert_eq!(requests.get(), 0);
}

#[test]
fn start_cancellation_is_one_independent_cloneable_atomic_value() {
    let cancellation = ApiStartCancellation::new();
    let worker = cancellation.clone();

    assert!(!worker.is_cancelled());
    cancellation.cancel();
    assert!(worker.is_cancelled());
}

#[test]
fn public_host_contract_is_owned_sendable_and_narrow() {
    fn assert_send_static<T: Send + 'static>() {}
    fn assert_clone_send_sync_static<T: Clone + Send + Sync + 'static>() {}
    fn assert_error<T: std::error::Error + Send + 'static>() {}

    assert_send_static::<ApiRuntimeHost>();
    assert_clone_send_sync_static::<ApiStartCancellation>();
    assert_error::<ApiStartError>();
    assert_error::<ApiStopError>();
    let _: fn(AppPaths) -> ApiRuntimeHost = ApiRuntimeHost::new;
    let _: fn(&ApiRuntimeHost) -> Option<ApiRuntimeEndpoint> = ApiRuntimeHost::endpoint;
    let _: fn(
        &mut ApiRuntimeHost,
        &str,
        &ApiStartCancellation,
    ) -> Result<ApiStartOutcome, ApiStartError> = ApiRuntimeHost::start;
    let _: fn(&mut ApiRuntimeHost) -> Result<ApiStopOutcome, ApiStopError> = ApiRuntimeHost::stop;
    let _: fn(&mut ApiRuntimeHost) -> ApiRuntimeActivity = ApiRuntimeHost::activity;
    let _: fn(&ApiRuntimeEndpoint) -> &str = ApiRuntimeEndpoint::model_id;
    let _: fn(&ApiRuntimeEndpoint) -> u16 = ApiRuntimeEndpoint::port;
}

#[cfg(unix)]
#[test]
fn successful_start_retains_exact_endpoint_lease_and_model_lock_until_clean_stop() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    let outcome = host.start("demo", &ApiStartCancellation::new()).unwrap();
    let endpoint = match outcome {
        ApiStartOutcome::Started(endpoint) => endpoint,
        other => panic!("unexpected start outcome: {other:?}"),
    };

    assert_eq!(endpoint.model_id(), "demo");
    assert_ne!(endpoint.port(), 0);
    assert_eq!(host.endpoint(), Some(endpoint.clone()));
    assert_eq!(fixture.launch_count(), 1);
    assert_eq!(
        fs::read_to_string(&fixture.requests)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["GET /v1/models HTTP/1.1"]
    );
    let lease: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.paths.run.join("foreground.json")).unwrap())
            .unwrap();
    assert_eq!(lease["owner_mode"], "persistent_app");
    assert_eq!(lease["model_id"], "demo");
    assert_eq!(lease["port"], endpoint.port());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());
    assert!(RuntimeOwnership::acquire(&fixture.paths.run).is_err());

    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
    assert_eq!(host.endpoint(), None);
    assert!(!fixture.paths.run.join("foreground.json").exists());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
    assert!(TcpStream::connect(("127.0.0.1", endpoint.port())).is_err());
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::AlreadyStopped);
}

#[cfg(unix)]
#[test]
fn duplicate_and_different_model_start_never_spawn_or_replace_the_owner() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    fixture.install("other");
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    let endpoint = match host.start("demo", &ApiStartCancellation::new()).unwrap() {
        ApiStartOutcome::Started(endpoint) => endpoint,
        other => panic!("unexpected start outcome: {other:?}"),
    };
    let cancelled = ApiStartCancellation::new();
    cancelled.cancel();

    assert_eq!(
        host.start("demo", &cancelled).unwrap(),
        ApiStartOutcome::AlreadyRunning(endpoint.clone())
    );
    assert_eq!(
        host.start("other", &ApiStartCancellation::new()),
        Err(ApiStartError::Conflict)
    );
    assert_eq!(host.endpoint(), Some(endpoint));
    assert_eq!(fixture.launch_count(), 1);

    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn independent_runtime_conflict_is_typed_and_left_untouched() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let ownership = RuntimeOwnership::acquire(&fixture.paths.run).unwrap();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    assert_eq!(
        host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::Conflict)
    );
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 0);
    assert!(RuntimeOwnership::acquire(&fixture.paths.run).is_err());
    assert!(!fixture.paths.run.join("foreground.json").exists());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());

    drop(ownership);
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
}

#[cfg(unix)]
#[test]
fn same_model_foreground_runtime_is_typed_conflict_and_left_untouched() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let runnable = crate::runnable::resolve_runnable(
        "demo".into(),
        crate::cli::RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(fixture.paths.managed_server.clone()),
        },
        &fixture.paths,
    )
    .unwrap();
    let mut foreground =
        match crate::runner::start_foreground(runnable.launch(), &fixture.paths.run).unwrap() {
            crate::runner::ForegroundStart::Ready(server) => server,
            crate::runner::ForegroundStart::Stopped(exit) => {
                panic!("foreground runtime stopped before ready: {exit:?}")
            }
        };
    let foreground_port = foreground.port();
    let lease_path = fixture.paths.run.join("foreground.json");
    let lease_before = fs::read(&lease_path).unwrap();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    assert_eq!(
        host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::Conflict)
    );
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 1);
    assert_eq!(fs::read(&lease_path).unwrap(), lease_before);
    assert_eq!(foreground.port(), foreground_port);
    assert!(matches!(foreground.poll(), Ok(None)));
    assert!(RuntimeOwnership::acquire(&fixture.paths.run).is_err());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());

    foreground.terminate().unwrap();
    drop(foreground);
    drop(runnable);
    assert!(!lease_path.exists());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn nonruntime_model_busy_conflict_has_generic_static_text_and_zero_spawn() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let busy = ModelLock::acquire(&fixture.model_dir("demo")).unwrap();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    let error = host
        .start("demo", &ApiStartCancellation::new())
        .unwrap_err();

    assert_eq!(error, ApiStartError::Conflict);
    assert_eq!(error.to_string(), "another Loxa model operation is active");
    assert!(!error.to_string().contains("API runtime"));
    assert!(!error.to_string().contains("busy in another Loxa command"));
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 0);
    assert!(!fixture.paths.run.join("foreground.json").exists());

    drop(busy);
    assert!(matches!(
        host.start("demo", &ApiStartCancellation::new()),
        Ok(ApiStartOutcome::Started(_))
    ));
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn unavailable_invalid_and_precancelled_models_never_spawn() {
    let _guard = process_test_lock();

    let missing = InstalledFixture::new();
    let mut missing_host = ApiRuntimeHost::new(missing.paths.clone());
    assert_eq!(
        missing_host.start("not-installed", &ApiStartCancellation::new()),
        Err(ApiStartError::ModelUnavailable)
    );
    assert_eq!(missing.launch_count(), 0);

    let malformed = InstalledFixture::new();
    let malformed_dir = malformed.paths.models.join("malformed");
    fs::create_dir_all(&malformed_dir).unwrap();
    fs::write(
        malformed_dir.join("manifest.json"),
        br#"{"HOSTILE-catalog":"\u001b[31m"}"#,
    )
    .unwrap();
    let mut malformed_host = ApiRuntimeHost::new(malformed.paths.clone());
    assert_eq!(
        malformed_host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::ModelUnavailable)
    );
    assert_eq!(malformed.launch_count(), 0);

    let cancelled = InstalledFixture::new();
    let cancellation = ApiStartCancellation::new();
    cancellation.cancel();
    let mut cancelled_host = ApiRuntimeHost::new(cancelled.paths.clone());
    assert_eq!(
        cancelled_host.start("demo", &cancellation),
        Err(ApiStartError::Cancelled)
    );
    assert_eq!(cancelled.launch_count(), 0);
    drop(ModelLock::acquire(&cancelled.model_dir("demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn startup_failure_leaves_the_host_and_all_ownership_empty() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    fixture.set_mode("startup-fail");
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    assert_eq!(
        host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::StartupFailed)
    );
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 1);
    assert!(!fixture.paths.run.join("foreground.json").exists());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
}

#[cfg(unix)]
#[test]
fn stop_failure_retains_exact_ownership_and_retry_succeeds() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    fixture.install("other");
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    let endpoint = match host.start("demo", &ApiStartCancellation::new()).unwrap() {
        ApiStartOutcome::Started(endpoint) => endpoint,
        other => panic!("unexpected start outcome: {other:?}"),
    };
    host.fail_next_stop_for_test();

    assert_eq!(host.stop(), Err(ApiStopError));
    assert_eq!(host.endpoint(), Some(endpoint));
    assert!(fixture.paths.run.join("foreground.json").exists());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());
    assert!(matches!(
        host.start("demo", &ApiStartCancellation::new()),
        Ok(ApiStartOutcome::AlreadyRunning(_))
    ));
    assert_eq!(
        host.start("other", &ApiStartCancellation::new()),
        Err(ApiStartError::Conflict)
    );

    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
    assert_eq!(host.endpoint(), None);
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn unsafe_lease_cleanup_failure_retains_exact_ownership_and_retry_succeeds() {
    use std::os::unix::fs::symlink;

    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    let endpoint = match host.start("demo", &ApiStartCancellation::new()).unwrap() {
        ApiStartOutcome::Started(endpoint) => endpoint,
        other => panic!("unexpected start outcome: {other:?}"),
    };
    let lease_path = fixture.paths.run.join("foreground.json");
    let original_lease = fs::read(&lease_path).unwrap();
    fs::remove_file(&lease_path).unwrap();
    symlink("missing-lease-target", &lease_path).unwrap();

    assert_eq!(host.stop(), Err(ApiStopError));
    assert_eq!(host.endpoint(), Some(endpoint));
    assert!(fs::symlink_metadata(&lease_path)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(RuntimeOwnership::acquire(&fixture.paths.run).is_err());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());

    fs::remove_file(&lease_path).unwrap();
    fs::write(&lease_path, original_lease).unwrap();
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
    assert_eq!(host.endpoint(), None);
    assert!(!lease_path.exists());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn empty_host_stop_cannot_terminate_another_hosts_runtime() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let mut owner = ApiRuntimeHost::new(fixture.paths.clone());
    let endpoint = match owner.start("demo", &ApiStartCancellation::new()).unwrap() {
        ApiStartOutcome::Started(endpoint) => endpoint,
        other => panic!("unexpected start outcome: {other:?}"),
    };
    let mut empty = ApiRuntimeHost::new(fixture.paths.clone());

    assert_eq!(empty.stop().unwrap(), ApiStopOutcome::AlreadyStopped);
    assert_eq!(owner.endpoint(), Some(endpoint));
    assert!(fixture.paths.run.join("foreground.json").exists());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());

    assert_eq!(owner.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn drop_tears_down_the_exact_owned_runtime() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let endpoint = {
        let mut host = ApiRuntimeHost::new(fixture.paths.clone());
        match host.start("demo", &ApiStartCancellation::new()).unwrap() {
            ApiStartOutcome::Started(endpoint) => endpoint,
            other => panic!("unexpected start outcome: {other:?}"),
        }
    };

    wait_for(Duration::from_secs(2), || {
        !fixture.paths.run.join("foreground.json").exists()
    });
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
    assert!(TcpStream::connect(("127.0.0.1", endpoint.port())).is_err());
}

#[cfg(unix)]
#[test]
fn cancellation_after_admission_before_spawn_releases_the_model_lock() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let cancellation = ApiStartCancellation::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    let result = host.start_with_test_hooks("demo", &cancellation, || cancellation.cancel(), || {});

    assert_eq!(result, Err(ApiStartError::Cancelled));
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 0);
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
}

#[cfg(unix)]
#[test]
fn cancellation_while_startup_owns_a_lease_cleans_every_resource() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    fixture.set_mode("block-ready");
    let cancellation = ApiStartCancellation::new();
    let canceller = cancellation.clone();
    let lease = fixture.paths.run.join("foreground.json");
    let waiter = thread::spawn(move || {
        wait_for(Duration::from_secs(5), || lease.is_file());
        canceller.cancel();
    });
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    let result = host.start("demo", &cancellation);
    waiter.join().unwrap();

    assert_eq!(result, Err(ApiStartError::Cancelled));
    assert_eq!(host.endpoint(), None);
    assert_eq!(fixture.launch_count(), 1);
    assert!(!fixture.paths.run.join("foreground.json").exists());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
}

#[cfg(unix)]
#[test]
fn final_cancellation_after_ready_cleans_before_reporting_cancelled() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let cancellation = ApiStartCancellation::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());

    let result = host.start_with_test_hooks("demo", &cancellation, || {}, || cancellation.cancel());

    assert_eq!(result, Err(ApiStartError::Cancelled));
    assert_eq!(host.endpoint(), None);
    assert!(!fixture.paths.run.join("foreground.json").exists());
    drop(ModelLock::acquire(&fixture.model_dir("demo")).unwrap());
    drop(RuntimeOwnership::acquire(&fixture.paths.run).unwrap());
}

#[cfg(unix)]
#[test]
fn failed_final_cancellation_cleanup_returns_the_owned_server_to_the_host() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let cancellation = ApiStartCancellation::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    host.fail_next_stop_for_test();

    let result = host.start_with_test_hooks("demo", &cancellation, || {}, || cancellation.cancel());

    assert_eq!(result, Err(ApiStartError::StartupFailed));
    assert!(host.endpoint().is_some());
    assert!(fixture.paths.run.join("foreground.json").exists());
    assert!(ModelLock::acquire(&fixture.model_dir("demo")).is_err());
    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn managed_runtime_damage_is_startup_failure_but_invalid_artifact_is_unavailable() {
    let _guard = process_test_lock();

    let damaged_runtime = InstalledFixture::new();
    write_executable(
        &damaged_runtime.paths.managed_server,
        b"#!/bin/sh\nprintf '%s\\n' 'version: hostile-wrong-runtime' >&2\n",
    );
    let mut runtime_host = ApiRuntimeHost::new(damaged_runtime.paths.clone());
    assert_eq!(
        runtime_host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::StartupFailed)
    );

    let invalid_model = InstalledFixture::new();
    fs::write(
        invalid_model.model_dir("demo").join("model.gguf"),
        b"changed",
    )
    .unwrap();
    let mut model_host = ApiRuntimeHost::new(invalid_model.paths.clone());
    assert_eq!(
        model_host.start("demo", &ApiStartCancellation::new()),
        Err(ApiStartError::ModelUnavailable)
    );
}

#[test]
fn typed_start_failures_and_public_displays_never_expose_hostile_diagnostics() {
    let hostile = "another Loxa runtime is active; HOSTILE-\u{1b}[31m";

    assert_eq!(
        ApiRuntimeHost::map_start_error(crate::runner::PersistentStartError::Failed(
            hostile.into()
        )),
        ApiStartError::StartupFailed
    );
    assert_eq!(
        ApiStartError::Conflict.to_string(),
        "another Loxa model operation is active"
    );
    assert_eq!(
        ApiStartError::Cancelled.to_string(),
        "API startup was cancelled"
    );
    assert_eq!(
        ApiStartError::ModelUnavailable.to_string(),
        "the selected installed model is unavailable"
    );
    assert_eq!(
        ApiStartError::StartupFailed.to_string(),
        "the API runtime could not be started"
    );
    assert_eq!(
        ApiStopError.to_string(),
        "the API runtime could not be stopped"
    );
    for text in [
        ApiStartError::Conflict.to_string(),
        ApiStartError::Cancelled.to_string(),
        ApiStartError::ModelUnavailable.to_string(),
        ApiStartError::StartupFailed.to_string(),
        ApiStopError.to_string(),
    ] {
        assert!(!text.contains("HOSTILE"), "{text:?}");
        assert!(!text.contains('\u{1b}'), "{text:?}");
    }
}

#[cfg(unix)]
#[test]
fn activity_maps_only_exact_booleans_and_bounds_every_response() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    host.start("demo", &ApiStartCancellation::new()).unwrap();

    let rows = [
        ("loaded", ApiRuntimeActivity::Loaded),
        ("sleeping", ApiRuntimeActivity::Sleeping),
        ("missing", ApiRuntimeActivity::Unknown),
        ("wrong-type", ApiRuntimeActivity::Unknown),
        ("invalid", ApiRuntimeActivity::Unknown),
        ("oversized", ApiRuntimeActivity::Unknown),
        ("redirect", ApiRuntimeActivity::Unknown),
    ];
    for (index, (mode, expected)) in rows.into_iter().enumerate() {
        fixture.set_mode(mode);
        assert_eq!(host.activity(), expected, "mode {mode}");
        assert_eq!(fixture.props_request_count(), index + 1, "mode {mode}");
    }
    assert!(fixture
        .request_lines()
        .iter()
        .skip(1)
        .all(|line| line == "GET /props HTTP/1.1"));

    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn activity_timeout_is_unknown_and_remains_bounded() {
    let _guard = process_test_lock();
    let fixture = InstalledFixture::new();
    let mut host = ApiRuntimeHost::new(fixture.paths.clone());
    host.start("demo", &ApiStartCancellation::new()).unwrap();
    fixture.set_mode("timeout");
    let started = Instant::now();

    assert_eq!(host.activity(), ApiRuntimeActivity::Unknown);
    assert!(started.elapsed() < Duration::from_millis(1800));
    assert_eq!(fixture.props_request_count(), 1);

    assert_eq!(host.stop().unwrap(), ApiStopOutcome::Stopped);
}

#[cfg(unix)]
#[test]
fn activity_checks_the_exact_child_before_and_after_the_probe() {
    let _guard = process_test_lock();

    let pre = InstalledFixture::new();
    let mut pre_host = ApiRuntimeHost::new(pre.paths.clone());
    pre_host
        .start("demo", &ApiStartCancellation::new())
        .unwrap();
    let requests_before = pre.props_request_count();
    kill_child(child_pid(&pre.paths));
    assert_eq!(pre_host.activity(), ApiRuntimeActivity::Unknown);
    assert_eq!(pre.props_request_count(), requests_before);
    assert_eq!(pre_host.activity(), ApiRuntimeActivity::Unknown);
    assert_eq!(pre.props_request_count(), requests_before);
    assert_eq!(pre_host.stop().unwrap(), ApiStopOutcome::Stopped);

    let post = InstalledFixture::new();
    let mut post_host = ApiRuntimeHost::new(post.paths.clone());
    post_host
        .start("demo", &ApiStartCancellation::new())
        .unwrap();
    let pid = child_pid(&post.paths);
    let lease_path = post.paths.run.join("foreground.json");
    let original_lease = fs::read(&lease_path).unwrap();
    let mut changed_lease: serde_json::Value = serde_json::from_slice(&original_lease).unwrap();
    changed_lease["port"] = serde_json::json!(changed_lease["port"]
        .as_u64()
        .unwrap()
        .checked_add(1)
        .unwrap());
    assert_eq!(
        post_host.activity_with(|_| {
            fs::write(&lease_path, serde_json::to_vec(&changed_lease).unwrap()).unwrap();
            kill_child(pid);
            ApiRuntimeActivity::Loaded
        }),
        ApiRuntimeActivity::Unknown
    );
    assert_eq!(post_host.activity(), ApiRuntimeActivity::Unknown);
    assert!(lease_path.exists());
    assert!(RuntimeOwnership::acquire(&post.paths.run).is_err());
    assert!(ModelLock::acquire(&post.model_dir("demo")).is_err());
    fs::write(&lease_path, original_lease).unwrap();
    assert_eq!(post_host.stop().unwrap(), ApiStopOutcome::Stopped);
    assert!(!lease_path.exists());
}
