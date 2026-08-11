use std::ffi::{OsStr, OsString};
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;

static PROCESS_ENVIRONMENT: Mutex<()> = Mutex::new(());

struct ScopedEnvironment {
    original: Vec<(&'static str, Option<OsString>)>,
}

impl ScopedEnvironment {
    fn set<const N: usize>(values: [(&'static str, Option<&OsStr>); N]) -> Self {
        let original = values
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in values {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        Self { original }
    }
}

impl Drop for ScopedEnvironment {
    fn drop(&mut self) {
        for (name, value) in self.original.drain(..).rev() {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn install_small_model(models: &std::path::Path, id: &str, model: &std::path::Path) {
    let model_dir = models.join(id);
    std::fs::create_dir_all(&model_dir).unwrap();
    assert_eq!(
        std::fs::copy(model, model_dir.join("model.gguf")).unwrap(),
        88_202_080
    );
    std::fs::write(
        model_dir.join("manifest.json"),
        format!(
            r#"{{
  "version": 1,
  "id": "{id}",
  "repo": "bartowski/SmolLM2-135M-Instruct-GGUF",
  "revision": "09816acd5d99df7be770d85ea30822623dab342c",
  "remote_filename": "SmolLM2-135M-Instruct-Q2_K.gguf",
  "local_filename": "model.gguf",
  "sha256": "741ad12b64088fedc17c33aacb22e48be1972ef36a39f03666dd68bd15614fb9",
  "size": 88202080
}}
"#
        ),
    )
    .unwrap();
    drop(loxa::catalog::ModelLock::acquire(&model_dir).unwrap());
}

fn request(port: u16, request: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    response
}

fn lease_number(path: &std::path::Path, key: &str) -> u64 {
    let lease = std::fs::read_to_string(path).unwrap();
    let tail = lease
        .split_once(&format!("\"{key}\":"))
        .unwrap_or_else(|| panic!("lease omitted {key}: {lease}"))
        .1
        .trim_start();
    tail.bytes()
        .take_while(u8::is_ascii_digit)
        .fold(0_u64, |value, digit| {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(digit - b'0')))
                .unwrap()
        })
}

fn process_or_group_is_gone(pid: u32, group: bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let target = if group {
            format!("-{pid}")
        } else {
            pid.to_string()
        };
        if !std::process::Command::new("/bin/kill")
            .args(["-0", "--", target.as_str()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[cfg(unix)]
#[test]
#[ignore = "requires a finalized built app and the exact small-model fixture"]
fn finalized_app_real_service_threads_ignore_hostile_environment_and_join_cleanly() {
    use crate::menu::api_runtime::{ApiRuntimeController, ApiRuntimePhase};
    use crate::menu::observation::{BackendClient, BackendMessage, ObservationMessage};
    use crate::menu::presentation::{ObservedRuntimeOwner, RuntimeInventory};
    use std::os::unix::fs::PermissionsExt as _;

    let _environment = PROCESS_ENVIRONMENT.lock().unwrap();
    let app = std::path::PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let model = std::path::PathBuf::from(std::env::var_os("LOXA_SMALL_MODEL").unwrap());
    let executable = app.join("Contents/MacOS/loxa-app");
    let root = std::env::temp_dir().join(format!(
        "loxa-final-threaded-acceptance-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let loxa_home = root.join("loxa-home");
    let hostile_path_dir = root.join("hostile-path");
    let hostile_path = hostile_path_dir.join("llama-server");
    let hostile_environment = root.join("hostile-environment");
    let hostile_home = loxa_home.join("runtimes/llama.cpp/b10121/llama-server");
    let witnesses = [
        (&hostile_path, root.join("path-witness-ran")),
        (&hostile_environment, root.join("environment-witness-ran")),
        (&hostile_home, root.join("home-witness-ran")),
    ];
    for (script, marker) in &witnesses {
        std::fs::create_dir_all(script.parent().unwrap()).unwrap();
        let bytes = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'version: 10121 (555881ebc)' >&2; exit 0; fi\nprintf HOSTILE > '{}'\nexit 97\n",
            marker.display()
        );
        std::fs::write(script, bytes.as_bytes()).unwrap();
        std::fs::set_permissions(script, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let hostile_bytes = witnesses
        .iter()
        .map(|(script, _)| std::fs::read(script).unwrap())
        .collect::<Vec<_>>();

    let launch =
        super::ApplicationLaunch::from_values(&executable, Some(&loxa_home), None).unwrap();
    let (backend_paths, runtime_paths) = launch.worker_paths();
    assert_eq!(backend_paths, runtime_paths);
    assert_eq!(
        runtime_paths.managed_server,
        app.join("Contents/MacOS/llama-server")
    );
    assert_eq!(
        runtime_paths.runtime_inventory,
        Some(app.join("Contents/Resources/loxa-runtime/b10344/inventory.json"))
    );

    let id = "loxa-runtime-small-acceptance";
    install_small_model(&runtime_paths.models, id, &model);

    let hostile_search_path = std::env::join_paths([
        hostile_path_dir.as_path(),
        std::path::Path::new("/usr/bin"),
        std::path::Path::new("/bin"),
    ])
    .unwrap();
    let _variables = ScopedEnvironment::set([
        ("LOXA_HOME", Some(loxa_home.as_os_str())),
        ("HOME", Some(root.as_os_str())),
        ("PATH", Some(hostile_search_path.as_os_str())),
        ("LOXA_LLAMA_SERVER", Some(hostile_environment.as_os_str())),
        (
            "HTTP_PROXY",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "HTTPS_PROXY",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "ALL_PROXY",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "NO_PROXY",
            Some(std::ffi::OsStr::new("127.0.0.1,localhost")),
        ),
    ]);

    let mut backend = BackendClient::start(backend_paths);
    let mut runtime = ApiRuntimeController::start(runtime_paths.clone());
    let observation_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut saw_installed = false;
    let mut saw_idle = false;
    while !(saw_installed && saw_idle) {
        for message in backend.drain(std::time::Instant::now()) {
            match message {
                BackendMessage::Installed {
                    result: Ok(items), ..
                } => {
                    saw_installed = items.iter().any(|item| item.id() == id);
                }
                BackendMessage::Observation(ObservationMessage::Snapshot(snapshot)) => {
                    saw_idle |= snapshot.runtime_label() == "Inference: Idle";
                }
                _ => {}
            }
        }
        assert!(
            std::time::Instant::now() < observation_deadline,
            "real backend worker did not publish installed and idle observations"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(runtime.request_start(id.into()));
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(45);
    let port = loop {
        let _ = runtime.drain();
        if let ApiRuntimePhase::Ready { endpoint, .. } = runtime.phase() {
            break endpoint.port();
        }
        assert!(
            std::time::Instant::now() < ready_deadline,
            "real API controller did not become ready: {:?} / {:?}",
            runtime.phase(),
            runtime.notice()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let child_pid = u32::try_from(lease_number(
        &runtime_paths.run.join("foreground.json"),
        "child_pid",
    ))
    .unwrap();

    let models = request(
        port,
        b"GET /v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    assert!(
        models.starts_with(b"HTTP/1.1 200") && String::from_utf8_lossy(&models).contains(id),
        "{}",
        String::from_utf8_lossy(&models)
    );
    let body = format!(
        r#"{{"model":"{id}","messages":[{{"role":"user","content":"Reply with OK."}}],"max_tokens":8,"stream":false}}"#
    );
    let chat = request(
        port,
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    );
    assert!(
        chat.starts_with(b"HTTP/1.1 200") && String::from_utf8_lossy(&chat).contains("\"choices\""),
        "{}",
        String::from_utf8_lossy(&chat)
    );

    assert!(backend.request_popover_open(std::time::Instant::now()));
    let running_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut observed_running = false;
    while !observed_running {
        for message in backend.drain(std::time::Instant::now()) {
            if let BackendMessage::Observation(ObservationMessage::Snapshot(snapshot)) = message {
                if let Some(observed) = snapshot.observed_runtime() {
                    let exact_runtime = observed.owner() == ObservedRuntimeOwner::PersistentApp
                        && observed.model_id() == id
                        && observed.port() == port;
                    if exact_runtime {
                        assert_eq!(
                            snapshot.runtime_inventory_for_test(),
                            Some(RuntimeInventory::Missing),
                            "the backend worker did not retain managed bundled provenance"
                        );
                        observed_running = true;
                    }
                }
            }
        }
        assert!(
            std::time::Instant::now() < running_deadline,
            "real backend worker did not observe the API worker's runtime"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(runtime.request_stop());
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let _ = runtime.drain();
        if matches!(runtime.phase(), ApiRuntimePhase::Idle) {
            break;
        }
        assert!(
            std::time::Instant::now() < stop_deadline,
            "real API controller did not return to idle: {:?}",
            runtime.phase()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(runtime.shutdown_and_join(), Ok(()));
    assert_eq!(backend.shutdown_and_join(), Ok(()));

    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(!runtime_paths.run.join("foreground.json").exists());
    assert!(process_or_group_is_gone(child_pid, false));
    assert!(process_or_group_is_gone(child_pid, true));
    for ((script, marker), expected) in witnesses.iter().zip(hostile_bytes) {
        assert_eq!(std::fs::read(script).unwrap(), expected);
        assert!(
            !marker.exists(),
            "hostile runtime was executed: {}",
            script.display()
        );
    }
    assert!(!loxa_home.join("runtimes/llama.cpp/b10344").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn bundled_sigkill_owner_child() {
    use loxa::api_runtime::{ApiRuntimeHost, ApiStartCancellation, ApiStartOutcome};

    let Some(loxa_home) = std::env::var_os("LOXA_BUNDLED_OWNER_ROOT") else {
        return;
    };
    let app = std::path::PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let ready = std::path::PathBuf::from(std::env::var_os("LOXA_BUNDLED_OWNER_READY").unwrap());
    let launch = super::ApplicationLaunch::from_values(
        &app.join("Contents/MacOS/loxa-app"),
        Some(std::path::Path::new(&loxa_home)),
        None,
    )
    .unwrap();
    let (_, paths) = launch.worker_paths();
    let mut host = ApiRuntimeHost::new(paths);
    let endpoint = match host
        .start("loxa-runtime-small-recovery", &ApiStartCancellation::new())
        .unwrap()
    {
        ApiStartOutcome::Started(endpoint) => endpoint,
        outcome => panic!("unexpected child start outcome: {outcome:?}"),
    };
    std::fs::write(ready, endpoint.port().to_string()).unwrap();
    std::hint::black_box(&host);
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
#[ignore = "requires a finalized built app and the exact small-model fixture"]
fn finalized_app_recovers_embedded_runtime_after_owner_sigkill_and_relaunches() {
    use loxa::api_runtime::{
        ApiRuntimeHost, ApiStartCancellation, ApiStartOutcome, ApiStopOutcome,
    };
    use std::os::unix::process::ExitStatusExt as _;

    let app = std::path::PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let model = std::path::PathBuf::from(std::env::var_os("LOXA_SMALL_MODEL").unwrap());
    let root = std::env::temp_dir().join(format!(
        "loxa-final-app-recovery-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let loxa_home = root.join("loxa-home");
    let model_dir = loxa_home.join("models/loxa-runtime-small-recovery");
    let ready = root.join("owner-ready");
    std::fs::create_dir_all(&model_dir).unwrap();
    assert_eq!(
        std::fs::copy(&model, model_dir.join("model.gguf")).unwrap(),
        88_202_080
    );
    std::fs::write(
        model_dir.join("manifest.json"),
        r#"{
  "version": 1,
  "id": "loxa-runtime-small-recovery",
  "repo": "bartowski/SmolLM2-135M-Instruct-GGUF",
  "revision": "09816acd5d99df7be770d85ea30822623dab342c",
  "remote_filename": "SmolLM2-135M-Instruct-Q2_K.gguf",
  "local_filename": "model.gguf",
  "sha256": "741ad12b64088fedc17c33aacb22e48be1972ef36a39f03666dd68bd15614fb9",
  "size": 88202080
}
"#,
    )
    .unwrap();
    drop(loxa::catalog::ModelLock::acquire(&model_dir).unwrap());

    let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "app::runtime_acceptance_tests::bundled_sigkill_owner_child",
            "--nocapture",
        ])
        .env("LOXA_BUILT_APP", &app)
        .env("LOXA_SMALL_MODEL", &model)
        .env("LOXA_BUNDLED_OWNER_ROOT", &loxa_home)
        .env("LOXA_BUNDLED_OWNER_READY", &ready)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !ready.is_file() {
        if let Some(status) = owner.try_wait().unwrap() {
            let output = owner.wait_with_output().unwrap();
            panic!("owner exited before ready: {status}; {output:?}");
        }
        if std::time::Instant::now() >= deadline {
            let _ = owner.kill();
            let output = owner.wait_with_output().unwrap();
            panic!("owner readiness timed out: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let old_port = std::fs::read_to_string(&ready)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let lease_path = loxa_home.join("run/foreground.json");
    let old_child = u32::try_from(lease_number(&lease_path, "child_pid")).unwrap();
    assert!(TcpStream::connect(("127.0.0.1", old_port)).is_ok());
    owner.kill().unwrap();
    let output = owner.wait_with_output().unwrap();
    assert_eq!(output.status.signal(), Some(9), "{output:?}");
    assert!(lease_path.is_file());
    assert!(!process_or_group_is_gone(old_child, false));

    let launch = super::ApplicationLaunch::from_values(
        &app.join("Contents/MacOS/loxa-app"),
        Some(&loxa_home),
        None,
    )
    .unwrap();
    let (_, paths) = launch.worker_paths();
    let mut relaunched = ApiRuntimeHost::new(paths);
    let endpoint = match relaunched
        .start("loxa-runtime-small-recovery", &ApiStartCancellation::new())
        .unwrap()
    {
        ApiStartOutcome::Started(endpoint) => endpoint,
        outcome => panic!("unexpected relaunch outcome: {outcome:?}"),
    };
    let new_child = u32::try_from(lease_number(&lease_path, "child_pid")).unwrap();
    assert_ne!(new_child, old_child);
    assert!(process_or_group_is_gone(old_child, false));
    assert!(process_or_group_is_gone(old_child, true));
    assert_eq!(relaunched.stop().unwrap(), ApiStopOutcome::Stopped);
    assert!(TcpStream::connect(("127.0.0.1", endpoint.port())).is_err());
    assert!(!lease_path.exists());
    std::fs::remove_dir_all(root).unwrap();
}
