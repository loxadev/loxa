use super::arguments::resolve_requested_port;
#[cfg(unix)]
use super::child::{process_group_exists, terminate_owned_group, LAST_GUARDED_GROUP};
#[cfg(target_os = "macos")]
use super::discovery::probe_validated_version_with_timeout_for_test;
use super::discovery::{
    managed_version_first_line, probe_validated_version, probe_version_with_timeout,
    VERSION_PROBE_TIMEOUT,
};
use super::foreground::{
    ready_line, start_foreground_with, start_foreground_with_signal, stopped_for_signal,
};
use super::launch::report_mtp_draft_start_failure;
use super::output::{
    install_reader_spawn_fault_for_test, validate_announcement_line, ReaderSpawnFault,
    MAX_DIAGNOSTIC_TAIL,
};
use super::owned::readiness::{
    models_reader_has_alias, readiness, readiness_client, MAX_MODELS_BODY,
};
use super::owned::StartOutcome;
#[cfg(unix)]
use super::signal::{
    deactivate_server, pack_server_identity, process_termination_signal,
    reset_process_termination_signal_for_test, unpack_server_identity, ACTIVE_SERVER,
    PROCESS_TERMINATION_SIGNAL,
};
use super::*;
use crate::catalog::{Artifact, ArtifactProvenance, ArtifactRole, Manifest};
use crate::paths::AppPaths;
use crate::runnable::Runnable;
use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
use crate::runtime_identity::RuntimeIdentity;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[cfg(not(unix))]
#[test]
fn bundled_runtime_is_a_closed_refusal_on_non_unix_targets() {
    let root = std::path::PathBuf::from(r"C:\loxa-test");
    let paths = AppPaths {
        models: root.join("models"),
        config: root.join("config.json"),
        run: root.join("run"),
        logs: root.join("logs"),
        runtimes: root.join("runtimes"),
        managed_server: root.join("Loxa.app/Contents/MacOS/llama-server"),
        runtime_identity: RuntimeIdentity::BundledB10344,
        runtime_inventory: Some(
            root.join("Loxa.app/Contents/Resources/loxa-runtime/b10344/inventory.json"),
        ),
        root,
    };

    assert_eq!(
        validate_managed_runtime(&paths).unwrap_err(),
        "bundled b10344 runtime is supported only on Unix"
    );
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires a finalized built app"]
fn bundled_validation_keeps_the_inspected_closure_for_later_execution() {
    let _process = process_test_lock();
    let built_app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let root = tempdir().unwrap();
    let app = root.path().join("Loxa.app");
    let copied = std::process::Command::new("/usr/bin/ditto")
        .args([built_app.as_os_str(), app.as_os_str()])
        .status()
        .unwrap();
    assert!(copied.success());
    let executable = app.join("Contents/MacOS/loxa-app");
    let paths =
        AppPaths::from_application_values(&executable, Some(&root.path().join("loxa-home")), None)
            .unwrap();

    let validated = validate_managed_runtime(&paths).unwrap();
    let staged_server = validated.execution_server();
    assert_ne!(staged_server, paths.managed_server);
    assert!(staged_server.is_file());
    let stage = staged_server
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf();
    assert!(
        stage
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".bundled-runtime-exec-"),
        "{}",
        stage.display()
    );
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&stage).unwrap().permissions())
            & 0o777,
        0o700
    );
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(&staged_server).unwrap().permissions()
        ) & 0o777,
        0o555
    );
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(stage.join("Contents/Resources/loxa-runtime/b10344/inventory.json"))
                .unwrap()
                .permissions()
        ) & 0o777,
        0o444
    );
    let contents = app.join("Contents");
    std::fs::rename(contents.join("MacOS"), contents.join("MacOS.inspected")).unwrap();
    std::fs::rename(
        contents.join("Frameworks"),
        contents.join("Frameworks.inspected"),
    )
    .unwrap();
    std::fs::create_dir(contents.join("MacOS")).unwrap();
    std::fs::create_dir(contents.join("Frameworks")).unwrap();
    let witness = root.path().join("replacement-helper-ran");
    write_executable_script(
        &contents.join("MacOS/llama-server"),
        format!(
            "#!/bin/sh\nprintf replacement > '{}'\nprintf '%s\\n' '{}' >&2\n",
            witness.display(),
            RuntimeIdentity::BundledB10344.version_line(),
        )
        .as_bytes(),
    );

    let first_line = probe_validated_version(&validated)
        .and_then(managed_version_first_line)
        .unwrap();
    assert_eq!(first_line, RuntimeIdentity::BundledB10344.version_line(),);
    assert!(
        !witness.exists(),
        "the post-validation command reopened the replaced source closure"
    );
    drop(validated);
    assert!(!stage.exists(), "prepared runtime stage was not cleaned");
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires a finalized built app and the exact small-model fixture"]
fn bundled_prepared_closure_starts_the_exact_small_model() {
    let _process = process_test_lock();
    let app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let model = PathBuf::from(std::env::var_os("LOXA_SMALL_MODEL").unwrap());
    let root = tempdir().unwrap();
    let paths = AppPaths::from_application_values(
        &app.join("Contents/MacOS/loxa-app"),
        Some(root.path()),
        None,
    )
    .unwrap();
    let runtime = validate_managed_runtime(&paths).unwrap();
    let launch = Launch {
        server: runtime.source_server().to_path_buf(),
        managed_runtime: Some(runtime),
        model,
        id: "loxa-runtime-stage-smoke".into(),
        requested_port: 0,
        ctx: 4096,
        profile: LaunchProfile::generic(),
        policy: LaunchPolicy::PersistentApp,
    };

    match OwnedServer::start_inner(&launch, Duration::from_secs(20), None, || None).unwrap() {
        StartOutcome::Ready(mut server) => server.terminate().unwrap(),
        StartOutcome::Exited(exit) => panic!("prepared runtime exited: {exit:?}"),
        StartOutcome::Signaled(signal) => panic!("prepared runtime signaled: {signal}"),
        StartOutcome::Interrupted(interruption) => {
            panic!("prepared runtime interrupted: {interruption:?}")
        }
        StartOutcome::CleanupFailed(_) => panic!("prepared runtime cleanup failed"),
    }
}

#[cfg(unix)]
static RUN_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(unix)]
fn process_test_lock() -> std::sync::MutexGuard<'static, ()> {
    RUN_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(unix)]
#[test]
fn version_reader_spawn_failure_kills_and_waits_for_the_exact_child_group() {
    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let server = root.path().join("server");
    write_executable_script(&server, b"#!/bin/sh\nwhile :; do :; done\n");
    let _fault = install_reader_spawn_fault_for_test(ReaderSpawnFault::Fail);

    let error = probe_version_with_timeout(&server, Duration::from_secs(5)).unwrap_err();
    let group = LAST_GUARDED_GROUP.load(Ordering::SeqCst);
    assert!(group > 1, "the version child was not immediately guarded");
    let group_survived = crate::runtime::process_group_has_live_members(group).unwrap();
    if group_survived {
        crate::runtime::terminate_stale_process_group(group).unwrap();
    }

    assert!(
        error.contains("injected output reader spawn failure"),
        "{error}"
    );
    assert!(
        !group_survived,
        "the version-probe group survived reader creation failure"
    );
}

#[cfg(unix)]
#[test]
fn owned_start_reader_spawn_panic_drops_the_group_and_runtime_lock() {
    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let server = root.path().join("server");
    let run_dir = root.path().join("run");
    write_executable_script(&server, b"#!/bin/sh\nwhile :; do :; done\n");
    let launch = Launch::generic(&server, Path::new("/models/model.gguf"), "demo", 0, 4096);
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let _fault = install_reader_spawn_fault_for_test(ReaderSpawnFault::Panic);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ =
            OwnedServer::start_with_ownership(&launch, Duration::from_secs(5), ownership, || None);
    }));
    let group = LAST_GUARDED_GROUP.load(Ordering::SeqCst);
    assert!(group > 1, "the owned child was not immediately guarded");
    let group_survived = crate::runtime::process_group_has_live_members(group).unwrap();
    if group_survived {
        crate::runtime::terminate_stale_process_group(group).unwrap();
    }

    assert!(panic.is_err(), "the injected reader panic did not unwind");
    assert!(
        !group_survived,
        "the owned server group survived reader creation panic"
    );
    assert_eq!(ACTIVE_SERVER.load(Ordering::SeqCst), 0);
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_reader_error_with_cleanup_failure_returns_retryable_server() {
    if std::env::var_os("LOXA_PERSISTENT_READER_CLEANUP_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runner::tests::persistent_reader_error_with_cleanup_failure_returns_retryable_server",
                    "--nocapture",
                ])
                .env("LOXA_PERSISTENT_READER_CLEANUP_CHILD", "1")
                .output()
                .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }

    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let server = root.path().join("server");
    let run_dir = root.path().join("run");
    write_executable_script(&server, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
    let runnable = persistent_runnable(root.path(), &server, 0);
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let _reader_fault = install_reader_spawn_fault_for_test(ReaderSpawnFault::Fail);
    let _termination_fault = crate::runtime::fail_next_owned_group_terminations_for_test(1);

    let started = start_persistent_with_ownership(
        runnable,
        &ownership,
        || false,
        PersistentSignalPolicy::CallerManaged,
    );
    let mut owned = match started {
        Ok(PersistentStart::CleanupFailed(owned)) => owned,
        Ok(_) => panic!("reader start failure did not retain cleanup ownership"),
        Err(error) => panic!("reader start cleanup failure was flattened: {error:?}"),
    };
    let group = owned.server.group;
    assert!(crate::runtime::process_group_has_live_members(group).unwrap());
    assert!(ownership.reserve_child().is_err());
    assert!(crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err());
    assert!(crate::catalog::ModelLock::acquire(&root.path().join("models/demo")).is_err());

    owned.terminate().unwrap();

    assert!(!crate::runtime::process_group_has_live_members(group).unwrap());
    drop(owned);
    let next_child = ownership.reserve_child().unwrap();
    drop(next_child);
    drop(ownership);
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&root.path().join("models/demo")).unwrap());
}

#[cfg(target_os = "macos")]
#[test]
fn version_probe_one_shot_termination_failure_retries_before_releasing_stage() {
    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let run_dir = root.path().join("run");
    let stage = run_dir.join(".bundled-runtime-exec-11111111111111111111111111111111");
    let server = stage.join("Contents/MacOS/llama-server");
    std::fs::create_dir_all(server.parent().unwrap()).unwrap();
    build_prelease_test_server(root.path(), &server);
    let prepared = crate::runtime_bundle::PreparedRuntime::for_test(stage.clone()).unwrap();
    let validated = ValidatedManagedRuntime::bundled(
        PathBuf::from("/Applications/Loxa.app/Contents/MacOS/llama-server"),
        prepared,
    );
    let _fault = crate::runtime::fail_next_owned_group_terminations_for_test(1);

    let error = probe_validated_version_with_timeout_for_test(&validated, Duration::from_secs(1))
        .unwrap_err();
    let group = LAST_GUARDED_GROUP.load(Ordering::SeqCst);
    let descendant_was_started = root.path().join("descendant-pid").is_file();
    let group_survived = process_group_exists(group).unwrap();
    drop(validated);
    let stage_survived = stage.exists();
    if group_survived {
        crate::runtime::terminate_stale_process_group(group).unwrap();
    }
    if stage_survived {
        std::fs::remove_dir_all(&stage).unwrap();
    }

    assert!(error.contains("injected owned process-group termination failure"));
    assert!(
        descendant_was_started,
        "version probe did not start its descendant"
    );
    assert!(
        !group_survived,
        "one-shot cleanup retry left the process group alive"
    );
    assert!(
        !stage_survived,
        "stage survived after the exact group was gone"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn version_probe_persistent_termination_failure_is_reaped_on_next_acquire() {
    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let run_dir = root.path().join("run");
    let stage = run_dir.join(".bundled-runtime-exec-22222222222222222222222222222222");
    let server = stage.join("Contents/MacOS/llama-server");
    std::fs::create_dir_all(server.parent().unwrap()).unwrap();
    build_prelease_test_server(root.path(), &server);
    let prepared = crate::runtime_bundle::PreparedRuntime::for_test(stage.clone()).unwrap();
    let validated = ValidatedManagedRuntime::bundled(
        PathBuf::from("/Applications/Loxa.app/Contents/MacOS/llama-server"),
        prepared,
    );
    let _fault = crate::runtime::fail_next_owned_group_terminations_for_test(2);

    let error = probe_validated_version_with_timeout_for_test(&validated, Duration::from_secs(1))
        .unwrap_err();
    let group = LAST_GUARDED_GROUP.load(Ordering::SeqCst);
    let descendant_was_started = root.path().join("descendant-pid").is_file();
    let group_was_retained = crate::runtime::process_group_has_live_members(group).unwrap();
    drop(validated);
    let stage_was_retained = stage.is_dir();
    let abandoned = std::fs::read(stage.join(".loxa-execution-stage-v1"))
        .ok()
        .is_some_and(|bytes| bytes.get(crate::runtime_bundle::STAGE_STATE_OFFSET) == Some(&1));
    let recovered = crate::runtime::RuntimeOwnership::acquire(&run_dir);
    let recovery_error = recovered.as_ref().err().cloned();
    let group_survived = crate::runtime::process_group_has_live_members(group).unwrap();
    let stage_survived = stage.exists();
    drop(recovered.ok());
    if group_survived {
        crate::runtime::terminate_stale_process_group(group).unwrap();
    }
    if stage_survived {
        std::fs::remove_dir_all(&stage).unwrap();
    }

    assert!(error.contains("injected owned process-group termination failure"));
    assert!(
        descendant_was_started,
        "version probe did not start its descendant"
    );
    assert!(
        group_was_retained,
        "persistent failure did not retain the live group"
    );
    assert!(
        stage_was_retained,
        "persistent failure deleted the live group's stage"
    );
    assert!(
        abandoned,
        "persistent failure did not publish an abandoned record"
    );
    assert!(
        recovery_error.is_none(),
        "next acquire failed: {recovery_error:?}"
    );
    assert!(
        !group_survived,
        "next acquire left the abandoned group alive"
    );
    assert!(
        !stage_survived,
        "next acquire left the abandoned stage behind"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn real_start_persistent_termination_failure_retains_lease_and_stage_until_recovery() {
    let _process = process_test_lock();
    let root = tempdir().unwrap();
    let run_dir = root.path().join("run");
    let lease = run_dir.join("foreground.json");
    let stage = run_dir.join(".bundled-runtime-exec-33333333333333333333333333333333");
    let server_path = stage.join("Contents/MacOS/llama-server");
    std::fs::create_dir_all(server_path.parent().unwrap()).unwrap();
    build_prelease_test_server(root.path(), &server_path);
    let prepared = crate::runtime_bundle::PreparedRuntime::for_test(stage.clone()).unwrap();
    let managed = ValidatedManagedRuntime::bundled(
        PathBuf::from("/Applications/Loxa.app/Contents/MacOS/llama-server"),
        prepared,
    );
    let port = resolve_requested_port(0).unwrap();
    let launch = Launch {
        server: managed.source_server().to_path_buf(),
        managed_runtime: Some(managed),
        model: root.path().join("model.gguf"),
        id: "demo".into(),
        requested_port: port,
        ctx: 4096,
        profile: LaunchProfile::generic(),
        policy: LaunchPolicy::Foreground,
    };
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let mut owned = match OwnedServer::start_with_ownership(
        &launch,
        Duration::from_secs(5),
        ownership,
        || None,
    )
    .unwrap()
    {
        StartOutcome::Ready(owned) => owned,
        StartOutcome::Exited(exit) => panic!("bundled descendant server exited: {exit:?}"),
        StartOutcome::Signaled(signal) => {
            panic!("bundled descendant server was signaled: {signal}")
        }
        StartOutcome::Interrupted(interruption) => {
            panic!("bundled descendant server was interrupted: {interruption:?}")
        }
        StartOutcome::CleanupFailed(_) => {
            panic!("bundled descendant server cleanup failed during startup")
        }
    };
    let group = owned.group;
    assert!(root.path().join("descendant-pid").is_file());
    let _fault = crate::runtime::fail_next_owned_group_terminations_for_test(3);

    let first_error = owned.terminate().unwrap_err();
    let group_was_retained = crate::runtime::process_group_has_live_members(group).unwrap();
    let stage_while_owned = stage.is_dir();
    let lease_while_owned = lease.is_file();
    let ownership_conflicted = crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err();
    drop(owned);
    let abandoned = std::fs::read(stage.join(".loxa-execution-stage-v1"))
        .ok()
        .is_some_and(|bytes| bytes.get(crate::runtime_bundle::STAGE_STATE_OFFSET) == Some(&1));
    drop(launch);
    let stage_after_owner_drop = stage.is_dir();
    let lease_after_owner_drop = lease.is_file();

    let recovered = crate::runtime::RuntimeOwnership::acquire(&run_dir);
    let recovery_error = recovered.as_ref().err().cloned();
    let group_survived = crate::runtime::process_group_has_live_members(group).unwrap();
    let listener_survived = std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
    let stage_survived = stage.exists();
    let lease_survived = lease.exists();
    drop(recovered.ok());
    if group_survived {
        crate::runtime::terminate_stale_process_group(group).unwrap();
    }
    if lease.exists() {
        std::fs::remove_file(&lease).unwrap();
    }
    if stage.exists() {
        std::fs::remove_dir_all(&stage).unwrap();
    }
    deactivate_server(u32::try_from(group).unwrap(), group);

    assert_eq!(
        first_error,
        "injected owned process-group termination failure"
    );
    assert!(
        group_was_retained,
        "termination failure did not retain the group"
    );
    assert!(
        stage_while_owned,
        "termination failure deleted the live stage"
    );
    assert!(
        lease_while_owned,
        "termination failure deleted the live lease"
    );
    assert!(
        ownership_conflicted,
        "live ownership was released before guard drop"
    );
    assert!(abandoned, "guard drop did not publish the abandoned record");
    assert!(
        stage_after_owner_drop,
        "guard drop deleted the live group's stage"
    );
    assert!(
        lease_after_owner_drop,
        "guard drop deleted the live group's lease"
    );
    assert!(
        recovery_error.is_none(),
        "next acquire failed: {recovery_error:?}"
    );
    assert!(!group_survived, "next acquire left the exact group alive");
    assert!(!listener_survived, "next acquire left the listener alive");
    assert!(!stage_survived, "next acquire left the stage behind");
    assert!(!lease_survived, "next acquire left the lease behind");
}

#[derive(Clone)]
struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(unix)]
fn write_executable_script(path: &Path, script: &[u8]) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
fn write_version_script(path: &Path, first_line: &str, exit_code: i32) {
    assert!(!first_line.contains('\''));
    write_executable_script(
        path,
        format!("#!/bin/sh\nprintf '%s\\n' '{first_line}' >&2\nexit {exit_code}\n").as_bytes(),
    );
}

#[cfg(unix)]
fn write_dual_stream_version_script(path: &Path) {
    write_executable_script(
        path,
        b"#!/bin/sh\nprintf '%s\\n' 'version: usable'\nprintf '%s\\n' 'harmless warning' >&2\n",
    );
}

fn serve_models(alias: &'static str) -> (u16, std::thread::JoinHandle<()>) {
    serve_models_with_ready_action(alias, || {})
}

fn serve_models_with_ready_action<F>(
    alias: &'static str,
    ready: F,
) -> (u16, std::thread::JoinHandle<()>)
where
    F: FnOnce() + Send + 'static,
{
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        assert!(
            request.starts_with(b"GET /v1/models HTTP/1.1\r\n"),
            "{}",
            String::from_utf8_lossy(&request)
        );
        ready();
        let body = format!(r#"{{"data":[{{"id":"{alias}"}}]}}"#);
        write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
    });
    (port, server)
}

#[cfg(unix)]
fn test_mtp_launch(server: &Path, port: u16) -> Launch {
    Launch {
        server: server.to_path_buf(),
        managed_runtime: None,
        model: PathBuf::from("/models/model.gguf"),
        id: "demo".into(),
        requested_port: port,
        ctx: 8192,
        profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
        policy: LaunchPolicy::Foreground,
    }
}

#[cfg(unix)]
fn persistent_runnable(root: &Path, server: &Path, port: u16) -> Runnable {
    let model_dir = root.join("models/demo");
    std::fs::create_dir_all(&model_dir).unwrap();
    let manifest = Manifest {
        version: 1,
        id: "demo".into(),
        repo: Some("owner/repo".into()),
        revision: Some("0".repeat(40)),
        remote_filename: Some("model.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 1,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let policy = LaunchPolicy::PersistentApp;
    let launch = Launch {
        server: server.to_path_buf(),
        managed_runtime: None,
        model: model_dir.join("model.gguf"),
        id: manifest.id.clone(),
        requested_port: port,
        ctx: 4096,
        profile: LaunchProfile::generic(),
        policy,
    };
    let fingerprint = RuntimeFingerprint::from_manifest(
        &manifest,
        launch.ctx,
        launch.profile.effective_profile(),
        policy.sleep_idle_seconds(),
    )
    .unwrap();
    Runnable::for_test(
        crate::catalog::ModelLock::acquire(&model_dir).unwrap(),
        launch,
        fingerprint,
    )
}

#[cfg(unix)]
fn persistent_mtp_runnable(root: &Path, server: &Path, port: u16) -> Runnable {
    let model_dir = root.join("models/demo");
    std::fs::create_dir_all(&model_dir).unwrap();
    let manifest = Manifest {
        version: 3,
        id: "demo".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 1,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: "a".repeat(64),
                size: 1,
                provenance: ArtifactProvenance::Local {
                    source_filename: "model-source.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: "b".repeat(64),
                size: 1,
                provenance: ArtifactProvenance::Local {
                    source_filename: "draft-source.gguf".into(),
                },
            },
        ]),
        profile: Some(crate::catalog::TEST_MTP_PROFILE.into()),
        runtime: Some(crate::catalog::RuntimeQualification {
            engine: "llama.cpp".into(),
            build: crate::catalog::TEST_LLAMA_BUILD.into(),
        }),
    };
    let policy = LaunchPolicy::PersistentApp;
    let launch = Launch {
        server: server.to_path_buf(),
        managed_runtime: None,
        model: model_dir.join("model.gguf"),
        id: manifest.id.clone(),
        requested_port: port,
        ctx: 8192,
        profile: LaunchProfile::gemma4_mtp(Some(model_dir.join("draft.gguf"))),
        policy,
    };
    let fingerprint = RuntimeFingerprint::from_manifest(
        &manifest,
        launch.ctx,
        launch.profile.effective_profile(),
        policy.sleep_idle_seconds(),
    )
    .unwrap();
    Runnable::for_test(
        crate::catalog::ModelLock::acquire(&model_dir).unwrap(),
        launch,
        fingerprint,
    )
}

#[cfg(unix)]
fn write_persistent_test_server(path: &Path, announced: &Path, ready: &Path) {
    let executable = std::env::current_exe().unwrap();
    for value in [executable.as_path(), announced, ready] {
        assert!(!value.to_string_lossy().contains('\''));
    }
    write_executable_script(
            path,
            format!(
                "#!/bin/sh\nport=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--port' ]; then shift; port=\"$1\"; fi\n  shift\ndone\nexport LOXA_PERSISTENT_SERVER_CHILD=1\nexport LOXA_PERSISTENT_SERVER_PORT=\"$port\"\nexport LOXA_PERSISTENT_ANNOUNCED='{}'\nexport LOXA_PERSISTENT_READY='{}'\nexec '{}' --exact runner::tests::persistent_server_child --nocapture\n",
                announced.display(),
                ready.display(),
                executable.display(),
            )
            .as_bytes(),
        );
}

#[cfg(target_os = "macos")]
fn build_prelease_test_server(root: &Path, path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    let source = root.join("prelease-server.c");
    std::fs::write(
            &source,
            br#"#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static void write_file(const char *name, const char *value) {
    const char *path = getenv(name);
    if (path == NULL) return;
    FILE *file = fopen(path, "w");
    if (file == NULL || fputs(value, file) == EOF || fclose(file) != 0) exit(90);
}

int main(int argc, char **argv) {
    int port = 0;
    int version = 0;
    for (int index = 1; index + 1 < argc; ++index) {
        if (strcmp(argv[index], "--port") == 0) port = atoi(argv[index + 1]);
    }
    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--version") == 0) version = 1;
    }
    if (!version && (port <= 0 || port > 65535)) return 91;
    pid_t descendant = fork();
    if (descendant < 0) return 97;
    if (descendant == 0) {
        signal(SIGTERM, SIG_IGN);
        for (;;) pause();
    }
    char descendant_pid[32];
    snprintf(descendant_pid, sizeof(descendant_pid), "%d", descendant);
    FILE *descendant_file = fopen(LOXA_DESCENDANT_PATH, "w");
    if (descendant_file == NULL || fputs(descendant_pid, descendant_file) == EOF || fclose(descendant_file) != 0) return 98;
    if (version) for (;;) pause();
    char pid[32];
    snprintf(pid, sizeof(pid), "%d", getpid());
    write_file("LOXA_PRELEASE_CHILD_PID", pid);
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    int enabled = 1;
    if (listener < 0 || setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) return 92;
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons((unsigned short)port);
    if (bind(listener, (struct sockaddr *)&address, sizeof(address)) != 0 || listen(listener, 1) != 0) return 93;
    fprintf(stderr, "test server listening on http://127.0.0.1:%d\n", port);
    fflush(stderr);
    write_file("LOXA_PRELEASE_ANNOUNCED", "announced");
    int client = accept(listener, NULL, NULL);
    if (client < 0) return 94;
    char request[4096];
    if (recv(client, request, sizeof(request), 0) <= 0) return 95;
    const char *body = "{\"data\":[{\"id\":\"demo\"}]}";
    char response[512];
    int length = snprintf(response, sizeof(response), "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s", strlen(body), body);
    if (length <= 0 || send(client, response, (size_t)length, 0) != length) return 96;
    close(client);
    for (;;) pause();
}
"#,
        )
        .unwrap();
    let output = std::process::Command::new("/usr/bin/clang")
        .args(["-std=c11", "-O0", "-Wall", "-Werror"])
        .arg(format!(
            "-DLOXA_DESCENDANT_PATH=\"{}\"",
            root.join("descendant-pid").display()
        ))
        .arg(&source)
        .arg("-o")
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500)).unwrap();
}

#[cfg(unix)]
fn write_mtp_persistent_test_server(path: &Path, argv: &Path, announced: &Path, ready: &Path) {
    let executable = std::env::current_exe().unwrap();
    for value in [executable.as_path(), argv, announced, ready] {
        assert!(!value.to_string_lossy().contains('\''));
    }
    write_executable_script(
            path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'draft startup failed\\n' >&2; exit 42 ;;\nesac\nport=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--port' ]; then shift; port=\"$1\"; fi\n  shift\ndone\nexport LOXA_PERSISTENT_SERVER_CHILD=1\nexport LOXA_PERSISTENT_SERVER_PORT=\"$port\"\nexport LOXA_PERSISTENT_ANNOUNCED='{}'\nexport LOXA_PERSISTENT_READY='{}'\nexec '{}' --exact runner::tests::persistent_server_child --nocapture\n",
                argv.display(),
                announced.display(),
                ready.display(),
                executable.display(),
            )
            .as_bytes(),
        );
}

#[cfg(unix)]
#[test]
fn persistent_server_child() {
    if std::env::var_os("LOXA_PERSISTENT_SERVER_CHILD").is_none() {
        return;
    }
    let port = std::env::var("LOXA_PERSISTENT_SERVER_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let announced = PathBuf::from(std::env::var_os("LOXA_PERSISTENT_ANNOUNCED").unwrap());
    let ready = PathBuf::from(std::env::var_os("LOXA_PERSISTENT_READY").unwrap());
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    eprintln!("test server listening on http://127.0.0.1:{port}");
    std::fs::write(announced, b"announced").unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
    }
    std::fs::write(ready, b"ready").unwrap();
    let body = r#"{"data":[{"id":"demo"}]}"#;
    write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    drop(stream);
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn persistent_signal_owner_child() {
    let Some(root) = std::env::var_os("LOXA_PERSISTENT_SIGNAL_OWNER_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let server = root.join("server");
    let announced = root.join("announced");
    let ready = root.join("ready");
    let run_dir = root.join("run");
    let owner_ready = root.join("owner-ready");
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(&root, &server, 0);

    let runtime = match start_persistent(runnable, &run_dir, || false).unwrap() {
        PersistentStart::Ready(runtime) => runtime,
        _ => panic!("persistent signal owner did not become ready"),
    };
    std::fs::write(owner_ready, b"ready").unwrap();
    std::hint::black_box(&runtime);
    loop {
        std::thread::park();
    }
}

#[cfg(target_os = "macos")]
#[test]
fn persistent_caller_managed_owner_child() {
    let Some(root) = std::env::var_os("LOXA_PERSISTENT_CALLER_MANAGED_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let server = root.join("server");
    let run_dir = root.join("run");
    let owner_ready = root.join("owner-ready");
    build_prelease_test_server(&root, &server);
    let runnable = persistent_runnable(&root, &server, 0);
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let marker = pack_server_identity(u32::MAX, i32::MAX);
    ACTIVE_SERVER.store(marker, Ordering::SeqCst);

    let runtime = match start_persistent_with_ownership(
        runnable,
        &ownership,
        || {
            assert_eq!(
                ACTIVE_SERVER.load(Ordering::SeqCst),
                marker,
                "caller-managed start changed the process-exit marker"
            );
            false
        },
        PersistentSignalPolicy::CallerManaged,
    )
    .unwrap()
    {
        PersistentStart::Ready(runtime) => runtime,
        _ => panic!("caller-managed persistent owner did not become ready"),
    };
    assert_eq!(
        ACTIVE_SERVER.load(Ordering::SeqCst),
        marker,
        "caller-managed readiness changed the process-exit marker"
    );
    std::fs::write(owner_ready, b"ready").unwrap();
    std::hint::black_box((&ownership, &runtime));
    loop {
        std::thread::park();
    }
}

#[cfg(target_os = "macos")]
#[test]
fn bundled_prelease_sigkill_owner_child() {
    let Some(root) = std::env::var_os("LOXA_BUNDLED_PRELEASE_OWNER_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let run_dir = root.join("run");
    let stage = run_dir.join(".bundled-runtime-exec-0123456789abcdef0123456789abcdef");
    let server = stage.join("Contents/MacOS/llama-server");
    let owner_ready = root.join("owner-ready");
    std::fs::create_dir_all(server.parent().unwrap()).unwrap();
    build_prelease_test_server(&root, &server);
    let prepared = crate::runtime_bundle::PreparedRuntime::for_test(stage).unwrap();
    let managed = ValidatedManagedRuntime::bundled(
        PathBuf::from("/Applications/Loxa.app/Contents/MacOS/llama-server"),
        prepared,
    );
    let launch = Launch {
        server: managed.source_server().to_path_buf(),
        managed_runtime: Some(managed),
        model: root.join("model.gguf"),
        id: "demo".into(),
        requested_port: std::env::var("LOXA_BUNDLED_PRELEASE_PORT")
            .unwrap()
            .parse()
            .unwrap(),
        ctx: 4096,
        profile: LaunchProfile::generic(),
        policy: LaunchPolicy::Foreground,
    };
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();

    match OwnedServer::start_with_ownership(&launch, Duration::from_secs(5), ownership, || None)
        .unwrap()
    {
        StartOutcome::Ready(mut server) => {
            std::fs::write(owner_ready, b"lease-published").unwrap();
            server.terminate().unwrap();
        }
        _ => panic!("pre-lease owner failed to become ready"),
    }
}

#[cfg(target_os = "macos")]
#[test]
fn next_acquire_reaps_an_exact_bundled_child_left_before_lease_publication() {
    use std::os::unix::process::ExitStatusExt as _;

    let _lock = process_test_lock();
    let root_guard = tempdir().unwrap();
    let root = root_guard.path().canonicalize().unwrap();
    let run_dir = root.join("run");
    let stage = run_dir.join(".bundled-runtime-exec-0123456789abcdef0123456789abcdef");
    let lookalike = run_dir.join(".bundled-runtime-exec-0123456789abcdef0123456789abcdeg");
    let announced = root.join("announced");
    let owner_ready = root.join("owner-ready");
    let child_pid_path = root.join("child-pid");
    std::fs::create_dir_all(&lookalike).unwrap();
    let port = resolve_requested_port(0).unwrap();
    let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runner::tests::bundled_prelease_sigkill_owner_child",
            "--nocapture",
        ])
        .env("LOXA_BUNDLED_PRELEASE_OWNER_ROOT", &root)
        .env("LOXA_BUNDLED_PRELEASE_PORT", port.to_string())
        .env("LOXA_TEST_POST_SPAWN_KILL_AFTER", &announced)
        .env("LOXA_TEST_POST_SPAWN_KILL_READY", &owner_ready)
        .env("LOXA_PRELEASE_ANNOUNCED", &announced)
        .env("LOXA_PRELEASE_CHILD_PID", &child_pid_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let handshake_deadline = Instant::now() + Duration::from_secs(5);
    while !owner_ready.is_file() {
        if let Some(status) = owner.try_wait().unwrap() {
            let output = owner.wait_with_output().unwrap();
            panic!("pre-lease owner exited before handshake: {status}; {output:?}");
        }
        if Instant::now() >= handshake_deadline {
            let _ = owner.kill();
            let output = owner.wait_with_output().unwrap();
            panic!("pre-lease owner handshake timed out: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(std::fs::read(&owner_ready).unwrap(), b"prelease");

    let output = owner.wait_with_output().unwrap();
    assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
    assert!(!run_dir.join("foreground.json").exists());
    assert!(
        stage.is_dir(),
        "the interrupted execution stage disappeared"
    );
    let child_pid = std::fs::read_to_string(&child_pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert!(process_group_exists(child_pid).unwrap());
    assert!(readiness(&readiness_client().unwrap(), port, "demo").unwrap());

    let _termination_fault = crate::runtime::fail_next_execution_stage_termination_for_test();
    let failed_acquire = match crate::runtime::RuntimeOwnership::acquire(&run_dir) {
        Ok(ownership) => {
            drop(ownership);
            crate::runtime::terminate_stale_process_group(child_pid).unwrap();
            if stage.exists() {
                std::fs::remove_dir_all(&stage).unwrap();
            }
            panic!("injected stage-termination failure did not block acquisition");
        }
        Err(error) => error,
    };
    assert_eq!(
        failed_acquire,
        "injected prepared runtime termination failure"
    );
    assert!(
        process_group_exists(child_pid).unwrap(),
        "termination failure did not preserve the exact child group"
    );
    assert!(
        stage.is_dir(),
        "termination failure removed the claimed stage"
    );
    assert!(!run_dir.join("foreground.json").exists());

    let ownership = match crate::runtime::RuntimeOwnership::acquire(&run_dir) {
        Ok(ownership) => ownership,
        Err(error) => {
            crate::runtime::terminate_stale_process_group(child_pid).unwrap();
            if stage.exists() {
                std::fs::remove_dir_all(&stage).unwrap();
            }
            panic!("runtime recovery acquire failed: {error}");
        }
    };
    let child_survived = process_group_exists(child_pid).unwrap();
    let listener_survived = std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
    let stage_survived = stage.exists();
    if child_survived {
        crate::runtime::terminate_stale_process_group(child_pid).unwrap();
    }
    if stage_survived {
        std::fs::remove_dir_all(&stage).unwrap();
    }

    assert!(
        !child_survived,
        "the exact unleased child group survived recovery"
    );
    assert!(
        !listener_survived,
        "the exact unleased listener survived recovery"
    );
    assert!(
        !stage_survived,
        "the exact unleased stage survived recovery"
    );
    assert!(lookalike.is_dir(), "a stage-name lookalike was removed");
    assert!(!run_dir.join("foreground.json").exists());

    let retry_server = root.join("retry-server");
    let (retry_port, responder) = serve_models("demo");
    write_executable_script(
            &retry_server,
            format!(
                "#!/bin/sh\nprintf '%s\\n' 'test server listening on http://127.0.0.1:{retry_port}' >&2\nwhile :; do sleep 1; done\n"
            )
            .as_bytes(),
        );
    let retry = Launch::generic(
        &retry_server,
        Path::new("/models/model.gguf"),
        "demo",
        retry_port,
        4096,
    );
    match OwnedServer::start_with_ownership(&retry, Duration::from_secs(5), ownership, || None)
        .unwrap()
    {
        StartOutcome::Ready(mut server) => server.terminate().unwrap(),
        _ => panic!("retry did not become ready"),
    }
    responder.join().unwrap();
    assert!(!run_dir.join("foreground.json").exists());
}

#[cfg(unix)]
#[test]
fn sigterm_of_persistent_owner_cleans_the_exact_group_lease_and_locks() {
    use std::os::unix::process::ExitStatusExt as _;

    let _lock = process_test_lock();
    let root = tempdir().unwrap();
    let owner_ready = root.path().join("owner-ready");
    let run_dir = root.path().join("run");
    let lease_path = run_dir.join("foreground.json");
    let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runner::tests::persistent_signal_owner_child",
            "--nocapture",
        ])
        .env("LOXA_PERSISTENT_SIGNAL_OWNER_ROOT", root.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let handshake_deadline = Instant::now() + Duration::from_secs(5);
    while !owner_ready.is_file() {
        if let Some(status) = owner.try_wait().unwrap() {
            let output = owner.wait_with_output().unwrap();
            panic!("persistent owner exited before ready: {status}; {output:?}");
        }
        if Instant::now() >= handshake_deadline {
            let _ = owner.kill();
            let output = owner.wait_with_output().unwrap();
            panic!("persistent owner readiness timed out: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let lease: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&lease_path).unwrap()).unwrap();
    let child_pid = lease["child_pid"].as_u64().unwrap() as u32;
    let child_group = lease["child_pgid"].as_i64().unwrap() as i32;
    assert_eq!(
        unsafe { libc::kill(owner.id() as libc::pid_t, libc::SIGTERM) },
        0
    );

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let output = loop {
        match owner.try_wait().unwrap() {
            Some(_) => break owner.wait_with_output().unwrap(),
            None if Instant::now() < exit_deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            None => {
                let _ = owner.kill();
                let output = owner.wait_with_output().unwrap();
                let _ = crate::runtime::terminate_stale_process_group(child_group);
                let _ = crate::runtime::RuntimeOwnership::acquire(&run_dir);
                panic!("persistent owner SIGTERM timed out: {output:?}");
            }
        }
    };
    let group_survived = process_group_exists(child_group).unwrap();
    let child_survived = unsafe { libc::kill(child_pid as libc::pid_t, 0) } == 0;
    let lease_survived = lease_path.exists();
    if group_survived {
        crate::runtime::terminate_stale_process_group(child_group).unwrap();
    }
    let recovered_ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();

    assert_eq!(
        output.status.code(),
        Some(128 + libc::SIGTERM),
        "{output:?}"
    );
    assert_eq!(output.status.signal(), None, "{output:?}");
    assert!(
        !child_survived,
        "persistent server child survived owner SIGTERM"
    );
    assert!(
        !group_survived,
        "persistent server group survived owner SIGTERM"
    );
    assert!(!lease_survived, "persistent lease survived owner SIGTERM");
    drop(recovered_ownership);
    drop(crate::catalog::ModelLock::acquire(&root.path().join("models/demo")).unwrap());
}

#[cfg(target_os = "macos")]
#[test]
fn caller_managed_owner_does_not_install_process_exit_policy() {
    use std::os::unix::process::ExitStatusExt as _;

    let _lock = process_test_lock();
    let root = tempdir().unwrap();
    let owner_ready = root.path().join("owner-ready");
    let run_dir = root.path().join("run");
    let lease_path = run_dir.join("foreground.json");
    let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runner::tests::persistent_caller_managed_owner_child",
            "--nocapture",
        ])
        .env("LOXA_PERSISTENT_CALLER_MANAGED_ROOT", root.path())
        .env("LOXA_PRELEASE_ANNOUNCED", root.path().join("announced"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let handshake_deadline = Instant::now() + Duration::from_secs(5);
    while !owner_ready.is_file() {
        if let Some(status) = owner.try_wait().unwrap() {
            let output = owner.wait_with_output().unwrap();
            panic!("caller-managed owner exited before ready: {status}; {output:?}");
        }
        if Instant::now() >= handshake_deadline {
            let _ = owner.kill();
            let output = owner.wait_with_output().unwrap();
            panic!("caller-managed owner readiness timed out: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let lease: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&lease_path).unwrap()).unwrap();
    let child_group = lease["child_pgid"].as_i64().unwrap() as i32;
    assert_eq!(
        unsafe { libc::kill(owner.id() as libc::pid_t, libc::SIGTERM) },
        0
    );

    let exit_deadline = Instant::now() + Duration::from_secs(5);
    let output = loop {
        match owner.try_wait().unwrap() {
            Some(_) => break owner.wait_with_output().unwrap(),
            None if Instant::now() < exit_deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            None => {
                let _ = owner.kill();
                let output = owner.wait_with_output().unwrap();
                let _ = crate::runtime::terminate_stale_process_group(child_group);
                let _ = crate::runtime::RuntimeOwnership::acquire(&run_dir);
                panic!("caller-managed owner SIGTERM timed out: {output:?}");
            }
        }
    };

    let recovered = crate::runtime::RuntimeOwnership::acquire(&run_dir);
    let recovery_error = recovered.as_ref().err().cloned();
    let recovery_deadline = Instant::now() + Duration::from_secs(1);
    let group_survived = loop {
        let group_is_live = crate::runtime::process_group_has_live_members(child_group).unwrap();
        if !group_is_live || Instant::now() >= recovery_deadline {
            break group_is_live;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    if group_survived {
        crate::runtime::terminate_stale_process_group(child_group).unwrap();
    }

    assert_eq!(output.status.signal(), Some(libc::SIGTERM), "{output:?}");
    assert!(
        recovery_error.is_none(),
        "caller-managed runtime recovery failed: {recovery_error:?}"
    );
    assert!(!group_survived, "caller-managed child survived recovery");
    assert!(!lease_path.exists());
    drop(recovered.ok());
    drop(crate::catalog::ModelLock::acquire(&root.path().join("models/demo")).unwrap());
}

#[test]
fn argv_and_readiness_are_exact_and_generic() {
    let launch = Launch::generic(
        Path::new("/servers/llama-server"),
        std::path::Path::new("/models/model.gguf"),
        "demo",
        1234,
        8192,
    );
    let args = build_args(&launch, 1234);
    assert_eq!(
        args,
        [
            "--model",
            "/models/model.gguf",
            "--alias",
            "demo",
            "--host",
            "127.0.0.1",
            "--cors-origins",
            "localhost",
            "--no-ui",
            "--port",
            "1234",
            "--ctx-size",
            "8192",
            "--n-gpu-layers",
            "99",
            "--jinja",
            "--reasoning",
            "off"
        ]
        .map(OsStr::new)
    );
    assert!(models_body_has_alias(r#"{"data":[{"id":"demo"}]}"#, "demo"));
    assert!(!models_body_has_alias(
        r#"{"data":[{"id":"demo-extra"}]}"#,
        "demo"
    ));
    assert_eq!(STARTUP_TIMEOUT, Duration::from_secs(120));
}

#[test]
fn attachment_argv_rebuilds_each_persistent_profile_from_the_exact_fingerprint() {
    let models = Path::new("/models");
    let generic: crate::runtime_fingerprint::RuntimeFingerprint =
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "model_id": "demo",
            "effective_context": 4096,
            "effective_profile": "generic",
            "sleep_policy": 60,
            "primary": {
                "local_filename": "model.gguf",
                "sha256": "a".repeat(64),
                "size": 7,
            },
            "draft": null,
        }))
        .unwrap();
    let mtp: crate::runtime_fingerprint::RuntimeFingerprint =
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "model_id": "demo",
            "effective_context": 8192,
            "effective_profile": "gemma4_mtp",
            "sleep_policy": 60,
            "primary": {
                "local_filename": "model.gguf",
                "sha256": "a".repeat(64),
                "size": 7,
            },
            "draft": {
                "local_filename": "draft.gguf",
                "sha256": "b".repeat(64),
                "size": 5,
            },
        }))
        .unwrap();
    let primary_only = mtp.primary_only().unwrap();

    let generic_expected = [
        "--model",
        "/models/demo/model.gguf",
        "--alias",
        "demo",
        "--host",
        "127.0.0.1",
        "--cors-origins",
        "localhost",
        "--no-ui",
        "--port",
        "43123",
        "--ctx-size",
        "4096",
        "--n-gpu-layers",
        "99",
        "--jinja",
        "--reasoning",
        "off",
        "--sleep-idle-seconds",
        "60",
    ]
    .map(OsString::from)
    .to_vec();
    let mtp_expected = [
        "--model",
        "/models/demo/model.gguf",
        "--alias",
        "demo",
        "--host",
        "127.0.0.1",
        "--cors-origins",
        "localhost",
        "--no-ui",
        "--port",
        "43124",
        "--ctx-size",
        "8192",
        "--n-gpu-layers",
        "all",
        "--fit",
        "off",
        "--jinja",
        "--reasoning",
        "off",
        "--spec-draft-model",
        "/models/demo/draft.gguf",
        "--spec-type",
        "draft-mtp",
        "--spec-draft-n-max",
        "4",
        "--n-gpu-layers-draft",
        "all",
        "--sleep-idle-seconds",
        "60",
    ]
    .map(OsString::from)
    .to_vec();
    let primary_expected = [
        "--model",
        "/models/demo/model.gguf",
        "--alias",
        "demo",
        "--host",
        "127.0.0.1",
        "--cors-origins",
        "localhost",
        "--no-ui",
        "--port",
        "43125",
        "--ctx-size",
        "8192",
        "--n-gpu-layers",
        "all",
        "--fit",
        "off",
        "--jinja",
        "--reasoning",
        "off",
        "--sleep-idle-seconds",
        "60",
    ]
    .map(OsString::from)
    .to_vec();

    assert_eq!(
        build_persistent_args_for_fingerprint(models, &generic, 43123).unwrap(),
        generic_expected
    );
    assert_eq!(
        build_persistent_args_for_fingerprint(models, &mtp, 43124).unwrap(),
        mtp_expected
    );
    assert_eq!(
        build_persistent_args_for_fingerprint(models, &primary_only, 43125).unwrap(),
        primary_expected
    );
}

#[test]
fn persistent_argv_adds_one_sleep_policy_without_changing_foreground_or_fallback() {
    let foreground = Launch::generic(
        Path::new("/servers/llama-server"),
        Path::new("/models/model.gguf"),
        "demo",
        1234,
        8192,
    );
    let persistent = Launch {
        policy: LaunchPolicy::PersistentApp,
        ..foreground.clone()
    };
    let foreground_mtp = Launch {
        profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
        ..foreground.clone()
    };
    let foreground_fallback = foreground_mtp.primary_only().unwrap();

    let persistent_args = build_args(&persistent, 1234);
    let sleep_positions = persistent_args
        .iter()
        .enumerate()
        .filter_map(|(index, argument)| (argument == "--sleep-idle-seconds").then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(sleep_positions.len(), 1);
    assert_eq!(persistent_args[sleep_positions[0] + 1], "60");

    for launch in [&foreground, &foreground_mtp, &foreground_fallback] {
        assert!(
            !build_args(launch, 1234)
                .iter()
                .any(|argument| argument == "--sleep-idle-seconds"),
            "foreground launch policy gained a persistent sleep argument"
        );
    }
}

#[cfg(unix)]
#[test]
fn caller_managed_start_retains_idle_ownership() {
    if std::env::var_os("LOXA_CALLER_MANAGED_IDLE_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runner::tests::caller_managed_start_retains_idle_ownership",
                "--nocapture",
            ])
            .env("LOXA_CALLER_MANAGED_IDLE_CHILD", "1")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }

    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    let port = resolve_requested_port(0).unwrap();
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(dir.path(), &server, port);
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();

    let started = start_persistent_with_ownership(
        runnable,
        &ownership,
        || false,
        PersistentSignalPolicy::CallerManaged,
    )
    .unwrap();
    let mut runtime = match started {
        PersistentStart::Ready(runtime) => runtime,
        PersistentStart::Stopped(exit) => panic!("caller-managed runtime stopped: {exit:?}"),
        PersistentStart::Interrupted(interruption) => {
            panic!("caller-managed runtime was interrupted: {interruption:?}")
        }
        PersistentStart::CleanupFailed(_) => {
            panic!("caller-managed runtime cleanup failed unexpectedly")
        }
    };

    assert!(crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err());
    runtime.terminate().unwrap();
    drop(runtime);
    let next_child = ownership.reserve_child().unwrap();
    drop(next_child);
    assert!(
        crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err(),
        "idle retained ownership released the common lock"
    );
    drop(ownership);
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn foreground_does_not_repoll_the_signal_callback_after_readiness() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    let ready_for_server = ready.clone();
    let (port, http) = serve_models_with_ready_action("demo", move || {
        std::fs::write(ready_for_server, b"ready").unwrap();
    });
    write_executable_script(
            &server,
            format!(
                "#!/bin/sh\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n"
            )
            .as_bytes(),
        );
    let launch = Launch::generic(&server, Path::new("/models/model.gguf"), "demo", port, 4096);
    let polls_after_ready = AtomicUsize::new(0);

    let started = start_foreground_with_signal(&launch, &run_dir, || {
        ready.exists().then(|| {
            polls_after_ready.fetch_add(1, Ordering::SeqCst);
            libc::SIGINT
        })
    })
    .unwrap();
    http.join().unwrap();

    let mut foreground = match started {
        ForegroundStart::Ready(server) => server,
        ForegroundStart::Stopped(exit) => {
            panic!("post-readiness signal poll changed parent behavior: {exit:?}")
        }
    };
    assert_eq!(polls_after_ready.load(Ordering::SeqCst), 0);
    foreground.terminate().unwrap();
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_before_spawn_and_after_ownership_is_closed() {
    let _lock = process_test_lock();
    for cancel_on_call in [1, 2] {
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let spawned = dir.path().join("spawned");
        let run_dir = dir.path().join("run");
        write_executable_script(
            &server,
            format!("#!/bin/sh\nprintf spawned > '{}'\n", spawned.display()).as_bytes(),
        );
        let runnable = persistent_runnable(dir.path(), &server, 0);
        let calls = AtomicUsize::new(0);

        let started = start_persistent(runnable, &run_dir, || {
            calls.fetch_add(1, Ordering::SeqCst) + 1 == cancel_on_call
        })
        .unwrap();

        assert!(matches!(
            started,
            PersistentStart::Interrupted(StartupInterruption::Cancelled)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), cancel_on_call);
        assert!(!spawned.exists());
        assert!(!run_dir.join("foreground.json").exists());
        drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
        drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    }
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_after_lease_cleans_group_listener_and_ownership() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let run_dir = dir.path().join("run");
    let port = resolve_requested_port(0).unwrap();
    write_executable_script(&server, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
    let runnable = persistent_runnable(dir.path(), &server, port);
    let group = Mutex::new(None);

    let started = start_persistent(runnable, &run_dir, || {
        if let Some((_, active_group)) =
            unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
        {
            *group.lock().unwrap() = Some(active_group);
        }
        run_dir.join("foreground.json").is_file()
    })
    .unwrap();

    assert!(matches!(
        started,
        PersistentStart::Interrupted(StartupInterruption::Cancelled)
    ));
    let group = group.into_inner().unwrap().expect("owned process group");
    assert!(!process_group_exists(group).unwrap());
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_cleanup_failure_before_ready_returns_retryable_owned_server() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let run_dir = dir.path().join("run");
    let lease_path = run_dir.join("foreground.json");
    let port = resolve_requested_port(0).unwrap();
    write_executable_script(&server, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
    let runnable = persistent_runnable(dir.path(), &server, port);
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let group = Mutex::new(None);
    let original_lease = Mutex::new(None);

    let started = start_persistent_with_ownership(
        runnable,
        &ownership,
        || {
            if !lease_path.is_file() {
                return false;
            }
            let mut original = original_lease.lock().unwrap();
            if original.is_none() {
                let bytes = std::fs::read(&lease_path).unwrap();
                let lease: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                *group.lock().unwrap() = Some(lease["child_pgid"].as_i64().unwrap() as i32);
                let mut changed = lease;
                changed["port"] =
                    serde_json::json!(changed["port"].as_u64().unwrap().checked_add(1).unwrap());
                std::fs::write(&lease_path, serde_json::to_vec(&changed).unwrap()).unwrap();
                *original = Some(bytes);
            }
            true
        },
        PersistentSignalPolicy::CallerManaged,
    );

    let mut owned = match started {
        Ok(PersistentStart::CleanupFailed(owned)) => owned,
        Ok(PersistentStart::Ready(_)) => panic!("cancelled startup returned ready"),
        Ok(PersistentStart::Stopped(exit)) => panic!("cancelled startup stopped: {exit:?}"),
        Ok(PersistentStart::Interrupted(interruption)) => {
            panic!("cleanup failure was reported as interrupted: {interruption:?}")
        }
        Err(error) => panic!("pre-ready cleanup failure dropped ownership: {error:?}"),
    };

    let group = group.into_inner().unwrap().expect("owned process group");
    assert!(!process_group_exists(group).unwrap());
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(lease_path.exists());
    assert!(crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err());
    assert!(ownership.reserve_child().is_err());
    assert!(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).is_err());
    assert!(
        owned.poll().is_err(),
        "poll accepted a server whose child cleanup is incomplete"
    );
    let original = original_lease
        .into_inner()
        .unwrap()
        .expect("captured exact lease");
    std::fs::write(&lease_path, original).unwrap();

    owned.terminate().unwrap();

    assert!(!lease_path.exists());
    drop(owned);
    let next_child = ownership.reserve_child().unwrap();
    drop(next_child);
    assert!(crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
    drop(ownership);
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_after_announcement_closes_the_child_listener() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    let port = resolve_requested_port(0).unwrap();
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(dir.path(), &server, port);
    let group = Mutex::new(None);

    let started = start_persistent(runnable, &run_dir, || {
        if let Some((_, active_group)) =
            unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
        {
            *group.lock().unwrap() = Some(active_group);
        }
        announced.is_file()
    })
    .unwrap();

    assert!(matches!(
        started,
        PersistentStart::Interrupted(StartupInterruption::Cancelled)
    ));
    let group = group.into_inner().unwrap().expect("owned process group");
    assert!(!process_group_exists(group).unwrap());
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(!run_dir.join("foreground.json").exists());
    assert!(
        !ready.exists(),
        "readiness must not race past announcement cancellation"
    );
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_after_readiness_cleans_every_owned_resource() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    let port = resolve_requested_port(0).unwrap();
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(dir.path(), &server, port);
    let group = Mutex::new(None);

    let started = start_persistent(runnable, &run_dir, || {
        if let Some((_, active_group)) =
            unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
        {
            *group.lock().unwrap() = Some(active_group);
        }
        ready.is_file()
    })
    .unwrap();

    assert!(matches!(
        started,
        PersistentStart::Interrupted(StartupInterruption::Cancelled)
    ));
    assert!(announced.is_file());
    assert!(ready.is_file());
    let group = group.into_inner().unwrap().expect("owned process group");
    assert!(!process_group_exists(group).unwrap());
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn cancellation_cleanup_failure_after_ready_returns_retryable_owned_server() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    let lease_path = run_dir.join("foreground.json");
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(dir.path(), &server, 0);
    let ready_polls = AtomicUsize::new(0);
    let original_lease = Mutex::new(None);

    let started = start_persistent(runnable, &run_dir, || {
        if !ready.is_file() {
            return false;
        }
        let poll = ready_polls.fetch_add(1, Ordering::SeqCst) + 1;
        if poll != 2 {
            return false;
        }
        let original = std::fs::read(&lease_path).unwrap();
        let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
        changed["port"] =
            serde_json::json!(changed["port"].as_u64().unwrap().checked_add(1).unwrap());
        std::fs::write(&lease_path, serde_json::to_vec(&changed).unwrap()).unwrap();
        *original_lease.lock().unwrap() = Some(original);
        true
    });

    let mut owned = match started {
        Ok(PersistentStart::CleanupFailed(owned)) => owned,
        Ok(PersistentStart::Ready(_)) => panic!("cancelled startup returned ready"),
        Ok(PersistentStart::Stopped(exit)) => panic!("cancelled startup stopped: {exit:?}"),
        Ok(PersistentStart::Interrupted(interruption)) => {
            panic!("cleanup failure was reported as interrupted: {interruption:?}")
        }
        Err(error) => panic!("cleanup failure dropped ownership: {error:?}"),
    };

    assert_eq!(ready_polls.load(Ordering::SeqCst), 2);
    assert!(announced.is_file());
    assert!(ready.is_file());
    assert!(lease_path.exists());
    assert!(crate::runtime::RuntimeOwnership::acquire(&run_dir).is_err());
    assert!(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).is_err());
    let original = original_lease
        .into_inner()
        .unwrap()
        .expect("captured exact lease");
    std::fs::write(&lease_path, original).unwrap();

    owned.terminate().unwrap();

    assert!(!lease_path.exists());
    drop(owned);
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_publication_carries_the_exact_whole_fingerprint() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    write_persistent_test_server(&server, &announced, &ready);
    let runnable = persistent_runnable(dir.path(), &server, 0);
    let expected_fingerprint = serde_json::to_value(runnable.fingerprint()).unwrap();

    let started = start_persistent(runnable, &run_dir, || false).unwrap();
    let mut server = match started {
        PersistentStart::Ready(server) => server,
        PersistentStart::Stopped(exit) => panic!("persistent server stopped: {exit:?}"),
        PersistentStart::Interrupted(interruption) => {
            panic!("persistent server was interrupted: {interruption:?}")
        }
        PersistentStart::CleanupFailed(_) => {
            panic!("persistent server cleanup failed unexpectedly")
        }
    };
    let lease: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap()).unwrap();

    assert_eq!(lease["version"], 3);
    assert_eq!(lease["owner_mode"], "persistent_app");
    assert_eq!(lease["fingerprint"], expected_fingerprint);
    assert_eq!(lease["managed_source"], serde_json::Value::Null);
    assert_eq!(lease["fingerprint"]["sleep_policy"], 60);
    assert_eq!(lease["model_id"], lease["fingerprint"]["model_id"]);

    server.terminate().unwrap();
    assert!(!run_dir.join("foreground.json").exists());
}

#[cfg(unix)]
#[test]
fn persistent_cancellation_at_mtp_retry_never_spawns_the_primary() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let argv = dir.path().join("argv");
    let primary = dir.path().join("primary");
    let run_dir = dir.path().join("run");
    write_executable_script(
            &server,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) exit 42 ;;\nesac\nprintf primary > '{}'\nwhile :; do sleep 1; done\n",
                argv.display(),
                primary.display(),
            )
            .as_bytes(),
        );
    let runnable = persistent_mtp_runnable(dir.path(), &server, 0);
    let saw_child = AtomicBool::new(false);
    let group = Mutex::new(None);

    let started = start_persistent(runnable, &run_dir, || {
        if let Some((_, active_group)) =
            unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
        {
            saw_child.store(true, Ordering::SeqCst);
            *group.lock().unwrap() = Some(active_group);
            false
        } else {
            saw_child.load(Ordering::SeqCst) && argv.is_file()
        }
    })
    .unwrap();

    assert!(matches!(
        started,
        PersistentStart::Interrupted(StartupInterruption::Cancelled)
    ));
    let argv = std::fs::read_to_string(&argv).unwrap();
    assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
    assert_eq!(
        argv.lines()
            .filter(|line| *line == "--spec-draft-model")
            .count(),
        1
    );
    assert!(!primary.exists());
    let group = group.into_inner().unwrap().expect("owned process group");
    assert!(!process_group_exists(group).unwrap());
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn persistent_mtp_fallback_transforms_fingerprint_and_keeps_sleep_policy() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let argv = dir.path().join("argv");
    let announced = dir.path().join("announced");
    let ready = dir.path().join("ready");
    let run_dir = dir.path().join("run");
    write_mtp_persistent_test_server(&server, &argv, &announced, &ready);
    let runnable = persistent_mtp_runnable(dir.path(), &server, 0);

    let started = start_persistent(runnable, &run_dir, || false).unwrap();
    let mut server = match started {
        PersistentStart::Ready(server) => server,
        PersistentStart::Stopped(exit) => panic!("persistent MTP fallback stopped: {exit:?}"),
        PersistentStart::Interrupted(interruption) => {
            panic!("persistent MTP fallback was interrupted: {interruption:?}")
        }
        PersistentStart::CleanupFailed(_) => {
            panic!("persistent MTP fallback cleanup failed unexpectedly")
        }
    };
    let port = server.port();

    assert_eq!(
        server.fingerprint().effective_profile(),
        EffectiveProfile::PrimaryOnly
    );
    assert!(server.fingerprint().draft().is_none());
    assert_eq!(server.fingerprint().sleep_policy(), Some(60));
    let lease: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap()).unwrap();
    assert_eq!(lease["owner_mode"], "persistent_app");
    assert_eq!(
        lease["fingerprint"],
        serde_json::to_value(server.fingerprint()).unwrap()
    );
    assert_eq!(lease["fingerprint"]["effective_profile"], "primary_only");
    assert_eq!(lease["fingerprint"]["draft"], serde_json::Value::Null);
    assert_eq!(lease["fingerprint"]["sleep_policy"], 60);
    assert!(server.poll().unwrap().is_none());
    let argv = std::fs::read_to_string(&argv).unwrap();
    let argv = argv.lines().collect::<Vec<_>>();
    assert_eq!(
        argv.iter()
            .filter(|argument| **argument == "--sleep-idle-seconds")
            .count(),
        2,
        "both persistent MTP attempts need exactly one sleep policy"
    );
    for index in argv
        .iter()
        .enumerate()
        .filter_map(|(index, argument)| (*argument == "--sleep-idle-seconds").then_some(index))
    {
        assert_eq!(argv[index + 1], "60");
    }
    assert_eq!(
        argv.iter()
            .filter(|argument| **argument == "--spec-draft-model")
            .count(),
        1
    );
    server.terminate().unwrap();
    drop(server);

    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[cfg(unix)]
#[test]
fn bundled_persistent_mtp_failure_never_promotes_or_retries_primary_only() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let argv = dir.path().join("argv");
    let primary = dir.path().join("primary");
    let run_dir = dir.path().join("run");
    write_executable_script(
            &server,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) exit 42 ;;\nesac\nprintf primary > '{}'\nexit 99\n",
                argv.display(),
                primary.display(),
            )
            .as_bytes(),
        );
    let runnable =
        persistent_mtp_runnable(dir.path(), &server, 0).without_primary_fallback_for_test();

    let started = start_persistent(runnable, &run_dir, || false).unwrap();

    let exit = match started {
        PersistentStart::Stopped(exit) => exit,
        PersistentStart::Ready(_) => panic!("failed MTP was promoted"),
        PersistentStart::Interrupted(interruption) => {
            panic!("failed MTP was interrupted: {interruption:?}")
        }
        PersistentStart::CleanupFailed(_) => panic!("failed MTP cleanup was retained"),
    };
    assert_eq!(exit.code, 42);
    let argv = std::fs::read_to_string(&argv).unwrap();
    assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
    assert_eq!(
        argv.lines()
            .filter(|line| *line == "--spec-draft-model")
            .count(),
        1
    );
    assert!(!primary.exists());
    assert!(!run_dir.join("foreground.json").exists());
    drop(crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap());
    drop(crate::catalog::ModelLock::acquire(&dir.path().join("models/demo")).unwrap());
}

#[test]
fn ready_line_displays_the_port_before_the_model_id() {
    let line = ready_line(58922, "gemma-4-12b-it-qat-ud-q4-k-xl");

    assert!(line.contains("http://127.0.0.1:58922"), "{line}");
    assert!(
        line.contains("(model gemma-4-12b-it-qat-ud-q4-k-xl)"),
        "{line}"
    );
    assert!(!line.contains("127.0.0.1:gemma-"), "{line}");
    assert!(!line.contains("(model 58922)"), "{line}");
}

#[test]
fn termination_signal_at_fallback_boundary_stops_without_retrying() {
    let stopped = stopped_for_signal(&|| Some(libc::SIGINT));

    assert!(matches!(
        stopped,
        Some(ForegroundStart::Stopped(ServerExit { code: 130, .. }))
    ));
}

#[cfg(unix)]
#[test]
fn argv_is_exact_for_the_known_mtp_profile() {
    let launch = Launch {
        server: PathBuf::from("/servers/llama-server"),
        managed_runtime: None,
        model: PathBuf::from("/models/model.gguf"),
        id: "gemma4".into(),
        requested_port: 1234,
        ctx: 8192,
        profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
        policy: LaunchPolicy::Foreground,
    };

    assert_eq!(
        build_args(&launch, 1234),
        [
            "--model",
            "/models/model.gguf",
            "--alias",
            "gemma4",
            "--host",
            "127.0.0.1",
            "--cors-origins",
            "localhost",
            "--no-ui",
            "--port",
            "1234",
            "--ctx-size",
            "8192",
            "--n-gpu-layers",
            "all",
            "--fit",
            "off",
            "--jinja",
            "--reasoning",
            "off",
            "--spec-draft-model",
            "/models/draft.gguf",
            "--spec-type",
            "draft-mtp",
            "--spec-draft-n-max",
            "4",
            "--n-gpu-layers-draft",
            "all",
        ]
        .map(OsStr::new)
    );
}

#[test]
fn announcement_validation_accepts_only_an_exact_loopback_http_endpoint() {
    assert_eq!(
        validate_announcement_line(
            "0.00.000.000 I srv  llama_server: listening on http://127.0.0.1:43123",
        )
        .unwrap(),
        43123
    );
    assert!(validate_announcement_line("listening on http://0.0.0.0:43123").is_err());
}

#[test]
fn models_readiness_body_is_bounded_and_requires_the_exact_alias() {
    assert!(
        models_reader_has_alias(std::io::Cursor::new(br#"{"data":[{"id":"demo"}]}"#), "demo",)
            .unwrap()
    );
    assert!(!models_reader_has_alias(
        std::io::Cursor::new(br#"{"data":[{"id":"demo-extra"}]}"#),
        "demo",
    )
    .unwrap());

    let oversized = vec![b' '; MAX_MODELS_BODY + 1];
    let error = models_reader_has_alias(std::io::Cursor::new(oversized), "demo").unwrap_err();
    assert!(error.contains("too large"), "{error}");
}

#[test]
fn automatic_port_is_concrete_and_explicit_port_is_preserved() {
    let automatic = resolve_requested_port(0).unwrap();

    assert_ne!(automatic, 0);
    assert_eq!(resolve_requested_port(43123).unwrap(), 43123);
}

#[cfg(unix)]
#[test]
fn owned_server_passes_runtime_args_and_owns_both_output_drains() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let argv = dir.path().join("argv");
    let (port, http) = serve_models("demo");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '0.00 I srv: listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n",
                argv.display()
            )
            .as_bytes(),
        );

    let outcome = OwnedServer::start(
        &server_path,
        Path::new("/models/model.gguf"),
        "demo",
        port,
        8192,
        Duration::from_secs(2),
        || None,
    )
    .unwrap();
    let mut server = match outcome {
        StartOutcome::Ready(server) => server,
        _ => panic!("server did not become ready"),
    };
    http.join().unwrap();

    assert_eq!(
            std::fs::read_to_string(&argv).unwrap(),
            format!(
                "--model\n/models/model.gguf\n--alias\ndemo\n--host\n127.0.0.1\n--cors-origins\nlocalhost\n--no-ui\n--port\n{port}\n--ctx-size\n8192\n--n-gpu-layers\n99\n--jinja\n--reasoning\noff\n"
            )
        );
    assert_eq!(server.port(), port);
    assert!(server.output_readers_owned());
    server.terminate().unwrap();
    assert!(!server.output_readers_owned());
    server.terminate().unwrap();
}

#[cfg(unix)]
#[test]
fn managed_server_publishes_and_clears_its_runtime_lease() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let run_dir = dir.path().join("run");
    let (port, http) = serve_models("demo");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n"
            )
            .as_bytes(),
        );
    let ownership = crate::runtime::RuntimeOwnership::acquire(&run_dir).unwrap();
    let launch = Launch::generic(
        &server_path,
        Path::new("/models/model.gguf"),
        "demo",
        port,
        8192,
    );

    let outcome =
        OwnedServer::start_with_ownership(&launch, Duration::from_secs(2), ownership, || None)
            .unwrap();
    let mut server = match outcome {
        StartOutcome::Ready(server) => server,
        _ => panic!("server did not become ready"),
    };
    http.join().unwrap();

    let lease: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run_dir.join("foreground.json")).unwrap()).unwrap();
    assert_eq!(lease["version"], 3);
    assert_eq!(lease["owner_mode"], "foreground");
    assert_eq!(lease["fingerprint"], serde_json::Value::Null);
    assert_eq!(lease["managed_source"], serde_json::Value::Null);
    server.terminate().unwrap();
    assert!(!run_dir.join("foreground.json").exists());
}

#[test]
fn mtp_draft_child_diagnostics_never_enter_debug_structured_output() {
    let sensitive_markers = [
        "LOXA_TEST_PROMPT_MARKER",
        "LOXA_TEST_RESPONSE_MARKER",
        "LOXA_TEST_TOKEN_MARKER",
        "LOXA_TEST_AUTHORIZATION_MARKER",
        "LOXA_TEST_BODY_MARKER",
    ];
    let diagnostic = format!(
        "prompt={} response={} token={} authorization={} body={}",
        sensitive_markers[0],
        sensitive_markers[1],
        sensitive_markers[2],
        sensitive_markers[3],
        sensitive_markers[4],
    );
    let launch = Launch {
        server: PathBuf::from("/servers/llama-server"),
        managed_runtime: None,
        model: PathBuf::from("/models/model.gguf"),
        id: "demo".into(),
        requested_port: 1234,
        ctx: 8192,
        profile: LaunchProfile::gemma4_mtp(Some(PathBuf::from("/models/draft.gguf"))),
        policy: LaunchPolicy::Foreground,
    };
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || SharedLogWriter(writer.clone()))
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        report_mtp_draft_start_failure(&launch, "exited", Some(&diagnostic));
    });

    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let events = output
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();

    for marker in sensitive_markers {
        assert!(
            !output.contains(marker),
            "debug structured output leaked {marker}"
        );
    }

    let failure = events
        .iter()
        .find(|event| event["event"] == "gemma_mtp_draft_start_failed")
        .expect("MTP failure classification event");
    assert_eq!(failure["outcome"], "exited");

    let diagnostic = events
        .iter()
        .find(|event| event["event"] == "gemma_mtp_draft_start_diagnostic")
        .expect("MTP diagnostic-presence event");
    assert_eq!(diagnostic["diagnostic_present"], true);
    assert!(diagnostic.get("diagnostic").is_none(), "{diagnostic}");
}

#[cfg(unix)]
#[test]
fn mtp_start_failure_retries_once_with_the_primary_and_clears_its_lease() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let argv = dir.path().join("argv");
    let run_dir = dir.path().join("run");
    let (port, http) = serve_models("demo");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'draft startup failed\\n' >&2; exit 42 ;;\nesac\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile :; do sleep 1; done\n",
                argv.display()
            )
            .as_bytes(),
        );
    let launch = test_mtp_launch(&server_path, port);

    let started = start_foreground_with_signal(&launch, &run_dir, || None).unwrap();
    let mut server = match started {
        ForegroundStart::Ready(server) => server,
        ForegroundStart::Stopped(exit) => panic!("MTP did not retry: {exit:?}"),
    };
    http.join().unwrap();

    let argv = std::fs::read_to_string(&argv).unwrap();
    assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 2);
    assert_eq!(
        argv.lines()
            .filter(|line| *line == "--spec-draft-model")
            .count(),
        1
    );
    assert!(run_dir.join("foreground.json").is_file());
    server.terminate().unwrap();
    assert!(!run_dir.join("foreground.json").exists());
}

#[cfg(unix)]
#[test]
fn mtp_start_error_does_not_spawn_a_primary_retry() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let argv = dir.path().join("argv");
    let primary = dir.path().join("primary");
    let run_dir = dir.path().join("run");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \"$*\" in\n  *--spec-draft-model*) printf 'listening on http://0.0.0.0:43123\\n' >&2; while :; do sleep 1; done ;;\n  *) printf primary > '{}'; exit 99 ;;\nesac\n",
                argv.display(),
                primary.display(),
            )
            .as_bytes(),
        );
    let launch = test_mtp_launch(&server_path, 43123);

    let error = match start_foreground_with_signal(&launch, &run_dir, || None) {
        Err(error) => error,
        Ok(_) => panic!("MTP startup error unexpectedly retried the primary model"),
    };

    assert!(error.contains("exact loopback HTTP endpoint"), "{error}");
    let argv = std::fs::read_to_string(&argv).unwrap();
    assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
    assert!(!primary.exists());
    assert!(!run_dir.join("foreground.json").exists());
}

#[cfg(unix)]
#[test]
fn process_termination_signal_during_mtp_startup_never_retries() {
    let _lock = process_test_lock();
    let _termination_signal = reset_process_termination_signal_for_test();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let argv = dir.path().join("argv");
    let marker = dir.path().join("started");
    let run_dir = dir.path().join("run");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf started > '{}'\nwhile :; do sleep 1; done\n",
                argv.display(),
                marker.display(),
            )
            .as_bytes(),
        );
    let launch = test_mtp_launch(&server_path, 43123);

    assert_eq!(process_termination_signal(), None);
    let started = start_foreground_with(&launch, &run_dir, || {
        if marker.exists() {
            PROCESS_TERMINATION_SIGNAL.store(libc::SIGINT, Ordering::SeqCst);
        }
        process_termination_signal()
    })
    .unwrap();

    assert!(matches!(
        started,
        ForegroundStart::Stopped(ServerExit { code: 130, .. })
    ));
    let argv = std::fs::read_to_string(&argv).unwrap();
    assert_eq!(argv.lines().filter(|line| *line == "--model").count(), 1);
    assert_eq!(
        argv.lines()
            .filter(|line| *line == "--spec-draft-model")
            .count(),
        1
    );
    assert!(!run_dir.join("foreground.json").exists());
}

#[cfg(unix)]
#[test]
fn startup_timeout_and_explicit_port_mismatch_clean_the_owned_group() {
    let _lock = process_test_lock();
    {
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        write_executable_script(&server_path, b"#!/bin/sh\nwhile :; do sleep 1; done\n");
        let (group_sender, group_receiver) = mpsc::sync_channel(1);
        let (release_sender, release) = mpsc::sync_channel(1);

        std::thread::scope(|scope| {
            let start = scope.spawn(move || {
                OwnedServer::start(
                    &server_path,
                    Path::new("/models/model.gguf"),
                    "demo",
                    0,
                    1,
                    Duration::ZERO,
                    || {
                        let (_, group) =
                            unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
                                .expect("server did not publish its owned process group");
                        group_sender.send(group).unwrap();
                        release.recv().unwrap();
                        None
                    },
                )
            });
            let group = group_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("server did not reach startup polling");
            release_sender.send(()).unwrap();
            let error = match start.join().unwrap() {
                Err(error) => error,
                Ok(_) => panic!("server unexpectedly started"),
            };
            assert!(error.contains("did not announce"), "{error}");
            assert!(!process_group_exists(group).unwrap());
        });
    }

    {
        let dir = tempdir().unwrap();
        let server_path = dir.path().join("server");
        let release_path = dir.path().join("release");
        write_executable_script(
                &server_path,
                format!(
                    "#!/bin/sh\nwhile [ ! -f '{}' ]; do :; done\nprintf 'listening on http://127.0.0.1:43123\\n' >&2\nwhile :; do sleep 1; done\n",
                    release_path.display(),
                )
                .as_bytes(),
            );
        let (group_sender, group_receiver) = mpsc::sync_channel(1);
        let (release_sender, release) = mpsc::sync_channel(1);

        std::thread::scope(|scope| {
            let start = scope.spawn(move || {
                let first_poll = AtomicBool::new(true);
                OwnedServer::start(
                    &server_path,
                    Path::new("/models/model.gguf"),
                    "demo",
                    43124,
                    1,
                    Duration::from_secs(5),
                    || {
                        if first_poll.swap(false, Ordering::SeqCst) {
                            let (_, group) =
                                unpack_server_identity(ACTIVE_SERVER.load(Ordering::SeqCst))
                                    .expect("server did not publish its owned process group");
                            group_sender.send(group).unwrap();
                            release.recv().unwrap();
                        }
                        None
                    },
                )
            });
            let group = group_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("server did not reach startup polling");
            std::fs::write(&release_path, b"release").unwrap();
            release_sender.send(()).unwrap();
            let error = match start.join().unwrap() {
                Err(error) => error,
                Ok(_) => panic!("server unexpectedly started"),
            };
            assert!(error.contains("expected 43124"), "{error}");
            assert!(!process_group_exists(group).unwrap());
        });
    }
}

#[cfg(unix)]
#[test]
fn stdout_listening_line_is_drain_only() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    write_executable_script(
        &server_path,
        b"#!/bin/sh\nprintf 'listening on http://127.0.0.1:43123\\n'\nwhile :; do sleep 1; done\n",
    );

    let error = match OwnedServer::start(
        &server_path,
        Path::new("/models/model.gguf"),
        "demo",
        0,
        1,
        Duration::from_millis(500),
        || None,
    ) {
        Err(error) => error,
        Ok(_) => panic!("stdout announcement unexpectedly started the server"),
    };

    assert!(error.contains("did not announce"), "{error}");
}

#[cfg(unix)]
#[test]
fn post_ready_poll_returns_diagnostic_after_descendant_cleanup() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server_path = dir.path().join("server");
    let release = dir.path().join("release");
    let (port, http) = serve_models("demo");
    write_executable_script(
            &server_path,
            format!(
                "#!/bin/sh\n(trap '' TERM; while :; do sleep 1; done) &\nprintf 'listening on http://127.0.0.1:{port}\\n' >&2\nwhile [ ! -f '{}' ]; do sleep 1; done\nprintf 'fatal: post-ready model crash\\n' >&2\nexit 7\n",
                release.display()
            )
            .as_bytes(),
        );
    let outcome = OwnedServer::start(
        &server_path,
        Path::new("/models/model.gguf"),
        "demo",
        port,
        1,
        Duration::from_secs(2),
        || None,
    )
    .unwrap();
    let server = match outcome {
        StartOutcome::Ready(server) => server,
        _ => panic!("server did not become ready"),
    };
    let group = server.group;
    let mut server = ForegroundServer { server };
    http.join().unwrap();
    std::fs::write(release, b"go").unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let exit = loop {
        match server.poll().unwrap() {
            None => {}
            Some(exit) => break exit,
        }
        assert!(Instant::now() < deadline, "leader did not exit");
        std::thread::yield_now();
    };

    assert_eq!(exit.code, 7);
    assert!(
        exit.diagnostic
            .as_deref()
            .is_some_and(|text| text.contains("fatal: post-ready model crash")),
        "{exit:?}"
    );
    assert!(!process_group_exists(group).unwrap());
    assert!(!server.server.output_readers_owned());
}

#[cfg(unix)]
#[test]
fn discovery_uses_explicit_then_environment_then_managed_then_path() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let explicit = dir.path().join("explicit");
    let environment = dir.path().join("environment");
    let managed = dir.path().join("managed");
    let path_dir = dir.path().join("path");
    std::fs::create_dir(&path_dir).unwrap();
    let path_server = path_dir.join("llama-server");
    write_version_script(&explicit, "explicit", 0);
    write_version_script(&environment, "environment", 0);
    write_version_script(&managed, "version: 10121 (555881ebc)", 0);
    write_version_script(&path_server, "path", 0);
    let search_path = std::env::join_paths([&path_dir]).unwrap();

    assert_eq!(
        discover_server(
            Some(&explicit),
            Some(environment.as_os_str()),
            &managed,
            Some(search_path.as_os_str()),
        )
        .unwrap(),
        explicit
    );
    assert_eq!(
        discover_server(
            None,
            Some(environment.as_os_str()),
            &managed,
            Some(search_path.as_os_str()),
        )
        .unwrap(),
        environment
    );
    assert_eq!(
        discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
        managed
    );
    std::fs::remove_file(&managed).unwrap();
    assert_eq!(
        discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
        path_server
    );
}

#[cfg(unix)]
#[test]
fn successful_dual_stream_explicit_candidate_is_accepted() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let explicit = dir.path().join("explicit");
    let managed = dir.path().join("missing-managed");
    write_dual_stream_version_script(&explicit);

    assert_eq!(
        discover_server(Some(&explicit), None, &managed, None).unwrap(),
        explicit
    );
}

#[cfg(unix)]
#[test]
fn successful_dual_stream_environment_candidate_is_accepted() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let environment = dir.path().join("environment");
    let managed = dir.path().join("missing-managed");
    write_dual_stream_version_script(&environment);

    assert_eq!(
        discover_server(None, Some(environment.as_os_str()), &managed, None).unwrap(),
        environment
    );
}

#[cfg(unix)]
#[test]
fn successful_dual_stream_path_candidate_is_accepted() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("missing-managed");
    let path_server = dir.path().join("llama-server");
    write_dual_stream_version_script(&path_server);
    let search_path = std::env::join_paths([dir.path()]).unwrap();

    assert_eq!(
        discover_server(None, None, &managed, Some(search_path.as_os_str())).unwrap(),
        path_server
    );
}

#[cfg(unix)]
#[test]
fn explicit_and_environment_candidates_fail_immediately_when_invalid() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let missing_explicit = dir.path().join("missing-explicit");
    let missing_environment = dir.path().join("missing-environment");
    let failed_explicit = dir.path().join("failed-explicit");
    let failed_environment = dir.path().join("failed-environment");
    let managed = dir.path().join("managed");
    write_version_script(&failed_explicit, "explicit", 1);
    write_version_script(&failed_environment, "environment", 1);
    write_version_script(&managed, "version: 10121 (555881ebc)", 0);

    let explicit_error =
        discover_server(Some(&missing_explicit), None, &managed, None).unwrap_err();
    assert!(explicit_error.contains("--server"), "{explicit_error}");
    let explicit_error = discover_server(Some(&failed_explicit), None, &managed, None).unwrap_err();
    assert!(explicit_error.contains("--server"), "{explicit_error}");

    let environment_error =
        discover_server(None, Some(missing_environment.as_os_str()), &managed, None).unwrap_err();
    assert!(
        environment_error.contains("LOXA_LLAMA_SERVER"),
        "{environment_error}"
    );
    let environment_error =
        discover_server(None, Some(failed_environment.as_os_str()), &managed, None).unwrap_err();
    assert!(
        environment_error.contains("LOXA_LLAMA_SERVER"),
        "{environment_error}"
    );
}

#[cfg(unix)]
#[test]
fn managed_runtime_requires_exact_identity_and_reports_damage() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("managed");
    std::fs::write(&managed, b"not executable").unwrap();

    let error = discover_server(None, None, &managed, None).unwrap_err();
    assert!(
        error.contains("managed llama-server bundle is damaged"),
        "{error}"
    );
    assert!(error.contains("not executable"), "{error}");

    write_version_script(&managed, "version: 10090 (not-the-managed-build)", 0);
    let error = discover_server(None, None, &managed, None).unwrap_err();
    assert!(
        error.contains("managed llama-server bundle is damaged"),
        "{error}"
    );
    assert!(error.contains("version: 10121 (555881ebc)"), "{error}");
}

#[cfg(unix)]
#[test]
fn managed_version_probe_uses_the_selected_runtime_identity_not_manifest_provenance() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("managed");

    for identity in [
        RuntimeIdentity::LegacyCliB10121,
        RuntimeIdentity::BundledB10344,
    ] {
        write_version_script(&managed, identity.version_line(), 0);
        assert_eq!(
            validate_managed_server(&managed, identity).unwrap(),
            managed,
            "{identity:?}"
        );
        let other = if identity == RuntimeIdentity::LegacyCliB10121 {
            RuntimeIdentity::BundledB10344
        } else {
            RuntimeIdentity::LegacyCliB10121
        };
        let error = validate_managed_server(&managed, other).unwrap_err();
        assert!(error.contains(other.version_line()), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn managed_launch_admission_keeps_exact_probe_without_publishing_inventory_evidence() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("managed");
    write_version_script(&managed, "version: 10121 (555881ebc)", 0);

    assert_eq!(
        discover_from_process(
            None,
            &managed,
            &LaunchProfile::gemma4_mtp(None),
            RuntimeIdentity::LegacyCliB10121,
        )
        .unwrap(),
        managed
    );
    assert!(
        !managed.with_extension("qualification.json").exists(),
        "launch admission must not publish managed runtime inventory evidence"
    );
}

#[cfg(unix)]
#[test]
fn managed_runtime_rejects_ambiguous_dual_stream_identity() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("managed");
    write_executable_script(
            &managed,
            b"#!/bin/sh\nprintf '%s\\n' 'version: 10090 (wrong)' \nprintf '%s\\n' 'version: 10121 (555881ebc)' >&2\n",
        );

    let error = discover_server(None, None, &managed, None).unwrap_err();

    assert!(
        error.contains("managed llama-server bundle is damaged"),
        "{error}"
    );
    assert!(error.contains("ambiguous"), "{error}");
}

#[cfg(unix)]
#[test]
fn managed_runtime_allows_a_slow_cold_version_probe() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let managed = dir.path().join("managed");
    write_executable_script(
        &managed,
        b"#!/bin/sh\nsleep 4\nprintf '%s\\n' 'version: 10121 (555881ebc)' >&2\n",
    );

    assert_eq!(
        discover_server(None, None, &managed, None).unwrap(),
        managed
    );
}

#[cfg(unix)]
#[test]
fn version_probes_are_bounded() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    write_executable_script(&server, b"#!/bin/sh\n(sleep 2) &\nexit 0\n");
    let started = Instant::now();

    let error = probe_version_with_timeout(&server, VERSION_PROBE_TIMEOUT).unwrap_err();

    assert!(error.contains("timed out reading"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn missing_runtime_recommends_homebrew_without_environment_setup() {
    let dir = tempdir().unwrap();
    let managed = dir.path().join("missing-managed");

    let error = discover_server(None, None, &managed, None).unwrap_err();

    assert!(error.contains("brew install llama.cpp"), "{error}");
    assert!(!error.contains("LOXA_LLAMA_SERVER"), "{error}");
}

#[test]
fn readiness_rejects_a_redirect_to_an_alias_response() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let redirected = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server_redirected = Arc::clone(&redirected);
    let server = std::thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        let mut request = [0_u8; 1];
        first.read_exact(&mut request).unwrap();
        first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        drop(first);

        listener.set_nonblocking(true).unwrap();
        while !server_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut second, _)) => {
                    server_redirected.store(true, Ordering::SeqCst);
                    second.set_nonblocking(false).unwrap();
                    second.read_exact(&mut request).unwrap();
                    let body = r#"{"data":[{"id":"demo"}]}"#;
                    write!(
                            second,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .unwrap();
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("redirect server failed: {error}"),
            }
        }
    });

    let ready = readiness(&readiness_client().unwrap(), port, "demo").unwrap();
    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();

    assert!(!ready, "readiness followed a loopback redirect");
    assert!(!redirected.load(Ordering::SeqCst));
}

#[cfg(unix)]
#[test]
fn leader_exit_cleans_surviving_process_group_before_returning_status() {
    let _lock = process_test_lock();
    let dir = tempdir().unwrap();
    let server = dir.path().join("server");
    let child_ready = dir.path().join("child-ready");
    let group_file = dir.path().join("group");
    write_executable_script(
            &server,
            b"#!/bin/sh\n(\n  trap '' TERM\n  printf ready > \"$2\"\n  while :; do sleep 1; done\n) &\nwhile [ ! -f \"$2\" ]; do :; done\nprintf '%s\\n' \"$$\" > \"$4\"\nprintf 'fatal: model load failed\\n' >&2\nexit 7\n",
        );

    let status = run(
        &server,
        &child_ready,
        group_file.to_str().unwrap(),
        0,
        1,
        &dir.path().join("run"),
    )
    .unwrap();
    let group = std::fs::read_to_string(&group_file)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    let survived = process_group_exists(group).unwrap();
    if survived {
        // SAFETY: the script recorded the exact process group created by run.
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
    }

    assert_eq!(status, 7);
    assert!(!survived, "leader status returned while its group survived");
}

#[cfg(unix)]
#[test]
fn pre_ready_diagnostic_uses_stdout_fallback_and_caps_oversized_output() {
    let _lock = process_test_lock();
    for (stream, diagnostic) in [
        ("stdout", "stdout model failure"),
        ("stderr", "bounded tail marker"),
    ] {
        let dir = tempdir().unwrap();
        let server = dir.path().join("server");
        let output = if stream == "stdout" {
            format!("#!/bin/sh\nprintf '{diagnostic}\\n'\nexit 8\n")
        } else {
            format!(
                "#!/bin/sh\nprintf '{}{diagnostic}\\n' >&2\nexit 9\n",
                "x".repeat(8192)
            )
        };
        write_executable_script(&server, output.as_bytes());

        let outcome = OwnedServer::start(
            &server,
            Path::new("/models/model.gguf"),
            "demo",
            0,
            1,
            Duration::from_secs(2),
            || None,
        )
        .unwrap();
        let exit = match outcome {
            StartOutcome::Exited(exit) => exit,
            _ => panic!("expected pre-ready exit"),
        };
        let retained = exit.diagnostic.as_deref().unwrap_or_default();

        assert_eq!(exit.code, if stream == "stdout" { 8 } else { 9 });
        assert!(retained.contains(diagnostic), "{retained}");
        assert!(
            retained.len() <= MAX_DIAGNOSTIC_TAIL,
            "diagnostic was not capped: {}",
            retained.len()
        );
    }
}

#[cfg(unix)]
#[test]
fn teardown_waits_for_the_exact_process_group_to_disappear() {
    let _lock = process_test_lock();
    use std::os::unix::process::CommandExt;
    let dir = tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("(trap '' TERM; echo ready > \"$1\"; while :; do sleep 1; done) & wait")
        .arg("sh")
        .arg(&ready);
    command.process_group(0);
    let mut child = command.spawn().unwrap();
    let pgid = i32::try_from(child.id()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !ready.exists() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(ready.exists());
    terminate_owned_group(&mut child, pgid).unwrap();
    assert!(!process_group_exists(pgid).unwrap());
}
