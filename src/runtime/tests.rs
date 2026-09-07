use super::observation::exact_live_provenance;
use super::*;
use crate::app::{RuntimeInventorySnapshot, RuntimeOwnerSnapshot, RuntimeSnapshot, SnapshotReader};
use crate::paths::AppPaths;
use std::fs::TryLockError;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn fingerprint_value() -> serde_json::Value {
    serde_json::json!({
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
    })
}

fn lease_value(version: u32) -> serde_json::Value {
    serde_json::json!({
        "version": version,
        "owner_pid": 11,
        "owner_start_time": 12,
        "child_pid": 13,
        "child_start_time": 14,
        "child_pgid": 13,
        "server": "/bin/llama-server",
        "model_id": "demo",
        "port": 43123,
    })
}

#[test]
fn v2_wire_roundtrip_is_strict_and_requires_explicit_attachment_state() {
    let mut expected = lease_value(2);
    expected["owner_mode"] = serde_json::json!("persistent_app");
    expected["fingerprint"] = fingerprint_value();

    let lease = decode_lease(&serde_json::to_vec(&expected).unwrap()).unwrap();
    assert_eq!(lease.owner_mode(), Some(LeaseOwnerMode::PersistentApp));
    assert_eq!(
        serde_json::to_value(lease.persistent_fingerprint().unwrap()).unwrap(),
        expected["fingerprint"]
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encode_v2_lease_fixture(&lease)).unwrap(),
        expected
    );

    let mut missing_owner_mode = expected.clone();
    missing_owner_mode
        .as_object_mut()
        .unwrap()
        .remove("owner_mode");
    let mut missing_fingerprint = expected.clone();
    missing_fingerprint
        .as_object_mut()
        .unwrap()
        .remove("fingerprint");
    let mut unknown_lease_field = expected.clone();
    unknown_lease_field["unexpected"] = serde_json::json!(true);
    let mut unknown_fingerprint_field = expected.clone();
    unknown_fingerprint_field["fingerprint"]["unexpected"] = serde_json::json!(true);
    let mut unsupported_version = expected.clone();
    unsupported_version["version"] = serde_json::json!(99);
    let mut unknown_owner_mode = expected.clone();
    unknown_owner_mode["owner_mode"] = serde_json::json!("background");
    let mut persistent_without_fingerprint = expected.clone();
    persistent_without_fingerprint["fingerprint"] = serde_json::Value::Null;
    let mut foreground_with_fingerprint = expected.clone();
    foreground_with_fingerprint["owner_mode"] = serde_json::json!("foreground");
    let mut mismatched_model = expected.clone();
    mismatched_model["fingerprint"]["model_id"] = serde_json::json!("other");
    let mut wrong_sleep_policy = expected.clone();
    wrong_sleep_policy["fingerprint"]["sleep_policy"] = serde_json::Value::Null;
    let mut v1_with_v2_fields = lease_value(LEGACY_LEASE_VERSION);
    v1_with_v2_fields["owner_mode"] = serde_json::json!("persistent_app");
    v1_with_v2_fields["fingerprint"] = fingerprint_value();

    for (name, invalid) in [
        ("missing owner mode", missing_owner_mode),
        ("missing fingerprint", missing_fingerprint),
        ("unknown lease field", unknown_lease_field),
        ("unknown fingerprint field", unknown_fingerprint_field),
        ("unsupported version", unsupported_version),
        ("unknown owner mode", unknown_owner_mode),
        (
            "persistent owner without fingerprint",
            persistent_without_fingerprint,
        ),
        (
            "foreground owner with fingerprint",
            foreground_with_fingerprint,
        ),
        ("fingerprint for another model", mismatched_model),
        (
            "persistent fingerprint without sleep policy",
            wrong_sleep_policy,
        ),
        ("v1 state carrying v2 fields", v1_with_v2_fields),
    ] {
        assert!(
            decode_lease(&serde_json::to_vec(&invalid).unwrap()).is_err(),
            "accepted {name}"
        );
    }
}

#[test]
fn v4_service_wire_preserves_the_qualified_private_endpoint_contract() {
    let mut wire = lease_value(4);
    wire["owner_mode"] = serde_json::json!("service");
    wire["managed_source"] = serde_json::Value::Null;
    wire["port"] = serde_json::json!(0);
    wire["endpoint"] = serde_json::json!("/private/tmp/loxa/engine.sock");
    wire["parallel"] = serde_json::json!(1);
    wire["offline"] = serde_json::json!(true);
    wire["fingerprint"] = fingerprint_value();
    wire["fingerprint"]["schema_version"] = serde_json::json!(2);
    wire["fingerprint"]["sleep_policy"] = serde_json::Value::Null;
    wire["fingerprint"]["service_profile"] =
        serde_json::to_value(crate::runtime_fingerprint::ServiceRuntimeProfile::qualified())
            .unwrap();

    let mut lease = decode_lease(&serde_json::to_vec(&wire).unwrap()).unwrap();
    assert!(lease.persistent_fingerprint().is_none());
    lease.service = Some(ServiceLeaseFields::qualified(Path::new(
        "/private/tmp/loxa/engine.sock",
    )));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encode_lease(&lease).unwrap()).unwrap(),
        wire
    );

    for (field, value) in [
        ("parallel", serde_json::json!(2)),
        ("offline", serde_json::json!(false)),
        ("port", serde_json::json!(43123)),
        ("endpoint", serde_json::json!("relative.sock")),
        ("owner_mode", serde_json::json!("persistent_app")),
        ("model_id", serde_json::json!("other")),
    ] {
        let mut invalid = wire.clone();
        invalid[field] = value;
        assert!(
            decode_lease(&serde_json::to_vec(&invalid).unwrap()).is_err(),
            "accepted incompatible service field {field}"
        );
    }

    let mut missing_endpoint = wire;
    missing_endpoint.as_object_mut().unwrap().remove("endpoint");
    assert!(decode_lease(&serde_json::to_vec(&missing_endpoint).unwrap()).is_err());
}

#[test]
fn v2_staged_process_remains_readable_but_does_not_invent_managed_provenance() {
    let mut child = spawn_observable_server("demo", 43123);
    let observed = observed_lease(&child, "demo", 43123);
    let mut wire = lease_value(2);
    wire["owner_mode"] = serde_json::json!("foreground");
    wire["fingerprint"] = serde_json::Value::Null;
    wire["owner_pid"] = serde_json::json!(observed.owner_pid);
    wire["owner_start_time"] = serde_json::json!(observed.owner_start_time);
    wire["child_pid"] = serde_json::json!(observed.child_pid);
    wire["child_start_time"] = serde_json::json!(observed.child_start_time);
    wire["child_pgid"] = serde_json::json!(observed.child_pgid);
    wire["server"] = serde_json::json!(observed.server);

    let decoded = decode_lease(&serde_json::to_vec(&wire).unwrap()).unwrap();
    assert_eq!(decoded.managed_source.as_deref(), None);
    assert_eq!(decoded.server, observed.server);
    assert_eq!(
        exact_live_provenance(&decoded, Path::new("/managed/llama-server")).unwrap(),
        Some(RuntimeProvenance::External),
        "a v2 lease cannot safely attribute a staged executable to a canonical source"
    );

    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn v3_wire_separates_exact_process_identity_from_managed_source() {
    let mut child = spawn_observable_server("demo", 43123);
    let observed = observed_lease(&child, "demo", 43123);
    let managed = PathBuf::from("/managed/llama-server");
    let mut wire = lease_value(3);
    wire["owner_mode"] = serde_json::json!("foreground");
    wire["fingerprint"] = serde_json::Value::Null;
    wire["managed_source"] = serde_json::json!(managed);
    wire["owner_pid"] = serde_json::json!(observed.owner_pid);
    wire["owner_start_time"] = serde_json::json!(observed.owner_start_time);
    wire["child_pid"] = serde_json::json!(observed.child_pid);
    wire["child_start_time"] = serde_json::json!(observed.child_start_time);
    wire["child_pgid"] = serde_json::json!(observed.child_pgid);
    wire["server"] = serde_json::json!(observed.server);

    let decoded = decode_lease(&serde_json::to_vec(&wire).unwrap()).unwrap();
    assert_eq!(decoded.server, observed.server);
    assert_eq!(decoded.managed_source.as_deref(), Some(managed.as_path()));
    assert_eq!(
        exact_live_provenance(&decoded, &managed).unwrap(),
        Some(RuntimeProvenance::Managed)
    );
    assert_eq!(
        exact_live_provenance(&decoded, Path::new("/other/llama-server")).unwrap(),
        Some(RuntimeProvenance::External),
        "a mismatched canonical source must not be reported as managed"
    );

    let mut forged_process_identity = decoded.clone();
    forged_process_identity.server = PathBuf::from("/managed/llama-server");
    assert_eq!(
        exact_live_provenance(&forged_process_identity, &managed).unwrap(),
        None,
        "canonical-source metadata must not replace exact child-process identity"
    );

    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn v3_wire_requires_an_explicit_absolute_managed_source() {
    let mut valid = lease_value(3);
    valid["owner_mode"] = serde_json::json!("foreground");
    valid["fingerprint"] = serde_json::Value::Null;
    valid["managed_source"] = serde_json::Value::Null;
    assert!(decode_lease(&serde_json::to_vec(&valid).unwrap()).is_ok());

    let mut missing = valid.clone();
    missing.as_object_mut().unwrap().remove("managed_source");
    let mut relative = valid.clone();
    relative["managed_source"] = serde_json::json!("relative/llama-server");
    let mut empty = valid;
    empty["managed_source"] = serde_json::json!("");

    for (name, invalid) in [
        ("missing managed source", missing),
        ("relative managed source", relative),
        ("empty managed source", empty),
    ] {
        assert!(
            decode_lease(&serde_json::to_vec(&invalid).unwrap()).is_err(),
            "accepted {name}"
        );
    }
}

#[test]
fn exclusive_recovery_discards_untrusted_regular_state_without_using_embedded_identities() {
    let mut child = spawn_sleep();
    let pid = child.id();
    let group = i32::try_from(pid).unwrap();
    let snapshot = process_snapshot(pid).unwrap().unwrap();
    let mut v1 = lease_value(LEGACY_LEASE_VERSION);
    v1["owner_pid"] = serde_json::json!(u32::MAX);
    v1["owner_start_time"] = serde_json::json!(1);
    v1["child_pid"] = serde_json::json!(pid);
    v1["child_start_time"] = serde_json::json!(snapshot.start_identity);
    v1["child_pgid"] = serde_json::json!(group);
    v1["server"] = serde_json::json!(snapshot.executable);
    let mut v2 = v1.clone();
    v2["version"] = serde_json::json!(PERSISTENT_LEASE_VERSION);
    v2["owner_mode"] = serde_json::json!("foreground");
    v2["fingerprint"] = serde_json::Value::Null;

    let mut unsupported = v2.clone();
    unsupported["version"] = serde_json::json!(77);
    let mut malformed_v1 = v1;
    malformed_v1["owner_mode"] = serde_json::json!("persistent_app");
    let mut malformed_v2 = v2;
    malformed_v2.as_object_mut().unwrap().remove("owner_mode");
    let cases = [
        ("malformed JSON", b"not JSON".to_vec()),
        (
            "unsupported version",
            serde_json::to_vec(&unsupported).unwrap(),
        ),
        ("malformed v1", serde_json::to_vec(&malformed_v1).unwrap()),
        ("malformed v2", serde_json::to_vec(&malformed_v2).unwrap()),
    ];

    for (name, bytes) in cases {
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("foreground.json");
        fs::write(&state_path, bytes).unwrap();

        let ownership = RuntimeOwnership::acquire(dir.path())
            .unwrap_or_else(|error| panic!("{name} blocked exclusive recovery: {error}"));

        assert!(!state_path.exists(), "{name} survived exclusive recovery");
        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "{name} caused an embedded process identity to be signaled"
        );
        drop(ownership);
    }

    terminate_process_group(&mut child, group).unwrap();
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum UnsafeLeaseEntry {
    Symlink,
    HardLink,
    Directory,
    Fifo,
}

#[cfg(unix)]
#[test]
fn exclusive_recovery_rejects_unsafe_filesystem_objects_without_waiting() {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    for kind in [
        UnsafeLeaseEntry::Symlink,
        UnsafeLeaseEntry::HardLink,
        UnsafeLeaseEntry::Directory,
        UnsafeLeaseEntry::Fifo,
    ] {
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("foreground.json");
        let witness = dir.path().join("witness");
        let result = dir.path().join("unsafe-acquisition.result");
        fs::write(&witness, b"witness").unwrap();
        match kind {
            UnsafeLeaseEntry::Symlink => std::os::unix::fs::symlink(&witness, &state_path).unwrap(),
            UnsafeLeaseEntry::HardLink => fs::hard_link(&witness, &state_path).unwrap(),
            UnsafeLeaseEntry::Directory => fs::create_dir(&state_path).unwrap(),
            UnsafeLeaseEntry::Fifo => {
                let path = std::ffi::CString::new(state_path.as_os_str().as_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            }
        }

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--ignored")
            .arg("--exact")
            .arg("runtime::tests::unsafe_lease_acquisition_child")
            .env("LOXA_UNSAFE_LEASE_RUN_DIR", dir.path())
            .env("LOXA_UNSAFE_LEASE_RESULT", &result)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("unsafe {kind:?} lease blocked during acquisition");
            }
            thread::sleep(Duration::from_millis(10));
        };

        assert!(status.success(), "unsafe {kind:?} helper failed");
        assert_eq!(fs::read(&result).unwrap(), b"error", "accepted {kind:?}");
        assert_eq!(fs::read(&witness).unwrap(), b"witness");
        let metadata = fs::symlink_metadata(&state_path).unwrap();
        match kind {
            UnsafeLeaseEntry::Symlink => assert!(metadata.file_type().is_symlink()),
            UnsafeLeaseEntry::HardLink => assert_eq!(metadata.nlink(), 2),
            UnsafeLeaseEntry::Directory => assert!(metadata.file_type().is_dir()),
            UnsafeLeaseEntry::Fifo => assert!(metadata.file_type().is_fifo()),
        }
    }
}

fn spawn_sleep() -> Child {
    let mut command = Command::new("/bin/sleep");
    command.arg("60");
    command.process_group(0);
    command.spawn().unwrap()
}

fn spawn_lease_observing_sleep(lease: &Path, witness: &Path, ready: &Path) -> Child {
    let mut command = Command::new("/bin/bash");
    command
            .arg("-c")
            .arg(
                r#"trap 'if [ -e "$LOXA_LEASE" ]; then printf present > "$LOXA_WITNESS"; else printf absent > "$LOXA_WITNESS"; fi; exit 0' TERM
: > "$LOXA_READY"
while :; do sleep 60; done"#,
            )
            .env("LOXA_LEASE", lease)
            .env("LOXA_WITNESS", witness)
            .env("LOXA_READY", ready)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
    command.spawn().unwrap()
}

fn spawn_observable_server(model_id: &str, port: u16) -> Child {
    let mut command = Command::new("/bin/bash");
    command
        .arg("-c")
        .arg("while :; do sleep 60; done")
        .arg("--alias")
        .arg(model_id)
        .arg("--port")
        .arg(port.to_string())
        .process_group(0);
    command.spawn().unwrap()
}

fn observed_lease(child: &Child, model_id: &str, port: u16) -> RuntimeLease {
    let child_pid = child.id();
    let child = process_snapshot(child_pid).unwrap().unwrap();
    let owner = process_snapshot(std::process::id()).unwrap().unwrap();
    RuntimeLease {
        version: LEASE_VERSION,
        owner_mode: Some(LeaseOwnerMode::Foreground),
        fingerprint: None,
        managed_source: None,
        owner_pid: std::process::id(),
        owner_start_time: owner.start_identity,
        child_pid,
        child_start_time: child.start_identity,
        child_pgid: i32::try_from(child_pid).unwrap(),
        server: child.executable,
        model_id: model_id.into(),
        port,
        service: None,
    }
}

fn write_lease_fixture(path: &Path, lease: &RuntimeLease) {
    let bytes = match lease.version {
        LEGACY_LEASE_VERSION => serde_json::to_vec_pretty(&RuntimeLeaseV1 {
            version: lease.version,
            owner_pid: lease.owner_pid,
            owner_start_time: lease.owner_start_time,
            child_pid: lease.child_pid,
            child_start_time: lease.child_start_time,
            child_pgid: lease.child_pgid,
            server: lease.server.clone(),
            model_id: lease.model_id.clone(),
            port: lease.port,
        })
        .unwrap(),
        PERSISTENT_LEASE_VERSION => encode_v2_lease_fixture(lease),
        LEASE_VERSION => encode_v3_lease(lease).unwrap(),
        version => panic!("unsupported runtime lease fixture version {version}"),
    };
    fs::write(path, bytes).unwrap();
}

fn encode_v2_lease_fixture(lease: &RuntimeLease) -> Vec<u8> {
    serde_json::to_vec_pretty(&RuntimeLeaseV2 {
        version: lease.version,
        owner_mode: lease.owner_mode.unwrap(),
        fingerprint: lease.fingerprint.clone(),
        owner_pid: lease.owner_pid,
        owner_start_time: lease.owner_start_time,
        child_pid: lease.child_pid,
        child_start_time: lease.child_start_time,
        child_pgid: lease.child_pgid,
        server: lease.server.clone(),
        model_id: lease.model_id.clone(),
        port: lease.port,
    })
    .unwrap()
}

struct ForegroundLockHolder {
    child: Child,
    release: PathBuf,
}

impl Drop for ForegroundLockHolder {
    fn drop(&mut self) {
        let _ = fs::write(&self.release, b"release");
        let _ = self.child.wait();
    }
}

fn wait_for_path(path: &Path, description: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}: {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn hold_foreground_lock(run_dir: &Path) -> ForegroundLockHolder {
    fs::create_dir_all(run_dir).unwrap();
    let ready = run_dir.join("foreground-lock.ready");
    let release = run_dir.join("foreground-lock.release");
    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--ignored")
        .arg("--exact")
        .arg("runtime::tests::foreground_record_lock_holder_process")
        .env("LOXA_FOREGROUND_LOCK_PATH", run_dir.join("foreground.lock"))
        .env("LOXA_FOREGROUND_LOCK_READY", &ready)
        .env("LOXA_FOREGROUND_LOCK_RELEASE", &release)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_path(&ready, "foreground lock holder");
    ForegroundLockHolder { child, release }
}

fn foreground_contender_acquires(run_dir: &Path) -> bool {
    fs::create_dir_all(run_dir).unwrap();
    let result = run_dir.join("foreground-contender.result");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--ignored")
        .arg("--exact")
        .arg("runtime::tests::foreground_record_lock_contender_process")
        .env("LOXA_FOREGROUND_LOCK_PATH", run_dir.join("foreground.lock"))
        .env("LOXA_FOREGROUND_LOCK_RESULT", &result)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    assert!(child.wait().unwrap().success());
    fs::read(&result).unwrap() == b"acquired"
}

#[cfg(unix)]
#[test]
#[ignore]
fn foreground_record_lock_holder_process() {
    let path = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_PATH").unwrap());
    let ready = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_READY").unwrap());
    let release = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_RELEASE").unwrap());
    let lock = open_lock(&path).unwrap();
    try_acquire_foreground_lock(&lock).unwrap();
    fs::write(ready, b"ready").unwrap();
    wait_for_path(&release, "foreground lock release");
}

#[cfg(unix)]
#[test]
#[ignore]
fn foreground_record_lock_contender_process() {
    let path = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_PATH").unwrap());
    let result = PathBuf::from(std::env::var_os("LOXA_FOREGROUND_LOCK_RESULT").unwrap());
    let lock = open_lock(&path).unwrap();
    let outcome = match try_acquire_foreground_lock(&lock) {
        Ok(_) => b"acquired".as_slice(),
        Err(TryLockError::WouldBlock) => b"blocked".as_slice(),
        Err(TryLockError::Error(error)) => panic!("unexpected contender error: {error}"),
    };
    fs::write(result, outcome).unwrap();
}

#[cfg(unix)]
#[test]
#[ignore]
fn unsafe_lease_acquisition_child() {
    let run_dir = PathBuf::from(std::env::var_os("LOXA_UNSAFE_LEASE_RUN_DIR").unwrap());
    let result = PathBuf::from(std::env::var_os("LOXA_UNSAFE_LEASE_RESULT").unwrap());
    let outcome = if RuntimeOwnership::acquire(&run_dir).is_err() {
        b"error".as_slice()
    } else {
        b"acquired".as_slice()
    };
    fs::write(result, outcome).unwrap();
}

#[cfg(unix)]
#[test]
fn foreground_lock_query_never_owns_an_available_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("foreground.lock");
    drop(open_lock(&path).unwrap());

    assert!(!foreground_lock_is_held(&path).unwrap());

    let _holder = hold_foreground_lock(dir.path());
    let contender = open_lock(&path).unwrap();
    assert!(matches!(
        try_acquire_foreground_lock(&contender),
        Err(TryLockError::WouldBlock)
    ));
}

#[cfg(unix)]
#[test]
fn foreground_query_holds_its_local_operation_guard_while_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("foreground.lock");
    drop(open_lock(&path).unwrap());

    assert!(!foreground_lock_is_held_with_after_open(&path, || {
        assert!(
            LOCAL_FOREGROUND_LOCK_OPERATIONS.try_lock().is_err(),
            "the query's descriptor lifetime must be serialized with local acquisition"
        );
    })
    .unwrap());
}

#[cfg(unix)]
#[test]
fn same_process_observation_keeps_foreground_owner_exclusive() {
    let dir = tempdir().unwrap();
    let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();
    recover_stale(dir.path()).unwrap();
    let mut observer = ForegroundObserver::new(dir.path().to_path_buf());

    let observation = observer.observe(Path::new("/managed/llama-server"));
    let contender_acquired = foreground_contender_acquires(dir.path());
    assert_eq!(
        (observation, contender_acquired),
        (ForegroundObservation::Starting, false),
        "an in-process observation must neither hide nor release lifecycle ownership"
    );

    drop(ownership);
    assert!(
        foreground_contender_acquires(dir.path()),
        "dropping ownership must release the lifecycle lock"
    );
}

#[cfg(unix)]
#[test]
fn child_token_keeps_common_ownership_after_parent_handle_drops() {
    if std::env::var_os("LOXA_CHILD_TOKEN_LIFETIME_CHILD").is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::tests::child_token_keeps_common_ownership_after_parent_handle_drops",
                "--nocapture",
            ])
            .env("LOXA_CHILD_TOKEN_LIFETIME_CHILD", "1")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }

    let dir = tempdir().unwrap();
    let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();
    let child = ownership.reserve_child().unwrap();

    drop(ownership);

    assert!(
        RuntimeOwnership::acquire(dir.path()).is_err(),
        "the child token released common runtime ownership"
    );
    drop(child);
    drop(RuntimeOwnership::acquire(dir.path()).unwrap());
}

#[cfg(unix)]
#[test]
fn spawned_child_without_verified_cleanup_keeps_retained_owner_closed() {
    if std::env::var_os("LOXA_SPAWNED_CHILD_FAIL_CLOSED_CHILD").is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::tests::spawned_child_without_verified_cleanup_keeps_retained_owner_closed",
                    "--nocapture",
                ])
                .env("LOXA_SPAWNED_CHILD_FAIL_CLOSED_CHILD", "1")
                .output()
                .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }

    let dir = tempdir().unwrap();
    let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();
    let mut child = ownership.reserve_child().unwrap();
    child.child_spawned();

    drop(child);

    assert!(
        ownership.reserve_child().is_err(),
        "an unverified spawned child released its reservation"
    );
    assert!(
        RuntimeOwnership::acquire(dir.path()).is_err(),
        "an unverified spawned child released common ownership"
    );
    drop(ownership);
    drop(RuntimeOwnership::acquire(dir.path()).unwrap());
}

#[cfg(unix)]
#[test]
fn traditional_lock_fallback_isolates_parent_symlink_aliases() {
    let dir = tempdir().unwrap();
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
    let path = dir.path().join("foreground.lock");
    let mut local_lock = LocalForegroundLock::reserve(&path).unwrap();
    let lock = open_lock(&path).unwrap();
    local_lock.bind_to_file(&lock).unwrap();
    set_foreground_lock_with_command(&lock, libc::F_SETLK).unwrap();

    assert!(
        foreground_lock_is_held(&alias.join("foreground.lock")).unwrap(),
        "the fallback must not open and close an alias of its own lock"
    );
    assert!(
        !foreground_contender_acquires(&alias),
        "an alias observation must not release traditional ownership"
    );
}

#[cfg(unix)]
#[test]
fn traditional_fallback_reconciliation_failure_preserves_the_next_owner_lock() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("foreground.json");
    let lock_path = dir.path().join("foreground.lock");
    let witness = dir.path().join("unsafe-state-witness");
    fs::write(&witness, b"not valid runtime state").unwrap();
    std::os::unix::fs::symlink(&witness, &state_path).unwrap();

    let (closed_tx, closed_rx) = std::sync::mpsc::sync_channel(0);
    let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(0);
    let first_dir = dir.path().to_path_buf();
    let first_lock_path = lock_path.clone();
    let first = thread::spawn(move || {
        RuntimeOwnership::acquire_forced_traditional_with_after_file_close(&first_dir, move || {
            assert!(
                LocalForegroundLock::is_held(&first_lock_path).unwrap(),
                "the traditional reservation must survive until after its descriptor closes"
            );
            assert!(
                LOCAL_FOREGROUND_LOCK_OPERATIONS.try_lock().is_err(),
                "the operation guard must cover reconciliation unwind"
            );
            closed_tx.send(()).unwrap();
            release_first_rx.recv().unwrap();
        })
    });

    closed_rx.recv().unwrap();

    let (second_attempt_tx, second_attempt_rx) = std::sync::mpsc::sync_channel(0);
    let (second_ready_tx, second_ready_rx) = std::sync::mpsc::sync_channel(0);
    let (release_second_tx, release_second_rx) = std::sync::mpsc::sync_channel(0);
    let second_dir = dir.path().to_path_buf();
    let second = thread::spawn(move || {
        second_attempt_tx.send(()).unwrap();
        let ownership = RuntimeOwnership::acquire_forced_traditional(&second_dir).unwrap();
        second_ready_tx.send(()).unwrap();
        release_second_rx.recv().unwrap();
        drop(ownership);
    });
    second_attempt_rx.recv().unwrap();

    fs::remove_file(&state_path).unwrap();
    release_first_tx.send(()).unwrap();
    let first_result = first.join().unwrap();
    assert!(
        first_result.is_err(),
        "first acquisition unexpectedly succeeded"
    );
    let first_error = first_result.err().unwrap();
    assert!(first_error.contains("foreground.json"), "{first_error}");

    second_ready_rx.recv().unwrap();
    assert!(
        !foreground_contender_acquires(dir.path()),
        "the next traditional owner must stay exclusive to child contenders"
    );

    release_second_tx.send(()).unwrap();
    second.join().unwrap();
    assert!(
        foreground_contender_acquires(dir.path()),
        "dropping the next traditional owner must release the lifecycle lock"
    );
}

#[cfg(unix)]
#[test]
fn foreground_observer_reports_a_real_contending_owner_without_acquiring_the_lock() {
    let dir = tempdir().unwrap();
    let _holder = hold_foreground_lock(dir.path());
    let mut observer = ForegroundObserver::new(dir.path().to_path_buf());

    assert_eq!(
        observer.observe(Path::new("/managed/llama-server")),
        ForegroundObservation::Starting
    );

    let contender = open_lock(&dir.path().join("foreground.lock")).unwrap();
    assert!(matches!(
        try_acquire_foreground_lock(&contender),
        Err(TryLockError::WouldBlock)
    ));
}

#[test]
fn foreground_observer_distinguishes_cold_start_running_stopping_and_idle_without_mutation() {
    let dir = tempdir().unwrap();
    let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
    let managed = Path::new("/managed/llama-server");
    let lock = hold_foreground_lock(dir.path());
    assert_eq!(observer.observe(managed), ForegroundObservation::Starting);

    let mut child = spawn_observable_server("demo", 43123);
    let lease = observed_lease(&child, "demo", 43123);
    let state_path = dir.path().join("foreground.json");
    write_lease(&state_path, &lease).unwrap();
    let before = fs::read(&state_path).unwrap();
    let owner = process_snapshot(lease.owner_pid).unwrap().unwrap();
    let observed_child = process_snapshot(lease.child_pid).unwrap().unwrap();
    assert_eq!(owner.start_identity, lease.owner_start_time);
    assert_eq!(observed_child.start_identity, lease.child_start_time);
    assert_eq!(observed_child.executable, lease.server);
    assert_eq!(process_group(lease.child_pid).unwrap(), lease.child_pgid);
    assert!(command_has_unique_option(
        &observed_child.command,
        "--alias",
        OsStr::new("demo")
    ));
    assert!(command_has_unique_option(
        &observed_child.command,
        "--port",
        OsStr::new("43123")
    ));
    assert_eq!(
        observer.observe(managed),
        ForegroundObservation::Running {
            provenance: RuntimeProvenance::External,
            owner: RuntimeOwner::Foreground,
            model_id: "demo".into(),
            port: 43123,
        }
    );
    assert_eq!(fs::read(&state_path).unwrap(), before);
    assert!(dir.path().join("foreground.lock").is_file());

    fs::remove_file(&state_path).unwrap();
    assert_eq!(observer.observe(managed), ForegroundObservation::Stopping);
    drop(lock);
    assert_eq!(observer.observe(managed), ForegroundObservation::Idle);
    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn live_v1_remains_observable_but_never_attachable_or_rewritten() {
    let dir = tempdir().unwrap();
    let _lock = hold_foreground_lock(dir.path());
    let mut child = spawn_observable_server("demo", 43123);
    let mut lease = observed_lease(&child, "demo", 43123);
    lease.version = LEGACY_LEASE_VERSION;
    lease.owner_mode = None;
    lease.fingerprint = None;
    let state_path = dir.path().join("foreground.json");
    write_lease_fixture(&state_path, &lease);
    let before = fs::read(&state_path).unwrap();

    let decoded = read_lease(&state_path).unwrap();
    assert_eq!(decoded.owner_mode(), None);
    assert!(decoded.persistent_fingerprint().is_none());
    assert!(encode_v3_lease(&decoded).is_err());
    let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
    assert_eq!(
        observer.observe(Path::new("/managed/llama-server")),
        ForegroundObservation::Running {
            provenance: RuntimeProvenance::External,
            owner: RuntimeOwner::Legacy,
            model_id: "demo".into(),
            port: 43123,
        }
    );
    assert_eq!(fs::read(&state_path).unwrap(), before);

    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn live_v1_and_v2_owner_matrix_blocks_takeover_without_rewriting() {
    let mut child = spawn_observable_server("demo", 43123);
    let base = observed_lease(&child, "demo", 43123);
    let fingerprint: RuntimeFingerprint = serde_json::from_value(fingerprint_value()).unwrap();
    let cases = [
        ("v1", LEGACY_LEASE_VERSION, None, None),
        (
            "v2 foreground",
            PERSISTENT_LEASE_VERSION,
            Some(LeaseOwnerMode::Foreground),
            None,
        ),
        (
            "v2 persistent",
            PERSISTENT_LEASE_VERSION,
            Some(LeaseOwnerMode::PersistentApp),
            Some(fingerprint.clone()),
        ),
        (
            "v3 foreground",
            LEASE_VERSION,
            Some(LeaseOwnerMode::Foreground),
            None,
        ),
        (
            "v3 persistent",
            LEASE_VERSION,
            Some(LeaseOwnerMode::PersistentApp),
            Some(fingerprint),
        ),
    ];

    for (name, version, owner_mode, fingerprint) in cases {
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("foreground.json");
        let lease = RuntimeLease {
            version,
            owner_mode,
            fingerprint,
            managed_source: None,
            ..base.clone()
        };
        write_lease_fixture(&state_path, &lease);
        let before = fs::read(&state_path).unwrap();

        let error = match RuntimeOwnership::acquire(dir.path()) {
            Ok(_) => panic!("{name} live owner allowed a takeover"),
            Err(error) => error,
        };

        assert!(
            error.contains("another Loxa runtime owns"),
            "{name}: {error}"
        );
        assert_eq!(fs::read(&state_path).unwrap(), before, "rewrote {name}");
        assert!(
            process_snapshot(child.id()).unwrap().is_some(),
            "{name} live child was signaled"
        );
    }

    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn held_lock_preserves_malformed_and_unsupported_v1_v2_state() {
    let mut unsupported = lease_value(91);
    unsupported["owner_mode"] = serde_json::json!("persistent_app");
    unsupported["fingerprint"] = fingerprint_value();
    let mut malformed_v1 = lease_value(LEGACY_LEASE_VERSION);
    malformed_v1["unexpected"] = serde_json::json!(true);
    let mut malformed_v2 = lease_value(PERSISTENT_LEASE_VERSION);
    malformed_v2["owner_mode"] = serde_json::json!("foreground");
    let cases = [
        ("malformed JSON", b"not JSON".to_vec()),
        (
            "unsupported version",
            serde_json::to_vec(&unsupported).unwrap(),
        ),
        ("malformed v1", serde_json::to_vec(&malformed_v1).unwrap()),
        ("malformed v2", serde_json::to_vec(&malformed_v2).unwrap()),
    ];

    for (name, bytes) in cases {
        let dir = tempdir().unwrap();
        let _lock = hold_foreground_lock(dir.path());
        let state_path = dir.path().join("foreground.json");
        fs::write(&state_path, &bytes).unwrap();

        recover_stale(dir.path()).unwrap();
        let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
        assert_eq!(
            observer.observe(Path::new("/managed/llama-server")),
            ForegroundObservation::Starting,
            "{name} changed observation"
        );
        assert_eq!(fs::read(&state_path).unwrap(), bytes, "rewrote {name}");
        let error = match RuntimeOwnership::acquire(dir.path()) {
            Ok(_) => panic!("{name} bypassed a held lifecycle lock"),
            Err(error) => error,
        };
        assert!(error.contains("another Loxa runtime is active"), "{error}");
        assert_eq!(fs::read(&state_path).unwrap(), bytes, "rewrote {name}");
    }
}

#[test]
fn foreground_observer_treats_lease_lock_contradictions_and_malformed_leases_conservatively() {
    let managed = Path::new("/managed/llama-server");
    let mut child = spawn_observable_server("demo", 43123);
    let lease = observed_lease(&child, "demo", 43123);

    let contradictory = tempdir().unwrap();
    write_lease(&contradictory.path().join("foreground.json"), &lease).unwrap();
    let mut contradictory_observer = ForegroundObserver::new(contradictory.path().to_path_buf());
    assert_eq!(
        contradictory_observer.observe(managed),
        ForegroundObservation::Error
    );
    assert!(contradictory.path().join("foreground.json").is_file());

    let malformed = tempdir().unwrap();
    let _lock = hold_foreground_lock(malformed.path());
    let malformed_path = malformed.path().join("foreground.json");
    let bytes = b"Authorization: Bearer secret";
    fs::write(&malformed_path, bytes).unwrap();
    let mut malformed_observer = ForegroundObserver::new(malformed.path().to_path_buf());
    assert_eq!(
        malformed_observer.observe(managed),
        ForegroundObservation::Starting
    );
    assert_eq!(fs::read(&malformed_path).unwrap(), bytes);
    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn oversized_runtime_record_is_bounded_and_observed_conservatively() {
    let root = tempdir().unwrap();
    let _lock = hold_foreground_lock(root.path());
    let lease_path = root.path().join("foreground.json");
    let bytes = vec![b'x'; MAX_RUNTIME_RECORD_BYTES + 1];
    fs::write(&lease_path, &bytes).unwrap();

    let error = read_regular_file(&lease_path).unwrap_err();
    assert!(error.contains("65536-byte limit"), "{error}");
    let mut observer = ForegroundObserver::new(root.path().to_path_buf());
    assert_eq!(
        observer.observe(Path::new("/managed/llama-server")),
        ForegroundObservation::Starting
    );
    assert_eq!(fs::metadata(&lease_path).unwrap().len(), bytes.len() as u64);
}

#[test]
fn foreground_observer_requires_every_live_identity_and_allows_only_teardown_grace() {
    let managed = Path::new("/managed/llama-server");
    let mut child = spawn_observable_server("demo", 43123);
    let lease = observed_lease(&child, "demo", 43123);

    for mismatch in [
        {
            let mut mismatch = lease.clone();
            mismatch.owner_start_time = mismatch.owner_start_time.saturating_add(1);
            mismatch
        },
        {
            let mut mismatch = lease.clone();
            mismatch.child_start_time = mismatch.child_start_time.saturating_add(1);
            mismatch
        },
        {
            let mut mismatch = lease.clone();
            mismatch.child_pgid = -1;
            mismatch
        },
        {
            let mut mismatch = lease.clone();
            mismatch.server = PathBuf::from("/other/llama-server");
            mismatch
        },
        {
            let mut mismatch = lease.clone();
            mismatch.model_id = "other".into();
            mismatch
        },
        {
            let mut mismatch = lease.clone();
            mismatch.port = 43124;
            mismatch
        },
    ] {
        let dir = tempdir().unwrap();
        let _lock = hold_foreground_lock(dir.path());
        fs::write(
            dir.path().join("foreground.json"),
            encode_v2_lease_fixture(&mismatch),
        )
        .unwrap();
        let mut observer = ForegroundObserver::new(dir.path().to_path_buf());
        assert_ne!(
            observer.observe(managed),
            ForegroundObservation::Running {
                provenance: RuntimeProvenance::External,
                owner: RuntimeOwner::Foreground,
                model_id: "demo".into(),
                port: 43123,
            },
            "mismatch unexpectedly became Running: {mismatch:?}"
        );
    }

    let grace = tempdir().unwrap();
    let _lock = hold_foreground_lock(grace.path());
    let state_path = grace.path().join("foreground.json");
    write_lease(&state_path, &lease).unwrap();
    let mut observer = ForegroundObserver::new(grace.path().to_path_buf());
    assert_eq!(
        observer.observe(managed),
        ForegroundObservation::Running {
            provenance: RuntimeProvenance::External,
            owner: RuntimeOwner::Foreground,
            model_id: "demo".into(),
            port: 43123,
        }
    );
    let mut wrong_port = lease;
    wrong_port.port = 43124;
    write_lease(&state_path, &wrong_port).unwrap();
    assert_eq!(observer.observe(managed), ForegroundObservation::Stopping);
    thread::sleep(Duration::from_millis(510));
    assert_eq!(observer.observe(managed), ForegroundObservation::Error);
    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

#[test]
fn snapshot_reader_surfaces_only_an_exact_external_foreground_runtime() {
    let root = tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let lock = hold_foreground_lock(&paths.run);
    let mut child = spawn_observable_server("demo", 43123);
    let lease = observed_lease(&child, "demo", 43123);
    let lease_path = paths.run.join("foreground.json");
    write_lease(&lease_path, &lease).unwrap();
    let before = fs::read(&lease_path).unwrap();

    let mut reader = SnapshotReader::new(paths);
    let snapshot = reader.observe();

    assert_eq!(snapshot.runtime(), RuntimeSnapshot::Running);
    assert_eq!(
        snapshot.runtime_owner(),
        Some(RuntimeOwnerSnapshot::Foreground)
    );
    assert_eq!(snapshot.runtime_model_id(), Some("demo"));
    assert_eq!(
        snapshot.runtime_inventory(),
        RuntimeInventorySnapshot::External
    );
    assert_eq!(fs::read(&lease_path).unwrap(), before);
    drop(lock);
    let group = i32::try_from(child.id()).unwrap();
    terminate_process_group(&mut child, group).unwrap();
}

fn wait_for_child_exit(child: &mut Child) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child
            .try_wait()
            .expect("failed to reap test child")
            .is_some()
        {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

fn staged_sleep(run_dir: &Path, token: &str) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;

    let stage = run_dir.join(format!(".bundled-runtime-exec-{token}"));
    let server = stage.join("Contents/MacOS/llama-server");
    fs::create_dir_all(server.parent().unwrap()).unwrap();
    fs::set_permissions(&stage, fs::Permissions::from_mode(0o700)).unwrap();
    fs::copy("/bin/sleep", &server).unwrap();
    fs::set_permissions(&server, fs::Permissions::from_mode(0o555)).unwrap();
    (stage, server)
}

#[test]
fn graceful_clear_removes_only_the_exact_recorded_execution_stage() {
    let dir = tempdir().unwrap();
    let run_dir = dir.path().join("run");
    let (stage, server) = staged_sleep(&run_dir, "0123456789abcdef0123456789abcdef");
    let neighbor = run_dir.join(".bundled-runtime-exec-0123456789abcdef0123456789abcdeg");
    fs::create_dir_all(&neighbor).unwrap();
    let mut child = Command::new(&server)
        .arg("60")
        .process_group(0)
        .spawn()
        .unwrap();
    let group = i32::try_from(child.id()).unwrap();
    let ownership = RuntimeOwnership::acquire(&run_dir).unwrap();
    let mut child_ownership = ownership.reserve_child().unwrap();
    child_ownership.child_spawned();
    child_ownership
        .record(
            child.id(),
            group,
            "demo",
            43123,
            Some(Path::new(
                "/Applications/Loxa.app/Contents/MacOS/llama-server",
            )),
            RuntimeLeasePublication::Foreground,
        )
        .unwrap();

    terminate_process_group(&mut child, group).unwrap();
    child_ownership.clear().unwrap();

    assert!(!stage.exists(), "graceful clear leaked the execution stage");
    assert!(neighbor.is_dir(), "cleanup removed an adjacent lookalike");
    assert!(!run_dir.join("foreground.json").exists());
}

#[test]
fn stale_recovery_removes_only_the_exact_orphaned_execution_stage() {
    let dir = tempdir().unwrap();
    let run_dir = dir.path().join("run");
    let (stage, server) = staged_sleep(&run_dir, "fedcba9876543210fedcba9876543210");
    let neighbor = run_dir.join(".bundled-runtime-exec-fedcba9876543210fedcba987654321g");
    fs::create_dir_all(&neighbor).unwrap();
    let mut child = Command::new(&server)
        .arg("60")
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id();
    let snapshot = process_snapshot(pid).unwrap().unwrap();
    let lease = RuntimeLease {
        version: LEASE_VERSION,
        owner_mode: Some(LeaseOwnerMode::Foreground),
        fingerprint: None,
        managed_source: Some(PathBuf::from(
            "/Applications/Loxa.app/Contents/MacOS/llama-server",
        )),
        owner_pid: u32::MAX,
        owner_start_time: 1,
        child_pid: pid,
        child_start_time: snapshot.start_identity,
        child_pgid: i32::try_from(pid).unwrap(),
        server: snapshot.executable,
        model_id: "demo".into(),
        port: 43123,
        service: None,
    };
    fs::create_dir_all(&run_dir).unwrap();
    write_lease_fixture(&run_dir.join("foreground.json"), &lease);

    recover_stale(&run_dir).unwrap();

    let gone = wait_for_child_exit(&mut child);
    if !gone {
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
    }
    assert!(gone, "stale recovery left the exact staged child running");
    assert!(!stage.exists(), "stale recovery leaked the execution stage");
    assert!(neighbor.is_dir(), "cleanup removed an adjacent lookalike");
    assert!(!run_dir.join("foreground.json").exists());
}

#[test]
fn recovery_reconciles_a_prior_300_second_persistent_lease_before_removal() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("foreground.json");
    let witness = dir.path().join("lease-at-termination");
    let ready = dir.path().join("lease-observer.ready");
    let mut child = spawn_lease_observing_sleep(&state_path, &witness, &ready);
    wait_for_path(&ready, "lease-observing child");
    let pid = child.id();
    let group = i32::try_from(pid).unwrap();
    let snapshot = process_snapshot(pid).unwrap().unwrap();
    let mut fingerprint = fingerprint_value();
    fingerprint["sleep_policy"] = serde_json::json!(300);
    let mut lease = lease_value(PERSISTENT_LEASE_VERSION);
    lease["owner_mode"] = serde_json::json!("persistent_app");
    lease["fingerprint"] = fingerprint;
    lease["owner_pid"] = serde_json::json!(u32::MAX);
    lease["owner_start_time"] = serde_json::json!(1);
    lease["child_pid"] = serde_json::json!(pid);
    lease["child_start_time"] = serde_json::json!(snapshot.start_identity);
    lease["child_pgid"] = serde_json::json!(group);
    lease["server"] = serde_json::json!(snapshot.executable);
    fs::write(&state_path, serde_json::to_vec(&lease).unwrap()).unwrap();

    recover_stale(dir.path()).unwrap();

    let child_was_reconciled = wait_for_child_exit(&mut child);
    if !child_was_reconciled {
        terminate_process_group(&mut child, group).unwrap();
    }
    assert!(
        child_was_reconciled,
        "prior-policy llama-server survived lease recovery"
    );
    assert_eq!(
        fs::read(&witness).unwrap(),
        b"present",
        "runtime lease was removed before the exact old process was terminated"
    );
    assert!(!state_path.exists());
}

#[test]
fn general_recovery_stops_an_exact_orphaned_child() {
    let persistent: RuntimeFingerprint = serde_json::from_value(fingerprint_value()).unwrap();
    for (name, version, owner_mode, fingerprint) in [
        ("v1", LEGACY_LEASE_VERSION, None, None),
        (
            "v2 foreground",
            PERSISTENT_LEASE_VERSION,
            Some(LeaseOwnerMode::Foreground),
            None,
        ),
        (
            "v2 persistent",
            PERSISTENT_LEASE_VERSION,
            Some(LeaseOwnerMode::PersistentApp),
            Some(persistent.clone()),
        ),
        (
            "v3 foreground",
            LEASE_VERSION,
            Some(LeaseOwnerMode::Foreground),
            None,
        ),
        (
            "v3 persistent",
            LEASE_VERSION,
            Some(LeaseOwnerMode::PersistentApp),
            Some(persistent),
        ),
    ] {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version,
            owner_mode,
            fingerprint,
            managed_source: None,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity,
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
            service: None,
        };
        write_lease_fixture(&dir.path().join("foreground.json"), &lease);

        recover_stale(dir.path()).unwrap();

        assert!(
            wait_for_child_exit(&mut child),
            "exact {name} orphan survived recovery"
        );
        assert!(!dir.path().join("foreground.json").exists());
    }
}

#[test]
fn signal_cleanup_removes_the_exact_owned_lease_after_group_termination() {
    let dir = tempdir().unwrap();
    let mut child = spawn_sleep();
    let pid = child.id();
    let group = i32::try_from(pid).unwrap();
    let child_snapshot = process_snapshot(pid).unwrap().unwrap();
    let owner_snapshot = process_snapshot(std::process::id()).unwrap().unwrap();
    let lease = RuntimeLease {
        version: LEASE_VERSION,
        owner_mode: Some(LeaseOwnerMode::Foreground),
        fingerprint: None,
        managed_source: None,
        owner_pid: std::process::id(),
        owner_start_time: owner_snapshot.start_identity,
        child_pid: pid,
        child_start_time: child_snapshot.start_identity,
        child_pgid: group,
        server: child_snapshot.executable,
        model_id: "demo".into(),
        port: 1234,
        service: None,
    };
    let state_path = dir.path().join("foreground.json");
    write_lease(&state_path, &lease).unwrap();

    terminate_stale_process_group(group).unwrap();
    clear_terminated_owned_lease(dir.path(), pid, group).unwrap();

    assert!(!state_path.exists());
    let _ = child.wait();
}

#[test]
fn signal_cleanup_preserves_a_foreign_lease() {
    let dir = tempdir().unwrap();
    let mut child = spawn_sleep();
    let pid = child.id();
    let group = i32::try_from(pid).unwrap();
    let child_snapshot = process_snapshot(pid).unwrap().unwrap();
    let lease = RuntimeLease {
        version: LEASE_VERSION,
        owner_mode: Some(LeaseOwnerMode::Foreground),
        fingerprint: None,
        managed_source: None,
        owner_pid: u32::MAX,
        owner_start_time: 1,
        child_pid: pid,
        child_start_time: child_snapshot.start_identity,
        child_pgid: group,
        server: child_snapshot.executable,
        model_id: "demo".into(),
        port: 1234,
        service: None,
    };
    let state_path = dir.path().join("foreground.json");
    write_lease(&state_path, &lease).unwrap();

    terminate_stale_process_group(group).unwrap();
    let error = clear_terminated_owned_lease(dir.path(), pid, group).unwrap_err();

    assert!(
        error.contains("runtime lease changed unexpectedly"),
        "{error}"
    );
    assert!(state_path.exists());
    let _ = child.wait();
}

#[test]
fn acquiring_runtime_never_signals_a_reused_process_identity() {
    for version in [
        LEGACY_LEASE_VERSION,
        PERSISTENT_LEASE_VERSION,
        LEASE_VERSION,
    ] {
        let dir = tempdir().unwrap();
        let mut child = spawn_sleep();
        let pid = child.id();
        let snapshot = process_snapshot(pid).unwrap().unwrap();
        let lease = RuntimeLease {
            version,
            owner_mode: (version != LEGACY_LEASE_VERSION).then_some(LeaseOwnerMode::Foreground),
            fingerprint: None,
            managed_source: None,
            owner_pid: u32::MAX,
            owner_start_time: 1,
            child_pid: pid,
            child_start_time: snapshot.start_identity.saturating_add(1),
            child_pgid: i32::try_from(pid).unwrap(),
            server: snapshot.executable,
            model_id: "demo".into(),
            port: 1234,
            service: None,
        };
        write_lease_fixture(&dir.path().join("foreground.json"), &lease);

        let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();

        assert!(
            process_snapshot(pid).unwrap().is_some(),
            "reused PID in v{version} state was signaled"
        );
        assert!(!dir.path().join("foreground.json").exists());
        terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
        drop(ownership);
    }
}

#[test]
fn legacy_recovery_never_signals_without_unique_command_identity() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let server = dir.path().join("llama-server");
    fs::copy("/bin/sleep", &server).unwrap();
    fs::set_permissions(&server, fs::Permissions::from_mode(0o755)).unwrap();
    let mut command = Command::new(&server);
    command.arg("60").process_group(0);
    let mut child = command.spawn().unwrap();
    let pid = child.id();
    let snapshot = process_snapshot(pid).unwrap().unwrap();
    let legacy = serde_json::json!({
        "schema_version": 4,
        "runs": [{
            "schema_version": 4,
            "run_id": "legacy",
            "model_id": "demo",
            "owner_pid": u32::MAX,
            "owner_process_start_time_unix_s": 1,
            "stop_requested": false,
            "lifecycle": "running",
            "generation": 1,
            "generation_alias": "legacy-g1",
            "control_port": 11436,
            "port": 1234,
            "log_path": "/tmp/legacy.log",
            "child_pid": pid,
            "child_process_start_time_unix_s": snapshot.start_time_seconds,
            "child_pgid": pid
        }]
    });
    fs::write(
        dir.path().join("managed.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    recover_stale(dir.path()).unwrap();

    assert!(
        process_snapshot(pid).unwrap().is_some(),
        "ambiguous legacy process was signaled"
    );
    assert!(!dir.path().join("managed.json").exists());
    terminate_process_group(&mut child, i32::try_from(pid).unwrap()).unwrap();
}

#[test]
fn legacy_command_identity_requires_exact_alias_and_port_pairs() {
    let command = [
        OsString::from("llama-server"),
        OsString::from("--alias"),
        OsString::from("legacy-g1"),
        OsString::from("--port"),
        OsString::from("1234"),
    ];

    assert!(command_has_unique_option(
        &command,
        "--alias",
        OsStr::new("legacy-g1")
    ));
    assert!(command_has_unique_option(
        &command,
        "--port",
        OsStr::new("1234")
    ));
    assert!(!command_has_unique_option(
        &command,
        "--alias",
        OsStr::new("other")
    ));
    let duplicate = [
        command.as_slice(),
        &[OsString::from("--alias"), OsString::from("other")],
    ]
    .concat();
    assert!(!command_has_unique_option(
        &duplicate,
        "--alias",
        OsStr::new("legacy-g1")
    ));
}

#[test]
fn recovery_accepts_a_stale_unloaded_legacy_record() {
    let dir = tempdir().unwrap();
    let legacy = serde_json::json!({
        "schema_version": 4,
        "runs": [{
            "owner_pid": u32::MAX,
            "owner_process_start_time_unix_s": 1,
            "child_pid": null,
            "child_process_start_time_unix_s": null,
            "child_pgid": null
        }]
    });
    fs::write(
        dir.path().join("managed.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    recover_stale(dir.path()).unwrap();

    assert!(!dir.path().join("managed.json").exists());
}

#[test]
fn recovery_blocks_a_second_runtime_while_a_legacy_owner_is_alive() {
    let dir = tempdir().unwrap();
    let owner_pid = std::process::id();
    let owner = process_snapshot(owner_pid).unwrap().unwrap();
    let legacy = serde_json::json!({
        "schema_version": 4,
        "runs": [{
            "owner_pid": owner_pid,
            "owner_process_start_time_unix_s": owner.start_time_seconds,
            "child_pid": null,
            "child_process_start_time_unix_s": null,
            "child_pgid": null
        }]
    });
    fs::write(
        dir.path().join("managed.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();

    let error = recover_stale(dir.path()).unwrap_err();

    assert!(error.contains("legacy llama-server"), "{error}");
    assert!(dir.path().join("managed.json").exists());
}

#[test]
fn stale_cleanup_waits_for_term_resistant_descendants() {
    let dir = tempdir().unwrap();
    let marker = dir.path().join("descendant-ready");
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(format!(
            "(trap '' TERM; printf ready > '{}'; while :; do sleep 1; done) & wait",
            marker.display()
        ))
        .process_group(0);
    let mut child = command.spawn().unwrap();
    let group = i32::try_from(child.id()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !marker.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "descendant did not start");

    terminate_stale_process_group(group).unwrap();

    assert!(!process_group_has_live_members(group).unwrap());
    let _ = child.wait();
}
