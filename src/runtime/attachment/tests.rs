use super::*;
use crate::runtime::{
    decode_lease, terminate_process_group, RuntimeLeasePublication, RuntimeOwnership,
    LEGACY_LEASE_VERSION,
};
use std::cell::Cell;
use std::fs;
use std::io::{Read as _, Write as _};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

fn fingerprint_value() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "model_id": "demo",
        "effective_context": 4096,
        "effective_profile": "generic",
        "sleep_policy": 300,
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

#[cfg(unix)]
fn generic_attachment_args(models_root: &Path, port: u16) -> Vec<OsString> {
    vec![
        OsString::from("--model"),
        models_root.join("demo/model.gguf").into_os_string(),
        OsString::from("--alias"),
        OsString::from("demo"),
        OsString::from("--host"),
        OsString::from("127.0.0.1"),
        OsString::from("--cors-origins"),
        OsString::from("localhost"),
        OsString::from("--no-ui"),
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--ctx-size"),
        OsString::from("4096"),
        OsString::from("--n-gpu-layers"),
        OsString::from("99"),
        OsString::from("--jinja"),
        OsString::from("--reasoning"),
        OsString::from("off"),
        OsString::from("--sleep-idle-seconds"),
        OsString::from("300"),
    ]
}

#[cfg(unix)]
struct RecordedPersistentRuntime {
    root: TempDir,
    models_root: PathBuf,
    managed_server: PathBuf,
    fingerprint: RuntimeFingerprint,
    port: u16,
    ownership: Option<RuntimeOwnership>,
    child: Child,
    child_pgid: i32,
}

#[cfg(unix)]
impl RecordedPersistentRuntime {
    fn generic(port: u16) -> Self {
        Self::with_args(port, generic_attachment_args)
    }

    fn with_args(port: u16, build_args: impl FnOnce(&Path, u16) -> Vec<OsString>) -> Self {
        let root = tempdir().unwrap();
        let models_root = root.path().join("models");
        let managed_server = PathBuf::from("/usr/bin/yes");
        let fingerprint: RuntimeFingerprint = serde_json::from_value(fingerprint_value()).unwrap();
        let mut command = Command::new(&managed_server);
        command
            .args(build_args(&models_root, port))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().unwrap();
        let child_pgid = i32::try_from(child.id()).unwrap();
        let mut ownership = RuntimeOwnership::acquire(root.path()).unwrap();
        if let Err(error) = ownership.record(
            child.id(),
            child_pgid,
            "demo",
            port,
            RuntimeLeasePublication::PersistentApp(&fingerprint),
        ) {
            let _ = terminate_process_group(&mut child, child_pgid);
            panic!("failed to record persistent test runtime: {error}");
        }
        Self {
            root,
            models_root,
            managed_server,
            fingerprint,
            port,
            ownership: Some(ownership),
            child,
            child_pgid,
        }
    }

    fn run_dir(&self) -> &Path {
        self.root.path()
    }

    fn lease_path(&self) -> PathBuf {
        self.run_dir().join("foreground.json")
    }
}

#[cfg(unix)]
impl Drop for RecordedPersistentRuntime {
    fn drop(&mut self) {
        let _ = terminate_process_group(&mut self.child, self.child_pgid);
        if let Some(mut ownership) = self.ownership.take() {
            let _ = ownership.clear();
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum AttachmentIdentityMutation {
    None,
    OwnerStart,
    ChildStart,
    ProcessGroup,
    Executable,
    ModelPath,
    Host,
    Port,
    Alias,
    Context,
    ExtraArgument,
    MissingSleepPair,
    DuplicateSleepPair,
}

#[cfg(unix)]
fn exact_test_attachment_identity(attached: &AttachedRuntime) -> Result<(), String> {
    test_attachment_identity(attached, AttachmentIdentityMutation::None)
}

#[cfg(unix)]
fn test_attachment_identity(
    attached: &AttachedRuntime,
    mutation: AttachmentIdentityMutation,
) -> Result<(), String> {
    let owner_pid = attached.expected_lease.owner_pid;
    let child_pid = attached.expected_lease.child_pid;
    let child_pgid = attached.expected_lease.child_pgid;
    let managed_server = attached.expected_managed_server.clone();
    let mut command = vec![managed_server.as_os_str().to_owned()];
    command.extend(attached.expected_argv.iter().cloned());
    validate_attachment_identity_with(
        attached,
        move |pid| {
            let mut process = process_snapshot(pid)?;
            let Some(snapshot) = &mut process else {
                return Ok(None);
            };
            if pid == owner_pid && matches!(mutation, AttachmentIdentityMutation::OwnerStart) {
                snapshot.start_identity = snapshot.start_identity.wrapping_add(1);
            }
            if pid == child_pid {
                snapshot.command = command.clone();
                match mutation {
                    AttachmentIdentityMutation::ChildStart => {
                        snapshot.start_identity = snapshot.start_identity.wrapping_add(1);
                    }
                    AttachmentIdentityMutation::Executable => {
                        snapshot.executable = PathBuf::from("/usr/bin/false");
                    }
                    AttachmentIdentityMutation::ModelPath => {
                        replace_option_value(&mut snapshot.command, "--model", "/other.gguf");
                    }
                    AttachmentIdentityMutation::Host => {
                        replace_option_value(&mut snapshot.command, "--host", "0.0.0.0");
                    }
                    AttachmentIdentityMutation::Port => {
                        replace_option_value(&mut snapshot.command, "--port", "43124");
                    }
                    AttachmentIdentityMutation::Alias => {
                        replace_option_value(&mut snapshot.command, "--alias", "other");
                    }
                    AttachmentIdentityMutation::Context => {
                        replace_option_value(&mut snapshot.command, "--ctx-size", "8192");
                    }
                    AttachmentIdentityMutation::ExtraArgument => {
                        snapshot.command.push(OsString::from("--extra"));
                    }
                    AttachmentIdentityMutation::MissingSleepPair => {
                        snapshot.command.truncate(snapshot.command.len() - 2);
                    }
                    AttachmentIdentityMutation::DuplicateSleepPair => {
                        snapshot.command.extend([
                            OsString::from("--sleep-idle-seconds"),
                            OsString::from("300"),
                        ]);
                    }
                    AttachmentIdentityMutation::None
                    | AttachmentIdentityMutation::OwnerStart
                    | AttachmentIdentityMutation::ProcessGroup => {}
                }
            }
            Ok(process)
        },
        move |pid| {
            if pid == child_pid {
                if matches!(mutation, AttachmentIdentityMutation::ProcessGroup) {
                    Ok(child_pgid + 1)
                } else {
                    Ok(child_pgid)
                }
            } else {
                Err("unexpected process-group probe".into())
            }
        },
    )
}

#[cfg(unix)]
fn replace_option_value(command: &mut [OsString], option: &str, replacement: &str) {
    let index = command
        .iter()
        .position(|argument| argument == option)
        .unwrap();
    command[index + 1] = OsString::from(replacement);
}

#[cfg(unix)]
#[test]
fn exact_persistent_runtime_returns_an_opaque_revalidatable_attachment() {
    let fixture = RecordedPersistentRuntime::generic(43123);
    assert!(fixture.lease_path().is_file());

    let lookup = lookup_persistent_runtime_with_probe(
        fixture.run_dir(),
        &fixture.models_root,
        &fixture.managed_server,
        &fixture.fingerprint,
        exact_test_attachment_identity,
        |port, model_id| {
            assert_eq!(port, 43123);
            assert_eq!(model_id, "demo");
            Ok(true)
        },
    );

    let PersistentRuntimeLookup::Attached(attached) = lookup else {
        panic!("exact persistent runtime did not attach")
    };
    assert_eq!(attached.port(), fixture.port);
    assert_eq!(attached.model_id(), "demo");
    exact_test_attachment_identity(&attached).unwrap();
}

#[cfg(unix)]
#[test]
fn every_process_or_complete_argv_identity_failure_makes_zero_http_calls() {
    let fixture = RecordedPersistentRuntime::generic(43123);

    for mutation in [
        AttachmentIdentityMutation::OwnerStart,
        AttachmentIdentityMutation::ChildStart,
        AttachmentIdentityMutation::ProcessGroup,
        AttachmentIdentityMutation::Executable,
        AttachmentIdentityMutation::ModelPath,
        AttachmentIdentityMutation::Host,
        AttachmentIdentityMutation::Port,
        AttachmentIdentityMutation::Alias,
        AttachmentIdentityMutation::Context,
        AttachmentIdentityMutation::ExtraArgument,
        AttachmentIdentityMutation::MissingSleepPair,
        AttachmentIdentityMutation::DuplicateSleepPair,
    ] {
        let calls = Cell::new(0);
        let lookup = lookup_persistent_runtime_with_probe(
            fixture.run_dir(),
            &fixture.models_root,
            &fixture.managed_server,
            &fixture.fingerprint,
            |attached| test_attachment_identity(attached, mutation),
            |_, _| {
                calls.set(calls.get() + 1);
                Ok(true)
            },
        );
        assert!(
            matches!(lookup, PersistentRuntimeLookup::ActiveButNotAttachable),
            "accepted {mutation:?}"
        );
        assert_eq!(calls.get(), 0, "probed HTTP for {mutation:?}");
    }
}

#[cfg(unix)]
#[test]
fn nonpersistent_malformed_external_or_mismatched_state_is_active_without_probing() {
    fn assert_active_without_probes(
        fixture: &RecordedPersistentRuntime,
        managed_server: &Path,
        expected: &RuntimeFingerprint,
    ) {
        let lookup = lookup_persistent_runtime_with_probe(
            fixture.run_dir(),
            &fixture.models_root,
            managed_server,
            expected,
            |_| panic!("prevalidation mismatch reached process identity"),
            |_, _| panic!("prevalidation mismatch reached HTTP"),
        );
        assert!(matches!(
            lookup,
            PersistentRuntimeLookup::ActiveButNotAttachable
        ));
    }

    let foreground = RecordedPersistentRuntime::generic(43123);
    let mut foreground_lease: serde_json::Value =
        serde_json::from_slice(&fs::read(foreground.lease_path()).unwrap()).unwrap();
    foreground_lease["owner_mode"] = serde_json::json!("foreground");
    foreground_lease["fingerprint"] = serde_json::Value::Null;
    fs::write(
        foreground.lease_path(),
        serde_json::to_vec(&foreground_lease).unwrap(),
    )
    .unwrap();
    assert_active_without_probes(
        &foreground,
        &foreground.managed_server,
        &foreground.fingerprint,
    );

    let legacy = RecordedPersistentRuntime::generic(43124);
    let mut legacy_lease: serde_json::Value =
        serde_json::from_slice(&fs::read(legacy.lease_path()).unwrap()).unwrap();
    legacy_lease["version"] = serde_json::json!(LEGACY_LEASE_VERSION);
    legacy_lease.as_object_mut().unwrap().remove("owner_mode");
    legacy_lease.as_object_mut().unwrap().remove("fingerprint");
    fs::write(
        legacy.lease_path(),
        serde_json::to_vec(&legacy_lease).unwrap(),
    )
    .unwrap();
    assert_active_without_probes(&legacy, &legacy.managed_server, &legacy.fingerprint);

    let external = RecordedPersistentRuntime::generic(43125);
    assert_active_without_probes(
        &external,
        Path::new("/usr/bin/false"),
        &external.fingerprint,
    );

    for (name, field, value) in [
        ("model", "model_id", serde_json::json!("other")),
        ("context", "effective_context", serde_json::json!(8192)),
        (
            "profile",
            "effective_profile",
            serde_json::json!("primary_only"),
        ),
        (
            "artifact",
            "primary",
            serde_json::json!({
                "local_filename": "model.gguf",
                "sha256": "b".repeat(64),
                "size": 7,
            }),
        ),
    ] {
        let fixture = RecordedPersistentRuntime::generic(43126);
        let mut expected = fingerprint_value();
        expected[field] = value;
        let expected: RuntimeFingerprint = serde_json::from_value(expected).unwrap();
        let lookup = lookup_persistent_runtime_with_probe(
            fixture.run_dir(),
            &fixture.models_root,
            &fixture.managed_server,
            &expected,
            |_| panic!("{name} mismatch reached process identity"),
            |_, _| panic!("{name} mismatch reached HTTP"),
        );
        assert!(
            matches!(lookup, PersistentRuntimeLookup::ActiveButNotAttachable),
            "accepted mismatched {name}"
        );
    }

    let malformed = RecordedPersistentRuntime::generic(43127);
    fs::write(malformed.lease_path(), b"not JSON").unwrap();
    assert_active_without_probes(
        &malformed,
        &malformed.managed_server,
        &malformed.fingerprint,
    );

    let unsupported = RecordedPersistentRuntime::generic(43128);
    let mut unsupported_lease: serde_json::Value =
        serde_json::from_slice(&fs::read(unsupported.lease_path()).unwrap()).unwrap();
    unsupported_lease["version"] = serde_json::json!(77);
    fs::write(
        unsupported.lease_path(),
        serde_json::to_vec(&unsupported_lease).unwrap(),
    )
    .unwrap();
    assert_active_without_probes(
        &unsupported,
        &unsupported.managed_server,
        &unsupported.fingerprint,
    );
}

#[cfg(unix)]
enum AttachmentHttpReply {
    Bytes(Vec<u8>),
    Stall,
}

#[cfg(unix)]
fn http_response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

#[cfg(unix)]
fn serve_attachment_http_once(
    listener: std::net::TcpListener,
    reply: AttachmentHttpReply,
) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).unwrap();
        let request = std::str::from_utf8(&request[..read]).unwrap();
        assert!(
            request.starts_with("GET /v1/models HTTP/1.1\r\n"),
            "unexpected attachment request: {request:?}"
        );
        match reply {
            AttachmentHttpReply::Bytes(response) => {
                let _ = stream.write_all(&response);
            }
            AttachmentHttpReply::Stall => {
                thread::sleep(Duration::from_millis(1250));
            }
        }
    })
}

#[cfg(unix)]
#[test]
fn bounded_readiness_probe_accepts_only_the_exact_alias_response() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let fixture = RecordedPersistentRuntime::generic(port);
    let server = serve_attachment_http_once(
        listener,
        AttachmentHttpReply::Bytes(http_response(
            "200 OK",
            "Content-Type: application/json\r\n",
            br#"{"data":[{"id":"demo"}]}"#,
        )),
    );

    let lookup = lookup_persistent_runtime_with_probe(
        fixture.run_dir(),
        &fixture.models_root,
        &fixture.managed_server,
        &fixture.fingerprint,
        exact_test_attachment_identity,
        crate::runner::probe_model_alias,
    );

    assert!(matches!(lookup, PersistentRuntimeLookup::Attached(_)));
    server.join().unwrap();
}

#[cfg(unix)]
#[test]
fn readiness_rejects_wrong_alias_redirect_invalid_oversized_and_timeout_responses() {
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let cases = [
        (
            "wrong alias",
            AttachmentHttpReply::Bytes(http_response(
                "200 OK",
                "Content-Type: application/json\r\n",
                br#"{"data":[{"id":"demo-copy"}]}"#,
            )),
        ),
        (
            "redirect",
            AttachmentHttpReply::Bytes(http_response(
                "302 Found",
                "Location: http://127.0.0.1:9/v1/models\r\n",
                b"",
            )),
        ),
        (
            "invalid JSON",
            AttachmentHttpReply::Bytes(http_response(
                "200 OK",
                "Content-Type: application/json\r\n",
                b"not JSON",
            )),
        ),
        (
            "invalid UTF-8",
            AttachmentHttpReply::Bytes(http_response(
                "200 OK",
                "Content-Type: application/json\r\n",
                &[0xff],
            )),
        ),
        (
            "oversized body",
            AttachmentHttpReply::Bytes(http_response(
                "200 OK",
                "Content-Type: application/json\r\n",
                &oversized,
            )),
        ),
        ("timeout", AttachmentHttpReply::Stall),
    ];

    for (name, reply) in cases {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let fixture = RecordedPersistentRuntime::generic(port);
        let server = serve_attachment_http_once(listener, reply);

        let lookup = lookup_persistent_runtime_with_probe(
            fixture.run_dir(),
            &fixture.models_root,
            &fixture.managed_server,
            &fixture.fingerprint,
            exact_test_attachment_identity,
            crate::runner::probe_model_alias,
        );

        assert!(
            matches!(lookup, PersistentRuntimeLookup::ActiveButNotAttachable),
            "accepted {name}"
        );
        server.join().unwrap();
    }
}

#[cfg(unix)]
fn attached_from_fixture(fixture: &RecordedPersistentRuntime) -> AttachedRuntime {
    let lookup = lookup_persistent_runtime_with_probe(
        fixture.run_dir(),
        &fixture.models_root,
        &fixture.managed_server,
        &fixture.fingerprint,
        exact_test_attachment_identity,
        |_, _| Ok(true),
    );
    let PersistentRuntimeLookup::Attached(attached) = lookup else {
        panic!("exact test fixture did not attach")
    };
    attached
}

#[cfg(unix)]
#[test]
fn post_probe_identity_pass_rejects_mid_probe_lease_or_process_replacement() {
    let lease_replaced = RecordedPersistentRuntime::generic(43123);
    let identity_calls = Cell::new(0);
    let lease_path = lease_replaced.lease_path();
    let lease_lookup = lookup_persistent_runtime_with_probe(
        lease_replaced.run_dir(),
        &lease_replaced.models_root,
        &lease_replaced.managed_server,
        &lease_replaced.fingerprint,
        |attached| {
            identity_calls.set(identity_calls.get() + 1);
            exact_test_attachment_identity(attached)
        },
        |_, _| {
            let mut lease: serde_json::Value =
                serde_json::from_slice(&fs::read(&lease_path).unwrap()).unwrap();
            lease["port"] = serde_json::json!(43124);
            fs::write(&lease_path, serde_json::to_vec(&lease).unwrap()).unwrap();
            Ok(true)
        },
    );
    assert!(matches!(
        lease_lookup,
        PersistentRuntimeLookup::ActiveButNotAttachable
    ));
    assert_eq!(identity_calls.get(), 2);

    let mut process_replaced = RecordedPersistentRuntime::generic(43125);
    let run_dir = process_replaced.run_dir().to_path_buf();
    let models_root = process_replaced.models_root.clone();
    let managed_server = process_replaced.managed_server.clone();
    let fingerprint = process_replaced.fingerprint.clone();
    let child_pgid = process_replaced.child_pgid;
    let process_identity_calls = Cell::new(0);
    let process_lookup = lookup_persistent_runtime_with_probe(
        &run_dir,
        &models_root,
        &managed_server,
        &fingerprint,
        |attached| {
            process_identity_calls.set(process_identity_calls.get() + 1);
            exact_test_attachment_identity(attached)
        },
        |_, _| {
            terminate_process_group(&mut process_replaced.child, child_pgid)?;
            Ok(true)
        },
    );
    assert!(matches!(
        process_lookup,
        PersistentRuntimeLookup::ActiveButNotAttachable
    ));
    assert_eq!(process_identity_calls.get(), 2);
}

#[cfg(unix)]
#[test]
fn copied_alias_on_a_rebound_port_fails_identity_before_http() {
    let mut fixture = RecordedPersistentRuntime::generic(43123);
    terminate_process_group(&mut fixture.child, fixture.child_pgid).unwrap();
    let mut replacement = Command::new(&fixture.managed_server);
    replacement
        .args(generic_attachment_args(&fixture.models_root, fixture.port))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let mut replacement = replacement.spawn().unwrap();
    let replacement_group = i32::try_from(replacement.id()).unwrap();
    let http_calls = Cell::new(0);

    let lookup = lookup_persistent_runtime_with_probe(
        fixture.run_dir(),
        &fixture.models_root,
        &fixture.managed_server,
        &fixture.fingerprint,
        exact_test_attachment_identity,
        |_, _| {
            http_calls.set(http_calls.get() + 1);
            Ok(true)
        },
    );

    assert!(matches!(
        lookup,
        PersistentRuntimeLookup::ActiveButNotAttachable
    ));
    assert_eq!(http_calls.get(), 0);
    terminate_process_group(&mut replacement, replacement_group).unwrap();
}

#[cfg(unix)]
#[test]
fn attachment_revalidation_rejects_lease_and_process_replacement() {
    enum LeaseChange {
        Mode,
        Server,
        Port,
        Model,
        Profile,
        Fingerprint,
    }

    for change in [
        LeaseChange::Mode,
        LeaseChange::Server,
        LeaseChange::Port,
        LeaseChange::Model,
        LeaseChange::Profile,
        LeaseChange::Fingerprint,
    ] {
        let fixture = RecordedPersistentRuntime::generic(43123);
        let attached = attached_from_fixture(&fixture);
        let mut lease: serde_json::Value =
            serde_json::from_slice(&fs::read(fixture.lease_path()).unwrap()).unwrap();
        match change {
            LeaseChange::Mode => {
                lease["owner_mode"] = serde_json::json!("foreground");
                lease["fingerprint"] = serde_json::Value::Null;
            }
            LeaseChange::Server => {
                lease["server"] = serde_json::json!("/usr/bin/false");
            }
            LeaseChange::Port => lease["port"] = serde_json::json!(43124),
            LeaseChange::Model => {
                lease["model_id"] = serde_json::json!("other");
                lease["fingerprint"]["model_id"] = serde_json::json!("other");
            }
            LeaseChange::Profile => {
                lease["fingerprint"]["effective_profile"] = serde_json::json!("primary_only");
            }
            LeaseChange::Fingerprint => {
                lease["fingerprint"]["primary"]["sha256"] = serde_json::json!("b".repeat(64));
            }
        }
        fs::write(fixture.lease_path(), serde_json::to_vec(&lease).unwrap()).unwrap();
        assert!(attached.revalidate().is_err());
    }

    let mut process_replaced = RecordedPersistentRuntime::generic(43125);
    let attached = attached_from_fixture(&process_replaced);
    let child_pgid = process_replaced.child_pgid;
    terminate_process_group(&mut process_replaced.child, child_pgid).unwrap();
    assert!(attached.revalidate().is_err());

    let argv_replaced = RecordedPersistentRuntime::generic(43126);
    let attached = attached_from_fixture(&argv_replaced);
    for mutation in [
        AttachmentIdentityMutation::ProcessGroup,
        AttachmentIdentityMutation::ModelPath,
        AttachmentIdentityMutation::Host,
        AttachmentIdentityMutation::Port,
        AttachmentIdentityMutation::Alias,
        AttachmentIdentityMutation::Context,
        AttachmentIdentityMutation::ExtraArgument,
        AttachmentIdentityMutation::MissingSleepPair,
        AttachmentIdentityMutation::DuplicateSleepPair,
    ] {
        assert!(
            test_attachment_identity(&attached, mutation).is_err(),
            "revalidation accepted {mutation:?}"
        );
    }
}

#[test]
fn persistent_lookup_reports_no_runtime_only_without_a_held_lock_or_lease() {
    let dir = tempdir().unwrap();
    let models = dir.path().join("models");
    let managed_server = Path::new("/usr/bin/yes");
    let expected: RuntimeFingerprint = serde_json::from_value(fingerprint_value()).unwrap();

    assert!(matches!(
        lookup_persistent_runtime(dir.path(), &models, managed_server, &expected),
        PersistentRuntimeLookup::NoRuntime
    ));

    let ownership = RuntimeOwnership::acquire(dir.path()).unwrap();
    assert!(matches!(
        lookup_persistent_runtime(dir.path(), &models, managed_server, &expected),
        PersistentRuntimeLookup::ActiveButNotAttachable
    ));
    drop(ownership);

    fs::write(dir.path().join("foreground.json"), b"not JSON").unwrap();
    assert!(matches!(
        lookup_persistent_runtime(dir.path(), &models, managed_server, &expected),
        PersistentRuntimeLookup::ActiveButNotAttachable
    ));
}

#[test]
fn attachment_lease_expectation_independently_requires_mode_and_whole_fingerprint() {
    let expected_fingerprint: RuntimeFingerprint =
        serde_json::from_value(fingerprint_value()).unwrap();
    let mut value = lease_value(LEASE_VERSION);
    value["owner_mode"] = serde_json::json!("persistent_app");
    value["fingerprint"] = fingerprint_value();
    value["server"] = serde_json::json!("/usr/bin/llama-server");
    let exact = decode_lease(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(lease_matches_attachment_expectation(
        &exact,
        Path::new("/usr/bin/llama-server"),
        &expected_fingerprint,
    ));

    let mut wrong_mode = exact.clone();
    wrong_mode.owner_mode = Some(LeaseOwnerMode::Foreground);
    assert!(!lease_matches_attachment_expectation(
        &wrong_mode,
        Path::new("/usr/bin/llama-server"),
        &expected_fingerprint,
    ));

    let mut changed_fingerprint = fingerprint_value();
    changed_fingerprint["effective_context"] = serde_json::json!(8192);
    let changed_fingerprint: RuntimeFingerprint =
        serde_json::from_value(changed_fingerprint).unwrap();
    let mut wrong_fingerprint = exact.clone();
    wrong_fingerprint.fingerprint = Some(changed_fingerprint);
    assert!(!lease_matches_attachment_expectation(
        &wrong_fingerprint,
        Path::new("/usr/bin/llama-server"),
        &expected_fingerprint,
    ));

    let mut wrong_version = exact.clone();
    wrong_version.version = LEGACY_LEASE_VERSION;
    assert!(!lease_matches_attachment_expectation(
        &wrong_version,
        Path::new("/usr/bin/llama-server"),
        &expected_fingerprint,
    ));
    assert!(!lease_matches_attachment_expectation(
        &exact,
        Path::new("/usr/bin/other"),
        &expected_fingerprint,
    ));
}
