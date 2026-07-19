mod support;

use loxa_app_lib::bootstrap::{BootstrapConfig, BootstrapState, Ownership, StartNodeRequest};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use support::{
    EPOCH, FIRST_DOWNLOAD_OPERATION_ID, INSTANCE_ID, LIFECYCLE_OPERATION_ID, NODE_ID,
    REPLACEMENT_EPOCH, SLOT_ID, ScriptedPeer, ScriptedResponse, active_load_operation_collection,
    bootstrap_config, concurrent_active_operation_collection, loading_slot_collection,
    node_collection, slot_collection, successful_state_script, successful_state_script_with_epoch,
    terminal_operation_collection,
};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake loxa executable")
}

fn fixture_named(name: &str) -> PathBuf {
    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "loxa fixture directory with spaces {} {}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let executable = directory.join(name);
    std::fs::copy(fixture(), &executable).unwrap();
    executable
}

fn args_path(executable: &std::path::Path) -> PathBuf {
    let mut path = executable.as_os_str().to_os_string();
    path.push(".args");
    PathBuf::from(path)
}

fn test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    let credential_path = std::env::temp_dir()
        .join(format!("loxa-bootstrap-auth-{}", std::process::id()))
        .join("control.token");
    std::fs::create_dir_all(credential_path.parent().unwrap()).unwrap();
    std::fs::write(
        &credential_path,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            credential_path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        std::fs::set_permissions(&credential_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    BootstrapConfig {
        executable: Some(executable),
        credential_path,
        startup_timeout: Duration::from_secs(2),
        poll_interval: Duration::from_millis(10),
        inherit_debug_stderr: false,
    }
}

fn request(port: u16) -> StartNodeRequest {
    StartNodeRequest {
        endpoint: endpoint(port),
    }
}

fn spawn_fixture(port: u16) -> Child {
    Command::new(fixture())
        .args(["--port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn wait_ready(port: u16) {
    wait_health(port, "ready");
}

fn wait_health(port: u16, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            use std::io::{Read, Write};
            stream
                .write_all(
                    b"GET /loxa/v1/node HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Loxa-Challenge: 0101010101010101010101010101010101010101010101010101010101010101\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            let marker = format!("\"status\":\"{expected}\"");
            if response
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
            {
                return;
            }
        }
        assert!(Instant::now() < deadline, "fixture did not become ready");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_bootstrap_decodes_and_correlates_strict_v2_state_behind_the_proven_peer() {
    let _guard = test_guard();
    let mut script = vec![ScriptedResponse::proof()];
    script.extend(successful_state_script(11));
    let peer = ScriptedPeer::spawn(script);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();
    assert_eq!(state.snapshot().ownership, Ownership::Attached);
    assert!(!state.snapshot().child_running);

    let v2 = state.read_v2_state(Duration::from_secs(1)).unwrap();

    assert_eq!(v2.nodes.nodes[0].node_id.to_string(), NODE_ID);
    assert_eq!(v2.nodes.nodes[0].node_instance_id.to_string(), INSTANCE_ID);
    assert_eq!(v2.nodes.nodes[0].control_endpoint, peer.endpoint);
    assert_eq!(v2.slots.node_id.to_string(), NODE_ID);
    assert_eq!(v2.slots.slots[0].name, "default");
    assert!(v2.operations.operations.is_empty());
    assert!(!state.snapshot().endpoint.contains('@'));
    assert_eq!(state.snapshot().ownership, Ownership::Attached);
    assert!(!state.snapshot().child_running);
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_malformed_v2_keys_ids_decimals_and_size_bounds() {
    let _guard = test_guard();
    let valid = String::from_utf8(node_collection(11)).unwrap();
    let cases = [
        valid.replace(
            "\"schema_version\":2",
            "\"schema_version\":2,\"extra\":true",
        ),
        valid.replace(NODE_ID, "123E4567-E89B-42D3-A456-426614174000"),
        valid.replace("\"revision\":\"11\"", "\"revision\":11"),
        format!("{{\"padding\":\"{}\"}}", "x".repeat(2 * 1024 * 1024)),
    ];
    for malformed in cases {
        let peer = ScriptedPeer::spawn(vec![
            ScriptedResponse::proof(),
            ScriptedResponse::json("/loxa/v2/nodes", malformed),
        ]);
        let mut state = BootstrapState::default();
        state
            .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
            .unwrap();

        let error = state.read_v2_state(Duration::from_secs(1)).unwrap_err();

        assert!(error.contains("unavailable or unsafe"), "{error}");
        peer.finish();
    }
}

#[test]
fn native_bootstrap_rejects_same_endpoint_instance_replacement_after_slot_state() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
        ScriptedResponse::replacement_proof(),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    let error = state.read_v2_state(Duration::from_secs(1)).unwrap_err();

    assert!(error.contains("unavailable or unsafe"), "{error}");
    assert!(
        state
            .read_control_token(&peer.endpoint, Duration::from_millis(10))
            .is_err()
    );
    peer.finish();
}

#[test]
fn native_bootstrap_never_sends_a_bearer_to_a_replacement_before_operations() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            slot_collection(11),
        ),
        ScriptedResponse::replacement_proof(),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    let error = state.read_v2_state(Duration::from_secs(1)).unwrap_err();

    assert!(error.contains("unavailable or unsafe"), "{error}");
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_a_regressing_revision_from_the_same_proven_instance() {
    let _guard = test_guard();
    let mut script = vec![ScriptedResponse::proof()];
    script.extend(successful_state_script(11));
    script.extend([ScriptedResponse::json(
        "/loxa/v2/nodes",
        node_collection(10),
    )]);
    let peer = ScriptedPeer::spawn(script);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();
    state.read_v2_state(Duration::from_secs(1)).unwrap();

    let error = state.read_v2_state(Duration::from_secs(1)).unwrap_err();

    assert!(error.contains("unavailable or unsafe"), "{error}");
    peer.finish();
}

#[test]
fn native_bootstrap_accepts_a_new_epoch_as_a_full_replacement_baseline() {
    let _guard = test_guard();
    let mut script = vec![ScriptedResponse::proof()];
    script.extend(successful_state_script(11));
    script.extend(successful_state_script_with_epoch(1, REPLACEMENT_EPOCH));
    let peer = ScriptedPeer::spawn(script);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();
    state.read_v2_state(Duration::from_secs(1)).unwrap();

    let replaced = state.read_v2_state(Duration::from_secs(1)).unwrap();

    assert_eq!(replaced.nodes.epoch.to_string(), REPLACEMENT_EPOCH);
    assert_eq!(replaced.nodes.revision.to_string(), "1");
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_generation_time_regression_across_collections() {
    let _guard = test_guard();
    let regressing_slots = String::from_utf8(slot_collection(11))
        .unwrap()
        .replace("1784246400600", "1784246400599");
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
        ScriptedResponse::json(format!("/loxa/v2/nodes/{NODE_ID}/slots"), regressing_slots),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_individually_valid_but_incoherent_capacity_one_state() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            slot_collection(11),
        ),
        ScriptedResponse::json("/loxa/v2/operations", active_load_operation_collection(11)),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn native_bootstrap_accepts_many_active_downloads_and_one_correlated_lifecycle_operation() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(20)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            loading_slot_collection(20, LIFECYCLE_OPERATION_ID),
        ),
        ScriptedResponse::json(
            "/loxa/v2/operations",
            concurrent_active_operation_collection(21, 4, 1),
        ),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    let accepted = state.read_v2_state(Duration::from_secs(1)).unwrap();

    assert_eq!(accepted.operations.operations.len(), 5);
    assert_eq!(
        accepted.slots.slots[0].operation_id.unwrap().to_string(),
        LIFECYCLE_OPERATION_ID
    );
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_two_active_lifecycle_operations() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(20)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            loading_slot_collection(20, LIFECYCLE_OPERATION_ID),
        ),
        ScriptedResponse::json(
            "/loxa/v2/operations",
            concurrent_active_operation_collection(20, 2, 2),
        ),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_download_as_the_default_slot_operation() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(20)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            loading_slot_collection(20, FIRST_DOWNLOAD_OPERATION_ID),
        ),
        ScriptedResponse::json(
            "/loxa/v2/operations",
            concurrent_active_operation_collection(21, 4, 1),
        ),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_cross_collection_identity_position_and_shape_mismatches() {
    let _guard = test_guard();
    let valid = String::from_utf8(concurrent_active_operation_collection(20, 2, 1)).unwrap();
    let wrong_node = "123e4567-e89b-42d3-a456-426614174099";
    let wrong_slot = "123e4567-e89b-42d3-a456-426614174098";
    let wrong_epoch = "123e4567-e89b-42d3-a456-426614174097";
    let cases = [
        valid.replacen(NODE_ID, wrong_node, 1),
        valid.replacen(SLOT_ID, wrong_slot, 1),
        valid.replacen(EPOCH, wrong_epoch, 1),
        valid.replace("\"revision\":\"20\"", "\"revision\":\"19\""),
        valid.replacen("\"progress\":null", "\"progress\":null,\"unknown\":true", 1),
    ];
    for operations in cases {
        let peer = ScriptedPeer::spawn(vec![
            ScriptedResponse::proof(),
            ScriptedResponse::json("/loxa/v2/nodes", node_collection(20)),
            ScriptedResponse::json(
                format!("/loxa/v2/nodes/{NODE_ID}/slots"),
                loading_slot_collection(20, LIFECYCLE_OPERATION_ID),
            ),
            ScriptedResponse::json("/loxa/v2/operations", operations),
        ]);
        let mut state = BootstrapState::default();
        state
            .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
            .unwrap();

        assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
        peer.finish();
    }
}

#[test]
fn native_bootstrap_enforces_the_bounded_recent_operation_collection() {
    let _guard = test_guard();
    let peer = ScriptedPeer::spawn(vec![
        ScriptedResponse::proof(),
        ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
        ScriptedResponse::json(
            format!("/loxa/v2/nodes/{NODE_ID}/slots"),
            slot_collection(11),
        ),
        ScriptedResponse::json("/loxa/v2/operations", terminal_operation_collection(129)),
    ]);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn native_bootstrap_rejects_unordered_or_future_operation_rows() {
    let _guard = test_guard();
    let first_id = "123e4567-e89b-42d3-a456-000000000000";
    let second_id = "123e4567-e89b-42d3-a456-000000000001";
    let placeholder = "123e4567-e89b-42d3-a456-ffffffffffff";
    let ordered = String::from_utf8(terminal_operation_collection(2)).unwrap();
    let unordered = ordered
        .replace(first_id, placeholder)
        .replace(second_id, first_id)
        .replace(placeholder, second_id);
    let one = String::from_utf8(terminal_operation_collection(1)).unwrap();
    let cases = [
        unordered,
        one.replace("\"updated_revision\":\"1\"", "\"updated_revision\":\"12\""),
        one.replace(
            "\"updated_at_unix_ms\":\"1\"",
            "\"updated_at_unix_ms\":\"1784246400601\"",
        ),
    ];
    for operations in cases {
        let peer = ScriptedPeer::spawn(vec![
            ScriptedResponse::proof(),
            ScriptedResponse::json("/loxa/v2/nodes", node_collection(11)),
            ScriptedResponse::json(
                format!("/loxa/v2/nodes/{NODE_ID}/slots"),
                slot_collection(11),
            ),
            ScriptedResponse::json("/loxa/v2/operations", operations),
        ]);
        let mut state = BootstrapState::default();
        state
            .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
            .unwrap();

        assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
        peer.finish();
    }
}

#[test]
fn native_bootstrap_rejects_generation_time_regression_across_same_epoch_reads() {
    let _guard = test_guard();
    let mut script = vec![ScriptedResponse::proof()];
    script.extend(successful_state_script(11));
    let regressing_nodes = String::from_utf8(node_collection(11))
        .unwrap()
        .replace("1784246400600", "1784246400599");
    script.push(ScriptedResponse::json("/loxa/v2/nodes", regressing_nodes));
    let peer = ScriptedPeer::spawn(script);
    let mut state = BootstrapState::default();
    state
        .attach_with_config(peer.endpoint.clone(), &bootstrap_config(&peer))
        .unwrap();
    state.read_v2_state(Duration::from_secs(1)).unwrap();

    assert!(state.read_v2_state(Duration::from_secs(1)).is_err());
    peer.finish();
}

#[test]
fn start_attaches_to_an_existing_unloaded_node_without_spawning_a_second_node() {
    let _guard = test_guard();
    let p = port();
    let mut external = Command::new(fixture())
        .args(["--port", &p.to_string(), "--unavailable"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_health(p, "unloaded");

    let mut state = BootstrapState::default();
    let snapshot = state
        .start_with_config(
            request(p),
            &config(PathBuf::from("/must-not-be-spawned/loxa-node")),
        )
        .unwrap();
    assert_eq!(snapshot.ownership, Ownership::Attached);
    assert!(!snapshot.child_running);
    assert!(
        external.try_wait().unwrap().is_none(),
        "existing node exited"
    );

    external.kill().unwrap();
    external.wait().unwrap();
}

#[test]
fn start_rejects_a_spoof_or_old_node_that_cannot_prove_the_user_credential() {
    let _guard = test_guard();
    let p = port();
    let mut external = spawn_fixture(p);
    wait_ready(p);
    let wrong = config(PathBuf::from("/must-not-be-spawned/loxa-node"));
    std::fs::write(
        &wrong.credential_path,
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            &wrong.credential_path,
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    let mut state = BootstrapState::default();
    let error = state.start_with_config(request(p), &wrong).unwrap_err();
    assert!(error.contains("must-not-be-spawned"), "{error}");
    assert_eq!(state.snapshot().ownership, Ownership::None);
    assert!(external.try_wait().unwrap().is_none());
    external.kill().unwrap();
    external.wait().unwrap();
}

#[test]
fn native_bootstrap_ownership_matrix() {
    let _guard = test_guard();
    let missing = PathBuf::from("/definitely missing/loxa executable");
    let mut state = BootstrapState::default();
    let error = state
        .start_with_config(request(port()), &config(missing))
        .unwrap_err();
    assert!(error.contains("executable"), "{error}");

    let p = port();
    let spaced_executable = fixture_named("fake loxa executable with spaces");
    let snapshot = state
        .start_with_config(request(p), &config(spaced_executable.clone()))
        .unwrap();
    assert_eq!(snapshot.ownership, Ownership::Owned);
    let captured = std::fs::read_to_string(args_path(&spaced_executable)).unwrap();
    assert_eq!(
        captured.lines().collect::<Vec<_>>(),
        ["--port", p.to_string().as_str()]
    );
    state.stop_owned().unwrap();
    let _ = std::fs::remove_file(args_path(&spaced_executable));
    let _ = std::fs::remove_file(&spaced_executable);
    let _ = std::fs::remove_dir(spaced_executable.parent().unwrap());

    let timeout_executable = fixture_named("timeout loxa-node");
    let mut quick = config(timeout_executable.clone());
    quick.startup_timeout = Duration::from_millis(100);
    let error = state
        .start_with_config(request(port()), &quick)
        .unwrap_err();
    assert!(error.contains("timed out"), "{error}");
    assert!(
        !state.snapshot().child_running,
        "timeout must reap the child"
    );
    let early_exit_executable = PathBuf::from("/usr/bin/false");
    let error = state
        .start_with_config(request(port()), &config(early_exit_executable.clone()))
        .unwrap_err();
    assert!(error.contains("status"), "{error}");
    assert!(
        !state.snapshot().child_running,
        "early exit must clear ownership"
    );
    let _ = std::fs::remove_file(timeout_executable);

    let attached_port = port();
    let mut external = spawn_fixture(attached_port);
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
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    for _ in 0..10 {
        let p = port();
        let started = state
            .start_with_config(request(p), &config(fixture()))
            .unwrap();
        assert_eq!(started.ownership, Ownership::Owned);
        let stopped = state.stop_owned().unwrap();
        assert_eq!(stopped.ownership, Ownership::None);
        assert!(!stopped.child_running);
        assert!(std::net::TcpStream::connect(("127.0.0.1", p)).is_err());
    }

    let p = port();
    state
        .start_with_config(request(p), &config(fixture()))
        .unwrap();
    state.stop_owned().unwrap();
    let mut replacement = spawn_fixture(p);
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
        .start_with_config(request(p), &config(fixture()))
        .unwrap();
    state.exit_app().unwrap();
    let snapshot = state.snapshot();
    assert_eq!(snapshot.ownership, Ownership::None);
    assert!(!snapshot.child_running);
}

#[test]
fn child_exit_clears_stale_ownership_and_preserves_replacement() {
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    let p = port();
    let started = state
        .start_with_config(
            request(p),
            &config(fixture_named("exit-after-ready loxa-node")),
        )
        .unwrap();
    assert_eq!(started.ownership, Ownership::Owned);
    thread::sleep(Duration::from_millis(1_500));
    let exited = state.snapshot();
    assert_eq!(exited.ownership, Ownership::None);
    assert!(!exited.child_running);

    let mut replacement = spawn_fixture(p);
    wait_ready(p);
    let refreshed = state
        .attach_with_config(endpoint(p), &config(fixture()))
        .unwrap();
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
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    for endpoint in [
        "http://example.com:8080",
        "https://127.0.0.1:8080",
        "http://127.0.0.1/no-port",
        "http://[::1]:8080",
        "http://127.0.0.1:0",
    ] {
        let req = StartNodeRequest {
            endpoint: endpoint.into(),
        };
        assert!(state.start_with_config(req, &config(fixture())).is_err());
    }
}

#[test]
fn rejects_ipv6_loopback_and_port_zero_during_validation() {
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    let initial = state.snapshot();
    let began = Instant::now();
    let ipv6_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: "http://[::1]:8080".into(),
            },
            &config(fixture()),
        )
        .unwrap_err();
    assert!(ipv6_error.contains("IPv4"), "{ipv6_error}");
    let zero_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: "http://127.0.0.1:0".into(),
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
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    let original_port = port();
    let original = state
        .start_with_config(request(original_port), &config(fixture()))
        .unwrap();
    let different_endpoint = endpoint(port());

    let start_error = state
        .start_with_config(
            StartNodeRequest {
                endpoint: different_endpoint.clone(),
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
    let _guard = test_guard();
    let mut state = BootstrapState::default();
    let initial = state.snapshot();

    let invalid = StartNodeRequest {
        endpoint: "http://127.0.0.1:0".into(),
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
            },
            &config(PathBuf::from("/missing/loxa")),
        )
        .unwrap_err();
    let after_spawn = state.snapshot();
    assert_eq!(after_spawn.endpoint, requested_endpoint);
    assert_eq!(after_spawn.ownership, Ownership::None);
    assert_eq!(after_spawn.error.as_deref(), Some(spawn_error.as_str()));
}
