use crate::runtime::{
    shutdown_failure_rank, FatalShutdown, FatalShutdownParts, InjectedRetainedOwner,
    PublicationGate, ShutdownDeadlines, ShutdownFailureClass,
};
use crate::{ManagedRunsSnapshot, NodePaths, RunTermination, ShutdownResult};
use loxa_core::engine::RuntimeBackendKind;
use std::time::{Duration, Instant};

fn runtime_paths(label: &str) -> (std::path::PathBuf, NodePaths) {
    let requested_root = std::env::temp_dir().join(format!(
        "slice4-runtime-{label}-{}-{}",
        std::process::id(),
        loxa_protocol::NodeInstanceId::new_v4()
    ));
    std::fs::create_dir_all(requested_root.join("run/logs")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [
            requested_root.as_path(),
            requested_root.join("run").as_path(),
            requested_root.join("run/logs").as_path(),
        ] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    let root = std::fs::canonicalize(requested_root).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    (root, paths)
}

#[test]
fn shutdown_deadlines_are_absolute_offsets_from_one_start() {
    let started = Instant::now();
    let deadlines = ShutdownDeadlines::from_started(started);

    assert_eq!(deadlines.admission, started + Duration::from_secs(2));
    assert_eq!(deadlines.signal, started + Duration::from_secs(3));
    assert_eq!(deadlines.verification, started + Duration::from_secs(6));
    assert_eq!(deadlines.download, started + Duration::from_secs(10));
    assert_eq!(deadlines.lifecycle, started + Duration::from_secs(18));
    assert_eq!(deadlines.repository, started + Duration::from_secs(20));
}

#[test]
fn shutdown_failure_precedence_is_independent_of_stop_order() {
    let mut observed = [
        (ShutdownFailureClass::OrdinaryCancellation, "ordinary"),
        (ShutdownFailureClass::Routes, "routes"),
        (ShutdownFailureClass::Verification, "verification"),
        (ShutdownFailureClass::Download, "download"),
        (ShutdownFailureClass::Lifecycle, "lifecycle"),
        (ShutdownFailureClass::DurableRepository, "repository"),
        (ShutdownFailureClass::Artifact, "artifact"),
        (ShutdownFailureClass::ExactChild, "exact child"),
    ];
    observed.sort_by_key(|(class, _)| shutdown_failure_rank(*class));

    assert_eq!(
        observed.map(|(_, diagnostic)| diagnostic),
        [
            "exact child",
            "artifact",
            "repository",
            "lifecycle",
            "download",
            "verification",
            "routes",
            "ordinary",
        ],
        "the primary must be first while every secondary diagnostic is preserved"
    );
}

#[test]
fn successful_shutdown_closes_admission_and_leaves_no_managed_owner_or_listener() {
    let (root, paths) = runtime_paths("clean");
    let runtime =
        crate::bootstrap::NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
            .build()
            .unwrap();
    let port = runtime.port();

    assert!(runtime.publication_gate_is_open_for_test());
    let snapshot = runtime.control_snapshot_for_test();
    assert_eq!(
        snapshot.node.as_ref().map(|node| node.status),
        Some(loxa_protocol::v2::V2NodeStatus::Running),
        "publication may open only after the durable Running observation exists"
    );

    assert!(matches!(
        runtime.shutdown_for_test(),
        ShutdownResult::Stopped(RunTermination::Interrupted)
    ));
    assert_eq!(
        crate::managed_servers(&paths).unwrap(),
        ManagedRunsSnapshot::Runs(Vec::new()),
        "successful shutdown must remove the exact managed owner"
    );
    assert!(
        std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(100),
        )
        .is_err(),
        "successful shutdown must not orphan the gateway listener"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn durable_control_failure_closes_admission_and_returns_retained_fatal_bundle() {
    let (root, paths) = runtime_paths("control-fatal");
    let runtime =
        crate::bootstrap::NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
            .build()
            .unwrap();

    runtime.poison_control_for_test();
    let deadline = Instant::now() + Duration::from_secs(2);
    while runtime.publication_gate_is_open_for_test() {
        assert!(
            Instant::now() < deadline,
            "durable authority loss did not seal admission"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let fatal = match runtime.shutdown_for_test() {
        ShutdownResult::RequiresProcessExit(fatal) => fatal,
        ShutdownResult::Stopped(_) => panic!("poisoned durable owner stopped successfully"),
        ShutdownResult::Failed(error) => {
            panic!("poisoned durable owner was released as an ordinary error: {error}")
        }
    };
    assert!(
        fatal.diagnostic_for_test().contains("durable control"),
        "fatal diagnostic must retain the durable-owner failure: {}",
        fatal.diagnostic_for_test()
    );
    std::mem::forget(fatal);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn every_runtime_owner_class_moves_into_the_fatal_bundle() {
    const CHILD_ENV: &str = "LOXA_TEST_RETAINED_OWNER_CLASS";
    const PORT_FILE_ENV: &str = "LOXA_TEST_RETAINED_OWNER_PORT_FILE";
    if let Some(label) = std::env::var_os(CHILD_ENV) {
        let label = label.to_string_lossy();
        let (injected, expected) = match label.as_ref() {
            "gateway" => (InjectedRetainedOwner::Gateway, ShutdownFailureClass::Routes),
            "routes" => (InjectedRetainedOwner::Routes, ShutdownFailureClass::Routes),
            "history" => (
                InjectedRetainedOwner::History,
                ShutdownFailureClass::DurableRepository,
            ),
            "health" => (InjectedRetainedOwner::Health, ShutdownFailureClass::Routes),
            "execution" => (
                InjectedRetainedOwner::Execution,
                ShutdownFailureClass::ExactChild,
            ),
            "control" => (
                InjectedRetainedOwner::Control,
                ShutdownFailureClass::DurableRepository,
            ),
            "exact-owner" => (
                InjectedRetainedOwner::ExactOwner,
                ShutdownFailureClass::ExactChild,
            ),
            _ => panic!("unknown injected owner class"),
        };
        let (_root, paths) = runtime_paths(&label);
        let runtime =
            crate::bootstrap::NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
                .build()
                .unwrap();
        if let Some(port_file) = std::env::var_os(PORT_FILE_ENV) {
            std::fs::write(port_file, runtime.port().to_string()).unwrap();
        }
        let fatal = match runtime.shutdown_with_injected_retained_for_test(injected) {
            ShutdownResult::RequiresProcessExit(fatal) => fatal,
            _ => panic!("injected owner must require process exit"),
        };
        assert!(
            fatal.retained_classes_for_test().contains(&expected),
            "{label} did not retain its real production owner class: {:?}",
            fatal.retained_classes_for_test()
        );
        (*fatal).exit(9);
    }

    for label in [
        "gateway",
        "routes",
        "history",
        "health",
        "execution",
        "control",
        "exact-owner",
    ] {
        let port_file = std::env::temp_dir().join(format!(
            "loxa-retained-owner-port-{label}-{}-{}",
            std::process::id(),
            loxa_protocol::NodeInstanceId::new_v4()
        ));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("slice4_test_support::runtime::every_runtime_owner_class_moves_into_the_fatal_bundle")
            .arg("--nocapture")
            .env(CHILD_ENV, label)
            .env(PORT_FILE_ENV, &port_file)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(9), "retained owner class {label}");
        let port: u16 = std::fs::read_to_string(&port_file)
            .unwrap()
            .parse()
            .unwrap();
        std::fs::remove_file(port_file).unwrap();
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .unwrap_or_else(|error| panic!("{label} orphaned gateway listener {port}: {error}"));
    }
}

fn empty_fatal(publication: PublicationGate) -> FatalShutdown {
    FatalShutdown::new(FatalShutdownParts {
        diagnostic: "injected fatal shutdown".into(),
        gateway: None,
        gateway_failure: None,
        history: None,
        history_failure: None,
        health: None,
        health_failure: None,
        execution: None,
        control_failure: None,
        control_startup_failure: None,
        control: None,
        unloaded_run: None,
        publication: Some(publication),
        owner: None,
        owner_failure: None,
        routes: None,
        routes_failure: None,
        gateway_state: None,
    })
}

#[test]
fn fatal_shutdown_accidental_drop_retains_capabilities() {
    const CHILD_ENV: &str = "LOXA_TEST_FATAL_SHUTDOWN_DROP";
    if std::env::var_os(CHILD_ENV).is_some() {
        drop(empty_fatal(PublicationGate::default()));
        unreachable!("fatal Drop must not return");
    }
    assert_fatal_child_nonzero(
        CHILD_ENV,
        "fatal_shutdown_accidental_drop_retains_capabilities",
    );
}

#[test]
fn fatal_shutdown_preserves_primary_and_secondary_diagnostics() {
    let fatal = FatalShutdown::new(FatalShutdownParts {
        diagnostic: "exact child failed; durable repository failed; routes failed".into(),
        gateway: None,
        gateway_failure: None,
        history: None,
        history_failure: None,
        health: None,
        health_failure: None,
        execution: None,
        control_failure: None,
        control_startup_failure: None,
        control: None,
        unloaded_run: None,
        publication: None,
        owner: None,
        owner_failure: None,
        routes: None,
        routes_failure: None,
        gateway_state: None,
    });

    assert_eq!(
        fatal.diagnostic_for_test(),
        "exact child failed; durable repository failed; routes failed"
    );
    std::mem::forget(fatal);
}

#[test]
fn fatal_shutdown_unwind_retains_capabilities() {
    const CHILD_ENV: &str = "LOXA_TEST_FATAL_SHUTDOWN_UNWIND";
    if std::env::var_os(CHILD_ENV).is_some() {
        let _fatal = empty_fatal(PublicationGate::default());
        panic!("injected unwind")
    }
    assert_fatal_child_nonzero(CHILD_ENV, "fatal_shutdown_unwind_retains_capabilities");
}

fn assert_fatal_child_nonzero(environment: &str, test_name: &str) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(format!("slice4_test_support::runtime::{test_name}"))
        .arg("--nocapture")
        .env(environment, "1")
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success());
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("fatal shutdown subprocess did not exit within its bound");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn fatal_shutdown_exit_is_nonzero_and_bounded() {
    const CHILD_ENV: &str = "LOXA_TEST_FATAL_SHUTDOWN_EXIT";
    if std::env::var_os(CHILD_ENV).is_some() {
        empty_fatal(PublicationGate::default()).exit(7);
    }

    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("slice4_test_support::runtime::fatal_shutdown_exit_is_nonzero_and_bounded")
        .arg("--nocapture")
        .env(CHILD_ENV, "1");
    #[cfg(unix)]
    let _undrained_stderr = {
        use std::io::Write as _;
        use std::os::fd::OwnedFd;
        let (reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        let bytes = [b'x'; 4096];
        loop {
            match writer.write(&bytes) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("saturating child stderr failed: {error}"),
            }
        }
        writer.set_nonblocking(false).unwrap();
        let descriptor: OwnedFd = writer.into();
        command.stderr(std::process::Stdio::from(descriptor));
        reader
    };
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("fatal shutdown subprocess did not exit within its bound");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(status.code(), Some(7));
}

#[test]
fn unresolved_node_runtime_drop_exits_nonzero_and_bounded() {
    const CHILD_ENV: &str = "LOXA_TEST_NODE_RUNTIME_DROP";
    if std::env::var_os(CHILD_ENV).is_some() {
        let (_root, paths) = runtime_paths("node-runtime-drop");
        let runtime =
            crate::bootstrap::NodeBuilder::new(None, Some(0), RuntimeBackendKind::LlamaCpp, &paths)
                .build()
                .unwrap();
        runtime.poison_control_for_test();
        while runtime.publication_gate_is_open_for_test() {
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(runtime);
        unreachable!("unresolved NodeRuntime Drop must not return");
    }
    assert_fatal_child_nonzero(
        CHILD_ENV,
        "unresolved_node_runtime_drop_exits_nonzero_and_bounded",
    );
}
