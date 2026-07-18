use crate::runtime::{effective_capabilities, EffectiveCapabilityInputs, PublicationGate};
use loxa_protocol::v2::V2NodeCapabilities;

#[test]
fn effective_capabilities_follow_each_independent_authority_input() {
    let all = EffectiveCapabilityInputs {
        downloader_owner: true,
        slot_load_support: true,
        slot_unload_support: true,
        cancellation_authority: true,
        durable_writer_healthy: true,
        subscription_healthy: true,
    };
    assert_eq!(
        effective_capabilities(all),
        V2NodeCapabilities {
            model_download: true,
            slot_load: true,
            slot_unload: true,
            operation_cancel: true,
            operation_stream: true,
        }
    );
    for (input, expected) in [
        (
            EffectiveCapabilityInputs {
                downloader_owner: false,
                ..all
            },
            V2NodeCapabilities {
                model_download: false,
                ..effective_capabilities(all)
            },
        ),
        (
            EffectiveCapabilityInputs {
                slot_load_support: false,
                ..all
            },
            V2NodeCapabilities {
                slot_load: false,
                ..effective_capabilities(all)
            },
        ),
        (
            EffectiveCapabilityInputs {
                slot_unload_support: false,
                ..all
            },
            V2NodeCapabilities {
                slot_unload: false,
                ..effective_capabilities(all)
            },
        ),
        (
            EffectiveCapabilityInputs {
                cancellation_authority: false,
                ..all
            },
            V2NodeCapabilities {
                operation_cancel: false,
                ..effective_capabilities(all)
            },
        ),
        (
            EffectiveCapabilityInputs {
                durable_writer_healthy: false,
                ..all
            },
            V2NodeCapabilities {
                operation_stream: false,
                ..effective_capabilities(all)
            },
        ),
        (
            EffectiveCapabilityInputs {
                subscription_healthy: false,
                ..all
            },
            V2NodeCapabilities {
                operation_stream: false,
                ..effective_capabilities(all)
            },
        ),
    ] {
        assert_eq!(effective_capabilities(input), expected);
    }
}

#[test]
fn publication_gate_opens_once_and_shutdown_close_is_irreversible() {
    let gate = PublicationGate::default();
    assert!(!gate.is_open());
    assert!(gate.open());
    assert!(gate.is_open());
    assert!(!gate.open());
    gate.close();
    assert!(!gate.is_open());
    assert!(!gate.open(), "a shutdown gate must never reopen");
}

#[test]
fn builder_publishes_running_instance_only_after_durable_composition() {
    let requested_root = std::env::temp_dir().join(format!("l7b-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&requested_root);
    std::fs::create_dir_all(requested_root.join("run/logs")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&requested_root, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(
            requested_root.join("run"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        std::fs::set_permissions(
            requested_root.join("run/logs"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
    let root = std::fs::canonicalize(requested_root).unwrap();
    let paths = crate::NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let runtime = crate::bootstrap::NodeBuilder::new(
        None,
        Some(0),
        loxa_core::engine::RuntimeBackendKind::LlamaCpp,
        &paths,
    )
    .build()
    .unwrap();
    let snapshot = runtime.control_snapshot_for_test();
    let node = snapshot.node.as_ref().expect("instance durably published");
    assert_eq!(node.status, loxa_protocol::v2::V2NodeStatus::Running);
    assert_eq!(
        node.capabilities,
        V2NodeCapabilities {
            model_download: true,
            slot_load: true,
            slot_unload: true,
            operation_cancel: true,
            operation_stream: true,
        }
    );
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "the synchronous runtime test must not be nested in Tokio"
    );
    assert_eq!(
        runtime.shutdown_for_test().unwrap(),
        crate::RunTermination::Interrupted
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn durable_writer_poison_seals_publication_withdraws_inference_and_fails_shutdown() {
    let requested_root = std::env::temp_dir().join(format!(
        "l7-health-{}-{}",
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
    let paths = crate::NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let runtime = crate::bootstrap::NodeBuilder::new(
        None,
        Some(0),
        loxa_core::engine::RuntimeBackendKind::LlamaCpp,
        &paths,
    )
    .build()
    .unwrap();
    let gateway = runtime.gateway_state_for_test();
    gateway.publish(loxa_core::gateway::EngineTarget {
        base_url: "http://127.0.0.1:9".into(),
        backend_alias: "fixture".into(),
        engine: "fixture".into(),
        engine_version: "fixture".into(),
        model_id: "fixture".into(),
        profile: "fixture".into(),
    });

    let poisoned_at = std::time::Instant::now();
    runtime.poison_control_for_test();
    assert!(
        !runtime.publication_gate_is_open_for_test(),
        "route admission must observe writer poison before the monitor polls"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let stop_was_recorded = || {
        matches!(
            loxa_core::supervisor::read_runtime_state(&paths.state_path),
            Ok(loxa_core::supervisor::RuntimeStateRead::Loaded(ref runs))
                if runs.len() == 1 && runs[0].stop_requested
        )
    };
    while runtime.publication_gate_is_open_for_test()
        || gateway.snapshot().is_some()
        || !stop_was_recorded()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "durable health loss did not fail closed"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        poisoned_at.elapsed() < std::time::Duration::from_secs(1),
        "durable health loss must not wait for owner teardown"
    );
    assert!(stop_was_recorded());
    let error = runtime
        .shutdown_for_test()
        .expect_err("durable health loss cannot report graceful shutdown");
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    let _ = std::fs::remove_dir_all(root);
}
