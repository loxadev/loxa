use super::*;
use crate::catalog::Manifest;
use crate::chat::Event;
use crate::cli::RuntimeArgs;
use crate::paths::AppPaths;
use crate::runtime::AttachedRuntime;
use crate::runtime::PersistentRuntimeLookup;
use std::cell::Cell;
use std::io::{Read, Write};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn manifest() -> Manifest {
    Manifest {
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
    }
}

#[test]
fn history_snapshot_precedes_the_literal_pre_worker_revalidation() {
    let source = include_str!("../../session.rs");
    assert!(source.contains(concat!(
        "        let messages = session.request(&user);\n",
        "        let request_started = Instant::now();\n",
        "        if let Some(code) = runtime.poll()? {\n",
        "            return Ok(code);\n",
        "        }\n\n",
        "        let worker = start_worker(model.to_owned(), messages, max_tokens)?;"
    )));
}

fn no_overrides() -> RuntimeArgs {
    RuntimeArgs {
        ctx: None,
        port: None,
        server: None,
    }
}

#[test]
fn attached_poll_revalidates_and_terminate_owned_is_a_strict_noop() {
    let revalidations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&revalidations);
    let attached = AttachedRuntime::for_session_test("demo", 43123, move || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let mut runtime = ChatRuntime::attached(attached);

    assert_eq!(runtime.model_id(), "demo");
    assert_eq!(runtime.port().unwrap(), 43123);
    assert_eq!(runtime.poll().unwrap(), None);
    assert_eq!(runtime.poll().unwrap(), None);
    assert_eq!(revalidations.load(Ordering::SeqCst), 2);

    runtime.terminate_owned().unwrap();
    runtime.terminate_owned().unwrap();
    assert_eq!(revalidations.load(Ordering::SeqCst), 2);
}

#[test]
fn attached_poll_maps_hostile_identity_diagnostics_to_one_static_error() {
    let attached = AttachedRuntime::for_session_test("demo", 43123, || {
        Err("HOSTILE_TOKEN /private/runtime/foreground.json".into())
    });
    let mut runtime = ChatRuntime::attached(attached);

    assert_eq!(runtime.poll().unwrap_err(), ATTACHED_RUNTIME_UNAVAILABLE);
}

#[test]
fn installed_chat_routes_one_exact_lookup_directly_to_attachment() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let manifest = manifest();
    let model_dir = paths.model_dir(&manifest.id).unwrap();
    std::fs::create_dir_all(&model_dir).unwrap();
    let _busy = crate::catalog::ModelLock::acquire(&model_dir).unwrap();
    let receipt = model_dir.join("verification-receipt.json");
    std::fs::write(&receipt, b"receipt must remain byte-exact").unwrap();
    let attachment_lookups = Cell::new(0);
    let presence_lookups = Cell::new(0);

    let route = route_chat_with(
        Some(&manifest),
        &no_overrides(),
        &paths,
        |run_dir, models_root, managed_server, candidates| {
            attachment_lookups.set(attachment_lookups.get() + 1);
            assert_eq!(run_dir, paths.run);
            assert_eq!(models_root, paths.models);
            assert_eq!(managed_server, paths.managed_server);
            assert_eq!(candidates.len(), 1);
            PersistentRuntimeLookup::Attached(AttachedRuntime::for_session_test(
                "demo",
                43123,
                || Ok(()),
            ))
        },
        |_| {
            presence_lookups.set(presence_lookups.get() + 1);
            panic!("installed routing must not perform a presence lookup")
        },
    )
    .unwrap();

    let ChatRoute::Attached(attached) = route else {
        panic!("exact persistent runtime must attach")
    };
    assert_eq!(attached.model_id(), "demo");
    assert_eq!(attached.port(), 43123);
    assert_eq!(attachment_lookups.get(), 1);
    assert_eq!(presence_lookups.get(), 0);
    assert_eq!(
        std::fs::read(&receipt).unwrap(),
        b"receipt must remain byte-exact"
    );
    assert!(!model_dir.join("model.gguf").exists());
    assert!(!paths.managed_server.exists());
}

#[test]
fn every_explicit_runtime_override_conflicts_after_one_exact_attachment_lookup() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let manifest = manifest();
    let overrides = [
        RuntimeArgs {
            ctx: Some(4096),
            ..no_overrides()
        },
        RuntimeArgs {
            port: Some(0),
            ..no_overrides()
        },
        RuntimeArgs {
            server: Some(paths.managed_server.clone()),
            ..no_overrides()
        },
    ];

    for runtime in overrides {
        let lookups = Cell::new(0);
        let error = route_chat_with(
            Some(&manifest),
            &runtime,
            &paths,
            |_, _, _, _| {
                lookups.set(lookups.get() + 1);
                PersistentRuntimeLookup::Attached(AttachedRuntime::for_session_test(
                    "demo",
                    43123,
                    || Ok(()),
                ))
            },
            |_| panic!("installed routing must not use presence-only lookup"),
        )
        .err()
        .expect("explicit runtime override must conflict");

        assert_eq!(error, CHAT_OVERRIDE_CONFLICT);
        assert_eq!(lookups.get(), 1);
    }
}

#[test]
fn installed_and_local_active_states_share_one_static_conflict() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let manifest = manifest();

    let installed = route_chat_with(
        Some(&manifest),
        &no_overrides(),
        &paths,
        |_, _, _, _| PersistentRuntimeLookup::ActiveButNotAttachable,
        |_| panic!("installed routing must not use presence-only lookup"),
    )
    .err()
    .expect("active installed runtime must conflict");
    let local = route_chat_with(
        None,
        &no_overrides(),
        &paths,
        |_, _, _, _| panic!("local routing must not use fingerprint lookup"),
        |run_dir| {
            assert_eq!(run_dir, paths.run);
            crate::runtime::RuntimePresence::Active
        },
    )
    .err()
    .expect("active local runtime must conflict");

    assert_eq!(installed, CHAT_ACTIVE_RUNTIME_CONFLICT);
    assert_eq!(local, CHAT_ACTIVE_RUNTIME_CONFLICT);
}

#[test]
fn hostile_expected_fingerprint_diagnostics_map_to_static_chat_configuration_text() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    std::fs::write(&paths.config, b"HOSTILE_TOKEN /private/config/path").unwrap();
    let lookups = Cell::new(0);

    let error = route_chat_with(
        Some(&manifest()),
        &no_overrides(),
        &paths,
        |_, _, _, _| {
            lookups.set(lookups.get() + 1);
            PersistentRuntimeLookup::NoRuntime
        },
        |_| panic!("installed routing must not use presence-only lookup"),
    )
    .err()
    .expect("invalid managed chat configuration must fail closed");

    assert_eq!(error, CHAT_CONFIGURATION_INVALID);
    assert_eq!(lookups.get(), 0);
    assert!(!error.contains("HOSTILE_TOKEN"));
    assert!(!error.contains("/private"));
}

#[test]
fn only_no_runtime_preserves_the_foreground_route_after_one_lookup() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let manifest = manifest();
    let installed_lookups = Cell::new(0);
    let local_lookups = Cell::new(0);

    let installed = route_chat_with(
        Some(&manifest),
        &no_overrides(),
        &paths,
        |_, _, _, _| {
            installed_lookups.set(installed_lookups.get() + 1);
            PersistentRuntimeLookup::NoRuntime
        },
        |_| panic!("installed routing must not use presence-only lookup"),
    )
    .unwrap();
    let local = route_chat_with(
        None,
        &no_overrides(),
        &paths,
        |_, _, _, _| panic!("local routing must not use fingerprint lookup"),
        |_| {
            local_lookups.set(local_lookups.get() + 1);
            crate::runtime::RuntimePresence::NoRuntime
        },
    )
    .unwrap();

    assert!(matches!(installed, ChatRoute::Foreground));
    assert!(matches!(local, ChatRoute::Foreground));
    assert_eq!(installed_lookups.get(), 1);
    assert_eq!(local_lookups.get(), 1);
}

#[cfg(unix)]
#[test]
fn post_no_runtime_lock_race_fails_before_spawn_without_relookup() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let manifest = manifest();
    let lookups = Cell::new(0);
    let route = route_chat_with(
        Some(&manifest),
        &no_overrides(),
        &paths,
        |_, _, _, _| {
            lookups.set(lookups.get() + 1);
            PersistentRuntimeLookup::NoRuntime
        },
        |_| panic!("installed routing must not use presence-only lookup"),
    )
    .unwrap();
    assert!(matches!(route, ChatRoute::Foreground));

    let spawn_witness = root.path().join("spawned");
    let server = root.path().join("server");
    std::fs::write(
        &server,
        format!("#!/bin/sh\ntouch '{}'\nexit 99\n", spawn_witness.display()),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&server).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&server, permissions).unwrap();
    let launch = crate::runner::Launch {
        server,
        managed_runtime: None,
        model: root.path().join("missing-model.gguf"),
        id: "demo".into(),
        requested_port: 0,
        ctx: 4096,
        profile: crate::runner::LaunchProfile::generic(),
        policy: crate::runner::LaunchPolicy::Foreground,
    };
    let _persistent_winner = match crate::runtime::RuntimeOwnership::acquire_persistent(&paths.run)
    {
        Ok(ownership) => ownership,
        Err(_) => panic!("persistent winner acquires the empty runtime boundary"),
    };

    let error = crate::runner::start_foreground(&launch, &paths.run)
        .err()
        .expect("foreground start must lose the post-lookup lock race");
    assert_eq!(error, "another Loxa runtime is active");
    assert_eq!(lookups.get(), 1);
    assert!(!spawn_witness.exists());
}

#[test]
fn owned_runtime_delegates_poll_and_teardown_to_the_incumbent_server_boundary() {
    let polls = Arc::new(AtomicUsize::new(0));
    let terminations = Arc::new(AtomicUsize::new(0));
    let poll_calls = Arc::clone(&polls);
    let termination_calls = Arc::clone(&terminations);
    let mut runtime = ChatRuntime::owned_for_test(
        "demo",
        43123,
        move || {
            poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(37))
        },
        move || {
            termination_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );

    assert_eq!(runtime.model_id(), "demo");
    assert_eq!(runtime.port().unwrap(), 43123);
    assert_eq!(runtime.poll().unwrap(), Some(37));
    runtime.terminate_owned().unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(terminations.load(Ordering::SeqCst), 1);
}

#[test]
fn every_chat_outcome_crosses_one_ownership_aware_teardown_boundary() {
    let outcomes = [
        Ok(0),
        Ok(130),
        Err("terminal input failed"),
        Err("worker join failed"),
    ];

    for expected in outcomes {
        let terminations = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&terminations);
        let runtime = ChatRuntime::owned_for_test(
            "demo",
            43123,
            || Ok(None),
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        let outcome = expected.map_err(str::to_owned);

        assert_eq!(with_runtime_teardown(runtime, |_| outcome.clone()), outcome);
        assert_eq!(terminations.load(Ordering::SeqCst), 1);
    }

    let owner_root = tempfile::tempdir().unwrap();
    let owner_state = owner_root.path().join("foreground.json");
    std::fs::write(&owner_state, b"menu owner lease").unwrap();
    let attached_owner_controls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&attached_owner_controls);
    let controlled_state = owner_state.clone();
    let attached = AttachedRuntime::for_session_test_with_owner_witness(
        "demo",
        43123,
        || Ok(()),
        move || {
            witness.fetch_add(1, Ordering::SeqCst);
            std::fs::write(&controlled_state, b"owner was controlled")
                .map_err(|error| error.to_string())?;
            Ok(())
        },
    );
    attached.control_owner_for_session_test().unwrap();
    assert_eq!(attached_owner_controls.load(Ordering::SeqCst), 1);
    attached_owner_controls.store(0, Ordering::SeqCst);
    std::fs::write(&owner_state, b"menu owner lease").unwrap();
    let attached = ChatRuntime::attached(attached);
    assert_eq!(
        with_runtime_teardown(attached, |_| Err("output failed".into())),
        Err("output failed".into())
    );
    assert_eq!(attached_owner_controls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(&owner_state).unwrap(), b"menu owner lease");
}

#[test]
fn attached_revalidates_before_input_before_worker_and_during_each_request_poll() {
    let before_input_calls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&before_input_calls);
    let input_calls = Cell::new(0);
    let runtime = ChatRuntime::attached(AttachedRuntime::for_session_test(
        "demo",
        43123,
        move || {
            witness.fetch_add(1, Ordering::SeqCst);
            Err("hostile before-input diagnostic".into())
        },
    ));
    let error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_input(runtime, 1, || {
            input_calls.set(input_calls.get() + 1);
            crate::session::InputEvent::Interrupted
        })
    })
    .unwrap_err();
    assert_eq!(error, ATTACHED_RUNTIME_UNAVAILABLE);
    assert_eq!(before_input_calls.load(Ordering::SeqCst), 1);
    assert_eq!(input_calls.get(), 0);

    let before_worker_calls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&before_worker_calls);
    let runtime = ChatRuntime::attached(AttachedRuntime::for_session_test(
        "demo",
        43123,
        move || {
            let call = witness.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 2 {
                Err("hostile before-worker diagnostic".into())
            } else {
                Ok(())
            }
        },
    ));
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_input(runtime, 1, || {
            input.take().expect("only one prompt is read")
        })
    })
    .unwrap_err();
    assert_eq!(error, ATTACHED_RUNTIME_UNAVAILABLE);
    assert_eq!(before_worker_calls.load(Ordering::SeqCst), 2);

    let (port, server) = serve_delayed_chat_completion();
    let request_poll_calls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&request_poll_calls);
    let runtime =
        ChatRuntime::attached(AttachedRuntime::for_session_test("demo", port, move || {
            let call = witness.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 4 {
                Err("hostile mid-request diagnostic".into())
            } else {
                Ok(())
            }
        }));
    let mut inputs = vec![
        crate::session::InputEvent::Interrupted,
        crate::session::InputEvent::Line("hello".into()),
    ];
    let error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_input(runtime, 1, || {
            inputs.pop().expect("bounded test input")
        })
    })
    .unwrap_err();
    server.join().unwrap();
    assert_eq!(error, ATTACHED_RUNTIME_UNAVAILABLE);
    assert_eq!(request_poll_calls.load(Ordering::SeqCst), 4);
    assert_eq!(inputs.len(), 1, "replacement aborts before another prompt");
}

#[test]
fn attached_identity_loss_during_a_stalled_request_returns_without_waiting_for_request_timeout() {
    let (port, request_is_stalled, release, server) = serve_stalled_chat_request();
    let witness = Arc::clone(&request_is_stalled);
    let runtime =
        ChatRuntime::attached(AttachedRuntime::for_session_test("demo", port, move || {
            if witness.load(Ordering::SeqCst) {
                Err("hostile mid-request diagnostic".into())
            } else {
                Ok(())
            }
        }));
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let started = std::time::Instant::now();

    let error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_input(runtime, 1, || {
            input.take().expect("only one prompt is read")
        })
    })
    .unwrap_err();
    let elapsed = started.elapsed();
    let _ = release.send(());
    server.join().unwrap();

    assert_eq!(error, ATTACHED_RUNTIME_UNAVAILABLE);
    assert!(
        elapsed < Duration::from_millis(400),
        "attached identity loss waited {elapsed:?} for the stalled request"
    );
}

fn serve_stalled_chat_request() -> (
    u16,
    Arc<std::sync::atomic::AtomicBool>,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let request_is_stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let witness = Arc::clone(&request_is_stalled);
    let (release, released) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
            if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while request.len() < header_end + content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
        }
        witness.store(true, Ordering::SeqCst);
        let _ = released.recv_timeout(Duration::from_millis(750));
    });
    (port, request_is_stalled, release, server)
}

fn serve_delayed_chat_completion() -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
            if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while request.len() < header_end + content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
        }
        std::thread::sleep(Duration::from_millis(60));
        let _ = stream.write_all(
            concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: text/event-stream\r\n",
                "Connection: close\r\n\r\n",
                "data: [DONE]\n\n",
            )
            .as_bytes(),
        );
    });
    (port, server)
}

#[test]
fn attached_terminal_worker_channel_join_and_output_failures_never_control_the_owner() {
    for (input, expected) in [
        (crate::session::InputEvent::Interrupted, Ok(130)),
        (crate::session::InputEvent::Canceled, Ok(0)),
        (
            crate::session::InputEvent::Error("terminal exploded".into()),
            Err("failed to read terminal input: terminal exploded".into()),
        ),
    ] {
        let (runtime, owner_controls) = witnessed_attached_runtime();
        let mut input = Some(input);
        let mut output = Vec::new();
        let outcome = with_runtime_teardown(runtime, |runtime| {
            crate::session::run_session_with_seams(
                runtime,
                1,
                || input.take().expect("one terminal event"),
                &mut output,
                |_, _, _| panic!("terminal exit must not start a Worker"),
            )
        });
        assert_eq!(outcome, expected);
        assert_eq!(owner_controls.load(Ordering::SeqCst), 0);
    }

    let (runtime, owner_controls) = witnessed_attached_runtime();
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let mut output = Vec::new();
    let start_error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_seams(
            runtime,
            1,
            || input.take().expect("one prompt"),
            &mut output,
            |_, _, _| Err("Worker start failed".into()),
        )
    })
    .unwrap_err();
    assert_eq!(start_error, "Worker start failed");
    assert_eq!(owner_controls.load(Ordering::SeqCst), 0);

    for events in [vec![Event::Error("request failed".into())], Vec::new()] {
        let (runtime, owner_controls) = witnessed_attached_runtime();
        let mut events = Some(events);
        let mut inputs = vec![
            crate::session::InputEvent::Interrupted,
            crate::session::InputEvent::Line("hello".into()),
        ];
        let mut output = Vec::new();
        let outcome = with_runtime_teardown(runtime, |runtime| {
            crate::session::run_session_with_seams(
                runtime,
                1,
                || inputs.pop().expect("bounded terminal input"),
                &mut output,
                |_, _, _| {
                    Ok(crate::chat::Worker::for_session_test(
                        events.take().expect("one Worker"),
                        false,
                    ))
                },
            )
        });
        assert_eq!(outcome, Ok(130));
        assert_eq!(owner_controls.load(Ordering::SeqCst), 0);
    }

    let (runtime, owner_controls) = witnessed_attached_runtime();
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let mut output = Vec::new();
    let join_error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_seams(
            runtime,
            1,
            || input.take().expect("one prompt"),
            &mut output,
            |_, _, _| {
                Ok(crate::chat::Worker::for_session_test(
                    vec![Event::Complete("done".into())],
                    true,
                ))
            },
        )
    })
    .unwrap_err();
    assert_eq!(join_error, "chat request worker panicked");
    assert_eq!(owner_controls.load(Ordering::SeqCst), 0);

    let (runtime, owner_controls) = witnessed_attached_runtime();
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let mut output = FailingWriter;
    let output_error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_seams(
            runtime,
            1,
            || input.take().expect("one prompt"),
            &mut output,
            |_, _, _| {
                Ok(crate::chat::Worker::for_session_test(
                    vec![
                        Event::Delta("token".into()),
                        Event::Complete("token".into()),
                    ],
                    false,
                ))
            },
        )
    })
    .unwrap_err();
    assert_eq!(output_error, "injected output failure");
    assert_eq!(owner_controls.load(Ordering::SeqCst), 0);
}

#[test]
fn owned_endpoint_error_terminates_once_before_joining_the_worker() {
    let polls = Arc::new(AtomicUsize::new(0));
    let poll_calls = Arc::clone(&polls);
    let terminations = Arc::new(AtomicUsize::new(0));
    let termination_calls = Arc::clone(&terminations);
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let termination_order = Arc::clone(&order);
    let runtime = ChatRuntime::owned_for_test(
        "demo",
        43123,
        move || {
            let call = poll_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 3 {
                Err("owned endpoint poll failed".into())
            } else {
                Ok(None)
            }
        },
        move || {
            termination_calls.fetch_add(1, Ordering::SeqCst);
            termination_order.lock().unwrap().push("terminate");
            Ok(())
        },
    );
    let mut input = Some(crate::session::InputEvent::Line("hello".into()));
    let mut output = Vec::new();
    let worker_order = Arc::clone(&order);

    let error = with_runtime_teardown(runtime, |runtime| {
        crate::session::run_session_with_seams(
            runtime,
            1,
            || input.take().expect("one prompt"),
            &mut output,
            move |_, _, _| {
                let worker_order = Arc::clone(&worker_order);
                Ok(crate::chat::Worker::for_session_test_thread(move || {
                    std::thread::sleep(Duration::from_millis(30));
                    worker_order.lock().unwrap().push("join");
                }))
            },
        )
    })
    .unwrap_err();

    assert_eq!(error, "owned endpoint poll failed");
    assert_eq!(terminations.load(Ordering::SeqCst), 1);
    assert_eq!(*order.lock().unwrap(), ["terminate", "join"]);
}

fn witnessed_attached_runtime() -> (ChatRuntime, Arc<AtomicUsize>) {
    let owner_controls = Arc::new(AtomicUsize::new(0));
    let witness = Arc::clone(&owner_controls);
    let attached = AttachedRuntime::for_session_test_with_owner_witness(
        "demo",
        43123,
        || Ok(()),
        move || {
            witness.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    (ChatRuntime::attached(attached), owner_controls)
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("injected output failure"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
