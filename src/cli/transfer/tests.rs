use super::*;
use crate::catalog::Manifest;
use crate::cli::dispatch::{run, run_with_recovery};
use crate::cli::{Cli, Command, PullArgs};
use crate::discovery::{DiscoveryError, DiscoveryErrorKind};
use crate::paths::AppPaths;
use clap::Parser;

#[test]
fn default_id_owns_repo_artifact_and_digest_identity() {
    let first = default_id(
        "alice/demo",
        "demo-Q4_K_M.gguf",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let other_owner = default_id(
        "bob/demo",
        "demo-Q4_K_M.gguf",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let other_file = default_id(
        "alice/demo",
        "demo-Q8_0.gguf",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );

    assert_ne!(first, other_owner);
    assert_ne!(first, other_file);
    assert!(first.starts_with("alice-demo-demo-q4-k-m-"));
    let long = default_id(
        &format!("owner/{}", "a".repeat(100)),
        &format!("{}.gguf", "b".repeat(100)),
        "0123456789abcdefaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    assert!(long.ends_with("0123456789abcdef"));
}

fn pull_cli(
    repo: &str,
    revision: Option<&str>,
    filename: Option<&str>,
    quant: Option<&str>,
) -> Cli {
    Cli {
        command: Command::Pull(PullArgs {
            repo: repo.into(),
            revision: revision.map(str::to_owned),
            filename: filename.map(str::to_owned),
            quant: quant.map(str::to_owned),
            name: None,
        }),
    }
}

#[test]
fn pull_adapter_resolves_once_then_transfers_the_exact_returned_value_once() {
    let root = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
    let service = crate::app::AppService::from_paths(paths.clone());
    let artifact = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "exact.gguf",
        "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
        6,
    );
    let installed = Manifest {
        version: 1,
        id: "demo".into(),
        repo: Some(artifact.repo().into()),
        revision: Some(artifact.commit().into()),
        remote_filename: Some(artifact.path().into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: artifact.sha256().into(),
        size: artifact.size(),
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let model_dir = paths.model_dir("demo").unwrap();
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    std::fs::write(
        model_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&installed).unwrap(),
    )
    .unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();

    let calls = std::cell::RefCell::new(Vec::new());
    let result = pull_adapter_with(
        crate::cli::parse_pull_input(&PullArgs {
            repo: "owner/repo".into(),
            revision: Some("moving-branch".into()),
            filename: Some("exact.gguf".into()),
            quant: None,
            name: Some("demo".into()),
        })
        .unwrap(),
        |_| {
            calls.borrow_mut().push("resolve");
            Ok(artifact.clone())
        },
        |selected, control, progress| {
            calls.borrow_mut().push("transfer");
            service.transfer_selected(selected, control, progress)
        },
        |_, _, _| {},
    )
    .unwrap();

    assert_eq!(&*calls.borrow(), &["resolve", "transfer"]);
    assert_eq!(result.artifact(), &artifact);
    assert_eq!(result.model_id(), "demo");
}

#[cfg(unix)]
fn kill_and_reap_sigint_child(child: &mut std::process::Child) -> String {
    let kill = child.kill();
    let wait = child.wait();
    format!("kill={kill:?}; wait={wait:?}")
}

#[cfg(unix)]
fn wait_for_sigint_path(path: &std::path::Path, child: &mut std::process::Child) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !path.exists() {
        match child.try_wait() {
            Ok(Some(status)) => {
                use std::io::Read as _;

                let mut stdout = String::new();
                let mut stderr = String::new();
                child
                    .stdout
                    .take()
                    .unwrap()
                    .read_to_string(&mut stdout)
                    .unwrap();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                panic!(
                        "SIGINT child exited before handshake: {status}; stdout={stdout:?}; stderr={stderr:?}"
                    );
            }
            Ok(None) => {}
            Err(error) => {
                let cleanup = kill_and_reap_sigint_child(child);
                panic!("SIGINT child handshake poll failed: {error}; {cleanup}");
            }
        }
        if std::time::Instant::now() >= deadline {
            let cleanup = kill_and_reap_sigint_child(child);
            panic!("SIGINT child handshake timed out; {cleanup}");
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(unix)]
fn wait_for_sigint_child_with<P>(
    mut child: std::process::Child,
    mut poll: P,
) -> std::process::Output
where
    P: FnMut(&mut std::process::Child) -> std::io::Result<Option<std::process::ExitStatus>>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match poll(&mut child) {
            Ok(Some(_)) => return child.wait_with_output().unwrap(),
            Ok(None) => {}
            Err(error) => {
                let cleanup = kill_and_reap_sigint_child(&mut child);
                panic!("SIGINT child poll failed: {error}; {cleanup}");
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("SIGINT child timed out and was killed: {output:?}");
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(unix)]
fn wait_for_sigint_child(child: std::process::Child) -> std::process::Output {
    wait_for_sigint_child_with(child, std::process::Child::try_wait)
}

#[cfg(unix)]
fn wait_for_sigint_pause(control: &crate::app::TransferControl) {
    // Signal delivery and the listener's pause request are separate steps.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !control.pause_requested_for_test() {
        assert!(
            std::time::Instant::now() < deadline,
            "SIGINT listener did not request pause"
        );
        std::thread::yield_now();
    }
}

#[cfg(unix)]
fn run_sigint_child(scenario: &str) -> (tempfile::TempDir, std::process::Output) {
    let root = tempfile::tempdir().unwrap();
    let scenario_path = root.path().join("scenario");
    let handshake = root.path().join("handshake");
    let release = root.path().join("release");
    std::fs::write(&scenario_path, scenario).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli::transfer::tests::selected_transfer_sigint_child_helper",
            "--nocapture",
        ])
        .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
        .env("LOXA_SIGINT_HANDSHAKE", &handshake)
        .env("LOXA_SIGINT_RELEASE", &release)
        .env("LOXA_HOME", root.path().join("loxa-home"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_sigint_path(&handshake, &mut child);
    let signal_count = if scenario == "repeated" { 3 } else { 1 };
    for _ in 0..signal_count {
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    }
    std::fs::write(release, b"continue").unwrap();
    let output = wait_for_sigint_child(child);
    (root, output)
}

#[cfg(unix)]
#[test]
fn sigint_handshake_timeout_kills_and_reaps_child() {
    let root = tempfile::tempdir().unwrap();
    let scenario_path = root.path().join("scenario");
    let handshake = root.path().join("never-created-handshake");
    std::fs::write(&scenario_path, "handshake-timeout").unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli::transfer::tests::selected_transfer_sigint_child_helper",
            "--nocapture",
        ])
        .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for_sigint_path(&handshake, &mut child);
    }));
    assert!(result.is_err());
    let reaped = child.try_wait().is_ok_and(|status| status.is_some());
    if !reaped {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(reaped, "handshake timeout left its child running");
}

#[cfg(unix)]
#[test]
fn sigint_child_poll_error_kills_and_reaps_child() {
    let root = tempfile::tempdir().unwrap();
    let scenario_path = root.path().join("scenario");
    std::fs::write(&scenario_path, "handshake-timeout").unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli::transfer::tests::selected_transfer_sigint_child_helper",
            "--nocapture",
        ])
        .env("LOXA_SIGINT_SCENARIO_PATH", &scenario_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id() as libc::pid_t;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for_sigint_child_with(child, |_| {
            Err(std::io::Error::other("injected child poll failure"))
        });
    }));
    assert!(result.is_err());
    let still_running = unsafe { libc::kill(pid, 0) } == 0;
    if still_running {
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    }
    assert!(!still_running, "poll error left its child running");
}

#[cfg(unix)]
#[test]
fn selected_transfer_sigint_child_helper() {
    let Some(scenario_path) = std::env::var_os("LOXA_SIGINT_SCENARIO_PATH") else {
        return;
    };
    let scenario = std::fs::read_to_string(scenario_path).unwrap();
    if scenario == "handshake-timeout" {
        loop {
            std::thread::park();
        }
    }
    let handshake = std::path::PathBuf::from(
        std::env::var_os("LOXA_SIGINT_HANDSHAKE").expect("handshake path"),
    );
    let release =
        std::path::PathBuf::from(std::env::var_os("LOXA_SIGINT_RELEASE").expect("release path"));
    let paths = AppPaths::from_values(
        std::env::var_os("LOXA_HOME")
            .as_deref()
            .map(std::path::Path::new),
        None,
    )
    .unwrap();
    let service = crate::app::AppService::from_paths(paths.clone());
    let artifact = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "exact.gguf",
        "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
        6,
    );
    if scenario == "old-control" {
        let manifest = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some(artifact.repo().into()),
            revision: Some(artifact.commit().into()),
            remote_filename: Some(artifact.path().into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: artifact.sha256().into(),
            size: artifact.size(),
            artifacts: None,
            profile: None,
            runtime: None,
        };
        let model_dir = paths.model_dir("demo").unwrap();
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        drop(lock);
        std::fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();

        let old_control = crate::app::TransferControl::new();
        drop(super::ScopedTransferInterrupt::install(old_control.clone()).unwrap());
        let prompt = super::PromptInterrupt::install().unwrap();
        std::fs::write(&handshake, b"ready").unwrap();
        while !release.exists() {
            std::thread::yield_now();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !prompt.received() {
            assert!(
                std::time::Instant::now() < deadline,
                "SIGINT was not observed"
            );
            std::thread::yield_now();
        }
        drop(prompt);
        let result = service
            .transfer_selected(
                crate::app::TransferSelected::new(artifact, Some("demo".into())),
                old_control,
                |_| {},
            )
            .unwrap();
        assert_eq!(
            result.disposition(),
            crate::app::TransferDisposition::AlreadyInstalled
        );
        return;
    }
    if matches!(scenario.as_str(), "verification" | "repeated" | "late") {
        let model_dir = paths.model_dir("demo").unwrap();
        let manifest = Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some(artifact.repo().into()),
            revision: Some(artifact.commit().into()),
            remote_filename: Some(artifact.path().into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: artifact.sha256().into(),
            size: artifact.size(),
            artifacts: None,
            profile: None,
            runtime: None,
        };
        crate::catalog::prepare_pull(&model_dir, &manifest).unwrap();
        std::fs::write(model_dir.join("model.gguf.part"), b"abcdef").unwrap();
    }
    let result = pull_adapter_with(
        crate::cli::parse_pull_input(&PullArgs {
            repo: "owner/repo".into(),
            revision: Some("main".into()),
            filename: Some("exact.gguf".into()),
            quant: None,
            name: Some("demo".into()),
        })
        .unwrap(),
        |_| Ok(artifact),
        |selected, control, progress| {
            if scenario == "before" {
                std::fs::write(&handshake, b"ready").unwrap();
                while !release.exists() {
                    std::thread::yield_now();
                }
                wait_for_sigint_pause(&control);
                return service.transfer_selected(selected, control, progress);
            }
            let handshake_phase = if scenario == "late" {
                crate::app::TransferPhase::Publishing
            } else {
                crate::app::TransferPhase::Verifying
            };
            let observed_control = control.clone();
            service.transfer_selected(selected, control, |update| {
                if update.phase() == handshake_phase && !handshake.exists() {
                    std::fs::write(&handshake, b"ready").unwrap();
                    while !release.exists() {
                        std::thread::yield_now();
                    }
                    wait_for_sigint_pause(&observed_control);
                }
                progress(update);
            })
        },
        |_, _, _| {},
    )
    .unwrap();
    match scenario.as_str() {
        "before" => {
            assert_eq!(
                result.disposition(),
                crate::app::TransferDisposition::Interrupted
            );
            assert_eq!(result.retained_bytes(), None);
            eprintln!("Interrupted before transfer began");
            std::process::exit(130);
        }
        "verification" | "repeated" => {
            assert_eq!(
                result.disposition(),
                crate::app::TransferDisposition::Paused
            );
            assert_eq!(result.retained_bytes(), Some(6));
            let recovery = RecoveryNotice {
                model_id: result.model_id(),
                artifact: result.artifact(),
                retained_bytes: 6,
                discardable: result.discardable(),
            };
            eprintln!("{}", format_paused(&recovery));
            std::process::exit(130);
        }
        "late" => {
            assert_eq!(
                result.disposition(),
                crate::app::TransferDisposition::Installed
            );
            print_pull_completion(result.model_id(), result.disposition());
        }
        _ => panic!("unknown SIGINT scenario {scenario:?}"),
    }
}

#[cfg(unix)]
#[test]
fn sigint_before_mutable_transfer_exits_130_without_retained_claim() {
    use std::os::unix::process::ExitStatusExt as _;

    let (_root, output) = run_sigint_child("before");
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    assert_eq!(output.status.signal(), None, "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Interrupted before transfer began"),
        "{stderr}"
    );
    assert!(!stderr.contains("bytes retained"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn sigint_during_verification_exits_130_only_after_durable_pause_barrier() {
    let (root, output) = run_sigint_child("verification");
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Paused demo · 6 / 6 bytes retained"),
        "{stderr}"
    );
    let model_dir = root.path().join("loxa-home/models/demo");
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
        b"abcdef"
    );
    assert!(!model_dir.join("model.gguf").exists());
    assert!(model_dir.join("pending.json").exists());
}

#[cfg(unix)]
#[test]
fn repeated_sigint_is_idempotent_and_cannot_bypass_sync_or_join() {
    let (root, output) = run_sigint_child("repeated");
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(
        stderr.matches("Paused demo · 6 / 6 bytes retained").count(),
        1
    );
    let model_dir = root.path().join("loxa-home/models/demo");
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
        b"abcdef"
    );
    assert!(!model_dir.join("model.gguf").exists());
}

#[cfg(unix)]
#[test]
fn sigint_after_completion_fence_allows_normal_installed_completion() {
    let (root, output) = run_sigint_child("late");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("Pulled demo\nRun: loxa run demo"),
        "{stdout}"
    );
    let model_dir = root.path().join("loxa-home/models/demo");
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf")).unwrap(),
        b"abcdef"
    );
    assert!(!model_dir.join("model.gguf.part").exists());
    assert!(model_dir.join("manifest.json").exists());
}

#[cfg(unix)]
#[test]
fn scoped_pause_listener_closes_and_joins_on_success_error_pause_and_panic_unwind() {
    fn artifact() -> crate::huggingface::ResolvedFile {
        crate::huggingface::test_resolved_file_for(
            "owner/repo",
            "exact.gguf",
            "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".into(),
            6,
        )
    }
    fn input() -> crate::cli::PullInput {
        crate::cli::parse_pull_input(&PullArgs {
            repo: "owner/repo".into(),
            revision: Some("main".into()),
            filename: Some("exact.gguf".into()),
            quant: None,
            name: Some("demo".into()),
        })
        .unwrap()
    }

    let success_root = tempfile::tempdir().unwrap();
    let success_paths = AppPaths::from_values(Some(success_root.path()), None).unwrap();
    let success_artifact = artifact();
    let success_manifest = Manifest {
        version: 1,
        id: "demo".into(),
        repo: Some(success_artifact.repo().into()),
        revision: Some(success_artifact.commit().into()),
        remote_filename: Some(success_artifact.path().into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: success_artifact.sha256().into(),
        size: success_artifact.size(),
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let success_dir = success_paths.model_dir("demo").unwrap();
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&success_dir).unwrap();
    drop(lock);
    std::fs::write(
        success_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&success_manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(success_dir.join("model.gguf"), b"abcdef").unwrap();
    let success_service = crate::app::AppService::from_paths(success_paths);
    assert_eq!(
        pull_adapter_with(
            input(),
            |_| Ok(success_artifact),
            |selected, control, progress| {
                success_service.transfer_selected(selected, control, progress)
            },
            |_, _, _| {},
        )
        .unwrap()
        .disposition(),
        crate::app::TransferDisposition::AlreadyInstalled
    );

    let error_root = tempfile::tempdir().unwrap();
    let error_paths = AppPaths::from_values(Some(error_root.path()), None).unwrap();
    let error_dir = error_paths.model_dir("demo").unwrap();
    let _busy_lock = crate::catalog::ModelLock::acquire_for_transfer(&error_dir).unwrap();
    let error_service = crate::app::AppService::from_paths(error_paths);
    let error = match pull_adapter_with(
        input(),
        |_| Ok(artifact()),
        |selected, control, progress| error_service.transfer_selected(selected, control, progress),
        |_, _, _| {},
    ) {
        Ok(_) => panic!("busy transfer unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        super::PullAdapterError::Transfer { error, .. }
            if error.kind() == crate::app::transfer::TransferErrorKind::Busy
    ));

    let pause_root = tempfile::tempdir().unwrap();
    let pause_paths = AppPaths::from_values(Some(pause_root.path()), None).unwrap();
    let pause_artifact = artifact();
    let pause_manifest = Manifest {
        version: 1,
        id: "demo".into(),
        repo: Some(pause_artifact.repo().into()),
        revision: Some(pause_artifact.commit().into()),
        remote_filename: Some(pause_artifact.path().into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: pause_artifact.sha256().into(),
        size: pause_artifact.size(),
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let pause_dir = pause_paths.model_dir("demo").unwrap();
    crate::catalog::prepare_pull(&pause_dir, &pause_manifest).unwrap();
    std::fs::write(pause_dir.join("model.gguf.part"), b"abcdef").unwrap();
    let pause_service = crate::app::AppService::from_paths(pause_paths);
    let paused = pull_adapter_with(
        input(),
        |_| Ok(pause_artifact),
        |selected, control, progress| {
            let pause = control.clone();
            pause_service.transfer_selected(selected, control, |update| {
                if update.phase() == crate::app::TransferPhase::Verifying {
                    pause.request_pause();
                }
                progress(update);
            })
        },
        |_, _, _| {},
    )
    .unwrap();
    assert_eq!(
        paused.disposition(),
        crate::app::TransferDisposition::Paused
    );

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = pull_adapter_with(
            input(),
            |_| Ok(artifact()),
            |_, _, _| -> Result<crate::app::TransferResult, crate::app::TransferError> {
                panic!("injected transfer panic")
            },
            |_, _, _| {},
        );
    }));
    assert!(panic.is_err());
}

#[cfg(unix)]
#[test]
fn old_signal_control_cannot_pause_a_later_activation() {
    let (_root, output) = run_sigint_child("old-control");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

#[test]
fn ordinary_pull_and_clean_disk_retry_preserve_the_users_optional_revision() {
    let input = crate::cli::parse_pull_input(&PullArgs {
        repo: "owner/repo".into(),
        revision: Some("moving branch".into()),
        filename: Some("exact.gguf".into()),
        quant: None,
        name: Some("demo".into()),
    })
    .unwrap();
    let command = ordinary_pull_command(&input);
    assert_eq!(
        command,
        "loxa pull 'owner/repo' --file='exact.gguf' --revision='moving branch' --name='demo'"
    );
    assert_eq!(
        format_insufficient_disk("demo", 6, 100, 40, None, &command),
        concat!(
            "Not enough disk space to transfer demo.\n",
            "Required available: 100 bytes\n",
            "Available now:      40 bytes\n",
            "Shortfall:          60 bytes\n",
            "No artifact bytes were downloaded. Free space and rerun: ",
            "loxa pull 'owner/repo' --file='exact.gguf' --revision='moving branch' --name='demo'",
        )
    );
}

#[test]
fn pause_and_resumable_error_commands_pin_full_commit_exact_file_and_same_id() {
    let artifact =
        crate::huggingface::test_resolved_file_for("owner/repo", "exact.gguf", "a".repeat(64), 6);
    let recovery = RecoveryNotice {
        model_id: "demo",
        artifact: &artifact,
        retained_bytes: 3,
        discardable: true,
    };
    assert_eq!(
        format_paused(&recovery),
        concat!(
            "Paused demo · 3 / 6 bytes retained\n",
            "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
            "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
            "Discard: loxa discard 'demo'",
        )
    );

    let zero_prefix = RecoveryNotice {
        retained_bytes: 0,
        ..recovery
    };
    assert_eq!(
        format_resumable_failure(
            static_transfer_error_message(crate::app::transfer::TransferErrorKind::Remote),
            &zero_prefix,
        ),
        concat!(
            "Artifact transfer failed.\n",
            "0 / 6 bytes retained\n",
            "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
            "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
            "Discard: loxa discard 'demo'",
        )
    );
}

#[cfg(unix)]
fn shell_argv(command: &str, directory: &std::path::Path) -> Vec<String> {
    let script = format!("set -- {command}; printf '%s\\n' \"$@\"");
    let output = std::process::Command::new("/bin/sh")
        .args(["-c", &script])
        .current_dir(directory)
        .output()
        .expect("evaluate fixture-generated command words");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stderr, b"");
    String::from_utf8(output.stdout)
        .expect("UTF-8 shell argv")
        .lines()
        .map(str::to_owned)
        .collect()
}

#[cfg(unix)]
#[test]
fn recovery_commands_shell_round_trip_hostile_complete_words_without_evaluation() {
    let temp = tempfile::tempdir().unwrap();
    let hostile = "-model ' $(touch recovery-dollar) `touch recovery-backtick`.gguf";
    let artifact =
        crate::huggingface::test_resolved_file_for("owner/repo", hostile, "a".repeat(64), 6);
    let command = recovery_command(&artifact, "demo");
    let argv = shell_argv(&command, temp.path());
    assert_eq!(
        argv,
        [
            "loxa",
            "pull",
            "owner/repo",
            "--file=-model ' $(touch recovery-dollar) `touch recovery-backtick`.gguf",
            "--revision=0123456789abcdef0123456789abcdef01234567",
            "--name=demo",
        ]
    );
    assert!(!temp.path().join("recovery-dollar").exists());
    assert!(!temp.path().join("recovery-backtick").exists());
}

#[test]
fn insufficient_disk_prints_exact_required_available_shortfall_and_no_reservation_claim() {
    let output = format_insufficient_disk(
        "demo",
        6,
        8192,
        4096,
        None,
        "loxa pull 'owner/repo' --quant='Q4_K_M'",
    );
    assert_eq!(
        output,
        concat!(
            "Not enough disk space to transfer demo.\n",
            "Required available: 8192 bytes\n",
            "Available now:      4096 bytes\n",
            "Shortfall:          4096 bytes\n",
            "No artifact bytes were downloaded. Free space and rerun: ",
            "loxa pull 'owner/repo' --quant='Q4_K_M'",
        )
    );
    for excluded in ["reserved", "compatible", "fits", "will run"] {
        assert!(!output.to_ascii_lowercase().contains(excluded), "{output}");
    }
}

#[test]
fn resume_disk_error_prints_exact_retained_bytes_and_only_safe_discard_guidance() {
    let artifact =
        crate::huggingface::test_resolved_file_for("owner/repo", "exact.gguf", "a".repeat(64), 6);
    let safe = RecoveryNotice {
        model_id: "demo",
        artifact: &artifact,
        retained_bytes: 3,
        discardable: true,
    };
    assert_eq!(
        format_resumable_failure("Disk space was exhausted while transferring demo.", &safe),
        concat!(
            "Disk space was exhausted while transferring demo.\n",
            "3 / 6 bytes retained\n",
            "Resume: loxa pull 'owner/repo' --file='exact.gguf' ",
            "--revision='0123456789abcdef0123456789abcdef01234567' --name='demo'\n",
            "Discard: loxa discard 'demo'",
        )
    );

    let repair = RecoveryNotice {
        discardable: false,
        ..safe
    };
    let repair_output =
        format_resumable_failure("Disk space was exhausted while transferring demo.", &repair);
    assert!(!repair_output.contains("loxa discard"), "{repair_output}");
    assert!(
        repair_output
            .ends_with("Installed or repair evidence was retained; discard is unavailable."),
        "{repair_output}"
    );
}

#[test]
fn non_tty_progress_emits_at_most_one_plain_line_per_phase_without_ansi_or_cursor_controls() {
    let mut renderer = PlainProgressRenderer::default();
    let updates = [
        (crate::app::TransferPhase::Transferring, 1, 6),
        (crate::app::TransferPhase::Transferring, 4, 6),
        (crate::app::TransferPhase::Verifying, 6, 6),
        (crate::app::TransferPhase::Verifying, 6, 6),
        (crate::app::TransferPhase::Publishing, 6, 6),
        (crate::app::TransferPhase::Publishing, 6, 6),
    ];
    let output = updates
        .into_iter()
        .filter_map(|(phase, transferred, total)| {
            renderer.line("exact.gguf", "demo", phase, transferred, total)
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        output,
        "Downloading exact.gguf  1 / 6\nVerifying exact.gguf\nPublishing demo"
    );
    for control in ['\u{1b}', '\r', '\u{8}'] {
        assert!(!output.contains(control), "{output:?}");
    }
}

#[test]
fn completion_output_preserves_pulled_verified_and_optional_run_guidance_without_compatibility_claims(
) {
    for (disposition, expected) in [
        (
            crate::app::TransferDisposition::Installed,
            "Pulled demo\nRun: loxa run demo\n",
        ),
        (
            crate::app::TransferDisposition::AlreadyInstalled,
            "Verified demo · already installed\nRun: loxa run demo\n",
        ),
    ] {
        let output = completion_output("demo", disposition).unwrap();
        assert_eq!(output, expected);
        let words = output
            .split(|character: char| !character.is_ascii_alphanumeric())
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        for excluded in ["compatible", "supported", "ready", "recommended", "fits"] {
            assert!(!words.iter().any(|word| word == excluded), "{output}");
        }
    }
}

#[test]
fn transfer_error_mapping_is_static_exhaustive_and_redacted() {
    use crate::app::transfer::TransferErrorKind;

    let cases = [
        (TransferErrorKind::InvalidModelId, "Invalid model ID."),
        (
            TransferErrorKind::Busy,
            "Another transfer is already using this model.",
        ),
        (
            TransferErrorKind::UnsafeLocalState,
            "Local model state is unsafe; no files were changed.",
        ),
        (
            TransferErrorKind::ArtifactConflict,
            "This model ID already refers to a different artifact.",
        ),
        (
            TransferErrorKind::CapacityUnavailable,
            "Destination disk capacity could not be determined.",
        ),
        (
            TransferErrorKind::CapacityOverflow,
            "Destination disk capacity could not be calculated safely.",
        ),
        (
            TransferErrorKind::CatalogManifestTooLarge,
            "Selected artifact metadata exceeds Loxa's 4,194,304-byte catalog limit.",
        ),
        (
            TransferErrorKind::InsufficientDisk,
            "Insufficient disk space.",
        ),
        (TransferErrorKind::Remote, "Artifact transfer failed."),
        (
            TransferErrorKind::Integrity,
            "Artifact integrity verification failed.",
        ),
        (
            TransferErrorKind::DiskExhausted,
            "The destination ran out of disk space during transfer.",
        ),
        (
            TransferErrorKind::Durability,
            "Artifact durability could not be confirmed.",
        ),
        (
            TransferErrorKind::Publication,
            "Artifact publication failed.",
        ),
        (
            TransferErrorKind::NoIncompleteTransfer,
            "No incomplete transfer exists.",
        ),
        (
            TransferErrorKind::CompletionWon,
            "The artifact completed before this action.",
        ),
        (
            TransferErrorKind::IncompleteTransferChanged,
            "The incomplete transfer changed; rerun the command.",
        ),
    ];
    for (kind, expected) in cases {
        let output = static_transfer_error_message(kind);
        assert_eq!(output, expected);
        for secret in [
            "https://evil.invalid",
            "HF_TOKEN",
            "/private/model",
            "\u{1b}",
        ] {
            assert!(!output.contains(secret));
        }
    }
}

#[test]
fn catalog_manifest_too_large_maps_to_the_exact_static_cli_text() {
    assert_eq!(
        static_transfer_error_message(
            crate::app::transfer::TransferErrorKind::CatalogManifestTooLarge
        ),
        "Selected artifact metadata exceeds Loxa's 4,194,304-byte catalog limit."
    );
}

#[test]
fn pull_preflight_rejects_missing_selection_before_recovery_with_actionable_revision() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    let calls = std::cell::Cell::new(0);

    let error = run_with_recovery(
        pull_cli("owner/repo", Some("release candidate"), None, None),
        paths,
        |_| {
            calls.set(calls.get() + 1);
            Err("injected recovery must not run".into())
        },
    )
    .unwrap_err();

    assert_eq!(calls.get(), 0);
    for expected in [
        "no GGUF was selected for owner/repo",
        "loxa inspect owner/repo --revision='release candidate'",
        "loxa pull owner/repo --file <FILENAME> --revision='release candidate'",
        "loxa pull owner/repo --quant <QUANT> --revision='release candidate'",
        "loxa pull hf.co/owner/repo:<FILENAME-or-QUANT> --revision='release candidate'",
    ] {
        assert!(error.contains(expected), "missing {expected:?} in {error}");
    }
}

#[test]
fn pull_preflight_rejects_conflicting_and_malformed_inputs_before_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    let cases = [
        pull_cli("owner/repo", None, Some("model.gguf"), Some("Q4_K_M")),
        pull_cli("hf.co/owner/repo:Q4_K_M", None, Some("model.gguf"), None),
        pull_cli("hf.co/owner/repo:", None, None, None),
        pull_cli("owner/extra/repo", None, Some("model.gguf"), None),
        pull_cli(
            "owner/repo",
            Some("release\u{202e}UNSAFE"),
            Some("model.gguf"),
            None,
        ),
        pull_cli("owner/\u{202e}repo\nUNSAFE", None, Some("model.gguf"), None),
    ];

    for cli in cases {
        let calls = std::cell::Cell::new(0);
        let error = run_with_recovery(cli, paths.clone(), |_| {
            calls.set(calls.get() + 1);
            Err("injected recovery must not run".into())
        })
        .unwrap_err();

        assert_eq!(calls.get(), 0, "{error}");
        assert!(!error.contains("UNSAFE"), "{error:?}");
        assert!(!error.contains('\u{202e}'), "{error:?}");
        assert!(!error.contains("injected recovery"), "{error:?}");
    }
}

fn normalized_pull_for_selection(
    filename: Option<&str>,
    quant: Option<&str>,
    revision: Option<&str>,
) -> crate::cli::PullInput {
    crate::cli::parse_pull_input(&PullArgs {
        repo: "owner/repo".into(),
        revision: revision.map(str::to_owned),
        filename: filename.map(str::to_owned),
        quant: quant.map(str::to_owned),
        name: None,
    })
    .unwrap()
}

#[test]
fn missing_quant_selection_error_preserves_labels_and_appends_inspect() {
    let args = normalized_pull_for_selection(None, Some("NOT_A_QUANT"), None);
    let error = execute_pull_resolution(&args, |repo, revision, filename, quant| {
        assert_eq!(repo, "owner/repo");
        assert_eq!(revision, None);
        assert_eq!(filename, None);
        assert_eq!(quant, Some("NOT_A_QUANT"));
        Err(crate::huggingface::ResolveError::Selection(
            crate::huggingface::SelectionError::QuantUnavailable {
                requested: "NOT_A_QUANT".into(),
                available: vec!["Q4_K_M".into(), "Q8_0".into()],
            },
        ))
    })
    .unwrap_err();

    assert_eq!(
            error,
            concat!(
                "quantization \"NOT_A_QUANT\" is not available; available quantizations: Q4_K_M, Q8_0. Retry with --quant <one of these values>.\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect owner/repo",
            )
        );
}

#[test]
fn ambiguous_quant_selection_error_preserves_filenames_and_revision_inspect() {
    let args = normalized_pull_for_selection(None, Some("Q4_K_M"), Some("release candidate"));
    let error = execute_pull_resolution(&args, |_, _, _, _| {
        Err(crate::huggingface::ResolveError::Selection(
            crate::huggingface::SelectionError::AmbiguousQuant {
                requested: "Q4_K_M".into(),
                filenames: vec!["first-Q4_K_M.gguf".into(), "second-Q4_K_M.gguf".into()],
            },
        ))
    })
    .unwrap_err();

    assert_eq!(
            error,
            concat!(
                "quantization \"Q4_K_M\" matched multiple files: first-Q4_K_M.gguf, second-Q4_K_M.gguf. Use --file <filename> to choose one.\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect owner/repo --revision='release candidate'",
            )
        );
}

#[test]
fn missing_exact_file_selection_error_appends_only_safe_inspect_guidance() {
    let args = normalized_pull_for_selection(Some("missing.gguf"), None, None);
    let error = execute_pull_resolution(&args, |_, _, _, _| {
        Err(crate::huggingface::ResolveError::Selection(
            crate::huggingface::SelectionError::FileNotFound("missing.gguf".into()),
        ))
    })
    .unwrap_err();

    assert_eq!(
        error,
        concat!(
            "verified file \"missing.gguf\" not found\n",
            "\n",
            "Inspect every eligible GGUF:\n",
            "  loxa inspect owner/repo",
        )
    );
    for character in error.chars() {
        assert!(
            !character.is_control() || character == '\n',
            "unsafe character in {error:?}"
        );
        assert!(
            !crate::huggingface::unsafe_presentation_character(character) || character == '\n',
            "unsafe presentation character in {error:?}"
        );
    }
    for secret in ["REMOTE_BODY", "HF_TOKEN", "token path"] {
        assert!(!error.contains(secret), "{error}");
    }
}

#[test]
fn discovery_failures_never_gain_selection_recovery_guidance() {
    let args = normalized_pull_for_selection(Some("model.gguf"), None, None);
    for kind in [
        DiscoveryErrorKind::AuthenticationRequired,
        DiscoveryErrorKind::RateLimited,
        DiscoveryErrorKind::DeadlineExceeded,
        DiscoveryErrorKind::MalformedResponse,
    ] {
        let error = execute_pull_resolution(&args, |_, _, _, _| {
            Err(crate::huggingface::ResolveError::Discovery(
                DiscoveryError::new(kind),
            ))
        })
        .unwrap_err();

        assert_eq!(error, "Hugging Face discovery request failed", "{kind:?}");
        assert!(!error.contains("loxa inspect"), "{kind:?}: {error}");
    }
}

#[test]
fn pull_completion_prints_observable_status_and_run_guidance_once() {
    const CHILD_OUTCOME: &str = "LOXA_PULL_COMPLETION_TEST_OUTCOME";
    if let Ok(outcome) = std::env::var(CHILD_OUTCOME) {
        let outcome = match outcome.as_str() {
            "pulled" => crate::app::TransferDisposition::Installed,
            "already-installed" => crate::app::TransferDisposition::AlreadyInstalled,
            unexpected => panic!("unexpected child outcome {unexpected:?}"),
        };
        print_pull_completion("demo-model", outcome);
        return;
    }

    for (outcome, status) in [
        ("pulled", "Pulled demo-model"),
        (
            "already-installed",
            "Verified demo-model · already installed",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "cli::transfer::tests::pull_completion_prints_observable_status_and_run_guidance_once",
                "--nocapture",
            ])
            .env(CHILD_OUTCOME, outcome)
            .env("NO_COLOR", "1")
            .current_dir(root.path())
            .output()
            .expect("capture pull completion output");

        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stderr, b"");
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 completion output");
        assert!(stdout.contains(status), "{stdout:?}");
        assert_eq!(
            stdout.matches("Run: loxa run demo-model").count(),
            1,
            "{stdout:?}"
        );
        assert!(
            root.path().read_dir().unwrap().next().is_none(),
            "completion guidance created local state"
        );
    }
}

#[test]
fn pull_normalizes_only_hf_wrapper_before_existing_local_validation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    let invalid_name = "invalid/name";
    let canonical_error = run(
        Cli::parse_from([
            "loxa",
            "pull",
            "owner/repo",
            "--file",
            "model.gguf",
            "--name",
            invalid_name,
        ]),
        paths.clone(),
    )
    .unwrap_err();
    let wrapped_error = run(
        Cli::parse_from([
            "loxa",
            "pull",
            "hf://owner/repo",
            "--file",
            "model.gguf",
            "--name",
            invalid_name,
        ]),
        paths.clone(),
    )
    .unwrap_err();

    assert_eq!(canonical_error, "invalid model id \"invalid/name\"");
    assert_eq!(wrapped_error, canonical_error);

    for compact in [
        "hf.co/owner/repo:Q4_K_M",
        "huggingface.co/owner/repo:model.GgUf",
    ] {
        let error = run(
            Cli::parse_from(["loxa", "pull", compact, "--name", invalid_name]),
            paths.clone(),
        )
        .unwrap_err();
        assert_eq!(error, canonical_error, "{compact}");
    }

    for repo in ["hf://owner", "https://huggingface.co/owner/repo"] {
        let error = run(
            Cli::parse_from([
                "loxa",
                "pull",
                repo,
                "--file",
                "model.gguf",
                "--name",
                invalid_name,
            ]),
            paths.clone(),
        )
        .unwrap_err();
        assert!(
            error.contains("repository must be exactly owner/repo"),
            "{repo}: {error}"
        );
    }
}
