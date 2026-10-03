use super::*;
use std::fs;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;

const ROOT_ENV: &str = "LOXA_SERVICE_LOCK_TEST_ROOT";
const FALLBACK_ENV: &str = "LOXA_SERVICE_LOCK_TEST_FALLBACK";

fn manifest() -> Manifest {
    Manifest {
        version: 2,
        id: "demo".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(crate::catalog::Origin::Local),
        source_filename: Some("source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 1,
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn mtp_manifest() -> Manifest {
    Manifest {
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
    }
}

fn matching_descriptors(path: &Path) -> Vec<i32> {
    let expected = fs::metadata(path).unwrap();
    (3..4096)
        .filter(|descriptor| {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe { libc::fstat(*descriptor, stat.as_mut_ptr()) } != 0 {
                return false;
            }
            let stat = unsafe { stat.assume_init() };
            #[cfg(target_os = "macos")]
            let same_device = u64::try_from(stat.st_dev).ok() == Some(expected.dev());
            #[cfg(not(target_os = "macos"))]
            let same_device = stat.st_dev == expected.dev();
            same_device && stat.st_ino == expected.ino()
        })
        .collect()
}

#[test]
fn service_model_guard_drop_keeps_the_selected_description_locked() {
    let directory = tempdir().unwrap();
    let mut guard = crate::catalog::ModelLock::acquire(directory.path()).unwrap();
    let selected = guard.duplicate_for_service_child().unwrap();
    drop(guard);
    assert_eq!(
        crate::catalog::ModelLock::acquire_existing(directory.path()).err(),
        Some(crate::catalog::ModelLockError::Busy)
    );
    drop(selected);
    crate::catalog::ModelLock::acquire_existing(directory.path()).unwrap();
}

#[test]
fn service_lock_engine_child() {
    let Some(root) = std::env::var_os(ROOT_ENV) else {
        return;
    };
    let root = PathBuf::from(root);
    let common = matching_descriptors(&root.join("run/foreground.lock"));
    let model = matching_descriptors(&root.join("models/demo/.lock"));
    assert_eq!(
        common.len(),
        1,
        "exact common descriptor did not survive exec"
    );
    assert_eq!(
        model.len(),
        1,
        "exact model descriptor did not survive exec"
    );
    for descriptor in [common[0], model[0]] {
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
    }
    assert!(
        matching_descriptors(&root.join("unrelated")).is_empty(),
        "incidental descriptor crossed exec"
    );
    let endpoint = root.join("run/engine.sock");
    let listener = std::os::unix::net::UnixListener::bind(&endpoint).unwrap();
    listener.set_nonblocking(true).unwrap();
    fs::write(root.join("engine-pid"), std::process::id().to_string()).unwrap();
    fs::write(root.join("engine-ready"), b"ready").unwrap();
    while !root.join("release-engine").is_file() {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                let body = if request[..read].starts_with(b"GET /props ") {
                    r#"{"default_generation_settings":{"n_ctx":4096},"total_slots":1,"model_alias":"demo","endpoint_slots":true}"#
                } else {
                    r#"{"data":[{"id":"demo"}]}"#
                };
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(error) => panic!("stand-in service endpoint failed: {error}"),
        }
    }
    unsafe { libc::_exit(0) }
}

#[test]
fn service_lock_owner_child() {
    let Some(root) = std::env::var_os(ROOT_ENV) else {
        return;
    };
    let root = PathBuf::from(root);
    let server = root.join("server");
    let model_dir = root.join("models/demo");
    let fallback = std::env::var_os(FALLBACK_ENV).is_some();
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.gguf"), b"x").unwrap();
    if fallback {
        fs::write(model_dir.join("draft.gguf"), b"x").unwrap();
    }
    for path in [&root, &std::env::current_exe().unwrap()] {
        assert!(!path.to_string_lossy().contains('\''));
    }
    let fallback_check = if fallback {
        format!("for arg in \"$@\"; do if [ \"$arg\" = '--spec-draft-model' ]; then printf first > '{}'; exit 7; fi; done\n", root.join("fallback-first").display())
    } else {
        String::new()
    };
    write_executable_script(&server, format!(
        "#!/bin/sh\n{fallback_check}export {ROOT_ENV}='{}'\nexec '{}' --exact runner::tests::service_locks::service_lock_engine_child --nocapture\n",
        root.display(), std::env::current_exe().unwrap().display(),
    ).as_bytes());
    let ownership =
        crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&root.join("run"))
            .unwrap_or_else(|_| panic!("service OFD ownership could not be acquired"));
    ownership
        .audit_clean_for_service()
        .unwrap_or_else(|_| panic!("isolated service root was not clean"));
    let selected_manifest = if fallback { mtp_manifest() } else { manifest() };
    let selected_profile = if fallback {
        EffectiveProfile::Gemma4Mtp
    } else {
        EffectiveProfile::Generic
    };
    let fingerprint =
        RuntimeFingerprint::from_manifest_for_service(&selected_manifest, 4096, selected_profile)
            .unwrap();
    let mut runnable = Runnable::for_test(
        crate::catalog::ModelLock::acquire(&model_dir).unwrap(),
        Launch {
            server,
            managed_runtime: None,
            model: model_dir.join("model.gguf"),
            id: "demo".into(),
            requested_port: 0,
            ctx: 4096,
            profile: if fallback {
                LaunchProfile::gemma4_mtp(Some(model_dir.join("draft.gguf")))
            } else {
                LaunchProfile::generic()
            },
            policy: LaunchPolicy::Service,
        },
        fingerprint,
    );
    let unrelated = fs::File::create(root.join("unrelated")).unwrap();
    let token = ownership.reserve_child().unwrap();
    let common = token.duplicate_common_lock_for_service_child().unwrap();
    let model = runnable.duplicate_model_lock_for_service_child().unwrap();
    for file in [&common, &model, &unrelated] {
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
    }
    drop((common, model, token));
    let reactor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let started = start_service_with_ownership(
        runnable,
        &ownership,
        &root.join("run/engine.sock"),
        reactor.handle(),
        || false,
    )
    .unwrap();
    let server = match started {
        PersistentStart::Ready(server) => server,
        PersistentStart::Stopped(exit) => panic!("stand-in service engine exited: {exit:?}"),
        _ => panic!("stand-in service engine did not reach readiness"),
    };
    fs::write(root.join("owner-ready"), b"ready").unwrap();
    std::hint::black_box((&ownership, &server));
    loop {
        std::thread::park();
    }
}

fn wait_for_file(path: &Path, owner: &mut std::process::Child, prelease: bool, fallback: bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !path.is_file() {
        let status = owner.try_wait().unwrap();
        assert!(
            status.is_none(),
            "service owner exited with {status:?} before {} (prelease={prelease}, fallback={fallback})",
            path.display()
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {} (prelease={prelease}, fallback={fallback})",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct ServiceLockFixture {
    root: PathBuf,
    directory: Option<tempfile::TempDir>,
    owner: std::process::Child,
    cleanup_attempted: bool,
}

impl ServiceLockFixture {
    fn wait_for_owner_exit(&mut self) -> Result<std::process::ExitStatus, String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.owner.try_wait().map_err(|error| error.to_string())? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err("service owner did not exit".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn cleanup(&mut self) -> Result<(), String> {
        self.cleanup_attempted = true;
        let release = fs::write(self.root.join("release-engine"), b"release");
        if !matches!(self.owner.try_wait(), Ok(Some(_))) {
            let _ = self.owner.kill();
        }
        let owner_exit = self.wait_for_owner_exit();
        release.map_err(|error| format!("could not release stand-in engine: {error}"))?;
        owner_exit?;

        let model_lock_exists = self.root.join("models/demo/.lock").is_file();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let common = crate::runtime::RuntimeOwnership::acquire_service_unreconciled(
                &self.root.join("run"),
            );
            let model = crate::catalog::ModelLock::acquire_existing(&self.root.join("models/demo"));
            let model_clear = match model {
                Ok(_) => true,
                Err(crate::catalog::ModelLockError::Missing) if !model_lock_exists => true,
                Err(_) => false,
            };
            if common.is_ok() && model_clear {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("inherited locks did not close with the exact child".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn preserve_directory(&mut self, error: &str) {
        if let Some(directory) = self.directory.take() {
            eprintln!(
                "service lock fixture cleanup failed: {error}; fixture retained at {}",
                directory.keep().display()
            );
        }
    }

    fn finish(mut self) {
        if let Err(error) = self.cleanup() {
            self.preserve_directory(&error);
            panic!("{error}");
        }
    }
}

impl Drop for ServiceLockFixture {
    fn drop(&mut self) {
        if !self.cleanup_attempted {
            if let Err(error) = self.cleanup() {
                self.preserve_directory(&error);
            }
        }
    }
}

#[test]
fn service_child_retains_cross_version_and_model_exclusion_after_owner_death() {
    let _serial = process_test_lock();
    for (prelease, fallback) in [(false, false), (true, false), (false, true)] {
        let directory = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "runner::tests::service_locks::service_lock_owner_child",
                "--nocapture",
            ])
            .env(ROOT_ENV, &root)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if fallback {
            command.env(FALLBACK_ENV, "1");
        }
        if prelease {
            command
                .env("LOXA_TEST_POST_SPAWN_KILL_AFTER", root.join("engine-ready"))
                .env("LOXA_TEST_POST_SPAWN_KILL_READY", root.join("owner-ready"));
        }
        let owner = command.spawn().unwrap();
        let mut fixture = ServiceLockFixture {
            root: root.clone(),
            directory: Some(directory),
            owner,
            cleanup_attempted: false,
        };
        wait_for_file(
            &root.join("owner-ready"),
            &mut fixture.owner,
            prelease,
            fallback,
        );
        if !prelease {
            fixture.owner.kill().unwrap();
        }
        let status = fixture.wait_for_owner_exit().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert_eq!(root.join("run/foreground.json").is_file(), !prelease);
        assert_eq!(root.join("fallback-first").is_file(), fallback);
        assert!(root.join("engine-ready").is_file());
        let engine_pid = fs::read_to_string(root.join("engine-pid"))
            .unwrap()
            .parse::<i32>()
            .unwrap();
        assert_eq!(unsafe { libc::kill(engine_pid, 0) }, 0);
        assert!(
            crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&root.join("run"))
                .is_err(),
            "the surviving child released the common lock"
        );
        assert!(
            crate::runtime::RuntimeOwnership::acquire(&root.join("run")).is_err(),
            "legacy recovery acquired through the surviving child"
        );
        assert_eq!(
            crate::catalog::ModelLock::acquire_existing(&root.join("models/demo")).err(),
            Some(crate::catalog::ModelLockError::Busy)
        );
        assert!(crate::catalog::model_is_busy(&root.join("models/demo")).unwrap());
        assert!(
            crate::catalog::remove_model(&root.join("models"), &manifest())
                .unwrap_err()
                .contains("busy")
        );
        fixture.finish();
    }
}
