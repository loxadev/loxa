use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::paths::AppPaths;

use super::inventory::digest;
use super::record::{
    read_stage_record, BUILD_STAGING_PREFIX, STAGE_CHILD_PID_OFFSET, STAGE_TOKEN_OFFSET,
};
use super::stage::{
    create_execution_stage, prepare_embedded_runtime_with_after_capture_fence,
    prepare_embedded_runtime_with_after_inventory_open, publish_regular_bytes,
};
use super::PreparedRuntime;

#[test]
fn restrictive_umask_publish_child() {
    let Some(contents) = std::env::var_os("LOXA_TEST_RESTRICTIVE_UMASK_CONTENTS") else {
        return;
    };
    // SAFETY: this exact-filter subprocess runs only this test, so changing
    // its process umask cannot race another test or escape the child.
    unsafe { libc::umask(0o077) };
    let contents = PathBuf::from(contents);
    fs::create_dir_all(&contents).unwrap();
    publish_regular_bytes(&contents, "read-only", 0o444, b"captured").unwrap();
    assert_eq!(
        fs::metadata(contents.join("read-only"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o444
    );
}

#[test]
fn captured_read_only_mode_survives_a_restrictive_service_umask() {
    let root = tempfile::tempdir().unwrap();
    let contents = root.path().join("Contents");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime_bundle::tests::restrictive_umask_publish_child",
            "--nocapture",
        ])
        .env("LOXA_TEST_RESTRICTIVE_UMASK_CONTENTS", &contents)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fs::metadata(contents.join("read-only"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o444
    );
}

#[test]
fn interrupted_stage_constructor_child() {
    let Some(run) = std::env::var_os("LOXA_TEST_STAGE_CONSTRUCTION_RUN") else {
        return;
    };
    let result = create_execution_stage(Path::new(&run), &[]);
    panic!("stage construction checkpoint did not terminate the child: {result:?}");
}

#[test]
fn interrupted_construction_is_never_published_and_next_acquire_recovers_it() {
    use std::os::unix::process::ExitStatusExt as _;

    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    fs::create_dir_all(&run).unwrap();
    let lookalike = run.join(".bundled-runtime-build-2-3-0123456789abcdef0123456789abcdeg");
    fs::create_dir(&lookalike).unwrap();

    for phase in ["after-mkdir", "mid-record", "after-seal"] {
        let ready = root.path().join(format!("{phase}.ready"));
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime_bundle::tests::interrupted_stage_constructor_child",
                "--nocapture",
            ])
            .env("LOXA_TEST_STAGE_CONSTRUCTION_RUN", &run)
            .env("LOXA_TEST_STAGE_CONSTRUCTION_KILL_PHASE", phase)
            .env("LOXA_TEST_STAGE_CONSTRUCTION_READY", &ready)
            .output()
            .unwrap();
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
        let name = String::from_utf8(fs::read(&ready).unwrap()).unwrap();
        let interrupted = run.join(&name);
        assert!(interrupted.is_dir(), "{phase} checkpoint was not retained");

        drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());

        assert!(
            name.starts_with(BUILD_STAGING_PREFIX),
            "{phase} exposed an executable stage before construction committed: {name}"
        );
        assert!(
            !interrupted.exists(),
            "{phase} interrupted construction was not recovered"
        );
        assert!(lookalike.is_dir(), "{phase} recovery removed a lookalike");
    }
}

#[test]
fn live_constructor_is_preserved_until_its_exact_owner_dies() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    fs::create_dir_all(&run).unwrap();
    let ready = root.path().join("live.ready");
    let lookalike = run.join(".bundled-runtime-build-2-3-0123456789abcdef0123456789abcdeg");
    fs::create_dir(&lookalike).unwrap();
    let mut constructor = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime_bundle::tests::interrupted_stage_constructor_child",
            "--nocapture",
        ])
        .env("LOXA_TEST_STAGE_CONSTRUCTION_RUN", &run)
        .env("LOXA_TEST_STAGE_CONSTRUCTION_PAUSE_PHASE", "after-mkdir")
        .env("LOXA_TEST_STAGE_CONSTRUCTION_READY", &ready)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready.is_file() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        ready.is_file(),
        "live constructor did not publish its checkpoint"
    );
    let name = String::from_utf8(fs::read(&ready).unwrap()).unwrap();
    let build = run.join(&name);

    drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
    assert!(build.is_dir(), "recovery removed a live exact constructor");
    assert!(
        lookalike.is_dir(),
        "recovery removed a construction lookalike"
    );

    constructor.kill().unwrap();
    let _ = constructor.wait().unwrap();
    drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
    assert!(
        !build.exists(),
        "recovery retained a dead exact constructor"
    );
    assert!(
        lookalike.is_dir(),
        "dead-owner recovery removed a lookalike"
    );
}

#[test]
fn pre_exec_group_refusal_is_prompt_errno_only_and_cleans_the_stage() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let stage = run.join(".bundled-runtime-exec-44444444444444444444444444444444");
    let server = stage.join("Contents/MacOS/llama-server");
    fs::create_dir_all(server.parent().unwrap()).unwrap();
    fs::copy("/usr/bin/true", &server).unwrap();
    fs::set_permissions(&server, fs::Permissions::from_mode(0o500)).unwrap();
    let prepared = PreparedRuntime::for_test(stage.clone()).unwrap();
    let mut command = prepared.command();
    command
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    let started = std::time::Instant::now();
    let error = command.spawn().unwrap_err();
    let elapsed = started.elapsed();
    let record = read_stage_record(&prepared.0.owner).unwrap();
    drop(command);
    drop(prepared);

    assert_eq!(error.raw_os_error(), Some(libc::EPERM), "{error:?}");
    assert!(elapsed < std::time::Duration::from_secs(1), "{elapsed:?}");
    assert_eq!(
        &record[STAGE_CHILD_PID_OFFSET..STAGE_TOKEN_OFFSET],
        &[0; 8],
        "failed pre_exec published a child claim"
    );
    assert!(
        !stage.exists(),
        "failed pre_exec leaked its execution stage"
    );
    drop(crate::runtime::RuntimeOwnership::acquire(&run).unwrap());
}

#[test]
#[ignore = "requires a finalized built app"]
fn source_replacement_after_inventory_open_cannot_redefine_the_captured_closure() {
    let built_app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("Loxa.app");
    let replacement = root.path().join("replacement.app");
    for destination in [&source, &replacement] {
        let copied = Command::new("/usr/bin/ditto")
            .args([built_app.as_os_str(), destination.as_os_str()])
            .status()
            .unwrap();
        assert!(copied.success());
    }

    let replacement_resources = replacement.join("Contents/Resources/loxa-runtime/b10344");
    let normalized_path = replacement_resources.join("normalized-inventory.json");
    let mut normalized = fs::read(&normalized_path).unwrap();
    normalized.push(b' ');
    fs::write(&normalized_path, &normalized).unwrap();
    let inventory_path = replacement_resources.join("inventory.json");
    let mut inventory: serde_json::Value =
        serde_json::from_slice(&fs::read(&inventory_path).unwrap()).unwrap();
    let entry = inventory["regular_files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["path"] == "Resources/loxa-runtime/b10344/normalized-inventory.json")
        .unwrap();
    entry["size"] = serde_json::json!(normalized.len() as u64);
    entry["sha256"] = serde_json::json!(digest(&normalized));
    let mut inventory_bytes = serde_json::to_vec_pretty(&inventory).unwrap();
    inventory_bytes.push(b'\n');
    fs::write(&inventory_path, inventory_bytes).unwrap();

    let paths = AppPaths::from_application_values(
        &source.join("Contents/MacOS/loxa-app"),
        Some(&root.path().join("loxa-home")),
        None,
    )
    .unwrap();
    let inspected = root.path().join("inspected.app");
    let error = prepare_embedded_runtime_with_after_inventory_open(&paths, || {
        fs::rename(&source, &inspected).unwrap();
        fs::rename(&replacement, &source).unwrap();
    })
    .unwrap_err();

    assert_eq!(error, "embedded runtime source changed while capturing");
    assert_eq!(
        fs::read(
            inspected.join("Contents/Resources/loxa-runtime/b10344/normalized-inventory.json")
        )
        .unwrap()
        .len()
            + 1,
        normalized.len()
    );
}

#[test]
#[ignore = "requires a finalized built app"]
fn in_place_source_mutation_after_the_fence_cannot_redefine_published_bytes() {
    let built_app = PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("Loxa.app");
    let copied = Command::new("/usr/bin/ditto")
        .args([built_app.as_os_str(), source.as_os_str()])
        .status()
        .unwrap();
    assert!(copied.success());

    let helper = source.join("Contents/MacOS/llama-server");
    let inventory = source.join("Contents/Resources/loxa-runtime/b10344/inventory.json");
    let original_helper = fs::read(&helper).unwrap();
    let original_inventory = fs::read(&inventory).unwrap();
    let paths = AppPaths::from_application_values(
        &source.join("Contents/MacOS/loxa-app"),
        Some(&root.path().join("loxa-home")),
        None,
    )
    .unwrap();

    let prepared = prepare_embedded_runtime_with_after_capture_fence(&paths, || {
        for path in [&helper, &inventory] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut changed_helper = original_helper.clone();
        changed_helper.extend_from_slice(b"post-fence replacement");
        fs::write(&helper, &changed_helper).unwrap();

        let mut changed_inventory: serde_json::Value =
            serde_json::from_slice(&original_inventory).unwrap();
        let helper_entry = changed_inventory["regular_files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["path"] == "MacOS/llama-server")
            .unwrap();
        helper_entry["size"] = serde_json::json!(changed_helper.len() as u64);
        helper_entry["sha256"] = serde_json::json!(digest(&changed_helper));
        let mut changed_inventory = serde_json::to_vec_pretty(&changed_inventory).unwrap();
        changed_inventory.push(b'\n');
        fs::write(&inventory, changed_inventory).unwrap();
    })
    .unwrap();

    let staged_helper = prepared.execution_server();
    let staged_inventory = staged_helper
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("Resources/loxa-runtime/b10344/inventory.json");
    assert!(
        fs::read(staged_helper).unwrap() == original_helper,
        "the published helper did not use the already-captured bytes"
    );
    assert!(
        fs::read(staged_inventory).unwrap() == original_inventory,
        "the published inventory did not use the original captured authority"
    );
    assert_ne!(fs::read(helper).unwrap(), original_helper);
    assert_ne!(fs::read(inventory).unwrap(), original_inventory);
}
