use super::*;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

fn fixture() -> (tempfile::TempDir, PathBuf, ClientBootstrap) {
    let directory = tempfile::Builder::new()
        .prefix("lp-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = fs::canonicalize(directory.path()).unwrap().join("normal");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let private = match loxa_ipc::UserRootInspection::inspect(&root).unwrap() {
        loxa_ipc::UserRootInspection::Private(root) => root,
        _ => panic!("fixture root must be private"),
    };
    let bootstrap = initialize_user_root(private).unwrap();
    (directory, root, bootstrap)
}

#[test]
fn hidden_invocations_require_an_explicit_valid_root_mode() {
    for (mode, expected) in [
        ("user", RootMode::User),
        ("development", RootMode::Development),
    ] {
        let arguments = [
            "--root-mode",
            mode,
            "--data-root",
            "/fixture",
            "--root-identity",
            "identity",
        ]
        .map(OsString::from)
        .to_vec();
        let (actual, root, identity) = parse_hidden_invocation(arguments).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(root, Path::new("/fixture"));
        assert_eq!(identity, "identity");
    }
    for arguments in [
        vec!["--data-root", "/fixture", "--root-identity", "identity"],
        vec![
            "--root-mode",
            "automatic",
            "--data-root",
            "/fixture",
            "--root-identity",
            "identity",
        ],
    ] {
        assert!(
            parse_hidden_invocation(arguments.into_iter().map(OsString::from).collect()).is_err()
        );
    }
}

#[test]
fn hidden_user_loader_rejects_wrong_mode_identity_and_running_origin() {
    let (_directory, root, bootstrap) = fixture();
    let identity = bootstrap.root().root_identity();
    load_hidden_bootstrap(RootMode::User, &root, identity).unwrap();
    assert!(load_hidden_bootstrap(RootMode::Development, &root, identity).is_err());
    assert!(load_hidden_bootstrap(RootMode::User, &root, "other-root").is_err());

    let other_root = root.with_file_name("other");
    fs::create_dir(&other_root).unwrap();
    fs::set_permissions(&other_root, fs::Permissions::from_mode(0o700)).unwrap();
    let other_origin = root.with_file_name("other-origin");
    fs::write(&other_origin, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&other_origin, fs::Permissions::from_mode(0o700)).unwrap();
    let private = match loxa_ipc::UserRootInspection::inspect(&other_root).unwrap() {
        loxa_ipc::UserRootInspection::Private(root) => root,
        _ => unreachable!(),
    };
    let other = initialize_user_root_from(private, &other_origin).unwrap();
    assert!(
        load_hidden_bootstrap(RootMode::User, &other_root, other.root().root_identity()).is_err()
    );
}

#[test]
fn user_service_and_legacy_runtime_share_the_existing_common_lock() {
    let (_directory, root, bootstrap) = fixture();
    let run = root.join("run");
    let descriptor = bootstrap.root().run_directory().unwrap();
    let owner = crate::runtime::RuntimeOwnership::acquire_service_unreconciled_at(descriptor, &run)
        .unwrap_or_else(|_| panic!("acquire service ownership"));
    let inode = fs::metadata(run.join("foreground.lock")).unwrap().ino();
    assert!(crate::runtime::RuntimeOwnership::acquire(&run).is_err());
    drop(owner);
    let legacy = crate::runtime::RuntimeOwnership::acquire(&run).unwrap();
    assert!(matches!(
        crate::runtime::RuntimeOwnership::acquire_service_unreconciled_at(descriptor, &run),
        Err(crate::runtime::RuntimeOwnershipAcquireError::Conflict)
    ));
    drop(legacy);
    let retained = b"retained prior lease";
    fs::write(run.join("foreground.json"), retained).unwrap();
    let owner = crate::runtime::RuntimeOwnership::acquire_service_unreconciled_at(descriptor, &run)
        .unwrap_or_else(|_| panic!("reacquire service ownership"));
    assert_eq!(
        fs::metadata(run.join("foreground.lock")).unwrap().ino(),
        inode
    );
    assert_eq!(fs::read(run.join("foreground.json")).unwrap(), retained);
    assert!(owner.audit_clean_for_service().is_err());
}

#[test]
fn descriptor_runtime_acquisition_cannot_create_a_lock_in_a_replacement_root() {
    let (_directory, root, bootstrap) = fixture();
    let original = root.with_file_name("original");
    fs::rename(&root, &original).unwrap();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let owner = crate::runtime::RuntimeOwnership::acquire_service_unreconciled_at(
        bootstrap.root().run_directory().unwrap(),
        &root.join("run"),
    )
    .unwrap_or_else(|_| panic!("acquire retained directory ownership"));
    assert!(original.join("run/foreground.lock").exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert!(bootstrap.root().validate_current().is_err());
    drop(owner);
}

#[test]
fn launch_root_handles_survive_descriptor_closure() {
    let (_directory, root, bootstrap) = fixture();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "service::tests::launch_root_handles_survive_descriptor_closure_child",
            "--nocapture",
        ])
        .env("LOXA_BOOTSTRAP_CHILD_ROOT", &root)
        .env(
            "LOXA_BOOTSTRAP_CHILD_IDENTITY",
            bootstrap.root().root_identity(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "launcher handle child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn launch_root_handles_survive_descriptor_closure_child() {
    let Some(root) = std::env::var_os("LOXA_BOOTSTRAP_CHILD_ROOT") else {
        return;
    };
    let identity = std::env::var("LOXA_BOOTSTRAP_CHILD_IDENTITY").unwrap();
    let bootstrap = load_launch_bootstrap(RootMode::User, Path::new(&root), &identity).unwrap();
    bootstrap.root().validate_current().unwrap();
    assert!(bootstrap
        .root()
        .run_directory()
        .unwrap()
        .metadata()
        .unwrap()
        .is_dir());
}
