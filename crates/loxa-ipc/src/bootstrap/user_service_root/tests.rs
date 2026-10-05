use super::*;
use std::os::unix::fs::symlink;

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let directory = tempfile::Builder::new()
        .prefix("lu-")
        .tempdir_in("/tmp")
        .unwrap();
    let parent = fs::canonicalize(directory.path()).unwrap();
    let root = parent.join("normal");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let executable = parent.join("origin");
    fs::write(&executable, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    (directory, root, executable)
}

fn private(path: &Path) -> PrivateUserRoot {
    match UserRootInspection::inspect(path).unwrap() {
        UserRootInspection::Private(root) => root,
        UserRootInspection::RepairRequired(_) => panic!("fixture root is not private"),
    }
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().mode() & 0o7777
}

#[test]
fn user_initialization_preserves_safe_legacy_run_and_library_children() {
    let (_directory, root, executable) = fixture();
    fs::create_dir(root.join("run")).unwrap();
    fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o755)).unwrap();
    fs::create_dir(root.join("models")).unwrap();
    fs::write(root.join("models/model.gguf"), b"retained model").unwrap();
    fs::write(root.join("run/retained-state"), b"retained lease evidence").unwrap();

    let bootstrap = initialize(private(&root), &executable, "test-build").unwrap();

    assert_eq!(bootstrap.root().mode(), RootMode::User);
    assert_eq!(mode(&root.join("run")), 0o755);
    assert_eq!(mode(&root.join("run/service")), 0o700);
    assert_eq!(
        fs::read(root.join("models/model.gguf")).unwrap(),
        b"retained model"
    );
    assert_eq!(
        fs::read(root.join("run/retained-state")).unwrap(),
        b"retained lease evidence"
    );
    assert!(!root.join(DEVELOPMENT_MARKER_FILENAME).exists());
    assert_eq!(
        ClientBootstrap::load_user(&root)
            .unwrap()
            .root()
            .root_identity(),
        bootstrap.root().root_identity()
    );
    assert!(ClientBootstrap::load(&root, None).is_err());
}

#[test]
fn missing_service_directories_are_created_private_without_repairing_existing_ones() {
    let (_directory, root, executable) = fixture();
    initialize(private(&root), &executable, "test-build").unwrap();
    assert_eq!(mode(&root.join("run")), 0o700);
    assert_eq!(mode(&root.join("run/service")), 0o700);
    fs::set_permissions(root.join("run/service"), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(ClientBootstrap::load_user(&root).is_err());
    assert!(initialize(private(&root), &executable, "test-build").is_err());
    assert_eq!(mode(&root.join("run/service")), 0o755);
}

#[test]
fn user_and_development_modes_and_expected_identities_are_not_interchangeable() {
    let (_directory, root, executable) = fixture();
    let bootstrap = initialize(private(&root), &executable, "test-build").unwrap();
    assert!(ClientBootstrap::load_user_expected(&root, "different-root").is_err());
    let development = root.with_file_name("dev");
    super::super::initialize_development_root(&development, &root, &executable, "test-build")
        .unwrap();
    assert!(ClientBootstrap::load_user(&development).is_err());
    assert!(ClientBootstrap::load_expected(&root, None, bootstrap.root().root_identity()).is_err());
    assert!(
        super::super::initialize_development_root(&root, &root, &executable, "test-build").is_err()
    );
}

#[test]
fn existing_origin_is_never_updated_during_user_initialization() {
    let (_directory, root, executable) = fixture();
    initialize(private(&root), &executable, "test-build").unwrap();
    let origin = fs::read(root.join("run/service").join(ORIGIN_FILENAME)).unwrap();
    let other = root.with_file_name("other-origin");
    fs::write(&other, b"#!/bin/sh\nexit 1\n").unwrap();
    fs::set_permissions(&other, fs::Permissions::from_mode(0o700)).unwrap();

    assert!(initialize(private(&root), &other, "test-build").is_err());
    assert!(initialize(private(&root), &executable, "other-build").is_err());
    assert_eq!(
        fs::read(root.join("run/service").join(ORIGIN_FILENAME)).unwrap(),
        origin
    );
    initialize(private(&root), &executable, "test-build").unwrap();
}

#[test]
fn origin_only_retry_preserves_the_published_record_after_marker_failure() {
    let (_directory, root, executable) = fixture();
    let state = UserServiceRoot::open(private(&root), true).unwrap();
    let instance = state.acquire_instance().unwrap();
    let requested = OriginRecord::from_executable(&executable, "test-build").unwrap();
    write_record_at(&state.control, ORIGIN_FILENAME, &requested).unwrap();
    let origin_path = root.join("run/service").join(ORIGIN_FILENAME);
    let bytes = fs::read(&origin_path).unwrap();
    let inode = fs::metadata(&origin_path).unwrap().ino();
    let marker_path = root.join(USER_SERVICE_MARKER_FILENAME);
    fs::write(&marker_path, b"fixture publication collision").unwrap();
    assert!(write_record_at(
        state.root.directory(),
        USER_SERVICE_MARKER_FILENAME,
        &UserMarker {
            schema_version: MARKER_SCHEMA,
            root_identity: state.paths.root_identity.clone(),
        },
    )
    .is_err());
    assert_eq!(
        fs::read(&marker_path).unwrap(),
        b"fixture publication collision"
    );
    fs::remove_file(&marker_path).unwrap();
    drop(instance);
    drop(state);

    initialize(private(&root), &executable, "test-build").unwrap();

    assert_eq!(fs::read(&origin_path).unwrap(), bytes);
    assert_eq!(fs::metadata(&origin_path).unwrap().ino(), inode);
    assert!(marker_path.is_file());
    ClientBootstrap::load_user(&root).unwrap();
}

#[test]
fn origin_only_retry_rejects_different_origins_and_unexpected_entries() {
    let (_directory, root, executable) = fixture();
    initialize(private(&root), &executable, "test-build").unwrap();
    fs::remove_file(root.join(USER_SERVICE_MARKER_FILENAME)).unwrap();
    let origin_path = root.join("run/service").join(ORIGIN_FILENAME);
    let bytes = fs::read(&origin_path).unwrap();
    assert!(initialize(private(&root), &executable, "other-build").is_err());
    assert!(!root.join(USER_SERVICE_MARKER_FILENAME).exists());
    fs::write(root.join("run/service/unexpected"), b"retained evidence").unwrap();
    assert!(initialize(private(&root), &executable, "test-build").is_err());
    assert_eq!(fs::read(&origin_path).unwrap(), bytes);
    assert!(!root.join(USER_SERVICE_MARKER_FILENAME).exists());
}

#[test]
fn origin_only_retry_requires_a_private_validated_record() {
    let (_directory, root, executable) = fixture();
    initialize(private(&root), &executable, "test-build").unwrap();
    fs::remove_file(root.join(USER_SERVICE_MARKER_FILENAME)).unwrap();
    let origin_path = root.join("run/service").join(ORIGIN_FILENAME);
    let bytes = fs::read(&origin_path).unwrap();
    fs::set_permissions(&origin_path, fs::Permissions::from_mode(0o644)).unwrap();

    assert!(initialize(private(&root), &executable, "test-build").is_err());

    assert_eq!(mode(&origin_path), 0o644);
    assert_eq!(fs::read(&origin_path).unwrap(), bytes);
    assert!(!root.join(USER_SERVICE_MARKER_FILENAME).exists());
}

#[test]
fn cloned_bootstrap_retains_handles_and_launches_with_explicit_user_mode() {
    let (_directory, root, executable) = fixture();
    let arguments = root.with_file_name("launch-arguments");
    assert!(!arguments.to_string_lossy().contains('\''));
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
            arguments.display()
        ),
    )
    .unwrap();
    let bootstrap = initialize(private(&root), &executable, "test-build").unwrap();
    let clone = bootstrap.clone();
    drop(bootstrap);
    let run = clone.root().run_directory().unwrap();
    // SAFETY: run is a live retained descriptor; F_GETFD has no additional input.
    let flags = unsafe { libc::fcntl(run.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0);
    clone.launch_service().unwrap();
    clone.root().validate_current().unwrap();
    assert_eq!(
        fs::read_to_string(arguments).unwrap(),
        format!(
            "__service-launch\n--root-mode\nuser\n--data-root\n{}\n--root-identity\n{}\n",
            root.display(),
            clone.root().root_identity()
        )
    );
}

#[test]
fn user_instance_lock_is_exclusive_and_reuses_the_same_inode() {
    let (_directory, root, executable) = fixture();
    let bootstrap = initialize(private(&root), &executable, "test-build").unwrap();
    let inode = fs::metadata(root.join("run/service").join(INSTANCE_LOCK_FILENAME))
        .unwrap()
        .ino();
    let instance = bootstrap.root().acquire_instance().unwrap();
    assert!(bootstrap.clone().root().acquire_instance().is_err());
    assert!(initialize(private(&root), &executable, "test-build").is_err());
    drop(instance);
    drop(bootstrap.root().acquire_instance().unwrap());
    assert_eq!(
        fs::metadata(root.join("run/service").join(INSTANCE_LOCK_FILENAME))
            .unwrap()
            .ino(),
        inode
    );
}

#[test]
fn root_and_control_replacements_cannot_receive_bootstrap_mutations() {
    let (_directory, root, executable) = fixture();
    let retained = private(&root);
    let original = root.with_file_name("original");
    fs::rename(&root, &original).unwrap();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(initialize(retained, &executable, "test-build").is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);

    let bootstrap = initialize(private(&root), &executable, "test-build").unwrap();
    let control = root.join("run/service");
    fs::rename(&control, root.join("run/original-service")).unwrap();
    fs::create_dir(&control).unwrap();
    fs::set_permissions(&control, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(bootstrap.root().acquire_instance().is_err());
    assert_eq!(fs::read_dir(&control).unwrap().count(), 0);
}

#[test]
fn descriptor_record_publication_never_writes_into_a_replacement_root() {
    let (_directory, root, _executable) = fixture();
    let state = UserServiceRoot::open(private(&root), true).unwrap();
    fs::rename(&root, root.with_file_name("original")).unwrap();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    write_record_at(
        &state.control,
        "test-record.json",
        &serde_json::json!({"retained": true}),
    )
    .unwrap();
    assert!(root
        .with_file_name("original")
        .join("run/service/test-record.json")
        .exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert!(state.validate_directories().is_err());
}

#[test]
fn unsafe_run_and_symlink_controls_are_rejected_without_chmod_or_following() {
    let (_directory, root, executable) = fixture();
    fs::create_dir(root.join("run")).unwrap();
    fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(initialize(private(&root), &executable, "test-build").is_err());
    assert_eq!(mode(&root.join("run")), 0o777);
    fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o755)).unwrap();
    let outside = root.with_file_name("outside");
    fs::create_dir(&outside).unwrap();
    symlink(&outside, root.join("run/service")).unwrap();
    assert!(initialize(private(&root), &executable, "test-build").is_err());
    assert_eq!(fs::read_dir(outside).unwrap().count(), 0);
}
