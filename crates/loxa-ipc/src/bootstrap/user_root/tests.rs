use super::*;
use std::os::unix::fs::symlink;

fn fixture() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::Builder::new()
        .prefix("lr-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = fs::canonicalize(directory.path()).unwrap().join("normal");
    create_directory(&root, 0o755);
    (directory, root)
}

fn create_directory(path: &Path, mode: u32) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn repair(path: &Path) -> RootPermissionRepair {
    match UserRootInspection::inspect(path).unwrap() {
        UserRootInspection::RepairRequired(repair) => repair,
        UserRootInspection::Private(_) => panic!("expected a permission repair"),
    }
}

#[test]
fn inspection_and_declined_confirmation_leave_the_tree_unchanged() {
    let (_directory, root) = fixture();
    let child = root.join("models");
    create_directory(&child, 0o775);
    let file = child.join("model.gguf");
    fs::write(&file, b"unchanged model bytes").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o664)).unwrap();

    let inspected = UserRootInspection::inspect(&root).unwrap();
    let accepted = false;
    let private = match inspected {
        UserRootInspection::Private(private) => Some(private),
        UserRootInspection::RepairRequired(repair) if accepted => Some(repair.confirm().unwrap()),
        UserRootInspection::RepairRequired(_) => None,
    };

    assert!(private.is_none());
    assert_eq!(mode(&root), 0o755);
    assert_eq!(mode(&child), 0o775);
    assert_eq!(mode(&file), 0o664);
    assert_eq!(fs::read(&file).unwrap(), b"unchanged model bytes");
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn confirmed_repair_changes_only_the_retained_root() {
    let (_directory, root) = fixture();
    let child = root.join("run");
    create_directory(&child, 0o755);
    let file = child.join("retained-record");
    fs::write(&file, b"retained record bytes").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();

    let private = repair(&root).confirm().unwrap();

    private.validate_current().unwrap();
    assert_eq!(mode(&root), 0o700);
    assert_eq!(mode(&child), 0o755);
    assert_eq!(mode(&file), 0o644);
    assert_eq!(fs::read(&file).unwrap(), b"retained record bytes");
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn changed_permissions_require_a_new_confirmation() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    fs::set_permissions(&root, fs::Permissions::from_mode(0o775)).unwrap();

    assert!(matches!(
        pending.confirm(),
        Err(UserRootError::PermissionsChanged)
    ));
    assert_eq!(mode(&root), 0o775);
}

#[test]
fn an_independently_repaired_root_needs_no_further_mutation() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();

    pending.confirm().unwrap().validate_current().unwrap();
    assert_eq!(mode(&root), 0o700);
}

#[test]
fn replacing_the_root_before_confirmation_changes_neither_directory() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let original = root.with_file_name("original");
    fs::rename(&root, &original).unwrap();
    create_directory(&root, 0o755);

    assert!(matches!(
        pending.confirm(),
        Err(UserRootError::IdentityChanged)
    ));
    assert_eq!(mode(&original), 0o755);
    assert_eq!(mode(&root), 0o755);
}

#[test]
fn replacement_after_validation_never_receives_the_descriptor_chmod() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let original = root.with_file_name("original");

    let result = pending.confirm_with_before_chmod(|| {
        fs::rename(&root, &original).unwrap();
        create_directory(&root, 0o755);
    });

    assert!(matches!(result, Err(UserRootError::IdentityChanged)));
    assert_eq!(mode(&original), 0o700);
    assert_eq!(mode(&root), 0o755);
}

#[test]
fn private_root_validation_rejects_a_replacement() {
    let (_directory, root) = fixture();
    let private = repair(&root).confirm().unwrap();
    let original = root.with_file_name("original");
    fs::rename(&root, &original).unwrap();
    create_directory(&root, 0o700);

    assert!(matches!(
        private.validate_current(),
        Err(UserRootError::IdentityChanged)
    ));
    assert_eq!(mode(&original), 0o700);
    assert_eq!(mode(&root), 0o700);
}

#[test]
fn inspection_rejects_final_and_ancestor_symlinks() {
    let (_directory, root) = fixture();
    let alias = root.with_file_name("alias");
    symlink(&root, &alias).unwrap();
    assert!(matches!(
        UserRootInspection::inspect(&alias),
        Err(UserRootError::UnsafePath)
    ));

    let child = root.join("child");
    create_directory(&child, 0o755);
    assert!(matches!(
        UserRootInspection::inspect(&alias.join("child")),
        Err(UserRootError::UnsafePath)
    ));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(mode(&child), 0o755);
}

#[test]
fn confirmation_rejects_a_substituted_root_symlink() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let original = root.with_file_name("original");
    let outside = root.with_file_name("outside");
    create_directory(&outside, 0o755);
    fs::rename(&root, &original).unwrap();
    symlink(&outside, &root).unwrap();

    assert!(matches!(pending.confirm(), Err(UserRootError::UnsafePath)));
    assert_eq!(mode(&original), 0o755);
    assert_eq!(mode(&outside), 0o755);
}

#[test]
fn confirmation_rejects_a_substituted_ancestor_symlink() {
    let (_directory, ancestor) = fixture();
    let root = ancestor.join("root");
    create_directory(&root, 0o755);
    let pending = repair(&root);
    let original = ancestor.with_file_name("original");
    let outside = ancestor.with_file_name("outside");
    create_directory(&outside, 0o755);
    create_directory(&outside.join("root"), 0o755);
    fs::rename(&ancestor, &original).unwrap();
    symlink(&outside, &ancestor).unwrap();

    assert!(matches!(pending.confirm(), Err(UserRootError::UnsafePath)));
    assert_eq!(mode(&original.join("root")), 0o755);
    assert_eq!(mode(&outside.join("root")), 0o755);
}

#[test]
fn development_markers_block_inspection_and_a_pending_repair() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let marker = root.join(super::super::DEVELOPMENT_MARKER_FILENAME);
    symlink(root.join("missing-marker-target"), &marker).unwrap();

    assert!(matches!(
        UserRootInspection::inspect(&root),
        Err(UserRootError::DevelopmentRoot)
    ));
    assert!(matches!(
        pending.confirm(),
        Err(UserRootError::DevelopmentRoot)
    ));
    assert_eq!(mode(&root), 0o755);
}

#[test]
fn missing_invalid_and_file_roots_are_never_created_or_repaired() {
    let (_directory, root) = fixture();
    let missing = root.join("missing");
    assert!(
        matches!(UserRootInspection::inspect(&missing), Err(UserRootError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(!missing.exists());
    assert!(matches!(
        UserRootInspection::inspect(&root.join(".")),
        Err(UserRootError::InvalidPath)
    ));
    assert!(matches!(
        UserRootInspection::inspect(Path::new("/")),
        Err(UserRootError::InvalidPath)
    ));
    let file = root.join("file");
    fs::write(&file, b"unchanged bytes").unwrap();
    assert!(matches!(
        UserRootInspection::inspect(&file),
        Err(UserRootError::UnsafePath)
    ));
    assert_eq!(fs::read(&file).unwrap(), b"unchanged bytes");
    assert_eq!(mode(&root), 0o755);
}

#[test]
fn owner_validation_uses_descriptor_metadata() {
    let (_directory, root) = fixture();
    let directory = open_directory_path(&root).unwrap();
    assert!(matches!(
        RootIdentity::from_metadata(
            &directory.metadata().unwrap(),
            current_uid().wrapping_add(1)
        ),
        Err(UserRootError::ForeignOwner)
    ));
    assert_eq!(mode(&root), 0o755);
}

#[test]
fn nonsticky_writable_ancestry_is_rejected_without_changing_the_root() {
    let (_directory, root) = fixture();
    let parent = root.parent().unwrap();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o777)).unwrap();

    assert!(matches!(
        UserRootInspection::inspect(&root),
        Err(UserRootError::UnsafeAncestry)
    ));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(mode(parent), 0o777);
}

#[test]
fn trusted_sticky_and_normal_ancestry_are_admitted() {
    let (_directory, root) = fixture();
    let parent = root.parent().unwrap();
    for permissions in [0o755, 0o1777] {
        fs::set_permissions(parent, fs::Permissions::from_mode(permissions)).unwrap();
        let pending = repair(&root);
        drop(pending);
        assert_eq!(mode(parent), permissions);
        assert_eq!(mode(&root), 0o755);
    }
    repair(&root).confirm().unwrap().validate_current().unwrap();
}

#[test]
fn ancestry_permissions_are_revalidated_before_confirmation() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    fs::set_permissions(root.parent().unwrap(), fs::Permissions::from_mode(0o777)).unwrap();

    assert!(matches!(
        pending.confirm(),
        Err(UserRootError::UnsafeAncestry)
    ));
    assert_eq!(mode(&root), 0o755);

    fs::set_permissions(root.parent().unwrap(), fs::Permissions::from_mode(0o755)).unwrap();
    let private = repair(&root).confirm().unwrap();
    fs::set_permissions(root.parent().unwrap(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(matches!(
        private.validate_current(),
        Err(UserRootError::UnsafeAncestry)
    ));
    assert_eq!(mode(&root), 0o700);
}

#[test]
fn ancestor_owner_policy_trusts_only_root_and_the_current_user() {
    let foreign = if current_uid() == 1 { 2 } else { 1 };
    for permissions in [0o755, 0o1777] {
        assert!(matches!(
            validate_ancestor_permissions(foreign, permissions),
            Err(UserRootError::UnsafeAncestry)
        ));
        validate_ancestor_permissions(0, permissions).unwrap();
        validate_ancestor_permissions(current_uid(), permissions).unwrap();
    }
}

#[cfg(target_os = "macos")]
fn add_extended_acl(path: &Path) -> Vec<String> {
    add_acl(path, "everyone allow list,search")
}

#[cfg(target_os = "macos")]
fn add_acl(path: &Path, entry: &str) -> Vec<String> {
    let output = std::process::Command::new("/bin/chmod")
        .args(["+a", entry])
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "could not add fixture ACL: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let acl = extended_acl(path);
    assert!(!acl.is_empty(), "fixture ACL was not installed");
    acl
}

#[cfg(target_os = "macos")]
fn extended_acl(path: &Path) -> Vec<String> {
    let output = std::process::Command::new("/bin/ls")
        .arg("-lde")
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .skip(1)
        .map(str::to_owned)
        .collect()
}

#[cfg(target_os = "macos")]
#[test]
fn extended_acl_is_rejected_even_when_chmod_makes_the_mode_private() {
    let (_directory, root) = fixture();
    let acl = add_extended_acl(&root);

    assert!(matches!(
        UserRootInspection::inspect(&root),
        Err(UserRootError::ExtendedAcl)
    ));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(extended_acl(&root), acl);

    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();

    assert_eq!(mode(&root), 0o700);
    assert_eq!(extended_acl(&root), acl, "chmod removed the fixture ACL");
    assert!(matches!(
        UserRootInspection::inspect(&root),
        Err(UserRootError::ExtendedAcl)
    ));
    assert_eq!(extended_acl(&root), acl);
}

#[cfg(target_os = "macos")]
#[test]
fn acl_added_during_confirmation_leaves_mode_and_acl_unchanged() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let acl = add_extended_acl(&root);

    assert!(matches!(pending.confirm(), Err(UserRootError::ExtendedAcl)));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(extended_acl(&root), acl);
}

#[cfg(target_os = "macos")]
#[test]
fn private_root_revalidation_rejects_an_added_acl_without_mutation() {
    let (_directory, root) = fixture();
    let private = repair(&root).confirm().unwrap();
    let acl = add_extended_acl(&root);

    assert!(matches!(
        private.validate_current(),
        Err(UserRootError::ExtendedAcl)
    ));
    assert_eq!(mode(&root), 0o700);
    assert_eq!(extended_acl(&root), acl);
}

#[cfg(target_os = "macos")]
fn remove_fixture_acl(path: &Path) {
    assert!(std::process::Command::new("/bin/chmod")
        .arg("-N")
        .arg(path)
        .status()
        .unwrap()
        .success());
}

#[cfg(target_os = "macos")]
#[test]
fn ancestor_deny_delete_and_read_only_acls_preserve_normal_home_admission() {
    let (_directory, root) = fixture();
    let parent = root.parent().unwrap();
    add_acl(parent, "everyone deny delete");
    let acl = add_acl(parent, "everyone allow list,search,readattr,readsecurity");

    repair(&root).confirm().unwrap().validate_current().unwrap();

    assert_eq!(mode(&root), 0o700);
    assert_eq!(extended_acl(parent), acl);
    remove_fixture_acl(parent);
}

#[cfg(target_os = "macos")]
#[test]
fn sticky_ancestor_delete_child_acl_is_rejected_without_mutation() {
    let (_directory, root) = fixture();
    let parent = root.parent().unwrap();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o1777)).unwrap();
    let acl = add_acl(parent, "everyone allow delete_child");

    assert!(matches!(
        UserRootInspection::inspect(&root),
        Err(UserRootError::UnsafeAncestry)
    ));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(mode(parent), 0o1777);
    assert_eq!(extended_acl(parent), acl);
    remove_fixture_acl(parent);
}

#[cfg(target_os = "macos")]
#[test]
fn ancestor_acl_grant_added_during_confirmation_is_rejected() {
    let (_directory, root) = fixture();
    let pending = repair(&root);
    let parent = root.parent().unwrap();
    let acl = add_acl(parent, "everyone allow writesecurity");

    assert!(matches!(
        pending.confirm(),
        Err(UserRootError::UnsafeAncestry)
    ));
    assert_eq!(mode(&root), 0o755);
    assert_eq!(extended_acl(parent), acl);
    remove_fixture_acl(parent);
}

#[cfg(target_os = "macos")]
#[test]
fn ancestor_acl_parser_rejects_unknown_grants_and_malformed_payloads() {
    let mut payload = vec![0; 44 + 24];
    payload[0..4].copy_from_slice(&0x012c_c16d_u32.to_ne_bytes());
    payload[36..40].copy_from_slice(&1_u32.to_ne_bytes());
    payload[60..64].copy_from_slice(&2_u32.to_ne_bytes()); // Deny delete.
    payload[64..68].copy_from_slice(&(1_u32 << 4).to_ne_bytes());
    validate_ancestor_acl_payload(&payload).unwrap();
    payload[60..64].copy_from_slice(&1_u32.to_ne_bytes());
    assert!(matches!(
        validate_ancestor_acl_payload(&payload),
        Err(UserRootError::UnsafeAncestry)
    ));
    payload[64..68].copy_from_slice(&(1_u32 << 1).to_ne_bytes());
    validate_ancestor_acl_payload(&payload).unwrap();
    payload[64..68].copy_from_slice(&(1_u32 << 31).to_ne_bytes());
    assert!(validate_ancestor_acl_payload(&payload).is_err());
    payload[60..64].copy_from_slice(&3_u32.to_ne_bytes());
    assert!(validate_ancestor_acl_payload(&payload).is_err());
    assert!(validate_ancestor_acl_payload(&payload[..payload.len() - 1]).is_err());
    payload[36..40].copy_from_slice(&129_u32.to_ne_bytes());
    assert!(validate_ancestor_acl_payload(&payload).is_err());
}
