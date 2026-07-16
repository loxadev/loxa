#![cfg(unix)]

use super::unix::{
    cleanup_diagnostic_observed, inject_boundary_hook, inject_fault, inject_repeated_fault,
    inject_stat_override, BoundaryPoint, FaultPoint, StatOverride, StatTarget,
};
use super::{open_or_create, IdentityErrorClass};
use loxa_protocol::NodeId;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let suffix = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "loxa-identity-test-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn identity(&self) -> PathBuf {
        self.0.join("identity")
    }

    fn primary(&self) -> PathBuf {
        self.identity().join("node.json")
    }

    fn backup(&self) -> PathBuf {
        self.identity().join("node.json.bak")
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn canonical(id: NodeId) -> Vec<u8> {
    format!("{{\"schema_version\":1,\"node_id\":\"{id}\"}}\n").into_bytes()
}

fn write_record(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn temporary_paths(root: &TestRoot) -> Vec<PathBuf> {
    if !root.identity().is_dir() {
        return Vec::new();
    }
    fs::read_dir(root.identity())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b".node.json.tmp-")
        })
        .collect()
}

#[test]
fn fresh_create_writes_two_canonical_records_and_reopens_same_id() {
    let root = TestRoot::new();

    let created = open_or_create(root.path()).unwrap();
    let primary = fs::read(root.primary()).unwrap();
    let backup = fs::read(root.backup()).unwrap();

    assert_eq!(primary, canonical(created));
    assert_eq!(backup, primary);
    assert_eq!(
        fs::metadata(root.identity()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(root.primary()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(open_or_create(root.path()).unwrap(), created);
}

#[test]
fn missing_copy_is_repaired_from_the_valid_copy() {
    for missing_primary in [true, false] {
        let root = TestRoot::new();
        let id = open_or_create(root.path()).unwrap();
        let missing = if missing_primary {
            root.primary()
        } else {
            root.backup()
        };
        fs::remove_file(missing).unwrap();

        assert_eq!(open_or_create(root.path()).unwrap(), id);
        assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
        assert_eq!(fs::read(root.backup()).unwrap(), canonical(id));
    }
}

#[test]
fn present_corruption_fails_closed_in_both_asymmetric_cases() {
    for corrupt_primary in [true, false] {
        let root = TestRoot::new();
        let id = open_or_create(root.path()).unwrap();
        let corrupt = if corrupt_primary {
            root.primary()
        } else {
            root.backup()
        };
        write_record(&corrupt, b"{not-json}\n");

        let error = open_or_create(root.path()).unwrap_err();

        assert_eq!(error.class(), IdentityErrorClass::Corrupt);
        let valid = if corrupt_primary {
            root.backup()
        } else {
            root.primary()
        };
        assert_eq!(fs::read(valid).unwrap(), canonical(id));
        assert_eq!(fs::read(corrupt).unwrap(), b"{not-json}\n");
    }
}

#[test]
fn conflicting_valid_records_fail_without_mutation() {
    let root = TestRoot::new();
    let first = open_or_create(root.path()).unwrap();
    let second = NodeId::from_str("123e4567-e89b-42d3-a456-426614174000").unwrap();
    write_record(&root.backup(), &canonical(second));

    let error = open_or_create(root.path()).unwrap_err();

    assert_eq!(error.class(), IdentityErrorClass::Conflict);
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(first));
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(second));
}

#[test]
fn schema_and_canonical_failures_have_stable_sanitized_classes() {
    let cases: &[(&[u8], IdentityErrorClass)] = &[
        (b"{\"schema_version\":2,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"}\n", IdentityErrorClass::SchemaUnsupported),
        (b"{\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\",\"schema_version\":1}\n", IdentityErrorClass::Corrupt),
        (b"{\"schema_version\":1,\"schema_version\":1,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"}\n", IdentityErrorClass::Corrupt),
        (b"{\"schema_version\":1,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\",\"extra\":true}\n", IdentityErrorClass::Corrupt),
    ];

    for (bytes, expected) in cases {
        let root = TestRoot::new();
        fs::create_dir(root.identity()).unwrap();
        fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
        write_record(&root.primary(), bytes);

        let error = open_or_create(root.path()).unwrap_err();

        assert_eq!(error.class(), *expected);
        assert_eq!(error.to_string(), expected.as_str());
        assert!(!error.to_string().contains(root.path().to_str().unwrap()));
    }
}

#[test]
fn parser_rejects_bounds_encoding_truncation_and_non_v4_ids() {
    let cases: Vec<Vec<u8>> = vec![
        vec![b'x'; 4097],
        b"{\"schema_version\":1,\"node_id\":\"\xff\"}\n".to_vec(),
        b"{\"schema_version\":1,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"".to_vec(),
        b"{\"schema_version\":1,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"}\n ".to_vec(),
        b"{\"schema_version\":1,\"node_id\":\"123E4567-E89B-42D3-A456-426614174000\"}\n".to_vec(),
        b"{\"schema_version\":1,\"node_id\":\"123e4567-e89b-12d3-a456-426614174000\"}\n".to_vec(),
        b"{\"schema_version\":1,\"node_id\":\"00000000-0000-0000-0000-000000000000\"}\n".to_vec(),
    ];

    for bytes in cases {
        let root = TestRoot::new();
        fs::create_dir(root.identity()).unwrap();
        fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
        write_record(&root.primary(), &bytes);

        assert_eq!(
            open_or_create(root.path()).unwrap_err().class(),
            IdentityErrorClass::Corrupt
        );
        assert_eq!(fs::read(root.primary()).unwrap(), bytes);
        assert!(!root.backup().exists());
    }
}

#[test]
fn unsafe_record_types_and_file_as_directory_fail_closed() {
    let file_root = TestRoot::new();
    fs::write(file_root.identity(), b"not a directory").unwrap();
    assert_eq!(
        open_or_create(file_root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeDirectory
    );

    for kind in ["directory", "fifo", "socket"] {
        let root = TestRoot::new();
        fs::create_dir(root.identity()).unwrap();
        fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
        match kind {
            "directory" => fs::create_dir(root.primary()).unwrap(),
            "fifo" => {
                let path = std::ffi::CString::new(root.primary().as_os_str().as_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            }
            "socket" => {
                let _listener = UnixListener::bind(root.primary()).unwrap();
                assert_eq!(
                    open_or_create(root.path()).unwrap_err().class(),
                    IdentityErrorClass::UnsafeRecord
                );
                continue;
            }
            _ => unreachable!(),
        }
        assert_eq!(
            open_or_create(root.path()).unwrap_err().class(),
            IdentityErrorClass::UnsafeRecord,
            "{kind}"
        );
    }
}

#[test]
fn captured_stat_rejects_wrong_owner_and_device_evidence() {
    assert_eq!(
        fs::metadata("/dev/null").unwrap().mode() & u32::from(libc::S_IFMT),
        u32::from(libc::S_IFCHR)
    );
    for (target, stat_override, expected) in [
        (
            StatTarget::Root,
            StatOverride::WrongOwner,
            IdentityErrorClass::UnsafeRoot,
        ),
        (
            StatTarget::IdentityDirectory,
            StatOverride::WrongOwner,
            IdentityErrorClass::UnsafeDirectory,
        ),
        (
            StatTarget::PrimaryRecord,
            StatOverride::WrongOwner,
            IdentityErrorClass::UnsafeRecord,
        ),
        (
            StatTarget::PrimaryRecord,
            StatOverride::Device,
            IdentityErrorClass::UnsafeRecord,
        ),
    ] {
        let root = TestRoot::new();
        let _ = open_or_create(root.path()).unwrap();
        inject_stat_override(target, stat_override);
        assert_eq!(open_or_create(root.path()).unwrap_err().class(), expected);
    }
}

#[test]
fn unsafe_precedes_io_unsupported_and_corrupt_observations() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    fs::set_permissions(root.primary(), fs::Permissions::from_mode(0o644)).unwrap();
    write_record(
        &root.backup(),
        b"{\"schema_version\":2,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"}\n",
    );
    inject_fault(FaultPoint::BackupRead);
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
}

#[test]
fn io_precedes_unsupported_schema_and_corrupt_observations() {
    for backup in [
        b"{\"schema_version\":2,\"node_id\":\"123e4567-e89b-42d3-a456-426614174000\"}\n".as_slice(),
        b"{not-json}\n".as_slice(),
    ] {
        let root = TestRoot::new();
        fs::create_dir(root.identity()).unwrap();
        fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
        write_record(&root.primary(), &canonical(NodeId::new_v4()));
        write_record(&root.backup(), backup);
        inject_fault(FaultPoint::PrimaryRead);
        assert_eq!(
            open_or_create(root.path()).unwrap_err().class(),
            IdentityErrorClass::Io
        );
    }
}

#[test]
fn unsafe_root_directory_and_record_permissions_fail_closed() {
    let root = TestRoot::new();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o722)).unwrap();
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRoot
    );

    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let id = open_or_create(root.path()).unwrap();
    fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeDirectory
    );

    fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(root.primary(), fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(id));
}

#[test]
fn deterministic_root_and_identity_path_swaps_fail_closed() {
    let root = TestRoot::new();
    let original_root = root.path().with_extension("original");
    let root_path = root.path().to_owned();
    inject_boundary_hook(BoundaryPoint::RootRevalidate, move || {
        fs::rename(&root_path, &original_root).unwrap();
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
    });
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRoot
    );
    assert!(!root.identity().exists());

    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let original_identity = root.path().join("identity-original");
    let identity_path = root.identity();
    inject_boundary_hook(BoundaryPoint::DirectoryRevalidate, move || {
        fs::rename(&identity_path, &original_identity).unwrap();
        fs::create_dir(&identity_path).unwrap();
        fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o700)).unwrap();
    });
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeDirectory
    );
    assert_eq!(
        fs::read(root.path().join("identity-original/node.json")).unwrap(),
        canonical(id)
    );
    assert!(!root.primary().exists());
}

#[test]
fn three_consecutive_destination_changes_are_bounded_without_publication() {
    let root = TestRoot::new();
    inject_repeated_fault(FaultPoint::DestinationContention, 3);

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::ConcurrentChange
    );
    assert!(!root.primary().exists());
    assert!(!root.backup().exists());
    assert!(temporary_paths(&root).is_empty());

    let id = open_or_create(root.path()).unwrap();
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(id));
}

#[test]
fn every_publication_fault_has_exact_post_state_and_safe_retry() {
    let cases = [
        (FaultPoint::Mkdir, "no-directory"),
        (FaultPoint::RootSync, "empty-directory"),
        (FaultPoint::DirectoryReopen, "empty-directory"),
        (FaultPoint::FileWrite, "empty-temp"),
        (FaultPoint::PartialWrite, "partial-temp"),
        (FaultPoint::FileSync, "full-temp"),
        (FaultPoint::Publish, "full-temp"),
        (FaultPoint::PostLink, "linked-temp"),
        (FaultPoint::Unlink, "linked-temp"),
        (FaultPoint::DirectorySync, "primary-only"),
        (FaultPoint::Reopen, "complete"),
    ];

    for (fault, state) in cases {
        let root = TestRoot::new();
        inject_fault(fault);
        assert!(open_or_create(root.path()).is_err(), "{fault:?}");
        let temporaries = temporary_paths(&root);
        match state {
            "no-directory" => assert!(!root.identity().exists()),
            "empty-directory" => {
                assert!(root.identity().is_dir());
                assert!(temporaries.is_empty());
                assert!(!root.primary().exists());
                assert!(!root.backup().exists());
            }
            "empty-temp" | "partial-temp" | "full-temp" => {
                assert_eq!(temporaries.len(), 1, "{fault:?}");
                assert!(!root.primary().exists());
                assert!(!root.backup().exists());
                let length = fs::metadata(&temporaries[0]).unwrap().len();
                let canonical_length =
                    canonical(NodeId::from_str("123e4567-e89b-42d3-a456-426614174000").unwrap())
                        .len() as u64;
                match state {
                    "empty-temp" => assert_eq!(length, 0),
                    "partial-temp" => assert_eq!(length, canonical_length / 2),
                    "full-temp" => assert_eq!(length, canonical_length),
                    _ => unreachable!(),
                }
                assert_eq!(fs::metadata(&temporaries[0]).unwrap().nlink(), 1);
            }
            "linked-temp" => {
                assert_eq!(temporaries.len(), 1, "{fault:?}");
                assert!(root.primary().exists());
                assert!(!root.backup().exists());
                assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 2);
                assert_eq!(
                    fs::metadata(root.primary()).unwrap().ino(),
                    fs::metadata(&temporaries[0]).unwrap().ino()
                );
            }
            "primary-only" => {
                assert!(temporaries.is_empty());
                assert!(root.primary().exists());
                assert!(!root.backup().exists());
                assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 1);
            }
            "complete" => {
                assert!(temporaries.is_empty());
                assert_eq!(
                    fs::read(root.primary()).unwrap(),
                    fs::read(root.backup()).unwrap()
                );
            }
            _ => unreachable!(),
        }

        let id = open_or_create(root.path()).unwrap();
        assert_eq!(
            fs::read(root.primary()).unwrap(),
            canonical(id),
            "{fault:?}"
        );
        assert_eq!(fs::read(root.backup()).unwrap(), canonical(id), "{fault:?}");
        assert!(temporary_paths(&root).is_empty(), "{fault:?}");
    }
}

#[test]
fn symlinked_root_directory_and_record_fail_closed() {
    let real_root = TestRoot::new();
    let link_parent = TestRoot::new();
    let root_link = link_parent.path().join("root-link");
    symlink(real_root.path(), &root_link).unwrap();
    assert_eq!(
        open_or_create(&root_link).unwrap_err().class(),
        IdentityErrorClass::UnsafeRoot
    );

    let directory_root = TestRoot::new();
    symlink(real_root.path(), directory_root.identity()).unwrap();
    assert_eq!(
        open_or_create(directory_root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeDirectory
    );

    let record_root = TestRoot::new();
    fs::create_dir(record_root.identity()).unwrap();
    fs::set_permissions(record_root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
    symlink(real_root.path(), record_root.primary()).unwrap();
    assert_eq!(
        open_or_create(record_root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
}

#[test]
fn unrecognized_hard_link_is_rejected_without_removal() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let unexpected = root.identity().join("unexpected-link");
    fs::hard_link(root.primary(), &unexpected).unwrap();

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert_eq!(fs::read(&unexpected).unwrap(), canonical(id));
    assert!(root.primary().exists());
}

#[test]
fn recognized_post_link_crash_is_recovered_exactly() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let temporary = root
        .identity()
        .join(format!(".node.json.tmp-{}-recovery", std::process::id()));
    fs::hard_link(root.primary(), &temporary).unwrap();

    assert_eq!(open_or_create(root.path()).unwrap(), id);
    assert!(!temporary.exists());
    assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 1);
}

#[test]
fn interrupted_publication_rejects_multiple_or_extra_links_without_mutation() {
    for recognized_second_link in [true, false] {
        let root = TestRoot::new();
        let id = open_or_create(root.path()).unwrap();
        let first = root
            .identity()
            .join(format!(".node.json.tmp-{}-first", std::process::id()));
        let second = if recognized_second_link {
            root.identity()
                .join(format!(".node.json.tmp-{}-second", std::process::id()))
        } else {
            root.identity().join("unexpected-extra-link")
        };
        fs::hard_link(root.primary(), &first).unwrap();
        fs::hard_link(root.primary(), &second).unwrap();

        assert_eq!(
            open_or_create(root.path()).unwrap_err().class(),
            IdentityErrorClass::UnsafeRecord
        );
        assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 3);
        assert_eq!(fs::read(&first).unwrap(), canonical(id));
        assert_eq!(fs::read(&second).unwrap(), canonical(id));
    }
}

#[test]
fn interrupted_publication_rejects_temp_name_swap() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let temporary = root
        .identity()
        .join(format!(".node.json.tmp-{}-swap", std::process::id()));
    fs::hard_link(root.primary(), &temporary).unwrap();
    let swapped_path = temporary.clone();
    inject_boundary_hook(BoundaryPoint::RecoveryUnlink, move || {
        fs::remove_file(&swapped_path).unwrap();
        write_record(&swapped_path, b"swapped");
    });

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
    assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 1);
    assert_eq!(fs::read(&temporary).unwrap(), b"swapped");
}

#[test]
fn interrupted_publication_sync_failure_has_exact_recoverable_state() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let temporary = root
        .identity()
        .join(format!(".node.json.tmp-{}-sync", std::process::id()));
    fs::hard_link(root.primary(), &temporary).unwrap();
    inject_fault(FaultPoint::RecoverySync);

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::Durability
    );
    assert!(!temporary.exists());
    assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 1);
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
    assert_eq!(open_or_create(root.path()).unwrap(), id);
}

#[test]
fn unsafe_unrelated_recognized_temp_blocks_two_link_recovery() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let matching = root
        .identity()
        .join(format!(".node.json.tmp-{}-matching", std::process::id()));
    let unsafe_temp = root
        .identity()
        .join(format!(".node.json.tmp-{}-bad-mode", std::process::id()));
    fs::hard_link(root.primary(), &matching).unwrap();
    write_record(&unsafe_temp, b"different");
    fs::set_permissions(&unsafe_temp, fs::Permissions::from_mode(0o644)).unwrap();

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert!(matching.exists());
    assert!(unsafe_temp.exists());
    assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 2);
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
}

#[test]
fn recoverable_two_link_allows_safe_unrelated_temp_cleanup() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let matching = root.identity().join(format!(
        ".node.json.tmp-{}-matching-safe",
        std::process::id()
    ));
    let unrelated = root
        .identity()
        .join(format!(".node.json.tmp-{}-unrelated", std::process::id()));
    fs::hard_link(root.primary(), &matching).unwrap();
    write_record(&unrelated, b"different inode and bytes");

    assert_eq!(open_or_create(root.path()).unwrap(), id);
    assert!(!matching.exists());
    assert!(!unrelated.exists());
    assert_eq!(fs::metadata(root.primary()).unwrap().nlink(), 1);
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
}

#[test]
fn captured_stat_rejects_wrong_owner_temporary_evidence() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let stale = root
        .identity()
        .join(format!(".node.json.tmp-{}-owner", std::process::id()));
    write_record(&stale, &canonical(id));
    inject_stat_override(StatTarget::TemporaryRecord, StatOverride::WrongOwner);

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert!(stale.exists());
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
}

#[test]
fn external_restore_of_both_or_either_copy_preserves_identity() {
    for restored_copy in ["both", "primary", "backup"] {
        let root = TestRoot::new();
        let id = open_or_create(root.path()).unwrap();
        let bytes = fs::read(root.primary()).unwrap();
        fs::remove_dir_all(root.identity()).unwrap();
        fs::create_dir(root.identity()).unwrap();
        fs::set_permissions(root.identity(), fs::Permissions::from_mode(0o700)).unwrap();
        if restored_copy != "backup" {
            write_record(&root.primary(), &bytes);
        }
        if restored_copy != "primary" {
            write_record(&root.backup(), &bytes);
        }

        assert_eq!(open_or_create(root.path()).unwrap(), id, "{restored_copy}");
        assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
        assert_eq!(fs::read(root.backup()).unwrap(), canonical(id));
    }
}

#[test]
fn stale_recognized_temp_is_cleaned_but_unrecognized_entry_is_preserved() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let stale = root
        .identity()
        .join(format!(".node.json.tmp-{}-stale", std::process::id()));
    write_record(&stale, &canonical(id));
    let unrecognized = root.identity().join("keep-me");
    write_record(&unrecognized, b"private data");

    assert_eq!(open_or_create(root.path()).unwrap(), id);
    assert!(!stale.exists());
    assert!(unrecognized.exists());
    assert_eq!(fs::metadata(unrecognized).unwrap().nlink(), 1);
}

#[test]
fn unsafe_recognized_temp_fails_closed() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let unsafe_temp = root
        .identity()
        .join(format!(".node.json.tmp-{}-unsafe", std::process::id()));
    symlink(root.primary(), &unsafe_temp).unwrap();

    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::UnsafeRecord
    );
    assert!(unsafe_temp.symlink_metadata().is_ok());
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
}

#[test]
fn durability_faults_return_stable_classes_and_never_overwrite_committed_identity() {
    let fresh_cases = [
        (FaultPoint::Mkdir, IdentityErrorClass::Io),
        (FaultPoint::RootSync, IdentityErrorClass::Durability),
        (
            FaultPoint::DirectoryReopen,
            IdentityErrorClass::UnsafeDirectory,
        ),
        (FaultPoint::FileWrite, IdentityErrorClass::Io),
        (FaultPoint::PartialWrite, IdentityErrorClass::Io),
        (FaultPoint::FileSync, IdentityErrorClass::Durability),
        (FaultPoint::Publish, IdentityErrorClass::Io),
        (FaultPoint::PostLink, IdentityErrorClass::Durability),
        (FaultPoint::Unlink, IdentityErrorClass::Durability),
        (FaultPoint::DirectorySync, IdentityErrorClass::Durability),
        (FaultPoint::Reopen, IdentityErrorClass::Io),
    ];

    for (fault, expected) in fresh_cases {
        let root = TestRoot::new();
        inject_fault(fault);
        let error = match open_or_create(root.path()) {
            Err(error) => error,
            Ok(_) => panic!("fault {fault:?} did not fail"),
        };
        assert_eq!(error.class(), expected, "fault {fault:?}");
    }

    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    fs::remove_file(root.backup()).unwrap();
    inject_fault(FaultPoint::FileWrite);
    assert_eq!(
        open_or_create(root.path()).unwrap_err().class(),
        IdentityErrorClass::Io
    );
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(id));
    assert!(!root.backup().exists());
}

#[test]
fn stale_cleanup_failure_is_nonfatal_and_emits_only_static_signal() {
    let root = TestRoot::new();
    let id = open_or_create(root.path()).unwrap();
    let stale = root
        .identity()
        .join(format!(".node.json.tmp-{}-cleanup", std::process::id()));
    write_record(&stale, &canonical(id));
    inject_fault(FaultPoint::Cleanup);

    assert_eq!(open_or_create(root.path()).unwrap(), id);
    assert!(stale.exists());
    assert!(cleanup_diagnostic_observed());
}

#[test]
fn concurrent_creators_return_one_validated_winner() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let root = TestRoot::new();
    let path = Arc::new(root.path().to_owned());
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                open_or_create(&path)
            })
        })
        .collect();
    let ids: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect();

    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(ids[0]));
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(ids[0]));
}

#[test]
fn higher_contention_creators_return_one_validated_winner() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let root = TestRoot::new();
    let path = Arc::new(root.path().to_owned());
    let barrier = Arc::new(Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                open_or_create(&path)
            })
        })
        .collect();
    let ids: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect();

    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(ids[0]));
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(ids[0]));
    assert!(temporary_paths(&root).is_empty());
}

#[test]
fn cross_process_identity_helper() {
    let Some(path) = std::env::var_os("LOXA_IDENTITY_CROSS_PROCESS_ROOT") else {
        return;
    };
    let id = open_or_create(Path::new(&path)).unwrap();
    println!("CROSS_PROCESS_ID={id}");
}

#[test]
fn cross_process_creators_return_one_validated_winner() {
    use std::process::{Command, Stdio};

    let root = TestRoot::new();
    let executable = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..8)
        .map(|_| {
            Command::new(&executable)
                .args([
                    "identity::tests::cross_process_identity_helper",
                    "--exact",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("LOXA_IDENTITY_CROSS_PROCESS_ROOT", root.path())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let outputs: Vec<_> = children
        .drain(..)
        .map(|child| child.wait_with_output().unwrap())
        .collect();
    for output in &outputs {
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let ids: Vec<_> = outputs
        .iter()
        .map(|output| {
            let stdout = String::from_utf8(output.stdout.clone()).unwrap();
            let marker = stdout.find("CROSS_PROCESS_ID=").unwrap();
            let id = &stdout
                [marker + "CROSS_PROCESS_ID=".len()..marker + "CROSS_PROCESS_ID=".len() + 36];
            NodeId::from_str(id).unwrap()
        })
        .collect();

    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(fs::read(root.primary()).unwrap(), canonical(ids[0]));
    assert_eq!(fs::read(root.backup()).unwrap(), canonical(ids[0]));
    assert!(temporary_paths(&root).is_empty());
}
