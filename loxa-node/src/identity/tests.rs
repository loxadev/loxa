#![cfg(unix)]

use super::unix::{cleanup_diagnostic_observed, inject_fault, FaultPoint};
use super::{open_or_create, IdentityErrorClass};
use loxa_protocol::NodeId;
use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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
