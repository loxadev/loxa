use crate::artifact_coordinator::{ArtifactAcquireError, ArtifactKey, ArtifactMutationCoordinator};
use crate::download_scheduler::DownloadKey;
use crate::operation_cancellation::OperationCancellation;
use crate::verification_scheduler::{
    DownloadCompletionQueue, DownloadVerificationOutcome, DownloadVerificationOwnership,
    VerificationResult,
};
use loxa_protocol::v2::{DecimalU64, OperationId};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct TestDir(PathBuf);

impl TestDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-slice4-{label}-{}-{}",
            std::process::id(),
            OperationId::new_v4()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn artifact_key(root: &std::path::Path, name: &str) -> ArtifactKey {
    ArtifactKey::from_destination(&root.join(name)).expect("canonical artifact key")
}

fn download_key(model_id: &str, source: &str, artifact: ArtifactKey) -> DownloadKey {
    DownloadKey::new(
        model_id,
        "hugging-face",
        source,
        Some("0123456789abcdef0123456789abcdef01234567"),
        "weights/model.gguf",
        Some([7; 32]),
        Some(42),
        artifact,
    )
    .expect("valid closed download identity")
}

#[test]
fn canonical_keys_separate_public_aliases_but_share_destination_exclusion() {
    let dir = TestDir::new("identity");
    let artifact = artifact_key(dir.path(), "model.gguf");
    let same = download_key("coding", "publisher/repository", artifact.clone());
    let identical = download_key("coding", "publisher/repository", artifact.clone());
    let alias = download_key("coding-fast", "publisher/repository", artifact.clone());

    assert_eq!(same, identical);
    assert_ne!(same, alias);
    assert_eq!(same.artifact(), alias.artifact());
}

#[test]
fn canonical_key_rejects_ambiguous_or_secret_bearing_source_identity() {
    let dir = TestDir::new("ambiguous");
    let artifact = artifact_key(dir.path(), "model.gguf");
    for source in [
        "",
        "https://example.invalid/model?token=secret",
        "publisher/repository?download=true",
        "publisher/repository#mutable",
        "user:password@repository",
    ] {
        assert!(DownloadKey::new(
            "coding",
            "hugging-face",
            source,
            Some("0123456789abcdef0123456789abcdef01234567"),
            "weights/model.gguf",
            Some([7; 32]),
            Some(42),
            artifact.clone(),
        )
        .is_err());
    }
}

#[test]
fn source_identity_requires_the_closed_recipe_grammar() {
    let dir = TestDir::new("source-grammar");
    let artifact = artifact_key(dir.path(), "model.gguf");
    let invalid = [
        (
            "Hugging-Face",
            "publisher/repository",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face ",
            "publisher/repository",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher%2frepository",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher/repository%3ftoken",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher/repository&token=x",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher/repository=secret",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher//repository",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher/repository/extra",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            " publisher/repository",
            Some("0123456789abcdef0123456789abcdef01234567"),
        ),
        (
            "hugging-face",
            "publisher/repository",
            Some("ABCDEF0123456789abcdef0123456789abcdef01"),
        ),
        ("hugging-face", "publisher/repository", Some("main")),
        ("hugging-face", "publisher/repository", None),
    ];

    for (namespace, source, revision) in invalid {
        assert!(
            DownloadKey::new(
                "coding",
                namespace,
                source,
                revision,
                "weights/model.gguf",
                Some([7; 32]),
                Some(42),
                artifact.clone(),
            )
            .is_err(),
            "accepted {namespace:?} {source:?} {revision:?}"
        );
    }
}

#[test]
fn artifact_destination_rejects_nonportable_path_forms() {
    let dir = TestDir::new("portable-path");
    for name in [
        "C:model.gguf",
        "C:\\model.gguf",
        "model.gguf:stream",
        "CON",
        "nul.gguf",
        "model.gguf.",
        "model.gguf ",
        "nested\\model.gguf",
    ] {
        assert!(
            ArtifactKey::from_destination(&dir.path().join(name)).is_err(),
            "accepted nonportable destination {name:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn artifact_destination_rejects_existing_hardlink_ambiguity() {
    let dir = TestDir::new("hardlink");
    let first = dir.path().join("first.gguf");
    let second = dir.path().join("second.gguf");
    std::fs::write(&first, b"artifact").unwrap();
    std::fs::hard_link(&first, &second).unwrap();

    assert!(ArtifactKey::from_destination(&first).is_err());
    assert!(ArtifactKey::from_destination(&second).is_err());
}

#[cfg(unix)]
#[test]
fn artifact_destination_rejects_parent_replacement_during_identity_capture() {
    let root = TestDir::new("parent-swap");
    let parent = root.path().join("models");
    let moved = root.path().join("models-old");
    std::fs::create_dir(&parent).unwrap();
    let destination = parent.join("model.gguf");

    let result = ArtifactKey::from_destination_with_test_hook(&destination, || {
        std::fs::rename(&parent, &moved).unwrap();
        std::fs::create_dir(&parent).unwrap();
    });

    assert!(result.is_err());
}

#[test]
fn dropping_unpublished_completions_returns_bounded_capacity() {
    let downloads = DownloadCompletionQueue::new(1);
    let download = downloads.reserve().expect("download slot");
    assert!(downloads.reserve().is_none());
    drop(download);
    assert!(downloads.reserve().is_some());

    let lifecycle = crate::lifecycle_controller::LifecycleMailboxInner::new(1);
    let completion = lifecycle
        .reserve_verification()
        .expect("lifecycle verification slot");
    assert!(lifecycle.reserve_verification().is_none());
    drop(completion);
    assert!(lifecycle.reserve_verification().is_some());
}

#[test]
fn mutation_and_read_leases_exclude_same_artifact_only() {
    let dir = TestDir::new("exclusion");
    let first = artifact_key(dir.path(), "first.gguf");
    let second = artifact_key(dir.path(), "second.gguf");
    let coordinator = ArtifactMutationCoordinator::new();

    let mutation = coordinator.try_acquire_mutation(first.clone()).unwrap();
    assert_eq!(
        coordinator.try_acquire_mutation(first.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );
    assert_eq!(
        coordinator.try_acquire_read(first.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );
    let other = coordinator.try_acquire_mutation(second).unwrap();
    drop(other);
    drop(mutation);

    let read_one = coordinator.try_acquire_read(first.clone()).unwrap();
    let read_two = coordinator.try_acquire_read(first.clone()).unwrap();
    assert_eq!(
        coordinator.try_acquire_mutation(first.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );
    drop(read_one);
    drop(read_two);
    assert!(coordinator.try_acquire_mutation(first).is_ok());
}

#[test]
fn cancelled_waiter_does_not_acquire_and_drop_wakes_next_owner() {
    let dir = TestDir::new("cancel");
    let key = artifact_key(dir.path(), "model.gguf");
    let coordinator = Arc::new(ArtifactMutationCoordinator::new());
    let held = coordinator.try_acquire_mutation(key.clone()).unwrap();
    let cancellation = OperationCancellation::new();
    let waiter_coordinator = Arc::clone(&coordinator);
    let waiter_key = key.clone();
    let waiter_cancellation = cancellation.clone();
    let waiter = std::thread::spawn(move || {
        waiter_coordinator.acquire_mutation(waiter_key, &waiter_cancellation)
    });

    cancellation.request_cancel();
    assert_eq!(
        waiter.join().unwrap().unwrap_err(),
        ArtifactAcquireError::Cancelled
    );
    drop(held);
    assert!(coordinator.try_acquire_mutation(key).is_ok());

    let already_cancelled = OperationCancellation::new();
    already_cancelled.request_cancel();
    assert_eq!(
        coordinator
            .acquire_mutation(
                artifact_key(dir.path(), "never-acquired.gguf"),
                &already_cancelled,
            )
            .unwrap_err(),
        ArtifactAcquireError::Cancelled
    );
}

#[test]
fn panic_unwind_drops_an_ordinary_lease() {
    let dir = TestDir::new("unwind");
    let key = artifact_key(dir.path(), "model.gguf");
    let coordinator = ArtifactMutationCoordinator::new();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _lease = coordinator.try_acquire_mutation(key.clone()).unwrap();
        panic!("injected mutation panic");
    }));
    assert!(result.is_err());
    assert!(coordinator.try_acquire_mutation(key).is_ok());
}

#[test]
fn sealing_rejects_new_access_and_poison_retains_uncertain_key() {
    let dir = TestDir::new("seal");
    let poisoned_key = artifact_key(dir.path(), "poisoned.gguf");
    let other_key = artifact_key(dir.path(), "other.gguf");
    let coordinator = ArtifactMutationCoordinator::new();
    coordinator
        .try_acquire_mutation(poisoned_key.clone())
        .unwrap()
        .poison();

    assert_eq!(
        coordinator.try_acquire_mutation(poisoned_key).unwrap_err(),
        ArtifactAcquireError::Poisoned
    );
    coordinator.seal();
    assert_eq!(
        coordinator.try_acquire_read(other_key).unwrap_err(),
        ArtifactAcquireError::Sealed
    );
}

#[test]
fn ready_completion_survives_processing_panic_and_unknown_ack_poison() {
    let dir = TestDir::new("completion");
    let key = artifact_key(dir.path(), "model.gguf");
    let coordinator = ArtifactMutationCoordinator::new();
    let lease = coordinator.try_acquire_mutation(key.clone()).unwrap();
    let queue = DownloadCompletionQueue::new(1);
    let completion = queue.reserve().expect("bounded completion slot");
    let outcome = DownloadVerificationOutcome {
        ownership: DownloadVerificationOwnership {
            operation_id: OperationId::new_v4(),
            admission_revision: DecimalU64::new(1),
            cancellation: OperationCancellation::new(),
            artifact: lease,
        },
        result: VerificationResult::Verified(loxa_core::model_inventory::VerifiedArtifact {
            size_bytes: 42,
            expected_sha256: "07".repeat(32),
            matches: true,
        }),
    };
    assert!(completion.publish(outcome).is_ok());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let retained = queue.ready().expect("ready completion retained");
        let mut ready = retained.lock_ready().expect("ready completion guard");
        assert!(matches!(
            ready.outcome_mut().result,
            VerificationResult::Verified(_)
        ));
        panic!("injected destination panic");
    }));
    assert!(panicked.is_err());
    assert_eq!(
        coordinator.try_acquire_mutation(key.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );

    let retained = queue.ready().expect("panic kept ready envelope");
    retained.lock_ready().expect("ready after panic").poison();
    assert_eq!(
        coordinator.try_acquire_mutation(key.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );

    queue.dispose_poisoned_for_test();
    assert!(coordinator.try_acquire_mutation(key).is_ok());
}

#[test]
fn confirmed_completion_acknowledgement_releases_the_artifact_lease() {
    let dir = TestDir::new("acknowledge");
    let key = artifact_key(dir.path(), "model.gguf");
    let coordinator = ArtifactMutationCoordinator::new();
    let queue = DownloadCompletionQueue::new(1);
    let completion = queue.reserve().expect("bounded completion slot");
    let outcome = DownloadVerificationOutcome {
        ownership: DownloadVerificationOwnership {
            operation_id: OperationId::new_v4(),
            admission_revision: DecimalU64::new(1),
            cancellation: OperationCancellation::new(),
            artifact: coordinator.try_acquire_mutation(key.clone()).unwrap(),
        },
        result: VerificationResult::Verified(loxa_core::model_inventory::VerifiedArtifact {
            size_bytes: 42,
            expected_sha256: "07".repeat(32),
            matches: true,
        }),
    };
    assert!(completion.publish(outcome).is_ok());

    let retained = queue.ready().expect("ready completion retained");
    retained
        .lock_ready()
        .expect("ready completion guard")
        .acknowledge();

    assert!(coordinator.try_acquire_mutation(key).is_ok());
}

#[test]
fn failed_destination_upgrade_returns_poisoned_retention_to_fatal_owner() {
    let dir = TestDir::new("destination-failure");
    let key = artifact_key(dir.path(), "model.gguf");
    let coordinator = ArtifactMutationCoordinator::new();
    let queue = DownloadCompletionQueue::new(1);
    let completion = queue.reserve().expect("bounded completion slot");
    let outcome = DownloadVerificationOutcome {
        ownership: DownloadVerificationOwnership {
            operation_id: OperationId::new_v4(),
            admission_revision: DecimalU64::new(1),
            cancellation: OperationCancellation::new(),
            artifact: coordinator.try_acquire_mutation(key.clone()).unwrap(),
        },
        result: VerificationResult::Verified(loxa_core::model_inventory::VerifiedArtifact {
            size_bytes: 42,
            expected_sha256: "07".repeat(32),
            matches: true,
        }),
    };
    drop(queue);

    let retained = completion
        .publish(outcome)
        .expect_err("lost destination must return retained poison");
    assert_eq!(
        coordinator.try_acquire_mutation(key.clone()).unwrap_err(),
        ArtifactAcquireError::Busy
    );

    retained.dispose_poisoned_for_test();
    assert!(coordinator.try_acquire_mutation(key).is_ok());
}
