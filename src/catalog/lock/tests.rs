use super::*;
use tempfile::tempdir;

#[cfg(unix)]
#[test]
fn a_second_model_lock_is_rejected_until_the_first_is_released() {
    let dir = tempdir().unwrap();
    let first = ModelLock::acquire(dir.path()).unwrap();
    assert!(ModelLock::acquire(dir.path()).is_err());
    drop(first);
    ModelLock::acquire(dir.path()).unwrap();
}

#[cfg(unix)]
#[test]
fn model_lock_release_does_not_wait_for_an_inherited_descriptor() {
    let dir = tempdir().unwrap();
    let first = ModelLock::acquire(dir.path()).unwrap();
    // A forked child retains the same open file description until exec.
    let inherited = first.lock_file.try_clone().unwrap();
    assert_eq!(
        ModelLock::acquire_existing(dir.path()).err(),
        Some(ModelLockError::Busy)
    );

    drop(first);
    let second = ModelLock::acquire_existing(dir.path()).unwrap();
    drop(inherited);
    assert_eq!(
        ModelLock::acquire_existing(dir.path()).err(),
        Some(ModelLockError::Busy)
    );

    drop(second);
    ModelLock::acquire_existing(dir.path()).unwrap();
}

#[cfg(unix)]
#[test]
fn model_lock_retains_and_revalidates_the_opened_model_directory() {
    use std::os::unix::fs::MetadataExt;

    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let opened_dir = root.path().join("opened-demo");
    std::fs::create_dir(&model_dir).unwrap();
    let lock = ModelLock::acquire(&model_dir).unwrap();

    std::fs::rename(&model_dir, &opened_dir).unwrap();
    std::fs::create_dir(&model_dir).unwrap();
    let retained = lock.model_directory().metadata().unwrap();
    let moved_original = std::fs::metadata(&opened_dir).unwrap();
    let replacement = std::fs::metadata(&model_dir).unwrap();

    assert_eq!(
        (retained.dev(), retained.ino()),
        (moved_original.dev(), moved_original.ino())
    );
    assert_ne!(
        (retained.dev(), retained.ino()),
        (replacement.dev(), replacement.ino())
    );

    let error = lock.revalidate_model_directory().unwrap_err();

    assert!(error.contains(&model_dir.display().to_string()), "{error}");
    assert!(opened_dir.join(".lock").is_file());
    assert!(!model_dir.join(".lock").exists());
}

#[cfg(unix)]
#[test]
fn model_lock_rejects_lock_entry_substitution_after_open() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let lock_path = model_dir.join(".lock");
    let opened_lock = model_dir.join("opened.lock");
    std::fs::create_dir(&model_dir).unwrap();

    let error = match ModelLock::acquire_with_after_open(&model_dir, || {
        std::fs::rename(&lock_path, &opened_lock).unwrap();
        std::fs::write(&lock_path, b"replacement").unwrap();
    }) {
        Ok(_) => panic!("a substituted model-lock entry must be rejected"),
        Err(error) => error,
    };

    assert!(error.contains(&lock_path.display().to_string()), "{error}");
    assert_eq!(std::fs::read(&opened_lock).unwrap(), b"");
    assert_eq!(std::fs::read(&lock_path).unwrap(), b"replacement");
}

#[cfg(unix)]
#[test]
fn model_lock_resolves_lock_entry_relative_to_retained_directory() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let moved_dir = root.path().join("moved-demo");
    std::fs::create_dir(&model_dir).unwrap();
    let lock = ModelLock::acquire(&model_dir).unwrap();

    std::fs::rename(&model_dir, &moved_dir).unwrap();
    std::fs::create_dir(&model_dir).unwrap();
    let replacement_lock = model_dir.join(".lock");
    std::fs::write(&replacement_lock, b"replacement sentinel").unwrap();

    lock.revalidate_lock_entry().unwrap();

    assert_eq!(std::fs::read(moved_dir.join(".lock")).unwrap(), b"");
    assert_eq!(
        std::fs::read(&replacement_lock).unwrap(),
        b"replacement sentinel"
    );
}

#[test]
fn existing_model_lock_acquisition_never_creates_a_missing_directory_or_lock() {
    let root = tempdir().unwrap();
    let missing_dir = root.path().join("missing-model");

    assert!(ModelLock::acquire_existing(&missing_dir).is_err());
    assert!(!missing_dir.exists());

    let unlocked_dir = root.path().join("model-without-lock");
    std::fs::create_dir(&unlocked_dir).unwrap();
    let missing_lock = unlocked_dir.join(".lock");

    assert!(ModelLock::acquire_existing(&unlocked_dir).is_err());
    assert!(unlocked_dir.is_dir());
    assert!(!missing_lock.exists());

    std::fs::write(&missing_lock, b"existing lock sentinel").unwrap();
    let metadata = std::fs::symlink_metadata(&missing_lock).unwrap();
    assert!(metadata.file_type().is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        assert_eq!(metadata.nlink(), 1);
    }

    let first = ModelLock::acquire_existing(&unlocked_dir).unwrap();
    assert!(ModelLock::acquire_existing(&unlocked_dir).is_err());
    drop(first);
    let reacquired = ModelLock::acquire_existing(&unlocked_dir).unwrap();
    drop(reacquired);

    assert_eq!(
        std::fs::read(&missing_lock).unwrap(),
        b"existing lock sentinel"
    );
}

#[cfg(unix)]
#[test]
fn typed_model_lock_results_distinguish_missing_busy_and_unsafe_without_creation() {
    use std::os::unix::fs::symlink;

    let root = tempdir().unwrap();
    let missing_dir = root.path().join("typed-missing-directory");

    assert_eq!(
        ModelLock::acquire_existing(&missing_dir).err(),
        Some(ModelLockError::Missing)
    );
    assert!(!missing_dir.exists());

    let missing_lock_dir = root.path().join("typed-missing-lock");
    std::fs::create_dir(&missing_lock_dir).unwrap();
    assert_eq!(
        ModelLock::acquire_existing(&missing_lock_dir).err(),
        Some(ModelLockError::Missing)
    );
    assert!(!missing_lock_dir.join(".lock").exists());

    let transfer_dir = root.path().join("typed-transfer-anchor");
    let transfer_lock = ModelLock::acquire_for_transfer(&transfer_dir).unwrap();
    assert!(transfer_dir.is_dir());
    assert!(transfer_dir.join(".lock").is_file());
    drop(transfer_lock);

    let contended_dir = root.path().join("typed-busy-lock");
    std::fs::create_dir(&contended_dir).unwrap();
    let contended_path = contended_dir.join(".lock");
    std::fs::write(&contended_path, b"typed lock sentinel").unwrap();
    let first = ModelLock::acquire_existing(&contended_dir).unwrap();
    assert_eq!(
        ModelLock::acquire_existing(&contended_dir).err(),
        Some(ModelLockError::Busy)
    );
    assert_eq!(
        ModelLock::acquire(&contended_dir).err().as_deref(),
        Some("model is busy in another Loxa command")
    );
    drop(first);
    let reacquired = ModelLock::acquire_existing(&contended_dir).unwrap();
    drop(reacquired);
    assert_eq!(
        std::fs::read(&contended_path).unwrap(),
        b"typed lock sentinel"
    );

    let outside_dir = root.path().join("typed-outside-directory");
    std::fs::create_dir(&outside_dir).unwrap();
    std::fs::write(outside_dir.join(".lock"), b"outside lock witness").unwrap();
    let symlinked_dir = root.path().join("typed-symlinked-directory");
    symlink(&outside_dir, &symlinked_dir).unwrap();
    assert_eq!(
        ModelLock::acquire_existing(&symlinked_dir).err(),
        Some(ModelLockError::UnsafeLocalState)
    );
    assert_eq!(
        std::fs::read(outside_dir.join(".lock")).unwrap(),
        b"outside lock witness"
    );

    let outside_file = root.path().join("typed-outside-file");
    std::fs::write(&outside_file, b"outside file witness").unwrap();
    let hard_linked_dir = root.path().join("typed-hard-linked-lock");
    std::fs::create_dir(&hard_linked_dir).unwrap();
    std::fs::hard_link(&outside_file, hard_linked_dir.join(".lock")).unwrap();
    assert_eq!(
        ModelLock::acquire_existing(&hard_linked_dir).err(),
        Some(ModelLockError::UnsafeLocalState)
    );
    assert_eq!(
        std::fs::read(&outside_file).unwrap(),
        b"outside file witness"
    );

    let nonregular_dir = root.path().join("typed-nonregular-lock");
    std::fs::create_dir(&nonregular_dir).unwrap();
    std::fs::create_dir(nonregular_dir.join(".lock")).unwrap();
    assert_eq!(
        ModelLock::acquire_existing(&nonregular_dir).err(),
        Some(ModelLockError::UnsafeLocalState)
    );
    assert_eq!(format!("{:?}", ModelLockError::Missing), "Missing");
    assert_eq!(format!("{:?}", ModelLockError::Busy), "Busy");
    assert_eq!(
        format!("{:?}", ModelLockError::UnsafeLocalState),
        "UnsafeLocalState"
    );
}

#[test]
fn model_lock_reports_lock_path_when_lock_is_a_directory() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let lock_path = model_dir.join(".lock");
    std::fs::create_dir_all(&lock_path).unwrap();

    let error = match ModelLock::acquire(&model_dir) {
        Ok(_) => panic!("a directory cannot be acquired as a model lock"),
        Err(error) => error,
    };

    assert!(error.contains(&lock_path.display().to_string()), "{error}");
}

#[test]
fn an_existing_unlocked_lock_file_does_not_block_acquisition() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(model_dir.join(".lock"), b"stale").unwrap();

    ModelLock::acquire(&model_dir).unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn symlinked_or_hard_linked_model_lock_is_rejected() {
    use std::os::unix::fs::symlink;

    for link in ["symlink", "hard-link"] {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let outside = root.path().join("outside");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(&outside, b"keep").unwrap();
        let lock = model_dir.join(".lock");
        if link == "symlink" {
            symlink(&outside, &lock).unwrap();
        } else {
            std::fs::hard_link(&outside, &lock).unwrap();
        }

        assert!(ModelLock::acquire(&model_dir).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
    }
}
