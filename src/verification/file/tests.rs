use super::hash::verify_regular_with_observer;
use super::*;
use crate::safe_file::open_directory;

#[test]
fn captured_verification_is_bound_to_the_canonical_digest_and_size() {
    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("model.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&artifact, bytes).unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());
    let verified = verify_regular_captured(&artifact, bytes.len() as u64, &checksum).unwrap();

    assert!(verified
        .proves(&artifact, bytes.len() as u64, &checksum)
        .is_ok());
    assert!(verified
        .proves(&artifact, bytes.len() as u64 + 1, &checksum)
        .is_err());
    assert!(verified
        .proves(&artifact, bytes.len() as u64, &"f".repeat(64))
        .is_err());
}

#[cfg(unix)]
#[test]
fn captured_verification_can_rebind_only_the_same_file_after_rename() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.gguf");
    let destination = root.path().join("destination.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&source, bytes).unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());
    let verified = verify_regular_captured(&source, bytes.len() as u64, &checksum).unwrap();

    std::fs::rename(&source, &destination).unwrap();
    let rebound = verified.rebind_after_rename(&destination).unwrap();

    assert!(rebound
        .proves(&destination, bytes.len() as u64, &checksum)
        .is_ok());
}

#[cfg(unix)]
#[test]
fn captured_verification_rejects_source_or_destination_substitution() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.gguf");
    let destination = root.path().join("destination.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&source, bytes).unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());
    let verified = verify_regular_captured(&source, bytes.len() as u64, &checksum).unwrap();
    let replacement = root.path().join("replacement.gguf");
    std::fs::write(&replacement, bytes).unwrap();
    std::fs::rename(&replacement, &source).unwrap();
    assert!(verified
        .proves(&source, bytes.len() as u64, &checksum)
        .is_err());

    let verified = verify_regular_captured(&source, bytes.len() as u64, &checksum).unwrap();
    std::fs::rename(&source, &destination).unwrap();
    std::fs::write(&source, bytes).unwrap();
    std::fs::rename(&source, &destination).unwrap();
    assert!(verified.rebind_after_rename(&destination).is_err());
}

#[cfg(unix)]
#[test]
fn local_hash_rejects_a_swapped_source_directory() {
    let root = tempfile::tempdir().unwrap();
    let models = root.path().join("models");
    let moved = root.path().join("moved-models");
    std::fs::create_dir(&models).unwrap();
    let source = models.join("model.gguf");
    let mut bytes = b"GGUF".to_vec();
    bytes.extend(3_u32.to_le_bytes());
    bytes.extend(b"payload");
    std::fs::write(&source, &bytes).unwrap();
    let (directory, identity) = open_directory(&models).unwrap();
    std::fs::rename(&models, &moved).unwrap();
    std::fs::create_dir(&models).unwrap();
    std::fs::write(&source, &bytes).unwrap();

    assert!(
        hash_local_gguf_captured(&directory, &identity, &models, &source, bytes.len() as u64,)
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn verify_regular_rejects_a_hard_link_even_when_its_bytes_match() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.gguf");
    let linked = root.path().join("linked.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&source, bytes).unwrap();
    std::fs::hard_link(&source, &linked).unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());

    assert!(verify_regular(&linked, bytes.len() as u64, &checksum).is_err());
    assert_eq!(std::fs::read(source).unwrap(), bytes);
}

#[cfg(unix)]
#[test]
fn verify_regular_rejects_a_symlink_even_when_its_target_matches() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.gguf");
    let linked = root.path().join("linked.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&source, bytes).unwrap();
    symlink(&source, &linked).unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());

    assert!(verify_regular(&linked, bytes.len() as u64, &checksum).is_err());
    assert_eq!(std::fs::read(source).unwrap(), bytes);
}

#[cfg(unix)]
#[test]
fn verify_regular_rejects_an_in_place_rewrite_with_restored_mtime() {
    use std::os::unix::fs::MetadataExt;

    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("model.gguf");
    let bytes = b"verified artifact";
    std::fs::write(&artifact, bytes).unwrap();
    let original = std::fs::metadata(&artifact).unwrap();
    let original_modified = original.modified().unwrap();
    let checksum = hex(Sha256::digest(bytes).as_ref());

    let error = verify_regular_with_observer(
        &artifact,
        bytes.len() as u64,
        &checksum,
        None,
        &|| false,
        |path| {
            std::fs::write(path, bytes).unwrap();
            OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                .unwrap();
            let restored = std::fs::metadata(path).unwrap();
            assert_eq!(restored.mtime(), original.mtime());
            assert_eq!(restored.mtime_nsec(), original.mtime_nsec());
            Ok(())
        },
    )
    .unwrap_err();

    assert!(error.contains("changed while hashing"), "{error}");
}
