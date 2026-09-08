use super::*;
use tempfile::tempdir;

fn manifest(id: &str) -> Manifest {
    Manifest {
        version: 1,
        id: id.into(),
        repo: Some("owner/repo".into()),
        revision: Some("0123456789abcdef0123456789abcdef01234567".into()),
        remote_filename: Some("demo-Q4_K_M.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        size: 3,
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn local_manifest(id: &str) -> Manifest {
    let mut manifest = manifest(id);
    manifest.version = 2;
    manifest.repo = None;
    manifest.revision = None;
    manifest.remote_filename = None;
    manifest.origin = Some(Origin::Local);
    manifest.source_filename = Some("source.gguf".into());
    manifest
}

fn test_model_bundle(id: &str) -> Manifest {
    let primary = local_manifest(id);
    Manifest {
        version: 3,
        id: id.into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: primary.local_filename.clone(),
        sha256: primary.sha256.clone(),
        size: primary.size,
        artifacts: Some(vec![Artifact {
            role: ArtifactRole::Model,
            local_filename: primary.local_filename,
            sha256: primary.sha256,
            size: primary.size,
            provenance: ArtifactProvenance::Local {
                source_filename: "source.gguf".into(),
            },
        }]),
        profile: Some(TEST_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: TEST_LLAMA_BUILD.into(),
        }),
    }
}

fn bundle_manifest(id: &str) -> Manifest {
    Manifest {
        version: 3,
        id: id.into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: GEMMA4_MODEL_SHA256.into(),
        size: GEMMA4_MODEL_SIZE,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: GEMMA4_MODEL_SHA256.into(),
                size: GEMMA4_MODEL_SIZE,
                provenance: ArtifactProvenance::Local {
                    source_filename: "gemma-4.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: GEMMA4_DRAFT_SHA256.into(),
                size: GEMMA4_DRAFT_SIZE,
                provenance: ArtifactProvenance::Local {
                    source_filename: "mtp-gemma-4.gguf".into(),
                },
            },
        ]),
        profile: Some(GEMMA4_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: GEMMA4_LLAMA_BUILD.into(),
        }),
    }
}

#[test]
fn version_three_bundle_is_validated_and_round_trips() {
    let expected = bundle_manifest("gemma4");

    let encoded = serde_json::to_vec_pretty(&expected).unwrap();
    let decoded: Manifest = serde_json::from_slice(&encoded).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded, expected);
}

#[test]
fn production_bundle_requires_the_exact_qualified_draft_artifact() {
    let mut model_only = bundle_manifest("gemma4");
    model_only.artifacts.as_mut().unwrap().pop();
    assert_eq!(
        model_only.validate().unwrap_err(),
        "qualified production bundle is missing its draft artifact"
    );
    assert!(model_only.draft_artifact().is_none());
    assert!(!is_qualified_gemma4_bundle(&model_only));
}

#[test]
fn intentionally_test_only_bundle_can_remain_primary_only() {
    let model_only = test_model_bundle("test-only");
    model_only.validate().unwrap();
    assert!(model_only.draft_artifact().is_none());
    assert_eq!(model_only.total_size(), model_only.size);
}

#[test]
fn version_three_publication_verifies_every_artifact_before_writing_manifest() {
    let root = tempdir().unwrap();
    let expected = bundle_manifest("gemma4");
    let manifest_path = root.path().join("gemma4/manifest.json");
    let mut failed_verifications = Vec::new();

    let error = publish_manifest_with_verifier(root.path(), &expected, |path, _, _| {
        assert!(!manifest_path.exists());
        let filename = path.file_name().unwrap().to_string_lossy().into_owned();
        failed_verifications.push(filename.clone());
        if filename == "draft.gguf" {
            Err("draft verification failed".into())
        } else {
            Ok(())
        }
    })
    .unwrap_err();

    assert_eq!(error, "draft verification failed");
    assert_eq!(failed_verifications, ["model.gguf", "draft.gguf"]);
    assert!(!manifest_path.exists());

    let mut successful_verifications = Vec::new();
    let published = publish_manifest_with_verifier(root.path(), &expected, |path, _, _| {
        assert!(!manifest_path.exists());
        successful_verifications.push(path.file_name().unwrap().to_string_lossy().into_owned());
        Ok(())
    })
    .unwrap();

    assert_eq!(successful_verifications, ["model.gguf", "draft.gguf"]);
    assert_eq!(published, manifest_path);
    assert!(manifest_path.exists());
}

#[test]
fn captured_publication_accepts_an_exact_remote_single_file_manifest() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let published =
        publish_manifest_verified(root.path(), &expected, &model_lock, &verified).unwrap();

    assert_eq!(published, model_dir.join("manifest.json"));
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    assert!(!model_dir.join("pending.json").exists());
}

#[cfg(unix)]
#[test]
fn captured_remote_publication_refuses_a_byte_identical_inode_substitution_after_proof() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    let model_path = model_dir.join("model.gguf");
    let replacement_path = model_dir.join("replacement.gguf");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(&model_path, b"abc").unwrap();
    std::fs::write(&replacement_path, b"abc").unwrap();
    let pending = std::fs::read(model_dir.join("pending.json")).unwrap();
    let original_identity = crate::safe_file::regular_file_identity(
        &std::fs::File::open(&model_path).unwrap(),
        &model_path,
    )
    .unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified =
        crate::download::verify_regular_captured(&model_path, expected.size, &expected.sha256)
            .unwrap();

    let result = publish_manifest_verified_with_hook(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |point| {
            assert_eq!(point, VerifiedPublicationPoint::AfterProof);
            std::fs::rename(&replacement_path, &model_path).unwrap();
            Ok(())
        },
    );

    assert!(result.is_err());
    assert_eq!(std::fs::read(&model_path).unwrap(), b"abc");
    let replacement_identity = crate::safe_file::regular_file_identity(
        &std::fs::File::open(&model_path).unwrap(),
        &model_path,
    )
    .unwrap();
    assert_ne!(replacement_identity, original_identity);
    assert_eq!(
        std::fs::read(model_dir.join("pending.json")).unwrap(),
        pending
    );
    assert!(!model_dir.join("manifest.json").exists());
    assert!(!model_dir.join("manifest.json.tmp").exists());
}

#[cfg(unix)]
#[test]
fn captured_remote_publication_rolls_back_if_the_artifact_changes_after_manifest_visibility() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    let model_path = model_dir.join("model.gguf");
    let replacement_path = model_dir.join("replacement.gguf");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(&model_path, b"abc").unwrap();
    std::fs::write(&replacement_path, b"abc").unwrap();
    let pending = std::fs::read(model_dir.join("pending.json")).unwrap();
    let original_identity = crate::safe_file::regular_file_identity(
        &std::fs::File::open(&model_path).unwrap(),
        &model_path,
    )
    .unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified =
        crate::download::verify_regular_captured(&model_path, expected.size, &expected.sha256)
            .unwrap();

    let result = publish_manifest_verified_with_hook(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |point| {
            if point == VerifiedPublicationPoint::AfterManifestVisible {
                std::fs::rename(&replacement_path, &model_path).unwrap();
            }
            Ok(())
        },
    );

    assert!(result.is_err());
    assert_eq!(std::fs::read(&model_path).unwrap(), b"abc");
    let replacement_identity = crate::safe_file::regular_file_identity(
        &std::fs::File::open(&model_path).unwrap(),
        &model_path,
    )
    .unwrap();
    assert_ne!(replacement_identity, original_identity);
    assert_eq!(
        std::fs::read(model_dir.join("pending.json")).unwrap(),
        pending
    );
    assert!(!model_dir.join("manifest.json").exists());
    assert!(!model_dir.join("manifest.json.tmp").exists());
}

#[cfg(unix)]
#[test]
fn captured_remote_publication_accepts_exact_installed_when_pending_disappears_after_visibility() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let published = publish_manifest_verified_with_hook(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |point| {
            if point == VerifiedPublicationPoint::AfterManifestVisible {
                std::fs::remove_file(model_dir.join("pending.json")).unwrap();
                std::fs::File::open(&model_dir).unwrap().sync_all().unwrap();
            }
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(published, model_dir.join("manifest.json"));
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    assert!(!model_dir.join("pending.json").exists());
    assert!(!model_dir.join("manifest.json.tmp").exists());
}

#[cfg(unix)]
#[test]
fn captured_remote_publication_preserves_installed_manifest_after_pending_removal_sync_failure() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();
    let sync_failed = std::cell::Cell::new(false);

    let error = publish_manifest_verified_with_recovery(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |_| Ok(()),
        |lock, expected, plan| {
            transfer::recover_installed_completion_with_sync(lock, expected, plan, |_| {
                assert!(!model_dir.join("pending.json").exists());
                assert!(model_dir.join("manifest.json").exists());
                sync_failed.set(true);
                Err(std::io::Error::other("injected directory sync failure"))
            })
        },
    )
    .unwrap_err();

    assert!(error.contains("completion durability failed"), "{error}");
    assert!(sync_failed.get());
    assert_eq!(
        std::fs::read(model_dir.join("manifest.json")).unwrap(),
        serde_json::to_vec_pretty(&expected).unwrap()
    );
    assert!(!model_dir.join("pending.json").exists());
    assert!(!model_dir.join("manifest.json.tmp").exists());
    assert_eq!(
        transfer::plan_transfer(&model_lock, &expected).state(),
        transfer::CatalogTransferState::Installed
    );
}

#[test]
fn captured_remote_publication_accepts_an_exact_existing_manifest_and_cleans_pending() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    std::fs::create_dir(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    publish_manifest(root.path(), &expected).unwrap();
    let manifest_bytes = std::fs::read(model_dir.join("manifest.json")).unwrap();
    std::fs::write(model_dir.join("pending.json"), &manifest_bytes).unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let published =
        publish_manifest_verified(root.path(), &expected, &model_lock, &verified).unwrap();

    assert_eq!(published, model_dir.join("manifest.json"));
    assert_eq!(std::fs::read(&published).unwrap(), manifest_bytes);
    assert!(!model_dir.join("pending.json").exists());
}

#[test]
fn captured_single_file_publication_rejects_a_version_three_bundle() {
    let root = tempdir().unwrap();
    let expected = test_model_bundle("demo");
    let model_dir = root.path().join("demo");
    std::fs::create_dir(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let error =
        publish_manifest_verified(root.path(), &expected, &model_lock, &verified).unwrap_err();

    assert!(error.contains("single-file"), "{error}");
    assert!(!model_dir.join("manifest.json").exists());
}

#[cfg(unix)]
#[test]
fn captured_remote_publication_refuses_a_model_directory_swap_without_touching_either_directory() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    let model_dir = root.path().join("demo");
    let moved_dir = root.path().join("moved-demo");
    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    let pending = std::fs::read(model_dir.join("pending.json")).unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let result = publish_manifest_verified_with_hook(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |point| {
            assert_eq!(point, VerifiedPublicationPoint::AfterProof);
            std::fs::rename(&model_dir, &moved_dir).unwrap();
            std::fs::create_dir(&model_dir).unwrap();
            std::fs::write(model_dir.join("witness"), b"replacement witness").unwrap();
            std::fs::write(model_dir.join("pending.json"), b"replacement pending").unwrap();
            Ok(())
        },
    );

    assert!(result.is_err());
    assert_eq!(
        std::fs::read(model_dir.join("witness")).unwrap(),
        b"replacement witness"
    );
    assert_eq!(
        std::fs::read(model_dir.join("pending.json")).unwrap(),
        b"replacement pending"
    );
    assert!(!model_dir.join("manifest.json").exists());
    assert_eq!(
        std::fs::read(moved_dir.join("pending.json")).unwrap(),
        pending
    );
    assert!(!moved_dir.join("manifest.json").exists());
    assert!(!moved_dir.join("manifest.json.tmp").exists());
}

#[cfg(unix)]
#[test]
fn captured_publication_refuses_a_model_directory_swap_without_touching_replacement() {
    let root = tempdir().unwrap();
    let expected = local_manifest("demo");
    let model_dir = root.path().join("demo");
    let moved_dir = root.path().join("moved-demo");
    std::fs::create_dir(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    std::fs::write(
        model_dir.join("pending.json"),
        serde_json::to_vec_pretty(&expected).unwrap(),
    )
    .unwrap();
    let model_lock = ModelLock::acquire(&model_dir).unwrap();
    let verified = crate::download::verify_regular_captured(
        &model_dir.join("model.gguf"),
        expected.size,
        &expected.sha256,
    )
    .unwrap();

    let result = publish_manifest_verified_with_hook(
        root.path(),
        &expected,
        &model_lock,
        &verified,
        |point| {
            assert_eq!(point, VerifiedPublicationPoint::AfterProof);
            std::fs::rename(&model_dir, &moved_dir).unwrap();
            std::fs::create_dir(&model_dir).unwrap();
            std::fs::write(model_dir.join("witness"), b"replacement witness").unwrap();
            std::fs::write(model_dir.join("pending.json"), b"replacement pending").unwrap();
            Ok(())
        },
    );

    assert!(result.is_err());
    assert_eq!(
        std::fs::read(model_dir.join("witness")).unwrap(),
        b"replacement witness"
    );
    assert_eq!(
        std::fs::read(model_dir.join("pending.json")).unwrap(),
        b"replacement pending"
    );
    assert!(!model_dir.join("manifest.json").exists());
    assert!(moved_dir.join("pending.json").is_file());
    assert!(!moved_dir.join("manifest.json").exists());
    assert!(!moved_dir.join("manifest.json.tmp").exists());
}

#[test]
fn bundle_publication_keeps_manifest_visible_at_exchange_checkpoints() {
    let root = tempdir().unwrap();
    let expected = local_manifest("gemma4");
    let replacement = test_model_bundle("gemma4");
    write_artifact(root.path(), "gemma4");
    publish_manifest(root.path(), &expected).unwrap();
    let manifest_path = root.path().join("gemma4/manifest.json");
    let mut observed = Vec::new();

    let published =
        replace_manifest_atomic_with_hook(root.path(), &expected, &replacement, |point| {
            assert!(manifest_path.is_file());
            match point {
                ManifestPublicationPoint::BeforeExchange => {
                    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected.clone()]);
                }
                ManifestPublicationPoint::AfterExchange => {
                    assert_eq!(
                        load_catalog(root.path()).unwrap(),
                        vec![replacement.clone()]
                    );
                }
            }
            observed.push(point);
            Ok(())
        })
        .unwrap();

    assert!(published);
    assert_eq!(
        observed,
        [
            ManifestPublicationPoint::BeforeExchange,
            ManifestPublicationPoint::AfterExchange
        ]
    );
    assert_eq!(load_catalog(root.path()).unwrap(), vec![replacement]);
}

#[test]
fn artifact_changed_after_initial_verification_restores_the_prior_manifest() {
    let root = tempdir().unwrap();
    let expected = local_manifest("gemma4");
    let replacement = test_model_bundle("gemma4");
    write_artifact(root.path(), "gemma4");
    publish_manifest(root.path(), &expected).unwrap();
    let artifact_path = root.path().join("gemma4/model.gguf");

    let error = replace_manifest_atomic_with_hook(root.path(), &expected, &replacement, |point| {
        if point == ManifestPublicationPoint::BeforeExchange {
            std::fs::write(&artifact_path, b"xyz").unwrap();
        }
        Ok(())
    })
    .unwrap_err();

    assert!(error.contains("restored the prior manifest"), "{error}");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    assert_ne!(
        load_catalog(root.path()).unwrap(),
        vec![replacement],
        "a changed artifact must not publish its qualified replacement"
    );
}

#[test]
fn a_late_manifest_change_is_restored_after_the_exchange_check() {
    let root = tempdir().unwrap();
    let expected = local_manifest("gemma4");
    let replacement = test_model_bundle("gemma4");
    let mut changed = expected.clone();
    changed.source_filename = Some("changed-source.gguf".into());
    write_artifact(root.path(), "gemma4");
    publish_manifest(root.path(), &expected).unwrap();
    let manifest_path = root.path().join("gemma4/manifest.json");

    let error = replace_manifest_atomic_with_hook(root.path(), &expected, &replacement, |point| {
        if point == ManifestPublicationPoint::BeforeExchange {
            std::fs::write(&manifest_path, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
        }
        Ok(())
    })
    .unwrap_err();

    assert!(error.contains("restored the prior manifest"), "{error}");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![changed]);
    let retained = std::fs::read_dir(root.path().join("gemma4"))
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".bundle-manifest-")
        })
        .expect("rollback keeps the replacement temp for recovery");
    let retained: Manifest =
        serde_json::from_slice(&std::fs::read(retained.path()).unwrap()).unwrap();
    assert_eq!(retained, replacement);
}

#[test]
fn a_post_exchange_manifest_change_retains_recovery_state() {
    let root = tempdir().unwrap();
    let expected = local_manifest("gemma4");
    let replacement = test_model_bundle("gemma4");
    let mut changed = replacement.clone();
    changed.artifacts.as_mut().unwrap()[0].provenance = ArtifactProvenance::Local {
        source_filename: "changed-after-exchange.gguf".into(),
    };
    write_artifact(root.path(), "gemma4");
    publish_manifest(root.path(), &expected).unwrap();
    let manifest_path = root.path().join("gemma4/manifest.json");

    let error = replace_manifest_atomic_with_hook(root.path(), &expected, &replacement, |point| {
        if point == ManifestPublicationPoint::AfterExchange {
            std::fs::write(&manifest_path, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
        }
        Ok(())
    })
    .unwrap_err();

    assert!(error.contains("retained recovery state"), "{error}");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![changed]);
    assert!(std::fs::read_dir(root.path().join("gemma4"))
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(".bundle-manifest-")));
}

#[test]
fn version_three_bundle_rejects_unqualified_or_unsafe_artifacts() {
    let valid = bundle_manifest("gemma4");
    valid.validate().unwrap();
    assert_eq!(valid.primary_artifact().role, ArtifactRole::Model);
    assert_eq!(valid.draft_artifact().unwrap().role, ArtifactRole::Draft);
    assert_eq!(valid.total_size(), GEMMA4_MODEL_SIZE + GEMMA4_DRAFT_SIZE);

    let mut duplicate = valid.clone();
    duplicate.artifacts.as_mut().unwrap()[1].role = ArtifactRole::Model;
    assert!(duplicate.validate().is_err());

    let mut traversal = valid.clone();
    traversal.artifacts.as_mut().unwrap()[1].local_filename = "../draft.gguf".into();
    assert!(traversal.validate().is_err());

    let mut wrong_profile = valid.clone();
    wrong_profile.profile = Some("arbitrary".into());
    assert!(wrong_profile.validate().is_err());

    let mut wrong_build = valid;
    wrong_build.runtime.as_mut().unwrap().build = "latest".into();
    assert!(wrong_build.validate().is_err());
}

fn write_artifact(models_root: &Path, id: &str) {
    let model_dir = models_root.join(id);
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
}

#[test]
fn validates_and_atomically_publishes_fresh_deterministic_catalog() {
    let dir = tempdir().unwrap();
    write_artifact(dir.path(), "zeta");
    write_artifact(dir.path(), "alpha");
    publish_manifest(dir.path(), &manifest("zeta")).unwrap();
    publish_manifest(dir.path(), &manifest("alpha")).unwrap();
    publish_manifest(dir.path(), &manifest("alpha")).unwrap();

    let entries = load_catalog(dir.path()).unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
    assert!(!dir.path().join("alpha/manifest.json.tmp").exists());

    let mut invalid = manifest("../bad");
    assert!(invalid.validate().is_err());
    invalid = manifest("valid");
    invalid.remote_filename = Some("part-00001-of-00002.gguf".into());
    assert!(invalid.validate().is_err());
}

#[test]
fn foreign_store_entries_do_not_hide_managed_models() {
    let root = tempdir().unwrap();
    let expected = manifest("smollm2-135m");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    std::fs::write(root.path().join("legacy.gguf.part"), b"partial").unwrap();
    std::fs::write(root.path().join("legacy.gguf"), b"model").unwrap();
    let foreign = root.path().join("source-checkout");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("model.safetensors"), b"weights").unwrap();

    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
}

#[cfg(target_os = "macos")]
#[test]
fn symlinked_model_directory_is_rejected_before_catalog_mutation() {
    use std::os::unix::fs::symlink;

    let root = tempdir().unwrap();
    let models = root.path().join("models");
    let outside = root.path().join("outside");
    std::fs::create_dir_all(&models).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("model.gguf"), b"artifact").unwrap();
    std::fs::write(outside.join("model.gguf.part"), b"partial").unwrap();
    std::fs::write(outside.join("model.gguf.part.restart"), b"restart").unwrap();
    std::fs::write(outside.join("model.gguf.invalid"), b"invalid").unwrap();
    let model_dir = models.join("demo");
    symlink(&outside, &model_dir).unwrap();
    let expected = manifest("demo");

    assert!(ModelLock::acquire(&model_dir).is_err());
    assert!(prepare_pull(&model_dir, &expected).is_err());
    assert!(publish_manifest(&models, &expected).is_err());

    assert!(!outside.join(".lock").exists());
    assert!(!outside.join("pending.json").exists());
    assert!(!outside.join("manifest.json").exists());
    assert_eq!(
        std::fs::read(outside.join("model.gguf")).unwrap(),
        b"artifact"
    );
    assert_eq!(
        std::fs::read(outside.join("model.gguf.part")).unwrap(),
        b"partial"
    );
    assert_eq!(
        std::fs::read(outside.join("model.gguf.part.restart")).unwrap(),
        b"restart"
    );
    assert_eq!(
        std::fs::read(outside.join("model.gguf.invalid")).unwrap(),
        b"invalid"
    );
}

#[test]
fn incomplete_pull_is_hidden_and_can_only_resume_the_same_artifact() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let expected = manifest("demo");

    prepare_pull(&model_dir, &expected).unwrap();
    std::fs::write(model_dir.join("model.gguf.part"), b"partial").unwrap();
    assert!(load_catalog(root.path()).unwrap().is_empty());
    prepare_pull(&model_dir, &expected).unwrap();
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
        b"partial"
    );

    let mut different = expected.clone();
    different.sha256 = "b".repeat(64);
    assert!(prepare_pull(&model_dir, &different)
        .unwrap_err()
        .contains("different artifact"));

    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    publish_manifest(root.path(), &expected).unwrap();
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    assert!(!model_dir.join("pending.json").exists());
}

#[test]
fn unidentified_transfer_state_is_refused_without_mutation() {
    for name in [
        "model.gguf",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ] {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        let path = model_dir.join(name);
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(&path, b"unidentified bytes").unwrap();

        let error = prepare_pull(&model_dir, &manifest("demo")).unwrap_err();

        assert!(error.contains(&path.display().to_string()), "{error}");
        assert!(error.contains("move or remove"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), b"unidentified bytes");
        assert!(!model_dir.join("pending.json").exists());
    }
}

#[test]
fn manifest_rejects_control_characters_in_remote_identity() {
    let mut invalid_repo = manifest("demo");
    invalid_repo.repo = Some("owner/repo\u{1b}".into());
    assert!(invalid_repo.validate().is_err());

    let mut invalid_filename = manifest("demo");
    invalid_filename.remote_filename = Some("demo-\u{85}Q4_K_M.gguf".into());
    assert!(invalid_filename.validate().is_err());
}

#[test]
fn stale_atomic_temps_do_not_poison_or_block_an_incomplete_pull() {
    let root = tempdir().unwrap();
    let model_dir = root.path().join("demo");
    let expected = manifest("demo");
    std::fs::create_dir_all(&model_dir).unwrap();

    std::fs::write(model_dir.join("pending.json.tmp"), b"interrupted").unwrap();
    prepare_pull(&model_dir, &expected).unwrap();
    assert!(!model_dir.join("pending.json.tmp").exists());

    std::fs::write(model_dir.join("manifest.json.tmp"), b"interrupted").unwrap();
    assert!(load_catalog(root.path()).unwrap().is_empty());
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    publish_manifest(root.path(), &expected).unwrap();

    assert!(!model_dir.join("manifest.json.tmp").exists());
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
}

#[test]
fn missing_or_checksum_invalid_artifact_is_not_published() {
    let root = tempdir().unwrap();
    let missing = manifest("missing");

    assert!(publish_manifest(root.path(), &missing).is_err());
    assert!(!root.path().join("missing/manifest.json").exists());

    let invalid = manifest("invalid");
    let invalid_dir = root.path().join("invalid");
    std::fs::create_dir_all(&invalid_dir).unwrap();
    std::fs::write(invalid_dir.join("model.gguf"), b"xyz").unwrap();

    assert!(publish_manifest(root.path(), &invalid).is_err());
    assert!(!invalid_dir.join("manifest.json").exists());
    assert!(load_catalog(root.path()).unwrap().is_empty());
}

#[test]
fn removing_a_model_deletes_owned_state_and_keeps_a_stable_lock_anchor() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    std::fs::write(root.path().join("foreign.gguf"), b"keep").unwrap();

    remove_model(root.path(), &expected).unwrap();

    let model_dir = root.path().join("demo");
    assert!(model_dir.is_dir());
    assert!(!model_dir.join("manifest.json").exists());
    assert!(!model_dir.join("model.gguf").exists());
    assert!(model_dir.join(".lock").is_file());
    ModelLock::acquire(&model_dir).unwrap();
    assert_eq!(
        std::fs::read(root.path().join("foreign.gguf")).unwrap(),
        b"keep"
    );
    assert!(load_catalog(root.path()).unwrap().is_empty());
}

#[test]
fn removing_a_model_cleans_only_known_recovery_state() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    let model_dir = root.path().join("demo");
    for name in [
        "pending.json.tmp",
        "manifest.json.tmp",
        "model.gguf.part",
        "model.gguf.part.restart",
        "model.gguf.invalid",
    ] {
        std::fs::write(model_dir.join(name), b"recovery").unwrap();
    }
    std::fs::write(
        model_dir.join("pending.json"),
        serde_json::to_vec(&expected).unwrap(),
    )
    .unwrap();

    remove_model(root.path(), &expected).unwrap();

    assert_eq!(
        std::fs::read_dir(&model_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>(),
        [".lock"]
    );
    assert!(load_catalog(root.path()).unwrap().is_empty());
}

#[test]
fn removing_a_model_refuses_mismatched_pending_state() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    let mut pending = expected.clone();
    pending.sha256 = "b".repeat(64);
    std::fs::write(
        root.path().join("demo/pending.json"),
        serde_json::to_vec(&pending).unwrap(),
    )
    .unwrap();

    let error = remove_model(root.path(), &expected).unwrap_err();

    assert!(error.contains("different artifact"), "{error}");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
}

#[test]
fn removing_a_model_refuses_unexpected_entries_without_mutation() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    let unexpected = root.path().join("demo/notes.txt");
    std::fs::write(&unexpected, b"keep").unwrap();

    let error = remove_model(root.path(), &expected).unwrap_err();

    assert!(error.contains("unexpected model entry"), "{error}");
    assert_eq!(std::fs::read(&unexpected).unwrap(), b"keep");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
}

#[cfg(unix)]
#[test]
fn removing_a_busy_model_is_rejected() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    let lock = ModelLock::acquire(&root.path().join("demo")).unwrap();

    let error = remove_model(root.path(), &expected).unwrap_err();

    assert!(error.contains("busy"), "{error}");
    assert_eq!(load_catalog(root.path()).unwrap(), vec![expected]);
    drop(lock);
}
#[test]
fn reconciliation_failure_does_not_block_a_valid_catalog() {
    let root = tempdir().unwrap();
    let expected = manifest("demo");
    write_artifact(root.path(), &expected.id);
    publish_manifest(root.path(), &expected).unwrap();
    let reconciled = std::cell::Cell::new(false);

    let loaded = load_reconciled_catalog_with(root.path(), |models_root| {
        assert_eq!(models_root, root.path());
        reconciled.set(true);
        Err("injected reconciliation failure".into())
    })
    .unwrap();

    assert!(reconciled.get());
    assert_eq!(loaded, vec![expected]);
}
