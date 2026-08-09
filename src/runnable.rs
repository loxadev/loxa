use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::{catalog, cli, config, load_installed_models, runner, verification};
use std::path::Path;
use std::time::Instant;

pub(crate) struct Runnable {
    _model_lock: catalog::ModelLock,
    pub(crate) launch: runner::Launch,
}

enum AdmissionSource {
    Installed(Manifest),
    Local(catalog::local::Candidate),
}

pub(crate) fn resolve_runnable(
    id: String,
    runtime: cli::RuntimeArgs,
    paths: &AppPaths,
) -> Result<Runnable, String> {
    let config = config::load(&paths.config)?;
    let ctx = config::resolve_value(runtime.ctx, config.ctx, 4096);
    let port = config::resolve_value(runtime.port, config.port, 0);
    let installed = load_installed_models(paths)?;
    let installed = installed.into_iter().find(|entry| entry.id == id);
    let (source, profile, server) = match installed {
        Some(manifest) => {
            let profile = launch_profile(&manifest, &paths.models)?;
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
            )?;
            (AdmissionSource::Installed(manifest), profile, server)
        }
        None => {
            let candidate = catalog::local::discover(&paths.models)?
                .into_iter()
                .find(|candidate| candidate.id == id)
                .ok_or_else(|| format!("unknown model id {id}"))?;
            let profile = runner::LaunchProfile::generic();
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
            )?;
            (AdmissionSource::Local(candidate), profile, server)
        }
    };
    let admission_started = Instant::now();
    let (manifest, model_lock, admission) = match source {
        AdmissionSource::Local(candidate) => {
            let catalog::local::CapturedAdoption {
                manifest,
                model_lock,
                verified,
            } = catalog::local::adopt_captured(&paths.models, &candidate)?;
            let artifact = manifest.artifact_path(&paths.models);
            verification::refresh_verified(&model_lock, &manifest, &artifact, &verified, None)?;
            (manifest, model_lock, verification::Admission::Verified)
        }
        AdmissionSource::Installed(manifest) => {
            let model_dir = paths.model_dir(&manifest.id)?;
            let artifact = manifest.artifact_path(&paths.models);
            let draft = manifest
                .draft_artifact()
                .map(|draft| paths.models.join(&manifest.id).join(draft.local_filename));
            let model_lock = catalog::ModelLock::acquire(&model_dir)?;
            let admission = verification::verify_or_refresh(
                &model_lock,
                &model_dir,
                &manifest,
                &artifact,
                draft.as_deref(),
                || verification::verify_artifacts(&manifest, &artifact, draft.as_deref()),
            )?;
            (manifest, model_lock, admission)
        }
    };
    tracing::info!(
        event = "model_admission_complete",
        model_id = %manifest.id,
        result = match admission {
            verification::Admission::ReceiptHit => "receipt_hit",
            verification::Admission::Verified => "verified",
        },
        elapsed_ms = admission_started.elapsed().as_millis() as u64,
    );
    let artifact = manifest.artifact_path(&paths.models);
    Ok(Runnable {
        _model_lock: model_lock,
        launch: runner::Launch {
            server,
            model: artifact,
            id: manifest.id,
            requested_port: port,
            ctx,
            profile,
        },
    })
}

fn launch_profile(
    manifest: &Manifest,
    models_root: &Path,
) -> Result<runner::LaunchProfile, String> {
    match (
        manifest.version,
        manifest.profile.as_deref(),
        manifest.runtime.as_ref(),
    ) {
        (3, Some(catalog::GEMMA4_MTP_PROFILE), Some(runtime))
            if runtime.engine == "llama.cpp" && runtime.build == catalog::GEMMA4_LLAMA_BUILD =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp(
                manifest.draft_path(models_root),
            ))
        }
        #[cfg(test)]
        (3, Some(catalog::TEST_MTP_PROFILE), Some(runtime))
            if runtime.engine == "llama.cpp" && runtime.build == catalog::TEST_LLAMA_BUILD =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp_for_test(
                manifest.draft_path(models_root),
                runtime.build.clone(),
            ))
        }
        (1 | 2, None, None) => Ok(runner::LaunchProfile::generic()),
        _ => Err("unsupported validated runtime profile".into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Artifact, ArtifactProvenance, ArtifactRole, Manifest, ModelLock};
    use crate::download;
    use crate::verification::{refresh_verified, verify_or_refresh, VerifiedArtifacts};
    use sha2::{Digest, Sha256};
    use std::cell::Cell;
    use std::fmt::Write as _;
    use std::path::Path;
    use tempfile::tempdir;

    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            id: "demo".into(),
            repo: Some("owner/repo".into()),
            revision: Some("0".repeat(40)),
            remote_filename: Some("model-Q4_K_M.gguf".into()),
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

    fn bundle_manifest() -> Manifest {
        let mut manifest = manifest();
        manifest.version = 3;
        manifest.repo = None;
        manifest.revision = None;
        manifest.remote_filename = None;
        manifest.artifacts = Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: manifest.sha256.clone(),
                size: manifest.size,
                provenance: ArtifactProvenance::Local {
                    source_filename: "model-source.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: "7743ce348d9284d677a185f33295b92266cc435a5b5f775029b300066d26693a".into(),
                size: 5,
                provenance: ArtifactProvenance::Local {
                    source_filename: "draft-source.gguf".into(),
                },
            },
        ]);
        manifest
    }

    fn verified_file(path: &Path) -> download::VerifiedRegularFile {
        let bytes = std::fs::read(path).unwrap();
        let mut checksum = String::with_capacity(64);
        for byte in Sha256::digest(&bytes) {
            write!(&mut checksum, "{byte:02x}").unwrap();
        }
        download::verify_regular_captured(path, bytes.len() as u64, &checksum).unwrap()
    }

    fn verified_artifacts(primary: &Path, draft: Option<&Path>) -> VerifiedArtifacts {
        VerifiedArtifacts {
            primary: verified_file(primary),
            draft: draft.map(verified_file),
        }
    }

    #[cfg(unix)]
    #[test]
    fn unchanged_receipt_skips_the_full_artifact_verifier() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();
        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();

        assert_eq!(calls.get(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn receipt_miss_matrix_reverifies_missing_corrupt_manifest_and_same_size_replacement() {
        enum Change {
            Missing,
            Corrupt,
            Manifest,
            SameSizeReplacement,
        }

        for change in [
            Change::Missing,
            Change::Corrupt,
            Change::Manifest,
            Change::SameSizeReplacement,
        ] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("demo");
            std::fs::create_dir(&model_dir).unwrap();
            let primary = model_dir.join("model.gguf");
            std::fs::write(&primary, b"abc").unwrap();
            let manifest = manifest();
            let model_lock = ModelLock::acquire(&model_dir).unwrap();
            let calls = Cell::new(0);

            verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, None))
            })
            .unwrap();

            let mut candidate = manifest.clone();
            let (label, valid) = match change {
                Change::Missing => {
                    std::fs::remove_file(model_dir.join("verification-receipt.json")).unwrap();
                    ("missing receipt", true)
                }
                Change::Corrupt => {
                    std::fs::write(model_dir.join("verification-receipt.json"), b"{not-json")
                        .unwrap();
                    ("corrupt receipt", true)
                }
                Change::Manifest => {
                    candidate.sha256 = "f".repeat(64);
                    ("manifest mismatch", false)
                }
                Change::SameSizeReplacement => {
                    let replacement = model_dir.join("replacement.gguf");
                    std::fs::write(&replacement, b"xyz").unwrap();
                    std::fs::rename(replacement, &primary).unwrap();
                    ("same-size replacement", false)
                }
            };

            let result =
                verify_or_refresh(&model_lock, &model_dir, &candidate, &primary, None, || {
                    calls.set(calls.get() + 1);
                    Ok(verified_artifacts(&primary, None))
                });

            assert_eq!(result.is_ok(), valid, "{label}");
            assert_eq!(calls.get(), 2, "{label} must invoke the full verifier");
        }
    }

    #[cfg(unix)]
    #[test]
    fn receipt_rechecks_optional_draft_identity_before_hitting() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        let draft = model_dir.join("draft.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        std::fs::write(&draft, b"draft").unwrap();
        let manifest = bundle_manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        )
        .unwrap();
        verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        )
        .unwrap();
        let replacement = model_dir.join("replacement-draft.gguf");
        std::fs::write(&replacement, b"other").unwrap();
        std::fs::rename(replacement, &draft).unwrap();
        let result = verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        );

        assert!(result.is_err());
        assert_eq!(calls.get(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn swapped_model_directory_is_fail_closed_before_a_receipt_can_hit() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();

        std::fs::rename(&model_dir, root.path().join("replaced-demo")).unwrap();
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&primary, b"abc").unwrap();

        let error = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap_err();

        assert!(error.contains("demo"), "{error}");
        assert_eq!(
            calls.get(),
            1,
            "a swapped directory must not hit or hash by path"
        );
    }

    #[cfg(unix)]
    #[test]
    fn failed_full_verification_leaves_no_receipt_to_trust_later() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt = model_dir.join("verification-receipt.json");

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();
        std::fs::write(&primary, b"xyz").unwrap();

        let error = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            Err("full verification failed".into())
        })
        .unwrap_err();

        assert_eq!(error, "full verification failed");
        assert!(!receipt.exists());
    }

    #[cfg(unix)]
    #[test]
    fn replacement_after_full_verification_cannot_receive_a_receipt() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt = model_dir.join("verification-receipt.json");

        let result = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            let verified = verified_artifacts(&primary, None);
            let replacement = model_dir.join("replacement.gguf");
            std::fs::write(&replacement, b"xyz").unwrap();
            std::fs::rename(replacement, &primary).unwrap();
            Ok(verified)
        });

        assert!(
            result.is_err(),
            "an unverified replacement must not be blessed"
        );
        assert!(!receipt.exists());
    }

    #[cfg(unix)]
    #[test]
    fn receipt_refresh_replaces_a_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt_path = model_dir.join("verification-receipt.json");
        let outside = root.path().join("outside-receipt");
        std::fs::write(&outside, b"outside witness").unwrap();
        symlink(&outside, &receipt_path).unwrap();
        let verified = verified_file(&primary);

        refresh_verified(&model_lock, &manifest, &primary, &verified, None).unwrap();

        assert_eq!(std::fs::read(&outside).unwrap(), b"outside witness");
        assert!(std::fs::symlink_metadata(receipt_path)
            .unwrap()
            .file_type()
            .is_file());
    }

    #[cfg(unix)]
    #[test]
    fn receipt_refresh_rejects_a_proof_for_a_different_digest() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let mut candidate = manifest();
        candidate.sha256 = "f".repeat(64);
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let verified = verified_file(&primary);

        assert!(refresh_verified(&model_lock, &candidate, &primary, &verified, None).is_err());
        assert!(!model_dir.join("verification-receipt.json").exists());
    }
}
