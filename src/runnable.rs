mod receipt;

use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::{catalog, cli, config, download, load_installed_models, runner};
use std::path::Path;

pub(crate) struct Runnable {
    _model_lock: catalog::ModelLock,
    pub(crate) launch: runner::Launch,
}

struct VerifiedArtifacts {
    primary: download::VerifiedRegularFile,
    draft: Option<download::VerifiedRegularFile>,
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
    let (manifest, profile, server) = match installed {
        Some(manifest) => {
            let profile = launch_profile(&manifest, &paths.models)?;
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
            )?;
            (manifest, profile, server)
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
            let manifest = catalog::local::adopt(&paths.models, &candidate)?;
            tracing::info!(event = "local_model_adopted", model_id = %manifest.id);
            (manifest, profile, server)
        }
    };
    let model_dir = paths.model_dir(&manifest.id)?;
    let model_lock = catalog::ModelLock::acquire(&model_dir)?;
    let artifact = manifest.artifact_path(&paths.models);
    let draft = manifest
        .draft_artifact()
        .map(|draft| paths.models.join(&manifest.id).join(draft.local_filename));
    verify_or_refresh(
        &model_lock,
        &model_dir,
        &manifest,
        &artifact,
        draft.as_deref(),
        || verify_artifacts(&manifest, &artifact, draft.as_deref()),
    )?;
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

fn verify_or_refresh<F>(
    model_lock: &catalog::ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    primary: &Path,
    draft: Option<&Path>,
    verify: F,
) -> Result<(), String>
where
    F: FnOnce() -> Result<VerifiedArtifacts, String>,
{
    if receipt::matches(model_lock, model_dir, manifest, primary, draft)? {
        tracing::info!(event = "model_verification_receipt_hit", model_id = %manifest.id);
        return Ok(());
    }
    tracing::debug!(event = "model_verification_receipt_miss", model_id = %manifest.id);
    receipt::discard(model_lock, model_dir)?;
    let verified = verify()?;
    let verified_draft = match (draft, verified.draft.as_ref()) {
        (Some(path), Some(file)) => Some((path, file)),
        (None, None) => None,
        _ => return Err("verified draft artifact mismatch".into()),
    };
    receipt::refresh(
        model_lock,
        manifest,
        primary,
        &verified.primary,
        verified_draft,
    )
}

fn verify_artifacts(
    manifest: &Manifest,
    primary_path: &Path,
    draft_path: Option<&Path>,
) -> Result<VerifiedArtifacts, String> {
    let primary = manifest.primary_artifact();
    let primary = download::verify_regular_captured(primary_path, primary.size, primary.sha256)?;
    let draft = match (manifest.draft_artifact(), draft_path) {
        (Some(artifact), Some(path)) => Some(download::verify_regular_captured(
            path,
            artifact.size,
            artifact.sha256,
        )?),
        (None, None) => None,
        _ => return Err("verified draft artifact mismatch".into()),
    };
    Ok(VerifiedArtifacts { primary, draft })
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
    use super::{receipt, verify_or_refresh, VerifiedArtifacts};
    use crate::catalog::{Artifact, ArtifactProvenance, ArtifactRole, Manifest, ModelLock};
    use crate::download;
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
            let label = match change {
                Change::Missing => {
                    std::fs::remove_file(model_dir.join("verification-receipt.json")).unwrap();
                    "missing receipt"
                }
                Change::Corrupt => {
                    std::fs::write(model_dir.join("verification-receipt.json"), b"{not-json")
                        .unwrap();
                    "corrupt receipt"
                }
                Change::Manifest => {
                    candidate.sha256 = "f".repeat(64);
                    "manifest mismatch"
                }
                Change::SameSizeReplacement => {
                    let replacement = model_dir.join("replacement.gguf");
                    std::fs::write(&replacement, b"xyz").unwrap();
                    std::fs::rename(replacement, &primary).unwrap();
                    "same-size replacement"
                }
            };

            verify_or_refresh(&model_lock, &model_dir, &candidate, &primary, None, || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, None))
            })
            .unwrap();

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

        receipt::refresh(&model_lock, &manifest, &primary, &verified, None).unwrap();

        assert_eq!(std::fs::read(&outside).unwrap(), b"outside witness");
        assert!(std::fs::symlink_metadata(receipt_path)
            .unwrap()
            .file_type()
            .is_file());
    }
}
