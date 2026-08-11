use std::fs;
use std::path::Path;

#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::hex;
use super::{discover, Candidate, CandidateKind, FileIdentity};
use crate::catalog::{
    self, Artifact, ArtifactProvenance, ArtifactRole, BundlePending, Manifest, NoReplaceRename,
    Origin, RuntimeQualification, GEMMA4_DRAFT_SHA256, GEMMA4_DRAFT_SIZE, GEMMA4_LLAMA_BUILD,
    GEMMA4_MODEL_SHA256, GEMMA4_MODEL_SIZE, GEMMA4_MTP_PROFILE,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UpgradePoint {
    BeforePrimaryVerification,
    BeforePending,
    BeforeDraft,
    AfterDraft,
}

pub(super) struct BundleQualification {
    pub(super) profile: String,
    pub(super) build: String,
    pub(super) target_sha256: String,
    pub(super) target_size: u64,
    draft_sha256: String,
    draft_size: u64,
}

impl BundleQualification {
    fn production() -> Self {
        Self {
            profile: GEMMA4_MTP_PROFILE.into(),
            build: GEMMA4_LLAMA_BUILD.into(),
            target_sha256: GEMMA4_MODEL_SHA256.into(),
            target_size: GEMMA4_MODEL_SIZE,
            draft_sha256: GEMMA4_DRAFT_SHA256.into(),
            draft_size: GEMMA4_DRAFT_SIZE,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(target: &[u8], draft: &[u8]) -> Self {
        Self {
            profile: catalog::TEST_MTP_PROFILE.into(),
            build: catalog::TEST_LLAMA_BUILD.into(),
            target_sha256: hex(Sha256::digest(target).as_ref()),
            target_size: target.len() as u64,
            draft_sha256: hex(Sha256::digest(draft).as_ref()),
            draft_size: draft.len() as u64,
        }
    }
}

pub fn reconcile_qualified_bundle(models_root: &Path) -> Result<Option<Manifest>, String> {
    reconcile_with(models_root, &BundleQualification::production())
}

pub(super) fn reconcile_with(
    models_root: &Path,
    qualification: &BundleQualification,
) -> Result<Option<Manifest>, String> {
    reconcile_with_observer(models_root, qualification, |_| Ok(()))
}

#[cfg(test)]
pub(super) fn reconcile_with_hook<F>(
    models_root: &Path,
    qualification: &BundleQualification,
    observer: F,
) -> Result<Option<Manifest>, String>
where
    F: FnMut(UpgradePoint) -> Result<(), String>,
{
    reconcile_with_observer(models_root, qualification, observer)
}

fn reconcile_with_observer<F>(
    models_root: &Path,
    qualification: &BundleQualification,
    mut observer: F,
) -> Result<Option<Manifest>, String>
where
    F: FnMut(UpgradePoint) -> Result<(), String>,
{
    let targets = catalog::load_catalog(models_root)?
        .into_iter()
        .filter(|manifest| qualified_target(manifest, qualification))
        .collect::<Vec<_>>();
    if targets.len() != 1 {
        return Ok(None);
    }
    let expected = targets.into_iter().next().expect("one target exists");
    let model_dir = models_root.join(&expected.id);
    let _lock = catalog::ModelLock::acquire(&model_dir)?;
    let Some(current) = catalog::load_catalog(models_root)?
        .into_iter()
        .find(|manifest| manifest.id == expected.id)
    else {
        return Ok(None);
    };
    if current != expected || !qualified_target(&current, qualification) {
        return Ok(None);
    }
    if qualified_draft(&current, qualification) {
        if !complete_bundle_needs_recovery_cleanup(&model_dir, &current) {
            return Ok(Some(current));
        }
        if complete_bundle_is_verified(models_root, &current, qualification) {
            let _ = catalog::cleanup_completed_bundle_debris(&model_dir, &current);
            return Ok(Some(current));
        }
        return Ok(None);
    }
    let pending = catalog::bundle_pending(&model_dir);
    if matches!(pending, BundlePending::UnsafeOrInvalid) {
        return Ok(None);
    }
    let candidate_pool = discover(models_root)?
        .into_iter()
        .filter(|candidate| {
            candidate.kind == CandidateKind::Auxiliary && candidate.size == qualification.draft_size
        })
        .collect::<Vec<_>>();
    if matches!(pending, BundlePending::Absent) && candidate_pool.is_empty() {
        return Ok(None);
    }
    observer(UpgradePoint::BeforePrimaryVerification)?;
    if crate::download::verify_regular(
        &current.artifact_path(models_root),
        qualification.target_size,
        &qualification.target_sha256,
    )
    .is_err()
    {
        return Ok(None);
    }
    match pending {
        BundlePending::Absent => {}
        BundlePending::UnsafeOrInvalid => unreachable!("unsafe pending state returned above"),
        BundlePending::Valid(pending) => {
            if !matching_pending(&current, &pending, qualification) {
                return Ok(None);
            }
            match managed_draft_state(&model_dir, qualification) {
                ManagedDraft::Verified => {
                    return if catalog::replace_manifest_atomic(models_root, &current, &pending)
                        .unwrap_or(false)
                    {
                        Ok(Some(*pending))
                    } else {
                        Ok(None)
                    };
                }
                ManagedDraft::UnsafeOrInvalid => return Ok(None),
                ManagedDraft::Missing => {}
            }
        }
    }
    let candidates = candidate_pool
        .into_iter()
        .filter(|candidate| verified_candidate(candidate, qualification))
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Ok(None);
    }
    let candidate = candidates.into_iter().next().expect("one draft exists");
    let Ok(before) = FileIdentity::read(&candidate.path) else {
        return Ok(None);
    };
    if before.size != qualification.draft_size
        || crate::download::verify_regular(
            &candidate.path,
            qualification.draft_size,
            &qualification.draft_sha256,
        )
        .is_err()
        || !FileIdentity::read(&candidate.path).is_ok_and(|after| after == before)
    {
        return Ok(None);
    }
    let Ok(replacement) = bundle_manifest(&current, &candidate, qualification) else {
        return Ok(None);
    };
    let destination = model_dir.join("draft.gguf");
    observer(UpgradePoint::BeforePending)?;
    if !catalog::prepare_bundle_upgrade(&model_dir, &replacement).unwrap_or(false) {
        return Ok(None);
    }
    if !FileIdentity::read(&candidate.path).is_ok_and(|after| after == before) {
        return Ok(None);
    }
    observer(UpgradePoint::BeforeDraft)?;
    if !matches!(
        catalog::move_no_replace_and_sync(&candidate.path, &destination),
        Ok(NoReplaceRename::Renamed)
    ) {
        return Ok(None);
    }
    if crate::download::verify_regular(
        &destination,
        qualification.draft_size,
        &qualification.draft_sha256,
    )
    .is_err()
    {
        return Ok(None);
    }
    observer(UpgradePoint::AfterDraft)?;
    if !catalog::replace_manifest_atomic(models_root, &current, &replacement).unwrap_or(false) {
        return Ok(None);
    }
    Ok(Some(replacement))
}

fn complete_bundle_needs_recovery_cleanup(model_dir: &Path, complete: &Manifest) -> bool {
    let Ok(entries) = fs::read_dir(model_dir) else {
        return true;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return true;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if catalog::removable_bundle_debris(&entry.path(), name, complete) {
            return true;
        }
    }
    false
}

fn complete_bundle_is_verified(
    models_root: &Path,
    manifest: &Manifest,
    qualification: &BundleQualification,
) -> bool {
    crate::download::verify_regular(
        &manifest.artifact_path(models_root),
        qualification.target_size,
        &qualification.target_sha256,
    )
    .is_ok()
        && manifest.draft_artifact().is_some_and(|draft| {
            draft.local_filename == "draft.gguf"
                && draft.size == qualification.draft_size
                && draft.sha256 == qualification.draft_sha256
                && crate::download::verify_regular(
                    &models_root.join(&manifest.id).join(draft.local_filename),
                    draft.size,
                    draft.sha256,
                )
                .is_ok()
        })
}

fn matching_pending(
    current: &Manifest,
    pending: &Manifest,
    qualification: &BundleQualification,
) -> bool {
    current.id == pending.id
        && current.local_filename == pending.local_filename
        && current.sha256 == pending.sha256
        && current.size == pending.size
        && matching_primary_provenance(current, pending)
        && qualified_target(pending, qualification)
        && qualified_draft(pending, qualification)
}

fn matching_primary_provenance(current: &Manifest, pending: &Manifest) -> bool {
    let Some(pending_model) = pending.artifacts.as_deref().and_then(|artifacts| {
        artifacts
            .iter()
            .find(|artifact| artifact.role == ArtifactRole::Model)
    }) else {
        return false;
    };
    match current.version {
        1 => matches!(
            &pending_model.provenance,
            ArtifactProvenance::HuggingFace {
                repo,
                revision,
                remote_filename,
            } if current.repo.as_deref() == Some(repo.as_str())
                && current.revision.as_deref() == Some(revision.as_str())
                && current.remote_filename.as_deref() == Some(remote_filename.as_str())
        ),
        2 => matches!(
            &pending_model.provenance,
            ArtifactProvenance::Local { source_filename }
                if current.source_filename.as_deref() == Some(source_filename)
        ),
        3 => current
            .artifacts
            .as_deref()
            .and_then(|artifacts| {
                artifacts
                    .iter()
                    .find(|artifact| artifact.role == ArtifactRole::Model)
            })
            .is_some_and(|current_model| current_model.provenance == pending_model.provenance),
        _ => false,
    }
}

enum ManagedDraft {
    Missing,
    Verified,
    UnsafeOrInvalid,
}

fn managed_draft_state(model_dir: &Path, qualification: &BundleQualification) -> ManagedDraft {
    let path = model_dir.join("draft.gguf");
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ManagedDraft::Missing,
        Err(_) => ManagedDraft::UnsafeOrInvalid,
        Ok(_) => {
            let Ok(before) = FileIdentity::read(&path) else {
                return ManagedDraft::UnsafeOrInvalid;
            };
            if before.size == qualification.draft_size
                && crate::download::verify_regular(
                    &path,
                    qualification.draft_size,
                    &qualification.draft_sha256,
                )
                .is_ok()
                && FileIdentity::read(&path).is_ok_and(|after| after == before)
            {
                ManagedDraft::Verified
            } else {
                ManagedDraft::UnsafeOrInvalid
            }
        }
    }
}

fn qualified_target(manifest: &Manifest, qualification: &BundleQualification) -> bool {
    if manifest.sha256 != qualification.target_sha256 || manifest.size != qualification.target_size
    {
        return false;
    }
    match manifest.version {
        1 => true,
        2 => manifest.origin == Some(Origin::Local),
        3 => {
            manifest.profile.as_deref() == Some(qualification.profile.as_str())
                && manifest.runtime.as_ref().is_some_and(|runtime| {
                    runtime.engine == "llama.cpp"
                        && if qualification.profile == GEMMA4_MTP_PROFILE
                            && qualification.build == GEMMA4_LLAMA_BUILD
                        {
                            catalog::is_qualified_gemma4_bundle(manifest)
                        } else {
                            runtime.build == qualification.build
                        }
                })
        }
        _ => false,
    }
}

fn qualified_draft(manifest: &Manifest, qualification: &BundleQualification) -> bool {
    manifest.draft_artifact().is_some_and(|draft| {
        draft.local_filename == "draft.gguf"
            && draft.sha256 == qualification.draft_sha256
            && draft.size == qualification.draft_size
    })
}

fn verified_candidate(candidate: &Candidate, qualification: &BundleQualification) -> bool {
    candidate.size == qualification.draft_size
        && crate::download::verify_regular(
            &candidate.path,
            qualification.draft_size,
            &qualification.draft_sha256,
        )
        .is_ok()
}

fn bundle_manifest(
    current: &Manifest,
    draft: &Candidate,
    qualification: &BundleQualification,
) -> Result<Manifest, String> {
    let model_provenance = match current.version {
        1 => ArtifactProvenance::HuggingFace {
            repo: current
                .repo
                .clone()
                .ok_or("missing Hugging Face repository")?,
            revision: current
                .revision
                .clone()
                .ok_or("missing Hugging Face revision")?,
            remote_filename: current
                .remote_filename
                .clone()
                .ok_or("missing Hugging Face filename")?,
        },
        2 => ArtifactProvenance::Local {
            source_filename: current
                .source_filename
                .clone()
                .ok_or("missing local source filename")?,
        },
        3 => current
            .artifacts
            .as_deref()
            .and_then(|artifacts| {
                artifacts
                    .iter()
                    .find(|artifact| artifact.role == ArtifactRole::Model)
            })
            .map(|artifact| artifact.provenance.clone())
            .ok_or("missing model provenance")?,
        _ => return Err("unsupported bundle target manifest".into()),
    };
    let manifest = Manifest {
        version: 3,
        id: current.id.clone(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: qualification.target_sha256.clone(),
        size: qualification.target_size,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: qualification.target_sha256.clone(),
                size: qualification.target_size,
                provenance: model_provenance,
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: qualification.draft_sha256.clone(),
                size: qualification.draft_size,
                provenance: ArtifactProvenance::Local {
                    source_filename: draft.filename.clone(),
                },
            },
        ]),
        profile: Some(qualification.profile.clone()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: qualification.build.clone(),
        }),
    };
    manifest.validate()?;
    Ok(manifest)
}
