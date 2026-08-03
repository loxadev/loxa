use std::fs;
use std::path::Path;

#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::hex;
use super::{discover, sha256, Candidate, CandidateKind, FileIdentity};
use crate::catalog::{
    self, Artifact, ArtifactProvenance, ArtifactRole, Manifest, Origin, RuntimeQualification,
    GEMMA4_DRAFT_SHA256, GEMMA4_DRAFT_SIZE, GEMMA4_LLAMA_BUILD, GEMMA4_MODEL_SHA256,
    GEMMA4_MODEL_SIZE, GEMMA4_MTP_PROFILE,
};

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
    let targets = catalog::load_catalog(models_root)?
        .into_iter()
        .filter(|manifest| qualified_target(manifest, qualification))
        .collect::<Vec<_>>();
    if targets.len() != 1 {
        return Ok(None);
    }
    let expected = targets.into_iter().next().expect("one target exists");
    if qualified_draft(&expected, qualification) {
        return Ok(Some(expected));
    }

    let candidates = discover(models_root)?
        .into_iter()
        .filter(|candidate| {
            candidate.kind == CandidateKind::Auxiliary && candidate.size == qualification.draft_size
        })
        .filter(|candidate| verified_candidate(candidate, qualification))
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Ok(None);
    }
    let candidate = candidates.into_iter().next().expect("one draft exists");
    let model_dir = models_root.join(&expected.id);
    let _lock = catalog::ModelLock::acquire(&model_dir)?;
    let current = catalog::load_catalog(models_root)?
        .into_iter()
        .find(|manifest| manifest.id == expected.id)
        .ok_or_else(|| format!("model {} disappeared during bundle upgrade", expected.id))?;
    if current != expected || !qualified_target(&current, qualification) {
        return Err(format!(
            "model {} changed during bundle upgrade",
            expected.id
        ));
    }
    crate::download::verify_regular(
        &current.artifact_path(models_root),
        qualification.target_size,
        &qualification.target_sha256,
    )?;
    let before = FileIdentity::read(&candidate.path)?;
    if before.size != qualification.draft_size
        || sha256(&candidate.path)? != qualification.draft_sha256
        || FileIdentity::read(&candidate.path)? != before
    {
        return Ok(None);
    }
    let replacement = bundle_manifest(&current, &candidate, qualification)?;
    let destination = model_dir.join("draft.gguf");
    if fs::symlink_metadata(&destination).is_ok() {
        return Ok(None);
    }
    catalog::prepare_bundle_upgrade(&model_dir, &replacement)?;
    if FileIdentity::read(&candidate.path)? != before {
        fs::remove_file(model_dir.join("bundle.pending.json"))
            .map_err(|error| error.to_string())?;
        return Ok(None);
    }
    fs::rename(&candidate.path, &destination).map_err(|error| {
        format!(
            "failed to adopt draft {} into {}: {error}",
            candidate.path.display(),
            destination.display()
        )
    })?;
    crate::download::verify_regular(
        &destination,
        qualification.draft_size,
        &qualification.draft_sha256,
    )?;
    catalog::replace_manifest_atomic(models_root, &current, &replacement)?;
    Ok(Some(replacement))
}

fn qualified_target(manifest: &Manifest, qualification: &BundleQualification) -> bool {
    if manifest.sha256 != qualification.target_sha256 || manifest.size != qualification.target_size
    {
        return false;
    }
    match manifest.version {
        2 => manifest.origin == Some(Origin::Local),
        3 => {
            manifest.profile.as_deref() == Some(qualification.profile.as_str())
                && manifest.runtime.as_ref().is_some_and(|runtime| {
                    runtime.engine == "llama.cpp" && runtime.build == qualification.build
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
    let Ok(before) = FileIdentity::read(&candidate.path) else {
        return false;
    };
    before.size == qualification.draft_size
        && sha256(&candidate.path).is_ok_and(|digest| digest == qualification.draft_sha256)
        && FileIdentity::read(&candidate.path).is_ok_and(|after| after == before)
}

fn bundle_manifest(
    current: &Manifest,
    draft: &Candidate,
    qualification: &BundleQualification,
) -> Result<Manifest, String> {
    let model_provenance = match current.version {
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
