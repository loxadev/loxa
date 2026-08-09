mod receipt;

use crate::catalog::{Manifest, ModelLock};
use crate::download::{self, VerifiedRegularFile};
use std::path::Path;

pub(crate) struct VerifiedArtifacts {
    pub(crate) primary: VerifiedRegularFile,
    pub(crate) draft: Option<VerifiedRegularFile>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    ReceiptHit,
    Verified,
}

pub(crate) fn verify_or_refresh<F>(
    model_lock: &ModelLock,
    model_dir: &Path,
    manifest: &Manifest,
    primary: &Path,
    draft: Option<&Path>,
    verify: F,
) -> Result<Admission, String>
where
    F: FnOnce() -> Result<VerifiedArtifacts, String>,
{
    if receipt::matches(model_lock, model_dir, manifest, primary, draft)? {
        return Ok(Admission::ReceiptHit);
    }
    receipt::discard(model_lock, model_dir)?;
    let verified = verify()?;
    let verified_draft = match (draft, verified.draft.as_ref()) {
        (Some(path), Some(file)) => Some((path, file)),
        (None, None) => None,
        _ => return Err("verified draft artifact mismatch".into()),
    };
    refresh_verified(
        model_lock,
        manifest,
        primary,
        &verified.primary,
        verified_draft,
    )?;
    Ok(Admission::Verified)
}

pub(crate) fn verify_artifacts(
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

pub(crate) fn refresh_verified(
    model_lock: &ModelLock,
    manifest: &Manifest,
    primary_path: &Path,
    primary: &VerifiedRegularFile,
    draft: Option<(&Path, &VerifiedRegularFile)>,
) -> Result<(), String> {
    let primary_artifact = manifest.primary_artifact();
    primary.proves(primary_path, primary_artifact.size, primary_artifact.sha256)?;
    match (manifest.draft_artifact(), draft) {
        (Some(artifact), Some((path, verified))) => {
            verified.proves(path, artifact.size, artifact.sha256)?;
        }
        (None, None) => {}
        _ => return Err("verified draft artifact mismatch".into()),
    }
    receipt::refresh(model_lock, manifest, primary_path, primary, draft)
}
