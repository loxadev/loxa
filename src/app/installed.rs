use crate::catalog::{self, Manifest};
use crate::huggingface::ResolvedFile;
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledModelSummary {
    id: String,
    display_name: String,
    total_bytes: u64,
    remote_identity: Option<RemoteIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RemoteIdentity {
    repo: String,
    commit: String,
    path: String,
    sha256: String,
    size: u64,
}

impl InstalledModelSummary {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn matches_remote(&self, artifact: &ResolvedFile) -> bool {
        self.remote_identity.as_ref().is_some_and(|identity| {
            identity.repo == artifact.repo()
                && identity.commit == artifact.commit()
                && identity.path == artifact.path()
                && identity.sha256 == artifact.sha256()
                && identity.size == artifact.size()
        })
    }
}

pub(super) fn load(models_root: &Path) -> Result<Vec<InstalledModelSummary>, String> {
    catalog::load_catalog(models_root)?
        .into_iter()
        .map(from_manifest)
        .collect()
}

pub(super) fn exact_remote_model_id(
    models_root: &Path,
    artifact: &ResolvedFile,
) -> Result<Option<String>, String> {
    Ok(load(models_root)?
        .into_iter()
        .find(|summary| summary.matches_remote(artifact))
        .map(|summary| summary.id))
}

fn from_manifest(manifest: Manifest) -> Result<InstalledModelSummary, String> {
    let display_name = manifest.description().1.to_owned();
    let total_bytes = manifest.total_size();
    let remote_identity = match &manifest {
        Manifest {
            version: 1,
            repo: Some(repo),
            revision: Some(commit),
            remote_filename: Some(path),
            sha256,
            size,
            ..
        } => Some(RemoteIdentity {
            repo: repo.clone(),
            commit: commit.clone(),
            path: path.clone(),
            sha256: sha256.clone(),
            size: *size,
        }),
        _ => None,
    };
    Ok(InstalledModelSummary {
        id: manifest.id,
        display_name,
        total_bytes,
        remote_identity,
    })
}
