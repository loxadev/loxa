//! Manifest schema and the concrete runtime qualification facts it validates.
use crate::paths::validate_id;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const GEMMA4_MTP_PROFILE: &str = "gemma4-mtp-v1";
pub const GEMMA4_LEGACY_LLAMA_BUILD: &str = "b10121";
pub const GEMMA4_BUNDLED_LLAMA_BUILD: &str = "b10344";
pub const GEMMA4_LLAMA_BUILD: &str = GEMMA4_BUNDLED_LLAMA_BUILD;
pub const GEMMA4_MODEL_SHA256: &str =
    "90fd44e29e0d7cffeb0fd00dc73cfdab9ed0b0e95306ecf7821ea634c940c370";
pub const GEMMA4_MODEL_SIZE: u64 = 6_716_356_800;
pub const GEMMA4_DRAFT_SHA256: &str =
    "fcb35dea42c71333db904cee11baac525c9ef872818ee3753f6cb156f3c6f4f6";
pub const GEMMA4_DRAFT_SIZE: u64 = 253_708_800;
#[cfg(test)]
pub(crate) const TEST_MTP_PROFILE: &str = "test-mtp-v1";
#[cfg(test)]
pub(crate) const TEST_LLAMA_BUILD: &str = "test-build";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_filename: Option<String>,
    pub local_filename: String,
    pub sha256: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<Vec<Artifact>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeQualification>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactRole {
    Model,
    Draft,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub role: ArtifactRole,
    pub local_filename: String,
    pub sha256: String,
    pub size: u64,
    pub provenance: ArtifactProvenance,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactProvenance {
    Local {
        source_filename: String,
    },
    HuggingFace {
        repo: String,
        revision: String,
        remote_filename: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeQualification {
    pub engine: String,
    pub build: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactRef<'a> {
    pub role: ArtifactRole,
    pub local_filename: &'a str,
    pub sha256: &'a str,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Local,
}

impl Manifest {
    pub fn validate(&self) -> Result<(), String> {
        validate_id(&self.id)?;
        validate_filename(&self.local_filename)?;
        if self.local_filename != "model.gguf" {
            return Err("local filename must be model.gguf".into());
        }
        validate_hex(&self.sha256, 64, "SHA-256")?;
        if self.size == 0 {
            return Err("invalid manifest size".into());
        }
        match self.version {
            1 if self.origin.is_none()
                && self.source_filename.is_none()
                && self.artifacts.is_none()
                && self.profile.is_none()
                && self.runtime.is_none() =>
            {
                validate_repo(self.repo.as_deref().ok_or("missing repository")?)?;
                validate_hex(
                    self.revision.as_deref().ok_or("missing revision")?,
                    40,
                    "revision",
                )?;
                validate_filename(
                    self.remote_filename
                        .as_deref()
                        .ok_or("missing remote filename")?,
                )
            }
            2 if self.origin == Some(Origin::Local)
                && self.repo.is_none()
                && self.revision.is_none()
                && self.remote_filename.is_none()
                && self.artifacts.is_none()
                && self.profile.is_none()
                && self.runtime.is_none() =>
            {
                validate_filename(
                    self.source_filename
                        .as_deref()
                        .ok_or("missing local source filename")?,
                )
            }
            3 if self.repo.is_none()
                && self.revision.is_none()
                && self.remote_filename.is_none()
                && self.origin.is_none()
                && self.source_filename.is_none() =>
            {
                self.validate_bundle()
            }
            _ => Err("invalid manifest origin or version".into()),
        }
    }

    fn validate_bundle(&self) -> Result<(), String> {
        let artifacts = self
            .artifacts
            .as_deref()
            .ok_or("missing bundle artifacts")?;
        let profile = self.profile.as_deref().ok_or("missing bundle profile")?;
        let runtime = self.runtime.as_ref().ok_or("missing qualified runtime")?;
        let production_profile = qualified_gemma4_runtime(profile, runtime);
        #[cfg(test)]
        let test_profile = profile == TEST_MTP_PROFILE
            && runtime.engine == "llama.cpp"
            && runtime.build == TEST_LLAMA_BUILD;
        #[cfg(not(test))]
        let test_profile = false;
        if !production_profile && !test_profile {
            return Err("unknown bundle profile".into());
        }
        let mut model = None;
        let mut draft = None;
        for artifact in artifacts {
            validate_filename(&artifact.local_filename)?;
            validate_hex(&artifact.sha256, 64, "SHA-256")?;
            if artifact.size == 0 {
                return Err("invalid artifact size".into());
            }
            validate_provenance(&artifact.provenance)?;
            match artifact.role {
                ArtifactRole::Model if model.replace(artifact).is_none() => {}
                ArtifactRole::Draft if draft.replace(artifact).is_none() => {}
                _ => return Err("duplicate bundle artifact role".into()),
            }
        }
        let model = model.ok_or("missing model artifact")?;
        if model.local_filename != "model.gguf"
            || draft.is_some_and(|artifact| artifact.local_filename != "draft.gguf")
        {
            return Err("invalid managed bundle filenames".into());
        }
        if production_profile && draft.is_none() {
            return Err("qualified production bundle is missing its draft artifact".into());
        }
        if production_profile
            && (model.sha256 != GEMMA4_MODEL_SHA256 || model.size != GEMMA4_MODEL_SIZE)
        {
            return Err("bundle does not match qualified profile".into());
        }
        if let Some(draft) = draft {
            if production_profile
                && (draft.sha256 != GEMMA4_DRAFT_SHA256 || draft.size != GEMMA4_DRAFT_SIZE)
            {
                return Err("bundle does not match qualified profile".into());
            }
        }
        if self.local_filename != model.local_filename
            || self.sha256 != model.sha256
            || self.size != model.size
        {
            return Err("bundle primary metadata does not match model artifact".into());
        }
        Ok(())
    }

    pub fn primary_artifact(&self) -> ArtifactRef<'_> {
        match self.artifacts.as_deref() {
            Some(artifacts) => artifact_ref(
                artifacts
                    .iter()
                    .find(|artifact| artifact.role == ArtifactRole::Model)
                    .expect("validated bundle has a model artifact"),
            ),
            None => ArtifactRef {
                role: ArtifactRole::Model,
                local_filename: &self.local_filename,
                sha256: &self.sha256,
                size: self.size,
            },
        }
    }

    pub fn draft_artifact(&self) -> Option<ArtifactRef<'_>> {
        self.artifacts.as_deref()?.iter().find_map(|artifact| {
            (artifact.role == ArtifactRole::Draft).then(|| artifact_ref(artifact))
        })
    }

    pub fn total_size(&self) -> u64 {
        self.artifacts.as_deref().map_or(self.size, |artifacts| {
            artifacts.iter().map(|item| item.size).sum()
        })
    }

    pub fn description(&self) -> (&str, &str, Option<&str>) {
        if let Some(artifacts) = self.artifacts.as_deref() {
            let model = artifacts
                .iter()
                .find(|artifact| artifact.role == ArtifactRole::Model)
                .expect("validated bundle has a model artifact");
            return match &model.provenance {
                ArtifactProvenance::Local { source_filename } => {
                    ("local file", source_filename, None)
                }
                ArtifactProvenance::HuggingFace {
                    repo,
                    revision,
                    remote_filename,
                } => (repo, remote_filename, Some(revision)),
            };
        }
        match self.origin {
            Some(Origin::Local) => (
                "local file",
                self.source_filename
                    .as_deref()
                    .expect("validated local manifest has source filename"),
                None,
            ),
            None => (
                self.repo
                    .as_deref()
                    .expect("validated HF manifest has repository"),
                self.remote_filename
                    .as_deref()
                    .expect("validated HF manifest has remote filename"),
                Some(
                    self.revision
                        .as_deref()
                        .expect("validated HF manifest has revision"),
                ),
            ),
        }
    }

    pub fn artifact_path(&self, models_root: &Path) -> PathBuf {
        models_root
            .join(&self.id)
            .join(self.primary_artifact().local_filename)
    }

    pub fn draft_path(&self, models_root: &Path) -> Option<PathBuf> {
        self.draft_artifact()
            .map(|artifact| models_root.join(&self.id).join(artifact.local_filename))
    }
}

fn qualified_gemma4_runtime(profile: &str, runtime: &RuntimeQualification) -> bool {
    profile == GEMMA4_MTP_PROFILE
        && runtime.engine == "llama.cpp"
        && matches!(
            runtime.build.as_str(),
            GEMMA4_LEGACY_LLAMA_BUILD | GEMMA4_BUNDLED_LLAMA_BUILD
        )
}

pub fn is_qualified_gemma4_bundle(manifest: &Manifest) -> bool {
    manifest.validate().is_ok()
        && manifest.version == 3
        && manifest
            .profile
            .as_deref()
            .zip(manifest.runtime.as_ref())
            .is_some_and(|(profile, runtime)| qualified_gemma4_runtime(profile, runtime))
}

fn artifact_ref(artifact: &Artifact) -> ArtifactRef<'_> {
    ArtifactRef {
        role: artifact.role,
        local_filename: &artifact.local_filename,
        sha256: &artifact.sha256,
        size: artifact.size,
    }
}

fn validate_provenance(provenance: &ArtifactProvenance) -> Result<(), String> {
    match provenance {
        ArtifactProvenance::Local { source_filename } => validate_filename(source_filename),
        ArtifactProvenance::HuggingFace {
            repo,
            revision,
            remote_filename,
        } => {
            validate_repo(repo)?;
            validate_hex(revision, 40, "revision")?;
            validate_filename(remote_filename)
        }
    }
}

fn validate_repo(repo: &str) -> Result<(), String> {
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty() && *part != "." && *part != ".." && !part.chars().any(char::is_control)
        })
    {
        Ok(())
    } else {
        Err("repository must be owner/repo".into())
    }
}

fn validate_filename(filename: &str) -> Result<(), String> {
    let lower = filename.to_ascii_lowercase();
    if !filename.is_empty()
        && !filename.contains(['/', '\\'])
        && !filename.contains("..")
        && !filename.chars().any(char::is_control)
        && lower.ends_with(".gguf")
        && !lower.contains("-of-")
    {
        Ok(())
    } else {
        Err(format!("invalid single-file GGUF name {filename:?}"))
    }
}

fn validate_hex(value: &str, len: usize, label: &str) -> Result<(), String> {
    if value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(format!("invalid {label}"))
    }
}
