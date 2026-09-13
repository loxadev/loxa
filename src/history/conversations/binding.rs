use super::super::{HistoryError, HistoryErrorKind};
use crate::catalog::{ArtifactProvenance, ArtifactRole, Manifest};
use crate::runtime_identity::RuntimeIdentity;

const MAX_ARTIFACT_IDENTITY_BYTES: usize = 1024;
const MAX_REPOSITORY_BYTES: usize = 256;

pub(super) struct Binding {
    pub(super) model_id: String,
    pub(super) manifest_version: i64,
    pub(super) effective_profile: i64,
    pub(super) qualified_profile: Option<String>,
    pub(super) qualified_engine: Option<String>,
    pub(super) qualified_engine_build: Option<String>,
    pub(super) primary: BoundArtifact,
    pub(super) draft: Option<BoundArtifact>,
}

pub(super) struct BoundArtifact {
    pub(super) local_filename: String,
    pub(super) sha256: [u8; 32],
    pub(super) size: i64,
    pub(super) source: BoundSource,
}

pub(super) enum BoundSource {
    Local {
        filename: String,
    },
    HuggingFace {
        repo: String,
        revision: String,
        filename: String,
    },
}

impl Binding {
    pub(super) fn load(
        models_root: &std::path::Path,
        model_id: &str,
        runtime_identity: RuntimeIdentity,
    ) -> Result<Self, HistoryError> {
        crate::paths::validate_id(model_id).map_err(invalid_binding)?;
        let manifest = crate::catalog::load_model_manifest(models_root, model_id)
            .map_err(|_| HistoryError::new(HistoryErrorKind::Io, "model catalog is unavailable"))?
            .ok_or_else(|| {
                HistoryError::new(HistoryErrorKind::NotFound, "model is not installed")
            })?;
        Self::from_manifest(&manifest, runtime_identity)
    }

    fn from_manifest(
        manifest: &Manifest,
        runtime_identity: RuntimeIdentity,
    ) -> Result<Self, HistoryError> {
        manifest.validate().map_err(invalid_binding)?;
        let effective_profile = match manifest.version {
            1 | 2 => 0,
            3 if runtime_identity.supports_manifest(manifest) => 1,
            _ => return Err(invalid_binding("model has no supported runtime profile")),
        };
        let primary = bound_artifact(manifest, ArtifactRole::Model, manifest.primary_artifact())?;
        let draft = manifest
            .draft_artifact()
            .map(|artifact| bound_artifact(manifest, ArtifactRole::Draft, artifact))
            .transpose()?;
        if (effective_profile == 1) != draft.is_some() {
            return Err(invalid_binding(
                "model artifact binding contradicts its runtime profile",
            ));
        }
        let (qualified_profile, qualified_engine, qualified_engine_build) =
            if effective_profile == 1 {
                let profile = bounded_text(
                    manifest
                        .profile
                        .as_deref()
                        .ok_or_else(|| invalid_binding("missing qualified bundle profile"))?,
                    64,
                    "qualified bundle profile exceeds the history binding limit",
                )?;
                let runtime = manifest
                    .runtime
                    .as_ref()
                    .ok_or_else(|| invalid_binding("missing qualified runtime"))?;
                (
                    Some(profile),
                    Some(bounded_text(
                        &runtime.engine,
                        64,
                        "qualified engine exceeds the history binding limit",
                    )?),
                    Some(bounded_text(
                        &runtime.build,
                        64,
                        "qualified engine build exceeds the history binding limit",
                    )?),
                )
            } else {
                (None, None, None)
            };
        Ok(Self {
            model_id: manifest.id.clone(),
            manifest_version: i64::from(manifest.version),
            effective_profile,
            qualified_profile,
            qualified_engine,
            qualified_engine_build,
            primary,
            draft,
        })
    }
}

fn bound_artifact(
    manifest: &Manifest,
    role: ArtifactRole,
    artifact: crate::catalog::ArtifactRef<'_>,
) -> Result<BoundArtifact, HistoryError> {
    validate_text(
        artifact.local_filename,
        MAX_ARTIFACT_IDENTITY_BYTES,
        "model filename exceeds the history binding limit",
    )?;
    let source = match manifest.version {
        1 => BoundSource::HuggingFace {
            repo: bounded_text(
                manifest
                    .repo
                    .as_deref()
                    .ok_or_else(|| invalid_binding("missing repository"))?,
                MAX_REPOSITORY_BYTES,
                "model repository exceeds the history binding limit",
            )?,
            revision: manifest
                .revision
                .clone()
                .ok_or_else(|| invalid_binding("missing revision"))?,
            filename: bounded_text(
                manifest
                    .remote_filename
                    .as_deref()
                    .ok_or_else(|| invalid_binding("missing remote filename"))?,
                MAX_ARTIFACT_IDENTITY_BYTES,
                "model source filename exceeds the history binding limit",
            )?,
        },
        2 => BoundSource::Local {
            filename: bounded_text(
                manifest
                    .source_filename
                    .as_deref()
                    .ok_or_else(|| invalid_binding("missing source filename"))?,
                MAX_ARTIFACT_IDENTITY_BYTES,
                "model source filename exceeds the history binding limit",
            )?,
        },
        3 => {
            let item = manifest
                .artifacts
                .as_deref()
                .and_then(|items| items.iter().find(|item| item.role == role))
                .ok_or_else(|| invalid_binding("missing bundle artifact"))?;
            match &item.provenance {
                ArtifactProvenance::Local { source_filename } => BoundSource::Local {
                    filename: bounded_text(
                        source_filename,
                        MAX_ARTIFACT_IDENTITY_BYTES,
                        "model source filename exceeds the history binding limit",
                    )?,
                },
                ArtifactProvenance::HuggingFace {
                    repo,
                    revision,
                    remote_filename,
                } => BoundSource::HuggingFace {
                    repo: bounded_text(
                        repo,
                        MAX_REPOSITORY_BYTES,
                        "model repository exceeds the history binding limit",
                    )?,
                    revision: revision.clone(),
                    filename: bounded_text(
                        remote_filename,
                        MAX_ARTIFACT_IDENTITY_BYTES,
                        "model source filename exceeds the history binding limit",
                    )?,
                },
            }
        }
        _ => return Err(invalid_binding("unsupported manifest version")),
    };
    let size = i64::try_from(artifact.size)
        .map_err(|_| invalid_binding("model artifact size exceeds storage range"))?;
    Ok(BoundArtifact {
        local_filename: artifact.local_filename.to_owned(),
        sha256: decode_sha256(artifact.sha256)?,
        size,
        source,
    })
}

pub(super) fn source_columns(source: &BoundSource) -> (i64, Option<&str>, Option<&str>, &str) {
    match source {
        BoundSource::Local { filename } => (0, None, None, filename),
        BoundSource::HuggingFace {
            repo,
            revision,
            filename,
        } => (1, Some(repo), Some(revision), filename),
    }
}

fn decode_sha256(value: &str) -> Result<[u8; 32], HistoryError> {
    if value.len() != 64 {
        return Err(invalid_binding("invalid model SHA-256"));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Ok(bytes)
}

fn hex_digit(byte: u8) -> Result<u8, HistoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid_binding("invalid model SHA-256")),
    }
}

fn validate_text(value: &str, maximum: usize, context: &'static str) -> Result<(), HistoryError> {
    if value.is_empty() || value.len() > maximum {
        Err(invalid_binding(context))
    } else {
        Ok(())
    }
}

fn bounded_text(
    value: &str,
    maximum: usize,
    context: &'static str,
) -> Result<String, HistoryError> {
    validate_text(value, maximum, context)?;
    Ok(value.to_owned())
}

fn invalid_binding(context: impl Into<String>) -> HistoryError {
    HistoryError::new(HistoryErrorKind::InvalidInput, context)
}
