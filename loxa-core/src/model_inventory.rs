//! Read-only inventory for the compiled, verified model recipes.

use crate::registry::{ModelEntry, REGISTRY};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

const GIB: f64 = 1_073_741_824.0;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    NotDownloaded,
    Partial { bytes: u64 },
    Downloaded,
    Invalid { reason: ArtifactInvalidReason },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactInvalidReason {
    SizeMismatch,
    ChecksumMismatch,
    Unreadable,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Compatibility {
    pub compatible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct EngineEligibility {
    pub engine: String,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct VerifiedRecipeInventoryEntry {
    pub id: String,
    pub repo: String,
    pub revision: String,
    pub filename: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub license: String,
    pub params: String,
    pub quant: String,
    pub min_free_mem_gb: f32,
    pub artifact: ArtifactState,
    pub compatibility: Compatibility,
    pub engine: EngineEligibility,
}

/// Inspects only Loxa's compiled verified recipes. These recipes are not an
/// allowlist for the wider model-intake product.
pub fn known_registry_inventory(
    models_dir: &Path,
    available_memory_bytes: u64,
) -> Vec<VerifiedRecipeInventoryEntry> {
    REGISTRY
        .iter()
        .map(|recipe| inspect_recipe(recipe, models_dir, available_memory_bytes))
        .collect()
}

fn inspect_recipe(
    recipe: &ModelEntry,
    models_dir: &Path,
    available_memory_bytes: u64,
) -> VerifiedRecipeInventoryEntry {
    let required = (recipe.min_free_mem_gb as f64 * GIB).round() as u64;
    let compatibility = if available_memory_bytes >= required {
        Compatibility {
            compatible: true,
            reason: "available memory meets the verified recipe minimum".into(),
        }
    } else {
        Compatibility {
            compatible: false,
            reason: format!(
                "requires {:.1} GiB free memory; {:.1} GiB is available",
                recipe.min_free_mem_gb,
                available_memory_bytes as f64 / GIB
            ),
        }
    };

    VerifiedRecipeInventoryEntry {
        id: recipe.id.into(),
        repo: recipe.repo.into(),
        revision: recipe.revision.into(),
        filename: recipe.filename.into(),
        sha256: recipe.sha256.into(),
        size_bytes: recipe.size_bytes,
        license: recipe.license.into(),
        params: recipe.params.into(),
        quant: recipe.quant.into(),
        min_free_mem_gb: recipe.min_free_mem_gb,
        artifact: artifact_state(recipe, models_dir),
        compatibility: compatibility.clone(),
        engine: EngineEligibility {
            engine: "llama-cpp".into(),
            eligible: true,
            reason: "verified GGUF recipe is eligible for the managed llama.cpp engine".into(),
        },
    }
}

fn artifact_state(recipe: &ModelEntry, models_dir: &Path) -> ArtifactState {
    let final_path = models_dir.join(recipe.filename);
    if final_path.exists() {
        return match file_matches(&final_path, recipe) {
            Ok(true) => ArtifactState::Downloaded,
            Ok(false)
                if final_path.metadata().map(|m| m.len()).unwrap_or(0) != recipe.size_bytes =>
            {
                ArtifactState::Invalid {
                    reason: ArtifactInvalidReason::SizeMismatch,
                }
            }
            Ok(false) => ArtifactState::Invalid {
                reason: ArtifactInvalidReason::ChecksumMismatch,
            },
            Err(_) => ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable,
            },
        };
    }

    let part_path = models_dir.join(format!("{}.part", recipe.filename));
    match part_path.metadata() {
        Ok(metadata) if metadata.len() < recipe.size_bytes => ArtifactState::Partial {
            bytes: metadata.len(),
        },
        Ok(metadata) if metadata.len() != recipe.size_bytes => ArtifactState::Invalid {
            reason: ArtifactInvalidReason::SizeMismatch,
        },
        Ok(_) => match file_matches(&part_path, recipe) {
            Ok(true) => ArtifactState::Partial {
                bytes: recipe.size_bytes,
            },
            Ok(false) => ArtifactState::Invalid {
                reason: ArtifactInvalidReason::ChecksumMismatch,
            },
            Err(_) => ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable,
            },
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => ArtifactState::NotDownloaded,
        Err(_) => ArtifactState::Invalid {
            reason: ArtifactInvalidReason::Unreadable,
        },
    }
}

fn file_matches(path: &Path, recipe: &ModelEntry) -> io::Result<bool> {
    let metadata = path.metadata()?;
    if metadata.len() != recipe.size_bytes {
        return Ok(false);
    }
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let actual = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(actual == recipe.sha256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn inventory_exposes_verified_recipe_metadata_and_memory_reason() {
        let dir = tempdir().unwrap();
        let inventory = known_registry_inventory(dir.path(), 0);
        assert_eq!(inventory.len(), REGISTRY.len());
        let first = &inventory[0];
        assert_eq!(first.id, REGISTRY[0].id);
        assert_eq!(first.license, REGISTRY[0].license);
        assert_eq!(first.quant, REGISTRY[0].quant);
        assert_eq!(first.artifact, ArtifactState::NotDownloaded);
        assert!(!first.compatibility.compatible);
        assert!(first.compatibility.reason.contains("requires"));
        assert_eq!(first.engine.engine, "llama-cpp");
        assert!(first.engine.eligible);
    }

    #[test]
    fn inventory_distinguishes_partial_and_invalid_final_artifacts() {
        let dir = tempdir().unwrap();
        let recipe = &REGISTRY[0];
        std::fs::write(
            dir.path().join(format!("{}.part", recipe.filename)),
            b"partial",
        )
        .unwrap();
        let entry = &known_registry_inventory(dir.path(), u64::MAX)[0];
        assert_eq!(entry.artifact, ArtifactState::Partial { bytes: 7 });
        assert!(entry.compatibility.compatible && entry.engine.eligible);

        std::fs::write(dir.path().join(recipe.filename), b"wrong").unwrap();
        let entry = &known_registry_inventory(dir.path(), u64::MAX)[0];
        assert_eq!(
            entry.artifact,
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::SizeMismatch
            }
        );
    }

    #[test]
    fn artifact_inspection_distinguishes_checksum_invalid_from_downloaded() {
        let dir = tempdir().unwrap();
        let good = b"good";
        let digest = Sha256::digest(good);
        let sha = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let recipe = ModelEntry {
            id: "fixture",
            repo: "owner/repo",
            revision: "main",
            filename: "fixture.gguf",
            sha256: Box::leak(sha.into_boxed_str()),
            size_bytes: good.len() as u64,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        };

        std::fs::write(dir.path().join(recipe.filename), b"evil").unwrap();
        assert_eq!(
            artifact_state(&recipe, dir.path()),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::ChecksumMismatch
            }
        );
        std::fs::write(dir.path().join(recipe.filename), good).unwrap();
        assert_eq!(
            artifact_state(&recipe, dir.path()),
            ArtifactState::Downloaded
        );
    }
}
