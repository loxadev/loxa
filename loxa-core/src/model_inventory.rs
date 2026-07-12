//! Non-blocking inventory snapshots for the compiled, verified model recipes.

use crate::registry::{ModelEntry, REGISTRY};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::UNIX_EPOCH;

const GIB: f64 = 1_073_741_824.0;
const DEFAULT_CACHE_CAPACITY: usize = 64;

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
    VerificationRequired,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StableMetadata {
    len: u64,
    modified_ns: Option<u128>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl StableMetadata {
    fn from(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            len: metadata.len(),
            modified_ns: metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|value| value.as_nanos()),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct VerifiedArtifact {
    pub size_bytes: u64,
    pub expected_sha256: String,
    pub matches: bool,
}

#[derive(Clone)]
struct CachedVerification {
    metadata: StableMetadata,
    evidence: VerifiedArtifact,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<PathBuf, CachedVerification>,
    order: VecDeque<PathBuf>,
}

/// Bounded checksum evidence store. `snapshot` users never perform file hashing.
/// Call `verify_recipe` from a worker before publishing a downloaded state.
pub struct VerificationCache {
    capacity: usize,
    state: Mutex<CacheState>,
    verification_runs: AtomicU64,
}

impl Default for VerificationCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_CAPACITY)
    }
}

impl VerificationCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(CacheState::default()),
            verification_runs: AtomicU64::new(0),
        }
    }

    /// Potentially expensive; intended for a bounded background/blocking worker.
    pub fn verify_recipe(
        &self,
        models_dir: &Path,
        recipe: &ModelEntry,
    ) -> io::Result<VerifiedArtifact> {
        let path = checked_regular_path(models_dir, recipe.filename)?;
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "artifact is not a regular file",
            ));
        }
        let stable = StableMetadata::from(&metadata);
        if let Some(evidence) = self.cached(&path, &stable, recipe.sha256) {
            return Ok(evidence);
        }

        self.verification_runs.fetch_add(1, Ordering::Relaxed);
        let matches = hash_file(&path)? == recipe.sha256;
        let after = fs::symlink_metadata(&path)?;
        if !after.file_type().is_file() || StableMetadata::from(&after) != stable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "artifact changed during verification",
            ));
        }
        let evidence = VerifiedArtifact {
            size_bytes: stable.len,
            expected_sha256: recipe.sha256.into(),
            matches,
        };
        self.insert(
            path,
            CachedVerification {
                metadata: stable,
                evidence: evidence.clone(),
            },
        );
        Ok(evidence)
    }

    pub fn verification_runs(&self) -> u64 {
        self.verification_runs.load(Ordering::Relaxed)
    }

    fn cached(
        &self,
        path: &Path,
        metadata: &StableMetadata,
        expected: &str,
    ) -> Option<VerifiedArtifact> {
        self.state
            .lock()
            .ok()?
            .entries
            .get(path)
            .filter(|item| &item.metadata == metadata && item.evidence.expected_sha256 == expected)
            .map(|item| item.evidence.clone())
    }

    fn insert(&self, path: PathBuf, item: CachedVerification) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.order.retain(|known| known != &path);
        state.order.push_back(path.clone());
        state.entries.insert(path, item);
        while state.entries.len() > self.capacity {
            if let Some(oldest) = state.order.pop_front() {
                state.entries.remove(&oldest);
            }
        }
    }
}

static DEFAULT_CACHE: OnceLock<VerificationCache> = OnceLock::new();

/// Non-blocking snapshot over verified recipes. Full-size artifacts remain
/// `verification_required` until checksum evidence is populated by a worker.
pub fn known_registry_inventory(
    models_dir: &Path,
    available_memory_bytes: u64,
) -> Vec<VerifiedRecipeInventoryEntry> {
    known_registry_inventory_with_cache(
        models_dir,
        available_memory_bytes,
        DEFAULT_CACHE.get_or_init(VerificationCache::default),
    )
}

pub fn known_registry_inventory_with_cache(
    models_dir: &Path,
    available_memory_bytes: u64,
    cache: &VerificationCache,
) -> Vec<VerifiedRecipeInventoryEntry> {
    REGISTRY
        .iter()
        .map(|recipe| inspect_recipe(recipe, models_dir, available_memory_bytes, cache))
        .collect()
}

fn inspect_recipe(
    recipe: &ModelEntry,
    models_dir: &Path,
    available_memory_bytes: u64,
    cache: &VerificationCache,
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
        artifact: artifact_state(recipe, models_dir, cache),
        compatibility,
        engine: EngineEligibility {
            engine: "llama-cpp".into(),
            eligible: true,
            reason: "verified GGUF recipe is eligible for the managed llama.cpp engine".into(),
        },
    }
}

fn artifact_state(
    recipe: &ModelEntry,
    models_dir: &Path,
    cache: &VerificationCache,
) -> ArtifactState {
    let final_path = models_dir.join(recipe.filename);
    match regular_metadata(&final_path) {
        Ok(Some(metadata)) => {
            if metadata.len() != recipe.size_bytes {
                return ArtifactState::Invalid {
                    reason: ArtifactInvalidReason::SizeMismatch,
                };
            }
            return match cache.cached(&final_path, &StableMetadata::from(&metadata), recipe.sha256)
            {
                Some(evidence) if evidence.matches => ArtifactState::Downloaded,
                Some(_) => ArtifactState::Invalid {
                    reason: ArtifactInvalidReason::ChecksumMismatch,
                },
                None => ArtifactState::Invalid {
                    reason: ArtifactInvalidReason::VerificationRequired,
                },
            };
        }
        Ok(None) => {}
        Err(_) => {
            return ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable,
            }
        }
    }
    let part_path = models_dir.join(format!("{}.part", recipe.filename));
    match regular_metadata(&part_path) {
        Ok(Some(metadata)) if metadata.len() < recipe.size_bytes => ArtifactState::Partial {
            bytes: metadata.len(),
        },
        Ok(Some(metadata)) if metadata.len() > recipe.size_bytes => ArtifactState::Invalid {
            reason: ArtifactInvalidReason::SizeMismatch,
        },
        Ok(Some(metadata)) => {
            match cache.cached(&part_path, &StableMetadata::from(&metadata), recipe.sha256) {
                Some(evidence) if !evidence.matches => ArtifactState::Invalid {
                    reason: ArtifactInvalidReason::ChecksumMismatch,
                },
                _ => ArtifactState::Partial {
                    bytes: metadata.len(),
                },
            }
        }
        Ok(None) => ArtifactState::NotDownloaded,
        Err(_) => ArtifactState::Invalid {
            reason: ArtifactInvalidReason::Unreadable,
        },
    }
}

fn regular_metadata(path: &Path) -> io::Result<Option<Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(Some(metadata)),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn checked_regular_path(models_dir: &Path, filename: &str) -> io::Result<PathBuf> {
    if filename.is_empty() || filename.contains(['/', '\\']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "artifact filename is not flat",
        ));
    }
    Ok(models_dir.join(filename))
}

fn hash_file(path: &Path) -> io::Result<String> {
    let mut file = open_regular_no_follow(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const NO_FOLLOW_FLAG: i32 = 0x20_000;
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
const NO_FOLLOW_FLAG: i32 = 0x100;

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd"
))]
fn open_regular_no_follow(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NO_FOLLOW_FLAG)
        .open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_regular_no_follow(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd"
)))]
fn open_regular_no_follow(path: &Path) -> io::Result<File> {
    let before = fs::symlink_metadata(path)?;
    if !before.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        ));
    }
    let file = File::open(path)?;
    if StableMetadata::from(&file.metadata()?) != StableMetadata::from(&before) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact changed while opening",
        ));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture(bytes: &'static [u8]) -> ModelEntry {
        let sha: String = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        ModelEntry {
            id: "fixture",
            repo: "owner/repo",
            revision: "main",
            filename: "fixture.gguf",
            sha256: Box::leak(sha.into_boxed_str()),
            size_bytes: bytes.len() as u64,
            license: "apache-2.0",
            params: "tiny",
            quant: "Q4",
            min_free_mem_gb: 0.1,
        }
    }

    #[test]
    fn inventory_metadata_compatibility_and_partial_are_truthful() {
        let dir = tempdir().unwrap();
        let inventory =
            known_registry_inventory_with_cache(dir.path(), 0, &VerificationCache::default());
        assert_eq!(inventory.len(), REGISTRY.len());
        assert_eq!(inventory[0].license, REGISTRY[0].license);
        assert!(!inventory[0].compatibility.compatible);
        assert!(inventory[0].compatibility.reason.contains("requires"));
        assert!(
            inventory[0].engine.eligible && inventory[0].engine.reason.contains("verified GGUF")
        );
        std::fs::write(
            dir.path().join(format!("{}.part", REGISTRY[0].filename)),
            b"partial",
        )
        .unwrap();
        assert_eq!(
            known_registry_inventory_with_cache(
                dir.path(),
                u64::MAX,
                &VerificationCache::default()
            )[0]
            .artifact,
            ArtifactState::Partial { bytes: 7 }
        );
    }

    #[test]
    fn full_file_never_claims_downloaded_without_cached_checksum_and_invalidates_on_change() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        fs::write(&path, b"good").unwrap();
        let cache = VerificationCache::new(2);
        assert_eq!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::VerificationRequired
            }
        );
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(cache.verification_runs(), 1);
        assert_eq!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Downloaded
        );
        fs::write(&path, b"evil").unwrap();
        assert_ne!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Downloaded
        );
        assert!(!cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(cache.verification_runs(), 2);
        assert_eq!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::ChecksumMismatch
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn inventory_never_follows_symlink_or_accepts_directory_for_final_or_part() {
        use std::os::unix::fs::symlink;
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let recipe = fixture(b"good");
        fs::write(outside.path().join("outside"), b"good").unwrap();
        symlink(
            outside.path().join("outside"),
            dir.path().join(recipe.filename),
        )
        .unwrap();
        let cache = VerificationCache::default();
        assert_eq!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable
            }
        );
        fs::remove_file(dir.path().join(recipe.filename)).unwrap();
        fs::create_dir(dir.path().join(format!("{}.part", recipe.filename))).unwrap();
        assert_eq!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable
            }
        );
    }
}
