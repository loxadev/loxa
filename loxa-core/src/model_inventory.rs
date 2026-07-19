//! Non-blocking inventory snapshots for the compiled, verified model recipes.

use crate::registry::{ModelEntry, REGISTRY};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::UNIX_EPOCH;

const GIB: f64 = 1_073_741_824.0;
const DEFAULT_CACHE_CAPACITY: usize = 64;
const DEFAULT_MAX_CONCURRENT_VERIFICATIONS: usize = 2;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    NotDownloaded,
    Partial { bytes: u64 },
    Downloaded,
    Invalid { reason: ArtifactInvalidReason },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactInvalidReason {
    SizeMismatch,
    ChecksumMismatch,
    Unreadable,
    VerificationRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Compatibility {
    pub compatible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct EngineEligibility {
    pub engine: String,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
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
pub struct StableVerificationIdentity {
    len: u64,
    modified_ns: Option<u128>,
    platform: PlatformFileIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PlatformFileIdentity {
    #[cfg(unix)]
    Unix {
        device: u64,
        inode: u64,
        change_time_s: i64,
        change_time_ns: i64,
    },
    #[cfg(windows)]
    Windows {
        volume_serial: u32,
        file_index: u64,
        last_write_time: u64,
    },
    #[cfg(not(any(unix, windows)))]
    Unsupported,
}

impl StableVerificationIdentity {
    fn from(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        #[cfg(windows)]
        use std::os::windows::fs::MetadataExt;
        Self {
            len: metadata.len(),
            modified_ns: metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|value| value.as_nanos()),
            #[cfg(unix)]
            platform: PlatformFileIdentity::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
                change_time_s: metadata.ctime(),
                change_time_ns: metadata.ctime_nsec(),
            },
            #[cfg(windows)]
            platform: PlatformFileIdentity::Windows {
                volume_serial: metadata.volume_serial_number().unwrap_or_default(),
                file_index: metadata.file_index().unwrap_or_default(),
                last_write_time: metadata.last_write_time(),
            },
            #[cfg(not(any(unix, windows)))]
            platform: PlatformFileIdentity::Unsupported,
        }
    }
}

type StableMetadata = StableVerificationIdentity;

pub struct StableVerificationInput {
    pub opened: File,
    pub stable: StableVerificationIdentity,
    pub expected_sha256: [u8; 32],
}

impl StableVerificationInput {
    pub fn open(path: &Path, expected_sha256: [u8; 32]) -> io::Result<Self> {
        let opened = open_regular_no_follow(path)?;
        let metadata = opened.metadata()?;
        let stable = StableVerificationIdentity::from(&metadata);
        #[cfg(windows)]
        if match &stable.platform {
            PlatformFileIdentity::Windows {
                volume_serial,
                file_index,
                ..
            } => *volume_serial == 0 || *file_index == 0,
        } {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "strong opened-file identity is unavailable",
            ));
        }
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "strong opened-file identity is unavailable",
        ));
        #[cfg(any(unix, windows))]
        Ok(Self {
            opened,
            stable,
            expected_sha256,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct VerifiedArtifact {
    pub size_bytes: u64,
    pub expected_sha256: String,
    pub matches: bool,
}

pub fn verify_opened_artifact(
    mut input: StableVerificationInput,
    cancellation: &dyn VerificationCancellation,
) -> io::Result<VerifiedArtifact> {
    input.opened.rewind()?;
    let digest = hash_open_file_with_cancellation(&mut input.opened, &input.stable, cancellation)?;
    if StableVerificationIdentity::from(&input.opened.metadata()?) != input.stable {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact changed during verification",
        ));
    }
    let expected_sha256: String = input
        .expected_sha256
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(VerifiedArtifact {
        size_bytes: input.stable.len,
        matches: digest == expected_sha256,
        expected_sha256,
    })
}

#[derive(Clone)]
struct CachedVerification {
    metadata: StableMetadata,
    evidence: VerifiedArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct VerificationKey {
    path: PathBuf,
    metadata: StableMetadata,
    expected_sha256: String,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<PathBuf, CachedVerification>,
    order: VecDeque<PathBuf>,
    in_flight: HashMap<VerificationKey, Arc<VerificationFlight>>,
}

#[derive(Clone)]
struct SharedVerificationError {
    kind: io::ErrorKind,
    message: String,
}

type SharedVerificationResult = Result<VerifiedArtifact, SharedVerificationError>;

#[derive(Default)]
struct VerificationFlight {
    result: Mutex<Option<SharedVerificationResult>>,
    ready: Condvar,
}

struct VerificationGate {
    max: usize,
    active: Mutex<usize>,
    available: Condvar,
    max_observed: AtomicU64,
}

struct VerificationPermit {
    gate: Arc<VerificationGate>,
}

impl VerificationGate {
    fn acquire(
        self: &Arc<Self>,
        cancellation: &dyn VerificationCancellation,
    ) -> io::Result<VerificationPermit> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *active >= self.max {
            if cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let (next, _) = self
                .available
                .wait_timeout(active, std::time::Duration::from_millis(10))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active = next;
        }
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        *active += 1;
        self.max_observed
            .fetch_max(*active as u64, Ordering::Relaxed);
        Ok(VerificationPermit { gate: self.clone() })
    }
}

impl Drop for VerificationPermit {
    fn drop(&mut self) {
        let mut active = self
            .gate
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = active.saturating_sub(1);
        self.gate.available.notify_one();
    }
}

pub trait VerificationCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

struct NeverCancel;
impl VerificationCancellation for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Bounded checksum evidence store. `snapshot` users never perform file hashing.
/// Call `verify_recipe` from a worker before publishing a downloaded state.
pub struct VerificationCache {
    capacity: usize,
    state: Mutex<CacheState>,
    verification_runs: AtomicU64,
    gate: Arc<VerificationGate>,
}

impl Default for VerificationCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_CAPACITY)
    }
}

impl VerificationCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_limits(capacity, DEFAULT_MAX_CONCURRENT_VERIFICATIONS)
    }

    pub fn with_limits(capacity: usize, max_concurrent_verifications: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(CacheState::default()),
            verification_runs: AtomicU64::new(0),
            gate: Arc::new(VerificationGate {
                max: max_concurrent_verifications.max(1),
                active: Mutex::new(0),
                available: Condvar::new(),
                max_observed: AtomicU64::new(0),
            }),
        }
    }

    /// Potentially expensive; intended for a bounded background/blocking worker.
    pub fn verify_recipe(
        &self,
        models_dir: &Path,
        recipe: &ModelEntry,
    ) -> io::Result<VerifiedArtifact> {
        self.verify_recipe_with_cancellation(models_dir, recipe, &NeverCancel)
    }

    pub fn verify_recipe_with_cancellation(
        &self,
        models_dir: &Path,
        recipe: &ModelEntry,
        cancellation: &dyn VerificationCancellation,
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

        let key = VerificationKey {
            path: path.clone(),
            metadata: stable.clone(),
            expected_sha256: recipe.sha256.into(),
        };
        let (flight, leader) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(item) = state.entries.get(&path).filter(|item| {
                item.metadata == stable
                    && item.evidence.expected_sha256 == recipe.sha256
                    && evidence_reusable(&item.evidence, positive_cache_reusable())
            }) {
                return Ok(item.evidence.clone());
            }
            if let Some(flight) = state.in_flight.get(&key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(VerificationFlight::default());
                state.in_flight.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };

        if !leader {
            return wait_for_flight(&flight, cancellation);
        }

        let permit = match self.gate.acquire(cancellation) {
            Ok(permit) => permit,
            Err(error) => {
                publish_flight(
                    &flight,
                    &Err(io::Error::new(error.kind(), error.to_string())),
                );
                self.state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .in_flight
                    .remove(&key);
                return Err(error);
            }
        };
        self.verification_runs.fetch_add(1, Ordering::Relaxed);
        let result = (|| {
            let matches =
                hash_file_with_cancellation(&path, &stable, cancellation)? == recipe.sha256;
            let after = fs::symlink_metadata(&path)?;
            if !after.file_type().is_file() || StableMetadata::from(&after) != stable {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "artifact changed during verification",
                ));
            }
            Ok(VerifiedArtifact {
                size_bytes: stable.len,
                expected_sha256: recipe.sha256.into(),
                matches,
            })
        })();
        drop(permit);

        if let Ok(evidence) = &result {
            self.insert(
                path.clone(),
                CachedVerification {
                    metadata: stable,
                    evidence: evidence.clone(),
                },
            );
        }
        publish_flight(&flight, &result);
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight
            .remove(&key);
        result
    }

    pub fn verification_runs(&self) -> u64 {
        self.verification_runs.load(Ordering::Relaxed)
    }

    pub fn max_observed_concurrency(&self) -> usize {
        self.gate.max_observed.load(Ordering::Relaxed) as usize
    }

    /// Returns the current non-blocking artifact state using only evidence
    /// already published into this cache.
    pub fn artifact_state(&self, models_dir: &Path, recipe: &ModelEntry) -> ArtifactState {
        artifact_state(recipe, models_dir, self)
    }

    /// Revalidates scheduler-produced evidence against the exact current
    /// no-follow file identity without hashing the artifact again.
    pub fn revalidate_verified_recipe(
        &self,
        models_dir: &Path,
        recipe: &ModelEntry,
        stable: &StableVerificationIdentity,
        evidence: &VerifiedArtifact,
    ) -> io::Result<()> {
        validate_scheduler_evidence(recipe, stable, evidence)?;
        let path = checked_regular_path(models_dir, recipe.filename)?;
        let opened = open_regular_no_follow(&path)?;
        let current = StableVerificationIdentity::from(&opened.metadata()?);
        if &current != stable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "verified artifact identity changed before publication",
            ));
        }
        Ok(())
    }

    /// Publishes already-hashed scheduler evidence after the owning durable
    /// success commit. The current path identity is checked again immediately
    /// before insertion.
    pub fn publish_verified_recipe(
        &self,
        models_dir: &Path,
        recipe: &ModelEntry,
        stable: &StableVerificationIdentity,
        evidence: &VerifiedArtifact,
    ) -> io::Result<()> {
        self.revalidate_verified_recipe(models_dir, recipe, stable, evidence)?;
        let path = checked_regular_path(models_dir, recipe.filename)?;
        let opened = open_regular_no_follow(&path)?;
        let current = StableVerificationIdentity::from(&opened.metadata()?);
        if &current != stable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "verified artifact identity changed before cache insertion",
            ));
        }
        drop(opened);
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("verification cache lock poisoned"))?;
        insert_cached(
            &mut state,
            self.capacity,
            path,
            CachedVerification {
                metadata: stable.clone(),
                evidence: evidence.clone(),
            },
        );
        Ok(())
    }

    /// Invalidates cached evidence after a failed or destructive artifact
    /// mutation. A later inventory snapshot stays fail-closed until a worker
    /// verifies the current on-disk bytes again.
    pub fn invalidate_recipe(&self, models_dir: &Path, recipe: &ModelEntry) {
        let path = models_dir.join(recipe.filename);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.entries.remove(&path);
        state.order.retain(|known| known != &path);
    }

    fn cached(
        &self,
        path: &Path,
        metadata: &StableMetadata,
        expected: &str,
    ) -> Option<VerifiedArtifact> {
        self.cached_with_positive_policy(path, metadata, expected, positive_cache_reusable())
    }

    fn cached_with_positive_policy(
        &self,
        path: &Path,
        metadata: &StableMetadata,
        expected: &str,
        allow_positive: bool,
    ) -> Option<VerifiedArtifact> {
        self.state
            .lock()
            .ok()?
            .entries
            .get(path)
            .filter(|item| {
                &item.metadata == metadata
                    && item.evidence.expected_sha256 == expected
                    && evidence_reusable(&item.evidence, allow_positive)
            })
            .map(|item| item.evidence.clone())
    }

    fn insert(&self, path: PathBuf, item: CachedVerification) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        insert_cached(&mut state, self.capacity, path, item);
    }
}

fn validate_scheduler_evidence(
    recipe: &ModelEntry,
    stable: &StableVerificationIdentity,
    evidence: &VerifiedArtifact,
) -> io::Result<()> {
    if !evidence.matches
        || evidence.size_bytes != recipe.size_bytes
        || stable.len != recipe.size_bytes
        || evidence.expected_sha256 != recipe.sha256
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scheduler verification evidence does not match the recipe",
        ));
    }
    Ok(())
}

fn insert_cached(state: &mut CacheState, capacity: usize, path: PathBuf, item: CachedVerification) {
    state.order.retain(|known| known != &path);
    state.order.push_back(path.clone());
    state.entries.insert(path, item);
    while state.entries.len() > capacity {
        if let Some(oldest) = state.order.pop_front() {
            state.entries.remove(&oldest);
        }
    }
}

fn evidence_reusable(evidence: &VerifiedArtifact, allow_positive: bool) -> bool {
    !evidence.matches || allow_positive
}

#[cfg(unix)]
fn positive_cache_reusable() -> bool {
    true
}

#[cfg(not(unix))]
fn positive_cache_reusable() -> bool {
    false
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

pub fn current_available_memory_bytes() -> u64 {
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    system.available_memory()
}

pub fn known_registry_inventory_with_cache(
    models_dir: &Path,
    available_memory_bytes: u64,
    cache: &VerificationCache,
) -> Vec<VerifiedRecipeInventoryEntry> {
    verified_recipe_inventory_with_cache(REGISTRY, models_dir, available_memory_bytes, cache)
}

pub fn verified_recipe_inventory_with_cache(
    recipes: &[ModelEntry],
    models_dir: &Path,
    available_memory_bytes: u64,
    cache: &VerificationCache,
) -> Vec<VerifiedRecipeInventoryEntry> {
    recipes
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
            };
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
        Ok(metadata) if metadata.file_type().is_file() && artifact_has_single_link(&metadata) => {
            Ok(Some(metadata))
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact is not a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn artifact_has_single_link(metadata: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}

#[cfg(windows)]
fn artifact_has_single_link(metadata: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.number_of_links() == Some(1)
}

#[cfg(not(any(unix, windows)))]
fn artifact_has_single_link(_: &Metadata) -> bool {
    false
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

fn hash_file_with_cancellation(
    path: &Path,
    expected: &StableMetadata,
    cancellation: &dyn VerificationCancellation,
) -> io::Result<String> {
    let mut file = open_regular_no_follow(path)?;
    hash_open_file_with_cancellation(&mut file, expected, cancellation)
}

fn hash_open_file_with_cancellation(
    file: &mut File,
    expected: &StableMetadata,
    cancellation: &dyn VerificationCancellation,
) -> io::Result<String> {
    let opened = file.metadata()?;
    if !opened.file_type().is_file() || StableMetadata::from(&opened) != *expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "artifact changed while opening for verification",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "artifact verification cancelled",
            ));
        }
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

fn publish_flight(flight: &VerificationFlight, result: &io::Result<VerifiedArtifact>) {
    let shared = match result {
        Ok(value) => Ok(value.clone()),
        Err(error) => Err(SharedVerificationError {
            kind: error.kind(),
            message: error.to_string(),
        }),
    };
    *flight
        .result
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(shared);
    flight.ready.notify_all();
}

fn wait_for_flight(
    flight: &VerificationFlight,
    cancellation: &dyn VerificationCancellation,
) -> io::Result<VerifiedArtifact> {
    let mut result = flight
        .result
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while result.is_none() {
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let (next, _) = flight
            .ready
            .wait_timeout(result, std::time::Duration::from_millis(10))
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        result = next;
    }
    match result.as_ref().unwrap() {
        Ok(value) => Ok(value.clone()),
        Err(error) => Err(io::Error::new(error.kind, error.message.clone())),
    }
}

fn cancelled_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "artifact verification cancelled",
    )
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
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || !artifact_has_single_link(&metadata) {
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
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || !artifact_has_single_link(&metadata) {
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
    let opened = file.metadata()?;
    if !artifact_has_single_link(&before)
        || !artifact_has_single_link(&opened)
        || StableMetadata::from(&opened) != StableMetadata::from(&before)
    {
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
    use std::sync::Barrier;
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

    fn fixture_digest(recipe: &ModelEntry) -> [u8; 32] {
        let mut digest = [0_u8; 32];
        for (index, pair) in recipe.sha256.as_bytes().chunks_exact(2).enumerate() {
            let pair = std::str::from_utf8(pair).unwrap();
            digest[index] = u8::from_str_radix(pair, 16).unwrap();
        }
        digest
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

    #[test]
    fn explicit_invalidation_withdraws_positive_evidence_until_reverified() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        fs::write(dir.path().join(recipe.filename), b"good").unwrap();
        let cache = VerificationCache::default();
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(
            cache.artifact_state(dir.path(), &recipe),
            ArtifactState::Downloaded
        );

        cache.invalidate_recipe(dir.path(), &recipe);

        assert_eq!(
            cache.artifact_state(dir.path(), &recipe),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::VerificationRequired
            }
        );
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(cache.verification_runs(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn verification_rejects_rename_swap_between_path_check_and_open_file() {
        struct RenameSwap {
            calls: std::sync::atomic::AtomicUsize,
            target: PathBuf,
            original: PathBuf,
            replacement: PathBuf,
        }

        impl VerificationCancellation for RenameSwap {
            fn is_cancelled(&self) -> bool {
                match self.calls.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        fs::rename(&self.target, &self.original).unwrap();
                        fs::rename(&self.replacement, &self.target).unwrap();
                    }
                    1 => {
                        fs::rename(&self.target, &self.replacement).unwrap();
                        fs::rename(&self.original, &self.target).unwrap();
                    }
                    _ => {}
                }
                false
            }
        }

        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let target = dir.path().join(recipe.filename);
        let original = dir.path().join("original-bad.gguf");
        let replacement = dir.path().join("replacement-good.gguf");
        fs::write(&target, b"evil").unwrap();
        fs::write(&replacement, b"good").unwrap();
        let cancellation = RenameSwap {
            calls: std::sync::atomic::AtomicUsize::new(0),
            target,
            original,
            replacement,
        };

        let error = VerificationCache::default()
            .verify_recipe_with_cancellation(dir.path(), &recipe, &cancellation)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            !cancellation.is_cancelled(),
            "test hook restores original path"
        );
        assert_eq!(fs::read(dir.path().join(recipe.filename)).unwrap(), b"evil");
    }

    #[test]
    fn opened_file_identity_must_match_the_checked_path_identity() {
        let dir = tempdir().unwrap();
        let checked = dir.path().join("checked.gguf");
        let opened = dir.path().join("opened.gguf");
        fs::write(&checked, b"good").unwrap();
        fs::write(&opened, b"good").unwrap();
        let expected = StableMetadata::from(&fs::symlink_metadata(&checked).unwrap());
        let mut file = open_regular_no_follow(&opened).unwrap();

        let error =
            hash_open_file_with_cancellation(&mut file, &expected, &NeverCancel).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(unix)]
    #[test]
    fn cache_invalidates_same_length_rewrite_even_when_modified_time_is_restored() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        fs::write(&path, b"good").unwrap();
        let original_modified = fs::metadata(&path).unwrap().modified().unwrap();
        let cache = VerificationCache::default();
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);

        fs::write(&path, b"evil").unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(original_modified))
            .unwrap();

        assert_ne!(
            artifact_state(&recipe, dir.path(), &cache),
            ArtifactState::Downloaded
        );
        assert!(!cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(cache.verification_runs(), 2);
    }

    #[test]
    fn concurrent_verification_is_single_flight_and_shares_typed_evidence() {
        let dir = tempdir().unwrap();
        let bytes = Box::leak(vec![7_u8; 8 * 1024 * 1024].into_boxed_slice());
        let recipe = Arc::new(fixture(bytes));
        fs::write(dir.path().join(recipe.filename), &*bytes).unwrap();
        let cache = Arc::new(VerificationCache::default());
        let barrier = Arc::new(Barrier::new(9));
        let models_dir = dir.path().to_path_buf();
        let mut workers = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            let recipe = recipe.clone();
            let models_dir = models_dir.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                cache.verify_recipe(&models_dir, &recipe).unwrap()
            }));
        }
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(results
            .iter()
            .all(|evidence| evidence == &results[0] && evidence.matches));
        assert_eq!(cache.verification_runs(), 1);
    }

    #[test]
    fn distinct_verifications_respect_global_concurrency_limit() {
        let dir = tempdir().unwrap();
        let bytes = Box::leak(vec![4_u8; 4 * 1024 * 1024].into_boxed_slice());
        let cache = Arc::new(VerificationCache::with_limits(64, 1));
        let barrier = Arc::new(Barrier::new(5));
        let mut workers = Vec::new();
        for index in 0..4 {
            let mut recipe = fixture(bytes);
            recipe.id = Box::leak(format!("fixture-{index}").into_boxed_str());
            recipe.filename = Box::leak(format!("fixture-{index}.gguf").into_boxed_str());
            fs::write(dir.path().join(recipe.filename), &*bytes).unwrap();
            let cache = cache.clone();
            let barrier = barrier.clone();
            let models_dir = dir.path().to_path_buf();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                cache.verify_recipe(&models_dir, &recipe).unwrap()
            }));
        }
        barrier.wait();
        for worker in workers {
            assert!(worker.join().unwrap().matches);
        }
        assert_eq!(cache.verification_runs(), 4);
        assert_eq!(cache.max_observed_concurrency(), 1);
    }

    struct GatedCancellation {
        release: std::sync::atomic::AtomicBool,
    }

    struct GatedProceed {
        release: std::sync::atomic::AtomicBool,
    }
    impl VerificationCancellation for GatedProceed {
        fn is_cancelled(&self) -> bool {
            while !self.release.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            false
        }
    }
    struct AlwaysCancelled;
    impl VerificationCancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    #[test]
    fn cancelled_follower_detaches_promptly_without_cancelling_leader() {
        let dir = tempdir().unwrap();
        let recipe = Arc::new(fixture(b"good"));
        fs::write(dir.path().join(recipe.filename), b"good").unwrap();
        let cache = Arc::new(VerificationCache::default());
        let gate = Arc::new(GatedProceed {
            release: std::sync::atomic::AtomicBool::new(false),
        });
        let path = dir.path().to_path_buf();
        let leader = {
            let cache = cache.clone();
            let recipe = recipe.clone();
            let gate = gate.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                cache.verify_recipe_with_cancellation(&path, &recipe, gate.as_ref())
            })
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while cache.state.lock().unwrap().in_flight.is_empty() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        let started = std::time::Instant::now();
        let error = cache
            .verify_recipe_with_cancellation(dir.path(), &recipe, &AlwaysCancelled)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
        gate.release.store(true, Ordering::Release);
        assert!(leader.join().unwrap().unwrap().matches);
        assert_eq!(cache.verification_runs(), 1);
    }
    impl VerificationCancellation for GatedCancellation {
        fn is_cancelled(&self) -> bool {
            while !self.release.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            true
        }
    }

    #[test]
    fn concurrent_cancelled_verification_shares_failure_and_releases_flight_for_retry() {
        let dir = tempdir().unwrap();
        let recipe = Arc::new(fixture(b"good"));
        fs::write(dir.path().join(recipe.filename), b"good").unwrap();
        let cache = Arc::new(VerificationCache::default());
        let cancellation = Arc::new(GatedCancellation {
            release: std::sync::atomic::AtomicBool::new(false),
        });
        let path = dir.path().to_path_buf();
        let leader = {
            let cache = cache.clone();
            let recipe = recipe.clone();
            let cancellation = cancellation.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                cache.verify_recipe_with_cancellation(&path, &recipe, cancellation.as_ref())
            })
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while cache.state.lock().unwrap().in_flight.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "leader did not publish flight"
            );
            std::thread::yield_now();
        }
        let follower = {
            let cache = cache.clone();
            let recipe = recipe.clone();
            let path = path.clone();
            std::thread::spawn(move || cache.verify_recipe(&path, &recipe))
        };
        loop {
            let joined = cache
                .state
                .lock()
                .unwrap()
                .in_flight
                .values()
                .next()
                .map(Arc::strong_count)
                .unwrap_or(0);
            if joined >= 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "follower did not join flight"
            );
            std::thread::yield_now();
        }
        cancellation.release.store(true, Ordering::Release);
        assert_eq!(
            leader.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(
            follower.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(cache.verification_runs(), 0);
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        assert_eq!(cache.verification_runs(), 1);
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

    #[test]
    fn portable_identity_policy_never_reuses_positive_evidence() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        fs::write(&path, b"good").unwrap();
        let cache = VerificationCache::default();
        assert!(cache.verify_recipe(dir.path(), &recipe).unwrap().matches);
        let metadata = StableMetadata::from(&fs::symlink_metadata(&path).unwrap());
        assert!(cache
            .cached_with_positive_policy(&path, &metadata, recipe.sha256, false)
            .is_none());
        assert!(
            cache
                .cached_with_positive_policy(&path, &metadata, recipe.sha256, true)
                .unwrap()
                .matches
        );
    }

    #[test]
    fn scheduler_evidence_publishes_without_a_second_hash_run() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        fs::write(&path, b"good").unwrap();
        let input = StableVerificationInput::open(&path, fixture_digest(&recipe)).unwrap();
        let stable = input.stable.clone();
        let evidence = verify_opened_artifact(input, &NeverCancel).unwrap();
        let cache = VerificationCache::default();

        cache
            .publish_verified_recipe(dir.path(), &recipe, &stable, &evidence)
            .unwrap();

        assert_eq!(cache.verification_runs(), 0);
        assert_eq!(
            cache.artifact_state(dir.path(), &recipe),
            ArtifactState::Downloaded
        );
    }

    #[test]
    fn scheduler_evidence_rejects_replaced_path_identity() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        fs::write(&path, b"good").unwrap();
        let input = StableVerificationInput::open(&path, fixture_digest(&recipe)).unwrap();
        let stable = input.stable.clone();
        let evidence = verify_opened_artifact(input, &NeverCancel).unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"good").unwrap();
        let cache = VerificationCache::default();

        let error = cache
            .publish_verified_recipe(dir.path(), &recipe, &stable, &evidence)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            cache.artifact_state(dir.path(), &recipe),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::VerificationRequired
            }
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn stable_verification_input_rejects_hardlinked_artifact() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        let alias = dir.path().join("artifact-alias.gguf");
        fs::write(&path, b"good").unwrap();
        fs::hard_link(&path, alias).unwrap();

        let error = match StableVerificationInput::open(&path, fixture_digest(&recipe)) {
            Ok(_) => panic!("hardlinked artifact must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn scheduler_evidence_revalidation_rejects_post_verify_hardlink() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        let alias = dir.path().join("artifact-alias.gguf");
        fs::write(&path, b"good").unwrap();
        let input = StableVerificationInput::open(&path, fixture_digest(&recipe)).unwrap();
        let stable = input.stable.clone();
        let evidence = verify_opened_artifact(input, &NeverCancel).unwrap();
        fs::hard_link(&path, alias).unwrap();
        let cache = VerificationCache::default();

        let error = cache
            .revalidate_verified_recipe(dir.path(), &recipe, &stable, &evidence)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn scheduler_evidence_publication_rejects_post_verify_hardlink() {
        let dir = tempdir().unwrap();
        let recipe = fixture(b"good");
        let path = dir.path().join(recipe.filename);
        let alias = dir.path().join("artifact-alias.gguf");
        fs::write(&path, b"good").unwrap();
        let input = StableVerificationInput::open(&path, fixture_digest(&recipe)).unwrap();
        let stable = input.stable.clone();
        let evidence = verify_opened_artifact(input, &NeverCancel).unwrap();
        fs::hard_link(&path, alias).unwrap();
        let cache = VerificationCache::default();

        let error = cache
            .publish_verified_recipe(dir.path(), &recipe, &stable, &evidence)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            cache.artifact_state(dir.path(), &recipe),
            ArtifactState::Invalid {
                reason: ArtifactInvalidReason::Unreadable
            }
        );
    }
}
